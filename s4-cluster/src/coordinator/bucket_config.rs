// Copyright 2026 S4Core Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! A bucket's configuration as the quorums hold it.
//!
//! A configuration belongs to the generation of its bucket
//! ([`config_of_generation`]): it counts only when it is newer than the
//! bucket's live marker. A write of a configuration reads the marker first,
//! and that read moves the writing node's clock past the marker, so a
//! configuration of the bucket is always newer than its marker; one older
//! than the marker was left by an earlier bucket of the same name.
//!
//! A configuration an earlier release stored has no HLC; the one made up for
//! it comes from the clock of the node that stored it, while the marker's
//! came from the node that created the bucket. Two clocks of a pool differ by
//! a little, and a configuration written right after its bucket — as
//! Terraform and scripts do — may come out a few milliseconds older than the
//! marker by them. Such a configuration is weighed with
//! [`LEGACY_CONFIG_CLOCK_TOLERANCE`] of slack: the clock skew the cluster
//! tolerates. Within it the two clocks cannot tell a configuration set right
//! after the bucket was created from one set right before an earlier bucket of
//! the same name was deleted; the first is taken, and the adoption that takes
//! it says so in the log ([`within_clock_tolerance`]).

use std::time::Duration;

use s4_core::types::{BUCKET_MARKER_KEY_PREFIX, SYSTEM_BUCKET};
use tracing::debug;

use s4_core::storage::{VersionKind, VersionRank};

use super::read::QuorumReadCoordinator;
use crate::clock::hlc::HlcTimestamp;
use crate::error::ClusterError;
use crate::repair::reconcile::KeyStateSource;

/// How much older than its bucket's marker a configuration with a made-up
/// HLC may look and still belong to the bucket: the clock skew between nodes
/// the cluster tolerates ([`MAX_CLOCK_SKEW`](crate::gc::tombstone::MAX_CLOCK_SKEW)),
/// the most the marker's HLC — the clock of the node that created the bucket,
/// or one ahead of it that clock observed — can run ahead of the clock of the
/// node that stored the configuration after it.
pub const LEGACY_CONFIG_CLOCK_TOLERANCE: Duration = crate::gc::tombstone::MAX_CLOCK_SKEW;

/// Whether a configuration of HLC `config` belongs to the bucket whose live
/// marker has HLC `marker`, and not to an earlier bucket of the same name.
///
/// One issued by a node is newer than the marker. One made up for a record
/// stored without an HLC ([`HlcTimestamp::is_made_up`]) is ordered only by
/// the wall clock of the node that stored it, and gets
/// [`LEGACY_CONFIG_CLOCK_TOLERANCE`] of slack against the marker.
pub fn config_of_generation(config: HlcTimestamp, marker: HlcTimestamp) -> bool {
    if config.is_made_up() {
        let tolerance =
            u64::try_from(LEGACY_CONFIG_CLOCK_TOLERANCE.as_millis()).unwrap_or(u64::MAX);
        config.wall_time.saturating_add(tolerance) >= marker.wall_time
    } else {
        config > marker
    }
}

/// Whether a configuration of HLC `config` belongs to the bucket of marker
/// `marker` only by [`LEGACY_CONFIG_CLOCK_TOLERANCE`]: its made-up HLC is not
/// newer than the marker, and the clocks cannot tell it from one of an earlier
/// bucket.
pub fn within_clock_tolerance(config: HlcTimestamp, marker: HlcTimestamp) -> bool {
    config.is_made_up()
        && config.wall_time <= marker.wall_time
        && config_of_generation(config, marker)
}

/// What the quorums hold for one configuration of a bucket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BucketConfigRead {
    /// No read quorum knows the bucket.
    NoSuchBucket,
    /// The bucket exists, and the configuration is not set for it: never
    /// written, deleted, or left from an earlier bucket of the same name.
    NotSet,
    /// The bucket's configuration.
    Set(Vec<u8>),
}

/// The copy of a configuration the asking node holds in its own storage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnConfig {
    /// The stored configuration.
    pub data: Vec<u8>,
    /// Its HLC, as [`HlcTimestamp::of_record`] gives it.
    pub hlc: HlcTimestamp,
    /// Its ETag.
    pub etag: String,
}

impl QuorumReadCoordinator {
    /// The configuration of `bucket` stored under `config_key` of the system
    /// bucket: the bucket's marker and the configuration are read through
    /// read quorums, side by side — the rule of generations needs no order of
    /// the reads.
    ///
    /// # Errors
    ///
    /// Whatever error kept a read quorum of the marker or of the
    /// configuration from answering.
    pub async fn read_bucket_config(
        &self,
        bucket: &str,
        config_key: &str,
    ) -> Result<BucketConfigRead, ClusterError> {
        self.read_bucket_config_with_own(bucket, config_key, None).await
    }

    /// [`Self::read_bucket_config`] on a node that may hold the only copy of
    /// the configuration: one an earlier release stored on the node that took
    /// it, before the pool adopted such copies (`ConfigReadGate`).
    ///
    /// `own` is the answer when it belongs to the generation of the bucket's
    /// marker and no replica of the key that answered holds a version ranked
    /// above it — a newer value, or the delete of this one. The quorums answer
    /// otherwise. The bucket itself is always asked of the quorums.
    ///
    /// # Errors
    ///
    /// Whatever error kept a read quorum of the marker from answering, or of
    /// the configuration when `own` does not answer for it.
    pub async fn read_bucket_config_with_own(
        &self,
        bucket: &str,
        config_key: &str,
        own: Option<OwnConfig>,
    ) -> Result<BucketConfigRead, ClusterError> {
        // Local fallback serves configurations written before quorum writes,
        // never a modern copy left behind outside current placement.
        let own = own.filter(|copy| copy.hlc.is_made_up());
        let marker_key = format!("{BUCKET_MARKER_KEY_PREFIX}{bucket}");
        let held = async {
            match &own {
                Some(_) => Some(self.key_versions(SYSTEM_BUCKET, config_key).await),
                None => None,
            }
        };
        let (marker, config, held) = tokio::join!(
            self.head(SYSTEM_BUCKET, &marker_key),
            self.read(SYSTEM_BUCKET, config_key),
            held
        );
        let marker = match marker {
            Ok(marker) => marker,
            Err(ClusterError::ObjectNotFound { .. }) => return Ok(BucketConfigRead::NoSuchBucket),
            Err(e) => return Err(e),
        };
        if let (Some(own), Some(held)) = (own, held) {
            let rank = VersionRank::new(own.hlc.as_tuple(), VersionKind::Live, &own.etag);
            let outranked = match &held {
                Ok(held) => held.answers.iter().any(|(_, holds)| holds.rank() > Some(rank)),
                Err(_) => false,
            };
            if config_of_generation(own.hlc, marker.hlc) && !outranked {
                return Ok(BucketConfigRead::Set(own.data));
            }
            debug!(
                bucket,
                config_key,
                config_hlc = %own.hlc,
                marker_hlc = %marker.hlc,
                outranked,
                "own copy of a configuration of an earlier bucket, or one replaced since, not read"
            );
        }
        match config {
            Ok(config) if config_of_generation(config.hlc, marker.hlc) => {
                Ok(BucketConfigRead::Set(config.data))
            }
            Ok(config) => {
                debug!(
                    bucket,
                    config_key,
                    config_hlc = %config.hlc,
                    marker_hlc = %marker.hlc,
                    "configuration of an earlier bucket of the same name, read as not set"
                );
                Ok(BucketConfigRead::NotSet)
            }
            Err(ClusterError::ObjectNotFound { .. }) => Ok(BucketConfigRead::NotSet),
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordinator::test_pool::{at, Replica, TestPool, BUCKET};

    const CORS: &str = "__s4_bucket_cors_bucket";

    fn made_up(wall: u64) -> HlcTimestamp {
        HlcTimestamp::from_record(wall, 0, 0)
    }

    /// A configuration a node issued an HLC for is newer than its marker; one
    /// with a made-up HLC gets the tolerance of the clocks, and no more.
    #[test]
    fn the_rule_of_generations_gives_made_up_clocks_their_tolerance() {
        let marker = at(100_000);
        assert!(config_of_generation(at(100_001), marker));
        assert!(!config_of_generation(at(100_000), marker));
        assert!(!config_of_generation(at(99_999), marker));

        // Stored right after CreateBucket by a node whose clock runs behind.
        assert!(config_of_generation(made_up(99_950), marker));
        assert!(within_clock_tolerance(made_up(99_950), marker));
        // A made-up marker and configuration of one millisecond: single mode.
        assert!(config_of_generation(made_up(100_000), made_up(100_000)));
        // Newer than the marker by the clocks: no tolerance needed.
        assert!(config_of_generation(made_up(100_001), marker));
        assert!(!within_clock_tolerance(made_up(100_001), marker));
        // The tolerance is the clock skew the cluster tolerates, and no more.
        let tolerance = LEGACY_CONFIG_CLOCK_TOLERANCE.as_millis() as u64;
        assert_eq!(tolerance, 500);
        assert!(config_of_generation(made_up(100_000 - tolerance), marker));
        assert!(!config_of_generation(
            made_up(100_000 - tolerance - 1),
            marker
        ));
        assert!(!within_clock_tolerance(
            made_up(100_000 - tolerance - 1),
            marker
        ));
    }

    /// Node 1's own copy is read when it is of the bucket's generation and
    /// nothing newer is held; the quorums answer when the copy is of an
    /// earlier bucket, or when a replica holds a newer value or its delete.
    #[tokio::test]
    async fn the_own_copy_is_read_only_when_it_is_current() {
        let now = crate::clock::hlc::now_millis();
        let own = |data: &[u8], hlc| OwnConfig {
            data: data.to_vec(),
            hlc,
            etag: format!("{:x}", md5::compute(data)),
        };

        let pool = TestPool::start(Replica::Up, Replica::Up, 21350).await;
        for node in 1..=3 {
            pool.create_bucket(node, at(now - 60_000)).await;
        }
        let read = pool.read_coordinator();
        let current = own(b"own", made_up(now - 30_000));
        assert_eq!(
            read.read_bucket_config_with_own(BUCKET, CORS, Some(current.clone()))
                .await
                .unwrap(),
            BucketConfigRead::Set(b"own".to_vec())
        );
        assert_eq!(
            read.read_bucket_config(BUCKET, CORS).await.unwrap(),
            BucketConfigRead::NotSet
        );

        // Of a bucket of the same name deleted before this one was created.
        let earlier = own(b"earlier", made_up(now - 120_000));
        assert_eq!(
            read.read_bucket_config_with_own(BUCKET, CORS, Some(earlier)).await.unwrap(),
            BucketConfigRead::NotSet
        );

        // Replaced through the quorums since.
        for node in 2..=3 {
            pool.put_system(node, CORS, b"newer", at(now - 10_000)).await;
        }
        assert_eq!(
            read.read_bucket_config_with_own(BUCKET, CORS, Some(current.clone()))
                .await
                .unwrap(),
            BucketConfigRead::Set(b"newer".to_vec())
        );

        // Deleted through the quorums since.
        for node in 2..=3 {
            pool.delete_system(node, CORS, at(now - 5_000)).await;
        }
        assert_eq!(
            read.read_bucket_config_with_own(BUCKET, CORS, Some(current)).await.unwrap(),
            BucketConfigRead::NotSet
        );
    }

    /// Reopening the adoption gate after a crash must not make a former
    /// owner's modern copy authoritative again, including after the current
    /// owners have collected the tombstone of its deletion.
    #[tokio::test]
    async fn a_modern_foreign_copy_is_not_read_after_its_delete_was_collected() {
        use s4_core::{storage::TombstoneRef, StorageEngine};

        let now = crate::clock::hlc::now_millis();
        let pool = TestPool::start(Replica::Up, Replica::Up, 21354).await;
        let config = at(now - 30_000);
        let deleted = at(now - 10_000);
        for node in 1..=3 {
            pool.create_bucket(node, at(now - 60_000)).await;
            pool.put_system(node, CORS, b"former-owner", config).await;
            pool.delete_system(node, CORS, deleted).await;
            assert_eq!(
                pool.storage(node)
                    .gc_tombstones(&[TombstoneRef {
                        full_key: format!("{SYSTEM_BUCKET}/{CORS}"),
                        hlc: deleted.as_tuple(),
                    }])
                    .await
                    .unwrap(),
                1
            );
        }
        // Supplied by the handler while its gate is closed; this copy is
        // outside current placement and so cannot participate in the quorum.
        let own = OwnConfig {
            data: b"former-owner".to_vec(),
            hlc: config,
            etag: format!("{:x}", md5::compute(b"former-owner")),
        };
        let read = pool.read_coordinator();
        assert_eq!(
            read.read_bucket_config(BUCKET, CORS).await.unwrap(),
            BucketConfigRead::NotSet
        );
        assert_eq!(
            read.read_bucket_config_with_own(BUCKET, CORS, Some(own)).await.unwrap(),
            BucketConfigRead::NotSet,
            "a closed adoption gate must not resurrect a deleted modern configuration"
        );
    }
}
