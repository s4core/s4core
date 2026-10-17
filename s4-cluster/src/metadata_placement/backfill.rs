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

//! The backfill of the new replicas (plan BUCKET-METADATA-REPLICATION.md,
//! decision 5, task BMR-04-001).
//!
//! At stage 1, once every node of the pool announced it, no write of bucket
//! metadata goes by the old layout alone any more; what the old layout holds
//! from before is brought to the new replicas here. A node walks the bucket
//! metadata keys of its own version digests, tombstones included, and settles
//! each on every replica of both layouts: every one of them has to answer.
//!
//! A copy is spread only when it is confirmed by the old layout's rule: as
//! many replicas of the old layout hold it as a confirmed write leaves there,
//! or every replica without the key keeps tombstones from before it. The
//! router places a key at stage 1 with the old layout as the one reads take,
//! and the confirmation counts its replicas alone
//! ([`KeyVersions::confirming_replicas`](crate::repair::reconcile::KeyVersions)).
//! A copy short of that is one whose write failed: it reads as absent before
//! the move and after, is left as it is and counted `unconfirmed`. A
//! tombstone is carried to every replica, however old: no tombstone of bucket
//! metadata is removed while the pool is at stage 1 or 2.
//!
//! A marker this node made up rather than was written — it took a write into
//! a bucket whose marker it did not hold, and an earlier release stamped that
//! copy with its modification time, which can rank above the bucket's real
//! marker or outlive the bucket's delete — is no copy of the marker. Before
//! the move only the old layout's replicas are read, and a node outside it
//! keeps such a copy harmlessly; the new layout makes it a replica. The
//! backfill removes exactly such copies outside the old layout before it
//! settles the marker (`dropped`), whichever node settles it first, and the
//! key is settled as any other: a bucket that exists gets its real marker
//! there.
//!
//! A pass in which every key was settled or left unconfirmed is complete,
//! and the node may announce its backfill finished. A pass in which a replica
//! did not answer, or did not take a key, is not; the driver runs another.

use std::sync::Arc;
use std::time::Duration;

use s4_core::types::{is_bucket_marker, SYSTEM_BUCKET};
use s4_core::StorageEngine;
use tracing::debug;

use crate::placement::ReplicationClass;
use crate::repair::reconcile::{KeyRef, Outcome, Reconciler, ReplicaHolds, Settled};

/// Version digests read per page of a pass.
const SCAN_PAGE: usize = 4096;

/// What one pass did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BackfillReport {
    /// Keys at least one replica was brought to.
    pub copied: u64,
    /// Keys every replica already held.
    pub already: u64,
    /// Keys whose newest copy the old layout does not confirm, left as they
    /// are.
    pub unconfirmed: u64,
    /// Keys not settled: a replica did not answer or did not take them.
    pub failed: u64,
    /// Copies of markers made up outside the old layout, removed.
    pub dropped: u64,
    /// Why the first key failed, or the pass could not read its digests.
    pub first_failure: Option<String>,
}

impl BackfillReport {
    /// Whether the pass brought every confirmed key to every replica.
    pub fn complete(&self) -> bool {
        self.failed == 0 && self.first_failure.is_none()
    }
}

/// One pass of the backfill, as the driver of the stages runs it.
#[async_trait::async_trait]
pub trait BackfillPass: Send + Sync {
    /// Brings the bucket metadata this node holds to every replica.
    async fn pass(&self) -> BackfillReport;
}

/// The backfill of one node: its own digests, settled through `reconciler`,
/// whose source places keys by this node's stage.
pub struct Backfill {
    storage: Arc<dyn StorageEngine>,
    reconciler: Reconciler,
    page: usize,
}

impl Backfill {
    /// The backfill of the node whose storage is `storage`.
    pub fn new(storage: Arc<dyn StorageEngine>, reconciler: Reconciler) -> Self {
        Self {
            storage,
            reconciler,
            page: SCAN_PAGE,
        }
    }

    /// The same backfill, reading `page` digests at a time.
    #[cfg(test)]
    pub(crate) fn reading_pages_of(mut self, page: usize) -> Self {
        self.page = page;
        self
    }
}

impl Backfill {
    /// Removes every copy of the marker `key` that its node made up and that
    /// is outside the old layout; how many. Done before the key is settled,
    /// whichever node settles it first: such a copy, stamped by an earlier
    /// release with its modification time, ranks above the real marker and
    /// would be spread over it.
    async fn clear_made_up_markers(&self, key: &str) -> Result<u64, String> {
        let versions = self
            .reconciler
            .source()
            .key_versions(SYSTEM_BUCKET, key)
            .await
            .map_err(|e| format!("the marker's replicas could not be read: {e}"))?;
        let marker = KeyRef {
            bucket: SYSTEM_BUCKET,
            key,
        };
        let mut dropped = 0;
        for (node, holds) in &versions.answers {
            if versions.confirming_replicas.contains(node) {
                continue;
            }
            let ReplicaHolds::Version {
                hlc,
                tombstone: false,
                erasure_coded: false,
                ..
            } = *holds
            else {
                continue;
            };
            if !hlc.is_made_up() {
                continue;
            }
            match self.reconciler.remove_made_up(*node, marker, hlc, hlc).await {
                Ok(Outcome::Delivered) => {
                    debug!(key, %node, "backfill removed a marker made up outside the old layout");
                    dropped += 1;
                }
                Ok(other) => return Err(format!("replica {node}: {other:?}")),
                Err(e) => return Err(format!("replica {node}: {e}")),
            }
        }
        Ok(dropped)
    }
}

#[async_trait::async_trait]
impl BackfillPass for Backfill {
    async fn pass(&self) -> BackfillReport {
        let mut report = BackfillReport::default();
        let mut after: Option<String> = None;
        loop {
            let page =
                match self.storage.version_digests_in(0, None, after.as_deref(), self.page).await {
                    Ok(page) => page,
                    Err(e) => {
                        report.first_failure = Some(format!("version digests: {e}"));
                        return report;
                    }
                };
            for entry in &page {
                if ReplicationClass::of_full_key(&entry.key) != ReplicationClass::BucketMetadata {
                    continue;
                }
                let Some((bucket, key)) = entry.key.split_once('/') else {
                    continue;
                };
                if is_bucket_marker(bucket, key) {
                    match self.clear_made_up_markers(key).await {
                        Ok(dropped) => report.dropped += dropped,
                        Err(why) => {
                            report.failed += 1;
                            report.first_failure.get_or_insert(format!("{}: {why}", entry.key));
                            continue;
                        }
                    }
                }
                let (settled, brought) = self
                    .reconciler
                    .settle_counting(KeyRef { bucket, key }, None, Duration::MAX)
                    .await;
                match settled {
                    Settled::InStep if brought > 0 => report.copied += 1,
                    Settled::InStep | Settled::NotOurs => report.already += 1,
                    Settled::Ambiguous(why) => {
                        debug!(key = %entry.key, %why, "backfill left an unconfirmed copy");
                        report.unconfirmed += 1;
                    }
                    Settled::Failed(why) => {
                        report.failed += 1;
                        report.first_failure.get_or_insert(format!("{}: {why}", entry.key));
                    }
                }
            }
            match page.last() {
                Some(last) if page.len() == self.page => after = Some(last.key.clone()),
                _ => return report,
            }
        }
    }
}
