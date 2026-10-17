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

//! This node's copy of the cluster's control state, as the pool compares it.

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use s4_core::storage::{KeyVersion, VersionKind, VersionRank};
use s4_core::types::{is_cluster_control, IAM_V2_KEY_PREFIX, SYSTEM_BUCKET};
use s4_core::StorageEngine;
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;

use super::IamAnnouncement;
use crate::error::ClusterError;
use crate::hlc::HlcTimestamp;

/// Records read from storage at a time while the copy loads.
const LOAD_PAGE: usize = 1000;

/// One version of a key of the cluster's control state, as a node holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClassVersion {
    /// Its key in `__system__`.
    pub key: String,
    /// The HLC of the version.
    pub hlc: HlcTimestamp,
    /// The version is the key's tombstone.
    pub tombstone: bool,
    /// The ETag of the version; empty for a tombstone.
    pub etag: String,
}

impl ClassVersion {
    /// The version `record` of `key` holds.
    fn of_record(key: String, record: &s4_core::IndexRecord) -> Self {
        let tombstone = record.is_tombstone();
        Self {
            key,
            hlc: HlcTimestamp::of_record(record),
            tombstone,
            etag: if tombstone {
                String::new()
            } else {
                record.etag.clone()
            },
        }
    }

    /// The rank of the version, as every path of the pool orders versions.
    pub fn rank(&self) -> VersionRank {
        let kind = if self.tombstone {
            VersionKind::Tombstone
        } else {
            VersionKind::Live
        };
        VersionRank::new(self.hlc.as_tuple(), kind, &self.etag)
    }

    /// Whether a node holding `held` of this key lacks this version: it
    /// holds a version of a lower rank, or nothing while this version is a
    /// live one. A tombstone of a key the node holds nothing of is no change
    /// it lacks: either way the key is absent.
    pub fn is_missing_from(&self, held: Option<&ClassVersion>) -> bool {
        match held {
            None => !self.tombstone,
            Some(held) => held.rank() < self.rank(),
        }
    }
}

/// A page of the versions a node holds.
#[derive(Debug, Clone)]
pub struct VersionsPage {
    /// The versions, in key order.
    pub versions: Vec<ClassVersion>,
    /// More keys follow the last one.
    pub is_truncated: bool,
    /// The fingerprint of the node's live records taken before the page was
    /// read: the page holds that state or a later one.
    pub digest: [u8; 16],
}

/// What the copy keeps in memory: the live records' versions and the epoch.
#[derive(Debug)]
struct Held {
    live: BTreeMap<String, (HlcTimestamp, String)>,
    epoch: HlcTimestamp,
    loaded: bool,
}

/// This node's copy of the cluster's control state: what it announces of it
/// and what it answers a peer that catches up with it.
pub struct IamCopy {
    storage: Arc<dyn StorageEngine>,
    /// Held while the copy reads storage and applies what it read, so that a
    /// later read is never applied before an earlier one.
    held: Mutex<Held>,
    announcement: RwLock<Option<IamAnnouncement>>,
}

impl std::fmt::Debug for IamCopy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IamCopy")
            .field("announcement", &self.announcement())
            .finish_non_exhaustive()
    }
}

/// The fingerprint of `live`, in key order.
fn digest_of(live: &BTreeMap<String, (HlcTimestamp, String)>) -> [u8; 16] {
    let mut hasher = Sha256::new();
    for (key, (hlc, etag)) in live {
        hasher.update((key.len() as u64).to_be_bytes());
        hasher.update(key.as_bytes());
        hasher.update(hlc.wall_time.to_be_bytes());
        hasher.update(hlc.logical.to_be_bytes());
        hasher.update(hlc.node_id.to_be_bytes());
        hasher.update((etag.len() as u64).to_be_bytes());
        hasher.update(etag.as_bytes());
    }
    let mut digest = [0u8; 16];
    digest.copy_from_slice(&hasher.finalize()[..16]);
    digest
}

fn storage_error(error: s4_core::error::StorageError) -> ClusterError {
    ClusterError::Storage(error.to_string())
}

impl IamCopy {
    /// The copy `storage` holds, this node's own. Nothing is read until
    /// [`Self::load`].
    pub fn new(storage: Arc<dyn StorageEngine>) -> Self {
        Self {
            storage,
            held: Mutex::new(Held {
                live: BTreeMap::new(),
                epoch: HlcTimestamp::zero(),
                loaded: false,
            }),
            announcement: RwLock::new(None),
        }
    }

    /// What this node announces of its copy; `None` until it is loaded.
    pub fn announcement(&self) -> Option<IamAnnouncement> {
        *self.announcement.read().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn announce(&self, held: &Held) -> IamAnnouncement {
        let announcement = IamAnnouncement {
            epoch: held.epoch,
            digest: digest_of(&held.live),
        };
        *self.announcement.write().unwrap_or_else(|poisoned| poisoned.into_inner()) =
            Some(announcement);
        announcement
    }

    /// Reads every record of the cluster's control state this node holds,
    /// and what it announces of them. The epoch never goes back: a purged
    /// tombstone was the newest record a moment ago.
    ///
    /// # Errors
    ///
    /// `Storage` when the records cannot be listed.
    pub async fn load(&self) -> Result<IamAnnouncement, ClusterError> {
        let mut held = self.held.lock().await;
        let mut live = BTreeMap::new();
        let mut epoch = held.epoch;
        let mut after: Option<String> = None;
        loop {
            let page = self
                .storage
                .list_objects_with_tombstones(
                    SYSTEM_BUCKET,
                    IAM_V2_KEY_PREFIX,
                    after.as_deref(),
                    LOAD_PAGE,
                )
                .await
                .map_err(storage_error)?;
            let full = page.len() == LOAD_PAGE;
            after = page.last().map(|(key, _)| key.clone());
            for (key, record) in page {
                if !key.starts_with(IAM_V2_KEY_PREFIX) {
                    continue;
                }
                let version = ClassVersion::of_record(key, &record);
                epoch = epoch.max(version.hlc);
                if !version.tombstone {
                    live.insert(version.key, (version.hlc, version.etag));
                }
            }
            if !full {
                break;
            }
        }
        held.live = live;
        held.epoch = epoch;
        held.loaded = true;
        Ok(self.announce(&held))
    }

    /// Brings the copy up to the record at `bucket/key` of this node's
    /// storage when it is one of the cluster's control state; any other key
    /// is not the copy's. Called after every change of the storage a cluster
    /// node makes — through this node, from a peer, a hint or a repair.
    ///
    /// # Errors
    ///
    /// `Storage` when the record cannot be read.
    pub async fn observe(&self, bucket: &str, key: &str) -> Result<(), ClusterError> {
        if !is_cluster_control(bucket, key) {
            return Ok(());
        }
        let mut held = self.held.lock().await;
        if !held.loaded {
            // The load reads every record, this one included.
            return Ok(());
        }
        match self.version_of(key).await? {
            Some(version) => {
                held.epoch = held.epoch.max(version.hlc);
                if version.tombstone {
                    held.live.remove(key);
                } else {
                    held.live.insert(version.key, (version.hlc, version.etag));
                }
            }
            None => {
                held.live.remove(key);
            }
        }
        self.announce(&held);
        Ok(())
    }

    /// The version of `key` this node's storage holds, `None` for nothing.
    ///
    /// # Errors
    ///
    /// `Storage` when it cannot be read.
    pub async fn version_of(&self, key: &str) -> Result<Option<ClassVersion>, ClusterError> {
        match self.storage.key_version(SYSTEM_BUCKET, key).await.map_err(storage_error)? {
            KeyVersion::Live(record) | KeyVersion::Tombstone(record) => {
                Ok(Some(ClassVersion::of_record(key.to_string(), &record)))
            }
            KeyVersion::Absent => Ok(None),
        }
    }

    /// Up to `max_keys` versions this node holds after `start_after`,
    /// tombstones included, with the fingerprint taken before they were
    /// read.
    ///
    /// # Errors
    ///
    /// `Unavailable` before the copy is loaded; `Storage` when the versions
    /// cannot be listed.
    pub async fn versions(
        &self,
        start_after: Option<&str>,
        max_keys: usize,
    ) -> Result<VersionsPage, ClusterError> {
        let Some(announcement) = self.announcement() else {
            return Err(ClusterError::Unavailable(
                "this node has not read its copy of IAM yet".into(),
            ));
        };
        let listed = self
            .storage
            .list_objects_with_tombstones(
                SYSTEM_BUCKET,
                IAM_V2_KEY_PREFIX,
                start_after,
                max_keys + 1,
            )
            .await
            .map_err(storage_error)?;
        let is_truncated = listed.len() > max_keys;
        let versions = listed
            .into_iter()
            .take(max_keys)
            .filter(|(key, _)| key.starts_with(IAM_V2_KEY_PREFIX))
            .map(|(key, record)| ClassVersion::of_record(key, &record))
            .collect();
        Ok(VersionsPage {
            versions,
            is_truncated,
            digest: announcement.digest,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use s4_core::storage::BitcaskStorageEngine;
    use std::collections::HashMap;

    async fn storage() -> (Arc<dyn StorageEngine>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let engine = BitcaskStorageEngine::new(
            dir.path().join("volumes"),
            dir.path().join("metadata"),
            10 * 1024 * 1024,
            4096,
            false,
        )
        .await
        .unwrap();
        (Arc::new(engine), dir)
    }

    async fn put(storage: &dyn StorageEngine, key: &str, body: &[u8]) {
        storage
            .put_object(
                SYSTEM_BUCKET,
                key,
                body,
                "application/json",
                &HashMap::new(),
            )
            .await
            .unwrap();
    }

    fn version(key: &str, wall: u64, tombstone: bool, etag: &str) -> ClassVersion {
        ClassVersion {
            key: key.to_string(),
            hlc: HlcTimestamp::from_record(wall, 0, 7),
            tombstone,
            etag: etag.to_string(),
        }
    }

    /// Two copies of the same records announce the same fingerprint, and a
    /// change of one record — written, deleted — changes it; the epoch never
    /// goes back.
    #[tokio::test]
    async fn the_fingerprint_follows_the_live_records() {
        let (a, _da) = storage().await;
        let (b, _db) = storage().await;
        let copy_a = IamCopy::new(a.clone());
        let copy_b = IamCopy::new(b.clone());
        assert_eq!(copy_a.announcement(), None);
        let empty = copy_a.load().await.unwrap();
        assert_eq!(empty, copy_b.load().await.unwrap());
        assert!(empty.epoch.is_zero());

        put(&*a, "__s4_iam_v2_user_1", b"one").await;
        copy_a.observe(SYSTEM_BUCKET, "__s4_iam_v2_user_1").await.unwrap();
        let written = copy_a.announcement().unwrap();
        assert_ne!(written.digest, empty.digest);
        assert!(written.epoch > empty.epoch);

        // Keys outside the class are not the copy's.
        put(&*a, "__s4_iam_node_lease", b"ledger").await;
        copy_a.observe(SYSTEM_BUCKET, "__s4_iam_node_lease").await.unwrap();
        copy_a.observe("photos", "__s4_iam_v2_user_1").await.unwrap();
        assert_eq!(copy_a.announcement(), Some(written));

        // A load reads what observing found.
        assert_eq!(copy_a.load().await.unwrap(), written);

        a.delete_object(SYSTEM_BUCKET, "__s4_iam_v2_user_1").await.unwrap();
        copy_a.observe(SYSTEM_BUCKET, "__s4_iam_v2_user_1").await.unwrap();
        let deleted = copy_a.announcement().unwrap();
        assert_eq!(deleted.digest, empty.digest);
        assert_eq!(deleted.epoch, written.epoch, "the epoch never goes back");
    }

    /// A page lists tombstones too, in key order, a page at a time, with the
    /// fingerprint of the copy.
    #[tokio::test]
    async fn the_versions_come_a_page_at_a_time() {
        let (storage, _dir) = storage().await;
        for id in 0..5 {
            put(&*storage, &format!("__s4_iam_v2_user_{id}"), b"x").await;
        }
        put(&*storage, "__s4_iam_node_lease", b"ledger").await;
        let copy = IamCopy::new(storage);
        assert!(matches!(
            copy.versions(None, 10).await,
            Err(ClusterError::Unavailable(_))
        ));
        let loaded = copy.load().await.unwrap();

        let first = copy.versions(None, 3).await.unwrap();
        assert!(first.is_truncated);
        assert_eq!(first.digest, loaded.digest);
        let keys: Vec<_> = first.versions.iter().map(|v| v.key.as_str()).collect();
        assert_eq!(
            keys,
            [
                "__s4_iam_v2_user_0",
                "__s4_iam_v2_user_1",
                "__s4_iam_v2_user_2"
            ]
        );
        let rest = copy.versions(Some("__s4_iam_v2_user_2"), 3).await.unwrap();
        assert!(!rest.is_truncated);
        assert_eq!(rest.versions.len(), 2);
        assert!(rest.versions.iter().all(|v| !v.tombstone && !v.etag.is_empty()));
    }

    #[test]
    fn a_version_is_missing_only_from_a_node_holding_less() {
        let live = version("k", 10, false, "e1");
        assert!(live.is_missing_from(None));
        assert!(live.is_missing_from(Some(&version("k", 9, false, "e0"))));
        assert!(!live.is_missing_from(Some(&live)));
        assert!(!live.is_missing_from(Some(&version("k", 11, true, ""))));

        let deleted = version("k", 10, true, "");
        assert!(
            !deleted.is_missing_from(None),
            "absent is as good as deleted"
        );
        assert!(deleted.is_missing_from(Some(&version("k", 9, false, "e0"))));
        assert!(!deleted.is_missing_from(Some(&deleted)));
        // At one HLC the tombstone outranks the copy.
        assert!(deleted.is_missing_from(Some(&version("k", 10, false, "e1"))));
    }
}
