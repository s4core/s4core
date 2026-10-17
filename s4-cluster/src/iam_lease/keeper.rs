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

//! The lease of IAM changes as a node keeps it: its [`LeaseLedger`], kept in
//! the node's own storage before any answer leaves, so that a restart forgets
//! neither an epoch it granted nor the floor the next holder must write above.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use s4_core::error::StorageError;
use s4_core::types::{IAM_V2_KEY_PREFIX, SYSTEM_BUCKET};
use s4_core::StorageEngine;
use tokio::sync::{Mutex, OwnedMutexGuard};
use tracing::{debug, warn};

use super::ledger::{Fenced, Grant, LeaseFence, LeaseLedger, LedgerRecord, LiveGrant};
use crate::error::ClusterError;
use crate::hlc::HlcTimestamp;
use crate::identity::NodeId;

/// Where a node keeps its ledger: a record of `__system__` of its own, never
/// replicated (only `__s4_iam_v2_*` is).
pub const LEASE_RECORD_KEY: &str = "__s4_iam_node_lease";

/// Why a node did not take a write of the cluster's control state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refusal {
    /// Made outside the current lease.
    Fenced(Fenced),
    /// The epoch it taught this node could not be kept.
    Storage(String),
}

impl std::fmt::Display for Refusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Fenced(reason) => write!(f, "{reason}"),
            Self::Storage(error) => write!(f, "IAM lease not kept: {error}"),
        }
    }
}

/// A record of the cluster's control state as one node holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClassRecord {
    /// Its key in `__system__`.
    pub key: String,
    /// What it holds; `None` for a tombstone.
    pub data: Option<Vec<u8>>,
    /// The HLC of the version.
    pub hlc: HlcTimestamp,
    /// The ETag of the version; empty for a tombstone.
    pub etag: String,
}

impl ClassRecord {
    /// The rank of the version, as every path of the pool orders versions.
    pub fn rank(&self) -> s4_core::storage::VersionRank {
        let kind = if self.data.is_some() {
            s4_core::storage::VersionKind::Live
        } else {
            s4_core::storage::VersionKind::Tombstone
        };
        s4_core::storage::VersionRank::new(self.hlc.as_tuple(), kind, &self.etag)
    }
}

/// A write this node admitted. Until it is dropped the node grants no new
/// epoch, so the write lands before any later holder reads this node.
pub struct Admitted {
    _ledger: OwnedMutexGuard<LeaseLedger>,
}

/// One node's grants of the lease of IAM changes and its fencing of their
/// writes.
pub struct IamLeaseKeeper {
    storage: Arc<dyn StorageEngine>,
    ledger: Arc<Mutex<LeaseLedger>>,
    origin: Instant,
}

impl IamLeaseKeeper {
    /// The keeper of the ledger `storage` holds: what this node granted
    /// before it restarted has run out, and every epoch and floor is as it
    /// left them.
    ///
    /// # Errors
    ///
    /// `Storage` when the ledger cannot be read: a node that forgot its
    /// grants could grant an epoch twice.
    pub async fn open(storage: Arc<dyn StorageEngine>) -> Result<Self, ClusterError> {
        let record = match storage.get_object(SYSTEM_BUCKET, LEASE_RECORD_KEY).await {
            Ok((bytes, _)) => serde_json::from_slice::<LedgerRecord>(&bytes)
                .map_err(|e| ClusterError::Storage(format!("IAM lease ledger unreadable: {e}")))?,
            Err(StorageError::ObjectNotFound { .. }) => LedgerRecord::default(),
            Err(e) => return Err(ClusterError::Storage(e.to_string())),
        };
        Ok(Self {
            storage,
            ledger: Arc::new(Mutex::new(LeaseLedger::from_record(record))),
            origin: Instant::now(),
        })
    }

    /// Milliseconds of this node's monotonic clock.
    fn now_ms(&self) -> u64 {
        u64::try_from(self.origin.elapsed().as_millis()).unwrap_or(u64::MAX)
    }

    async fn keep(&self, record: &LedgerRecord) -> Result<(), String> {
        let bytes = serde_json::to_vec(record).map_err(|e| e.to_string())?;
        self.storage
            .put_object(
                SYSTEM_BUCKET,
                LEASE_RECORD_KEY,
                &bytes,
                "application/json",
                &HashMap::new(),
            )
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }

    /// The highest epoch this node granted or learned of.
    pub async fn max_epoch(&self) -> u64 {
        self.ledger.lock().await.max_epoch()
    }

    /// The grant this node holds out now for a change of IAM under way, if
    /// any ([`LeaseLedger::live_at`]).
    pub async fn live_grant(&self) -> Option<LiveGrant> {
        let now_ms = self.now_ms();
        self.ledger.lock().await.live_at(now_ms)
    }

    /// What survives this node's restart.
    pub async fn record(&self) -> LedgerRecord {
        self.ledger.lock().await.record()
    }

    /// Asks this node for `epoch` for `holder` ([`LeaseLedger::acquire`]),
    /// with the highest epoch it has seen. A grant is kept before it is
    /// given.
    ///
    /// # Errors
    ///
    /// `Storage` when the grant could not be kept: nothing is granted.
    pub async fn acquire(
        &self,
        holder: NodeId,
        epoch: u64,
        ceiling: HlcTimestamp,
        duration: Duration,
    ) -> Result<(Grant, u64), ClusterError> {
        let duration_ms = u64::try_from(duration.as_millis()).unwrap_or(u64::MAX);
        let mut ledger = self.ledger.lock().await;
        let before = ledger.clone();
        let (grant, changed) = ledger.acquire(holder, epoch, ceiling, duration_ms, self.now_ms());
        if changed {
            if let Err(error) = self.keep(&ledger.record()).await {
                *ledger = before;
                return Err(ClusterError::Storage(format!(
                    "IAM lease grant not kept: {error}"
                )));
            }
        }
        Ok((grant, ledger.max_epoch()))
    }

    /// `holder` releases `epoch` ([`LeaseLedger::release`]). Whether this
    /// node held it for them.
    ///
    /// # Errors
    ///
    /// `Storage` when the release could not be kept; the grant then runs out
    /// by itself.
    pub async fn release(
        &self,
        holder: NodeId,
        epoch: u64,
        closing: HlcTimestamp,
    ) -> Result<bool, ClusterError> {
        let mut ledger = self.ledger.lock().await;
        let before = ledger.clone();
        if !ledger.release(holder, epoch, closing) {
            return Ok(false);
        }
        if let Err(error) = self.keep(&ledger.record()).await {
            *ledger = before;
            return Err(ClusterError::Storage(format!(
                "IAM lease release not kept: {error}"
            )));
        }
        Ok(true)
    }

    /// Up to `max_keys` records of the cluster's control state under
    /// `prefix` after `start_after`, tombstones included, read for a change
    /// made under `fence` ([`LeaseLedger::admit_read`]); whether more follow.
    ///
    /// # Errors
    ///
    /// [`Refusal::Fenced`] when the reader's epoch is behind this node's;
    /// [`Refusal::Storage`] when the records or the epoch could not be read
    /// or kept.
    pub async fn read(
        &self,
        fence: Option<&LeaseFence>,
        prefix: &str,
        start_after: Option<&str>,
        max_keys: usize,
    ) -> Result<(Vec<ClassRecord>, bool), Refusal> {
        if !prefix.starts_with(IAM_V2_KEY_PREFIX) {
            return Err(Refusal::Storage(format!(
                "{prefix} is not a prefix of the cluster's control state"
            )));
        }
        let mut ledger = self.ledger.lock().await;
        let before = ledger.clone();
        match ledger.admit_read(fence) {
            Ok(false) => {}
            Ok(true) => {
                if let Err(error) = self.keep(&ledger.record()).await {
                    *ledger = before;
                    return Err(Refusal::Storage(error));
                }
            }
            Err(reason) => {
                debug!(%reason, "refused a read of IAM made outside the current lease");
                return Err(Refusal::Fenced(reason));
            }
        }
        // Read under the ledger: no write of the reader's epoch or an earlier
        // one is admitted meanwhile.
        let listed = self
            .storage
            .list_objects_with_tombstones(SYSTEM_BUCKET, prefix, start_after, max_keys + 1)
            .await
            .map_err(|e| Refusal::Storage(e.to_string()))?;
        let truncated = listed.len() > max_keys;
        let mut records = Vec::with_capacity(listed.len().min(max_keys));
        for (key, record) in listed.into_iter().take(max_keys) {
            let hlc = HlcTimestamp::of_record(&record);
            if record.is_tombstone() {
                records.push(ClassRecord {
                    key,
                    data: None,
                    hlc,
                    etag: String::new(),
                });
                continue;
            }
            match self.storage.get_object(SYSTEM_BUCKET, &key).await {
                Ok((data, record)) => records.push(ClassRecord {
                    key,
                    data: Some(data),
                    hlc: HlcTimestamp::of_record(&record),
                    etag: record.etag,
                }),
                // Deleted since it was listed, by a repair: the next read sees
                // its tombstone.
                Err(StorageError::ObjectNotFound { .. }) => {}
                Err(e) => return Err(Refusal::Storage(e.to_string())),
            }
        }
        Ok((records, truncated))
    }

    /// Whether this node takes a write of the cluster's control state at
    /// `hlc` made under `fence` ([`LeaseLedger::admit`]). Hold the answer
    /// until the write is in storage.
    ///
    /// # Errors
    ///
    /// [`Refusal`] when the write is not to land here; a fenced one is
    /// counted in `s4_iam_fenced_writes_total`.
    pub async fn admit(
        &self,
        fence: Option<&LeaseFence>,
        hlc: HlcTimestamp,
    ) -> Result<Admitted, Refusal> {
        let mut ledger = self.ledger.clone().lock_owned().await;
        let before = ledger.clone();
        match ledger.admit(fence, hlc, self.now_ms()) {
            Ok(false) => {}
            Ok(true) => {
                if let Err(error) = self.keep(&ledger.record()).await {
                    *ledger = before;
                    return Err(Refusal::Storage(error));
                }
            }
            Err(reason) => {
                crate::cluster_metrics::record_iam_fenced_write();
                warn!(
                    epoch = fence.map(|fence| fence.epoch),
                    holder = ?fence.map(|fence| fence.holder),
                    max_epoch = ledger.max_epoch(),
                    %reason,
                    "refused a write of IAM made outside the current lease"
                );
                return Err(Refusal::Fenced(reason));
            }
        }
        Ok(Admitted { _ledger: ledger })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use s4_core::storage::BitcaskStorageEngine;

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

    fn mark(wall: u64) -> HlcTimestamp {
        HlcTimestamp::from_record(wall, 0, 0)
    }

    /// A restart keeps every epoch and floor, and lets nothing granted before
    /// it write.
    #[tokio::test]
    async fn a_restart_forgets_no_epoch_and_no_grant_outlives_it() {
        let (storage, _dir) = storage().await;
        let keeper = IamLeaseKeeper::open(storage.clone()).await.unwrap();
        let (grant, max) =
            keeper.acquire(NodeId(1), 3, mark(5_000), Duration::from_secs(5)).await.unwrap();
        assert_eq!(grant, Grant::Granted { floor: mark(0) });
        assert_eq!(max, 3);

        let restarted = IamLeaseKeeper::open(storage).await.unwrap();
        assert_eq!(restarted.max_epoch().await, 3);
        let fence = LeaseFence {
            epoch: 3,
            holder: NodeId(1),
            floor: mark(0),
            ceiling: mark(5_000),
        };
        assert!(matches!(
            restarted.admit(Some(&fence), HlcTimestamp::from_record(10, 0, 1)).await,
            Err(Refusal::Fenced(Fenced::Expired))
        ));
        // The epoch is not granted again, and the next one is, above the old ceiling.
        assert_eq!(
            restarted
                .acquire(NodeId(2), 3, mark(9_000), Duration::from_secs(5))
                .await
                .unwrap()
                .0,
            Grant::Stale
        );
        assert_eq!(
            restarted
                .acquire(NodeId(2), 4, mark(9_000), Duration::from_secs(5))
                .await
                .unwrap()
                .0,
            Grant::Granted { floor: mark(5_000) }
        );
    }

    /// A node grants no new epoch while a write it admitted is still landing.
    #[tokio::test]
    async fn no_grant_while_an_admitted_write_lands() {
        let (storage, _dir) = storage().await;
        let keeper = Arc::new(IamLeaseKeeper::open(storage).await.unwrap());
        keeper
            .acquire(NodeId(1), 1, mark(u64::MAX / 2), Duration::from_secs(5))
            .await
            .unwrap();
        let fence = LeaseFence {
            epoch: 1,
            holder: NodeId(1),
            floor: mark(0),
            ceiling: mark(u64::MAX / 2),
        };
        let admitted =
            keeper.admit(Some(&fence), HlcTimestamp::from_record(10, 0, 1)).await.unwrap();
        let asking = {
            let keeper = keeper.clone();
            tokio::spawn(async move {
                keeper.acquire(NodeId(2), 2, mark(9_000), Duration::from_secs(5)).await
            })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!asking.is_finished());
        drop(admitted);
        // Still the first holder's: busy, not granted.
        assert!(matches!(
            asking.await.unwrap().unwrap().0,
            Grant::Busy { .. }
        ));
    }
}
