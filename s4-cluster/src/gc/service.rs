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

//! Background distributed GC service (Phase 8).
//!
//! Runs periodic tombstone purge cycles. Each cycle:
//!
//! 1. Scans the objects keyspace for tombstones (delete markers)
//! 2. For each tombstone, checks:
//!    - Age ≥ `gc_grace` (tombstone is old enough)
//!    - Repair frontier confirms ALL replicas have seen it
//! 3. Purges eligible tombstones from the index
//!
//! # Design Notes
//!
//! - fjall does not support custom compaction filters, so tombstone purge
//!   is a separate background process (scan → check → delete eligible).
//! - Distributed safety is achieved through `gc_grace + repair frontier`,
//!   not through synchronized compaction between nodes.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use tokio::sync::{watch, RwLock};
use tracing::{debug, error, info, warn};

use crate::clock::hlc;
use crate::gc::tombstone::{can_purge_tombstone, GcConfig};
use crate::identity::NodeId;
use crate::repair::frontier::RepairFrontier;
use crate::repair::merkle::KeyRange;
use s4_core::storage::IndexDb;

/// Metrics emitted by the GC service.
#[derive(Debug, Default)]
pub struct GcMetrics {
    /// Total number of purge cycles completed.
    pub cycles_completed: AtomicU64,
    /// Total number of tombstones scanned across all cycles.
    pub tombstones_scanned: AtomicU64,
    /// Total number of tombstones successfully purged.
    pub tombstones_purged: AtomicU64,
    /// Number of tombstones currently active (last scan count).
    pub tombstones_active: AtomicU64,
    /// Number of purge errors encountered.
    pub purge_errors: AtomicU64,
}

/// Background service for distributed tombstone garbage collection.
///
/// The service scans for tombstones periodically and purges those that
/// satisfy both the age and repair frontier conditions.
pub struct DistributedGcService {
    config: GcConfig,
    index_db: Arc<IndexDb>,
    repair_frontier: Arc<RwLock<RepairFrontier>>,
    pool_replicas: Arc<RwLock<Vec<NodeId>>>,
    metrics: Arc<GcMetrics>,
    running: AtomicBool,
}

impl DistributedGcService {
    /// Create a new distributed GC service.
    ///
    /// # Arguments
    ///
    /// * `config` — GC configuration (interval, tombstone policy)
    /// * `index_db` — the metadata index database to scan and purge from
    /// * `repair_frontier` — shared repair frontier updated by anti-entropy
    /// * `pool_replicas` — the set of replica node IDs in this pool
    pub fn new(
        config: GcConfig,
        index_db: Arc<IndexDb>,
        repair_frontier: Arc<RwLock<RepairFrontier>>,
        pool_replicas: Arc<RwLock<Vec<NodeId>>>,
    ) -> Self {
        Self {
            config,
            index_db,
            repair_frontier,
            pool_replicas,
            metrics: Arc::new(GcMetrics::default()),
            running: AtomicBool::new(false),
        }
    }

    /// Returns a reference to the GC metrics.
    pub fn metrics(&self) -> &Arc<GcMetrics> {
        &self.metrics
    }

    /// Run the GC service loop until shutdown is signaled.
    ///
    /// The service runs a purge cycle every `config.interval`, then sleeps.
    /// It gracefully stops when `shutdown` receives `true`.
    pub async fn run(&self, mut shutdown: watch::Receiver<bool>) {
        if !self.config.enabled {
            info!("distributed GC service is disabled, exiting");
            return;
        }

        if let Err(e) = self.config.validate() {
            error!(error = %e, "invalid GC config, service will not start");
            return;
        }

        self.running.store(true, Ordering::SeqCst);
        info!(
            interval_secs = self.config.interval.as_secs(),
            gc_grace_days = self.config.tombstone_policy.gc_grace.as_secs() / 86400,
            "distributed GC service started"
        );

        loop {
            tokio::select! {
                _ = tokio::time::sleep(self.config.interval) => {
                    match self.run_purge_cycle().await {
                        Ok(stats) => {
                            info!(
                                scanned = stats.scanned,
                                purged = stats.purged,
                                remaining = stats.remaining,
                                "GC purge cycle completed"
                            );
                        }
                        Err(e) => {
                            error!(error = %e, "GC purge cycle failed");
                            self.metrics.purge_errors.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        info!("distributed GC service shutting down");
                        break;
                    }
                }
            }
        }

        self.running.store(false, Ordering::SeqCst);
    }

    /// Run a single purge cycle.
    ///
    /// Returns statistics about the cycle for logging and metrics.
    pub async fn run_purge_cycle(&self) -> Result<PurgeCycleStats, String> {
        let now = hlc::now_millis();
        let gc_grace = self.config.tombstone_policy.gc_grace;

        // Scan all tombstones
        let tombstones = self
            .index_db
            .scan_tombstones()
            .await
            .map_err(|e| format!("tombstone scan failed: {e}"))?;

        let scanned = tombstones.len() as u64;
        self.metrics.tombstones_scanned.fetch_add(scanned, Ordering::Relaxed);

        let frontier = self.repair_frontier.read().await;
        let replicas = self.pool_replicas.read().await;

        let mut purged = 0u64;
        let mut keys_to_purge = Vec::new();

        for (key, record) in &tombstones {
            // Derive the key range from the record's position.
            // Using a coarse range (0, u64::MAX) as a conservative check —
            // in production the placement router would provide exact ranges.
            let key_range = KeyRange::new(0, u64::MAX);

            if can_purge_tombstone(
                record.hlc_timestamp,
                record.hlc_logical,
                now,
                gc_grace,
                &frontier,
                &key_range,
                &replicas,
            ) {
                keys_to_purge.push(key.clone());
            }
        }

        // Release locks before performing deletes
        drop(frontier);
        drop(replicas);

        // Purge eligible tombstones
        for key in &keys_to_purge {
            match self.index_db.delete(key).await {
                Ok(()) => {
                    purged += 1;
                    debug!(key = %key, "tombstone purged");
                }
                Err(e) => {
                    warn!(key = %key, error = %e, "failed to purge tombstone");
                    self.metrics.purge_errors.fetch_add(1, Ordering::Relaxed);
                }
            }
        }

        let remaining = scanned.saturating_sub(purged);

        self.metrics.tombstones_purged.fetch_add(purged, Ordering::Relaxed);
        self.metrics.tombstones_active.store(remaining, Ordering::Relaxed);
        self.metrics.cycles_completed.fetch_add(1, Ordering::Relaxed);

        Ok(PurgeCycleStats {
            scanned,
            purged,
            remaining,
        })
    }

    /// Check if the service loop is currently running.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }
}

/// Statistics from a single GC purge cycle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PurgeCycleStats {
    /// Number of tombstones scanned.
    pub scanned: u64,
    /// Number of tombstones purged.
    pub purged: u64,
    /// Number of tombstones remaining (not yet eligible).
    pub remaining: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use s4_core::types::IndexRecord;
    use std::time::Duration;
    use tempfile::TempDir;

    async fn setup_test_db() -> (TempDir, Arc<IndexDb>) {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_gc_db");
        let index_db = Arc::new(IndexDb::new(&db_path).unwrap());
        (temp_dir, index_db)
    }

    fn make_tombstone(hlc_wall: u64) -> IndexRecord {
        let mut record = IndexRecord::new_delete_marker("v1".to_string());
        record.hlc_timestamp = hlc_wall;
        record.hlc_logical = 0;
        record
    }

    fn make_gc_config(gc_grace_secs: u64) -> GcConfig {
        GcConfig {
            enabled: true,
            interval: Duration::from_secs(1),
            tombstone_policy: crate::gc::tombstone::TombstonePolicy {
                gc_grace: Duration::from_secs(gc_grace_secs),
                max_rejoin_downtime: Duration::from_secs(1),
            },
        }
    }

    #[tokio::test]
    async fn purge_cycle_with_no_tombstones() {
        let (_dir, db) = setup_test_db().await;
        let frontier = Arc::new(RwLock::new(RepairFrontier::new()));
        let replicas = Arc::new(RwLock::new(vec![NodeId(1)]));

        let service = DistributedGcService::new(make_gc_config(1), db, frontier, replicas);

        let stats = service.run_purge_cycle().await.unwrap();
        assert_eq!(stats.scanned, 0);
        assert_eq!(stats.purged, 0);
        assert_eq!(stats.remaining, 0);
    }

    #[tokio::test]
    async fn purge_cycle_skips_young_tombstones() {
        let (_dir, db) = setup_test_db().await;

        // Insert a tombstone with a very recent HLC timestamp
        let now_ms = hlc::now_millis();
        let tombstone = make_tombstone(now_ms);
        db.put("bucket/deleted-obj", &tombstone).await.unwrap();

        let frontier = Arc::new(RwLock::new(RepairFrontier::new()));
        let replicas = Arc::new(RwLock::new(vec![NodeId(1)]));

        let service = DistributedGcService::new(
            make_gc_config(3600), // 1 hour grace
            db,
            frontier,
            replicas,
        );

        let stats = service.run_purge_cycle().await.unwrap();
        assert_eq!(stats.scanned, 1);
        assert_eq!(stats.purged, 0);
        assert_eq!(stats.remaining, 1);
    }

    #[tokio::test]
    async fn purge_cycle_purges_old_tombstone_with_frontier() {
        let (_dir, db) = setup_test_db().await;

        // Insert a very old tombstone
        let old_hlc = 1000u64; // ancient
        let tombstone = make_tombstone(old_hlc);
        db.put("bucket/ancient-obj", &tombstone).await.unwrap();

        // Set up frontier: all replicas confirmed past the tombstone
        let mut frontier = RepairFrontier::new();
        let key_range = KeyRange::new(0, u64::MAX);
        let n1 = NodeId(1);
        let past = crate::clock::hlc::HlcTimestamp {
            wall_time: hlc::now_millis(),
            logical: 0,
            node_id: 0,
        };
        frontier.advance(&key_range, n1, past);

        let frontier = Arc::new(RwLock::new(frontier));
        let replicas = Arc::new(RwLock::new(vec![n1]));

        let service = DistributedGcService::new(
            make_gc_config(1), // 1 second grace (tombstone is way older)
            db.clone(),
            frontier,
            replicas,
        );

        let stats = service.run_purge_cycle().await.unwrap();
        assert_eq!(stats.scanned, 1);
        assert_eq!(stats.purged, 1);
        assert_eq!(stats.remaining, 0);

        // Verify tombstone is actually deleted
        let result = db.get("bucket/ancient-obj").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn purge_cycle_does_not_purge_live_objects() {
        let (_dir, db) = setup_test_db().await;

        // Insert a live object (not a tombstone)
        let record = IndexRecord::new(
            1,
            0,
            100,
            [1u8; 32],
            "etag".to_string(),
            "text/plain".to_string(),
        );
        db.put("bucket/live-obj", &record).await.unwrap();

        // Insert a tombstone
        let tombstone = make_tombstone(1000);
        db.put("bucket/dead-obj", &tombstone).await.unwrap();

        let mut frontier = RepairFrontier::new();
        let key_range = KeyRange::new(0, u64::MAX);
        let n1 = NodeId(1);
        let past = crate::clock::hlc::HlcTimestamp {
            wall_time: hlc::now_millis(),
            logical: 0,
            node_id: 0,
        };
        frontier.advance(&key_range, n1, past);

        let frontier = Arc::new(RwLock::new(frontier));
        let replicas = Arc::new(RwLock::new(vec![n1]));

        let service = DistributedGcService::new(make_gc_config(1), db.clone(), frontier, replicas);

        let stats = service.run_purge_cycle().await.unwrap();
        assert_eq!(stats.scanned, 1); // only the tombstone was scanned
        assert_eq!(stats.purged, 1);

        // Live object still exists
        let live = db.get("bucket/live-obj").await.unwrap();
        assert!(live.is_some());
        assert!(!live.unwrap().is_delete_marker);
    }

    #[tokio::test]
    async fn metrics_accumulate_across_cycles() {
        let (_dir, db) = setup_test_db().await;
        let frontier = Arc::new(RwLock::new(RepairFrontier::new()));
        let replicas = Arc::new(RwLock::new(vec![NodeId(1)]));

        let service = DistributedGcService::new(make_gc_config(1), db, frontier, replicas);

        service.run_purge_cycle().await.unwrap();
        service.run_purge_cycle().await.unwrap();

        assert_eq!(
            service.metrics().cycles_completed.load(Ordering::Relaxed),
            2
        );
    }
}
