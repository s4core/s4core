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

//! Mark-sweep GC for per-node deduplication entries (Phase 8).
//!
//! # Algorithm
//!
//! 1. **MARK phase**: Scan the objects keyspace and collect all content
//!    hashes referenced by live (non-tombstone) objects.
//!
//! 2. **SWEEP phase**: Scan the dedup keyspace. For each `DedupEntry`,
//!    if its content hash is NOT in the live set, remove it. The
//!    corresponding blob in the volume becomes dead space for the
//!    compactor (`s4-compactor`).
//!
//! # Safety Properties
//!
//! - **Crash-safe**: mark-sweep does not depend on exact ref-counts.
//!   If the process crashes mid-sweep, the next cycle will re-scan
//!   and correctly identify dead entries.
//! - **Idempotent**: running the same cycle twice is harmless.
//! - **No cross-node coordination**: each node runs its own GC.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tracing::{debug, error, info, warn};

use s4_core::storage::IndexDb;

/// Configuration for the mark-sweep dedup GC.
#[derive(Debug, Clone)]
pub struct DedupGcConfig {
    /// Whether dedup GC is enabled.
    pub enabled: bool,
    /// Interval between mark-sweep cycles.
    /// Default: 24 hours.
    pub interval: Duration,
}

impl Default for DedupGcConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval: Duration::from_secs(24 * 3600),
        }
    }
}

/// Statistics from a single mark-sweep cycle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SweepStats {
    /// Number of live content hashes found in the MARK phase.
    pub live_hashes: u64,
    /// Number of dedup entries scanned in the SWEEP phase.
    pub dedup_entries_scanned: u64,
    /// Number of orphaned dedup entries removed.
    pub orphans_removed: u64,
}

/// Metrics for the dedup GC service.
#[derive(Debug, Default)]
pub struct DedupGcMetrics {
    /// Total sweep cycles completed.
    pub cycles_completed: AtomicU64,
    /// Total orphans removed across all cycles.
    pub total_orphans_removed: AtomicU64,
    /// Total errors encountered.
    pub errors: AtomicU64,
}

/// Background mark-sweep GC for per-node dedup entries.
pub struct DistributedDedupGc {
    config: DedupGcConfig,
    index_db: Arc<IndexDb>,
    metrics: Arc<DedupGcMetrics>,
}

impl DistributedDedupGc {
    /// Create a new dedup GC service.
    pub fn new(config: DedupGcConfig, index_db: Arc<IndexDb>) -> Self {
        Self {
            config,
            index_db,
            metrics: Arc::new(DedupGcMetrics::default()),
        }
    }

    /// Returns a reference to the dedup GC metrics.
    pub fn metrics(&self) -> &Arc<DedupGcMetrics> {
        &self.metrics
    }

    /// Run the dedup GC service loop until shutdown is signaled.
    pub async fn run(&self, mut shutdown: watch::Receiver<bool>) {
        if !self.config.enabled {
            info!("dedup GC service is disabled, exiting");
            return;
        }

        info!(
            interval_hours = self.config.interval.as_secs() / 3600,
            "dedup GC service started"
        );

        loop {
            tokio::select! {
                _ = tokio::time::sleep(self.config.interval) => {
                    match self.run_sweep_cycle().await {
                        Ok(stats) => {
                            info!(
                                live_hashes = stats.live_hashes,
                                scanned = stats.dedup_entries_scanned,
                                orphans_removed = stats.orphans_removed,
                                "dedup GC sweep cycle completed"
                            );
                        }
                        Err(e) => {
                            error!(error = %e, "dedup GC sweep cycle failed");
                            self.metrics.errors.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        info!("dedup GC service shutting down");
                        break;
                    }
                }
            }
        }
    }

    /// Run a single mark-sweep cycle.
    pub async fn run_sweep_cycle(&self) -> Result<SweepStats, String> {
        // MARK phase: collect all live content hashes from objects keyspace
        let live_hashes = self
            .index_db
            .scan_live_content_hashes()
            .await
            .map_err(|e| format!("mark phase failed: {e}"))?;

        let live_count = live_hashes.len() as u64;
        debug!(live_hashes = live_count, "mark phase complete");

        // SWEEP phase: scan dedup keyspace, remove orphaned entries
        let dedup_ks = self.index_db.dedup_keyspace();
        let dedup_ks_clone = dedup_ks.clone();

        let (scanned, orphan_keys) = tokio::task::spawn_blocking(move || {
            let mut scanned = 0u64;
            let mut orphan_keys: Vec<Vec<u8>> = Vec::new();

            for guard in dedup_ks_clone.iter() {
                let (key_bytes, _value_bytes) = match guard.into_inner() {
                    Ok(kv) => kv,
                    Err(e) => {
                        warn!(error = %e, "failed to read dedup entry during sweep");
                        continue;
                    }
                };

                scanned += 1;

                // Dedup keys are 32-byte SHA-256 hashes (possibly with a 4-byte PG prefix)
                let hash_key: [u8; 32] = if key_bytes.len() == 32 {
                    let mut arr = [0u8; 32];
                    arr.copy_from_slice(&key_bytes);
                    arr
                } else if key_bytes.len() == 36 {
                    // 4-byte PG prefix + 32-byte hash
                    let mut arr = [0u8; 32];
                    arr.copy_from_slice(&key_bytes[4..]);
                    arr
                } else {
                    // Unknown key format, skip
                    continue;
                };

                if !live_hashes.contains(&hash_key) {
                    orphan_keys.push(key_bytes.to_vec());
                }
            }

            (scanned, orphan_keys)
        })
        .await
        .map_err(|e| format!("sweep phase join error: {e}"))?;

        // Delete orphaned entries
        let mut removed = 0u64;
        for key in &orphan_keys {
            match tokio::task::spawn_blocking({
                let ks = dedup_ks.clone();
                let key = key.clone();
                move || ks.remove(&key[..])
            })
            .await
            {
                Ok(Ok(())) => {
                    removed += 1;
                }
                Ok(Err(e)) => {
                    warn!(error = %e, "failed to remove orphaned dedup entry");
                }
                Err(e) => {
                    warn!(error = %e, "task join error during orphan removal");
                }
            }
        }

        self.metrics.total_orphans_removed.fetch_add(removed, Ordering::Relaxed);
        self.metrics.cycles_completed.fetch_add(1, Ordering::Relaxed);

        Ok(SweepStats {
            live_hashes: live_count,
            dedup_entries_scanned: scanned,
            orphans_removed: removed,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use s4_core::storage::index::{BatchAction, BatchOp, KeyspaceId};
    use s4_core::types::IndexRecord;
    use tempfile::TempDir;

    async fn setup_test_db() -> (TempDir, Arc<IndexDb>) {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_dedup_gc_db");
        let index_db = Arc::new(IndexDb::new(&db_path).unwrap());
        (temp_dir, index_db)
    }

    #[tokio::test]
    async fn sweep_with_no_data() {
        let (_dir, db) = setup_test_db().await;
        let gc = DistributedDedupGc::new(DedupGcConfig::default(), db);

        let stats = gc.run_sweep_cycle().await.unwrap();
        assert_eq!(stats.live_hashes, 0);
        assert_eq!(stats.dedup_entries_scanned, 0);
        assert_eq!(stats.orphans_removed, 0);
    }

    #[tokio::test]
    async fn sweep_keeps_referenced_dedup_entries() {
        let (_dir, db) = setup_test_db().await;

        let content_hash = [42u8; 32];

        // Insert a live object referencing this hash
        let record = IndexRecord::new(
            1,
            0,
            100,
            content_hash,
            "etag".to_string(),
            "text/plain".to_string(),
        );
        db.put("bucket/live-obj", &record).await.unwrap();

        // Insert a dedup entry for the same hash (32-byte key)
        let dedup_value = bincode::serialize(&s4_core::storage::dedup::DedupEntry {
            volume_id: 1,
            offset: 0,
            ref_count: 1,
        })
        .unwrap();

        db.batch_write(vec![BatchOp {
            keyspace: KeyspaceId::Dedup,
            action: BatchAction::Put(content_hash.to_vec(), dedup_value),
        }])
        .await
        .unwrap();

        let gc = DistributedDedupGc::new(DedupGcConfig::default(), db);
        let stats = gc.run_sweep_cycle().await.unwrap();

        assert_eq!(stats.live_hashes, 1);
        assert_eq!(stats.dedup_entries_scanned, 1);
        assert_eq!(stats.orphans_removed, 0);
    }

    #[tokio::test]
    async fn sweep_removes_orphaned_dedup_entries() {
        let (_dir, db) = setup_test_db().await;

        let orphan_hash = [99u8; 32];

        // Insert a dedup entry with NO corresponding live object
        let dedup_value = bincode::serialize(&s4_core::storage::dedup::DedupEntry {
            volume_id: 1,
            offset: 0,
            ref_count: 1,
        })
        .unwrap();

        db.batch_write(vec![BatchOp {
            keyspace: KeyspaceId::Dedup,
            action: BatchAction::Put(orphan_hash.to_vec(), dedup_value),
        }])
        .await
        .unwrap();

        let gc = DistributedDedupGc::new(DedupGcConfig::default(), db);
        let stats = gc.run_sweep_cycle().await.unwrap();

        assert_eq!(stats.live_hashes, 0);
        assert_eq!(stats.dedup_entries_scanned, 1);
        assert_eq!(stats.orphans_removed, 1);
    }

    #[tokio::test]
    async fn sweep_mixed_live_and_orphan() {
        let (_dir, db) = setup_test_db().await;

        let live_hash = [10u8; 32];
        let orphan_hash = [20u8; 32];

        // Live object
        let record = IndexRecord::new(
            1,
            0,
            100,
            live_hash,
            "etag".to_string(),
            "text/plain".to_string(),
        );
        db.put("bucket/live", &record).await.unwrap();

        // Dedup entry for live hash
        let dedup_value_live = bincode::serialize(&s4_core::storage::dedup::DedupEntry {
            volume_id: 1,
            offset: 0,
            ref_count: 1,
        })
        .unwrap();
        db.batch_write(vec![BatchOp {
            keyspace: KeyspaceId::Dedup,
            action: BatchAction::Put(live_hash.to_vec(), dedup_value_live),
        }])
        .await
        .unwrap();

        // Dedup entry for orphan hash (no live object references it)
        let dedup_value_orphan = bincode::serialize(&s4_core::storage::dedup::DedupEntry {
            volume_id: 2,
            offset: 500,
            ref_count: 1,
        })
        .unwrap();
        db.batch_write(vec![BatchOp {
            keyspace: KeyspaceId::Dedup,
            action: BatchAction::Put(orphan_hash.to_vec(), dedup_value_orphan),
        }])
        .await
        .unwrap();

        let gc = DistributedDedupGc::new(DedupGcConfig::default(), db);
        let stats = gc.run_sweep_cycle().await.unwrap();

        assert_eq!(stats.live_hashes, 1);
        assert_eq!(stats.dedup_entries_scanned, 2);
        assert_eq!(stats.orphans_removed, 1);
    }

    #[tokio::test]
    async fn metrics_accumulate() {
        let (_dir, db) = setup_test_db().await;
        let gc = DistributedDedupGc::new(DedupGcConfig::default(), db);

        gc.run_sweep_cycle().await.unwrap();
        gc.run_sweep_cycle().await.unwrap();

        assert_eq!(gc.metrics().cycles_completed.load(Ordering::Relaxed), 2);
    }
}
