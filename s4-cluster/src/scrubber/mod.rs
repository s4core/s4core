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

//! Background Data Scrubber — CRC32 integrity verification with replica healing (Phase 9).
//!
//! The scrubber periodically walks every blob in every volume file,
//! recomputes the CRC32 checksum, and compares it to the checksum stored
//! in the [`BlobHeader`](s4_core::types::BlobHeader).
//!
//! - **Corruption detected**: logs a warning, increments metrics, and
//!   attempts to heal the blob from a healthy replica (cluster mode only).
//! - **Single-node mode**: alerts on corruption but cannot heal (no replicas).
//! - **I/O throttling**: configurable inter-blob sleep prevents disk saturation.
//!
//! # CE/EE Boundary
//!
//! The CRC32 background scrubber is a **CE feature** — available in all editions.
//! The deep SHA-256 scrubber (see [`deep_verify`]) is a utility whose background
//! scheduling is gated by the [`DeepScrubber`](crate::traits::DeepScrubber) EE trait.

pub mod deep_verify;
pub mod metrics;

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tracing::{debug, error, info, warn};

use sha2::Digest;

use crate::error::ClusterError;
use crate::gossip::GossipHandle;
use crate::identity::NodeId;
use crate::identity::NodeStatus;
use crate::placement::PoolLayout;
use crate::rpc::NodeClient;
use metrics::ScrubberMetrics;
use s4_core::storage::{IndexDb, KeyspaceId, VolumeReader};
use s4_core::StorageEngine;

/// Configuration for the background data scrubber.
#[derive(Debug, Clone)]
pub struct ScrubberConfig {
    /// Whether the scrubber is enabled.
    pub enabled: bool,
    /// Target duration for one complete scan of all volumes (default: 30 days).
    pub full_scan_period: Duration,
    /// Sleep between individual blob checks to throttle I/O (default: 100us).
    pub throttle_delay: Duration,
    /// Sleep between volume scans within a cycle (default: 1s).
    pub inter_volume_delay: Duration,
    /// How long the first cycle waits after start (default: 30s).
    ///
    /// Healing needs peers, and a node that has just started has not finished
    /// hearing about them. A scan that runs first would find a damaged blob,
    /// fail to reach anybody who has a healthy copy and write the damage off —
    /// for a whole `full_scan_period`.
    pub start_delay: Duration,
    /// How long until the next cycle when this one left damage unhealed
    /// (default: 5 minutes).
    ///
    /// A copy that could not be fetched now — the peer was busy, restarting,
    /// or briefly unreachable — is worth asking for again long before the next
    /// full scan is due. A cycle that healed everything it found goes back to
    /// the full scan period.
    pub unhealed_retry_period: Duration,
}

impl Default for ScrubberConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            full_scan_period: Duration::from_secs(30 * 24 * 3600), // 30 days
            throttle_delay: Duration::from_micros(100),
            inter_volume_delay: Duration::from_secs(1),
            start_delay: Duration::from_secs(30),
            unhealed_retry_period: Duration::from_secs(300),
        }
    }
}

/// Context for replica-based healing (only available in cluster mode).
pub struct HealingContext {
    /// This node's ID.
    pub node_id: NodeId,
    /// gRPC client for fetching blobs from peer replicas.
    pub rpc_client: Arc<NodeClient>,
    /// Gossip handle for discovering live nodes.
    pub gossip: Arc<GossipHandle>,
    /// The pool's placement, for finding the replica set of a key by its
    /// replication class.
    pub placement: Arc<PoolLayout>,
    /// Local storage engine, where a healthy copy fetched from a replica is
    /// written back. Without it a scrubber can only report damage.
    pub storage: Arc<dyn StorageEngine>,
}

/// Statistics from a single scrub cycle.
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct ScrubCycleStats {
    /// Blobs checked in this cycle.
    pub blobs_checked: u64,
    /// CRC32 corruptions found in this cycle.
    pub corruptions_found: u64,
    /// Corruptions healed from replicas.
    pub corruptions_healed: u64,
    /// Corruptions that could not be healed.
    pub corruptions_unrecoverable: u64,
    /// Volumes scanned.
    pub volumes_scanned: u64,
    /// Bytes scanned.
    pub bytes_scanned: u64,
}

/// Background CRC32 data scrubber.
///
/// Periodically walks all volume files on this node and verifies CRC32
/// checksums. In cluster mode, corrupted blobs are healed from replicas.
/// In single-node mode, corruption is logged but cannot be repaired.
pub struct DataScrubber {
    config: ScrubberConfig,
    volumes_dir: PathBuf,
    index_db: Arc<IndexDb>,
    healing: Option<HealingContext>,
    metrics: Arc<ScrubberMetrics>,
    running: AtomicBool,
}

impl DataScrubber {
    /// Create a scrubber for single-node mode (no healing).
    pub fn new_standalone(
        config: ScrubberConfig,
        volumes_dir: impl Into<PathBuf>,
        index_db: Arc<IndexDb>,
    ) -> Self {
        Self {
            config,
            volumes_dir: volumes_dir.into(),
            index_db,
            healing: None,
            metrics: Arc::new(ScrubberMetrics::default()),
            running: AtomicBool::new(false),
        }
    }

    /// Create a scrubber for cluster mode (with replica healing).
    pub fn new_cluster(
        config: ScrubberConfig,
        volumes_dir: impl Into<PathBuf>,
        index_db: Arc<IndexDb>,
        healing: HealingContext,
    ) -> Self {
        Self {
            config,
            volumes_dir: volumes_dir.into(),
            index_db,
            healing: Some(healing),
            metrics: Arc::new(ScrubberMetrics::default()),
            running: AtomicBool::new(false),
        }
    }

    /// Returns a reference to the scrubber metrics.
    pub fn metrics(&self) -> &Arc<ScrubberMetrics> {
        &self.metrics
    }

    /// Returns `true` if the scrubber loop is currently running.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Run the scrubber loop until `shutdown` signals.
    pub async fn run(&self, mut shutdown: watch::Receiver<bool>) {
        if !self.config.enabled {
            info!("data scrubber is disabled, exiting");
            return;
        }

        self.running.store(true, Ordering::SeqCst);

        let mode = if self.healing.is_some() {
            "cluster (healing enabled)"
        } else {
            "standalone (alert-only)"
        };
        info!(
            mode,
            full_scan_days = self.config.full_scan_period.as_secs() / 86400,
            "data scrubber started"
        );

        // Let membership settle before the first cycle: see `start_delay`.
        if !self.config.start_delay.is_zero() {
            tokio::select! {
                _ = tokio::time::sleep(self.config.start_delay) => {}
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        self.running.store(false, Ordering::SeqCst);
                        return;
                    }
                }
            }
        }

        loop {
            // How long until the next cycle: set by what this one finds.
            let next_cycle_in;

            // Run a full scan cycle
            match self.run_scrub_cycle(&mut shutdown).await {
                Ok(Some(stats)) => {
                    let ts = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs();
                    self.metrics.last_full_scan_timestamp.store(ts, Ordering::Relaxed);
                    self.metrics.cycles_completed.fetch_add(1, Ordering::Relaxed);
                    self.metrics.scan_progress_pct.store(100, Ordering::Relaxed);
                    crate::cluster_metrics::set_scrubber_scan_progress(1.0);

                    next_cycle_in = self.inter_cycle_delay(&stats);

                    info!(
                        blobs_checked = stats.blobs_checked,
                        corruptions_found = stats.corruptions_found,
                        corruptions_healed = stats.corruptions_healed,
                        corruptions_unrecoverable = stats.corruptions_unrecoverable,
                        volumes_scanned = stats.volumes_scanned,
                        next_cycle_secs = next_cycle_in.as_secs(),
                        "scrub cycle completed"
                    );
                }
                Ok(None) => {
                    // Shutdown requested during scan
                    break;
                }
                Err(e) => {
                    next_cycle_in = self.config.unhealed_retry_period;
                    error!(error = %e, "scrub cycle failed");
                }
            }

            // Sleep until next cycle (interruptible by shutdown)
            tokio::select! {
                _ = tokio::time::sleep(next_cycle_in) => {}
                _ = shutdown.changed() => {
                    if *shutdown.borrow() { break; }
                }
            }
        }

        self.running.store(false, Ordering::SeqCst);
        info!("data scrubber stopped");
    }

    /// Run a single full scrub cycle. Returns `None` if shutdown was requested.
    pub async fn run_scrub_cycle(
        &self,
        shutdown: &mut watch::Receiver<bool>,
    ) -> Result<Option<ScrubCycleStats>, ClusterError> {
        let volume_ids = self.list_volume_ids().await?;
        if volume_ids.is_empty() {
            // A node with nothing stored yet has no volumes, and that is
            // ordinary. A directory that is not there at all is not: it means
            // this scrubber is looking somewhere the data is not, and would go
            // on reporting a clean pool forever.
            if !self.volumes_dir.exists() {
                warn!(
                    volumes_dir = %self.volumes_dir.display(),
                    "scrubber found no volumes directory; nothing on this node is being checked"
                );
            } else {
                debug!(volumes_dir = %self.volumes_dir.display(), "scrubber found no volumes yet");
            }
            return Ok(Some(ScrubCycleStats::default()));
        }

        let reader = VolumeReader::new(&self.volumes_dir);
        let total_volumes = volume_ids.len() as u64;
        let mut stats = ScrubCycleStats::default();

        for (vol_idx, volume_id) in volume_ids.iter().enumerate() {
            // Update progress
            let progress = (vol_idx as u64 * 100) / total_volumes;
            self.metrics.scan_progress_pct.store(progress, Ordering::Relaxed);
            crate::cluster_metrics::set_scrubber_scan_progress(progress as f64 / 100.0);

            match self.scrub_volume(&reader, *volume_id, shutdown).await {
                Ok(Some(vol_stats)) => {
                    stats.blobs_checked += vol_stats.blobs_checked;
                    stats.corruptions_found += vol_stats.corruptions_found;
                    stats.corruptions_healed += vol_stats.corruptions_healed;
                    stats.corruptions_unrecoverable += vol_stats.corruptions_unrecoverable;
                    stats.bytes_scanned += vol_stats.bytes_scanned;
                    stats.volumes_scanned += 1;

                    self.metrics.volumes_scanned_total.fetch_add(1, Ordering::Relaxed);
                }
                Ok(None) => return Ok(None), // shutdown
                Err(e) => {
                    warn!(volume_id, error = %e, "failed to scrub volume, skipping");
                }
            }

            // Inter-volume delay (interruptible)
            tokio::select! {
                _ = tokio::time::sleep(self.config.inter_volume_delay) => {}
                _ = shutdown.changed() => {
                    if *shutdown.borrow() { return Ok(None); }
                }
            }
        }

        Ok(Some(stats))
    }

    /// Scrub a single volume. Returns `None` on shutdown.
    async fn scrub_volume(
        &self,
        reader: &VolumeReader,
        volume_id: u32,
        shutdown: &mut watch::Receiver<bool>,
    ) -> Result<Option<ScrubCycleStats>, ClusterError> {
        let records = self
            .index_db
            .scan_objects_by_volume(volume_id)
            .await
            .map_err(|e| ClusterError::Storage(format!("scan_objects_by_volume: {e}")))?;

        let mut stats = ScrubCycleStats::default();

        for (keyspace_id, key, record) in &records {
            // Check shutdown
            if shutdown.has_changed().unwrap_or(false) && *shutdown.borrow() {
                return Ok(None);
            }

            // Skip delete markers and inline objects
            if record.is_delete_marker || record.file_id == u32::MAX {
                continue;
            }

            stats.blobs_checked += 1;
            self.metrics.blobs_scanned_total.fetch_add(1, Ordering::Relaxed);
            crate::cluster_metrics::record_scrubber_blobs_scanned(1);

            match reader.read_blob(record.file_id, record.offset).await {
                Ok((header, _read_key, data)) => {
                    stats.bytes_scanned += data.len() as u64;
                    self.metrics
                        .bytes_scanned_total
                        .fetch_add(data.len() as u64, Ordering::Relaxed);

                    let actual_crc = crc32fast::hash(&data);
                    if actual_crc != header.crc {
                        stats.corruptions_found += 1;
                        self.metrics.corruptions_found_total.fetch_add(1, Ordering::Relaxed);
                        crate::cluster_metrics::record_scrubber_corruption_found();

                        warn!(
                            key = %key,
                            volume_id,
                            offset = record.offset,
                            expected_crc = header.crc,
                            actual_crc,
                            "bit rot detected"
                        );

                        // Attempt healing in cluster mode. Only objects: the
                        // other records this scan meets — IAM, bucket markers —
                        // have no replica to fetch a copy from, and reporting a
                        // heal for them would be reporting work nobody did.
                        if self.healing.is_some() && *keyspace_id == KeyspaceId::Objects {
                            match self.heal_from_replica(key, record).await {
                                Ok(true) => {
                                    stats.corruptions_healed += 1;
                                    self.metrics
                                        .corruptions_healed_total
                                        .fetch_add(1, Ordering::Relaxed);
                                    crate::cluster_metrics::record_scrubber_corruption_healed();
                                    info!(key = %key, "bit rot healed: the bytes are back on this node");
                                }
                                Ok(false) => {
                                    stats.corruptions_unrecoverable += 1;
                                    self.metrics
                                        .corruptions_unrecoverable_total
                                        .fetch_add(1, Ordering::Relaxed);
                                    error!(key = %key, "bit rot unrecoverable: no healthy replica");
                                }
                                Err(e) => {
                                    stats.corruptions_unrecoverable += 1;
                                    self.metrics
                                        .corruptions_unrecoverable_total
                                        .fetch_add(1, Ordering::Relaxed);
                                    error!(key = %key, error = %e, "healing failed");
                                }
                            }
                        } else {
                            stats.corruptions_unrecoverable += 1;
                            self.metrics
                                .corruptions_unrecoverable_total
                                .fetch_add(1, Ordering::Relaxed);
                            warn!(
                                key = %key,
                                "damaged record cannot be healed from a replica \
                                 (single-node mode, or a record that is not an object)"
                            );
                        }
                    }
                }
                Err(e) => {
                    debug!(key = %key, volume_id, error = %e, "failed to read blob, skipping");
                }
            }

            // Throttle I/O
            tokio::time::sleep(self.config.throttle_delay).await;
        }

        Ok(Some(stats))
    }

    /// Attempt to heal a corrupted blob by fetching it from a healthy replica.
    ///
    /// Returns `Ok(true)` if healed, `Ok(false)` if no healthy replica found.
    async fn heal_from_replica(
        &self,
        key: &str,
        record: &s4_core::types::IndexRecord,
    ) -> Result<bool, ClusterError> {
        let ctx = self
            .healing
            .as_ref()
            .ok_or_else(|| ClusterError::Storage("healing context not available".into()))?;

        let (bucket, object_key) = object_reference(key, record.version_id.as_deref())?;

        // Find replicas for this key
        let replicas = ctx.placement.replicas_of_full_key(key);

        for replica_id in &replicas {
            if *replica_id == ctx.node_id {
                continue; // skip self
            }

            // Check if replica is alive via gossip
            if let Some(state) = ctx.gossip.get_node(replica_id) {
                if !matches!(state.status, NodeStatus::Alive) {
                    continue;
                }
            } else {
                continue; // unknown node
            }

            // Request the blob from the replica
            let request = crate::rpc::proto::ReadBlobRequest {
                bucket: bucket.to_string(),
                key: object_key.to_string(),
                version_id: record.version_id.clone(),
                coordinator_id: ctx.node_id.to_bytes().to_vec(),
                operation_id: uuid::Uuid::new_v4().as_bytes().to_vec(),
                topology_epoch: 0,
                range_start: None,
                range_end: None,
            };

            match ctx.rpc_client.send_read(*replica_id, request).await {
                Ok(response) if response.success => {
                    // Verify the fetched data is not empty and has valid content
                    if !response.data.is_empty() {
                        // Verify the SHA-256 content hash if available.
                        let fetched_hash: [u8; 32] =
                            sha2::Sha256::new().chain_update(&response.data).finalize().into();
                        if response.content_hash == fetched_hash.as_slice()
                            || response.content_hash.is_empty()
                        {
                            info!(
                                key = %key,
                                replica = %replica_id,
                                "fetched healthy copy from replica"
                            );
                            // Fetching the copy is half the job. Writing it
                            // back is the other half, and until it is done
                            // this node still holds the damage — however
                            // confidently the counter says otherwise.
                            ctx.storage
                                .heal_object_blob(
                                    bucket,
                                    object_key,
                                    record.version_id.as_deref(),
                                    &response.data,
                                )
                                .await
                                .map_err(|e| {
                                    ClusterError::Storage(format!(
                                        "writing the healthy copy of {key} back failed: {e}"
                                    ))
                                })?;
                            info!(
                                key = %key,
                                replica = %replica_id,
                                bytes = response.data.len(),
                                "bit rot repaired from replica"
                            );
                            return Ok(true);
                        }
                        warn!(
                            replica = %replica_id,
                            "replica data fails hash verification, trying next"
                        );
                    }
                }
                Ok(_) => {
                    debug!(
                        replica = %replica_id,
                        "replica returned error response"
                    );
                }
                Err(e) => {
                    debug!(
                        replica = %replica_id,
                        error = %e,
                        "failed to read from replica"
                    );
                }
            }
        }

        Ok(false)
    }

    /// List all volume IDs by scanning the volumes directory.
    async fn list_volume_ids(&self) -> Result<Vec<u32>, ClusterError> {
        list_volume_ids(&self.volumes_dir).await
    }

    /// How long to wait before the next cycle, given what this one found.
    ///
    /// A cycle that left damage behind comes back soon: the copy it could not
    /// fetch may be one restart away, and a month is a long time to carry a
    /// blob nobody can read. A clean cycle paces itself by the full scan
    /// period, which is what the scrubber's I/O budget is written against.
    fn inter_cycle_delay(&self, stats: &ScrubCycleStats) -> Duration {
        if stats.corruptions_unrecoverable > 0 {
            self.config.unhealed_retry_period.min(self.config.full_scan_period)
        } else {
            self.config.full_scan_period
        }
    }
}

/// The bucket and object key an index key names.
///
/// Index keys are `bucket/key`, and a version of an object is indexed as
/// `bucket/key#version`. Everything outside the index — a read from a replica,
/// a heal on this node — takes the version beside the key rather than inside
/// it, so the suffix comes off here and exactly once.
fn object_reference<'a>(
    key: &'a str,
    version_id: Option<&str>,
) -> Result<(&'a str, &'a str), ClusterError> {
    let (bucket, indexed_key) = key
        .split_once('/')
        .ok_or_else(|| ClusterError::Storage(format!("invalid composite key: {key}")))?;
    let object_key = match version_id {
        Some(version_id) => {
            let suffix = format!("#{version_id}");
            indexed_key.strip_suffix(&suffix).unwrap_or(indexed_key)
        }
        None => indexed_key,
    };
    Ok((bucket, object_key))
}

/// List volume IDs from a volumes directory.
pub async fn list_volume_ids(volumes_dir: &Path) -> Result<Vec<u32>, ClusterError> {
    if !volumes_dir.exists() {
        return Ok(Vec::new());
    }
    let mut entries = tokio::fs::read_dir(volumes_dir).await?;
    let mut ids = Vec::new();

    while let Some(entry) = entries.next_entry().await? {
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if let Some(id_str) = name_str.strip_prefix("volume_").and_then(|s| s.strip_suffix(".dat"))
        {
            if let Ok(id) = id_str.parse::<u32>() {
                ids.push(id);
            }
        }
    }
    ids.sort_unstable();
    Ok(ids)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::Digest;

    use s4_core::storage::{IndexDb, VolumeWriter};
    use s4_core::types::{BlobHeader, IndexRecord};
    use tempfile::TempDir;

    async fn setup_test_env() -> (TempDir, Arc<IndexDb>, PathBuf) {
        let dir = TempDir::new().unwrap();
        let volumes_dir = dir.path().join("volumes");
        tokio::fs::create_dir_all(&volumes_dir).await.unwrap();
        let db_path = dir.path().join("metadata");
        let index_db = Arc::new(IndexDb::new(&db_path).unwrap());
        (dir, index_db, volumes_dir)
    }

    async fn write_test_blob(volumes_dir: &Path, key: &str, data: &[u8]) -> (u32, u64, BlobHeader) {
        let mut writer = VolumeWriter::new(volumes_dir, 1024 * 1024).await.unwrap();
        let crc = crc32fast::hash(data);
        let header = BlobHeader::new(key.len() as u32, data.len() as u64, crc);
        let (vol_id, offset) = writer.write_blob(&header, key, data).await.unwrap();
        writer.sync().await.unwrap();
        (vol_id, offset, header)
    }

    /// A version is healed as the version it is. Sending the index key as the
    /// object key would ask a replica for an object called "key#version",
    /// which nothing has, and the damage would be reported as unrecoverable
    /// while a healthy copy sat on the next node.
    #[test]
    fn an_index_key_names_the_object_and_the_version_separately() {
        assert_eq!(
            object_reference("photos/raw.bin", None).unwrap(),
            ("photos", "raw.bin")
        );
        assert_eq!(
            object_reference("photos/raw.bin#v7", Some("v7")).unwrap(),
            ("photos", "raw.bin")
        );
        // A key that ends in something that merely looks like the version is
        // left alone: only the version this record carries comes off.
        assert_eq!(
            object_reference("photos/raw.bin#v7", Some("v8")).unwrap(),
            ("photos", "raw.bin#v7")
        );
        // And a hash inside an ordinary key is part of the key.
        assert_eq!(
            object_reference("photos/raw#1.bin", None).unwrap(),
            ("photos", "raw#1.bin")
        );
        assert!(object_reference("no-bucket-here", None).is_err());
    }

    /// Damage that could not be healed is worth another attempt long before the
    /// next full scan: the copy this cycle could not fetch may be one restart
    /// away, and a month is a long time to carry a blob nobody can read.
    #[tokio::test]
    async fn a_cycle_that_left_damage_behind_comes_back_sooner() {
        let (_dir, index_db, volumes_dir) = setup_test_env().await;
        let config = ScrubberConfig {
            full_scan_period: Duration::from_secs(30 * 24 * 3600),
            unhealed_retry_period: Duration::from_secs(300),
            ..Default::default()
        };
        let scrubber = DataScrubber::new_standalone(config, &volumes_dir, index_db);

        let healed = ScrubCycleStats {
            corruptions_found: 2,
            corruptions_healed: 2,
            ..Default::default()
        };
        let unhealed = ScrubCycleStats {
            corruptions_found: 2,
            corruptions_healed: 1,
            corruptions_unrecoverable: 1,
            ..Default::default()
        };

        assert_eq!(
            scrubber.inter_cycle_delay(&healed),
            Duration::from_secs(30 * 24 * 3600)
        );
        assert_eq!(
            scrubber.inter_cycle_delay(&unhealed),
            Duration::from_secs(300)
        );
    }

    /// And never later than the full scan it belongs to: a retry period longer
    /// than the scan period would slow the scrubber down for finding something.
    #[tokio::test]
    async fn the_retry_never_pushes_a_cycle_past_the_full_scan_period() {
        let (_dir, index_db, volumes_dir) = setup_test_env().await;
        let config = ScrubberConfig {
            full_scan_period: Duration::from_secs(60),
            unhealed_retry_period: Duration::from_secs(300),
            ..Default::default()
        };
        let scrubber = DataScrubber::new_standalone(config, &volumes_dir, index_db);

        let unhealed = ScrubCycleStats {
            corruptions_found: 1,
            corruptions_unrecoverable: 1,
            ..Default::default()
        };

        assert_eq!(
            scrubber.inter_cycle_delay(&unhealed),
            Duration::from_secs(60)
        );
    }

    /// A scrubber pointed at a directory that is not there scans nothing, and
    /// nothing is exactly what it used to report as a clean pool. It still
    /// cannot fail the cycle — a node may legitimately hold no volumes — so the
    /// guard is the warning beside this, and the wiring is held by
    /// `scripts/52-bitrot-durability-test.sh`, which rots a blob and requires
    /// the scrubber to find it.
    #[tokio::test]
    async fn a_scrubber_pointed_at_nothing_scans_nothing() {
        let (dir, index_db, _volumes_dir) = setup_test_env().await;
        let missing = dir.path().join("volumes").join("volumes");
        let scrubber = DataScrubber::new_standalone(
            ScrubberConfig {
                start_delay: Duration::from_millis(0),
                ..Default::default()
            },
            &missing,
            index_db,
        );

        let (_tx, mut shutdown) = watch::channel(false);
        let stats = scrubber.run_scrub_cycle(&mut shutdown).await.unwrap().unwrap();

        assert_eq!(stats.volumes_scanned, 0);
        assert_eq!(stats.blobs_checked, 0);
    }

    #[tokio::test]
    async fn list_volume_ids_finds_volumes() {
        let dir = TempDir::new().unwrap();
        let vols = dir.path();

        tokio::fs::write(vols.join("volume_000000.dat"), b"data").await.unwrap();
        tokio::fs::write(vols.join("volume_000001.dat"), b"data").await.unwrap();
        tokio::fs::write(vols.join("other_file.txt"), b"ignore").await.unwrap();

        let ids = list_volume_ids(vols).await.unwrap();
        assert_eq!(ids, vec![0, 1]);
    }

    #[tokio::test]
    async fn scrub_cycle_no_volumes() {
        let (_dir, index_db, volumes_dir) = setup_test_env().await;
        let config = ScrubberConfig {
            enabled: true,
            throttle_delay: Duration::from_millis(0),
            ..Default::default()
        };
        let scrubber = DataScrubber::new_standalone(config, &volumes_dir, index_db);

        let (_tx, mut shutdown) = watch::channel(false);
        let stats = scrubber.run_scrub_cycle(&mut shutdown).await.unwrap().unwrap();
        assert_eq!(stats.blobs_checked, 0);
        assert_eq!(stats.volumes_scanned, 0);
    }

    #[tokio::test]
    async fn scrub_cycle_healthy_blobs() {
        let (_dir, index_db, volumes_dir) = setup_test_env().await;

        let data = b"healthy blob data";
        let hash: [u8; 32] = sha2::Sha256::new().chain_update(data).finalize().into();
        let (vol_id, offset, _header) =
            write_test_blob(&volumes_dir, "test-bucket/key1", data).await;

        // Insert index record pointing to the volume blob
        let record = IndexRecord::new(
            vol_id,
            offset,
            data.len() as u64,
            hash,
            "etag".into(),
            "text/plain".into(),
        );
        index_db.put("test-bucket/key1", &record).await.unwrap();

        let config = ScrubberConfig {
            enabled: true,
            throttle_delay: Duration::from_millis(0),
            inter_volume_delay: Duration::from_millis(0),
            ..Default::default()
        };
        let scrubber = DataScrubber::new_standalone(config, &volumes_dir, index_db);

        let (_tx, mut shutdown) = watch::channel(false);
        let stats = scrubber.run_scrub_cycle(&mut shutdown).await.unwrap().unwrap();

        assert_eq!(stats.blobs_checked, 1);
        assert_eq!(stats.corruptions_found, 0);
        assert_eq!(stats.volumes_scanned, 1);
        assert!(stats.bytes_scanned > 0);

        let snap = scrubber.metrics().snapshot();
        assert_eq!(snap.blobs_scanned_total, 1);
        assert_eq!(snap.corruptions_found_total, 0);
    }

    #[tokio::test]
    async fn scrub_cycle_detects_corruption_standalone() {
        let metrics = crate::test_metrics::TestMetrics::install();
        let (_dir, index_db, volumes_dir) = setup_test_env().await;

        // Write a blob, then corrupt it on disk
        let data = b"will be corrupted";
        let hash: [u8; 32] = [0u8; 32];
        let (vol_id, offset, _header) = write_test_blob(&volumes_dir, "bucket/corrupt", data).await;

        let record = IndexRecord::new(
            vol_id,
            offset,
            data.len() as u64,
            hash,
            "etag".into(),
            "text/plain".into(),
        );
        index_db.put("bucket/corrupt", &record).await.unwrap();

        // Corrupt the volume file by overwriting data bytes
        let vol_path = volumes_dir.join(format!("volume_{:06}.dat", vol_id));
        let mut file_data = tokio::fs::read(&vol_path).await.unwrap();
        // Flip some bytes near the end (in the data section)
        let len = file_data.len();
        if len > 5 {
            file_data[len - 1] ^= 0xFF;
            file_data[len - 2] ^= 0xFF;
        }
        tokio::fs::write(&vol_path, &file_data).await.unwrap();

        let config = ScrubberConfig {
            enabled: true,
            throttle_delay: Duration::from_millis(0),
            inter_volume_delay: Duration::from_millis(0),
            ..Default::default()
        };
        let scrubber = DataScrubber::new_standalone(config, &volumes_dir, index_db);

        let (_tx, mut shutdown) = watch::channel(false);
        let stats = scrubber.run_scrub_cycle(&mut shutdown).await.unwrap().unwrap();

        assert_eq!(stats.blobs_checked, 1);
        assert_eq!(stats.corruptions_found, 1);
        assert_eq!(stats.corruptions_healed, 0);
        // In standalone mode, corruption is unrecoverable
        assert_eq!(stats.corruptions_unrecoverable, 1);

        let snap = scrubber.metrics().snapshot();
        assert_eq!(snap.corruptions_found_total, 1);
        assert_eq!(snap.corruptions_unrecoverable_total, 1);

        // The admin API reads the counters; the metrics must say the same.
        for (name, counter) in [
            ("s4_scrubber_blobs_scanned_total", snap.blobs_scanned_total),
            (
                "s4_scrubber_corruptions_found_total",
                snap.corruptions_found_total,
            ),
        ] {
            assert_eq!(metrics.value(name, &[]), Some(counter as f64), "{name}");
        }
        assert_eq!(
            metrics.value("s4_scrubber_corruptions_healed_total", &[]),
            None
        );
        assert_eq!(snap.corruptions_healed_total, 0);
        assert_eq!(
            metrics.value("s4_scrubber_scan_progress", &[]),
            Some(snap.scan_progress_pct as f64 / 100.0)
        );
    }

    #[tokio::test]
    async fn scrub_skips_delete_markers() {
        let (_dir, index_db, volumes_dir) = setup_test_env().await;

        // Write a normal blob to ensure the volume exists
        let data = b"live data";
        let hash: [u8; 32] = [0u8; 32];
        let (vol_id, offset, _) = write_test_blob(&volumes_dir, "bucket/live", data).await;

        let live_record = IndexRecord::new(
            vol_id,
            offset,
            data.len() as u64,
            hash,
            "etag".into(),
            "text/plain".into(),
        );
        index_db.put("bucket/live", &live_record).await.unwrap();

        // Insert a delete marker in the same volume
        let mut tombstone = IndexRecord::new_delete_marker("v1".into());
        tombstone.file_id = vol_id;
        index_db.put("bucket/deleted", &tombstone).await.unwrap();

        let config = ScrubberConfig {
            enabled: true,
            throttle_delay: Duration::from_millis(0),
            inter_volume_delay: Duration::from_millis(0),
            ..Default::default()
        };
        let scrubber = DataScrubber::new_standalone(config, &volumes_dir, index_db);

        let (_tx, mut shutdown) = watch::channel(false);
        let stats = scrubber.run_scrub_cycle(&mut shutdown).await.unwrap().unwrap();

        // Only the live blob should be checked, delete marker is skipped
        assert_eq!(stats.blobs_checked, 1);
        assert_eq!(stats.corruptions_found, 0);
    }

    #[tokio::test]
    async fn scrubber_disabled() {
        let (_dir, index_db, volumes_dir) = setup_test_env().await;
        let config = ScrubberConfig {
            enabled: false,
            ..Default::default()
        };
        let scrubber = DataScrubber::new_standalone(config, &volumes_dir, index_db);

        let (_tx, shutdown) = watch::channel(false);
        // Should return immediately without starting
        scrubber.run(shutdown).await;
        assert!(!scrubber.is_running());
    }

    #[tokio::test]
    async fn scrubber_shutdown_signal() {
        let (_dir, index_db, volumes_dir) = setup_test_env().await;
        let config = ScrubberConfig {
            enabled: true,
            throttle_delay: Duration::from_millis(0),
            inter_volume_delay: Duration::from_millis(0),
            full_scan_period: Duration::from_secs(1),
            start_delay: Duration::from_millis(0),
            ..Default::default()
        };
        let scrubber = DataScrubber::new_standalone(config, &volumes_dir, index_db);

        let (tx, shutdown) = watch::channel(false);

        let handle = tokio::spawn(async move {
            scrubber.run(shutdown).await;
        });

        // Let it run briefly, then signal shutdown
        tokio::time::sleep(Duration::from_millis(50)).await;
        tx.send(true).unwrap();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn metrics_accumulate_across_cycles() {
        let (_dir, index_db, volumes_dir) = setup_test_env().await;

        let data = b"test data";
        let hash: [u8; 32] = [0u8; 32];
        let (vol_id, offset, _) = write_test_blob(&volumes_dir, "bucket/key", data).await;

        let record = IndexRecord::new(
            vol_id,
            offset,
            data.len() as u64,
            hash,
            "etag".into(),
            "text/plain".into(),
        );
        index_db.put("bucket/key", &record).await.unwrap();

        let config = ScrubberConfig {
            enabled: true,
            throttle_delay: Duration::from_millis(0),
            inter_volume_delay: Duration::from_millis(0),
            ..Default::default()
        };
        let scrubber = DataScrubber::new_standalone(config, &volumes_dir, index_db);

        let (_tx, mut shutdown) = watch::channel(false);
        scrubber.run_scrub_cycle(&mut shutdown).await.unwrap();
        scrubber.run_scrub_cycle(&mut shutdown).await.unwrap();

        let snap = scrubber.metrics().snapshot();
        assert_eq!(snap.blobs_scanned_total, 2);
        assert_eq!(snap.volumes_scanned_total, 2);
    }
}
