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

//! Volume compaction — reclaims dead space from append-only volume files.
//!
//! The compactor scans volume files, identifies dead blobs (those with no
//! matching DedupEntry), copies live blobs to new volumes, atomically updates
//! IndexRecords and DedupEntries via fjall batch writes, then safely deletes
//! old volumes.

use s4_core::error::StorageError;
use s4_core::storage::{
    BatchAction, BatchOp, Deduplicator, IndexDb, KeyspaceId, VolumeReader, VolumeWriter,
};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Configuration for the compaction process.
#[derive(Debug, Clone)]
pub struct CompactionConfig {
    /// Minimum fragmentation ratio (0.0–1.0) to trigger compaction.
    /// Default: 0.3 (30% dead space).
    pub fragmentation_threshold: f64,
    /// Minimum dead bytes in a volume to consider it for compaction.
    /// Default: 10MB. Prevents compacting nearly-empty volumes.
    pub min_dead_bytes: u64,
    /// Maximum number of volumes to compact in a single run.
    /// Default: 10. Prevents long-running compaction.
    pub max_volumes_per_run: usize,
    /// Maximum volume size (bytes) for new compacted volumes.
    pub max_volume_size: u64,
    /// Whether to perform a dry run (report only, don't compact).
    pub dry_run: bool,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            fragmentation_threshold: 0.3,
            min_dead_bytes: 10 * 1024 * 1024, // 10MB
            max_volumes_per_run: 10,
            max_volume_size: 1024 * 1024 * 1024, // 1GB
            dry_run: false,
        }
    }
}

/// Result of compacting a single volume.
#[derive(Debug, Clone)]
pub struct CompactionResult {
    /// Volume ID that was compacted.
    pub source_volume_id: u32,
    /// Number of live blobs copied.
    pub live_blobs_copied: u64,
    /// Number of dead blobs skipped.
    pub dead_blobs_skipped: u64,
    /// Bytes reclaimed (dead space freed).
    pub bytes_reclaimed: u64,
    /// New volume ID where live blobs were written.
    pub target_volume_id: Option<u32>,
}

/// Aggregate statistics for a compaction run.
#[derive(Debug, Clone, Default)]
pub struct CompactionStats {
    /// Total volumes scanned.
    pub volumes_scanned: u64,
    /// Volumes that were compacted.
    pub volumes_compacted: u64,
    /// Volumes skipped (below threshold).
    pub volumes_skipped: u64,
    /// Total live blobs copied.
    pub total_live_blobs: u64,
    /// Total dead blobs removed.
    pub total_dead_blobs: u64,
    /// Total bytes reclaimed.
    pub total_bytes_reclaimed: u64,
    /// Errors encountered (non-fatal).
    pub errors: u64,
}

/// Per-volume fragmentation analysis.
#[derive(Debug, Clone)]
pub struct VolumeAnalysis {
    /// Volume ID.
    pub volume_id: u32,
    /// Total bytes used by blobs (live + dead).
    pub total_bytes: u64,
    /// Bytes used by live blobs.
    pub live_bytes: u64,
    /// Bytes used by dead blobs.
    pub dead_bytes: u64,
    /// Number of live blobs.
    pub live_count: u64,
    /// Number of dead blobs.
    pub dead_count: u64,
    /// Fragmentation ratio (dead_bytes / total_bytes).
    pub fragmentation: f64,
}

/// The volume compactor.
///
/// Holds references to the storage components needed for compaction.
/// Designed to be called from a background worker or CLI tool.
pub struct VolumeCompactor {
    volumes_dir: PathBuf,
    index_db: Arc<IndexDb>,
    deduplicator: Deduplicator,
    volume_writer: Arc<RwLock<VolumeWriter>>,
    config: CompactionConfig,
}

impl VolumeCompactor {
    /// Creates a new compactor.
    pub fn new(
        volumes_dir: PathBuf,
        index_db: Arc<IndexDb>,
        deduplicator: Deduplicator,
        volume_writer: Arc<RwLock<VolumeWriter>>,
        config: CompactionConfig,
    ) -> Self {
        Self {
            volumes_dir,
            index_db,
            deduplicator,
            volume_writer,
            config,
        }
    }

    /// Runs a full compaction cycle.
    ///
    /// 1. Discovers all volume files
    /// 2. Analyzes fragmentation for each
    /// 3. Compacts volumes above the threshold
    ///
    /// Returns aggregate statistics.
    pub async fn run(&self) -> Result<CompactionStats, StorageError> {
        let mut stats = CompactionStats::default();

        // Step 1: Discover volume files
        let volume_ids = self.discover_volumes().await?;
        info!("Compactor: discovered {} volume files", volume_ids.len());

        // Step 2: Build dedup location index (volume_id, offset) -> content_hash
        // This avoids O(n) dedup scan per blob
        let dedup_index = self.build_dedup_index()?;
        info!(
            "Compactor: built dedup index with {} entries",
            dedup_index.len()
        );

        // Step 3: Get current writer volume to skip it (it's being written to)
        let current_volume_id = {
            let writer = self.volume_writer.read().await;
            writer.current_volume_id()
        };

        // Step 4: Analyze and compact
        let mut compacted = 0usize;
        for &vol_id in &volume_ids {
            if vol_id == current_volume_id {
                debug!("Compactor: skipping active volume {}", vol_id);
                continue;
            }

            stats.volumes_scanned += 1;

            match self.analyze_volume(vol_id, &dedup_index).await {
                Ok(analysis) => {
                    if analysis.fragmentation >= self.config.fragmentation_threshold
                        && analysis.dead_bytes >= self.config.min_dead_bytes
                    {
                        if self.config.dry_run {
                            info!(
                                "Compactor [DRY-RUN]: would compact volume {} \
                                 (frag: {:.1}%, dead: {} bytes, live: {} blobs)",
                                vol_id,
                                analysis.fragmentation * 100.0,
                                analysis.dead_bytes,
                                analysis.live_count,
                            );
                            stats.volumes_skipped += 1;
                        } else {
                            match self.compact_volume(vol_id, &dedup_index).await {
                                Ok(result) => {
                                    info!(
                                        "Compactor: compacted volume {} -> {}: \
                                         {} live blobs copied, {} dead skipped, \
                                         {} bytes reclaimed",
                                        vol_id,
                                        result.target_volume_id.unwrap_or(0),
                                        result.live_blobs_copied,
                                        result.dead_blobs_skipped,
                                        result.bytes_reclaimed,
                                    );
                                    stats.total_live_blobs += result.live_blobs_copied;
                                    stats.total_dead_blobs += result.dead_blobs_skipped;
                                    stats.total_bytes_reclaimed += result.bytes_reclaimed;
                                    stats.volumes_compacted += 1;
                                    compacted += 1;
                                }
                                Err(e) => {
                                    warn!("Compactor: failed to compact volume {}: {}", vol_id, e);
                                    stats.errors += 1;
                                }
                            }
                        }
                    } else {
                        debug!(
                            "Compactor: skipping volume {} (frag: {:.1}%, dead: {} bytes)",
                            vol_id,
                            analysis.fragmentation * 100.0,
                            analysis.dead_bytes,
                        );
                        stats.volumes_skipped += 1;
                    }
                }
                Err(e) => {
                    warn!("Compactor: failed to analyze volume {}: {}", vol_id, e);
                    stats.errors += 1;
                }
            }

            if compacted >= self.config.max_volumes_per_run {
                info!(
                    "Compactor: reached max volumes per run ({})",
                    self.config.max_volumes_per_run
                );
                break;
            }
        }

        Ok(stats)
    }

    /// Discovers all volume files in the volumes directory.
    async fn discover_volumes(&self) -> Result<Vec<u32>, StorageError> {
        let mut volume_ids = Vec::new();
        let mut entries = tokio::fs::read_dir(&self.volumes_dir).await.map_err(StorageError::Io)?;

        while let Some(entry) = entries.next_entry().await.map_err(StorageError::Io)? {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if let Some(id_str) =
                name_str.strip_prefix("volume_").and_then(|s| s.strip_suffix(".dat"))
            {
                if let Ok(id) = id_str.parse::<u32>() {
                    volume_ids.push(id);
                }
            }
        }

        volume_ids.sort();
        Ok(volume_ids)
    }

    /// Builds an in-memory index of (volume_id, offset) -> content_hash
    /// from the dedup keyspace for fast blob liveness checks.
    fn build_dedup_index(&self) -> Result<HashMap<(u32, u64), [u8; 32]>, StorageError> {
        let entries = self.deduplicator.iter_entries()?;
        let mut index = HashMap::with_capacity(entries.len());
        for (hash, entry) in entries {
            index.insert((entry.volume_id, entry.offset), hash);
        }
        Ok(index)
    }

    /// Analyzes a single volume for fragmentation.
    async fn analyze_volume(
        &self,
        volume_id: u32,
        dedup_index: &HashMap<(u32, u64), [u8; 32]>,
    ) -> Result<VolumeAnalysis, StorageError> {
        let reader = VolumeReader::new(&self.volumes_dir);
        let volume_path = self.volumes_dir.join(format!("volume_{:06}.dat", volume_id));
        let file_size = tokio::fs::metadata(&volume_path).await.map_err(StorageError::Io)?.len();

        let mut offset = 0u64;
        let mut live_bytes = 0u64;
        let mut dead_bytes = 0u64;
        let mut live_count = 0u64;
        let mut dead_count = 0u64;

        while offset < file_size {
            match reader.read_blob(volume_id, offset).await {
                Ok((header, _key, _data)) => {
                    let header_size = header
                        .serialized_size()
                        .map_err(|e| StorageError::Serialization(e.to_string()))?
                        as u64;
                    let blob_total = header_size + header.key_len as u64 + header.blob_len;

                    if dedup_index.contains_key(&(volume_id, offset)) {
                        live_bytes += blob_total;
                        live_count += 1;
                    } else {
                        dead_bytes += blob_total;
                        dead_count += 1;
                    }

                    offset += blob_total;
                }
                Err(_) => break, // End of readable data
            }
        }

        let total_bytes = live_bytes + dead_bytes;
        let fragmentation = if total_bytes > 0 {
            dead_bytes as f64 / total_bytes as f64
        } else {
            0.0
        };

        Ok(VolumeAnalysis {
            volume_id,
            total_bytes,
            live_bytes,
            dead_bytes,
            live_count,
            dead_count,
            fragmentation,
        })
    }

    /// Compacts a single volume: copies live blobs to a new volume,
    /// updates index and dedup, then deletes the old volume.
    async fn compact_volume(
        &self,
        volume_id: u32,
        dedup_index: &HashMap<(u32, u64), [u8; 32]>,
    ) -> Result<CompactionResult, StorageError> {
        let reader = VolumeReader::new(&self.volumes_dir);
        let volume_path = self.volumes_dir.join(format!("volume_{:06}.dat", volume_id));
        let file_size = tokio::fs::metadata(&volume_path).await.map_err(StorageError::Io)?.len();

        let mut live_blobs_copied = 0u64;
        let mut dead_blobs_skipped = 0u64;
        let mut bytes_reclaimed = 0u64;
        let mut target_volume_id = None;

        // Collect relocations: (old_offset, content_hash, key, new_volume_id, new_offset)
        let mut relocations: Vec<(u64, [u8; 32], String, u32, u64)> = Vec::new();

        let mut offset = 0u64;
        while offset < file_size {
            match reader.read_blob(volume_id, offset).await {
                Ok((header, key, data)) => {
                    let header_size = header
                        .serialized_size()
                        .map_err(|e| StorageError::Serialization(e.to_string()))?
                        as u64;
                    let blob_total = header_size + header.key_len as u64 + header.blob_len;

                    if let Some(&content_hash) = dedup_index.get(&(volume_id, offset)) {
                        // Live blob — copy to new volume via the shared writer
                        let (new_vol_id, new_offset) = {
                            let mut writer = self.volume_writer.write().await;
                            writer.write_blob(&header, &key, &data).await?
                        };

                        target_volume_id = Some(new_vol_id);
                        relocations.push((offset, content_hash, key, new_vol_id, new_offset));
                        live_blobs_copied += 1;
                    } else {
                        dead_blobs_skipped += 1;
                        bytes_reclaimed += blob_total;
                    }

                    offset += blob_total;
                }
                Err(_) => break,
            }
        }

        // Atomically update all IndexRecords and DedupEntries in a single batch
        if !relocations.is_empty() {
            let mut ops = Vec::with_capacity(relocations.len() * 2);

            // Pre-fetch all index records for this volume once (not per relocation)
            let volume_records = self.index_db.scan_objects_by_volume(volume_id).await?;

            for (old_offset, content_hash, _key, new_vol_id, new_offset) in &relocations {
                // Update DedupEntry location
                let dedup_op = self.deduplicator.make_update_location_op(
                    content_hash,
                    *new_vol_id,
                    *new_offset,
                )?;
                ops.push(dedup_op);

                // Find IndexRecords pointing to old location in this volume
                for (rec_key, record) in &volume_records {
                    if record.offset == *old_offset && record.file_id == volume_id {
                        let mut updated = record.clone();
                        updated.file_id = *new_vol_id;
                        updated.offset = *new_offset;
                        let value = bincode::serialize(&updated)
                            .map_err(|e| StorageError::Serialization(e.to_string()))?;
                        ops.push(BatchOp {
                            keyspace: KeyspaceId::Objects,
                            action: BatchAction::Put(rec_key.clone().into_bytes(), value),
                        });
                    }
                }
            }

            // Single atomic commit for all relocations
            self.index_db.batch_write(ops).await?;

            // Sync the volume writer to ensure new blobs are durable
            {
                let mut writer = self.volume_writer.write().await;
                writer.sync().await?;
            }
        }

        // Delete old volume file ONLY after all updates are committed and synced
        if live_blobs_copied > 0 || dead_blobs_skipped > 0 {
            // Rename to .compacted first (recoverable if crash between rename and delete)
            let compacted_path = volume_path.with_extension("dat.compacted");
            tokio::fs::rename(&volume_path, &compacted_path)
                .await
                .map_err(StorageError::Io)?;
            tokio::fs::remove_file(&compacted_path).await.map_err(StorageError::Io)?;
            info!("Compactor: deleted old volume {}", volume_id);
        }

        Ok(CompactionResult {
            source_volume_id: volume_id,
            live_blobs_copied,
            dead_blobs_skipped,
            bytes_reclaimed,
            target_volume_id,
        })
    }
}
