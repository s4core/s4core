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
//! IndexRecords and  DedupEntries via fjall batch writes, then safely deletes
//! old volumes.

use s4_core::error::StorageError;
use s4_core::storage::{
    BatchAction, BatchOp, Deduplicator, IndexDb, KeyspaceId, VolumeReader, VolumeWriter,
};
use s4_core::types::{BlobId, BlobLocation, BlobRefEntry, CompositeManifest};
use std::collections::{HashMap, HashSet};
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
    /// Maximum age (seconds) for multipart upload sessions before the
    /// compactor considers them expired and purges their metadata.
    /// Default: 86400 (24 hours).  Must match or exceed the server's
    /// `S4_MULTIPART_UPLOAD_TTL_HOURS` setting.
    pub multipart_session_ttl_secs: u64,
    /// Maximum number of volumes to compact in a single run.
    /// Default: 10. Prevents long-running compaction.
    /// Set to 0 for unlimited.
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
            min_dead_bytes: 10 * 1024 * 1024,      // 10MB
            multipart_session_ttl_secs: 24 * 3600, // 24 hours
            max_volumes_per_run: 10,
            max_volume_size: 1024 * 1024 * 1024, // 1GB
            dry_run: false,
        }
    }
}

impl CompactionConfig {
    /// Resolves multipart session TTL from environment variables.
    ///
    /// Priority: `S4_COMPACTION_MULTIPART_TTL_SECS` (seconds, for testing)
    /// → `S4_MULTIPART_UPLOAD_TTL_HOURS` (hours) → default 24 hours.
    pub fn multipart_ttl_from_env() -> u64 {
        std::env::var("S4_COMPACTION_MULTIPART_TTL_SECS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or_else(|| {
                std::env::var("S4_MULTIPART_UPLOAD_TTL_HOURS")
                    .ok()
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(24)
                    * 3600
            })
    }

    /// Returns a config suitable for on-demand compaction via admin API.
    /// No volume limit, no minimum dead bytes, low threshold.
    pub fn on_demand(threshold: f64, dry_run: bool, multipart_session_ttl_secs: u64) -> Self {
        Self {
            fragmentation_threshold: threshold,
            min_dead_bytes: 0,
            max_volumes_per_run: 0, // unlimited
            multipart_session_ttl_secs,
            dry_run,
            ..Default::default()
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
    /// Fast-path: if both dedup and blob_ref indices are empty (no live data
    /// exists anywhere), non-active volumes are deleted outright.
    ///
    /// Returns aggregate statistics.
    pub async fn run(&self) -> Result<CompactionStats, StorageError> {
        let mut stats = CompactionStats::default();

        // Step 1: Discover volume files
        let volume_ids = self.discover_volumes().await?;
        info!("Compactor: discovered {} volume files", volume_ids.len());

        // Step 1b: Purge orphaned dedup/blob_ref entries before building indices.
        // This handles the case where objects were deleted but ref_count-based
        // cleanup left stale entries (e.g., due to bugs or incomplete deletes).
        // Running this first ensures the indices reflect only truly live data.
        match self.purge_orphaned_refs().await {
            Ok((dedup_purged, blob_ref_purged)) => {
                if dedup_purged > 0 || blob_ref_purged > 0 {
                    info!(
                        "Compactor: orphan purge complete — {} dedup + {} blob_ref entries removed",
                        dedup_purged, blob_ref_purged
                    );
                }
            }
            Err(e) => {
                warn!("Compactor: orphan purge failed (non-fatal): {}", e);
            }
        }

        // Step 2: Build dedup location index (volume_id, offset) -> content_hash
        // This avoids O(n) dedup scan per blob
        let dedup_index = self.build_dedup_index()?;
        info!(
            "Compactor: built dedup index with {} entries",
            dedup_index.len()
        );

        // Step 2b: Build blob_ref location index (volume_id, offset) -> Vec<BlobId>
        // This tracks composite manifest blobs and segment blobs that may not
        // have DedupEntry records (e.g., manifest blobs are only in BlobRefs).
        let blob_ref_index = self.build_blob_ref_index()?;
        info!(
            "Compactor: built blob_ref index with {} entries",
            blob_ref_index.len()
        );

        // Step 3: Get current writer volume to skip it (it's being written to)
        let current_volume_id = {
            let writer = self.volume_writer.read().await;
            writer.current_volume_id()
        };

        // Fast-path: if BOTH indices are empty, no live data exists anywhere.
        // All non-active volumes can be deleted outright without blob-level analysis.
        if dedup_index.is_empty() && blob_ref_index.is_empty() {
            return self.purge_all_dead_volumes(&volume_ids, current_volume_id, &mut stats).await;
        }

        // Step 4: Analyze and compact
        let mut compacted = 0usize;
        let max_per_run = self.config.max_volumes_per_run;
        for &vol_id in &volume_ids {
            if vol_id == current_volume_id {
                debug!("Compactor: skipping active volume {}", vol_id);
                continue;
            }

            stats.volumes_scanned += 1;

            match self.analyze_volume(vol_id, &dedup_index, &blob_ref_index).await {
                Ok(analysis) => {
                    // A volume with non-zero file size but 0 total_bytes means
                    // read_blob failed at offset 0.  Only safe to delete if NO
                    // live index entry references this volume — otherwise bit-rot
                    // or corruption could cause us to destroy live data.
                    let should_compact = if analysis.total_bytes == 0
                        && analysis.live_count == 0
                        && analysis.dead_count == 0
                    {
                        let volume_path =
                            self.volumes_dir.join(format!("volume_{:06}.dat", vol_id));
                        let file_size =
                            tokio::fs::metadata(&volume_path).await.map(|m| m.len()).unwrap_or(0);

                        if file_size == 0 {
                            false
                        } else {
                            // Check if ANY live index entry points to this volume.
                            // If so, the volume has live data that we can't read
                            // (bit-rot / corruption) — we must NOT delete it.
                            let has_live_refs = dedup_index.keys().any(|(vid, _)| *vid == vol_id)
                                || blob_ref_index.keys().any(|(vid, _)| *vid == vol_id);

                            if has_live_refs {
                                warn!(
                                    "Compactor: volume {} has {} bytes on disk, 0 readable blobs, \
                                     but live index entries reference it — NOT deleting \
                                     (possible bit-rot / corruption, needs manual inspection)",
                                    vol_id, file_size
                                );
                                false
                            } else {
                                warn!(
                                    "Compactor: volume {} has {} bytes on disk but 0 readable blobs \
                                     and no live index references — treating as dead",
                                    vol_id, file_size
                                );
                                true
                            }
                        }
                    } else {
                        analysis.fragmentation >= self.config.fragmentation_threshold
                            && analysis.dead_bytes >= self.config.min_dead_bytes
                    };

                    if should_compact {
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
                        } else if analysis.total_bytes == 0
                            && analysis.live_count == 0
                            && analysis.dead_count == 0
                        {
                            // Unreadable orphan volume: no live index refs, no readable
                            // blobs.  Delete directly — compact_volume() would also fail
                            // to read any blobs and skip the deletion guard.
                            let volume_path =
                                self.volumes_dir.join(format!("volume_{:06}.dat", vol_id));
                            let file_size = tokio::fs::metadata(&volume_path)
                                .await
                                .map(|m| m.len())
                                .unwrap_or(0);
                            let compacted_path = volume_path.with_extension("dat.compacted");
                            match tokio::fs::rename(&volume_path, &compacted_path).await {
                                Ok(()) => {
                                    if let Err(e) = tokio::fs::remove_file(&compacted_path).await {
                                        warn!(
                                            "Compactor: failed to remove unreadable volume {}: {}",
                                            vol_id, e
                                        );
                                        stats.errors += 1;
                                    } else {
                                        info!(
                                            "Compactor: purged unreadable orphan volume {} ({} bytes)",
                                            vol_id, file_size
                                        );
                                        stats.total_bytes_reclaimed += file_size;
                                        stats.volumes_compacted += 1;
                                        compacted += 1;
                                    }
                                }
                                Err(e) => {
                                    warn!(
                                        "Compactor: failed to rename unreadable volume {} for purge: {}",
                                        vol_id, e
                                    );
                                    stats.errors += 1;
                                }
                            }
                        } else {
                            match self.compact_volume(vol_id, &dedup_index, &blob_ref_index).await {
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
                            "Compactor: skipping volume {} (frag: {:.1}%, dead: {} bytes, live: {} blobs)",
                            vol_id,
                            analysis.fragmentation * 100.0,
                            analysis.dead_bytes,
                            analysis.live_count,
                        );
                        stats.volumes_skipped += 1;
                    }
                }
                Err(e) => {
                    warn!("Compactor: failed to analyze volume {}: {}", vol_id, e);
                    stats.errors += 1;
                }
            }

            if max_per_run > 0 && compacted >= max_per_run {
                info!("Compactor: reached max volumes per run ({})", max_per_run);
                break;
            }
        }

        Ok(stats)
    }

    /// Fast-path: delete ALL non-active volumes when both dedup and blob_ref
    /// indices are empty (no live data exists anywhere in the system).
    async fn purge_all_dead_volumes(
        &self,
        volume_ids: &[u32],
        current_volume_id: u32,
        stats: &mut CompactionStats,
    ) -> Result<CompactionStats, StorageError> {
        info!(
            "Compactor: dedup and blob_ref indices are both empty — \
             purging all non-active volumes"
        );

        for &vol_id in volume_ids {
            if vol_id == current_volume_id {
                debug!("Compactor: skipping active volume {}", vol_id);
                continue;
            }

            stats.volumes_scanned += 1;

            let volume_path = self.volumes_dir.join(format!("volume_{:06}.dat", vol_id));
            let file_size = tokio::fs::metadata(&volume_path).await.map(|m| m.len()).unwrap_or(0);

            if file_size == 0 {
                stats.volumes_skipped += 1;
                continue;
            }

            if self.config.dry_run {
                info!(
                    "Compactor [DRY-RUN]: would purge dead volume {} ({} bytes)",
                    vol_id, file_size
                );
                stats.volumes_skipped += 1;
                continue;
            }

            // Safe to delete — no live references exist
            let compacted_path = volume_path.with_extension("dat.compacted");
            match tokio::fs::rename(&volume_path, &compacted_path).await {
                Ok(()) => {
                    if let Err(e) = tokio::fs::remove_file(&compacted_path).await {
                        warn!(
                            "Compactor: failed to remove compacted volume {}: {}",
                            vol_id, e
                        );
                        stats.errors += 1;
                        continue;
                    }
                    info!(
                        "Compactor: purged dead volume {} ({} bytes)",
                        vol_id, file_size
                    );
                    stats.total_bytes_reclaimed += file_size;
                    stats.volumes_compacted += 1;
                }
                Err(e) => {
                    warn!(
                        "Compactor: failed to rename volume {} for purge: {}",
                        vol_id, e
                    );
                    stats.errors += 1;
                }
            }
        }

        Ok(stats.clone())
    }

    /// Purges orphaned dedup and blob_ref entries that are not referenced
    /// by any live object in the index.
    ///
    /// This handles the case where objects were deleted but their dedup/blob_ref
    /// entries survived (e.g., due to ref_count bugs or incomplete deletes).
    /// Without this cleanup, orphaned entries make the compactor treat dead
    /// blobs as live, preventing volume reclamation.
    ///
    /// # Algorithm
    ///
    /// 1. Scan all objects + IAM records to collect live content hashes and
    ///    manifest blob IDs.
    /// 2. For each live composite manifest, read its segment blob IDs from
    ///    the volume to build the complete set of live blob IDs.
    /// 3. Delete dedup entries whose content hash is not in the live set.
    /// 4. Scan multipart parts to find blob IDs of active in-progress uploads.
    /// 5. Delete blob_ref entries whose blob ID is not referenced by any
    ///    committed object or active multipart part.
    async fn purge_orphaned_refs(&self) -> Result<(usize, usize), StorageError> {
        // Step 1: Collect all live content hashes and manifest blob IDs
        // from the objects + IAM keyspaces.
        let (live_content_hashes, live_manifest_ids) =
            self.index_db.scan_all_content_refs().await?;

        info!(
            "Compactor: live refs scan: {} content hashes, {} composite manifests",
            live_content_hashes.len(),
            live_manifest_ids.len()
        );

        // Step 2: Resolve composite manifests to find all live blob IDs
        // (manifest blob itself + each segment blob).
        let mut live_blob_ids: HashSet<BlobId> = live_manifest_ids.clone();

        let reader = VolumeReader::new(&self.volumes_dir);
        let blob_refs_ks = self.index_db.blob_refs_keyspace();

        for manifest_id in &live_manifest_ids {
            // Look up the manifest's physical location from blob_refs
            match blob_refs_ks.get(manifest_id.as_bytes()) {
                Ok(Some(value)) => {
                    let entry: BlobRefEntry = bincode::deserialize(&value)
                        .map_err(|e| StorageError::Serialization(e.to_string()))?;

                    // Read the manifest blob from the volume
                    match reader.read_blob(entry.location.volume_id, entry.location.offset).await {
                        Ok((_header, _key, data)) => {
                            match bincode::deserialize::<CompositeManifest>(&data) {
                                Ok(manifest) => {
                                    for seg in &manifest.segments {
                                        live_blob_ids.insert(seg.blob_id);
                                    }
                                }
                                Err(e) => {
                                    warn!(
                                        "Compactor: failed to deserialize manifest {}: {}",
                                        manifest_id, e
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            warn!(
                                "Compactor: failed to read manifest blob {}: {}",
                                manifest_id, e
                            );
                        }
                    }
                }
                Ok(None) => {
                    warn!(
                        "Compactor: manifest blob {} not found in blob_refs",
                        manifest_id
                    );
                }
                Err(e) => {
                    warn!(
                        "Compactor: failed to look up manifest blob {}: {}",
                        manifest_id, e
                    );
                }
            }
        }

        // Step 3: Purge orphaned dedup entries
        let dedup_entries = self.deduplicator.iter_entries()?;
        let mut dedup_purged = 0usize;

        for (content_hash, _entry) in &dedup_entries {
            if !live_content_hashes.contains(content_hash) {
                self.deduplicator.remove_entry(content_hash)?;
                dedup_purged += 1;
            }
        }

        if dedup_purged > 0 {
            info!(
                "Compactor: purged {} orphaned dedup entries (of {} total)",
                dedup_purged,
                dedup_entries.len()
            );
        }

        // Step 4: Collect blob IDs referenced by active multipart parts
        // that have a corresponding live session.  Parts whose session has
        // been cleaned up (or never existed) are themselves orphans.
        let multipart_sessions_ks = self.index_db.multipart_sessions_keyspace();
        let multipart_parts_ks = self.index_db.multipart_parts_keyspace();

        // 4a. Collect live session IDs, expiring sessions older than TTL.
        let now_nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        let ttl_nanos = self.config.multipart_session_ttl_secs * 1_000_000_000;

        let mut live_session_ids: HashSet<String> = HashSet::new();
        let mut expired_session_keys: Vec<Vec<u8>> = Vec::new();

        for guard in multipart_sessions_ks.iter() {
            let (key_bytes, value_bytes) =
                guard.into_inner().map_err(|e| StorageError::Database(e.to_string()))?;

            if let Ok(session) =
                bincode::deserialize::<s4_core::types::MultipartUploadSession>(&value_bytes)
            {
                // Use the most recent timestamp: a session in `Completing`
                // state has `updated_at` refreshed by mark_session_completing,
                // so we won't expire a session that is actively being completed.
                let last_activity = session.updated_at.max(session.created_at);
                let age_nanos = now_nanos.saturating_sub(last_activity);
                if age_nanos > ttl_nanos {
                    // Session expired — no activity for longer than TTL.
                    expired_session_keys.push(key_bytes.to_vec());
                } else if let Ok(session_id) = std::str::from_utf8(&key_bytes) {
                    live_session_ids.insert(session_id.to_string());
                }
            } else if let Ok(session_id) = std::str::from_utf8(&key_bytes) {
                // Can't deserialize — treat as live to be safe.
                live_session_ids.insert(session_id.to_string());
            }
        }

        // Delete expired session records from fjall.
        if !expired_session_keys.is_empty() {
            let count = expired_session_keys.len();
            for key in &expired_session_keys {
                multipart_sessions_ks
                    .remove(&key[..])
                    .map_err(|e| StorageError::Database(e.to_string()))?;
            }
            info!(
                "Compactor: purged {} expired multipart sessions (TTL: {}s)",
                count, self.config.multipart_session_ttl_secs
            );
        }

        // 4b. Collect blob IDs from parts that belong to a live session.
        let mut active_staged_blob_ids: HashSet<BlobId> = HashSet::new();
        let mut orphaned_part_keys: Vec<Vec<u8>> = Vec::new();

        for guard in multipart_parts_ks.iter() {
            let (key_bytes, value_bytes) =
                guard.into_inner().map_err(|e| StorageError::Database(e.to_string()))?;

            if let Ok(part) =
                bincode::deserialize::<s4_core::types::MultipartPartRecord>(&value_bytes)
            {
                if live_session_ids.contains(&part.upload_id) {
                    active_staged_blob_ids.insert(part.blob_id);
                } else {
                    // Part's session is gone — this is an orphaned part.
                    orphaned_part_keys.push(key_bytes.to_vec());
                }
            }
        }

        // 4c. Delete orphaned multipart part records.
        if !orphaned_part_keys.is_empty() {
            let count = orphaned_part_keys.len();
            for key in &orphaned_part_keys {
                multipart_parts_ks
                    .remove(&key[..])
                    .map_err(|e| StorageError::Database(e.to_string()))?;
            }
            info!(
                "Compactor: purged {} orphaned multipart part records (no live session)",
                count
            );
        }

        info!(
            "Compactor: {} live sessions, {} blob IDs in active multipart parts",
            live_session_ids.len(),
            active_staged_blob_ids.len()
        );

        // Step 5: Purge orphaned blob_ref entries.
        // A blob_ref is orphaned if:
        //   - NOT referenced by any committed object (not in live_blob_ids), AND
        //   - NOT referenced by any active multipart part (not in active_staged_blob_ids)
        let mut blob_ref_purged = 0usize;
        let mut blob_ref_keys_to_delete: Vec<Vec<u8>> = Vec::new();

        for guard in blob_refs_ks.iter() {
            let (key_bytes, _value_bytes) =
                guard.into_inner().map_err(|e| StorageError::Database(e.to_string()))?;

            if key_bytes.len() != 32 {
                continue;
            }
            let mut blob_id_bytes = [0u8; 32];
            blob_id_bytes.copy_from_slice(&key_bytes);
            let blob_id = BlobId::from_content_hash(blob_id_bytes);

            if !live_blob_ids.contains(&blob_id) && !active_staged_blob_ids.contains(&blob_id) {
                blob_ref_keys_to_delete.push(key_bytes.to_vec());
            }
        }

        for key in &blob_ref_keys_to_delete {
            blob_refs_ks
                .remove(&key[..])
                .map_err(|e| StorageError::Database(e.to_string()))?;
            blob_ref_purged += 1;
        }

        if blob_ref_purged > 0 {
            info!(
                "Compactor: purged {} orphaned blob_ref entries",
                blob_ref_purged
            );
        }

        Ok((dedup_purged, blob_ref_purged))
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

    /// Builds an in-memory index of (volume_id, offset) -> Vec<BlobId>
    /// from the BlobRefs keyspace. This captures blobs that may not have
    /// DedupEntry records (e.g., composite manifest blobs).
    fn build_blob_ref_index(&self) -> Result<HashMap<(u32, u64), Vec<BlobId>>, StorageError> {
        let ks = self.index_db.blob_refs_keyspace();
        let mut index: HashMap<(u32, u64), Vec<BlobId>> = HashMap::new();

        for guard in ks.iter() {
            let (key_bytes, value_bytes) =
                guard.into_inner().map_err(|e| StorageError::Database(e.to_string()))?;

            if key_bytes.len() != 32 {
                continue;
            }
            let mut blob_id_bytes = [0u8; 32];
            blob_id_bytes.copy_from_slice(&key_bytes);
            let blob_id = BlobId::from_content_hash(blob_id_bytes);

            let entry: BlobRefEntry = bincode::deserialize(&value_bytes)
                .map_err(|e| StorageError::Serialization(e.to_string()))?;

            if entry.is_live() {
                let loc = (entry.location.volume_id, entry.location.offset);
                index.entry(loc).or_default().push(blob_id);
            }
        }

        Ok(index)
    }

    /// Analyzes a single volume for fragmentation.
    async fn analyze_volume(
        &self,
        volume_id: u32,
        dedup_index: &HashMap<(u32, u64), [u8; 32]>,
        blob_ref_index: &HashMap<(u32, u64), Vec<BlobId>>,
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

                    let loc = (volume_id, offset);
                    if dedup_index.contains_key(&loc) || blob_ref_index.contains_key(&loc) {
                        live_bytes += blob_total;
                        live_count += 1;
                    } else {
                        dead_bytes += blob_total;
                        dead_count += 1;
                    }

                    offset += blob_total;
                }
                Err(e) => {
                    warn!(
                        volume_id,
                        offset, "Failed to read blob during analysis, stopping scan: {e}"
                    );
                    break;
                }
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
    /// updates index, dedup, and blob refs, then deletes the old volume.
    async fn compact_volume(
        &self,
        volume_id: u32,
        dedup_index: &HashMap<(u32, u64), [u8; 32]>,
        blob_ref_index: &HashMap<(u32, u64), Vec<BlobId>>,
    ) -> Result<CompactionResult, StorageError> {
        let reader = VolumeReader::new(&self.volumes_dir);
        let volume_path = self.volumes_dir.join(format!("volume_{:06}.dat", volume_id));
        let file_size = tokio::fs::metadata(&volume_path).await.map_err(StorageError::Io)?.len();

        let mut live_blobs_copied = 0u64;
        let mut dead_blobs_skipped = 0u64;
        let mut bytes_reclaimed = 0u64;
        let mut target_volume_id = None;

        // Collect relocations: (old_offset, content_hash_opt, blob_ids, key, new_volume_id, new_offset)
        struct Relocation {
            old_offset: u64,
            content_hash: Option<[u8; 32]>,
            blob_ids: Vec<BlobId>,
            new_vol_id: u32,
            new_offset: u64,
        }
        let mut relocations: Vec<Relocation> = Vec::new();

        let mut offset = 0u64;
        while offset < file_size {
            match reader.read_blob(volume_id, offset).await {
                Ok((header, key, data)) => {
                    let header_size = header
                        .serialized_size()
                        .map_err(|e| StorageError::Serialization(e.to_string()))?
                        as u64;
                    let blob_total = header_size + header.key_len as u64 + header.blob_len;

                    let loc = (volume_id, offset);
                    let content_hash = dedup_index.get(&loc).copied();
                    let blob_ids = blob_ref_index.get(&loc).cloned().unwrap_or_default();

                    if content_hash.is_some() || !blob_ids.is_empty() {
                        // Live blob — copy to new volume via the shared writer
                        let (new_vol_id, new_offset) = {
                            let mut writer = self.volume_writer.write().await;
                            writer.write_blob(&header, &key, &data).await?
                        };

                        target_volume_id = Some(new_vol_id);
                        relocations.push(Relocation {
                            old_offset: offset,
                            content_hash,
                            blob_ids,
                            new_vol_id,
                            new_offset,
                        });
                        live_blobs_copied += 1;
                    } else {
                        dead_blobs_skipped += 1;
                        bytes_reclaimed += blob_total;
                    }

                    offset += blob_total;
                }
                Err(e) => {
                    warn!(
                        volume_id,
                        offset, "Failed to read blob during compaction, stopping scan: {e}"
                    );
                    break;
                }
            }
        }

        // Atomically update all IndexRecords, DedupEntries, and BlobRefEntries
        if !relocations.is_empty() {
            let mut ops = Vec::with_capacity(relocations.len() * 3);

            // Pre-fetch all index records for this volume once (not per relocation)
            let volume_records = self.index_db.scan_objects_by_volume(volume_id).await?;

            for reloc in &relocations {
                // Update DedupEntry location (if this blob is tracked by dedup)
                if let Some(ref content_hash) = reloc.content_hash {
                    let dedup_op = self.deduplicator.make_update_location_op(
                        content_hash,
                        reloc.new_vol_id,
                        reloc.new_offset,
                    );
                    ops.push(dedup_op);
                }

                // Update BlobRefEntry locations for all BlobIds pointing to old location
                let ks = self.index_db.blob_refs_keyspace();
                for blob_id in &reloc.blob_ids {
                    if let Some(ref_bytes) = ks
                        .get(blob_id.as_bytes())
                        .map_err(|e| StorageError::Database(e.to_string()))?
                    {
                        let mut entry: BlobRefEntry = bincode::deserialize(&ref_bytes)
                            .map_err(|e| StorageError::Serialization(e.to_string()))?;
                        entry.location = BlobLocation {
                            volume_id: reloc.new_vol_id,
                            offset: reloc.new_offset,
                        };
                        let value = bincode::serialize(&entry)
                            .map_err(|e| StorageError::Serialization(e.to_string()))?;
                        ops.push(BatchOp {
                            keyspace: KeyspaceId::BlobRefs,
                            action: BatchAction::Put(blob_id.as_bytes().to_vec(), value),
                        });
                    }
                }

                // Find IndexRecords pointing to old location in this volume
                for (ks_id, rec_key, record) in &volume_records {
                    if record.offset == reloc.old_offset && record.file_id == volume_id {
                        let mut updated = record.clone();
                        updated.file_id = reloc.new_vol_id;
                        updated.offset = reloc.new_offset;
                        let value = bincode::serialize(&updated)
                            .map_err(|e| StorageError::Serialization(e.to_string()))?;
                        ops.push(BatchOp {
                            keyspace: *ks_id,
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
