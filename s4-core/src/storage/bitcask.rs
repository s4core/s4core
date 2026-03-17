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

//! Bitcask storage engine implementation.
//!
//! This module implements the StorageEngine trait using the Bitcask/Haystack approach:
//! - All objects stored in append-only volume files (including tiny objects)
//! - Content-addressable deduplication
//! - S3-compatible object versioning
//! - Metadata journal for full-fidelity crash recovery
//!
//! # Inline Storage (Legacy)
//!
//! Prior to Phase 5, objects smaller than 4 KB were stored inline in the metadata
//! database. New writes always go to volume files. Legacy inline objects are still
//! readable and can be migrated to volumes via [`BitcaskStorageEngine::migrate_inline_objects`].
//!
//! # Atomicity (Phase 3)
//!
//! All multi-key mutations (versioned PUT, versioned DELETE) are performed as
//! a single atomic fjall batch write. This eliminates the crash windows that
//! existed in Phase 2 where sequential individual writes could leave the
//! system in an inconsistent state after a process crash.
//!
//! # Metadata Journal (Phase 5)
//!
//! Every mutation is recorded in an append-only metadata journal (fjall `journal`
//! keyspace) inside the same atomic batch as the index update. The journal enables
//! full-fidelity recovery of `content_type`, user metadata, Object Lock fields,
//! and delete markers — information that was previously lost during volume-only
//! recovery.

use crate::error::StorageError;
use crate::storage::engine::{DeleteMarkerEntry, DeleteResult, ListVersionsResult, ObjectVersion};
use crate::storage::journal::JournalEventType;
use crate::storage::{
    generate_version_id, BatchAction, BatchOp, Deduplicator, IndexDb, KeyspaceId, MetadataJournal,
    VersionList, VolumeReader, VolumeWriter, NULL_VERSION_ID,
};
use crate::types::composite::{
    BlobId, BlobLocation, BlobRefEntry, CompositeManifest, ManifestSegmentRef, MultipartPartRecord,
    MultipartUploadSession, MultipartUploadState, MANIFEST_FORMAT_VERSION,
};
use crate::types::{
    BlobHeader, DefaultRetention, IndexRecord, VersioningStatus, DELETE_MARKER_FILE_ID,
};
use crate::StorageEngine;
use base64::Engine;
use serde::Serialize;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;

// ============================================================================
// Diagnostic Counters
// ============================================================================

/// Diagnostic counters for monitoring storage engine health.
///
/// These atomic counters track operational metrics that help detect potential
/// issues, especially around inline storage usage and deduplication rebuild
/// performance.
///
/// All counters are lock-free and safe to read concurrently from any thread.
#[derive(Debug)]
pub struct DiagnosticCounters {
    /// Number of objects stored inline in the metadata database (file_id == u32::MAX).
    ///
    /// Inline objects are stored as base64 in the IndexRecord metadata and are
    /// NOT recoverable from volume files if the metadata database is lost.
    /// This counter helps track how many objects are at risk.
    pub inline_objects_count: AtomicU64,

    /// Number of orphaned versions found during the last consistency audit.
    ///
    /// An orphaned version is one that is referenced by a version list but
    /// has no corresponding record in the index (or vice versa). With Phase 3
    /// atomic batches, this should always be 0 for new writes.
    pub orphaned_versions_found: AtomicU64,

    /// Time in milliseconds to rebuild the dedup map from the index at startup.
    ///
    /// A high value indicates a large index and may suggest the need for
    /// persistent (on-disk) dedup storage (planned for Phase 4).
    pub dedup_rebuild_duration_ms: AtomicU64,
}

impl DiagnosticCounters {
    fn new() -> Self {
        Self {
            inline_objects_count: AtomicU64::new(0),
            orphaned_versions_found: AtomicU64::new(0),
            dedup_rebuild_duration_ms: AtomicU64::new(0),
        }
    }
}

/// Snapshot of diagnostic counter values at a point in time.
///
/// This is a plain data struct returned by [`BitcaskStorageEngine::diagnostics`]
/// for monitoring and observability purposes.
#[derive(Debug, Clone, Serialize)]
pub struct DiagnosticSnapshot {
    /// Number of objects stored inline in the metadata database.
    pub inline_objects_count: u64,
    /// Number of orphaned versions found during the last consistency audit.
    pub orphaned_versions_found: u64,
    /// Time in milliseconds to rebuild the dedup map at last startup.
    pub dedup_rebuild_duration_ms: u64,
    /// Current journal sequence number (latest entry).
    ///
    /// In future distributed mode, the difference between leader and replica
    /// sequence numbers indicates replication lag.
    pub journal_sequence_current: u64,
    /// Total number of entries in the journal keyspace.
    ///
    /// Grows with each mutation until compaction removes old entries.
    /// Monitor to detect when compaction is needed.
    pub journal_entries_total: u64,
    /// Approximate total size in bytes of all journal entries (keys + values).
    ///
    /// Useful for monitoring journal growth and deciding when to trigger
    /// compaction. This is a logical size; on-disk size may differ due to
    /// LSM-tree compression.
    pub journal_size_bytes: u64,
}

/// Aggregated storage statistics for monitoring and observability.
#[derive(Debug, Clone, Serialize)]
pub struct StorageStats {
    /// Number of buckets.
    pub buckets_count: u64,
    /// Number of objects (excluding delete markers and internal keys).
    pub objects_count: u64,
    /// Total size of stored objects in bytes.
    pub storage_used_bytes: u64,
    /// Number of unique content blobs in the deduplicator.
    pub dedup_unique_blobs: u64,
    /// Total number of content references (including duplicates).
    pub dedup_total_references: u64,
}

/// Bitcask storage engine implementation.
///
/// This is the main storage engine that combines:
/// - VolumeWriter/Reader for append-only log storage
/// - IndexDb for metadata storage (fjall with separate keyspaces)
/// - Deduplicator for content-addressable storage
///
/// # Atomicity
///
/// All versioned write operations use [`IndexDb::batch_write`] to ensure
/// that version records, version lists, and current pointers are updated
/// atomically. There are no crash windows between related writes.
pub struct BitcaskStorageEngine {
    /// Directory for volume files
    volumes_dir: PathBuf,
    /// Path to the metadata database (stored for potential recovery operations)
    #[allow(dead_code)]
    metadata_path: PathBuf,
    /// Volume writer (protected by RwLock for concurrent access)
    volume_writer: Arc<RwLock<VolumeWriter>>,
    /// Index database
    index_db: Arc<IndexDb>,
    /// Disk-backed deduplicator (fjall keyspace, no locking required).
    deduplicator: Deduplicator,
    /// Metadata journal for ordered mutation logging (Phase 5).
    journal: MetadataJournal,
    /// Threshold for inline storage (default: 4KB).
    ///
    /// Phase 5: No longer used for new writes (all objects go to volumes).
    /// Kept for backward compatibility in the constructor signature.
    #[allow(dead_code)]
    inline_threshold: usize,
    /// Whether to fsync after each write (strict consistency)
    strict_sync: bool,
    /// Diagnostic counters for monitoring and observability.
    diagnostic_counters: Arc<DiagnosticCounters>,
}

impl BitcaskStorageEngine {
    /// Creates a new Bitcask storage engine.
    ///
    /// # Arguments
    ///
    /// * `volumes_dir` - Directory where volume files are stored
    /// * `metadata_path` - Path to the fjall metadata database directory
    /// * `max_volume_size` - Maximum size of a volume file in bytes (default: 1GB)
    /// * `inline_threshold` - Objects smaller than this are stored inline (default: 4KB)
    /// * `strict_sync` - If true, fsync after each write (default: true)
    pub async fn new(
        volumes_dir: impl Into<PathBuf>,
        metadata_path: impl Into<PathBuf>,
        max_volume_size: u64,
        inline_threshold: usize,
        strict_sync: bool,
    ) -> Result<Self, StorageError> {
        let volumes_dir = volumes_dir.into();
        let metadata_path = metadata_path.into();

        // Create volume writer
        let volume_writer = VolumeWriter::new(&volumes_dir, max_volume_size).await?;
        let volume_writer = Arc::new(RwLock::new(volume_writer));

        // Create index database
        let index_db = Arc::new(IndexDb::new(&metadata_path)?);

        // Create disk-backed deduplicator using the fjall dedup keyspace (per-PG scoped in Phase 6).
        // If the keyspace is empty (first startup after Phase 4 migration),
        // rebuild from the objects keyspace.
        let deduplicator = Deduplicator::new(
            index_db.dedup_keyspace(),
            crate::storage::placement::PlacementGroupId::LOCAL,
        );

        // Phase 6: Migrate old-format dedup keys (32 bytes) to new PG-prefixed format (36 bytes).
        // The dedup data is always rebuildable from the objects keyspace.
        if deduplicator.needs_key_format_migration()? {
            tracing::info!("Migrating dedup keyspace to PG-scoped key format (Phase 6)");
            deduplicator.clear_all()?;
        }

        // Initialize metadata journal (Phase 5, PG-aware in Phase 6).
        let journal = MetadataJournal::new(
            index_db.journal_keyspace(),
            crate::storage::placement::PlacementGroupId::LOCAL,
        )?;

        // Auto-detect metadata loss and trigger recovery.
        // If objects keyspace is empty but volume files exist, the metadata
        // database was deleted or corrupted — run automatic recovery.
        if index_db.is_objects_empty()? {
            let has_volumes = {
                let mut found = false;
                if let Ok(mut entries) = tokio::fs::read_dir(&volumes_dir).await {
                    while let Ok(Some(entry)) = entries.next_entry().await {
                        if let Some(name) = entry.file_name().to_str() {
                            if name.starts_with("volume_") && name.ends_with(".dat") {
                                found = true;
                                break;
                            }
                        }
                    }
                }
                found
            };
            if has_volumes {
                tracing::warn!(
                    "Empty metadata detected with existing volume files — triggering automatic recovery"
                );
                let recovered = crate::storage::recovery::recover_index(
                    &volumes_dir,
                    &index_db,
                    &journal,
                    &deduplicator,
                )
                .await?;
                tracing::info!(
                    "Automatic recovery complete: {} records restored",
                    recovered
                );
            }
        }

        let dedup_start = Instant::now();

        // Count inline objects from objects keyspace
        let all_records = index_db.list_objects("", usize::MAX).await?;
        let mut inline_count = 0u64;
        for (_, record) in &all_records {
            if record.file_id == u32::MAX {
                inline_count += 1;
            }
        }

        // Rebuild dedup from index only if the dedup keyspace is empty
        // (first startup after Phase 4 migration or after data loss).
        // On subsequent startups, dedup data is already persistent — no rebuild.
        if deduplicator.is_empty()? {
            deduplicator.build_from_index(all_records.into_iter())?;
        }

        let dedup_rebuild_ms = dedup_start.elapsed().as_millis() as u64;

        // Initialize diagnostic counters
        let diagnostic_counters = Arc::new(DiagnosticCounters::new());
        diagnostic_counters.inline_objects_count.store(inline_count, Ordering::Relaxed);
        diagnostic_counters
            .dedup_rebuild_duration_ms
            .store(dedup_rebuild_ms, Ordering::Relaxed);

        Ok(Self {
            volumes_dir,
            metadata_path,
            volume_writer,
            index_db,
            deduplicator,
            journal,
            inline_threshold,
            strict_sync,
            diagnostic_counters,
        })
    }

    /// Constructs the full key (bucket/path) for storage.
    fn make_key(&self, bucket: &str, key: &str) -> String {
        format!("{}/{}", bucket, key)
    }

    /// Computes the ETag (MD5 hash as hex string, per S3 spec).
    fn compute_etag(data: &[u8]) -> String {
        use md5::{Digest, Md5};
        let hash = Md5::digest(data);
        hex::encode(hash)
    }

    /// Recovers the index database using the best available strategy.
    ///
    /// If the metadata journal contains entries, replays them for full-fidelity
    /// recovery (content_type, metadata, Object Lock fields preserved). Otherwise,
    /// falls back to legacy volume-only recovery (lossy).
    ///
    /// # Returns
    ///
    /// Returns the number of records recovered.
    pub async fn recover_index(&self) -> Result<usize, StorageError> {
        use crate::storage::recovery::recover_index;
        recover_index(
            &self.volumes_dir,
            &self.index_db,
            &self.journal,
            &self.deduplicator,
        )
        .await
    }

    /// Syncs all pending writes to disk.
    ///
    /// This ensures all data written up to this point is persisted to disk.
    /// Called automatically after each write if `strict_sync` is enabled.
    pub async fn sync(&self) -> Result<(), StorageError> {
        let mut writer = self.volume_writer.write().await;
        writer.sync().await
    }

    /// Returns aggregated storage statistics.
    ///
    /// Scans the index database to count buckets, objects, and storage used.
    /// Also queries the deduplicator for dedup ratio data.
    pub async fn get_stats(&self) -> Result<StorageStats, StorageError> {
        let all_records = self.index_db.list_objects("", usize::MAX).await?;

        let mut objects_count: u64 = 0;
        let mut storage_used_bytes: u64 = 0;

        // Count buckets via buckets keyspace
        let system_records = self
            .index_db
            .list("__system__/__s4_bucket_marker_", usize::MAX)
            .await
            .unwrap_or_default();
        let buckets_count =
            system_records.iter().filter(|(_, r)| !r.is_delete_marker).count() as u64;

        for (key, record) in &all_records {
            // Skip versioned keys (bucket/key#version_id)
            if key.contains('#') {
                continue;
            }
            // Skip delete markers
            if record.is_delete_marker {
                continue;
            }

            // Extract bucket name (first path component)
            let bucket_name = if let Some(slash_pos) = key.find('/') {
                &key[..slash_pos]
            } else {
                continue;
            };
            // Skip internal buckets (e.g., __system__ used by IAM)
            if bucket_name.starts_with("__") {
                continue;
            }

            objects_count += 1;
            storage_used_bytes += record.size;
        }

        let dedup = self.deduplicator.clone();
        let (dedup_unique_blobs, dedup_total_references) =
            tokio::task::spawn_blocking(move || dedup.stats())
                .await
                .map_err(|e| StorageError::Database(e.to_string()))??;

        Ok(StorageStats {
            buckets_count,
            objects_count,
            storage_used_bytes,
            dedup_unique_blobs,
            dedup_total_references,
        })
    }

    // ========================================================================
    // Diagnostic Methods
    // ========================================================================

    /// Returns a snapshot of diagnostic counter values.
    ///
    /// These counters track operational health metrics including inline object
    /// counts, orphaned version detection, and dedup rebuild performance.
    pub fn diagnostics(&self) -> DiagnosticSnapshot {
        DiagnosticSnapshot {
            inline_objects_count: self
                .diagnostic_counters
                .inline_objects_count
                .load(Ordering::Relaxed),
            orphaned_versions_found: self
                .diagnostic_counters
                .orphaned_versions_found
                .load(Ordering::Relaxed),
            dedup_rebuild_duration_ms: self
                .diagnostic_counters
                .dedup_rebuild_duration_ms
                .load(Ordering::Relaxed),
            journal_sequence_current: self.journal.current_sequence(),
            journal_entries_total: self.journal.entry_count().unwrap_or(0),
            journal_size_bytes: self.journal.size_bytes().unwrap_or(0),
        }
    }

    /// Returns a reference to the metadata journal.
    ///
    /// Useful for checking journal metrics (sequence number, entry count) and
    /// triggering compaction.
    pub fn journal(&self) -> &MetadataJournal {
        &self.journal
    }

    // ========================================================================
    // Compaction support — internal accessors for s4-compactor
    // ========================================================================

    /// Returns the volumes directory path.
    ///
    /// Used by the compaction worker to discover and scan volume files.
    pub fn volumes_dir(&self) -> &std::path::Path {
        &self.volumes_dir
    }

    /// Returns the deduplicator.
    ///
    /// Used by the compaction worker to iterate entries and update
    /// blob locations after compaction.
    pub fn deduplicator(&self) -> &Deduplicator {
        &self.deduplicator
    }

    /// Returns the volume writer (shared `Arc<RwLock>`).
    ///
    /// Used by the compaction worker to write relocated live blobs
    /// to new volumes.
    pub fn volume_writer(&self) -> &Arc<RwLock<VolumeWriter>> {
        &self.volume_writer
    }

    /// Compacts the metadata journal by removing old entries.
    ///
    /// In single-node mode, all entries up to the current sequence are removed.
    /// In future distributed mode, `min_confirmed_sequence` ensures that entries
    /// not yet confirmed by all replicas are preserved.
    ///
    /// This method is safe to call from a background task while the engine is
    /// actively processing writes.
    ///
    /// # Returns
    ///
    /// The number of journal entries removed.
    pub fn compact_journal(
        &self,
        min_confirmed_sequence: Option<u64>,
    ) -> Result<u64, StorageError> {
        self.journal.compact(min_confirmed_sequence)
    }

    /// Audits version list consistency and updates the orphaned versions counter.
    ///
    /// Scans all version lists and checks that every referenced version ID has
    /// a corresponding record in the index. Versions that appear in a version
    /// list but have no index record are counted as orphaned.
    ///
    /// This method also checks the reverse: version records that exist in the
    /// index (keys containing `#`) but are not referenced by any version list.
    ///
    /// # Returns
    ///
    /// The total number of orphaned version references found (both directions).
    ///
    /// # Note
    ///
    /// With Phase 3 atomic batch writes, new writes should never produce orphaned
    /// versions. A non-zero result indicates either legacy data from before Phase 3
    /// or a bug in the batch construction.
    pub async fn audit_version_consistency(&self) -> Result<u64, StorageError> {
        let mut orphaned_count = 0u64;

        // Direction 1: Versions referenced in version lists but missing from index
        let version_list_entries = self.index_db.list_version_lists("", usize::MAX).await?;

        for (bucket_key, version_list) in &version_list_entries {
            // Check each version in the list has a corresponding index record
            for vid in &version_list.versions {
                let version_key = format!("{}#{}", bucket_key, vid);
                if self.index_db.get(&version_key).await?.is_none() {
                    orphaned_count += 1;
                }
            }
        }

        // Direction 2: Version records in index not referenced by any version list
        let mut known_versions: std::collections::HashSet<String> =
            std::collections::HashSet::new();
        for (bucket_key, version_list) in &version_list_entries {
            for vid in &version_list.versions {
                known_versions.insert(format!("{}#{}", bucket_key, vid));
            }
        }

        // Scan all object records and find versioned records not in any version list
        let all_records = self.index_db.list_objects("", usize::MAX).await?;
        for (key, _record) in &all_records {
            // Skip non-versioned keys
            if !key.contains('#') {
                continue;
            }
            if !known_versions.contains(key.as_str()) {
                orphaned_count += 1;
            }
        }

        self.diagnostic_counters
            .orphaned_versions_found
            .store(orphaned_count, Ordering::Relaxed);

        Ok(orphaned_count)
    }

    // ========================================================================
    // Inline Migration (Phase 5)
    // ========================================================================

    /// Migrates legacy inline objects from the metadata database to volume files.
    ///
    /// Prior to Phase 5, objects smaller than `inline_threshold` were stored as
    /// base64-encoded data inside the `IndexRecord` metadata. These objects are
    /// NOT recoverable from volume files if the metadata database is lost.
    ///
    /// This method scans all object records, finds those with `file_id == u32::MAX`
    /// (inline marker), writes the data to a volume, and atomically updates the
    /// `IndexRecord` to point to the new volume location.
    ///
    /// Safe to call at startup or in a background task. Progress is tracked via
    /// the `inline_objects_count` diagnostic counter.
    ///
    /// # Returns
    ///
    /// The number of inline objects that were successfully migrated.
    pub async fn migrate_inline_objects(&self) -> Result<usize, StorageError> {
        let all_records = self.index_db.list_objects("", usize::MAX).await?;
        let mut migrated = 0usize;

        for (key, record) in all_records {
            // Only migrate inline objects (file_id == u32::MAX, not delete markers)
            if record.file_id != u32::MAX || record.is_delete_marker {
                continue;
            }

            // Decode the inline data
            let inline_data_str = match record.metadata.get("_inline_data") {
                Some(s) => s,
                None => continue,
            };
            let data = match base64::engine::general_purpose::STANDARD.decode(inline_data_str) {
                Ok(d) => d,
                Err(_) => continue, // Skip corrupted inline data
            };

            // Write data to volume
            let mut writer = self.volume_writer.write().await;
            let (volume_id, offset) = writer
                .write_blob(
                    &BlobHeader::new(key.len() as u32, data.len() as u64, crc32fast::hash(&data)),
                    &key,
                    &data,
                )
                .await?;

            if self.strict_sync {
                writer.sync().await?;
            }
            drop(writer);

            // Build updated record without _inline_data, pointing to volume
            let mut updated = record.clone();
            updated.file_id = volume_id;
            updated.offset = offset;
            updated.metadata.remove("_inline_data");

            // Register in dedup
            let content_hash = updated.content_hash;
            let dedup_op = self.deduplicator.make_register_op(content_hash, volume_id, offset)?;

            // Atomic batch: update record + register dedup
            let record_bytes = bincode::serialize(&updated)
                .map_err(|e| StorageError::Serialization(e.to_string()))?;
            let (keyspace, stripped_key) = self.index_db.route_key_id(&key);
            let ops = vec![
                BatchOp {
                    keyspace,
                    action: BatchAction::Put(stripped_key.as_bytes().to_vec(), record_bytes),
                },
                dedup_op,
            ];
            self.index_db.batch_write(ops).await?;

            migrated += 1;
        }

        // Update diagnostic counter
        let remaining = self
            .diagnostic_counters
            .inline_objects_count
            .load(Ordering::Relaxed)
            .saturating_sub(migrated as u64);
        self.diagnostic_counters
            .inline_objects_count
            .store(remaining, Ordering::Relaxed);

        Ok(migrated)
    }

    // ========================================================================
    // Delete Marker Volume Storage (Phase 5)
    // ========================================================================

    /// Writes a zero-length delete marker blob to volume so it can be discovered
    /// during recovery. Returns an `IndexRecord` with `is_delete_marker = true`
    /// and a valid `file_id` / `offset` pointing to the volume entry.
    async fn write_delete_marker_to_volume(
        &self,
        version_key: &str,
        version_id: String,
    ) -> Result<IndexRecord, StorageError> {
        let mut header = BlobHeader::new(version_key.len() as u32, 0, 0);
        header.flags.is_delete_marker = true;

        let mut writer = self.volume_writer.write().await;
        let (volume_id, offset) = writer.write_blob(&header, version_key, &[]).await?;
        if self.strict_sync {
            writer.sync().await?;
        }
        drop(writer);

        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64;
        Ok(IndexRecord {
            file_id: volume_id,
            offset,
            size: 0,
            etag: String::new(),
            content_type: String::new(),
            metadata: HashMap::new(),
            content_hash: [0u8; 32],
            created_at: now,
            modified_at: now,
            version_id: Some(version_id),
            is_delete_marker: true,
            retention_mode: None,
            retain_until_timestamp: None,
            legal_hold: false,
            layout: None,
            pool_id: 0,
            hlc_timestamp: 0,
            hlc_logical: 0,
            origin_node_id: 0,
            operation_id: 0,
        })
    }

    // ========================================================================
    // Versioning Helper Methods
    // ========================================================================

    /// Creates a versioned key for storing a specific version.
    ///
    /// Format: `bucket/key#version_id`
    fn make_version_key(&self, bucket: &str, key: &str, version_id: &str) -> String {
        format!("{}/{}#{}", bucket, key, version_id)
    }

    /// Retrieves the version list for an object.
    ///
    /// Uses the direct `versions` keyspace (Phase 3) with fallback to legacy
    /// format (Phase 2 IndexRecord with `_inline_data`) for backward compatibility
    /// during migration.
    async fn get_version_list(&self, bucket: &str, key: &str) -> Result<VersionList, StorageError> {
        // Phase 3: Read directly from versions keyspace
        let vl = self.index_db.get_version_list(bucket, key).await?;
        if !vl.is_empty() {
            return Ok(vl);
        }

        // Fallback: Check for legacy Phase 2 format (IndexRecord with _inline_data)
        let legacy_key = format!("__s4_versions_{}/{}", bucket, key);
        if let Some(record) = self.index_db.get(&legacy_key).await? {
            if let Some(json_b64) = record.metadata.get("_inline_data") {
                if let Ok(decoded) = base64::engine::general_purpose::STANDARD.decode(json_b64) {
                    if let Ok(legacy_vl) = serde_json::from_slice::<VersionList>(&decoded) {
                        return Ok(legacy_vl);
                    }
                }
            }
        }

        Ok(VersionList::new())
    }

    /// Checks if deduplicated content already exists and can be reused.
    ///
    /// Returns `Some((IndexRecord, BatchOp))` if content exists in a volume,
    /// where the `BatchOp` increments the dedup ref count atomically.
    /// Returns `None` if the data must be written fresh.
    fn try_reuse_existing(
        dedup: &Deduplicator,
        content_hash: [u8; 32],
        data_len: u64,
        etag: &str,
        content_type: &str,
    ) -> Result<Option<(IndexRecord, BatchOp)>, StorageError> {
        let existing = match dedup.check_existing(&content_hash)? {
            Some(loc) => loc,
            None => return Ok(None),
        };
        let (volume_id, offset) = existing;
        if volume_id == u32::MAX {
            // Inline objects should not be in deduplicator — treat as new
            return Ok(None);
        }
        // Create batch op to increment ref count (committed atomically with object write)
        let dedup_op = dedup.make_register_op(content_hash, volume_id, offset)?;
        let record = IndexRecord::new(
            volume_id,
            offset,
            data_len,
            content_hash,
            etag.to_string(),
            content_type.to_string(),
        );
        Ok(Some((record, dedup_op)))
    }

    /// Internal method to write object data and return the record + dedup batch op.
    ///
    /// Returns `(IndexRecord, etag, Option<BatchOp>)` where the `BatchOp` is a
    /// dedup registration that should be committed atomically with the object
    /// record via [`IndexDb::batch_write`].
    async fn write_object_data(
        &self,
        full_key: &str,
        data: &[u8],
        content_type: &str,
        metadata: &HashMap<String, String>,
    ) -> Result<(IndexRecord, String, Option<BatchOp>), StorageError> {
        let content_hash = Deduplicator::compute_hash(data);
        let etag = Self::compute_etag(data);

        // Check for dedup reuse (lock-free MVCC read)
        let reused = Self::try_reuse_existing(
            &self.deduplicator,
            content_hash,
            data.len() as u64,
            &etag,
            content_type,
        )?;

        // Phase 5: All new objects are written to volumes, regardless of size.
        // Legacy inline objects (file_id == u32::MAX) are still readable but no
        // longer created. See `migrate_inline_objects` for background migration.
        let (record, dedup_op) = if let Some((record, dedup_op)) = reused {
            (record, Some(dedup_op))
        } else {
            let mut writer = self.volume_writer.write().await;
            let (volume_id, offset) = writer
                .write_blob(
                    &BlobHeader::new(
                        full_key.len() as u32,
                        data.len() as u64,
                        crc32fast::hash(data),
                    ),
                    full_key,
                    data,
                )
                .await?;

            if self.strict_sync {
                writer.sync().await?;
            }

            drop(writer);

            // Create batch op for dedup registration (committed atomically with object)
            let dedup_op = self.deduplicator.make_register_op(content_hash, volume_id, offset)?;

            let record = IndexRecord::new(
                volume_id,
                offset,
                data.len() as u64,
                content_hash,
                etag.clone(),
                content_type.to_string(),
            );
            (record, Some(dedup_op))
        };

        let mut record = record;
        record.metadata.extend(metadata.clone());
        record.modified_at = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64;

        Ok((record, etag, dedup_op))
    }

    /// Reads object data from a record.
    ///
    /// Supports both volume-backed objects and legacy inline objects
    /// (Phase 5: new objects always go to volumes; inline read is kept
    /// for backward compatibility until all inline objects are migrated).
    async fn read_object_data(&self, record: &IndexRecord) -> Result<Vec<u8>, StorageError> {
        // Delete markers (both legacy with DELETE_MARKER_FILE_ID and Phase 5 with
        // real volume file_id) carry no data.
        if record.is_delete_marker {
            return Err(StorageError::DeleteMarker {
                key: String::new(),
                version_id: record.version_id.clone().unwrap_or_default(),
            });
        }

        if record.file_id == u32::MAX {
            // Legacy inline storage (Phase < 5)
            let inline_data_str = record
                .metadata
                .get("_inline_data")
                .ok_or_else(|| StorageError::InvalidData("Inline data not found".to_string()))?;
            base64::engine::general_purpose::STANDARD.decode(inline_data_str).map_err(|e| {
                StorageError::InvalidData(format!("Failed to decode inline data: {}", e))
            })
        } else {
            let reader = VolumeReader::new(&self.volumes_dir);
            let (_header, _read_key, data) =
                reader.read_blob(record.file_id, record.offset).await?;
            Ok(data)
        }
    }

    /// Recalculates the current version from the version list and returns
    /// batch operations for updating the current pointer.
    ///
    /// This method does NOT write to the database directly — it returns the
    /// batch operations so the caller can include them in an atomic batch.
    async fn recalculate_current_version_ops(
        &self,
        bucket: &str,
        key: &str,
        version_list: &mut VersionList,
    ) -> Result<Vec<BatchOp>, StorageError> {
        let full_key = self.make_key(bucket, key);
        let mut ops = Vec::new();

        // Find first non-delete-marker version
        for vid in version_list.versions.iter() {
            let version_key = self.make_version_key(bucket, key, vid);
            if let Some(record) = self.index_db.get(&version_key).await? {
                if !record.is_delete_marker {
                    version_list.set_current_version(Some(vid.clone()));
                    let record_bytes = bincode::serialize(&record)
                        .map_err(|e| StorageError::Serialization(e.to_string()))?;
                    ops.push(BatchOp {
                        keyspace: KeyspaceId::Objects,
                        action: BatchAction::Put(full_key.into_bytes(), record_bytes),
                    });
                    return Ok(ops);
                }
            }
        }

        // No non-delete-marker found, delete current pointer
        version_list.set_current_version(None);
        ops.push(BatchOp {
            keyspace: KeyspaceId::Objects,
            action: BatchAction::Delete(full_key.into_bytes()),
        });
        Ok(ops)
    }
}

// ============================================================================
// StateMachine implementation (Phase 6)
// ============================================================================

#[async_trait::async_trait]
impl crate::storage::engine::StateMachine for BitcaskStorageEngine {
    async fn apply(
        &self,
        entry: &crate::storage::journal::JournalEntry,
    ) -> Result<(), StorageError> {
        use crate::storage::journal::{BucketOp, IamOp};

        match &entry.event {
            JournalEventType::ObjectPut {
                bucket,
                key,
                version_id,
                record,
            } => {
                let full_key = match version_id {
                    Some(vid) => format!("{}/{}#{}", bucket, key, vid),
                    None => format!("{}/{}", bucket, key),
                };

                // Idempotency: check if already applied by comparing etag
                if let Some(existing) = self.index_db.get(&full_key).await? {
                    if existing.etag == record.etag && existing.created_at == record.created_at {
                        return Ok(());
                    }
                }

                self.index_db.put(&full_key, record).await?;

                // Update version list if versioned
                if let Some(vid) = version_id {
                    let mut vlist = self.index_db.get_version_list(bucket, key).await?;
                    if !vlist.versions.contains(vid) {
                        vlist.add_version(vid.clone(), false);
                        self.index_db.put_version_list(bucket, key, &vlist).await?;
                    }
                }

                // Register dedup entry
                if !record.is_delete_marker && record.content_hash != [0u8; 32] {
                    let dedup_op = self.deduplicator.make_register_op(
                        record.content_hash,
                        record.file_id,
                        record.offset,
                    )?;
                    self.index_db.batch_write(vec![dedup_op]).await?;
                }
            }

            JournalEventType::ObjectDelete {
                bucket,
                key,
                version_id,
            } => {
                let full_key = match version_id {
                    Some(vid) => format!("{}/{}#{}", bucket, key, vid),
                    None => format!("{}/{}", bucket, key),
                };

                // Idempotency: skip if already deleted
                if let Some(existing) = self.index_db.get(&full_key).await? {
                    // Unregister dedup before deleting
                    if !existing.is_delete_marker && existing.content_hash != [0u8; 32] {
                        if let Ok(Some(op)) =
                            self.deduplicator.make_unregister_op(&existing.content_hash)
                        {
                            self.index_db.batch_write(vec![op]).await?;
                        }
                    }

                    self.index_db.delete(&full_key).await?;

                    // Update version list if versioned
                    if let Some(vid) = version_id {
                        let mut vlist = self.index_db.get_version_list(bucket, key).await?;
                        vlist.remove_version(vid);
                        self.index_db.put_version_list(bucket, key, &vlist).await?;
                    }
                }
            }

            JournalEventType::DeleteMarkerCreated {
                bucket,
                key,
                marker_version_id,
                record,
            } => {
                let full_key = format!("{}/{}#{}", bucket, key, marker_version_id);

                // Idempotency
                if self.index_db.get(&full_key).await?.is_some() {
                    return Ok(());
                }

                self.index_db.put(&full_key, record).await?;

                let mut vlist = self.index_db.get_version_list(bucket, key).await?;
                if !vlist.versions.contains(marker_version_id) {
                    vlist.add_version(marker_version_id.clone(), true);
                    self.index_db.put_version_list(bucket, key, &vlist).await?;
                }
            }

            JournalEventType::MetadataUpdate {
                bucket,
                key,
                version_id,
                updated_fields,
            } => {
                let full_key = match version_id {
                    Some(vid) => format!("{}/{}#{}", bucket, key, vid),
                    None => format!("{}/{}", bucket, key),
                };

                if let Some(mut record) = self.index_db.get(&full_key).await? {
                    crate::storage::recovery::apply_metadata_updates_from_journal(
                        &mut record,
                        updated_fields,
                    );
                    self.index_db.put(&full_key, &record).await?;
                }
            }

            JournalEventType::BucketOperation { bucket, op } => {
                let marker_key = format!("__system__/__s4_bucket_marker_{}", bucket);
                match op {
                    BucketOp::Created => {
                        // Idempotency
                        if self.index_db.get(&marker_key).await?.is_none() {
                            let record =
                                IndexRecord::new(0, 0, 0, [0u8; 32], String::new(), String::new());
                            self.index_db.put(&marker_key, &record).await?;
                        }
                    }
                    BucketOp::Deleted => {
                        self.index_db.delete(&marker_key).await?;
                    }
                    BucketOp::VersioningChanged { .. } => {
                        // Versioning status is part of bucket config, not the marker record.
                    }
                }
            }

            JournalEventType::IamOperation { op } => match op {
                IamOp::UserCreated { user_id } => {
                    let iam_key = format!("__system__/__s4_iam_user_{}", user_id);
                    if self.index_db.get(&iam_key).await?.is_none() {
                        let record =
                            IndexRecord::new(0, 0, 0, [0u8; 32], String::new(), String::new());
                        self.index_db.put(&iam_key, &record).await?;
                    }
                }
                IamOp::UserDeleted { user_id } => {
                    let iam_key = format!("__system__/__s4_iam_user_{}", user_id);
                    self.index_db.delete(&iam_key).await?;
                }
                IamOp::AccessKeyCreated { access_key_id } => {
                    let iam_key = format!("__system__/__s4_iam_key_{}", access_key_id);
                    if self.index_db.get(&iam_key).await?.is_none() {
                        let record =
                            IndexRecord::new(0, 0, 0, [0u8; 32], String::new(), String::new());
                        self.index_db.put(&iam_key, &record).await?;
                    }
                }
                IamOp::AccessKeyDeleted { access_key_id } => {
                    let iam_key = format!("__system__/__s4_iam_key_{}", access_key_id);
                    self.index_db.delete(&iam_key).await?;
                }
            },
        }

        Ok(())
    }

    async fn snapshot(&self) -> Result<crate::storage::engine::Snapshot, StorageError> {
        use crate::storage::engine::{Snapshot, VolumeFileInfo};

        let pg = self.journal.placement_group();
        let sequence = self.journal.current_sequence();

        // Collect volume file information
        let mut volume_files = Vec::new();
        let mut entries = tokio::fs::read_dir(&self.volumes_dir).await.map_err(StorageError::Io)?;
        while let Some(entry) = entries.next_entry().await.map_err(StorageError::Io)? {
            let path = entry.path();
            if path.is_file() {
                if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
                    if filename.starts_with("volume_") && filename.ends_with(".dat") {
                        if let Some(id_str) =
                            filename.strip_prefix("volume_").and_then(|s| s.strip_suffix(".dat"))
                        {
                            if let Ok(volume_id) = id_str.parse::<u32>() {
                                let meta = entry.metadata().await.map_err(StorageError::Io)?;
                                volume_files.push(VolumeFileInfo {
                                    volume_id,
                                    size_bytes: meta.len(),
                                });
                            }
                        }
                    }
                }
            }
        }
        volume_files.sort_by_key(|v| v.volume_id);

        // Dump all keyspaces
        let keyspaces = self.index_db.dump_keyspaces().await?;

        Ok(Snapshot {
            placement_group: pg,
            sequence,
            timestamp: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64,
            volume_files,
            keyspaces,
        })
    }

    async fn restore(
        &self,
        snapshot: crate::storage::engine::Snapshot,
    ) -> Result<(), StorageError> {
        // Restore all keyspaces from the snapshot
        self.index_db.restore_keyspaces(snapshot.keyspaces).await?;
        Ok(())
    }
}

// ============================================================================
// Test-only helpers for state inspection
// ============================================================================

/// Returns the index database (shared Arc).
///
/// Used by the compaction worker to scan and update IndexRecords
/// when blobs are relocated to new volumes.
impl BitcaskStorageEngine {
    /// Returns a reference to the internal index database.
    pub fn index_db(&self) -> &Arc<IndexDb> {
        &self.index_db
    }
}

#[cfg(test)]
impl BitcaskStorageEngine {
    /// Test helper: retrieves the version list for an object.
    pub(crate) async fn test_get_version_list(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<VersionList, StorageError> {
        self.get_version_list(bucket, key).await
    }

    /// Test helper: constructs a version key.
    pub(crate) fn test_make_version_key(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> String {
        self.make_version_key(bucket, key, version_id)
    }
}

// ============================================================================
// Native Composite Multipart Upload
// ============================================================================

/// Result of a native part upload (hashes computed during streaming write).
pub struct NativePartResult {
    /// The durable part record stored in fjall.
    pub record: MultipartPartRecord,
    /// MD5 hex string ETag for S3 compatibility.
    pub etag: String,
}

/// Wraps an `AsyncRead` to compute both SHA-256 and MD5 as data passes through.
struct Sha256Md5TeeReader<'a, R> {
    inner: R,
    sha256: &'a mut sha2::Sha256,
    md5: &'a mut md5::Md5,
}

impl<R: tokio::io::AsyncRead + Unpin> tokio::io::AsyncRead for Sha256Md5TeeReader<'_, R> {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        use md5::Digest as Md5Digest;
        use sha2::Digest as Sha2Digest;
        let this = self.get_mut();
        let before = buf.filled().len();
        let result = std::pin::Pin::new(&mut this.inner).poll_read(cx, buf);
        if let std::task::Poll::Ready(Ok(())) = &result {
            let after = buf.filled().len();
            if after > before {
                let chunk = &buf.filled()[before..after];
                Sha2Digest::update(this.sha256, chunk);
                Md5Digest::update(this.md5, chunk);
            }
        }
        result
    }
}

impl BitcaskStorageEngine {
    // -- Blob key for segment blobs (internal, not user-visible) ---------------

    fn make_segment_blob_key(upload_id: &str, part_number: u32) -> String {
        format!("__mp_seg_{}_{:05}", upload_id, part_number)
    }

    fn make_manifest_blob_key(upload_id: &str) -> String {
        format!("__mp_manifest_{}", upload_id)
    }

    // -- Session management ---------------------------------------------------

    /// Creates a durable multipart upload session in fjall.
    pub async fn create_multipart_session(
        &self,
        upload_id: &str,
        bucket: &str,
        key: &str,
        content_type: &str,
        metadata: &HashMap<String, String>,
    ) -> Result<(), StorageError> {
        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64;
        let session = MultipartUploadSession {
            upload_id: upload_id.to_string(),
            bucket: bucket.to_string(),
            key: key.to_string(),
            content_type: content_type.to_string(),
            metadata: metadata.clone(),
            state: MultipartUploadState::Open,
            created_at: now,
            updated_at: now,
        };
        let value =
            bincode::serialize(&session).map_err(|e| StorageError::Serialization(e.to_string()))?;
        self.index_db
            .batch_write(vec![BatchOp {
                keyspace: KeyspaceId::MultipartSessions,
                action: BatchAction::Put(upload_id.as_bytes().to_vec(), value),
            }])
            .await
    }

    /// Retrieves a multipart upload session.
    pub async fn get_multipart_session(
        &self,
        upload_id: &str,
    ) -> Result<MultipartUploadSession, StorageError> {
        let ks = self.index_db.multipart_sessions_keyspace();
        let value = ks
            .get(upload_id.as_bytes())
            .map_err(|e| StorageError::Database(e.to_string()))?
            .ok_or_else(|| {
                StorageError::InvalidData(format!("Upload session not found: {}", upload_id))
            })?;
        bincode::deserialize(&value).map_err(|e| StorageError::Serialization(e.to_string()))
    }

    /// Transitions a session to Completing state. Returns the session if successful.
    pub async fn mark_session_completing(
        &self,
        upload_id: &str,
    ) -> Result<MultipartUploadSession, StorageError> {
        let mut session = self.get_multipart_session(upload_id).await?;
        if session.state != MultipartUploadState::Open {
            return Err(StorageError::InvalidData(
                "Upload is already being completed or does not exist".to_string(),
            ));
        }
        session.state = MultipartUploadState::Completing;
        session.updated_at = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64;
        let value =
            bincode::serialize(&session).map_err(|e| StorageError::Serialization(e.to_string()))?;
        self.index_db
            .batch_write(vec![BatchOp {
                keyspace: KeyspaceId::MultipartSessions,
                action: BatchAction::Put(upload_id.as_bytes().to_vec(), value),
            }])
            .await?;
        Ok(session)
    }

    /// Reverts a session from `Completing` back to `Open`.
    ///
    /// Used when validation fails after `mark_session_completing` so the
    /// upload can still be aborted or retried.
    pub async fn revert_session_to_open(&self, upload_id: &str) -> Result<(), StorageError> {
        let mut session = self.get_multipart_session(upload_id).await?;
        if session.state != MultipartUploadState::Completing {
            return Ok(()); // nothing to revert
        }
        session.state = MultipartUploadState::Open;
        session.updated_at = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64;
        let value =
            bincode::serialize(&session).map_err(|e| StorageError::Serialization(e.to_string()))?;
        self.index_db
            .batch_write(vec![BatchOp {
                keyspace: KeyspaceId::MultipartSessions,
                action: BatchAction::Put(upload_id.as_bytes().to_vec(), value),
            }])
            .await?;
        Ok(())
    }

    // -- Part upload (streaming to volume) ------------------------------------

    /// Uploads a part by streaming directly to a volume file.
    ///
    /// Computes SHA-256, MD5, and CRC32 in a single streaming pass.
    /// Creates a staged `BlobRefEntry` and a `MultipartPartRecord` in fjall.
    /// Returns the part record with ETag for the S3 response.
    pub async fn upload_part_streaming(
        &self,
        upload_id: &str,
        part_number: u32,
        reader: Box<dyn tokio::io::AsyncRead + Unpin + Send>,
        content_length: u64,
    ) -> Result<NativePartResult, StorageError> {
        use sha2::Digest;
        const BUFFER_SIZE: usize = 1024 * 1024; // 1 MB

        let blob_key = Self::make_segment_blob_key(upload_id, part_number);

        // Triple-tee: SHA-256 + MD5 computed in tee reader, CRC32 by volume writer.
        let mut sha256_hasher = sha2::Sha256::new();
        let mut md5_hasher = {
            use md5::Digest;
            md5::Md5::new()
        };

        let tee_reader = Sha256Md5TeeReader {
            inner: reader,
            sha256: &mut sha256_hasher,
            md5: &mut md5_hasher,
        };

        let (volume_id, offset, crc32) = {
            let mut writer = self.volume_writer.write().await;
            let result = writer
                .write_blob_streaming(&blob_key, content_length, tee_reader, BUFFER_SIZE)
                .await?;
            if self.strict_sync {
                writer.sync().await?;
            }
            result
        };

        let content_hash: [u8; 32] = {
            use sha2::Digest;
            sha256_hasher.finalize().into()
        };
        let md5_result: [u8; 16] = {
            use md5::Digest;
            md5_hasher.finalize().into()
        };
        let etag_md5_hex = hex::encode(md5_result);
        let etag_md5_bytes: [u8; 16] = md5_result;
        let blob_id = BlobId::from_content_hash(content_hash);

        // Check for dedup: if identical content already exists, use existing location.
        let (final_vol, final_off) = if let Some((existing_vol, existing_off)) =
            self.deduplicator.check_existing(&content_hash)?
        {
            // Duplicate detected — written blob is dead space (compactor reclaims).
            (existing_vol, existing_off)
        } else {
            (volume_id, offset)
        };

        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64;
        let part_record = MultipartPartRecord {
            upload_id: upload_id.to_string(),
            part_number,
            size: content_length,
            etag_md5_hex: etag_md5_hex.clone(),
            etag_md5_bytes,
            physical_hash: content_hash,
            blob_id,
            crc32,
            created_at: now,
        };

        // Build atomic batch:
        // 1. MultipartPartRecord in MultipartParts keyspace
        // 2. BlobRefEntry with staged ref in BlobRefs keyspace
        // 3. Dedup registration
        let part_key = format!("{}_{:05}", upload_id, part_number);
        let part_value = bincode::serialize(&part_record)
            .map_err(|e| StorageError::Serialization(e.to_string()))?;

        // Create or update BlobRefEntry (increment staged ref count)
        let blob_ref = self.get_or_create_blob_ref(&blob_id, final_vol, final_off)?;
        let updated_ref = BlobRefEntry {
            location: blob_ref.location,
            ref_count_committed: blob_ref.ref_count_committed,
            ref_count_staged: blob_ref.ref_count_staged + 1,
        };
        let blob_ref_value = bincode::serialize(&updated_ref)
            .map_err(|e| StorageError::Serialization(e.to_string()))?;

        let dedup_op = self.deduplicator.make_register_op(content_hash, final_vol, final_off)?;

        // Check if this part_number was already uploaded (overwrite case).
        // If so, we need to decrement the old part's staged ref.
        let mut ops = vec![
            BatchOp {
                keyspace: KeyspaceId::MultipartParts,
                action: BatchAction::Put(part_key.as_bytes().to_vec(), part_value),
            },
            BatchOp {
                keyspace: KeyspaceId::BlobRefs,
                action: BatchAction::Put(blob_id.as_bytes().to_vec(), blob_ref_value),
            },
            dedup_op,
        ];

        // Handle part overwrite: decrement old part's staged ref
        if let Some(old_part) = self.get_multipart_part(upload_id, part_number)? {
            if old_part.blob_id != blob_id {
                if let Some(old_ref_op) = self.decrement_staged_ref_op(&old_part.blob_id)? {
                    ops.push(old_ref_op);
                }
            }
        }

        self.index_db.batch_write(ops).await?;

        Ok(NativePartResult {
            record: part_record,
            etag: etag_md5_hex,
        })
    }

    // -- Part retrieval -------------------------------------------------------

    /// Reads a single part record from the MultipartParts keyspace.
    fn get_multipart_part(
        &self,
        upload_id: &str,
        part_number: u32,
    ) -> Result<Option<MultipartPartRecord>, StorageError> {
        let part_key = format!("{}_{:05}", upload_id, part_number);
        let ks = self.index_db.multipart_parts_keyspace();
        match ks.get(part_key.as_bytes()) {
            Ok(Some(value)) => {
                let record: MultipartPartRecord = bincode::deserialize(&value)
                    .map_err(|e| StorageError::Serialization(e.to_string()))?;
                Ok(Some(record))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(StorageError::Database(e.to_string())),
        }
    }

    /// Lists all parts for an upload, sorted by part number.
    pub fn list_multipart_parts(
        &self,
        upload_id: &str,
    ) -> Result<Vec<MultipartPartRecord>, StorageError> {
        let ks = self.index_db.multipart_parts_keyspace();
        let prefix = format!("{}_", upload_id);
        let mut parts = Vec::new();
        for guard in ks.prefix(prefix.as_bytes()) {
            let (_, value) =
                guard.into_inner().map_err(|e| StorageError::Database(e.to_string()))?;
            let record: MultipartPartRecord = bincode::deserialize(&value)
                .map_err(|e| StorageError::Serialization(e.to_string()))?;
            parts.push(record);
        }
        parts.sort_by_key(|p| p.part_number);
        Ok(parts)
    }

    // -- BlobRef helpers ------------------------------------------------------

    /// Gets or creates a BlobRefEntry for a blob.
    fn get_or_create_blob_ref(
        &self,
        blob_id: &BlobId,
        volume_id: u32,
        offset: u64,
    ) -> Result<BlobRefEntry, StorageError> {
        let ks = self.index_db.blob_refs_keyspace();
        match ks.get(blob_id.as_bytes()) {
            Ok(Some(value)) => {
                bincode::deserialize(&value).map_err(|e| StorageError::Serialization(e.to_string()))
            }
            Ok(None) => Ok(BlobRefEntry {
                location: BlobLocation { volume_id, offset },
                ref_count_committed: 0,
                ref_count_staged: 0,
            }),
            Err(e) => Err(StorageError::Database(e.to_string())),
        }
    }

    /// Creates a batch op to decrement staged ref count for a blob.
    /// Returns None if the blob ref doesn't exist.
    fn decrement_staged_ref_op(&self, blob_id: &BlobId) -> Result<Option<BatchOp>, StorageError> {
        let ks = self.index_db.blob_refs_keyspace();
        match ks.get(blob_id.as_bytes()) {
            Ok(Some(value)) => {
                let mut entry: BlobRefEntry = bincode::deserialize(&value)
                    .map_err(|e| StorageError::Serialization(e.to_string()))?;
                entry.ref_count_staged = entry.ref_count_staged.saturating_sub(1);
                let new_value = bincode::serialize(&entry)
                    .map_err(|e| StorageError::Serialization(e.to_string()))?;
                Ok(Some(BatchOp {
                    keyspace: KeyspaceId::BlobRefs,
                    action: BatchAction::Put(blob_id.as_bytes().to_vec(), new_value),
                }))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(StorageError::Database(e.to_string())),
        }
    }

    /// Creates a batch operation to decrement the committed ref count for a blob.
    fn decrement_committed_ref_op(
        &self,
        blob_id: &BlobId,
    ) -> Result<Option<BatchOp>, StorageError> {
        let ks = self.index_db.blob_refs_keyspace();
        match ks.get(blob_id.as_bytes()) {
            Ok(Some(value)) => {
                let mut entry: BlobRefEntry = bincode::deserialize(&value)
                    .map_err(|e| StorageError::Serialization(e.to_string()))?;
                entry.ref_count_committed = entry.ref_count_committed.saturating_sub(1);
                let new_value = bincode::serialize(&entry)
                    .map_err(|e| StorageError::Serialization(e.to_string()))?;
                Ok(Some(BatchOp {
                    keyspace: KeyspaceId::BlobRefs,
                    action: BatchAction::Put(blob_id.as_bytes().to_vec(), new_value),
                }))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(StorageError::Database(e.to_string())),
        }
    }

    /// Collects batch operations to decrement committed refs for all segments
    /// in a composite object (including the manifest blob itself).
    async fn composite_delete_ref_ops(
        &self,
        record: &IndexRecord,
    ) -> Result<Vec<BatchOp>, StorageError> {
        let mut ops = Vec::new();
        if let Some(manifest_blob_id) = record.composite_manifest_blob_id() {
            // Load manifest to find all segment BlobIds
            let manifest = self.load_composite_manifest(&manifest_blob_id).await?;
            for seg in &manifest.segments {
                if let Some(op) = self.decrement_committed_ref_op(&seg.blob_id)? {
                    ops.push(op);
                }
            }
            // Also decrement the manifest blob itself
            if let Some(op) = self.decrement_committed_ref_op(&manifest_blob_id)? {
                ops.push(op);
            }
        }
        Ok(ops)
    }

    // -- Complete multipart (metadata-only) -----------------------------------

    /// Completes a multipart upload by publishing a CompositeManifest.
    ///
    /// This is O(num_parts), NOT O(total_bytes). No part data is re-read or
    /// re-written. The method:
    /// 1. Builds a CompositeManifest from the validated part records
    /// 2. Serializes and writes the small manifest blob to volume
    /// 3. Creates an IndexRecord with ObjectLayout::CompositeManifest
    /// 4. Atomically: publishes object + converts staged→committed refs +
    ///    cleans up session/part metadata
    #[allow(clippy::too_many_arguments)]
    pub async fn complete_multipart_native(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        selected_parts: &[MultipartPartRecord],
        s3_etag: &str,
        content_type: &str,
        metadata: &HashMap<String, String>,
    ) -> Result<IndexRecord, StorageError> {
        let total_size: u64 = selected_parts.iter().map(|p| p.size).sum();
        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64;

        // Build CompositeManifest
        let segments: Vec<ManifestSegmentRef> = selected_parts
            .iter()
            .map(|p| ManifestSegmentRef {
                part_number: p.part_number,
                size: p.size,
                physical_hash: p.physical_hash,
                blob_id: p.blob_id,
                crc32: p.crc32,
            })
            .collect();

        let manifest = CompositeManifest {
            format_version: MANIFEST_FORMAT_VERSION,
            total_size,
            part_count: selected_parts.len() as u32,
            multipart_etag: s3_etag.to_string(),
            whole_object_sha256: None, // Too expensive for large objects
            segments,
            bucket: bucket.to_string(),
            key: key.to_string(),
        };

        // Serialize manifest and write as a small blob to volume
        let manifest_bytes = bincode::serialize(&manifest)
            .map_err(|e| StorageError::Serialization(e.to_string()))?;
        let manifest_hash = {
            use sha2::Digest;
            let mut hasher = sha2::Sha256::new();
            hasher.update(&manifest_bytes);
            let h: [u8; 32] = hasher.finalize().into();
            h
        };
        let manifest_blob_id = BlobId::from_content_hash(manifest_hash);
        let manifest_blob_key = Self::make_manifest_blob_key(upload_id);

        let (manifest_vol, manifest_off) = {
            let mut writer = self.volume_writer.write().await;
            let header = BlobHeader::new(
                manifest_blob_key.len() as u32,
                manifest_bytes.len() as u64,
                crc32fast::hash(&manifest_bytes),
            );
            let (vol, off) =
                writer.write_blob(&header, &manifest_blob_key, &manifest_bytes).await?;
            if self.strict_sync {
                writer.sync().await?;
            }
            (vol, off)
        };

        // Build IndexRecord with CompositeManifest layout
        use crate::types::composite::ObjectLayout;
        let mut record = IndexRecord::new(
            manifest_vol,
            manifest_off,
            total_size,
            [0u8; 32], // No whole-object hash for composite
            s3_etag.to_string(),
            content_type.to_string(),
        );
        record.metadata.extend(metadata.clone());
        record.modified_at = now;
        record.layout = Some(ObjectLayout::CompositeManifest {
            manifest_blob_id,
            manifest_blob_len: manifest_bytes.len() as u32,
            part_count: selected_parts.len() as u32,
        });

        // Build atomic batch with all operations:
        let full_key = self.make_key(bucket, key);
        let record_bytes =
            bincode::serialize(&record).map_err(|e| StorageError::Serialization(e.to_string()))?;
        let (keyspace, stripped_key) = self.index_db.route_key_id(&full_key);

        let mut ops = vec![
            // 1. Publish the object IndexRecord
            BatchOp {
                keyspace,
                action: BatchAction::Put(stripped_key.as_bytes().to_vec(), record_bytes),
            },
            // 2. Journal entry
            self.journal.make_journal_entry_op(JournalEventType::ObjectPut {
                bucket: bucket.to_string(),
                key: key.to_string(),
                version_id: None,
                record: record.clone(),
            })?,
        ];

        // 3. Manifest blob ref (committed)
        let manifest_ref = BlobRefEntry {
            location: BlobLocation {
                volume_id: manifest_vol,
                offset: manifest_off,
            },
            ref_count_committed: 1,
            ref_count_staged: 0,
        };
        let manifest_ref_value = bincode::serialize(&manifest_ref)
            .map_err(|e| StorageError::Serialization(e.to_string()))?;
        ops.push(BatchOp {
            keyspace: KeyspaceId::BlobRefs,
            action: BatchAction::Put(manifest_blob_id.as_bytes().to_vec(), manifest_ref_value),
        });

        // 4. Convert staged→committed refs for selected parts
        for part in selected_parts {
            let blob_ref = self.get_or_create_blob_ref(
                &part.blob_id,
                0,
                0, // won't be used since it must exist
            )?;
            let updated = BlobRefEntry {
                location: blob_ref.location,
                ref_count_committed: blob_ref.ref_count_committed + 1,
                ref_count_staged: blob_ref.ref_count_staged.saturating_sub(1),
            };
            let value = bincode::serialize(&updated)
                .map_err(|e| StorageError::Serialization(e.to_string()))?;
            ops.push(BatchOp {
                keyspace: KeyspaceId::BlobRefs,
                action: BatchAction::Put(part.blob_id.as_bytes().to_vec(), value),
            });
        }

        // 5. Delete session metadata
        ops.push(BatchOp {
            keyspace: KeyspaceId::MultipartSessions,
            action: BatchAction::Delete(upload_id.as_bytes().to_vec()),
        });

        // 6. Delete all part records for this upload
        let all_parts = self.list_multipart_parts(upload_id)?;
        for part in &all_parts {
            let part_key = format!("{}_{:05}", upload_id, part.part_number);
            ops.push(BatchOp {
                keyspace: KeyspaceId::MultipartParts,
                action: BatchAction::Delete(part_key.as_bytes().to_vec()),
            });

            // 7. Decrement staged refs for unselected parts
            let is_selected = selected_parts.iter().any(|sp| sp.part_number == part.part_number);
            if !is_selected {
                if let Some(dec_op) = self.decrement_staged_ref_op(&part.blob_id)? {
                    ops.push(dec_op);
                }
            }
        }

        self.index_db.batch_write(ops).await?;

        Ok(record)
    }

    // -- Abort multipart (cleanup staged refs) --------------------------------

    /// Aborts a multipart upload: decrements all staged refs and cleans up metadata.
    pub async fn abort_multipart_native(&self, upload_id: &str) -> Result<(), StorageError> {
        let parts = self.list_multipart_parts(upload_id)?;

        let mut ops = vec![
            // Delete session
            BatchOp {
                keyspace: KeyspaceId::MultipartSessions,
                action: BatchAction::Delete(upload_id.as_bytes().to_vec()),
            },
        ];

        // Delete part records and decrement staged refs
        for part in &parts {
            let part_key = format!("{}_{:05}", upload_id, part.part_number);
            ops.push(BatchOp {
                keyspace: KeyspaceId::MultipartParts,
                action: BatchAction::Delete(part_key.as_bytes().to_vec()),
            });
            if let Some(dec_op) = self.decrement_staged_ref_op(&part.blob_id)? {
                ops.push(dec_op);
            }
        }

        self.index_db.batch_write(ops).await
    }

    // -- Composite manifest loading (for read path) ---------------------------

    /// Loads a CompositeManifest from volume storage given its BlobId.
    pub async fn load_composite_manifest(
        &self,
        manifest_blob_id: &BlobId,
    ) -> Result<CompositeManifest, StorageError> {
        // Resolve BlobId → BlobLocation via BlobRefs keyspace
        let ks = self.index_db.blob_refs_keyspace();
        let ref_bytes = ks
            .get(manifest_blob_id.as_bytes())
            .map_err(|e| StorageError::Database(e.to_string()))?
            .ok_or_else(|| {
                StorageError::InvalidData(format!(
                    "Manifest blob ref not found: {}",
                    manifest_blob_id
                ))
            })?;
        let blob_ref: BlobRefEntry = bincode::deserialize(&ref_bytes)
            .map_err(|e| StorageError::Serialization(e.to_string()))?;

        // Read manifest blob from volume
        let reader = VolumeReader::new(&self.volumes_dir);
        let (_header, _key, data) =
            reader.read_blob(blob_ref.location.volume_id, blob_ref.location.offset).await?;

        // Deserialize
        let manifest: CompositeManifest = bincode::deserialize(&data).map_err(|e| {
            StorageError::Serialization(format!("Failed to deserialize CompositeManifest: {}", e))
        })?;
        Ok(manifest)
    }

    /// Resolves a segment's BlobId to its physical disk location.
    pub fn resolve_blob_location(&self, blob_id: &BlobId) -> Result<BlobLocation, StorageError> {
        let ks = self.index_db.blob_refs_keyspace();
        let ref_bytes = ks
            .get(blob_id.as_bytes())
            .map_err(|e| StorageError::Database(e.to_string()))?
            .ok_or_else(|| StorageError::InvalidData(format!("Blob ref not found: {}", blob_id)))?;
        let blob_ref: BlobRefEntry = bincode::deserialize(&ref_bytes)
            .map_err(|e| StorageError::Serialization(e.to_string()))?;
        Ok(blob_ref.location)
    }
}

#[async_trait::async_trait]
impl StorageEngine for BitcaskStorageEngine {
    async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        data: &[u8],
        content_type: &str,
        metadata: &HashMap<String, String>,
    ) -> Result<String, StorageError> {
        let full_key = self.make_key(bucket, key);

        // Write object data and get dedup batch op
        let (record, etag, dedup_op) =
            self.write_object_data(&full_key, data, content_type, metadata).await?;

        // ATOMIC BATCH: object record + dedup registration + journal entry
        let record_bytes =
            bincode::serialize(&record).map_err(|e| StorageError::Serialization(e.to_string()))?;
        let (keyspace, stripped_key) = self.index_db.route_key_id(&full_key);
        let mut ops = vec![BatchOp {
            keyspace,
            action: BatchAction::Put(stripped_key.as_bytes().to_vec(), record_bytes),
        }];
        if let Some(op) = dedup_op {
            ops.push(op);
        }
        ops.push(
            self.journal.make_journal_entry_op(JournalEventType::ObjectPut {
                bucket: bucket.to_string(),
                key: key.to_string(),
                version_id: None,
                record: record.clone(),
            })?,
        );
        self.index_db.batch_write(ops).await?;

        Ok(etag)
    }

    async fn get_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<(Vec<u8>, IndexRecord), StorageError> {
        let full_key = self.make_key(bucket, key);

        // Get metadata from index
        let record =
            self.index_db
                .get(&full_key)
                .await?
                .ok_or_else(|| StorageError::ObjectNotFound {
                    key: full_key.clone(),
                })?;

        if record.is_composite() {
            // Composite object: assemble data from segment blobs via streaming reader
            use crate::storage::engine::ReadOptions;
            use tokio::io::AsyncReadExt;
            let mut stream =
                self.open_composite_stream(record.clone(), ReadOptions { range: None }).await?;
            let mut data = Vec::with_capacity(record.size as usize);
            stream.body.read_to_end(&mut data).await.map_err(StorageError::Io)?;
            Ok((data, record))
        } else {
            // Single blob: read directly and verify checksum
            let data = self.read_object_data(&record).await?;
            let computed_hash = Deduplicator::compute_hash(&data);
            if computed_hash != record.content_hash {
                return Err(StorageError::ChecksumMismatch { key: full_key });
            }
            Ok((data, record))
        }
    }

    async fn delete_object(&self, bucket: &str, key: &str) -> Result<(), StorageError> {
        let full_key = self.make_key(bucket, key);

        // ATOMIC BATCH: index delete + dedup unregister
        let (keyspace, stripped_key) = self.index_db.route_key_id(&full_key);
        let mut ops = vec![BatchOp {
            keyspace,
            action: BatchAction::Delete(stripped_key.as_bytes().to_vec()),
        }];

        // Unregister from dedup if not inline; handle composite objects
        if let Some(record) = self.index_db.get(&full_key).await? {
            if record.is_composite() {
                // Composite object: decrement committed refs for all segments + manifest
                let mut ref_ops = self.composite_delete_ref_ops(&record).await?;
                ops.append(&mut ref_ops);
            } else if record.file_id != u32::MAX {
                if let Some(dedup_op) =
                    self.deduplicator.make_unregister_op(&record.content_hash)?
                {
                    ops.push(dedup_op);
                }
            }
        }

        // Journal entry
        ops.push(
            self.journal.make_journal_entry_op(JournalEventType::ObjectDelete {
                bucket: bucket.to_string(),
                key: key.to_string(),
                version_id: None,
            })?,
        );

        self.index_db.batch_write(ops).await?;

        Ok(())
    }

    async fn head_object(&self, bucket: &str, key: &str) -> Result<IndexRecord, StorageError> {
        let full_key = self.make_key(bucket, key);

        self.index_db
            .get(&full_key)
            .await?
            .ok_or(StorageError::ObjectNotFound { key: full_key })
    }

    async fn list_objects(
        &self,
        bucket: &str,
        prefix: &str,
        max_keys: usize,
    ) -> Result<Vec<(String, IndexRecord)>, StorageError> {
        let bucket_prefix = format!("{}/{}", bucket, prefix);
        let results = self.index_db.list(&bucket_prefix, max_keys).await?;

        // Remove bucket prefix from keys in results
        let results: Vec<(String, IndexRecord)> = results
            .into_iter()
            .map(|(key, record)| {
                let key_without_bucket =
                    key.strip_prefix(&format!("{}/", bucket)).unwrap_or(&key).to_string();
                (key_without_bucket, record)
            })
            .collect();

        Ok(results)
    }

    async fn list_objects_after(
        &self,
        bucket: &str,
        prefix: &str,
        start_after: &str,
        max_keys: usize,
    ) -> Result<Vec<(String, IndexRecord)>, StorageError> {
        let bucket_prefix = format!("{}/{}", bucket, prefix);
        let full_start_after = format!("{}/{}", bucket, start_after);
        let results = self
            .index_db
            .list_after(&bucket_prefix, Some(&full_start_after), max_keys)
            .await?;

        // Remove bucket prefix from keys in results
        let results: Vec<(String, IndexRecord)> = results
            .into_iter()
            .map(|(key, record)| {
                let key_without_bucket =
                    key.strip_prefix(&format!("{}/", bucket)).unwrap_or(&key).to_string();
                (key_without_bucket, record)
            })
            .collect();

        Ok(results)
    }

    // ========================================================================
    // Versioning Methods Implementation (Phase 3: Atomic Batches)
    // ========================================================================

    async fn put_object_versioned(
        &self,
        bucket: &str,
        key: &str,
        data: &[u8],
        content_type: &str,
        metadata: &HashMap<String, String>,
        versioning_status: VersioningStatus,
    ) -> Result<(String, Option<String>), StorageError> {
        match versioning_status {
            VersioningStatus::Unversioned => {
                // Standard put - no versioning
                let etag = self.put_object(bucket, key, data, content_type, metadata).await?;
                Ok((etag, None))
            }
            VersioningStatus::Enabled => {
                // Generate new version ID
                let version_id = generate_version_id();
                let full_key = self.make_key(bucket, key);
                let version_key = self.make_version_key(bucket, key, &version_id);

                // Write object data to volume (or inline)
                let (mut record, etag, dedup_op) =
                    self.write_object_data(&version_key, data, content_type, metadata).await?;
                record.version_id = Some(version_id.clone());

                // Prepare version list
                let mut version_list = self.get_version_list(bucket, key).await?;
                version_list.add_version(version_id.clone(), false);

                // Serialize all data for the atomic batch
                let record_bytes = bincode::serialize(&record)
                    .map_err(|e| StorageError::Serialization(e.to_string()))?;
                let vl_bytes = serde_json::to_vec(&version_list)
                    .map_err(|e| StorageError::Serialization(e.to_string()))?;

                // ATOMIC BATCH: version record + version list + current pointer + dedup
                // All writes succeed or fail together. No crash windows.
                let mut ops = vec![
                    BatchOp {
                        keyspace: KeyspaceId::Objects,
                        action: BatchAction::Put(version_key.into_bytes(), record_bytes.clone()),
                    },
                    BatchOp {
                        keyspace: KeyspaceId::Versions,
                        action: BatchAction::Put(
                            format!("{}/{}", bucket, key).into_bytes(),
                            vl_bytes,
                        ),
                    },
                    BatchOp {
                        keyspace: KeyspaceId::Objects,
                        action: BatchAction::Put(full_key.into_bytes(), record_bytes),
                    },
                ];
                if let Some(op) = dedup_op {
                    ops.push(op);
                }
                ops.push(
                    self.journal.make_journal_entry_op(JournalEventType::ObjectPut {
                        bucket: bucket.to_string(),
                        key: key.to_string(),
                        version_id: Some(version_id.clone()),
                        record: record.clone(),
                    })?,
                );

                self.index_db.batch_write(ops).await?;

                Ok((etag, Some(version_id)))
            }
            VersioningStatus::Suspended => {
                // Use "null" version ID, replace existing null if any
                let version_id = NULL_VERSION_ID.to_string();
                let full_key = self.make_key(bucket, key);
                let version_key = self.make_version_key(bucket, key, &version_id);

                // Get existing version list
                let mut version_list = self.get_version_list(bucket, key).await?;

                // Remove existing null version if present
                version_list.remove_version(&version_id);

                // Write object data
                let (mut record, etag, dedup_op) =
                    self.write_object_data(&version_key, data, content_type, metadata).await?;
                record.version_id = Some(version_id.clone());

                // Add to version list
                version_list.add_version(version_id.clone(), false);

                // Serialize all data for the atomic batch
                let record_bytes = bincode::serialize(&record)
                    .map_err(|e| StorageError::Serialization(e.to_string()))?;
                let vl_bytes = serde_json::to_vec(&version_list)
                    .map_err(|e| StorageError::Serialization(e.to_string()))?;

                // ATOMIC BATCH: version record + version list + current pointer + dedup
                let mut ops = vec![
                    BatchOp {
                        keyspace: KeyspaceId::Objects,
                        action: BatchAction::Put(version_key.into_bytes(), record_bytes.clone()),
                    },
                    BatchOp {
                        keyspace: KeyspaceId::Versions,
                        action: BatchAction::Put(
                            format!("{}/{}", bucket, key).into_bytes(),
                            vl_bytes,
                        ),
                    },
                    BatchOp {
                        keyspace: KeyspaceId::Objects,
                        action: BatchAction::Put(full_key.into_bytes(), record_bytes),
                    },
                ];
                if let Some(op) = dedup_op {
                    ops.push(op);
                }
                ops.push(
                    self.journal.make_journal_entry_op(JournalEventType::ObjectPut {
                        bucket: bucket.to_string(),
                        key: key.to_string(),
                        version_id: Some(version_id.clone()),
                        record: record.clone(),
                    })?,
                );

                self.index_db.batch_write(ops).await?;

                Ok((etag, Some(version_id)))
            }
        }
    }

    async fn get_object_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<(Vec<u8>, IndexRecord), StorageError> {
        let version_key = self.make_version_key(bucket, key, version_id);

        let record = self.index_db.get(&version_key).await?.ok_or_else(|| {
            StorageError::VersionNotFound {
                key: key.to_string(),
                version_id: version_id.to_string(),
            }
        })?;

        if record.is_delete_marker {
            return Err(StorageError::DeleteMarker {
                key: key.to_string(),
                version_id: version_id.to_string(),
            });
        }

        let data = self.read_object_data(&record).await?;

        // Verify checksum
        let computed_hash = Deduplicator::compute_hash(&data);
        if computed_hash != record.content_hash {
            return Err(StorageError::ChecksumMismatch {
                key: format!("{}#{}", key, version_id),
            });
        }

        Ok((data, record))
    }

    async fn delete_object_versioned(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        versioning_status: VersioningStatus,
    ) -> Result<DeleteResult, StorageError> {
        match (versioning_status, version_id) {
            (VersioningStatus::Unversioned, _) => {
                // Standard delete - no versioning
                self.delete_object(bucket, key).await?;
                Ok(DeleteResult {
                    delete_marker: false,
                    version_id: None,
                })
            }
            (_, Some(vid)) => {
                // Permanently delete specific version
                let version_key = self.make_version_key(bucket, key, vid);

                // Check if version exists
                let record = self.index_db.get(&version_key).await?.ok_or_else(|| {
                    StorageError::VersionNotFound {
                        key: key.to_string(),
                        version_id: vid.to_string(),
                    }
                })?;

                // Prepare version list update
                let mut version_list = self.get_version_list(bucket, key).await?;
                version_list.remove_version(vid);

                // Prepare batch operations
                let mut ops = vec![
                    // Delete the versioned record
                    BatchOp {
                        keyspace: KeyspaceId::Objects,
                        action: BatchAction::Delete(version_key.into_bytes()),
                    },
                ];

                // Include dedup/blob-ref unregister in the batch
                if record.is_composite() {
                    let mut ref_ops = self.composite_delete_ref_ops(&record).await?;
                    ops.append(&mut ref_ops);
                } else if record.file_id != u32::MAX && record.file_id != DELETE_MARKER_FILE_ID {
                    if let Some(dedup_op) =
                        self.deduplicator.make_unregister_op(&record.content_hash)?
                    {
                        ops.push(dedup_op);
                    }
                }

                // Recalculate current version and add ops
                let mut recalc_ops =
                    self.recalculate_current_version_ops(bucket, key, &mut version_list).await?;
                ops.append(&mut recalc_ops);

                // Add version list update
                let vl_bytes = serde_json::to_vec(&version_list)
                    .map_err(|e| StorageError::Serialization(e.to_string()))?;
                if version_list.is_empty() {
                    ops.push(BatchOp {
                        keyspace: KeyspaceId::Versions,
                        action: BatchAction::Delete(format!("{}/{}", bucket, key).into_bytes()),
                    });
                } else {
                    ops.push(BatchOp {
                        keyspace: KeyspaceId::Versions,
                        action: BatchAction::Put(
                            format!("{}/{}", bucket, key).into_bytes(),
                            vl_bytes,
                        ),
                    });
                }

                // Journal entry for specific version delete
                ops.push(
                    self.journal.make_journal_entry_op(JournalEventType::ObjectDelete {
                        bucket: bucket.to_string(),
                        key: key.to_string(),
                        version_id: Some(vid.to_string()),
                    })?,
                );

                // ATOMIC BATCH: delete version + dedup unregister + version list + current + journal
                self.index_db.batch_write(ops).await?;

                Ok(DeleteResult {
                    delete_marker: record.is_delete_marker,
                    version_id: Some(vid.to_string()),
                })
            }
            (VersioningStatus::Enabled, None) => {
                // Create delete marker with new version ID
                let marker_version_id = generate_version_id();
                let version_key = self.make_version_key(bucket, key, &marker_version_id);
                let full_key = self.make_key(bucket, key);

                // Phase 5: Write a zero-length delete marker blob to volume so it
                // can be discovered during recovery.
                let marker = self
                    .write_delete_marker_to_volume(&version_key, marker_version_id.clone())
                    .await?;

                // Prepare version list
                let mut version_list = self.get_version_list(bucket, key).await?;
                version_list.add_version(marker_version_id.clone(), true);

                // Serialize
                let marker_bytes = bincode::serialize(&marker)
                    .map_err(|e| StorageError::Serialization(e.to_string()))?;
                let vl_bytes = serde_json::to_vec(&version_list)
                    .map_err(|e| StorageError::Serialization(e.to_string()))?;

                // ATOMIC BATCH: delete marker + version list + remove current pointer + journal
                let mut ops = vec![
                    BatchOp {
                        keyspace: KeyspaceId::Objects,
                        action: BatchAction::Put(version_key.into_bytes(), marker_bytes),
                    },
                    BatchOp {
                        keyspace: KeyspaceId::Versions,
                        action: BatchAction::Put(
                            format!("{}/{}", bucket, key).into_bytes(),
                            vl_bytes,
                        ),
                    },
                    BatchOp {
                        keyspace: KeyspaceId::Objects,
                        action: BatchAction::Delete(full_key.into_bytes()),
                    },
                ];
                ops.push(self.journal.make_journal_entry_op(
                    JournalEventType::DeleteMarkerCreated {
                        bucket: bucket.to_string(),
                        key: key.to_string(),
                        marker_version_id: marker_version_id.clone(),
                        record: marker,
                    },
                )?);

                self.index_db.batch_write(ops).await?;

                Ok(DeleteResult {
                    delete_marker: true,
                    version_id: Some(marker_version_id),
                })
            }
            (VersioningStatus::Suspended, None) => {
                // Create delete marker with null version ID
                let marker_version_id = NULL_VERSION_ID.to_string();
                let version_key = self.make_version_key(bucket, key, &marker_version_id);
                let full_key = self.make_key(bucket, key);

                // Get existing version list
                let mut version_list = self.get_version_list(bucket, key).await?;

                // Remove existing null version if present
                let old_null_key = self.make_version_key(bucket, key, NULL_VERSION_ID);
                let mut ops = Vec::new();

                if let Some(old_record) = self.index_db.get(&old_null_key).await? {
                    // Include dedup unregister in the batch
                    if old_record.file_id != u32::MAX && old_record.file_id != DELETE_MARKER_FILE_ID
                    {
                        if let Some(dedup_op) =
                            self.deduplicator.make_unregister_op(&old_record.content_hash)?
                        {
                            ops.push(dedup_op);
                        }
                    }
                    // Delete old null version record (will be in the batch)
                    ops.push(BatchOp {
                        keyspace: KeyspaceId::Objects,
                        action: BatchAction::Delete(old_null_key.into_bytes()),
                    });
                }
                version_list.remove_version(&marker_version_id);

                // Phase 5: Write delete marker to volume
                let marker = self
                    .write_delete_marker_to_volume(&version_key, marker_version_id.clone())
                    .await?;

                // Update version list
                version_list.add_version(marker_version_id.clone(), true);

                // Serialize
                let marker_bytes = bincode::serialize(&marker)
                    .map_err(|e| StorageError::Serialization(e.to_string()))?;
                let vl_bytes = serde_json::to_vec(&version_list)
                    .map_err(|e| StorageError::Serialization(e.to_string()))?;

                // ATOMIC BATCH: [delete old null] + new marker + version list + remove current
                ops.push(BatchOp {
                    keyspace: KeyspaceId::Objects,
                    action: BatchAction::Put(version_key.into_bytes(), marker_bytes),
                });
                ops.push(BatchOp {
                    keyspace: KeyspaceId::Versions,
                    action: BatchAction::Put(format!("{}/{}", bucket, key).into_bytes(), vl_bytes),
                });
                ops.push(BatchOp {
                    keyspace: KeyspaceId::Objects,
                    action: BatchAction::Delete(full_key.into_bytes()),
                });

                ops.push(self.journal.make_journal_entry_op(
                    JournalEventType::DeleteMarkerCreated {
                        bucket: bucket.to_string(),
                        key: key.to_string(),
                        marker_version_id: marker_version_id.clone(),
                        record: marker,
                    },
                )?);

                self.index_db.batch_write(ops).await?;

                Ok(DeleteResult {
                    delete_marker: true,
                    version_id: Some(marker_version_id),
                })
            }
        }
    }

    async fn head_object_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<IndexRecord, StorageError> {
        let version_key = self.make_version_key(bucket, key, version_id);

        self.index_db
            .get(&version_key)
            .await?
            .ok_or_else(|| StorageError::VersionNotFound {
                key: key.to_string(),
                version_id: version_id.to_string(),
            })
    }

    async fn list_object_versions(
        &self,
        bucket: &str,
        prefix: &str,
        key_marker: Option<&str>,
        version_id_marker: Option<&str>,
        max_keys: usize,
    ) -> Result<ListVersionsResult, StorageError> {
        // Collect versioned keys from the versions keyspace
        let vl_prefix = format!("{}/{}", bucket, prefix);
        let version_list_entries = self.index_db.list_version_lists(&vl_prefix, usize::MAX).await?;

        let mut versioned_keys: std::collections::BTreeSet<String> =
            std::collections::BTreeSet::new();
        for (bucket_key, _) in &version_list_entries {
            let key_part = bucket_key.strip_prefix(&format!("{}/", bucket)).unwrap_or(bucket_key);
            versioned_keys.insert(key_part.to_string());
        }

        // List regular objects to find unversioned ones
        let bucket_prefix = format!("{}/{}", bucket, prefix);
        let all_entries = self.index_db.list(&bucket_prefix, usize::MAX).await?;

        let mut unversioned_keys: Vec<String> = Vec::new();
        for (full_key, _) in &all_entries {
            let key_part = full_key.strip_prefix(&format!("{}/", bucket)).unwrap_or(full_key);

            // Skip version record keys (contain #)
            if key_part.contains('#') {
                continue;
            }

            // Skip keys that have version lists (they're versioned)
            if versioned_keys.contains(key_part) {
                continue;
            }

            unversioned_keys.push(key_part.to_string());
        }

        // Merge versioned and unversioned keys in sorted order
        let mut all_keys: Vec<(String, bool)> = Vec::new();
        for k in &versioned_keys {
            all_keys.push((k.clone(), true));
        }
        for k in &unversioned_keys {
            all_keys.push((k.clone(), false));
        }
        all_keys.sort_by(|a, b| a.0.cmp(&b.0));

        let mut result = ListVersionsResult::default();
        let mut count = 0;
        let mut past_marker = key_marker.is_none();
        let mut last_key: Option<String> = None;
        let mut last_version_id: Option<String> = None;

        for (key_part, is_versioned) in &all_keys {
            // Handle key marker pagination
            if !past_marker {
                if Some(key_part.as_str()) == key_marker {
                    past_marker = true;
                }
                if !past_marker {
                    continue;
                }
            }

            if *is_versioned {
                // Versioned object: enumerate all versions from version list
                let version_list = self.get_version_list(bucket, key_part).await?;

                let mut past_version_marker =
                    version_id_marker.is_none() || key_marker != Some(key_part.as_str());

                for (i, vid) in version_list.versions.iter().enumerate() {
                    if !past_version_marker {
                        if Some(vid.as_str()) == version_id_marker {
                            past_version_marker = true;
                        }
                        continue;
                    }

                    if count >= max_keys {
                        result.is_truncated = true;
                        result.next_key_marker = last_key;
                        result.next_version_id_marker = last_version_id;
                        return Ok(result);
                    }

                    let version_key = self.make_version_key(bucket, key_part, vid);
                    if let Some(record) = self.index_db.get(&version_key).await? {
                        let is_latest = i == 0;

                        if record.is_delete_marker {
                            result.delete_markers.push(DeleteMarkerEntry {
                                key: key_part.to_string(),
                                version_id: vid.clone(),
                                is_latest,
                                last_modified: record.modified_at,
                            });
                        } else {
                            result.versions.push(ObjectVersion {
                                key: key_part.to_string(),
                                version_id: vid.clone(),
                                is_latest,
                                last_modified: record.modified_at,
                                etag: record.etag.clone(),
                                size: record.size,
                            });
                        }

                        last_key = Some(key_part.to_string());
                        last_version_id = Some(vid.clone());
                        count += 1;
                    }
                }
            } else {
                // Unversioned object: include with "null" version ID
                if count >= max_keys {
                    result.is_truncated = true;
                    result.next_key_marker = last_key;
                    result.next_version_id_marker = last_version_id;
                    return Ok(result);
                }

                let full_key = self.make_key(bucket, key_part);
                if let Some(record) = self.index_db.get(&full_key).await? {
                    result.versions.push(ObjectVersion {
                        key: key_part.to_string(),
                        version_id: NULL_VERSION_ID.to_string(),
                        is_latest: true,
                        last_modified: record.modified_at,
                        etag: record.etag.clone(),
                        size: record.size,
                    });
                    last_key = Some(key_part.to_string());
                    last_version_id = Some(NULL_VERSION_ID.to_string());
                    count += 1;
                }
            }
        }

        Ok(result)
    }

    // ========================================================================
    // Object Lock Methods
    // ========================================================================

    async fn put_object_with_retention(
        &self,
        bucket: &str,
        key: &str,
        data: &[u8],
        content_type: &str,
        metadata: &HashMap<String, String>,
        versioning_status: VersioningStatus,
        default_retention: Option<DefaultRetention>,
    ) -> Result<(String, Option<String>), StorageError> {
        // If no default retention, use standard versioned put
        let Some(retention) = default_retention else {
            return self
                .put_object_versioned(bucket, key, data, content_type, metadata, versioning_status)
                .await;
        };

        // Apply retention based on versioning status
        match versioning_status {
            VersioningStatus::Unversioned => {
                let full_key = self.make_key(bucket, key);

                let (mut record, etag, dedup_op) =
                    self.write_object_data(&full_key, data, content_type, metadata).await?;

                record.retention_mode = Some(retention.mode);
                record.retain_until_timestamp =
                    Some(retention.calculate_retain_until(record.created_at));

                // ATOMIC BATCH: object record + dedup + journal
                let record_bytes = bincode::serialize(&record)
                    .map_err(|e| StorageError::Serialization(e.to_string()))?;
                let mut ops = vec![BatchOp {
                    keyspace: KeyspaceId::Objects,
                    action: BatchAction::Put(full_key.into_bytes(), record_bytes),
                }];
                if let Some(op) = dedup_op {
                    ops.push(op);
                }
                ops.push(
                    self.journal.make_journal_entry_op(JournalEventType::ObjectPut {
                        bucket: bucket.to_string(),
                        key: key.to_string(),
                        version_id: None,
                        record: record.clone(),
                    })?,
                );
                self.index_db.batch_write(ops).await?;

                Ok((etag, None))
            }
            VersioningStatus::Enabled => {
                let version_id = generate_version_id();
                let full_key = self.make_key(bucket, key);
                let version_key = self.make_version_key(bucket, key, &version_id);

                let (mut record, etag, dedup_op) =
                    self.write_object_data(&version_key, data, content_type, metadata).await?;
                record.version_id = Some(version_id.clone());

                record.retention_mode = Some(retention.mode);
                record.retain_until_timestamp =
                    Some(retention.calculate_retain_until(record.created_at));

                // Prepare version list
                let mut version_list = self.get_version_list(bucket, key).await?;
                version_list.add_version(version_id.clone(), false);

                let record_bytes = bincode::serialize(&record)
                    .map_err(|e| StorageError::Serialization(e.to_string()))?;
                let vl_bytes = serde_json::to_vec(&version_list)
                    .map_err(|e| StorageError::Serialization(e.to_string()))?;

                // ATOMIC BATCH + dedup + journal
                let mut ops = vec![
                    BatchOp {
                        keyspace: KeyspaceId::Objects,
                        action: BatchAction::Put(version_key.into_bytes(), record_bytes.clone()),
                    },
                    BatchOp {
                        keyspace: KeyspaceId::Versions,
                        action: BatchAction::Put(
                            format!("{}/{}", bucket, key).into_bytes(),
                            vl_bytes,
                        ),
                    },
                    BatchOp {
                        keyspace: KeyspaceId::Objects,
                        action: BatchAction::Put(full_key.into_bytes(), record_bytes),
                    },
                ];
                if let Some(op) = dedup_op {
                    ops.push(op);
                }
                ops.push(
                    self.journal.make_journal_entry_op(JournalEventType::ObjectPut {
                        bucket: bucket.to_string(),
                        key: key.to_string(),
                        version_id: Some(version_id.clone()),
                        record: record.clone(),
                    })?,
                );

                self.index_db.batch_write(ops).await?;

                Ok((etag, Some(version_id)))
            }
            VersioningStatus::Suspended => {
                let version_id = NULL_VERSION_ID.to_string();
                let full_key = self.make_key(bucket, key);
                let version_key = self.make_version_key(bucket, key, &version_id);

                let mut version_list = self.get_version_list(bucket, key).await?;
                version_list.remove_version(&version_id);

                let (mut record, etag, dedup_op) =
                    self.write_object_data(&version_key, data, content_type, metadata).await?;
                record.version_id = Some(version_id.clone());

                record.retention_mode = Some(retention.mode);
                record.retain_until_timestamp =
                    Some(retention.calculate_retain_until(record.created_at));

                version_list.add_version(version_id.clone(), false);

                let record_bytes = bincode::serialize(&record)
                    .map_err(|e| StorageError::Serialization(e.to_string()))?;
                let vl_bytes = serde_json::to_vec(&version_list)
                    .map_err(|e| StorageError::Serialization(e.to_string()))?;

                // ATOMIC BATCH + dedup + journal
                let mut ops = vec![
                    BatchOp {
                        keyspace: KeyspaceId::Objects,
                        action: BatchAction::Put(version_key.into_bytes(), record_bytes.clone()),
                    },
                    BatchOp {
                        keyspace: KeyspaceId::Versions,
                        action: BatchAction::Put(
                            format!("{}/{}", bucket, key).into_bytes(),
                            vl_bytes,
                        ),
                    },
                    BatchOp {
                        keyspace: KeyspaceId::Objects,
                        action: BatchAction::Put(full_key.into_bytes(), record_bytes),
                    },
                ];
                if let Some(op) = dedup_op {
                    ops.push(op);
                }
                ops.push(
                    self.journal.make_journal_entry_op(JournalEventType::ObjectPut {
                        bucket: bucket.to_string(),
                        key: key.to_string(),
                        version_id: Some(version_id.clone()),
                        record: record.clone(),
                    })?,
                );

                self.index_db.batch_write(ops).await?;

                Ok((etag, Some(version_id)))
            }
        }
    }

    async fn update_object_metadata(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
        updated_record: IndexRecord,
    ) -> Result<(), StorageError> {
        // For non-versioned objects (empty version_id), update the primary key directly
        if version_id.is_empty() {
            let full_key = self.make_key(bucket, key);
            let existing = self.index_db.get(&full_key).await?.ok_or_else(|| {
                StorageError::VersionNotFound {
                    key: key.to_string(),
                    version_id: String::new(),
                }
            })?;
            if existing.is_delete_marker {
                return Err(StorageError::InvalidOperation {
                    operation: "update_object_metadata".to_string(),
                    reason: "Cannot update metadata of delete marker".to_string(),
                });
            }
            // Journal entry for metadata update (non-versioned)
            let journal_op =
                self.journal.make_journal_entry_op(JournalEventType::MetadataUpdate {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                    version_id: None,
                    updated_fields: HashMap::new(), // Fields tracked by record snapshot
                })?;
            let record_bytes = bincode::serialize(&updated_record)
                .map_err(|e| StorageError::Serialization(e.to_string()))?;
            let (ks, stripped) = self.index_db.route_key_id(&full_key);
            let ops = vec![
                BatchOp {
                    keyspace: ks,
                    action: BatchAction::Put(stripped.as_bytes().to_vec(), record_bytes),
                },
                journal_op,
            ];
            self.index_db.batch_write(ops).await?;
            return Ok(());
        }

        let version_key = self.make_version_key(bucket, key, version_id);

        // Verify version exists
        let existing = self.index_db.get(&version_key).await?.ok_or_else(|| {
            StorageError::VersionNotFound {
                key: key.to_string(),
                version_id: version_id.to_string(),
            }
        })?;

        if existing.is_delete_marker {
            return Err(StorageError::InvalidOperation {
                operation: "update_object_metadata".to_string(),
                reason: "Cannot update metadata of delete marker".to_string(),
            });
        }

        // Check if this is the current version — if so, update both atomically
        let version_list = self.get_version_list(bucket, key).await?;
        if version_list.current_version.as_deref() == Some(version_id) {
            let full_key = self.make_key(bucket, key);
            let record_bytes = bincode::serialize(&updated_record)
                .map_err(|e| StorageError::Serialization(e.to_string()))?;

            // ATOMIC BATCH: version record + current pointer + journal
            let journal_op =
                self.journal.make_journal_entry_op(JournalEventType::MetadataUpdate {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                    version_id: Some(version_id.to_string()),
                    updated_fields: HashMap::new(),
                })?;
            let ops = vec![
                BatchOp {
                    keyspace: KeyspaceId::Objects,
                    action: BatchAction::Put(version_key.into_bytes(), record_bytes.clone()),
                },
                BatchOp {
                    keyspace: KeyspaceId::Objects,
                    action: BatchAction::Put(full_key.into_bytes(), record_bytes),
                },
                journal_op,
            ];
            self.index_db.batch_write(ops).await?;
        } else {
            // Not current version — single write + journal
            let journal_op =
                self.journal.make_journal_entry_op(JournalEventType::MetadataUpdate {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                    version_id: Some(version_id.to_string()),
                    updated_fields: HashMap::new(),
                })?;
            let record_bytes = bincode::serialize(&updated_record)
                .map_err(|e| StorageError::Serialization(e.to_string()))?;
            let ops = vec![
                BatchOp {
                    keyspace: KeyspaceId::Objects,
                    action: BatchAction::Put(version_key.into_bytes(), record_bytes),
                },
                journal_op,
            ];
            self.index_db.batch_write(ops).await?;
        }

        Ok(())
    }

    async fn put_object_streaming(
        &self,
        bucket: &str,
        key: &str,
        reader: Box<dyn tokio::io::AsyncRead + Unpin + Send>,
        content_length: u64,
        content_type: &str,
        metadata: &HashMap<String, String>,
        etag: &str,
    ) -> Result<crate::storage::engine::StreamingPutResult, StorageError> {
        use sha2::{Digest, Sha256};

        let full_key = self.make_key(bucket, key);
        const BUFFER_SIZE: usize = 1024 * 1024; // 1 MB

        // Write to volume with streaming, computing CRC32 via the volume writer.
        // SHA-256 must be computed separately since the volume writer doesn't do it.
        let mut sha256_hasher = Sha256::new();

        // Wrap reader to tee SHA-256 computation
        let tee_reader = Sha256TeeReader {
            inner: reader,
            hasher: &mut sha256_hasher,
        };

        let (volume_id, offset, crc32) = {
            let mut writer = self.volume_writer.write().await;
            let result = writer
                .write_blob_streaming(&full_key, content_length, tee_reader, BUFFER_SIZE)
                .await?;

            if self.strict_sync {
                writer.sync().await?;
            }
            result
        };

        let content_hash: [u8; 32] = sha256_hasher.finalize().into();

        // Post-write dedup check: content was already written, now check if duplicate
        let dedup_op = if let Some((existing_vol, existing_off)) =
            self.deduplicator.check_existing(&content_hash)?
        {
            // Duplicate detected — the blob we just wrote is dead space.
            // Use the existing location for the index record.
            let record = IndexRecord::new(
                existing_vol,
                existing_off,
                content_length,
                content_hash,
                etag.to_string(),
                content_type.to_string(),
            );
            let mut record = record;
            record.metadata.extend(metadata.clone());
            record.modified_at = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64;

            let dedup_op =
                self.deduplicator.make_register_op(content_hash, existing_vol, existing_off)?;

            // Atomic batch: object record + dedup ref increment + journal
            let record_bytes = bincode::serialize(&record)
                .map_err(|e| StorageError::Serialization(e.to_string()))?;
            let (keyspace, stripped_key) = self.index_db.route_key_id(&full_key);
            let ops = vec![
                BatchOp {
                    keyspace,
                    action: BatchAction::Put(stripped_key.as_bytes().to_vec(), record_bytes),
                },
                dedup_op,
                self.journal.make_journal_entry_op(JournalEventType::ObjectPut {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                    version_id: None,
                    record: record.clone(),
                })?,
            ];
            self.index_db.batch_write(ops).await?;

            return Ok(crate::storage::engine::StreamingPutResult {
                content_hash,
                crc32,
                bytes_written: content_length,
                etag: etag.to_string(),
            });
        } else {
            // New content — register in dedup with the location we just wrote
            self.deduplicator.make_register_op(content_hash, volume_id, offset)?
        };

        let record = IndexRecord::new(
            volume_id,
            offset,
            content_length,
            content_hash,
            etag.to_string(),
            content_type.to_string(),
        );
        let mut record = record;
        record.metadata.extend(metadata.clone());
        record.modified_at = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64;

        // Atomic batch: object record + dedup registration + journal
        let record_bytes =
            bincode::serialize(&record).map_err(|e| StorageError::Serialization(e.to_string()))?;
        let (keyspace, stripped_key) = self.index_db.route_key_id(&full_key);
        let ops = vec![
            BatchOp {
                keyspace,
                action: BatchAction::Put(stripped_key.as_bytes().to_vec(), record_bytes),
            },
            dedup_op,
            self.journal.make_journal_entry_op(JournalEventType::ObjectPut {
                bucket: bucket.to_string(),
                key: key.to_string(),
                version_id: None,
                record: record.clone(),
            })?,
        ];
        self.index_db.batch_write(ops).await?;

        Ok(crate::storage::engine::StreamingPutResult {
            content_hash,
            crc32,
            bytes_written: content_length,
            etag: etag.to_string(),
        })
    }

    async fn open_object_stream(
        &self,
        bucket: &str,
        key: &str,
        options: crate::storage::engine::ReadOptions,
    ) -> Result<crate::storage::engine::ObjectStream, StorageError> {
        let full_key = self.make_key(bucket, key);
        let record =
            self.index_db
                .get(&full_key)
                .await?
                .ok_or_else(|| StorageError::ObjectNotFound {
                    key: full_key.clone(),
                })?;
        self.open_record_stream(record, options).await
    }

    async fn open_object_version_stream(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
        options: crate::storage::engine::ReadOptions,
    ) -> Result<crate::storage::engine::ObjectStream, StorageError> {
        let version_key = self.make_version_key(bucket, key, version_id);
        let record = self.index_db.get(&version_key).await?.ok_or_else(|| {
            StorageError::VersionNotFound {
                key: key.to_string(),
                version_id: version_id.to_string(),
            }
        })?;
        if record.is_delete_marker {
            return Err(StorageError::DeleteMarker {
                key: key.to_string(),
                version_id: version_id.to_string(),
            });
        }
        self.open_record_stream(record, options).await
    }
}

/// A slice specification for one segment in a composite read.
struct SegmentSlice {
    /// Volume ID containing the segment blob.
    volume_id: u32,
    /// Byte offset of the blob in the volume.
    blob_offset: u64,
    /// Byte offset within the segment data to start reading.
    data_start: u64,
    /// Number of bytes to read from this segment.
    data_len: u64,
}

/// Reads a composite object by chaining segment blob readers from volumes.
///
/// Opens one file descriptor at a time, sequentially streaming each segment's
/// data. Memory usage is O(buffer_size), not O(object_size).
struct CompositeObjectReader {
    volumes_dir: PathBuf,
    slices: std::collections::VecDeque<SegmentSlice>,
    current: Option<tokio::io::Take<tokio::fs::File>>,
    remaining: u64,
}

impl CompositeObjectReader {
    fn new(volumes_dir: PathBuf, slices: Vec<SegmentSlice>) -> Self {
        let remaining: u64 = slices.iter().map(|s| s.data_len).sum();
        Self {
            volumes_dir,
            slices: slices.into(),
            current: None,
            remaining,
        }
    }
}

impl tokio::io::AsyncRead for CompositeObjectReader {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let this = self.get_mut();
        loop {
            // Try reading from current segment
            if let Some(ref mut reader) = this.current {
                let before = buf.filled().len();
                match std::pin::Pin::new(reader).poll_read(cx, buf) {
                    std::task::Poll::Ready(Ok(())) => {
                        let read = buf.filled().len() - before;
                        if read > 0 {
                            this.remaining -= read as u64;
                            return std::task::Poll::Ready(Ok(()));
                        }
                        // EOF on this segment — close and try next
                        this.current = None;
                        continue;
                    }
                    other => return other,
                }
            }

            // Open next segment
            if let Some(slice) = this.slices.pop_front() {
                let volume_path =
                    this.volumes_dir.join(format!("volume_{:06}.dat", slice.volume_id));
                // Use sync open (temp files are local, fast — same pattern as LazyPartsReader)
                let std_file = match std::fs::File::open(&volume_path) {
                    Ok(f) => f,
                    Err(e) => return std::task::Poll::Ready(Err(e)),
                };
                let file = tokio::fs::File::from_std(std_file);

                // Read blob header to find data start position
                // We need to do this synchronously in poll_read context
                use std::io::{Read, Seek};
                let raw_file = file
                    .try_into_std()
                    .map_err(|_| std::io::Error::other("file conversion failed"))?;
                let mut raw_file = raw_file;
                raw_file.seek(std::io::SeekFrom::Start(slice.blob_offset))?;
                let mut header_buf = [0u8; 1024];
                let n = raw_file.read(&mut header_buf)?;
                let header: BlobHeader = bincode::deserialize(&header_buf[..n]).map_err(|e| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())
                })?;
                let header_size = bincode::serialized_size(&header).map_err(|e| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())
                })? as u64;
                let data_start =
                    slice.blob_offset + header_size + header.key_len as u64 + slice.data_start;
                raw_file.seek(std::io::SeekFrom::Start(data_start))?;

                let tokio_file = tokio::fs::File::from_std(raw_file);
                this.current = Some(tokio::io::AsyncReadExt::take(tokio_file, slice.data_len));
                continue;
            }

            // No more segments — EOF
            return std::task::Poll::Ready(Ok(()));
        }
    }
}

impl BitcaskStorageEngine {
    /// Opens a streaming reader for a given IndexRecord.
    ///
    /// Handles legacy inline objects, single-blob volume objects, and
    /// composite multipart objects (ordered segment streaming).
    async fn open_record_stream(
        &self,
        record: IndexRecord,
        options: crate::storage::engine::ReadOptions,
    ) -> Result<crate::storage::engine::ObjectStream, StorageError> {
        if record.is_delete_marker {
            return Err(StorageError::DeleteMarker {
                key: String::new(),
                version_id: record.version_id.clone().unwrap_or_default(),
            });
        }

        let total_size = record.size;

        // Legacy inline storage: fall back to buffered read
        if record.file_id == u32::MAX && !record.is_composite() {
            let inline_data_str = record
                .metadata
                .get("_inline_data")
                .ok_or_else(|| StorageError::InvalidData("Inline data not found".to_string()))?;
            let data =
                base64::engine::general_purpose::STANDARD.decode(inline_data_str).map_err(|e| {
                    StorageError::InvalidData(format!("Failed to decode inline data: {}", e))
                })?;
            let (response_data, content_length, content_range) =
                apply_range_to_vec(data, total_size, &options);
            return Ok(crate::storage::engine::ObjectStream {
                record,
                total_size,
                content_length,
                content_range,
                body: Box::new(std::io::Cursor::new(response_data)),
            });
        }

        // Composite multipart object: stream from ordered segments
        if record.is_composite() {
            return self.open_composite_stream(record, options).await;
        }

        // Single-blob object: direct volume read
        let reader = VolumeReader::new(&self.volumes_dir);

        match options.range {
            Some((start, end)) => {
                let start = start.min(total_size.saturating_sub(1));
                let end = end.min(total_size - 1);
                if start > end {
                    return Err(StorageError::InvalidData(
                        "Range start exceeds end".to_string(),
                    ));
                }
                let range_len = end - start + 1;

                let (_header, data_reader) = reader
                    .open_blob_range_stream(record.file_id, record.offset, start, range_len)
                    .await?;

                Ok(crate::storage::engine::ObjectStream {
                    record,
                    total_size,
                    content_length: range_len,
                    content_range: Some((start, end, total_size)),
                    body: Box::new(data_reader),
                })
            }
            None => {
                let (_header, _key, data_reader) =
                    reader.open_blob_stream(record.file_id, record.offset).await?;

                Ok(crate::storage::engine::ObjectStream {
                    record,
                    total_size,
                    content_length: total_size,
                    content_range: None,
                    body: Box::new(data_reader),
                })
            }
        }
    }

    /// Opens a streaming reader for a composite multipart object.
    ///
    /// Loads the manifest, resolves segment locations, and builds slice specs
    /// for the CompositeObjectReader. Supports full and range reads.
    async fn open_composite_stream(
        &self,
        record: IndexRecord,
        options: crate::storage::engine::ReadOptions,
    ) -> Result<crate::storage::engine::ObjectStream, StorageError> {
        use crate::types::composite::ObjectLayout;

        let total_size = record.size;
        let manifest_blob_id = match record.effective_layout() {
            ObjectLayout::CompositeManifest {
                manifest_blob_id, ..
            } => manifest_blob_id,
            _ => {
                return Err(StorageError::InvalidData(
                    "Expected CompositeManifest layout".to_string(),
                ))
            }
        };

        let manifest = self.load_composite_manifest(&manifest_blob_id).await?;

        // Resolve all segment blob locations
        let mut segment_locs: Vec<(BlobLocation, u64)> =
            Vec::with_capacity(manifest.segments.len());
        for seg in &manifest.segments {
            let loc = self.resolve_blob_location(&seg.blob_id)?;
            segment_locs.push((loc, seg.size));
        }

        // Determine range
        let (range_start, range_end) = match options.range {
            Some((start, end)) => {
                let start = start.min(total_size.saturating_sub(1));
                let end = end.min(total_size - 1);
                if start > end {
                    return Err(StorageError::InvalidData(
                        "Range start exceeds end".to_string(),
                    ));
                }
                (start, end)
            }
            None => (0, total_size.saturating_sub(1)),
        };
        let range_len = range_end - range_start + 1;

        // Build slice specs: determine which segments overlap [range_start, range_end]
        let mut slices = Vec::new();
        let mut cumulative: u64 = 0;
        for (loc, seg_size) in &segment_locs {
            let seg_start = cumulative;
            let seg_end = cumulative + seg_size - 1;
            cumulative += seg_size;

            // Check overlap with [range_start, range_end]
            if seg_end < range_start || seg_start > range_end {
                continue; // No overlap
            }

            // Calculate the portion of this segment to read
            let data_start = range_start.saturating_sub(seg_start);
            let data_end = if range_end < seg_end {
                range_end - seg_start
            } else {
                seg_size - 1
            };

            slices.push(SegmentSlice {
                volume_id: loc.volume_id,
                blob_offset: loc.offset,
                data_start,
                data_len: data_end - data_start + 1,
            });
        }

        let composite_reader = CompositeObjectReader::new(self.volumes_dir.clone(), slices);

        let content_range = if options.range.is_some() {
            Some((range_start, range_end, total_size))
        } else {
            None
        };

        Ok(crate::storage::engine::ObjectStream {
            record,
            total_size,
            content_length: range_len,
            content_range,
            body: Box::new(composite_reader),
        })
    }
}

/// Applies range options to an in-memory buffer (for legacy inline objects).
fn apply_range_to_vec(
    data: Vec<u8>,
    total_size: u64,
    options: &crate::storage::engine::ReadOptions,
) -> (Vec<u8>, u64, Option<(u64, u64, u64)>) {
    match options.range {
        Some((start, end)) => {
            let start = (start as usize).min(data.len().saturating_sub(1));
            let end = (end as usize).min(data.len() - 1);
            let slice = data[start..=end].to_vec();
            let len = slice.len() as u64;
            (slice, len, Some((start as u64, end as u64, total_size)))
        }
        None => {
            let len = data.len() as u64;
            (data, len, None)
        }
    }
}

/// Wraps an `AsyncRead` to compute SHA-256 as data passes through.
struct Sha256TeeReader<'a, R> {
    inner: R,
    hasher: &'a mut sha2::Sha256,
}

impl<R: tokio::io::AsyncRead + Unpin> tokio::io::AsyncRead for Sha256TeeReader<'_, R> {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        use sha2::Digest;
        let this = self.get_mut();
        let before = buf.filled().len();
        let result = std::pin::Pin::new(&mut this.inner).poll_read(cx, buf);
        if let std::task::Poll::Ready(Ok(())) = &result {
            let after = buf.filled().len();
            if after > before {
                this.hasher.update(&buf.filled()[before..after]);
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_put_and_get_object() {
        let temp_dir = TempDir::new().unwrap();
        let volumes_dir = temp_dir.path().join("volumes");
        let metadata_path = temp_dir.path().join("metadata_db");

        let engine = BitcaskStorageEngine::new(
            &volumes_dir,
            &metadata_path,
            1024 * 1024,
            4096,
            false, // Don't sync for tests
        )
        .await
        .unwrap();

        let data = b"test data";
        let mut metadata = HashMap::new();
        metadata.insert("key".to_string(), "value".to_string());

        let etag = engine.put_object("bucket", "key", data, "text/plain", &metadata).await.unwrap();

        let (retrieved_data, record) = engine.get_object("bucket", "key").await.unwrap();

        assert_eq!(retrieved_data, data);
        assert_eq!(record.etag, etag);
        assert_eq!(record.content_type, "text/plain");
        assert_eq!(record.metadata.get("key"), Some(&"value".to_string()));
    }

    #[tokio::test]
    async fn test_small_objects_go_to_volume() {
        // Phase 5: Even tiny objects are written to volumes, not inline.
        let temp_dir = TempDir::new().unwrap();
        let volumes_dir = temp_dir.path().join("volumes");
        let metadata_path = temp_dir.path().join("metadata_db");

        let engine =
            BitcaskStorageEngine::new(&volumes_dir, &metadata_path, 1024 * 1024, 4096, false)
                .await
                .unwrap();

        let data = b"tiny";
        engine
            .put_object("bucket", "tiny", data, "text/plain", &HashMap::new())
            .await
            .unwrap();

        let (retrieved_data, record) = engine.get_object("bucket", "tiny").await.unwrap();

        assert_eq!(retrieved_data, data);
        // Phase 5: file_id should be a real volume ID, not the inline sentinel
        assert_ne!(record.file_id, u32::MAX);
        // No _inline_data in metadata
        assert!(!record.metadata.contains_key("_inline_data"));
    }

    #[tokio::test]
    async fn test_deduplication() {
        let temp_dir = TempDir::new().unwrap();
        let volumes_dir = temp_dir.path().join("volumes");
        let metadata_path = temp_dir.path().join("metadata_db");

        let engine =
            BitcaskStorageEngine::new(&volumes_dir, &metadata_path, 1024 * 1024, 4096, false)
                .await
                .unwrap();

        let data = b"duplicate data";

        // Write same data twice
        let etag1 = engine
            .put_object("bucket", "key1", data, "text/plain", &HashMap::new())
            .await
            .unwrap();

        let etag2 = engine
            .put_object("bucket", "key2", data, "text/plain", &HashMap::new())
            .await
            .unwrap();

        // ETags should be the same (same content)
        assert_eq!(etag1, etag2);

        // Both should point to same location
        let record1 = engine.head_object("bucket", "key1").await.unwrap();
        let record2 = engine.head_object("bucket", "key2").await.unwrap();

        assert_eq!(record1.file_id, record2.file_id);
        assert_eq!(record1.offset, record2.offset);
    }

    #[tokio::test]
    async fn test_delete_object() {
        let temp_dir = TempDir::new().unwrap();
        let volumes_dir = temp_dir.path().join("volumes");
        let metadata_path = temp_dir.path().join("metadata_db");

        let engine =
            BitcaskStorageEngine::new(&volumes_dir, &metadata_path, 1024 * 1024, 4096, false)
                .await
                .unwrap();

        engine
            .put_object("bucket", "key", b"data", "text/plain", &HashMap::new())
            .await
            .unwrap();

        engine.delete_object("bucket", "key").await.unwrap();

        let result = engine.get_object("bucket", "key").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_list_objects() {
        let temp_dir = TempDir::new().unwrap();
        let volumes_dir = temp_dir.path().join("volumes");
        let metadata_path = temp_dir.path().join("metadata_db");

        let engine =
            BitcaskStorageEngine::new(&volumes_dir, &metadata_path, 1024 * 1024, 4096, false)
                .await
                .unwrap();

        engine
            .put_object(
                "bucket",
                "prefix/key1",
                b"data1",
                "text/plain",
                &HashMap::new(),
            )
            .await
            .unwrap();
        engine
            .put_object(
                "bucket",
                "prefix/key2",
                b"data2",
                "text/plain",
                &HashMap::new(),
            )
            .await
            .unwrap();
        engine
            .put_object(
                "bucket",
                "other/key3",
                b"data3",
                "text/plain",
                &HashMap::new(),
            )
            .await
            .unwrap();

        let results = engine.list_objects("bucket", "prefix/", 10).await.unwrap();

        assert_eq!(results.len(), 2);
        assert!(results.iter().any(|(k, _)| k == "prefix/key1"));
        assert!(results.iter().any(|(k, _)| k == "prefix/key2"));
    }

    // -- Native composite multipart unit tests --------------------------------

    /// Helper: creates a test engine with 1GB max volume size.
    async fn create_test_engine(temp_dir: &TempDir) -> BitcaskStorageEngine {
        let volumes_dir = temp_dir.path().join("volumes");
        let metadata_path = temp_dir.path().join("metadata_db");
        BitcaskStorageEngine::new(
            &volumes_dir,
            &metadata_path,
            1024 * 1024 * 1024, // 1GB volumes
            4096,
            false,
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn test_native_multipart_session_lifecycle() {
        let temp_dir = TempDir::new().unwrap();
        let engine = create_test_engine(&temp_dir).await;

        let upload_id = "test-session-001";
        let metadata = HashMap::new();

        // Create session
        engine
            .create_multipart_session(
                upload_id,
                "bucket",
                "key.bin",
                "application/octet-stream",
                &metadata,
            )
            .await
            .unwrap();

        // Get session
        let session = engine.get_multipart_session(upload_id).await.unwrap();
        assert_eq!(session.upload_id, upload_id);
        assert_eq!(session.bucket, "bucket");
        assert_eq!(session.key, "key.bin");
        assert_eq!(session.content_type, "application/octet-stream");
        assert_eq!(session.state, MultipartUploadState::Open);

        // Mark completing
        let session = engine.mark_session_completing(upload_id).await.unwrap();
        assert_eq!(session.state, MultipartUploadState::Completing);

        // Get again — should still be Completing
        let session = engine.get_multipart_session(upload_id).await.unwrap();
        assert_eq!(session.state, MultipartUploadState::Completing);
    }

    #[tokio::test]
    async fn test_native_multipart_session_not_found() {
        let temp_dir = TempDir::new().unwrap();
        let engine = create_test_engine(&temp_dir).await;

        let result = engine.get_multipart_session("nonexistent").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_native_multipart_upload_part_and_list() {
        let temp_dir = TempDir::new().unwrap();
        let engine = create_test_engine(&temp_dir).await;

        let upload_id = "test-parts-001";
        engine
            .create_multipart_session(
                upload_id,
                "bucket",
                "key.bin",
                "application/octet-stream",
                &HashMap::new(),
            )
            .await
            .unwrap();

        // Upload 3 parts
        let part1_data = vec![0xAAu8; 1024];
        let part2_data = vec![0xBBu8; 2048];
        let part3_data = vec![0xCCu8; 512];

        let r1 = engine
            .upload_part_streaming(
                upload_id,
                1,
                Box::new(std::io::Cursor::new(part1_data.clone())),
                part1_data.len() as u64,
            )
            .await
            .unwrap();
        assert_eq!(r1.record.part_number, 1);
        assert_eq!(r1.record.size, 1024);
        assert!(!r1.etag.is_empty());

        let r2 = engine
            .upload_part_streaming(
                upload_id,
                2,
                Box::new(std::io::Cursor::new(part2_data.clone())),
                part2_data.len() as u64,
            )
            .await
            .unwrap();
        assert_eq!(r2.record.part_number, 2);
        assert_eq!(r2.record.size, 2048);

        let r3 = engine
            .upload_part_streaming(
                upload_id,
                3,
                Box::new(std::io::Cursor::new(part3_data.clone())),
                part3_data.len() as u64,
            )
            .await
            .unwrap();
        assert_eq!(r3.record.part_number, 3);

        // List parts
        let parts = engine.list_multipart_parts(upload_id).unwrap();
        assert_eq!(parts.len(), 3);
        assert_eq!(parts[0].part_number, 1);
        assert_eq!(parts[1].part_number, 2);
        assert_eq!(parts[2].part_number, 3);
        assert_eq!(parts[0].size, 1024);
        assert_eq!(parts[1].size, 2048);
        assert_eq!(parts[2].size, 512);
    }

    #[tokio::test]
    async fn test_native_multipart_part_overwrite() {
        let temp_dir = TempDir::new().unwrap();
        let engine = create_test_engine(&temp_dir).await;

        let upload_id = "test-overwrite-001";
        engine
            .create_multipart_session(
                upload_id,
                "bucket",
                "key.bin",
                "application/octet-stream",
                &HashMap::new(),
            )
            .await
            .unwrap();

        // Upload part 1
        let data_v1 = vec![0xAAu8; 1024];
        let r1 = engine
            .upload_part_streaming(upload_id, 1, Box::new(std::io::Cursor::new(data_v1)), 1024)
            .await
            .unwrap();

        // Overwrite part 1 with different data
        let data_v2 = vec![0xBBu8; 2048];
        let r2 = engine
            .upload_part_streaming(upload_id, 1, Box::new(std::io::Cursor::new(data_v2)), 2048)
            .await
            .unwrap();

        // ETags should differ (different content)
        assert_ne!(r1.etag, r2.etag);

        // List parts — should only show the latest version
        let parts = engine.list_multipart_parts(upload_id).unwrap();
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].size, 2048);
        assert_eq!(parts[0].etag_md5_hex, r2.etag);
    }

    #[tokio::test]
    async fn test_native_multipart_complete_and_read() {
        let temp_dir = TempDir::new().unwrap();
        let engine = create_test_engine(&temp_dir).await;

        let upload_id = "test-complete-001";
        let metadata = HashMap::new();
        engine
            .create_multipart_session(
                upload_id,
                "bucket",
                "key.bin",
                "application/octet-stream",
                &metadata,
            )
            .await
            .unwrap();

        // Upload 2 parts
        let part1 = vec![1u8; 1024];
        let part2 = vec![2u8; 512];

        let r1 = engine
            .upload_part_streaming(
                upload_id,
                1,
                Box::new(std::io::Cursor::new(part1.clone())),
                1024,
            )
            .await
            .unwrap();
        let r2 = engine
            .upload_part_streaming(
                upload_id,
                2,
                Box::new(std::io::Cursor::new(part2.clone())),
                512,
            )
            .await
            .unwrap();

        // Complete
        let selected = vec![r1.record.clone(), r2.record.clone()];
        engine.mark_session_completing(upload_id).await.unwrap();
        let record = engine
            .complete_multipart_native(
                "bucket",
                "key.bin",
                upload_id,
                &selected,
                "etag-test",
                "application/octet-stream",
                &metadata,
            )
            .await
            .unwrap();

        assert!(record.is_composite());
        assert_eq!(record.size, 1536); // 1024 + 512
        assert_eq!(record.etag, "etag-test");
        assert_eq!(record.content_type, "application/octet-stream");

        // Read back via get_object (tests CompositeObjectReader)
        let (data, rec) = engine.get_object("bucket", "key.bin").await.unwrap();
        assert_eq!(rec.etag, "etag-test");
        assert_eq!(data.len(), 1536);
        assert_eq!(&data[..1024], &part1[..]);
        assert_eq!(&data[1024..], &part2[..]);
    }

    #[tokio::test]
    async fn test_native_multipart_abort() {
        let temp_dir = TempDir::new().unwrap();
        let engine = create_test_engine(&temp_dir).await;

        let upload_id = "test-abort-001";
        engine
            .create_multipart_session(
                upload_id,
                "bucket",
                "key.bin",
                "application/octet-stream",
                &HashMap::new(),
            )
            .await
            .unwrap();

        // Upload a part
        let data = vec![0xFFu8; 1024];
        engine
            .upload_part_streaming(upload_id, 1, Box::new(std::io::Cursor::new(data)), 1024)
            .await
            .unwrap();

        // Abort
        engine.abort_multipart_native(upload_id).await.unwrap();

        // Session should be gone
        assert!(engine.get_multipart_session(upload_id).await.is_err());

        // Parts should be gone
        let parts = engine.list_multipart_parts(upload_id).unwrap();
        assert!(parts.is_empty());
    }

    #[tokio::test]
    async fn test_native_multipart_delete_composite_object() {
        let temp_dir = TempDir::new().unwrap();
        let engine = create_test_engine(&temp_dir).await;

        let upload_id = "test-delete-composite-001";
        let metadata = HashMap::new();
        engine
            .create_multipart_session(
                upload_id,
                "bucket",
                "key.bin",
                "application/octet-stream",
                &metadata,
            )
            .await
            .unwrap();

        let part1 = vec![1u8; 256];
        let part2 = vec![2u8; 256];

        let r1 = engine
            .upload_part_streaming(upload_id, 1, Box::new(std::io::Cursor::new(part1)), 256)
            .await
            .unwrap();
        let r2 = engine
            .upload_part_streaming(upload_id, 2, Box::new(std::io::Cursor::new(part2)), 256)
            .await
            .unwrap();

        engine.mark_session_completing(upload_id).await.unwrap();
        engine
            .complete_multipart_native(
                "bucket",
                "key.bin",
                upload_id,
                &[r1.record, r2.record],
                "etag",
                "application/octet-stream",
                &metadata,
            )
            .await
            .unwrap();

        // Verify object exists
        assert!(engine.get_object("bucket", "key.bin").await.is_ok());

        // Delete composite object
        engine.delete_object("bucket", "key.bin").await.unwrap();

        // Should be gone
        assert!(engine.get_object("bucket", "key.bin").await.is_err());
    }

    #[tokio::test]
    async fn test_native_multipart_head_object() {
        let temp_dir = TempDir::new().unwrap();
        let engine = create_test_engine(&temp_dir).await;

        let upload_id = "test-head-001";
        let mut metadata = HashMap::new();
        metadata.insert("custom-key".to_string(), "custom-value".to_string());

        engine
            .create_multipart_session(upload_id, "bucket", "head.bin", "video/mp4", &metadata)
            .await
            .unwrap();

        let part1 = vec![0xABu8; 100];
        let r1 = engine
            .upload_part_streaming(upload_id, 1, Box::new(std::io::Cursor::new(part1)), 100)
            .await
            .unwrap();

        engine.mark_session_completing(upload_id).await.unwrap();
        engine
            .complete_multipart_native(
                "bucket",
                "head.bin",
                upload_id,
                &[r1.record],
                "head-etag",
                "video/mp4",
                &metadata,
            )
            .await
            .unwrap();

        // HEAD
        let record = engine.head_object("bucket", "head.bin").await.unwrap();
        assert_eq!(record.size, 100);
        assert_eq!(record.content_type, "video/mp4");
        assert_eq!(record.etag, "head-etag");
        assert!(record.is_composite());
        assert_eq!(
            record.metadata.get("custom-key"),
            Some(&"custom-value".to_string())
        );
    }
}
