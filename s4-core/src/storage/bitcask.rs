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
//! - Tiny objects (< 4KB) stored inline in the metadata database
//! - All other objects stored in append-only volume files
//! - Content-addressable deduplication
//! - S3-compatible object versioning

use crate::error::StorageError;
use crate::storage::engine::{DeleteMarkerEntry, DeleteResult, ListVersionsResult, ObjectVersion};
use crate::storage::{
    generate_version_id, Deduplicator, IndexDb, VersionList, VolumeReader, VolumeWriter,
    NULL_VERSION_ID,
};
use crate::types::{
    BlobHeader, DefaultRetention, IndexRecord, VersioningStatus, DELETE_MARKER_FILE_ID,
};
use crate::StorageEngine;
use base64::Engine;
use serde::Serialize;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;

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
/// - IndexDb for metadata storage
/// - Deduplicator for content-addressable storage
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
    /// Deduplicator
    deduplicator: Arc<RwLock<Deduplicator>>,
    /// Threshold for inline storage (default: 4KB)
    inline_threshold: usize,
    /// Whether to fsync after each write (strict consistency)
    strict_sync: bool,
}

impl BitcaskStorageEngine {
    /// Creates a new Bitcask storage engine.
    ///
    /// # Arguments
    ///
    /// * `volumes_dir` - Directory where volume files are stored
    /// * `metadata_path` - Path to the redb metadata database
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

        // Create deduplicator
        let mut deduplicator = Deduplicator::new();

        // Build deduplicator map from existing index
        // This is important for crash recovery
        let all_records = index_db.list("", usize::MAX).await?;
        deduplicator.build_from_index(all_records.into_iter());

        let deduplicator = Arc::new(RwLock::new(deduplicator));

        Ok(Self {
            volumes_dir,
            metadata_path,
            volume_writer,
            index_db,
            deduplicator,
            inline_threshold,
            strict_sync,
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

    /// Recovers the index database from volume files.
    ///
    /// This method scans all volume files and reconstructs the index database.
    /// Useful for crash recovery when the metadata database is lost.
    ///
    /// # Returns
    ///
    /// Returns the number of records recovered.
    pub async fn recover_index(&self) -> Result<usize, StorageError> {
        use crate::storage::recover_index_from_volumes;
        recover_index_from_volumes(&self.volumes_dir, &self.index_db).await
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
        let all_records = self.index_db.list("", usize::MAX).await?;

        let mut objects_count: u64 = 0;
        let mut storage_used_bytes: u64 = 0;

        // Count buckets via __s4_bucket_marker_ keys in __system__ bucket
        let system_records = self
            .index_db
            .list("__system__/__s4_bucket_marker_", usize::MAX)
            .await
            .unwrap_or_default();
        let buckets_count =
            system_records.iter().filter(|(_, r)| !r.is_delete_marker).count() as u64;

        for (key, record) in &all_records {
            // Skip internal keys (version lists, IAM data, etc.)
            if key.starts_with("__s4_") {
                continue;
            }
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

        let dedup = self.deduplicator.read().await;
        let (dedup_unique_blobs, dedup_total_references) = dedup.stats();

        Ok(StorageStats {
            buckets_count,
            objects_count,
            storage_used_bytes,
            dedup_unique_blobs,
            dedup_total_references,
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

    /// Creates the key for storing the version list.
    ///
    /// Format: `__s4_versions_{bucket}/{key}`
    fn make_version_list_key(&self, bucket: &str, key: &str) -> String {
        format!("__s4_versions_{}/{}", bucket, key)
    }

    /// Retrieves the version list for an object.
    async fn get_version_list(&self, bucket: &str, key: &str) -> Result<VersionList, StorageError> {
        let list_key = self.make_version_list_key(bucket, key);
        match self.index_db.get(&list_key).await? {
            Some(record) => {
                // Version list is stored as JSON in _inline_data
                let json = record
                    .metadata
                    .get("_inline_data")
                    .ok_or_else(|| StorageError::InvalidData("Version list data missing".into()))?;
                let decoded =
                    base64::engine::general_purpose::STANDARD.decode(json).map_err(|e| {
                        StorageError::InvalidData(format!("Base64 decode error: {}", e))
                    })?;
                serde_json::from_slice(&decoded)
                    .map_err(|e| StorageError::Serialization(format!("JSON parse error: {}", e)))
            }
            None => Ok(VersionList::new()),
        }
    }

    /// Saves the version list for an object.
    async fn save_version_list(
        &self,
        bucket: &str,
        key: &str,
        list: &VersionList,
    ) -> Result<(), StorageError> {
        let list_key = self.make_version_list_key(bucket, key);

        if list.is_empty() {
            // Delete the version list if empty
            self.index_db.delete(&list_key).await?;
        } else {
            // Store version list as inline data
            let json = serde_json::to_vec(list)
                .map_err(|e| StorageError::Serialization(format!("JSON serialize error: {}", e)))?;

            let mut record = IndexRecord::new(
                u32::MAX, // Inline storage
                0,
                json.len() as u64,
                [0u8; 32],
                String::new(),
                "application/json".to_string(),
            );
            record.metadata.insert(
                "_inline_data".to_string(),
                base64::engine::general_purpose::STANDARD.encode(&json),
            );

            self.index_db.put(&list_key, &record).await?;
        }

        Ok(())
    }

    /// Tries to reuse existing deduplicated content, incrementing the ref count.
    ///
    /// Returns `Some(IndexRecord)` if content already exists in a volume,
    /// or `None` if the data must be written fresh.
    fn try_reuse_existing(
        dedup: &mut Deduplicator,
        content_hash: [u8; 32],
        data_len: u64,
        etag: &str,
        content_type: &str,
    ) -> Option<IndexRecord> {
        let existing = dedup.check_existing(&content_hash)?;
        let (volume_id, offset) = existing;
        if volume_id == u32::MAX {
            // Inline objects should not be in deduplicator — treat as new
            return None;
        }
        // Increment ref count so deletion of one copy doesn't break dedup for others
        dedup.register_content(content_hash, volume_id, offset);
        Some(IndexRecord::new(
            volume_id,
            offset,
            data_len,
            content_hash,
            etag.to_string(),
            content_type.to_string(),
        ))
    }

    /// Internal method to write object data and return the record.
    async fn write_object_data(
        &self,
        full_key: &str,
        data: &[u8],
        content_type: &str,
        metadata: &HashMap<String, String>,
    ) -> Result<(IndexRecord, String), StorageError> {
        let content_hash = Deduplicator::compute_hash(data);
        let etag = Self::compute_etag(data);

        // Check for deduplication
        let mut dedup = self.deduplicator.write().await;
        let reused = Self::try_reuse_existing(
            &mut dedup,
            content_hash,
            data.len() as u64,
            &etag,
            content_type,
        );

        let record = if let Some(record) = reused {
            record
        } else if data.len() < self.inline_threshold {
            let mut record = IndexRecord::new(
                u32::MAX,
                0,
                data.len() as u64,
                content_hash,
                etag.clone(),
                content_type.to_string(),
            );
            record.metadata.insert(
                "_inline_data".to_string(),
                base64::engine::general_purpose::STANDARD.encode(data),
            );
            record
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

            dedup.register_content(content_hash, volume_id, offset);

            IndexRecord::new(
                volume_id,
                offset,
                data.len() as u64,
                content_hash,
                etag.clone(),
                content_type.to_string(),
            )
        };

        drop(dedup);

        let mut record = record;
        record.metadata.extend(metadata.clone());
        record.modified_at = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64;

        Ok((record, etag))
    }

    /// Reads object data from a record.
    async fn read_object_data(&self, record: &IndexRecord) -> Result<Vec<u8>, StorageError> {
        if record.file_id == u32::MAX || record.file_id == DELETE_MARKER_FILE_ID {
            // Inline storage or delete marker check
            if record.is_delete_marker {
                return Err(StorageError::DeleteMarker {
                    key: String::new(),
                    version_id: record.version_id.clone().unwrap_or_default(),
                });
            }
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

    /// Recalculates and sets the current version from the version list.
    ///
    /// Scans from newest to oldest to find the first non-delete-marker.
    async fn recalculate_current_version(
        &self,
        bucket: &str,
        key: &str,
        version_list: &mut VersionList,
    ) -> Result<(), StorageError> {
        let full_key = self.make_key(bucket, key);

        // Find first non-delete-marker version
        for vid in version_list.versions.iter() {
            let version_key = self.make_version_key(bucket, key, vid);
            if let Some(record) = self.index_db.get(&version_key).await? {
                if !record.is_delete_marker {
                    version_list.set_current_version(Some(vid.clone()));
                    self.index_db.put(&full_key, &record).await?;
                    return Ok(());
                }
            }
        }

        // No non-delete-marker found, delete current pointer
        version_list.set_current_version(None);
        self.index_db.delete(&full_key).await?;
        Ok(())
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
        let content_hash = Deduplicator::compute_hash(data);
        let etag = Self::compute_etag(data);

        // Check for deduplication
        let mut dedup = self.deduplicator.write().await;
        let reused = Self::try_reuse_existing(
            &mut dedup,
            content_hash,
            data.len() as u64,
            &etag,
            content_type,
        );

        let record = if let Some(record) = reused {
            record
        } else if data.len() < self.inline_threshold {
            // Tiny object - store inline in database
            // For tiny objects, we store data in a special metadata field
            // Use u32::MAX for file_id to indicate "inline storage"
            // (cannot use 0 because volume_000000.dat at offset 0 would conflict)
            let mut record = IndexRecord::new(
                u32::MAX,
                0,
                data.len() as u64,
                content_hash,
                etag.clone(),
                content_type.to_string(),
            );
            // Store data in metadata with special key
            record.metadata.insert(
                "_inline_data".to_string(),
                base64::engine::general_purpose::STANDARD.encode(data),
            );
            record
        } else {
            // Regular object - write to volume
            // Use full_key for crash recovery - volume must contain bucket/path
            let mut writer = self.volume_writer.write().await;
            let (volume_id, offset) = writer
                .write_blob(
                    &BlobHeader::new(
                        full_key.len() as u32,
                        data.len() as u64,
                        crc32fast::hash(data),
                    ),
                    &full_key,
                    data,
                )
                .await?;

            if self.strict_sync {
                writer.sync().await?;
            }

            drop(writer);

            // Register in deduplicator
            dedup.register_content(content_hash, volume_id, offset);

            IndexRecord::new(
                volume_id,
                offset,
                data.len() as u64,
                content_hash,
                etag.clone(),
                content_type.to_string(),
            )
        };

        // Update metadata
        let mut record = record;
        record.metadata.extend(metadata.clone());
        record.modified_at = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64;

        // Store in index
        self.index_db.put(&full_key, &record).await?;

        drop(dedup);
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

        // Check if it's a tiny object (inline)
        // Inline objects have file_id = u32::MAX
        let data = if record.file_id == u32::MAX {
            // Inline storage - get from metadata
            let inline_data_str = record
                .metadata
                .get("_inline_data")
                .ok_or_else(|| StorageError::InvalidData("Inline data not found".to_string()))?;
            base64::engine::general_purpose::STANDARD.decode(inline_data_str).map_err(|e| {
                StorageError::InvalidData(format!("Failed to decode inline data: {}", e))
            })?
        } else {
            // Read from volume
            let reader = VolumeReader::new(&self.volumes_dir);
            let (_header, _read_key, data) =
                reader.read_blob(record.file_id, record.offset).await?;
            data
        };

        // Verify checksum
        let computed_hash = Deduplicator::compute_hash(&data);
        if computed_hash != record.content_hash {
            return Err(StorageError::ChecksumMismatch { key: full_key });
        }

        Ok((data, record))
    }

    async fn delete_object(&self, bucket: &str, key: &str) -> Result<(), StorageError> {
        let full_key = self.make_key(bucket, key);

        // Get record to check content hash for deduplicator
        if let Some(record) = self.index_db.get(&full_key).await? {
            // Only unregister if not inline (inline objects don't use deduplication)
            // Inline objects have file_id = u32::MAX
            if record.file_id != u32::MAX {
                let mut dedup = self.deduplicator.write().await;
                dedup.unregister_content(&record.content_hash);
            }
        }

        // Delete from index
        self.index_db.delete(&full_key).await?;

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

    // ========================================================================
    // Versioning Methods Implementation
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

                // Write object data
                let (mut record, etag) =
                    self.write_object_data(&version_key, data, content_type, metadata).await?;
                record.version_id = Some(version_id.clone());

                // Store versioned record
                self.index_db.put(&version_key, &record).await?;

                // Update version list
                let mut version_list = self.get_version_list(bucket, key).await?;
                version_list.add_version(version_id.clone(), false);
                self.save_version_list(bucket, key, &version_list).await?;

                // Update current pointer
                self.index_db.put(&full_key, &record).await?;

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
                let (mut record, etag) =
                    self.write_object_data(&version_key, data, content_type, metadata).await?;
                record.version_id = Some(version_id.clone());

                // Store versioned record
                self.index_db.put(&version_key, &record).await?;

                // Add to version list
                version_list.add_version(version_id.clone(), false);
                self.save_version_list(bucket, key, &version_list).await?;

                // Update current pointer
                self.index_db.put(&full_key, &record).await?;

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

                // Remove from dedup if not inline
                if record.file_id != u32::MAX && record.file_id != DELETE_MARKER_FILE_ID {
                    let mut dedup = self.deduplicator.write().await;
                    dedup.unregister_content(&record.content_hash);
                }

                // Delete the versioned record
                self.index_db.delete(&version_key).await?;

                // Update version list
                let mut version_list = self.get_version_list(bucket, key).await?;
                version_list.remove_version(vid);

                // Recalculate current version
                self.recalculate_current_version(bucket, key, &mut version_list).await?;
                self.save_version_list(bucket, key, &version_list).await?;

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

                // Create delete marker record
                let marker = IndexRecord::new_delete_marker(marker_version_id.clone());

                // Store delete marker
                self.index_db.put(&version_key, &marker).await?;

                // Update version list
                let mut version_list = self.get_version_list(bucket, key).await?;
                version_list.add_version(marker_version_id.clone(), true);
                self.save_version_list(bucket, key, &version_list).await?;

                // Delete current pointer (object appears deleted)
                self.index_db.delete(&full_key).await?;

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
                if let Some(old_record) = self.index_db.get(&old_null_key).await? {
                    // Unregister old content from dedup
                    if old_record.file_id != u32::MAX && old_record.file_id != DELETE_MARKER_FILE_ID
                    {
                        let mut dedup = self.deduplicator.write().await;
                        dedup.unregister_content(&old_record.content_hash);
                    }
                    self.index_db.delete(&old_null_key).await?;
                }
                version_list.remove_version(&marker_version_id);

                // Create delete marker record
                let marker = IndexRecord::new_delete_marker(marker_version_id.clone());

                // Store delete marker
                self.index_db.put(&version_key, &marker).await?;

                // Update version list
                version_list.add_version(marker_version_id.clone(), true);
                self.save_version_list(bucket, key, &version_list).await?;

                // Delete current pointer
                self.index_db.delete(&full_key).await?;

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
        // List all version list keys for this bucket/prefix
        let version_list_prefix = format!("__s4_versions_{}/{}", bucket, prefix);
        let version_list_entries = self.index_db.list(&version_list_prefix, usize::MAX).await?;

        let mut result = ListVersionsResult::default();
        let mut count = 0;
        let mut past_marker = key_marker.is_none();

        for (list_key, _) in version_list_entries {
            // Extract object key from version list key
            let key_part = list_key
                .strip_prefix(&format!("__s4_versions_{}/", bucket))
                .unwrap_or(&list_key);

            // Handle key marker pagination
            if !past_marker {
                if Some(key_part) == key_marker {
                    past_marker = true;
                }
                if !past_marker {
                    continue;
                }
            }

            // Get the version list for this key
            let version_list = self.get_version_list(bucket, key_part).await?;

            let mut past_version_marker =
                version_id_marker.is_none() || key_marker != Some(key_part);

            for (i, vid) in version_list.versions.iter().enumerate() {
                // Handle version ID marker pagination
                if !past_version_marker {
                    if Some(vid.as_str()) == version_id_marker {
                        past_version_marker = true;
                    }
                    continue;
                }

                if count >= max_keys {
                    result.is_truncated = true;
                    result.next_key_marker = Some(key_part.to_string());
                    result.next_version_id_marker = Some(vid.clone());
                    return Ok(result);
                }

                // Get version record
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

                    count += 1;
                }
            }
        }

        Ok(result)
    }

    // ========================================================================
    // Object Lock Methods (Phase 3)
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
                // For unversioned buckets, store with retention but no version ID
                let full_key = self.make_key(bucket, key);

                // Write object data
                let (mut record, etag) =
                    self.write_object_data(&full_key, data, content_type, metadata).await?;

                // Apply default retention
                record.retention_mode = Some(retention.mode);
                record.retain_until_timestamp =
                    Some(retention.calculate_retain_until(record.created_at));

                // Update index
                self.index_db.put(&full_key, &record).await?;

                Ok((etag, None))
            }
            VersioningStatus::Enabled => {
                // Generate new version ID
                let version_id = generate_version_id();
                let full_key = self.make_key(bucket, key);
                let version_key = self.make_version_key(bucket, key, &version_id);

                // Write object data
                let (mut record, etag) =
                    self.write_object_data(&version_key, data, content_type, metadata).await?;
                record.version_id = Some(version_id.clone());

                // Apply default retention
                record.retention_mode = Some(retention.mode);
                record.retain_until_timestamp =
                    Some(retention.calculate_retain_until(record.created_at));

                // Store versioned record
                self.index_db.put(&version_key, &record).await?;

                // Update version list
                let mut version_list = self.get_version_list(bucket, key).await?;
                version_list.add_version(version_id.clone(), false);
                self.save_version_list(bucket, key, &version_list).await?;

                // Update current pointer
                self.index_db.put(&full_key, &record).await?;

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
                let (mut record, etag) =
                    self.write_object_data(&version_key, data, content_type, metadata).await?;
                record.version_id = Some(version_id.clone());

                // Apply default retention
                record.retention_mode = Some(retention.mode);
                record.retain_until_timestamp =
                    Some(retention.calculate_retain_until(record.created_at));

                // Store versioned record
                self.index_db.put(&version_key, &record).await?;

                // Add to version list
                version_list.add_version(version_id.clone(), false);
                self.save_version_list(bucket, key, &version_list).await?;

                // Update current pointer
                self.index_db.put(&full_key, &record).await?;

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
        let version_key = self.make_version_key(bucket, key, version_id);

        // Verify version exists
        let existing = self.index_db.get(&version_key).await?.ok_or_else(|| {
            StorageError::VersionNotFound {
                key: key.to_string(),
                version_id: version_id.to_string(),
            }
        })?;

        // Verify it's not a delete marker (cannot lock delete markers)
        if existing.is_delete_marker {
            return Err(StorageError::InvalidOperation {
                operation: "update_object_metadata".to_string(),
                reason: "Cannot update metadata of delete marker".to_string(),
            });
        }

        // Update versioned record in index
        self.index_db.put(&version_key, &updated_record).await?;

        // If this is the current version, also update the current pointer
        let version_list = self.get_version_list(bucket, key).await?;
        if version_list.current_version.as_deref() == Some(version_id) {
            let full_key = self.make_key(bucket, key);
            self.index_db.put(&full_key, &updated_record).await?;
        }

        Ok(())
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
        let metadata_path = temp_dir.path().join("metadata.redb");

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
    async fn test_inline_storage() {
        let temp_dir = TempDir::new().unwrap();
        let volumes_dir = temp_dir.path().join("volumes");
        let metadata_path = temp_dir.path().join("metadata.redb");

        let engine =
            BitcaskStorageEngine::new(&volumes_dir, &metadata_path, 1024 * 1024, 4096, false)
                .await
                .unwrap();

        // Small object should be stored inline
        let data = b"tiny";
        engine
            .put_object("bucket", "tiny", data, "text/plain", &HashMap::new())
            .await
            .unwrap();

        let (retrieved_data, record) = engine.get_object("bucket", "tiny").await.unwrap();

        assert_eq!(retrieved_data, data);
        assert_eq!(record.file_id, u32::MAX); // Inline storage marker
        assert_eq!(record.offset, 0);
    }

    #[tokio::test]
    async fn test_deduplication() {
        let temp_dir = TempDir::new().unwrap();
        let volumes_dir = temp_dir.path().join("volumes");
        let metadata_path = temp_dir.path().join("metadata.redb");

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
        let metadata_path = temp_dir.path().join("metadata.redb");

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
        let metadata_path = temp_dir.path().join("metadata.redb");

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
}
