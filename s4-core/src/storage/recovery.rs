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

//! Crash recovery implementation.
//!
//! This module provides two recovery strategies:
//!
//! 1. **Journal recovery (Phase 5, full-fidelity):** If the metadata journal
//!    is not empty, replay all journal entries in sequence order to reconstruct
//!    the objects, versions, and dedup keyspaces. This preserves `content_type`,
//!    user metadata, Object Lock fields, and delete markers — information that
//!    is lost during volume-only recovery.
//!
//! 2. **Volume-only recovery (legacy, lossy):** If the journal is empty (first
//!    startup or pre-journal data), fall back to scanning volume files and
//!    reconstructing the index from blob headers. This loses `content_type`,
//!    metadata, Object Lock, and version ordering information.
//!
//! The [`recover_index`] function automatically selects the best strategy.

use crate::error::StorageError;
use crate::storage::journal::{BucketOp, IamOp, JournalEventType, MetadataJournal};
use crate::storage::{Deduplicator, IndexDb, VolumeReader};
use crate::types::IndexRecord;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::Path;
use tokio::fs;

/// Recovers the index database using the best available strategy.
///
/// If the metadata journal contains entries, replays them for full-fidelity
/// recovery (preserving content_type, metadata, Object Lock fields, and
/// version ordering). Otherwise, falls back to legacy volume-only recovery.
///
/// # Arguments
///
/// * `volumes_dir` - Directory containing volume files
/// * `index_db` - Index database to populate
/// * `journal` - Metadata journal to check and replay
/// * `deduplicator` - Deduplicator to rebuild during recovery
///
/// # Returns
///
/// Returns the number of records recovered.
pub async fn recover_index(
    volumes_dir: &Path,
    index_db: &IndexDb,
    journal: &MetadataJournal,
    deduplicator: &Deduplicator,
) -> Result<usize, StorageError> {
    if !journal.is_empty()? {
        tracing::info!("Journal is not empty, performing full-fidelity journal recovery");
        recover_index_from_journal(index_db, journal, deduplicator).await
    } else {
        tracing::info!("Journal is empty, falling back to legacy volume-only recovery");
        recover_index_from_volumes(volumes_dir, index_db).await
    }
}

/// Replays journal entries to reconstruct the full index state.
///
/// This is the preferred recovery path (Phase 5). Unlike volume-only recovery,
/// journal replay preserves:
/// - `content_type` (exact MIME type, not a default)
/// - User metadata (`x-amz-meta-*` headers)
/// - Object Lock fields (`retention_mode`, `retain_until_timestamp`, `legal_hold`)
/// - Delete marker metadata
/// - Version ordering
///
/// The journal is processed in sequence order. Each event type is handled:
/// - `ObjectPut` → insert the record (with version list update if versioned)
/// - `ObjectDelete` → remove the record (with version list update if versioned)
/// - `DeleteMarkerCreated` → insert the delete marker record + version list
/// - `MetadataUpdate` → update fields on the existing record
/// - `BucketOperation` → create/delete bucket marker
/// - `IamOperation` → create/delete IAM record
///
/// Dedup entries are rebuilt from `ObjectPut` records that have non-zero content hashes.
async fn recover_index_from_journal(
    index_db: &IndexDb,
    journal: &MetadataJournal,
    deduplicator: &Deduplicator,
) -> Result<usize, StorageError> {
    let entries = journal.iter_entries()?;
    let total = entries.len();
    tracing::info!("Replaying {} journal entries for recovery", total);

    let mut recovered = 0usize;

    for entry in entries {
        match entry.event {
            JournalEventType::ObjectPut {
                bucket,
                key,
                version_id,
                record,
            } => {
                let full_key = match &version_id {
                    Some(vid) => format!("{}/{}#{}", bucket, key, vid),
                    None => format!("{}/{}", bucket, key),
                };
                index_db.put(&full_key, &record).await?;

                // Update version list if versioned
                if let Some(vid) = &version_id {
                    let mut vlist = index_db.get_version_list(&bucket, &key).await?;
                    vlist.add_version(vid.clone(), false);
                    index_db.put_version_list(&bucket, &key, &vlist).await?;
                }

                // Rebuild dedup entry (skip delete markers and zero hashes)
                if !record.is_delete_marker && record.content_hash != [0u8; 32] {
                    let dedup_op = deduplicator.make_register_op(
                        record.content_hash,
                        record.file_id,
                        record.offset,
                    )?;
                    index_db.batch_write(vec![dedup_op]).await?;
                }

                recovered += 1;
            }

            JournalEventType::ObjectDelete {
                bucket,
                key,
                version_id,
            } => {
                let full_key = match &version_id {
                    Some(vid) => format!("{}/{}#{}", bucket, key, vid),
                    None => format!("{}/{}", bucket, key),
                };

                // Get existing record for dedup unregistration before deleting
                if let Some(existing) = index_db.get(&full_key).await? {
                    if !existing.is_delete_marker && existing.content_hash != [0u8; 32] {
                        if let Ok(Some(op)) =
                            deduplicator.make_unregister_op(&existing.content_hash)
                        {
                            index_db.batch_write(vec![op]).await?;
                        }
                    }
                }

                index_db.delete(&full_key).await?;

                // Update version list if versioned
                if let Some(vid) = &version_id {
                    let mut vlist = index_db.get_version_list(&bucket, &key).await?;
                    vlist.remove_version(vid);
                    index_db.put_version_list(&bucket, &key, &vlist).await?;
                }

                recovered += 1;
            }

            JournalEventType::DeleteMarkerCreated {
                bucket,
                key,
                marker_version_id,
                record,
            } => {
                let full_key = format!("{}/{}#{}", bucket, key, marker_version_id);
                index_db.put(&full_key, &record).await?;

                // Update version list
                let mut vlist = index_db.get_version_list(&bucket, &key).await?;
                vlist.add_version(marker_version_id, true);
                index_db.put_version_list(&bucket, &key, &vlist).await?;

                recovered += 1;
            }

            JournalEventType::MetadataUpdate {
                bucket,
                key,
                version_id,
                updated_fields,
            } => {
                let full_key = match &version_id {
                    Some(vid) => format!("{}/{}#{}", bucket, key, vid),
                    None => format!("{}/{}", bucket, key),
                };

                if let Some(mut record) = index_db.get(&full_key).await? {
                    apply_metadata_updates_from_journal(&mut record, &updated_fields);
                    index_db.put(&full_key, &record).await?;
                    recovered += 1;
                }
            }

            JournalEventType::BucketOperation { bucket, op } => {
                let marker_key = format!("__system__/__s4_bucket_marker_{}", bucket);
                match op {
                    BucketOp::Created => {
                        let record =
                            IndexRecord::new(0, 0, 0, [0u8; 32], String::new(), String::new());
                        index_db.put(&marker_key, &record).await?;
                    }
                    BucketOp::Deleted => {
                        index_db.delete(&marker_key).await?;
                    }
                    BucketOp::VersioningChanged { .. } => {
                        // Versioning status is part of bucket config, not the marker record.
                        // The marker just needs to exist.
                    }
                }
                recovered += 1;
            }

            JournalEventType::IamOperation { op } => {
                match op {
                    IamOp::UserCreated { user_id } => {
                        let iam_key = format!("__system__/__s4_iam_user_{}", user_id);
                        let record =
                            IndexRecord::new(0, 0, 0, [0u8; 32], String::new(), String::new());
                        index_db.put(&iam_key, &record).await?;
                    }
                    IamOp::UserDeleted { user_id } => {
                        let iam_key = format!("__system__/__s4_iam_user_{}", user_id);
                        index_db.delete(&iam_key).await?;
                    }
                    IamOp::AccessKeyCreated { access_key_id } => {
                        let iam_key = format!("__system__/__s4_iam_key_{}", access_key_id);
                        let record =
                            IndexRecord::new(0, 0, 0, [0u8; 32], String::new(), String::new());
                        index_db.put(&iam_key, &record).await?;
                    }
                    IamOp::AccessKeyDeleted { access_key_id } => {
                        let iam_key = format!("__system__/__s4_iam_key_{}", access_key_id);
                        index_db.delete(&iam_key).await?;
                    }
                }
                recovered += 1;
            }
        }
    }

    tracing::info!(
        "Journal recovery complete: {} events replayed, {} records recovered",
        total,
        recovered
    );
    Ok(recovered)
}

/// Applies metadata field updates from a `MetadataUpdate` journal event to a record.
///
/// This function is also used by `StateMachine::apply` in bitcask.rs.
pub(crate) fn apply_metadata_updates_from_journal(
    record: &mut IndexRecord,
    updated_fields: &HashMap<String, String>,
) {
    for (field, value) in updated_fields {
        match field.as_str() {
            "retention_mode" => {
                record.retention_mode = if value == "GOVERNANCE" {
                    Some(crate::types::RetentionMode::GOVERNANCE)
                } else if value == "COMPLIANCE" {
                    Some(crate::types::RetentionMode::COMPLIANCE)
                } else {
                    None
                };
            }
            "retain_until_timestamp" => {
                record.retain_until_timestamp = value.parse::<u64>().ok();
            }
            "legal_hold" => {
                record.legal_hold = value == "true";
            }
            "content_type" => {
                record.content_type.clone_from(value);
            }
            _ => {
                // User metadata or unknown field — store in metadata map
                record.metadata.insert(field.clone(), value.clone());
            }
        }
    }
}

/// Recovers the index database by scanning all volume files.
///
/// This is the legacy (lossy) recovery path. It scans volume files and
/// reconstructs index records from blob headers. Information not stored in
/// volume headers (content_type, user metadata, Object Lock fields, version
/// ordering) is lost.
///
/// # Arguments
///
/// * `volumes_dir` - Directory containing volume files
/// * `index_db` - Index database to populate
///
/// # Returns
///
/// Returns the number of records recovered.
pub async fn recover_index_from_volumes(
    volumes_dir: &Path,
    index_db: &IndexDb,
) -> Result<usize, StorageError> {
    let mut recovered_count = 0;
    let reader = VolumeReader::new(volumes_dir);

    // Find all volume files
    let mut entries = fs::read_dir(volumes_dir).await.map_err(StorageError::Io)?;
    let mut volume_files = Vec::new();

    while let Some(entry) = entries.next_entry().await.map_err(StorageError::Io)? {
        let path = entry.path();
        if path.is_file() {
            if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
                if filename.starts_with("volume_") && filename.ends_with(".dat") {
                    // Extract volume ID from filename
                    if let Some(id_str) =
                        filename.strip_prefix("volume_").and_then(|s| s.strip_suffix(".dat"))
                    {
                        if let Ok(volume_id) = id_str.parse::<u32>() {
                            volume_files.push((volume_id, path));
                        }
                    }
                }
            }
        }
    }

    // Sort by volume ID to process in order
    volume_files.sort_by_key(|(id, _)| *id);

    // Scan each volume file
    for (volume_id, volume_path) in volume_files {
        let recovered = scan_volume_file(&reader, volume_id, &volume_path, index_db).await?;
        recovered_count += recovered;
    }

    // Reconstruct bucket markers from recovered object keys.
    // Keys are stored as "bucket/path", so extract unique bucket names
    // and create bucket marker entries in the buckets keyspace.
    if recovered_count > 0 {
        let all_objects = index_db.list_objects("", usize::MAX).await?;
        let mut buckets_seen = std::collections::HashSet::new();
        for (key, _) in &all_objects {
            if let Some(bucket) = key.split('/').next() {
                if !bucket.is_empty()
                    && !bucket.starts_with("__system__")
                    && !bucket.starts_with("__s4_")
                {
                    buckets_seen.insert(bucket.to_string());
                }
            }
        }

        for bucket_name in &buckets_seen {
            let marker_key = format!("__system__/__s4_bucket_marker_{}", bucket_name);
            let marker_record = IndexRecord::new(0, 0, 0, [0u8; 32], String::new(), String::new());
            index_db.put(&marker_key, &marker_record).await?;
        }

        if !buckets_seen.is_empty() {
            tracing::info!(
                "Volume recovery: recreated {} bucket marker(s)",
                buckets_seen.len()
            );
        }
    }

    Ok(recovered_count)
}

/// Scans a single volume file and recovers all blob records.
async fn scan_volume_file(
    reader: &VolumeReader,
    volume_id: u32,
    volume_path: &Path,
    index_db: &IndexDb,
) -> Result<usize, StorageError> {
    use tokio::fs::File;

    let file = File::open(volume_path).await.map_err(StorageError::Io)?;
    let file_size = file.metadata().await.map_err(StorageError::Io)?.len();
    let mut offset = 0u64;
    let mut recovered = 0;

    loop {
        // Check if we've reached the end of the file
        if offset >= file_size {
            break;
        }

        // Try to read a blob using VolumeReader (which handles header deserialization correctly)
        let result = reader.read_blob(volume_id, offset).await;
        match result {
            Ok((header, key, data)) => {
                // Verify CRC if header has it
                let computed_crc = crc32fast::hash(&data);
                if header.crc != 0 && header.crc != computed_crc {
                    // CRC mismatch - skip this blob
                    // Calculate total size and move to next blob
                    let header_size = header
                        .serialized_size()
                        .map_err(|e| StorageError::Serialization(e.to_string()))?
                        as u64;
                    let total_size = header_size + header.key_len as u64 + header.blob_len;
                    offset += total_size;
                    continue;
                }

                // Phase 5: Check if this blob is a delete marker written to volume
                let is_delete_marker = header.flags.is_delete_marker;

                // Compute content hash
                let mut hasher = Sha256::new();
                hasher.update(&data);
                let content_hash: [u8; 32] = hasher.finalize().into();

                // Create index record
                let etag = if is_delete_marker {
                    String::new()
                } else {
                    hex::encode(content_hash)
                };
                let record = IndexRecord {
                    file_id: volume_id,
                    offset,
                    size: header.blob_len,
                    etag,
                    content_type: if is_delete_marker {
                        String::new()
                    } else {
                        "application/octet-stream".to_string() // Default, lost in crash
                    },
                    metadata: HashMap::new(), // Lost in crash (unless journal recovery is used)
                    content_hash: if is_delete_marker {
                        [0u8; 32]
                    } else {
                        content_hash
                    },
                    created_at: header.timestamp,
                    modified_at: header.timestamp,
                    version_id: None,
                    is_delete_marker,
                    // Object Lock fields - lost in crash, default to no lock
                    retention_mode: None,
                    retain_until_timestamp: None,
                    legal_hold: false,
                    // Native composite multipart & federation fields
                    layout: None,
                    pool_id: 0,
                    hlc_timestamp: 0,
                    hlc_logical: 0,
                    origin_node_id: 0,
                    operation_id: 0,
                };

                // Store in index
                // The key read from volume is the full_key (bucket/path) format
                // This ensures crash recovery can reconstruct the correct index
                index_db.put(&key, &record).await?;

                recovered += 1;

                // Calculate total size and move to next blob
                let header_size = header
                    .serialized_size()
                    .map_err(|e| StorageError::Serialization(e.to_string()))?
                    as u64;
                let total_size = header_size + header.key_len as u64 + header.blob_len;
                offset += total_size;
            }
            Err(StorageError::Serialization(_)) => {
                // Invalid header - might be end of file or corruption
                // Try to find next valid header by scanning byte by byte
                offset += 1;
                if offset >= file_size {
                    break;
                }
            }
            Err(StorageError::InvalidData(_)) => {
                // Invalid data (e.g., invalid UTF-8 in key) - skip this blob
                // Try to find next valid header by scanning byte by byte
                offset += 1;
                if offset >= file_size {
                    break;
                }
            }
            Err(StorageError::Io(_)) => {
                // IO error - likely end of file (read_exact couldn't read enough data)
                // This means we've reached the end of valid data, so stop scanning
                break;
            }
            Err(StorageError::VolumeNotFound { .. }) => {
                // Volume file not found - this shouldn't happen in recovery
                // but if it does, stop scanning
                break;
            }
            Err(e) => {
                // Other unexpected error - stop scanning
                return Err(e);
            }
        }
    }

    Ok(recovered)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::index::BatchAction;
    use crate::storage::{IndexDb, VolumeWriter};
    use crate::types::BlobHeaderFlags;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_recover_index_from_volumes() {
        let temp_dir = TempDir::new().unwrap();
        let volumes_dir = temp_dir.path();
        let metadata_path = temp_dir.path().join("metadata_db");

        // Create a volume with some data
        let mut writer = VolumeWriter::new(volumes_dir, 1024 * 1024).await.unwrap();

        let key = "bucket/key";
        let data = b"test data";
        let header = crate::types::BlobHeader {
            crc: crc32fast::hash(data),
            timestamp: 1000000,
            key_len: key.len() as u32,
            blob_len: data.len() as u64,
            flags: BlobHeaderFlags::default(),
        };

        let (volume_id, offset) = writer.write_blob(&header, key, data).await.unwrap();

        // Sync and drop writer to ensure file is closed and data is flushed
        writer.sync().await.unwrap();
        drop(writer);

        // Create a new index database (simulating lost database)
        let index_db = IndexDb::new(&metadata_path).unwrap();

        // Recover index from volumes
        let recovered = recover_index_from_volumes(volumes_dir, &index_db).await.unwrap();

        assert_eq!(recovered, 1);

        // Verify we can retrieve the record
        let record = index_db.get("bucket/key").await.unwrap();
        assert!(record.is_some());
        let record = record.unwrap();
        assert_eq!(record.file_id, volume_id);
        assert_eq!(record.offset, offset);
        assert_eq!(record.size, 9);
    }

    /// Helper: creates IndexDb + MetadataJournal + Deduplicator for testing.
    fn create_test_env(temp_dir: &TempDir) -> (IndexDb, MetadataJournal, Deduplicator) {
        let index_db = IndexDb::new(&temp_dir.path().join("metadata_db")).unwrap();
        let journal = MetadataJournal::new(
            index_db.journal_keyspace(),
            crate::storage::placement::PlacementGroupId::LOCAL,
        )
        .unwrap();
        let deduplicator = Deduplicator::new(
            index_db.dedup_keyspace(),
            crate::storage::placement::PlacementGroupId::LOCAL,
        );
        (index_db, journal, deduplicator)
    }

    /// Helper: writes a journal entry directly to the keyspace (simulating batch_write).
    fn write_journal_op(journal: &MetadataJournal, event: JournalEventType) {
        let op = journal.make_journal_entry_op(event).unwrap();
        if let BatchAction::Put(key, value) = op.action {
            journal.keyspace().insert(&key, &value).unwrap();
        }
    }

    fn make_test_record(file_id: u32, offset: u64, size: u64) -> IndexRecord {
        IndexRecord::new(
            file_id,
            offset,
            size,
            [1u8; 32], // non-zero content hash
            "abc123".to_string(),
            "text/plain".to_string(),
        )
    }

    #[tokio::test]
    async fn test_journal_recovery_object_put() {
        let temp_dir = TempDir::new().unwrap();
        let (index_db, journal, deduplicator) = create_test_env(&temp_dir);

        // Write journal entries
        let record = make_test_record(1, 100, 50);
        write_journal_op(
            &journal,
            JournalEventType::ObjectPut {
                bucket: "mybucket".to_string(),
                key: "mykey.txt".to_string(),
                version_id: None,
                record: record.clone(),
            },
        );

        // Recovery
        let recovered =
            recover_index_from_journal(&index_db, &journal, &deduplicator).await.unwrap();
        assert_eq!(recovered, 1);

        // Verify the record was restored with full fidelity
        let restored = index_db.get("mybucket/mykey.txt").await.unwrap().unwrap();
        assert_eq!(restored.file_id, 1);
        assert_eq!(restored.offset, 100);
        assert_eq!(restored.size, 50);
        assert_eq!(restored.content_type, "text/plain");
        assert_eq!(restored.etag, "abc123");
    }

    #[tokio::test]
    async fn test_journal_recovery_versioned_put() {
        let temp_dir = TempDir::new().unwrap();
        let (index_db, journal, deduplicator) = create_test_env(&temp_dir);

        let record = make_test_record(1, 100, 50);
        write_journal_op(
            &journal,
            JournalEventType::ObjectPut {
                bucket: "mybucket".to_string(),
                key: "mykey.txt".to_string(),
                version_id: Some("v1".to_string()),
                record: record.clone(),
            },
        );

        let recovered =
            recover_index_from_journal(&index_db, &journal, &deduplicator).await.unwrap();
        assert_eq!(recovered, 1);

        // Verify versioned record
        let restored = index_db.get("mybucket/mykey.txt#v1").await.unwrap().unwrap();
        assert_eq!(restored.file_id, 1);

        // Verify version list was rebuilt
        let vlist = index_db.get_version_list("mybucket", "mykey.txt").await.unwrap();
        assert_eq!(vlist.versions, vec!["v1"]);
        assert_eq!(vlist.current_version, Some("v1".to_string()));
    }

    #[tokio::test]
    async fn test_journal_recovery_delete_marker() {
        let temp_dir = TempDir::new().unwrap();
        let (index_db, journal, deduplicator) = create_test_env(&temp_dir);

        // First put a version
        let record = make_test_record(1, 100, 50);
        write_journal_op(
            &journal,
            JournalEventType::ObjectPut {
                bucket: "mybucket".to_string(),
                key: "mykey.txt".to_string(),
                version_id: Some("v1".to_string()),
                record,
            },
        );

        // Then create a delete marker
        let dm_record = IndexRecord::new_delete_marker("dm1".to_string());
        write_journal_op(
            &journal,
            JournalEventType::DeleteMarkerCreated {
                bucket: "mybucket".to_string(),
                key: "mykey.txt".to_string(),
                marker_version_id: "dm1".to_string(),
                record: dm_record,
            },
        );

        let recovered =
            recover_index_from_journal(&index_db, &journal, &deduplicator).await.unwrap();
        assert_eq!(recovered, 2);

        // Verify delete marker exists
        let dm = index_db.get("mybucket/mykey.txt#dm1").await.unwrap().unwrap();
        assert!(dm.is_delete_marker);

        // Verify version list: current should be None (delete marker is latest)
        let vlist = index_db.get_version_list("mybucket", "mykey.txt").await.unwrap();
        assert_eq!(vlist.versions, vec!["dm1", "v1"]);
        assert_eq!(vlist.current_version, None);
    }

    #[tokio::test]
    async fn test_journal_recovery_object_delete() {
        let temp_dir = TempDir::new().unwrap();
        let (index_db, journal, deduplicator) = create_test_env(&temp_dir);

        // Put then delete
        let record = make_test_record(1, 100, 50);
        write_journal_op(
            &journal,
            JournalEventType::ObjectPut {
                bucket: "mybucket".to_string(),
                key: "mykey.txt".to_string(),
                version_id: None,
                record,
            },
        );
        write_journal_op(
            &journal,
            JournalEventType::ObjectDelete {
                bucket: "mybucket".to_string(),
                key: "mykey.txt".to_string(),
                version_id: None,
            },
        );

        let recovered =
            recover_index_from_journal(&index_db, &journal, &deduplicator).await.unwrap();
        assert_eq!(recovered, 2);

        // Verify the record is gone
        let restored = index_db.get("mybucket/mykey.txt").await.unwrap();
        assert!(restored.is_none());
    }

    #[tokio::test]
    async fn test_journal_recovery_metadata_update() {
        let temp_dir = TempDir::new().unwrap();
        let (index_db, journal, deduplicator) = create_test_env(&temp_dir);

        // Put an object
        let record = make_test_record(1, 100, 50);
        write_journal_op(
            &journal,
            JournalEventType::ObjectPut {
                bucket: "mybucket".to_string(),
                key: "mykey.txt".to_string(),
                version_id: None,
                record,
            },
        );

        // Update metadata (add retention)
        let mut fields = HashMap::new();
        fields.insert("retention_mode".to_string(), "COMPLIANCE".to_string());
        fields.insert("legal_hold".to_string(), "true".to_string());
        write_journal_op(
            &journal,
            JournalEventType::MetadataUpdate {
                bucket: "mybucket".to_string(),
                key: "mykey.txt".to_string(),
                version_id: None,
                updated_fields: fields,
            },
        );

        let recovered =
            recover_index_from_journal(&index_db, &journal, &deduplicator).await.unwrap();
        assert_eq!(recovered, 2);

        let restored = index_db.get("mybucket/mykey.txt").await.unwrap().unwrap();
        assert_eq!(
            restored.retention_mode,
            Some(crate::types::RetentionMode::COMPLIANCE)
        );
        assert!(restored.legal_hold);
    }

    #[tokio::test]
    async fn test_recover_index_prefers_journal() {
        let temp_dir = TempDir::new().unwrap();
        let volumes_dir = temp_dir.path().join("volumes");
        std::fs::create_dir_all(&volumes_dir).unwrap();
        let (index_db, journal, deduplicator) = create_test_env(&temp_dir);

        // Write a journal entry with full-fidelity metadata
        let mut record = make_test_record(1, 100, 50);
        record.content_type = "image/png".to_string();
        record.metadata.insert("x-amz-meta-author".to_string(), "test".to_string());
        write_journal_op(
            &journal,
            JournalEventType::ObjectPut {
                bucket: "mybucket".to_string(),
                key: "photo.png".to_string(),
                version_id: None,
                record,
            },
        );

        // Use the unified recover_index which should prefer journal
        let recovered =
            recover_index(&volumes_dir, &index_db, &journal, &deduplicator).await.unwrap();
        assert_eq!(recovered, 1);

        // Verify full-fidelity metadata was preserved
        let restored = index_db.get("mybucket/photo.png").await.unwrap().unwrap();
        assert_eq!(restored.content_type, "image/png");
        assert_eq!(
            restored.metadata.get("x-amz-meta-author"),
            Some(&"test".to_string())
        );
    }

    #[tokio::test]
    async fn test_recover_index_falls_back_to_volumes() {
        let temp_dir = TempDir::new().unwrap();
        let volumes_dir = temp_dir.path();
        let (index_db, journal, deduplicator) = create_test_env(&temp_dir);

        // Write a volume file (no journal entries)
        let mut writer = VolumeWriter::new(volumes_dir, 1024 * 1024).await.unwrap();
        let key = "bucket/fallback.txt";
        let data = b"fallback data";
        let header = crate::types::BlobHeader {
            crc: crc32fast::hash(data),
            timestamp: 2000000,
            key_len: key.len() as u32,
            blob_len: data.len() as u64,
            flags: BlobHeaderFlags::default(),
        };
        writer.write_blob(&header, key, data).await.unwrap();
        writer.sync().await.unwrap();
        drop(writer);

        // Journal is empty → should fall back to volume recovery
        assert!(journal.is_empty().unwrap());
        let recovered =
            recover_index(volumes_dir, &index_db, &journal, &deduplicator).await.unwrap();
        assert_eq!(recovered, 1);

        // Volume recovery loses content_type (defaults to application/octet-stream)
        let restored = index_db.get("bucket/fallback.txt").await.unwrap().unwrap();
        assert_eq!(restored.content_type, "application/octet-stream");
    }

    #[tokio::test]
    async fn test_journal_recovery_bucket_operations() {
        let temp_dir = TempDir::new().unwrap();
        let (index_db, journal, deduplicator) = create_test_env(&temp_dir);

        write_journal_op(
            &journal,
            JournalEventType::BucketOperation {
                bucket: "test-bucket".to_string(),
                op: BucketOp::Created,
            },
        );

        let recovered =
            recover_index_from_journal(&index_db, &journal, &deduplicator).await.unwrap();
        assert_eq!(recovered, 1);

        // Verify bucket marker exists
        let marker = index_db.get("__system__/__s4_bucket_marker_test-bucket").await.unwrap();
        assert!(marker.is_some());
    }
}
