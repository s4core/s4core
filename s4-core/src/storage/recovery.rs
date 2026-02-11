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
//! This module provides functionality to recover the index database from volume files
//! in case the metadata database is lost or corrupted.

use crate::error::StorageError;
use crate::storage::{IndexDb, VolumeReader};
use crate::types::IndexRecord;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::Path;
use tokio::fs;

/// Recovers the index database by scanning all volume files.
///
/// This function scans all volume files in the volumes directory, reads each blob header,
/// and reconstructs the index database. This is useful for crash recovery when the
/// metadata database is lost.
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

                // Compute content hash
                let mut hasher = Sha256::new();
                hasher.update(&data);
                let content_hash: [u8; 32] = hasher.finalize().into();

                // Create index record
                let etag = hex::encode(content_hash);
                let record = IndexRecord {
                    file_id: volume_id,
                    offset,
                    size: header.blob_len,
                    etag,
                    content_type: "application/octet-stream".to_string(), // Default, lost in crash
                    metadata: HashMap::new(),                             // Lost in crash
                    content_hash,
                    created_at: header.timestamp,
                    modified_at: header.timestamp,
                    version_id: None,
                    is_delete_marker: false, // Data records are never delete markers
                    // Object Lock fields - lost in crash, default to no lock
                    retention_mode: None,
                    retain_until_timestamp: None,
                    legal_hold: false,
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
    use crate::storage::{IndexDb, VolumeWriter};
    use crate::types::BlobHeaderFlags;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_recover_index_from_volumes() {
        let temp_dir = TempDir::new().unwrap();
        let volumes_dir = temp_dir.path();
        let metadata_path = temp_dir.path().join("metadata.redb");

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
}
