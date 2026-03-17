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

//! Volume scrubber — CRC32 integrity verification for stored blobs.
//!
//! Scans volume files and verifies each blob's CRC32 checksum against its
//! stored data. Reports any corruption detected.

use s4_core::error::StorageError;
use s4_core::storage::VolumeReader;
use std::path::PathBuf;
use tracing::{info, warn};

/// Details of a single corrupted blob found during scrubbing.
#[derive(Debug, Clone)]
pub struct ScrubError {
    /// Volume ID where corruption was found.
    pub volume_id: u32,
    /// Offset of the corrupted blob.
    pub offset: u64,
    /// Object key.
    pub key: String,
    /// Expected CRC32 (from header).
    pub expected_crc: u32,
    /// Actual CRC32 (computed from data).
    pub actual_crc: u32,
}

/// Aggregate scrub results.
#[derive(Debug, Clone, Default)]
pub struct ScrubResult {
    /// Total blobs checked.
    pub blobs_checked: u64,
    /// Blobs with valid CRC.
    pub blobs_ok: u64,
    /// Blobs with CRC mismatch.
    pub blobs_corrupted: u64,
    /// Volume files scanned.
    pub volumes_scanned: u64,
    /// Details of corrupted blobs.
    pub errors: Vec<ScrubError>,
}

/// Volume integrity scrubber.
///
/// Reads every blob in every volume file and verifies the CRC32 checksum
/// stored in the blob header matches the actual data on disk.
pub struct VolumeScrubber {
    volumes_dir: PathBuf,
}

impl VolumeScrubber {
    /// Creates a new scrubber.
    pub fn new(volumes_dir: impl Into<PathBuf>) -> Self {
        Self {
            volumes_dir: volumes_dir.into(),
        }
    }

    /// Scrubs all volume files for CRC32 integrity.
    pub async fn scrub_all(&self) -> Result<ScrubResult, StorageError> {
        let mut result = ScrubResult::default();
        let mut entries = tokio::fs::read_dir(&self.volumes_dir).await.map_err(StorageError::Io)?;

        let mut volume_ids = Vec::new();
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

        for vol_id in volume_ids {
            match self.scrub_volume(vol_id).await {
                Ok(vol_result) => {
                    result.blobs_checked += vol_result.blobs_checked;
                    result.blobs_ok += vol_result.blobs_ok;
                    result.blobs_corrupted += vol_result.blobs_corrupted;
                    result.errors.extend(vol_result.errors);
                    result.volumes_scanned += 1;
                }
                Err(e) => {
                    warn!("Scrubber: failed to scrub volume {}: {}", vol_id, e);
                }
            }
        }

        info!(
            "Scrub complete: {} blobs checked, {} ok, {} corrupted across {} volumes",
            result.blobs_checked, result.blobs_ok, result.blobs_corrupted, result.volumes_scanned,
        );

        Ok(result)
    }

    /// Scrubs a single volume file.
    pub async fn scrub_volume(&self, volume_id: u32) -> Result<ScrubResult, StorageError> {
        let reader = VolumeReader::new(&self.volumes_dir);
        let volume_path = self.volumes_dir.join(format!("volume_{:06}.dat", volume_id));
        let file_size = tokio::fs::metadata(&volume_path).await.map_err(StorageError::Io)?.len();

        let mut result = ScrubResult::default();
        let mut offset = 0u64;

        while offset < file_size {
            match reader.read_blob(volume_id, offset).await {
                Ok((header, key, data)) => {
                    result.blobs_checked += 1;

                    let actual_crc = crc32fast::hash(&data);
                    if actual_crc == header.crc {
                        result.blobs_ok += 1;
                    } else {
                        result.blobs_corrupted += 1;
                        result.errors.push(ScrubError {
                            volume_id,
                            offset,
                            key,
                            expected_crc: header.crc,
                            actual_crc,
                        });
                    }

                    let header_size = header
                        .serialized_size()
                        .map_err(|e| StorageError::Serialization(e.to_string()))?
                        as u64;
                    offset += header_size + header.key_len as u64 + header.blob_len;
                }
                Err(_) => break,
            }
        }

        result.volumes_scanned = 1;
        Ok(result)
    }
}
