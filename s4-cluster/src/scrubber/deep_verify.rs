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

//! Deep SHA-256 content hash verification.
//!
//! CRC32 catches random bit flips in volume data. Deep verification
//! additionally validates the SHA-256 content hash stored in the
//! [`IndexRecord`] against the actual blob data, catching more severe
//! corruption or silent data mutation.
//!
//! This is a utility module — the background service that runs deep
//! verification periodically is an Enterprise Edition feature gated
//! by the [`DeepScrubber`](crate::traits::DeepScrubber) trait.

use sha2::{Digest, Sha256};

use s4_core::error::StorageError;
use s4_core::storage::VolumeReader;
use s4_core::types::IndexRecord;

/// Result of verifying a single object's content integrity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VerifyResult {
    /// Object data matches its stored SHA-256 hash.
    Healthy,
    /// CRC32 mismatch — data corrupted at the volume level.
    CrcMismatch {
        /// CRC32 stored in the blob header.
        expected_crc: u32,
        /// CRC32 computed from the actual data on disk.
        actual_crc: u32,
    },
    /// SHA-256 content hash mismatch — data does not match index record.
    ContentCorrupted {
        /// SHA-256 stored in the index record.
        expected_hash: [u8; 32],
        /// SHA-256 computed from the actual data on disk.
        actual_hash: [u8; 32],
    },
    /// Object is a delete marker or inline — nothing to verify on disk.
    Skipped,
}

/// Verify a single object's CRC32 and SHA-256 integrity.
///
/// Reads the blob from the volume file and checks:
/// 1. CRC32 of the data against the blob header
/// 2. SHA-256 of the data against the index record's `content_hash`
///
/// Returns [`VerifyResult::Skipped`] for delete markers and inline objects.
pub async fn deep_verify_object(
    reader: &VolumeReader,
    record: &IndexRecord,
    volume_id: u32,
    offset: u64,
) -> Result<VerifyResult, StorageError> {
    // Skip delete markers and inline objects (file_id == u32::MAX)
    if record.is_delete_marker || record.file_id == u32::MAX {
        return Ok(VerifyResult::Skipped);
    }

    let (header, _key, data) = reader.read_blob(volume_id, offset).await?;

    // Step 1: CRC32 verification
    let actual_crc = crc32fast::hash(&data);
    if actual_crc != header.crc {
        return Ok(VerifyResult::CrcMismatch {
            expected_crc: header.crc,
            actual_crc,
        });
    }

    // Step 2: SHA-256 content hash verification
    let actual_hash: [u8; 32] = Sha256::digest(&data).into();
    if actual_hash != record.content_hash {
        return Ok(VerifyResult::ContentCorrupted {
            expected_hash: record.content_hash,
            actual_hash,
        });
    }

    Ok(VerifyResult::Healthy)
}

#[cfg(test)]
mod tests {
    use super::*;
    use s4_core::storage::VolumeWriter;
    use s4_core::types::BlobHeader;
    use tempfile::TempDir;

    async fn setup_volume_with_blob(
        data: &[u8],
        key: &str,
    ) -> (TempDir, VolumeReader, u32, u64, BlobHeader) {
        let dir = TempDir::new().unwrap();
        let mut writer = VolumeWriter::new(dir.path(), 1024 * 1024).await.unwrap();

        let crc = crc32fast::hash(data);
        let header = BlobHeader::new(key.len() as u32, data.len() as u64, crc);
        let (vol_id, offset) = writer.write_blob(&header, key, data).await.unwrap();
        writer.sync().await.unwrap();

        let reader = VolumeReader::new(dir.path());
        (dir, reader, vol_id, offset, header)
    }

    #[tokio::test]
    async fn healthy_object_passes_deep_verify() {
        let data = b"hello deep verify";
        let hash: [u8; 32] = Sha256::digest(data).into();

        let (_dir, reader, vol_id, offset, _header) =
            setup_volume_with_blob(data, "bucket/key").await;

        let record = IndexRecord::new(
            vol_id,
            offset,
            data.len() as u64,
            hash,
            "etag".into(),
            "text/plain".into(),
        );

        let result = deep_verify_object(&reader, &record, vol_id, offset).await.unwrap();
        assert_eq!(result, VerifyResult::Healthy);
    }

    #[tokio::test]
    async fn hash_mismatch_detected() {
        let data = b"original data";
        let wrong_hash = [0xFFu8; 32];

        let (_dir, reader, vol_id, offset, _header) =
            setup_volume_with_blob(data, "bucket/key").await;

        let record = IndexRecord::new(
            vol_id,
            offset,
            data.len() as u64,
            wrong_hash,
            "etag".into(),
            "text/plain".into(),
        );

        let result = deep_verify_object(&reader, &record, vol_id, offset).await.unwrap();
        match result {
            VerifyResult::ContentCorrupted {
                expected_hash,
                actual_hash,
            } => {
                assert_eq!(expected_hash, wrong_hash);
                assert_ne!(actual_hash, wrong_hash);
            }
            other => panic!("expected ContentCorrupted, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn delete_marker_is_skipped() {
        let record = IndexRecord::new_delete_marker("v1".into());

        let dir = TempDir::new().unwrap();
        let reader = VolumeReader::new(dir.path());

        let result = deep_verify_object(&reader, &record, 0, 0).await.unwrap();
        assert_eq!(result, VerifyResult::Skipped);
    }

    #[tokio::test]
    async fn inline_object_is_skipped() {
        let mut record = IndexRecord::new(
            u32::MAX,
            0,
            10,
            [0u8; 32],
            "etag".into(),
            "text/plain".into(),
        );
        record.file_id = u32::MAX;

        let dir = TempDir::new().unwrap();
        let reader = VolumeReader::new(dir.path());

        let result = deep_verify_object(&reader, &record, u32::MAX, 0).await.unwrap();
        assert_eq!(result, VerifyResult::Skipped);
    }
}
