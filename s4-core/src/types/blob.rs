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

//! Blob header structure for append-only log entries.

use serde::{Deserialize, Serialize};

/// Header written to disk before each blob in the append-only log.
///
/// This header is needed for disaster recovery if the metadata database is lost.
/// It contains all information necessary to reconstruct the index.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlobHeader {
    /// CRC32 checksum of the blob data (protection against bit rot)
    pub crc: u32,
    /// Timestamp when the blob was written (Unix epoch in nanoseconds)
    pub timestamp: u64,
    /// Length of the object key (bucket/path)
    pub key_len: u32,
    /// Length of the blob data
    pub blob_len: u64,
    /// Flags for the blob (deleted marker, etc.)
    pub flags: BlobHeaderFlags,
}

/// Flags for blob header.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
pub struct BlobHeaderFlags {
    /// True if this is a tombstone marker (deleted object)
    pub is_deleted: bool,
    /// True if this is part of a multipart upload
    pub is_multipart: bool,
    /// Part number for multipart uploads
    pub part_number: Option<u32>,
}

impl BlobHeader {
    /// Creates a new blob header.
    pub fn new(key_len: u32, blob_len: u64, crc: u32) -> Self {
        Self {
            crc,
            timestamp: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64,
            key_len,
            blob_len,
            flags: BlobHeaderFlags::default(),
        }
    }

    /// Returns the total size of the header when serialized.
    ///
    /// This uses bincode to calculate the exact serialized size of the header.
    /// The result matches what will actually be written to disk.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails, as this would indicate a serious problem
    /// that could lead to data corruption.
    pub fn serialized_size(&self) -> Result<usize, bincode::Error> {
        bincode::serialized_size(self).map(|size| size as usize)
    }
}
