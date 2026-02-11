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

//! Metadata record structure for the index database.

use crate::types::RetentionMode;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Sentinel value for delete markers (no actual data storage).
pub const DELETE_MARKER_FILE_ID: u32 = u32::MAX;

/// Record stored in the fast index database (redb).
///
/// This is the "map" that points to where the actual bytes are stored.
/// For delete markers, `is_delete_marker` is true and no actual data is stored.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexRecord {
    /// Volume file ID (e.g., 42 -> "volume_042.dat").
    /// For delete markers, this is set to `DELETE_MARKER_FILE_ID`.
    pub file_id: u32,
    /// Byte offset from the start of the volume file
    pub offset: u64,
    /// Size of the object in bytes
    pub size: u64,

    // S3 metadata
    /// ETag (usually MD5 or SHA256 hash)
    pub etag: String,
    /// Content type (MIME type)
    pub content_type: String,
    /// User-defined metadata (x-amz-meta-* headers)
    pub metadata: HashMap<String, String>,

    // For deduplication
    /// SHA-256 hash of the content (for Content-Addressable Storage)
    pub content_hash: [u8; 32],

    // Additional metadata
    /// Creation timestamp
    pub created_at: u64,
    /// Last modification timestamp
    pub modified_at: u64,
    /// Version ID (for versioning support)
    pub version_id: Option<String>,
    /// True if this record is a delete marker (S3 versioning).
    /// Delete markers indicate that the object was deleted but versions are preserved.
    #[serde(default)]
    pub is_delete_marker: bool,

    // Object Lock fields (Phase 3)
    /// Object Lock retention mode (GOVERNANCE or COMPLIANCE).
    ///
    /// When set, the object cannot be deleted until `retain_until_timestamp`.
    /// None if no retention is set on this version.
    #[serde(default)]
    pub retention_mode: Option<RetentionMode>,

    /// Retain-until date as Unix timestamp (nanoseconds).
    ///
    /// Object cannot be deleted before this timestamp if retention is set.
    /// Timestamp format matches `created_at` and `modified_at` fields.
    #[serde(default)]
    pub retain_until_timestamp: Option<u64>,

    /// Legal hold status (ON = true, OFF = false).
    ///
    /// When true, object cannot be deleted regardless of retention period.
    /// Independent of retention settings and can be toggled separately.
    #[serde(default)]
    pub legal_hold: bool,
}

impl IndexRecord {
    /// Creates a new index record.
    pub fn new(
        file_id: u32,
        offset: u64,
        size: u64,
        content_hash: [u8; 32],
        etag: String,
        content_type: String,
    ) -> Self {
        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64;
        Self {
            file_id,
            offset,
            size,
            etag,
            content_type,
            metadata: HashMap::new(),
            content_hash,
            created_at: now,
            modified_at: now,
            version_id: None,
            is_delete_marker: false,
            // Object Lock fields default to no lock
            retention_mode: None,
            retain_until_timestamp: None,
            legal_hold: false,
        }
    }

    /// Creates a delete marker record (no actual data storage).
    ///
    /// Delete markers are used when versioning is enabled and an object is deleted
    /// without specifying a version ID. The marker indicates the object is "deleted"
    /// while preserving all previous versions.
    pub fn new_delete_marker(version_id: String) -> Self {
        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64;
        Self {
            file_id: DELETE_MARKER_FILE_ID,
            offset: 0,
            size: 0,
            etag: String::new(),
            content_type: String::new(),
            metadata: HashMap::new(),
            content_hash: [0u8; 32],
            created_at: now,
            modified_at: now,
            version_id: Some(version_id),
            is_delete_marker: true,
            // Object Lock fields - delete markers cannot be locked
            retention_mode: None,
            retain_until_timestamp: None,
            legal_hold: false,
        }
    }

    /// Checks if this record represents actual object data (not a delete marker).
    pub fn is_data_record(&self) -> bool {
        !self.is_delete_marker
    }

    /// Checks if this record references the same content as another record.
    pub fn has_same_content(&self, other: &Self) -> bool {
        self.content_hash == other.content_hash
    }
}
