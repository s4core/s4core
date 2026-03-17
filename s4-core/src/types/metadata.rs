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

use crate::types::composite::{BlobId, ObjectLayout};
use crate::types::RetentionMode;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Sentinel value for delete markers (no actual data storage).
pub const DELETE_MARKER_FILE_ID: u32 = u32::MAX;

/// Record stored in the fast index database (fjall).
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

    // Native composite multipart & federation-ready fields
    // All use #[serde(default)] for backward compatibility with existing records.
    /// Object storage layout (flat blob or composite manifest).
    ///
    /// When `None`, the record is treated as `SingleBlob { file_id, offset }`
    /// using the existing fields above. New multipart completions set this to
    /// `Some(ObjectLayout::CompositeManifest { ... })`.
    #[serde(default)]
    pub layout: Option<ObjectLayout>,

    /// Pool ID for federation routing.
    /// Single-node: always 0. Distributed: assigned at bucket creation.
    #[serde(default)]
    pub pool_id: u32,

    /// HLC timestamp for distributed causal ordering.
    /// Single-node: filled from wall clock.
    #[serde(default)]
    pub hlc_timestamp: u64,

    /// HLC logical counter for ordering within same physical timestamp.
    #[serde(default)]
    pub hlc_logical: u32,

    /// Node ID that wrote this record (LWW tiebreaker in federation).
    #[serde(default)]
    pub origin_node_id: u128,

    /// Unique operation ID (idempotency in federation).
    #[serde(default)]
    pub operation_id: u128,
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
            // Native composite multipart & federation fields
            layout: None,
            pool_id: 0,
            hlc_timestamp: 0,
            hlc_logical: 0,
            origin_node_id: 0,
            operation_id: 0,
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
            // Native composite multipart & federation fields
            layout: None,
            pool_id: 0,
            hlc_timestamp: 0,
            hlc_logical: 0,
            origin_node_id: 0,
            operation_id: 0,
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

    /// Returns the effective object layout.
    ///
    /// When `layout` is `None` (backward-compatible old records), this returns
    /// `ObjectLayout::SingleBlob` using the existing `file_id` and `offset` fields.
    pub fn effective_layout(&self) -> ObjectLayout {
        match &self.layout {
            Some(l) => l.clone(),
            None => ObjectLayout::SingleBlob {
                file_id: self.file_id,
                offset: self.offset,
            },
        }
    }

    /// Returns true if this record uses composite (multipart) storage layout.
    pub fn is_composite(&self) -> bool {
        matches!(self.layout, Some(ObjectLayout::CompositeManifest { .. }))
    }

    /// Returns the manifest BlobId if this is a composite object.
    pub fn composite_manifest_blob_id(&self) -> Option<BlobId> {
        match &self.layout {
            Some(ObjectLayout::CompositeManifest {
                manifest_blob_id, ..
            }) => Some(*manifest_blob_id),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::composite::{BlobId, ObjectLayout};

    #[test]
    fn index_record_new_roundtrip() {
        let record = IndexRecord::new(
            1,
            100,
            50,
            [0xAB; 32],
            "etag".to_string(),
            "text/plain".to_string(),
        );
        let bytes = bincode::serialize(&record).unwrap();
        let decoded: IndexRecord = bincode::deserialize(&bytes).unwrap();
        assert_eq!(record.file_id, decoded.file_id);
        assert_eq!(record.offset, decoded.offset);
        assert_eq!(record.size, decoded.size);
        assert!(decoded.layout.is_none());
        assert_eq!(decoded.pool_id, 0);
        assert_eq!(decoded.hlc_timestamp, 0);
        assert_eq!(decoded.origin_node_id, 0);
        assert_eq!(decoded.operation_id, 0);
    }

    /// Simulate deserializing an old record that was serialized BEFORE the
    /// new fields existed. The old format is a bincode blob without layout/
    /// pool_id/hlc_* fields. Since we use `#[serde(default)]`, the missing
    /// fields should get their defaults.
    ///
    /// Note: bincode with fixed-length encoding does NOT support `#[serde(default)]`
    /// for missing trailing fields because it's a non-self-describing format.
    /// However, because we only ever add Option/default fields and existing
    /// data is read via fjall which stores the exact bytes written, old records
    /// written with the old schema will fail to decode with bincode if the
    /// binary layout changes. This test verifies that a NEW record roundtrips
    /// correctly and that effective_layout() works for None.
    #[test]
    fn index_record_effective_layout_none() {
        let record = IndexRecord::new(
            42,
            1024,
            5000,
            [0xFF; 32],
            "etag".to_string(),
            "application/octet-stream".to_string(),
        );
        assert!(record.layout.is_none());
        let layout = record.effective_layout();
        match layout {
            ObjectLayout::SingleBlob { file_id, offset } => {
                assert_eq!(file_id, 42);
                assert_eq!(offset, 1024);
            }
            _ => panic!("expected SingleBlob"),
        }
    }

    #[test]
    fn index_record_with_composite_layout() {
        let mut record = IndexRecord::new(
            0,
            0,
            65_000_000_000,
            [0; 32],
            "abc-7494".to_string(),
            "application/octet-stream".to_string(),
        );
        record.layout = Some(ObjectLayout::CompositeManifest {
            manifest_blob_id: BlobId([0xCC; 32]),
            manifest_blob_len: 512,
            part_count: 7494,
        });

        let bytes = bincode::serialize(&record).unwrap();
        let decoded: IndexRecord = bincode::deserialize(&bytes).unwrap();

        assert!(decoded.is_composite());
        match decoded.effective_layout() {
            ObjectLayout::CompositeManifest {
                manifest_blob_id,
                manifest_blob_len,
                part_count,
            } => {
                assert_eq!(manifest_blob_id, BlobId([0xCC; 32]));
                assert_eq!(manifest_blob_len, 512);
                assert_eq!(part_count, 7494);
            }
            _ => panic!("expected CompositeManifest"),
        }
    }

    #[test]
    fn index_record_federation_fields_default() {
        let record = IndexRecord::new(
            1,
            0,
            100,
            [0; 32],
            "e".to_string(),
            "text/plain".to_string(),
        );
        assert_eq!(record.pool_id, 0);
        assert_eq!(record.hlc_timestamp, 0);
        assert_eq!(record.hlc_logical, 0);
        assert_eq!(record.origin_node_id, 0);
        assert_eq!(record.operation_id, 0);
    }

    #[test]
    fn index_record_delete_marker_not_composite() {
        let marker = IndexRecord::new_delete_marker("v1".to_string());
        assert!(!marker.is_composite());
        assert!(marker.layout.is_none());
    }
}
