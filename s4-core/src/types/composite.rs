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

//! Types for native composite multipart objects.
//!
//! These types support the storage model where multipart uploads produce
//! composite objects referencing ordered segments, rather than rewriting
//! all part bytes into a single flat blob at completion time.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

// ---------------------------------------------------------------------------
// BlobId — placement-scoped logical blob identifier
// ---------------------------------------------------------------------------

/// Placement-scoped logical blob identifier.
///
/// In single-node MVP, `BlobId` equals the SHA-256 hash of the blob content.
/// This uniquely identifies content and is resolved to local `(volume_id, offset)`
/// via [`BlobRefEntry`] at read time.
///
/// In federation, `BlobId` remains the same — each node resolves it to its own
/// local [`BlobLocation`] independently.
///
/// **Critical invariant:** `BlobId` must NEVER be replaced by raw disk coordinates
/// in durable manifests or cross-node protocols.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BlobId(pub [u8; 32]);

impl BlobId {
    /// Creates a `BlobId` from a SHA-256 content hash.
    pub fn from_content_hash(hash: [u8; 32]) -> Self {
        Self(hash)
    }

    /// Returns the raw bytes of this blob identifier.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Display for BlobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in &self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// BlobLocation — node-local disk coordinates (NEVER in durable manifests)
// ---------------------------------------------------------------------------

/// Local disk coordinates for a physical blob.
///
/// **Internal to the storage engine on one node.**
/// Must NEVER appear in durable object manifests or cross-node protocols.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobLocation {
    /// Volume file ID (e.g., 42 -> "volume_000042.dat").
    pub volume_id: u32,
    /// Byte offset from the start of the volume file.
    pub offset: u64,
}

// ---------------------------------------------------------------------------
// BlobRefEntry — generalized physical blob reference with ref counting
// ---------------------------------------------------------------------------

/// Tracks a physical blob's location and reference counts.
///
/// Keyed by [`BlobId`] in the blob-refs keyspace.
/// Generalizes the old `DedupEntry` to support both committed objects
/// and staged (in-progress multipart) references.
///
/// - `ref_count_committed`: referenced by visible logical objects.
/// - `ref_count_staged`: referenced only by in-progress multipart uploads.
///
/// This allows:
/// - dedup across multipart parts;
/// - dedup across different uploads using identical parts;
/// - correct cleanup on abort or overwrite;
/// - compactor visibility into both staged and committed live bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobRefEntry {
    /// Local storage coordinates — implementation detail of the storage engine.
    /// NEVER serialized into durable manifests or cross-node messages.
    pub location: BlobLocation,
    /// Number of committed (visible) object references to this blob.
    pub ref_count_committed: u32,
    /// Number of staged (in-progress multipart) references to this blob.
    pub ref_count_staged: u32,
}

impl BlobRefEntry {
    /// Returns true if this blob has any live references (staged or committed).
    pub fn is_live(&self) -> bool {
        self.ref_count_committed > 0 || self.ref_count_staged > 0
    }

    /// Total reference count across both staged and committed.
    pub fn total_ref_count(&self) -> u32 {
        self.ref_count_committed + self.ref_count_staged
    }
}

// ---------------------------------------------------------------------------
// ObjectLayout — discriminator for flat vs composite object storage
// ---------------------------------------------------------------------------

/// Describes how an object's data is physically stored.
///
/// Existing flat objects use `SingleBlob`. Native multipart objects
/// use `CompositeManifest` to reference ordered segments without
/// rewriting all bytes at completion time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ObjectLayout {
    /// Legacy and new single-blob objects.
    /// `file_id`/`offset` from `IndexRecord` are the local disk coordinates.
    SingleBlob {
        /// Volume file ID.
        file_id: u32,
        /// Byte offset in volume file.
        offset: u64,
    },
    /// Composite multipart object — references a manifest blob via `BlobId`.
    /// The manifest blob is resolved to local `(volume_id, offset)` via
    /// `BlobRefEntry` at read time.
    CompositeManifest {
        /// Logical reference to the manifest blob.
        manifest_blob_id: BlobId,
        /// Manifest blob size in bytes (for reading without additional lookup).
        manifest_blob_len: u32,
        /// Number of segments in the manifest.
        part_count: u32,
    },
}

// ---------------------------------------------------------------------------
// CompositeManifest — the manifest blob stored in volume storage
// ---------------------------------------------------------------------------

/// A durable manifest defining a composite object as an ordered list of
/// segment references.
///
/// Persisted as a small blob in volume storage. Referenced by
/// `ObjectLayout::CompositeManifest` in the `IndexRecord`.
///
/// Contains enough information for crash recovery to reconstruct
/// the logical object without depending solely on fjall metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompositeManifest {
    /// Format version for forward compatibility.
    pub format_version: u8,
    /// Total logical size of the composite object in bytes.
    pub total_size: u64,
    /// Number of segments (parts) in this manifest.
    pub part_count: u32,
    /// S3-compatible multipart ETag (e.g., "abc123-7494").
    pub multipart_etag: String,
    /// Whole-object SHA-256 if computed (expensive for large objects).
    pub whole_object_sha256: Option<[u8; 32]>,
    /// Ordered list of segment references.
    pub segments: Vec<ManifestSegmentRef>,
    /// Bucket name (for crash recovery).
    pub bucket: String,
    /// Object key (for crash recovery).
    pub key: String,
}

/// Current manifest format version.
pub const MANIFEST_FORMAT_VERSION: u8 = 1;

/// A reference to one segment (part) within a composite manifest.
///
/// Each segment references a physical blob via [`BlobId`] — a placement-scoped
/// logical identifier. The `BlobId` is resolved to local `(volume_id, offset)`
/// only inside the storage engine at read time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestSegmentRef {
    /// Original part number from the multipart upload.
    pub part_number: u32,
    /// Size of this segment in bytes.
    pub size: u64,
    /// SHA-256 hash of the segment blob content.
    pub physical_hash: [u8; 32],
    /// Logical blob identifier — NEVER raw disk coordinates.
    /// Resolved to `BlobLocation` via `BlobRefEntry` at read time.
    pub blob_id: BlobId,
    /// CRC32 checksum for volume-level integrity.
    pub crc32: u32,
}

// ---------------------------------------------------------------------------
// Multipart upload session and part records (durable metadata)
// ---------------------------------------------------------------------------

/// State of a multipart upload session.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum MultipartUploadState {
    /// Upload is open and accepting parts.
    #[default]
    Open,
    /// Completion is in progress (blocks abort).
    Completing,
}

/// Durable metadata for an in-progress multipart upload.
///
/// Stored in the `MultipartSessions` keyspace, keyed by `upload_id`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultipartUploadSession {
    /// Unique upload identifier.
    pub upload_id: String,
    /// Target bucket.
    pub bucket: String,
    /// Target object key.
    pub key: String,
    /// Content-Type for the final object.
    pub content_type: String,
    /// User-defined metadata (x-amz-meta-* headers).
    pub metadata: HashMap<String, String>,
    /// Current state of the upload.
    pub state: MultipartUploadState,
    /// Creation timestamp (nanoseconds since epoch).
    pub created_at: u64,
    /// Last update timestamp (nanoseconds since epoch).
    pub updated_at: u64,
}

/// Durable metadata for one uploaded part in a multipart upload session.
///
/// Stored in the `MultipartParts` keyspace, keyed by `{upload_id}_{part_number:05}`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultipartPartRecord {
    /// Upload session this part belongs to.
    pub upload_id: String,
    /// Part number (1-10000).
    pub part_number: u32,
    /// Size of the part in bytes.
    pub size: u64,
    /// MD5 hex string for S3 ETag semantics.
    pub etag_md5_hex: String,
    /// Raw MD5 bytes for multipart ETag computation.
    pub etag_md5_bytes: [u8; 16],
    /// SHA-256 hash of the part content (used as `BlobId` in single-node MVP).
    pub physical_hash: [u8; 32],
    /// Logical blob reference — resolved to local disk coordinates only
    /// inside the storage engine. Never leaked into durable manifests
    /// as raw `(volume_id, offset)`.
    pub blob_id: BlobId,
    /// CRC32 checksum for volume-level integrity.
    pub crc32: u32,
    /// When this part was uploaded (nanoseconds since epoch).
    pub created_at: u64,
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_blob_id() -> BlobId {
        BlobId([0xAB; 32])
    }

    fn sample_manifest() -> CompositeManifest {
        CompositeManifest {
            format_version: MANIFEST_FORMAT_VERSION,
            total_size: 100_000_000,
            part_count: 2,
            multipart_etag: "abc123-2".to_string(),
            whole_object_sha256: None,
            segments: vec![
                ManifestSegmentRef {
                    part_number: 1,
                    size: 50_000_000,
                    physical_hash: [1u8; 32],
                    blob_id: BlobId([1u8; 32]),
                    crc32: 0x1234,
                },
                ManifestSegmentRef {
                    part_number: 2,
                    size: 50_000_000,
                    physical_hash: [2u8; 32],
                    blob_id: BlobId([2u8; 32]),
                    crc32: 0x5678,
                },
            ],
            bucket: "test-bucket".to_string(),
            key: "large-file.bin".to_string(),
        }
    }

    #[test]
    fn blob_id_display() {
        let id = BlobId([0xAB; 32]);
        let s = format!("{id}");
        assert_eq!(s.len(), 64);
        assert!(s.chars().all(|c| c == 'a' || c == 'b'));
    }

    #[test]
    fn blob_id_bincode_roundtrip() {
        let id = sample_blob_id();
        let bytes = bincode::serialize(&id).unwrap();
        let decoded: BlobId = bincode::deserialize(&bytes).unwrap();
        assert_eq!(id, decoded);
    }

    #[test]
    fn blob_location_roundtrip() {
        let loc = BlobLocation {
            volume_id: 42,
            offset: 1_000_000,
        };
        let bytes = bincode::serialize(&loc).unwrap();
        let decoded: BlobLocation = bincode::deserialize(&bytes).unwrap();
        assert_eq!(loc, decoded);
    }

    #[test]
    fn blob_ref_entry_roundtrip() {
        let entry = BlobRefEntry {
            location: BlobLocation {
                volume_id: 7,
                offset: 8192,
            },
            ref_count_committed: 3,
            ref_count_staged: 1,
        };
        let bytes = bincode::serialize(&entry).unwrap();
        let decoded: BlobRefEntry = bincode::deserialize(&bytes).unwrap();
        assert_eq!(entry, decoded);
        assert!(entry.is_live());
        assert_eq!(entry.total_ref_count(), 4);
    }

    #[test]
    fn blob_ref_entry_dead_when_zero() {
        let entry = BlobRefEntry {
            location: BlobLocation {
                volume_id: 0,
                offset: 0,
            },
            ref_count_committed: 0,
            ref_count_staged: 0,
        };
        assert!(!entry.is_live());
    }

    #[test]
    fn object_layout_single_blob_roundtrip() {
        let layout = ObjectLayout::SingleBlob {
            file_id: 42,
            offset: 1024,
        };
        let bytes = bincode::serialize(&layout).unwrap();
        let decoded: ObjectLayout = bincode::deserialize(&bytes).unwrap();
        assert_eq!(layout, decoded);
    }

    #[test]
    fn object_layout_composite_roundtrip() {
        let layout = ObjectLayout::CompositeManifest {
            manifest_blob_id: sample_blob_id(),
            manifest_blob_len: 256,
            part_count: 7494,
        };
        let bytes = bincode::serialize(&layout).unwrap();
        let decoded: ObjectLayout = bincode::deserialize(&bytes).unwrap();
        assert_eq!(layout, decoded);
    }

    #[test]
    fn composite_manifest_roundtrip() {
        let manifest = sample_manifest();
        let bytes = bincode::serialize(&manifest).unwrap();
        let decoded: CompositeManifest = bincode::deserialize(&bytes).unwrap();
        assert_eq!(manifest, decoded);
        assert_eq!(decoded.segments.len(), 2);
        assert_eq!(decoded.total_size, 100_000_000);
    }

    #[test]
    fn composite_manifest_json_roundtrip() {
        let manifest = sample_manifest();
        let json = serde_json::to_string(&manifest).unwrap();
        let decoded: CompositeManifest = serde_json::from_str(&json).unwrap();
        assert_eq!(manifest, decoded);
    }

    #[test]
    fn multipart_upload_state_default() {
        assert_eq!(MultipartUploadState::default(), MultipartUploadState::Open);
    }

    #[test]
    fn multipart_session_roundtrip() {
        let session = MultipartUploadSession {
            upload_id: "abc123".to_string(),
            bucket: "test".to_string(),
            key: "file.bin".to_string(),
            content_type: "application/octet-stream".to_string(),
            metadata: HashMap::new(),
            state: MultipartUploadState::Open,
            created_at: 1000,
            updated_at: 2000,
        };
        let bytes = bincode::serialize(&session).unwrap();
        let decoded: MultipartUploadSession = bincode::deserialize(&bytes).unwrap();
        assert_eq!(session.upload_id, decoded.upload_id);
        assert_eq!(session.state, decoded.state);
    }

    #[test]
    fn multipart_part_record_roundtrip() {
        let record = MultipartPartRecord {
            upload_id: "abc123".to_string(),
            part_number: 42,
            size: 5_242_880,
            etag_md5_hex: "d41d8cd98f00b204e9800998ecf8427e".to_string(),
            etag_md5_bytes: [0xD4; 16],
            physical_hash: [0xFF; 32],
            blob_id: BlobId([0xFF; 32]),
            crc32: 0xDEADBEEF,
            created_at: 1234567890,
        };
        let bytes = bincode::serialize(&record).unwrap();
        let decoded: MultipartPartRecord = bincode::deserialize(&bytes).unwrap();
        assert_eq!(record.part_number, decoded.part_number);
        assert_eq!(record.blob_id, decoded.blob_id);
        assert_eq!(record.size, decoded.size);
    }

    #[test]
    fn manifest_with_many_segments() {
        let segments: Vec<ManifestSegmentRef> = (1..=10000)
            .map(|i| ManifestSegmentRef {
                part_number: i,
                size: 8_388_608,
                physical_hash: [i as u8; 32],
                blob_id: BlobId([i as u8; 32]),
                crc32: i,
            })
            .collect();

        let manifest = CompositeManifest {
            format_version: MANIFEST_FORMAT_VERSION,
            total_size: 10000 * 8_388_608,
            part_count: 10000,
            multipart_etag: "hash-10000".to_string(),
            whole_object_sha256: Some([0xAA; 32]),
            segments,
            bucket: "big".to_string(),
            key: "huge.bin".to_string(),
        };

        let bytes = bincode::serialize(&manifest).unwrap();
        let decoded: CompositeManifest = bincode::deserialize(&bytes).unwrap();
        assert_eq!(decoded.part_count, 10000);
        assert_eq!(decoded.segments.len(), 10000);
    }
}
