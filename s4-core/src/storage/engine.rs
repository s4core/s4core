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

//! Storage engine trait — the public API contract for S4 storage operations.
//!
//! # Stability
//!
//! The [`StorageEngine`] trait is the **public boundary** of the storage layer.
//! All consumer crates (`s4-api`, `s4-features`, `s4-server`) depend on this
//! trait, not on any specific implementation. Changes to this trait are breaking
//! changes and must be coordinated across the entire workspace.
//!
//! # Implementations
//!
//! Currently there is one implementation:
//! - [`super::BitcaskStorageEngine`] — Bitcask-style append-only log storage
//!   with metadata indexing (fjall LSM-tree, MVCC, LZ4 compression).
//!
//! # Internal vs External
//!
//! - **External (stable):** This trait and all types defined in this module.
//! - **Internal (may change):** [`super::IndexDb`], [`super::Deduplicator`],
//!   [`super::VolumeWriter`], [`super::VolumeReader`] — these are implementation
//!   details of [`super::BitcaskStorageEngine`] and may change across versions.
//!
//! # Concurrency
//!
//! All methods on `StorageEngine` are `&self` (shared reference) and the trait
//! requires `Send + Sync`. Implementations must handle concurrent access
//! internally.
//!
//! # Durability
//!
//! When `strict_sync` is enabled, implementations guarantee that data is
//! fsync'd to disk before returning success. A successful return from
//! `put_object` or `put_object_versioned` means the data survives a process
//! crash or power failure.

use std::collections::HashMap;

use crate::error::StorageError;
use crate::storage::journal::JournalEntry;
use crate::storage::placement::PlacementGroupId;
use crate::types::{DefaultRetention, IndexRecord, VersioningStatus};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncRead;

// ============================================================================
// Healing writes
// ============================================================================

/// Reserved metadata key that turns an ordinary write into a **healing write**.
///
/// An ordinary write of content the engine has seen before costs nothing: the
/// deduplicator points the new record at the blob that already holds those
/// bytes. That is the right answer while the blob is intact, and the wrong one
/// for the caller that is writing precisely because it is not — a repair
/// rebuilding a shard whose bytes rotted on disk, say. Answering such a write
/// from the dedup index would report success and leave the damage where it was.
///
/// A write carrying this key with a truthy value therefore always puts its
/// bytes into a volume of its own, and the dedup entry for that content is
/// moved to the copy it just wrote, so the next writer of the same content
/// lands on the healthy one. The key is stripped before the record is
/// persisted: it describes the write, not the object.
///
/// Callers should use [`is_force_physical_write`] rather than reading the key
/// by hand, so that every layer agrees on what a healing write looks like.
pub const FORCE_PHYSICAL_WRITE_METADATA_KEY: &str = "_s4_force_physical_write";

/// Metadata key that carries the ETag a replica keeps for a copy repaired
/// onto it.
///
/// A replica computes the ETag of a write from its bytes. A copy of an object
/// completed by a multipart upload, whose ETag ends in `-N`, would get another
/// one, and the replicas of one version would disagree about it. So whoever
/// brings a replica a copy — anti-entropy, hint delivery, read repair — sends
/// the source's ETag under this key. The storage keeps it as the record's ETag
/// only for a write that also carries an HLC, the way only cluster nodes
/// write, and strips the key either way. Clients cannot set it: the HTTP layer
/// drops user metadata keys starting with `_s4_`.
pub const REPAIRED_ETAG_METADATA_KEY: &str = "_s4_etag";

/// Whether `metadata` asks for a healing write.
///
/// See [`FORCE_PHYSICAL_WRITE_METADATA_KEY`]. Accepts the spellings an
/// operator or an RPC hop is likely to produce for "yes", and treats anything
/// else — including a missing key — as an ordinary write.
pub fn is_force_physical_write(metadata: &HashMap<String, String>) -> bool {
    metadata
        .get(FORCE_PHYSICAL_WRITE_METADATA_KEY)
        .is_some_and(|value| matches!(value.as_str(), "1" | "true" | "yes" | "on"))
}

// ============================================================================
// Streaming Write Types
// ============================================================================

/// Options for streaming object reads.
#[derive(Debug, Clone, Default)]
pub struct ReadOptions {
    /// Byte range to read (inclusive start, inclusive end).
    /// When `None`, reads the entire object.
    pub range: Option<(u64, u64)>,
}

/// Metadata and streaming body for an object read.
pub struct ObjectStream {
    /// Object metadata from the index.
    pub record: IndexRecord,
    /// Total size of the object (full, not range).
    pub total_size: u64,
    /// Size of data in this response (may be less than total for range reads).
    pub content_length: u64,
    /// Content range if this is a partial response: (start, end, total).
    pub content_range: Option<(u64, u64, u64)>,
    /// Streaming body. Memory usage is O(buffer_size), not O(object_size).
    pub body: Box<dyn AsyncRead + Unpin + Send>,
}

impl std::fmt::Debug for ObjectStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectStream")
            .field("total_size", &self.total_size)
            .field("content_length", &self.content_length)
            .field("content_range", &self.content_range)
            .finish()
    }
}

/// Result of a streaming put operation (hashes computed during write).
#[derive(Debug, Clone)]
pub struct StreamingPutResult {
    /// SHA-256 content hash (used for deduplication).
    pub content_hash: [u8; 32],
    /// CRC32 checksum (for volume integrity verification).
    pub crc32: u32,
    /// Total bytes written to the volume.
    pub bytes_written: u64,
    /// ETag of the stored object (MD5 hex string or multipart format).
    pub etag: String,
}

// ============================================================================
// Tombstones
// ============================================================================

/// What became of a write that last-writer-wins weighs
/// ([`StorageEngine::put_object_outcome`]).
#[derive(Debug, Clone)]
pub enum PutOutcome {
    /// The write is what the key holds now.
    Stored {
        /// ETag of the stored object.
        etag: String,
    },
    /// The key holds a newer version — an object or a tombstone — and the
    /// write was not applied.
    Superseded {
        /// The record that outranks the write.
        by: Box<IndexRecord>,
    },
}

/// What the index holds for one key, a tombstone included.
///
/// Every read path of [`StorageEngine`] answers a tombstone as an absent
/// object. A replicated pool needs to tell the two apart — a tombstone is a
/// version of the key, ordered against its writes by HLC, and absence is
/// not — and asks with [`StorageEngine::key_version`].
#[derive(Debug, Clone)]
pub enum KeyVersion {
    /// The key holds an object.
    Live(IndexRecord),
    /// The key was deleted; the record is its tombstone, with the delete's HLC
    /// (see [`IndexRecord::new_tombstone`]).
    Tombstone(IndexRecord),
    /// The index holds nothing for the key.
    Absent,
}

impl KeyVersion {
    /// The record behind this version, if there is one.
    pub fn record(&self) -> Option<&IndexRecord> {
        match self {
            Self::Live(record) | Self::Tombstone(record) => Some(record),
            Self::Absent => None,
        }
    }
}

// ============================================================================
// Versioning Types
// ============================================================================

/// Result of a versioned delete operation.
#[derive(Debug, Clone)]
pub struct DeleteResult {
    /// True if a delete marker was created.
    pub delete_marker: bool,
    /// Version ID of the delete marker or permanently deleted version.
    pub version_id: Option<String>,
}

/// A single object version entry.
#[derive(Debug, Clone)]
pub struct ObjectVersion {
    /// Object key.
    pub key: String,
    /// Version ID.
    pub version_id: String,
    /// Whether this is the latest version.
    pub is_latest: bool,
    /// Last modified timestamp (nanoseconds since Unix epoch).
    pub last_modified: u64,
    /// ETag of the object.
    pub etag: String,
    /// Size in bytes.
    pub size: u64,
}

/// A delete marker entry.
#[derive(Debug, Clone)]
pub struct DeleteMarkerEntry {
    /// Object key.
    pub key: String,
    /// Version ID of the delete marker.
    pub version_id: String,
    /// Whether this is the latest version (delete marker is current).
    pub is_latest: bool,
    /// Timestamp when the delete marker was created.
    pub last_modified: u64,
}

/// Result of ListObjectVersions API.
#[derive(Debug, Clone, Default)]
pub struct ListVersionsResult {
    /// Object versions (actual data entries).
    pub versions: Vec<ObjectVersion>,
    /// Delete markers.
    pub delete_markers: Vec<DeleteMarkerEntry>,
    /// Common prefixes (when delimiter is used).
    pub common_prefixes: Vec<String>,
    /// True if there are more results.
    pub is_truncated: bool,
    /// Next key marker for pagination.
    pub next_key_marker: Option<String>,
    /// Next version ID marker for pagination.
    pub next_version_id_marker: Option<String>,
}

/// A tombstone tombstone GC found due
/// ([`StorageEngine::gc_tombstones`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TombstoneRef {
    /// The key as the index holds it: `{bucket}/{key}`, a bucket marker as
    /// `__system__/__s4_bucket_marker_{bucket}`.
    pub full_key: String,
    /// The HLC of its delete: wall time, logical counter, origin node.
    pub hlc: (u64, u32, u128),
}

/// Main storage engine interface — the public API contract for all S4 storage operations.
///
/// This trait defines the core operations for storing and retrieving objects,
/// including versioning and Object Lock support. It is the **stable public boundary**
/// between the storage layer and consumer crates (`s4-api`, `s4-features`, `s4-server`).
///
/// # Contract Guarantees
///
/// 1. **Durability:** When `strict_sync` is enabled, a successful write is persisted
///    to disk before the method returns. Process crashes after a successful return
///    will NOT lose the written data.
///
/// 2. **Consistency:** Each individual operation (PUT, GET, DELETE) is consistent
///    in isolation. However, multi-step versioned operations (e.g., PUT versioned
///    which writes version record + version list + current pointer) currently have
///    crash windows between steps. See Phase 3 migration plan for atomic batch fix.
///
/// 3. **Isolation:** Concurrent reads do not block writes and vice versa
///    (fjall provides MVCC with lock-free reads).
///
/// # Versioning Semantics
///
/// - `Unversioned`: Standard PUT/GET/DELETE, no version history.
/// - `Enabled`: Each PUT creates a new version with a unique UUID. DELETE creates
///   a delete marker. Previous versions remain accessible by version ID.
/// - `Suspended`: New writes use the "null" version ID, replacing any existing
///   null version. Existing non-null versions are preserved.
///
/// # Known Limitations (pre-Phase 3)
///
/// - Versioned PUT/DELETE perform multiple sequential database writes that are
///   NOT atomic. A crash between writes can leave orphaned version records or
///   stale current pointers. The `audit_version_consistency` method on
///   `BitcaskStorageEngine` can detect these inconsistencies.
#[async_trait]
pub trait StorageEngine: Send + Sync {
    /// Writes an object to storage.
    ///
    /// # Arguments
    ///
    /// * `bucket` - Bucket name
    /// * `key` - Object key (path)
    /// * `data` - Object data
    /// * `content_type` - MIME type
    /// * `metadata` - User-defined metadata
    ///
    /// # Returns
    ///
    /// Returns the ETag of the stored object.
    async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        data: &[u8],
        content_type: &str,
        metadata: &std::collections::HashMap<String, String>,
    ) -> Result<String, StorageError>;

    /// Writes an object like [`put_object`](Self::put_object), and says
    /// whether the write is what the key holds now or a newer version
    /// outranked it.
    ///
    /// A write that carries an HLC is weighed against the key's version by
    /// last-writer-wins, and an older one is not applied — which
    /// `put_object` answers with the ETag the key keeps. A coordinator needs
    /// to know, to raise its write above that version. The default
    /// implementation serves an engine without last-writer-wins, where every
    /// write is stored.
    async fn put_object_outcome(
        &self,
        bucket: &str,
        key: &str,
        data: &[u8],
        content_type: &str,
        metadata: &std::collections::HashMap<String, String>,
    ) -> Result<PutOutcome, StorageError> {
        Ok(PutOutcome::Stored {
            etag: self.put_object(bucket, key, data, content_type, metadata).await?,
        })
    }

    /// Reads an object from storage.
    ///
    /// # Arguments
    ///
    /// * `bucket` - Bucket name
    /// * `key` - Object key
    ///
    /// # Returns
    ///
    /// Returns the object data and metadata. A tombstone is no object: it
    /// answers [`StorageError::ObjectNotFound`], as it does on every read path
    /// below; [`key_version`](Self::key_version) tells the two apart.
    async fn get_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<(Vec<u8>, IndexRecord), StorageError>;

    /// Deletes an object from storage.
    ///
    /// # Arguments
    ///
    /// * `bucket` - Bucket name
    /// * `key` - Object key
    async fn delete_object(&self, bucket: &str, key: &str) -> Result<(), StorageError>;

    /// Deletes an object only when the current record matches the expected HLC generation.
    ///
    /// Returns `Ok(true)` when the matching record was removed and `Ok(false)` when the
    /// object is absent or points at a different generation.
    async fn delete_object_if_hlc(
        &self,
        bucket: &str,
        key: &str,
        expected_hlc_timestamp: u64,
        expected_hlc_logical: u32,
        expected_origin_node_id: u128,
    ) -> Result<bool, StorageError> {
        let record = match self.head_object(bucket, key).await {
            Ok(record) => record,
            Err(StorageError::ObjectNotFound { .. }) => return Ok(false),
            Err(error) => return Err(error),
        };
        if record.hlc_timestamp != expected_hlc_timestamp
            || record.hlc_logical != expected_hlc_logical
            || record.origin_node_id != expected_origin_node_id
        {
            return Ok(false);
        }

        self.delete_object(bucket, key).await?;
        Ok(true)
    }

    /// Deletes a key of a replicated pool by leaving its tombstone, issued at
    /// the given HLC.
    ///
    /// The tombstone takes the key's place under the same last-writer-wins rule
    /// as a write: it replaces a record with an older HLC, or one stored
    /// without an HLC, and leaves a record with the same or a newer HLC alone.
    /// A key the index does not hold gets a tombstone too, so that a copy that
    /// missed the delete cannot come back as the newest version. The replaced
    /// record's data is released in the same batch, as
    /// [`delete_object`](Self::delete_object) releases it.
    ///
    /// Returns `Ok(true)` when the tombstone was written, `Ok(false)` when the
    /// key holds a version that is not older.
    ///
    /// # Errors
    ///
    /// [`StorageError::InvalidOperation`] for a zero HLC: a tombstone without
    /// one could not be ordered against anything. The default implementation
    /// refuses as well: an engine has to keep the tombstone itself, and a
    /// delete in its place would lose exactly what the tombstone is for.
    async fn tombstone_object(
        &self,
        _bucket: &str,
        _key: &str,
        _hlc_timestamp: u64,
        _hlc_logical: u32,
        _origin_node_id: u128,
    ) -> Result<bool, StorageError> {
        Err(StorageError::InvalidOperation {
            operation: "tombstone_object".to_string(),
            reason: "this storage engine does not keep tombstones".to_string(),
        })
    }

    /// Removes the tombstones in `tombstones` that the index still holds as
    /// found, leaving their keys absent: tombstone GC, once every replica of
    /// each key has seen its delete. Returns how many it removed.
    ///
    /// A key that holds anything else by now — an object, or a tombstone of
    /// another HLC that replaced the one found — or nothing, is left as it is.
    /// The removals, their keys' version digests and the raised tombstone
    /// purge horizon go in one atomic batch, with one sync to disk.
    ///
    /// The default implementation serves an engine without tombstones, which
    /// has none to remove.
    async fn gc_tombstones(&self, _tombstones: &[TombstoneRef]) -> Result<u64, StorageError> {
        Ok(0)
    }

    /// What the index holds for `bucket/key`: an object, a tombstone or
    /// nothing.
    ///
    /// The default implementation serves an engine without tombstones, whose
    /// [`head_object`](Self::head_object) answers for every version it has.
    async fn key_version(&self, bucket: &str, key: &str) -> Result<KeyVersion, StorageError> {
        match self.head_object(bucket, key).await {
            Ok(record) => Ok(KeyVersion::Live(record)),
            Err(StorageError::ObjectNotFound { .. }) => Ok(KeyVersion::Absent),
            Err(error) => Err(error),
        }
    }

    /// The tombstone horizon of this node's storage: the HLC from which every
    /// delete on it left a tombstone, once settled
    /// ([`IndexDb::settle_tombstone_horizon`](crate::storage::IndexDb::settle_tombstone_horizon)).
    /// An absent key says nothing about a delete older than it.
    ///
    /// The default implementation serves an engine without tombstones, which
    /// has none.
    async fn tombstone_horizon(
        &self,
    ) -> Result<Option<crate::storage::TombstoneHorizon>, StorageError> {
        Ok(None)
    }

    /// A page of this node's version digests in ring order
    /// ([`IndexDb::version_digests_in`](crate::storage::IndexDb::version_digests_in)):
    /// ring positions from `from` up to `to`, right after the key `after`,
    /// at most `limit`. Anti-entropy compares replicas by them.
    ///
    /// The default implementation serves an engine that keeps no digests.
    async fn version_digests_in(
        &self,
        _from: u64,
        _to: Option<u64>,
        _after: Option<&str>,
        _limit: usize,
    ) -> Result<Vec<crate::storage::VersionDigestEntry>, StorageError> {
        Err(StorageError::InvalidOperation {
            operation: "version digests".to_string(),
            reason: "this storage keeps none".to_string(),
        })
    }

    /// Lists a bucket like [`list_objects_after`](Self::list_objects_after),
    /// or like [`list_objects`](Self::list_objects) without `start_after`,
    /// but with tombstones among the entries: a replicated pool merges the
    /// listings of its nodes by HLC, and a tombstone has to outrank the older
    /// copy another node still lists.
    ///
    /// The default implementation serves an engine without tombstones.
    async fn list_objects_with_tombstones(
        &self,
        bucket: &str,
        prefix: &str,
        start_after: Option<&str>,
        max_keys: usize,
    ) -> Result<Vec<(String, IndexRecord)>, StorageError> {
        match start_after {
            Some(start_after) => {
                self.list_objects_after(bucket, prefix, start_after, max_keys).await
            }
            None => self.list_objects(bucket, prefix, max_keys).await,
        }
    }

    /// Releases an object's local data while keeping its namespace entry.
    ///
    /// This is what a storage-layout migration needs, and it is deliberately not
    /// a delete. The bytes move elsewhere — today, to erasure-coded shards — but
    /// the object itself does not go anywhere: it must keep appearing in
    /// listings, lifecycle scans and bucket statistics, all of which read the
    /// object index. Deleting the index entry is how an object becomes readable
    /// by key yet invisible to every client that enumerates a bucket.
    ///
    /// The record is rewritten with [`crate::types::ObjectLayout::ErasureCoded`], keeping
    /// size, ETag, content type, metadata, timestamps and the HLC generation,
    /// and the local blob reference is released so compaction can reclaim the
    /// space.
    ///
    /// Guarded by the same generation check as
    /// [`delete_object_if_hlc`](Self::delete_object_if_hlc): a record that has
    /// been overwritten since the migration started must not be touched.
    ///
    /// Returns `Ok(true)` when the matching record was converted, `Ok(false)`
    /// when the object is absent or points at a different generation.
    ///
    /// # Errors
    ///
    /// The default implementation returns [`StorageError::InvalidOperation`]:
    /// releasing a blob reference cannot be expressed through the other trait
    /// methods, so a backend that supports layout migration has to implement
    /// this itself.
    async fn detach_object_data_if_hlc(
        &self,
        _bucket: &str,
        _key: &str,
        _expected_hlc_timestamp: u64,
        _expected_hlc_logical: u32,
        _expected_origin_node_id: u128,
        _layout: crate::types::ObjectLayout,
    ) -> Result<bool, StorageError> {
        Err(StorageError::InvalidOperation {
            operation: "detach_object_data_if_hlc".to_string(),
            reason: "this storage engine does not support erasure-coded layout migration"
                .to_string(),
        })
    }

    /// Brings the bytes of an erasure-coded object back into local storage.
    ///
    /// The mirror of [`detach_object_data_if_hlc`](Self::detach_object_data_if_hlc),
    /// used by auto-tiering to promote an object that became hot again. `data`
    /// is written to a volume and the namespace entry is rewritten with a local
    /// layout, keeping size, ETag, content type, metadata, timestamps and the
    /// HLC generation — a promotion moves bytes, it does not write a new object.
    ///
    /// Guarded by the same generation check as its mirror, and for a stronger
    /// reason: a client PUT that landed while the promotion was decoding must
    /// win, and overwriting it with the decoded bytes would silently roll the
    /// object back.
    ///
    /// `data` must be the object's real content, already verified against the
    /// EC manifest by the caller. This method checks the one fact it can check
    /// on its own — that the byte count matches the record — and refuses
    /// anything else, so a caller that mixed up two objects cannot quietly
    /// corrupt one of them. The content-addressing hash is recomputed from the
    /// bytes written, because the dedup index and the read-path checksum both
    /// read it and both have to agree with what is on disk.
    ///
    /// Returns `Ok(true)` when the matching record was converted or already
    /// carried local data, `Ok(false)` when the object is absent or points at a
    /// different generation.
    ///
    /// # Errors
    ///
    /// The default implementation returns [`StorageError::InvalidOperation`]:
    /// like its mirror, writing a blob and rewriting the index entry in one
    /// batch cannot be expressed through the other trait methods.
    async fn attach_object_data_if_hlc(
        &self,
        _bucket: &str,
        _key: &str,
        _expected_hlc_timestamp: u64,
        _expected_hlc_logical: u32,
        _expected_origin_node_id: u128,
        _data: &[u8],
    ) -> Result<bool, StorageError> {
        Err(StorageError::InvalidOperation {
            operation: "attach_object_data_if_hlc".to_string(),
            reason: "this storage engine does not support erasure-coded layout migration"
                .to_string(),
        })
    }

    /// Writes a namespace entry for an object whose data lives elsewhere.
    ///
    /// Reconciliation uses this to put back an entry that was lost — by a
    /// cutover from before layout migration existed, or by a node that was
    /// down when its peers converted theirs. `record` is rebuilt from the EC
    /// manifest, which carries everything a client can see.
    ///
    /// Does nothing when a record already exists: the live index always wins
    /// over a manifest, because an object may have been overwritten since.
    ///
    /// Returns `Ok(true)` when an entry was written.
    ///
    /// # Errors
    ///
    /// The default implementation returns [`StorageError::InvalidOperation`],
    /// like [`detach_object_data_if_hlc`](Self::detach_object_data_if_hlc), and
    /// for the same reason.
    async fn restore_erasure_coded_entry(
        &self,
        _bucket: &str,
        _key: &str,
        _record: IndexRecord,
    ) -> Result<bool, StorageError> {
        Err(StorageError::InvalidOperation {
            operation: "restore_erasure_coded_entry".to_string(),
            reason: "this storage engine does not support erasure-coded layout migration"
                .to_string(),
        })
    }

    /// Gets object metadata without reading the data.
    ///
    /// # Arguments
    ///
    /// * `bucket` - Bucket name
    /// * `key` - Object key
    ///
    /// # Returns
    ///
    /// Returns the object metadata, or [`StorageError::ObjectNotFound`] for a
    /// tombstone.
    async fn head_object(&self, bucket: &str, key: &str) -> Result<IndexRecord, StorageError>;

    /// Lists objects in a bucket with the given prefix.
    ///
    /// # Arguments
    ///
    /// * `bucket` - Bucket name
    /// * `prefix` - Key prefix to filter by
    /// * `max_keys` - Maximum number of keys to return
    ///
    /// # Returns
    ///
    /// Returns a vector of (key, metadata) pairs. Tombstones are not listed,
    /// and do not count toward `max_keys`.
    async fn list_objects(
        &self,
        bucket: &str,
        prefix: &str,
        max_keys: usize,
    ) -> Result<Vec<(String, IndexRecord)>, StorageError>;

    /// Lists objects in a bucket starting after a given key (for pagination).
    ///
    /// # Arguments
    ///
    /// * `bucket` - Bucket name
    /// * `prefix` - Key prefix to filter by
    /// * `start_after` - Only return keys lexicographically after this key
    /// * `max_keys` - Maximum number of keys to return
    ///
    /// # Returns
    ///
    /// Returns a vector of (key, metadata) pairs, tombstones left out as in
    /// [`list_objects`](Self::list_objects).
    async fn list_objects_after(
        &self,
        bucket: &str,
        prefix: &str,
        start_after: &str,
        max_keys: usize,
    ) -> Result<Vec<(String, IndexRecord)>, StorageError>;

    // ========================================================================
    // Versioning Methods
    // ========================================================================

    /// Writes an object with versioning support.
    ///
    /// # Arguments
    ///
    /// * `bucket` - Bucket name
    /// * `key` - Object key
    /// * `data` - Object data
    /// * `content_type` - MIME type
    /// * `metadata` - User-defined metadata
    /// * `versioning_status` - Current versioning status of the bucket
    ///
    /// # Returns
    ///
    /// Returns (ETag, `Option<VersionId>`). Version ID is:
    /// - `None` if versioning is `Unversioned`
    /// - A UUID if versioning is `Enabled`
    /// - `"null"` if versioning is `Suspended`
    async fn put_object_versioned(
        &self,
        bucket: &str,
        key: &str,
        data: &[u8],
        content_type: &str,
        metadata: &std::collections::HashMap<String, String>,
        versioning_status: VersioningStatus,
    ) -> Result<(String, Option<String>), StorageError>;

    /// Gets a specific version of an object.
    ///
    /// # Arguments
    ///
    /// * `bucket` - Bucket name
    /// * `key` - Object key
    /// * `version_id` - Version ID to retrieve
    ///
    /// # Returns
    ///
    /// Returns the object data and metadata.
    ///
    /// # Errors
    ///
    /// - `VersionNotFound` if the version doesn't exist
    /// - `DeleteMarker` if the version is a delete marker
    async fn get_object_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<(Vec<u8>, IndexRecord), StorageError>;

    /// Deletes an object with versioning support.
    ///
    /// # Arguments
    ///
    /// * `bucket` - Bucket name
    /// * `key` - Object key
    /// * `version_id` - Optional version ID to delete permanently
    /// * `versioning_status` - Current versioning status of the bucket
    ///
    /// # Returns
    ///
    /// Returns `DeleteResult` indicating:
    /// - If `delete_marker` is true, a delete marker was created
    /// - `version_id` contains the ID of the delete marker or deleted version
    async fn delete_object_versioned(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        versioning_status: VersioningStatus,
    ) -> Result<DeleteResult, StorageError>;

    /// Gets metadata for a specific version.
    ///
    /// # Arguments
    ///
    /// * `bucket` - Bucket name
    /// * `key` - Object key
    /// * `version_id` - Version ID to retrieve metadata for
    ///
    /// # Returns
    ///
    /// Returns the object metadata (IndexRecord).
    async fn head_object_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<IndexRecord, StorageError>;

    /// Lists all versions of objects in a bucket.
    ///
    /// # Arguments
    ///
    /// * `bucket` - Bucket name
    /// * `prefix` - Key prefix to filter by
    /// * `key_marker` - Start listing after this key
    /// * `version_id_marker` - Start listing after this version (requires key_marker)
    /// * `max_keys` - Maximum number of versions to return
    ///
    /// # Returns
    ///
    /// Returns `ListVersionsResult` containing versions and delete markers.
    async fn list_object_versions(
        &self,
        bucket: &str,
        prefix: &str,
        key_marker: Option<&str>,
        version_id_marker: Option<&str>,
        max_keys: usize,
    ) -> Result<ListVersionsResult, StorageError>;

    // ========================================================================
    // Object Lock Methods (Phase 3)
    // ========================================================================

    /// Writes an object with versioning and optional default retention.
    ///
    /// This method combines versioning with Object Lock support.
    ///
    /// # Arguments
    ///
    /// * `bucket` - Bucket name
    /// * `key` - Object key
    /// * `data` - Object data
    /// * `content_type` - MIME type
    /// * `metadata` - User-defined metadata
    /// * `versioning_status` - Current versioning status of the bucket
    /// * `default_retention` - Optional default retention to apply
    ///
    /// # Returns
    ///
    /// Returns (ETag, `Option<VersionId>`). Version ID is:
    /// - `None` if versioning is `Unversioned`
    /// - A UUID if versioning is `Enabled`
    /// - `"null"` if versioning is `Suspended`
    ///
    /// # Behavior
    ///
    /// If `default_retention` is provided:
    /// - The created IndexRecord will have `retention_mode` and `retain_until_timestamp` set
    /// - Retention is calculated from creation time + days
    /// - Only applies to new objects (not updates)
    #[allow(clippy::too_many_arguments)]
    async fn put_object_with_retention(
        &self,
        bucket: &str,
        key: &str,
        data: &[u8],
        content_type: &str,
        metadata: &std::collections::HashMap<String, String>,
        versioning_status: VersioningStatus,
        default_retention: Option<DefaultRetention>,
    ) -> Result<(String, Option<String>), StorageError>;

    /// Updates object metadata for a specific version (for retention/legal hold changes).
    ///
    /// This method updates ONLY the IndexRecord in the metadata database.
    /// The actual object data in volumes remains unchanged.
    ///
    /// # Arguments
    ///
    /// * `bucket` - Bucket name
    /// * `key` - Object key
    /// * `version_id` - Version ID to update
    /// * `updated_record` - New IndexRecord with updated metadata
    ///
    /// # Errors
    ///
    /// Returns `VersionNotFound` if the version doesn't exist.
    ///
    /// # Safety
    ///
    /// This method should only be used for updating Object Lock fields:
    /// - `retention_mode`
    /// - `retain_until_timestamp`
    /// - `legal_hold`
    ///
    /// Updating other fields may cause inconsistencies with volume data.
    async fn update_object_metadata(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
        updated_record: IndexRecord,
    ) -> Result<(), StorageError>;

    // ========================================================================
    // Streaming Write Methods
    // ========================================================================

    /// Opens a streaming reader for an object (current version).
    ///
    /// Returns an `ObjectStream` with a bounded `AsyncRead` body.
    /// For range requests, only the requested byte range is read from disk.
    /// Memory usage is O(buffer_size), not O(object_size).
    ///
    /// Implementations should validate local storage integrity before exposing
    /// bytes to the caller when content hashes or per-blob checksums are
    /// available. This includes ranged reads: implementations may need to
    /// validate the backing blob/object before returning a range stream.
    async fn open_object_stream(
        &self,
        bucket: &str,
        key: &str,
        options: ReadOptions,
    ) -> Result<ObjectStream, StorageError>;

    /// Opens a streaming reader for a specific object version.
    ///
    /// Same semantics as `open_object_stream` but for a specific version.
    async fn open_object_version_stream(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
        options: ReadOptions,
    ) -> Result<ObjectStream, StorageError>;

    /// Writes an object from a streaming reader without buffering the entire body.
    ///
    /// Computes SHA-256 and CRC32 incrementally during the write. After writing,
    /// performs a post-write dedup check — if the content already exists, the
    /// written blob is marked as dead space (reclaimed by compactor).
    ///
    /// # Arguments
    ///
    /// * `bucket` - Bucket name
    /// * `key` - Object key
    /// * `reader` - Async reader providing object data
    /// * `content_length` - Expected total size in bytes
    /// * `content_type` - MIME type
    /// * `metadata` - User-defined metadata
    /// * `etag` - Pre-computed ETag (e.g., S3 multipart format `hash-N`)
    ///
    /// # Memory
    ///
    /// Uses a fixed I/O buffer — memory usage is `O(buffer_size)`, not `O(data_size)`.
    #[allow(clippy::too_many_arguments)]
    async fn put_object_streaming(
        &self,
        bucket: &str,
        key: &str,
        reader: Box<dyn AsyncRead + Unpin + Send>,
        content_length: u64,
        content_type: &str,
        metadata: &std::collections::HashMap<String, String>,
        etag: &str,
    ) -> Result<StreamingPutResult, StorageError>;

    /// Put the bytes of an object that is already indexed back on the disk.
    ///
    /// The repair primitive: a scrubber, or any path that has found a healthy
    /// copy of an object whose local blob is damaged, hands those bytes here
    /// and the engine writes them somewhere of its own. Only the location
    /// changes — the record keeps its ETag, size, metadata, HLC, version and
    /// content hash, because this is the same object, not a new write of it.
    ///
    /// `data` must hash to what the record already claims: the caller is
    /// restoring this object, and bytes that are not this object's would be a
    /// silent overwrite. Content that does not match is refused with
    /// [`StorageError::ChecksumMismatch`].
    ///
    /// The blob is materialised rather than deduplicated — the copy the dedup
    /// index names may be exactly the damaged one — and the dedup entry for
    /// this content is moved onto it, so the next writer of the same content
    /// lands on the healthy copy.
    ///
    /// # Errors
    ///
    /// - [`StorageError::ObjectNotFound`] when no record matches.
    /// - [`StorageError::ChecksumMismatch`] when `data` is not this object's
    ///   content.
    /// - [`StorageError::InvalidData`] for objects this primitive does not
    ///   cover: legacy inline objects, multipart composites and objects whose
    ///   bytes live in an erasure-coded pool rather than in a local volume.
    async fn heal_object_blob(
        &self,
        _bucket: &str,
        _key: &str,
        _version_id: Option<&str>,
        _data: &[u8],
    ) -> Result<(), StorageError> {
        Err(StorageError::InvalidData(
            "this storage engine cannot heal object bytes".to_string(),
        ))
    }
}

// ============================================================================
// StateMachine Trait (Phase 6)
// ============================================================================

/// Information about a single volume file, used in snapshots.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeFileInfo {
    /// Volume file ID (maps to `volume_XXXXXX.dat`).
    pub volume_id: u32,
    /// Size of the volume file in bytes.
    pub size_bytes: u64,
}

/// Keyspace data dump — raw key-value pairs from a single keyspace.
///
/// Used in snapshots to transfer the full state of a keyspace to a replica.
/// The key and value formats are opaque bytes (bincode/JSON depending on
/// the keyspace).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyspaceSnapshot {
    /// Name of the keyspace (e.g. "objects", "versions", "buckets").
    pub name: String,
    /// Raw key-value pairs.
    pub entries: Vec<(Vec<u8>, Vec<u8>)>,
}

/// A point-in-time snapshot of the storage engine state.
///
/// Snapshots are used for:
/// - Bootstrapping a new replica from scratch
/// - Catching up a lagging replica when the journal tail is insufficient
///
/// A snapshot combined with the journal entries after `sequence` provides
/// a complete, consistent view of the storage state.
///
/// # Size Considerations
///
/// Snapshots include full keyspace dumps and can be large for datasets
/// with millions of objects. In production distributed mode, snapshot
/// transfer should be streamed rather than held entirely in memory.
/// This struct is designed for correctness first; streaming optimization
/// is deferred to the distributed implementation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    /// Placement group this snapshot belongs to.
    pub placement_group: PlacementGroupId,
    /// The sequence number of the last journal entry included in this snapshot.
    ///
    /// Replaying journal entries with `sequence > this value` on top of
    /// this snapshot produces the current state.
    pub sequence: u64,
    /// Timestamp when the snapshot was taken (Unix nanoseconds).
    pub timestamp: u64,
    /// List of volume files and their sizes.
    ///
    /// The snapshot does NOT include volume data — volumes are transferred
    /// separately (lazy fetch). This list tells the replica which volumes
    /// it needs.
    pub volume_files: Vec<VolumeFileInfo>,
    /// Full dump of each keyspace (objects, versions, buckets, iam, dedup).
    ///
    /// The journal keyspace is NOT included — it is reconstructed from
    /// entries after `sequence`.
    pub keyspaces: Vec<KeyspaceSnapshot>,
}

/// Deterministic state machine for replay and replication (Phase 6).
///
/// In single-node mode, this is called directly after writing to the journal.
/// In future distributed mode (EE), this will be called after Raft consensus
/// to apply committed operations.
///
/// # Separation from StorageEngine
///
/// `StorageEngine` defines the **public contract** for S3 API consumers
/// (s4-api, s4-features, s4-server). `StateMachine` defines the **internal
/// contract** for the replication layer:
///
/// - `StorageEngine` = "what to do" (high-level S3 operations)
/// - `StateMachine` = "how to deterministically apply" (journal replay)
///
/// `snapshot()` and `restore()` are not needed for S3 API operations,
/// only for replication — hence the separate trait.
///
/// # Future Replication Model
///
/// ```text
/// Leader Node (PG owner):
///   1. HTTP request → StorageEngine
///   2. Write to volume
///   3. Append to journal
///   4. Raft consensus (future) or direct apply (single-node)
///   5. Apply to materialized index
///
/// Replica Node (future):
///   1. Receive journal stream from leader
///   2. Fetch volume segments (lazy)
///   3. Replay journal entries → apply to local index
///   4. Serve read requests from local materialized index
///
/// Catch-up (lagging replica):
///   1. Request snapshot from leader
///   2. restore(snapshot)
///   3. Resume journal streaming from snapshot.sequence
/// ```
///
/// # Distributed Semantics
///
/// Atomic directory rename is guaranteed ONLY within a single
/// Placement Group. Cross-PG atomic rename is NOT supported
/// and will return an error in distributed mode.
///
/// For single-node deployment, all data is in PG(0),
/// so this limitation does not apply.
#[async_trait]
pub trait StateMachine: Send + Sync {
    /// Applies a committed journal entry to the materialized index.
    ///
    /// Used for:
    /// - Replay during recovery
    /// - Applying replicated operations from a leader (EE)
    ///
    /// # Idempotency
    ///
    /// This method MUST be idempotent: applying the same journal entry
    /// (identified by its sequence number) multiple times must produce
    /// the same result as applying it once. This is critical for
    /// at-least-once delivery in replication scenarios.
    ///
    /// # Errors
    ///
    /// Returns `StorageError` if the entry cannot be applied (e.g.,
    /// I/O error, corrupt entry). The caller should retry or escalate.
    async fn apply(&self, entry: &JournalEntry) -> Result<(), StorageError>;

    /// Creates a point-in-time snapshot of the current state.
    ///
    /// Used for:
    /// - Bootstrapping a new replica
    /// - Catching up a lagging replica when the journal tail is insufficient
    ///
    /// The snapshot includes:
    /// - Current state of all fjall keyspaces (objects, versions, buckets, iam, dedup)
    /// - List of volume files with their sizes
    /// - Last applied sequence number
    ///
    /// The journal keyspace is NOT snapshotted — it is replayed from the
    /// sequence number onward.
    async fn snapshot(&self) -> Result<Snapshot, StorageError>;

    /// Restores state from a snapshot, replacing the current local state.
    ///
    /// After restoration, the caller should resume journal streaming from
    /// `snapshot.sequence` to catch up to the leader's current state.
    ///
    /// # Warning
    ///
    /// This method replaces ALL local index state. It should only be called
    /// during initial replica bootstrap or after determining that incremental
    /// catch-up via journal replay is insufficient.
    async fn restore(&self, snapshot: Snapshot) -> Result<(), StorageError>;
}
