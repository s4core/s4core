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

//! Content-addressable deduplication engine (Phase 4: disk-backed).
//!
//! Stores dedup entries in a dedicated fjall keyspace instead of an in-memory
//! `HashMap`. This eliminates startup rebuild time and reduces RAM usage from
//! O(n) to near-zero (fjall block cache handles hot entries automatically).
//!
//! # Storage Layout
//!
//! | Key (32 bytes)         | Value (bincode)                       |
//! |------------------------|---------------------------------------|
//! | SHA-256 content hash   | `DedupEntry { volume_id, offset, ref_count }` |
//!
//! # Thread Safety
//!
//! fjall keyspaces are internally thread-safe (MVCC, lock-free reads).
//! No external mutex is required. The `Deduplicator` can be shared across
//! threads without `Arc<RwLock<>>`.
//!
//! # Atomic Batch Integration
//!
//! For crash-safe reference counting, use [`make_register_op`](Deduplicator::make_register_op)
//! and [`make_unregister_op`](Deduplicator::make_unregister_op) to create
//! [`BatchOp`] entries that can be committed atomically
//! together with object/version writes via `IndexDb::batch_write`.

use crate::error::StorageError;
use crate::storage::index::{BatchAction, BatchOp, KeyspaceId};
use crate::storage::placement::PlacementGroupId;
use crate::types::IndexRecord;
use fjall::Keyspace;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

// ============================================================================
// DedupEntry
// ============================================================================

/// Persistent dedup entry stored in the fjall `dedup` keyspace.
///
/// Maps a content hash to the physical location of the unique blob in a
/// volume file, and tracks how many index records reference this content.
///
/// # Reference Counting
///
/// - `ref_count` is incremented when a new object references the same content.
/// - `ref_count` is decremented when an object referencing this content is deleted.
/// - When `ref_count` reaches 0, the entry is removed (content is eligible for
///   compaction/GC in the volume file).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DedupEntry {
    /// Volume file ID where the blob is stored.
    pub volume_id: u32,
    /// Byte offset within the volume file.
    pub offset: u64,
    /// Number of objects referencing this content.
    pub ref_count: u32,
}

// ============================================================================
// Deduplicator
// ============================================================================

/// Disk-backed content-addressable deduplication engine.
///
/// Replaces the previous in-memory `HashMap<[u8; 32], (u32, u64, u32)>` with
/// a persistent fjall keyspace. Benefits:
///
/// - **Zero startup rebuild**: Data survives crash/restart without scanning the
///   entire objects keyspace.
/// - **Near-zero RAM**: fjall block cache automatically manages hot entries.
///   10M objects = ~0 MB RAM overhead (vs. ~480 MB with HashMap).
/// - **Crash-safe refcount**: Register/unregister ops are included in atomic
///   batch writes, so object record and dedup entry are always consistent.
///
/// # Placement Group Scoping (Phase 6)
///
/// Deduplication is scoped per placement group. Each dedup key is prefixed
/// with the PG ID (4 bytes big-endian), ensuring that dedup lookups only
/// match content within the same PG. In single-node mode, all entries use
/// `PlacementGroupId::LOCAL` (0) and the behavior is identical to pre-Phase 6.
///
/// Global cross-cluster dedup is an explicit non-goal (see ADR-3 in the
/// migration plan). Per-PG dedup avoids distributed lock coordination
/// while still achieving effective dedup within each shard.
///
/// # Thread Safety
///
/// All methods take `&self` (no `&mut self`). fjall provides MVCC with
/// lock-free reads and internally synchronized writes.
#[derive(Clone)]
pub struct Deduplicator {
    /// The fjall keyspace backing dedup storage.
    partition: Keyspace,
    /// Placement group this deduplicator is scoped to.
    pg_id: PlacementGroupId,
}

impl Deduplicator {
    /// Creates a new deduplicator backed by the given fjall keyspace.
    ///
    /// # Arguments
    ///
    /// * `partition` - A fjall `Keyspace` handle for the `"dedup"` keyspace,
    ///   typically obtained via [`IndexDb::dedup_keyspace()`](super::IndexDb::dedup_keyspace).
    /// * `pg_id` - Placement group this deduplicator is scoped to.
    ///   Use `PlacementGroupId::LOCAL` for single-node deployments.
    pub fn new(partition: Keyspace, pg_id: PlacementGroupId) -> Self {
        Self { partition, pg_id }
    }

    /// Returns the placement group this deduplicator is scoped to.
    pub fn placement_group(&self) -> PlacementGroupId {
        self.pg_id
    }

    /// Constructs a PG-scoped dedup key: `PG_ID (4 bytes) + content_hash (32 bytes)`.
    ///
    /// The PG prefix ensures that dedup lookups only match content within the
    /// same placement group. For single-node (`PG_ID = 0`), the prefix is
    /// `[0, 0, 0, 0]`.
    #[inline]
    pub(crate) fn make_key(&self, content_hash: &[u8; 32]) -> Vec<u8> {
        let mut key = Vec::with_capacity(36);
        key.extend_from_slice(&self.pg_id.to_be_bytes());
        key.extend_from_slice(content_hash);
        key
    }

    /// Checks if the dedup keyspace contains entries in the old format
    /// (32-byte keys without PG prefix).
    ///
    /// Returns `true` if migration is needed (old format detected).
    /// Returns `false` if the keyspace is empty or already uses new format.
    pub fn needs_key_format_migration(&self) -> Result<bool, StorageError> {
        if let Some(guard) = self.partition.iter().next() {
            let (key_bytes, _) =
                guard.into_inner().map_err(|e| StorageError::Database(e.to_string()))?;
            // Old format: 32 bytes (just hash), new format: 36 bytes (PG_ID + hash)
            return Ok(key_bytes.len() == 32);
        }
        Ok(false)
    }

    /// Removes all entries from the dedup keyspace.
    ///
    /// Used during key format migration (Phase 6) to clear old-format entries
    /// before rebuilding with PG-prefixed keys. The dedup data is always
    /// rebuildable from the objects keyspace via [`build_from_index`](Self::build_from_index).
    pub fn clear_all(&self) -> Result<(), StorageError> {
        let keys_to_remove: Vec<Vec<u8>> = self
            .partition
            .iter()
            .filter_map(|guard| guard.into_inner().ok().map(|(key_bytes, _)| key_bytes.to_vec()))
            .collect();

        for key in keys_to_remove {
            self.partition
                .remove(&key[..])
                .map_err(|e| StorageError::Database(e.to_string()))?;
        }
        Ok(())
    }

    /// Computes the SHA-256 hash of the given data.
    ///
    /// This is a pure function with no I/O. Used both for dedup lookups
    /// and for content integrity verification on read.
    pub fn compute_hash(data: &[u8]) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(data);
        hasher.finalize().into()
    }

    // ========================================================================
    // Read operations (lock-free MVCC)
    // ========================================================================

    /// Checks if content with the given hash already exists.
    ///
    /// Returns `Some((volume_id, offset))` if the content exists, `None` otherwise.
    ///
    /// This is a lock-free MVCC read — safe to call concurrently from any thread.
    /// The lookup is scoped to this deduplicator's placement group.
    pub fn check_existing(
        &self,
        content_hash: &[u8; 32],
    ) -> Result<Option<(u32, u64)>, StorageError> {
        let key = self.make_key(content_hash);
        match self.partition.get(&key) {
            Ok(Some(value)) => {
                let entry: DedupEntry = bincode::deserialize(&value)
                    .map_err(|e| StorageError::Serialization(e.to_string()))?;
                Ok(Some((entry.volume_id, entry.offset)))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(StorageError::Database(e.to_string())),
        }
    }

    /// Retrieves the full [`DedupEntry`] for a content hash.
    ///
    /// Unlike [`check_existing`](Self::check_existing), this returns the
    /// complete entry including the reference count. The lookup is scoped
    /// to this deduplicator's placement group.
    pub fn get_entry(&self, content_hash: &[u8; 32]) -> Result<Option<DedupEntry>, StorageError> {
        let key = self.make_key(content_hash);
        match self.partition.get(&key) {
            Ok(Some(value)) => {
                let entry: DedupEntry = bincode::deserialize(&value)
                    .map_err(|e| StorageError::Serialization(e.to_string()))?;
                Ok(Some(entry))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(StorageError::Database(e.to_string())),
        }
    }

    // ========================================================================
    // Batch operation builders (for atomic batch writes)
    // ========================================================================

    /// Creates a [`BatchOp`] to register content (create or increment ref count).
    ///
    /// Reads the current dedup entry and produces a `BatchOp` that, when
    /// committed as part of an atomic batch via `IndexDb::batch_write`,
    /// will atomically update the dedup state together with the object record.
    ///
    /// # Arguments
    ///
    /// * `content_hash` - SHA-256 hash of the content
    /// * `volume_id` - Volume file ID where the blob is stored
    /// * `offset` - Byte offset within the volume file
    ///
    /// # Note
    ///
    /// This method does NOT write to the database directly. The returned
    /// `BatchOp` must be included in a call to `IndexDb::batch_write`.
    pub fn make_register_op(
        &self,
        content_hash: [u8; 32],
        volume_id: u32,
        offset: u64,
    ) -> Result<BatchOp, StorageError> {
        let existing = self.get_entry(&content_hash)?;
        let new_entry = match existing {
            Some(mut entry) => {
                entry.ref_count += 1;
                entry
            }
            None => DedupEntry {
                volume_id,
                offset,
                ref_count: 1,
            },
        };
        let pg_key = self.make_key(&content_hash);
        let value = bincode::serialize(&new_entry)
            .map_err(|e| StorageError::Serialization(e.to_string()))?;
        Ok(BatchOp {
            keyspace: KeyspaceId::Dedup,
            action: BatchAction::Put(pg_key, value),
        })
    }

    /// Creates a [`BatchOp`] to unregister content (decrement or remove ref count).
    ///
    /// Reads the current dedup entry and produces a `BatchOp` that will either:
    /// - Decrement the ref count (if `ref_count > 1`), or
    /// - Delete the entry entirely (if `ref_count <= 1`).
    ///
    /// Returns `None` if no entry exists for the given hash (no-op).
    ///
    /// # Note
    ///
    /// This method does NOT write to the database directly.
    pub fn make_unregister_op(
        &self,
        content_hash: &[u8; 32],
    ) -> Result<Option<BatchOp>, StorageError> {
        let pg_key = self.make_key(content_hash);
        let existing = self.get_entry(content_hash)?;
        match existing {
            Some(entry) if entry.ref_count <= 1 => {
                // Last reference — remove entry entirely
                Ok(Some(BatchOp {
                    keyspace: KeyspaceId::Dedup,
                    action: BatchAction::Delete(pg_key),
                }))
            }
            Some(mut entry) => {
                // Decrement ref count
                entry.ref_count -= 1;
                let value = bincode::serialize(&entry)
                    .map_err(|e| StorageError::Serialization(e.to_string()))?;
                Ok(Some(BatchOp {
                    keyspace: KeyspaceId::Dedup,
                    action: BatchAction::Put(pg_key, value),
                }))
            }
            None => Ok(None),
        }
    }

    // ========================================================================
    // Direct write operations (for initialization and backward compatibility)
    // ========================================================================

    /// Registers content directly (without batch).
    ///
    /// Creates a new entry with `ref_count = 1`, or increments the existing
    /// ref count if the hash already exists.
    ///
    /// # When to Use
    ///
    /// - During [`build_from_index`](Self::build_from_index) at startup.
    /// - For tests that need simple, non-batch writes.
    ///
    /// For normal production writes, prefer [`make_register_op`](Self::make_register_op)
    /// with `IndexDb::batch_write` to ensure atomic consistency.
    pub fn register_content(
        &self,
        content_hash: [u8; 32],
        volume_id: u32,
        offset: u64,
    ) -> Result<(), StorageError> {
        let pg_key = self.make_key(&content_hash);
        let existing = self.get_entry(&content_hash)?;
        let new_entry = match existing {
            Some(mut entry) => {
                entry.ref_count += 1;
                entry
            }
            None => DedupEntry {
                volume_id,
                offset,
                ref_count: 1,
            },
        };
        let value = bincode::serialize(&new_entry)
            .map_err(|e| StorageError::Serialization(e.to_string()))?;
        self.partition
            .insert(&pg_key[..], &value[..])
            .map_err(|e| StorageError::Database(e.to_string()))?;
        Ok(())
    }

    /// Unregisters content directly (without batch).
    ///
    /// Decrements the ref count, removing the entry if it reaches zero.
    ///
    /// For normal production writes, prefer [`make_unregister_op`](Self::make_unregister_op)
    /// with `IndexDb::batch_write`.
    pub fn unregister_content(&self, content_hash: &[u8; 32]) -> Result<(), StorageError> {
        let pg_key = self.make_key(content_hash);
        let existing = self.get_entry(content_hash)?;
        if let Some(mut entry) = existing {
            if entry.ref_count <= 1 {
                self.partition
                    .remove(&pg_key[..])
                    .map_err(|e| StorageError::Database(e.to_string()))?;
            } else {
                entry.ref_count -= 1;
                let value = bincode::serialize(&entry)
                    .map_err(|e| StorageError::Serialization(e.to_string()))?;
                self.partition
                    .insert(&pg_key[..], &value[..])
                    .map_err(|e| StorageError::Database(e.to_string()))?;
            }
        }
        Ok(())
    }

    // ========================================================================
    // Compaction support
    // ========================================================================

    /// Iterates all dedup entries in this placement group.
    ///
    /// Returns a vector of `(content_hash, DedupEntry)` pairs.
    /// Used by the compactor to find which blobs are live in a given volume.
    ///
    /// # Performance
    ///
    /// Full keyspace scan filtered by PG prefix. Call from a blocking thread
    /// for large datasets.
    pub fn iter_entries(&self) -> Result<Vec<([u8; 32], DedupEntry)>, StorageError> {
        let pg_prefix = self.pg_id.to_be_bytes();
        let mut results = Vec::new();

        for guard in self.partition.prefix(pg_prefix) {
            let (key_bytes, value_bytes) =
                guard.into_inner().map_err(|e| StorageError::Database(e.to_string()))?;
            if key_bytes.len() != 36 {
                continue; // skip malformed entries
            }
            let mut hash = [0u8; 32];
            hash.copy_from_slice(&key_bytes[4..36]);
            let entry: DedupEntry = bincode::deserialize(&value_bytes)
                .map_err(|e| StorageError::Serialization(e.to_string()))?;
            results.push((hash, entry));
        }

        Ok(results)
    }

    /// Updates the physical location of a dedup entry without changing ref_count.
    ///
    /// Used by the compactor after copying a blob to a new volume.
    /// The content hash stays the same — only volume_id and offset change.
    pub fn update_location(
        &self,
        content_hash: &[u8; 32],
        new_volume_id: u32,
        new_offset: u64,
    ) -> Result<(), StorageError> {
        let pg_key = self.make_key(content_hash);
        let existing = self.get_entry(content_hash)?.ok_or_else(|| {
            StorageError::InvalidData("Cannot update location: dedup entry not found".to_string())
        })?;
        let updated = DedupEntry {
            volume_id: new_volume_id,
            offset: new_offset,
            ref_count: existing.ref_count,
        };
        let value =
            bincode::serialize(&updated).map_err(|e| StorageError::Serialization(e.to_string()))?;
        self.partition
            .insert(&pg_key[..], &value[..])
            .map_err(|e| StorageError::Database(e.to_string()))?;
        Ok(())
    }

    /// Creates a BatchOp to update the physical location of a dedup entry.
    ///
    /// Used by the compactor for atomic batch updates when relocating blobs.
    /// The ref_count is preserved — only volume_id and offset change.
    pub fn make_update_location_op(
        &self,
        content_hash: &[u8; 32],
        new_volume_id: u32,
        new_offset: u64,
    ) -> Result<BatchOp, StorageError> {
        let pg_key = self.make_key(content_hash);
        let existing = self.get_entry(content_hash)?.ok_or_else(|| {
            StorageError::InvalidData("Cannot update location: dedup entry not found".to_string())
        })?;
        let updated = DedupEntry {
            volume_id: new_volume_id,
            offset: new_offset,
            ref_count: existing.ref_count,
        };
        let value =
            bincode::serialize(&updated).map_err(|e| StorageError::Serialization(e.to_string()))?;
        Ok(BatchOp {
            keyspace: KeyspaceId::Dedup,
            action: BatchAction::Put(pg_key, value),
        })
    }

    // ========================================================================
    // Statistics and initialization
    // ========================================================================

    /// Returns dedup statistics: `(unique_blobs, total_references)`.
    ///
    /// Scans the entire dedup keyspace to compute aggregate statistics.
    ///
    /// - `unique_blobs`: Number of distinct content hashes.
    /// - `total_references`: Sum of all reference counts.
    ///
    /// Dedup ratio = `1.0 - (unique_blobs / total_references)` when
    /// `total_references > 0`.
    ///
    /// # Performance
    ///
    /// This method performs a full keyspace scan. For large datasets, call
    /// from a blocking thread (e.g., `tokio::task::spawn_blocking`).
    pub fn stats(&self) -> Result<(u64, u64), StorageError> {
        let mut unique_blobs = 0u64;
        let mut total_references = 0u64;

        for guard in self.partition.iter() {
            let (_, value_bytes) =
                guard.into_inner().map_err(|e| StorageError::Database(e.to_string()))?;
            let entry: DedupEntry = bincode::deserialize(&value_bytes)
                .map_err(|e| StorageError::Serialization(e.to_string()))?;
            unique_blobs += 1;
            total_references += u64::from(entry.ref_count);
        }

        Ok((unique_blobs, total_references))
    }

    /// Returns `true` if the dedup keyspace has no entries.
    ///
    /// Used at startup to determine whether a rebuild from the objects
    /// keyspace is needed (e.g., after Phase 4 migration or data loss).
    pub fn is_empty(&self) -> Result<bool, StorageError> {
        self.partition.is_empty().map_err(|e| StorageError::Database(e.to_string()))
    }

    /// Populates the dedup keyspace from existing index records.
    ///
    /// Should only be called when the dedup keyspace is empty:
    /// - First startup after Phase 4 migration (redb → fjall dedup).
    /// - After metadata database loss (rebuild from volume scan).
    ///
    /// Inline objects (`file_id == u32::MAX`) are excluded since they don't
    /// participate in volume-level deduplication.
    ///
    /// # Arguments
    ///
    /// * `records` - Iterator over `(key, IndexRecord)` pairs from the objects
    ///   keyspace (typically from `IndexDb::list_objects("", usize::MAX)`).
    pub fn build_from_index<I>(&self, records: I) -> Result<(), StorageError>
    where
        I: Iterator<Item = (String, IndexRecord)>,
    {
        for (_, record) in records {
            // Skip inline objects — they don't participate in deduplication
            if record.file_id == u32::MAX {
                continue;
            }
            self.register_content(record.content_hash, record.file_id, record.offset)?;
        }
        Ok(())
    }
}

// ============================================================================
// Unit Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    /// Creates a temporary fjall database and returns the dedup keyspace.
    fn create_test_dedup() -> (TempDir, Deduplicator) {
        let temp = TempDir::new().unwrap();
        let db = fjall::Database::builder(temp.path().join("test_db")).open().unwrap();
        let ks = db.keyspace("dedup", fjall::KeyspaceCreateOptions::default).unwrap();
        (temp, Deduplicator::new(ks, PlacementGroupId::LOCAL))
    }

    #[test]
    fn test_compute_hash() {
        let data1 = b"hello world";
        let data2 = b"hello world";
        let data3 = b"different";

        let hash1 = Deduplicator::compute_hash(data1);
        let hash2 = Deduplicator::compute_hash(data2);
        let hash3 = Deduplicator::compute_hash(data3);

        // Same data produces same hash
        assert_eq!(hash1, hash2);
        // Different data produces different hash
        assert_ne!(hash1, hash3);
    }

    #[test]
    fn test_register_and_check() {
        let (_temp, dedup) = create_test_dedup();
        let hash = Deduplicator::compute_hash(b"test data");

        // Should not exist initially
        assert_eq!(dedup.check_existing(&hash).unwrap(), None);

        // Register content
        dedup.register_content(hash, 1, 100).unwrap();

        // Should now exist
        assert_eq!(dedup.check_existing(&hash).unwrap(), Some((1, 100)));
    }

    #[test]
    fn test_reference_counting() {
        let (_temp, dedup) = create_test_dedup();
        let hash = Deduplicator::compute_hash(b"test data");

        // Register first reference
        dedup.register_content(hash, 1, 100).unwrap();
        let entry = dedup.get_entry(&hash).unwrap().unwrap();
        assert_eq!(entry.ref_count, 1);

        // Register second reference (same content, increments ref count)
        dedup.register_content(hash, 1, 100).unwrap();
        let entry = dedup.get_entry(&hash).unwrap().unwrap();
        assert_eq!(entry.ref_count, 2);

        // Unregister first reference — should still exist with ref_count = 1
        dedup.unregister_content(&hash).unwrap();
        let entry = dedup.get_entry(&hash).unwrap().unwrap();
        assert_eq!(entry.ref_count, 1);
        assert_eq!(dedup.check_existing(&hash).unwrap(), Some((1, 100)));

        // Unregister second reference — entry should be removed
        dedup.unregister_content(&hash).unwrap();
        assert_eq!(dedup.check_existing(&hash).unwrap(), None);
        assert!(dedup.get_entry(&hash).unwrap().is_none());
    }

    #[test]
    fn test_unregister_nonexistent() {
        let (_temp, dedup) = create_test_dedup();
        let hash = Deduplicator::compute_hash(b"does not exist");

        // Unregistering a hash that doesn't exist should be a no-op
        dedup.unregister_content(&hash).unwrap();
        assert!(dedup.check_existing(&hash).unwrap().is_none());
    }

    #[test]
    fn test_stats() {
        let (_temp, dedup) = create_test_dedup();

        // Empty stats
        let (unique, refs) = dedup.stats().unwrap();
        assert_eq!(unique, 0);
        assert_eq!(refs, 0);

        // Register 3 unique entries
        let h1 = Deduplicator::compute_hash(b"data1");
        let h2 = Deduplicator::compute_hash(b"data2");
        let h3 = Deduplicator::compute_hash(b"data3");

        dedup.register_content(h1, 1, 0).unwrap();
        dedup.register_content(h2, 1, 100).unwrap();
        dedup.register_content(h3, 1, 200).unwrap();

        // Add duplicates of h1 (3 refs total)
        dedup.register_content(h1, 1, 0).unwrap();
        dedup.register_content(h1, 1, 0).unwrap();

        let (unique, refs) = dedup.stats().unwrap();
        assert_eq!(unique, 3);
        assert_eq!(refs, 5); // h1=3, h2=1, h3=1
    }

    #[test]
    fn test_is_empty() {
        let (_temp, dedup) = create_test_dedup();

        assert!(dedup.is_empty().unwrap());

        let hash = Deduplicator::compute_hash(b"data");
        dedup.register_content(hash, 1, 0).unwrap();

        assert!(!dedup.is_empty().unwrap());
    }

    #[test]
    fn test_build_from_index() {
        let (_temp, dedup) = create_test_dedup();

        // Create regular and inline records
        let inline_record = IndexRecord::new(
            u32::MAX,
            0,
            100,
            [0u8; 32],
            "etag_inline".to_string(),
            "text/plain".to_string(),
        );

        let regular_record1 = IndexRecord::new(
            1,
            200,
            100,
            [1u8; 32],
            "etag1".to_string(),
            "text/plain".to_string(),
        );

        let regular_record2 = IndexRecord::new(
            2,
            300,
            100,
            [2u8; 32],
            "etag2".to_string(),
            "text/plain".to_string(),
        );

        // Duplicate content (same hash as regular_record1)
        let duplicate_record = IndexRecord::new(
            1,
            200,
            100,
            [1u8; 32],
            "etag1_dup".to_string(),
            "text/plain".to_string(),
        );

        let records = vec![
            ("key1".to_string(), inline_record),
            ("key2".to_string(), regular_record1),
            ("key3".to_string(), regular_record2),
            ("key4".to_string(), duplicate_record),
        ];

        dedup.build_from_index(records.into_iter()).unwrap();

        // Inline record should not be registered
        assert!(dedup.check_existing(&[0u8; 32]).unwrap().is_none());

        // Regular records should be registered
        assert_eq!(dedup.check_existing(&[1u8; 32]).unwrap(), Some((1, 200)));
        assert_eq!(dedup.check_existing(&[2u8; 32]).unwrap(), Some((2, 300)));

        // Duplicate content should have ref_count = 2
        let entry = dedup.get_entry(&[1u8; 32]).unwrap().unwrap();
        assert_eq!(entry.ref_count, 2);
    }

    #[test]
    fn test_make_register_op_new_entry() {
        let (_temp, dedup) = create_test_dedup();
        let hash = Deduplicator::compute_hash(b"new content");

        let op = dedup.make_register_op(hash, 1, 100).unwrap();
        assert_eq!(op.keyspace, KeyspaceId::Dedup);

        // Verify the batch op contains correct PG-scoped key (36 bytes)
        let expected_key = dedup.make_key(&hash);
        match &op.action {
            BatchAction::Put(key, value) => {
                assert_eq!(key, &expected_key);
                assert_eq!(key.len(), 36); // 4 (PG) + 32 (hash)
                let entry: DedupEntry = bincode::deserialize(value).unwrap();
                assert_eq!(entry.volume_id, 1);
                assert_eq!(entry.offset, 100);
                assert_eq!(entry.ref_count, 1);
            }
            BatchAction::Delete(_) => panic!("Expected Put, got Delete"),
        }
    }

    #[test]
    fn test_make_register_op_existing_entry() {
        let (_temp, dedup) = create_test_dedup();
        let hash = Deduplicator::compute_hash(b"existing content");

        // Pre-populate
        dedup.register_content(hash, 1, 100).unwrap();

        // Make register op should increment ref count
        let op = dedup.make_register_op(hash, 1, 100).unwrap();
        match &op.action {
            BatchAction::Put(_, value) => {
                let entry: DedupEntry = bincode::deserialize(value).unwrap();
                assert_eq!(entry.ref_count, 2);
            }
            _ => panic!("Expected Put"),
        }
    }

    #[test]
    fn test_make_unregister_op_decrement() {
        let (_temp, dedup) = create_test_dedup();
        let hash = Deduplicator::compute_hash(b"multi ref");

        // Register twice
        dedup.register_content(hash, 1, 100).unwrap();
        dedup.register_content(hash, 1, 100).unwrap();

        // Unregister op should decrement (not delete)
        let op = dedup.make_unregister_op(&hash).unwrap().unwrap();
        match &op.action {
            BatchAction::Put(_, value) => {
                let entry: DedupEntry = bincode::deserialize(value).unwrap();
                assert_eq!(entry.ref_count, 1);
            }
            _ => panic!("Expected Put with decremented ref count"),
        }
    }

    #[test]
    fn test_make_unregister_op_delete() {
        let (_temp, dedup) = create_test_dedup();
        let hash = Deduplicator::compute_hash(b"single ref");

        // Register once
        dedup.register_content(hash, 1, 100).unwrap();

        // Unregister op should delete the entry with PG-scoped key
        let expected_key = dedup.make_key(&hash);
        let op = dedup.make_unregister_op(&hash).unwrap().unwrap();
        match &op.action {
            BatchAction::Delete(key) => {
                assert_eq!(key, &expected_key);
                assert_eq!(key.len(), 36);
            }
            _ => panic!("Expected Delete for last reference"),
        }
    }

    #[test]
    fn test_make_unregister_op_nonexistent() {
        let (_temp, dedup) = create_test_dedup();
        let hash = Deduplicator::compute_hash(b"does not exist");

        // Should return None (no-op)
        assert!(dedup.make_unregister_op(&hash).unwrap().is_none());
    }

    #[test]
    fn test_dedup_entry_serialization() {
        let entry = DedupEntry {
            volume_id: 42,
            offset: 123456,
            ref_count: 7,
        };

        let bytes = bincode::serialize(&entry).unwrap();
        let deserialized: DedupEntry = bincode::deserialize(&bytes).unwrap();

        assert_eq!(entry, deserialized);
    }

    #[test]
    fn test_persistence_across_reopen() {
        let temp = TempDir::new().unwrap();
        let db_path = temp.path().join("test_db");
        let hash = Deduplicator::compute_hash(b"persistent data");

        // Write data
        {
            let db = fjall::Database::builder(&db_path).open().unwrap();
            let ks = db.keyspace("dedup", fjall::KeyspaceCreateOptions::default).unwrap();
            let dedup = Deduplicator::new(ks, PlacementGroupId::LOCAL);
            dedup.register_content(hash, 5, 999).unwrap();
            db.persist(fjall::PersistMode::SyncAll).unwrap();
        }

        // Reopen and verify data persists
        {
            let db = fjall::Database::builder(&db_path).open().unwrap();
            let ks = db.keyspace("dedup", fjall::KeyspaceCreateOptions::default).unwrap();
            let dedup = Deduplicator::new(ks, PlacementGroupId::LOCAL);

            let entry = dedup.get_entry(&hash).unwrap().unwrap();
            assert_eq!(entry.volume_id, 5);
            assert_eq!(entry.offset, 999);
            assert_eq!(entry.ref_count, 1);
        }
    }

    // ================================================================
    // Phase 6: Placement group scoping tests
    // ================================================================

    #[test]
    fn test_pg_scoped_keys_are_36_bytes() {
        let (_temp, dedup) = create_test_dedup();
        let hash = Deduplicator::compute_hash(b"test");

        dedup.register_content(hash, 1, 0).unwrap();

        // Verify the key in the keyspace is 36 bytes (4 PG + 32 hash)
        let first_entry = dedup.partition.iter().next().unwrap();
        let (key_bytes, _) = first_entry.into_inner().unwrap();
        assert_eq!(key_bytes.len(), 36);

        // First 4 bytes should be PG_ID = 0 (LOCAL)
        assert_eq!(&key_bytes[..4], &[0, 0, 0, 0]);
    }

    #[test]
    fn test_different_pgs_are_isolated() {
        let temp = TempDir::new().unwrap();
        let db = fjall::Database::builder(temp.path().join("test_db")).open().unwrap();
        let ks = db.keyspace("dedup", fjall::KeyspaceCreateOptions::default).unwrap();

        let dedup_pg0 = Deduplicator::new(ks.clone(), PlacementGroupId(0));
        let dedup_pg1 = Deduplicator::new(ks.clone(), PlacementGroupId(1));

        let hash = Deduplicator::compute_hash(b"shared content");

        // Register in PG 0
        dedup_pg0.register_content(hash, 1, 100).unwrap();

        // PG 0 should see it
        assert!(dedup_pg0.check_existing(&hash).unwrap().is_some());

        // PG 1 should NOT see it (different PG scope)
        assert!(dedup_pg1.check_existing(&hash).unwrap().is_none());

        // Register same content in PG 1
        dedup_pg1.register_content(hash, 2, 200).unwrap();

        // Both should see their own entries
        let entry_pg0 = dedup_pg0.get_entry(&hash).unwrap().unwrap();
        assert_eq!(entry_pg0.volume_id, 1);
        assert_eq!(entry_pg0.ref_count, 1);

        let entry_pg1 = dedup_pg1.get_entry(&hash).unwrap().unwrap();
        assert_eq!(entry_pg1.volume_id, 2);
        assert_eq!(entry_pg1.ref_count, 1);
    }

    #[test]
    fn test_needs_key_format_migration_empty() {
        let (_temp, dedup) = create_test_dedup();
        // Empty keyspace should not need migration
        assert!(!dedup.needs_key_format_migration().unwrap());
    }

    #[test]
    fn test_needs_key_format_migration_new_format() {
        let (_temp, dedup) = create_test_dedup();
        let hash = Deduplicator::compute_hash(b"test");
        dedup.register_content(hash, 1, 0).unwrap();

        // New format (36 bytes) should not need migration
        assert!(!dedup.needs_key_format_migration().unwrap());
    }

    #[test]
    fn test_needs_key_format_migration_old_format() {
        let temp = TempDir::new().unwrap();
        let db = fjall::Database::builder(temp.path().join("test_db")).open().unwrap();
        let ks = db.keyspace("dedup", fjall::KeyspaceCreateOptions::default).unwrap();

        // Simulate old format: write with 32-byte key directly
        let hash = [42u8; 32];
        let entry = DedupEntry {
            volume_id: 1,
            offset: 0,
            ref_count: 1,
        };
        let value = bincode::serialize(&entry).unwrap();
        ks.insert(hash.as_slice(), &value[..]).unwrap();

        let dedup = Deduplicator::new(ks, PlacementGroupId::LOCAL);
        assert!(dedup.needs_key_format_migration().unwrap());
    }

    #[test]
    fn test_clear_all() {
        let (_temp, dedup) = create_test_dedup();

        let h1 = Deduplicator::compute_hash(b"data1");
        let h2 = Deduplicator::compute_hash(b"data2");
        dedup.register_content(h1, 1, 0).unwrap();
        dedup.register_content(h2, 1, 100).unwrap();

        assert!(!dedup.is_empty().unwrap());

        dedup.clear_all().unwrap();
        assert!(dedup.is_empty().unwrap());
    }

    #[test]
    fn test_placement_group_accessor() {
        let (_temp, dedup) = create_test_dedup();
        assert_eq!(dedup.placement_group(), PlacementGroupId::LOCAL);
    }

    // ================================================================
    // Compaction support tests
    // ================================================================

    #[test]
    fn test_iter_entries() {
        let (_temp, dedup) = create_test_dedup();

        let h1 = Deduplicator::compute_hash(b"data1");
        let h2 = Deduplicator::compute_hash(b"data2");
        let h3 = Deduplicator::compute_hash(b"data3");

        dedup.register_content(h1, 0, 0).unwrap();
        dedup.register_content(h2, 0, 100).unwrap();
        dedup.register_content(h3, 1, 0).unwrap();

        let entries = dedup.iter_entries().unwrap();
        assert_eq!(entries.len(), 3);

        // Filter by volume 0
        let vol0: Vec<_> = entries.iter().filter(|(_, e)| e.volume_id == 0).collect();
        assert_eq!(vol0.len(), 2);
    }

    #[test]
    fn test_iter_entries_empty() {
        let (_temp, dedup) = create_test_dedup();
        let entries = dedup.iter_entries().unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn test_update_location() {
        let (_temp, dedup) = create_test_dedup();
        let hash = Deduplicator::compute_hash(b"movable");

        dedup.register_content(hash, 0, 100).unwrap();

        // Update location
        dedup.update_location(&hash, 5, 200).unwrap();

        let entry = dedup.get_entry(&hash).unwrap().unwrap();
        assert_eq!(entry.volume_id, 5);
        assert_eq!(entry.offset, 200);
        assert_eq!(entry.ref_count, 1); // ref_count unchanged
    }

    #[test]
    fn test_update_location_preserves_ref_count() {
        let (_temp, dedup) = create_test_dedup();
        let hash = Deduplicator::compute_hash(b"multi-ref");

        // Register 3 references
        dedup.register_content(hash, 0, 100).unwrap();
        dedup.register_content(hash, 0, 100).unwrap();
        dedup.register_content(hash, 0, 100).unwrap();

        dedup.update_location(&hash, 7, 500).unwrap();

        let entry = dedup.get_entry(&hash).unwrap().unwrap();
        assert_eq!(entry.volume_id, 7);
        assert_eq!(entry.offset, 500);
        assert_eq!(entry.ref_count, 3); // preserved
    }

    #[test]
    fn test_update_location_nonexistent() {
        let (_temp, dedup) = create_test_dedup();
        let hash = Deduplicator::compute_hash(b"ghost");

        let result = dedup.update_location(&hash, 1, 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_make_update_location_op() {
        let (_temp, dedup) = create_test_dedup();
        let hash = Deduplicator::compute_hash(b"batch-move");

        dedup.register_content(hash, 0, 100).unwrap();
        dedup.register_content(hash, 0, 100).unwrap(); // ref_count = 2

        let op = dedup.make_update_location_op(&hash, 3, 400).unwrap();
        assert_eq!(op.keyspace, KeyspaceId::Dedup);

        match &op.action {
            BatchAction::Put(key, value) => {
                assert_eq!(key.len(), 36); // PG(4) + hash(32)
                let entry: DedupEntry = bincode::deserialize(&value[..]).unwrap();
                assert_eq!(entry.volume_id, 3);
                assert_eq!(entry.offset, 400);
                assert_eq!(entry.ref_count, 2); // preserved
            }
            BatchAction::Delete(_) => panic!("Expected Put, got Delete"),
        }
    }
}
