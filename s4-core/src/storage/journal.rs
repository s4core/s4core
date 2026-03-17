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

//! Metadata journal for ordered, replayable mutation logging (Phase 5).
//!
//! The journal records every metadata mutation in the `journal` keyspace of the
//! fjall database. Each entry has a monotonically increasing sequence number and
//! is written in the same atomic batch as the corresponding index update. This
//! guarantees that the journal is always consistent with the materialized index.
//!
//! # Purpose
//!
//! 1. **Full-fidelity recovery:** Replaying the journal restores `content_type`,
//!    user metadata, Object Lock fields, and delete markers — information that
//!    volume-only recovery cannot provide.
//!
//! 2. **Replication readiness:** The ordered, per-entry journal stream is the
//!    foundation for future leader → replica journal shipping (Phase 6).
//!
//! # Compaction
//!
//! The journal grows without bound unless compacted. [`MetadataJournal::compact`]
//! removes entries older than a safe checkpoint. In future distributed mode,
//! compaction must respect the minimum confirmed sequence across all replicas.

use crate::error::StorageError;
use crate::storage::placement::PlacementGroupId;
use crate::types::IndexRecord;
use fjall::Keyspace;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

// ============================================================================
// Journal Event Types
// ============================================================================

/// Type of mutation event recorded in the journal.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum JournalEventType {
    /// An object was created or overwritten (PUT).
    ObjectPut {
        /// Bucket name.
        bucket: String,
        /// Object key (without bucket prefix).
        key: String,
        /// Version ID (if versioned).
        version_id: Option<String>,
        /// Full index record snapshot at the time of the mutation.
        record: IndexRecord,
    },

    /// An object was permanently deleted (DELETE).
    ObjectDelete {
        /// Bucket name.
        bucket: String,
        /// Object key.
        key: String,
        /// Version ID (if a specific version was deleted).
        version_id: Option<String>,
    },

    /// A delete marker was created (versioned DELETE without version ID).
    DeleteMarkerCreated {
        /// Bucket name.
        bucket: String,
        /// Object key.
        key: String,
        /// Version ID of the newly created delete marker.
        marker_version_id: String,
        /// The delete marker index record.
        record: IndexRecord,
    },

    /// Object metadata was updated (e.g. Object Lock retention/legal hold).
    MetadataUpdate {
        /// Bucket name.
        bucket: String,
        /// Object key.
        key: String,
        /// Version ID of the updated object.
        version_id: Option<String>,
        /// Map of updated field names to their new values.
        updated_fields: HashMap<String, String>,
    },

    /// A bucket-level operation (create, delete, configuration change).
    BucketOperation {
        /// Bucket name.
        bucket: String,
        /// Type of bucket operation.
        op: BucketOp,
    },

    /// An IAM-related operation.
    IamOperation {
        /// Type of IAM operation.
        op: IamOp,
    },
}

/// Bucket operation type.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BucketOp {
    /// Bucket was created.
    Created,
    /// Bucket was deleted.
    Deleted,
    /// Bucket versioning status was changed.
    VersioningChanged {
        /// New versioning status as a string.
        status: String,
    },
}

/// IAM operation type.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IamOp {
    /// A user was created.
    UserCreated {
        /// User identifier.
        user_id: String,
    },
    /// A user was deleted.
    UserDeleted {
        /// User identifier.
        user_id: String,
    },
    /// An access key was created.
    AccessKeyCreated {
        /// Access key ID.
        access_key_id: String,
    },
    /// An access key was deleted.
    AccessKeyDeleted {
        /// Access key ID.
        access_key_id: String,
    },
}

/// A single entry in the metadata journal.
///
/// Each entry is uniquely identified by its `sequence` number within a
/// placement group. In single-node mode, all entries belong to
/// [`PlacementGroupId::LOCAL`].
///
/// # Backward Compatibility
///
/// The `placement_group` field was added in Phase 6. Journal entries created
/// before Phase 6 will deserialize with the default value
/// (`PlacementGroupId::LOCAL`), preserving full backward compatibility.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JournalEntry {
    /// Monotonically increasing sequence number within this placement group.
    ///
    /// Sequence numbers are guaranteed to be strictly increasing within a PG.
    /// This enables:
    /// - Ordered replay during recovery
    /// - Journal shipping for leader → replica replication
    /// - Catch-up of lagging replicas via `iter_entries_after(sequence)`
    pub sequence: u64,
    /// Timestamp when this entry was created (Unix nanoseconds).
    pub timestamp: u64,
    /// Placement group this entry belongs to.
    ///
    /// In single-node mode, this is always `PlacementGroupId::LOCAL` (0).
    /// In distributed mode, each PG has its own monotonic sequence space.
    #[serde(default)]
    pub placement_group: PlacementGroupId,
    /// The mutation event.
    pub event: JournalEventType,
}

// ============================================================================
// MetadataJournal
// ============================================================================

/// Append-only metadata journal backed by a fjall keyspace.
///
/// Thread-safe: the internal sequence counter is atomic and fjall handles
/// concurrent writes internally. No external locking is required.
///
/// # Placement Group Awareness (Phase 6)
///
/// Each journal instance is scoped to a single placement group. In
/// single-node mode, there is one journal with `PlacementGroupId::LOCAL`.
/// Every entry produced by [`make_journal_entry_op`](Self::make_journal_entry_op)
/// is tagged with the journal's placement group ID, enabling future
/// per-PG journal shipping for replication.
///
/// # Batch Integration
///
/// The journal does NOT write entries on its own. Instead, callers use
/// [`make_journal_entry_op`](Self::make_journal_entry_op) to obtain a
/// [`super::BatchOp`] that is included in the same atomic batch as the
/// index mutation. This guarantees that the journal and index are always
/// consistent.
pub struct MetadataJournal {
    /// The fjall keyspace used for journal storage.
    partition: Keyspace,
    /// Current sequence number (monotonically increasing within this PG).
    sequence: AtomicU64,
    /// Placement group this journal belongs to.
    placement_group: PlacementGroupId,
}

impl MetadataJournal {
    /// Creates a new journal or opens an existing one.
    ///
    /// On open, scans the keyspace to find the highest existing sequence number
    /// so that new entries continue from the correct position.
    ///
    /// # Arguments
    ///
    /// * `partition` - The fjall keyspace handle for journal storage.
    /// * `placement_group` - The placement group this journal is scoped to.
    ///   Use `PlacementGroupId::LOCAL` for single-node deployments.
    pub fn new(
        partition: Keyspace,
        placement_group: PlacementGroupId,
    ) -> Result<Self, StorageError> {
        // Find the current max sequence by reading the last entry.
        // Journal keys are big-endian u64 sequence numbers, so the last
        // entry in iteration order is the highest.
        let max_seq = if let Some(guard) = partition.iter().next_back() {
            let (key_bytes, _) =
                guard.into_inner().map_err(|e| StorageError::Database(e.to_string()))?;
            if key_bytes.len() == 8 {
                u64::from_be_bytes(key_bytes[..8].try_into().unwrap_or([0u8; 8]))
            } else {
                0
            }
        } else {
            0
        };

        Ok(Self {
            partition,
            sequence: AtomicU64::new(max_seq),
            placement_group,
        })
    }

    /// Returns the current (latest) sequence number.
    pub fn current_sequence(&self) -> u64 {
        self.sequence.load(Ordering::SeqCst)
    }

    /// Returns the total number of entries in the journal.
    pub fn entry_count(&self) -> Result<u64, StorageError> {
        let mut count = 0u64;
        for guard in self.partition.iter() {
            let _ = guard.into_inner().map_err(|e| StorageError::Database(e.to_string()))?;
            count += 1;
        }
        Ok(count)
    }

    /// Returns the approximate total size in bytes of all journal entries.
    ///
    /// This sums the key and value sizes for every entry in the journal
    /// keyspace. The result is an approximation of the logical data size;
    /// actual on-disk size may differ due to LSM-tree compression and
    /// internal metadata overhead.
    pub fn size_bytes(&self) -> Result<u64, StorageError> {
        let mut total = 0u64;
        for guard in self.partition.iter() {
            let (key, value) =
                guard.into_inner().map_err(|e| StorageError::Database(e.to_string()))?;
            total += key.len() as u64 + value.len() as u64;
        }
        Ok(total)
    }

    /// Allocates the next sequence number and creates a [`super::BatchOp`]
    /// that writes the journal entry to the `journal` keyspace.
    ///
    /// The returned `BatchOp` should be included in the same atomic batch as
    /// the corresponding index mutation.
    pub fn make_journal_entry_op(
        &self,
        event: JournalEventType,
    ) -> Result<super::BatchOp, StorageError> {
        let seq = self.sequence.fetch_add(1, Ordering::SeqCst) + 1;
        let entry = JournalEntry {
            sequence: seq,
            timestamp: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64,
            placement_group: self.placement_group,
            event,
        };

        let key = seq.to_be_bytes().to_vec();
        let value =
            bincode::serialize(&entry).map_err(|e| StorageError::Serialization(e.to_string()))?;

        Ok(super::BatchOp {
            keyspace: super::KeyspaceId::Journal,
            action: super::BatchAction::Put(key, value),
        })
    }

    /// Returns the placement group this journal is scoped to.
    pub fn placement_group(&self) -> PlacementGroupId {
        self.placement_group
    }

    /// Returns a clone of the underlying keyspace handle.
    pub fn keyspace(&self) -> Keyspace {
        self.partition.clone()
    }

    /// Iterates over all journal entries in sequence order.
    ///
    /// This is used for recovery replay and is intentionally synchronous
    /// because it runs inside `spawn_blocking`.
    pub fn iter_entries(&self) -> Result<Vec<JournalEntry>, StorageError> {
        let mut entries = Vec::new();
        for guard in self.partition.iter() {
            let (_key_bytes, value_bytes) =
                guard.into_inner().map_err(|e| StorageError::Database(e.to_string()))?;
            let entry: JournalEntry = bincode::deserialize(&value_bytes)
                .map_err(|e| StorageError::Serialization(e.to_string()))?;
            entries.push(entry);
        }
        Ok(entries)
    }

    /// Iterates over journal entries starting from (exclusive) a given sequence.
    pub fn iter_entries_after(
        &self,
        after_sequence: u64,
    ) -> Result<Vec<JournalEntry>, StorageError> {
        let mut entries = Vec::new();
        for guard in self.partition.iter() {
            let (key_bytes, value_bytes) =
                guard.into_inner().map_err(|e| StorageError::Database(e.to_string()))?;
            if key_bytes.len() == 8 {
                let seq = u64::from_be_bytes(key_bytes[..8].try_into().unwrap_or([0u8; 8]));
                if seq <= after_sequence {
                    continue;
                }
            }
            let entry: JournalEntry = bincode::deserialize(&value_bytes)
                .map_err(|e| StorageError::Serialization(e.to_string()))?;
            entries.push(entry);
        }
        Ok(entries)
    }

    /// Compacts the journal by removing entries with sequence ≤ `safe_sequence`.
    ///
    /// In single-node mode, `min_confirmed_sequence` is `None` and compaction
    /// removes all entries up to the current sequence. In future distributed
    /// mode, `min_confirmed_sequence` ensures that entries not yet confirmed
    /// by all replicas are preserved.
    ///
    /// # Returns
    ///
    /// The number of entries removed.
    pub fn compact(&self, min_confirmed_sequence: Option<u64>) -> Result<u64, StorageError> {
        let safe_seq = match min_confirmed_sequence {
            Some(confirmed) => confirmed,
            None => self.sequence.load(Ordering::SeqCst),
        };

        let mut removed = 0u64;
        for guard in self.partition.iter() {
            let (key_bytes, _) =
                guard.into_inner().map_err(|e| StorageError::Database(e.to_string()))?;
            if key_bytes.len() != 8 {
                continue;
            }
            let seq = u64::from_be_bytes(key_bytes[..8].try_into().unwrap_or([0u8; 8]));
            if seq > safe_seq {
                break;
            }
            self.partition
                .remove(key_bytes.to_vec())
                .map_err(|e| StorageError::Database(e.to_string()))?;
            removed += 1;
        }

        Ok(removed)
    }

    /// Returns true if the journal keyspace is empty.
    pub fn is_empty(&self) -> Result<bool, StorageError> {
        Ok(self.partition.is_empty().unwrap_or(true))
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_test_journal() -> (TempDir, MetadataJournal) {
        let temp_dir = TempDir::new().unwrap();
        let db = fjall::Database::builder(temp_dir.path().join("test_db")).open().unwrap();
        let ks = db.keyspace("journal", fjall::KeyspaceCreateOptions::default).unwrap();
        let journal = MetadataJournal::new(ks, PlacementGroupId::LOCAL).unwrap();
        (temp_dir, journal)
    }

    fn make_test_record() -> IndexRecord {
        IndexRecord::new(
            1,
            100,
            50,
            [0u8; 32],
            "etag".to_string(),
            "text/plain".to_string(),
        )
    }

    #[test]
    fn test_journal_creation_empty() {
        let (_dir, journal) = create_test_journal();
        assert_eq!(journal.current_sequence(), 0);
        assert!(journal.is_empty().unwrap());
    }

    #[test]
    fn test_journal_make_entry_op() {
        let (_dir, journal) = create_test_journal();

        let op = journal
            .make_journal_entry_op(JournalEventType::ObjectPut {
                bucket: "test-bucket".to_string(),
                key: "test-key".to_string(),
                version_id: None,
                record: make_test_record(),
            })
            .unwrap();

        assert_eq!(op.keyspace, super::super::KeyspaceId::Journal);
        assert_eq!(journal.current_sequence(), 1);
    }

    #[test]
    fn test_journal_monotonic_sequence() {
        let (_dir, journal) = create_test_journal();

        for i in 1..=5 {
            let _ = journal
                .make_journal_entry_op(JournalEventType::ObjectDelete {
                    bucket: "b".to_string(),
                    key: format!("k{}", i),
                    version_id: None,
                })
                .unwrap();
            assert_eq!(journal.current_sequence(), i);
        }
    }

    #[test]
    fn test_journal_iter_entries() {
        let temp_dir = TempDir::new().unwrap();
        let db = fjall::Database::builder(temp_dir.path().join("test_db")).open().unwrap();
        let ks = db.keyspace("journal", fjall::KeyspaceCreateOptions::default).unwrap();
        let journal = MetadataJournal::new(ks.clone(), PlacementGroupId::LOCAL).unwrap();

        // Write entries directly (simulating batch_write)
        for i in 0..3 {
            let op = journal
                .make_journal_entry_op(JournalEventType::ObjectPut {
                    bucket: "b".to_string(),
                    key: format!("k{}", i),
                    version_id: None,
                    record: make_test_record(),
                })
                .unwrap();

            // Simulate batch_write: insert directly
            if let super::super::BatchAction::Put(key, value) = op.action {
                ks.insert(&key, &value).unwrap();
            }
        }

        let entries = journal.iter_entries().unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].sequence, 1);
        assert_eq!(entries[1].sequence, 2);
        assert_eq!(entries[2].sequence, 3);
    }

    #[test]
    fn test_journal_iter_entries_after() {
        let temp_dir = TempDir::new().unwrap();
        let db = fjall::Database::builder(temp_dir.path().join("test_db")).open().unwrap();
        let ks = db.keyspace("journal", fjall::KeyspaceCreateOptions::default).unwrap();
        let journal = MetadataJournal::new(ks.clone(), PlacementGroupId::LOCAL).unwrap();

        for i in 0..5 {
            let op = journal
                .make_journal_entry_op(JournalEventType::ObjectPut {
                    bucket: "b".to_string(),
                    key: format!("k{}", i),
                    version_id: None,
                    record: make_test_record(),
                })
                .unwrap();
            if let super::super::BatchAction::Put(key, value) = op.action {
                ks.insert(&key, &value).unwrap();
            }
        }

        let entries = journal.iter_entries_after(3).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].sequence, 4);
        assert_eq!(entries[1].sequence, 5);
    }

    #[test]
    fn test_journal_compact() {
        let temp_dir = TempDir::new().unwrap();
        let db = fjall::Database::builder(temp_dir.path().join("test_db")).open().unwrap();
        let ks = db.keyspace("journal", fjall::KeyspaceCreateOptions::default).unwrap();
        let journal = MetadataJournal::new(ks.clone(), PlacementGroupId::LOCAL).unwrap();

        for i in 0..10 {
            let op = journal
                .make_journal_entry_op(JournalEventType::ObjectPut {
                    bucket: "b".to_string(),
                    key: format!("k{}", i),
                    version_id: None,
                    record: make_test_record(),
                })
                .unwrap();
            if let super::super::BatchAction::Put(key, value) = op.action {
                ks.insert(&key, &value).unwrap();
            }
        }

        assert_eq!(journal.entry_count().unwrap(), 10);

        // Compact entries up to sequence 5
        let removed = journal.compact(Some(5)).unwrap();
        assert_eq!(removed, 5);

        let remaining = journal.iter_entries().unwrap();
        assert_eq!(remaining.len(), 5);
        assert_eq!(remaining[0].sequence, 6);
    }

    #[test]
    fn test_journal_compact_with_replica_safety() {
        let temp_dir = TempDir::new().unwrap();
        let db = fjall::Database::builder(temp_dir.path().join("test_db")).open().unwrap();
        let ks = db.keyspace("journal", fjall::KeyspaceCreateOptions::default).unwrap();
        let journal = MetadataJournal::new(ks.clone(), PlacementGroupId::LOCAL).unwrap();

        for i in 0..10 {
            let op = journal
                .make_journal_entry_op(JournalEventType::ObjectDelete {
                    bucket: "b".to_string(),
                    key: format!("k{}", i),
                    version_id: None,
                })
                .unwrap();
            if let super::super::BatchAction::Put(key, value) = op.action {
                ks.insert(&key, &value).unwrap();
            }
        }

        // Compact with replica safety: only remove up to sequence 3
        let removed = journal.compact(Some(3)).unwrap();
        assert_eq!(removed, 3);
        assert_eq!(journal.iter_entries().unwrap().len(), 7);
    }

    #[test]
    fn test_journal_reopen_preserves_sequence() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db");

        // Write some entries
        {
            let db = fjall::Database::builder(&db_path).open().unwrap();
            let ks = db.keyspace("journal", fjall::KeyspaceCreateOptions::default).unwrap();
            let journal = MetadataJournal::new(ks.clone(), PlacementGroupId::LOCAL).unwrap();

            for _ in 0..5 {
                let op = journal
                    .make_journal_entry_op(JournalEventType::ObjectPut {
                        bucket: "b".to_string(),
                        key: "k".to_string(),
                        version_id: None,
                        record: make_test_record(),
                    })
                    .unwrap();
                if let super::super::BatchAction::Put(key, value) = op.action {
                    ks.insert(&key, &value).unwrap();
                }
            }
            assert_eq!(journal.current_sequence(), 5);
        }

        // Reopen and verify sequence continues
        {
            let db = fjall::Database::builder(&db_path).open().unwrap();
            let ks = db.keyspace("journal", fjall::KeyspaceCreateOptions::default).unwrap();
            let journal = MetadataJournal::new(ks, PlacementGroupId::LOCAL).unwrap();

            assert_eq!(journal.current_sequence(), 5);
        }
    }

    #[test]
    fn test_journal_delete_marker_event() {
        let (_dir, journal) = create_test_journal();

        let marker = IndexRecord::new_delete_marker("v1".to_string());
        let op = journal
            .make_journal_entry_op(JournalEventType::DeleteMarkerCreated {
                bucket: "b".to_string(),
                key: "k".to_string(),
                marker_version_id: "v1".to_string(),
                record: marker,
            })
            .unwrap();

        assert_eq!(op.keyspace, super::super::KeyspaceId::Journal);
        assert_eq!(journal.current_sequence(), 1);
    }

    #[test]
    fn test_journal_bucket_and_iam_events() {
        let (_dir, journal) = create_test_journal();

        let _ = journal
            .make_journal_entry_op(JournalEventType::BucketOperation {
                bucket: "my-bucket".to_string(),
                op: BucketOp::Created,
            })
            .unwrap();

        let _ = journal
            .make_journal_entry_op(JournalEventType::IamOperation {
                op: IamOp::UserCreated {
                    user_id: "admin".to_string(),
                },
            })
            .unwrap();

        assert_eq!(journal.current_sequence(), 2);
    }

    #[test]
    fn test_journal_metadata_update_event() {
        let (_dir, journal) = create_test_journal();

        let mut fields = HashMap::new();
        fields.insert("retention_mode".to_string(), "COMPLIANCE".to_string());
        fields.insert("legal_hold".to_string(), "true".to_string());

        let op = journal
            .make_journal_entry_op(JournalEventType::MetadataUpdate {
                bucket: "b".to_string(),
                key: "k".to_string(),
                version_id: Some("v1".to_string()),
                updated_fields: fields,
            })
            .unwrap();

        assert_eq!(op.keyspace, super::super::KeyspaceId::Journal);
        assert_eq!(journal.current_sequence(), 1);
    }

    // ================================================================
    // Phase 6: Placement group tests
    // ================================================================

    #[test]
    fn test_journal_placement_group_local() {
        let (_dir, journal) = create_test_journal();
        assert_eq!(journal.placement_group(), PlacementGroupId::LOCAL);
    }

    #[test]
    fn test_journal_entries_contain_placement_group() {
        let temp_dir = TempDir::new().unwrap();
        let db = fjall::Database::builder(temp_dir.path().join("test_db")).open().unwrap();
        let ks = db.keyspace("journal", fjall::KeyspaceCreateOptions::default).unwrap();
        let journal = MetadataJournal::new(ks.clone(), PlacementGroupId::LOCAL).unwrap();

        let op = journal
            .make_journal_entry_op(JournalEventType::ObjectPut {
                bucket: "b".to_string(),
                key: "k".to_string(),
                version_id: None,
                record: make_test_record(),
            })
            .unwrap();

        // Write and read back to verify PG is persisted
        if let super::super::BatchAction::Put(key, value) = op.action {
            ks.insert(&key, &value).unwrap();
        }

        let entries = journal.iter_entries().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].placement_group, PlacementGroupId::LOCAL);
        assert_eq!(entries[0].sequence, 1);
    }

    #[test]
    fn test_journal_entries_with_custom_pg() {
        let temp_dir = TempDir::new().unwrap();
        let db = fjall::Database::builder(temp_dir.path().join("test_db")).open().unwrap();
        let ks = db.keyspace("journal", fjall::KeyspaceCreateOptions::default).unwrap();
        let pg = PlacementGroupId(42);
        let journal = MetadataJournal::new(ks.clone(), pg).unwrap();

        let op = journal
            .make_journal_entry_op(JournalEventType::ObjectDelete {
                bucket: "b".to_string(),
                key: "k".to_string(),
                version_id: None,
            })
            .unwrap();

        if let super::super::BatchAction::Put(key, value) = op.action {
            ks.insert(&key, &value).unwrap();
        }

        let entries = journal.iter_entries().unwrap();
        assert_eq!(entries[0].placement_group, PlacementGroupId(42));
    }

    #[test]
    fn test_journal_backward_compat_missing_pg_defaults_to_local() {
        // Simulate a pre-Phase 6 journal entry (no placement_group field)
        // by manually constructing JSON without it.
        let json = r#"{
            "sequence": 1,
            "timestamp": 1000000,
            "event": {"ObjectDelete": {"bucket": "b", "key": "k", "version_id": null}}
        }"#;

        let entry: JournalEntry = serde_json::from_str(json).unwrap();
        assert_eq!(entry.placement_group, PlacementGroupId::LOCAL);
        assert_eq!(entry.sequence, 1);
    }
}
