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

//! Index database wrapper for metadata storage.
//!
//! # Internal Implementation Detail
//!
//! This module is an **internal implementation detail** of `BitcaskStorageEngine`.
//! It is NOT part of the stable public API. The backing store, key format, and
//! concurrency model may change between versions.
//!
//! # Current Implementation (fjall, Phase 3)
//!
//! Uses a fjall database directory with **separate keyspaces** for each entity type:
//!
//! | Keyspace   | Key format                        | Value format         |
//! |------------|-----------------------------------|----------------------|
//! | `objects`  | `{bucket}/{key}` or `...#version` | `bincode(IndexRecord)` |
//! | `versions` | `{bucket}/{key}`                  | `JSON(VersionList)`  |
//! | `buckets`  | `{bucket_name}`                   | `bincode(IndexRecord)` |
//! | `iam`      | `{type}_{id}`                     | `bincode(IndexRecord)` |
//! | `dedup`    | content hash (32 bytes)           | reserved for Phase 4 |
//! | `journal`  | sequence number                   | reserved for Phase 5 |
//!
//! ## Backward Compatibility
//!
//! The public `put`, `get`, `delete`, and `list` methods still accept the same
//! string keys used in Phase 2 (e.g. `__s4_versions_bucket/key`). An internal
//! [`route_key_id`](IndexDb::route_key_id) function transparently strips prefixes and
//! directs operations to the correct keyspace. This means callers in `bitcask.rs`
//! do **not** need changes for basic operations.
//!
//! ## Atomic Batch Writes
//!
//! The new [`batch_write`](IndexDb::batch_write) method accepts a list of
//! [`BatchOp`] operations spanning multiple keyspaces and commits them in a
//! single atomic fjall write batch. This eliminates all crash windows that
//! existed when multi-step versioning writes were performed as individual puts.
//!
//! ## Direct Keyspace Access
//!
//! For new code that no longer needs string-prefix routing (e.g. version list
//! storage), direct methods like [`put_version_list`](IndexDb::put_version_list)
//! and [`get_version_list`](IndexDb::get_version_list) are provided.
//!
//! # Concurrency Model
//!
//! fjall uses MVCC internally, providing lock-free reads and thread-safe writes
//! without requiring an external mutex. All operations are dispatched to a
//! blocking thread pool via `tokio::task::spawn_blocking` since fjall is a
//! synchronous library.

use crate::error::StorageError;
use crate::storage::engine::KeyspaceSnapshot;
use crate::storage::VersionList;
use crate::types::IndexRecord;
use fjall::{Database, Keyspace, KeyspaceCreateOptions, PersistMode};
use std::path::Path;
use tokio::task;

// ============================================================================
// Batch Write Types
// ============================================================================

/// Identifies a target keyspace for batch operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyspaceId {
    /// Main object records (`{bucket}/{key}` and `{bucket}/{key}#{version_id}`).
    Objects,
    /// Version lists (`{bucket}/{key}` → JSON `VersionList`).
    Versions,
    /// Bucket markers (`{bucket_name}` → `IndexRecord`).
    Buckets,
    /// IAM records (`{type}_{id}` → `IndexRecord`).
    Iam,
    /// Deduplication entries: content hash (32 bytes) → `DedupEntry`.
    Dedup,
    /// Metadata journal entries (reserved for Phase 5).
    Journal,
    /// Multipart upload sessions: `upload_id` → `MultipartUploadSession`.
    MultipartSessions,
    /// Multipart part records: `{upload_id}_{part_number:05}` → `MultipartPartRecord`.
    MultipartParts,
    /// Generalized blob references: `BlobId` (32 bytes) → `BlobRefEntry`.
    BlobRefs,
}

/// An individual action inside a [`BatchOp`].
#[derive(Debug)]
pub enum BatchAction {
    /// Insert or overwrite a key-value pair.
    Put(Vec<u8>, Vec<u8>),
    /// Delete a key.
    Delete(Vec<u8>),
}

/// A single operation in an atomic batch, targeting a specific keyspace.
#[derive(Debug)]
pub struct BatchOp {
    /// Which keyspace this operation targets.
    pub keyspace: KeyspaceId,
    /// The action to perform (put or delete).
    pub action: BatchAction,
}

// ============================================================================
// IndexDb
// ============================================================================

/// Wrapper around fjall for storing the metadata index.
///
/// This is an **internal implementation detail** of [`super::BitcaskStorageEngine`].
/// External consumers should use the [`super::StorageEngine`] trait instead.
///
/// # Thread Safety
///
/// fjall is internally thread-safe (MVCC, lock-free reads). No external mutex
/// is required. The `Database` and `Keyspace` handles are `Clone + Send + Sync`.
///
/// # Atomicity
///
/// Individual `put`, `get`, `delete`, and `list` calls are atomic within their
/// keyspace. For multi-key atomic writes across keyspaces, use [`batch_write`](Self::batch_write).
///
/// # Durability
///
/// Every write is followed by `db.persist(PersistMode::SyncAll)` to guarantee
/// that data is durable on disk before returning. This matches the fsync-on-commit
/// behavior of the previous redb backend.
pub struct IndexDb {
    db: Database,
    /// Main object records: `{bucket}/{key}` or `{bucket}/{key}#{version_id}`.
    objects: Keyspace,
    /// Version lists: `{bucket}/{key}` → JSON `VersionList`.
    versions: Keyspace,
    /// Bucket markers: `{bucket_name}` → `IndexRecord`.
    buckets: Keyspace,
    /// IAM records: `{type}_{id}` → `IndexRecord`.
    iam: Keyspace,
    /// Deduplication entries: content hash → `DedupEntry` (Phase 4).
    dedup: Keyspace,
    /// Metadata journal entries (reserved for Phase 5).
    journal: Keyspace,
    /// Multipart upload sessions: `upload_id` → `MultipartUploadSession`.
    multipart_sessions: Keyspace,
    /// Multipart part records: `{upload_id}_{part_number:05}` → `MultipartPartRecord`.
    multipart_parts: Keyspace,
    /// Generalized blob references: `BlobId` (32 bytes) → `BlobRefEntry`.
    blob_refs: Keyspace,
}

// ============================================================================
// Public keyspace accessors
// ============================================================================

impl IndexDb {
    /// Returns a clone of the `dedup` keyspace handle.
    ///
    /// Used by [`Deduplicator`](super::Deduplicator) to store and retrieve
    /// content hash → `DedupEntry` mappings (Phase 4).
    pub fn dedup_keyspace(&self) -> Keyspace {
        self.dedup.clone()
    }

    /// Returns a clone of the `journal` keyspace handle.
    ///
    /// Used by [`MetadataJournal`](super::MetadataJournal) to store ordered
    /// mutation entries (Phase 5).
    pub fn journal_keyspace(&self) -> Keyspace {
        self.journal.clone()
    }

    /// Returns a clone of the `multipart_sessions` keyspace handle.
    pub fn multipart_sessions_keyspace(&self) -> Keyspace {
        self.multipart_sessions.clone()
    }

    /// Returns a clone of the `multipart_parts` keyspace handle.
    pub fn multipart_parts_keyspace(&self) -> Keyspace {
        self.multipart_parts.clone()
    }

    /// Returns a clone of the `blob_refs` keyspace handle.
    pub fn blob_refs_keyspace(&self) -> Keyspace {
        self.blob_refs.clone()
    }

    /// Returns `true` if the objects keyspace contains no entries.
    ///
    /// Used by automatic crash recovery to detect a freshly created (empty)
    /// metadata database that may need to be rebuilt from volume files.
    pub fn is_objects_empty(&self) -> Result<bool, StorageError> {
        self.objects.is_empty().map_err(|e| StorageError::Database(e.to_string()))
    }
}

impl IndexDb {
    /// Creates or opens an index database with all keyspaces.
    ///
    /// On first open this creates six keyspaces. On subsequent opens the
    /// existing keyspaces are reused. A legacy `"index"` keyspace from Phase 2,
    /// if present, is **not** deleted — it simply becomes unused.
    ///
    /// # Arguments
    ///
    /// * `db_path` - Path to the fjall database directory
    pub fn new(db_path: &Path) -> Result<Self, StorageError> {
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent).map_err(StorageError::Io)?;
        }

        let db = Database::builder(db_path)
            .open()
            .map_err(|e| StorageError::Database(e.to_string()))?;

        let open_ks = |name: &str| -> Result<Keyspace, StorageError> {
            db.keyspace(name, KeyspaceCreateOptions::default)
                .map_err(|e| StorageError::Database(e.to_string()))
        };

        let objects = open_ks("objects")?;
        let versions = open_ks("versions")?;
        let buckets = open_ks("buckets")?;
        let iam = open_ks("iam")?;
        let dedup = open_ks("dedup")?;
        let journal = open_ks("journal")?;
        let multipart_sessions = open_ks("multipart_sessions")?;
        let multipart_parts = open_ks("multipart_parts")?;
        let blob_refs = open_ks("blob_refs")?;

        // Migrate data from legacy "index" keyspace if it exists and the new
        // keyspaces are empty. This provides seamless Phase 2 → Phase 3 upgrade.
        if db.keyspace_exists("index") {
            let objects_empty = objects.is_empty().unwrap_or(true);
            if objects_empty {
                let legacy = open_ks("index")?;
                Self::migrate_from_legacy(&db, &legacy, &objects, &versions, &buckets, &iam)?;
            }
        }

        Ok(Self {
            db,
            objects,
            versions,
            buckets,
            iam,
            dedup,
            journal,
            multipart_sessions,
            multipart_parts,
            blob_refs,
        })
    }

    /// Migrates all data from the legacy single "index" keyspace into the new
    /// separate keyspaces using an atomic batch.
    fn migrate_from_legacy(
        db: &Database,
        legacy: &Keyspace,
        objects: &Keyspace,
        versions: &Keyspace,
        buckets: &Keyspace,
        iam: &Keyspace,
    ) -> Result<(), StorageError> {
        let mut batch = db.batch();

        for guard in legacy.iter() {
            let (key_bytes, value_bytes) =
                guard.into_inner().map_err(|e| StorageError::Database(e.to_string()))?;

            let key_str = std::str::from_utf8(&key_bytes)
                .map_err(|e| StorageError::Database(e.to_string()))?;

            // Route to the correct new keyspace, stripping the prefix.
            if let Some(rest) = key_str.strip_prefix("__s4_versions_") {
                batch.insert(versions, rest.as_bytes(), &*value_bytes);
            } else if let Some(rest) = key_str.strip_prefix("__system__/__s4_bucket_marker_") {
                batch.insert(buckets, rest.as_bytes(), &*value_bytes);
            } else if let Some(rest) = key_str.strip_prefix("__system__/__s4_iam_") {
                batch.insert(iam, rest.as_bytes(), &*value_bytes);
            } else {
                batch.insert(objects, &*key_bytes, &*value_bytes);
            }
        }

        batch.commit().map_err(|e| StorageError::Database(e.to_string()))?;
        db.persist(PersistMode::SyncAll)
            .map_err(|e| StorageError::Database(e.to_string()))?;

        Ok(())
    }

    // ========================================================================
    // Key routing (backward compatibility)
    // ========================================================================

    /// Routes a legacy string key to the correct (keyspace, stripped_key) pair.
    ///
    /// This allows existing callers that still use prefixed string keys to
    /// transparently hit the right keyspace without code changes.
    fn route_key<'a>(&self, key: &'a str) -> (&Keyspace, &'a str) {
        if let Some(rest) = key.strip_prefix("__s4_versions_") {
            (&self.versions, rest)
        } else if let Some(rest) = key.strip_prefix("__system__/__s4_bucket_marker_") {
            (&self.buckets, rest)
        } else if let Some(rest) = key.strip_prefix("__system__/__s4_iam_") {
            (&self.iam, rest)
        } else {
            (&self.objects, key)
        }
    }

    /// Routes a key string to the correct [`KeyspaceId`] and stripped key.
    ///
    /// This is the public counterpart of the private `route_key` method that
    /// returns a [`KeyspaceId`] instead of a keyspace handle, making it suitable
    /// for constructing [`BatchOp`] entries.
    pub fn route_key_id<'a>(&self, key: &'a str) -> (KeyspaceId, &'a str) {
        if let Some(rest) = key.strip_prefix("__s4_versions_") {
            (KeyspaceId::Versions, rest)
        } else if let Some(rest) = key.strip_prefix("__system__/__s4_bucket_marker_") {
            (KeyspaceId::Buckets, rest)
        } else if let Some(rest) = key.strip_prefix("__system__/__s4_iam_") {
            (KeyspaceId::Iam, rest)
        } else {
            (KeyspaceId::Objects, key)
        }
    }

    /// Returns the keyspace handle for the given [`KeyspaceId`].
    #[allow(dead_code)]
    fn keyspace_for(&self, id: KeyspaceId) -> &Keyspace {
        match id {
            KeyspaceId::Objects => &self.objects,
            KeyspaceId::Versions => &self.versions,
            KeyspaceId::Buckets => &self.buckets,
            KeyspaceId::Iam => &self.iam,
            KeyspaceId::Dedup => &self.dedup,
            KeyspaceId::Journal => &self.journal,
            KeyspaceId::MultipartSessions => &self.multipart_sessions,
            KeyspaceId::MultipartParts => &self.multipart_parts,
            KeyspaceId::BlobRefs => &self.blob_refs,
        }
    }

    // ========================================================================
    // Legacy-compatible single-key operations
    // ========================================================================

    /// Stores an index record, routing to the correct keyspace by key prefix.
    ///
    /// # Arguments
    ///
    /// * `key` - Object key (may contain legacy prefixes like `__s4_versions_`)
    /// * `record` - Index record to store
    pub async fn put(&self, key: &str, record: &IndexRecord) -> Result<(), StorageError> {
        let (ks, stripped) = self.route_key(key);
        let db = self.db.clone();
        let ks = ks.clone();
        let stripped = stripped.to_string();
        let record_bytes =
            bincode::serialize(record).map_err(|e| StorageError::Serialization(e.to_string()))?;

        task::spawn_blocking(move || {
            ks.insert(&stripped, &record_bytes[..])
                .map_err(|e| StorageError::Database(e.to_string()))?;
            db.persist(PersistMode::SyncAll)
                .map_err(|e| StorageError::Database(e.to_string()))?;
            Ok::<(), StorageError>(())
        })
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?
    }

    /// Retrieves an index record, routing to the correct keyspace by key prefix.
    pub async fn get(&self, key: &str) -> Result<Option<IndexRecord>, StorageError> {
        let (ks, stripped) = self.route_key(key);
        let ks = ks.clone();
        let stripped = stripped.to_string();

        task::spawn_blocking(move || match ks.get(&stripped) {
            Ok(Some(value)) => {
                let record: IndexRecord = bincode::deserialize(&value)
                    .map_err(|e| StorageError::Serialization(e.to_string()))?;
                Ok(Some(record))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(StorageError::Database(e.to_string())),
        })
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?
    }

    /// Deletes an index record, routing to the correct keyspace by key prefix.
    pub async fn delete(&self, key: &str) -> Result<(), StorageError> {
        let (ks, stripped) = self.route_key(key);
        let db = self.db.clone();
        let ks = ks.clone();
        let stripped = stripped.to_string();

        task::spawn_blocking(move || {
            ks.remove(&stripped).map_err(|e| StorageError::Database(e.to_string()))?;
            db.persist(PersistMode::SyncAll)
                .map_err(|e| StorageError::Database(e.to_string()))?;
            Ok::<(), StorageError>(())
        })
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?
    }

    /// Lists records matching a prefix, routing to the correct keyspace.
    ///
    /// Uses fjall's native prefix scan for efficient O(log n) seek.
    pub async fn list(
        &self,
        prefix: &str,
        max_keys: usize,
    ) -> Result<Vec<(String, IndexRecord)>, StorageError> {
        self.list_after(prefix, None, max_keys).await
    }

    /// Lists entries matching the given prefix, starting after `start_after` key.
    ///
    /// If `start_after` is `Some`, only keys lexicographically greater than
    /// `start_after` are returned. The `start_after` key itself is excluded.
    pub async fn list_after(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        max_keys: usize,
    ) -> Result<Vec<(String, IndexRecord)>, StorageError> {
        let (ks, stripped_prefix) = self.route_key(prefix);

        // We need the original prefix to reconstruct full keys in the result
        // so that callers see the same key format they passed in.
        let key_prefix_to_restore = &prefix[..prefix.len() - stripped_prefix.len()];

        let ks = ks.clone();
        let stripped_prefix = stripped_prefix.to_string();
        let restore_prefix = key_prefix_to_restore.to_string();
        let start_after_owned = start_after.map(|s| {
            // Strip the same prefix that route_key strips, so we compare
            // against the same key space used in the iterator.
            s.strip_prefix(&restore_prefix).unwrap_or(s).to_string()
        });

        task::spawn_blocking(move || {
            let mut results = Vec::new();

            let iter: Box<dyn Iterator<Item = fjall::Guard>> = if stripped_prefix.is_empty() {
                Box::new(ks.iter())
            } else {
                Box::new(ks.prefix(&stripped_prefix))
            };

            for guard in iter {
                let (key_bytes, value_bytes) =
                    guard.into_inner().map_err(|e| StorageError::Database(e.to_string()))?;

                let key_str = std::str::from_utf8(&key_bytes)
                    .map_err(|e| StorageError::Database(e.to_string()))?;

                // Skip keys up to and including start_after
                if let Some(ref sa) = start_after_owned {
                    if key_str <= sa.as_str() {
                        continue;
                    }
                }

                // Restore the full key with original prefix so callers see
                // the same format they used to store/query.
                let full_key = format!("{}{}", restore_prefix, key_str);

                let record: IndexRecord = bincode::deserialize(&value_bytes)
                    .map_err(|e| StorageError::Serialization(e.to_string()))?;

                results.push((full_key, record));

                if results.len() >= max_keys {
                    break;
                }
            }

            Ok(results)
        })
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?
    }

    // ========================================================================
    // Atomic batch writes (Phase 3)
    // ========================================================================

    /// Atomically writes multiple operations across different keyspaces.
    ///
    /// All operations in the batch are committed as a single atomic unit.
    /// If any operation fails, the entire batch is rolled back. This eliminates
    /// the crash windows that existed when multi-step versioning writes were
    /// performed as individual puts.
    ///
    /// # Arguments
    ///
    /// * `operations` - A list of [`BatchOp`] operations to execute atomically.
    ///
    /// # Durability
    ///
    /// The batch is committed with `PersistMode::SyncAll` to guarantee that all
    /// data is durable on disk before this method returns.
    pub async fn batch_write(&self, operations: Vec<BatchOp>) -> Result<(), StorageError> {
        if operations.is_empty() {
            return Ok(());
        }

        let db = self.db.clone();
        let objects = self.objects.clone();
        let versions = self.versions.clone();
        let buckets = self.buckets.clone();
        let iam = self.iam.clone();
        let dedup = self.dedup.clone();
        let journal = self.journal.clone();
        let multipart_sessions = self.multipart_sessions.clone();
        let multipart_parts = self.multipart_parts.clone();
        let blob_refs = self.blob_refs.clone();

        task::spawn_blocking(move || {
            let mut batch = db.batch();

            let ks_for = |id: KeyspaceId| -> &Keyspace {
                match id {
                    KeyspaceId::Objects => &objects,
                    KeyspaceId::Versions => &versions,
                    KeyspaceId::Buckets => &buckets,
                    KeyspaceId::Iam => &iam,
                    KeyspaceId::Dedup => &dedup,
                    KeyspaceId::Journal => &journal,
                    KeyspaceId::MultipartSessions => &multipart_sessions,
                    KeyspaceId::MultipartParts => &multipart_parts,
                    KeyspaceId::BlobRefs => &blob_refs,
                }
            };

            for op in &operations {
                let ks = ks_for(op.keyspace);
                match &op.action {
                    BatchAction::Put(key, value) => {
                        batch.insert(ks, &key[..], &value[..]);
                    }
                    BatchAction::Delete(key) => {
                        batch.remove(ks, &key[..]);
                    }
                }
            }

            batch.commit().map_err(|e| StorageError::Database(e.to_string()))?;
            db.persist(PersistMode::SyncAll)
                .map_err(|e| StorageError::Database(e.to_string()))?;

            Ok::<(), StorageError>(())
        })
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?
    }

    // ========================================================================
    // Direct keyspace access (new Phase 3 API)
    // ========================================================================

    /// Stores a version list directly in the `versions` keyspace.
    ///
    /// The version list is serialized as JSON. If the list is empty, the
    /// key is deleted instead.
    pub async fn put_version_list(
        &self,
        bucket: &str,
        key: &str,
        list: &VersionList,
    ) -> Result<(), StorageError> {
        let db = self.db.clone();
        let versions = self.versions.clone();
        let ks_key = format!("{}/{}", bucket, key);
        let json = serde_json::to_vec(list)
            .map_err(|e| StorageError::Serialization(format!("JSON serialize error: {}", e)))?;
        let is_empty = list.is_empty();

        task::spawn_blocking(move || {
            if is_empty {
                versions.remove(&ks_key).map_err(|e| StorageError::Database(e.to_string()))?;
            } else {
                versions
                    .insert(&ks_key, &json[..])
                    .map_err(|e| StorageError::Database(e.to_string()))?;
            }
            db.persist(PersistMode::SyncAll)
                .map_err(|e| StorageError::Database(e.to_string()))?;
            Ok::<(), StorageError>(())
        })
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?
    }

    /// Retrieves a version list directly from the `versions` keyspace.
    ///
    /// Returns an empty [`VersionList`] if no entry exists.
    pub async fn get_version_list(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<VersionList, StorageError> {
        let versions = self.versions.clone();
        let ks_key = format!("{}/{}", bucket, key);

        task::spawn_blocking(move || match versions.get(&ks_key) {
            Ok(Some(value)) => serde_json::from_slice(&value)
                .map_err(|e| StorageError::Serialization(format!("JSON parse error: {}", e))),
            Ok(None) => Ok(VersionList::new()),
            Err(e) => Err(StorageError::Database(e.to_string())),
        })
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?
    }

    /// Lists all version list entries matching a prefix from the `versions` keyspace.
    ///
    /// Returns `(bucket/key, VersionList)` pairs.
    pub async fn list_version_lists(
        &self,
        prefix: &str,
        max_keys: usize,
    ) -> Result<Vec<(String, VersionList)>, StorageError> {
        let versions = self.versions.clone();
        let prefix = prefix.to_string();

        task::spawn_blocking(move || {
            let mut results = Vec::new();

            let iter: Box<dyn Iterator<Item = fjall::Guard>> = if prefix.is_empty() {
                Box::new(versions.iter())
            } else {
                Box::new(versions.prefix(&prefix))
            };

            for guard in iter {
                let (key_bytes, value_bytes) =
                    guard.into_inner().map_err(|e| StorageError::Database(e.to_string()))?;

                let key_str = std::str::from_utf8(&key_bytes)
                    .map_err(|e| StorageError::Database(e.to_string()))?
                    .to_string();

                let vl: VersionList = serde_json::from_slice(&value_bytes)
                    .map_err(|e| StorageError::Serialization(format!("JSON parse error: {}", e)))?;

                results.push((key_str, vl));

                if results.len() >= max_keys {
                    break;
                }
            }

            Ok(results)
        })
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?
    }

    /// Lists records from the `objects` keyspace with a given prefix.
    ///
    /// Unlike [`list`](Self::list), this method does **not** perform key routing
    /// and always reads from the `objects` keyspace directly.
    pub async fn list_objects(
        &self,
        prefix: &str,
        max_keys: usize,
    ) -> Result<Vec<(String, IndexRecord)>, StorageError> {
        let objects = self.objects.clone();
        let prefix = prefix.to_string();

        task::spawn_blocking(move || {
            let mut results = Vec::new();

            let iter: Box<dyn Iterator<Item = fjall::Guard>> = if prefix.is_empty() {
                Box::new(objects.iter())
            } else {
                Box::new(objects.prefix(&prefix))
            };

            for guard in iter {
                let (key_bytes, value_bytes) =
                    guard.into_inner().map_err(|e| StorageError::Database(e.to_string()))?;

                let key_str = std::str::from_utf8(&key_bytes)
                    .map_err(|e| StorageError::Database(e.to_string()))?
                    .to_string();

                let record: IndexRecord = bincode::deserialize(&value_bytes)
                    .map_err(|e| StorageError::Serialization(e.to_string()))?;

                results.push((key_str, record));

                if results.len() >= max_keys {
                    break;
                }
            }

            Ok(results)
        })
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?
    }

    // ========================================================================
    // Snapshot / restore (Phase 6 — StateMachine support)
    // ========================================================================

    /// Dumps all data keyspaces as a list of [`KeyspaceSnapshot`] entries.
    ///
    /// The journal keyspace is NOT included — it is handled separately
    /// by the journal's own replay mechanism.
    ///
    /// This is an expensive operation that scans all keyspaces. It should
    /// only be called during snapshot creation for replica bootstrap.
    pub async fn dump_keyspaces(&self) -> Result<Vec<KeyspaceSnapshot>, StorageError> {
        let objects = self.objects.clone();
        let versions = self.versions.clone();
        let buckets = self.buckets.clone();
        let iam = self.iam.clone();
        let dedup = self.dedup.clone();
        let multipart_sessions = self.multipart_sessions.clone();
        let multipart_parts = self.multipart_parts.clone();
        let blob_refs = self.blob_refs.clone();

        task::spawn_blocking(move || {
            let mut snapshots = Vec::new();

            let dump_ks = |name: &str, ks: &Keyspace| -> Result<KeyspaceSnapshot, StorageError> {
                let mut entries = Vec::new();
                for guard in ks.iter() {
                    let (k, v) =
                        guard.into_inner().map_err(|e| StorageError::Database(e.to_string()))?;
                    entries.push((k.to_vec(), v.to_vec()));
                }
                Ok(KeyspaceSnapshot {
                    name: name.to_string(),
                    entries,
                })
            };

            snapshots.push(dump_ks("objects", &objects)?);
            snapshots.push(dump_ks("versions", &versions)?);
            snapshots.push(dump_ks("buckets", &buckets)?);
            snapshots.push(dump_ks("iam", &iam)?);
            snapshots.push(dump_ks("dedup", &dedup)?);
            snapshots.push(dump_ks("multipart_sessions", &multipart_sessions)?);
            snapshots.push(dump_ks("multipart_parts", &multipart_parts)?);
            snapshots.push(dump_ks("blob_refs", &blob_refs)?);

            Ok(snapshots)
        })
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?
    }

    /// Restores keyspaces from a list of [`KeyspaceSnapshot`] entries.
    ///
    /// This clears the target keyspaces and writes the snapshot data
    /// atomically. Used during replica bootstrap or catch-up.
    ///
    /// # Warning
    ///
    /// This replaces ALL data in the target keyspaces. It should only
    /// be called during controlled restore operations.
    pub async fn restore_keyspaces(
        &self,
        snapshots: Vec<KeyspaceSnapshot>,
    ) -> Result<(), StorageError> {
        let db = self.db.clone();
        let objects = self.objects.clone();
        let versions = self.versions.clone();
        let buckets = self.buckets.clone();
        let iam = self.iam.clone();
        let dedup = self.dedup.clone();
        let multipart_sessions = self.multipart_sessions.clone();
        let multipart_parts = self.multipart_parts.clone();
        let blob_refs = self.blob_refs.clone();

        task::spawn_blocking(move || {
            let resolve_ks = |name: &str| -> Option<&Keyspace> {
                match name {
                    "objects" => Some(&objects),
                    "versions" => Some(&versions),
                    "buckets" => Some(&buckets),
                    "iam" => Some(&iam),
                    "dedup" => Some(&dedup),
                    "multipart_sessions" => Some(&multipart_sessions),
                    "multipart_parts" => Some(&multipart_parts),
                    "blob_refs" => Some(&blob_refs),
                    _ => None,
                }
            };

            // Clear target keyspaces first
            for snap in &snapshots {
                if let Some(ks) = resolve_ks(&snap.name) {
                    let keys: Vec<Vec<u8>> = ks
                        .iter()
                        .filter_map(|guard| guard.into_inner().ok().map(|(k, _)| k.to_vec()))
                        .collect();
                    for key in keys {
                        ks.remove(&key[..]).map_err(|e| StorageError::Database(e.to_string()))?;
                    }
                }
            }

            // Write snapshot data in a batch
            let mut batch = db.batch();
            for snap in &snapshots {
                if let Some(ks) = resolve_ks(&snap.name) {
                    for (key, value) in &snap.entries {
                        batch.insert(ks, &key[..], &value[..]);
                    }
                }
            }

            batch.commit().map_err(|e| StorageError::Database(e.to_string()))?;
            db.persist(PersistMode::SyncAll)
                .map_err(|e| StorageError::Database(e.to_string()))?;

            Ok::<(), StorageError>(())
        })
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?
    }
    // ========================================================================
    // Compaction support
    // ========================================================================

    /// Scans the objects keyspace and returns all records stored in the given volume.
    ///
    /// Returns `(object_key, IndexRecord)` pairs where `record.file_id == volume_id`.
    /// Inline objects (`file_id == u32::MAX`) are always excluded.
    ///
    /// Used by the compactor to find which index records need updating after
    /// relocating blobs from one volume to another.
    ///
    /// # Performance
    ///
    /// Full objects keyspace scan. Call from a blocking thread for large datasets.
    pub async fn scan_objects_by_volume(
        &self,
        volume_id: u32,
    ) -> Result<Vec<(String, IndexRecord)>, StorageError> {
        let objects = self.objects.clone();

        task::spawn_blocking(move || {
            let mut results = Vec::new();

            for guard in objects.iter() {
                let (key_bytes, value_bytes) =
                    guard.into_inner().map_err(|e| StorageError::Database(e.to_string()))?;

                let record: IndexRecord = bincode::deserialize(&value_bytes)
                    .map_err(|e| StorageError::Serialization(e.to_string()))?;

                // Skip inline objects and objects in other volumes
                if record.file_id == u32::MAX || record.file_id != volume_id {
                    continue;
                }

                let key_str = std::str::from_utf8(&key_bytes)
                    .map_err(|e| StorageError::Database(e.to_string()))?
                    .to_string();

                results.push((key_str, record));
            }

            Ok(results)
        })
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_index_db_put_and_get() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db");

        let index_db = IndexDb::new(&db_path).unwrap();

        let record = crate::types::IndexRecord::new(
            1,
            100,
            50,
            [0u8; 32],
            "etag".to_string(),
            "text/plain".to_string(),
        );

        index_db.put("bucket/key", &record).await.unwrap();

        let retrieved = index_db.get("bucket/key").await.unwrap();
        assert!(retrieved.is_some());
        let retrieved = retrieved.unwrap();
        assert_eq!(retrieved.file_id, 1);
        assert_eq!(retrieved.offset, 100);
        assert_eq!(retrieved.size, 50);
    }

    #[tokio::test]
    async fn test_index_db_delete() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db");

        let index_db = IndexDb::new(&db_path).unwrap();

        let record = crate::types::IndexRecord::new(
            1,
            100,
            50,
            [0u8; 32],
            "etag".to_string(),
            "text/plain".to_string(),
        );

        index_db.put("bucket/key", &record).await.unwrap();
        assert!(index_db.get("bucket/key").await.unwrap().is_some());

        index_db.delete("bucket/key").await.unwrap();
        assert!(index_db.get("bucket/key").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_index_db_list() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db");

        let index_db = IndexDb::new(&db_path).unwrap();

        let record1 = crate::types::IndexRecord::new(
            1,
            100,
            50,
            [0u8; 32],
            "etag1".to_string(),
            "text/plain".to_string(),
        );
        let record2 = crate::types::IndexRecord::new(
            1,
            200,
            50,
            [1u8; 32],
            "etag2".to_string(),
            "text/plain".to_string(),
        );

        index_db.put("bucket/key1", &record1).await.unwrap();
        index_db.put("bucket/key2", &record2).await.unwrap();
        index_db.put("other/key3", &record1).await.unwrap();

        let results = index_db.list("bucket/", 10).await.unwrap();
        assert_eq!(results.len(), 2);
        assert!(results.iter().any(|(k, _)| k == "bucket/key1"));
        assert!(results.iter().any(|(k, _)| k == "bucket/key2"));
    }

    #[tokio::test]
    async fn test_index_db_reopen() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db");

        // Write data
        {
            let index_db = IndexDb::new(&db_path).unwrap();
            let record = crate::types::IndexRecord::new(
                1,
                100,
                50,
                [0u8; 32],
                "etag".to_string(),
                "text/plain".to_string(),
            );
            index_db.put("bucket/key", &record).await.unwrap();
        }

        // Reopen and verify data persisted
        {
            let index_db = IndexDb::new(&db_path).unwrap();
            let retrieved = index_db.get("bucket/key").await.unwrap();
            assert!(retrieved.is_some());
            let retrieved = retrieved.unwrap();
            assert_eq!(retrieved.file_id, 1);
            assert_eq!(retrieved.offset, 100);
        }
    }

    #[tokio::test]
    async fn test_index_db_prefix_scan_efficiency() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db");

        let index_db = IndexDb::new(&db_path).unwrap();

        let record = crate::types::IndexRecord::new(
            1,
            0,
            10,
            [0u8; 32],
            "etag".to_string(),
            "text/plain".to_string(),
        );

        for i in 0..100 {
            let key = format!("bucket-a/key{:03}", i);
            index_db.put(&key, &record).await.unwrap();
        }
        for i in 0..50 {
            let key = format!("bucket-b/key{:03}", i);
            index_db.put(&key, &record).await.unwrap();
        }

        let results_a = index_db.list("bucket-a/", usize::MAX).await.unwrap();
        assert_eq!(results_a.len(), 100);

        let results_b = index_db.list("bucket-b/", usize::MAX).await.unwrap();
        assert_eq!(results_b.len(), 50);

        let limited = index_db.list("bucket-a/", 10).await.unwrap();
        assert_eq!(limited.len(), 10);

        // Full scan (empty prefix) returns all records from objects keyspace
        let all = index_db.list("", usize::MAX).await.unwrap();
        assert_eq!(all.len(), 150);
    }

    // ====================================================================
    // Phase 3 specific tests
    // ====================================================================

    #[tokio::test]
    async fn test_keyspace_routing_versions() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db");
        let index_db = IndexDb::new(&db_path).unwrap();

        let record = crate::types::IndexRecord::new(
            u32::MAX,
            0,
            10,
            [0u8; 32],
            "".to_string(),
            "application/json".to_string(),
        );

        // Write through legacy prefixed key
        index_db.put("__s4_versions_mybucket/mykey", &record).await.unwrap();

        // Read back through legacy key
        let retrieved = index_db.get("__s4_versions_mybucket/mykey").await.unwrap();
        assert!(retrieved.is_some());

        // List with legacy prefix
        let results = index_db.list("__s4_versions_mybucket/", 100).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, "__s4_versions_mybucket/mykey");
    }

    #[tokio::test]
    async fn test_keyspace_routing_buckets() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db");
        let index_db = IndexDb::new(&db_path).unwrap();

        let record =
            crate::types::IndexRecord::new(0, 0, 0, [0u8; 32], "".to_string(), "".to_string());

        index_db.put("__system__/__s4_bucket_marker_testbucket", &record).await.unwrap();

        let retrieved = index_db.get("__system__/__s4_bucket_marker_testbucket").await.unwrap();
        assert!(retrieved.is_some());

        let results = index_db.list("__system__/__s4_bucket_marker_", 100).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, "__system__/__s4_bucket_marker_testbucket");
    }

    #[tokio::test]
    async fn test_keyspace_routing_iam() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db");
        let index_db = IndexDb::new(&db_path).unwrap();

        let record =
            crate::types::IndexRecord::new(0, 0, 0, [0u8; 32], "".to_string(), "".to_string());

        index_db.put("__system__/__s4_iam_user_admin", &record).await.unwrap();

        let retrieved = index_db.get("__system__/__s4_iam_user_admin").await.unwrap();
        assert!(retrieved.is_some());

        let results = index_db.list("__system__/__s4_iam_", 100).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, "__system__/__s4_iam_user_admin");
    }

    #[tokio::test]
    async fn test_batch_write_atomic() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db");
        let index_db = IndexDb::new(&db_path).unwrap();

        let record = crate::types::IndexRecord::new(
            1,
            100,
            50,
            [0u8; 32],
            "etag".to_string(),
            "text/plain".to_string(),
        );
        let record_bytes = bincode::serialize(&record).unwrap();

        let version_list = VersionList::new();
        let vl_bytes = serde_json::to_vec(&version_list).unwrap();

        // Write to two keyspaces atomically
        let ops = vec![
            BatchOp {
                keyspace: KeyspaceId::Objects,
                action: BatchAction::Put(b"bucket/key".to_vec(), record_bytes),
            },
            BatchOp {
                keyspace: KeyspaceId::Versions,
                action: BatchAction::Put(b"bucket/key".to_vec(), vl_bytes),
            },
        ];

        index_db.batch_write(ops).await.unwrap();

        // Both should be present
        let obj = index_db.get("bucket/key").await.unwrap();
        assert!(obj.is_some());

        let vl = index_db.get_version_list("bucket", "key").await.unwrap();
        assert!(vl.is_empty()); // Empty version list was stored
    }

    #[tokio::test]
    async fn test_batch_write_with_deletes() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db");
        let index_db = IndexDb::new(&db_path).unwrap();

        let record = crate::types::IndexRecord::new(
            1,
            100,
            50,
            [0u8; 32],
            "etag".to_string(),
            "text/plain".to_string(),
        );

        // Pre-populate
        index_db.put("bucket/key", &record).await.unwrap();

        // Batch: delete from objects, insert into versions
        let mut vl = VersionList::new();
        vl.add_version("v1".to_string(), true);
        let vl_bytes = serde_json::to_vec(&vl).unwrap();

        let ops = vec![
            BatchOp {
                keyspace: KeyspaceId::Objects,
                action: BatchAction::Delete(b"bucket/key".to_vec()),
            },
            BatchOp {
                keyspace: KeyspaceId::Versions,
                action: BatchAction::Put(b"bucket/key".to_vec(), vl_bytes),
            },
        ];

        index_db.batch_write(ops).await.unwrap();

        // Object should be gone
        assert!(index_db.get("bucket/key").await.unwrap().is_none());

        // Version list should exist
        let vl = index_db.get_version_list("bucket", "key").await.unwrap();
        assert_eq!(vl.len(), 1);
    }

    #[tokio::test]
    async fn test_direct_version_list_operations() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db");
        let index_db = IndexDb::new(&db_path).unwrap();

        // Initially empty
        let vl = index_db.get_version_list("bucket", "key").await.unwrap();
        assert!(vl.is_empty());

        // Store a version list
        let mut vl = VersionList::new();
        vl.add_version("v1".to_string(), false);
        vl.add_version("v2".to_string(), false);
        index_db.put_version_list("bucket", "key", &vl).await.unwrap();

        // Retrieve it
        let retrieved = index_db.get_version_list("bucket", "key").await.unwrap();
        assert_eq!(retrieved.len(), 2);
        assert_eq!(retrieved.versions, vec!["v2", "v1"]);

        // Delete by storing empty
        let empty = VersionList::new();
        index_db.put_version_list("bucket", "key", &empty).await.unwrap();
        let retrieved = index_db.get_version_list("bucket", "key").await.unwrap();
        assert!(retrieved.is_empty());
    }

    #[tokio::test]
    async fn test_list_version_lists() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db");
        let index_db = IndexDb::new(&db_path).unwrap();

        let mut vl1 = VersionList::new();
        vl1.add_version("v1".to_string(), false);
        index_db.put_version_list("bucket", "key1", &vl1).await.unwrap();

        let mut vl2 = VersionList::new();
        vl2.add_version("v2".to_string(), false);
        index_db.put_version_list("bucket", "key2", &vl2).await.unwrap();

        let mut vl3 = VersionList::new();
        vl3.add_version("v3".to_string(), false);
        index_db.put_version_list("other", "key3", &vl3).await.unwrap();

        let results = index_db.list_version_lists("bucket/", 100).await.unwrap();
        assert_eq!(results.len(), 2);

        let all = index_db.list_version_lists("", 100).await.unwrap();
        assert_eq!(all.len(), 3);
    }

    #[tokio::test]
    async fn test_list_objects_direct() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db");
        let index_db = IndexDb::new(&db_path).unwrap();

        let record = crate::types::IndexRecord::new(
            1,
            0,
            10,
            [0u8; 32],
            "etag".to_string(),
            "text/plain".to_string(),
        );

        index_db.put("bucket/key1", &record).await.unwrap();
        index_db.put("bucket/key2", &record).await.unwrap();
        index_db.put("other/key3", &record).await.unwrap();

        // list_objects always reads from objects keyspace
        let results = index_db.list_objects("bucket/", 100).await.unwrap();
        assert_eq!(results.len(), 2);
    }

    #[tokio::test]
    async fn test_batch_write_empty() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db");
        let index_db = IndexDb::new(&db_path).unwrap();

        // Empty batch should succeed without errors
        index_db.batch_write(vec![]).await.unwrap();
    }

    #[tokio::test]
    async fn test_keyspace_isolation() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db");
        let index_db = IndexDb::new(&db_path).unwrap();

        let record = crate::types::IndexRecord::new(
            1,
            0,
            10,
            [0u8; 32],
            "etag".to_string(),
            "text/plain".to_string(),
        );

        // Write to objects keyspace
        index_db.put("bucket/key", &record).await.unwrap();

        // Write to versions keyspace with same stripped key
        let mut vl = VersionList::new();
        vl.add_version("v1".to_string(), false);
        index_db.put_version_list("bucket", "key", &vl).await.unwrap();

        // Both should be independently accessible
        let obj = index_db.get("bucket/key").await.unwrap();
        assert!(obj.is_some());

        let vl = index_db.get_version_list("bucket", "key").await.unwrap();
        assert_eq!(vl.len(), 1);

        // Deleting from one keyspace should not affect the other
        index_db.delete("bucket/key").await.unwrap();
        assert!(index_db.get("bucket/key").await.unwrap().is_none());

        let vl = index_db.get_version_list("bucket", "key").await.unwrap();
        assert_eq!(vl.len(), 1); // Still exists in versions keyspace
    }

    // ================================================================
    // Compaction support tests
    // ================================================================

    #[tokio::test]
    async fn test_scan_objects_by_volume() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db");
        let index_db = IndexDb::new(&db_path).unwrap();

        // Create records in different volumes
        let rec_vol0_a = crate::types::IndexRecord::new(
            0,
            0,
            100,
            [1u8; 32],
            "etag1".to_string(),
            "text/plain".to_string(),
        );
        let rec_vol0_b = crate::types::IndexRecord::new(
            0,
            200,
            50,
            [2u8; 32],
            "etag2".to_string(),
            "text/plain".to_string(),
        );
        let rec_vol1 = crate::types::IndexRecord::new(
            1,
            0,
            300,
            [3u8; 32],
            "etag3".to_string(),
            "text/plain".to_string(),
        );
        // Inline object (should be excluded)
        let rec_inline = crate::types::IndexRecord::new(
            u32::MAX,
            0,
            10,
            [4u8; 32],
            "etag4".to_string(),
            "text/plain".to_string(),
        );

        index_db.put("bucket/obj1", &rec_vol0_a).await.unwrap();
        index_db.put("bucket/obj2", &rec_vol0_b).await.unwrap();
        index_db.put("bucket/obj3", &rec_vol1).await.unwrap();
        index_db.put("bucket/inline", &rec_inline).await.unwrap();

        // Scan volume 0 — should get 2 results
        let vol0_results = index_db.scan_objects_by_volume(0).await.unwrap();
        assert_eq!(vol0_results.len(), 2);

        // Scan volume 1 — should get 1 result
        let vol1_results = index_db.scan_objects_by_volume(1).await.unwrap();
        assert_eq!(vol1_results.len(), 1);
        assert_eq!(vol1_results[0].0, "bucket/obj3");

        // Scan volume 999 — should get 0 results
        let empty = index_db.scan_objects_by_volume(999).await.unwrap();
        assert!(empty.is_empty());
    }
}
