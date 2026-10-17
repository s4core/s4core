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
//! | `version_digests` | ring position, `{bucket}/{key}` | [`VersionDigest`](crate::storage::VersionDigest), cluster mode |
//! | `cluster_meta` | name                         | digest layout, tombstone horizon, configurations adopted, bucket metadata placement |
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
use crate::storage::dedup::DedupEntry;
use crate::storage::engine::KeyspaceSnapshot;
use crate::storage::version_digest::{
    digest_change, digest_key_of, DigestChange, DigestSource, TombstoneHorizon, VersionDigestEntry,
    VERSION_DIGESTS_FORMAT,
};
use crate::storage::VersionList;
use crate::types::{BlobId, BlobLocation, BlobRefEntry, IndexRecord};
use fjall::{Database, Keyspace, KeyspaceCreateOptions, PersistMode, Readable};
use std::collections::{BTreeMap, HashSet};
use std::io::Write;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tokio::task;

/// Name of the keyspace holding the version digests of the replicated keys.
const VERSION_DIGESTS_KEYSPACE: &str = "version_digests";
/// Name of the keyspace holding what the cluster relies on about this node's
/// history: the digest layout and the tombstone horizon.
pub(crate) const CLUSTER_META_KEYSPACE: &str = "cluster_meta";
/// Key of the digest layout version in [`CLUSTER_META_KEYSPACE`]. Written last
/// by a rebuild: its absence means the digests were not built, or not kept.
const DIGESTS_FORMAT_KEY: &[u8] = b"version_digests_format";
/// Key of the tombstone horizon in [`CLUSTER_META_KEYSPACE`].
const TOMBSTONE_HORIZON_KEY: &[u8] = b"tombstone_horizon";
/// Monotone lower bound of the settled tombstone history, kept across opens.
const TOMBSTONE_HORIZON_FLOOR_KEY: &[u8] = b"tombstone_horizon_floor";

/// The `cluster_meta` key of the tombstone purge horizon: the newest HLC of a
/// tombstone GC removed. Kept when the index is opened without digests: the
/// next cluster start settles the tombstone horizon past it anyway.
const TOMBSTONE_PURGE_HORIZON_KEY: &[u8] = b"tombstone_purge_horizon";
/// Key in [`CLUSTER_META_KEYSPACE`] present when the index was created by an
/// open with digests and every open since kept them: no delete in its history
/// went without a tombstone.
const BORN_WITH_DIGESTS_KEY: &[u8] = b"index_born_with_digests";

/// The seal of the version digests of the index at `db_path`: a file beside
/// it, outside the database, so that writing it moves no sequence number.
fn digests_seal_path(db_path: &Path) -> PathBuf {
    let mut name = db_path.file_name().map(std::ffi::OsString::from).unwrap_or_default();
    name.push(".digests-sealed");
    db_path.with_file_name(name)
}

/// The sequence number a seal records, `None` without a readable one.
fn read_digests_seal(path: &Path) -> Option<u64> {
    std::fs::read_to_string(path).ok()?.trim().parse().ok()
}
/// Key in [`CLUSTER_META_KEYSPACE`] present once a pass of adoption brought
/// every bucket configuration this node stored before protocol 6 to the
/// replicas of its key. An open without digests drops it: in single mode a
/// configuration is written without an HLC again.
const BUCKET_CONFIGS_ADOPTED_KEY: &[u8] = b"bucket_configs_adopted";
/// Key in [`CLUSTER_META_KEYSPACE`] present once this node saw every node of
/// its pool adopt its bucket configurations, and reads them through the
/// quorums from then on. This cutover survives opens without digests.
const BUCKET_CONFIG_QUORUM_READS_KEY: &[u8] = b"bucket_config_quorum_reads";
/// Monotone evidence that local configurations may already have been
/// replicated and deleted. Earlier releases do not erase this marker.
const BUCKET_CONFIG_HISTORY_CHECK_KEY: &[u8] = b"bucket_config_history_check";
/// An explicit single-mode open after the directory's most recent cluster open.
pub(crate) const LAST_OPEN_SINGLE_KEY: &[u8] = b"last_open_single";
/// Completion of single-node object migration, invalidated with digest history.
const LEGACY_MIGRATION_COMPLETE_KEY: &[u8] = b"legacy_migration_complete";
pub(crate) const LEGACY_MIGRATION_QUEUE_PREFIX: &[u8] = b"legacy_migration_queue/";
/// Key in [`CLUSTER_META_KEYSPACE`] present once this node saw every node of
/// its pool hold no IAM of its own that the pool has not taken over: IAM is
/// changed through the pool from then on, across restarts. An open without
/// digests drops it: a single node keeps IAM of its own again.
const IAM_ADOPTED_KEY: &[u8] = b"iam_adopted";
/// Key in [`CLUSTER_META_KEYSPACE`] of where this node is in the move of the
/// pool's bucket metadata to its replication factor: an encoded record the
/// cluster crate reads and writes, opaque here. Kept by an open without
/// digests: the stage of a node only grows, and a node that forgot it would
/// come back two stages behind its pool.
const BUCKET_METADATA_PLACEMENT_KEY: &[u8] = b"bucket_metadata_placement";
/// Durable pool membership, retained even by an open without version digests.
const POOL_GENERATION_KEY: &[u8] = b"pool_generation";
/// Prefix in [`CLUSTER_META_KEYSPACE`] of this node's progress in moving the
/// pool's keys to a grown generation: one opaque record per name, which the
/// cluster crate encodes. Kept by an open without digests, like the
/// generation it belongs to.
const REBALANCE_PREFIX: &[u8] = b"rebalance/";
/// Digests written per batch while rebuilding.
const REBUILD_BATCH: usize = 10_000;

fn db_error(error: impl std::fmt::Display) -> StorageError {
    StorageError::Database(error.to_string())
}

/// How an index database is opened.
#[derive(Debug, Clone, Copy, Default)]
pub struct IndexOptions {
    /// Keep the version digest of every replicated key
    /// ([`version_digest`](crate::storage::version_digest)), in the same batch
    /// as the record: cluster mode.
    ///
    /// An index opened without them forgets the digests and the tombstone
    /// horizon it had: its writes do not keep the first, and its deletes leave
    /// no tombstones, which the second promises.
    pub version_digests: bool,
}

/// What opening the index found about the version digests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VersionDigestsOpening {
    /// The stored digests are of the current layout and were kept by every
    /// write since they were built.
    Current,
    /// The digests were rebuilt from the records.
    Rebuilt {
        /// Digests written: one per replicated key.
        records: u64,
        /// How long the rebuild took.
        elapsed: Duration,
        /// The layout found, `None` when the digests had not been built or
        /// were not kept by every write.
        previous_format: Option<u32>,
    },
}

/// The stored version digests compared with the ones the records give.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VersionDigestCheck {
    /// Digests the records give.
    pub expected: u64,
    /// Digests stored.
    pub stored: u64,
    /// Keys whose stored digest is missing, different, or describes no record.
    pub mismatched_keys: Vec<String>,
}

impl VersionDigestCheck {
    /// Whether the stored digests are exactly the ones the records give.
    pub fn is_consistent(&self) -> bool {
        self.mismatched_keys.is_empty() && self.expected == self.stored
    }
}

/// The keyspaces that keep the version digests, when the index keeps them.
#[derive(Clone)]
struct DigestKeyspaces {
    digests: Keyspace,
    meta: Keyspace,
}

/// The digest source of a keyspace, if writes to it change digests.
fn digest_source(id: KeyspaceId) -> Option<DigestSource> {
    match id {
        KeyspaceId::Objects => Some(DigestSource::Objects),
        KeyspaceId::Buckets => Some(DigestSource::Buckets),
        KeyspaceId::Iam => Some(DigestSource::Iam),
        _ => None,
    }
}

/// Adds a digest change to `batch`.
fn apply_digest_change(
    batch: &mut fjall::OwnedWriteBatch,
    digests: &Keyspace,
    change: DigestChange,
) {
    match change {
        (key, Some(value)) => batch.insert(digests, key, value),
        (key, None) => batch.remove(digests, key),
    }
}

// ============================================================================
// Batch Write Types
// ============================================================================

/// Identifies a target keyspace for batch operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
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
    /// Atomically read-modify-write a dedup entry: increment ref_count or create
    /// with ref_count=1. The read happens inside `batch_write` under `rmw_lock`.
    IncrementDedupRef {
        /// PG-scoped dedup key (4-byte PG prefix + 32-byte content hash).
        key: Vec<u8>,
        /// Volume file ID for a newly created entry.
        volume_id: u32,
        /// Byte offset within the volume for a newly created entry.
        offset: u64,
    },
    /// Atomically read-modify-write a dedup entry: decrement ref_count, removing
    /// the entry if it reaches zero. No-op if the entry does not exist.
    DecrementDedupRef {
        /// PG-scoped dedup key (4-byte PG prefix + 32-byte content hash).
        key: Vec<u8>,
    },
    /// Atomically read-modify-write a dedup entry: update volume_id and offset,
    /// preserving ref_count. Returns an error if the entry does not exist.
    UpdateDedupLocation {
        /// PG-scoped dedup key (4-byte PG prefix + 32-byte content hash).
        key: Vec<u8>,
        /// New volume file ID after relocation.
        new_volume_id: u32,
        /// New byte offset after relocation.
        new_offset: u64,
    },
    /// Atomically read-modify-write a blob reference: add `committed` and
    /// `staged` to its counts, never below zero. A blob without an entry gets
    /// one at `location`; with no `location` that is an error when a count
    /// grows (the blob was expected to exist), and nothing when it shrinks.
    /// The read happens inside `batch_write` under `rmw_lock`.
    AdjustBlobRef {
        /// The blob's id (32 bytes).
        blob_id: Vec<u8>,
        /// Change of the committed count.
        committed: i64,
        /// Change of the staged count.
        staged: i64,
        /// Where a blob without an entry lies: `(volume_id, offset)`.
        location: Option<(u32, u64)>,
    },
    /// Atomically read-modify-write a blob reference: move it to a new
    /// location, keeping its counts. No-op if the entry does not exist.
    RelocateBlobRef {
        /// The blob's id (32 bytes).
        blob_id: Vec<u8>,
        /// New volume file ID.
        volume_id: u32,
        /// New byte offset within the volume.
        offset: u64,
    },
}

impl BatchAction {
    /// Whether the action reads the value it writes, and so must run under
    /// `rmw_lock`.
    fn reads_before_write(&self) -> bool {
        !matches!(self, Self::Put(..) | Self::Delete(..))
    }
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
    /// Serializes batch_write calls that contain read-modify-write operations,
    /// preventing lost-update races on dedup ref_count.
    rmw_lock: std::sync::Arc<std::sync::Mutex<()>>,
    /// The version digests and the cluster facts, when the index keeps them
    /// ([`IndexOptions::version_digests`]).
    digest_keyspaces: Option<DigestKeyspaces>,
    /// What opening found about the version digests.
    digests_opening: Option<VersionDigestsOpening>,
    /// Where [`seal_version_digests`](Self::seal_version_digests) records the
    /// sequence number the digests matched the records at.
    digests_seal: PathBuf,
}

/// The digest changes of a batch, one per digest key, from the last operation
/// on the key: changes of one batch commit under one sequence number, so two
/// for the same key would leave either.
fn batch_digest_changes(operations: &[BatchOp]) -> Result<Vec<DigestChange>, StorageError> {
    let mut changes: Vec<DigestChange> = Vec::new();
    let mut position_of_key = std::collections::HashMap::new();
    for op in operations {
        let Some(source) = digest_source(op.keyspace) else {
            continue;
        };
        let change = match &op.action {
            BatchAction::Put(key, value) => digest_change(source, key, Some(value))?,
            BatchAction::Delete(key) => digest_change(source, key, None)?,
            _ => None,
        };
        if let Some(change) = change {
            match position_of_key.get(&change.0) {
                Some(&position) => changes[position] = change,
                None => {
                    position_of_key.insert(change.0.clone(), changes.len());
                    changes.push(change);
                }
            }
        }
    }
    Ok(changes)
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

    /// Whether the index holds no record at all: no object, version list,
    /// bucket marker or IAM record. An index whose objects were all deleted
    /// still holds its buckets and, with a root configured, its IAM state.
    pub fn has_no_records(&self) -> Result<bool, StorageError> {
        for keyspace in [&self.objects, &self.versions, &self.buckets, &self.iam] {
            if !keyspace.is_empty().map_err(|e| StorageError::Database(e.to_string()))? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Flushes the fjall write-ahead log to disk and triggers memtable rotation.
    ///
    /// Call this after bulk deletes (e.g. compactor purging sessions, parts,
    /// blob_refs) to ensure deleted entries are persisted and the WAL does not
    /// grow unbounded. This also allows fjall's internal LSM compaction to
    /// reclaim tombstone space sooner.
    pub fn persist(&self) -> Result<(), StorageError> {
        self.db
            .persist(PersistMode::SyncAll)
            .map_err(|e| StorageError::Database(e.to_string()))
    }

    /// Counts objects with `file_id == u32::MAX` (inline objects) by iterating
    /// the objects keyspace without collecting into a Vec. Delete markers and
    /// tombstones share that file id and hold no data, so they are not counted.
    ///
    /// Uses O(1) memory vs O(n) for `list_objects("", usize::MAX)` which
    /// allocates a Vec of all records. CPU cost (deserialization) is the same,
    /// but memory pressure drops dramatically on large keyspaces, avoiding
    /// the multi-minute startup hangs observed with 100k+ stale entries.
    pub async fn count_inline_objects(&self) -> Result<u64, StorageError> {
        let objects = self.objects.clone();
        task::spawn_blocking(move || {
            let mut count = 0u64;
            for guard in objects.iter() {
                let (_key_bytes, value_bytes) =
                    guard.into_inner().map_err(|e| StorageError::Database(e.to_string()))?;
                let record: IndexRecord = bincode::deserialize(&value_bytes)
                    .map_err(|e| StorageError::Serialization(e.to_string()))?;
                if record.file_id == u32::MAX && !record.is_delete_marker {
                    count += 1;
                }
            }
            Ok(count)
        })
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?
    }

    /// Removes all entries from the metadata journal keyspace.
    ///
    /// In single-node mode, the journal is only needed for crash recovery
    /// replay. Once the materialized index is consistent, all journal entries
    /// are safe to discard. This prevents the journal from growing without
    /// bound and bloating the metadata database.
    ///
    /// In federation/replication mode, journal compaction must respect the
    /// minimum confirmed sequence across replicas — use
    /// [`MetadataJournal::compact`](super::MetadataJournal::compact) with an explicit sequence instead.
    pub fn compact_journal_all(&self) -> Result<u64, StorageError> {
        let approx_count = self.journal.approximate_len() as u64;
        self.journal.clear().map_err(|e| StorageError::Database(e.to_string()))?;
        Ok(approx_count)
    }
}

impl IndexDb {
    /// Creates or opens an index database with all keyspaces, without version
    /// digests: [`open`](Self::open) with the default options.
    ///
    /// # Arguments
    ///
    /// * `db_path` - Path to the fjall database directory
    pub fn new(db_path: &Path) -> Result<Self, StorageError> {
        Self::open(db_path, IndexOptions::default())
    }

    /// Creates or opens an index database with all keyspaces.
    ///
    /// On first open this creates the keyspaces. On subsequent opens the
    /// existing keyspaces are reused. A legacy `"index"` keyspace from Phase 2,
    /// if present, is **not** deleted — it simply becomes unused.
    ///
    /// With [`IndexOptions::version_digests`] the digests are rebuilt from the
    /// records before this returns, when they are of another layout or were
    /// not kept by every write ([`version_digests_opening`](Self::version_digests_opening)
    /// says which). Without it, digests and the tombstone horizon left by an
    /// earlier open are dropped.
    pub fn open(db_path: &Path, options: IndexOptions) -> Result<Self, StorageError> {
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent).map_err(StorageError::Io)?;
        }

        let db = Database::builder(db_path)
            .open()
            .map_err(|e| StorageError::Database(e.to_string()))?;
        // Neither the keyspaces nor the legacy one they migrate from: this
        // open creates the index
        let created = !db.keyspace_exists("objects") && !db.keyspace_exists("index");

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

        let digests_seal = digests_seal_path(db_path);
        let (digest_keyspaces, digests_opening) = if options.version_digests {
            // A marker beside a keyspace that is gone describes nothing.
            let digests_existed = db.keyspace_exists(VERSION_DIGESTS_KEYSPACE);
            // Only an unchanged, readable seal attests that every write kept
            // digests and tombstones. Without it, a crash and writes by a
            // release that keeps neither cannot be distinguished.
            let seal_matches = read_digests_seal(&digests_seal) == Some(db.seqno());
            let keyspaces = DigestKeyspaces {
                digests: open_ks(VERSION_DIGESTS_KEYSPACE)?,
                meta: open_ks(CLUSTER_META_KEYSPACE)?,
            };
            if created {
                keyspaces.meta.insert(BORN_WITH_DIGESTS_KEY, [1]).map_err(db_error)?;
                db.persist(PersistMode::SyncAll).map_err(db_error)?;
            } else if !seal_matches {
                Self::forget_digest_history(&db, &keyspaces.meta, digests_existed)?;
            } else if !keyspaces
                .meta
                .contains_key(BUCKET_CONFIG_HISTORY_CHECK_KEY)
                .map_err(db_error)?
                && (keyspaces.meta.contains_key(BUCKET_CONFIGS_ADOPTED_KEY).map_err(db_error)?
                    || keyspaces
                        .meta
                        .contains_key(BUCKET_CONFIG_QUORUM_READS_KEY)
                        .map_err(db_error)?)
            {
                // Preserve evidence written by earlier digest-aware builds
                // before one of those builds can erase its older markers.
                keyspaces.meta.insert(BUCKET_CONFIG_HISTORY_CHECK_KEY, [1]).map_err(db_error)?;
                db.persist(PersistMode::SyncAll).map_err(db_error)?;
            }
            let opening = Self::settle_digests(
                &db,
                &objects,
                &buckets,
                &iam,
                &keyspaces,
                digests_existed && seal_matches,
            )?;
            (Some(keyspaces), Some(opening))
        } else {
            Self::forget_digests(&db)?;
            (None, None)
        };

        // Keep single-origin evidence until migration completion: a failed
        // bootstrap may already have created the persistent node identity.

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
            rmw_lock: std::sync::Arc::new(std::sync::Mutex::new(())),
            digest_keyspaces,
            digests_opening,
            digests_seal,
        })
    }

    /// Records that the version digests match the records as the index stands
    /// now, its writes on disk: the last thing a node does before it stops.
    /// The next open keeps the digests only while nothing has written to the
    /// index since — a crash or a release without digests running on this
    /// data both rebuild them there.
    ///
    /// # Errors
    ///
    /// The index or its seal could not be written to disk. The next open
    /// rebuilds the digests and forgets its history guarantees unless an
    /// earlier readable seal still matches the index.
    pub fn seal_version_digests(&self) -> Result<(), StorageError> {
        if self.digest_keyspaces.is_none() {
            return Ok(());
        }
        // Capture before syncing: a concurrent write may make the seal old,
        // but it must never attest to a sequence not yet persisted. The
        // server calls this only after its writers have stopped.
        let seqno = self.db.seqno();
        self.db.persist(PersistMode::SyncAll).map_err(db_error)?;
        let tmp = self.digests_seal.with_extension("tmp");
        let mut file = std::fs::File::create(&tmp).map_err(StorageError::Io)?;
        write!(file, "{seqno}").map_err(StorageError::Io)?;
        file.sync_all().map_err(StorageError::Io)?;
        drop(file);
        std::fs::rename(&tmp, &self.digests_seal).map_err(StorageError::Io)?;
        Ok(())
    }

    /// Keeps the stored digests when they are of the current layout and were
    /// kept by every write, rebuilds them otherwise.
    fn settle_digests(
        db: &Database,
        objects: &Keyspace,
        buckets: &Keyspace,
        iam: &Keyspace,
        keyspaces: &DigestKeyspaces,
        digests_existed: bool,
    ) -> Result<VersionDigestsOpening, StorageError> {
        let stored_format = keyspaces
            .meta
            .get(DIGESTS_FORMAT_KEY)
            .map_err(db_error)?
            .map(|value| {
                <[u8; 4]>::try_from(&value[..]).map(u32::from_be_bytes).map_err(|_| {
                    StorageError::Serialization("a digest format of the wrong size".into())
                })
            })
            .transpose()?;
        let previous_format = stored_format.filter(|_| digests_existed);
        if previous_format == Some(VERSION_DIGESTS_FORMAT) {
            return Ok(VersionDigestsOpening::Current);
        }
        let started = Instant::now();
        let records = Self::rebuild_digests(db, objects, buckets, iam, keyspaces)?;
        Ok(VersionDigestsOpening::Rebuilt {
            records,
            elapsed: started.elapsed(),
            previous_format,
        })
    }

    /// Writes the digest of every record anew.
    ///
    /// The layout marker goes first and comes back last: a rebuild cut short
    /// leaves none, and the next open rebuilds again. Records written while
    /// it runs could be overwritten by their older digest, so it runs only
    /// where nothing else writes: while the index opens, and inside a restore.
    fn rebuild_digests(
        db: &Database,
        objects: &Keyspace,
        buckets: &Keyspace,
        iam: &Keyspace,
        keyspaces: &DigestKeyspaces,
    ) -> Result<u64, StorageError> {
        keyspaces.meta.remove(DIGESTS_FORMAT_KEY).map_err(db_error)?;
        db.persist(PersistMode::SyncAll).map_err(db_error)?;
        keyspaces.digests.clear().map_err(db_error)?;

        let mut records = 0u64;
        let mut batch = db.batch();
        for (source, keyspace) in [
            (DigestSource::Objects, objects),
            (DigestSource::Buckets, buckets),
            (DigestSource::Iam, iam),
        ] {
            for guard in keyspace.iter() {
                let (key, value) = guard.into_inner().map_err(db_error)?;
                if let Some((digest_key, Some(digest))) = digest_change(source, &key, Some(&value))?
                {
                    batch.insert(&keyspaces.digests, digest_key, digest);
                    records += 1;
                    if batch.len() >= REBUILD_BATCH {
                        batch.commit().map_err(db_error)?;
                        batch = db.batch();
                    }
                }
            }
        }
        batch.commit().map_err(db_error)?;
        db.persist(PersistMode::SyncAll).map_err(db_error)?;

        keyspaces
            .meta
            .insert(DIGESTS_FORMAT_KEY, VERSION_DIGESTS_FORMAT.to_be_bytes())
            .map_err(db_error)?;
        db.persist(PersistMode::SyncAll).map_err(db_error)?;
        Ok(records)
    }

    /// Forgets guarantees that depend on every previous write keeping digests
    /// and tombstones. Persisted as one batch before rebuilding digests, so an
    /// interrupted rebuild cannot leave trusted history behind. Placement,
    /// rebalance progress, the monotone purge horizon and the pool's switch
    /// to quorum configuration reads are independent and remain recorded.
    fn forget_digest_history(
        db: &Database,
        meta: &Keyspace,
        had_digests: bool,
    ) -> Result<(), StorageError> {
        let mut batch = db.batch();
        // Keep the old boundary as a floor: clock corrections cannot make
        // the next settled history stronger than the previous one.
        if let Some(stored) = meta.get(TOMBSTONE_HORIZON_KEY).map_err(db_error)? {
            let mut floor = TombstoneHorizon::decode(&stored)?;
            if let Some(stored) = meta.get(TOMBSTONE_HORIZON_FLOOR_KEY).map_err(db_error)? {
                floor = floor.max(TombstoneHorizon::decode(&stored)?);
            }
            batch.insert(meta, TOMBSTONE_HORIZON_FLOOR_KEY, floor.encode());
        }
        // A configuration-digest-aware start may have completed adoption even if the
        // pool's final cutover marker was never recorded (or an older build
        // erased it). Only the very first legacy adoption can trust a lone
        // local copy without checking its owners' history.
        let predates_config_digests = meta
            .get(DIGESTS_FORMAT_KEY)
            .map_err(db_error)?
            .and_then(|value| <[u8; 4]>::try_from(value.as_ref()).ok())
            .map(u32::from_be_bytes)
            .is_some_and(|format| (1..=3).contains(&format));
        if (had_digests && !predates_config_digests)
            || meta.contains_key(BUCKET_CONFIGS_ADOPTED_KEY).map_err(db_error)?
            || meta.contains_key(BUCKET_CONFIG_QUORUM_READS_KEY).map_err(db_error)?
        {
            batch.insert(meta, BUCKET_CONFIG_HISTORY_CHECK_KEY, [1]);
        }
        for key in [
            TOMBSTONE_HORIZON_KEY,
            BORN_WITH_DIGESTS_KEY,
            BUCKET_CONFIGS_ADOPTED_KEY,
            IAM_ADOPTED_KEY,
            LEGACY_MIGRATION_COMPLETE_KEY,
        ] {
            if meta.contains_key(key).map_err(db_error)? {
                batch.remove(meta, key);
            }
        }
        if !batch.is_empty() {
            batch.commit().map_err(db_error)?;
            db.persist(PersistMode::SyncAll).map_err(db_error)?;
        }
        Ok(())
    }

    /// Drops the digests and the tombstone horizon an index opened with
    /// digests left: writes from now on do not keep the first, and deletes
    /// leave no tombstones, which the second promises.
    fn forget_digests(db: &Database) -> Result<(), StorageError> {
        let mut changed = false;
        if db.keyspace_exists(CLUSTER_META_KEYSPACE) {
            let meta = db
                .keyspace(CLUSTER_META_KEYSPACE, KeyspaceCreateOptions::default)
                .map_err(db_error)?;
            Self::forget_digest_history(db, &meta, db.keyspace_exists(VERSION_DIGESTS_KEYSPACE))?;
            if meta.contains_key(DIGESTS_FORMAT_KEY).map_err(db_error)? {
                meta.remove(DIGESTS_FORMAT_KEY).map_err(db_error)?;
                changed = true;
            }
        }
        if db.keyspace_exists(VERSION_DIGESTS_KEYSPACE) {
            let digests = db
                .keyspace(VERSION_DIGESTS_KEYSPACE, KeyspaceCreateOptions::default)
                .map_err(db_error)?;
            if !digests.is_empty().map_err(db_error)? {
                digests.clear().map_err(db_error)?;
                changed = true;
            }
        }
        if changed {
            db.persist(PersistMode::SyncAll).map_err(db_error)?;
        }
        Ok(())
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

    /// The digest source and keyspace a write to `id` changes, when the index
    /// keeps digests and the keyspace has them.
    fn digests_for(&self, id: KeyspaceId) -> Option<(DigestSource, Keyspace)> {
        let keyspaces = self.digest_keyspaces.as_ref()?;
        digest_source(id).map(|source| (source, keyspaces.digests.clone()))
    }

    /// Returns the keyspace handle for the given [`KeyspaceId`].
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
        let (ks_id, stripped) = self.route_key_id(key);
        let db = self.db.clone();
        let ks = self.keyspace_for(ks_id).clone();
        let digests = self.digests_for(ks_id);
        let stripped = stripped.to_string();
        let record_bytes =
            bincode::serialize(record).map_err(|e| StorageError::Serialization(e.to_string()))?;

        task::spawn_blocking(move || {
            match digests {
                Some((source, digests)) => {
                    let mut batch = db.batch();
                    if let Some(change) =
                        digest_change(source, stripped.as_bytes(), Some(&record_bytes))?
                    {
                        apply_digest_change(&mut batch, &digests, change);
                    }
                    batch.insert(&ks, stripped.as_bytes(), &record_bytes[..]);
                    batch.commit().map_err(db_error)?;
                }
                None => ks.insert(&stripped, &record_bytes[..]).map_err(db_error)?,
            }
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
        let (ks_id, stripped) = self.route_key_id(key);
        let db = self.db.clone();
        let ks = self.keyspace_for(ks_id).clone();
        let digests = self.digests_for(ks_id);
        let stripped = stripped.to_string();

        task::spawn_blocking(move || {
            match digests {
                Some((source, digests)) => {
                    let mut batch = db.batch();
                    if let Some(change) = digest_change(source, stripped.as_bytes(), None)? {
                        apply_digest_change(&mut batch, &digests, change);
                    }
                    batch.remove(&ks, stripped.as_bytes());
                    batch.commit().map_err(db_error)?;
                }
                None => ks.remove(&stripped).map_err(db_error)?,
            }
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
    /// Every record is listed, tombstones included.
    pub async fn list_after(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        max_keys: usize,
    ) -> Result<Vec<(String, IndexRecord)>, StorageError> {
        self.scan_after(prefix, start_after, max_keys, false).await
    }

    /// Lists entries like [`list_after`](Self::list_after), leaving out
    /// tombstones ([`IndexRecord::is_tombstone`]).
    ///
    /// They are left out while scanning, so `max_keys` counts the entries
    /// returned: a page cut short by tombstones would read as the last one.
    pub async fn list_live_after(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        max_keys: usize,
    ) -> Result<Vec<(String, IndexRecord)>, StorageError> {
        self.scan_after(prefix, start_after, max_keys, true).await
    }

    async fn scan_after(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        max_keys: usize,
        skip_tombstones: bool,
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

            // A scan that resumes inside the prefix starts right after
            // `start_after` rather than skipping every key before it: a
            // listing that pages through a long run of tombstones would
            // otherwise read the whole run again on every page.
            let iter: Box<dyn Iterator<Item = fjall::Guard>> = match &start_after_owned {
                Some(sa) if sa.as_str() >= stripped_prefix.as_str() => Box::new(ks.range((
                    std::ops::Bound::Excluded(sa.as_bytes().to_vec()),
                    std::ops::Bound::Unbounded,
                ))),
                _ if stripped_prefix.is_empty() => Box::new(ks.iter()),
                _ => Box::new(ks.prefix(&stripped_prefix)),
            };

            for guard in iter {
                let (key_bytes, value_bytes) =
                    guard.into_inner().map_err(|e| StorageError::Database(e.to_string()))?;

                // A range scan runs past the prefix; the keys under it end here
                if !key_bytes.starts_with(stripped_prefix.as_bytes()) {
                    break;
                }

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
                if skip_tombstones && record.is_tombstone() {
                    continue;
                }

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
        self.commit(operations, None).await
    }

    /// Commits `operations` — the removal of tombstones tombstone GC found
    /// due — and, in the same atomic batch, raises the tombstone purge horizon
    /// ([`tombstone_purge_horizon`](Self::tombstone_purge_horizon)) to
    /// `purged_up_to`, the newest HLC among them. A crash can never leave a
    /// tombstone removed without the horizon that tells peers so.
    ///
    /// # Errors
    ///
    /// [`StorageError::InvalidOperation`] on an index that keeps no version
    /// digests: it keeps no tombstones of a replicated pool either.
    pub async fn purge_tombstones(
        &self,
        operations: Vec<BatchOp>,
        purged_up_to: TombstoneHorizon,
    ) -> Result<(), StorageError> {
        self.commit(operations, Some(purged_up_to)).await
    }

    /// The tombstone purge horizon: the newest HLC of a tombstone tombstone
    /// GC removed from this index, `None` before the first. A key this node
    /// holds nothing for may have had a delete up to it whose tombstone is
    /// gone, so its absence proves nothing about a version older than it.
    pub async fn tombstone_purge_horizon(&self) -> Result<Option<TombstoneHorizon>, StorageError> {
        let keyspaces = self.digest_keyspaces()?;
        task::spawn_blocking(move || {
            keyspaces
                .meta
                .get(TOMBSTONE_PURGE_HORIZON_KEY)
                .map_err(db_error)?
                .map(|value| TombstoneHorizon::decode(&value))
                .transpose()
        })
        .await
        .map_err(db_error)?
    }

    async fn commit(
        &self,
        operations: Vec<BatchOp>,
        purge_horizon: Option<TombstoneHorizon>,
    ) -> Result<(), StorageError> {
        if operations.is_empty() {
            return Ok(());
        }
        let purge_meta = match purge_horizon {
            Some(horizon) => Some((self.digest_keyspaces()?.meta, horizon)),
            None => None,
        };

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
        let rmw_lock = self.rmw_lock.clone();
        let digests = self.digest_keyspaces.as_ref().map(|keyspaces| keyspaces.digests.clone());

        task::spawn_blocking(move || {
            // Raising the purge horizon reads the stored one first.
            let has_rmw =
                purge_meta.is_some() || operations.iter().any(|op| op.action.reads_before_write());

            // The digest changes depend on the operations alone: worked out
            // before the lock, they keep its critical section as short as it
            // was without digests.
            let digest_changes = match &digests {
                Some(digests) => Some((digests, batch_digest_changes(&operations)?)),
                None => None,
            };

            let rmw_guard = if has_rmw {
                Some(rmw_lock.lock().unwrap_or_else(|e| e.into_inner()))
            } else {
                None
            };

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

            // The reference counts this batch changes, as they stand after
            // each of its actions: an action reads what the actions before it
            // in the same batch left, not the value from before the batch,
            // so two changes of one count both count. Written once at the end.
            let mut counts = PendingCounts::default();

            for op in &operations {
                match &op.action {
                    BatchAction::Put(key, value) => match op.keyspace {
                        KeyspaceId::Dedup | KeyspaceId::BlobRefs => {
                            counts.set(op.keyspace, key, Some(value.clone()));
                        }
                        _ => batch.insert(ks_for(op.keyspace), &key[..], &value[..]),
                    },
                    BatchAction::Delete(key) => match op.keyspace {
                        KeyspaceId::Dedup | KeyspaceId::BlobRefs => {
                            counts.set(op.keyspace, key, None);
                        }
                        _ => batch.remove(ks_for(op.keyspace), &key[..]),
                    },
                    BatchAction::IncrementDedupRef {
                        key,
                        volume_id,
                        offset,
                    } => {
                        let new_entry = match counts.get(KeyspaceId::Dedup, &dedup, key)? {
                            Some(bytes) => {
                                let mut entry: DedupEntry = bincode::deserialize(&bytes)
                                    .map_err(|e| StorageError::Serialization(e.to_string()))?;
                                entry.ref_count += 1;
                                // An entry that names no copy is one whose blob
                                // a read found damaged (`make_quarantine_op`).
                                // The bytes this write just put down are the
                                // healthy copy of that content, so the entry
                                // takes them: without this it would name
                                // nothing forever, and the compactor decides
                                // what is live by what the entries name.
                                if entry.volume_id == crate::storage::dedup::NO_LOCATION_VOLUME_ID
                                    && *volume_id != crate::storage::dedup::NO_LOCATION_VOLUME_ID
                                {
                                    entry.volume_id = *volume_id;
                                    entry.offset = *offset;
                                }
                                entry
                            }
                            None => DedupEntry {
                                volume_id: *volume_id,
                                offset: *offset,
                                ref_count: 1,
                            },
                        };
                        let value = bincode::serialize(&new_entry)
                            .map_err(|e| StorageError::Serialization(e.to_string()))?;
                        counts.set(KeyspaceId::Dedup, key, Some(value));
                    }
                    BatchAction::DecrementDedupRef { key } => {
                        if let Some(bytes) = counts.get(KeyspaceId::Dedup, &dedup, key)? {
                            let mut entry: DedupEntry = bincode::deserialize(&bytes)
                                .map_err(|e| StorageError::Serialization(e.to_string()))?;
                            if entry.ref_count <= 1 {
                                counts.set(KeyspaceId::Dedup, key, None);
                            } else {
                                entry.ref_count -= 1;
                                let value = bincode::serialize(&entry)
                                    .map_err(|e| StorageError::Serialization(e.to_string()))?;
                                counts.set(KeyspaceId::Dedup, key, Some(value));
                            }
                        }
                    }
                    BatchAction::UpdateDedupLocation {
                        key,
                        new_volume_id,
                        new_offset,
                    } => {
                        if let Some(bytes) = counts.get(KeyspaceId::Dedup, &dedup, key)? {
                            let mut entry: DedupEntry = bincode::deserialize(&bytes)
                                .map_err(|e| StorageError::Serialization(e.to_string()))?;
                            entry.volume_id = *new_volume_id;
                            entry.offset = *new_offset;
                            let value = bincode::serialize(&entry)
                                .map_err(|e| StorageError::Serialization(e.to_string()))?;
                            counts.set(KeyspaceId::Dedup, key, Some(value));
                        }
                    }
                    BatchAction::AdjustBlobRef {
                        blob_id,
                        committed,
                        staged,
                        location,
                    } => {
                        let entry = match counts.get(KeyspaceId::BlobRefs, &blob_refs, blob_id)? {
                            Some(bytes) => Some(
                                bincode::deserialize::<BlobRefEntry>(&bytes)
                                    .map_err(|e| StorageError::Serialization(e.to_string()))?,
                            ),
                            None => location.map(|(volume_id, offset)| BlobRefEntry {
                                location: BlobLocation { volume_id, offset },
                                ref_count_committed: 0,
                                ref_count_staged: 0,
                            }),
                        };
                        let Some(mut entry) = entry else {
                            if *committed > 0 || *staged > 0 {
                                return Err(StorageError::BlobRefNotFound {
                                    blob_id: hex_id(blob_id),
                                });
                            }
                            continue;
                        };
                        entry.ref_count_committed = adjust(entry.ref_count_committed, *committed);
                        entry.ref_count_staged = adjust(entry.ref_count_staged, *staged);
                        let value = bincode::serialize(&entry)
                            .map_err(|e| StorageError::Serialization(e.to_string()))?;
                        counts.set(KeyspaceId::BlobRefs, blob_id, Some(value));
                    }
                    BatchAction::RelocateBlobRef {
                        blob_id,
                        volume_id,
                        offset,
                    } => {
                        if let Some(bytes) =
                            counts.get(KeyspaceId::BlobRefs, &blob_refs, blob_id)?
                        {
                            let mut entry: BlobRefEntry = bincode::deserialize(&bytes)
                                .map_err(|e| StorageError::Serialization(e.to_string()))?;
                            entry.location = BlobLocation {
                                volume_id: *volume_id,
                                offset: *offset,
                            };
                            let value = bincode::serialize(&entry)
                                .map_err(|e| StorageError::Serialization(e.to_string()))?;
                            counts.set(KeyspaceId::BlobRefs, blob_id, Some(value));
                        }
                    }
                }
            }

            for ((id, key), value) in counts.values {
                match value {
                    Some(value) => batch.insert(ks_for(id), &key[..], &value[..]),
                    None => batch.remove(ks_for(id), &key[..]),
                }
            }

            if let Some((digests, changes)) = digest_changes {
                for change in changes {
                    apply_digest_change(&mut batch, digests, change);
                }
            }

            if let Some((meta, horizon)) = &purge_meta {
                let stored = meta
                    .get(TOMBSTONE_PURGE_HORIZON_KEY)
                    .map_err(db_error)?
                    .map(|value| TombstoneHorizon::decode(&value))
                    .transpose()?;
                let raised = stored.map_or(*horizon, |stored| stored.max(*horizon));
                batch.insert(meta, TOMBSTONE_PURGE_HORIZON_KEY, raised.encode());
            }

            batch.commit().map_err(|e| StorageError::Database(e.to_string()))?;
            // The next batch that reads these counts may go on: it reads them
            // committed. fjall keeps one journal for the whole database and
            // appends batches in seqno order, so the fsync below — or the
            // next batch's, whichever comes first — makes this batch durable
            // before any batch that read it. This batch still answers only
            // after its fsync.
            drop(rmw_guard);
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
        let digest_keyspaces = self.digest_keyspaces.clone();

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

            // The restored records bypassed the digests: describe them anew.
            // They also bring a history this index did not see, so a horizon
            // not yet settled is no longer zero.
            if let Some(keyspaces) = &digest_keyspaces {
                keyspaces.meta.remove(BORN_WITH_DIGESTS_KEY).map_err(db_error)?;
                keyspaces.meta.remove(LEGACY_MIGRATION_COMPLETE_KEY).map_err(db_error)?;
                db.persist(PersistMode::SyncAll).map_err(db_error)?;
                Self::rebuild_digests(&db, &objects, &buckets, &iam, keyspaces)?;
            }

            Ok::<(), StorageError>(())
        })
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?
    }

    // ========================================================================
    // Version digests and the tombstone horizon (cluster mode)
    // ========================================================================

    /// Whether this index keeps version digests ([`IndexOptions::version_digests`]).
    pub fn keeps_version_digests(&self) -> bool {
        self.digest_keyspaces.is_some()
    }

    /// What opening the index found about the version digests; `None` when
    /// the index does not keep them.
    pub fn version_digests_opening(&self) -> Option<&VersionDigestsOpening> {
        self.digests_opening.as_ref()
    }

    fn digest_keyspaces(&self) -> Result<DigestKeyspaces, StorageError> {
        self.digest_keyspaces.clone().ok_or_else(|| StorageError::InvalidOperation {
            operation: "version digests".to_string(),
            reason: "the index was opened without them".to_string(),
        })
    }

    /// Rebuilds every version digest from the records.
    ///
    /// Only where nothing else writes to the index: a write that lands while
    /// it runs could get its older digest back.
    pub(crate) async fn rebuild_version_digests(&self) -> Result<u64, StorageError> {
        let keyspaces = self.digest_keyspaces()?;
        let db = self.db.clone();
        let objects = self.objects.clone();
        let buckets = self.buckets.clone();
        let iam = self.iam.clone();
        task::spawn_blocking(move || {
            Self::rebuild_digests(&db, &objects, &buckets, &iam, &keyspaces)
        })
        .await
        .map_err(db_error)?
    }

    /// Every stored version digest, in ring order.
    ///
    /// Reads the whole keyspace into memory: for checks and diagnostics.
    pub async fn version_digests(&self) -> Result<Vec<VersionDigestEntry>, StorageError> {
        let keyspaces = self.digest_keyspaces()?;
        task::spawn_blocking(move || {
            keyspaces
                .digests
                .iter()
                .map(|guard| {
                    let (key, value) = guard.into_inner().map_err(db_error)?;
                    VersionDigestEntry::decode(&key, &value)
                })
                .collect()
        })
        .await
        .map_err(db_error)?
    }

    /// A page of the stored version digests in ring order: those whose ring
    /// position is at least `from` and below `to` (`None` for the end of the
    /// ring), and that come after the key `after` when given, at most
    /// `limit` of them. An `after` past the range gives an empty page.
    ///
    /// Anti-entropy reads its pages this way, so a pass over the whole
    /// keyspace never holds more than one page in memory. Each page is read
    /// on its own: writes between two pages show in the later one.
    pub async fn version_digests_in(
        &self,
        from: u64,
        to: Option<u64>,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<VersionDigestEntry>, StorageError> {
        let keyspaces = self.digest_keyspaces()?;
        // Every key of a position below `from` sorts before `from`'s eight
        // bytes, and every key at `from` or beyond after them; so for `to`.
        let lowest = from.to_be_bytes().to_vec();
        let start = match after.map(digest_key_of) {
            Some(after) if after >= lowest => Bound::Excluded(after),
            _ => Bound::Included(lowest),
        };
        let end = match to {
            Some(to) => Bound::Excluded(to.to_be_bytes().to_vec()),
            None => Bound::Unbounded,
        };
        if let (Bound::Included(first) | Bound::Excluded(first), Bound::Excluded(last)) =
            (&start, &end)
        {
            // A range that starts at or past its end holds nothing, and the
            // keyspace is not asked for one.
            if first >= last {
                return Ok(Vec::new());
            }
        }
        task::spawn_blocking(move || {
            keyspaces
                .digests
                .range((start, end))
                .take(limit)
                .map(|guard| {
                    let (key, value) = guard.into_inner().map_err(db_error)?;
                    VersionDigestEntry::decode(&key, &value)
                })
                .collect()
        })
        .await
        .map_err(db_error)?
    }

    /// Compares the stored version digests with the ones the records give, in
    /// one consistent snapshot of the index.
    ///
    /// Holds every expected digest in memory: for checks and diagnostics.
    pub async fn verify_version_digests(&self) -> Result<VersionDigestCheck, StorageError> {
        let keyspaces = self.digest_keyspaces()?;
        let db = self.db.clone();
        let objects = self.objects.clone();
        let buckets = self.buckets.clone();
        let iam = self.iam.clone();
        task::spawn_blocking(move || {
            let snapshot = db.snapshot();
            let mut expected = BTreeMap::new();
            for (source, keyspace) in [
                (DigestSource::Objects, &objects),
                (DigestSource::Buckets, &buckets),
                (DigestSource::Iam, &iam),
            ] {
                for guard in snapshot.iter(keyspace) {
                    let (key, value) = guard.into_inner().map_err(db_error)?;
                    if let Some((digest_key, Some(digest))) =
                        digest_change(source, &key, Some(&value))?
                    {
                        expected.insert(digest_key, digest);
                    }
                }
            }

            let mut check = VersionDigestCheck {
                expected: expected.len() as u64,
                ..VersionDigestCheck::default()
            };
            let name_of =
                |key: &[u8]| String::from_utf8_lossy(key.get(8..).unwrap_or(key)).into_owned();
            for guard in snapshot.iter(&keyspaces.digests) {
                let (key, value) = guard.into_inner().map_err(db_error)?;
                check.stored += 1;
                if expected.remove(&key[..]).as_deref() != Some(&value[..]) {
                    check.mismatched_keys.push(name_of(&key));
                }
            }
            check.mismatched_keys.extend(expected.keys().map(|key| name_of(key)));
            Ok(check)
        })
        .await
        .map_err(db_error)?
    }

    /// The tombstone horizon of this node, once settled
    /// ([`settle_tombstone_horizon`](Self::settle_tombstone_horizon)).
    pub async fn tombstone_horizon(&self) -> Result<Option<TombstoneHorizon>, StorageError> {
        let keyspaces = self.digest_keyspaces()?;
        task::spawn_blocking(move || {
            let Some(stored) = keyspaces.meta.get(TOMBSTONE_HORIZON_KEY).map_err(db_error)? else {
                return Ok(None);
            };
            let mut horizon = TombstoneHorizon::decode(&stored)?;
            if let Some(floor) =
                keyspaces.meta.get(TOMBSTONE_HORIZON_FLOOR_KEY).map_err(db_error)?
            {
                horizon = horizon.max(TombstoneHorizon::decode(&floor)?);
            }
            Ok(Some(horizon))
        })
        .await
        .map_err(db_error)?
    }

    /// Settles the tombstone horizon: the HLC from which every delete on this
    /// node left a tombstone.
    ///
    /// The first call decides it and later calls return what it decided. An
    /// index created by an open with digests, and kept by every open since,
    /// has no delete without a tombstone in its history, and its horizon is
    /// zero. Any other index may have one, even with no record left, and its
    /// horizon is at least `now`: the caller takes it from the cluster clock before the
    /// node serves any write, ahead of every HLC a peer may have issued
    /// before. An uncertain open forgets the settled guarantee but retains
    /// its lower bound: resettling takes their maximum even if the clock went
    /// backward ([`IndexOptions::version_digests`]).
    pub async fn settle_tombstone_horizon(
        &self,
        now: TombstoneHorizon,
    ) -> Result<TombstoneHorizon, StorageError> {
        let keyspaces = self.digest_keyspaces()?;
        let db = self.db.clone();
        task::spawn_blocking(move || {
            let stored = keyspaces
                .meta
                .get(TOMBSTONE_HORIZON_KEY)
                .map_err(db_error)?
                .map(|value| TombstoneHorizon::decode(&value))
                .transpose()?;
            let floor = keyspaces
                .meta
                .get(TOMBSTONE_HORIZON_FLOOR_KEY)
                .map_err(db_error)?
                .map(|value| TombstoneHorizon::decode(&value))
                .transpose()?;
            let horizon = stored
                .unwrap_or(
                    if keyspaces.meta.contains_key(BORN_WITH_DIGESTS_KEY).map_err(db_error)? {
                        TombstoneHorizon::ZERO
                    } else {
                        now
                    },
                )
                .max(floor.unwrap_or(TombstoneHorizon::ZERO));
            if stored == Some(horizon) && floor == Some(horizon) {
                return Ok(horizon);
            }
            let mut batch = db.batch();
            batch.insert(&keyspaces.meta, TOMBSTONE_HORIZON_KEY, horizon.encode());
            // Retain the floor permanently: earlier builds can reset the
            // settled field, but must not erase this lower bound.
            batch.insert(
                &keyspaces.meta,
                TOMBSTONE_HORIZON_FLOOR_KEY,
                horizon.encode(),
            );
            batch.commit().map_err(db_error)?;
            db.persist(PersistMode::SyncAll).map_err(db_error)?;
            Ok(horizon)
        })
        .await
        .map_err(db_error)?
    }

    /// Durably records an explicit start of a server in single mode.
    ///
    /// Called by single-server startup before it serves writes. An engine
    /// opened without digests for maintenance must not call this method:
    /// dropping digest history alone is not evidence of a single-server start.
    /// Cluster startup also calls this after preflight proved that no earlier
    /// cluster identity exists. The evidence lasts until migration completion,
    /// so an identity created by a failed bootstrap cannot bypass preflight.
    pub async fn mark_single_mode_open(&self) -> Result<(), StorageError> {
        let db = self.db.clone();
        task::spawn_blocking(move || {
            let meta = db
                .keyspace(CLUSTER_META_KEYSPACE, KeyspaceCreateOptions::default)
                .map_err(db_error)?;
            meta.insert(LAST_OPEN_SINGLE_KEY, [1]).map_err(db_error)?;
            db.persist(PersistMode::SyncAll).map_err(db_error)
        })
        .await
        .map_err(db_error)?
    }

    /// Durably registers a bounded scan page with one atomic batch and fsync.
    /// Existing obligations retain their original snapshots.
    pub async fn track_legacy_migration_page(
        &self,
        page: &[(String, IndexRecord)],
    ) -> Result<(), StorageError> {
        let keyspaces = self.digest_keyspaces()?;
        let db = self.db.clone();
        let entries = page
            .iter()
            .map(|(name, snapshot)| {
                let key = [LEGACY_MIGRATION_QUEUE_PREFIX, name.as_bytes()].concat();
                let value = bincode::serialize(snapshot)
                    .map_err(|e| StorageError::Serialization(e.to_string()))?;
                Ok((key, value))
            })
            .collect::<Result<Vec<_>, StorageError>>()?;
        task::spawn_blocking(move || {
            let mut batch = db.batch();
            for (key, value) in entries {
                if !keyspaces.meta.contains_key(&key).map_err(db_error)? {
                    batch.insert(&keyspaces.meta, key, value);
                }
            }
            if !batch.is_empty() {
                batch.commit().map_err(db_error)?;
                db.persist(PersistMode::SyncAll).map_err(db_error)?;
            }
            Ok(())
        })
        .await
        .map_err(db_error)?
    }

    /// Whether one key still owes confirmation from its owners.
    pub async fn legacy_migration_key_pending(&self, full_key: &str) -> Result<bool, StorageError> {
        let keyspaces = self.digest_keyspaces()?;
        let key = [LEGACY_MIGRATION_QUEUE_PREFIX, full_key.as_bytes()].concat();
        task::spawn_blocking(move || keyspaces.meta.contains_key(key).map_err(db_error))
            .await
            .map_err(db_error)?
    }

    /// A bounded page of outstanding migration obligations, including keys
    /// superseded by a client while some owner had not yet acknowledged.
    pub async fn legacy_migration_queue_page(
        &self,
        start_after: Option<&str>,
        max: usize,
    ) -> Result<Vec<(String, IndexRecord)>, StorageError> {
        let keyspaces = self.digest_keyspaces()?;
        let after = start_after.map(|key| [LEGACY_MIGRATION_QUEUE_PREFIX, key.as_bytes()].concat());
        task::spawn_blocking(move || {
            let mut page = Vec::new();
            if max == 0 {
                return Ok(page);
            }
            let start = after.clone().unwrap_or_else(|| LEGACY_MIGRATION_QUEUE_PREFIX.to_vec());
            for guard in keyspaces.meta.range(start..) {
                let (key, value) = guard.into_inner().map_err(db_error)?;
                if !key.starts_with(LEGACY_MIGRATION_QUEUE_PREFIX) {
                    break;
                }
                if after.as_ref().is_some_and(|after| key.as_ref() <= after.as_slice()) {
                    continue;
                }
                let name = std::str::from_utf8(&key[LEGACY_MIGRATION_QUEUE_PREFIX.len()..])
                    .map_err(|e| StorageError::Serialization(e.to_string()))?;
                let snapshot = bincode::deserialize(&value)
                    .map_err(|e| StorageError::Serialization(e.to_string()))?;
                page.push((name.to_string(), snapshot));
                if page.len() == max {
                    break;
                }
            }
            Ok(page)
        })
        .await
        .map_err(db_error)?
    }

    /// Durably removes an obligation after every current owner was confirmed.
    pub async fn finish_legacy_migration_key(&self, full_key: &str) -> Result<(), StorageError> {
        let keyspaces = self.digest_keyspaces()?;
        let db = self.db.clone();
        let key = [LEGACY_MIGRATION_QUEUE_PREFIX, full_key.as_bytes()].concat();
        task::spawn_blocking(move || {
            keyspaces.meta.remove(key).map_err(db_error)?;
            db.persist(PersistMode::SyncAll).map_err(db_error)
        })
        .await
        .map_err(db_error)?
    }

    /// A page of unversioned live records stored without an HLC.
    ///
    /// `markers` selects bucket markers; false selects user objects. System
    /// configurations, IAM, EC records, version history, tombstones and staged
    /// multipart state are excluded. Keys are full `{bucket}/{key}` keys in
    /// ascending order, strictly past `start_after`. A short page is the end.
    /// Reads at most one result page into memory; callers scan markers first.
    pub async fn legacy_migration_page(
        &self,
        markers: bool,
        start_after: Option<&str>,
        max_keys: usize,
    ) -> Result<Vec<(String, IndexRecord)>, StorageError> {
        if max_keys == 0 {
            return Ok(Vec::new());
        }
        let (ks, restore) = if markers {
            (self.buckets.clone(), "__system__/__s4_bucket_marker_")
        } else {
            (self.objects.clone(), "")
        };
        let after = start_after.map(|s| s.strip_prefix(restore).unwrap_or(s).as_bytes().to_vec());
        task::spawn_blocking(move || {
            let iter: Box<dyn Iterator<Item = fjall::Guard>> = match after {
                Some(after) => Box::new(ks.range((Bound::Excluded(after), Bound::Unbounded))),
                None => Box::new(ks.iter()),
            };
            let mut result = Vec::new();
            for guard in iter {
                let (key, value) = guard.into_inner().map_err(db_error)?;
                let key = std::str::from_utf8(&key).map_err(db_error)?;
                let full_key = format!("{restore}{key}");
                // Some objects-keyspace entries (e.g. old multipart state)
                // need not be IndexRecords. Reject system keys before decoding.
                if !markers && full_key.starts_with("__system__/") {
                    continue;
                }
                let record: IndexRecord = bincode::deserialize(&value).map_err(db_error)?;
                if super::migration::is_legacy_migration_record(&full_key, &record) {
                    result.push((full_key, record));
                    if result.len() == max_keys {
                        break;
                    }
                }
            }
            Ok(result)
        })
        .await
        .map_err(db_error)?
    }

    /// Whether the last complete legacy-object migration is still trusted.
    /// Single-mode opens, restore and opens without a matching digest seal
    /// invalidate the marker, so migration scans again after any such event.
    pub async fn legacy_migration_complete(&self) -> Result<bool, StorageError> {
        let keyspaces = self.digest_keyspaces()?;
        task::spawn_blocking(move || {
            keyspaces.meta.contains_key(LEGACY_MIGRATION_COMPLETE_KEY).map_err(db_error)
        })
        .await
        .map_err(db_error)?
    }

    /// Durably records completion after a caller scanned every migration page.
    /// The caller must finish both marker and object scans and resolve every
    /// candidate before recording completion, under the external write lock.
    pub async fn mark_legacy_migration_complete(&self) -> Result<(), StorageError> {
        let keyspaces = self.digest_keyspaces()?;
        let db = self.db.clone();
        task::spawn_blocking(move || {
            let mut batch = db.batch();
            batch.insert(&keyspaces.meta, LEGACY_MIGRATION_COMPLETE_KEY, [1]);
            batch.remove(&keyspaces.meta, LAST_OPEN_SINGLE_KEY);
            batch.commit().map_err(db_error)?;
            db.persist(PersistMode::SyncAll).map_err(db_error)
        })
        .await
        .map_err(db_error)?
    }

    /// Whether this node finished adopting the bucket configurations it
    /// stored before protocol 6 ([`mark_bucket_configs_adopted`](Self::mark_bucket_configs_adopted)),
    /// and its history has remained trusted ever since.
    pub async fn bucket_configs_adopted(&self) -> Result<bool, StorageError> {
        let keyspaces = self.digest_keyspaces()?;
        task::spawn_blocking(move || {
            keyspaces.meta.contains_key(BUCKET_CONFIGS_ADOPTED_KEY).map_err(db_error)
        })
        .await
        .map_err(db_error)?
    }

    /// Records, durably, that every bucket configuration this node stored
    /// before protocol 6 is on the replicas of its key. Opening the index
    /// without digests or without a matching seal forgets it.
    pub async fn mark_bucket_configs_adopted(&self) -> Result<(), StorageError> {
        let keyspaces = self.digest_keyspaces()?;
        let db = self.db.clone();
        task::spawn_blocking(move || {
            let mut batch = db.batch();
            batch.insert(&keyspaces.meta, BUCKET_CONFIGS_ADOPTED_KEY, [1]);
            batch.insert(&keyspaces.meta, BUCKET_CONFIG_HISTORY_CHECK_KEY, [1]);
            batch.commit().map_err(db_error)?;
            db.persist(PersistMode::SyncAll).map_err(db_error)
        })
        .await
        .map_err(db_error)?
    }

    /// Whether local configurations require confirmation of their owners'
    /// deletion history before readoption. Unlike adoption completion, this
    /// marker survives every restart and opens without digests.
    pub async fn bucket_config_history_requires_confirmation(&self) -> Result<bool, StorageError> {
        let keyspaces = self.digest_keyspaces()?;
        task::spawn_blocking(move || {
            keyspaces.meta.contains_key(BUCKET_CONFIG_HISTORY_CHECK_KEY).map_err(db_error)
        })
        .await
        .map_err(db_error)?
    }

    /// Whether this node saw every node of its pool adopt its bucket
    /// configurations ([`mark_bucket_config_quorum_reads`](Self::mark_bucket_config_quorum_reads)).
    /// This cutover survives uncertain history and opens without digests.
    pub async fn bucket_config_quorum_reads(&self) -> Result<bool, StorageError> {
        let keyspaces = self.digest_keyspaces()?;
        task::spawn_blocking(move || {
            keyspaces.meta.contains_key(BUCKET_CONFIG_QUORUM_READS_KEY).map_err(db_error)
        })
        .await
        .map_err(db_error)?
    }

    /// Records, durably, that every node of this node's pool adopted its
    /// bucket configurations: they are read through the quorums from now on,
    /// across restarts, including ones with uncertain deletion history.
    pub async fn mark_bucket_config_quorum_reads(&self) -> Result<(), StorageError> {
        let keyspaces = self.digest_keyspaces()?;
        let db = self.db.clone();
        task::spawn_blocking(move || {
            let mut batch = db.batch();
            batch.insert(&keyspaces.meta, BUCKET_CONFIG_QUORUM_READS_KEY, [1]);
            batch.insert(&keyspaces.meta, BUCKET_CONFIG_HISTORY_CHECK_KEY, [1]);
            batch.commit().map_err(db_error)?;
            db.persist(PersistMode::SyncAll).map_err(db_error)
        })
        .await
        .map_err(db_error)?
    }

    /// Whether this node saw every node of its pool hold no IAM of its own
    /// that the pool has not taken over ([`mark_iam_adopted`](Self::mark_iam_adopted)),
    /// and its history has remained trusted ever since.
    pub async fn iam_adopted(&self) -> Result<bool, StorageError> {
        let keyspaces = self.digest_keyspaces()?;
        task::spawn_blocking(move || keyspaces.meta.contains_key(IAM_ADOPTED_KEY).map_err(db_error))
            .await
            .map_err(db_error)?
    }

    /// Records, durably, that every node of this node's pool holds no IAM of
    /// its own the pool has not taken over: IAM is changed through the pool
    /// from now on, across restarts. Opening the index without digests or
    /// without a matching seal forgets it.
    pub async fn mark_iam_adopted(&self) -> Result<(), StorageError> {
        let keyspaces = self.digest_keyspaces()?;
        let db = self.db.clone();
        task::spawn_blocking(move || {
            keyspaces.meta.insert(IAM_ADOPTED_KEY, [1]).map_err(db_error)?;
            db.persist(PersistMode::SyncAll).map_err(db_error)
        })
        .await
        .map_err(db_error)?
    }

    /// The record of this node's place in the move of the pool's bucket
    /// metadata to its replication factor, as last stored
    /// ([`store_bucket_metadata_placement`](Self::store_bucket_metadata_placement)),
    /// or `None` when it never was.
    pub async fn bucket_metadata_placement(&self) -> Result<Option<Vec<u8>>, StorageError> {
        let keyspaces = self.digest_keyspaces()?;
        task::spawn_blocking(move || {
            Ok(keyspaces
                .meta
                .get(BUCKET_METADATA_PLACEMENT_KEY)
                .map_err(db_error)?
                .map(|value| value.to_vec()))
        })
        .await
        .map_err(db_error)?
    }

    /// Stores, durably, the record of this node's place in the move of the
    /// pool's bucket metadata. The caller keeps it from going backwards.
    pub async fn store_bucket_metadata_placement(
        &self,
        record: Vec<u8>,
    ) -> Result<(), StorageError> {
        let keyspaces = self.digest_keyspaces()?;
        let db = self.db.clone();
        task::spawn_blocking(move || {
            keyspaces.meta.insert(BUCKET_METADATA_PLACEMENT_KEY, record).map_err(db_error)?;
            db.persist(PersistMode::SyncAll).map_err(db_error)
        })
        .await
        .map_err(db_error)?
    }

    /// The opaque pool generation record, or none before its first cluster start.
    pub async fn pool_generation(&self) -> Result<Option<Vec<u8>>, StorageError> {
        let keyspaces = self.digest_keyspaces()?;
        task::spawn_blocking(move || {
            Ok(keyspaces.meta.get(POOL_GENERATION_KEY).map_err(db_error)?.map(|v| v.to_vec()))
        })
        .await
        .map_err(db_error)?
    }

    /// Stores pool membership durably. The cluster layer validates its evolution.
    pub async fn store_pool_generation(&self, record: Vec<u8>) -> Result<(), StorageError> {
        let keyspaces = self.digest_keyspaces()?;
        let db = self.db.clone();
        task::spawn_blocking(move || {
            keyspaces.meta.insert(POOL_GENERATION_KEY, record).map_err(db_error)?;
            db.persist(PersistMode::SyncAll).map_err(db_error)
        })
        .await
        .map_err(db_error)?
    }

    /// Every record of this node's progress in moving the pool's keys to a
    /// grown generation, by name, in name order
    /// ([`store_rebalance_entry`](Self::store_rebalance_entry)).
    pub async fn rebalance_entries(&self) -> Result<Vec<(String, Vec<u8>)>, StorageError> {
        let keyspaces = self.digest_keyspaces()?;
        task::spawn_blocking(move || {
            keyspaces
                .meta
                .prefix(REBALANCE_PREFIX)
                .map(|guard| {
                    let (key, value) = guard.into_inner().map_err(db_error)?;
                    let name = std::str::from_utf8(&key[REBALANCE_PREFIX.len()..])
                        .map_err(|e| StorageError::Serialization(e.to_string()))?;
                    Ok((name.to_string(), value.to_vec()))
                })
                .collect()
        })
        .await
        .map_err(db_error)?
    }

    /// Stores, durably, one record of this node's progress in moving the
    /// pool's keys. The caller keeps it from going backwards.
    pub async fn store_rebalance_entry(
        &self,
        name: &str,
        record: Vec<u8>,
    ) -> Result<(), StorageError> {
        let keyspaces = self.digest_keyspaces()?;
        let db = self.db.clone();
        let mut key = REBALANCE_PREFIX.to_vec();
        key.extend_from_slice(name.as_bytes());
        task::spawn_blocking(move || {
            keyspaces.meta.insert(key, record).map_err(db_error)?;
            db.persist(PersistMode::SyncAll).map_err(db_error)
        })
        .await
        .map_err(db_error)?
    }

    // ========================================================================
    // Tombstone scanning (Phase 8: Distributed GC)
    // ========================================================================

    /// Returns a clone of the `objects` keyspace handle.
    ///
    /// Used by distributed GC (Phase 8) to scan for tombstones and live
    /// objects without going through the string-prefix routing layer.
    /// A write through it goes around the version digests: write through the
    /// methods of `IndexDb`.
    pub fn objects_keyspace(&self) -> Keyspace {
        self.objects.clone()
    }

    /// The keys of every tombstone ([`IndexRecord::is_tombstone`]) in the
    /// objects and buckets keyspaces, in the form [`get`](Self::get) takes
    /// them: a bucket marker's with its `__system__/__s4_bucket_marker_`
    /// prefix restored.
    ///
    /// # Performance
    ///
    /// Full scan of both keyspaces; memory grows with the tombstones found,
    /// not with the keys scanned.
    pub async fn tombstone_keys(&self) -> Result<Vec<String>, StorageError> {
        let objects = self.objects.clone();
        let buckets = self.buckets.clone();

        task::spawn_blocking(move || {
            let mut keys = Vec::new();
            for (keyspace, restore_prefix) in
                [(&objects, ""), (&buckets, "__system__/__s4_bucket_marker_")]
            {
                for guard in keyspace.iter() {
                    let (key_bytes, value_bytes) =
                        guard.into_inner().map_err(|e| StorageError::Database(e.to_string()))?;
                    let record: IndexRecord = bincode::deserialize(&value_bytes)
                        .map_err(|e| StorageError::Serialization(e.to_string()))?;
                    if record.is_tombstone() {
                        let key = std::str::from_utf8(&key_bytes)
                            .map_err(|e| StorageError::Database(e.to_string()))?;
                        keys.push(format!("{restore_prefix}{key}"));
                    }
                }
            }
            Ok(keys)
        })
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?
    }

    /// Scans the objects keyspace and returns all live (non-tombstone) content hashes.
    ///
    /// Used by mark-sweep dedup GC (Phase 8) to identify which content hashes
    /// are still referenced by at least one live object.
    ///
    /// # Returns
    ///
    /// A set of SHA-256 content hashes (`[u8; 32]`) for all live objects.
    pub async fn scan_live_content_hashes(
        &self,
    ) -> Result<std::collections::HashSet<[u8; 32]>, StorageError> {
        let objects = self.objects.clone();

        task::spawn_blocking(move || {
            let mut live_hashes = std::collections::HashSet::new();

            for guard in objects.iter() {
                let (_key_bytes, value_bytes) =
                    guard.into_inner().map_err(|e| StorageError::Database(e.to_string()))?;

                let record: IndexRecord = bincode::deserialize(&value_bytes)
                    .map_err(|e| StorageError::Serialization(e.to_string()))?;

                if !record.is_delete_marker && record.content_hash != [0u8; 32] {
                    live_hashes.insert(record.content_hash);
                }
            }

            Ok(live_hashes)
        })
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?
    }

    // ========================================================================
    // Compaction support
    // ========================================================================

    /// Collects all live content references from the objects and IAM keyspaces.
    ///
    /// Returns:
    /// - `content_hashes`: SHA-256 hashes of all non-composite, non-inline,
    ///   non-delete-marker records. These correspond to dedup entries.
    /// - `manifest_blob_ids`: [`BlobId`]s of all composite (multipart) manifests.
    ///   These correspond to blob_ref entries (segments are resolved later).
    ///
    /// Used by the compactor to cross-reference against dedup/blob_ref keyspaces
    /// and purge orphaned entries.
    pub async fn scan_all_content_refs(
        &self,
    ) -> Result<(HashSet<[u8; 32]>, HashSet<BlobId>), StorageError> {
        let objects = self.objects.clone();
        let buckets = self.buckets.clone();
        let iam = self.iam.clone();

        task::spawn_blocking(move || {
            let mut content_hashes = HashSet::new();
            let mut manifest_blob_ids = HashSet::new();

            let scan_ks = |ks: &Keyspace,
                           content_hashes: &mut HashSet<[u8; 32]>,
                           manifest_blob_ids: &mut HashSet<BlobId>|
             -> Result<(), StorageError> {
                for guard in ks.iter() {
                    let (_key_bytes, value_bytes) =
                        guard.into_inner().map_err(|e| StorageError::Database(e.to_string()))?;

                    let record: IndexRecord = bincode::deserialize(&value_bytes)
                        .map_err(|e| StorageError::Serialization(e.to_string()))?;

                    if record.is_delete_marker {
                        continue;
                    }

                    // An erasure-coded record is a namespace entry whose bytes
                    // live in a pool, not in a volume. Counting its content hash
                    // as a live reference would pin the hot blob it no longer
                    // owns, and compaction would never reclaim the space the
                    // migration was performed to free.
                    if record.is_erasure_coded() {
                        continue;
                    }

                    if record.is_composite() {
                        if let Some(blob_id) = record.composite_manifest_blob_id() {
                            manifest_blob_ids.insert(blob_id);
                        }
                    } else if record.file_id != u32::MAX && record.content_hash != [0u8; 32] {
                        content_hashes.insert(record.content_hash);
                    }
                }
                Ok(())
            };

            scan_ks(&objects, &mut content_hashes, &mut manifest_blob_ids)?;
            scan_ks(&buckets, &mut content_hashes, &mut manifest_blob_ids)?;
            scan_ks(&iam, &mut content_hashes, &mut manifest_blob_ids)?;

            Ok((content_hashes, manifest_blob_ids))
        })
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?
    }

    /// Where the live records of the index keep their data in the volumes:
    /// each plain object's blob by location, with its content hash, and
    /// each composite object's manifest by blob id, with its location.
    ///
    /// The compactor keeps these whatever the reference counts say: a count
    /// that drifted low must not cost a live object its bytes. Delete
    /// markers, erasure-coded entries and inline objects keep no data in a
    /// volume.
    pub async fn scan_live_data_refs(&self) -> Result<LiveDataRefs, StorageError> {
        let keyspaces = [self.objects.clone(), self.buckets.clone(), self.iam.clone()];
        task::spawn_blocking(move || {
            let mut refs = LiveDataRefs::default();
            for keyspace in &keyspaces {
                for guard in keyspace.iter() {
                    let (_, value) = guard.into_inner().map_err(db_error)?;
                    let record: IndexRecord = bincode::deserialize(&value)
                        .map_err(|e| StorageError::Serialization(e.to_string()))?;
                    if record.is_delete_marker
                        || record.is_erasure_coded()
                        || record.file_id == u32::MAX
                        || record.file_id == crate::types::EXTERNAL_DATA_FILE_ID
                    {
                        continue;
                    }
                    let location = (record.file_id, record.offset);
                    match record.composite_manifest_blob_id() {
                        Some(manifest) => {
                            refs.manifests.insert(manifest, location);
                        }
                        None => {
                            refs.blobs.insert(location, record.content_hash);
                        }
                    }
                }
            }
            Ok(refs)
        })
        .await
        .map_err(db_error)?
    }

    /// Scans the objects keyspace and returns all records stored in the given volume.
    ///
    /// Returns `(keyspace_id, stripped_key, IndexRecord)` tuples where
    /// `record.file_id == volume_id`.
    /// Inline objects (`file_id == u32::MAX`) are always excluded.
    ///
    /// Scans **all** keyspaces that store [`IndexRecord`]s pointing to volume
    /// blobs — `objects`, `buckets` (the bucket markers) and `iam`, the same
    /// ones [`Self::scan_live_data_refs`] keeps alive: a blob kept alive whose
    /// record is not moved with it is lost with its old volume. The returned [`KeyspaceId`]
    /// tells the caller which keyspace the record belongs to so that updates
    /// are written back to the correct keyspace.
    ///
    /// Used by the compactor to find which index records need updating after
    /// relocating blobs from one volume to another.
    ///
    /// # Performance
    ///
    /// Full keyspace scan over objects, buckets and iam. Call from a blocking thread for
    /// large datasets.
    pub async fn scan_objects_by_volume(
        &self,
        volume_id: u32,
    ) -> Result<Vec<(KeyspaceId, String, IndexRecord)>, StorageError> {
        let objects = self.objects.clone();
        let buckets = self.buckets.clone();
        let iam = self.iam.clone();

        task::spawn_blocking(move || {
            let mut results = Vec::new();

            // Helper: scan a single keyspace, tagging each result with its KeyspaceId.
            let scan_ks = |ks: &Keyspace,
                           ks_id: KeyspaceId,
                           results: &mut Vec<(KeyspaceId, String, IndexRecord)>|
             -> Result<(), StorageError> {
                for guard in ks.iter() {
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

                    results.push((ks_id, key_str, record));
                }
                Ok(())
            };

            scan_ks(&objects, KeyspaceId::Objects, &mut results)?;
            scan_ks(&buckets, KeyspaceId::Buckets, &mut results)?;
            scan_ks(&iam, KeyspaceId::Iam, &mut results)?;

            Ok(results)
        })
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?
    }
}

/// What the live records of an index keep in the volumes
/// ([`IndexDb::scan_live_data_refs`]).
#[derive(Debug, Default)]
pub struct LiveDataRefs {
    /// Each plain object's blob: its location `(volume_id, offset)` and its
    /// content hash.
    pub blobs: std::collections::HashMap<(u32, u64), [u8; 32]>,
    /// Each composite object's manifest blob, with its location.
    pub manifests: std::collections::HashMap<BlobId, (u32, u64)>,
}

/// The reference counts a batch changes, by keyspace and key, as the
/// actions of the batch leave them: `None` for a removed entry.
#[derive(Default)]
struct PendingCounts {
    values: BTreeMap<(KeyspaceId, Vec<u8>), Option<Vec<u8>>>,
}

impl PendingCounts {
    /// The entry of `key` as the batch stands: what an earlier action of the
    /// batch left, or what `keyspace` holds.
    fn get(
        &self,
        id: KeyspaceId,
        keyspace: &Keyspace,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, StorageError> {
        match self.values.get(&(id, key.to_vec())) {
            Some(value) => Ok(value.clone()),
            None => Ok(keyspace.get(key).map_err(db_error)?.map(|value| value.to_vec())),
        }
    }

    fn set(&mut self, id: KeyspaceId, key: &[u8], value: Option<Vec<u8>>) {
        self.values.insert((id, key.to_vec()), value);
    }
}

/// `count` changed by `delta`, never below zero or past `u32::MAX`.
fn adjust(count: u32, delta: i64) -> u32 {
    (i64::from(count) + delta).clamp(0, i64::from(u32::MAX)) as u32
}

/// A blob id as an error names it.
fn hex_id(blob_id: &[u8]) -> String {
    blob_id.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    /// Two changes of one count in one batch both count: each reads what the
    /// one before it left, not the value from before the batch.
    #[tokio::test]
    async fn two_changes_of_one_count_in_a_batch_both_count() {
        let temp_dir = TempDir::new().unwrap();
        let index_db = IndexDb::new(&temp_dir.path().join("test_db")).unwrap();
        let increment = || BatchOp {
            keyspace: KeyspaceId::Dedup,
            action: BatchAction::IncrementDedupRef {
                key: b"hash".to_vec(),
                volume_id: 1,
                offset: 0,
            },
        };
        let blob = |committed, staged| BatchOp {
            keyspace: KeyspaceId::BlobRefs,
            action: BatchAction::AdjustBlobRef {
                blob_id: b"blob".to_vec(),
                committed,
                staged,
                location: Some((1, 0)),
            },
        };
        index_db
            .batch_write(vec![
                increment(),
                increment(),
                blob(0, 1),
                blob(1, -1),
                blob(1, 0),
            ])
            .await
            .unwrap();

        let dedup: DedupEntry =
            bincode::deserialize(&index_db.dedup.get(b"hash").unwrap().unwrap()).unwrap();
        assert_eq!(dedup.ref_count, 2);
        let blob_ref: BlobRefEntry =
            bincode::deserialize(&index_db.blob_refs.get(b"blob").unwrap().unwrap()).unwrap();
        assert_eq!(
            (blob_ref.ref_count_committed, blob_ref.ref_count_staged),
            (2, 0)
        );

        // A count the blob is expected to have, and has not, is an error
        let missing = BatchOp {
            keyspace: KeyspaceId::BlobRefs,
            action: BatchAction::AdjustBlobRef {
                blob_id: b"missing".to_vec(),
                committed: 1,
                staged: 0,
                location: None,
            },
        };
        assert!(matches!(
            index_db.batch_write(vec![missing]).await,
            Err(StorageError::BlobRefNotFound { .. })
        ));
    }

    /// A scan after a key returns the keys of the prefix past it, wherever
    /// that key lies: inside the prefix, before it, past it, or with no
    /// prefix at all.
    #[tokio::test]
    async fn a_scan_resumes_right_after_its_start_key() {
        let temp_dir = TempDir::new().unwrap();
        let index_db =
            IndexDb::open(&temp_dir.path().join("test_db"), IndexOptions::default()).unwrap();
        let record = IndexRecord::new(
            1,
            0,
            4,
            [1; 32],
            "etag".to_string(),
            "text/plain".to_string(),
        );
        for key in ["a/1", "a/2", "a/3", "ab/1", "b/1"] {
            index_db.put(key, &record).await.unwrap();
        }
        let scan = |prefix: &'static str, start_after: Option<&'static str>| {
            let index_db = &index_db;
            async move {
                index_db
                    .list_after(prefix, start_after, 10)
                    .await
                    .unwrap()
                    .into_iter()
                    .map(|(key, _)| key)
                    .collect::<Vec<_>>()
            }
        };

        assert_eq!(scan("a/", Some("a/1")).await, ["a/2", "a/3"]);
        assert_eq!(scan("a/", Some("a/15")).await, ["a/2", "a/3"]);
        assert_eq!(scan("a/", Some("0")).await, ["a/1", "a/2", "a/3"]);
        assert_eq!(scan("a/", Some("a/3")).await, Vec::<String>::new());
        assert_eq!(scan("a/", Some("aa")).await, Vec::<String>::new());
        assert_eq!(scan("", Some("a/3")).await, ["ab/1", "b/1"]);
        assert_eq!(scan("a", Some("a/2")).await, ["a/3", "ab/1"]);
    }

    /// The paths a batch of the storage engine does not take keep the digests
    /// too: single puts and deletes (journal replay, recovery, GC) in both
    /// keyspaces that have digests, and a restore, which bypasses them.
    #[tokio::test]
    async fn single_writes_and_restores_keep_the_version_digests() {
        let temp_dir = TempDir::new().unwrap();
        let index_db = IndexDb::open(
            &temp_dir.path().join("test_db"),
            IndexOptions {
                version_digests: true,
            },
        )
        .unwrap();
        let at = |wall: u64| {
            let mut record = IndexRecord::new(
                1,
                0,
                4,
                [wall as u8; 32],
                "etag".to_string(),
                "text/plain".to_string(),
            );
            record.hlc_timestamp = wall;
            record
        };
        let consistent = |check: VersionDigestCheck| {
            assert!(check.is_consistent(), "{check:?}");
            check.stored
        };

        index_db.put("photos/a", &at(1)).await.unwrap();
        index_db.put("photos/b", &at(2)).await.unwrap();
        index_db.put("photos/a", &at(3)).await.unwrap();
        index_db.put("__system__/__s4_bucket_marker_photos", &at(4)).await.unwrap();
        index_db.put("__system__/cors/photos", &at(5)).await.unwrap();
        index_db.put("__system__/__s4_iam_user_admin", &at(6)).await.unwrap();
        assert_eq!(
            consistent(index_db.verify_version_digests().await.unwrap()),
            3
        );

        index_db.delete("photos/b").await.unwrap();
        index_db.delete("__system__/__s4_bucket_marker_photos").await.unwrap();
        assert_eq!(
            consistent(index_db.verify_version_digests().await.unwrap()),
            1
        );

        let snapshot = index_db.dump_keyspaces().await.unwrap();
        index_db.put("photos/c", &at(7)).await.unwrap();
        index_db.restore_keyspaces(snapshot).await.unwrap();
        assert_eq!(
            consistent(index_db.verify_version_digests().await.unwrap()),
            1,
            "the restore brought back the index without photos/c"
        );
        let digests = index_db.version_digests().await.unwrap();
        assert_eq!(digests[0].key, "photos/a");
        assert_eq!(digests[0].digest.hlc_timestamp, 3);
    }

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
        assert_eq!(vol1_results[0].0, KeyspaceId::Objects);
        assert_eq!(vol1_results[0].1, "bucket/obj3");

        // IAM record in volume 1 — should also be found
        let iam_rec = crate::types::IndexRecord::new(
            1,
            500,
            200,
            [5u8; 32],
            "etag5".to_string(),
            "application/json".to_string(),
        );
        // route_key routes __system__/__s4_iam_* to the iam keyspace
        index_db.put("__system__/__s4_iam_user_abc123", &iam_rec).await.unwrap();

        // Scan volume 1 — should now get 2 results (1 object + 1 iam)
        let vol1_results = index_db.scan_objects_by_volume(1).await.unwrap();
        assert_eq!(vol1_results.len(), 2);

        // Verify both keyspaces are represented
        let has_objects = vol1_results.iter().any(|(ks, _, _)| *ks == KeyspaceId::Objects);
        let has_iam = vol1_results.iter().any(|(ks, _, _)| *ks == KeyspaceId::Iam);
        assert!(has_objects, "should find object record in volume 1");
        assert!(has_iam, "should find IAM record in volume 1");

        // IAM key should be stripped (no __system__/__s4_iam_ prefix)
        let iam_entry = vol1_results.iter().find(|(ks, _, _)| *ks == KeyspaceId::Iam).unwrap();
        assert_eq!(iam_entry.1, "user_abc123");

        // Scan volume 999 — should get 0 results
        let empty = index_db.scan_objects_by_volume(999).await.unwrap();
        assert!(empty.is_empty());
    }

    // ================================================================
    // RMW batch action tests
    // ================================================================

    #[tokio::test]
    async fn test_increment_dedup_ref_creates_new_entry() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db");
        let index_db = IndexDb::new(&db_path).unwrap();

        let key = vec![0u8; 36]; // PG(4) + hash(32)
        let ops = vec![BatchOp {
            keyspace: KeyspaceId::Dedup,
            action: BatchAction::IncrementDedupRef {
                key: key.clone(),
                volume_id: 1,
                offset: 100,
            },
        }];
        index_db.batch_write(ops).await.unwrap();

        let raw = index_db.dedup_keyspace().get(&key).unwrap().unwrap();
        let entry: DedupEntry = bincode::deserialize(&raw).unwrap();
        assert_eq!(entry.ref_count, 1);
        assert_eq!(entry.volume_id, 1);
        assert_eq!(entry.offset, 100);
    }

    #[tokio::test]
    async fn test_increment_dedup_ref_increments_existing() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db");
        let index_db = IndexDb::new(&db_path).unwrap();

        let key = vec![0u8; 36];
        // Create entry
        let ops = vec![BatchOp {
            keyspace: KeyspaceId::Dedup,
            action: BatchAction::IncrementDedupRef {
                key: key.clone(),
                volume_id: 1,
                offset: 100,
            },
        }];
        index_db.batch_write(ops).await.unwrap();

        // Increment again
        let ops = vec![BatchOp {
            keyspace: KeyspaceId::Dedup,
            action: BatchAction::IncrementDedupRef {
                key: key.clone(),
                volume_id: 1,
                offset: 100,
            },
        }];
        index_db.batch_write(ops).await.unwrap();

        let raw = index_db.dedup_keyspace().get(&key).unwrap().unwrap();
        let entry: DedupEntry = bincode::deserialize(&raw).unwrap();
        assert_eq!(entry.ref_count, 2);
    }

    #[tokio::test]
    async fn test_decrement_dedup_ref_decrements() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db");
        let index_db = IndexDb::new(&db_path).unwrap();

        let key = vec![0u8; 36];
        // Create with ref_count=2 via two increments
        for _ in 0..2 {
            index_db
                .batch_write(vec![BatchOp {
                    keyspace: KeyspaceId::Dedup,
                    action: BatchAction::IncrementDedupRef {
                        key: key.clone(),
                        volume_id: 1,
                        offset: 100,
                    },
                }])
                .await
                .unwrap();
        }

        // Decrement once
        index_db
            .batch_write(vec![BatchOp {
                keyspace: KeyspaceId::Dedup,
                action: BatchAction::DecrementDedupRef { key: key.clone() },
            }])
            .await
            .unwrap();

        let raw = index_db.dedup_keyspace().get(&key).unwrap().unwrap();
        let entry: DedupEntry = bincode::deserialize(&raw).unwrap();
        assert_eq!(entry.ref_count, 1);
    }

    #[tokio::test]
    async fn test_decrement_dedup_ref_removes_at_zero() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db");
        let index_db = IndexDb::new(&db_path).unwrap();

        let key = vec![0u8; 36];
        // Create with ref_count=1
        index_db
            .batch_write(vec![BatchOp {
                keyspace: KeyspaceId::Dedup,
                action: BatchAction::IncrementDedupRef {
                    key: key.clone(),
                    volume_id: 1,
                    offset: 100,
                },
            }])
            .await
            .unwrap();

        // Decrement to 0 — entry should be removed
        index_db
            .batch_write(vec![BatchOp {
                keyspace: KeyspaceId::Dedup,
                action: BatchAction::DecrementDedupRef { key: key.clone() },
            }])
            .await
            .unwrap();

        assert!(index_db.dedup_keyspace().get(&key).unwrap().is_none());
    }

    #[tokio::test]
    async fn test_decrement_dedup_ref_noop_on_missing() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db");
        let index_db = IndexDb::new(&db_path).unwrap();

        let key = vec![0u8; 36];
        // Decrement non-existent entry — should be no-op, not error
        index_db
            .batch_write(vec![BatchOp {
                keyspace: KeyspaceId::Dedup,
                action: BatchAction::DecrementDedupRef { key },
            }])
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_update_dedup_location() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db");
        let index_db = IndexDb::new(&db_path).unwrap();

        let key = vec![0u8; 36];
        // Create entry with ref_count=3
        for _ in 0..3 {
            index_db
                .batch_write(vec![BatchOp {
                    keyspace: KeyspaceId::Dedup,
                    action: BatchAction::IncrementDedupRef {
                        key: key.clone(),
                        volume_id: 1,
                        offset: 100,
                    },
                }])
                .await
                .unwrap();
        }

        // Update location
        index_db
            .batch_write(vec![BatchOp {
                keyspace: KeyspaceId::Dedup,
                action: BatchAction::UpdateDedupLocation {
                    key: key.clone(),
                    new_volume_id: 5,
                    new_offset: 999,
                },
            }])
            .await
            .unwrap();

        let raw = index_db.dedup_keyspace().get(&key).unwrap().unwrap();
        let entry: DedupEntry = bincode::deserialize(&raw).unwrap();
        assert_eq!(entry.volume_id, 5);
        assert_eq!(entry.offset, 999);
        assert_eq!(entry.ref_count, 3); // preserved
    }

    #[tokio::test]
    async fn test_update_dedup_location_skips_missing() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db");
        let index_db = IndexDb::new(&db_path).unwrap();

        let key = vec![0u8; 36];
        let result = index_db
            .batch_write(vec![BatchOp {
                keyspace: KeyspaceId::Dedup,
                action: BatchAction::UpdateDedupLocation {
                    key,
                    new_volume_id: 5,
                    new_offset: 999,
                },
            }])
            .await;

        assert!(result.is_ok());
    }

    /// Regression test: a missing dedup entry must NOT kill the entire batch.
    ///
    /// During compaction, a concurrent delete_object() can remove a dedup entry
    /// between blob relocation and the atomic batch commit. The fix (PR #7)
    /// gracefully skips the missing dedup entry so that all other operations
    /// in the batch — especially IndexRecord relocations — still succeed.
    ///
    /// If someone reverts the `if let Some` back to `ok_or_else(|| Error)?`
    /// in UpdateDedupLocation handling, this test will fail.
    #[tokio::test]
    async fn test_missing_dedup_entry_does_not_kill_batch() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db");
        let index_db = IndexDb::new(&db_path).unwrap();

        // Pre-create an IndexRecord (simulates an object living in old volume 1)
        let record = crate::types::IndexRecord::new(
            1,   // old volume
            100, // old offset
            50,
            [0u8; 32],
            "etag".to_string(),
            "text/plain".to_string(),
        );
        index_db.put("bucket/important-object", &record).await.unwrap();

        // Build a batch that the compactor would create:
        // 1. Update the IndexRecord to point to new volume (critical!)
        // 2. Update a dedup entry location (for a different blob whose
        //    dedup entry was concurrently deleted)
        let mut updated_record = record.clone();
        updated_record.file_id = 5; // new volume
        updated_record.offset = 200; // new offset
        let record_bytes = bincode::serialize(&updated_record).unwrap();

        let missing_dedup_key = vec![99u8; 36]; // does not exist

        let ops = vec![
            // Op 1: Relocate IndexRecord to new volume (must succeed!)
            BatchOp {
                keyspace: KeyspaceId::Objects,
                action: BatchAction::Put(b"bucket/important-object".to_vec(), record_bytes),
            },
            // Op 2: Update dedup location for a concurrently-deleted entry
            BatchOp {
                keyspace: KeyspaceId::Dedup,
                action: BatchAction::UpdateDedupLocation {
                    key: missing_dedup_key,
                    new_volume_id: 5,
                    new_offset: 300,
                },
            },
        ];

        let result = index_db.batch_write(ops).await;

        // The batch must SUCCEED — missing dedup entry is gracefully skipped
        assert!(
            result.is_ok(),
            "batch_write must not fail on missing dedup entry (would cause data loss)"
        );

        // CRITICAL: The IndexRecord MUST be updated to the new volume.
        // If this fails, a revert of the fix has reintroduced the data loss bug:
        // the old volume gets deleted and this object becomes inaccessible.
        let obj = index_db.get("bucket/important-object").await.unwrap().unwrap();
        assert_eq!(
            obj.file_id, 5,
            "IndexRecord must point to NEW volume after compaction batch"
        );
        assert_eq!(
            obj.offset, 200,
            "IndexRecord must have NEW offset after compaction batch"
        );
    }

    #[tokio::test]
    async fn test_mixed_rmw_and_plain_ops_in_single_batch() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db");
        let index_db = IndexDb::new(&db_path).unwrap();

        let dedup_key = vec![0u8; 36];
        let record = crate::types::IndexRecord::new(
            1,
            100,
            50,
            [0u8; 32],
            "etag".to_string(),
            "text/plain".to_string(),
        );
        let record_bytes = bincode::serialize(&record).unwrap();

        let ops = vec![
            BatchOp {
                keyspace: KeyspaceId::Objects,
                action: BatchAction::Put(b"bucket/key".to_vec(), record_bytes),
            },
            BatchOp {
                keyspace: KeyspaceId::Dedup,
                action: BatchAction::IncrementDedupRef {
                    key: dedup_key.clone(),
                    volume_id: 1,
                    offset: 100,
                },
            },
        ];
        index_db.batch_write(ops).await.unwrap();

        // Both should succeed atomically
        let obj = index_db.get("bucket/key").await.unwrap();
        assert!(obj.is_some());

        let raw = index_db.dedup_keyspace().get(&dedup_key).unwrap().unwrap();
        let entry: DedupEntry = bincode::deserialize(&raw).unwrap();
        assert_eq!(entry.ref_count, 1);
    }

    #[tokio::test]
    async fn test_concurrent_increment_no_lost_updates() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db");
        let index_db = std::sync::Arc::new(IndexDb::new(&db_path).unwrap());

        let key = vec![0u8; 36];
        let num_tasks = 50;

        let mut handles = Vec::new();
        for _ in 0..num_tasks {
            let db = index_db.clone();
            let k = key.clone();
            handles.push(tokio::spawn(async move {
                db.batch_write(vec![BatchOp {
                    keyspace: KeyspaceId::Dedup,
                    action: BatchAction::IncrementDedupRef {
                        key: k,
                        volume_id: 1,
                        offset: 100,
                    },
                }])
                .await
                .unwrap();
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        let raw = index_db.dedup_keyspace().get(&key).unwrap().unwrap();
        let entry: DedupEntry = bincode::deserialize(&raw).unwrap();
        assert_eq!(
            entry.ref_count, num_tasks,
            "Expected {} increments, got {} — lost update detected",
            num_tasks, entry.ref_count
        );
    }

    #[tokio::test]
    async fn test_concurrent_increment_and_decrement_consistency() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db");
        let index_db = std::sync::Arc::new(IndexDb::new(&db_path).unwrap());

        let key = vec![0u8; 36];
        let increments = 80u32;
        let decrements = 30u32;
        let expected = increments - decrements;

        let mut handles = Vec::new();

        for _ in 0..increments {
            let db = index_db.clone();
            let k = key.clone();
            handles.push(tokio::spawn(async move {
                db.batch_write(vec![BatchOp {
                    keyspace: KeyspaceId::Dedup,
                    action: BatchAction::IncrementDedupRef {
                        key: k,
                        volume_id: 1,
                        offset: 100,
                    },
                }])
                .await
                .unwrap();
            }));
        }

        // Wait for all increments first to ensure entry exists
        for h in handles {
            h.await.unwrap();
        }

        let mut dec_handles = Vec::new();
        for _ in 0..decrements {
            let db = index_db.clone();
            let k = key.clone();
            dec_handles.push(tokio::spawn(async move {
                db.batch_write(vec![BatchOp {
                    keyspace: KeyspaceId::Dedup,
                    action: BatchAction::DecrementDedupRef { key: k },
                }])
                .await
                .unwrap();
            }));
        }

        for h in dec_handles {
            h.await.unwrap();
        }

        let raw = index_db.dedup_keyspace().get(&key).unwrap().unwrap();
        let entry: DedupEntry = bincode::deserialize(&raw).unwrap();
        assert_eq!(
            entry.ref_count, expected,
            "Expected ref_count={}, got {} — lost update detected",
            expected, entry.ref_count
        );
    }

    #[tokio::test]
    async fn test_count_inline_objects() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db");
        let index_db = IndexDb::new(&db_path).unwrap();

        // No objects — count should be 0.
        assert_eq!(index_db.count_inline_objects().await.unwrap(), 0);

        // Insert a regular (volume-backed) object.
        let regular = crate::types::IndexRecord::new(
            1,
            100,
            50,
            [0u8; 32],
            "etag".to_string(),
            "text/plain".to_string(),
        );
        index_db.put("bucket/regular", &regular).await.unwrap();
        assert_eq!(index_db.count_inline_objects().await.unwrap(), 0);

        // Insert an inline object (file_id == u32::MAX).
        let inline = crate::types::IndexRecord::new(
            u32::MAX,
            0,
            10,
            [1u8; 32],
            "etag2".to_string(),
            "text/plain".to_string(),
        );
        index_db.put("bucket/inline1", &inline).await.unwrap();
        assert_eq!(index_db.count_inline_objects().await.unwrap(), 1);

        // Insert another inline object.
        let inline2 = crate::types::IndexRecord::new(
            u32::MAX,
            0,
            20,
            [2u8; 32],
            "etag3".to_string(),
            "text/plain".to_string(),
        );
        index_db.put("bucket/inline2", &inline2).await.unwrap();
        assert_eq!(index_db.count_inline_objects().await.unwrap(), 2);
    }

    #[test]
    fn test_compact_journal_all_empty() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db");
        let index_db = IndexDb::new(&db_path).unwrap();

        // Compact empty journal — should return 0.
        let removed = index_db.compact_journal_all().unwrap();
        assert_eq!(removed, 0);
    }

    #[test]
    fn test_compact_journal_all_clears_entries() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db");
        let index_db = IndexDb::new(&db_path).unwrap();

        // Insert entries directly into the journal keyspace.
        let journal_ks = index_db.journal_keyspace();
        for i in 0u64..5 {
            journal_ks.insert(i.to_be_bytes(), b"test_entry").unwrap();
        }
        assert!(!journal_ks.is_empty().unwrap());

        // Compact — should clear all entries.
        let removed = index_db.compact_journal_all().unwrap();
        assert!(removed > 0);
        assert!(journal_ks.is_empty().unwrap());
    }

    #[test]
    fn test_persist_does_not_error() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db");
        let index_db = IndexDb::new(&db_path).unwrap();

        // persist() should succeed even with no pending data.
        index_db.persist().unwrap();
    }
}
