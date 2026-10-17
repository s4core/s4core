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

//! Exact-generation primitives for migrating single-node objects to a pool.

use super::{BatchAction, BatchOp, BitcaskStorageEngine, JournalEventType};
use crate::types::{BUCKET_MARKER_KEY_PREFIX, SYSTEM_BUCKET};
use crate::{IndexRecord, StorageError};

pub(crate) fn is_legacy_migration_record(full_key: &str, record: &IndexRecord) -> bool {
    let Some((bucket, key)) = full_key.split_once('/') else {
        return false;
    };
    record.hlc_timestamp == 0
        && record.hlc_logical == 0
        && record.origin_node_id == 0
        && record.version_id.is_none()
        && !record.is_delete_marker
        && !record.is_erasure_coded()
        && (bucket != SYSTEM_BUCKET || key.starts_with(BUCKET_MARKER_KEY_PREFIX))
}

impl BitcaskStorageEngine {
    /// Checks a stopped single-node directory before its cluster engine opens.
    ///
    /// This opens only existing fjall keyspaces: WAL recovery is allowed, but
    /// no records, history markers, identity or digests are written. A prior
    /// cluster directory is exempt unless it was subsequently opened in single
    /// mode. `has_cluster_identity` must describe its persisted node identity.
    /// Versioning Enabled/Suspended and existing single data in an EC pool are
    /// refused. Missing and empty metadata directories are allowed.
    pub async fn preflight_cluster_transition(
        volumes: &std::path::Path,
        metadata: &std::path::Path,
        pool_name: &str,
        erasure_coded: bool,
        has_cluster_identity: bool,
    ) -> Result<(), StorageError> {
        if !metadata.exists()
            || std::fs::read_dir(metadata).map_err(StorageError::Io)?.next().is_none()
        {
            return Ok(());
        }
        let metadata = metadata.to_path_buf();
        let (configs, has_legacy) = tokio::task::spawn_blocking(move || {
            let db = fjall::Database::builder(&metadata)
                .open()
                .map_err(|e| StorageError::Database(e.to_string()))?;
            let open = |name: &str| {
                db.keyspace(name, fjall::KeyspaceCreateOptions::default)
                    .map_err(|e| StorageError::Database(e.to_string()))
            };
            let pending = db.keyspace_exists(super::index::CLUSTER_META_KEYSPACE)
                && open(super::index::CLUSTER_META_KEYSPACE)?
                    .prefix(super::index::LEGACY_MIGRATION_QUEUE_PREFIX)
                    .next()
                    .is_some();
            if has_cluster_identity {
                let single = db.keyspace_exists(super::index::CLUSTER_META_KEYSPACE)
                    && open(super::index::CLUSTER_META_KEYSPACE)?
                        .contains_key(super::index::LAST_OPEN_SINGLE_KEY)
                        .map_err(|e| StorageError::Database(e.to_string()))?;
                if !single {
                    return Ok((Vec::new(), pending));
                }
            }
            let mut configs = Vec::new();
            let mut has_legacy = pending;
            let versioning_prefix = format!(
                "{SYSTEM_BUCKET}/{}",
                crate::types::VERSIONING_CONFIG_KEY_PREFIX
            );
            // Match IndexDb::open: the retained legacy keyspace is a source
            // only while objects is empty. After migration it can contain
            // obsolete configurations and point at blobs compaction removed.
            let legacy_source = if db.keyspace_exists("index")
                && (!db.keyspace_exists("objects")
                    || open("objects")?
                        .is_empty()
                        .map_err(|e| StorageError::Database(e.to_string()))?)
            {
                Some(open("index")?)
            } else {
                None
            };
            let object_source = if legacy_source.is_some() {
                "index"
            } else {
                "objects"
            };
            for (name, restore) in [
                (object_source, ""),
                ("buckets", "__system__/__s4_bucket_marker_"),
            ] {
                if !db.keyspace_exists(name) {
                    continue;
                }
                let ks = open(name)?;
                if name != "buckets" {
                    for guard in ks.prefix(&versioning_prefix) {
                        let (key, value) = guard
                            .into_inner()
                            .map_err(|e| StorageError::Database(e.to_string()))?;
                        let full_key = std::str::from_utf8(&key)
                            .map_err(|e| StorageError::Serialization(e.to_string()))?;
                        let record: IndexRecord = bincode::deserialize(&value)
                            .map_err(|e| StorageError::Serialization(e.to_string()))?;
                        if !record.is_delete_marker {
                            configs.push((full_key[versioning_prefix.len()..].to_string(), record));
                        }
                    }
                }
                if has_legacy {
                    continue;
                }
                for guard in ks.iter() {
                    let (key, value) =
                        guard.into_inner().map_err(|e| StorageError::Database(e.to_string()))?;
                    let full_key = format!(
                        "{restore}{}",
                        std::str::from_utf8(&key)
                            .map_err(|e| StorageError::Serialization(e.to_string()))?
                    );
                    if name == "buckets" {
                        if let Some(legacy) = &legacy_source {
                            // Migration overlays a legacy marker on an
                            // existing bucket entry, including its delete.
                            if legacy
                                .contains_key(full_key.as_bytes())
                                .map_err(|e| StorageError::Database(e.to_string()))?
                            {
                                continue;
                            }
                        }
                    }
                    if full_key.starts_with("__system__/")
                        && !full_key.starts_with("__system__/__s4_bucket_marker_")
                    {
                        continue;
                    }
                    // Version-list keys in the legacy index are not IndexRecords.
                    if full_key.starts_with("__s4_versions_") {
                        continue;
                    }
                    let record: IndexRecord = bincode::deserialize(&value)
                        .map_err(|e| StorageError::Serialization(e.to_string()))?;
                    if is_legacy_migration_record(&full_key, &record) {
                        has_legacy = true;
                        break;
                    }
                }
            }
            Ok::<_, StorageError>((configs, has_legacy))
        })
        .await
        .map_err(|e| StorageError::Database(e.to_string()))??;
        let reader = super::VolumeReader::new(volumes);
        for (bucket, record) in configs {
            let bytes = if record.file_id == u32::MAX {
                use base64::Engine;
                let encoded = record.metadata.get("_inline_data").ok_or_else(|| {
                    StorageError::InvalidData(format!(
                        "bucket '{bucket}' has an unreadable versioning configuration"
                    ))
                })?;
                base64::engine::general_purpose::STANDARD
                    .decode(encoded)
                    .map_err(|e| StorageError::Serialization(e.to_string()))?
            } else {
                reader.read_blob(record.file_id, record.offset).await?.2
            };
            let status = String::from_utf8_lossy(&bytes);
            if matches!(status.trim(), "Enabled" | "Suspended") {
                return Err(StorageError::InvalidOperation {
                    operation:"single-to-cluster migration".to_string(),
                    reason:format!("bucket '{bucket}' has versioning {}; cluster mode cannot preserve its version history",status.trim()),
                });
            }
        }
        if erasure_coded && has_legacy {
            return Err(StorageError::InvalidOperation {
                operation:"single-to-cluster migration".to_string(),
                reason:format!("pool '{pool_name}' uses erasure coding; migration of single-node data requires a replicated pool"),
            });
        }
        Ok(())
    }

    /// Gives an unchanged legacy record its made-up HLC, without rewriting its blob.
    ///
    /// The HLC is the original modification time in milliseconds,
    /// logical counter 0 and node 0. The record and digest change atomically;
    /// ETag, metadata, modification time and storage layout stay intact. A
    /// modification time below one millisecond is refused without mutation:
    /// its made-up HLC would be zero, which means no HLC in the index.
    /// Returns false when any field differs from `expected`, or it is no longer
    /// a legacy user object or bucket marker. The caller must hold the engine's
    /// external exclusive write lock, as for its other read-modify-write methods.
    pub async fn stamp_legacy_if_unchanged(
        &self,
        bucket: &str,
        key: &str,
        expected: &IndexRecord,
    ) -> Result<bool, StorageError> {
        let full_key = format!("{bucket}/{key}");
        if !is_legacy_migration_record(&full_key, expected)
            || self.index_db.get(&full_key).await?.as_ref() != Some(expected)
        {
            return Ok(false);
        }
        let mut record = expected.clone();
        record.hlc_timestamp = record.modified_at / 1_000_000;
        if record.hlc_timestamp == 0 {
            return Err(StorageError::InvalidOperation {
                operation: "stamp legacy migration record".to_string(),
                reason: format!("record '{full_key}' has a zero made-up HLC; its modification time cannot be represented without changing its rank"),
            });
        }
        let (keyspace, stripped_key) = self.index_db.route_key_id(&full_key);
        let value =
            bincode::serialize(&record).map_err(|e| StorageError::Serialization(e.to_string()))?;
        let ops = vec![
            BatchOp {
                keyspace,
                action: BatchAction::Put(stripped_key.as_bytes().to_vec(), value),
            },
            self.journal.make_journal_entry_op(JournalEventType::ObjectPut {
                bucket: bucket.to_string(),
                key: key.to_string(),
                version_id: None,
                record,
            })?,
        ];
        self.index_db.batch_write(ops).await?;
        Ok(true)
    }

    /// Removes only the exact legacy generation copied to its current replicas.
    ///
    /// An absent, changed, stamped, versioned or erasure-coded record is left
    /// alone and returns false. Blob and dedup references, the record, digest
    /// and journal entry change in the same durable batch. The caller must hold
    /// the engine's external exclusive write lock, including against compaction.
    pub async fn remove_legacy_if_unchanged(
        &self,
        bucket: &str,
        key: &str,
        expected: &IndexRecord,
    ) -> Result<bool, StorageError> {
        let full_key = format!("{bucket}/{key}");
        if !is_legacy_migration_record(&full_key, expected)
            || self.index_db.get(&full_key).await?.as_ref() != Some(expected)
        {
            return Ok(false);
        }
        let (keyspace, stripped_key) = self.index_db.route_key_id(&full_key);
        let mut ops = vec![BatchOp {
            keyspace,
            action: BatchAction::Delete(stripped_key.as_bytes().to_vec()),
        }];
        ops.append(&mut self.data_release_ops(expected).await?);
        ops.push(
            self.journal.make_journal_entry_op(JournalEventType::ObjectDelete {
                bucket: bucket.to_string(),
                key: key.to_string(),
                version_id: None,
            })?,
        );
        self.index_db.batch_write(ops).await?;
        Ok(true)
    }
}
