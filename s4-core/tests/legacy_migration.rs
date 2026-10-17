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

//! Legacy migration primitives retain the precise single-node generation.
use s4_core::storage::{BitcaskStorageEngine, IndexOptions, StateMachine, VersionDigest};
use s4_core::{StorageEngine, VersioningStatus};
use std::collections::HashMap;
use tempfile::TempDir;

async fn engine(dir: &TempDir, cluster: bool) -> BitcaskStorageEngine {
    BitcaskStorageEngine::open(
        dir.path().join("volumes"),
        dir.path().join("metadata"),
        1024 * 1024,
        4096,
        true,
        IndexOptions {
            version_digests: cluster,
        },
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn pages_filter_system_records_versions_and_tombstones_without_losing_later_keys() {
    let dir = TempDir::new().unwrap();
    let e = engine(&dir, true).await;
    for (bucket, key) in [
        ("bucket", "a"),
        ("bucket", "b"),
        ("__system__", "__s4_bucket_marker_bucket"),
        ("__system__", "__s4_bucket_versioning_bucket"),
        ("__system__", "__s4_iam_user_x"),
        ("__system__", "__s4_ec_v1/manifests/x"),
    ] {
        e.put_object(bucket, key, b"data", "text/plain", &HashMap::new()).await.unwrap();
    }
    e.put_object_versioned(
        "bucket",
        "version",
        b"history",
        "text/plain",
        &HashMap::new(),
        VersioningStatus::Enabled,
    )
    .await
    .unwrap();
    e.tombstone_object("bucket", "aa", 10, 0, 1).await.unwrap();
    e.put_object("bucket", "ab", b"EC payload", "text/plain", &HashMap::new())
        .await
        .unwrap();
    assert!(e
        .detach_object_data_if_hlc(
            "bucket",
            "ab",
            0,
            0,
            0,
            s4_core::types::ObjectLayout::ErasureCoded {
                erasure_set_id: 1,
                topology_epoch: 1
            }
        )
        .await
        .unwrap());
    assert!(e.head_object("bucket", "ab").await.unwrap().is_erasure_coded());

    e.create_multipart_session(
        "unfinished",
        "bucket",
        "pending",
        "text/plain",
        &HashMap::new(),
    )
    .await
    .unwrap();
    let index = e.index_db();
    let markers = index.legacy_migration_page(true, None, 1).await.unwrap();
    assert_eq!(markers.len(), 1);
    assert_eq!(markers[0].0, "__system__/__s4_bucket_marker_bucket");
    assert!(index
        .legacy_migration_page(true, Some(&markers[0].0), 1)
        .await
        .unwrap()
        .is_empty());
    let first = index.legacy_migration_page(false, None, 1).await.unwrap();
    assert_eq!(first[0].0, "bucket/a");
    let second = index.legacy_migration_page(false, Some(&first[0].0), 1).await.unwrap();
    assert_eq!(second[0].0, "bucket/b");
    assert!(index
        .legacy_migration_page(false, Some(&second[0].0), 1)
        .await
        .unwrap()
        .is_empty());
    assert!(index.legacy_migration_page(false, None, 0).await.unwrap().is_empty());
}

#[tokio::test]
async fn stamping_preserves_bytes_metadata_location_and_changes_digest_durably() {
    let dir = TempDir::new().unwrap();
    let e = engine(&dir, true).await;
    e.put_object(
        "bucket",
        "key",
        b"original",
        "application/example",
        &HashMap::from([("label".into(), "value".into())]),
    )
    .await
    .unwrap();
    let before = e.head_object("bucket", "key").await.unwrap();
    let disk = std::fs::read(dir.path().join("volumes/volume_000000.dat")).unwrap();
    assert!(e.stamp_legacy_if_unchanged("bucket", "key", &before).await.unwrap());
    assert!(!e.stamp_legacy_if_unchanged("bucket", "key", &before).await.unwrap());
    let (bytes, after) = e.get_object("bucket", "key").await.unwrap();
    assert_eq!(bytes, b"original");
    let mut expected = before.clone();
    expected.hlc_timestamp = before.modified_at / 1_000_000;
    expected.hlc_logical = 0;
    expected.origin_node_id = 0;
    assert_eq!(
        serde_json::to_value(&after).unwrap(),
        serde_json::to_value(expected).unwrap()
    );
    assert_eq!(
        disk,
        std::fs::read(dir.path().join("volumes/volume_000000.dat")).unwrap()
    );
    let digest = e.index_db().version_digests_in(0, None, None, 100).await.unwrap();
    assert!(
        digest
            .iter()
            .any(|entry| entry.key == "bucket/key"
                && entry.digest == VersionDigest::of_record(&after))
    );
    e.index_db().seal_version_digests().unwrap();
    drop(e);
    let e = engine(&dir, true).await;
    assert_eq!(
        e.head_object("bucket", "key").await.unwrap().hlc_timestamp,
        after.hlc_timestamp
    );
}

#[tokio::test]
async fn full_snapshot_fences_compaction_metadata_changes_and_foreign_removal() {
    let dir = TempDir::new().unwrap();
    let e = engine(&dir, true).await;
    e.put_object("bucket", "a", b"same", "text/plain", &HashMap::new())
        .await
        .unwrap();
    e.put_object("bucket", "b", b"same", "text/plain", &HashMap::new())
        .await
        .unwrap();
    let before = e.head_object("bucket", "a").await.unwrap();
    let mut relocated = before.clone();
    relocated.offset += 1;
    assert!(!e.stamp_legacy_if_unchanged("bucket", "a", &relocated).await.unwrap());
    let mut metadata_changed = before.clone();
    metadata_changed.metadata.insert("updated".into(), "yes".into());
    assert!(!e.remove_legacy_if_unchanged("bucket", "a", &metadata_changed).await.unwrap());
    assert!(e.remove_legacy_if_unchanged("bucket", "a", &before).await.unwrap());
    assert!(!e.remove_legacy_if_unchanged("bucket", "a", &before).await.unwrap());
    assert_eq!(e.get_object("bucket", "b").await.unwrap().0, b"same");
    assert!(e
        .index_db()
        .legacy_migration_page(false, None, 10)
        .await
        .unwrap()
        .iter()
        .all(|(key, _)| key != "bucket/a"));
}

#[tokio::test]
async fn completion_survives_sealed_restart_and_is_invalidated_by_single_restore_or_uncertain_restart(
) {
    let dir = TempDir::new().unwrap();
    let e = engine(&dir, true).await;
    assert!(!e.index_db().legacy_migration_complete().await.unwrap());
    e.index_db().mark_legacy_migration_complete().await.unwrap();
    e.index_db().seal_version_digests().unwrap();
    drop(e);
    let e = engine(&dir, true).await;
    assert!(e.index_db().legacy_migration_complete().await.unwrap());
    let snapshot = e.snapshot().await.unwrap();
    e.restore(snapshot).await.unwrap();
    assert!(!e.index_db().legacy_migration_complete().await.unwrap());
    e.index_db().mark_legacy_migration_complete().await.unwrap();
    e.index_db().seal_version_digests().unwrap();
    drop(e);
    drop(engine(&dir, false).await);
    let e = engine(&dir, true).await;
    assert!(!e.index_db().legacy_migration_complete().await.unwrap());
    e.index_db().mark_legacy_migration_complete().await.unwrap();
    drop(e);
    let e = engine(&dir, true).await;
    assert!(!e.index_db().legacy_migration_complete().await.unwrap());
}

#[tokio::test]
async fn preflight_refuses_versioning_and_ec_without_mutating_the_source() {
    for status in [VersioningStatus::Enabled, VersioningStatus::Suspended] {
        let dir = TempDir::new().unwrap();
        let e = engine(&dir, false).await;
        e.put_object_versioned(
            "history",
            "key",
            b"first",
            "text/plain",
            &HashMap::new(),
            VersioningStatus::Enabled,
        )
        .await
        .unwrap();
        e.put_object_versioned(
            "history",
            "key",
            b"second",
            "text/plain",
            &HashMap::new(),
            VersioningStatus::Enabled,
        )
        .await
        .unwrap();
        e.put_object(
            "__system__",
            "__s4_bucket_versioning_history",
            status.as_xml_status().unwrap().as_bytes(),
            "text/plain",
            &HashMap::new(),
        )
        .await
        .unwrap();
        let before = e.snapshot().await.unwrap();
        drop(e);
        let metadata = dir.path().join("metadata");
        let digest_seal = dir.path().join("metadata.digests-sealed");
        assert!(!digest_seal.exists());
        let raw = fjall::Database::builder(&metadata).open().unwrap();
        let seqno = raw.seqno();
        drop(raw);
        let error = BitcaskStorageEngine::preflight_cluster_transition(
            &dir.path().join("volumes"),
            &metadata,
            "replicated",
            false,
            false,
        )
        .await
        .unwrap_err();
        let raw = fjall::Database::builder(&metadata).open().unwrap();
        assert_eq!(
            raw.seqno(),
            seqno,
            "preflight must not commit any logical mutation"
        );
        drop(raw);
        assert!(error.to_string().contains("history"));
        assert!(error.to_string().contains(status.as_xml_status().unwrap()));
        assert!(!digest_seal.exists());
        let e = engine(&dir, false).await;
        let after = e.snapshot().await.unwrap();
        assert_eq!(before.sequence, after.sequence);
        assert_eq!(
            e.list_object_versions("history", "key", None, None, 10)
                .await
                .unwrap()
                .versions
                .len(),
            2
        );
        assert_eq!(e.get_object("history", "key").await.unwrap().0, b"second");
    }
    let dir = TempDir::new().unwrap();
    let volumes = dir.path().join("volumes");
    let metadata = dir.path().join("metadata");
    BitcaskStorageEngine::preflight_cluster_transition(&volumes, &metadata, "ec-pool", true, false)
        .await
        .unwrap();
    assert!(!metadata.exists());
    let e = engine(&dir, false).await;
    e.put_object("bucket", "key", b"single", "text/plain", &HashMap::new())
        .await
        .unwrap();
    drop(e);
    BitcaskStorageEngine::preflight_cluster_transition(
        &volumes,
        &metadata,
        "replicated",
        false,
        false,
    )
    .await
    .unwrap();
    let error = BitcaskStorageEngine::preflight_cluster_transition(
        &volumes, &metadata, "ec-pool", true, false,
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("ec-pool"));
    assert!(error.to_string().contains("erasure"));
    assert_eq!(
        engine(&dir, false).await.get_object("bucket", "key").await.unwrap().0,
        b"single"
    );
}

#[tokio::test]
async fn existing_cluster_versioning_is_allowed_but_return_from_single_is_checked() {
    let dir = TempDir::new().unwrap();
    let e = engine(&dir, true).await;
    e.put_object(
        "__system__",
        "__s4_bucket_versioning_old",
        b"Enabled",
        "text/plain",
        &HashMap::new(),
    )
    .await
    .unwrap();
    e.index_db().seal_version_digests().unwrap();
    drop(e);
    let volumes = dir.path().join("volumes");
    let metadata = dir.path().join("metadata");
    BitcaskStorageEngine::preflight_cluster_transition(
        &volumes,
        &metadata,
        "replicated",
        false,
        true,
    )
    .await
    .unwrap();
    let e = engine(&dir, false).await;
    e.index_db().mark_single_mode_open().await.unwrap();
    e.put_object_versioned(
        "old",
        "key",
        b"history",
        "text/plain",
        &HashMap::new(),
        VersioningStatus::Enabled,
    )
    .await
    .unwrap();
    drop(e);
    let error = BitcaskStorageEngine::preflight_cluster_transition(
        &volumes,
        &metadata,
        "replicated",
        false,
        true,
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("old"));
    assert!(error.to_string().contains("Enabled"));
    assert_eq!(
        engine(&dir, false).await.get_object("old", "key").await.unwrap().0,
        b"history"
    );
}

#[tokio::test]
async fn actual_compaction_invalidates_the_old_snapshot_and_the_new_snapshot_can_be_stamped() {
    use s4_compactor::{CompactionConfig, VolumeCompactor};
    let dir = TempDir::new().unwrap();
    let e = engine(&dir, true).await;
    e.put_object(
        "bucket",
        "dead",
        &vec![1; 300_000],
        "text/plain",
        &HashMap::new(),
    )
    .await
    .unwrap();
    let body = vec![2; 500_000];
    e.put_object(
        "bucket",
        "live",
        &body,
        "application/example",
        &HashMap::new(),
    )
    .await
    .unwrap();
    let before = e.head_object("bucket", "live").await.unwrap();
    e.put_object(
        "bucket",
        "roll",
        &vec![3; 500_000],
        "text/plain",
        &HashMap::new(),
    )
    .await
    .unwrap();
    e.delete_object("bucket", "dead").await.unwrap();
    e.sync().await.unwrap();
    let compactor = VolumeCompactor::new(
        e.volumes_dir().to_path_buf(),
        e.index_db().clone(),
        e.deduplicator().clone(),
        e.volume_writer().clone(),
        CompactionConfig {
            fragmentation_threshold: 0.01,
            min_dead_bytes: 0,
            max_volumes_per_run: 100,
            dry_run: false,
            ..Default::default()
        },
    );
    let stats = compactor.run().await.unwrap();
    assert_eq!(stats.errors, 0);
    let after = e.head_object("bucket", "live").await.unwrap();
    assert_ne!(
        (before.file_id, before.offset),
        (after.file_id, after.offset)
    );
    assert!(!e.stamp_legacy_if_unchanged("bucket", "live", &before).await.unwrap());
    assert!(!e.remove_legacy_if_unchanged("bucket", "live", &before).await.unwrap());
    assert!(e.stamp_legacy_if_unchanged("bucket", "live", &after).await.unwrap());
    assert_eq!(e.get_object("bucket", "live").await.unwrap().0, body);
}

#[tokio::test]
async fn a_legacy_timestamp_below_one_millisecond_is_refused_without_changing_rank() {
    let dir = TempDir::new().unwrap();
    let e = engine(&dir, true).await;
    e.put_object(
        "bucket",
        "ancient",
        b"preserved",
        "text/plain",
        &HashMap::new(),
    )
    .await
    .unwrap();
    let mut original = e.head_object("bucket", "ancient").await.unwrap();
    original.modified_at = 999_999;
    e.index_db().put("bucket/ancient", &original).await.unwrap();
    let error = e.stamp_legacy_if_unchanged("bucket", "ancient", &original).await.unwrap_err();
    assert!(error.to_string().contains("zero"));
    let after = e.head_object("bucket", "ancient").await.unwrap();
    assert_eq!(after, original);
    assert_eq!(after.reported_hlc(), (0, 0, 0));
    assert_eq!(
        e.get_object("bucket", "ancient").await.unwrap().0,
        b"preserved"
    );
}

#[tokio::test]
async fn preflight_uses_current_keyspaces_and_ignores_stale_legacy_versioning_blobs() {
    let dir = TempDir::new().unwrap();
    let e = engine(&dir, false).await;
    e.put_object("bucket", "live", b"current", "text/plain", &HashMap::new())
        .await
        .unwrap();
    e.put_object(
        "__system__",
        "__s4_bucket_versioning_deleted",
        b"Enabled",
        "text/plain",
        &HashMap::new(),
    )
    .await
    .unwrap();
    let config = e.head_object("__system__", "__s4_bucket_versioning_deleted").await.unwrap();
    e.delete_object("__system__", "__s4_bucket_versioning_deleted").await.unwrap();
    drop(e);
    let metadata = dir.path().join("metadata");
    let volumes = dir.path().join("volumes");
    let legacy_key = "__system__/__s4_bucket_versioning_deleted";
    {
        let db = fjall::Database::builder(&metadata).open().unwrap();
        let legacy = db.keyspace("index", fjall::KeyspaceCreateOptions::default).unwrap();
        legacy.insert(legacy_key, bincode::serialize(&config).unwrap()).unwrap();
        db.persist(fjall::PersistMode::SyncAll).unwrap();
    }
    // This is a normal upgraded directory: migration retained the old
    // keyspace, but the current index has since deleted its configuration.
    BitcaskStorageEngine::preflight_cluster_transition(
        &volumes,
        &metadata,
        "replicated",
        false,
        false,
    )
    .await
    .unwrap();
    {
        let db = fjall::Database::builder(&metadata).open().unwrap();
        let legacy = db.keyspace("index", fjall::KeyspaceCreateOptions::default).unwrap();
        let mut stale = config.clone();
        stale.file_id = 999_999;
        legacy.insert(legacy_key, bincode::serialize(&stale).unwrap()).unwrap();
        db.persist(fjall::PersistMode::SyncAll).unwrap();
    }
    // Compaction may already have removed the old blob: never read it.
    BitcaskStorageEngine::preflight_cluster_transition(
        &volumes,
        &metadata,
        "replicated",
        false,
        false,
    )
    .await
    .unwrap();
    assert_eq!(
        engine(&dir, false).await.get_object("bucket", "live").await.unwrap().0,
        b"current"
    );
}

#[tokio::test]
async fn preflight_still_refuses_a_legacy_index_that_will_be_migrated() {
    for current_keyspaces in [false, true] {
        let dir = TempDir::new().unwrap();
        let e = engine(&dir, false).await;
        e.put_object(
            "__system__",
            "__s4_bucket_versioning_history",
            b"Suspended",
            "text/plain",
            &HashMap::new(),
        )
        .await
        .unwrap();
        let config = e.head_object("__system__", "__s4_bucket_versioning_history").await.unwrap();
        drop(e);
        let metadata = dir.path().join("metadata");
        {
            let db = fjall::Database::builder(&metadata).open().unwrap();
            let legacy = db.keyspace("index", fjall::KeyspaceCreateOptions::default).unwrap();
            legacy
                .insert(
                    "__system__/__s4_bucket_versioning_history",
                    bincode::serialize(&config).unwrap(),
                )
                .unwrap();
            let objects = db.keyspace("objects", fjall::KeyspaceCreateOptions::default).unwrap();
            objects.clear().unwrap();
            if !current_keyspaces {
                db.delete_keyspace(objects).unwrap();
            }
            db.persist(fjall::PersistMode::SyncAll).unwrap();
        }
        let error = BitcaskStorageEngine::preflight_cluster_transition(
            &dir.path().join("volumes"),
            &metadata,
            "replicated",
            false,
            false,
        )
        .await
        .unwrap_err();
        assert!(
            error.to_string().contains("history") && error.to_string().contains("Suspended"),
            "{error}"
        );
    }
}

#[tokio::test]
async fn outstanding_snapshot_survives_overwrite_and_uncertain_restart() {
    let dir = tempfile::tempdir().unwrap();
    let volumes = dir.path().join("volumes");
    let metadata = dir.path().join("metadata");
    let storage = BitcaskStorageEngine::new(&volumes, &metadata, 1024 * 1024, 4096, true)
        .await
        .unwrap();
    storage.index_db().mark_single_mode_open().await.unwrap();
    storage
        .put_object("bucket", "key", b"source", "text/plain", &HashMap::new())
        .await
        .unwrap();
    let snapshot = storage.head_object("bucket", "key").await.unwrap();
    drop(storage);
    let storage = BitcaskStorageEngine::open(
        &volumes,
        &metadata,
        1024 * 1024,
        4096,
        true,
        IndexOptions {
            version_digests: true,
        },
    )
    .await
    .unwrap();
    storage
        .index_db()
        .track_legacy_migration_page(&[("bucket/key".to_string(), snapshot.clone())])
        .await
        .unwrap();
    storage
        .put_object(
            "bucket",
            "key",
            b"client",
            "text/plain",
            &HashMap::from([
                ("_s4_hlc_wall".into(), "9000000000000".into()),
                ("_s4_hlc_node_id".into(), "7".into()),
            ]),
        )
        .await
        .unwrap();
    drop(storage);
    let storage = BitcaskStorageEngine::open(
        &volumes,
        &metadata,
        1024 * 1024,
        4096,
        true,
        IndexOptions {
            version_digests: true,
        },
    )
    .await
    .unwrap();
    assert_eq!(
        storage.index_db().legacy_migration_queue_page(None, 1).await.unwrap(),
        vec![("bucket/key".into(), snapshot)]
    );
    storage.index_db().finish_legacy_migration_key("bucket/key").await.unwrap();
    storage.index_db().mark_legacy_migration_complete().await.unwrap();
}

#[tokio::test]
async fn restore_does_not_hide_an_outstanding_migration_obligation() {
    let dir = TempDir::new().unwrap();
    let e = engine(&dir, true).await;
    e.put_object("bucket", "key", b"source", "text/plain", &HashMap::new())
        .await
        .unwrap();
    let snapshot = e.head_object("bucket", "key").await.unwrap();
    e.index_db()
        .track_legacy_migration_page(&[("bucket/key".to_string(), snapshot.clone())])
        .await
        .unwrap();
    e.index_db().restore_keyspaces(Vec::new()).await.unwrap();
    assert!(e.index_db().legacy_migration_key_pending("bucket/key").await.unwrap());
    assert!(!e.index_db().legacy_migration_complete().await.unwrap());
}

#[tokio::test]
async fn migration_stream_checks_zero_hash_composite_segments_and_rejects_stale_snapshots() {
    use std::io::{Seek, SeekFrom, Write};
    use tokio::io::AsyncReadExt;
    let dir = TempDir::new().unwrap();
    let e = engine(&dir, true).await;
    e.create_multipart_session("upload", "bucket", "parts", "text/plain", &HashMap::new())
        .await
        .unwrap();
    let part = e
        .upload_part_streaming(
            "upload",
            1,
            Box::new(std::io::Cursor::new(b"composite bytes".to_vec())),
            15,
        )
        .await
        .unwrap()
        .record;
    let location = e.resolve_blob_location(&part.blob_id).unwrap();
    e.mark_session_completing("upload").await.unwrap();
    e.complete_multipart_native(
        "bucket",
        "parts",
        "upload",
        std::slice::from_ref(&part),
        "\"abcdef-1\"",
        "text/plain",
        &HashMap::new(),
    )
    .await
    .unwrap();
    let snapshot = e.head_object("bucket", "parts").await.unwrap();
    assert_eq!(snapshot.content_hash, [0; 32]);
    let mut stream = e.open_legacy_migration_stream("bucket", "parts", &snapshot).await.unwrap();
    let mut bytes = Vec::new();
    stream.body.read_to_end(&mut bytes).await.unwrap();
    assert_eq!(bytes, b"composite bytes");
    let path = dir.path().join("volumes").join(format!("volume_{:06}.dat", location.volume_id));
    let mut file = std::fs::OpenOptions::new().read(true).write(true).open(path).unwrap();
    file.seek(SeekFrom::Start(location.offset)).unwrap();
    let header: s4_core::types::BlobHeader = bincode::deserialize_from(&mut file).unwrap();
    let offset =
        location.offset + bincode::serialized_size(&header).unwrap() + u64::from(header.key_len);
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.write_all(b"X").unwrap();
    file.sync_all().unwrap();
    assert!(matches!(
        e.open_legacy_migration_stream("bucket", "parts", &snapshot).await,
        Err(s4_core::StorageError::ChecksumMismatch { .. })
    ));
    e.put_object(
        "bucket",
        "parts",
        b"client bytes",
        "text/plain",
        &HashMap::new(),
    )
    .await
    .unwrap();
    assert!(matches!(
        e.open_legacy_migration_stream("bucket", "parts", &snapshot).await,
        Err(s4_core::StorageError::InvalidOperation { .. })
    ));
}

#[tokio::test]
async fn incomplete_transition_keeps_preflight_evidence_and_refusal_does_not_change_seqno() {
    let dir = TempDir::new().unwrap();
    let single = engine(&dir, false).await;
    single.index_db().mark_single_mode_open().await.unwrap();
    single
        .put_object(
            "bucket",
            "key",
            b"single bytes",
            "text/plain",
            &HashMap::new(),
        )
        .await
        .unwrap();
    drop(single);
    let cluster = engine(&dir, true).await;
    let snapshot = cluster.head_object("bucket", "key").await.unwrap();
    cluster
        .index_db()
        .track_legacy_migration_page(&[("bucket/key".into(), snapshot.clone())])
        .await
        .unwrap();
    drop(cluster); // A failed bootstrap has now persisted an identity.
    let metadata = dir.path().join("metadata");
    let volumes = dir.path().join("volumes");
    let seqno = {
        let raw = fjall::Database::builder(&metadata).open().unwrap();
        raw.seqno()
    };
    assert!(BitcaskStorageEngine::preflight_cluster_transition(
        &volumes, &metadata, "ec", true, true
    )
    .await
    .is_err());
    assert_eq!(
        {
            let raw = fjall::Database::builder(&metadata).open().unwrap();
            raw.seqno()
        },
        seqno
    );
    let cluster = engine(&dir, true).await;
    assert!(cluster.stamp_legacy_if_unchanged("bucket", "key", &snapshot).await.unwrap());
    // The queue alone still prohibits EC after the raw legacy candidate disappeared.
    drop(cluster);
    assert!(BitcaskStorageEngine::preflight_cluster_transition(
        &volumes, &metadata, "ec", true, true
    )
    .await
    .is_err());
    let cluster = engine(&dir, true).await;
    cluster.index_db().finish_legacy_migration_key("bucket/key").await.unwrap();
    cluster.index_db().mark_legacy_migration_complete().await.unwrap();
    drop(cluster);
    assert!(BitcaskStorageEngine::preflight_cluster_transition(
        &volumes, &metadata, "ec", true, true
    )
    .await
    .is_ok());
}
