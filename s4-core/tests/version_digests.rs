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

//! Version digests kept by the index: every write path leaves the digests a
//! full rebuild would give, a kill in the middle of writes leaves them so too,
//! and the tombstone horizon is decided once.

use s4_compactor::{CompactionConfig, VolumeCompactor};
use s4_core::storage::{
    BitcaskStorageEngine, IndexOptions, TombstoneHorizon, TombstoneRef, VersionDigest,
    VersionDigestsOpening, VersionKind,
};
use s4_core::types::ObjectLayout;
use s4_core::{KeyVersion, StorageEngine, VersioningStatus};
use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::time::Duration;
use tempfile::TempDir;

const CLUSTER: IndexOptions = IndexOptions {
    version_digests: true,
};

/// Close a raw fixture as a digest-aware build would, so format migration
/// tests vary the format independently of whether the history is trusted.
fn seal_fixture(dir: &Path, db: &fjall::Database) {
    let seqno = db.seqno();
    db.persist(fjall::PersistMode::SyncAll).unwrap();
    std::fs::write(dir.join("metadata_db.digests-sealed"), seqno.to_string()).unwrap();
}

/// Verify the recovered WAL before IndexDb can rebuild anything. Startup's
/// conservative rebuild must not hide a regression in atomic digest writes.
fn check_crashed_digests(dir: &Path) {
    let db = fjall::Database::builder(dir.join("metadata_db")).open().unwrap();
    let mut expected = BTreeMap::new();
    for (name, prefix) in [
        ("objects", ""),
        ("buckets", "__system__/__s4_bucket_marker_"),
        ("iam", "__system__/__s4_iam_"),
    ] {
        let keyspace = db.keyspace(name, fjall::KeyspaceCreateOptions::default).unwrap();
        for guard in keyspace.iter() {
            let (key, value) = guard.into_inner().unwrap();
            let full_key = format!("{prefix}{}", std::str::from_utf8(&key).unwrap());
            let (bucket, key) = full_key.split_once('/').unwrap();
            let record: s4_core::IndexRecord = bincode::deserialize(&value).unwrap();
            if s4_core::types::is_replicated_key(bucket, key)
                && (name != "objects" || record.version_id.is_none())
            {
                expected.insert(full_key, VersionDigest::of_record(&record));
            }
        }
    }
    let digests = db.keyspace("version_digests", fjall::KeyspaceCreateOptions::default).unwrap();
    let stored: BTreeMap<_, _> = digests
        .iter()
        .map(|guard| {
            let (key, value) = guard.into_inner().unwrap();
            (
                std::str::from_utf8(&key[8..]).unwrap().to_string(),
                VersionDigest::decode(&value).unwrap(),
            )
        })
        .collect();
    assert!(
        !expected.is_empty(),
        "the killed writer must have left records"
    );
    assert_eq!(
        stored, expected,
        "WAL recovery must preserve record/digest atomicity before rebuilding"
    );
}

async fn open(dir: &Path, options: IndexOptions) -> BitcaskStorageEngine {
    let volumes = dir.join("volumes");
    std::fs::create_dir_all(&volumes).unwrap();
    // Small volumes, so that compaction has whole volumes to rewrite.
    BitcaskStorageEngine::open(
        volumes,
        dir.join("metadata_db"),
        64 * 1024,
        0,
        false,
        options,
    )
    .await
    .unwrap()
}

/// A small deterministic generator: the sequence is the same on every run.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }
}

fn hlc_metadata(wall: u64) -> HashMap<String, String> {
    HashMap::from([
        ("_s4_hlc_wall".to_string(), wall.to_string()),
        ("_s4_hlc_logical".to_string(), "0".to_string()),
        ("_s4_hlc_node_id".to_string(), "1".to_string()),
    ])
}

/// The HLC generation a key holds, when it holds a version.
async fn generation(
    engine: &BitcaskStorageEngine,
    bucket: &str,
    key: &str,
) -> Option<(u64, u32, u128, bool)> {
    match engine.key_version(bucket, key).await.unwrap() {
        KeyVersion::Live(record) => Some((
            record.hlc_timestamp,
            record.hlc_logical,
            record.origin_node_id,
            record.is_erasure_coded(),
        )),
        KeyVersion::Tombstone(_) | KeyVersion::Absent => None,
    }
}

/// One step of the random sequence: a write through one of the paths that
/// change the `objects` or `buckets` keyspace.
async fn random_write(
    engine: &BitcaskStorageEngine,
    rng: &mut Rng,
    clock: &mut u64,
    written: &mut HashMap<String, Vec<u8>>,
) {
    let keys = ["a", "b", "c", "d", "e", "f", "g", "h"];
    let key = keys[rng.below(keys.len() as u64) as usize];
    // Mostly forward, sometimes behind: the index's LWW then refuses it.
    *clock += 1 + rng.below(5);
    let wall = clock.saturating_sub(rng.below(3) * 4);
    let body = format!("{key}-{wall}-{}", "x".repeat(rng.below(2048) as usize)).into_bytes();

    match rng.below(16) {
        0..=2 => {
            if engine
                .put_object("photos", key, &body, "text/plain", &hlc_metadata(wall))
                .await
                .is_ok()
            {
                written.insert(key.to_string(), body);
            }
        }
        3 => {
            // A record without an HLC, as single mode writes it.
            engine
                .put_object("photos", key, &body, "text/plain", &HashMap::new())
                .await
                .unwrap();
        }
        4 | 5 => {
            engine.tombstone_object("photos", key, wall, 0, 1).await.unwrap();
        }
        6 => match engine.delete_object("photos", key).await {
            Ok(()) | Err(s4_core::StorageError::ObjectNotFound { .. }) => {}
            Err(error) => panic!("delete failed: {error}"),
        },
        7 => {
            if let Some((wall, logical, node, _)) = generation(engine, "photos", key).await {
                engine.delete_object_if_hlc("photos", key, wall, logical, node).await.unwrap();
            }
        }
        8 => {
            if let Some((wall, logical, node, false)) = generation(engine, "photos", key).await {
                let layout = ObjectLayout::ErasureCoded {
                    erasure_set_id: 7,
                    topology_epoch: 1,
                };
                engine
                    .detach_object_data_if_hlc("photos", key, wall, logical, node, layout)
                    .await
                    .unwrap();
            }
        }
        9 => {
            if let (Some((wall, logical, node, true)), Some(data)) =
                (generation(engine, "photos", key).await, written.get(key))
            {
                // Refused when the bytes are not the object's: that is a
                // write path too, one that must leave the digest alone.
                let _ = engine
                    .attach_object_data_if_hlc("photos", key, wall, logical, node, data)
                    .await;
            }
        }
        10 => {
            let mut record = s4_core::IndexRecord::new(
                0,
                0,
                body.len() as u64,
                [3; 32],
                "etag".to_string(),
                "text/plain".to_string(),
            )
            .into_erasure_coded(9, 2);
            record.hlc_timestamp = wall;
            record.origin_node_id = 2;
            engine.restore_erasure_coded_entry("photos", key, record).await.unwrap();
        }
        11 => {
            let marker = format!("__s4_bucket_marker_{key}");
            if rng.below(2) == 0 {
                engine
                    .put_object(
                        "__system__",
                        &marker,
                        b"",
                        "application/octet-stream",
                        &hlc_metadata(wall),
                    )
                    .await
                    .unwrap();
            } else {
                engine.tombstone_object("__system__", &marker, wall, 0, 1).await.unwrap();
            }
        }
        12 => {
            // Bucket configurations live in `__system__` and have no digest.
            let config = format!("cors/{key}");
            if rng.below(2) == 0 {
                engine
                    .put_object(
                        "__system__",
                        &config,
                        &body,
                        "application/json",
                        &HashMap::new(),
                    )
                    .await
                    .unwrap();
            } else {
                let _ = engine.delete_object("__system__", &config).await;
            }
        }
        13 => {
            // Versioned records have no digest, the key's current pointer
            // included.
            engine
                .put_object_versioned(
                    "versioned",
                    key,
                    &body,
                    "text/plain",
                    &HashMap::new(),
                    VersioningStatus::Enabled,
                )
                .await
                .unwrap();
            if rng.below(3) == 0 {
                engine
                    .delete_object_versioned("versioned", key, None, VersioningStatus::Enabled)
                    .await
                    .unwrap();
            }
        }
        14 => {
            let length = body.len() as u64;
            if engine
                .put_object_streaming(
                    "photos",
                    key,
                    Box::new(std::io::Cursor::new(body.clone())),
                    length,
                    "text/plain",
                    &hlc_metadata(wall),
                    "",
                )
                .await
                .is_ok()
            {
                written.insert(key.to_string(), body);
            }
        }
        _ => {
            engine.purge_tombstones().await.unwrap();
        }
    }
}

async fn compact(engine: &BitcaskStorageEngine) {
    engine.sync().await.unwrap();
    let compactor = VolumeCompactor::new(
        engine.volumes_dir().to_path_buf(),
        engine.index_db().clone(),
        engine.deduplicator().clone(),
        engine.volume_writer().clone(),
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
}

async fn multipart(engine: &BitcaskStorageEngine, key: &str) {
    let upload_id = format!("upload-{key}");
    let metadata = hlc_metadata(1);
    engine
        .create_multipart_session(
            &upload_id,
            "photos",
            key,
            "application/octet-stream",
            &metadata,
        )
        .await
        .unwrap();
    let mut parts = Vec::new();
    for number in 1..=2u32 {
        let part = vec![number as u8; 700];
        let uploaded = engine
            .upload_part_streaming(
                &upload_id,
                number,
                Box::new(std::io::Cursor::new(part)),
                700,
            )
            .await
            .unwrap();
        parts.push(uploaded.record);
    }
    engine.mark_session_completing(&upload_id).await.unwrap();
    engine
        .complete_multipart_native(
            "photos",
            key,
            &upload_id,
            &parts,
            "etag-2",
            "application/octet-stream",
            &metadata,
        )
        .await
        .unwrap();
}

/// Deletes the digests keyspace of a closed index, as an operator or an
/// older binary could leave it.
fn drop_digests_keyspace(dir: &Path) {
    let db = fjall::Database::builder(dir.join("metadata_db")).open().unwrap();
    let digests = db.keyspace("version_digests", fjall::KeyspaceCreateOptions::default).unwrap();
    db.delete_keyspace(digests).unwrap();
}

/// Every write path keeps the digest of the key it writes, in the same batch:
/// after a long random sequence of writes, overwrites, tombstones, moves to
/// erasure coding and back, purges and compactions, the stored digests are
/// exactly the ones a full rebuild writes.
#[tokio::test]
async fn every_write_path_leaves_the_digests_a_rebuild_gives() {
    let temp = TempDir::new().unwrap();
    let mut clock = 1_000u64;
    let mut written = HashMap::new();
    let mut rng = Rng(0x005e_edd1_6e57);
    let before = {
        let engine = open(temp.path(), CLUSTER).await;
        multipart(&engine, "composite").await;
        let mut kinds = Vec::new();
        for step in 1..=600 {
            random_write(&engine, &mut rng, &mut clock, &mut written).await;
            if step % 150 == 0 {
                compact(&engine).await;
            }
            if step % 25 == 0 {
                let check = engine.index_db().verify_version_digests().await.unwrap();
                assert!(check.is_consistent(), "after step {step}: {check:?}");
                let digests = engine.index_db().version_digests().await.unwrap();
                kinds.extend(digests.iter().map(|entry| entry.digest.kind));
            }
        }
        for kind in [
            VersionKind::Live,
            VersionKind::Tombstone,
            VersionKind::ErasureCoded,
        ] {
            assert!(
                kinds.contains(&kind),
                "the sequence never left a {kind:?} digest"
            );
        }
        let digests = engine.index_db().version_digests().await.unwrap();
        assert!(
            digests
                .iter()
                .any(|entry| entry.key.starts_with("__system__/__s4_bucket_marker_")),
            "bucket markers have digests"
        );
        assert!(
            digests.iter().all(|entry| !entry.key.starts_with("__system__/cors/")
                && !entry.key.starts_with("versioned/")),
            "configurations and versioned records have none"
        );
        assert!(
            digests.windows(2).all(|pair| pair[0].ring_position <= pair[1].ring_position),
            "digests are in ring order"
        );
        digests
    };

    drop_digests_keyspace(temp.path());
    let engine = open(temp.path(), CLUSTER).await;
    assert_eq!(
        engine.index_db().version_digests_opening(),
        Some(&VersionDigestsOpening::Rebuilt {
            records: before.len() as u64,
            elapsed: match engine.index_db().version_digests_opening() {
                Some(VersionDigestsOpening::Rebuilt { elapsed, .. }) => *elapsed,
                other => panic!("expected a rebuild, got {other:?}"),
            },
            previous_format: None,
        })
    );
    assert_eq!(engine.index_db().version_digests().await.unwrap(), before);
}

/// Pages read range by range, each continued after its last key, give every
/// digest exactly once and in ring order, and none outside its range.
#[tokio::test]
async fn pages_of_ring_ranges_cover_every_digest_once() {
    let dir = TempDir::new().unwrap();
    let engine = open(dir.path(), CLUSTER).await;
    for n in 0..300 {
        engine
            .put_object(
                "photos",
                &format!("k{n:03}"),
                b"v",
                "text/plain",
                &hlc_metadata(10),
            )
            .await
            .unwrap();
    }
    engine.tombstone_object("photos", "gone", 20, 0, 1).await.unwrap();
    let all = engine.index_db().version_digests().await.unwrap();
    assert_eq!(all.len(), 301);

    // Four ranges of the ring, the last one open-ended, in pages of seven.
    let quarter = 1u64 << 62;
    let ranges = [
        (0, Some(quarter)),
        (quarter, Some(2 * quarter)),
        (2 * quarter, Some(3 * quarter)),
        (3 * quarter, None),
    ];
    let mut paged = Vec::new();
    for (from, to) in ranges {
        let mut after: Option<String> = None;
        loop {
            let page = engine.version_digests_in(from, to, after.as_deref(), 7).await.unwrap();
            for entry in &page {
                assert!(entry.ring_position >= from, "{} below its range", entry.key);
                assert!(
                    to.is_none_or(|to| entry.ring_position < to),
                    "{} past its range",
                    entry.key
                );
            }
            let Some(last) = page.last() else { break };
            after = Some(last.key.clone());
            paged.extend(page);
        }
    }
    assert_eq!(paged, all);

    // A key to continue after that lies below the range starts nothing below
    // it; one past the range gives an empty page rather than a reversed range.
    let below = all.iter().find(|entry| entry.ring_position < quarter).unwrap();
    let past = all.iter().find(|entry| entry.ring_position >= 2 * quarter).unwrap();
    let page = engine
        .version_digests_in(quarter, Some(2 * quarter), Some(&below.key), 1000)
        .await
        .unwrap();
    let in_range: Vec<_> = all
        .iter()
        .filter(|entry| entry.ring_position >= quarter && entry.ring_position < 2 * quarter)
        .cloned()
        .collect();
    assert_eq!(page, in_range);
    let page = engine
        .version_digests_in(quarter, Some(2 * quarter), Some(&past.key), 1000)
        .await
        .unwrap();
    assert!(page.is_empty(), "{page:?}");
}

/// The keys with a digest are exactly the keys the pool replicates: the rule
/// that decides which deletes leave a tombstone decides which keys have a
/// digest, so the two cannot drift apart.
#[tokio::test]
async fn the_keys_with_a_digest_are_the_replicated_keys() {
    let dir = TempDir::new().unwrap();
    let engine = open(dir.path(), CLUSTER).await;
    let keys = [
        ("photos", "cat.jpg"),
        ("photos", "__system__/inside-a-user-bucket"),
        ("__system__", "__s4_bucket_marker_photos"),
        ("__system__", "__s4_bucket_cors_photos"),
        ("__system__", "__s4_iam_user_alice"),
        ("__system__", "__s4_iam_username_alice"),
        ("__system__", "__s4_iam_v2_user_alice"),
        ("__system__", "__s4_iam_v2_root_state"),
        ("__system__", "__s4_ec_manifest_photos/cat.jpg"),
    ];
    for (bucket, key) in keys {
        engine
            .put_object(bucket, key, b"v", "text/plain", &hlc_metadata(10))
            .await
            .unwrap();
    }

    let digested: Vec<String> = engine
        .index_db()
        .version_digests()
        .await
        .unwrap()
        .into_iter()
        .map(|entry| entry.key)
        .collect();
    for (bucket, key) in keys {
        assert_eq!(
            digested.contains(&format!("{bucket}/{key}")),
            s4_core::types::is_replicated_key(bucket, key),
            "{bucket}/{key}"
        );
    }
}

/// An index a release of digest format 3 left — bucket configurations
/// without digests — is rebuilt when it opens: the configurations get their
/// digests, deleted ones as tombstones, and every other digest stays as it
/// was.
#[tokio::test]
async fn an_index_of_format_3_gets_the_digests_of_bucket_configurations() {
    let temp = TempDir::new().unwrap();
    let before = {
        let engine = open(temp.path(), CLUSTER).await;
        engine
            .put_object("photos", "cat.jpg", b"cat", "image/jpeg", &hlc_metadata(10))
            .await
            .unwrap();
        engine
            .put_object(
                "__system__",
                "__s4_bucket_marker_photos",
                b"1",
                "application/octet-stream",
                &hlc_metadata(5),
            )
            .await
            .unwrap();
        engine
            .put_object(
                "__system__",
                "__s4_bucket_cors_photos",
                b"<CORSConfiguration/>",
                "application/xml",
                &hlc_metadata(11),
            )
            .await
            .unwrap();
        engine
            .put_object(
                "__system__",
                "__s4_bucket_lifecycle_photos",
                b"<LifecycleConfiguration/>",
                "application/xml",
                &HashMap::new(),
            )
            .await
            .unwrap();
        engine
            .tombstone_object("__system__", "__s4_bucket_policy_photos", 12, 0, 1)
            .await
            .unwrap();
        engine.index_db().version_digests().await.unwrap()
    };
    let configs: Vec<&str> = before
        .iter()
        .map(|entry| entry.key.as_str())
        .filter(|key| key.starts_with("__system__/__s4_bucket_") && !key.contains("marker"))
        .collect();
    assert_eq!(
        configs.len(),
        3,
        "every configuration has a digest: {before:?}"
    );

    // What a release of format 3 left: the same index, with no digest of a
    // configuration.
    {
        let db = fjall::Database::builder(temp.path().join("metadata_db")).open().unwrap();
        let digests =
            db.keyspace("version_digests", fjall::KeyspaceCreateOptions::default).unwrap();
        let config_keys: Vec<_> = digests
            .iter()
            .map(|guard| guard.into_inner().unwrap().0)
            .filter(|key| {
                let name = String::from_utf8_lossy(&key[8..]);
                name.starts_with("__system__/__s4_bucket_") && !name.contains("marker")
            })
            .collect();
        assert_eq!(config_keys.len(), 3);
        for key in config_keys {
            digests.remove(key).unwrap();
        }
        let meta = db.keyspace("cluster_meta", fjall::KeyspaceCreateOptions::default).unwrap();
        meta.insert("version_digests_format", 3u32.to_be_bytes()).unwrap();
        seal_fixture(temp.path(), &db);
    }

    let engine = open(temp.path(), CLUSTER).await;
    assert!(matches!(
        engine.index_db().version_digests_opening(),
        Some(VersionDigestsOpening::Rebuilt {
            records: 5,
            previous_format: Some(3),
            ..
        })
    ));
    assert_eq!(engine.index_db().version_digests().await.unwrap(), before);
    assert!(engine.index_db().verify_version_digests().await.unwrap().is_consistent());
}

/// An index a release of digest format 4 left — IAM records of version 2
/// without digests — is rebuilt when it opens: the records get their digests,
/// a deleted one as a tombstone, the IAM records of earlier releases get
/// none, and a tombstone of a record reads as the record's version.
#[tokio::test]
async fn an_index_of_format_4_gets_the_digests_of_iam_records() {
    let temp = TempDir::new().unwrap();
    let before = {
        let engine = open(temp.path(), CLUSTER).await;
        engine
            .put_object("photos", "cat.jpg", b"cat", "image/jpeg", &hlc_metadata(10))
            .await
            .unwrap();
        for key in ["__s4_iam_v2_user_alice", "__s4_iam_v2_user_bob"] {
            engine
                .put_object(
                    "__system__",
                    key,
                    b"{}",
                    "application/json",
                    &hlc_metadata(11),
                )
                .await
                .unwrap();
        }
        engine
            .put_object(
                "__system__",
                "__s4_iam_v2_root_state",
                b"{}",
                "application/json",
                &HashMap::new(),
            )
            .await
            .unwrap();
        engine
            .put_object(
                "__system__",
                "__s4_iam_user_carol",
                b"{}",
                "application/json",
                &HashMap::new(),
            )
            .await
            .unwrap();
        assert!(engine
            .tombstone_object("__system__", "__s4_iam_v2_user_bob", 12, 0, 1)
            .await
            .unwrap());
        engine.index_db().version_digests().await.unwrap()
    };
    let iam: Vec<(&str, VersionKind)> = before
        .iter()
        .filter(|entry| entry.key.starts_with("__system__/__s4_iam_"))
        .map(|entry| (entry.key.as_str(), entry.digest.kind))
        .collect();
    let mut sorted = iam.clone();
    sorted.sort_by_key(|(key, _)| *key);
    assert_eq!(
        sorted,
        vec![
            ("__system__/__s4_iam_v2_root_state", VersionKind::Live),
            ("__system__/__s4_iam_v2_user_alice", VersionKind::Live),
            ("__system__/__s4_iam_v2_user_bob", VersionKind::Tombstone),
        ],
        "only the records of version 2 have digests: {before:?}"
    );

    // What a release of format 4 left: the same index, with no digest of an
    // IAM record.
    {
        let db = fjall::Database::builder(temp.path().join("metadata_db")).open().unwrap();
        let digests =
            db.keyspace("version_digests", fjall::KeyspaceCreateOptions::default).unwrap();
        let iam_keys: Vec<_> = digests
            .iter()
            .map(|guard| guard.into_inner().unwrap().0)
            .filter(|key| String::from_utf8_lossy(&key[8..]).starts_with("__system__/__s4_iam_"))
            .collect();
        assert_eq!(iam_keys.len(), 3);
        for key in iam_keys {
            digests.remove(key).unwrap();
        }
        let meta = db.keyspace("cluster_meta", fjall::KeyspaceCreateOptions::default).unwrap();
        meta.insert("version_digests_format", 4u32.to_be_bytes()).unwrap();
        seal_fixture(temp.path(), &db);
    }

    let engine = open(temp.path(), CLUSTER).await;
    assert!(matches!(
        engine.index_db().version_digests_opening(),
        Some(VersionDigestsOpening::Rebuilt {
            records: 4,
            previous_format: Some(4),
            ..
        })
    ));
    assert_eq!(engine.index_db().version_digests().await.unwrap(), before);
    assert!(engine.index_db().verify_version_digests().await.unwrap().is_consistent());

    // The tombstone is the deleted record's version, and no live record.
    assert!(engine.get_object("__system__", "__s4_iam_v2_user_bob").await.is_err());
    let version = engine.key_version("__system__", "__s4_iam_v2_user_bob").await.unwrap();
    assert!(version.record().is_some_and(|record| record.is_delete_marker));
    let listed: Vec<String> = engine
        .list_objects_with_tombstones("__system__", "__s4_iam_v2_user_", None, 10)
        .await
        .unwrap()
        .into_iter()
        .map(|(key, _)| key)
        .collect();
    assert_eq!(
        listed,
        vec!["__s4_iam_v2_user_alice", "__s4_iam_v2_user_bob"]
    );
}

/// A stored layout of another version is rebuilt, the current one is kept,
/// and an index opened without digests forgets them and the horizon.
#[tokio::test]
async fn digests_are_rebuilt_only_when_they_cannot_be_trusted() {
    let temp = TempDir::new().unwrap();
    {
        let engine = open(temp.path(), CLUSTER).await;
        assert!(matches!(
            engine.index_db().version_digests_opening(),
            Some(VersionDigestsOpening::Rebuilt {
                records: 0,
                previous_format: None,
                ..
            })
        ));
        engine
            .put_object("photos", "a", b"one", "text/plain", &hlc_metadata(10))
            .await
            .unwrap();
        engine.tombstone_object("photos", "b", 11, 0, 1).await.unwrap();
        engine.index_db().seal_version_digests().unwrap();
    }
    {
        let engine = open(temp.path(), CLUSTER).await;
        assert_eq!(
            engine.index_db().version_digests_opening(),
            Some(&VersionDigestsOpening::Current)
        );
        assert_eq!(engine.index_db().version_digests().await.unwrap().len(), 2);
    }

    {
        let db = fjall::Database::builder(temp.path().join("metadata_db")).open().unwrap();
        let meta = db.keyspace("cluster_meta", fjall::KeyspaceCreateOptions::default).unwrap();
        meta.insert("version_digests_format", 0u32.to_be_bytes()).unwrap();
        seal_fixture(temp.path(), &db);
    }
    {
        let engine = open(temp.path(), CLUSTER).await;
        assert!(matches!(
            engine.index_db().version_digests_opening(),
            Some(VersionDigestsOpening::Rebuilt {
                records: 2,
                previous_format: Some(0),
                ..
            })
        ));
        engine
            .index_db()
            .settle_tombstone_horizon(TombstoneHorizon {
                hlc_timestamp: 50,
                hlc_logical: 0,
                origin_node_id: 1,
            })
            .await
            .unwrap();
    }

    {
        // Single mode: its writes keep no digests, its deletes no tombstones.
        let engine = open(temp.path(), IndexOptions::default()).await;
        assert!(!engine.index_db().keeps_version_digests());
        assert_eq!(engine.index_db().version_digests_opening(), None);
        engine.delete_object("photos", "a").await.unwrap();
    }
    let engine = open(temp.path(), CLUSTER).await;
    assert!(matches!(
        engine.index_db().version_digests_opening(),
        Some(VersionDigestsOpening::Rebuilt {
            records: 1,
            previous_format: None,
            ..
        })
    ));
    assert_eq!(engine.index_db().tombstone_horizon().await.unwrap(), None);
    assert!(engine.index_db().verify_version_digests().await.unwrap().is_consistent());
}

/// A release that keeps no digests, run on this data — a node rolled back and
/// upgraded again — changes records without their digests. The seal a node
/// leaves when it stops tells: an open that finds the index as sealed keeps
/// the digests, one that finds it written since rebuilds them. Kept stale,
/// they held a growing pool's last range from ever being verified (scenario
/// 61, group H).
#[tokio::test]
async fn digests_written_behind_the_seal_are_rebuilt() {
    let temp = TempDir::new().unwrap();
    {
        let engine = open(temp.path(), CLUSTER).await;
        engine
            .put_object("photos", "a", b"one", "text/plain", &hlc_metadata(10))
            .await
            .unwrap();
        engine
            .put_object("photos", "b", b"two", "text/plain", &hlc_metadata(11))
            .await
            .unwrap();
        engine.index_db().seal_version_digests().unwrap();
    }
    {
        let engine = open(temp.path(), CLUSTER).await;
        assert_eq!(
            engine.index_db().version_digests_opening(),
            Some(&VersionDigestsOpening::Current)
        );
        engine.index_db().seal_version_digests().unwrap();
    }
    {
        // The earlier release deletes a record outright.
        let db = fjall::Database::builder(temp.path().join("metadata_db")).open().unwrap();
        let objects = db.keyspace("objects", fjall::KeyspaceCreateOptions::default).unwrap();
        let key = objects
            .iter()
            .map(|guard| guard.key().unwrap())
            .find(|key| key.ends_with(b"/a"))
            .expect("the record of photos/a");
        objects.remove(key).unwrap();
        db.persist(fjall::PersistMode::SyncAll).unwrap();
    }
    let engine = open(temp.path(), CLUSTER).await;
    assert!(
        matches!(
            engine.index_db().version_digests_opening(),
            Some(VersionDigestsOpening::Rebuilt { records: 1, .. })
        ),
        "{:?}",
        engine.index_db().version_digests_opening()
    );
    assert!(engine.index_db().verify_version_digests().await.unwrap().is_consistent());
}

/// An index rebuilt from the volumes after its records were lost keeps no
/// digest of a record the rebuild could not bring back: the namespace entry
/// of an object in erasure coding has no blob in a volume.
#[tokio::test]
async fn a_recovered_index_keeps_no_digest_of_a_lost_record() {
    let temp = TempDir::new().unwrap();
    {
        let engine = open(temp.path(), CLUSTER).await;
        engine
            .put_object("photos", "a", b"one", "text/plain", &hlc_metadata(10))
            .await
            .unwrap();
        let mut coded = s4_core::IndexRecord::new(
            0,
            0,
            3,
            [3; 32],
            "etag".to_string(),
            "text/plain".to_string(),
        )
        .into_erasure_coded(9, 2);
        coded.hlc_timestamp = 11;
        coded.origin_node_id = 2;
        engine.restore_erasure_coded_entry("photos", "coded", coded).await.unwrap();
        engine.sync().await.unwrap();
        assert_eq!(engine.index_db().version_digests().await.unwrap().len(), 2);
    }
    {
        let db = fjall::Database::builder(temp.path().join("metadata_db")).open().unwrap();
        for name in ["objects", "journal"] {
            db.keyspace(name, fjall::KeyspaceCreateOptions::default)
                .unwrap()
                .clear()
                .unwrap();
        }
        db.persist(fjall::PersistMode::SyncAll).unwrap();
    }

    let engine = open(temp.path(), CLUSTER).await;
    assert!(matches!(
        engine.key_version("photos", "a").await.unwrap(),
        KeyVersion::Live(_)
    ));
    assert!(matches!(
        engine.key_version("photos", "coded").await.unwrap(),
        KeyVersion::Absent
    ));
    let check = engine.index_db().verify_version_digests().await.unwrap();
    assert!(check.is_consistent(), "{check:?}");
    // The recovery from the volumes also puts back the bucket's marker.
    let keys: Vec<String> = engine
        .index_db()
        .version_digests()
        .await
        .unwrap()
        .into_iter()
        .map(|entry| entry.key)
        .collect();
    assert!(!keys.contains(&"photos/coded".to_string()), "{keys:?}");
}

/// The horizon is decided on the first start that keeps tombstones: zero for
/// an index that start created, the given time for any other — one with
/// records, and one emptied or never written before, whose history may still
/// hold deletes without tombstones. A restart keeps it, whatever the index
/// holds by then.
/// Tombstone GC removes a page of tombstones, their digests and raises the
/// purge horizon in one batch; the horizon survives a restart, never falls,
/// and is what the node reports once it is past the settled horizon. The
/// settled horizon itself stays as decided.
#[tokio::test]
async fn removing_tombstones_raises_a_horizon_that_survives_a_restart() {
    let dir = TempDir::new().unwrap();
    let tombstone = |key: &str, wall: u64| TombstoneRef {
        full_key: format!("photos/{key}"),
        hlc: (wall, 0, 4),
    };
    {
        let engine = open(dir.path(), CLUSTER).await;
        engine
            .index_db()
            .settle_tombstone_horizon(TombstoneHorizon::ZERO)
            .await
            .unwrap();
        for (key, wall) in [("a", 300), ("b", 500), ("c", 400)] {
            assert!(engine.tombstone_object("photos", key, wall, 0, 4).await.unwrap());
        }
        assert_eq!(
            engine.index_db().tombstone_purge_horizon().await.unwrap(),
            None
        );
        assert_eq!(
            engine.gc_tombstones(&[tombstone("a", 300), tombstone("b", 500)]).await.unwrap(),
            2
        );
        assert!(engine.index_db().verify_version_digests().await.unwrap().is_consistent());
        engine.index_db().seal_version_digests().unwrap();
    }
    let engine = open(dir.path(), CLUSTER).await;
    let at = |wall| TombstoneHorizon {
        hlc_timestamp: wall,
        hlc_logical: 0,
        origin_node_id: 4,
    };
    assert_eq!(
        engine.index_db().tombstone_purge_horizon().await.unwrap(),
        Some(at(500))
    );
    assert_eq!(engine.tombstone_horizon().await.unwrap(), Some(at(500)));
    assert_eq!(
        engine.gc_tombstones(&[tombstone("c", 400)]).await.unwrap(),
        1
    );
    assert_eq!(
        engine.tombstone_horizon().await.unwrap(),
        Some(at(500)),
        "the horizon never falls"
    );
    assert_eq!(
        engine.index_db().tombstone_horizon().await.unwrap(),
        Some(TombstoneHorizon::ZERO),
        "the settled horizon is the one the start decided"
    );
    let digests = engine.index_db().version_digests().await.unwrap();
    assert!(digests.is_empty(), "{digests:?}");
}

#[tokio::test]
async fn the_tombstone_horizon_is_decided_once() {
    let at = |wall: u64| TombstoneHorizon {
        hlc_timestamp: wall,
        hlc_logical: 1,
        origin_node_id: 4,
    };

    let empty = TempDir::new().unwrap();
    {
        let engine = open(empty.path(), CLUSTER).await;
        assert_eq!(engine.index_db().tombstone_horizon().await.unwrap(), None);
        assert_eq!(
            engine.index_db().settle_tombstone_horizon(at(100)).await.unwrap(),
            TombstoneHorizon::ZERO
        );
        engine
            .put_object("photos", "a", b"one", "text/plain", &hlc_metadata(200))
            .await
            .unwrap();
        engine.index_db().seal_version_digests().unwrap();
    }
    {
        let engine = open(empty.path(), CLUSTER).await;
        assert_eq!(
            engine.index_db().settle_tombstone_horizon(at(300)).await.unwrap(),
            TombstoneHorizon::ZERO,
            "a restart keeps the horizon decided first"
        );
        assert_eq!(
            engine.index_db().tombstone_horizon().await.unwrap(),
            Some(TombstoneHorizon::ZERO)
        );
    }

    let with_data = TempDir::new().unwrap();
    {
        // Written before tombstones were kept: its deletes left nothing.
        let engine = open(with_data.path(), IndexOptions::default()).await;
        engine
            .put_object("photos", "a", b"one", "text/plain", &HashMap::new())
            .await
            .unwrap();
    }
    {
        let engine = open(with_data.path(), CLUSTER).await;
        assert_eq!(
            engine.index_db().settle_tombstone_horizon(at(100)).await.unwrap(),
            at(100)
        );
        engine.delete_object("photos", "a").await.unwrap();
        engine.index_db().seal_version_digests().unwrap();
    }
    let engine = open(with_data.path(), CLUSTER).await;
    assert_eq!(
        engine.index_db().settle_tombstone_horizon(at(900)).await.unwrap(),
        at(100),
        "emptied since, the index still had deletes without tombstones"
    );

    let emptied_before = TempDir::new().unwrap();
    {
        // Every key deleted before the first start with tombstones: the
        // deletes left nothing, and a copy another replica kept of one of
        // them must not count as never deleted here.
        let engine = open(emptied_before.path(), IndexOptions::default()).await;
        engine
            .put_object("photos", "a", b"one", "text/plain", &HashMap::new())
            .await
            .unwrap();
        engine.delete_object("photos", "a").await.unwrap();
    }
    {
        let engine = open(emptied_before.path(), CLUSTER).await;
        assert!(engine.index_db().version_digests().await.unwrap().is_empty());
        assert_eq!(
            engine.index_db().settle_tombstone_horizon(at(100)).await.unwrap(),
            at(100)
        );
    }

    let opened_before = TempDir::new().unwrap();
    drop(open(opened_before.path(), IndexOptions::default()).await);
    let engine = open(opened_before.path(), CLUSTER).await;
    assert_eq!(
        engine.index_db().settle_tombstone_horizon(at(100)).await.unwrap(),
        at(100),
        "an index opened without tombstones before has a history this one did not see"
    );

    let without_digests = TempDir::new().unwrap();
    let engine = open(without_digests.path(), IndexOptions::default()).await;
    assert!(engine.index_db().settle_tombstone_horizon(at(1)).await.is_err());
}

const KILLED_WRITER_DIR: &str = "S4_TEST_DIGEST_WRITER_DIR";

/// The writer the kill test runs in a process of its own: it writes until it
/// is killed.
async fn write_until_killed(dir: &Path) {
    let engine = open(dir, CLUSTER).await;
    let mut rng = Rng(std::process::id() as u64 | 1);
    let mut clock = 1_000_000u64;
    let mut written = HashMap::new();
    // Exercise a descheduled writer before its first record is persisted.
    tokio::time::sleep(Duration::from_millis(500)).await;
    engine
        .put_object(
            "photos",
            &format!("acknowledged-{}", std::process::id()),
            b"persisted before the kill",
            "text/plain",
            &hlc_metadata(clock),
        )
        .await
        .unwrap();
    engine.sync().await.unwrap();
    println!("record persisted");
    loop {
        random_write(&engine, &mut rng, &mut clock, &mut written).await;
    }
}

/// A process killed in the middle of writes leaves digests that match its
/// records: the digest is in the batch of the record. Verify this before
/// startup's conservative rebuild, which cannot distinguish the crash from
/// writes by a release that keeps no digests.
#[tokio::test]
async fn a_kill_in_the_middle_of_writes_leaves_digests_that_match_the_records() {
    if let Ok(dir) = std::env::var(KILLED_WRITER_DIR) {
        write_until_killed(Path::new(&dir)).await;
        return;
    }

    let temp = TempDir::new().unwrap();
    {
        let engine = open(temp.path(), CLUSTER).await;
        engine.index_db().seal_version_digests().unwrap();
    }
    for round in 0..3u64 {
        let mut writer = tokio::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "a_kill_in_the_middle_of_writes_leaves_digests_that_match_the_records",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(KILLED_WRITER_DIR, temp.path())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let mut stdout = tokio::io::BufReader::new(writer.stdout.take().unwrap());
        let ready = tokio::time::timeout(Duration::from_secs(10), async {
            let mut line = String::new();
            loop {
                line.clear();
                if tokio::io::AsyncBufReadExt::read_line(&mut stdout, &mut line).await? == 0 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "the writer stopped before persisting a record",
                    ));
                }
                if line.contains("record persisted") {
                    return Ok(());
                }
            }
        })
        .await;
        if matches!(ready, Ok(Ok(()))) {
            tokio::time::sleep(Duration::from_millis(150 + 100 * round)).await;
        }
        // Kill and reap on timeout, EOF and read errors too, before reporting them.
        writer.kill().await.unwrap();
        ready.expect("the writer did not persist a record within 10 seconds").unwrap();

        check_crashed_digests(temp.path());
        let engine = open(temp.path(), CLUSTER).await;
        assert!(
            matches!(
                engine.index_db().version_digests_opening(),
                Some(VersionDigestsOpening::Rebuilt { .. })
            ),
            "round {round}: the old seal cannot attest to the recovered history"
        );
        let check = engine.index_db().verify_version_digests().await.unwrap();
        assert!(check.is_consistent(), "round {round}: {check:?}");
        assert!(
            check.expected > 0,
            "round {round}: the writer wrote nothing"
        );
    }
}

/// The adoption of bucket configurations, and the quorum reads of them the
/// pool's adoption opened, are remembered across cluster starts, and
/// forgotten by an open without digests: single mode writes configurations
/// without an HLC again, and the next cluster start has them to adopt.
#[tokio::test]
async fn adopted_configurations_are_remembered_until_an_open_without_digests() {
    let temp = TempDir::new().unwrap();
    {
        let engine = open(temp.path(), CLUSTER).await;
        let index = engine.index_db();
        assert!(!index.bucket_configs_adopted().await.unwrap());
        assert!(!index.bucket_config_quorum_reads().await.unwrap());
        index.mark_bucket_configs_adopted().await.unwrap();
        index.mark_bucket_config_quorum_reads().await.unwrap();
        assert!(index.bucket_configs_adopted().await.unwrap());
        assert!(index.bucket_config_quorum_reads().await.unwrap());
        index.seal_version_digests().unwrap();
    }
    {
        let engine = open(temp.path(), CLUSTER).await;
        assert!(engine.index_db().bucket_configs_adopted().await.unwrap());
        assert!(engine.index_db().bucket_config_quorum_reads().await.unwrap());
    }
    drop(open(temp.path(), IndexOptions::default()).await);
    let engine = open(temp.path(), CLUSTER).await;
    assert!(!engine.index_db().bucket_configs_adopted().await.unwrap());
    assert!(engine.index_db().bucket_config_quorum_reads().await.unwrap());
}

/// The pool's adoption of IAM is remembered across cluster starts and
/// forgotten by an open without digests: a single node keeps IAM of its own
/// again, which the next cluster start has to be taken over.
#[tokio::test]
async fn adopted_iam_is_remembered_until_an_open_without_digests() {
    let temp = TempDir::new().unwrap();
    {
        let engine = open(temp.path(), CLUSTER).await;
        assert!(!engine.index_db().iam_adopted().await.unwrap());
        engine.index_db().mark_iam_adopted().await.unwrap();
        assert!(engine.index_db().iam_adopted().await.unwrap());
        engine.index_db().seal_version_digests().unwrap();
    }
    {
        let engine = open(temp.path(), CLUSTER).await;
        assert!(engine.index_db().iam_adopted().await.unwrap());
    }
    drop(open(temp.path(), IndexOptions::default()).await);
    let engine = open(temp.path(), CLUSTER).await;
    assert!(!engine.index_db().iam_adopted().await.unwrap());
}

/// The records of a node's progress in moving the pool's keys come back in
/// name order across cluster starts, a later record of one name in place of
/// an earlier one, and an open without digests keeps them: a restarted move
/// continues from its cursors rather than from the beginning.
#[tokio::test]
async fn rebalance_progress_survives_every_open_in_name_order() {
    let temp = TempDir::new().unwrap();
    {
        let engine = open(temp.path(), CLUSTER).await;
        let index = engine.index_db();
        assert!(index.rebalance_entries().await.unwrap().is_empty());
        index.store_rebalance_entry("2/range/9", vec![9]).await.unwrap();
        index.store_rebalance_entry("2/control", vec![1]).await.unwrap();
        index.store_rebalance_entry("2/range/10", vec![10]).await.unwrap();
        index.store_rebalance_entry("2/range/9", vec![9, 9]).await.unwrap();
    }
    let expected = vec![
        ("2/control".to_string(), vec![1]),
        ("2/range/10".to_string(), vec![10]),
        ("2/range/9".to_string(), vec![9, 9]),
    ];
    {
        let engine = open(temp.path(), CLUSTER).await;
        assert_eq!(
            engine.index_db().rebalance_entries().await.unwrap(),
            expected
        );
    }
    drop(open(temp.path(), IndexOptions::default()).await);
    let engine = open(temp.path(), CLUSTER).await;
    assert_eq!(
        engine.index_db().rebalance_entries().await.unwrap(),
        expected
    );
}

/// The record of the bucket metadata placement is kept across cluster
/// starts, and an open without digests keeps it too: the stage of a node
/// only grows, so no open may take it back.
#[tokio::test]
async fn the_bucket_metadata_placement_survives_every_open() {
    let temp = TempDir::new().unwrap();
    {
        let engine = open(temp.path(), CLUSTER).await;
        let index = engine.index_db();
        assert_eq!(index.bucket_metadata_placement().await.unwrap(), None);
        index.store_bucket_metadata_placement(vec![1, 3, 5, 1, 0]).await.unwrap();
        index.store_bucket_metadata_placement(vec![1, 3, 5, 2, 1]).await.unwrap();
    }
    {
        let engine = open(temp.path(), CLUSTER).await;
        assert_eq!(
            engine.index_db().bucket_metadata_placement().await.unwrap(),
            Some(vec![1, 3, 5, 2, 1])
        );
    }
    drop(open(temp.path(), IndexOptions::default()).await);
    let engine = open(temp.path(), CLUSTER).await;
    assert_eq!(
        engine.index_db().bucket_metadata_placement().await.unwrap(),
        Some(vec![1, 3, 5, 2, 1])
    );
}
