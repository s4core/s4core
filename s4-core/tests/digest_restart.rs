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

//! Restart proofs for digests and the history of deletes and adoption.

use s4_core::storage::{
    BitcaskStorageEngine, IndexOptions, TombstoneHorizon, TombstoneRef, VersionDigestsOpening,
};
use s4_core::StorageEngine;
use std::collections::HashMap;
use std::path::Path;
use tempfile::TempDir;

const NOW: TombstoneHorizon = TombstoneHorizon {
    hlc_timestamp: 100,
    hlc_logical: 0,
    origin_node_id: 1,
};

async fn adopt(engine: &BitcaskStorageEngine) {
    let index = engine.index_db();
    assert_eq!(
        index.settle_tombstone_horizon(NOW).await.unwrap(),
        TombstoneHorizon::ZERO
    );
    index.mark_bucket_configs_adopted().await.unwrap();
    index.mark_bucket_config_quorum_reads().await.unwrap();
    index.mark_iam_adopted().await.unwrap();
}

async fn history_is_unknown(engine: &BitcaskStorageEngine) {
    let index = engine.index_db();
    assert_eq!(index.tombstone_horizon().await.unwrap(), None);
    assert!(!index.bucket_configs_adopted().await.unwrap());
    assert!(
        index.bucket_config_quorum_reads().await.unwrap(),
        "quorum reads never revert to local fallback"
    );
    assert!(!index.iam_adopted().await.unwrap());
    assert_eq!(index.settle_tombstone_horizon(NOW).await.unwrap(), NOW);
}

async fn open(dir: &Path) -> BitcaskStorageEngine {
    BitcaskStorageEngine::open(
        dir.join("volumes"),
        dir.join("metadata_db"),
        64 * 1024,
        0,
        false,
        IndexOptions {
            version_digests: true,
        },
    )
    .await
    .unwrap()
}

async fn seed(engine: &BitcaskStorageEngine, key: &str) {
    let metadata = HashMap::from([
        ("_s4_hlc_wall".into(), "10".into()),
        ("_s4_hlc_logical".into(), "0".into()),
        ("_s4_hlc_node_id".into(), "1".into()),
    ]);
    engine.put_object("photos", key, b"one", "text/plain", &metadata).await.unwrap();
}

fn legacy_delete(dir: &Path) {
    let db = fjall::Database::builder(dir.join("metadata_db")).open().unwrap();
    let objects = db.keyspace("objects", fjall::KeyspaceCreateOptions::default).unwrap();
    let key = objects
        .iter()
        .map(|guard| guard.key().unwrap())
        .find(|key| key.ends_with(b"/a"))
        .expect("the stored record of photos/a");
    assert!(
        objects.len().unwrap() > 1,
        "keep the index populated to avoid lost-index recovery"
    );
    objects.remove(key.clone()).unwrap();
    assert!(!objects.contains_key(key).unwrap());
    db.persist(fjall::PersistMode::SyncAll).unwrap();
}

#[tokio::test]
async fn sealed_clean_restart_keeps_digests() {
    let dir = TempDir::new().unwrap();
    {
        let engine = open(dir.path()).await;
        adopt(&engine).await;
        seed(&engine, "a").await;
        seed(&engine, "kept").await;
        engine.index_db().seal_version_digests().unwrap();
    }
    let engine = open(dir.path()).await;
    assert_eq!(
        engine.index_db().version_digests_opening(),
        Some(&VersionDigestsOpening::Current)
    );
    assert!(engine.index_db().verify_version_digests().await.unwrap().is_consistent());
    let index = engine.index_db();
    assert_eq!(
        index.settle_tombstone_horizon(NOW).await.unwrap(),
        TombstoneHorizon::ZERO
    );
    assert!(index.bucket_configs_adopted().await.unwrap());
    assert!(index.bucket_config_quorum_reads().await.unwrap());
    assert!(index.iam_adopted().await.unwrap());
}

#[tokio::test]
async fn writes_after_an_existing_seal_force_rebuild_without_corruption() {
    const WRITER_DIR: &str = "S4_DIGEST_AUDIT_WRITER_DIR";
    if let Ok(dir) = std::env::var(WRITER_DIR) {
        let engine = open(Path::new(&dir)).await;
        assert_eq!(
            engine.index_db().version_digests_opening(),
            Some(&VersionDigestsOpening::Current)
        );
        seed(&engine, "b").await;
        println!("audit-ready");
        std::future::pending::<()>().await;
        return;
    }
    let dir = TempDir::new().unwrap();
    {
        let engine = open(dir.path()).await;
        adopt(&engine).await;
        seed(&engine, "a").await;
        seed(&engine, "kept").await;
        engine.index_db().seal_version_digests().unwrap();
    }
    let mut writer = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "writes_after_an_existing_seal_force_rebuild_without_corruption",
            "--nocapture",
        ])
        .env(WRITER_DIR, dir.path())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdout = std::io::BufReader::new(writer.stdout.take().unwrap());
    loop {
        let mut line = String::new();
        assert!(
            std::io::BufRead::read_line(&mut stdout, &mut line).unwrap() > 0,
            "writer exited before its write"
        );
        if line.contains("audit-ready") {
            break;
        }
    }
    writer.kill().unwrap();
    writer.wait().unwrap();
    let engine = open(dir.path()).await;
    assert!(matches!(
        engine.index_db().version_digests_opening(),
        Some(VersionDigestsOpening::Rebuilt { .. })
    ));
    assert!(engine.index_db().verify_version_digests().await.unwrap().is_consistent());
    history_is_unknown(&engine).await;
}

#[tokio::test]
async fn legacy_deletion_is_rebuilt_before_later_supported_writes() {
    let dir = TempDir::new().unwrap();
    {
        let engine = open(dir.path()).await;
        adopt(&engine).await;
        seed(&engine, "a").await;
        seed(&engine, "kept").await;
    }
    legacy_delete(dir.path());
    {
        let engine = open(dir.path()).await;
        history_is_unknown(&engine).await;
        seed(&engine, "later").await;
        let check = engine.index_db().verify_version_digests().await.unwrap();
        assert!(
            check.is_consistent(),
            "a later write must not hide an earlier gap: {check:?}"
        );
        engine.index_db().seal_version_digests().unwrap();
    }
    let engine = open(dir.path()).await;
    assert_eq!(
        engine.index_db().version_digests_opening(),
        Some(&VersionDigestsOpening::Current)
    );
    assert!(engine.index_db().verify_version_digests().await.unwrap().is_consistent());
    assert_eq!(
        engine.index_db().settle_tombstone_horizon(NOW).await.unwrap(),
        NOW
    );
}

#[tokio::test]
async fn legacy_write_without_any_seal_must_not_leave_stale_digests() {
    let dir = TempDir::new().unwrap();
    {
        let engine = open(dir.path()).await;
        adopt(&engine).await;
        seed(&engine, "a").await;
        seed(&engine, "kept").await;
        // No seal: first start ended before graceful shutdown.
    }
    legacy_delete(dir.path());
    let engine = open(dir.path()).await;
    let check = engine.index_db().verify_version_digests().await.unwrap();
    assert!(
        check.is_consistent(),
        "opening={:?}, check={check:?}",
        engine.index_db().version_digests_opening()
    );
    history_is_unknown(&engine).await;
}

#[tokio::test]
async fn legacy_write_with_unreadable_seal_must_not_leave_stale_digests() {
    for contents in ["incomplete", "", "18446744073709551616"] {
        let dir = TempDir::new().unwrap();
        {
            let engine = open(dir.path()).await;
            adopt(&engine).await;
            seed(&engine, "a").await;
            seed(&engine, "kept").await;
            engine.index_db().seal_version_digests().unwrap();
        }
        std::fs::write(dir.path().join("metadata_db.digests-sealed"), contents).unwrap();
        legacy_delete(dir.path());
        let engine = open(dir.path()).await;
        let check = engine.index_db().verify_version_digests().await.unwrap();
        assert!(
            check.is_consistent(),
            "opening={:?}, check={check:?}",
            engine.index_db().version_digests_opening()
        );
        history_is_unknown(&engine).await;
    }
}

#[tokio::test]
async fn legacy_delete_after_seal_must_not_keep_a_zero_tombstone_horizon() {
    let dir = TempDir::new().unwrap();
    let now = TombstoneHorizon {
        hlc_timestamp: 100,
        hlc_logical: 0,
        origin_node_id: 1,
    };
    {
        let engine = open(dir.path()).await;
        assert_eq!(
            engine.index_db().settle_tombstone_horizon(now).await.unwrap(),
            TombstoneHorizon::ZERO
        );
        seed(&engine, "a").await;
        seed(&engine, "kept").await;
        engine.index_db().seal_version_digests().unwrap();
    }
    legacy_delete(dir.path());
    let engine = open(dir.path()).await;
    assert!(engine.index_db().verify_version_digests().await.unwrap().is_consistent());
    assert_eq!(
        engine.index_db().settle_tombstone_horizon(now).await.unwrap(),
        now,
        "the legacy delete left no tombstone, so zero cannot attest to the delete history"
    );
}

#[tokio::test]
async fn untrusted_restart_retains_purge_horizon_and_move_progress() {
    let dir = TempDir::new().unwrap();
    let purged = TombstoneHorizon {
        hlc_timestamp: 20,
        ..NOW
    };
    {
        let engine = open(dir.path()).await;
        adopt(&engine).await;
        seed(&engine, "a").await;
        seed(&engine, "kept").await;
        engine.tombstone_object("photos", "removed", 20, 0, 1).await.unwrap();
        assert_eq!(
            engine
                .gc_tombstones(&[TombstoneRef {
                    full_key: "photos/removed".into(),
                    hlc: (20, 0, 1),
                }])
                .await
                .unwrap(),
            1
        );
        let index = engine.index_db();
        index.store_pool_generation(vec![1, 2]).await.unwrap();
        index.store_bucket_metadata_placement(vec![3]).await.unwrap();
        index.store_rebalance_entry("2/range/1", vec![4]).await.unwrap();
        index.seal_version_digests().unwrap();
    }
    legacy_delete(dir.path());
    let engine = open(dir.path()).await;
    history_is_unknown(&engine).await;
    let index = engine.index_db();
    assert_eq!(index.tombstone_purge_horizon().await.unwrap(), Some(purged));
    assert_eq!(index.pool_generation().await.unwrap(), Some(vec![1, 2]));
    assert_eq!(
        index.bucket_metadata_placement().await.unwrap(),
        Some(vec![3])
    );
    assert_eq!(
        index.rebalance_entries().await.unwrap(),
        vec![("2/range/1".into(), vec![4])]
    );
}

#[tokio::test]
async fn an_untrusted_restart_cannot_lower_the_previous_history_boundary() {
    let dir = TempDir::new().unwrap();
    {
        let engine = open(dir.path()).await;
        seed(&engine, "a").await;
        seed(&engine, "kept").await;
    }
    let previous = TombstoneHorizon {
        hlc_timestamp: 200,
        ..NOW
    };
    {
        let engine = open(dir.path()).await;
        assert_eq!(
            engine.index_db().settle_tombstone_horizon(previous).await.unwrap(),
            previous
        );
        engine.index_db().seal_version_digests().unwrap();
    }
    legacy_delete(dir.path());
    let engine = open(dir.path()).await;
    assert_eq!(
        engine.index_db().settle_tombstone_horizon(NOW).await.unwrap(),
        previous,
        "a backward clock adjustment cannot strengthen an uncertain history guarantee"
    );
}

#[tokio::test]
async fn an_older_build_cannot_lower_a_retained_history_boundary() {
    let dir = TempDir::new().unwrap();
    {
        let engine = open(dir.path()).await;
        seed(&engine, "a").await;
        seed(&engine, "kept").await;
    }
    let previous = TombstoneHorizon {
        hlc_timestamp: 200,
        ..NOW
    };
    {
        let engine = open(dir.path()).await;
        assert_eq!(
            engine.index_db().settle_tombstone_horizon(previous).await.unwrap(),
            previous
        );
        engine.index_db().seal_version_digests().unwrap();
    }
    legacy_delete(dir.path());
    // Interrupted before bootstrap settles the invalidated history.
    drop(open(dir.path()).await);
    {
        let db = fjall::Database::builder(dir.path().join("metadata_db")).open().unwrap();
        let meta = db.keyspace("cluster_meta", fjall::KeyspaceCreateOptions::default).unwrap();
        // An older digest-aware build ignores the retained lower bound.
        let mut encoded = Vec::new();
        encoded.extend_from_slice(&NOW.hlc_timestamp.to_be_bytes());
        encoded.extend_from_slice(&NOW.hlc_logical.to_be_bytes());
        encoded.extend_from_slice(&NOW.origin_node_id.to_be_bytes());
        meta.insert("tombstone_horizon", encoded).unwrap();
        db.persist(fjall::PersistMode::SyncAll).unwrap();
        std::fs::write(
            dir.path().join("metadata_db.digests-sealed"),
            db.seqno().to_string(),
        )
        .unwrap();
    }
    let engine = open(dir.path()).await;
    assert_eq!(
        engine.index_db().settle_tombstone_horizon(NOW).await.unwrap(),
        previous,
        "a matching legacy seal cannot erase the retained lower bound"
    );
}

/// Digest format 3 predates replicated bucket configurations; its first
/// upgrade must retain the original adoption of its sole local legacy copy.
#[tokio::test]
async fn first_upgrade_of_format_three_keeps_initial_configuration_adoption() {
    let dir = TempDir::new().unwrap();
    {
        let engine = open(dir.path()).await;
        engine
            .put_object(
                "__system__",
                "__s4_bucket_cors_photos",
                b"<legacy/>",
                "application/xml",
                &HashMap::new(),
            )
            .await
            .unwrap();
    }
    {
        let db = fjall::Database::builder(dir.path().join("metadata_db")).open().unwrap();
        let meta = db.keyspace("cluster_meta", fjall::KeyspaceCreateOptions::default).unwrap();
        meta.insert("version_digests_format", 3u32.to_be_bytes()).unwrap();
        db.persist(fjall::PersistMode::SyncAll).unwrap();
    }
    let engine = open(dir.path()).await;
    assert!(
        !engine.index_db().bucket_config_history_requires_confirmation().await.unwrap(),
        "the first upgrade from before configuration replication retains initial adoption"
    );
    assert_eq!(
        engine.get_object("__system__", "__s4_bucket_cors_photos").await.unwrap().0,
        b"<legacy/>"
    );
}
