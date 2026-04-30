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

//! Integration tests for volume compaction.
//!
//! These tests create a full BitcaskStorageEngine, write objects,
//! delete some to create dead space, then run compaction and verify
//! that all live data is preserved and dead space is reclaimed.

use s4_compactor::{CompactionConfig, VolumeCompactor};
use s4_core::storage::BitcaskStorageEngine;
use s4_core::StorageEngine;
use std::collections::HashMap;
use tempfile::TempDir;

/// Creates a test engine with small volume size to force rotation.
async fn create_test_engine(temp: &TempDir) -> BitcaskStorageEngine {
    let data_path = temp.path().join("volumes");
    let metadata_path = temp.path().join("metadata_db");
    std::fs::create_dir_all(&data_path).unwrap();

    BitcaskStorageEngine::new(
        data_path,
        metadata_path,
        4096, // 4KB volumes — forces frequent rotation
        0,    // no inline storage
        false,
    )
    .await
    .unwrap()
}

/// Writes N objects with unique content, returns the keys and data.
async fn write_objects(
    engine: &BitcaskStorageEngine,
    bucket: &str,
    count: usize,
    prefix: &str,
) -> Vec<(String, Vec<u8>)> {
    let mut written = Vec::new();
    let metadata = HashMap::new();

    for i in 0..count {
        let key = format!("{}{:04}", prefix, i);
        // Each object gets unique content (200 bytes) to avoid dedup
        let data: Vec<u8> = format!(
            "unique-content-{}-{:04}-padding{}",
            prefix,
            i,
            "x".repeat(150)
        )
        .into_bytes();

        engine
            .put_object(bucket, &key, &data, "application/octet-stream", &metadata)
            .await
            .unwrap();
        written.push((key, data));
    }

    written
}

#[tokio::test]
async fn test_compaction_reclaims_dead_space() {
    let temp = TempDir::new().unwrap();
    let engine = create_test_engine(&temp).await;
    let bucket = "test-compact";

    // Write 20 objects (spread across multiple small volumes)
    let objects = write_objects(&engine, bucket, 20, "obj-").await;
    engine.sync().await.unwrap();

    // Delete 15 of 20 objects to create >50% dead space
    for (key, _) in objects.iter().take(15) {
        engine.delete_object(bucket, key).await.unwrap();
    }
    engine.sync().await.unwrap();

    // Count volume files before compaction
    let volumes_before: Vec<_> = std::fs::read_dir(temp.path().join("volumes"))
        .unwrap()
        .filter_map(|e| {
            let e = e.ok()?;
            let name = e.file_name().to_string_lossy().to_string();
            if name.starts_with("volume_") && name.ends_with(".dat") {
                Some(name)
            } else {
                None
            }
        })
        .collect();

    assert!(
        volumes_before.len() > 1,
        "Expected multiple volumes, got {}",
        volumes_before.len()
    );

    // Run compaction with low threshold to ensure it triggers
    let config = CompactionConfig {
        fragmentation_threshold: 0.1, // 10% dead → compact
        min_dead_bytes: 0,            // no minimum
        max_volumes_per_run: 100,
        dry_run: false,
        ..Default::default()
    };

    let compactor = VolumeCompactor::new(
        engine.volumes_dir().to_path_buf(),
        engine.index_db().clone(),
        engine.deduplicator().clone(),
        engine.volume_writer().clone(),
        config,
    );

    let stats = compactor.run().await.unwrap();

    // Should have compacted at least some volumes
    assert!(
        stats.volumes_compacted > 0,
        "Expected compacted volumes, stats: {:?}",
        stats
    );
    assert!(stats.total_bytes_reclaimed > 0, "Expected bytes reclaimed");

    // Verify all surviving objects are still readable with correct data
    for (key, expected_data) in objects.iter().skip(15) {
        let (data, _record) = engine.get_object(bucket, key).await.unwrap();
        assert_eq!(
            &data, expected_data,
            "Data mismatch for surviving object {}",
            key
        );
    }
}

#[tokio::test]
async fn test_compaction_dry_run_preserves_data() {
    let temp = TempDir::new().unwrap();
    let engine = create_test_engine(&temp).await;
    let bucket = "test-dryrun";

    let objects = write_objects(&engine, bucket, 10, "dry-").await;
    engine.sync().await.unwrap();

    // Delete half
    for (key, _) in objects.iter().take(5) {
        engine.delete_object(bucket, key).await.unwrap();
    }

    let config = CompactionConfig {
        fragmentation_threshold: 0.1,
        min_dead_bytes: 0,
        dry_run: true, // DRY RUN
        ..Default::default()
    };

    let compactor = VolumeCompactor::new(
        engine.volumes_dir().to_path_buf(),
        engine.index_db().clone(),
        engine.deduplicator().clone(),
        engine.volume_writer().clone(),
        config,
    );

    let stats = compactor.run().await.unwrap();

    // Dry run should not compact anything
    assert_eq!(stats.volumes_compacted, 0);

    // All surviving objects must still be readable
    for (key, expected_data) in objects.iter().skip(5) {
        let (data, _) = engine.get_object(bucket, key).await.unwrap();
        assert_eq!(&data, expected_data);
    }
}

#[tokio::test]
async fn test_compaction_skips_below_threshold() {
    let temp = TempDir::new().unwrap();
    let engine = create_test_engine(&temp).await;
    let bucket = "test-skip";

    // Write objects but don't delete any — 0% fragmentation
    write_objects(&engine, bucket, 5, "skip-").await;
    engine.sync().await.unwrap();

    let config = CompactionConfig {
        fragmentation_threshold: 0.3, // 30% threshold
        min_dead_bytes: 0,
        dry_run: false,
        ..Default::default()
    };

    let compactor = VolumeCompactor::new(
        engine.volumes_dir().to_path_buf(),
        engine.index_db().clone(),
        engine.deduplicator().clone(),
        engine.volume_writer().clone(),
        config,
    );

    let stats = compactor.run().await.unwrap();

    // Should skip all volumes (no dead space)
    assert_eq!(stats.volumes_compacted, 0);
}

#[tokio::test]
async fn test_compaction_preserves_iam_records() {
    let temp = TempDir::new().unwrap();
    let engine = create_test_engine(&temp).await;
    let bucket = "test-iam-compact";

    // 1. Write regular objects to fill volumes
    let objects = write_objects(&engine, bucket, 20, "obj-").await;
    engine.sync().await.unwrap();

    // 2. Write IAM data into the __system__ bucket (same as IamStorage does).
    //    These blobs land in the same partially-filled volume as the tail of
    //    the regular objects above.
    let iam_user_json = br#"{"id":"user1","username":"alice","role":"Writer"}"#;
    let iam_key_json = br#"{"access_key":"AK123","user_id":"user1"}"#;
    engine
        .put_object(
            "__system__",
            "__s4_iam_user_user1",
            iam_user_json,
            "application/json",
            &HashMap::new(),
        )
        .await
        .unwrap();
    engine
        .put_object(
            "__system__",
            "__s4_iam_access_key_AK123",
            iam_key_json,
            "application/json",
            &HashMap::new(),
        )
        .await
        .unwrap();
    engine.sync().await.unwrap();

    // 3. Verify IAM data is readable before compaction
    let (data, _) = engine.get_object("__system__", "__s4_iam_user_user1").await.unwrap();
    assert_eq!(&data, iam_user_json);

    let (data, _) = engine.get_object("__system__", "__s4_iam_access_key_AK123").await.unwrap();
    assert_eq!(&data, iam_key_json);

    // 4. Delete most regular objects to create >50% dead space in volumes
    //    that also contain IAM blobs
    for (key, _) in objects.iter().take(18) {
        engine.delete_object(bucket, key).await.unwrap();
    }
    engine.sync().await.unwrap();

    // 5. Run compaction
    let config = CompactionConfig {
        fragmentation_threshold: 0.1,
        min_dead_bytes: 0,
        max_volumes_per_run: 100,
        dry_run: false,
        ..Default::default()
    };

    let compactor = VolumeCompactor::new(
        engine.volumes_dir().to_path_buf(),
        engine.index_db().clone(),
        engine.deduplicator().clone(),
        engine.volume_writer().clone(),
        config,
    );

    let stats = compactor.run().await.unwrap();
    assert!(
        stats.volumes_compacted > 0,
        "Expected compacted volumes, stats: {:?}",
        stats
    );

    // 6. CRITICAL: Verify IAM data is still readable after compaction.
    //    Before the fix, this would fail with "Volume not found" because
    //    scan_objects_by_volume() only scanned the objects keyspace and
    //    missed IAM IndexRecords in the iam keyspace.
    let (data, _) = engine
        .get_object("__system__", "__s4_iam_user_user1")
        .await
        .expect("IAM user record must survive compaction");
    assert_eq!(
        &data, iam_user_json,
        "IAM user data corrupted after compaction"
    );

    let (data, _) = engine
        .get_object("__system__", "__s4_iam_access_key_AK123")
        .await
        .expect("IAM access key record must survive compaction");
    assert_eq!(
        &data, iam_key_json,
        "IAM access key data corrupted after compaction"
    );

    // 7. Surviving regular objects should also still be readable
    for (key, expected_data) in objects.iter().skip(18) {
        let (data, _) = engine.get_object(bucket, key).await.unwrap();
        assert_eq!(&data, expected_data);
    }
}

#[tokio::test]
async fn test_scrubber_detects_no_corruption() {
    let temp = TempDir::new().unwrap();
    let engine = create_test_engine(&temp).await;
    let bucket = "test-scrub";

    write_objects(&engine, bucket, 5, "scrub-").await;
    engine.sync().await.unwrap();

    let scrubber = s4_compactor::VolumeScrubber::new(engine.volumes_dir());
    let result = scrubber.scrub_all().await.unwrap();

    assert!(result.blobs_checked > 0);
    assert_eq!(result.blobs_corrupted, 0);
    assert_eq!(result.blobs_ok, result.blobs_checked);
}

/// Regression test: after deleting ALL objects in ALL buckets, the compactor
/// must reclaim ALL volume space. Previously, the compactor would skip 100%-dead
/// volumes because both dedup_index and blob_ref_index were empty, causing
/// `analyze_volume` to return 0 fragmentation (read_blob failure on orphaned
/// data) or because `min_dead_bytes` / `max_volumes_per_run` blocked progress.
#[tokio::test]
async fn test_compaction_purges_all_dead_volumes_after_full_delete() {
    let temp = TempDir::new().unwrap();
    let engine = create_test_engine(&temp).await;
    let bucket = "test-purge";

    // 1. Write enough objects to create multiple volumes
    let objects = write_objects(&engine, bucket, 30, "purge-").await;
    engine.sync().await.unwrap();

    let volumes_dir = temp.path().join("volumes");
    let count_volumes = || -> usize {
        std::fs::read_dir(&volumes_dir)
            .unwrap()
            .filter_map(|e| {
                let name = e.ok()?.file_name().to_string_lossy().to_string();
                if name.starts_with("volume_") && name.ends_with(".dat") {
                    Some(())
                } else {
                    None
                }
            })
            .count()
    };

    let volumes_after_write = count_volumes();
    assert!(
        volumes_after_write >= 2,
        "Expected at least 2 volumes, got {}",
        volumes_after_write
    );

    // 2. Delete ALL objects — this should clean up all dedup entries
    for (key, _) in &objects {
        engine.delete_object(bucket, key).await.unwrap();
    }
    engine.sync().await.unwrap();

    // 3. Run compaction with on_demand config (no limits)
    let config = CompactionConfig::on_demand(0.0, false, 24 * 3600);

    let compactor = VolumeCompactor::new(
        engine.volumes_dir().to_path_buf(),
        engine.index_db().clone(),
        engine.deduplicator().clone(),
        engine.volume_writer().clone(),
        config,
    );

    let stats = compactor.run().await.unwrap();

    // Should have purged (compacted) the dead volumes
    assert!(
        stats.volumes_compacted > 0,
        "Expected dead volumes to be purged, stats: {:?}",
        stats
    );
    assert!(
        stats.total_bytes_reclaimed > 0,
        "Expected bytes reclaimed from dead volumes"
    );
    assert_eq!(stats.errors, 0, "Expected 0 errors during purge");

    // Only the active volume should remain
    let volumes_after_compact = count_volumes();
    assert!(
        volumes_after_compact < volumes_after_write,
        "Volumes should have decreased: before={}, after={}",
        volumes_after_write,
        volumes_after_compact
    );
}

/// Regression test: orphaned dedup entries (ref_count > 0 but no corresponding
/// object in the index) must be purged by the compactor so that dead blobs are
/// correctly identified and volumes can be reclaimed.
///
/// Reproduces the real-world scenario where objects are deleted but dedup entries
/// survive, causing the compactor to treat all blobs as live indefinitely.
#[tokio::test]
async fn test_compaction_purges_orphaned_dedup_entries() {
    let temp = TempDir::new().unwrap();
    let engine = create_test_engine(&temp).await;
    let bucket = "test-orphan";

    // 1. Write objects to create dedup entries
    let objects = write_objects(&engine, bucket, 20, "orphan-").await;
    engine.sync().await.unwrap();

    // Verify dedup entries exist
    let dedup_before = engine.deduplicator().iter_entries().unwrap();
    assert!(
        !dedup_before.is_empty(),
        "Expected dedup entries after writes"
    );

    // 2. Delete all objects through the engine (proper ref_count decrement)
    for (key, _) in &objects {
        engine.delete_object(bucket, key).await.unwrap();
    }
    engine.sync().await.unwrap();

    // After proper deletion, dedup entries SHOULD be gone. But let's
    // simulate the bug: manually re-insert orphaned dedup entries.
    // This replicates what happens when ref_count cleanup fails.
    for (hash, entry) in &dedup_before {
        engine
            .deduplicator()
            .register_content(*hash, entry.volume_id, entry.offset)
            .unwrap();
    }

    // Verify orphaned entries are back
    let dedup_orphaned = engine.deduplicator().iter_entries().unwrap();
    assert!(
        !dedup_orphaned.is_empty(),
        "Expected orphaned dedup entries after re-insertion"
    );

    let volumes_before = count_volume_files(&temp);
    assert!(volumes_before > 1, "Expected multiple volumes");

    // 3. Run compaction — should purge orphaned dedup entries first,
    //    then be able to delete or compact the dead volumes.
    let config = CompactionConfig::on_demand(0.0, false, 24 * 3600);

    let compactor = VolumeCompactor::new(
        engine.volumes_dir().to_path_buf(),
        engine.index_db().clone(),
        engine.deduplicator().clone(),
        engine.volume_writer().clone(),
        config,
    );

    let stats = compactor.run().await.unwrap();

    // Orphaned dedup entries should now be purged
    let dedup_after = engine.deduplicator().iter_entries().unwrap();
    assert!(
        dedup_after.is_empty(),
        "Expected all orphaned dedup entries to be purged, got {}",
        dedup_after.len()
    );

    // Dead volumes should be reclaimed (fast-path or normal compaction)
    assert!(
        stats.volumes_compacted > 0,
        "Expected dead volumes to be reclaimed after orphan purge, stats: {:?}",
        stats
    );
    assert_eq!(stats.errors, 0);

    // Only the active volume should remain
    let volumes_after = count_volume_files(&temp);
    assert!(
        volumes_after < volumes_before,
        "Volumes should decrease: before={}, after={}",
        volumes_before,
        volumes_after
    );
}

/// Verifies that the compactor expires old multipart sessions and purges
/// the associated parts and blob_ref entries that were blocking volume
/// reclamation.
#[tokio::test]
async fn test_compaction_purges_expired_multipart_sessions() {
    let temp = TempDir::new().unwrap();
    let engine = create_test_engine(&temp).await;

    // Write some data so volumes get created, then delete all objects.
    let objs = write_objects(&engine, "bucket", 5, "obj").await;
    for (key, _) in &objs {
        engine.delete_object("bucket", key).await.unwrap();
    }

    // Inject an expired multipart session (created_at = 48 hours ago).
    let sessions_ks = engine.index_db().multipart_sessions_keyspace();
    let parts_ks = engine.index_db().multipart_parts_keyspace();
    let blob_refs_ks = engine.index_db().blob_refs_keyspace();

    let now_nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    let expired_created_at = now_nanos - 48 * 3600 * 1_000_000_000; // 48 hours ago

    let session = s4_core::types::MultipartUploadSession {
        upload_id: "expired-upload-123".to_string(),
        bucket: "bucket".to_string(),
        key: "bigfile.bin".to_string(),
        content_type: "application/octet-stream".to_string(),
        metadata: HashMap::new(),
        state: s4_core::types::MultipartUploadState::Open,
        created_at: expired_created_at,
        updated_at: expired_created_at,
    };
    let session_bytes = bincode::serialize(&session).unwrap();
    sessions_ks.insert("expired-upload-123", &session_bytes[..]).unwrap();

    // Inject 3 multipart part records for this expired session.
    let mut blob_ids = Vec::new();
    for i in 1..=3u32 {
        let blob_id = s4_core::types::BlobId([i as u8; 32]);
        blob_ids.push(blob_id);

        let part = s4_core::types::MultipartPartRecord {
            upload_id: "expired-upload-123".to_string(),
            part_number: i,
            size: 1024,
            etag_md5_hex: "d41d8cd98f00b204e9800998ecf8427e".to_string(),
            etag_md5_bytes: [0u8; 16],
            physical_hash: [i as u8; 32],
            blob_id,
            crc32: 0,
            created_at: expired_created_at,
        };
        let part_bytes = bincode::serialize(&part).unwrap();
        let part_key = format!("expired-upload-123_{:05}", i);
        parts_ks.insert(part_key, &part_bytes[..]).unwrap();

        // Inject corresponding blob_ref entry.
        let blob_ref = s4_core::types::BlobRefEntry {
            location: s4_core::types::BlobLocation {
                volume_id: 1,
                offset: (i as u64) * 100,
            },
            ref_count_committed: 0,
            ref_count_staged: 1,
        };
        let blob_ref_bytes = bincode::serialize(&blob_ref).unwrap();
        blob_refs_ks.insert(blob_id.as_bytes(), &blob_ref_bytes[..]).unwrap();
    }

    // Verify they exist.
    assert!(sessions_ks.get("expired-upload-123").unwrap().is_some());
    assert_eq!(parts_ks.iter().count(), 3);
    assert_eq!(blob_refs_ks.iter().count(), 3);

    // Run compactor with 24h TTL (session is 48h old, so it should expire).
    let config = CompactionConfig {
        fragmentation_threshold: 0.0,
        min_dead_bytes: 0,
        max_volumes_per_run: 0,
        multipart_session_ttl_secs: 24 * 3600,
        ..Default::default()
    };

    let compactor = VolumeCompactor::new(
        engine.volumes_dir().to_path_buf(),
        engine.index_db().clone(),
        engine.deduplicator().clone(),
        engine.volume_writer().clone(),
        config,
    );

    let _stats = compactor.run().await.unwrap();

    // The expired session, its parts, and blob_refs should all be purged.
    assert!(
        sessions_ks.get("expired-upload-123").unwrap().is_none(),
        "expired session should be purged"
    );
    assert_eq!(
        parts_ks.iter().count(),
        0,
        "orphaned multipart parts should be purged"
    );
    assert_eq!(
        blob_refs_ks.iter().count(),
        0,
        "orphaned blob_refs should be purged"
    );
}

/// Verifies that a session with old `created_at` but recent `updated_at`
/// (e.g., CompleteMultipartUpload in progress) is NOT expired by the compactor.
#[tokio::test]
async fn test_compaction_preserves_recently_active_session() {
    let temp = TempDir::new().unwrap();
    let engine = create_test_engine(&temp).await;

    // Write + delete to create volumes.
    let objs = write_objects(&engine, "bucket", 3, "obj").await;
    for (key, _) in &objs {
        engine.delete_object("bucket", key).await.unwrap();
    }

    let sessions_ks = engine.index_db().multipart_sessions_keyspace();
    let parts_ks = engine.index_db().multipart_parts_keyspace();
    let blob_refs_ks = engine.index_db().blob_refs_keyspace();

    let now_nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    let old_created_at = now_nanos - 48 * 3600 * 1_000_000_000; // 48h ago
    let recent_updated_at = now_nanos - 60 * 1_000_000_000; // 1 minute ago

    // Session created 48h ago but updated 1 min ago (Completing state).
    let session = s4_core::types::MultipartUploadSession {
        upload_id: "active-completing".to_string(),
        bucket: "bucket".to_string(),
        key: "bigfile.bin".to_string(),
        content_type: "application/octet-stream".to_string(),
        metadata: HashMap::new(),
        state: s4_core::types::MultipartUploadState::Completing,
        created_at: old_created_at,
        updated_at: recent_updated_at,
    };
    let session_bytes = bincode::serialize(&session).unwrap();
    sessions_ks.insert("active-completing", &session_bytes[..]).unwrap();

    // One part + blob_ref for this session.
    let blob_id = s4_core::types::BlobId([42u8; 32]);
    let part = s4_core::types::MultipartPartRecord {
        upload_id: "active-completing".to_string(),
        part_number: 1,
        size: 1024,
        etag_md5_hex: "aabb".to_string(),
        etag_md5_bytes: [0u8; 16],
        physical_hash: [42u8; 32],
        blob_id,
        crc32: 0,
        created_at: old_created_at,
    };
    let part_bytes = bincode::serialize(&part).unwrap();
    parts_ks.insert("active-completing_00001", &part_bytes[..]).unwrap();

    let blob_ref = s4_core::types::BlobRefEntry {
        location: s4_core::types::BlobLocation {
            volume_id: 1,
            offset: 100,
        },
        ref_count_committed: 0,
        ref_count_staged: 1,
    };
    let blob_ref_bytes = bincode::serialize(&blob_ref).unwrap();
    blob_refs_ks.insert(blob_id.as_bytes(), &blob_ref_bytes[..]).unwrap();

    // Run compactor — session should be preserved (updated_at is recent).
    let config = CompactionConfig {
        fragmentation_threshold: 0.0,
        min_dead_bytes: 0,
        max_volumes_per_run: 0,
        multipart_session_ttl_secs: 24 * 3600,
        ..Default::default()
    };

    let compactor = VolumeCompactor::new(
        engine.volumes_dir().to_path_buf(),
        engine.index_db().clone(),
        engine.deduplicator().clone(),
        engine.volume_writer().clone(),
        config,
    );

    let _stats = compactor.run().await.unwrap();

    // Session, part, and blob_ref must all survive.
    assert!(
        sessions_ks.get("active-completing").unwrap().is_some(),
        "recently active session must NOT be purged"
    );
    assert_eq!(
        parts_ks.iter().count(),
        1,
        "part of active session must survive"
    );
    assert_eq!(
        blob_refs_ks.iter().count(),
        1,
        "blob_ref of active session must survive"
    );
}

/// Regression test: partial compaction (max_volumes_per_run hit) must NOT clear
/// the metadata journal. Clearing journal prematurely causes ghost objects on
/// server restart — volume-only recovery resurrects deleted objects from
/// uncompacted volumes when the objects keyspace is empty and the journal
/// cannot provide the delete events.
///
/// Bug scenario (before fix):
///   1. Upload objects → journal has ObjectPut entries
///   2. Delete all objects → journal has ObjectDelete entries
///   3. Background compactor runs with max_volumes_per_run=10
///      → compacts 10 volumes, clears ENTIRE journal
///   4. Server restarts → objects keyspace empty (fjall compacted tombstones),
///      journal empty → falls back to volume-only recovery → ghost objects!
///
/// Fix: journal is only cleared when ALL volumes have been processed
/// (no max_volumes_per_run limit hit).
#[tokio::test]
async fn test_partial_compaction_preserves_journal() {
    let temp = TempDir::new().unwrap();
    let engine = create_test_engine(&temp).await;
    let bucket = "test-ghost";

    // 1. Write enough objects to create multiple volumes (4KB each, ~200B per object)
    //    Need at least 4 volumes: 2 will be compacted (limited run), 1+ remain, 1 active.
    let objects = write_objects(&engine, bucket, 60, "ghost-").await;
    engine.sync().await.unwrap();

    let volumes_after_write = count_volume_files(&temp);
    assert!(
        volumes_after_write >= 4,
        "Need at least 4 volumes to test partial compaction, got {}",
        volumes_after_write
    );

    // Verify journal has entries (ObjectPut events)
    assert!(
        !engine.journal().is_empty().unwrap(),
        "Journal should have entries after writing objects"
    );

    // 2. Delete most objects, keep one alive to prevent the purge_all_dead_volumes
    //    fast-path (which triggers when dedup index is empty). We need the normal
    //    compaction path to test max_volumes_per_run behavior.
    let survivor_key = &objects[0].0;
    for (key, _) in &objects[1..] {
        engine.delete_object(bucket, key).await.unwrap();
    }
    engine.sync().await.unwrap();

    // Journal should still have entries (both Put and Delete events)
    assert!(
        !engine.journal().is_empty().unwrap(),
        "Journal should have entries after deleting objects"
    );

    // 3. Run compaction with max_volumes_per_run=2 (partial — simulates background compactor)
    let config = CompactionConfig {
        fragmentation_threshold: 0.0,
        min_dead_bytes: 0,
        max_volumes_per_run: 2,
        compact_journal: true,
        max_volume_size: 1024 * 1024 * 1024,
        multipart_session_ttl_secs: 24 * 3600,
        dry_run: false,
    };

    let compactor = VolumeCompactor::new(
        engine.volumes_dir().to_path_buf(),
        engine.index_db().clone(),
        engine.deduplicator().clone(),
        engine.volume_writer().clone(),
        config,
    );

    let stats = compactor.run().await.unwrap();

    // Should have compacted exactly 2 volumes (hit the limit)
    assert_eq!(
        stats.volumes_compacted, 2,
        "Expected exactly 2 volumes compacted (limited run), got {}",
        stats.volumes_compacted
    );

    // KEY ASSERTION: journal must NOT be cleared after partial compaction!
    // If journal is empty here, a server restart could trigger volume-only
    // recovery and resurrect deleted objects from uncompacted volumes.
    assert!(
        !engine.journal().is_empty().unwrap(),
        "BUG: journal was cleared after partial compaction! \
         This causes ghost objects on server restart."
    );

    // Verify the survivor is still readable
    let (data, _record) = engine.get_object(bucket, survivor_key).await.unwrap();
    assert_eq!(
        data, objects[0].1,
        "Surviving object data should be intact after partial compaction"
    );

    // 4. Delete the survivor and run unlimited compaction
    engine.delete_object(bucket, survivor_key).await.unwrap();
    engine.sync().await.unwrap();

    let config_unlimited = CompactionConfig::on_demand(0.0, false, 24 * 3600);

    let compactor2 = VolumeCompactor::new(
        engine.volumes_dir().to_path_buf(),
        engine.index_db().clone(),
        engine.deduplicator().clone(),
        engine.volume_writer().clone(),
        config_unlimited,
    );

    let stats2 = compactor2.run().await.unwrap();
    assert_eq!(stats2.errors, 0);

    // After unlimited compaction, journal SHOULD be cleared
    assert!(
        engine.journal().is_empty().unwrap(),
        "Journal should be cleared after full (unlimited) compaction"
    );
}

/// Regression test: the purge_all_dead_volumes fast-path (when both dedup and
/// blob_ref indices are empty) must also clear the journal and flush WAL.
/// Before the fix, this path returned early and skipped Step 5 (journal) and
/// Step 6 (WAL flush).
#[tokio::test]
async fn test_purge_fast_path_clears_journal() {
    let temp = TempDir::new().unwrap();
    let engine = create_test_engine(&temp).await;
    let bucket = "test-purge-journal";

    // 1. Write objects to create volumes
    let objects = write_objects(&engine, bucket, 20, "purge-j-").await;
    engine.sync().await.unwrap();

    // 2. Delete ALL objects (dedup entries will be removed)
    for (key, _) in &objects {
        engine.delete_object(bucket, key).await.unwrap();
    }
    engine.sync().await.unwrap();

    // Journal should have entries
    assert!(
        !engine.journal().is_empty().unwrap(),
        "Journal should have entries before compaction"
    );

    // 3. Run unlimited compaction — should take the purge_all_dead_volumes fast-path
    //    (dedup index is empty after all objects deleted)
    let config = CompactionConfig::on_demand(0.0, false, 24 * 3600);
    let compactor = VolumeCompactor::new(
        engine.volumes_dir().to_path_buf(),
        engine.index_db().clone(),
        engine.deduplicator().clone(),
        engine.volume_writer().clone(),
        config,
    );

    let stats = compactor.run().await.unwrap();
    assert_eq!(stats.errors, 0);
    assert!(stats.volumes_compacted > 0);

    // Journal MUST be cleared by the fast-path (before fix, it wasn't)
    assert!(
        engine.journal().is_empty().unwrap(),
        "BUG: purge_all_dead_volumes fast-path did not clear journal!"
    );
}

fn count_volume_files(temp: &TempDir) -> usize {
    std::fs::read_dir(temp.path().join("volumes"))
        .unwrap()
        .filter_map(|e| {
            let name = e.ok()?.file_name().to_string_lossy().to_string();
            if name.starts_with("volume_") && name.ends_with(".dat") {
                Some(())
            } else {
                None
            }
        })
        .count()
}
