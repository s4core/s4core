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

//! End-to-end compaction tests.
//!
//! Full lifecycle: write objects across multiple volumes, delete a portion,
//! run compaction, verify surviving objects, then simulate crash recovery
//! by reopening the engine and verifying data integrity.

use s4_compactor::{CompactionConfig, VolumeCompactor};
use s4_core::storage::BitcaskStorageEngine;
use s4_core::StorageEngine;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use tempfile::TempDir;

/// Helper: create engine with small volumes.
async fn create_engine(temp: &TempDir) -> BitcaskStorageEngine {
    let data_path = temp.path().join("volumes");
    let metadata_path = temp.path().join("metadata_db");
    std::fs::create_dir_all(&data_path).unwrap();

    BitcaskStorageEngine::new(data_path, metadata_path, 4096, 0, false)
        .await
        .unwrap()
}

/// Helper: reopen engine (simulates crash recovery).
async fn reopen_engine(temp: &TempDir) -> BitcaskStorageEngine {
    let data_path = temp.path().join("volumes");
    let metadata_path = temp.path().join("metadata_db");

    BitcaskStorageEngine::new(data_path, metadata_path, 4096, 0, false)
        .await
        .unwrap()
}

fn sha256(data: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hasher.finalize().into()
}

/// Full E2E test:
/// 1. Write 30 objects across multiple volumes
/// 2. Delete 20 of them
/// 3. Run compaction
/// 4. Verify remaining 10 objects return correct SHA-256 hashes
/// 5. Reopen engine (crash recovery) and verify again
#[tokio::test]
async fn test_compaction_e2e_with_recovery() {
    let temp = TempDir::new().unwrap();
    let bucket = "e2e-compact";
    let metadata = HashMap::new();

    // Collect objects for verification across engine lifetimes
    let mut all_objects: Vec<(String, Vec<u8>, [u8; 32])> = Vec::new();

    // Phase 1-4: Write, delete, compact, verify — all in one engine lifetime
    {
        let engine = create_engine(&temp).await;

        // Step 1: Write 30 objects with unique content
        for i in 0..30 {
            let key = format!("object-{:04}", i);
            let data = format!(
                "e2e-data-{:04}-{}-end",
                i,
                "payload".repeat(20 + i) // varying sizes
            )
            .into_bytes();
            let hash = sha256(&data);

            engine
                .put_object(bucket, &key, &data, "application/octet-stream", &metadata)
                .await
                .unwrap();
            all_objects.push((key, data, hash));
        }
        engine.sync().await.unwrap();

        // Verify all written
        for (key, expected_data, _) in &all_objects {
            let (data, _) = engine.get_object(bucket, key).await.unwrap();
            assert_eq!(
                &data, expected_data,
                "Pre-compaction read failed for {}",
                key
            );
        }

        // Step 2: Delete first 20 objects
        for (key, _, _) in all_objects.iter().take(20) {
            engine.delete_object(bucket, key).await.unwrap();
        }
        engine.sync().await.unwrap();

        // Step 3: Run compaction
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
        assert!(stats.volumes_compacted > 0, "Expected compaction to run");
        assert!(stats.total_bytes_reclaimed > 0, "Expected bytes reclaimed");
        assert_eq!(stats.errors, 0, "Expected no errors");

        // Step 4: Verify surviving objects with SHA-256
        for (key, expected_data, expected_hash) in all_objects.iter().skip(20) {
            let (data, _) = engine.get_object(bucket, key).await.unwrap();
            let actual_hash = sha256(&data);
            assert_eq!(
                &actual_hash, expected_hash,
                "SHA-256 mismatch after compaction for {}",
                key
            );
            assert_eq!(
                &data, expected_data,
                "Data mismatch after compaction for {}",
                key
            );
        }
    } // engine dropped here, fjall lock released

    // Step 5: Reopen engine (crash recovery simulation)
    let engine2 = reopen_engine(&temp).await;

    // Verify all surviving objects after recovery
    for (key, expected_data, expected_hash) in all_objects.iter().skip(20) {
        let (data, _) = engine2
            .get_object(bucket, key)
            .await
            .unwrap_or_else(|e| panic!("Failed to read {} after recovery: {}", key, e));
        let actual_hash = sha256(&data);
        assert_eq!(
            &actual_hash, expected_hash,
            "SHA-256 mismatch after recovery for {}",
            key
        );
        assert_eq!(
            &data, expected_data,
            "Data mismatch after recovery for {}",
            key
        );
    }

    // Deleted objects should still be gone
    for (key, _, _) in all_objects.iter().take(20) {
        assert!(
            engine2.get_object(bucket, key).await.is_err(),
            "Deleted object {} should not exist after recovery",
            key
        );
    }
}

/// Test that compaction works with deduplicated content.
/// Multiple objects reference the same blob — compaction must preserve
/// the dedup entry and all objects remain readable.
#[tokio::test]
async fn test_compaction_with_dedup() {
    let temp = TempDir::new().unwrap();
    let engine = create_engine(&temp).await;
    let bucket = "e2e-dedup";
    let metadata = HashMap::new();

    // Write 5 objects with identical content (dedup should store blob once)
    let shared_data = b"shared-content-for-dedup-testing-padding".to_vec();
    for i in 0..5 {
        let key = format!("dedup-{:04}", i);
        engine
            .put_object(bucket, &key, &shared_data, "text/plain", &metadata)
            .await
            .unwrap();
    }

    // Write 5 objects with unique content, then delete them
    for i in 0..5 {
        let key = format!("dead-{:04}", i);
        let data = format!("dead-content-{:04}-{}", i, "x".repeat(100)).into_bytes();
        engine.put_object(bucket, &key, &data, "text/plain", &metadata).await.unwrap();
    }
    engine.sync().await.unwrap();

    // Delete the unique objects to create dead space
    for i in 0..5 {
        let key = format!("dead-{:04}", i);
        engine.delete_object(bucket, &key).await.unwrap();
    }
    engine.sync().await.unwrap();

    // Run compaction
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

    let _stats = compactor.run().await.unwrap();

    // All deduplicated objects should still be readable
    for i in 0..5 {
        let key = format!("dedup-{:04}", i);
        let (data, _) = engine.get_object(bucket, &key).await.unwrap();
        assert_eq!(
            data, shared_data,
            "Dedup object {} corrupted after compaction",
            key
        );
    }
}

/// Test that composite (multipart-uploaded) objects survive compaction.
///
/// This verifies that:
/// 1. Manifest blobs (only in BlobRefs, not in Dedup) are preserved
/// 2. BlobRefEntry locations are updated when blobs move to new volumes
/// 3. Composite objects can be read (GET) and deleted (DELETE) after compaction
#[tokio::test]
async fn test_compaction_with_composite_objects() {
    let temp = TempDir::new().unwrap();
    let engine = create_engine(&temp).await;
    let bucket = "e2e-composite";
    let metadata = HashMap::new();

    // Create a multipart upload with 3 parts
    let upload_id = "test-composite-upload-001";
    engine
        .create_multipart_session(
            upload_id,
            bucket,
            "composite.bin",
            "application/octet-stream",
            &metadata,
        )
        .await
        .unwrap();

    // Upload 3 parts with unique content
    let mut part_records = Vec::new();
    let mut all_part_data = Vec::new();
    for part_num in 1..=3u32 {
        let part_data = format!(
            "composite-part-{}-data-{}",
            part_num,
            "x".repeat(200 + part_num as usize * 50)
        )
        .into_bytes();
        all_part_data.push(part_data.clone());

        let reader: Box<dyn tokio::io::AsyncRead + Unpin + Send> =
            Box::new(std::io::Cursor::new(part_data.clone()));
        let result = engine
            .upload_part_streaming(upload_id, part_num, reader, part_data.len() as u64)
            .await
            .unwrap();
        part_records.push(result.record);
    }

    // Complete the multipart upload
    let s3_etag = "\"abc123-3\"";
    let _record = engine
        .complete_multipart_native(
            bucket,
            "composite.bin",
            upload_id,
            &part_records,
            s3_etag,
            "application/octet-stream",
            &metadata,
        )
        .await
        .unwrap();
    engine.sync().await.unwrap();

    // Verify composite object is readable
    let (data_before, _) = engine.get_object(bucket, "composite.bin").await.unwrap();
    let expected_data: Vec<u8> = all_part_data.iter().flat_map(|d| d.iter().copied()).collect();
    assert_eq!(
        data_before, expected_data,
        "Pre-compaction composite read failed"
    );
    let expected_hash = sha256(&expected_data);

    // Write filler objects to create dead space in the same volumes
    for i in 0..20 {
        let key = format!("filler-{:04}", i);
        let data = format!("filler-unique-content-{:04}-{}", i, "y".repeat(200)).into_bytes();
        engine.put_object(bucket, &key, &data, "text/plain", &metadata).await.unwrap();
    }
    engine.sync().await.unwrap();

    // Delete all fillers to create dead space
    for i in 0..20 {
        let key = format!("filler-{:04}", i);
        engine.delete_object(bucket, &key).await.unwrap();
    }
    engine.sync().await.unwrap();

    // Run compaction with aggressive threshold
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
    assert_eq!(stats.errors, 0, "Compaction should have 0 errors");

    // CRITICAL: Verify composite object is still readable after compaction (GET)
    let (data_after, _) = engine
        .get_object(bucket, "composite.bin")
        .await
        .expect("Composite object should be readable after compaction");
    let actual_hash = sha256(&data_after);
    assert_eq!(
        actual_hash, expected_hash,
        "Composite object SHA-256 mismatch after compaction"
    );
    assert_eq!(
        data_after, expected_data,
        "Composite object data mismatch after compaction"
    );

    // CRITICAL: Verify composite object can be deleted after compaction (DELETE)
    engine
        .delete_object(bucket, "composite.bin")
        .await
        .expect("Composite object should be deletable after compaction");

    // Verify it's gone
    assert!(
        engine.get_object(bucket, "composite.bin").await.is_err(),
        "Deleted composite object should not exist"
    );
}

/// Regression test: deleted composite objects must have their segment blobs
/// reclaimed by compaction, not copied to new volumes.
///
/// Before the fix, `composite_delete_ref_ops` decremented BlobRefEntry
/// ref counts but did NOT unregister segments from the dedup keyspace.
/// This left orphaned DedupEntries with ref_count >= 1, causing the
/// compactor to treat dead segment blobs as live and copy them forever.
#[tokio::test]
async fn test_compaction_reclaims_deleted_composite_segments() {
    let temp = TempDir::new().unwrap();
    let engine = create_engine(&temp).await;
    let bucket = "reclaim-composite";
    let metadata = HashMap::new();

    // Step 1: Create and complete a multipart upload
    let upload_id = "reclaim-test-001";
    engine
        .create_multipart_session(
            upload_id,
            bucket,
            "big-file.bin",
            "application/octet-stream",
            &metadata,
        )
        .await
        .unwrap();

    let mut part_records = Vec::new();
    for part_num in 1..=3u32 {
        let part_data = format!(
            "segment-data-part-{}-{}",
            part_num,
            "Z".repeat(300 + part_num as usize * 100)
        )
        .into_bytes();
        let reader: Box<dyn tokio::io::AsyncRead + Unpin + Send> =
            Box::new(std::io::Cursor::new(part_data.clone()));
        let result = engine
            .upload_part_streaming(upload_id, part_num, reader, part_data.len() as u64)
            .await
            .unwrap();
        part_records.push(result.record);
    }

    engine
        .complete_multipart_native(
            bucket,
            "big-file.bin",
            upload_id,
            &part_records,
            "\"etag-3\"",
            "application/octet-stream",
            &metadata,
        )
        .await
        .unwrap();
    engine.sync().await.unwrap();

    // Step 2: Delete the composite object
    engine.delete_object(bucket, "big-file.bin").await.unwrap();
    engine.sync().await.unwrap();

    // Step 3: Verify dedup entries are cleaned up (the root cause check)
    let dedup = engine.deduplicator().clone();
    let dedup_entries = dedup.iter_entries().unwrap();
    assert!(
        dedup_entries.is_empty(),
        "Dedup entries should be empty after deleting all objects, \
         but found {} orphaned entries",
        dedup_entries.len()
    );

    // Step 4: Run compaction
    let config = CompactionConfig {
        fragmentation_threshold: 0.0, // compact everything
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
    assert_eq!(stats.errors, 0, "Compaction should have 0 errors");
    // The key assertion: no orphaned dedup entries should cause live blob copies.
    // If the bug is present, the compactor would copy dead segment blobs thinking
    // they're live (because orphaned DedupEntries would still exist).
    assert_eq!(
        stats.total_live_blobs, 0,
        "No live blobs should exist — all data was deleted. \
         If live_blobs > 0, orphaned dedup/blob_ref entries are keeping dead data alive."
    );
}
