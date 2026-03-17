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
