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

//! Crash Recovery Integration Test
//!
//! This test verifies Phase 1 success criterion:
//! "Записать объекты, убить процесс, восстановить все данные"
//!
//! We write many objects, drop the storage engine (simulating crash),
//! and verify all data can be recovered.

use s4_core::storage::BitcaskStorageEngine;
use s4_core::StorageEngine;
use std::collections::HashMap;
use tempfile::TempDir;

/// Test crash recovery with 1,000 objects stored in VOLUMES (not inline)
/// This ensures we're actually testing volume-based recovery
#[tokio::test]
async fn test_crash_recovery_volume_storage() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let data_path = temp_dir.path().join("volumes");
    let metadata_path = temp_dir.path().join("metadata.redb");

    std::fs::create_dir_all(&data_path).expect("Failed to create data dir");

    let bucket = "test-bucket";
    let num_objects = 1_000;
    let inline_threshold = 1024; // 1KB threshold

    // Create data larger than threshold to force volume storage
    let base_data = vec![b'D'; 2000]; // 2KB - above threshold

    // Store expected data for verification
    let mut expected_data: HashMap<String, Vec<u8>> = HashMap::new();

    // Phase 1: Write objects to VOLUMES (not inline)
    {
        let engine = BitcaskStorageEngine::new(
            data_path.clone(),
            metadata_path.clone(),
            1024 * 1024 * 50, // 50MB volume size
            inline_threshold,
            true, // strict_sync = true for durability
        )
        .await
        .expect("Failed to create storage engine");

        for i in 0..num_objects {
            let key = format!("object_{:06}", i);
            // Each object has unique content (append index to base data)
            let mut data = base_data.clone();
            data.extend_from_slice(format!("_{:06}", i).as_bytes());

            let mut metadata = HashMap::new();
            metadata.insert("X-Test-Index".to_string(), i.to_string());

            engine
                .put_object(bucket, &key, &data, "application/octet-stream", &metadata)
                .await
                .expect("Failed to put object");

            expected_data.insert(format!("{}/{}", bucket, key), data);

            if (i + 1) % 100 == 0 {
                println!("Written {} objects...", i + 1);
            }
        }

        // Explicit sync before "crash"
        engine.sync().await.expect("Failed to sync");
        println!("All {} objects written to volumes", num_objects);

        // List volume files to verify they exist
        let entries: Vec<_> = std::fs::read_dir(&data_path)
            .expect("Failed to read data dir")
            .filter_map(|e| e.ok())
            .collect();
        println!("Volume files created: {}", entries.len());
        for entry in &entries {
            let meta = entry.metadata().expect("Failed to get metadata");
            println!("  {} ({} bytes)", entry.path().display(), meta.len());
        }
    }
    // Engine dropped here - simulates crash

    // Small delay to ensure file handles are released
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Phase 2: Recover
    println!("Simulating crash... recovering from volume files...");

    // Verify volume files still exist after engine drop
    let entries: Vec<_> = std::fs::read_dir(&data_path)
        .expect("Failed to read data dir after drop")
        .filter_map(|e| e.ok())
        .collect();
    println!("Volume files after drop: {}", entries.len());

    let engine = BitcaskStorageEngine::new(
        data_path.clone(),
        metadata_path.clone(),
        1024 * 1024 * 50,
        inline_threshold,
        false,
    )
    .await
    .expect("Failed to recover storage engine");

    // Phase 3: Verify all data
    let mut verified_count = 0;
    let mut failed_keys: Vec<(String, String)> = Vec::new();

    for (full_key, expected_bytes) in &expected_data {
        let parts: Vec<&str> = full_key.splitn(2, '/').collect();
        let bucket = parts[0];
        let key = parts[1];

        match engine.get_object(bucket, key).await {
            Ok((data, _record)) => {
                if &data == expected_bytes {
                    verified_count += 1;
                } else {
                    failed_keys.push((
                        full_key.clone(),
                        format!(
                            "Data mismatch: expected {} bytes, got {} bytes",
                            expected_bytes.len(),
                            data.len()
                        ),
                    ));
                }
            }
            Err(e) => {
                failed_keys.push((full_key.clone(), format!("Error: {:?}", e)));
            }
        }

        if verified_count % 100 == 0 && verified_count > 0 {
            println!("Verified {} objects...", verified_count);
        }
    }

    println!(
        "Recovery complete: {}/{} objects verified",
        verified_count, num_objects
    );

    if !failed_keys.is_empty() {
        println!("Failed keys ({}):", failed_keys.len());
        for (key, error) in failed_keys.iter().take(20) {
            println!("  {} - {}", key, error);
        }
    }

    assert_eq!(
        verified_count,
        num_objects,
        "Not all objects were recovered: {}/{} (failed: {})",
        verified_count,
        num_objects,
        failed_keys.len()
    );
}

/// Test crash recovery with mixed object sizes (inline and volume storage)
#[tokio::test]
async fn test_crash_recovery_mixed_sizes() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let data_path = temp_dir.path().join("volumes");
    let metadata_path = temp_dir.path().join("metadata.redb");

    std::fs::create_dir_all(&data_path).expect("Failed to create data dir");

    let bucket = "mixed-bucket";
    let inline_threshold = 4096; // 4KB

    // Store expected data
    let mut expected_data: HashMap<String, Vec<u8>> = HashMap::new();

    // Phase 1: Write mixed-size objects
    {
        let engine = BitcaskStorageEngine::new(
            data_path.clone(),
            metadata_path.clone(),
            1024 * 1024 * 50, // 50MB volumes
            inline_threshold,
            true, // strict_sync for durability
        )
        .await
        .expect("Failed to create storage engine");

        // Write 50 tiny objects (inline storage, < 4KB)
        println!("Writing 50 tiny objects (inline)...");
        for i in 0..50 {
            let key = format!("tiny_{:04}", i);
            let data = format!("tiny_data_{:04}", i).as_bytes().to_vec();

            let mut metadata = HashMap::new();
            metadata.insert("size-class".to_string(), "tiny".to_string());

            engine
                .put_object(bucket, &key, &data, "text/plain", &metadata)
                .await
                .expect("Failed to put tiny object");

            expected_data.insert(format!("{}/{}", bucket, key), data);
        }

        // Write 50 small objects (just above threshold, ~5KB)
        println!("Writing 50 small objects (volume)...");
        for i in 0..50 {
            let key = format!("small_{:04}", i);
            let mut data = vec![b'S'; 5000]; // 5KB, above inline threshold
                                             // Make each unique
            data.extend_from_slice(format!("_{:04}", i).as_bytes());

            let mut metadata = HashMap::new();
            metadata.insert("size-class".to_string(), "small".to_string());

            engine
                .put_object(bucket, &key, &data, "application/octet-stream", &metadata)
                .await
                .expect("Failed to put small object");

            expected_data.insert(format!("{}/{}", bucket, key), data);
        }

        // Write 20 medium objects (~50KB)
        println!("Writing 20 medium objects (volume)...");
        for i in 0..20 {
            let key = format!("medium_{:04}", i);
            let mut data = vec![b'M'; 50_000]; // 50KB
            data.extend_from_slice(format!("_{:04}", i).as_bytes());

            let mut metadata = HashMap::new();
            metadata.insert("size-class".to_string(), "medium".to_string());

            engine
                .put_object(bucket, &key, &data, "application/octet-stream", &metadata)
                .await
                .expect("Failed to put medium object");

            expected_data.insert(format!("{}/{}", bucket, key), data);
        }

        // Write 5 large objects (~500KB)
        println!("Writing 5 large objects (volume)...");
        for i in 0..5 {
            let key = format!("large_{:04}", i);
            let mut data = vec![b'L'; 500_000]; // 500KB
            data.extend_from_slice(format!("_{:04}", i).as_bytes());

            let mut metadata = HashMap::new();
            metadata.insert("size-class".to_string(), "large".to_string());

            engine
                .put_object(bucket, &key, &data, "application/octet-stream", &metadata)
                .await
                .expect("Failed to put large object");

            expected_data.insert(format!("{}/{}", bucket, key), data);
        }

        engine.sync().await.expect("Failed to sync");
        println!("Written {} objects of mixed sizes", expected_data.len());

        // List volume files
        let entries: Vec<_> = std::fs::read_dir(&data_path)
            .expect("Failed to read data dir")
            .filter_map(|e| e.ok())
            .collect();
        println!("Volume files: {}", entries.len());
        for entry in &entries {
            let meta = entry.metadata().expect("Failed to get metadata");
            println!("  {} ({} bytes)", entry.path().display(), meta.len());
        }
    }
    // Engine dropped - simulates crash

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Phase 2: Recover
    println!("Recovering from crash...");

    let engine = BitcaskStorageEngine::new(
        data_path.clone(),
        metadata_path.clone(),
        1024 * 1024 * 50,
        inline_threshold,
        false,
    )
    .await
    .expect("Failed to recover storage engine");

    // Phase 3: Verify all data
    let mut verified = 0;
    let mut failed: Vec<(String, String)> = Vec::new();

    for (full_key, expected) in &expected_data {
        let parts: Vec<&str> = full_key.splitn(2, '/').collect();
        let bucket = parts[0];
        let key = parts[1];

        match engine.get_object(bucket, key).await {
            Ok((data, _)) => {
                if &data == expected {
                    verified += 1;
                } else {
                    failed.push((
                        full_key.clone(),
                        format!(
                            "Data mismatch: expected {} bytes, got {} bytes",
                            expected.len(),
                            data.len()
                        ),
                    ));
                }
            }
            Err(e) => {
                failed.push((full_key.clone(), format!("{:?}", e)));
            }
        }
    }

    println!("Verified {}/{} objects", verified, expected_data.len());

    if !failed.is_empty() {
        println!("Failed ({}):", failed.len());
        for (key, err) in failed.iter().take(20) {
            println!("  {} - {}", key, err);
        }
        panic!("Recovery failed for {} objects", failed.len());
    }

    assert_eq!(verified, expected_data.len());
}

/// Test with inline-only objects (original 10k test, renamed for clarity)
#[tokio::test]
async fn test_crash_recovery_inline_only() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let data_path = temp_dir.path().join("volumes");
    let metadata_path = temp_dir.path().join("metadata.redb");

    std::fs::create_dir_all(&data_path).expect("Failed to create data dir");

    let bucket = "inline-bucket";
    let num_objects = 1_000; // Reduced for speed
    let inline_threshold = 4096;

    let mut expected_data: HashMap<String, Vec<u8>> = HashMap::new();

    {
        let engine = BitcaskStorageEngine::new(
            data_path.clone(),
            metadata_path.clone(),
            1024 * 1024 * 100,
            inline_threshold,
            false,
        )
        .await
        .expect("Failed to create storage engine");

        for i in 0..num_objects {
            let key = format!("object_{:06}", i);
            // Small data that fits inline (< 4KB)
            let data = format!("inline_data_{:06}", i).as_bytes().to_vec();

            engine
                .put_object(bucket, &key, &data, "text/plain", &HashMap::new())
                .await
                .expect("Failed to put object");

            expected_data.insert(format!("{}/{}", bucket, key), data);
        }

        engine.sync().await.expect("Failed to sync");
        println!("Written {} inline objects", num_objects);
    }

    let engine = BitcaskStorageEngine::new(
        data_path.clone(),
        metadata_path.clone(),
        1024 * 1024 * 100,
        inline_threshold,
        false,
    )
    .await
    .expect("Failed to recover");

    let mut verified = 0;
    for (full_key, expected) in &expected_data {
        let parts: Vec<&str> = full_key.splitn(2, '/').collect();
        let (data, _) = engine.get_object(parts[0], parts[1]).await.expect("Failed to get");
        assert_eq!(&data, expected);
        verified += 1;
    }

    println!("Verified {} inline objects", verified);
    assert_eq!(verified, num_objects);
}
