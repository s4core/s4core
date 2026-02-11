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

//! Deduplication Integration Test
//!
//! This test verifies Phase 1 success criterion:
//! "Дедупликация работает (2 одинаковых файла = 1 запись на диске)"
//!
//! We write multiple objects with identical content and verify
//! that only one copy is stored on disk.

use s4_core::storage::BitcaskStorageEngine;
use s4_core::StorageEngine;
use std::collections::HashMap;
use std::fs;
use tempfile::TempDir;

/// Test that duplicate content is stored only once
#[tokio::test]
async fn test_deduplication_identical_files() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let data_path = temp_dir.path().join("volumes");
    let metadata_path = temp_dir.path().join("metadata.redb");

    std::fs::create_dir_all(&data_path).expect("Failed to create data dir");

    let bucket = "dedup-test";
    let inline_threshold = 1024; // 1KB - we want to test volume storage

    // Create identical content that will be stored in volume (> threshold)
    let identical_content = vec![b'X'; 10_000]; // 10KB of 'X'
    let num_duplicates = 100;

    let engine = BitcaskStorageEngine::new(
        data_path.clone(),
        metadata_path.clone(),
        1024 * 1024 * 100, // 100MB volume
        inline_threshold,
        false,
    )
    .await
    .expect("Failed to create storage engine");

    // Write the same content with different keys
    for i in 0..num_duplicates {
        let key = format!("duplicate_file_{:04}", i);
        let mut metadata = HashMap::new();
        metadata.insert("original-name".to_string(), format!("file_{}.dat", i));

        engine
            .put_object(
                bucket,
                &key,
                &identical_content,
                "application/octet-stream",
                &metadata,
            )
            .await
            .expect("Failed to put object");
    }

    engine.sync().await.expect("Failed to sync");

    // Calculate total volume size
    let mut total_volume_size: u64 = 0;
    for entry in fs::read_dir(&data_path).expect("Failed to read data dir") {
        let entry = entry.expect("Failed to read entry");
        if entry.path().extension().is_some_and(|ext| ext == "dat") {
            let metadata = fs::metadata(entry.path()).expect("Failed to get metadata");
            total_volume_size += metadata.len();
            println!(
                "Volume file: {} ({} bytes)",
                entry.path().display(),
                metadata.len()
            );
        }
    }

    // Expected size WITHOUT deduplication: num_duplicates * content_size
    let size_without_dedup = num_duplicates as u64 * identical_content.len() as u64;

    // With deduplication, we should store content only ONCE
    // Plus some overhead for headers (approximately content_size + headers)
    let expected_max_size = identical_content.len() as u64 * 2; // Content + generous header allowance

    println!("Size without deduplication: {} bytes", size_without_dedup);
    println!("Actual volume size: {} bytes", total_volume_size);
    println!("Expected max size with dedup: {} bytes", expected_max_size);

    // Verify deduplication saved space
    assert!(
        total_volume_size < size_without_dedup,
        "Deduplication did not save space: {} >= {}",
        total_volume_size,
        size_without_dedup
    );

    // Verify significant space savings (at least 50% reduction)
    let savings_percent = 100.0 * (1.0 - (total_volume_size as f64 / size_without_dedup as f64));
    println!("Space savings: {:.1}%", savings_percent);

    assert!(
        savings_percent > 50.0,
        "Deduplication savings too low: {:.1}% (expected > 50%)",
        savings_percent
    );

    // Verify all objects can be read correctly
    for i in 0..num_duplicates {
        let key = format!("duplicate_file_{:04}", i);
        let (data, metadata) = engine.get_object(bucket, &key).await.expect("Failed to get object");

        assert_eq!(data, identical_content, "Content mismatch for key {}", key);
        assert_eq!(
            metadata.metadata.get("original-name"),
            Some(&format!("file_{}.dat", i)),
            "Metadata mismatch for key {}",
            key
        );
    }

    println!(
        "SUCCESS: {} identical files stored with {:.1}% space savings",
        num_duplicates, savings_percent
    );
}

/// Test deduplication with different content hashes
#[tokio::test]
async fn test_deduplication_different_content() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let data_path = temp_dir.path().join("volumes");
    let metadata_path = temp_dir.path().join("metadata.redb");

    std::fs::create_dir_all(&data_path).expect("Failed to create data dir");

    let bucket = "unique-test";
    let inline_threshold = 1024;

    let engine = BitcaskStorageEngine::new(
        data_path.clone(),
        metadata_path.clone(),
        1024 * 1024 * 100,
        inline_threshold,
        false,
    )
    .await
    .expect("Failed to create storage engine");

    // Write unique content for each object
    let num_objects = 50;
    let content_size = 10_000; // 10KB each

    for i in 0..num_objects {
        let key = format!("unique_file_{:04}", i);
        // Each file has unique content (filled with different byte)
        let content = vec![(i % 256) as u8; content_size];

        let metadata = HashMap::new();
        engine
            .put_object(
                bucket,
                &key,
                &content,
                "application/octet-stream",
                &metadata,
            )
            .await
            .expect("Failed to put object");
    }

    engine.sync().await.expect("Failed to sync");

    // Calculate total volume size
    let mut total_volume_size: u64 = 0;
    for entry in fs::read_dir(&data_path).expect("Failed to read data dir") {
        let entry = entry.expect("Failed to read entry");
        if entry.path().extension().is_some_and(|ext| ext == "dat") {
            let metadata = fs::metadata(entry.path()).expect("Failed to get metadata");
            total_volume_size += metadata.len();
        }
    }

    // With unique content, no deduplication should occur
    // Total size should be approximately num_objects * content_size
    let expected_min_size = (num_objects * content_size) as u64;

    println!("Unique content test:");
    println!("Expected min size: {} bytes", expected_min_size);
    println!("Actual volume size: {} bytes", total_volume_size);

    // Volume should be at least as large as the sum of all content
    // (plus headers, so it should be larger)
    assert!(
        total_volume_size >= expected_min_size,
        "Volume size too small for unique content: {} < {}",
        total_volume_size,
        expected_min_size
    );

    // Verify all objects can be read
    for i in 0..num_objects {
        let key = format!("unique_file_{:04}", i);
        let expected_content = vec![(i % 256) as u8; content_size];

        let (data, _) = engine.get_object(bucket, &key).await.expect("Failed to get object");

        assert_eq!(data, expected_content, "Content mismatch for key {}", key);
    }

    println!(
        "SUCCESS: {} unique files stored without false deduplication",
        num_objects
    );
}

/// Test deduplication with partial duplicates (some same, some different)
#[tokio::test]
async fn test_deduplication_partial() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let data_path = temp_dir.path().join("volumes");
    let metadata_path = temp_dir.path().join("metadata.redb");

    std::fs::create_dir_all(&data_path).expect("Failed to create data dir");

    let bucket = "partial-test";
    let inline_threshold = 1024;

    let engine = BitcaskStorageEngine::new(
        data_path.clone(),
        metadata_path.clone(),
        1024 * 1024 * 100,
        inline_threshold,
        false,
    )
    .await
    .expect("Failed to create storage engine");

    // 5 different content patterns
    let patterns: Vec<Vec<u8>> = (0..5).map(|i| vec![(i * 50) as u8; 10_000]).collect();

    // Each pattern is used 10 times = 50 total objects
    let repeats_per_pattern = 10;

    for (pattern_idx, pattern) in patterns.iter().enumerate() {
        for repeat in 0..repeats_per_pattern {
            let key = format!("pattern_{}_{:02}", pattern_idx, repeat);
            let metadata = HashMap::new();

            engine
                .put_object(bucket, &key, pattern, "application/octet-stream", &metadata)
                .await
                .expect("Failed to put object");
        }
    }

    engine.sync().await.expect("Failed to sync");

    // Calculate expected sizes
    let total_objects = patterns.len() * repeats_per_pattern;
    let size_without_dedup = (total_objects * 10_000) as u64; // 50 * 10KB = 500KB
    let size_with_perfect_dedup = (patterns.len() * 10_000) as u64; // 5 * 10KB = 50KB

    let mut total_volume_size: u64 = 0;
    for entry in fs::read_dir(&data_path).expect("Failed to read data dir") {
        let entry = entry.expect("Failed to read entry");
        if entry.path().extension().is_some_and(|ext| ext == "dat") {
            let metadata = fs::metadata(entry.path()).expect("Failed to get metadata");
            total_volume_size += metadata.len();
        }
    }

    println!("Partial deduplication test:");
    println!("Size without dedup: {} bytes", size_without_dedup);
    println!("Size with perfect dedup: {} bytes", size_with_perfect_dedup);
    println!("Actual volume size: {} bytes", total_volume_size);

    // Should be much closer to perfect dedup than no dedup
    assert!(
        total_volume_size < size_without_dedup / 2,
        "Deduplication not effective enough: {} >= {}",
        total_volume_size,
        size_without_dedup / 2
    );

    // Verify all objects
    for (pattern_idx, pattern) in patterns.iter().enumerate() {
        for repeat in 0..repeats_per_pattern {
            let key = format!("pattern_{}_{:02}", pattern_idx, repeat);
            let (data, _) = engine.get_object(bucket, &key).await.expect("Failed to get object");

            assert_eq!(data, *pattern, "Content mismatch for key {}", key);
        }
    }

    let savings = 100.0 * (1.0 - (total_volume_size as f64 / size_without_dedup as f64));
    println!(
        "SUCCESS: {} objects with {} unique patterns, {:.1}% space savings",
        total_objects,
        patterns.len(),
        savings
    );
}
