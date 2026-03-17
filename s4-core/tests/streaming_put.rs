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

//! Tests for `put_object_streaming` in the storage engine.

use s4_core::storage::BitcaskStorageEngine;
use s4_core::StorageEngine;
use std::collections::HashMap;
use tempfile::TempDir;

async fn create_engine() -> (BitcaskStorageEngine, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path().join("volumes");
    let metadata_path = temp_dir.path().join("metadata_db");
    std::fs::create_dir_all(&data_path).unwrap();

    let engine = BitcaskStorageEngine::new(data_path, metadata_path, 10 * 1024 * 1024, 4096, false)
        .await
        .unwrap();

    (engine, temp_dir)
}

#[tokio::test]
async fn test_streaming_put_basic() {
    let (engine, _tmp) = create_engine().await;

    let data = b"hello streaming world! this is test data for streaming put";
    let reader: Box<dyn tokio::io::AsyncRead + Unpin + Send> =
        Box::new(std::io::Cursor::new(data.to_vec()));

    let result = engine
        .put_object_streaming(
            "test-bucket",
            "streamed.txt",
            reader,
            data.len() as u64,
            "text/plain",
            &HashMap::new(),
            "test-etag",
        )
        .await
        .unwrap();

    assert_eq!(result.bytes_written, data.len() as u64);
    assert_ne!(result.content_hash, [0u8; 32]);
    assert_ne!(result.crc32, 0);

    // Read back and verify
    let (retrieved, _meta) = engine.get_object("test-bucket", "streamed.txt").await.unwrap();
    assert_eq!(retrieved, data);
}

#[tokio::test]
async fn test_streaming_put_dedup() {
    let (engine, _tmp) = create_engine().await;

    let data = b"identical content for dedup test";

    // First streaming write
    let reader1: Box<dyn tokio::io::AsyncRead + Unpin + Send> =
        Box::new(std::io::Cursor::new(data.to_vec()));
    let result1 = engine
        .put_object_streaming(
            "test-bucket",
            "file1.txt",
            reader1,
            data.len() as u64,
            "text/plain",
            &HashMap::new(),
            "etag1",
        )
        .await
        .unwrap();

    // Second streaming write with same content under different key
    let reader2: Box<dyn tokio::io::AsyncRead + Unpin + Send> =
        Box::new(std::io::Cursor::new(data.to_vec()));
    let result2 = engine
        .put_object_streaming(
            "test-bucket",
            "file2.txt",
            reader2,
            data.len() as u64,
            "text/plain",
            &HashMap::new(),
            "etag2",
        )
        .await
        .unwrap();

    // Both should produce the same content hash (SHA-256)
    assert_eq!(result1.content_hash, result2.content_hash);

    // Both files should be readable
    let (d1, _) = engine.get_object("test-bucket", "file1.txt").await.unwrap();
    let (d2, _) = engine.get_object("test-bucket", "file2.txt").await.unwrap();
    assert_eq!(d1, data);
    assert_eq!(d2, data);
}

#[tokio::test]
async fn test_streaming_put_large() {
    let (engine, _tmp) = create_engine().await;

    // 2MB of patterned data (large enough to exercise multi-buffer streaming)
    let size = 2 * 1024 * 1024;
    let data: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();

    let reader: Box<dyn tokio::io::AsyncRead + Unpin + Send> =
        Box::new(std::io::Cursor::new(data.clone()));

    let result = engine
        .put_object_streaming(
            "test-bucket",
            "large.bin",
            reader,
            data.len() as u64,
            "application/octet-stream",
            &HashMap::new(),
            "etag-large",
        )
        .await
        .unwrap();

    assert_eq!(result.bytes_written, size as u64);

    // Read back and verify integrity
    let (retrieved, _meta) = engine.get_object("test-bucket", "large.bin").await.unwrap();
    assert_eq!(retrieved.len(), data.len());
    assert_eq!(retrieved, data);
}
