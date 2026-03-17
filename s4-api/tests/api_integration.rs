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

//! API Integration Tests
//!
//! Tests the S3-compatible HTTP API using in-process requests.
//! No actual network I/O - uses tower::ServiceExt::oneshot directly.
//!
//! This is the standard approach for testing Axum applications.
//!
//! Note: Authentication is disabled for these tests via S4_DISABLE_AUTH=1
//! to focus on testing the API functionality, not authentication.

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use http_body_util::BodyExt;
use s4_api::{create_router, AppState};
use s4_core::storage::BitcaskStorageEngine;
use std::sync::Once;
use tempfile::TempDir;
use tower::ServiceExt;

/// Initialize test environment once for all tests.
/// This sets S4_DISABLE_AUTH=1 to bypass authentication.
static INIT: Once = Once::new();

fn init_test_env() {
    INIT.call_once(|| {
        std::env::set_var("S4_DISABLE_AUTH", "1");
    });
}

/// Creates a test storage engine in a temporary directory.
async fn create_test_storage() -> (BitcaskStorageEngine, TempDir) {
    // Ensure auth is disabled before any test runs
    init_test_env();

    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let data_path = temp_dir.path().join("volumes");
    let metadata_path = temp_dir.path().join("metadata_db");

    std::fs::create_dir_all(&data_path).expect("Failed to create data dir");

    let engine = BitcaskStorageEngine::new(
        data_path,
        metadata_path,
        1024 * 1024 * 10, // 10MB volume
        4096,             // 4KB inline threshold
        false,            // no strict sync for tests
    )
    .await
    .expect("Failed to create storage engine");

    (engine, temp_dir)
}

/// Creates an AppState with test credentials.
async fn create_test_state(storage: BitcaskStorageEngine, data_dir: &std::path::Path) -> AppState {
    AppState::new(
        storage,
        "test-access-key".to_string(),
        "test-secret-key".to_string(),
        data_dir,
    )
    .await
}

/// Helper to read response body as string.
async fn body_to_string(body: Body) -> String {
    let bytes = body.collect().await.unwrap().to_bytes();
    String::from_utf8(bytes.to_vec()).unwrap()
}

// ============================================================================
// Bucket Operations Tests
// ============================================================================

#[tokio::test]
async fn test_create_bucket() {
    let (storage, _temp) = create_test_storage().await;
    let app = create_router(create_test_state(storage, _temp.path()).await);

    let response = app
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/test-bucket")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_create_bucket_invalid_name() {
    let (storage, _temp) = create_test_storage().await;
    let app = create_router(create_test_state(storage, _temp.path()).await);

    // Bucket name too short
    let response = app
        .oneshot(Request::builder().method("PUT").uri("/ab").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_create_bucket_already_exists() {
    let (storage, _temp) = create_test_storage().await;
    let state = create_test_state(storage, _temp.path()).await;

    // Create first bucket
    let app1 = create_router(state.clone());
    let response = app1
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/test-bucket")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Try to create same bucket again
    let app2 = create_router(state);
    let response = app2
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/test-bucket")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn test_list_buckets_empty() {
    let (storage, _temp) = create_test_storage().await;
    let app = create_router(create_test_state(storage, _temp.path()).await);

    let response = app
        .oneshot(Request::builder().method("GET").uri("/").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = body_to_string(response.into_body()).await;
    assert!(body.contains("ListAllMyBucketsResult"));
    assert!(body.contains("<Buckets>"));
}

#[tokio::test]
async fn test_list_buckets_with_buckets() {
    let (storage, _temp) = create_test_storage().await;
    let state = create_test_state(storage, _temp.path()).await;

    // Create a bucket first
    let app1 = create_router(state.clone());
    app1.oneshot(
        Request::builder()
            .method("PUT")
            .uri("/my-test-bucket")
            .body(Body::empty())
            .unwrap(),
    )
    .await
    .unwrap();

    // List buckets
    let app2 = create_router(state);
    let response = app2
        .oneshot(Request::builder().method("GET").uri("/").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = body_to_string(response.into_body()).await;
    assert!(body.contains("<Name>my-test-bucket</Name>"));
}

#[tokio::test]
async fn test_head_bucket_exists() {
    let (storage, _temp) = create_test_storage().await;
    let state = create_test_state(storage, _temp.path()).await;

    // Create bucket
    let app1 = create_router(state.clone());
    app1.oneshot(
        Request::builder()
            .method("PUT")
            .uri("/test-bucket")
            .body(Body::empty())
            .unwrap(),
    )
    .await
    .unwrap();

    // HEAD bucket
    let app2 = create_router(state);
    let response = app2
        .oneshot(
            Request::builder()
                .method("HEAD")
                .uri("/test-bucket")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_head_bucket_not_exists() {
    let (storage, _temp) = create_test_storage().await;
    let app = create_router(create_test_state(storage, _temp.path()).await);

    let response = app
        .oneshot(
            Request::builder()
                .method("HEAD")
                .uri("/nonexistent-bucket")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_delete_bucket() {
    let (storage, _temp) = create_test_storage().await;
    let state = create_test_state(storage, _temp.path()).await;

    // Create bucket
    let app1 = create_router(state.clone());
    app1.oneshot(
        Request::builder()
            .method("PUT")
            .uri("/test-bucket")
            .body(Body::empty())
            .unwrap(),
    )
    .await
    .unwrap();

    // Delete bucket
    let app2 = create_router(state.clone());
    let response = app2
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/test-bucket")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    // Verify bucket is gone
    let app3 = create_router(state);
    let response = app3
        .oneshot(
            Request::builder()
                .method("HEAD")
                .uri("/test-bucket")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_delete_bucket_not_exists() {
    let (storage, _temp) = create_test_storage().await;
    let app = create_router(create_test_state(storage, _temp.path()).await);

    let response = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/nonexistent-bucket")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

// ============================================================================
// Object Operations Tests
// ============================================================================

#[tokio::test]
async fn test_put_object() {
    let (storage, _temp) = create_test_storage().await;
    let state = create_test_state(storage, _temp.path()).await;

    // Create bucket first
    let app1 = create_router(state.clone());
    app1.oneshot(
        Request::builder()
            .method("PUT")
            .uri("/test-bucket")
            .body(Body::empty())
            .unwrap(),
    )
    .await
    .unwrap();

    // Put object
    let app2 = create_router(state);
    let response = app2
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/test-bucket/hello.txt")
                .header("content-type", "text/plain")
                .body(Body::from("Hello, S4!"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.headers().contains_key("etag"));
}

#[tokio::test]
async fn test_put_object_bucket_not_exists() {
    let (storage, _temp) = create_test_storage().await;
    let app = create_router(create_test_state(storage, _temp.path()).await);

    let response = app
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/nonexistent-bucket/hello.txt")
                .body(Body::from("Hello"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_get_object() {
    let (storage, _temp) = create_test_storage().await;
    let state = create_test_state(storage, _temp.path()).await;

    // Create bucket
    let app1 = create_router(state.clone());
    app1.oneshot(
        Request::builder()
            .method("PUT")
            .uri("/test-bucket")
            .body(Body::empty())
            .unwrap(),
    )
    .await
    .unwrap();

    // Put object
    let app2 = create_router(state.clone());
    app2.oneshot(
        Request::builder()
            .method("PUT")
            .uri("/test-bucket/hello.txt")
            .header("content-type", "text/plain")
            .body(Body::from("Hello, S4!"))
            .unwrap(),
    )
    .await
    .unwrap();

    // Get object
    let app3 = create_router(state);
    let response = app3
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/test-bucket/hello.txt")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.headers().contains_key("etag"));
    assert!(response.headers().contains_key("content-type"));
    assert!(response.headers().contains_key("last-modified"));

    let body = body_to_string(response.into_body()).await;
    assert_eq!(body, "Hello, S4!");
}

#[tokio::test]
async fn test_get_object_not_exists() {
    let (storage, _temp) = create_test_storage().await;
    let state = create_test_state(storage, _temp.path()).await;

    // Create bucket
    let app1 = create_router(state.clone());
    app1.oneshot(
        Request::builder()
            .method("PUT")
            .uri("/test-bucket")
            .body(Body::empty())
            .unwrap(),
    )
    .await
    .unwrap();

    // Try to get non-existent object
    let app2 = create_router(state);
    let response = app2
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/test-bucket/nonexistent.txt")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_head_object() {
    let (storage, _temp) = create_test_storage().await;
    let state = create_test_state(storage, _temp.path()).await;

    // Create bucket
    let app1 = create_router(state.clone());
    app1.oneshot(
        Request::builder()
            .method("PUT")
            .uri("/test-bucket")
            .body(Body::empty())
            .unwrap(),
    )
    .await
    .unwrap();

    // Put object
    let app2 = create_router(state.clone());
    app2.oneshot(
        Request::builder()
            .method("PUT")
            .uri("/test-bucket/hello.txt")
            .header("content-type", "text/plain")
            .body(Body::from("Hello, S4!"))
            .unwrap(),
    )
    .await
    .unwrap();

    // HEAD object
    let app3 = create_router(state);
    let response = app3
        .oneshot(
            Request::builder()
                .method("HEAD")
                .uri("/test-bucket/hello.txt")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.headers().contains_key("etag"));
    assert!(response.headers().contains_key("content-length"));
    assert_eq!(
        response.headers().get("content-length").unwrap().to_str().unwrap(),
        "10" // "Hello, S4!" is 10 bytes
    );
}

#[tokio::test]
async fn test_delete_object() {
    let (storage, _temp) = create_test_storage().await;
    let state = create_test_state(storage, _temp.path()).await;

    // Create bucket
    let app1 = create_router(state.clone());
    app1.oneshot(
        Request::builder()
            .method("PUT")
            .uri("/test-bucket")
            .body(Body::empty())
            .unwrap(),
    )
    .await
    .unwrap();

    // Put object
    let app2 = create_router(state.clone());
    app2.oneshot(
        Request::builder()
            .method("PUT")
            .uri("/test-bucket/hello.txt")
            .body(Body::from("Hello"))
            .unwrap(),
    )
    .await
    .unwrap();

    // Delete object
    let app3 = create_router(state.clone());
    let response = app3
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/test-bucket/hello.txt")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    // Verify object is gone
    let app4 = create_router(state);
    let response = app4
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/test-bucket/hello.txt")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_list_objects_empty() {
    let (storage, _temp) = create_test_storage().await;
    let state = create_test_state(storage, _temp.path()).await;

    // Create bucket
    let app1 = create_router(state.clone());
    app1.oneshot(
        Request::builder()
            .method("PUT")
            .uri("/test-bucket")
            .body(Body::empty())
            .unwrap(),
    )
    .await
    .unwrap();

    // List objects
    let app2 = create_router(state);
    let response = app2
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/test-bucket")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = body_to_string(response.into_body()).await;
    assert!(body.contains("ListBucketResult"));
    assert!(body.contains("<Name>test-bucket</Name>"));
    assert!(body.contains("<IsTruncated>false</IsTruncated>"));
}

#[tokio::test]
async fn test_list_objects_with_objects() {
    let (storage, _temp) = create_test_storage().await;
    let state = create_test_state(storage, _temp.path()).await;

    // Create bucket
    let app1 = create_router(state.clone());
    app1.oneshot(
        Request::builder()
            .method("PUT")
            .uri("/test-bucket")
            .body(Body::empty())
            .unwrap(),
    )
    .await
    .unwrap();

    // Put objects
    for name in ["file1.txt", "file2.txt", "subdir/file3.txt"] {
        let app = create_router(state.clone());
        app.oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/test-bucket/{}", name))
                .body(Body::from("content"))
                .unwrap(),
        )
        .await
        .unwrap();
    }

    // List objects
    let app2 = create_router(state);
    let response = app2
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/test-bucket")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = body_to_string(response.into_body()).await;
    assert!(body.contains("<Key>file1.txt</Key>"));
    assert!(body.contains("<Key>file2.txt</Key>"));
    assert!(body.contains("<Key>subdir/file3.txt</Key>"));
}

#[tokio::test]
async fn test_list_objects_with_prefix() {
    let (storage, _temp) = create_test_storage().await;
    let state = create_test_state(storage, _temp.path()).await;

    // Create bucket
    let app1 = create_router(state.clone());
    app1.oneshot(
        Request::builder()
            .method("PUT")
            .uri("/test-bucket")
            .body(Body::empty())
            .unwrap(),
    )
    .await
    .unwrap();

    // Put objects
    for name in ["logs/app.log", "logs/error.log", "data/file.txt"] {
        let app = create_router(state.clone());
        app.oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/test-bucket/{}", name))
                .body(Body::from("content"))
                .unwrap(),
        )
        .await
        .unwrap();
    }

    // List objects with prefix
    let app2 = create_router(state);
    let response = app2
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/test-bucket?prefix=logs/")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = body_to_string(response.into_body()).await;
    assert!(body.contains("<Key>logs/app.log</Key>"));
    assert!(body.contains("<Key>logs/error.log</Key>"));
    assert!(!body.contains("<Key>data/file.txt</Key>"));
}

// ============================================================================
// Object with custom metadata
// ============================================================================

#[tokio::test]
async fn test_object_custom_metadata() {
    let (storage, _temp) = create_test_storage().await;
    let state = create_test_state(storage, _temp.path()).await;

    // Create bucket
    let app1 = create_router(state.clone());
    app1.oneshot(
        Request::builder()
            .method("PUT")
            .uri("/test-bucket")
            .body(Body::empty())
            .unwrap(),
    )
    .await
    .unwrap();

    // Put object with custom metadata
    let app2 = create_router(state.clone());
    app2.oneshot(
        Request::builder()
            .method("PUT")
            .uri("/test-bucket/doc.pdf")
            .header("content-type", "application/pdf")
            .header("x-amz-meta-author", "John Doe")
            .header("x-amz-meta-version", "1.0")
            .body(Body::from("PDF content"))
            .unwrap(),
    )
    .await
    .unwrap();

    // Get object and verify metadata
    let app3 = create_router(state);
    let response = app3
        .oneshot(
            Request::builder()
                .method("HEAD")
                .uri("/test-bucket/doc.pdf")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("x-amz-meta-author").map(|v| v.to_str().unwrap()),
        Some("John Doe")
    );
    assert_eq!(
        response.headers().get("x-amz-meta-version").map(|v| v.to_str().unwrap()),
        Some("1.0")
    );
}

// ============================================================================
// Full workflow test
// ============================================================================

#[tokio::test]
async fn test_full_s3_workflow() {
    let (storage, _temp) = create_test_storage().await;
    let state = create_test_state(storage, _temp.path()).await;

    // 1. List buckets (empty)
    let app = create_router(state.clone());
    let response = app
        .oneshot(Request::builder().method("GET").uri("/").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = body_to_string(response.into_body()).await;
    assert!(!body.contains("<Bucket>"));

    // 2. Create bucket
    let app = create_router(state.clone());
    let response = app
        .oneshot(Request::builder().method("PUT").uri("/my-bucket").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // 3. Upload files
    for i in 0..5 {
        let app = create_router(state.clone());
        app.oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/my-bucket/file{}.txt", i))
                .body(Body::from(format!("Content of file {}", i)))
                .unwrap(),
        )
        .await
        .unwrap();
    }

    // 4. List objects
    let app = create_router(state.clone());
    let response = app
        .oneshot(Request::builder().method("GET").uri("/my-bucket").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = body_to_string(response.into_body()).await;
    for i in 0..5 {
        assert!(body.contains(&format!("<Key>file{}.txt</Key>", i)));
    }

    // 5. Download and verify file
    let app = create_router(state.clone());
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/my-bucket/file2.txt")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = body_to_string(response.into_body()).await;
    assert_eq!(body, "Content of file 2");

    // 6. Delete some objects
    for i in 0..3 {
        let app = create_router(state.clone());
        app.oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/my-bucket/file{}.txt", i))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    }

    // 7. List objects again (should have 2 left)
    let app = create_router(state.clone());
    let response = app
        .oneshot(Request::builder().method("GET").uri("/my-bucket").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let body = body_to_string(response.into_body()).await;
    assert!(!body.contains("<Key>file0.txt</Key>"));
    assert!(!body.contains("<Key>file1.txt</Key>"));
    assert!(!body.contains("<Key>file2.txt</Key>"));
    assert!(body.contains("<Key>file3.txt</Key>"));
    assert!(body.contains("<Key>file4.txt</Key>"));

    // 8. Delete remaining objects
    for i in 3..5 {
        let app = create_router(state.clone());
        app.oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/my-bucket/file{}.txt", i))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    }

    // 9. Delete bucket
    let app = create_router(state.clone());
    let response = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/my-bucket")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    // 10. Verify bucket is gone
    let app = create_router(state);
    let response = app
        .oneshot(Request::builder().method("GET").uri("/").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let body = body_to_string(response.into_body()).await;
    assert!(!body.contains("<Name>my-bucket</Name>"));
}

// ============================================================================
// Large Object Tests (data stored in volume files, not inline)
// ============================================================================

/// Helper to read response body as raw bytes.
async fn body_to_bytes(body: Body) -> Vec<u8> {
    body.collect().await.unwrap().to_bytes().to_vec()
}

#[tokio::test]
async fn test_large_object_roundtrip() {
    let (storage, _temp) = create_test_storage().await;
    let state = create_test_state(storage, _temp.path()).await;

    // Create bucket
    let app = create_router(state.clone());
    app.oneshot(
        Request::builder()
            .method("PUT")
            .uri("/test-bucket")
            .body(Body::empty())
            .unwrap(),
    )
    .await
    .unwrap();

    // Create data larger than 4KB inline threshold (5000 bytes)
    let original_data: Vec<u8> = (0..5000).map(|i| (i % 256) as u8).collect();

    // Put large object
    let app = create_router(state.clone());
    let response = app
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/test-bucket/large-file.bin")
                .header("content-type", "application/octet-stream")
                .body(Body::from(original_data.clone()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Get large object
    let app = create_router(state);
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/test-bucket/large-file.bin")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let retrieved_data = body_to_bytes(response.into_body()).await;

    // Verify exact match
    assert_eq!(
        retrieved_data.len(),
        original_data.len(),
        "Data length mismatch: got {} bytes, expected {} bytes",
        retrieved_data.len(),
        original_data.len()
    );

    // Check first 100 bytes for debugging
    if retrieved_data != original_data {
        eprintln!(
            "First 100 bytes of original: {:?}",
            &original_data[..100.min(original_data.len())]
        );
        eprintln!(
            "First 100 bytes of retrieved: {:?}",
            &retrieved_data[..100.min(retrieved_data.len())]
        );
        panic!("Data mismatch: retrieved data differs from original");
    }

    assert_eq!(retrieved_data, original_data, "Large object data mismatch");
}

#[tokio::test]
async fn test_large_object_with_text_content() {
    let (storage, _temp) = create_test_storage().await;
    let state = create_test_state(storage, _temp.path()).await;

    // Create bucket
    let app = create_router(state.clone());
    app.oneshot(
        Request::builder()
            .method("PUT")
            .uri("/test-bucket")
            .body(Body::empty())
            .unwrap(),
    )
    .await
    .unwrap();

    // Create text data larger than 4KB (simulating a script file like install.sh)
    let mut original_data = String::from("#!/bin/bash\n\n");
    while original_data.len() < 5000 {
        original_data.push_str("echo \"Line of script\"\n");
    }
    let original_bytes = original_data.as_bytes().to_vec();

    // Put large text object
    let app = create_router(state.clone());
    let response = app
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/test-bucket/install.sh")
                .header("content-type", "text/x-shellscript")
                .body(Body::from(original_bytes.clone()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Get large text object
    let app = create_router(state);
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/test-bucket/install.sh")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let retrieved_bytes = body_to_bytes(response.into_body()).await;
    let retrieved_str = String::from_utf8_lossy(&retrieved_bytes);

    // Check that content starts correctly (catches the "fa2\n" bug)
    assert!(
        retrieved_str.starts_with("#!/bin/bash"),
        "Content should start with shebang, but starts with: {:?}",
        &retrieved_str[..50.min(retrieved_str.len())]
    );

    assert_eq!(
        retrieved_bytes.len(),
        original_bytes.len(),
        "Data length mismatch"
    );
    assert_eq!(
        retrieved_bytes, original_bytes,
        "Large text object data mismatch"
    );
}

#[tokio::test]
async fn test_multipart_upload_large_file() {
    let (storage, _temp) = create_test_storage().await;
    let state = create_test_state(storage, _temp.path()).await;

    // Create bucket
    let app = create_router(state.clone());
    app.oneshot(
        Request::builder()
            .method("PUT")
            .uri("/test-bucket")
            .body(Body::empty())
            .unwrap(),
    )
    .await
    .unwrap();

    // Create data larger than 5MB (12MB total, split into 2 parts: 6MB + 6MB)
    // S3 spec requires all non-last parts to be at least 5MB
    let part_size = 6 * 1024 * 1024; // 6MB
    let total_size = part_size * 2;
    let original_data: Vec<u8> = (0..total_size).map(|i| (i % 256) as u8).collect();

    // Initiate multipart upload
    let app = create_router(state.clone());
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/test-bucket/multipart-test.bin?uploads")
                .header("content-type", "application/octet-stream")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = body_to_string(response.into_body()).await;
    let upload_id_start = body.find("<UploadId>").unwrap() + "<UploadId>".len();
    let upload_id_end = body.find("</UploadId>").unwrap();
    let upload_id = &body[upload_id_start..upload_id_end];

    // Upload parts (2 parts of 6MB each)
    let mut etags = Vec::new();
    for part_num in 1..=2 {
        let start = (part_num - 1) * part_size;
        let end = part_num * part_size;
        let part_data = &original_data[start..end];

        let app = create_router(state.clone());
        let response = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(format!(
                        "/test-bucket/multipart-test.bin?uploadId={}&partNumber={}",
                        upload_id, part_num
                    ))
                    .body(Body::from(part_data.to_vec()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let etag = response.headers().get("etag").unwrap().to_str().unwrap().to_string();
        etags.push((part_num, etag));
    }

    // Complete multipart upload
    let complete_xml = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<CompleteMultipartUpload>
  <Part><PartNumber>1</PartNumber><ETag>{}</ETag></Part>
  <Part><PartNumber>2</PartNumber><ETag>{}</ETag></Part>
</CompleteMultipartUpload>"#,
        etags[0].1, etags[1].1
    );

    let app = create_router(state.clone());
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!(
                    "/test-bucket/multipart-test.bin?uploadId={}",
                    upload_id
                ))
                .header("content-type", "application/xml")
                .body(Body::from(complete_xml))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    // Must consume the full body to wait for streaming assembly to complete
    let _ = body_to_bytes(response.into_body()).await;

    // Download and verify
    let app = create_router(state);
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/test-bucket/multipart-test.bin")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let retrieved_data = body_to_bytes(response.into_body()).await;

    // Verify first bytes (catches chunk header corruption)
    assert_eq!(
        &retrieved_data[..10],
        &original_data[..10],
        "First 10 bytes mismatch. Got: {:?}, Expected: {:?}",
        &retrieved_data[..10],
        &original_data[..10]
    );

    // Verify total length
    assert_eq!(
        retrieved_data.len(),
        original_data.len(),
        "Data length mismatch"
    );

    // Verify complete data
    assert_eq!(retrieved_data, original_data, "Multipart data mismatch");
}

#[tokio::test]
async fn test_object_just_under_inline_threshold() {
    let (storage, _temp) = create_test_storage().await;
    let state = create_test_state(storage, _temp.path()).await;

    // Create bucket
    let app = create_router(state.clone());
    app.oneshot(
        Request::builder()
            .method("PUT")
            .uri("/test-bucket")
            .body(Body::empty())
            .unwrap(),
    )
    .await
    .unwrap();

    // Create data just under 4KB threshold (4095 bytes) - stored inline
    let original_data: Vec<u8> = (0..4095).map(|i| (i % 256) as u8).collect();

    // Put object
    let app = create_router(state.clone());
    let response = app
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/test-bucket/inline-test.bin")
                .body(Body::from(original_data.clone()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Get object
    let app = create_router(state);
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/test-bucket/inline-test.bin")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let retrieved_data = body_to_bytes(response.into_body()).await;

    assert_eq!(retrieved_data.len(), original_data.len());
    assert_eq!(retrieved_data, original_data, "Inline data mismatch");
}

#[tokio::test]
async fn test_object_just_over_inline_threshold() {
    let (storage, _temp) = create_test_storage().await;
    let state = create_test_state(storage, _temp.path()).await;

    // Create bucket
    let app = create_router(state.clone());
    app.oneshot(
        Request::builder()
            .method("PUT")
            .uri("/test-bucket")
            .body(Body::empty())
            .unwrap(),
    )
    .await
    .unwrap();

    // Create data just over 4KB threshold (4097 bytes) - stored in volume
    let original_data: Vec<u8> = (0..4097).map(|i| (i % 256) as u8).collect();

    // Put object
    let app = create_router(state.clone());
    let response = app
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/test-bucket/volume-test.bin")
                .body(Body::from(original_data.clone()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Get object
    let app = create_router(state);
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/test-bucket/volume-test.bin")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let retrieved_data = body_to_bytes(response.into_body()).await;

    // Check first few bytes (catches the "fa2" bug)
    assert_eq!(
        &retrieved_data[..10],
        &original_data[..10],
        "First 10 bytes mismatch (checking for corrupted prefix)"
    );

    assert_eq!(retrieved_data.len(), original_data.len());
    assert_eq!(retrieved_data, original_data, "Volume data mismatch");
}

#[tokio::test]
async fn test_aws_chunked_encoding() {
    let (storage, _temp) = create_test_storage().await;
    let state = create_test_state(storage, _temp.path()).await;

    // Create bucket
    let app = create_router(state.clone());
    app.oneshot(
        Request::builder()
            .method("PUT")
            .uri("/test-bucket")
            .body(Body::empty())
            .unwrap(),
    )
    .await
    .unwrap();

    // Create AWS chunked encoded data
    // Original content: "#!/bin/bash\necho hello\n" (24 bytes)
    let original_content = b"#!/bin/bash\necho hello\n";
    let content_len = original_content.len(); // 24

    // Encode it in AWS chunked format: "<hex size>\r\n<data>\r\n0\r\n\r\n"
    let mut aws_chunked_body = Vec::new();
    aws_chunked_body.extend_from_slice(format!("{:x}\r\n", content_len).as_bytes());
    aws_chunked_body.extend_from_slice(original_content);
    aws_chunked_body.extend_from_slice(b"\r\n0\r\n\r\n");

    // Put object with aws-chunked encoding
    let app = create_router(state.clone());
    let response = app
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/test-bucket/script.sh")
                .header("content-type", "text/x-shellscript")
                .header("content-encoding", "aws-chunked")
                .header("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER")
                .body(Body::from(aws_chunked_body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Get object - should get decoded content, not the chunked format
    let app = create_router(state);
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/test-bucket/script.sh")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let retrieved_data = body_to_bytes(response.into_body()).await;

    // Content should start with shebang, not chunk size
    assert!(
        retrieved_data.starts_with(b"#!/bin/bash"),
        "Content should start with shebang, but got: {:?}",
        String::from_utf8_lossy(&retrieved_data[..20.min(retrieved_data.len())])
    );

    assert_eq!(
        retrieved_data.len(),
        original_content.len(),
        "Data length mismatch: got {}, expected {}",
        retrieved_data.len(),
        original_content.len()
    );

    assert_eq!(
        retrieved_data.as_slice(),
        original_content,
        "AWS chunked decoded data mismatch"
    );
}

#[tokio::test]
async fn test_aws_chunked_encoding_large_file() {
    let (storage, _temp) = create_test_storage().await;
    let state = create_test_state(storage, _temp.path()).await;

    // Create bucket
    let app = create_router(state.clone());
    app.oneshot(
        Request::builder()
            .method("PUT")
            .uri("/test-bucket")
            .body(Body::empty())
            .unwrap(),
    )
    .await
    .unwrap();

    // Create 5000 bytes of original content (above 4KB inline threshold)
    let original_content: Vec<u8> = (0..5000).map(|i| (i % 256) as u8).collect();

    // Encode it in AWS chunked format
    // 5000 in hex is "1388"
    let mut aws_chunked_body = Vec::new();
    aws_chunked_body.extend_from_slice(b"1388\r\n");
    aws_chunked_body.extend_from_slice(&original_content);
    aws_chunked_body.extend_from_slice(b"\r\n0\r\n\r\n");

    // Put object with aws-chunked encoding
    let app = create_router(state.clone());
    let response = app
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/test-bucket/large-chunked.bin")
                .header("content-type", "application/octet-stream")
                .header("content-encoding", "aws-chunked")
                .body(Body::from(aws_chunked_body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Get object
    let app = create_router(state);
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/test-bucket/large-chunked.bin")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let retrieved_data = body_to_bytes(response.into_body()).await;

    // First byte should be 0, not '1' (from "1388")
    assert_eq!(
        retrieved_data[0],
        0,
        "First byte should be 0, not chunk header. Got: {:?}",
        &retrieved_data[..10]
    );

    assert_eq!(retrieved_data.len(), original_content.len());
    assert_eq!(retrieved_data, original_content);
}

// ============================================================================
// Multipart Upload — Disk Storage Tests
// ============================================================================

/// Helper: runs a full multipart upload flow. Returns (upload_id, etags, original_data).
async fn do_multipart_upload(
    state: &AppState,
    key: &str,
    part_size: usize,
    num_parts: usize,
) -> (String, Vec<(usize, String)>, Vec<u8>) {
    let total = part_size * num_parts;
    let data: Vec<u8> = (0..total).map(|i| (i % 256) as u8).collect();

    // Initiate
    let app = create_router(state.clone());
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/test-bucket/{}?uploads", key))
                .header("content-type", "application/octet-stream")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = body_to_string(resp.into_body()).await;
    let upload_id =
        body[body.find("<UploadId>").unwrap() + 10..body.find("</UploadId>").unwrap()].to_string();

    // Upload parts
    let mut etags = Vec::new();
    for pn in 1..=num_parts {
        let start = (pn - 1) * part_size;
        let end = pn * part_size;

        let app = create_router(state.clone());
        let resp = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(format!(
                        "/test-bucket/{}?uploadId={}&partNumber={}",
                        key, upload_id, pn
                    ))
                    .body(Body::from(data[start..end].to_vec()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let etag = resp.headers().get("etag").unwrap().to_str().unwrap().to_string();
        etags.push((pn, etag));
    }

    (upload_id, etags, data)
}

/// Helper: sends CompleteMultipartUpload.
async fn complete_multipart(
    state: &AppState,
    key: &str,
    upload_id: &str,
    etags: &[(usize, String)],
) -> StatusCode {
    let parts_xml: String = etags
        .iter()
        .map(|(pn, etag)| {
            format!(
                "  <Part><PartNumber>{}</PartNumber><ETag>{}</ETag></Part>",
                pn, etag
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    let xml = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<CompleteMultipartUpload>\n{}\n</CompleteMultipartUpload>",
        parts_xml
    );

    let app = create_router(state.clone());
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/test-bucket/{}?uploadId={}", key, upload_id))
                .header("content-type", "application/xml")
                .body(Body::from(xml))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    // Must consume the full body to wait for streaming assembly to complete
    let _ = body_to_bytes(resp.into_body()).await;
    status
}

#[tokio::test]
async fn test_multipart_disk_storage() {
    let (storage, _temp) = create_test_storage().await;
    let state = create_test_state(storage, _temp.path()).await;

    // Create bucket
    let app = create_router(state.clone());
    app.oneshot(
        Request::builder()
            .method("PUT")
            .uri("/test-bucket")
            .body(Body::empty())
            .unwrap(),
    )
    .await
    .unwrap();

    // Upload parts and verify they are stored durably
    let part_size = 6 * 1024 * 1024; // 6MB
    let (upload_id, etags, _data) =
        do_multipart_upload(&state, "disk-test.bin", part_size, 2).await;

    // Parts should be listed via ListParts API (stored in fjall, data in volumes)
    let app = create_router(state.clone());
    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/test-bucket/disk-test.bin?uploadId={}", upload_id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = body_to_string(resp.into_body()).await;
    // Verify both parts are present in the ListParts response
    assert!(
        body.contains("<PartNumber>1</PartNumber>"),
        "Part 1 should be listed"
    );
    assert!(
        body.contains("<PartNumber>2</PartNumber>"),
        "Part 2 should be listed"
    );
    // Verify ETags match
    for (_pn, etag) in &etags {
        let clean_etag = etag.trim_matches('"');
        assert!(
            body.contains(clean_etag),
            "ETag {} should be in ListParts",
            clean_etag
        );
    }
}

#[tokio::test]
async fn test_multipart_temp_files_cleaned_on_complete() {
    let (storage, _temp) = create_test_storage().await;
    let state = create_test_state(storage, _temp.path()).await;

    let app = create_router(state.clone());
    app.oneshot(
        Request::builder()
            .method("PUT")
            .uri("/test-bucket")
            .body(Body::empty())
            .unwrap(),
    )
    .await
    .unwrap();

    let part_size = 6 * 1024 * 1024;
    let (upload_id, etags, _data) =
        do_multipart_upload(&state, "cleanup-test.bin", part_size, 2).await;

    // Complete the upload
    let status = complete_multipart(&state, "cleanup-test.bin", &upload_id, &etags).await;
    assert_eq!(status, StatusCode::OK);

    // Temp files should be cleaned up
    let tmp_dir = _temp.path().join("multipart_tmp");
    let remaining: Vec<_> = std::fs::read_dir(&tmp_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_str().is_some_and(|n| n.starts_with(&upload_id)))
        .collect();
    assert_eq!(
        remaining.len(),
        0,
        "Temp files should be removed after CompleteMultipartUpload"
    );
}

#[tokio::test]
async fn test_multipart_temp_files_cleaned_on_abort() {
    let (storage, _temp) = create_test_storage().await;
    let state = create_test_state(storage, _temp.path()).await;

    let app = create_router(state.clone());
    app.oneshot(
        Request::builder()
            .method("PUT")
            .uri("/test-bucket")
            .body(Body::empty())
            .unwrap(),
    )
    .await
    .unwrap();

    let part_size = 6 * 1024 * 1024;
    let (upload_id, _etags, _data) =
        do_multipart_upload(&state, "abort-test.bin", part_size, 2).await;

    // Abort the upload
    let app = create_router(state.clone());
    let resp = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!(
                    "/test-bucket/abort-test.bin?uploadId={}",
                    upload_id
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    // Temp files should be cleaned up
    let tmp_dir = _temp.path().join("multipart_tmp");
    let remaining: Vec<_> = std::fs::read_dir(&tmp_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_str().is_some_and(|n| n.starts_with(&upload_id)))
        .collect();
    assert_eq!(
        remaining.len(),
        0,
        "Temp files should be removed after AbortMultipartUpload"
    );
}

#[tokio::test]
async fn test_multipart_streaming_assembly() {
    let (storage, _temp) = create_test_storage().await;
    let state = create_test_state(storage, _temp.path()).await;

    let app = create_router(state.clone());
    app.oneshot(
        Request::builder()
            .method("PUT")
            .uri("/test-bucket")
            .body(Body::empty())
            .unwrap(),
    )
    .await
    .unwrap();

    let part_size = 6 * 1024 * 1024; // 6MB per part
    let (upload_id, etags, original_data) =
        do_multipart_upload(&state, "stream-assembly.bin", part_size, 3).await;

    // Complete
    let status = complete_multipart(&state, "stream-assembly.bin", &upload_id, &etags).await;
    assert_eq!(status, StatusCode::OK);

    // Download and verify data integrity
    let app = create_router(state.clone());
    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/test-bucket/stream-assembly.bin")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let retrieved = body_to_bytes(resp.into_body()).await;
    assert_eq!(retrieved.len(), original_data.len());
    assert_eq!(retrieved, original_data);
}

#[tokio::test]
async fn test_multipart_s3_etag_format() {
    let (storage, _temp) = create_test_storage().await;
    let state = create_test_state(storage, _temp.path()).await;

    let app = create_router(state.clone());
    app.oneshot(
        Request::builder()
            .method("PUT")
            .uri("/test-bucket")
            .body(Body::empty())
            .unwrap(),
    )
    .await
    .unwrap();

    let part_size = 6 * 1024 * 1024;
    let (upload_id, etags, _data) =
        do_multipart_upload(&state, "etag-test.bin", part_size, 2).await;

    let status = complete_multipart(&state, "etag-test.bin", &upload_id, &etags).await;
    assert_eq!(status, StatusCode::OK);

    // HEAD to get the final ETag
    let app = create_router(state.clone());
    let resp = app
        .oneshot(
            Request::builder()
                .method("HEAD")
                .uri("/test-bucket/etag-test.bin")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let etag = resp.headers().get("etag").unwrap().to_str().unwrap().trim_matches('"');

    // S3 multipart ETag format: hex_md5-N
    assert!(
        etag.contains('-'),
        "Multipart ETag should contain '-': got {}",
        etag
    );
    let parts: Vec<&str> = etag.split('-').collect();
    assert_eq!(parts.len(), 2, "ETag should have exactly one '-'");
    assert_eq!(parts[0].len(), 32, "Hash portion should be 32 hex chars");
    assert_eq!(
        parts[1], "2",
        "Part count should match number of uploaded parts"
    );
}

#[tokio::test]
async fn test_multipart_ttl_expiration() {
    use s4_api::multipart_store::DiskPartStore;
    use std::time::Duration;

    let tmp = TempDir::new().unwrap();
    let store = DiskPartStore::with_defaults(tmp.path()).await.unwrap();

    // Store a part
    store.store_part("old-upload", 1, b"data").await.unwrap();
    assert!(store.upload_exists("old-upload").await);

    // With a zero TTL, the upload should be considered expired immediately
    // (created_at was set to Instant::now(), and any elapsed time > Duration::ZERO)
    tokio::time::sleep(Duration::from_millis(10)).await;
    let cleaned = store.cleanup_expired(Duration::ZERO).await.unwrap();
    assert_eq!(cleaned, 1);

    assert!(!store.upload_exists("old-upload").await);
}

#[tokio::test]
async fn test_multipart_startup_cleanup() {
    use s4_api::multipart_store::DiskPartStore;

    let tmp = TempDir::new().unwrap();

    // Create orphaned temp files (simulating crash leftovers)
    let mp_dir = tmp.path().join("multipart_tmp");
    std::fs::create_dir_all(&mp_dir).unwrap();
    std::fs::write(mp_dir.join("crashed_upload_00001.part"), b"orphan1").unwrap();
    std::fs::write(mp_dir.join("crashed_upload_00002.part"), b"orphan2").unwrap();
    std::fs::write(mp_dir.join("other_upload_00001.part"), b"orphan3").unwrap();

    // Create a fresh DiskPartStore (simulating server restart)
    let store = DiskPartStore::with_defaults(tmp.path()).await.unwrap();

    // Cleanup orphaned files
    let cleaned = store.cleanup_orphaned().await.unwrap();
    assert_eq!(cleaned, 3);

    // No .part files should remain
    let remaining: Vec<_> = std::fs::read_dir(&mp_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "part"))
        .collect();
    assert!(remaining.is_empty());
}

// -- Native composite multipart integration tests ----------------------------

#[tokio::test]
async fn test_multipart_list_parts_api() {
    let (storage, _temp) = create_test_storage().await;
    let state = create_test_state(storage, _temp.path()).await;

    let app = create_router(state.clone());
    app.oneshot(
        Request::builder()
            .method("PUT")
            .uri("/test-bucket")
            .body(Body::empty())
            .unwrap(),
    )
    .await
    .unwrap();

    let part_size = 6 * 1024 * 1024;
    let (upload_id, etags, _data) =
        do_multipart_upload(&state, "list-parts.bin", part_size, 3).await;

    // ListParts
    let app = create_router(state.clone());
    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!(
                    "/test-bucket/list-parts.bin?uploadId={}",
                    upload_id
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = body_to_string(resp.into_body()).await;
    assert!(body.contains("<PartNumber>1</PartNumber>"));
    assert!(body.contains("<PartNumber>2</PartNumber>"));
    assert!(body.contains("<PartNumber>3</PartNumber>"));
    assert!(body.contains(&format!("<UploadId>{}</UploadId>", upload_id)));

    // Verify ETags
    for (_pn, etag) in &etags {
        let clean_etag = etag.trim_matches('"');
        assert!(
            body.contains(clean_etag),
            "ETag {} should appear in ListParts",
            clean_etag
        );
    }
}

#[tokio::test]
async fn test_multipart_list_parts_pagination() {
    let (storage, _temp) = create_test_storage().await;
    let state = create_test_state(storage, _temp.path()).await;

    let app = create_router(state.clone());
    app.oneshot(
        Request::builder()
            .method("PUT")
            .uri("/test-bucket")
            .body(Body::empty())
            .unwrap(),
    )
    .await
    .unwrap();

    let part_size = 6 * 1024 * 1024;
    let (upload_id, _etags, _data) =
        do_multipart_upload(&state, "paginate.bin", part_size, 3).await;

    // ListParts with max-parts=2
    let app = create_router(state.clone());
    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!(
                    "/test-bucket/paginate.bin?uploadId={}&max-parts=2",
                    upload_id
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = body_to_string(resp.into_body()).await;
    assert!(body.contains("<IsTruncated>true</IsTruncated>"));
    assert!(body.contains("<MaxParts>2</MaxParts>"));
    assert!(body.contains("<PartNumber>1</PartNumber>"));
    assert!(body.contains("<PartNumber>2</PartNumber>"));
    // Part 3 should NOT be in this page
    assert!(!body.contains("<PartNumber>3</PartNumber>"));
}

#[tokio::test]
async fn test_multipart_abort_prevents_access() {
    let (storage, _temp) = create_test_storage().await;
    let state = create_test_state(storage, _temp.path()).await;

    let app = create_router(state.clone());
    app.oneshot(
        Request::builder()
            .method("PUT")
            .uri("/test-bucket")
            .body(Body::empty())
            .unwrap(),
    )
    .await
    .unwrap();

    let part_size = 6 * 1024 * 1024;
    let (upload_id, _etags, _data) =
        do_multipart_upload(&state, "abort-test.bin", part_size, 2).await;

    // Abort the upload
    let app = create_router(state.clone());
    let resp = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!(
                    "/test-bucket/abort-test.bin?uploadId={}",
                    upload_id
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    // ListParts should fail (session gone)
    let app = create_router(state.clone());
    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!(
                    "/test-bucket/abort-test.bin?uploadId={}",
                    upload_id
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);

    // UploadPart should fail (session gone)
    let app = create_router(state.clone());
    let resp = app
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!(
                    "/test-bucket/abort-test.bin?uploadId={}&partNumber=1",
                    upload_id
                ))
                .body(Body::from(vec![0u8; 1024]))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_multipart_delete_composite_object() {
    let (storage, _temp) = create_test_storage().await;
    let state = create_test_state(storage, _temp.path()).await;

    let app = create_router(state.clone());
    app.oneshot(
        Request::builder()
            .method("PUT")
            .uri("/test-bucket")
            .body(Body::empty())
            .unwrap(),
    )
    .await
    .unwrap();

    let part_size = 6 * 1024 * 1024;
    let (upload_id, etags, _data) =
        do_multipart_upload(&state, "delete-composite.bin", part_size, 2).await;

    let status = complete_multipart(&state, "delete-composite.bin", &upload_id, &etags).await;
    assert_eq!(status, StatusCode::OK);

    // Verify it exists
    let app = create_router(state.clone());
    let resp = app
        .oneshot(
            Request::builder()
                .method("HEAD")
                .uri("/test-bucket/delete-composite.bin")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Delete the composite object
    let app = create_router(state.clone());
    let resp = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/test-bucket/delete-composite.bin")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    // Verify it's gone
    let app = create_router(state.clone());
    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/test-bucket/delete-composite.bin")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_multipart_head_composite_object() {
    let (storage, _temp) = create_test_storage().await;
    let state = create_test_state(storage, _temp.path()).await;

    let app = create_router(state.clone());
    app.oneshot(
        Request::builder()
            .method("PUT")
            .uri("/test-bucket")
            .body(Body::empty())
            .unwrap(),
    )
    .await
    .unwrap();

    let part_size = 6 * 1024 * 1024;
    let total_size = part_size * 2;
    let (upload_id, etags, _data) =
        do_multipart_upload(&state, "head-composite.bin", part_size, 2).await;

    let status = complete_multipart(&state, "head-composite.bin", &upload_id, &etags).await;
    assert_eq!(status, StatusCode::OK);

    // HEAD
    let app = create_router(state.clone());
    let resp = app
        .oneshot(
            Request::builder()
                .method("HEAD")
                .uri("/test-bucket/head-composite.bin")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Verify Content-Length matches total size
    let content_length: usize =
        resp.headers().get("content-length").unwrap().to_str().unwrap().parse().unwrap();
    assert_eq!(content_length, total_size);

    // Verify Content-Type
    let content_type = resp.headers().get("content-type").unwrap().to_str().unwrap();
    assert_eq!(content_type, "application/octet-stream");

    // Verify ETag has multipart format
    let etag = resp.headers().get("etag").unwrap().to_str().unwrap().trim_matches('"');
    assert!(etag.contains('-'), "ETag should be multipart format");
}

#[tokio::test]
async fn test_multipart_complete_invalid_etag() {
    let (storage, _temp) = create_test_storage().await;
    let state = create_test_state(storage, _temp.path()).await;

    let app = create_router(state.clone());
    app.oneshot(
        Request::builder()
            .method("PUT")
            .uri("/test-bucket")
            .body(Body::empty())
            .unwrap(),
    )
    .await
    .unwrap();

    let part_size = 6 * 1024 * 1024;
    let (upload_id, _etags, _data) =
        do_multipart_upload(&state, "bad-etag.bin", part_size, 1).await;

    // Complete with wrong ETag
    let complete_xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<CompleteMultipartUpload>
  <Part><PartNumber>1</PartNumber><ETag>"0000000000000000000000000000dead"</ETag></Part>
</CompleteMultipartUpload>"#
        .to_string();

    let app = create_router(state.clone());
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/test-bucket/bad-etag.bin?uploadId={}", upload_id))
                .body(Body::from(complete_xml))
                .unwrap(),
        )
        .await
        .unwrap();

    // Should fail with 400 (ETag mismatch)
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_multipart_complete_missing_part() {
    let (storage, _temp) = create_test_storage().await;
    let state = create_test_state(storage, _temp.path()).await;

    let app = create_router(state.clone());
    app.oneshot(
        Request::builder()
            .method("PUT")
            .uri("/test-bucket")
            .body(Body::empty())
            .unwrap(),
    )
    .await
    .unwrap();

    let part_size = 6 * 1024 * 1024;
    let (upload_id, etags, _data) =
        do_multipart_upload(&state, "missing-part.bin", part_size, 1).await;

    // Complete referencing part 2 (which was never uploaded)
    let complete_xml = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<CompleteMultipartUpload>
  <Part><PartNumber>1</PartNumber><ETag>{}</ETag></Part>
  <Part><PartNumber>2</PartNumber><ETag>"deadbeef"</ETag></Part>
</CompleteMultipartUpload>"#,
        etags[0].1
    );

    let app = create_router(state.clone());
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!(
                    "/test-bucket/missing-part.bin?uploadId={}",
                    upload_id
                ))
                .body(Body::from(complete_xml))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}
