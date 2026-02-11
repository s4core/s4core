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
    let metadata_path = temp_dir.path().join("metadata.redb");

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
fn create_test_state(storage: BitcaskStorageEngine) -> AppState {
    AppState::new(
        storage,
        "test-access-key".to_string(),
        "test-secret-key".to_string(),
    )
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
    let app = create_router(create_test_state(storage));

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
    let app = create_router(create_test_state(storage));

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
    let state = create_test_state(storage);

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
    let app = create_router(create_test_state(storage));

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
    let state = create_test_state(storage);

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
    let state = create_test_state(storage);

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
    let app = create_router(create_test_state(storage));

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
    let state = create_test_state(storage);

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
    let app = create_router(create_test_state(storage));

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
    let state = create_test_state(storage);

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
    let app = create_router(create_test_state(storage));

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
    let state = create_test_state(storage);

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
    let state = create_test_state(storage);

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
    let state = create_test_state(storage);

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
    let state = create_test_state(storage);

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
    let state = create_test_state(storage);

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
    let state = create_test_state(storage);

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
    let state = create_test_state(storage);

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
    let state = create_test_state(storage);

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
    let state = create_test_state(storage);

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
    let state = create_test_state(storage);

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
    let state = create_test_state(storage);

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
    let state = create_test_state(storage);

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

    // Create data larger than 4KB (10KB total, split into parts)
    let original_data: Vec<u8> = (0..10240).map(|i| (i % 256) as u8).collect();

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

    // Upload parts (2 parts of 5KB each)
    let mut etags = Vec::new();
    for part_num in 1..=2 {
        let start = (part_num - 1) * 5120;
        let end = part_num * 5120;
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
    let state = create_test_state(storage);

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
    let state = create_test_state(storage);

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
    let state = create_test_state(storage);

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
    let state = create_test_state(storage);

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
