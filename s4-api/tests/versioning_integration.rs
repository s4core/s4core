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

//! Integration tests for S3-compatible object versioning.
//!
//! These tests verify the full versioning workflow through the HTTP API.
//!
//! Note: Authentication is disabled for these tests via S4_DISABLE_AUTH=1
//! to focus on testing the versioning functionality.

use axum::{
    body::Body,
    http::{header, Method, Request, StatusCode},
};
use s4_api::server::{create_router, AppState};
use s4_core::storage::BitcaskStorageEngine;
use std::sync::Once;
use tempfile::TempDir;
use tower::ServiceExt;

/// Initialize test environment once for all tests.
static INIT: Once = Once::new();

fn init_test_env() {
    INIT.call_once(|| {
        std::env::set_var("S4_DISABLE_AUTH", "1");
    });
}

/// Creates a test app with a fresh storage instance.
async fn create_test_app() -> (axum::Router, TempDir) {
    init_test_env();

    let temp_dir = TempDir::new().unwrap();
    let volumes_dir = temp_dir.path().join("volumes");
    let metadata_path = temp_dir.path().join("metadata.redb");

    std::fs::create_dir_all(&volumes_dir).unwrap();

    let engine = BitcaskStorageEngine::new(&volumes_dir, &metadata_path, 1024 * 1024, 4096, false)
        .await
        .unwrap();

    let state = AppState::new(engine, "test-key".to_string(), "test-secret".to_string());
    let app = create_router(state);

    (app, temp_dir)
}

/// Creates a bucket for testing.
async fn create_bucket(app: &axum::Router, bucket: &str) {
    let request = Request::builder()
        .method(Method::PUT)
        .uri(format!("/{}", bucket))
        .body(Body::empty())
        .unwrap();

    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

/// Enables versioning on a bucket.
async fn enable_versioning(app: &axum::Router, bucket: &str) {
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<VersioningConfiguration>
  <Status>Enabled</Status>
</VersioningConfiguration>"#;

    let request = Request::builder()
        .method(Method::PUT)
        .uri(format!("/{}?versioning", bucket))
        .header(header::CONTENT_TYPE, "application/xml")
        .body(Body::from(xml))
        .unwrap();

    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

/// Suspends versioning on a bucket.
async fn suspend_versioning(app: &axum::Router, bucket: &str) {
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<VersioningConfiguration>
  <Status>Suspended</Status>
</VersioningConfiguration>"#;

    let request = Request::builder()
        .method(Method::PUT)
        .uri(format!("/{}?versioning", bucket))
        .header(header::CONTENT_TYPE, "application/xml")
        .body(Body::from(xml))
        .unwrap();

    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

/// Gets the x-amz-version-id header from a response.
fn get_version_id(response: &axum::response::Response) -> Option<String> {
    response
        .headers()
        .get("x-amz-version-id")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
}

/// Checks if response has delete marker header.
fn has_delete_marker(response: &axum::response::Response) -> bool {
    response
        .headers()
        .get("x-amz-delete-marker")
        .and_then(|v| v.to_str().ok())
        .map(|s| s == "true")
        .unwrap_or(false)
}

// ============================================================================
// Tests
// ============================================================================

#[tokio::test]
async fn test_unversioned_bucket_behavior() {
    let (app, _temp) = create_test_app().await;
    create_bucket(&app, "test-bucket").await;

    // PUT object - should NOT return version ID
    let request = Request::builder()
        .method(Method::PUT)
        .uri("/test-bucket/file.txt")
        .header(header::CONTENT_TYPE, "text/plain")
        .body(Body::from("version 1"))
        .unwrap();

    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        get_version_id(&response).is_none(),
        "Unversioned bucket should not return version ID"
    );

    // Overwrite - should replace the object
    let request = Request::builder()
        .method(Method::PUT)
        .uri("/test-bucket/file.txt")
        .header(header::CONTENT_TYPE, "text/plain")
        .body(Body::from("version 2"))
        .unwrap();

    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // GET - should return latest content
    let request = Request::builder()
        .method(Method::GET)
        .uri("/test-bucket/file.txt")
        .body(Body::empty())
        .unwrap();

    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(body.as_ref(), b"version 2");

    // DELETE - should permanently remove
    let request = Request::builder()
        .method(Method::DELETE)
        .uri("/test-bucket/file.txt")
        .body(Body::empty())
        .unwrap();

    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert!(
        !has_delete_marker(&response),
        "Unversioned delete should not create delete marker"
    );

    // GET after delete - should return 404
    let request = Request::builder()
        .method(Method::GET)
        .uri("/test-bucket/file.txt")
        .body(Body::empty())
        .unwrap();

    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_enable_versioning_returns_version_ids() {
    let (app, _temp) = create_test_app().await;
    create_bucket(&app, "versioned-bucket").await;
    enable_versioning(&app, "versioned-bucket").await;

    // PUT object - should return version ID
    let request = Request::builder()
        .method(Method::PUT)
        .uri("/versioned-bucket/file.txt")
        .header(header::CONTENT_TYPE, "text/plain")
        .body(Body::from("version 1"))
        .unwrap();

    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let version1 = get_version_id(&response);
    assert!(
        version1.is_some(),
        "Versioned bucket should return version ID"
    );
    let version1 = version1.unwrap();
    assert!(!version1.is_empty());

    // PUT again - should return DIFFERENT version ID
    let request = Request::builder()
        .method(Method::PUT)
        .uri("/versioned-bucket/file.txt")
        .header(header::CONTENT_TYPE, "text/plain")
        .body(Body::from("version 2"))
        .unwrap();

    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let version2 = get_version_id(&response);
    assert!(version2.is_some());
    let version2 = version2.unwrap();

    assert_ne!(version1, version2, "Each PUT should create a new version");
}

#[tokio::test]
async fn test_get_specific_version() {
    let (app, _temp) = create_test_app().await;
    create_bucket(&app, "versioned-bucket").await;
    enable_versioning(&app, "versioned-bucket").await;

    // Create version 1
    let request = Request::builder()
        .method(Method::PUT)
        .uri("/versioned-bucket/file.txt")
        .body(Body::from("content v1"))
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let version1 = get_version_id(&response).unwrap();

    // Create version 2
    let request = Request::builder()
        .method(Method::PUT)
        .uri("/versioned-bucket/file.txt")
        .body(Body::from("content v2"))
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let version2 = get_version_id(&response).unwrap();

    // GET without versionId - should return v2 (latest)
    let request = Request::builder()
        .method(Method::GET)
        .uri("/versioned-bucket/file.txt")
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(body.as_ref(), b"content v2");

    // GET with versionId=v1 - should return v1
    let request = Request::builder()
        .method(Method::GET)
        .uri(format!("/versioned-bucket/file.txt?versionId={}", version1))
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(body.as_ref(), b"content v1");

    // GET with versionId=v2 - should return v2
    let request = Request::builder()
        .method(Method::GET)
        .uri(format!("/versioned-bucket/file.txt?versionId={}", version2))
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(body.as_ref(), b"content v2");
}

#[tokio::test]
async fn test_delete_creates_marker() {
    let (app, _temp) = create_test_app().await;
    create_bucket(&app, "versioned-bucket").await;
    enable_versioning(&app, "versioned-bucket").await;

    // Create object
    let request = Request::builder()
        .method(Method::PUT)
        .uri("/versioned-bucket/file.txt")
        .body(Body::from("content"))
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let original_version = get_version_id(&response).unwrap();

    // DELETE without versionId - should create delete marker
    let request = Request::builder()
        .method(Method::DELETE)
        .uri("/versioned-bucket/file.txt")
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert!(
        has_delete_marker(&response),
        "Versioned delete should create delete marker"
    );

    let delete_marker_version = get_version_id(&response);
    assert!(delete_marker_version.is_some());

    // GET without versionId - should return 404
    let request = Request::builder()
        .method(Method::GET)
        .uri("/versioned-bucket/file.txt")
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    // GET with original version - should still work!
    let request = Request::builder()
        .method(Method::GET)
        .uri(format!(
            "/versioned-bucket/file.txt?versionId={}",
            original_version
        ))
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "Old version should still be accessible after delete"
    );

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(body.as_ref(), b"content");
}

#[tokio::test]
async fn test_delete_marker_removal_restores_object() {
    let (app, _temp) = create_test_app().await;
    create_bucket(&app, "versioned-bucket").await;
    enable_versioning(&app, "versioned-bucket").await;

    // Create object
    let request = Request::builder()
        .method(Method::PUT)
        .uri("/versioned-bucket/file.txt")
        .body(Body::from("content"))
        .unwrap();
    app.clone().oneshot(request).await.unwrap();

    // Delete (creates marker)
    let request = Request::builder()
        .method(Method::DELETE)
        .uri("/versioned-bucket/file.txt")
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let delete_marker_version = get_version_id(&response).unwrap();

    // Verify object is "deleted"
    let request = Request::builder()
        .method(Method::GET)
        .uri("/versioned-bucket/file.txt")
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    // DELETE the delete marker specifically
    let request = Request::builder()
        .method(Method::DELETE)
        .uri(format!(
            "/versioned-bucket/file.txt?versionId={}",
            delete_marker_version
        ))
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    // Object should now be accessible again!
    let request = Request::builder()
        .method(Method::GET)
        .uri("/versioned-bucket/file.txt")
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "Object should be restored after delete marker removal"
    );

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(body.as_ref(), b"content");
}

#[tokio::test]
async fn test_suspended_versioning_uses_null_version() {
    let (app, _temp) = create_test_app().await;
    create_bucket(&app, "suspended-bucket").await;
    enable_versioning(&app, "suspended-bucket").await;

    // Create versioned object
    let request = Request::builder()
        .method(Method::PUT)
        .uri("/suspended-bucket/file.txt")
        .body(Body::from("versioned content"))
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let versioned_id = get_version_id(&response).unwrap();
    assert_ne!(versioned_id, "null");

    // Suspend versioning
    suspend_versioning(&app, "suspended-bucket").await;

    // PUT now - should get "null" version
    let request = Request::builder()
        .method(Method::PUT)
        .uri("/suspended-bucket/file.txt")
        .body(Body::from("suspended content"))
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let suspended_id = get_version_id(&response).unwrap();
    assert_eq!(
        suspended_id, "null",
        "Suspended versioning should use 'null' version ID"
    );

    // Old version should still be accessible
    let request = Request::builder()
        .method(Method::GET)
        .uri(format!(
            "/suspended-bucket/file.txt?versionId={}",
            versioned_id
        ))
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(body.as_ref(), b"versioned content");
}

#[tokio::test]
async fn test_list_object_versions() {
    let (app, _temp) = create_test_app().await;
    create_bucket(&app, "versioned-bucket").await;
    enable_versioning(&app, "versioned-bucket").await;

    // Create multiple versions
    for i in 1..=3 {
        let request = Request::builder()
            .method(Method::PUT)
            .uri("/versioned-bucket/file.txt")
            .body(Body::from(format!("version {}", i)))
            .unwrap();
        app.clone().oneshot(request).await.unwrap();
    }

    // Delete (creates delete marker)
    let request = Request::builder()
        .method(Method::DELETE)
        .uri("/versioned-bucket/file.txt")
        .body(Body::empty())
        .unwrap();
    app.clone().oneshot(request).await.unwrap();

    // List versions
    let request = Request::builder()
        .method(Method::GET)
        .uri("/versioned-bucket?versions")
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body_str = String::from_utf8_lossy(&body);

    // Should contain ListVersionsResult
    assert!(body_str.contains("<ListVersionsResult"));
    // Should have 3 Version elements
    assert_eq!(
        body_str.matches("<Version>").count(),
        3,
        "Should have 3 version entries"
    );
    // Should have 1 DeleteMarker element
    assert_eq!(
        body_str.matches("<DeleteMarker>").count(),
        1,
        "Should have 1 delete marker"
    );
    // Should have key
    assert!(body_str.contains("<Key>file.txt</Key>"));
}

#[tokio::test]
async fn test_versioning_cannot_be_disabled() {
    let (app, _temp) = create_test_app().await;
    create_bucket(&app, "test-bucket").await;

    // Enable versioning
    enable_versioning(&app, "test-bucket").await;

    // Try to disable versioning (set to empty/unversioned)
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<VersioningConfiguration>
</VersioningConfiguration>"#;

    let request = Request::builder()
        .method(Method::PUT)
        .uri("/test-bucket?versioning")
        .header(header::CONTENT_TYPE, "application/xml")
        .body(Body::from(xml))
        .unwrap();

    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(
        response.status(),
        StatusCode::BAD_REQUEST,
        "Should not be able to disable versioning once enabled"
    );

    // Suspending should still work
    suspend_versioning(&app, "test-bucket").await;

    // Verify it's suspended
    let request = Request::builder()
        .method(Method::GET)
        .uri("/test-bucket?versioning")
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body_str = String::from_utf8_lossy(&body);
    assert!(body_str.contains("<Status>Suspended</Status>"));

    // Trying to disable again should still fail
    let request = Request::builder()
        .method(Method::PUT)
        .uri("/test-bucket?versioning")
        .header(header::CONTENT_TYPE, "application/xml")
        .body(Body::from(xml))
        .unwrap();

    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(
        response.status(),
        StatusCode::BAD_REQUEST,
        "Should not be able to disable versioning even from suspended state"
    );
}

#[tokio::test]
async fn test_head_object_version() {
    let (app, _temp) = create_test_app().await;
    create_bucket(&app, "versioned-bucket").await;
    enable_versioning(&app, "versioned-bucket").await;

    // Create two versions
    let request = Request::builder()
        .method(Method::PUT)
        .uri("/versioned-bucket/file.txt")
        .header(header::CONTENT_TYPE, "text/plain")
        .body(Body::from("v1 content"))
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let version1 = get_version_id(&response).unwrap();

    let request = Request::builder()
        .method(Method::PUT)
        .uri("/versioned-bucket/file.txt")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from("v2 content"))
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let version2 = get_version_id(&response).unwrap();

    // HEAD without version - should return v2 metadata
    let request = Request::builder()
        .method(Method::HEAD)
        .uri("/versioned-bucket/file.txt")
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(header::CONTENT_TYPE).unwrap(),
        "application/json"
    );
    assert_eq!(get_version_id(&response).unwrap(), version2);

    // HEAD with version1 - should return v1 metadata
    let request = Request::builder()
        .method(Method::HEAD)
        .uri(format!("/versioned-bucket/file.txt?versionId={}", version1))
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(header::CONTENT_TYPE).unwrap(),
        "text/plain"
    );
    assert_eq!(get_version_id(&response).unwrap(), version1);
}

#[tokio::test]
async fn test_delete_specific_version_permanently() {
    let (app, _temp) = create_test_app().await;
    create_bucket(&app, "versioned-bucket").await;
    enable_versioning(&app, "versioned-bucket").await;

    // Create version
    let request = Request::builder()
        .method(Method::PUT)
        .uri("/versioned-bucket/file.txt")
        .body(Body::from("content"))
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let version_id = get_version_id(&response).unwrap();

    // DELETE with specific versionId - permanently removes
    let request = Request::builder()
        .method(Method::DELETE)
        .uri(format!(
            "/versioned-bucket/file.txt?versionId={}",
            version_id
        ))
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert!(
        !has_delete_marker(&response),
        "Deleting specific version should not create delete marker"
    );

    // GET that version - should return 404 (version not found)
    let request = Request::builder()
        .method(Method::GET)
        .uri(format!(
            "/versioned-bucket/file.txt?versionId={}",
            version_id
        ))
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(
        response.status(),
        StatusCode::NOT_FOUND,
        "Permanently deleted version should not be accessible"
    );
}
