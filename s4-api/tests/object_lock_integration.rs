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

//! Integration tests for S3-compatible Object Lock.
//!
//! These tests verify the full Object Lock workflow through the HTTP API.
//!
//! Note: Authentication is disabled for these tests via S4_DISABLE_AUTH=1
//! to focus on testing the Object Lock functionality.

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

/// Enables Object Lock on a bucket.
async fn enable_object_lock(app: &axum::Router, bucket: &str, default_retention: Option<&str>) {
    let xml = if let Some(retention) = default_retention {
        format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<ObjectLockConfiguration>
    <ObjectLockEnabled>Enabled</ObjectLockEnabled>
    <Rule>
        <DefaultRetention>
            {}
        </DefaultRetention>
    </Rule>
</ObjectLockConfiguration>"#,
            retention
        )
    } else {
        r#"<?xml version="1.0" encoding="UTF-8"?>
<ObjectLockConfiguration>
    <ObjectLockEnabled>Enabled</ObjectLockEnabled>
</ObjectLockConfiguration>"#
            .to_string()
    };

    let request = Request::builder()
        .method(Method::PUT)
        .uri(format!("/{}?object-lock", bucket))
        .header(header::CONTENT_TYPE, "application/xml")
        .body(Body::from(xml))
        .unwrap();

    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

/// Puts an object and returns the version ID.
async fn put_object(app: &axum::Router, bucket: &str, key: &str, content: &str) -> String {
    let request = Request::builder()
        .method(Method::PUT)
        .uri(format!("/{}/{}", bucket, key))
        .body(Body::from(content.to_string()))
        .unwrap();

    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Extract version ID from response header
    response
        .headers()
        .get("x-amz-version-id")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("null")
        .to_string()
}

/// Sets retention on a specific object version.
async fn set_retention(
    app: &axum::Router,
    bucket: &str,
    key: &str,
    version_id: &str,
    mode: &str,
    retain_until: &str,
) -> StatusCode {
    let xml = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<Retention>
    <Mode>{}</Mode>
    <RetainUntilDate>{}</RetainUntilDate>
</Retention>"#,
        mode, retain_until
    );

    let request = Request::builder()
        .method(Method::PUT)
        .uri(format!(
            "/{}/{}?retention&versionId={}",
            bucket, key, version_id
        ))
        .header(header::CONTENT_TYPE, "application/xml")
        .body(Body::from(xml))
        .unwrap();

    let response = app.clone().oneshot(request).await.unwrap();
    response.status()
}

/// Sets legal hold on a specific object version.
async fn set_legal_hold(
    app: &axum::Router,
    bucket: &str,
    key: &str,
    version_id: &str,
    status: &str,
) -> StatusCode {
    let xml = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<LegalHold>
    <Status>{}</Status>
</LegalHold>"#,
        status
    );

    let request = Request::builder()
        .method(Method::PUT)
        .uri(format!(
            "/{}/{}?legal-hold&versionId={}",
            bucket, key, version_id
        ))
        .header(header::CONTENT_TYPE, "application/xml")
        .body(Body::from(xml))
        .unwrap();

    let response = app.clone().oneshot(request).await.unwrap();
    response.status()
}

/// Deletes an object and returns the status code.
async fn delete_object(
    app: &axum::Router,
    bucket: &str,
    key: &str,
    version_id: &str,
) -> StatusCode {
    let request = Request::builder()
        .method(Method::DELETE)
        .uri(format!("/{}/{}?versionId={}", bucket, key, version_id))
        .body(Body::empty())
        .unwrap();

    let response = app.clone().oneshot(request).await.unwrap();
    response.status()
}

// ============================================================================
// Tests
// ============================================================================

#[tokio::test]
async fn test_object_lock_requires_versioning() {
    let (app, _temp_dir) = create_test_app().await;
    let bucket = "lock-test-versioning";

    // Create bucket without versioning
    create_bucket(&app, bucket).await;

    // Try to enable Object Lock without versioning - should fail
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<ObjectLockConfiguration>
    <ObjectLockEnabled>Enabled</ObjectLockEnabled>
</ObjectLockConfiguration>"#;

    let request = Request::builder()
        .method(Method::PUT)
        .uri(format!("/{}?object-lock", bucket))
        .header(header::CONTENT_TYPE, "application/xml")
        .body(Body::from(xml))
        .unwrap();

    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(
        response.status(),
        StatusCode::BAD_REQUEST,
        "Should fail to enable Object Lock without versioning"
    );
}

#[tokio::test]
async fn test_delete_locked_object_fails() {
    let (app, _temp_dir) = create_test_app().await;
    let bucket = "lock-test-retention";

    // Setup: Create bucket, enable versioning and Object Lock
    create_bucket(&app, bucket).await;
    enable_versioning(&app, bucket).await;
    enable_object_lock(&app, bucket, None).await;

    // Put an object
    let version_id = put_object(&app, bucket, "locked.txt", "test content").await;

    // Set retention with a future date (1 day from now)
    let future_date = chrono::Utc::now() + chrono::Duration::days(1);
    let retain_until = future_date.format("%Y-%m-%dT%H:%M:%SZ").to_string();

    let status = set_retention(
        &app,
        bucket,
        "locked.txt",
        &version_id,
        "GOVERNANCE",
        &retain_until,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "Should set retention successfully");

    // Try to delete - should fail with AccessDenied (403)
    let delete_status = delete_object(&app, bucket, "locked.txt", &version_id).await;
    assert_eq!(
        delete_status,
        StatusCode::FORBIDDEN,
        "Should deny deletion of object with active retention"
    );
}

#[tokio::test]
async fn test_legal_hold_prevents_deletion() {
    let (app, _temp_dir) = create_test_app().await;
    let bucket = "lock-test-legal-hold";

    // Setup
    create_bucket(&app, bucket).await;
    enable_versioning(&app, bucket).await;
    enable_object_lock(&app, bucket, None).await;

    // Put an object
    let version_id = put_object(&app, bucket, "held.txt", "test content").await;

    // Set legal hold to ON
    let status = set_legal_hold(&app, bucket, "held.txt", &version_id, "ON").await;
    assert_eq!(status, StatusCode::OK, "Should set legal hold successfully");

    // Try to delete - should fail
    let delete_status = delete_object(&app, bucket, "held.txt", &version_id).await;
    assert_eq!(
        delete_status,
        StatusCode::FORBIDDEN,
        "Should deny deletion of object with legal hold ON"
    );
}

#[tokio::test]
async fn test_cannot_modify_compliance_retention() {
    let (app, _temp_dir) = create_test_app().await;
    let bucket = "lock-test-compliance";

    // Setup
    create_bucket(&app, bucket).await;
    enable_versioning(&app, bucket).await;
    enable_object_lock(&app, bucket, None).await;

    // Put an object
    let version_id = put_object(&app, bucket, "compliance.txt", "test content").await;

    // Set COMPLIANCE retention
    let future_date = chrono::Utc::now() + chrono::Duration::days(7);
    let retain_until = future_date.format("%Y-%m-%dT%H:%M:%SZ").to_string();

    let status = set_retention(
        &app,
        bucket,
        "compliance.txt",
        &version_id,
        "COMPLIANCE",
        &retain_until,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "Should set COMPLIANCE retention");

    // Try to modify retention - should fail in Phase 3
    let new_date = chrono::Utc::now() + chrono::Duration::days(14);
    let new_retain_until = new_date.format("%Y-%m-%dT%H:%M:%SZ").to_string();

    let modify_status = set_retention(
        &app,
        bucket,
        "compliance.txt",
        &version_id,
        "COMPLIANCE",
        &new_retain_until,
    )
    .await;
    assert_eq!(
        modify_status,
        StatusCode::BAD_REQUEST,
        "Should deny modification of COMPLIANCE retention in Phase 3"
    );
}

#[tokio::test]
async fn test_default_retention_on_put() {
    let (app, _temp_dir) = create_test_app().await;
    let bucket = "lock-test-default";

    // Setup with default retention
    create_bucket(&app, bucket).await;
    enable_versioning(&app, bucket).await;
    enable_object_lock(&app, bucket, Some("<Mode>GOVERNANCE</Mode><Days>30</Days>")).await;

    // Put an object (should get default retention automatically)
    let version_id = put_object(&app, bucket, "default.txt", "test content").await;

    // Get retention to verify it was applied
    let request = Request::builder()
        .method(Method::GET)
        .uri(format!(
            "/{}/default.txt?retention&versionId={}",
            bucket, version_id
        ))
        .body(Body::empty())
        .unwrap();

    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "Should have retention applied"
    );

    // Parse response body to verify retention exists
    let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body_str = String::from_utf8(body_bytes.to_vec()).unwrap();

    assert!(
        body_str.contains("GOVERNANCE"),
        "Default retention mode should be GOVERNANCE"
    );
    assert!(
        body_str.contains("RetainUntilDate"),
        "Should have retain-until date"
    );
}

#[tokio::test]
async fn test_expired_retention_allows_delete() {
    let (app, _temp_dir) = create_test_app().await;
    let bucket = "lock-test-expired";

    // Setup
    create_bucket(&app, bucket).await;
    enable_versioning(&app, bucket).await;
    enable_object_lock(&app, bucket, None).await;

    // Put an object
    let version_id = put_object(&app, bucket, "expired.txt", "test content").await;

    // Set retention with a very short duration (2 seconds from now)
    let short_future = chrono::Utc::now() + chrono::Duration::seconds(2);
    let retain_until = short_future.format("%Y-%m-%dT%H:%M:%SZ").to_string();

    let status = set_retention(
        &app,
        bucket,
        "expired.txt",
        &version_id,
        "GOVERNANCE",
        &retain_until,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "Should set retention with future date"
    );

    // Verify deletion fails while retention is active
    let delete_before = delete_object(&app, bucket, "expired.txt", &version_id).await;
    assert_eq!(
        delete_before,
        StatusCode::FORBIDDEN,
        "Should deny deletion while retention is active"
    );

    // Wait for retention to expire (3 seconds to be safe)
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    // Try to delete again - should succeed because retention expired
    let delete_status = delete_object(&app, bucket, "expired.txt", &version_id).await;
    assert_eq!(
        delete_status,
        StatusCode::NO_CONTENT,
        "Should allow deletion when retention has expired (SAFETY TEST)"
    );
}

#[tokio::test]
async fn test_legal_hold_removable() {
    let (app, _temp_dir) = create_test_app().await;
    let bucket = "lock-test-removable";

    // Setup
    create_bucket(&app, bucket).await;
    enable_versioning(&app, bucket).await;
    enable_object_lock(&app, bucket, None).await;

    // Put an object
    let version_id = put_object(&app, bucket, "removable.txt", "test content").await;

    // Set legal hold ON
    let status = set_legal_hold(&app, bucket, "removable.txt", &version_id, "ON").await;
    assert_eq!(status, StatusCode::OK, "Should set legal hold ON");

    // Remove legal hold (set to OFF)
    let remove_status = set_legal_hold(&app, bucket, "removable.txt", &version_id, "OFF").await;
    assert_eq!(remove_status, StatusCode::OK, "Should remove legal hold");

    // Now delete should succeed
    let delete_status = delete_object(&app, bucket, "removable.txt", &version_id).await;
    assert_eq!(
        delete_status,
        StatusCode::NO_CONTENT,
        "Should allow deletion after legal hold removed (SAFETY TEST)"
    );
}

#[tokio::test]
async fn test_get_object_lock_configuration() {
    let (app, _temp_dir) = create_test_app().await;
    let bucket = "lock-test-get-config";

    // Setup
    create_bucket(&app, bucket).await;
    enable_versioning(&app, bucket).await;
    enable_object_lock(&app, bucket, Some("<Mode>COMPLIANCE</Mode><Days>90</Days>")).await;

    // Get Object Lock configuration
    let request = Request::builder()
        .method(Method::GET)
        .uri(format!("/{}?object-lock", bucket))
        .body(Body::empty())
        .unwrap();

    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body_str = String::from_utf8(body_bytes.to_vec()).unwrap();

    assert!(body_str.contains("ObjectLockEnabled"));
    assert!(body_str.contains("COMPLIANCE"));
    assert!(body_str.contains("<Days>90</Days>"));
}

#[tokio::test]
async fn test_object_lock_not_found() {
    let (app, _temp_dir) = create_test_app().await;
    let bucket = "lock-test-not-found";

    // Create bucket without Object Lock
    create_bucket(&app, bucket).await;

    // Try to get Object Lock configuration - should return 404
    let request = Request::builder()
        .method(Method::GET)
        .uri(format!("/{}?object-lock", bucket))
        .body(Body::empty())
        .unwrap();

    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(
        response.status(),
        StatusCode::NOT_FOUND,
        "Should return 404 when Object Lock not configured"
    );
}

#[tokio::test]
async fn test_retention_without_version_id_fails() {
    let (app, _temp_dir) = create_test_app().await;
    let bucket = "lock-test-no-version";

    // Setup
    create_bucket(&app, bucket).await;
    enable_versioning(&app, bucket).await;
    enable_object_lock(&app, bucket, None).await;

    // Put an object
    put_object(&app, bucket, "test.txt", "test content").await;

    // Try to set retention WITHOUT version ID - should fail
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<Retention>
    <Mode>GOVERNANCE</Mode>
    <RetainUntilDate>2030-01-01T00:00:00Z</RetainUntilDate>
</Retention>"#;

    let request = Request::builder()
        .method(Method::PUT)
        .uri(format!("/{}/test.txt?retention", bucket))
        .header(header::CONTENT_TYPE, "application/xml")
        .body(Body::from(xml))
        .unwrap();

    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(
        response.status(),
        StatusCode::BAD_REQUEST,
        "Should require versionId for retention operations"
    );
}

#[tokio::test]
async fn test_combined_retention_and_legal_hold() {
    let (app, _temp_dir) = create_test_app().await;
    let bucket = "lock-test-combined";

    // Setup
    create_bucket(&app, bucket).await;
    enable_versioning(&app, bucket).await;
    enable_object_lock(&app, bucket, None).await;

    // Put an object
    let version_id = put_object(&app, bucket, "combined.txt", "test content").await;

    // Set both retention AND legal hold
    let future_date = chrono::Utc::now() + chrono::Duration::days(1);
    let retain_until = future_date.format("%Y-%m-%dT%H:%M:%SZ").to_string();

    set_retention(
        &app,
        bucket,
        "combined.txt",
        &version_id,
        "GOVERNANCE",
        &retain_until,
    )
    .await;
    set_legal_hold(&app, bucket, "combined.txt", &version_id, "ON").await;

    // Try to delete - should fail due to legal hold
    let delete_status = delete_object(&app, bucket, "combined.txt", &version_id).await;
    assert_eq!(
        delete_status,
        StatusCode::FORBIDDEN,
        "Should deny deletion with both retention and legal hold"
    );

    // Remove legal hold but retention still active
    set_legal_hold(&app, bucket, "combined.txt", &version_id, "OFF").await;

    // Try to delete again - should still fail due to retention
    let delete_status2 = delete_object(&app, bucket, "combined.txt", &version_id).await;
    assert_eq!(
        delete_status2,
        StatusCode::FORBIDDEN,
        "Should deny deletion when retention still active"
    );
}
