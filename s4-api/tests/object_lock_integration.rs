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
    let metadata_path = temp_dir.path().join("metadata_db");

    std::fs::create_dir_all(&volumes_dir).unwrap();

    let engine = BitcaskStorageEngine::new(&volumes_dir, &metadata_path, 1024 * 1024, 4096, false)
        .await
        .unwrap();

    let state = AppState::new(
        engine,
        "test-key".to_string(),
        "test-secret".to_string(),
        temp_dir.path(),
    )
    .await;
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

/// Deletes an object with the x-amz-bypass-governance-retention header.
async fn delete_object_with_bypass(
    app: &axum::Router,
    bucket: &str,
    key: &str,
    version_id: &str,
) -> StatusCode {
    let request = Request::builder()
        .method(Method::DELETE)
        .uri(format!("/{}/{}?versionId={}", bucket, key, version_id))
        .header("x-amz-bypass-governance-retention", "true")
        .body(Body::empty())
        .unwrap();

    let response = app.clone().oneshot(request).await.unwrap();
    response.status()
}

/// Sets retention with optional bypass governance header.
async fn set_retention_with_bypass(
    app: &axum::Router,
    bucket: &str,
    key: &str,
    version_id: &str,
    body_xml: &str,
    bypass: bool,
) -> StatusCode {
    let mut builder = Request::builder()
        .method(Method::PUT)
        .uri(format!(
            "/{}/{}?retention&versionId={}",
            bucket, key, version_id
        ))
        .header(header::CONTENT_TYPE, "application/xml");

    if bypass {
        builder = builder.header("x-amz-bypass-governance-retention", "true");
    }

    let request = builder.body(Body::from(body_xml.to_string())).unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    response.status()
}

/// Batch-deletes objects with optional bypass governance header.
async fn delete_objects_batch(
    app: &axum::Router,
    bucket: &str,
    objects_xml: &str,
    bypass: bool,
) -> (StatusCode, String) {
    let body_bytes = objects_xml.as_bytes();
    let digest = md5::compute(body_bytes);
    let content_md5 =
        base64::Engine::encode(&base64::engine::general_purpose::STANDARD, digest.as_ref());

    let mut builder = Request::builder()
        .method(Method::POST)
        .uri(format!("/{}?delete", bucket))
        .header(header::CONTENT_TYPE, "application/xml")
        .header("content-md5", content_md5);

    if bypass {
        builder = builder.header("x-amz-bypass-governance-retention", "true");
    }

    let request = builder.body(Body::from(objects_xml.to_string())).unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body_str = String::from_utf8(body_bytes.to_vec()).unwrap();
    (status, body_str)
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

    // Extending COMPLIANCE retention (later date, same mode) should succeed per S3 spec
    let new_date = chrono::Utc::now() + chrono::Duration::days(14);
    let new_retain_until = new_date.format("%Y-%m-%dT%H:%M:%SZ").to_string();

    let extend_status = set_retention(
        &app,
        bucket,
        "compliance.txt",
        &version_id,
        "COMPLIANCE",
        &new_retain_until,
    )
    .await;
    assert_eq!(
        extend_status,
        StatusCode::OK,
        "Should allow extending COMPLIANCE retention period"
    );

    // Shortening COMPLIANCE retention (earlier date) should be denied
    let shorter_date = chrono::Utc::now() + chrono::Duration::days(1);
    let shorter_retain_until = shorter_date.format("%Y-%m-%dT%H:%M:%SZ").to_string();

    let shorten_status = set_retention(
        &app,
        bucket,
        "compliance.txt",
        &version_id,
        "COMPLIANCE",
        &shorter_retain_until,
    )
    .await;
    assert_eq!(
        shorten_status,
        StatusCode::FORBIDDEN,
        "Should deny shortening COMPLIANCE retention (AccessDenied)"
    );

    // Changing COMPLIANCE to GOVERNANCE should be denied
    let gov_date = chrono::Utc::now() + chrono::Duration::days(30);
    let gov_retain_until = gov_date.format("%Y-%m-%dT%H:%M:%SZ").to_string();

    let change_mode_status = set_retention(
        &app,
        bucket,
        "compliance.txt",
        &version_id,
        "GOVERNANCE",
        &gov_retain_until,
    )
    .await;
    assert_eq!(
        change_mode_status,
        StatusCode::FORBIDDEN,
        "Should deny changing COMPLIANCE to GOVERNANCE (AccessDenied)"
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
async fn test_retention_without_version_id_resolves_latest() {
    let (app, _temp_dir) = create_test_app().await;
    let bucket = "lock-test-no-version";

    // Setup
    create_bucket(&app, bucket).await;
    enable_versioning(&app, bucket).await;
    enable_object_lock(&app, bucket, None).await;

    // Put an object
    put_object(&app, bucket, "test.txt", "test content").await;

    // Set retention WITHOUT version ID - should resolve to latest version
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
        StatusCode::OK,
        "Should resolve to latest version when versionId is omitted"
    );

    // Verify retention was set by GET without versionId
    let request = Request::builder()
        .method(Method::GET)
        .uri(format!("/{}/test.txt?retention", bucket))
        .body(Body::empty())
        .unwrap();

    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "GET retention without versionId should also resolve to latest version"
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

// ============================================================================
// Bypass Governance Retention Tests
// ============================================================================

#[tokio::test]
async fn test_bypass_governance_delete() {
    let (app, _temp_dir) = create_test_app().await;
    let bucket = "lock-test-bypass-del";

    // Setup
    create_bucket(&app, bucket).await;
    enable_versioning(&app, bucket).await;
    enable_object_lock(&app, bucket, None).await;

    // Put an object and set GOVERNANCE retention
    let version_id = put_object(&app, bucket, "gov.txt", "test content").await;
    let future_date = chrono::Utc::now() + chrono::Duration::days(1);
    let retain_until = future_date.format("%Y-%m-%dT%H:%M:%SZ").to_string();
    let status = set_retention(
        &app,
        bucket,
        "gov.txt",
        &version_id,
        "GOVERNANCE",
        &retain_until,
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // Delete WITHOUT bypass header — should fail
    let del_no_bypass = delete_object(&app, bucket, "gov.txt", &version_id).await;
    assert_eq!(
        del_no_bypass,
        StatusCode::FORBIDDEN,
        "GOVERNANCE delete without bypass should be denied"
    );

    // Delete WITH bypass header — should succeed
    let del_bypass = delete_object_with_bypass(&app, bucket, "gov.txt", &version_id).await;
    assert_eq!(
        del_bypass,
        StatusCode::NO_CONTENT,
        "GOVERNANCE delete with bypass should succeed"
    );
}

#[tokio::test]
async fn test_bypass_governance_delete_does_not_bypass_compliance() {
    let (app, _temp_dir) = create_test_app().await;
    let bucket = "lock-test-bypass-compl-del";

    // Setup
    create_bucket(&app, bucket).await;
    enable_versioning(&app, bucket).await;
    enable_object_lock(&app, bucket, None).await;

    // Put an object and set COMPLIANCE retention
    let version_id = put_object(&app, bucket, "compl.txt", "test content").await;
    let future_date = chrono::Utc::now() + chrono::Duration::days(1);
    let retain_until = future_date.format("%Y-%m-%dT%H:%M:%SZ").to_string();
    let status = set_retention(
        &app,
        bucket,
        "compl.txt",
        &version_id,
        "COMPLIANCE",
        &retain_until,
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // Delete WITH bypass header — should STILL fail for COMPLIANCE
    let del_bypass = delete_object_with_bypass(&app, bucket, "compl.txt", &version_id).await;
    assert_eq!(
        del_bypass,
        StatusCode::FORBIDDEN,
        "COMPLIANCE delete should be denied even with bypass header"
    );
}

#[tokio::test]
async fn test_bypass_governance_modify_retention() {
    let (app, _temp_dir) = create_test_app().await;
    let bucket = "lock-test-bypass-modify";

    // Setup
    create_bucket(&app, bucket).await;
    enable_versioning(&app, bucket).await;
    enable_object_lock(&app, bucket, None).await;

    // Put an object and set GOVERNANCE retention
    let version_id = put_object(&app, bucket, "gov.txt", "test content").await;
    let future_date = chrono::Utc::now() + chrono::Duration::days(7);
    let retain_until = future_date.format("%Y-%m-%dT%H:%M:%SZ").to_string();
    let status = set_retention(
        &app,
        bucket,
        "gov.txt",
        &version_id,
        "GOVERNANCE",
        &retain_until,
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // Modify WITHOUT bypass — should fail (403)
    let new_date = chrono::Utc::now() + chrono::Duration::days(3);
    let new_retain = new_date.format("%Y-%m-%dT%H:%M:%SZ").to_string();
    let modify_xml = format!(
        r#"<Retention><Mode>GOVERNANCE</Mode><RetainUntilDate>{}</RetainUntilDate></Retention>"#,
        new_retain
    );
    let status_no_bypass =
        set_retention_with_bypass(&app, bucket, "gov.txt", &version_id, &modify_xml, false).await;
    assert_eq!(
        status_no_bypass,
        StatusCode::FORBIDDEN,
        "Modifying GOVERNANCE retention without bypass should be denied"
    );

    // Modify WITH bypass — should succeed
    let status_bypass =
        set_retention_with_bypass(&app, bucket, "gov.txt", &version_id, &modify_xml, true).await;
    assert_eq!(
        status_bypass,
        StatusCode::OK,
        "Modifying GOVERNANCE retention with bypass should succeed"
    );
}

#[tokio::test]
async fn test_bypass_governance_clear_retention() {
    let (app, _temp_dir) = create_test_app().await;
    let bucket = "lock-test-bypass-clear";

    // Setup
    create_bucket(&app, bucket).await;
    enable_versioning(&app, bucket).await;
    enable_object_lock(&app, bucket, None).await;

    // Put an object and set GOVERNANCE retention
    let version_id = put_object(&app, bucket, "gov.txt", "test content").await;
    let future_date = chrono::Utc::now() + chrono::Duration::days(7);
    let retain_until = future_date.format("%Y-%m-%dT%H:%M:%SZ").to_string();
    let status = set_retention(
        &app,
        bucket,
        "gov.txt",
        &version_id,
        "GOVERNANCE",
        &retain_until,
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // Clear retention with empty body + bypass header
    let empty_xml = "<Retention></Retention>";
    let clear_status =
        set_retention_with_bypass(&app, bucket, "gov.txt", &version_id, empty_xml, true).await;
    assert_eq!(
        clear_status,
        StatusCode::OK,
        "Clearing GOVERNANCE retention with bypass should succeed"
    );

    // Now delete should succeed (no retention)
    let del_status = delete_object(&app, bucket, "gov.txt", &version_id).await;
    assert_eq!(
        del_status,
        StatusCode::NO_CONTENT,
        "Delete should succeed after retention is cleared"
    );
}

#[tokio::test]
async fn test_bypass_governance_clear_compliance_denied() {
    let (app, _temp_dir) = create_test_app().await;
    let bucket = "lock-test-bypass-clear-compl";

    // Setup
    create_bucket(&app, bucket).await;
    enable_versioning(&app, bucket).await;
    enable_object_lock(&app, bucket, None).await;

    // Put an object and set COMPLIANCE retention
    let version_id = put_object(&app, bucket, "compl.txt", "test content").await;
    let future_date = chrono::Utc::now() + chrono::Duration::days(7);
    let retain_until = future_date.format("%Y-%m-%dT%H:%M:%SZ").to_string();
    let status = set_retention(
        &app,
        bucket,
        "compl.txt",
        &version_id,
        "COMPLIANCE",
        &retain_until,
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // Clear with bypass — should STILL fail for COMPLIANCE
    let empty_xml = "<Retention></Retention>";
    let clear_status =
        set_retention_with_bypass(&app, bucket, "compl.txt", &version_id, empty_xml, true).await;
    assert_eq!(
        clear_status,
        StatusCode::FORBIDDEN,
        "Clearing COMPLIANCE retention should be denied even with bypass"
    );
}

#[tokio::test]
async fn test_bypass_governance_batch_delete() {
    let (app, _temp_dir) = create_test_app().await;
    let bucket = "lock-test-bypass-batch";

    // Setup
    create_bucket(&app, bucket).await;
    enable_versioning(&app, bucket).await;
    enable_object_lock(&app, bucket, None).await;

    // Put objects and set GOVERNANCE retention on both
    let v1 = put_object(&app, bucket, "a.txt", "content a").await;
    let v2 = put_object(&app, bucket, "b.txt", "content b").await;
    let future_date = chrono::Utc::now() + chrono::Duration::days(1);
    let retain_until = future_date.format("%Y-%m-%dT%H:%M:%SZ").to_string();
    set_retention(&app, bucket, "a.txt", &v1, "GOVERNANCE", &retain_until).await;
    set_retention(&app, bucket, "b.txt", &v2, "GOVERNANCE", &retain_until).await;

    let delete_xml = format!(
        r#"<Delete>
  <Object><Key>a.txt</Key><VersionId>{}</VersionId></Object>
  <Object><Key>b.txt</Key><VersionId>{}</VersionId></Object>
  <Quiet>false</Quiet>
</Delete>"#,
        v1, v2
    );

    // Batch delete WITHOUT bypass — both should fail
    let (status, body) = delete_objects_batch(&app, bucket, &delete_xml, false).await;
    assert_eq!(status, StatusCode::OK, "Batch delete always returns 200");
    assert!(
        body.contains("<Error>"),
        "Should have errors for locked objects: {}",
        body
    );
    assert!(
        body.contains("AccessDenied"),
        "Errors should be AccessDenied: {}",
        body
    );

    // Batch delete WITH bypass — both should succeed
    let (status2, body2) = delete_objects_batch(&app, bucket, &delete_xml, true).await;
    assert_eq!(status2, StatusCode::OK);
    assert!(
        body2.contains("<Deleted>"),
        "Should have deletions with bypass: {}",
        body2
    );
    assert!(
        !body2.contains("<Error>"),
        "Should have no errors with bypass: {}",
        body2
    );
}
