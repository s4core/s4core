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

//! Integration tests for Lifecycle functionality.

use axum::{
    body::Body,
    http::{Method, Request, StatusCode},
};
use http_body_util::BodyExt;
use s4_api::{create_router, AppState};
use s4_core::storage::BitcaskStorageEngine;
use std::sync::Once;
use tempfile::TempDir;
use tower::ServiceExt;

static INIT: Once = Once::new();

/// Disables authentication for tests.
fn disable_auth() {
    INIT.call_once(|| {
        std::env::set_var("S4_DISABLE_AUTH", "1");
    });
}

/// Creates a test application state.
async fn create_test_state() -> (AppState, TempDir) {
    disable_auth();
    let temp_dir = TempDir::new().unwrap();
    let volumes_path = temp_dir.path().join("volumes");
    let metadata_path = temp_dir.path().join("metadata.redb");

    std::fs::create_dir_all(&volumes_path).unwrap();

    let storage = BitcaskStorageEngine::new(
        &volumes_path,
        &metadata_path,
        1024 * 1024 * 1024, // 1GB volume size
        4096,               // 4KB inline threshold
        false,              // no strict sync for tests
    )
    .await
    .unwrap();

    let state = AppState::new(
        storage,
        "test-access-key".to_string(),
        "test-secret-key".to_string(),
    );

    (state, temp_dir)
}

/// Creates a bucket for testing.
async fn create_test_bucket(state: &AppState) {
    let router = create_router(state.clone());
    let request = Request::builder()
        .method(Method::PUT)
        .uri("/test-bucket")
        .header("Host", "localhost")
        .body(Body::empty())
        .unwrap();

    let response = router.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_put_get_delete_lifecycle() {
    let (state, _temp_dir) = create_test_state().await;
    create_test_bucket(&state).await;

    let lifecycle_xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<LifecycleConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Rule>
    <ID>expire-logs</ID>
    <Status>Enabled</Status>
    <Filter>
      <Prefix>logs/</Prefix>
    </Filter>
    <Expiration>
      <Days>30</Days>
    </Expiration>
  </Rule>
</LifecycleConfiguration>"#;

    // PUT lifecycle configuration
    let router = create_router(state.clone());
    let request = Request::builder()
        .method(Method::PUT)
        .uri("/test-bucket?lifecycle")
        .header("Host", "localhost")
        .header("Content-Type", "application/xml")
        .body(Body::from(lifecycle_xml))
        .unwrap();

    let response = router.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // GET lifecycle configuration
    let router = create_router(state.clone());
    let request = Request::builder()
        .method(Method::GET)
        .uri("/test-bucket?lifecycle")
        .header("Host", "localhost")
        .body(Body::empty())
        .unwrap();

    let response = router.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
    let body_str = String::from_utf8_lossy(&body_bytes);
    assert!(body_str.contains("<ID>expire-logs</ID>"));
    assert!(body_str.contains("<Prefix>logs/</Prefix>"));
    assert!(body_str.contains("<Days>30</Days>"));

    // DELETE lifecycle configuration
    let router = create_router(state.clone());
    let request = Request::builder()
        .method(Method::DELETE)
        .uri("/test-bucket?lifecycle")
        .header("Host", "localhost")
        .body(Body::empty())
        .unwrap();

    let response = router.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    // GET after DELETE should return 404
    let router = create_router(state.clone());
    let request = Request::builder()
        .method(Method::GET)
        .uri("/test-bucket?lifecycle")
        .header("Host", "localhost")
        .body(Body::empty())
        .unwrap();

    let response = router.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_lifecycle_validation_invalid_xml() {
    let (state, _temp_dir) = create_test_state().await;
    create_test_bucket(&state).await;

    let invalid_xml = r#"<InvalidXML>not a lifecycle config</InvalidXML>"#;

    let router = create_router(state.clone());
    let request = Request::builder()
        .method(Method::PUT)
        .uri("/test-bucket?lifecycle")
        .header("Host", "localhost")
        .header("Content-Type", "application/xml")
        .body(Body::from(invalid_xml))
        .unwrap();

    let response = router.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_lifecycle_validation_invalid_rule() {
    let (state, _temp_dir) = create_test_state().await;
    create_test_bucket(&state).await;

    // Rule without action (invalid)
    let invalid_lifecycle = r#"<?xml version="1.0" encoding="UTF-8"?>
<LifecycleConfiguration>
  <Rule>
    <ID>invalid-rule</ID>
    <Status>Enabled</Status>
    <Filter><Prefix></Prefix></Filter>
  </Rule>
</LifecycleConfiguration>"#;

    let router = create_router(state.clone());
    let request = Request::builder()
        .method(Method::PUT)
        .uri("/test-bucket?lifecycle")
        .header("Host", "localhost")
        .header("Content-Type", "application/xml")
        .body(Body::from(invalid_lifecycle))
        .unwrap();

    let response = router.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_lifecycle_requires_bucket() {
    let (state, _temp_dir) = create_test_state().await;

    let lifecycle_xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<LifecycleConfiguration>
  <Rule>
    <ID>test</ID>
    <Status>Enabled</Status>
    <Filter><Prefix></Prefix></Filter>
    <Expiration><Days>30</Days></Expiration>
  </Rule>
</LifecycleConfiguration>"#;

    let router = create_router(state.clone());
    let request = Request::builder()
        .method(Method::PUT)
        .uri("/nonexistent-bucket?lifecycle")
        .header("Host", "localhost")
        .header("Content-Type", "application/xml")
        .body(Body::from(lifecycle_xml))
        .unwrap();

    let response = router.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_lifecycle_multiple_rules() {
    let (state, _temp_dir) = create_test_state().await;
    create_test_bucket(&state).await;

    let lifecycle_xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<LifecycleConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Rule>
    <ID>expire-logs</ID>
    <Status>Enabled</Status>
    <Filter><Prefix>logs/</Prefix></Filter>
    <Expiration><Days>30</Days></Expiration>
  </Rule>
  <Rule>
    <ID>expire-temp</ID>
    <Status>Enabled</Status>
    <Filter><Prefix>temp/</Prefix></Filter>
    <Expiration><Days>7</Days></Expiration>
  </Rule>
  <Rule>
    <ID>cleanup-versions</ID>
    <Status>Enabled</Status>
    <Filter><Prefix></Prefix></Filter>
    <NoncurrentVersionExpiration>
      <NoncurrentDays>90</NoncurrentDays>
    </NoncurrentVersionExpiration>
  </Rule>
</LifecycleConfiguration>"#;

    let router = create_router(state.clone());
    let request = Request::builder()
        .method(Method::PUT)
        .uri("/test-bucket?lifecycle")
        .header("Host", "localhost")
        .header("Content-Type", "application/xml")
        .body(Body::from(lifecycle_xml))
        .unwrap();

    let response = router.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // GET and verify all rules
    let router = create_router(state.clone());
    let request = Request::builder()
        .method(Method::GET)
        .uri("/test-bucket?lifecycle")
        .header("Host", "localhost")
        .body(Body::empty())
        .unwrap();

    let response = router.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
    let body_str = String::from_utf8_lossy(&body_bytes);
    assert!(body_str.contains("<ID>expire-logs</ID>"));
    assert!(body_str.contains("<ID>expire-temp</ID>"));
    assert!(body_str.contains("<ID>cleanup-versions</ID>"));
    assert!(body_str.contains("<NoncurrentDays>90</NoncurrentDays>"));
}

#[tokio::test]
async fn test_lifecycle_with_expired_delete_marker() {
    let (state, _temp_dir) = create_test_state().await;
    create_test_bucket(&state).await;

    let lifecycle_xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<LifecycleConfiguration>
  <Rule>
    <ID>cleanup-markers</ID>
    <Status>Enabled</Status>
    <Filter><Prefix></Prefix></Filter>
    <ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker>
  </Rule>
</LifecycleConfiguration>"#;

    let router = create_router(state.clone());
    let request = Request::builder()
        .method(Method::PUT)
        .uri("/test-bucket?lifecycle")
        .header("Host", "localhost")
        .header("Content-Type", "application/xml")
        .body(Body::from(lifecycle_xml))
        .unwrap();

    let response = router.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Verify it was stored
    let router = create_router(state.clone());
    let request = Request::builder()
        .method(Method::GET)
        .uri("/test-bucket?lifecycle")
        .header("Host", "localhost")
        .body(Body::empty())
        .unwrap();

    let response = router.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
    let body_str = String::from_utf8_lossy(&body_bytes);
    assert!(body_str.contains("<ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker>"));
}

#[tokio::test]
async fn test_lifecycle_duplicate_rule_ids() {
    let (state, _temp_dir) = create_test_state().await;
    create_test_bucket(&state).await;

    let lifecycle_xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<LifecycleConfiguration>
  <Rule>
    <ID>same-id</ID>
    <Status>Enabled</Status>
    <Filter><Prefix>logs/</Prefix></Filter>
    <Expiration><Days>30</Days></Expiration>
  </Rule>
  <Rule>
    <ID>same-id</ID>
    <Status>Enabled</Status>
    <Filter><Prefix>temp/</Prefix></Filter>
    <Expiration><Days>7</Days></Expiration>
  </Rule>
</LifecycleConfiguration>"#;

    let router = create_router(state.clone());
    let request = Request::builder()
        .method(Method::PUT)
        .uri("/test-bucket?lifecycle")
        .header("Host", "localhost")
        .header("Content-Type", "application/xml")
        .body(Body::from(lifecycle_xml))
        .unwrap();

    let response = router.oneshot(request).await.unwrap();
    // Should fail validation due to duplicate IDs
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}
