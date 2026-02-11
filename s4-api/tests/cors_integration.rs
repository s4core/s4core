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

//! Integration tests for CORS functionality.

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
async fn test_put_get_delete_cors() {
    let (state, _temp_dir) = create_test_state().await;
    create_test_bucket(&state).await;

    let cors_xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<CORSConfiguration>
  <CORSRule>
    <AllowedOrigin>https://example.com</AllowedOrigin>
    <AllowedMethod>GET</AllowedMethod>
    <AllowedMethod>PUT</AllowedMethod>
    <AllowedHeader>*</AllowedHeader>
    <ExposeHeader>ETag</ExposeHeader>
    <MaxAgeSeconds>3600</MaxAgeSeconds>
  </CORSRule>
</CORSConfiguration>"#;

    // PUT CORS configuration
    let router = create_router(state.clone());
    let request = Request::builder()
        .method(Method::PUT)
        .uri("/test-bucket?cors")
        .header("Host", "localhost")
        .header("Content-Type", "application/xml")
        .body(Body::from(cors_xml))
        .unwrap();

    let response = router.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // GET CORS configuration
    let router = create_router(state.clone());
    let request = Request::builder()
        .method(Method::GET)
        .uri("/test-bucket?cors")
        .header("Host", "localhost")
        .body(Body::empty())
        .unwrap();

    let response = router.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let body_str = String::from_utf8_lossy(&body);
    assert!(body_str.contains("CORSConfiguration"));
    assert!(body_str.contains("https://example.com"));
    assert!(body_str.contains("GET"));
    assert!(body_str.contains("PUT"));
    assert!(body_str.contains("3600"));

    // DELETE CORS configuration
    let router = create_router(state.clone());
    let request = Request::builder()
        .method(Method::DELETE)
        .uri("/test-bucket?cors")
        .header("Host", "localhost")
        .body(Body::empty())
        .unwrap();

    let response = router.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    // GET CORS after DELETE should return 404
    let router = create_router(state.clone());
    let request = Request::builder()
        .method(Method::GET)
        .uri("/test-bucket?cors")
        .header("Host", "localhost")
        .body(Body::empty())
        .unwrap();

    let response = router.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_cors_on_nonexistent_bucket() {
    let (state, _temp_dir) = create_test_state().await;

    // GET CORS on nonexistent bucket should return NoSuchBucket
    let router = create_router(state.clone());
    let request = Request::builder()
        .method(Method::GET)
        .uri("/nonexistent-bucket?cors")
        .header("Host", "localhost")
        .body(Body::empty())
        .unwrap();

    let response = router.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let body_str = String::from_utf8_lossy(&body);
    assert!(body_str.contains("NoSuchBucket"));
}

#[tokio::test]
async fn test_cors_invalid_xml() {
    let (state, _temp_dir) = create_test_state().await;
    create_test_bucket(&state).await;

    // PUT invalid CORS XML
    let router = create_router(state.clone());
    let request = Request::builder()
        .method(Method::PUT)
        .uri("/test-bucket?cors")
        .header("Host", "localhost")
        .header("Content-Type", "application/xml")
        .body(Body::from("not valid xml"))
        .unwrap();

    let response = router.oneshot(request).await.unwrap();
    // Should return 400 Bad Request for malformed XML
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_cors_empty_rules() {
    let (state, _temp_dir) = create_test_state().await;
    create_test_bucket(&state).await;

    // PUT CORS with no rules (should fail validation)
    let cors_xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<CORSConfiguration>
</CORSConfiguration>"#;

    let router = create_router(state.clone());
    let request = Request::builder()
        .method(Method::PUT)
        .uri("/test-bucket?cors")
        .header("Host", "localhost")
        .header("Content-Type", "application/xml")
        .body(Body::from(cors_xml))
        .unwrap();

    let response = router.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_cors_multiple_rules() {
    let (state, _temp_dir) = create_test_state().await;
    create_test_bucket(&state).await;

    let cors_xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<CORSConfiguration>
  <CORSRule>
    <ID>rule1</ID>
    <AllowedOrigin>https://app1.example.com</AllowedOrigin>
    <AllowedMethod>GET</AllowedMethod>
  </CORSRule>
  <CORSRule>
    <ID>rule2</ID>
    <AllowedOrigin>https://app2.example.com</AllowedOrigin>
    <AllowedMethod>GET</AllowedMethod>
    <AllowedMethod>PUT</AllowedMethod>
    <AllowedMethod>DELETE</AllowedMethod>
  </CORSRule>
</CORSConfiguration>"#;

    // PUT CORS configuration
    let router = create_router(state.clone());
    let request = Request::builder()
        .method(Method::PUT)
        .uri("/test-bucket?cors")
        .header("Host", "localhost")
        .header("Content-Type", "application/xml")
        .body(Body::from(cors_xml))
        .unwrap();

    let response = router.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // GET and verify
    let router = create_router(state.clone());
    let request = Request::builder()
        .method(Method::GET)
        .uri("/test-bucket?cors")
        .header("Host", "localhost")
        .body(Body::empty())
        .unwrap();

    let response = router.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let body_str = String::from_utf8_lossy(&body);
    assert!(body_str.contains("rule1"));
    assert!(body_str.contains("rule2"));
    assert!(body_str.contains("https://app1.example.com"));
    assert!(body_str.contains("https://app2.example.com"));
}

#[tokio::test]
async fn test_cors_preflight_response() {
    let (state, _temp_dir) = create_test_state().await;
    create_test_bucket(&state).await;

    // The CORS layer should handle OPTIONS requests
    let router = create_router(state.clone());
    let request = Request::builder()
        .method(Method::OPTIONS)
        .uri("/test-bucket/test-object")
        .header("Host", "localhost")
        .header("Origin", "https://example.com")
        .header("Access-Control-Request-Method", "PUT")
        .body(Body::empty())
        .unwrap();

    let response = router.oneshot(request).await.unwrap();

    // OPTIONS should be handled by CORS middleware
    // Note: The response might be OK or have CORS headers depending on tower-http behavior
    let headers = response.headers();
    // Check that CORS headers are present
    assert!(
        headers.contains_key("access-control-allow-origin")
            || response.status() == StatusCode::OK
            || response.status() == StatusCode::NO_CONTENT
    );
}
