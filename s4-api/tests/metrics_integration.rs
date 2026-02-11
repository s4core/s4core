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

//! Integration tests for the metrics and stats endpoints.
//!
//! Tests `/metrics` (Prometheus format) and `/api/stats` (JSON format).
//! Uses in-process requests via tower::ServiceExt::oneshot.

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

static INIT: Once = Once::new();

fn init_test_env() {
    INIT.call_once(|| {
        std::env::set_var("S4_DISABLE_AUTH", "1");
    });
}

async fn create_test_storage() -> (BitcaskStorageEngine, TempDir) {
    init_test_env();

    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let data_path = temp_dir.path().join("volumes");
    let metadata_path = temp_dir.path().join("metadata.redb");

    std::fs::create_dir_all(&data_path).expect("Failed to create data dir");

    let engine = BitcaskStorageEngine::new(data_path, metadata_path, 1024 * 1024 * 10, 4096, false)
        .await
        .expect("Failed to create storage engine");

    (engine, temp_dir)
}

fn create_test_state(storage: BitcaskStorageEngine) -> AppState {
    AppState::new(
        storage,
        "test-access-key".to_string(),
        "test-secret-key".to_string(),
    )
}

async fn body_to_string(body: Body) -> String {
    let bytes = body.collect().await.unwrap().to_bytes();
    String::from_utf8(bytes.to_vec()).unwrap()
}

// ============================================================================
// Prometheus Metrics Endpoint Tests
// ============================================================================

#[tokio::test]
async fn test_prometheus_metrics_endpoint_returns_ok() {
    let (storage, _temp) = create_test_storage().await;
    let app = create_router(create_test_state(storage));

    let response = app
        .oneshot(Request::builder().method("GET").uri("/metrics").body(Body::empty()).unwrap())
        .await
        .unwrap();

    // Without a Prometheus recorder installed, the handler returns 503
    // (PrometheusHandle is None in test state).
    // This verifies the endpoint is routed correctly.
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn test_prometheus_metrics_with_recorder() {
    let (storage, _temp) = create_test_storage().await;

    // Install a test-local Prometheus recorder
    let builder = metrics_exporter_prometheus::PrometheusBuilder::new();
    let handle = builder.install_recorder().expect("Failed to install recorder");

    let state = create_test_state(storage).with_prometheus_handle(handle);
    let app = create_router(state);

    // Make a request to generate some metrics first
    let app_clone = app.clone();
    let _ = app_clone
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/test-bucket")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Now check /metrics
    let response = app
        .oneshot(Request::builder().method("GET").uri("/metrics").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = body_to_string(response.into_body()).await;
    // Should contain Prometheus-format metrics
    assert!(
        body.contains("http_requests_total") || body.contains("http_request_duration_seconds"),
        "Expected Prometheus metrics in body, got: {}",
        &body[..body.len().min(500)]
    );
}

// ============================================================================
// JSON Stats Endpoint Tests
// ============================================================================

#[tokio::test]
async fn test_api_stats_returns_json() {
    let (storage, _temp) = create_test_storage().await;
    let app = create_router(create_test_state(storage));

    let response = app
        .oneshot(Request::builder().method("GET").uri("/api/stats").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = body_to_string(response.into_body()).await;
    let json: serde_json::Value =
        serde_json::from_str(&body).expect("Response should be valid JSON");

    // Verify all expected fields exist
    assert!(
        json.get("uptime_seconds").is_some(),
        "Missing uptime_seconds"
    );
    assert!(json.get("buckets_count").is_some(), "Missing buckets_count");
    assert!(json.get("objects_count").is_some(), "Missing objects_count");
    assert!(
        json.get("storage_used_bytes").is_some(),
        "Missing storage_used_bytes"
    );
    assert!(
        json.get("dedup_unique_blobs").is_some(),
        "Missing dedup_unique_blobs"
    );
    assert!(
        json.get("dedup_total_references").is_some(),
        "Missing dedup_total_references"
    );
    assert!(json.get("dedup_ratio").is_some(), "Missing dedup_ratio");

    // Empty storage should have 0 counts
    assert_eq!(json["buckets_count"], 0);
    assert_eq!(json["objects_count"], 0);
    assert_eq!(json["storage_used_bytes"], 0);
}

#[tokio::test]
async fn test_api_stats_after_operations() {
    let (storage, _temp) = create_test_storage().await;
    let state = create_test_state(storage);

    // Create a bucket
    let app = create_router(state.clone());
    let response = app
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/stats-bucket")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Upload an object
    let app = create_router(state.clone());
    let response = app
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/stats-bucket/hello.txt")
                .header("content-type", "text/plain")
                .body(Body::from("Hello, metrics!"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Check stats
    let app = create_router(state.clone());
    let response = app
        .oneshot(Request::builder().method("GET").uri("/api/stats").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = body_to_string(response.into_body()).await;
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();

    assert_eq!(json["buckets_count"], 1, "Should have 1 bucket");
    assert_eq!(json["objects_count"], 1, "Should have 1 object");
    assert!(
        json["storage_used_bytes"].as_u64().unwrap() > 0,
        "Should have non-zero storage used"
    );
    assert!(
        json["uptime_seconds"].as_u64().is_some(),
        "Should have uptime"
    );
}
