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
    let metadata_path = temp_dir.path().join("metadata_db");

    std::fs::create_dir_all(&data_path).expect("Failed to create data dir");

    let engine = BitcaskStorageEngine::new(data_path, metadata_path, 1024 * 1024 * 10, 4096, false)
        .await
        .expect("Failed to create storage engine");

    (engine, temp_dir)
}

async fn create_test_state(storage: BitcaskStorageEngine, data_dir: &std::path::Path) -> AppState {
    AppState::new(
        storage,
        "test-access-key".to_string(),
        "test-secret-key".to_string(),
        data_dir,
    )
    .await
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
    let app = create_router(create_test_state(storage, _temp.path()).await);

    let response = app
        .oneshot(Request::builder().method("GET").uri("/metrics").body(Body::empty()).unwrap())
        .await
        .unwrap();

    // Without a Prometheus recorder installed, the handler returns 503
    // (PrometheusHandle is None in test state).
    // This verifies the endpoint is routed correctly.
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}

// The tests below read the exposition the way Prometheus does: a series is a
// name and a set of labels, and its value is a number. Checking only that a
// name appears in the body passed for as long as `http_requests_total` was
// registered and never incremented.
//
// Each test records into a recorder of its own, built the way the server
// builds it and installed for the test's thread only. That is why they run on
// a current-thread runtime: every request is polled, and so recorded, on the
// thread that sees the recorder.

/// Sends one request and returns its status.
async fn send(state: &AppState, method: &str, uri: &str) -> StatusCode {
    create_router(state.clone())
        .oneshot(Request::builder().method(method).uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap()
        .status()
}

/// Scrapes `/metrics` and returns the exposition.
async fn scrape(state: &AppState) -> String {
    let response = create_router(state.clone())
        .oneshot(Request::builder().method("GET").uri("/metrics").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    body_to_string(response.into_body()).await
}

/// One series of the exposition.
struct Sample<'a> {
    name: &'a str,
    labels: Vec<(String, String)>,
    value: f64,
}

/// Splits one exposition line into its name, labels and value.
///
/// Label values are read as quoted strings, because the path label itself
/// holds braces (`/{bucket}`).
fn parse_line(line: &str) -> Option<Sample<'_>> {
    if line.starts_with('#') || line.trim().is_empty() {
        return None;
    }
    let name_end = line.find(['{', ' '])?;
    let name = &line[..name_end];
    let mut labels = Vec::new();
    let mut rest = &line[name_end..];
    if let Some(inner) = rest.strip_prefix('{') {
        let mut chars = inner.char_indices().peekable();
        loop {
            let (key_start, first) = chars.next()?;
            if first == '}' {
                rest = &inner[key_start + 1..];
                break;
            }
            let eq = inner[key_start..].find('=')? + key_start;
            let key = inner[key_start..eq].to_string();
            while chars.peek()?.0 <= eq {
                chars.next();
            }
            if chars.next()?.1 != '"' {
                return None;
            }
            let mut value = String::new();
            loop {
                match chars.next()?.1 {
                    '\\' => value.push(chars.next()?.1),
                    '"' => break,
                    c => value.push(c),
                }
            }
            labels.push((key, value));
            if let Some(&(_, ',')) = chars.peek() {
                chars.next();
            }
        }
    }
    let value = rest.split_whitespace().next()?.parse().ok()?;
    Some(Sample {
        name,
        labels,
        value,
    })
}

/// The value of the series `name` whose labels are exactly `labels`.
fn sample(body: &str, name: &str, labels: &[(&str, &str)]) -> Option<f64> {
    let mut wanted: Vec<(String, String)> =
        labels.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
    wanted.sort();
    body.lines().filter_map(parse_line).find_map(|mut s| {
        s.labels.sort();
        (s.name == name && s.labels == wanted).then_some(s.value)
    })
}

/// The `le` bounds of the histogram series `name` with `labels`.
fn bucket_bounds(body: &str, name: &str, labels: &[(&str, &str)]) -> Vec<f64> {
    let bucket = format!("{name}_bucket");
    body.lines()
        .filter_map(parse_line)
        .filter(|s| {
            s.name == bucket
                && s.labels.len() == labels.len() + 1
                && labels.iter().all(|(k, v)| s.labels.iter().any(|(lk, lv)| lk == k && lv == v))
        })
        .filter_map(|s| {
            let le = s.labels.into_iter().find(|(k, _)| k == "le")?.1;
            if le == "+Inf" {
                Some(f64::INFINITY)
            } else {
                le.parse().ok()
            }
        })
        .collect()
}

/// The type `# TYPE` declares for `name`.
fn metric_type<'a>(body: &'a str, name: &str) -> Option<&'a str> {
    body.lines().find_map(|line| {
        let mut words = line.strip_prefix("# TYPE ")?.split_whitespace();
        (words.next()? == name).then_some(())?;
        words.next()
    })
}

#[test]
fn exposition_parser_reads_braces_inside_label_values() {
    let line = r#"http_requests_total{method="GET",path="/{bucket}/{key}",status="404"} 3"#;
    let sample_line = parse_line(line).expect("line parses");
    assert_eq!(sample_line.name, "http_requests_total");
    assert_eq!(
        sample_line.labels,
        vec![
            ("method".to_string(), "GET".to_string()),
            ("path".to_string(), "/{bucket}/{key}".to_string()),
            ("status".to_string(), "404".to_string()),
        ]
    );
    assert_eq!(sample_line.value, 3.0);
    assert_eq!(
        sample(line, "http_requests_total", &[("method", "GET")]),
        None
    );
}

#[tokio::test(flavor = "current_thread")]
async fn http_requests_total_counts_every_request() {
    let (storage, temp) = create_test_storage().await;
    let recorder = s4_api::prometheus::builder().expect("recorder builder").build_recorder();
    let _local = metrics::set_default_local_recorder(&recorder);
    let state = create_test_state(storage, temp.path())
        .await
        .with_prometheus_handle(recorder.handle());

    assert_eq!(send(&state, "PUT", "/bucket-a").await, StatusCode::OK);
    assert_eq!(send(&state, "PUT", "/bucket-b").await, StatusCode::OK);
    assert_eq!(
        send(&state, "GET", "/no-such-bucket/key").await,
        StatusCode::NOT_FOUND
    );

    let first = scrape(&state).await;
    assert_eq!(metric_type(&first, "http_requests_total"), Some("counter"));
    assert_eq!(
        sample(
            &first,
            "http_requests_total",
            &[("method", "PUT"), ("path", "/{bucket}"), ("status", "200")]
        ),
        Some(2.0),
        "two bucket creations, got:\n{first}"
    );
    assert_eq!(
        sample(
            &first,
            "http_requests_total",
            &[
                ("method", "GET"),
                ("path", "/{bucket}/{key}"),
                ("status", "404")
            ]
        ),
        Some(1.0),
        "the 404 is counted under its own status, got:\n{first}"
    );

    // A request is counted once it has been answered, so a scrape shows up in
    // the next one and not in itself.
    let metrics_scrape = [("method", "GET"), ("path", "/metrics"), ("status", "200")];
    assert_eq!(sample(&first, "http_requests_total", &metrics_scrape), None);
    let second = scrape(&state).await;
    assert_eq!(
        sample(&second, "http_requests_total", &metrics_scrape),
        Some(1.0)
    );
}

#[tokio::test(flavor = "current_thread")]
async fn http_request_duration_is_a_histogram_with_the_shared_buckets() {
    let (storage, temp) = create_test_storage().await;
    let recorder = s4_api::prometheus::builder().expect("recorder builder").build_recorder();
    let _local = metrics::set_default_local_recorder(&recorder);
    let state = create_test_state(storage, temp.path())
        .await
        .with_prometheus_handle(recorder.handle());

    assert_eq!(send(&state, "PUT", "/bucket-a").await, StatusCode::OK);
    assert_eq!(send(&state, "PUT", "/bucket-b").await, StatusCode::OK);

    let body = scrape(&state).await;
    // A summary carries quantiles computed on one node, which cannot be added
    // up across the nodes of a cluster; a histogram's buckets can.
    assert_eq!(
        metric_type(&body, "http_request_duration_seconds"),
        Some("histogram"),
        "got:\n{body}"
    );

    let labels = [("method", "PUT"), ("path", "/{bucket}")];
    let mut expected: Vec<f64> = s4_api::prometheus::DURATION_BUCKETS_SECONDS.to_vec();
    expected.push(f64::INFINITY);
    assert_eq!(
        bucket_bounds(&body, "http_request_duration_seconds", &labels),
        expected
    );

    let mut inf = labels.to_vec();
    inf.push(("le", "+Inf"));
    assert_eq!(
        sample(&body, "http_request_duration_seconds_bucket", &inf),
        Some(2.0)
    );
    assert_eq!(
        sample(&body, "http_request_duration_seconds_count", &labels),
        Some(2.0),
        "one observation per request, as many as the counter holds"
    );
}

// ============================================================================
// JSON Stats Endpoint Tests
// ============================================================================

#[tokio::test]
async fn test_api_stats_returns_json() {
    let (storage, _temp) = create_test_storage().await;
    let app = create_router(create_test_state(storage, _temp.path()).await);

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
    let state = create_test_state(storage, _temp.path()).await;

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
