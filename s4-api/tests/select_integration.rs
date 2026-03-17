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

//! S3 Select and S4 SQL Integration Tests
//!
//! Tests SelectObjectContent (POST /{bucket}/{key}?select&select-type=2) and
//! S4 multi-object SQL (POST /{bucket}?sql) using in-process requests.

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

/// Helper to create a bucket and upload an object.
async fn setup_bucket_with_object(
    app: &axum::Router,
    bucket: &str,
    key: &str,
    data: &[u8],
    content_type: &str,
) {
    // Create bucket
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/{bucket}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(
        response.status() == StatusCode::OK || response.status() == StatusCode::CONFLICT,
        "Failed to create bucket: {}",
        response.status()
    );

    // Upload object
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/{bucket}/{key}"))
                .header("content-type", content_type)
                .body(Body::from(data.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

/// Build an S3 SelectObjectContent XML request body.
fn build_select_xml(expression: &str, input_format: &str, output_format: &str) -> String {
    let input_serialization = match input_format {
        "CSV" => {
            r#"<InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization>"#
        }
        "CSV_NO_HEADER" => {
            r#"<InputSerialization><CSV><FileHeaderInfo>NONE</FileHeaderInfo></CSV></InputSerialization>"#
        }
        "JSON" => r#"<InputSerialization><JSON><Type>LINES</Type></JSON></InputSerialization>"#,
        _ => panic!("Unknown input format: {input_format}"),
    };

    let output_serialization = match output_format {
        "CSV" => r#"<OutputSerialization><CSV></CSV></OutputSerialization>"#,
        "JSON" => r#"<OutputSerialization><JSON></JSON></OutputSerialization>"#,
        _ => panic!("Unknown output format: {output_format}"),
    };

    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<SelectObjectContentRequest>
    <Expression>{expression}</Expression>
    <ExpressionType>SQL</ExpressionType>
    {input_serialization}
    {output_serialization}
</SelectObjectContentRequest>"#
    )
}

// ============================================================================
// S3 Select (SelectObjectContent) Tests
// ============================================================================

#[tokio::test]
async fn test_select_csv_basic() {
    let (storage, _temp) = create_test_storage().await;
    let state = create_test_state(storage, _temp.path()).await;
    let app = create_router(state);

    let csv_data = b"name,age,city\nAlice,30,NYC\nBob,25,LA\nCharlie,35,Chicago\n";
    setup_bucket_with_object(&app, "test-select", "data.csv", csv_data, "text/csv").await;

    let xml_body = build_select_xml("SELECT * FROM s3object", "CSV", "CSV");

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/test-select/data.csv?select&select-type=2")
                .header("content-type", "application/xml")
                .body(Body::from(xml_body))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        "application/octet-stream"
    );

    // Response is a binary event stream — verify it's non-empty
    let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
    assert!(
        !body_bytes.is_empty(),
        "Event stream response should not be empty"
    );

    // Verify event stream structure: first 4 bytes = total message length (big-endian u32)
    assert!(body_bytes.len() >= 16, "Event stream too short");
    let total_len =
        u32::from_be_bytes([body_bytes[0], body_bytes[1], body_bytes[2], body_bytes[3]]);
    assert!(total_len > 0, "First message length should be > 0");
}

#[tokio::test]
async fn test_select_csv_where_clause() {
    let (storage, _temp) = create_test_storage().await;
    let state = create_test_state(storage, _temp.path()).await;
    let app = create_router(state);

    let csv_data = b"name,age,city\nAlice,30,NYC\nBob,25,LA\nCharlie,35,Chicago\n";
    setup_bucket_with_object(&app, "test-where", "data.csv", csv_data, "text/csv").await;

    let xml_body = build_select_xml(
        "SELECT name, age FROM s3object WHERE age > 28",
        "CSV",
        "CSV",
    );

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/test-where/data.csv?select&select-type=2")
                .header("content-type", "application/xml")
                .body(Body::from(xml_body))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
    assert!(!body_bytes.is_empty());
}

#[tokio::test]
async fn test_select_csv_aggregate() {
    let (storage, _temp) = create_test_storage().await;
    let state = create_test_state(storage, _temp.path()).await;
    let app = create_router(state);

    let csv_data = b"name,age,city\nAlice,30,NYC\nBob,25,LA\nCharlie,35,Chicago\n";
    setup_bucket_with_object(&app, "test-agg", "data.csv", csv_data, "text/csv").await;

    let xml_body = build_select_xml("SELECT COUNT(*) FROM s3object", "CSV", "CSV");

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/test-agg/data.csv?select&select-type=2")
                .header("content-type", "application/xml")
                .body(Body::from(xml_body))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_select_json_basic() {
    let (storage, _temp) = create_test_storage().await;
    let state = create_test_state(storage, _temp.path()).await;
    let app = create_router(state);

    let json_data = b"{\"name\":\"Alice\",\"age\":30}\n{\"name\":\"Bob\",\"age\":25}\n";
    setup_bucket_with_object(
        &app,
        "test-json",
        "data.jsonl",
        json_data,
        "application/json",
    )
    .await;

    let xml_body = build_select_xml("SELECT * FROM s3object", "JSON", "JSON");

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/test-json/data.jsonl?select&select-type=2")
                .header("content-type", "application/xml")
                .body(Body::from(xml_body))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_select_nonexistent_object() {
    let (storage, _temp) = create_test_storage().await;
    let state = create_test_state(storage, _temp.path()).await;
    let app = create_router(state);

    // Create bucket but don't upload object
    let response = app
        .clone()
        .oneshot(Request::builder().method("PUT").uri("/test-noobj").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let xml_body = build_select_xml("SELECT * FROM s3object", "CSV", "CSV");

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/test-noobj/nonexistent.csv?select&select-type=2")
                .header("content-type", "application/xml")
                .body(Body::from(xml_body))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_select_invalid_sql() {
    let (storage, _temp) = create_test_storage().await;
    let state = create_test_state(storage, _temp.path()).await;
    let app = create_router(state);

    let csv_data = b"name,age\nAlice,30\n";
    setup_bucket_with_object(&app, "test-badsql", "data.csv", csv_data, "text/csv").await;

    // INSERT is not a valid SELECT statement
    let xml_body = build_select_xml("INSERT INTO s3object VALUES (1)", "CSV", "CSV");

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/test-badsql/data.csv?select&select-type=2")
                .header("content-type", "application/xml")
                .body(Body::from(xml_body))
                .unwrap(),
        )
        .await
        .unwrap();

    // Should return an error (400 Bad Request)
    assert_ne!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_select_invalid_xml_body() {
    let (storage, _temp) = create_test_storage().await;
    let state = create_test_state(storage, _temp.path()).await;
    let app = create_router(state);

    let csv_data = b"name,age\nAlice,30\n";
    setup_bucket_with_object(&app, "test-badxml", "data.csv", csv_data, "text/csv").await;

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/test-badxml/data.csv?select&select-type=2")
                .header("content-type", "application/xml")
                .body(Body::from("not xml at all"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

// ============================================================================
// S4 SQL (Multi-Object Query) Tests
// ============================================================================

#[tokio::test]
async fn test_sql_query_basic() {
    let (storage, _temp) = create_test_storage().await;
    let state = create_test_state(storage, _temp.path()).await;
    let app = create_router(state);

    let csv_data = b"name,age,city\nAlice,30,NYC\nBob,25,LA\n";
    setup_bucket_with_object(&app, "test-sql", "data.csv", csv_data, "text/csv").await;

    let json_body = serde_json::json!({
        "sql": "SELECT * FROM s3object",
        "format": "csv",
        "output": "json"
    });

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/test-sql?sql")
                .header("content-type", "application/json")
                .body(Body::from(json_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        "application/json"
    );

    let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
    let body_str = String::from_utf8(body_bytes.to_vec()).unwrap();
    assert!(
        body_str.contains("Alice"),
        "Response should contain 'Alice': {body_str}"
    );
}

#[tokio::test]
async fn test_sql_query_csv_output() {
    let (storage, _temp) = create_test_storage().await;
    let state = create_test_state(storage, _temp.path()).await;
    let app = create_router(state);

    let csv_data = b"name,age\nAlice,30\nBob,25\n";
    setup_bucket_with_object(&app, "test-sql-csv", "data.csv", csv_data, "text/csv").await;

    let json_body = serde_json::json!({
        "sql": "SELECT * FROM s3object",
        "format": "csv",
        "output": "csv"
    });

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/test-sql-csv?sql")
                .header("content-type", "application/json")
                .body(Body::from(json_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("content-type").unwrap(), "text/csv");
}

#[tokio::test]
async fn test_sql_query_json_input() {
    let (storage, _temp) = create_test_storage().await;
    let state = create_test_state(storage, _temp.path()).await;
    let app = create_router(state);

    let json_data = b"{\"name\":\"Alice\",\"score\":95}\n{\"name\":\"Bob\",\"score\":87}\n";
    setup_bucket_with_object(
        &app,
        "test-sql-json",
        "data.jsonl",
        json_data,
        "application/json",
    )
    .await;

    let json_body = serde_json::json!({
        "sql": "SELECT * FROM s3object WHERE score > 90",
        "format": "json",
        "output": "json"
    });

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/test-sql-json?sql")
                .header("content-type", "application/json")
                .body(Body::from(json_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
    let body_str = String::from_utf8(body_bytes.to_vec()).unwrap();
    assert!(
        body_str.contains("Alice"),
        "Should contain Alice (score 95 > 90)"
    );
}

#[tokio::test]
async fn test_sql_query_invalid_json_body() {
    let (storage, _temp) = create_test_storage().await;
    let state = create_test_state(storage, _temp.path()).await;
    let app = create_router(state);

    // Create bucket
    let _ = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/test-sql-bad")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/test-sql-bad?sql")
                .header("content-type", "application/json")
                .body(Body::from("not json"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_sql_query_unsupported_format() {
    let (storage, _temp) = create_test_storage().await;
    let state = create_test_state(storage, _temp.path()).await;
    let app = create_router(state);

    let csv_data = b"name,age\nAlice,30\n";
    setup_bucket_with_object(&app, "test-sql-fmt", "data.csv", csv_data, "text/csv").await;

    let json_body = serde_json::json!({
        "sql": "SELECT * FROM s3object",
        "format": "avro",
        "output": "json"
    });

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/test-sql-fmt?sql")
                .header("content-type", "application/json")
                .body(Body::from(json_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}
