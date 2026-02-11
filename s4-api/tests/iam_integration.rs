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

//! IAM Integration Tests
//!
//! Tests for IAM-based authentication and permission checks.
//! These tests verify that:
//! - Reader role can only perform GET/HEAD operations
//! - Writer role can perform GET/HEAD/PUT/POST/DELETE
//! - SuperUser role can do everything
//! - Users without proper permissions get 403 AccessDenied

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use chrono::Utc;
use hmac::{Hmac, Mac};
use http_body_util::BodyExt;
use s4_api::{create_router, AppState};
use s4_core::storage::BitcaskStorageEngine;
use s4_features::iam::models::Role;
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use tower::ServiceExt;

type HmacSha256 = Hmac<Sha256>;

/// Creates a test storage engine in a temporary directory.
async fn create_test_storage() -> (BitcaskStorageEngine, TempDir) {
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
// AWS Signature V4 Helpers
// ============================================================================

/// Signs a request using AWS Signature V4.
fn sign_request(
    method: &str,
    uri: &str,
    host: &str,
    access_key: &str,
    secret_key: &str,
    body: &[u8],
) -> (String, String, String) {
    let now = Utc::now();
    let timestamp = now.format("%Y%m%dT%H%M%SZ").to_string();
    let date = now.format("%Y%m%d").to_string();
    let region = "us-east-1";
    let service = "s3";

    // Calculate payload hash
    let payload_hash = hex::encode(Sha256::digest(body));

    // Create canonical request
    let canonical_uri = uri.split('?').next().unwrap_or("/");
    let canonical_query = uri.split('?').nth(1).unwrap_or("");

    let signed_headers = "host;x-amz-content-sha256;x-amz-date";
    let canonical_headers = format!(
        "host:{}\nx-amz-content-sha256:{}\nx-amz-date:{}\n",
        host, payload_hash, timestamp
    );

    let canonical_request = format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        method, canonical_uri, canonical_query, canonical_headers, signed_headers, payload_hash
    );

    // Create string to sign
    let credential_scope = format!("{}/{}/{}/aws4_request", date, region, service);
    let hashed_request = hex::encode(Sha256::digest(canonical_request.as_bytes()));
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{}\n{}\n{}",
        timestamp, credential_scope, hashed_request
    );

    // Calculate signing key
    let k_secret = format!("AWS4{}", secret_key);
    let k_date = hmac_sha256(k_secret.as_bytes(), date.as_bytes());
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, service.as_bytes());
    let k_signing = hmac_sha256(&k_service, b"aws4_request");

    // Calculate signature
    let signature = hex::encode(hmac_sha256(&k_signing, string_to_sign.as_bytes()));

    // Create Authorization header
    let auth_header = format!(
        "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
        access_key, credential_scope, signed_headers, signature
    );

    (auth_header, timestamp, payload_hash)
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC can take key of any size");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

// ============================================================================
// Test Helpers
// ============================================================================

/// Creates a user and generates S3 credentials.
async fn create_user_with_credentials(
    state: &AppState,
    username: &str,
    password: &str,
    role: Role,
) -> (String, String) {
    // Create user
    let user = state
        .iam_storage
        .create_user(username.to_string(), password.to_string(), role)
        .await
        .expect("Failed to create user");

    // Generate S3 credentials
    let credentials = state
        .iam_storage
        .generate_s3_credentials(&user.id)
        .await
        .expect("Failed to generate credentials");

    (credentials.access_key, credentials.secret_key)
}

/// Builds and signs a request.
fn build_signed_request(
    method: &str,
    uri: &str,
    access_key: &str,
    secret_key: &str,
    body: Vec<u8>,
) -> Request<Body> {
    let host = "localhost:9000";
    let (auth_header, timestamp, payload_hash) =
        sign_request(method, uri, host, access_key, secret_key, &body);

    Request::builder()
        .method(method)
        .uri(uri)
        .header("host", host)
        .header("x-amz-date", timestamp)
        .header("x-amz-content-sha256", payload_hash)
        .header("authorization", auth_header)
        .body(Body::from(body))
        .unwrap()
}

// ============================================================================
// Reader Permission Tests
// ============================================================================

#[tokio::test]
async fn test_reader_can_list_buckets() {
    let (storage, _temp) = create_test_storage().await;
    let state = create_test_state(storage);

    // Create a reader user
    let (access_key, secret_key) =
        create_user_with_credentials(&state, "reader_user", "password123", Role::Reader).await;

    let app = create_router(state);

    let request = build_signed_request("GET", "/", &access_key, &secret_key, vec![]);
    let response = app.oneshot(request).await.unwrap();

    assert_eq!(
        response.status(),
        StatusCode::OK,
        "Reader should be able to list buckets"
    );
}

#[tokio::test]
async fn test_reader_can_head_bucket() {
    let (storage, _temp) = create_test_storage().await;
    let state = create_test_state(storage);

    // First create a bucket as a writer
    let (writer_ak, writer_sk) =
        create_user_with_credentials(&state, "writer_for_reader", "password123", Role::Writer)
            .await;

    let app = create_router(state.clone());
    let request = build_signed_request("PUT", "/test-bucket", &writer_ak, &writer_sk, vec![]);
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Now test reader can HEAD the bucket
    let (reader_ak, reader_sk) =
        create_user_with_credentials(&state, "reader_head", "password123", Role::Reader).await;

    let app = create_router(state);
    let request = build_signed_request("HEAD", "/test-bucket", &reader_ak, &reader_sk, vec![]);
    let response = app.oneshot(request).await.unwrap();

    assert_eq!(
        response.status(),
        StatusCode::OK,
        "Reader should be able to HEAD bucket"
    );
}

#[tokio::test]
async fn test_reader_cannot_create_bucket() {
    let (storage, _temp) = create_test_storage().await;
    let state = create_test_state(storage);

    // Create a reader user
    let (access_key, secret_key) =
        create_user_with_credentials(&state, "reader_create", "password123", Role::Reader).await;

    let app = create_router(state);

    let request = build_signed_request("PUT", "/new-bucket", &access_key, &secret_key, vec![]);
    let response = app.oneshot(request).await.unwrap();

    assert_eq!(
        response.status(),
        StatusCode::FORBIDDEN,
        "Reader should NOT be able to create bucket"
    );

    let body = body_to_string(response.into_body()).await;
    assert!(body.contains("AccessDenied"));
}

#[tokio::test]
async fn test_reader_cannot_put_object() {
    let (storage, _temp) = create_test_storage().await;
    let state = create_test_state(storage);

    // First create a bucket as a writer
    let (writer_ak, writer_sk) =
        create_user_with_credentials(&state, "writer_for_put", "password123", Role::Writer).await;

    let app = create_router(state.clone());
    let request = build_signed_request("PUT", "/test-put-bucket", &writer_ak, &writer_sk, vec![]);
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Now test reader cannot PUT object
    let (reader_ak, reader_sk) =
        create_user_with_credentials(&state, "reader_put", "password123", Role::Reader).await;

    let app = create_router(state);
    let request = build_signed_request(
        "PUT",
        "/test-put-bucket/test-object",
        &reader_ak,
        &reader_sk,
        b"test content".to_vec(),
    );
    let response = app.oneshot(request).await.unwrap();

    assert_eq!(
        response.status(),
        StatusCode::FORBIDDEN,
        "Reader should NOT be able to PUT object"
    );
}

#[tokio::test]
async fn test_reader_cannot_delete_bucket() {
    let (storage, _temp) = create_test_storage().await;
    let state = create_test_state(storage);

    // First create a bucket as a writer
    let (writer_ak, writer_sk) =
        create_user_with_credentials(&state, "writer_delete", "password123", Role::Writer).await;

    let app = create_router(state.clone());
    let request = build_signed_request("PUT", "/bucket-to-delete", &writer_ak, &writer_sk, vec![]);
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Now test reader cannot DELETE bucket
    let (reader_ak, reader_sk) =
        create_user_with_credentials(&state, "reader_del", "password123", Role::Reader).await;

    let app = create_router(state);
    let request = build_signed_request(
        "DELETE",
        "/bucket-to-delete",
        &reader_ak,
        &reader_sk,
        vec![],
    );
    let response = app.oneshot(request).await.unwrap();

    assert_eq!(
        response.status(),
        StatusCode::FORBIDDEN,
        "Reader should NOT be able to DELETE bucket"
    );
}

// ============================================================================
// Writer Permission Tests
// ============================================================================

#[tokio::test]
async fn test_writer_can_create_bucket() {
    let (storage, _temp) = create_test_storage().await;
    let state = create_test_state(storage);

    let (access_key, secret_key) =
        create_user_with_credentials(&state, "writer_create", "password123", Role::Writer).await;

    let app = create_router(state);

    let request = build_signed_request("PUT", "/writer-bucket", &access_key, &secret_key, vec![]);
    let response = app.oneshot(request).await.unwrap();

    assert_eq!(
        response.status(),
        StatusCode::OK,
        "Writer should be able to create bucket"
    );
}

#[tokio::test]
async fn test_writer_can_put_and_get_object() {
    let (storage, _temp) = create_test_storage().await;
    let state = create_test_state(storage);

    let (access_key, secret_key) =
        create_user_with_credentials(&state, "writer_putget", "password123", Role::Writer).await;

    // Create bucket
    let app = create_router(state.clone());
    let request = build_signed_request("PUT", "/rw-bucket", &access_key, &secret_key, vec![]);
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // PUT object
    let app = create_router(state.clone());
    let request = build_signed_request(
        "PUT",
        "/rw-bucket/test.txt",
        &access_key,
        &secret_key,
        b"Hello World".to_vec(),
    );
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "Writer should be able to PUT object"
    );

    // GET object
    let app = create_router(state);
    let request = build_signed_request(
        "GET",
        "/rw-bucket/test.txt",
        &access_key,
        &secret_key,
        vec![],
    );
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "Writer should be able to GET object"
    );

    let body = body_to_string(response.into_body()).await;
    assert_eq!(body, "Hello World");
}

#[tokio::test]
async fn test_writer_can_delete_object() {
    let (storage, _temp) = create_test_storage().await;
    let state = create_test_state(storage);

    let (access_key, secret_key) =
        create_user_with_credentials(&state, "writer_del_obj", "password123", Role::Writer).await;

    // Create bucket
    let app = create_router(state.clone());
    let request = build_signed_request("PUT", "/del-obj-bucket", &access_key, &secret_key, vec![]);
    app.oneshot(request).await.unwrap();

    // PUT object
    let app = create_router(state.clone());
    let request = build_signed_request(
        "PUT",
        "/del-obj-bucket/to-delete.txt",
        &access_key,
        &secret_key,
        b"delete me".to_vec(),
    );
    app.oneshot(request).await.unwrap();

    // DELETE object
    let app = create_router(state.clone());
    let request = build_signed_request(
        "DELETE",
        "/del-obj-bucket/to-delete.txt",
        &access_key,
        &secret_key,
        vec![],
    );
    let response = app.oneshot(request).await.unwrap();

    assert_eq!(
        response.status(),
        StatusCode::NO_CONTENT,
        "Writer should be able to DELETE object"
    );
}

// ============================================================================
// SuperUser Permission Tests
// ============================================================================

#[tokio::test]
async fn test_superuser_can_do_everything() {
    let (storage, _temp) = create_test_storage().await;
    let state = create_test_state(storage);

    let (access_key, secret_key) =
        create_user_with_credentials(&state, "superuser", "password123", Role::SuperUser).await;

    // Create bucket
    let app = create_router(state.clone());
    let request = build_signed_request("PUT", "/super-bucket", &access_key, &secret_key, vec![]);
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // PUT object
    let app = create_router(state.clone());
    let request = build_signed_request(
        "PUT",
        "/super-bucket/test.txt",
        &access_key,
        &secret_key,
        b"test".to_vec(),
    );
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // GET object
    let app = create_router(state.clone());
    let request = build_signed_request(
        "GET",
        "/super-bucket/test.txt",
        &access_key,
        &secret_key,
        vec![],
    );
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // DELETE object
    let app = create_router(state.clone());
    let request = build_signed_request(
        "DELETE",
        "/super-bucket/test.txt",
        &access_key,
        &secret_key,
        vec![],
    );
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    // DELETE bucket
    let app = create_router(state);
    let request = build_signed_request("DELETE", "/super-bucket", &access_key, &secret_key, vec![]);
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
}

// ============================================================================
// Edge Cases
// ============================================================================

#[tokio::test]
async fn test_invalid_access_key_rejected() {
    let (storage, _temp) = create_test_storage().await;
    let state = create_test_state(storage);
    let app = create_router(state);

    let request = build_signed_request("GET", "/", "INVALID_KEY", "invalid_secret", vec![]);
    let response = app.oneshot(request).await.unwrap();

    assert_eq!(
        response.status(),
        StatusCode::FORBIDDEN,
        "Invalid access key should be rejected"
    );
}

#[tokio::test]
async fn test_wrong_signature_rejected() {
    let (storage, _temp) = create_test_storage().await;
    let state = create_test_state(storage);

    let (access_key, _secret_key) =
        create_user_with_credentials(&state, "wrong_sig", "password123", Role::Writer).await;

    let app = create_router(state);

    // Use correct access key but wrong secret (results in wrong signature)
    let request = build_signed_request("GET", "/", &access_key, "wrong_secret_key", vec![]);
    let response = app.oneshot(request).await.unwrap();

    assert_eq!(
        response.status(),
        StatusCode::FORBIDDEN,
        "Wrong signature should be rejected"
    );
}

#[tokio::test]
async fn test_reader_can_get_object() {
    let (storage, _temp) = create_test_storage().await;
    let state = create_test_state(storage);

    // Create bucket and object as writer
    let (writer_ak, writer_sk) =
        create_user_with_credentials(&state, "writer_obj", "password123", Role::Writer).await;

    let app = create_router(state.clone());
    let request = build_signed_request("PUT", "/reader-get-bucket", &writer_ak, &writer_sk, vec![]);
    app.oneshot(request).await.unwrap();

    let app = create_router(state.clone());
    let request = build_signed_request(
        "PUT",
        "/reader-get-bucket/readable.txt",
        &writer_ak,
        &writer_sk,
        b"readable content".to_vec(),
    );
    app.oneshot(request).await.unwrap();

    // Now reader should be able to GET the object
    let (reader_ak, reader_sk) =
        create_user_with_credentials(&state, "reader_get", "password123", Role::Reader).await;

    let app = create_router(state);
    let request = build_signed_request(
        "GET",
        "/reader-get-bucket/readable.txt",
        &reader_ak,
        &reader_sk,
        vec![],
    );
    let response = app.oneshot(request).await.unwrap();

    assert_eq!(
        response.status(),
        StatusCode::OK,
        "Reader should be able to GET object"
    );

    let body = body_to_string(response.into_body()).await;
    assert_eq!(body, "readable content");
}
