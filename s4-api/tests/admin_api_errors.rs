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

//! Admin API error-response tests.
//!
//! The admin API is what the web console talks to, so every failure it can
//! produce must arrive as the right HTTP status and a stable `code` field.
//! These tests pin that contract down: a caller's own mistake answers 4xx and
//! names itself, and 5xx is reserved for genuine server faults.

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use http_body_util::BodyExt;
use s4_api::{create_router, AppState};
use s4_core::storage::{BitcaskStorageEngine, StorageEngine};
use s4_features::iam::models::Role;
use std::collections::HashMap;
use tempfile::TempDir;
use tower::ServiceExt;

/// Creates a test storage engine in a temporary directory.
async fn create_test_storage() -> (BitcaskStorageEngine, TempDir) {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let data_path = temp_dir.path().join("volumes");
    let metadata_path = temp_dir.path().join("metadata_db");

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
async fn create_test_state(storage: BitcaskStorageEngine, data_dir: &std::path::Path) -> AppState {
    AppState::new(
        storage,
        "test-access-key".to_string(),
        "test-secret-key".to_string(),
        data_dir,
    )
    .await
}

/// A state with one SuperUser ("admin") and one Reader ("viewer").
async fn state_with_users() -> (AppState, TempDir) {
    let (storage, temp) = create_test_storage().await;
    let state = create_test_state(storage, temp.path()).await;

    for (name, role) in [("admin", Role::SuperUser), ("viewer", Role::Reader)] {
        state
            .iam_storage
            .create_user(name.to_string(), "password123".to_string(), role)
            .await
            .expect("Failed to create user");
    }

    (state, temp)
}

/// Sends a request and returns its status plus the parsed JSON body.
///
/// A body that is not JSON (an empty 204, for instance) comes back as `Null`.
async fn send(state: &AppState, request: Request<Body>) -> (StatusCode, serde_json::Value) {
    let response = create_router(state.clone()).oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let json = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
    (status, json)
}

/// Logs in and returns the JWT, panicking if the login did not succeed.
async fn login_token(state: &AppState, username: &str, password: &str) -> String {
    let body = serde_json::json!({ "username": username, "password": password });
    let request = Request::builder()
        .method("POST")
        .uri("/api/admin/login")
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();

    let (status, json) = send(state, request).await;
    assert_eq!(status, StatusCode::OK, "login should succeed: {json}");
    json["token"].as_str().expect("token in response").to_string()
}

/// Builds an authenticated admin request.
fn admin_request(
    method: &str,
    uri: &str,
    token: &str,
    body: Option<serde_json::Value>,
) -> Request<Body> {
    let builder = Request::builder()
        .method(method)
        .uri(uri)
        .header("authorization", format!("Bearer {token}"));

    match body {
        Some(json) => builder
            .header("content-type", "application/json")
            .body(Body::from(json.to_string()))
            .unwrap(),
        None => builder.body(Body::empty()).unwrap(),
    }
}

/// Asserts the response carries the expected status and `code`.
fn assert_error(
    status: StatusCode,
    json: &serde_json::Value,
    expected_status: StatusCode,
    expected_code: &str,
) {
    assert_eq!(status, expected_status, "unexpected status, body: {json}");
    assert_eq!(
        json["code"].as_str(),
        Some(expected_code),
        "unexpected error code, body: {json}"
    );
    assert!(
        json["error"].as_str().is_some_and(|m| !m.is_empty()),
        "error message must not be empty, body: {json}"
    );
}

/// Creates a bucket through the admin API, asserting it worked.
async fn create_bucket(state: &AppState, token: &str, name: &str) {
    let (status, json) = send(
        state,
        admin_request("PUT", &format!("/api/admin/buckets/{name}"), token, None),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "bucket creation failed: {json}");
}

/// Writes one object straight into storage, bypassing the S3 signing path.
async fn put_object(state: &AppState, bucket: &str, key: &str) {
    state
        .storage
        .read()
        .await
        .put_object(bucket, key, b"payload", "text/plain", &HashMap::new())
        .await
        .expect("Failed to store object");
}

// ============================================================================
// Login
// ============================================================================

#[tokio::test]
async fn wrong_password_is_401_not_500() {
    let (state, _temp) = state_with_users().await;

    let body = serde_json::json!({ "username": "admin", "password": "not-the-password" });
    let request = Request::builder()
        .method("POST")
        .uri("/api/admin/login")
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();

    let (status, json) = send(&state, request).await;
    assert_error(
        status,
        &json,
        StatusCode::UNAUTHORIZED,
        "InvalidCredentials",
    );
}

#[tokio::test]
async fn unknown_account_answers_exactly_like_a_wrong_password() {
    let (state, _temp) = state_with_users().await;

    // Telling the two apart would let an unauthenticated caller enumerate
    // account names, so status, code and message must be identical.
    let mut answers = Vec::new();
    for username in ["admin", "no-such-account"] {
        let body = serde_json::json!({ "username": username, "password": "not-the-password" });
        let request = Request::builder()
            .method("POST")
            .uri("/api/admin/login")
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap();
        answers.push(send(&state, request).await);
    }

    assert_error(
        answers[1].0,
        &answers[1].1,
        StatusCode::UNAUTHORIZED,
        "InvalidCredentials",
    );
    assert_eq!(answers[0].0, answers[1].0);
    assert_eq!(answers[0].1, answers[1].1);
}

#[tokio::test]
async fn correct_password_still_returns_a_token() {
    let (state, _temp) = state_with_users().await;
    let token = login_token(&state, "admin", "password123").await;
    assert!(!token.is_empty());
}

// ============================================================================
// Authentication and authorisation
// ============================================================================

#[tokio::test]
async fn missing_and_malformed_tokens_are_401() {
    let (state, _temp) = state_with_users().await;

    // No header at all, a header that is not Bearer, and a Bearer token that
    // does not validate are all the same failure — the caller has to log in —
    // so none of them may answer 400.
    let no_header = Request::builder()
        .method("GET")
        .uri("/api/admin/users")
        .body(Body::empty())
        .unwrap();
    let (status, json) = send(&state, no_header).await;
    assert_error(status, &json, StatusCode::UNAUTHORIZED, "Unauthorized");

    let wrong_scheme = Request::builder()
        .method("GET")
        .uri("/api/admin/users")
        .header("authorization", "Basic dXNlcjpwYXNz")
        .body(Body::empty())
        .unwrap();
    let (status, json) = send(&state, wrong_scheme).await;
    assert_error(status, &json, StatusCode::UNAUTHORIZED, "Unauthorized");

    let (status, json) = send(
        &state,
        admin_request("GET", "/api/admin/users", "not-a-jwt", None),
    )
    .await;
    assert_error(status, &json, StatusCode::UNAUTHORIZED, "Unauthorized");
}

#[tokio::test]
async fn non_admin_role_is_403() {
    let (state, _temp) = state_with_users().await;
    let token = login_token(&state, "viewer", "password123").await;

    let (status, json) = send(
        &state,
        admin_request("GET", "/api/admin/users", &token, None),
    )
    .await;
    assert_error(status, &json, StatusCode::FORBIDDEN, "Forbidden");
}

// ============================================================================
// User management
// ============================================================================

#[tokio::test]
async fn duplicate_user_is_409_not_500() {
    let (state, _temp) = state_with_users().await;
    let token = login_token(&state, "admin", "password123").await;

    let body = serde_json::json!({
        "username": "admin",
        "password": "password123",
        "role": "Reader",
    });
    let (status, json) = send(
        &state,
        admin_request("POST", "/api/admin/users", &token, Some(body)),
    )
    .await;

    assert_error(status, &json, StatusCode::CONFLICT, "UserAlreadyExists");
}

#[tokio::test]
async fn invalid_username_is_400_not_500() {
    let (state, _temp) = state_with_users().await;
    let token = login_token(&state, "admin", "password123").await;

    let body = serde_json::json!({
        "username": "a!",
        "password": "password123",
        "role": "Reader",
    });
    let (status, json) = send(
        &state,
        admin_request("POST", "/api/admin/users", &token, Some(body)),
    )
    .await;

    assert_error(status, &json, StatusCode::BAD_REQUEST, "InvalidUsername");
}

#[tokio::test]
async fn unknown_user_id_is_404() {
    let (state, _temp) = state_with_users().await;
    let token = login_token(&state, "admin", "password123").await;

    let (status, json) = send(
        &state,
        admin_request(
            "GET",
            "/api/admin/users/00000000-0000-0000-0000-000000000000",
            &token,
            None,
        ),
    )
    .await;

    assert_error(status, &json, StatusCode::NOT_FOUND, "UserNotFound");
}

// ============================================================================
// Buckets
// ============================================================================

#[tokio::test]
async fn deleting_a_non_empty_bucket_is_409_bucket_not_empty() {
    let (state, _temp) = state_with_users().await;
    let token = login_token(&state, "admin", "password123").await;

    create_bucket(&state, &token, "reports").await;
    put_object(&state, "reports", "q3.csv").await;

    let (status, json) = send(
        &state,
        admin_request("DELETE", "/api/admin/buckets/reports", &token, None),
    )
    .await;

    assert_error(status, &json, StatusCode::CONFLICT, "BucketNotEmpty");

    // The message has to tell the operator how to proceed.
    let message = json["error"].as_str().unwrap();
    assert!(
        message.contains("reports") && message.contains("force=true"),
        "message should name the bucket and the force option, got: {message}"
    );
}

#[tokio::test]
async fn force_delete_removes_a_non_empty_bucket() {
    let (state, _temp) = state_with_users().await;
    let token = login_token(&state, "admin", "password123").await;

    create_bucket(&state, &token, "reports").await;
    put_object(&state, "reports", "q3.csv").await;

    let (status, _) = send(
        &state,
        admin_request(
            "DELETE",
            "/api/admin/buckets/reports?force=true",
            &token,
            None,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    // And the bucket really is gone afterwards.
    let (status, json) = send(
        &state,
        admin_request("DELETE", "/api/admin/buckets/reports", &token, None),
    )
    .await;
    assert_error(status, &json, StatusCode::NOT_FOUND, "NoSuchBucket");
}

#[tokio::test]
async fn deleting_a_missing_bucket_is_404_not_user_not_found() {
    let (state, _temp) = state_with_users().await;
    let token = login_token(&state, "admin", "password123").await;

    let (status, json) = send(
        &state,
        admin_request("DELETE", "/api/admin/buckets/ghost", &token, None),
    )
    .await;

    assert_error(status, &json, StatusCode::NOT_FOUND, "NoSuchBucket");
    assert!(
        !json["error"].as_str().unwrap().contains("User"),
        "a missing bucket must not be reported as a missing user: {json}"
    );
}

#[tokio::test]
async fn creating_an_existing_bucket_is_409() {
    let (state, _temp) = state_with_users().await;
    let token = login_token(&state, "admin", "password123").await;

    create_bucket(&state, &token, "reports").await;

    let (status, json) = send(
        &state,
        admin_request("PUT", "/api/admin/buckets/reports", &token, None),
    )
    .await;

    assert_error(status, &json, StatusCode::CONFLICT, "BucketAlreadyExists");
}

#[tokio::test]
async fn invalid_bucket_name_is_rejected_the_same_way_as_on_the_s3_path() {
    let (state, _temp) = state_with_users().await;
    let token = login_token(&state, "admin", "password123").await;

    // Uppercase is not a legal S3 bucket name; accepting it here would create a
    // bucket the S3 API could never address.
    let (status, json) = send(
        &state,
        admin_request("PUT", "/api/admin/buckets/NotValid", &token, None),
    )
    .await;

    assert_error(status, &json, StatusCode::BAD_REQUEST, "InvalidBucketName");
}

#[tokio::test]
async fn listing_a_missing_bucket_is_404_rather_than_an_empty_list() {
    let (state, _temp) = state_with_users().await;
    let token = login_token(&state, "admin", "password123").await;

    let (status, json) = send(
        &state,
        admin_request("GET", "/api/admin/buckets/ghost/objects", &token, None),
    )
    .await;

    assert_error(status, &json, StatusCode::NOT_FOUND, "NoSuchBucket");
}

#[tokio::test]
async fn listing_an_empty_bucket_is_200_with_no_objects() {
    let (state, _temp) = state_with_users().await;
    let token = login_token(&state, "admin", "password123").await;

    create_bucket(&state, &token, "empty-bucket").await;

    let (status, json) = send(
        &state,
        admin_request(
            "GET",
            "/api/admin/buckets/empty-bucket/objects",
            &token,
            None,
        ),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "body: {json}");
    assert_eq!(
        json["objects"].as_array().map(|a| a.len()),
        Some(0),
        "an existing but empty bucket lists nothing: {json}"
    );
}

// ============================================================================
// Cluster routes on a single node
// ============================================================================

#[tokio::test]
async fn drain_on_a_single_node_asks_for_cluster_mode_and_starts_nothing() {
    let (state, _temp) = state_with_users().await;
    let token = login_token(&state, "admin", "password123").await;

    let (status, json) = send(
        &state,
        admin_request("POST", "/api/admin/node/drain", &token, None),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST, "body: {json}");
    assert!(
        json["error"].as_str().is_some_and(|m| m.contains("cluster mode")),
        "body: {json}"
    );
    assert!(
        json.get("status").is_none(),
        "nothing was initiated: {json}"
    );
}

#[tokio::test]
async fn drain_needs_a_superuser() {
    let (state, _temp) = state_with_users().await;
    let token = login_token(&state, "viewer", "password123").await;

    let (status, json) = send(
        &state,
        admin_request("POST", "/api/admin/node/drain", &token, None),
    )
    .await;

    assert_eq!(status, StatusCode::FORBIDDEN, "body: {json}");
}
