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

//! Admin JWT signing secret.
//!
//! The admin API trusts the role inside a validly signed token, so whoever knows
//! the signing secret can mint a SuperUser token. These tests pin down that the
//! secret is never a value known in advance: without configuration every state
//! gets its own random secret, and a configured secret is the only one accepted.

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use s4_api::{create_router, AppState};
use s4_core::storage::BitcaskStorageEngine;
use s4_features::iam::{models::Role, JwtManager};
use tempfile::TempDir;
use tower::ServiceExt;

/// The fixed fallback secret older releases signed tokens with when
/// `S4_JWT_SECRET` was unset. It is public in the source history.
const OLD_DEFAULT_SECRET: &str = "default-jwt-secret-change-me-in-production-minimum-32-bytes";

const CONFIGURED_SECRET: &str = "configured-secret-for-tests-0123456789abcdef";

/// A fresh state with one SuperUser, `admin` / `password123`.
async fn state_with_admin() -> (AppState, TempDir) {
    let temp = TempDir::new().expect("Failed to create temp dir");
    let data_path = temp.path().join("volumes");
    std::fs::create_dir_all(&data_path).expect("Failed to create data dir");
    let storage = BitcaskStorageEngine::new(
        data_path,
        temp.path().join("metadata_db"),
        1024 * 1024 * 10,
        4096,
        false,
    )
    .await
    .expect("Failed to create storage engine");

    let state = AppState::new(
        storage,
        "test-access-key".to_string(),
        "test-secret-key".to_string(),
        temp.path(),
    )
    .await;
    state
        .iam_storage
        .create_user(
            "admin".to_string(),
            "password123".to_string(),
            Role::SuperUser,
        )
        .await
        .expect("Failed to create admin");

    (state, temp)
}

/// Signs a token for `admin` with `secret`, the way an attacker who knows the secret would.
async fn token_signed_with(state: &AppState, secret: &str) -> String {
    let admin = state
        .iam_storage
        .get_user_by_username("admin")
        .await
        .expect("Failed to read admin")
        .expect("admin exists");
    JwtManager::new(secret, 1).generate_token(&admin).expect("Failed to sign token")
}

/// Logs in as `admin` through the API and returns the issued token.
async fn login(state: &AppState) -> String {
    let body = serde_json::json!({ "username": "admin", "password": "password123" });
    let request = Request::builder()
        .method("POST")
        .uri("/api/admin/login")
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let response = create_router(state.clone()).oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK, "login should succeed");
    let bytes = http_body_util::BodyExt::collect(response.into_body()).await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    json["token"].as_str().expect("token in response").to_string()
}

/// Status of an admin-only request made with `token`.
async fn list_users_status(state: &AppState, token: &str) -> StatusCode {
    let request = Request::builder()
        .method("GET")
        .uri("/api/admin/users")
        .header("authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    create_router(state.clone()).oneshot(request).await.unwrap().status()
}

#[tokio::test]
async fn token_signed_with_the_old_default_secret_is_rejected() {
    let (state, _temp) = state_with_admin().await;

    let forged = token_signed_with(&state, OLD_DEFAULT_SECRET).await;

    assert_eq!(
        list_users_status(&state, &forged).await,
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn states_without_a_configured_secret_do_not_share_one() {
    let (first, _t1) = state_with_admin().await;
    let (second, _t2) = state_with_admin().await;

    let token = login(&first).await;

    assert_eq!(list_users_status(&first, &token).await, StatusCode::OK);
    assert_eq!(
        list_users_status(&second, &token).await,
        StatusCode::UNAUTHORIZED,
        "a token from one unconfigured state must not validate on another"
    );
}

#[tokio::test]
async fn configured_secret_signs_and_verifies_tokens() {
    let (state, _temp) = state_with_admin().await;
    let state = state.with_jwt_secret(CONFIGURED_SECRET);

    let issued = login(&state).await;
    JwtManager::new(CONFIGURED_SECRET, 1)
        .validate_token(&issued)
        .expect("login must sign with the configured secret");

    let external = token_signed_with(&state, CONFIGURED_SECRET).await;
    assert_eq!(list_users_status(&state, &external).await, StatusCode::OK);

    let foreign = token_signed_with(&state, OLD_DEFAULT_SECRET).await;
    assert_eq!(
        list_users_status(&state, &foreign).await,
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn nodes_sharing_a_configured_secret_accept_each_others_tokens() {
    let (first, _t1) = state_with_admin().await;
    let (second, _t2) = state_with_admin().await;
    let first = first.with_jwt_secret(CONFIGURED_SECRET);
    let second = second.with_jwt_secret(CONFIGURED_SECRET);

    let token = login(&first).await;

    assert_eq!(list_users_status(&second, &token).await, StatusCode::OK);
}
