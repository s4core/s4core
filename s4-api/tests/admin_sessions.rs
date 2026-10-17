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

//! Admin sessions are references to an account, not copies of it.
//!
//! A token is accepted only while its account exists, is active and keeps the
//! credential version the token was issued under, and the role comes from the
//! account at every request. So deleting, deactivating, demoting a user,
//! changing their password, revoking their sessions or their logout ends the
//! tokens they hold at once. Root is configured by the environment: it is not
//! stored, its id is the same everywhere, and a new password ends its earlier
//! sessions.

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use jsonwebtoken::{encode, EncodingKey, Header};
use s4_api::{create_router, AppState};
use s4_core::storage::BitcaskStorageEngine;
use s4_features::iam::{models::Role, JwtManager, RootAccount};
use serde_json::{json, Value};
use tempfile::TempDir;
use tower::ServiceExt;

const SECRET: &str = "admin-sessions-secret-0123456789abcdef";
const ROOT_PASSWORD: &str = "root-password-123";

/// A state on the storage in `temp`, signing with [`SECRET`], with root
/// `root` / `root_password` when given.
async fn state_on(temp: &TempDir, root_password: Option<&str>) -> AppState {
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

    let mut state = AppState::new(
        storage,
        "test-access-key".to_string(),
        "test-secret-key".to_string(),
        temp.path(),
    )
    .await
    .with_jwt_secret(SECRET);
    if let Some(password) = root_password {
        state = state.with_root_account(RootAccount::new("root", password, SECRET).unwrap());
        state.auth_service.start_root().await.expect("start root");
    }
    state
}

async fn fresh_state() -> (AppState, TempDir) {
    let temp = TempDir::new().expect("Failed to create temp dir");
    let state = state_on(&temp, Some(ROOT_PASSWORD)).await;
    (state, temp)
}

/// Sends one admin request and returns its status and JSON body.
async fn call(
    state: &AppState,
    method: &str,
    path: &str,
    token: Option<&str>,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let mut request = Request::builder().method(method).uri(path);
    if let Some(token) = token {
        request = request.header("authorization", format!("Bearer {token}"));
    }
    let request = match body {
        Some(body) => request
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap(),
        None => request.body(Body::empty()).unwrap(),
    };
    let response = create_router(state.clone()).oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = http_body_util::BodyExt::collect(response.into_body()).await.unwrap().to_bytes();
    let json = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, json)
}

async fn login(state: &AppState, username: &str, password: &str) -> Result<String, StatusCode> {
    let (status, body) = call(
        state,
        "POST",
        "/api/admin/login",
        None,
        Some(json!({ "username": username, "password": password })),
    )
    .await;
    if status == StatusCode::OK {
        Ok(body["token"].as_str().expect("token in response").to_string())
    } else {
        Err(status)
    }
}

async fn root_token(state: &AppState) -> String {
    login(state, "root", ROOT_PASSWORD).await.expect("root logs in")
}

/// Creates `username` with `role` through root and returns its id.
async fn create(state: &AppState, username: &str, password: &str, role: &str) -> String {
    let root = root_token(state).await;
    let (status, body) = call(
        state,
        "POST",
        "/api/admin/users",
        Some(&root),
        Some(json!({ "username": username, "password": password, "role": role })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "create {username}: {body}");
    body["id"].as_str().unwrap().to_string()
}

/// Status of an admin-only request (the user list) made with `token`.
async fn list_status(state: &AppState, token: &str) -> StatusCode {
    call(state, "GET", "/api/admin/users", Some(token), None).await.0
}

/// Changes `user_id` through root.
async fn update(state: &AppState, user_id: &str, change: Value) {
    let root = root_token(state).await;
    let (status, body) = call(
        state,
        "PUT",
        &format!("/api/admin/users/{user_id}"),
        Some(&root),
        Some(change),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "update: {body}");
}

#[tokio::test]
async fn deleting_a_user_ends_their_sessions() {
    let (state, _temp) = fresh_state().await;
    let id = create(&state, "admin_one", "pass-12345", "SuperUser").await;
    let token = login(&state, "admin_one", "pass-12345").await.unwrap();
    assert_eq!(list_status(&state, &token).await, StatusCode::OK);

    let root = root_token(&state).await;
    let (status, _) = call(
        &state,
        "DELETE",
        &format!("/api/admin/users/{id}"),
        Some(&root),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    assert_eq!(list_status(&state, &token).await, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn deactivating_a_user_ends_their_sessions() {
    let (state, _temp) = fresh_state().await;
    let id = create(&state, "admin_two", "pass-12345", "SuperUser").await;
    let token = login(&state, "admin_two", "pass-12345").await.unwrap();

    update(&state, &id, json!({ "is_active": false })).await;
    assert_eq!(list_status(&state, &token).await, StatusCode::UNAUTHORIZED);

    // Active again, the old token stays dead: deactivation moved the version on.
    update(&state, &id, json!({ "is_active": true })).await;
    assert_eq!(list_status(&state, &token).await, StatusCode::UNAUTHORIZED);
    let fresh = login(&state, "admin_two", "pass-12345").await.unwrap();
    assert_eq!(list_status(&state, &fresh).await, StatusCode::OK);
}

#[tokio::test]
async fn demoting_a_user_ends_their_sessions_and_the_new_one_has_the_new_role() {
    let (state, _temp) = fresh_state().await;
    let id = create(&state, "admin_three", "pass-12345", "SuperUser").await;
    let token = login(&state, "admin_three", "pass-12345").await.unwrap();

    update(&state, &id, json!({ "role": "Reader" })).await;

    assert_eq!(list_status(&state, &token).await, StatusCode::UNAUTHORIZED);
    let as_reader = login(&state, "admin_three", "pass-12345").await.unwrap();
    assert_eq!(list_status(&state, &as_reader).await, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn a_change_that_changes_nothing_keeps_the_sessions() {
    let (state, _temp) = fresh_state().await;
    let id = create(&state, "admin_four", "pass-12345", "SuperUser").await;
    let token = login(&state, "admin_four", "pass-12345").await.unwrap();

    update(
        &state,
        &id,
        json!({ "role": "SuperUser", "is_active": true }),
    )
    .await;

    assert_eq!(list_status(&state, &token).await, StatusCode::OK);
}

#[tokio::test]
async fn a_new_password_ends_the_sessions_of_the_old_one() {
    let (state, _temp) = fresh_state().await;
    let id = create(&state, "admin_five", "old-pass-123", "SuperUser").await;
    let token = login(&state, "admin_five", "old-pass-123").await.unwrap();

    update(&state, &id, json!({ "password": "new-pass-456" })).await;

    assert_eq!(list_status(&state, &token).await, StatusCode::UNAUTHORIZED);
    assert_eq!(
        login(&state, "admin_five", "old-pass-123").await.unwrap_err(),
        StatusCode::UNAUTHORIZED
    );
    let fresh = login(&state, "admin_five", "new-pass-456").await.unwrap();
    assert_eq!(list_status(&state, &fresh).await, StatusCode::OK);
}

#[tokio::test]
async fn revoking_sessions_ends_them_and_keeps_the_account() {
    let (state, _temp) = fresh_state().await;
    let id = create(&state, "admin_six", "pass-12345", "SuperUser").await;
    let first = login(&state, "admin_six", "pass-12345").await.unwrap();
    let second = login(&state, "admin_six", "pass-12345").await.unwrap();

    let root = root_token(&state).await;
    let (status, _) = call(
        &state,
        "POST",
        &format!("/api/admin/users/{id}/sessions/revoke"),
        Some(&root),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    assert_eq!(list_status(&state, &first).await, StatusCode::UNAUTHORIZED);
    assert_eq!(list_status(&state, &second).await, StatusCode::UNAUTHORIZED);
    let fresh = login(&state, "admin_six", "pass-12345").await.unwrap();
    assert_eq!(list_status(&state, &fresh).await, StatusCode::OK);
    assert_eq!(
        list_status(&state, &root).await,
        StatusCode::OK,
        "root's session is not touched"
    );
}

#[tokio::test]
async fn revoking_needs_superuser_and_an_existing_account() {
    let (state, _temp) = fresh_state().await;
    let reader = create(&state, "reader_one", "pass-12345", "Reader").await;
    let token = login(&state, "reader_one", "pass-12345").await.unwrap();

    let (status, _) = call(
        &state,
        "POST",
        &format!("/api/admin/users/{reader}/sessions/revoke"),
        Some(&token),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    let root = root_token(&state).await;
    let (status, _) = call(
        &state,
        "POST",
        "/api/admin/users/no-such-user/sessions/revoke",
        Some(&root),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn logout_ends_the_callers_sessions() {
    let (state, _temp) = fresh_state().await;
    create(&state, "reader_two", "pass-12345", "Reader").await;
    let token = login(&state, "reader_two", "pass-12345").await.unwrap();

    let (status, _) = call(&state, "POST", "/api/admin/logout", Some(&token), None).await;
    assert_eq!(status, StatusCode::NO_CONTENT, "any account may log out");

    let (status, _) = call(&state, "POST", "/api/admin/logout", Some(&token), None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn the_role_inside_a_token_grants_nothing() {
    let (state, _temp) = fresh_state().await;
    create(&state, "reader_three", "pass-12345", "Reader").await;
    let reader = state.iam_storage.get_user_by_username("reader_three").await.unwrap().unwrap();

    // Signed with the real secret, at the account's real credential version,
    // but claiming SuperUser: the role is read from the account.
    let forged = JwtManager::new(SECRET, 1)
        .issue(
            &reader.id,
            &reader.username,
            Role::SuperUser,
            reader.credential_version,
        )
        .unwrap()
        .token;

    assert_eq!(list_status(&state, &forged).await, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn tokens_of_earlier_releases_are_rejected() {
    #[derive(serde::Serialize)]
    struct OldClaims {
        sub: String,
        username: String,
        role: Role,
        exp: i64,
    }
    let (state, _temp) = fresh_state().await;
    create(&state, "admin_seven", "pass-12345", "SuperUser").await;
    let user = state.iam_storage.get_user_by_username("admin_seven").await.unwrap().unwrap();

    let old = encode(
        &Header::default(),
        &OldClaims {
            sub: user.id,
            username: user.username,
            role: Role::SuperUser,
            exp: chrono::Utc::now().timestamp() + 3600,
        },
        &EncodingKey::from_secret(SECRET.as_bytes()),
    )
    .unwrap();

    assert_eq!(list_status(&state, &old).await, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn login_reports_the_real_expiry() {
    let (state, _temp) = fresh_state().await;
    let (_, body) = call(
        &state,
        "POST",
        "/api/admin/login",
        None,
        Some(json!({ "username": "root", "password": ROOT_PASSWORD })),
    )
    .await;

    let token = body["token"].as_str().unwrap();
    let claims = JwtManager::new(SECRET, 1).validate_token(token).unwrap();
    let expires_at: chrono::DateTime<chrono::Utc> =
        body["expires_at"].as_str().unwrap().parse().unwrap();
    assert_eq!(expires_at.timestamp(), claims.exp);
}

#[tokio::test]
async fn root_is_configured_not_stored() {
    let (state, _temp) = fresh_state().await;
    let token = root_token(&state).await;
    let root_id = RootAccount::id_for("root");

    // Listed once, under its derived id, and readable like any account.
    let (_, body) = call(&state, "GET", "/api/admin/users", Some(&token), None).await;
    let roots: Vec<&Value> = body["users"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|u| u["username"] == "root")
        .collect();
    assert_eq!(roots.len(), 1);
    assert_eq!(roots[0]["id"], root_id.as_str());
    assert_eq!(roots[0]["role"], "SuperUser");
    let (status, body) = call(
        &state,
        "GET",
        &format!("/api/admin/users/{root_id}"),
        Some(&token),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["username"], "root");

    // Nothing of root is in the user store.
    assert!(state.iam_storage.get_user_by_username("root").await.unwrap().is_none());

    // Root is changed in the environment, not through the API.
    for (method, path, body) in [
        (
            "PUT",
            format!("/api/admin/users/{root_id}"),
            Some(json!({ "role": "Reader" })),
        ),
        ("DELETE", format!("/api/admin/users/{root_id}"), None),
        (
            "POST",
            format!("/api/admin/users/{root_id}/credentials"),
            None,
        ),
        (
            "DELETE",
            format!("/api/admin/users/{root_id}/credentials"),
            None,
        ),
    ] {
        let (status, answer) = call(&state, method, &path, Some(&token), body).await;
        assert_eq!(status, StatusCode::CONFLICT, "{method} {path}");
        assert_eq!(answer["code"], "RootFromEnvironment");
    }

    // Its name is taken.
    let (status, _) = call(
        &state,
        "POST",
        "/api/admin/users",
        Some(&token),
        Some(json!({ "username": "root", "password": "another-123", "role": "Reader" })),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);

    assert_eq!(
        login(&state, "root", "wrong-password").await.unwrap_err(),
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn root_sessions_end_on_revocation_logout_and_a_new_password() {
    let temp = TempDir::new().unwrap();
    let state = state_on(&temp, Some(ROOT_PASSWORD)).await;
    let root_id = RootAccount::id_for("root");

    let first = root_token(&state).await;
    let second = root_token(&state).await;
    let (status, _) = call(
        &state,
        "POST",
        &format!("/api/admin/users/{root_id}/sessions/revoke"),
        Some(&first),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert_eq!(list_status(&state, &second).await, StatusCode::UNAUTHORIZED);

    let third = root_token(&state).await;
    let (status, _) = call(&state, "POST", "/api/admin/logout", Some(&third), None).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert_eq!(list_status(&state, &third).await, StatusCode::UNAUTHORIZED);

    // The same password at the next start keeps the sessions.
    let kept = root_token(&state).await;
    drop(state);
    let state = state_on(&temp, Some(ROOT_PASSWORD)).await;
    assert_eq!(list_status(&state, &kept).await, StatusCode::OK);

    // A new password at the next start ends them, and is the only one accepted.
    drop(state);
    let state = state_on(&temp, Some("brand-new-root-pass")).await;
    assert_eq!(list_status(&state, &kept).await, StatusCode::UNAUTHORIZED);
    assert_eq!(
        login(&state, "root", ROOT_PASSWORD).await.unwrap_err(),
        StatusCode::UNAUTHORIZED
    );
    let fresh = login(&state, "root", "brand-new-root-pass").await.unwrap();
    assert_eq!(list_status(&state, &fresh).await, StatusCode::OK);
}

#[tokio::test]
async fn a_root_record_of_an_earlier_release_cannot_log_in_or_act_as_root() {
    let temp = TempDir::new().unwrap();

    // An earlier release stored root as a user, with the password of that time.
    let old = state_on(&temp, None).await;
    old.iam_storage
        .create_user(
            "root".to_string(),
            "old-stored-pass".to_string(),
            Role::SuperUser,
        )
        .await
        .unwrap();
    let stored = old.iam_storage.get_user_by_username("root").await.unwrap().unwrap();
    let stored_token = JwtManager::new(SECRET, 1).generate_token(&stored).unwrap();
    drop(old);

    let state = state_on(&temp, Some(ROOT_PASSWORD)).await;

    assert_eq!(
        login(&state, "root", "old-stored-pass").await.unwrap_err(),
        StatusCode::UNAUTHORIZED,
        "the stored password is not root's"
    );
    assert_eq!(
        list_status(&state, &stored_token).await,
        StatusCode::UNAUTHORIZED
    );

    let token = root_token(&state).await;
    let (_, body) = call(&state, "GET", "/api/admin/users", Some(&token), None).await;
    let roots: Vec<&Value> = body["users"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|u| u["username"] == "root")
        .collect();
    assert_eq!(roots.len(), 1, "one root in the list: {body}");
    assert_eq!(roots[0]["id"], RootAccount::id_for("root").as_str());

    // The stored record stays deletable, as cleanup.
    let (status, _) = call(
        &state,
        "DELETE",
        &format!("/api/admin/users/{}", stored.id),
        Some(&token),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn without_a_root_password_there_is_no_root() {
    let temp = TempDir::new().unwrap();
    let state = state_on(&temp, None).await;
    state
        .iam_storage
        .create_user(
            "admin_eight".to_string(),
            "pass-12345".to_string(),
            Role::SuperUser,
        )
        .await
        .unwrap();
    let token = login(&state, "admin_eight", "pass-12345").await.unwrap();

    assert_eq!(
        login(&state, "root", ROOT_PASSWORD).await.unwrap_err(),
        StatusCode::UNAUTHORIZED
    );
    let (_, body) = call(&state, "GET", "/api/admin/users", Some(&token), None).await;
    assert_eq!(body["users"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn nodes_with_the_same_root_and_secret_accept_each_others_root_tokens() {
    let first_dir = TempDir::new().unwrap();
    let second_dir = TempDir::new().unwrap();
    let first = state_on(&first_dir, Some(ROOT_PASSWORD)).await;
    let second = state_on(&second_dir, Some(ROOT_PASSWORD)).await;

    let token = root_token(&first).await;

    assert_eq!(list_status(&second, &token).await, StatusCode::OK);
}
