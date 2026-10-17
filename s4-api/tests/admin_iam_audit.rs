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

//! The audit log of IAM changes, the user listing a page at a time, and the
//! secrets of S3 credentials kept sealed (plan CLUSTER-IAM-REPLICATION.md,
//! decisions 7 and 10), through the Admin API of a single node.

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use s4_api::{create_router, AppState};
use s4_core::storage::BitcaskStorageEngine;
use s4_features::iam::RootAccount;
use serde_json::{json, Value};
use tempfile::TempDir;
use tower::ServiceExt;

const SECRET: &str = "admin-iam-audit-secret-0123456789abcdef";
const ROOT_PASSWORD: &str = "root-password-123";

async fn fresh_state() -> (AppState, TempDir) {
    let temp = TempDir::new().expect("temp dir");
    let data_path = temp.path().join("volumes");
    std::fs::create_dir_all(&data_path).expect("data dir");
    let storage = BitcaskStorageEngine::new(
        data_path,
        temp.path().join("metadata_db"),
        1024 * 1024 * 10,
        4096,
        false,
    )
    .await
    .expect("storage engine");
    let state = AppState::new(
        storage,
        "test-access-key".to_string(),
        "test-secret-key".to_string(),
        temp.path(),
    )
    .await
    .with_jwt_secret(SECRET)
    .with_root_account(RootAccount::new("root", ROOT_PASSWORD, SECRET).unwrap())
    .with_iam_node_name("node-a");
    state.auth_service.start_root().await.expect("start root");
    (state, temp)
}

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
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

async fn root_token(state: &AppState) -> String {
    let (status, body) = call(
        state,
        "POST",
        "/api/admin/login",
        None,
        Some(json!({ "username": "root", "password": ROOT_PASSWORD })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    body["token"].as_str().unwrap().to_string()
}

async fn create(state: &AppState, token: &str, name: &str) -> String {
    let (status, body) = call(
        state,
        "POST",
        "/api/admin/users",
        Some(token),
        Some(json!({ "username": name, "password": "Some-Pass-123", "role": "Writer" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    body["id"].as_str().unwrap().to_string()
}

/// Users come a page at a time, root first, until the last page; without
/// `max-keys` the whole list is one page, and a bad page is a `400`.
#[tokio::test]
async fn users_are_listed_a_page_at_a_time() {
    let (state, _temp) = fresh_state().await;
    let token = root_token(&state).await;
    for n in 0..5 {
        create(&state, &token, &format!("lister_{n}")).await;
    }

    let (status, whole) = call(&state, "GET", "/api/admin/users", Some(&token), None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(whole["is_truncated"], false);
    let all: Vec<String> = whole["users"]
        .as_array()
        .unwrap()
        .iter()
        .map(|user| user["username"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(all.len(), 6);
    assert_eq!(all[0], "root");

    let mut paged = Vec::new();
    let mut path = "/api/admin/users?max-keys=2".to_string();
    let mut pages = 0;
    loop {
        let (status, page) = call(&state, "GET", &path, Some(&token), None).await;
        assert_eq!(status, StatusCode::OK, "{page}");
        pages += 1;
        let users = page["users"].as_array().unwrap();
        assert!(users.len() <= 2);
        paged.extend(users.iter().map(|user| user["username"].as_str().unwrap().to_string()));
        if page["is_truncated"] == false {
            assert!(page.get("next_continuation_token").is_none());
            break;
        }
        let next = page["next_continuation_token"].as_str().unwrap();
        path = format!("/api/admin/users?max-keys=2&continuation-token={next}");
    }
    assert_eq!(paged, all);
    assert_eq!(pages, 3);

    // Root alone on a page of one, then the users.
    let (_, first) = call(
        &state,
        "GET",
        "/api/admin/users?max-keys=1",
        Some(&token),
        None,
    )
    .await;
    assert_eq!(first["users"][0]["username"], "root");
    assert_eq!(first["is_truncated"], true);
    let next = first["next_continuation_token"].as_str().unwrap().to_string();
    let (_, second) = call(
        &state,
        "GET",
        &format!("/api/admin/users?max-keys=1&continuation-token={next}"),
        Some(&token),
        None,
    )
    .await;
    assert_eq!(second["users"][0]["username"].as_str().unwrap(), all[1]);

    for bad in ["max-keys=0", "max-keys=1001", "continuation-token=%21%21"] {
        let (status, body) = call(
            &state,
            "GET",
            &format!("/api/admin/users?{bad}"),
            Some(&token),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{bad}: {body}");
        assert_eq!(body["code"], "InvalidArgument", "{body}");
    }
}

/// Every change is in the audit log — who, what, to whom, through which node
/// — a page at a time, without a password or a secret; only a SuperUser
/// reads it.
#[tokio::test]
async fn every_change_is_in_the_audit_log() {
    let (state, _temp) = fresh_state().await;
    let token = root_token(&state).await;
    let id = create(&state, &token, "audited").await;
    let (status, keys) = call(
        &state,
        "POST",
        &format!("/api/admin/users/{id}/credentials"),
        Some(&token),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let secret = keys["secret_key"].as_str().unwrap().to_string();
    let (status, _) = call(
        &state,
        "PUT",
        &format!("/api/admin/users/{id}"),
        Some(&token),
        Some(json!({ "password": "Another-Pass-1", "role": "Reader" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = call(
        &state,
        "POST",
        &format!("/api/admin/users/{id}/sessions/revoke"),
        Some(&token),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, _) = call(
        &state,
        "DELETE",
        &format!("/api/admin/users/{id}"),
        Some(&token),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let (status, log) = call(&state, "GET", "/api/admin/iam/audit", Some(&token), None).await;
    assert_eq!(status, StatusCode::OK, "{log}");
    let entries = log["entries"].as_array().unwrap();
    let actions: Vec<&str> =
        entries.iter().map(|entry| entry["action"].as_str().unwrap()).collect();
    assert_eq!(
        actions,
        [
            "user_created",
            "credentials_issued",
            "user_updated",
            "sessions_revoked",
            "user_deleted"
        ]
    );
    for entry in entries {
        assert_eq!(entry["actor"]["name"], "root");
        assert_eq!(entry["through"], "node-a");
        assert_eq!(entry["target"], "audited");
    }
    assert_eq!(entries[2]["details"]["role"], "Writer -> Reader");
    let text = log.to_string();
    assert!(!text.contains(&secret));
    assert!(!text.contains("Another-Pass-1"));

    let (_, first) = call(
        &state,
        "GET",
        "/api/admin/iam/audit?max-keys=3",
        Some(&token),
        None,
    )
    .await;
    assert_eq!(first["entries"].as_array().unwrap().len(), 3);
    let next = first["next_continuation_token"].as_str().unwrap();
    let (_, rest) = call(
        &state,
        "GET",
        &format!("/api/admin/iam/audit?max-keys=3&continuation-token={next}"),
        Some(&token),
        None,
    )
    .await;
    assert_eq!(rest["entries"].as_array().unwrap().len(), 2);
    assert_eq!(rest["is_truncated"], false);

    // A Reader does not read it.
    let reader = create(&state, &token, "reader_one").await;
    let (_, _) = call(
        &state,
        "PUT",
        &format!("/api/admin/users/{reader}"),
        Some(&token),
        Some(json!({ "role": "Reader" })),
    )
    .await;
    let (status, body) = call(
        &state,
        "POST",
        "/api/admin/login",
        None,
        Some(json!({ "username": "reader_one", "password": "Some-Pass-123" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let reader_token = body["token"].as_str().unwrap();
    let (status, _) = call(
        &state,
        "GET",
        "/api/admin/iam/audit",
        Some(reader_token),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

/// The secret of S3 credentials issued through the Admin API is nowhere on
/// disk in clear text, and the credentials sign requests.
#[tokio::test]
async fn an_issued_secret_is_not_on_disk() {
    let (state, temp) = fresh_state().await;
    let token = root_token(&state).await;
    let id = create(&state, &token, "sealed_one").await;
    let (_, keys) = call(
        &state,
        "POST",
        &format!("/api/admin/users/{id}/credentials"),
        Some(&token),
        None,
    )
    .await;
    let secret = keys["secret_key"].as_str().unwrap().to_string();
    let access_key = keys["access_key"].as_str().unwrap().to_string();
    let found = state.iam_storage.verify_s3_credentials(&access_key, &secret).await.unwrap();
    assert!(found.is_some());
    let with_secret = state
        .iam_storage
        .get_user_by_access_key_with_secret(&access_key)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(with_secret.secret_key.as_deref(), Some(secret.as_str()));

    let mut stack = vec![temp.path().to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                stack.push(path);
            } else if let Ok(bytes) = std::fs::read(&path) {
                assert!(
                    !bytes.windows(secret.len()).any(|window| window == secret.as_bytes()),
                    "the secret is in {}",
                    path.display()
                );
            }
        }
    }
}

/// A single node's IAM is all there is: its status answers credentials
/// served and changes open, to SuperUser only.
#[tokio::test]
async fn a_single_node_reports_its_iam_served() {
    let (state, _dir) = fresh_state().await;
    let root = root_token(&state).await;
    let (status, body) = call(&state, "GET", "/api/admin/iam/status", Some(&root), None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["mode"], "standalone");
    assert_eq!(body["credentials"]["state"], "serving");
    assert_eq!(body["adoption"], "adopted");

    let id = create(&state, &root, "rhea").await;
    let (status, _) = call(
        &state,
        "PUT",
        &format!("/api/admin/users/{id}"),
        Some(&root),
        Some(json!({ "role": "Reader" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, body) = call(
        &state,
        "POST",
        "/api/admin/login",
        None,
        Some(json!({ "username": "rhea", "password": "Some-Pass-123" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let reader = body["token"].as_str().unwrap().to_string();
    let (status, _) = call(&state, "GET", "/api/admin/iam/status", Some(&reader), None).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}
