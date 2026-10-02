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

//! `GET /api/admin/bucket-stats` on a single node: every bucket with its
//! objects and bytes, gathered page by page.

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use http_body_util::BodyExt;
use s4_api::{create_router, AppState};
use s4_core::storage::{BitcaskStorageEngine, StorageEngine};
use s4_features::iam::models::Role;
use serde_json::{json, Value};
use std::collections::HashMap;
use tempfile::TempDir;
use tower::ServiceExt;

async fn single_node() -> (AppState, TempDir) {
    let temp = TempDir::new().expect("temp dir");
    let data_path = temp.path().join("volumes");
    std::fs::create_dir_all(&data_path).expect("data dir");
    let storage = BitcaskStorageEngine::new(
        data_path,
        temp.path().join("metadata_db"),
        64 * 1024 * 1024,
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
    .await;
    state
        .iam_storage
        .create_user("admin".into(), "password123".into(), Role::SuperUser)
        .await
        .expect("user");
    (state, temp)
}

async fn send(state: &AppState, request: Request<Body>) -> (StatusCode, Value) {
    let response = create_router(state.clone()).oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

async fn admin(state: &AppState, method: &str, route: &str) -> (StatusCode, Value) {
    let login = Request::builder()
        .method("POST")
        .uri("/api/admin/login")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({ "username": "admin", "password": "password123" }).to_string(),
        ))
        .unwrap();
    let (status, body) = send(state, login).await;
    assert_eq!(status, StatusCode::OK, "login: {body}");
    let token = body["token"].as_str().expect("token").to_string();
    let request = Request::builder()
        .method(method)
        .uri(format!("/api/admin{route}"))
        .header("authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    send(state, request).await
}

/// Each bucket counts its objects and their bytes; a bucket larger than one
/// listing page is counted whole, an empty one is listed with zeros, and
/// internal keys are not objects.
#[tokio::test]
async fn every_bucket_is_counted_whole_page_after_page() {
    let (state, _temp) = single_node().await;
    for bucket in ["alpha", "beta", "empty"] {
        let (status, body) = admin(&state, "PUT", &format!("/buckets/{bucket}")).await;
        assert_eq!(status, StatusCode::OK, "{body}");
    }
    let mut alpha_bytes = 0u64;
    {
        let storage = state.storage.read().await;
        for i in 0..1205u64 {
            let data = vec![b'a'; (i % 7 + 1) as usize];
            alpha_bytes += data.len() as u64;
            storage
                .put_object(
                    "alpha",
                    &format!("key-{i:05}"),
                    &data,
                    "text/plain",
                    &HashMap::new(),
                )
                .await
                .unwrap();
        }
        for (key, data) in [("one", &b"12345"[..]), ("two", &b"123"[..])] {
            storage
                .put_object("beta", key, data, "text/plain", &HashMap::new())
                .await
                .unwrap();
        }
        storage
            .put_object(
                "beta",
                "__s4_internal",
                b"internal",
                "text/plain",
                &HashMap::new(),
            )
            .await
            .unwrap();
    }

    let (status, body) = admin(&state, "GET", "/bucket-stats").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let bucket = |name: &str| {
        body["buckets"]
            .as_array()
            .unwrap()
            .iter()
            .find(|b| b["name"] == name)
            .cloned()
            .unwrap_or_else(|| panic!("{name} missing from {body}"))
    };
    assert_eq!(bucket("alpha")["objects_count"], 1205);
    assert_eq!(bucket("alpha")["storage_used_bytes"], alpha_bytes);
    assert_eq!(bucket("beta")["objects_count"], 2);
    assert_eq!(bucket("beta")["storage_used_bytes"], 8);
    assert_eq!(bucket("empty")["objects_count"], 0);
    assert_eq!(body["buckets"].as_array().unwrap().len(), 3);
    assert_eq!(body["total_storage_bytes"], alpha_bytes + 8);
    assert_eq!(body["complete"], true);
}
