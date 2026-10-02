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

//! `GET /api/admin/buckets/{name}/objects` on a single node: a bucket listed
//! page by page, every object and every directory exactly once.

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

async fn single_node() -> (AppState, TempDir, String) {
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
    let login = Request::builder()
        .method("POST")
        .uri("/api/admin/login")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({ "username": "admin", "password": "password123" }).to_string(),
        ))
        .unwrap();
    let (status, body) = send(&state, login).await;
    assert_eq!(status, StatusCode::OK, "login: {body}");
    let token = body["token"].as_str().expect("token").to_string();
    (state, temp, token)
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

async fn admin(state: &AppState, token: &str, method: &str, route: &str) -> (StatusCode, Value) {
    let request = Request::builder()
        .method(method)
        .uri(format!("/api/admin{route}"))
        .header("authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    send(state, request).await
}

/// Every page the listing hands out, following its tokens: the objects and
/// the directories, in order. A listing that does not end within a minute is
/// a failure, not a hang.
async fn every_page(
    state: &AppState,
    token: &str,
    bucket: &str,
    query: &str,
) -> (Vec<String>, Vec<String>, usize) {
    tokio::time::timeout(
        std::time::Duration::from_secs(60),
        pages(state, token, bucket, query),
    )
    .await
    .expect("the listing did not end within a minute")
}

async fn pages(
    state: &AppState,
    token: &str,
    bucket: &str,
    query: &str,
) -> (Vec<String>, Vec<String>, usize) {
    let (mut objects, mut directories, mut pages) = (Vec::new(), Vec::new(), 0);
    let mut continuation: Option<String> = None;
    loop {
        let mut route = format!("/buckets/{bucket}/objects?{query}");
        if let Some(ref next) = continuation {
            route.push_str("&continuation-token=");
            route.push_str(&urlencode(next));
        }
        let (status, body) = admin(state, token, "GET", &route).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["complete"], true, "{body}");
        pages += 1;
        assert!(pages < 100, "the listing does not end");
        for object in body["objects"].as_array().unwrap() {
            assert!(
                object["etag"].is_string() && object["last_modified"].is_u64(),
                "{object}"
            );
            objects.push(object["key"].as_str().unwrap().to_string());
        }
        for directory in body["common_prefixes"].as_array().unwrap() {
            directories.push(directory.as_str().unwrap().to_string());
        }
        if body["is_truncated"] != true {
            return (objects, directories, pages);
        }
        continuation = Some(body["next_continuation_token"].as_str().unwrap().to_string());
    }
}

fn urlencode(value: &str) -> String {
    value
        .bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}

/// Paged two at a time, the listing hands out each object and each directory
/// once, in key order — a directory that ended a page does not open the next
/// one, and one wider than a page of storage is skipped whole — and internal
/// keys never.
#[tokio::test]
async fn a_bucket_is_listed_page_by_page_each_entry_once() {
    let (state, _temp, token) = single_node().await;
    assert_eq!(
        admin(&state, &token, "PUT", "/buckets/browse").await.0,
        StatusCode::OK
    );
    {
        let mut keys: Vec<String> = [
            "a.txt", "b.txt", "c.txt", "dir1/x", "dir1/y", "dir2/w", "dir2/z", "z.txt",
        ]
        .iter()
        .map(|key| key.to_string())
        .collect();
        keys.extend((0..1003).map(|i| format!("huge/k{i:04}")));
        keys.push("__s4_internal".to_string());
        let storage = state.storage.read().await;
        for key in &keys {
            storage
                .put_object("browse", key, b"data", "text/plain", &HashMap::new())
                .await
                .unwrap();
        }
    }

    let (objects, directories, pages) = every_page(&state, &token, "browse", "max-keys=2").await;
    assert_eq!(objects, ["a.txt", "b.txt", "c.txt", "z.txt"]);
    assert_eq!(directories, ["dir1/", "dir2/", "huge/"]);
    assert_eq!(pages, 4);

    let (objects, directories, pages) = every_page(&state, &token, "browse", "").await;
    assert_eq!((objects.len(), directories.len(), pages), (4, 3, 1));

    let (objects, directories, _) = every_page(&state, &token, "browse", "prefix=dir1/").await;
    assert_eq!(
        (objects, directories),
        (vec!["dir1/x".to_string(), "dir1/y".to_string()], vec![])
    );
}
