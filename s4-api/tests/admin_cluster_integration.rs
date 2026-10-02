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

//! The cluster routes of the admin API, through the router.
//!
//! A single node answers them as it always has. A cluster node answers what its
//! cluster admin service reports — the same service that the metrics are read
//! from — and nothing the handler makes up on its own.

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use http_body_util::BodyExt;
use s4_api::{create_router, AppState};
use s4_cluster::{
    ClusterAdminService, ClusterHealthStatus, ClusterTopology, HealthLevel, NodeHealthInfo,
    NodeTopology, PoolTopology, RepairStatus, ScrubberMetricsSnapshot, ScrubberStatus,
    PROTOCOL_VERSION, S4_VERSION,
};
use s4_core::storage::BitcaskStorageEngine;
use s4_features::iam::models::Role;
use serde_json::{json, Value};
use std::sync::Arc;
use tempfile::TempDir;
use tower::ServiceExt;

/// A single node with one SuperUser ("admin") and one Reader ("viewer").
async fn single_node() -> (AppState, TempDir) {
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
    .await;
    for (name, role) in [("admin", Role::SuperUser), ("viewer", Role::Reader)] {
        state
            .iam_storage
            .create_user(name.to_string(), "password123".to_string(), role)
            .await
            .expect("user");
    }
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

async fn token(state: &AppState, username: &str) -> String {
    let request = Request::builder()
        .method("POST")
        .uri("/api/admin/login")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({ "username": username, "password": "password123" }).to_string(),
        ))
        .unwrap();
    let (status, body) = send(state, request).await;
    assert_eq!(status, StatusCode::OK, "login: {body}");
    body["token"].as_str().expect("token").to_string()
}

async fn get(state: &AppState, token: &str, route: &str) -> (StatusCode, Value) {
    let request = Request::builder()
        .uri(format!("/api/admin{route}"))
        .header("authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    send(state, request).await
}

/// The five read routes and what a single node answered on each of them
/// before they were wired to a cluster: the same fields with the same values,
/// except the node's uptime.
#[tokio::test]
async fn a_single_node_answers_as_it_always_has() {
    let (state, _temp) = single_node().await;
    let token = token(&state, "admin").await;

    let expected = [
        (
            "/cluster/health",
            json!({
                "status": "healthy",
                "nodes_total": 1,
                "nodes_alive": 1,
                "nodes_suspect": 0,
                "nodes_dead": 0,
                "epoch": 0,
                "protocol_version": PROTOCOL_VERSION,
                "s4_version": S4_VERSION,
            }),
        ),
        (
            "/cluster/topology",
            json!({ "mode": "standalone", "pools": [], "epoch": 0 }),
        ),
        (
            "/cluster/repair-status",
            json!({
                "mode": "standalone",
                "message": "Repair status not available in standalone mode",
            }),
        ),
        (
            "/scrubber/status",
            json!({
                "mode": "standalone",
                "background_scrubber": false,
                "message": "Use POST /api/admin/scrubber/run for on-demand CRC32 integrity check. \
                            Background scrubber is available in cluster mode.",
            }),
        ),
    ];
    for (route, answer) in expected {
        let (status, body) = get(&state, &token, route).await;
        assert_eq!(status, StatusCode::OK, "{route}: {body}");
        assert_eq!(body, answer, "{route}");
    }

    let (status, mut body) = get(&state, &token, "/node/health").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body["uptime_secs"].is_u64(), "{body}");
    body.as_object_mut().expect("object").remove("uptime_secs");
    assert_eq!(
        body,
        json!({
            "node": {
                "node_id": "standalone",
                "node_name": "s4-server",
                "status": "alive",
                "pool_id": 0,
                "protocol_version": PROTOCOL_VERSION,
                "s4_version": S4_VERSION,
                "grpc_addr": "",
                "http_addr": "",
            },
            "cluster_mode": false,
        })
    );
}

#[tokio::test]
async fn every_cluster_route_needs_a_superuser() {
    let (state, _temp) = single_node().await;
    let token = token(&state, "viewer").await;
    for route in [
        "/cluster/health",
        "/cluster/topology",
        "/node/health",
        "/cluster/repair-status",
        "/scrubber/status",
    ] {
        let (status, body) = get(&state, &token, route).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{route}: {body}");
    }
}

/// A cluster node whose service reports fixed values: whatever it reports is
/// what the routes must answer, field for field.
struct Reported;

impl Reported {
    fn health() -> ClusterHealthStatus {
        ClusterHealthStatus {
            status: HealthLevel::Degraded,
            nodes_total: 3,
            nodes_alive: 2,
            nodes_suspect: 0,
            nodes_dead: 1,
            nodes_left: 0,
            epoch: 7,
            protocol_version: PROTOCOL_VERSION,
            s4_version: S4_VERSION.to_string(),
            pool: "pool-1".to_string(),
            replication_factor: 3,
            write_quorum: 2,
            read_quorum: 2,
        }
    }

    fn topology() -> ClusterTopology {
        ClusterTopology {
            mode: "cluster".to_string(),
            epoch: 7,
            pools: vec![PoolTopology {
                pool_id: 0,
                name: "pool-1".to_string(),
                storage_class: s4_core::types::StorageClass::Standard,
                replication_factor: 3,
                write_quorum: 2,
                read_quorum: 2,
                nodes: vec![NodeTopology {
                    name: "node-3".to_string(),
                    node_id: "00000000-0000-0000-0000-000000000003".to_string(),
                    status: "dead".to_string(),
                    local: false,
                    grpc_addr: None,
                    http_addr: None,
                    protocol_version: None,
                    s4_version: None,
                }],
            }],
        }
    }

    fn node() -> NodeHealthInfo {
        NodeHealthInfo {
            node_id: "00000000-0000-0000-0000-000000000001".to_string(),
            node_name: "node-1".to_string(),
            status: "alive".to_string(),
            pool_id: 0,
            pool: "pool-1".to_string(),
            protocol_version: PROTOCOL_VERSION,
            s4_version: S4_VERSION.to_string(),
            grpc_addr: "127.0.0.1:9100".to_string(),
            http_addr: "127.0.0.1:9000".to_string(),
        }
    }

    fn repair() -> RepairStatus {
        RepairStatus {
            divergent_keys: 5,
            repair_frontier_lag_secs: 1.5,
            total_repairs: 4,
            last_repair: Some("2026-10-01T10:00:00.000Z".to_string()),
            rounds_completed: 2,
            repairs_failed: 1,
            tombstones_active: 3,
            tombstones_purged: 6,
        }
    }

    fn scrubber() -> ScrubberStatus {
        ScrubberStatus {
            mode: "cluster",
            background_scrubber: true,
            message: "scrubbing",
            scan_progress: 0.5,
            counters: ScrubberMetricsSnapshot {
                blobs_scanned_total: 40,
                corruptions_found_total: 2,
                corruptions_healed_total: 1,
                corruptions_unrecoverable_total: 1,
                scan_progress_pct: 50,
                last_full_scan_timestamp: 0,
                cycles_completed: 0,
                volumes_scanned_total: 4,
                bytes_scanned_total: 4096,
            },
        }
    }
}

impl ClusterAdminService for Reported {
    fn cluster_health(&self) -> Option<ClusterHealthStatus> {
        Some(Self::health())
    }
    fn topology(&self) -> Option<ClusterTopology> {
        Some(Self::topology())
    }
    fn node_health(&self) -> Option<NodeHealthInfo> {
        Some(Self::node())
    }
    fn repair_status(&self) -> Option<RepairStatus> {
        Some(Self::repair())
    }
    fn scrubber_status(&self) -> Option<ScrubberStatus> {
        Some(Self::scrubber())
    }
}

#[tokio::test]
async fn a_cluster_node_answers_what_its_service_reports() {
    let (state, _temp) = single_node().await;
    let state = state.with_cluster_admin_service(Arc::new(Reported));
    let token = token(&state, "admin").await;

    for (route, reported) in [
        ("/cluster/health", json!(Reported::health())),
        ("/cluster/topology", json!(Reported::topology())),
        ("/cluster/repair-status", json!(Reported::repair())),
        ("/scrubber/status", json!(Reported::scrubber())),
    ] {
        let (status, body) = get(&state, &token, route).await;
        assert_eq!(status, StatusCode::OK, "{route}: {body}");
        assert_eq!(body, reported, "{route}");
    }
    let (_, body) = get(&state, &token, "/cluster/health").await;
    assert_eq!(body["status"], "degraded");
    let (_, body) = get(&state, &token, "/scrubber/status").await;
    assert_eq!(
        body["blobs_scanned_total"], 40,
        "the counters are flattened: {body}"
    );

    let (status, body) = get(&state, &token, "/node/health").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["node"], json!(Reported::node()));
    assert_eq!(body["cluster_mode"], true);
    assert!(body["uptime_secs"].is_u64(), "{body}");
}

#[tokio::test]
async fn a_cluster_route_still_needs_a_superuser() {
    let (state, _temp) = single_node().await;
    let state = state.with_cluster_admin_service(Arc::new(Reported));
    let token = token(&state, "viewer").await;
    let (status, body) = get(&state, &token, "/cluster/health").await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
}
