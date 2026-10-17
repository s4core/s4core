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
    BucketMetadataHealth, ClusterAdminService, ClusterHealthStatus, ClusterTopology,
    ConfigAdoptionPhase, ConfigAdoptionSnapshot, HealthLevel, IamAdoptionHealth, IamFreshness,
    NodeHealthInfo, NodeTopology, PlacementHoldout, PlacementShape, PoolTopology, RepairStatus,
    RoundCounts, ScrubberMetricsSnapshot, ScrubberStatus, PROTOCOL_VERSION, S4_VERSION,
};
use s4_core::storage::BitcaskStorageEngine;
use s4_core::StorageEngine;
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

/// A local configuration has not passed adoption yet. Listening for HTTP is
/// sufficient for liveness, but cannot promise completion of that recovery.
#[tokio::test]
async fn readiness_waits_for_local_bucket_config_adoption() {
    let (state, _temp) = single_node().await;
    {
        let storage = state.storage.read().await;
        storage
            .put_object(
                "__system__",
                "__s4_bucket_cors_pending",
                b"<CORSConfiguration><CORSRule><AllowedMethod>GET</AllowedMethod><AllowedOrigin>https://pending.example</AllowedOrigin></CORSRule></CORSConfiguration>",
                "application/xml",
                &std::collections::HashMap::new(),
            )
            .await
            .unwrap();
        let record = storage.head_object("__system__", "__s4_bucket_cors_pending").await.unwrap();
        assert_eq!(record.origin_node_id, 0, "a local write has no cluster HLC");
    }
    let state = state.with_cluster_admin_service(Arc::new(Reported));
    for route in ["/health", "/minio/health/live"] {
        let response = create_router(state.clone())
            .oneshot(Request::builder().uri(route).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{route}");
    }
    let response = create_router(state)
        .oneshot(Request::builder().uri("/minio/health/ready").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(response.headers()["retry-after"], "1");
}

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
            bucket_metadata: Some(BucketMetadataHealth {
                stage: 1,
                stage_name: "dual_write".to_string(),
                from: PlacementShape {
                    replication_factor: 3,
                    write_quorum: 2,
                    read_quorum: 2,
                },
                to: PlacementShape {
                    replication_factor: 5,
                    write_quorum: 3,
                    read_quorum: 3,
                },
                backfill_complete: false,
                held_by: vec![PlacementHoldout {
                    node: "node-3".to_string(),
                    reason: "not alive".to_string(),
                }],
                held_for_secs: Some(42),
            }),
            bucket_config_adoption: Some(ConfigAdoptionSnapshot::default()),
            iam_adoption: None,
            iam_config: None,
            migration_pending_records: 0,
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
            repairs_ambiguous: 1,
            rounds: RoundCounts {
                complete: 2,
                incomplete: 1,
                skipped: 3,
            },
            anti_entropy_enabled: true,
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

    let mut health = json!(Reported::health());
    health["unenforced_bucket_configs"] = json!([]);
    for (route, reported) in [
        ("/cluster/health", health),
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

/// A cluster node's health names the versioning and Object Lock its own
/// storage holds from before cluster mode refused them, by bucket and kind,
/// as the node's background count last found them: a health check scans no
/// storage, and a configuration stored since waits for the next count.
#[tokio::test]
async fn cluster_health_names_the_configurations_cluster_mode_does_not_enforce() {
    let (state, _temp) = single_node().await;
    let lock = s4_features::object_lock::object_lock_to_xml(
        &s4_features::object_lock::ObjectLockConfiguration {
            object_lock_enabled: true,
            default_retention: None,
        },
    );
    for (key, value) in [
        ("__s4_bucket_versioning_vault", "Enabled"),
        ("__s4_bucket_object_lock_vault", lock.as_str()),
        ("__s4_bucket_versioning_plain", "Suspended"),
    ] {
        state
            .storage
            .read()
            .await
            .put_object(
                "__system__",
                key,
                value.as_bytes(),
                "text/plain",
                &std::collections::HashMap::new(),
            )
            .await
            .expect("configuration stored");
    }
    let counted = s4_api::handlers::UnenforcedConfigs::default();
    let state = state
        .with_cluster_admin_service(Arc::new(Reported))
        .with_unenforced_configs(counted.clone());
    let token = token(&state, "admin").await;

    let (status, body) = get(&state, &token, "/cluster/health").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["unenforced_bucket_configs"],
        json!([]),
        "nothing counted yet: {body}"
    );

    counted.set(s4_api::handlers::unenforced_bucket_configs(&*state.storage.read().await).await);
    let (status, body) = get(&state, &token, "/cluster/health").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["unenforced_bucket_configs"],
        json!([
            {"bucket": "plain", "kind": "versioning"},
            {"bucket": "vault", "kind": "versioning"},
            {"bucket": "vault", "kind": "object_lock"},
        ]),
        "{body}"
    );
}

#[tokio::test]
async fn a_cluster_route_still_needs_a_superuser() {
    let (state, _temp) = single_node().await;
    let state = state.with_cluster_admin_service(Arc::new(Reported));
    let token = token(&state, "viewer").await;
    let (status, body) = get(&state, &token, "/cluster/health").await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
}

/// An in-memory report; probes must never ask for RPC-backed diagnostic data.
struct ReadinessReport(std::sync::RwLock<ClusterHealthStatus>);

impl ClusterAdminService for ReadinessReport {
    fn cluster_health(&self) -> Option<ClusterHealthStatus> {
        Some(self.0.read().unwrap().clone())
    }
    fn topology(&self) -> Option<ClusterTopology> {
        panic!("probe must not inspect topology")
    }
    fn node_health(&self) -> Option<NodeHealthInfo> {
        panic!("probe must not inspect node health")
    }
    fn repair_status(&self) -> Option<RepairStatus> {
        panic!("probe must not inspect repair")
    }
    fn scrubber_status(&self) -> Option<ScrubberStatus> {
        panic!("probe must not inspect scrubber")
    }
}

async fn probe(state: &AppState, method: &str, path: &str) -> axum::response::Response {
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        create_router(state.clone())
            .oneshot(Request::builder().method(method).uri(path).body(Body::empty()).unwrap()),
    )
    .await
    .expect("an in-memory probe must not wait for storage or RPC")
    .unwrap()
}

async fn assert_readiness(state: &AppState, reason: Option<&str>) {
    for method in ["GET", "HEAD"] {
        let response = probe(state, method, "/minio/health/ready").await;
        assert_eq!(
            response.headers().get("cache-control").and_then(|v| v.to_str().ok()),
            Some("no-store"),
            "{method}: {reason:?}"
        );
        assert_eq!(
            response.status(),
            if reason.is_some() {
                StatusCode::SERVICE_UNAVAILABLE
            } else {
                StatusCode::OK
            },
            "{method}: {reason:?}",
        );
        if let Some(reason) = reason {
            assert_eq!(response.headers()["x-s4-readiness"], reason);
            assert_eq!(response.headers()["retry-after"], "1");
        } else {
            assert!(response.headers().get("retry-after").is_none());
        }
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert!(
            body.is_empty(),
            "public probes must not expose bucket names or reasons"
        );
        for path in ["/health", "/minio/health/live"] {
            assert_eq!(probe(state, method, path).await.status(), StatusCode::OK);
        }
    }
}

#[tokio::test]
async fn readiness_single_and_gateway_do_not_wait_for_cluster() {
    let (state, _temp) = single_node().await;
    // Single and gateway share NoopClusterAdminService; a closed cluster gate
    // must not accidentally make either mode wait for adoption.
    assert!(!state.config_read_gate.is_open());
    let _locked = state.storage.write().await;
    assert_readiness(&state, None).await;
}

#[tokio::test]
async fn readiness_reports_local_adoption_transitions_without_storage_or_rpc() {
    let (state, _temp) = single_node().await;
    let report = Arc::new(ReadinessReport(std::sync::RwLock::new(Reported::health())));
    let state = state.with_cluster_admin_service(report.clone());
    let _locked = state.storage.write().await;
    assert_readiness(&state, Some("bucket_config_starting")).await;
    {
        let mut health = report.0.write().unwrap();
        health.bucket_config_adoption = Some(ConfigAdoptionSnapshot {
            phase: ConfigAdoptionPhase::Pending,
            pending_records: Some(1),
            reasons: vec!["private-bucket/__s4_bucket_cors_private: owner offline".into()],
        });
    }
    assert_readiness(&state, Some("bucket_config_pending")).await;
    report.0.write().unwrap().bucket_config_adoption = Some(ConfigAdoptionSnapshot {
        phase: ConfigAdoptionPhase::Ready,
        pending_records: Some(0),
        reasons: vec![],
    });
    // The report remains degraded and bucket metadata backfill is unfinished.
    // The pool-wide config read gate is also closed; none blocks local readiness.
    assert!(!state.config_read_gate.is_open());
    assert_readiness(&state, None).await;
    report.0.write().unwrap().bucket_config_adoption = None;
    assert_readiness(&state, Some("bucket_config_starting")).await;
}

#[tokio::test]
async fn readiness_waits_for_migration_and_iam_adoption() {
    let (state, _temp) = single_node().await;
    let mut health = Reported::health();
    health.bucket_config_adoption = Some(ConfigAdoptionSnapshot {
        phase: ConfigAdoptionPhase::Ready,
        pending_records: Some(0),
        reasons: vec![],
    });
    health.migration_pending_records = 2;
    let report = Arc::new(ReadinessReport(std::sync::RwLock::new(health)));
    let state = state.with_cluster_admin_service(report.clone());
    let _locked = state.storage.write().await;
    assert_readiness(&state, Some("migration_pending")).await;
    {
        let mut health = report.0.write().unwrap();
        health.migration_pending_records = 0;
        health.iam_adoption = Some(IamAdoptionHealth {
            pending: true,
            reason: Some("private legacy user needs a decision".into()),
        });
    }
    assert_readiness(&state, Some("iam_adoption_pending")).await;
    report.0.write().unwrap().iam_adoption.as_mut().unwrap().pending = false;
    assert_readiness(&state, None).await;
}

#[tokio::test(flavor = "current_thread")]
async fn readiness_uses_the_shared_iam_gate_without_counting_auth_refusals() {
    let (state, _temp) = single_node().await;
    let recorder = s4_api::prometheus::builder().unwrap().build_recorder();
    let _local = metrics::set_default_local_recorder(&recorder);
    // Pre-create both label sets so the assertion also catches increments.
    for reason in ["starting", "stale"] {
        metrics::counter!("s4_iam_auth_refused_total", "reason" => reason).absolute(0);
    }
    let mut health = Reported::health();
    health.bucket_config_adoption = Some(ConfigAdoptionSnapshot {
        phase: ConfigAdoptionPhase::Ready,
        pending_records: Some(0),
        reasons: vec![],
    });
    let freshness = Arc::new(IamFreshness::new(std::time::Duration::from_secs(1)));
    let state = state
        .with_cluster_admin_service(Arc::new(ReadinessReport(std::sync::RwLock::new(health))))
        .with_iam_freshness(freshness.clone())
        .with_prometheus_handle(recorder.handle());
    let _locked = state.storage.write().await;
    assert_readiness(&state, Some("iam_starting")).await;
    freshness.confirm(std::time::Instant::now());
    assert_readiness(&state, None).await;
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    assert_readiness(&state, Some("iam_stale")).await;
    assert_eq!(
        probe(&state, "GET", "/metrics").await.status(),
        StatusCode::OK
    );
    freshness.confirm(std::time::Instant::now());
    assert_readiness(&state, None).await;
    let rendered = recorder.handle().render();
    for reason in ["starting", "stale"] {
        assert!(
            rendered.contains(&format!(
                "s4_iam_auth_refused_total{{reason=\"{reason}\"}} 0"
            )),
            "{rendered}"
        );
    }
    assert!(
        !rendered.contains("s4_quorum_operations_total"),
        "probes cannot issue RPC: {rendered}"
    );
    assert_eq!(
        probe(&state, "GET", "/metrics").await.status(),
        StatusCode::OK
    );
}

#[tokio::test]
async fn readiness_keeps_detailed_diagnostics_protected_and_accessible() {
    let (state, _temp) = single_node().await;
    let token = token(&state, "admin").await;
    let mut health = Reported::health();
    let details = ConfigAdoptionSnapshot {
        phase: ConfigAdoptionPhase::Pending,
        pending_records: Some(1),
        reasons: vec!["private-bucket CORS: owner offline".into()],
    };
    health.bucket_config_adoption = Some(details.clone());
    let state =
        state.with_cluster_admin_service(Arc::new(ReadinessReport(std::sync::RwLock::new(health))));
    assert_readiness(&state, Some("bucket_config_pending")).await;
    let (status, body) = get(&state, &token, "/cluster/health").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["bucket_config_adoption"], json!(details));
    let response = probe(&state, "GET", "/api/admin/cluster/health").await;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn readiness_observes_the_actual_iam_adoption_gate() {
    let (mut state, _temp) = single_node().await;
    let gate = s4_cluster::IamAdoptionGate::default();
    state.iam_storage = Arc::new(state.iam_storage.with_adoption_gate(Arc::new(
        s4_api::cluster_iam::ClusterAdoptionGate::new(gate.clone()),
    )));
    let mut health = Reported::health();
    health.bucket_config_adoption = Some(ConfigAdoptionSnapshot {
        phase: ConfigAdoptionPhase::Ready,
        pending_records: Some(0),
        reasons: vec![],
    });
    // The admin snapshot does not carry IAM adoption; readiness still follows
    // the same gate used to admit IAM changes.
    let state =
        state.with_cluster_admin_service(Arc::new(ReadinessReport(std::sync::RwLock::new(health))));
    let _locked = state.storage.write().await;
    assert_readiness(&state, Some("iam_adoption_pending")).await;
    gate.open();
    assert_readiness(&state, None).await;
}
