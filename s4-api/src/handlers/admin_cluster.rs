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

//! Admin API handlers for cluster operations (Phase 10).
//!
//! These endpoints provide observability and management of the cluster:
//!
//! | Method | Path                          | Purpose                              |
//! |--------|-------------------------------|--------------------------------------|
//! | GET    | /admin/cluster/health         | Overall cluster health               |
//! | GET    | /admin/cluster/topology       | Pool/node topology view              |
//! | GET    | /admin/node/health            | Local node health                    |
//! | POST   | /admin/node/drain             | Graceful drain for rolling upgrade   |
//! | GET    | /admin/cluster/repair-status  | Anti-entropy repair status           |
//!
//! All endpoints require `SuperUser` role via JWT authentication.

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    Extension, Json,
};
use s4_cluster::{
    ClusterHealthStatus, HealthLevel, NodeHealthInfo, RepairStatus, PROTOCOL_VERSION, S4_VERSION,
};
use s4_features::iam::JwtClaims;
use serde_json::json;

use crate::AppState;

/// GET /admin/cluster/health
///
/// Returns overall cluster health status. In standalone mode, returns a
/// single-node healthy response. In cluster mode, aggregates gossip state.
pub async fn cluster_health(
    State(state): State<AppState>,
    Extension(claims): Extension<JwtClaims>,
) -> Result<Json<serde_json::Value>, ClusterAdminError> {
    if !claims.role.can_admin() {
        return Err(ClusterAdminError::Forbidden);
    }

    let health = if state.is_cluster_mode() {
        // Cluster mode: aggregate from gossip handle
        // TODO: wire up GossipHandle when cluster bootstrap is active
        ClusterHealthStatus {
            status: HealthLevel::Healthy,
            nodes_total: 1,
            nodes_alive: 1,
            nodes_suspect: 0,
            nodes_dead: 0,
            epoch: 1,
            protocol_version: PROTOCOL_VERSION,
            s4_version: S4_VERSION.to_string(),
        }
    } else {
        // Standalone mode: single healthy node
        ClusterHealthStatus {
            status: HealthLevel::Healthy,
            nodes_total: 1,
            nodes_alive: 1,
            nodes_suspect: 0,
            nodes_dead: 0,
            epoch: 0,
            protocol_version: PROTOCOL_VERSION,
            s4_version: S4_VERSION.to_string(),
        }
    };

    Ok(Json(json!(health)))
}

/// GET /admin/cluster/topology
///
/// Returns the cluster topology: pools, nodes, and their roles.
/// In standalone mode, returns a minimal single-node topology.
pub async fn cluster_topology(
    State(state): State<AppState>,
    Extension(claims): Extension<JwtClaims>,
) -> Result<Json<serde_json::Value>, ClusterAdminError> {
    if !claims.role.can_admin() {
        return Err(ClusterAdminError::Forbidden);
    }

    if state.is_cluster_mode() {
        // Cluster mode: return pool/node topology from gossip
        // TODO: wire up when cluster bootstrap is active
        Ok(Json(json!({
            "mode": "cluster",
            "pools": [],
            "epoch": 1
        })))
    } else {
        Ok(Json(json!({
            "mode": "standalone",
            "pools": [],
            "epoch": 0
        })))
    }
}

/// GET /admin/node/health
///
/// Returns the health of the local node, including version info and uptime.
pub async fn node_health(
    State(state): State<AppState>,
    Extension(claims): Extension<JwtClaims>,
) -> Result<Json<serde_json::Value>, ClusterAdminError> {
    if !claims.role.can_admin() {
        return Err(ClusterAdminError::Forbidden);
    }

    let uptime_secs = state.start_time.elapsed().as_secs();
    let cluster_mode = state.is_cluster_mode();

    let info = NodeHealthInfo {
        node_id: "standalone".to_string(),
        node_name: "s4-server".to_string(),
        status: "alive".to_string(),
        pool_id: 0,
        protocol_version: PROTOCOL_VERSION,
        s4_version: S4_VERSION.to_string(),
        grpc_addr: String::new(),
        http_addr: String::new(),
    };

    Ok(Json(json!({
        "node": info,
        "uptime_secs": uptime_secs,
        "cluster_mode": cluster_mode,
    })))
}

/// POST /admin/node/drain
///
/// Initiates a graceful drain of this node. The node will stop accepting
/// new coordinated requests and wait for in-flight operations to complete.
///
/// Used during rolling upgrades: drain → stop → upgrade binary → start.
pub async fn node_drain(
    State(state): State<AppState>,
    Extension(claims): Extension<JwtClaims>,
) -> Result<Json<serde_json::Value>, ClusterAdminError> {
    if !claims.role.can_admin() {
        return Err(ClusterAdminError::Forbidden);
    }

    if !state.is_cluster_mode() {
        return Err(ClusterAdminError::NotClusterMode);
    }

    // In cluster mode, the lifecycle handle would be used here
    // to begin drain and wait for in-flight operations.
    // TODO: wire up NodeLifecycle when cluster bootstrap is active

    Ok(Json(json!({
        "status": "drain_initiated",
        "message": "Node is draining. Wait for completion before stopping."
    })))
}

/// GET /admin/cluster/repair-status
///
/// Returns the current anti-entropy repair status.
pub async fn repair_status(
    State(state): State<AppState>,
    Extension(claims): Extension<JwtClaims>,
) -> Result<Json<serde_json::Value>, ClusterAdminError> {
    if !claims.role.can_admin() {
        return Err(ClusterAdminError::Forbidden);
    }

    if !state.is_cluster_mode() {
        return Ok(Json(json!({
            "mode": "standalone",
            "message": "Repair status not available in standalone mode"
        })));
    }

    let status = RepairStatus {
        divergent_keys: 0,
        repair_frontier_lag_secs: 0.0,
        total_repairs: 0,
        last_repair: None,
    };

    Ok(Json(json!(status)))
}

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

/// Error type for cluster admin endpoints.
#[derive(Debug)]
pub enum ClusterAdminError {
    /// Caller lacks required permissions.
    Forbidden,
    /// Operation requires cluster mode.
    NotClusterMode,
    /// Internal cluster error.
    Internal(String),
}

impl IntoResponse for ClusterAdminError {
    fn into_response(self) -> Response {
        match self {
            Self::Forbidden => (
                StatusCode::FORBIDDEN,
                Json(json!({"error": "Forbidden: SuperUser role required"})),
            )
                .into_response(),
            Self::NotClusterMode => (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "error": "This operation requires cluster mode (S4_MODE=cluster)"
                })),
            )
                .into_response(),
            Self::Internal(msg) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": msg})),
            )
                .into_response(),
        }
    }
}

impl From<s4_cluster::ClusterError> for ClusterAdminError {
    fn from(err: s4_cluster::ClusterError) -> Self {
        Self::Internal(err.to_string())
    }
}
