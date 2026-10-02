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

//! Admin API handlers for cluster operations.
//!
//! These endpoints provide observability and management of the cluster:
//!
//! | Method | Path                             | Purpose                            |
//! |--------|----------------------------------|------------------------------------|
//! | GET    | /api/admin/cluster/health        | Health of the pool                 |
//! | GET    | /api/admin/cluster/topology      | The pool and its members           |
//! | GET    | /api/admin/node/health           | This node                          |
//! | POST   | /api/admin/node/drain            | Refused with 501: use SIGTERM      |
//! | GET    | /api/admin/cluster/repair-status | Anti-entropy and tombstone GC      |
//!
//! All endpoints require `SuperUser` role via JWT authentication.
//!
//! A cluster node answers what its [`s4_cluster::ClusterAdminService`]
//! reports, read from the state its metrics are written from. A single node
//! has nothing to report and answers with the fixed values it always has.

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    Extension, Json,
};
use s4_cluster::{PROTOCOL_VERSION, S4_VERSION};
use s4_features::iam::JwtClaims;
use serde_json::json;

use crate::AppState;

/// GET /api/admin/cluster/health
///
/// The pool's health as this node sees it.
pub async fn cluster_health(
    State(state): State<AppState>,
    Extension(claims): Extension<JwtClaims>,
) -> Result<Json<serde_json::Value>, ClusterAdminError> {
    if !claims.role.can_admin() {
        return Err(ClusterAdminError::Forbidden);
    }

    Ok(Json(match state.cluster_admin_service.cluster_health() {
        Some(health) => json!(health),
        None => json!({
            "status": "healthy",
            "nodes_total": 1,
            "nodes_alive": 1,
            "nodes_suspect": 0,
            "nodes_dead": 0,
            "epoch": 0,
            "protocol_version": PROTOCOL_VERSION,
            "s4_version": S4_VERSION,
        }),
    }))
}

/// GET /api/admin/cluster/topology
///
/// The pool and every configured member, as this node sees them.
pub async fn cluster_topology(
    State(state): State<AppState>,
    Extension(claims): Extension<JwtClaims>,
) -> Result<Json<serde_json::Value>, ClusterAdminError> {
    if !claims.role.can_admin() {
        return Err(ClusterAdminError::Forbidden);
    }

    Ok(Json(match state.cluster_admin_service.topology() {
        Some(topology) => json!(topology),
        None => json!({
            "mode": "standalone",
            "pools": [],
            "epoch": 0
        }),
    }))
}

/// GET /api/admin/node/health
///
/// The node answering, its version and its uptime.
pub async fn node_health(
    State(state): State<AppState>,
    Extension(claims): Extension<JwtClaims>,
) -> Result<Json<serde_json::Value>, ClusterAdminError> {
    if !claims.role.can_admin() {
        return Err(ClusterAdminError::Forbidden);
    }

    let uptime_secs = state.start_time.elapsed().as_secs();
    let (node, cluster_mode) = match state.cluster_admin_service.node_health() {
        Some(node) => (json!(node), true),
        None => (
            json!({
                "node_id": "standalone",
                "node_name": "s4-server",
                "status": "alive",
                "pool_id": 0,
                "protocol_version": PROTOCOL_VERSION,
                "s4_version": S4_VERSION,
                "grpc_addr": "",
                "http_addr": "",
            }),
            false,
        ),
    };

    Ok(Json(json!({
        "node": node,
        "uptime_secs": uptime_secs,
        "cluster_mode": cluster_mode,
    })))
}

/// What `/node/drain` answers in cluster mode.
pub const DRAIN_NOT_IMPLEMENTED: &str = "Drain through the admin API is not implemented. \
     Stop the node with SIGTERM instead: it stops accepting connections and finishes \
     the requests in flight before it exits.";

/// POST /api/admin/node/drain
///
/// Refuses: drain through the API is not implemented. It used to answer
/// `drain_initiated` and do nothing, which a rolling upgrade would take at its
/// word and stop a node still serving requests. SIGTERM does what an operator
/// wants from a drain — the server stops accepting connections and finishes
/// the requests in flight — so the answer points there.
pub async fn node_drain(
    State(state): State<AppState>,
    Extension(claims): Extension<JwtClaims>,
) -> ClusterAdminError {
    if !claims.role.can_admin() {
        return ClusterAdminError::Forbidden;
    }
    drain_refusal(state.is_cluster_mode())
}

/// Why `/node/drain` refuses on a node in `cluster_mode` or not.
fn drain_refusal(cluster_mode: bool) -> ClusterAdminError {
    if cluster_mode {
        ClusterAdminError::NotImplemented(DRAIN_NOT_IMPLEMENTED.to_string())
    } else {
        ClusterAdminError::NotClusterMode
    }
}

/// GET /api/admin/cluster/repair-status
///
/// Anti-entropy and tombstone GC on this node since it started.
pub async fn repair_status(
    State(state): State<AppState>,
    Extension(claims): Extension<JwtClaims>,
) -> Result<Json<serde_json::Value>, ClusterAdminError> {
    if !claims.role.can_admin() {
        return Err(ClusterAdminError::Forbidden);
    }

    Ok(Json(match state.cluster_admin_service.repair_status() {
        Some(status) => json!(status),
        None => json!({
            "mode": "standalone",
            "message": "Repair status not available in standalone mode"
        }),
    }))
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
    /// The route exists but does nothing yet; the message says what to do instead.
    NotImplemented(String),
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
            Self::NotImplemented(msg) => (
                StatusCode::NOT_IMPLEMENTED,
                Json(json!({"error": msg, "code": "NotImplemented"})),
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

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;

    async fn status_and_body(error: ClusterAdminError) -> (StatusCode, serde_json::Value) {
        let response = error.into_response();
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        (status, serde_json::from_slice(&bytes).unwrap())
    }

    #[tokio::test]
    async fn drain_in_cluster_mode_is_refused_as_not_implemented() {
        let (status, body) = status_and_body(drain_refusal(true)).await;
        assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
        assert_eq!(body["code"], "NotImplemented");
        assert_eq!(body["error"], DRAIN_NOT_IMPLEMENTED);
        assert!(DRAIN_NOT_IMPLEMENTED.contains("SIGTERM"));
    }

    #[tokio::test]
    async fn drain_outside_cluster_mode_still_asks_for_cluster_mode() {
        let (status, body) = status_and_body(drain_refusal(false)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body["error"].as_str().unwrap().contains("cluster mode"));
    }
}
