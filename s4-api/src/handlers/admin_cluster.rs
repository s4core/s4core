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
//! | GET    | /api/admin/cluster/rebalance     | The pool's growth on this node     |
//! | POST   | /api/admin/cluster/rebalance/pause  | Pause the move of the pool's keys |
//! | POST   | /api/admin/cluster/rebalance/resume | Resume it                       |
//!
//! All endpoints require `SuperUser` role via JWT authentication.
//!
//! A cluster node answers what its [`s4_cluster::ClusterAdminService`]
//! reports, read from the state its metrics are written from. A single node
//! has nothing to report and answers with the fixed values it always has.

use axum::{
    body::Bytes,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    Extension, Json,
};
use s4_cluster::rebalance::GrowthControlError;
use s4_cluster::{PROTOCOL_VERSION, S4_VERSION};
use s4_features::iam::AdminPrincipal;
use serde::Deserialize;
use serde_json::json;

use crate::AppState;

/// GET /api/admin/cluster/health
///
/// The pool's health as this node sees it.
///
/// A cluster node adds `unenforced_bucket_configs`: the versioning and Object
/// Lock configurations its own storage holds from before cluster mode refused
/// them, which it does not enforce, as its background count last found them.
/// Empty when there are none.
pub async fn cluster_health(
    State(state): State<AppState>,
    Extension(principal): Extension<AdminPrincipal>,
) -> Result<Json<serde_json::Value>, ClusterAdminError> {
    if !principal.role.can_admin() {
        return Err(ClusterAdminError::Forbidden);
    }

    Ok(Json(match state.cluster_admin_service.cluster_health() {
        Some(health) => {
            let unenforced = state.unenforced_configs.get();
            let mut health = json!(health);
            if let Some(fields) = health.as_object_mut() {
                fields.insert("unenforced_bucket_configs".to_string(), json!(unenforced));
            }
            health
        }
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
    Extension(principal): Extension<AdminPrincipal>,
) -> Result<Json<serde_json::Value>, ClusterAdminError> {
    if !principal.role.can_admin() {
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
    Extension(principal): Extension<AdminPrincipal>,
) -> Result<Json<serde_json::Value>, ClusterAdminError> {
    if !principal.role.can_admin() {
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
    Extension(principal): Extension<AdminPrincipal>,
) -> ClusterAdminError {
    if !principal.role.can_admin() {
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
    Extension(principal): Extension<AdminPrincipal>,
) -> Result<Json<serde_json::Value>, ClusterAdminError> {
    if !principal.role.can_admin() {
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

/// GET /api/admin/cluster/rebalance
///
/// The pool's growth as this node is at it: the generation, the state of the
/// growth and why it waits, the ranges of the ring by state, and the move of
/// this node's keys to the new owners with what is left and an estimate of
/// the time. The same numbers as the `s4_pool_generation` and
/// `s4_rebalance_*` metrics of this node.
pub async fn rebalance_status(
    State(state): State<AppState>,
    Extension(principal): Extension<AdminPrincipal>,
) -> Result<Json<serde_json::Value>, ClusterAdminError> {
    if !principal.role.can_admin() {
        return Err(ClusterAdminError::Forbidden);
    }

    Ok(Json(match state.cluster_admin_service.pool_growth() {
        Some(growth) => json!(growth.report()),
        None => json!({
            "mode": "standalone",
            "message": "A single node has no pool to grow"
        }),
    }))
}

/// The body of `POST /api/admin/cluster/rebalance/pause`, which may be empty.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PauseRequest {
    /// Why the move is paused, shown by every member's status and log.
    pub reason: Option<String>,
}

/// POST /api/admin/cluster/rebalance/pause
///
/// Pauses the move of the pool's keys on every member: the pause is written
/// through the quorums, applied on this node at once and by every other
/// member's driver within a few seconds, and outlives restarts until it is
/// resumed. Writes keep reaching both generations meanwhile. `409` when the
/// pool does not grow, `503` when the pause could not be written.
pub async fn pause_rebalance(
    State(state): State<AppState>,
    Extension(principal): Extension<AdminPrincipal>,
    body: Bytes,
) -> Result<Json<serde_json::Value>, ClusterAdminError> {
    if !principal.role.can_admin() {
        return Err(ClusterAdminError::Forbidden);
    }
    let request = parse_pause_request(&body)?;
    let growth = state
        .cluster_admin_service
        .pool_growth()
        .ok_or(ClusterAdminError::NotClusterMode)?;
    Ok(Json(json!(growth.pause(request.reason).await?)))
}

/// POST /api/admin/cluster/rebalance/resume
///
/// Resumes the move of the pool's keys on every member, as a pause does.
pub async fn resume_rebalance(
    State(state): State<AppState>,
    Extension(principal): Extension<AdminPrincipal>,
) -> Result<Json<serde_json::Value>, ClusterAdminError> {
    if !principal.role.can_admin() {
        return Err(ClusterAdminError::Forbidden);
    }
    let growth = state
        .cluster_admin_service
        .pool_growth()
        .ok_or(ClusterAdminError::NotClusterMode)?;
    Ok(Json(json!(growth.resume().await?)))
}

/// The pause request in `body`: empty, or a JSON object with `reason`.
fn parse_pause_request(body: &[u8]) -> Result<PauseRequest, ClusterAdminError> {
    if body.iter().all(u8::is_ascii_whitespace) {
        return Ok(PauseRequest::default());
    }
    serde_json::from_slice(body).map_err(|e| {
        ClusterAdminError::BadRequest(format!(
            "the body is empty or a JSON object with \"reason\": {e}"
        ))
    })
}

impl From<GrowthControlError> for ClusterAdminError {
    fn from(err: GrowthControlError) -> Self {
        match err {
            GrowthControlError::NotGrowing(msg) => Self::Conflict(msg),
            GrowthControlError::Unavailable(msg) => Self::Unavailable(msg),
        }
    }
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
    /// The request is malformed.
    BadRequest(String),
    /// The cluster is not in a state the operation applies to.
    Conflict(String),
    /// The cluster could not carry the operation out now; it may be retried.
    Unavailable(String),
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
            Self::BadRequest(msg) => (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": msg, "code": "BadRequest"})),
            )
                .into_response(),
            Self::Conflict(msg) => (
                StatusCode::CONFLICT,
                Json(json!({"error": msg, "code": "Conflict"})),
            )
                .into_response(),
            Self::Unavailable(msg) => (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": msg, "code": "ServiceUnavailable"})),
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

    #[test]
    fn a_pause_takes_an_empty_body_or_a_reason() {
        assert_eq!(parse_pause_request(b"").unwrap().reason, None);
        assert_eq!(parse_pause_request(b" \n").unwrap().reason, None);
        assert_eq!(
            parse_pause_request(br#"{"reason":"night window"}"#).unwrap().reason.as_deref(),
            Some("night window")
        );
        assert!(matches!(
            parse_pause_request(br#"{"why":"x"}"#),
            Err(ClusterAdminError::BadRequest(_))
        ));
        assert!(matches!(
            parse_pause_request(b"not json"),
            Err(ClusterAdminError::BadRequest(_))
        ));
    }

    #[tokio::test]
    async fn growth_refusals_answer_conflict_and_unavailable() {
        let (status, body) =
            status_and_body(GrowthControlError::NotGrowing("the pool does not grow".into()).into())
                .await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(body["code"], "Conflict");
        let (status, body) =
            status_and_body(GrowthControlError::Unavailable("no quorum".into()).into()).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["error"], "no quorum");
    }

    #[tokio::test]
    async fn drain_outside_cluster_mode_still_asks_for_cluster_mode() {
        let (status, body) = status_and_body(drain_refusal(false)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body["error"].as_str().unwrap().contains("cluster mode"));
    }
}
