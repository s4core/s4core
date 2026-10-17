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

//! Admin API handlers for Enterprise Erasure Coding operations.
//!
//! The route layer is edition-neutral. It calls the `s4-cluster` trait object
//! injected into `AppState`; CE and unlicensed EE builds use a noop service that
//! returns an explicit Enterprise-required error.

use axum::{
    body::Bytes,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Extension, Json,
};
use s4_cluster::{
    ec_admin::{EcAdminAddSetRequest, EcAdminApplyTopologyRequest, EcAdminDeactivateSetRequest},
    ClusterError, EcAdminApproveRejoinRequest, EcAdminCodecHookRequest, EcAdminDedupReportRequest,
    EcAdminObjectRef, EcAdminOrphanGcRequest, EcAdminRepairRequest, EcAdminRepairStatusRequest,
    EcAdminTestHookRequest, EcAdminTieringDryRunRequest, EcAdminTieringPolicyRequest,
    EcAdminTieringPromoteRequest, EcAdminTombstoneGcRequest, EcAdminTranscodePauseRequest,
    EcAdminUnpackSegmentsRequest, DEFAULT_EC_ADMIN_REPAIR_MAX_TASKS,
};
use s4_features::iam::AdminPrincipal;
use serde::Deserialize;
use serde_json::json;

use crate::AppState;

/// Query parameters accepted by EC object admin endpoints.
#[derive(Debug, Deserialize, Default)]
pub struct EcObjectQuery {
    /// Optional object version ID.
    #[serde(rename = "versionId")]
    pub version_id: Option<String>,
}

/// Query parameters accepted by the repair-status endpoint.
#[derive(Debug, Deserialize, Default)]
pub struct EcRepairStatusQuery {
    /// Slot owner whose rebuild progress is asked about.
    #[serde(rename = "node-id")]
    pub node_id: Option<String>,
}

/// Query parameters accepted by orphan-list endpoint.
#[derive(Debug, Deserialize, Default)]
pub struct EcOrphanListQuery {
    /// Maximum records returned.
    #[serde(rename = "max-entries")]
    pub max_entries: Option<usize>,
}

#[derive(Debug, Deserialize, Default)]
struct EcRepairBody {
    #[serde(default)]
    object: Option<EcAdminObjectRef>,
    #[serde(default)]
    chunk_index: Option<u32>,
    #[serde(default)]
    slot_index: Option<u8>,
    #[serde(default)]
    node_id: Option<String>,
    #[serde(default)]
    max_tasks: Option<usize>,
}

#[derive(Debug, Deserialize, Default)]
struct EcOrphanGcBody {
    #[serde(default)]
    dry_run: Option<bool>,
    #[serde(default)]
    max_entries: Option<usize>,
}

#[derive(Debug, Deserialize, Default)]
struct EcTombstoneGcBody {
    /// `dry_run` or `run_once`, for a caller that prefers a verb to a flag.
    #[serde(default)]
    action: Option<String>,
    #[serde(default)]
    dry_run: Option<bool>,
    #[serde(default)]
    max_entries: Option<usize>,
    #[serde(default)]
    cursor: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct EcUnpackSegmentsBody {
    #[serde(default)]
    segment_id: Option<String>,
    #[serde(default)]
    dry_run: Option<bool>,
    #[serde(default)]
    max_segments: Option<usize>,
}

/// Query parameters accepted by the dedup measurement endpoint.
#[derive(Debug, Deserialize, Default)]
pub struct EcDedupReportQuery {
    /// Manifests one node may walk.
    #[serde(rename = "max-entries")]
    pub max_entries: Option<usize>,
    /// Measure this node's own manifests only, without asking the pool.
    #[serde(rename = "local-only")]
    pub local_only: Option<bool>,
}

#[derive(Debug, Deserialize, Default)]
struct EcCodecHookBody {
    /// Defaults to false, so an empty body disarms rather than arms.
    #[serde(default)]
    enabled: bool,
}

#[derive(Debug, Deserialize, Default)]
struct EcTestHookBody {
    bucket: String,
    key: String,
    #[serde(default)]
    version_id: Option<String>,
    mode: String,
}

#[derive(Debug, Deserialize, Default)]
struct EcTranscodePauseBody {
    action: String,
    bucket: String,
    key: String,
    #[serde(default)]
    version_id: Option<String>,
    state: String,
    mode: String,
}

/// Body of the apply-topology endpoint. Every field is optional.
#[derive(Debug, Deserialize, Default)]
struct EcApplyTopologyBody {
    /// The one set to lay out again; every set of the pool when absent.
    #[serde(default)]
    set_id: Option<u64>,
    /// Report what would change and write nothing.
    #[serde(default)]
    dry_run: bool,
}

#[derive(Debug, Deserialize, Default)]
struct EcAddSetBody {
    #[serde(default)]
    node_ids: Vec<String>,
}

#[derive(Debug, Deserialize, Default)]
struct EcApproveRejoinBody {
    #[serde(default)]
    node_id: String,
}

/// GET /api/admin/ec/pools
pub async fn list_pools(
    State(state): State<AppState>,
    Extension(principal): Extension<AdminPrincipal>,
) -> Result<Json<serde_json::Value>, EcAdminHttpError> {
    ensure_admin(&principal)?;
    Ok(Json(state.ec_admin_service.list_pools().await?))
}

/// GET /api/admin/ec/pools/:pool/health
pub async fn pool_health(
    State(state): State<AppState>,
    Extension(principal): Extension<AdminPrincipal>,
    Path(pool): Path<String>,
) -> Result<Json<serde_json::Value>, EcAdminHttpError> {
    ensure_admin(&principal)?;
    Ok(Json(state.ec_admin_service.pool_health(&pool).await?))
}

/// GET /api/admin/ec/pools/:pool/failure-matrix
pub async fn failure_matrix(
    State(state): State<AppState>,
    Extension(principal): Extension<AdminPrincipal>,
    Path(pool): Path<String>,
) -> Result<Json<serde_json::Value>, EcAdminHttpError> {
    ensure_admin(&principal)?;
    Ok(Json(state.ec_admin_service.failure_matrix(&pool).await?))
}

/// GET /api/admin/ec/objects/:bucket/*key where `key` ends with `/manifest`.
pub async fn object_manifest(
    State(state): State<AppState>,
    Extension(principal): Extension<AdminPrincipal>,
    Path((bucket, tail)): Path<(String, String)>,
    Query(query): Query<EcObjectQuery>,
) -> Result<Json<serde_json::Value>, EcAdminHttpError> {
    ensure_admin(&principal)?;
    let key = object_key_from_tail(&tail, "manifest")?;
    let object = EcAdminObjectRef {
        bucket,
        key,
        version_id: query.version_id,
    };
    Ok(Json(state.ec_admin_service.object_manifest(object).await?))
}

/// POST /api/admin/ec/objects/:bucket/*key where `key` ends with `/verify`.
pub async fn verify_object(
    State(state): State<AppState>,
    Extension(principal): Extension<AdminPrincipal>,
    Path((bucket, tail)): Path<(String, String)>,
    Query(query): Query<EcObjectQuery>,
) -> Result<Json<serde_json::Value>, EcAdminHttpError> {
    ensure_admin(&principal)?;
    let key = object_key_from_tail(&tail, "verify")?;
    let object = EcAdminObjectRef {
        bucket,
        key,
        version_id: query.version_id,
    };
    Ok(Json(state.ec_admin_service.verify_object(object).await?))
}

/// POST /api/admin/ec/pools/:pool/repair
pub async fn repair_pool(
    State(state): State<AppState>,
    Extension(principal): Extension<AdminPrincipal>,
    Path(pool): Path<String>,
    body: Bytes,
) -> Result<Json<serde_json::Value>, EcAdminHttpError> {
    ensure_admin(&principal)?;
    let body: EcRepairBody = parse_optional_body(body)?;
    let request = EcAdminRepairRequest {
        pool,
        object: body.object,
        chunk_index: body.chunk_index,
        slot_index: body.slot_index,
        node_id: body.node_id,
        max_tasks: body.max_tasks.unwrap_or(DEFAULT_EC_ADMIN_REPAIR_MAX_TASKS),
    };
    Ok(Json(state.ec_admin_service.repair_pool(request).await?))
}

/// `GET /api/admin/ec/pools/:pool/repair-status[?node-id=<uuid>]`
pub async fn repair_status(
    State(state): State<AppState>,
    Extension(principal): Extension<AdminPrincipal>,
    Path(pool): Path<String>,
    Query(query): Query<EcRepairStatusQuery>,
) -> Result<Json<serde_json::Value>, EcAdminHttpError> {
    ensure_admin(&principal)?;
    let request = EcAdminRepairStatusRequest {
        pool,
        node_id: query.node_id,
    };
    Ok(Json(state.ec_admin_service.repair_status(request).await?))
}

/// POST /api/admin/ec/pools/:pool/sets
pub async fn add_erasure_set(
    State(state): State<AppState>,
    Extension(principal): Extension<AdminPrincipal>,
    Path(pool): Path<String>,
    body: Bytes,
) -> Result<Json<serde_json::Value>, EcAdminHttpError> {
    ensure_admin(&principal)?;
    let body: EcAddSetBody = parse_body(body)?;
    let request = EcAdminAddSetRequest {
        pool,
        node_ids: body.node_ids,
    };
    Ok(Json(state.ec_admin_service.add_erasure_set(request).await?))
}

/// GET /api/admin/ec/pools/:pool/sets
pub async fn list_erasure_sets(
    State(state): State<AppState>,
    Extension(principal): Extension<AdminPrincipal>,
    Path(pool): Path<String>,
) -> Result<Json<serde_json::Value>, EcAdminHttpError> {
    ensure_admin(&principal)?;
    Ok(Json(state.ec_admin_service.list_erasure_sets(&pool).await?))
}

/// POST /api/admin/ec/pools/:pool/sets/:set_id/deactivate
pub async fn deactivate_erasure_set(
    State(state): State<AppState>,
    Extension(principal): Extension<AdminPrincipal>,
    Path((pool, set_id)): Path<(String, u64)>,
) -> Result<Json<serde_json::Value>, EcAdminHttpError> {
    ensure_admin(&principal)?;
    let request = EcAdminDeactivateSetRequest { pool, set_id };
    Ok(Json(
        state.ec_admin_service.deactivate_erasure_set(request).await?,
    ))
}

/// POST /api/admin/ec/pools/:pool/topology
///
/// The body is optional: with none, every set of the pool is laid out again by
/// this node's topology rule.
pub async fn apply_pool_topology(
    State(state): State<AppState>,
    Extension(principal): Extension<AdminPrincipal>,
    Path(pool): Path<String>,
    body: Bytes,
) -> Result<Json<serde_json::Value>, EcAdminHttpError> {
    ensure_admin(&principal)?;
    let body: EcApplyTopologyBody = parse_optional_body(body)?;
    let request = EcAdminApplyTopologyRequest {
        pool,
        set_id: body.set_id,
        dry_run: body.dry_run,
    };
    Ok(Json(
        state.ec_admin_service.apply_pool_topology(request).await?,
    ))
}

/// POST /api/admin/ec/pools/:pool/rejoin/approve
///
/// Addressed to the node that is held back, about itself: its verdict was
/// reached from its own storage, so no other node can lift it.
pub async fn approve_node_rejoin(
    State(state): State<AppState>,
    Extension(principal): Extension<AdminPrincipal>,
    Path(pool): Path<String>,
    body: Bytes,
) -> Result<Json<serde_json::Value>, EcAdminHttpError> {
    ensure_admin(&principal)?;
    let body: EcApproveRejoinBody = parse_body(body)?;
    let request = EcAdminApproveRejoinRequest {
        pool,
        node_id: body.node_id,
    };
    Ok(Json(
        state.ec_admin_service.approve_node_rejoin(request).await?,
    ))
}

/// GET /api/admin/ec/orphans
pub async fn list_orphans(
    State(state): State<AppState>,
    Extension(principal): Extension<AdminPrincipal>,
    Query(query): Query<EcOrphanListQuery>,
) -> Result<Json<serde_json::Value>, EcAdminHttpError> {
    ensure_admin(&principal)?;
    let max_entries = query.max_entries.unwrap_or(DEFAULT_EC_ADMIN_REPAIR_MAX_TASKS);
    Ok(Json(
        state.ec_admin_service.list_orphans(max_entries).await?,
    ))
}

/// GET /api/admin/ec/dedup/report
///
/// Measures how much erasure-coded content the pool stores more than once.
/// Read-only: it walks manifests and writes nothing.
///
/// The walk asks every node of the pool, because a node holds only the
/// manifests it transcoded or read, and a repeat split across two nodes is
/// invisible on both. `local-only=true` narrows it to this node, which answers
/// a different question and says so in its own report.
pub async fn dedup_report(
    State(state): State<AppState>,
    Extension(principal): Extension<AdminPrincipal>,
    Query(query): Query<EcDedupReportQuery>,
) -> Result<Json<serde_json::Value>, EcAdminHttpError> {
    ensure_admin(&principal)?;
    let request = EcAdminDedupReportRequest {
        max_entries: query.max_entries.unwrap_or(DEFAULT_EC_ADMIN_REPAIR_MAX_TASKS),
        local_only: query.local_only.unwrap_or(false),
    };
    Ok(Json(state.ec_admin_service.dedup_report(request).await?))
}

/// POST /api/admin/ec/orphans/gc
pub async fn gc_orphans(
    State(state): State<AppState>,
    Extension(principal): Extension<AdminPrincipal>,
    body: Bytes,
) -> Result<Json<serde_json::Value>, EcAdminHttpError> {
    ensure_admin(&principal)?;
    let body: EcOrphanGcBody = parse_optional_body(body)?;
    let request = EcAdminOrphanGcRequest {
        dry_run: body.dry_run.unwrap_or(false),
        max_entries: body.max_entries.unwrap_or(DEFAULT_EC_ADMIN_REPAIR_MAX_TASKS),
    };
    Ok(Json(state.ec_admin_service.gc_orphans(request).await?))
}

/// POST /api/admin/ec/tombstones/gc
///
/// Runs one EC tombstone GC cycle on this node now: releases the shards of
/// objects whose delete has reached every owner of their erasure set, and
/// retires the tombstone pool-wide once its second grace has passed. The
/// background sweep does the same work on a schedule; asking for it here is
/// what lets an operator reclaim space at a chosen moment, and an acceptance
/// scenario assert on the result rather than on a timer.
///
/// An ordinary operator action, and deliberately not a test hook: it removes
/// data, and a route under `/ec/test/` reads as something a production build
/// does not do. Its neighbour `/ec/orphans/gc` collects the other kind of dead
/// shard the same way.
///
/// Node-local, like the shards it releases. Sweeping a pool means asking each
/// of its nodes.
pub async fn gc_tombstones(
    State(state): State<AppState>,
    Extension(principal): Extension<AdminPrincipal>,
    body: Bytes,
) -> Result<Json<serde_json::Value>, EcAdminHttpError> {
    ensure_admin(&principal)?;
    let body: EcTombstoneGcBody = parse_optional_body(body)?;
    let dry_run = match body.action.as_deref() {
        Some("dry_run") => true,
        Some("run_once") | Some("run") => false,
        Some(other) => {
            return Err(EcAdminHttpError::BadRequest(format!(
                "EC tombstone GC action must be 'dry_run' or 'run_once', got '{other}'"
            )))
        }
        None => body.dry_run.unwrap_or(false),
    };
    let request = EcAdminTombstoneGcRequest {
        dry_run,
        max_entries: body.max_entries.unwrap_or(DEFAULT_EC_ADMIN_REPAIR_MAX_TASKS),
        cursor: body.cursor,
    };
    Ok(Json(
        state.ec_admin_service.run_tombstone_gc(request).await?,
    ))
}

/// POST /api/admin/ec/segments/unpack
///
/// Returns packed segments to ordinary RF=3 replication. This is the way back
/// out of packed segments: switching the feature off stops new segments but
/// leaves the existing ones packed, so an operator rolling the feature back has
/// to unpack what is already there.
///
/// With no body every segment that can be unpacked is; with `segment_id` only
/// that one. `dry_run` reports what would be touched and changes nothing.
pub async fn unpack_packed_segments(
    State(state): State<AppState>,
    Extension(principal): Extension<AdminPrincipal>,
    body: Bytes,
) -> Result<Json<serde_json::Value>, EcAdminHttpError> {
    ensure_admin(&principal)?;
    let body: EcUnpackSegmentsBody = parse_optional_body(body)?;
    let request = EcAdminUnpackSegmentsRequest {
        segment_id: body.segment_id,
        dry_run: body.dry_run.unwrap_or(false),
        max_segments: body.max_segments.unwrap_or(DEFAULT_EC_ADMIN_REPAIR_MAX_TASKS),
    };
    Ok(Json(
        state.ec_admin_service.unpack_packed_segments(request).await?,
    ))
}

/// POST /api/admin/ec/test/fail-reconstruction
pub async fn test_fail_reconstruction(
    State(state): State<AppState>,
    Extension(principal): Extension<AdminPrincipal>,
    body: Bytes,
) -> Result<Json<serde_json::Value>, EcAdminHttpError> {
    ensure_admin(&principal)?;
    let body: EcTestHookBody = parse_optional_body(body)?;
    let request = EcAdminTestHookRequest {
        bucket: body.bucket,
        key: body.key,
        version_id: body.version_id,
        mode: body.mode,
    };
    Ok(Json(state.ec_admin_service.test_hook(request).await?))
}

/// POST /api/admin/ec/test/transcode/pause
///
/// Arms, inspects or releases a one-shot stop of the staged transcode pipeline
/// at a named state. The staged states are too short-lived to observe from
/// outside, and the crash-safety scenarios need the object to sit in one while
/// they look at it. Refused unless EC test hooks are enabled for the process.
pub async fn test_transcode_pause(
    State(state): State<AppState>,
    Extension(principal): Extension<AdminPrincipal>,
    body: Bytes,
) -> Result<Json<serde_json::Value>, EcAdminHttpError> {
    ensure_admin(&principal)?;
    let body: EcTranscodePauseBody = parse_optional_body(body)?;
    let request = EcAdminTranscodePauseRequest {
        action: body.action,
        bucket: body.bucket,
        key: body.key,
        version_id: body.version_id,
        state: body.state,
        mode: body.mode,
    };
    Ok(Json(
        state.ec_admin_service.transcode_pause_hook(request).await?,
    ))
}

/// POST /api/admin/ec/test/orphans/scan
///
/// Sweeps this node's shards for ones no live manifest references and records
/// what it finds, so the result can be read back from
/// `GET /api/admin/ec/orphans`. The background scrub looks for the same thing
/// on its schedule; this makes the look happen now. Refused unless EC test
/// hooks are enabled for the process.
pub async fn test_scan_orphans(
    State(state): State<AppState>,
    Extension(principal): Extension<AdminPrincipal>,
    Query(query): Query<EcOrphanListQuery>,
) -> Result<Json<serde_json::Value>, EcAdminHttpError> {
    ensure_admin(&principal)?;
    let max_entries = query.max_entries.unwrap_or(DEFAULT_EC_ADMIN_REPAIR_MAX_TASKS);
    Ok(Json(
        state.ec_admin_service.scan_orphans(max_entries).await?,
    ))
}

/// POST /api/admin/ec/test/codec/foreign-matrix
///
/// Makes the accelerated codec encode with a matrix that is not S4's, so the
/// mixed-cluster acceptance test can prove its byte-equality check is able to
/// fail. Refused unless EC test hooks are enabled for the process.
pub async fn test_codec_foreign_matrix(
    State(state): State<AppState>,
    Extension(principal): Extension<AdminPrincipal>,
    body: Bytes,
) -> Result<Json<serde_json::Value>, EcAdminHttpError> {
    ensure_admin(&principal)?;
    let body: EcCodecHookBody = parse_optional_body(body)?;
    let request = EcAdminCodecHookRequest {
        enabled: body.enabled,
    };
    Ok(Json(state.ec_admin_service.codec_test_hook(request).await?))
}

/// POST /api/admin/ec/test/scrub/run
///
/// Runs one local shard scrub pass immediately. The background worker does the
/// same on a schedule; this makes the result observable at a chosen moment.
pub async fn test_run_scrub(
    State(state): State<AppState>,
    Extension(principal): Extension<AdminPrincipal>,
) -> Result<Json<serde_json::Value>, EcAdminHttpError> {
    ensure_admin(&principal)?;
    Ok(Json(state.ec_admin_service.run_scrub().await?))
}

/// POST /api/admin/ec/test/anti-entropy/run
///
/// Audits local shard inventory against every manifest immediately.
pub async fn test_run_anti_entropy(
    State(state): State<AppState>,
    Extension(principal): Extension<AdminPrincipal>,
) -> Result<Json<serde_json::Value>, EcAdminHttpError> {
    ensure_admin(&principal)?;
    Ok(Json(state.ec_admin_service.run_anti_entropy().await?))
}

/// POST /api/admin/ec/test/namespace/reconcile
///
/// Puts back namespace entries that erasure-coded objects are missing. The
/// anti-entropy worker does the same on its schedule; this makes it observable
/// at a chosen moment.
pub async fn test_reconcile_namespace(
    State(state): State<AppState>,
    Extension(principal): Extension<AdminPrincipal>,
) -> Result<Json<serde_json::Value>, EcAdminHttpError> {
    ensure_admin(&principal)?;
    Ok(Json(state.ec_admin_service.reconcile_namespace().await?))
}

/// GET /api/admin/ec/tiering/:bucket
///
/// Returns the stored bucket policy and the values in force after inheritance.
pub async fn tiering_policy(
    State(state): State<AppState>,
    Extension(principal): Extension<AdminPrincipal>,
    Path(bucket): Path<String>,
) -> Result<Json<serde_json::Value>, EcAdminHttpError> {
    ensure_admin(&principal)?;
    Ok(Json(state.ec_admin_service.tiering_policy(bucket).await?))
}

/// PUT /api/admin/ec/tiering/:bucket
///
/// Stores a bucket policy. An empty body clears every override, which is how a
/// bucket goes back to inheriting the node's values.
pub async fn put_tiering_policy(
    State(state): State<AppState>,
    Extension(principal): Extension<AdminPrincipal>,
    Path(bucket): Path<String>,
    body: Bytes,
) -> Result<Json<serde_json::Value>, EcAdminHttpError> {
    ensure_admin(&principal)?;
    let request = parse_optional_body::<EcAdminTieringPolicyRequest>(body)?;
    Ok(Json(
        state.ec_admin_service.put_tiering_policy(bucket, request).await?,
    ))
}

/// POST /api/admin/ec/tiering/dry-run
///
/// Reports what tiering would do right now and changes nothing.
pub async fn tiering_dry_run(
    State(state): State<AppState>,
    Extension(principal): Extension<AdminPrincipal>,
    body: Bytes,
) -> Result<Json<serde_json::Value>, EcAdminHttpError> {
    ensure_admin(&principal)?;
    let request = parse_body::<EcAdminTieringDryRunRequest>(body)?;
    Ok(Json(state.ec_admin_service.tiering_dry_run(request).await?))
}

/// POST /api/admin/ec/tiering/:bucket/promote
///
/// Brings erasure-coded objects under a prefix back to RF=3. This is the
/// documented rollback for auto-tiering, so it answers whether or not automatic
/// reverse tiering is switched on: an operator who turned the feature off
/// during an incident must not lose the way to undo what it did.
pub async fn promote_objects(
    State(state): State<AppState>,
    Extension(principal): Extension<AdminPrincipal>,
    Path(bucket): Path<String>,
    body: Bytes,
) -> Result<Json<serde_json::Value>, EcAdminHttpError> {
    ensure_admin(&principal)?;
    let mut request = parse_optional_body::<EcAdminTieringPromoteRequest>(body)?;
    // The path names the bucket. A body naming a different one would be two
    // answers to the same question, so the path wins and the body cannot
    // redirect the command at a bucket the caller did not ask for.
    request.bucket = bucket;
    Ok(Json(state.ec_admin_service.promote_objects(request).await?))
}

/// POST /api/admin/ec/test/promotion/run
///
/// Advances the reverse migration queue immediately and returns what it did.
/// The background worker does the same on its interval, which is not a schedule
/// an acceptance test can wait for.
pub async fn test_run_promotion_cycle(
    State(state): State<AppState>,
    Extension(principal): Extension<AdminPrincipal>,
) -> Result<Json<serde_json::Value>, EcAdminHttpError> {
    ensure_admin(&principal)?;
    Ok(Json(state.ec_admin_service.run_promotion_cycle().await?))
}

/// GET /api/admin/ec/test/promotion/:bucket/*key
///
/// Returns this node's reverse migration job for the object, if it has one.
/// The queue is invisible to S3 clients and, until the promoted manifest is
/// published, invisible to every other admin route as well, so without this
/// route a promotion that was never queued looks exactly like one that is
/// queued and waiting.
pub async fn test_promotion_job(
    State(state): State<AppState>,
    Extension(principal): Extension<AdminPrincipal>,
    Path((bucket, tail)): Path<(String, String)>,
    Query(query): Query<EcObjectQuery>,
) -> Result<Json<serde_json::Value>, EcAdminHttpError> {
    ensure_admin(&principal)?;
    // The whole tail is the object key, as on the activity-mark route: there is
    // no trailing action segment to strip.
    let key = tail.trim_start_matches('/').to_string();
    if key.is_empty() {
        return Err(EcAdminHttpError::BadRequest(
            "EC object admin URL must include an object key".to_string(),
        ));
    }
    let object = EcAdminObjectRef {
        bucket,
        key,
        version_id: query.version_id,
    };
    Ok(Json(state.ec_admin_service.promotion_job(object).await?))
}

/// GET /api/admin/ec/test/transcode/job/:bucket/*key
///
/// This node's forward migration queue entry for one object. The counterpart of
/// the promotion job route, and needed for the same reason: a staged transcode
/// publishes no manifest until it commits, so until then a migration in
/// progress is indistinguishable from one that was never queued.
pub async fn test_transcode_job(
    State(state): State<AppState>,
    Extension(principal): Extension<AdminPrincipal>,
    Path((bucket, tail)): Path<(String, String)>,
    Query(query): Query<EcObjectQuery>,
) -> Result<Json<serde_json::Value>, EcAdminHttpError> {
    ensure_admin(&principal)?;
    // The whole tail is the object key, as on the promotion job route: there is
    // no trailing action segment to strip.
    let key = tail.trim_start_matches('/').to_string();
    if key.is_empty() {
        return Err(EcAdminHttpError::BadRequest(
            "EC object admin URL must include an object key".to_string(),
        ));
    }
    let object = EcAdminObjectRef {
        bucket,
        key,
        version_id: query.version_id,
    };
    Ok(Json(state.ec_admin_service.transcode_job(object).await?))
}

/// POST /api/admin/ec/test/tiering/run
///
/// Runs one auto-tiering scan cycle immediately and returns what it did. The
/// background worker does the same on an interval measured in hours, which is
/// not a schedule an acceptance test can wait for.
pub async fn test_run_tiering_cycle(
    State(state): State<AppState>,
    Extension(principal): Extension<AdminPrincipal>,
) -> Result<Json<serde_json::Value>, EcAdminHttpError> {
    ensure_admin(&principal)?;
    Ok(Json(state.ec_admin_service.run_tiering_cycle().await?))
}

/// GET /api/admin/ec/test/tiering/heat/:bucket/*key
///
/// Returns the activity mark this node recorded for the object, if any. The
/// mark never reaches an S3 client, so without this route the tracker has no
/// observable behaviour to assert on.
pub async fn test_object_heat(
    State(state): State<AppState>,
    Extension(principal): Extension<AdminPrincipal>,
    Path((bucket, tail)): Path<(String, String)>,
    Query(query): Query<EcObjectQuery>,
) -> Result<Json<serde_json::Value>, EcAdminHttpError> {
    ensure_admin(&principal)?;
    // The whole tail is the object key here: unlike the manifest and verify
    // routes, this one carries no trailing action segment to strip.
    let key = tail.trim_start_matches('/').to_string();
    if key.is_empty() {
        return Err(EcAdminHttpError::BadRequest(
            "EC object admin URL must include an object key".to_string(),
        ));
    }
    let object = EcAdminObjectRef {
        bucket,
        key,
        version_id: query.version_id,
    };
    Ok(Json(state.ec_admin_service.object_heat(object).await?))
}

fn ensure_admin(principal: &AdminPrincipal) -> Result<(), EcAdminHttpError> {
    if principal.role.can_admin() {
        Ok(())
    } else {
        Err(EcAdminHttpError::Forbidden)
    }
}

fn parse_body<T>(body: Bytes) -> Result<T, EcAdminHttpError>
where
    T: for<'de> Deserialize<'de>,
{
    if body.is_empty() {
        return Err(EcAdminHttpError::BadRequest(
            "request body is required".to_string(),
        ));
    }
    serde_json::from_slice(&body).map_err(|error| EcAdminHttpError::BadRequest(error.to_string()))
}

fn parse_optional_body<T>(body: Bytes) -> Result<T, EcAdminHttpError>
where
    T: Default + for<'de> Deserialize<'de>,
{
    if body.is_empty() {
        return Ok(T::default());
    }
    serde_json::from_slice(&body).map_err(|error| EcAdminHttpError::BadRequest(error.to_string()))
}

fn object_key_from_tail(tail: &str, operation: &'static str) -> Result<String, EcAdminHttpError> {
    let tail = tail.trim_start_matches('/');
    let suffix = format!("/{operation}");
    let Some(key) = tail.strip_suffix(&suffix) else {
        return Err(EcAdminHttpError::BadRequest(format!(
            "EC object admin URL must end with '/{operation}'"
        )));
    };
    if key.is_empty() {
        return Err(EcAdminHttpError::BadRequest(
            "EC object admin URL must include an object key".to_string(),
        ));
    }
    Ok(key.to_string())
}

/// Error type for EC admin endpoints.
#[derive(Debug)]
pub enum EcAdminHttpError {
    /// Caller lacks required permissions.
    Forbidden,
    /// Request body or query is invalid.
    BadRequest(String),
    /// Underlying EC admin service failed.
    Cluster(ClusterError),
}

impl IntoResponse for EcAdminHttpError {
    fn into_response(self) -> Response {
        match self {
            Self::Forbidden => (
                StatusCode::FORBIDDEN,
                Json(json!({"error": "Forbidden: SuperUser role required"})),
            )
                .into_response(),
            Self::BadRequest(message) => {
                (StatusCode::BAD_REQUEST, Json(json!({"error": message}))).into_response()
            }
            Self::Cluster(error) => cluster_error_response(error),
        }
    }
}

impl From<ClusterError> for EcAdminHttpError {
    fn from(error: ClusterError) -> Self {
        Self::Cluster(error)
    }
}

fn cluster_error_response(error: ClusterError) -> Response {
    let status = match &error {
        ClusterError::EnterpriseRequired(_) | ClusterError::LicenseLimit(_) => {
            StatusCode::FORBIDDEN
        }
        ClusterError::Config(_) | ClusterError::Placement(_) => StatusCode::BAD_REQUEST,
        ClusterError::ObjectNotFound { .. } => StatusCode::NOT_FOUND,
        // Too few replicas to answer safely is unavailability, not a fault, and
        // the object data path already says so with 503. An admin route that
        // answered 500 for the same condition would send the operator looking
        // for a bug while the real answer is "bring the nodes back".
        ClusterError::Unavailable(_)
        | ClusterError::QuorumNotMet { .. }
        | ClusterError::ReadTimeout(_)
        | ClusterError::WriteTimeout(_) => StatusCode::SERVICE_UNAVAILABLE,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (status, Json(json!({"error": error.to_string()}))).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn malformed_tombstone_cursor_is_a_client_error() {
        let request = EcAdminTombstoneGcRequest {
            dry_run: true,
            max_entries: 1,
            cursor: Some("not-hex".to_string()),
        };
        let error = request.validate().unwrap_err();

        assert_eq!(
            cluster_error_response(error).status(),
            StatusCode::BAD_REQUEST
        );
    }

    #[test]
    fn tombstone_gc_body_accepts_an_optional_cursor() {
        let first: EcTombstoneGcBody = serde_json::from_str(r#"{"max_entries":2}"#).unwrap();
        let continued: EcTombstoneGcBody =
            serde_json::from_str(r#"{"max_entries":2,"cursor":"00ff"}"#).unwrap();

        assert_eq!(first.cursor, None);
        assert_eq!(continued.cursor.as_deref(), Some("00ff"));
    }
}
