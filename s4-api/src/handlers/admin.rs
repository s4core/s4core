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

//! Admin API handlers for IAM user management.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Extension, Json,
};
use serde_json::json;

use s4_features::iam::{
    CreateUserRequest, IamError, JwtClaims, LoginRequest, UpdateUserRequest, UserResponse,
};

use s4_core::storage::StorageEngine;
use serde::Deserialize;
use tracing::{error, warn};

use crate::AppState;

/// Login endpoint - authenticate with username/password, returns JWT token.
///
/// POST /api/admin/login
/// Body: { "username": "...", "password": "..." }
/// Returns: { "token": "...", "expires_at": "..." }
pub async fn login(
    State(state): State<AppState>,
    Json(request): Json<LoginRequest>,
) -> Result<Json<serde_json::Value>, AdminError> {
    let response = state.auth_service.login(request).await.map_err(|err| match err {
        // A wrong password and an unknown account answer the same 401, so the
        // login endpoint cannot be used to enumerate account names.
        IamError::UserNotFound | IamError::InvalidCredentials => AdminError::InvalidCredentials,
        other => AdminError::from(other),
    })?;

    Ok(Json(json!({
        "token": response.token,
        "expires_at": response.expires_at,
    })))
}

/// List all users - requires SuperUser role.
///
/// GET /api/admin/users
/// Headers: `Authorization: Bearer <token>`
/// Returns: [{ "id": "...", "username": "...", "role": "...", ... }]
pub async fn list_users(
    State(state): State<AppState>,
    Extension(claims): Extension<JwtClaims>,
) -> Result<Json<serde_json::Value>, AdminError> {
    // Check permissions
    if !claims.role.can_admin() {
        return Err(AdminError::Forbidden);
    }

    let users = state.iam_storage.list_users().await?;
    let users: Vec<UserResponse> = users.into_iter().map(UserResponse::from).collect();

    Ok(Json(json!({ "users": users })))
}

/// Create new user - requires SuperUser role.
///
/// POST /api/admin/users
/// Headers: `Authorization: Bearer <token>`
/// Body: { "username": "...", "password": "...", "role": "Reader|Writer|SuperUser" }
/// Returns: { "id": "...", "username": "...", "role": "...", ... }
pub async fn create_user(
    State(state): State<AppState>,
    Extension(claims): Extension<JwtClaims>,
    Json(request): Json<CreateUserRequest>,
) -> Result<Json<serde_json::Value>, AdminError> {
    // Check permissions
    if !claims.role.can_admin() {
        return Err(AdminError::Forbidden);
    }

    let user = state
        .iam_storage
        .create_user(request.username, request.password, request.role)
        .await?;

    Ok(Json(json!(UserResponse::from(user))))
}

/// Get user by ID - requires SuperUser role.
///
/// GET /api/admin/users/:id
/// Headers: `Authorization: Bearer <token>`
/// Returns: { "id": "...", "username": "...", "role": "...", ... }
pub async fn get_user(
    State(state): State<AppState>,
    Extension(claims): Extension<JwtClaims>,
    Path(user_id): Path<String>,
) -> Result<Json<serde_json::Value>, AdminError> {
    // Check permissions
    if !claims.role.can_admin() {
        return Err(AdminError::Forbidden);
    }

    // Get user by ID using direct storage access
    let user_key = format!("__s4_iam_user_{}", user_id);
    let storage = state.storage.read().await;
    let (user_bytes, _) = storage
        .get_object("__system__", &user_key)
        .await
        .map_err(|_| AdminError::UserNotFound)?;

    let user: s4_features::iam::User = serde_json::from_slice(&user_bytes).map_err(|e| {
        AdminError::Storage(format!("Stored user record {user_id} is unreadable: {e}"))
    })?;

    Ok(Json(json!(UserResponse::from(user))))
}

/// Update user - requires SuperUser role.
///
/// PUT /api/admin/users/:id
/// Headers: `Authorization: Bearer <token>`
/// Body: { "password": "...", "role": "...", "is_active": true }
/// Returns: { "id": "...", "username": "...", "role": "...", ... }
pub async fn update_user(
    State(state): State<AppState>,
    Extension(claims): Extension<JwtClaims>,
    Path(user_id): Path<String>,
    Json(request): Json<UpdateUserRequest>,
) -> Result<Json<serde_json::Value>, AdminError> {
    // Check permissions
    if !claims.role.can_admin() {
        return Err(AdminError::Forbidden);
    }

    let user = state
        .iam_storage
        .update_user(&user_id, request.password, request.role, request.is_active)
        .await?;

    Ok(Json(json!(UserResponse::from(user))))
}

/// Delete user - requires SuperUser role.
///
/// DELETE /api/admin/users/:id
/// Headers: `Authorization: Bearer <token>`
/// Returns: 204 No Content
pub async fn delete_user(
    State(state): State<AppState>,
    Extension(claims): Extension<JwtClaims>,
    Path(user_id): Path<String>,
) -> Result<StatusCode, AdminError> {
    // Check permissions
    if !claims.role.can_admin() {
        return Err(AdminError::Forbidden);
    }

    state.iam_storage.delete_user(&user_id).await?;

    Ok(StatusCode::NO_CONTENT)
}

/// Generate S3 credentials for user - requires SuperUser role.
///
/// POST /api/admin/users/:id/credentials
/// Headers: `Authorization: Bearer <token>`
/// Returns: { "access_key": "...", "secret_key": "..." }
/// Note: Secret key is only shown once!
pub async fn generate_credentials(
    State(state): State<AppState>,
    Extension(claims): Extension<JwtClaims>,
    Path(user_id): Path<String>,
) -> Result<Json<serde_json::Value>, AdminError> {
    // Check permissions
    if !claims.role.can_admin() {
        return Err(AdminError::Forbidden);
    }

    let credentials = state.iam_storage.generate_s3_credentials(&user_id).await?;

    Ok(Json(json!(credentials)))
}

/// Delete S3 credentials for user - requires SuperUser role.
///
/// DELETE /api/admin/users/:id/credentials
/// Headers: `Authorization: Bearer <token>`
/// Returns: 204 No Content
pub async fn delete_credentials(
    State(state): State<AppState>,
    Extension(claims): Extension<JwtClaims>,
    Path(user_id): Path<String>,
) -> Result<StatusCode, AdminError> {
    // Check permissions
    if !claims.role.can_admin() {
        return Err(AdminError::Forbidden);
    }

    state.iam_storage.delete_s3_credentials(&user_id).await?;

    Ok(StatusCode::NO_CONTENT)
}

/// Per-bucket storage statistics.
///
/// GET /api/admin/bucket-stats
/// Headers: `Authorization: Bearer <token>`
/// Returns: { "buckets": [{ "name": "...", "objects_count": N, "storage_used_bytes": N,
/// "created_at": N }], "total_storage_bytes": N, "complete": true }
///
/// In cluster mode the numbers are the pool's, not this node's: buckets and
/// objects are listed from every node of the pool and merged by key, so each
/// object counts once, at the size of its newest version, wherever its copies
/// are. They are counted at most a few seconds ago (see [`super::usage`]).
/// `complete` is false when a node did not answer a listing: an object held
/// only by such a node is missing from the numbers.
pub async fn bucket_stats(
    State(state): State<AppState>,
    Extension(claims): Extension<JwtClaims>,
) -> Result<Json<serde_json::Value>, AdminError> {
    if !claims.role.can_admin() {
        return Err(AdminError::Forbidden);
    }

    let usage = super::usage::usage(&state).await.map_err(admin_usage_error)?;
    if !usage.complete {
        warn!("bucket statistics are incomplete: a node of the pool did not answer a listing");
    }
    let buckets: Vec<_> = usage
        .buckets
        .iter()
        .map(|bucket| {
            json!({
                "name": bucket.name,
                "objects_count": bucket.objects_count,
                "storage_used_bytes": bucket.storage_used_bytes,
                "created_at": bucket.created_at,
            })
        })
        .collect();

    Ok(Json(json!({
        "buckets": buckets,
        "total_storage_bytes": usage.storage_used_bytes(),
        "complete": usage.complete,
    })))
}

/// The admin answer for usage that could not be read.
fn admin_usage_error(error: super::usage::UsageError) -> AdminError {
    match error {
        super::usage::UsageError::Cluster(e) => admin_cluster_error(e),
        super::usage::UsageError::Storage(message) => AdminError::Storage(message),
    }
}

/// The admin answer for a quorum operation that failed: `503` when too few
/// replicas answered, `500` otherwise.
fn admin_cluster_error(error: s4_cluster::ClusterError) -> AdminError {
    match error {
        s4_cluster::ClusterError::QuorumNotMet { .. }
        | s4_cluster::ClusterError::WriteTimeout(_)
        | s4_cluster::ClusterError::ReadTimeout(_)
        | s4_cluster::ClusterError::EpochMismatch { .. } => {
            AdminError::Unavailable(error.to_string())
        }
        other => AdminError::Storage(other.to_string()),
    }
}

/// Create a bucket via admin API.
///
/// PUT /api/admin/buckets/:name
/// Headers: `Authorization: Bearer <token>`
/// Returns: 200 OK with { "name": "..." }
pub async fn create_bucket(
    State(state): State<AppState>,
    Extension(claims): Extension<JwtClaims>,
    Path(bucket_name): Path<String>,
) -> Result<Json<serde_json::Value>, AdminError> {
    if !claims.role.can_admin() {
        return Err(AdminError::Forbidden);
    }

    // The same naming rules the S3 path enforces: a bucket the admin API
    // accepted but S3 cannot address would be unusable.
    if let Err(e) = super::bucket::validate_bucket_name(&bucket_name) {
        return Err(AdminError::InvalidBucketName(e.to_string()));
    }

    // The same code the S3 route creates a bucket with: in cluster mode the
    // marker goes through the write quorum, so every node sees the bucket.
    super::bucket::create_bucket_marker(&state, &bucket_name)
        .await
        .map_err(|e| AdminError::from_bucket_change(&bucket_name, e))?;

    Ok(Json(json!({ "name": bucket_name })))
}

/// Query parameters for delete bucket.
#[derive(Debug, Deserialize)]
pub struct DeleteBucketQuery {
    /// When true, recursively delete all objects before removing the bucket.
    #[serde(default)]
    pub force: bool,
}

/// Delete a bucket via admin API.
///
/// DELETE /api/admin/buckets/:name?force=true
/// Headers: `Authorization: Bearer <token>`
/// Returns: 204 No Content
///
/// Without `force`, the bucket must be empty (returns 400 otherwise).
/// With `force=true`, all objects are deleted first, then the bucket itself.
pub async fn delete_bucket(
    State(state): State<AppState>,
    Extension(claims): Extension<JwtClaims>,
    Path(bucket_name): Path<String>,
    Query(query): Query<DeleteBucketQuery>,
) -> Result<StatusCode, AdminError> {
    if !claims.role.can_admin() {
        return Err(AdminError::Forbidden);
    }

    // The same code the S3 route deletes a bucket with: in cluster mode the
    // objects, the marker and the configuration go through the quorums.
    super::bucket::delete_bucket_and_contents(&state, &bucket_name, query.force)
        .await
        .map_err(|e| AdminError::from_bucket_change(&bucket_name, e))?;

    Ok(StatusCode::NO_CONTENT)
}

/// Query parameters for listing objects via admin API.
#[derive(Debug, Deserialize)]
pub struct ListObjectsQuery {
    /// Filter objects by prefix.
    pub prefix: Option<String>,
    /// Maximum number of keys to return.
    #[serde(rename = "max-keys")]
    pub max_keys: Option<usize>,
    /// Continuation token for pagination.
    #[serde(rename = "continuation-token")]
    pub continuation_token: Option<String>,
}

/// List objects in a bucket via admin API (no S3 signature needed).
///
/// GET /api/admin/buckets/:name/objects
/// Headers: `Authorization: Bearer <token>`
/// Returns: { "objects": [...], "common_prefixes": [...], "is_truncated": bool,
/// "next_continuation_token": "...", "complete": bool }
///
/// Keys below the prefix that contain another `/` are grouped into a
/// directory, listed once in `common_prefixes`. `max-keys` (default 50, at
/// most 1000, at least 1) counts objects and directories together. The bucket is read page
/// by page, never whole; on a cluster node from every node of the pool, merged
/// by key, and `complete` is false when a node did not answer.
pub async fn list_bucket_objects(
    State(state): State<AppState>,
    Extension(claims): Extension<JwtClaims>,
    Path(bucket_name): Path<String>,
    Query(params): Query<ListObjectsQuery>,
) -> Result<Json<serde_json::Value>, AdminError> {
    if !claims.role.can_admin() {
        return Err(AdminError::Forbidden);
    }

    // Without this check a deleted bucket and an empty one both answer 200 with
    // an empty list, and the caller cannot tell them apart. In cluster mode the
    // bucket is what a read quorum of its marker says, as on the S3 routes.
    match super::bucket_in_cluster(&state, &bucket_name).await {
        Some(Ok(true)) => {}
        Some(Ok(false)) => return Err(AdminError::BucketNotFound(bucket_name)),
        Some(Err(e)) => return Err(admin_cluster_error(e)),
        None => {
            let storage = state.storage.read().await;
            let marker_key = super::bucket_marker_key(&bucket_name);
            if storage.head_object("__system__", &marker_key).await.is_err() {
                return Err(AdminError::BucketNotFound(bucket_name));
            }
        }
    }

    let prefix = params.prefix.unwrap_or_default();
    // At least one: a page that could list nothing would hand back the token it
    // was given, and a client following tokens would never finish.
    let max_keys = params.max_keys.unwrap_or(50).clamp(1, 1000);

    let mut objects_list: Vec<serde_json::Value> = Vec::new();
    let mut common_prefixes: Vec<String> = Vec::new();
    let mut count = 0usize;
    let mut is_truncated = false;
    let mut complete = true;
    // Where the next page of the bucket starts, and where a client's next
    // request does: past the last object listed, or past the whole of the last
    // directory, so that a directory never comes back on the next page.
    let mut after = params.continuation_token.unwrap_or_default();
    let mut resume = after.clone();
    // The directory being skipped: its keys follow the one that listed it.
    let mut directory: Option<String> = None;

    'pages: loop {
        let page = super::usage::page(&state, &bucket_name, &prefix, &after)
            .await
            .map_err(admin_usage_error)?;
        complete &= page.complete;
        for listed in &page.objects {
            if directory.as_ref().is_some_and(|dir| listed.key.starts_with(dir.as_str())) {
                continue;
            }
            directory = None;
            if !super::usage::is_object_key(&listed.key) {
                continue;
            }
            if count >= max_keys {
                is_truncated = true;
                break 'pages;
            }
            let relative = listed.key.strip_prefix(prefix.as_str()).unwrap_or(&listed.key);
            if let Some(slash) = relative.find('/') {
                let dir = format!("{}{}/", prefix, &relative[..slash]);
                resume = format!("{dir}{DIRECTORY_END}");
                common_prefixes.push(dir.clone());
                directory = Some(dir);
            } else {
                resume = listed.key.clone();
                objects_list.push(json!({
                    "key": listed.key,
                    "size": listed.size,
                    "content_type": listed.content_type,
                    "last_modified": listed.modified_at,
                    "etag": listed.etag,
                }));
            }
            count += 1;
        }
        let next = match page.last_key {
            // A directory the page ended in is skipped whole, not paged through.
            Some(_) if page.more && directory.is_some() => resume.clone(),
            Some(last) if page.more => last,
            _ => break,
        };
        if !super::usage::advance(&mut after, next) {
            complete = false;
            break;
        }
    }
    if !complete {
        warn!(bucket = %bucket_name, "object listing is incomplete: a node of the pool did not answer");
    }

    let mut result = json!({
        "objects": objects_list,
        "common_prefixes": common_prefixes,
        "is_truncated": is_truncated,
        "complete": complete,
    });
    if is_truncated {
        result["next_continuation_token"] = json!(resume);
    }

    Ok(Json(result))
}

/// Sorts after every key in a directory: a listing resumed after
/// `directory + DIRECTORY_END` starts past the whole directory.
const DIRECTORY_END: char = '\u{10FFFF}';

/// Trigger on-demand volume compaction — requires SuperUser role.
///
/// POST /api/admin/compaction/run
/// Headers: `Authorization: Bearer <token>`
/// Query: `?dry_run=true` (optional, default false)
/// Query: `?threshold=0.1` (optional, fragmentation threshold, default 0.0 for on-demand)
/// Returns: `{ "volumes_scanned": N, "volumes_compacted": N, "bytes_reclaimed": N, "errors": N }`
pub async fn run_compaction(
    State(state): State<AppState>,
    Extension(claims): Extension<JwtClaims>,
    Query(params): Query<CompactionParams>,
) -> Result<Json<serde_json::Value>, AdminError> {
    if !claims.role.can_admin() {
        return Err(AdminError::Forbidden);
    }

    let storage = state.storage.read().await;

    let mut config = s4_compactor::CompactionConfig::on_demand(
        params.threshold.unwrap_or(0.0),
        params.dry_run.unwrap_or(false),
        s4_compactor::CompactionConfig::multipart_ttl_from_env(),
    );

    // Override max volumes if specified (useful for testing partial compaction)
    if let Some(max_vols) = params.max_volumes {
        config.max_volumes_per_run = max_vols;
    }

    // In cluster mode, journal compaction is managed by the replication layer
    // with replica-aware sequence tracking — disable it for on-demand runs too.
    if state.is_cluster_mode() {
        config.compact_journal = false;
    }

    let compactor = s4_compactor::VolumeCompactor::new(
        storage.volumes_dir().to_path_buf(),
        storage.index_db().clone(),
        storage.deduplicator().clone(),
        storage.volume_writer().clone(),
        config,
    );

    let stats = compactor
        .run()
        .await
        .map_err(|e| AdminError::Storage(format!("Compaction failed: {}", e)))?;

    Ok(Json(json!({
        "volumes_scanned": stats.volumes_scanned,
        "volumes_compacted": stats.volumes_compacted,
        "volumes_skipped": stats.volumes_skipped,
        "live_blobs": stats.total_live_blobs,
        "dead_blobs": stats.total_dead_blobs,
        "bytes_reclaimed": stats.total_bytes_reclaimed,
        "errors": stats.errors,
        "dry_run": params.dry_run.unwrap_or(false),
    })))
}

/// Query parameters for the compaction endpoint.
#[derive(Debug, Deserialize)]
pub struct CompactionParams {
    /// If true, analyze without compacting.
    pub dry_run: Option<bool>,
    /// Fragmentation threshold (0.0-1.0). Default 0.0 = compact all fragmented volumes.
    pub threshold: Option<f64>,
    /// Maximum number of volumes to compact per run. Default 0 = unlimited.
    pub max_volumes: Option<usize>,
}

/// Run an on-demand CRC32 integrity scrub of all volumes.
///
/// POST /api/admin/scrubber/run
/// Headers: `Authorization: Bearer <token>`
/// Returns: `{ "blobs_checked": N, "blobs_ok": N, "blobs_corrupted": N, ... }`
pub async fn run_scrubber(
    State(state): State<AppState>,
    Extension(claims): Extension<JwtClaims>,
) -> Result<Json<serde_json::Value>, AdminError> {
    if !claims.role.can_admin() {
        return Err(AdminError::Forbidden);
    }

    let storage = state.storage.read().await;
    let scrubber = s4_compactor::VolumeScrubber::new(storage.volumes_dir());

    let result = scrubber
        .scrub_all()
        .await
        .map_err(|e| AdminError::Storage(format!("Scrubber failed: {e}")))?;

    let errors: Vec<serde_json::Value> = result
        .errors
        .iter()
        .map(|e| {
            json!({
                "volume_id": e.volume_id,
                "offset": e.offset,
                "key": e.key,
                "expected_crc": e.expected_crc,
                "actual_crc": e.actual_crc,
            })
        })
        .collect();

    Ok(Json(json!({
        "blobs_checked": result.blobs_checked,
        "blobs_ok": result.blobs_ok,
        "blobs_corrupted": result.blobs_corrupted,
        "volumes_scanned": result.volumes_scanned,
        "errors": errors,
    })))
}

/// Get the status of the background scrubber.
///
/// GET /api/admin/scrubber/status
/// Headers: `Authorization: Bearer <token>`
///
/// A cluster node answers the counters of its background scrubber, the ones
/// the `s4_scrubber_*` metrics mirror. A single node runs no background
/// scrubber and answers with a static message.
pub async fn scrubber_status(
    State(state): State<AppState>,
    Extension(claims): Extension<JwtClaims>,
) -> Result<Json<serde_json::Value>, AdminError> {
    if !claims.role.can_admin() {
        return Err(AdminError::Forbidden);
    }

    Ok(Json(match state.cluster_admin_service.scrubber_status() {
        Some(status) => json!(status),
        // Use POST /api/admin/scrubber/run for on-demand scrubbing.
        None => json!({
            "mode": "standalone",
            "background_scrubber": false,
            "message": "Use POST /api/admin/scrubber/run for on-demand CRC32 integrity check. Background scrubber is available in cluster mode.",
        }),
    }))
}

/// Admin API error type.
///
/// Every variant carries its own HTTP status and a stable, machine-readable
/// code, so a caller (the web console, a script) can branch on the outcome
/// instead of matching on the human-readable message. Only genuine server
/// faults — `Iam` and `Storage` — answer 5xx; anything the caller can correct
/// itself answers 4xx.
#[derive(Debug)]
pub enum AdminError {
    /// Token is missing, malformed or expired (401).
    Unauthorized,
    /// Login rejected: unknown account or wrong password (401).
    ///
    /// Both cases share one variant deliberately — telling them apart would let
    /// an unauthenticated caller enumerate account names.
    InvalidCredentials,
    /// Caller is authenticated but lacks the required role (403).
    Forbidden,
    /// No user with the requested id or name (404).
    UserNotFound,
    /// A user with this name already exists (409).
    UserAlreadyExists,
    /// Username does not satisfy the format rules (400).
    InvalidUsername(String),
    /// The user has no S3 credentials issued (404).
    NoS3Credentials,
    /// No bucket with the requested name (404).
    BucketNotFound(String),
    /// A bucket with this name already exists (409).
    BucketAlreadyExists(String),
    /// The bucket still holds objects and `force` was not requested (409).
    BucketNotEmpty(String),
    /// Bucket name does not satisfy the S3 naming rules (400).
    InvalidBucketName(String),
    /// Request body or parameters could not be interpreted (400).
    InvalidData,
    /// IAM subsystem fault (500).
    Iam(IamError),
    /// Storage engine fault (500).
    Storage(String),
    /// Too few cluster replicas answered (503); the request may be retried.
    Unavailable(String),
}

impl AdminError {
    /// The admin answer for a bucket the shared bucket code did not create or
    /// delete, the same cases the S3 route answers.
    fn from_bucket_change(bucket: &str, error: super::bucket::BucketChangeError) -> Self {
        use super::bucket::BucketChangeError;
        match error {
            BucketChangeError::AlreadyExists => AdminError::BucketAlreadyExists(bucket.to_string()),
            BucketChangeError::NotFound => AdminError::BucketNotFound(bucket.to_string()),
            BucketChangeError::NotEmpty => AdminError::BucketNotEmpty(bucket.to_string()),
            BucketChangeError::Cluster(e) => admin_cluster_error(e),
            BucketChangeError::Unavailable(message) => AdminError::Unavailable(message),
            BucketChangeError::Storage(message) => AdminError::Storage(message),
        }
    }
}

impl AdminError {
    /// HTTP status and stable error code for this error.
    fn status_and_code(&self) -> (StatusCode, &'static str) {
        match self {
            AdminError::Unauthorized => (StatusCode::UNAUTHORIZED, "Unauthorized"),
            AdminError::InvalidCredentials => (StatusCode::UNAUTHORIZED, "InvalidCredentials"),
            AdminError::Forbidden => (StatusCode::FORBIDDEN, "Forbidden"),
            AdminError::UserNotFound => (StatusCode::NOT_FOUND, "UserNotFound"),
            AdminError::UserAlreadyExists => (StatusCode::CONFLICT, "UserAlreadyExists"),
            AdminError::InvalidUsername(_) => (StatusCode::BAD_REQUEST, "InvalidUsername"),
            AdminError::NoS3Credentials => (StatusCode::NOT_FOUND, "NoS3Credentials"),
            // Same code the S3 path uses for a missing bucket, so a client can
            // share one branch between the two APIs.
            AdminError::BucketNotFound(_) => (StatusCode::NOT_FOUND, "NoSuchBucket"),
            AdminError::BucketAlreadyExists(_) => (StatusCode::CONFLICT, "BucketAlreadyExists"),
            AdminError::BucketNotEmpty(_) => (StatusCode::CONFLICT, "BucketNotEmpty"),
            AdminError::InvalidBucketName(_) => (StatusCode::BAD_REQUEST, "InvalidBucketName"),
            AdminError::InvalidData => (StatusCode::BAD_REQUEST, "InvalidData"),
            AdminError::Iam(_) | AdminError::Storage(_) => {
                (StatusCode::INTERNAL_SERVER_ERROR, "InternalError")
            }
            AdminError::Unavailable(_) => (StatusCode::SERVICE_UNAVAILABLE, "ServiceUnavailable"),
        }
    }

    /// Human-readable message for this error.
    fn message(&self) -> String {
        match self {
            AdminError::Unauthorized => "Unauthorized".to_string(),
            AdminError::InvalidCredentials => "Invalid credentials".to_string(),
            AdminError::Forbidden => "Forbidden".to_string(),
            AdminError::UserNotFound => "User not found".to_string(),
            AdminError::UserAlreadyExists => "User already exists".to_string(),
            AdminError::InvalidUsername(reason) => reason.clone(),
            AdminError::NoS3Credentials => "No S3 credentials configured for this user".to_string(),
            AdminError::BucketNotFound(name) => format!("Bucket '{name}' not found"),
            AdminError::BucketAlreadyExists(name) => format!("Bucket '{name}' already exists"),
            AdminError::BucketNotEmpty(name) => format!(
                "Bucket '{name}' is not empty. Delete its objects first, \
                 or repeat the request with ?force=true"
            ),
            AdminError::InvalidBucketName(reason) => reason.clone(),
            AdminError::InvalidData => "Invalid request data".to_string(),
            AdminError::Iam(err) => err.to_string(),
            AdminError::Storage(err) => err.clone(),
            AdminError::Unavailable(err) => err.clone(),
        }
    }
}

impl From<IamError> for AdminError {
    fn from(err: IamError) -> Self {
        match err {
            IamError::UserNotFound => AdminError::UserNotFound,
            IamError::UserAlreadyExists => AdminError::UserAlreadyExists,
            // IamError's own wording spells out the format rule — pass it on.
            IamError::InvalidUsername => {
                AdminError::InvalidUsername(IamError::InvalidUsername.to_string())
            }
            IamError::InvalidCredentials => AdminError::InvalidCredentials,
            IamError::InsufficientPermissions => AdminError::Forbidden,
            IamError::InvalidToken => AdminError::Unauthorized,
            IamError::NoS3Credentials => AdminError::NoS3Credentials,
            // What is left is a server-side fault: hashing, token generation,
            // storage, or a record we wrote ourselves and can no longer read.
            other => AdminError::Iam(other),
        }
    }
}

impl IntoResponse for AdminError {
    fn into_response(self) -> Response {
        let (status, code) = self.status_and_code();
        let message = self.message();

        if status.is_server_error() {
            error!("Admin API fault ({}): {}", code, message);
        }

        let body = Json(json!({
            "error": message,
            "code": code,
        }));

        (status, body).into_response()
    }
}
