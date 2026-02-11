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
    let response = state.auth_service.login(request).await?;

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

    let user: s4_features::iam::User =
        serde_json::from_slice(&user_bytes).map_err(|_| AdminError::InvalidData)?;

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
/// Returns: { "buckets": [{ "name": "...", "objects_count": N, "storage_used_bytes": N }], "total_storage_bytes": N }
pub async fn bucket_stats(
    State(state): State<AppState>,
    Extension(claims): Extension<JwtClaims>,
) -> Result<Json<serde_json::Value>, AdminError> {
    if !claims.role.can_admin() {
        return Err(AdminError::Forbidden);
    }

    let storage = state.storage.read().await;

    // List all bucket markers to get bucket names
    let markers = storage
        .list_objects("__system__", "__s4_bucket_marker_", 10000)
        .await
        .map_err(|e| AdminError::Storage(e.to_string()))?;

    let bucket_entries: Vec<(String, u64)> = markers
        .into_iter()
        .filter(|(_, r)| !r.is_delete_marker)
        .filter_map(|(key, record)| {
            key.strip_prefix("__s4_bucket_marker_")
                .map(|s| (s.to_string(), record.created_at))
        })
        .collect();

    let mut buckets = Vec::new();
    let mut total_storage_bytes: u64 = 0;

    for (name, created_at) in &bucket_entries {
        let objects = storage.list_objects(name, "", usize::MAX).await.unwrap_or_default();

        let mut objects_count: u64 = 0;
        let mut storage_used: u64 = 0;

        for (key, record) in &objects {
            if key.starts_with("__s4_") || record.is_delete_marker || key.contains('#') {
                continue;
            }
            objects_count += 1;
            storage_used += record.size;
        }

        total_storage_bytes += storage_used;
        buckets.push(json!({
            "name": name,
            "objects_count": objects_count,
            "storage_used_bytes": storage_used,
            "created_at": created_at,
        }));
    }

    Ok(Json(json!({
        "buckets": buckets,
        "total_storage_bytes": total_storage_bytes,
    })))
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

    let storage = state.storage.read().await;
    let marker_key = format!("__s4_bucket_marker_{}", bucket_name);

    // Check if already exists
    if storage.head_object("__system__", &marker_key).await.is_ok() {
        return Err(AdminError::InvalidData);
    }

    storage
        .put_object(
            "__system__",
            &marker_key,
            b"1",
            "application/octet-stream",
            &std::collections::HashMap::new(),
        )
        .await
        .map_err(|e| AdminError::Storage(e.to_string()))?;

    Ok(Json(json!({ "name": bucket_name })))
}

/// Delete a bucket via admin API.
///
/// DELETE /api/admin/buckets/:name
/// Headers: `Authorization: Bearer <token>`
/// Returns: 204 No Content
pub async fn delete_bucket(
    State(state): State<AppState>,
    Extension(claims): Extension<JwtClaims>,
    Path(bucket_name): Path<String>,
) -> Result<StatusCode, AdminError> {
    if !claims.role.can_admin() {
        return Err(AdminError::Forbidden);
    }

    let storage = state.storage.read().await;
    let marker_key = format!("__s4_bucket_marker_{}", bucket_name);

    // Check exists
    if storage.head_object("__system__", &marker_key).await.is_err() {
        return Err(AdminError::UserNotFound); // reusing as "not found"
    }

    // Check empty
    let objects = storage.list_objects(&bucket_name, "", 1).await.unwrap_or_default();
    if !objects.is_empty() {
        return Err(AdminError::InvalidData);
    }

    storage
        .delete_object("__system__", &marker_key)
        .await
        .map_err(|e| AdminError::Storage(e.to_string()))?;

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
/// Returns: { "objects": [...], "is_truncated": bool, "next_continuation_token": "..." }
pub async fn list_bucket_objects(
    State(state): State<AppState>,
    Extension(claims): Extension<JwtClaims>,
    Path(bucket_name): Path<String>,
    Query(params): Query<ListObjectsQuery>,
) -> Result<Json<serde_json::Value>, AdminError> {
    if !claims.role.can_admin() {
        return Err(AdminError::Forbidden);
    }

    let storage = state.storage.read().await;
    let prefix = params.prefix.unwrap_or_default();
    let max_keys = params.max_keys.unwrap_or(50);

    // Fetch one extra to detect truncation
    let all_objects = storage
        .list_objects(&bucket_name, &prefix, usize::MAX)
        .await
        .map_err(|e| AdminError::Storage(e.to_string()))?;

    // Filter out internal keys, delete markers, version keys
    let filtered: Vec<_> = all_objects
        .into_iter()
        .filter(|(key, record)| {
            !key.starts_with("__s4_") && !record.is_delete_marker && !key.contains('#')
        })
        .collect();

    // Handle continuation token (it's the last key from previous page)
    let start_index = if let Some(ref token) = params.continuation_token {
        filtered
            .iter()
            .position(|(key, _)| key.as_str() > token.as_str())
            .unwrap_or(filtered.len())
    } else {
        0
    };

    // Identify common prefixes (directories) vs objects
    let mut common_prefixes: Vec<String> = Vec::new();
    let mut objects_list: Vec<serde_json::Value> = Vec::new();
    let mut seen_prefixes = std::collections::HashSet::new();
    let mut count = 0;
    let mut last_key = String::new();
    let mut is_truncated = false;

    for (key, record) in filtered.iter().skip(start_index) {
        if count >= max_keys {
            is_truncated = true;
            break;
        }

        let relative_key = if prefix.is_empty() {
            key.as_str()
        } else {
            key.strip_prefix(prefix.as_str()).unwrap_or(key.as_str())
        };

        // Check if this is a "directory" (has a / after prefix)
        if let Some(slash_pos) = relative_key.find('/') {
            let dir_prefix = format!("{}{}/", prefix, &relative_key[..slash_pos]);
            if seen_prefixes.insert(dir_prefix.clone()) {
                common_prefixes.push(dir_prefix);
                count += 1;
            }
        } else {
            objects_list.push(json!({
                "key": key,
                "size": record.size,
                "content_type": record.content_type,
                "last_modified": record.modified_at,
                "etag": record.etag,
            }));
            count += 1;
        }

        last_key = key.clone();
    }

    let mut result = json!({
        "objects": objects_list,
        "common_prefixes": common_prefixes,
        "is_truncated": is_truncated,
    });

    if is_truncated {
        result["next_continuation_token"] = json!(last_key);
    }

    Ok(Json(result))
}

/// Admin API error type.
#[derive(Debug)]
pub enum AdminError {
    /// User is not authorized (invalid token)
    Unauthorized,
    /// User lacks required permissions
    Forbidden,
    /// User not found
    UserNotFound,
    /// Invalid request data
    InvalidData,
    /// IAM error
    Iam(IamError),
    /// Storage error
    Storage(String),
}

impl From<IamError> for AdminError {
    fn from(err: IamError) -> Self {
        match err {
            IamError::UserNotFound => AdminError::UserNotFound,
            IamError::InsufficientPermissions => AdminError::Forbidden,
            IamError::InvalidToken => AdminError::Unauthorized,
            _ => AdminError::Iam(err),
        }
    }
}

impl IntoResponse for AdminError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            AdminError::Unauthorized => (StatusCode::UNAUTHORIZED, "Unauthorized".to_string()),
            AdminError::Forbidden => (StatusCode::FORBIDDEN, "Forbidden".to_string()),
            AdminError::UserNotFound => (StatusCode::NOT_FOUND, "User not found".to_string()),
            AdminError::InvalidData => (StatusCode::BAD_REQUEST, "Invalid data".to_string()),
            AdminError::Iam(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
            AdminError::Storage(err) => (StatusCode::INTERNAL_SERVER_ERROR, err),
        };

        let body = Json(json!({
            "error": message,
        }));

        (status, body).into_response()
    }
}
