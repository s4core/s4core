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

//! Axum HTTP server setup and routing.
//!
//! This module provides the HTTP server infrastructure for S4,
//! including routing for S3-compatible API endpoints.

use axum::{
    body::Bytes,
    extract::{DefaultBodyLimit, Extension, Path, Query, State},
    http::{header::HeaderName, HeaderMap, Method, StatusCode},
    middleware,
    response::{IntoResponse, Response},
    routing::{delete, get, head, post, put},
    Router,
};
use s4_core::storage::{BitcaskStorageEngine, StorageEngine};
use s4_features::iam::{models::User, AuthService, IamStorage, JwtManager};

use crate::s3::errors::S3Error;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tower_http::{
    cors::{Any, CorsLayer},
    trace::TraceLayer,
};

use crate::handlers;
use crate::middleware::{admin_auth_middleware, iam_auth_middleware, metrics_middleware};

/// Default maximum upload size (5GB).
pub const DEFAULT_MAX_UPLOAD_SIZE: usize = 5 * 1024 * 1024 * 1024;

/// In-memory store for multipart upload parts, keyed by part identifier.
pub type PartStore = Arc<RwLock<HashMap<String, Vec<u8>>>>;

/// Shared application state for all handlers.
#[derive(Clone)]
pub struct AppState {
    /// Storage engine instance.
    pub storage: Arc<RwLock<BitcaskStorageEngine>>,
    /// IAM storage for user management.
    pub iam_storage: Arc<IamStorage>,
    /// Authentication service (JWT + credential verification).
    pub auth_service: Arc<AuthService>,
    /// Access key ID for authentication (DEPRECATED - will be removed in Week 3).
    pub access_key_id: String,
    /// Secret access key for authentication (DEPRECATED - will be removed in Week 3).
    pub secret_access_key: String,
    /// Maximum upload size in bytes.
    pub max_upload_size: usize,
    /// Prometheus metrics handle for rendering `/metrics` endpoint.
    pub prometheus_handle: Option<metrics_exporter_prometheus::PrometheusHandle>,
    /// Server start time for uptime calculation.
    pub start_time: std::time::Instant,
    /// In-memory store for multipart upload parts (avoids writing temp data to volumes).
    ///
    /// **Note**: Parts are held in RAM until `CompleteMultipartUpload` or `AbortMultipartUpload`.
    /// Data is lost on server crash — incomplete multipart uploads are not recoverable.
    pub part_store: PartStore,
}

impl AppState {
    /// Creates a new application state.
    pub fn new(
        storage: BitcaskStorageEngine,
        access_key_id: String,
        secret_access_key: String,
    ) -> Self {
        Self::with_max_upload_size(
            storage,
            access_key_id,
            secret_access_key,
            DEFAULT_MAX_UPLOAD_SIZE,
        )
    }

    /// Creates a new application state with custom max upload size.
    pub fn with_max_upload_size(
        storage: BitcaskStorageEngine,
        access_key_id: String,
        secret_access_key: String,
        max_upload_size: usize,
    ) -> Self {
        let storage_arc = Arc::new(RwLock::new(storage));

        // Initialize IAM components
        // Use storage as Arc<dyn StorageEngine> for IAM
        let iam_storage_engine: Arc<dyn StorageEngine> = {
            // Clone the Arc and convert to trait object
            let storage_clone = storage_arc.clone();
            // Create a new Arc that implements StorageEngine trait
            Arc::new(IamStorageAdapter(storage_clone))
        };
        let iam_storage = Arc::new(IamStorage::new(iam_storage_engine));

        // Get JWT secret from environment or use default for development
        let jwt_secret = std::env::var("S4_JWT_SECRET").unwrap_or_else(|_| {
            "default-jwt-secret-change-me-in-production-minimum-32-bytes".to_string()
        });
        let token_lifetime_hours = std::env::var("S4_JWT_LIFETIME_HOURS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(24);

        // Create JWT manager
        let jwt_manager = JwtManager::new(&jwt_secret, token_lifetime_hours);

        // Create auth service (needs a clone of iam_storage since AuthService takes ownership)
        let auth_service = Arc::new(AuthService::new((*iam_storage).clone(), jwt_manager));

        Self {
            storage: storage_arc,
            iam_storage,
            auth_service,
            access_key_id,
            secret_access_key,
            max_upload_size,
            prometheus_handle: None,
            start_time: std::time::Instant::now(),
            part_store: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Sets the Prometheus handle for rendering metrics.
    pub fn with_prometheus_handle(
        mut self,
        handle: metrics_exporter_prometheus::PrometheusHandle,
    ) -> Self {
        self.prometheus_handle = Some(handle);
        self
    }
}

/// Adapter to make BitcaskStorageEngine work with StorageEngine trait for IAM.
///
/// This adapter wraps Arc<RwLock<BitcaskStorageEngine>> and implements
/// the async StorageEngine trait required by IamStorage.
struct IamStorageAdapter(Arc<RwLock<BitcaskStorageEngine>>);

#[async_trait::async_trait]
impl StorageEngine for IamStorageAdapter {
    async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        data: &[u8],
        content_type: &str,
        metadata: &std::collections::HashMap<String, String>,
    ) -> Result<String, s4_core::error::StorageError> {
        let storage = self.0.write().await;
        storage.put_object(bucket, key, data, content_type, metadata).await
    }

    async fn get_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<(Vec<u8>, s4_core::types::IndexRecord), s4_core::error::StorageError> {
        let storage = self.0.read().await;
        storage.get_object(bucket, key).await
    }

    async fn delete_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<(), s4_core::error::StorageError> {
        let storage = self.0.write().await;
        storage.delete_object(bucket, key).await
    }

    async fn head_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<s4_core::types::IndexRecord, s4_core::error::StorageError> {
        let storage = self.0.read().await;
        storage.head_object(bucket, key).await
    }

    async fn list_objects(
        &self,
        bucket: &str,
        prefix: &str,
        max_keys: usize,
    ) -> Result<Vec<(String, s4_core::types::IndexRecord)>, s4_core::error::StorageError> {
        let storage = self.0.read().await;
        storage.list_objects(bucket, prefix, max_keys).await
    }

    // Versioning methods - not needed for IAM but required by trait
    async fn put_object_versioned(
        &self,
        _bucket: &str,
        _key: &str,
        _data: &[u8],
        _content_type: &str,
        _metadata: &std::collections::HashMap<String, String>,
        _versioning_status: s4_core::types::VersioningStatus,
    ) -> Result<(String, Option<String>), s4_core::error::StorageError> {
        Err(s4_core::error::StorageError::InvalidOperation {
            operation: "versioning".to_string(),
            reason: "Not needed for IAM storage".to_string(),
        })
    }

    async fn get_object_version(
        &self,
        _bucket: &str,
        _key: &str,
        _version_id: &str,
    ) -> Result<(Vec<u8>, s4_core::types::IndexRecord), s4_core::error::StorageError> {
        Err(s4_core::error::StorageError::InvalidOperation {
            operation: "versioning".to_string(),
            reason: "Not needed for IAM storage".to_string(),
        })
    }

    async fn delete_object_versioned(
        &self,
        _bucket: &str,
        _key: &str,
        _version_id: Option<&str>,
        _versioning_status: s4_core::types::VersioningStatus,
    ) -> Result<s4_core::storage::DeleteResult, s4_core::error::StorageError> {
        Err(s4_core::error::StorageError::InvalidOperation {
            operation: "versioning".to_string(),
            reason: "Not needed for IAM storage".to_string(),
        })
    }

    async fn head_object_version(
        &self,
        _bucket: &str,
        _key: &str,
        _version_id: &str,
    ) -> Result<s4_core::types::IndexRecord, s4_core::error::StorageError> {
        Err(s4_core::error::StorageError::InvalidOperation {
            operation: "versioning".to_string(),
            reason: "Not needed for IAM storage".to_string(),
        })
    }

    async fn list_object_versions(
        &self,
        _bucket: &str,
        _prefix: &str,
        _key_marker: Option<&str>,
        _version_id_marker: Option<&str>,
        _max_keys: usize,
    ) -> Result<s4_core::storage::ListVersionsResult, s4_core::error::StorageError> {
        Err(s4_core::error::StorageError::InvalidOperation {
            operation: "versioning".to_string(),
            reason: "Not needed for IAM storage".to_string(),
        })
    }

    async fn put_object_with_retention(
        &self,
        _bucket: &str,
        _key: &str,
        _data: &[u8],
        _content_type: &str,
        _metadata: &std::collections::HashMap<String, String>,
        _versioning_status: s4_core::types::VersioningStatus,
        _default_retention: Option<s4_core::types::DefaultRetention>,
    ) -> Result<(String, Option<String>), s4_core::error::StorageError> {
        Err(s4_core::error::StorageError::InvalidOperation {
            operation: "object_lock".to_string(),
            reason: "Not needed for IAM storage".to_string(),
        })
    }

    async fn update_object_metadata(
        &self,
        _bucket: &str,
        _key: &str,
        _version_id: &str,
        _updated_record: s4_core::types::IndexRecord,
    ) -> Result<(), s4_core::error::StorageError> {
        Err(s4_core::error::StorageError::InvalidOperation {
            operation: "metadata_update".to_string(),
            reason: "Not needed for IAM storage".to_string(),
        })
    }
}

// ============================================================================
// Permission Check Helpers
// ============================================================================

/// Checks if the user has read permission.
///
/// Returns `Ok(())` if the user can read, or an error response if not.
/// When auth is disabled (S4_DISABLE_AUTH=1), the user will be None and
/// all operations are allowed.
#[inline]
#[allow(clippy::result_large_err)] // Response is the standard axum error type
fn check_read_permission(user: Option<&User>) -> Result<(), Response> {
    if let Some(user) = user {
        if !user.role.can_read() {
            return Err(S3Error::AccessDenied.into_response());
        }
    }
    // If user is None, auth is disabled - allow all
    Ok(())
}

/// Checks if the user has write permission.
///
/// Returns `Ok(())` if the user can write, or an error response if not.
/// When auth is disabled (S4_DISABLE_AUTH=1), the user will be None and
/// all operations are allowed.
#[inline]
#[allow(clippy::result_large_err)] // Response is the standard axum error type
fn check_write_permission(user: Option<&User>) -> Result<(), Response> {
    if let Some(user) = user {
        if !user.role.can_write() {
            return Err(S3Error::AccessDenied.into_response());
        }
    }
    // If user is None, auth is disabled - allow all
    Ok(())
}

/// Query parameters for routing decisions.
#[derive(Debug, Deserialize, Default)]
struct ObjectQueryParams {
    /// Upload ID for multipart operations.
    #[serde(rename = "uploadId")]
    upload_id: Option<String>,
    /// Part number for UploadPart.
    #[serde(rename = "partNumber")]
    part_number: Option<u32>,
    /// Marker for CreateMultipartUpload (presence indicates initiation).
    uploads: Option<String>,
    /// Version ID for version-specific operations.
    #[serde(rename = "versionId")]
    version_id: Option<String>,
    /// Retention configuration marker for Object Lock.
    retention: Option<String>,
    /// Legal hold marker for Object Lock.
    #[serde(rename = "legal-hold")]
    legal_hold: Option<String>,
    /// Tagging configuration marker for Object tagging.
    tagging: Option<String>,
}

/// Query parameters for bucket configuration operations.
#[derive(Debug, Deserialize, Default)]
struct BucketConfigQueryParams {
    /// CORS configuration marker.
    cors: Option<String>,
    /// Versioning configuration marker.
    versioning: Option<String>,
    /// Lifecycle configuration marker.
    lifecycle: Option<String>,
    /// Object Lock configuration marker.
    #[serde(rename = "object-lock")]
    object_lock: Option<String>,
    /// Versions listing marker (GET /{bucket}?versions).
    versions: Option<String>,
    /// DeleteObjects marker (POST /{bucket}?delete).
    delete: Option<String>,
    /// Location marker (GET /{bucket}?location).
    location: Option<String>,
}

/// Routes GET requests for bucket - either ListObjects, ListObjectVersions, or bucket configuration.
async fn get_bucket_router(
    state: State<AppState>,
    path: Path<String>,
    user: Option<Extension<User>>,
    Query(config_params): Query<BucketConfigQueryParams>,
    Query(list_params): Query<handlers::bucket::ListObjectsQuery>,
    Query(versions_params): Query<handlers::ListObjectVersionsQuery>,
) -> axum::response::Response {
    // Check read permission (all GET bucket operations require read)
    if let Err(response) = check_read_permission(user.as_ref().map(|e| &e.0)) {
        return response;
    }

    // Check for bucket configuration queries
    if config_params.location.is_some() {
        handlers::get_bucket_location(state, path).await.into_response()
    } else if config_params.cors.is_some() {
        handlers::get_bucket_cors(state, path).await.into_response()
    } else if config_params.versioning.is_some() {
        handlers::get_bucket_versioning(state, path).await.into_response()
    } else if config_params.lifecycle.is_some() {
        handlers::get_bucket_lifecycle(state, path).await.into_response()
    } else if config_params.object_lock.is_some() {
        handlers::get_bucket_object_lock_configuration(state, path)
            .await
            .into_response()
    } else if config_params.versions.is_some() {
        // ListObjectVersions
        handlers::list_object_versions(state, path, Query(versions_params))
            .await
            .into_response()
    } else {
        // Default: ListObjects
        handlers::list_objects(state, path, Query(list_params)).await.into_response()
    }
}

/// Routes PUT requests for bucket - either CreateBucket or bucket configuration.
async fn put_bucket_router(
    state: State<AppState>,
    path: Path<String>,
    user: Option<Extension<User>>,
    Query(params): Query<BucketConfigQueryParams>,
    body: Bytes,
) -> axum::response::Response {
    // Check write permission (all PUT bucket operations require write)
    if let Err(response) = check_write_permission(user.as_ref().map(|e| &e.0)) {
        return response;
    }

    if params.cors.is_some() {
        handlers::put_bucket_cors(state, path, body).await.into_response()
    } else if params.versioning.is_some() {
        handlers::put_bucket_versioning(state, path, body).await.into_response()
    } else if params.lifecycle.is_some() {
        handlers::put_bucket_lifecycle(state, path, body).await.into_response()
    } else if params.object_lock.is_some() {
        handlers::put_bucket_object_lock_configuration(state, path, body)
            .await
            .into_response()
    } else {
        // Default: CreateBucket
        handlers::create_bucket(state, path).await.into_response()
    }
}

/// Routes DELETE requests for bucket - either DeleteBucket or bucket configuration.
async fn delete_bucket_router(
    state: State<AppState>,
    path: Path<String>,
    user: Option<Extension<User>>,
    Query(params): Query<BucketConfigQueryParams>,
) -> axum::response::Response {
    // Check write permission (all DELETE bucket operations require write)
    if let Err(response) = check_write_permission(user.as_ref().map(|e| &e.0)) {
        return response;
    }

    if params.cors.is_some() {
        handlers::delete_bucket_cors(state, path).await.into_response()
    } else if params.lifecycle.is_some() {
        handlers::delete_bucket_lifecycle(state, path).await.into_response()
    } else {
        // Default: DeleteBucket
        handlers::delete_bucket(state, path).await.into_response()
    }
}

/// Routes POST requests for bucket operations.
async fn post_bucket_router(
    state: State<AppState>,
    path: Path<String>,
    user: Option<Extension<User>>,
    Query(params): Query<BucketConfigQueryParams>,
    headers: HeaderMap,
    body: Bytes,
) -> axum::response::Response {
    // Check write permission (DeleteObjects requires write)
    if let Err(response) = check_write_permission(user.as_ref().map(|e| &e.0)) {
        return response;
    }

    if params.delete.is_some() {
        handlers::delete_objects(state, path, headers, body).await.into_response()
    } else {
        (StatusCode::BAD_REQUEST, "Invalid POST request").into_response()
    }
}

/// Routes PUT requests - either PutObject, UploadPart, PutObjectRetention, or PutObjectLegalHold.
async fn put_object_router(
    state: State<AppState>,
    path: Path<(String, String)>,
    user: Option<Extension<User>>,
    Query(params): Query<ObjectQueryParams>,
    headers: HeaderMap,
    body: Bytes,
) -> axum::response::Response {
    // Check write permission (all PUT object operations require write)
    if let Err(response) = check_write_permission(user.as_ref().map(|e| &e.0)) {
        return response;
    }

    // Check for Object Lock operations first
    if params.retention.is_some() {
        let version_query = handlers::object_lock::ObjectVersionQuery {
            version_id: params.version_id,
        };
        handlers::put_object_retention(state, path, Query(version_query), body)
            .await
            .into_response()
    } else if params.legal_hold.is_some() {
        let version_query = handlers::object_lock::ObjectVersionQuery {
            version_id: params.version_id,
        };
        handlers::put_object_legal_hold(state, path, Query(version_query), body)
            .await
            .into_response()
    } else if params.part_number.is_some() && params.upload_id.is_some() {
        // UploadPart
        let query = handlers::multipart::MultipartQuery {
            upload_id: params.upload_id,
            part_number: params.part_number,
            uploads: None,
        };
        handlers::upload_part(state, path, Query(query), headers, body)
            .await
            .into_response()
    } else {
        // Default: PutObject
        handlers::put_object(state, path, headers, body).await.into_response()
    }
}

/// Routes DELETE requests - either DeleteObject or AbortMultipartUpload.
async fn delete_object_router(
    state: State<AppState>,
    path: Path<(String, String)>,
    user: Option<Extension<User>>,
    Query(params): Query<ObjectQueryParams>,
) -> axum::response::Response {
    // Check write permission (all DELETE object operations require write)
    if let Err(response) = check_write_permission(user.as_ref().map(|e| &e.0)) {
        return response;
    }

    // If uploadId is present, this is AbortMultipartUpload
    if params.upload_id.is_some() {
        let query = handlers::multipart::MultipartQuery {
            upload_id: params.upload_id,
            part_number: None,
            uploads: None,
        };
        handlers::abort_multipart_upload(state, path, Query(query))
            .await
            .into_response()
    } else {
        // Pass version_id to delete_object for versioning support
        let version_query = handlers::ObjectVersionQuery {
            version_id: params.version_id,
        };
        handlers::delete_object(state, path, Query(version_query)).await.into_response()
    }
}

/// Routes POST requests - either CreateMultipartUpload or CompleteMultipartUpload.
async fn post_object_router(
    state: State<AppState>,
    path: Path<(String, String)>,
    user: Option<Extension<User>>,
    Query(params): Query<ObjectQueryParams>,
    headers: HeaderMap,
    body: Bytes,
) -> axum::response::Response {
    // Check write permission (all POST object operations require write)
    if let Err(response) = check_write_permission(user.as_ref().map(|e| &e.0)) {
        return response;
    }

    // If "uploads" query param is present, this is CreateMultipartUpload
    if params.uploads.is_some() {
        handlers::create_multipart_upload(state, path, headers).await.into_response()
    } else if params.upload_id.is_some() {
        // If uploadId is present, this is CompleteMultipartUpload
        let query = handlers::multipart::MultipartQuery {
            upload_id: params.upload_id,
            part_number: None,
            uploads: None,
        };
        handlers::complete_multipart_upload(state, path, Query(query), headers, body)
            .await
            .into_response()
    } else {
        // Unknown POST operation
        (axum::http::StatusCode::BAD_REQUEST, "Invalid POST request").into_response()
    }
}

/// Routes GET requests for objects - either GetObject, GetObjectRetention, or GetObjectLegalHold.
async fn get_object_router(
    state: State<AppState>,
    path: Path<(String, String)>,
    user: Option<Extension<User>>,
    headers: HeaderMap,
    Query(params): Query<ObjectQueryParams>,
) -> axum::response::Response {
    // Check read permission (all GET object operations require read)
    if let Err(response) = check_read_permission(user.as_ref().map(|e| &e.0)) {
        return response;
    }

    // Check for Object Lock query parameters
    if params.retention.is_some() {
        let version_query = handlers::object_lock::ObjectVersionQuery {
            version_id: params.version_id,
        };
        handlers::get_object_retention(state, path, Query(version_query))
            .await
            .into_response()
    } else if params.legal_hold.is_some() {
        let version_query = handlers::object_lock::ObjectVersionQuery {
            version_id: params.version_id,
        };
        handlers::get_object_legal_hold(state, path, Query(version_query))
            .await
            .into_response()
    } else if params.tagging.is_some() {
        let version_query = handlers::ObjectVersionQuery {
            version_id: params.version_id,
        };
        handlers::get_object_tagging(state, path, Query(version_query))
            .await
            .into_response()
    } else {
        // Default: GetObject
        let version_query = handlers::ObjectVersionQuery {
            version_id: params.version_id,
        };
        handlers::get_object(state, path, Query(version_query), headers)
            .await
            .into_response()
    }
}

/// Routes GET / request - ListBuckets.
async fn list_buckets_router(
    state: State<AppState>,
    user: Option<Extension<User>>,
) -> axum::response::Response {
    // Check read permission (ListBuckets requires read)
    if let Err(response) = check_read_permission(user.as_ref().map(|e| &e.0)) {
        return response;
    }

    handlers::list_buckets(state).await.into_response()
}

/// Routes HEAD /{bucket} request - HeadBucket.
async fn head_bucket_router(
    state: State<AppState>,
    path: Path<String>,
    user: Option<Extension<User>>,
) -> axum::response::Response {
    // Check read permission (HeadBucket requires read)
    if let Err(response) = check_read_permission(user.as_ref().map(|e| &e.0)) {
        return response;
    }

    handlers::head_bucket(state, path).await.into_response()
}

/// Routes HEAD /{bucket}/{key} request - HeadObject.
async fn head_object_router(
    state: State<AppState>,
    path: Path<(String, String)>,
    user: Option<Extension<User>>,
    Query(params): Query<ObjectQueryParams>,
) -> axum::response::Response {
    // Check read permission (HeadObject requires read)
    if let Err(response) = check_read_permission(user.as_ref().map(|e| &e.0)) {
        return response;
    }

    let version_query = handlers::ObjectVersionQuery {
        version_id: params.version_id,
    };
    handlers::head_object(state, path, Query(version_query)).await.into_response()
}

/// Creates the main router with all S3-compatible and Admin API endpoints.
///
/// # S3 API Routing
///
/// The S3 API uses a combination of path and query parameters:
/// - `GET /` - ListBuckets
/// - `PUT /{bucket}` - CreateBucket
/// - `PUT /{bucket}?cors` - PutBucketCors
/// - `PUT /{bucket}?versioning` - PutBucketVersioning
/// - `PUT /{bucket}?lifecycle` - PutBucketLifecycle
/// - `PUT /{bucket}?object-lock` - PutBucketObjectLockConfiguration
/// - `DELETE /{bucket}` - DeleteBucket
/// - `DELETE /{bucket}?cors` - DeleteBucketCors
/// - `DELETE /{bucket}?lifecycle` - DeleteBucketLifecycle
/// - `GET /{bucket}` - ListObjects (with query params)
/// - `GET /{bucket}?cors` - GetBucketCors
/// - `GET /{bucket}?versioning` - GetBucketVersioning
/// - `GET /{bucket}?lifecycle` - GetBucketLifecycle
/// - `GET /{bucket}?object-lock` - GetBucketObjectLockConfiguration
/// - `PUT /{bucket}/{key}` - PutObject
/// - `PUT /{bucket}/{key}?partNumber=N&uploadId=X` - UploadPart
/// - `PUT /{bucket}/{key}?retention&versionId=X` - PutObjectRetention
/// - `PUT /{bucket}/{key}?legal-hold&versionId=X` - PutObjectLegalHold
/// - `GET /{bucket}/{key}` - GetObject
/// - `GET /{bucket}/{key}?retention&versionId=X` - GetObjectRetention
/// - `GET /{bucket}/{key}?legal-hold&versionId=X` - GetObjectLegalHold
/// - `DELETE /{bucket}/{key}` - DeleteObject
/// - `DELETE /{bucket}/{key}?uploadId=X` - AbortMultipartUpload
/// - `HEAD /{bucket}/{key}` - HeadObject
/// - `POST /{bucket}/{key}?uploads` - CreateMultipartUpload
/// - `POST /{bucket}/{key}?uploadId=X` - CompleteMultipartUpload
///
/// # Admin API Routing (JWT-based authentication)
///
/// - `POST /api/admin/login` - Login (no auth required)
/// - `GET /api/admin/users` - List users (SuperUser only)
/// - `POST /api/admin/users` - Create user (SuperUser only)
/// - `GET /api/admin/users/:id` - Get user (SuperUser only)
/// - `PUT /api/admin/users/:id` - Update user (SuperUser only)
/// - `DELETE /api/admin/users/:id` - Delete user (SuperUser only)
/// - `POST /api/admin/users/:id/credentials` - Generate S3 credentials (SuperUser only)
/// - `DELETE /api/admin/users/:id/credentials` - Delete S3 credentials (SuperUser only)
pub fn create_router(state: AppState) -> Router {
    // Create a permissive CORS layer for browser-based clients
    // Individual bucket CORS policies are handled at the handler level
    let cors = CorsLayer::new()
        .allow_methods([
            Method::GET,
            Method::PUT,
            Method::POST,
            Method::DELETE,
            Method::HEAD,
            Method::OPTIONS,
        ])
        .allow_headers(Any)
        .allow_origin(Any)
        .expose_headers([
            HeaderName::from_static("etag"),
            HeaderName::from_static("x-amz-request-id"),
            HeaderName::from_static("x-amz-version-id"),
            HeaderName::from_static("x-amz-delete-marker"),
        ]);

    // Create S3 API router with AWS Signature V4 authentication
    let s3_router = Router::new()
        // Service endpoints
        .route("/", get(list_buckets_router))
        // Bucket operations - use routers for query-based dispatch
        .route("/:bucket", put(put_bucket_router))
        .route("/:bucket", delete(delete_bucket_router))
        .route("/:bucket", get(get_bucket_router))
        .route("/:bucket", head(head_bucket_router))
        .route("/:bucket", post(post_bucket_router))
        // Object operations (note: key can contain slashes, so we use catch-all)
        // PUT routes to either PutObject, UploadPart, or Object Lock operations
        .route("/:bucket/*key", put(put_object_router))
        // GET routes to either GetObject or Object Lock operations
        .route("/:bucket/*key", get(get_object_router))
        // DELETE routes to either DeleteObject or AbortMultipartUpload based on query params
        .route("/:bucket/*key", delete(delete_object_router))
        .route("/:bucket/*key", head(head_object_router))
        // POST routes to either CreateMultipartUpload or CompleteMultipartUpload
        .route("/:bucket/*key", post(post_object_router))
        // Add CORS middleware (before auth for preflight requests)
        .layer(cors)
        // Add IAM authentication middleware (verifies AWS Signature V4 and stores User)
        .layer(middleware::from_fn_with_state(
            state.clone(),
            iam_auth_middleware,
        ));

    // Create Admin API router with JWT authentication
    let admin_protected_router = Router::new()
        .route("/users", get(handlers::admin::list_users))
        .route("/users", post(handlers::admin::create_user))
        .route("/users/:id", get(handlers::admin::get_user))
        .route("/users/:id", put(handlers::admin::update_user))
        .route("/users/:id", delete(handlers::admin::delete_user))
        .route(
            "/users/:id/credentials",
            post(handlers::admin::generate_credentials),
        )
        .route(
            "/users/:id/credentials",
            delete(handlers::admin::delete_credentials),
        )
        .route("/bucket-stats", get(handlers::admin::bucket_stats))
        .route(
            "/buckets/:name",
            put(handlers::admin::create_bucket).delete(handlers::admin::delete_bucket),
        )
        .route(
            "/buckets/:name/objects",
            get(handlers::admin::list_bucket_objects),
        )
        // Add JWT authentication middleware
        .layer(middleware::from_fn_with_state(
            state.clone(),
            admin_auth_middleware,
        ));

    // Public admin routes (no auth required)
    let admin_public_router = Router::new().route("/login", post(handlers::admin::login));

    // Combine admin routes
    let admin_router = Router::new().merge(admin_public_router).merge(admin_protected_router);

    // Combine all routers
    Router::new()
        .nest("/api/admin", admin_router)
        // Observability endpoints (no auth required)
        .route("/metrics", get(handlers::stats::prometheus_metrics))
        .route("/api/stats", get(handlers::stats::api_stats))
        .merge(s3_router)
        // Add tracing layer for request logging
        .layer(TraceLayer::new_for_http())
        // Add metrics middleware to record request count and latency
        .layer(middleware::from_fn(metrics_middleware))
        // Increase body size limit for large uploads
        .layer(DefaultBodyLimit::max(state.max_upload_size))
        // Attach shared state
        .with_state(state)
}

/// Server configuration for binding.
pub struct ServerConfig {
    /// Bind address (e.g., "0.0.0.0:9000").
    pub bind_address: String,
}

impl ServerConfig {
    /// Creates a new server configuration.
    pub fn new(bind_address: impl Into<String>) -> Self {
        Self {
            bind_address: bind_address.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_server_config_creation() {
        let config = ServerConfig::new("127.0.0.1:9000");
        assert_eq!(config.bind_address, "127.0.0.1:9000");
    }
}
