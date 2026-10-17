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
    body::{Body, Bytes},
    extract::{DefaultBodyLimit, Extension, Path, Query, State},
    http::{header, header::HeaderName, HeaderMap, HeaderValue, Method, StatusCode},
    middleware,
    response::{IntoResponse, Response},
    routing::{delete, get, head, post, put},
    Router,
};
use s4_cluster::{
    ClusterAdminService, ConfigReadGate, DistributedListCoordinator, EcAdminService,
    EcObjectRuntime, NoopClusterAdminService, NoopEcAdminService, NoopEcObjectRuntime,
    QuorumReadCoordinator, QuorumWriteCoordinator,
};
use s4_core::storage::{BitcaskStorageEngine, StorageEngine};
use s4_core::types::StorageClass;
use s4_features::iam::{
    jwt::generate_secret as generate_jwt_secret, models::User, AuthService, IamStorage, JwtManager,
};

use crate::s3::errors::S3Error;
use serde::Deserialize;
use std::sync::Arc;
use tokio::sync::RwLock;
use tower_http::{
    cors::{Any, CorsLayer},
    trace::TraceLayer,
};

use crate::handlers;
use crate::middleware::{admin_auth_middleware, iam_auth_middleware, metrics_middleware};
use crate::multipart_store::DiskPartStore;

/// Default maximum upload size (5GB).
pub const DEFAULT_MAX_UPLOAD_SIZE: usize = 5 * 1024 * 1024 * 1024;

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
    /// Disk-backed store for multipart upload parts.
    ///
    /// Parts are written to temp files on disk, keeping memory usage flat.
    /// Data is lost on server crash — incomplete multipart uploads are not recoverable.
    pub part_store: Arc<DiskPartStore>,

    // --- Cluster coordinators (Phase 4-5 integration) ---
    // When `Some`, handlers route operations through quorum coordinators.
    // When `None` (standalone mode), handlers use the local storage engine directly.
    /// Quorum write coordinator for distributed PUT/DELETE.
    pub write_coordinator: Option<Arc<QuorumWriteCoordinator>>,
    /// Quorum read coordinator for distributed GET/HEAD.
    pub read_coordinator: Option<Arc<QuorumReadCoordinator>>,
    /// Distributed LIST coordinator for scatter-gather list operations.
    pub list_coordinator: Option<Arc<DistributedListCoordinator>>,
    /// Physical storage class assigned to newly-created bucket markers.
    pub default_bucket_storage_class: StorageClass,
    /// Edition-injected Erasure Coding admin operations.
    pub ec_admin_service: Arc<dyn EcAdminService>,
    /// Edition-injected Erasure Coding S3 object runtime hooks.
    pub ec_object_runtime: Arc<dyn EcObjectRuntime>,
    /// What the cluster routes of the admin API report: the pool, this node,
    /// anti-entropy and the scrubber. A single node reports nothing.
    pub cluster_admin_service: Arc<dyn ClusterAdminService>,
    /// The usage of the pool a cluster node last counted, reused for a few
    /// seconds by the bucket statistics and `/api/stats`.
    pub usage_cache: Arc<crate::handlers::usage::UsageCache>,
    /// Whether a cluster node reads bucket configurations through the
    /// quorums. Closed until every node of the pool adopted the
    /// configurations it stored before protocol 6; until then a node reads
    /// its own storage, as an earlier release did.
    pub config_read_gate: ConfigReadGate,
    /// The latest count of the versioning and Object Lock configurations a
    /// cluster node stores and does not enforce, reported by the cluster
    /// health.
    pub unenforced_configs: crate::handlers::UnenforcedConfigs,
    /// The pool taking over the IAM its nodes kept of their own; `None` on a
    /// single node, whose IAM is all there is.
    pub iam_adoption: Option<Arc<crate::cluster_iam::ClusterIamAdoption>>,
    /// How IAM stands on a cluster node — its copy, the pool's agreement with
    /// it, the lease of IAM changes; `None` on a single node.
    pub iam_status: Option<Arc<s4_cluster::IamStatusSource>>,
    /// The same freshness gate used for IAM credentials, checked without
    /// counting credential refusals by public readiness probes.
    pub iam_freshness: Option<Arc<s4_cluster::IamFreshness>>,
}

impl AppState {
    /// Creates a new application state.
    pub async fn new(
        storage: BitcaskStorageEngine,
        access_key_id: String,
        secret_access_key: String,
        data_dir: &std::path::Path,
    ) -> Self {
        Self::with_max_upload_size(
            storage,
            access_key_id,
            secret_access_key,
            DEFAULT_MAX_UPLOAD_SIZE,
            data_dir,
        )
        .await
    }

    /// Creates a new application state with custom max upload size.
    pub async fn with_max_upload_size(
        storage: BitcaskStorageEngine,
        access_key_id: String,
        secret_access_key: String,
        max_upload_size: usize,
        data_dir: &std::path::Path,
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

        // Sign tokens with a random secret until the server supplies the configured one
        // through `with_jwt_secret`. Never a fixed value: a secret anyone can read in the
        // source lets anyone mint an admin token.
        let auth_service = Arc::new(AuthService::new(
            (*iam_storage).clone(),
            JwtManager::new(&generate_jwt_secret(), jwt_lifetime_hours()),
        ));

        // Initialize disk-backed part store for multipart uploads
        let part_store = Arc::new(
            DiskPartStore::with_defaults(data_dir)
                .await
                .expect("Failed to initialize multipart part store"),
        );

        Self {
            storage: storage_arc,
            iam_storage,
            auth_service,
            access_key_id,
            secret_access_key,
            max_upload_size,
            prometheus_handle: None,
            start_time: std::time::Instant::now(),
            part_store,
            write_coordinator: None,
            read_coordinator: None,
            list_coordinator: None,
            default_bucket_storage_class: StorageClass::Standard,
            ec_admin_service: Arc::new(NoopEcAdminService),
            ec_object_runtime: Arc::new(NoopEcObjectRuntime),
            cluster_admin_service: Arc::new(NoopClusterAdminService),
            usage_cache: Arc::default(),
            config_read_gate: ConfigReadGate::default(),
            unenforced_configs: Default::default(),
            iam_adoption: None,
            iam_status: None,
            iam_freshness: None,
        }
    }

    /// Signs and verifies admin JWTs with `secret` from now on.
    ///
    /// Tokens issued before the call stop validating, so set it before the
    /// router starts serving. Every node of a cluster must use the same secret,
    /// or a token issued by one node is rejected by the others.
    pub fn with_jwt_secret(mut self, secret: &str) -> Self {
        let mut auth_service = AuthService::new(
            (*self.iam_storage).clone(),
            JwtManager::new(secret, jwt_lifetime_hours()),
        );
        if let Some(root) = self.auth_service.root() {
            auth_service = auth_service.with_root(root.clone());
        }
        self.auth_service = Arc::new(auth_service);
        self
    }

    /// Makes `root` the root account: logins with its name are checked against
    /// its configured password, and its id acts as SuperUser.
    ///
    /// The root account is not stored. Call [`AuthService::start_root`] once the
    /// state is built, to end root's earlier sessions after a password change.
    pub fn with_root_account(mut self, root: s4_features::iam::RootAccount) -> Self {
        self.auth_service = Arc::new((*self.auth_service).clone().with_root(root));
        self
    }

    /// Sets the Prometheus handle for rendering metrics.
    pub fn with_prometheus_handle(
        mut self,
        handle: metrics_exporter_prometheus::PrometheusHandle,
    ) -> Self {
        self.prometheus_handle = Some(handle);
        self
    }

    /// Sets the cluster coordinators for distributed mode.
    ///
    /// When coordinators are set, handlers route PUT/GET/HEAD/DELETE/LIST
    /// through quorum coordinators instead of the local storage engine.
    ///
    /// IAM changes go through them too, under the lease of IAM changes
    /// `iam_lease` takes, to a majority of the pool, while credentials are
    /// still checked against this node's own copy (plan
    /// CLUSTER-IAM-REPLICATION.md, decisions 1 and 3).
    pub fn with_cluster_coordinators(
        mut self,
        write: Arc<QuorumWriteCoordinator>,
        read: Arc<QuorumReadCoordinator>,
        list: Arc<DistributedListCoordinator>,
        iam_lease: Arc<s4_cluster::IamLeaseHolder>,
    ) -> Self {
        let iam_storage = self.iam_storage.with_record_store(Arc::new(
            crate::cluster_iam::ClusterRecordStore::new(write.clone(), iam_lease),
        ));
        self.auth_service = Arc::new(self.auth_service.with_storage(iam_storage.clone()));
        self.iam_storage = Arc::new(iam_storage);
        self.write_coordinator = Some(write);
        self.read_coordinator = Some(read);
        self.list_coordinator = Some(list);
        self
    }

    /// Checks IAM credentials only while `freshness` says a majority of the
    /// pool agrees with this node's copy of IAM (plan
    /// CLUSTER-IAM-REPLICATION.md, decision 4); root and the S3 credentials
    /// of the environment are not IAM's and work regardless.
    pub fn with_iam_freshness(mut self, freshness: Arc<s4_cluster::IamFreshness>) -> Self {
        self.iam_freshness = Some(freshness.clone());
        let iam_storage = self.iam_storage.with_credential_gate(Arc::new(
            crate::cluster_iam::ClusterCredentialGate::new(freshness),
        ));
        self.auth_service = Arc::new(self.auth_service.with_storage(iam_storage.clone()));
        self.iam_storage = Arc::new(iam_storage);
        self
    }

    /// Changes IAM only once `adoption`'s gate opens — every node of the pool
    /// keeps no IAM of its own the pool has not taken over — and answers the
    /// report and the decision of that adoption (plan
    /// CLUSTER-IAM-REPLICATION.md, decision 8).
    pub fn with_iam_adoption(mut self, adoption: crate::cluster_iam::ClusterIamAdoption) -> Self {
        let iam_storage = self.iam_storage.with_adoption_gate(Arc::new(
            crate::cluster_iam::ClusterAdoptionGate::new(adoption.gate().clone()),
        ));
        self.auth_service = Arc::new(self.auth_service.with_storage(iam_storage.clone()));
        self.iam_storage = Arc::new(iam_storage);
        self.iam_adoption = Some(Arc::new(adoption));
        self
    }

    /// Answers `GET /api/admin/iam/status` from `status`.
    pub fn with_iam_status(mut self, status: Arc<s4_cluster::IamStatusSource>) -> Self {
        self.iam_status = Some(status);
        self
    }

    /// Records the IAM changes made through this node under `name` in the
    /// audit log (plan CLUSTER-IAM-REPLICATION.md, decision 10).
    pub fn with_iam_node_name(mut self, name: &str) -> Self {
        let iam_storage = self.iam_storage.with_node_name(name);
        self.auth_service = Arc::new(self.auth_service.with_storage(iam_storage.clone()));
        self.iam_storage = Arc::new(iam_storage);
        self
    }

    /// Sets the gate that opens the quorum reads of bucket configurations.
    pub fn with_config_read_gate(mut self, gate: ConfigReadGate) -> Self {
        self.config_read_gate = gate;
        self
    }

    /// Sets the count of unenforced bucket configurations the cluster health
    /// reports.
    pub fn with_unenforced_configs(mut self, configs: crate::handlers::UnenforcedConfigs) -> Self {
        self.unenforced_configs = configs;
        self
    }

    /// Sets the physical storage class used for new bucket markers.
    pub fn with_default_bucket_storage_class(mut self, storage_class: StorageClass) -> Self {
        self.default_bucket_storage_class = storage_class;
        self
    }

    /// Sets the EC admin service implementation.
    pub fn with_ec_admin_service(mut self, service: Arc<dyn EcAdminService>) -> Self {
        self.ec_admin_service = service;
        self
    }

    /// Sets the EC object runtime implementation.
    pub fn with_ec_object_runtime(mut self, runtime: Arc<dyn EcObjectRuntime>) -> Self {
        self.ec_object_runtime = runtime;
        self
    }

    /// Sets what the cluster routes of the admin API report.
    pub fn with_cluster_admin_service(mut self, service: Arc<dyn ClusterAdminService>) -> Self {
        self.cluster_admin_service = service;
        self
    }

    /// Returns true if cluster mode is active (coordinators are set).
    pub fn is_cluster_mode(&self) -> bool {
        self.write_coordinator.is_some()
    }
}

/// Admin JWT lifetime in hours (`S4_JWT_LIFETIME_HOURS`, default 24).
fn jwt_lifetime_hours() -> i64 {
    std::env::var("S4_JWT_LIFETIME_HOURS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(24)
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

    // Must be delegated explicitly: the trait's default refuses, because
    // releasing a blob reference cannot be expressed through the other methods.
    async fn detach_object_data_if_hlc(
        &self,
        bucket: &str,
        key: &str,
        expected_hlc_timestamp: u64,
        expected_hlc_logical: u32,
        expected_origin_node_id: u128,
        layout: s4_core::types::ObjectLayout,
    ) -> Result<bool, s4_core::error::StorageError> {
        let storage = self.0.write().await;
        storage
            .detach_object_data_if_hlc(
                bucket,
                key,
                expected_hlc_timestamp,
                expected_hlc_logical,
                expected_origin_node_id,
                layout,
            )
            .await
    }

    // The mirror of the detach above, and delegated for the same reason: the
    // trait's default refuses, because writing a blob and rewriting the index
    // entry in one batch cannot be expressed through the other methods. This is
    // the path a reverse migration takes on every replica.
    async fn attach_object_data_if_hlc(
        &self,
        bucket: &str,
        key: &str,
        expected_hlc_timestamp: u64,
        expected_hlc_logical: u32,
        expected_origin_node_id: u128,
        data: &[u8],
    ) -> Result<bool, s4_core::error::StorageError> {
        let storage = self.0.write().await;
        storage
            .attach_object_data_if_hlc(
                bucket,
                key,
                expected_hlc_timestamp,
                expected_hlc_logical,
                expected_origin_node_id,
                data,
            )
            .await
    }

    // Local-only counterpart of the detach above; see the trait for why the
    // default refuses.
    async fn restore_erasure_coded_entry(
        &self,
        bucket: &str,
        key: &str,
        record: s4_core::types::IndexRecord,
    ) -> Result<bool, s4_core::error::StorageError> {
        let storage = self.0.write().await;
        storage.restore_erasure_coded_entry(bucket, key, record).await
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

    async fn list_objects_after(
        &self,
        bucket: &str,
        prefix: &str,
        start_after: &str,
        max_keys: usize,
    ) -> Result<Vec<(String, s4_core::types::IndexRecord)>, s4_core::error::StorageError> {
        let storage = self.0.read().await;
        storage.list_objects_after(bucket, prefix, start_after, max_keys).await
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

    async fn put_object_streaming(
        &self,
        _bucket: &str,
        _key: &str,
        _reader: Box<dyn tokio::io::AsyncRead + Unpin + Send>,
        _content_length: u64,
        _content_type: &str,
        _metadata: &std::collections::HashMap<String, String>,
        _etag: &str,
    ) -> Result<s4_core::storage::StreamingPutResult, s4_core::error::StorageError> {
        Err(s4_core::error::StorageError::InvalidOperation {
            operation: "streaming_put".to_string(),
            reason: "Not needed for IAM storage".to_string(),
        })
    }

    async fn open_object_stream(
        &self,
        _bucket: &str,
        _key: &str,
        _options: s4_core::storage::ReadOptions,
    ) -> Result<s4_core::storage::ObjectStream, s4_core::error::StorageError> {
        Err(s4_core::error::StorageError::InvalidOperation {
            operation: "streaming_read".to_string(),
            reason: "Not needed for IAM storage".to_string(),
        })
    }

    async fn open_object_version_stream(
        &self,
        _bucket: &str,
        _key: &str,
        _version_id: &str,
        _options: s4_core::storage::ReadOptions,
    ) -> Result<s4_core::storage::ObjectStream, s4_core::error::StorageError> {
        Err(s4_core::error::StorageError::InvalidOperation {
            operation: "streaming_read".to_string(),
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
    /// Maximum number of parts to return in ListParts.
    #[serde(rename = "max-parts")]
    max_parts: Option<i64>,
    /// Part number marker for ListParts pagination.
    #[serde(rename = "part-number-marker")]
    part_number_marker: Option<i64>,
    /// S3 Select marker (SelectObjectContent).
    select: Option<String>,
    /// S3 Select type parameter.
    #[serde(rename = "select-type")]
    select_type: Option<String>,
    /// Response override: Content-Disposition
    #[serde(rename = "response-content-disposition")]
    response_content_disposition: Option<String>,
    /// Response override: Content-Type
    #[serde(rename = "response-content-type")]
    response_content_type: Option<String>,
    /// Response override: Cache-Control
    #[serde(rename = "response-cache-control")]
    response_cache_control: Option<String>,
    /// Response override: Content-Encoding
    #[serde(rename = "response-content-encoding")]
    response_content_encoding: Option<String>,
    /// Response override: Content-Language
    #[serde(rename = "response-content-language")]
    response_content_language: Option<String>,
    /// Response override: Expires
    #[serde(rename = "response-expires")]
    response_expires: Option<String>,
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
    /// Bucket policy marker (GET/PUT/DELETE /{bucket}?policy).
    policy: Option<String>,
    /// Bucket policy status marker (GET /{bucket}?policyStatus).
    #[serde(rename = "policyStatus")]
    policy_status: Option<String>,
    /// S4 SQL query marker (POST /{bucket}?sql).
    sql: Option<String>,
    /// Bucket encryption marker (GET/PUT/DELETE /{bucket}?encryption).
    encryption: Option<String>,
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
    } else if config_params.policy.is_some() {
        handlers::get_bucket_policy(state, path).await.into_response()
    } else if config_params.policy_status.is_some() {
        handlers::get_bucket_policy_status(state, path).await.into_response()
    } else if config_params.encryption.is_some() {
        handlers::get_bucket_encryption(state, path).await.into_response()
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
    headers: HeaderMap,
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
    } else if params.policy.is_some() {
        handlers::put_bucket_policy(state, path, body).await.into_response()
    } else if params.encryption.is_some() {
        handlers::put_bucket_encryption(state, path, body).await.into_response()
    } else {
        // Default: CreateBucket (pass headers for x-amz-bucket-object-lock-enabled)
        handlers::create_bucket(state, path, headers).await.into_response()
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
    } else if params.policy.is_some() {
        handlers::delete_bucket_policy(state, path).await.into_response()
    } else if params.encryption.is_some() {
        handlers::delete_bucket_encryption(state, path).await.into_response()
    } else {
        // Default: DeleteBucket
        handlers::delete_bucket(state, path).await.into_response()
    }
}

/// Routes POST requests for bucket operations (including POST object upload).
async fn post_bucket_router(
    state: State<AppState>,
    path: Path<String>,
    user: Option<Extension<User>>,
    Query(params): Query<BucketConfigQueryParams>,
    headers: HeaderMap,
    request: axum::extract::Request,
) -> axum::response::Response {
    let is_multipart = headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.starts_with("multipart/form-data"))
        .unwrap_or(false);

    if params.sql.is_some() {
        // S4 SQL query — requires read permission (read-only query)
        if let Err(response) = check_read_permission(user.as_ref().map(|e| &e.0)) {
            return response;
        }

        use axum::extract::FromRequest;
        let body = match Bytes::from_request(request, &state.0).await {
            Ok(b) => b,
            Err(_) => {
                return S3Error::InternalError("Failed to read body".to_string()).into_response()
            }
        };
        handlers::bucket_sql_query(state, path, headers, body).await
    } else if params.delete.is_some() {
        // Check write permission (DeleteObjects requires write)
        if let Err(response) = check_write_permission(user.as_ref().map(|e| &e.0)) {
            return response;
        }

        use axum::extract::FromRequest;
        let body = match Bytes::from_request(request, &state.0).await {
            Ok(b) => b,
            Err(_) => {
                return S3Error::InternalError("Failed to read body".to_string()).into_response()
            }
        };
        handlers::delete_objects(state, path, headers, body).await.into_response()
    } else if is_multipart {
        use axum::extract::FromRequest;
        let multipart = match axum::extract::Multipart::from_request(request, &state.0).await {
            Ok(m) => m,
            Err(_) => {
                return S3Error::InvalidRequest("Invalid multipart body".into()).into_response()
            }
        };
        handlers::post_object(state, path, headers, multipart).await.into_response()
    } else {
        S3Error::MethodNotAllowed.into_response()
    }
}

/// Routes PUT requests - either PutObject, UploadPart, PutObjectRetention, or PutObjectLegalHold.
///
/// For UploadPart, the body is passed as `Body` (streaming) to avoid buffering
/// the entire part in memory. For other operations, the body is collected into `Bytes`.
async fn put_object_router(
    state: State<AppState>,
    path: Path<(String, String)>,
    user: Option<Extension<User>>,
    Query(params): Query<ObjectQueryParams>,
    headers: HeaderMap,
    body: Body,
) -> axum::response::Response {
    // Check write permission (all PUT object operations require write)
    if let Err(response) = check_write_permission(user.as_ref().map(|e| &e.0)) {
        return response;
    }

    // UploadPart: pass Body directly for streaming (no full buffering)
    if params.part_number.is_some() && params.upload_id.is_some() {
        let query = handlers::multipart::MultipartQuery {
            upload_id: params.upload_id,
            part_number: params.part_number,
            uploads: None,
            max_parts: None,
            part_number_marker: None,
        };
        return handlers::upload_part(state, path, Query(query), headers, body)
            .await
            .into_response();
    }

    // CopyObject: no body needed
    if headers.get("x-amz-copy-source").is_some() {
        return handlers::copy_object(state, path, headers).await.into_response();
    }

    // All other PUT operations need the body as Bytes
    let body = match axum::body::to_bytes(body, state.max_upload_size).await {
        Ok(b) => b,
        Err(_) => {
            return S3Error::InternalError("Failed to read request body".to_string())
                .into_response()
        }
    };

    // Check for Object Lock operations first
    if params.retention.is_some() {
        let version_query = handlers::object_lock::ObjectVersionQuery {
            version_id: params.version_id,
        };
        handlers::put_object_retention(state, path, Query(version_query), headers, body)
            .await
            .into_response()
    } else if params.legal_hold.is_some() {
        let version_query = handlers::object_lock::ObjectVersionQuery {
            version_id: params.version_id,
        };
        handlers::put_object_legal_hold(state, path, Query(version_query), body)
            .await
            .into_response()
    } else if params.tagging.is_some() {
        // PutObjectTagging: PUT /{bucket}/{key}?tagging
        let version_query = handlers::ObjectVersionQuery {
            version_id: params.version_id,
        };
        handlers::put_object_tagging(state, path, Query(version_query), body)
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
    headers: HeaderMap,
    Query(params): Query<ObjectQueryParams>,
) -> axum::response::Response {
    // Check write permission (all DELETE object operations require write)
    if let Err(response) = check_write_permission(user.as_ref().map(|e| &e.0)) {
        return response;
    }

    // DeleteObjectTagging: DELETE /{bucket}/{key}?tagging
    if params.tagging.is_some() {
        let version_query = handlers::ObjectVersionQuery {
            version_id: params.version_id,
        };
        return handlers::delete_object_tagging(state, path, Query(version_query))
            .await
            .into_response();
    }

    // If uploadId is present, this is AbortMultipartUpload
    if params.upload_id.is_some() {
        let query = handlers::multipart::MultipartQuery {
            upload_id: params.upload_id,
            part_number: None,
            uploads: None,
            max_parts: None,
            part_number_marker: None,
        };
        handlers::abort_multipart_upload(state, path, Query(query))
            .await
            .into_response()
    } else {
        // Pass version_id to delete_object for versioning support
        let bypass_governance = handlers::object_lock::is_bypass_governance(&headers);
        let version_query = handlers::ObjectVersionQuery {
            version_id: params.version_id,
        };
        handlers::delete_object(state, path, Query(version_query), bypass_governance)
            .await
            .into_response()
    }
}

/// Routes POST requests - CreateMultipartUpload, CompleteMultipartUpload, or SelectObjectContent.
async fn post_object_router(
    state: State<AppState>,
    path: Path<(String, String)>,
    user: Option<Extension<User>>,
    Query(params): Query<ObjectQueryParams>,
    headers: HeaderMap,
    body: Bytes,
) -> axum::response::Response {
    // If "uploads" query param is present, this is CreateMultipartUpload
    if params.uploads.is_some() {
        if let Err(response) = check_write_permission(user.as_ref().map(|e| &e.0)) {
            return response;
        }
        handlers::create_multipart_upload(state, path, headers).await.into_response()
    } else if params.upload_id.is_some() {
        // If uploadId is present, this is CompleteMultipartUpload
        if let Err(response) = check_write_permission(user.as_ref().map(|e| &e.0)) {
            return response;
        }
        let query = handlers::multipart::MultipartQuery {
            upload_id: params.upload_id,
            part_number: None,
            uploads: None,
            max_parts: None,
            part_number_marker: None,
        };
        handlers::complete_multipart_upload(state, path, Query(query), headers, body)
            .await
            .into_response()
    } else if params.select.is_some() || params.select_type.is_some() {
        // S3 SelectObjectContent — requires read permission (read-only query)
        if let Err(response) = check_read_permission(user.as_ref().map(|e| &e.0)) {
            return response;
        }
        handlers::select_object_content(state, path, headers, body).await
    } else {
        // Unknown POST operation on object path
        S3Error::MethodNotAllowed.into_response()
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
    } else if params.upload_id.is_some() {
        // ListParts: GET /{bucket}/{key}?uploadId=X
        let query = handlers::multipart::MultipartQuery {
            upload_id: params.upload_id,
            part_number: None,
            uploads: None,
            max_parts: params.max_parts,
            part_number_marker: params.part_number_marker,
        };
        handlers::list_parts(state, path, Query(query)).await.into_response()
    } else {
        // Default: GetObject
        let version_query = handlers::ObjectVersionQuery {
            version_id: params.version_id,
        };
        let mut response = handlers::get_object(state, path, Query(version_query), headers)
            .await
            .into_response();

        // Apply response override query parameters (for presigned URLs)
        if let Some(ref v) = params.response_content_disposition {
            response.headers_mut().insert(
                header::CONTENT_DISPOSITION,
                HeaderValue::from_str(v).unwrap_or_else(|_| HeaderValue::from_static("")),
            );
        }
        if let Some(ref v) = params.response_content_type {
            response.headers_mut().insert(
                header::CONTENT_TYPE,
                HeaderValue::from_str(v).unwrap_or_else(|_| HeaderValue::from_static("")),
            );
        }
        if let Some(ref v) = params.response_cache_control {
            response.headers_mut().insert(
                header::CACHE_CONTROL,
                HeaderValue::from_str(v).unwrap_or_else(|_| HeaderValue::from_static("")),
            );
        }
        if let Some(ref v) = params.response_content_encoding {
            response.headers_mut().insert(
                header::CONTENT_ENCODING,
                HeaderValue::from_str(v).unwrap_or_else(|_| HeaderValue::from_static("")),
            );
        }
        if let Some(ref v) = params.response_content_language {
            response.headers_mut().insert(
                header::CONTENT_LANGUAGE,
                HeaderValue::from_str(v).unwrap_or_else(|_| HeaderValue::from_static("")),
            );
        }
        if let Some(ref v) = params.response_expires {
            response.headers_mut().insert(
                header::EXPIRES,
                HeaderValue::from_str(v).unwrap_or_else(|_| HeaderValue::from_static("")),
            );
        }

        response
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
/// - `GET /api/admin/users` - List users, a page at a time (SuperUser only)
/// - `POST /api/admin/users` - Create user (SuperUser only)
/// - `GET /api/admin/users/:id` - Get user (SuperUser only)
/// - `PUT /api/admin/users/:id` - Update user (SuperUser only)
/// - `DELETE /api/admin/users/:id` - Delete user (SuperUser only)
/// - `POST /api/admin/users/:id/credentials` - Generate S3 credentials (SuperUser only)
/// - `DELETE /api/admin/users/:id/credentials` - Delete S3 credentials (SuperUser only)
/// - `POST /api/admin/users/:id/sessions/revoke` - End a user's sessions (SuperUser only)
/// - `POST /api/admin/logout` - End the caller's own sessions (any account)
/// - `GET /api/admin/iam/adoption` - What the nodes keep of their own (SuperUser only)
/// - `POST /api/admin/iam/adoption` - Take it over as decided (SuperUser only)
/// - `GET /api/admin/iam/audit` - The audit log of IAM changes (SuperUser only)
/// - `GET /api/admin/iam/status` - How IAM stands on this node and its pool (SuperUser only)
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
        .route(
            "/users/:id/sessions/revoke",
            post(handlers::admin::revoke_sessions),
        )
        .route("/logout", post(handlers::admin::logout))
        .route(
            "/iam/adoption",
            get(handlers::admin::iam_adoption_report).post(handlers::admin::adopt_iam),
        )
        .route("/iam/audit", get(handlers::admin::iam_audit))
        .route("/iam/status", get(handlers::admin::iam_status))
        .route("/bucket-stats", get(handlers::admin::bucket_stats))
        .route(
            "/buckets/:name",
            put(handlers::admin::create_bucket).delete(handlers::admin::delete_bucket),
        )
        .route(
            "/buckets/:name/objects",
            get(handlers::admin::list_bucket_objects),
        )
        .route("/compaction/run", post(handlers::admin::run_compaction))
        .route("/scrubber/run", post(handlers::admin::run_scrubber))
        .route("/scrubber/status", get(handlers::admin::scrubber_status))
        // Cluster operations (Phase 10)
        .route(
            "/cluster/health",
            get(handlers::admin_cluster::cluster_health),
        )
        .route(
            "/cluster/topology",
            get(handlers::admin_cluster::cluster_topology),
        )
        .route(
            "/cluster/repair-status",
            get(handlers::admin_cluster::repair_status),
        )
        .route(
            "/cluster/rebalance",
            get(handlers::admin_cluster::rebalance_status),
        )
        .route(
            "/cluster/rebalance/pause",
            post(handlers::admin_cluster::pause_rebalance),
        )
        .route(
            "/cluster/rebalance/resume",
            post(handlers::admin_cluster::resume_rebalance),
        )
        .route("/node/health", get(handlers::admin_cluster::node_health))
        .route("/node/drain", post(handlers::admin_cluster::node_drain))
        // Enterprise Erasure Coding operations (Phase 9)
        .route("/ec/pools", get(handlers::admin_ec::list_pools))
        .route(
            "/ec/pools/:pool/health",
            get(handlers::admin_ec::pool_health),
        )
        .route(
            "/ec/objects/:bucket/*key",
            get(handlers::admin_ec::object_manifest).post(handlers::admin_ec::verify_object),
        )
        .route(
            "/ec/pools/:pool/failure-matrix",
            get(handlers::admin_ec::failure_matrix),
        )
        .route(
            "/ec/pools/:pool/repair",
            post(handlers::admin_ec::repair_pool),
        )
        .route(
            "/ec/pools/:pool/repair-status",
            get(handlers::admin_ec::repair_status),
        )
        .route(
            "/ec/pools/:pool/sets",
            get(handlers::admin_ec::list_erasure_sets).post(handlers::admin_ec::add_erasure_set),
        )
        .route(
            "/ec/pools/:pool/sets/:set_id/deactivate",
            post(handlers::admin_ec::deactivate_erasure_set),
        )
        .route(
            "/ec/pools/:pool/topology",
            post(handlers::admin_ec::apply_pool_topology),
        )
        .route(
            "/ec/pools/:pool/rejoin/approve",
            post(handlers::admin_ec::approve_node_rejoin),
        )
        .route("/ec/dedup/report", get(handlers::admin_ec::dedup_report))
        .route("/ec/orphans", get(handlers::admin_ec::list_orphans))
        .route("/ec/orphans/gc", post(handlers::admin_ec::gc_orphans))
        .route("/ec/tombstones/gc", post(handlers::admin_ec::gc_tombstones))
        .route(
            "/ec/segments/unpack",
            post(handlers::admin_ec::unpack_packed_segments),
        )
        .route(
            "/ec/test/fail-reconstruction",
            post(handlers::admin_ec::test_fail_reconstruction),
        )
        .route(
            "/ec/test/transcode/pause",
            post(handlers::admin_ec::test_transcode_pause),
        )
        .route(
            "/ec/test/orphans/scan",
            post(handlers::admin_ec::test_scan_orphans),
        )
        .route(
            "/ec/test/codec/foreign-matrix",
            post(handlers::admin_ec::test_codec_foreign_matrix),
        )
        .route(
            "/ec/test/scrub/run",
            post(handlers::admin_ec::test_run_scrub),
        )
        .route(
            "/ec/test/anti-entropy/run",
            post(handlers::admin_ec::test_run_anti_entropy),
        )
        .route(
            "/ec/test/namespace/reconcile",
            post(handlers::admin_ec::test_reconcile_namespace),
        )
        // The static segment is matched before the parameter, so a bucket
        // named "dry-run" cannot shadow the dry-run route.
        .route(
            "/ec/tiering/dry-run",
            post(handlers::admin_ec::tiering_dry_run),
        )
        .route(
            "/ec/tiering/:bucket",
            get(handlers::admin_ec::tiering_policy).put(handlers::admin_ec::put_tiering_policy),
        )
        .route(
            "/ec/test/tiering/heat/:bucket/*key",
            get(handlers::admin_ec::test_object_heat),
        )
        .route(
            "/ec/tiering/:bucket/promote",
            post(handlers::admin_ec::promote_objects),
        )
        .route(
            "/ec/test/tiering/run",
            post(handlers::admin_ec::test_run_tiering_cycle),
        )
        .route(
            "/ec/test/promotion/run",
            post(handlers::admin_ec::test_run_promotion_cycle),
        )
        .route(
            "/ec/test/promotion/job/:bucket/*key",
            get(handlers::admin_ec::test_promotion_job),
        )
        .route(
            "/ec/test/transcode/job/:bucket/*key",
            get(handlers::admin_ec::test_transcode_job),
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
        // Health check endpoint (no auth required) — for Docker/Kubernetes probes
        .route("/health", get(|| async { StatusCode::OK }))
        // MinIO-compatible health check endpoints (no auth required)
        .route("/minio/health/live", get(|| async { StatusCode::OK }))
        .route("/minio/health/ready", get(handlers::readiness::readiness))
        // MinIO-compatible Prometheus metrics endpoints (no auth required)
        .route(
            "/minio/v2/metrics/cluster",
            get(handlers::stats::prometheus_metrics),
        )
        .route(
            "/minio/v2/metrics/node",
            get(handlers::stats::prometheus_metrics),
        )
        .route(
            "/minio/v2/metrics/bucket",
            get(handlers::stats::prometheus_metrics),
        )
        .route(
            "/minio/v2/metrics/resource",
            get(handlers::stats::prometheus_metrics),
        )
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

    /// The adapter must forward both halves of the EC layout migration.
    ///
    /// The trait defaults refuse rather than do nothing, so a method this
    /// adapter forgets to delegate becomes a loud failure somewhere far from
    /// here. Its sibling in `s4-server` was missed exactly that way, and the
    /// two are edited together.
    #[tokio::test]
    async fn the_iam_adapter_forwards_the_ec_layout_migration_both_ways() {
        let temp = tempfile::tempdir().unwrap();
        let storage = BitcaskStorageEngine::new(
            &temp.path().join("volumes"),
            &temp.path().join("metadata"),
            1_024 * 1_024,
            4_096,
            false,
        )
        .await
        .unwrap();
        let adapter = IamStorageAdapter(Arc::new(RwLock::new(storage)));

        let mut metadata = std::collections::HashMap::new();
        metadata.insert("_s4_hlc_wall".to_string(), "10".to_string());
        metadata.insert("_s4_hlc_logical".to_string(), "0".to_string());
        metadata.insert("_s4_hlc_node_id".to_string(), "1".to_string());
        adapter
            .put_object("bucket", "object.bin", b"payload", "text/plain", &metadata)
            .await
            .expect("write the object");

        assert!(adapter
            .detach_object_data_if_hlc(
                "bucket",
                "object.bin",
                10,
                0,
                1,
                s4_core::types::ObjectLayout::ErasureCoded {
                    erasure_set_id: 1,
                    topology_epoch: 1,
                },
            )
            .await
            .expect("the detach must be delegated, not refused by the trait default"));

        assert!(adapter
            .attach_object_data_if_hlc("bucket", "object.bin", 10, 0, 1, b"payload")
            .await
            .expect("the attach must be delegated, not refused by the trait default"));

        let (body, record) = adapter.get_object("bucket", "object.bin").await.expect("read back");
        assert_eq!(body, b"payload");
        assert!(!record.is_erasure_coded());
    }

    /// EC2-FIX-04-001: collecting the shards of deleted objects is an operator
    /// route, and the test-hook path it used to answer on is gone.
    ///
    /// Asked of the router itself rather than of a running cluster: which
    /// namespace a destructive action lives in is a property of this table, and
    /// a scenario that needs six nodes to notice a moved route notices it late.
    ///
    /// Registration is read off the answer's body, not its status. A path the
    /// admin router does not claim falls through to the S3 router, which reads
    /// it as a bucket request and refuses it — with the same `403` this CE
    /// build gives a registered EC route it has no Enterprise service for. The
    /// bodies differ, and that is what says which of the two happened.
    #[tokio::test]
    async fn the_tombstone_sweep_is_an_operator_route_and_not_a_test_hook() {
        /// Whether the EC admin layer answered — that is, whether this path is
        /// a route at all.
        async fn reaches_ec_admin(router: &Router, token: &str, uri: &str) -> bool {
            let request = axum::http::Request::builder()
                .method("POST")
                .uri(uri)
                .header("Host", "localhost")
                .header("Authorization", format!("Bearer {token}"))
                .body(axum::body::Body::empty())
                .unwrap();
            let response = tower::ServiceExt::oneshot(router.clone(), request).await.unwrap();
            let body = axum::body::to_bytes(response.into_body(), 64 * 1024).await.unwrap();
            String::from_utf8_lossy(&body).contains("erasure coding")
        }

        let temp = tempfile::tempdir().unwrap();
        let data_dir = temp.path().join("volumes");
        let metadata_dir = temp.path().join("metadata");
        let storage =
            BitcaskStorageEngine::new(&data_dir, &metadata_dir, 1_024 * 1_024, 4_096, false)
                .await
                .unwrap();
        let state = AppState::with_max_upload_size(
            storage,
            "access-key".to_string(),
            "secret-key".to_string(),
            DEFAULT_MAX_UPLOAD_SIZE,
            &data_dir,
        )
        .await;

        // A SuperUser token, because the admin router authorises before it
        // routes: without one every path under /api/admin answers the same.
        let user = state
            .iam_storage
            .create_user(
                "route_probe".to_string(),
                "route-probe-password".to_string(),
                s4_features::iam::Role::SuperUser,
            )
            .await
            .expect("create the probing user");
        let token = state
            .auth_service
            .login(s4_features::iam::LoginRequest {
                username: user.username.clone(),
                password: "route-probe-password".to_string(),
            })
            .await
            .expect("log the probing user in")
            .token;
        let router = create_router(state);

        assert!(
            reaches_ec_admin(&router, &token, "/api/admin/ec/tombstones/gc").await,
            "the operator route for the tombstone sweep is not registered"
        );
        assert!(
            !reaches_ec_admin(&router, &token, "/api/admin/ec/test/tombstone-gc/run").await,
            "an action that removes data must not live under /ec/test/"
        );

        // Two controls, so neither assertion above can pass for the wrong
        // reason: the neighbour this sweep now sits beside is registered, and
        // an address nobody registered is not.
        assert!(reaches_ec_admin(&router, &token, "/api/admin/ec/orphans/gc").await);
        assert!(!reaches_ec_admin(&router, &token, "/api/admin/ec/no-such-route").await);
    }

    #[tokio::test]
    async fn ec_admin_routes_do_not_panic_on_router_creation() {
        let temp = tempfile::tempdir().unwrap();
        let data_dir = temp.path().join("volumes");
        let metadata_dir = temp.path().join("metadata");
        let storage =
            BitcaskStorageEngine::new(&data_dir, &metadata_dir, 1_024 * 1_024, 4_096, false)
                .await
                .unwrap();

        let state = AppState::with_max_upload_size(
            storage,
            "access-key".to_string(),
            "secret-key".to_string(),
            DEFAULT_MAX_UPLOAD_SIZE,
            &data_dir,
        )
        .await;

        let _router = create_router(state);
    }
}
