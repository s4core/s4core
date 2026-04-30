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

//! HTTP request handlers for S3-compatible  API.
//!
//! This module provides handlers for:
//! - Bucket operations (create, delete, list, head)
//! - Bucket configuration (CORS, versioning, lifecycle, Object Lock)
//! - Object operations (put, get, delete, head)
//! - Object Lock operations (retention, legal hold)
//! - Multipart upload operations (create, upload part, complete, abort)
//! - Admin API operations (user management, IAM)

pub mod admin;
pub mod admin_cluster;
pub mod bucket;
pub mod bucket_config;
pub mod multipart;
pub mod object;
pub mod object_lock;
pub mod select;
pub mod stats;

pub use bucket::{
    create_bucket, delete_bucket, delete_objects, head_bucket, list_buckets, list_object_versions,
    list_objects, DeleteError, DeleteObjectsResult, DeletedObject, ListObjectVersionsQuery,
    ListObjectsQuery,
};
pub use bucket_config::{
    delete_bucket_cors, delete_bucket_encryption, delete_bucket_lifecycle, delete_bucket_policy,
    get_bucket_cors, get_bucket_encryption, get_bucket_lifecycle, get_bucket_location,
    get_bucket_policy, get_bucket_policy_status, get_bucket_versioning,
    get_bucket_versioning_status, get_cors_config, is_versioning_enabled, put_bucket_cors,
    put_bucket_encryption, put_bucket_lifecycle, put_bucket_policy, put_bucket_versioning,
};
pub use multipart::{
    abort_multipart_upload, complete_multipart_upload, create_multipart_upload, list_parts,
    upload_part,
};
pub use object::{
    copy_object, delete_object, delete_object_tagging, get_object, get_object_tagging, head_object,
    post_object, put_object, put_object_tagging, ObjectVersionQuery,
};
pub use object_lock::{
    get_bucket_object_lock_configuration, get_object_legal_hold, get_object_retention,
    put_bucket_object_lock_configuration, put_object_legal_hold, put_object_retention,
};
pub use select::{bucket_sql_query, select_object_content};

/// Check if a bucket exists in standalone mode.
///
/// In cluster mode (when `write_coordinator` is present), bucket markers are
/// replicated to all nodes via the write coordinator during `CreateBucket`, and
/// the replica write handler auto-creates markers as a defense-in-depth measure.
/// Therefore, this check is only needed in standalone mode.
///
/// Returns `Some(NoSuchBucket response)` if the bucket does not exist in
/// standalone mode, or `None` if the check passes (or cluster mode is active).
pub(crate) async fn check_bucket_standalone(
    state: &crate::server::AppState,
    storage: &dyn s4_core::StorageEngine,
    bucket: &str,
) -> Option<axum::response::Response> {
    use axum::response::IntoResponse;
    if state.write_coordinator.is_some() {
        return None; // cluster mode: bucket markers are replicated
    }
    let marker_key = format!("__s4_bucket_marker_{}", bucket);
    if storage.head_object("__system__", &marker_key).await.is_err() {
        return Some(crate::s3::errors::S3Error::NoSuchBucket.into_response());
    }
    None
}

/// Convert a cluster error into an HTTP response.
///
/// Shared by all handlers that interact with cluster coordinators.
pub(crate) fn cluster_error_to_response(
    err: &s4_cluster::ClusterError,
) -> axum::response::Response {
    use crate::s3::errors::S3Error;
    use axum::body::Body;
    use axum::http::StatusCode;
    use axum::response::IntoResponse;

    match err {
        s4_cluster::ClusterError::ObjectNotFound { .. } => S3Error::NoSuchKey.into_response(),
        s4_cluster::ClusterError::MultipartUploadNotFound { .. } => {
            S3Error::NoSuchUpload.into_response()
        }
        s4_cluster::ClusterError::InvalidMultipartPart { message, .. } => {
            S3Error::InvalidRequest(message.clone()).into_response()
        }
        s4_cluster::ClusterError::MultipartPartTooSmall { .. } => {
            S3Error::EntityTooSmall.into_response()
        }
        s4_cluster::ClusterError::QuorumNotMet { .. }
        | s4_cluster::ClusterError::WriteTimeout(_)
        | s4_cluster::ClusterError::ReadTimeout(_) => axum::response::Response::builder()
            .status(StatusCode::SERVICE_UNAVAILABLE)
            .body(Body::from(format!("{}", err)))
            .unwrap(),
        s4_cluster::ClusterError::EpochMismatch { .. } => axum::response::Response::builder()
            .status(StatusCode::SERVICE_UNAVAILABLE)
            .body(Body::from("Cluster topology is changing, retry"))
            .unwrap(),
        s4_cluster::ClusterError::Placement(msg) if msg.contains("no pool") => {
            S3Error::NoSuchBucket.into_response()
        }
        _ => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Cluster error: {}", err),
        )
            .into_response(),
    }
}
