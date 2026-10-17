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

//! Bucket configuration handlers.
//!
//! Implements S3-compatible bucket configuration operations:
//! - CORS (GET/PUT/DELETE /{bucket}?cors)
//! - Versioning (GET/PUT /{bucket}?versioning; not supported in cluster mode)
//! - Lifecycle (GET/PUT/DELETE /{bucket}?lifecycle)
//! - Policy (GET/PUT/DELETE /{bucket}?policy, GET /{bucket}?policyStatus)
//! - Encryption (GET/PUT/DELETE /{bucket}?encryption)
//! - Location (GET /{bucket}?location)
//!
//! In cluster mode CORS, lifecycle, policy and encryption are written and
//! read through the quorums, as the bucket itself, and a configuration
//! belongs to the bucket it was written to: one left from an earlier bucket
//! of the same name reads as not set (see `read_bucket_config`).

use axum::{
    body::{Body, Bytes},
    extract::{Path, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
};
use s4_cluster::{BucketConfigRead, HlcTimestamp, OwnConfig};
use s4_core::{StorageEngine, VersioningStatus};
use s4_features::cors::{cors_to_xml, parse_cors_xml};
use s4_features::lifecycle::{lifecycle_to_xml, parse_lifecycle_xml};
use std::collections::HashMap;
use tracing::{debug, error, info, warn};

use crate::s3::errors::S3Error;
use crate::server::AppState;

use s4_core::types::{
    CORS_CONFIG_KEY_PREFIX as CORS_CONFIG_PREFIX,
    ENCRYPTION_CONFIG_KEY_PREFIX as ENCRYPTION_CONFIG_PREFIX,
    LIFECYCLE_CONFIG_KEY_PREFIX as LIFECYCLE_CONFIG_PREFIX,
    POLICY_CONFIG_KEY_PREFIX as POLICY_CONFIG_PREFIX, SYSTEM_BUCKET,
    VERSIONING_CONFIG_KEY_PREFIX as VERSIONING_CONFIG_PREFIX,
};

// ============================================================================
// CORS Handlers
// ============================================================================

/// Gets the CORS configuration for a bucket.
///
/// S3 API: GET /{bucket}?cors
///
/// # Returns
///
/// - 200 OK with XML CORS configuration
/// - 404 NoSuchCORSConfiguration if CORS not configured
pub async fn get_bucket_cors(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
) -> impl IntoResponse {
    debug!("GetBucketCors: {}", bucket);

    match read_bucket_config(&state, &bucket, CORS_CONFIG_PREFIX).await {
        Ok(Some(data)) => config_response(data, "application/xml"),
        Ok(None) => S3Error::NoSuchCORSConfiguration.into_response(),
        Err(resp) => resp,
    }
}

/// Sets the CORS configuration for a bucket.
///
/// S3 API: PUT /{bucket}?cors
///
/// # Returns
///
/// - 200 OK if CORS configuration saved successfully
/// - 400 Bad Request if invalid CORS configuration
#[allow(clippy::result_large_err)]
pub async fn put_bucket_cors(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
    body: Bytes,
) -> impl IntoResponse {
    info!("PutBucketCors: {}", bucket);

    let stored = write_bucket_config(
        &state,
        &bucket,
        CORS_CONFIG_PREFIX,
        "application/xml",
        || {
            let config = parse_cors_xml(&String::from_utf8_lossy(&body)).map_err(|e| {
                error!("Failed to parse CORS XML: {:?}", e);
                S3Error::MalformedXML.into_response()
            })?;
            config.validate().map_err(|e| {
                error!("Invalid CORS configuration: {:?}", e);
                S3Error::InvalidRequest(e.to_string()).into_response()
            })?;
            Ok(cors_to_xml(&config).into_bytes())
        },
    )
    .await;
    if let Err(resp) = stored {
        return resp;
    }

    info!("CORS configuration saved for bucket: {}", bucket);
    StatusCode::OK.into_response()
}

/// Deletes the CORS configuration for a bucket.
///
/// S3 API: DELETE /{bucket}?cors
///
/// # Returns
///
/// - 204 No Content if CORS configuration deleted
/// - 404 Not Found if bucket doesn't exist
pub async fn delete_bucket_cors(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
) -> impl IntoResponse {
    info!("DeleteBucketCors: {}", bucket);

    if let Err(resp) = delete_bucket_config(&state, &bucket, CORS_CONFIG_PREFIX).await {
        return resp;
    }

    info!("CORS configuration deleted for bucket: {}", bucket);
    StatusCode::NO_CONTENT.into_response()
}

// ============================================================================
// Versioning Handlers
// ============================================================================

// Note: VersioningStatus is imported from s4_core

/// Gets the versioning configuration for a bucket.
///
/// S3 API: GET /{bucket}?versioning
///
/// # Returns
///
/// - 200 OK with XML versioning configuration
///
/// In cluster mode the configuration is always empty: cluster mode keeps no
/// versions, whatever a node stored before it refused to turn them on.
pub async fn get_bucket_versioning(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
) -> impl IntoResponse {
    debug!("GetBucketVersioning: {}", bucket);

    if let Some(resp) = super::check_bucket_cluster(&state, &bucket).await {
        return resp;
    }
    let storage = state.storage.read().await;

    // Check if bucket exists (standalone; cluster mode checked above)
    if let Some(resp) = super::check_bucket_standalone(&state, &*storage, &bucket).await {
        return resp;
    }

    let status = if state.is_cluster_mode() {
        VersioningStatus::Unversioned
    } else {
        get_versioning_status_internal(&*storage, &bucket).await
    };
    let xml = versioning_configuration_xml(status);

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/xml")
        .body(Body::from(xml))
        .unwrap()
}

/// Sets the versioning configuration for a bucket.
///
/// S3 API: PUT /{bucket}?versioning
///
/// **S3 Rule**: Once versioning is enabled, the bucket can NEVER return to
/// `Unversioned`. It can only be suspended.
///
/// # Returns
///
/// - 200 OK if versioning configuration saved successfully
/// - 400 Bad Request if invalid versioning configuration or trying to disable versioning
/// - 501 Not Implemented in cluster mode, which keeps no versions
pub async fn put_bucket_versioning(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
    body: Bytes,
) -> impl IntoResponse {
    info!("PutBucketVersioning: {}", bucket);

    if let Some(resp) = super::cluster_unsupported::refuse_in_cluster(
        &state,
        super::cluster_unsupported::VERSIONING_UNSUPPORTED,
    ) {
        return resp;
    }
    if let Some(resp) = super::check_bucket_cluster(&state, &bucket).await {
        return resp;
    }
    let storage = state.storage.read().await;

    // Check if bucket exists (standalone; cluster mode checked above)
    if let Some(resp) = super::check_bucket_standalone(&state, &*storage, &bucket).await {
        return resp;
    }

    // Parse versioning configuration from XML
    let xml = String::from_utf8_lossy(&body);
    let new_status = parse_versioning_xml(&xml);

    // S3 Rule: Once versioning is enabled, bucket can NEVER return to unversioned
    let current_status = get_versioning_status_internal(&*storage, &bucket).await;
    if current_status != VersioningStatus::Unversioned
        && new_status == VersioningStatus::Unversioned
    {
        warn!(
            "Attempt to disable versioning on bucket {} (current: {:?})",
            bucket, current_status
        );
        return S3Error::InvalidRequest(
            "Versioning cannot be disabled once enabled. Use 'Suspended' instead.".to_string(),
        )
        .into_response();
    }

    // Store versioning configuration
    let versioning_key = format!("{}{}", VERSIONING_CONFIG_PREFIX, bucket);

    match new_status {
        VersioningStatus::Enabled | VersioningStatus::Suspended => {
            let status_str = new_status.as_xml_status().unwrap_or("Enabled");
            if let Err(e) = storage
                .put_object(
                    SYSTEM_BUCKET,
                    &versioning_key,
                    status_str.as_bytes(),
                    "text/plain",
                    &HashMap::new(),
                )
                .await
            {
                error!("Failed to store versioning configuration: {:?}", e);
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to store versioning configuration",
                )
                    .into_response();
            }
        }
        VersioningStatus::Unversioned => {
            // This case is only reached if current is also Unversioned (no-op)
            // We don't delete the config key because it was never set
        }
    }

    info!(
        "Versioning configuration saved for bucket: {} (status: {:?})",
        bucket, new_status
    );
    StatusCode::OK.into_response()
}

/// Generates XML for versioning configuration.
fn versioning_configuration_xml(status: VersioningStatus) -> String {
    let mut xml = String::from(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">"#,
    );

    if let Some(status_str) = status.as_xml_status() {
        xml.push_str(&format!("\n  <Status>{}</Status>", status_str));
    }

    xml.push_str("\n</VersioningConfiguration>");
    xml
}

/// Parses versioning status from S3 XML.
fn parse_versioning_xml(xml: &str) -> VersioningStatus {
    // Simple parsing - look for <Status>Enabled</Status> or <Status>Suspended</Status>
    if xml.contains("<Status>Enabled</Status>") {
        VersioningStatus::Enabled
    } else if xml.contains("<Status>Suspended</Status>") {
        VersioningStatus::Suspended
    } else {
        VersioningStatus::Unversioned
    }
}

/// Internal helper to get versioning status from storage.
async fn get_versioning_status_internal(
    storage: &impl StorageEngine,
    bucket: &str,
) -> VersioningStatus {
    let versioning_key = format!("{}{}", VERSIONING_CONFIG_PREFIX, bucket);
    match storage.get_object(SYSTEM_BUCKET, &versioning_key).await {
        Ok((data, _)) => {
            let status_str = String::from_utf8_lossy(&data);
            VersioningStatus::from_xml_status(status_str.trim())
        }
        Err(_) => VersioningStatus::Unversioned,
    }
}

// ============================================================================
// Lifecycle Handlers
// ============================================================================

/// Gets the lifecycle configuration for a bucket.
///
/// S3 API: GET /{bucket}?lifecycle
///
/// # Returns
///
/// - 200 OK with XML lifecycle configuration
/// - 404 NoSuchLifecycleConfiguration if lifecycle not configured
pub async fn get_bucket_lifecycle(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
) -> impl IntoResponse {
    debug!("GetBucketLifecycle: {}", bucket);

    match read_bucket_config(&state, &bucket, LIFECYCLE_CONFIG_PREFIX).await {
        Ok(Some(data)) => config_response(data, "application/xml"),
        Ok(None) => S3Error::NoSuchLifecycleConfiguration.into_response(),
        Err(resp) => resp,
    }
}

/// Sets the lifecycle configuration for a bucket.
///
/// S3 API: PUT /{bucket}?lifecycle
///
/// # Returns
///
/// - 200 OK if lifecycle configuration saved successfully
/// - 400 Bad Request if invalid lifecycle configuration
#[allow(clippy::result_large_err)]
pub async fn put_bucket_lifecycle(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
    body: Bytes,
) -> impl IntoResponse {
    info!("PutBucketLifecycle: {}", bucket);

    let mut rules = 0;
    let stored = write_bucket_config(
        &state,
        &bucket,
        LIFECYCLE_CONFIG_PREFIX,
        "application/xml",
        || {
            let config = parse_lifecycle_xml(&String::from_utf8_lossy(&body)).map_err(|e| {
                error!("Failed to parse lifecycle XML: {:?}", e);
                S3Error::MalformedXML.into_response()
            })?;
            config.validate().map_err(|e| {
                error!("Invalid lifecycle configuration: {:?}", e);
                S3Error::InvalidRequest(e.to_string()).into_response()
            })?;
            rules = config.rules.len();
            Ok(lifecycle_to_xml(&config).into_bytes())
        },
    )
    .await;
    if let Err(resp) = stored {
        return resp;
    }

    info!(
        "Lifecycle configuration saved for bucket: {} ({} rules)",
        bucket, rules
    );
    StatusCode::OK.into_response()
}

/// Deletes the lifecycle configuration for a bucket.
///
/// S3 API: DELETE /{bucket}?lifecycle
///
/// # Returns
///
/// - 204 No Content if lifecycle configuration deleted
/// - 404 Not Found if bucket doesn't exist
pub async fn delete_bucket_lifecycle(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
) -> impl IntoResponse {
    info!("DeleteBucketLifecycle: {}", bucket);

    if let Err(resp) = delete_bucket_config(&state, &bucket, LIFECYCLE_CONFIG_PREFIX).await {
        return resp;
    }

    info!("Lifecycle configuration deleted for bucket: {}", bucket);
    StatusCode::NO_CONTENT.into_response()
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Gets the versioning status for a bucket.
///
/// This is the primary function for object handlers to get versioning state.
pub async fn get_bucket_versioning_status(state: &AppState, bucket: &str) -> VersioningStatus {
    let storage = state.storage.read().await;
    get_versioning_status_internal(&*storage, bucket).await
}

/// Checks if versioning is enabled for a bucket.
///
/// This is a convenience function - returns true only if status is `Enabled`.
pub async fn is_versioning_enabled(state: &AppState, bucket: &str) -> bool {
    get_bucket_versioning_status(state, bucket).await == VersioningStatus::Enabled
}

// ============================================================================
// Configuration Storage
// ============================================================================

/// The `__system__` key of the bucket's configuration stored under `prefix`.
fn config_key(prefix: &str, bucket: &str) -> String {
    format!("{prefix}{bucket}")
}

/// A stored configuration as the answer to a GET.
fn config_response(data: Vec<u8>, content_type: &str) -> Response {
    let mut response = Response::new(Body::from(data));
    if let Ok(value) = header::HeaderValue::from_str(content_type) {
        response.headers_mut().insert(header::CONTENT_TYPE, value);
    }
    response
}

/// The bucket's configuration stored under `prefix`: `Ok(None)` when it is
/// not set, and `Err` with the answer to give when the bucket does not exist
/// or, in cluster mode, too few replicas answered to tell (`503`).
///
/// In cluster mode the bucket's marker and the configuration are each read
/// through a read quorum, so a configuration written through any node is seen
/// through every other at once. A configuration counts only when it belongs
/// to the generation of the live marker
/// ([`config_of_generation`](s4_cluster::config_of_generation)): an older one
/// belongs to an earlier bucket of the same name — a write that passed its
/// bucket check before that bucket was deleted and landed after — and reads
/// as not set.
///
/// Until the pool adopted the configurations an earlier release stored on
/// single nodes ([`AppState::config_read_gate`]), such a configuration may
/// sit only on this node, which a quorum read would miss: this node's own copy
/// is then weighed too, by the same rule, and is the answer unless a replica
/// holds a newer value or its delete
/// ([`read_bucket_config_with_own`](s4_cluster::QuorumReadCoordinator::read_bucket_config_with_own)).
#[allow(clippy::result_large_err)]
pub(crate) async fn read_bucket_config(
    state: &AppState,
    bucket: &str,
    prefix: &str,
) -> Result<Option<Vec<u8>>, Response> {
    let key = config_key(prefix, bucket);
    if let Some(read_coord) = state.read_coordinator.as_ref() {
        let own = if state.config_read_gate.is_open() {
            None
        } else {
            let storage = state.storage.read().await;
            storage
                .get_object(SYSTEM_BUCKET, &key)
                .await
                .ok()
                .map(|(data, record)| OwnConfig {
                    data,
                    hlc: HlcTimestamp::of_record(&record),
                    etag: record.etag,
                })
        };
        return match read_coord.read_bucket_config_with_own(bucket, &key, own).await {
            Ok(BucketConfigRead::Set(data)) => Ok(Some(data)),
            Ok(BucketConfigRead::NotSet) => Ok(None),
            Ok(BucketConfigRead::NoSuchBucket) => Err(S3Error::NoSuchBucket.into_response()),
            Err(e) => {
                warn!(bucket, key, error = %e, "bucket configuration could not be read");
                Err(super::cluster_error_to_response(&e))
            }
        };
    }

    let storage = state.storage.read().await;
    if let Some(resp) = super::check_bucket_standalone(state, &*storage, bucket).await {
        return Err(resp);
    }
    Ok(storage.get_object(SYSTEM_BUCKET, &key).await.ok().map(|(data, _)| data))
}

/// Stores the bucket's configuration under `prefix`, as `value` makes it once
/// the bucket is known to exist: its error is the answer to an invalid
/// configuration, given only for a bucket that exists.
///
/// In cluster mode the bucket is checked through a read quorum and the
/// configuration written through the write quorum; when too few replicas
/// answer, the answer is `503` and the client may retry. The write's HLC is
/// newer than the marker the check read, since the check moved this node's
/// clock past it: the configuration belongs to the bucket it was written to
/// ([`read_bucket_config`]).
#[allow(clippy::result_large_err)]
pub(crate) async fn write_bucket_config(
    state: &AppState,
    bucket: &str,
    prefix: &str,
    content_type: &str,
    value: impl FnOnce() -> Result<Vec<u8>, Response>,
) -> Result<(), Response> {
    let key = config_key(prefix, bucket);
    if let Some(write_coord) = state.write_coordinator.as_ref() {
        if let Some(resp) = super::check_bucket_cluster(state, bucket).await {
            return Err(resp);
        }
        let data = value()?;
        return write_coord
            .write(SYSTEM_BUCKET, &key, &data, content_type, &HashMap::new())
            .await
            .map(|_| ())
            .map_err(|e| {
                error!(bucket, key, error = %e, "failed to replicate bucket configuration");
                super::cluster_error_to_response(&e)
            });
    }

    let storage = state.storage.read().await;
    if let Some(resp) = super::check_bucket_standalone(state, &*storage, bucket).await {
        return Err(resp);
    }
    let data = value()?;
    storage
        .put_object(SYSTEM_BUCKET, &key, &data, content_type, &HashMap::new())
        .await
        .map(|_| ())
        .map_err(|e| {
            error!(bucket, key, error = %e, "failed to store bucket configuration");
            S3Error::InternalError("Failed to store bucket configuration".to_string())
                .into_response()
        })
}

/// Deletes the bucket's configuration under `prefix`; deleting one that is
/// not set succeeds.
///
/// In cluster mode the delete goes through the write quorum and leaves a
/// tombstone on the replicas, so that no replica that missed it brings the
/// configuration back; when too few replicas answer, the answer is `503`.
#[allow(clippy::result_large_err)]
pub(crate) async fn delete_bucket_config(
    state: &AppState,
    bucket: &str,
    prefix: &str,
) -> Result<(), Response> {
    let key = config_key(prefix, bucket);
    if let Some(write_coord) = state.write_coordinator.as_ref() {
        if let Some(resp) = super::check_bucket_cluster(state, bucket).await {
            return Err(resp);
        }
        return write_coord.delete(SYSTEM_BUCKET, &key).await.map(|_| ()).map_err(|e| {
            error!(bucket, key, error = %e, "failed to replicate bucket configuration delete");
            super::cluster_error_to_response(&e)
        });
    }

    let storage = state.storage.read().await;
    if let Some(resp) = super::check_bucket_standalone(state, &*storage, bucket).await {
        return Err(resp);
    }
    // A configuration that is not set is deleted already.
    let _ = storage.delete_object(SYSTEM_BUCKET, &key).await;
    Ok(())
}

// ============================================================================
// Location Handler
// ============================================================================

/// Handle GET /{bucket}?location - returns the bucket's location constraint.
/// S4 always returns `us-east-1` since it is a single-region storage server.
pub async fn get_bucket_location(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
) -> impl IntoResponse {
    debug!("GetBucketLocation: {}", bucket);

    if let Some(resp) = super::check_bucket_cluster(&state, &bucket).await {
        return resp;
    }
    let storage = state.storage.read().await;

    // Check if bucket exists (standalone; cluster mode checked above)
    if let Some(resp) = super::check_bucket_standalone(&state, &*storage, &bucket).await {
        return resp;
    }

    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>"#;

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/xml")
        .body(Body::from(xml))
        .unwrap()
        .into_response()
}

// ============================================================================
// Bucket Policy Handlers
// ============================================================================

/// Gets the bucket policy.
///
/// S3 API: GET /{bucket}?policy
pub async fn get_bucket_policy(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
) -> impl IntoResponse {
    debug!("GetBucketPolicy: {}", bucket);

    match read_bucket_config(&state, &bucket, POLICY_CONFIG_PREFIX).await {
        Ok(Some(data)) => config_response(data, "application/json"),
        Ok(None) => no_such_bucket_policy(&bucket),
        Err(resp) => resp,
    }
}

/// `NoSuchBucketPolicy`: the bucket exists and has no policy. S3 returns it
/// as a generic error with this code.
fn no_such_bucket_policy(bucket: &str) -> Response {
    let request_id = uuid::Uuid::new_v4().to_string();
    let xml = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>NoSuchBucketPolicy</Code>
  <Message>The bucket policy does not exist</Message>
  <BucketName>{}</BucketName>
  <RequestId>{}</RequestId>
</Error>"#,
        bucket, request_id
    );
    let mut response = (StatusCode::NOT_FOUND, xml).into_response();
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static("application/xml"),
    );
    if let Ok(value) = header::HeaderValue::from_str(&request_id) {
        headers.insert("x-amz-request-id", value);
    }
    response
}

/// Sets the bucket policy.
///
/// S3 API: PUT /{bucket}?policy
#[allow(clippy::result_large_err)]
pub async fn put_bucket_policy(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
    body: Bytes,
) -> impl IntoResponse {
    info!("PutBucketPolicy: {}", bucket);

    let stored = write_bucket_config(
        &state,
        &bucket,
        POLICY_CONFIG_PREFIX,
        "application/json",
        || {
            if serde_json::from_slice::<serde_json::Value>(&body).is_err() {
                return Err(S3Error::MalformedXML.into_response());
            }
            Ok(body.to_vec())
        },
    )
    .await;
    if let Err(resp) = stored {
        return resp;
    }

    info!("Bucket policy saved for bucket: {}", bucket);
    StatusCode::NO_CONTENT.into_response()
}

/// Deletes the bucket policy.
///
/// S3 API: DELETE /{bucket}?policy
pub async fn delete_bucket_policy(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
) -> impl IntoResponse {
    info!("DeleteBucketPolicy: {}", bucket);

    if let Err(resp) = delete_bucket_config(&state, &bucket, POLICY_CONFIG_PREFIX).await {
        return resp;
    }

    info!("Bucket policy deleted for bucket: {}", bucket);
    StatusCode::NO_CONTENT.into_response()
}

/// Gets the bucket policy status (whether the policy is public).
///
/// S3 API: GET /{bucket}?policyStatus
pub async fn get_bucket_policy_status(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
) -> impl IntoResponse {
    debug!("GetBucketPolicyStatus: {}", bucket);

    if let Some(resp) = super::check_bucket_cluster(&state, &bucket).await {
        return resp;
    }
    let storage = state.storage.read().await;

    // Check if bucket exists (standalone; cluster mode checked above)
    if let Some(resp) = super::check_bucket_standalone(&state, &*storage, &bucket).await {
        return resp;
    }

    // Without Block Public Access settings, IsPublic is always false
    let is_public = false;

    let xml = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<PolicyStatus xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <IsPublic>{}</IsPublic>
</PolicyStatus>"#,
        if is_public { "true" } else { "false" }
    );

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/xml")
        .body(Body::from(xml))
        .unwrap()
}

// ============================================================================
// Bucket Encryption Configuration (stub — S4 uses AES-256 for all objects)
// ============================================================================

/// Default SSE-S3 encryption configuration XML (AES256).
const DEFAULT_ENCRYPTION_XML: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<ServerSideEncryptionConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Rule>
    <ApplyServerSideEncryptionByDefault>
      <SSEAlgorithm>AES256</SSEAlgorithm>
    </ApplyServerSideEncryptionByDefault>
  </Rule>
</ServerSideEncryptionConfiguration>"#;

/// Sets bucket encryption configuration (stub — accepts and stores config).
///
/// S3 API: PUT /{bucket}?encryption
#[allow(clippy::result_large_err)]
pub async fn put_bucket_encryption(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
    body: Bytes,
) -> impl IntoResponse {
    debug!("PutBucketEncryption: {}", bucket);

    let stored = write_bucket_config(
        &state,
        &bucket,
        ENCRYPTION_CONFIG_PREFIX,
        "application/xml",
        || {
            Ok(if body.is_empty() {
                DEFAULT_ENCRYPTION_XML.as_bytes().to_vec()
            } else {
                body.to_vec()
            })
        },
    )
    .await;
    match stored {
        Ok(()) => StatusCode::OK.into_response(),
        Err(resp) => resp,
    }
}

/// Gets bucket encryption configuration.
///
/// S3 API: GET /{bucket}?encryption
pub async fn get_bucket_encryption(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
) -> impl IntoResponse {
    debug!("GetBucketEncryption: {}", bucket);

    match read_bucket_config(&state, &bucket, ENCRYPTION_CONFIG_PREFIX).await {
        Ok(Some(data)) => config_response(data, "application/xml"),
        Ok(None) => S3Error::ServerSideEncryptionConfigurationNotFound.into_response(),
        Err(resp) => resp,
    }
}

/// Deletes bucket encryption configuration.
///
/// S3 API: DELETE /{bucket}?encryption
pub async fn delete_bucket_encryption(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
) -> impl IntoResponse {
    debug!("DeleteBucketEncryption: {}", bucket);

    match delete_bucket_config(&state, &bucket, ENCRYPTION_CONFIG_PREFIX).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(resp) => resp,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_versioning_status_conversion() {
        assert_eq!(
            VersioningStatus::from_xml_status("Enabled"),
            VersioningStatus::Enabled
        );
        assert_eq!(
            VersioningStatus::from_xml_status("Suspended"),
            VersioningStatus::Suspended
        );
        assert_eq!(
            VersioningStatus::from_xml_status(""),
            VersioningStatus::Unversioned
        );
    }

    #[test]
    fn test_versioning_xml_generation() {
        let xml = versioning_configuration_xml(VersioningStatus::Enabled);
        assert!(xml.contains("<Status>Enabled</Status>"));

        let xml = versioning_configuration_xml(VersioningStatus::Suspended);
        assert!(xml.contains("<Status>Suspended</Status>"));

        let xml = versioning_configuration_xml(VersioningStatus::Unversioned);
        assert!(!xml.contains("<Status>"));
    }

    #[test]
    fn test_parse_versioning_xml() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<VersioningConfiguration>
  <Status>Enabled</Status>
</VersioningConfiguration>"#;
        assert_eq!(parse_versioning_xml(xml), VersioningStatus::Enabled);

        let xml =
            r#"<VersioningConfiguration><Status>Suspended</Status></VersioningConfiguration>"#;
        assert_eq!(parse_versioning_xml(xml), VersioningStatus::Suspended);

        let xml = r#"<VersioningConfiguration></VersioningConfiguration>"#;
        assert_eq!(parse_versioning_xml(xml), VersioningStatus::Unversioned);
    }
}
