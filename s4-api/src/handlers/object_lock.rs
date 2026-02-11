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

//! Object Lock configuration handlers.
//!
//! Implements S3-compatible Object Lock operations:
//! - Bucket Object Lock configuration (GET/PUT /{bucket}?object-lock)
//! - Object retention (GET/PUT /{bucket}/{key}?retention)
//! - Legal hold (GET/PUT /{bucket}/{key}?legal-hold)

use axum::{
    body::{Body, Bytes},
    extract::{Path, Query, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
};
use s4_core::{StorageEngine, VersioningStatus};
use s4_features::object_lock::{
    legal_hold_to_xml, object_lock_to_xml, parse_legal_hold_xml, parse_object_lock_xml,
    parse_retention_xml, retention_to_xml, LegalHoldStatus, ObjectLockConfiguration, RetentionMode,
};
use std::collections::HashMap;
use tracing::{debug, error, info, warn};

use crate::s3::errors::S3Error;
use crate::server::AppState;

// Storage key prefix for Object Lock configuration
const OBJECT_LOCK_CONFIG_PREFIX: &str = "__s4_bucket_object_lock_";

// ============================================================================
// Bucket Object Lock Configuration Handlers
// ============================================================================

/// Gets the Object Lock configuration for a bucket.
///
/// S3 API: GET /{bucket}?object-lock
///
/// # Returns
///
/// - 200 OK with XML Object Lock configuration
/// - 404 ObjectLockConfigurationNotFoundError if not configured
pub async fn get_bucket_object_lock_configuration(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
) -> impl IntoResponse {
    debug!("GetBucketObjectLockConfiguration: {}", bucket);

    let storage = state.storage.read().await;

    // Check if bucket exists
    let bucket_marker_key = format!("__s4_bucket_marker_{}", bucket);
    if storage.head_object("__system__", &bucket_marker_key).await.is_err() {
        return S3Error::NoSuchBucket.into_response();
    }

    // Get Object Lock configuration
    let lock_key = format!("{}{}", OBJECT_LOCK_CONFIG_PREFIX, bucket);
    match storage.get_object("__system__", &lock_key).await {
        Ok((data, _)) => {
            // Return stored XML directly
            let xml = String::from_utf8_lossy(&data).to_string();
            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/xml")
                .body(Body::from(xml))
                .unwrap()
        }
        Err(_) => {
            // No Object Lock configuration
            S3Error::ObjectLockConfigurationNotFound.into_response()
        }
    }
}

/// Sets the Object Lock configuration for a bucket.
///
/// S3 API: PUT /{bucket}?object-lock
///
/// # Returns
///
/// - 200 OK if configuration saved successfully
/// - 400 Bad Request if invalid configuration or versioning not enabled
///
/// # Critical Validations
///
/// - Versioning MUST be Enabled (not just Suspended)
/// - Object Lock cannot be disabled once enabled
pub async fn put_bucket_object_lock_configuration(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
    body: Bytes,
) -> impl IntoResponse {
    info!("PutBucketObjectLockConfiguration: {}", bucket);

    let storage = state.storage.read().await;

    // Check if bucket exists
    let bucket_marker_key = format!("__s4_bucket_marker_{}", bucket);
    if storage.head_object("__system__", &bucket_marker_key).await.is_err() {
        return S3Error::NoSuchBucket.into_response();
    }

    // CRITICAL: Check versioning is Enabled (not just Suspended)
    // Object Lock requires versioning to be fully enabled
    let versioning_status = get_bucket_versioning_status_internal(&*storage, &bucket).await;
    if versioning_status != VersioningStatus::Enabled {
        error!(
            "Object Lock requires versioning to be enabled, current status: {:?}",
            versioning_status
        );
        return S3Error::InvalidBucketState(
            "Versioning must be enabled to use Object Lock".to_string(),
        )
        .into_response();
    }

    // Parse and validate Object Lock configuration
    let xml = String::from_utf8_lossy(&body);
    let config = match parse_object_lock_xml(&xml) {
        Ok(c) => c,
        Err(e) => {
            error!("Failed to parse Object Lock XML: {:?}", e);
            return S3Error::MalformedXML.into_response();
        }
    };

    if let Err(e) = config.validate() {
        error!("Invalid Object Lock configuration: {:?}", e);
        return S3Error::InvalidRequest(e.to_string()).into_response();
    }

    // Check if Object Lock already configured
    // AWS Constraint: Once enabled, cannot be disabled
    if let Some(existing_config) = get_object_lock_config_internal(&*storage, &bucket).await {
        if existing_config.object_lock_enabled && !config.object_lock_enabled {
            warn!(
                "Attempt to disable Object Lock on bucket {} (not allowed)",
                bucket
            );
            return S3Error::InvalidRequest(
                "Object Lock cannot be disabled once enabled".to_string(),
            )
            .into_response();
        }
    }

    // Store configuration in __system__ bucket
    let lock_key = format!("{}{}", OBJECT_LOCK_CONFIG_PREFIX, bucket);
    let xml_output = object_lock_to_xml(&config);

    match storage
        .put_object(
            "__system__",
            &lock_key,
            xml_output.as_bytes(),
            "application/xml",
            &HashMap::new(),
        )
        .await
    {
        Ok(_) => {
            info!("Object Lock configuration saved for bucket: {}", bucket);
            StatusCode::OK.into_response()
        }
        Err(e) => {
            error!("Failed to save Object Lock configuration: {:?}", e);
            S3Error::InternalError(format!("Failed to save Object Lock configuration: {}", e))
                .into_response()
        }
    }
}

// ============================================================================
// Object Retention Handlers
// ============================================================================

/// Query parameters for object version operations.
#[derive(serde::Deserialize)]
pub struct ObjectVersionQuery {
    /// Version ID from query parameter (?versionId=...)
    #[serde(rename = "versionId")]
    pub version_id: Option<String>,
}

/// Gets the retention configuration for a specific object version.
///
/// S3 API: GET /{bucket}/{key}?retention&versionId=...
///
/// # Returns
///
/// - 200 OK with XML retention configuration
/// - 404 NoSuchObjectLockConfiguration if retention not set
pub async fn get_object_retention(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
    Query(query): Query<ObjectVersionQuery>,
) -> impl IntoResponse {
    debug!(
        "GetObjectRetention: {}/{} version={:?}",
        bucket, key, query.version_id
    );

    let storage = state.storage.read().await;

    // Version ID is required
    let version_id = match query.version_id {
        Some(v) => v,
        None => {
            return S3Error::InvalidRequest("versionId is required for retention".to_string())
                .into_response()
        }
    };

    // Get object metadata
    let record = match storage.head_object_version(&bucket, &key, &version_id).await {
        Ok(r) => r,
        Err(_) => return S3Error::NoSuchKey.into_response(),
    };

    // Check if retention is set
    match (record.retention_mode, record.retain_until_timestamp) {
        (Some(mode), Some(retain_until)) => {
            // Convert timestamp to ISO 8601
            let retain_until_date = timestamp_to_iso8601(retain_until);

            let retention = s4_features::object_lock::ObjectRetention {
                mode,
                retain_until_date,
            };

            let xml = retention_to_xml(&retention);
            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/xml")
                .body(Body::from(xml))
                .unwrap()
        }
        _ => {
            // No retention set
            S3Error::NoSuchObjectLockConfiguration.into_response()
        }
    }
}

/// Sets the retention configuration for a specific object version.
///
/// S3 API: PUT /{bucket}/{key}?retention&versionId=...
///
/// # Returns
///
/// - 200 OK if retention set successfully
/// - 400 Bad Request if invalid retention or COMPLIANCE retention modification attempted
///
/// # Safety (Phase 3)
///
/// - BLOCKS ALL changes to COMPLIANCE retention
/// - Phase 5 will allow extension with bypass permission
pub async fn put_object_retention(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
    Query(query): Query<ObjectVersionQuery>,
    body: Bytes,
) -> impl IntoResponse {
    info!(
        "PutObjectRetention: {}/{} version={:?}",
        bucket, key, query.version_id
    );

    let storage = state.storage.read().await;

    // Validate Object Lock is enabled on bucket
    let _lock_config = match get_object_lock_config_internal(&*storage, &bucket).await {
        Some(c) if c.object_lock_enabled => c,
        _ => {
            return S3Error::InvalidRequest("Object Lock not enabled on bucket".to_string())
                .into_response()
        }
    };

    // Version ID is required
    let version_id = match query.version_id {
        Some(v) => v,
        None => {
            return S3Error::InvalidRequest("versionId is required for retention".to_string())
                .into_response()
        }
    };

    // Parse retention XML
    let xml = String::from_utf8_lossy(&body);
    let retention = match parse_retention_xml(&xml) {
        Ok(r) => r,
        Err(e) => {
            error!("Failed to parse retention XML: {:?}", e);
            return S3Error::MalformedXML.into_response();
        }
    };

    // Validate retain-until-date is in future
    let retain_until = match iso8601_to_timestamp(&retention.retain_until_date) {
        Ok(ts) => ts,
        Err(e) => {
            error!("Invalid retain-until-date: {:?}", e);
            return S3Error::InvalidRequest(format!("Invalid date format: {}", e)).into_response();
        }
    };

    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64;
    if retain_until <= now {
        return S3Error::InvalidRequest("Retain until date must be in future".to_string())
            .into_response();
    }

    // Get existing object version
    let mut record = match storage.head_object_version(&bucket, &key, &version_id).await {
        Ok(r) => r,
        Err(_) => return S3Error::NoSuchKey.into_response(),
    };

    // CRITICAL SAFETY CHECK (Phase 3):
    // Block ALL changes to COMPLIANCE mode retention
    if let Some(existing_mode) = record.retention_mode {
        if existing_mode == RetentionMode::COMPLIANCE {
            warn!(
                "Attempt to modify COMPLIANCE retention on {}/{} version {}",
                bucket, key, version_id
            );
            return S3Error::InvalidRequest(
                "Cannot modify COMPLIANCE mode retention in Phase 3".to_string(),
            )
            .into_response();
        }
    }

    // Update retention
    record.retention_mode = Some(retention.mode);
    record.retain_until_timestamp = Some(retain_until);

    // Update metadata in storage
    match storage.update_object_metadata(&bucket, &key, &version_id, record).await {
        Ok(_) => {
            info!(
                "Retention set for {}/{} version {}: mode={:?}, until={}",
                bucket, key, version_id, retention.mode, retention.retain_until_date
            );
            StatusCode::OK.into_response()
        }
        Err(e) => {
            error!("Failed to update object retention: {:?}", e);
            S3Error::InternalError(format!("Failed to update object retention: {}", e))
                .into_response()
        }
    }
}

// ============================================================================
// Legal Hold Handlers
// ============================================================================

/// Gets the legal hold status for a specific object version.
///
/// S3 API: GET /{bucket}/{key}?legal-hold&versionId=...
///
/// # Returns
///
/// - 200 OK with XML legal hold status
pub async fn get_object_legal_hold(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
    Query(query): Query<ObjectVersionQuery>,
) -> impl IntoResponse {
    debug!(
        "GetObjectLegalHold: {}/{} version={:?}",
        bucket, key, query.version_id
    );

    let storage = state.storage.read().await;

    // Version ID is required
    let version_id = match query.version_id {
        Some(v) => v,
        None => {
            return S3Error::InvalidRequest("versionId is required for legal hold".to_string())
                .into_response()
        }
    };

    // Get object metadata
    let record = match storage.head_object_version(&bucket, &key, &version_id).await {
        Ok(r) => r,
        Err(_) => return S3Error::NoSuchKey.into_response(),
    };

    // Return legal hold status
    let status = LegalHoldStatus::from_bool(record.legal_hold);
    let hold = s4_features::object_lock::LegalHold { status };

    let xml = legal_hold_to_xml(&hold);
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/xml")
        .body(Body::from(xml))
        .unwrap()
}

/// Sets the legal hold status for a specific object version.
///
/// S3 API: PUT /{bucket}/{key}?legal-hold&versionId=...
///
/// # Returns
///
/// - 200 OK if legal hold set successfully
pub async fn put_object_legal_hold(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
    Query(query): Query<ObjectVersionQuery>,
    body: Bytes,
) -> impl IntoResponse {
    info!(
        "PutObjectLegalHold: {}/{} version={:?}",
        bucket, key, query.version_id
    );

    let storage = state.storage.read().await;

    // Validate Object Lock is enabled on bucket
    match get_object_lock_config_internal(&*storage, &bucket).await {
        Some(c) if c.object_lock_enabled => {}
        _ => {
            return S3Error::InvalidRequest("Object Lock not enabled on bucket".to_string())
                .into_response()
        }
    }

    // Version ID is required
    let version_id = match query.version_id {
        Some(v) => v,
        None => {
            return S3Error::InvalidRequest("versionId is required for legal hold".to_string())
                .into_response()
        }
    };

    // Parse legal hold XML
    let xml = String::from_utf8_lossy(&body);
    let hold = match parse_legal_hold_xml(&xml) {
        Ok(h) => h,
        Err(e) => {
            error!("Failed to parse legal hold XML: {:?}", e);
            return S3Error::MalformedXML.into_response();
        }
    };

    // Get existing object version
    let mut record = match storage.head_object_version(&bucket, &key, &version_id).await {
        Ok(r) => r,
        Err(_) => return S3Error::NoSuchKey.into_response(),
    };

    // Update legal hold
    record.legal_hold = hold.status.to_bool();

    // Update metadata in storage
    match storage.update_object_metadata(&bucket, &key, &version_id, record).await {
        Ok(_) => {
            info!(
                "Legal hold set for {}/{} version {}: status={:?}",
                bucket, key, version_id, hold.status
            );
            StatusCode::OK.into_response()
        }
        Err(e) => {
            error!("Failed to update legal hold: {:?}", e);
            S3Error::InternalError(format!("Failed to update legal hold: {}", e)).into_response()
        }
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Gets the Object Lock configuration for a bucket (internal helper).
///
/// Returns `None` if not configured.
pub async fn get_object_lock_config_internal(
    storage: &dyn StorageEngine,
    bucket: &str,
) -> Option<ObjectLockConfiguration> {
    let lock_key = format!("{}{}", OBJECT_LOCK_CONFIG_PREFIX, bucket);
    match storage.get_object("__system__", &lock_key).await {
        Ok((data, _)) => {
            let xml = String::from_utf8_lossy(&data);
            parse_object_lock_xml(&xml).ok()
        }
        Err(_) => None,
    }
}

/// Gets the versioning status for a bucket (internal helper).
///
/// Copied from bucket_config.rs to avoid circular dependency.
async fn get_bucket_versioning_status_internal(
    storage: &dyn StorageEngine,
    bucket: &str,
) -> VersioningStatus {
    const VERSIONING_CONFIG_PREFIX: &str = "__s4_bucket_versioning_";
    let versioning_key = format!("{}{}", VERSIONING_CONFIG_PREFIX, bucket);

    match storage.get_object("__system__", &versioning_key).await {
        Ok((data, _)) => {
            let status_str = String::from_utf8_lossy(&data);
            match status_str.trim() {
                "Enabled" => VersioningStatus::Enabled,
                "Suspended" => VersioningStatus::Suspended,
                _ => VersioningStatus::Unversioned,
            }
        }
        Err(_) => VersioningStatus::Unversioned,
    }
}

/// Converts Unix timestamp (nanoseconds) to ISO 8601 string.
fn timestamp_to_iso8601(timestamp_nanos: u64) -> String {
    let timestamp_secs = (timestamp_nanos / 1_000_000_000) as i64;
    let datetime =
        chrono::DateTime::from_timestamp(timestamp_secs, 0).unwrap_or_else(chrono::Utc::now);
    datetime.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

/// Converts ISO 8601 string to Unix timestamp (nanoseconds).
fn iso8601_to_timestamp(date_str: &str) -> Result<u64, String> {
    use chrono::DateTime;

    let datetime = DateTime::parse_from_rfc3339(date_str)
        .map_err(|e| format!("Invalid ISO 8601 date: {}", e))?;

    let timestamp_secs = datetime.timestamp();
    if timestamp_secs < 0 {
        return Err("Date must be in the future".to_string());
    }

    Ok(timestamp_secs as u64 * 1_000_000_000)
}
