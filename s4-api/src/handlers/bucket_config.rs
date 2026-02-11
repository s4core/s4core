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
//! - Versioning (GET/PUT /{bucket}?versioning)
//! - Lifecycle (GET/PUT/DELETE /{bucket}?lifecycle)
//! - Location (GET /{bucket}?location)

use axum::{
    body::{Body, Bytes},
    extract::{Path, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
};
use s4_core::{StorageEngine, VersioningStatus};
use s4_features::cors::{cors_to_xml, parse_cors_xml, CORSConfiguration};
use s4_features::lifecycle::{lifecycle_to_xml, parse_lifecycle_xml, LifecycleConfiguration};
use std::collections::HashMap;
use tracing::{debug, error, info, warn};

use crate::s3::errors::S3Error;
use crate::server::AppState;

// Storage key prefixes for bucket configurations
const CORS_CONFIG_PREFIX: &str = "__s4_bucket_cors_";
const VERSIONING_CONFIG_PREFIX: &str = "__s4_bucket_versioning_";
const LIFECYCLE_CONFIG_PREFIX: &str = "__s4_bucket_lifecycle_";

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

    let storage = state.storage.read().await;

    // Check if bucket exists
    let bucket_marker_key = format!("__s4_bucket_marker_{}", bucket);
    if storage.head_object("__system__", &bucket_marker_key).await.is_err() {
        return S3Error::NoSuchBucket.into_response();
    }

    // Get CORS configuration
    let cors_key = format!("{}{}", CORS_CONFIG_PREFIX, bucket);
    match storage.get_object("__system__", &cors_key).await {
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
            // No CORS configuration
            S3Error::NoSuchCORSConfiguration.into_response()
        }
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
pub async fn put_bucket_cors(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
    body: Bytes,
) -> impl IntoResponse {
    info!("PutBucketCors: {}", bucket);

    let storage = state.storage.read().await;

    // Check if bucket exists
    let bucket_marker_key = format!("__s4_bucket_marker_{}", bucket);
    if storage.head_object("__system__", &bucket_marker_key).await.is_err() {
        return S3Error::NoSuchBucket.into_response();
    }

    // Parse and validate CORS configuration
    let xml = String::from_utf8_lossy(&body);
    let config = match parse_cors_xml(&xml) {
        Ok(c) => c,
        Err(e) => {
            error!("Failed to parse CORS XML: {:?}", e);
            return S3Error::MalformedXML.into_response();
        }
    };

    if let Err(e) = config.validate() {
        error!("Invalid CORS configuration: {:?}", e);
        return S3Error::InvalidRequest(e.to_string()).into_response();
    }

    // Store CORS configuration
    let cors_key = format!("{}{}", CORS_CONFIG_PREFIX, bucket);
    let cors_xml = cors_to_xml(&config);
    if let Err(e) = storage
        .put_object(
            "__system__",
            &cors_key,
            cors_xml.as_bytes(),
            "application/xml",
            &HashMap::new(),
        )
        .await
    {
        error!("Failed to store CORS configuration: {:?}", e);
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to store CORS configuration",
        )
            .into_response();
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

    let storage = state.storage.read().await;

    // Check if bucket exists
    let bucket_marker_key = format!("__s4_bucket_marker_{}", bucket);
    if storage.head_object("__system__", &bucket_marker_key).await.is_err() {
        return S3Error::NoSuchBucket.into_response();
    }

    // Delete CORS configuration
    let cors_key = format!("{}{}", CORS_CONFIG_PREFIX, bucket);
    let _ = storage.delete_object("__system__", &cors_key).await;

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
pub async fn get_bucket_versioning(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
) -> impl IntoResponse {
    debug!("GetBucketVersioning: {}", bucket);

    let storage = state.storage.read().await;

    // Check if bucket exists
    let bucket_marker_key = format!("__s4_bucket_marker_{}", bucket);
    if storage.head_object("__system__", &bucket_marker_key).await.is_err() {
        return S3Error::NoSuchBucket.into_response();
    }

    let status = get_versioning_status_internal(&*storage, &bucket).await;
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
pub async fn put_bucket_versioning(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
    body: Bytes,
) -> impl IntoResponse {
    info!("PutBucketVersioning: {}", bucket);

    let storage = state.storage.read().await;

    // Check if bucket exists
    let bucket_marker_key = format!("__s4_bucket_marker_{}", bucket);
    if storage.head_object("__system__", &bucket_marker_key).await.is_err() {
        return S3Error::NoSuchBucket.into_response();
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
                    "__system__",
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
    match storage.get_object("__system__", &versioning_key).await {
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

    let storage = state.storage.read().await;

    // Check if bucket exists
    let bucket_marker_key = format!("__s4_bucket_marker_{}", bucket);
    if storage.head_object("__system__", &bucket_marker_key).await.is_err() {
        return S3Error::NoSuchBucket.into_response();
    }

    // Get lifecycle configuration
    let lifecycle_key = format!("{}{}", LIFECYCLE_CONFIG_PREFIX, bucket);
    match storage.get_object("__system__", &lifecycle_key).await {
        Ok((data, _)) => {
            let xml = String::from_utf8_lossy(&data).to_string();
            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/xml")
                .body(Body::from(xml))
                .unwrap()
        }
        Err(_) => S3Error::NoSuchLifecycleConfiguration.into_response(),
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
pub async fn put_bucket_lifecycle(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
    body: Bytes,
) -> impl IntoResponse {
    info!("PutBucketLifecycle: {}", bucket);

    let storage = state.storage.read().await;

    // Check if bucket exists
    let bucket_marker_key = format!("__s4_bucket_marker_{}", bucket);
    if storage.head_object("__system__", &bucket_marker_key).await.is_err() {
        return S3Error::NoSuchBucket.into_response();
    }

    // Parse and validate lifecycle configuration
    let xml = String::from_utf8_lossy(&body);
    let config = match parse_lifecycle_xml(&xml) {
        Ok(c) => c,
        Err(e) => {
            error!("Failed to parse lifecycle XML: {:?}", e);
            return S3Error::MalformedXML.into_response();
        }
    };

    if let Err(e) = config.validate() {
        error!("Invalid lifecycle configuration: {:?}", e);
        return S3Error::InvalidRequest(e.to_string()).into_response();
    }

    // Store validated lifecycle configuration
    let lifecycle_key = format!("{}{}", LIFECYCLE_CONFIG_PREFIX, bucket);
    let validated_xml = lifecycle_to_xml(&config);
    if let Err(e) = storage
        .put_object(
            "__system__",
            &lifecycle_key,
            validated_xml.as_bytes(),
            "application/xml",
            &HashMap::new(),
        )
        .await
    {
        error!("Failed to store lifecycle configuration: {:?}", e);
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to store lifecycle configuration",
        )
            .into_response();
    }

    info!(
        "Lifecycle configuration saved for bucket: {} ({} rules)",
        bucket,
        config.rules.len()
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

    let storage = state.storage.read().await;

    // Check if bucket exists
    let bucket_marker_key = format!("__s4_bucket_marker_{}", bucket);
    if storage.head_object("__system__", &bucket_marker_key).await.is_err() {
        return S3Error::NoSuchBucket.into_response();
    }

    // Delete lifecycle configuration
    let lifecycle_key = format!("{}{}", LIFECYCLE_CONFIG_PREFIX, bucket);
    let _ = storage.delete_object("__system__", &lifecycle_key).await;

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

/// Gets the CORS configuration for a bucket (if any).
pub async fn get_cors_config(state: &AppState, bucket: &str) -> Option<CORSConfiguration> {
    let storage = state.storage.read().await;
    let cors_key = format!("{}{}", CORS_CONFIG_PREFIX, bucket);

    match storage.get_object("__system__", &cors_key).await {
        Ok((data, _)) => {
            let xml = String::from_utf8_lossy(&data);
            parse_cors_xml(&xml).ok()
        }
        Err(_) => None,
    }
}

/// Gets the lifecycle configuration for a bucket (if any).
pub async fn get_lifecycle_config(
    state: &AppState,
    bucket: &str,
) -> Option<LifecycleConfiguration> {
    let storage = state.storage.read().await;
    let lifecycle_key = format!("{}{}", LIFECYCLE_CONFIG_PREFIX, bucket);

    match storage.get_object("__system__", &lifecycle_key).await {
        Ok((data, _)) => {
            let xml = String::from_utf8_lossy(&data);
            parse_lifecycle_xml(&xml).ok()
        }
        Err(_) => None,
    }
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

    let storage = state.storage.read().await;

    // Check if bucket exists
    let bucket_marker_key = format!("__s4_bucket_marker_{}", bucket);
    if storage.head_object("__system__", &bucket_marker_key).await.is_err() {
        return S3Error::NoSuchBucket.into_response();
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
