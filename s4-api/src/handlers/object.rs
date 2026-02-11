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

//! Object operation handlers.
//!
//! Implements S3-compatible object operations:
//! - PutObject (PUT /{bucket}/{key})
//! - GetObject (GET /{bucket}/{key})
//! - DeleteObject (DELETE /{bucket}/{key})
//! - HeadObject (HEAD /{bucket}/{key})
//!
//! All operations support S3-compatible versioning when enabled on the bucket.

use axum::{
    body::{Body, Bytes},
    extract::{Path, Query, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use s4_core::StorageEngine;
use serde::Deserialize;
use std::collections::HashMap;
use tracing::{debug, error, info};

use crate::handlers::{get_bucket_versioning_status, object_lock::get_object_lock_config_internal};
use crate::middleware::{decode_aws_chunked, is_aws_chunked, validate_decoded_content_length};
use crate::s3::errors::S3Error;
use crate::server::AppState;

/// Query parameters for object operations.
#[derive(Debug, Deserialize, Default)]
pub struct ObjectVersionQuery {
    /// Version ID for version-specific operations.
    #[serde(rename = "versionId")]
    pub version_id: Option<String>,
}

/// Uploads an object to a bucket.
///
/// S3 API: PUT /{bucket}/{key}
///
/// # Headers
///
/// - `Content-Type`: MIME type of the object (default: application/octet-stream)
/// - `x-amz-meta-*`: Custom metadata headers
///
/// # Response Headers
///
/// - `ETag`: Content hash of the stored object
/// - `x-amz-version-id`: Version ID (only when versioning is enabled/suspended)
///
/// # Returns
///
/// - 200 OK with ETag header on success
/// - 404 Not Found if bucket doesn't exist
pub async fn put_object(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    // Check for AWS chunked encoding and decode if necessary
    let body = if is_aws_chunked(&headers) {
        debug!(
            "PutObject: detected aws-chunked encoding, decoding body (raw size={})",
            body.len()
        );
        let decoded = match decode_aws_chunked(&body) {
            Ok(d) => d,
            Err(e) => return e.into_response(),
        };
        debug!(
            "PutObject: decoded aws-chunked body (decoded size={})",
            decoded.len()
        );
        if let Err(e) = validate_decoded_content_length(&headers, &decoded) {
            return e.into_response();
        }
        Bytes::from(decoded)
    } else {
        body
    };

    info!(
        "PutObject: bucket={}, key={}, size={}",
        bucket,
        key,
        body.len()
    );

    // Get versioning status before acquiring storage lock
    let versioning_status = get_bucket_versioning_status(&state, &bucket).await;

    let storage = state.storage.read().await;

    // Check if bucket exists
    let bucket_marker_key = format!("__s4_bucket_marker_{}", bucket);
    if storage.head_object("__system__", &bucket_marker_key).await.is_err() {
        return S3Error::NoSuchBucket.into_response();
    }

    // Extract content type
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/octet-stream");

    // Extract custom metadata (x-amz-meta-* headers)
    let mut metadata = HashMap::new();
    for (name, value) in headers.iter() {
        let name_str = name.as_str().to_lowercase();
        if name_str.starts_with("x-amz-meta-") {
            if let Ok(v) = value.to_str() {
                let meta_key = name_str.strip_prefix("x-amz-meta-").unwrap().to_string();
                metadata.insert(meta_key, v.to_string());
            }
        }
    }

    // Get bucket Object Lock configuration to check for default retention
    let default_retention =
        if let Some(lock_config) = get_object_lock_config_internal(&*storage, &bucket).await {
            if lock_config.object_lock_enabled {
                lock_config.default_retention
            } else {
                None
            }
        } else {
            None
        };

    // Store the object with versioning and optional default retention
    match storage
        .put_object_with_retention(
            &bucket,
            &key,
            &body,
            content_type,
            &metadata,
            versioning_status,
            default_retention,
        )
        .await
    {
        Ok((etag, version_id)) => {
            info!(
                "Object stored: bucket={}, key={}, etag={}, version_id={:?}",
                bucket, key, etag, version_id
            );

            let mut builder = Response::builder()
                .status(StatusCode::OK)
                .header("ETag", format!("\"{}\"", etag));

            // Add version ID header if versioning is active
            if let Some(vid) = version_id {
                builder = builder.header("x-amz-version-id", vid);
            }

            builder.body(Body::empty()).unwrap()
        }
        Err(e) => {
            error!("Failed to store object: {:?}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to store object: {}", e),
            )
                .into_response()
        }
    }
}

/// Downloads an object from a bucket.
///
/// S3 API: GET /{bucket}/{key}[?versionId=...]
///
/// # Query Parameters
///
/// - `versionId`: Optional version ID to retrieve a specific version
///
/// # Response Headers
///
/// - `x-amz-version-id`: Version ID of the returned object (when versioning is active)
/// - `x-amz-delete-marker`: "true" if requesting a delete marker (returns 404)
///
/// # Returns
///
/// - 200 OK with object data and headers
/// - 404 Not Found if object or version doesn't exist
pub async fn get_object(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
    Query(query): Query<ObjectVersionQuery>,
    headers: HeaderMap,
) -> impl IntoResponse {
    debug!(
        "GetObject: bucket={}, key={}, version_id={:?}",
        bucket, key, query.version_id
    );

    let storage = state.storage.read().await;

    // Check if bucket exists
    let bucket_marker_key = format!("__s4_bucket_marker_{}", bucket);
    if storage.head_object("__system__", &bucket_marker_key).await.is_err() {
        return S3Error::NoSuchBucket.into_response();
    }

    // Get the object (with or without specific version)
    let result = if let Some(ref version_id) = query.version_id {
        storage.get_object_version(&bucket, &key, version_id).await
    } else {
        storage.get_object(&bucket, &key).await
    };

    match result {
        Ok((data, record)) => {
            // Parse Range header if present
            let has_range_header = headers.get(header::RANGE).is_some();
            let range = headers
                .get(header::RANGE)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| parse_range_header(v, data.len()));

            let (status, response_data, content_range) = if has_range_header {
                match range {
                    Some((start, end)) => {
                        let slice = data[start..=end].to_vec();
                        let cr = format!("bytes {}-{}/{}", start, end, data.len());
                        (StatusCode::PARTIAL_CONTENT, slice, Some(cr))
                    }
                    None => {
                        // RFC 7233: unsatisfiable range → 416
                        let cr = format!("bytes */{}", data.len());
                        return Response::builder()
                            .status(StatusCode::RANGE_NOT_SATISFIABLE)
                            .header(header::CONTENT_RANGE, cr)
                            .body(Body::empty())
                            .unwrap();
                    }
                }
            } else {
                (StatusCode::OK, data, None)
            };

            let mut builder = Response::builder()
                .status(status)
                .header(header::CONTENT_LENGTH, response_data.len())
                .header(header::CONTENT_TYPE, &record.content_type)
                .header("ETag", format!("\"{}\"", record.etag))
                .header(header::ACCEPT_RANGES, "bytes")
                .header(
                    header::LAST_MODIFIED,
                    format_last_modified(record.modified_at),
                );

            if let Some(cr) = content_range {
                builder = builder.header(header::CONTENT_RANGE, cr);
            }

            // Add version ID header if present
            if let Some(ref vid) = record.version_id {
                builder = builder.header("x-amz-version-id", vid.clone());
            }

            // Add custom metadata headers
            for (key, value) in &record.metadata {
                if !key.starts_with('_') {
                    // Skip internal metadata
                    builder = builder.header(format!("x-amz-meta-{}", key), value);
                }
            }

            builder.body(Body::from(response_data)).unwrap()
        }
        Err(s4_core::StorageError::ObjectNotFound { .. }) => S3Error::NoSuchKey.into_response(),
        Err(s4_core::StorageError::VersionNotFound { version_id, .. }) => {
            // Return NoSuchVersion with the version ID
            debug!("Version not found: {}", version_id);
            S3Error::NoSuchVersion.into_response()
        }
        Err(s4_core::StorageError::DeleteMarker { version_id, .. }) => {
            // S3 returns 404 with x-amz-delete-marker header for delete markers
            Response::builder()
                .status(StatusCode::NOT_FOUND)
                .header("x-amz-delete-marker", "true")
                .header("x-amz-version-id", version_id)
                .body(Body::empty())
                .unwrap()
        }
        Err(e) => {
            error!("Failed to get object: {:?}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to get object: {}", e),
            )
                .into_response()
        }
    }
}

/// Deletes an object from a bucket.
///
/// S3 API: DELETE /{bucket}/{key}[?versionId=...]
///
/// # Query Parameters
///
/// - `versionId`: Optional version ID to permanently delete a specific version
///
/// # Versioning Behavior
///
/// - **Unversioned bucket**: Permanently deletes the object
/// - **Versioned bucket (no versionId)**: Creates a delete marker, object appears deleted
/// - **Versioned bucket (with versionId)**: Permanently deletes that specific version
///
/// # Response Headers
///
/// - `x-amz-version-id`: Version ID of deleted version or created delete marker
/// - `x-amz-delete-marker`: "true" if a delete marker was created
///
/// # Returns
///
/// - 204 No Content on success
/// - 404 Not Found if bucket doesn't exist
pub async fn delete_object(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
    Query(query): Query<ObjectVersionQuery>,
) -> impl IntoResponse {
    info!(
        "DeleteObject: bucket={}, key={}, version_id={:?}",
        bucket, key, query.version_id
    );

    // Get versioning status before acquiring storage lock
    let versioning_status = get_bucket_versioning_status(&state, &bucket).await;

    let storage = state.storage.read().await;

    // Check if bucket exists
    let bucket_marker_key = format!("__s4_bucket_marker_{}", bucket);
    if storage.head_object("__system__", &bucket_marker_key).await.is_err() {
        return S3Error::NoSuchBucket.into_response();
    }

    // CRITICAL: Check Object Lock before deletion
    // This prevents COMPLIANCE mode from creating permanently undeletable files
    if let Some(version_id) = &query.version_id {
        // Permanent deletion of a specific version - check that version's locks
        match storage.head_object_version(&bucket, &key, version_id).await {
            Ok(record) => {
                // Check legal hold
                if record.legal_hold {
                    error!(
                        "Cannot delete {}/{} version {}: legal hold is ON",
                        bucket, key, version_id
                    );
                    return S3Error::AccessDenied.into_response();
                }

                // Check retention period
                if let (Some(_mode), Some(retain_until)) =
                    (record.retention_mode, record.retain_until_timestamp)
                {
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_nanos() as u64;

                    if now < retain_until {
                        error!(
                            "Cannot delete {}/{} version {}: retention until {}",
                            bucket, key, version_id, retain_until
                        );
                        return S3Error::AccessDenied.into_response();
                    }
                }
            }
            Err(s4_core::StorageError::VersionNotFound { .. }) => {
                // Version doesn't exist - let delete_object_versioned handle it
            }
            Err(s4_core::StorageError::DeleteMarker { .. }) => {
                // Delete marker can be deleted without lock check
                // (delete markers don't have retention/legal hold)
            }
            Err(e) => {
                error!("Failed to check object locks: {:?}", e);
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Failed to check object locks: {}", e),
                )
                    .into_response();
            }
        }
    } else {
        // Creating a delete marker - check current version's locks
        // If versioning is disabled/suspended, this will permanently delete the object
        match storage.head_object(&bucket, &key).await {
            Ok(record) => {
                // Check legal hold
                if record.legal_hold {
                    error!("Cannot delete {}/{}: legal hold is ON", bucket, key);
                    return S3Error::AccessDenied.into_response();
                }

                // Check retention period
                if let (Some(_mode), Some(retain_until)) =
                    (record.retention_mode, record.retain_until_timestamp)
                {
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_nanos() as u64;

                    if now < retain_until {
                        error!(
                            "Cannot delete {}/{}: retention until {}",
                            bucket, key, retain_until
                        );
                        return S3Error::AccessDenied.into_response();
                    }
                }
            }
            Err(s4_core::StorageError::ObjectNotFound { .. }) => {
                // Object doesn't exist - let delete_object_versioned handle it
            }
            Err(e) => {
                error!("Failed to check object locks: {:?}", e);
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Failed to check object locks: {}", e),
                )
                    .into_response();
            }
        }
    }

    // Delete the object with versioning support
    match storage
        .delete_object_versioned(
            &bucket,
            &key,
            query.version_id.as_deref(),
            versioning_status,
        )
        .await
    {
        Ok(result) => {
            info!(
                "Object deleted: bucket={}, key={}, delete_marker={}, version_id={:?}",
                bucket, key, result.delete_marker, result.version_id
            );

            let mut builder = Response::builder().status(StatusCode::NO_CONTENT);

            // Add delete marker header if one was created
            if result.delete_marker {
                builder = builder.header("x-amz-delete-marker", "true");
            }

            // Add version ID header
            if let Some(vid) = result.version_id {
                builder = builder.header("x-amz-version-id", vid);
            }

            builder.body(Body::empty()).unwrap()
        }
        Err(s4_core::StorageError::VersionNotFound { version_id, .. }) => {
            debug!("Version not found for delete: {}", version_id);
            S3Error::NoSuchVersion.into_response()
        }
        Err(e) => {
            error!("Failed to delete object: {:?}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to delete object: {}", e),
            )
                .into_response()
        }
    }
}

/// Gets object metadata without downloading the object.
///
/// S3 API: HEAD /{bucket}/{key}[?versionId=...]
///
/// # Query Parameters
///
/// - `versionId`: Optional version ID to get metadata for a specific version
///
/// # Response Headers
///
/// - `x-amz-version-id`: Version ID (when versioning is active)
/// - `x-amz-delete-marker`: "true" if the version is a delete marker (returns 404)
///
/// # Returns
///
/// - 200 OK with headers but no body
/// - 404 Not Found if object or version doesn't exist
pub async fn head_object(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
    Query(query): Query<ObjectVersionQuery>,
) -> impl IntoResponse {
    debug!(
        "HeadObject: bucket={}, key={}, version_id={:?}",
        bucket, key, query.version_id
    );

    let storage = state.storage.read().await;

    // Check if bucket exists
    let bucket_marker_key = format!("__s4_bucket_marker_{}", bucket);
    if storage.head_object("__system__", &bucket_marker_key).await.is_err() {
        return S3Error::NoSuchBucket.into_response();
    }

    // Get object metadata (with or without specific version)
    let result = if let Some(ref version_id) = query.version_id {
        storage.head_object_version(&bucket, &key, version_id).await
    } else {
        storage.head_object(&bucket, &key).await
    };

    match result {
        Ok(record) => {
            // Check if this is a delete marker
            if record.is_delete_marker {
                return Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .header("x-amz-delete-marker", "true")
                    .header(
                        "x-amz-version-id",
                        record.version_id.clone().unwrap_or_default(),
                    )
                    .body(Body::empty())
                    .unwrap();
            }

            let mut builder = Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_LENGTH, record.size)
                .header(header::CONTENT_TYPE, &record.content_type)
                .header("ETag", format!("\"{}\"", record.etag))
                .header(
                    header::LAST_MODIFIED,
                    format_last_modified(record.modified_at),
                );

            // Add version ID header if present
            if let Some(ref vid) = record.version_id {
                builder = builder.header("x-amz-version-id", vid.clone());
            }

            // Add custom metadata headers
            for (key, value) in &record.metadata {
                if !key.starts_with('_') {
                    builder = builder.header(format!("x-amz-meta-{}", key), value);
                }
            }

            builder.body(Body::empty()).unwrap()
        }
        Err(s4_core::StorageError::ObjectNotFound { .. }) => S3Error::NoSuchKey.into_response(),
        Err(s4_core::StorageError::VersionNotFound { version_id, .. }) => {
            debug!("Version not found: {}", version_id);
            S3Error::NoSuchVersion.into_response()
        }
        Err(e) => {
            error!("Failed to get object metadata: {:?}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to get object metadata: {}", e),
            )
                .into_response()
        }
    }
}

/// Parses an HTTP Range header value like "bytes=0-499" or "bytes=-500" or "bytes=500-".
///
/// Returns `Some((start, end))` where both are inclusive byte positions, or `None` if invalid.
fn parse_range_header(value: &str, total_len: usize) -> Option<(usize, usize)> {
    let value = value.strip_prefix("bytes=")?;
    let (start_str, end_str) = value.split_once('-')?;

    if total_len == 0 {
        return None;
    }

    if start_str.is_empty() {
        // Suffix range: bytes=-N (last N bytes)
        let suffix_len: usize = end_str.parse().ok()?;
        if suffix_len == 0 || suffix_len > total_len {
            return None;
        }
        Some((total_len - suffix_len, total_len - 1))
    } else {
        let start: usize = start_str.parse().ok()?;
        if start >= total_len {
            return None;
        }
        let end = if end_str.is_empty() {
            total_len - 1
        } else {
            let end: usize = end_str.parse().ok()?;
            end.min(total_len - 1)
        };
        if start > end {
            return None;
        }
        Some((start, end))
    }
}

/// Formats a timestamp as HTTP date format.
///
/// Handles invalid timestamps gracefully by falling back to Unix epoch.
fn format_last_modified(timestamp_nanos: u64) -> String {
    use chrono::{LocalResult, TimeZone, Utc};
    let secs = (timestamp_nanos / 1_000_000_000) as i64;
    let dt = match Utc.timestamp_opt(secs, 0) {
        LocalResult::Single(dt) => dt,
        // Fallback to Unix epoch for invalid/ambiguous timestamps
        _ => Utc.timestamp_opt(0, 0).single().unwrap_or_else(Utc::now),
    };
    dt.format("%a, %d %b %Y %H:%M:%S GMT").to_string()
}

/// Gets the tag set for an object.
///
/// S3 API: GET /{bucket}/{key}?tagging
///
/// # Returns
///
/// - 200 OK with XML tagging response (currently returns empty TagSet)
/// - 404 NoSuchBucket if bucket doesn't exist
/// - 404 NoSuchKey if object doesn't exist
///
/// # Notes
///
/// This is a minimal implementation that always returns an empty TagSet.
/// Full tagging support (storing/retrieving tags) is not yet implemented.
pub async fn get_object_tagging(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
    Query(version_query): Query<ObjectVersionQuery>,
) -> impl IntoResponse {
    debug!(
        "GetObjectTagging: bucket={}, key={}, version_id={:?}",
        bucket, key, version_query.version_id
    );

    let storage = state.storage.read().await;

    // Check if bucket exists
    let bucket_marker_key = format!("__s4_bucket_marker_{}", bucket);
    if storage.head_object("__system__", &bucket_marker_key).await.is_err() {
        return S3Error::NoSuchBucket.into_response();
    }

    // Check if object exists
    let head_result = if let Some(ref version_id) = version_query.version_id {
        storage.head_object_version(&bucket, &key, version_id).await
    } else {
        storage.head_object(&bucket, &key).await
    };

    if head_result.is_err() {
        return S3Error::NoSuchKey.into_response();
    }

    // Return empty TagSet (tagging not yet implemented)
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<Tagging>
    <TagSet/>
</Tagging>"#;

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/xml")
        .body(Body::from(xml))
        .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_last_modified() {
        // Test with known timestamp
        let timestamp = 1_704_067_200_000_000_000_u64; // 2024-01-01 00:00:00 UTC
        let formatted = format_last_modified(timestamp);
        assert!(formatted.contains("Jan"));
        assert!(formatted.contains("2024"));
    }

    #[test]
    fn test_parse_range_full() {
        assert_eq!(parse_range_header("bytes=0-499", 1000), Some((0, 499)));
    }

    #[test]
    fn test_parse_range_open_end() {
        assert_eq!(parse_range_header("bytes=500-", 1000), Some((500, 999)));
    }

    #[test]
    fn test_parse_range_suffix() {
        assert_eq!(parse_range_header("bytes=-200", 1000), Some((800, 999)));
    }

    #[test]
    fn test_parse_range_first_byte() {
        assert_eq!(parse_range_header("bytes=0-0", 1000), Some((0, 0)));
    }

    #[test]
    fn test_parse_range_clamped_end() {
        // End beyond file size should be clamped
        assert_eq!(parse_range_header("bytes=0-9999", 100), Some((0, 99)));
    }

    #[test]
    fn test_parse_range_start_beyond_length() {
        assert_eq!(parse_range_header("bytes=1000-", 100), None);
    }

    #[test]
    fn test_parse_range_suffix_zero() {
        assert_eq!(parse_range_header("bytes=-0", 100), None);
    }

    #[test]
    fn test_parse_range_suffix_exceeds_length() {
        assert_eq!(parse_range_header("bytes=-200", 100), None);
    }

    #[test]
    fn test_parse_range_empty_file() {
        assert_eq!(parse_range_header("bytes=0-0", 0), None);
    }

    #[test]
    fn test_parse_range_invalid_format() {
        assert_eq!(parse_range_header("invalid", 100), None);
        assert_eq!(parse_range_header("bytes=abc-def", 100), None);
    }

    #[test]
    fn test_parse_range_inverted() {
        // start > end
        assert_eq!(parse_range_header("bytes=500-100", 1000), None);
    }
}
