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
use s4_core::{ReadOptions, StorageEngine};
use serde::Deserialize;
use std::collections::HashMap;
use tokio_util::io::ReaderStream;
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

    // Validate x-amz-content-sha256 if provided (must be done after decoding aws-chunked body)
    if let Some(sha256_header) = headers.get("x-amz-content-sha256") {
        if let Ok(expected_hash) = sha256_header.to_str() {
            if expected_hash != "UNSIGNED-PAYLOAD" && !expected_hash.starts_with("STREAMING-") {
                use sha2::{Digest, Sha256};
                let mut hasher = Sha256::new();
                hasher.update(&body);
                let actual_hash = format!("{:064x}", hasher.finalize());

                if !expected_hash.eq_ignore_ascii_case(&actual_hash) {
                    return S3Error::XAmzContentSHA256Mismatch.into_response();
                }
            }
        }
    }

    // Validate Content-MD5 if provided
    if let Err(resp) = super::bucket::validate_content_md5(&headers, &body) {
        return resp;
    }

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
        .unwrap_or("binary/octet-stream");

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

    // Store x-amz-storage-class as internal metadata (validate first)
    if let Some(sc) = headers.get("x-amz-storage-class").and_then(|v| v.to_str().ok()) {
        match sc {
            "STANDARD"
            | "REDUCED_REDUNDANCY"
            | "STANDARD_IA"
            | "ONEZONE_IA"
            | "INTELLIGENT_TIERING"
            | "GLACIER"
            | "DEEP_ARCHIVE"
            | "GLACIER_IR" => {
                metadata.insert("_storage_class".to_string(), sc.to_string());
            }
            _ => {
                return S3Error::InvalidStorageClass.into_response();
            }
        }
    }

    // Parse x-amz-tagging header and store as internal metadata
    if let Some(tagging_header) = headers.get("x-amz-tagging").and_then(|v| v.to_str().ok()) {
        let tags = parse_url_encoded_tags(tagging_header);
        if tags.len() > 10 {
            return S3Error::BadRequest("Object tags cannot be greater than 10".to_string())
                .into_response();
        }
        for (key, value) in &tags {
            if key.len() > 128 {
                return S3Error::InvalidTag("The TagKey you have provided is too long".to_string())
                    .into_response();
            }
            if value.len() > 256 {
                return S3Error::InvalidTag(
                    "The TagValue you have provided is too long".to_string(),
                )
                .into_response();
            }
        }
        if !tags.is_empty() {
            if let Ok(tags_json) = serde_json::to_string(&tags) {
                metadata.insert("_tags".to_string(), tags_json);
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

    // Parse per-object Object Lock headers (x-amz-object-lock-*)
    let lock_legal_hold = headers
        .get("x-amz-object-lock-legal-hold")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.eq_ignore_ascii_case("ON"));

    let lock_mode =
        headers
            .get("x-amz-object-lock-mode")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| match v {
                "GOVERNANCE" => Some(s4_features::object_lock::RetentionMode::GOVERNANCE),
                "COMPLIANCE" => Some(s4_features::object_lock::RetentionMode::COMPLIANCE),
                _ => None,
            });

    let lock_retain_until = headers
        .get("x-amz-object-lock-retain-until-date")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| {
            chrono::DateTime::parse_from_rfc3339(v)
                .ok()
                .map(|dt| dt.timestamp_nanos_opt().unwrap_or(0) as u64)
        });

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

            // Apply per-object lock headers if provided
            let needs_lock_update =
                lock_legal_hold.is_some() || lock_mode.is_some() || lock_retain_until.is_some();

            if needs_lock_update {
                let vid = version_id.as_deref().unwrap_or("null");
                if let Ok(mut record) = storage.head_object_version(&bucket, &key, vid).await {
                    if let Some(hold) = lock_legal_hold {
                        record.legal_hold = hold;
                    }
                    if let Some(mode) = lock_mode {
                        record.retention_mode = Some(mode);
                    }
                    if let Some(retain_until) = lock_retain_until {
                        record.retain_until_timestamp = Some(retain_until);
                    }
                    if let Err(e) = storage.update_object_metadata(&bucket, &key, vid, record).await
                    {
                        error!("Failed to apply object lock headers: {:?}", e);
                    }
                }
            }

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

/// Copies an object within or between buckets.
///
/// S3 API: PUT /{bucket}/{key} with `x-amz-copy-source` header
///
/// # Headers
///
/// - `x-amz-copy-source`: Source object in the form `bucket/key` or `/bucket/key`
/// - `x-amz-metadata-directive`: `COPY` (default) or `REPLACE`
///
/// # Response
///
/// - 200 OK with CopyObjectResult XML
/// - 404 Not Found if source object doesn't exist
pub async fn copy_object(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
    headers: HeaderMap,
) -> impl IntoResponse {
    // Parse x-amz-copy-source header - use as_bytes to handle all encodings
    let copy_source = match headers.get("x-amz-copy-source") {
        Some(v) => String::from_utf8_lossy(v.as_bytes()).to_string(),
        None => {
            return S3Error::InvalidRequest("Missing x-amz-copy-source header".to_string())
                .into_response()
        }
    };

    info!("CopyObject: raw x-amz-copy-source='{}'", copy_source);

    // URL-decode the copy source (some SDKs double-encode)
    let copy_source_decoded = simple_url_decode(&copy_source);
    let copy_source_str = copy_source_decoded.trim_start_matches('/');

    // Strip query parameters (e.g. ?versionId=...)
    let copy_source_path = copy_source_str.split('?').next().unwrap_or(copy_source_str);

    debug!("CopyObject: decoded copy-source='{}'", copy_source_path);

    // Try to split into bucket/key
    let (src_bucket, src_key): (String, String) = if let Some((b, k)) =
        copy_source_path.split_once('/')
    {
        if !b.is_empty() && !k.is_empty() {
            (b.to_string(), k.to_string())
        } else {
            return S3Error::InvalidArgument("Invalid x-amz-copy-source format".to_string())
                .into_response();
        }
    } else {
        // No slash found after decode. Try double-decode (for %252F → %2F → /).
        let double_decoded = simple_url_decode(copy_source_path);
        let dd_trimmed = double_decoded.trim_start_matches('/');
        let dd_path = dd_trimmed.split('?').next().unwrap_or(dd_trimmed);
        if let Some((b, k)) = dd_path.split_once('/') {
            if !b.is_empty() && !k.is_empty() {
                (b.to_string(), k.to_string())
            } else {
                return S3Error::InvalidArgument("Invalid x-amz-copy-source format".to_string())
                    .into_response();
            }
        } else {
            // Copy source has no bucket/key separator — invalid per S3 spec
            return S3Error::InvalidArgument(
                "Copy Source must mention the source bucket and key: you have provided an invalid copy source".to_string()
            ).into_response();
        }
    };

    info!(
        "CopyObject: src_bucket={}, src_key={} -> dst_bucket={}, dst_key={}",
        &src_bucket, &src_key, &bucket, &key
    );

    let storage = state.storage.read().await;

    // Check destination bucket exists
    let bucket_marker_key = format!("__s4_bucket_marker_{}", bucket);
    if storage.head_object("__system__", &bucket_marker_key).await.is_err() {
        return S3Error::NoSuchBucket.into_response();
    }

    // Get the source object
    let (src_data, src_record) = match storage.get_object(&src_bucket, &src_key).await {
        Ok(data) => data,
        Err(_) => return S3Error::NoSuchKey.into_response(),
    };

    // Determine metadata: COPY (default) preserves source metadata, REPLACE uses request headers
    let metadata_directive = headers
        .get("x-amz-metadata-directive")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("COPY");

    // Capture storage class from request (applies in both COPY and REPLACE modes)
    let req_storage_class = headers
        .get("x-amz-storage-class")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    // S3 spec: copying an object to itself is only allowed if metadata directive is REPLACE
    // or if storage class (or other attributes) are being changed
    if src_bucket == bucket
        && src_key == key
        && !metadata_directive.eq_ignore_ascii_case("REPLACE")
        && req_storage_class.is_none()
    {
        return S3Error::InvalidArgument(
            "This copy request is illegal because it is trying to copy an object to itself without changing the object's metadata, storage class, website redirect location or encryption attributes.".to_string()
        ).into_response();
    }

    let (content_type, mut metadata) = if metadata_directive.eq_ignore_ascii_case("REPLACE") {
        let ct = headers
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("binary/octet-stream")
            .to_string();
        let mut meta = HashMap::new();
        for (name, value) in headers.iter() {
            let name_str = name.as_str().to_lowercase();
            if name_str.starts_with("x-amz-meta-") {
                if let Ok(v) = value.to_str() {
                    let meta_key = name_str.strip_prefix("x-amz-meta-").unwrap().to_string();
                    meta.insert(meta_key, v.to_string());
                }
            }
        }
        (ct, meta)
    } else {
        (src_record.content_type.clone(), src_record.metadata.clone())
    };

    // Apply storage class from request (overrides source metadata)
    if let Some(sc) = req_storage_class {
        metadata.insert("_storage_class".to_string(), sc);
    }

    // Get versioning status for destination bucket
    let versioning_status = get_bucket_versioning_status(&state, &bucket).await;

    // Get bucket Object Lock configuration for default retention
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

    // Store the copy
    match storage
        .put_object_with_retention(
            &bucket,
            &key,
            &src_data,
            &content_type,
            &metadata,
            versioning_status,
            default_retention,
        )
        .await
    {
        Ok((etag, version_id)) => {
            info!(
                "Object copied: src={}/{} -> dst={}/{}, etag={}, version_id={:?}",
                &src_bucket, &src_key, &bucket, &key, &etag, &version_id
            );

            let last_modified = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string();
            let xml = crate::s3::xml::copy_object_response(&etag, &last_modified);

            let mut builder = Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "application/xml");

            if let Some(vid) = version_id {
                builder = builder.header("x-amz-version-id", vid);
            }

            builder.body(Body::from(xml)).unwrap()
        }
        Err(e) => {
            error!("Failed to copy object: {:?}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to copy object: {}", e),
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

    // We need HEAD first to know total_size for range parsing.
    // For non-range requests we skip HEAD and go straight to streaming.
    let has_range_header = headers.get(header::RANGE).is_some();

    // Build ReadOptions based on Range header
    let read_options = if has_range_header {
        // We need the total size to parse suffix ranges (bytes=-N).
        // HEAD is cheap (metadata only, no volume I/O).
        let head_result = if let Some(ref version_id) = query.version_id {
            storage.head_object_version(&bucket, &key, version_id).await
        } else {
            storage.head_object(&bucket, &key).await
        };

        let total_size = match head_result {
            Ok(rec) => rec.size,
            Err(s4_core::StorageError::ObjectNotFound { .. }) => {
                return S3Error::NoSuchKey.into_response();
            }
            Err(s4_core::StorageError::VersionNotFound { version_id, .. }) => {
                debug!("Version not found: {}", version_id);
                return S3Error::NoSuchVersion.into_response();
            }
            Err(s4_core::StorageError::DeleteMarker { version_id, .. }) => {
                return build_delete_marker_response(&query, &version_id);
            }
            Err(e) => {
                error!("Failed to head object: {:?}", e);
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Failed to get object: {}", e),
                )
                    .into_response();
            }
        };

        let range_header_str =
            headers.get(header::RANGE).and_then(|v| v.to_str().ok()).unwrap_or("");

        match parse_range_header_u64(range_header_str, total_size) {
            Some((start, end)) => ReadOptions {
                range: Some((start, end)),
            },
            None => {
                // RFC 7233: unsatisfiable range → 416
                let cr = format!("bytes */{}", total_size);
                return Response::builder()
                    .status(StatusCode::RANGE_NOT_SATISFIABLE)
                    .header(header::CONTENT_RANGE, cr)
                    .body(Body::empty())
                    .unwrap();
            }
        }
    } else {
        ReadOptions::default()
    };

    // Open streaming reader
    let result = if let Some(ref version_id) = query.version_id {
        storage
            .open_object_version_stream(&bucket, &key, version_id, read_options)
            .await
    } else {
        storage.open_object_stream(&bucket, &key, read_options).await
    };

    match result {
        Ok(stream) => {
            let record = &stream.record;
            let status = if stream.content_range.is_some() {
                StatusCode::PARTIAL_CONTENT
            } else {
                StatusCode::OK
            };

            let mut builder = Response::builder()
                .status(status)
                .header(header::CONTENT_LENGTH, stream.content_length)
                .header(header::CONTENT_TYPE, &record.content_type)
                .header("ETag", format!("\"{}\"", record.etag))
                .header(header::ACCEPT_RANGES, "bytes")
                .header(
                    header::LAST_MODIFIED,
                    format_last_modified(record.modified_at),
                )
                .header(
                    "x-amz-storage-class",
                    record.metadata.get("_storage_class").map(|s| s.as_str()).unwrap_or("STANDARD"),
                );

            if let Some((start, end, total)) = stream.content_range {
                builder = builder.header(
                    header::CONTENT_RANGE,
                    format!("bytes {}-{}/{}", start, end, total),
                );
            }

            // Add version ID header if present
            if let Some(ref vid) = record.version_id {
                builder = builder.header("x-amz-version-id", vid.clone());
            }

            // Add custom metadata headers
            for (meta_key, value) in &record.metadata {
                if !meta_key.starts_with('_') {
                    builder = builder.header(format!("x-amz-meta-{}", meta_key), value);
                }
            }

            // Stream the body — memory usage is O(buffer_size), not O(object_size)
            let reader_stream = ReaderStream::new(stream.body);
            builder.body(Body::from_stream(reader_stream)).unwrap()
        }
        Err(s4_core::StorageError::ObjectNotFound { .. }) => S3Error::NoSuchKey.into_response(),
        Err(s4_core::StorageError::VersionNotFound { version_id, .. }) => {
            debug!("Version not found: {}", version_id);
            S3Error::NoSuchVersion.into_response()
        }
        Err(s4_core::StorageError::DeleteMarker { version_id, .. }) => {
            build_delete_marker_response(&query, &version_id)
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

/// Builds the appropriate response for a delete marker.
fn build_delete_marker_response(query: &ObjectVersionQuery, version_id: &str) -> Response {
    if query.version_id.is_some() {
        // S3 returns 405 MethodNotAllowed when requesting a specific delete marker version
        S3Error::MethodNotAllowed.into_response()
    } else {
        // S3 returns 404 with x-amz-delete-marker header when latest version is a delete marker
        Response::builder()
            .status(StatusCode::NOT_FOUND)
            .header("x-amz-delete-marker", "true")
            .header("x-amz-version-id", version_id)
            .body(Body::empty())
            .unwrap()
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
    bypass_governance: bool,
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
                if let (Some(mode), Some(retain_until)) =
                    (record.retention_mode, record.retain_until_timestamp)
                {
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_nanos() as u64;

                    if now < retain_until {
                        // GOVERNANCE mode can be bypassed with the header
                        let blocked = match mode {
                            s4_features::object_lock::RetentionMode::GOVERNANCE => {
                                !bypass_governance
                            }
                            s4_features::object_lock::RetentionMode::COMPLIANCE => true,
                        };
                        if blocked {
                            error!(
                                "Cannot delete {}/{} version {}: retention until {}",
                                bucket, key, version_id, retain_until
                            );
                            return S3Error::AccessDenied.into_response();
                        }
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
                if let (Some(mode), Some(retain_until)) =
                    (record.retention_mode, record.retain_until_timestamp)
                {
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_nanos() as u64;

                    if now < retain_until {
                        // GOVERNANCE mode can be bypassed with the header
                        let blocked = match mode {
                            s4_features::object_lock::RetentionMode::GOVERNANCE => {
                                !bypass_governance
                            }
                            s4_features::object_lock::RetentionMode::COMPLIANCE => true,
                        };
                        if blocked {
                            error!(
                                "Cannot delete {}/{}: retention until {}",
                                bucket, key, retain_until
                            );
                            return S3Error::AccessDenied.into_response();
                        }
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
                if query.version_id.is_some() {
                    // S3 returns 405 MethodNotAllowed when requesting a specific delete marker version
                    return S3Error::MethodNotAllowed.into_response();
                }
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
                )
                .header(
                    "x-amz-storage-class",
                    record.metadata.get("_storage_class").map(|s| s.as_str()).unwrap_or("STANDARD"),
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
/// Parses an HTTP Range header into inclusive (start, end) byte positions.
///
/// Works with u64 sizes to support objects larger than 4 GB.
/// Returns `None` for unsatisfiable or malformed ranges.
fn parse_range_header_u64(value: &str, total_len: u64) -> Option<(u64, u64)> {
    let value = value.strip_prefix("bytes=")?;
    let (start_str, end_str) = value.split_once('-')?;

    if total_len == 0 {
        return None;
    }

    if start_str.is_empty() {
        // Suffix range: bytes=-N (last N bytes)
        let suffix_len: u64 = end_str.parse().ok()?;
        if suffix_len == 0 || suffix_len > total_len {
            return None;
        }
        Some((total_len - suffix_len, total_len - 1))
    } else {
        let start: u64 = start_str.parse().ok()?;
        if start >= total_len {
            return None;
        }
        let end = if end_str.is_empty() {
            total_len - 1
        } else {
            let end: u64 = end_str.parse().ok()?;
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

/// Parses URL-encoded tags (e.g., "Key1=Value1&Key2=Value2") into a HashMap.
fn parse_url_encoded_tags(input: &str) -> HashMap<String, String> {
    let mut tags = HashMap::new();
    for pair in input.split('&') {
        if let Some((k, v)) = pair.split_once('=') {
            let key = simple_url_decode(k);
            let value = simple_url_decode(v);
            if !key.is_empty() {
                tags.insert(key, value);
            }
        }
    }
    tags
}

/// Builds XML TagSet from a tags HashMap.
fn tags_to_xml(tags: &HashMap<String, String>) -> String {
    if tags.is_empty() {
        return r#"<?xml version="1.0" encoding="UTF-8"?>
<Tagging xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <TagSet/>
</Tagging>"#
            .to_string();
    }

    let mut tag_xml = String::new();
    let mut sorted_keys: Vec<&String> = tags.keys().collect();
    sorted_keys.sort();
    for key in sorted_keys {
        let value = &tags[key];
        tag_xml.push_str(&format!(
            "    <Tag><Key>{}</Key><Value>{}</Value></Tag>\n",
            escape_xml(key),
            escape_xml(value)
        ));
    }

    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<Tagging xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <TagSet>
{}  </TagSet>
</Tagging>"#,
        tag_xml
    )
}

/// Escapes special XML characters in a string.
fn escape_xml(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

/// Gets the tag set for an object.
///
/// S3 API: GET /{bucket}/{key}?tagging
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

    // Get object metadata
    let record = if let Some(ref version_id) = version_query.version_id {
        storage.head_object_version(&bucket, &key, version_id).await
    } else {
        storage.head_object(&bucket, &key).await
    };

    let record = match record {
        Ok(r) => r,
        Err(_) => return S3Error::NoSuchKey.into_response(),
    };

    // S3 returns 405 MethodNotAllowed when requesting tagging for a delete marker
    if record.is_delete_marker {
        return S3Error::MethodNotAllowed.into_response();
    }

    // Extract tags from metadata
    let tags: HashMap<String, String> = record
        .metadata
        .get("_tags")
        .and_then(|json| serde_json::from_str(json).ok())
        .unwrap_or_default();

    let xml = tags_to_xml(&tags);

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/xml")
        .body(Body::from(xml))
        .unwrap()
}

/// Sets the tag set for an object.
///
/// S3 API: PUT /{bucket}/{key}?tagging
pub async fn put_object_tagging(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
    Query(version_query): Query<ObjectVersionQuery>,
    body: Bytes,
) -> impl IntoResponse {
    debug!(
        "PutObjectTagging: bucket={}, key={}, version_id={:?}",
        bucket, key, version_query.version_id
    );

    let storage = state.storage.read().await;

    // Check if bucket exists
    let bucket_marker_key = format!("__s4_bucket_marker_{}", bucket);
    if storage.head_object("__system__", &bucket_marker_key).await.is_err() {
        return S3Error::NoSuchBucket.into_response();
    }

    // Get existing object record
    let mut record = if let Some(ref version_id) = version_query.version_id {
        match storage.head_object_version(&bucket, &key, version_id).await {
            Ok(r) => r,
            Err(_) => return S3Error::NoSuchKey.into_response(),
        }
    } else {
        match storage.head_object(&bucket, &key).await {
            Ok(r) => r,
            Err(_) => return S3Error::NoSuchKey.into_response(),
        }
    };

    // S3 returns 405 MethodNotAllowed when setting tagging on a delete marker
    if record.is_delete_marker {
        return S3Error::MethodNotAllowed.into_response();
    }

    // Parse XML body to extract tags
    let body_str = match std::str::from_utf8(&body) {
        Ok(s) => s,
        Err(_) => {
            return S3Error::InvalidRequest("Invalid UTF-8 in request body".to_string())
                .into_response()
        }
    };

    let tags = match parse_tagging_xml(body_str) {
        Ok(t) => t,
        Err(e) => {
            if e.contains("Cannot provide multiple Tags") {
                return S3Error::InvalidTag(e).into_response();
            }
            return S3Error::MalformedXML.into_response();
        }
    };

    // Validate tag limits (max 10 tags)
    if tags.len() > 10 {
        return S3Error::BadRequest("Object tags cannot be greater than 10".to_string())
            .into_response();
    }

    // Validate tag key and value lengths
    for (key, value) in &tags {
        if key.len() > 128 {
            return S3Error::InvalidTag("The TagKey you have provided is too long".to_string())
                .into_response();
        }
        if value.len() > 256 {
            return S3Error::InvalidTag("The TagValue you have provided is too long".to_string())
                .into_response();
        }
    }

    // Store tags as JSON in metadata
    if tags.is_empty() {
        record.metadata.remove("_tags");
    } else {
        match serde_json::to_string(&tags) {
            Ok(json) => {
                record.metadata.insert("_tags".to_string(), json);
            }
            Err(_) => {
                return S3Error::InternalError("Failed to serialize tags".to_string())
                    .into_response()
            }
        }
    }

    // Update object metadata
    let version_id = record.version_id.clone().unwrap_or_default();
    if let Err(e) = storage.update_object_metadata(&bucket, &key, &version_id, record).await {
        error!("Failed to update object tags: {:?}", e);
        return S3Error::InternalError("Failed to update tags".to_string()).into_response();
    }

    Response::builder().status(StatusCode::OK).body(Body::empty()).unwrap()
}

/// Deletes the tag set for an object.
///
/// S3 API: DELETE /{bucket}/{key}?tagging
pub async fn delete_object_tagging(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
    Query(version_query): Query<ObjectVersionQuery>,
) -> impl IntoResponse {
    debug!(
        "DeleteObjectTagging: bucket={}, key={}, version_id={:?}",
        bucket, key, version_query.version_id
    );

    let storage = state.storage.read().await;

    // Check if bucket exists
    let bucket_marker_key = format!("__s4_bucket_marker_{}", bucket);
    if storage.head_object("__system__", &bucket_marker_key).await.is_err() {
        return S3Error::NoSuchBucket.into_response();
    }

    // Get existing object record
    let mut record = if let Some(ref version_id) = version_query.version_id {
        match storage.head_object_version(&bucket, &key, version_id).await {
            Ok(r) => r,
            Err(_) => return S3Error::NoSuchKey.into_response(),
        }
    } else {
        match storage.head_object(&bucket, &key).await {
            Ok(r) => r,
            Err(_) => return S3Error::NoSuchKey.into_response(),
        }
    };

    // S3 returns 405 MethodNotAllowed when deleting tagging on a delete marker
    if record.is_delete_marker {
        return S3Error::MethodNotAllowed.into_response();
    }

    // Remove tags from metadata
    record.metadata.remove("_tags");

    let version_id = record.version_id.clone().unwrap_or_default();
    if let Err(e) = storage.update_object_metadata(&bucket, &key, &version_id, record).await {
        error!("Failed to delete object tags: {:?}", e);
        return S3Error::InternalError("Failed to delete tags".to_string()).into_response();
    }

    StatusCode::NO_CONTENT.into_response()
}

/// Parses S3 Tagging XML into a HashMap.
fn parse_tagging_xml(xml: &str) -> Result<HashMap<String, String>, String> {
    let mut tags = HashMap::new();
    // Simple XML parsing — extract <Tag><Key>k</Key><Value>v</Value></Tag>
    let mut remaining = xml;
    while let Some(tag_start) = remaining.find("<Tag>") {
        let tag_end =
            remaining[tag_start..].find("</Tag>").ok_or("Malformed XML: missing </Tag>")?;
        let tag_content = &remaining[tag_start + 5..tag_start + tag_end];

        let key = extract_xml_value(tag_content, "Key").ok_or("Malformed XML: missing <Key>")?;
        let value = extract_xml_value(tag_content, "Value").unwrap_or_default();

        if tags.contains_key(&key) {
            return Err(format!(
                "Cannot provide multiple Tags with the same key: '{}'",
                key
            ));
        }
        tags.insert(key, value);
        remaining = &remaining[tag_start + tag_end + 6..];
    }
    Ok(tags)
}

/// Extracts the text content of an XML element.
fn extract_xml_value(xml: &str, tag: &str) -> Option<String> {
    let open = format!("<{}>", tag);
    let close = format!("</{}>", tag);
    let start = xml.find(&open)? + open.len();
    let end = xml[start..].find(&close)? + start;
    Some(xml[start..end].to_string())
}

/// Simple URL percent-decoding for x-amz-copy-source header values.
fn simple_url_decode(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut chars = s.chars();

    while let Some(ch) = chars.next() {
        if ch == '%' {
            let hex: String = chars.by_ref().take(2).collect();
            if hex.len() == 2 {
                if let Ok(byte) = u8::from_str_radix(&hex, 16) {
                    result.push(byte as char);
                    continue;
                }
            }
            result.push('%');
            result.push_str(&hex);
        } else if ch == '+' {
            result.push(' ');
        } else {
            result.push(ch);
        }
    }
    result
}

/// Form endpoint for object uploads using `POST /bucket`.
pub async fn post_object(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
    _headers: HeaderMap,
    mut multipart: axum::extract::Multipart,
) -> impl IntoResponse {
    let mut key = String::new();
    let mut policy_opt = None;
    let mut signature = String::new();
    let mut _algorithm = String::new();
    let mut credential = String::new();
    let mut _date = String::new();
    let mut file_content: Option<Vec<u8>> = None;
    let mut file_name = String::new();
    let mut content_type = "binary/octet-stream".to_string();
    let mut success_action_status = String::new();
    let mut success_action_redirect = String::new();

    while let Ok(Some(field)) = multipart.next_field().await {
        let name = field.name().unwrap_or("").to_string();

        if name.eq_ignore_ascii_case("file") {
            if let Some(fname) = field.file_name() {
                file_name = fname.to_string();
            }
            if let Some(ct) = field.content_type() {
                content_type = ct.to_string();
            }
            if let Ok(data) = field.bytes().await {
                file_content = Some(data.to_vec());
            }
            continue;
        }

        let text = field.text().await.unwrap_or_default();
        if name.eq_ignore_ascii_case("key") {
            key = text;
        } else if name.eq_ignore_ascii_case("policy") {
            policy_opt = Some(text);
        } else if name.eq_ignore_ascii_case("x-amz-signature") {
            signature = text;
        } else if name.eq_ignore_ascii_case("x-amz-credential") {
            credential = text;
        } else if name.eq_ignore_ascii_case("x-amz-algorithm") {
            _algorithm = text;
        } else if name.eq_ignore_ascii_case("x-amz-date") {
            _date = text;
        } else if name.eq_ignore_ascii_case("content-type") {
            content_type = text;
        } else if name.eq_ignore_ascii_case("success_action_status") {
            success_action_status = text;
        } else if name.eq_ignore_ascii_case("success_action_redirect") {
            success_action_redirect = text;
        }
    }

    let file_content = match file_content {
        Some(fc) => fc,
        None => return S3Error::InvalidRequest("Missing file field".to_string()).into_response(),
    };

    if key.is_empty() {
        return S3Error::InvalidRequest("Missing key field".to_string()).into_response();
    }

    // Resolve ${filename} placeholder in key
    if key.contains("${filename}") {
        let replacement = if file_name.is_empty() {
            "file"
        } else {
            &file_name
        };
        key = key.replace("${filename}", replacement);
    }

    // Verify Policy signature
    if let Some(ref policy_b64) = policy_opt {
        let cred_parts: Vec<&str> = credential.split('/').collect();
        if cred_parts.len() >= 4 {
            let access_key = cred_parts[0];
            let cred_date = cred_parts[1];
            let region = cred_parts[2];
            let service = cred_parts[3];

            // Try to find the secret key for this access key
            let secret_key_opt: Option<String> = {
                // First try IAM storage
                match state.iam_storage.get_user_by_access_key_with_secret(access_key).await {
                    Ok(Some(user)) => user.secret_key,
                    _ => {
                        // Fallback to legacy ENV credentials
                        if access_key == state.access_key_id {
                            Some(state.secret_access_key.clone())
                        } else {
                            None
                        }
                    }
                }
            };

            match secret_key_opt {
                Some(secret) => {
                    let signing_key = crate::auth::signature_v4::calculate_signing_key(
                        &secret, cred_date, region, service,
                    );
                    let calc_sig =
                        crate::auth::signature_v4::calculate_signature(&signing_key, policy_b64);
                    let calc_sig_bytes = hex::decode(&calc_sig).unwrap_or_default();
                    let recv_sig_bytes = hex::decode(&signature).unwrap_or_default();

                    if !crate::auth::signature_v4::constant_time_eq(
                        &calc_sig_bytes,
                        &recv_sig_bytes,
                    ) {
                        return S3Error::SignatureDoesNotMatch.into_response();
                    }
                }
                None => {
                    return S3Error::AccessDenied.into_response();
                }
            }
        } else {
            return S3Error::AccessDenied.into_response();
        }
    }
    // If no policy is provided, allow (matches S3 behavior for anonymous uploads on public buckets)

    // Write object
    let versioning_status = get_bucket_versioning_status(&state, &bucket).await;
    let storage = state.storage.write().await;

    // Check bucket existence
    let bucket_marker_key = format!("__s4_bucket_marker_{}", bucket);
    if storage.head_object("__system__", &bucket_marker_key).await.is_err() {
        return S3Error::NoSuchBucket.into_response();
    }

    let metadata = std::collections::HashMap::new();
    let result = match versioning_status {
        s4_core::types::VersioningStatus::Enabled | s4_core::types::VersioningStatus::Suspended => {
            storage
                .put_object_versioned(
                    &bucket,
                    &key,
                    &file_content,
                    &content_type,
                    &metadata,
                    versioning_status,
                )
                .await
        }
        s4_core::types::VersioningStatus::Unversioned => {
            match storage.put_object(&bucket, &key, &file_content, &content_type, &metadata).await {
                Ok(etag) => Ok((etag, None)),
                Err(e) => Err(e),
            }
        }
    };

    match result {
        Ok((etag, version_id)) => {
            info!(
                "POST Object uploaded: bucket={}, key={}, size={}",
                bucket,
                key,
                file_content.len()
            );

            let mut builder = Response::builder().header("etag", format!("\"{}\"", etag));

            if let Some(vid) = version_id {
                builder = builder.header("x-amz-version-id", vid);
            }

            if !success_action_redirect.is_empty() {
                builder
                    .status(StatusCode::SEE_OTHER)
                    .header("location", success_action_redirect)
                    .body(Body::from(String::new()))
                    .unwrap()
            } else if success_action_status == "201" {
                let xml = format!(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<PostResponse><Bucket>{}</Bucket><Key>{}</Key><ETag>\"{}\"</ETag></PostResponse>",
                    bucket, key, etag
                );
                builder
                    .status(StatusCode::CREATED)
                    .header("content-type", "application/xml")
                    .body(Body::from(xml))
                    .unwrap()
            } else {
                builder.status(StatusCode::NO_CONTENT).body(Body::from(String::new())).unwrap()
            }
        }
        Err(e) => {
            error!("POST Object failed: {:?}", e);
            S3Error::InternalError("Failed to store object".to_string()).into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_range_header(value: &str, total_len: usize) -> Option<(usize, usize)> {
        let (s, e) = parse_range_header_u64(value, total_len as u64)?;
        Some((s as usize, e as usize))
    }

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
