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

//! Multipart upload handlers.
//!
//! Implements S3-compatible multipart upload operations:
//! - CreateMultipartUpload (POST /{bucket}/{key}?uploads)
//! - UploadPart (PUT /{bucket}/{key}?partNumber=N&uploadId=X)
//! - CompleteMultipartUpload (POST /{bucket}/{key}?uploadId=X)
//! - AbortMultipartUpload (DELETE /{bucket}/{key}?uploadId=X)

use axum::{
    body::{Body, Bytes},
    extract::{Path, Query, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use s4_core::StorageEngine;
use serde::Deserialize;
use std::collections::HashMap;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use sha2::{Digest, Sha256};

use crate::middleware::{decode_aws_chunked, is_aws_chunked, validate_decoded_content_length};
use crate::s3::errors::S3Error;
use crate::server::AppState;

/// Query parameters for multipart operations.
#[derive(Debug, Deserialize, Default)]
pub struct MultipartQuery {
    /// Upload ID for existing multipart upload.
    #[serde(rename = "uploadId")]
    pub upload_id: Option<String>,
    /// Part number (1-10000).
    #[serde(rename = "partNumber")]
    pub part_number: Option<u32>,
    /// Marker for uploads initiation (presence indicates CreateMultipartUpload).
    pub uploads: Option<String>,
}

/// Initiates a multipart upload.
///
/// S3 API: POST /{bucket}/{key}?uploads
///
/// # Returns
///
/// XML response with uploadId.
pub async fn create_multipart_upload(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
    headers: HeaderMap,
) -> impl IntoResponse {
    info!("CreateMultipartUpload: bucket={}, key={}", bucket, key);

    let storage = state.storage.read().await;

    // Check if bucket exists
    let bucket_marker_key = format!("__s4_bucket_marker_{}", bucket);
    if storage.head_object("__system__", &bucket_marker_key).await.is_err() {
        return S3Error::NoSuchBucket.into_response();
    }

    // Generate upload ID
    let upload_id = Uuid::new_v4().to_string().replace('-', "");

    // Extract content type for later use
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/octet-stream");

    // Store upload metadata
    let upload_marker_key = format!("__s4_multipart_{}_{}", upload_id, key);
    let metadata = format!(
        "{{\"bucket\":\"{}\",\"key\":\"{}\",\"content_type\":\"{}\"}}",
        bucket, key, content_type
    );

    if let Err(e) = storage
        .put_object(
            "__system__",
            &upload_marker_key,
            metadata.as_bytes(),
            "application/json",
            &HashMap::new(),
        )
        .await
    {
        error!("Failed to create multipart upload marker: {:?}", e);
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to initiate multipart upload",
        )
            .into_response();
    }

    info!(
        "Multipart upload initiated: bucket={}, key={}, uploadId={}",
        bucket, key, upload_id
    );

    // Return XML response
    let xml = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Bucket>{}</Bucket>
  <Key>{}</Key>
  <UploadId>{}</UploadId>
</InitiateMultipartUploadResult>"#,
        escape_xml(&bucket),
        escape_xml(&key),
        upload_id
    );

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/xml")
        .body(Body::from(xml))
        .unwrap()
}

/// Uploads a part.
///
/// S3 API: PUT /{bucket}/{key}?partNumber=N&uploadId=X
///
/// # Returns
///
/// - 200 OK with ETag header
pub async fn upload_part(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
    Query(params): Query<MultipartQuery>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let upload_id = match params.upload_id {
        Some(id) => id,
        None => return S3Error::InvalidRequest("Missing uploadId".to_string()).into_response(),
    };

    let part_number = match params.part_number {
        Some(n) if (1..=10000).contains(&n) => n,
        _ => {
            return S3Error::InvalidRequest("Invalid partNumber (must be 1-10000)".to_string())
                .into_response()
        }
    };

    let storage = state.storage.read().await;

    // Check for server-side copy (x-amz-copy-source header)
    let is_copy_request = headers.get("x-amz-copy-source").is_some();
    let body = if let Some(copy_source) = headers.get("x-amz-copy-source") {
        // Parse copy source: bucket/key or /bucket/key
        let copy_source_str = copy_source.to_str().unwrap_or("");
        let copy_source_str = copy_source_str.trim_start_matches('/');

        let (src_bucket, src_key) = match copy_source_str.split_once('/') {
            Some((b, k)) => (b.to_string(), k.to_string()),
            None => {
                return S3Error::InvalidRequest("Invalid x-amz-copy-source format".to_string())
                    .into_response()
            }
        };

        debug!(
            "UploadPart: server-side copy from bucket={}, key={}",
            src_bucket, src_key
        );

        // Get the source object
        let (src_data, _) = match storage.get_object(&src_bucket, &src_key).await {
            Ok(data) => data,
            Err(e) => {
                error!("Failed to get source object for copy: {:?}", e);
                return S3Error::NoSuchKey.into_response();
            }
        };

        // Check for range copy (x-amz-copy-source-range header)
        let data = if let Some(range_header) = headers.get("x-amz-copy-source-range") {
            let range_str = range_header.to_str().unwrap_or("");
            // Parse "bytes=start-end"
            let range_str = range_str.trim_start_matches("bytes=");
            let (start, end) = match range_str.split_once('-') {
                Some((s, e)) => {
                    let start: usize = s.parse().unwrap_or(0);
                    let end: usize = e.parse().unwrap_or(src_data.len() - 1);
                    (start, end)
                }
                None => (0, src_data.len() - 1),
            };

            debug!(
                "UploadPart: copying range bytes={}-{} (total size={})",
                start,
                end,
                src_data.len()
            );

            // Extract the range (end is inclusive in S3 range syntax)
            let end_exclusive = (end + 1).min(src_data.len());
            if start >= src_data.len() || start > end_exclusive {
                return S3Error::InvalidRequest("Invalid range".to_string()).into_response();
            }

            Bytes::from(src_data[start..end_exclusive].to_vec())
        } else {
            Bytes::from(src_data)
        };

        data
    } else {
        // Check for AWS chunked encoding and decode if necessary
        if is_aws_chunked(&headers) {
            debug!(
                "UploadPart: detected aws-chunked encoding, decoding body (raw size={})",
                body.len()
            );
            let decoded = match decode_aws_chunked(&body) {
                Ok(d) => d,
                Err(e) => return e.into_response(),
            };
            debug!(
                "UploadPart: decoded aws-chunked body (decoded size={})",
                decoded.len()
            );
            if let Err(e) = validate_decoded_content_length(&headers, &decoded) {
                return e.into_response();
            }
            Bytes::from(decoded)
        } else {
            body
        }
    };

    // Log early before any potential blocking operations
    info!(
        "UploadPart: bucket={}, key={}, uploadId={}, partNumber={}, size={}",
        bucket,
        key,
        upload_id,
        part_number,
        body.len()
    );

    // Check if bucket exists
    let bucket_marker_key = format!("__s4_bucket_marker_{}", bucket);
    match storage.head_object("__system__", &bucket_marker_key).await {
        Ok(_) => {}
        Err(e) => {
            error!(
                "UploadPart bucket check failed: bucket={}, error={:?}",
                bucket, e
            );
            return S3Error::NoSuchBucket.into_response();
        }
    }

    // Verify multipart upload exists
    let upload_marker_key = format!("__s4_multipart_{}_{}", upload_id, key);
    match storage.head_object("__system__", &upload_marker_key).await {
        Ok(_) => {}
        Err(e) => {
            error!(
                "UploadPart upload check failed: uploadId={}, error={:?}",
                upload_id, e
            );
            return S3Error::NoSuchUpload.into_response();
        }
    }

    // Store the part in memory (avoids polluting append-only volumes with temp data)
    let part_key = format!("__s4_part_{}_{}_{:05}", upload_id, key, part_number);
    let etag = format!("{:x}", Sha256::digest(&body));

    {
        let mut parts = state.part_store.write().await;
        parts.insert(part_key.clone(), body.to_vec());
        let total_bytes: usize = parts.values().map(|v| v.len()).sum();
        if total_bytes > 512 * 1024 * 1024 {
            warn!(
                "In-memory part store is large: {} parts, {} bytes total",
                parts.len(),
                total_bytes
            );
        }
    }

    info!(
        "Part uploaded: uploadId={}, partNumber={}, etag={}",
        upload_id, part_number, etag
    );

    // For server-side copy, return XML CopyPartResult
    if is_copy_request {
        use chrono::Utc;
        let now = Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ");
        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<CopyPartResult>
    <ETag>"{}"</ETag>
    <LastModified>{}</LastModified>
</CopyPartResult>"#,
            etag, now
        );
        Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/xml")
            .body(Body::from(xml))
            .unwrap()
    } else {
        Response::builder()
            .status(StatusCode::OK)
            .header("ETag", format!("\"{}\"", etag))
            .body(Body::empty())
            .unwrap()
    }
}

/// Completes a multipart upload.
///
/// S3 API: POST /{bucket}/{key}?uploadId=X
///
/// # Returns
///
/// XML response with final ETag and location.
pub async fn complete_multipart_upload(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
    Query(params): Query<MultipartQuery>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let upload_id = match params.upload_id {
        Some(id) => id,
        None => return S3Error::InvalidRequest("Missing uploadId".to_string()).into_response(),
    };

    info!(
        "CompleteMultipartUpload: bucket={}, key={}, uploadId={}",
        bucket, key, upload_id
    );
    debug!(
        "Complete request body: {:?}",
        String::from_utf8_lossy(&body)
    );

    let storage = state.storage.read().await;

    // Check if bucket exists
    let bucket_marker_key = format!("__s4_bucket_marker_{}", bucket);
    if storage.head_object("__system__", &bucket_marker_key).await.is_err() {
        return S3Error::NoSuchBucket.into_response();
    }

    // Verify multipart upload exists and get metadata
    let upload_marker_key = format!("__s4_multipart_{}_{}", upload_id, key);
    let upload_metadata = match storage.get_object("__system__", &upload_marker_key).await {
        Ok((data, _)) => String::from_utf8_lossy(&data).to_string(),
        Err(_) => return S3Error::NoSuchUpload.into_response(),
    };

    // Parse content type from metadata
    let content_type = if let Some(start) = upload_metadata.find("\"content_type\":\"") {
        let start = start + "\"content_type\":\"".len();
        if let Some(end) = upload_metadata[start..].find('"') {
            &upload_metadata[start..start + end]
        } else {
            "application/octet-stream"
        }
    } else {
        "application/octet-stream"
    };

    // Collect parts from in-memory store
    let part_prefix = format!("__s4_part_{}_{}_", upload_id, key);
    let sorted_parts: Vec<(String, Vec<u8>)> = {
        let parts = state.part_store.read().await;
        let mut matching: Vec<(String, Vec<u8>)> = parts
            .iter()
            .filter(|(k, _)| k.starts_with(&part_prefix))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        matching.sort_by(|a, b| a.0.cmp(&b.0));
        matching
    };

    if sorted_parts.is_empty() {
        return S3Error::InvalidRequest("No parts uploaded".to_string()).into_response();
    }

    let mut combined_data = Vec::new();
    for (_, data) in &sorted_parts {
        combined_data.extend_from_slice(data);
    }

    // Extract custom metadata from headers
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

    // Store the combined object
    let etag =
        match storage.put_object(&bucket, &key, &combined_data, content_type, &metadata).await {
            Ok(etag) => etag,
            Err(e) => {
                error!("Failed to store combined object: {:?}", e);
                return (StatusCode::INTERNAL_SERVER_ERROR, "Failed to store object")
                    .into_response();
            }
        };

    // Clean up parts from in-memory store and upload marker from storage
    {
        let mut parts = state.part_store.write().await;
        for (part_key, _) in &sorted_parts {
            parts.remove(part_key);
        }
    }
    if let Err(e) = storage.delete_object("__system__", &upload_marker_key).await {
        debug!("Failed to delete upload marker: {:?}", e);
    }

    info!(
        "Multipart upload completed: bucket={}, key={}, etag={}, size={}",
        bucket,
        key,
        etag,
        combined_data.len()
    );

    // Return XML response
    let xml = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Location>http://localhost:9000/{}/{}</Location>
  <Bucket>{}</Bucket>
  <Key>{}</Key>
  <ETag>"{}"</ETag>
</CompleteMultipartUploadResult>"#,
        escape_xml(&bucket),
        escape_xml(&key),
        escape_xml(&bucket),
        escape_xml(&key),
        etag
    );

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/xml")
        .body(Body::from(xml))
        .unwrap()
}

/// Aborts a multipart upload.
///
/// S3 API: DELETE /{bucket}/{key}?uploadId=X
///
/// # Returns
///
/// - 204 No Content on success
pub async fn abort_multipart_upload(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
    Query(params): Query<MultipartQuery>,
) -> impl IntoResponse {
    let upload_id = match params.upload_id {
        Some(id) => id,
        None => return S3Error::InvalidRequest("Missing uploadId".to_string()).into_response(),
    };

    info!(
        "AbortMultipartUpload: bucket={}, key={}, uploadId={}",
        bucket, key, upload_id
    );

    let storage = state.storage.read().await;

    // Delete all parts from in-memory store
    let part_prefix = format!("__s4_part_{}_{}_", upload_id, key);
    {
        let mut parts = state.part_store.write().await;
        parts.retain(|k, _| !k.starts_with(&part_prefix));
    }

    // Delete upload marker
    let upload_marker_key = format!("__s4_multipart_{}_{}", upload_id, key);
    let _ = storage.delete_object("__system__", &upload_marker_key).await;

    info!("Multipart upload aborted: uploadId={}", upload_id);
    StatusCode::NO_CONTENT.into_response()
}

/// Escapes special XML characters.
fn escape_xml(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}
