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

//! Multipart upload handlers — native composite multipart.
//!
//! Implements S3-compatible multipart upload operations:
//! - CreateMultipartUpload (POST /{bucket}/{key}?uploads)
//! - UploadPart (PUT /{bucket}/{key}?partNumber=N&uploadId=X)
//! - CompleteMultipartUpload (POST /{bucket}/{key}?uploadId=X)
//! - AbortMultipartUpload (DELETE /{bucket}/{key}?uploadId=X)
//!
//! Parts are written directly to volume storage during UploadPart.
//! CompleteMultipartUpload publishes a CompositeManifest (O(num_parts),
//! not O(total_bytes)) — no re-reading or re-writing of part data.

use axum::{
    body::{Body, Bytes},
    extract::{Path, Query, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use s4_core::StorageEngine;
use serde::Deserialize;
use std::collections::HashMap;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, error, info};
use uuid::Uuid;

use s4_core::types::composite::MultipartPartRecord;
use tokio_stream::StreamExt;

use crate::middleware::{decode_aws_chunked, is_aws_chunked, validate_decoded_content_length};
use crate::s3::errors::S3Error;
use crate::server::AppState;

/// A part entry parsed from the CompleteMultipartUpload XML manifest.
#[derive(Debug)]
struct ManifestPart {
    part_number: u32,
    etag: String,
}

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
    /// Maximum number of parts to return in ListParts (0-1000).
    #[serde(rename = "max-parts")]
    pub max_parts: Option<i64>,
    /// Part number marker for ListParts pagination.
    #[serde(rename = "part-number-marker")]
    pub part_number_marker: Option<i64>,
}

/// Initiates a multipart upload.
///
/// S3 API: POST /{bucket}/{key}?uploads
///
/// Creates a durable upload session in the storage engine (fjall keyspace).
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
        .unwrap_or("binary/octet-stream");

    // Extract custom metadata (x-amz-meta-* headers)
    let mut custom_metadata = HashMap::new();
    for (name, value) in headers.iter() {
        let name_str = name.as_str().to_lowercase();
        if name_str.starts_with("x-amz-meta-") {
            if let Ok(v) = value.to_str() {
                let meta_key = name_str.strip_prefix("x-amz-meta-").unwrap().to_string();
                custom_metadata.insert(meta_key, v.to_string());
            }
        }
    }

    // Create durable session in storage engine
    if let Err(e) = storage
        .create_multipart_session(&upload_id, &bucket, &key, content_type, &custom_metadata)
        .await
    {
        error!("Failed to create multipart session: {:?}", e);
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
/// Streams the request body directly into volume storage (not temp files).
/// SHA-256, MD5, and CRC32 are computed in a single streaming pass.
pub async fn upload_part(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
    Query(params): Query<MultipartQuery>,
    headers: HeaderMap,
    body: Body,
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

    // Validate bucket exists
    let bucket_marker_key = format!("__s4_bucket_marker_{}", bucket);
    if storage.head_object("__system__", &bucket_marker_key).await.is_err() {
        return S3Error::NoSuchBucket.into_response();
    }

    // Verify multipart upload session exists
    if storage.get_multipart_session(&upload_id).await.is_err() {
        return S3Error::NoSuchUpload.into_response();
    }

    // Check for server-side copy (x-amz-copy-source header)
    if let Some(copy_source) = headers.get("x-amz-copy-source") {
        return upload_part_copy(&storage, &upload_id, part_number, copy_source, &headers).await;
    }

    // Check for AWS chunked encoding (needs full body to decode)
    if is_aws_chunked(&headers) {
        return upload_part_chunked(&storage, &upload_id, part_number, &headers, body).await;
    }

    // Normal upload: stream body directly into volume storage
    let content_length = headers
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);

    // If Content-Length is missing/zero, buffer the body to determine size.
    // (S3 spec requires Content-Length, but some clients omit it.)
    let (reader, actual_length): (Box<dyn tokio::io::AsyncRead + Unpin + Send>, u64) =
        if content_length > 0 {
            let body_stream = body.into_data_stream().map(|item: Result<Bytes, _>| {
                item.map_err(|e| std::io::Error::other(e.to_string()))
            });
            (
                Box::new(tokio_util::io::StreamReader::new(body_stream)),
                content_length,
            )
        } else {
            // Buffer to learn the size
            match collect_body(body).await {
                Ok(bytes) => {
                    let len = bytes.len() as u64;
                    (Box::new(std::io::Cursor::new(bytes.to_vec())), len)
                }
                Err(e) => {
                    error!("Failed to read part body: {:?}", e);
                    return S3Error::InternalError("Failed to read request body".to_string())
                        .into_response();
                }
            }
        };

    info!(
        "UploadPart: bucket={}, key={}, uploadId={}, partNumber={}, content_length={} (streaming)",
        bucket, key, upload_id, part_number, actual_length
    );

    let result = storage
        .upload_part_streaming(&upload_id, part_number, reader, actual_length)
        .await;

    match result {
        Ok(native_result) => {
            info!(
                "Part uploaded: uploadId={}, partNumber={}, etag={}, size={}",
                upload_id, part_number, native_result.etag, native_result.record.size
            );
            Response::builder()
                .status(StatusCode::OK)
                .header("ETag", format!("\"{}\"", native_result.etag))
                .body(Body::empty())
                .unwrap()
        }
        Err(e) => {
            error!("Failed to store part: {:?}", e);
            S3Error::InternalError("Failed to store part".to_string()).into_response()
        }
    }
}

/// Handles UploadPart with server-side copy (x-amz-copy-source).
///
/// Source data is fetched from another object and written as a part
/// directly to volume storage via the native multipart API.
async fn upload_part_copy(
    storage: &s4_core::BitcaskStorageEngine,
    upload_id: &str,
    part_number: u32,
    copy_source: &axum::http::HeaderValue,
    headers: &HeaderMap,
) -> Response {
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
        let range_str = range_str.trim_start_matches("bytes=");
        let (start, end) = match range_str.split_once('-') {
            Some((s, e)) => {
                let start: usize = s.parse().unwrap_or(0);
                let end: usize = e.parse().unwrap_or(src_data.len() - 1);
                (start, end)
            }
            None => (0, src_data.len() - 1),
        };

        let end_exclusive = (end + 1).min(src_data.len());
        if start >= src_data.len() || start > end_exclusive {
            return S3Error::InvalidRequest("Invalid range".to_string()).into_response();
        }

        src_data[start..end_exclusive].to_vec()
    } else {
        src_data
    };

    let content_length = data.len() as u64;
    let reader: Box<dyn tokio::io::AsyncRead + Unpin + Send> = Box::new(std::io::Cursor::new(data));

    match storage
        .upload_part_streaming(upload_id, part_number, reader, content_length)
        .await
    {
        Ok(native_result) => {
            info!(
                "Part uploaded: uploadId={}, partNumber={}, etag={} (copy)",
                upload_id, part_number, native_result.etag
            );
            use chrono::Utc;
            let now = Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ");
            let xml = format!(
                r#"<?xml version="1.0" encoding="UTF-8"?>
<CopyPartResult>
    <ETag>"{}"</ETag>
    <LastModified>{}</LastModified>
</CopyPartResult>"#,
                native_result.etag, now
            );
            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/xml")
                .body(Body::from(xml))
                .unwrap()
        }
        Err(e) => {
            error!("Failed to store copied part: {:?}", e);
            S3Error::InternalError("Failed to store part".to_string()).into_response()
        }
    }
}

/// Handles UploadPart with AWS chunked transfer encoding.
///
/// AWS chunked encoding requires the full body to decode, so this path
/// buffers the body, decodes, then writes to volume storage.
async fn upload_part_chunked(
    storage: &s4_core::BitcaskStorageEngine,
    upload_id: &str,
    part_number: u32,
    headers: &HeaderMap,
    body: Body,
) -> Response {
    // Collect body chunks for decoding (aws-chunked requires full body)
    let body_bytes = match collect_body(body).await {
        Ok(b) => b,
        Err(e) => {
            error!("Failed to read aws-chunked body: {:?}", e);
            return S3Error::InternalError("Failed to read request body".to_string())
                .into_response();
        }
    };

    debug!(
        "UploadPart: detected aws-chunked encoding, decoding body (raw size={})",
        body_bytes.len()
    );
    let decoded = match decode_aws_chunked(&body_bytes) {
        Ok(d) => d,
        Err(e) => return e.into_response(),
    };
    debug!(
        "UploadPart: decoded aws-chunked body (decoded size={})",
        decoded.len()
    );
    if let Err(e) = validate_decoded_content_length(headers, &decoded) {
        return e.into_response();
    }

    let content_length = decoded.len() as u64;
    let reader: Box<dyn tokio::io::AsyncRead + Unpin + Send> =
        Box::new(std::io::Cursor::new(decoded));

    match storage
        .upload_part_streaming(upload_id, part_number, reader, content_length)
        .await
    {
        Ok(native_result) => {
            info!(
                "Part uploaded: uploadId={}, partNumber={}, etag={} (aws-chunked)",
                upload_id, part_number, native_result.etag
            );
            Response::builder()
                .status(StatusCode::OK)
                .header("ETag", format!("\"{}\"", native_result.etag))
                .body(Body::empty())
                .unwrap()
        }
        Err(e) => {
            error!("Failed to store chunked part: {:?}", e);
            S3Error::InternalError("Failed to store part".to_string()).into_response()
        }
    }
}

/// Collects a Body into a contiguous Bytes buffer.
async fn collect_body(body: Body) -> Result<Bytes, String> {
    use tokio_stream::StreamExt;
    let mut chunks = Vec::new();
    let mut stream = body.into_data_stream();
    while let Some(chunk) = stream.next().await {
        let data = chunk.map_err(|e| e.to_string())?;
        chunks.push(data);
    }
    // Concatenate all chunks
    let total_len: usize = chunks.iter().map(|c| c.len()).sum();
    let mut result = Vec::with_capacity(total_len);
    for chunk in chunks {
        result.extend_from_slice(&chunk);
    }
    Ok(Bytes::from(result))
}

/// Completes a multipart upload — native metadata-only completion.
///
/// S3 API: POST /{bucket}/{key}?uploadId=X
///
/// This is O(num_parts), NOT O(total_bytes). No part data is re-read or
/// re-written. Builds a CompositeManifest and publishes it atomically.
///
/// Uses AWS S3-compatible streaming response: sends 200 OK immediately with
/// keep-alive whitespace, then sends final XML. This pattern is required by
/// the S3 spec even though our completion is fast (milliseconds).
pub async fn complete_multipart_upload(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
    Query(params): Query<MultipartQuery>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let upload_id = match params.upload_id {
        Some(id) if !id.is_empty() => id,
        _ => return S3Error::InvalidRequest("Missing uploadId".to_string()).into_response(),
    };

    // Decode aws-chunked encoding if present (AWS CLI may use it even for small bodies)
    let decoded_body = if is_aws_chunked(&headers) {
        debug!("CompleteMultipartUpload: decoding aws-chunked body");
        match decode_aws_chunked(&body) {
            Ok(d) => d,
            Err(e) => return e.into_response(),
        }
    } else {
        body.to_vec()
    };

    let body_str = String::from_utf8_lossy(&decoded_body);
    debug!(
        "CompleteMultipartUpload: bucket={}, key={}, uploadId={}",
        bucket, key, upload_id
    );

    // --- SYNC VALIDATION PHASE ---

    // Parse the XML manifest from the request body
    let manifest_parts = match parse_complete_manifest(&body_str) {
        Ok(parts) => parts,
        Err(e) => {
            error!(
                "CompleteMultipartUpload: failed to parse manifest XML: {:?}",
                e
            );
            return e.into_response();
        }
    };

    let storage = state.storage.read().await;

    // Check if bucket exists
    let bucket_marker_key = format!("__s4_bucket_marker_{}", bucket);
    if storage.head_object("__system__", &bucket_marker_key).await.is_err() {
        return S3Error::NoSuchBucket.into_response();
    }

    // Get upload session from durable storage
    let session = match storage.mark_session_completing(&upload_id).await {
        Ok(s) => s,
        Err(_) => {
            return S3Error::NoSuchUpload.into_response();
        }
    };

    let content_type = session.content_type.clone();
    let metadata = session.metadata.clone();

    // Get all uploaded parts from durable storage (metadata only — no data reads)
    let all_parts = match storage.list_multipart_parts(&upload_id) {
        Ok(parts) => parts,
        Err(e) => {
            error!("CompleteMultipartUpload: failed to list parts: {:?}", e);
            let _ = storage.revert_session_to_open(&upload_id).await;
            return S3Error::InternalError("Failed to list parts".to_string()).into_response();
        }
    };

    // Validate manifest against uploaded parts and check part sizes.
    // On any validation failure, revert session to Open so abort/retry works.
    let selected_parts = match validate_manifest_parts(&manifest_parts, &all_parts) {
        Ok(parts) => parts,
        Err(e) => {
            let _ = storage.revert_session_to_open(&upload_id).await;
            return e.into_response();
        }
    };

    let total_size: u64 = selected_parts.iter().map(|p| p.size).sum();
    info!(
        "CompleteMultipartUpload: bucket={}, key={}, uploadId={}, parts={}, total_size={}",
        bucket,
        key,
        upload_id,
        selected_parts.len(),
        total_size
    );

    // Compute S3 multipart ETag from part MD5 hashes (no data reads needed)
    let s3_etag = compute_native_multipart_etag(&selected_parts);

    // --- STREAMING RESPONSE PHASE ---
    // Send 200 OK immediately (S3 spec), complete in background

    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(4);

    let upload_id_clone = upload_id.clone();
    let bucket_clone = bucket.clone();
    let key_clone = key.clone();
    let s3_etag_clone = s3_etag.clone();

    // Drop storage lock before spawning — will re-acquire in the task
    drop(storage);

    let storage_arc = state.storage.clone();

    tokio::spawn(async move {
        // Keep-alive: send whitespace periodically (safety mechanism —
        // native complete is fast but we keep this for S3 spec compliance)
        let keepalive_tx = tx.clone();
        let keepalive_handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(15)).await;
                if keepalive_tx
                    .send(Ok::<Bytes, std::io::Error>(Bytes::from_static(b" ")))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });

        // Native metadata-only completion — O(num_parts)
        let storage = storage_arc.read().await;
        let result = storage
            .complete_multipart_native(
                &bucket_clone,
                &key_clone,
                &upload_id_clone,
                &selected_parts,
                &s3_etag_clone,
                &content_type,
                &metadata,
            )
            .await;
        drop(storage);

        keepalive_handle.abort();

        match result {
            Ok(_) => {
                info!(
                    "Multipart upload completed (native): bucket={}, key={}, etag={}, size={}, parts={}",
                    bucket_clone, key_clone, s3_etag_clone, total_size, selected_parts.len()
                );

                let xml = format!(
                    r#"<?xml version="1.0" encoding="UTF-8"?>
<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Location>http://localhost:9000/{}/{}</Location>
  <Bucket>{}</Bucket>
  <Key>{}</Key>
  <ETag>"{}"</ETag>
</CompleteMultipartUploadResult>"#,
                    escape_xml(&bucket_clone),
                    escape_xml(&key_clone),
                    escape_xml(&bucket_clone),
                    escape_xml(&key_clone),
                    s3_etag_clone
                );

                let _ = tx.send(Ok(Bytes::from(xml))).await;
            }
            Err(e) => {
                error!("Failed to complete multipart upload: {:?}", e);
                let error_xml = format!(
                    r#"<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>InternalError</Code>
  <Message>Failed to complete multipart upload: {}</Message>
  <RequestId>0</RequestId>
</Error>"#,
                    escape_xml(&format!("{:?}", e))
                );
                let _ = tx.send(Ok(Bytes::from(error_xml))).await;
            }
        }
    });

    let stream = ReceiverStream::new(rx);
    let body = Body::from_stream(stream);

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/xml")
        .body(body)
        .unwrap()
}

/// Aborts a multipart upload.
///
/// S3 API: DELETE /{bucket}/{key}?uploadId=X
///
/// Decrements staged blob refs for all uploaded parts and removes
/// the session and part metadata from durable storage.
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

    // Check session state — reject only if actively completing
    match storage.get_multipart_session(&upload_id).await {
        Ok(session) => {
            if session.state == s4_core::types::composite::MultipartUploadState::Completing {
                info!(
                    "AbortMultipartUpload: rejected — upload {} is currently being completed",
                    upload_id
                );
                return S3Error::InvalidRequest(
                    "Cannot abort upload while CompleteMultipartUpload is in progress".to_string(),
                )
                .into_response();
            }
            // Session exists and is Open — proceed with abort
            if let Err(e) = storage.abort_multipart_native(&upload_id).await {
                error!("Failed to abort multipart upload: {:?}", e);
                return S3Error::InternalError("Failed to abort multipart upload".to_string())
                    .into_response();
            }
        }
        Err(_) => {
            // Session doesn't exist — S3 abort is idempotent, return 204
            debug!(
                "AbortMultipartUpload: upload {} not found, returning 204 (idempotent)",
                upload_id
            );
        }
    }

    info!("Multipart upload aborted: uploadId={}", upload_id);
    StatusCode::NO_CONTENT.into_response()
}

/// Lists parts uploaded for a multipart upload.
///
/// S3 API: GET /{bucket}/{key}?uploadId=X
///
/// Returns metadata from fjall MultipartParts keyspace — no data reads needed.
pub async fn list_parts(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
    Query(params): Query<MultipartQuery>,
) -> impl IntoResponse {
    let upload_id = match params.upload_id {
        Some(id) => id,
        None => return S3Error::InvalidRequest("Missing uploadId".to_string()).into_response(),
    };

    // Validate max-parts (must be non-negative, max 1000)
    let max_parts = match params.max_parts {
        Some(v) if v < 0 => {
            return S3Error::InvalidArgument(
                "Argument max-parts must be a non-negative integer".to_string(),
            )
            .into_response();
        }
        Some(v) => std::cmp::min(v as usize, 1000),
        None => 1000,
    };

    // Validate part-number-marker (must be non-negative)
    let part_number_marker = match params.part_number_marker {
        Some(v) if v < 0 => {
            return S3Error::InvalidArgument(
                "Argument part-number-marker must be a non-negative integer".to_string(),
            )
            .into_response();
        }
        Some(v) => Some(v as u32),
        None => None,
    };

    debug!(
        "ListParts: bucket={}, key={}, uploadId={}, maxParts={}, partNumberMarker={:?}",
        bucket, key, upload_id, max_parts, part_number_marker
    );

    let storage = state.storage.read().await;

    // Check if bucket exists
    let bucket_marker_key = format!("__s4_bucket_marker_{}", bucket);
    if storage.head_object("__system__", &bucket_marker_key).await.is_err() {
        return S3Error::NoSuchBucket.into_response();
    }

    // Verify multipart upload session exists
    if storage.get_multipart_session(&upload_id).await.is_err() {
        return S3Error::NoSuchUpload.into_response();
    }

    // Get parts from durable storage (metadata only — no data reads)
    let all_parts = match storage.list_multipart_parts(&upload_id) {
        Ok(parts) => parts,
        Err(e) => {
            error!("ListParts: failed to list parts: {:?}", e);
            return S3Error::InternalError("Failed to list parts".to_string()).into_response();
        }
    };

    // Convert to display tuples
    let parts_list: Vec<(u32, String, u64)> = all_parts
        .iter()
        .map(|p| (p.part_number, p.etag_md5_hex.clone(), p.size))
        .collect();

    // Apply part-number-marker filter (return parts AFTER the marker)
    let filtered_parts: Vec<_> = if let Some(marker) = part_number_marker {
        parts_list.into_iter().filter(|(num, _, _)| *num > marker).collect()
    } else {
        parts_list
    };

    // Apply max-parts pagination
    let is_truncated = filtered_parts.len() > max_parts;
    let page_parts: Vec<_> = filtered_parts.into_iter().take(max_parts).collect();
    let next_marker = if is_truncated {
        page_parts.last().map(|(num, _, _)| *num)
    } else {
        None
    };

    // Build XML response
    let mut parts_xml = String::new();
    for (part_num, etag, size) in &page_parts {
        parts_xml.push_str(&format!(
            "  <Part>\n    <PartNumber>{}</PartNumber>\n    <ETag>\"{}\"</ETag>\n    <Size>{}</Size>\n    <LastModified>{}</LastModified>\n  </Part>\n",
            part_num, etag, size, chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ")
        ));
    }

    let mut extra_xml = String::new();
    if let Some(marker) = part_number_marker {
        extra_xml.push_str(&format!(
            "  <PartNumberMarker>{}</PartNumberMarker>\n",
            marker
        ));
    }
    if let Some(next) = next_marker {
        extra_xml.push_str(&format!(
            "  <NextPartNumberMarker>{}</NextPartNumberMarker>\n",
            next
        ));
    }

    let xml = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<ListPartsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Bucket>{}</Bucket>
  <Key>{}</Key>
  <UploadId>{}</UploadId>
  <IsTruncated>{}</IsTruncated>
  <MaxParts>{}</MaxParts>
{}{}
</ListPartsResult>"#,
        escape_xml(&bucket),
        escape_xml(&key),
        upload_id,
        is_truncated,
        max_parts,
        extra_xml,
        parts_xml
    );

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/xml")
        .body(Body::from(xml))
        .unwrap()
}

/// Computes the S3-compatible multipart ETag from part MD5 hashes.
///
/// S3 multipart ETag = MD5(concat(part_md5_bytes...)) + "-" + num_parts.
/// This uses the raw MD5 bytes stored in each part record — no data reads.
fn compute_native_multipart_etag(parts: &[MultipartPartRecord]) -> String {
    let mut ctx = md5::Context::new();
    for part in parts {
        ctx.consume(part.etag_md5_bytes);
    }
    let hash = ctx.compute();
    format!("{:x}-{}", hash, parts.len())
}

/// Parses the CompleteMultipartUpload XML manifest into a list of parts.
///
/// Expected format:
/// ```xml
/// <CompleteMultipartUpload>
///   <Part><PartNumber>1</PartNumber><ETag>"abc123"</ETag></Part>
///   <Part><PartNumber>2</PartNumber><ETag>"def456"</ETag></Part>
/// </CompleteMultipartUpload>
/// ```
fn parse_complete_manifest(xml: &str) -> Result<Vec<ManifestPart>, S3Error> {
    let mut parts = Vec::new();
    let mut remaining = xml;

    // Find each <Part>...</Part> block
    while let Some(part_start) = remaining.find("<Part>") {
        let after_tag = &remaining[part_start + 6..];
        let part_end = after_tag.find("</Part>").ok_or(S3Error::MalformedXML)?;
        let part_content = &after_tag[..part_end];

        // Extract <PartNumber>
        let pn_start = part_content.find("<PartNumber>").ok_or(S3Error::MalformedXML)?;
        let pn_value_start = pn_start + 12;
        let pn_end = part_content[pn_value_start..]
            .find("</PartNumber>")
            .ok_or(S3Error::MalformedXML)?;
        let part_number: u32 = part_content[pn_value_start..pn_value_start + pn_end]
            .trim()
            .parse()
            .map_err(|_| S3Error::MalformedXML)?;

        // Extract <ETag>
        let etag_start = part_content.find("<ETag>").ok_or(S3Error::MalformedXML)?;
        let etag_value_start = etag_start + 6;
        let etag_end =
            part_content[etag_value_start..].find("</ETag>").ok_or(S3Error::MalformedXML)?;
        let etag = part_content[etag_value_start..etag_value_start + etag_end]
            .trim()
            .replace("&quot;", "\"")
            .replace("&#34;", "\"")
            .trim_matches('"')
            .to_string();

        parts.push(ManifestPart { part_number, etag });
        remaining = &after_tag[part_end + 7..];
    }

    if parts.is_empty() {
        return Err(S3Error::MalformedXML);
    }

    // S3 requires parts to be in ascending order in the manifest
    for window in parts.windows(2) {
        if window[0].part_number >= window[1].part_number {
            return Err(S3Error::InvalidRequest(
                "Part numbers must be in ascending order".to_string(),
            ));
        }
    }

    Ok(parts)
}

/// Validates the manifest parts against uploaded parts.
///
/// Checks that every part in the manifest exists, ETags match, and all
/// parts except the last are at least 5 MB. Returns the selected parts
/// in manifest order.
fn validate_manifest_parts(
    manifest_parts: &[ManifestPart],
    all_parts: &[MultipartPartRecord],
) -> Result<Vec<MultipartPartRecord>, S3Error> {
    let mut selected_parts = Vec::with_capacity(manifest_parts.len());
    for mp in manifest_parts {
        match all_parts.iter().find(|p| p.part_number == mp.part_number) {
            Some(stored) => {
                if stored.etag_md5_hex != mp.etag {
                    error!(
                        "CompleteMultipartUpload: ETag mismatch for part {}: manifest={}, stored={}",
                        mp.part_number, mp.etag, stored.etag_md5_hex
                    );
                    return Err(S3Error::InvalidRequest(format!(
                        "ETag mismatch for part {}: expected \"{}\", got \"{}\"",
                        mp.part_number, mp.etag, stored.etag_md5_hex
                    )));
                }
                selected_parts.push(stored.clone());
            }
            None => {
                error!("CompleteMultipartUpload: part {} not found", mp.part_number);
                return Err(S3Error::InvalidRequest(format!(
                    "One or more of the specified parts could not be found: part {}",
                    mp.part_number
                )));
            }
        }
    }

    if selected_parts.is_empty() {
        return Err(S3Error::InvalidRequest("No parts uploaded".to_string()));
    }

    // S3 spec: all parts except the last must be at least 5MB
    const MIN_PART_SIZE: u64 = 5 * 1024 * 1024;
    if selected_parts.len() > 1 {
        for part in &selected_parts[..selected_parts.len() - 1] {
            if part.size < MIN_PART_SIZE {
                return Err(S3Error::EntityTooSmall);
            }
        }
    }

    Ok(selected_parts)
}

/// Escapes special XML characters.
fn escape_xml(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}
