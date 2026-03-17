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

//! Bucket operation handlers.
//!
//! Implements S3-compatible bucket operations:
//! - CreateBucket (PUT /{bucket})
//! - DeleteBucket (DELETE /{bucket})
//! - ListBuckets (GET /)
//! - HeadBucket (HEAD /{bucket})
//! - ListObjects (GET /{bucket})
//! - ListObjectVersions (GET /{bucket}?versions)
//! - DeleteObjects (POST /{bucket}?delete)

use axum::{
    body::{Body, Bytes},
    extract::{Path, Query, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use base64::Engine;
use s4_core::StorageEngine;
use serde::Deserialize;
use tracing::{debug, error, info};

use crate::handlers::get_bucket_versioning_status;
use crate::handlers::object_lock::is_bypass_governance;
use crate::s3::{errors::S3Error, xml};
use crate::server::AppState;
use s4_features::object_lock::{object_lock_to_xml, ObjectLockConfiguration, RetentionMode};

/// Query parameters for ListObjects (v1).
#[derive(Debug, Deserialize, Default)]
pub struct ListObjectsQuery {
    /// Limits the response to keys that begin with the specified prefix.
    pub prefix: Option<String>,
    /// A delimiter is a character you use to group keys.
    pub delimiter: Option<String>,
    /// Sets the maximum number of keys returned in the response.
    #[serde(rename = "max-keys")]
    pub max_keys: Option<String>,
    /// Marker is where you want S3 to start listing from.
    pub marker: Option<String>,
    /// List type: "2" for ListObjectsV2, otherwise ListObjects v1.
    #[serde(rename = "list-type")]
    pub list_type: Option<String>,
    /// Continuation token for ListObjectsV2.
    #[serde(rename = "continuation-token")]
    pub continuation_token: Option<String>,
    /// StartAfter for ListObjectsV2 — returns keys after this value.
    #[serde(rename = "start-after")]
    pub start_after: Option<String>,
}

/// Query parameters for ListObjectVersions.
#[derive(Debug, Deserialize, Default)]
pub struct ListObjectVersionsQuery {
    /// Limits the response to keys that begin with the specified prefix.
    pub prefix: Option<String>,
    /// A delimiter is a character you use to group keys.
    pub delimiter: Option<String>,
    /// Sets the maximum number of keys returned in the response.
    #[serde(rename = "max-keys")]
    pub max_keys: Option<String>,
    /// Specifies the key to start with when listing objects in a bucket.
    #[serde(rename = "key-marker")]
    pub key_marker: Option<String>,
    /// Specifies the object version to start with.
    #[serde(rename = "version-id-marker")]
    pub version_id_marker: Option<String>,
    /// Encoding type for keys in the response.
    #[serde(rename = "encoding-type")]
    pub encoding_type: Option<String>,
}

/// Request body for DeleteObjects API.
#[derive(Debug, Clone)]
pub struct DeleteObjectsRequest {
    /// List of objects to delete (up to 1000).
    pub objects: Vec<ObjectIdentifier>,
    /// If true, only errors are returned in response (omit successful deletions).
    pub quiet: bool,
}

/// Identifies an object to delete in a DeleteObjects request.
#[derive(Debug, Clone)]
pub struct ObjectIdentifier {
    /// Object key to delete.
    pub key: String,
    /// Optional version ID for version-specific deletion.
    pub version_id: Option<String>,
}

/// Represents a successfully deleted object.
#[derive(Debug)]
pub struct DeletedObject {
    /// Object key that was deleted.
    pub key: String,
    /// Version ID of deleted object (if versioning enabled).
    pub version_id: Option<String>,
    /// True if a delete marker was created.
    pub delete_marker: bool,
    /// Version ID of created delete marker.
    pub delete_marker_version_id: Option<String>,
}

/// Represents an error when deleting an object.
#[derive(Debug)]
pub struct DeleteError {
    /// Object key that failed to delete.
    pub key: String,
    /// Version ID that failed to delete (if specified).
    pub version_id: Option<String>,
    /// S3 error code (e.g., "NoSuchVersion", "AccessDenied").
    pub code: String,
    /// Human-readable error message.
    pub message: String,
}

/// Result of a DeleteObjects operation.
#[derive(Debug)]
pub struct DeleteObjectsResult {
    /// Successfully deleted objects.
    pub deleted: Vec<DeletedObject>,
    /// Errors encountered during deletion.
    pub errors: Vec<DeleteError>,
}

/// Creates a new bucket.
///
/// S3 API: PUT /{bucket}
///
/// # Returns
///
/// - 200 OK if bucket created successfully
/// - 409 Conflict if bucket already exists
pub async fn create_bucket(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
    headers: HeaderMap,
) -> impl IntoResponse {
    info!("CreateBucket: {}", bucket);

    // Validate bucket name
    if let Err(e) = validate_bucket_name(&bucket) {
        return e.into_response();
    }

    // Check if Object Lock is requested via header
    let object_lock_enabled = headers
        .get("x-amz-bucket-object-lock-enabled")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);

    // Create bucket marker (this tracks bucket existence)
    let storage = state.storage.read().await;
    let bucket_marker_key = format!("__s4_bucket_marker_{}", bucket);

    // Check if marker exists
    if storage.head_object("__system__", &bucket_marker_key).await.is_ok() {
        return S3Error::BucketAlreadyExists.into_response();
    }
    drop(storage);

    // Create the bucket marker
    let storage = state.storage.read().await;
    if let Err(e) = storage
        .put_object(
            "__system__",
            &bucket_marker_key,
            b"1",
            "application/octet-stream",
            &std::collections::HashMap::new(),
        )
        .await
    {
        error!("Failed to create bucket marker: {:?}", e);
        return S3Error::InternalError("Failed to create bucket".to_string()).into_response();
    }

    // If Object Lock requested, enable versioning and Object Lock atomically
    if object_lock_enabled {
        info!(
            "Enabling Object Lock (with versioning) on bucket: {}",
            bucket
        );

        // Enable versioning (required for Object Lock)
        let versioning_key = format!("__s4_bucket_versioning_{}", bucket);
        if let Err(e) = storage
            .put_object(
                "__system__",
                &versioning_key,
                b"Enabled",
                "text/plain",
                &std::collections::HashMap::new(),
            )
            .await
        {
            error!("Failed to enable versioning for Object Lock: {:?}", e);
            return S3Error::InternalError(
                "Failed to enable versioning for Object Lock".to_string(),
            )
            .into_response();
        }

        // Enable Object Lock
        let lock_config = ObjectLockConfiguration {
            object_lock_enabled: true,
            default_retention: None,
        };
        let lock_key = format!("__s4_bucket_object_lock_{}", bucket);
        let lock_xml = object_lock_to_xml(&lock_config);
        if let Err(e) = storage
            .put_object(
                "__system__",
                &lock_key,
                lock_xml.as_bytes(),
                "application/xml",
                &std::collections::HashMap::new(),
            )
            .await
        {
            error!("Failed to enable Object Lock: {:?}", e);
            return S3Error::InternalError("Failed to enable Object Lock".to_string())
                .into_response();
        }

        info!("Object Lock enabled on bucket: {}", bucket);
    }

    info!("Bucket created: {}", bucket);

    // S3 CreateBucket returns empty body with Location header
    Response::builder()
        .status(StatusCode::OK)
        .header(header::LOCATION, format!("/{}", bucket))
        .body(Body::empty())
        .unwrap()
}

/// Deletes a bucket.
///
/// S3 API: DELETE /{bucket}
///
/// # Returns
///
/// - 204 No Content if bucket deleted successfully
/// - 404 Not Found if bucket doesn't exist
/// - 409 Conflict if bucket is not empty
pub async fn delete_bucket(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
) -> impl IntoResponse {
    info!("DeleteBucket: {}", bucket);

    let storage = state.storage.read().await;
    let bucket_marker_key = format!("__s4_bucket_marker_{}", bucket);

    // Check if bucket exists
    if storage.head_object("__system__", &bucket_marker_key).await.is_err() {
        return S3Error::NoSuchBucket.into_response();
    }

    // Check if bucket is empty (must have no objects, versions, or delete markers)
    // Per S3 spec: all object versions AND delete markers must be removed before deletion
    let objects = storage.list_objects(&bucket, "", 1).await;
    if let Ok(list) = objects {
        if !list.is_empty() {
            return S3Error::BucketNotEmpty.into_response();
        }
    }

    // Delete bucket marker
    if let Err(e) = storage.delete_object("__system__", &bucket_marker_key).await {
        error!("Failed to delete bucket marker: {:?}", e);
        return S3Error::InternalError("Failed to delete bucket".to_string()).into_response();
    }

    // Clean up bucket configurations
    let config_keys = [
        format!("__s4_bucket_cors_{}", bucket),
        format!("__s4_bucket_versioning_{}", bucket),
        format!("__s4_bucket_lifecycle_{}", bucket),
        format!("__s4_bucket_policy_{}", bucket),
    ];
    for key in &config_keys {
        let _ = storage.delete_object("__system__", key).await;
    }

    info!("Bucket deleted: {}", bucket);
    StatusCode::NO_CONTENT.into_response()
}

/// Lists all buckets.
///
/// S3 API: GET /
///
/// # Returns
///
/// XML response with list of buckets.
pub async fn list_buckets(State(state): State<AppState>) -> impl IntoResponse {
    debug!("ListBuckets");

    let storage = state.storage.read().await;

    // List all bucket markers
    let markers = storage.list_objects("__system__", "__s4_bucket_marker_", 1000).await;

    let buckets: Vec<String> = match markers {
        Ok(list) => list
            .into_iter()
            .filter_map(|(key, _)| key.strip_prefix("__s4_bucket_marker_").map(|s| s.to_string()))
            .collect(),
        Err(e) => {
            error!("Failed to list buckets: {:?}", e);
            Vec::new()
        }
    };

    let xml_response = xml::list_buckets_response(&buckets);

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/xml")
        .body(Body::from(xml_response))
        .unwrap()
}

/// Checks if a bucket exists.
///
/// S3 API: HEAD /{bucket}
///
/// # Returns
///
/// - 200 OK if bucket exists
/// - 404 Not Found if bucket doesn't exist
pub async fn head_bucket(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
) -> impl IntoResponse {
    debug!("HeadBucket: {}", bucket);

    let storage = state.storage.read().await;
    let bucket_marker_key = format!("__s4_bucket_marker_{}", bucket);

    if storage.head_object("__system__", &bucket_marker_key).await.is_ok() {
        StatusCode::OK.into_response()
    } else {
        S3Error::NoSuchBucket.into_response()
    }
}

/// Lists objects in a bucket.
///
/// S3 API: GET /{bucket}
///
/// # Returns
///
/// XML response with list of objects.
pub async fn list_objects(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
    Query(params): Query<ListObjectsQuery>,
) -> impl IntoResponse {
    debug!("ListObjects: bucket={}, params={:?}", bucket, params);

    let storage = state.storage.read().await;
    let bucket_marker_key = format!("__s4_bucket_marker_{}", bucket);

    // Check if bucket exists
    if storage.head_object("__system__", &bucket_marker_key).await.is_err() {
        return S3Error::NoSuchBucket.into_response();
    }

    let prefix = params.prefix.unwrap_or_default();
    let max_keys = match &params.max_keys {
        Some(s) => match s.parse::<isize>() {
            Ok(n) if n >= 0 => (n as usize).min(1000),
            _ => {
                return S3Error::InvalidArgument(
                    "Argument max-keys must be an integer between 0 and 2147483647".to_string(),
                )
                .into_response()
            }
        },
        None => 1000,
    };

    // Determine start_after for pagination:
    // - ListObjectsV2: use continuation-token (which is the last key from previous page)
    //   or start-after parameter
    // - ListObjects v1: use marker
    let is_v2 = params.list_type.as_deref() == Some("2");
    let start_after = if is_v2 {
        params.continuation_token.as_deref().or(params.start_after.as_deref())
    } else {
        params.marker.as_deref()
    };

    // Fetch max_keys + 1 to determine if there are more results (proper truncation detection)
    let fetch_limit = max_keys + 1;
    let mut objects = if let Some(start_after_key) = start_after {
        match storage.list_objects_after(&bucket, &prefix, start_after_key, fetch_limit).await {
            Ok(list) => list,
            Err(e) => {
                error!("Failed to list objects: {:?}", e);
                return S3Error::InternalError("Failed to list objects".to_string())
                    .into_response();
            }
        }
    } else {
        match storage.list_objects(&bucket, &prefix, fetch_limit).await {
            Ok(list) => list,
            Err(e) => {
                error!("Failed to list objects: {:?}", e);
                return S3Error::InternalError("Failed to list objects".to_string())
                    .into_response();
            }
        }
    };

    // Determine if results are truncated (more items exist beyond max_keys)
    let is_truncated = objects.len() > max_keys;

    // Trim to max_keys if we fetched extra for truncation detection
    if objects.len() > max_keys {
        objects.truncate(max_keys);
    }

    // Strip version IDs from keys (S4 internal format: "key#version_id")
    // ListObjects should return only the latest version of each object, with clean keys
    let objects = strip_version_ids_from_keys(objects);

    let xml_response = if is_v2 {
        xml::list_objects_v2_response(
            &bucket,
            &prefix,
            params.delimiter.as_deref(),
            max_keys,
            is_truncated,
            &objects,
            params.continuation_token.as_deref(),
        )
    } else {
        xml::list_objects_response(
            &bucket,
            &prefix,
            params.delimiter.as_deref(),
            max_keys,
            is_truncated,
            &objects,
        )
    };

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/xml")
        .body(Body::from(xml_response))
        .unwrap()
}

/// Lists all versions of objects in a bucket.
///
/// S3 API: GET /{bucket}?versions
///
/// # Query Parameters
///
/// - `prefix`: Limits the response to keys that begin with the specified prefix
/// - `delimiter`: A character you use to group keys
/// - `max-keys`: Sets the maximum number of keys returned (default: 1000)
/// - `key-marker`: Specifies the key to start with when listing
/// - `version-id-marker`: Specifies the object version to start with (requires key-marker)
///
/// # Returns
///
/// XML response with list of object versions and delete markers.
pub async fn list_object_versions(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
    Query(params): Query<ListObjectVersionsQuery>,
) -> impl IntoResponse {
    debug!("ListObjectVersions: bucket={}, params={:?}", bucket, params);

    // Validate: version-id-marker requires key-marker
    if params.version_id_marker.is_some() && params.key_marker.is_none() {
        return S3Error::InvalidArgument(
            "A version-id marker cannot be specified without a key marker.".to_string(),
        )
        .into_response();
    }

    // Validate encoding-type
    if let Some(ref encoding_type) = params.encoding_type {
        if !encoding_type.is_empty() && encoding_type != "url" {
            return S3Error::InvalidArgument(format!(
                "Invalid Encoding Method specified in Request: {}",
                encoding_type
            ))
            .into_response();
        }
    }

    let storage = state.storage.read().await;
    let bucket_marker_key = format!("__s4_bucket_marker_{}", bucket);

    // Check if bucket exists
    if storage.head_object("__system__", &bucket_marker_key).await.is_err() {
        return S3Error::NoSuchBucket.into_response();
    }

    let prefix = params.prefix.unwrap_or_default();
    let max_keys = match &params.max_keys {
        Some(s) => match s.parse::<isize>() {
            Ok(n) if n >= 0 => (n as usize).min(1000),
            _ => {
                return S3Error::InvalidArgument(
                    "Argument max-keys must be an integer between 0 and 2147483647".to_string(),
                )
                .into_response()
            }
        },
        None => 1000,
    };

    // List object versions
    let mut result = match storage
        .list_object_versions(
            &bucket,
            &prefix,
            params.key_marker.as_deref(),
            params.version_id_marker.as_deref(),
            max_keys,
        )
        .await
    {
        Ok(r) => r,
        Err(e) => {
            error!("Failed to list object versions: {:?}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to list object versions",
            )
                .into_response();
        }
    };

    // Apply delimiter filtering: group keys into common prefixes
    let delimiter = params.delimiter.as_deref();
    if let Some(delim) = delimiter {
        let mut common_prefixes = std::collections::BTreeSet::new();

        result.versions.retain(|v| {
            let after_prefix = &v.key[prefix.len()..];
            if let Some(pos) = after_prefix.find(delim) {
                let cp = format!("{}{}", prefix, &after_prefix[..pos + delim.len()]);
                common_prefixes.insert(cp);
                false
            } else {
                true
            }
        });

        result.delete_markers.retain(|dm| {
            let after_prefix = &dm.key[prefix.len()..];
            if let Some(pos) = after_prefix.find(delim) {
                let cp = format!("{}{}", prefix, &after_prefix[..pos + delim.len()]);
                common_prefixes.insert(cp);
                false
            } else {
                true
            }
        });

        result.common_prefixes = common_prefixes.into_iter().collect();
    }

    let xml_response =
        xml::list_object_versions_response(&bucket, &prefix, delimiter, max_keys, &result);

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/xml")
        .body(Body::from(xml_response))
        .unwrap()
}

/// Deletes multiple objects from a bucket (batch delete).
///
/// S3 API: POST /{bucket}?delete
///
/// # Request Format
///
/// XML body with up to 1000 objects to delete:
/// ```xml
/// <Delete>
///   <Object>
///     <Key>file.txt</Key>
///     <VersionId>optional-version-id</VersionId>
///   </Object>
///   <Quiet>true|false</Quiet>
/// </Delete>
/// ```
///
/// # Returns
///
/// Always returns 200 OK with XML response containing deleted objects and errors.
pub async fn delete_objects(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    info!("DeleteObjects: bucket={}, body_len={}", bucket, body.len());

    // Validate Content-MD5 if provided
    if let Err(response) = validate_content_md5(&headers, &body) {
        return response;
    }

    // Parse XML
    let xml = String::from_utf8_lossy(&body);
    let request = match parse_delete_objects_xml(&xml) {
        Ok(req) => req,
        Err(e) => return e.into_response(),
    };

    let versioning_status = get_bucket_versioning_status(&state, &bucket).await;
    let storage = state.storage.read().await;

    // Check bucket exists
    let bucket_marker_key = format!("__s4_bucket_marker_{}", bucket);
    if storage.head_object("__system__", &bucket_marker_key).await.is_err() {
        return S3Error::NoSuchBucket.into_response();
    }

    let bypass_governance = is_bypass_governance(&headers);

    let mut result = DeleteObjectsResult {
        deleted: Vec::new(),
        errors: Vec::new(),
    };

    // Delete objects sequentially
    for obj in request.objects {
        match delete_single_object(
            &*storage,
            &bucket,
            &obj.key,
            obj.version_id.as_deref(),
            versioning_status,
            bypass_governance,
        )
        .await
        {
            Ok(deleted) => result.deleted.push(deleted),
            Err(error) => result.errors.push(error),
        }
    }

    let xml_response = xml::delete_objects_response(&result, request.quiet);

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/xml")
        .body(Body::from(xml_response))
        .unwrap()
}

/// Deletes a single object within batch delete operation.
async fn delete_single_object(
    storage: &impl StorageEngine,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
    versioning_status: s4_core::VersioningStatus,
    bypass_governance: bool,
) -> Result<DeletedObject, DeleteError> {
    // Check Object Lock
    if let Err(lock_error) =
        check_object_locks(storage, bucket, key, version_id, bypass_governance).await
    {
        return Err(DeleteError {
            key: key.to_string(),
            version_id: version_id.map(|s| s.to_string()),
            code: "AccessDenied".to_string(),
            message: lock_error,
        });
    }

    // Delete object
    match storage
        .delete_object_versioned(bucket, key, version_id, versioning_status)
        .await
    {
        Ok(delete_result) => Ok(DeletedObject {
            key: key.to_string(),
            version_id: delete_result.version_id.clone(),
            delete_marker: delete_result.delete_marker,
            delete_marker_version_id: if delete_result.delete_marker {
                delete_result.version_id
            } else {
                None
            },
        }),
        Err(s4_core::StorageError::ObjectNotFound { .. }) => {
            // S3 behavior: Deleting non-existent object is success
            Ok(DeletedObject {
                key: key.to_string(),
                version_id: None,
                delete_marker: false,
                delete_marker_version_id: None,
            })
        }
        Err(s4_core::StorageError::VersionNotFound { .. }) => Err(DeleteError {
            key: key.to_string(),
            version_id: version_id.map(|s| s.to_string()),
            code: "NoSuchVersion".to_string(),
            message: "Version not found".to_string(),
        }),
        Err(e) => Err(DeleteError {
            key: key.to_string(),
            version_id: version_id.map(|s| s.to_string()),
            code: "InternalError".to_string(),
            message: format!("Failed to delete: {}", e),
        }),
    }
}

/// Checks Object Lock constraints before deletion.
async fn check_object_locks(
    storage: &impl StorageEngine,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
    bypass_governance: bool,
) -> Result<(), String> {
    // Normalize "null" version ID to None (unversioned objects use "null" in ListObjectVersions)
    let version_id = version_id.filter(|v| *v != "null");

    let record = if let Some(vid) = version_id {
        storage.head_object_version(bucket, key, vid).await
    } else {
        storage.head_object(bucket, key).await
    };

    let record = match record {
        Ok(r) => r,
        Err(s4_core::StorageError::ObjectNotFound { .. })
        | Err(s4_core::StorageError::DeleteMarker { .. })
        | Err(s4_core::StorageError::VersionNotFound { .. }) => return Ok(()),
        Err(e) => return Err(format!("Failed to check locks: {}", e)),
    };

    if record.legal_hold {
        return Err("Object has legal hold enabled".to_string());
    }

    if let (Some(mode), Some(retain_until)) = (record.retention_mode, record.retain_until_timestamp)
    {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;

        if now < retain_until {
            // GOVERNANCE mode can be bypassed with the header
            let blocked = match mode {
                RetentionMode::GOVERNANCE => !bypass_governance,
                RetentionMode::COMPLIANCE => true,
            };
            if blocked {
                return Err(format!(
                    "Object retention period active until {}",
                    retain_until
                ));
            }
        }
    }

    Ok(())
}

/// Validates Content-MD5 header against request body.
#[allow(clippy::result_large_err)]
pub fn validate_content_md5(headers: &HeaderMap, body: &[u8]) -> Result<(), Response> {
    if let Some(content_md5) = headers.get("content-md5") {
        let provided = content_md5.to_str().unwrap_or("");
        let digest = md5::compute(body);
        let computed = base64::engine::general_purpose::STANDARD.encode(digest.as_ref());

        if provided != computed {
            return Err(S3Error::BadDigest.into_response());
        }
    }
    Ok(())
}

/// Strips version IDs from object keys and returns only unique keys (latest version).
///
/// S4 stores versioned objects with keys like "key#version_id". For ListObjects,
/// we need to return clean keys without version suffixes and only include each
/// unique key once (representing the latest version).
fn strip_version_ids_from_keys(
    objects: Vec<(String, s4_core::types::IndexRecord)>,
) -> Vec<(String, s4_core::types::IndexRecord)> {
    use std::collections::HashMap;

    // Group all versions by clean key
    let mut grouped: HashMap<String, Vec<(String, s4_core::types::IndexRecord)>> = HashMap::new();

    for (key, record) in objects {
        // Split key at '#' to separate object key from version ID
        let clean_key = if let Some(pos) = key.find('#') {
            key[..pos].to_string()
        } else {
            key.clone()
        };

        grouped.entry(clean_key).or_default().push((key, record));
    }

    // For each group, find the latest version (max modified_at timestamp)
    let mut result = Vec::new();

    for (clean_key, mut versions) in grouped {
        // Sort by modified_at descending (newest first)
        versions.sort_by(|a, b| b.1.modified_at.cmp(&a.1.modified_at));

        // Get the latest version (first after sorting)
        if let Some((_, latest_record)) = versions.first() {
            // Skip if latest version is a delete marker
            // (objects with delete markers as latest version are considered "deleted")
            if !latest_record.is_delete_marker {
                result.push((clean_key.clone(), latest_record.clone()));
            }
        }
    }

    // Re-sort by key to restore lexicographic order (HashMap iteration is unordered)
    result.sort_by(|a, b| a.0.cmp(&b.0));

    result
}

/// Validates bucket name according to S3 rules .
fn validate_bucket_name(name: &str) -> Result<(), S3Error> {
    if name.len() < 3 || name.len() > 63 {
        return Err(S3Error::InvalidRequest(
            "Bucket name must be between 3 and 63 characters".to_string(),
        ));
    }

    // Must start with lowercase letter or number
    if !name
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
    {
        return Err(S3Error::InvalidRequest(
            "Bucket name must start with a lowercase letter or number".to_string(),
        ));
    }

    // Only lowercase letters, numbers, hyphens, and dots
    if !name
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '.')
    {
        return Err(S3Error::InvalidRequest(
            "Bucket name can only contain lowercase letters, numbers, hyphens, and dots"
                .to_string(),
        ));
    }

    // Cannot end with hyphen
    if name.ends_with('-') {
        return Err(S3Error::InvalidRequest(
            "Bucket name cannot end with a hyphen".to_string(),
        ));
    }

    Ok(())
}

/// Parses DeleteObjects request XML.
///
/// Expected format:
/// ```xml
/// <Delete>
///   <Object>
///     <Key>file.txt</Key>
///     <VersionId>optional-version-id</VersionId>
///   </Object>
///   <Quiet>true|false</Quiet>
/// </Delete>
/// ```
fn parse_delete_objects_xml(xml: &str) -> Result<DeleteObjectsRequest, S3Error> {
    use quick_xml::events::Event;
    use quick_xml::Reader;

    let mut reader = Reader::from_str(xml);
    reader.trim_text(true);

    let mut objects = Vec::new();
    let mut quiet = false;
    let mut current_element = String::new();
    let mut current_key: Option<String> = None;
    let mut current_version_id: Option<String> = None;
    let mut in_object = false;
    let mut buf = Vec::new();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => {
                current_element = String::from_utf8_lossy(e.name().as_ref()).to_string();
                if current_element == "Object" {
                    in_object = true;
                    current_key = None;
                    current_version_id = None;
                }
            }
            Ok(Event::Text(e)) => {
                let text = e.unescape().map_err(|_| S3Error::MalformedXML)?.to_string();

                if in_object {
                    match current_element.as_str() {
                        "Key" => current_key = Some(text),
                        "VersionId" => current_version_id = Some(text),
                        _ => {}
                    }
                } else if current_element == "Quiet" {
                    quiet = text.to_lowercase() == "true";
                }
            }
            Ok(Event::End(e)) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                if name == "Object" {
                    if let Some(key) = current_key.take() {
                        objects.push(ObjectIdentifier {
                            key,
                            version_id: current_version_id.take(),
                        });
                    }
                    in_object = false;
                }
            }
            Ok(Event::Eof) => break,
            Err(_) => return Err(S3Error::MalformedXML),
            _ => {}
        }
        buf.clear();
    }

    if objects.len() > 1000 {
        return Err(S3Error::InvalidRequest(
            "Cannot delete more than 1000 objects".to_string(),
        ));
    }

    if objects.is_empty() {
        return Err(S3Error::MalformedXML);
    }

    Ok(DeleteObjectsRequest { objects, quiet })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_bucket_name() {
        // Valid names
        assert!(validate_bucket_name("my-bucket").is_ok());
        assert!(validate_bucket_name("bucket123").is_ok());
        assert!(validate_bucket_name("123bucket").is_ok());

        // Invalid names
        assert!(validate_bucket_name("ab").is_err()); // Too short
        assert!(validate_bucket_name("My-Bucket").is_err()); // Uppercase
        assert!(validate_bucket_name("bucket_name").is_err()); // Underscore
        assert!(validate_bucket_name("bucket-").is_err()); // Ends with hyphen
    }

    #[test]
    fn test_parse_delete_objects_basic() {
        let xml = r#"<Delete>
  <Object><Key>file1.txt</Key></Object>
  <Object><Key>file2.txt</Key></Object>
  <Quiet>false</Quiet>
</Delete>"#;

        let request = parse_delete_objects_xml(xml).unwrap();
        assert_eq!(request.objects.len(), 2);
        assert_eq!(request.objects[0].key, "file1.txt");
        assert_eq!(request.objects[1].key, "file2.txt");
        assert!(!request.quiet);
    }

    #[test]
    fn test_parse_delete_objects_with_versions() {
        let xml = r#"<Delete>
  <Object>
    <Key>file.txt</Key>
    <VersionId>abc123</VersionId>
  </Object>
</Delete>"#;

        let request = parse_delete_objects_xml(xml).unwrap();
        assert_eq!(request.objects.len(), 1);
        assert_eq!(request.objects[0].key, "file.txt");
        assert_eq!(request.objects[0].version_id, Some("abc123".to_string()));
    }

    #[test]
    fn test_parse_delete_objects_too_many() {
        let mut xml = String::from("<Delete>");
        for i in 0..1001 {
            xml.push_str(&format!("<Object><Key>file{}.txt</Key></Object>", i));
        }
        xml.push_str("</Delete>");

        assert!(parse_delete_objects_xml(&xml).is_err());
    }
}
