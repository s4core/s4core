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

//! Storage engine trait and implementation.

use crate::error::StorageError;
use crate::types::{DefaultRetention, IndexRecord, VersioningStatus};
use async_trait::async_trait;

// ============================================================================
// Versioning Types
// ============================================================================

/// Result of a versioned delete operation.
#[derive(Debug, Clone)]
pub struct DeleteResult {
    /// True if a delete marker was created.
    pub delete_marker: bool,
    /// Version ID of the delete marker or permanently deleted version.
    pub version_id: Option<String>,
}

/// A single object version entry.
#[derive(Debug, Clone)]
pub struct ObjectVersion {
    /// Object key.
    pub key: String,
    /// Version ID.
    pub version_id: String,
    /// Whether this is the latest version.
    pub is_latest: bool,
    /// Last modified timestamp (nanoseconds since Unix epoch).
    pub last_modified: u64,
    /// ETag of the object.
    pub etag: String,
    /// Size in bytes.
    pub size: u64,
}

/// A delete marker entry.
#[derive(Debug, Clone)]
pub struct DeleteMarkerEntry {
    /// Object key.
    pub key: String,
    /// Version ID of the delete marker.
    pub version_id: String,
    /// Whether this is the latest version (delete marker is current).
    pub is_latest: bool,
    /// Timestamp when the delete marker was created.
    pub last_modified: u64,
}

/// Result of ListObjectVersions API.
#[derive(Debug, Clone, Default)]
pub struct ListVersionsResult {
    /// Object versions (actual data entries).
    pub versions: Vec<ObjectVersion>,
    /// Delete markers.
    pub delete_markers: Vec<DeleteMarkerEntry>,
    /// True if there are more results.
    pub is_truncated: bool,
    /// Next key marker for pagination.
    pub next_key_marker: Option<String>,
    /// Next version ID marker for pagination.
    pub next_version_id_marker: Option<String>,
}

/// Main storage engine interface.
///
/// This trait defines the core operations for storing and retrieving objects.
#[async_trait]
pub trait StorageEngine: Send + Sync {
    /// Writes an object to storage.
    ///
    /// # Arguments
    ///
    /// * `bucket` - Bucket name
    /// * `key` - Object key (path)
    /// * `data` - Object data
    /// * `content_type` - MIME type
    /// * `metadata` - User-defined metadata
    ///
    /// # Returns
    ///
    /// Returns the ETag of the stored object.
    async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        data: &[u8],
        content_type: &str,
        metadata: &std::collections::HashMap<String, String>,
    ) -> Result<String, StorageError>;

    /// Reads an object from storage.
    ///
    /// # Arguments
    ///
    /// * `bucket` - Bucket name
    /// * `key` - Object key
    ///
    /// # Returns
    ///
    /// Returns the object data and metadata.
    async fn get_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<(Vec<u8>, IndexRecord), StorageError>;

    /// Deletes an object from storage.
    ///
    /// # Arguments
    ///
    /// * `bucket` - Bucket name
    /// * `key` - Object key
    async fn delete_object(&self, bucket: &str, key: &str) -> Result<(), StorageError>;

    /// Gets object metadata without reading the data.
    ///
    /// # Arguments
    ///
    /// * `bucket` - Bucket name
    /// * `key` - Object key
    ///
    /// # Returns
    ///
    /// Returns the object metadata.
    async fn head_object(&self, bucket: &str, key: &str) -> Result<IndexRecord, StorageError>;

    /// Lists objects in a bucket with the given prefix.
    ///
    /// # Arguments
    ///
    /// * `bucket` - Bucket name
    /// * `prefix` - Key prefix to filter by
    /// * `max_keys` - Maximum number of keys to return
    ///
    /// # Returns
    ///
    /// Returns a vector of (key, metadata) pairs.
    async fn list_objects(
        &self,
        bucket: &str,
        prefix: &str,
        max_keys: usize,
    ) -> Result<Vec<(String, IndexRecord)>, StorageError>;

    // ========================================================================
    // Versioning Methods
    // ========================================================================

    /// Writes an object with versioning support.
    ///
    /// # Arguments
    ///
    /// * `bucket` - Bucket name
    /// * `key` - Object key
    /// * `data` - Object data
    /// * `content_type` - MIME type
    /// * `metadata` - User-defined metadata
    /// * `versioning_status` - Current versioning status of the bucket
    ///
    /// # Returns
    ///
    /// Returns (ETag, `Option<VersionId>`). Version ID is:
    /// - `None` if versioning is `Unversioned`
    /// - A UUID if versioning is `Enabled`
    /// - `"null"` if versioning is `Suspended`
    async fn put_object_versioned(
        &self,
        bucket: &str,
        key: &str,
        data: &[u8],
        content_type: &str,
        metadata: &std::collections::HashMap<String, String>,
        versioning_status: VersioningStatus,
    ) -> Result<(String, Option<String>), StorageError>;

    /// Gets a specific version of an object.
    ///
    /// # Arguments
    ///
    /// * `bucket` - Bucket name
    /// * `key` - Object key
    /// * `version_id` - Version ID to retrieve
    ///
    /// # Returns
    ///
    /// Returns the object data and metadata.
    ///
    /// # Errors
    ///
    /// - `VersionNotFound` if the version doesn't exist
    /// - `DeleteMarker` if the version is a delete marker
    async fn get_object_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<(Vec<u8>, IndexRecord), StorageError>;

    /// Deletes an object with versioning support.
    ///
    /// # Arguments
    ///
    /// * `bucket` - Bucket name
    /// * `key` - Object key
    /// * `version_id` - Optional version ID to delete permanently
    /// * `versioning_status` - Current versioning status of the bucket
    ///
    /// # Returns
    ///
    /// Returns `DeleteResult` indicating:
    /// - If `delete_marker` is true, a delete marker was created
    /// - `version_id` contains the ID of the delete marker or deleted version
    async fn delete_object_versioned(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        versioning_status: VersioningStatus,
    ) -> Result<DeleteResult, StorageError>;

    /// Gets metadata for a specific version.
    ///
    /// # Arguments
    ///
    /// * `bucket` - Bucket name
    /// * `key` - Object key
    /// * `version_id` - Version ID to retrieve metadata for
    ///
    /// # Returns
    ///
    /// Returns the object metadata (IndexRecord).
    async fn head_object_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<IndexRecord, StorageError>;

    /// Lists all versions of objects in a bucket.
    ///
    /// # Arguments
    ///
    /// * `bucket` - Bucket name
    /// * `prefix` - Key prefix to filter by
    /// * `key_marker` - Start listing after this key
    /// * `version_id_marker` - Start listing after this version (requires key_marker)
    /// * `max_keys` - Maximum number of versions to return
    ///
    /// # Returns
    ///
    /// Returns `ListVersionsResult` containing versions and delete markers.
    async fn list_object_versions(
        &self,
        bucket: &str,
        prefix: &str,
        key_marker: Option<&str>,
        version_id_marker: Option<&str>,
        max_keys: usize,
    ) -> Result<ListVersionsResult, StorageError>;

    // ========================================================================
    // Object Lock Methods (Phase 3)
    // ========================================================================

    /// Writes an object with versioning and optional default retention.
    ///
    /// This method combines versioning with Object Lock support.
    ///
    /// # Arguments
    ///
    /// * `bucket` - Bucket name
    /// * `key` - Object key
    /// * `data` - Object data
    /// * `content_type` - MIME type
    /// * `metadata` - User-defined metadata
    /// * `versioning_status` - Current versioning status of the bucket
    /// * `default_retention` - Optional default retention to apply
    ///
    /// # Returns
    ///
    /// Returns (ETag, `Option<VersionId>`). Version ID is:
    /// - `None` if versioning is `Unversioned`
    /// - A UUID if versioning is `Enabled`
    /// - `"null"` if versioning is `Suspended`
    ///
    /// # Behavior
    ///
    /// If `default_retention` is provided:
    /// - The created IndexRecord will have `retention_mode` and `retain_until_timestamp` set
    /// - Retention is calculated from creation time + days
    /// - Only applies to new objects (not updates)
    #[allow(clippy::too_many_arguments)]
    async fn put_object_with_retention(
        &self,
        bucket: &str,
        key: &str,
        data: &[u8],
        content_type: &str,
        metadata: &std::collections::HashMap<String, String>,
        versioning_status: VersioningStatus,
        default_retention: Option<DefaultRetention>,
    ) -> Result<(String, Option<String>), StorageError>;

    /// Updates object metadata for a specific version (for retention/legal hold changes).
    ///
    /// This method updates ONLY the IndexRecord in the metadata database.
    /// The actual object data in volumes remains unchanged.
    ///
    /// # Arguments
    ///
    /// * `bucket` - Bucket name
    /// * `key` - Object key
    /// * `version_id` - Version ID to update
    /// * `updated_record` - New IndexRecord with updated metadata
    ///
    /// # Errors
    ///
    /// Returns `VersionNotFound` if the version doesn't exist.
    ///
    /// # Safety
    ///
    /// This method should only be used for updating Object Lock fields:
    /// - `retention_mode`
    /// - `retain_until_timestamp`
    /// - `legal_hold`
    ///
    /// Updating other fields may cause inconsistencies with volume data.
    async fn update_object_metadata(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
        updated_record: IndexRecord,
    ) -> Result<(), StorageError>;
}
