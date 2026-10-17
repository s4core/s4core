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

//! Core data types for storage.

pub mod blob;
pub mod composite;
pub mod metadata;
pub mod object_lock;
pub mod storage_class;
pub mod versioning;

pub use blob::{BlobHeader, BlobHeaderFlags};
pub use composite::{
    BlobId, BlobLocation, BlobRefEntry, CompositeManifest, ManifestSegmentRef, MultipartPartRecord,
    MultipartUploadSession, MultipartUploadState, ObjectLayout, MANIFEST_FORMAT_VERSION,
};
pub use metadata::{
    is_bucket_config_key, is_bucket_marker, is_bucket_metadata, is_bucket_metadata_full_key,
    is_cluster_control, is_cluster_control_full_key, is_replicated_key, IndexRecord,
    BUCKET_CONFIG_KEY_PREFIXES, BUCKET_MARKER_KEY_PREFIX, CORS_CONFIG_KEY_PREFIX,
    DELETE_MARKER_FILE_ID, ENCRYPTION_CONFIG_KEY_PREFIX, EXTERNAL_DATA_FILE_ID,
    IAM_LEGACY_USER_KEY_PREFIX, IAM_V2_KEY_PREFIX, LIFECYCLE_CONFIG_KEY_PREFIX,
    OBJECT_LOCK_CONFIG_KEY_PREFIX, OBJECT_TAGS_METADATA_KEY, POLICY_CONFIG_KEY_PREFIX,
    SYSTEM_BUCKET, VERSIONING_CONFIG_KEY_PREFIX,
};
pub use object_lock::{DefaultRetention, RetentionMode};
pub use storage_class::{StorageClass, S4_STORAGE_CLASS_METADATA_KEY};
pub use versioning::VersioningStatus;
