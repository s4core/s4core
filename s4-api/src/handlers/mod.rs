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

//! HTTP request handlers for S3-compatible API.
//!
//! This module provides handlers for:
//! - Bucket operations (create, delete, list, head)
//! - Bucket configuration (CORS, versioning, lifecycle, Object Lock)
//! - Object operations (put, get, delete, head)
//! - Object Lock operations (retention, legal hold)
//! - Multipart upload operations (create, upload part, complete, abort)
//! - Admin API operations (user management, IAM)

pub mod admin;
pub mod bucket;
pub mod bucket_config;
pub mod multipart;
pub mod object;
pub mod object_lock;
pub mod stats;

pub use bucket::{
    create_bucket, delete_bucket, delete_objects, head_bucket, list_buckets, list_object_versions,
    list_objects, DeleteError, DeleteObjectsResult, DeletedObject, ListObjectVersionsQuery,
    ListObjectsQuery,
};
pub use bucket_config::{
    delete_bucket_cors, delete_bucket_lifecycle, get_bucket_cors, get_bucket_lifecycle,
    get_bucket_location, get_bucket_versioning, get_bucket_versioning_status, get_cors_config,
    is_versioning_enabled, put_bucket_cors, put_bucket_lifecycle, put_bucket_versioning,
};
pub use multipart::{
    abort_multipart_upload, complete_multipart_upload, create_multipart_upload, upload_part,
};
pub use object::{
    delete_object, get_object, get_object_tagging, head_object, put_object, ObjectVersionQuery,
};
pub use object_lock::{
    get_bucket_object_lock_configuration, get_object_legal_hold, get_object_retention,
    put_bucket_object_lock_configuration, put_object_legal_hold, put_object_retention,
};
