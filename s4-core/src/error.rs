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

//! Error types for the storage engine.

use thiserror::Error;

/// Errors that can occur in the storage engine.
#[derive(Error, Debug)]
pub enum StorageError {
    /// Volume file not found.
    #[error("Volume not found: {volume_id}")]
    VolumeNotFound {
        /// Volume ID that was not found.
        volume_id: u32,
    },

    /// Object not found in storage.
    #[error("Object not found: {key}")]
    ObjectNotFound {
        /// Object key that was not found.
        key: String,
    },

    /// IO error occurred.
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// Database operation error.
    #[error("Database error: {0}")]
    Database(String),

    /// Serialization/deserialization error.
    #[error("Serialization error: {0}")]
    Serialization(String),

    /// Invalid data format or content.
    #[error("Invalid data: {0}")]
    InvalidData(String),

    /// Volume file is full and cannot accept more data.
    #[error("Volume is full: {volume_id}")]
    VolumeFull {
        /// Volume ID that is full.
        volume_id: u32,
    },

    /// Checksum verification failed.
    #[error("Checksum mismatch for object: {key}")]
    ChecksumMismatch {
        /// Object key with checksum mismatch.
        key: String,
    },

    /// Specific object version not found.
    #[error("Version not found: {key} (version: {version_id})")]
    VersionNotFound {
        /// Object key.
        key: String,
        /// Version ID that was not found.
        version_id: String,
    },

    /// Object is a delete marker (for versioned GET operations).
    #[error("Object is a delete marker: {key}")]
    DeleteMarker {
        /// Object key.
        key: String,
        /// Version ID of the delete marker.
        version_id: String,
    },

    /// Invalid operation attempted.
    #[error("Invalid operation '{operation}': {reason}")]
    InvalidOperation {
        /// Operation that was attempted.
        operation: String,
        /// Reason why the operation is invalid.
        reason: String,
    },
}
