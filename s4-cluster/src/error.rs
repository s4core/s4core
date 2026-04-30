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

//! Error types for the cluster subsystem.

use thiserror::Error;

/// Errors that can occur in the cluster subsystem.
#[derive(Error, Debug)]
pub enum ClusterError {
    /// I/O error (file operations, network).
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Codec serialization/deserialization error.
    #[error("codec error: {0}")]
    Codec(String),

    /// Gossip protocol error.
    #[error("gossip protocol error: {0}")]
    Gossip(String),

    /// Cluster configuration is invalid.
    #[error("configuration error: {0}")]
    Config(String),

    /// Node identity error (generation or persistence).
    #[error("node identity error: {0}")]
    Identity(String),

    /// gRPC inter-node communication error.
    #[error("RPC error: {0}")]
    Rpc(String),

    /// TLS configuration error.
    #[error("TLS error: {0}")]
    Tls(String),

    /// Operation exceeds the current license limits (CE or EE tier).
    #[error("license limit: {0}")]
    LicenseLimit(String),

    /// Requested feature requires an Enterprise license.
    #[error("enterprise feature required: {0}")]
    EnterpriseRequired(String),

    /// Data placement error (routing, hash ring, bucket assignment).
    #[error("placement error: {0}")]
    Placement(String),

    /// Quorum write did not reach the required number of replicas.
    #[error("quorum not met: needed {needed}, got {achieved}")]
    QuorumNotMet {
        /// Number of ACKs required for quorum.
        needed: u8,
        /// Number of ACKs actually received.
        achieved: u8,
    },

    /// A quorum operation timed out waiting for replica responses.
    #[error("write timeout after {0:?}")]
    WriteTimeout(std::time::Duration),

    /// Topology epoch mismatch between coordinator and replica.
    #[error("epoch mismatch: coordinator={coordinator}, replica={replica}")]
    EpochMismatch {
        /// Coordinator's epoch.
        coordinator: u64,
        /// Replica's epoch.
        replica: u64,
    },

    /// A quorum read operation timed out waiting for replica responses.
    #[error("read timeout after {0:?}")]
    ReadTimeout(std::time::Duration),

    /// Object not found on any replica.
    #[error("object not found: {bucket}/{key}")]
    ObjectNotFound {
        /// Bucket name.
        bucket: String,
        /// Object key.
        key: String,
    },

    /// Multipart upload session was not found on enough replicas.
    #[error("multipart upload not found: {upload_id}")]
    MultipartUploadNotFound {
        /// Upload ID.
        upload_id: String,
    },

    /// Multipart manifest references parts that are missing or mismatched.
    #[error("invalid multipart part for upload {upload_id}: {message}")]
    InvalidMultipartPart {
        /// Upload ID.
        upload_id: String,
        /// Human-readable validation failure.
        message: String,
    },

    /// A non-final multipart part is smaller than the S3 minimum.
    #[error("multipart part too small: part {part_number}, size {size}")]
    MultipartPartTooSmall {
        /// Part number.
        part_number: u32,
        /// Part size in bytes.
        size: u64,
    },

    /// Local storage engine error during replica write.
    #[error("storage error: {0}")]
    Storage(String),
}
