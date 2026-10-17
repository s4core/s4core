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

//! IAM error types.

use thiserror::Error;

/// IAM-related errors.
#[derive(Error, Debug)]
pub enum IamError {
    /// User not found in database
    #[error("User not found")]
    UserNotFound,

    /// Username already exists
    #[error("User already exists")]
    UserAlreadyExists,

    /// Username format is invalid
    #[error("Invalid username (must be 3-32 alphanumeric characters or underscore)")]
    InvalidUsername,

    /// Invalid username or password
    #[error("Invalid credentials")]
    InvalidCredentials,

    /// User lacks required permissions
    #[error("Insufficient permissions")]
    InsufficientPermissions,

    /// Password hashing operation failed
    #[error("Password hashing failed")]
    HashingFailed,

    /// Password hash format is invalid
    #[error("Invalid password hash format")]
    InvalidHash,

    /// JWT token generation failed
    #[error("Token generation failed")]
    TokenGenerationFailed,

    /// JWT token is invalid or expired
    #[error("Invalid or expired token")]
    InvalidToken,

    /// User has no S3 credentials configured
    #[error("No S3 credentials configured for this user")]
    NoS3Credentials,

    /// Too few nodes of the pool answered to read or confirm a change of IAM.
    /// The change may be retried; credentials are still checked against the
    /// node's own copy.
    #[error("IAM is unavailable: {0}")]
    Unavailable(String),

    /// Another change of IAM held the lease of IAM changes for the whole
    /// wait. The change may be retried after `retry_after_secs`.
    #[error("another change of IAM is in progress; retry in {retry_after_secs} s")]
    Busy {
        /// How long the lease the change waited for is still held.
        retry_after_secs: u64,
    },

    /// The pool failed a read or a change of IAM for another reason than too
    /// few nodes answering: a fault to investigate.
    #[error("IAM replication failed: {0}")]
    Cluster(String),

    /// Some node of the pool — this one, or another — keeps IAM of its own
    /// that the pool has not taken over yet: an earlier release, or the node
    /// as a single node, stored users on it alone. IAM is not changed until
    /// the operator decides on it, so that the nodes do not drift further
    /// apart (plan CLUSTER-IAM-REPLICATION.md, decision 8).
    #[error(
        "IAM changes are refused until the users nodes of an earlier release kept of their \
         own are taken over by the cluster: see GET /api/admin/iam/adoption"
    )]
    AdoptionPending,

    /// A decision on the IAM nodes kept of their own names users or a mode
    /// that do not fit the report.
    #[error("Invalid IAM adoption: {0}")]
    AdoptionInvalid(String),

    /// No node of the pool keeps IAM of its own: there is nothing to decide.
    #[error("No node keeps IAM users of its own: there is nothing to take over")]
    AdoptionNotPending,

    /// Some node of the pool did not answer what it keeps of its own: a
    /// decision would not cover it. The request may be retried.
    #[error("IAM adoption needs every node of the pool to answer: {0}")]
    AdoptionIncomplete(String),

    /// The decision names another report than the nodes answer now.
    #[error(
        "The IAM adoption report changed since it was read: read GET /api/admin/iam/adoption \
         again and decide on the new report_id"
    )]
    AdoptionReportChanged,

    /// IAM credentials are not checked on this node for now: its copy of
    /// IAM is not confirmed current with its pool. The request may be retried
    /// through another node; root and the credentials of the environment
    /// still work here.
    #[error("IAM credentials cannot be checked on this node now: {0}; retry through another node")]
    NotServing(super::gate::NotServing),

    /// An IAM encryption key of the configuration is not one.
    #[error("Invalid IAM encryption key: {0}")]
    InvalidSecretKey(String),

    /// A secret of S3 credentials could not be sealed or opened: the record
    /// is corrupted, or its sealed secret was copied from another user.
    #[error("The secret of S3 credentials cannot be sealed or opened: {0}")]
    SecretSealing(String),

    /// A secret of S3 credentials is sealed with a key this node does not
    /// have: `S4_IAM_ENCRYPTION_KEY` or `S4_IAM_ENCRYPTION_KEY_PREVIOUS`
    /// differ between the nodes.
    #[error(
        "The secret of S3 credentials is sealed with key {0}, which this node does not have: \
         set the same S4_IAM_ENCRYPTION_KEY (or S4_IAM_ENCRYPTION_KEY_PREVIOUS) on every node"
    )]
    SecretKeyUnknown(String),

    /// Underlying storage error
    #[error("Storage error: {0}")]
    Storage(#[from] s4_core::error::StorageError),

    /// JSON serialization/deserialization error
    #[error("JSON serialization error: {0}")]
    Json(#[from] serde_json::Error),

    /// UTF-8 decoding error
    #[error("UTF-8 decoding error: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),
}
