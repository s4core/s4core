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
