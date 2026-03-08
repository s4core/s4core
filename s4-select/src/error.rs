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

//! Error types for the S4 Select engine.

use thiserror::Error;

/// Errors that can occur during S3 Select / S4 SQL query execution.
#[derive(Error, Debug)]
pub enum SelectError {
    /// The SQL expression is invalid or could not be parsed.
    #[error("Invalid SQL expression: {0}")]
    InvalidExpression(String),

    /// The input or output format is not supported.
    #[error("Unsupported format: {0}")]
    UnsupportedFormat(String),

    /// The query execution failed (DataFusion error).
    #[error("Query execution failed: {0}")]
    ExecutionError(String),

    /// The query exceeded the configured memory limit.
    #[error("Memory limit exceeded: used {used} bytes, limit {limit} bytes")]
    MemoryLimitExceeded {
        /// Bytes of memory used.
        used: usize,
        /// Configured memory limit in bytes.
        limit: usize,
    },

    /// The XML request body is malformed or missing required fields.
    #[error("Invalid request: {0}")]
    InvalidRequest(String),

    /// Failed to parse the object data in the declared format.
    #[error("Data parsing error: {0}")]
    DataParseError(String),

    /// The requested object was not found in storage.
    #[error("Object not found: {bucket}/{key}")]
    ObjectNotFound {
        /// Bucket name.
        bucket: String,
        /// Object key.
        key: String,
    },

    /// The query timed out.
    #[error("Query timed out after {0} seconds")]
    Timeout(u64),

    /// Only SELECT statements are allowed.
    #[error("Only SELECT statements are allowed, got: {0}")]
    NonSelectStatement(String),

    /// No objects matched the glob pattern.
    #[error("No objects matched the pattern: {0}")]
    NoMatchingObjects(String),

    /// Internal error.
    #[error("Internal error: {0}")]
    Internal(String),
}

impl From<arrow::error::ArrowError> for SelectError {
    fn from(err: arrow::error::ArrowError) -> Self {
        SelectError::DataParseError(err.to_string())
    }
}
