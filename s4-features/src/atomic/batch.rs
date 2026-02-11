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

//! Atomic batch operations.

use s4_core::StorageError;

/// Performs atomic batch operations.
pub async fn atomic_batch_operation(
    _storage: &dyn s4_core::StorageEngine,
    _operations: &[BatchOperation],
) -> Result<(), StorageError> {
    // TODO: Implement in Phase 4
    Err(StorageError::InvalidData("Not implemented".to_string()))
}

/// Batch operation type.
#[derive(Debug, Clone)]
pub enum BatchOperation {
    /// Put operation to upload an object.
    Put {
        /// Bucket name.
        bucket: String,
        /// Object key.
        key: String,
        /// Object data.
        data: Vec<u8>,
    },
    /// Delete operation to remove an object.
    Delete {
        /// Bucket name.
        bucket: String,
        /// Object key.
        key: String,
    },
}
