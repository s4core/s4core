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

//! Version ID generation for S3-compatible versioning.
//!
//! This module provides utilities for generating and managing version IDs
//! that are compatible with the S3 versioning specification.

use uuid::Uuid;

/// The version ID used for suspended versioning mode.
///
/// When versioning is suspended, new objects are assigned the "null" version ID.
/// Multiple overwrites replace the existing "null" version.
pub const NULL_VERSION_ID: &str = "null";

/// Generates a unique, URL-safe version ID.
///
/// Uses UUID v4 for uniqueness and simplicity.
/// The resulting ID is:
/// - UTF-8 encoded
/// - URL-safe (alphanumeric and hyphens only)
/// - 36 characters long (≤1024 bytes, S3 spec compliant)
/// - Globally unique
///
/// # Example
/// ```
/// use s4_core::storage::versioning::generate_version_id;
///
/// let version_id = generate_version_id();
/// assert_eq!(version_id.len(), 36);
/// assert!(version_id.chars().all(|c| c.is_alphanumeric() || c == '-'));
/// ```
pub fn generate_version_id() -> String {
    Uuid::new_v4().to_string()
}

/// Checks if the given version ID is the null version.
///
/// # Arguments
/// * `version_id` - The version ID to check
///
/// # Returns
/// `true` if the version ID is "null", `false` otherwise.
pub fn is_null_version(version_id: &str) -> bool {
    version_id == NULL_VERSION_ID
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn test_generate_version_id_format() {
        let version_id = generate_version_id();

        // UUID v4 format: 8-4-4-4-12 hex characters
        assert_eq!(version_id.len(), 36);

        // Check format
        let parts: Vec<&str> = version_id.split('-').collect();
        assert_eq!(parts.len(), 5);
        assert_eq!(parts[0].len(), 8);
        assert_eq!(parts[1].len(), 4);
        assert_eq!(parts[2].len(), 4);
        assert_eq!(parts[3].len(), 4);
        assert_eq!(parts[4].len(), 12);
    }

    #[test]
    fn test_generate_version_id_uniqueness() {
        let mut ids = HashSet::new();
        for _ in 0..1000 {
            let id = generate_version_id();
            assert!(ids.insert(id), "Generated duplicate version ID");
        }
    }

    #[test]
    fn test_generate_version_id_url_safe() {
        let version_id = generate_version_id();
        assert!(version_id.chars().all(|c| c.is_alphanumeric() || c == '-'));
    }

    #[test]
    fn test_is_null_version() {
        assert!(is_null_version("null"));
        assert!(is_null_version(NULL_VERSION_ID));
        assert!(!is_null_version("abc123"));
        assert!(!is_null_version(""));
        assert!(!is_null_version("NULL"));
    }

    #[test]
    fn test_version_id_s3_compliant() {
        let version_id = generate_version_id();

        // S3 spec: version ID must be ≤1024 bytes
        assert!(version_id.len() <= 1024);

        // S3 spec: version ID must be UTF-8
        assert!(version_id.is_ascii()); // ASCII is valid UTF-8
    }
}
