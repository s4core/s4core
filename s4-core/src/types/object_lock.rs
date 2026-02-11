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

//! Object Lock types for retention and legal hold.

use serde::{Deserialize, Serialize};
use std::fmt;

/// Default retention settings for a bucket (core type for storage engine).
///
/// This is a simplified version used by the storage engine.
/// The full version with validation is in s4-features.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DefaultRetention {
    /// Retention mode (GOVERNANCE or COMPLIANCE).
    pub mode: RetentionMode,

    /// Number of days to retain objects from creation.
    pub days: u32,
}

impl DefaultRetention {
    /// Creates a new default retention configuration.
    pub fn new(mode: RetentionMode, days: u32) -> Self {
        Self { mode, days }
    }

    /// Calculates the retain-until timestamp from creation timestamp.
    ///
    /// # Arguments
    ///
    /// * `created_at` - Creation timestamp in nanoseconds
    ///
    /// # Returns
    ///
    /// Retain-until timestamp in nanoseconds
    pub fn calculate_retain_until(&self, created_at: u64) -> u64 {
        const NANOS_PER_DAY: u64 = 86_400_000_000_000; // 24 * 60 * 60 * 1_000_000_000
        created_at + (self.days as u64 * NANOS_PER_DAY)
    }
}

/// Retention mode for Object Lock.
///
/// Determines whether retention can be overridden and by whom.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[allow(clippy::upper_case_acronyms)] // AWS S3 API uses uppercase for retention modes
pub enum RetentionMode {
    /// Retention can be overridden with special permission (Phase 5).
    ///
    /// In Phase 3, treated same as COMPLIANCE but designed for future extension.
    GOVERNANCE,

    /// Retention cannot be deleted by anyone until expiration.
    ///
    /// **CRITICAL**: If implemented incorrectly, can create permanently
    /// undeletable files. Phase 3 blocks all modifications to COMPLIANCE retention.
    COMPLIANCE,
}

impl RetentionMode {
    /// Returns the S3 API string representation of this mode.
    pub fn as_str(&self) -> &'static str {
        match self {
            RetentionMode::GOVERNANCE => "GOVERNANCE",
            RetentionMode::COMPLIANCE => "COMPLIANCE",
        }
    }

    /// Parses a retention mode from a string.
    pub fn parse_str(s: &str) -> Option<Self> {
        match s {
            "GOVERNANCE" => Some(RetentionMode::GOVERNANCE),
            "COMPLIANCE" => Some(RetentionMode::COMPLIANCE),
            _ => None,
        }
    }
}

impl fmt::Display for RetentionMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_retention_mode_as_str() {
        assert_eq!(RetentionMode::GOVERNANCE.as_str(), "GOVERNANCE");
        assert_eq!(RetentionMode::COMPLIANCE.as_str(), "COMPLIANCE");
    }

    #[test]
    fn test_retention_mode_parse_str() {
        assert_eq!(
            RetentionMode::parse_str("GOVERNANCE"),
            Some(RetentionMode::GOVERNANCE)
        );
        assert_eq!(
            RetentionMode::parse_str("COMPLIANCE"),
            Some(RetentionMode::COMPLIANCE)
        );
        assert_eq!(RetentionMode::parse_str("INVALID"), None);
        assert_eq!(RetentionMode::parse_str("governance"), None); // Case-sensitive
    }

    #[test]
    fn test_retention_mode_display() {
        assert_eq!(format!("{}", RetentionMode::GOVERNANCE), "GOVERNANCE");
        assert_eq!(format!("{}", RetentionMode::COMPLIANCE), "COMPLIANCE");
    }

    #[test]
    fn test_retention_mode_serialization() {
        // Test serde serialization
        let mode = RetentionMode::GOVERNANCE;
        let json = serde_json::to_string(&mode).unwrap();
        assert_eq!(json, r#""GOVERNANCE""#);

        let mode: RetentionMode = serde_json::from_str(&json).unwrap();
        assert_eq!(mode, RetentionMode::GOVERNANCE);
    }
}
