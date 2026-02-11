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

//! Versioning types for S3-compatible object versioning.

use serde::{Deserialize, Serialize};

/// Bucket versioning status.
///
/// S3-compatible versioning states:
/// - `Unversioned`: Default state, no version history kept
/// - `Enabled`: All PUT operations create new versions, DELETE creates delete markers
/// - `Suspended`: New objects get "null" version ID, overwrites replace "null" version only
///
/// **Important**: Once versioning is enabled, the bucket can never return to `Unversioned`.
/// It can only be suspended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum VersioningStatus {
    /// No versioning (default state for new buckets).
    #[default]
    Unversioned,
    /// Versioning is enabled. All operations preserve version history.
    Enabled,
    /// Versioning is suspended. New objects use "null" version ID.
    Suspended,
}

impl VersioningStatus {
    /// Parses versioning status from S3 XML value.
    ///
    /// # Arguments
    /// * `status` - The status string from XML ("Enabled" or "Suspended")
    ///
    /// # Returns
    /// The parsed versioning status. Invalid values return `Unversioned`.
    pub fn from_xml_status(status: &str) -> Self {
        match status {
            "Enabled" => Self::Enabled,
            "Suspended" => Self::Suspended,
            _ => Self::Unversioned,
        }
    }

    /// Converts versioning status to S3 XML value.
    ///
    /// # Returns
    /// - `Some("Enabled")` or `Some("Suspended")` for active states
    /// - `None` for `Unversioned` (S3 returns empty response for unversioned buckets)
    pub fn as_xml_status(&self) -> Option<&'static str> {
        match self {
            Self::Enabled => Some("Enabled"),
            Self::Suspended => Some("Suspended"),
            Self::Unversioned => None,
        }
    }

    /// Checks if versioning is active (enabled or suspended).
    ///
    /// When versioning is active, version IDs are assigned to objects.
    pub fn is_active(&self) -> bool {
        matches!(self, Self::Enabled | Self::Suspended)
    }

    /// Checks if versioning is enabled (not suspended).
    pub fn is_enabled(&self) -> bool {
        matches!(self, Self::Enabled)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_xml_status() {
        assert_eq!(
            VersioningStatus::from_xml_status("Enabled"),
            VersioningStatus::Enabled
        );
        assert_eq!(
            VersioningStatus::from_xml_status("Suspended"),
            VersioningStatus::Suspended
        );
        assert_eq!(
            VersioningStatus::from_xml_status(""),
            VersioningStatus::Unversioned
        );
        assert_eq!(
            VersioningStatus::from_xml_status("Invalid"),
            VersioningStatus::Unversioned
        );
    }

    #[test]
    fn test_as_xml_status() {
        assert_eq!(VersioningStatus::Enabled.as_xml_status(), Some("Enabled"));
        assert_eq!(
            VersioningStatus::Suspended.as_xml_status(),
            Some("Suspended")
        );
        assert_eq!(VersioningStatus::Unversioned.as_xml_status(), None);
    }

    #[test]
    fn test_is_active() {
        assert!(VersioningStatus::Enabled.is_active());
        assert!(VersioningStatus::Suspended.is_active());
        assert!(!VersioningStatus::Unversioned.is_active());
    }

    #[test]
    fn test_is_enabled() {
        assert!(VersioningStatus::Enabled.is_enabled());
        assert!(!VersioningStatus::Suspended.is_enabled());
        assert!(!VersioningStatus::Unversioned.is_enabled());
    }

    #[test]
    fn test_default() {
        assert_eq!(VersioningStatus::default(), VersioningStatus::Unversioned);
    }
}
