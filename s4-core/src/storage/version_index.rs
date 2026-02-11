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

//! Version list management for tracking all versions of an object.
//!
//! This module provides the `VersionList` structure that maintains an ordered
//! list of all versions for a given object key.

use serde::{Deserialize, Serialize};

/// Tracks all versions of an object.
///
/// The version list maintains:
/// - An ordered list of version IDs (newest first)
/// - A pointer to the current (latest non-delete-marker) version
///
/// This structure is stored as JSON in the index database at a special key:
/// `__s4_versions_{bucket}/{key}`
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VersionList {
    /// Version IDs ordered from newest to oldest.
    /// The first element is always the most recent version.
    pub versions: Vec<String>,
    /// Current version ID (latest non-delete-marker).
    /// `None` if the most recent version is a delete marker.
    pub current_version: Option<String>,
}

impl VersionList {
    /// Creates a new empty version list.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a new version to the front of the list.
    ///
    /// # Arguments
    /// * `version_id` - The version ID to add
    /// * `is_delete_marker` - Whether this version is a delete marker
    ///
    /// If `is_delete_marker` is true, `current_version` is set to `None`.
    /// Otherwise, `current_version` is updated to the new version ID.
    pub fn add_version(&mut self, version_id: String, is_delete_marker: bool) {
        self.versions.insert(0, version_id.clone());
        self.current_version = if is_delete_marker {
            None
        } else {
            Some(version_id)
        };
    }

    /// Removes a version from the list.
    ///
    /// # Arguments
    /// * `version_id` - The version ID to remove
    ///
    /// # Returns
    /// `true` if the version was found and removed, `false` otherwise.
    pub fn remove_version(&mut self, version_id: &str) -> bool {
        if let Some(pos) = self.versions.iter().position(|v| v == version_id) {
            self.versions.remove(pos);
            true
        } else {
            false
        }
    }

    /// Checks if the list has any versions.
    pub fn has_versions(&self) -> bool {
        !self.versions.is_empty()
    }

    /// Returns the most recent version ID.
    pub fn latest_version(&self) -> Option<&str> {
        self.versions.first().map(|s| s.as_str())
    }

    /// Returns the number of versions.
    pub fn len(&self) -> usize {
        self.versions.len()
    }

    /// Checks if the list is empty.
    pub fn is_empty(&self) -> bool {
        self.versions.is_empty()
    }

    /// Returns an iterator over all version IDs (newest first).
    pub fn iter(&self) -> impl Iterator<Item = &str> {
        self.versions.iter().map(|s| s.as_str())
    }

    /// Updates the current version pointer.
    ///
    /// This should be called after removing a delete marker to restore
    /// the previous current version.
    ///
    /// # Arguments
    /// * `version_id` - The version ID to set as current, or `None` if deleted
    pub fn set_current_version(&mut self, version_id: Option<String>) {
        self.current_version = version_id;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let list = VersionList::new();
        assert!(list.is_empty());
        assert_eq!(list.len(), 0);
        assert!(list.current_version.is_none());
    }

    #[test]
    fn test_add_version() {
        let mut list = VersionList::new();

        list.add_version("v1".to_string(), false);
        assert_eq!(list.len(), 1);
        assert_eq!(list.current_version, Some("v1".to_string()));
        assert_eq!(list.latest_version(), Some("v1"));

        list.add_version("v2".to_string(), false);
        assert_eq!(list.len(), 2);
        assert_eq!(list.current_version, Some("v2".to_string()));
        assert_eq!(list.latest_version(), Some("v2"));
        assert_eq!(list.versions, vec!["v2", "v1"]);
    }

    #[test]
    fn test_add_delete_marker() {
        let mut list = VersionList::new();

        list.add_version("v1".to_string(), false);
        assert_eq!(list.current_version, Some("v1".to_string()));

        list.add_version("dm1".to_string(), true);
        assert_eq!(list.len(), 2);
        assert!(list.current_version.is_none());
        assert_eq!(list.latest_version(), Some("dm1"));
    }

    #[test]
    fn test_remove_version() {
        let mut list = VersionList::new();
        list.add_version("v1".to_string(), false);
        list.add_version("v2".to_string(), false);
        list.add_version("v3".to_string(), false);

        assert!(list.remove_version("v2"));
        assert_eq!(list.len(), 2);
        assert_eq!(list.versions, vec!["v3", "v1"]);

        assert!(!list.remove_version("nonexistent"));
        assert_eq!(list.len(), 2);
    }

    #[test]
    fn test_has_versions() {
        let mut list = VersionList::new();
        assert!(!list.has_versions());

        list.add_version("v1".to_string(), false);
        assert!(list.has_versions());

        list.remove_version("v1");
        assert!(!list.has_versions());
    }

    #[test]
    fn test_iter() {
        let mut list = VersionList::new();
        list.add_version("v1".to_string(), false);
        list.add_version("v2".to_string(), false);
        list.add_version("v3".to_string(), false);

        let versions: Vec<&str> = list.iter().collect();
        assert_eq!(versions, vec!["v3", "v2", "v1"]);
    }

    #[test]
    fn test_serialization() {
        let mut list = VersionList::new();
        list.add_version("v1".to_string(), false);
        list.add_version("v2".to_string(), true);

        let json = serde_json::to_string(&list).unwrap();
        let deserialized: VersionList = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.versions, list.versions);
        assert_eq!(deserialized.current_version, list.current_version);
    }
}
