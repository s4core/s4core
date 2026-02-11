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

//! Lifecycle configuration for automatic object expiration.
//!
//! This module provides S3-compatible lifecycle policy management
//! for automatic deletion of objects and old versions.
//!
//! # Features
//!
//! - **Object Expiration**: Delete current versions after N days
//! - **Noncurrent Version Expiration**: Delete old versions after N days
//! - **Expired Delete Marker Cleanup**: Remove orphaned delete markers
//! - **Prefix Filtering**: Apply rules to specific object prefixes

mod evaluation;
mod xml;

pub use evaluation::*;
pub use xml::*;

use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// Maximum number of lifecycle rules per bucket (S3 limit).
pub const MAX_LIFECYCLE_RULES: usize = 1000;

/// Maximum length for a rule ID.
pub const MAX_RULE_ID_LENGTH: usize = 255;

/// Lifecycle configuration for a bucket.
///
/// Contains a list of lifecycle rules that define when objects
/// should be automatically deleted or transitioned.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LifecycleConfiguration {
    /// List of lifecycle rules (max 1000 per S3 spec).
    pub rules: Vec<LifecycleRule>,
}

/// A single lifecycle rule.
///
/// Each rule defines a filter (which objects to apply to) and actions
/// (what to do with matching objects).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LifecycleRule {
    /// Unique identifier for this rule (max 255 chars).
    pub id: String,

    /// Whether the rule is enabled or disabled.
    pub status: RuleStatus,

    /// Filter to select which objects this rule applies to.
    pub filter: LifecycleFilter,

    /// Expiration settings for current versions.
    #[serde(default)]
    pub expiration: Option<Expiration>,

    /// Expiration settings for noncurrent versions.
    #[serde(default)]
    pub noncurrent_version_expiration: Option<NoncurrentVersionExpiration>,

    /// Whether to clean up expired delete markers.
    ///
    /// An expired delete marker is one where:
    /// - It is the only version remaining for an object
    /// - No other versions exist
    #[serde(default)]
    pub expired_object_delete_marker: Option<bool>,
}

/// Rule status (enabled or disabled).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RuleStatus {
    /// Rule is active and will be evaluated.
    Enabled,
    /// Rule is inactive and will be skipped.
    Disabled,
}

impl RuleStatus {
    /// Returns the XML representation of this status.
    pub fn as_str(&self) -> &'static str {
        match self {
            RuleStatus::Enabled => "Enabled",
            RuleStatus::Disabled => "Disabled",
        }
    }

    /// Parses a status from a string.
    pub fn parse_str(s: &str) -> Option<Self> {
        match s {
            "Enabled" => Some(RuleStatus::Enabled),
            "Disabled" => Some(RuleStatus::Disabled),
            _ => None,
        }
    }
}

/// Filter to select objects for a rule.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LifecycleFilter {
    /// Key prefix to match (e.g., "logs/", "backups/").
    /// Empty string or None matches all objects.
    #[serde(default)]
    pub prefix: Option<String>,
    // Future extensions:
    // - tag: Option<Tag> for tag-based filtering
    // - object_size_greater_than: Option<u64>
    // - object_size_less_than: Option<u64>
}

/// Expiration settings for current object versions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Expiration {
    /// Number of days after object creation to expire.
    /// Objects older than this will be deleted.
    #[serde(default)]
    pub days: Option<u32>,
    // Future extensions:
    // - date: Option<DateTime<Utc>> for date-based expiration
}

/// Expiration settings for noncurrent (old) versions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NoncurrentVersionExpiration {
    /// Number of days after becoming noncurrent to expire.
    /// Versions older than this (since being replaced) will be deleted.
    pub noncurrent_days: u32,
    // Future extensions:
    // - newer_noncurrent_versions: Option<u32> for keeping N versions
}

impl LifecycleConfiguration {
    /// Creates an empty lifecycle configuration.
    pub fn new() -> Self {
        Self { rules: Vec::new() }
    }

    /// Adds a rule to the configuration.
    pub fn add_rule(&mut self, rule: LifecycleRule) {
        self.rules.push(rule);
    }

    /// Returns enabled rules only.
    pub fn enabled_rules(&self) -> impl Iterator<Item = &LifecycleRule> {
        self.rules.iter().filter(|r| r.status == RuleStatus::Enabled)
    }

    /// Validates the lifecycle configuration.
    pub fn validate(&self) -> Result<(), LifecycleError> {
        // Check for empty configuration
        if self.rules.is_empty() {
            return Err(LifecycleError::NoRules);
        }

        // Check for too many rules
        if self.rules.len() > MAX_LIFECYCLE_RULES {
            return Err(LifecycleError::TooManyRules {
                count: self.rules.len(),
                max: MAX_LIFECYCLE_RULES,
            });
        }

        // Check for duplicate rule IDs
        let mut seen_ids = HashSet::new();
        for rule in &self.rules {
            if !seen_ids.insert(&rule.id) {
                return Err(LifecycleError::DuplicateRuleId {
                    id: rule.id.clone(),
                });
            }
        }

        // Validate each rule
        for (i, rule) in self.rules.iter().enumerate() {
            rule.validate().map_err(|e| LifecycleError::InvalidRule {
                index: i,
                id: rule.id.clone(),
                error: e.to_string(),
            })?;
        }

        Ok(())
    }
}

impl LifecycleRule {
    /// Validates this lifecycle rule.
    pub fn validate(&self) -> Result<(), LifecycleRuleError> {
        // Validate ID
        if self.id.is_empty() {
            return Err(LifecycleRuleError::EmptyId);
        }

        if self.id.len() > MAX_RULE_ID_LENGTH {
            return Err(LifecycleRuleError::IdTooLong {
                length: self.id.len(),
                max: MAX_RULE_ID_LENGTH,
            });
        }

        // Validate that at least one action is specified
        let has_expiration = self.expiration.is_some();
        let has_noncurrent_expiration = self.noncurrent_version_expiration.is_some();
        let has_delete_marker_cleanup = self.expired_object_delete_marker == Some(true);

        if !has_expiration && !has_noncurrent_expiration && !has_delete_marker_cleanup {
            return Err(LifecycleRuleError::NoAction);
        }

        // Validate expiration days
        if let Some(ref expiration) = self.expiration {
            if let Some(days) = expiration.days {
                if days == 0 {
                    return Err(LifecycleRuleError::InvalidExpirationDays { days });
                }
            } else if self.expired_object_delete_marker != Some(true) {
                // Expiration must have Days if not using ExpiredObjectDeleteMarker alone
                return Err(LifecycleRuleError::MissingExpirationDays);
            }
        }

        // Validate noncurrent version expiration days
        if let Some(ref noncurrent) = self.noncurrent_version_expiration {
            if noncurrent.noncurrent_days == 0 {
                return Err(LifecycleRuleError::InvalidNoncurrentDays {
                    days: noncurrent.noncurrent_days,
                });
            }
        }

        Ok(())
    }

    /// Checks if an object key matches this rule's filter.
    pub fn matches(&self, key: &str) -> bool {
        match &self.filter.prefix {
            Some(prefix) if !prefix.is_empty() => key.starts_with(prefix),
            _ => true, // Empty or no prefix matches all
        }
    }
}

impl LifecycleFilter {
    /// Creates a filter that matches all objects.
    pub fn all() -> Self {
        Self { prefix: None }
    }

    /// Creates a filter with a prefix.
    pub fn with_prefix(prefix: impl Into<String>) -> Self {
        Self {
            prefix: Some(prefix.into()),
        }
    }
}

/// Errors related to lifecycle configuration.
#[derive(Debug, thiserror::Error)]
pub enum LifecycleError {
    /// No lifecycle rules defined.
    #[error("Lifecycle configuration must contain at least one rule")]
    NoRules,

    /// Too many lifecycle rules.
    #[error("Lifecycle configuration has {count} rules, maximum is {max}")]
    TooManyRules {
        /// Actual count.
        count: usize,
        /// Maximum allowed.
        max: usize,
    },

    /// Duplicate rule ID.
    #[error("Duplicate rule ID: {id}")]
    DuplicateRuleId {
        /// The duplicate ID.
        id: String,
    },

    /// Invalid rule at specified index.
    #[error("Invalid lifecycle rule at index {index} (ID: {id}): {error}")]
    InvalidRule {
        /// Rule index.
        index: usize,
        /// Rule ID.
        id: String,
        /// Error description.
        error: String,
    },
}

/// Errors related to a single lifecycle rule.
#[derive(Debug, thiserror::Error)]
pub enum LifecycleRuleError {
    /// Empty rule ID.
    #[error("Rule ID cannot be empty")]
    EmptyId,

    /// Rule ID too long.
    #[error("Rule ID length {length} exceeds maximum {max}")]
    IdTooLong {
        /// Actual length.
        length: usize,
        /// Maximum allowed.
        max: usize,
    },

    /// No action specified.
    #[error("Rule must have at least one action (Expiration, NoncurrentVersionExpiration, or ExpiredObjectDeleteMarker)")]
    NoAction,

    /// Invalid expiration days.
    #[error("Expiration days must be at least 1, got {days}")]
    InvalidExpirationDays {
        /// Invalid value.
        days: u32,
    },

    /// Missing expiration days.
    #[error("Expiration element must specify Days")]
    MissingExpirationDays,

    /// Invalid noncurrent expiration days.
    #[error("NoncurrentDays must be at least 1, got {days}")]
    InvalidNoncurrentDays {
        /// Invalid value.
        days: u32,
    },
}

/// Lifecycle XML parsing error.
#[derive(Debug, thiserror::Error)]
pub enum LifecycleParseError {
    /// Invalid XML format.
    #[error("Invalid XML format")]
    InvalidXml,

    /// Missing required element.
    #[error("Missing required element: {element}")]
    MissingElement {
        /// Missing element name.
        element: String,
    },

    /// Invalid element value.
    #[error("Invalid value for {element}: {value}")]
    InvalidValue {
        /// Element name.
        element: String,
        /// Invalid value.
        value: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rule_status_conversion() {
        assert_eq!(RuleStatus::Enabled.as_str(), "Enabled");
        assert_eq!(RuleStatus::Disabled.as_str(), "Disabled");

        assert_eq!(RuleStatus::parse_str("Enabled"), Some(RuleStatus::Enabled));
        assert_eq!(
            RuleStatus::parse_str("Disabled"),
            Some(RuleStatus::Disabled)
        );
        assert_eq!(RuleStatus::parse_str("Invalid"), None);
    }

    #[test]
    fn test_lifecycle_filter_matching() {
        // All objects filter
        let filter = LifecycleFilter::all();
        assert!(LifecycleRule {
            id: "test".to_string(),
            status: RuleStatus::Enabled,
            filter,
            expiration: Some(Expiration { days: Some(30) }),
            noncurrent_version_expiration: None,
            expired_object_delete_marker: None,
        }
        .matches("any/key/here.txt"));

        // Prefix filter
        let filter = LifecycleFilter::with_prefix("logs/");
        let rule = LifecycleRule {
            id: "test".to_string(),
            status: RuleStatus::Enabled,
            filter,
            expiration: Some(Expiration { days: Some(30) }),
            noncurrent_version_expiration: None,
            expired_object_delete_marker: None,
        };
        assert!(rule.matches("logs/access.log"));
        assert!(rule.matches("logs/errors/error.log"));
        assert!(!rule.matches("data/file.txt"));
        assert!(!rule.matches("log/file.txt")); // Missing trailing slash
    }

    #[test]
    fn test_valid_configuration() {
        let mut config = LifecycleConfiguration::new();
        config.add_rule(LifecycleRule {
            id: "expire-logs".to_string(),
            status: RuleStatus::Enabled,
            filter: LifecycleFilter::with_prefix("logs/"),
            expiration: Some(Expiration { days: Some(30) }),
            noncurrent_version_expiration: None,
            expired_object_delete_marker: None,
        });

        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_empty_configuration_invalid() {
        let config = LifecycleConfiguration::new();
        assert!(matches!(config.validate(), Err(LifecycleError::NoRules)));
    }

    #[test]
    fn test_duplicate_rule_id() {
        let mut config = LifecycleConfiguration::new();
        config.add_rule(LifecycleRule {
            id: "rule1".to_string(),
            status: RuleStatus::Enabled,
            filter: LifecycleFilter::all(),
            expiration: Some(Expiration { days: Some(30) }),
            noncurrent_version_expiration: None,
            expired_object_delete_marker: None,
        });
        config.add_rule(LifecycleRule {
            id: "rule1".to_string(), // Duplicate
            status: RuleStatus::Enabled,
            filter: LifecycleFilter::all(),
            expiration: Some(Expiration { days: Some(60) }),
            noncurrent_version_expiration: None,
            expired_object_delete_marker: None,
        });

        assert!(matches!(
            config.validate(),
            Err(LifecycleError::DuplicateRuleId { .. })
        ));
    }

    #[test]
    fn test_rule_without_action() {
        let rule = LifecycleRule {
            id: "empty-rule".to_string(),
            status: RuleStatus::Enabled,
            filter: LifecycleFilter::all(),
            expiration: None,
            noncurrent_version_expiration: None,
            expired_object_delete_marker: None,
        };

        assert!(matches!(rule.validate(), Err(LifecycleRuleError::NoAction)));
    }

    #[test]
    fn test_rule_with_zero_days() {
        let rule = LifecycleRule {
            id: "zero-days".to_string(),
            status: RuleStatus::Enabled,
            filter: LifecycleFilter::all(),
            expiration: Some(Expiration { days: Some(0) }),
            noncurrent_version_expiration: None,
            expired_object_delete_marker: None,
        };

        assert!(matches!(
            rule.validate(),
            Err(LifecycleRuleError::InvalidExpirationDays { days: 0 })
        ));
    }

    #[test]
    fn test_rule_with_empty_id() {
        let rule = LifecycleRule {
            id: "".to_string(),
            status: RuleStatus::Enabled,
            filter: LifecycleFilter::all(),
            expiration: Some(Expiration { days: Some(30) }),
            noncurrent_version_expiration: None,
            expired_object_delete_marker: None,
        };

        assert!(matches!(rule.validate(), Err(LifecycleRuleError::EmptyId)));
    }

    #[test]
    fn test_rule_with_long_id() {
        let rule = LifecycleRule {
            id: "x".repeat(256),
            status: RuleStatus::Enabled,
            filter: LifecycleFilter::all(),
            expiration: Some(Expiration { days: Some(30) }),
            noncurrent_version_expiration: None,
            expired_object_delete_marker: None,
        };

        assert!(matches!(
            rule.validate(),
            Err(LifecycleRuleError::IdTooLong { .. })
        ));
    }

    #[test]
    fn test_noncurrent_version_expiration() {
        let mut config = LifecycleConfiguration::new();
        config.add_rule(LifecycleRule {
            id: "cleanup-old-versions".to_string(),
            status: RuleStatus::Enabled,
            filter: LifecycleFilter::all(),
            expiration: None,
            noncurrent_version_expiration: Some(NoncurrentVersionExpiration {
                noncurrent_days: 90,
            }),
            expired_object_delete_marker: None,
        });

        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_expired_delete_marker_only() {
        let mut config = LifecycleConfiguration::new();
        config.add_rule(LifecycleRule {
            id: "cleanup-markers".to_string(),
            status: RuleStatus::Enabled,
            filter: LifecycleFilter::all(),
            expiration: None,
            noncurrent_version_expiration: None,
            expired_object_delete_marker: Some(true),
        });

        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_enabled_rules_iterator() {
        let mut config = LifecycleConfiguration::new();
        config.add_rule(LifecycleRule {
            id: "enabled-rule".to_string(),
            status: RuleStatus::Enabled,
            filter: LifecycleFilter::all(),
            expiration: Some(Expiration { days: Some(30) }),
            noncurrent_version_expiration: None,
            expired_object_delete_marker: None,
        });
        config.add_rule(LifecycleRule {
            id: "disabled-rule".to_string(),
            status: RuleStatus::Disabled,
            filter: LifecycleFilter::all(),
            expiration: Some(Expiration { days: Some(60) }),
            noncurrent_version_expiration: None,
            expired_object_delete_marker: None,
        });

        let enabled: Vec<_> = config.enabled_rules().collect();
        assert_eq!(enabled.len(), 1);
        assert_eq!(enabled[0].id, "enabled-rule");
    }

    #[test]
    fn test_combined_rule() {
        // Rule with multiple actions
        let mut config = LifecycleConfiguration::new();
        config.add_rule(LifecycleRule {
            id: "full-cleanup".to_string(),
            status: RuleStatus::Enabled,
            filter: LifecycleFilter::with_prefix("temp/"),
            expiration: Some(Expiration { days: Some(7) }),
            noncurrent_version_expiration: Some(NoncurrentVersionExpiration { noncurrent_days: 1 }),
            expired_object_delete_marker: Some(true),
        });

        assert!(config.validate().is_ok());
    }
}
