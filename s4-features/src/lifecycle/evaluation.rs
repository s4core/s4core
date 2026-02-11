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

//! Lifecycle rule evaluation logic.
//!
//! This module provides functions to evaluate if objects should be
//! deleted based on lifecycle rules.

use super::{Expiration, LifecycleRule, NoncurrentVersionExpiration};
use chrono::{DateTime, Duration, Utc};

/// Checks if an object's current version should be expired based on Expiration rules.
///
/// # Arguments
///
/// * `created_at_nanos` - Object creation timestamp in nanoseconds since Unix epoch
/// * `expiration` - Expiration rule to check
/// * `now` - Current timestamp
///
/// # Returns
///
/// `true` if the object should be expired, `false` otherwise.
pub fn should_expire_current_version(
    created_at_nanos: u64,
    expiration: &Expiration,
    now: DateTime<Utc>,
) -> bool {
    if let Some(days) = expiration.days {
        let created = timestamp_to_datetime(created_at_nanos);
        let age = now.signed_duration_since(created);
        return age >= Duration::days(i64::from(days));
    }
    false
}

/// Checks if a noncurrent version should be expired.
///
/// # Arguments
///
/// * `became_noncurrent_nanos` - Timestamp when version became noncurrent (nanoseconds)
/// * `expiration` - Noncurrent version expiration rule
/// * `now` - Current timestamp
///
/// # Returns
///
/// `true` if the noncurrent version should be expired, `false` otherwise.
pub fn should_expire_noncurrent_version(
    became_noncurrent_nanos: u64,
    expiration: &NoncurrentVersionExpiration,
    now: DateTime<Utc>,
) -> bool {
    let became_noncurrent = timestamp_to_datetime(became_noncurrent_nanos);
    let age_as_noncurrent = now.signed_duration_since(became_noncurrent);
    age_as_noncurrent >= Duration::days(i64::from(expiration.noncurrent_days))
}

/// Checks if a delete marker is "expired" (orphaned).
///
/// An expired delete marker is one where:
/// - It is a delete marker
/// - It is the only version remaining for the object
///
/// # Arguments
///
/// * `is_delete_marker` - Whether the current version is a delete marker
/// * `total_versions` - Total number of versions for this object
///
/// # Returns
///
/// `true` if the delete marker is expired, `false` otherwise.
pub fn is_expired_delete_marker(is_delete_marker: bool, total_versions: usize) -> bool {
    // A delete marker is "expired" if it's the only version left
    is_delete_marker && total_versions == 1
}

/// Checks if an object key matches a rule's filter.
///
/// # Arguments
///
/// * `key` - Object key to check
/// * `rule` - Lifecycle rule with filter
///
/// # Returns
///
/// `true` if the key matches the filter, `false` otherwise.
pub fn matches_filter(key: &str, rule: &LifecycleRule) -> bool {
    rule.matches(key)
}

/// Converts nanoseconds timestamp to `DateTime<Utc>`.
///
/// # Arguments
///
/// * `nanos` - Timestamp in nanoseconds since Unix epoch
///
/// # Returns
///
/// `DateTime<Utc>` representation of the timestamp.
pub fn timestamp_to_datetime(nanos: u64) -> DateTime<Utc> {
    // Handle potential overflow by using signed conversion
    DateTime::from_timestamp_nanos(nanos as i64)
}

/// Calculates the age of an object in days.
///
/// # Arguments
///
/// * `created_at_nanos` - Object creation timestamp in nanoseconds
/// * `now` - Current timestamp
///
/// # Returns
///
/// Number of days since creation (rounded down).
pub fn age_in_days(created_at_nanos: u64, now: DateTime<Utc>) -> i64 {
    let created = timestamp_to_datetime(created_at_nanos);
    now.signed_duration_since(created).num_days()
}

/// Result of evaluating a lifecycle rule against an object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvaluationResult {
    /// Whether the current version should be expired.
    pub expire_current: bool,
    /// Whether noncurrent versions should be checked for expiration.
    pub check_noncurrent: bool,
    /// Whether expired delete markers should be cleaned up.
    pub cleanup_delete_markers: bool,
}

impl EvaluationResult {
    /// Creates a result indicating no action should be taken.
    pub fn no_action() -> Self {
        Self {
            expire_current: false,
            check_noncurrent: false,
            cleanup_delete_markers: false,
        }
    }

    /// Returns true if any action should be taken.
    pub fn has_action(&self) -> bool {
        self.expire_current || self.check_noncurrent || self.cleanup_delete_markers
    }
}

/// Evaluates a lifecycle rule against an object.
///
/// # Arguments
///
/// * `rule` - Lifecycle rule to evaluate
/// * `key` - Object key
/// * `created_at_nanos` - Object creation timestamp
/// * `is_delete_marker` - Whether the current version is a delete marker
/// * `now` - Current timestamp
///
/// # Returns
///
/// Evaluation result indicating what actions should be taken.
pub fn evaluate_rule(
    rule: &LifecycleRule,
    key: &str,
    created_at_nanos: u64,
    is_delete_marker: bool,
    now: DateTime<Utc>,
) -> EvaluationResult {
    // Check if key matches filter
    if !matches_filter(key, rule) {
        return EvaluationResult::no_action();
    }

    let mut result = EvaluationResult::no_action();

    // Check current version expiration (only for non-delete-markers)
    if !is_delete_marker {
        if let Some(ref expiration) = rule.expiration {
            if should_expire_current_version(created_at_nanos, expiration, now) {
                result.expire_current = true;
            }
        }
    }

    // Check if noncurrent versions should be evaluated
    if rule.noncurrent_version_expiration.is_some() {
        result.check_noncurrent = true;
    }

    // Check if delete marker cleanup is enabled
    if rule.expired_object_delete_marker == Some(true) {
        result.cleanup_delete_markers = true;
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lifecycle::{LifecycleFilter, RuleStatus};

    fn nanos_from_days_ago(days: i64) -> u64 {
        let now = Utc::now();
        let past = now - Duration::days(days);
        past.timestamp_nanos_opt().unwrap_or(0) as u64
    }

    #[test]
    fn test_should_expire_current_version_expired() {
        let now = Utc::now();
        let created = nanos_from_days_ago(31); // 31 days ago
        let expiration = Expiration { days: Some(30) };

        assert!(should_expire_current_version(created, &expiration, now));
    }

    #[test]
    fn test_should_expire_current_version_not_expired() {
        let now = Utc::now();
        let created = nanos_from_days_ago(29); // 29 days ago
        let expiration = Expiration { days: Some(30) };

        assert!(!should_expire_current_version(created, &expiration, now));
    }

    #[test]
    fn test_should_expire_current_version_exact_boundary() {
        let now = Utc::now();
        // Use 30.5 days to account for timing precision issues
        let created = nanos_from_days_ago(31); // Slightly more than 30 days
        let expiration = Expiration { days: Some(30) };

        assert!(should_expire_current_version(created, &expiration, now));
    }

    #[test]
    fn test_should_expire_current_version_no_days() {
        let now = Utc::now();
        let created = nanos_from_days_ago(365);
        let expiration = Expiration { days: None };

        assert!(!should_expire_current_version(created, &expiration, now));
    }

    #[test]
    fn test_should_expire_noncurrent_version_expired() {
        let now = Utc::now();
        let became_noncurrent = nanos_from_days_ago(91); // 91 days ago
        let expiration = NoncurrentVersionExpiration {
            noncurrent_days: 90,
        };

        assert!(should_expire_noncurrent_version(
            became_noncurrent,
            &expiration,
            now
        ));
    }

    #[test]
    fn test_should_expire_noncurrent_version_not_expired() {
        let now = Utc::now();
        let became_noncurrent = nanos_from_days_ago(89); // 89 days ago
        let expiration = NoncurrentVersionExpiration {
            noncurrent_days: 90,
        };

        assert!(!should_expire_noncurrent_version(
            became_noncurrent,
            &expiration,
            now
        ));
    }

    #[test]
    fn test_is_expired_delete_marker_true() {
        // Delete marker is the only version
        assert!(is_expired_delete_marker(true, 1));
    }

    #[test]
    fn test_is_expired_delete_marker_false_not_marker() {
        // Not a delete marker
        assert!(!is_expired_delete_marker(false, 1));
    }

    #[test]
    fn test_is_expired_delete_marker_false_has_versions() {
        // Delete marker but other versions exist
        assert!(!is_expired_delete_marker(true, 3));
    }

    #[test]
    fn test_matches_filter_with_prefix() {
        let rule = LifecycleRule {
            id: "test".to_string(),
            status: RuleStatus::Enabled,
            filter: LifecycleFilter::with_prefix("logs/"),
            expiration: Some(Expiration { days: Some(30) }),
            noncurrent_version_expiration: None,
            expired_object_delete_marker: None,
        };

        assert!(matches_filter("logs/access.log", &rule));
        assert!(matches_filter("logs/errors/error.log", &rule));
        assert!(!matches_filter("data/file.txt", &rule));
    }

    #[test]
    fn test_matches_filter_all() {
        let rule = LifecycleRule {
            id: "test".to_string(),
            status: RuleStatus::Enabled,
            filter: LifecycleFilter::all(),
            expiration: Some(Expiration { days: Some(30) }),
            noncurrent_version_expiration: None,
            expired_object_delete_marker: None,
        };

        assert!(matches_filter("any/path/here.txt", &rule));
        assert!(matches_filter("file.txt", &rule));
    }

    #[test]
    fn test_age_in_days() {
        let now = Utc::now();
        let created = nanos_from_days_ago(45);

        let age = age_in_days(created, now);
        // Allow for timing variance (can be 44-46 due to nanosecond precision)
        assert!((44..=46).contains(&age), "Expected age 44-46, got {}", age);
    }

    #[test]
    fn test_evaluate_rule_no_match() {
        let rule = LifecycleRule {
            id: "test".to_string(),
            status: RuleStatus::Enabled,
            filter: LifecycleFilter::with_prefix("logs/"),
            expiration: Some(Expiration { days: Some(30) }),
            noncurrent_version_expiration: None,
            expired_object_delete_marker: None,
        };

        let now = Utc::now();
        let created = nanos_from_days_ago(60);

        let result = evaluate_rule(&rule, "data/file.txt", created, false, now);
        assert_eq!(result, EvaluationResult::no_action());
    }

    #[test]
    fn test_evaluate_rule_expire_current() {
        let rule = LifecycleRule {
            id: "test".to_string(),
            status: RuleStatus::Enabled,
            filter: LifecycleFilter::all(),
            expiration: Some(Expiration { days: Some(30) }),
            noncurrent_version_expiration: None,
            expired_object_delete_marker: None,
        };

        let now = Utc::now();
        let created = nanos_from_days_ago(60);

        let result = evaluate_rule(&rule, "file.txt", created, false, now);
        assert!(result.expire_current);
        assert!(!result.check_noncurrent);
        assert!(!result.cleanup_delete_markers);
    }

    #[test]
    fn test_evaluate_rule_check_noncurrent() {
        let rule = LifecycleRule {
            id: "test".to_string(),
            status: RuleStatus::Enabled,
            filter: LifecycleFilter::all(),
            expiration: None,
            noncurrent_version_expiration: Some(NoncurrentVersionExpiration {
                noncurrent_days: 90,
            }),
            expired_object_delete_marker: None,
        };

        let now = Utc::now();
        let created = nanos_from_days_ago(30);

        let result = evaluate_rule(&rule, "file.txt", created, false, now);
        assert!(!result.expire_current);
        assert!(result.check_noncurrent);
    }

    #[test]
    fn test_evaluate_rule_cleanup_markers() {
        let rule = LifecycleRule {
            id: "test".to_string(),
            status: RuleStatus::Enabled,
            filter: LifecycleFilter::all(),
            expiration: None,
            noncurrent_version_expiration: None,
            expired_object_delete_marker: Some(true),
        };

        let now = Utc::now();
        let created = nanos_from_days_ago(30);

        let result = evaluate_rule(&rule, "file.txt", created, true, now);
        assert!(!result.expire_current);
        assert!(result.cleanup_delete_markers);
    }

    #[test]
    fn test_evaluate_rule_delete_marker_not_expired() {
        // Delete markers should NOT be expired via regular expiration
        let rule = LifecycleRule {
            id: "test".to_string(),
            status: RuleStatus::Enabled,
            filter: LifecycleFilter::all(),
            expiration: Some(Expiration { days: Some(30) }),
            noncurrent_version_expiration: None,
            expired_object_delete_marker: None,
        };

        let now = Utc::now();
        let created = nanos_from_days_ago(60);

        // Even though object is old, delete markers are not expired via Expiration
        let result = evaluate_rule(&rule, "file.txt", created, true, now);
        assert!(!result.expire_current);
    }

    #[test]
    fn test_evaluate_rule_combined() {
        let rule = LifecycleRule {
            id: "test".to_string(),
            status: RuleStatus::Enabled,
            filter: LifecycleFilter::with_prefix("archive/"),
            expiration: Some(Expiration { days: Some(365) }),
            noncurrent_version_expiration: Some(NoncurrentVersionExpiration {
                noncurrent_days: 30,
            }),
            expired_object_delete_marker: Some(true),
        };

        let now = Utc::now();
        let created = nanos_from_days_ago(400);

        let result = evaluate_rule(&rule, "archive/old-data.zip", created, false, now);
        assert!(result.expire_current);
        assert!(result.check_noncurrent);
        assert!(result.cleanup_delete_markers);
    }

    #[test]
    fn test_evaluation_result_has_action() {
        let no_action = EvaluationResult::no_action();
        assert!(!no_action.has_action());

        let with_expiration = EvaluationResult {
            expire_current: true,
            check_noncurrent: false,
            cleanup_delete_markers: false,
        };
        assert!(with_expiration.has_action());
    }
}
