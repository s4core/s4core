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

//! Object Lock configuration for WORM (Write-Once-Read-Many) storage.
//!
//! This module provides S3-compatible Object Lock functionality
//! for retention periods and legal holds.
//!
//! # Features
//!
//! - **Retention Periods**: Objects cannot be deleted before a specific date
//! - **Legal Holds**: Indefinite lock that can be placed/removed independently
//! - **GOVERNANCE Mode**: Can be overridden with special permission (Phase 5)
//! - **COMPLIANCE Mode**: Cannot be deleted until expiration (strict WORM)
//!
//! # Safety
//!
//! COMPLIANCE mode is high-risk: incorrect implementation could create
//! permanently undeletable files. Phase 3 blocks ALL modifications to
//! COMPLIANCE retention to ensure safety.

mod xml;

pub use xml::*;

// Re-export types from s4-core (defined in core to avoid circular dependency)
pub use s4_core::types::{DefaultRetention, RetentionMode};

use serde::{Deserialize, Serialize};
use std::fmt;

/// Legal hold status for an object version.
///
/// Legal hold is independent of retention periods and can be
/// placed or removed at any time (subject to IAM permissions in Phase 5).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[allow(clippy::upper_case_acronyms)] // AWS S3 API uses ON/OFF for legal hold status
pub enum LegalHoldStatus {
    /// Legal hold is active - object cannot be deleted.
    ON,
    /// Legal hold is inactive - normal retention rules apply.
    OFF,
}

impl LegalHoldStatus {
    /// Returns the S3 API string representation.
    pub fn as_str(&self) -> &'static str {
        match self {
            LegalHoldStatus::ON => "ON",
            LegalHoldStatus::OFF => "OFF",
        }
    }

    /// Parses legal hold status from a string.
    pub fn parse_str(s: &str) -> Option<Self> {
        match s {
            "ON" => Some(LegalHoldStatus::ON),
            "OFF" => Some(LegalHoldStatus::OFF),
            _ => None,
        }
    }

    /// Converts to boolean (ON = true, OFF = false).
    pub fn to_bool(self) -> bool {
        matches!(self, LegalHoldStatus::ON)
    }

    /// Converts from boolean (true = ON, false = OFF).
    pub fn from_bool(value: bool) -> Self {
        if value {
            LegalHoldStatus::ON
        } else {
            LegalHoldStatus::OFF
        }
    }
}

impl fmt::Display for LegalHoldStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Bucket-level Object Lock configuration.
///
/// Controls whether Object Lock is enabled on a bucket and defines
/// default retention settings for new objects.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ObjectLockConfiguration {
    /// Whether Object Lock is enabled on this bucket.
    ///
    /// **Important**: Once enabled, Object Lock CANNOT be disabled.
    /// This is an AWS S3 constraint for data protection.
    pub object_lock_enabled: bool,

    /// Default retention settings applied to new objects.
    ///
    /// When set, all new objects will automatically get this retention
    /// unless explicitly overridden during PUT.
    #[serde(default)]
    pub default_retention: Option<DefaultRetention>,
}

impl ObjectLockConfiguration {
    /// Validates the configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Default retention is set but Object Lock is not enabled
    /// - Retention days is zero
    pub fn validate(&self) -> Result<(), ObjectLockError> {
        // If default retention exists, Object Lock must be enabled
        if let Some(ref retention) = self.default_retention {
            if !self.object_lock_enabled {
                return Err(ObjectLockError::RetentionWithoutLockEnabled);
            }

            // Validate retention days > 0
            if retention.days == 0 {
                return Err(ObjectLockError::InvalidRetentionDays { days: 0 });
            }
        }

        Ok(())
    }
}

/// Object-level retention configuration.
///
/// Specifies retention mode and retain-until date for a specific object version.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectRetention {
    /// Retention mode.
    pub mode: RetentionMode,

    /// Retain until this ISO 8601 timestamp.
    ///
    /// Format: YYYY-MM-DDTHH:MM:SSZ (e.g., "2025-12-31T23:59:59Z")
    pub retain_until_date: String,
}

impl ObjectRetention {
    /// Creates a new object retention configuration.
    pub fn new(mode: RetentionMode, retain_until_date: String) -> Self {
        Self {
            mode,
            retain_until_date,
        }
    }
}

/// Legal hold configuration for an object version.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LegalHold {
    /// Legal hold status (ON or OFF).
    pub status: LegalHoldStatus,
}

impl LegalHold {
    /// Creates a new legal hold configuration.
    pub fn new(status: LegalHoldStatus) -> Self {
        Self { status }
    }

    /// Creates a legal hold with status ON.
    pub fn on() -> Self {
        Self {
            status: LegalHoldStatus::ON,
        }
    }

    /// Creates a legal hold with status OFF.
    pub fn off() -> Self {
        Self {
            status: LegalHoldStatus::OFF,
        }
    }
}

/// Errors that can occur when working with Object Lock.
#[derive(Debug, thiserror::Error)]
pub enum ObjectLockError {
    /// Default retention specified but Object Lock is not enabled.
    #[error("Default retention requires Object Lock to be enabled")]
    RetentionWithoutLockEnabled,

    /// Invalid retention days (must be > 0).
    #[error("Retention days must be greater than 0, got {days}")]
    InvalidRetentionDays {
        /// The invalid days value
        days: u32,
    },

    /// Cannot disable Object Lock once enabled.
    #[error("Object Lock cannot be disabled once enabled")]
    CannotDisableObjectLock,

    /// Versioning must be enabled to use Object Lock.
    #[error("Versioning must be enabled to use Object Lock")]
    VersioningNotEnabled,

    /// Cannot modify COMPLIANCE mode retention.
    #[error("Cannot modify COMPLIANCE mode retention in Phase 3")]
    CannotModifyComplianceRetention,

    /// Retain until date must be in the future.
    #[error("Retain until date must be in the future")]
    RetainUntilDateInPast,

    /// Invalid ISO 8601 date format.
    #[error("Invalid ISO 8601 date format: {0}")]
    InvalidDateFormat(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_legal_hold_status() {
        assert_eq!(LegalHoldStatus::ON.as_str(), "ON");
        assert_eq!(LegalHoldStatus::OFF.as_str(), "OFF");

        assert!(LegalHoldStatus::ON.to_bool());
        assert!(!LegalHoldStatus::OFF.to_bool());

        assert_eq!(LegalHoldStatus::from_bool(true), LegalHoldStatus::ON);
        assert_eq!(LegalHoldStatus::from_bool(false), LegalHoldStatus::OFF);
    }

    #[test]
    fn test_object_lock_config_validation() {
        // Valid configuration with Object Lock enabled
        let config = ObjectLockConfiguration {
            object_lock_enabled: true,
            default_retention: Some(DefaultRetention {
                mode: RetentionMode::GOVERNANCE,
                days: 30,
            }),
        };
        assert!(config.validate().is_ok());

        // Valid configuration without default retention
        let config = ObjectLockConfiguration {
            object_lock_enabled: true,
            default_retention: None,
        };
        assert!(config.validate().is_ok());

        // Invalid: retention without lock enabled
        let config = ObjectLockConfiguration {
            object_lock_enabled: false,
            default_retention: Some(DefaultRetention {
                mode: RetentionMode::COMPLIANCE,
                days: 90,
            }),
        };
        assert!(matches!(
            config.validate(),
            Err(ObjectLockError::RetentionWithoutLockEnabled)
        ));

        // Invalid: zero retention days
        let config = ObjectLockConfiguration {
            object_lock_enabled: true,
            default_retention: Some(DefaultRetention {
                mode: RetentionMode::GOVERNANCE,
                days: 0,
            }),
        };
        assert!(matches!(
            config.validate(),
            Err(ObjectLockError::InvalidRetentionDays { days: 0 })
        ));
    }

    #[test]
    fn test_default_retention_calculate_retain_until() {
        let retention = DefaultRetention {
            mode: RetentionMode::GOVERNANCE,
            days: 30,
        };

        // Created at Unix epoch (0 nanoseconds)
        let created_at = 0_u64;
        let expected = 30 * 86_400_000_000_000_u64; // 30 days in nanoseconds
        assert_eq!(retention.calculate_retain_until(created_at), expected);

        // Created at some timestamp
        let created_at = 1_000_000_000_000_000_000_u64;
        let expected = created_at + (30 * 86_400_000_000_000_u64);
        assert_eq!(retention.calculate_retain_until(created_at), expected);
    }

    #[test]
    fn test_legal_hold_constructors() {
        let hold_on = LegalHold::on();
        assert_eq!(hold_on.status, LegalHoldStatus::ON);

        let hold_off = LegalHold::off();
        assert_eq!(hold_off.status, LegalHoldStatus::OFF);

        let hold_new = LegalHold::new(LegalHoldStatus::ON);
        assert_eq!(hold_new.status, LegalHoldStatus::ON);
    }
}
