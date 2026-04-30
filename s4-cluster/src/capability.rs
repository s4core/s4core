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

//! Edition-aware capability limits for CE/EE enforcement.
//!
//! [`ClusterLimits`] defines quantitative boundaries (max pools, max nodes)
//! that differ between Community Edition and Enterprise Edition. The struct
//! is created at startup and threaded through config validation, pool
//! creation, and node join handlers.
//!
//! # CE/EE Boundary
//!
//! - **CE**: 1 pool, 3 nodes per pool — fully functional cluster.
//! - **EE**: Limits come from the signed license payload.
//!
//! The EE crate (`ee/s4-ee-license`) produces `ClusterLimits` from the
//! license; this crate only defines the struct and CE defaults.

use serde::{Deserialize, Serialize};

/// Quantitative limits for the cluster, derived from the active edition/license.
///
/// Community Edition uses [`ClusterLimits::community()`]. Enterprise Edition
/// populates limits from the signed license key (see `ee/s4-ee-license`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterLimits {
    /// Whether these limits originate from an Enterprise license.
    ///
    /// Set explicitly at construction time — not inferred from numeric values.
    /// This prevents an EE license with minimal limits from being misidentified
    /// as CE (which would produce confusing error messages).
    pub is_enterprise: bool,

    /// Maximum number of server pools.
    pub max_pools: u32,

    /// Maximum number of nodes per server pool.
    pub max_nodes_per_pool: u32,

    /// Whether the Deep SHA-256 Scrubber (EE) is allowed.
    pub allow_deep_scrubber: bool,

    /// Whether zero-downtime rolling upgrades (EE) are allowed.
    pub allow_rolling_upgrade: bool,

    /// Whether automated node replacement (EE) is allowed.
    pub allow_auto_node_replace: bool,

    /// Whether LDAP/SAML/OIDC authentication (EE) is allowed.
    pub allow_external_auth: bool,

    /// Whether WebDAV access (EE) is allowed.
    pub allow_webdav: bool,

    /// Whether the audit log (EE) is allowed.
    pub allow_audit_log: bool,
}

impl ClusterLimits {
    /// Community Edition defaults: 1 pool, 3 nodes, no EE features.
    pub fn community() -> Self {
        Self {
            is_enterprise: false,
            max_pools: 1,
            max_nodes_per_pool: 3,
            allow_deep_scrubber: false,
            allow_rolling_upgrade: false,
            allow_auto_node_replace: false,
            allow_external_auth: false,
            allow_webdav: false,
            allow_audit_log: false,
        }
    }

    /// Unlimited limits — used by EE with a valid full license.
    ///
    /// In practice, EE limits come from the license payload, not this method.
    /// This is provided for testing convenience.
    pub fn unlimited() -> Self {
        Self {
            is_enterprise: true,
            max_pools: u32::MAX,
            max_nodes_per_pool: u32::MAX,
            allow_deep_scrubber: true,
            allow_rolling_upgrade: true,
            allow_auto_node_replace: true,
            allow_external_auth: true,
            allow_webdav: true,
            allow_audit_log: true,
        }
    }

    /// Check whether this is running in Community Edition mode.
    pub fn is_community(&self) -> bool {
        !self.is_enterprise
    }
}

impl Default for ClusterLimits {
    fn default() -> Self {
        Self::community()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn community_limits_are_default() {
        assert_eq!(ClusterLimits::default(), ClusterLimits::community());
    }

    #[test]
    fn community_limits_values() {
        let limits = ClusterLimits::community();
        assert!(!limits.is_enterprise);
        assert_eq!(limits.max_pools, 1);
        assert_eq!(limits.max_nodes_per_pool, 3);
        assert!(!limits.allow_deep_scrubber);
        assert!(!limits.allow_rolling_upgrade);
        assert!(!limits.allow_auto_node_replace);
        assert!(!limits.allow_external_auth);
        assert!(!limits.allow_webdav);
        assert!(!limits.allow_audit_log);
    }

    #[test]
    fn unlimited_has_all_features() {
        let limits = ClusterLimits::unlimited();
        assert!(limits.is_enterprise);
        assert_eq!(limits.max_pools, u32::MAX);
        assert_eq!(limits.max_nodes_per_pool, u32::MAX);
        assert!(limits.allow_deep_scrubber);
        assert!(limits.allow_rolling_upgrade);
        assert!(limits.allow_auto_node_replace);
        assert!(limits.allow_external_auth);
        assert!(limits.allow_webdav);
        assert!(limits.allow_audit_log);
    }

    #[test]
    fn is_community_detects_ce() {
        assert!(ClusterLimits::community().is_community());
        assert!(!ClusterLimits::unlimited().is_community());
    }

    #[test]
    fn ee_with_minimal_limits_not_community() {
        // An EE license that grants the same numeric limits as CE
        // must still be identified as Enterprise (not Community).
        let limits = ClusterLimits {
            is_enterprise: true,
            max_pools: 1,
            max_nodes_per_pool: 3,
            ..ClusterLimits::community()
        };
        assert!(!limits.is_community());
    }

    #[test]
    fn serialization_round_trip() {
        let limits = ClusterLimits::community();
        let json = serde_json::to_string(&limits).unwrap();
        let restored: ClusterLimits = serde_json::from_str(&json).unwrap();
        assert_eq!(limits, restored);
    }
}
