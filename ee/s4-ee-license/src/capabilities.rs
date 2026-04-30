// Copyright 2026 S4Core Team
//
// Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

//! Enterprise capabilities extracted from a validated license.

use serde::{Deserialize, Serialize};

/// Capabilities granted by an enterprise license.
///
/// Each field corresponds to a specific EE feature or limit.
/// When no valid license is present, `Capabilities::community()` is used,
/// which enforces CE limits (1 pool, 3 nodes, no EE features).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Capabilities {
    /// Maximum number of server pools allowed.
    pub max_pools: u32,
    /// Maximum number of nodes per server pool.
    pub max_nodes_per_pool: u32,
    /// Enable deep SHA-256 background scrubber with replica healing.
    pub deep_scrubber: bool,
    /// Enable zero-downtime rolling upgrades.
    pub rolling_upgrades: bool,
    /// Enable LDAP authentication integration.
    pub ldap: bool,
    /// Enable SAML/OIDC SSO integration.
    pub saml: bool,
    /// Enable WebDAV protocol support.
    pub webdav: bool,
    /// Enable audit logging.
    pub audit_log: bool,
}

impl Capabilities {
    /// Returns CE-mode capabilities: 1 pool, 3 nodes, no EE features.
    pub fn community() -> Self {
        Self {
            max_pools: 1,
            max_nodes_per_pool: 3,
            deep_scrubber: false,
            rolling_upgrades: false,
            ldap: false,
            saml: false,
            webdav: false,
            audit_log: false,
        }
    }

    /// Returns full capabilities for development and testing.
    #[cfg(any(test, debug_assertions))]
    pub fn development() -> Self {
        Self {
            max_pools: u32::MAX,
            max_nodes_per_pool: u32::MAX,
            deep_scrubber: true,
            rolling_upgrades: true,
            ldap: true,
            saml: true,
            webdav: true,
            audit_log: true,
        }
    }
}

impl Default for Capabilities {
    fn default() -> Self {
        Self::community()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn community_limits_are_correct() {
        let caps = Capabilities::community();
        assert_eq!(caps.max_pools, 1);
        assert_eq!(caps.max_nodes_per_pool, 3);
        assert!(!caps.deep_scrubber);
        assert!(!caps.rolling_upgrades);
        assert!(!caps.ldap);
        assert!(!caps.saml);
        assert!(!caps.webdav);
        assert!(!caps.audit_log);
    }

    #[test]
    fn development_unlocks_everything() {
        let caps = Capabilities::development();
        assert_eq!(caps.max_pools, u32::MAX);
        assert_eq!(caps.max_nodes_per_pool, u32::MAX);
        assert!(caps.deep_scrubber);
        assert!(caps.rolling_upgrades);
        assert!(caps.ldap);
        assert!(caps.saml);
        assert!(caps.webdav);
        assert!(caps.audit_log);
    }
}
