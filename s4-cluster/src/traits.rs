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

//! Trait boundaries for CE/EE behavioral features.
//!
//! Business logic in `s4-cluster` and `s4-core` interacts with these traits
//! via trait objects (`Box<dyn Trait>`). The concrete implementation is
//! injected at startup in `s4-server`:
//!
//! - **CE build**: Noop stubs defined in this module.
//! - **EE build with valid license**: Real implementations from `ee/s4-ee/`.
//! - **EE build without license**: Falls back to Noop stubs (CE mode).
//!
//! `#[cfg(feature = "enterprise")]` appears **only** in `s4-server/src/main.rs`.
//! Domain code never checks the edition directly.

use std::fmt;

use crate::config::PoolConfig;
use crate::error::ClusterError;
use crate::identity::NodeId;

// ---------------------------------------------------------------------------
// Deep Scrubber — background SHA-256 integrity verification with healing
// ---------------------------------------------------------------------------

/// Background deep integrity scrubber (EE feature).
///
/// Walks all stored objects, computes SHA-256, compares against the
/// content-addressable hash, and heals corrupt replicas from healthy peers.
pub trait DeepScrubber: Send + Sync + 'static {
    /// Start the background scrubber task.
    fn start(&self) -> Result<(), ClusterError>;

    /// Stop the background scrubber gracefully.
    fn stop(&self) -> Result<(), ClusterError>;

    /// Returns `true` if the scrubber is currently running.
    fn is_running(&self) -> bool;
}

/// CE stub: logs that the feature requires Enterprise and returns `Ok(())`.
pub struct NoopDeepScrubber;

impl DeepScrubber for NoopDeepScrubber {
    fn start(&self) -> Result<(), ClusterError> {
        tracing::info!("Deep SHA-256 scrubber requires an Enterprise license");
        Ok(())
    }

    fn stop(&self) -> Result<(), ClusterError> {
        Ok(())
    }

    fn is_running(&self) -> bool {
        false
    }
}

impl fmt::Debug for NoopDeepScrubber {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("NoopDeepScrubber")
    }
}

// ---------------------------------------------------------------------------
// Node Replacer — automated dead-node replacement workflow
// ---------------------------------------------------------------------------

/// Automated node replacement (EE feature).
///
/// Performs drain + remove + add + heal as a single operation with
/// progress reporting. CE uses manual 3-step replacement via Admin API.
pub trait NodeReplacer: Send + Sync + 'static {
    /// Replace a dead node in a pool with a new node.
    ///
    /// This is an atomic operation: drain the old node's responsibilities,
    /// remove it from the pool, add the new node, and trigger repair sync.
    fn replace_node(
        &self,
        pool: &PoolConfig,
        old_node: NodeId,
        new_node: NodeId,
    ) -> Result<(), ClusterError>;
}

/// CE stub: returns an error indicating this is an EE feature.
pub struct NoopNodeReplacer;

impl NodeReplacer for NoopNodeReplacer {
    fn replace_node(
        &self,
        _pool: &PoolConfig,
        _old_node: NodeId,
        _new_node: NodeId,
    ) -> Result<(), ClusterError> {
        Err(ClusterError::EnterpriseRequired(
            "automated node replacement requires an Enterprise license; \
             use the manual 3-step process via Admin API instead"
                .into(),
        ))
    }
}

impl fmt::Debug for NoopNodeReplacer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("NoopNodeReplacer")
    }
}

// ---------------------------------------------------------------------------
// Rolling Upgrader — zero-downtime rolling upgrade orchestration
// ---------------------------------------------------------------------------

/// Zero-downtime rolling upgrade orchestrator (EE feature).
///
/// Coordinates version upgrades across the cluster one node at a time,
/// ensuring quorum is maintained throughout the process.
pub trait RollingUpgrader: Send + Sync + 'static {
    /// Check whether a rolling upgrade is safe to start.
    fn preflight_check(&self) -> Result<(), ClusterError>;

    /// Begin orchestrated rolling upgrade of the cluster.
    fn start_upgrade(&self) -> Result<(), ClusterError>;
}

/// CE stub: returns an error indicating this is an EE feature.
pub struct NoopRollingUpgrader;

impl RollingUpgrader for NoopRollingUpgrader {
    fn preflight_check(&self) -> Result<(), ClusterError> {
        Err(ClusterError::EnterpriseRequired(
            "rolling upgrades require an Enterprise license; \
             use manual node-by-node restart instead"
                .into(),
        ))
    }

    fn start_upgrade(&self) -> Result<(), ClusterError> {
        Err(ClusterError::EnterpriseRequired(
            "rolling upgrades require an Enterprise license".into(),
        ))
    }
}

impl fmt::Debug for NoopRollingUpgrader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("NoopRollingUpgrader")
    }
}

// ---------------------------------------------------------------------------
// Audit Logger — structured audit trail for compliance
// ---------------------------------------------------------------------------

/// Structured audit logger for compliance (EE feature).
///
/// Records administrative and data-access events for regulatory compliance.
pub trait AuditLogger: Send + Sync + 'static {
    /// Log an audit event. In CE mode this is a no-op.
    fn log_event(&self, event: &AuditEvent);
}

/// An audit event to be recorded.
#[derive(Debug, Clone)]
pub struct AuditEvent {
    /// Who performed the action (user ID or API key).
    pub actor: String,
    /// What action was performed.
    pub action: String,
    /// The target resource (bucket, object, user, etc.).
    pub resource: String,
    /// Whether the action succeeded.
    pub success: bool,
    /// Optional additional details.
    pub detail: Option<String>,
}

/// CE stub: silently discards audit events.
pub struct NoopAuditLogger;

impl AuditLogger for NoopAuditLogger {
    fn log_event(&self, _event: &AuditEvent) {
        // CE: audit logging is an Enterprise feature; events are discarded.
    }
}

impl fmt::Debug for NoopAuditLogger {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("NoopAuditLogger")
    }
}

// ---------------------------------------------------------------------------
// ClusterServices — DI container for trait objects
// ---------------------------------------------------------------------------

/// Dependency-injection container for edition-dependent cluster services.
///
/// Created at startup in `s4-server` and passed through the application.
/// Business logic uses these trait objects without knowing the edition.
pub struct ClusterServices {
    /// Deep SHA-256 integrity scrubber.
    pub deep_scrubber: Box<dyn DeepScrubber>,

    /// Automated node replacement.
    pub node_replacer: Box<dyn NodeReplacer>,

    /// Rolling upgrade orchestrator.
    pub rolling_upgrader: Box<dyn RollingUpgrader>,

    /// Audit event logger.
    pub audit_logger: Box<dyn AuditLogger>,
}

impl ClusterServices {
    /// Create a CE-default service container with all Noop implementations.
    pub fn community() -> Self {
        Self {
            deep_scrubber: Box::new(NoopDeepScrubber),
            node_replacer: Box::new(NoopNodeReplacer),
            rolling_upgrader: Box::new(NoopRollingUpgrader),
            audit_logger: Box::new(NoopAuditLogger),
        }
    }
}

impl fmt::Debug for ClusterServices {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClusterServices")
            .field("deep_scrubber", &"<dyn DeepScrubber>")
            .field("node_replacer", &"<dyn NodeReplacer>")
            .field("rolling_upgrader", &"<dyn RollingUpgrader>")
            .field("audit_logger", &"<dyn AuditLogger>")
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn noop_deep_scrubber_is_not_running() {
        let scrubber = NoopDeepScrubber;
        assert!(!scrubber.is_running());
        assert!(scrubber.start().is_ok());
        assert!(scrubber.stop().is_ok());
    }

    #[test]
    fn noop_node_replacer_returns_enterprise_error() {
        let replacer = NoopNodeReplacer;
        let pool = PoolConfig {
            pool_id: crate::identity::PoolId(1),
            name: "test".into(),
            nodes: vec![],
            replication_factor: None,
            write_quorum: None,
            read_quorum: None,
        };
        let err = replacer
            .replace_node(&pool, NodeId::generate(), NodeId::generate())
            .unwrap_err();
        assert!(err.to_string().contains("Enterprise license"));
    }

    #[test]
    fn noop_rolling_upgrader_returns_enterprise_error() {
        let upgrader = NoopRollingUpgrader;
        assert!(upgrader.preflight_check().is_err());
        assert!(upgrader.start_upgrade().is_err());
    }

    #[test]
    fn noop_audit_logger_does_not_panic() {
        let logger = NoopAuditLogger;
        logger.log_event(&AuditEvent {
            actor: "test-user".into(),
            action: "PutObject".into(),
            resource: "bucket/key".into(),
            success: true,
            detail: None,
        });
    }

    #[test]
    fn community_services_all_noop() {
        let services = ClusterServices::community();
        assert!(!services.deep_scrubber.is_running());
        assert!(services.rolling_upgrader.preflight_check().is_err());
    }

    #[test]
    fn cluster_services_debug() {
        let services = ClusterServices::community();
        let debug = format!("{services:?}");
        assert!(debug.contains("ClusterServices"));
    }
}
