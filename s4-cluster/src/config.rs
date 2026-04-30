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

//! Declarative cluster configuration.
//!
//! A cluster is composed of one or more **server pools**. Each pool is an
//! immutable set of nodes that share data via quorum replication.
//! Horizontal scaling is achieved by adding new pools — existing pools
//! never change membership.

use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

use crate::capability::ClusterLimits;
use crate::error::ClusterError;
use crate::identity::{NodeAddr, PoolId};

/// Default replication factor.
pub const DEFAULT_REPLICATION_FACTOR: u8 = 3;
/// Default write quorum.
pub const DEFAULT_WRITE_QUORUM: u8 = 2;
/// Default read quorum.
pub const DEFAULT_READ_QUORUM: u8 = 2;

// ---------------------------------------------------------------------------
// ClusterConfig
// ---------------------------------------------------------------------------

/// Top-level cluster configuration.
///
/// Declared at cluster creation time. The `epoch` is bumped on every
/// configuration change so that nodes can detect stale topology views.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    /// Cluster name — isolates multiple clusters on the same network.
    pub cluster_name: String,

    /// Seed node addresses for initial gossip discovery.
    /// Two or three seeds are sufficient; listing every node is not required.
    pub seeds: Vec<SocketAddr>,

    /// Server pool definitions.
    pub pools: Vec<PoolConfig>,

    /// Global default replication factor.
    pub default_replication_factor: u8,

    /// Global default write quorum size.
    pub default_write_quorum: u8,

    /// Global default read quorum size.
    pub default_read_quorum: u8,

    /// Monotonically increasing topology epoch.
    pub epoch: u64,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            cluster_name: "s4-cluster".to_owned(),
            seeds: Vec::new(),
            pools: Vec::new(),
            default_replication_factor: DEFAULT_REPLICATION_FACTOR,
            default_write_quorum: DEFAULT_WRITE_QUORUM,
            default_read_quorum: DEFAULT_READ_QUORUM,
            epoch: 1,
        }
    }
}

impl ClusterConfig {
    /// Validate the configuration, returning an error on the first
    /// constraint violation.
    pub fn validate(&self) -> Result<(), ClusterError> {
        if self.cluster_name.is_empty() {
            return Err(ClusterError::Config(
                "cluster_name must not be empty".into(),
            ));
        }

        self.validate_quorum(
            self.default_replication_factor,
            self.default_write_quorum,
            self.default_read_quorum,
        )?;

        for pool in &self.pools {
            pool.validate(self)?;
        }

        // Pool IDs must be unique.
        let mut seen_ids = std::collections::HashSet::new();
        for pool in &self.pools {
            if !seen_ids.insert(pool.pool_id) {
                return Err(ClusterError::Config(format!(
                    "duplicate pool ID: {}",
                    pool.pool_id
                )));
            }
        }

        Ok(())
    }

    /// Validate quorum parameters.
    ///
    /// Requirements:
    /// - `rf >= 1`
    /// - `wq >= 1`, `rq >= 1`
    /// - `wq + rq > rf`   (read-your-writes guarantee)
    /// - `2 * wq > rf`    (split-brain write safety)
    fn validate_quorum(&self, rf: u8, wq: u8, rq: u8) -> Result<(), ClusterError> {
        if rf == 0 {
            return Err(ClusterError::Config(
                "replication factor must be at least 1".into(),
            ));
        }
        if wq == 0 || rq == 0 {
            return Err(ClusterError::Config(
                "write and read quorums must be at least 1".into(),
            ));
        }
        if wq + rq <= rf {
            return Err(ClusterError::Config(format!(
                "quorum intersection violated: W({wq}) + R({rq}) must be > RF({rf})"
            )));
        }
        if 2 * u16::from(wq) <= u16::from(rf) {
            return Err(ClusterError::Config(format!(
                "split-brain safety violated: 2*W({}) must be > RF({rf})",
                2 * u16::from(wq)
            )));
        }
        Ok(())
    }

    /// Validate the configuration against edition-specific limits.
    ///
    /// Calls [`validate()`](Self::validate) first, then checks that the
    /// topology does not exceed the provided [`ClusterLimits`].
    pub fn validate_with_limits(&self, limits: &ClusterLimits) -> Result<(), ClusterError> {
        // Run standard validation first.
        self.validate()?;

        // Check pool count.
        if self.pools.len() as u32 > limits.max_pools {
            let edition_hint = if limits.is_community() {
                "Community Edition supports 1 pool. \
                 Upgrade to Enterprise Edition for unlimited pools."
            } else {
                "Upgrade your Enterprise license for more pools."
            };
            return Err(ClusterError::LicenseLimit(format!(
                "topology has {} pool(s) but the current license allows max {}. {edition_hint}",
                self.pools.len(),
                limits.max_pools,
            )));
        }

        // Check node count per pool.
        for pool in &self.pools {
            if pool.nodes.len() as u32 > limits.max_nodes_per_pool {
                let edition_hint = if limits.is_community() {
                    format!(
                        "Community Edition supports up to {} nodes per pool. \
                         Upgrade to Enterprise Edition for unlimited nodes.",
                        limits.max_nodes_per_pool,
                    )
                } else {
                    "Upgrade your Enterprise license for more nodes.".into()
                };
                return Err(ClusterError::LicenseLimit(format!(
                    "pool '{}' has {} node(s) but the current license allows max {}. {edition_hint}",
                    pool.name,
                    pool.nodes.len(),
                    limits.max_nodes_per_pool,
                )));
            }
        }

        Ok(())
    }

    /// Resolve effective quorum parameters for a pool, falling back to
    /// cluster-level defaults.
    pub fn effective_quorum(&self, pool: &PoolConfig) -> (u8, u8, u8) {
        let rf = pool.replication_factor.unwrap_or(self.default_replication_factor);
        let wq = pool.write_quorum.unwrap_or(self.default_write_quorum);
        let rq = pool.read_quorum.unwrap_or(self.default_read_quorum);
        (rf, wq, rq)
    }
}

// ---------------------------------------------------------------------------
// PoolConfig
// ---------------------------------------------------------------------------

/// Configuration for a single server pool.
///
/// A pool is an immutable set of nodes. Once created, its membership
/// **never changes**. This eliminates the need for data rebalancing
/// within a pool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolConfig {
    /// Unique pool identifier.
    pub pool_id: PoolId,

    /// Human-readable pool name.
    pub name: String,

    /// Nodes belonging to this pool. Fixed after creation.
    pub nodes: Vec<NodeAddr>,

    /// Override the cluster-level replication factor for this pool.
    pub replication_factor: Option<u8>,

    /// Override the cluster-level write quorum for this pool.
    pub write_quorum: Option<u8>,

    /// Override the cluster-level read quorum for this pool.
    pub read_quorum: Option<u8>,
}

impl PoolConfig {
    /// Validate this pool against the cluster configuration.
    fn validate(&self, cluster: &ClusterConfig) -> Result<(), ClusterError> {
        if self.name.is_empty() {
            return Err(ClusterError::Config(format!(
                "pool {} name must not be empty",
                self.pool_id
            )));
        }

        if self.nodes.is_empty() {
            return Err(ClusterError::Config(format!(
                "pool {} must have at least one node",
                self.pool_id
            )));
        }

        let (rf, wq, rq) = cluster.effective_quorum(self);

        if self.nodes.len() < usize::from(rf) {
            return Err(ClusterError::Config(format!(
                "pool {} has {} nodes but replication factor is {rf}",
                self.pool_id,
                self.nodes.len()
            )));
        }

        // Validate overridden quorum if any value is specified.
        if self.replication_factor.is_some()
            || self.write_quorum.is_some()
            || self.read_quorum.is_some()
        {
            cluster.validate_quorum(rf, wq, rq)?;
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// AntiEntropyConfig
// ---------------------------------------------------------------------------

/// Configuration for the background anti-entropy service (Phase 7).
///
/// Controls how frequently Merkle tree exchanges happen and the size of
/// repair batches. The defaults are tuned for the 99.9% SLA: with a
/// 10-minute interval, divergences are detected within one hour with
/// probability > 99.9%.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AntiEntropyConfig {
    /// Whether anti-entropy is enabled.
    pub enabled: bool,

    /// Interval between Merkle tree exchange rounds (default: 10 min).
    ///
    /// Each round, the node compares its Merkle tree with one random
    /// replica peer per key range.
    pub interval_secs: u64,

    /// Depth of the Merkle tree (default: 15 → 32768 leaf buckets).
    ///
    /// Higher values give finer-grained divergence detection but use
    /// more memory and storage.
    pub merkle_depth: u8,

    /// Maximum number of keys to synchronize per repair round.
    ///
    /// Limits the amount of data transferred in a single anti-entropy
    /// cycle to avoid saturating the network.
    pub max_keys_per_batch: u32,

    /// Maximum number of concurrent repair streams.
    pub max_concurrent_repairs: u32,
}

impl Default for AntiEntropyConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval_secs: 600, // 10 minutes
            merkle_depth: 15,
            max_keys_per_batch: 1000,
            max_concurrent_repairs: 4,
        }
    }
}

impl AntiEntropyConfig {
    /// Validate the configuration.
    pub fn validate(&self) -> Result<(), ClusterError> {
        if self.merkle_depth == 0 || self.merkle_depth > 24 {
            return Err(ClusterError::Config(format!(
                "merkle_depth must be 1..=24, got {}",
                self.merkle_depth
            )));
        }
        if self.interval_secs == 0 {
            return Err(ClusterError::Config(
                "anti-entropy interval must be > 0".into(),
            ));
        }
        if self.max_keys_per_batch == 0 {
            return Err(ClusterError::Config(
                "max_keys_per_batch must be > 0".into(),
            ));
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, SocketAddrV4};

    fn test_addr(port: u16) -> NodeAddr {
        let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port));
        NodeAddr {
            grpc_addr: addr,
            http_addr: addr,
            gossip_addr: addr,
        }
    }

    fn valid_pool(id: u32, node_count: usize) -> PoolConfig {
        PoolConfig {
            pool_id: PoolId(id),
            name: format!("pool-{id}"),
            nodes: (0..node_count).map(|i| test_addr(9000 + i as u16)).collect(),
            replication_factor: None,
            write_quorum: None,
            read_quorum: None,
        }
    }

    #[test]
    fn valid_config_passes_validation() {
        let config = ClusterConfig {
            pools: vec![valid_pool(1, 3)],
            ..Default::default()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn empty_cluster_name_rejected() {
        let config = ClusterConfig {
            cluster_name: String::new(),
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn quorum_intersection_violated() {
        let config = ClusterConfig {
            default_replication_factor: 3,
            default_write_quorum: 1,
            default_read_quorum: 1,
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("quorum intersection"));
    }

    #[test]
    fn split_brain_safety_violated() {
        let config = ClusterConfig {
            default_replication_factor: 3,
            default_write_quorum: 1,
            default_read_quorum: 3,
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("split-brain"));
    }

    #[test]
    fn pool_with_too_few_nodes_rejected() {
        let config = ClusterConfig {
            pools: vec![valid_pool(1, 2)], // 2 nodes < RF=3
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("replication factor"));
    }

    #[test]
    fn duplicate_pool_ids_rejected() {
        let config = ClusterConfig {
            pools: vec![valid_pool(1, 3), valid_pool(1, 3)],
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("duplicate pool ID"));
    }

    #[test]
    fn pool_quorum_override_validated() {
        let config = ClusterConfig {
            pools: vec![PoolConfig {
                replication_factor: Some(3),
                write_quorum: Some(1),
                read_quorum: Some(1),
                ..valid_pool(1, 3)
            }],
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn effective_quorum_uses_defaults() {
        let config = ClusterConfig::default();
        let pool = valid_pool(1, 3);
        assert_eq!(config.effective_quorum(&pool), (3, 2, 2));
    }

    #[test]
    fn effective_quorum_uses_overrides() {
        let config = ClusterConfig::default();
        let pool = PoolConfig {
            replication_factor: Some(5),
            write_quorum: Some(3),
            read_quorum: Some(3),
            ..valid_pool(1, 5)
        };
        assert_eq!(config.effective_quorum(&pool), (5, 3, 3));
    }

    #[test]
    fn single_node_config_valid() {
        let config = ClusterConfig {
            default_replication_factor: 1,
            default_write_quorum: 1,
            default_read_quorum: 1,
            pools: vec![valid_pool(1, 1)],
            ..Default::default()
        };
        assert!(config.validate().is_ok());
    }

    // -----------------------------------------------------------------------
    // validate_with_limits tests
    // -----------------------------------------------------------------------

    #[test]
    fn ce_limits_accept_single_pool_three_nodes() {
        let config = ClusterConfig {
            pools: vec![valid_pool(1, 3)],
            ..Default::default()
        };
        let limits = ClusterLimits::community();
        assert!(config.validate_with_limits(&limits).is_ok());
    }

    #[test]
    fn ce_limits_reject_two_pools() {
        let config = ClusterConfig {
            pools: vec![valid_pool(1, 3), valid_pool(2, 3)],
            ..Default::default()
        };
        let limits = ClusterLimits::community();
        let err = config.validate_with_limits(&limits).unwrap_err();
        assert!(err.to_string().contains("license"));
        assert!(err.to_string().contains("pool"));
    }

    #[test]
    fn ce_limits_reject_four_nodes_per_pool() {
        let config = ClusterConfig {
            default_replication_factor: 3,
            default_write_quorum: 2,
            default_read_quorum: 2,
            pools: vec![valid_pool(1, 4)],
            ..Default::default()
        };
        let limits = ClusterLimits::community();
        let err = config.validate_with_limits(&limits).unwrap_err();
        assert!(err.to_string().contains("license"));
        assert!(err.to_string().contains("node"));
    }

    #[test]
    fn unlimited_limits_accept_many_pools() {
        let config = ClusterConfig {
            pools: vec![valid_pool(1, 3), valid_pool(2, 3), valid_pool(3, 3)],
            ..Default::default()
        };
        let limits = ClusterLimits::unlimited();
        assert!(config.validate_with_limits(&limits).is_ok());
    }

    #[test]
    fn custom_ee_limits_enforce_specific_caps() {
        let limits = ClusterLimits {
            is_enterprise: true,
            max_pools: 5,
            max_nodes_per_pool: 10,
            ..ClusterLimits::community()
        };

        // 5 pools with 3 nodes each — within limits.
        let config = ClusterConfig {
            pools: (1..=5).map(|i| valid_pool(i, 3)).collect(),
            ..Default::default()
        };
        assert!(config.validate_with_limits(&limits).is_ok());

        // 6 pools — exceeds limit.
        let config = ClusterConfig {
            pools: (1..=6).map(|i| valid_pool(i, 3)).collect(),
            ..Default::default()
        };
        assert!(config.validate_with_limits(&limits).is_err());
    }

    #[test]
    fn validate_with_limits_runs_base_validation_first() {
        // Empty cluster name should fail even with unlimited limits.
        let config = ClusterConfig {
            cluster_name: String::new(),
            ..Default::default()
        };
        let err = config.validate_with_limits(&ClusterLimits::unlimited()).unwrap_err();
        assert!(err.to_string().contains("cluster_name"));
    }
}
