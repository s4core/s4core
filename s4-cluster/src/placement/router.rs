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

//! Full request routing: `bucket/key → pool → [replica nodes]`.
//!
//! The [`PlacementRouter`] combines bucket-to-pool mapping with
//! per-pool placement strategies to produce a [`PlacementDecision`]
//! for any S3 operation.

use std::collections::HashMap;

use crate::config::{ClusterConfig, PoolConfig};
use crate::error::ClusterError;
use crate::identity::{NodeId, PoolId};
use crate::placement::bucket_placement::BucketPlacement;
use crate::placement::hash_ring::PlacementStrategy;

/// The result of routing a `bucket/key` to its replica set.
#[derive(Debug, Clone)]
pub struct PlacementDecision {
    /// Pool that owns this bucket.
    pub pool_id: PoolId,
    /// Ordered replica list: `[primary, secondary, …]`.
    pub replicas: Vec<NodeId>,
    /// The current node acting as request coordinator.
    pub coordinator: NodeId,
}

/// Full two-level router: bucket → pool → replica nodes.
///
/// Built from [`ClusterConfig`] on startup and rebuilt on topology
/// epoch changes. The router is read-only after construction; bucket
/// assignment mutations go through [`BucketPlacement`] and trigger
/// a rebuild.
#[derive(Debug, Clone)]
pub struct PlacementRouter {
    /// Per-pool placement strategies.
    strategies: HashMap<PoolId, PlacementStrategy>,
    /// Pool configurations indexed by pool ID.
    pool_configs: HashMap<PoolId, PoolConfig>,
    /// Bucket-to-pool mapping.
    bucket_placement: BucketPlacement,
    /// Local node identity (always the coordinator for received requests).
    local_node_id: NodeId,
}

impl PlacementRouter {
    /// Build a router from cluster configuration.
    ///
    /// For each pool, the appropriate [`PlacementStrategy`] is created
    /// based on pool size vs. replication factor.
    ///
    /// # Arguments
    ///
    /// - `config` — cluster topology.
    /// - `local_node_id` — this node's identity (used as coordinator).
    /// - `bucket_placement` — pre-loaded bucket-to-pool mapping.
    /// - `node_id_resolver` — maps node addresses to `NodeId`s. In a
    ///   running cluster this comes from the gossip member table.
    pub fn new(
        config: &ClusterConfig,
        local_node_id: NodeId,
        bucket_placement: BucketPlacement,
        node_id_resolver: &HashMap<std::net::SocketAddr, NodeId>,
    ) -> Result<Self, ClusterError> {
        let mut strategies = HashMap::new();
        let mut pool_configs = HashMap::new();

        for pool in &config.pools {
            let (rf, _, _) = config.effective_quorum(pool);

            let node_ids: Vec<NodeId> = pool
                .nodes
                .iter()
                .map(|addr| {
                    node_id_resolver.get(&addr.grpc_addr).copied().ok_or_else(|| {
                        ClusterError::Placement(format!(
                            "no NodeId for address {} in pool {}",
                            addr.grpc_addr, pool.pool_id,
                        ))
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;

            let strategy = PlacementStrategy::new(node_ids, rf)?;
            strategies.insert(pool.pool_id, strategy);
            pool_configs.insert(pool.pool_id, pool.clone());
        }

        Ok(Self {
            strategies,
            pool_configs,
            bucket_placement,
            local_node_id,
        })
    }

    /// Build a single-node router (no cluster, single pool).
    ///
    /// Used when the server runs in standalone mode. All buckets map
    /// to pool 0 with the local node as the sole replica.
    pub fn single_node(local_node_id: NodeId) -> Self {
        let pool_id = PoolId(0);
        let strategy = PlacementStrategy::FullReplication {
            nodes: vec![local_node_id],
        };

        let mut strategies = HashMap::new();
        strategies.insert(pool_id, strategy);

        Self {
            strategies,
            pool_configs: HashMap::new(),
            bucket_placement: BucketPlacement::new(),
            local_node_id,
        }
    }

    /// Route an existing object: look up the bucket's pool and resolve
    /// the replica set for `bucket/key`.
    pub fn route(&self, bucket: &str, key: &str) -> Result<PlacementDecision, ClusterError> {
        let pool_id = self.resolve_pool(bucket)?;
        self.route_in_pool(pool_id, bucket, key)
    }

    /// Route within a known pool. Used when the pool is already known
    /// (e.g., from `IndexRecord.pool_id`).
    pub fn route_in_pool(
        &self,
        pool_id: PoolId,
        bucket: &str,
        key: &str,
    ) -> Result<PlacementDecision, ClusterError> {
        let strategy = self
            .strategies
            .get(&pool_id)
            .ok_or_else(|| ClusterError::Placement(format!("unknown pool: {pool_id}")))?;

        let placement_key = format!("{bucket}/{key}");
        let replicas = strategy.get_replicas(&placement_key);

        Ok(PlacementDecision {
            pool_id,
            replicas,
            coordinator: self.local_node_id,
        })
    }

    /// Resolve which pool owns a bucket.
    ///
    /// In single-node mode, always returns `PoolId(0)`.
    fn resolve_pool(&self, bucket: &str) -> Result<PoolId, ClusterError> {
        // Single-node: only pool 0 exists.
        if self.strategies.len() == 1 && self.strategies.contains_key(&PoolId(0)) {
            return Ok(PoolId(0));
        }

        self.bucket_placement.get(bucket).ok_or_else(|| {
            ClusterError::Placement(format!("bucket '{}' is not assigned to any pool", bucket,))
        })
    }

    /// Assign a new bucket to the best available pool.
    ///
    /// `pool_capacities` maps `(pool_id, used_bytes, total_bytes)`.
    /// Returns the chosen pool ID.
    pub fn assign_bucket(
        &mut self,
        bucket: &str,
        pool_capacities: &[(PoolId, u64, u64)],
    ) -> Result<PoolId, ClusterError> {
        if let Some(existing) = self.bucket_placement.get(bucket) {
            return Ok(existing);
        }

        let pool_id = BucketPlacement::select_pool(pool_capacities).ok_or_else(|| {
            ClusterError::Placement("no pools available for bucket assignment".into())
        })?;

        if !self.strategies.contains_key(&pool_id) {
            return Err(ClusterError::Placement(format!(
                "selected pool {pool_id} has no placement strategy"
            )));
        }

        self.bucket_placement.assign(bucket, pool_id)?;
        Ok(pool_id)
    }

    /// Remove a bucket assignment.
    pub fn remove_bucket(&mut self, bucket: &str) -> Option<PoolId> {
        self.bucket_placement.remove(bucket)
    }

    /// Access the underlying bucket placement (read-only).
    pub fn bucket_placement(&self) -> &BucketPlacement {
        &self.bucket_placement
    }

    /// Access the placement strategy for a pool.
    pub fn strategy(&self, pool_id: &PoolId) -> Option<&PlacementStrategy> {
        self.strategies.get(pool_id)
    }

    /// Local node ID (coordinator for received requests).
    pub fn local_node_id(&self) -> NodeId {
        self.local_node_id
    }

    /// All known pool IDs.
    pub fn pool_ids(&self) -> Vec<PoolId> {
        self.strategies.keys().copied().collect()
    }

    /// Access a pool configuration by ID.
    pub fn pool_config(&self, pool_id: &PoolId) -> Option<&PoolConfig> {
        self.pool_configs.get(pool_id)
    }

    /// Return all physical nodes in the pool that owns the given bucket.
    pub fn pool_nodes(&self, bucket: &str) -> Result<Vec<NodeId>, ClusterError> {
        let pool_id = self.resolve_pool(bucket)?;
        let strategy = self
            .strategies
            .get(&pool_id)
            .ok_or_else(|| ClusterError::Placement(format!("unknown pool: {pool_id}")))?;
        Ok(strategy.all_nodes().to_vec())
    }

    /// Check whether the local node is a replica for the given key.
    pub fn is_local_replica(&self, bucket: &str, key: &str) -> Result<bool, ClusterError> {
        let decision = self.route(bucket, key)?;
        Ok(decision.replicas.contains(&self.local_node_id))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ClusterConfig;
    use crate::identity::NodeAddr;
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

    fn addr(port: u16) -> SocketAddr {
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port))
    }

    fn node_addr(port: u16) -> NodeAddr {
        NodeAddr {
            grpc_addr: addr(port),
            http_addr: addr(port + 1000),
            gossip_addr: addr(port + 2000),
        }
    }

    fn setup_three_node_cluster() -> (PlacementRouter, [NodeId; 3]) {
        let nodes = [NodeId(1), NodeId(2), NodeId(3)];
        let ports = [9100, 9101, 9102];

        let mut resolver = HashMap::new();
        for (i, &node) in nodes.iter().enumerate() {
            resolver.insert(addr(ports[i]), node);
        }

        let pool = PoolConfig {
            pool_id: PoolId(1),
            name: "pool-1".into(),
            nodes: ports.iter().map(|&p| node_addr(p)).collect(),
            replication_factor: None,
            write_quorum: None,
            read_quorum: None,
        };

        let config = ClusterConfig {
            pools: vec![pool],
            ..ClusterConfig::default()
        };

        let mut bp = BucketPlacement::new();
        bp.assign("test-bucket", PoolId(1)).unwrap();

        let router = PlacementRouter::new(&config, NodeId(1), bp, &resolver).unwrap();
        (router, nodes)
    }

    // -- Single-node mode -------------------------------------------------

    #[test]
    fn single_node_routes_to_self() {
        let node = NodeId(42);
        let router = PlacementRouter::single_node(node);
        let decision = router.route("any-bucket", "any-key").unwrap();
        assert_eq!(decision.pool_id, PoolId(0));
        assert_eq!(decision.replicas, vec![node]);
        assert_eq!(decision.coordinator, node);
    }

    // -- Three-node cluster -----------------------------------------------

    #[test]
    fn three_node_full_replication() {
        let (router, nodes) = setup_three_node_cluster();
        let decision = router.route("test-bucket", "hello.txt").unwrap();
        assert_eq!(decision.pool_id, PoolId(1));
        assert_eq!(decision.replicas.len(), 3);
        for node in &nodes {
            assert!(decision.replicas.contains(node));
        }
    }

    #[test]
    fn unassigned_bucket_rejected() {
        let (router, _) = setup_three_node_cluster();
        let err = router.route("unknown-bucket", "key").unwrap_err();
        assert!(err.to_string().contains("not assigned"));
    }

    #[test]
    fn coordinator_is_local_node() {
        let (router, _) = setup_three_node_cluster();
        let decision = router.route("test-bucket", "key").unwrap();
        assert_eq!(decision.coordinator, NodeId(1));
    }

    // -- Bucket assignment ------------------------------------------------

    #[test]
    fn assign_bucket_picks_pool() {
        let (mut router, _) = setup_three_node_cluster();
        let capacities = vec![(PoolId(1), 100, 1000)];
        let pool = router.assign_bucket("new-bucket", &capacities).unwrap();
        assert_eq!(pool, PoolId(1));

        // Now routing works.
        let decision = router.route("new-bucket", "file.txt").unwrap();
        assert_eq!(decision.pool_id, PoolId(1));
    }

    #[test]
    fn assign_bucket_idempotent() {
        let (mut router, _) = setup_three_node_cluster();
        let caps = vec![(PoolId(1), 0, 1000)];
        let p1 = router.assign_bucket("b", &caps).unwrap();
        let p2 = router.assign_bucket("b", &caps).unwrap();
        assert_eq!(p1, p2);
    }

    #[test]
    fn assign_bucket_no_pools_fails() {
        let (mut router, _) = setup_three_node_cluster();
        let err = router.assign_bucket("b", &[]).unwrap_err();
        assert!(err.to_string().contains("no pools available"));
    }

    // -- Remove bucket ----------------------------------------------------

    #[test]
    fn remove_bucket() {
        let (mut router, _) = setup_three_node_cluster();
        assert_eq!(router.remove_bucket("test-bucket"), Some(PoolId(1)));
        assert!(router.route("test-bucket", "key").is_err());
    }

    // -- is_local_replica -------------------------------------------------

    #[test]
    fn is_local_replica_in_full_replication() {
        let (router, _) = setup_three_node_cluster();
        assert!(router.is_local_replica("test-bucket", "key").unwrap());
    }

    // -- Pool IDs ---------------------------------------------------------

    #[test]
    fn pool_ids_returns_all() {
        let (router, _) = setup_three_node_cluster();
        assert_eq!(router.pool_ids(), vec![PoolId(1)]);
    }
}
