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

//! Key-to-replica mapping within a single server pool.
//!
//! Two strategies are supported:
//!
//! - [`PlacementStrategy::FullReplication`] — used when `pool_size == RF`
//!   (the MVP default). Every node in the pool stores every key. Lookup is O(1).
//! - [`PlacementStrategy::HashRing`] — used when `pool_size > RF`. A
//!   consistent hash ring with virtual nodes selects `RF` distinct physical
//!   nodes per key. Lookup is O(log V) where V = total virtual nodes.

use crate::error::ClusterError;
use crate::identity::NodeId;

/// Default number of virtual nodes per physical node on the hash ring.
///
/// 128 vnodes gives good distribution uniformity even with a small
/// number of physical nodes.
const DEFAULT_VNODES_PER_NODE: u32 = 128;

/// Routing strategy for mapping keys to replica nodes within a pool.
///
/// Constructed by [`PlacementStrategy::new`] which automatically picks
/// the right variant based on pool size vs. replication factor.
#[derive(Debug, Clone)]
pub enum PlacementStrategy {
    /// Every node in the pool is a replica (pool_size == RF).
    ///
    /// This is the recommended MVP deployment (3 nodes, RF=3). No hash
    /// ring overhead — any key maps to all pool members.
    FullReplication {
        /// Sorted node list (deterministic ordering across cluster).
        nodes: Vec<NodeId>,
    },

    /// Consistent hash ring (pool_size > RF).
    ///
    /// Each physical node is mapped to `vnodes_per_node` positions on a
    /// 64-bit ring. For a given key, the `rf` closest distinct physical
    /// nodes (walking clockwise) are selected as replicas.
    HashRing(ConsistentHashRing),
}

impl PlacementStrategy {
    /// Build the appropriate strategy for the given pool topology.
    ///
    /// Returns [`FullReplication`](PlacementStrategy::FullReplication) when
    /// `nodes.len() == rf`, and [`HashRing`](PlacementStrategy::HashRing)
    /// otherwise.
    ///
    /// # Errors
    ///
    /// - Empty node list.
    /// - `rf` is zero or exceeds the number of nodes.
    pub fn new(nodes: Vec<NodeId>, rf: u8) -> Result<Self, ClusterError> {
        if nodes.is_empty() {
            return Err(ClusterError::Placement(
                "node list must not be empty".into(),
            ));
        }
        if rf == 0 {
            return Err(ClusterError::Placement(
                "replication factor must be at least 1".into(),
            ));
        }
        if (rf as usize) > nodes.len() {
            return Err(ClusterError::Placement(format!(
                "replication factor ({rf}) exceeds node count ({})",
                nodes.len()
            )));
        }

        if nodes.len() == rf as usize {
            let mut sorted = nodes;
            sorted.sort_by_key(|n| n.0);
            Ok(Self::FullReplication { nodes: sorted })
        } else {
            Ok(Self::HashRing(ConsistentHashRing::new(
                nodes,
                rf,
                DEFAULT_VNODES_PER_NODE,
            )?))
        }
    }

    /// Return the ordered list of replica nodes for the given key.
    ///
    /// The first element is the "primary" replica (used as coordinator
    /// tiebreaker). The list length equals the replication factor.
    pub fn get_replicas(&self, key: &str) -> Vec<NodeId> {
        match self {
            Self::FullReplication { nodes } => nodes.clone(),
            Self::HashRing(ring) => ring.get_replicas(key),
        }
    }

    /// Number of replicas returned per key.
    pub fn replication_factor(&self) -> u8 {
        match self {
            Self::FullReplication { nodes } => nodes.len() as u8,
            Self::HashRing(ring) => ring.rf,
        }
    }

    /// Total number of physical nodes in this strategy.
    pub fn node_count(&self) -> usize {
        match self {
            Self::FullReplication { nodes } => nodes.len(),
            Self::HashRing(ring) => ring.node_count(),
        }
    }

    /// Return all physical nodes managed by this strategy.
    pub fn all_nodes(&self) -> &[NodeId] {
        match self {
            Self::FullReplication { nodes } => nodes,
            Self::HashRing(ring) => ring.all_nodes(),
        }
    }
}

// ---------------------------------------------------------------------------
// ConsistentHashRing
// ---------------------------------------------------------------------------

/// Consistent hash ring with virtual nodes for uniform key distribution.
///
/// Each physical node is hashed onto `vnodes_per_node` positions on a
/// 64-bit ring using BLAKE3. For a given key, we walk clockwise from
/// the key's hash position and collect `rf` distinct physical nodes.
#[derive(Debug, Clone)]
pub struct ConsistentHashRing {
    /// Sorted ring of (position, node_id) pairs.
    ring: Vec<(u64, NodeId)>,
    /// Virtual nodes per physical node.
    vnodes_per_node: u32,
    /// Replication factor.
    rf: u8,
    /// Distinct physical nodes on the ring.
    physical_nodes: Vec<NodeId>,
}

impl ConsistentHashRing {
    /// Build a new hash ring.
    ///
    /// # Errors
    ///
    /// - Empty node list.
    /// - `rf` exceeds node count.
    pub fn new(nodes: Vec<NodeId>, rf: u8, vnodes_per_node: u32) -> Result<Self, ClusterError> {
        if nodes.is_empty() {
            return Err(ClusterError::Placement(
                "node list must not be empty".into(),
            ));
        }
        if (rf as usize) > nodes.len() {
            return Err(ClusterError::Placement(format!(
                "replication factor ({rf}) exceeds node count ({})",
                nodes.len()
            )));
        }

        let mut ring = Vec::with_capacity(nodes.len() * vnodes_per_node as usize);

        for node in &nodes {
            for vnode_idx in 0..vnodes_per_node {
                let position = Self::hash_vnode(*node, vnode_idx);
                ring.push((position, *node));
            }
        }

        ring.sort_by_key(|&(pos, _)| pos);

        Ok(Self {
            ring,
            vnodes_per_node,
            rf,
            physical_nodes: nodes,
        })
    }

    /// Select `rf` distinct physical replica nodes for the given key.
    ///
    /// Walks clockwise from the key's hash position, skipping duplicate
    /// physical nodes, until `rf` distinct nodes are collected.
    pub fn get_replicas(&self, key: &str) -> Vec<NodeId> {
        let hash = Self::hash_key(key);
        let start = match self.ring.binary_search_by_key(&hash, |&(pos, _)| pos) {
            Ok(i) => i,
            Err(i) => i % self.ring.len(),
        };

        let mut replicas = Vec::with_capacity(self.rf as usize);
        let ring_len = self.ring.len();

        for offset in 0..ring_len {
            let idx = (start + offset) % ring_len;
            let (_, node_id) = self.ring[idx];
            if !replicas.contains(&node_id) {
                replicas.push(node_id);
                if replicas.len() == self.rf as usize {
                    break;
                }
            }
        }

        replicas
    }

    /// Number of distinct physical nodes.
    pub fn node_count(&self) -> usize {
        self.physical_nodes.len()
    }

    /// Return all physical nodes on the ring.
    pub fn all_nodes(&self) -> &[NodeId] {
        &self.physical_nodes
    }

    /// Virtual nodes per physical node.
    pub fn vnodes_per_node(&self) -> u32 {
        self.vnodes_per_node
    }

    /// Total positions on the ring.
    pub fn ring_size(&self) -> usize {
        self.ring.len()
    }

    /// Hash a key to a 64-bit ring position using BLAKE3.
    pub fn hash_key(key: &str) -> u64 {
        let hash = blake3::hash(key.as_bytes());
        let bytes: [u8; 8] = hash.as_bytes()[..8].try_into().expect("8 bytes from 32");
        u64::from_be_bytes(bytes)
    }

    /// Hash a virtual node to a ring position.
    ///
    /// Input: `BLAKE3(node_id_bytes || vnode_index_bytes)`.
    fn hash_vnode(node: NodeId, vnode_idx: u32) -> u64 {
        let mut input = [0u8; 20]; // 16 (node) + 4 (vnode idx)
        input[..16].copy_from_slice(&node.to_bytes());
        input[16..20].copy_from_slice(&vnode_idx.to_be_bytes());
        let hash = blake3::hash(&input);
        let bytes: [u8; 8] = hash.as_bytes()[..8].try_into().expect("8 bytes from 32");
        u64::from_be_bytes(bytes)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_nodes(count: usize) -> Vec<NodeId> {
        // Deterministic node IDs for reproducible tests.
        (1..=count as u128).map(NodeId).collect()
    }

    // -- PlacementStrategy::new -------------------------------------------

    #[test]
    fn full_replication_when_pool_equals_rf() {
        let nodes = make_nodes(3);
        let strategy = PlacementStrategy::new(nodes.clone(), 3).unwrap();
        assert!(matches!(
            strategy,
            PlacementStrategy::FullReplication { .. }
        ));
        assert_eq!(strategy.replication_factor(), 3);
        assert_eq!(strategy.node_count(), 3);
    }

    #[test]
    fn hash_ring_when_pool_exceeds_rf() {
        let nodes = make_nodes(6);
        let strategy = PlacementStrategy::new(nodes, 3).unwrap();
        assert!(matches!(strategy, PlacementStrategy::HashRing(_)));
        assert_eq!(strategy.replication_factor(), 3);
        assert_eq!(strategy.node_count(), 6);
    }

    #[test]
    fn empty_nodes_rejected() {
        let err = PlacementStrategy::new(vec![], 3).unwrap_err();
        assert!(err.to_string().contains("empty"));
    }

    #[test]
    fn rf_zero_rejected() {
        let err = PlacementStrategy::new(make_nodes(3), 0).unwrap_err();
        assert!(err.to_string().contains("at least 1"));
    }

    #[test]
    fn rf_exceeds_nodes_rejected() {
        let err = PlacementStrategy::new(make_nodes(2), 3).unwrap_err();
        assert!(err.to_string().contains("exceeds"));
    }

    // -- FullReplication --------------------------------------------------

    #[test]
    fn full_replication_returns_all_nodes_sorted() {
        let nodes = vec![NodeId(3), NodeId(1), NodeId(2)];
        let strategy = PlacementStrategy::new(nodes, 3).unwrap();
        let replicas = strategy.get_replicas("any-key");
        assert_eq!(replicas, vec![NodeId(1), NodeId(2), NodeId(3)]);
    }

    #[test]
    fn full_replication_same_result_for_any_key() {
        let strategy = PlacementStrategy::new(make_nodes(3), 3).unwrap();
        let r1 = strategy.get_replicas("bucket/foo.txt");
        let r2 = strategy.get_replicas("bucket/bar.txt");
        assert_eq!(r1, r2);
    }

    // -- ConsistentHashRing -----------------------------------------------

    #[test]
    fn hash_ring_returns_rf_replicas() {
        let strategy = PlacementStrategy::new(make_nodes(6), 3).unwrap();
        let replicas = strategy.get_replicas("test-bucket/some-key.bin");
        assert_eq!(replicas.len(), 3);
    }

    #[test]
    fn hash_ring_replicas_are_distinct() {
        let strategy = PlacementStrategy::new(make_nodes(12), 3).unwrap();
        for key in ["a", "b", "c/d/e", "long-key-with-many-parts/x/y/z"] {
            let replicas = strategy.get_replicas(key);
            assert_eq!(replicas.len(), 3);
            let unique: std::collections::HashSet<_> = replicas.iter().collect();
            assert_eq!(unique.len(), 3, "duplicate replica for key '{key}'");
        }
    }

    #[test]
    fn hash_ring_deterministic() {
        let nodes = make_nodes(6);
        let s1 = PlacementStrategy::new(nodes.clone(), 3).unwrap();
        let s2 = PlacementStrategy::new(nodes, 3).unwrap();
        let key = "bucket/deterministic-test.dat";
        assert_eq!(s1.get_replicas(key), s2.get_replicas(key));
    }

    #[test]
    fn hash_ring_distribution_is_reasonable() {
        let node_count = 6;
        let rf = 3_u8;
        let strategy = PlacementStrategy::new(make_nodes(node_count), rf).unwrap();

        let mut hit_count = std::collections::HashMap::<NodeId, usize>::new();
        let num_keys = 10_000;

        for i in 0..num_keys {
            let key = format!("bucket/key-{i:06}");
            for node in strategy.get_replicas(&key) {
                *hit_count.entry(node).or_default() += 1;
            }
        }

        // With 10k keys, RF=3, 6 nodes: each node should get ~5000 hits.
        // Allow 30% deviation for statistical variance.
        let expected = (num_keys * rf as usize) / node_count;
        let tolerance = expected * 30 / 100;

        for (node, count) in &hit_count {
            assert!(
                count.abs_diff(expected) <= tolerance,
                "node {node} got {count} hits, expected ~{expected} (±{tolerance})"
            );
        }
    }

    #[test]
    fn single_node_full_replication() {
        let strategy = PlacementStrategy::new(vec![NodeId(42)], 1).unwrap();
        assert!(matches!(
            strategy,
            PlacementStrategy::FullReplication { .. }
        ));
        assert_eq!(strategy.get_replicas("anything"), vec![NodeId(42)]);
    }

    #[test]
    fn hash_ring_minimal_case() {
        // 2 nodes, RF=1: hash ring with minimal setup.
        let strategy = PlacementStrategy::new(make_nodes(2), 1).unwrap();
        assert!(matches!(strategy, PlacementStrategy::HashRing(_)));
        let replicas = strategy.get_replicas("key");
        assert_eq!(replicas.len(), 1);
    }

    // -- ConsistentHashRing internals -------------------------------------

    #[test]
    fn vnode_hashing_is_deterministic() {
        let a = ConsistentHashRing::hash_vnode(NodeId(1), 0);
        let b = ConsistentHashRing::hash_vnode(NodeId(1), 0);
        assert_eq!(a, b);
    }

    #[test]
    fn different_vnodes_produce_different_positions() {
        let a = ConsistentHashRing::hash_vnode(NodeId(1), 0);
        let b = ConsistentHashRing::hash_vnode(NodeId(1), 1);
        assert_ne!(a, b);
    }

    #[test]
    fn different_nodes_produce_different_positions() {
        let a = ConsistentHashRing::hash_vnode(NodeId(1), 0);
        let b = ConsistentHashRing::hash_vnode(NodeId(2), 0);
        assert_ne!(a, b);
    }
}
