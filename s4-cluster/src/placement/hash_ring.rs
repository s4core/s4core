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
pub(crate) const DEFAULT_VNODES_PER_NODE: u32 = 128;

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

    /// The replicas of a key at ring position `position`
    /// ([`s4_core::ring_position_of`] of the key): what
    /// [`get_replicas`](Self::get_replicas) answers for it, without hashing
    /// the key again — for callers that hold its position already, such as a
    /// version digest.
    pub fn replicas_at(&self, position: u64) -> Vec<NodeId> {
        match self {
            Self::FullReplication { nodes } => nodes.clone(),
            Self::HashRing(ring) => ring.replicas_at(position),
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

/// The index of the first entry of `ring` (sorted by position) at or after
/// `position`, wrapping to the first past the last.
fn start_index_in<T>(ring: &[T], position: u64, position_of: impl Fn(&T) -> u64) -> usize {
    match ring.binary_search_by_key(&position, position_of) {
        Ok(i) => i,
        Err(i) => i % ring.len(),
    }
}

/// The arcs of a ring on which a set of nodes are all replicas
/// ([`ConsistentHashRing::cover_of`]).
#[derive(Debug, Clone)]
pub struct RingCover {
    /// The ring's virtual node positions, ascending.
    positions: Vec<u64>,
    /// Whether the walk from each virtual node takes every node of the set.
    covered: Vec<bool>,
}

impl RingCover {
    /// Whether every node of the set is a replica of a key at `position`.
    pub fn covers(&self, position: u64) -> bool {
        if self.positions.is_empty() {
            return false;
        }
        self.covered[start_index_in(&self.positions, position, |pos| *pos)]
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

    /// Sorted positions of virtual nodes, including coincident positions.
    pub fn vnode_positions(&self) -> Vec<u64> {
        self.ring.iter().map(|(position, _)| *position).collect()
    }

    /// Select `rf` distinct physical replica nodes for the given key.
    ///
    /// Walks clockwise from the key's hash position, skipping duplicate
    /// physical nodes, until `rf` distinct nodes are collected.
    pub fn get_replicas(&self, key: &str) -> Vec<NodeId> {
        self.replicas_at(s4_core::ring_position_of(key))
    }

    /// The replicas of a key at ring position `position`.
    pub fn replicas_at(&self, position: u64) -> Vec<NodeId> {
        match self.start_index(position) {
            Some(start) => self.replicas_from(start),
            None => Vec::new(),
        }
    }

    /// The arcs of the ring on which every node of `nodes` is a replica.
    ///
    /// A key's replicas depend only on the first virtual node at or after its
    /// position, so they are the same along each arc between two virtual
    /// nodes. Computed once, the cover answers for any position with a binary
    /// search, without hashing the key or collecting its replicas.
    pub fn cover_of(&self, nodes: &[NodeId]) -> RingCover {
        RingCover {
            positions: self.ring.iter().map(|(position, _)| *position).collect(),
            covered: (0..self.ring.len())
                .map(|start| {
                    let replicas = self.replicas_from(start);
                    nodes.iter().all(|node| replicas.contains(node))
                })
                .collect(),
        }
    }

    /// Where the walk for a key at `position` starts; `None` on an empty ring.
    fn start_index(&self, position: u64) -> Option<usize> {
        if self.ring.is_empty() {
            return None;
        }
        Some(start_index_in(&self.ring, position, |&(pos, _)| pos))
    }

    /// The `rf` distinct physical nodes met walking clockwise from virtual
    /// node `start`.
    fn replicas_from(&self, start: usize) -> Vec<NodeId> {
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

    /// The cover of a pair answers as the replicas of each key do, on rings of
    /// several sizes and replication factors, for every pair.
    #[test]
    fn a_ring_cover_agrees_with_the_replicas_of_every_key() {
        for (nodes, rf) in [(3u128, 3u8), (5, 2), (7, 3), (12, 3)] {
            let ids: Vec<NodeId> = (1..=nodes).map(NodeId).collect();
            let ring = ConsistentHashRing::new(ids.clone(), rf, 64).unwrap();
            for a in &ids {
                for b in ids.iter().filter(|b| *b != a) {
                    let cover = ring.cover_of(&[*a, *b]);
                    for n in 0..2_000 {
                        let key = format!("bucket/key-{n}");
                        let replicas = ring.get_replicas(&key);
                        assert_eq!(
                            cover.covers(s4_core::ring_position_of(&key)),
                            replicas.contains(a) && replicas.contains(b),
                            "{nodes} nodes, rf {rf}, pair {a}/{b}, {key}"
                        );
                    }
                }
            }
        }
    }

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

    /// Replica sets computed before the ring position moved to `s4-core`:
    /// placement of existing keys must not shift.
    #[test]
    fn hash_ring_places_keys_where_it_always_did() {
        let strategy = PlacementStrategy::new(make_nodes(6), 3).unwrap();
        let known: [(&str, [u128; 3]); 6] = [
            ("photos/cat.jpg", [1, 4, 2]),
            ("bucket/a", [6, 4, 1]),
            ("__system__/__s4_bucket_marker_photos", [5, 6, 2]),
            ("b/ключ с пробелом", [4, 6, 2]),
            ("bucket/x#v1", [1, 4, 6]),
            ("a/", [3, 6, 1]),
        ];
        for (key, replicas) in known {
            let expected: Vec<NodeId> = replicas.into_iter().map(NodeId).collect();
            assert_eq!(strategy.get_replicas(key), expected, "{key}");
        }
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

    /// More replicas of a key on the same nodes only add to the replicas it
    /// had (plan BUCKET-METADATA-REPLICATION.md, decision 3): on one ring, the
    /// replicas at RF 5 start with the replicas at RF 3, in order; when the
    /// larger factor is the whole pool, they hold every replica at RF 3. A
    /// change of the replication factor of a class therefore only adds
    /// replicas to each key and moves none.
    #[test]
    fn more_replicas_on_the_same_nodes_keep_the_replicas_a_key_had() {
        for (nodes, smaller, larger) in [(6u128, 3u8, 5u8), (9, 3, 5), (12, 3, 5), (7, 3, 4)] {
            let ids: Vec<NodeId> = (1..=nodes).map(NodeId).collect();
            let small = PlacementStrategy::new(ids.clone(), smaller).unwrap();
            let large = PlacementStrategy::new(ids, larger).unwrap();
            for i in 0..10_000 {
                let key = format!("__system__/__s4_bucket_marker_bucket-{i}");
                let before = small.get_replicas(&key);
                let after = large.get_replicas(&key);
                assert_eq!(after.len(), usize::from(larger), "{key}");
                assert_eq!(&after[..before.len()], &before[..], "{nodes} nodes, {key}");
            }
        }
        for (nodes, smaller) in [(5u128, 3u8), (6, 3), (4, 3)] {
            let ids: Vec<NodeId> = (1..=nodes).map(NodeId).collect();
            let small = PlacementStrategy::new(ids.clone(), smaller).unwrap();
            let all = PlacementStrategy::new(ids, nodes as u8).unwrap();
            assert!(matches!(all, PlacementStrategy::FullReplication { .. }));
            for i in 0..10_000 {
                let key = format!("__system__/__s4_bucket_marker_bucket-{i}");
                let after = all.get_replicas(&key);
                for node in small.get_replicas(&key) {
                    assert!(
                        after.contains(&node),
                        "{nodes} nodes, {key}: {node} dropped"
                    );
                }
            }
        }
    }

    /// The replicas at a key's ring position are the key's replicas, on a
    /// ring and under full replication alike.
    #[test]
    fn replicas_at_the_position_of_a_key_are_its_replicas() {
        let nodes: Vec<NodeId> = (1..=5).map(NodeId).collect();
        for strategy in [
            PlacementStrategy::new(nodes.clone(), 3).unwrap(),
            PlacementStrategy::new(nodes[..3].to_vec(), 3).unwrap(),
        ] {
            for i in 0..10_000 {
                let key = format!("bucket/key-{i}");
                assert_eq!(
                    strategy.replicas_at(s4_core::ring_position_of(&key)),
                    strategy.get_replicas(&key),
                    "{key}"
                );
            }
        }
    }
}
