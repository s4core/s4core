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

//! The tree two nodes compare their versions by.
//!
//! Anti-entropy compares a node with one peer at a time, by the keys both of
//! them replicate: in a pool wider than the replication factor the peer holds
//! only part of this node's keys, and the rest would look like divergence. The
//! tree is computed afresh for every round from the version digests the
//! storage keeps, in one pass in ring order; it is never stored.
//!
//! The ring is cut into [`LEAF_COUNT`] equal ranges by the top bits of a key's
//! ring position. A leaf is the XOR of `blake3(full key ‖ digest)` over the
//! keys of its range the pair replicates: the key is part of the hash, so two
//! keys holding the same version do not cancel each other out, and any change
//! of a key's HLC, content or kind changes its leaf.

use s4_core::storage::{VersionDigest, VersionDigestEntry};
use s4_core::StorageEngine;

use crate::error::ClusterError;
use crate::identity::NodeId;
use crate::placement::{PlacementStrategy, ReplicationClass, RingCover};

/// Bits of the ring position that pick a leaf.
pub const LEAF_BITS: u32 = 14;

/// Leaves of a pair's tree: 16 384, the leaves of a binary tree of depth 15.
pub const LEAF_COUNT: usize = 1 << LEAF_BITS;

/// Bytes of one leaf hash.
pub const LEAF_HASH_LEN: usize = 32;

/// Digests read from storage at a time while the leaves are computed.
const SCAN_PAGE: usize = 4096;

/// One leaf's hash.
pub type LeafHash = [u8; LEAF_HASH_LEN];

/// The leaf a ring position falls in.
pub fn leaf_of(ring_position: u64) -> u32 {
    // The top LEAF_BITS bits of a u64 always fit in a u32.
    (ring_position >> (64 - LEAF_BITS)) as u32
}

/// The ring positions of leaf `leaf`: from the first, up to the next leaf's
/// first (`None` past the end of the ring).
pub fn leaf_range(leaf: u32) -> (u64, Option<u64>) {
    let shift = 64 - LEAF_BITS;
    let from = u64::from(leaf) << shift;
    let to = u64::from(leaf).checked_add(1).filter(|next| *next < LEAF_COUNT as u64);
    (from, to.map(|next| next << shift))
}

/// The share of one key in its leaf.
pub fn entry_hash(full_key: &str, digest: &VersionDigest) -> LeafHash {
    let mut hasher = blake3::Hasher::new();
    hasher.update(full_key.as_bytes());
    // The digest is of a fixed length, so the key cannot run into it.
    hasher.update(&digest.encode());
    *hasher.finalize().as_bytes()
}

/// The keys two nodes of a pool compare: those whose replicas, by the pool's
/// placement of their class, include both.
///
/// While a pool moves its bucket metadata, the round's caller chooses how
/// they are compared — by the old layout, by the new one, or not at all —
/// and the peer compares them the same way, so the leaves of the two sides
/// hold the same keys whatever stage each side is at. The cluster's control
/// state is on every node of the pool, so any two of them compare all of it
/// (plan CLUSTER-IAM-REPLICATION.md, decision 1).
#[derive(Debug, Clone)]
pub struct PairScope {
    data: PairCover,
    /// `None`: the bucket metadata are left out.
    bucket_metadata: Option<PairCover>,
    /// Whether both nodes are of the pool, and so hold its control state.
    cluster_control: bool,
}

#[derive(Debug, Clone)]
enum PairCover {
    /// Every key, or none: every node replicates every key.
    Everything(bool),
    /// The arcs of the ring on which both are replicas.
    Arcs(RingCover),
}

impl PairCover {
    fn of(placement: &PlacementStrategy, a: NodeId, b: NodeId) -> Self {
        match placement {
            PlacementStrategy::FullReplication { nodes } => {
                Self::Everything(nodes.contains(&a) && nodes.contains(&b))
            }
            PlacementStrategy::HashRing(ring) => Self::Arcs(ring.cover_of(&[a, b])),
        }
    }

    fn covers(&self, ring_position: u64) -> bool {
        match self {
            Self::Everything(covered) => *covered,
            Self::Arcs(arcs) => arcs.covers(ring_position),
        }
    }
}

impl PairScope {
    /// The keys `a` and `b` both replicate under `placement`, every class
    /// placed alike. The arcs of the ring they share are found here, once.
    pub fn new(placement: PlacementStrategy, a: NodeId, b: NodeId) -> Self {
        let data = PairCover::of(&placement, a, b);
        Self {
            bucket_metadata: Some(data.clone()),
            data,
            cluster_control: both_of_pool(&placement, a, b),
        }
    }

    /// The keys `a` and `b` both replicate: the data under `data`, the
    /// bucket metadata under `bucket_metadata`, or none of them when it is
    /// `None`.
    pub fn by_class(
        data: &PlacementStrategy,
        bucket_metadata: Option<&PlacementStrategy>,
        a: NodeId,
        b: NodeId,
    ) -> Self {
        Self {
            data: PairCover::of(data, a, b),
            bucket_metadata: bucket_metadata.map(|placement| PairCover::of(placement, a, b)),
            cluster_control: both_of_pool(data, a, b),
        }
    }

    /// Whether the pair compares the key `full_key` at ring position
    /// `ring_position` — the key and position a version digest records.
    pub fn covers(&self, full_key: &str, ring_position: u64) -> bool {
        match ReplicationClass::of_full_key(full_key) {
            ReplicationClass::Data => self.data.covers(ring_position),
            ReplicationClass::BucketMetadata => {
                self.bucket_metadata.as_ref().is_some_and(|cover| cover.covers(ring_position))
            }
            ReplicationClass::ClusterControl => self.cluster_control,
        }
    }
}

/// Whether `a` and `b` are both nodes of the pool `placement` places the data
/// of: every node of a pool holds its control state.
fn both_of_pool(placement: &PlacementStrategy, a: NodeId, b: NodeId) -> bool {
    let nodes = placement.all_nodes();
    nodes.contains(&a) && nodes.contains(&b)
}

/// The leaves of the tree `scope` compares on this node's `storage`, in one
/// pass over its version digests.
pub async fn pair_leaves(
    storage: &dyn StorageEngine,
    scope: &PairScope,
) -> Result<Vec<LeafHash>, ClusterError> {
    let mut leaves = vec![[0u8; LEAF_HASH_LEN]; LEAF_COUNT];
    let mut after: Option<String> = None;
    loop {
        let page = storage
            .version_digests_in(0, None, after.as_deref(), SCAN_PAGE)
            .await
            .map_err(|e| ClusterError::Storage(e.to_string()))?;
        for entry in &page {
            if !scope.covers(&entry.key, entry.ring_position) {
                continue;
            }
            let leaf = &mut leaves[leaf_of(entry.ring_position) as usize];
            for (byte, share) in leaf.iter_mut().zip(entry_hash(&entry.key, &entry.digest)) {
                *byte ^= share;
            }
        }
        match page.last() {
            Some(last) if page.len() == SCAN_PAGE => after = Some(last.key.clone()),
            _ => return Ok(leaves),
        }
    }
}

/// Where a page of [`leaf_keys`] continues: in `leaf`, right after the key
/// `after`, or at its start.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeafCursor {
    /// The leaf.
    pub leaf: u32,
    /// The last key already returned in it.
    pub after: Option<String>,
}

/// A page of the keys `scope` compares in `leaves` (ascending), in ring order,
/// from `cursor` on, at most `limit` of them; and where the next page starts,
/// or `None` when the leaves are done.
pub async fn leaf_keys(
    storage: &dyn StorageEngine,
    scope: &PairScope,
    leaves: &[u32],
    cursor: Option<LeafCursor>,
    limit: usize,
) -> Result<(Vec<VersionDigestEntry>, Option<LeafCursor>), ClusterError> {
    let limit = limit.max(1);
    let start = cursor.as_ref().map_or(0, |cursor| {
        leaves.partition_point(|leaf| *leaf < cursor.leaf)
    });
    let mut after = cursor.and_then(|cursor| cursor.after);
    let mut keys = Vec::new();
    for &leaf in &leaves[start..] {
        let (from, to) = leaf_range(leaf);
        loop {
            let want = limit - keys.len();
            let page = storage
                .version_digests_in(from, to, after.as_deref(), want)
                .await
                .map_err(|e| ClusterError::Storage(e.to_string()))?;
            let full = page.len() == want;
            let last = page.last().map(|entry| entry.key.clone());
            keys.extend(
                page.into_iter().filter(|entry| scope.covers(&entry.key, entry.ring_position)),
            );
            if keys.len() == limit {
                return Ok((keys, Some(LeafCursor { leaf, after: last })));
            }
            if !full {
                break;
            }
            after = last;
        }
        after = None;
    }
    Ok((keys, None))
}

#[cfg(test)]
mod tests {
    use super::*;
    use s4_core::storage::{BitcaskStorageEngine, IndexOptions};
    use std::collections::HashMap;

    async fn storage() -> (BitcaskStorageEngine, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let storage = BitcaskStorageEngine::open(
            dir.path().join("volumes"),
            dir.path().join("metadata"),
            10 * 1024 * 1024,
            4096,
            false,
            IndexOptions {
                version_digests: true,
            },
        )
        .await
        .unwrap();
        (storage, dir)
    }

    async fn put(storage: &BitcaskStorageEngine, key: &str, data: &[u8], wall: u64) {
        let metadata = HashMap::from([
            ("_s4_hlc_wall".to_string(), wall.to_string()),
            ("_s4_hlc_logical".to_string(), "0".to_string()),
            ("_s4_hlc_node_id".to_string(), "7".to_string()),
        ]);
        storage.put_object("bucket", key, data, "text/plain", &metadata).await.unwrap();
    }

    fn full_replication() -> PlacementStrategy {
        PlacementStrategy::new(vec![NodeId(1), NodeId(2), NodeId(3)], 3).unwrap()
    }

    #[test]
    fn the_leaves_cut_the_ring_into_equal_ranges() {
        assert_eq!(leaf_range(0), (0, Some(1 << 50)));
        assert_eq!(
            leaf_range(LEAF_COUNT as u32 - 1),
            ((LEAF_COUNT as u64 - 1) << 50, None)
        );
        for position in [0, 1, (1 << 50) - 1, 1 << 50, u64::MAX] {
            let (from, to) = leaf_range(leaf_of(position));
            assert!(position >= from && to.is_none_or(|to| position < to));
        }
    }

    /// Two keys of one leaf holding the same version do not cancel out: the
    /// leaf of a node holding both differs from that of a node holding none.
    #[tokio::test]
    async fn equal_versions_of_two_keys_in_one_leaf_do_not_cancel_out() {
        let mut first_in_leaf = HashMap::new();
        let (a, b) = (0..)
            .map(|n| format!("k{n}"))
            .find_map(|key| {
                let leaf = leaf_of(s4_core::ring_position("bucket", &key));
                first_in_leaf.insert(leaf, key.clone()).map(|other| (other, key))
            })
            .unwrap();
        let (both, _dir) = storage().await;
        put(&both, &a, b"same", 10).await;
        put(&both, &b, b"same", 10).await;
        let (none, _dir2) = storage().await;

        let scope = PairScope::new(full_replication(), NodeId(1), NodeId(2));
        let leaf = leaf_of(s4_core::ring_position("bucket", &a)) as usize;
        let with_both = pair_leaves(&both, &scope).await.unwrap();
        let with_none = pair_leaves(&none, &scope).await.unwrap();
        assert_ne!(with_both[leaf], with_none[leaf]);
        assert_eq!(with_none[leaf], [0; LEAF_HASH_LEN]);
    }

    /// Any change of a key's version changes its leaf, and only its leaf.
    #[tokio::test]
    async fn a_new_version_changes_its_leaf_alone() {
        let (storage, _dir) = storage().await;
        for n in 0..50 {
            put(&storage, &format!("k{n}"), b"v", 10).await;
        }
        let scope = PairScope::new(full_replication(), NodeId(1), NodeId(2));
        let before = pair_leaves(&storage, &scope).await.unwrap();
        put(&storage, "k7", b"v", 11).await;
        let after = pair_leaves(&storage, &scope).await.unwrap();
        let changed: Vec<usize> =
            (0..LEAF_COUNT).filter(|leaf| before[*leaf] != after[*leaf]).collect();
        assert_eq!(
            changed,
            vec![leaf_of(s4_core::ring_position("bucket", "k7")) as usize]
        );
    }

    /// In a pool of five nodes with RF=3, a key node 1 holds and its peer
    /// does not replicate is outside their tree: the two nodes' leaves agree
    /// though only node 1 holds it.
    #[tokio::test]
    async fn keys_the_peer_does_not_replicate_are_no_divergence() {
        let nodes: Vec<NodeId> = (1..=5).map(NodeId).collect();
        let placement = PlacementStrategy::new(nodes, 3).unwrap();
        let PlacementStrategy::HashRing(ring) = &placement else {
            panic!("five nodes at RF=3 place by the ring");
        };
        let (node_1, _dir1) = storage().await;
        let (node_2, _dir2) = storage().await;
        let mut outside = 0;
        let mut shared = 0;
        for n in 0..200 {
            let key = format!("k{n}");
            let replicas = ring.get_replicas(&format!("bucket/{key}"));
            match (replicas.contains(&NodeId(1)), replicas.contains(&NodeId(2))) {
                (true, true) => {
                    put(&node_1, &key, b"v", 10).await;
                    put(&node_2, &key, b"v", 10).await;
                    shared += 1;
                }
                (true, false) => {
                    put(&node_1, &key, b"v", 10).await;
                    outside += 1;
                }
                (false, true) => {
                    put(&node_2, &key, b"v", 10).await;
                    outside += 1;
                }
                (false, false) => {}
            }
        }
        assert!(
            outside > 0 && shared > 0,
            "{outside} outside, {shared} shared"
        );

        let scope = PairScope::new(placement, NodeId(1), NodeId(2));
        assert_eq!(
            pair_leaves(&node_1, &scope).await.unwrap(),
            pair_leaves(&node_2, &scope).await.unwrap()
        );
        // Without the scope the same nodes diverge.
        let everything = PairScope::new(full_replication(), NodeId(1), NodeId(2));
        assert_ne!(
            pair_leaves(&node_1, &everything).await.unwrap(),
            pair_leaves(&node_2, &everything).await.unwrap()
        );
    }

    /// The cluster's control state is on every node of the pool, so every
    /// pair of them compares all of it, whatever the ring gives the pair of
    /// the data; a node outside the pool compares none of it, and the bucket
    /// metadata left out of a round stay out.
    #[test]
    fn every_pair_of_the_pool_compares_the_control_state() {
        let nodes: Vec<NodeId> = (1..=6).map(NodeId).collect();
        let placement = PlacementStrategy::new(nodes, 3).unwrap();
        let control = [
            "__system__/__s4_iam_v2_user_alice",
            "__system__/__s4_iam_v2_root_state",
        ];
        for a in 1..=6u128 {
            for b in (a + 1)..=6 {
                let scope = PairScope::by_class(&placement, None, NodeId(a), NodeId(b));
                for key in control {
                    let position = s4_core::ring_position_of(key);
                    assert!(scope.covers(key, position), "{a}-{b} {key}");
                }
                let marker = "__system__/__s4_bucket_marker_photos";
                assert!(!scope.covers(marker, s4_core::ring_position_of(marker)));
            }
        }
        let outside = PairScope::new(placement, NodeId(1), NodeId(7));
        for key in control {
            assert!(!outside.covers(key, s4_core::ring_position_of(key)));
        }
        // The IAM records of an earlier release are no control state: they
        // have no digest, and placement treats them as data.
        let legacy = "__system__/__s4_iam_user_alice";
        assert_eq!(
            ReplicationClass::of_full_key(legacy),
            ReplicationClass::Data
        );
    }

    /// Pages of a set of leaves give each of their keys once, in ring order,
    /// and nothing of the other leaves.
    #[tokio::test]
    async fn pages_of_leaves_give_their_keys_once() {
        let (storage, _dir) = storage().await;
        for n in 0..400 {
            put(&storage, &format!("k{n}"), b"v", 10).await;
        }
        let all = storage.index_db().version_digests().await.unwrap();
        let mut leaves: Vec<u32> = all.iter().map(|entry| leaf_of(entry.ring_position)).collect();
        leaves.dedup();
        let leaves: Vec<u32> = leaves.into_iter().step_by(3).collect();
        let wanted: Vec<String> = all
            .iter()
            .filter(|entry| leaves.contains(&leaf_of(entry.ring_position)))
            .map(|entry| entry.key.clone())
            .collect();

        let scope = PairScope::new(full_replication(), NodeId(1), NodeId(2));
        let mut paged = Vec::new();
        let mut cursor = None;
        loop {
            let (page, next) = leaf_keys(&storage, &scope, &leaves, cursor, 7).await.unwrap();
            assert!(page.len() <= 7);
            paged.extend(page.into_iter().map(|entry| entry.key));
            match next {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }
        assert_eq!(paged, wanted);
    }
}
