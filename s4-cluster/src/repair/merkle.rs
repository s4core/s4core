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

//! Persistent Merkle tree for anti-entropy divergence detection.
//!
//! The tree is built over key ranges on the hash ring. Each leaf node
//! covers a contiguous slice of the key space and stores the BLAKE3 hash
//! of all records in that range. Divergence between replicas is detected
//! in O(log K) comparisons by walking the tree top-down.
//!
//! The tree is stored as a flat array (heap-style indexing) where node `i`
//! has children at `2i+1` and `2i+2`. This avoids pointer-chasing and
//! makes serialization trivial.
//!
//! # Persistence
//!
//! Tree nodes are persisted to a dedicated storage keyspace so that
//! restarts do not require a full rescan of the data. Incremental updates
//! are applied on every write/delete and propagated up to the root.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use tracing::trace;

/// Default tree depth (15 levels = 32768 leaf buckets).
pub const DEFAULT_MERKLE_DEPTH: u8 = 15;

/// Hash size in bytes (BLAKE3 output).
const HASH_SIZE: usize = 32;

/// Empty hash sentinel (all zeros).
const EMPTY_HASH: [u8; HASH_SIZE] = [0u8; HASH_SIZE];

/// A key range represented as `[start, end)` on a 64-bit hash ring.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct KeyRange {
    /// Inclusive start of the range.
    pub start: u64,
    /// Exclusive end of the range.
    pub end: u64,
}

impl KeyRange {
    /// Create a new key range.
    pub fn new(start: u64, end: u64) -> Self {
        Self { start, end }
    }

    /// Full ring range covering `[0, u64::MAX)`.
    pub fn full() -> Self {
        Self {
            start: 0,
            end: u64::MAX,
        }
    }

    /// Check if a hash position falls within this range.
    pub fn contains(&self, hash: u64) -> bool {
        if self.start <= self.end {
            hash >= self.start && hash < self.end
        } else {
            // Wrap-around range on the ring.
            hash >= self.start || hash < self.end
        }
    }

    /// Split this range into two halves.
    fn split(&self) -> (KeyRange, KeyRange) {
        let mid = self.start.wrapping_add(self.end.wrapping_sub(self.start) / 2);
        (KeyRange::new(self.start, mid), KeyRange::new(mid, self.end))
    }
}

/// Persistent Merkle tree for detecting divergences between replicas.
///
/// The tree is stored as a flat array with heap-style indexing. Level 0
/// is the root; the leaves are at level `depth - 1`. Each node stores a
/// BLAKE3 hash that summarizes all records in its key range.
///
/// # Invariants
///
/// - `nodes.len() == 2^depth - 1`
/// - `nodes[0]` is the root hash
/// - Parent hash = `BLAKE3(left_child || right_child)`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MerkleTree {
    /// Flat array of node hashes (heap-style indexing).
    nodes: Vec<[u8; HASH_SIZE]>,
    /// Tree depth (number of levels, including root).
    depth: u8,
    /// Key range covered by this tree.
    key_range: KeyRange,
}

impl MerkleTree {
    /// Build a new Merkle tree from a set of key-hash pairs.
    ///
    /// Each entry is `(hash_ring_position, record_hash)` where
    /// `hash_ring_position` is the BLAKE3 hash of the object key mapped
    /// to a u64 ring position, and `record_hash` is the BLAKE3 hash of
    /// the record content (key + data + HLC).
    pub fn build(entries: &[(u64, [u8; HASH_SIZE])], key_range: KeyRange, depth: u8) -> Self {
        let num_nodes = (1usize << depth) - 1;
        let num_leaves = 1usize << (depth - 1);
        let mut nodes = vec![EMPTY_HASH; num_nodes];

        // Bucket entries into leaves.
        let leaf_start = num_leaves - 1;
        let range_size = key_range.end.wrapping_sub(key_range.start);

        for &(pos, hash) in entries {
            if !key_range.contains(pos) {
                continue;
            }
            let offset = pos.wrapping_sub(key_range.start);
            let bucket = if range_size == 0 {
                0
            } else {
                // Map position to leaf bucket index.
                ((offset as u128 * num_leaves as u128) / range_size as u128) as usize
            };
            let leaf_idx = leaf_start + bucket.min(num_leaves - 1);
            nodes[leaf_idx] = combine_hashes(&nodes[leaf_idx], &hash);
        }

        // Build up from leaves to root.
        for i in (0..leaf_start).rev() {
            let left = 2 * i + 1;
            let right = 2 * i + 2;
            nodes[i] = hash_children(&nodes[left], &nodes[right]);
        }

        Self {
            nodes,
            depth,
            key_range,
        }
    }

    /// Build an empty tree with properly computed internal nodes.
    ///
    /// Leaves are initialized to `EMPTY_HASH` (all zeros). Internal nodes
    /// are derived from their children via `hash_children` so that
    /// incremental inserts via [`update`](Self::update) produce the same
    /// result as a full [`build`](Self::build).
    pub fn empty(key_range: KeyRange, depth: u8) -> Self {
        let num_nodes = (1usize << depth) - 1;
        let num_leaves = 1usize << (depth - 1);
        let leaf_start = num_leaves - 1;
        let mut nodes = vec![EMPTY_HASH; num_nodes];

        // Build internal nodes bottom-up from empty leaves.
        for i in (0..leaf_start).rev() {
            let left = 2 * i + 1;
            let right = 2 * i + 2;
            nodes[i] = hash_children(&nodes[left], &nodes[right]);
        }

        Self {
            nodes,
            depth,
            key_range,
        }
    }

    /// Root hash of the tree.
    pub fn root_hash(&self) -> [u8; HASH_SIZE] {
        self.nodes[0]
    }

    /// Tree depth.
    pub fn depth(&self) -> u8 {
        self.depth
    }

    /// Key range covered by this tree.
    pub fn key_range(&self) -> KeyRange {
        self.key_range
    }

    /// Total number of nodes in the tree.
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Number of leaf buckets.
    pub fn leaf_count(&self) -> usize {
        1usize << (self.depth - 1)
    }

    /// Get the hash at a specific tree node index.
    pub fn hash_at(&self, index: usize) -> Option<[u8; HASH_SIZE]> {
        self.nodes.get(index).copied()
    }

    /// Update a single key's contribution to the tree.
    ///
    /// Recomputes the leaf bucket hash and propagates changes up to the
    /// root. The `old_hash` is XOR-removed and `new_hash` is XOR-added
    /// to support incremental updates without rescanning the leaf.
    pub fn update(
        &mut self,
        ring_position: u64,
        old_hash: [u8; HASH_SIZE],
        new_hash: [u8; HASH_SIZE],
    ) {
        let num_leaves = self.leaf_count();
        let leaf_start = num_leaves - 1;
        let range_size = self.key_range.end.wrapping_sub(self.key_range.start);

        let offset = ring_position.wrapping_sub(self.key_range.start);
        let bucket = if range_size == 0 {
            0
        } else {
            ((offset as u128 * num_leaves as u128) / range_size as u128) as usize
        };
        let leaf_idx = leaf_start + bucket.min(num_leaves - 1);

        // XOR-based incremental update: remove old contribution, add new.
        let leaf = &mut self.nodes[leaf_idx];
        xor_hash(leaf, &old_hash);
        xor_hash(leaf, &new_hash);

        // Propagate up to root.
        self.propagate_up(leaf_idx);

        trace!(ring_position, leaf_idx, "merkle tree updated");
    }

    /// Insert a new key (no previous hash to remove).
    pub fn insert(&mut self, ring_position: u64, record_hash: [u8; HASH_SIZE]) {
        self.update(ring_position, EMPTY_HASH, record_hash);
    }

    /// Remove a key (set its contribution to empty).
    pub fn remove(&mut self, ring_position: u64, record_hash: [u8; HASH_SIZE]) {
        self.update(ring_position, record_hash, EMPTY_HASH);
    }

    /// Find divergent key ranges by comparing this tree against a remote
    /// tree's node hashes, provided level by level.
    ///
    /// Returns a list of leaf-level `KeyRange`s that differ. The caller
    /// should fetch the actual keys in those ranges for reconciliation.
    pub fn find_divergent_ranges(
        &self,
        remote_hashes: &HashMap<usize, [u8; HASH_SIZE]>,
    ) -> Vec<KeyRange> {
        let mut divergent = Vec::new();
        self.diff_recurse(0, self.key_range, remote_hashes, &mut divergent);
        divergent
    }

    /// Compare this tree with another local tree and return divergent leaf ranges.
    pub fn diff(&self, other: &MerkleTree) -> Vec<KeyRange> {
        let remote: HashMap<usize, [u8; HASH_SIZE]> =
            other.nodes.iter().enumerate().map(|(i, h)| (i, *h)).collect();
        self.find_divergent_ranges(&remote)
    }

    /// Serialize the tree to bytes for persistence.
    pub fn to_bytes(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap_or_default()
    }

    /// Deserialize a tree from bytes.
    pub fn from_bytes(data: &[u8]) -> Result<Self, crate::error::ClusterError> {
        bincode::deserialize(data)
            .map_err(|e| crate::error::ClusterError::Codec(format!("merkle tree decode: {e}")))
    }

    /// Get hashes at a specific tree level for remote comparison.
    ///
    /// Level 0 = root, level `depth-1` = leaves.
    /// Returns `(node_index, hash)` pairs for all nodes at that level.
    pub fn hashes_at_level(&self, level: u8) -> Vec<(usize, [u8; HASH_SIZE])> {
        if level >= self.depth {
            return Vec::new();
        }
        let start = (1usize << level) - 1;
        let count = 1usize << level;
        (start..start + count)
            .filter_map(|i| self.nodes.get(i).map(|h| (i, *h)))
            .collect()
    }

    // -- Internal helpers --

    fn diff_recurse(
        &self,
        idx: usize,
        range: KeyRange,
        remote: &HashMap<usize, [u8; HASH_SIZE]>,
        divergent: &mut Vec<KeyRange>,
    ) {
        let local_hash = match self.nodes.get(idx) {
            Some(h) => h,
            None => return,
        };

        let remote_hash = match remote.get(&idx) {
            Some(h) => h,
            None => {
                // Remote doesn't have this node — treat entire range as divergent.
                divergent.push(range);
                return;
            }
        };

        if local_hash == remote_hash {
            return;
        }

        let left = 2 * idx + 1;
        let right = 2 * idx + 2;

        // If we're at a leaf or beyond the tree, report as divergent.
        if left >= self.nodes.len() {
            divergent.push(range);
            return;
        }

        let (left_range, right_range) = range.split();
        self.diff_recurse(left, left_range, remote, divergent);
        self.diff_recurse(right, right_range, remote, divergent);
    }

    fn propagate_up(&mut self, mut idx: usize) {
        while idx > 0 {
            let parent = (idx - 1) / 2;
            let left = 2 * parent + 1;
            let right = 2 * parent + 2;
            self.nodes[parent] = hash_children(&self.nodes[left], &self.nodes[right]);
            idx = parent;
        }
    }
}

/// Combine two hashes via XOR (commutative, supports incremental updates).
fn combine_hashes(a: &[u8; HASH_SIZE], b: &[u8; HASH_SIZE]) -> [u8; HASH_SIZE] {
    let mut result = *a;
    xor_hash(&mut result, b);
    result
}

/// XOR `other` into `target` in-place.
fn xor_hash(target: &mut [u8; HASH_SIZE], other: &[u8; HASH_SIZE]) {
    for (t, o) in target.iter_mut().zip(other.iter()) {
        *t ^= *o;
    }
}

/// Compute parent hash from two children: `BLAKE3(left || right)`.
fn hash_children(left: &[u8; HASH_SIZE], right: &[u8; HASH_SIZE]) -> [u8; HASH_SIZE] {
    let mut input = [0u8; HASH_SIZE * 2];
    input[..HASH_SIZE].copy_from_slice(left);
    input[HASH_SIZE..].copy_from_slice(right);
    *blake3::hash(&input).as_bytes()
}

/// Compute a record hash from key, data, and HLC timestamp.
///
/// Used by the anti-entropy system to produce a deterministic fingerprint
/// of a stored record for Merkle tree insertion.
pub fn record_hash(key: &str, data: &[u8], hlc_wall: u64, hlc_logical: u32) -> [u8; HASH_SIZE] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(key.as_bytes());
    hasher.update(data);
    hasher.update(&hlc_wall.to_be_bytes());
    hasher.update(&hlc_logical.to_be_bytes());
    *hasher.finalize().as_bytes()
}

/// Compute a ring position for a given bucket/key combination.
pub fn ring_position(bucket: &str, key: &str) -> u64 {
    let combined = format!("{bucket}/{key}");
    let hash = blake3::hash(combined.as_bytes());
    let bytes: [u8; 8] = hash.as_bytes()[..8].try_into().expect("8 bytes from 32");
    u64::from_be_bytes(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entry(pos: u64, data: &[u8]) -> (u64, [u8; HASH_SIZE]) {
        (pos, *blake3::hash(data).as_bytes())
    }

    #[test]
    fn empty_tree_properties() {
        let tree = MerkleTree::empty(KeyRange::full(), 4);
        // Root is not zero — it's hash_children propagated from empty leaves.
        assert_ne!(tree.root_hash(), EMPTY_HASH);
        assert_eq!(tree.depth(), 4);
        assert_eq!(tree.leaf_count(), 8);
        assert_eq!(tree.node_count(), 15);
    }

    #[test]
    fn empty_tree_root_is_deterministic() {
        let t1 = MerkleTree::empty(KeyRange::full(), 4);
        let t2 = MerkleTree::empty(KeyRange::full(), 4);
        assert_eq!(t1.root_hash(), t2.root_hash());
    }

    #[test]
    fn build_single_entry() {
        let entries = vec![make_entry(100, b"hello")];
        let tree = MerkleTree::build(&entries, KeyRange::full(), 4);
        assert_ne!(tree.root_hash(), EMPTY_HASH);
    }

    #[test]
    fn identical_trees_have_same_root() {
        let entries = vec![
            make_entry(100, b"a"),
            make_entry(200, b"b"),
            make_entry(300, b"c"),
        ];
        let t1 = MerkleTree::build(&entries, KeyRange::full(), 4);
        let t2 = MerkleTree::build(&entries, KeyRange::full(), 4);
        assert_eq!(t1.root_hash(), t2.root_hash());
    }

    #[test]
    fn different_data_produces_different_root() {
        let e1 = vec![make_entry(100, b"a")];
        let e2 = vec![make_entry(100, b"b")];
        let t1 = MerkleTree::build(&e1, KeyRange::full(), 4);
        let t2 = MerkleTree::build(&e2, KeyRange::full(), 4);
        assert_ne!(t1.root_hash(), t2.root_hash());
    }

    #[test]
    fn diff_identical_trees_returns_empty() {
        let entries = vec![make_entry(100, b"x"), make_entry(500, b"y")];
        let t1 = MerkleTree::build(&entries, KeyRange::full(), 4);
        let t2 = MerkleTree::build(&entries, KeyRange::full(), 4);
        assert!(t1.diff(&t2).is_empty());
    }

    #[test]
    fn diff_detects_divergence() {
        let e1 = vec![make_entry(100, b"a"), make_entry(500, b"same")];
        let e2 = vec![make_entry(100, b"b"), make_entry(500, b"same")];
        let t1 = MerkleTree::build(&e1, KeyRange::full(), 4);
        let t2 = MerkleTree::build(&e2, KeyRange::full(), 4);
        let divergent = t1.diff(&t2);
        assert!(!divergent.is_empty());
        // The divergent range should contain position 100.
        assert!(divergent.iter().any(|r| r.contains(100)));
    }

    #[test]
    fn diff_does_not_report_matching_ranges() {
        let e1 = vec![
            make_entry(100, b"a"),
            make_entry(u64::MAX / 2 + 1000, b"same"),
        ];
        let e2 = vec![
            make_entry(100, b"b"),
            make_entry(u64::MAX / 2 + 1000, b"same"),
        ];
        let t1 = MerkleTree::build(&e1, KeyRange::full(), 4);
        let t2 = MerkleTree::build(&e2, KeyRange::full(), 4);
        let divergent = t1.diff(&t2);
        // Position at MAX/2+1000 should NOT be in divergent ranges since data matches.
        let high_pos = u64::MAX / 2 + 1000;
        // The ranges containing high_pos should not be reported.
        let high_divergent = divergent.iter().any(|r| r.contains(high_pos));
        assert!(
            !high_divergent,
            "matching range incorrectly reported as divergent"
        );
    }

    #[test]
    fn incremental_insert_matches_full_build() {
        let entries = vec![make_entry(100, b"a"), make_entry(500, b"b")];
        let full = MerkleTree::build(&entries, KeyRange::full(), 4);

        let mut incremental = MerkleTree::empty(KeyRange::full(), 4);
        incremental.insert(100, *blake3::hash(b"a").as_bytes());
        incremental.insert(500, *blake3::hash(b"b").as_bytes());

        assert_eq!(full.root_hash(), incremental.root_hash());
    }

    #[test]
    fn incremental_remove_restores_previous_state() {
        let hash_a = *blake3::hash(b"a").as_bytes();
        let hash_b = *blake3::hash(b"b").as_bytes();

        let mut tree = MerkleTree::empty(KeyRange::full(), 4);
        tree.insert(100, hash_a);
        let root_with_a = tree.root_hash();

        tree.insert(500, hash_b);
        assert_ne!(tree.root_hash(), root_with_a);

        tree.remove(500, hash_b);
        assert_eq!(tree.root_hash(), root_with_a);
    }

    #[test]
    fn serialization_roundtrip() {
        let entries = vec![make_entry(100, b"x"), make_entry(200, b"y")];
        let tree = MerkleTree::build(&entries, KeyRange::full(), 4);
        let bytes = tree.to_bytes();
        let restored = MerkleTree::from_bytes(&bytes).unwrap();
        assert_eq!(tree.root_hash(), restored.root_hash());
        assert_eq!(tree.depth(), restored.depth());
        assert_eq!(tree.key_range(), restored.key_range());
    }

    #[test]
    fn hashes_at_level() {
        let tree = MerkleTree::empty(KeyRange::full(), 4);
        let level0 = tree.hashes_at_level(0);
        assert_eq!(level0.len(), 1); // root
        let level1 = tree.hashes_at_level(1);
        assert_eq!(level1.len(), 2);
        let level3 = tree.hashes_at_level(3);
        assert_eq!(level3.len(), 8); // leaves for depth=4
    }

    #[test]
    fn key_range_contains() {
        let range = KeyRange::new(100, 200);
        assert!(range.contains(100));
        assert!(range.contains(150));
        assert!(!range.contains(200));
        assert!(!range.contains(99));
    }

    #[test]
    fn key_range_split() {
        let range = KeyRange::new(0, 1000);
        let (left, right) = range.split();
        assert_eq!(left, KeyRange::new(0, 500));
        assert_eq!(right, KeyRange::new(500, 1000));
    }

    #[test]
    fn record_hash_deterministic() {
        let h1 = record_hash("key", b"data", 1000, 1);
        let h2 = record_hash("key", b"data", 1000, 1);
        assert_eq!(h1, h2);
    }

    #[test]
    fn record_hash_differs_for_different_input() {
        let h1 = record_hash("key1", b"data", 1000, 1);
        let h2 = record_hash("key2", b"data", 1000, 1);
        assert_ne!(h1, h2);
    }

    #[test]
    fn ring_position_deterministic() {
        let p1 = ring_position("bucket", "key");
        let p2 = ring_position("bucket", "key");
        assert_eq!(p1, p2);
    }

    #[test]
    fn ring_position_varies_by_key() {
        let p1 = ring_position("bucket", "key1");
        let p2 = ring_position("bucket", "key2");
        assert_ne!(p1, p2);
    }

    #[test]
    fn build_with_many_entries_performance() {
        let entries: Vec<_> = (0..10_000u64)
            .map(|i| make_entry(i * (u64::MAX / 10_000), &i.to_be_bytes()))
            .collect();

        let start = std::time::Instant::now();
        let tree = MerkleTree::build(&entries, KeyRange::full(), DEFAULT_MERKLE_DEPTH);
        let elapsed = start.elapsed();

        assert_ne!(tree.root_hash(), EMPTY_HASH);
        // Should complete well under 5 seconds even on slow CI.
        assert!(
            elapsed.as_secs() < 5,
            "build took {elapsed:?} for 10K entries"
        );
    }

    #[test]
    fn diff_olog_k_comparisons() {
        // With depth 4, divergence at one leaf should touch at most
        // `depth` nodes (root -> leaf path = 4 nodes).
        let e1 = vec![make_entry(100, b"a")];
        let e2 = vec![make_entry(100, b"b")];
        let t1 = MerkleTree::build(&e1, KeyRange::full(), 4);
        let t2 = MerkleTree::build(&e2, KeyRange::full(), 4);
        let divergent = t1.diff(&t2);
        // Exactly one leaf range should be divergent.
        assert_eq!(divergent.len(), 1);
    }

    #[test]
    fn update_changes_only_affected_path() {
        let entries = vec![make_entry(100, b"a"), make_entry(u64::MAX - 100, b"b")];
        let tree = MerkleTree::build(&entries, KeyRange::full(), 4);

        let mut updated = tree.clone();
        let old_hash = *blake3::hash(b"a").as_bytes();
        let new_hash = *blake3::hash(b"a_updated").as_bytes();
        updated.update(100, old_hash, new_hash);

        assert_ne!(tree.root_hash(), updated.root_hash());

        // The leaf containing position MAX-100 should be unchanged.
        let num_leaves = tree.leaf_count();
        let leaf_start = num_leaves - 1;
        // Last leaf should be the same.
        assert_eq!(
            tree.hash_at(leaf_start + num_leaves - 1),
            updated.hash_at(leaf_start + num_leaves - 1)
        );
    }
}
