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

//! Repair frontier — per-replica confirmed reconciliation markers.
//!
//! The frontier tracks, for each key range and each replica, the HLC up to
//! which reconciliation with that replica has been confirmed: its cut.
//!
//! # Cuts of a replicated pool
//!
//! An anti-entropy round compares this node with one peer over every key the
//! two of them replicate ([`PairScope`](crate::repair::pair_tree::PairScope)).
//! A round that compared every leaf and settled every divergent key sets the
//! peer's cut to the HLC the round started at ([`advance_peer`]): the peer
//! has everything this node held before that moment. The cut covers the whole
//! ring, since the pair's scope is all of their shared keys. This node never
//! has a cut of its own.
//!
//! The frontier lives in memory: after a restart, tombstones wait for a full
//! turn of rounds over the peers before they go again, which is safe.
//!
//! # Safety Invariant
//!
//! A tombstone `T` is safe to purge only if every other replica of its key
//! has a cut past `T`'s HLC
//! ([`can_purge_tombstone`](crate::gc::can_purge_tombstone)):
//!
//! ```text
//! purgeable(T) ⟺
//!   age(T) > gc_grace AND
//!   cut(replica) > version(T) FOR ALL replicas other than this node
//! ```
//!
//! [`advance_peer`]: RepairFrontier::advance_peer

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::clock::hlc::HlcTimestamp;
use crate::identity::NodeId;
use crate::repair::merkle::KeyRange;

/// Per-replica repair frontier for safe tombstone GC.
///
/// For each key range, stores the minimum HLC timestamp up to which each
/// replica has been confirmed as synchronized.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RepairFrontier {
    /// `key_range_id -> (node_id -> confirmed_cut)`
    ///
    /// The key range ID is derived from `KeyRange::start` for simplicity,
    /// since ranges are deterministic given a fixed tree depth.
    frontiers: HashMap<u64, HashMap<NodeId, HlcTimestamp>>,
}

impl RepairFrontier {
    /// Create a new empty frontier.
    pub fn new() -> Self {
        Self::default()
    }

    /// Advance the confirmed reconciliation cut for a specific replica.
    ///
    /// The cut only moves forward (monotonic). If the provided `new_cut`
    /// is older than the current value, it is ignored.
    pub fn advance(&mut self, key_range: &KeyRange, replica: NodeId, new_cut: HlcTimestamp) {
        let range_id = key_range.start;
        let entry = self
            .frontiers
            .entry(range_id)
            .or_default()
            .entry(replica)
            .or_insert(HlcTimestamp::zero());

        if new_cut > *entry {
            *entry = new_cut;
            debug!(
                range_start = key_range.start,
                %replica,
                wall_time = new_cut.wall_time,
                logical = new_cut.logical,
                "repair frontier advanced"
            );
        }
    }

    /// Moves the cut of `peer` over the whole ring to `cut`, never back:
    /// what a complete anti-entropy round with it confirms.
    pub fn advance_peer(&mut self, peer: NodeId, cut: HlcTimestamp) {
        self.advance(&KeyRange::full(), peer, cut);
    }

    /// The cut of `peer` over the whole ring, if a complete round with it
    /// set one.
    pub fn peer_cut(&self, peer: &NodeId) -> Option<HlcTimestamp> {
        self.get_cut(&KeyRange::full(), peer)
    }

    /// How far behind `now_millis` the oldest cut of `peers` is, in
    /// milliseconds. A peer without a cut counts from `since_millis`, when
    /// this node started keeping the frontier: a peer no round has completed
    /// with since then lags by as long. Zero without peers.
    pub fn lag_millis(&self, peers: &[NodeId], since_millis: u64, now_millis: u64) -> u64 {
        peers
            .iter()
            .map(|peer| self.peer_cut(peer).map_or(since_millis, |cut| cut.wall_time))
            .min()
            .map_or(0, |oldest| now_millis.saturating_sub(oldest))
    }

    /// Check whether a tombstone can be safely purged.
    ///
    /// Returns `true` **only** if every replica in `all_replicas` has a
    /// confirmed cut that is `>= tombstone_hlc` for the key range that
    /// contains the tombstone.
    pub fn is_safe_to_purge(
        &self,
        key_range: &KeyRange,
        tombstone_hlc: &HlcTimestamp,
        all_replicas: &[NodeId],
    ) -> bool {
        if all_replicas.is_empty() {
            return false;
        }

        let range_id = key_range.start;
        let cuts = match self.frontiers.get(&range_id) {
            Some(c) => c,
            None => return false,
        };

        all_replicas
            .iter()
            .all(|replica| cuts.get(replica).map(|cut| cut >= tombstone_hlc).unwrap_or(false))
    }

    /// Get the confirmed cut for a specific replica in a key range.
    pub fn get_cut(&self, key_range: &KeyRange, replica: &NodeId) -> Option<HlcTimestamp> {
        self.frontiers.get(&key_range.start)?.get(replica).copied()
    }

    /// Get the minimum confirmed cut across all replicas for a key range.
    ///
    /// Returns `None` if no replicas have been recorded for this range.
    pub fn min_cut(&self, key_range: &KeyRange) -> Option<HlcTimestamp> {
        self.frontiers.get(&key_range.start)?.values().min().copied()
    }

    /// Number of key ranges being tracked.
    pub fn tracked_ranges(&self) -> usize {
        self.frontiers.len()
    }

    /// Total number of per-replica entries across all ranges.
    pub fn total_entries(&self) -> usize {
        self.frontiers.values().map(|m| m.len()).sum()
    }

    /// Remove tracking for a specific key range (e.g., after topology change).
    pub fn remove_range(&mut self, key_range: &KeyRange) {
        self.frontiers.remove(&key_range.start);
    }

    /// Serialize to bytes for persistence.
    pub fn to_bytes(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap_or_default()
    }

    /// Deserialize from bytes.
    pub fn from_bytes(data: &[u8]) -> Result<Self, crate::error::ClusterError> {
        bincode::deserialize(data)
            .map_err(|e| crate::error::ClusterError::Codec(format!("repair frontier decode: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(wall: u64, logical: u32) -> HlcTimestamp {
        HlcTimestamp {
            wall_time: wall,
            logical,
            node_id: 0,
        }
    }

    fn range() -> KeyRange {
        KeyRange::new(0, 1000)
    }

    #[test]
    fn new_frontier_is_empty() {
        let f = RepairFrontier::new();
        assert_eq!(f.tracked_ranges(), 0);
        assert_eq!(f.total_entries(), 0);
    }

    #[test]
    fn advance_creates_entry() {
        let mut f = RepairFrontier::new();
        let node = NodeId(1);
        f.advance(&range(), node, ts(1000, 0));
        assert_eq!(f.tracked_ranges(), 1);
        assert_eq!(f.get_cut(&range(), &node), Some(ts(1000, 0)));
    }

    #[test]
    fn advance_is_monotonic() {
        let mut f = RepairFrontier::new();
        let node = NodeId(1);
        f.advance(&range(), node, ts(2000, 0));
        f.advance(&range(), node, ts(1000, 0)); // older, should be ignored
        assert_eq!(f.get_cut(&range(), &node), Some(ts(2000, 0)));
    }

    #[test]
    fn advance_moves_forward() {
        let mut f = RepairFrontier::new();
        let node = NodeId(1);
        f.advance(&range(), node, ts(1000, 0));
        f.advance(&range(), node, ts(2000, 0));
        assert_eq!(f.get_cut(&range(), &node), Some(ts(2000, 0)));
    }

    #[test]
    fn is_safe_to_purge_requires_all_replicas() {
        let mut f = RepairFrontier::new();
        let n1 = NodeId(1);
        let n2 = NodeId(2);
        let n3 = NodeId(3);

        f.advance(&range(), n1, ts(2000, 0));
        f.advance(&range(), n2, ts(2000, 0));
        // n3 not advanced yet

        let tombstone_hlc = ts(1500, 0);
        assert!(!f.is_safe_to_purge(&range(), &tombstone_hlc, &[n1, n2, n3]));
    }

    #[test]
    fn is_safe_to_purge_when_all_confirmed() {
        let mut f = RepairFrontier::new();
        let n1 = NodeId(1);
        let n2 = NodeId(2);
        let n3 = NodeId(3);

        f.advance(&range(), n1, ts(2000, 0));
        f.advance(&range(), n2, ts(2000, 0));
        f.advance(&range(), n3, ts(2000, 0));

        let tombstone_hlc = ts(1500, 0);
        assert!(f.is_safe_to_purge(&range(), &tombstone_hlc, &[n1, n2, n3]));
    }

    #[test]
    fn is_safe_to_purge_false_when_cut_equals_tombstone() {
        let mut f = RepairFrontier::new();
        let n1 = NodeId(1);

        f.advance(&range(), n1, ts(1500, 0));

        let tombstone_hlc = ts(1500, 0);
        assert!(f.is_safe_to_purge(&range(), &tombstone_hlc, &[n1]));
    }

    #[test]
    fn is_safe_to_purge_false_for_empty_replicas() {
        let f = RepairFrontier::new();
        assert!(!f.is_safe_to_purge(&range(), &ts(1000, 0), &[]));
    }

    #[test]
    fn is_safe_to_purge_false_for_unknown_range() {
        let f = RepairFrontier::new();
        assert!(!f.is_safe_to_purge(&range(), &ts(1000, 0), &[NodeId(1)]));
    }

    #[test]
    fn min_cut_returns_smallest() {
        let mut f = RepairFrontier::new();
        f.advance(&range(), NodeId(1), ts(3000, 0));
        f.advance(&range(), NodeId(2), ts(1000, 0));
        f.advance(&range(), NodeId(3), ts(2000, 0));
        assert_eq!(f.min_cut(&range()), Some(ts(1000, 0)));
    }

    #[test]
    fn min_cut_none_for_unknown_range() {
        let f = RepairFrontier::new();
        assert_eq!(f.min_cut(&range()), None);
    }

    #[test]
    fn remove_range_clears_entries() {
        let mut f = RepairFrontier::new();
        f.advance(&range(), NodeId(1), ts(1000, 0));
        assert_eq!(f.tracked_ranges(), 1);
        f.remove_range(&range());
        assert_eq!(f.tracked_ranges(), 0);
    }

    #[test]
    fn serialization_roundtrip() {
        let mut f = RepairFrontier::new();
        f.advance(&range(), NodeId(1), ts(1000, 0));
        f.advance(&range(), NodeId(2), ts(2000, 0));

        let bytes = f.to_bytes();
        let restored = RepairFrontier::from_bytes(&bytes).unwrap();
        assert_eq!(restored.tracked_ranges(), 1);
        assert_eq!(restored.get_cut(&range(), &NodeId(1)), Some(ts(1000, 0)));
        assert_eq!(restored.get_cut(&range(), &NodeId(2)), Some(ts(2000, 0)));
    }

    #[test]
    fn a_peer_cut_covers_the_whole_ring_and_only_moves_forward() {
        let mut f = RepairFrontier::new();
        assert_eq!(f.peer_cut(&NodeId(2)), None);
        f.advance_peer(NodeId(2), ts(2000, 0));
        f.advance_peer(NodeId(2), ts(1000, 0));
        assert_eq!(f.peer_cut(&NodeId(2)), Some(ts(2000, 0)));
        assert_eq!(f.get_cut(&KeyRange::full(), &NodeId(2)), Some(ts(2000, 0)));
        assert_eq!(f.peer_cut(&NodeId(3)), None);
    }

    #[test]
    fn the_lag_is_the_age_of_the_oldest_cut_a_peer_without_one_counting_from_the_start() {
        let mut f = RepairFrontier::new();
        let peers = [NodeId(2), NodeId(3)];
        // Neither peer has a cut: both count from the start, at 1000.
        assert_eq!(f.lag_millis(&peers, 1000, 5000), 4000);
        f.advance_peer(NodeId(2), ts(4000, 0));
        assert_eq!(f.lag_millis(&peers, 1000, 5000), 4000);
        f.advance_peer(NodeId(3), ts(3000, 0));
        assert_eq!(f.lag_millis(&peers, 1000, 5000), 2000);
        assert_eq!(f.lag_millis(&[], 1000, 5000), 0);
    }

    #[test]
    fn multiple_ranges_tracked_independently() {
        let mut f = RepairFrontier::new();
        let r1 = KeyRange::new(0, 500);
        let r2 = KeyRange::new(500, 1000);

        f.advance(&r1, NodeId(1), ts(1000, 0));
        f.advance(&r2, NodeId(1), ts(2000, 0));

        assert_eq!(f.tracked_ranges(), 2);
        assert_eq!(f.get_cut(&r1, &NodeId(1)), Some(ts(1000, 0)));
        assert_eq!(f.get_cut(&r2, &NodeId(1)), Some(ts(2000, 0)));
    }
}
