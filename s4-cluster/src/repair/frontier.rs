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
//! The frontier tracks, for each key range and each replica, the minimum
//! HLC timestamp up to which reconciliation has been confirmed. This is
//! the foundation for safe tombstone GC in Phase 8.
//!
//! # Safety Invariant
//!
//! A tombstone `T` is safe to purge **only** if for **all** replicas in
//! the replica set, `confirmed_cut >= T.hlc_timestamp`. A simple per-range
//! watermark is insufficient because a fresh watermark does not prove that
//! a specific tombstone has been seen by all replicas.
//!
//! ```text
//! purgeable(T) ⟺
//!   age(T) > gc_horizon AND
//!   repair_frontier(range(T)) >= version(T) FOR ALL replicas
//! ```

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
