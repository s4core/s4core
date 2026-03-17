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

//! Placement group abstraction for data ownership (Phase 6).
//!
//! A Placement Group (PG) is the unit of data ownership. In single-node mode,
//! there is exactly one PG (`PlacementGroupId::LOCAL`) which owns all data.
//! In future distributed mode, consistent hashing will assign each
//! bucket/key to a specific PG, enabling sharding and replication.
//!
//! # Single-Node Behavior
//!
//! On a single-node deployment, the PG is always `LOCAL` (ID 0). All
//! deduplication, journaling, and storage operations are scoped to this
//! single PG. The behavior is indistinguishable from a system without PGs.
//!
//! # Future Distributed Behavior
//!
//! When distributed mode is implemented (EE):
//! - Each PG is owned by a leader node
//! - Replica nodes can serve reads for PGs they replicate
//! - Journal shipping operates per-PG
//! - Deduplication is scoped per-PG (no cross-PG dedup)
//! - Atomic operations (e.g., directory rename) are guaranteed only within
//!   a single PG

use serde::{Deserialize, Serialize};

/// Identifier for a placement group.
///
/// On single-node deployments, this is always [`PlacementGroupId::LOCAL`] (0).
/// In distributed mode, it is determined by consistent hashing on the
/// bucket/key pair.
///
/// # Serialization
///
/// Serialized as a `u32` for compact storage in journal entries and dedup keys.
/// The big-endian byte representation (`to_be_bytes`) is used as a key prefix
/// in the dedup keyspace to scope entries per PG.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PlacementGroupId(pub u32);

impl PlacementGroupId {
    /// The local placement group for single-node deployments.
    ///
    /// All data belongs to this PG when running in non-distributed mode.
    pub const LOCAL: Self = PlacementGroupId(0);

    /// Returns the big-endian byte representation of this PG ID.
    ///
    /// Used as a key prefix in the dedup keyspace to scope entries per PG.
    #[inline]
    pub fn to_be_bytes(self) -> [u8; 4] {
        self.0.to_be_bytes()
    }

    /// Constructs a `PlacementGroupId` from big-endian bytes.
    #[inline]
    pub fn from_be_bytes(bytes: [u8; 4]) -> Self {
        Self(u32::from_be_bytes(bytes))
    }
}

impl Default for PlacementGroupId {
    /// Defaults to [`LOCAL`](Self::LOCAL) for backward compatibility.
    ///
    /// This ensures that deserialization of journal entries created before
    /// Phase 6 (which lack a `placement_group` field) will default to the
    /// local PG.
    fn default() -> Self {
        Self::LOCAL
    }
}

impl std::fmt::Display for PlacementGroupId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PG({})", self.0)
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_local_pg_is_zero() {
        assert_eq!(PlacementGroupId::LOCAL.0, 0);
    }

    #[test]
    fn test_default_is_local() {
        assert_eq!(PlacementGroupId::default(), PlacementGroupId::LOCAL);
    }

    #[test]
    fn test_to_from_be_bytes() {
        let pg = PlacementGroupId(42);
        let bytes = pg.to_be_bytes();
        let restored = PlacementGroupId::from_be_bytes(bytes);
        assert_eq!(pg, restored);
    }

    #[test]
    fn test_local_be_bytes_are_zeros() {
        let bytes = PlacementGroupId::LOCAL.to_be_bytes();
        assert_eq!(bytes, [0, 0, 0, 0]);
    }

    #[test]
    fn test_display() {
        assert_eq!(format!("{}", PlacementGroupId::LOCAL), "PG(0)");
        assert_eq!(format!("{}", PlacementGroupId(7)), "PG(7)");
    }

    #[test]
    fn test_serde_roundtrip() {
        let pg = PlacementGroupId(123);
        let json = serde_json::to_string(&pg).unwrap();
        let restored: PlacementGroupId = serde_json::from_str(&json).unwrap();
        assert_eq!(pg, restored);
    }

    #[test]
    fn test_bincode_roundtrip() {
        let pg = PlacementGroupId(999);
        let bytes = bincode::serialize(&pg).unwrap();
        let restored: PlacementGroupId = bincode::deserialize(&bytes).unwrap();
        assert_eq!(pg, restored);
    }

    #[test]
    fn test_hash_usable_in_collections() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(PlacementGroupId(0));
        set.insert(PlacementGroupId(1));
        set.insert(PlacementGroupId(0)); // duplicate
        assert_eq!(set.len(), 2);
    }
}
