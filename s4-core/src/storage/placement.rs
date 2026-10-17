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

/// Position of `bucket/key` on the consistent hash ring of a cluster pool.
///
/// The first eight bytes of the BLAKE3 hash of `"{bucket}/{key}"`, big-endian.
/// This is the one definition: placement picks a key's replicas from it, and
/// the version digests of the storage are ordered by it, so that the keys of a
/// ring range are one contiguous scan. Changing it moves every key to other
/// replicas.
pub fn ring_position(bucket: &str, key: &str) -> u64 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(bucket.as_bytes());
    hasher.update(b"/");
    hasher.update(key.as_bytes());
    position_of_hash(hasher.finalize())
}

/// [`ring_position`] of a key already joined as `"{bucket}/{key}"`, the form
/// the objects keyspace and placement hold it in.
pub fn ring_position_of(placement_key: &str) -> u64 {
    ring_position_of_bytes(placement_key.as_bytes())
}

/// [`ring_position_of`] for a key read back from the index as bytes.
pub(crate) fn ring_position_of_bytes(placement_key: &[u8]) -> u64 {
    position_of_hash(blake3::hash(placement_key))
}

fn position_of_hash(hash: blake3::Hash) -> u64 {
    let mut position = [0u8; 8];
    position.copy_from_slice(&hash.as_bytes()[..8]);
    u64::from_be_bytes(position)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    /// Values of the hash placement used before it moved here: a changed
    /// value would move the key to other replicas on every cluster.
    #[test]
    fn ring_positions_are_the_ones_placement_always_used() {
        let known = [
            ("photos", "cat.jpg", 0x93df_64df_ad8d_ee14_u64),
            ("bucket", "a", 0x9941_e5f1_e206_5f44),
            (
                "__system__",
                "__s4_bucket_marker_photos",
                0xb44a_e942_cce5_d990,
            ),
            ("b", "ключ с пробелом", 0x4fa8_5b43_f1ee_cf9e),
            ("bucket", "x#v1", 0xa9b2_a485_5301_a362),
            ("a", "", 0x6fbf_8949_5ee1_dd0d),
        ];
        for (bucket, key, position) in known {
            assert_eq!(ring_position(bucket, key), position, "{bucket}/{key}");
            assert_eq!(
                ring_position_of(&format!("{bucket}/{key}")),
                position,
                "{bucket}/{key}"
            );
        }
    }

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
