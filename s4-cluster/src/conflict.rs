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

//! Conflict resolution strategy for S3-compatible object storage.
//!
//! # MVP: Last-Writer-Wins (LWW) by HLC
//!
//! Each write carries an HLC timestamp. When two versions of the same key
//! are found on different replicas, the version with the higher HLC wins.
//! The comparison is deterministic and total-ordered:
//! `(wall_time, logical, node_id)`.
//!
//! This is appropriate for object storage because:
//! - Objects are overwritten entirely (PUT), not partially modified
//! - Concurrent PUT of the same key is an application-level race condition
//! - LWW is the de facto standard for S3 semantics ("last PUT wins")
//! - Clients can use `If-Match` / `If-None-Match` for conditional writes
//!
//! # Post-MVP: DVV + LWW Fallback
//!
//! When DVV is enabled (post-MVP), conflict resolution becomes two-phase:
//! 1. Compare DVVs — if one dominates, it is the clear winner
//! 2. If DVVs are concurrent — fall back to LWW by HLC
//!
//! The losing version is preserved as a hidden version (not visible via S3 API)
//! for audit and debugging purposes.

use std::cmp::Ordering;

use crate::clock::hlc::HlcTimestamp;

/// The outcome of resolving a conflict between two object versions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConflictResolution {
    /// Version A wins (has higher HLC timestamp).
    PickA,
    /// Version B wins (has higher HLC timestamp).
    PickB,
}

impl ConflictResolution {
    /// Returns `true` if version A was selected as the winner.
    pub fn is_a(self) -> bool {
        self == ConflictResolution::PickA
    }

    /// Returns `true` if version B was selected as the winner.
    pub fn is_b(self) -> bool {
        self == ConflictResolution::PickB
    }
}

/// Resolve a conflict between two object versions using LWW by HLC.
///
/// The version with the higher HLC timestamp wins. Comparison is
/// deterministic and total-ordered: `(wall_time, logical, node_id)`.
///
/// All nodes in the cluster will arrive at the same result for the
/// same pair of versions, regardless of the order they are presented.
///
/// # Commutativity
///
/// `resolve_conflict(a, b)` and `resolve_conflict(b, a)` always select
/// the same physical version as the winner.
pub fn resolve_conflict(hlc_a: &HlcTimestamp, hlc_b: &HlcTimestamp) -> ConflictResolution {
    match hlc_a.cmp(hlc_b) {
        Ordering::Greater => ConflictResolution::PickA,
        Ordering::Less => ConflictResolution::PickB,
        // Equal HLC is practically impossible (requires same wall_time,
        // same logical counter, AND same node_id — meaning the same event).
        // We default to A for safety.
        Ordering::Equal => ConflictResolution::PickA,
    }
}

/// Resolve a conflict using HLC fields extracted from IndexRecords.
///
/// Convenience wrapper for use with [`s4_core::IndexRecord`] fields.
pub fn resolve_conflict_from_fields(
    wall_time_a: u64,
    logical_a: u32,
    node_id_a: u128,
    wall_time_b: u64,
    logical_b: u32,
    node_id_b: u128,
) -> ConflictResolution {
    let hlc_a = HlcTimestamp::from_record(wall_time_a, logical_a, node_id_a);
    let hlc_b = HlcTimestamp::from_record(wall_time_b, logical_b, node_id_b);
    resolve_conflict(&hlc_a, &hlc_b)
}

/// Select the winning version from a slice of HLC timestamps.
///
/// Returns the index of the winning timestamp (highest HLC).
/// Returns `None` if the slice is empty.
pub fn select_winner(timestamps: &[HlcTimestamp]) -> Option<usize> {
    timestamps
        .iter()
        .enumerate()
        .max_by(|(_, a), (_, b)| a.cmp(b))
        .map(|(idx, _)| idx)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn higher_wall_time_wins() {
        let a = HlcTimestamp {
            wall_time: 1000,
            logical: 0,
            node_id: 1,
        };
        let b = HlcTimestamp {
            wall_time: 2000,
            logical: 0,
            node_id: 2,
        };
        assert_eq!(resolve_conflict(&a, &b), ConflictResolution::PickB);
        assert_eq!(resolve_conflict(&b, &a), ConflictResolution::PickA);
    }

    #[test]
    fn same_wall_time_logical_breaks_tie() {
        let a = HlcTimestamp {
            wall_time: 1000,
            logical: 1,
            node_id: 1,
        };
        let b = HlcTimestamp {
            wall_time: 1000,
            logical: 2,
            node_id: 1,
        };
        assert_eq!(resolve_conflict(&a, &b), ConflictResolution::PickB);
        assert_eq!(resolve_conflict(&b, &a), ConflictResolution::PickA);
    }

    #[test]
    fn same_wall_time_and_logical_node_id_breaks_tie() {
        let a = HlcTimestamp {
            wall_time: 1000,
            logical: 0,
            node_id: 1,
        };
        let b = HlcTimestamp {
            wall_time: 1000,
            logical: 0,
            node_id: 2,
        };
        assert_eq!(resolve_conflict(&a, &b), ConflictResolution::PickB);
        assert_eq!(resolve_conflict(&b, &a), ConflictResolution::PickA);
    }

    #[test]
    fn equal_timestamps_pick_a() {
        let a = HlcTimestamp {
            wall_time: 1000,
            logical: 0,
            node_id: 1,
        };
        let b = HlcTimestamp {
            wall_time: 1000,
            logical: 0,
            node_id: 1,
        };
        assert_eq!(resolve_conflict(&a, &b), ConflictResolution::PickA);
    }

    #[test]
    fn commutativity_always_picks_same_physical_version() {
        let a = HlcTimestamp {
            wall_time: 1000,
            logical: 5,
            node_id: 100,
        };
        let b = HlcTimestamp {
            wall_time: 1000,
            logical: 5,
            node_id: 200,
        };

        let result_ab = resolve_conflict(&a, &b);
        let result_ba = resolve_conflict(&b, &a);

        // Both orderings must select the same physical version (b, with node_id=200)
        assert_eq!(result_ab, ConflictResolution::PickB);
        assert_eq!(result_ba, ConflictResolution::PickA);

        // Verify they agree: in both cases, the HLC with node_id=200 wins
        let winner_ab = if result_ab.is_a() { &a } else { &b };
        let winner_ba = if result_ba.is_a() { &b } else { &a };
        assert_eq!(winner_ab, winner_ba);
    }

    #[test]
    fn resolve_from_fields() {
        let result = resolve_conflict_from_fields(1000, 0, 1, 2000, 0, 2);
        assert_eq!(result, ConflictResolution::PickB);
    }

    #[test]
    fn select_winner_picks_highest() {
        let timestamps = vec![
            HlcTimestamp {
                wall_time: 1000,
                logical: 0,
                node_id: 1,
            },
            HlcTimestamp {
                wall_time: 3000,
                logical: 0,
                node_id: 2,
            },
            HlcTimestamp {
                wall_time: 2000,
                logical: 0,
                node_id: 3,
            },
        ];
        assert_eq!(select_winner(&timestamps), Some(1));
    }

    #[test]
    fn select_winner_empty() {
        let timestamps: Vec<HlcTimestamp> = vec![];
        assert_eq!(select_winner(&timestamps), None);
    }

    #[test]
    fn select_winner_single() {
        let timestamps = vec![HlcTimestamp {
            wall_time: 1000,
            logical: 0,
            node_id: 1,
        }];
        assert_eq!(select_winner(&timestamps), Some(0));
    }

    #[test]
    fn resolution_accessors() {
        assert!(ConflictResolution::PickA.is_a());
        assert!(!ConflictResolution::PickA.is_b());
        assert!(ConflictResolution::PickB.is_b());
        assert!(!ConflictResolution::PickB.is_a());
    }
}
