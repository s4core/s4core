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

//! Dotted Version Vectors (DVV) for causal conflict detection.
//!
//! # Post-MVP Module
//!
//! These structures are **prepared for post-MVP** and are NOT used in the
//! current write path. The MVP uses pure LWW by HLC timestamp (see
//! [`crate::conflict`]).
//!
//! # Why DVV is Deferred (ADR-F4-REVISED)
//!
//! DVV requires a causal write path: the coordinator MUST read the current
//! causal context (the DVV of the current version) before writing a new one.
//! Without this, `increment()` creates a "blind" dot, and all overwrites
//! appear as concurrent events instead of sequential ones. This leads to:
//!
//! - Excessive sibling accumulation
//! - Tombstones failing to dominate previous versions
//! - LWW masking the problem rather than solving it
//!
//! # Post-MVP Plan
//!
//! The coordinator will perform a quorum HEAD before each write to obtain
//! the current DVV context, merge it, increment the dot, then write. This
//! adds one round-trip to the write path but enables true causal tracking.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Slot identifying a replica position within a pool (0, 1, 2 for RF=3).
///
/// Bound to the pool position, not a physical node. When a node is replaced,
/// the new node inherits the same slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ReplicaSlot(pub u8);

impl std::fmt::Display for ReplicaSlot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "slot-{}", self.0)
    }
}

/// A Dotted Version Vector for tracking causal history of an object.
///
/// Each DVV consists of:
/// - A version vector mapping each replica slot to its maximum known counter
/// - An optional dot representing the specific event that created this version
///
/// # Post-MVP
///
/// This structure exists to prepare the codebase for causal conflict detection.
/// It is NOT used in the MVP write path. See module-level docs for rationale.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DottedVersionVector {
    /// Version vector: replica_slot -> max counter.
    pub entries: HashMap<ReplicaSlot, u64>,
    /// The specific event (dot) that created this version.
    pub dot: Option<(ReplicaSlot, u64)>,
}

/// Result of comparing two version vectors.
#[derive(Debug, PartialEq, Eq)]
pub enum VersionOrder {
    /// `self` happened before `other` (self < other).
    Before,
    /// `self` happened after `other` (self > other).
    After,
    /// Both represent the same causal history.
    Equal,
    /// Neither dominates — these are concurrent writes.
    Concurrent,
}

impl DottedVersionVector {
    /// Create an empty DVV (no causal history).
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
            dot: None,
        }
    }

    /// Create a DVV with a single dot (first write by a replica).
    pub fn with_dot(slot: ReplicaSlot, counter: u64) -> Self {
        let mut entries = HashMap::new();
        entries.insert(slot, counter);
        Self {
            entries,
            dot: Some((slot, counter)),
        }
    }

    /// Post-MVP: Increment this DVV with causal context from a quorum read.
    ///
    /// The caller MUST provide `causal_context` obtained from a quorum HEAD
    /// before calling this method. Without it, the new dot would be "blind"
    /// and all writes would appear concurrent.
    ///
    /// Returns a new DVV that dominates both `self` and `causal_context`.
    pub fn increment_with_context(&self, causal_context: &Self, slot: ReplicaSlot) -> Self {
        let mut merged_entries = self.entries.clone();

        // Merge causal context into our entries (take max per slot)
        for (&ctx_slot, &ctx_counter) in &causal_context.entries {
            let entry = merged_entries.entry(ctx_slot).or_insert(0);
            *entry = (*entry).max(ctx_counter);
        }

        // Increment the counter for our slot
        let new_counter = merged_entries.get(&slot).copied().unwrap_or(0) + 1;
        merged_entries.insert(slot, new_counter);

        Self {
            entries: merged_entries,
            dot: Some((slot, new_counter)),
        }
    }

    /// Compare two DVVs to determine causal ordering.
    pub fn compare(&self, other: &Self) -> VersionOrder {
        let self_dominates = self.dominates(other);
        let other_dominates = other.dominates(self);

        match (self_dominates, other_dominates) {
            (true, true) => VersionOrder::Equal,
            (true, false) => VersionOrder::After,
            (false, true) => VersionOrder::Before,
            (false, false) => VersionOrder::Concurrent,
        }
    }

    /// Merge two DVVs, producing a new DVV that represents the union
    /// of both causal histories. The dot is cleared since the merged
    /// DVV doesn't represent a single event.
    pub fn merge(&self, other: &Self) -> Self {
        let mut entries = self.entries.clone();
        for (&slot, &counter) in &other.entries {
            let entry = entries.entry(slot).or_insert(0);
            *entry = (*entry).max(counter);
        }
        Self { entries, dot: None }
    }

    /// Returns true if `self` dominates (or equals) `other`.
    ///
    /// `self` dominates `other` when for every slot in `other`, `self`
    /// has an equal or greater counter value.
    fn dominates(&self, other: &Self) -> bool {
        for (&slot, &counter) in &other.entries {
            let self_counter = self.entries.get(&slot).copied().unwrap_or(0);
            if self_counter < counter {
                return false;
            }
        }
        true
    }
}

impl Default for DottedVersionVector {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_dvv_equals_empty() {
        let a = DottedVersionVector::new();
        let b = DottedVersionVector::new();
        assert_eq!(a.compare(&b), VersionOrder::Equal);
    }

    #[test]
    fn single_dot_dominates_empty() {
        let a = DottedVersionVector::with_dot(ReplicaSlot(0), 1);
        let b = DottedVersionVector::new();
        assert_eq!(a.compare(&b), VersionOrder::After);
        assert_eq!(b.compare(&a), VersionOrder::Before);
    }

    #[test]
    fn sequential_writes_are_ordered() {
        let slot = ReplicaSlot(0);
        let v1 = DottedVersionVector::with_dot(slot, 1);
        let v2 = v1.increment_with_context(&v1, slot);

        assert_eq!(v2.compare(&v1), VersionOrder::After);
        assert_eq!(v1.compare(&v2), VersionOrder::Before);
    }

    #[test]
    fn concurrent_writes_detected() {
        // Two replicas write independently without reading each other's context
        let a = DottedVersionVector::with_dot(ReplicaSlot(0), 1);
        let b = DottedVersionVector::with_dot(ReplicaSlot(1), 1);
        assert_eq!(a.compare(&b), VersionOrder::Concurrent);
        assert_eq!(b.compare(&a), VersionOrder::Concurrent);
    }

    #[test]
    fn merge_produces_union() {
        let a = DottedVersionVector::with_dot(ReplicaSlot(0), 3);
        let b = DottedVersionVector::with_dot(ReplicaSlot(1), 2);
        let merged = a.merge(&b);

        assert_eq!(merged.entries.get(&ReplicaSlot(0)), Some(&3));
        assert_eq!(merged.entries.get(&ReplicaSlot(1)), Some(&2));
        assert!(merged.dot.is_none(), "merged DVV should not have a dot");
    }

    #[test]
    fn merge_takes_max_per_slot() {
        let mut a = DottedVersionVector::new();
        a.entries.insert(ReplicaSlot(0), 5);
        a.entries.insert(ReplicaSlot(1), 2);

        let mut b = DottedVersionVector::new();
        b.entries.insert(ReplicaSlot(0), 3);
        b.entries.insert(ReplicaSlot(1), 7);

        let merged = a.merge(&b);
        assert_eq!(merged.entries[&ReplicaSlot(0)], 5);
        assert_eq!(merged.entries[&ReplicaSlot(1)], 7);
    }

    #[test]
    fn increment_with_context_advances_correctly() {
        let slot_a = ReplicaSlot(0);
        let slot_b = ReplicaSlot(1);

        // Replica A writes v1
        let v1 = DottedVersionVector::with_dot(slot_a, 1);

        // Replica B reads v1 as context, writes v2
        let v2 = DottedVersionVector::new().increment_with_context(&v1, slot_b);

        // v2 should dominate v1
        assert_eq!(v2.compare(&v1), VersionOrder::After);
        assert_eq!(v2.dot, Some((slot_b, 1)));
        assert_eq!(v2.entries[&slot_a], 1); // inherited from context
        assert_eq!(v2.entries[&slot_b], 1); // own write
    }

    #[test]
    fn increment_without_context_creates_concurrent() {
        let slot_a = ReplicaSlot(0);
        let slot_b = ReplicaSlot(1);

        // Both replicas write independently (no context exchange)
        let v1 = DottedVersionVector::with_dot(slot_a, 1);
        let v2 = DottedVersionVector::with_dot(slot_b, 1);

        // These are concurrent — exactly the problem that deferred DVV to post-MVP
        assert_eq!(v1.compare(&v2), VersionOrder::Concurrent);
    }

    #[test]
    fn dominates_checks_all_slots() {
        let mut a = DottedVersionVector::new();
        a.entries.insert(ReplicaSlot(0), 3);
        a.entries.insert(ReplicaSlot(1), 2);

        let mut b = DottedVersionVector::new();
        b.entries.insert(ReplicaSlot(0), 2);
        b.entries.insert(ReplicaSlot(1), 3);

        // Neither dominates: a has higher slot-0, b has higher slot-1
        assert_eq!(a.compare(&b), VersionOrder::Concurrent);
    }

    #[test]
    fn equal_dvvs() {
        let mut a = DottedVersionVector::new();
        a.entries.insert(ReplicaSlot(0), 3);
        a.entries.insert(ReplicaSlot(1), 2);

        let mut b = DottedVersionVector::new();
        b.entries.insert(ReplicaSlot(0), 3);
        b.entries.insert(ReplicaSlot(1), 2);

        assert_eq!(a.compare(&b), VersionOrder::Equal);
    }

    #[test]
    fn replica_slot_display() {
        assert_eq!(ReplicaSlot(0).to_string(), "slot-0");
        assert_eq!(ReplicaSlot(2).to_string(), "slot-2");
    }

    #[test]
    fn serialization_roundtrip() {
        let dvv = DottedVersionVector::with_dot(ReplicaSlot(1), 42);
        let json = serde_json::to_string(&dvv).unwrap();
        let back: DottedVersionVector = serde_json::from_str(&json).unwrap();
        assert_eq!(back.dot, Some((ReplicaSlot(1), 42)));
        assert_eq!(back.entries[&ReplicaSlot(1)], 42);
    }
}
