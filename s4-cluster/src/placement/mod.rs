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

//! Data placement: routing keys to replica sets within server pools.
//!
//! The placement subsystem answers one question: **for a given `bucket/key`,
//! which nodes store the replicas?**
//!
//! # Two-level routing
//!
//! 1. **Bucket → Pool** ([`BucketPlacement`]) — each bucket is pinned to
//!    exactly one pool at creation time.
//! 2. **Key → Replicas** ([`PlacementStrategy`]) — within a pool, a key
//!    maps to `RF` replica nodes.
//!
//! # MVP simplification
//!
//! When `pool_size == RF` (the recommended 3-node deployment), every node
//! in the pool stores 100% of the data. The consistent hash ring is
//! bypassed in favour of a trivial O(1) broadcast to all pool members.

pub mod bucket_placement;
pub mod hash_ring;
pub mod router;

pub use bucket_placement::BucketPlacement;
pub use hash_ring::{ConsistentHashRing, PlacementStrategy};

/// Compute the ring position for a `"bucket/key"` string using BLAKE3.
///
/// This is the same hash used by [`ConsistentHashRing`] to place keys on
/// the consistent hash ring. Anti-entropy, Merkle trees, and repair use
/// this to map objects to ring positions.
pub fn ring_position(bucket: &str, key: &str) -> u64 {
    ConsistentHashRing::hash_key(&format!("{bucket}/{key}"))
}
pub use router::{PlacementDecision, PlacementRouter};
