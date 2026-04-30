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

//! Distributed deduplication strategy (Phase 8).
//!
//! Deduplication remains **local** (per-node). Global cross-node dedup is
//! intentionally not implemented because distributed ref-counting in an
//! AP system introduces significant complexity without proportional benefit.
//!
//! Per-node dedup already provides substantial space savings. Cross-node
//! data duplication across replicas is the cost of replication, not a
//! dedup problem.
//!
//! # GC Strategy: Mark-Sweep
//!
//! Instead of relying on exact increment/decrement ref-counts (which are
//! fragile under crashes), the distributed dedup GC uses periodic
//! mark-sweep:
//!
//! 1. **MARK**: Scan objects keyspace → collect all live content hashes
//! 2. **SWEEP**: Scan dedup keyspace → remove entries not in live set
//!
//! This is crash-safe and idempotent.

pub mod distributed;

pub use distributed::{DedupGcConfig, DistributedDedupGc, SweepStats};
