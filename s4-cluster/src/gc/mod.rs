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

//! Distributed garbage collection of the tombstones of a replicated pool.
//!
//! This module removes tombstones without letting a deleted object come back.
//! The core safety properties are:
//!
//! - Tombstones live for at least `gc_grace` (default: 7 days)
//! - A node removes a tombstone only once the repair frontier holds a cut past
//!   it for every other replica of its key; the node itself is not in the
//!   condition
//! - `gc_grace > max_rejoin_downtime + anti-entropy interval + clock skew`
//!
//! # Architecture
//!
//! ```text
//! DistributedGcService (background loop, every S4_TOMBSTONE_GC_INTERVAL_SECS)
//! └── version digests → tombstones → age + cuts of the other replicas → purge
//! ```

pub mod service;
pub mod tombstone;
pub mod zombie_guard;

pub use service::DistributedGcService;
pub use tombstone::{can_purge_tombstone, GcConfig, TombstonePolicy};
pub use zombie_guard::{RejoinDecision, ZombieGuard};
