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

//! Distributed garbage collection for tombstones and dedup entries (Phase 8).
//!
//! This module provides safe distributed GC that prevents zombie resurrection
//! of deleted objects. The core safety properties are:
//!
//! - Tombstones live for at least `gc_grace` (default: 7 days)
//! - Tombstone purge requires repair frontier confirmation from ALL replicas
//! - Nodes offline longer than `max_rejoin_downtime` must full-bootstrap
//!
//! # Architecture
//!
//! ```text
//! DistributedGcService (background loop, every gc_interval)
//! ├── Tombstone scan → TombstonePolicy + RepairFrontier check → purge
//! └── ZombieGuard → node rejoin validation
//! ```

pub mod service;
pub mod tombstone;
pub mod zombie_guard;

pub use service::DistributedGcService;
pub use tombstone::{can_purge_tombstone, GcConfig, TombstonePolicy};
pub use zombie_guard::{RejoinDecision, ZombieGuard};
