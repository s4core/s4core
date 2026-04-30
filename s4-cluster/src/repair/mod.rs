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

//! Repair subsystem for maintaining replica consistency.
//!
//! The repair system has three layers:
//!
//! - **Read repair** (Phase 5) — opportunistic fix of stale replicas
//!   detected during quorum reads.
//! - **Anti-entropy** (Phase 7) — periodic background Merkle tree
//!   exchange with random replica peers for full-coverage consistency.
//! - **Repair frontier** (Phase 7) — per-replica reconciliation markers
//!   used by tombstone GC (Phase 8) to determine safe purge points.

pub mod anti_entropy;
pub mod frontier;
pub mod merkle;
pub mod read_repair;

pub use anti_entropy::{AntiEntropyMetrics, AntiEntropyService, AntiEntropySnapshot};
pub use frontier::RepairFrontier;
pub use merkle::{KeyRange, MerkleTree};
pub use read_repair::ReadRepairService;
