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
//! - **Read repair** — opportunistic fix of stale replicas detected during
//!   quorum reads.
//! - **Anti-entropy** — rounds that compare a node with one peer at a time by
//!   the version digests the storage keeps for every replicated key, and
//!   settle the keys that differ.
//! - **Settling a key** — bringing the replicas of one key to its newest
//!   version, shared by anti-entropy and hint delivery.
//! - **Repair frontier** — per-replica reconciliation markers used by
//!   tombstone GC to determine safe purge points.
//! - **Merkle tree** — built in memory by the erasure-coding anti-entropy of
//!   the Enterprise Edition.

pub mod anti_entropy;
pub mod config_adoption;
pub mod config_adoption_status;
pub mod frontier;
pub mod merkle;
pub mod pair_tree;
pub mod read_repair;
pub mod reconcile;

pub use anti_entropy::{
    AntiEntropyGrowth, AntiEntropyMetrics, AntiEntropyResponder, AntiEntropyRounds,
    AntiEntropyService, AntiEntropySnapshot, RoundEnd, RoundReport,
};
pub use config_adoption::{AdoptionReport, ConfigAdoption, ConfigReadGate};
pub use config_adoption_status::{
    ConfigAdoptionPhase, ConfigAdoptionSnapshot, ConfigAdoptionStatus,
};
pub use frontier::RepairFrontier;
pub use merkle::{KeyRange, MerkleTree};
pub use read_repair::ReadRepairService;
pub use reconcile::{CopyMeter, LocalReplica, Reconciler};
