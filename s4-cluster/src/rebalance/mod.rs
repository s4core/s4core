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

//! Moving a grown pool's keys to their new owners (plan
//! ERASURE-CODING-FIXING-POOL-GROWTH-REBALANCE.md, phase 3).
//!
//! While a pool moves from generation `g` to `g + 1`, writes reach the
//! quorums of both rings and reads stay on `g` until an arc cuts over
//! ([`crate::placement::generation`]). This module copies what the old
//! owners hold to the new ones in the background:
//!
//! - [`families`] — which keys move, and how the move finds them;
//! - [`progress`] — the work items and their cursors in `cluster_meta`;
//! - [`service`] — the move itself: pages, settling, pause and resume;
//! - [`multipart`] — open uploads with their parts;
//! - [`limiter`] — rates, and giving way to the clients' traffic;
//! - [`space`] — room on the receivers, before the move and during it;
//! - [`config`] — the `S4_REBALANCE_*` limits;
//! - [`transition`] and [`verify`] — this node's steps, the verification,
//!   the cut-over and the barrier (phase 4);
//! - [`foreign`] — the copies a node no longer owns, handed over and removed;
//! - [`report`] — what the admin API and the metrics show, and the pool's
//!   pause (phase 5).

pub mod config;
pub mod families;
pub mod foreign;
pub mod limiter;
pub mod multipart;
pub mod progress;
pub mod report;
pub mod service;
pub mod space;
pub mod transition;
pub mod verify;

pub use config::RebalanceConfig;
pub use foreign::{ForeignCopies, ForeignMode, ForeignParts, ForeignProgress};
pub use limiter::{ForegroundLoad, QuorumLoad};
pub use multipart::{MultipartTransfer, UploadHandedOver, UploadStanding};
pub use progress::{IndexProgressStore, MemoryProgressStore, ProgressStore};
pub use report::{
    GrowthControlError, GrowthState, PoolGrowth, PoolGrowthParts, PoolGrowthReport, TransferReport,
};
pub use service::{RebalanceHandle, RebalanceParts, RebalanceService, RebalanceStatus};
pub use space::{StoredBytes, VolumeFiles};
pub use transition::{FinishedMark, PoolTransition, TransitionParts};
pub use verify::{
    QuorumRecords, RangeCheck, RangeVerifier, ReplicatedRecords, TransitionConfig, TransitionDriver,
};
