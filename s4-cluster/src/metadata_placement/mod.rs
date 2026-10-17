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

//! The move of a pool's bucket metadata from the replicas of its data to as
//! many replicas as the pool survives failures (plan
//! BUCKET-METADATA-REPLICATION.md, decisions 2 and 4).
//!
//! A bucket's marker and configurations are placed on the pool's ring like
//! its objects, three replicas with quorums of two. An erasure-coded pool of
//! six nodes survives two failures, and the bucket metadata only one. The
//! pool's target for them ([`target`]) is derived from what the pool
//! survives, and the pool reaches it through four stages ([`Stage`]): it
//! writes to both layouts, then reads from the new one, then forgets the old
//! one. A node takes the next stage only once every node of its pool
//! announced the current one ([`gate`]), so no two nodes are ever more than
//! one stage apart, and the rules of two neighbouring stages always meet: a
//! read finds every acknowledged write (`compatibility`, the exhaustive
//! check). A node keeps its record in `cluster_meta` ([`store`]) and
//! announces it in gossip.
//!
//! The module speaks of an old and a new layout, not of three and five
//! replicas on one ring: the move of a pool to a grown ring is the same move
//! between two other layouts.
//!
//! The router and the replicas apply the stages to traffic: a key is placed,
//! and a write of it fenced, by the stage in this node's [`StageCell`]. The
//! [`driver`] takes the stages one after another, and at stage 1 runs the
//! [`backfill`] of the bucket metadata stored before the move; [`status`] is
//! where it stands, as the metrics and the admin API report it.

pub mod backfill;
pub mod cell;
pub mod driver;
pub mod gate;
pub mod stage;
pub mod status;
pub mod store;
pub mod target;

#[cfg(test)]
mod cluster_tests;
#[cfg(test)]
mod compatibility;
#[cfg(test)]
mod traffic_tests;

pub use backfill::{Backfill, BackfillPass, BackfillReport};
pub use cell::{StageCell, StageTicket};
pub use driver::{backfill_holdouts, PlacementBoard, StageDriver, BACKFILL_RETRY, GATE_INTERVAL};
pub use gate::{next_stage, GateVerdict, Holdout, HoldoutReason, PeerPlacement, PlacementView};
pub use stage::{
    PlacementNote, PlacementRecord, QuorumRequirement, ReplicaSide, Stage, StageRules,
};
pub use status::{PlacementSnapshot, PlacementStatus, HOLD_WARN_INTERVAL};
pub use target::{derive_target, QuorumShape};
