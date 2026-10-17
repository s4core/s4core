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

//! Where the move of the pool's bucket metadata stands on this node, as the
//! metrics and `/cluster/health` report it (plan
//! BUCKET-METADATA-REPLICATION.md, decision 8).
//!
//! The driver of the stages writes it: the record it is at, and the nodes the
//! gate of the next stage waits for. The metrics `s4_bucket_metadata_stage`
//! and `s4_bucket_metadata_replication_factor` are set from the same record
//! in the same call, so a scrape and the admin route of one node agree.

use std::sync::RwLock;
use std::time::{Duration, Instant};

use crate::cluster_metrics;
use crate::metadata_placement::gate::Holdout;
use crate::metadata_placement::stage::PlacementRecord;

/// How long the gate of the next stage holds before the driver warns, and
/// how often it warns again while it still holds.
pub const HOLD_WARN_INTERVAL: Duration = Duration::from_secs(600);

/// The move of one pool's bucket metadata as this node is at it.
#[derive(Debug)]
pub struct PlacementStatus {
    pool: String,
    state: RwLock<StatusState>,
}

#[derive(Debug, Clone)]
struct StatusState {
    record: PlacementRecord,
    holdouts: Vec<Holdout>,
    held_since: Option<Instant>,
    warned_at: Option<Instant>,
}

/// What [`PlacementStatus::snapshot`] reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlacementSnapshot {
    /// The record this node is at.
    pub record: PlacementRecord,
    /// The nodes the next stage waits for, empty once the move settled.
    pub holdouts: Vec<Holdout>,
    /// How long the next stage has waited.
    pub held_for: Option<Duration>,
}

impl PlacementStatus {
    /// The status of `pool`, whose node starts at `record`.
    pub fn new(pool: impl Into<String>, record: PlacementRecord) -> Self {
        let status = Self {
            pool: pool.into(),
            state: RwLock::new(StatusState {
                record,
                holdouts: Vec::new(),
                held_since: None,
                warned_at: None,
            }),
        };
        cluster_metrics::set_bucket_metadata_placement(&status.pool, &record);
        status
    }

    /// What the move stands at, as of `now`.
    pub fn snapshot(&self, now: Instant) -> PlacementSnapshot {
        let state = self.read();
        PlacementSnapshot {
            record: state.record,
            holdouts: state.holdouts.clone(),
            held_for: state.held_since.map(|since| now.saturating_duration_since(since)),
        }
    }

    /// This node is at `record` now. A new stage starts the wait for the next
    /// one afresh.
    pub(crate) fn set_record(&self, record: PlacementRecord) {
        let mut state = self.write();
        if state.record.stage != record.stage || record.stage.next().is_none() {
            state.holdouts.clear();
            state.held_since = None;
            state.warned_at = None;
        }
        state.record = record;
        cluster_metrics::set_bucket_metadata_placement(&self.pool, &record);
    }

    /// The next stage waits for `holdouts` as of `now`; `Some(how long)` when
    /// it has waited [`HOLD_WARN_INTERVAL`] since it began or since the last
    /// time this answered `Some`, so the driver warns once an interval.
    pub(crate) fn set_hold(&self, holdouts: Vec<Holdout>, now: Instant) -> Option<Duration> {
        let mut state = self.write();
        let since = *state.held_since.get_or_insert(now);
        state.holdouts = holdouts;
        let held_for = now.saturating_duration_since(since);
        let last = state.warned_at.unwrap_or(since);
        if now.saturating_duration_since(last) < HOLD_WARN_INTERVAL {
            return None;
        }
        state.warned_at = Some(now);
        Some(held_for)
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, StatusState> {
        self.state.read().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, StatusState> {
        self.state.write().unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::NodeId;
    use crate::metadata_placement::gate::HoldoutReason;
    use crate::metadata_placement::stage::Stage;
    use crate::metadata_placement::target::QuorumShape;

    fn record(stage: Stage) -> PlacementRecord {
        PlacementRecord {
            stage,
            ..PlacementRecord::starting(QuorumShape::majority(3), QuorumShape::majority(5))
        }
    }

    fn down(node: u128) -> Vec<Holdout> {
        vec![Holdout {
            node: NodeId(node),
            reason: HoldoutReason::Down,
        }]
    }

    #[test]
    fn a_gate_that_holds_is_warned_about_once_an_interval() {
        let status = PlacementStatus::new("pool-1", record(Stage::DualWrite));
        let start = Instant::now();
        assert_eq!(status.set_hold(down(2), start), None);
        assert_eq!(
            status.set_hold(down(3), start + HOLD_WARN_INTERVAL / 2),
            None
        );
        let first = start + HOLD_WARN_INTERVAL;
        assert_eq!(status.set_hold(down(3), first), Some(HOLD_WARN_INTERVAL));
        assert_eq!(
            status.set_hold(down(3), first + Duration::from_secs(1)),
            None
        );
        assert_eq!(
            status.set_hold(down(3), first + HOLD_WARN_INTERVAL),
            Some(HOLD_WARN_INTERVAL * 2)
        );
        let snapshot = status.snapshot(first + HOLD_WARN_INTERVAL);
        assert_eq!(snapshot.holdouts, down(3));
        assert_eq!(snapshot.held_for, Some(HOLD_WARN_INTERVAL * 2));
    }

    #[test]
    fn a_new_stage_waits_afresh_and_a_settled_move_waits_for_nothing() {
        let status = PlacementStatus::new("pool-1", record(Stage::DualWrite));
        let start = Instant::now();
        status.set_hold(down(2), start);
        status.set_record(PlacementRecord {
            backfill_complete: true,
            ..record(Stage::DualWrite)
        });
        assert_eq!(
            status.snapshot(start).holdouts,
            down(2),
            "the backfill ending is no new stage"
        );
        status.set_record(record(Stage::ReadNew));
        let snapshot = status.snapshot(start + HOLD_WARN_INTERVAL);
        assert!(snapshot.holdouts.is_empty());
        assert_eq!(snapshot.held_for, None);
        assert_eq!(status.set_hold(down(4), start + HOLD_WARN_INTERVAL), None);
        status.set_record(record(Stage::Settled));
        let snapshot = status.snapshot(start + HOLD_WARN_INTERVAL);
        assert_eq!(snapshot.record.stage, Stage::Settled);
        assert!(snapshot.holdouts.is_empty());
        assert_eq!(snapshot.held_for, None);
    }
}
