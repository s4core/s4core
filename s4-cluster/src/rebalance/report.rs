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

//! What an operator sees of a pool's growth, and how they pause it (plan
//! ERASURE-CODING-FIXING-POOL-GROWTH-REBALANCE.md, phase 5).
//!
//! [`PoolGrowth`] answers `GET /api/admin/cluster/rebalance` and the
//! `s4_pool_generation` and `s4_rebalance_*` gauges from the same state:
//! the node's generation, its part in the move ([`PoolTransition`]) and the
//! move of its keys ([`super::RebalanceHandle`]). The counters of keys, bytes
//! and errors are incremented where the move does the work.
//!
//! A pause is the pool's, not one node's: it is written through the quorums
//! as the pool's pause record ([`PauseRecord`]), and every member's driver
//! reads it and pauses or resumes its own move — a member that was down when
//! the operator paused does so once it is back.

use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::Serialize;
use tokio::sync::watch;
use tracing::info;

use super::space::StoredBytes;
use super::transition::PoolTransition;
use super::verify::ReplicatedRecords;
use crate::cluster_metrics::{set_pool_growth, PoolGrowthGauges};
use crate::placement::{GenerationPhase, GenerationRange, RangeCounts, ReplicationClass};
use crate::pool_generation::{self, PauseRecord, PoolGenerationRecord};
use crate::PlacementRouter;

/// The reason a pause through the admin API carries when the operator gave
/// none.
pub const DEFAULT_PAUSE_REASON: &str = "paused through the admin API";

/// Where a pool stands in its growth.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GrowthState {
    /// One generation places every key: the pool does not grow, or settled.
    Stable,
    /// A grown generation is configured; the move waits to begin.
    Prepared,
    /// The keys move: writes reach both generations, reads follow each
    /// range's cut-over.
    Moving,
}

/// A generation of the pool, in short.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct GenerationSummary {
    /// Its epoch.
    pub epoch: u64,
    /// Its members.
    pub nodes: usize,
}

/// The ranges of the ring of a growing pool by state, on this node.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct RangeReport {
    /// Every range.
    pub total: usize,
    /// Not begun.
    pub pending: usize,
    /// Being moved.
    pub streaming: usize,
    /// Verified, not yet read through the grown generation.
    pub verified: usize,
    /// Read through the grown generation.
    pub cut_over: usize,
}

impl From<RangeCounts> for RangeReport {
    fn from(counts: RangeCounts) -> Self {
        Self {
            total: counts.total(),
            pending: counts.pending,
            streaming: counts.streaming,
            verified: counts.verified,
            cut_over: counts.cut_over,
        }
    }
}

/// The move of this node's keys to the new owners.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TransferReport {
    /// Whether this node handed over everything it holds of the earlier
    /// generation.
    pub handed_over: bool,
    /// Work items: the ranges, the records of `__system__`, the uploads.
    pub items_total: usize,
    /// Items handed over in full.
    pub items_done: usize,
    /// Ranges among the items.
    pub ranges_total: usize,
    /// Ranges handed over in full.
    pub ranges_done: usize,
    /// Keys read since the move began, restarts included.
    pub keys_scanned: u64,
    /// Keys some new owner received a version of.
    pub keys_moved: u64,
    /// Bytes of copies sent.
    pub bytes_moved: u64,
    /// Estimate of what is left to go through: the bytes this node stores,
    /// in the share of the ranges it owned not handed over yet. It counts
    /// everything the node stores, erasure-coded shards included, which do
    /// not move, so on such a pool it is an upper bound.
    pub remaining_bytes: u64,
    /// Estimate of the seconds left, from the pace since this node started
    /// or the move was last resumed; `null` until there is a pace to go by.
    pub eta_secs: Option<u64>,
    /// Items that wait, and why: a new owner short of space, keys to retry.
    pub waiting: BTreeMap<String, String>,
    /// Keys that failed a try, by reason, since this node started; each is
    /// tried again.
    pub errors: BTreeMap<String, u64>,
}

/// What `GET /api/admin/cluster/rebalance` answers: the pool's growth as
/// this node is at it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PoolGrowthReport {
    /// The pool's name.
    pub pool: String,
    /// The pool's identifier.
    pub pool_id: u32,
    /// Where the growth stands.
    pub state: GrowthState,
    /// The generation that places keys on this node: while the pool grows,
    /// the one its keys move from.
    pub generation: GenerationSummary,
    /// The grown generation, while the pool grows.
    pub moving_to: Option<GenerationSummary>,
    /// Why the growth waits on this node, as its driver last saw it: a
    /// member that has not announced the grown generation, a new member
    /// short of space, a member that has not handed over yet.
    pub waiting: Option<String>,
    /// Why an operator paused the move, while they have.
    pub paused: Option<String>,
    /// Whether this node refuses writes of coordinators that place by the
    /// earlier generation: every member began the move.
    pub fenced_earlier_generation: bool,
    /// The ranges of the ring by state on this node; all zero while the
    /// pool does not grow.
    pub ranges: RangeReport,
    /// The move of this node's keys, when it holds keys of the earlier
    /// generation; `null` on a node new to the pool, or while it does not
    /// grow.
    pub transfer: Option<TransferReport>,
}

/// Why a pause or a resumption was refused.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum GrowthControlError {
    /// The pool does not grow: there is nothing to pause.
    #[error("{0}")]
    NotGrowing(String),
    /// The pool's pause record could not be written, or applied here.
    #[error("{0}")]
    Unavailable(String),
}

/// Everything [`PoolGrowth`] reads.
pub struct PoolGrowthParts {
    /// The pool's name, the `pool` label of the metrics.
    pub pool: String,
    /// The durable generation this node started with.
    pub record: PoolGenerationRecord,
    /// This node's part in the growth, while one is prepared or runs.
    pub transition: Option<Arc<PoolTransition>>,
    /// Placement of both generations: which ranges this node owned.
    pub router: Arc<PlacementRouter>,
    /// What this node stores.
    pub stored_bytes: Arc<dyn StoredBytes>,
    /// Where the pool's pause record is written.
    pub records: Arc<dyn ReplicatedRecords>,
}

/// The pace of the move, measured from a starting point.
#[derive(Debug, Clone, Copy)]
struct Pace {
    since: Instant,
    done_then: f64,
}

/// The pool's growth as the admin API and the metrics see it.
pub struct PoolGrowth {
    pool: String,
    record: PoolGenerationRecord,
    transition: Option<Arc<PoolTransition>>,
    router: Arc<PlacementRouter>,
    stored_bytes: Arc<dyn StoredBytes>,
    records: Arc<dyn ReplicatedRecords>,
    pace: Mutex<Option<Pace>>,
}

impl PoolGrowth {
    /// The growth `parts` describe.
    pub fn new(parts: PoolGrowthParts) -> Self {
        Self {
            pool: parts.pool,
            record: parts.record,
            transition: parts.transition,
            router: parts.router,
            stored_bytes: parts.stored_bytes,
            records: parts.records,
            pace: Mutex::new(None),
        }
    }

    /// Where the growth stands on this node now.
    pub fn report(&self) -> PoolGrowthReport {
        let pool_id = self.record.pool_id.as_u32();
        let Some(transition) = &self.transition else {
            return PoolGrowthReport {
                pool: self.pool.clone(),
                pool_id,
                state: GrowthState::Stable,
                generation: summary(&self.record),
                moving_to: None,
                waiting: None,
                paused: None,
                fenced_earlier_generation: false,
                ranges: RangeReport::default(),
                transfer: None,
            };
        };
        let phase = transition.phase();
        if phase == GenerationPhase::Finished {
            self.reset_pace();
            return PoolGrowthReport {
                pool: self.pool.clone(),
                pool_id,
                state: GrowthState::Stable,
                generation: summary(&transition.record()),
                moving_to: None,
                waiting: None,
                paused: None,
                fenced_earlier_generation: false,
                ranges: RangeReport::default(),
                transfer: None,
            };
        }
        let origin = transition.origin();
        let paused = transition.paused();
        PoolGrowthReport {
            pool: self.pool.clone(),
            pool_id,
            state: if phase == GenerationPhase::Moving {
                GrowthState::Moving
            } else {
                GrowthState::Prepared
            },
            generation: summary(origin),
            moving_to: origin.proposed.as_ref().map(|next| GenerationSummary {
                epoch: next.epoch,
                nodes: next.nodes.len(),
            }),
            waiting: transition.waiting(),
            fenced_earlier_generation: transition.fences_earlier_generation(),
            ranges: transition.ranges().counts().into(),
            transfer: self.transfer(transition, phase, paused.is_some()),
            paused,
        }
    }

    /// The move of this node's keys, when it holds keys of the earlier
    /// generation.
    fn transfer(
        &self,
        transition: &PoolTransition,
        phase: GenerationPhase,
        paused: bool,
    ) -> Option<TransferReport> {
        if !transition.origin().current.nodes.contains(&transition.local()) {
            return None;
        }
        let handle = transition.rebalance();
        let status = handle.as_ref().map(|handle| handle.status()).unwrap_or_default();
        let handed_over = transition.handed_over();
        let remaining_share = if handed_over {
            0.0
        } else {
            let done =
                handle.as_ref().map(|handle| handle.ranges_handed_over()).unwrap_or_default();
            self.remaining_share(transition, &done)
        };
        let stored = self.stored_bytes.stored_bytes().unwrap_or(0);
        let remaining_bytes = (stored as f64 * remaining_share).round() as u64;
        let held_up = paused || status.waiting_for_room > 0 || phase != GenerationPhase::Moving;
        let eta_secs = if handed_over {
            Some(0)
        } else {
            self.eta(1.0 - remaining_share, held_up)
        };
        Some(TransferReport {
            handed_over,
            items_total: status.items_total,
            items_done: status.items_done,
            ranges_total: status.ranges_total,
            ranges_done: status.ranges_done,
            keys_scanned: status.keys_scanned,
            keys_moved: status.keys_moved,
            bytes_moved: status.bytes_moved,
            remaining_bytes,
            eta_secs,
            waiting: status.waiting,
            errors: status.errors,
        })
    }

    /// The share, by length on the ring, of the ranges this node owned by
    /// the earlier generation that it has not handed over: where the keys
    /// it still has to go through are.
    fn remaining_share(&self, transition: &PoolTransition, done: &HashSet<u64>) -> f64 {
        let pool_id = transition.origin().pool_id;
        let local = transition.local();
        let (mut owned, mut left) = (0u128, 0u128);
        for range in transition.ranges().snapshot() {
            let owns = self
                .router
                .generation_owners(&pool_id, ReplicationClass::Data, range.upper_inclusive)
                .is_some_and(|owners| owners.old.contains(&local));
            if !owns {
                continue;
            }
            let length = arc_length(&range);
            owned += length;
            if !done.contains(&range.upper_inclusive) {
                left += length;
            }
        }
        if owned == 0 {
            0.0
        } else {
            left as f64 / owned as f64
        }
    }

    /// Seconds left at the pace since the move last ran unhindered, `None`
    /// until there is one. `done` is the share gone through.
    fn eta(&self, done: f64, held_up: bool) -> Option<u64> {
        let mut pace = self.pace.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if held_up {
            *pace = None;
            return None;
        }
        let start = *pace.get_or_insert(Pace {
            since: Instant::now(),
            done_then: done,
        });
        let elapsed = start.since.elapsed();
        let gone = done - start.done_then;
        if elapsed < Duration::from_secs(1) || gone <= 0.0 {
            return None;
        }
        let secs = elapsed.as_secs_f64() * (1.0 - done).max(0.0) / gone;
        Some(secs.ceil() as u64)
    }

    fn reset_pace(&self) {
        *self.pace.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = None;
    }

    /// Pauses the move of the pool's keys on every member for `reason`.
    ///
    /// # Errors
    ///
    /// The pool does not grow, or the pause could not be written through the
    /// quorums or applied here.
    pub async fn pause(
        &self,
        reason: Option<String>,
    ) -> Result<PoolGrowthReport, GrowthControlError> {
        let reason = reason
            .map(|reason| reason.trim().to_string())
            .filter(|reason| !reason.is_empty())
            .unwrap_or_else(|| DEFAULT_PAUSE_REASON.to_string());
        self.control(Some(reason)).await
    }

    /// Resumes the move of the pool's keys on every member.
    ///
    /// # Errors
    ///
    /// As [`Self::pause`].
    pub async fn resume(&self) -> Result<PoolGrowthReport, GrowthControlError> {
        self.control(None).await
    }

    async fn control(
        &self,
        paused: Option<String>,
    ) -> Result<PoolGrowthReport, GrowthControlError> {
        let transition = self
            .transition
            .as_ref()
            .filter(|transition| transition.phase() != GenerationPhase::Finished)
            .ok_or_else(|| {
                GrowthControlError::NotGrowing(
                    "the pool does not grow: there is no move of its keys to pause or resume"
                        .into(),
                )
            })?;
        let origin = transition.origin();
        let record = PauseRecord::of(origin, paused.clone())
            .and_then(|record| record.encode())
            .map_err(|e| GrowthControlError::Unavailable(e.to_string()))?;
        self.records
            .write(
                &pool_generation::pause_key(origin.pool_id, transition.to_epoch()),
                record,
            )
            .await
            .map_err(|e| {
                GrowthControlError::Unavailable(format!(
                    "the pool's pause record could not be written through the quorums: {e}"
                ))
            })?;
        transition.apply_pause(paused.clone()).await.map_err(|e| {
            GrowthControlError::Unavailable(format!(
                "the pool's pause record was written, but not applied here yet: {e}"
            ))
        })?;
        match &paused {
            Some(reason) => info!(%reason, "Pool growth paused through the admin API"),
            None => info!("Pool growth resumed through the admin API"),
        }
        Ok(self.report())
    }

    /// The gauges of the growth now.
    fn gauges(&self) -> PoolGrowthGauges {
        let report = self.report();
        let transfer = report.transfer.as_ref();
        PoolGrowthGauges {
            epoch: report.generation.epoch,
            ranges: RangeCounts {
                pending: report.ranges.pending,
                streaming: report.ranges.streaming,
                verified: report.ranges.verified,
                cut_over: report.ranges.cut_over,
            },
            remaining_bytes: transfer.map_or(0, |transfer| transfer.remaining_bytes),
            paused: report.paused.is_some(),
            no_room: self
                .transition
                .as_ref()
                .and_then(|transition| transition.rebalance())
                .is_some_and(|handle| handle.status().waiting_for_room > 0)
                && report.state == GrowthState::Moving,
        }
    }

    /// Sets the gauges of the growth now.
    pub fn publish_metrics(&self) {
        set_pool_growth(&self.pool, &self.gauges());
    }

    /// Keeps the gauges current every `period` until `shutdown`.
    pub async fn run_metrics(
        self: Arc<Self>,
        period: Duration,
        mut shutdown: watch::Receiver<bool>,
    ) {
        loop {
            self.publish_metrics();
            tokio::select! {
                _ = tokio::time::sleep(period) => {}
                changed = shutdown.changed() => if changed.is_err() || *shutdown.borrow() { return; },
            }
        }
    }
}

fn summary(record: &PoolGenerationRecord) -> GenerationSummary {
    GenerationSummary {
        epoch: record.current.epoch,
        nodes: record.current.nodes.len(),
    }
}

/// The ring positions `range` covers; an arc whose endpoints are equal is
/// the whole ring.
fn arc_length(range: &GenerationRange) -> u128 {
    if range.lower_exclusive == range.upper_inclusive {
        u128::from(u64::MAX) + 1
    } else {
        u128::from(range.upper_inclusive.wrapping_sub(range.lower_exclusive))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::placement::RangeState;

    fn range(lower: u64, upper: u64) -> GenerationRange {
        GenerationRange {
            lower_exclusive: lower,
            upper_inclusive: upper,
            state: RangeState::Pending,
        }
    }

    #[test]
    fn an_arc_is_as_long_as_the_positions_it_covers() {
        assert_eq!(arc_length(&range(10, 20)), 10);
        // Wraps through zero: 5 positions to the end of the ring, then 0..=3.
        assert_eq!(arc_length(&range(u64::MAX - 5, 3)), 9);
        assert_eq!(arc_length(&range(7, 7)), u128::from(u64::MAX) + 1);
    }
}
