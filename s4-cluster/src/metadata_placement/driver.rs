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

//! The driver of the stages: one node's way through the move (plan
//! BUCKET-METADATA-REPLICATION.md, decisions 4 and 5).
//!
//! The driver watches its pool on a [`PlacementBoard`] and takes the next
//! stage whenever its gate opens ([`next_stage`]): in the node's
//! [`StageCell`] first, where the router and the replicas read it, then —
//! once the writes of the previous stage ended — in `cluster_meta`, then on
//! the board. At stage 1, once every node of the pool announced stage 1, it
//! runs backfill passes ([`BackfillPass`]) until one is complete, and stores
//! and announces that its backfill finished: the gate of stage 2 waits for
//! that from every node.
//!
//! The board is gossip in a running node; tests put one of their own in its
//! place. Where the driver stands goes to a [`PlacementStatus`], which the
//! metrics and the admin API read; a gate that holds longer than
//! [`HOLD_WARN_INTERVAL`] is warned about once every interval.

use std::sync::Arc;
use std::time::Duration;

use s4_core::storage::IndexDb;
use tokio::sync::watch;
use tracing::{debug, info, warn};

use crate::cluster_metrics;
use crate::gossip::GossipHandle;
use crate::identity::NodeId;
use crate::metadata_placement::backfill::BackfillPass;
use crate::metadata_placement::cell::StageCell;
use crate::metadata_placement::gate::{
    next_stage, GateVerdict, Holdout, HoldoutReason, PlacementView,
};
use crate::metadata_placement::stage::{PlacementRecord, Stage};
use crate::metadata_placement::status::{PlacementStatus, HOLD_WARN_INTERVAL};
use crate::metadata_placement::store;

/// How often the driver reads the board for the gate.
pub const GATE_INTERVAL: Duration = Duration::from_secs(1);

/// How long the driver waits after a backfill pass that was not complete.
pub const BACKFILL_RETRY: Duration = Duration::from_secs(10);

/// Where a node reads what the other nodes of its pool announced, and
/// announces its own record.
pub trait PlacementBoard: PlacementView + Send + Sync {
    /// Announces `record` as this node's.
    fn announce(&self, record: PlacementRecord);
}

impl PlacementBoard for GossipHandle {
    fn announce(&self, record: PlacementRecord) {
        self.announce_bucket_metadata_placement(record);
    }
}

/// The holdouts of the backfill of `local`: the nodes of `pool` that have
/// not announced stage 1 of `record`'s move or later, or whose protocol and
/// state the gate counts against; empty when the backfill may run. No node
/// writes bucket metadata by the old layout alone once every node announced
/// stage 1, since a node announces a stage only after the writes of the
/// previous one ended.
pub fn backfill_holdouts(
    pool: &[NodeId],
    local: NodeId,
    record: &PlacementRecord,
    local_protocol: u32,
    min_protocol: u32,
    view: &dyn PlacementView,
) -> Vec<Holdout> {
    if record.stage != Stage::DualWrite {
        return Vec::new();
    }
    // The gate of stage 2, with this node's backfill counted as done and the
    // others' left out, holds exactly on the nodes not at stage 1 yet.
    let done = PlacementRecord {
        backfill_complete: true,
        ..*record
    };
    match next_stage(pool, local, &done, local_protocol, min_protocol, view) {
        GateVerdict::Settled | GateVerdict::Advance(_) => Vec::new(),
        GateVerdict::Hold(holdouts) => holdouts
            .into_iter()
            .filter(|holdout| holdout.reason != HoldoutReason::BackfillPending)
            .collect(),
    }
}

/// The driver of one node.
pub struct StageDriver {
    pool: Vec<NodeId>,
    local: NodeId,
    record: PlacementRecord,
    stage_cell: Option<StageCell>,
    board: Arc<dyn PlacementBoard>,
    index_db: Arc<IndexDb>,
    backfill: Option<Arc<dyn BackfillPass>>,
    local_protocol: u32,
    min_protocol: u32,
    gate_interval: Duration,
    backfill_retry: Duration,
    status: Option<Arc<PlacementStatus>>,
}

impl StageDriver {
    /// The driver of `local` in `pool` from `record`, which it stores in
    /// `index_db` and announces on `board`. `stage_cell` is the stage the
    /// router and the replicas read, `None` for a pool with nothing to move;
    /// `backfill` brings this node's bucket metadata to the new replicas.
    /// The node speaks `local_protocol`, the move needs `min_protocol`.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        pool: Vec<NodeId>,
        local: NodeId,
        record: PlacementRecord,
        stage_cell: Option<StageCell>,
        board: Arc<dyn PlacementBoard>,
        index_db: Arc<IndexDb>,
        backfill: Option<Arc<dyn BackfillPass>>,
        local_protocol: u32,
        min_protocol: u32,
    ) -> Self {
        Self {
            pool,
            local,
            record,
            stage_cell,
            board,
            index_db,
            backfill,
            local_protocol,
            min_protocol,
            gate_interval: GATE_INTERVAL,
            backfill_retry: BACKFILL_RETRY,
            status: None,
        }
    }

    /// The same driver, writing where it stands to `status`.
    pub fn with_status(mut self, status: Arc<PlacementStatus>) -> Self {
        self.status = Some(status);
        self
    }

    /// The same driver, reading the board every `gate` and waiting
    /// `backfill_retry` after a pass that was not complete.
    pub fn with_intervals(mut self, gate: Duration, backfill_retry: Duration) -> Self {
        self.gate_interval = gate;
        self.backfill_retry = backfill_retry;
        self
    }

    /// Takes the stages one after another until the move settled; stops
    /// early when `shutdown` turns true.
    pub async fn run(mut self, mut shutdown: watch::Receiver<bool>) {
        let mut last_hold: Vec<Holdout> = Vec::new();
        let mut next_backfill = tokio::time::Instant::now();
        loop {
            if self.record.stage == Stage::DualWrite
                && !self.record.backfill_complete
                && tokio::time::Instant::now() >= next_backfill
                && backfill_holdouts(
                    &self.pool,
                    self.local,
                    &self.record,
                    self.local_protocol,
                    self.min_protocol,
                    self.board.as_ref(),
                )
                .is_empty()
            {
                if let Some(backfill) = self.backfill.clone() {
                    let report = tokio::select! {
                        report = backfill.pass() => report,
                        changed = shutdown.changed() => {
                            if changed.is_err() || *shutdown.borrow() {
                                return;
                            }
                            continue;
                        }
                    };
                    cluster_metrics::record_backfill_pass(&report);
                    if report.complete() {
                        let done = PlacementRecord {
                            backfill_complete: true,
                            ..self.record
                        };
                        if !self.store(&done, &mut shutdown).await {
                            return;
                        }
                        self.record = done;
                        self.note_record();
                        self.board.announce(done);
                        info!(
                            copied = report.copied,
                            already = report.already,
                            unconfirmed = report.unconfirmed,
                            dropped = report.dropped,
                            "The backfill of the bucket metadata finished on this node"
                        );
                    } else {
                        next_backfill = tokio::time::Instant::now() + self.backfill_retry;
                        info!(
                            copied = report.copied,
                            already = report.already,
                            unconfirmed = report.unconfirmed,
                            failed = report.failed,
                            dropped = report.dropped,
                            first_failure = ?report.first_failure,
                            retry_secs = self.backfill_retry.as_secs(),
                            "A backfill pass of the bucket metadata did not settle every key; \
                             another one follows"
                        );
                    }
                }
            }
            match next_stage(
                &self.pool,
                self.local,
                &self.record,
                self.local_protocol,
                self.min_protocol,
                self.board.as_ref(),
            ) {
                GateVerdict::Settled => {
                    self.note_record();
                    return;
                }
                GateVerdict::Advance(stage) => {
                    let Some(next) = self.record.advanced() else {
                        return;
                    };
                    // The writes that begin from now on are placed by the new
                    // stage; the ones placed by the previous stage end first.
                    if let Some(cell) = &self.stage_cell {
                        cell.advance_to(stage).await;
                    }
                    // The node is at the new stage now, whatever the disk
                    // says: it is stored until that holds, and only then
                    // announced.
                    if !self.store(&next, &mut shutdown).await {
                        return;
                    }
                    self.record = next;
                    self.note_record();
                    self.board.announce(next);
                    last_hold.clear();
                    info!(
                        from = %next.from,
                        to = %next.to,
                        stage = %stage,
                        "Every node of the pool is ready: the bucket metadata takes the next stage"
                    );
                    continue;
                }
                GateVerdict::Hold(holdouts) => {
                    self.note_hold(&holdouts);
                    if holdouts != last_hold {
                        self.report_hold(&holdouts);
                        last_hold = holdouts;
                    }
                }
            }
            tokio::select! {
                _ = tokio::time::sleep(self.gate_interval) => {}
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return;
                    }
                }
            }
        }
    }

    /// Stores `record`, trying again until it holds; `false` when shutdown
    /// came first.
    async fn store(&self, record: &PlacementRecord, shutdown: &mut watch::Receiver<bool>) -> bool {
        loop {
            match store::store(&self.index_db, record).await {
                Ok(()) => return true,
                Err(e) => warn!(
                    error = %e,
                    stage = %record.stage,
                    "could not store the bucket metadata placement; tried again"
                ),
            }
            tokio::select! {
                _ = tokio::time::sleep(self.gate_interval) => {}
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return false;
                    }
                }
            }
        }
    }

    fn note_record(&self) {
        if let Some(status) = &self.status {
            status.set_record(self.record);
        }
    }

    /// Writes the hold to the status, and warns once every
    /// [`HOLD_WARN_INTERVAL`] it lasts.
    fn note_hold(&self, holdouts: &[Holdout]) {
        let Some(status) = &self.status else {
            return;
        };
        let Some(held_for) = status.set_hold(holdouts.to_vec(), std::time::Instant::now()) else {
            return;
        };
        let named: Vec<String> = holdouts
            .iter()
            .map(|holdout| format!("{}: {}", holdout.node, holdout.reason))
            .collect();
        warn!(
            stage = %self.record.stage,
            from = %self.record.from,
            to = %self.record.to,
            held_for_mins = held_for.as_secs() / 60,
            held_by = ?named,
            warn_every_mins = HOLD_WARN_INTERVAL.as_secs() / 60,
            "The move of the bucket metadata waits for nodes of the pool: their bucket records \
             stay on fewer replicas than the pool survives failures for until they take part"
        );
    }

    fn report_hold(&self, holdouts: &[Holdout]) {
        let named: Vec<String> = holdouts
            .iter()
            .map(|holdout| format!("{}: {}", holdout.node, holdout.reason))
            .collect();
        let disagree = holdouts
            .iter()
            .any(|holdout| matches!(holdout.reason, HoldoutReason::OtherMove(_)));
        if disagree {
            warn!(
                from = %self.record.from,
                to = %self.record.to,
                held_by = ?named,
                "Nodes of the pool derived another target for the bucket metadata: the move \
                 waits until every node derives the same \
                 (S4_BUCKET_METADATA_REPLICATION_FACTOR must be the same on every node)"
            );
        } else {
            debug!(
                stage = %self.record.stage,
                held_by = ?named,
                "The next stage of the bucket metadata waits"
            );
        }
    }
}
