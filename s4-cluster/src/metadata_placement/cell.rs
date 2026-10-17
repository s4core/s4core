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

//! The stage this node places bucket metadata by, and the writes begun at
//! each stage (plan BUCKET-METADATA-REPLICATION.md, decision 4, task
//! BMR-03-002).
//!
//! The gate lets a node take stage `s + 1` once every node announced `s`.
//! That says no node writes by the rules of `s − 1` any more only if a node
//! announces `s` after its last write by the rules of `s − 1` ended. So every
//! write of bucket metadata holds a [`StageTicket`] of the stage it was routed
//! by until its quorum is decided, and a change of stage
//! ([`StageCell::advance_to`]) returns only once no ticket of the previous
//! stage is left. Routing is synchronous and never waits: a write that takes
//! its ticket while the stage changes takes it again at the new stage.

use std::sync::atomic::{AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;

use tokio::sync::Notify;

use crate::clock::hlc::{now_millis, HlcTimestamp};
use crate::metadata_placement::stage::Stage;

/// This node's stage, shared by the router, the replica handlers and the
/// driver of the stages.
#[derive(Debug, Clone)]
pub struct StageCell(Arc<Inner>);

#[derive(Debug)]
struct Inner {
    stage: AtomicU8,
    /// When this process took stage 3, in Unix milliseconds; 0 before.
    settled_at: AtomicU64,
    /// Writes begun at each stage and not ended.
    writes: [AtomicUsize; 4],
    /// Woken whenever the last write of a stage ends.
    drained: Notify,
}

impl StageCell {
    /// A node at `stage`.
    pub fn new(stage: Stage) -> Self {
        Self(Arc::new(Inner {
            stage: AtomicU8::new(stage.number()),
            settled_at: AtomicU64::new(if stage == Stage::Settled {
                now_millis()
            } else {
                0
            }),
            writes: Default::default(),
            drained: Notify::new(),
        }))
    }

    /// The stage now.
    pub fn stage(&self) -> Stage {
        Stage::from_number(self.0.stage.load(Ordering::SeqCst)).unwrap_or(Stage::Legacy)
    }

    /// When this node took stage 3 — at the start of this process, if it
    /// started there; `None` before stage 3. The anti-entropy rounds this
    /// node begins after it compare the bucket metadata on their new
    /// replicas; the cuts of earlier rounds say nothing of them.
    pub fn settled_since(&self) -> Option<HlcTimestamp> {
        let at = self.0.settled_at.load(Ordering::SeqCst);
        (at != 0 && self.stage() == Stage::Settled).then(|| HlcTimestamp::from_record(at, 0, 0))
    }

    /// A ticket for one write, at the stage now; held until the write's
    /// quorum is decided.
    pub fn enter(&self) -> StageTicket {
        loop {
            let stage = self.stage();
            self.0.writes[usize::from(stage.number())].fetch_add(1, Ordering::SeqCst);
            // The change stores the stage before it counts the writes of the
            // previous one: a write counted at a stage the change already
            // left is seen here, and taken again at the new stage.
            if self.stage() == stage {
                return StageTicket {
                    cell: self.clone(),
                    stage,
                };
            }
            self.leave(stage);
        }
    }

    /// Writes begun at `stage` and not ended.
    pub fn writes_at(&self, stage: Stage) -> usize {
        self.0.writes[usize::from(stage.number())].load(Ordering::SeqCst)
    }

    /// Takes `next`, the stage after the current one, and returns once every
    /// write begun at the current one ended. A `next` that is not the stage
    /// after the current one changes nothing.
    pub async fn advance_to(&self, next: Stage) {
        let current = self.stage();
        if current.next() != Some(next) {
            return;
        }
        if next == Stage::Settled {
            self.0.settled_at.store(now_millis().max(1), Ordering::SeqCst);
        }
        self.0.stage.store(next.number(), Ordering::SeqCst);
        loop {
            let drained = self.0.drained.notified();
            tokio::pin!(drained);
            drained.as_mut().enable();
            if self.writes_at(current) == 0 {
                return;
            }
            drained.await;
        }
    }

    fn leave(&self, stage: Stage) {
        let left = self.0.writes[usize::from(stage.number())].fetch_sub(1, Ordering::SeqCst);
        if left == 1 {
            self.0.drained.notify_waiters();
        }
    }
}

/// One write of bucket metadata begun at a stage; the write ends when the
/// ticket is dropped.
#[derive(Debug)]
pub struct StageTicket {
    cell: StageCell,
    stage: Stage,
}

impl StageTicket {
    /// The stage the write was routed by.
    pub fn stage(&self) -> Stage {
        self.stage
    }
}

impl Drop for StageTicket {
    fn drop(&mut self) {
        self.cell.leave(self.stage);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    /// A write begun at stage 0 keeps the change to stage 1 from returning
    /// until it ends; a write begun meanwhile is at stage 1 and does not.
    #[tokio::test]
    async fn a_change_of_stage_waits_for_the_writes_of_the_previous_one() {
        let cell = StageCell::new(Stage::Legacy);
        let early = cell.enter();
        assert_eq!(early.stage(), Stage::Legacy);
        let changing = tokio::spawn({
            let cell = cell.clone();
            async move { cell.advance_to(Stage::DualWrite).await }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(cell.stage(), Stage::DualWrite);
        let late = cell.enter();
        assert_eq!(late.stage(), Stage::DualWrite);
        assert!(
            !changing.is_finished(),
            "the stage 0 write is still running"
        );
        drop(early);
        tokio::time::timeout(Duration::from_secs(5), changing)
            .await
            .expect("the change returns once the stage 0 write ended")
            .unwrap();
        assert_eq!(cell.writes_at(Stage::Legacy), 0);
        assert_eq!(cell.writes_at(Stage::DualWrite), 1);
        drop(late);
        assert_eq!(cell.writes_at(Stage::DualWrite), 0);
    }

    #[tokio::test]
    async fn the_stage_moves_one_step_forward_only() {
        let cell = StageCell::new(Stage::DualWrite);
        cell.advance_to(Stage::Settled).await;
        assert_eq!(cell.stage(), Stage::DualWrite);
        cell.advance_to(Stage::Legacy).await;
        assert_eq!(cell.stage(), Stage::DualWrite);
        cell.advance_to(Stage::ReadNew).await;
        assert_eq!(cell.stage(), Stage::ReadNew);
    }

    /// Many writes racing a change: every one is counted at the stage it
    /// holds, and the change returns only once the earlier ones ended.
    #[tokio::test]
    async fn racing_writes_are_counted_at_the_stage_they_hold() {
        let cell = StageCell::new(Stage::Legacy);
        let writers: Vec<_> = (0..64)
            .map(|i| {
                let cell = cell.clone();
                tokio::spawn(async move {
                    let ticket = cell.enter();
                    tokio::time::sleep(Duration::from_millis(i % 7)).await;
                    ticket.stage()
                })
            })
            .collect();
        cell.advance_to(Stage::DualWrite).await;
        assert_eq!(cell.writes_at(Stage::Legacy), 0);
        for writer in writers {
            writer.await.unwrap();
        }
        assert_eq!(cell.writes_at(Stage::DualWrite), 0);
    }
}
