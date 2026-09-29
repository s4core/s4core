// Copyright 2026 S4Core Team
//
// Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

//! What the pool still owes one slot owner, and how a rebuild of it reports.
//!
//! A machine that took over a dead node's identity holds every slot the pool
//! gives that identity and none of the shards behind them (decision EC2-145).
//! Ordinary repair puts them back, but until this module existed there was no
//! way to ask whether it had finished: the only visible signal was the repair
//! queue, and an empty queue is also what a rebuild that was never queued looks
//! like.
//!
//! So the answer here is counted from manifests and the shards themselves, not
//! from the queue. Two things are built on that count:
//!
//! * [`crate::ec_node_rebuild::EcNodeRebuildInventory`] — how many shards of
//!   one owner the pool considers missing right now. Zero, from a walk that
//!   reached the end and got an answer for every shard, is what "the rebuild is
//!   finished" means.
//! * [`crate::ec_node_rebuild::EcNodeRepairQueueOutcome`] — what one mass
//!   repair request queued and what it left behind, so a whole-node rebuild
//!   does not look finished after the first page of it.
//!
//! # Why a verdict can be absent
//!
//! Both types can answer "I cannot say", and that is deliberate. A walk that
//! stopped at `S4_EC_ADMIN_SCAN_LIMIT` counted the limit rather than the pool,
//! and a shard whose owner could not be asked was not found present — it was
//! not found at all. Reporting either as "nothing missing" would hand an
//! operator the one answer they act on, produced by the one case where it is
//! not known. It is the rule the dedup report already lives by (decision
//! EC2-121).
//!
//! Positive evidence still decides: one shard known to be missing means the
//! rebuild is not finished, whatever else the walk failed to see.

use s4_cluster::NodeId;
use serde_json::{json, Value};

/// Whether one shard of a rebuild target is where its manifest says it is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EcShardPresence {
    /// The owner holds the shard.
    Present,
    /// The owner does not hold the shard and said so.
    Missing,
    /// The owner could not be asked, so neither answer was given.
    Unknown {
        /// Why the shard could not be checked, in the words of the failure.
        reason: String,
    },
}

impl EcShardPresence {
    /// A shard that could not be checked, carrying why.
    pub fn unknown(reason: impl Into<String>) -> Self {
        Self::Unknown {
            reason: reason.into(),
        }
    }
}

/// Whether a rebuild of one node has finished, and on what evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EcRebuildVerdict {
    /// Every shard the pool gives this owner is in place.
    Complete,
    /// At least one shard is known to be missing.
    Incomplete,
    /// The walk could not see enough of the pool to answer.
    Unknown,
}

impl EcRebuildVerdict {
    /// The verdict as the admin surface renders it: `true`, `false` or `null`.
    pub fn as_json(self) -> Value {
        match self {
            Self::Complete => json!(true),
            Self::Incomplete => json!(false),
            Self::Unknown => Value::Null,
        }
    }

    /// Stable label value used by the rebuild gauge.
    ///
    /// A gauge has no way to be absent, so the third answer that JSON renders
    /// as `null` becomes a label of its own here rather than a missing series.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Complete => "complete",
            Self::Incomplete => "incomplete",
            Self::Unknown => "unknown",
        }
    }
}

/// Every verdict label, so the gauge can publish zero on the verdicts that do
/// not hold rather than leaving them absent with their last value.
pub const REBUILD_VERDICT_LABELS: [&str; 3] = ["complete", "incomplete", "unknown"];

/// Which piece of evidence produced the verdict.
///
/// Kept beside the verdict rather than folded into it, because "nothing is
/// missing" and "nothing could be asked" are the same three characters in a
/// dashboard and entirely different states of a pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EcRebuildVerdictReason {
    /// Shards of this owner are known to be missing.
    ShardsMissing,
    /// The manifest walk stopped at the traversal limit.
    ScanTruncated,
    /// At least one shard's owner could not be asked.
    ShardsUnanswered,
    /// The walk finished and this owner holds no shard of the pool.
    NoShardsOwned,
    /// The walk finished and every shard of this owner is in place.
    AllShardsPresent,
}

impl EcRebuildVerdictReason {
    /// Stable name used in admin responses and in dashboards.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ShardsMissing => "shards_missing",
            Self::ScanTruncated => "scan_truncated",
            Self::ShardsUnanswered => "shards_unanswered",
            Self::NoShardsOwned => "no_shards_owned",
            Self::AllShardsPresent => "all_shards_present",
        }
    }
}

/// What the pool still owes one slot owner, counted from its manifests.
///
/// Filled in by walking manifests: every manifest seen is noted once, and every
/// shard it assigns to the owner is checked and noted with what the check
/// found. The walk itself lives in the EC operations service, which is the only
/// place that can reach both the manifest store and the owner; this type holds
/// nothing but the tally and the rules that read it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EcNodeRebuildInventory {
    node_id: NodeId,
    slots: Vec<u8>,
    manifests_scanned: usize,
    shards_expected: usize,
    shards_present: usize,
    shards_missing: usize,
    shards_unknown: usize,
    first_unknown_reason: Option<String>,
    truncated: bool,
}

impl EcNodeRebuildInventory {
    /// An empty tally for `node_id`.
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            slots: Vec::new(),
            manifests_scanned: 0,
            shards_expected: 0,
            shards_present: 0,
            shards_missing: 0,
            shards_unknown: 0,
            first_unknown_reason: None,
            truncated: false,
        }
    }

    /// Record the slots the pool's layout gives this identity.
    ///
    /// Read from the layout rather than from the manifests, so an owner that
    /// holds slots and has no object in them yet is still recognisable as an
    /// owner rather than as a node the pool never heard of.
    pub fn with_slots(mut self, slots: Vec<u8>) -> Self {
        self.slots = slots;
        self
    }

    /// Count one manifest as read.
    pub fn note_manifest(&mut self) {
        self.manifests_scanned = self.manifests_scanned.saturating_add(1);
    }

    /// Count one shard of this owner and what checking it found.
    pub fn note_shard(&mut self, presence: EcShardPresence) {
        self.shards_expected = self.shards_expected.saturating_add(1);
        match presence {
            EcShardPresence::Present => {
                self.shards_present = self.shards_present.saturating_add(1);
            }
            EcShardPresence::Missing => {
                self.shards_missing = self.shards_missing.saturating_add(1);
            }
            EcShardPresence::Unknown { reason } => {
                self.shards_unknown = self.shards_unknown.saturating_add(1);
                if self.first_unknown_reason.is_none() {
                    self.first_unknown_reason = Some(reason);
                }
            }
        }
    }

    /// Record that the manifest walk stopped before the end of the pool.
    pub fn mark_truncated(&mut self) {
        self.truncated = true;
    }

    /// How many manifests the walk read.
    pub fn manifests_scanned(&self) -> usize {
        self.manifests_scanned
    }

    /// Shards of this owner the pool considers missing right now.
    pub fn shards_missing(&self) -> usize {
        self.shards_missing
    }

    /// Whether the rebuild of this owner has finished.
    pub fn verdict(&self) -> EcRebuildVerdict {
        match self.verdict_reason() {
            EcRebuildVerdictReason::ShardsMissing => EcRebuildVerdict::Incomplete,
            EcRebuildVerdictReason::ScanTruncated | EcRebuildVerdictReason::ShardsUnanswered => {
                EcRebuildVerdict::Unknown
            }
            EcRebuildVerdictReason::NoShardsOwned | EcRebuildVerdictReason::AllShardsPresent => {
                EcRebuildVerdict::Complete
            }
        }
    }

    /// Which piece of evidence the verdict rests on.
    ///
    /// The order is the whole of the rule. A shard known to be missing settles
    /// the question on its own, because an incomplete walk cannot make a shard
    /// that is gone come back. Only when there is no such evidence does what
    /// the walk failed to see start to matter.
    pub fn verdict_reason(&self) -> EcRebuildVerdictReason {
        if self.shards_missing > 0 {
            return EcRebuildVerdictReason::ShardsMissing;
        }
        if self.truncated {
            return EcRebuildVerdictReason::ScanTruncated;
        }
        if self.shards_unknown > 0 {
            return EcRebuildVerdictReason::ShardsUnanswered;
        }
        if self.shards_expected == 0 {
            return EcRebuildVerdictReason::NoShardsOwned;
        }
        EcRebuildVerdictReason::AllShardsPresent
    }

    /// The inventory as the admin surface renders it.
    pub fn admin_snapshot(&self) -> Value {
        json!({
            "node_id": self.node_id.to_string(),
            "slots": self.slots,
            "manifests_scanned": self.manifests_scanned,
            // A walk that stopped at the traversal limit counted the limit, not
            // the pool. Standing next to the counts, it says which of the two
            // the numbers below describe.
            "scan_truncated": self.truncated,
            "shards_expected": self.shards_expected,
            "shards_present": self.shards_present,
            "shards_missing": self.shards_missing,
            "shards_unknown": self.shards_unknown,
            "unknown_reason": self.first_unknown_reason,
            // `null` when the walk could not answer. Zero missing shards and no
            // answer look alike only if the verdict is forced into a boolean.
            "complete": self.verdict().as_json(),
            "verdict_reason": self.verdict_reason().as_str(),
        })
    }
}

/// Why a mass repair request stopped queueing.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum EcRebuildQueueStop {
    /// Nothing capped it: the walk reached the end of the pool's manifests.
    #[default]
    End,
    /// The request's own `max_tasks` was reached.
    MaxTasks,
    /// The walk stopped at `S4_EC_ADMIN_SCAN_LIMIT`.
    ScanLimit,
    /// The durable repair queue is full.
    QueueFull,
}

impl EcRebuildQueueStop {
    /// Stable name used in admin responses.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::End => "end",
            Self::MaxTasks => "max_tasks",
            Self::ScanLimit => "scan_limit",
            Self::QueueFull => "queue_full",
        }
    }
}

/// What one mass repair request did, and what it left for the next one.
///
/// `remaining` is the field this type exists for. Without it a request that
/// queued its full `max_tasks` is indistinguishable from one that queued the
/// last shard of the pool, and a whole-node rebuild looks finished after its
/// first page.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EcNodeRepairQueueOutcome {
    newly_queued: usize,
    already_queued: usize,
    remaining: usize,
    manifests_scanned: usize,
    stopped_at: EcRebuildQueueStop,
    truncated: bool,
}

impl EcNodeRepairQueueOutcome {
    /// Count one shard this request queued.
    ///
    /// `created` tells a task this call put in the queue from one that was
    /// already there for the same shard: repeating the request is how an
    /// operator drives a rebuild to the end, and it must not read as progress.
    pub fn note_queued(&mut self, created: bool) {
        if created {
            self.newly_queued = self.newly_queued.saturating_add(1);
        } else {
            self.already_queued = self.already_queued.saturating_add(1);
        }
    }

    /// Count one shard that needs repair and that this request did not queue.
    pub fn note_remaining(&mut self) {
        self.remaining = self.remaining.saturating_add(1);
    }

    /// Count one manifest as read.
    pub fn note_manifest(&mut self) {
        self.manifests_scanned = self.manifests_scanned.saturating_add(1);
    }

    /// Record why the request stopped queueing.
    ///
    /// The first cap to bite is the answer and later ones do not replace it: a
    /// walk that ran out of scan budget after the task limit had already
    /// stopped it says nothing about why queueing ended.
    pub fn stop_at(&mut self, stop: EcRebuildQueueStop) {
        if self.stopped_at == EcRebuildQueueStop::End {
            self.stopped_at = stop;
        }
    }

    /// Record that the manifest walk stopped before the end of the pool.
    ///
    /// Kept apart from [`Self::stop_at`] because the two make different claims:
    /// a request that filled `max_tasks` and then read the pool to the end
    /// knows exactly what it left, while a truncated walk only knows a floor.
    pub fn mark_truncated(&mut self) {
        self.truncated = true;
    }

    /// Where the request stopped queueing.
    pub fn stopped_at(&self) -> EcRebuildQueueStop {
        self.stopped_at
    }

    /// How many manifests the walk read.
    pub fn manifests_scanned(&self) -> usize {
        self.manifests_scanned
    }

    /// Shards left for a following request.
    pub fn remaining(&self) -> usize {
        self.remaining
    }

    /// The outcome as the admin surface renders it.
    pub fn admin_snapshot(&self) -> Value {
        json!({
            "newly_queued": self.newly_queued,
            "already_queued": self.already_queued,
            // Counted as far as the walk went, so `scan_truncated` below decides
            // whether this is the total or a floor.
            "remaining": self.remaining,
            "manifests_scanned": self.manifests_scanned,
            "stopped_at": self.stopped_at.as_str(),
            "scan_truncated": self.truncated,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node() -> NodeId {
        NodeId(7)
    }

    /// The state a replacement is in the moment it takes the identity: the
    /// pool gives it slots and it holds nothing behind them.
    #[test]
    fn a_missing_shard_settles_the_verdict_on_its_own() {
        let mut inventory = EcNodeRebuildInventory::new(node()).with_slots(vec![3]);
        inventory.note_manifest();
        inventory.note_shard(EcShardPresence::Missing);
        inventory.note_shard(EcShardPresence::Present);

        assert_eq!(inventory.verdict(), EcRebuildVerdict::Incomplete);
        assert_eq!(
            inventory.verdict_reason(),
            EcRebuildVerdictReason::ShardsMissing
        );
        assert_eq!(inventory.shards_missing(), 1);
        assert_eq!(inventory.admin_snapshot()["complete"], json!(false));
    }

    /// Evidence of loss outranks an incomplete walk: an unread page cannot put
    /// a shard back.
    #[test]
    fn a_truncated_walk_that_found_a_loss_still_says_incomplete() {
        let mut inventory = EcNodeRebuildInventory::new(node());
        inventory.note_manifest();
        inventory.note_shard(EcShardPresence::Missing);
        inventory.mark_truncated();

        assert_eq!(inventory.verdict(), EcRebuildVerdict::Incomplete);
        assert_eq!(
            inventory.verdict_reason(),
            EcRebuildVerdictReason::ShardsMissing
        );
    }

    /// The expensive wrong answer: a walk that stopped at the traversal limit
    /// found nothing missing in what it read, and that is not the pool.
    #[test]
    fn a_truncated_walk_refuses_to_call_a_rebuild_finished() {
        let mut inventory = EcNodeRebuildInventory::new(node());
        inventory.note_manifest();
        inventory.note_shard(EcShardPresence::Present);
        inventory.mark_truncated();

        assert_eq!(inventory.verdict(), EcRebuildVerdict::Unknown);
        assert_eq!(
            inventory.verdict_reason(),
            EcRebuildVerdictReason::ScanTruncated
        );
        let snapshot = inventory.admin_snapshot();
        assert_eq!(snapshot["complete"], Value::Null);
        assert_eq!(snapshot["scan_truncated"], json!(true));
    }

    /// A shard whose owner did not answer was not found present. The reason
    /// travels with the count so the operator sees which owner went quiet.
    #[test]
    fn a_shard_that_could_not_be_checked_withholds_the_verdict() {
        let mut inventory = EcNodeRebuildInventory::new(node());
        inventory.note_manifest();
        inventory.note_shard(EcShardPresence::Present);
        inventory.note_shard(EcShardPresence::unknown("node 7 did not answer"));

        assert_eq!(inventory.verdict(), EcRebuildVerdict::Unknown);
        assert_eq!(
            inventory.verdict_reason(),
            EcRebuildVerdictReason::ShardsUnanswered
        );
        assert_eq!(
            inventory.admin_snapshot()["unknown_reason"],
            json!("node 7 did not answer")
        );
    }

    /// The finish line: a walk that reached the end and found every shard.
    #[test]
    fn a_complete_walk_with_every_shard_in_place_says_finished() {
        let mut inventory = EcNodeRebuildInventory::new(node()).with_slots(vec![3, 4]);
        for _ in 0..4 {
            inventory.note_manifest();
            inventory.note_shard(EcShardPresence::Present);
        }

        assert_eq!(inventory.verdict(), EcRebuildVerdict::Complete);
        assert_eq!(
            inventory.verdict_reason(),
            EcRebuildVerdictReason::AllShardsPresent
        );
        let snapshot = inventory.admin_snapshot();
        assert_eq!(snapshot["complete"], json!(true));
        assert_eq!(snapshot["shards_expected"], json!(4));
        assert_eq!(snapshot["manifests_scanned"], json!(4));
        assert_eq!(snapshot["slots"], json!([3, 4]));
    }

    /// A node the pool holds no object for owes nothing, and saying so with a
    /// distinct reason keeps it apart from a node whose shards are all there.
    #[test]
    fn an_owner_with_no_shards_in_the_pool_owes_nothing() {
        let mut inventory = EcNodeRebuildInventory::new(node());
        inventory.note_manifest();

        assert_eq!(inventory.verdict(), EcRebuildVerdict::Complete);
        assert_eq!(
            inventory.verdict_reason(),
            EcRebuildVerdictReason::NoShardsOwned
        );
    }

    /// Repeating a mass request is the supported way to drive a rebuild to the
    /// end, so the tasks it finds already queued must not read as new work.
    #[test]
    fn a_repeated_request_reports_what_it_found_rather_than_what_it_made() {
        let mut outcome = EcNodeRepairQueueOutcome::default();
        outcome.note_manifest();
        outcome.note_queued(true);
        outcome.note_queued(false);
        outcome.note_queued(false);

        let snapshot = outcome.admin_snapshot();
        assert_eq!(snapshot["newly_queued"], json!(1));
        assert_eq!(snapshot["already_queued"], json!(2));
        assert_eq!(snapshot["remaining"], json!(0));
        assert_eq!(snapshot["stopped_at"], json!("end"));
        assert_eq!(snapshot["scan_truncated"], json!(false));
    }

    /// The reason this type exists: a request that filled its own `max_tasks`
    /// has to look different from one that finished the pool.
    #[test]
    fn a_request_that_hit_its_task_limit_says_what_it_left() {
        let mut outcome = EcNodeRepairQueueOutcome::default();
        outcome.note_queued(true);
        outcome.note_remaining();
        outcome.note_remaining();
        outcome.stop_at(EcRebuildQueueStop::MaxTasks);

        let snapshot = outcome.admin_snapshot();
        assert_eq!(snapshot["remaining"], json!(2));
        assert_eq!(snapshot["stopped_at"], json!("max_tasks"));
        // The walk read the pool to the end, so the remainder is exact.
        assert_eq!(snapshot["scan_truncated"], json!(false));
        assert_eq!(outcome.remaining(), 2);
        assert_eq!(outcome.stopped_at(), EcRebuildQueueStop::MaxTasks);
    }

    /// Once a cap has stopped queueing, running out of scan budget afterwards
    /// does not rewrite why it stopped — but it does turn the remainder into a
    /// floor, and those are two separate statements.
    #[test]
    fn the_first_cap_to_bite_is_the_one_reported() {
        let mut outcome = EcNodeRepairQueueOutcome::default();
        outcome.stop_at(EcRebuildQueueStop::QueueFull);
        outcome.stop_at(EcRebuildQueueStop::MaxTasks);
        outcome.mark_truncated();

        let snapshot = outcome.admin_snapshot();
        assert_eq!(snapshot["stopped_at"], json!("queue_full"));
        assert_eq!(snapshot["scan_truncated"], json!(true));
    }
}
