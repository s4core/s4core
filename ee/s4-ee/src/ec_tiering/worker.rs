// Copyright 2026 S4Core Team
//
// Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

//! The background worker that migrates cold RF=3 objects into erasure coding.
//!
//! The worker owns no I/O of its own. Everything it needs — the catalog, the
//! bucket policies, the facts about one object, the durable queue — arrives
//! through [`EcTieringEnvironment`], the same shape the staged transcode
//! pipeline uses for its shard store and its cutover. That keeps the whole
//! cycle, including the rate limits and the resume position, checkable without
//! a cluster.
//!
//! Three decisions from phase 0 shape the loop:
//!
//! - [EC2-020] makes the access mark node-local, so a verdict of "cold" is only
//!   this node's opinion. Before an object is queued the worker asks the pool
//!   for the newest mark any node holds, and only then commits to the decision.
//! - [EC2-022] limits admission rather than execution: the staged pipeline
//!   already has five limits on how fast it works, and none on how much work is
//!   handed to it. The cycle therefore caps both what it queues and how much
//!   tiering work may be outstanding at once.
//! - [EC2-025] restricts migration to the current version of a live object,
//!   which the decision engine enforces.
//!
//! [EC2-020]: `EC-v2/adr/EC2-020-heat-tracking.md`
//! [EC2-022]: `EC-v2/adr/EC2-022-tiering-rate-limiting.md`
//! [EC2-025]: `EC-v2/adr/EC2-025-tiering-eligibility.md`

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use s4_cluster::ClusterError;

use crate::ec_metadata::EcTieringCursorRecord;
use crate::ec_metrics;
use crate::ec_tiering::policy::{
    decide, TieringCandidate, TieringDecision, TieringPolicy, TieringSkipReason,
};

/// Objects requested from the catalog in one page.
const CATALOG_PAGE_SIZE: usize = 256;

/// One entry of the pool-wide catalog.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TieringCatalogEntry {
    /// Object key.
    pub key: String,
    /// Object size in bytes, as the listing reports it.
    pub size: u64,
}

/// One page of the pool-wide catalog of a bucket.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TieringCatalogPage {
    /// Entries in this page.
    pub entries: Vec<TieringCatalogEntry>,
    /// Token that continues the listing, absent when the bucket is exhausted.
    pub next_continuation: Option<String>,
}

/// What the pool as a whole knows about one object's activity.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PoolActivity {
    /// Newest access mark any node of the set holds.
    pub newest_access_nanos: u64,
    /// Reads counted across the set within the promotion window.
    ///
    /// A sum, not a maximum: each node counts only the reads it served itself,
    /// so a client spreading its reads over three nodes would look a third as
    /// busy as it is under any other rule.
    pub window_reads: u32,
}

/// Everything the worker needs from the rest of the node.
#[async_trait]
pub trait EcTieringEnvironment: Send + Sync + 'static {
    /// Buckets served by this pool, in a stable order.
    async fn list_buckets(&self) -> Result<Vec<String>, ClusterError>;

    /// One page of a bucket's catalog.
    async fn list_objects(
        &self,
        bucket: &str,
        continuation: Option<&str>,
        limit: usize,
    ) -> Result<TieringCatalogPage, ClusterError>;

    /// Settings in force for a bucket, after inheritance.
    async fn effective_policy(&self, bucket: &str) -> Result<TieringPolicy, ClusterError>;

    /// Facts about one object, or `None` when it is no longer there.
    ///
    /// The policy comes in because the read count depends on it: a bucket may
    /// narrow the promotion window, and the answer has to be counted over that
    /// window rather than over the node's.
    async fn candidate(
        &self,
        bucket: &str,
        key: &str,
        policy: &TieringPolicy,
    ) -> Result<Option<TieringCandidate>, ClusterError>;

    /// What the whole pool knows about this object's activity.
    ///
    /// Asked only about objects a local decision has already selected, because
    /// it costs a round trip per node. It can only make an object look busier
    /// than this node thinks it is, never quieter, so it can move a decision
    /// from "migrate" to "leave alone" and from "leave alone" to "bring back",
    /// and never the other way.
    async fn pool_activity(
        &self,
        candidate: &TieringCandidate,
        policy: &TieringPolicy,
    ) -> Result<PoolActivity, ClusterError>;

    /// Put an object in the staged transcode queue as a tiering migration.
    ///
    /// Returns `false` when a job for this exact object generation already
    /// existed, which is not a failure: the queue is idempotent, and a cycle
    /// that re-examines a page must not spend its budget on work already
    /// admitted.
    async fn enqueue_migration(&self, candidate: &TieringCandidate) -> Result<bool, ClusterError>;

    /// Put an object in the reverse migration queue.
    ///
    /// Returns `false` when a promotion for this object already exists, which
    /// is not a failure: the queue is keyed by object, and a cycle that
    /// re-examines a page must not spend its budget on work already admitted.
    async fn enqueue_promotion(&self, candidate: &TieringCandidate) -> Result<bool, ClusterError>;

    /// Reverse migrations that have not reached a terminal state.
    ///
    /// Counted separately from the forward queue on purpose. Sharing one
    /// ceiling would make the manual rollback impossible exactly when it is
    /// needed — with the forward queue full of the migrations being undone.
    async fn outstanding_promotions(&self, ceiling: usize) -> Result<usize, ClusterError>;

    /// Tiering-originated jobs that have not reached a terminal state.
    ///
    /// Counting stops at `ceiling`: the caller only needs to know how much
    /// headroom is left, and a pool with a large backlog should not pay for a
    /// full keyspace walk to be told it has none.
    async fn outstanding_migrations(&self, ceiling: usize) -> Result<usize, ClusterError>;

    /// Read this node's scan position.
    async fn load_cursor(&self) -> Result<Option<EcTieringCursorRecord>, ClusterError>;

    /// Store this node's scan position.
    async fn save_cursor(&self, cursor: &EcTieringCursorRecord) -> Result<(), ClusterError>;

    /// Whether every node in this pool is confirmed to support EC v2.
    ///
    /// Rolling-upgrade guard (EC2-TIER-05-004). A migration into erasure coding
    /// changes how an object is stored cluster-wide, so it must not start while
    /// any pool member still runs software that cannot follow it. Anything short
    /// of a confirmation — a node that has not joined membership, one whose
    /// metadata has not arrived, one reporting an older protocol — answers
    /// `false`.
    ///
    /// Asked once per cycle rather than once at startup: a node can be restarted
    /// into older software at any point during a rolling upgrade, and a verdict
    /// taken at boot would not notice.
    ///
    /// Only forward migration is gated. Reverse tiering runs regardless, because
    /// it moves objects back to RF=3 — a layout every version understands — and
    /// because it carries the operator's manual rollback, which has to work
    /// precisely when the cluster is in a state nobody planned.
    async fn cluster_tiering_supported(&self) -> Result<bool, ClusterError>;
}

/// Limits that bound one scan cycle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EcTieringWorkerConfig {
    /// Whether the worker runs at all.
    pub enabled: bool,
    /// Pause between two cycles.
    pub scan_interval: Duration,
    /// Objects one cycle may examine.
    pub scan_limit: usize,
    /// Objects one cycle may queue.
    pub enqueue_per_cycle: usize,
    /// Tiering jobs allowed to be in flight at once.
    pub max_queue_depth: usize,
    /// Objects one cycle may send back to RF=3.
    pub promote_per_cycle: usize,
    /// Reverse migrations allowed to be in flight at once.
    pub max_promotion_queue_depth: usize,
}

/// Why a cycle stopped before exhausting the catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EcTieringCycleStop {
    /// Every bucket was walked to the end.
    CatalogExhausted,
    /// The cycle examined as many objects as it is allowed to.
    ScanLimit,
    /// The cycle queued as many objects as it is allowed to.
    EnqueueLimit,
    /// Tiering work already in flight left no room to admit more.
    QueueFull,
    /// The cycle queued as many reverse migrations as it is allowed to.
    PromoteLimit,
    /// Tiering is switched off on this node.
    Disabled,
    /// Some node in the pool is not confirmed to support EC v2.
    MixedCluster,
}

/// What one cycle did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EcTieringCycleReport {
    /// Objects examined.
    pub scanned: usize,
    /// Objects queued for migration.
    pub queued: usize,
    /// Objects queued to come back to RF=3.
    pub promoted: usize,
    /// Objects left where they were.
    pub skipped: usize,
    /// Objects that disappeared between the listing and the decision.
    pub vanished: usize,
    /// Objects a peer's newer access mark rescued from migration.
    pub kept_warm_by_peer: usize,
    /// Bytes queued for migration.
    pub queued_bytes: u64,
    /// Bytes queued to come back to RF=3.
    pub promoted_bytes: u64,
    /// Reasons objects were skipped, by name, with counts.
    pub skip_reasons: BTreeMap<&'static str, usize>,
    /// Why the cycle ended.
    pub stop: Option<EcTieringCycleStop>,
}

impl EcTieringCycleReport {
    fn record_skip(&mut self, reason: &TieringSkipReason) {
        self.skipped = self.skipped.saturating_add(1);
        *self.skip_reasons.entry(skip_reason_name(reason)).or_insert(0) += 1;
    }
}

/// Stable name of a skip reason, for reports and metrics.
fn skip_reason_name(reason: &TieringSkipReason) -> &'static str {
    match reason {
        TieringSkipReason::Disabled => "disabled",
        TieringSkipReason::NotCurrentVersion => "not_current_version",
        TieringSkipReason::DeleteMarker => "delete_marker",
        TieringSkipReason::AlreadyErasureCoded => "already_erasure_coded",
        TieringSkipReason::TooSmall { .. } => "too_small",
        TieringSkipReason::TooLarge { .. } => "too_large",
        TieringSkipReason::StillWarm { .. } => "still_warm",
        TieringSkipReason::Excluded(_) => "excluded",
        TieringSkipReason::RecentlyMigrated { .. } => "recently_migrated",
    }
}

/// The background migration worker.
pub struct EcTieringWorker {
    config: EcTieringWorkerConfig,
    environment: Arc<dyn EcTieringEnvironment>,
}

impl EcTieringWorker {
    /// Build a worker over the given environment.
    pub fn new(config: EcTieringWorkerConfig, environment: Arc<dyn EcTieringEnvironment>) -> Self {
        Self {
            config,
            environment,
        }
    }

    /// Run cycles until the task is aborted by the server lifecycle.
    ///
    /// The pause comes first, not last. A node starts its worker while the rest
    /// of the cluster is still coming up, and a sweep at that moment asks the
    /// pool to list and HEAD objects that nobody can answer for yet: the cycle
    /// fails on quorum, logs, and achieves nothing. Waiting one interval also
    /// means a restart loop cannot turn into a sweep loop. An operator who
    /// wants a cycle now has the admin hook.
    pub async fn run(self) {
        loop {
            tokio::time::sleep(self.config.scan_interval).await;

            match self.run_once(unix_time_nanos()).await {
                Ok(report) => {
                    if report.queued > 0 || report.scanned > 0 {
                        tracing::debug!(
                            scanned = report.scanned,
                            queued = report.queued,
                            skipped = report.skipped,
                            stop = ?report.stop,
                            "EC tiering cycle finished"
                        );
                    }
                }
                Err(error) => {
                    tracing::warn!(error = %error, "EC tiering cycle failed");
                }
            }
        }
    }

    /// Run one bounded scan cycle.
    pub async fn run_once(&self, now: u64) -> Result<EcTieringCycleReport, ClusterError> {
        let mut report = EcTieringCycleReport::default();
        if !self.config.enabled {
            report.stop = Some(EcTieringCycleStop::Disabled);
            return Ok(report);
        }

        // Before admission control and before any listing: a pool that must not
        // migrate gains nothing from measuring its own queues.
        if !self.environment.cluster_tiering_supported().await? {
            tracing::info!(
                "EC auto-tiering paused: not every node in the pool is confirmed to support \
                 EC v2. Either a node still runs older software, or one has not yet published \
                 its metadata to cluster membership. Migrations resume by themselves once all \
                 of them have."
            );
            report.stop = Some(EcTieringCycleStop::MixedCluster);
            return Ok(report);
        }

        // Admission control comes before any listing. A pool whose queues are
        // both full gains nothing from walking its catalog, and the walk is the
        // expensive half of the cycle.
        let outstanding =
            self.environment.outstanding_migrations(self.config.max_queue_depth).await?;
        ec_metrics::set_tiering_queue_depth(outstanding);
        let outstanding_promotions = self
            .environment
            .outstanding_promotions(self.config.max_promotion_queue_depth)
            .await?;
        let mut budget = Budget {
            demote: direction_budget(
                self.config.enqueue_per_cycle,
                self.config.max_queue_depth.saturating_sub(outstanding),
            ),
            promote: direction_budget(
                self.config.promote_per_cycle,
                self.config.max_promotion_queue_depth.saturating_sub(outstanding_promotions),
            ),
        };
        if !budget.can_demote() && !budget.can_promote() {
            report.stop = Some(EcTieringCycleStop::QueueFull);
            return Ok(report);
        }

        let buckets = self.environment.list_buckets().await?;
        let cursor = self.environment.load_cursor().await?;
        let (mut bucket_index, mut continuation) = resume_position(&buckets, cursor.as_ref());

        // The policy is fetched once per bucket, not once per page. Resolving it
        // goes to the cluster so that a policy written through another node
        // reaches this worker; doing that per page would put a quorum read
        // behind every thousand objects for an answer that cannot change
        // between two pages of the same sweep.
        let mut policy_for: Option<(usize, TieringPolicy)> = None;

        while bucket_index < buckets.len() {
            let bucket = &buckets[bucket_index];
            let policy = match &policy_for {
                Some((index, policy)) if *index == bucket_index => policy.clone(),
                _ => {
                    let policy = self.environment.effective_policy(bucket).await?;
                    policy_for = Some((bucket_index, policy.clone()));
                    policy
                }
            };

            // A bucket with tiering switched off is skipped whole, without a
            // single listing call: the per-object answer would be the same for
            // every key in it.
            if !policy.enabled {
                bucket_index += 1;
                continuation = None;
                continue;
            }

            let page_token = continuation.clone();
            let page_size = CATALOG_PAGE_SIZE
                .min(self.config.scan_limit.saturating_sub(report.scanned))
                .max(1);
            let page =
                self.environment.list_objects(bucket, page_token.as_deref(), page_size).await?;

            for entry in &page.entries {
                if report.scanned >= self.config.scan_limit {
                    return self
                        .finish(
                            bucket_index,
                            page_token,
                            &buckets,
                            report,
                            EcTieringCycleStop::ScanLimit,
                        )
                        .await;
                }
                report.scanned += 1;

                match self.examine(bucket, entry, &policy, &budget, now, &mut report).await? {
                    ExamineOutcome::Queued { size } => {
                        report.queued += 1;
                        report.queued_bytes = report.queued_bytes.saturating_add(size);
                        ec_metrics::record_tiering_queued(size);
                        budget.spend_demote();
                    }
                    ExamineOutcome::PromotionQueued { size } => {
                        report.promoted += 1;
                        report.promoted_bytes = report.promoted_bytes.saturating_add(size);
                        budget.spend_promote();
                    }
                    ExamineOutcome::AlreadyQueued => {}
                    ExamineOutcome::Nothing => {}
                }

                // Either direction running out ends the cycle, not both. The
                // cursor is one position for both streams, so walking on with
                // one of them spent would move it past objects that needed
                // exactly that direction, and they would wait a whole sweep.
                if let Some(stop) = budget.spent_direction() {
                    // The rest of this page is unfinished work, so the cursor
                    // stays on the token that produced it. The next cycle
                    // re-reads the page and steps over what is already queued,
                    // which the durable queues report without spending any of
                    // the new budget.
                    return self.finish(bucket_index, page_token, &buckets, report, stop).await;
                }
            }

            match page.next_continuation {
                Some(token) => continuation = Some(token),
                None => {
                    bucket_index += 1;
                    continuation = None;
                }
            }
        }

        // The whole catalog fit in this cycle, so the next one starts over.
        self.finish(
            buckets.len(),
            None,
            &buckets,
            report,
            EcTieringCycleStop::CatalogExhausted,
        )
        .await
    }

    /// Decide about one catalog entry and act on the decision.
    async fn examine(
        &self,
        bucket: &str,
        entry: &TieringCatalogEntry,
        policy: &TieringPolicy,
        budget: &Budget,
        now: u64,
        report: &mut EcTieringCycleReport,
    ) -> Result<ExamineOutcome, ClusterError> {
        // Size is decidable from the listing, and in most buckets it rules out
        // the majority of keys. Asking the pool about them first would put a
        // quorum HEAD behind every object of the scan limit. Only the objects
        // that are candidates for going into EC can be ruled out this way: an
        // object already in EC is coming back regardless of the size rules that
        // put it there, and the listing does not say which of the two it is.
        if entry.size < policy.min_object_size {
            report.record_skip(&TieringSkipReason::TooSmall {
                size: entry.size,
                minimum: policy.min_object_size,
            });
            return Ok(ExamineOutcome::Nothing);
        }
        if entry.size > policy.max_object_size {
            report.record_skip(&TieringSkipReason::TooLarge {
                size: entry.size,
                maximum: policy.max_object_size,
            });
            return Ok(ExamineOutcome::Nothing);
        }

        // An object that vanished between the listing and now is not a
        // candidate and not an error. On a busy bucket this happens routinely.
        let Some(mut candidate) = self.environment.candidate(bucket, &entry.key, policy).await?
        else {
            report.vanished = report.vanished.saturating_add(1);
            return Ok(ExamineOutcome::Nothing);
        };

        match decide(policy, &candidate, now) {
            TieringDecision::Demote if !budget.can_demote() => return Ok(ExamineOutcome::Nothing),
            TieringDecision::Demote => {}
            // An object already in EC never becomes a demotion, so a local
            // verdict of "leave it there" is worth a second look against the
            // pool: this node may simply not be the one serving the reads.
            TieringDecision::Promote
            | TieringDecision::Skip(TieringSkipReason::AlreadyErasureCoded)
                if candidate.erasure_coded && budget.can_promote() =>
            {
                return self.consider_promotion(candidate, policy, now, report).await;
            }
            TieringDecision::Promote => return Ok(ExamineOutcome::Nothing),
            TieringDecision::Skip(reason) => {
                report.record_skip(&reason);
                return Ok(ExamineOutcome::Nothing);
            }
        }

        // The local mark said cold. Per EC2-020 that is one node's view, and a
        // read served by any other node would not appear in it, so the pool is
        // asked before the object is committed to migration.
        let activity = self.environment.pool_activity(&candidate, policy).await?;
        if activity.newest_access_nanos > candidate.last_access_nanos {
            candidate.last_access_nanos = activity.newest_access_nanos;
            if let TieringDecision::Skip(reason) = decide(policy, &candidate, now) {
                report.kept_warm_by_peer = report.kept_warm_by_peer.saturating_add(1);
                report.record_skip(&reason);
                return Ok(ExamineOutcome::Nothing);
            }
        }

        if self.environment.enqueue_migration(&candidate).await? {
            tracing::debug!(
                bucket = %candidate.object.bucket,
                key = %candidate.object.key,
                size = candidate.size,
                "EC tiering queued a cold object for migration"
            );
            Ok(ExamineOutcome::Queued {
                size: candidate.size,
            })
        } else {
            Ok(ExamineOutcome::AlreadyQueued)
        }
    }

    /// Ask the pool whether an erasure-coded object is busy enough to come back.
    ///
    /// The read count is the one number a single node cannot answer on its own:
    /// each node counts only the reads it served, so a client spreading its
    /// traffic across the set looks idle everywhere.
    async fn consider_promotion(
        &self,
        mut candidate: TieringCandidate,
        policy: &TieringPolicy,
        now: u64,
        report: &mut EcTieringCycleReport,
    ) -> Result<ExamineOutcome, ClusterError> {
        let activity = self.environment.pool_activity(&candidate, policy).await?;
        candidate.last_access_nanos = candidate.last_access_nanos.max(activity.newest_access_nanos);
        candidate.window_access_count = candidate.window_access_count.max(activity.window_reads);

        match decide(policy, &candidate, now) {
            TieringDecision::Promote => {}
            TieringDecision::Skip(reason) => {
                report.record_skip(&reason);
                return Ok(ExamineOutcome::Nothing);
            }
            // An object the pool holds in erasure coding cannot be demoted into
            // it again, whatever the numbers say.
            TieringDecision::Demote => return Ok(ExamineOutcome::Nothing),
        }

        if self.environment.enqueue_promotion(&candidate).await? {
            tracing::debug!(
                bucket = %candidate.object.bucket,
                key = %candidate.object.key,
                reads = candidate.window_access_count,
                "EC tiering queued a busy object to come back to RF=3"
            );
            Ok(ExamineOutcome::PromotionQueued {
                size: candidate.size,
            })
        } else {
            Ok(ExamineOutcome::AlreadyQueued)
        }
    }

    /// Store the resume position and close the cycle report.
    async fn finish(
        &self,
        bucket_index: usize,
        continuation: Option<String>,
        buckets: &[String],
        mut report: EcTieringCycleReport,
        stop: EcTieringCycleStop,
    ) -> Result<EcTieringCycleReport, ClusterError> {
        let cursor = match buckets.get(bucket_index) {
            Some(bucket) => EcTieringCursorRecord {
                bucket: bucket.clone(),
                continuation,
                updated_at: unix_time_nanos(),
            },
            // Past the last bucket: the sweep is complete and the next cycle
            // starts from the first bucket again.
            None => EcTieringCursorRecord::start(unix_time_nanos()),
        };
        self.environment.save_cursor(&cursor).await?;

        ec_metrics::record_tiering_scan_cycle();
        ec_metrics::record_tiering_scanned(report.scanned);
        ec_metrics::record_tiering_skipped(report.skipped);
        report.stop = Some(stop);
        Ok(report)
    }
}

enum ExamineOutcome {
    Queued { size: u64 },
    PromotionQueued { size: u64 },
    AlreadyQueued,
    Nothing,
}

/// What one cycle has left to admit, in each direction.
///
/// The two are tracked apart because they drain into different queues with
/// different ceilings. `None` means the direction is not participating at all —
/// its per-cycle limit is zero, or its queue was already full when the cycle
/// began — which is not the same as having run out mid-walk.
#[derive(Debug, Clone, Copy)]
struct Budget {
    demote: Option<usize>,
    promote: Option<usize>,
}

impl Budget {
    fn can_demote(&self) -> bool {
        self.demote.is_some_and(|left| left > 0)
    }

    fn can_promote(&self) -> bool {
        self.promote.is_some_and(|left| left > 0)
    }

    fn spend_demote(&mut self) {
        if let Some(left) = self.demote.as_mut() {
            *left = left.saturating_sub(1);
        }
    }

    fn spend_promote(&mut self) {
        if let Some(left) = self.promote.as_mut() {
            *left = left.saturating_sub(1);
        }
    }

    /// The direction that has just run out, if one has.
    fn spent_direction(&self) -> Option<EcTieringCycleStop> {
        if self.demote == Some(0) {
            return Some(EcTieringCycleStop::EnqueueLimit);
        }
        if self.promote == Some(0) {
            return Some(EcTieringCycleStop::PromoteLimit);
        }
        None
    }
}

/// One direction's budget for this cycle, or `None` when it is not running.
fn direction_budget(per_cycle: usize, headroom: usize) -> Option<usize> {
    let budget = per_cycle.min(headroom);
    (budget > 0).then_some(budget)
}

/// Where the next cycle starts, given the stored cursor.
///
/// A cursor naming a bucket that no longer exists rewinds to the beginning
/// rather than skipping ahead: the alternative is a deleted bucket quietly
/// hiding every bucket that sorted before it.
fn resume_position(
    buckets: &[String],
    cursor: Option<&EcTieringCursorRecord>,
) -> (usize, Option<String>) {
    let Some(cursor) = cursor else {
        return (0, None);
    };
    if cursor.bucket.is_empty() {
        return (0, None);
    }
    match buckets.iter().position(|bucket| bucket == &cursor.bucket) {
        Some(index) => (index, cursor.continuation.clone()),
        None => (0, None),
    }
}

fn unix_time_nanos() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashMap;
    use std::sync::Mutex;

    use crate::ec_metadata::EcObjectVersionKey;

    const SECOND: u64 = 1_000_000_000;
    const DAY: u64 = 24 * 3_600 * SECOND;
    const MIB: u64 = 1024 * 1024;
    const NOW: u64 = 100 * DAY;

    /// A catalog and a queue held in memory, with no cluster behind them.
    #[derive(Default)]
    struct FakeEnvironment {
        buckets: Vec<String>,
        objects: HashMap<String, Vec<TieringCatalogEntry>>,
        policies: HashMap<String, TieringPolicy>,
        last_access: HashMap<String, u64>,
        peer_access: HashMap<String, u64>,
        peer_reads: HashMap<String, u32>,
        erasure_coded: HashMap<String, bool>,
        missing: Vec<String>,
        state: Mutex<FakeState>,
    }

    #[derive(Default)]
    struct FakeState {
        queued: Vec<String>,
        promotions: Vec<String>,
        outstanding: usize,
        outstanding_promotions: usize,
        cursor: Option<EcTieringCursorRecord>,
        listed_pages: usize,
        peer_queries: usize,
        heads: usize,
        cluster_supported: Option<bool>,
        policy_lookups: usize,
    }

    impl FakeEnvironment {
        fn with_objects(count: usize) -> Self {
            let bucket = "cold".to_string();
            let entries = (0..count)
                .map(|index| TieringCatalogEntry {
                    key: format!("object-{index:04}"),
                    size: 8 * MIB,
                })
                .collect();
            Self {
                buckets: vec![bucket.clone()],
                objects: HashMap::from([(bucket.clone(), entries)]),
                policies: HashMap::from([(bucket, policy("cold"))]),
                ..Self::default()
            }
        }

        fn queued(&self) -> Vec<String> {
            self.state.lock().expect("fake state").queued.clone()
        }

        fn cursor(&self) -> Option<EcTieringCursorRecord> {
            self.state.lock().expect("fake state").cursor.clone()
        }

        fn promotions(&self) -> Vec<String> {
            self.state.lock().expect("fake state").promotions.clone()
        }
    }

    #[async_trait]
    impl EcTieringEnvironment for Arc<FakeEnvironment> {
        async fn list_buckets(&self) -> Result<Vec<String>, ClusterError> {
            Ok(self.buckets.clone())
        }

        async fn list_objects(
            &self,
            bucket: &str,
            continuation: Option<&str>,
            limit: usize,
        ) -> Result<TieringCatalogPage, ClusterError> {
            self.state.lock().expect("fake state").listed_pages += 1;
            let all = self.objects.get(bucket).cloned().unwrap_or_default();
            let start = match continuation {
                Some(token) => all.iter().position(|entry| entry.key == token).unwrap_or(all.len()),
                None => 0,
            };
            let end = (start + limit).min(all.len());
            Ok(TieringCatalogPage {
                entries: all[start..end].to_vec(),
                next_continuation: all.get(end).map(|entry| entry.key.clone()),
            })
        }

        async fn effective_policy(&self, bucket: &str) -> Result<TieringPolicy, ClusterError> {
            self.state.lock().expect("fake state").policy_lookups += 1;
            Ok(self.policies.get(bucket).cloned().unwrap_or_else(|| policy(bucket)))
        }

        async fn candidate(
            &self,
            bucket: &str,
            key: &str,
            _policy: &TieringPolicy,
        ) -> Result<Option<TieringCandidate>, ClusterError> {
            self.state.lock().expect("fake state").heads += 1;
            if self.missing.iter().any(|missing| missing == key) {
                return Ok(None);
            }
            let size = self
                .objects
                .get(bucket)
                .and_then(|entries| entries.iter().find(|entry| entry.key == key))
                .map_or(8 * MIB, |entry| entry.size);
            Ok(Some(TieringCandidate {
                object: EcObjectVersionKey::new(bucket, key, None),
                size,
                is_delete_marker: false,
                erasure_coded: self.erasure_coded.get(key).copied().unwrap_or(false),
                tags: HashMap::new(),
                last_access_nanos: self.last_access.get(key).copied().unwrap_or(NOW - 40 * DAY),
                window_access_count: 0,
                migrations: 0,
                last_migration_nanos: None,
            }))
        }

        async fn pool_activity(
            &self,
            candidate: &TieringCandidate,
            _policy: &TieringPolicy,
        ) -> Result<PoolActivity, ClusterError> {
            self.state.lock().expect("fake state").peer_queries += 1;
            Ok(PoolActivity {
                newest_access_nanos: self
                    .peer_access
                    .get(&candidate.object.key)
                    .copied()
                    .unwrap_or(candidate.last_access_nanos),
                window_reads: candidate.window_access_count.saturating_add(
                    self.peer_reads.get(&candidate.object.key).copied().unwrap_or(0),
                ),
            })
        }

        async fn enqueue_migration(
            &self,
            candidate: &TieringCandidate,
        ) -> Result<bool, ClusterError> {
            let mut state = self.state.lock().expect("fake state");
            if state.queued.contains(&candidate.object.key) {
                return Ok(false);
            }
            state.queued.push(candidate.object.key.clone());
            state.outstanding += 1;
            Ok(true)
        }

        async fn outstanding_migrations(&self, ceiling: usize) -> Result<usize, ClusterError> {
            Ok(self.state.lock().expect("fake state").outstanding.min(ceiling))
        }

        async fn enqueue_promotion(
            &self,
            candidate: &TieringCandidate,
        ) -> Result<bool, ClusterError> {
            let mut state = self.state.lock().expect("fake state");
            if state.promotions.contains(&candidate.object.key) {
                return Ok(false);
            }
            state.promotions.push(candidate.object.key.clone());
            state.outstanding_promotions += 1;
            Ok(true)
        }

        async fn outstanding_promotions(&self, ceiling: usize) -> Result<usize, ClusterError> {
            Ok(self.state.lock().expect("fake state").outstanding_promotions.min(ceiling))
        }

        async fn load_cursor(&self) -> Result<Option<EcTieringCursorRecord>, ClusterError> {
            Ok(self.state.lock().expect("fake state").cursor.clone())
        }

        async fn save_cursor(&self, cursor: &EcTieringCursorRecord) -> Result<(), ClusterError> {
            cursor.validate().map_err(|error| ClusterError::Storage(error.to_string()))?;
            self.state.lock().expect("fake state").cursor = Some(cursor.clone());
            Ok(())
        }

        async fn cluster_tiering_supported(&self) -> Result<bool, ClusterError> {
            Ok(self.state.lock().expect("fake state").cluster_supported.unwrap_or(true))
        }
    }

    fn policy(bucket: &str) -> TieringPolicy {
        TieringPolicy {
            bucket: bucket.to_string(),
            enabled: true,
            cold_after: Duration::from_secs(30 * 24 * 3_600),
            min_object_size: MIB,
            max_object_size: 64 * MIB,
            exclusions: Vec::new(),
            promote_after_reads: 0,
            promote_window: Duration::from_secs(24 * 3_600),
            min_migration_interval: Duration::from_secs(7 * 24 * 3_600),
            jitter_percent: 0,
        }
    }

    fn config() -> EcTieringWorkerConfig {
        EcTieringWorkerConfig {
            enabled: true,
            scan_interval: Duration::from_secs(3_600),
            scan_limit: 1_000,
            enqueue_per_cycle: 64,
            max_queue_depth: 1_024,
            promote_per_cycle: 16,
            max_promotion_queue_depth: 256,
        }
    }

    fn worker(
        environment: &Arc<FakeEnvironment>,
        config: EcTieringWorkerConfig,
    ) -> EcTieringWorker {
        EcTieringWorker::new(config, Arc::new(Arc::clone(environment)))
    }

    /// Turning tiering on where there is nothing to move queues nothing.
    #[tokio::test]
    async fn an_empty_catalog_produces_no_work() {
        let environment = Arc::new(FakeEnvironment::default());
        let worker = worker(&environment, config());

        let report = worker.run_once(NOW).await.unwrap();

        assert_eq!(report.scanned, 0);
        assert_eq!(report.queued, 0);
        assert_eq!(report.stop, Some(EcTieringCycleStop::CatalogExhausted));
        assert!(environment.queued().is_empty());
    }

    #[tokio::test]
    async fn a_disabled_worker_touches_nothing() {
        let environment = Arc::new(FakeEnvironment::with_objects(4));
        let worker = worker(
            &environment,
            EcTieringWorkerConfig {
                enabled: false,
                ..config()
            },
        );

        let report = worker.run_once(NOW).await.unwrap();

        assert_eq!(report.stop, Some(EcTieringCycleStop::Disabled));
        assert_eq!(report.scanned, 0);
        assert!(environment.queued().is_empty());
        // Not even the resume position moves: a disabled worker must leave the
        // node exactly as it found it.
        assert!(environment.cursor().is_none());
    }

    #[tokio::test]
    async fn cold_objects_are_queued_and_the_sweep_completes() {
        let environment = Arc::new(FakeEnvironment::with_objects(3));
        let worker = worker(&environment, config());

        let report = worker.run_once(NOW).await.unwrap();

        assert_eq!(report.scanned, 3);
        assert_eq!(report.queued, 3);
        assert_eq!(report.queued_bytes, 3 * 8 * MIB);
        assert_eq!(report.stop, Some(EcTieringCycleStop::CatalogExhausted));
        assert_eq!(environment.queued().len(), 3);
        // A finished sweep rewinds, so the next cycle sees new objects at the
        // head of the catalog as well as the tail.
        assert_eq!(environment.cursor().unwrap().bucket, "");
    }

    #[tokio::test]
    async fn a_warm_object_stays_where_it_is() {
        let mut environment = FakeEnvironment::with_objects(2);
        environment.last_access.insert("object-0000".to_string(), NOW - 3 * DAY);
        let environment = Arc::new(environment);
        let worker = worker(&environment, config());

        let report = worker.run_once(NOW).await.unwrap();

        assert_eq!(report.queued, 1);
        assert_eq!(environment.queued(), vec!["object-0001".to_string()]);
        assert_eq!(report.skip_reasons.get("still_warm"), Some(&1));
    }

    #[tokio::test]
    async fn sizes_outside_the_bounds_never_reach_the_network() {
        let mut environment = FakeEnvironment::with_objects(0);
        environment.objects.insert(
            "cold".to_string(),
            vec![
                TieringCatalogEntry {
                    key: "tiny".to_string(),
                    size: 1_024,
                },
                TieringCatalogEntry {
                    key: "huge".to_string(),
                    size: 512 * MIB,
                },
            ],
        );
        let environment = Arc::new(environment);
        let worker = worker(&environment, config());

        let report = worker.run_once(NOW).await.unwrap();

        assert_eq!(report.scanned, 2);
        assert_eq!(report.queued, 0);
        assert_eq!(report.skip_reasons.get("too_small"), Some(&1));
        assert_eq!(report.skip_reasons.get("too_large"), Some(&1));
        // The listing already answered, so neither object cost a quorum HEAD.
        assert_eq!(environment.state.lock().unwrap().heads, 0);
    }

    #[tokio::test]
    async fn a_peer_that_served_the_last_read_keeps_the_object_hot() {
        let mut environment = FakeEnvironment::with_objects(1);
        // Nothing read it on this node for forty days, but another node served
        // a read an hour ago. Only the pool-wide answer sees that.
        environment.peer_access.insert("object-0000".to_string(), NOW - 3_600 * SECOND);
        let environment = Arc::new(environment);
        let worker = worker(&environment, config());

        let report = worker.run_once(NOW).await.unwrap();

        assert_eq!(report.queued, 0);
        assert_eq!(report.kept_warm_by_peer, 1);
        assert_eq!(report.skip_reasons.get("still_warm"), Some(&1));
        assert!(environment.queued().is_empty());
    }

    #[tokio::test]
    async fn peers_are_only_asked_about_objects_that_would_migrate() {
        let mut environment = FakeEnvironment::with_objects(3);
        environment.last_access.insert("object-0000".to_string(), NOW - SECOND);
        environment.last_access.insert("object-0001".to_string(), NOW - SECOND);
        let environment = Arc::new(environment);
        let worker = worker(&environment, config());

        worker.run_once(NOW).await.unwrap();

        // Two of the three were warm locally and never reached the network.
        assert_eq!(environment.state.lock().unwrap().peer_queries, 1);
    }

    #[tokio::test]
    async fn the_enqueue_budget_bounds_one_cycle() {
        let environment = Arc::new(FakeEnvironment::with_objects(10));
        let worker = worker(
            &environment,
            EcTieringWorkerConfig {
                enqueue_per_cycle: 4,
                ..config()
            },
        );

        let report = worker.run_once(NOW).await.unwrap();

        assert_eq!(report.queued, 4);
        assert_eq!(report.stop, Some(EcTieringCycleStop::EnqueueLimit));
        assert_eq!(environment.queued().len(), 4);
    }

    #[tokio::test]
    async fn a_full_queue_skips_the_catalog_walk_entirely() {
        let environment = Arc::new(FakeEnvironment::with_objects(10));
        {
            let mut state = environment.state.lock().unwrap();
            state.outstanding = 8;
            state.outstanding_promotions = 4;
        }
        let worker = worker(
            &environment,
            EcTieringWorkerConfig {
                max_queue_depth: 8,
                max_promotion_queue_depth: 4,
                ..config()
            },
        );

        let report = worker.run_once(NOW).await.unwrap();

        assert_eq!(report.stop, Some(EcTieringCycleStop::QueueFull));
        assert_eq!(environment.state.lock().unwrap().listed_pages, 0);
    }

    /// A full forward queue must not stop objects coming back.
    ///
    /// The two directions have separate ceilings precisely so that a rollback
    /// stays possible while the queue of migrations being undone is full. One
    /// shared ceiling would take the rollback away at exactly the moment an
    /// operator reaches for it.
    #[tokio::test]
    async fn a_full_migration_queue_still_lets_objects_come_back() {
        let mut environment = FakeEnvironment::with_objects(4);
        environment.policies.insert(
            "cold".to_string(),
            TieringPolicy {
                promote_after_reads: 3,
                ..policy("cold")
            },
        );
        for index in 0..4 {
            let key = format!("object-{index:04}");
            environment.erasure_coded.insert(key.clone(), true);
            environment.peer_reads.insert(key, 5);
        }
        let environment = Arc::new(environment);
        environment.state.lock().unwrap().outstanding = 8;

        let worker = worker(
            &environment,
            EcTieringWorkerConfig {
                max_queue_depth: 8,
                ..config()
            },
        );

        let report = worker.run_once(NOW).await.unwrap();

        assert_eq!(report.queued, 0, "the forward queue is full");
        assert_eq!(report.promoted, 4);
        assert_eq!(environment.promotions().len(), 4);
    }

    /// One read does not bring an object back, however recent it is.
    ///
    /// Background sweeps touch every object in the store. A threshold of one
    /// would turn each of them into a cluster-wide reverse migration.
    #[tokio::test]
    async fn a_single_read_does_not_promote_an_erasure_coded_object() {
        let mut environment = FakeEnvironment::with_objects(1);
        environment.policies.insert(
            "cold".to_string(),
            TieringPolicy {
                promote_after_reads: 5,
                ..policy("cold")
            },
        );
        environment.erasure_coded.insert("object-0000".to_string(), true);
        environment.peer_reads.insert("object-0000".to_string(), 1);
        let environment = Arc::new(environment);

        let worker = worker(&environment, config());
        let report = worker.run_once(NOW).await.unwrap();

        assert_eq!(report.promoted, 0);
        assert!(environment.promotions().is_empty());
        assert_eq!(report.skip_reasons.get("already_erasure_coded"), Some(&1));
    }

    /// Reads served by different nodes add up to one object's traffic.
    ///
    /// Each node counts only what it served itself, so a client spreading its
    /// reads across the set looks idle on every one of them. Taking the highest
    /// single node's count instead of the sum would keep a busy object in
    /// erasure coding forever.
    #[tokio::test]
    async fn reads_from_several_nodes_add_up_to_the_promotion_threshold() {
        let mut environment = FakeEnvironment::with_objects(1);
        environment.policies.insert(
            "cold".to_string(),
            TieringPolicy {
                promote_after_reads: 6,
                ..policy("cold")
            },
        );
        environment.erasure_coded.insert("object-0000".to_string(), true);
        // Four reads here, and the peers report four more between them.
        environment.peer_reads.insert("object-0000".to_string(), 4);
        let environment = Arc::new(environment);

        let quiet_worker = worker(&environment, config());
        let report = quiet_worker.run_once(NOW).await.unwrap();

        assert_eq!(report.promoted, 0, "this node alone is under the threshold");

        let mut busier = FakeEnvironment::with_objects(1);
        busier.policies.insert(
            "cold".to_string(),
            TieringPolicy {
                promote_after_reads: 6,
                ..policy("cold")
            },
        );
        busier.erasure_coded.insert("object-0000".to_string(), true);
        busier.peer_reads.insert("object-0000".to_string(), 6);
        let busier = Arc::new(busier);

        let busier_worker = worker(&busier, config());
        assert_eq!(busier_worker.run_once(NOW).await.unwrap().promoted, 1);
    }

    #[tokio::test]
    async fn outstanding_work_narrows_the_budget_below_the_per_cycle_limit() {
        let environment = Arc::new(FakeEnvironment::with_objects(10));
        environment.state.lock().unwrap().outstanding = 6;
        let worker = worker(
            &environment,
            EcTieringWorkerConfig {
                enqueue_per_cycle: 5,
                max_queue_depth: 8,
                ..config()
            },
        );

        let report = worker.run_once(NOW).await.unwrap();

        // Room for two more, not for the five the per-cycle limit allows.
        assert_eq!(report.queued, 2);
        assert_eq!(report.stop, Some(EcTieringCycleStop::EnqueueLimit));
    }

    #[tokio::test]
    async fn the_scan_limit_stops_the_cycle_before_the_catalog_ends() {
        let environment = Arc::new(FakeEnvironment::with_objects(10));
        let worker = worker(
            &environment,
            EcTieringWorkerConfig {
                scan_limit: 4,
                enqueue_per_cycle: 64,
                ..config()
            },
        );

        let report = worker.run_once(NOW).await.unwrap();

        assert_eq!(report.scanned, 4);
        assert_eq!(report.stop, Some(EcTieringCycleStop::ScanLimit));
    }

    #[tokio::test]
    async fn a_second_cycle_continues_where_the_first_stopped() {
        let environment = Arc::new(FakeEnvironment::with_objects(10));
        let config = EcTieringWorkerConfig {
            scan_limit: 4,
            ..config()
        };
        let worker = worker(&environment, config.clone());

        worker.run_once(NOW).await.unwrap();
        let first = environment.queued();

        // A fresh worker over the same store stands in for a node restart:
        // nothing about the position lives in memory.
        let restarted = worker.clone_for_restart(&environment, config);
        let second = restarted.run_once(NOW).await.unwrap();

        assert_eq!(first.len(), 4);
        assert_eq!(second.queued, 4);
        let all = environment.queued();
        assert_eq!(all.len(), 8);
        // No object was examined twice, and none was skipped over.
        let mut sorted = all.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(sorted.len(), 8);
        assert_eq!(sorted[0], "object-0000");
        assert_eq!(sorted[7], "object-0007");
    }

    #[tokio::test]
    async fn a_page_cut_short_by_the_budget_is_finished_by_the_next_cycle() {
        let environment = Arc::new(FakeEnvironment::with_objects(6));
        let config = EcTieringWorkerConfig {
            enqueue_per_cycle: 2,
            ..config()
        };
        let worker = worker(&environment, config.clone());

        worker.run_once(NOW).await.unwrap();
        // The queue drains between cycles, so the ceiling is not what limits
        // the second one.
        environment.state.lock().unwrap().outstanding = 0;
        let second = worker.run_once(NOW).await.unwrap();

        assert_eq!(second.queued, 2);
        assert_eq!(
            environment.queued(),
            vec![
                "object-0000".to_string(),
                "object-0001".to_string(),
                "object-0002".to_string(),
                "object-0003".to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn an_object_that_vanished_is_not_a_failure() {
        let mut environment = FakeEnvironment::with_objects(3);
        environment.missing.push("object-0001".to_string());
        let environment = Arc::new(environment);
        let worker = worker(&environment, config());

        let report = worker.run_once(NOW).await.unwrap();

        assert_eq!(report.scanned, 3);
        assert_eq!(report.vanished, 1);
        assert_eq!(report.queued, 2);
    }

    #[tokio::test]
    async fn an_already_encoded_object_is_left_alone() {
        let mut environment = FakeEnvironment::with_objects(2);
        environment.erasure_coded.insert("object-0000".to_string(), true);
        let environment = Arc::new(environment);
        let worker = worker(&environment, config());

        let report = worker.run_once(NOW).await.unwrap();

        assert_eq!(report.queued, 1);
        assert_eq!(report.skip_reasons.get("already_erasure_coded"), Some(&1));
    }

    #[tokio::test]
    async fn a_bucket_with_tiering_switched_off_is_never_listed() {
        let mut environment = FakeEnvironment::with_objects(4);
        environment.policies.insert(
            "cold".to_string(),
            TieringPolicy {
                enabled: false,
                ..policy("cold")
            },
        );
        let environment = Arc::new(environment);
        let worker = worker(&environment, config());

        let report = worker.run_once(NOW).await.unwrap();

        assert_eq!(report.scanned, 0);
        assert_eq!(environment.state.lock().unwrap().listed_pages, 0);
        assert_eq!(report.stop, Some(EcTieringCycleStop::CatalogExhausted));
    }

    #[tokio::test]
    async fn a_cursor_naming_a_deleted_bucket_rewinds_to_the_start() {
        let environment = Arc::new(FakeEnvironment::with_objects(2));
        environment.state.lock().unwrap().cursor = Some(EcTieringCursorRecord {
            bucket: "bucket-that-was-deleted".to_string(),
            continuation: Some("object-9999".to_string()),
            updated_at: 1,
        });
        let worker = worker(&environment, config());

        let report = worker.run_once(NOW).await.unwrap();

        assert_eq!(report.scanned, 2);
        assert_eq!(report.queued, 2);
    }

    #[tokio::test]
    async fn a_mixed_cluster_refuses_to_run_tiering_migrations() {
        let environment = Arc::new(FakeEnvironment::with_objects(4));
        environment.state.lock().unwrap().cluster_supported = Some(false);
        let worker = worker(&environment, config());

        let report = worker.run_once(NOW).await.unwrap();

        assert_eq!(report.stop, Some(EcTieringCycleStop::MixedCluster));
        assert_eq!(report.scanned, 0);
        assert_eq!(report.queued, 0);
        assert_eq!(environment.state.lock().unwrap().listed_pages, 0);
    }

    /// The policy is resolved once per bucket, however many pages it takes.
    ///
    /// Resolving it goes to the cluster — that is what makes a policy written
    /// through another node reach this worker at all — so doing it per page
    /// would put a quorum read behind every page of every sweep, for an answer
    /// that cannot change between two pages of the same bucket.
    #[tokio::test]
    async fn the_bucket_policy_is_resolved_once_per_bucket() {
        // Comfortably more objects than one page holds.
        let environment = Arc::new(FakeEnvironment::with_objects(CATALOG_PAGE_SIZE * 3));
        let worker = worker(
            &environment,
            EcTieringWorkerConfig {
                scan_limit: CATALOG_PAGE_SIZE * 4,
                enqueue_per_cycle: CATALOG_PAGE_SIZE * 4,
                max_queue_depth: CATALOG_PAGE_SIZE * 8,
                ..config()
            },
        );

        let report = worker.run_once(NOW).await.unwrap();

        let state = environment.state.lock().unwrap();
        assert!(
            state.listed_pages > 1,
            "the test needs a bucket that spans several pages"
        );
        assert_eq!(
            state.policy_lookups, 1,
            "the policy was resolved {} times for one bucket",
            state.policy_lookups
        );
        assert_eq!(report.stop, Some(EcTieringCycleStop::CatalogExhausted));
    }

    /// The gate is re-read every cycle, so finishing the upgrade is enough.
    ///
    /// If the verdict were cached, an operator who completed a rolling upgrade
    /// would have to restart every node to get migrations back, and nothing in
    /// the status output would explain why.
    #[tokio::test]
    async fn tiering_resumes_by_itself_once_the_pool_is_uniform() {
        let environment = Arc::new(FakeEnvironment::with_objects(4));
        environment.state.lock().unwrap().cluster_supported = Some(false);
        let worker = worker(&environment, config());

        let blocked = worker.run_once(NOW).await.unwrap();
        assert_eq!(blocked.stop, Some(EcTieringCycleStop::MixedCluster));

        environment.state.lock().unwrap().cluster_supported = Some(true);
        let resumed = worker.run_once(NOW).await.unwrap();

        assert_ne!(resumed.stop, Some(EcTieringCycleStop::MixedCluster));
        assert!(
            resumed.scanned > 0,
            "the same worker must scan once the pool is uniform"
        );
        assert!(environment.state.lock().unwrap().listed_pages > 0);
    }

    /// A node with tiering switched off reports that, not a mixed cluster.
    ///
    /// The two stops send an operator to different places, so the cheaper and
    /// more specific one has to win.
    #[tokio::test]
    async fn a_disabled_node_reports_disabled_even_in_a_mixed_cluster() {
        let environment = Arc::new(FakeEnvironment::with_objects(4));
        environment.state.lock().unwrap().cluster_supported = Some(false);
        let worker = worker(
            &environment,
            EcTieringWorkerConfig {
                enabled: false,
                ..config()
            },
        );

        let report = worker.run_once(NOW).await.unwrap();

        assert_eq!(report.stop, Some(EcTieringCycleStop::Disabled));
    }

    impl EcTieringWorker {
        /// Build a second worker over the same environment, as a restart would.
        fn clone_for_restart(
            &self,
            environment: &Arc<FakeEnvironment>,
            config: EcTieringWorkerConfig,
        ) -> Self {
            Self::new(config, Arc::new(Arc::clone(environment)))
        }
    }
}
