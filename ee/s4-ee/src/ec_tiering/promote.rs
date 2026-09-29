// Copyright 2026 S4Core Team
//
// Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

//! The background worker that brings erasure-coded objects back to RF=3.
//!
//! Reverse tiering exists for two reasons, and the second one matters more. It
//! returns latency to an object that became hot again, and it is the only way
//! an operator can undo a migration the forward worker should not have made.
//! Without it, phase 3 is a one-way door. Decision [EC2-023].
//!
//! Like the forward worker, this one owns no I/O: the queue, the shards, the
//! replicas and the manifest all arrive through [`EcPromotionEnvironment`], so
//! the whole state machine — including every way it can be interrupted — is
//! checkable without a cluster.
//!
//! The order of the steps is the part worth reading:
//!
//! 1. decode the object from its shards and check it against the manifest;
//! 2. mark the manifest as being promoted, through the metadata quorum, and
//!    only then write the RF=3 copies back, guarded by the generation the
//!    manifest names;
//! 3. read that copy back through quorum and confirm it is the object;
//! 4. publish a promoted manifest, which is what makes readers stop using the
//!    shards — in the same pass as step 3, so the switch rests on a check made
//!    just before it;
//! 5. and only after a grace period, remove the shards and the manifest.
//!
//! Steps 3 and 5 are never merged and never reordered. This is the mirror of
//! the gates hot purge already applies in the other direction, and for the same
//! reason: until the new copy has been read back and given time to settle, the
//! old one is the only copy that is known to work.
//!
//! The mark in step 2 is what keeps the copies from being taken away again.
//! A restored copy has the generation, layout and cached manifest of a hot copy
//! an unfinished purge left behind, and namespace reconciliation on any node
//! releases those. The mark is the one thing that tells the two apart, and it
//! is at quorum before the first byte comes back. Decision [EC2-026].
//!
//! Throughout all five steps the object stays readable. Before step 4 every
//! node still resolves it through erasure coding, and the shards are all still
//! there; from step 4 on, every node resolves it through the hot path, and the
//! replicas are all still there. There is no moment when neither answers.
//!
//! [EC2-023]: `EC-v2/adr/EC2-023-reverse-tiering.md`
//! [EC2-026]: `EC-v2/adr/EC2-026-promotion-marker-before-restore.md`

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use s4_cluster::ClusterError;

use crate::ec_metadata::{EcObjectVersionKey, EcPromotionJobRecord, EcPromotionState};
use crate::ec_metrics;

/// What one step of the pipeline did to a job.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EcPromotionStep {
    /// The RF=3 copies are back and the namespace entries are local again.
    HotRestored,
    /// The RF=3 copy was read back, matched, and readers were switched to the
    /// hot path in the same pass.
    ManifestPromoted,
    /// The shards and the manifest are gone.
    Completed,
    /// The shards may not be removed yet; the grace period is still running.
    WaitingForGrace,
    /// A client wrote the object; the promotion is dropped.
    Superseded,
    /// The step failed and the job will be retried.
    Retrying,
    /// The step failed for good.
    Failed,
}

/// Everything the promotion worker needs from the rest of the node.
#[async_trait]
pub trait EcPromotionEnvironment: Send + Sync + 'static {
    /// Jobs eligible for work at `now`, at most `limit` of them.
    async fn ready_jobs(
        &self,
        now: u64,
        limit: usize,
    ) -> Result<Vec<EcPromotionJobRecord>, ClusterError>;

    /// Store a job's new state.
    async fn save_job(&self, job: &EcPromotionJobRecord) -> Result<(), ClusterError>;

    /// Forget a job that has reached its end.
    async fn delete_job(&self, object: &EcObjectVersionKey) -> Result<(), ClusterError>;

    /// Decode the object from its shards and check it against the manifest.
    ///
    /// Returns `None` when the object is no longer erasure coded — deleted, or
    /// already brought back by someone else. That is not a failure to retry:
    /// there is nothing left to promote.
    async fn read_erasure_coded(
        &self,
        job: &EcPromotionJobRecord,
    ) -> Result<Option<Vec<u8>>, ClusterError>;

    /// Tell the pool that this generation's hot copies are coming back.
    ///
    /// Commits the manifest as `promoting` through the metadata quorum, and
    /// has to succeed before a single byte is written. From then on every node
    /// that sees the restored copies also sees the mark, and namespace
    /// reconciliation leaves them alone instead of taking them for leftovers
    /// of an unfinished purge (EC2-026). Idempotent: a mark or a promoted
    /// manifest already at quorum is the state asked for.
    ///
    /// Returns `false` when the pool no longer holds this generation in
    /// erasure coding — deleted, or overwritten by a client.
    async fn mark_promotion_in_flight(
        &self,
        job: &EcPromotionJobRecord,
    ) -> Result<bool, ClusterError>;

    /// Write the bytes back to the RF=3 replicas under the generation guard.
    ///
    /// Returns `false` when the guard no longer holds, which means a client
    /// wrote the object and its write must be left alone.
    async fn restore_hot_copy(
        &self,
        job: &EcPromotionJobRecord,
        data: &[u8],
    ) -> Result<bool, ClusterError>;

    /// Read the RF=3 copy back through quorum and confirm it is the object.
    ///
    /// Returns `false` when the copy is not there or does not match, which is
    /// a reason to write it again rather than to go on: the shards are still
    /// the only copy known to work.
    async fn verify_hot_copy(&self, job: &EcPromotionJobRecord) -> Result<bool, ClusterError>;

    /// Publish the promoted manifest, switching every node to the hot path.
    ///
    /// Returns `false` when the manifest is gone or has been superseded.
    async fn publish_promoted_manifest(
        &self,
        job: &EcPromotionJobRecord,
    ) -> Result<bool, ClusterError>;

    /// Remove the shards and the manifest of a promoted object.
    ///
    /// Returns `false` when a gate still holds the shards back, which is not a
    /// failure: the next cycle asks again.
    async fn purge_erasure_coded_copy(
        &self,
        job: &EcPromotionJobRecord,
    ) -> Result<bool, ClusterError>;

    /// Record that the object changed layout, for the hysteresis pause.
    async fn record_migration(
        &self,
        object: &EcObjectVersionKey,
        now: u64,
    ) -> Result<(), ClusterError>;

    /// Release this node's shards for objects the pool has already promoted.
    ///
    /// Separate from the per-job step, and not the same work. A promotion is
    /// carried out by one node, but every node holds shards of its own, and
    /// only that node can release them. Returns the number released.
    async fn sweep_promoted_shards(&self, now: u64) -> Result<usize, ClusterError>;
}

/// Limits one promotion cycle runs under.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EcPromotionWorkerConfig {
    /// Whether the worker runs at all.
    pub enabled: bool,
    /// Pause between two cycles.
    pub poll_interval: Duration,
    /// Jobs one cycle may advance.
    pub batch_size: usize,
    /// How long the shards outlive a verified hot copy.
    pub shard_grace: Duration,
    /// Attempts a job gets before it is given up on.
    pub max_retry_attempts: u32,
    /// Base pause before a failed job is tried again.
    pub retry_backoff: Duration,
}

/// What one cycle did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EcPromotionCycleReport {
    /// Jobs looked at.
    pub examined: usize,
    /// Objects whose RF=3 copies came back in this cycle.
    pub restored: usize,
    /// Objects whose readers were switched to the hot path in this cycle.
    pub promoted: usize,
    /// Objects whose shards were removed in this cycle.
    pub completed: usize,
    /// Local shards released for objects the pool had already promoted.
    pub swept_shards: usize,
    /// Objects a client write took away from the promotion.
    pub superseded: usize,
    /// Steps that failed and will be tried again.
    pub retried: usize,
    /// Jobs given up on.
    pub failed: usize,
    /// Bytes promoted back to RF=3.
    pub restored_bytes: u64,
    /// Steps taken, by name, with counts.
    pub steps: BTreeMap<&'static str, usize>,
}

impl EcPromotionCycleReport {
    fn record(&mut self, step: EcPromotionStep, job: &EcPromotionJobRecord) {
        *self.steps.entry(step_name(step)).or_insert(0) += 1;
        match step {
            EcPromotionStep::HotRestored => {
                self.restored = self.restored.saturating_add(1);
                self.restored_bytes = self.restored_bytes.saturating_add(job.object_size);
            }
            EcPromotionStep::ManifestPromoted => self.promoted = self.promoted.saturating_add(1),
            EcPromotionStep::Completed => self.completed = self.completed.saturating_add(1),
            EcPromotionStep::Superseded => self.superseded = self.superseded.saturating_add(1),
            EcPromotionStep::Retrying => self.retried = self.retried.saturating_add(1),
            EcPromotionStep::Failed => self.failed = self.failed.saturating_add(1),
            EcPromotionStep::WaitingForGrace => {}
        }
    }
}

/// Stable name of a step, for reports and logs.
fn step_name(step: EcPromotionStep) -> &'static str {
    match step {
        EcPromotionStep::HotRestored => "hot_restored",
        EcPromotionStep::ManifestPromoted => "manifest_promoted",
        EcPromotionStep::Completed => "completed",
        EcPromotionStep::WaitingForGrace => "waiting_for_grace",
        EcPromotionStep::Superseded => "superseded",
        EcPromotionStep::Retrying => "retrying",
        EcPromotionStep::Failed => "failed",
    }
}

/// The background reverse migration worker.
pub struct EcPromotionWorker {
    config: EcPromotionWorkerConfig,
    environment: Arc<dyn EcPromotionEnvironment>,
}

impl EcPromotionWorker {
    /// Build a worker over the given environment.
    pub fn new(
        config: EcPromotionWorkerConfig,
        environment: Arc<dyn EcPromotionEnvironment>,
    ) -> Self {
        Self {
            config,
            environment,
        }
    }

    /// Run cycles until the task is aborted by the server lifecycle.
    ///
    /// The pause comes first for the same reason it does in the forward worker:
    /// a node starts this while the rest of the cluster is still coming up, and
    /// the first step of any job is a quorum read that nobody can answer yet.
    pub async fn run(self) {
        loop {
            tokio::time::sleep(self.config.poll_interval).await;

            match self.run_once(unix_time_nanos()).await {
                Ok(report) if report.examined > 0 => {
                    tracing::debug!(
                        examined = report.examined,
                        restored = report.restored,
                        promoted = report.promoted,
                        completed = report.completed,
                        "EC promotion cycle finished"
                    );
                }
                Ok(_) => {}
                Err(error) => {
                    tracing::warn!(error = %error, "EC promotion cycle failed");
                }
            }
        }
    }

    /// Advance every ready job by one step.
    ///
    /// One step per cycle on purpose. A promotion decodes a whole object and
    /// writes three copies of it, and the grace period between verification and
    /// purge is measured in hours anyway; draining a job to completion inside
    /// one cycle would only make the slowest step block every other job.
    ///
    /// Reading the copy back and publishing count as one step. Publishing is
    /// the switch that sends readers to the copy, and it has to rest on a check
    /// made just before it rather than one a pass ago (EC2-026).
    pub async fn run_once(&self, now: u64) -> Result<EcPromotionCycleReport, ClusterError> {
        let mut report = EcPromotionCycleReport::default();
        if !self.config.enabled {
            return Ok(report);
        }

        let jobs = self.environment.ready_jobs(now, self.config.batch_size).await?;
        for job in jobs {
            report.examined = report.examined.saturating_add(1);
            let step = match self.advance(&job, now).await {
                Ok(step) => step,
                // The step itself failed. The object is still readable through
                // whichever layout it currently has, so this is a retry, never
                // an incident.
                Err(error) => self.record_failure(job.clone(), error.to_string(), now).await?,
            };
            report.record(step, &job);
        }

        // Runs whether or not this node had a job of its own. The promotions in
        // the queue above are the ones this node is carrying out; the sweep is
        // about the ones some other node already carried out, whose shards are
        // sitting here.
        report.swept_shards = self.environment.sweep_promoted_shards(now).await?;

        ec_metrics::set_promotion_queue_depth(report.examined);
        Ok(report)
    }

    /// Take one job one step further.
    async fn advance(
        &self,
        job: &EcPromotionJobRecord,
        now: u64,
    ) -> Result<EcPromotionStep, ClusterError> {
        match job.state {
            EcPromotionState::Queued => self.restore(job, now).await,
            EcPromotionState::HotRestored => self.verify(job, now).await,
            EcPromotionState::HotVerified => self.publish(job, now).await,
            EcPromotionState::ManifestPromoted => self.purge(job, now).await,
            // Terminal states are never handed out by `ready_jobs`; reaching
            // one here means the queue and the worker disagree, and the queue
            // wins.
            EcPromotionState::Completed
            | EcPromotionState::FailedPermanent
            | EcPromotionState::Superseded => Ok(EcPromotionStep::Failed),
        }
    }

    /// Decode the object and put its bytes back on the replicas.
    async fn restore(
        &self,
        job: &EcPromotionJobRecord,
        now: u64,
    ) -> Result<EcPromotionStep, ClusterError> {
        let Some(data) = self.environment.read_erasure_coded(job).await? else {
            return self.supersede(job).await;
        };
        // After the decode, so that a promotion which cannot read the object
        // never marks it; before the write, so that no node can see a restored
        // copy without also seeing that it belongs to the object.
        if !self.environment.mark_promotion_in_flight(job).await? {
            return self.supersede(job).await;
        }
        if !self.environment.restore_hot_copy(job, &data).await? {
            return self.supersede(job).await;
        }
        self.commit(job, EcPromotionState::HotRestored, now).await?;
        tracing::debug!(
            bucket = %job.object.bucket,
            key = %job.object.key,
            size = job.object_size,
            "EC promotion restored the RF=3 copy"
        );
        ec_metrics::record_promotion_restored(job.object_size);
        Ok(EcPromotionStep::HotRestored)
    }

    /// Read the restored copy back, and switch readers to it straight away.
    ///
    /// The verified state is stored before the switch, so a node that dies
    /// between the two resumes at the switch — which reads the copy again
    /// first, see [`publish`](Self::publish).
    async fn verify(
        &self,
        job: &EcPromotionJobRecord,
        now: u64,
    ) -> Result<EcPromotionStep, ClusterError> {
        if !self.environment.verify_hot_copy(job).await? {
            return self.restore_again(job, now).await;
        }
        let verified = self.commit(job, EcPromotionState::HotVerified, now).await?;
        self.switch_readers(&verified, now).await
    }

    /// Resume a job that stopped between reading its copy back and switching.
    ///
    /// The check that job made belongs to an earlier pass, and the switch is
    /// the step that makes the copy the only one readers use. It is made again.
    async fn publish(
        &self,
        job: &EcPromotionJobRecord,
        now: u64,
    ) -> Result<EcPromotionStep, ClusterError> {
        if !self.environment.verify_hot_copy(job).await? {
            return self.restore_again(job, now).await;
        }
        self.switch_readers(job, now).await
    }

    /// Send a job whose restored copy did not read back to the restore step.
    ///
    /// Checking again would find the same thing: the bytes are either gone or
    /// wrong, and nothing but the restore puts them back. The restore is safe
    /// to repeat — a replica that already holds this generation's bytes is left
    /// as it is — and the attempt counts against the job's budget, so a copy
    /// that never reads back still ends in a permanent failure rather than a
    /// loop.
    async fn restore_again(
        &self,
        job: &EcPromotionJobRecord,
        now: u64,
    ) -> Result<EcPromotionStep, ClusterError> {
        let mut again = job.clone();
        again.state = EcPromotionState::Queued;
        self.record_failure(
            again,
            "the restored RF=3 copy did not read back as the object".to_string(),
            now,
        )
        .await
    }

    /// Switch every node in the pool to the hot copy.
    async fn switch_readers(
        &self,
        job: &EcPromotionJobRecord,
        now: u64,
    ) -> Result<EcPromotionStep, ClusterError> {
        if !self.environment.publish_promoted_manifest(job).await? {
            return self.supersede(job).await;
        }

        let mut promoted = job.clone();
        promoted.state = EcPromotionState::ManifestPromoted;
        // The clock on the shards starts here, not at verification: what the
        // grace period protects against is a node that has not yet seen the
        // promoted manifest, and no node could have seen it before now.
        promoted.shards_eligible_at =
            Some(now.saturating_add(duration_to_nanos(self.config.shard_grace)));
        promoted.attempt = 0;
        promoted.next_retry_at = None;
        promoted.failure = None;
        promoted.updated_at = now;
        self.environment.save_job(&promoted).await?;

        // Counted at the moment readers actually move, which is the point at
        // which the object stops being erasure coded as far as anything outside
        // this worker can tell.
        self.environment.record_migration(&job.object, now).await?;
        tracing::info!(
            bucket = %job.object.bucket,
            key = %job.object.key,
            manual = job.origin.is_manual(),
            "EC promotion switched the object back to RF=3"
        );
        ec_metrics::record_promotion_committed();
        Ok(EcPromotionStep::ManifestPromoted)
    }

    /// Remove the shards once they have outlived their grace period.
    async fn purge(
        &self,
        job: &EcPromotionJobRecord,
        now: u64,
    ) -> Result<EcPromotionStep, ClusterError> {
        if job.shards_eligible_at.is_none_or(|eligible_at| now < eligible_at) {
            return Ok(EcPromotionStep::WaitingForGrace);
        }
        if !self.environment.purge_erasure_coded_copy(job).await? {
            return Ok(EcPromotionStep::WaitingForGrace);
        }
        self.environment.delete_job(&job.object).await?;
        ec_metrics::record_promotion_shards_purged();
        Ok(EcPromotionStep::Completed)
    }

    /// Drop a promotion the client took away from it.
    async fn supersede(&self, job: &EcPromotionJobRecord) -> Result<EcPromotionStep, ClusterError> {
        // The job is forgotten rather than kept as a tombstone: a client write
        // is the ordinary outcome of racing a background migration, and every
        // one of them would otherwise leave a row behind. The counter is what
        // an operator watches instead.
        self.environment.delete_job(&job.object).await?;
        tracing::debug!(
            bucket = %job.object.bucket,
            key = %job.object.key,
            "EC promotion abandoned: the object changed"
        );
        ec_metrics::record_promotion_superseded();
        Ok(EcPromotionStep::Superseded)
    }

    /// Store a job at its new step, clearing any failure it carried.
    ///
    /// Except on the way into `HotRestored`, where the attempts and the reason
    /// for the last one are kept. A copy that did not read back sends its job
    /// back to the restore, and a restore that succeeds says nothing about
    /// whether the copy will read back this time; clearing the count there
    /// would let a copy that never reads back go round the two steps for ever.
    /// The count is cleared once the copy has read back.
    async fn commit(
        &self,
        job: &EcPromotionJobRecord,
        state: EcPromotionState,
        now: u64,
    ) -> Result<EcPromotionJobRecord, ClusterError> {
        let mut advanced = job.clone();
        advanced.state = state;
        if state != EcPromotionState::HotRestored {
            advanced.attempt = 0;
            advanced.failure = None;
        }
        advanced.next_retry_at = None;
        advanced.updated_at = now;
        self.environment.save_job(&advanced).await?;
        Ok(advanced)
    }

    /// Record a failed step, and give up once the budget is spent.
    ///
    /// The state the job carries is kept. A job that failed while purging
    /// shards must resume at the purge, not decode the object all over again;
    /// the one exception, a copy that did not read back, is sent back to the
    /// restore by its caller.
    async fn record_failure(
        &self,
        mut job: EcPromotionJobRecord,
        reason: String,
        now: u64,
    ) -> Result<EcPromotionStep, ClusterError> {
        job.attempt = job.attempt.saturating_add(1);
        job.failure = Some(reason.clone());
        job.updated_at = now;

        if job.attempt >= self.config.max_retry_attempts {
            job.state = EcPromotionState::FailedPermanent;
            job.next_retry_at = None;
            self.environment.save_job(&job).await?;
            tracing::warn!(
                bucket = %job.object.bucket,
                key = %job.object.key,
                attempts = job.attempt,
                reason,
                "EC promotion gave up; the object is still readable and unchanged"
            );
            ec_metrics::record_promotion_failed();
            return Ok(EcPromotionStep::Failed);
        }

        let backoff = duration_to_nanos(self.config.retry_backoff)
            .saturating_mul(u64::from(job.attempt.min(MAX_BACKOFF_MULTIPLIER)));
        job.next_retry_at = Some(now.saturating_add(backoff));
        self.environment.save_job(&job).await?;
        // Warned, not debugged. A promotion that is failing is holding the
        // object on two full copies of itself until it either finishes or is
        // given up on, and the retry budget bounds how many of these one job
        // can produce.
        tracing::warn!(
            bucket = %job.object.bucket,
            key = %job.object.key,
            attempt = job.attempt,
            reason,
            "EC promotion step failed and will be retried"
        );
        Ok(EcPromotionStep::Retrying)
    }
}

/// How far the linear retry backoff may grow.
///
/// Linear rather than doubling: a promotion that keeps failing is usually
/// waiting on a peer, and an hour-long pause would leave the object sitting on
/// two full copies of itself for no reason.
const MAX_BACKOFF_MULTIPLIER: u32 = 10;

fn duration_to_nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
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

    use s4_cluster::HlcTimestamp;
    use uuid::Uuid;

    use crate::ec_metadata::EcPromotionOrigin;

    const SECOND: u64 = 1_000_000_000;
    const HOUR: u64 = 3_600 * SECOND;
    const NOW: u64 = 1_000 * HOUR;

    /// A promotion queue and a pool held in memory, with no cluster behind them.
    #[derive(Default)]
    struct FakeEnvironment {
        state: Mutex<FakeState>,
    }

    #[derive(Default)]
    struct FakeState {
        jobs: HashMap<String, EcPromotionJobRecord>,
        /// Bytes each object decodes to, when it decodes at all.
        shards: HashMap<String, Vec<u8>>,
        decode_fails: bool,
        /// The pool no longer holds the generation, so the mark is refused.
        mark_refused: bool,
        guard_broken: bool,
        verify_succeeds: bool,
        publish_succeeds: bool,
        purge_succeeds: bool,
        restored: HashMap<String, Vec<u8>>,
        purged: Vec<String>,
        migrations: Vec<(String, u64)>,
        swept: usize,
        /// Order of the calls, so that the gates can be asserted on.
        calls: Vec<&'static str>,
    }

    impl FakeEnvironment {
        fn with_job(job: EcPromotionJobRecord, body: &[u8]) -> Arc<Self> {
            let environment = Self {
                state: Mutex::new(FakeState {
                    verify_succeeds: true,
                    publish_succeeds: true,
                    purge_succeeds: true,
                    ..FakeState::default()
                }),
            };
            {
                let mut state = environment.state.lock().expect("fake state");
                state.shards.insert(job.object.key.clone(), body.to_vec());
                state.jobs.insert(job.object.key.clone(), job);
            }
            Arc::new(environment)
        }

        fn job(&self, key: &str) -> Option<EcPromotionJobRecord> {
            self.state.lock().expect("fake state").jobs.get(key).cloned()
        }

        fn calls(&self) -> Vec<&'static str> {
            self.state.lock().expect("fake state").calls.clone()
        }

        /// The calls a job's own pipeline made, without the per-cycle sweep.
        ///
        /// The sweep runs once per cycle whatever the jobs are doing, so a test
        /// about the order of one job's steps has to look past it.
        fn job_calls(&self) -> Vec<&'static str> {
            self.calls().into_iter().filter(|call| *call != "sweep").collect()
        }

        fn purged(&self) -> Vec<String> {
            self.state.lock().expect("fake state").purged.clone()
        }

        fn migrations(&self) -> Vec<(String, u64)> {
            self.state.lock().expect("fake state").migrations.clone()
        }

        fn set(&self, mutate: impl FnOnce(&mut FakeState)) {
            mutate(&mut self.state.lock().expect("fake state"));
        }
    }

    #[async_trait]
    impl EcPromotionEnvironment for FakeEnvironment {
        async fn ready_jobs(
            &self,
            now: u64,
            limit: usize,
        ) -> Result<Vec<EcPromotionJobRecord>, ClusterError> {
            let state = self.state.lock().expect("fake state");
            let mut ready: Vec<EcPromotionJobRecord> = state
                .jobs
                .values()
                .filter(|job| job.state.is_in_flight())
                .filter(|job| job.next_retry_at.is_none_or(|retry_at| retry_at <= now))
                .cloned()
                .collect();
            ready.sort_by(|left, right| left.object.key.cmp(&right.object.key));
            ready.truncate(limit);
            Ok(ready)
        }

        async fn save_job(&self, job: &EcPromotionJobRecord) -> Result<(), ClusterError> {
            self.set(|state| {
                state.jobs.insert(job.object.key.clone(), job.clone());
            });
            Ok(())
        }

        async fn delete_job(&self, object: &EcObjectVersionKey) -> Result<(), ClusterError> {
            self.set(|state| {
                state.jobs.remove(&object.key);
            });
            Ok(())
        }

        async fn read_erasure_coded(
            &self,
            job: &EcPromotionJobRecord,
        ) -> Result<Option<Vec<u8>>, ClusterError> {
            let mut state = self.state.lock().expect("fake state");
            state.calls.push("read");
            if state.decode_fails {
                return Err(ClusterError::Storage("shards unavailable".to_string()));
            }
            Ok(state.shards.get(&job.object.key).cloned())
        }

        async fn mark_promotion_in_flight(
            &self,
            _job: &EcPromotionJobRecord,
        ) -> Result<bool, ClusterError> {
            let mut state = self.state.lock().expect("fake state");
            state.calls.push("mark");
            Ok(!state.mark_refused)
        }

        async fn restore_hot_copy(
            &self,
            job: &EcPromotionJobRecord,
            data: &[u8],
        ) -> Result<bool, ClusterError> {
            let mut state = self.state.lock().expect("fake state");
            state.calls.push("restore");
            if state.guard_broken {
                return Ok(false);
            }
            state.restored.insert(job.object.key.clone(), data.to_vec());
            Ok(true)
        }

        async fn verify_hot_copy(&self, job: &EcPromotionJobRecord) -> Result<bool, ClusterError> {
            let mut state = self.state.lock().expect("fake state");
            state.calls.push("verify");
            Ok(state.verify_succeeds && state.restored.contains_key(&job.object.key))
        }

        async fn publish_promoted_manifest(
            &self,
            _job: &EcPromotionJobRecord,
        ) -> Result<bool, ClusterError> {
            let mut state = self.state.lock().expect("fake state");
            state.calls.push("publish");
            Ok(state.publish_succeeds)
        }

        async fn purge_erasure_coded_copy(
            &self,
            job: &EcPromotionJobRecord,
        ) -> Result<bool, ClusterError> {
            let mut state = self.state.lock().expect("fake state");
            state.calls.push("purge");
            if !state.purge_succeeds {
                return Ok(false);
            }
            state.purged.push(job.object.key.clone());
            Ok(true)
        }

        async fn record_migration(
            &self,
            object: &EcObjectVersionKey,
            now: u64,
        ) -> Result<(), ClusterError> {
            self.set(|state| state.migrations.push((object.key.clone(), now)));
            Ok(())
        }

        async fn sweep_promoted_shards(&self, _now: u64) -> Result<usize, ClusterError> {
            let mut state = self.state.lock().expect("fake state");
            state.calls.push("sweep");
            Ok(state.swept)
        }
    }

    fn job(key: &str) -> EcPromotionJobRecord {
        EcPromotionJobRecord {
            object: EcObjectVersionKey::new("bucket", key, None),
            operation_id: Uuid::from_u128(7),
            source_hlc: HlcTimestamp::from_record(NOW / 1_000_000, 0, 1),
            erasure_set_id: 1,
            state: EcPromotionState::Queued,
            attempt: 0,
            object_size: 11,
            content_hash_sha256: [3u8; 32],
            origin: EcPromotionOrigin::Automatic,
            shards_eligible_at: None,
            updated_at: NOW,
            next_retry_at: None,
            failure: None,
        }
    }

    fn config() -> EcPromotionWorkerConfig {
        EcPromotionWorkerConfig {
            enabled: true,
            poll_interval: Duration::from_secs(30),
            batch_size: 8,
            shard_grace: Duration::from_secs(3_600),
            max_retry_attempts: 3,
            retry_backoff: Duration::from_secs(30),
        }
    }

    fn worker(environment: Arc<FakeEnvironment>) -> EcPromotionWorker {
        EcPromotionWorker::new(config(), environment)
    }

    /// The whole pipeline, in the order EC2-023 fixes.
    ///
    /// The assertion that matters is the call order: the shards must not be
    /// touched until the restored copy has been read back and has outlived its
    /// grace period.
    #[tokio::test]
    async fn a_promotion_runs_its_gates_in_order() {
        let environment = FakeEnvironment::with_job(job("report.bin"), b"hello world");
        let worker = worker(Arc::clone(&environment));

        assert_eq!(worker.run_once(NOW).await.unwrap().restored, 1);
        assert_eq!(
            environment.job("report.bin").unwrap().state,
            EcPromotionState::HotRestored
        );

        // The copy is read back and readers are switched to it in one pass:
        // the switch rests on a check made just before it.
        let report = worker.run_once(NOW + SECOND).await.unwrap();
        assert_eq!(report.promoted, 1);
        let promoted = environment.job("report.bin").unwrap();
        assert_eq!(promoted.state, EcPromotionState::ManifestPromoted);
        assert_eq!(
            promoted.shards_eligible_at,
            Some(NOW + SECOND + HOUR),
            "the grace period starts when readers move"
        );

        // The shards outlive the switch: a node that has not seen the promoted
        // manifest yet is still reading them.
        let report = worker.run_once(NOW + 2 * SECOND).await.unwrap();
        assert_eq!(report.completed, 0);
        assert!(environment.purged().is_empty());

        let report = worker.run_once(NOW + 2 * HOUR).await.unwrap();
        assert_eq!(report.completed, 1);
        assert_eq!(environment.purged(), vec!["report.bin".to_string()]);
        assert!(environment.job("report.bin").is_none());

        assert_eq!(
            environment.job_calls(),
            vec!["read", "mark", "restore", "verify", "publish", "purge"]
        );
    }

    /// The object is marked after it decodes and before any byte comes back.
    ///
    /// The mark is the only thing that tells a restored copy apart from a hot
    /// copy an unfinished purge left behind, and namespace reconciliation
    /// releases the second kind on any node. A copy written before the mark
    /// could be released in between; a mark placed before the decode would
    /// flag an object this promotion cannot even read (EC2-026).
    #[tokio::test]
    async fn the_object_is_marked_after_it_decodes_and_before_its_copy_is_written() {
        let environment = FakeEnvironment::with_job(job("report.bin"), b"hello world");
        let worker = worker(Arc::clone(&environment));

        worker.run_once(NOW).await.unwrap();

        assert_eq!(environment.job_calls(), vec!["read", "mark", "restore"]);
    }

    /// A promotion that cannot read the object leaves no mark behind.
    ///
    /// The mark cannot be taken back at the same generation, so an object
    /// marked by a promotion that never got its bytes would keep it until
    /// somebody resumed the promotion.
    #[tokio::test]
    async fn a_promotion_that_cannot_decode_never_marks_the_object() {
        let environment = FakeEnvironment::with_job(job("report.bin"), b"hello world");
        environment.set(|state| state.decode_fails = true);
        let worker = worker(Arc::clone(&environment));

        worker.run_once(NOW).await.unwrap();
        worker.run_once(NOW + HOUR).await.unwrap();

        assert!(!environment.calls().contains(&"mark"));
        assert!(!environment.calls().contains(&"restore"));
    }

    /// A mark the pool refuses means the generation is gone, and nothing is
    /// written back over whatever replaced it.
    #[tokio::test]
    async fn a_refused_mark_drops_the_promotion_without_writing_anything() {
        let environment = FakeEnvironment::with_job(job("report.bin"), b"hello world");
        environment.set(|state| state.mark_refused = true);
        let worker = worker(Arc::clone(&environment));

        let report = worker.run_once(NOW).await.unwrap();

        assert_eq!(report.superseded, 1);
        assert!(environment.job("report.bin").is_none());
        assert!(!environment.calls().contains(&"restore"));
    }

    /// A copy that disappeared before it was read back is written again.
    ///
    /// Checking again would only find it missing again: the restore is the one
    /// step that puts bytes back. This is the second line of defence behind the
    /// mark, for anything that took the copy away regardless (EC2-026).
    #[tokio::test]
    async fn a_copy_that_vanished_before_it_was_read_back_is_written_again() {
        let environment = FakeEnvironment::with_job(job("report.bin"), b"hello world");
        let worker = worker(Arc::clone(&environment));

        worker.run_once(NOW).await.unwrap();
        environment.set(|state| {
            state.restored.remove("report.bin");
        });

        let report = worker.run_once(NOW + SECOND).await.unwrap();
        assert_eq!(report.retried, 1);
        assert_eq!(report.promoted, 0);
        let again = environment.job("report.bin").unwrap();
        assert_eq!(again.state, EcPromotionState::Queued);
        assert_eq!(again.attempt, 1);

        worker.run_once(NOW + HOUR).await.unwrap();
        let restored = environment.job("report.bin").unwrap();
        assert_eq!(restored.state, EcPromotionState::HotRestored);
        assert_eq!(
            restored.attempt, 1,
            "a restore says nothing about whether the copy will read back"
        );

        let report = worker.run_once(NOW + HOUR + SECOND).await.unwrap();
        assert_eq!(report.promoted, 1);
        let promoted = environment.job("report.bin").unwrap();
        assert_eq!(promoted.state, EcPromotionState::ManifestPromoted);
        assert_eq!(promoted.attempt, 0);
        assert_eq!(promoted.failure, None);

        assert_eq!(
            environment.job_calls(),
            vec![
                "read", "mark", "restore", "verify", "read", "mark", "restore", "verify", "publish"
            ]
        );
    }

    /// A job that stopped between reading its copy back and switching readers
    /// reads it again before it switches.
    ///
    /// The earlier check belongs to a pass that may be long gone, and the
    /// switch is what makes the copy the only one readers use.
    #[tokio::test]
    async fn a_switch_resumed_after_a_stop_reads_the_copy_again_first() {
        let mut verified = job("report.bin");
        verified.state = EcPromotionState::HotVerified;
        let environment = FakeEnvironment::with_job(verified, b"hello world");
        let worker = worker(Arc::clone(&environment));

        // The copy is not there: the switch does not happen.
        let report = worker.run_once(NOW).await.unwrap();
        assert_eq!(report.promoted, 0);
        assert_eq!(
            environment.job("report.bin").unwrap().state,
            EcPromotionState::Queued
        );
        assert_eq!(environment.job_calls(), vec!["verify"]);

        // Restored again, it reads back, and the switch follows the check.
        worker.run_once(NOW + HOUR).await.unwrap();
        let report = worker.run_once(NOW + HOUR + SECOND).await.unwrap();
        assert_eq!(report.promoted, 1);
        assert_eq!(
            environment.job_calls(),
            vec!["verify", "read", "mark", "restore", "verify", "publish"]
        );
    }

    /// A promotion that cannot confirm its own copy never removes the shards.
    ///
    /// This is the gate that keeps a failed reverse migration from becoming
    /// data loss: until the RF=3 copy reads back, the shards are the only copy
    /// known to work, and they stay.
    ///
    /// Each failed check sends the job back to write the copy again, and each
    /// of those counts against the budget, so a copy that never reads back
    /// still ends in a permanent failure rather than going round for ever.
    #[tokio::test]
    async fn shards_survive_a_copy_that_does_not_read_back() {
        let environment = FakeEnvironment::with_job(job("report.bin"), b"hello world");
        environment.set(|state| state.verify_succeeds = false);
        let worker = worker(Arc::clone(&environment));

        worker.run_once(NOW).await.unwrap();
        for pass in 1..=8u64 {
            worker.run_once(NOW + pass * HOUR).await.unwrap();
        }

        assert!(environment.purged().is_empty());
        assert!(!environment.calls().contains(&"publish"));
        assert_eq!(
            environment.job("report.bin").unwrap().state,
            EcPromotionState::FailedPermanent
        );
        assert_eq!(
            environment.job_calls().iter().filter(|call| **call == "restore").count(),
            3,
            "one restore per attempt the budget allows"
        );
    }

    /// An object that stopped being erasure coded takes its promotion with it.
    ///
    /// Deleted, or already brought back by another node: either way there is
    /// nothing to decode, and retrying until the budget runs out would only
    /// delay the queue entry's removal.
    #[tokio::test]
    async fn an_object_no_longer_in_the_pool_drops_its_promotion() {
        let environment = FakeEnvironment::with_job(job("report.bin"), b"hello world");
        environment.set(|state| {
            state.shards.clear();
        });
        let worker = worker(Arc::clone(&environment));

        let report = worker.run_once(NOW).await.unwrap();

        assert_eq!(report.superseded, 1);
        assert_eq!(report.retried, 0, "this is not a transient failure");
        assert!(environment.job("report.bin").is_none());
        assert!(!environment.calls().contains(&"mark"));
    }

    /// A client write during the promotion wins, and takes the job with it.
    #[tokio::test]
    async fn a_client_write_supersedes_the_promotion() {
        let environment = FakeEnvironment::with_job(job("report.bin"), b"hello world");
        environment.set(|state| state.guard_broken = true);
        let worker = worker(Arc::clone(&environment));

        let report = worker.run_once(NOW).await.unwrap();

        assert_eq!(report.superseded, 1);
        assert!(environment.job("report.bin").is_none());
        assert!(environment.purged().is_empty());
        assert!(environment.migrations().is_empty());
    }

    /// A failed step is retried from where it stopped, not from the beginning.
    ///
    /// Decoding an object again to re-run a purge would cost the whole read a
    /// second time, on an object the pool has already paid to decode once.
    #[tokio::test]
    async fn a_retry_resumes_at_the_step_that_failed() {
        let environment = FakeEnvironment::with_job(job("report.bin"), b"hello world");
        environment.set(|state| state.purge_succeeds = false);
        let worker = worker(Arc::clone(&environment));

        worker.run_once(NOW).await.unwrap();
        worker.run_once(NOW + SECOND).await.unwrap();
        worker.run_once(NOW + 2 * SECOND).await.unwrap();
        let before = environment.job_calls().len();

        worker.run_once(NOW + 2 * HOUR).await.unwrap();
        worker.run_once(NOW + 3 * HOUR).await.unwrap();

        assert_eq!(
            environment.job_calls()[before..],
            ["purge", "purge"],
            "a retry must not decode the object again"
        );
        assert_eq!(
            environment.job("report.bin").unwrap().state,
            EcPromotionState::ManifestPromoted
        );
    }

    /// A transient failure keeps the job, backs it off, and gives up in the end.
    #[tokio::test]
    async fn a_failing_job_backs_off_and_is_eventually_given_up_on() {
        let environment = FakeEnvironment::with_job(job("report.bin"), b"hello world");
        environment.set(|state| state.decode_fails = true);
        let worker = worker(Arc::clone(&environment));

        let report = worker.run_once(NOW).await.unwrap();
        assert_eq!(report.retried, 1);
        let failing = environment.job("report.bin").unwrap();
        assert_eq!(failing.attempt, 1);
        assert_eq!(failing.next_retry_at, Some(NOW + 30 * SECOND));
        assert!(failing.failure.is_some());

        // Before the backoff elapses the job is not handed out at all.
        assert_eq!(worker.run_once(NOW + SECOND).await.unwrap().examined, 0);

        worker.run_once(NOW + HOUR).await.unwrap();
        let report = worker.run_once(NOW + 2 * HOUR).await.unwrap();

        assert_eq!(report.failed, 1);
        let given_up = environment.job("report.bin").unwrap();
        assert_eq!(given_up.state, EcPromotionState::FailedPermanent);
        assert_eq!(given_up.next_retry_at, None);
        // Nothing was removed and nothing was switched: the object is exactly
        // where it was before the promotion was attempted.
        assert!(environment.purged().is_empty());
        assert!(environment.migrations().is_empty());
    }

    /// The migration is counted when readers move, which is what the pause
    /// between two migrations of one object is measured from.
    #[tokio::test]
    async fn the_migration_is_counted_at_the_moment_readers_move() {
        let environment = FakeEnvironment::with_job(job("report.bin"), b"hello world");
        let worker = worker(Arc::clone(&environment));

        worker.run_once(NOW).await.unwrap();
        assert!(environment.migrations().is_empty());

        worker.run_once(NOW + SECOND).await.unwrap();
        assert_eq!(
            environment.migrations(),
            vec![("report.bin".to_string(), NOW + SECOND)]
        );
    }

    /// Every node releases its own shards, whoever carried the promotion out.
    ///
    /// A promotion is run by one node, but the shards are spread across the
    /// set, and only the node holding one can release it. Without a sweep on
    /// every node the other five would keep their shards forever, and the
    /// migration would save nothing.
    #[tokio::test]
    async fn every_cycle_releases_shards_of_objects_promoted_elsewhere() {
        let environment = FakeEnvironment::with_job(job("report.bin"), b"hello world");
        environment.set(|state| {
            // Nothing of this node's own is in flight; the shards belong to an
            // object some other node moved back.
            state.jobs.clear();
            state.swept = 3;
        });
        let worker = worker(Arc::clone(&environment));

        let report = worker.run_once(NOW).await.unwrap();

        assert_eq!(report.examined, 0);
        assert_eq!(report.swept_shards, 3);
        assert_eq!(environment.calls(), vec!["sweep"]);
    }

    /// A disabled worker does nothing at all, including asking for work.
    #[tokio::test]
    async fn a_disabled_worker_touches_nothing() {
        let environment = FakeEnvironment::with_job(job("report.bin"), b"hello world");
        let worker = EcPromotionWorker::new(
            EcPromotionWorkerConfig {
                enabled: false,
                ..config()
            },
            Arc::clone(&environment) as Arc<dyn EcPromotionEnvironment>,
        );

        let report = worker.run_once(NOW).await.unwrap();

        assert_eq!(report, EcPromotionCycleReport::default());
        assert!(environment.calls().is_empty());
        assert_eq!(
            environment.job("report.bin").unwrap().state,
            EcPromotionState::Queued
        );
    }

    /// One cycle advances each job by one step, and no further.
    #[tokio::test]
    async fn a_cycle_advances_every_job_exactly_one_step() {
        let environment = FakeEnvironment::with_job(job("a.bin"), b"hello world");
        environment.set(|state| {
            state.shards.insert("b.bin".to_string(), b"hello world".to_vec());
            state.jobs.insert("b.bin".to_string(), job("b.bin"));
        });
        let worker = worker(Arc::clone(&environment));

        let report = worker.run_once(NOW).await.unwrap();

        assert_eq!(report.examined, 2);
        assert_eq!(report.restored, 2);
        assert_eq!(report.restored_bytes, 22);
        for key in ["a.bin", "b.bin"] {
            assert_eq!(
                environment.job(key).unwrap().state,
                EcPromotionState::HotRestored
            );
        }
    }
}
