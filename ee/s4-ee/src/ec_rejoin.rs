// Copyright 2026 S4Core Team
//
// Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

//! What a returning node is allowed to do before its shards are back.
//!
//! [`EcRepairCoordinator::evaluate_rejoin`](crate::ec_repair::EcRepairCoordinator::evaluate_rejoin)
//! has always known the answer and nothing ever asked it: until this module
//! existed, `S4_MAX_REJOIN_DOWNTIME_DAYS` and `S4_EC_LONG_OFFLINE_REJOIN_POLICY`
//! were documented settings with no effect anywhere in the product
//! (decision EC2-141). This is the other half — what asks the question, on what
//! evidence, and what the answer does.
//!
//! # What the decision is made of
//!
//! A node returning to a pool has exactly two kinds of local evidence about
//! itself, and this module uses both and nothing else:
//!
//! * **its own presence record** — written into the EC metadata store on every
//!   start and refreshed while it runs, so a node that came back with its
//!   storage can say in wall-clock terms how long it was away. That is the
//!   input `evaluate_rejoin` was written for, and it is what finally gives the
//!   downtime limit and its policy an effect. The same record carries whether
//!   the node still owed shards when it wrote it, so that restarting a node
//!   held back for a rebuild is not mistaken for rebuilding it.
//! * **where this start's identity came from** — an identity adopted from
//!   `S4_EXPECTED_NODE_IDENTITY` into a blank data directory is the operator
//!   saying this machine is standing in for a node that is gone (EC2-142). Its
//!   storage is empty by construction, so there is no downtime to measure and
//!   none is measured: the answer is already known.
//!
//! # What closes the gate, and what deliberately does not
//!
//! The gate closes only on **positive evidence** that this node is behind: an
//! adopted identity, a presence record showing an absence past the limit, or a
//! presence record saying the node still owed shards when it last wrote one.
//! Everything else keeps it open — a node that cannot prove it is behind is not
//! held back on suspicion, because holding a healthy node out of the read path
//! is itself an availability incident.
//!
//! Two consequences worth stating plainly, because both are chosen rather than
//! overlooked:
//!
//! * A brand new node in a brand new pool is not held. Its identity was
//!   generated in this start, so no pool can have shards under it.
//! * A replacement carried out by copying the sixteen bytes of `node_id` by
//!   hand is not recognised as one. Nothing local distinguishes it from the
//!   first start of a new node, and inventing a distinction would mean holding
//!   new nodes back. `S4_EXPECTED_NODE_IDENTITY` is the supported way to
//!   perform a replacement precisely because it says out loud what a copied
//!   file cannot.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::RwLock;

use s4_cluster::NodeIdentityOrigin;
use serde_json::{json, Value};

use crate::ec_metadata::EcNodePresenceRecord;
use crate::ec_repair::{EcNodeRejoinDecision, EcRepairCoordinator};

/// Environment variable that arms or disarms the gate.
pub const REJOIN_GATE_ENABLED_ENV: &str = "S4_EC_REJOIN_GATE_ENABLED";

/// What the local evidence about this node's return consists of.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EcNodeReturn {
    /// Where the identity this start runs under came from.
    pub identity_origin: NodeIdentityOrigin,
    /// This node's own record of serving the pool, if its storage kept one.
    pub last_participation: Option<EcNodePresenceRecord>,
    /// Whether the pool's layout gives this identity any slot to hold.
    pub owns_slots: bool,
}

/// Why the gate reached the decision it did.
///
/// Kept apart from the decision because the two answer different questions: the
/// decision says what the node may do, the reason says which piece of evidence
/// settled it. An operator reading "this node is not serving reads" needs the
/// second one to know what to do about it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EcRejoinReason {
    /// No evaluation has run: not a cluster EC node, or the pool has no sets.
    NotEvaluated,
    /// The pool assigns this identity no slot, so it owes the pool no shards.
    NoSlotsOwned,
    /// The identity was generated in this start; no pool can remember it.
    NewIdentity,
    /// Back with its storage, within the downtime limit.
    WithinDowntimeLimit,
    /// Back with its storage, after longer than the downtime limit.
    BeyondDowntimeLimit,
    /// Storage gone: this machine adopted the identity of a node that is gone.
    StorageAdopted,
    /// The node was still owing the pool shards when it was last running.
    RebuildPending,
    /// Storage is here but carries no record of how long the node was away.
    ///
    /// The state of a node upgraded from a build that did not keep the record.
    /// Its shards are where it left them, so it is not held.
    DowntimeUnrecorded,
}

impl EcRejoinReason {
    /// A stable label for logs, admin responses and metrics.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NotEvaluated => "not_evaluated",
            Self::NoSlotsOwned => "no_slots_owned",
            Self::NewIdentity => "new_identity",
            Self::WithinDowntimeLimit => "within_downtime_limit",
            Self::BeyondDowntimeLimit => "beyond_downtime_limit",
            Self::StorageAdopted => "storage_adopted",
            Self::RebuildPending => "rebuild_pending",
            Self::DowntimeUnrecorded => "downtime_unrecorded",
        }
    }
}

/// A stable label for a rejoin decision.
pub fn rejoin_decision_label(decision: EcNodeRejoinDecision) -> &'static str {
    match decision {
        EcNodeRejoinDecision::IncrementalRepairAllowed => "incremental_repair_allowed",
        EcNodeRejoinDecision::NeedsBootstrap => "needs_bootstrap",
        EcNodeRejoinDecision::AdminApprovalRequired => "admin_approval_required",
    }
}

/// Every decision label, so a per-decision gauge can publish zero on the ones
/// that do not hold rather than leaving them absent.
pub const REJOIN_DECISION_LABELS: [&str; 3] = [
    "incremental_repair_allowed",
    "needs_bootstrap",
    "admin_approval_required",
];

/// The verdict on this node's return, and everything it was reached from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EcRejoinStatus {
    /// What the node is allowed to do.
    pub decision: EcNodeRejoinDecision,
    /// Which piece of evidence settled it.
    pub reason: EcRejoinReason,
    /// Whether the verdict is acted on, or only reported.
    pub enforced: bool,
    /// Whether an operator has said this return is acceptable.
    pub admin_approved: bool,
    /// When the verdict was reached, in nanoseconds since the Unix epoch.
    pub evaluated_at: u64,
    /// When this node last recorded itself as serving the pool.
    pub last_participation_at: Option<u64>,
    /// How long it was away, when that is known.
    pub downtime_secs: Option<u64>,
}

impl EcRejoinStatus {
    /// The verdict of a node that has not evaluated its return.
    ///
    /// Open, because this is also the state of every node that is not an EC
    /// cluster member, and of every node before startup reaches the question.
    pub fn not_evaluated() -> Self {
        Self {
            decision: EcNodeRejoinDecision::IncrementalRepairAllowed,
            reason: EcRejoinReason::NotEvaluated,
            enforced: false,
            admin_approved: false,
            evaluated_at: 0,
            last_participation_at: None,
            downtime_secs: None,
        }
    }

    /// Whether this verdict lets the node serve EC reads.
    pub fn serves_ec_reads(&self) -> bool {
        !self.enforced
            || self.admin_approved
            || self.decision == EcNodeRejoinDecision::IncrementalRepairAllowed
    }

    /// Whether an operator opened a gate the decision itself keeps shut.
    ///
    /// Reported rather than folded into the decision: an override is a fact
    /// about who decided, and hiding it would leave the admin surface saying a
    /// node is whole when what happened is that someone said it was.
    pub fn admin_overrode_decision(&self) -> bool {
        self.admin_approved && self.decision != EcNodeRejoinDecision::IncrementalRepairAllowed
    }

    /// This verdict as the admin surface reports it.
    pub fn admin_snapshot(&self) -> Value {
        json!({
            "decision": rejoin_decision_label(self.decision),
            "reason": self.reason.as_str(),
            "serving_ec_reads": self.serves_ec_reads(),
            "enforced": self.enforced,
            "admin_approved": self.admin_approved,
            "admin_override": self.admin_overrode_decision(),
            "evaluated_at": self.evaluated_at,
            "last_participation_at": self.last_participation_at,
            "downtime_secs": self.downtime_secs,
        })
    }
}

/// Decide what a returning node may do, from the local evidence it has.
///
/// The time-based part of the answer is not reimplemented here: it is asked of
/// [`EcRepairCoordinator::evaluate_rejoin`], which owns the downtime limit and
/// the long-offline policy. This function decides only *what to ask it about* —
/// which is the whole of what was missing (decision EC2-141).
pub fn decide_rejoin(
    repair: &EcRepairCoordinator,
    facts: &EcNodeReturn,
    now: u64,
    admin_approved: bool,
    enforced: bool,
) -> EcRejoinStatus {
    let last_participation_at = facts.last_participation.as_ref().map(|record| record.last_seen_at);
    let base = EcRejoinStatus {
        decision: EcNodeRejoinDecision::IncrementalRepairAllowed,
        reason: EcRejoinReason::NotEvaluated,
        enforced,
        admin_approved,
        evaluated_at: now,
        last_participation_at,
        downtime_secs: last_participation_at.map(|seen| nanos_to_secs(now.saturating_sub(seen))),
    };

    // A node the pool gives no slot holds none of its shards, so there is
    // nothing for it to be behind on. This is also the answer for a machine
    // that came back with a fresh identity: the pool does not know that name.
    if !facts.owns_slots {
        return EcRejoinStatus {
            reason: EcRejoinReason::NoSlotsOwned,
            ..base
        };
    }

    // An identity invented one moment ago cannot be one the pool has shards
    // under, whatever the state of this machine's disks.
    if matches!(facts.identity_origin, NodeIdentityOrigin::Generated) {
        return EcRejoinStatus {
            reason: EcRejoinReason::NewIdentity,
            ..base
        };
    }

    // The node was already held back when it last wrote its record, and a
    // restart is not a rebuild. Checked before the clock, because the record is
    // seconds old and the obligation it carries is not.
    if facts.last_participation.as_ref().is_some_and(|record| record.rebuild_pending) {
        return EcRejoinStatus {
            decision: repair.evaluate_rejoin(0, now, admin_approved),
            reason: EcRejoinReason::RebuildPending,
            ..base
        };
    }

    // The operator said this machine takes over a node that is gone, and a
    // blank data directory is what made that possible. Its downtime is not
    // unknown by accident — there is no storage left that could have recorded
    // it — so it is not asked for. EC2-141 gives this two independent grounds:
    // a repair with nothing to increment is a rebuild, and a replacement has no
    // storage that could have recorded when it was last up.
    if facts.identity_origin.is_adopted() {
        return EcRejoinStatus {
            decision: repair.evaluate_rejoin(0, now, admin_approved),
            reason: EcRejoinReason::StorageAdopted,
            ..base
        };
    }

    // Storage is here. What it remembers about its own absence decides.
    let Some(record) = facts.last_participation.as_ref() else {
        return EcRejoinStatus {
            reason: EcRejoinReason::DowntimeUnrecorded,
            ..base
        };
    };

    let decision = repair.evaluate_rejoin(record.last_seen_at, now, admin_approved);
    let reason = if decision == EcNodeRejoinDecision::IncrementalRepairAllowed && !admin_approved {
        EcRejoinReason::WithinDowntimeLimit
    } else {
        EcRejoinReason::BeyondDowntimeLimit
    };
    EcRejoinStatus {
        decision,
        reason,
        ..base
    }
}

/// Whether the gate acts on its verdict, from the environment.
///
/// Off leaves the verdict computed and reported and stops it holding anything
/// back, which is what makes the whole of this plan reversible by unsetting one
/// variable — and what lets an acceptance run prove the gate is what refuses a
/// read, by watching the same scenario pass with it off.
pub fn rejoin_gate_enforced_from_env() -> bool {
    std::env::var(REJOIN_GATE_ENABLED_ENV)
        .map(|value| {
            let value = value.trim().to_ascii_lowercase();
            value != "false" && value != "0"
        })
        .unwrap_or(true)
}

/// The verdict and the evidence it was reached from, held together.
#[derive(Debug, Clone)]
struct EcRejoinState {
    status: EcRejoinStatus,
    /// What the verdict was decided on, kept so that an operator's approval
    /// re-decides the same return rather than a fresher one.
    evidence: Option<EcNodeReturn>,
}

/// This node's standing verdict about its own return.
///
/// Read on every EC read, so the common answer costs one atomic load and never
/// takes the lock.
#[derive(Debug)]
pub struct EcRejoinGate {
    serving: AtomicBool,
    state: RwLock<EcRejoinState>,
}

impl Default for EcRejoinGate {
    fn default() -> Self {
        Self::new()
    }
}

impl EcRejoinGate {
    /// A gate that has not evaluated anything and holds nothing back.
    pub fn new() -> Self {
        Self {
            serving: AtomicBool::new(true),
            state: RwLock::new(EcRejoinState {
                status: EcRejoinStatus::not_evaluated(),
                evidence: None,
            }),
        }
    }

    /// Whether this node may serve EC reads right now.
    pub fn serves_ec_reads(&self) -> bool {
        self.serving.load(Ordering::Acquire)
    }

    /// The current verdict in full.
    pub fn status(&self) -> EcRejoinStatus {
        self.read_state().status
    }

    /// Decide this node's return from `evidence` and act on the answer.
    pub fn evaluate(
        &self,
        repair: &EcRepairCoordinator,
        evidence: EcNodeReturn,
        now: u64,
        enforced: bool,
    ) -> EcRejoinStatus {
        let status = decide_rejoin(repair, &evidence, now, false, enforced);
        self.install(status, Some(evidence))
    }

    /// Record an operator's statement that this return is acceptable.
    ///
    /// Bound to this return by construction: the verdict lives only as long as
    /// the process, so the next start evaluates the evidence again and asks for
    /// the statement again. It never becomes a setting.
    ///
    /// A gate that has evaluated nothing has nothing to approve, and its
    /// unchanged verdict comes back.
    pub fn approve(&self, repair: &EcRepairCoordinator, now: u64) -> EcRejoinStatus {
        let state = self.read_state();
        let Some(evidence) = state.evidence else {
            return state.status;
        };
        let status = decide_rejoin(repair, &evidence, now, true, state.status.enforced);
        self.install(status, Some(evidence))
    }

    /// Install a verdict together with the evidence behind it.
    fn install(&self, status: EcRejoinStatus, evidence: Option<EcNodeReturn>) -> EcRejoinStatus {
        let serving = status.serves_ec_reads();
        {
            let mut guard = self.state.write().unwrap_or_else(|poisoned| poisoned.into_inner());
            *guard = EcRejoinState {
                status: status.clone(),
                evidence,
            };
        }
        // Written after the state so that a reader which saw the gate open
        // finds a status that agrees with it.
        self.serving.store(serving, Ordering::Release);
        status
    }

    fn read_state(&self) -> EcRejoinState {
        self.state.read().unwrap_or_else(|poisoned| poisoned.into_inner()).clone()
    }
}

/// Whole seconds in a nanosecond duration.
fn nanos_to_secs(nanos: u64) -> u64 {
    nanos / 1_000_000_000
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec_runtime::{EcCodecCpuConfig, EcCodecCpuLimiter};
    use crate::ec_metadata::EcMetadataStore;
    use crate::ec_repair::{EcLongOfflineRejoinPolicy, EcRepairConfig};
    use crate::erasure_coding::PoolTopologyView;
    use s4_cluster::NodeId;
    use std::num::NonZeroUsize;
    use std::sync::Arc;

    const DAY_NANOS: u64 = 24 * 60 * 60 * 1_000_000_000;

    /// A coordinator whose only job here is to answer `evaluate_rejoin`.
    ///
    /// The temporary directory is kept alive by the returned guard, so no test
    /// leaks one; nothing in this module touches the repair queue itself.
    fn coordinator(
        policy: EcLongOfflineRejoinPolicy,
        max_downtime_days: u64,
    ) -> (EcRepairCoordinator, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("temp dir");
        let metadata = Arc::new(EcMetadataStore::open(&dir.path().join("db")).expect("open store"));
        let cpu = EcCodecCpuLimiter::new(EcCodecCpuConfig {
            workers: NonZeroUsize::new(1).expect("one worker"),
            cpu_budget_percent: 60,
            max_inflight_encode_bytes: 1024 * 1024,
        });
        let config = EcRepairConfig {
            max_rejoin_downtime_days: max_downtime_days,
            long_offline_rejoin_policy: policy,
            ..EcRepairConfig::default()
        };
        let coordinator = EcRepairCoordinator::new(
            metadata,
            cpu,
            config,
            PoolTopologyView::unlabeled(NodeId::generate()),
        );
        (coordinator, dir)
    }

    fn facts(origin: NodeIdentityOrigin, last_seen: Option<u64>) -> EcNodeReturn {
        EcNodeReturn {
            identity_origin: origin,
            last_participation: last_seen
                .map(|seen| EcNodePresenceRecord::first(NodeId::generate(), "pool-1", seen, false)),
            owns_slots: true,
        }
    }

    /// The same evidence, from a node that was still owing shards when it last
    /// wrote its record.
    fn facts_still_rebuilding(last_seen: u64) -> EcNodeReturn {
        EcNodeReturn {
            identity_origin: NodeIdentityOrigin::Existing,
            last_participation: Some(EcNodePresenceRecord::first(
                NodeId::generate(),
                "pool-1",
                last_seen,
                true,
            )),
            owns_slots: true,
        }
    }

    #[test]
    fn a_node_the_pool_gives_no_slot_is_never_held() {
        let (repair, _dir) = coordinator(EcLongOfflineRejoinPolicy::NeedsBootstrap, 3);
        let mut evidence = facts(NodeIdentityOrigin::Adopted, None);
        evidence.owns_slots = false;

        let status = decide_rejoin(&repair, &evidence, 100 * DAY_NANOS, false, true);

        assert_eq!(status.reason, EcRejoinReason::NoSlotsOwned);
        assert!(status.serves_ec_reads());
    }

    #[test]
    fn a_generated_identity_is_not_a_replacement() {
        let (repair, _dir) = coordinator(EcLongOfflineRejoinPolicy::NeedsBootstrap, 3);

        // A blank data directory, no memory of anything, and the pool assigning
        // it slots: the first start of every node in a new pool. Holding this
        // back would gate a cluster that has nothing to rebuild.
        let status = decide_rejoin(
            &repair,
            &facts(NodeIdentityOrigin::Generated, None),
            100 * DAY_NANOS,
            false,
            true,
        );

        assert_eq!(status.reason, EcRejoinReason::NewIdentity);
        assert!(status.serves_ec_reads());
    }

    #[test]
    fn an_adopted_identity_must_rebuild_before_it_serves() {
        let (repair, _dir) = coordinator(EcLongOfflineRejoinPolicy::NeedsBootstrap, 3);

        let status = decide_rejoin(
            &repair,
            &facts(NodeIdentityOrigin::Adopted, None),
            100 * DAY_NANOS,
            false,
            true,
        );

        assert_eq!(status.decision, EcNodeRejoinDecision::NeedsBootstrap);
        assert_eq!(status.reason, EcRejoinReason::StorageAdopted);
        assert!(!status.serves_ec_reads());
        // No downtime is claimed, because none could have been measured.
        assert_eq!(status.downtime_secs, None);
    }

    #[test]
    fn a_short_absence_with_storage_intact_serves_immediately() {
        let (repair, _dir) = coordinator(EcLongOfflineRejoinPolicy::NeedsBootstrap, 3);
        let now = 10 * DAY_NANOS;

        let status = decide_rejoin(
            &repair,
            &facts(NodeIdentityOrigin::Existing, Some(now - DAY_NANOS)),
            now,
            false,
            true,
        );

        assert_eq!(
            status.decision,
            EcNodeRejoinDecision::IncrementalRepairAllowed
        );
        assert_eq!(status.reason, EcRejoinReason::WithinDowntimeLimit);
        assert!(status.serves_ec_reads());
        assert_eq!(status.downtime_secs, Some(24 * 60 * 60));
    }

    #[test]
    fn a_long_absence_is_held_and_the_policy_decides_how() {
        let now = 30 * DAY_NANOS;
        let away_since = Some(now - 10 * DAY_NANOS);

        let (bootstrap, _bootstrap_dir) = coordinator(EcLongOfflineRejoinPolicy::NeedsBootstrap, 3);
        let held = decide_rejoin(
            &bootstrap,
            &facts(NodeIdentityOrigin::Existing, away_since),
            now,
            false,
            true,
        );
        assert_eq!(held.decision, EcNodeRejoinDecision::NeedsBootstrap);
        assert_eq!(held.reason, EcRejoinReason::BeyondDowntimeLimit);
        assert!(!held.serves_ec_reads());

        let (approval, _approval_dir) =
            coordinator(EcLongOfflineRejoinPolicy::RequireAdminApproval, 3);
        let waiting = decide_rejoin(
            &approval,
            &facts(NodeIdentityOrigin::Existing, away_since),
            now,
            false,
            true,
        );
        assert_eq!(
            waiting.decision,
            EcNodeRejoinDecision::AdminApprovalRequired
        );
        assert!(!waiting.serves_ec_reads());

        // The same absence, now approved: the policy's own way out.
        let approved = decide_rejoin(
            &approval,
            &facts(NodeIdentityOrigin::Existing, away_since),
            now,
            true,
            true,
        );
        assert_eq!(
            approved.decision,
            EcNodeRejoinDecision::IncrementalRepairAllowed
        );
        assert!(approved.serves_ec_reads());
        assert!(!approved.admin_overrode_decision());
    }

    #[test]
    fn the_downtime_limit_is_the_boundary_it_says_it_is() {
        let (repair, _dir) = coordinator(EcLongOfflineRejoinPolicy::NeedsBootstrap, 3);
        let now = 30 * DAY_NANOS;

        let at_the_limit = decide_rejoin(
            &repair,
            &facts(NodeIdentityOrigin::Existing, Some(now - 3 * DAY_NANOS)),
            now,
            false,
            true,
        );
        assert!(at_the_limit.serves_ec_reads());

        let one_second_past = decide_rejoin(
            &repair,
            &facts(
                NodeIdentityOrigin::Existing,
                Some(now - 3 * DAY_NANOS - 1_000_000_000),
            ),
            now,
            false,
            true,
        );
        assert!(!one_second_past.serves_ec_reads());
    }

    #[test]
    fn storage_without_a_record_is_not_treated_as_an_absence() {
        let (repair, _dir) = coordinator(EcLongOfflineRejoinPolicy::NeedsBootstrap, 3);

        // The first start after an upgrade from a build that kept no record.
        // Its shards are where it left them; holding it would gate every node
        // of every existing pool, one restart at a time.
        let status = decide_rejoin(
            &repair,
            &facts(NodeIdentityOrigin::Existing, None),
            100 * DAY_NANOS,
            false,
            true,
        );

        assert_eq!(status.reason, EcRejoinReason::DowntimeUnrecorded);
        assert!(status.serves_ec_reads());
    }

    #[test]
    fn a_clock_that_stepped_back_does_not_invent_an_absence() {
        let (repair, _dir) = coordinator(EcLongOfflineRejoinPolicy::NeedsBootstrap, 3);
        let now = 10 * DAY_NANOS;

        let status = decide_rejoin(
            &repair,
            &facts(NodeIdentityOrigin::Existing, Some(now + DAY_NANOS)),
            now,
            false,
            true,
        );

        assert!(status.serves_ec_reads());
        assert_eq!(status.downtime_secs, Some(0));
    }

    #[test]
    fn an_unenforced_gate_reports_the_verdict_and_holds_nothing() {
        let (repair, _dir) = coordinator(EcLongOfflineRejoinPolicy::NeedsBootstrap, 3);

        let status = decide_rejoin(
            &repair,
            &facts(NodeIdentityOrigin::Adopted, None),
            100 * DAY_NANOS,
            false,
            false,
        );

        assert_eq!(status.decision, EcNodeRejoinDecision::NeedsBootstrap);
        assert!(status.serves_ec_reads());
        assert!(!status.enforced);
    }

    #[test]
    fn a_restart_is_not_a_rebuild() {
        let (repair, _dir) = coordinator(EcLongOfflineRejoinPolicy::NeedsBootstrap, 3);
        let now = 10 * DAY_NANOS;

        // Seconds of downtime, and a record saying the node still owed shards
        // when it wrote it. Without the flag this reads as a healthy restart.
        let status = decide_rejoin(&repair, &facts_still_rebuilding(now - 1), now, false, true);

        assert_eq!(status.decision, EcNodeRejoinDecision::NeedsBootstrap);
        assert_eq!(status.reason, EcRejoinReason::RebuildPending);
        assert!(!status.serves_ec_reads());
    }

    #[test]
    fn an_operator_can_open_a_gate_the_decision_keeps_shut_and_it_shows() {
        let (repair, _dir) = coordinator(EcLongOfflineRejoinPolicy::NeedsBootstrap, 3);
        let gate = EcRejoinGate::new();

        gate.evaluate(
            &repair,
            facts(NodeIdentityOrigin::Adopted, None),
            100 * DAY_NANOS,
            true,
        );
        assert!(!gate.serves_ec_reads());

        let approved = gate.approve(&repair, 100 * DAY_NANOS);
        assert!(gate.serves_ec_reads());
        // The decision itself has not changed its mind, and the surface says so
        // rather than reporting a node that rebuilt itself.
        assert_eq!(approved.decision, EcNodeRejoinDecision::NeedsBootstrap);
        assert!(approved.admin_overrode_decision());
    }

    #[test]
    fn approval_decides_the_return_it_was_given_and_not_a_fresher_one() {
        let (repair, _dir) = coordinator(EcLongOfflineRejoinPolicy::RequireAdminApproval, 3);
        let now = 30 * DAY_NANOS;
        let gate = EcRejoinGate::new();

        gate.evaluate(
            &repair,
            facts(NodeIdentityOrigin::Existing, Some(now - 10 * DAY_NANOS)),
            now,
            true,
        );
        assert_eq!(
            gate.status().decision,
            EcNodeRejoinDecision::AdminApprovalRequired
        );

        // The approval re-runs the same evidence, so the ten-day absence is
        // still what is being approved rather than a downtime of zero.
        let approved = gate.approve(&repair, now);
        assert_eq!(
            approved.decision,
            EcNodeRejoinDecision::IncrementalRepairAllowed
        );
        assert_eq!(approved.downtime_secs, Some(10 * 24 * 60 * 60));
        assert!(gate.serves_ec_reads());
    }

    #[test]
    fn a_gate_that_evaluated_nothing_has_nothing_to_approve() {
        let (repair, _dir) = coordinator(EcLongOfflineRejoinPolicy::NeedsBootstrap, 3);
        let gate = EcRejoinGate::new();

        let status = gate.approve(&repair, 10 * DAY_NANOS);

        assert_eq!(status.reason, EcRejoinReason::NotEvaluated);
        assert!(!status.admin_approved);
        assert!(gate.serves_ec_reads());
    }

    #[test]
    fn a_fresh_gate_serves_and_claims_no_verdict() {
        let gate = EcRejoinGate::new();

        assert!(gate.serves_ec_reads());
        assert_eq!(gate.status().reason, EcRejoinReason::NotEvaluated);
        assert!(!gate.status().enforced);
    }
}
