// Copyright 2026 S4Core Team
//
// Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

//! Prometheus metric helpers for Enterprise Erasure Coding.
//!
//! # Erasure sets and the `erasure_set` label
//!
//! A pool holds one erasure set or several, and most EC work belongs to exactly
//! one of them: a transcode encodes into a set, a repair rebuilds a slot of a
//! set, a degraded read decodes a set's shards. Those metrics carry the set id
//! in the `erasure_set` label, so an operator can see one set fall behind
//! instead of watching a pool average hide it.
//!
//! The pool-wide figure is not lost, and is not published twice either: it is
//! the sum over the label, `sum without (erasure_set) (s4_ec_repair_jobs_total)`.
//! Emitting an unlabelled series next to the labelled ones would double every
//! total that a dashboard adds up, which is why it is not done.
//!
//! Tiering metrics stay unlabelled. Tiering is decided per bucket against the
//! pool, and the set only enters when a migration is admitted — which the
//! transcode metrics already count. So do repair bytes: they measure what the
//! links between zones carry, and a link does not belong to a set.

use std::collections::HashMap;

use crate::ec_repair_plan::{RepairLeg, TransferScope};
use crate::erasure_coding::{EcLayoutViolationReason, EcTopologyPolicy, LayoutViolationOrigin};

/// Label naming the erasure set a metric belongs to.
pub const ERASURE_SET_LABEL: &str = "erasure_set";

/// EC transcode jobs started.
pub const TRANSCODE_JOBS_TOTAL: &str = "s4_ec_transcode_jobs_total";
/// EC transcode jobs that failed.
pub const TRANSCODE_FAILURES_TOTAL: &str = "s4_ec_transcode_failures_total";
/// Durable transcode queue depth.
pub const TRANSCODE_QUEUE_DEPTH: &str = "s4_ec_transcode_queue_depth";
/// Inline EC writes accepted.
pub const INLINE_WRITES_TOTAL: &str = "s4_ec_inline_writes_total";
/// Inline EC writes rejected before work started.
pub const INLINE_WRITES_REJECTED_TOTAL: &str = "s4_ec_inline_writes_rejected_total";
/// EC reads that required parity reconstruction.
pub const READ_DEGRADED_TOTAL: &str = "s4_ec_read_degraded_total";
/// Recoveries no EC v1 path would have reached: the unified LRC solver did.
///
/// Labelled by what the solver was carrying, because the two mean different
/// things to whoever is watching: a read is a request already in flight, a
/// repair is the pool healing itself. Either way a moving counter says the
/// pool is leaning on the full recovery path — a statement about how much
/// redundancy is left, not about the codec (decision EC2-103, section 5).
pub const UNIFIED_SOLVER_RECOVERIES_TOTAL: &str = "s4_ec_unified_solver_recoveries_total";
/// Label naming what the unified solver was reached for.
pub const SOLVER_OPERATION_LABEL: &str = "operation";
/// Shard payloads rejected by manifest-authoritative hash checks.
pub const SHARD_HASH_MISMATCH_TOTAL: &str = "s4_ec_shard_hash_mismatch_total";

/// Packed-segment objects served straight from a data shard, without decoding.
pub const PACKED_DIRECT_READS_TOTAL: &str = "s4_ec_packed_direct_reads_total";

/// Packed-segment objects that had to fall back to rebuilding whole chunks.
pub const PACKED_DEGRADED_READS_TOTAL: &str = "s4_ec_packed_degraded_reads_total";

/// Fragmented packed segments whose live objects were moved to a replacement.
pub const PACKED_REPACK_SEGMENTS_TOTAL: &str = "s4_ec_packed_repack_segments_total";

/// Objects a repack moved into a replacement segment.
pub const PACKED_REPACK_OBJECTS_TOTAL: &str = "s4_ec_packed_repack_objects_total";

/// Space a repack gives back once the segment it emptied loses its shards.
pub const PACKED_REPACK_RECLAIMED_BYTES_TOTAL: &str = "s4_ec_packed_repack_reclaimed_bytes_total";

/// Repacks not attempted because the segment holds an object under Object Lock.
///
/// An expected state rather than a fault: the hold expires and the segment
/// repacks on its own. Watch it to see how much space retention is pinning.
pub const PACKED_REPACK_BLOCKED_TOTAL: &str = "s4_ec_packed_repack_blocked_total";

/// Retired segments whose shards were handed to the tombstone collector.
pub const PACKED_SEGMENTS_RETIRED_TOTAL: &str = "s4_ec_packed_segments_retired_total";

/// Segments an operator unpacked back into ordinary RF=3 small objects.
pub const PACKED_SEGMENTS_UNPACKED_TOTAL: &str = "s4_ec_packed_segments_unpacked_total";

/// Packed segments in each lifecycle state, tracked per erasure set.
pub const PACKED_SEGMENTS: &str = "s4_ec_packed_segments";

/// Total bytes allocated to objects across segments in an erasure set.
pub const PACKED_SEGMENT_USED_BYTES: &str = "s4_ec_packed_segment_used_bytes";

/// Bytes held by deleted objects across segments in an erasure set.
pub const PACKED_SEGMENT_DEAD_BYTES: &str = "s4_ec_packed_segment_dead_bytes";

/// Ratio of dead bytes to total used bytes across segments in an erasure set.
pub const PACKED_SEGMENT_DEAD_RATIO: &str = "s4_ec_packed_segment_dead_ratio";

/// Number of fragmented packed segments queued for repacking in an erasure set.
pub const PACKED_REPACK_QUEUE_DEPTH: &str = "s4_ec_packed_repack_queue_depth";

/// Label naming the lifecycle state of a packed segment.
const PACKED_STATE_LABEL: &str = "state";

/// Every lifecycle state a packed segment can be in, in progression order.
const PACKED_SEGMENT_STATES: [&str; 5] = ["open", "sealed", "encoded", "purged", "dead"];

/// Label naming how many shards of one chunk the pool is missing.
const REPAIR_LOSSES_LABEL: &str = "losses";

/// Every bucket the repair queue is split into, least damaged chunk first.
///
/// Four of them, and the last one open-ended, because the widest profile has
/// fourteen slots and a series per possible loss count would say little more:
/// past three losses every profile is beyond the losses it survives whatever
/// falls where, and the action is the same one.
const REPAIR_LOSS_BUCKETS: [&str; 4] = ["1", "2", "3", "4+"];
/// EC repair tasks queued or started.
pub const REPAIR_JOBS_TOTAL: &str = "s4_ec_repair_jobs_total";
/// Durable repair queue depth.
pub const REPAIR_QUEUE_DEPTH: &str = "s4_ec_repair_queue_depth";
/// Durable repair queue depth split by how damaged each task's chunk is.
pub const REPAIR_QUEUE_DEPTH_BY_LOSSES: &str = "s4_ec_repair_queue_depth_by_losses";
/// EC repair tasks that succeeded.
pub const REPAIR_SUCCEEDED_TOTAL: &str = "s4_ec_repair_succeeded_total";
/// EC repair tasks that failed.
pub const REPAIR_FAILURES_TOTAL: &str = "s4_ec_repair_failures_total";
/// Local orphan shard candidates.
pub const ORPHAN_SHARDS_TOTAL: &str = "s4_ec_orphan_shards_total";
/// Manifest quorum commit/read failures.
pub const MANIFEST_QUORUM_FAILURES_TOTAL: &str = "s4_ec_manifest_quorum_failures_total";
/// Hot RF=3 replica sets that became GC eligible after EC verification.
pub const HOT_REPLICAS_GC_ELIGIBLE_TOTAL: &str = "s4_ec_hot_replicas_gc_eligible_total";
/// Hot RF=3 replica sets purged after EC verification and grace.
pub const HOT_REPLICAS_PURGED_TOTAL: &str = "s4_ec_hot_replicas_purged_total";
/// Object access signals accepted by the tiering heat tracker.
pub const TIERING_HEAT_SIGNALS_TOTAL: &str = "s4_ec_tiering_heat_signals_total";
/// Object access signals dropped because the tracker queue was full.
pub const TIERING_HEAT_SIGNALS_DROPPED_TOTAL: &str = "s4_ec_tiering_heat_signals_dropped_total";
/// Flush cycles the heat tracker completed.
pub const TIERING_HEAT_FLUSHES_TOTAL: &str = "s4_ec_tiering_heat_flushes_total";
/// Activity marks actually written to durable metadata.
pub const TIERING_HEAT_RECORDS_WRITTEN_TOTAL: &str = "s4_ec_tiering_heat_records_written_total";
/// Objects held in the heat tracker's in-memory batch at the last flush.
pub const TIERING_HEAT_PENDING_DEPTH: &str = "s4_ec_tiering_heat_pending_depth";
/// Scan cycles the tiering migration worker completed.
pub const TIERING_SCAN_CYCLES_TOTAL: &str = "s4_ec_tiering_scan_cycles_total";
/// Objects the tiering worker examined.
pub const TIERING_SCANNED_TOTAL: &str = "s4_ec_tiering_scanned_total";
/// Objects the tiering worker queued for migration into EC.
pub const TIERING_QUEUED_TOTAL: &str = "s4_ec_tiering_queued_total";
/// Objects the tiering worker examined and left where they were.
pub const TIERING_SKIPPED_TOTAL: &str = "s4_ec_tiering_skipped_total";
/// Migrations abandoned because the object changed or disappeared.
pub const TIERING_CANCELLED_TOTAL: &str = "s4_ec_tiering_cancelled_total";
/// Tiering-originated transcode jobs still in flight.
pub const TIERING_QUEUE_DEPTH: &str = "s4_ec_tiering_queue_depth";
/// Bytes queued for migration into EC.
pub const TIERING_QUEUED_BYTES_TOTAL: &str = "s4_ec_tiering_queued_bytes_total";
/// Objects whose RF=3 copy was written back by reverse tiering.
pub const TIERING_PROMOTION_RESTORED_TOTAL: &str = "s4_ec_tiering_promotion_restored_total";
/// Bytes written back to RF=3 by reverse tiering.
pub const TIERING_PROMOTION_RESTORED_BYTES_TOTAL: &str =
    "s4_ec_tiering_promotion_restored_bytes_total";
/// Objects whose readers were switched back to the hot path.
pub const TIERING_PROMOTION_COMMITTED_TOTAL: &str = "s4_ec_tiering_promotion_committed_total";
/// Promoted objects whose shards and manifest were removed.
pub const TIERING_PROMOTION_PURGED_TOTAL: &str = "s4_ec_tiering_promotion_purged_total";
/// Reverse migrations dropped because a client wrote the object.
pub const TIERING_PROMOTION_SUPERSEDED_TOTAL: &str = "s4_ec_tiering_promotion_superseded_total";
/// Reverse migrations given up on after exhausting their retries.
pub const TIERING_PROMOTION_FAILED_TOTAL: &str = "s4_ec_tiering_promotion_failed_total";
/// Reverse migrations still in flight.
pub const TIERING_PROMOTION_QUEUE_DEPTH: &str = "s4_ec_tiering_promotion_queue_depth";

/// New erasure sets whose nodes could not be laid out by the LRC group rule.
///
/// Labelled by `reason` and by `policy`: under `strict` the set was not
/// created, under `warn` it was created with degraded topology (EC2-083).
pub const SET_LAYOUT_UNSATISFIED_TOTAL: &str = "s4_ec_set_layout_unsatisfied_total";

/// Erasure sets whose layout no longer keeps the guarantee of their own rule.
///
/// One series per set and origin, `1` when the set breaches its rule now and
/// `0` when it does not, so the pool's number of violators is
/// `sum without (erasure_set) (s4_ec_set_layout_violations)`. A set laid out by
/// the EC v1 rule makes no placement guarantee and is always `0`: switching
/// `S4_EC_LRC_GROUP_DOMAIN` on must not turn existing sets into violators
/// (EC2-084, point 1).
///
/// The `origin` label separates a set that drifted — its labels changed under
/// it — from one created in breach under `warn`, which has been a violator
/// since birth. They need different actions, so they are not summed together.
pub const SET_LAYOUT_VIOLATIONS: &str = "s4_ec_set_layout_violations";

/// Label naming where a layout violation came from: `drift` or `creation`.
const LAYOUT_ORIGIN_LABEL: &str = "origin";

/// Every origin a current layout violation can have.
const LAYOUT_ORIGINS: [LayoutViolationOrigin; 2] = [
    LayoutViolationOrigin::Drift,
    LayoutViolationOrigin::Creation,
];

/// Bytes EC repair moved, by leg and by zone scope.
///
/// Counted from bytes that actually travelled: a source that answered counts
/// with the bytes it sent, verified or not; a written shard counts once the
/// write returned. `scope="cross_zone"` is what the links between zones
/// carried for repair (plan 05, phase 3).
pub const REPAIR_BYTES_TOTAL: &str = "s4_ec_repair_bytes_total";

/// Bytes EC reads pulled out of shards, by zone scope.
///
/// The read path had no byte counter at all before plan 10: the only read
/// metric was [`READ_DEGRADED_TOTAL`], which says how often a read decoded and
/// nothing about what it moved. Without this series "a read costs `n/k` of the
/// object" is an argument rather than a measurement (decision EC2-183).
///
/// Counted from bytes the read received: a shard that came back counts with the
/// bytes it carried, whether or not it then passed its manifest hash. A fetch
/// that failed — the owner had no such shard, or refused it because it did not
/// match its own recorded hash — counts as a fetch of no bytes, because a read
/// that was handed nothing has nothing to measure. Unlabelled by erasure set
/// like [`REPAIR_BYTES_TOTAL`]: a link between zones is one physical thing
/// whichever set's shards cross it.
pub const READ_SHARD_BYTES_TOTAL: &str = "s4_ec_read_shard_bytes_total";

/// Shard fetches EC reads made, by zone scope.
///
/// Counted apart from the bytes because the two are saved separately. Shards
/// are fetched one after another, so a fetch the read no longer makes is a
/// network round trip out of the latency chain, not only bytes off the link
/// (decision EC2-180).
pub const READ_SHARD_FETCHES_TOTAL: &str = "s4_ec_read_shard_fetches_total";

/// How a small object's placement record was resolved, by where it came from.
///
/// `source` is `replicas` when the nodes holding the object answered,
/// `pool_fanout` when the reader had to ask the rest of the pool, and `absent`
/// when the object has no placement record at all. A pool whose reads keep
/// landing on `pool_fanout` is a pool whose placement records did not reach
/// their replicas (decision EC2-201).
pub const SMALL_PLACEMENT_RESOLVES_TOTAL: &str = "s4_ec_small_placement_resolves_total";

/// Reads that attempted to resolve placement beyond its RF owners, even if no
/// node answered with a record. Counts attempts, not successful resolutions.
pub const SMALL_PLACEMENT_FALLBACK_ATTEMPTS_TOTAL: &str =
    "s4_ec_small_placement_fallback_attempts_total";

/// Placement records that owed copies to replicas, by what became of the debt.
///
/// `outcome` is `recorded` when a write could not reach a majority of the
/// object's replicas, `cleared` when the sweep finished the job, and `dropped`
/// when the object turned out to be gone and the hint went with it.
pub const SMALL_PLACEMENT_HINTS_TOTAL: &str = "s4_ec_small_placement_hints_total";

/// Repairs a limit held back, by the limit that held them.
///
/// The counter that makes "the rebuild stayed inside its budget" something an
/// operator can read rather than assume. A whole-node rebuild is the heaviest
/// repair load a pool ever carries, and it is paced by the same admission the
/// limits apply to every repair — so a rebuild running slower than the hardware
/// could go is either the budget working or something wrong, and only this
/// series tells the two apart.
pub const REPAIR_THROTTLED_TOTAL: &str = "s4_ec_repair_throttled_total";

/// Label naming which limit held a repair back.
const REPAIR_LIMIT_LABEL: &str = "limit";

/// Label naming the leg of a repair: `source` or `write`.
const REPAIR_LEG_LABEL: &str = "leg";

/// Label naming where transferred bytes went: `local`, `same_zone`,
/// `cross_zone` or `unknown_zone`.
///
/// Shared by the repair and the read byte counters, so the same query shapes
/// answer "what did this link carry" for both.
const TRANSFER_SCOPE_LABEL: &str = "scope";

/// Label naming why a layout fell short of its rule.
const LAYOUT_REASON_LABEL: &str = "reason";

/// Label naming the topology policy a layout fell short under.
const LAYOUT_POLICY_LABEL: &str = "policy";

/// Every reason a layout can fall short, in the order EC2-083 lists them.
const LAYOUT_REASONS: [EcLayoutViolationReason; 3] = [
    EcLayoutViolationReason::TooFewDomains,
    EcLayoutViolationReason::DomainOverloaded,
    EcLayoutViolationReason::IncompleteLabels,
];

/// Every topology policy.
const LAYOUT_POLICIES: [EcTopologyPolicy; 2] = [EcTopologyPolicy::Strict, EcTopologyPolicy::Warn];

/// Label value for one erasure set.
fn set_label(erasure_set_id: u64) -> String {
    erasure_set_id.to_string()
}

/// Publish one gauge for every set the pool has.
///
/// A set with nothing to report is published as zero rather than left out. An
/// absent series reads as "no data" on a dashboard, which is the same shape as
/// a broken exporter, and a series that stops being written keeps its last
/// value forever — so a set whose queue drained would look permanently busy.
fn publish_per_set_gauge(name: &'static str, known_sets: &[u64], values: &HashMap<u64, usize>) {
    for (erasure_set_id, value) in per_set_gauge_values(known_sets, values) {
        metrics::gauge!(name, ERASURE_SET_LABEL => set_label(erasure_set_id)).set(value);
    }
}

/// The value to publish for each known set, in the order they were given.
///
/// Split out from the publishing so the "a quiet set is a zero, not a gap" rule
/// can be read back in a test without a metrics recorder.
fn per_set_gauge_values(known_sets: &[u64], values: &HashMap<u64, usize>) -> Vec<(u64, f64)> {
    known_sets
        .iter()
        .map(|erasure_set_id| {
            let value = values.get(erasure_set_id).copied().unwrap_or(0);
            (*erasure_set_id, value as f64)
        })
        .collect()
}

/// Publish one byte-count gauge for every set the pool has.
///
/// Sets missing from `values` are published as zero to avoid metric gaps.
fn publish_per_set_bytes(name: &'static str, known_sets: &[u64], values: &HashMap<u64, u64>) {
    for (erasure_set_id, value) in per_set_bytes_values(known_sets, values) {
        metrics::gauge!(name, ERASURE_SET_LABEL => set_label(erasure_set_id)).set(value);
    }
}

/// The byte values to publish for each known set, defaulting missing sets to zero.
fn per_set_bytes_values(known_sets: &[u64], values: &HashMap<u64, u64>) -> Vec<(u64, f64)> {
    known_sets
        .iter()
        .map(|erasure_set_id| {
            let value = values.get(erasure_set_id).copied().unwrap_or(0);
            (*erasure_set_id, value as f64)
        })
        .collect()
}

/// Publish one ratio gauge for every set the pool has.
///
/// Sets missing from `values` are published as zero to avoid metric gaps.
fn publish_per_set_ratio(name: &'static str, known_sets: &[u64], values: &HashMap<u64, f64>) {
    for (erasure_set_id, value) in per_set_ratio_values(known_sets, values) {
        metrics::gauge!(name, ERASURE_SET_LABEL => set_label(erasure_set_id)).set(value);
    }
}

/// The ratio values to publish for each known set, defaulting missing sets to 0.0.
fn per_set_ratio_values(known_sets: &[u64], values: &HashMap<u64, f64>) -> Vec<(u64, f64)> {
    known_sets
        .iter()
        .map(|erasure_set_id| {
            let value = values.get(erasure_set_id).copied().unwrap_or(0.0);
            (*erasure_set_id, value)
        })
        .collect()
}

/// Values to publish for packed segments by lifecycle state, for each known set.
///
/// For every known set and every state in [`PACKED_SEGMENT_STATES`], publishes
/// the observed count or zero if absent.
fn per_set_packed_segments_values(
    known_sets: &[u64],
    counts: &HashMap<u64, HashMap<&'static str, usize>>,
) -> Vec<(u64, &'static str, f64)> {
    let mut result = Vec::with_capacity(known_sets.len() * PACKED_SEGMENT_STATES.len());
    for &erasure_set_id in known_sets {
        let set_counts = counts.get(&erasure_set_id);
        for &state in &PACKED_SEGMENT_STATES {
            let count = set_counts.and_then(|map| map.get(state)).copied().unwrap_or(0);
            result.push((erasure_set_id, state, count as f64));
        }
    }
    result
}

/// Increment transcode job count for one erasure set.
pub fn record_transcode_job(erasure_set_id: u64) {
    metrics::counter!(TRANSCODE_JOBS_TOTAL, ERASURE_SET_LABEL => set_label(erasure_set_id))
        .increment(1);
}

/// Increment transcode failure count for one erasure set.
pub fn record_transcode_failure(erasure_set_id: u64) {
    metrics::counter!(TRANSCODE_FAILURES_TOTAL, ERASURE_SET_LABEL => set_label(erasure_set_id))
        .increment(1);
}

/// Publish the durable transcode queue depth of every erasure set.
pub fn set_transcode_queue_depth(known_sets: &[u64], depth_by_set: &HashMap<u64, usize>) {
    publish_per_set_gauge(TRANSCODE_QUEUE_DEPTH, known_sets, depth_by_set);
}

/// Increment accepted inline-write count for one erasure set.
pub fn record_inline_write(erasure_set_id: u64) {
    metrics::counter!(INLINE_WRITES_TOTAL, ERASURE_SET_LABEL => set_label(erasure_set_id))
        .increment(1);
}

/// Increment rejected inline-write count for one erasure set.
pub fn record_inline_write_rejected(erasure_set_id: u64) {
    metrics::counter!(INLINE_WRITES_REJECTED_TOTAL, ERASURE_SET_LABEL => set_label(erasure_set_id))
        .increment(1);
}

/// Increment degraded read count for one erasure set.
pub fn record_degraded_read(erasure_set_id: u64) {
    metrics::counter!(READ_DEGRADED_TOTAL, ERASURE_SET_LABEL => set_label(erasure_set_id))
        .increment(1);
}

/// What the unified LRC solver was reached for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnifiedSolverOperation {
    /// A degraded read: a chunk rebuilt on its way to a client.
    Read,
    /// A repair: a shard rebuilt back onto a node.
    Repair,
}

impl UnifiedSolverOperation {
    /// Every value the label can take.
    pub const ALL: [Self; 2] = [Self::Read, Self::Repair];

    /// Stable label value, also used as the log field.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Repair => "repair",
        }
    }
}

/// Count one recovery that only the unified LRC solver reached.
///
/// Counted where the recovery happened, not where it was planned: a pattern
/// the fast paths could have handled never gets here, which is what makes the
/// counter mean "the pool needed the full path" rather than "the pool has an
/// LRC profile".
pub fn record_unified_solver_recovery(erasure_set_id: u64, operation: UnifiedSolverOperation) {
    metrics::counter!(
        UNIFIED_SOLVER_RECOVERIES_TOTAL,
        ERASURE_SET_LABEL => set_label(erasure_set_id),
        SOLVER_OPERATION_LABEL => operation.as_str(),
    )
    .increment(1);
}

/// Count one packed object served by the direct read.
pub fn record_packed_direct_read(erasure_set_id: u64) {
    metrics::counter!(PACKED_DIRECT_READS_TOTAL, ERASURE_SET_LABEL => set_label(erasure_set_id))
        .increment(1);
}

/// Count one packed object that fell back to rebuilding chunks.
///
/// The ratio of this to [`record_packed_direct_read`] is the number plan 04 is
/// judged on in a healthy cluster: the fallback reads whole chunks, which is the
/// read amplification packing exists to avoid.
pub fn record_packed_degraded_read(erasure_set_id: u64) {
    metrics::counter!(PACKED_DEGRADED_READS_TOTAL, ERASURE_SET_LABEL => set_label(erasure_set_id))
        .increment(1);
}

/// Count one segment whose live objects were moved into a replacement.
pub fn record_packed_repack_segment(erasure_set_id: u64) {
    metrics::counter!(
        PACKED_REPACK_SEGMENTS_TOTAL,
        ERASURE_SET_LABEL => set_label(erasure_set_id)
    )
    .increment(1);
}

/// Add the objects one repack moved.
pub fn record_packed_repack_objects(erasure_set_id: u64, objects: u64) {
    metrics::counter!(
        PACKED_REPACK_OBJECTS_TOTAL,
        ERASURE_SET_LABEL => set_label(erasure_set_id)
    )
    .increment(objects);
}

/// Add the space one repack will give back once the old shards go.
///
/// Counted at the cutover rather than at the deletion, because that is the
/// point from which the space is certain to come back: the segment is retired
/// and nothing can start reading it again.
pub fn record_packed_repack_reclaimed(erasure_set_id: u64, bytes: u64) {
    metrics::counter!(
        PACKED_REPACK_RECLAIMED_BYTES_TOTAL,
        ERASURE_SET_LABEL => set_label(erasure_set_id)
    )
    .increment(bytes);
}

/// Count one segment left fragmented because Object Lock holds an object in it.
///
/// Not a failure. It is how an operator sees space that retention is pinning,
/// which is otherwise invisible: the segment looks the same as one nobody has
/// got round to yet.
pub fn record_packed_repack_blocked(erasure_set_id: u64) {
    metrics::counter!(
        PACKED_REPACK_BLOCKED_TOTAL,
        ERASURE_SET_LABEL => set_label(erasure_set_id)
    )
    .increment(1);
}

/// Count one segment an operator returned to ordinary RF=3 replication.
///
/// The rollback path of the feature. An operator watching a rollback finish
/// counts this against the segments the pool still has.
pub fn record_packed_segment_unpacked(erasure_set_id: u64) {
    metrics::counter!(
        PACKED_SEGMENTS_UNPACKED_TOTAL,
        ERASURE_SET_LABEL => set_label(erasure_set_id)
    )
    .increment(1);
}

/// Count one retired segment whose shards were handed to the tombstone collector.
pub fn record_packed_segment_retired(erasure_set_id: u64) {
    metrics::counter!(
        PACKED_SEGMENTS_RETIRED_TOTAL,
        ERASURE_SET_LABEL => set_label(erasure_set_id)
    )
    .increment(1);
}

/// Publish how many segments of every set are in each lifecycle state.
pub fn set_packed_segments_by_state(
    known_sets: &[u64],
    counts: &HashMap<u64, HashMap<&'static str, usize>>,
) {
    for (erasure_set_id, state, count) in per_set_packed_segments_values(known_sets, counts) {
        metrics::gauge!(
            PACKED_SEGMENTS,
            ERASURE_SET_LABEL => set_label(erasure_set_id),
            PACKED_STATE_LABEL => state,
        )
        .set(count);
    }
}

/// Publish total bytes allocated to objects across segments in each erasure set.
pub fn set_packed_segment_used_bytes(known_sets: &[u64], values: &HashMap<u64, u64>) {
    publish_per_set_bytes(PACKED_SEGMENT_USED_BYTES, known_sets, values);
}

/// Publish bytes held by deleted objects across segments in each erasure set.
pub fn set_packed_segment_dead_bytes(known_sets: &[u64], values: &HashMap<u64, u64>) {
    publish_per_set_bytes(PACKED_SEGMENT_DEAD_BYTES, known_sets, values);
}

/// Publish ratio of dead bytes to used bytes across segments in each erasure set.
pub fn set_packed_segment_dead_ratio(known_sets: &[u64], values: &HashMap<u64, f64>) {
    publish_per_set_ratio(PACKED_SEGMENT_DEAD_RATIO, known_sets, values);
}

/// Publish the number of packed segments awaiting repacking in each erasure set.
pub fn set_packed_repack_queue_depth(known_sets: &[u64], depth_by_set: &HashMap<u64, usize>) {
    publish_per_set_gauge(PACKED_REPACK_QUEUE_DEPTH, known_sets, depth_by_set);
}

/// Increment shard hash mismatch count for one erasure set.
pub fn record_shard_hash_mismatch(erasure_set_id: u64) {
    metrics::counter!(SHARD_HASH_MISMATCH_TOTAL, ERASURE_SET_LABEL => set_label(erasure_set_id))
        .increment(1);
}

/// Increment repair job count for one erasure set.
pub fn record_repair_job(erasure_set_id: u64) {
    metrics::counter!(REPAIR_JOBS_TOTAL, ERASURE_SET_LABEL => set_label(erasure_set_id))
        .increment(1);
}

/// Publish the durable repair queue depth of every erasure set.
///
/// Per set because that is the number the isolation rule is read from: a
/// backlog in one set must not be visible as pressure on another.
pub fn set_repair_queue_depth(known_sets: &[u64], depth_by_set: &HashMap<u64, usize>) {
    publish_per_set_gauge(REPAIR_QUEUE_DEPTH, known_sets, depth_by_set);
}

/// Publish the repair queue depth of every erasure set, split by damage.
///
/// The same tasks as [`set_repair_queue_depth`], counted once more under the
/// number of shards their own chunk is missing, so that summing the buckets of
/// a set gives that set's queue depth back. One number alone cannot tell a
/// quiet pool with a long tail of single losses from a pool a shard away from
/// losing data, and those call for opposite reactions.
///
/// Every bucket of every set is published, including the empty ones: a series
/// that stops being written keeps its last value, which would leave a set that
/// has been repaired looking endangered forever.
pub fn set_repair_queue_depth_by_losses(
    known_sets: &[u64],
    counts: &HashMap<u64, HashMap<&'static str, usize>>,
) {
    for (erasure_set_id, bucket, count) in per_set_repair_loss_values(known_sets, counts) {
        metrics::gauge!(
            REPAIR_QUEUE_DEPTH_BY_LOSSES,
            ERASURE_SET_LABEL => set_label(erasure_set_id),
            REPAIR_LOSSES_LABEL => bucket,
        )
        .set(count);
    }
}

/// The bucket a task whose chunk is missing `losses` shards is counted in.
///
/// A task counts its own shard as missing, so one loss is the smallest a
/// queued task can report and anything below it is read as one.
pub fn repair_loss_bucket(losses: usize) -> &'static str {
    match losses {
        0 | 1 => REPAIR_LOSS_BUCKETS[0],
        2 => REPAIR_LOSS_BUCKETS[1],
        3 => REPAIR_LOSS_BUCKETS[2],
        _ => REPAIR_LOSS_BUCKETS[3],
    }
}

/// The value to publish for every set and bucket, in the order they are given.
///
/// Split out from the publishing so the "an empty bucket is a zero, not a gap"
/// rule can be read back in a test without a metrics recorder.
fn per_set_repair_loss_values(
    known_sets: &[u64],
    counts: &HashMap<u64, HashMap<&'static str, usize>>,
) -> Vec<(u64, &'static str, f64)> {
    let mut result = Vec::with_capacity(known_sets.len() * REPAIR_LOSS_BUCKETS.len());
    for &erasure_set_id in known_sets {
        let set_counts = counts.get(&erasure_set_id);
        for &bucket in &REPAIR_LOSS_BUCKETS {
            let count = set_counts.and_then(|map| map.get(bucket)).copied().unwrap_or(0);
            result.push((erasure_set_id, bucket, count as f64));
        }
    }
    result
}

/// Increment repair success count for one erasure set.
pub fn record_repair_success(erasure_set_id: u64) {
    metrics::counter!(REPAIR_SUCCEEDED_TOTAL, ERASURE_SET_LABEL => set_label(erasure_set_id))
        .increment(1);
}

/// Increment repair failure count for one erasure set.
pub fn record_repair_failure(erasure_set_id: u64) {
    metrics::counter!(REPAIR_FAILURES_TOTAL, ERASURE_SET_LABEL => set_label(erasure_set_id))
        .increment(1);
}

/// Publish the local orphan shard count of every erasure set.
pub fn set_orphan_shards(known_sets: &[u64], count_by_set: &HashMap<u64, usize>) {
    publish_per_set_gauge(ORPHAN_SHARDS_TOTAL, known_sets, count_by_set);
}

/// Increment manifest quorum failure count for one erasure set.
pub fn record_manifest_quorum_failure(erasure_set_id: u64) {
    metrics::counter!(
        MANIFEST_QUORUM_FAILURES_TOTAL,
        ERASURE_SET_LABEL => set_label(erasure_set_id)
    )
    .increment(1);
}

/// Increment hot replica GC eligibility count for one erasure set.
pub fn record_hot_replicas_gc_eligible(erasure_set_id: u64) {
    metrics::counter!(
        HOT_REPLICAS_GC_ELIGIBLE_TOTAL,
        ERASURE_SET_LABEL => set_label(erasure_set_id)
    )
    .increment(1);
}

/// Increment hot replica purge count for one erasure set.
pub fn record_hot_replicas_purged(erasure_set_id: u64) {
    metrics::counter!(HOT_REPLICAS_PURGED_TOTAL, ERASURE_SET_LABEL => set_label(erasure_set_id))
        .increment(1);
}

/// Increment the count of accepted access signals.
pub fn record_heat_signal() {
    metrics::counter!(TIERING_HEAT_SIGNALS_TOTAL).increment(1);
}

/// Increment the count of access signals dropped by a full queue.
///
/// A rising value is not an error. It means reads outran the flusher, and the
/// only consequence is that an object looks colder than it is.
pub fn record_heat_signal_dropped() {
    metrics::counter!(TIERING_HEAT_SIGNALS_DROPPED_TOTAL).increment(1);
}

/// Increment the completed flush count.
pub fn record_heat_flush() {
    metrics::counter!(TIERING_HEAT_FLUSHES_TOTAL).increment(1);
}

/// Increment the count of activity marks written durably.
///
/// This stays well below the signal count: signals collapse in memory, and a
/// mark is only rewritten once it is older than the configured resolution.
pub fn record_heat_record_written() {
    metrics::counter!(TIERING_HEAT_RECORDS_WRITTEN_TOTAL).increment(1);
}

/// Set the in-memory batch depth observed at the last flush.
pub fn set_heat_pending_depth(depth: usize) {
    metrics::gauge!(TIERING_HEAT_PENDING_DEPTH).set(depth as f64);
}

/// Increment the completed tiering scan-cycle count.
pub fn record_tiering_scan_cycle() {
    metrics::counter!(TIERING_SCAN_CYCLES_TOTAL).increment(1);
}

/// Add to the count of objects the tiering worker examined.
pub fn record_tiering_scanned(objects: usize) {
    metrics::counter!(TIERING_SCANNED_TOTAL).increment(objects as u64);
}

/// Record one object queued for migration into EC.
pub fn record_tiering_queued(object_size: u64) {
    metrics::counter!(TIERING_QUEUED_TOTAL).increment(1);
    metrics::counter!(TIERING_QUEUED_BYTES_TOTAL).increment(object_size);
}

/// Add to the count of objects examined and left in place.
///
/// Read against the scanned count: a worker that migrates nothing is healthy
/// when the two match, and misconfigured when they do not.
pub fn record_tiering_skipped(objects: usize) {
    metrics::counter!(TIERING_SKIPPED_TOTAL).increment(objects as u64);
}

/// Record one migration abandoned because the object moved under it.
pub fn record_tiering_cancelled() {
    metrics::counter!(TIERING_CANCELLED_TOTAL).increment(1);
}

/// Set the number of tiering-originated transcode jobs still in flight.
pub fn set_tiering_queue_depth(depth: usize) {
    metrics::gauge!(TIERING_QUEUE_DEPTH).set(depth as f64);
}

/// Record one object whose RF=3 copy has been written back.
///
/// Counted before readers move, so the gap against the committed count is the
/// number of promotions currently sitting on two copies of themselves.
pub fn record_promotion_restored(object_size: u64) {
    metrics::counter!(TIERING_PROMOTION_RESTORED_TOTAL).increment(1);
    metrics::counter!(TIERING_PROMOTION_RESTORED_BYTES_TOTAL).increment(object_size);
}

/// Record one object whose readers were switched back to the hot path.
pub fn record_promotion_committed() {
    metrics::counter!(TIERING_PROMOTION_COMMITTED_TOTAL).increment(1);
}

/// Record one promoted object whose shards and manifest were removed.
pub fn record_promotion_shards_purged() {
    metrics::counter!(TIERING_PROMOTION_PURGED_TOTAL).increment(1);
}

/// Record one reverse migration dropped because a client wrote the object.
pub fn record_promotion_superseded() {
    metrics::counter!(TIERING_PROMOTION_SUPERSEDED_TOTAL).increment(1);
}

/// Record one reverse migration that exhausted its retries.
///
/// Every one of these needs an operator: the object is intact and readable, but
/// it is holding an unused RF=3 copy alongside its shards.
pub fn record_promotion_failed() {
    metrics::counter!(TIERING_PROMOTION_FAILED_TOTAL).increment(1);
}

/// Set the number of reverse migrations still in flight.
pub fn set_promotion_queue_depth(depth: usize) {
    metrics::gauge!(TIERING_PROMOTION_QUEUE_DEPTH).set(depth as f64);
}

/// Every tiering gauge, in the order an exporter should first see them.
///
/// Kept as data rather than a sequence of calls so that
/// [`init_tiering_metrics`] and the test that guards it read the same list: a
/// metric added to the module but forgotten here fails
/// `every_tiering_metric_is_initialized` instead of quietly going missing from
/// a dashboard.
const TIERING_GAUGES: &[&str] = &[
    TIERING_QUEUE_DEPTH,
    TIERING_PROMOTION_QUEUE_DEPTH,
    TIERING_HEAT_PENDING_DEPTH,
];

/// Every tiering counter. See [`TIERING_GAUGES`] for why this is a list.
const TIERING_COUNTERS: &[&str] = &[
    TIERING_SCAN_CYCLES_TOTAL,
    TIERING_SCANNED_TOTAL,
    TIERING_QUEUED_TOTAL,
    TIERING_QUEUED_BYTES_TOTAL,
    TIERING_SKIPPED_TOTAL,
    TIERING_CANCELLED_TOTAL,
    TIERING_HEAT_SIGNALS_TOTAL,
    TIERING_HEAT_SIGNALS_DROPPED_TOTAL,
    TIERING_HEAT_FLUSHES_TOTAL,
    TIERING_HEAT_RECORDS_WRITTEN_TOTAL,
    TIERING_PROMOTION_RESTORED_TOTAL,
    TIERING_PROMOTION_RESTORED_BYTES_TOTAL,
    TIERING_PROMOTION_COMMITTED_TOTAL,
    TIERING_PROMOTION_PURGED_TOTAL,
    TIERING_PROMOTION_SUPERSEDED_TOTAL,
    TIERING_PROMOTION_FAILED_TOTAL,
];

/// Register every tiering metric at zero so `/metrics` carries the whole set
/// from the moment the node starts.
///
/// Without this a counter is absent from the exposition until the first event,
/// which turns an alert like `rate(s4_ec_tiering_promotion_failed_total[5m])`
/// into "no data" on exactly the node that has never failed a promotion. The
/// set has to be complete: a dashboard where half the panels populate at boot
/// and half stay empty is harder to read than one where none do.
pub fn init_tiering_metrics() {
    for name in TIERING_GAUGES {
        metrics::gauge!(*name).set(0.0);
    }
    for name in TIERING_COUNTERS {
        metrics::counter!(*name).increment(0);
    }
}

/// The `policy` label value of a topology policy.
fn layout_policy_label(policy: EcTopologyPolicy) -> &'static str {
    match policy {
        EcTopologyPolicy::Strict => "strict",
        EcTopologyPolicy::Warn => "warn",
    }
}

/// Count one new set that could not be laid out by its LRC group rule.
///
/// Under `strict` that set was refused; under `warn` it was created with
/// degraded topology.
pub fn record_set_layout_unsatisfied(reason: EcLayoutViolationReason, policy: EcTopologyPolicy) {
    metrics::counter!(
        SET_LAYOUT_UNSATISFIED_TOTAL,
        LAYOUT_REASON_LABEL => reason.as_str(),
        LAYOUT_POLICY_LABEL => layout_policy_label(policy),
    )
    .increment(1);
}

/// Every `(reason, policy)` pair the layout counter can carry.
fn layout_label_pairs() -> Vec<(&'static str, &'static str)> {
    LAYOUT_REASONS
        .iter()
        .flat_map(|reason| {
            LAYOUT_POLICIES
                .iter()
                .map(move |policy| (reason.as_str(), layout_policy_label(*policy)))
        })
        .collect()
}

/// Register every series of the layout counter at zero, so an alert on it
/// reads zero rather than "no data" on a node that never refused a layout.
pub fn init_layout_metrics() {
    for (reason, policy) in layout_label_pairs() {
        metrics::counter!(
            SET_LAYOUT_UNSATISFIED_TOTAL,
            LAYOUT_REASON_LABEL => reason,
            LAYOUT_POLICY_LABEL => policy,
        )
        .increment(0);
    }
}

/// Publish, for every set of the pool, whether it breaches its own layout rule.
///
/// A set missing from `violations` publishes zero on both origins rather than
/// being left out, for the reason every per-set gauge here does: an absent
/// series reads as "no data", and a gauge that stops being written keeps its
/// last value, so a set that was repaired would look like a violator forever.
pub fn set_layout_violations(known_sets: &[u64], violations: &HashMap<u64, LayoutViolationOrigin>) {
    for (erasure_set_id, origin, value) in per_set_violation_values(known_sets, violations) {
        metrics::gauge!(
            SET_LAYOUT_VIOLATIONS,
            ERASURE_SET_LABEL => set_label(erasure_set_id),
            LAYOUT_ORIGIN_LABEL => origin,
        )
        .set(value);
    }
}

/// The value to publish for each known set and origin.
///
/// Split from the publishing so the "a set is 1 on its own origin and 0 on the
/// other" rule can be read back in a test without a metrics recorder.
fn per_set_violation_values(
    known_sets: &[u64],
    violations: &HashMap<u64, LayoutViolationOrigin>,
) -> Vec<(u64, &'static str, f64)> {
    let mut result = Vec::with_capacity(known_sets.len() * LAYOUT_ORIGINS.len());
    for &erasure_set_id in known_sets {
        let current = violations.get(&erasure_set_id).copied();
        for origin in LAYOUT_ORIGINS {
            result.push((
                erasure_set_id,
                origin.as_str(),
                f64::from(u8::from(current == Some(origin))),
            ));
        }
    }
    result
}

/// Count `bytes` a repair moved on `leg` within `scope`.
///
/// Not split per erasure set, like the repair bandwidth limits: a zone link is
/// one physical thing whichever set's shards cross it.
pub fn record_repair_bytes(leg: RepairLeg, scope: TransferScope, bytes: u64) {
    metrics::counter!(
        REPAIR_BYTES_TOTAL,
        REPAIR_LEG_LABEL => leg.as_str(),
        TRANSFER_SCOPE_LABEL => scope.as_str(),
    )
    .increment(bytes);
}

/// Count one shard fetch of `bytes` that a read made within `scope`.
///
/// Both counters move together, because the question they answer together —
/// how much of a read is round trips and how much is payload — cannot be asked
/// of either alone. A fetch that returned nothing still counts as a fetch: the
/// round trip was made and paid for.
pub fn record_read_shard_fetch(scope: TransferScope, bytes: u64) {
    record_read_shard_fetch_by(scope, 1, bytes);
}

/// Register every series of the read traffic counters at zero, so a dashboard
/// comparing read traffic before and after a switch of
/// `S4_EC_MINIMAL_READ_ENABLED` reads zero on a node that has served nothing
/// yet rather than "no data".
pub fn init_read_traffic_metrics() {
    for scope in TransferScope::ALL {
        record_read_shard_fetch_by(scope, 0, 0);
    }
}

/// Count one resolution of a small object's placement record.
pub fn record_small_placement_resolve(source: SmallPlacementSource) {
    metrics::counter!(SMALL_PLACEMENT_RESOLVES_TOTAL, "source" => source.as_str()).increment(1);
}

/// Count a nonempty fallback before its I/O, independently of the result.
pub fn record_small_placement_fallback_attempt() {
    metrics::counter!(SMALL_PLACEMENT_FALLBACK_ATTEMPTS_TOTAL).increment(1);
}

/// Count one placement hint reaching `outcome`.
pub fn record_small_placement_hint(outcome: SmallPlacementHintOutcome) {
    metrics::counter!(SMALL_PLACEMENT_HINTS_TOTAL, "outcome" => outcome.as_str()).increment(1);
}

/// Register every series of both small-placement counters at zero, so that a
/// pool that has never fanned out reads zero rather than "no data".
pub fn init_small_placement_metrics() {
    metrics::counter!(SMALL_PLACEMENT_FALLBACK_ATTEMPTS_TOTAL).increment(0);
    for source in SmallPlacementSource::ALL {
        metrics::counter!(SMALL_PLACEMENT_RESOLVES_TOTAL, "source" => source.as_str()).increment(0);
    }
    for outcome in SmallPlacementHintOutcome::ALL {
        metrics::counter!(SMALL_PLACEMENT_HINTS_TOTAL, "outcome" => outcome.as_str()).increment(0);
    }
}

/// Where a small object's placement record was found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SmallPlacementSource {
    /// From the nodes that hold the object's replicas.
    Replicas,
    /// From the rest of the pool, after the replicas had nothing.
    PoolFanout,
    /// Nowhere: the object has no placement record.
    Absent,
}

impl SmallPlacementSource {
    /// Every source, in the order a dashboard lists them.
    pub const ALL: [Self; 3] = [Self::Replicas, Self::PoolFanout, Self::Absent];

    /// Label value for this source.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Replicas => "replicas",
            Self::PoolFanout => "pool_fanout",
            Self::Absent => "absent",
        }
    }
}

/// What became of a placement record that owed copies to replicas.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SmallPlacementHintOutcome {
    /// A write could not reach a majority of the object's replicas.
    Recorded,
    /// The sweep delivered the record and the debt is settled.
    Cleared,
    /// The object is gone, so the record it owed is moot.
    Dropped,
}

impl SmallPlacementHintOutcome {
    /// Every outcome, in the order a dashboard lists them.
    pub const ALL: [Self; 3] = [Self::Recorded, Self::Cleared, Self::Dropped];

    /// Label value for this outcome.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Recorded => "recorded",
            Self::Cleared => "cleared",
            Self::Dropped => "dropped",
        }
    }
}

/// Add `fetches` and `bytes` to one scope of the read traffic counters.
///
/// Split out so that registration can write a zero to both series without
/// claiming a fetch was made.
fn record_read_shard_fetch_by(scope: TransferScope, fetches: u64, bytes: u64) {
    metrics::counter!(READ_SHARD_FETCHES_TOTAL, TRANSFER_SCOPE_LABEL => scope.as_str())
        .increment(fetches);
    metrics::counter!(READ_SHARD_BYTES_TOTAL, TRANSFER_SCOPE_LABEL => scope.as_str())
        .increment(bytes);
}

/// A limit that can hold a repair back at admission.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EcRepairLimit {
    /// Repairs running at once on this node.
    GlobalConcurrency,
    /// Repairs targeting one shard owner at once.
    NodeConcurrency,
    /// Repairs inside one erasure set at once.
    SetConcurrency,
    /// Repairs crossing one failure domain at once.
    FailureDomainConcurrency,
    /// Bytes a second this node may spend on repair.
    GlobalBandwidth,
    /// Bytes a second repair may push across one failure domain.
    FailureDomainBandwidth,
}

impl EcRepairLimit {
    /// Every limit, so each series exists before the first refusal.
    pub const ALL: [EcRepairLimit; 6] = [
        Self::GlobalConcurrency,
        Self::NodeConcurrency,
        Self::SetConcurrency,
        Self::FailureDomainConcurrency,
        Self::GlobalBandwidth,
        Self::FailureDomainBandwidth,
    ];

    /// Stable label value.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::GlobalConcurrency => "global_concurrency",
            Self::NodeConcurrency => "node_concurrency",
            Self::SetConcurrency => "set_concurrency",
            Self::FailureDomainConcurrency => "failure_domain_concurrency",
            Self::GlobalBandwidth => "global_bandwidth",
            Self::FailureDomainBandwidth => "failure_domain_bandwidth",
        }
    }
}

/// Count one repair `limit` held back at admission.
pub fn record_repair_throttled(limit: EcRepairLimit) {
    metrics::counter!(REPAIR_THROTTLED_TOTAL, REPAIR_LIMIT_LABEL => limit.as_str()).increment(1);
}

/// Register every throttle series at zero, so a pool that has never been held
/// back reads zero rather than "no data".
pub fn init_repair_throttle_metrics() {
    for limit in EcRepairLimit::ALL {
        metrics::counter!(REPAIR_THROTTLED_TOTAL, REPAIR_LIMIT_LABEL => limit.as_str())
            .increment(0);
    }
}

/// Every `(leg, scope)` pair the repair byte counter can carry.
fn repair_bytes_label_pairs() -> Vec<(&'static str, &'static str)> {
    RepairLeg::ALL
        .iter()
        .flat_map(|leg| TransferScope::ALL.iter().map(move |scope| (leg.as_str(), scope.as_str())))
        .collect()
}

/// Whether this node is serving EC reads, or is held back until it rebuilds.
///
/// Unlabelled by erasure set on purpose: the verdict is about the node, not
/// about one of its sets. A node reading 0 here holds slots it cannot answer
/// for, which is the state `slot_owners` cannot show — membership says that
/// node is alive (`EC2-NREP-02-004`).
pub const NODE_REJOIN_SERVING: &str = "s4_ec_node_rejoin_serving";

/// Which rejoin verdict this node currently stands on: 1 on one label, 0 on
/// the rest.
pub const NODE_REJOIN_DECISION: &str = "s4_ec_node_rejoin_decision";

/// Label naming a rejoin verdict.
pub const REJOIN_DECISION_LABEL: &str = "decision";

/// Publish this node's rejoin verdict.
///
/// Every decision label is written on every call, so the one that stopped
/// holding reads back falls to zero instead of keeping its last value — the
/// same rule the per-set layout gauges follow, and for the same reason.
pub fn set_node_rejoin(serving: bool, decision: &str) {
    metrics::gauge!(NODE_REJOIN_SERVING).set(f64::from(u8::from(serving)));
    for label in crate::ec_rejoin::REJOIN_DECISION_LABELS {
        metrics::gauge!(NODE_REJOIN_DECISION, REJOIN_DECISION_LABEL => label)
            .set(f64::from(u8::from(label == decision)));
    }
}

/// Register the rejoin gauges, so a node that never evaluated a return still
/// reports "serving" rather than "no data".
pub fn init_rejoin_metrics() {
    set_node_rejoin(true, "incremental_repair_allowed");
}

/// Shards of one slot owner the pool still considers missing.
///
/// The rebuild of a replaced node has no other end. The repair queue cannot
/// stand in for it: an empty queue is what a finished rebuild looks like and
/// equally what a rebuild nobody started looks like, so the count comes from
/// walking the manifests and asking the owner for each shard it should hold
/// (`EC2-NREP-03-002`).
///
/// Labelled by the owner and by nothing else. A slot belongs to a set, but the
/// question here is about a machine, and a machine holds slots in one set of a
/// pool at a time.
pub const NODE_SHARDS_MISSING: &str = "s4_ec_node_shards_missing";

/// Whether the rebuild of one slot owner has finished: 1 on the verdict in
/// force, 0 on the other two.
///
/// Reading [`NODE_SHARDS_MISSING`] alone is not enough, and this is why. A walk
/// that stopped at `S4_EC_ADMIN_SCAN_LIMIT`, or one that could not reach the
/// owner, counted neither the pool nor the shards — and it reports zero missing
/// shards, which is the one answer an operator acts on. The verdict says which
/// of the two a zero is.
pub const NODE_REBUILD_VERDICT: &str = "s4_ec_node_rebuild_verdict";

/// Label naming the slot owner a rebuild gauge is about.
pub const NODE_ID_LABEL: &str = "node_id";

/// Label naming a rebuild verdict.
pub const REBUILD_VERDICT_LABEL: &str = "verdict";

/// Publish what the pool still owes one slot owner.
///
/// Written when an operator asks for that owner's rebuild status, because the
/// count behind it is a walk over the pool's manifests rather than something a
/// scrape can afford. The series therefore appears for a node once the question
/// has been asked about it, and there is no startup registration to make it
/// read zero before then: the label is an identity, and inventing identities to
/// publish zeroes for would say the pool owes nothing to nodes it has never
/// heard of.
///
/// Every verdict label is written on every call, so a verdict that stopped
/// holding falls to zero instead of keeping its last value — the same rule
/// [`set_node_rejoin`] follows.
pub fn set_node_rebuild(node_id: &str, shards_missing: usize, verdict: &str) {
    metrics::gauge!(NODE_SHARDS_MISSING, NODE_ID_LABEL => node_id.to_string())
        .set(shards_missing as f64);
    for label in crate::ec_node_rebuild::REBUILD_VERDICT_LABELS {
        metrics::gauge!(
            NODE_REBUILD_VERDICT,
            NODE_ID_LABEL => node_id.to_string(),
            REBUILD_VERDICT_LABEL => label,
        )
        .set(f64::from(u8::from(label == verdict)));
    }
}

/// Register every series of the repair byte counter at zero, so a dashboard
/// of cross-zone repair traffic reads zero on a node that has repaired
/// nothing yet rather than "no data".
pub fn init_repair_traffic_metrics() {
    for (leg, scope) in repair_bytes_label_pairs() {
        metrics::counter!(
            REPAIR_BYTES_TOTAL,
            REPAIR_LEG_LABEL => leg,
            TRANSFER_SCOPE_LABEL => scope,
        )
        .increment(0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The name and labels `scripts/46-ec-lrc-solver-test.sh` scrapes, and
    /// what a dashboard of degraded pools matches on.
    #[test]
    fn the_unified_solver_counter_labels_are_stable() {
        assert_eq!(
            UNIFIED_SOLVER_RECOVERIES_TOTAL,
            "s4_ec_unified_solver_recoveries_total"
        );
        assert_eq!(SOLVER_OPERATION_LABEL, "operation");
        assert_eq!(
            UnifiedSolverOperation::ALL.map(UnifiedSolverOperation::as_str),
            ["read", "repair"]
        );
    }

    /// Label names and values are what the cross-zone traffic dashboards and
    /// the phase 3 exit gate of plan 05 match on.
    #[test]
    fn the_repair_byte_counter_labels_are_stable() {
        assert_eq!(REPAIR_BYTES_TOTAL, "s4_ec_repair_bytes_total");
        assert_eq!(REPAIR_LEG_LABEL, "leg");
        assert_eq!(TRANSFER_SCOPE_LABEL, "scope");
        assert_eq!(
            repair_bytes_label_pairs(),
            vec![
                ("source", "local"),
                ("source", "same_zone"),
                ("source", "cross_zone"),
                ("source", "unknown_zone"),
                ("write", "local"),
                ("write", "same_zone"),
                ("write", "cross_zone"),
                ("write", "unknown_zone"),
            ]
        );
    }

    /// The names and labels `scripts/51-ec-minimal-read-test.sh` scrapes to
    /// show that a narrowed read moved fewer bytes, and that fewer of them
    /// crossed a zone.
    #[test]
    fn the_read_traffic_counter_labels_are_stable() {
        assert_eq!(READ_SHARD_BYTES_TOTAL, "s4_ec_read_shard_bytes_total");
        assert_eq!(READ_SHARD_FETCHES_TOTAL, "s4_ec_read_shard_fetches_total");
        assert_eq!(TRANSFER_SCOPE_LABEL, "scope");
        assert_eq!(
            TransferScope::ALL.map(TransferScope::as_str),
            ["local", "same_zone", "cross_zone", "unknown_zone"]
        );
    }

    /// The names and labels `scripts/28-ec-small-object-fallback-test.sh`
    /// scrapes to show that a placement record reached its replicas and that
    /// reads found it there.
    #[test]
    fn the_small_placement_counter_labels_are_stable() {
        assert_eq!(
            SMALL_PLACEMENT_RESOLVES_TOTAL,
            "s4_ec_small_placement_resolves_total"
        );
        assert_eq!(
            SMALL_PLACEMENT_HINTS_TOTAL,
            "s4_ec_small_placement_hints_total"
        );
        assert_eq!(
            SmallPlacementSource::ALL.map(SmallPlacementSource::as_str),
            ["replicas", "pool_fanout", "absent"]
        );
        assert_eq!(
            SmallPlacementHintOutcome::ALL.map(SmallPlacementHintOutcome::as_str),
            ["recorded", "cleared", "dropped"]
        );
    }

    #[test]
    fn small_placement_metrics_are_callable_without_a_recorder() {
        init_small_placement_metrics();
        record_small_placement_resolve(SmallPlacementSource::Replicas);
        record_small_placement_hint(SmallPlacementHintOutcome::Recorded);
    }

    #[test]
    fn read_traffic_metrics_are_callable_without_a_recorder() {
        init_read_traffic_metrics();
        init_read_traffic_metrics();
        record_read_shard_fetch(TransferScope::CrossZone, 42);
    }

    #[test]
    fn repair_traffic_metrics_are_callable_without_a_recorder() {
        init_repair_traffic_metrics();
        init_repair_traffic_metrics();
        record_repair_bytes(RepairLeg::Source, TransferScope::CrossZone, 42);
    }

    /// The names and labels an alert on a held-back node matches on.
    #[test]
    fn the_rejoin_gauge_labels_are_stable() {
        assert_eq!(NODE_REJOIN_SERVING, "s4_ec_node_rejoin_serving");
        assert_eq!(NODE_REJOIN_DECISION, "s4_ec_node_rejoin_decision");
        assert_eq!(REJOIN_DECISION_LABEL, "decision");
        assert_eq!(
            crate::ec_rejoin::REJOIN_DECISION_LABELS,
            [
                "incremental_repair_allowed",
                "needs_bootstrap",
                "admin_approval_required"
            ]
        );
    }

    /// The names and labels `scripts/49-ec-node-replacement-test.sh` scrapes
    /// while it drives a whole-node rebuild, and what an alert on a pool that
    /// still owes a slot owner its shards matches on.
    #[test]
    fn the_node_rebuild_gauge_labels_are_stable() {
        assert_eq!(NODE_SHARDS_MISSING, "s4_ec_node_shards_missing");
        assert_eq!(NODE_REBUILD_VERDICT, "s4_ec_node_rebuild_verdict");
        assert_eq!(NODE_ID_LABEL, "node_id");
        assert_eq!(REBUILD_VERDICT_LABEL, "verdict");
        assert_eq!(
            crate::ec_node_rebuild::REBUILD_VERDICT_LABELS,
            ["complete", "incomplete", "unknown"]
        );
    }

    /// Every verdict the inventory can reach has a label, because a gauge
    /// cannot be absent the way the admin surface's `null` is.
    #[test]
    fn every_rebuild_verdict_has_a_label_of_its_own() {
        use crate::ec_node_rebuild::{EcRebuildVerdict, REBUILD_VERDICT_LABELS};

        for verdict in [
            EcRebuildVerdict::Complete,
            EcRebuildVerdict::Incomplete,
            EcRebuildVerdict::Unknown,
        ] {
            assert!(
                REBUILD_VERDICT_LABELS.contains(&verdict.as_str()),
                "verdict {verdict:?} is published under a label nothing publishes zero for"
            );
        }
    }

    #[test]
    fn node_rebuild_gauges_are_callable_without_a_recorder() {
        set_node_rebuild("0d3bb4d8-876d-4b6a-a27f-49a69d11d3ae", 6, "incomplete");
        set_node_rebuild("0d3bb4d8-876d-4b6a-a27f-49a69d11d3ae", 0, "complete");
    }

    /// The names and labels a rebuild dashboard matches on to show that the
    /// repair budget, rather than a stuck queue, is what is pacing a rebuild.
    #[test]
    fn the_repair_throttle_counter_labels_are_stable() {
        assert_eq!(REPAIR_THROTTLED_TOTAL, "s4_ec_repair_throttled_total");
        assert_eq!(REPAIR_LIMIT_LABEL, "limit");
        assert_eq!(
            EcRepairLimit::ALL.map(EcRepairLimit::as_str),
            [
                "global_concurrency",
                "node_concurrency",
                "set_concurrency",
                "failure_domain_concurrency",
                "global_bandwidth",
                "failure_domain_bandwidth",
            ]
        );
    }

    #[test]
    fn repair_throttle_metrics_are_callable_without_a_recorder() {
        init_repair_throttle_metrics();
        record_repair_throttled(EcRepairLimit::GlobalBandwidth);
    }

    #[test]
    fn rejoin_metrics_are_callable_without_a_recorder() {
        init_rejoin_metrics();
        set_node_rejoin(false, "needs_bootstrap");
    }

    /// The name and labels `scripts/50-ec-repair-priority-test.sh` scrapes, and
    /// what an alert on a pool close to losing redundancy matches on.
    #[test]
    fn the_repair_queue_damage_gauge_labels_are_stable() {
        assert_eq!(
            REPAIR_QUEUE_DEPTH_BY_LOSSES,
            "s4_ec_repair_queue_depth_by_losses"
        );
        assert_eq!(REPAIR_LOSSES_LABEL, "losses");
        assert_eq!(REPAIR_LOSS_BUCKETS, ["1", "2", "3", "4+"]);
    }

    /// Every loss count lands in a bucket, and the last one is open so that a
    /// chunk missing more shards than any profile survives is still counted.
    #[test]
    fn every_loss_count_has_a_bucket() {
        assert_eq!(repair_loss_bucket(1), "1");
        assert_eq!(repair_loss_bucket(2), "2");
        assert_eq!(repair_loss_bucket(3), "3");
        assert_eq!(repair_loss_bucket(4), "4+");
        assert_eq!(repair_loss_bucket(14), "4+");
        // A task counts its own shard, so this is unreachable rather than
        // meaningful; it reads as one loss instead of inventing a bucket.
        assert_eq!(repair_loss_bucket(0), "1");
    }

    /// A set whose queue holds nothing of a given damage publishes that bucket
    /// as zero, and the buckets of a set sum back to its queue depth.
    #[test]
    fn every_set_publishes_every_damage_bucket() {
        let known = [1u64, 2];
        let counts = HashMap::from([(1u64, HashMap::from([("1", 3usize), ("2", 2usize)]))]);

        let published = per_set_repair_loss_values(&known, &counts);

        assert_eq!(
            published,
            vec![
                (1, "1", 3.0),
                (1, "2", 2.0),
                (1, "3", 0.0),
                (1, "4+", 0.0),
                (2, "1", 0.0),
                (2, "2", 0.0),
                (2, "3", 0.0),
                (2, "4+", 0.0),
            ]
        );
        let set_one: f64 = published
            .iter()
            .filter(|(erasure_set_id, _, _)| *erasure_set_id == 1)
            .map(|(_, _, count)| count)
            .sum();
        assert_eq!(set_one, 5.0);
    }

    #[test]
    fn the_repair_queue_damage_gauge_is_callable_without_a_recorder() {
        let counts = HashMap::from([(1u64, HashMap::from([("2", 4usize)]))]);
        set_repair_queue_depth_by_losses(&[1], &counts);
    }

    /// The label names and values are what dashboards and alerts match on,
    /// and the values are the ones EC2-083 names.
    #[test]
    fn the_layout_counter_labels_are_stable() {
        assert_eq!(
            SET_LAYOUT_UNSATISFIED_TOTAL,
            "s4_ec_set_layout_unsatisfied_total"
        );
        assert_eq!(LAYOUT_REASON_LABEL, "reason");
        assert_eq!(LAYOUT_POLICY_LABEL, "policy");
        assert_eq!(
            layout_label_pairs(),
            vec![
                ("too_few_domains", "strict"),
                ("too_few_domains", "warn"),
                ("domain_overloaded", "strict"),
                ("domain_overloaded", "warn"),
                ("incomplete_labels", "strict"),
                ("incomplete_labels", "warn"),
            ]
        );
    }

    /// A violating set reads 1 on the origin its violation has and 0 on the
    /// other; a set that meets its rule reads 0 on both rather than being left
    /// out, so a repaired set stops alerting instead of keeping its last value.
    #[test]
    fn a_violating_set_is_one_on_its_own_origin_and_zero_on_the_other() {
        let known = [1u64, 2, 3];
        let violations = HashMap::from([
            (1u64, LayoutViolationOrigin::Drift),
            (3u64, LayoutViolationOrigin::Creation),
        ]);

        let published = per_set_violation_values(&known, &violations);

        assert_eq!(
            published,
            vec![
                (1, "drift", 1.0),
                (1, "creation", 0.0),
                (2, "drift", 0.0),
                (2, "creation", 0.0),
                (3, "drift", 0.0),
                (3, "creation", 1.0),
            ]
        );
    }

    /// A violation held for a set the pool does not have is not published: it
    /// would invent a series no dashboard can explain.
    #[test]
    fn a_violation_for_an_unknown_set_is_not_published() {
        let known = [1u64];
        let violations = HashMap::from([(99u64, LayoutViolationOrigin::Drift)]);

        let published = per_set_violation_values(&known, &violations);

        assert_eq!(published, vec![(1, "drift", 0.0), (1, "creation", 0.0)]);
    }

    /// The gauge name and its label are what the drift dashboards and the
    /// phase 4 exit gate of plan 05 match on.
    #[test]
    fn the_layout_violation_gauge_labels_are_stable() {
        assert_eq!(SET_LAYOUT_VIOLATIONS, "s4_ec_set_layout_violations");
        assert_eq!(LAYOUT_ORIGIN_LABEL, "origin");
        assert_eq!(
            LAYOUT_ORIGINS.map(LayoutViolationOrigin::as_str),
            ["drift", "creation"]
        );
    }

    #[test]
    fn layout_violation_metrics_are_callable_without_a_recorder() {
        set_layout_violations(&[1], &HashMap::from([(1u64, LayoutViolationOrigin::Drift)]));
        set_layout_violations(&[1], &HashMap::new());
    }

    #[test]
    fn layout_metrics_are_callable_without_a_recorder() {
        init_layout_metrics();
        init_layout_metrics();
        record_set_layout_unsatisfied(
            EcLayoutViolationReason::DomainOverloaded,
            EcTopologyPolicy::Warn,
        );
    }

    #[test]
    fn metric_names_are_stable() {
        let names = [
            TRANSCODE_JOBS_TOTAL,
            TRANSCODE_FAILURES_TOTAL,
            TRANSCODE_QUEUE_DEPTH,
            INLINE_WRITES_TOTAL,
            INLINE_WRITES_REJECTED_TOTAL,
            READ_DEGRADED_TOTAL,
            SHARD_HASH_MISMATCH_TOTAL,
            REPAIR_JOBS_TOTAL,
            REPAIR_QUEUE_DEPTH,
            REPAIR_QUEUE_DEPTH_BY_LOSSES,
            REPAIR_SUCCEEDED_TOTAL,
            REPAIR_FAILURES_TOTAL,
            ORPHAN_SHARDS_TOTAL,
            MANIFEST_QUORUM_FAILURES_TOTAL,
            HOT_REPLICAS_GC_ELIGIBLE_TOTAL,
            HOT_REPLICAS_PURGED_TOTAL,
            TIERING_HEAT_SIGNALS_TOTAL,
            TIERING_HEAT_SIGNALS_DROPPED_TOTAL,
            TIERING_HEAT_FLUSHES_TOTAL,
            TIERING_HEAT_RECORDS_WRITTEN_TOTAL,
            TIERING_HEAT_PENDING_DEPTH,
            TIERING_SCAN_CYCLES_TOTAL,
            TIERING_SCANNED_TOTAL,
            TIERING_QUEUED_TOTAL,
            TIERING_SKIPPED_TOTAL,
            TIERING_CANCELLED_TOTAL,
            TIERING_QUEUE_DEPTH,
            TIERING_PROMOTION_RESTORED_TOTAL,
            TIERING_PROMOTION_RESTORED_BYTES_TOTAL,
            TIERING_PROMOTION_COMMITTED_TOTAL,
            TIERING_PROMOTION_PURGED_TOTAL,
            TIERING_PROMOTION_SUPERSEDED_TOTAL,
            TIERING_PROMOTION_FAILED_TOTAL,
            TIERING_PROMOTION_QUEUE_DEPTH,
            TIERING_QUEUED_BYTES_TOTAL,
            PACKED_SEGMENTS,
            PACKED_SEGMENT_USED_BYTES,
            PACKED_SEGMENT_DEAD_BYTES,
            PACKED_SEGMENT_DEAD_RATIO,
            PACKED_REPACK_QUEUE_DEPTH,
            PACKED_DIRECT_READS_TOTAL,
            PACKED_DEGRADED_READS_TOTAL,
            PACKED_REPACK_SEGMENTS_TOTAL,
            PACKED_REPACK_OBJECTS_TOTAL,
            PACKED_REPACK_RECLAIMED_BYTES_TOTAL,
            PACKED_REPACK_BLOCKED_TOTAL,
            PACKED_SEGMENTS_RETIRED_TOTAL,
            PACKED_SEGMENTS_UNPACKED_TOTAL,
            SET_LAYOUT_UNSATISFIED_TOTAL,
            SET_LAYOUT_VIOLATIONS,
            REPAIR_BYTES_TOTAL,
        ];
        assert_eq!(names.len(), 51);
        assert!(names.iter().all(|name| name.starts_with("s4_ec_")));

        // The guard below only means something if it is checking against the
        // full inventory, so tie the two together here.
        for name in TIERING_GAUGES.iter().chain(TIERING_COUNTERS) {
            assert!(
                names.contains(name),
                "{name} is initialized but not inventoried"
            );
        }
    }

    /// Every `s4_ec_tiering_*` metric must be registered at startup.
    ///
    /// A counter that only appears after its first event reads as "no data" on
    /// a dashboard, which is indistinguishable from a broken exporter. Adding a
    /// tiering metric without adding it to `init_tiering_metrics` fails here.
    #[test]
    fn every_tiering_metric_is_initialized() {
        let names = [
            TIERING_HEAT_SIGNALS_TOTAL,
            TIERING_HEAT_SIGNALS_DROPPED_TOTAL,
            TIERING_HEAT_FLUSHES_TOTAL,
            TIERING_HEAT_RECORDS_WRITTEN_TOTAL,
            TIERING_HEAT_PENDING_DEPTH,
            TIERING_SCAN_CYCLES_TOTAL,
            TIERING_SCANNED_TOTAL,
            TIERING_QUEUED_TOTAL,
            TIERING_QUEUED_BYTES_TOTAL,
            TIERING_SKIPPED_TOTAL,
            TIERING_CANCELLED_TOTAL,
            TIERING_QUEUE_DEPTH,
            TIERING_PROMOTION_RESTORED_TOTAL,
            TIERING_PROMOTION_RESTORED_BYTES_TOTAL,
            TIERING_PROMOTION_COMMITTED_TOTAL,
            TIERING_PROMOTION_PURGED_TOTAL,
            TIERING_PROMOTION_SUPERSEDED_TOTAL,
            TIERING_PROMOTION_FAILED_TOTAL,
            TIERING_PROMOTION_QUEUE_DEPTH,
        ];
        assert!(names.iter().all(|name| name.starts_with("s4_ec_tiering_")));

        let initialized: Vec<&str> =
            TIERING_GAUGES.iter().chain(TIERING_COUNTERS).copied().collect();
        for name in names {
            assert!(
                initialized.contains(&name),
                "{name} is not registered by init_tiering_metrics"
            );
        }
        assert_eq!(
            initialized.len(),
            names.len(),
            "init_tiering_metrics registers a metric that is not a tiering metric"
        );

        // Registering the same name twice would be a copy-paste slip that the
        // length check above cannot see.
        let mut sorted = initialized.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(
            sorted.len(),
            initialized.len(),
            "a metric is registered twice"
        );
    }

    /// Initialization must be callable with no recorder installed and twice in
    /// a row: `EcOperationsService::open` runs on every node start, and tests
    /// open several services in one process.
    #[test]
    fn init_tiering_metrics_is_idempotent() {
        init_tiering_metrics();
        init_tiering_metrics();
    }

    /// The label name is part of every EC dashboard and alert expression, so it
    /// is fixed here rather than left to whoever edits a call site next.
    #[test]
    fn the_erasure_set_label_is_stable() {
        assert_eq!(ERASURE_SET_LABEL, "erasure_set");
    }

    /// A set with nothing queued must still be published, as a zero.
    ///
    /// Leaving it out would read as "no data" on a dashboard — the same shape a
    /// broken exporter has — and a gauge that stops being written keeps its last
    /// value, so a drained queue would look busy forever.
    #[test]
    fn a_quiet_set_is_published_as_zero_rather_than_left_out() {
        let known = [1u64, 2, 7];
        let values = HashMap::from([(1u64, 4usize), (7u64, 0usize)]);

        let published = per_set_gauge_values(&known, &values);

        assert_eq!(published, vec![(1, 4.0), (2, 0.0), (7, 0.0)]);
    }

    /// A set the pool does not have contributes nothing, even if a stale record
    /// still names it: publishing it would invent a series for a set no
    /// dashboard can explain.
    #[test]
    fn a_value_for_an_unknown_set_is_not_published() {
        let known = [1u64];
        let values = HashMap::from([(1u64, 2usize), (99u64, 5usize)]);

        let published = per_set_gauge_values(&known, &values);

        assert_eq!(published, vec![(1, 2.0)]);
    }

    /// The state label name is part of dashboards and alert expressions.
    #[test]
    fn the_packed_state_label_is_stable() {
        assert_eq!(PACKED_STATE_LABEL, "state");
    }

    /// A set with no packed segments must still publish zero bytes and ratio.
    #[test]
    fn a_set_with_no_segments_publishes_zeroes_rather_than_gaps() {
        let known = [1u64, 2, 7];
        let byte_values = HashMap::from([(1u64, 1024u64)]);
        let ratio_values = HashMap::from([(1u64, 0.25f64)]);

        let published_bytes = per_set_bytes_values(&known, &byte_values);
        assert_eq!(published_bytes, vec![(1, 1024.0), (2, 0.0), (7, 0.0)]);

        let published_ratios = per_set_ratio_values(&known, &ratio_values);
        assert_eq!(published_ratios, vec![(1, 0.25), (2, 0.0), (7, 0.0)]);
    }

    /// Every lifecycle state must be published for every known set, missing counts defaulting to zero.
    #[test]
    fn every_lifecycle_state_is_published_for_every_set() {
        assert_eq!(
            PACKED_SEGMENT_STATES,
            ["open", "sealed", "encoded", "purged", "dead"]
        );

        let known = [1u64, 2];
        let mut counts = HashMap::new();
        let mut set1 = HashMap::new();
        set1.insert("open", 3usize);
        set1.insert("sealed", 1usize);
        counts.insert(1u64, set1);

        let published = per_set_packed_segments_values(&known, &counts);
        assert_eq!(
            published,
            vec![
                (1, "open", 3.0),
                (1, "sealed", 1.0),
                (1, "encoded", 0.0),
                (1, "purged", 0.0),
                (1, "dead", 0.0),
                (2, "open", 0.0),
                (2, "sealed", 0.0),
                (2, "encoded", 0.0),
                (2, "purged", 0.0),
                (2, "dead", 0.0),
            ]
        );
    }
}
