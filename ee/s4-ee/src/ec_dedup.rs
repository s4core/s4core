// Copyright 2026 S4Core Team
//
// Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

//! Measurement of how much erasure-coded content is stored more than once.
//!
//! This module is the whole of phase 0 of plan 07: it answers whether dedup at
//! the erasure-coding tier would save anything **beyond** what content-addressable
//! dedup on the write path already saves, and it decides nothing else. No index,
//! no reference count and no garbage collection live here — those belong to the
//! later phases, and those phases only run if the number this module produces is
//! above the threshold fixed in decision `EC2-121`.
//!
//! Three properties of the answer matter more than the answer itself, because
//! each of them can turn a correct number into a wrong decision:
//!
//! 1. **Only repeats inside one erasure set are savings.** Shards live on the
//!    nodes of a specific set, so a manifest of another set could not reference
//!    them. A repeat spread over two sets is counted, reported, and excluded
//!    from the saving.
//! 2. **Only bytes that erasure coding actually stores count.** Tombstones,
//!    promoted objects and permanently failed transcodes describe no shards.
//!    Packed objects share one encoded segment already, so sharing shard sets
//!    between their manifests would save nothing.
//! 3. **An incomplete walk decides nothing.** A node that was not read is not a
//!    node without duplicates. The report says how much of the pool it covered,
//!    and refuses a verdict when that is less than all of it.

use std::collections::{BTreeMap, HashMap};

use s4_cluster::EcContentDigest;
use serde::{Deserialize, Serialize};

/// Share of redundant bytes at or above which plan 07 proceeds (`EC2-121`).
pub const DEDUP_GO_RATIO: f64 = 0.25;

/// Share of redundant bytes below which plan 07 is closed (`EC2-121`).
pub const DEDUP_NO_GO_RATIO: f64 = 0.10;

/// Fewest distinct EC objects a measurement needs to be evidence (`EC2-121`).
pub const DEDUP_MIN_SAMPLE_OBJECTS: usize = 1_000;

/// Duplicate groups listed as examples in one report.
const MAX_SAMPLE_GROUPS: usize = 20;

/// Objects listed inside one example group.
const MAX_SAMPLE_OBJECTS: usize = 8;

/// Decision record this measurement is evaluated against.
const BENEFIT_THRESHOLD_ADR: &str = "EC2-121";

/// One node the report could not read, and why.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EcDedupUnreadNode {
    /// Node that did not contribute its manifests.
    pub node_id: String,
    /// Why it did not: unreachable, too old to know the call, or failing.
    pub reason: String,
}

/// How much of the pool the measurement actually covered.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EcDedupScope {
    /// Nodes holding shards of this pool.
    pub nodes_total: usize,
    /// Nodes that answered with their manifests, including this one.
    pub nodes_answered: usize,
    /// Nodes that did not answer, with the reason for each.
    pub nodes_unread: Vec<EcDedupUnreadNode>,
    /// Manifests one node was allowed to walk.
    pub scan_limit: usize,
    /// Nodes whose walk stopped at the limit instead of the end of the store.
    pub nodes_truncated: usize,
    /// True when every node answered and none of them hit the limit.
    pub complete: bool,
}

/// Counts describing the population the ratio is taken over.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EcDedupTotals {
    /// Manifests read across all answering nodes, before any filtering.
    pub manifests_scanned: usize,
    /// Digests received, including the same object reported by several nodes.
    pub digests_received: usize,
    /// Distinct object versions that erasure coding stores bytes for.
    pub objects: usize,
    /// Bytes those objects hold, before encoding — the ratio's denominator.
    pub ec_bytes: u64,
    /// Distinct content hashes among them.
    pub distinct_contents: usize,
    /// Objects left out because their bytes live inside a packed segment.
    pub packed_objects_excluded: usize,
    /// Content hashes seen with more than one object size.
    ///
    /// Identical content cannot have two sizes, so a non-zero count means a
    /// manifest is wrong somewhere. Reported rather than ignored, because a
    /// measurement built on such rows would be reporting a defect as a saving.
    pub content_size_conflicts: usize,
}

/// What sharing shards would save over one class of repeats.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
pub struct EcDedupSavings {
    /// Groups of two or more objects with identical content.
    pub groups: usize,
    /// Objects that are repeats — one copy per group is never a repeat.
    pub redundant_objects: usize,
    /// Bytes those repeats occupy.
    pub redundant_bytes: u64,
    /// `redundant_bytes` as a share of [`EcDedupTotals::ec_bytes`].
    pub redundant_ratio: f64,
}

impl EcDedupSavings {
    /// Finish the ratio once the denominator is known.
    fn with_ratio(mut self, ec_bytes: u64) -> Self {
        self.redundant_ratio = ratio(self.redundant_bytes, ec_bytes);
        self
    }
}

/// How the objects of a duplicate group relate to each other.
///
/// This is what the gap analysis of `EC2-DDP-00-002` reads: each relation points
/// at a different reason why write-path dedup left the repeat in place, and
/// therefore at a different fix.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EcDedupRelation {
    /// Every object is a version of one key: the same content re-uploaded.
    Versions,
    /// Different keys of one bucket.
    SameBucket,
    /// The group spans buckets.
    CrossBucket,
}

impl EcDedupRelation {
    /// Stable name used as a JSON object key.
    fn as_str(self) -> &'static str {
        match self {
            Self::Versions => "versions",
            Self::SameBucket => "same_bucket",
            Self::CrossBucket => "cross_bucket",
        }
    }
}

/// One object named in an example group.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EcDedupSampleObject {
    /// Bucket of the object.
    pub bucket: String,
    /// Key of the object.
    pub key: String,
    /// Version ID, absent for an unversioned object.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version_id: Option<String>,
}

/// One duplicate group, shown so the numbers can be checked against reality.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EcDedupSample {
    /// Content hash shared by the group, hex-encoded.
    pub content_hash_sha256: String,
    /// Erasure set the group sits in.
    pub erasure_set_id: u64,
    /// How its objects relate to each other.
    pub relation: EcDedupRelation,
    /// Objects in the group.
    pub copies: usize,
    /// Size of one copy.
    pub object_size: u64,
    /// Bytes the repeats occupy.
    pub redundant_bytes: u64,
    /// Up to `MAX_SAMPLE_OBJECTS` of the group's objects.
    pub objects: Vec<EcDedupSampleObject>,
}

/// Where the measurement leaves plan 07.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EcDedupVerdict {
    /// Above the go threshold: the remaining phases are justified.
    Go,
    /// In the band where the gap analysis of `EC2-DDP-00-002` decides.
    Conditional,
    /// Below the threshold: plan 07 closes, and that is a normal outcome.
    NoGo,
    /// The measurement does not meet its own preconditions and decides nothing.
    Inconclusive,
}

/// The verdict together with the rule that produced it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EcDedupDecision {
    /// Decision record holding the thresholds.
    pub adr: String,
    /// Outcome of applying them to this measurement.
    pub verdict: EcDedupVerdict,
    /// Which condition decided, in words an operator can act on.
    pub reason: String,
    /// Share at or above which the verdict is `go`.
    pub go_ratio: f64,
    /// Share below which the verdict is `no_go`.
    pub no_go_ratio: f64,
    /// Fewest objects the measurement needs to count as evidence.
    pub min_sample_objects: usize,
}

/// The pool's duplicate-content measurement.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EcDedupReport {
    /// How much of the pool was read.
    pub scope: EcDedupScope,
    /// The population the shares are taken over.
    pub totals: EcDedupTotals,
    /// Savings dedup could actually realize — repeats inside one erasure set.
    pub within_set: EcDedupSavings,
    /// Savings if a shard set could be shared across erasure sets. It cannot;
    /// the figure exists to show what the set boundary costs.
    pub ignoring_set_boundary: EcDedupSavings,
    /// Bytes that are repeats but sit in different sets, so no dedup reaches them.
    pub lost_to_set_boundary_bytes: u64,
    /// Within-set savings split by how the repeated objects relate.
    pub by_relation: BTreeMap<String, EcDedupSavings>,
    /// The largest duplicate groups, for checking the numbers against reality.
    pub samples: Vec<EcDedupSample>,
    /// What this measurement means for plan 07.
    pub decision: EcDedupDecision,
}

/// One object as the merge keeps it, keyed by its object version.
#[derive(Debug, Clone)]
struct MergedObject {
    digest: EcContentDigest,
}

/// Facts about the walk that the digests themselves do not carry.
#[derive(Debug, Clone, Default)]
pub struct EcDedupScanFacts {
    /// Nodes holding shards of this pool.
    pub nodes_total: usize,
    /// Nodes that answered.
    pub nodes_answered: usize,
    /// Nodes that did not, with a reason each.
    pub nodes_unread: Vec<EcDedupUnreadNode>,
    /// Manifests one node was allowed to walk.
    pub scan_limit: usize,
    /// Nodes whose walk stopped at the limit.
    pub nodes_truncated: usize,
    /// Manifests read across all answering nodes.
    pub manifests_scanned: usize,
}

/// Build the report from the digests every answering node returned.
///
/// The same object may arrive from several nodes — a manifest is committed to a
/// quorum and cached by readers — so objects are merged by their version key
/// before anything is counted. Without that, a widely-read object would look
/// like its own duplicate.
pub fn build_dedup_report(digests: Vec<EcContentDigest>, facts: EcDedupScanFacts) -> EcDedupReport {
    let digests_received = digests.len();

    let mut packed_objects_excluded = 0usize;
    let mut merged: HashMap<(String, String, Option<String>), MergedObject> = HashMap::new();
    for digest in digests {
        if digest.packed {
            packed_objects_excluded += 1;
            continue;
        }
        let key = (
            digest.bucket.clone(),
            digest.key.clone(),
            digest.version_id.clone(),
        );
        merged.entry(key).or_insert(MergedObject { digest });
    }
    // The same object seen twice is one object, so the exclusion count has to be
    // per object as well, not per digest received.
    packed_objects_excluded = packed_objects_excluded.min(digests_received);

    let mut objects = merged.into_values().map(|object| object.digest).collect::<Vec<_>>();
    // Storage order differs between nodes, so the merge has to impose its own.
    // Two runs over the same data must produce the same samples, or comparing
    // one report with the next means nothing.
    objects.sort_by(|left, right| {
        left.content_hash_sha256
            .cmp(&right.content_hash_sha256)
            .then_with(|| left.erasure_set_id.cmp(&right.erasure_set_id))
            .then_with(|| left.bucket.cmp(&right.bucket))
            .then_with(|| left.key.cmp(&right.key))
            .then_with(|| left.version_id.cmp(&right.version_id))
    });

    let ec_bytes = objects.iter().map(|object| object.original_size).sum::<u64>();

    // Grouped twice: once ignoring the set boundary, to show what that boundary
    // costs, and once respecting it, which is the only figure dedup could ever
    // deliver.
    let by_content = group_by(&objects, |object| (object.content_hash_sha256, 0u64));
    let by_content_and_set = group_by(&objects, |object| {
        (object.content_hash_sha256, object.erasure_set_id)
    });

    let distinct_contents = by_content.len();
    let content_size_conflicts = by_content
        .values()
        .filter(|group| {
            let first = group.first().map(|object| object.original_size);
            group.iter().any(|object| Some(object.original_size) != first)
        })
        .count();

    let ignoring_set_boundary = savings_over(by_content.values()).with_ratio(ec_bytes);
    let within_set = savings_over(by_content_and_set.values()).with_ratio(ec_bytes);
    let lost_to_set_boundary_bytes =
        ignoring_set_boundary.redundant_bytes.saturating_sub(within_set.redundant_bytes);

    let mut by_relation: BTreeMap<String, EcDedupSavings> = BTreeMap::new();
    for relation in [
        EcDedupRelation::Versions,
        EcDedupRelation::SameBucket,
        EcDedupRelation::CrossBucket,
    ] {
        let groups = by_content_and_set
            .values()
            .filter(|group| group.len() > 1 && group_relation(group) == relation);
        by_relation.insert(
            relation.as_str().to_string(),
            savings_over(groups).with_ratio(ec_bytes),
        );
    }

    let samples = build_samples(&by_content_and_set);

    let totals = EcDedupTotals {
        manifests_scanned: facts.manifests_scanned,
        digests_received,
        objects: objects.len(),
        ec_bytes,
        distinct_contents,
        packed_objects_excluded,
        content_size_conflicts,
    };
    let scope = EcDedupScope {
        nodes_total: facts.nodes_total,
        nodes_answered: facts.nodes_answered,
        complete: facts.nodes_unread.is_empty()
            && facts.nodes_answered >= facts.nodes_total
            && facts.nodes_truncated == 0,
        nodes_unread: facts.nodes_unread,
        scan_limit: facts.scan_limit,
        nodes_truncated: facts.nodes_truncated,
    };
    let decision = decide(&scope, &totals, &within_set);

    EcDedupReport {
        scope,
        totals,
        within_set,
        ignoring_set_boundary,
        lost_to_set_boundary_bytes,
        by_relation,
        samples,
        decision,
    }
}

/// Apply the thresholds of `EC2-121` to a finished measurement.
///
/// The preconditions are checked before the ratio, and they are checked in the
/// order that names the most actionable failure first. A report that does not
/// cover the pool is not a low number — it is no number at all, and reading it
/// as "no duplicates found" is exactly the mistake the decision record forbids.
fn decide(
    scope: &EcDedupScope,
    totals: &EcDedupTotals,
    within_set: &EcDedupSavings,
) -> EcDedupDecision {
    let (verdict, reason) = if !scope.nodes_unread.is_empty() {
        (
            EcDedupVerdict::Inconclusive,
            format!(
                "{} of {} node(s) did not report their manifests; a node that was not read is not \
                 a node without duplicates",
                scope.nodes_unread.len(),
                scope.nodes_total
            ),
        )
    } else if scope.nodes_truncated > 0 {
        (
            EcDedupVerdict::Inconclusive,
            format!(
                "{} node(s) stopped at the scan limit of {}, so the walk covered a prefix of the \
                 store and not all of it; raise S4_EC_ADMIN_SCAN_LIMIT above the pool's manifest \
                 count and measure again",
                scope.nodes_truncated, scope.scan_limit
            ),
        )
    } else if totals.objects < DEDUP_MIN_SAMPLE_OBJECTS {
        (
            EcDedupVerdict::Inconclusive,
            format!(
                "{} erasure-coded object(s) is below the {DEDUP_MIN_SAMPLE_OBJECTS} this \
                 measurement needs to be evidence rather than noise",
                totals.objects
            ),
        )
    } else if within_set.redundant_ratio < DEDUP_NO_GO_RATIO {
        (
            EcDedupVerdict::NoGo,
            format!(
                "{:.4}% of erasure-coded bytes are repeats inside one set, below the {:.0}% that \
                 would justify a distributed reference count; plan 07 closes",
                within_set.redundant_ratio * 100.0,
                DEDUP_NO_GO_RATIO * 100.0
            ),
        )
    } else if within_set.redundant_ratio > DEDUP_GO_RATIO {
        (
            EcDedupVerdict::Go,
            format!(
                "{:.4}% of erasure-coded bytes are repeats inside one set, above the {:.0}% go \
                 threshold",
                within_set.redundant_ratio * 100.0,
                DEDUP_GO_RATIO * 100.0
            ),
        )
    } else {
        (
            EcDedupVerdict::Conditional,
            format!(
                "{:.4}% of erasure-coded bytes are repeats inside one set, between the two \
                 thresholds; EC2-DDP-00-002 decides, and it decides against starting if the \
                 repeats are explained by a defect in write-path dedup",
                within_set.redundant_ratio * 100.0
            ),
        )
    };

    EcDedupDecision {
        adr: BENEFIT_THRESHOLD_ADR.to_string(),
        verdict,
        reason,
        go_ratio: DEDUP_GO_RATIO,
        no_go_ratio: DEDUP_NO_GO_RATIO,
        min_sample_objects: DEDUP_MIN_SAMPLE_OBJECTS,
    }
}

/// Group sorted objects by a key, keeping each group in the sorted order.
fn group_by<K, F>(objects: &[EcContentDigest], key: F) -> BTreeMap<K, Vec<&EcContentDigest>>
where
    K: Ord,
    F: Fn(&EcContentDigest) -> K,
{
    let mut groups: BTreeMap<K, Vec<&EcContentDigest>> = BTreeMap::new();
    for object in objects {
        groups.entry(key(object)).or_default().push(object);
    }
    groups
}

/// Sum what sharing one copy per group would save.
///
/// The saving of a group of `n` objects is `n - 1` copies, never `n`: one copy
/// is stored either way. When a group somehow holds two different sizes, the
/// largest is the one kept, so the saving is the smallest it could be — an
/// inconsistency must not inflate the case for the feature.
fn savings_over<'a, I>(groups: I) -> EcDedupSavings
where
    I: IntoIterator<Item = &'a Vec<&'a EcContentDigest>>,
{
    let mut savings = EcDedupSavings::default();
    for group in groups {
        if group.len() < 2 {
            continue;
        }
        let total = group.iter().map(|object| object.original_size).sum::<u64>();
        let kept = group.iter().map(|object| object.original_size).max().unwrap_or(0);
        savings.groups += 1;
        savings.redundant_objects += group.len() - 1;
        savings.redundant_bytes += total.saturating_sub(kept);
    }
    savings
}

/// Classify a group by the widest difference between its objects.
///
/// The widest one, not the narrowest: a group of two versions of one key plus an
/// object under another key is only fully collapsed by dedup that crosses keys,
/// so calling it a version repeat would understate what it takes to collapse it.
fn group_relation(group: &[&EcContentDigest]) -> EcDedupRelation {
    let Some(first) = group.first() else {
        return EcDedupRelation::Versions;
    };
    if group.iter().any(|object| object.bucket != first.bucket) {
        EcDedupRelation::CrossBucket
    } else if group.iter().any(|object| object.key != first.key) {
        EcDedupRelation::SameBucket
    } else {
        EcDedupRelation::Versions
    }
}

/// Take the groups worth the most bytes as examples.
fn build_samples(groups: &BTreeMap<([u8; 32], u64), Vec<&EcContentDigest>>) -> Vec<EcDedupSample> {
    let mut samples = groups
        .iter()
        .filter(|(_, group)| group.len() > 1)
        .map(|((hash, set_id), group)| {
            let total = group.iter().map(|object| object.original_size).sum::<u64>();
            let kept = group.iter().map(|object| object.original_size).max().unwrap_or(0);
            EcDedupSample {
                content_hash_sha256: hex_encode(hash),
                erasure_set_id: *set_id,
                relation: group_relation(group),
                copies: group.len(),
                object_size: kept,
                redundant_bytes: total.saturating_sub(kept),
                objects: group
                    .iter()
                    .take(MAX_SAMPLE_OBJECTS)
                    .map(|object| EcDedupSampleObject {
                        bucket: object.bucket.clone(),
                        key: object.key.clone(),
                        version_id: object.version_id.clone(),
                    })
                    .collect(),
            }
        })
        .collect::<Vec<_>>();
    samples.sort_by(|left, right| {
        right
            .redundant_bytes
            .cmp(&left.redundant_bytes)
            .then_with(|| left.content_hash_sha256.cmp(&right.content_hash_sha256))
    });
    samples.truncate(MAX_SAMPLE_GROUPS);
    samples
}

fn ratio(part: u64, whole: u64) -> f64 {
    if whole == 0 {
        return 0.0;
    }
    part as f64 / whole as f64
}

fn hex_encode(bytes: &[u8; 32]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn digest(
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        hash: u8,
        size: u64,
        set_id: u64,
    ) -> EcContentDigest {
        EcContentDigest {
            bucket: bucket.to_string(),
            key: key.to_string(),
            version_id: version_id.map(str::to_string),
            content_hash_sha256: [hash; 32],
            original_size: size,
            erasure_set_id: set_id,
            packed: false,
        }
    }

    /// A whole, untruncated walk of every node.
    fn complete_facts(nodes: usize, manifests_scanned: usize) -> EcDedupScanFacts {
        EcDedupScanFacts {
            nodes_total: nodes,
            nodes_answered: nodes,
            nodes_unread: Vec::new(),
            scan_limit: 4_096,
            nodes_truncated: 0,
            manifests_scanned,
        }
    }

    #[test]
    fn counts_one_saving_per_group_not_one_per_object() {
        let report = build_dedup_report(
            vec![
                digest("a", "one", None, 7, 100, 1),
                digest("a", "two", None, 7, 100, 1),
                digest("a", "three", None, 7, 100, 1),
            ],
            complete_facts(1, 3),
        );

        assert_eq!(report.totals.objects, 3);
        assert_eq!(report.totals.distinct_contents, 1);
        assert_eq!(report.within_set.groups, 1);
        // Three copies of one hundred bytes save two hundred, not three hundred:
        // one copy is stored either way.
        assert_eq!(report.within_set.redundant_objects, 2);
        assert_eq!(report.within_set.redundant_bytes, 200);
        assert_eq!(report.totals.ec_bytes, 300);
    }

    #[test]
    fn distinct_content_is_never_a_repeat() {
        let report = build_dedup_report(
            vec![
                digest("a", "one", None, 1, 100, 1),
                digest("a", "two", None, 2, 100, 1),
            ],
            complete_facts(1, 2),
        );

        assert_eq!(report.within_set.groups, 0);
        assert_eq!(report.within_set.redundant_bytes, 0);
        assert_eq!(report.within_set.redundant_ratio, 0.0);
    }

    #[test]
    fn a_repeat_across_erasure_sets_is_not_a_saving() {
        let report = build_dedup_report(
            vec![
                digest("a", "one", None, 7, 100, 1),
                digest("a", "two", None, 7, 100, 2),
            ],
            complete_facts(2, 2),
        );

        // Shards of set 2 cannot be referenced from a manifest of set 1, so this
        // repeat is real but unreachable. Both figures are reported so the cost
        // of the boundary is visible instead of silently folded into the total.
        assert_eq!(report.within_set.redundant_bytes, 0);
        assert_eq!(report.ignoring_set_boundary.redundant_bytes, 100);
        assert_eq!(report.lost_to_set_boundary_bytes, 100);
    }

    #[test]
    fn the_same_object_reported_by_several_nodes_is_one_object() {
        // A manifest is committed to a quorum and cached by every node that
        // reads it, so the same object arrives many times. Counted naively, a
        // widely read object becomes its own duplicate.
        let report = build_dedup_report(
            vec![
                digest("a", "one", None, 7, 100, 1),
                digest("a", "one", None, 7, 100, 1),
                digest("a", "one", None, 7, 100, 1),
            ],
            complete_facts(3, 3),
        );

        assert_eq!(report.totals.digests_received, 3);
        assert_eq!(report.totals.objects, 1);
        assert_eq!(report.within_set.groups, 0);
        assert_eq!(report.within_set.redundant_bytes, 0);
    }

    #[test]
    fn versions_of_one_key_are_told_apart_from_repeats_across_buckets() {
        let report = build_dedup_report(
            vec![
                digest("a", "one", Some("v1"), 7, 100, 1),
                digest("a", "one", Some("v2"), 7, 100, 1),
                digest("a", "other", None, 8, 50, 1),
                digest("b", "other", None, 8, 50, 1),
            ],
            complete_facts(1, 4),
        );

        assert_eq!(report.by_relation["versions"].redundant_bytes, 100);
        assert_eq!(report.by_relation["cross_bucket"].redundant_bytes, 50);
        assert_eq!(report.by_relation["same_bucket"].redundant_bytes, 0);
    }

    #[test]
    fn a_mixed_group_is_classified_by_its_widest_difference() {
        let group = [
            digest("a", "one", Some("v1"), 7, 100, 1),
            digest("a", "one", Some("v2"), 7, 100, 1),
            digest("a", "two", None, 7, 100, 1),
        ];
        let refs = group.iter().collect::<Vec<_>>();

        assert_eq!(group_relation(&refs), EcDedupRelation::SameBucket);
    }

    #[test]
    fn packed_objects_are_left_out_of_the_measurement() {
        let mut packed = digest("a", "packed", None, 7, 100, 1);
        packed.packed = true;
        let report = build_dedup_report(
            vec![packed, digest("a", "plain", None, 7, 100, 1)],
            complete_facts(1, 2),
        );

        // Bytes in a packed segment already share one encoded segment, so
        // sharing shard sets between their manifests would save nothing.
        assert_eq!(report.totals.packed_objects_excluded, 1);
        assert_eq!(report.totals.objects, 1);
        assert_eq!(report.within_set.redundant_bytes, 0);
    }

    #[test]
    fn a_size_conflict_is_reported_and_counted_at_its_lowest() {
        let report = build_dedup_report(
            vec![
                digest("a", "one", None, 7, 100, 1),
                digest("a", "two", None, 7, 400, 1),
            ],
            complete_facts(1, 2),
        );

        assert_eq!(report.totals.content_size_conflicts, 1);
        // The larger copy is the one kept, so the saving claimed is the smaller
        // of the two readings. An inconsistent manifest must not inflate the
        // case for building the feature.
        assert_eq!(report.within_set.redundant_bytes, 100);
    }

    #[test]
    fn an_unread_node_makes_the_report_decide_nothing() {
        let mut facts = complete_facts(6, 3_000);
        facts.nodes_answered = 5;
        facts.nodes_unread = vec![EcDedupUnreadNode {
            node_id: "node-6".to_string(),
            reason: "unreachable".to_string(),
        }];
        let objects = (0..1_200)
            .map(|index| digest("a", &format!("key-{index}"), None, 1, 100, 1))
            .collect::<Vec<_>>();

        let report = build_dedup_report(objects, facts);

        // Every object here is a repeat, so the ratio is far above the go
        // threshold — and the verdict is still inconclusive, because a node that
        // was not read could have changed the number in either direction.
        assert!(report.within_set.redundant_ratio > DEDUP_GO_RATIO);
        assert!(!report.scope.complete);
        assert_eq!(report.decision.verdict, EcDedupVerdict::Inconclusive);
    }

    #[test]
    fn a_truncated_walk_decides_nothing() {
        let mut facts = complete_facts(1, 4_096);
        facts.nodes_truncated = 1;
        let objects = (0..1_200)
            .map(|index| digest("a", &format!("key-{index}"), None, 1, 100, 1))
            .collect::<Vec<_>>();

        let report = build_dedup_report(objects, facts);

        assert!(!report.scope.complete);
        assert_eq!(report.decision.verdict, EcDedupVerdict::Inconclusive);
    }

    #[test]
    fn too_few_objects_is_noise_rather_than_evidence() {
        let objects = (0..10)
            .map(|index| digest("a", &format!("key-{index}"), None, 1, 100, 1))
            .collect::<Vec<_>>();

        let report = build_dedup_report(objects, complete_facts(1, 10));

        assert_eq!(report.decision.verdict, EcDedupVerdict::Inconclusive);
        assert!(report.decision.reason.contains("noise"));
    }

    #[test]
    fn a_complete_walk_with_no_repeats_closes_the_plan() {
        let objects = (0..1_200)
            .map(|index| {
                let mut object = digest("a", &format!("key-{index}"), None, 0, 100, 1);
                // Distinct content per object.
                object.content_hash_sha256[0..4]
                    .copy_from_slice(&u32::try_from(index).unwrap_or(0).to_be_bytes());
                object
            })
            .collect::<Vec<_>>();

        let report = build_dedup_report(objects, complete_facts(1, 1_200));

        assert_eq!(report.within_set.redundant_bytes, 0);
        assert_eq!(report.decision.verdict, EcDedupVerdict::NoGo);
    }

    #[test]
    fn a_pool_of_repeats_clears_the_go_threshold() {
        // Six hundred pairs: half the bytes are repeats, which is above the go
        // threshold of a quarter.
        let objects = (0..600)
            .flat_map(|index| {
                let mut left = digest("a", &format!("left-{index}"), None, 0, 100, 1);
                let mut right = digest("b", &format!("right-{index}"), None, 0, 100, 1);
                let tag = u32::try_from(index).unwrap_or(0).to_be_bytes();
                left.content_hash_sha256[0..4].copy_from_slice(&tag);
                right.content_hash_sha256[0..4].copy_from_slice(&tag);
                [left, right]
            })
            .collect::<Vec<_>>();

        let report = build_dedup_report(objects, complete_facts(1, 1_200));

        assert_eq!(report.totals.objects, 1_200);
        assert!((report.within_set.redundant_ratio - 0.5).abs() < 1e-9);
        assert_eq!(report.decision.verdict, EcDedupVerdict::Go);
        assert_eq!(report.by_relation["cross_bucket"].groups, 600);
    }

    #[test]
    fn the_band_between_the_thresholds_hands_the_decision_to_the_gap_analysis() {
        // One object in six is a repeat: a sixth of the bytes, between the ten
        // and twenty-five percent thresholds.
        let mut objects = Vec::new();
        for index in 0..600 {
            let tag = u32::try_from(index).unwrap_or(0).to_be_bytes();
            let mut first = digest("a", &format!("key-{index}"), None, 0, 100, 1);
            first.content_hash_sha256[0..4].copy_from_slice(&tag);
            objects.push(first.clone());
            objects.push(first.clone());
            if index % 2 == 0 {
                let mut unique = digest("a", &format!("unique-{index}"), None, 0, 100, 1);
                unique.content_hash_sha256[0..4].copy_from_slice(&tag);
                unique.content_hash_sha256[4] = 1;
                objects.push(unique);
            }
        }
        // The duplicated entry above is the same object twice, so it merges into
        // one; the repeats come from the pairs below.
        for index in 0..100 {
            let tag = u32::try_from(index).unwrap_or(0).to_be_bytes();
            let mut twin = digest("a", &format!("twin-{index}"), None, 0, 100, 1);
            twin.content_hash_sha256[0..4].copy_from_slice(&tag);
            objects.push(twin);
        }

        let report = build_dedup_report(objects, complete_facts(1, 2_000));

        assert!(report.within_set.redundant_ratio >= DEDUP_NO_GO_RATIO);
        assert!(report.within_set.redundant_ratio <= DEDUP_GO_RATIO);
        assert_eq!(report.decision.verdict, EcDedupVerdict::Conditional);
    }

    #[test]
    fn samples_lead_with_the_groups_worth_the_most_bytes() {
        let report = build_dedup_report(
            vec![
                digest("a", "small-1", None, 1, 10, 1),
                digest("a", "small-2", None, 1, 10, 1),
                digest("a", "big-1", None, 2, 1_000, 1),
                digest("a", "big-2", None, 2, 1_000, 1),
            ],
            complete_facts(1, 4),
        );

        assert_eq!(report.samples.len(), 2);
        assert_eq!(report.samples[0].redundant_bytes, 1_000);
        assert_eq!(report.samples[0].copies, 2);
        assert_eq!(report.samples[1].redundant_bytes, 10);
    }

    #[test]
    fn the_report_is_stable_across_runs_over_the_same_data() {
        let objects = vec![
            digest("b", "two", None, 3, 100, 1),
            digest("a", "one", None, 3, 100, 1),
            digest("c", "three", None, 3, 100, 1),
        ];
        let mut shuffled = objects.clone();
        shuffled.reverse();

        let first = build_dedup_report(objects, complete_facts(1, 3));
        let second = build_dedup_report(shuffled, complete_facts(1, 3));

        assert_eq!(first, second);
    }
}
