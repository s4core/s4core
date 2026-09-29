// Copyright 2026 S4Core Team
//
// Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

//! Tiering policy: what to do with one object.
//!
//! The engine answers a single question — migrate this object into erasure
//! coding, bring it back to RF=3, or leave it alone — and answers it as a pure
//! function. Nothing here reads the environment, the clock or the metadata
//! store, so the whole decision table can be checked case by case.
//!
//! Two decisions shape the module. [EC2-021] puts bucket settings on top of the
//! node's global configuration: an unset field means "inherit", never "zero".
//! [EC2-025] fixes which objects are eligible at all — the current version, not
//! a delete marker, within the size bounds the transcode path can actually
//! handle.
//!
//! [EC2-021]: `EC-v2/adr/EC2-021-tiering-policies.md`
//! [EC2-025]: `EC-v2/adr/EC2-025-tiering-eligibility.md`

use std::collections::HashMap;
use std::time::Duration;

use crate::ec_metadata::{EcObjectVersionKey, EcTieringPolicyRecord};
use crate::ec_runtime::sha256;

/// How far the growing pause between two migrations of one object may stretch.
///
/// Doubling without a ceiling would take an object that migrated a handful of
/// times out of reach for years, which is indistinguishable from a bug when an
/// operator looks at it.
const MAX_MIGRATION_INTERVAL_MULTIPLIER: u32 = 32;

/// Node-wide values a bucket policy may override.
///
/// Built from `EcTieringConfig` and the active profile by the operations layer;
/// the tiering domain deliberately does not read configuration itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TieringGlobals {
    /// Whether migration is enabled on this node at all.
    pub enabled: bool,
    /// Idle time before an object is considered cold.
    pub cold_after: Duration,
    /// Smallest object worth migrating, from the erasure profile.
    pub min_object_size: u64,
    /// Largest object the transcode worker can take.
    pub max_object_size: u64,
    /// Reads within the window that bring an object back to RF=3. Zero disables.
    pub promote_after_reads: u32,
    /// Width of the read-counting window.
    pub promote_window: Duration,
    /// Base pause between two migrations of one object.
    pub min_migration_interval: Duration,
    /// Deterministic spread of the cold-after threshold, in percent.
    pub jitter_percent: u8,
}

/// One exclusion a bucket policy can express.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TieringRule {
    /// Objects whose key starts with this prefix never migrate.
    Prefix(String),
    /// Objects carrying this tag never migrate.
    Tag {
        /// Tag name.
        key: String,
        /// Tag value.
        value: String,
    },
}

impl TieringRule {
    /// Whether this rule keeps the candidate where it is.
    pub fn excludes(&self, candidate: &TieringCandidate) -> bool {
        match self {
            Self::Prefix(prefix) => candidate.object.key.starts_with(prefix.as_str()),
            Self::Tag { key, value } => {
                candidate.tags.get(key).map(String::as_str) == Some(value.as_str())
            }
        }
    }
}

/// Settings actually in force for one bucket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TieringPolicy {
    /// Bucket these settings belong to.
    pub bucket: String,
    /// Whether migration is enabled for this bucket.
    pub enabled: bool,
    /// Idle time before an object of this bucket is considered cold.
    pub cold_after: Duration,
    /// Smallest object this bucket migrates.
    pub min_object_size: u64,
    /// Largest object this bucket migrates.
    pub max_object_size: u64,
    /// Exclusions, evaluated in order.
    pub exclusions: Vec<TieringRule>,
    /// Reads within the window that bring an object back to RF=3.
    pub promote_after_reads: u32,
    /// Width of the read-counting window.
    pub promote_window: Duration,
    /// Base pause between two migrations of one object.
    pub min_migration_interval: Duration,
    /// Deterministic spread of the cold-after threshold, in percent.
    pub jitter_percent: u8,
}

impl TieringPolicy {
    /// Combine node-wide values with a bucket record.
    ///
    /// A missing record means the bucket inherits everything. A present record
    /// overrides only the fields it sets, which is why they are optional in the
    /// stored shape.
    pub fn resolve(
        bucket: impl Into<String>,
        globals: &TieringGlobals,
        record: Option<&EcTieringPolicyRecord>,
    ) -> Self {
        let bucket = bucket.into();
        let mut policy = Self {
            bucket,
            enabled: globals.enabled,
            cold_after: globals.cold_after,
            min_object_size: globals.min_object_size,
            max_object_size: globals.max_object_size,
            exclusions: Vec::new(),
            promote_after_reads: globals.promote_after_reads,
            promote_window: globals.promote_window,
            min_migration_interval: globals.min_migration_interval,
            jitter_percent: globals.jitter_percent,
        };

        let Some(record) = record else {
            return policy;
        };

        // A bucket may switch tiering off for itself, but never on where the
        // node has it off: the levels narrow downwards, never widen.
        if let Some(enabled) = record.enabled {
            policy.enabled = policy.enabled && enabled;
        }
        if let Some(cold_after_secs) = record.cold_after_secs {
            policy.cold_after = Duration::from_secs(cold_after_secs);
        }
        if let Some(min_object_size) = record.min_object_size {
            // The profile threshold is a floor, not a default: below it an
            // object is stored as replicated-small and has nothing to gain.
            policy.min_object_size = min_object_size.max(globals.min_object_size);
        }
        if let Some(promote_after_reads) = record.promote_after_reads {
            policy.promote_after_reads = promote_after_reads;
        }
        if let Some(promote_window_secs) = record.promote_window_secs {
            policy.promote_window = Duration::from_secs(promote_window_secs);
        }

        policy.exclusions = record
            .exclude_prefixes
            .iter()
            .map(|prefix| TieringRule::Prefix(prefix.clone()))
            .chain(
                record.exclude_tags.iter().map(|(key, value)| TieringRule::Tag {
                    key: key.clone(),
                    value: value.clone(),
                }),
            )
            .collect();

        policy
    }

    /// Idle time this particular object must reach before it counts as cold.
    ///
    /// The threshold is stretched by up to `jitter_percent`, deterministically
    /// per object key. A batch of objects written on one day would otherwise
    /// cool down on one day too, and the queue would see the whole batch at
    /// once. Deterministic because the answer has to be the same on every node
    /// and after every restart; a random spread would make an object at the
    /// boundary flip between cold and warm from one scan to the next.
    pub fn effective_cold_after(&self, key: &str) -> Duration {
        if self.jitter_percent == 0 {
            return self.cold_after;
        }
        let spread = self.cold_after.as_secs() / 100 * u64::from(self.jitter_percent);
        if spread == 0 {
            return self.cold_after;
        }
        self.cold_after + Duration::from_secs(key_fraction_of(key, spread))
    }

    /// The pause this object has to observe before it may migrate again.
    pub fn migration_pause(&self, migrations: u32) -> Duration {
        if migrations == 0 {
            return Duration::ZERO;
        }
        let multiplier = 1u32
            .checked_shl(migrations - 1)
            .unwrap_or(MAX_MIGRATION_INTERVAL_MULTIPLIER)
            .min(MAX_MIGRATION_INTERVAL_MULTIPLIER);
        self.min_migration_interval.saturating_mul(multiplier)
    }

    /// The first exclusion that applies, if any.
    pub fn excluded_by(&self, candidate: &TieringCandidate) -> Option<&TieringRule> {
        self.exclusions.iter().find(|rule| rule.excludes(candidate))
    }
}

/// Everything the engine needs to know about one object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TieringCandidate {
    /// Object version being considered.
    pub object: EcObjectVersionKey,
    /// Object size in bytes.
    pub size: u64,
    /// Whether the object is a delete marker.
    pub is_delete_marker: bool,
    /// Whether its bytes already live in erasure coding.
    pub erasure_coded: bool,
    /// Object tags, as stored by the S3 tagging API.
    pub tags: HashMap<String, String>,
    /// Last use, in nanoseconds since the Unix epoch.
    ///
    /// Falls back to the creation time when no access has been recorded, so an
    /// object nobody has read yet is as old as itself, never infinitely old.
    pub last_access_nanos: u64,
    /// Reads counted over the trailing promotion window, across the pool.
    ///
    /// Already evaluated against the window's width by the caller: the counting
    /// window slides, so there is no start for the engine to compare against
    /// and nothing here for it to age.
    pub window_access_count: u32,
    /// How many times this object has already changed layout.
    pub migrations: u32,
    /// When it last changed layout.
    pub last_migration_nanos: Option<u64>,
}

/// What the engine decided about one object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TieringDecision {
    /// Move the object into erasure coding.
    Demote,
    /// Bring the object back to RF=3.
    Promote,
    /// Leave it where it is, for this reason.
    Skip(TieringSkipReason),
}

/// Why an object stays where it is.
///
/// The reason travels into the dry-run report: an operator who sees that
/// nothing will migrate needs to know whether that is the thresholds, the
/// exclusions or the sizes, and a bare count would not tell them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TieringSkipReason {
    /// Tiering is off for this bucket or this node.
    Disabled,
    /// Not the current version of the object.
    NotCurrentVersion,
    /// A delete marker has no bytes to move.
    DeleteMarker,
    /// Already erasure coded and not read often enough to come back.
    AlreadyErasureCoded,
    /// Below the size where erasure coding saves anything.
    TooSmall {
        /// Object size.
        size: u64,
        /// Smallest size this bucket migrates.
        minimum: u64,
    },
    /// Above what the transcode worker can read in one piece.
    TooLarge {
        /// Object size.
        size: u64,
        /// Largest size this bucket migrates.
        maximum: u64,
    },
    /// Used too recently.
    StillWarm {
        /// How long the object has been idle.
        idle: Duration,
        /// How long it has to stay idle.
        threshold: Duration,
    },
    /// Kept in place by a bucket exclusion.
    Excluded(TieringRule),
    /// Migrated so recently that the hysteresis pause has not elapsed.
    RecentlyMigrated {
        /// Time since the last migration.
        elapsed: Duration,
        /// Pause this object has to observe.
        pause: Duration,
    },
}

/// Decide what to do with one object.
///
/// Order matters. Cheap and absolute conditions come first, so a delete marker
/// or a versioned object never reaches the arithmetic, and the reason reported
/// for a skip is the most specific one that applies.
pub fn decide(
    policy: &TieringPolicy,
    candidate: &TieringCandidate,
    now_nanos: u64,
) -> TieringDecision {
    if !policy.enabled {
        return TieringDecision::Skip(TieringSkipReason::Disabled);
    }
    // Only the current version migrates: the transcode path refuses versioned
    // sources outright, and no activity is ever recorded for an older version.
    if candidate.object.version_id.is_some() {
        return TieringDecision::Skip(TieringSkipReason::NotCurrentVersion);
    }
    if candidate.is_delete_marker {
        return TieringDecision::Skip(TieringSkipReason::DeleteMarker);
    }

    // The pause between two migrations is checked last in each branch, not
    // first. It applies to an object that would otherwise move, so reporting it
    // ahead of "too small" or "excluded by a prefix" would point the operator at
    // a knob that has nothing to do with why this object stays put.
    if candidate.erasure_coded {
        if !is_hot_again(policy, candidate) {
            return TieringDecision::Skip(TieringSkipReason::AlreadyErasureCoded);
        }
        return match hysteresis_block(policy, candidate, now_nanos) {
            Some(reason) => TieringDecision::Skip(reason),
            None => TieringDecision::Promote,
        };
    }

    if candidate.size < policy.min_object_size {
        return TieringDecision::Skip(TieringSkipReason::TooSmall {
            size: candidate.size,
            minimum: policy.min_object_size,
        });
    }
    if candidate.size > policy.max_object_size {
        return TieringDecision::Skip(TieringSkipReason::TooLarge {
            size: candidate.size,
            maximum: policy.max_object_size,
        });
    }
    if let Some(rule) = policy.excluded_by(candidate) {
        return TieringDecision::Skip(TieringSkipReason::Excluded(rule.clone()));
    }

    let idle = Duration::from_nanos(now_nanos.saturating_sub(candidate.last_access_nanos));
    let threshold = policy.effective_cold_after(&candidate.object.key);
    if idle < threshold {
        return TieringDecision::Skip(TieringSkipReason::StillWarm { idle, threshold });
    }

    match hysteresis_block(policy, candidate, now_nanos) {
        Some(reason) => TieringDecision::Skip(reason),
        None => TieringDecision::Demote,
    }
}

/// Whether an erasure-coded object is being read often enough to come back.
///
/// One read never promotes anything, and that is the point of counting at all:
/// the store walks its own objects regularly — client-invariance checks,
/// lifecycle scans, inventory — and a threshold of one would turn every such
/// sweep into a cluster-wide reverse migration. Decision EC2-023.
fn is_hot_again(policy: &TieringPolicy, candidate: &TieringCandidate) -> bool {
    if policy.promote_after_reads == 0 {
        return false;
    }
    candidate.window_access_count >= policy.promote_after_reads
}

/// The pause an object has to observe after its previous migration.
fn hysteresis_block(
    policy: &TieringPolicy,
    candidate: &TieringCandidate,
    now_nanos: u64,
) -> Option<TieringSkipReason> {
    let last_migration = candidate.last_migration_nanos?;
    let pause = policy.migration_pause(candidate.migrations);
    if pause.is_zero() {
        return None;
    }
    let elapsed = Duration::from_nanos(now_nanos.saturating_sub(last_migration));
    (elapsed < pause).then_some(TieringSkipReason::RecentlyMigrated { elapsed, pause })
}

/// A stable number in `0..span` derived from the object key.
fn key_fraction_of(key: &str, span: u64) -> u64 {
    if span == 0 {
        return 0;
    }
    let digest = sha256(key.as_bytes());
    let mut head = [0u8; 8];
    head.copy_from_slice(&digest[..8]);
    u64::from_be_bytes(head) % span
}

#[cfg(test)]
mod tests {
    use super::*;

    use s4_cluster::HlcTimestamp;

    const SECOND: u64 = 1_000_000_000;
    const DAY: u64 = 24 * 3_600 * SECOND;
    const MIB: u64 = 1024 * 1024;

    fn globals() -> TieringGlobals {
        TieringGlobals {
            enabled: true,
            cold_after: Duration::from_secs(30 * 24 * 3_600),
            min_object_size: MIB,
            max_object_size: 64 * MIB,
            promote_after_reads: 0,
            promote_window: Duration::from_secs(24 * 3_600),
            min_migration_interval: Duration::from_secs(7 * 24 * 3_600),
            jitter_percent: 0,
        }
    }

    fn policy() -> TieringPolicy {
        TieringPolicy::resolve("bucket", &globals(), None)
    }

    fn record() -> EcTieringPolicyRecord {
        EcTieringPolicyRecord::inheriting("bucket", HlcTimestamp::from_record(1, 0, 1), 1)
    }

    /// An object of 5 MiB nobody has touched for 40 days.
    fn candidate(now_nanos: u64, idle_days: u64) -> TieringCandidate {
        TieringCandidate {
            object: EcObjectVersionKey::new("bucket", "data/report.bin", None),
            size: 5 * MIB,
            is_delete_marker: false,
            erasure_coded: false,
            tags: HashMap::new(),
            last_access_nanos: now_nanos - idle_days * DAY,
            window_access_count: 0,
            migrations: 0,
            last_migration_nanos: None,
        }
    }

    fn now() -> u64 {
        400 * DAY
    }

    // ---------------------------------------------------------------- merge

    #[test]
    fn an_absent_bucket_policy_inherits_every_global_value() {
        let globals = globals();
        let policy = TieringPolicy::resolve("bucket", &globals, None);

        assert!(policy.enabled);
        assert_eq!(policy.cold_after, globals.cold_after);
        assert_eq!(policy.min_object_size, globals.min_object_size);
        assert_eq!(policy.max_object_size, globals.max_object_size);
        assert_eq!(policy.promote_after_reads, globals.promote_after_reads);
        assert!(policy.exclusions.is_empty());
    }

    #[test]
    fn a_bucket_policy_overrides_only_the_fields_it_sets() {
        let mut stored = record();
        stored.cold_after_secs = Some(7 * 24 * 3_600);
        stored.exclude_prefixes = vec!["keep-hot/".to_string()];

        let policy = TieringPolicy::resolve("bucket", &globals(), Some(&stored));

        assert_eq!(policy.cold_after, Duration::from_secs(7 * 24 * 3_600));
        assert_eq!(
            policy.exclusions,
            vec![TieringRule::Prefix("keep-hot/".to_string())]
        );
        // Untouched fields keep the node's values rather than falling to zero.
        assert_eq!(policy.min_object_size, globals().min_object_size);
        assert_eq!(policy.promote_window, globals().promote_window);
    }

    /// The levels narrow downwards. A bucket must not be able to run tiering on
    /// a node whose operator switched it off.
    #[test]
    fn a_bucket_cannot_switch_tiering_on_where_the_node_has_it_off() {
        let mut stored = record();
        stored.enabled = Some(true);
        let node_off = TieringGlobals {
            enabled: false,
            ..globals()
        };

        assert!(!TieringPolicy::resolve("bucket", &node_off, Some(&stored)).enabled);

        // The other direction works: a bucket may opt out on its own.
        stored.enabled = Some(false);
        assert!(!TieringPolicy::resolve("bucket", &globals(), Some(&stored)).enabled);
    }

    #[test]
    fn a_bucket_minimum_size_cannot_go_below_the_profile_floor() {
        let mut stored = record();
        stored.min_object_size = Some(1024);
        let policy = TieringPolicy::resolve("bucket", &globals(), Some(&stored));
        // Below the profile threshold an object is stored replicated-small, so
        // migrating it would move nothing.
        assert_eq!(policy.min_object_size, MIB);

        stored.min_object_size = Some(8 * MIB);
        let policy = TieringPolicy::resolve("bucket", &globals(), Some(&stored));
        assert_eq!(policy.min_object_size, 8 * MIB);
    }

    // ------------------------------------------------------- decision table

    #[test]
    fn a_cold_object_of_sufficient_size_migrates() {
        assert_eq!(
            decide(&policy(), &candidate(now(), 40), now()),
            TieringDecision::Demote
        );
    }

    #[test]
    fn a_recently_used_object_stays_where_it_is() {
        let decision = decide(&policy(), &candidate(now(), 3), now());
        assert!(matches!(
            decision,
            TieringDecision::Skip(TieringSkipReason::StillWarm { .. })
        ));
    }

    #[test]
    fn an_object_below_the_minimum_size_is_skipped() {
        let mut candidate = candidate(now(), 40);
        candidate.size = 4096;
        assert_eq!(
            decide(&policy(), &candidate, now()),
            TieringDecision::Skip(TieringSkipReason::TooSmall {
                size: 4096,
                minimum: MIB
            })
        );
    }

    /// Above the transcode source limit the worker would only produce retryable
    /// failures, so the candidate is filtered before it is ever queued.
    #[test]
    fn an_object_above_the_source_limit_is_skipped() {
        let mut candidate = candidate(now(), 40);
        candidate.size = 128 * MIB;
        assert_eq!(
            decide(&policy(), &candidate, now()),
            TieringDecision::Skip(TieringSkipReason::TooLarge {
                size: 128 * MIB,
                maximum: 64 * MIB
            })
        );
    }

    #[test]
    fn an_excluded_prefix_or_tag_keeps_an_object_in_place() {
        let mut stored = record();
        stored.exclude_prefixes = vec!["data/".to_string()];
        let policy = TieringPolicy::resolve("bucket", &globals(), Some(&stored));
        assert_eq!(
            decide(&policy, &candidate(now(), 40), now()),
            TieringDecision::Skip(TieringSkipReason::Excluded(TieringRule::Prefix(
                "data/".to_string()
            )))
        );

        let mut stored = record();
        stored.exclude_tags = HashMap::from([("keep".to_string(), "hot".to_string())]);
        let policy = TieringPolicy::resolve("bucket", &globals(), Some(&stored));
        let mut tagged = candidate(now(), 40);
        tagged.tags = HashMap::from([("keep".to_string(), "hot".to_string())]);
        assert_eq!(
            decide(&policy, &tagged, now()),
            TieringDecision::Skip(TieringSkipReason::Excluded(TieringRule::Tag {
                key: "keep".to_string(),
                value: "hot".to_string()
            }))
        );

        // A different value on the same tag is not an exclusion.
        tagged.tags = HashMap::from([("keep".to_string(), "cold".to_string())]);
        assert_eq!(decide(&policy, &tagged, now()), TieringDecision::Demote);
    }

    #[test]
    fn delete_markers_and_older_versions_never_migrate() {
        let mut marker = candidate(now(), 40);
        marker.is_delete_marker = true;
        assert_eq!(
            decide(&policy(), &marker, now()),
            TieringDecision::Skip(TieringSkipReason::DeleteMarker)
        );

        let mut versioned = candidate(now(), 40);
        versioned.object =
            EcObjectVersionKey::new("bucket", "data/report.bin", Some("v-1".to_string()));
        assert_eq!(
            decide(&policy(), &versioned, now()),
            TieringDecision::Skip(TieringSkipReason::NotCurrentVersion)
        );
    }

    #[test]
    fn a_disabled_policy_decides_nothing_else() {
        let globals = TieringGlobals {
            enabled: false,
            ..globals()
        };
        let policy = TieringPolicy::resolve("bucket", &globals, None);
        assert_eq!(
            decide(&policy, &candidate(now(), 40), now()),
            TieringDecision::Skip(TieringSkipReason::Disabled)
        );
    }

    /// An object already in erasure coding has nowhere to go: this is also the
    /// EC-only pool case, where everything is encoded on write.
    #[test]
    fn an_erasure_coded_object_is_not_migrated_again() {
        let mut coded = candidate(now(), 40);
        coded.erasure_coded = true;
        assert_eq!(
            decide(&policy(), &coded, now()),
            TieringDecision::Skip(TieringSkipReason::AlreadyErasureCoded)
        );
    }

    // ------------------------------------------------------------ promotion

    #[test]
    fn an_erasure_coded_object_read_often_enough_comes_back() {
        let globals = TieringGlobals {
            promote_after_reads: 10,
            ..globals()
        };
        let policy = TieringPolicy::resolve("bucket", &globals, None);

        let mut coded = candidate(now(), 0);
        coded.erasure_coded = true;
        coded.window_access_count = 10;
        assert_eq!(decide(&policy, &coded, now()), TieringDecision::Promote);

        // One read short of the threshold is not enough.
        coded.window_access_count = 9;
        assert_eq!(
            decide(&policy, &coded, now()),
            TieringDecision::Skip(TieringSkipReason::AlreadyErasureCoded)
        );
    }

    #[test]
    fn promotion_is_off_while_the_threshold_is_zero() {
        let mut coded = candidate(now(), 0);
        coded.erasure_coded = true;
        coded.window_access_count = 100_000;
        assert_eq!(
            decide(&policy(), &coded, now()),
            TieringDecision::Skip(TieringSkipReason::AlreadyErasureCoded)
        );
    }

    // ----------------------------------------------------------- hysteresis

    /// An object that has already moved waits, and waits longer each time.
    ///
    /// Without this an object sitting between the two thresholds would migrate
    /// back and forth forever, paying a full transcode each way.
    #[test]
    fn a_recently_migrated_object_waits_out_a_growing_pause() {
        let policy = policy();
        let mut candidate = candidate(now(), 40);
        candidate.migrations = 1;
        candidate.last_migration_nanos = Some(now() - 3 * DAY);

        assert!(matches!(
            decide(&policy, &candidate, now()),
            TieringDecision::Skip(TieringSkipReason::RecentlyMigrated { .. })
        ));

        // Eight days after a single migration the base pause has elapsed.
        candidate.last_migration_nanos = Some(now() - 8 * DAY);
        assert_eq!(decide(&policy, &candidate, now()), TieringDecision::Demote);

        // After the second migration the same eight days are no longer enough.
        candidate.migrations = 2;
        assert!(matches!(
            decide(&policy, &candidate, now()),
            TieringDecision::Skip(TieringSkipReason::RecentlyMigrated { .. })
        ));
    }

    /// A more specific rule outranks the pause in the report.
    ///
    /// An object kept in place by an exclusion has nothing to do with how
    /// recently it migrated, and telling the operator otherwise sends them to
    /// the wrong setting.
    #[test]
    fn a_specific_rule_is_reported_ahead_of_the_migration_pause() {
        let mut stored = record();
        stored.exclude_prefixes = vec!["data/".to_string()];
        let policy = TieringPolicy::resolve("bucket", &globals(), Some(&stored));

        let mut candidate = candidate(now(), 40);
        candidate.migrations = 1;
        candidate.last_migration_nanos = Some(now() - DAY);

        assert_eq!(
            decide(&policy, &candidate, now()),
            TieringDecision::Skip(TieringSkipReason::Excluded(TieringRule::Prefix(
                "data/".to_string()
            )))
        );

        // Small objects likewise: the size is the reason, not the pause.
        let mut small = candidate;
        small.size = 4096;
        assert!(matches!(
            decide(&policy, &small, now()),
            TieringDecision::Skip(TieringSkipReason::TooSmall { .. })
        ));
    }

    /// An object on the boundary of both thresholds must not oscillate.
    ///
    /// This is the test EC2-023 asks for first, and it is written to fail
    /// without the pause: the object below is simultaneously idle enough to go
    /// into erasure coding and read often enough to come back, which is exactly
    /// the state a threshold pair cannot resolve on its own. Only the interval
    /// between two migrations stops it travelling.
    #[test]
    fn an_object_on_the_boundary_does_not_travel_back_and_forth() {
        let globals = TieringGlobals {
            promote_after_reads: 10,
            ..globals()
        };
        let policy = TieringPolicy::resolve("bucket", &globals, None);

        // Cold enough to demote, and busy enough to promote.
        let mut object = candidate(now(), 40);
        object.window_access_count = 10;
        assert_eq!(decide(&policy, &object, now()), TieringDecision::Demote);

        // The migration happens: the layout flips and the move is recorded.
        let demoted_at = now();
        object.erasure_coded = true;
        object.migrations = 1;
        object.last_migration_nanos = Some(demoted_at);

        // A day later it is still being read hard, and it still may not move.
        let a_day_later = demoted_at + DAY;
        assert!(matches!(
            decide(&policy, &object, a_day_later),
            TieringDecision::Skip(TieringSkipReason::RecentlyMigrated { .. })
        ));

        // Past the pause it comes back, which is the behaviour being asked for.
        let past_the_pause = demoted_at + 8 * DAY;
        assert_eq!(
            decide(&policy, &object, past_the_pause),
            TieringDecision::Promote
        );

        // And having come back it may not turn around again the next day, even
        // though it is once more idle enough to qualify.
        object.erasure_coded = false;
        object.migrations = 2;
        object.last_migration_nanos = Some(past_the_pause);
        assert!(matches!(
            decide(&policy, &object, past_the_pause + DAY),
            TieringDecision::Skip(TieringSkipReason::RecentlyMigrated { .. })
        ));

        // Without the interval the very same object moves on every evaluation,
        // which is the failure this test exists to catch.
        let unguarded = TieringPolicy {
            min_migration_interval: Duration::ZERO,
            ..policy
        };
        assert_eq!(
            decide(&unguarded, &object, past_the_pause + DAY),
            TieringDecision::Demote
        );
    }

    /// The two thresholds cannot be compared, so the pause is what separates
    /// them.
    ///
    /// Going into erasure coding is decided by how long an object has been
    /// idle; coming back is decided by how often it was read. There is no
    /// arithmetic that puts one strictly above the other, and pretending there
    /// is would be the kind of separation that looks correct and holds nothing.
    #[test]
    fn the_two_directions_measure_different_things() {
        let globals = TieringGlobals {
            promote_after_reads: 10,
            ..globals()
        };
        let policy = TieringPolicy::resolve("bucket", &globals, None);

        // Idle for forty days and unread: goes into erasure coding.
        let cold = candidate(now(), 40);
        assert_eq!(decide(&policy, &cold, now()), TieringDecision::Demote);

        // Erasure coded and read ten times in the window: comes back. Its idle
        // time is not consulted at all, and does not have to be: an object
        // being read is not idle.
        let mut busy = candidate(now(), 40);
        busy.erasure_coded = true;
        busy.window_access_count = 10;
        assert_eq!(decide(&policy, &busy, now()), TieringDecision::Promote);
    }

    #[test]
    fn the_migration_pause_doubles_and_then_stops_growing() {
        let policy = policy();
        let base = policy.min_migration_interval;

        assert_eq!(policy.migration_pause(0), Duration::ZERO);
        assert_eq!(policy.migration_pause(1), base);
        assert_eq!(policy.migration_pause(2), base * 2);
        assert_eq!(policy.migration_pause(3), base * 4);
        // A ceiling keeps an object that moved many times reachable at all.
        assert_eq!(
            policy.migration_pause(99),
            base * MAX_MIGRATION_INTERVAL_MULTIPLIER
        );
    }

    // --------------------------------------------------------------- jitter

    #[test]
    fn the_cold_threshold_is_spread_deterministically_per_key() {
        let globals = TieringGlobals {
            jitter_percent: 10,
            ..globals()
        };
        let policy = TieringPolicy::resolve("bucket", &globals, None);
        let base = policy.cold_after;
        let spread = Duration::from_secs(base.as_secs() / 100 * 10);

        let first = policy.effective_cold_after("data/a.bin");
        assert_eq!(
            first,
            policy.effective_cold_after("data/a.bin"),
            "must be stable"
        );
        assert!(first >= base && first < base + spread);

        // Different keys must not all land on the same threshold, or a batch
        // written together would still cool down together.
        let distinct = ["a", "b", "c", "d", "e", "f", "g", "h"]
            .iter()
            .map(|key| policy.effective_cold_after(key))
            .collect::<std::collections::HashSet<_>>();
        assert!(
            distinct.len() > 1,
            "jitter produced one value for every key"
        );
    }

    #[test]
    fn zero_jitter_leaves_the_threshold_exact() {
        assert_eq!(policy().effective_cold_after("any"), policy().cold_after);
    }
}
