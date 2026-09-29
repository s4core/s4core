// Copyright 2026 S4Core Team
//
// Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

//! Erasure Coding v1 pool foundation.
//!
//! This module intentionally stops at Phase 1 boundaries: profile validation,
//! fixed-width erasure-set activation, deterministic shard-slot assignment,
//! and topology risk classification. Codec math, manifests, shard writes, and
//! read repair live in later phases.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};

use s4_cluster::{NodeId, PoolConfig, PoolId};
use s4_core::types::StorageClass;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::ec_lrc_placement::{self, LrcGroupPlanner, LrcPlacementError, UnlabeledNode};
use crate::ec_metadata::{EC_LRC_GROUP_LAYOUT_ALGORITHM_VERSION, EC_V1_LAYOUT_ALGORITHM_VERSION};

/// Default minimum object size for EC transcode eligibility: 1 MiB.
pub const DEFAULT_MIN_OBJECT_SIZE: u64 = 1_048_576;
/// Default EC chunk size: 6 MiB.
pub const DEFAULT_CHUNK_SIZE: u64 = 6_291_456;

/// EC configuration and validation errors.
#[derive(Debug, Error)]
pub enum EcConfigError {
    /// Invalid environment variable.
    #[error("invalid {name}: {reason}")]
    Env {
        /// Environment variable name.
        name: &'static str,
        /// Human-readable validation failure.
        reason: String,
    },

    /// Invalid profile definition.
    #[error("invalid EC profile '{profile}': {reason}")]
    Profile {
        /// Profile name.
        profile: String,
        /// Human-readable validation failure.
        reason: String,
    },

    /// Pool shape cannot host this EC profile.
    #[error("invalid EC pool '{pool}': {reason}")]
    Pool {
        /// Pool name.
        pool: String,
        /// Human-readable validation failure.
        reason: String,
    },

    /// Topology labels are invalid for the active policy.
    #[error("invalid EC topology: {0}")]
    Topology(String),

    /// A new set cannot be laid out by its LRC group rule under `strict`
    /// (EC2-083).
    ///
    /// A variant of its own rather than [`Self::Topology`], so the reason
    /// reaches a metric as a code instead of being guessed from the text.
    #[error("invalid EC topology: {}", .0.detail)]
    LayoutRule(EcLayoutViolation),
}

/// Erasure coding family.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EcCodecKind {
    /// MDS Reed-Solomon profile.
    ReedSolomon,
    /// Locally repairable code profile.
    LocallyRepairable,
}

/// Parity layout for an EC profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EcParity {
    /// Reed-Solomon parity shards.
    ReedSolomon {
        /// Number of parity shards.
        m: u8,
    },
    /// LRC local and global parity shards.
    Lrc {
        /// Number of local parity groups.
        local: u8,
        /// Number of global parity shards.
        global: u8,
    },
}

impl EcParity {
    fn total_parity(self) -> u16 {
        match self {
            Self::ReedSolomon { m } => u16::from(m),
            Self::Lrc { local, global } => u16::from(local) + u16::from(global),
        }
    }
}

/// Validated EC profile.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EcProfile {
    /// Stable profile name.
    pub name: String,
    /// Codec family.
    pub codec: EcCodecKind,
    /// Number of data shards.
    pub k: u8,
    /// Parity layout.
    pub parity: EcParity,
    /// Objects below this size remain explicitly replicated in the EC pool.
    pub min_object_size: u64,
    /// Logical chunk size before shard encoding.
    pub chunk_size: u64,
}

impl EcProfile {
    /// `EC-RS-Small`: RS(4,2), 6 total shards.
    pub fn rs_small() -> Self {
        Self::new_rs("ec-rs-small", 4, 2)
    }

    /// `EC-RS-Standard`: RS(6,3), 9 total shards.
    pub fn rs_standard() -> Self {
        Self::new_rs("ec-rs-standard", 6, 3)
    }

    /// `EC-RS-Dense`: RS(8,3), 11 total shards.
    pub fn rs_dense() -> Self {
        Self::new_rs("ec-rs-dense", 8, 3)
    }

    /// `EC-LRC-Small`: LRC(4,2,1), 7 total shards.
    pub fn lrc_small() -> Self {
        Self::new_lrc("ec-lrc-small", 4, 2, 1)
    }

    /// `EC-LRC-Standard`: LRC(6,2,2), 10 total shards.
    pub fn lrc_standard() -> Self {
        Self::new_lrc("ec-lrc-standard", 6, 2, 2)
    }

    /// `EC-LRC-Dense`: LRC(10,2,2), 14 total shards.
    pub fn lrc_dense() -> Self {
        Self::new_lrc("ec-lrc-dense", 10, 2, 2)
    }

    fn new_rs(name: &str, k: u8, m: u8) -> Self {
        Self {
            name: name.to_string(),
            codec: EcCodecKind::ReedSolomon,
            k,
            parity: EcParity::ReedSolomon { m },
            min_object_size: DEFAULT_MIN_OBJECT_SIZE,
            chunk_size: DEFAULT_CHUNK_SIZE,
        }
    }

    fn new_lrc(name: &str, k: u8, local: u8, global: u8) -> Self {
        Self {
            name: name.to_string(),
            codec: EcCodecKind::LocallyRepairable,
            k,
            parity: EcParity::Lrc { local, global },
            min_object_size: DEFAULT_MIN_OBJECT_SIZE,
            chunk_size: DEFAULT_CHUNK_SIZE,
        }
    }

    /// Parse `S4_EC_*` profile settings from the environment.
    pub fn from_env() -> Result<Self, EcConfigError> {
        let profile_name =
            std::env::var("S4_EC_PROFILE").unwrap_or_else(|_| "ec-rs-standard".to_string());
        let mut profile = match profile_name.as_str() {
            "ec-rs-small" => Self::rs_small(),
            "ec-rs-standard" => Self::rs_standard(),
            "ec-rs-dense" => Self::rs_dense(),
            "ec-lrc-small" => Self::lrc_small(),
            "ec-lrc-standard" => Self::lrc_standard(),
            "ec-lrc-dense" => Self::lrc_dense(),
            "custom" => Self::custom_from_env()?,
            other => {
                return Err(EcConfigError::Env {
                    name: "S4_EC_PROFILE",
                    reason: format!(
                        "unknown profile '{other}'; expected ec-rs-small, ec-rs-standard, \
                         ec-rs-dense, ec-lrc-small, ec-lrc-standard, ec-lrc-dense, or custom"
                    ),
                });
            }
        };

        if let Some(min_object_size) = parse_optional_u64("S4_EC_MIN_OBJECT_SIZE")? {
            profile.min_object_size = min_object_size;
        }
        if let Some(chunk_size) = parse_optional_u64("S4_EC_CHUNK_SIZE")? {
            profile.chunk_size = chunk_size;
        }

        profile.validate()?;
        Ok(profile)
    }

    fn custom_from_env() -> Result<Self, EcConfigError> {
        let codec = std::env::var("S4_EC_CODEC").unwrap_or_else(|_| "rs".to_string());
        let k = parse_required_u8("S4_EC_K")?;
        match codec.as_str() {
            "rs" | "reed_solomon" | "reed-solomon" => {
                let m = parse_required_u8("S4_EC_RS_M")?;
                Ok(Self {
                    name: "custom".to_string(),
                    codec: EcCodecKind::ReedSolomon,
                    k,
                    parity: EcParity::ReedSolomon { m },
                    min_object_size: DEFAULT_MIN_OBJECT_SIZE,
                    chunk_size: DEFAULT_CHUNK_SIZE,
                })
            }
            "lrc" | "locally_repairable" | "locally-repairable" => {
                let local = parse_required_u8("S4_EC_LRC_L")?;
                let global = parse_required_u8("S4_EC_LRC_G")?;
                Ok(Self {
                    name: "custom".to_string(),
                    codec: EcCodecKind::LocallyRepairable,
                    k,
                    parity: EcParity::Lrc { local, global },
                    min_object_size: DEFAULT_MIN_OBJECT_SIZE,
                    chunk_size: DEFAULT_CHUNK_SIZE,
                })
            }
            other => Err(EcConfigError::Env {
                name: "S4_EC_CODEC",
                reason: format!("unknown codec '{other}'; expected rs or lrc"),
            }),
        }
    }

    /// Validate the profile in isolation.
    pub fn validate(&self) -> Result<(), EcConfigError> {
        if self.name.trim().is_empty() {
            return Err(invalid_profile(&self.name, "name must not be empty"));
        }
        if self.k == 0 {
            return Err(invalid_profile(&self.name, "k must be at least 1"));
        }
        if self.min_object_size == 0 {
            return Err(invalid_profile(
                &self.name,
                "min_object_size must be greater than 0",
            ));
        }
        if self.chunk_size == 0 {
            return Err(invalid_profile(
                &self.name,
                "chunk_size must be greater than 0",
            ));
        }

        match (self.codec, self.parity) {
            (EcCodecKind::ReedSolomon, EcParity::ReedSolomon { m }) => {
                if m == 0 {
                    return Err(invalid_profile(
                        &self.name,
                        "Reed-Solomon parity m must be at least 1",
                    ));
                }
            }
            (EcCodecKind::LocallyRepairable, EcParity::Lrc { local, global }) => {
                if local == 0 {
                    return Err(invalid_profile(
                        &self.name,
                        "LRC local parity count must be at least 1",
                    ));
                }
                if global == 0 {
                    return Err(invalid_profile(
                        &self.name,
                        "LRC global parity count must be at least 1",
                    ));
                }
                if local > self.k {
                    return Err(invalid_profile(
                        &self.name,
                        "LRC local parity count must not exceed k",
                    ));
                }
                if !self.k.is_multiple_of(local) {
                    return Err(invalid_profile(
                        &self.name,
                        "LRC v1 requires k to be divisible by local parity count",
                    ));
                }
            }
            (EcCodecKind::ReedSolomon, EcParity::Lrc { .. }) => {
                return Err(invalid_profile(
                    &self.name,
                    "Reed-Solomon profiles must use ReedSolomon parity",
                ));
            }
            (EcCodecKind::LocallyRepairable, EcParity::ReedSolomon { .. }) => {
                return Err(invalid_profile(
                    &self.name,
                    "LRC profiles must use Lrc parity",
                ));
            }
        }

        let total_shards = u16::from(self.k) + self.parity.total_parity();
        if total_shards > u16::from(u8::MAX) {
            return Err(invalid_profile(
                &self.name,
                "total shard count must fit in u8, got {total_shards}",
            ));
        }

        Ok(())
    }

    /// Total shard slots in this profile.
    pub fn total_shards(&self) -> u8 {
        let total = u16::from(self.k) + self.parity.total_parity();
        u8::try_from(total).unwrap_or(u8::MAX)
    }

    /// Inline EC write requires every shard to be durable in v1.
    pub fn required_shard_acks_inline(&self) -> u8 {
        self.total_shards()
    }

    /// Quorum for manifest commit/tombstones.
    pub fn manifest_quorum(&self) -> u8 {
        (self.total_shards() / 2) + 1
    }

    /// Normal read minimum.
    pub fn read_minimum_normal(&self) -> u8 {
        self.k
    }

    /// Snapshot used by the runtime-independent EE codec crate.
    pub fn codec_snapshot(&self) -> s4_codec::EcProfileSnapshot {
        s4_codec::EcProfileSnapshot::from(self)
    }

    fn parity_count_for_topology(&self) -> u16 {
        self.parity.total_parity()
    }
}

impl From<&EcProfile> for s4_codec::EcProfileSnapshot {
    fn from(profile: &EcProfile) -> Self {
        let parity = match profile.parity {
            EcParity::ReedSolomon { m } => s4_codec::EcParity::ReedSolomon { m },
            EcParity::Lrc { local, global } => s4_codec::EcParity::Lrc { local, global },
        };
        let codec = match profile.codec {
            EcCodecKind::ReedSolomon => s4_codec::EcCodecKind::ReedSolomon,
            EcCodecKind::LocallyRepairable => s4_codec::EcCodecKind::LocallyRepairable,
        };
        Self {
            name: profile.name.clone(),
            codec,
            k: profile.k,
            parity,
            min_object_size: profile.min_object_size,
            chunk_size: profile.chunk_size,
        }
    }
}

/// EC topology policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EcTopologyPolicy {
    /// Partial labels fail startup.
    Strict,
    /// Partial and imbalanced labels are reported as degraded.
    Warn,
}

impl EcTopologyPolicy {
    /// Parse `S4_EC_TOPOLOGY_POLICY`.
    pub fn from_env() -> Result<Self, EcConfigError> {
        let value = std::env::var("S4_EC_TOPOLOGY_POLICY").unwrap_or_else(|_| "strict".into());
        match value.as_str() {
            "strict" => Ok(Self::Strict),
            "warn" => Ok(Self::Warn),
            other => Err(EcConfigError::Env {
                name: "S4_EC_TOPOLOGY_POLICY",
                reason: format!("unknown policy '{other}'; expected strict or warn"),
            }),
        }
    }
}

/// Environment variable that selects the failure-domain level the members of
/// an LRC local group are spread over.
pub const LRC_GROUP_DOMAIN_ENV: &str = "S4_EC_LRC_GROUP_DOMAIN";

/// How this node lays out new erasure sets and judges their topology.
///
/// Two independent settings travel together here (EC2-081, point 4).
/// `S4_EC_TOPOLOGY_POLICY` answers "what to do with a violation" and keeps its
/// EC v1 meaning. `S4_EC_LRC_GROUP_DOMAIN` answers "what counts as one". The
/// level is kept next to the policy rather than inside its variants, so every
/// place that compares the policy with `Strict` stays as it is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct EcTopologyRule {
    /// What to do with a violation.
    pub policy: EcTopologyPolicy,
    /// Level the members of an LRC local group must be spread over, or `None`
    /// when the mode is off and sets are laid out exactly as in EC v1.
    pub lrc_group_domain: Option<EcFailureDomainLevel>,
}

impl EcTopologyRule {
    /// The EC v1 rule: `policy` with the LRC group mode off.
    pub fn v1(policy: EcTopologyPolicy) -> Self {
        Self {
            policy,
            lrc_group_domain: None,
        }
    }

    /// Parse `S4_EC_TOPOLOGY_POLICY` and `S4_EC_LRC_GROUP_DOMAIN`.
    ///
    /// A deployment that sets neither gets the EC v1 rule. An unknown level is
    /// refused with the variable's name, even for an RS profile where the level
    /// has no effect: a typo must fail at startup, not on the day the profile
    /// changes.
    pub fn from_env() -> Result<Self, EcConfigError> {
        let policy = EcTopologyPolicy::from_env()?;
        let lrc_group_domain = match std::env::var(LRC_GROUP_DOMAIN_ENV) {
            Ok(raw) => parse_lrc_group_domain(&raw)?,
            Err(std::env::VarError::NotPresent) => None,
            Err(error) => {
                return Err(EcConfigError::Env {
                    name: LRC_GROUP_DOMAIN_ENV,
                    reason: error.to_string(),
                });
            }
        };
        Ok(Self {
            policy,
            lrc_group_domain,
        })
    }

    /// The rule a new set of `profile` is laid out by.
    ///
    /// Only LRC profiles have local groups to spread. An RS profile is laid out
    /// by the EC v1 rule whatever the level says: all its slots are equal, so
    /// which node holds which slot does not change what the set survives
    /// (EC2-080, "RS profiles are not affected").
    pub fn layout_rule_for(&self, profile: &EcProfile) -> EcSetLayoutRule {
        match (profile.codec, self.lrc_group_domain) {
            (EcCodecKind::LocallyRepairable, Some(level)) => EcSetLayoutRule::LrcGroups { level },
            _ => EcSetLayoutRule::V1,
        }
    }
}

/// Parse one `S4_EC_LRC_GROUP_DOMAIN` value: `off`, `host`, `rack` or `zone`.
fn parse_lrc_group_domain(raw: &str) -> Result<Option<EcFailureDomainLevel>, EcConfigError> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "off" => Ok(None),
        "zone" => Ok(Some(EcFailureDomainLevel::Zone)),
        "rack" => Ok(Some(EcFailureDomainLevel::Rack)),
        "host" => Ok(Some(EcFailureDomainLevel::Host)),
        other => Err(EcConfigError::Env {
            name: LRC_GROUP_DOMAIN_ENV,
            reason: format!("unknown level '{other}'; expected off, host, rack or zone"),
        }),
    }
}

/// The rule an erasure set's slot order was chosen by.
///
/// Remembered with the set in its layout record (EC2-084), so the set is
/// always judged by its own rule: a set laid out by EC v1 never becomes a
/// violator just because a node switched the LRC group mode on.
///
/// Part of the layout record's bincode encoding, which writes the variant
/// index: new variants may only be appended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EcSetLayoutRule {
    /// The EC v1 order produced by `ShardSlotAssignment::build`. Judged by the
    /// EC v1 topology checks alone.
    V1,
    /// Every member of an LRC local group — its data shards and its local
    /// parity — sits in a different failure domain of `level` (EC2-080).
    LrcGroups {
        /// Level the group members are spread over.
        level: EcFailureDomainLevel,
    },
}

/// Why a set was created without fully meeting its rule.
///
/// Only possible under `S4_EC_TOPOLOGY_POLICY=warn`; under `strict` such a set
/// is not created at all (EC2-083). The code is machine-readable so a metric
/// can count it without parsing text.
///
/// Part of the layout record's bincode encoding: new variants may only be
/// appended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EcLayoutViolationReason {
    /// Fewer failure domains than an LRC local group has members.
    TooFewDomains,
    /// Enough domains, but some hold more nodes than the local groups and the
    /// global parity slots can absorb.
    DomainOverloaded,
    /// A node of the set lacks the labels the rule needs.
    IncompleteLabels,
}

/// A rule a set was created in breach of, and the operator-facing explanation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EcLayoutViolation {
    /// Machine-readable reason.
    pub reason: EcLayoutViolationReason,
    /// What is needed and what the set has. `strict` refuses the set with this
    /// text, followed by the advice to switch to `warn`.
    pub detail: String,
}

impl EcLayoutViolationReason {
    /// The value metrics and the admin API use for this reason.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::TooFewDomains => "too_few_domains",
            Self::DomainOverloaded => "domain_overloaded",
            Self::IncompleteLabels => "incomplete_labels",
        }
    }
}

/// Environment variable that allows more than one erasure set in a pool.
pub const MULTI_SET_ENABLED_ENV: &str = "S4_EC_MULTI_SET_ENABLED";

/// Environment variable that selects how a new object picks its erasure set.
pub const PLACEMENT_STRATEGY_ENV: &str = "S4_EC_PLACEMENT_STRATEGY";

/// Strategy used to pick an erasure set for a new object.
///
/// Meaningful only when a pool holds more than one set. With a single set the
/// choice is forced, so every strategy behaves identically — which is what keeps
/// a disabled multi-set pool equal to EC v1.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EcPlacementStrategy {
    /// Cycle through the eligible sets in order.
    ///
    /// The default because it needs no capacity data: a node that has not
    /// reported real disk usage still spreads writes evenly.
    #[default]
    RoundRobin,
    /// Weight the choice by free space, so a set with more room receives
    /// proportionally more objects.
    ///
    /// Falls back to [`Self::RoundRobin`] while capacity data is missing or
    /// stale, so switching it on cannot make writes less available.
    FreeSpace,
}

impl EcPlacementStrategy {
    /// Parse `S4_EC_PLACEMENT_STRATEGY`, defaulting to [`Self::RoundRobin`].
    pub fn from_env() -> Result<Self, EcConfigError> {
        match std::env::var(PLACEMENT_STRATEGY_ENV) {
            Ok(raw) => Self::parse(&raw),
            Err(std::env::VarError::NotPresent) => Ok(Self::default()),
            Err(error) => Err(EcConfigError::Env {
                name: PLACEMENT_STRATEGY_ENV,
                reason: error.to_string(),
            }),
        }
    }

    /// Parse one strategy name, accepting `-` and `_` as separators.
    fn parse(raw: &str) -> Result<Self, EcConfigError> {
        let normalized = raw.trim().to_ascii_lowercase().replace('-', "_");
        match normalized.as_str() {
            "round_robin" => Ok(Self::RoundRobin),
            "free_space" => Ok(Self::FreeSpace),
            other => Err(EcConfigError::Env {
                name: PLACEMENT_STRATEGY_ENV,
                reason: format!(
                    "unknown placement strategy '{other}'; expected round_robin or free_space"
                ),
            }),
        }
    }
}

/// Multi-set pool configuration.
///
/// Decides whether a pool may hold more than one erasure set, and how a new
/// object picks between them. Disabled by default: a pool then keeps exactly
/// one inherited set and behaves as it did in EC v1.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EcMultiSetConfig {
    /// Whether a pool may hold more than one erasure set.
    pub enabled: bool,
    /// Strategy used to pick a set for a new object.
    pub placement: EcPlacementStrategy,
}

impl EcMultiSetConfig {
    /// Parse `S4_EC_MULTI_SET_ENABLED` and `S4_EC_PLACEMENT_STRATEGY`.
    ///
    /// Both variables are optional and both default to the EC v1 behaviour, so
    /// a deployment that sets neither is unaffected by multi-set pools.
    pub fn from_env() -> Result<Self, EcConfigError> {
        Ok(Self {
            enabled: parse_optional_bool_env(MULTI_SET_ENABLED_ENV)?.unwrap_or(false),
            placement: EcPlacementStrategy::from_env()?,
        })
    }

    /// Whether more than one set may be created in a pool.
    ///
    /// Reading and repairing objects never consults this: sets that already
    /// exist keep serving whatever their state allows, so switching the feature
    /// off stops new sets without making existing objects unreadable.
    pub fn allows_multiple_sets(&self) -> bool {
        self.enabled
    }
}

/// Serializes access to the `S4_EC_*` placement variables in tests.
///
/// The environment is process-wide and tests run as threads of one process, so
/// a test that sets an invalid value would otherwise break whichever test
/// parsed the environment at that moment. Shared beyond this module because the
/// placement router reads these variables too.
#[cfg(test)]
pub(crate) static PLACEMENT_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Parse an optional boolean, accepting the spellings the other `S4_EC_*`
/// switches accept.
///
/// Shared with `ec_packed_segments`, so that every `S4_EC_*` switch in the
/// crate accepts exactly the same spellings.
pub(crate) fn parse_optional_bool_env(name: &'static str) -> Result<Option<bool>, EcConfigError> {
    match std::env::var(name) {
        Ok(raw) => match raw.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Ok(Some(true)),
            "0" | "false" | "no" | "off" => Ok(Some(false)),
            other => Err(EcConfigError::Env {
                name,
                reason: format!("unknown boolean value '{other}'; expected true or false"),
            }),
        },
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(EcConfigError::Env {
            name,
            reason: error.to_string(),
        }),
    }
}

/// Failure-domain labels for one node.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopologyLabels {
    /// Zone label.
    pub zone: Option<String>,
    /// Rack label.
    pub rack: Option<String>,
    /// Host label.
    pub host: Option<String>,
    /// Disk group label.
    pub disk_group: Option<String>,
}

impl TopologyLabels {
    /// True when no topology label is present.
    pub fn is_empty(&self) -> bool {
        self.zone.is_none()
            && self.rack.is_none()
            && self.host.is_none()
            && self.disk_group.is_none()
    }

    /// True when every v1 topology label is present.
    pub fn is_complete(&self) -> bool {
        self.zone.is_some()
            && self.rack.is_some()
            && self.host.is_some()
            && self.disk_group.is_some()
    }
}

/// A level of the failure-domain hierarchy.
///
/// Selects how many of a node's [`TopologyLabels`] identify its domain; it is
/// not a second topology model (EC2-081). `disk_group` is never a failure
/// domain here: the disks of one host fail together.
///
/// Part of the layout record's bincode encoding: new variants may only be
/// appended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EcFailureDomainLevel {
    /// A zone, identified by `zone`.
    Zone,
    /// A rack, identified by `zone` and `rack`.
    Rack,
    /// A host, identified by `zone`, `rack` and `host`.
    Host,
}

impl EcFailureDomainLevel {
    /// The value `S4_EC_LRC_GROUP_DOMAIN` uses for this level.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Zone => "zone",
            Self::Rack => "rack",
            Self::Host => "host",
        }
    }

    /// The domain `labels` put a node in at this level.
    ///
    /// `None` when a label the level needs is missing. Labels below the level
    /// are ignored: two nodes on different hosts of one rack share their rack.
    pub fn domain_of(self, labels: &TopologyLabels) -> Option<FailureDomainKey> {
        let mut path = vec![labels.zone.clone()?];
        if self >= Self::Rack {
            path.push(labels.rack.clone()?);
        }
        if self >= Self::Host {
            path.push(labels.host.clone()?);
        }
        Some(FailureDomainKey { level: self, path })
    }
}

impl std::fmt::Display for EcFailureDomainLevel {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Identity of one failure domain.
///
/// Hierarchical: a domain is named by every label above it as well as its own,
/// so rack `r1` of zone `a` and rack `r1` of zone `b` are two racks. That is how
/// they fail, and how `topology_spread_order` already orders nodes
/// (EC2-081, point 1). Displayed as the labels joined by `/`, e.g. `z1/r1`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FailureDomainKey {
    level: EcFailureDomainLevel,
    /// Labels from the zone down to `level`, outermost first.
    path: Vec<String>,
}

impl FailureDomainKey {
    /// Level of this domain.
    pub fn level(&self) -> EcFailureDomainLevel {
        self.level
    }

    /// The labels that name this domain, outermost first.
    pub fn labels(&self) -> &[String] {
        &self.path
    }

    /// The zone this domain lies in.
    pub fn zone(&self) -> &str {
        // Every level starts with the zone, so the path is never empty.
        self.path.first().map(String::as_str).unwrap_or_default()
    }
}

impl std::fmt::Display for FailureDomainKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.path.join("/"))
    }
}

/// Which failure domain each node is in, at one level.
///
/// Built once from the labels a node started with, so that laying out a set
/// and choosing repair sources look a node up in a map instead of re-deriving
/// its domain from labels each time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailureDomainIndex {
    level: EcFailureDomainLevel,
    domains: HashMap<NodeId, FailureDomainKey>,
    /// Nodes whose labels do not name a domain at this level, sorted.
    unplaced: Vec<NodeId>,
}

impl FailureDomainIndex {
    /// Index `nodes` by their domain at `level`.
    ///
    /// A node without the labels the level needs is kept aside rather than
    /// dropped, so a caller can say which nodes lack them.
    pub fn build(
        level: EcFailureDomainLevel,
        nodes: impl IntoIterator<Item = NodeId>,
        labels: &HashMap<NodeId, TopologyLabels>,
    ) -> Self {
        let mut domains = HashMap::new();
        let mut unplaced = Vec::new();
        for node_id in nodes {
            match labels.get(&node_id).and_then(|label| level.domain_of(label)) {
                Some(domain) => {
                    domains.insert(node_id, domain);
                }
                None => unplaced.push(node_id),
            }
        }
        unplaced.sort_unstable_by_key(|node| node.0);
        unplaced.dedup();
        Self {
            level,
            domains,
            unplaced,
        }
    }

    /// Level this index was built for.
    pub fn level(&self) -> EcFailureDomainLevel {
        self.level
    }

    /// The domain of `node_id`, or `None` when the index does not know one.
    pub fn domain_of(&self, node_id: NodeId) -> Option<&FailureDomainKey> {
        self.domains.get(&node_id)
    }

    /// Indexed nodes that have no domain at this level, sorted by `NodeId`.
    pub fn unplaced_nodes(&self) -> &[NodeId] {
        &self.unplaced
    }

    /// The given nodes grouped by domain.
    ///
    /// Domains and the nodes inside each are sorted, so the answer depends on
    /// the set of nodes and not on the order they were passed in. Nodes the
    /// index has no domain for are left out; [`Self::unplaced_nodes`] names
    /// them.
    pub fn group_by_domain(&self, nodes: &[NodeId]) -> BTreeMap<FailureDomainKey, Vec<NodeId>> {
        let mut groups: BTreeMap<FailureDomainKey, Vec<NodeId>> = BTreeMap::new();
        for node_id in nodes {
            if let Some(domain) = self.domains.get(node_id) {
                groups.entry(domain.clone()).or_default().push(*node_id);
            }
        }
        for members in groups.values_mut() {
            members.sort_unstable_by_key(|node| node.0);
            members.dedup();
        }
        groups
    }
}

/// Members of one LRC local group inside one failure domain.
///
/// More than one member of the same group in one domain is exactly what the
/// group rule forbids, so this is where a violation is read off the placement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DomainGroupMembers {
    /// The local group.
    pub local_group: u8,
    /// Members of it this domain holds.
    pub members: u8,
}

/// One failure domain and the slots of a set it holds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DomainOccupancy {
    /// The domain, named by every label above it: `z1/r1` for a rack.
    pub domain: String,
    /// Slots of the set in it, ascending.
    pub slots: Vec<u8>,
    /// Local groups with a member here, ascending. A `members` above one is a
    /// breach of the group rule.
    pub local_groups: Vec<DomainGroupMembers>,
}

/// Where one shard slot sits, named at every failure-domain level.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlotPlacement {
    /// Zero-based slot index.
    pub slot_index: u8,
    /// Node holding the slot.
    pub node_id: String,
    /// Slot role.
    pub role: ShardSlotRole,
    /// LRC local group, when the profile has local groups.
    pub local_group: Option<u8>,
    /// Zone of the node, when its labels name one.
    pub zone: Option<String>,
    /// Rack of the node, when its labels name one.
    pub rack: Option<String>,
    /// Host of the node, when its labels name one.
    pub host: Option<String>,
}

/// Where a whole erasure set sits, by failure domain.
///
/// Built from the labels this node started with, so two nodes with different
/// labels describe the same set differently — which is why the admin surface
/// says whose view it is answering with (EC2-086, point 4).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SetDomainPlacement {
    /// Level the domains below are grouped at: the set's own rule level, or
    /// the rack for a set laid out by the EC v1 rule, which is the level EC v1
    /// already warns about.
    pub level: EcFailureDomainLevel,
    /// Every slot with the domains of its node.
    pub slots: Vec<SlotPlacement>,
    /// The set's slots grouped by domain at [`Self::level`], domains sorted.
    pub domains: Vec<DomainOccupancy>,
    /// Nodes of the set whose labels do not name a domain at that level.
    pub unlabeled_nodes: Vec<String>,
}

/// Where a set's breach of its own layout rule came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LayoutViolationOrigin {
    /// The set was created in breach of its rule, under `warn` (EC2-083). A
    /// violator since birth rather than a drifted one, and its layout record
    /// says so.
    Creation,
    /// The set met its rule when it was laid out and does not now: the labels
    /// changed under it (EC2-086, point 3).
    Drift,
}

impl LayoutViolationOrigin {
    /// The value metrics and the admin API use for this origin.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Creation => "creation",
            Self::Drift => "drift",
        }
    }
}

/// Whether a set's recorded layout still keeps the guarantee of its own rule.
///
/// Judged against the rule the set was laid out by and no other: a set laid out
/// by the EC v1 rule never becomes a violator because a node switched
/// `S4_EC_LRC_GROUP_DOMAIN` on (EC2-084, point 1).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "state")]
pub enum SetLayoutCompliance {
    /// The set's rule makes no placement guarantee, so nothing can breach it.
    /// Every EC v1 set is here.
    NoGuarantee,
    /// The guarantee holds under this node's labels.
    Meets,
    /// The guarantee is breached.
    Violated {
        /// Whether the breach was there at creation or appeared since.
        origin: LayoutViolationOrigin,
        /// Group members sharing a domain with another member of their group.
        coincidences: usize,
        /// What is breached, in the words the risks use.
        detail: String,
    },
    /// The rule cannot be judged from this node's labels — they are incomplete,
    /// or the record names a rule that cannot describe this profile. Reported
    /// rather than counted: an unjudgeable set is not a known violator.
    NotJudgeable {
        /// Why the rule could not be judged.
        detail: String,
    },
}

impl SetLayoutCompliance {
    /// The breach this compliance reports, when it is one.
    pub fn violation_origin(&self) -> Option<LayoutViolationOrigin> {
        match self {
            Self::Violated { origin, .. } => Some(*origin),
            _ => None,
        }
    }
}

/// How far a node is from this one, nearest first.
///
/// Read from topology labels. A node whose labels do not say where it is
/// counts as [`Self::Remote`]: nothing shows it is near.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum NodeDistance {
    /// This node itself; reaching it crosses no network.
    Local,
    /// Another node in the same rack.
    SameRack,
    /// A node in another rack of the same zone.
    SameZone,
    /// A node in another zone, or one whose labels do not place it.
    Remote,
}

/// The pool's topology as one node sees it: which zone and which rack each
/// node of the pool is in.
///
/// Repair and degraded reads use it to prefer near shards and to tell which
/// bytes cross a zone boundary (plan 05, phase 3). It is a view over the
/// labels the node started with, through [`FailureDomainIndex`], not a second
/// topology model.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PoolTopologyView {
    local_node_id: NodeId,
    zones: FailureDomainIndex,
    racks: FailureDomainIndex,
}

impl PoolTopologyView {
    /// The view of `local_node_id` over `nodes`, placed by `labels`.
    pub fn build(
        local_node_id: NodeId,
        nodes: &[NodeId],
        labels: &HashMap<NodeId, TopologyLabels>,
    ) -> Self {
        Self {
            local_node_id,
            zones: FailureDomainIndex::build(
                EcFailureDomainLevel::Zone,
                nodes.iter().copied(),
                labels,
            ),
            racks: FailureDomainIndex::build(
                EcFailureDomainLevel::Rack,
                nodes.iter().copied(),
                labels,
            ),
        }
    }

    /// The view of a node that knows no labels: every other node is remote,
    /// and no zone is known.
    pub fn unlabeled(local_node_id: NodeId) -> Self {
        Self::build(local_node_id, &[], &HashMap::new())
    }

    /// The node this view is taken from.
    pub fn local_node_id(&self) -> NodeId {
        self.local_node_id
    }

    /// The zone of `node_id`, or `None` when its labels do not name one.
    pub fn zone_of(&self, node_id: NodeId) -> Option<&FailureDomainKey> {
        self.zones.domain_of(node_id)
    }

    /// How far `node_id` is from this node.
    pub fn distance_to(&self, node_id: NodeId) -> NodeDistance {
        if node_id == self.local_node_id {
            return NodeDistance::Local;
        }
        let shares = |index: &FailureDomainIndex| {
            matches!(
                (index.domain_of(self.local_node_id), index.domain_of(node_id)),
                (Some(here), Some(there)) if here == there
            )
        };
        if shares(&self.racks) {
            NodeDistance::SameRack
        } else if shares(&self.zones) {
            NodeDistance::SameZone
        } else {
            NodeDistance::Remote
        }
    }

    /// Whether bytes moving between `from` and `to` stay inside one zone.
    ///
    /// `None` when either node has no zone label — then nobody can say.
    pub fn same_zone(&self, from: NodeId, to: NodeId) -> Option<bool> {
        Some(self.zone_of(from)? == self.zone_of(to)?)
    }
}

/// EC pool topology health.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EcTopologyHealth {
    /// All placement checks passed.
    Healthy,
    /// No node has labels; deterministic slot spread is used.
    TopologyUnknown,
    /// Labels exist, but topology diversity is incomplete or imbalanced.
    TopologyDegraded,
}

/// Topology validation result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EcTopologyReport {
    /// Health summary.
    pub health: EcTopologyHealth,
    /// Operator-facing risk messages.
    pub risks: Vec<String>,
}

/// Role of a shard slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShardSlotRole {
    /// Data shard.
    Data,
    /// Reed-Solomon parity shard.
    ReedSolomonParity,
    /// LRC local parity shard.
    LrcLocalParity,
    /// LRC global parity shard.
    LrcGlobalParity,
}

/// One shard-slot assignment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardSlot {
    /// Zero-based slot index.
    pub slot_index: u8,
    /// Node that owns this slot.
    pub node_id: NodeId,
    /// Slot role.
    pub role: ShardSlotRole,
    /// LRC local group index when applicable.
    pub local_group: Option<u8>,
}

/// Deterministic shard-slot assignment for a fixed-width erasure set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardSlotAssignment {
    slots: Vec<ShardSlot>,
}

impl ShardSlotAssignment {
    /// Build slot assignment for the given profile and nodes.
    pub fn build(
        profile: &EcProfile,
        nodes: &[NodeId],
        labels: &HashMap<NodeId, TopologyLabels>,
    ) -> Result<Self, EcConfigError> {
        profile.validate()?;
        if nodes.len() != usize::from(profile.total_shards()) {
            return Err(EcConfigError::Pool {
                pool: "erasure-set".to_string(),
                reason: format!(
                    "fixed-width EC v1 requires exactly {} node(s), got {}",
                    profile.total_shards(),
                    nodes.len()
                ),
            });
        }
        ensure_distinct_nodes("erasure-set", nodes)?;

        let ordered_nodes = topology_spread_order(nodes, labels);
        let mut slots = Vec::with_capacity(ordered_nodes.len());

        match profile.parity {
            EcParity::ReedSolomon { m } => {
                for (idx, node_id) in ordered_nodes.into_iter().enumerate() {
                    let slot_index = idx as u8;
                    let role = if slot_index < profile.k {
                        ShardSlotRole::Data
                    } else {
                        debug_assert!(slot_index < profile.k + m);
                        ShardSlotRole::ReedSolomonParity
                    };
                    slots.push(ShardSlot {
                        slot_index,
                        node_id,
                        role,
                        local_group: None,
                    });
                }
            }
            EcParity::Lrc { local, global } => {
                let data_per_group = profile.k / local;
                for (idx, node_id) in ordered_nodes.into_iter().enumerate() {
                    let slot_index = idx as u8;
                    let (role, local_group) = if slot_index < profile.k {
                        (ShardSlotRole::Data, Some(slot_index / data_per_group))
                    } else if slot_index < profile.k + local {
                        (ShardSlotRole::LrcLocalParity, Some(slot_index - profile.k))
                    } else {
                        debug_assert!(slot_index < profile.k + local + global);
                        (ShardSlotRole::LrcGlobalParity, None)
                    };
                    slots.push(ShardSlot {
                        slot_index,
                        node_id,
                        role,
                        local_group,
                    });
                }
            }
        }

        Ok(Self { slots })
    }

    /// Rebuild an assignment from a slot order chosen earlier.
    ///
    /// `nodes[i]` takes slot `i`. Labels play no part: the order was decided
    /// when the set was created and written into its layout record, and taking
    /// it verbatim is what stops a label change from moving slots (EC2-084,
    /// point 3). Only the structure is checked — the width of the profile and
    /// distinct nodes. A slot's role and local group follow from its index
    /// exactly as they do in [`Self::build`].
    pub fn from_slot_order(profile: &EcProfile, nodes: &[NodeId]) -> Result<Self, EcConfigError> {
        profile.validate()?;
        if nodes.len() != usize::from(profile.total_shards()) {
            return Err(EcConfigError::Pool {
                pool: "erasure-set".to_string(),
                reason: format!(
                    "a recorded slot order for profile '{}' must name exactly {} node(s), got {}",
                    profile.name,
                    profile.total_shards(),
                    nodes.len()
                ),
            });
        }
        ensure_distinct_nodes("erasure-set", nodes)?;

        let slots = nodes
            .iter()
            .zip(0..=u8::MAX)
            .map(|(node_id, slot_index)| {
                let (role, local_group) = slot_role(profile, slot_index);
                ShardSlot {
                    slot_index,
                    node_id: *node_id,
                    role,
                    local_group,
                }
            })
            .collect();
        Ok(Self { slots })
    }

    /// Nodes in slot order: the node of slot `i` at index `i`.
    ///
    /// This is what a layout record stores, and what
    /// [`Self::from_slot_order`] takes back.
    pub fn slot_order(&self) -> Vec<NodeId> {
        self.slots.iter().map(|slot| slot.node_id).collect()
    }

    /// All shard slots.
    pub fn slots(&self) -> &[ShardSlot] {
        &self.slots
    }

    /// Nodes holding at least one slot, each listed once, in a stable order.
    ///
    /// A profile may put more than one slot on a node, so the raw slot list is
    /// the wrong thing to iterate when the question is about nodes rather than
    /// shards.
    pub fn distinct_nodes(&self) -> Vec<NodeId> {
        let mut nodes = self.slots.iter().map(|slot| slot.node_id).collect::<Vec<_>>();
        nodes.sort_unstable_by_key(|node| node.0);
        nodes.dedup();
        nodes
    }

    /// Number of assigned slots.
    pub fn len(&self) -> usize {
        self.slots.len()
    }

    /// True if no slots are assigned.
    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }
}

/// Erasure set lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErasureSetState {
    /// Serving writes and reads.
    Active,
    /// New writes are moving elsewhere; reads still allowed.
    Draining,
    /// Reads allowed, no new writes.
    ReadOnly,
    /// No longer serving traffic.
    Retired,
}

/// Fixed-width erasure set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErasureSet {
    /// Erasure-set identifier, unique within the owning pool.
    ///
    /// Issued explicitly since EC v2. EC v1 derived it from the pool, which held
    /// while a pool had one set and collides the moment it has two — see
    /// decision EC2-041.
    pub set_id: u64,
    /// Owning pool.
    pub pool_id: PoolId,
    /// Whether this set carries the pool's EC v1 identity.
    ///
    /// At most one set per pool may hold it. That set is where manifests written
    /// before the decoupling resolve, because they name the pool rather than a
    /// set.
    #[serde(default)]
    pub inherited: bool,
    /// EC profile.
    pub profile: EcProfile,
    /// Shard slot assignment.
    pub slot_assignment: ShardSlotAssignment,
    /// Rule the slot assignment was laid out by, and the one the set is judged
    /// against. [`EcSetLayoutRule::V1`] for every set built from configuration
    /// or a roster; a layout record can say otherwise.
    pub layout_rule: EcSetLayoutRule,
    /// Topology epoch used to build this set.
    pub topology_epoch: u64,
    /// Epoch/time when this set was created.
    pub created_epoch: u64,
    /// Lifecycle state.
    pub state: ErasureSetState,
    /// Topology health visible to admin surfaces.
    pub topology_health: EcTopologyHealth,
    /// Operator-facing topology risks.
    pub topology_risks: Vec<String>,
    /// Whether this node has yet to read the set's layout record.
    ///
    /// True only for a set whose record this node has never known and could
    /// not learn: a node started without a local copy at a moment the cluster
    /// could not answer, or a node added to a grown pool before the record
    /// reached the new owners of its key. Such a set may be laid out by a rule
    /// this node cannot see yet, and even an EC v1 set may be recorded with
    /// another order than this node's labels give. It serves reads, which go
    /// by the manifest, but takes no new objects until the record is read
    /// (plan 05, phase 1 gate, point 7).
    #[serde(default)]
    pub layout_pending: bool,
}

impl ErasureSet {
    /// Build an active erasure set with an explicitly issued identifier.
    ///
    /// `set_id` must be unique within `pool_id`; the registry enforces that when
    /// sets are collected into a pool.
    #[allow(clippy::too_many_arguments)]
    pub fn active(
        pool_id: PoolId,
        set_id: u64,
        profile: EcProfile,
        nodes: &[NodeId],
        labels: &HashMap<NodeId, TopologyLabels>,
        policy: EcTopologyPolicy,
        topology_epoch: u64,
        created_epoch: u64,
    ) -> Result<Self, EcConfigError> {
        Self::build(
            pool_id,
            set_id,
            false,
            profile,
            nodes,
            labels,
            policy,
            topology_epoch,
            created_epoch,
        )
    }

    /// Build the pool's inherited set — the one EC v1 would have created.
    ///
    /// It takes [`legacy_pool_set_id`] of `pool_id`: the exact value EC v1 wrote
    /// into every manifest's `erasure_set_id`. Keeping that value means the
    /// manifests this set produces stay byte-identical, in `erasure_set_id`, to
    /// what EC v1 wrote — so a rollback to an EC v1 binary and a mixed-version
    /// cluster (conventions section 5) can still read, repair and scrub the
    /// objects. The decoupling is still real: `set_id` is an explicit parameter,
    /// and the registry issues independent ids ([`FIRST_SET_ID`] and up) to every
    /// *additional* set.
    #[allow(clippy::too_many_arguments)]
    pub fn inherited(
        pool_id: PoolId,
        profile: EcProfile,
        nodes: &[NodeId],
        labels: &HashMap<NodeId, TopologyLabels>,
        policy: EcTopologyPolicy,
        topology_epoch: u64,
        created_epoch: u64,
    ) -> Result<Self, EcConfigError> {
        Self::build(
            pool_id,
            legacy_pool_set_id(pool_id),
            true,
            profile,
            nodes,
            labels,
            policy,
            topology_epoch,
            created_epoch,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn build(
        pool_id: PoolId,
        set_id: u64,
        inherited: bool,
        profile: EcProfile,
        nodes: &[NodeId],
        labels: &HashMap<NodeId, TopologyLabels>,
        policy: EcTopologyPolicy,
        topology_epoch: u64,
        created_epoch: u64,
    ) -> Result<Self, EcConfigError> {
        profile.validate()?;
        if nodes.len() != usize::from(profile.total_shards()) {
            return Err(EcConfigError::Pool {
                pool: pool_id.to_string(),
                reason: format!(
                    "EC fixed-width pool requires exactly {} node(s) for profile '{}', got {}",
                    profile.total_shards(),
                    profile.name,
                    nodes.len()
                ),
            });
        }
        ensure_distinct_nodes(&pool_id.to_string(), nodes)?;

        let slot_assignment = ShardSlotAssignment::build(&profile, nodes, labels)?;
        let topology = validate_topology(&profile, &slot_assignment, labels, policy)?;

        Ok(Self::from_layout(
            SetIdentity {
                pool_id,
                set_id,
                inherited,
                topology_epoch,
                created_epoch,
            },
            profile,
            slot_assignment,
            EcSetLayoutRule::V1,
            topology,
        ))
    }

    /// An active set from a layout that is already decided and judged.
    fn from_layout(
        identity: SetIdentity,
        profile: EcProfile,
        slot_assignment: ShardSlotAssignment,
        layout_rule: EcSetLayoutRule,
        topology: EcTopologyReport,
    ) -> Self {
        Self {
            set_id: identity.set_id,
            pool_id: identity.pool_id,
            inherited: identity.inherited,
            profile,
            slot_assignment,
            layout_rule,
            topology_epoch: identity.topology_epoch,
            created_epoch: identity.created_epoch,
            state: ErasureSetState::Active,
            topology_health: topology.health,
            topology_risks: topology.risks,
            layout_pending: false,
        }
    }

    /// Whether `node_id` holds a slot of this set.
    ///
    /// This, and not "holds this very slot in the current layout", is what a
    /// manifest's shard placement is checked against. The manifest says where
    /// its own shards are; the set's layout only decides where new objects go
    /// (EC2-086, point 1).
    pub fn holds_node(&self, node_id: NodeId) -> bool {
        self.slot_assignment.slots().iter().any(|slot| slot.node_id == node_id)
    }

    /// Whether a manifest naming `manifest_set_id` belongs to this set.
    ///
    /// A set always answers to its own id. The inherited set of a pool also
    /// answers to the value EC v1 derived from the pool id, because manifests
    /// recorded before the decoupling carry it and must keep resolving here.
    ///
    /// This is a per-set predicate, not the resolver. With the current
    /// [`ErasureSet::inherited`] the inherited set's own `set_id` already equals
    /// `legacy_pool_set_id(pool_id)`, so the first term covers EC v1 manifests;
    /// the second term stays as a fallback for a pool whose inherited set was
    /// ever renumbered. [`ErasureSetRegistry::resolve_manifest_set`] gives
    /// explicit ids priority, the order EC2-041 requires.
    pub fn accepts_manifest_set_id(&self, manifest_set_id: u64) -> bool {
        manifest_set_id == self.set_id
            || (self.inherited && manifest_set_id == legacy_pool_set_id(self.pool_id))
    }

    /// Whether this set may receive new objects: it is active, and this node
    /// knows the layout the set is recorded with.
    pub fn accepts_writes(&self) -> bool {
        self.state == ErasureSetState::Active && !self.layout_pending
    }
}

/// Who a set is, apart from its layout.
struct SetIdentity {
    pool_id: PoolId,
    set_id: u64,
    inherited: bool,
    topology_epoch: u64,
    created_epoch: u64,
}

/// The set id of a pool's inherited set: the pool id widened to `u64`.
///
/// This is the value EC v1 wrote into every manifest's `erasure_set_id`. The
/// inherited set carries it as its own `set_id` (see [`ErasureSet::inherited`]),
/// so an EC v1 manifest resolves to the inherited set directly.
pub fn legacy_pool_set_id(pool_id: PoolId) -> u64 {
    u64::from(pool_id.as_u32())
}

/// First id issued to an *additional* set in a pool.
///
/// The inherited set does not take this — it keeps its EC v1 value via
/// [`legacy_pool_set_id`]. Additional-set numbering starts here; the registry's
/// uniqueness check prevents an additional set from taking the inherited set's
/// id.
pub const FIRST_SET_ID: u64 = 1;

/// The erasure sets of one pool.
///
/// Lookup by id is a hash probe and enumeration keeps insertion order, so
/// resolving a manifest costs no more than it did when a pool held a single set
/// and the answer was a field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErasureSetRegistry {
    pool_id: PoolId,
    sets: HashMap<u64, ErasureSet>,
    /// Insertion order, kept so enumeration is stable across calls.
    order: Vec<u64>,
    /// The set carrying the pool's EC v1 identity, if the pool has one.
    inherited_id: Option<u64>,
}

impl ErasureSetRegistry {
    /// An empty registry for `pool_id`.
    pub fn new(pool_id: PoolId) -> Self {
        Self {
            pool_id,
            sets: HashMap::new(),
            order: Vec::new(),
            inherited_id: None,
        }
    }

    /// Collect the sets of one pool, rejecting a configuration that would make
    /// manifest resolution ambiguous.
    pub fn from_sets(pool_id: PoolId, sets: Vec<ErasureSet>) -> Result<Self, EcConfigError> {
        let mut registry = Self::new(pool_id);
        for set in sets {
            registry.insert(set)?;
        }
        Ok(registry)
    }

    /// Add one set.
    ///
    /// Rejects a set of another pool, a duplicate id, and a second inherited set:
    /// each of those would leave a manifest with more than one set to resolve to,
    /// or with none.
    pub fn insert(&mut self, set: ErasureSet) -> Result<(), EcConfigError> {
        if set.pool_id != self.pool_id {
            return Err(EcConfigError::Pool {
                pool: self.pool_id.to_string(),
                reason: format!(
                    "erasure set {} belongs to pool {}, not to this pool",
                    set.set_id, set.pool_id
                ),
            });
        }
        if self.sets.contains_key(&set.set_id) {
            return Err(EcConfigError::Pool {
                pool: self.pool_id.to_string(),
                reason: format!(
                    "erasure set id {} is already taken in this pool; set ids must be unique \
                     within a pool",
                    set.set_id
                ),
            });
        }
        if set.inherited {
            if let Some(existing) = self.inherited_id {
                return Err(EcConfigError::Pool {
                    pool: self.pool_id.to_string(),
                    reason: format!(
                        "set {} already carries the inherited identity of this pool; set {} \
                         cannot carry it too, or a manifest written by EC v1 would resolve \
                         to both",
                        existing, set.set_id
                    ),
                });
            }
            self.inherited_id = Some(set.set_id);
        }

        self.order.push(set.set_id);
        self.sets.insert(set.set_id, set);
        Ok(())
    }

    /// Pool these sets belong to.
    pub fn pool_id(&self) -> PoolId {
        self.pool_id
    }

    /// Number of sets in the pool.
    pub fn len(&self) -> usize {
        self.sets.len()
    }

    /// Whether the pool has no sets.
    pub fn is_empty(&self) -> bool {
        self.sets.is_empty()
    }

    /// Set by its issued identifier.
    pub fn get(&self, set_id: u64) -> Option<&ErasureSet> {
        self.sets.get(&set_id)
    }

    /// All sets in insertion order.
    pub fn sets(&self) -> Vec<&ErasureSet> {
        self.order.iter().filter_map(|set_id| self.sets.get(set_id)).collect()
    }

    /// Sets that may still receive new objects, in insertion order.
    pub fn active_sets(&self) -> Vec<&ErasureSet> {
        self.sets().into_iter().filter(|set| set.accepts_writes()).collect()
    }

    /// The set carrying the pool's EC v1 identity.
    pub fn inherited_set(&self) -> Option<&ErasureSet> {
        self.inherited_id.and_then(|set_id| self.sets.get(&set_id))
    }

    /// Set new objects go to while there is no placement router.
    ///
    /// The inherited set when the pool has one, otherwise the first set that may
    /// still take writes. Plan 03 phase 2 replaces this with a strategy-driven
    /// choice; until then a pool behaves as it did in EC v1.
    pub fn default_set(&self) -> Option<&ErasureSet> {
        self.inherited_set().or_else(|| self.active_sets().into_iter().next())
    }

    /// Every node holding a slot in any set of the pool, each listed once.
    pub fn distinct_nodes(&self) -> Vec<NodeId> {
        let mut nodes = self
            .sets()
            .into_iter()
            .flat_map(|set| set.slot_assignment.distinct_nodes())
            .collect::<Vec<_>>();
        nodes.sort_unstable_by_key(|node| node.0);
        nodes.dedup();
        nodes
    }

    /// Resolve the `erasure_set_id` of a manifest to the set that holds it.
    ///
    /// The three steps of EC2-041, in order:
    ///
    /// 1. an explicitly issued id wins (this now resolves the EC v1 value
    ///    directly, because the inherited set carries it);
    /// 2. otherwise the pool's legacy value resolves to the inherited set
    ///    (a defensive fallback for a renumbered inherited set);
    /// 3. otherwise the id is unknown to this pool and resolution fails.
    ///
    /// Step 3 returning `None` rather than falling back to the inherited set is
    /// deliberate: a manifest naming a set this pool never issued is corrupt or
    /// belongs elsewhere, and serving it from an unrelated set would read the
    /// wrong shards.
    pub fn resolve_manifest_set(&self, manifest_set_id: u64) -> Option<&ErasureSet> {
        if let Some(set) = self.sets.get(&manifest_set_id) {
            return Some(set);
        }
        if manifest_set_id == legacy_pool_set_id(self.pool_id) {
            return self.inherited_set();
        }
        None
    }
}

/// Role and LRC local group of one slot, which depend on its index alone.
///
/// Data slots come first, then local parity, then global parity — the codec's
/// order. Mirrors the per-slot logic of [`ShardSlotAssignment::build`], which is
/// kept as it is so the EC v1 layout cannot drift; a test holds the two equal.
fn slot_role(profile: &EcProfile, slot_index: u8) -> (ShardSlotRole, Option<u8>) {
    match profile.parity {
        EcParity::ReedSolomon { .. } => {
            if slot_index < profile.k {
                (ShardSlotRole::Data, None)
            } else {
                (ShardSlotRole::ReedSolomonParity, None)
            }
        }
        EcParity::Lrc { local, .. } => {
            let data_per_group = profile.k / local;
            if slot_index < profile.k {
                (ShardSlotRole::Data, Some(slot_index / data_per_group))
            } else if slot_index < profile.k + local {
                (ShardSlotRole::LrcLocalParity, Some(slot_index - profile.k))
            } else {
                (ShardSlotRole::LrcGlobalParity, None)
            }
        }
    }
}

fn ensure_distinct_nodes(pool: &str, nodes: &[NodeId]) -> Result<(), EcConfigError> {
    let mut seen = HashSet::with_capacity(nodes.len());
    for node_id in nodes {
        if !seen.insert(*node_id) {
            return Err(EcConfigError::Pool {
                pool: pool.to_string(),
                reason: format!(
                    "EC shard slots require distinct NodeIds; duplicate NodeId {node_id}"
                ),
            });
        }
    }
    Ok(())
}

/// Everything a node needs to build another set of the same pool.
///
/// A set added at runtime must come out identical on every node, and the parts
/// that decide that — the profile, the topology labels, the topology policy and
/// the epoch — are node configuration, not something an operator sends with the
/// request. Capturing them once at startup is what makes "add these nodes as a
/// set" mean the same thing everywhere.
#[derive(Debug, Clone)]
pub struct ErasureSetBuilder {
    pool_id: PoolId,
    pool_name: String,
    profile: EcProfile,
    /// Every node of the pool, whether or not it carries topology labels.
    pool_nodes: HashSet<NodeId>,
    labels: HashMap<NodeId, TopologyLabels>,
    rule: EcTopologyRule,
    topology_epoch: u64,
}

impl ErasureSetBuilder {
    /// Describe how a pool's sets are built.
    ///
    /// `pool_nodes` is the pool's roster: a set may only be built on nodes that
    /// appear in it, which is what stops an operator from placing a set on a
    /// node the pool does not have. `labels` covers only the nodes that carry
    /// topology labels, which may be none of them.
    pub fn new(
        pool_id: PoolId,
        pool_name: impl Into<String>,
        profile: EcProfile,
        pool_nodes: impl IntoIterator<Item = NodeId>,
        labels: HashMap<NodeId, TopologyLabels>,
        policy: EcTopologyPolicy,
        topology_epoch: u64,
    ) -> Self {
        Self {
            pool_id,
            pool_name: pool_name.into(),
            profile,
            pool_nodes: pool_nodes.into_iter().collect(),
            labels,
            rule: EcTopologyRule::v1(policy),
            topology_epoch,
        }
    }

    /// The same builder with the LRC group mode set to `level`.
    ///
    /// [`Self::new`] starts with the mode off, which is the EC v1 rule.
    pub fn with_lrc_group_domain(mut self, level: Option<EcFailureDomainLevel>) -> Self {
        self.rule.lrc_group_domain = level;
        self
    }

    /// Pool these sets belong to.
    pub fn pool_id(&self) -> PoolId {
        self.pool_id
    }

    /// The topology rule this node was configured with.
    pub fn rule(&self) -> EcTopologyRule {
        self.rule
    }

    /// The rule a set created on this node now would be laid out by.
    pub fn layout_rule_for_new_set(&self) -> EcSetLayoutRule {
        self.rule.layout_rule_for(&self.profile)
    }

    /// Which failure domain each node of the pool is in, at `level`.
    pub fn failure_domain_index(&self, level: EcFailureDomainLevel) -> FailureDomainIndex {
        FailureDomainIndex::build(level, self.pool_nodes(), &self.labels)
    }

    /// The pool's topology as `local_node_id` sees it.
    pub fn topology_view(&self, local_node_id: NodeId) -> PoolTopologyView {
        PoolTopologyView::build(local_node_id, &self.pool_nodes(), &self.labels)
    }

    /// Configured name of the pool.
    pub fn pool_name(&self) -> &str {
        &self.pool_name
    }

    /// Profile every set of this pool is built with.
    ///
    /// One profile per pool: the width of a set is fixed by it, and a set of a
    /// different width would change what `encode_chunk` produces, which no EC v2
    /// plan may do (conventions section 5, decision EC2-040).
    pub fn profile(&self) -> &EcProfile {
        &self.profile
    }

    /// Topology epoch every set of this pool is built at.
    pub fn topology_epoch(&self) -> u64 {
        self.topology_epoch
    }

    /// Every node of the pool, in configured order.
    pub fn pool_nodes(&self) -> Vec<NodeId> {
        let mut nodes = self.pool_nodes.iter().copied().collect::<Vec<_>>();
        nodes.sort_unstable_by_key(|node| node.0);
        nodes
    }

    /// Build one additional set on the given nodes with the EC v1 layout.
    ///
    /// This is how a set named by the roster is rebuilt before its layout
    /// record is known, whatever the LRC group mode says: a set that already
    /// exists keeps the rule it was created under (EC2-084, point 1). A new set
    /// goes through [`Self::build_new`] instead.
    ///
    /// Rejects a node the pool was not configured with: a set on a node outside
    /// the pool would place shards where nothing else looks for them.
    pub fn build(
        &self,
        set_id: u64,
        nodes: &[NodeId],
        created_epoch: u64,
    ) -> Result<ErasureSet, EcConfigError> {
        self.ensure_pool_nodes(nodes)?;
        ErasureSet::active(
            self.pool_id,
            set_id,
            self.profile.clone(),
            nodes,
            &self.labels,
            self.rule.policy,
            self.topology_epoch,
            created_epoch,
        )
        .map_err(|error| self.in_pool(error))
    }

    /// Build a set an operator is creating now, by the rule this node is
    /// configured with.
    ///
    /// With the LRC group mode off, or for an RS profile, that is the EC v1
    /// layout of [`Self::build`], unchanged to the last slot. With the mode on
    /// for an LRC profile the members of every local group are spread over
    /// distinct failure domains by the algorithm of EC2-082, and the set is
    /// judged by that rule. What happens when the nodes cannot be laid out that
    /// way is EC2-083: `strict` refuses with [`EcConfigError::LayoutRule`],
    /// `warn` builds the set as close to the rule as the nodes allow and says
    /// in the result how it falls short.
    ///
    /// Only the nodes of this set are judged; the pool's other sets play no
    /// part, so one set refused does not affect another (`EC2-ZONE-02-006`).
    pub fn build_new(
        &self,
        set_id: u64,
        nodes: &[NodeId],
        created_epoch: u64,
    ) -> Result<EcNewErasureSet, EcConfigError> {
        match self.layout_rule_for_new_set() {
            EcSetLayoutRule::V1 => Ok(EcNewErasureSet {
                set: self.build(set_id, nodes, created_epoch)?,
                violation: None,
                algorithm_version: EC_V1_LAYOUT_ALGORITHM_VERSION,
            }),
            EcSetLayoutRule::LrcGroups { level } => {
                self.build_by_group_rule(set_id, nodes, created_epoch, level)
            }
        }
    }

    fn build_by_group_rule(
        &self,
        set_id: u64,
        nodes: &[NodeId],
        created_epoch: u64,
        level: EcFailureDomainLevel,
    ) -> Result<EcNewErasureSet, EcConfigError> {
        self.ensure_pool_nodes(nodes)?;
        let width = usize::from(self.profile.total_shards());
        if nodes.len() != width {
            return Err(EcConfigError::Pool {
                pool: self.pool_name.clone(),
                reason: format!(
                    "EC fixed-width pool requires exactly {width} node(s) for profile '{}', got {}",
                    self.profile.name,
                    nodes.len()
                ),
            });
        }
        ensure_distinct_nodes(&self.pool_name, nodes)?;
        let strict = self.rule.policy == EcTopologyPolicy::Strict;

        // Without complete labels there is nothing to spread the groups by.
        let unlabeled = ec_lrc_placement::nodes_without_complete_labels(nodes, &self.labels);
        if !unlabeled.is_empty() {
            let violation = EcLayoutViolation {
                reason: EcLayoutViolationReason::IncompleteLabels,
                detail: incomplete_labels_detail(level, nodes.len(), &unlabeled),
            };
            if strict {
                return Err(refused_under_strict(violation));
            }
            // `warn`: the EC v1 layout, judged by the EC v1 checks, and told
            // plainly that the rule was not applied.
            let mut set = self.build(set_id, nodes, created_epoch)?;
            set.layout_rule = EcSetLayoutRule::LrcGroups { level };
            set.topology_risks.push(rule_not_applied_risk(level, &unlabeled, nodes.len()));
            return Ok(EcNewErasureSet {
                set,
                violation: Some(violation),
                algorithm_version: EC_V1_LAYOUT_ALGORITHM_VERSION,
            });
        }

        let planner = LrcGroupPlanner::new(&self.profile, nodes, &self.labels, level)
            .map_err(|error| self.placement_error(error))?;
        // The criterion is exact, so a refusal is decided before the search
        // runs, and nothing is built or written for it.
        let feasibility = planner.feasibility();
        let violation = feasibility.violation().map(|reason| EcLayoutViolation {
            reason,
            detail: feasibility.explain(&self.profile.name).unwrap_or_default(),
        });
        if let Some(violation) = violation.as_ref().filter(|_| strict) {
            return Err(refused_under_strict(violation.clone()));
        }

        let layout = planner.plan();
        let slot_assignment =
            ShardSlotAssignment::from_slot_order(&self.profile, &layout.slot_order)
                .map_err(|error| self.in_pool(error))?;
        let topology = validate_group_rule_topology(
            &self.profile,
            &slot_assignment,
            &self.labels,
            self.rule.policy,
            level,
        )?;
        Ok(EcNewErasureSet {
            set: ErasureSet::from_layout(
                SetIdentity {
                    pool_id: self.pool_id,
                    set_id,
                    inherited: false,
                    topology_epoch: self.topology_epoch,
                    created_epoch,
                },
                self.profile.clone(),
                slot_assignment,
                EcSetLayoutRule::LrcGroups { level },
                topology,
            ),
            violation,
            algorithm_version: EC_LRC_GROUP_LAYOUT_ALGORITHM_VERSION,
        })
    }

    /// Whether a layout record's slot order is an ordering of exactly `nodes`.
    ///
    /// A record naming any other node describes a different set — what a
    /// losing concurrent add leaves behind, or the trace of a configuration
    /// that changed the set's nodes — and must not be applied.
    pub fn record_fits(nodes: &[NodeId], slot_order: &[NodeId]) -> bool {
        let mut recorded = slot_order.to_vec();
        recorded.sort_unstable_by_key(|node| node.0);
        let mut configured = nodes.to_vec();
        configured.sort_unstable_by_key(|node| node.0);
        configured.dedup();
        recorded == configured
    }

    /// Build a set of the roster straight from its layout record.
    ///
    /// Never passes through the EC v1 layout on the way. A set laid out by the
    /// LRC group rule is judged by that rule alone (EC2-081, point 1): the EC
    /// v1 checks count racks and hosts by name only, and under `strict` they
    /// could refuse, at every start, a set the rule accepted.
    ///
    /// `nodes` are the nodes the roster gives the set; the record must be an
    /// ordering of exactly them ([`Self::record_fits`]).
    pub fn build_recorded(
        &self,
        set_id: u64,
        nodes: &[NodeId],
        created_epoch: u64,
        rule: EcSetLayoutRule,
        slot_order: &[NodeId],
    ) -> Result<ErasureSet, EcConfigError> {
        self.ensure_pool_nodes(nodes)?;
        let (slot_assignment, topology) =
            self.recorded_layout(set_id, &self.profile, nodes, rule, slot_order)?;
        Ok(ErasureSet::from_layout(
            SetIdentity {
                pool_id: self.pool_id,
                set_id,
                inherited: false,
                topology_epoch: self.topology_epoch,
                created_epoch,
            },
            self.profile.clone(),
            slot_assignment,
            rule,
            topology,
        ))
    }

    /// Rebuild `set` with the slot order its layout record holds.
    ///
    /// The recorded order replaces the one the labels would give, so a label
    /// change no longer moves any slot (EC2-086, point 2). The record must name
    /// exactly the set's nodes: a record that adds, drops or swaps a node
    /// describes a different set, and taking it would change which nodes the
    /// set's manifests are checked against.
    ///
    /// The topology report is recomputed for the recorded order under this
    /// node's labels and policy, by the set's own rule: the EC v1 checks for
    /// [`EcSetLayoutRule::V1`], the checks of the group rule for
    /// [`EcSetLayoutRule::LrcGroups`]. A broken group rule is reported as a
    /// risk and never refuses the set: restoring a set is how a node starts,
    /// and a rule must not stop a node from starting (EC2-083).
    pub fn with_recorded_layout(
        &self,
        set: &ErasureSet,
        rule: EcSetLayoutRule,
        slot_order: &[NodeId],
    ) -> Result<ErasureSet, EcConfigError> {
        let (slot_assignment, topology) = self.recorded_layout(
            set.set_id,
            &set.profile,
            &set.slot_assignment.distinct_nodes(),
            rule,
            slot_order,
        )?;
        let mut restored = set.clone();
        restored.slot_assignment = slot_assignment;
        restored.layout_rule = rule;
        restored.topology_health = topology.health;
        restored.topology_risks = topology.risks;
        restored.layout_pending = false;
        Ok(restored)
    }

    /// Where a set's slots sit, by failure domain, as this node's labels say.
    ///
    /// Grouped at the set's own rule level, so the placement is read against
    /// the guarantee the set actually carries. A set laid out by the EC v1 rule
    /// is grouped by rack — the level EC v1 already warns about — because it
    /// carries no rule level of its own.
    ///
    /// Labels are this node's (EC2-086, point 4): a node whose environment
    /// disagrees describes the same set differently, and the admin surface says
    /// whose view it is answering with.
    pub fn domain_placement(&self, set: &ErasureSet) -> SetDomainPlacement {
        let level = match set.layout_rule {
            EcSetLayoutRule::LrcGroups { level } => level,
            EcSetLayoutRule::V1 => EcFailureDomainLevel::Rack,
        };
        let domain_at = |node_id: NodeId, at: EcFailureDomainLevel| {
            self.labels
                .get(&node_id)
                .and_then(|label| at.domain_of(label))
                .map(|domain| domain.to_string())
        };

        let slots = set
            .slot_assignment
            .slots()
            .iter()
            .map(|slot| SlotPlacement {
                slot_index: slot.slot_index,
                node_id: slot.node_id.to_string(),
                role: slot.role,
                local_group: slot.local_group,
                zone: domain_at(slot.node_id, EcFailureDomainLevel::Zone),
                rack: domain_at(slot.node_id, EcFailureDomainLevel::Rack),
                host: domain_at(slot.node_id, EcFailureDomainLevel::Host),
            })
            .collect::<Vec<_>>();

        // Slots, and members of each local group, per domain of `level`.
        let mut occupancy: BTreeMap<String, (Vec<u8>, BTreeMap<u8, u8>)> = BTreeMap::new();
        let mut unlabeled: BTreeSet<String> = BTreeSet::new();
        for slot in set.slot_assignment.slots() {
            match domain_at(slot.node_id, level) {
                Some(domain) => {
                    let entry = occupancy.entry(domain).or_default();
                    entry.0.push(slot.slot_index);
                    if let Some(group) = slot.local_group {
                        *entry.1.entry(group).or_insert(0) += 1;
                    }
                }
                None => {
                    unlabeled.insert(slot.node_id.to_string());
                }
            }
        }

        let domains = occupancy
            .into_iter()
            .map(|(domain, (mut slots, groups))| {
                slots.sort_unstable();
                DomainOccupancy {
                    domain,
                    slots,
                    local_groups: groups
                        .into_iter()
                        .map(|(local_group, members)| DomainGroupMembers {
                            local_group,
                            members,
                        })
                        .collect(),
                }
            })
            .collect();

        SetDomainPlacement {
            level,
            slots,
            domains,
            unlabeled_nodes: unlabeled.into_iter().collect(),
        }
    }

    /// Whether `set` still keeps the guarantee of the rule it was laid out by.
    ///
    /// Only the set's own rule is applied. A set laid out by the EC v1 rule
    /// makes no placement guarantee and can never be a violator, so switching
    /// `S4_EC_LRC_GROUP_DOMAIN` on does not turn the pool's existing sets into
    /// violators (EC2-084, point 1).
    ///
    /// `created_in_breach` is what the set's layout record says about its
    /// birth. A set created under `warn` in breach of its rule has been a
    /// violator from the start and is reported as such rather than as drift
    /// (EC2-083, EC2-086 point 3).
    pub fn assess_set_layout(
        &self,
        set: &ErasureSet,
        created_in_breach: bool,
    ) -> SetLayoutCompliance {
        let EcSetLayoutRule::LrcGroups { level } = set.layout_rule else {
            return SetLayoutCompliance::NoGuarantee;
        };

        let slot_order = set.slot_assignment.slot_order();
        let planner = match LrcGroupPlanner::new(&set.profile, &slot_order, &self.labels, level) {
            Ok(planner) => planner,
            // Incomplete labels here, or a record naming a rule that cannot
            // describe this profile. Neither is a known violation: the rule
            // simply cannot be judged from what this node can see.
            Err(error) => {
                return SetLayoutCompliance::NotJudgeable {
                    detail: error.to_string(),
                };
            }
        };
        let Some(assessment) = planner.assess(&slot_order) else {
            return SetLayoutCompliance::NotJudgeable {
                detail: format!(
                    "the recorded slot order is not an ordering of the {} node(s) of erasure set {}",
                    slot_order.len(),
                    set.set_id
                ),
            };
        };
        if assessment.shared_domains.is_empty() {
            return SetLayoutCompliance::Meets;
        }

        let group_size = planner.feasibility().group_size();
        let detail = assessment
            .shared_domains
            .iter()
            .map(|shared| {
                format!(
                    "local group {} has {} of its {group_size} members in {level} '{}'",
                    shared.group, shared.members, shared.domain
                )
            })
            .collect::<Vec<_>>()
            .join("; ");
        SetLayoutCompliance::Violated {
            origin: if created_in_breach {
                LayoutViolationOrigin::Creation
            } else {
                LayoutViolationOrigin::Drift
            },
            coincidences: assessment.score.coincidences,
            detail,
        }
    }

    /// The assignment a record describes, and its report by the record's rule.
    fn recorded_layout(
        &self,
        set_id: u64,
        profile: &EcProfile,
        nodes: &[NodeId],
        rule: EcSetLayoutRule,
        slot_order: &[NodeId],
    ) -> Result<(ShardSlotAssignment, EcTopologyReport), EcConfigError> {
        if !Self::record_fits(nodes, slot_order) {
            let mut configured = nodes.to_vec();
            configured.sort_unstable_by_key(|node| node.0);
            return Err(EcConfigError::Pool {
                pool: self.pool_name.clone(),
                reason: format!(
                    "the layout record of erasure set {set_id} names nodes {slot_order:?}, but \
                     the set is built from {configured:?}; a layout record may only reorder a \
                     set's own nodes"
                ),
            });
        }

        let slot_assignment = ShardSlotAssignment::from_slot_order(profile, slot_order)
            .map_err(|error| self.in_pool(error))?;
        let topology = match rule {
            EcSetLayoutRule::V1 => {
                validate_topology(profile, &slot_assignment, &self.labels, self.rule.policy)?
            }
            EcSetLayoutRule::LrcGroups { level } => validate_group_rule_topology(
                profile,
                &slot_assignment,
                &self.labels,
                self.rule.policy,
                level,
            )?,
        };
        Ok((slot_assignment, topology))
    }

    /// A placement error, in the words of this builder's pool.
    fn placement_error(&self, error: LrcPlacementError) -> EcConfigError {
        EcConfigError::Pool {
            pool: self.pool_name.clone(),
            reason: error.to_string(),
        }
    }

    fn ensure_pool_nodes(&self, nodes: &[NodeId]) -> Result<(), EcConfigError> {
        for node_id in nodes {
            if !self.pool_nodes.contains(node_id) {
                return Err(EcConfigError::Pool {
                    pool: self.pool_name.clone(),
                    reason: format!("node {node_id} is not a node of this pool"),
                });
            }
        }
        Ok(())
    }

    /// Name this builder's pool in a pool error raised below it.
    fn in_pool(&self, error: EcConfigError) -> EcConfigError {
        match error {
            EcConfigError::Pool { reason, .. } => EcConfigError::Pool {
                pool: self.pool_name.clone(),
                reason,
            },
            other => other,
        }
    }
}

/// A set an operator is creating, with what its layout record must say about
/// how it was laid out.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EcNewErasureSet {
    /// The set, laid out and judged.
    pub set: ErasureSet,
    /// How the set falls short of its rule, when it was created under `warn`
    /// in breach of it. Always `None` for the EC v1 rule and under `strict`.
    pub violation: Option<EcLayoutViolation>,
    /// Version of the algorithm that chose the slot order.
    pub algorithm_version: u32,
}

/// The error `strict` refuses a set with: what is needed and what the set
/// has, and the other way out.
fn refused_under_strict(mut violation: EcLayoutViolation) -> EcConfigError {
    violation
        .detail
        .push_str(", or set S4_EC_TOPOLOGY_POLICY=warn to create the set with degraded topology");
    EcConfigError::LayoutRule(violation)
}

/// Which nodes lack which labels, for a rule that needs them all (EC2-083).
fn incomplete_labels_detail(
    level: EcFailureDomainLevel,
    set_nodes: usize,
    unlabeled: &[UnlabeledNode],
) -> String {
    let nodes = unlabeled
        .iter()
        .map(|node| format!("{} ({})", node.node_id, node.missing.join(", ")))
        .collect::<Vec<_>>()
        .join("; ");
    format!(
        "{LRC_GROUP_DOMAIN_ENV}={level} needs complete zone/rack/host/disk_group labels on every \
         node of the set; {} of {set_nodes} nodes lack them: {nodes}",
        unlabeled.len()
    )
}

/// The risk a set created under `warn` without complete labels carries.
fn rule_not_applied_risk(
    level: EcFailureDomainLevel,
    unlabeled: &[UnlabeledNode],
    set_nodes: usize,
) -> String {
    let why =
        if unlabeled.len() == set_nodes && unlabeled.iter().all(|node| node.missing.len() == 4) {
            "no node of the set carries topology labels".to_string()
        } else {
            format!(
                "{} of {set_nodes} nodes lack complete topology labels",
                unlabeled.len()
            )
        };
    format!(
        "{LRC_GROUP_DOMAIN_ENV}={level} is not applied to this set: {why}, so it keeps the EC v1 \
         layout"
    )
}

/// The erasure-set layout of one pool as the node's configuration describes it.
#[derive(Debug, Clone)]
pub struct EcPoolLayout {
    /// The sets the node starts with — today exactly the inherited one.
    pub sets: ErasureSetRegistry,
    /// What further sets are built from, when an operator adds one.
    pub builder: ErasureSetBuilder,
}

/// Validate an EC pool from the process environment.
///
/// `nodes` must preserve the configured pool-node names so topology labels can
/// be matched before the durable shard assignment is stored by NodeId.
///
/// The environment describes one set — the pool's inherited set — exactly as EC
/// v1 did, and the result is a registry holding it. Further sets are added at
/// runtime through the admin API rather than through startup configuration, so a
/// deployment that changes nothing keeps a single set and the v1 behaviour.
///
/// The inherited set is laid out by the EC v1 rule whatever
/// `S4_EC_LRC_GROUP_DOMAIN` says. Startup never lays a set out by the LRC group
/// rule: that takes an explicit operator action, so switching the mode on
/// cannot quietly move data or stop a node from starting (EC2-084, points 1 and
/// 2; EC2-083). The variable is still parsed here, so an unknown value stops
/// the start.
pub fn validate_pool_from_env(
    pool: &PoolConfig,
    nodes: &[(String, NodeId)],
    topology_epoch: u64,
    created_epoch: u64,
) -> Result<EcPoolLayout, EcConfigError> {
    if pool.storage_class != StorageClass::ErasureCoded {
        return Err(EcConfigError::Pool {
            pool: pool.name.clone(),
            reason: "validate_pool_from_env only accepts erasure-coded pools".to_string(),
        });
    }

    let profile = EcProfile::from_env()?;
    let rule = EcTopologyRule::from_env()?;
    let policy = rule.policy;
    let labels_by_name = parse_node_topology_env()?;
    let labels = labels_by_node_id(nodes, labels_by_name)?;
    let node_ids: Vec<NodeId> = nodes.iter().map(|(_, node_id)| *node_id).collect();

    // A pool used to be exactly one set wide. It may now be a whole number of
    // sets wide, so that an operator who has added nodes to the pool has
    // somewhere to put the next set — which is the point of multi-set pools.
    //
    // The inherited set takes the first `total_shards` nodes in configured
    // order, and that order is load-bearing: it is what every manifest written
    // so far names. New nodes are therefore appended, never inserted, and the
    // roster refuses a configuration that moved them (see
    // `EcErasureSetRoster::inherited_node_ids`).
    let width = usize::from(profile.total_shards());
    if node_ids.is_empty() || !node_ids.len().is_multiple_of(width) {
        return Err(EcConfigError::Pool {
            pool: pool.name.clone(),
            reason: format!(
                "EC pool needs a whole number of erasure sets' worth of nodes: profile '{}' is {} \
                 node(s) wide and the pool has {}",
                profile.name,
                width,
                node_ids.len()
            ),
        });
    }

    let set = ErasureSet::inherited(
        pool.pool_id,
        profile.clone(),
        &node_ids[..width],
        &labels,
        policy,
        topology_epoch,
        created_epoch,
    )
    .map_err(|error| match error {
        EcConfigError::Pool { reason, .. } => EcConfigError::Pool {
            pool: pool.name.clone(),
            reason,
        },
        other => other,
    })?;

    let sets =
        ErasureSetRegistry::from_sets(pool.pool_id, vec![set]).map_err(|error| match error {
            EcConfigError::Pool { reason, .. } => EcConfigError::Pool {
                pool: pool.name.clone(),
                reason,
            },
            other => other,
        })?;

    Ok(EcPoolLayout {
        sets,
        builder: ErasureSetBuilder {
            pool_id: pool.pool_id,
            pool_name: pool.name.clone(),
            profile,
            pool_nodes: node_ids.iter().copied().collect(),
            labels,
            rule,
            topology_epoch,
        },
    })
}

fn validate_topology(
    profile: &EcProfile,
    assignment: &ShardSlotAssignment,
    labels: &HashMap<NodeId, TopologyLabels>,
    policy: EcTopologyPolicy,
) -> Result<EcTopologyReport, EcConfigError> {
    let node_ids: Vec<NodeId> = assignment.slots().iter().map(|slot| slot.node_id).collect();
    let labeled = node_ids
        .iter()
        .filter(|node_id| labels.get(node_id).is_some_and(|label| !label.is_empty()))
        .count();

    if labeled == 0 {
        return Ok(EcTopologyReport {
            health: EcTopologyHealth::TopologyUnknown,
            risks: vec![
                "all EC pool nodes are unlabeled; deterministic slot spread is used".to_string(),
            ],
        });
    }

    let complete = node_ids
        .iter()
        .filter(|node_id| labels.get(node_id).is_some_and(TopologyLabels::is_complete))
        .count();

    if complete != node_ids.len() {
        let message = format!(
            "partial topology labels: {complete}/{} node(s) have complete zone/rack/host/disk_group labels",
            node_ids.len()
        );
        if policy == EcTopologyPolicy::Strict {
            return Err(EcConfigError::Topology(format!(
                "{message}; set S4_EC_TOPOLOGY_POLICY=warn to continue with degraded health"
            )));
        }
        return Ok(EcTopologyReport {
            health: EcTopologyHealth::TopologyDegraded,
            risks: vec![message],
        });
    }

    let mut risks = Vec::new();
    let zones = distinct_label_values(&node_ids, labels, |label| label.zone.as_deref());
    let racks = distinct_label_values(&node_ids, labels, |label| label.rack.as_deref());
    let hosts = distinct_label_values(&node_ids, labels, |label| label.host.as_deref());

    if hosts.len() < node_ids.len() {
        let message = format!(
            "host labels identify {} distinct host(s) for {} shard slot(s); strict EC placement requires one host per slot",
            hosts.len(),
            node_ids.len()
        );
        if policy == EcTopologyPolicy::Strict {
            return Err(EcConfigError::Topology(message));
        }
        risks.push(message);
    }

    let required_failure_domains =
        usize::from(profile.parity_count_for_topology()).saturating_add(1);
    if racks.len() < required_failure_domains.min(node_ids.len()) {
        risks.push(format!(
            "rack diversity is {} for {} shard slot(s); profile '{}' has {} parity shard(s)",
            racks.len(),
            node_ids.len(),
            profile.name,
            profile.parity_count_for_topology()
        ));
    }
    if zones.len() == 1 && node_ids.len() > 1 {
        risks.push("all EC shard slots are in one zone".to_string());
    }

    if matches!(profile.parity, EcParity::Lrc { .. }) {
        for group in local_group_slots(assignment) {
            let group_racks = distinct_label_values(&group, labels, |label| label.rack.as_deref());
            if group.len() > 1 && racks.len() > 1 && group_racks.len() == 1 {
                risks.push(format!(
                    "LRC local group is entirely placed in rack '{}'",
                    group_racks.iter().next().cloned().unwrap_or_default()
                ));
            }
        }
    }

    Ok(EcTopologyReport {
        health: if risks.is_empty() {
            EcTopologyHealth::Healthy
        } else {
            EcTopologyHealth::TopologyDegraded
        },
        risks,
    })
}

/// Topology report of a set laid out by the LRC group rule at `level`.
///
/// The EC v1 checks of the whole set stay, with one difference: a rack and a
/// host are named by every label above them, so rack `r1` of two zones counts
/// as two racks (EC2-081, point 1). The EC v1 warning about a group entirely in
/// one rack gives way to the rule itself: every domain holding more than one
/// member of a group is a risk. And every zone whose loss the decoder does not
/// survive is named (EC2-081, point 3).
///
/// Only the EC v1 checks can fail, under `strict`, as they always could. The
/// group rule is a risk here, never an error: this report is also how a
/// restored set is judged, and a rule must not stop a node from starting
/// (EC2-083). Creating a set checks the rule before it comes here.
fn validate_group_rule_topology(
    profile: &EcProfile,
    assignment: &ShardSlotAssignment,
    labels: &HashMap<NodeId, TopologyLabels>,
    policy: EcTopologyPolicy,
    level: EcFailureDomainLevel,
) -> Result<EcTopologyReport, EcConfigError> {
    let node_ids = assignment.slot_order();
    let unlabeled = ec_lrc_placement::nodes_without_complete_labels(&node_ids, labels);
    if !unlabeled.is_empty() {
        // Not complete: exactly the EC v1 verdict, plus the reason the rule
        // cannot be judged.
        let mut report = validate_topology(profile, assignment, labels, policy)?;
        report.risks.push(rule_not_applied_risk(level, &unlabeled, node_ids.len()));
        return Ok(report);
    }

    let distinct = |level: EcFailureDomainLevel| {
        node_ids
            .iter()
            .filter_map(|node_id| labels.get(node_id).and_then(|label| level.domain_of(label)))
            .collect::<BTreeSet<_>>()
    };
    let zones = distinct(EcFailureDomainLevel::Zone);
    let racks = distinct(EcFailureDomainLevel::Rack);
    let hosts = distinct(EcFailureDomainLevel::Host);

    let mut risks = Vec::new();
    if hosts.len() < node_ids.len() {
        let message = format!(
            "host labels identify {} distinct host(s) for {} shard slot(s); strict EC placement requires one host per slot",
            hosts.len(),
            node_ids.len()
        );
        if policy == EcTopologyPolicy::Strict {
            return Err(EcConfigError::Topology(message));
        }
        risks.push(message);
    }
    let required_failure_domains =
        usize::from(profile.parity_count_for_topology()).saturating_add(1);
    if racks.len() < required_failure_domains.min(node_ids.len()) {
        risks.push(format!(
            "rack diversity is {} for {} shard slot(s); profile '{}' has {} parity shard(s)",
            racks.len(),
            node_ids.len(),
            profile.name,
            profile.parity_count_for_topology()
        ));
    }
    if zones.len() == 1 && node_ids.len() > 1 {
        risks.push("all EC shard slots are in one zone".to_string());
    }

    // A record naming this rule for a set it cannot describe — an RS profile,
    // say — is only possible if the record is damaged. It is reported, not
    // refused: judging a set must not stop a node from starting.
    let planner = match LrcGroupPlanner::new(profile, &node_ids, labels, level) {
        Ok(planner) => Some(planner),
        Err(error) => {
            risks.push(format!(
                "the {LRC_GROUP_DOMAIN_ENV}={level} rule this set is recorded with cannot judge \
                 it: {error}"
            ));
            None
        }
    };
    if let Some(explanation) = planner
        .as_ref()
        .and_then(|planner| planner.feasibility().explain(&profile.name))
    {
        risks.push(explanation);
    }
    if let Some((planner, assessment)) = planner
        .as_ref()
        .and_then(|planner| planner.assess(&node_ids).map(|assessment| (planner, assessment)))
    {
        let group_size = planner.feasibility().group_size();
        for shared in &assessment.shared_domains {
            risks.push(if shared.members == group_size {
                format!(
                    "LRC local group {} is entirely placed in {level} '{}'",
                    shared.group, shared.domain
                )
            } else {
                format!(
                    "LRC local group {} has {} of its {group_size} members in {level} '{}'; \
                     {LRC_GROUP_DOMAIN_ENV}={level} puts each member in a different {level}",
                    shared.group, shared.members, shared.domain
                )
            });
        }
        for fatal in &assessment.fatal_zones {
            risks.push(format!(
                "profile '{}' does not survive the loss of zone '{}', which holds {} of the \
                 set's {} slot(s)",
                profile.name,
                fatal.zone,
                fatal.slots,
                node_ids.len()
            ));
        }
    }

    Ok(EcTopologyReport {
        health: if risks.is_empty() {
            EcTopologyHealth::Healthy
        } else {
            EcTopologyHealth::TopologyDegraded
        },
        risks,
    })
}

fn local_group_slots(assignment: &ShardSlotAssignment) -> Vec<Vec<NodeId>> {
    let mut groups: BTreeMap<u8, Vec<NodeId>> = BTreeMap::new();
    for slot in assignment.slots() {
        if let Some(group) = slot.local_group {
            groups.entry(group).or_default().push(slot.node_id);
        }
    }
    groups.into_values().collect()
}

fn distinct_label_values<'a>(
    node_ids: &[NodeId],
    labels: &'a HashMap<NodeId, TopologyLabels>,
    selector: impl Fn(&'a TopologyLabels) -> Option<&'a str>,
) -> BTreeSet<String> {
    node_ids
        .iter()
        .filter_map(|node_id| labels.get(node_id))
        .filter_map(selector)
        .map(ToOwned::to_owned)
        .collect()
}

fn topology_spread_order(
    nodes: &[NodeId],
    labels: &HashMap<NodeId, TopologyLabels>,
) -> Vec<NodeId> {
    let all_complete = nodes
        .iter()
        .all(|node_id| labels.get(node_id).is_some_and(TopologyLabels::is_complete));
    if !all_complete {
        let mut sorted = nodes.to_vec();
        sorted.sort_by_key(|node| node.0);
        return sorted;
    }

    let mut by_domain: BTreeMap<(String, String), Vec<NodeId>> = BTreeMap::new();
    for node in nodes {
        let label = labels.get(node).expect("all_complete ensures every node has labels");
        by_domain
            .entry((
                label.zone.clone().unwrap_or_default(),
                label.rack.clone().unwrap_or_default(),
            ))
            .or_default()
            .push(*node);
    }
    for domain_nodes in by_domain.values_mut() {
        domain_nodes.sort_by_key(|node| node.0);
    }

    let mut queues: Vec<VecDeque<NodeId>> = by_domain.into_values().map(VecDeque::from).collect();
    let mut ordered = Vec::with_capacity(nodes.len());
    while ordered.len() < nodes.len() {
        for queue in &mut queues {
            if let Some(node) = queue.pop_front() {
                ordered.push(node);
            }
        }
    }
    ordered
}

fn labels_by_node_id(
    nodes: &[(String, NodeId)],
    labels_by_name: BTreeMap<String, TopologyLabels>,
) -> Result<HashMap<NodeId, TopologyLabels>, EcConfigError> {
    let node_names: BTreeSet<&str> = nodes.iter().map(|(name, _)| name.as_str()).collect();
    for configured_name in labels_by_name.keys() {
        if !node_names.contains(configured_name.as_str()) {
            return Err(EcConfigError::Topology(format!(
                "S4_EC_NODE_TOPOLOGY contains unknown node '{configured_name}'"
            )));
        }
    }

    let mut labels = HashMap::new();
    for (name, node_id) in nodes {
        if let Some(label) = labels_by_name.get(name) {
            labels.insert(*node_id, label.clone());
        }
    }
    Ok(labels)
}

fn parse_node_topology_env() -> Result<BTreeMap<String, TopologyLabels>, EcConfigError> {
    let Ok(raw) = std::env::var("S4_EC_NODE_TOPOLOGY") else {
        return Ok(BTreeMap::new());
    };
    parse_node_topology(&raw)
}

fn parse_node_topology(raw: &str) -> Result<BTreeMap<String, TopologyLabels>, EcConfigError> {
    let mut result = BTreeMap::new();
    for entry in raw.split(';').map(str::trim).filter(|entry| !entry.is_empty()) {
        let (node_name, labels_raw) = entry.split_once(':').ok_or_else(|| EcConfigError::Env {
            name: "S4_EC_NODE_TOPOLOGY",
            reason: format!(
                "entry '{entry}' must use node:zone=...,rack=...,host=...,disk_group=..."
            ),
        })?;

        let mut labels = TopologyLabels::default();
        for pair in labels_raw.split(',').map(str::trim).filter(|pair| !pair.is_empty()) {
            let (key, value) = pair.split_once('=').ok_or_else(|| EcConfigError::Env {
                name: "S4_EC_NODE_TOPOLOGY",
                reason: format!("label '{pair}' must use key=value"),
            })?;
            let value = value.trim();
            if value.is_empty() {
                return Err(EcConfigError::Env {
                    name: "S4_EC_NODE_TOPOLOGY",
                    reason: format!("label '{key}' for node '{node_name}' must not be empty"),
                });
            }
            match key.trim() {
                "zone" => labels.zone = Some(value.to_string()),
                "rack" => labels.rack = Some(value.to_string()),
                "host" => labels.host = Some(value.to_string()),
                "disk_group" => labels.disk_group = Some(value.to_string()),
                other => {
                    return Err(EcConfigError::Env {
                        name: "S4_EC_NODE_TOPOLOGY",
                        reason: format!("unknown label '{other}' for node '{node_name}'"),
                    });
                }
            }
        }

        if result.insert(node_name.to_string(), labels).is_some() {
            return Err(EcConfigError::Env {
                name: "S4_EC_NODE_TOPOLOGY",
                reason: format!("duplicate topology entry for node '{node_name}'"),
            });
        }
    }
    Ok(result)
}

fn invalid_profile(profile: &str, reason: impl Into<String>) -> EcConfigError {
    EcConfigError::Profile {
        profile: profile.to_string(),
        reason: reason.into(),
    }
}

fn parse_required_u8(name: &'static str) -> Result<u8, EcConfigError> {
    let raw = std::env::var(name).map_err(|_| EcConfigError::Env {
        name,
        reason: "is required for S4_EC_PROFILE=custom".to_string(),
    })?;
    parse_u8(name, &raw)
}

fn parse_optional_u64(name: &'static str) -> Result<Option<u64>, EcConfigError> {
    match std::env::var(name) {
        Ok(raw) => raw.parse::<u64>().map(Some).map_err(|e| EcConfigError::Env {
            name,
            reason: e.to_string(),
        }),
        Err(_) => Ok(None),
    }
}

fn parse_u8(name: &'static str, raw: &str) -> Result<u8, EcConfigError> {
    raw.parse::<u8>().map_err(|e| EcConfigError::Env {
        name,
        reason: e.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nodes(count: u8) -> Vec<NodeId> {
        (1..=count).map(|idx| NodeId(u128::from(idx))).collect()
    }

    fn complete_labels(nodes: &[NodeId]) -> HashMap<NodeId, TopologyLabels> {
        nodes
            .iter()
            .enumerate()
            .map(|(idx, node)| {
                (
                    *node,
                    TopologyLabels {
                        zone: Some(format!("z{}", idx % 3)),
                        rack: Some(format!("r{}", idx % 4)),
                        host: Some(format!("h{idx}")),
                        disk_group: Some(format!("d{}", idx % 2)),
                    },
                )
            })
            .collect()
    }

    #[test]
    fn rs_thresholds_are_separate() {
        let profile = EcProfile::rs_standard();
        assert_eq!(profile.total_shards(), 9);
        assert_eq!(profile.required_shard_acks_inline(), 9);
        assert_eq!(profile.manifest_quorum(), 5);
        assert_eq!(profile.read_minimum_normal(), 6);
    }

    #[test]
    fn invalid_profiles_are_rejected() {
        let profile = EcProfile {
            name: "bad".to_string(),
            codec: EcCodecKind::ReedSolomon,
            k: 6,
            parity: EcParity::Lrc {
                local: 2,
                global: 2,
            },
            min_object_size: DEFAULT_MIN_OBJECT_SIZE,
            chunk_size: DEFAULT_CHUNK_SIZE,
        };
        assert!(profile.validate().is_err());
    }

    #[test]
    fn erasure_set_rejects_wrong_fixed_width() {
        let profile = EcProfile::rs_small();
        let nodes = nodes(5);
        let labels = complete_labels(&nodes);
        let err = ErasureSet::active(
            PoolId::new(7),
            legacy_pool_set_id(PoolId::new(7)),
            profile,
            &nodes,
            &labels,
            EcTopologyPolicy::Strict,
            1,
            1,
        )
        .unwrap_err();
        assert!(err.to_string().contains("fixed-width"));
    }

    #[test]
    fn erasure_set_rejects_duplicate_nodes() {
        let profile = EcProfile::rs_small();
        let mut nodes = nodes(profile.total_shards());
        nodes[1] = nodes[0];
        let err = ErasureSet::active(
            PoolId::new(7),
            legacy_pool_set_id(PoolId::new(7)),
            profile,
            &nodes,
            &HashMap::new(),
            EcTopologyPolicy::Strict,
            1,
            1,
        )
        .unwrap_err();
        assert!(err.to_string().contains("distinct NodeIds"));
    }

    #[test]
    fn unlabeled_pool_is_topology_unknown_but_valid() {
        let profile = EcProfile::rs_small();
        let nodes = nodes(profile.total_shards());
        let set = ErasureSet::active(
            PoolId::new(1),
            legacy_pool_set_id(PoolId::new(1)),
            profile,
            &nodes,
            &HashMap::new(),
            EcTopologyPolicy::Strict,
            1,
            1,
        )
        .unwrap();
        assert_eq!(set.topology_health, EcTopologyHealth::TopologyUnknown);
        assert_eq!(set.slot_assignment.len(), 6);
    }

    #[test]
    fn partial_topology_fails_in_strict_mode() {
        let profile = EcProfile::rs_small();
        let nodes = nodes(profile.total_shards());
        let mut labels = HashMap::new();
        labels.insert(
            nodes[0],
            TopologyLabels {
                zone: Some("z1".to_string()),
                ..TopologyLabels::default()
            },
        );

        let err = ErasureSet::active(
            PoolId::new(1),
            legacy_pool_set_id(PoolId::new(1)),
            profile,
            &nodes,
            &labels,
            EcTopologyPolicy::Strict,
            1,
            1,
        )
        .unwrap_err();
        assert!(err.to_string().contains("partial topology labels"));
    }

    #[test]
    fn partial_topology_is_degraded_in_warn_mode() {
        let profile = EcProfile::rs_small();
        let nodes = nodes(profile.total_shards());
        let mut labels = HashMap::new();
        labels.insert(
            nodes[0],
            TopologyLabels {
                zone: Some("z1".to_string()),
                ..TopologyLabels::default()
            },
        );

        let set = ErasureSet::active(
            PoolId::new(1),
            legacy_pool_set_id(PoolId::new(1)),
            profile,
            &nodes,
            &labels,
            EcTopologyPolicy::Warn,
            1,
            1,
        )
        .unwrap();
        assert_eq!(set.topology_health, EcTopologyHealth::TopologyDegraded);
        assert!(set.topology_risks[0].contains("partial topology labels"));
    }

    #[test]
    fn imbalanced_racks_are_reported_degraded() {
        let profile = EcProfile::rs_small();
        let nodes = nodes(profile.total_shards());
        let labels: HashMap<_, _> = nodes
            .iter()
            .enumerate()
            .map(|(idx, node)| {
                let rack = if idx == 0 { "rack-b" } else { "rack-a" };
                (
                    *node,
                    TopologyLabels {
                        zone: Some("zone-a".to_string()),
                        rack: Some(rack.to_string()),
                        host: Some(format!("host-{idx}")),
                        disk_group: Some(format!("disk-{idx}")),
                    },
                )
            })
            .collect();

        let set = ErasureSet::active(
            PoolId::new(1),
            legacy_pool_set_id(PoolId::new(1)),
            profile,
            &nodes,
            &labels,
            EcTopologyPolicy::Strict,
            1,
            1,
        )
        .unwrap();
        assert_eq!(set.topology_health, EcTopologyHealth::TopologyDegraded);
        assert!(set.topology_risks.iter().any(|risk| risk.contains("rack diversity")));
    }

    #[test]
    fn node_topology_parser_accepts_complete_entries() {
        let parsed = parse_node_topology(
            "n1:zone=z1,rack=r1,host=h1,disk_group=d1;\
             n2:zone=z2,rack=r2,host=h2,disk_group=d2",
        )
        .unwrap();
        assert_eq!(parsed.len(), 2);
        assert!(parsed["n1"].is_complete());
    }

    use super::PLACEMENT_ENV_LOCK as MULTI_SET_ENV_LOCK;

    struct MultiSetEnvGuard {
        _lock: std::sync::MutexGuard<'static, ()>,
    }

    impl MultiSetEnvGuard {
        fn acquire() -> Self {
            let lock = MULTI_SET_ENV_LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            Self::clear();
            Self { _lock: lock }
        }

        fn clear() {
            std::env::remove_var(MULTI_SET_ENABLED_ENV);
            std::env::remove_var(PLACEMENT_STRATEGY_ENV);
        }
    }

    impl Drop for MultiSetEnvGuard {
        fn drop(&mut self) {
            Self::clear();
        }
    }

    /// A deployment that sets neither variable keeps the EC v1 behaviour: one
    /// inherited set per pool, and no placement choice to make.
    #[test]
    fn multi_set_defaults_are_disabled_and_round_robin() {
        let _guard = MultiSetEnvGuard::acquire();

        let config = EcMultiSetConfig::from_env().unwrap();
        assert!(!config.enabled);
        assert!(!config.allows_multiple_sets());
        assert_eq!(config.placement, EcPlacementStrategy::RoundRobin);
        assert_eq!(config, EcMultiSetConfig::default());
    }

    #[test]
    fn multi_set_enabled_accepts_every_truthy_spelling() {
        let _guard = MultiSetEnvGuard::acquire();

        for value in ["1", "true", "TRUE", "yes", "on"] {
            std::env::set_var(MULTI_SET_ENABLED_ENV, value);
            let config = EcMultiSetConfig::from_env().unwrap();
            assert!(config.enabled, "value '{value}' must enable multi-set");
        }
    }

    #[test]
    fn multi_set_disabled_accepts_every_falsy_spelling() {
        let _guard = MultiSetEnvGuard::acquire();

        for value in ["0", "false", "FALSE", "no", "off"] {
            std::env::set_var(MULTI_SET_ENABLED_ENV, value);
            let config = EcMultiSetConfig::from_env().unwrap();
            assert!(!config.enabled, "value '{value}' must disable multi-set");
        }
    }

    #[test]
    fn placement_strategy_accepts_both_names_and_hyphens() {
        let _guard = MultiSetEnvGuard::acquire();

        for value in ["round_robin", "round-robin", "ROUND_ROBIN"] {
            std::env::set_var(PLACEMENT_STRATEGY_ENV, value);
            assert_eq!(
                EcPlacementStrategy::from_env().unwrap(),
                EcPlacementStrategy::RoundRobin,
                "value '{value}' must select round robin"
            );
        }
        for value in ["free_space", "free-space", "FREE_SPACE"] {
            std::env::set_var(PLACEMENT_STRATEGY_ENV, value);
            assert_eq!(
                EcPlacementStrategy::from_env().unwrap(),
                EcPlacementStrategy::FreeSpace,
                "value '{value}' must select free space"
            );
        }
    }

    #[test]
    fn invalid_multi_set_values_are_rejected_by_name() {
        let _guard = MultiSetEnvGuard::acquire();

        std::env::set_var(MULTI_SET_ENABLED_ENV, "maybe");
        let err = EcMultiSetConfig::from_env().unwrap_err();
        assert!(matches!(
            err,
            EcConfigError::Env {
                name: MULTI_SET_ENABLED_ENV,
                ..
            }
        ));
        assert!(err.to_string().contains("maybe"));

        MultiSetEnvGuard::clear();
        std::env::set_var(PLACEMENT_STRATEGY_ENV, "least_loaded");
        let err = EcPlacementStrategy::from_env().unwrap_err();
        assert!(matches!(
            err,
            EcConfigError::Env {
                name: PLACEMENT_STRATEGY_ENV,
                ..
            }
        ));
        assert!(err.to_string().contains("least_loaded"));
    }

    /// A misspelled strategy is rejected even while the feature is off. Failing
    /// at startup beats silently ignoring a setting the operator meant to apply
    /// the moment they switch multi-set on.
    #[test]
    fn invalid_strategy_is_rejected_while_feature_is_disabled() {
        let _guard = MultiSetEnvGuard::acquire();

        std::env::set_var(MULTI_SET_ENABLED_ENV, "false");
        std::env::set_var(PLACEMENT_STRATEGY_ENV, "roundrobin");
        assert!(EcMultiSetConfig::from_env().is_err());
    }

    // -----------------------------------------------------------------------
    // Multi-set pools: identity, registry, inherited manifests (plan 03, phase 1)
    // -----------------------------------------------------------------------

    /// Build a set over `count` distinct nodes with an explicit id.
    fn set_with_id(pool_id: PoolId, set_id: u64, count: u8) -> ErasureSet {
        let profile = EcProfile::rs_small();
        let set_nodes = nodes(count);
        ErasureSet::active(
            pool_id,
            set_id,
            profile,
            &set_nodes,
            &HashMap::new(),
            EcTopologyPolicy::Strict,
            1,
            1,
        )
        .unwrap()
    }

    /// The pool's inherited set: the one carrying EC v1's identity.
    fn inherited_set(pool_id: PoolId) -> ErasureSet {
        let profile = EcProfile::rs_small();
        let set_nodes = nodes(profile.total_shards());
        ErasureSet::inherited(
            pool_id,
            profile,
            &set_nodes,
            &HashMap::new(),
            EcTopologyPolicy::Strict,
            1,
            1,
        )
        .unwrap()
    }

    /// `set_id` is issued, not derived. This is the whole point of EC2-041: two
    /// sets in one pool must not collide, which they inevitably did while the id
    /// was computed from the pool.
    #[test]
    fn set_id_is_issued_independently_of_pool_id() {
        let pool_id = PoolId::new(9);
        let set = set_with_id(pool_id, 2, EcProfile::rs_small().total_shards());

        assert_eq!(set.set_id, 2);
        assert_ne!(set.set_id, u64::from(pool_id.as_u32()));
        assert!(!set.inherited);
    }

    /// The inherited set takes the pool's EC v1 value, and is the only set that
    /// answers to the value EC v1 wrote into manifests.
    #[test]
    fn inherited_set_carries_the_ec_v1_value_and_accepts_legacy_manifests() {
        let pool_id = PoolId::new(9);
        let set = inherited_set(pool_id);

        assert_eq!(set.set_id, legacy_pool_set_id(pool_id));
        assert!(set.inherited);
        // Its own id — which is the value EC v1 wrote into every manifest — qualifies.
        assert!(set.accepts_manifest_set_id(set.set_id));
        assert!(set.accepts_manifest_set_id(legacy_pool_set_id(pool_id)));
        // Anything else does not.
        assert!(!set.accepts_manifest_set_id(77));
    }

    /// A set that is not the inherited one must not answer to the legacy value:
    /// otherwise every set in the pool would claim every old manifest.
    #[test]
    fn non_inherited_set_rejects_legacy_manifest_value() {
        let pool_id = PoolId::new(9);
        let set = set_with_id(pool_id, 2, EcProfile::rs_small().total_shards());

        assert!(set.accepts_manifest_set_id(2));
        assert!(!set.accepts_manifest_set_id(legacy_pool_set_id(pool_id)));
    }

    /// `FIRST_SET_ID` is never `0`, so an *additional* set can never collide with
    /// the inherited set of pool `0`, whose id is the legacy value `0`.
    #[test]
    fn first_issued_set_id_is_never_zero() {
        assert_eq!(FIRST_SET_ID, 1);
        assert_eq!(legacy_pool_set_id(PoolId::new(0)), 0);
        assert_ne!(FIRST_SET_ID, legacy_pool_set_id(PoolId::new(0)));
        assert!(ErasureSetRegistry::from_sets(
            PoolId::new(9),
            vec![
                inherited_set(PoolId::new(9)),
                set_with_id(
                    PoolId::new(9),
                    FIRST_SET_ID,
                    EcProfile::rs_small().total_shards()
                )
            ]
        )
        .is_ok());
    }

    /// A minimal erasure-coded pool.
    ///
    /// `validate_pool_from_env` reads the pool's name, id and storage class; the
    /// node roster it works from is the `(name, NodeId)` list it is given
    /// separately, which is what bootstrap passes.
    fn test_pool_config() -> PoolConfig {
        PoolConfig {
            pool_id: PoolId::new(0),
            name: "ec".to_string(),
            nodes: Vec::new(),
            replication_factor: None,
            write_quorum: None,
            read_quorum: None,
            storage_class: StorageClass::ErasureCoded,
        }
    }

    /// A pool may be configured wider than one set, and the inherited set takes
    /// the first slots' worth of nodes.
    ///
    /// This is what gives an operator somewhere to put the next set. The nodes
    /// beyond the first set are pool capacity waiting to be formed into one.
    #[test]
    fn a_pool_may_be_several_sets_wide_and_the_inherited_set_takes_the_first() {
        let _guard = PLACEMENT_ENV_LOCK.lock().unwrap_or_else(|error| error.into_inner());
        // The same profile `validate_pool_from_env` will read, so the widths
        // agree whatever the environment says.
        let width = usize::from(EcProfile::from_env().unwrap().total_shards());
        let nodes: Vec<(String, NodeId)> = (1..=width * 2)
            .map(|index| (format!("node-{index}"), NodeId(index as u128)))
            .collect();
        let pool = test_pool_config();

        let layout = validate_pool_from_env(&pool, &nodes, 3, 1).unwrap();

        assert_eq!(layout.sets.len(), 1);
        let inherited = layout.sets.inherited_set().unwrap();
        assert_eq!(
            inherited.slot_assignment.distinct_nodes(),
            nodes[..width].iter().map(|(_, node_id)| *node_id).collect::<Vec<_>>()
        );
        // The spare nodes are pool members the builder will accept for a set.
        let spare: Vec<NodeId> = nodes[width..].iter().map(|(_, node_id)| *node_id).collect();
        let second = layout.builder.build(FIRST_SET_ID, &spare, 2).unwrap();
        assert_eq!(second.slot_assignment.distinct_nodes(), spare);
        assert!(!second.inherited);
    }

    /// A pool that is not a whole number of sets wide is refused at startup.
    #[test]
    fn a_pool_that_is_not_a_whole_number_of_sets_wide_is_refused() {
        let _guard = PLACEMENT_ENV_LOCK.lock().unwrap_or_else(|error| error.into_inner());
        // The same profile `validate_pool_from_env` will read, so the widths
        // agree whatever the environment says.
        let width = usize::from(EcProfile::from_env().unwrap().total_shards());
        let nodes: Vec<(String, NodeId)> = (1..=width + 1)
            .map(|index| (format!("node-{index}"), NodeId(index as u128)))
            .collect();
        let pool = test_pool_config();

        let error = validate_pool_from_env(&pool, &nodes, 3, 1).unwrap_err().to_string();

        assert!(
            error.contains("whole number of erasure sets"),
            "the refusal must say what is wrong: {error}"
        );
    }

    /// A set may only be built on nodes the pool has.
    #[test]
    fn the_builder_refuses_a_node_outside_the_pool() {
        let _guard = PLACEMENT_ENV_LOCK.lock().unwrap_or_else(|error| error.into_inner());
        // The same profile `validate_pool_from_env` will read, so the widths
        // agree whatever the environment says.
        let width = usize::from(EcProfile::from_env().unwrap().total_shards());
        let nodes: Vec<(String, NodeId)> = (1..=width)
            .map(|index| (format!("node-{index}"), NodeId(index as u128)))
            .collect();
        let pool = test_pool_config();
        let layout = validate_pool_from_env(&pool, &nodes, 3, 1).unwrap();
        let mut outside: Vec<NodeId> = nodes.iter().map(|(_, node_id)| *node_id).collect();
        outside[0] = NodeId(9_999);

        let error = layout.builder.build(FIRST_SET_ID, &outside, 2).unwrap_err().to_string();

        assert!(
            error.contains("is not a node of this pool"),
            "the refusal must name the problem: {error}"
        );
    }

    #[test]
    fn registry_finds_set_by_id() {
        let pool_id = PoolId::new(9);
        let registry = ErasureSetRegistry::from_sets(
            pool_id,
            vec![
                inherited_set(pool_id),
                set_with_id(pool_id, 2, EcProfile::rs_small().total_shards()),
            ],
        )
        .unwrap();

        assert_eq!(registry.len(), 2);
        assert_eq!(
            registry.get(legacy_pool_set_id(pool_id)).map(|set| set.set_id),
            Some(legacy_pool_set_id(pool_id))
        );
        assert_eq!(registry.get(2).map(|set| set.set_id), Some(2));
        assert!(registry.get(3).is_none());
        assert!(registry.inherited_set().is_some());
    }

    /// Two sets with the same id in one pool would make manifest resolution
    /// ambiguous, which is exactly the breakage EC2-041 exists to prevent.
    #[test]
    fn registry_rejects_duplicate_set_ids() {
        let pool_id = PoolId::new(9);
        let width = EcProfile::rs_small().total_shards();
        let err = ErasureSetRegistry::from_sets(
            pool_id,
            vec![
                set_with_id(pool_id, 2, width),
                set_with_id(pool_id, 2, width),
            ],
        )
        .unwrap_err();

        assert!(
            err.to_string().contains("2"),
            "error must name the id: {err}"
        );
    }

    /// A registry holds the sets of one pool; a set of another pool is a
    /// configuration mistake, not something to silently adopt.
    #[test]
    fn registry_rejects_set_of_another_pool() {
        let err = ErasureSetRegistry::from_sets(
            PoolId::new(9),
            vec![set_with_id(
                PoolId::new(4),
                1,
                EcProfile::rs_small().total_shards(),
            )],
        )
        .unwrap_err();

        assert!(err.to_string().contains("pool"));
    }

    /// Only one set per pool may carry the inherited identity, or the legacy
    /// resolution rule would have two answers.
    #[test]
    fn registry_rejects_two_inherited_sets() {
        let pool_id = PoolId::new(9);
        let err = ErasureSetRegistry::from_sets(
            pool_id,
            vec![inherited_set(pool_id), {
                let mut second = set_with_id(pool_id, 2, EcProfile::rs_small().total_shards());
                second.inherited = true;
                second
            }],
        )
        .unwrap_err();

        assert!(err.to_string().contains("inherited"));
    }

    #[test]
    fn registry_lists_active_sets_only() {
        let pool_id = PoolId::new(9);
        let width = EcProfile::rs_small().total_shards();
        let mut draining = set_with_id(pool_id, 2, width);
        draining.state = ErasureSetState::Draining;
        let mut read_only = set_with_id(pool_id, 3, width);
        read_only.state = ErasureSetState::ReadOnly;
        let mut retired = set_with_id(pool_id, 4, width);
        retired.state = ErasureSetState::Retired;

        let registry = ErasureSetRegistry::from_sets(
            pool_id,
            vec![inherited_set(pool_id), draining, read_only, retired],
        )
        .unwrap();

        assert_eq!(registry.len(), 4);
        let active: Vec<u64> = registry.active_sets().iter().map(|set| set.set_id).collect();
        assert_eq!(active, vec![legacy_pool_set_id(pool_id)]);
    }

    /// The rule from EC2-041: a manifest written before the decoupling carries
    /// the pool id and must resolve to the inherited set.
    #[test]
    fn legacy_manifest_resolves_to_inherited_set() {
        let pool_id = PoolId::new(9);
        let registry = ErasureSetRegistry::from_sets(
            pool_id,
            vec![
                inherited_set(pool_id),
                set_with_id(pool_id, 2, EcProfile::rs_small().total_shards()),
            ],
        )
        .unwrap();

        let resolved = registry
            .resolve_manifest_set(legacy_pool_set_id(pool_id))
            .expect("a manifest written by EC v1 must still resolve");
        assert_eq!(resolved.set_id, legacy_pool_set_id(pool_id));
        assert!(resolved.inherited);
    }

    /// Stronger than the old "explicit id wins" tie-break: a set explicitly
    /// numbered onto the pool's EC v1 value cannot even be registered next to
    /// the inherited set, which already carries that id. The ambiguity EC2-041
    /// worried about is prevented by construction.
    #[test]
    fn registry_rejects_an_explicit_set_that_collides_with_the_inherited_value() {
        let pool_id = PoolId::new(9);
        let width = EcProfile::rs_small().total_shards();
        let err = ErasureSetRegistry::from_sets(
            pool_id,
            vec![
                inherited_set(pool_id),
                set_with_id(pool_id, legacy_pool_set_id(pool_id), width),
            ],
        )
        .unwrap_err();
        assert!(
            err.to_string().contains(&legacy_pool_set_id(pool_id).to_string()),
            "error must name the id: {err}"
        );
    }

    /// Regression guard for EC2-041. A v2 binary must stamp into a manifest exactly
    /// the `erasure_set_id` an EC v1 binary would, for any object in the inherited
    /// set — otherwise a rollback to an EC v1 binary, or a v1 node in a mixed
    /// cluster, rejects the object in `validate_against_erasure_set`. In production
    /// the pool id is always `PoolId::new(0)` (see `s4-server/src/cluster_bootstrap.rs`),
    /// so the EC v1 value is `0`.
    #[test]
    fn inherited_set_manifest_value_is_wire_compatible_with_ec_v1() {
        let pool_id = PoolId::new(0);
        let profile = EcProfile::rs_small();
        let set_nodes = nodes(profile.total_shards());
        let set = ErasureSet::inherited(
            pool_id,
            profile,
            &set_nodes,
            &HashMap::new(),
            EcTopologyPolicy::Strict,
            1,
            1,
        )
        .unwrap();

        // The value the transcode path writes into `EcObjectManifest::erasure_set_id`.
        assert_eq!(set.set_id, legacy_pool_set_id(pool_id));
        assert_eq!(set.set_id, u64::from(pool_id.as_u32()));
        assert_eq!(set.set_id, 0);
        assert!(set.inherited);

        // A v2 registry still resolves that value to the inherited set.
        let registry = ErasureSetRegistry::from_sets(pool_id, vec![set]).unwrap();
        let resolved = registry
            .resolve_manifest_set(legacy_pool_set_id(pool_id))
            .expect("the EC v1 value must resolve to the inherited set");
        assert!(resolved.inherited);
        assert_eq!(resolved.set_id, legacy_pool_set_id(pool_id));
    }

    /// Step 3 of the rule: an id that is neither issued nor the pool's legacy
    /// value is rejected rather than quietly served from the inherited set.
    #[test]
    fn unknown_manifest_set_id_is_rejected() {
        let pool_id = PoolId::new(9);
        let registry =
            ErasureSetRegistry::from_sets(pool_id, vec![inherited_set(pool_id)]).unwrap();

        assert!(registry.resolve_manifest_set(77).is_none());
    }

    /// A node may hold slots in more than one set of the same pool. Within one
    /// set its slots stay unique, which `ensure_distinct_nodes` already guards.
    #[test]
    fn node_may_hold_slots_in_two_sets() {
        let pool_id = PoolId::new(9);
        let width = EcProfile::rs_small().total_shards();
        // Both sets are built over the same nodes.
        let first = set_with_id(pool_id, 1, width);
        let second = set_with_id(pool_id, 2, width);

        let shared = first.slot_assignment.distinct_nodes();
        assert_eq!(shared, second.slot_assignment.distinct_nodes());

        let registry = ErasureSetRegistry::from_sets(pool_id, vec![first, second]).unwrap();
        assert_eq!(registry.len(), 2);

        // Slots stay unique inside each set.
        for set in registry.sets() {
            let slots = set.slot_assignment.slots();
            let unique =
                slots.iter().map(|slot| (slot.node_id, slot.slot_index)).collect::<HashSet<_>>();
            assert_eq!(unique.len(), slots.len());
        }
    }

    // -----------------------------------------------------------------------
    // Topology rule, failure domains, recorded layouts (plan 05, phase 1)
    // -----------------------------------------------------------------------

    /// Holds the placement lock and clears every topology variable, before and
    /// after. The variables are process-wide, and the tests that read them take
    /// the same lock.
    struct TopologyEnvGuard {
        _lock: std::sync::MutexGuard<'static, ()>,
    }

    impl TopologyEnvGuard {
        const VARIABLES: [&'static str; 4] = [
            LRC_GROUP_DOMAIN_ENV,
            "S4_EC_TOPOLOGY_POLICY",
            "S4_EC_PROFILE",
            "S4_EC_NODE_TOPOLOGY",
        ];

        fn acquire() -> Self {
            let lock = PLACEMENT_ENV_LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            Self::clear();
            Self { _lock: lock }
        }

        fn clear() {
            for name in Self::VARIABLES {
                std::env::remove_var(name);
            }
        }
    }

    impl Drop for TopologyEnvGuard {
        fn drop(&mut self) {
            Self::clear();
        }
    }

    fn labels(zone: &str, rack: &str, host: &str) -> TopologyLabels {
        TopologyLabels {
            zone: Some(zone.to_string()),
            rack: Some(rack.to_string()),
            host: Some(host.to_string()),
            disk_group: Some("d0".to_string()),
        }
    }

    /// Complete labels filling `racks` racks of one zone with consecutive
    /// nodes, one host per node.
    ///
    /// Consecutive rather than round-robin on purpose: round-robin labels make
    /// the EC v1 spread come out in `NodeId` order, the same order an
    /// unlabeled pool gets, and a test of a label change would move nothing.
    fn racked_labels(nodes: &[NodeId], racks: usize) -> HashMap<NodeId, TopologyLabels> {
        nodes
            .iter()
            .enumerate()
            .map(|(idx, node)| {
                (
                    *node,
                    labels(
                        "z1",
                        &format!("r{}", idx * racks / nodes.len()),
                        &format!("h{idx}"),
                    ),
                )
            })
            .collect()
    }

    fn all_profiles() -> [EcProfile; 6] {
        [
            EcProfile::rs_small(),
            EcProfile::rs_standard(),
            EcProfile::rs_dense(),
            EcProfile::lrc_small(),
            EcProfile::lrc_standard(),
            EcProfile::lrc_dense(),
        ]
    }

    /// Without the variable the rule is the EC v1 one: no level, and the
    /// policy parsed exactly as before.
    #[test]
    fn the_lrc_group_domain_defaults_to_off() {
        let _guard = TopologyEnvGuard::acquire();

        let rule = EcTopologyRule::from_env().unwrap();

        assert_eq!(rule, EcTopologyRule::v1(EcTopologyPolicy::Strict));
        assert_eq!(rule.lrc_group_domain, None);
    }

    #[test]
    fn the_lrc_group_domain_accepts_every_level_and_off() {
        let _guard = TopologyEnvGuard::acquire();

        for (raw, expected) in [
            ("off", None),
            ("OFF", None),
            ("zone", Some(EcFailureDomainLevel::Zone)),
            (" rack ", Some(EcFailureDomainLevel::Rack)),
            ("Rack", Some(EcFailureDomainLevel::Rack)),
            ("host", Some(EcFailureDomainLevel::Host)),
        ] {
            std::env::set_var(LRC_GROUP_DOMAIN_ENV, raw);
            assert_eq!(
                EcTopologyRule::from_env().unwrap().lrc_group_domain,
                expected,
                "value '{raw}'"
            );
        }
    }

    /// An unknown level stops the start and names the variable, and an empty
    /// value is not silently read as "off".
    #[test]
    fn an_unknown_lrc_group_domain_is_refused_by_name() {
        let _guard = TopologyEnvGuard::acquire();

        for raw in ["disk_group", "racks", ""] {
            std::env::set_var(LRC_GROUP_DOMAIN_ENV, raw);
            let error = EcTopologyRule::from_env().unwrap_err();
            assert!(
                matches!(
                    error,
                    EcConfigError::Env {
                        name: LRC_GROUP_DOMAIN_ENV,
                        ..
                    }
                ),
                "value '{raw}' gave {error:?}"
            );
            assert!(error.to_string().contains(LRC_GROUP_DOMAIN_ENV), "{error}");
            assert!(
                error.to_string().contains("off, host, rack or zone"),
                "{error}"
            );
        }
    }

    /// The node refuses to start over an unknown level: the variable is parsed
    /// on the startup path, not only when a set is created.
    #[test]
    fn an_unknown_lrc_group_domain_stops_the_pool_from_starting() {
        let _guard = TopologyEnvGuard::acquire();
        std::env::set_var(LRC_GROUP_DOMAIN_ENV, "datacenter");
        let width = usize::from(EcProfile::from_env().unwrap().total_shards());
        let nodes: Vec<(String, NodeId)> = (1..=width)
            .map(|index| (format!("node-{index}"), NodeId(index as u128)))
            .collect();

        let error = validate_pool_from_env(&test_pool_config(), &nodes, 3, 1).unwrap_err();

        assert!(error.to_string().contains(LRC_GROUP_DOMAIN_ENV), "{error}");
    }

    /// `S4_EC_TOPOLOGY_POLICY` is parsed exactly as in EC v1, next to the new
    /// variable and independently of it.
    #[test]
    fn the_topology_policy_parse_is_unchanged() {
        let _guard = TopologyEnvGuard::acquire();
        std::env::set_var(LRC_GROUP_DOMAIN_ENV, "rack");

        assert_eq!(
            EcTopologyPolicy::from_env().unwrap(),
            EcTopologyPolicy::Strict
        );
        std::env::set_var("S4_EC_TOPOLOGY_POLICY", "warn");
        assert_eq!(
            EcTopologyRule::from_env().unwrap().policy,
            EcTopologyPolicy::Warn
        );
        std::env::set_var("S4_EC_TOPOLOGY_POLICY", "strict");
        assert_eq!(
            EcTopologyRule::from_env().unwrap().policy,
            EcTopologyPolicy::Strict
        );
        // Still an exact, case-sensitive match, as it always was.
        std::env::set_var("S4_EC_TOPOLOGY_POLICY", "Warn");
        assert!(EcTopologyPolicy::from_env().is_err());
        assert!(EcTopologyRule::from_env().is_err());
    }

    /// The level lays out LRC local groups; an RS profile has none, so it keeps
    /// the EC v1 rule at every level.
    #[test]
    fn the_level_only_changes_the_rule_of_an_lrc_profile() {
        for profile in all_profiles() {
            assert_eq!(
                EcTopologyRule::v1(EcTopologyPolicy::Strict).layout_rule_for(&profile),
                EcSetLayoutRule::V1
            );
            for level in [
                EcFailureDomainLevel::Zone,
                EcFailureDomainLevel::Rack,
                EcFailureDomainLevel::Host,
            ] {
                let rule = EcTopologyRule {
                    policy: EcTopologyPolicy::Warn,
                    lrc_group_domain: Some(level),
                };
                let expected = match profile.codec {
                    EcCodecKind::ReedSolomon => EcSetLayoutRule::V1,
                    EcCodecKind::LocallyRepairable => EcSetLayoutRule::LrcGroups { level },
                };
                assert_eq!(rule.layout_rule_for(&profile), expected, "{}", profile.name);
            }
        }
    }

    /// A domain is named by every label above it: rack `r1` of zone `a` and
    /// rack `r1` of zone `b` are two racks (EC2-081, point 1).
    #[test]
    fn a_failure_domain_is_named_by_every_label_above_it() {
        let a = labels("a", "r1", "h1");
        let b = labels("b", "r1", "h1");

        let rack_a = EcFailureDomainLevel::Rack.domain_of(&a).unwrap();
        let rack_b = EcFailureDomainLevel::Rack.domain_of(&b).unwrap();
        assert_ne!(rack_a, rack_b);
        assert_eq!(rack_a.to_string(), "a/r1");
        assert_eq!(rack_a.labels(), ["a".to_string(), "r1".to_string()]);
        assert_eq!(rack_a.zone(), "a");
        assert_eq!(rack_a.level(), EcFailureDomainLevel::Rack);

        let host = EcFailureDomainLevel::Host.domain_of(&a).unwrap();
        assert_eq!(host.to_string(), "a/r1/h1");
        // Labels below the level do not count: two hosts of one rack share it.
        let other_host = labels("a", "r1", "h2");
        assert_eq!(
            EcFailureDomainLevel::Rack.domain_of(&other_host),
            Some(rack_a.clone())
        );
        assert_eq!(
            EcFailureDomainLevel::Zone.domain_of(&b).unwrap().to_string(),
            "b"
        );
        // `disk_group` is not a failure domain and not needed for one.
        let mut no_disk_group = a.clone();
        no_disk_group.disk_group = None;
        assert_eq!(
            EcFailureDomainLevel::Host.domain_of(&no_disk_group),
            Some(host)
        );
    }

    /// A node without a label its level needs has no domain at that level, but
    /// still has one at the levels above.
    #[test]
    fn a_missing_label_leaves_a_node_without_a_domain_at_that_level() {
        let no_rack = TopologyLabels {
            zone: Some("a".to_string()),
            rack: None,
            host: Some("h1".to_string()),
            disk_group: Some("d0".to_string()),
        };

        assert!(EcFailureDomainLevel::Zone.domain_of(&no_rack).is_some());
        assert!(EcFailureDomainLevel::Rack.domain_of(&no_rack).is_none());
        assert!(EcFailureDomainLevel::Host.domain_of(&no_rack).is_none());
        assert!(EcFailureDomainLevel::Zone.domain_of(&TopologyLabels::default()).is_none());
    }

    /// The index answers per node, groups nodes by domain whatever order they
    /// come in, and names the nodes it has no domain for.
    #[test]
    fn the_failure_domain_index_finds_each_node_and_names_the_unlabeled() {
        let node_ids = nodes(5);
        let mut labels_by_node = HashMap::new();
        labels_by_node.insert(node_ids[0], labels("a", "r1", "h0"));
        labels_by_node.insert(node_ids[1], labels("b", "r1", "h1"));
        labels_by_node.insert(node_ids[2], labels("a", "r1", "h2"));
        labels_by_node.insert(node_ids[3], labels("a", "r2", "h3"));
        // node_ids[4] carries no labels.

        let index = FailureDomainIndex::build(
            EcFailureDomainLevel::Rack,
            node_ids.iter().rev().copied(),
            &labels_by_node,
        );

        assert_eq!(index.level(), EcFailureDomainLevel::Rack);
        assert_eq!(index.domain_of(node_ids[1]).unwrap().to_string(), "b/r1");
        assert!(index.domain_of(node_ids[4]).is_none());
        assert_eq!(index.unplaced_nodes(), [node_ids[4]]);

        let forward = index.group_by_domain(&node_ids);
        let mut reversed = node_ids.clone();
        reversed.reverse();
        assert_eq!(forward, index.group_by_domain(&reversed));
        let grouped = forward
            .iter()
            .map(|(domain, members)| (domain.to_string(), members.clone()))
            .collect::<Vec<_>>();
        assert_eq!(
            grouped,
            vec![
                ("a/r1".to_string(), vec![node_ids[0], node_ids[2]]),
                ("a/r2".to_string(), vec![node_ids[3]]),
                ("b/r1".to_string(), vec![node_ids[1]]),
            ]
        );
    }

    /// The builder indexes every node of its pool, labeled or not.
    #[test]
    fn the_builder_indexes_every_node_of_its_pool() {
        let profile = EcProfile::lrc_standard();
        let pool_nodes = nodes(profile.total_shards() + 1);
        let mut labels_by_node = racked_labels(&pool_nodes[..10], 4);
        labels_by_node.remove(&pool_nodes[3]);
        let builder = ErasureSetBuilder::new(
            PoolId::new(1),
            "ec",
            profile,
            pool_nodes.clone(),
            labels_by_node,
            EcTopologyPolicy::Warn,
            1,
        );

        let index = builder.failure_domain_index(EcFailureDomainLevel::Rack);

        assert_eq!(index.unplaced_nodes(), [pool_nodes[3], pool_nodes[10]]);
        assert_eq!(index.domain_of(pool_nodes[0]).unwrap().to_string(), "z1/r0");
    }

    /// Distance goes local, same rack, same zone, the rest — and racks are
    /// named with their zone, so rack `r1` of another zone is remote. A node
    /// without labels is remote too, and nobody can say whether bytes to it
    /// cross a zone.
    #[test]
    fn the_topology_view_ranks_nodes_by_rack_then_zone() {
        let node_ids = nodes(6);
        let mut labels_by_node = HashMap::new();
        labels_by_node.insert(node_ids[0], labels("a", "r1", "h0"));
        labels_by_node.insert(node_ids[1], labels("a", "r1", "h1"));
        labels_by_node.insert(node_ids[2], labels("a", "r2", "h2"));
        labels_by_node.insert(node_ids[3], labels("b", "r1", "h3"));
        labels_by_node.insert(node_ids[5], labels("b", "r2", "h5"));
        // node_ids[4] carries no labels.

        let view = PoolTopologyView::build(node_ids[0], &node_ids, &labels_by_node);

        let distances = node_ids.iter().map(|node| view.distance_to(*node)).collect::<Vec<_>>();
        assert_eq!(
            distances,
            [
                NodeDistance::Local,
                NodeDistance::SameRack,
                NodeDistance::SameZone,
                NodeDistance::Remote,
                NodeDistance::Remote,
                NodeDistance::Remote,
            ]
        );
        assert_eq!(view.zone_of(node_ids[3]).unwrap().to_string(), "b");
        assert_eq!(view.same_zone(node_ids[0], node_ids[2]), Some(true));
        assert_eq!(view.same_zone(node_ids[3], node_ids[5]), Some(true));
        assert_eq!(view.same_zone(node_ids[2], node_ids[3]), Some(false));
        assert_eq!(view.same_zone(node_ids[0], node_ids[4]), None);

        // A node that knows no labels still knows itself.
        let blind = PoolTopologyView::unlabeled(node_ids[1]);
        assert_eq!(blind.distance_to(node_ids[1]), NodeDistance::Local);
        assert_eq!(blind.distance_to(node_ids[0]), NodeDistance::Remote);
        assert_eq!(blind.same_zone(node_ids[1], node_ids[1]), None);
    }

    /// `from_slot_order` derives every slot's role and local group exactly as
    /// `build` does, on every standard profile, labeled or not. This is what
    /// lets `build` stay untouched while a recorded order is rebuilt apart
    /// from it.
    #[test]
    fn a_slot_order_rebuilds_exactly_the_assignment_build_made() {
        for profile in all_profiles() {
            let set_nodes = nodes(profile.total_shards());
            for labels_by_node in [HashMap::new(), racked_labels(&set_nodes, 3)] {
                let built =
                    ShardSlotAssignment::build(&profile, &set_nodes, &labels_by_node).unwrap();
                let rebuilt =
                    ShardSlotAssignment::from_slot_order(&profile, &built.slot_order()).unwrap();
                assert_eq!(rebuilt, built, "{}", profile.name);
            }
        }
    }

    #[test]
    fn a_slot_order_must_fit_the_profile_and_name_distinct_nodes() {
        let profile = EcProfile::lrc_small();
        let set_nodes = nodes(profile.total_shards());

        let short = ShardSlotAssignment::from_slot_order(&profile, &set_nodes[1..]).unwrap_err();
        assert!(short.to_string().contains("exactly 7 node(s)"), "{short}");

        let mut repeated = set_nodes.clone();
        repeated[6] = repeated[0];
        let error = ShardSlotAssignment::from_slot_order(&profile, &repeated).unwrap_err();
        assert!(error.to_string().contains("distinct NodeIds"), "{error}");
    }

    fn lrc_builder(labels_by_node: HashMap<NodeId, TopologyLabels>) -> ErasureSetBuilder {
        let profile = EcProfile::lrc_standard();
        let pool_nodes = nodes(profile.total_shards());
        ErasureSetBuilder::new(
            PoolId::new(1),
            "ec",
            profile,
            pool_nodes,
            labels_by_node,
            EcTopologyPolicy::Warn,
            1,
        )
    }

    /// The recorded order is taken verbatim, whatever the labels of the node
    /// restoring it would give — which is the whole point of recording it
    /// (EC2-086, point 2).
    #[test]
    fn a_recorded_layout_is_taken_verbatim_whatever_the_labels_say() {
        let set_nodes = nodes(10);
        let before = lrc_builder(HashMap::new());
        let after = lrc_builder(racked_labels(&set_nodes, 4));
        let set_before = before.build(FIRST_SET_ID, &set_nodes, 1).unwrap();
        let set_after = after.build(FIRST_SET_ID, &set_nodes, 1).unwrap();
        // The label change really does move slots when nothing is recorded.
        assert_ne!(
            set_before.slot_assignment.slot_order(),
            set_after.slot_assignment.slot_order()
        );

        let recorded = set_before.slot_assignment.slot_order();
        let restored =
            after.with_recorded_layout(&set_after, EcSetLayoutRule::V1, &recorded).unwrap();

        assert_eq!(restored.slot_assignment.slot_order(), recorded);
        assert_eq!(restored.slot_assignment, set_before.slot_assignment);
        assert_eq!(restored.layout_rule, EcSetLayoutRule::V1);
        assert_eq!(restored.set_id, set_after.set_id);
    }

    /// A restored set is judged on its recorded order, not on the order the
    /// labels would give: a local group the record puts into one rack is
    /// reported although the labels alone would have spread it.
    #[test]
    fn a_restored_set_is_judged_on_its_recorded_order() {
        // Rack r0 holds nodes 1-4, r1 nodes 5-7, r2 nodes 8-10.
        let rack_of = |node: u128| match node {
            1..=4 => "r0",
            5..=7 => "r1",
            _ => "r2",
        };
        let set_nodes = nodes(10);
        let labels_by_node = set_nodes
            .iter()
            .map(|node| {
                (
                    *node,
                    labels("z1", rack_of(node.0), &format!("h{}", node.0)),
                )
            })
            .collect();
        let builder = lrc_builder(labels_by_node);
        let set = builder.build(FIRST_SET_ID, &set_nodes, 1).unwrap();
        let one_rack_risk = |set: &ErasureSet| {
            set.topology_risks.iter().any(|risk| risk.contains("entirely placed"))
        };
        // Round-robin over the racks never puts a whole group in one of them.
        assert!(!one_rack_risk(&set), "{:?}", set.topology_risks);

        // Local group 0 is data slots 0-2 plus local parity slot 6. This order
        // gives it nodes 1, 2, 3 and 4 — all of rack r0.
        let order = [1u128, 2, 3, 5, 8, 6, 4, 9, 7, 10].into_iter().map(NodeId).collect::<Vec<_>>();
        let restored = builder.with_recorded_layout(&set, EcSetLayoutRule::V1, &order).unwrap();

        assert!(one_rack_risk(&restored), "{:?}", restored.topology_risks);
        assert_eq!(restored.topology_health, EcTopologyHealth::TopologyDegraded);
    }

    /// A record naming a node the set does not have describes another set and
    /// is refused, as is one that drops a node.
    #[test]
    fn a_recorded_layout_may_only_reorder_the_sets_own_nodes() {
        let set_nodes = nodes(10);
        let builder = lrc_builder(HashMap::new());
        let set = builder.build(FIRST_SET_ID, &set_nodes, 1).unwrap();

        let mut foreign = set.slot_assignment.slot_order();
        foreign[4] = NodeId(9_999);
        let error = builder.with_recorded_layout(&set, EcSetLayoutRule::V1, &foreign).unwrap_err();
        assert!(
            error.to_string().contains("may only reorder a set's own nodes"),
            "{error}"
        );

        let short = &set.slot_assignment.slot_order()[..9];
        assert!(builder.with_recorded_layout(&set, EcSetLayoutRule::V1, short).is_err());
    }

    /// `holds_node` is about membership, not about one slot.
    #[test]
    fn a_set_holds_exactly_its_own_nodes() {
        let set_nodes = nodes(10);
        let set = lrc_builder(HashMap::new()).build(FIRST_SET_ID, &set_nodes, 1).unwrap();

        for node_id in &set_nodes {
            assert!(set.holds_node(*node_id));
        }
        assert!(!set.holds_node(NodeId(9_999)));
    }

    /// Switching the mode on changes nothing about a set that already exists:
    /// the inherited set, and a set the roster names, are both laid out and
    /// judged exactly as with the mode off (`EC2-ZONE-01-005`).
    #[test]
    fn existing_sets_keep_the_v1_layout_and_report_with_the_mode_on() {
        let _guard = TopologyEnvGuard::acquire();
        let profile = EcProfile::lrc_standard();
        let width = usize::from(profile.total_shards());
        let nodes: Vec<(String, NodeId)> = (1..=width * 2)
            .map(|index| (format!("node-{index}"), NodeId(index as u128)))
            .collect();
        // Five racks of two nodes: the EC v1 layout puts two members of a local
        // group into one rack here (EC2-080), so the mode has something it
        // would change.
        let topology = nodes
            .iter()
            .enumerate()
            .map(|(idx, (name, _))| {
                format!(
                    "{name}:zone=z1,rack=r{},host=h{idx},disk_group=d0",
                    (idx % width) % 5
                )
            })
            .collect::<Vec<_>>()
            .join(";");
        std::env::set_var("S4_EC_PROFILE", &profile.name);
        std::env::set_var("S4_EC_NODE_TOPOLOGY", &topology);
        std::env::set_var("S4_EC_TOPOLOGY_POLICY", "warn");

        let off = validate_pool_from_env(&test_pool_config(), &nodes, 3, 1).unwrap();
        std::env::set_var(LRC_GROUP_DOMAIN_ENV, "rack");
        let on = validate_pool_from_env(&test_pool_config(), &nodes, 3, 1).unwrap();

        let inherited_off = off.sets.inherited_set().unwrap();
        let inherited_on = on.sets.inherited_set().unwrap();
        assert_eq!(inherited_on, inherited_off);
        assert_eq!(inherited_on.layout_rule, EcSetLayoutRule::V1);
        assert_eq!(
            on.builder.rule().lrc_group_domain,
            Some(EcFailureDomainLevel::Rack)
        );

        let spare: Vec<NodeId> = nodes[width..].iter().map(|(_, node_id)| *node_id).collect();
        assert_eq!(
            on.builder.build(FIRST_SET_ID, &spare, 2).unwrap(),
            off.builder.build(FIRST_SET_ID, &spare, 2).unwrap()
        );
        // Only a set created now is laid out by the rule.
        assert_eq!(
            on.builder.layout_rule_for_new_set(),
            EcSetLayoutRule::LrcGroups {
                level: EcFailureDomainLevel::Rack
            }
        );
        let created = on.builder.build_new(FIRST_SET_ID, &spare, 2).unwrap();
        assert_eq!(
            created.set.layout_rule,
            EcSetLayoutRule::LrcGroups {
                level: EcFailureDomainLevel::Rack
            }
        );
        let off_created = off.builder.build_new(FIRST_SET_ID, &spare, 2).unwrap();
        assert_eq!(
            off_created.set,
            off.builder.build(FIRST_SET_ID, &spare, 2).unwrap()
        );
        assert_eq!(off_created.violation, None);
        assert_eq!(
            off_created.algorithm_version,
            EC_V1_LAYOUT_ALGORITHM_VERSION
        );
    }

    // -----------------------------------------------------------------------
    // Layout by the LRC group rule (plan 05, phase 2)
    // -----------------------------------------------------------------------

    /// Complete labels from `(zone, rack)` per node, nodes numbered from 1,
    /// one host each.
    fn labels_from(racks: &[(&str, &str)]) -> HashMap<NodeId, TopologyLabels> {
        racks
            .iter()
            .enumerate()
            .map(|(idx, (zone, rack))| {
                (
                    NodeId(idx as u128 + 1),
                    labels(zone, rack, &format!("h{}", idx + 1)),
                )
            })
            .collect()
    }

    /// A builder for a pool exactly one set of `profile` wide.
    fn rule_builder(
        profile: EcProfile,
        labels_by_node: HashMap<NodeId, TopologyLabels>,
        policy: EcTopologyPolicy,
        level: Option<EcFailureDomainLevel>,
    ) -> ErasureSetBuilder {
        let pool_nodes = nodes(profile.total_shards());
        ErasureSetBuilder::new(
            PoolId::new(1),
            "ec",
            profile,
            pool_nodes,
            labels_by_node,
            policy,
            1,
        )
        .with_lrc_group_domain(level)
    }

    const RACK: Option<EcFailureDomainLevel> = Some(EcFailureDomainLevel::Rack);

    /// Five racks of two nodes: `lrc_standard` is satisfiable here, so a set
    /// laid out by the group rule keeps every group in four distinct racks.
    fn five_racks_of_two() -> HashMap<NodeId, TopologyLabels> {
        labels_from(&[
            ("z1", "r1"),
            ("z1", "r2"),
            ("z1", "r3"),
            ("z1", "r4"),
            ("z1", "r5"),
            ("z1", "r1"),
            ("z1", "r2"),
            ("z1", "r3"),
            ("z1", "r4"),
            ("z1", "r5"),
        ])
    }

    /// A set laid out by the EC v1 rule promises nothing about placement, so it
    /// can never be a violator — whatever the labels say and whatever the mode
    /// is set to. This is the promise of EC2-084, point 1, and the phase 4 exit
    /// gate: switching `S4_EC_LRC_GROUP_DOMAIN` on must not turn a pool's
    /// existing sets into violators.
    #[test]
    fn an_ec_v1_set_never_violates_a_rule_it_was_not_laid_out_by() {
        // Every node in one rack — the worst placement the group rule knows.
        let builder = rule_builder(
            EcProfile::lrc_standard(),
            labels_from(&[("z1", "r1"); 10]),
            EcTopologyPolicy::Warn,
            RACK,
        );
        let set = builder.build(FIRST_SET_ID, &nodes(10), 1).unwrap();

        assert_eq!(set.layout_rule, EcSetLayoutRule::V1);
        assert_eq!(
            builder.assess_set_layout(&set, false),
            SetLayoutCompliance::NoGuarantee
        );
    }

    /// A set laid out by the group rule meets it; once the labels move its
    /// nodes together it does not. The set was laid out correctly and the
    /// labels changed under it, which is exactly drift (EC2-086, point 3).
    #[test]
    fn a_label_change_that_puts_group_members_in_one_rack_is_drift() {
        let builder = rule_builder(
            EcProfile::lrc_standard(),
            five_racks_of_two(),
            EcTopologyPolicy::Strict,
            RACK,
        );
        let created = builder.build_new(FIRST_SET_ID, &nodes(10), 1).unwrap();
        assert!(created.violation.is_none());
        assert_eq!(
            builder.assess_set_layout(&created.set, false),
            SetLayoutCompliance::Meets
        );

        // The same set, seen by a node whose labels put everything in one rack.
        let moved = rule_builder(
            EcProfile::lrc_standard(),
            labels_from(&[("z1", "r1"); 10]),
            EcTopologyPolicy::Strict,
            RACK,
        );
        let compliance = moved.assess_set_layout(&created.set, false);

        assert_eq!(
            compliance.violation_origin(),
            Some(LayoutViolationOrigin::Drift)
        );
        let SetLayoutCompliance::Violated {
            coincidences,
            detail,
            ..
        } = &compliance
        else {
            panic!("expected a violation, got {compliance:?}");
        };
        // Both groups are four members in one rack: three coincidences each.
        assert_eq!(*coincidences, 6);
        assert!(detail.contains("local group 0"), "{detail}");
        assert!(detail.contains("z1/r1"), "{detail}");
    }

    /// A set created in breach of its rule under `warn` is a violator from
    /// birth, not a drifted one: its layout record says so, and the two need
    /// different actions from an operator (EC2-083).
    #[test]
    fn a_set_created_in_breach_is_reported_as_such_and_not_as_drift() {
        let builder = rule_builder(
            EcProfile::lrc_standard(),
            labels_from(&[("z1", "r1"); 10]),
            EcTopologyPolicy::Warn,
            RACK,
        );
        let created = builder.build_new(FIRST_SET_ID, &nodes(10), 1).unwrap();
        assert!(created.violation.is_some());

        assert_eq!(
            builder.assess_set_layout(&created.set, true).violation_origin(),
            Some(LayoutViolationOrigin::Creation)
        );
    }

    /// Incomplete labels leave the rule unjudgeable. That is not a violation:
    /// an unknown answer must not be counted as a known breach.
    #[test]
    fn a_rule_cannot_be_judged_without_complete_labels() {
        let laid_out = rule_builder(
            EcProfile::lrc_standard(),
            five_racks_of_two(),
            EcTopologyPolicy::Strict,
            RACK,
        );
        let created = laid_out.build_new(FIRST_SET_ID, &nodes(10), 1).unwrap();

        let unlabeled = rule_builder(
            EcProfile::lrc_standard(),
            HashMap::new(),
            EcTopologyPolicy::Warn,
            RACK,
        );
        let compliance = unlabeled.assess_set_layout(&created.set, false);

        assert!(
            matches!(compliance, SetLayoutCompliance::NotJudgeable { .. }),
            "{compliance:?}"
        );
        assert_eq!(compliance.violation_origin(), None);
    }

    /// The placement says where every slot sits and groups the set's slots by
    /// the level of the set's own rule, naming each domain by every label above
    /// it (`EC2-ZONE-04-001`, EC2-081 point 1).
    #[test]
    fn the_placement_groups_a_sets_slots_by_the_level_of_its_own_rule() {
        let builder = rule_builder(
            EcProfile::lrc_standard(),
            five_racks_of_two(),
            EcTopologyPolicy::Strict,
            RACK,
        );
        let created = builder.build_new(FIRST_SET_ID, &nodes(10), 1).unwrap();

        let placement = builder.domain_placement(&created.set);

        assert_eq!(placement.level, EcFailureDomainLevel::Rack);
        assert_eq!(placement.slots.len(), 10);
        assert!(placement.unlabeled_nodes.is_empty());
        assert_eq!(placement.domains.len(), 5);
        assert!(placement.domains.iter().all(|domain| domain.slots.len() == 2));
        // A rack is named by its zone too, so `r1` of two zones is two racks.
        assert!(placement.domains.iter().all(|domain| domain.domain.starts_with("z1/")));
        // The rule holds here, so no rack holds two members of one group.
        assert!(placement
            .domains
            .iter()
            .all(|domain| domain.local_groups.iter().all(|group| group.members == 1)));
        // Every slot carries all three levels of its node.
        assert!(placement
            .slots
            .iter()
            .all(|slot| slot.zone.is_some() && slot.rack.is_some() && slot.host.is_some()));
    }

    /// A set whose nodes carry no labels has no domains to group by, and the
    /// nodes are named rather than silently dropped.
    #[test]
    fn the_placement_names_the_nodes_it_cannot_place() {
        let builder = rule_builder(
            EcProfile::lrc_standard(),
            HashMap::new(),
            EcTopologyPolicy::Warn,
            RACK,
        );
        let set = builder.build(FIRST_SET_ID, &nodes(10), 1).unwrap();

        let placement = builder.domain_placement(&set);

        assert!(placement.domains.is_empty());
        assert_eq!(placement.unlabeled_nodes.len(), 10);
        assert!(placement.slots.iter().all(|slot| slot.rack.is_none()));
    }

    /// With the mode off, a new set is the EC v1 set to the last slot, on every
    /// standard profile and whatever the labels; an RS profile is the EC v1
    /// set at every level too (`EC2-ZONE-02-005`).
    #[test]
    fn with_the_mode_off_a_new_set_is_the_ec_v1_set_to_the_last_slot() {
        for profile in all_profiles() {
            let set_nodes = nodes(profile.total_shards());
            for labels_by_node in [
                HashMap::new(),
                racked_labels(&set_nodes, 3),
                racked_labels(&set_nodes, 5),
                complete_labels(&set_nodes),
            ] {
                let levels: Vec<Option<EcFailureDomainLevel>> = match profile.codec {
                    EcCodecKind::ReedSolomon => vec![
                        None,
                        Some(EcFailureDomainLevel::Zone),
                        Some(EcFailureDomainLevel::Rack),
                        Some(EcFailureDomainLevel::Host),
                    ],
                    EcCodecKind::LocallyRepairable => vec![None],
                };
                for level in levels {
                    let builder = rule_builder(
                        profile.clone(),
                        labels_by_node.clone(),
                        EcTopologyPolicy::Warn,
                        level,
                    );
                    let created = builder.build_new(FIRST_SET_ID, &set_nodes, 2).unwrap();
                    let v1 =
                        ShardSlotAssignment::build(&profile, &set_nodes, &labels_by_node).unwrap();
                    assert_eq!(
                        created.set.slot_assignment, v1,
                        "{} {level:?}",
                        profile.name
                    );
                    assert_eq!(
                        created.set,
                        builder.build(FIRST_SET_ID, &set_nodes, 2).unwrap()
                    );
                    assert_eq!(created.set.layout_rule, EcSetLayoutRule::V1);
                    assert_eq!(created.violation, None);
                    assert_eq!(created.algorithm_version, EC_V1_LAYOUT_ALGORITHM_VERSION);
                }
            }
        }
    }

    /// With the mode on, a new LRC set keeps every group in distinct racks,
    /// is recorded as laid out by the rule, and is judged by it.
    #[test]
    fn with_the_mode_on_a_new_lrc_set_keeps_every_group_in_distinct_racks() {
        let profile = EcProfile::lrc_standard();
        // Five racks of two: the EC v1 layout puts two members of group 0
        // into one rack here (EC2-080).
        let racks = [
            ("z1", "a"),
            ("z1", "a"),
            ("z1", "b"),
            ("z1", "b"),
            ("z1", "c"),
            ("z1", "c"),
            ("z1", "d"),
            ("z1", "d"),
            ("z1", "e"),
            ("z1", "e"),
        ];
        let labels_by_node = labels_from(&racks);
        let set_nodes = nodes(10);
        let v1 = ShardSlotAssignment::build(&profile, &set_nodes, &labels_by_node).unwrap();

        for policy in [EcTopologyPolicy::Strict, EcTopologyPolicy::Warn] {
            let builder = rule_builder(profile.clone(), labels_by_node.clone(), policy, RACK);
            let created = builder.build_new(FIRST_SET_ID, &set_nodes, 2).unwrap();

            assert_ne!(created.set.slot_assignment, v1);
            assert_eq!(
                created.set.layout_rule,
                EcSetLayoutRule::LrcGroups {
                    level: EcFailureDomainLevel::Rack
                }
            );
            assert_eq!(created.violation, None);
            assert_eq!(
                created.algorithm_version,
                EC_LRC_GROUP_LAYOUT_ALGORITHM_VERSION
            );
            for group in local_group_slots(&created.set.slot_assignment) {
                let racks = group
                    .iter()
                    .map(|node| labels_by_node[node].rack.clone())
                    .collect::<BTreeSet<_>>();
                assert_eq!(racks.len(), group.len(), "{group:?}");
            }
            assert!(
                !created.set.topology_risks.iter().any(|risk| risk.contains("LRC local group")),
                "{:?}",
                created.set.topology_risks
            );
            // One zone: the zone risks are the true ones, and nothing else.
            assert_eq!(
                created.set.topology_risks,
                vec![
                    "all EC shard slots are in one zone".to_string(),
                    "profile 'ec-lrc-standard' does not survive the loss of zone 'z1', which \
                     holds 10 of the set's 10 slot(s)"
                        .to_string(),
                ]
            );
        }
    }

    /// A set whose layout meets every goal is healthy: three zones that each
    /// can be lost, globals apart, one host per slot.
    #[test]
    fn a_set_meeting_the_rule_and_surviving_every_zone_is_healthy() {
        let racks = [
            ("x", "a"),
            ("x", "a"),
            ("x", "b"),
            ("y", "a"),
            ("y", "b"),
            ("w", "a"),
            ("w", "b"),
        ];
        let builder = rule_builder(
            EcProfile::lrc_small(),
            labels_from(&racks),
            EcTopologyPolicy::Strict,
            RACK,
        );

        let created = builder.build_new(FIRST_SET_ID, &nodes(7), 2).unwrap();

        assert_eq!(created.set.topology_risks, Vec::<String>::new());
        assert_eq!(created.set.topology_health, EcTopologyHealth::Healthy);
    }

    fn assert_layout_refusal(error: EcConfigError, reason: EcLayoutViolationReason, needle: &str) {
        let text = error.to_string();
        match error {
            EcConfigError::LayoutRule(violation) => assert_eq!(violation.reason, reason),
            other => panic!("expected a layout-rule refusal, got {other:?}"),
        }
        assert!(text.starts_with("invalid EC topology: "), "{text}");
        assert!(text.contains(needle), "{text}");
        assert!(text.contains("S4_EC_TOPOLOGY_POLICY=warn"), "{text}");
    }

    /// Under `strict` a set whose nodes cannot meet the rule is refused, for
    /// each of the three reasons, with the numbers that explain it
    /// (`EC2-ZONE-02-002`, EC2-083).
    #[test]
    fn under_strict_each_unsatisfiable_topology_is_refused_with_its_reason() {
        let profile = EcProfile::lrc_standard();
        let set_nodes = nodes(10);
        let refuse = |labels_by_node: HashMap<NodeId, TopologyLabels>| {
            rule_builder(
                profile.clone(),
                labels_by_node,
                EcTopologyPolicy::Strict,
                RACK,
            )
            .build_new(FIRST_SET_ID, &set_nodes, 2)
            .unwrap_err()
        };

        let three_racks = labels_from(&[
            ("z1", "a"),
            ("z1", "a"),
            ("z1", "a"),
            ("z1", "a"),
            ("z1", "b"),
            ("z1", "b"),
            ("z1", "b"),
            ("z1", "c"),
            ("z1", "c"),
            ("z1", "c"),
        ]);
        assert_layout_refusal(
            refuse(three_racks),
            EcLayoutViolationReason::TooFewDomains,
            "are in 3 rack(s) (z1/a=4, z1/b=3, z1/c=3): build the set from nodes in at least 4 racks",
        );

        let overloaded = labels_from(&[
            ("z1", "a"),
            ("z1", "a"),
            ("z1", "a"),
            ("z1", "a"),
            ("z1", "a"),
            ("z1", "b"),
            ("z1", "b"),
            ("z1", "c"),
            ("z1", "c"),
            ("z1", "d"),
        ]);
        assert_layout_refusal(
            refuse(overloaded),
            EcLayoutViolationReason::DomainOverloaded,
            "racks z1/a=5 leave 3 node(s) for those 2 slot(s): move 1 node(s)",
        );

        let mut partial = racked_labels(&set_nodes, 5);
        partial.get_mut(&set_nodes[2]).unwrap().rack = None;
        partial.remove(&set_nodes[6]);
        assert_layout_refusal(
            refuse(partial),
            EcLayoutViolationReason::IncompleteLabels,
            &format!(
                "2 of 10 nodes lack them: {} (rack); {} (zone, rack, host, disk_group)",
                set_nodes[2], set_nodes[6]
            ),
        );

        // A pool without any labels is refused too, although EC v1 accepts it
        // under `strict`: the operator asked for racks the set cannot name.
        assert_layout_refusal(
            refuse(HashMap::new()),
            EcLayoutViolationReason::IncompleteLabels,
            "10 of 10 nodes lack them",
        );
    }

    /// Under `warn` the same topologies give a set, degraded, carrying its
    /// reason — never silently the EC v1 set as if the rule had been met.
    #[test]
    fn under_warn_each_unsatisfiable_topology_gives_a_degraded_set_with_its_reason() {
        let profile = EcProfile::lrc_standard();
        let set_nodes = nodes(10);
        let build = |labels_by_node: HashMap<NodeId, TopologyLabels>| {
            rule_builder(
                profile.clone(),
                labels_by_node,
                EcTopologyPolicy::Warn,
                RACK,
            )
            .build_new(FIRST_SET_ID, &set_nodes, 2)
            .unwrap()
        };
        let rule = EcSetLayoutRule::LrcGroups {
            level: EcFailureDomainLevel::Rack,
        };

        // Three racks: two members of a group must share one. As few as the
        // criterion forces: E − g = 4 − 2 = 2.
        let three_racks = labels_from(&[
            ("z1", "a"),
            ("z1", "a"),
            ("z1", "a"),
            ("z1", "a"),
            ("z1", "b"),
            ("z1", "b"),
            ("z1", "b"),
            ("z1", "c"),
            ("z1", "c"),
            ("z1", "c"),
        ]);
        let created = build(three_racks.clone());
        let violation = created.violation.clone().unwrap();
        assert_eq!(violation.reason, EcLayoutViolationReason::TooFewDomains);
        assert_eq!(created.set.layout_rule, rule);
        assert_eq!(
            created.algorithm_version,
            EC_LRC_GROUP_LAYOUT_ALGORITHM_VERSION
        );
        assert_eq!(
            created.set.topology_health,
            EcTopologyHealth::TopologyDegraded
        );
        assert!(created.set.topology_risks.contains(&violation.detail));
        let planner = LrcGroupPlanner::new(
            &profile,
            &set_nodes,
            &three_racks,
            EcFailureDomainLevel::Rack,
        )
        .unwrap();
        assert_eq!(
            planner
                .score_of(&created.set.slot_assignment.slot_order())
                .unwrap()
                .coincidences,
            2
        );
        let shared = created
            .set
            .topology_risks
            .iter()
            .filter(|risk| risk.contains("members in rack"))
            .count();
        assert_eq!(shared, 2, "{:?}", created.set.topology_risks);

        // Incomplete labels: nothing to spread by, so the EC v1 layout, and a
        // risk saying the rule was not applied.
        let mut partial = racked_labels(&set_nodes, 5);
        partial.get_mut(&set_nodes[2]).unwrap().rack = None;
        let created = build(partial.clone());
        assert_eq!(
            created.violation.as_ref().map(|violation| violation.reason),
            Some(EcLayoutViolationReason::IncompleteLabels)
        );
        assert_eq!(
            created.set.slot_assignment,
            ShardSlotAssignment::build(&profile, &set_nodes, &partial).unwrap()
        );
        assert_eq!(created.set.layout_rule, rule);
        assert_eq!(created.algorithm_version, EC_V1_LAYOUT_ALGORITHM_VERSION);
        assert_eq!(
            created.set.topology_health,
            EcTopologyHealth::TopologyDegraded
        );
        assert!(
            created.set.topology_risks.iter().any(|risk| {
                risk.contains("S4_EC_LRC_GROUP_DOMAIN=rack is not applied")
                    && risk.contains("1 of 10 nodes lack complete topology labels")
            }),
            "{:?}",
            created.set.topology_risks
        );

        // No labels at all: the EC v1 verdict — unknown topology — and why.
        let created = build(HashMap::new());
        assert_eq!(
            created.set.topology_health,
            EcTopologyHealth::TopologyUnknown
        );
        assert!(
            created
                .set
                .topology_risks
                .iter()
                .any(|risk| risk.contains("no node of the set carries topology labels")),
            "{:?}",
            created.set.topology_risks
        );
    }

    /// The EC v1 warning "a local group is entirely placed in one rack" is a
    /// refusal under `strict` with the mode on, and stays a risk under `warn`
    /// (`EC2-ZONE-02-004`).
    #[test]
    fn a_group_entirely_in_one_rack_is_refused_under_strict_and_a_risk_under_warn() {
        let profile = EcProfile::lrc_small();
        // Six nodes in rack a, one in rack b.
        let one_heavy_rack = labels_from(&[
            ("z1", "a"),
            ("z1", "a"),
            ("z1", "a"),
            ("z1", "a"),
            ("z1", "a"),
            ("z1", "a"),
            ("z1", "b"),
        ]);
        let set_nodes = nodes(7);
        let entirely = |set: &ErasureSet| {
            set.topology_risks
                .iter()
                .any(|risk| risk.contains("is entirely placed in rack"))
        };

        // The mode off: the EC v1 warning, as before.
        let v1 = rule_builder(
            profile.clone(),
            one_heavy_rack.clone(),
            EcTopologyPolicy::Warn,
            None,
        )
        .build_new(FIRST_SET_ID, &set_nodes, 2)
        .unwrap();
        assert!(entirely(&v1.set), "{:?}", v1.set.topology_risks);

        let refused = rule_builder(
            profile.clone(),
            one_heavy_rack.clone(),
            EcTopologyPolicy::Strict,
            RACK,
        )
        .build_new(FIRST_SET_ID, &set_nodes, 2)
        .unwrap_err();
        assert!(
            matches!(refused, EcConfigError::LayoutRule(_)),
            "{refused:?}"
        );

        let warned = rule_builder(profile, one_heavy_rack, EcTopologyPolicy::Warn, RACK)
            .build_new(FIRST_SET_ID, &set_nodes, 2)
            .unwrap();
        assert!(entirely(&warned.set), "{:?}", warned.set.topology_risks);
        assert!(
            warned
                .set
                .topology_risks
                .iter()
                .any(|risk| risk.contains("is entirely placed in rack 'z1/a'")),
            "{:?}",
            warned.set.topology_risks
        );
        assert_eq!(
            warned.set.topology_health,
            EcTopologyHealth::TopologyDegraded
        );
    }

    /// Each set is judged on its own nodes: in a pool three sets wide, a set
    /// on badly racked nodes is refused while a set on well racked nodes is
    /// built, and exactly as it would be in a pool of one set
    /// (`EC2-ZONE-02-006`).
    #[test]
    fn every_set_of_a_pool_is_judged_on_its_own_nodes() {
        let profile = EcProfile::lrc_small();
        let pool_nodes = nodes(21);
        let mut labels_by_node = HashMap::new();
        for (idx, node) in pool_nodes.iter().enumerate() {
            let rack = match idx {
                // Nodes 1-7: the inherited set, never judged by the rule.
                0..=6 => "inherited".to_string(),
                // Nodes 8-14: all in one rack.
                7..=13 => "crowded".to_string(),
                // Nodes 15-21: a rack each.
                _ => format!("r{idx}"),
            };
            labels_by_node.insert(*node, labels("z1", &rack, &format!("h{idx}")));
        }
        let builder = ErasureSetBuilder::new(
            PoolId::new(1),
            "ec",
            profile.clone(),
            pool_nodes.clone(),
            labels_by_node.clone(),
            EcTopologyPolicy::Strict,
            1,
        )
        .with_lrc_group_domain(RACK);

        let crowded = builder.build_new(FIRST_SET_ID, &pool_nodes[7..14], 2).unwrap_err();
        assert!(
            matches!(crowded, EcConfigError::LayoutRule(_)),
            "{crowded:?}"
        );
        let spread = builder.build_new(FIRST_SET_ID + 1, &pool_nodes[14..], 2).unwrap();

        let alone_labels = pool_nodes[14..]
            .iter()
            .map(|node| (*node, labels_by_node[node].clone()))
            .collect::<HashMap<_, _>>();
        let alone = ErasureSetBuilder::new(
            PoolId::new(1),
            "ec",
            profile,
            pool_nodes[14..].to_vec(),
            alone_labels,
            EcTopologyPolicy::Strict,
            1,
        )
        .with_lrc_group_domain(RACK)
        .build_new(FIRST_SET_ID + 1, &pool_nodes[14..], 2)
        .unwrap();
        assert_eq!(spread, alone);
    }

    /// A set laid out by the rule and restored from its record is the set that
    /// was created. After a label change that puts two members of a group in
    /// one rack, the restored set reports it as a risk under `strict` — it is
    /// never refused, because a rule must not stop a node from starting
    /// (EC2-083).
    #[test]
    fn a_rule_set_is_restored_verbatim_and_a_broken_rule_is_a_risk_not_a_refusal() {
        let profile = EcProfile::lrc_standard();
        let racks = [
            ("z1", "a"),
            ("z1", "a"),
            ("z1", "b"),
            ("z1", "b"),
            ("z1", "c"),
            ("z1", "c"),
            ("z1", "d"),
            ("z1", "d"),
            ("z1", "e"),
            ("z1", "e"),
        ];
        let set_nodes = nodes(10);
        let builder = rule_builder(
            profile.clone(),
            labels_from(&racks),
            EcTopologyPolicy::Strict,
            RACK,
        );
        let created = builder.build_new(FIRST_SET_ID, &set_nodes, 2).unwrap();
        let rule = created.set.layout_rule;
        let order = created.set.slot_assignment.slot_order();

        let restored = builder.build_recorded(FIRST_SET_ID, &set_nodes, 2, rule, &order).unwrap();
        assert_eq!(restored, created.set);

        // Every node now in one rack: the recorded order stays, and every
        // group breaks the rule.
        let one_rack = labels_from(&[("z1", "a"); 10]);
        let relabeled = rule_builder(profile, one_rack, EcTopologyPolicy::Strict, RACK);
        let restored = relabeled.build_recorded(FIRST_SET_ID, &set_nodes, 2, rule, &order).unwrap();
        assert_eq!(restored.slot_assignment.slot_order(), order);
        assert_eq!(restored.topology_health, EcTopologyHealth::TopologyDegraded);
        assert!(
            restored.topology_risks.iter().any(|risk| risk.contains("in 1 rack(s)")),
            "{:?}",
            restored.topology_risks
        );
        assert!(
            restored
                .topology_risks
                .iter()
                .any(|risk| risk.contains("LRC local group 0 is entirely placed in rack 'z1/a'")),
            "{:?}",
            restored.topology_risks
        );
        let with_record = relabeled.with_recorded_layout(&restored, rule, &order).unwrap();
        assert_eq!(with_record.topology_risks, restored.topology_risks);

        // A record naming another node is refused, whatever its rule.
        let mut foreign = order.clone();
        foreign[0] = NodeId(9_999);
        assert!(!ErasureSetBuilder::record_fits(&set_nodes, &foreign));
        assert!(relabeled.build_recorded(FIRST_SET_ID, &set_nodes, 2, rule, &foreign).is_err());
    }

    /// A record naming the group rule for an RS set can only be damaged; the
    /// set is still restored, and the risk says what is wrong with it.
    #[test]
    fn a_group_rule_record_for_an_rs_set_is_a_risk_not_a_refusal() {
        let profile = EcProfile::rs_small();
        let set_nodes = nodes(6);
        let builder = rule_builder(
            profile,
            complete_labels(&set_nodes),
            EcTopologyPolicy::Strict,
            None,
        );
        let rule = EcSetLayoutRule::LrcGroups {
            level: EcFailureDomainLevel::Rack,
        };

        let restored =
            builder.build_recorded(FIRST_SET_ID, &set_nodes, 2, rule, &set_nodes).unwrap();

        assert_eq!(restored.slot_assignment.slot_order(), set_nodes);
        assert!(
            restored
                .topology_risks
                .iter()
                .any(|risk| risk.contains("is not an LRC profile")),
            "{:?}",
            restored.topology_risks
        );
    }

    /// Racks and hosts of a set laid out by the rule are named by every label
    /// above them (EC2-081, point 1): rack `r1` of two zones is two racks, and
    /// host `h1` of two racks is two hosts, so `strict` accepts the set.
    #[test]
    fn a_rule_set_names_racks_and_hosts_by_the_labels_above_them() {
        let profile = EcProfile::lrc_small();
        let labels_by_node = nodes(7)
            .into_iter()
            .zip([
                ("x", "r1", "h1"),
                ("x", "r2", "h1"),
                ("y", "r1", "h1"),
                ("y", "r2", "h1"),
                ("w", "r1", "h1"),
                ("w", "r2", "h1"),
                ("w", "r3", "h1"),
            ])
            .map(|(node, (zone, rack, host))| (node, labels(zone, rack, host)))
            .collect::<HashMap<_, _>>();
        let set_nodes = nodes(7);

        let v1 = rule_builder(
            profile.clone(),
            labels_by_node.clone(),
            EcTopologyPolicy::Strict,
            None,
        )
        .build_new(FIRST_SET_ID, &set_nodes, 2)
        .unwrap_err();
        assert!(v1.to_string().contains("one host per slot"), "{v1}");

        let created = rule_builder(profile, labels_by_node, EcTopologyPolicy::Strict, RACK)
            .build_new(FIRST_SET_ID, &set_nodes, 2)
            .unwrap();
        assert!(
            !created.set.topology_risks.iter().any(|risk| risk.contains("rack diversity")),
            "{:?}",
            created.set.topology_risks
        );
    }

    /// The other two levels go through the same builder: `zone` needs a
    /// group's worth of zones, `host` is met by one host per slot.
    #[test]
    fn the_zone_and_host_levels_are_laid_out_by_the_same_rule() {
        let profile = EcProfile::lrc_small();
        let set_nodes = nodes(7);
        let three_zones = labels_from(&[
            ("x", "a"),
            ("x", "b"),
            ("x", "c"),
            ("y", "a"),
            ("y", "b"),
            ("w", "a"),
            ("w", "b"),
        ]);
        let zone = Some(EcFailureDomainLevel::Zone);
        let created = rule_builder(
            profile.clone(),
            three_zones.clone(),
            EcTopologyPolicy::Strict,
            zone,
        )
        .build_new(FIRST_SET_ID, &set_nodes, 2)
        .unwrap();
        for group in local_group_slots(&created.set.slot_assignment) {
            let zones =
                group.iter().map(|node| three_zones[node].zone.clone()).collect::<BTreeSet<_>>();
            assert_eq!(zones.len(), group.len(), "{group:?}");
        }

        let two_zones = labels_from(&[
            ("x", "a"),
            ("x", "b"),
            ("x", "c"),
            ("x", "d"),
            ("y", "a"),
            ("y", "b"),
            ("y", "c"),
        ]);
        let refused = rule_builder(profile.clone(), two_zones, EcTopologyPolicy::Strict, zone)
            .build_new(FIRST_SET_ID, &set_nodes, 2)
            .unwrap_err();
        assert!(
            refused.to_string().contains("over 3 distinct zones"),
            "{refused}"
        );

        let one_rack = labels_from(&[("z1", "a"); 7]);
        let created = rule_builder(
            profile,
            one_rack,
            EcTopologyPolicy::Strict,
            Some(EcFailureDomainLevel::Host),
        )
        .build_new(FIRST_SET_ID, &set_nodes, 2)
        .unwrap();
        assert_eq!(created.violation, None);
        assert_eq!(
            created.set.layout_rule,
            EcSetLayoutRule::LrcGroups {
                level: EcFailureDomainLevel::Host
            }
        );
    }

    /// Creating a set through the configuration path: the environment gives
    /// the builder, the builder lays the set out by the rule, and a set built
    /// from its record comes out the same.
    #[test]
    fn a_set_is_created_by_the_rule_through_the_configuration_path() {
        let _guard = TopologyEnvGuard::acquire();
        let profile = EcProfile::lrc_small();
        let width = usize::from(profile.total_shards());
        let nodes: Vec<(String, NodeId)> = (1..=width * 2)
            .map(|index| (format!("node-{index}"), NodeId(index as u128)))
            .collect();
        let topology = nodes
            .iter()
            .enumerate()
            .map(|(idx, (name, _))| {
                format!("{name}:zone=z1,rack=r{},host=h{idx},disk_group=d0", idx % 4)
            })
            .collect::<Vec<_>>()
            .join(";");
        std::env::set_var("S4_EC_PROFILE", &profile.name);
        std::env::set_var("S4_EC_NODE_TOPOLOGY", &topology);
        std::env::set_var(LRC_GROUP_DOMAIN_ENV, "rack");

        let layout = validate_pool_from_env(&test_pool_config(), &nodes, 3, 1).unwrap();
        let spare: Vec<NodeId> = nodes[width..].iter().map(|(_, node_id)| *node_id).collect();
        let created = layout.builder.build_new(FIRST_SET_ID, &spare, 2).unwrap();

        assert_eq!(
            created.set.layout_rule,
            EcSetLayoutRule::LrcGroups {
                level: EcFailureDomainLevel::Rack
            }
        );
        assert_eq!(created.violation, None);
        let restored = layout
            .builder
            .build_recorded(
                FIRST_SET_ID,
                &spare,
                2,
                created.set.layout_rule,
                &created.set.slot_assignment.slot_order(),
            )
            .unwrap();
        assert_eq!(restored, created.set);
    }

    /// A set of another pool must not be adopted, and duplicate ids inside one
    /// set must stay rejected — the v1 guarantees survive the refactor.
    #[test]
    fn duplicate_nodes_within_one_set_are_still_rejected() {
        let profile = EcProfile::rs_small();
        let mut set_nodes = nodes(profile.total_shards());
        set_nodes[1] = set_nodes[0];

        let err = ErasureSet::active(
            PoolId::new(9),
            1,
            profile,
            &set_nodes,
            &HashMap::new(),
            EcTopologyPolicy::Strict,
            1,
            1,
        )
        .unwrap_err();

        assert!(err.to_string().contains("distinct NodeIds"));
    }
}
