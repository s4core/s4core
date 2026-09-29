// Copyright 2026 S4Core Team
//
// Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

//! Erasure Coding v1 tombstone, orphan, and distributed GC foundation.
//!
//! This module owns the Enterprise-only Phase 8 boundary. It writes EC
//! tombstones through metadata quorum, retains physical shards until distributed
//! safety gates pass, and removes orphan shards only after lease and age checks.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use s4_cluster::{HlcTimestamp, KeyRange, NodeId, RepairFrontier};
use thiserror::Error;
use uuid::Uuid;

use crate::ec_metadata::{
    EcManifestReadResult, EcMetadataError, EcMetadataStore, EcObjectManifest, EcObjectVersionKey,
    EcOrphanShardKey, EcOrphanShardRecord, EcOrphanShardState, EcPlacementState,
    EcReplicatedSmallGcRecord, EcTranscodeJobRecord, LocalShardKey, LocalShardRecord,
    LocalShardState, ObjectDataLayout, PackedRepackMove, PackedSegmentObjectPlacement,
    PackedUnpackMove, ReplicatedSmallObjectPlacement, EC_MANIFEST_FORMAT_VERSION,
};
use crate::ec_metrics;
use crate::ec_packed_segments::{
    PackedObjectIdentity, PackedSegment, PackedSegmentConfig, PackedSegmentEntry,
    PackedSegmentError, PackedSegmentId, PackedSegmentState, SealReason,
};
use crate::ec_placement::EcErasureSets;
use crate::ec_read::EcObjectLayoutReadResult;
use crate::erasure_coding::{ErasureSet, ErasureSetRegistry, ErasureSetState};

/// Default EC tombstone GC grace period: 7 days.
pub const DEFAULT_EC_GC_GRACE_SECS: u64 = 7 * 24 * 3_600;
/// Default active transcode lease TTL.
pub const DEFAULT_EC_GC_TRANSCODE_LEASE_SECS: u64 = 300;
/// Default extra safety grace after a transcode lease expires.
pub const DEFAULT_EC_ORPHAN_LEASE_GRACE_SECS: u64 = 3_600;
/// Default minimum orphan shard age before physical GC.
pub const DEFAULT_EC_ORPHAN_MIN_AGE_SECS: u64 = 86_400;
/// Default metadata records scanned per GC page.
pub const DEFAULT_EC_GC_SCAN_LIMIT: usize = 1_024;
/// Default maximum local shards removed for one tombstone in a single cycle.
pub const DEFAULT_EC_GC_MAX_SHARDS_PER_OBJECT: usize = 100_000;
/// Default drain window before a retired packed segment may be tombstoned: 1 hour.
///
/// Not the tombstone grace and not a substitute for it. This window covers one
/// thing only: a read that resolved its object layout just before a repack
/// cutover and is still assembling bytes from the segment it named. Once the
/// tombstone is written the shards themselves wait out the full
/// `S4_EC_GC_GRACE_SECS` before anything is deleted, exactly as `EC2-062`
/// requires — this only decides when that clock starts.
pub const DEFAULT_EC_PACKED_RETIRE_GRACE_SECS: u64 = 3_600;
/// Default fraction of a segment that must be dead before it is repacked.
///
/// The break-even point of `EC2-061`: below it a repack reads and re-encodes
/// more than it gives back.
pub const DEFAULT_EC_PACKED_REPACK_DEAD_RATIO: f64 = 0.5;
/// Default absolute amount of dead space worth a repack: 8 MiB.
pub const DEFAULT_EC_PACKED_REPACK_MIN_DEAD_BYTES: u64 = 8 * 1024 * 1024;

/// Default grace between promoting an object and removing its shards: 1 hour.
///
/// Far shorter than the tombstone grace, because it protects against something
/// much shorter-lived: a node that has not yet read the promoted manifest and
/// is still serving the object from its shards. A tombstone has to outlive an
/// offline node; this only has to outlive a metadata read.
pub const DEFAULT_EC_PROMOTED_GC_GRACE_SECS: u64 = 3_600;

/// Default number of manifests one tombstone sweep walks before it stops and
/// leaves the rest to the next cycle.
///
/// The sweep looks for tombstones among every manifest the node holds, so its
/// cost is the walk and not the findings. A budget turns that into a fixed
/// slice per cycle, and successive cycles cover the keyspace; ten thousand
/// matches what an anti-entropy pass already walks (decision EC2-202).
pub const DEFAULT_EC_TOMBSTONE_GC_SCAN_BUDGET: usize = 10_000;

/// Default grace between releasing a tombstone's shards and retiring the
/// tombstone itself pool-wide: 1 hour.
///
/// Measured from the end of the tombstone grace, and it protects a different
/// thing. The tombstone grace makes sure the delete has reached every owner;
/// this one makes sure every owner has had a sweep in which to act on it.
/// Retirement removes the tombstone from the whole pool at once, and an owner
/// that had not swept by then keeps shards no manifest names any more — which
/// the orphan sweep collects, a slower road than the one it should have taken.
pub const DEFAULT_EC_TOMBSTONE_RETIRE_GRACE_SECS: u64 = 3_600;

/// EC Phase 8 GC configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EcGcConfig {
    /// Tombstone grace period before metadata and physical shard purge.
    pub gc_grace_secs: u64,
    /// Active transcode lease TTL used when only a last heartbeat was observed.
    pub transcode_lease_secs: u64,
    /// Extra safety grace after a transcode lease expires.
    pub orphan_lease_grace_secs: u64,
    /// Minimum orphan shard age before physical purge.
    pub orphan_min_age_secs: u64,
    /// Metadata records scanned per GC page.
    pub scan_limit: usize,
    /// Maximum local shards removed for one tombstone in a single cycle.
    pub max_shards_per_object: usize,
    /// Grace between promoting an object and removing its shards.
    pub promoted_grace_secs: u64,
    /// Manifests one tombstone sweep walks before leaving the rest to the next.
    pub tombstone_scan_budget: usize,
    /// Extra grace after the tombstone grace before a tombstone is retired
    /// from every metadata node of the pool.
    pub tombstone_retire_grace_secs: u64,
    /// Drain window between retiring a packed segment and tombstoning it.
    pub packed_retire_grace_secs: u64,
}

impl EcGcConfig {
    /// Parse Phase 8 `S4_EC_*` and `S4_GC_*` settings from the environment.
    pub fn from_env() -> Result<Self, EcGcError> {
        let gc_grace_secs = match parse_optional_u64("S4_EC_GC_GRACE_SECS")? {
            Some(value) => value,
            None => parse_optional_u64("S4_GC_GRACE_DAYS")?
                .map(|days| days.saturating_mul(24 * 3_600))
                .unwrap_or(DEFAULT_EC_GC_GRACE_SECS),
        };
        let transcode_lease_secs = parse_optional_u64("S4_EC_TRANSCODE_LEASE_SECS")?
            .unwrap_or(DEFAULT_EC_GC_TRANSCODE_LEASE_SECS);
        let orphan_lease_grace_secs = parse_optional_u64("S4_EC_ORPHAN_LEASE_GRACE_SECS")?
            .unwrap_or(DEFAULT_EC_ORPHAN_LEASE_GRACE_SECS);
        let orphan_min_age_secs = parse_optional_u64("S4_EC_ORPHAN_MIN_AGE_SECS")?
            .unwrap_or(DEFAULT_EC_ORPHAN_MIN_AGE_SECS);
        let scan_limit =
            parse_optional_usize("S4_EC_GC_SCAN_LIMIT")?.unwrap_or(DEFAULT_EC_GC_SCAN_LIMIT);
        let max_shards_per_object = parse_optional_usize("S4_EC_GC_MAX_SHARDS_PER_OBJECT")?
            .unwrap_or(DEFAULT_EC_GC_MAX_SHARDS_PER_OBJECT);
        let promoted_grace_secs = parse_optional_u64("S4_EC_PROMOTED_GC_GRACE_SECS")?
            .unwrap_or(DEFAULT_EC_PROMOTED_GC_GRACE_SECS);
        let tombstone_scan_budget = parse_optional_usize("S4_EC_TOMBSTONE_GC_SCAN_BUDGET")?
            .unwrap_or(DEFAULT_EC_TOMBSTONE_GC_SCAN_BUDGET);
        if tombstone_scan_budget == 0 {
            return Err(EcGcError::InvalidConfig {
                name: "S4_EC_TOMBSTONE_GC_SCAN_BUDGET",
                reason: "must be greater than 0".to_string(),
            });
        }
        let tombstone_retire_grace_secs = parse_optional_u64("S4_EC_TOMBSTONE_RETIRE_GRACE_SECS")?
            .unwrap_or(DEFAULT_EC_TOMBSTONE_RETIRE_GRACE_SECS);
        let packed_retire_grace_secs =
            parse_optional_u64("S4_EC_PACKED_SEGMENT_RETIRE_GRACE_SECS")?
                .unwrap_or(DEFAULT_EC_PACKED_RETIRE_GRACE_SECS);

        for (name, value) in [
            ("S4_EC_GC_GRACE_SECS", gc_grace_secs),
            ("S4_EC_TRANSCODE_LEASE_SECS", transcode_lease_secs),
            ("S4_EC_ORPHAN_LEASE_GRACE_SECS", orphan_lease_grace_secs),
            ("S4_EC_ORPHAN_MIN_AGE_SECS", orphan_min_age_secs),
            ("S4_EC_PROMOTED_GC_GRACE_SECS", promoted_grace_secs),
            (
                "S4_EC_TOMBSTONE_RETIRE_GRACE_SECS",
                tombstone_retire_grace_secs,
            ),
            (
                "S4_EC_PACKED_SEGMENT_RETIRE_GRACE_SECS",
                packed_retire_grace_secs,
            ),
        ] {
            if value == 0 {
                return Err(EcGcError::InvalidConfig {
                    name,
                    reason: "must be greater than 0".to_string(),
                });
            }
        }
        for (name, value) in [
            ("S4_EC_GC_SCAN_LIMIT", scan_limit),
            ("S4_EC_GC_MAX_SHARDS_PER_OBJECT", max_shards_per_object),
        ] {
            if value == 0 {
                return Err(EcGcError::InvalidConfig {
                    name,
                    reason: "must be greater than 0".to_string(),
                });
            }
        }

        Ok(Self {
            gc_grace_secs,
            transcode_lease_secs,
            orphan_lease_grace_secs,
            orphan_min_age_secs,
            scan_limit,
            max_shards_per_object,
            promoted_grace_secs,
            tombstone_scan_budget,
            tombstone_retire_grace_secs,
            packed_retire_grace_secs,
        })
    }

    fn gc_grace_nanos(&self) -> u64 {
        seconds_to_nanos(self.gc_grace_secs)
    }

    fn transcode_lease_nanos(&self) -> u64 {
        seconds_to_nanos(self.transcode_lease_secs)
    }

    fn orphan_lease_grace_nanos(&self) -> u64 {
        seconds_to_nanos(self.orphan_lease_grace_secs)
    }

    fn orphan_min_age_nanos(&self) -> u64 {
        seconds_to_nanos(self.orphan_min_age_secs)
    }

    fn promoted_grace_nanos(&self) -> u64 {
        seconds_to_nanos(self.promoted_grace_secs)
    }

    fn tombstone_retire_grace_nanos(&self) -> u64 {
        seconds_to_nanos(self.tombstone_retire_grace_secs)
    }

    fn packed_retire_grace_nanos(&self) -> u64 {
        seconds_to_nanos(self.packed_retire_grace_secs)
    }
}

impl Default for EcGcConfig {
    fn default() -> Self {
        Self {
            gc_grace_secs: DEFAULT_EC_GC_GRACE_SECS,
            transcode_lease_secs: DEFAULT_EC_GC_TRANSCODE_LEASE_SECS,
            orphan_lease_grace_secs: DEFAULT_EC_ORPHAN_LEASE_GRACE_SECS,
            orphan_min_age_secs: DEFAULT_EC_ORPHAN_MIN_AGE_SECS,
            scan_limit: DEFAULT_EC_GC_SCAN_LIMIT,
            max_shards_per_object: DEFAULT_EC_GC_MAX_SHARDS_PER_OBJECT,
            promoted_grace_secs: DEFAULT_EC_PROMOTED_GC_GRACE_SECS,
            tombstone_scan_budget: DEFAULT_EC_TOMBSTONE_GC_SCAN_BUDGET,
            tombstone_retire_grace_secs: DEFAULT_EC_TOMBSTONE_RETIRE_GRACE_SECS,
            packed_retire_grace_secs: DEFAULT_EC_PACKED_RETIRE_GRACE_SECS,
        }
    }
}

/// Request to delete one EC-bucket object.
#[derive(Debug, Clone)]
pub struct EcDeleteRequest {
    /// Object version to delete.
    pub object: EcObjectVersionKey,
    /// Erasure set the tombstone is written against.
    ///
    /// Only used for an object that has no set of its own — an explicit RF=3
    /// small object, whose tombstone still has to name one. An erasure-coded
    /// object's tombstone inherits the set from its live manifest, so it lands
    /// on the same nodes that served the object.
    pub erasure_set: ErasureSet,
    /// HLC timestamp assigned to the delete marker.
    pub delete_hlc: HlcTimestamp,
    /// Idempotency ID for this delete operation.
    pub operation_id: Uuid,
    /// Node that originated the tombstone manifest.
    pub origin_node_id: NodeId,
    /// Delete creation time in nanoseconds since Unix epoch.
    pub now_nanos: u64,
}

impl EcDeleteRequest {
    /// Validate delete request invariants.
    pub fn validate(&self) -> Result<(), EcGcError> {
        self.object.validate()?;
        // Deleting is not writing. A set that has stopped taking new objects —
        // draining, or read-only because an operator deactivated it — still owns
        // the objects already in it and still has to accept their tombstones;
        // refusing here would make those objects undeletable. Only a retired set
        // serves nothing at all.
        if self.erasure_set.state == ErasureSetState::Retired {
            return Err(EcGcError::InvalidRequest {
                reason: "delete erasure set must not be Retired".to_string(),
            });
        }
        self.erasure_set.profile.validate().map_err(|error| EcGcError::InvalidRequest {
            reason: error.to_string(),
        })?;
        if self.delete_hlc.is_zero() {
            return Err(EcGcError::InvalidRequest {
                reason: "delete_hlc must not be zero".to_string(),
            });
        }
        if self.operation_id.is_nil() {
            return Err(EcGcError::InvalidRequest {
                reason: "operation_id must not be nil".to_string(),
            });
        }
        if self.now_nanos == 0 {
            return Err(EcGcError::InvalidRequest {
                reason: "now_nanos must be greater than 0".to_string(),
            });
        }
        Ok(())
    }
}

/// Layout class deleted by the EC GC coordinator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EcDeletedLayout {
    /// Object was represented by EC shards.
    ErasureCoded,
    /// Object was stored through explicit RF=3 small-object placement.
    ReplicatedSmall,
    /// Object lived inside a packed segment.
    PackedSegment,
}

/// Result of an EC-bucket delete operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EcDeleteOutcome {
    /// A tombstone was committed and quorum-confirmed.
    Deleted {
        /// Deleted layout class.
        layout: EcDeletedLayout,
        /// Quorum-confirmed tombstone manifest.
        tombstone: Box<EcObjectManifest>,
        /// Metadata commit ACKs.
        manifest_acks: u8,
        /// RF=3 small-object tombstone ACKs, when applicable.
        replicated_small_acks: Option<u8>,
    },
    /// The object/version was already quorum-confirmed deleted.
    AlreadyDeleted {
        /// Existing tombstone manifest.
        tombstone: Box<EcObjectManifest>,
    },
    /// Quorum confirmed the object/version is absent.
    NotFound,
    /// A live version with an equal or newer HLC won over this delete.
    SupersededByLive {
        /// HLC of the live version that won.
        live_hlc: HlcTimestamp,
    },
}

/// Manifest quorum commit result for a tombstone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EcTombstoneCommitResult {
    /// Number of metadata replicas that ACKed the tombstone.
    pub acks: u8,
}

/// Port that can retire a manifest through the metadata quorum.
///
/// Split from the full GC metadata port because retiring something is a
/// strictly smaller job than deleting an object: it needs to read the manifest
/// it is about to replace and to commit the replacement, and nothing else. A
/// packed segment has no object layout to resolve — it is not an object — so
/// asking its handle for one would be asking for something that cannot exist.
#[async_trait]
pub trait EcTombstoneQuorum: Send + Sync {
    /// Read an EC manifest through quorum metadata semantics.
    async fn quorum_read_manifest(
        &self,
        object: &EcObjectVersionKey,
    ) -> Result<EcManifestReadResult, EcGcError>;

    /// Commit a tombstone manifest through the metadata quorum path.
    async fn commit_tombstone(
        &self,
        tombstone: EcObjectManifest,
        required_acks: u8,
    ) -> Result<EcTombstoneCommitResult, EcGcError>;
}

/// Port used by Phase 8 for quorum-visible EC metadata.
#[async_trait]
pub trait EcGcMetadataCoordinator: EcTombstoneQuorum {
    /// Read object layout through quorum metadata semantics.
    async fn quorum_read_object_layout(
        &self,
        object: &EcObjectVersionKey,
    ) -> Result<EcObjectLayoutReadResult, EcGcError>;

    /// Remove a retired tombstone from every metadata node of the pool at once.
    ///
    /// All or nothing, and from one node rather than from each in turn. A
    /// tombstone that is gone from some nodes and still on others is a manifest
    /// the pool can no longer agree on: with half the copies removed a quorum
    /// read reaches neither "deleted" nor "never existed", so every later node
    /// is refused the very answer its own GC gate needs, and the shards it
    /// holds are stranded for good.
    ///
    /// Retirement is therefore one step taken by the node whose gates passed.
    /// Every other owner finds the tombstone already gone, reads that as
    /// "collected" and releases its shards on its own sweep.
    async fn retire_tombstone_everywhere(
        &self,
        tombstone: &EcObjectManifest,
    ) -> Result<(), EcGcError>;
}

/// Request to tombstone an explicit RF=3 small object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EcReplicatedSmallDeleteRequest {
    /// Object version to delete.
    pub object: EcObjectVersionKey,
    /// Explicit RF=3 placement.
    pub placement: ReplicatedSmallObjectPlacement,
    /// Delete HLC timestamp.
    pub delete_hlc: HlcTimestamp,
    /// Delete operation ID.
    pub operation_id: Uuid,
    /// Required RF ACKs.
    pub required_acks: u8,
}

/// Result of RF=3 small-object delete/tombstone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EcReplicatedSmallDeleteResult {
    /// Number of replica ACKs.
    pub acks: u8,
}

/// Request to physically purge a replicated-small object after GC gates pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EcReplicatedSmallPurgeRequest {
    /// Object version to purge.
    pub object: EcObjectVersionKey,
    /// Explicit RF=3 placement.
    pub placement: ReplicatedSmallObjectPlacement,
    /// Delete operation ID.
    pub operation_id: Uuid,
    /// Required RF ACKs.
    pub required_acks: u8,
}

/// Result of RF=3 small-object physical purge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EcReplicatedSmallPurgeResult {
    /// Number of replica ACKs.
    pub acks: u8,
}

/// Port used to delete and purge explicit RF=3 small-object layouts.
#[async_trait]
pub trait EcReplicatedSmallDeleter: Send + Sync {
    /// Write a replicated-small delete marker to the selected RF replicas.
    async fn tombstone_replicated_small(
        &self,
        request: EcReplicatedSmallDeleteRequest,
    ) -> Result<EcReplicatedSmallDeleteResult, EcGcError>;

    /// Physically purge a replicated-small object after GC safety gates pass.
    async fn purge_replicated_small(
        &self,
        request: EcReplicatedSmallPurgeRequest,
    ) -> Result<EcReplicatedSmallPurgeResult, EcGcError>;
}

/// Class of physical EC shard purge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EcDeadShardPurgeKind {
    /// Shard is dead because its object/version tombstone is safely purgeable.
    Tombstone,
    /// Shard is dead because it belongs to a failed or losing operation.
    Orphan,
    /// Shard is dead because the object was promoted back to RF=3.
    Promoted,
}

/// Request to physically purge one dead local EC shard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EcDeadShardPurgeRequest {
    /// Local shard key.
    pub key: LocalShardKey,
    /// Local physical shard record.
    pub record: LocalShardRecord,
    /// Erasure set the shard was written for.
    ///
    /// A pool may hold several sets, and every purge is decided from metadata
    /// that belongs to exactly one of them. Naming it on the request is what
    /// lets the purger say, in a log line or a metric, whose shard it removed —
    /// and what makes a shard removed under the wrong set's metadata visible
    /// rather than silent.
    pub erasure_set_id: u64,
    /// Purge class.
    pub kind: EcDeadShardPurgeKind,
}

/// Port used to purge physical EC shard bytes.
#[async_trait]
pub trait EcDeadShardPurger: Send + Sync {
    /// Purge one dead local shard from physical storage.
    async fn purge_dead_shard(&self, request: EcDeadShardPurgeRequest) -> Result<(), EcGcError>;
}

/// What one packed object's EC probe found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EcPackedProbeOutcome {
    /// Whether serving the object needed chunk reconstruction.
    ///
    /// A degraded probe still proves the data is recoverable, so it does not
    /// block the release; it does say the segment is one failure closer to
    /// unreadable, which is worth an operator's attention before its last other
    /// copy is deleted.
    pub degraded: bool,
}

/// Port that reads one packed object out of its segment's shards.
#[async_trait]
pub trait EcPackedSegmentProbe: Send + Sync {
    /// Read the object from EC alone and confirm it against the segment index.
    async fn probe_packed_object(
        &self,
        segment_id: PackedSegmentId,
        entry: &PackedSegmentEntry,
    ) -> Result<EcPackedProbeOutcome, EcGcError>;
}

/// Port that makes one packed object's location durable across the cluster.
///
/// Until this succeeds the object's whereabouts are known to one node only, so
/// the hot copies are the sole cluster-wide readable form of the data and must
/// not be deleted. See decision EC2-068.
#[async_trait]
pub trait EcPackedPointerCommitter: Send + Sync {
    /// Commit the object's pointer manifest to its erasure set's metadata quorum.
    ///
    /// Must fail rather than return when the quorum is not reached: the caller
    /// treats success as permission to delete the last hot copy.
    async fn commit_packed_pointer(
        &self,
        segment: &PackedSegment,
        entry: &PackedSegmentEntry,
    ) -> Result<(), EcGcError>;
}

/// Port that deletes the hot RF=3 copies of one packed object.
#[async_trait]
pub trait EcPackedHotCopyPurger: Send + Sync {
    /// Delete the object's hot copies, leaving the object itself alive in EC.
    async fn purge_packed_hot_copies(
        &self,
        object: &EcObjectVersionKey,
        expected_source_hlc: HlcTimestamp,
    ) -> Result<(), EcGcError>;
}

/// What releasing one packed segment's hot copies did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EcPackedSegmentRelease {
    /// Segment that was released.
    pub segment_id: PackedSegmentId,
    /// Objects read back out of EC before anything was changed.
    pub probed: usize,
    /// How many of those needed chunk reconstruction.
    pub degraded_probes: usize,
    /// Live objects whose pointer manifest reached the metadata quorum.
    pub committed: usize,
    /// Objects whose layout stopped naming hot copies.
    pub switched: usize,
    /// Objects whose hot copies were deleted.
    pub purged: usize,
}

/// When a fragmented packed segment is worth repacking.
///
/// A repack costs a read of the segment's live part, an encode of it and a
/// write of new shards, and returns the dead part multiplied by the profile's
/// expansion. Both thresholds of `EC2-061` have to hold: the fraction says the
/// exchange is favourable, and the absolute floor keeps a node from spending an
/// encode on a few megabytes.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PackedRepackPolicy {
    /// Fraction of the segment that must be dead, in `0.0..=1.0`.
    pub dead_ratio: f64,
    /// Dead bytes a segment must hold before the work is worth doing.
    pub min_dead_bytes: u64,
}

impl Default for PackedRepackPolicy {
    fn default() -> Self {
        Self {
            dead_ratio: DEFAULT_EC_PACKED_REPACK_DEAD_RATIO,
            min_dead_bytes: DEFAULT_EC_PACKED_REPACK_MIN_DEAD_BYTES,
        }
    }
}

impl PackedRepackPolicy {
    /// Parse the repack thresholds from the environment.
    ///
    /// # Errors
    ///
    /// Returns [`EcGcError::InvalidConfig`] when a variable is present but does
    /// not parse, or parses outside the range a threshold can mean anything in.
    pub fn from_env() -> Result<Self, EcGcError> {
        let dead_ratio = parse_optional_f64("S4_EC_PACKED_SEGMENT_REPACK_DEAD_RATIO")?
            .unwrap_or(DEFAULT_EC_PACKED_REPACK_DEAD_RATIO);
        let min_dead_bytes = parse_optional_u64("S4_EC_PACKED_SEGMENT_REPACK_MIN_DEAD_BYTES")?
            .unwrap_or(DEFAULT_EC_PACKED_REPACK_MIN_DEAD_BYTES);
        let policy = Self {
            dead_ratio,
            min_dead_bytes,
        };
        policy.validate()?;
        Ok(policy)
    }

    /// Check the thresholds against the range they are meaningful in.
    ///
    /// # Errors
    ///
    /// Returns [`EcGcError::InvalidConfig`] when the fraction is not a real
    /// number above zero and at most one, or the floor is zero.
    pub fn validate(&self) -> Result<(), EcGcError> {
        if !self.dead_ratio.is_finite() || self.dead_ratio <= 0.0 || self.dead_ratio > 1.0 {
            return Err(EcGcError::InvalidConfig {
                name: "S4_EC_PACKED_SEGMENT_REPACK_DEAD_RATIO",
                reason: format!(
                    "must be greater than 0 and at most 1, got {}",
                    self.dead_ratio
                ),
            });
        }
        if self.min_dead_bytes == 0 {
            return Err(EcGcError::InvalidConfig {
                name: "S4_EC_PACKED_SEGMENT_REPACK_MIN_DEAD_BYTES",
                reason: "must be greater than 0".to_string(),
            });
        }
        Ok(())
    }
}

/// What the fragmentation analyzer decided about one segment.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PackedRepackDecision {
    /// Live objects should be moved out and the segment retired.
    Repack {
        /// Dead bytes the repack gives back.
        dead_bytes: u64,
        /// Fraction of the segment those bytes are.
        dead_ratio: f64,
    },
    /// Nothing live is left, so the segment is retired without moving anything.
    Retire,
    /// The segment holds dead space, but not enough to pay for the work.
    Fragmented {
        /// Dead bytes the segment is holding.
        dead_bytes: u64,
        /// Fraction of the segment those bytes are.
        dead_ratio: f64,
    },
    /// The segment is not in a state a repack can act on.
    NotEligible {
        /// State it is in instead.
        state: PackedSegmentState,
    },
}

/// Decide what to do with one packed segment, from its record alone.
///
/// Deliberately reads nothing but the segment: `EC2-061` requires the analyzer
/// to work off the record, so that scanning a pool of segments costs one pass
/// over one keyspace rather than a lookup per packed object.
///
/// Only a `Purged` segment is a candidate. Earlier in the lifecycle the
/// objects still have hot copies and the segment's own shards may not exist
/// yet, so there is nothing a repack could reclaim; later it is already
/// retired.
pub fn classify_packed_segment(
    segment: &PackedSegment,
    policy: &PackedRepackPolicy,
) -> PackedRepackDecision {
    if segment.state != PackedSegmentState::Purged {
        return PackedRepackDecision::NotEligible {
            state: segment.state,
        };
    }
    if segment.entries.is_empty() {
        return PackedRepackDecision::Retire;
    }

    let dead_bytes = segment.dead_bytes;
    let dead_ratio = segment.dead_ratio();
    if dead_ratio >= policy.dead_ratio && dead_bytes >= policy.min_dead_bytes {
        PackedRepackDecision::Repack {
            dead_bytes,
            dead_ratio,
        }
    } else {
        PackedRepackDecision::Fragmented {
            dead_bytes,
            dead_ratio,
        }
    }
}

/// Port that answers whether any of these objects is under Object Lock.
///
/// The EC layer does not depend on `s4-features`, so it asks rather than looks.
/// A segment holding one object under retention is not repacked at all: the
/// alternative — moving everything except the held object — would have to keep
/// the source segment anyway, because a chunk cannot be cut out of an encoded
/// container. That repack would create a second copy of the live data and free
/// nothing (`EC2-065`).
#[async_trait]
pub trait PackedSegmentRetentionSource: Send + Sync {
    /// Whether any of these object versions is under retention or legal hold.
    async fn is_any_under_retention(
        &self,
        objects: &[EcObjectVersionKey],
    ) -> Result<bool, EcGcError>;
}

/// Retention source for a deployment without Object Lock.
///
/// Answers "nothing is held", which is the correct answer when the subsystem
/// that could hold something is not there, and the safe one: a segment without
/// retention repacks normally.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoObjectLockRetention;

#[async_trait]
impl PackedSegmentRetentionSource for NoObjectLockRetention {
    async fn is_any_under_retention(
        &self,
        _objects: &[EcObjectVersionKey],
    ) -> Result<bool, EcGcError> {
        Ok(false)
    }
}

/// The work one analyzer pass found, and what it passed over.
#[derive(Debug, Clone, Default)]
pub struct EcPackedRepackPlan {
    /// Segment records examined.
    pub scanned: usize,
    /// Fragmented segments with no replacement staged for them yet.
    pub fragmented: Vec<PackedSegment>,
    /// Encoded replacements whose objects are still reading from their source.
    pub pending_cutover: Vec<PackedSegment>,
    /// Staged replacements still waiting for the encoder.
    pub pending_encode: Vec<PackedSegment>,
    /// Segments with nothing live left, which are retired as they are.
    pub empty: Vec<PackedSegment>,
    /// Segments left alone because an object in them is under Object Lock.
    pub blocked_by_retention: usize,
    /// What the same pass saw across every segment, for the operator.
    pub stats: EcPackedSegmentStats,
}

/// The census one analyzer pass took of every packed segment this node holds.
///
/// Gathered by the pass that was already walking the records, because the
/// alternative is a second scan of the same keyspace to answer the same
/// question. Everything is per erasure set, which is the unit an operator
/// balances and retires.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct EcPackedSegmentStats {
    /// Segments in each lifecycle state, keyed by set and then by state name.
    pub segments_by_state: HashMap<u64, HashMap<&'static str, usize>>,
    /// Bytes the set's segments handed out, including to objects since deleted.
    pub used_bytes: HashMap<u64, u64>,
    /// How many of those belong to deleted objects.
    pub dead_bytes: HashMap<u64, u64>,
    /// The dead fraction of each set, which is what the repack threshold reads.
    pub dead_ratio: HashMap<u64, f64>,
    /// Segments waiting for a repack, including the ones retention is holding.
    ///
    /// A blocked segment is counted here on purpose: it is work the node has
    /// found and cannot do, and leaving it out would show an empty queue on a
    /// pool that is steadily losing space to retention.
    pub repack_queue: HashMap<u64, usize>,
}

impl EcPackedSegmentStats {
    /// Fold one segment into the census.
    fn observe(&mut self, segment: &PackedSegment) {
        *self
            .segments_by_state
            .entry(segment.erasure_set_id)
            .or_default()
            .entry(packed_state_name(segment.state))
            .or_insert(0) += 1;

        // A dead segment's bytes are already spoken for: its objects have moved
        // and its shards are waiting out a grace period. Counting them would
        // make the dead fraction of a healthy pool climb every time a repack
        // succeeded, which is precisely backwards.
        if segment.state == PackedSegmentState::Dead {
            return;
        }
        *self.used_bytes.entry(segment.erasure_set_id).or_insert(0) += segment.used_bytes;
        *self.dead_bytes.entry(segment.erasure_set_id).or_insert(0) += segment.dead_bytes;
    }

    /// Note that a segment of this set is waiting for a repack.
    fn queue_repack(&mut self, erasure_set_id: u64) {
        *self.repack_queue.entry(erasure_set_id).or_insert(0) += 1;
    }

    /// Work out each set's dead fraction, once every segment has been seen.
    fn finish(&mut self) {
        for (erasure_set_id, used) in &self.used_bytes {
            let dead = self.dead_bytes.get(erasure_set_id).copied().unwrap_or(0);
            let ratio = if *used == 0 {
                0.0
            } else {
                (dead as f64) / (*used as f64)
            };
            self.dead_ratio.insert(*erasure_set_id, ratio);
        }
    }
}

/// The metric label a segment state is published under.
fn packed_state_name(state: PackedSegmentState) -> &'static str {
    match state {
        PackedSegmentState::Open => "open",
        PackedSegmentState::Sealed => "sealed",
        PackedSegmentState::Encoded => "encoded",
        PackedSegmentState::Purged => "purged",
        PackedSegmentState::Dead => "dead",
    }
}

/// The replacement a repack staged for one fragmented segment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EcPackedRepackStaged {
    /// Segment being repacked.
    pub source_id: PackedSegmentId,
    /// Replacement waiting for the pipeline to encode it.
    pub destination_id: PackedSegmentId,
    /// Live objects it will hold.
    pub objects: usize,
    /// Bytes those objects occupy, against the source's whole extent.
    pub live_bytes: u64,
}

/// What the cutover of one repack switched.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EcPackedRepackReport {
    /// Segment that was repacked and is now retired.
    pub source_id: PackedSegmentId,
    /// Replacement its objects now read from.
    pub destination_id: PackedSegmentId,
    /// Objects whose layout now names the replacement.
    pub moved: usize,
    /// Objects that had left the source by the time the cutover ran.
    pub abandoned: usize,
    /// Objects read back out of the replacement before anything was switched.
    pub probed: usize,
    /// Objects whose pointer manifest reached the metadata quorum.
    pub committed: usize,
    /// How many of those needed chunk reconstruction.
    pub degraded_probes: usize,
    /// Bytes the source gives back once its shards are removed.
    pub reclaimed_bytes: u64,
}

/// What one retirement sweep did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EcPackedRetireReport {
    /// Retired segments examined.
    pub scanned: usize,
    /// Segments whose shards were tombstoned and whose record was removed.
    pub retired: usize,
    /// Segments still inside their drain window, or still referenced.
    pub retained: usize,
}

/// Port that returns one packed object to ordinary RF=3 replication.
///
/// Two halves, because a packed object is two things: bytes inside a container
/// and a quorum-visible manifest that says so. Unpacking has to undo both, and
/// undoing only one leaves an object that some nodes serve and others cannot
/// find.
#[async_trait]
pub trait EcPackedSegmentUnpacker: Send + Sync {
    /// Make sure the object's bytes are on RF=3 replicas, and say which.
    ///
    /// An object whose segment was never released still has its hot copies, and
    /// the implementation is expected to keep them rather than write them
    /// again. One whose copies were purged has to be read out of the segment's
    /// shards and written back.
    async fn restore_hot_copies(
        &self,
        segment: &PackedSegment,
        entry: &PackedSegmentEntry,
        placement: &PackedSegmentObjectPlacement,
    ) -> Result<Vec<NodeId>, EcGcError>;

    /// Stop the cluster from describing the object as packed.
    ///
    /// Until this succeeds, a node that has not been told otherwise resolves the
    /// object through its pointer manifest and reads it out of a segment that is
    /// about to be retired (`EC2-068`).
    async fn publish_unpacked_manifest(&self, object: &EcObjectVersionKey)
        -> Result<(), EcGcError>;
}

/// What unpacking one segment did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EcPackedUnpackReport {
    /// Segment that was unpacked and retired.
    pub segment_id: PackedSegmentId,
    /// Live objects the segment held.
    pub objects: usize,
    /// Objects whose layout is now `ReplicatedSmall`.
    pub unpacked: usize,
    /// Objects whose bytes had to be written back to their replicas.
    pub restored: usize,
    /// Objects that still had their hot copies and only needed the layout switch.
    pub kept_hot_copies: usize,
    /// Objects that had left the segment before the unpack reached them.
    pub skipped: usize,
}

/// Port used to confirm tombstone propagation through anti-entropy.
#[async_trait]
pub trait EcTombstonePropagation: Send + Sync {
    /// Return `true` when anti-entropy has propagated this tombstone far enough for GC.
    async fn tombstone_propagated(&self, tombstone: &EcObjectManifest) -> Result<bool, EcGcError>;
}

/// Safety inputs required before tombstone GC may purge EC shards.
#[derive(Debug, Clone, Copy)]
pub struct EcTombstoneGcSafety<'a, A>
where
    A: EcTombstonePropagation,
{
    /// Anti-entropy propagation checker.
    pub propagation: &'a A,
    /// Repair frontier updated by EC anti-entropy.
    pub repair_frontier: &'a RepairFrontier,
    /// Key range used for frontier checks.
    pub key_range: &'a KeyRange,
    /// Shard owners that must have observed the tombstone.
    pub shard_owners: &'a [NodeId],
}

/// Observation used to create a durable orphan shard record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EcOrphanShardObservation {
    /// Local shard key observed as orphaned.
    pub key: LocalShardKey,
    /// Local physical shard record.
    pub record: LocalShardRecord,
    /// Erasure set targeted by the operation that wrote the shard.
    pub target_erasure_set_id: u64,
    /// Observation or shard creation time in nanoseconds since Unix epoch.
    pub observed_at: u64,
    /// Human-readable reason for retaining this orphan candidate.
    pub reason: String,
}

/// Result of recording an orphan shard candidate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EcOrphanShardRecorded {
    /// Durable orphan shard key.
    pub key: EcOrphanShardKey,
    /// Durable orphan shard record.
    pub record: EcOrphanShardRecord,
}

/// Tombstone GC report.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EcTombstoneGcReport {
    /// Tombstone manifests scanned.
    pub tombstones_scanned: u64,
    /// Tombstones retained by safety gates.
    pub tombstones_retained: u64,
    /// Tombstone manifests purged locally.
    pub tombstones_purged: u64,
    /// Local shard records scanned for purge.
    pub shards_scanned: u64,
    /// Local shards physically purged.
    pub shards_purged: u64,
    /// Objects whose shard list was capped for this cycle.
    pub shard_scan_capped: u64,
    /// Tombstones whose local shards this node released this cycle.
    pub tombstones_shards_released: u64,
    /// Tombstones this node retired from every metadata node of the pool.
    pub tombstones_retired: u64,
}

/// What a sweep may do with one tombstone.
///
/// Releasing the shards and retiring the tombstone are deliberately two
/// answers rather than one. Shards are node-local, so every owner releases its
/// own; the tombstone is what the whole pool agrees on, so it can only be
/// removed from every node at once — half a pool holding a tombstone the other
/// half has dropped is a manifest no quorum read can decide either way, which
/// would leave a deleted object answering "unavailable" instead of "gone".
/// Retirement therefore waits out a second grace, long enough for every owner
/// to have had a sweep of its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EcTombstoneGcVerdict {
    /// A safety gate says not yet.
    Retain,
    /// Release the local shards and keep the tombstone where it is.
    ReleaseShards,
    /// Release the local shards, then retire the tombstone pool-wide.
    ReleaseShardsAndRetire,
    /// Release the local shards; another node has already retired the tombstone.
    ReleaseShardsAlreadyRetired,
}

impl EcTombstoneGcVerdict {
    /// Whether the tombstone manifest itself goes in this cycle.
    fn removes_manifest(self) -> bool {
        matches!(
            self,
            Self::ReleaseShardsAndRetire | Self::ReleaseShardsAlreadyRetired
        )
    }
}

/// Promoted-object GC report.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EcPromotedGcReport {
    /// Promoted manifests scanned.
    pub promoted_scanned: u64,
    /// Promoted objects retained by safety gates.
    pub promoted_retained: u64,
    /// Promoted manifests purged locally.
    pub promoted_purged: u64,
    /// Local shard records scanned for purge.
    pub shards_scanned: u64,
    /// Local shards physically purged.
    pub shards_purged: u64,
    /// Objects whose shard list was capped for this cycle.
    pub shard_scan_capped: u64,
}

/// Orphan shard GC report.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EcOrphanGcReport {
    /// Orphan records scanned.
    pub orphans_scanned: u64,
    /// Orphan records retained by safety gates.
    pub orphans_retained: u64,
    /// Orphan records physically purged.
    pub orphans_purged: u64,
    /// Orphan records whose local shard metadata had already moved on.
    pub stale_orphan_records: u64,
}

/// Replicated-small GC report.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EcReplicatedSmallGcReport {
    /// Small-object GC records scanned.
    pub records_scanned: u64,
    /// Records retained by safety gates.
    pub records_retained: u64,
    /// Small objects physically purged.
    pub records_purged: u64,
}

/// Enterprise EC Phase 8 coordinator.
#[derive(Clone)]
pub struct EcGcCoordinator {
    metadata: Arc<EcMetadataStore>,
    config: EcGcConfig,
    sets: Arc<EcErasureSets>,
}

impl EcGcCoordinator {
    /// Create a Phase 8 EC GC coordinator for the sets of one pool.
    ///
    /// The registry is what keeps collection inside set boundaries: work is
    /// found by scanning local records, and a record naming a set this pool does
    /// not have is left alone rather than acted on.
    pub fn new(
        metadata: Arc<EcMetadataStore>,
        config: EcGcConfig,
        sets: Arc<EcErasureSets>,
    ) -> Self {
        Self {
            metadata,
            config,
            sets,
        }
    }

    /// Current GC configuration.
    pub fn config(&self) -> &EcGcConfig {
        &self.config
    }

    /// The sets this coordinator collects for, as they are now.
    pub fn sets(&self) -> Arc<ErasureSetRegistry> {
        self.sets.current()
    }

    /// Delete one EC-bucket object through quorum-visible tombstone metadata.
    pub async fn delete_object<M, S>(
        &self,
        request: EcDeleteRequest,
        metadata_coordinator: &M,
        small_deleter: &S,
    ) -> Result<EcDeleteOutcome, EcGcError>
    where
        M: EcGcMetadataCoordinator,
        S: EcReplicatedSmallDeleter,
    {
        request.validate()?;

        match metadata_coordinator.quorum_read_object_layout(&request.object).await? {
            EcObjectLayoutReadResult::Absent => {
                self.metadata.delete_object_layout(&request.object).await?;
                Ok(EcDeleteOutcome::NotFound)
            }
            EcObjectLayoutReadResult::Tombstone(tombstone) => {
                self.metadata.put_manifest(&tombstone).await?;
                self.metadata.delete_object_layout(&request.object).await?;
                self.resume_replicated_small_delete_if_needed(&tombstone, small_deleter).await?;
                Ok(EcDeleteOutcome::AlreadyDeleted { tombstone })
            }
            EcObjectLayoutReadResult::Present(ObjectDataLayout::ErasureCoded(reference)) => {
                reference.validate()?;
                let manifest =
                    match metadata_coordinator.quorum_read_manifest(&request.object).await? {
                        EcManifestReadResult::Present(manifest) => manifest,
                        EcManifestReadResult::Tombstone(tombstone) => {
                            self.metadata.put_manifest(&tombstone).await?;
                            self.metadata.delete_object_layout(&request.object).await?;
                            return Ok(EcDeleteOutcome::AlreadyDeleted {
                                tombstone: Box::new(tombstone),
                            });
                        }
                        EcManifestReadResult::Absent => {
                            self.metadata.delete_object_layout(&request.object).await?;
                            return Ok(EcDeleteOutcome::NotFound);
                        }
                    };
                if request.delete_hlc <= manifest.hlc {
                    return Ok(EcDeleteOutcome::SupersededByLive {
                        live_hlc: manifest.hlc,
                    });
                }
                let tombstone = tombstone_from_live_manifest(&request, &manifest)?;
                self.commit_and_confirm_tombstone(
                    tombstone,
                    EcDeletedLayout::ErasureCoded,
                    None,
                    metadata_coordinator,
                )
                .await
            }
            EcObjectLayoutReadResult::Present(ObjectDataLayout::PackedSegment(placement)) => {
                if request.delete_hlc <= placement.hlc {
                    return Ok(EcDeleteOutcome::SupersededByLive {
                        live_hlc: placement.hlc,
                    });
                }
                self.delete_packed_object(request, placement, small_deleter, metadata_coordinator)
                    .await
            }
            EcObjectLayoutReadResult::Present(ObjectDataLayout::ReplicatedSmall(placement)) => {
                if request.delete_hlc <= placement.hlc {
                    return Ok(EcDeleteOutcome::SupersededByLive {
                        live_hlc: placement.hlc,
                    });
                }
                let gc_record = EcReplicatedSmallGcRecord {
                    object: request.object.clone(),
                    operation_id: request.operation_id,
                    placement: placement.clone(),
                    tombstone_hlc: request.delete_hlc,
                    eligible_at: request.now_nanos.saturating_add(self.config.gc_grace_nanos()),
                    purged_at: None,
                    updated_at: request.now_nanos,
                };
                self.metadata.put_replicated_small_gc(&gc_record).await?;

                let tombstone = tombstone_from_small_placement(&request, &placement)?;
                let mut outcome = self
                    .commit_and_confirm_tombstone(
                        tombstone,
                        EcDeletedLayout::ReplicatedSmall,
                        None,
                        metadata_coordinator,
                    )
                    .await?;
                if !matches!(outcome, EcDeleteOutcome::Deleted { .. }) {
                    self.metadata.delete_replicated_small_gc(&request.object).await?;
                    return Ok(outcome);
                }
                let required_acks = replicated_small_quorum(&placement)?;
                let delete_result = small_deleter
                    .tombstone_replicated_small(EcReplicatedSmallDeleteRequest {
                        object: request.object.clone(),
                        placement: placement.clone(),
                        delete_hlc: request.delete_hlc,
                        operation_id: request.operation_id,
                        required_acks,
                    })
                    .await?;
                if delete_result.acks < required_acks {
                    return Err(EcGcError::ReplicatedSmallQuorum {
                        required_acks,
                        actual_acks: delete_result.acks,
                    });
                }
                if let EcDeleteOutcome::Deleted {
                    replicated_small_acks,
                    ..
                } = &mut outcome
                {
                    *replicated_small_acks = Some(delete_result.acks);
                }
                Ok(outcome)
            }
        }
    }

    /// Release a segment's objects from their hot copies, once EC can serve them.
    ///
    /// The order is the safety property, and it is the reverse of the tempting
    /// one. Every object is first read back out of the segment's shards and
    /// checked against the hash the index recorded. Then every live object's
    /// pointer manifest is committed to the metadata quorum, which is what makes
    /// its location survive the loss of this node. Only then are the layouts
    /// switched, and only then are the hot copies deleted.
    ///
    /// A failure at either of the first two steps leaves the segment exactly as
    /// it was — hot copies intact, layouts untouched. Before the probe, EC has
    /// not proven it can serve the data; before the commit, only this node knows
    /// where the data is. In both cases the hot copies are the only thing that
    /// can still answer a read.
    ///
    /// The probe covers every entry of the segment, including objects deleted
    /// while it was open: it is a statement about the container. The commit
    /// covers only the live ones — a pointer manifest for a deleted object would
    /// resurrect it.
    ///
    /// # Errors
    ///
    /// Returns [`EcGcError::InvalidRequest`] when the segment has not been
    /// encoded, and propagates a probe, commit or purge failure. A failed probe
    /// or commit is reported before anything is changed.
    pub async fn release_packed_segment_hot_copies<P, C, U>(
        &self,
        segment: &PackedSegment,
        probe: &P,
        committer: &C,
        purger: &U,
    ) -> Result<EcPackedSegmentRelease, EcGcError>
    where
        P: EcPackedSegmentProbe,
        C: EcPackedPointerCommitter,
        U: EcPackedHotCopyPurger,
    {
        if segment.state != PackedSegmentState::Encoded {
            return Err(EcGcError::InvalidRequest {
                reason: format!(
                    "segment {} is {} and cannot release its hot copies; only an encoded \
                     segment can",
                    segment.segment_id, segment.state
                ),
            });
        }
        if segment.repacked_from.is_some() {
            // A replacement's objects have no hot copies to release: they are
            // still being served by the segment it replaces, and moving them is
            // `complete_packed_repack`. Releasing here would move the
            // replacement to `Purged` without ever switching them, stranding
            // both segments.
            return Err(EcGcError::InvalidRequest {
                reason: format!(
                    "segment {} is a repack replacement; its objects are switched by the repack \
                     cutover, not released",
                    segment.segment_id
                ),
            });
        }

        let mut degraded_probes = 0usize;
        for entry in &segment.entries {
            match probe.probe_packed_object(segment.segment_id, entry).await {
                Ok(outcome) => {
                    if outcome.degraded {
                        degraded_probes = degraded_probes.saturating_add(1);
                        tracing::warn!(
                            segment_id = %segment.segment_id,
                            bucket = %entry.object.bucket,
                            key = %entry.object.key,
                            "packed object could only be read from EC in degraded mode"
                        );
                    }
                }
                Err(error) => {
                    tracing::error!(
                        segment_id = %segment.segment_id,
                        bucket = %entry.object.bucket,
                        key = %entry.object.key,
                        %error,
                        "packed object is not yet readable from EC; keeping the hot copies"
                    );
                    return Err(error);
                }
            }
        }

        // Committed before the switch, so a failure here still finds the hot
        // copies where it left them.
        let live = self.metadata.live_packed_segment_entries(segment).await?;
        let mut committed = 0usize;
        for entry in &live {
            committer.commit_packed_pointer(segment, entry).await.inspect_err(|error| {
                tracing::error!(
                    segment_id = %segment.segment_id,
                    bucket = %entry.object.bucket,
                    key = %entry.object.key,
                    %error,
                    "packed object has no durable pointer yet; keeping the hot copies"
                );
            })?;
            committed = committed.saturating_add(1);
        }

        let switched = self.metadata.switch_packed_segment_to_ec(segment).await?;
        let mut purged = 0usize;
        for object in &switched {
            let hlc = segment
                .entry_for(object)
                .map(|entry| entry.hlc)
                .unwrap_or_else(HlcTimestamp::zero);
            purger.purge_packed_hot_copies(object, hlc).await?;
            purged = purged.saturating_add(1);
        }

        ec_metrics::record_hot_replicas_purged(segment.erasure_set_id);
        Ok(EcPackedSegmentRelease {
            segment_id: segment.segment_id,
            probed: segment.entries.len(),
            degraded_probes,
            committed,
            switched: switched.len(),
            purged,
        })
    }

    /// Find the packed-segment work this node has, in one pass over the records.
    ///
    /// Reads segment records only. A segment carries its own occupancy, so the
    /// whole pool is judged in one scan of one keyspace — no lookup per packed
    /// object, which at four thousand objects a segment is the difference
    /// between a scan and a storm.
    ///
    /// Three kinds of work come out of it: fragmented segments that need a
    /// replacement staged, encoded replacements whose objects are ready to be
    /// switched onto them, and segments nothing lives in any more. A segment
    /// that already has a replacement is not offered again — that is what keeps
    /// a repack interrupted by a restart from starting a second one.
    ///
    /// Object Lock is asked about last, after the cheap thresholds have already
    /// excluded almost everything. A held segment is counted and passed over,
    /// not reported as an error: the hold expires on its own and the segment
    /// repacks then (`EC2-065`).
    ///
    /// # Errors
    ///
    /// Propagates a metadata read failure and whatever the retention source
    /// returns.
    pub async fn plan_packed_repacks<R>(
        &self,
        policy: &PackedRepackPolicy,
        retention: &R,
        max_segments: usize,
    ) -> Result<EcPackedRepackPlan, EcGcError>
    where
        R: PackedSegmentRetentionSource + ?Sized,
    {
        policy.validate()?;
        let mut plan = EcPackedRepackPlan::default();
        // Rebuilt, not read as stored. A delete arrives at whichever node the
        // client chose and is recorded as an idempotent set entry against the
        // segment, never as an edit of the segment's own record (`EC2-061`).
        // Reading the records as they lie would therefore show every segment as
        // pristine and the analyzer would never fire.
        let segments = self.metadata.load_packed_segments(self.config.scan_limit).await?.segments;

        // A source with a replacement already staged is mid-repack, whatever
        // state that replacement is in. Offering it again would encode the same
        // objects a second time.
        let staged: HashSet<PackedSegmentId> = segments
            .iter()
            .filter(|segment| segment.state != PackedSegmentState::Dead)
            .filter_map(|segment| segment.repacked_from)
            .collect();

        for segment in segments {
            // The census covers every segment, even once the work lists are
            // full: an operator's view of the pool must not depend on how much
            // of it one cycle had room to act on.
            plan.stats.observe(&segment);
            if plan.fragmented.len() >= max_segments
                && plan.pending_cutover.len() >= max_segments
                && plan.empty.len() >= max_segments
            {
                continue;
            }
            plan.scanned = plan.scanned.saturating_add(1);

            if segment.state == PackedSegmentState::Encoded
                && segment.repacked_from.is_some()
                && plan.pending_cutover.len() < max_segments
            {
                plan.pending_cutover.push(segment);
                continue;
            }
            if segment.state == PackedSegmentState::Sealed
                && segment.repacked_from.is_some()
                && plan.pending_encode.len() < max_segments
            {
                plan.pending_encode.push(segment);
                continue;
            }
            if staged.contains(&segment.segment_id) {
                continue;
            }

            match classify_packed_segment(&segment, policy) {
                PackedRepackDecision::Retire if plan.empty.len() < max_segments => {
                    plan.empty.push(segment);
                }
                PackedRepackDecision::Repack {
                    dead_bytes,
                    dead_ratio,
                } if plan.fragmented.len() < max_segments => {
                    let objects: Vec<EcObjectVersionKey> =
                        segment.entries.iter().map(|entry| entry.object.clone()).collect();
                    plan.stats.queue_repack(segment.erasure_set_id);
                    if retention.is_any_under_retention(&objects).await? {
                        plan.blocked_by_retention = plan.blocked_by_retention.saturating_add(1);
                        ec_metrics::record_packed_repack_blocked(segment.erasure_set_id);
                        tracing::debug!(
                            segment_id = %segment.segment_id,
                            dead_bytes,
                            "packed segment holds an object under retention and is not repacked"
                        );
                        continue;
                    }
                    tracing::info!(
                        segment_id = %segment.segment_id,
                        dead_bytes,
                        dead_ratio,
                        live_objects = segment.entries.len(),
                        "packed segment queued for repacking"
                    );
                    plan.fragmented.push(segment);
                }
                // Only the near misses are worth a line. A segment holding
                // dead space that has not reached the threshold is what an
                // operator asking "why has this not been repacked yet" needs to
                // see; a pristine or ineligible one is just noise.
                PackedRepackDecision::Fragmented {
                    dead_bytes,
                    dead_ratio,
                } if dead_bytes > 0 => {
                    tracing::debug!(
                        segment_id = %segment.segment_id,
                        dead_bytes,
                        dead_ratio,
                        threshold_ratio = policy.dead_ratio,
                        threshold_bytes = policy.min_dead_bytes,
                        "packed segment holds dead space but has not reached the repack threshold"
                    );
                }
                _ => {}
            }
        }
        plan.stats.finish();
        Ok(plan)
    }

    /// Stage a replacement for a fragmented segment, without moving anything yet.
    ///
    /// Pure metadata. The replacement is described — which objects it holds and
    /// where — and handed to the ordinary transcode pipeline, which reads those
    /// objects out of the source segment's shards and encodes them into the
    /// replacement's own. Nothing is copied here, and nothing points at the
    /// replacement until [`Self::complete_packed_repack`] switches the layouts.
    ///
    /// Deliberately no re-replication: the objects are already durable in the
    /// source's shards, and the two places that check them — the encoder on the
    /// way in and the probe on the way out — are both stronger than a check
    /// performed here would be, because they check the bytes through the
    /// placement that will actually serve them.
    ///
    /// A crash at any point after this leaves a sealed replacement nobody
    /// references and a source still serving every one of its objects. The next
    /// cycle finishes the repack from the `repacked_from` link.
    ///
    /// # Errors
    ///
    /// Returns [`EcGcError::InvalidRequest`] when the segment is not a repack
    /// candidate, [`EcGcError::PackedRepack`] when the replacement cannot hold
    /// what the source has live, and propagates a metadata write failure.
    pub async fn repack_packed_segment(
        &self,
        source: &PackedSegment,
        segment_config: &PackedSegmentConfig,
        now: u64,
    ) -> Result<EcPackedRepackStaged, EcGcError> {
        if source.state != PackedSegmentState::Purged {
            return Err(EcGcError::InvalidRequest {
                reason: format!(
                    "segment {} is {} and cannot be repacked; only a purged segment has \
                     reclaimable space",
                    source.segment_id, source.state
                ),
            });
        }
        if source.entries.is_empty() {
            return Err(EcGcError::InvalidRequest {
                reason: format!(
                    "segment {} holds nothing live and is retired rather than repacked",
                    source.segment_id
                ),
            });
        }

        let mut destination =
            PackedSegment::open_repack(PackedSegmentId::new_random(), source, segment_config, now);
        for entry in &source.entries {
            let identity = PackedObjectIdentity::new(
                entry.object.clone(),
                entry.content_hash_sha256,
                entry.hlc,
            )
            .with_s3_metadata(
                entry.etag.clone(),
                entry.content_type.clone(),
                entry.user_metadata.clone(),
            );
            destination
                .append_without_journal(identity, entry.length)
                .map_err(packed_repack_error)?;
        }
        destination.seal(now, SealReason::Repacked).map_err(packed_repack_error)?;

        let staged = EcPackedRepackStaged {
            source_id: source.segment_id,
            destination_id: destination.segment_id,
            objects: destination.entries.len(),
            live_bytes: destination.used_bytes,
        };
        self.metadata.put_packed_segment(&destination).await?;
        tracing::info!(
            source_id = %staged.source_id,
            destination_id = %staged.destination_id,
            objects = staged.objects,
            live_bytes = staged.live_bytes,
            dead_bytes = source.dead_bytes,
            "staged a replacement for a fragmented packed segment"
        );
        Ok(staged)
    }

    /// Throw away a staged replacement that no longer matches its source.
    ///
    /// A replacement is described before it is encoded, and the gap between the
    /// two is long enough for a client to delete one of the objects it was
    /// built from. The encoder then asks for bytes that no longer exist and the
    /// job retries for ever, which is the worst of both worlds: the source stays
    /// fragmented and the node keeps paying for the attempt.
    ///
    /// Discarding is safe precisely because a staged replacement is referenced
    /// by nothing — no object points at it, and it has no shards yet. The next
    /// analyzer pass stages a fresh one from whatever is live by then, so the
    /// repack heals itself rather than needing an operator.
    ///
    /// Answers whether the replacement was discarded.
    ///
    /// # Errors
    ///
    /// Propagates a metadata read or write failure.
    pub async fn discard_drifted_replacement(
        &self,
        destination: &PackedSegment,
        now: u64,
    ) -> Result<bool, EcGcError> {
        let Some(source_id) = destination.repacked_from else {
            return Ok(false);
        };
        let mut drifted = Vec::new();
        for entry in &destination.entries {
            if !self.metadata.packed_layout_names_segment(&entry.object, source_id).await? {
                drifted.push(entry.object.clone());
            }
        }
        if drifted.is_empty() {
            return Ok(false);
        }

        self.metadata.retire_drifted_replacement(destination, now).await?;
        tracing::info!(
            segment_id = %destination.segment_id,
            source_id = %source_id,
            drifted = drifted.len(),
            objects = destination.entries.len(),
            "discarded a staged replacement whose objects moved on before it was encoded"
        );
        Ok(true)
    }

    /// Switch a repack's objects onto the replacement now that it is encoded.
    ///
    /// The same order the hot-copy release uses, for the same reason. Every
    /// object is first read back out of the replacement's shards and checked
    /// against the hash the index recorded. Then every object's pointer manifest
    /// is committed to the metadata quorum, which is what makes its new location
    /// survive the loss of this node. Only then are the layouts switched, and
    /// only at that moment does the source segment stop being the thing that
    /// answers reads.
    ///
    /// A failure at either of the first two steps changes nothing: the source
    /// segment is untouched and still serving every object it holds.
    ///
    /// # Errors
    ///
    /// Returns [`EcGcError::InvalidRequest`] when the replacement is not an
    /// encoded repack destination or its source is gone, and propagates probe,
    /// commit and metadata failures.
    pub async fn complete_packed_repack<P, C>(
        &self,
        destination: &PackedSegment,
        probe: &P,
        committer: &C,
        now: u64,
    ) -> Result<EcPackedRepackReport, EcGcError>
    where
        P: EcPackedSegmentProbe,
        C: EcPackedPointerCommitter,
    {
        if destination.state != PackedSegmentState::Encoded {
            return Err(EcGcError::InvalidRequest {
                reason: format!(
                    "replacement segment {} is {} and cannot take over reads; only an encoded \
                     segment can",
                    destination.segment_id, destination.state
                ),
            });
        }
        let Some(source_id) = destination.repacked_from else {
            return Err(EcGcError::InvalidRequest {
                reason: format!(
                    "segment {} is not a repack replacement",
                    destination.segment_id
                ),
            });
        };
        let source = self.metadata.get_packed_segment(source_id).await?.ok_or_else(|| {
            EcGcError::InvalidRequest {
                reason: format!(
                    "segment {source_id} was repacked into {} but has no record any more",
                    destination.segment_id
                ),
            }
        })?;

        let mut degraded_probes = 0usize;
        let mut moves = Vec::with_capacity(destination.entries.len());
        for entry in &destination.entries {
            match probe.probe_packed_object(destination.segment_id, entry).await {
                Ok(outcome) => {
                    if outcome.degraded {
                        degraded_probes = degraded_probes.saturating_add(1);
                        tracing::warn!(
                            segment_id = %destination.segment_id,
                            bucket = %entry.object.bucket,
                            key = %entry.object.key,
                            "repacked object could only be read from EC in degraded mode"
                        );
                    }
                }
                Err(error) => {
                    tracing::error!(
                        segment_id = %destination.segment_id,
                        bucket = %entry.object.bucket,
                        key = %entry.object.key,
                        %error,
                        "repacked object is not readable yet; leaving the objects where they are"
                    );
                    return Err(error);
                }
            }
            moves.push(PackedRepackMove {
                object: entry.object.clone(),
                placement: PackedSegmentObjectPlacement::from_entry(
                    destination,
                    entry,
                    Vec::new(),
                )?,
            });
        }

        // Committed before the switch, so a failure here still finds every
        // object reading from the segment it was reading from before.
        //
        // Only for the objects that are actually about to move. An object that
        // was deleted or overwritten while the repack ran no longer names the
        // source, and committing a pointer manifest for it would put a live
        // manifest back over its tombstone — bringing it back from the dead.
        let mut committed = 0usize;
        for entry in &destination.entries {
            if !self
                .metadata
                .packed_layout_names_segment(&entry.object, source.segment_id)
                .await?
            {
                continue;
            }
            committer.commit_packed_pointer(destination, entry).await.inspect_err(|error| {
                tracing::error!(
                    segment_id = %destination.segment_id,
                    bucket = %entry.object.bucket,
                    key = %entry.object.key,
                    %error,
                    "repacked object has no durable pointer yet; leaving it where it is"
                );
            })?;
            committed = committed.saturating_add(1);
        }

        let cutover = self.metadata.commit_packed_repack(&source, destination, &moves, now).await?;

        ec_metrics::record_packed_repack_segment(source.erasure_set_id);
        ec_metrics::record_packed_repack_objects(
            source.erasure_set_id,
            cutover.switched.len() as u64,
        );
        ec_metrics::record_packed_repack_reclaimed(source.erasure_set_id, cutover.reclaimed_bytes);
        tracing::info!(
            source_id = %source.segment_id,
            destination_id = %destination.segment_id,
            moved = cutover.switched.len(),
            abandoned = cutover.abandoned.len(),
            reclaimed_bytes = cutover.reclaimed_bytes,
            "packed segment repacked; its shards go once the drain window closes"
        );

        Ok(EcPackedRepackReport {
            source_id: source.segment_id,
            destination_id: destination.segment_id,
            moved: cutover.switched.len(),
            abandoned: cutover.abandoned.len(),
            probed: destination.entries.len(),
            committed,
            degraded_probes,
            reclaimed_bytes: cutover.reclaimed_bytes,
        })
    }

    /// Return one segment's objects to RF=3 and retire the segment.
    ///
    /// The way back out of packed segments, and the reason the feature can be
    /// switched off at all: turning packing off stops new segments but does
    /// nothing for the ones that exist, so without this an operator who enabled
    /// the feature could never fully leave it.
    ///
    /// Every object is put back on its own three copies and its manifest is
    /// republished so that no node still resolves it through the segment. Only
    /// then are the layouts switched and the segment retired, in one batch. The
    /// order is the same one every other cutover in this module uses, and for
    /// the same reason: until the bytes are somewhere else, the segment is the
    /// only thing that can answer a read.
    ///
    /// A segment that has not been encoded is unpacked just as readily — its
    /// objects still have the hot copies the write path gave them, so there is
    /// nothing to restore and the whole operation is the layout switch.
    ///
    /// # Errors
    ///
    /// Returns [`EcGcError::InvalidRequest`] when the segment is open or already
    /// retired, and propagates restore, manifest and metadata failures. A
    /// failure leaves every object reading from the segment, exactly as before.
    pub async fn unpack_packed_segment<U>(
        &self,
        segment: &PackedSegment,
        unpacker: &U,
        now: u64,
    ) -> Result<EcPackedUnpackReport, EcGcError>
    where
        U: EcPackedSegmentUnpacker + ?Sized,
    {
        if segment.state == PackedSegmentState::Open {
            return Err(EcGcError::InvalidRequest {
                reason: format!(
                    "segment {} is still open and is a live pool's write cursor; it has to be \
                     sealed before it can be unpacked",
                    segment.segment_id
                ),
            });
        }
        if segment.state == PackedSegmentState::Dead {
            return Err(EcGcError::InvalidRequest {
                reason: format!("segment {} is already retired", segment.segment_id),
            });
        }

        let mut moves = Vec::with_capacity(segment.entries.len());
        let mut restored = 0usize;
        let mut kept_hot_copies = 0usize;
        let mut left_behind = 0usize;

        for entry in &segment.entries {
            let Some(ObjectDataLayout::PackedSegment(placement)) =
                self.metadata.get_object_layout(&entry.object).await?
            else {
                // Overwritten or deleted since the segment was built. Whoever
                // did it owns the object now.
                left_behind = left_behind.saturating_add(1);
                continue;
            };
            if placement.segment_id != segment.segment_id {
                left_behind = left_behind.saturating_add(1);
                continue;
            }

            let had_hot_copies = !placement.hot_replica_nodes.is_empty();
            let replica_nodes = unpacker
                .restore_hot_copies(segment, entry, &placement)
                .await
                .inspect_err(|error| {
                    tracing::error!(
                        segment_id = %segment.segment_id,
                        bucket = %entry.object.bucket,
                        key = %entry.object.key,
                        %error,
                        "packed object could not be put back on its replicas; leaving the \
                         segment as it is"
                    );
                })?;
            if had_hot_copies {
                kept_hot_copies = kept_hot_copies.saturating_add(1);
            } else {
                restored = restored.saturating_add(1);
            }

            // Published before the switch, so a failure here still leaves every
            // node resolving the object the way it did a moment ago.
            unpacker.publish_unpacked_manifest(&entry.object).await.inspect_err(|error| {
                tracing::error!(
                    segment_id = %segment.segment_id,
                    bucket = %entry.object.bucket,
                    key = %entry.object.key,
                    %error,
                    "packed object is still described as packed cluster-wide; leaving the \
                     segment as it is"
                );
            })?;

            moves.push(PackedUnpackMove {
                object: entry.object.clone(),
                placement: ReplicatedSmallObjectPlacement::new(
                    replica_nodes,
                    entry.content_hash_sha256,
                    entry.length,
                    entry.hlc,
                )?,
            });
        }

        let result = self.metadata.commit_packed_unpack(segment, &moves, now).await?;

        ec_metrics::record_packed_segment_unpacked(segment.erasure_set_id);
        tracing::info!(
            segment_id = %segment.segment_id,
            objects = segment.entries.len(),
            unpacked = result.unpacked.len(),
            restored,
            kept_hot_copies,
            "packed segment unpacked; its objects are ordinary RF=3 small objects again"
        );

        Ok(EcPackedUnpackReport {
            segment_id: segment.segment_id,
            objects: segment.entries.len(),
            unpacked: result.unpacked.len(),
            restored,
            kept_hot_copies,
            skipped: left_behind.saturating_add(result.skipped.len()),
        })
    }

    /// Retire a segment whose objects have all been deleted.
    ///
    /// There is nothing to move, so the whole repack collapses into its last
    /// step. The shards still wait out the drain window before they are
    /// tombstoned, because a read that started before the last delete may still
    /// be assembling bytes from them.
    ///
    /// # Errors
    ///
    /// Returns [`EcGcError::InvalidRequest`] when the segment still holds live
    /// entries, and propagates a metadata write failure.
    pub async fn retire_empty_packed_segment(
        &self,
        segment: &PackedSegment,
        now: u64,
    ) -> Result<(), EcGcError> {
        if !segment.entries.is_empty() {
            return Err(EcGcError::InvalidRequest {
                reason: format!(
                    "segment {} still holds {} live object(s)",
                    segment.segment_id,
                    segment.entries.len()
                ),
            });
        }
        self.metadata.retire_empty_packed_segment(segment, now).await?;
        tracing::info!(
            segment_id = %segment.segment_id,
            "packed segment emptied by deletions and retired"
        );
        Ok(())
    }

    /// Tombstone the shards of every retired segment whose readers have gone.
    ///
    /// The last step of the repack, and the only one that destroys anything.
    /// Two gates stand in front of it. The drain window says no read can still
    /// be holding a layout that names the segment; the referrer check says no
    /// layout names it any more at all, which catches the case the window
    /// cannot — a cutover that switched some objects and left others behind.
    ///
    /// What it writes is an ordinary tombstone against the segment's manifest,
    /// so the shards are removed by the tombstone GC that already runs on every
    /// node. Each node holds its own shards and is the only one that can
    /// release them, and the tombstone is what reaches all of them.
    ///
    /// The operation id is the segment's own id, which makes a retry of a
    /// half-finished retirement recognisable as the same operation rather than
    /// a second one.
    ///
    /// # Errors
    ///
    /// Propagates metadata and quorum failures. A segment that cannot be
    /// retired this cycle is counted as retained rather than failing the sweep.
    pub async fn retire_drained_packed_segments<M>(
        &self,
        now_nanos: u64,
        delete_hlc: HlcTimestamp,
        origin_node_id: NodeId,
        metadata_coordinator: &M,
        max_segments: usize,
    ) -> Result<EcPackedRetireReport, EcGcError>
    where
        M: EcTombstoneQuorum + ?Sized,
    {
        let mut report = EcPackedRetireReport::default();
        let segments = self.metadata.list_packed_segments(self.config.scan_limit).await?;

        for segment in segments {
            if report.retired >= max_segments {
                break;
            }
            if segment.state != PackedSegmentState::Dead {
                continue;
            }
            report.scanned = report.scanned.saturating_add(1);

            if !segment.is_drained(now_nanos, self.config.packed_retire_grace_nanos()) {
                report.retained = report.retained.saturating_add(1);
                continue;
            }
            let referrers =
                self.metadata.packed_segment_referrers(&segment, self.config.scan_limit).await?;
            if !referrers.is_empty() {
                tracing::warn!(
                    segment_id = %segment.segment_id,
                    referrers = referrers.len(),
                    "retired packed segment is still named by object layouts; keeping its shards"
                );
                report.retained = report.retained.saturating_add(1);
                continue;
            }

            match self
                .tombstone_retired_segment(
                    &segment,
                    now_nanos,
                    delete_hlc,
                    origin_node_id,
                    metadata_coordinator,
                )
                .await
            {
                Ok(()) => {
                    self.metadata.delete_packed_segment(segment.segment_id).await?;
                    ec_metrics::record_packed_segment_retired(segment.erasure_set_id);
                    report.retired = report.retired.saturating_add(1);
                    tracing::info!(
                        segment_id = %segment.segment_id,
                        "retired packed segment tombstoned; its shards are now collectable"
                    );
                }
                Err(error) => {
                    // The segment keeps its shards and its record. Nothing has
                    // been lost, and the next sweep tries again.
                    tracing::warn!(
                        segment_id = %segment.segment_id,
                        %error,
                        "retired packed segment could not be tombstoned this cycle"
                    );
                    report.retained = report.retained.saturating_add(1);
                }
            }
        }
        Ok(report)
    }

    /// Write the tombstone that makes one retired segment's shards collectable.
    ///
    /// A segment with no manifest never reached the encoder — an empty
    /// replacement, or one abandoned before its shards were written. There is
    /// nothing to tombstone and nothing to collect, so the record is simply
    /// dropped by the caller.
    async fn tombstone_retired_segment<M>(
        &self,
        segment: &PackedSegment,
        now_nanos: u64,
        delete_hlc: HlcTimestamp,
        origin_node_id: NodeId,
        metadata_coordinator: &M,
    ) -> Result<(), EcGcError>
    where
        M: EcTombstoneQuorum + ?Sized,
    {
        let manifest_key = segment.manifest_object_key();
        let live = match metadata_coordinator.quorum_read_manifest(&manifest_key).await? {
            EcManifestReadResult::Present(manifest) => manifest,
            EcManifestReadResult::Tombstone(_) | EcManifestReadResult::Absent => return Ok(()),
        };

        let request = EcDeleteRequest {
            object: manifest_key,
            erasure_set: self.resolve_segment_set(segment)?,
            delete_hlc,
            operation_id: segment.segment_id.as_uuid(),
            origin_node_id,
            now_nanos,
        };
        request.validate()?;
        let tombstone = tombstone_from_live_manifest(&request, &live)?;
        match self
            .commit_and_confirm_tombstone(
                tombstone,
                EcDeletedLayout::PackedSegment,
                None,
                metadata_coordinator,
            )
            .await?
        {
            EcDeleteOutcome::Deleted { .. } | EcDeleteOutcome::AlreadyDeleted { .. } => Ok(()),
            outcome => Err(EcGcError::InvalidRequest {
                reason: format!(
                    "tombstoning segment {} returned {outcome:?} instead of a tombstone",
                    segment.segment_id
                ),
            }),
        }
    }

    /// The erasure set a segment's shards belong to.
    fn resolve_segment_set(&self, segment: &PackedSegment) -> Result<ErasureSet, EcGcError> {
        self.sets
            .current()
            .resolve_manifest_set(segment.erasure_set_id)
            .cloned()
            .ok_or_else(|| EcGcError::InvalidRequest {
                reason: format!(
                    "segment {} names erasure set {}, which this node does not know",
                    segment.segment_id, segment.erasure_set_id
                ),
            })
    }

    /// Delete an object that lives inside a packed segment.
    ///
    /// Two things have to happen, and the order between them is the whole
    /// point. The object's bytes become dead inside the segment, which is
    /// recorded so the repacker of phase 5 can eventually reclaim them — the
    /// bytes themselves cannot be freed here, because they sit inside a
    /// container that is encoded as a whole. And while the segment is still
    /// open, the object's real bytes are the RF=3 copies named in the
    /// placement, so those are tombstoned exactly as a replicated-small
    /// object's are.
    ///
    /// Once the hot copies are purged the placement lists none, and there is
    /// nothing to tombstone: the segment's shards are all that remains, and
    /// they go when the segment does.
    async fn delete_packed_object<M, S>(
        &self,
        request: EcDeleteRequest,
        placement: PackedSegmentObjectPlacement,
        small_deleter: &S,
        metadata_coordinator: &M,
    ) -> Result<EcDeleteOutcome, EcGcError>
    where
        M: EcGcMetadataCoordinator,
        S: EcReplicatedSmallDeleter,
    {
        let hot = placement.hot_placement();
        if let Some(hot) = &hot {
            let gc_record = EcReplicatedSmallGcRecord {
                object: request.object.clone(),
                operation_id: request.operation_id,
                placement: hot.clone(),
                tombstone_hlc: request.delete_hlc,
                eligible_at: request.now_nanos.saturating_add(self.config.gc_grace_nanos()),
                purged_at: None,
                updated_at: request.now_nanos,
            };
            self.metadata.put_replicated_small_gc(&gc_record).await?;
        }

        let tombstone = tombstone_from_packed_placement(&request, &placement)?;
        let mut outcome = self
            .commit_and_confirm_tombstone(
                tombstone,
                EcDeletedLayout::PackedSegment,
                None,
                metadata_coordinator,
            )
            .await?;
        if !matches!(outcome, EcDeleteOutcome::Deleted { .. }) {
            if hot.is_some() {
                self.metadata.delete_replicated_small_gc(&request.object).await?;
            }
            return Ok(outcome);
        }

        // Charged from the placement resolved above, not from the layout: the
        // tombstone commit has already dropped the layout, so re-reading it
        // here would find nothing and the segment would never learn that these
        // bytes are dead. Charged only once the tombstone stands, so a delete
        // that failed does not send the repacker after space still in use.
        self.metadata.charge_packed_object_deleted(&request.object, &placement).await?;

        let Some(hot) = hot else {
            return Ok(outcome);
        };
        let required_acks = replicated_small_quorum(&hot)?;
        let delete_result = small_deleter
            .tombstone_replicated_small(EcReplicatedSmallDeleteRequest {
                object: request.object.clone(),
                placement: hot,
                delete_hlc: request.delete_hlc,
                operation_id: request.operation_id,
                required_acks,
            })
            .await?;
        if delete_result.acks < required_acks {
            return Err(EcGcError::ReplicatedSmallQuorum {
                required_acks,
                actual_acks: delete_result.acks,
            });
        }
        if let EcDeleteOutcome::Deleted {
            replicated_small_acks,
            ..
        } = &mut outcome
        {
            *replicated_small_acks = Some(delete_result.acks);
        }
        Ok(outcome)
    }

    /// Persist an orphan shard candidate with lease information observed locally.
    pub async fn record_orphan_shard(
        &self,
        observation: EcOrphanShardObservation,
    ) -> Result<EcOrphanShardRecorded, EcGcError> {
        observation.key.validate()?;
        observation.record.validate()?;
        if observation.record.operation_id.is_nil() {
            return Err(EcGcError::InvalidRequest {
                reason: "orphan observation operation_id must not be nil".to_string(),
            });
        }
        if observation.observed_at == 0 {
            return Err(EcGcError::InvalidRequest {
                reason: "orphan observation observed_at must be greater than 0".to_string(),
            });
        }

        let lease = self.metadata.get_transcode_lease(observation.record.operation_id).await?;
        let key = EcOrphanShardKey::new(
            observation.record.operation_id,
            observation.key.object.clone(),
            observation.key.chunk_index,
            observation.key.slot_index,
        );
        let record = EcOrphanShardRecord {
            operation_id: observation.record.operation_id,
            source_object: observation.key.object,
            target_erasure_set_id: observation.target_erasure_set_id,
            chunk_index: observation.key.chunk_index,
            slot_index: observation.key.slot_index,
            local_shard: observation.record,
            created_at: observation.observed_at,
            lease_epoch: lease.as_ref().map(|lease| lease.acquired_at).unwrap_or(0),
            last_lease_seen_at: lease.as_ref().map(|lease| lease.last_heartbeat_at),
            state: EcOrphanShardState::Candidate,
            reason: observation.reason,
        };
        self.metadata.put_orphan_shard(&key, &record).await?;
        Ok(EcOrphanShardRecorded { key, record })
    }

    /// Run one tombstone GC cycle over the tombstones it is handed.
    ///
    /// The caller does the walking, and for two reasons. Its walk is resumable,
    /// so a cycle costs a page of manifests rather than the whole keyspace; and
    /// a pool of several erasure sets sweeps set by set, which used to mean one
    /// full scan per set to answer about a fraction of it (decision EC2-202).
    ///
    /// Anything in `tombstones` that is not a tombstone is ignored rather than
    /// counted: the report is about what this cycle decided, and a live
    /// manifest handed here by mistake decided nothing.
    pub async fn run_tombstone_gc<M, P, A>(
        &self,
        now_nanos: u64,
        tombstones: &[EcObjectManifest],
        metadata_coordinator: &M,
        shard_purger: &P,
        safety: EcTombstoneGcSafety<'_, A>,
    ) -> Result<EcTombstoneGcReport, EcGcError>
    where
        M: EcGcMetadataCoordinator,
        P: EcDeadShardPurger,
        A: EcTombstonePropagation,
    {
        if now_nanos == 0 {
            return Err(EcGcError::InvalidRequest {
                reason: "now_nanos must be greater than 0".to_string(),
            });
        }

        let mut report = EcTombstoneGcReport::default();
        {
            for manifest in tombstones.iter().cloned() {
                if !manifest_is_tombstone(&manifest) {
                    continue;
                }
                report.tombstones_scanned = report.tombstones_scanned.saturating_add(1);
                let verdict = self
                    .tombstone_gc_verdict(&manifest, now_nanos, metadata_coordinator, &safety)
                    .await?;
                if verdict == EcTombstoneGcVerdict::Retain {
                    report.tombstones_retained = report.tombstones_retained.saturating_add(1);
                    continue;
                }

                let local_shards = self
                    .metadata
                    .list_local_shards_for_object(
                        &manifest.object_key(),
                        self.config.max_shards_per_object,
                    )
                    .await?;
                let capped = local_shards.len() >= self.config.max_shards_per_object;
                let mut active_transcode_found = false;
                for (_, record) in &local_shards {
                    if self.operation_has_active_transcode(record.operation_id, now_nanos).await? {
                        active_transcode_found = true;
                        break;
                    }
                }
                if active_transcode_found {
                    report.tombstones_retained = report.tombstones_retained.saturating_add(1);
                    continue;
                }

                for (key, mut record) in local_shards {
                    report.shards_scanned = report.shards_scanned.saturating_add(1);
                    record.state = LocalShardState::GcPending;
                    self.metadata.put_local_shard(&key, &record).await?;
                    shard_purger
                        .purge_dead_shard(EcDeadShardPurgeRequest {
                            key: key.clone(),
                            record: record.clone(),
                            erasure_set_id: manifest.erasure_set_id,
                            kind: EcDeadShardPurgeKind::Tombstone,
                        })
                        .await?;
                    self.metadata.delete_local_shard(&key).await?;
                    report.shards_purged = report.shards_purged.saturating_add(1);
                }
                if capped {
                    report.shard_scan_capped = report.shard_scan_capped.saturating_add(1);
                    report.tombstones_retained = report.tombstones_retained.saturating_add(1);
                    continue;
                }

                report.tombstones_shards_released =
                    report.tombstones_shards_released.saturating_add(1);
                if !verdict.removes_manifest() {
                    continue;
                }

                // The pool-wide retirement goes before this node drops its own
                // copy, and only on the node whose gates passed. Done the other
                // way round, a node that failed to reach its peers would have
                // removed the only copy it could still be asked about.
                if verdict == EcTombstoneGcVerdict::ReleaseShardsAndRetire {
                    metadata_coordinator.retire_tombstone_everywhere(&manifest).await?;
                    report.tombstones_retired = report.tombstones_retired.saturating_add(1);
                }

                let object = manifest.object_key();
                self.metadata.delete_object_layout(&object).await?;
                self.metadata.delete_manifest(&object).await?;
                report.tombstones_purged = report.tombstones_purged.saturating_add(1);
            }
        }
        Ok(report)
    }

    /// Remove the shards of objects that have been promoted back to RF=3.
    ///
    /// The mirror of [`run_tombstone_gc`](Self::run_tombstone_gc), and it runs
    /// on every node for the same reason: the promoted manifest is committed
    /// through the metadata quorum, but each node holds its own shards and is
    /// the only one that can release them.
    ///
    /// A promoted manifest is not a tombstone. The object is alive and readable
    /// from its RF=3 copies the whole time; what is being collected is the copy
    /// that stopped being authoritative. That is why the grace here is measured
    /// in an hour rather than in days: it only has to outlive a node that has
    /// not yet read the promoted manifest, not a node that has been offline.
    ///
    /// The sweep looks at what this node holds locally, so a node whose cached
    /// manifest still says the object is erasure coded finds nothing to do. It
    /// converges the way every other manifest divergence does, through
    /// anti-entropy reconciliation, and its shards are simply released a cycle
    /// later. Holding them a while longer costs space; releasing them from a
    /// stale opinion would cost the object.
    pub async fn run_promoted_gc<M, P>(
        &self,
        now_nanos: u64,
        metadata_coordinator: &M,
        shard_purger: &P,
    ) -> Result<EcPromotedGcReport, EcGcError>
    where
        M: EcGcMetadataCoordinator,
        P: EcDeadShardPurger,
    {
        if now_nanos == 0 {
            return Err(EcGcError::InvalidRequest {
                reason: "now_nanos must be greater than 0".to_string(),
            });
        }

        let mut report = EcPromotedGcReport::default();
        let mut cursor = None;
        loop {
            let page = self.metadata.list_manifests_page(cursor, self.config.scan_limit).await?;
            let has_next = page.next_cursor.is_some();
            cursor = page.next_cursor;

            for manifest in page.entries {
                if manifest.state != EcPlacementState::Promoted {
                    continue;
                }
                report.promoted_scanned = report.promoted_scanned.saturating_add(1);
                self.purge_one_promoted(
                    &manifest,
                    now_nanos,
                    metadata_coordinator,
                    shard_purger,
                    &mut report,
                )
                .await?;
            }

            if !has_next {
                break;
            }
        }
        Ok(report)
    }

    /// Release this node's shards for one promoted object.
    ///
    /// The step the promotion worker runs itself, so that the object it just
    /// promoted does not have to wait for the next sweep. Every gate is the
    /// sweep's, because they are the same gates.
    pub async fn purge_promoted_object<M, P>(
        &self,
        object: &EcObjectVersionKey,
        now_nanos: u64,
        metadata_coordinator: &M,
        shard_purger: &P,
    ) -> Result<EcPromotedGcReport, EcGcError>
    where
        M: EcGcMetadataCoordinator,
        P: EcDeadShardPurger,
    {
        let mut report = EcPromotedGcReport::default();
        let Some(manifest) = self.metadata.get_manifest(object).await? else {
            // Nothing to release here. Either this node never held the manifest
            // or an earlier pass already finished the job.
            return Ok(report);
        };
        if manifest.state != EcPlacementState::Promoted {
            report.promoted_retained = 1;
            return Ok(report);
        }
        report.promoted_scanned = 1;
        self.purge_one_promoted(
            &manifest,
            now_nanos,
            metadata_coordinator,
            shard_purger,
            &mut report,
        )
        .await?;
        Ok(report)
    }

    async fn purge_one_promoted<M, P>(
        &self,
        manifest: &EcObjectManifest,
        now_nanos: u64,
        metadata_coordinator: &M,
        shard_purger: &P,
        report: &mut EcPromotedGcReport,
    ) -> Result<(), EcGcError>
    where
        M: EcGcMetadataCoordinator,
        P: EcDeadShardPurger,
    {
        if !self.promoted_is_gc_safe(manifest, now_nanos, metadata_coordinator).await? {
            report.promoted_retained = report.promoted_retained.saturating_add(1);
            return Ok(());
        }

        let object = manifest.object_key();
        let local_shards = self
            .metadata
            .list_local_shards_for_object(&object, self.config.max_shards_per_object)
            .await?;
        let capped = local_shards.len() >= self.config.max_shards_per_object;

        // A transcode running against these shards would be writing into the
        // very records about to be removed. It is never this object's own
        // promotion — that finished before the manifest was published — so
        // waiting a cycle costs nothing.
        for (_, record) in &local_shards {
            if self.operation_has_active_transcode(record.operation_id, now_nanos).await? {
                report.promoted_retained = report.promoted_retained.saturating_add(1);
                return Ok(());
            }
        }

        for (key, mut record) in local_shards {
            report.shards_scanned = report.shards_scanned.saturating_add(1);
            record.state = LocalShardState::GcPending;
            self.metadata.put_local_shard(&key, &record).await?;
            shard_purger
                .purge_dead_shard(EcDeadShardPurgeRequest {
                    key: key.clone(),
                    record: record.clone(),
                    erasure_set_id: manifest.erasure_set_id,
                    kind: EcDeadShardPurgeKind::Promoted,
                })
                .await?;
            self.metadata.delete_local_shard(&key).await?;
            report.shards_purged = report.shards_purged.saturating_add(1);
        }
        if capped {
            report.shard_scan_capped = report.shard_scan_capped.saturating_add(1);
            report.promoted_retained = report.promoted_retained.saturating_add(1);
            return Ok(());
        }

        // The layout record goes first. It is what sends this node's readers to
        // the shards, and leaving it behind a deleted manifest would point them
        // at bytes that are already gone.
        self.metadata.delete_object_layout(&object).await?;
        self.metadata.delete_manifest(&object).await?;
        report.promoted_purged = report.promoted_purged.saturating_add(1);
        Ok(())
    }

    /// Run one orphan shard GC cycle.
    pub async fn run_orphan_gc<M, P>(
        &self,
        now_nanos: u64,
        metadata_coordinator: &M,
        shard_purger: &P,
    ) -> Result<EcOrphanGcReport, EcGcError>
    where
        M: EcGcMetadataCoordinator,
        P: EcDeadShardPurger,
    {
        self.run_orphan_gc_limited(now_nanos, usize::MAX, metadata_coordinator, shard_purger)
            .await
    }

    /// Run one orphan shard GC cycle, scanning at most `max_entries` records.
    pub async fn run_orphan_gc_limited<M, P>(
        &self,
        now_nanos: u64,
        max_entries: usize,
        metadata_coordinator: &M,
        shard_purger: &P,
    ) -> Result<EcOrphanGcReport, EcGcError>
    where
        M: EcGcMetadataCoordinator,
        P: EcDeadShardPurger,
    {
        if now_nanos == 0 {
            return Err(EcGcError::InvalidRequest {
                reason: "now_nanos must be greater than 0".to_string(),
            });
        }
        if max_entries == 0 {
            return Err(EcGcError::InvalidRequest {
                reason: "max_entries must be greater than 0".to_string(),
            });
        }

        let mut report = EcOrphanGcReport::default();
        let mut cursor = None;
        let mut scanned = 0usize;
        // One roster for the whole cycle. Taking it per record would let a set
        // appear halfway through and make the same pass judge two records by
        // two different pools.
        let sets = self.sets();
        loop {
            let remaining = max_entries.saturating_sub(scanned);
            if remaining == 0 {
                break;
            }
            let page_limit = self.config.scan_limit.min(remaining);
            let page = self.metadata.list_orphan_shards_page(cursor, page_limit).await?;
            let has_next = page.next_cursor.is_some();
            cursor = page.next_cursor;

            for (key, mut record) in page.entries {
                scanned = scanned.saturating_add(1);
                report.orphans_scanned = report.orphans_scanned.saturating_add(1);

                // A record naming a set this pool does not have is not this
                // pool's to collect: the shard belongs to a set nothing here can
                // reason about, and removing it would be a deletion decided
                // outside its own set. Retained and reported, so an operator
                // sees it instead of losing it.
                if sets.resolve_manifest_set(record.target_erasure_set_id).is_none() {
                    tracing::warn!(
                        operation_id = %record.operation_id,
                        erasure_set_id = record.target_erasure_set_id,
                        "retaining an orphan shard for an erasure set this pool does not have"
                    );
                    report.orphans_retained = report.orphans_retained.saturating_add(1);
                    continue;
                }

                if !self.orphan_is_gc_safe(&mut record, now_nanos, metadata_coordinator).await? {
                    self.metadata.put_orphan_shard(&key, &record).await?;
                    report.orphans_retained = report.orphans_retained.saturating_add(1);
                    continue;
                }

                record.state = EcOrphanShardState::GcPending;
                self.metadata.put_orphan_shard(&key, &record).await?;
                let local_key = LocalShardKey {
                    object: record.source_object.clone(),
                    chunk_index: record.chunk_index,
                    slot_index: record.slot_index,
                };
                let local_record = self.metadata.get_local_shard(&local_key).await?;
                shard_purger
                    .purge_dead_shard(EcDeadShardPurgeRequest {
                        key: local_key.clone(),
                        record: record.local_shard.clone(),
                        erasure_set_id: record.target_erasure_set_id,
                        kind: EcDeadShardPurgeKind::Orphan,
                    })
                    .await?;
                if local_record
                    .as_ref()
                    .is_some_and(|local| local.operation_id == record.operation_id)
                {
                    self.metadata.delete_local_shard(&local_key).await?;
                } else {
                    report.stale_orphan_records = report.stale_orphan_records.saturating_add(1);
                }
                self.metadata.delete_orphan_shard(&key).await?;
                report.orphans_purged = report.orphans_purged.saturating_add(1);
            }

            if !has_next {
                break;
            }
        }
        Ok(report)
    }

    /// Run one replicated-small physical GC cycle.
    pub async fn run_replicated_small_gc<M, S, A>(
        &self,
        now_nanos: u64,
        metadata_coordinator: &M,
        small_deleter: &S,
        propagation: &A,
        repair_frontier: &RepairFrontier,
        key_range: &KeyRange,
    ) -> Result<EcReplicatedSmallGcReport, EcGcError>
    where
        M: EcGcMetadataCoordinator,
        S: EcReplicatedSmallDeleter,
        A: EcTombstonePropagation,
    {
        if now_nanos == 0 {
            return Err(EcGcError::InvalidRequest {
                reason: "now_nanos must be greater than 0".to_string(),
            });
        }

        let mut report = EcReplicatedSmallGcReport::default();
        let mut cursor = None;
        loop {
            let page = self
                .metadata
                .list_replicated_small_gc_page(cursor, self.config.scan_limit)
                .await?;
            let has_next = page.next_cursor.is_some();
            cursor = page.next_cursor;

            for mut record in page.entries {
                report.records_scanned = report.records_scanned.saturating_add(1);
                if record.purged_at.is_some() {
                    self.finalize_replicated_small_gc_metadata(&record).await?;
                    report.records_purged = report.records_purged.saturating_add(1);
                    continue;
                }
                if now_nanos < record.eligible_at {
                    report.records_retained = report.records_retained.saturating_add(1);
                    continue;
                }
                let tombstone =
                    match metadata_coordinator.quorum_read_manifest(&record.object).await? {
                        EcManifestReadResult::Tombstone(tombstone)
                            if tombstone.operation_id == record.operation_id =>
                        {
                            tombstone
                        }
                        _ => {
                            report.records_retained = report.records_retained.saturating_add(1);
                            continue;
                        }
                    };
                if !propagation.tombstone_propagated(&tombstone).await? {
                    report.records_retained = report.records_retained.saturating_add(1);
                    continue;
                }
                if !repair_frontier.is_safe_to_purge(
                    key_range,
                    &record.tombstone_hlc,
                    &record.placement.replica_nodes,
                ) {
                    report.records_retained = report.records_retained.saturating_add(1);
                    continue;
                }

                let required_acks = replicated_small_quorum(&record.placement)?;
                let result = small_deleter
                    .purge_replicated_small(EcReplicatedSmallPurgeRequest {
                        object: record.object.clone(),
                        placement: record.placement.clone(),
                        operation_id: record.operation_id,
                        required_acks,
                    })
                    .await?;
                if result.acks < required_acks {
                    return Err(EcGcError::ReplicatedSmallQuorum {
                        required_acks,
                        actual_acks: result.acks,
                    });
                }
                record.purged_at = Some(now_nanos);
                record.updated_at = now_nanos;
                self.metadata.put_replicated_small_gc(&record).await?;
                self.finalize_replicated_small_gc_metadata(&record).await?;
                report.records_purged = report.records_purged.saturating_add(1);
            }

            if !has_next {
                break;
            }
        }
        Ok(report)
    }

    async fn resume_replicated_small_delete_if_needed<S>(
        &self,
        tombstone: &EcObjectManifest,
        small_deleter: &S,
    ) -> Result<Option<u8>, EcGcError>
    where
        S: EcReplicatedSmallDeleter,
    {
        let Some(record) = self.metadata.get_replicated_small_gc(&tombstone.object_key()).await?
        else {
            return Ok(None);
        };
        if record.operation_id != tombstone.operation_id || record.purged_at.is_some() {
            return Ok(None);
        }

        let required_acks = replicated_small_quorum(&record.placement)?;
        let result = small_deleter
            .tombstone_replicated_small(EcReplicatedSmallDeleteRequest {
                object: record.object.clone(),
                placement: record.placement,
                delete_hlc: tombstone.hlc,
                operation_id: tombstone.operation_id,
                required_acks,
            })
            .await?;
        if result.acks < required_acks {
            return Err(EcGcError::ReplicatedSmallQuorum {
                required_acks,
                actual_acks: result.acks,
            });
        }
        Ok(Some(result.acks))
    }

    async fn finalize_replicated_small_gc_metadata(
        &self,
        record: &EcReplicatedSmallGcRecord,
    ) -> Result<(), EcGcError> {
        self.metadata.delete_object_layout(&record.object).await?;
        self.metadata.delete_manifest(&record.object).await?;
        self.metadata.delete_replicated_small_gc(&record.object).await?;
        Ok(())
    }

    async fn commit_and_confirm_tombstone<M>(
        &self,
        tombstone: EcObjectManifest,
        layout: EcDeletedLayout,
        replicated_small_acks: Option<u8>,
        metadata_coordinator: &M,
    ) -> Result<EcDeleteOutcome, EcGcError>
    where
        M: EcTombstoneQuorum + ?Sized,
    {
        tombstone.validate()?;
        let required_acks = tombstone.profile.manifest_quorum();
        let commit =
            metadata_coordinator.commit_tombstone(tombstone.clone(), required_acks).await?;
        if commit.acks < required_acks {
            ec_metrics::record_manifest_quorum_failure(tombstone.erasure_set_id);
            return Err(EcGcError::ManifestQuorum {
                required_acks,
                actual_acks: commit.acks,
            });
        }
        self.metadata.put_manifest(&tombstone).await?;
        self.metadata.delete_object_layout(&tombstone.object_key()).await?;

        match metadata_coordinator.quorum_read_manifest(&tombstone.object_key()).await? {
            EcManifestReadResult::Tombstone(read) if tombstone_confirms(&tombstone, &read) => {
                self.metadata.put_manifest(&read).await?;
                Ok(EcDeleteOutcome::Deleted {
                    layout,
                    tombstone: Box::new(read),
                    manifest_acks: commit.acks,
                    replicated_small_acks,
                })
            }
            EcManifestReadResult::Present(live) if live.hlc >= tombstone.hlc => {
                self.metadata.put_manifest(&live).await?;
                Ok(EcDeleteOutcome::SupersededByLive { live_hlc: live.hlc })
            }
            EcManifestReadResult::Present(live) => Err(EcGcError::TombstoneNotConfirmed {
                reason: format!(
                    "quorum read returned older live manifest at HLC {}",
                    live.hlc
                ),
            }),
            EcManifestReadResult::Tombstone(read) => Err(EcGcError::TombstoneNotConfirmed {
                reason: format!(
                    "quorum read returned divergent tombstone operation {} at HLC {}",
                    read.operation_id, read.hlc
                ),
            }),
            EcManifestReadResult::Absent => Err(EcGcError::TombstoneNotConfirmed {
                reason: "quorum read did not find the committed tombstone".to_string(),
            }),
        }
    }

    /// What this node may do with one tombstone right now.
    ///
    /// Three answers rather than two, because "the pool no longer has this
    /// tombstone" is not the same as "it is too early to touch it". A node that
    /// was down while another node retired the tombstone comes back holding
    /// shards of an object the pool has finished collecting, and refusing it
    /// there would strand exactly the shards this sweep exists to release.
    async fn tombstone_gc_verdict<M, A>(
        &self,
        tombstone: &EcObjectManifest,
        now_nanos: u64,
        metadata_coordinator: &M,
        safety: &EcTombstoneGcSafety<'_, A>,
    ) -> Result<EcTombstoneGcVerdict, EcGcError>
    where
        M: EcGcMetadataCoordinator,
        A: EcTombstonePropagation,
    {
        if now_nanos.saturating_sub(tombstone.created_at) <= self.config.gc_grace_nanos() {
            return Ok(EcTombstoneGcVerdict::Retain);
        }
        match metadata_coordinator.quorum_read_manifest(&tombstone.object_key()).await? {
            EcManifestReadResult::Tombstone(read) if tombstone_confirms(tombstone, &read) => {}
            // Retired cluster-wide while this node was not looking. Reachable
            // only for a tombstone this node already holds, so "absent" cannot
            // mean "never committed"; it means the object finished being
            // collected and what is left here is the last copy of its shards.
            EcManifestReadResult::Absent => {
                return Ok(EcTombstoneGcVerdict::ReleaseShardsAlreadyRetired);
            }
            // A live manifest at this key is a newer generation of the object,
            // written after the delete. Its shards share the local keys of the
            // ones this tombstone describes, so releasing them here would take
            // the new object with them.
            _ => return Ok(EcTombstoneGcVerdict::Retain),
        }
        if !safety.propagation.tombstone_propagated(tombstone).await? {
            return Ok(EcTombstoneGcVerdict::Retain);
        }
        if !safety.repair_frontier.is_safe_to_purge(
            safety.key_range,
            &tombstone.hlc,
            safety.shard_owners,
        ) {
            return Ok(EcTombstoneGcVerdict::Retain);
        }
        let retire_after = self
            .config
            .gc_grace_nanos()
            .saturating_add(self.config.tombstone_retire_grace_nanos());
        if now_nanos.saturating_sub(tombstone.created_at) <= retire_after {
            return Ok(EcTombstoneGcVerdict::ReleaseShards);
        }
        Ok(EcTombstoneGcVerdict::ReleaseShardsAndRetire)
    }

    /// Whether a promoted object's shards may go.
    ///
    /// Two gates, and both are about other nodes rather than about this one.
    /// The quorum read confirms that the promotion is what the pool agrees on,
    /// so a manifest promoted by a node that then lost the race cannot take
    /// shards with it. The grace gives every node time to notice, because a
    /// node still reading through erasure coding has no other way to find out.
    async fn promoted_is_gc_safe<M>(
        &self,
        promoted: &EcObjectManifest,
        now_nanos: u64,
        metadata_coordinator: &M,
    ) -> Result<bool, EcGcError>
    where
        M: EcGcMetadataCoordinator,
    {
        if now_nanos.saturating_sub(promoted.created_at) <= self.config.promoted_grace_nanos() {
            return Ok(false);
        }
        match metadata_coordinator.quorum_read_manifest(&promoted.object_key()).await? {
            EcManifestReadResult::Present(read) => Ok(read.state == EcPlacementState::Promoted
                && read.operation_id == promoted.operation_id),
            // The marker is transient: the node that carried the promotion out
            // removes it from the metadata quorum once its own shards are gone.
            // A node still holding shards at that point reaches this branch, and
            // refusing here would strand exactly the shards the sweep exists to
            // release. It is only reachable for a manifest this node already
            // holds as promoted, so "absent" cannot mean "not committed yet".
            EcManifestReadResult::Absent => Ok(true),
            // A tombstone means the object was deleted while the promotion was
            // settling. Its own GC path owns the shards from here, and running
            // both would have two passes removing the same records.
            EcManifestReadResult::Tombstone(_) => Ok(false),
        }
    }

    async fn orphan_is_gc_safe<M>(
        &self,
        record: &mut EcOrphanShardRecord,
        now_nanos: u64,
        metadata_coordinator: &M,
    ) -> Result<bool, EcGcError>
    where
        M: EcGcMetadataCoordinator,
    {
        if now_nanos.saturating_sub(record.created_at) <= self.config.orphan_min_age_nanos() {
            return Ok(false);
        }
        if committed_manifest_references_operation(
            metadata_coordinator.quorum_read_manifest(&record.source_object).await?,
            record.operation_id,
        ) {
            return Ok(false);
        }
        if self.operation_has_active_transcode(record.operation_id, now_nanos).await? {
            return Ok(false);
        }
        self.update_orphan_lease_observation(record).await?;
        if !self.orphan_lease_grace_elapsed(record, now_nanos) {
            return Ok(false);
        }
        Ok(true)
    }

    async fn operation_has_active_transcode(
        &self,
        operation_id: Uuid,
        now_nanos: u64,
    ) -> Result<bool, EcGcError> {
        if let Some(lease) = self.metadata.get_transcode_lease(operation_id).await? {
            if now_nanos <= lease.expires_at.saturating_add(self.config.orphan_lease_grace_nanos())
            {
                return Ok(true);
            }
        }
        let Some(job) = self.metadata.get_transcode_job(operation_id).await? else {
            return Ok(false);
        };
        Ok(transcode_job_owns_operation(&job))
    }

    async fn update_orphan_lease_observation(
        &self,
        record: &mut EcOrphanShardRecord,
    ) -> Result<(), EcGcError> {
        if let Some(lease) = self.metadata.get_transcode_lease(record.operation_id).await? {
            record.lease_epoch = lease.acquired_at;
            record.last_lease_seen_at = Some(lease.last_heartbeat_at);
        }
        Ok(())
    }

    fn orphan_lease_grace_elapsed(&self, record: &EcOrphanShardRecord, now_nanos: u64) -> bool {
        let last_seen = record
            .last_lease_seen_at
            .unwrap_or(record.created_at)
            .saturating_add(self.config.transcode_lease_nanos());
        now_nanos > last_seen.saturating_add(self.config.orphan_lease_grace_nanos())
    }
}

fn tombstone_from_live_manifest(
    request: &EcDeleteRequest,
    live: &EcObjectManifest,
) -> Result<EcObjectManifest, EcGcError> {
    let mut tombstone = live.clone();
    tombstone.chunks.clear();
    tombstone.state = EcPlacementState::Tombstone;
    tombstone.hlc = request.delete_hlc;
    tombstone.origin_node_id = request.origin_node_id;
    tombstone.operation_id = request.operation_id;
    tombstone.created_at = request.now_nanos;
    tombstone.is_deleted = true;
    tombstone.validate()?;
    Ok(tombstone)
}

fn tombstone_from_small_placement(
    request: &EcDeleteRequest,
    placement: &ReplicatedSmallObjectPlacement,
) -> Result<EcObjectManifest, EcGcError> {
    let tombstone = EcObjectManifest {
        format_version: EC_MANIFEST_FORMAT_VERSION,
        bucket: request.object.bucket.clone(),
        key: request.object.key.clone(),
        version_id: request.object.version_id.clone(),
        original_size: placement.size,
        content_hash_sha256: placement.content_hash_sha256,
        etag: String::new(),
        content_type: String::new(),
        user_metadata: Default::default(),
        profile: request.erasure_set.profile.codec_snapshot(),
        erasure_set_id: request.erasure_set.set_id,
        topology_epoch: request.erasure_set.topology_epoch,
        chunks: Vec::new(),
        state: EcPlacementState::Tombstone,
        hlc: request.delete_hlc,
        origin_node_id: request.origin_node_id,
        operation_id: request.operation_id,
        created_at: request.now_nanos,
        is_deleted: true,
        packed: None,
    };
    tombstone.validate()?;
    Ok(tombstone)
}

fn tombstone_from_packed_placement(
    request: &EcDeleteRequest,
    placement: &PackedSegmentObjectPlacement,
) -> Result<EcObjectManifest, EcGcError> {
    let tombstone = EcObjectManifest {
        format_version: EC_MANIFEST_FORMAT_VERSION,
        bucket: request.object.bucket.clone(),
        key: request.object.key.clone(),
        version_id: request.object.version_id.clone(),
        original_size: placement.length,
        content_hash_sha256: placement.content_hash_sha256,
        etag: String::new(),
        content_type: String::new(),
        user_metadata: Default::default(),
        profile: request.erasure_set.profile.codec_snapshot(),
        erasure_set_id: request.erasure_set.set_id,
        topology_epoch: request.erasure_set.topology_epoch,
        chunks: Vec::new(),
        state: EcPlacementState::Tombstone,
        hlc: request.delete_hlc,
        origin_node_id: request.origin_node_id,
        operation_id: request.operation_id,
        created_at: request.now_nanos,
        is_deleted: true,
        packed: None,
    };
    tombstone.validate()?;
    Ok(tombstone)
}

fn tombstone_confirms(expected: &EcObjectManifest, actual: &EcObjectManifest) -> bool {
    manifest_is_tombstone(actual)
        && actual.object_key() == expected.object_key()
        && actual.operation_id == expected.operation_id
        && actual.hlc == expected.hlc
}

fn committed_manifest_references_operation(
    result: EcManifestReadResult,
    operation_id: Uuid,
) -> bool {
    match result {
        EcManifestReadResult::Present(manifest) => {
            !manifest_is_tombstone(&manifest)
                && manifest.operation_id == operation_id
                && manifest.state.is_ec_authoritative()
        }
        EcManifestReadResult::Tombstone(_) | EcManifestReadResult::Absent => false,
    }
}

fn transcode_job_owns_operation(job: &EcTranscodeJobRecord) -> bool {
    matches!(
        job.state,
        EcPlacementState::EcTranscodeQueued
            | EcPlacementState::EcEncoding
            | EcPlacementState::EcShardsDurable
            | EcPlacementState::EcManifestCommitted
            | EcPlacementState::EcReadVerified
            | EcPlacementState::EcFailedRetryable
    )
}

fn manifest_is_tombstone(manifest: &EcObjectManifest) -> bool {
    manifest.is_deleted || manifest.state == EcPlacementState::Tombstone
}

fn replicated_small_quorum(placement: &ReplicatedSmallObjectPlacement) -> Result<u8, EcGcError> {
    placement.validate()?;
    let quorum = (placement.replica_nodes.len() / 2) + 1;
    u8::try_from(quorum).map_err(|_| EcGcError::InvalidRequest {
        reason: "replicated-small quorum does not fit in u8".to_string(),
    })
}

fn seconds_to_nanos(seconds: u64) -> u64 {
    seconds.saturating_mul(1_000_000_000)
}

fn parse_optional_u64(name: &'static str) -> Result<Option<u64>, EcGcError> {
    match std::env::var(name) {
        Ok(value) => value.parse::<u64>().map(Some).map_err(|error| EcGcError::InvalidConfig {
            name,
            reason: error.to_string(),
        }),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(EcGcError::InvalidConfig {
            name,
            reason: error.to_string(),
        }),
    }
}

/// Present a packed-segment invariant failure as a repack failure.
fn packed_repack_error(error: PackedSegmentError) -> EcGcError {
    EcGcError::PackedRepack {
        reason: error.to_string(),
    }
}

fn parse_optional_f64(name: &'static str) -> Result<Option<f64>, EcGcError> {
    match std::env::var(name) {
        Ok(value) => value.parse::<f64>().map(Some).map_err(|error| EcGcError::InvalidConfig {
            name,
            reason: error.to_string(),
        }),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(EcGcError::InvalidConfig {
            name,
            reason: error.to_string(),
        }),
    }
}

fn parse_optional_usize(name: &'static str) -> Result<Option<usize>, EcGcError> {
    match std::env::var(name) {
        Ok(value) => value.parse::<usize>().map(Some).map_err(|error| EcGcError::InvalidConfig {
            name,
            reason: error.to_string(),
        }),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(EcGcError::InvalidConfig {
            name,
            reason: error.to_string(),
        }),
    }
}

/// Errors returned by EC Phase 8 GC code.
#[derive(Debug, Error)]
pub enum EcGcError {
    /// GC configuration is invalid.
    #[error("invalid EC GC config {name}: {reason}")]
    InvalidConfig {
        /// Environment variable or config key.
        name: &'static str,
        /// Human-readable reason.
        reason: String,
    },

    /// Caller supplied an invalid request.
    #[error("invalid EC GC request: {reason}")]
    InvalidRequest {
        /// Human-readable reason.
        reason: String,
    },

    /// A repack could not move an object out of its segment.
    #[error("EC packed segment repack failed: {reason}")]
    PackedRepack {
        /// Human-readable reason.
        reason: String,
    },

    /// Metadata quorum did not ACK enough tombstone replicas.
    #[error("EC tombstone quorum requires {required_acks} ACK(s), got {actual_acks}")]
    ManifestQuorum {
        /// Required metadata ACKs.
        required_acks: u8,
        /// Actual metadata ACKs.
        actual_acks: u8,
    },

    /// RF=3 small-object delete/purge did not ACK enough replicas.
    #[error("EC replicated-small quorum requires {required_acks} ACK(s), got {actual_acks}")]
    ReplicatedSmallQuorum {
        /// Required RF ACKs.
        required_acks: u8,
        /// Actual RF ACKs.
        actual_acks: u8,
    },

    /// Tombstone was not visible through quorum after commit.
    #[error("EC tombstone was not quorum-confirmed: {reason}")]
    TombstoneNotConfirmed {
        /// Human-readable reason.
        reason: String,
    },

    /// EC metadata operation failed.
    #[error(transparent)]
    Metadata(#[from] EcMetadataError),
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::{HashMap, HashSet};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    use crate::ec_packed_segments::{PackedObjectIdentity, PackedSegmentConfig, SealReason};

    use crate::ec_metadata::EcTranscodeOrigin;

    use crate::ec_metadata::{
        ChunkManifest, EcObjectManifestRef, EcTranscodeJobRecord, EcTranscodeTarget, ShardManifest,
    };
    use crate::erasure_coding::{
        legacy_pool_set_id, EcCodecKind, EcParity, EcProfile, EcTopologyPolicy, TopologyLabels,
        DEFAULT_CHUNK_SIZE, DEFAULT_MIN_OBJECT_SIZE,
    };

    fn node(index: u8) -> NodeId {
        NodeId(u128::from(index))
    }

    fn nodes(count: u8) -> Vec<NodeId> {
        (1..=count).map(node).collect()
    }

    fn hlc(wall_time: u64, logical: u32) -> HlcTimestamp {
        HlcTimestamp {
            wall_time,
            logical,
            node_id: 1,
        }
    }

    fn operation_id(value: u128) -> Uuid {
        Uuid::from_u128(value)
    }

    fn object(version_id: Option<&str>) -> EcObjectVersionKey {
        EcObjectVersionKey::new(
            "photos",
            "2026/raw.bin",
            version_id.map(ToString::to_string),
        )
    }

    fn active_set() -> ErasureSet {
        let profile = EcProfile {
            name: "ec-rs-small".to_string(),
            codec: EcCodecKind::ReedSolomon,
            k: 4,
            parity: EcParity::ReedSolomon { m: 2 },
            min_object_size: DEFAULT_MIN_OBJECT_SIZE,
            chunk_size: DEFAULT_CHUNK_SIZE,
        };
        let nodes = nodes(profile.total_shards());
        ErasureSet::active(
            s4_cluster::PoolId::new(9),
            // Kept at the value EC v1 derived from the pool so the fixtures that
            // name `erasure_set_id: 9` keep meaning the same set.
            legacy_pool_set_id(s4_cluster::PoolId::new(9)),
            profile,
            &nodes,
            &HashMap::<NodeId, TopologyLabels>::new(),
            EcTopologyPolicy::Strict,
            3,
            1,
        )
        .unwrap()
    }

    /// The pool of the fixtures: one inherited set, exactly as EC v1 had.
    fn test_sets() -> Arc<EcErasureSets> {
        Arc::new(EcErasureSets::new(
            ErasureSetRegistry::from_sets(s4_cluster::PoolId::new(9), vec![active_set()]).unwrap(),
        ))
    }

    fn live_manifest(version_id: Option<&str>, operation_id: Uuid) -> EcObjectManifest {
        let profile = active_set().profile.codec_snapshot();
        let shards = (0..profile.total_shards())
            .map(|slot_index| ShardManifest {
                slot_index,
                node_id: node(slot_index + 1),
                role: profile.role_for_slot(slot_index).unwrap(),
                crc32: u32::from(slot_index),
                sha256: [slot_index; 32],
                size: 8,
            })
            .collect();

        EcObjectManifest {
            format_version: EC_MANIFEST_FORMAT_VERSION,
            bucket: "photos".to_string(),
            key: "2026/raw.bin".to_string(),
            version_id: version_id.map(ToString::to_string),
            original_size: 16,
            content_hash_sha256: [7; 32],
            etag: "etag".to_string(),
            content_type: "application/octet-stream".to_string(),
            user_metadata: HashMap::new(),
            profile,
            erasure_set_id: 9,
            topology_epoch: 3,
            chunks: vec![ChunkManifest {
                chunk_index: 0,
                data_size: 16,
                encoded_size: 8,
                chunk_sha256: [8; 32],
                shards,
            }],
            state: EcPlacementState::EcCommitted,
            hlc: hlc(10, 0),
            origin_node_id: node(1),
            operation_id,
            created_at: 1_000,
            is_deleted: false,
            packed: None,
        }
    }

    fn tombstone_manifest(version_id: Option<&str>, operation_id: Uuid) -> EcObjectManifest {
        let mut manifest = live_manifest(version_id, operation_id);
        manifest.chunks.clear();
        manifest.state = EcPlacementState::Tombstone;
        manifest.hlc = hlc(20, 0);
        manifest.created_at = 1_000;
        manifest.is_deleted = true;
        manifest
    }

    fn promoted_manifest(version_id: Option<&str>, operation_id: Uuid) -> EcObjectManifest {
        let mut manifest = live_manifest(version_id, operation_id);
        manifest.state = EcPlacementState::Promoted;
        // Kept, unlike a tombstone's: the shards still exist and this is what
        // says where they are until GC removes them.
        assert!(!manifest.chunks.is_empty());
        manifest.created_at = 1_000;
        manifest
    }

    fn local_shard_record(operation_id: Uuid) -> LocalShardRecord {
        LocalShardRecord {
            volume_id: 1,
            offset: 10,
            size: 8,
            crc32: 1,
            sha256: [1; 32],
            state: LocalShardState::Durable,
            operation_id,
        }
    }

    /// Every tombstone the store holds, the way a sweep now hands them in.
    ///
    /// The walk moved to the caller (decision EC2-202), so the tests do it too
    /// rather than pretending the coordinator still scans for itself.
    async fn local_tombstones(store: &EcMetadataStore) -> Vec<EcObjectManifest> {
        store
            .list_manifests_page(None, 256)
            .await
            .unwrap()
            .entries
            .into_iter()
            .filter(manifest_is_tombstone)
            .collect()
    }

    fn gc_config() -> EcGcConfig {
        EcGcConfig {
            gc_grace_secs: 1,
            transcode_lease_secs: 1,
            orphan_lease_grace_secs: 1,
            orphan_min_age_secs: 1,
            scan_limit: 32,
            max_shards_per_object: 64,
            promoted_grace_secs: 1,
            tombstone_scan_budget: 256,
            tombstone_retire_grace_secs: 1,
            packed_retire_grace_secs: 1,
        }
    }

    struct MemoryMetadataCoordinator {
        layout: Mutex<EcObjectLayoutReadResult>,
        manifest: Mutex<EcManifestReadResult>,
        commits: Mutex<Vec<(EcObjectManifest, u8)>>,
        retired: Mutex<Vec<EcObjectVersionKey>>,
        acks: u8,
        confirm_commits: bool,
    }

    impl MemoryMetadataCoordinator {
        fn new(layout: EcObjectLayoutReadResult, manifest: EcManifestReadResult, acks: u8) -> Self {
            Self {
                layout: Mutex::new(layout),
                manifest: Mutex::new(manifest),
                commits: Mutex::new(Vec::new()),
                retired: Mutex::new(Vec::new()),
                acks,
                confirm_commits: true,
            }
        }

        fn new_without_commit_confirmation(
            layout: EcObjectLayoutReadResult,
            manifest: EcManifestReadResult,
            acks: u8,
        ) -> Self {
            Self {
                layout: Mutex::new(layout),
                manifest: Mutex::new(manifest),
                commits: Mutex::new(Vec::new()),
                retired: Mutex::new(Vec::new()),
                acks,
                confirm_commits: false,
            }
        }
    }

    #[async_trait]
    impl EcGcMetadataCoordinator for MemoryMetadataCoordinator {
        async fn quorum_read_object_layout(
            &self,
            _object: &EcObjectVersionKey,
        ) -> Result<EcObjectLayoutReadResult, EcGcError> {
            Ok(self.layout.lock().unwrap().clone())
        }

        async fn retire_tombstone_everywhere(
            &self,
            tombstone: &EcObjectManifest,
        ) -> Result<(), EcGcError> {
            self.retired.lock().unwrap().push(tombstone.object_key());
            *self.manifest.lock().unwrap() = EcManifestReadResult::Absent;
            Ok(())
        }
    }

    #[async_trait]
    impl EcTombstoneQuorum for MemoryMetadataCoordinator {
        async fn quorum_read_manifest(
            &self,
            _object: &EcObjectVersionKey,
        ) -> Result<EcManifestReadResult, EcGcError> {
            Ok(self.manifest.lock().unwrap().clone())
        }

        async fn commit_tombstone(
            &self,
            tombstone: EcObjectManifest,
            required_acks: u8,
        ) -> Result<EcTombstoneCommitResult, EcGcError> {
            self.commits.lock().unwrap().push((tombstone.clone(), required_acks));
            if self.confirm_commits {
                *self.manifest.lock().unwrap() = EcManifestReadResult::Tombstone(tombstone);
            }
            Ok(EcTombstoneCommitResult { acks: self.acks })
        }
    }

    #[derive(Default)]
    struct MemorySmallDeleter {
        delete_requests: Mutex<Vec<EcReplicatedSmallDeleteRequest>>,
        purge_requests: Mutex<Vec<EcReplicatedSmallPurgeRequest>>,
        delete_acks: u8,
        purge_acks: u8,
    }

    #[async_trait]
    impl EcReplicatedSmallDeleter for MemorySmallDeleter {
        async fn tombstone_replicated_small(
            &self,
            request: EcReplicatedSmallDeleteRequest,
        ) -> Result<EcReplicatedSmallDeleteResult, EcGcError> {
            self.delete_requests.lock().unwrap().push(request);
            Ok(EcReplicatedSmallDeleteResult {
                acks: self.delete_acks,
            })
        }

        async fn purge_replicated_small(
            &self,
            request: EcReplicatedSmallPurgeRequest,
        ) -> Result<EcReplicatedSmallPurgeResult, EcGcError> {
            self.purge_requests.lock().unwrap().push(request);
            Ok(EcReplicatedSmallPurgeResult {
                acks: self.purge_acks,
            })
        }
    }

    #[derive(Default)]
    struct MemoryShardPurger {
        requests: Mutex<Vec<EcDeadShardPurgeRequest>>,
    }

    impl MemoryShardPurger {
        fn purges(&self) -> Vec<EcDeadShardPurgeRequest> {
            self.requests.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl EcDeadShardPurger for MemoryShardPurger {
        async fn purge_dead_shard(
            &self,
            request: EcDeadShardPurgeRequest,
        ) -> Result<(), EcGcError> {
            self.requests.lock().unwrap().push(request);
            Ok(())
        }
    }

    struct Propagation(bool);

    #[async_trait]
    impl EcTombstonePropagation for Propagation {
        async fn tombstone_propagated(
            &self,
            _tombstone: &EcObjectManifest,
        ) -> Result<bool, EcGcError> {
            Ok(self.0)
        }
    }

    fn full_frontier(nodes: &[NodeId]) -> RepairFrontier {
        let mut frontier = RepairFrontier::new();
        let range = KeyRange::full();
        for node in nodes {
            frontier.advance(&range, *node, hlc(100, 0));
        }
        frontier
    }

    #[tokio::test]
    async fn delete_commits_tombstone_and_removes_local_layout() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let live = live_manifest(Some("v1"), operation_id(1));
        let object = live.object_key();
        let reference = EcObjectManifestRef::from_manifest(&live).unwrap();
        store
            .put_object_layout(&object, &ObjectDataLayout::ErasureCoded(reference.clone()))
            .await
            .unwrap();
        let coordinator = MemoryMetadataCoordinator::new(
            EcObjectLayoutReadResult::Present(ObjectDataLayout::ErasureCoded(reference)),
            EcManifestReadResult::Present(live.clone()),
            live.profile.manifest_quorum(),
        );
        let gc = EcGcCoordinator::new(store.clone(), gc_config(), test_sets());

        let outcome = gc
            .delete_object(
                EcDeleteRequest {
                    object: object.clone(),
                    erasure_set: active_set(),
                    delete_hlc: hlc(30, 0),
                    operation_id: operation_id(2),
                    origin_node_id: node(1),
                    now_nanos: 2_000,
                },
                &coordinator,
                &MemorySmallDeleter::default(),
            )
            .await
            .unwrap();

        assert!(matches!(
            outcome,
            EcDeleteOutcome::Deleted {
                layout: EcDeletedLayout::ErasureCoded,
                ..
            }
        ));
        assert!(store.get_object_layout(&object).await.unwrap().is_none());
        assert!(matches!(
            store.get_manifest(&object).await.unwrap().unwrap().into_read_result(),
            EcManifestReadResult::Tombstone(_)
        ));
        assert_eq!(coordinator.commits.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn orphan_gc_honors_age_and_lease_grace_before_purge() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let gc = EcGcCoordinator::new(store.clone(), gc_config(), test_sets());
        let object = object(Some("v1"));
        let operation_id = operation_id(10);
        let local_key = LocalShardKey {
            object: object.clone(),
            chunk_index: 0,
            slot_index: 0,
        };
        let local_record = local_shard_record(operation_id);
        store.put_local_shard(&local_key, &local_record).await.unwrap();
        gc.record_orphan_shard(EcOrphanShardObservation {
            key: local_key.clone(),
            record: local_record,
            target_erasure_set_id: 9,
            observed_at: 1_000_000_000,
            reason: "test orphan".to_string(),
        })
        .await
        .unwrap();
        let coordinator = MemoryMetadataCoordinator::new(
            EcObjectLayoutReadResult::Absent,
            EcManifestReadResult::Absent,
            1,
        );
        let purger = MemoryShardPurger::default();

        let young = gc.run_orphan_gc(1_500_000_000, &coordinator, &purger).await.unwrap();
        assert_eq!(young.orphans_retained, 1);
        assert_eq!(purger.requests.lock().unwrap().len(), 0);

        let old = gc.run_orphan_gc(4_000_000_001, &coordinator, &purger).await.unwrap();
        assert_eq!(old.orphans_purged, 1);
        assert_eq!(purger.requests.lock().unwrap().len(), 1);
        assert!(store.get_local_shard(&local_key).await.unwrap().is_none());
        assert!(store.list_orphan_shards(10).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn orphan_gc_limited_honors_max_entries() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let gc = EcGcCoordinator::new(store.clone(), gc_config(), test_sets());
        for slot_index in 0..3 {
            let object = object(Some(&format!("v{slot_index}")));
            let local_key = LocalShardKey {
                object,
                chunk_index: 0,
                slot_index,
            };
            let local_record = local_shard_record(operation_id(u128::from(slot_index) + 100));
            store.put_local_shard(&local_key, &local_record).await.unwrap();
            gc.record_orphan_shard(EcOrphanShardObservation {
                key: local_key,
                record: local_record,
                target_erasure_set_id: 9,
                observed_at: 1_000_000_000,
                reason: "test orphan".to_string(),
            })
            .await
            .unwrap();
        }
        let coordinator = MemoryMetadataCoordinator::new(
            EcObjectLayoutReadResult::Absent,
            EcManifestReadResult::Absent,
            1,
        );
        let purger = MemoryShardPurger::default();

        let report =
            gc.run_orphan_gc_limited(4_000_000_001, 2, &coordinator, &purger).await.unwrap();

        assert_eq!(report.orphans_scanned, 2);
        assert_eq!(report.orphans_purged, 2);
        assert_eq!(purger.requests.lock().unwrap().len(), 2);
        assert_eq!(store.list_orphan_shards(10).await.unwrap().len(), 1);
    }

    /// Deleting is not writing.
    ///
    /// A set an operator has taken out of the write rotation still owns the
    /// objects already in it. Refusing their tombstones would make those objects
    /// undeletable, which is why only a retired set is turned away.
    #[test]
    fn a_set_that_stopped_taking_writes_still_accepts_deletes() {
        let request = |state: ErasureSetState| {
            let mut erasure_set = active_set();
            erasure_set.state = state;
            EcDeleteRequest {
                object: object(None),
                erasure_set,
                delete_hlc: hlc(30, 0),
                operation_id: operation_id(77),
                origin_node_id: node(1),
                now_nanos: 1_000,
            }
        };

        request(ErasureSetState::Active).validate().unwrap();
        request(ErasureSetState::Draining).validate().unwrap();
        request(ErasureSetState::ReadOnly).validate().unwrap();

        let error = request(ErasureSetState::Retired).validate().unwrap_err();
        assert!(error.to_string().contains("Retired"), "{error}");
    }

    /// An orphan naming a set this pool does not have is left alone.
    ///
    /// Its shard belongs to a set nothing here can reason about, so removing it
    /// would be a deletion decided outside the shard's own set. Retained and
    /// counted, so an operator sees it rather than losing it.
    #[tokio::test]
    async fn an_orphan_of_an_unknown_set_is_retained_rather_than_purged() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let gc = EcGcCoordinator::new(store.clone(), gc_config(), test_sets());
        let mine = operation_id(41);
        let foreign = operation_id(42);
        for (operation, target_erasure_set_id, slot_index) in
            [(mine, 9u64, 0u8), (foreign, 4_242u64, 1u8)]
        {
            gc.record_orphan_shard(EcOrphanShardObservation {
                key: LocalShardKey {
                    object: object(Some("v1")),
                    chunk_index: 0,
                    slot_index,
                },
                record: local_shard_record(operation),
                target_erasure_set_id,
                observed_at: 1_000_000_000,
                reason: "test orphan".to_string(),
            })
            .await
            .unwrap();
        }
        let coordinator = MemoryMetadataCoordinator::new(
            EcObjectLayoutReadResult::Absent,
            EcManifestReadResult::Absent,
            1,
        );
        let purger = MemoryShardPurger::default();

        let report = gc.run_orphan_gc(10_000_000_000, &coordinator, &purger).await.unwrap();

        assert_eq!(report.orphans_scanned, 2);
        assert_eq!(report.orphans_purged, 1);
        assert_eq!(report.orphans_retained, 1);

        let purged = purger.requests.lock().unwrap().clone();
        assert_eq!(purged.len(), 1);
        assert_eq!(purged[0].erasure_set_id, 9);
        assert_eq!(purged[0].record.operation_id, mine);

        // The unknown set's record is still on disk, waiting for an operator.
        let left = store.list_orphan_shards(10).await.unwrap();
        assert_eq!(left.len(), 1);
        assert_eq!(left[0].1.target_erasure_set_id, 4_242);
    }

    #[tokio::test]
    async fn orphan_gc_does_not_delete_committed_live_operation() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let gc = EcGcCoordinator::new(store.clone(), gc_config(), test_sets());
        let operation_id = operation_id(11);
        let object = object(Some("v1"));
        let local_key = LocalShardKey {
            object: object.clone(),
            chunk_index: 0,
            slot_index: 0,
        };
        let local_record = local_shard_record(operation_id);
        gc.record_orphan_shard(EcOrphanShardObservation {
            key: local_key,
            record: local_record,
            target_erasure_set_id: 9,
            observed_at: 1_000_000_000,
            reason: "test orphan".to_string(),
        })
        .await
        .unwrap();
        let coordinator = MemoryMetadataCoordinator::new(
            EcObjectLayoutReadResult::Absent,
            EcManifestReadResult::Present(live_manifest(Some("v1"), operation_id)),
            1,
        );

        let report = gc
            .run_orphan_gc(10_000_000_000, &coordinator, &MemoryShardPurger::default())
            .await
            .unwrap();

        assert_eq!(report.orphans_retained, 1);
        assert_eq!(report.orphans_purged, 0);
    }

    /// A promoted object releases its shards, but only after the grace period.
    ///
    /// The grace is what covers a node that has not yet read the promoted
    /// manifest and is still answering reads from those shards.
    #[tokio::test]
    async fn promoted_gc_waits_out_the_grace_then_releases_the_shards() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let gc = EcGcCoordinator::new(store.clone(), gc_config(), test_sets());
        let promoted = promoted_manifest(None, operation_id(30));
        let object = promoted.object_key();
        store.put_manifest(&promoted).await.unwrap();
        let shard = LocalShardKey {
            object: object.clone(),
            chunk_index: 0,
            slot_index: 0,
        };
        store
            .put_local_shard(&shard, &local_shard_record(operation_id(31)))
            .await
            .unwrap();
        let coordinator = MemoryMetadataCoordinator::new(
            EcObjectLayoutReadResult::Absent,
            EcManifestReadResult::Present(promoted.clone()),
            1,
        );
        let purger = MemoryShardPurger::default();

        // Inside the grace: nothing moves.
        let held = gc
            .run_promoted_gc(promoted.created_at + 500_000_000, &coordinator, &purger)
            .await
            .unwrap();
        assert_eq!(held.promoted_retained, 1);
        assert_eq!(held.shards_purged, 0);
        assert!(store.get_local_shard(&shard).await.unwrap().is_some());

        // Past it: the shards and the manifest go.
        let purged = gc
            .run_promoted_gc(promoted.created_at + 5_000_000_000, &coordinator, &purger)
            .await
            .unwrap();
        assert_eq!(purged.promoted_purged, 1);
        assert_eq!(purged.shards_purged, 1);
        assert!(store.get_local_shard(&shard).await.unwrap().is_none());
        assert!(store.get_manifest(&object).await.unwrap().is_none());
        assert_eq!(
            purger.purges().first().map(|request| request.kind),
            Some(EcDeadShardPurgeKind::Promoted)
        );
    }

    /// The pool has to agree that the object was promoted.
    ///
    /// A node that promoted an object and then lost the race must not take the
    /// shards with it: the quorum manifest is what says which promotion stands.
    #[tokio::test]
    async fn promoted_gc_keeps_shards_the_pool_still_calls_erasure_coded() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let gc = EcGcCoordinator::new(store.clone(), gc_config(), test_sets());
        let promoted = promoted_manifest(None, operation_id(40));
        let object = promoted.object_key();
        store.put_manifest(&promoted).await.unwrap();
        let shard = LocalShardKey {
            object,
            chunk_index: 0,
            slot_index: 0,
        };
        store
            .put_local_shard(&shard, &local_shard_record(operation_id(41)))
            .await
            .unwrap();

        // The pool still reports the object as erasure coded.
        let coordinator = MemoryMetadataCoordinator::new(
            EcObjectLayoutReadResult::Absent,
            EcManifestReadResult::Present(live_manifest(None, operation_id(40))),
            1,
        );

        let report = gc
            .run_promoted_gc(
                promoted.created_at + 5_000_000_000,
                &coordinator,
                &MemoryShardPurger::default(),
            )
            .await
            .unwrap();

        assert_eq!(report.promoted_retained, 1);
        assert_eq!(report.shards_purged, 0);
        assert!(store.get_local_shard(&shard).await.unwrap().is_some());
    }

    /// A collected marker still lets a straggler release its shards.
    ///
    /// The node that carried the promotion out removes the marker from the
    /// metadata quorum once its own shards are gone. A node that had not swept
    /// by then reads "absent" — and refusing on that answer would strand
    /// exactly the shards this pass exists to release. The branch is only
    /// reachable for a manifest this node already holds as promoted, so
    /// "absent" cannot be confused with "not committed yet".
    #[tokio::test]
    async fn promoted_gc_releases_shards_after_the_marker_was_collected() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let gc = EcGcCoordinator::new(store.clone(), gc_config(), test_sets());
        let promoted = promoted_manifest(None, operation_id(70));
        let object = promoted.object_key();
        store.put_manifest(&promoted).await.unwrap();
        let shard = LocalShardKey {
            object: object.clone(),
            chunk_index: 0,
            slot_index: 0,
        };
        store
            .put_local_shard(&shard, &local_shard_record(operation_id(71)))
            .await
            .unwrap();

        // The promoting node has already taken the marker out of the quorum.
        let coordinator = MemoryMetadataCoordinator::new(
            EcObjectLayoutReadResult::Absent,
            EcManifestReadResult::Absent,
            1,
        );

        let report = gc
            .run_promoted_gc(
                promoted.created_at + 5_000_000_000,
                &coordinator,
                &MemoryShardPurger::default(),
            )
            .await
            .unwrap();

        assert_eq!(report.shards_purged, 1);
        assert_eq!(report.promoted_purged, 1);
        assert!(store.get_local_shard(&shard).await.unwrap().is_none());
    }

    /// An object whose marker is already collected is not an error to purge.
    ///
    /// Two passes collect the same local manifest — the promotion job's own
    /// step and the per-cycle sweep every node runs — and either may get there
    /// first. A caller that treated "nothing left to do" as failure would never
    /// finish the job, which is the step that has to take the marker out of the
    /// metadata quorum.
    #[tokio::test]
    async fn purging_an_already_collected_promotion_reports_nothing_and_succeeds() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let gc = EcGcCoordinator::new(store.clone(), gc_config(), test_sets());
        let object = object(None);
        let coordinator = MemoryMetadataCoordinator::new(
            EcObjectLayoutReadResult::Absent,
            EcManifestReadResult::Absent,
            1,
        );

        let report = gc
            .purge_promoted_object(
                &object,
                5_000_000_000,
                &coordinator,
                &MemoryShardPurger::default(),
            )
            .await
            .expect("a collected marker is not a failure");

        assert_eq!(report.promoted_purged, 0);
        assert_eq!(report.promoted_scanned, 0);
        assert_eq!(report.shards_purged, 0);
    }

    /// A delete that arrived during the promotion owns the shards from there.
    ///
    /// Both passes would otherwise remove the same records, and the tombstone
    /// path applies the stricter gates of the two.
    #[tokio::test]
    async fn promoted_gc_defers_to_a_tombstone() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let gc = EcGcCoordinator::new(store.clone(), gc_config(), test_sets());
        let promoted = promoted_manifest(None, operation_id(50));
        store.put_manifest(&promoted).await.unwrap();
        let coordinator = MemoryMetadataCoordinator::new(
            EcObjectLayoutReadResult::Absent,
            EcManifestReadResult::Tombstone(tombstone_manifest(None, operation_id(50))),
            1,
        );

        let report = gc
            .run_promoted_gc(
                promoted.created_at + 5_000_000_000,
                &coordinator,
                &MemoryShardPurger::default(),
            )
            .await
            .unwrap();

        assert_eq!(report.promoted_retained, 1);
        assert_eq!(report.promoted_purged, 0);
    }

    /// The object-scoped purge answers only about the object it was given.
    #[tokio::test]
    async fn purging_one_promoted_object_leaves_its_neighbours_alone() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let gc = EcGcCoordinator::new(store.clone(), gc_config(), test_sets());
        let promoted = promoted_manifest(None, operation_id(60));
        let object = promoted.object_key();
        store.put_manifest(&promoted).await.unwrap();

        let other = live_manifest(Some("v9"), operation_id(61));
        store.put_manifest(&other).await.unwrap();

        let coordinator = MemoryMetadataCoordinator::new(
            EcObjectLayoutReadResult::Absent,
            EcManifestReadResult::Present(promoted.clone()),
            1,
        );

        let report = gc
            .purge_promoted_object(
                &object,
                promoted.created_at + 5_000_000_000,
                &coordinator,
                &MemoryShardPurger::default(),
            )
            .await
            .unwrap();

        assert_eq!(report.promoted_purged, 1);
        assert!(store.get_manifest(&object).await.unwrap().is_none());
        assert!(store.get_manifest(&other.object_key()).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn tombstone_gc_waits_for_frontier_then_purges_only_matching_version() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let gc = EcGcCoordinator::new(store.clone(), gc_config(), test_sets());
        let tombstone = tombstone_manifest(Some("v1"), operation_id(20));
        let object_v1 = tombstone.object_key();
        let object_v2 = object(Some("v2"));
        store.put_manifest(&tombstone).await.unwrap();
        let shard_v1 = LocalShardKey {
            object: object_v1.clone(),
            chunk_index: 0,
            slot_index: 0,
        };
        let shard_v2 = LocalShardKey {
            object: object_v2.clone(),
            chunk_index: 0,
            slot_index: 0,
        };
        store
            .put_local_shard(&shard_v1, &local_shard_record(operation_id(21)))
            .await
            .unwrap();
        store
            .put_local_shard(&shard_v2, &local_shard_record(operation_id(22)))
            .await
            .unwrap();
        let coordinator = MemoryMetadataCoordinator::new(
            EcObjectLayoutReadResult::Tombstone(Box::new(tombstone.clone())),
            EcManifestReadResult::Tombstone(tombstone),
            1,
        );
        let purger = MemoryShardPurger::default();
        let owners = nodes(6);
        let propagation = Propagation(true);
        let range = KeyRange::full();
        let empty_frontier = RepairFrontier::new();

        let retained = gc
            .run_tombstone_gc(
                3_000_000_001,
                &local_tombstones(&store).await,
                &coordinator,
                &purger,
                EcTombstoneGcSafety {
                    propagation: &propagation,
                    repair_frontier: &empty_frontier,
                    key_range: &range,
                    shard_owners: &owners,
                },
            )
            .await
            .unwrap();
        assert_eq!(retained.tombstones_retained, 1);
        assert!(store.get_local_shard(&shard_v1).await.unwrap().is_some());

        let advanced_frontier = full_frontier(&owners);
        let purged = gc
            .run_tombstone_gc(
                3_000_000_001,
                &local_tombstones(&store).await,
                &coordinator,
                &purger,
                EcTombstoneGcSafety {
                    propagation: &propagation,
                    repair_frontier: &advanced_frontier,
                    key_range: &range,
                    shard_owners: &owners,
                },
            )
            .await
            .unwrap();
        assert_eq!(purged.tombstones_purged, 1);
        assert_eq!(purged.shards_purged, 1);
        assert!(store.get_local_shard(&shard_v1).await.unwrap().is_none());
        assert!(store.get_local_shard(&shard_v2).await.unwrap().is_some());

        // The purge names the set the tombstone belongs to. Collection is
        // decided from one set's metadata, and the request has to say which.
        let requests = purger.requests.lock().unwrap().clone();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].erasure_set_id, active_set().set_id);
    }

    /// EC2-202: the report is about what the cycle was handed, and it adds up.
    ///
    /// The coordinator no longer walks the keyspace, so "scanned" is the number
    /// of deletes the caller asked it about — not a count that grows with every
    /// erasure set the pool has. A live manifest handed in by mistake decides
    /// nothing and is not counted as decided.
    #[tokio::test]
    async fn the_tombstone_report_counts_only_the_deletes_it_was_handed() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let gc = EcGcCoordinator::new(store.clone(), gc_config(), test_sets());
        let tombstone = tombstone_manifest(Some("v1"), operation_id(20));
        let live = live_manifest(Some("v2"), operation_id(21));
        store.put_manifest(&tombstone).await.unwrap();
        store.put_manifest(&live).await.unwrap();
        let coordinator = MemoryMetadataCoordinator::new(
            EcObjectLayoutReadResult::Tombstone(Box::new(tombstone.clone())),
            EcManifestReadResult::Tombstone(tombstone.clone()),
            1,
        );
        let purger = MemoryShardPurger::default();
        let owners = nodes(6);
        let propagation = Propagation(true);
        let range = KeyRange::full();
        let frontier = full_frontier(&owners);

        let report = gc
            .run_tombstone_gc(
                3_000_000_001,
                &[live, tombstone],
                &coordinator,
                &purger,
                EcTombstoneGcSafety {
                    propagation: &propagation,
                    repair_frontier: &frontier,
                    key_range: &range,
                    shard_owners: &owners,
                },
            )
            .await
            .unwrap();

        assert_eq!(
            report.tombstones_scanned, 1,
            "the live manifest decided nothing"
        );
        assert!(
            report.tombstones_retained + report.tombstones_shards_released
                <= report.tombstones_scanned,
            "a report where the parts outgrow the whole cannot be read: {report:?}"
        );
    }

    #[tokio::test]
    async fn tombstone_gc_releases_shards_but_keeps_the_tombstone_until_its_own_grace() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let mut config = gc_config();
        // One second to be sure the delete reached everybody, then a hundred
        // before the tombstone itself may go.
        config.tombstone_retire_grace_secs = 100;
        let gc = EcGcCoordinator::new(store.clone(), config, test_sets());
        let tombstone = tombstone_manifest(Some("v1"), operation_id(20));
        let object = tombstone.object_key();
        store.put_manifest(&tombstone).await.unwrap();
        let shard = LocalShardKey {
            object: object.clone(),
            chunk_index: 0,
            slot_index: 0,
        };
        store
            .put_local_shard(&shard, &local_shard_record(operation_id(21)))
            .await
            .unwrap();
        let coordinator = MemoryMetadataCoordinator::new(
            EcObjectLayoutReadResult::Tombstone(Box::new(tombstone.clone())),
            EcManifestReadResult::Tombstone(tombstone),
            1,
        );
        let purger = MemoryShardPurger::default();
        let owners = nodes(6);
        let propagation = Propagation(true);
        let range = KeyRange::full();
        let frontier = full_frontier(&owners);

        let report = gc
            .run_tombstone_gc(
                3_000_000_001,
                &local_tombstones(&store).await,
                &coordinator,
                &purger,
                EcTombstoneGcSafety {
                    propagation: &propagation,
                    repair_frontier: &frontier,
                    key_range: &range,
                    shard_owners: &owners,
                },
            )
            .await
            .unwrap();

        // The shards are this node's to release, and they go.
        assert_eq!(report.shards_purged, 1);
        assert_eq!(report.tombstones_shards_released, 1);
        assert!(store.get_local_shard(&shard).await.unwrap().is_none());
        // The tombstone is the pool's, and it stays until every other owner
        // has had a sweep of its own.
        assert_eq!(report.tombstones_retired, 0);
        assert_eq!(report.tombstones_purged, 0);
        assert!(store.get_manifest(&object).await.unwrap().is_some());
        assert!(coordinator.retired.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn tombstone_gc_collects_shards_of_a_tombstone_the_pool_has_already_retired() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let gc = EcGcCoordinator::new(store.clone(), gc_config(), test_sets());
        let tombstone = tombstone_manifest(Some("v1"), operation_id(20));
        let object = tombstone.object_key();
        store.put_manifest(&tombstone).await.unwrap();
        let shard = LocalShardKey {
            object: object.clone(),
            chunk_index: 0,
            slot_index: 0,
        };
        store
            .put_local_shard(&shard, &local_shard_record(operation_id(21)))
            .await
            .unwrap();
        // The node was away while another owner retired the tombstone, so the
        // pool answers "no such object" for a delete this node still holds.
        let coordinator = MemoryMetadataCoordinator::new(
            EcObjectLayoutReadResult::Absent,
            EcManifestReadResult::Absent,
            1,
        );
        let purger = MemoryShardPurger::default();
        let owners = nodes(6);
        // Neither gate can be met any more: there is no tombstone left on the
        // other owners to confirm, and the frontier was never advanced.
        let propagation = Propagation(false);
        let range = KeyRange::full();
        let empty_frontier = RepairFrontier::new();

        let report = gc
            .run_tombstone_gc(
                3_000_000_001,
                &local_tombstones(&store).await,
                &coordinator,
                &purger,
                EcTombstoneGcSafety {
                    propagation: &propagation,
                    repair_frontier: &empty_frontier,
                    key_range: &range,
                    shard_owners: &owners,
                },
            )
            .await
            .unwrap();

        assert_eq!(report.shards_purged, 1);
        assert_eq!(report.tombstones_purged, 1);
        // Nothing to retire: it is already gone from the pool.
        assert_eq!(report.tombstones_retired, 0);
        assert!(store.get_local_shard(&shard).await.unwrap().is_none());
        assert!(store.get_manifest(&object).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn tombstone_gc_retires_the_tombstone_from_the_whole_pool_in_one_step() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let gc = EcGcCoordinator::new(store.clone(), gc_config(), test_sets());
        let tombstone = tombstone_manifest(Some("v1"), operation_id(20));
        let object = tombstone.object_key();
        store.put_manifest(&tombstone).await.unwrap();
        let shard = LocalShardKey {
            object: object.clone(),
            chunk_index: 0,
            slot_index: 0,
        };
        store
            .put_local_shard(&shard, &local_shard_record(operation_id(21)))
            .await
            .unwrap();
        let coordinator = MemoryMetadataCoordinator::new(
            EcObjectLayoutReadResult::Tombstone(Box::new(tombstone.clone())),
            EcManifestReadResult::Tombstone(tombstone),
            1,
        );
        let purger = MemoryShardPurger::default();
        let owners = nodes(6);
        let propagation = Propagation(true);
        let range = KeyRange::full();
        let frontier = full_frontier(&owners);

        let report = gc
            .run_tombstone_gc(
                3_000_000_001,
                &local_tombstones(&store).await,
                &coordinator,
                &purger,
                EcTombstoneGcSafety {
                    propagation: &propagation,
                    repair_frontier: &frontier,
                    key_range: &range,
                    shard_owners: &owners,
                },
            )
            .await
            .unwrap();

        assert_eq!(report.tombstones_retired, 1);
        assert_eq!(report.tombstones_purged, 1);
        assert_eq!(
            coordinator.retired.lock().unwrap().as_slice(),
            std::slice::from_ref(&object)
        );
        assert!(store.get_manifest(&object).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn tombstone_gc_waits_when_any_local_shard_operation_is_active() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let gc = EcGcCoordinator::new(store.clone(), gc_config(), test_sets());
        let tombstone = tombstone_manifest(Some("v1"), operation_id(25));
        let object = tombstone.object_key();
        let shard_operation = operation_id(26);
        store.put_manifest(&tombstone).await.unwrap();
        store
            .put_local_shard(
                &LocalShardKey {
                    object: object.clone(),
                    chunk_index: 0,
                    slot_index: 0,
                },
                &local_shard_record(shard_operation),
            )
            .await
            .unwrap();
        store
            .put_transcode_job(&EcTranscodeJobRecord {
                object: object.clone(),
                operation_id: shard_operation,
                source_hlc: hlc(10, 0),
                target_erasure_set_id: 9,
                state: EcPlacementState::EcEncoding,
                attempt: 1,
                object_size: 16,
                source_content_hash_sha256: [7; 32],
                updated_at: 2_000_000_000,
                next_retry_at: None,
                failure: None,
                origin: EcTranscodeOrigin::ClientWrite,
                target: EcTranscodeTarget::Object,
            })
            .await
            .unwrap();
        let coordinator = MemoryMetadataCoordinator::new(
            EcObjectLayoutReadResult::Tombstone(Box::new(tombstone.clone())),
            EcManifestReadResult::Tombstone(tombstone),
            1,
        );
        let purger = MemoryShardPurger::default();
        let owners = nodes(6);
        let propagation = Propagation(true);
        let range = KeyRange::full();
        let frontier = full_frontier(&owners);

        let report = gc
            .run_tombstone_gc(
                3_000_000_001,
                &local_tombstones(&store).await,
                &coordinator,
                &purger,
                EcTombstoneGcSafety {
                    propagation: &propagation,
                    repair_frontier: &frontier,
                    key_range: &range,
                    shard_owners: &owners,
                },
            )
            .await
            .unwrap();

        assert_eq!(report.tombstones_retained, 1);
        assert_eq!(report.tombstones_purged, 0);
        assert_eq!(purger.requests.lock().unwrap().len(), 0);
        assert!(store.get_manifest(&object).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn replicated_small_delete_and_gc_are_separate_from_ec_shards() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let gc = EcGcCoordinator::new(store.clone(), gc_config(), test_sets());
        let placement = ReplicatedSmallObjectPlacement::new(
            vec![node(1), node(2), node(3)],
            [3; 32],
            12,
            hlc(10, 0),
        )
        .unwrap();
        let object = EcObjectVersionKey::new("photos", "small.txt", None);
        let coordinator = MemoryMetadataCoordinator::new(
            EcObjectLayoutReadResult::Present(ObjectDataLayout::ReplicatedSmall(placement.clone())),
            EcManifestReadResult::Absent,
            active_set().profile.manifest_quorum(),
        );
        let small = MemorySmallDeleter {
            delete_acks: 2,
            purge_acks: 2,
            ..Default::default()
        };

        let outcome = gc
            .delete_object(
                EcDeleteRequest {
                    object: object.clone(),
                    erasure_set: active_set(),
                    delete_hlc: hlc(30, 0),
                    operation_id: operation_id(30),
                    origin_node_id: node(1),
                    now_nanos: 1_000,
                },
                &coordinator,
                &small,
            )
            .await
            .unwrap();
        assert!(matches!(
            outcome,
            EcDeleteOutcome::Deleted {
                layout: EcDeletedLayout::ReplicatedSmall,
                replicated_small_acks: Some(2),
                ..
            }
        ));
        assert_eq!(small.delete_requests.lock().unwrap().len(), 1);
        assert!(store.get_replicated_small_gc(&object).await.unwrap().is_some());

        let report = gc
            .run_replicated_small_gc(
                2_000_000_001,
                &coordinator,
                &small,
                &Propagation(true),
                &full_frontier(&placement.replica_nodes),
                &KeyRange::full(),
            )
            .await
            .unwrap();
        assert_eq!(report.records_purged, 1);
        assert_eq!(small.purge_requests.lock().unwrap().len(), 1);
        assert!(store.get_replicated_small_gc(&object).await.unwrap().is_none());
        assert!(store.get_manifest(&object).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn replicated_small_delete_resumes_after_tombstone_commit_failure_boundary() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let gc = EcGcCoordinator::new(store.clone(), gc_config(), test_sets());
        let placement = ReplicatedSmallObjectPlacement::new(
            vec![node(1), node(2), node(3)],
            [3; 32],
            12,
            hlc(10, 0),
        )
        .unwrap();
        let object = EcObjectVersionKey::new("photos", "small.txt", None);
        let coordinator = MemoryMetadataCoordinator::new(
            EcObjectLayoutReadResult::Present(ObjectDataLayout::ReplicatedSmall(placement)),
            EcManifestReadResult::Absent,
            active_set().profile.manifest_quorum(),
        );
        let failing_small = MemorySmallDeleter {
            delete_acks: 1,
            purge_acks: 2,
            ..Default::default()
        };
        let request = EcDeleteRequest {
            object: object.clone(),
            erasure_set: active_set(),
            delete_hlc: hlc(30, 0),
            operation_id: operation_id(33),
            origin_node_id: node(1),
            now_nanos: 1_000,
        };

        let error = gc
            .delete_object(request.clone(), &coordinator, &failing_small)
            .await
            .unwrap_err();
        assert!(matches!(error, EcGcError::ReplicatedSmallQuorum { .. }));
        assert!(store.get_replicated_small_gc(&object).await.unwrap().is_some());

        let tombstone = store.get_manifest(&object).await.unwrap().unwrap();
        let retry_coordinator = MemoryMetadataCoordinator::new(
            EcObjectLayoutReadResult::Tombstone(Box::new(tombstone.clone())),
            EcManifestReadResult::Tombstone(tombstone),
            active_set().profile.manifest_quorum(),
        );
        let resumed_small = MemorySmallDeleter {
            delete_acks: 2,
            purge_acks: 2,
            ..Default::default()
        };

        let outcome = gc.delete_object(request, &retry_coordinator, &resumed_small).await.unwrap();

        assert!(matches!(outcome, EcDeleteOutcome::AlreadyDeleted { .. }));
        assert_eq!(resumed_small.delete_requests.lock().unwrap().len(), 1);
        assert!(store.get_replicated_small_gc(&object).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn replicated_small_physical_delete_is_skipped_when_tombstone_loses() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let gc = EcGcCoordinator::new(store.clone(), gc_config(), test_sets());
        let placement = ReplicatedSmallObjectPlacement::new(
            vec![node(1), node(2), node(3)],
            [3; 32],
            12,
            hlc(10, 0),
        )
        .unwrap();
        let object = EcObjectVersionKey::new("photos", "small.txt", None);
        let mut live = live_manifest(None, operation_id(31));
        live.key = "small.txt".to_string();
        live.hlc = hlc(40, 0);
        let coordinator = MemoryMetadataCoordinator::new_without_commit_confirmation(
            EcObjectLayoutReadResult::Present(ObjectDataLayout::ReplicatedSmall(placement)),
            EcManifestReadResult::Present(live),
            active_set().profile.manifest_quorum(),
        );
        let small = MemorySmallDeleter {
            delete_acks: 2,
            purge_acks: 2,
            ..Default::default()
        };

        let outcome = gc
            .delete_object(
                EcDeleteRequest {
                    object: object.clone(),
                    erasure_set: active_set(),
                    delete_hlc: hlc(30, 0),
                    operation_id: operation_id(32),
                    origin_node_id: node(1),
                    now_nanos: 1_000,
                },
                &coordinator,
                &small,
            )
            .await
            .unwrap();

        assert!(matches!(outcome, EcDeleteOutcome::SupersededByLive { .. }));
        assert_eq!(small.delete_requests.lock().unwrap().len(), 0);
        assert!(store.get_replicated_small_gc(&object).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn replicated_small_gc_finalizes_records_already_marked_purged() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let gc = EcGcCoordinator::new(store.clone(), gc_config(), test_sets());
        let placement = ReplicatedSmallObjectPlacement::new(
            vec![node(1), node(2), node(3)],
            [3; 32],
            12,
            hlc(10, 0),
        )
        .unwrap();
        let object = EcObjectVersionKey::new("photos", "small.txt", None);
        let tombstone = tombstone_from_small_placement(
            &EcDeleteRequest {
                object: object.clone(),
                erasure_set: active_set(),
                delete_hlc: hlc(30, 0),
                operation_id: operation_id(34),
                origin_node_id: node(1),
                now_nanos: 1_000,
            },
            &placement,
        )
        .unwrap();
        store.put_manifest(&tombstone).await.unwrap();
        store
            .put_replicated_small_gc(&EcReplicatedSmallGcRecord {
                object: object.clone(),
                operation_id: operation_id(34),
                placement: placement.clone(),
                tombstone_hlc: hlc(30, 0),
                eligible_at: 1_000,
                purged_at: Some(2_000),
                updated_at: 2_000,
            })
            .await
            .unwrap();
        let coordinator = MemoryMetadataCoordinator::new(
            EcObjectLayoutReadResult::Tombstone(Box::new(tombstone.clone())),
            EcManifestReadResult::Tombstone(tombstone),
            active_set().profile.manifest_quorum(),
        );
        let small = MemorySmallDeleter {
            delete_acks: 2,
            purge_acks: 2,
            ..Default::default()
        };

        let report = gc
            .run_replicated_small_gc(
                3_000,
                &coordinator,
                &small,
                &Propagation(true),
                &full_frontier(&placement.replica_nodes),
                &KeyRange::full(),
            )
            .await
            .unwrap();

        assert_eq!(report.records_purged, 1);
        assert_eq!(small.purge_requests.lock().unwrap().len(), 0);
        assert!(store.get_replicated_small_gc(&object).await.unwrap().is_none());
        assert!(store.get_manifest(&object).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn active_transcode_job_keeps_orphan_from_gc() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let gc = EcGcCoordinator::new(store.clone(), gc_config(), test_sets());
        let operation_id = operation_id(40);
        let object = object(Some("v1"));
        let local_key = LocalShardKey {
            object: object.clone(),
            chunk_index: 0,
            slot_index: 0,
        };
        gc.record_orphan_shard(EcOrphanShardObservation {
            key: local_key,
            record: local_shard_record(operation_id),
            target_erasure_set_id: 9,
            observed_at: 1_000_000_000,
            reason: "test orphan".to_string(),
        })
        .await
        .unwrap();
        store
            .put_transcode_job(&EcTranscodeJobRecord {
                object,
                operation_id,
                source_hlc: hlc(10, 0),
                target_erasure_set_id: 9,
                state: EcPlacementState::EcFailedRetryable,
                attempt: 1,
                object_size: 16,
                source_content_hash_sha256: [7; 32],
                updated_at: 2_000_000_000,
                next_retry_at: Some(10_000_000_000),
                failure: None,
                origin: EcTranscodeOrigin::ClientWrite,
                target: EcTranscodeTarget::Object,
            })
            .await
            .unwrap();
        let coordinator = MemoryMetadataCoordinator::new(
            EcObjectLayoutReadResult::Absent,
            EcManifestReadResult::Absent,
            1,
        );

        let report = gc
            .run_orphan_gc(10_000_000_001, &coordinator, &MemoryShardPurger::default())
            .await
            .unwrap();

        assert_eq!(report.orphans_retained, 1);
        assert_eq!(report.orphans_purged, 0);
    }

    #[derive(Default)]
    struct MemoryPackedProbe {
        calls: Mutex<Vec<(PackedSegmentId, EcObjectVersionKey)>>,
        degraded_objects: Mutex<HashSet<EcObjectVersionKey>>,
        failing_objects: Mutex<HashSet<EcObjectVersionKey>>,
        store_to_verify: Option<Arc<EcMetadataStore>>,
        clock: Option<Arc<AtomicUsize>>,
        call_order: Mutex<Vec<(usize, EcObjectVersionKey)>>,
    }

    impl MemoryPackedProbe {
        fn with_degraded(self, object: EcObjectVersionKey) -> Self {
            self.degraded_objects.lock().unwrap().insert(object);
            self
        }

        fn with_failure(self, object: EcObjectVersionKey) -> Self {
            self.failing_objects.lock().unwrap().insert(object);
            self
        }

        fn with_store_verification(mut self, store: Arc<EcMetadataStore>) -> Self {
            self.store_to_verify = Some(store);
            self
        }

        fn with_clock(mut self, clock: Arc<AtomicUsize>) -> Self {
            self.clock = Some(clock);
            self
        }

        fn calls(&self) -> Vec<(PackedSegmentId, EcObjectVersionKey)> {
            self.calls.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl EcPackedSegmentProbe for MemoryPackedProbe {
        async fn probe_packed_object(
            &self,
            segment_id: PackedSegmentId,
            entry: &PackedSegmentEntry,
        ) -> Result<EcPackedProbeOutcome, EcGcError> {
            if let Some(clock) = &self.clock {
                let tick = clock.fetch_add(1, Ordering::SeqCst);
                self.call_order.lock().unwrap().push((tick, entry.object.clone()));
            }

            self.calls.lock().unwrap().push((segment_id, entry.object.clone()));

            if let Some(store) = &self.store_to_verify {
                let layout = store
                    .get_object_layout(&entry.object)
                    .await
                    .expect("metadata read failed in test probe")
                    .expect("layout should exist during probe");
                match layout {
                    ObjectDataLayout::PackedSegment(placement) => {
                        assert!(
                            !placement.hot_replica_nodes.is_empty(),
                            "hot replicas were prematurely cleared for {:?} before probe completed",
                            entry.object
                        );
                    }
                    other => panic!("expected PackedSegment layout, got {other:?}"),
                }
            }

            if self.failing_objects.lock().unwrap().contains(&entry.object) {
                return Err(EcGcError::InvalidRequest {
                    reason: format!("simulated probe failure for {:?}", entry.object),
                });
            }

            let degraded = self.degraded_objects.lock().unwrap().contains(&entry.object);

            Ok(EcPackedProbeOutcome { degraded })
        }
    }

    #[derive(Default)]
    struct MemoryPointerCommitter {
        commits: Mutex<Vec<EcObjectVersionKey>>,
        failing_objects: Mutex<HashSet<EcObjectVersionKey>>,
    }

    impl MemoryPointerCommitter {
        fn with_failure(self, object: EcObjectVersionKey) -> Self {
            self.failing_objects.lock().unwrap().insert(object);
            self
        }

        fn commits(&self) -> Vec<EcObjectVersionKey> {
            self.commits.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl EcPackedPointerCommitter for MemoryPointerCommitter {
        async fn commit_packed_pointer(
            &self,
            _segment: &PackedSegment,
            entry: &PackedSegmentEntry,
        ) -> Result<(), EcGcError> {
            if self.failing_objects.lock().unwrap().contains(&entry.object) {
                return Err(EcGcError::InvalidRequest {
                    reason: format!("simulated pointer commit failure for {:?}", entry.object),
                });
            }
            self.commits.lock().unwrap().push(entry.object.clone());
            Ok(())
        }
    }

    #[derive(Default)]
    struct MemoryHotCopyPurger {
        purges: Mutex<Vec<(EcObjectVersionKey, HlcTimestamp)>>,
        failing_objects: Mutex<HashSet<EcObjectVersionKey>>,
        clock: Option<Arc<AtomicUsize>>,
        call_order: Mutex<Vec<(usize, EcObjectVersionKey)>>,
    }

    impl MemoryHotCopyPurger {
        #[allow(dead_code)]
        fn with_failure(self, object: EcObjectVersionKey) -> Self {
            self.failing_objects.lock().unwrap().insert(object);
            self
        }

        fn with_clock(mut self, clock: Arc<AtomicUsize>) -> Self {
            self.clock = Some(clock);
            self
        }

        fn purges(&self) -> Vec<(EcObjectVersionKey, HlcTimestamp)> {
            self.purges.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl EcPackedHotCopyPurger for MemoryHotCopyPurger {
        async fn purge_packed_hot_copies(
            &self,
            object: &EcObjectVersionKey,
            expected_source_hlc: HlcTimestamp,
        ) -> Result<(), EcGcError> {
            if let Some(clock) = &self.clock {
                let tick = clock.fetch_add(1, Ordering::SeqCst);
                self.call_order.lock().unwrap().push((tick, object.clone()));
            }

            if self.failing_objects.lock().unwrap().contains(object) {
                return Err(EcGcError::InvalidRequest {
                    reason: format!("simulated purge failure for {object:?}"),
                });
            }

            self.purges.lock().unwrap().push((object.clone(), expected_source_hlc));
            Ok(())
        }
    }

    async fn prepare_encoded_packed_segment_fixture(
        object_count: usize,
        state: PackedSegmentState,
    ) -> (
        tempfile::TempDir,
        Arc<EcMetadataStore>,
        EcGcCoordinator,
        PackedSegment,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let gc = EcGcCoordinator::new(store.clone(), gc_config(), test_sets());

        let segment_id = PackedSegmentId::new_random();
        let config = PackedSegmentConfig {
            enabled: true,
            target_bytes: 64 * 1024 * 1024,
            max_age: std::time::Duration::from_secs(300),
            max_objects: 1000,
            max_open_segments: 4,
        };

        let mut segment = PackedSegment::open(segment_id, active_set().set_id, &config, 1_000);

        for i in 0..object_count {
            let obj = EcObjectVersionKey::new("photos", format!("packed-{i}.bin"), None);
            let object_length = 128u64;
            let object_hlc = hlc(100 + i as u64, 0);
            let object_hash = [i as u8 + 1; 32];

            let reservation = segment.reserve(object_length).unwrap();
            let identity = PackedObjectIdentity::new(obj.clone(), object_hash, object_hlc);
            let item = segment.journal_append(&reservation, identity).unwrap();
            segment.commit(&reservation, &item).unwrap();
        }

        segment.seal(2_000, SealReason::TargetSizeReached).unwrap();
        if state == PackedSegmentState::Encoded {
            segment.transition_to(PackedSegmentState::Encoded).unwrap();
        } else if state != PackedSegmentState::Sealed {
            panic!("unsupported initial test segment state: {state:?}");
        }

        store.put_packed_segment(&segment).await.unwrap();

        let replica_nodes = vec![node(1), node(2), node(3)];
        for entry in &segment.entries {
            let placement =
                PackedSegmentObjectPlacement::from_entry(&segment, entry, replica_nodes.clone())
                    .unwrap();
            store
                .put_object_layout(&entry.object, &ObjectDataLayout::PackedSegment(placement))
                .await
                .unwrap();
        }

        (dir, store, gc, segment)
    }

    #[tokio::test]
    async fn hot_copies_are_released_only_after_every_object_reads_from_ec() {
        let (_dir, store, gc, segment) =
            prepare_encoded_packed_segment_fixture(3, PackedSegmentState::Encoded).await;
        let probe = MemoryPackedProbe::default();
        let purger = MemoryHotCopyPurger::default();
        let committer = MemoryPointerCommitter::default();

        let release = gc
            .release_packed_segment_hot_copies(&segment, &probe, &committer, &purger)
            .await
            .unwrap();

        assert_eq!(release.segment_id, segment.segment_id);
        assert_eq!(release.probed, 3);
        assert_eq!(release.degraded_probes, 0);
        assert_eq!(release.switched, 3);
        assert_eq!(release.purged, 3);

        for entry in &segment.entries {
            let layout = store
                .get_object_layout(&entry.object)
                .await
                .unwrap()
                .expect("layout must exist");
            match layout {
                ObjectDataLayout::PackedSegment(placement) => {
                    assert!(
                        placement.hot_replica_nodes.is_empty(),
                        "hot replicas must be empty for {:?}",
                        entry.object
                    );
                }
                other => panic!("expected PackedSegment layout, got {other:?}"),
            }
        }

        let stored_segment = store
            .get_packed_segment(segment.segment_id)
            .await
            .unwrap()
            .expect("segment must exist in store");
        assert_eq!(stored_segment.state, PackedSegmentState::Purged);

        let purged = purger.purges();
        assert_eq!(purged.len(), 3);
        for entry in &segment.entries {
            let occurrences = purged
                .iter()
                .filter(|(obj, hlc)| obj == &entry.object && *hlc == entry.hlc)
                .count();
            assert_eq!(
                occurrences, 1,
                "purger log must contain {:?} exactly once with its entry.hlc",
                entry.object
            );
        }
    }

    #[tokio::test]
    async fn a_failed_probe_leaves_the_segment_exactly_as_it_was() {
        let (_dir, store, gc, segment) =
            prepare_encoded_packed_segment_fixture(3, PackedSegmentState::Encoded).await;
        let second_object = segment.entries[1].object.clone();
        let probe = MemoryPackedProbe::default().with_failure(second_object);
        let purger = MemoryHotCopyPurger::default();
        let committer = MemoryPointerCommitter::default();

        let result = gc
            .release_packed_segment_hot_copies(&segment, &probe, &committer, &purger)
            .await;
        assert!(result.is_err(), "expected release to fail on probe error");

        for entry in &segment.entries {
            let layout = store
                .get_object_layout(&entry.object)
                .await
                .unwrap()
                .expect("layout must exist");
            match layout {
                ObjectDataLayout::PackedSegment(placement) => {
                    assert_eq!(
                        placement.hot_replica_nodes,
                        vec![node(1), node(2), node(3)],
                        "hot replicas must remain intact for {:?}",
                        entry.object
                    );
                }
                other => panic!("expected PackedSegment layout, got {other:?}"),
            }
        }

        let stored_segment = store
            .get_packed_segment(segment.segment_id)
            .await
            .unwrap()
            .expect("segment must exist in store");
        assert_eq!(stored_segment.state, PackedSegmentState::Encoded);

        assert!(
            purger.purges().is_empty(),
            "purger log must be empty when probe fails"
        );
    }

    #[tokio::test]
    async fn a_probe_that_needed_reconstruction_does_not_block_the_release() {
        let (_dir, store, gc, segment) =
            prepare_encoded_packed_segment_fixture(3, PackedSegmentState::Encoded).await;
        let degraded_obj = segment.entries[0].object.clone();
        let probe = MemoryPackedProbe::default().with_degraded(degraded_obj);
        let purger = MemoryHotCopyPurger::default();
        let committer = MemoryPointerCommitter::default();

        let release = gc
            .release_packed_segment_hot_copies(&segment, &probe, &committer, &purger)
            .await
            .unwrap();

        assert_eq!(release.segment_id, segment.segment_id);
        assert_eq!(release.probed, 3);
        assert_eq!(release.degraded_probes, 1);
        assert_eq!(release.switched, 3);
        assert_eq!(release.purged, 3);

        for entry in &segment.entries {
            let layout = store
                .get_object_layout(&entry.object)
                .await
                .unwrap()
                .expect("layout must exist");
            match layout {
                ObjectDataLayout::PackedSegment(placement) => {
                    assert!(
                        placement.hot_replica_nodes.is_empty(),
                        "hot replicas must be empty for {:?}",
                        entry.object
                    );
                }
                other => panic!("expected PackedSegment layout, got {other:?}"),
            }
        }

        let stored_segment = store
            .get_packed_segment(segment.segment_id)
            .await
            .unwrap()
            .expect("segment must exist in store");
        assert_eq!(stored_segment.state, PackedSegmentState::Purged);

        let purged = purger.purges();
        assert_eq!(purged.len(), 3);
        for entry in &segment.entries {
            let occurrences = purged
                .iter()
                .filter(|(obj, hlc)| obj == &entry.object && *hlc == entry.hlc)
                .count();
            assert_eq!(
                occurrences, 1,
                "purger log must contain {:?} exactly once with its entry.hlc",
                entry.object
            );
        }
    }

    #[tokio::test]
    async fn releasing_a_segment_that_is_not_encoded_is_refused() {
        let (_dir, store, gc, segment) =
            prepare_encoded_packed_segment_fixture(3, PackedSegmentState::Sealed).await;
        let probe = MemoryPackedProbe::default();
        let purger = MemoryHotCopyPurger::default();
        let committer = MemoryPointerCommitter::default();

        let err = gc
            .release_packed_segment_hot_copies(&segment, &probe, &committer, &purger)
            .await
            .unwrap_err();

        assert!(
            matches!(err, EcGcError::InvalidRequest { .. }),
            "expected InvalidRequest, got {err:?}"
        );
        assert!(
            probe.calls().is_empty(),
            "probe log must be empty when release is refused"
        );
        assert!(
            purger.purges().is_empty(),
            "purger log must be empty when release is refused"
        );

        let stored_segment = store
            .get_packed_segment(segment.segment_id)
            .await
            .unwrap()
            .expect("segment must exist in store");
        assert_eq!(stored_segment.state, PackedSegmentState::Sealed);
    }

    #[tokio::test]
    async fn an_object_already_switched_is_not_purged_twice() {
        let (_dir, store, gc, segment) =
            prepare_encoded_packed_segment_fixture(3, PackedSegmentState::Encoded).await;
        let already_switched = segment.entries[1].object.clone();

        let mut placement = match store
            .get_object_layout(&already_switched)
            .await
            .unwrap()
            .expect("layout should exist")
        {
            ObjectDataLayout::PackedSegment(p) => p,
            other => panic!("expected PackedSegment layout, got {other:?}"),
        };
        placement.hot_replica_nodes.clear();
        store
            .put_object_layout(
                &already_switched,
                &ObjectDataLayout::PackedSegment(placement),
            )
            .await
            .unwrap();

        let probe = MemoryPackedProbe::default();
        let purger = MemoryHotCopyPurger::default();
        let committer = MemoryPointerCommitter::default();

        let release = gc
            .release_packed_segment_hot_copies(&segment, &probe, &committer, &purger)
            .await
            .unwrap();

        assert_eq!(release.probed, 3);
        assert_eq!(release.switched, 2);
        assert_eq!(release.purged, 2);

        let purged = purger.purges();
        assert_eq!(purged.len(), 2);
        assert!(
            !purged.iter().any(|(obj, _)| obj == &already_switched),
            "already switched object must not be purged again"
        );
        for entry in &segment.entries {
            if entry.object != already_switched {
                let occurrences = purged
                    .iter()
                    .filter(|(obj, hlc)| obj == &entry.object && *hlc == entry.hlc)
                    .count();
                assert_eq!(
                    occurrences, 1,
                    "remaining object must be purged exactly once"
                );
            }
        }
    }

    #[tokio::test]
    async fn every_object_is_probed_before_anything_is_switched() {
        let (_dir, store, gc, segment) =
            prepare_encoded_packed_segment_fixture(3, PackedSegmentState::Encoded).await;
        let clock = Arc::new(AtomicUsize::new(1));
        let probe = MemoryPackedProbe::default()
            .with_store_verification(store.clone())
            .with_clock(clock.clone());
        let purger = MemoryHotCopyPurger::default().with_clock(clock.clone());
        let committer = MemoryPointerCommitter::default();

        let release = gc
            .release_packed_segment_hot_copies(&segment, &probe, &committer, &purger)
            .await
            .unwrap();

        assert_eq!(release.probed, 3);
        assert_eq!(release.switched, 3);
        assert_eq!(release.purged, 3);

        let probe_order = probe.call_order.lock().unwrap().clone();
        let purge_order = purger.call_order.lock().unwrap().clone();

        assert_eq!(probe_order.len(), 3);
        assert_eq!(purge_order.len(), 3);

        let max_probe_tick = probe_order.iter().map(|(tick, _)| *tick).max().unwrap();
        let min_purge_tick = purge_order.iter().map(|(tick, _)| *tick).min().unwrap();

        assert!(
            max_probe_tick < min_purge_tick,
            "all probes must complete before any purge starts (max probe tick: {max_probe_tick}, min purge tick: {min_purge_tick})"
        );
    }

    #[tokio::test]
    async fn every_live_object_gets_a_durable_pointer_before_its_layout_is_switched() {
        let (_dir, _store, gc, segment) =
            prepare_encoded_packed_segment_fixture(3, PackedSegmentState::Encoded).await;
        let probe = MemoryPackedProbe::default();
        let purger = MemoryHotCopyPurger::default();
        let committer = MemoryPointerCommitter::default();

        let release = gc
            .release_packed_segment_hot_copies(&segment, &probe, &committer, &purger)
            .await
            .unwrap();

        assert_eq!(release.committed, 3);
        let expected_objects: Vec<_> =
            segment.entries.iter().map(|entry| entry.object.clone()).collect();
        assert_eq!(committer.commits(), expected_objects);
        assert_eq!(release.switched, 3);
        assert_eq!(release.purged, 3);
    }

    #[tokio::test]
    async fn a_failed_pointer_commit_leaves_the_hot_copies_where_they_were() {
        let (_dir, store, gc, segment) =
            prepare_encoded_packed_segment_fixture(3, PackedSegmentState::Encoded).await;
        let second_object = segment.entries[1].object.clone();
        let probe = MemoryPackedProbe::default();
        let purger = MemoryHotCopyPurger::default();
        let committer = MemoryPointerCommitter::default().with_failure(second_object);

        let result = gc
            .release_packed_segment_hot_copies(&segment, &probe, &committer, &purger)
            .await;
        assert!(result.is_err(), "expected release to fail on commit error");

        // Until the pointer is durable across the cluster, the hot copies are
        // the only form of the data every node can find, so nothing may move.

        assert!(
            purger.purges().is_empty(),
            "purger log must be empty when pointer commit fails"
        );

        for entry in &segment.entries {
            let layout = store
                .get_object_layout(&entry.object)
                .await
                .unwrap()
                .expect("layout must exist");
            match layout {
                ObjectDataLayout::PackedSegment(placement) => {
                    assert_eq!(
                        placement.hot_replica_nodes,
                        vec![node(1), node(2), node(3)],
                        "hot replicas must remain intact for {:?}",
                        entry.object
                    );
                }
                other => panic!("expected PackedSegment layout, got {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn an_object_deleted_while_the_segment_was_open_gets_no_pointer() {
        let (_dir, store, gc, segment) =
            prepare_encoded_packed_segment_fixture(3, PackedSegmentState::Encoded).await;
        let deleted_object = segment.entries[1].object.clone();
        store.delete_object_layout(&segment.entries[1].object).await.unwrap();

        let probe = MemoryPackedProbe::default();
        let purger = MemoryHotCopyPurger::default();
        let committer = MemoryPointerCommitter::default();

        let release = gc
            .release_packed_segment_hot_copies(&segment, &probe, &committer, &purger)
            .await
            .unwrap();

        assert_eq!(release.probed, 3);
        assert_eq!(release.committed, 2);
        assert!(
            !committer.commits().contains(&deleted_object),
            "deleted object must not receive a pointer manifest"
        );
        assert_eq!(release.switched, 2);
    }

    /// Retention source that answers the same way for every object.
    struct MemoryRetention {
        held: bool,
        asked: Mutex<Vec<usize>>,
    }

    impl MemoryRetention {
        fn free() -> Self {
            Self {
                held: false,
                asked: Mutex::new(Vec::new()),
            }
        }

        fn holding_everything() -> Self {
            Self {
                held: true,
                asked: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl PackedSegmentRetentionSource for MemoryRetention {
        async fn is_any_under_retention(
            &self,
            objects: &[EcObjectVersionKey],
        ) -> Result<bool, EcGcError> {
            self.asked.lock().unwrap().push(objects.len());
            Ok(self.held)
        }
    }

    fn repack_policy() -> PackedRepackPolicy {
        // The default floor is eight megabytes, which the fixtures below would
        // have to allocate for real to cross. The fraction is what these tests
        // are about, so the floor is lowered out of the way.
        PackedRepackPolicy {
            dead_ratio: 0.5,
            min_dead_bytes: 1,
        }
    }

    fn packed_config() -> PackedSegmentConfig {
        PackedSegmentConfig {
            enabled: true,
            target_bytes: 64 * 1024 * 1024,
            max_age: std::time::Duration::from_secs(300),
            max_objects: 1_000,
            max_open_segments: 4,
        }
    }

    /// A segment holding `live` objects with `dead` already deleted from it.
    ///
    /// Built the way a real one reaches this state: every object is appended,
    /// the segment is sealed, encoded and purged, and only then are the deleted
    /// ones charged — so the extent, the offsets and the dead count all agree
    /// the way they would in production.
    fn purged_segment_with_dead_objects(live: usize, dead: usize) -> PackedSegment {
        let config = packed_config();
        let mut segment = PackedSegment::open(
            PackedSegmentId::new_random(),
            active_set().set_id,
            &config,
            1_000,
        );

        for index in 0..(live + dead) {
            let object = EcObjectVersionKey::new("photos", format!("packed-{index}.bin"), None);
            let reservation = segment.reserve(128).unwrap();
            let identity = PackedObjectIdentity::new(
                object,
                [index as u8 + 1; 32],
                hlc(100 + index as u64, 0),
            )
            .with_s3_metadata(
                format!("\"etag-{index}\""),
                "application/octet-stream",
                HashMap::new(),
            );
            let item = segment.journal_append(&reservation, identity).unwrap();
            segment.commit(&reservation, &item).unwrap();
        }

        segment.seal(2_000, SealReason::TargetSizeReached).unwrap();
        segment.transition_to(PackedSegmentState::Encoded).unwrap();
        segment.transition_to(PackedSegmentState::Purged).unwrap();

        // Deleted from the front, so the survivors start at a non-zero offset
        // and the replacement genuinely has to close a hole rather than simply
        // copy a prefix.
        for index in 0..dead {
            let object = EcObjectVersionKey::new("photos", format!("packed-{index}.bin"), None);
            segment.record_object_deleted(&object).unwrap();
        }
        segment
    }

    /// A purged, fragmented segment with its surviving objects' layouts written.
    async fn prepare_repack_fixture(
        live: usize,
        dead: usize,
    ) -> (
        tempfile::TempDir,
        Arc<EcMetadataStore>,
        EcGcCoordinator,
        PackedSegment,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let gc = EcGcCoordinator::new(store.clone(), gc_config(), test_sets());
        let segment = purged_segment_with_dead_objects(live, dead);
        store.put_packed_segment(&segment).await.unwrap();

        // No hot replicas: a purged segment's objects are served by its shards,
        // which is exactly what makes its dead space unreclaimable without a
        // repack.
        for entry in &segment.entries {
            let placement =
                PackedSegmentObjectPlacement::from_entry(&segment, entry, Vec::new()).unwrap();
            store
                .put_object_layout(&entry.object, &ObjectDataLayout::PackedSegment(placement))
                .await
                .unwrap();
        }

        (dir, store, gc, segment)
    }

    /// The manifest that describes a segment's shards.
    fn segment_manifest(segment: &PackedSegment) -> EcObjectManifest {
        let key = segment.manifest_object_key();
        let mut manifest = live_manifest(None, operation_id(2_005));
        manifest.bucket = key.bucket.clone();
        manifest.key = key.key.clone();
        manifest.version_id = None;
        manifest
    }

    #[test]
    fn only_a_purged_segment_is_a_repack_candidate() {
        let policy = repack_policy();
        let mut segment = purged_segment_with_dead_objects(2, 2);

        for state in [
            PackedSegmentState::Open,
            PackedSegmentState::Sealed,
            PackedSegmentState::Encoded,
        ] {
            segment.state = state;
            assert!(
                matches!(
                    classify_packed_segment(&segment, &policy),
                    PackedRepackDecision::NotEligible { .. }
                ),
                "a {state} segment must not be repacked"
            );
        }

        segment.state = PackedSegmentState::Purged;
        assert!(matches!(
            classify_packed_segment(&segment, &policy),
            PackedRepackDecision::Repack { .. }
        ));
    }

    #[test]
    fn a_segment_below_either_threshold_is_left_alone() {
        let mut policy = repack_policy();
        // Three of four objects alive: a quarter dead, below the fraction.
        let barely_used = purged_segment_with_dead_objects(3, 1);
        assert!(matches!(
            classify_packed_segment(&barely_used, &policy),
            PackedRepackDecision::Fragmented { .. }
        ));

        // Half dead, which clears the fraction, but the floor is above what the
        // segment could give back.
        let half_dead = purged_segment_with_dead_objects(2, 2);
        policy.min_dead_bytes = half_dead.dead_bytes + 1;
        assert!(matches!(
            classify_packed_segment(&half_dead, &policy),
            PackedRepackDecision::Fragmented { .. }
        ));
    }

    #[test]
    fn a_segment_with_nothing_live_is_retired_rather_than_repacked() {
        let empty = purged_segment_with_dead_objects(0, 3);
        assert_eq!(
            classify_packed_segment(&empty, &repack_policy()),
            PackedRepackDecision::Retire
        );
    }

    #[test]
    fn a_repack_threshold_outside_its_range_is_refused() {
        for ratio in [0.0, -0.5, 1.5, f64::NAN, f64::INFINITY] {
            let policy = PackedRepackPolicy {
                dead_ratio: ratio,
                min_dead_bytes: 1,
            };
            assert!(policy.validate().is_err(), "ratio {ratio} must be refused");
        }
        assert!(PackedRepackPolicy {
            dead_ratio: 0.5,
            min_dead_bytes: 0,
        }
        .validate()
        .is_err());
    }

    #[tokio::test]
    async fn the_analyzer_offers_a_fragmented_segment_and_passes_over_the_rest() {
        let (_dir, store, gc, fragmented) = prepare_repack_fixture(2, 2).await;
        let intact = purged_segment_with_dead_objects(4, 0);
        store.put_packed_segment(&intact).await.unwrap();

        let plan = gc
            .plan_packed_repacks(&repack_policy(), &MemoryRetention::free(), 8)
            .await
            .unwrap();

        assert_eq!(plan.fragmented.len(), 1);
        assert_eq!(plan.fragmented[0].segment_id, fragmented.segment_id);
        assert!(plan.empty.is_empty());
        assert_eq!(plan.blocked_by_retention, 0);
    }

    #[tokio::test]
    async fn a_segment_under_object_lock_is_counted_and_left_alone() {
        let (_dir, _store, gc, _segment) = prepare_repack_fixture(2, 2).await;

        let retention = MemoryRetention::holding_everything();
        let plan = gc.plan_packed_repacks(&repack_policy(), &retention, 8).await.unwrap();

        assert!(
            plan.fragmented.is_empty(),
            "a segment holding an object under retention must not be repacked"
        );
        assert_eq!(plan.blocked_by_retention, 1);
        assert_eq!(
            retention.asked.lock().unwrap().as_slice(),
            &[2],
            "retention is asked about the live objects, once"
        );
    }

    #[tokio::test]
    async fn a_segment_already_being_repacked_is_not_offered_again() {
        let (_dir, store, gc, source) = prepare_repack_fixture(2, 2).await;
        gc.repack_packed_segment(&source, &packed_config(), 3_000).await.unwrap();

        let plan = gc
            .plan_packed_repacks(&repack_policy(), &MemoryRetention::free(), 8)
            .await
            .unwrap();

        assert!(
            plan.fragmented.is_empty(),
            "a segment with a replacement already staged must not be staged twice"
        );
        let staged = store.list_packed_segments(16).await.unwrap();
        assert_eq!(
            staged.iter().filter(|s| s.repacked_from == Some(source.segment_id)).count(),
            1
        );
    }

    #[tokio::test]
    async fn an_encoded_replacement_is_offered_for_cutover() {
        let (_dir, store, gc, source) = prepare_repack_fixture(2, 2).await;
        let staged = gc.repack_packed_segment(&source, &packed_config(), 3_000).await.unwrap();
        let mut destination =
            store.get_packed_segment(staged.destination_id).await.unwrap().unwrap();
        destination.transition_to(PackedSegmentState::Encoded).unwrap();
        store.put_packed_segment(&destination).await.unwrap();

        let plan = gc
            .plan_packed_repacks(&repack_policy(), &MemoryRetention::free(), 8)
            .await
            .unwrap();

        assert_eq!(plan.pending_cutover.len(), 1);
        assert_eq!(plan.pending_cutover[0].segment_id, staged.destination_id);
    }

    #[tokio::test]
    async fn staging_a_repack_leaves_the_source_serving_every_object() {
        let (_dir, store, gc, source) = prepare_repack_fixture(2, 2).await;

        let staged = gc.repack_packed_segment(&source, &packed_config(), 3_000).await.unwrap();

        let stored_source = store.get_packed_segment(source.segment_id).await.unwrap().unwrap();
        assert_eq!(stored_source.state, PackedSegmentState::Purged);
        assert_eq!(stored_source.entries.len(), 2);
        for entry in &source.entries {
            match store.get_object_layout(&entry.object).await.unwrap().unwrap() {
                ObjectDataLayout::PackedSegment(placement) => assert_eq!(
                    placement.segment_id, source.segment_id,
                    "staging must not move a single reference"
                ),
                other => panic!("expected PackedSegment layout, got {other:?}"),
            }
        }

        let destination = store.get_packed_segment(staged.destination_id).await.unwrap().unwrap();
        assert_eq!(destination.state, PackedSegmentState::Sealed);
        assert_eq!(destination.repacked_from, Some(source.segment_id));
        assert_eq!(destination.seal_reason, Some(SealReason::Repacked));
    }

    #[tokio::test]
    async fn a_repack_replacement_holds_the_live_objects_without_the_holes() {
        let (_dir, store, gc, source) = prepare_repack_fixture(2, 2).await;

        let staged = gc.repack_packed_segment(&source, &packed_config(), 3_000).await.unwrap();
        let destination = store.get_packed_segment(staged.destination_id).await.unwrap().unwrap();

        assert_eq!(destination.used_bytes, source.live_bytes());
        assert_eq!(destination.dead_bytes, 0);
        let mut expected_offset = 0u64;
        for (entry, source_entry) in destination.entries.iter().zip(&source.entries) {
            assert_eq!(
                entry.offset, expected_offset,
                "the replacement must have no holes"
            );
            assert_eq!(entry.object, source_entry.object);
            assert_eq!(entry.content_hash_sha256, source_entry.content_hash_sha256);
            assert_eq!(
                entry.etag, source_entry.etag,
                "S3 metadata must survive the move"
            );
            expected_offset += entry.length;
        }
    }

    #[tokio::test]
    async fn repacking_a_segment_that_is_not_purged_is_refused() {
        let (_dir, _store, gc, mut source) = prepare_repack_fixture(2, 2).await;
        source.state = PackedSegmentState::Encoded;

        let error = gc.repack_packed_segment(&source, &packed_config(), 3_000).await.unwrap_err();

        assert!(matches!(error, EcGcError::InvalidRequest { .. }));
    }

    #[tokio::test]
    async fn repacking_a_segment_with_nothing_live_is_refused() {
        let (_dir, _store, gc, _source) = prepare_repack_fixture(0, 3).await;
        let empty = purged_segment_with_dead_objects(0, 3);

        let error = gc.repack_packed_segment(&empty, &packed_config(), 3_000).await.unwrap_err();

        assert!(matches!(error, EcGcError::InvalidRequest { .. }));
    }

    /// Stage a repack and mark its replacement encoded, ready for cutover.
    async fn staged_replacement(
        store: &Arc<EcMetadataStore>,
        gc: &EcGcCoordinator,
        source: &PackedSegment,
    ) -> PackedSegment {
        let staged = gc.repack_packed_segment(source, &packed_config(), 3_000).await.unwrap();
        let mut destination =
            store.get_packed_segment(staged.destination_id).await.unwrap().unwrap();
        destination.transition_to(PackedSegmentState::Encoded).unwrap();
        store.put_packed_segment(&destination).await.unwrap();
        destination
    }

    #[tokio::test]
    async fn a_repack_cutover_switches_every_object_and_retires_the_source() {
        let (_dir, store, gc, source) = prepare_repack_fixture(2, 2).await;
        let destination = staged_replacement(&store, &gc, &source).await;
        let probe = MemoryPackedProbe::default();
        let committer = MemoryPointerCommitter::default();

        let report = gc
            .complete_packed_repack(&destination, &probe, &committer, 9_000)
            .await
            .unwrap();

        assert_eq!(report.moved, 2);
        assert_eq!(report.abandoned, 0);
        assert_eq!(report.probed, 2);
        assert_eq!(report.reclaimed_bytes, source.used_bytes);
        for entry in &destination.entries {
            match store.get_object_layout(&entry.object).await.unwrap().unwrap() {
                ObjectDataLayout::PackedSegment(placement) => {
                    assert_eq!(placement.segment_id, destination.segment_id);
                    assert!(
                        placement.hot_replica_nodes.is_empty(),
                        "a repacked object is served by shards and names no hot copies"
                    );
                }
                other => panic!("expected PackedSegment layout, got {other:?}"),
            }
        }
        let retired = store.get_packed_segment(source.segment_id).await.unwrap().unwrap();
        assert_eq!(retired.state, PackedSegmentState::Dead);
        assert_eq!(retired.dead_at, Some(9_000));
        let replacement = store.get_packed_segment(destination.segment_id).await.unwrap().unwrap();
        assert_eq!(replacement.state, PackedSegmentState::Purged);
    }

    #[tokio::test]
    async fn every_repacked_object_is_probed_and_gets_a_pointer_before_anything_moves() {
        let (_dir, store, gc, source) = prepare_repack_fixture(3, 3).await;
        let destination = staged_replacement(&store, &gc, &source).await;
        let probe = MemoryPackedProbe::default();
        let committer = MemoryPointerCommitter::default();

        gc.complete_packed_repack(&destination, &probe, &committer, 9_000)
            .await
            .unwrap();

        assert_eq!(probe.calls().len(), 3);
        assert_eq!(committer.commits().len(), 3);
    }

    #[tokio::test]
    async fn a_failed_probe_leaves_the_repack_exactly_where_it_was() {
        let (_dir, store, gc, source) = prepare_repack_fixture(2, 2).await;
        let destination = staged_replacement(&store, &gc, &source).await;
        let failing = destination.entries[1].object.clone();
        let probe = MemoryPackedProbe::default().with_failure(failing);
        let committer = MemoryPointerCommitter::default();

        let error = gc
            .complete_packed_repack(&destination, &probe, &committer, 9_000)
            .await
            .unwrap_err();

        assert!(matches!(error, EcGcError::InvalidRequest { .. }));
        assert!(
            committer.commits().is_empty(),
            "nothing may be committed while an object cannot be read back"
        );
        assert_eq!(
            store.get_packed_segment(source.segment_id).await.unwrap().unwrap().state,
            PackedSegmentState::Purged,
            "the source keeps serving its objects"
        );
        for entry in &source.entries {
            match store.get_object_layout(&entry.object).await.unwrap().unwrap() {
                ObjectDataLayout::PackedSegment(placement) => {
                    assert_eq!(placement.segment_id, source.segment_id)
                }
                other => panic!("expected PackedSegment layout, got {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn a_failed_pointer_commit_leaves_the_repack_exactly_where_it_was() {
        let (_dir, store, gc, source) = prepare_repack_fixture(2, 2).await;
        let destination = staged_replacement(&store, &gc, &source).await;
        let failing = destination.entries[1].object.clone();
        let probe = MemoryPackedProbe::default();
        let committer = MemoryPointerCommitter::default().with_failure(failing);

        let error = gc
            .complete_packed_repack(&destination, &probe, &committer, 9_000)
            .await
            .unwrap_err();

        assert!(matches!(error, EcGcError::InvalidRequest { .. }));
        assert_eq!(
            store.get_packed_segment(source.segment_id).await.unwrap().unwrap().state,
            PackedSegmentState::Purged
        );
        for entry in &source.entries {
            match store.get_object_layout(&entry.object).await.unwrap().unwrap() {
                ObjectDataLayout::PackedSegment(placement) => {
                    assert_eq!(placement.segment_id, source.segment_id)
                }
                other => panic!("expected PackedSegment layout, got {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn a_read_that_started_before_the_cutover_still_finds_its_segment() {
        let (_dir, store, gc, source) = prepare_repack_fixture(2, 2).await;
        // The snapshot a read takes on the way in, before anything moves.
        let reading = source.entries[0].object.clone();
        let in_flight = match store.get_object_layout(&reading).await.unwrap().unwrap() {
            ObjectDataLayout::PackedSegment(placement) => placement,
            other => panic!("expected PackedSegment layout, got {other:?}"),
        };
        let destination = staged_replacement(&store, &gc, &source).await;

        gc.complete_packed_repack(
            &destination,
            &MemoryPackedProbe::default(),
            &MemoryPointerCommitter::default(),
            9_000,
        )
        .await
        .unwrap();

        // Everything that read holds still resolves: the record it named, the
        // index entry inside it, and therefore the shards it is reading from.
        let retired = store.get_packed_segment(in_flight.segment_id).await.unwrap().unwrap();
        assert_eq!(retired.state, PackedSegmentState::Dead);
        let entry = retired.entry_for(&reading).expect("the entry a read resolves facts from");
        assert_eq!(entry.offset, in_flight.offset);
        assert_eq!(entry.length, in_flight.length);
        assert!(
            store.packed_segment_referrers(&retired, 16).await.unwrap().is_empty(),
            "no new read can reach the retired segment"
        );
    }

    #[tokio::test]
    async fn a_retired_segment_keeps_its_shards_until_the_drain_window_closes() {
        let (_dir, store, gc, source) = prepare_repack_fixture(2, 2).await;
        let destination = staged_replacement(&store, &gc, &source).await;
        gc.complete_packed_repack(
            &destination,
            &MemoryPackedProbe::default(),
            &MemoryPointerCommitter::default(),
            9_000,
        )
        .await
        .unwrap();
        let coordinator = MemoryMetadataCoordinator::new(
            EcObjectLayoutReadResult::Absent,
            EcManifestReadResult::Present(segment_manifest(&source)),
            5,
        );
        let grace = seconds_to_nanos(gc_config().packed_retire_grace_secs);

        let early = gc
            .retire_drained_packed_segments(9_000 + grace - 1, hlc(50, 0), node(1), &coordinator, 8)
            .await
            .unwrap();

        assert_eq!(early.retired, 0);
        assert_eq!(early.retained, 1);
        assert!(store.get_packed_segment(source.segment_id).await.unwrap().is_some());

        let late = gc
            .retire_drained_packed_segments(9_000 + grace, hlc(51, 0), node(1), &coordinator, 8)
            .await
            .unwrap();

        assert_eq!(late.retired, 1);
        assert!(
            store.get_packed_segment(source.segment_id).await.unwrap().is_none(),
            "a retired segment's record goes once its shards are tombstoned"
        );
        let tombstoned = coordinator.commits.lock().unwrap();
        assert_eq!(tombstoned.len(), 1);
        assert_eq!(tombstoned[0].0.object_key(), source.manifest_object_key());
        assert!(tombstoned[0].0.is_deleted);
    }

    #[tokio::test]
    async fn a_retired_segment_still_named_by_a_layout_keeps_its_shards() {
        let (_dir, store, gc, source) = prepare_repack_fixture(2, 2).await;
        let mut retired = source.clone();
        retired.retire(9_000).unwrap();
        store.put_packed_segment(&retired).await.unwrap();
        let coordinator = MemoryMetadataCoordinator::new(
            EcObjectLayoutReadResult::Absent,
            EcManifestReadResult::Present(segment_manifest(&source)),
            5,
        );
        let grace = seconds_to_nanos(gc_config().packed_retire_grace_secs);

        let report = gc
            .retire_drained_packed_segments(9_000 + grace, hlc(52, 0), node(1), &coordinator, 8)
            .await
            .unwrap();

        assert_eq!(report.retired, 0);
        assert_eq!(report.retained, 1);
        assert!(coordinator.commits.lock().unwrap().is_empty());
        assert!(store.get_packed_segment(source.segment_id).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn retiring_a_segment_that_was_never_encoded_just_drops_its_record() {
        let (_dir, store, gc, source) = prepare_repack_fixture(0, 3).await;
        let mut retired = source.clone();
        retired.retire(9_000).unwrap();
        store.put_packed_segment(&retired).await.unwrap();
        // No manifest: the segment never reached the encoder, so there are no
        // shards to tombstone.
        let coordinator = MemoryMetadataCoordinator::new(
            EcObjectLayoutReadResult::Absent,
            EcManifestReadResult::Absent,
            5,
        );
        let grace = seconds_to_nanos(gc_config().packed_retire_grace_secs);

        let report = gc
            .retire_drained_packed_segments(9_000 + grace, hlc(53, 0), node(1), &coordinator, 8)
            .await
            .unwrap();

        assert_eq!(report.retired, 1);
        assert!(coordinator.commits.lock().unwrap().is_empty());
        assert!(store.get_packed_segment(source.segment_id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn an_emptied_segment_is_retired_without_a_replacement() {
        let (_dir, store, gc, _source) = prepare_repack_fixture(0, 3).await;
        let plan = gc
            .plan_packed_repacks(&repack_policy(), &MemoryRetention::free(), 8)
            .await
            .unwrap();
        assert_eq!(plan.empty.len(), 1);

        gc.retire_empty_packed_segment(&plan.empty[0], 9_000).await.unwrap();

        let retired = store.get_packed_segment(plan.empty[0].segment_id).await.unwrap().unwrap();
        assert_eq!(retired.state, PackedSegmentState::Dead);
        assert_eq!(retired.dead_at, Some(9_000));
    }

    #[tokio::test]
    async fn a_repack_replacement_is_never_mistaken_for_a_segment_to_release() {
        let (_dir, store, gc, source) = prepare_repack_fixture(2, 2).await;
        let destination = staged_replacement(&store, &gc, &source).await;

        // The hot-copy release runs over every encoded segment. A replacement
        // reaches that state too, and releasing it would move it to `Purged`
        // without ever switching its objects — stranding the replacement and
        // leaving the source fragmented forever.
        let error = gc
            .release_packed_segment_hot_copies(
                &destination,
                &MemoryPackedProbe::default(),
                &MemoryPointerCommitter::default(),
                &MemoryHotCopyPurger::default(),
            )
            .await
            .unwrap_err();

        assert!(matches!(error, EcGcError::InvalidRequest { .. }));
        assert_eq!(
            store.get_packed_segment(destination.segment_id).await.unwrap().unwrap().state,
            PackedSegmentState::Encoded,
            "the replacement must still be waiting for its cutover"
        );
    }

    #[tokio::test]
    async fn an_object_deleted_during_the_repack_gets_no_pointer_of_its_own() {
        let (_dir, store, gc, source) = prepare_repack_fixture(2, 2).await;
        let destination = staged_replacement(&store, &gc, &source).await;
        // A DELETE lands after the replacement was encoded and before the
        // cutover: the object stops naming the source segment.
        let deleted = source.entries[0].object.clone();
        store.record_packed_object_deleted(&deleted).await.unwrap();
        let probe = MemoryPackedProbe::default();
        let committer = MemoryPointerCommitter::default();

        let report = gc
            .complete_packed_repack(&destination, &probe, &committer, 9_000)
            .await
            .unwrap();

        assert_eq!(
            report.probed, 2,
            "the probe is a statement about the container, so it covers every entry"
        );
        assert_eq!(report.committed, 1);
        assert_eq!(report.moved, 1);
        assert_eq!(report.abandoned, 1);
        assert!(
            !committer.commits().contains(&deleted),
            "a pointer manifest for a deleted object would put a live manifest back over its \
             tombstone"
        );
        assert!(
            store.get_object_layout(&deleted).await.unwrap().is_none(),
            "the cutover must not resurrect an object that was deleted while it ran"
        );
    }

    #[tokio::test]
    async fn the_analyzer_sees_deletions_that_were_never_written_into_the_record() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let gc = EcGcCoordinator::new(store.clone(), gc_config(), test_sets());

        // A pristine segment, recorded exactly as the node that sealed it wrote
        // it: four live objects and no dead bytes at all.
        let segment = purged_segment_with_dead_objects(4, 0);
        store.put_packed_segment(&segment).await.unwrap();
        for entry in &segment.entries {
            let placement =
                PackedSegmentObjectPlacement::from_entry(&segment, entry, Vec::new()).unwrap();
            store
                .put_object_layout(&entry.object, &ObjectDataLayout::PackedSegment(placement))
                .await
                .unwrap();
        }

        // Two deletes arrive. They are charged as set entries against the
        // segment; the segment's own record is not rewritten, and never is.
        for entry in segment.entries.iter().take(2) {
            store.record_packed_object_deleted(&entry.object).await.unwrap();
        }
        assert_eq!(
            store.get_packed_segment(segment.segment_id).await.unwrap().unwrap().dead_bytes,
            0,
            "the stored record still says the segment is pristine"
        );

        let plan = gc
            .plan_packed_repacks(&repack_policy(), &MemoryRetention::free(), 8)
            .await
            .unwrap();

        assert_eq!(
            plan.fragmented.len(),
            1,
            "the analyzer has to rebuild the segment to see what was deleted from it"
        );
        assert_eq!(plan.fragmented[0].dead_bytes, 256);
        assert_eq!(plan.fragmented[0].entries.len(), 2);
    }

    /// Unpacker that records what it was asked to do.
    #[derive(Default)]
    struct MemoryUnpacker {
        restored: Mutex<Vec<EcObjectVersionKey>>,
        published: Mutex<Vec<EcObjectVersionKey>>,
        failing_restore: Mutex<HashSet<EcObjectVersionKey>>,
        failing_publish: Mutex<HashSet<EcObjectVersionKey>>,
    }

    impl MemoryUnpacker {
        fn with_restore_failure(self, object: EcObjectVersionKey) -> Self {
            self.failing_restore.lock().unwrap().insert(object);
            self
        }

        fn with_publish_failure(self, object: EcObjectVersionKey) -> Self {
            self.failing_publish.lock().unwrap().insert(object);
            self
        }

        fn restored(&self) -> Vec<EcObjectVersionKey> {
            self.restored.lock().unwrap().clone()
        }

        fn published(&self) -> Vec<EcObjectVersionKey> {
            self.published.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl EcPackedSegmentUnpacker for MemoryUnpacker {
        async fn restore_hot_copies(
            &self,
            _segment: &PackedSegment,
            entry: &PackedSegmentEntry,
            placement: &PackedSegmentObjectPlacement,
        ) -> Result<Vec<NodeId>, EcGcError> {
            if self.failing_restore.lock().unwrap().contains(&entry.object) {
                return Err(EcGcError::PackedRepack {
                    reason: format!("simulated restore failure for {:?}", entry.object),
                });
            }
            self.restored.lock().unwrap().push(entry.object.clone());
            // The real implementation keeps copies that are still there rather
            // than writing the same bytes again.
            if !placement.hot_replica_nodes.is_empty() {
                return Ok(placement.hot_replica_nodes.clone());
            }
            Ok(nodes(3))
        }

        async fn publish_unpacked_manifest(
            &self,
            object: &EcObjectVersionKey,
        ) -> Result<(), EcGcError> {
            if self.failing_publish.lock().unwrap().contains(object) {
                return Err(EcGcError::PackedRepack {
                    reason: format!("simulated pointer withdrawal failure for {object:?}"),
                });
            }
            self.published.lock().unwrap().push(object.clone());
            Ok(())
        }
    }

    /// Every object's layout still names this segment.
    async fn assert_all_still_packed_into(store: &Arc<EcMetadataStore>, segment: &PackedSegment) {
        for entry in &segment.entries {
            match store.get_object_layout(&entry.object).await.unwrap().unwrap() {
                ObjectDataLayout::PackedSegment(placement) => {
                    assert_eq!(placement.segment_id, segment.segment_id)
                }
                other => panic!("expected PackedSegment layout, got {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn unpacking_a_segment_returns_every_object_to_its_own_replicas() {
        let (_dir, store, gc, segment) = prepare_repack_fixture(2, 2).await;
        let unpacker = MemoryUnpacker::default();

        let report = gc.unpack_packed_segment(&segment, &unpacker, 9_000).await.unwrap();

        assert_eq!(report.objects, 2);
        assert_eq!(report.unpacked, 2);
        assert_eq!(report.restored, 2);
        assert_eq!(report.kept_hot_copies, 0);
        assert_eq!(report.skipped, 0);

        for entry in &segment.entries {
            match store.get_object_layout(&entry.object).await.unwrap().unwrap() {
                ObjectDataLayout::ReplicatedSmall(placement) => {
                    assert_eq!(placement.replica_nodes, nodes(3));
                    assert_eq!(placement.size, entry.length);
                    assert_eq!(placement.content_hash_sha256, entry.content_hash_sha256);
                }
                other => panic!("expected ReplicatedSmall layout, got {other:?}"),
            }
        }
        let retired = store.get_packed_segment(segment.segment_id).await.unwrap().unwrap();
        assert_eq!(retired.state, PackedSegmentState::Dead);
        assert_eq!(retired.dead_at, Some(9_000));
    }

    #[tokio::test]
    async fn unpacking_a_segment_that_still_has_hot_copies_does_not_rewrite_them() {
        let (_dir, store, gc, segment) =
            prepare_encoded_packed_segment_fixture(3, PackedSegmentState::Encoded).await;
        let unpacker = MemoryUnpacker::default();

        let report = gc.unpack_packed_segment(&segment, &unpacker, 9_000).await.unwrap();

        assert_eq!(report.unpacked, 3);
        assert_eq!(
            report.kept_hot_copies, 3,
            "a segment that was never released still has the copies the write path made"
        );
        assert_eq!(report.restored, 0);
        assert_eq!(unpacker.restored().len(), 3);
        for entry in &segment.entries {
            match store.get_object_layout(&entry.object).await.unwrap().unwrap() {
                ObjectDataLayout::ReplicatedSmall(placement) => {
                    assert_eq!(placement.replica_nodes, nodes(3))
                }
                other => panic!("expected ReplicatedSmall layout, got {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn an_unpack_that_cannot_restore_an_object_changes_nothing() {
        let (_dir, store, gc, segment) = prepare_repack_fixture(2, 2).await;
        let failing = segment.entries[1].object.clone();
        let unpacker = MemoryUnpacker::default().with_restore_failure(failing.clone());

        let error = gc.unpack_packed_segment(&segment, &unpacker, 9_000).await.unwrap_err();

        assert!(matches!(error, EcGcError::PackedRepack { .. }));
        assert!(
            !unpacker.published().contains(&failing),
            "nothing may be withdrawn for an object whose bytes are not back yet"
        );
        assert_eq!(
            store.get_packed_segment(segment.segment_id).await.unwrap().unwrap().state,
            PackedSegmentState::Purged
        );
        assert_all_still_packed_into(&store, &segment).await;
    }

    #[tokio::test]
    async fn an_unpack_that_cannot_withdraw_a_pointer_changes_nothing() {
        let (_dir, store, gc, segment) = prepare_repack_fixture(2, 2).await;
        let failing = segment.entries[1].object.clone();
        let unpacker = MemoryUnpacker::default().with_publish_failure(failing);

        let error = gc.unpack_packed_segment(&segment, &unpacker, 9_000).await.unwrap_err();

        assert!(matches!(error, EcGcError::PackedRepack { .. }));
        assert_eq!(
            store.get_packed_segment(segment.segment_id).await.unwrap().unwrap().state,
            PackedSegmentState::Purged
        );
        assert_all_still_packed_into(&store, &segment).await;
    }

    #[tokio::test]
    async fn unpacking_an_open_segment_is_refused() {
        let (_dir, _store, gc, _segment) = prepare_repack_fixture(2, 2).await;
        let mut open = PackedSegment::open(
            PackedSegmentId::new_random(),
            active_set().set_id,
            &packed_config(),
            1_000,
        );
        open.append_without_journal(
            PackedObjectIdentity::new(object(None), [9; 32], hlc(10, 0)),
            128,
        )
        .unwrap();

        let error = gc
            .unpack_packed_segment(&open, &MemoryUnpacker::default(), 9_000)
            .await
            .unwrap_err();

        assert!(matches!(error, EcGcError::InvalidRequest { .. }));
    }

    #[tokio::test]
    async fn unpacking_a_retired_segment_is_refused() {
        let (_dir, _store, gc, segment) = prepare_repack_fixture(2, 2).await;
        let mut retired = segment.clone();
        retired.retire(1_000).unwrap();

        let error = gc
            .unpack_packed_segment(&retired, &MemoryUnpacker::default(), 9_000)
            .await
            .unwrap_err();

        assert!(matches!(error, EcGcError::InvalidRequest { .. }));
    }

    #[tokio::test]
    async fn an_object_that_left_the_segment_is_not_unpacked() {
        let (_dir, store, gc, segment) = prepare_repack_fixture(2, 2).await;
        let gone = segment.entries[0].object.clone();
        store.delete_object_layout(&gone).await.unwrap();
        let unpacker = MemoryUnpacker::default();

        let report = gc.unpack_packed_segment(&segment, &unpacker, 9_000).await.unwrap();

        assert_eq!(report.unpacked, 1);
        assert_eq!(report.skipped, 1);
        assert!(
            !unpacker.restored().contains(&gone),
            "an object that moved on is not written back on the segment's behalf"
        );
        assert!(
            store.get_object_layout(&gone).await.unwrap().is_none(),
            "the unpack must not resurrect an object that had already gone"
        );
    }

    #[tokio::test]
    async fn deleting_a_packed_object_charges_its_bytes_even_though_the_layout_is_gone() {
        let (_dir, store, _gc, segment) = prepare_repack_fixture(2, 0).await;
        let deleted = segment.entries[0].object.clone();
        let placement = match store.get_object_layout(&deleted).await.unwrap().unwrap() {
            ObjectDataLayout::PackedSegment(placement) => placement,
            other => panic!("expected PackedSegment layout, got {other:?}"),
        };

        // What the delete path does: the tombstone commit drops the layout, and
        // only then are the bytes charged. Reading the layout at that point
        // finds nothing, so the charge has to come from the placement the
        // delete already resolved.
        store.delete_object_layout(&deleted).await.unwrap();
        assert!(store.record_packed_object_deleted(&deleted).await.unwrap().is_none());

        let charged = store.charge_packed_object_deleted(&deleted, &placement).await.unwrap();
        assert_eq!(charged.segment_id, segment.segment_id);
        assert_eq!(charged.freed_bytes, placement.length);

        let rebuilt = store.load_packed_segments(64).await.unwrap();
        let seen = rebuilt
            .segments
            .iter()
            .find(|candidate| candidate.segment_id == segment.segment_id)
            .expect("the segment is still recorded");
        assert_eq!(seen.dead_bytes, placement.length);
        assert_eq!(seen.entries.len(), 1);
    }

    #[tokio::test]
    async fn a_seal_never_moves_a_segment_backwards() {
        let (_dir, store, _gc, segment) = prepare_repack_fixture(2, 0).await;
        // The in-memory pool keeps its own copy of a segment it sealed. By the
        // time a later seal offers that copy the pipeline has encoded and
        // released the segment, and writing the copy back would hand it to the
        // encoder again and lose the fact that its hot copies are gone.
        let mut stale = segment.clone();
        stale.state = PackedSegmentState::Sealed;

        store.seal_packed_segment(&stale).await.unwrap();

        let stored = store.get_packed_segment(segment.segment_id).await.unwrap().unwrap();
        assert_eq!(stored.state, PackedSegmentState::Purged);
    }
}
