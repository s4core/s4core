// Copyright 2026 S4Core Team
//
// Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

//! Erasure Coding v1 Phase 3 metadata.
//!
//! This module defines durable EC object layout metadata, global manifests,
//! and the node-local shard index. The global manifest never stores remote
//! physical volume coordinates; those live only in
//! [`LocalShardRecord`](crate::ec_metadata::LocalShardRecord) entries owned by
//! the node that wrote them.

use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::path::Path;

use fjall::{Database, Keyspace, KeyspaceCreateOptions, PersistMode};
use s4_cluster::{HlcTimestamp, NodeId};
use s4_codec::EcProfileSnapshot;
pub use s4_codec::ShardRole;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::task;
use uuid::Uuid;

use crate::ec_packed_segments::{
    recover_segments, PackedSegment, PackedSegmentEntry, PackedSegmentError, PackedSegmentId,
    PackedSegmentJournalItem, PackedSegmentJournalPayload, PackedSegmentRecovery,
    PackedSegmentState,
};
use crate::erasure_coding::{
    EcLayoutViolation, EcNewErasureSet, EcSetLayoutRule, EcTopologyPolicy, ErasureSet,
    ErasureSetState, ShardSlotRole,
};

/// Manifest format for an object encoded into its own chunks.
///
/// Also the format of every tombstone, packed or not: a tombstone describes no
/// placement, so there is nothing in it a v1 node could misread.
pub const EC_MANIFEST_FORMAT_VERSION: u8 = 1;

/// Manifest format for an object whose bytes live inside a packed segment.
///
/// A node that only knows version 1 rejects such a manifest in
/// [`EcObjectManifest::validate`] instead of reading it wrong — see decision
/// EC2-068.
pub const EC_PACKED_MANIFEST_FORMAT_VERSION: u8 = 2;

/// Replication factor used for small objects in EC buckets in v1.
pub const REPLICATED_SMALL_REPLICA_COUNT: usize = 3;

/// EC metadata errors.
#[derive(Debug, Error)]
pub enum EcMetadataError {
    /// Object layout is invalid.
    #[error("invalid EC object layout: {reason}")]
    InvalidLayout {
        /// Human-readable validation failure.
        reason: String,
    },

    /// EC manifest is invalid.
    #[error("invalid EC manifest for {bucket}/{key}: {reason}")]
    InvalidManifest {
        /// Bucket name.
        bucket: String,
        /// Object key.
        key: String,
        /// Human-readable validation failure.
        reason: String,
    },

    /// Local shard index entry is invalid.
    #[error("invalid EC local shard record: {reason}")]
    InvalidShard {
        /// Human-readable validation failure.
        reason: String,
    },

    /// Erasure-set roster is invalid.
    #[error("invalid EC erasure set roster: {reason}")]
    InvalidErasureSetRoster {
        /// Human-readable validation failure.
        reason: String,
    },

    /// Erasure-set layout record is invalid.
    #[error("invalid EC erasure set layout: {reason}")]
    InvalidSetLayout {
        /// Human-readable validation failure.
        reason: String,
    },

    /// Codec profile or role validation failed.
    #[error("EC codec metadata error: {0}")]
    Codec(String),

    /// Metadata database operation failed.
    #[error("EC metadata database error: {0}")]
    Database(String),

    /// Metadata serialization failed.
    #[error("EC metadata serialization error: {0}")]
    Serialization(String),

    /// Quorum metadata read failed.
    #[error("EC quorum metadata read failed: {0}")]
    QuorumRead(String),
}

/// Version-scoped EC object key.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EcObjectVersionKey {
    /// Bucket name.
    pub bucket: String,
    /// Object key.
    pub key: String,
    /// Version ID when bucket versioning is enabled.
    pub version_id: Option<String>,
}

impl EcObjectVersionKey {
    /// Build an object-version key.
    pub fn new(
        bucket: impl Into<String>,
        key: impl Into<String>,
        version_id: Option<String>,
    ) -> Self {
        Self {
            bucket: bucket.into(),
            key: key.into(),
            version_id,
        }
    }

    /// Validate that bucket/key/version fields are usable as durable metadata keys.
    pub fn validate(&self) -> Result<(), EcMetadataError> {
        validate_object_key(&self.bucket, &self.key, self.version_id.as_deref())
    }
}

/// Durable object data layout for objects in an EC bucket.
///
/// The variant order is part of the on-disk format: bincode writes the variant
/// index, so a new variant may only be appended. `PackedSegment` therefore sits
/// last. A node that predates it reads index `2` and fails with an explicit
/// deserialization error instead of silently reading the layout as something
/// else — see `EC2-PACK-02-005`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ObjectDataLayout {
    /// Object is below the EC threshold and remains explicitly RF=3 replicated.
    ReplicatedSmall(ReplicatedSmallObjectPlacement),
    /// Object is represented by an EC manifest.
    ErasureCoded(EcObjectManifestRef),
    /// Object lives inside a packed segment, which is erasure coded as a whole.
    PackedSegment(PackedSegmentObjectPlacement),
}

impl ObjectDataLayout {
    /// Validate the layout before writing it to durable metadata.
    pub fn validate(&self) -> Result<(), EcMetadataError> {
        match self {
            Self::ReplicatedSmall(placement) => placement.validate(),
            Self::ErasureCoded(reference) => reference.validate(),
            Self::PackedSegment(placement) => placement.validate(),
        }
    }

    /// Validate that this layout belongs to the object key it is stored under.
    pub fn validate_for_object(&self, object: &EcObjectVersionKey) -> Result<(), EcMetadataError> {
        object.validate()?;
        self.validate()?;
        if let Self::ErasureCoded(reference) = self {
            if reference.object != *object {
                return Err(EcMetadataError::InvalidLayout {
                    reason: format!(
                        "erasure-coded layout references {}/{} but is stored under {}/{}",
                        reference.object.bucket, reference.object.key, object.bucket, object.key
                    ),
                });
            }
        }
        Ok(())
    }
}

/// A small object whose placement record did not reach a majority of its
/// replicas.
///
/// The bytes of such an object are durable — the hot write path put them on
/// their replicas before any of this — but the record saying the object was
/// deliberately placed replicated-small reached too few of the nodes holding
/// them. The node that took the write keeps this note and the background sweep
/// finishes the job, which is the shape of a hinted handoff: the write is not
/// failed for a metadata copy, and the copy is not forgotten either
/// (decision EC2-201).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EcSmallPlacementHint {
    /// The placement record that still has to reach the replicas.
    pub placement: ReplicatedSmallObjectPlacement,
    /// When the shortfall was first noticed, in unix nanoseconds.
    pub recorded_at: u64,
    /// Replicas that confirmed the record on the last attempt.
    pub acks: u8,
    /// Attempts made since, including the write that recorded the hint.
    pub attempts: u32,
}

impl EcSmallPlacementHint {
    /// Validate a hint before it is written or after it is read.
    ///
    /// # Errors
    ///
    /// Returns [`EcMetadataError::InvalidLayout`] when the placement it carries
    /// is not a valid RF=3 small-object placement.
    pub fn validate(&self) -> Result<(), EcMetadataError> {
        self.placement.validate()
    }
}

/// Explicit RF=3 placement for a small object in an EC bucket.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicatedSmallObjectPlacement {
    /// Replica nodes. This must contain exactly three distinct nodes in v1.
    pub replica_nodes: Vec<NodeId>,
    /// SHA-256 of the complete object content.
    pub content_hash_sha256: [u8; 32],
    /// Object size in bytes.
    pub size: u64,
    /// HLC timestamp for conflict ordering.
    pub hlc: HlcTimestamp,
}

impl ReplicatedSmallObjectPlacement {
    /// Build and validate a small-object placement.
    pub fn new(
        replica_nodes: Vec<NodeId>,
        content_hash_sha256: [u8; 32],
        size: u64,
        hlc: HlcTimestamp,
    ) -> Result<Self, EcMetadataError> {
        let placement = Self {
            replica_nodes,
            content_hash_sha256,
            size,
            hlc,
        };
        placement.validate()?;
        Ok(placement)
    }

    /// Validate v1 small-object placement invariants.
    pub fn validate(&self) -> Result<(), EcMetadataError> {
        if self.replica_nodes.len() != REPLICATED_SMALL_REPLICA_COUNT {
            return Err(EcMetadataError::InvalidLayout {
                reason: format!(
                    "replicated-small placement requires exactly \
                     {REPLICATED_SMALL_REPLICA_COUNT} replica nodes, got {}",
                    self.replica_nodes.len()
                ),
            });
        }
        ensure_distinct_nodes(&self.replica_nodes, "replicated-small placement")?;
        Ok(())
    }
}

/// Placement of a small object inside a packed segment.
///
/// Everything a read needs to find the bytes and prove they are the right ones:
/// which segment, where in it, how long, and what the object must hash to. The
/// hash repeats the one in the segment's own index entry, and the duplication is
/// deliberate — a direct read (`EC2-063`) verifies what it assembled without a
/// second metadata lookup.
///
/// `hot_replica_nodes` is what keeps the promise of `EC2-060` that an open
/// segment never lowers durability. Until the segment is sealed, encoded and
/// verified, the object's bytes are the ordinary RF=3 copies named here and a
/// read is served from them; the segment index only records where the object
/// *will* sit. The list is cleared when those copies are purged, and an empty
/// list is therefore the marker that the object now lives only in the segment's
/// shards.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PackedSegmentObjectPlacement {
    /// Segment holding the object.
    pub segment_id: PackedSegmentId,
    /// Erasure set that owns the segment's shards.
    pub erasure_set_id: u64,
    /// Offset of the object from the start of the segment.
    pub offset: u64,
    /// Object length in bytes.
    pub length: u64,
    /// SHA-256 of the complete object content.
    pub content_hash_sha256: [u8; 32],
    /// HLC timestamp for conflict ordering.
    pub hlc: HlcTimestamp,
    /// Hot RF=3 replicas still holding the object, empty once they are purged.
    pub hot_replica_nodes: Vec<NodeId>,
}

impl PackedSegmentObjectPlacement {
    /// Build and validate a packed-object placement.
    ///
    /// # Errors
    ///
    /// Returns [`EcMetadataError::InvalidLayout`] when the placement does not
    /// describe a usable region of a segment.
    pub fn new(
        segment_id: PackedSegmentId,
        erasure_set_id: u64,
        offset: u64,
        length: u64,
        content_hash_sha256: [u8; 32],
        hlc: HlcTimestamp,
        hot_replica_nodes: Vec<NodeId>,
    ) -> Result<Self, EcMetadataError> {
        let placement = Self {
            segment_id,
            erasure_set_id,
            offset,
            length,
            content_hash_sha256,
            hlc,
            hot_replica_nodes,
        };
        placement.validate()?;
        Ok(placement)
    }

    /// Derive a placement from a segment's own index entry.
    ///
    /// # Errors
    ///
    /// Returns [`EcMetadataError::InvalidLayout`] when the entry does not
    /// describe a usable region of the segment.
    pub fn from_entry(
        segment: &PackedSegment,
        entry: &PackedSegmentEntry,
        hot_replica_nodes: Vec<NodeId>,
    ) -> Result<Self, EcMetadataError> {
        Self::new(
            segment.segment_id,
            segment.erasure_set_id,
            entry.offset,
            entry.length,
            entry.content_hash_sha256,
            entry.hlc,
            hot_replica_nodes,
        )
    }

    /// The RF=3 placement to read from while the hot copies still exist.
    ///
    /// `None` once they are purged, which is the point at which the segment's
    /// shards become the only source and the direct read of `EC2-063` takes
    /// over.
    pub fn hot_placement(&self) -> Option<ReplicatedSmallObjectPlacement> {
        if self.hot_replica_nodes.is_empty() {
            return None;
        }
        Some(ReplicatedSmallObjectPlacement {
            replica_nodes: self.hot_replica_nodes.clone(),
            content_hash_sha256: self.content_hash_sha256,
            size: self.length,
            hlc: self.hlc,
        })
    }

    /// Validate a durable packed-object placement.
    ///
    /// # Errors
    ///
    /// Returns [`EcMetadataError::InvalidLayout`] when the segment id is nil,
    /// the object is empty, the region runs past the end of `u64`, or the hot
    /// replica list is present but is not a valid RF=3 placement.
    pub fn validate(&self) -> Result<(), EcMetadataError> {
        if self.segment_id.is_nil() {
            return Err(EcMetadataError::InvalidLayout {
                reason: "packed-segment placement segment_id must not be nil".to_string(),
            });
        }
        if self.length == 0 {
            return Err(EcMetadataError::InvalidLayout {
                reason: format!(
                    "packed-segment placement in {} has zero length; empty objects are not packed",
                    self.segment_id
                ),
            });
        }
        self.end_offset()?;
        if !self.hot_replica_nodes.is_empty() {
            if self.hot_replica_nodes.len() != REPLICATED_SMALL_REPLICA_COUNT {
                return Err(EcMetadataError::InvalidLayout {
                    reason: format!(
                        "packed-segment placement in {} lists {} hot replica(s); it must list \
                         either none or exactly {REPLICATED_SMALL_REPLICA_COUNT}",
                        self.segment_id,
                        self.hot_replica_nodes.len()
                    ),
                });
            }
            ensure_distinct_nodes(&self.hot_replica_nodes, "packed-segment placement")?;
        }
        Ok(())
    }

    /// First offset past this object inside the segment.
    ///
    /// # Errors
    ///
    /// Returns [`EcMetadataError::InvalidLayout`] when the sum overflows `u64`.
    pub fn end_offset(&self) -> Result<u64, EcMetadataError> {
        self.offset
            .checked_add(self.length)
            .ok_or_else(|| EcMetadataError::InvalidLayout {
                reason: format!(
                    "packed-segment placement in {} at offset {} with length {} overflows u64",
                    self.segment_id, self.offset, self.length
                ),
            })
    }
}

/// What charging a deleted object to its packed segment freed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PackedSegmentDeletion {
    /// Segment the object belonged to.
    pub segment_id: PackedSegmentId,
    /// Bytes that become dead inside that segment.
    ///
    /// They are not reclaimed here and cannot be: the bytes sit inside a
    /// container that is encoded as a whole. Only a repack returns them, and
    /// this count is what tells the repacker the segment is worth the work.
    pub freed_bytes: u64,
}

/// One object's move out of a repacked segment and into its replacement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackedRepackMove {
    /// Object version being moved.
    pub object: EcObjectVersionKey,
    /// Where the object lives from now on, including its fresh hot copies.
    pub placement: PackedSegmentObjectPlacement,
}

/// What one repack cutover changed.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PackedRepackCutover {
    /// Objects whose layout now names a replacement segment.
    pub switched: Vec<EcObjectVersionKey>,
    /// Objects that stopped pointing at the source segment while the repack ran.
    ///
    /// An overwrite or a delete that landed between reading the object and
    /// committing this batch. The object is not lost — it moved on by itself —
    /// so the replacement segment charges its room to dead bytes and the layout
    /// is left as the newer writer wrote it.
    pub abandoned: Vec<EcObjectVersionKey>,
    /// Bytes the retired source segment gives back once its shards are removed.
    pub reclaimed_bytes: u64,
    /// Replacement segment retired because nothing live reached it.
    pub retired_destinations: Vec<PackedSegmentId>,
}

/// One object's return from a packed segment to ordinary RF=3 replication.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackedUnpackMove {
    /// Object version being returned.
    pub object: EcObjectVersionKey,
    /// The RF=3 placement it is read from once the segment is gone.
    pub placement: ReplicatedSmallObjectPlacement,
}

/// What one unpack changed.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PackedUnpackResult {
    /// Objects whose layout is now `ReplicatedSmall`.
    pub unpacked: Vec<EcObjectVersionKey>,
    /// Objects that had already stopped pointing at the segment.
    pub skipped: Vec<EcObjectVersionKey>,
}

/// Stable reference to an EC manifest from object layout metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EcObjectManifestRef {
    /// Object version addressed by this manifest.
    pub object: EcObjectVersionKey,
    /// Erasure set that owns the shard slots.
    pub erasure_set_id: u64,
    /// SHA-256 of the serialized manifest bytes.
    pub manifest_hash_sha256: [u8; 32],
    /// HLC timestamp for conflict ordering.
    pub hlc: HlcTimestamp,
    /// Idempotency ID for the operation that produced the manifest.
    pub operation_id: Uuid,
}

impl EcObjectManifestRef {
    /// Build a reference from a manifest.
    pub fn from_manifest(manifest: &EcObjectManifest) -> Result<Self, EcMetadataError> {
        manifest.validate()?;
        Ok(Self {
            object: manifest.object_key(),
            erasure_set_id: manifest.erasure_set_id,
            manifest_hash_sha256: manifest.serialized_sha256()?,
            hlc: manifest.hlc,
            operation_id: manifest.operation_id,
        })
    }

    /// Validate a durable manifest reference.
    pub fn validate(&self) -> Result<(), EcMetadataError> {
        self.object.validate()?;
        if self.operation_id.is_nil() {
            return Err(EcMetadataError::InvalidLayout {
                reason: "manifest reference operation_id must not be nil".to_string(),
            });
        }
        Ok(())
    }

    /// Verify that the reference points to the given manifest.
    pub fn matches_manifest(&self, manifest: &EcObjectManifest) -> Result<bool, EcMetadataError> {
        Ok(self.object == manifest.object_key()
            && self.erasure_set_id == manifest.erasure_set_id
            && self.operation_id == manifest.operation_id
            && self.manifest_hash_sha256 == manifest.serialized_sha256()?)
    }

    /// Whether the reference names `manifest` as it was at an earlier state.
    ///
    /// Later steps of an operation commit the same manifest again with only the
    /// state moved on — the hot-copy purge marks it `hot_replicas_purged`. The
    /// hash covers the state, so a layout record taken before such a step stops
    /// matching although it describes the very same shards. This tells that
    /// stale record apart from a reference to other work: the object, the set,
    /// the operation and every field but the state have to be identical, and the
    /// state the reference was taken at has to rank below the one `manifest` is
    /// in now.
    pub fn references_earlier_state_of(
        &self,
        manifest: &EcObjectManifest,
    ) -> Result<bool, EcMetadataError> {
        if self.object != manifest.object_key()
            || self.erasure_set_id != manifest.erasure_set_id
            || self.operation_id != manifest.operation_id
        {
            return Ok(false);
        }
        let current_rank = manifest_state_rank(manifest.state);
        let mut earlier = manifest.clone();
        for state in EcPlacementState::ALL {
            if manifest_state_rank(state) >= current_rank {
                continue;
            }
            earlier.state = state;
            if earlier.serialized_sha256()? == self.manifest_hash_sha256 {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

/// EC object placement state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EcPlacementState {
    /// Existing RF=3 hot layout is still authoritative.
    HotReplicated,
    /// Object has been queued for background EC transcode.
    EcTranscodeQueued,
    /// EC encoder is currently producing shards.
    EcEncoding,
    /// Shards have been written durably, but manifest cutover is not complete.
    EcShardsDurable,
    /// Manifest has been committed through metadata quorum.
    EcManifestCommitted,
    /// EC read path has verified that the object is reconstructable.
    EcReadVerified,
    /// EC layout is authoritative for reads.
    EcCommitted,
    /// Hot RF=3 copies may be garbage-collected.
    HotReplicasGcEligible,
    /// Hot RF=3 copies have been purged.
    HotReplicasPurged,
    /// Transcode failed but can be retried.
    EcFailedRetryable,
    /// Transcode failed and needs operator/configuration change.
    EcFailedPermanent,
    /// The object's bytes are back in RF=3 and this manifest is no longer read.
    ///
    /// Reverse tiering's durable marker, and deliberately not a tombstone: the
    /// object is alive, it simply stopped being erasure coded. Every node that
    /// reads a manifest in this state sends the reader to the hot path, which
    /// is what makes the switch converge without a second round of metadata.
    /// The chunks are kept so the shards stay locatable until GC removes them,
    /// and so a promotion that turns out to be wrong can be undone by putting
    /// the state back.
    Promoted,
    /// Metadata tombstone for a deleted EC object/version.
    Tombstone,
    /// Reverse tiering is putting the object's bytes back on its replicas.
    ///
    /// Erasure coding is still what readers use, and the shards are still the
    /// copy known to work. What the state adds is that the hot copies of this
    /// generation belong to the object: namespace reconciliation would
    /// otherwise take them for leftovers of an unfinished hot purge and release
    /// them, because nothing else about them tells the two apart. It is
    /// committed before the first byte comes back, so any node that sees those
    /// bytes also sees this state. Decision EC2-026.
    ///
    /// Declared last on purpose: bincode encodes a variant by its position, and
    /// every other state keeps the one it was written with.
    Promoting,
}

impl EcPlacementState {
    /// The state's name as it is serialized and as operators see it.
    ///
    /// Kept next to the variants so a name can never drift from what a
    /// manifest or an admin answer carries.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::HotReplicated => "hot_replicated",
            Self::EcTranscodeQueued => "ec_transcode_queued",
            Self::EcEncoding => "ec_encoding",
            Self::EcShardsDurable => "ec_shards_durable",
            Self::EcManifestCommitted => "ec_manifest_committed",
            Self::EcReadVerified => "ec_read_verified",
            Self::EcCommitted => "ec_committed",
            Self::HotReplicasGcEligible => "hot_replicas_gc_eligible",
            Self::HotReplicasPurged => "hot_replicas_purged",
            Self::EcFailedRetryable => "ec_failed_retryable",
            Self::EcFailedPermanent => "ec_failed_permanent",
            Self::Promoted => "promoted",
            Self::Tombstone => "tombstone",
            Self::Promoting => "promoting",
        }
    }

    /// Whether erasure coding is the layout readers of a live object use.
    ///
    /// One answer for every path that asks: reading, repair, shard
    /// anti-entropy, orphan GC and namespace reconciliation all have to agree
    /// on which manifests describe shards the object depends on. A path that
    /// disagreed would be the one that let those shards go. The match is
    /// exhaustive for the same reason, so that a new state cannot be added
    /// without deciding where it stands.
    pub fn is_ec_authoritative(self) -> bool {
        match self {
            Self::EcCommitted
            | Self::HotReplicasGcEligible
            | Self::HotReplicasPurged
            | Self::Promoting => true,
            Self::HotReplicated
            | Self::EcTranscodeQueued
            | Self::EcEncoding
            | Self::EcShardsDurable
            | Self::EcManifestCommitted
            | Self::EcReadVerified
            | Self::EcFailedRetryable
            | Self::EcFailedPermanent
            | Self::Promoted
            | Self::Tombstone => false,
        }
    }

    /// Every state, in declaration order.
    ///
    /// A manifest reference carries a hash and not the state it was taken at;
    /// trying each state against the manifest the quorum holds is how that
    /// state is recovered (see [`EcObjectManifestRef::references_earlier_state_of`]).
    pub const ALL: [Self; 14] = [
        Self::HotReplicated,
        Self::EcTranscodeQueued,
        Self::EcEncoding,
        Self::EcShardsDurable,
        Self::EcManifestCommitted,
        Self::EcReadVerified,
        Self::EcCommitted,
        Self::HotReplicasGcEligible,
        Self::HotReplicasPurged,
        Self::EcFailedRetryable,
        Self::EcFailedPermanent,
        Self::Promoted,
        Self::Tombstone,
        Self::Promoting,
    ];
}

/// EC repair trigger class.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EcRepairKind {
    /// Shard was missing or unreachable.
    MissingShard,
    /// Shard bytes failed manifest-authoritative verification.
    CorruptShard,
    /// Node returned after downtime and needs shard reconciliation.
    NodeRejoin,
    /// Operator explicitly requested repair.
    ManualAdmin,
    /// Anti-entropy detected manifest or shard inventory divergence.
    AntiEntropyMismatch,
}

/// Durable EC repair task status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EcRepairStatus {
    /// Task is queued and eligible once `next_retry_at` allows it.
    Queued,
    /// A worker is actively repairing the shard.
    Running,
    /// Shard was reconstructed, written durably, indexed locally, and frontiers were advanced.
    Succeeded,
    /// Repair failed but may succeed later.
    FailedRetryable,
    /// Repair cannot proceed without a new manifest or operator action.
    FailedPermanent,
}

impl EcRepairStatus {
    /// True when this status should count against active repair queue depth.
    pub fn counts_against_queue_depth(self) -> bool {
        matches!(self, Self::Queued | Self::Running | Self::FailedRetryable)
    }

    /// True when no worker should retry this task automatically.
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::FailedPermanent)
    }
}

/// Durable queue record for one EC shard repair.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EcRepairTaskRecord {
    /// Deterministic repair task ID.
    pub repair_id: Uuid,
    /// Object version needing repair.
    pub object: EcObjectVersionKey,
    /// Operation that produced the winning manifest.
    pub operation_id: Uuid,
    /// Erasure set that owns the shard slot.
    pub erasure_set_id: u64,
    /// Logical chunk index.
    pub chunk_index: u32,
    /// Shard slot index.
    pub slot_index: u8,
    /// Assigned shard owner node.
    pub node_id: NodeId,
    /// Repair trigger class.
    pub kind: EcRepairKind,
    /// Durable queue status.
    pub status: EcRepairStatus,
    /// Number of repair attempts already started.
    pub attempts: u32,
    /// Human-readable diagnostic reason.
    pub reason: String,
    /// Queue insertion time in nanoseconds since Unix epoch.
    pub enqueued_at: u64,
    /// Last update time in nanoseconds since Unix epoch.
    pub updated_at: u64,
    /// Earliest retry time in nanoseconds since Unix epoch.
    pub next_retry_at: Option<u64>,
    /// Optional topology or failure-domain key used for throttling.
    pub failure_domain: Option<String>,
}

impl EcRepairTaskRecord {
    /// Validate this repair task before durable write.
    pub fn validate(&self) -> Result<(), EcMetadataError> {
        self.object.validate()?;
        if self.repair_id.is_nil() {
            return Err(EcMetadataError::InvalidLayout {
                reason: "repair task repair_id must not be nil".to_string(),
            });
        }
        if self.operation_id.is_nil() {
            return Err(EcMetadataError::InvalidLayout {
                reason: "repair task operation_id must not be nil".to_string(),
            });
        }
        if self.reason.trim().is_empty() {
            return Err(EcMetadataError::InvalidLayout {
                reason: "repair task reason must not be empty".to_string(),
            });
        }
        if self.failure_domain.as_ref().is_some_and(|domain| domain.trim().is_empty()) {
            return Err(EcMetadataError::InvalidLayout {
                reason: "repair task failure_domain must not be empty when present".to_string(),
            });
        }
        Ok(())
    }
}

/// The two ready repair tasks one erasure set offers for its next turn.
///
/// `most_urgent` is the pick of decision EC2-160: the task whose chunk the pool
/// is closest to losing. `longest_waiting` is what the starvation guard of
/// EC2-162 serves on its own turns. They are the same task whenever the most
/// damaged chunk is also the one that has waited longest, which is the ordinary
/// case in a short queue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EcReadyRepairTasks {
    /// Task of the chunk closest to being unrecoverable.
    pub most_urgent: EcRepairTaskRecord,
    /// Oldest ready task of the set.
    pub longest_waiting: EcRepairTaskRecord,
}

/// Durable EC object manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EcObjectManifest {
    /// Manifest format version.
    pub format_version: u8,
    /// Bucket name.
    pub bucket: String,
    /// Object key.
    pub key: String,
    /// Version ID when bucket versioning is enabled.
    pub version_id: Option<String>,
    /// Original object size before encoding.
    pub original_size: u64,
    /// SHA-256 of the complete original object.
    pub content_hash_sha256: [u8; 32],
    /// S3 ETag for the original object.
    pub etag: String,
    /// Content-Type for the original object.
    pub content_type: String,
    /// User-defined metadata from S3 headers.
    pub user_metadata: HashMap<String, String>,
    /// Codec profile snapshot used to encode this object.
    pub profile: EcProfileSnapshot,
    /// Erasure set that owns the shard slots.
    pub erasure_set_id: u64,
    /// Topology epoch used for shard placement.
    pub topology_epoch: u64,
    /// Per-chunk shard manifests.
    pub chunks: Vec<ChunkManifest>,
    /// Current placement state.
    pub state: EcPlacementState,
    /// HLC timestamp for conflict ordering.
    pub hlc: HlcTimestamp,
    /// Node that originated this manifest.
    pub origin_node_id: NodeId,
    /// Idempotency ID for the operation that produced this manifest.
    pub operation_id: Uuid,
    /// Creation timestamp in nanoseconds since Unix epoch.
    pub created_at: u64,
    /// True when this manifest represents a deleted object/version.
    pub is_deleted: bool,
    /// Where the bytes live when the object is inside a packed segment.
    ///
    /// `None` for a classic object, whose bytes are described by `chunks`, and
    /// for every tombstone. `Some` only together with
    /// [`EC_PACKED_MANIFEST_FORMAT_VERSION`] and an empty `chunks`.
    ///
    /// Deliberately absent from the serialized body: bincode is positional, so
    /// a field written into the body would make every manifest recorded by an
    /// older build undecodable. The placement is appended after the body
    /// instead — see [`encode_manifest`] — which leaves the bytes of a classic
    /// manifest exactly as they were.
    #[serde(skip)]
    pub packed: Option<PackedSegmentObjectPlacement>,
}

#[derive(Serialize)]
struct CanonicalEcObjectManifest<'a> {
    format_version: u8,
    bucket: &'a str,
    key: &'a str,
    version_id: Option<&'a str>,
    original_size: u64,
    content_hash_sha256: [u8; 32],
    etag: &'a str,
    content_type: &'a str,
    user_metadata: Vec<(&'a str, &'a str)>,
    profile: &'a EcProfileSnapshot,
    erasure_set_id: u64,
    topology_epoch: u64,
    chunks: &'a [ChunkManifest],
    state: EcPlacementState,
    hlc: HlcTimestamp,
    origin_node_id: NodeId,
    operation_id: &'a Uuid,
    created_at: u64,
    is_deleted: bool,
}

impl EcObjectManifest {
    /// Build an object-version key for this manifest.
    pub fn object_key(&self) -> EcObjectVersionKey {
        EcObjectVersionKey::new(
            self.bucket.clone(),
            self.key.clone(),
            self.version_id.clone(),
        )
    }

    /// Build the quorum-durable pointer to an object inside a packed segment.
    ///
    /// This is the object's entire manifest. It answers `HEAD` on its own —
    /// size, ETag, content type and user metadata all live in it — and it tells
    /// a `GET` which segment, offset and length to read. The placement it
    /// carries lists no hot replicas, because the pointer describes the state
    /// the object is about to be left in: served from the segment's shards.
    ///
    /// `created_at` is derived from the entry's HLC rather than read from a
    /// clock, so a retried commit produces byte-identical bytes and the quorum
    /// still agrees on one manifest hash.
    ///
    /// # Errors
    ///
    /// Returns [`EcMetadataError::InvalidLayout`] when the entry does not
    /// describe a usable region of the segment, or the resulting manifest fails
    /// [`EcObjectManifest::validate`].
    pub fn packed_pointer(
        segment: &PackedSegment,
        entry: &PackedSegmentEntry,
        profile: EcProfileSnapshot,
        topology_epoch: u64,
        origin_node_id: NodeId,
        operation_id: Uuid,
    ) -> Result<Self, EcMetadataError> {
        let placement = PackedSegmentObjectPlacement::from_entry(segment, entry, Vec::new())?;
        let manifest = Self {
            format_version: EC_PACKED_MANIFEST_FORMAT_VERSION,
            bucket: entry.object.bucket.clone(),
            key: entry.object.key.clone(),
            version_id: entry.object.version_id.clone(),
            original_size: entry.length,
            content_hash_sha256: entry.content_hash_sha256,
            etag: entry.etag.clone(),
            content_type: entry.content_type.clone(),
            user_metadata: entry.user_metadata.clone(),
            profile,
            erasure_set_id: segment.erasure_set_id,
            topology_epoch,
            chunks: Vec::new(),
            state: EcPlacementState::EcCommitted,
            hlc: entry.hlc,
            origin_node_id,
            operation_id,
            created_at: entry.hlc.wall_time.saturating_mul(1_000_000),
            is_deleted: false,
            packed: Some(placement),
        };
        manifest.validate()?;
        Ok(manifest)
    }

    /// Compute SHA-256 over the serialized manifest.
    pub fn serialized_sha256(&self) -> Result<[u8; 32], EcMetadataError> {
        let bytes = self.canonical_hash_bytes()?;
        Ok(sha256(&bytes))
    }

    fn canonical_hash_bytes(&self) -> Result<Vec<u8>, EcMetadataError> {
        let mut user_metadata: Vec<_> = self
            .user_metadata
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
            .collect();
        user_metadata.sort_unstable_by(|left, right| left.0.cmp(right.0).then(left.1.cmp(right.1)));

        let canonical = CanonicalEcObjectManifest {
            format_version: self.format_version,
            bucket: self.bucket.as_str(),
            key: self.key.as_str(),
            version_id: self.version_id.as_deref(),
            original_size: self.original_size,
            content_hash_sha256: self.content_hash_sha256,
            etag: self.etag.as_str(),
            content_type: self.content_type.as_str(),
            user_metadata,
            profile: &self.profile,
            erasure_set_id: self.erasure_set_id,
            topology_epoch: self.topology_epoch,
            chunks: &self.chunks,
            state: self.state,
            hlc: self.hlc,
            origin_node_id: self.origin_node_id,
            operation_id: &self.operation_id,
            created_at: self.created_at,
            is_deleted: self.is_deleted,
        };

        let mut bytes = serialize(&canonical)?;
        // Appended, not added as a field, for the same reason the wire body
        // appends it: the canonical bytes of a classic manifest must stay
        // identical, or every `manifest_hash_sha256` already recorded in a
        // layout reference stops matching its manifest.
        if let Some(packed) = &self.packed {
            bytes.extend_from_slice(&serialize(packed)?);
        }
        Ok(bytes)
    }

    /// Convert this manifest into a metadata read result.
    pub fn into_read_result(self) -> EcManifestReadResult {
        if self.is_deleted || self.state == EcPlacementState::Tombstone {
            EcManifestReadResult::Tombstone(self)
        } else {
            EcManifestReadResult::Present(self)
        }
    }

    /// Validate manifest self-consistency without trusting local shard locations.
    pub fn validate(&self) -> Result<(), EcMetadataError> {
        validate_object_key(&self.bucket, &self.key, self.version_id.as_deref())?;
        match self.format_version {
            EC_MANIFEST_FORMAT_VERSION => {
                if self.packed.is_some() {
                    return self.invalid(format!(
                        "a packed placement needs format_version \
                         {EC_PACKED_MANIFEST_FORMAT_VERSION}"
                    ));
                }
            }
            EC_PACKED_MANIFEST_FORMAT_VERSION => {
                if self.packed.is_none() {
                    return self.invalid(format!(
                        "format_version {EC_PACKED_MANIFEST_FORMAT_VERSION} is only for a \
                         manifest that carries a packed placement"
                    ));
                }
            }
            other => {
                return self.invalid(format!(
                    "unsupported format_version {other}; this build writes \
                     {EC_MANIFEST_FORMAT_VERSION} and {EC_PACKED_MANIFEST_FORMAT_VERSION}"
                ));
            }
        }
        if self.operation_id.is_nil() {
            return self.invalid("operation_id must not be nil");
        }
        self.profile
            .validate()
            .map_err(|error| EcMetadataError::Codec(error.to_string()))?;

        if self.is_deleted || self.state == EcPlacementState::Tombstone {
            if !self.chunks.is_empty() {
                return self.invalid("tombstone manifest must not contain chunks");
            }
            // A tombstone describes no placement, so it stays on the format
            // every node can read, packed object or not.
            if self.packed.is_some() {
                return self.invalid("tombstone manifest must not carry a packed placement");
            }
            return Ok(());
        }

        if let Some(packed) = &self.packed {
            packed.validate()?;
            if !self.chunks.is_empty() {
                return self.invalid(
                    "a packed manifest points at a segment and must not contain chunks of its own",
                );
            }
            if packed.length != self.original_size {
                return self.invalid(format!(
                    "packed placement covers {} byte(s), expected original_size {}",
                    packed.length, self.original_size
                ));
            }
            if packed.content_hash_sha256 != self.content_hash_sha256 {
                return self.invalid("packed placement content hash does not match the manifest");
            }
            if packed.erasure_set_id != self.erasure_set_id {
                return self.invalid(format!(
                    "packed placement names erasure set {}, manifest names {}",
                    packed.erasure_set_id, self.erasure_set_id
                ));
            }
            return Ok(());
        }

        if self.chunks.is_empty() {
            return self.invalid("live EC manifest must contain at least one chunk");
        }

        let mut expected_chunk_index = 0u32;
        let mut logical_size = 0u64;
        for chunk in &self.chunks {
            if chunk.chunk_index != expected_chunk_index {
                return self.invalid(format!(
                    "chunk index sequence must be contiguous from 0; expected {}, got {}",
                    expected_chunk_index, chunk.chunk_index
                ));
            }
            chunk
                .validate(&self.profile)
                .map_err(|error| self.with_manifest_context(error))?;
            logical_size = logical_size.checked_add(chunk.data_size).ok_or_else(|| {
                EcMetadataError::InvalidManifest {
                    bucket: self.bucket.clone(),
                    key: self.key.clone(),
                    reason: "chunk data sizes overflow u64".to_string(),
                }
            })?;
            expected_chunk_index = expected_chunk_index.checked_add(1).ok_or_else(|| {
                EcMetadataError::InvalidManifest {
                    bucket: self.bucket.clone(),
                    key: self.key.clone(),
                    reason: "too many chunks for u32 chunk_index".to_string(),
                }
            })?;
        }

        if logical_size != self.original_size {
            return self.invalid(format!(
                "chunk data sizes sum to {logical_size} byte(s), expected original_size {}",
                self.original_size
            ));
        }

        Ok(())
    }

    /// Validate this manifest against an activated erasure set.
    ///
    /// The manifest is the source of truth about where its own shards are
    /// (EC2-086, point 1). So a shard's node is checked for membership in the
    /// set, not for holding the same slot in the set's current layout: that
    /// layout only decides where new objects go, and a label change or a
    /// layout record adopted since the write may have reordered it. Everything
    /// else is still checked — the set the manifest names, the profile, the
    /// epoch, and each shard's role against its slot index.
    ///
    /// Membership is enough because what the slot check used to guard is
    /// guarded elsewhere: the sets of a pool never share a node, a shard key
    /// names the object, operation, chunk and slot, and every shard is verified
    /// against the SHA-256 in this manifest before it is used.
    pub fn validate_against_erasure_set(
        &self,
        erasure_set: &ErasureSet,
    ) -> Result<(), EcMetadataError> {
        self.validate()?;
        let expected_profile = erasure_set.profile.codec_snapshot();
        if self.profile != expected_profile {
            return self.invalid(format!(
                "manifest profile '{}' does not match erasure set profile '{}'",
                self.profile.name, expected_profile.name
            ));
        }
        // Not a raw `== self.set_id`: manifests written before set_id was
        // decoupled from pool_id name the pool, and they still belong to the
        // set that inherited the pool's identity. The set decides what it
        // answers to; see decision EC2-041.
        if !erasure_set.accepts_manifest_set_id(self.erasure_set_id) {
            return self.invalid(format!(
                "manifest erasure_set_id {} does not belong to erasure set {} of pool {}",
                self.erasure_set_id, erasure_set.set_id, erasure_set.pool_id
            ));
        }
        if self.topology_epoch != erasure_set.topology_epoch {
            return self.invalid(format!(
                "manifest topology_epoch {} does not match active set epoch {}",
                self.topology_epoch, erasure_set.topology_epoch
            ));
        }

        for chunk in &self.chunks {
            // Distinct slots are checked by `ChunkManifest::validate`. Distinct
            // nodes used to follow from matching every shard to its slot; with
            // membership alone they have to be checked here, or one node could
            // hold two shards of a chunk and take both with it when it fails.
            let mut chunk_nodes = HashSet::with_capacity(chunk.shards.len());
            for shard in &chunk.shards {
                let Some(slot) = erasure_set
                    .slot_assignment
                    .slots()
                    .iter()
                    .find(|slot| slot.slot_index == shard.slot_index)
                else {
                    return self.invalid(format!(
                        "shard slot {} is not present in erasure set {}",
                        shard.slot_index, erasure_set.set_id
                    ));
                };
                let expected_role = shard_role_from_slot(slot.role);
                if !erasure_set.holds_node(shard.node_id) {
                    return self.invalid(format!(
                        "chunk {} slot {} is placed on node {}, which is not a node of erasure \
                         set {}",
                        chunk.chunk_index, shard.slot_index, shard.node_id, erasure_set.set_id
                    ));
                }
                if !chunk_nodes.insert(shard.node_id) {
                    return self.invalid(format!(
                        "chunk {} places more than one shard on node {}",
                        chunk.chunk_index, shard.node_id
                    ));
                }
                if shard.role != expected_role {
                    return self.invalid(format!(
                        "chunk {} slot {} has role {:?}, expected {:?}",
                        chunk.chunk_index, shard.slot_index, shard.role, expected_role
                    ));
                }
            }
        }

        Ok(())
    }

    fn invalid<T>(&self, reason: impl Into<String>) -> Result<T, EcMetadataError> {
        Err(EcMetadataError::InvalidManifest {
            bucket: self.bucket.clone(),
            key: self.key.clone(),
            reason: reason.into(),
        })
    }

    fn with_manifest_context(&self, error: EcMetadataError) -> EcMetadataError {
        match error {
            EcMetadataError::InvalidManifest { reason, .. } => EcMetadataError::InvalidManifest {
                bucket: self.bucket.clone(),
                key: self.key.clone(),
                reason,
            },
            other => other,
        }
    }
}

/// Manifest for one logical EC chunk.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkManifest {
    /// Zero-based chunk index.
    pub chunk_index: u32,
    /// Original byte length of this chunk before encoding.
    pub data_size: u64,
    /// Encoded byte length of each shard in this chunk.
    pub encoded_size: u64,
    /// SHA-256 of the original unencoded chunk bytes.
    pub chunk_sha256: [u8; 32],
    /// Per-shard expected placement and hashes.
    pub shards: Vec<ShardManifest>,
}

impl ChunkManifest {
    /// Validate chunk-level shard metadata against the profile.
    pub fn validate(&self, profile: &EcProfileSnapshot) -> Result<(), EcMetadataError> {
        if self.data_size == 0 {
            return Err(invalid_manifest_without_object(format!(
                "chunk {} data_size must be greater than 0",
                self.chunk_index
            )));
        }
        if self.data_size > profile.chunk_size {
            return Err(invalid_manifest_without_object(format!(
                "chunk {} data_size {} exceeds profile chunk_size {}",
                self.chunk_index, self.data_size, profile.chunk_size
            )));
        }
        if self.encoded_size == 0 {
            return Err(invalid_manifest_without_object(format!(
                "chunk {} encoded_size must be greater than 0",
                self.chunk_index
            )));
        }
        let encoded_capacity =
            self.encoded_size.checked_mul(u64::from(profile.k)).ok_or_else(|| {
                invalid_manifest_without_object(format!(
                    "chunk {} encoded capacity overflows u64",
                    self.chunk_index
                ))
            })?;
        if encoded_capacity < self.data_size {
            return Err(invalid_manifest_without_object(format!(
                "chunk {} encoded capacity {} is smaller than data_size {}",
                self.chunk_index, encoded_capacity, self.data_size
            )));
        }
        if self.shards.len() != usize::from(profile.total_shards()) {
            return Err(invalid_manifest_without_object(format!(
                "chunk {} must contain {} shard manifest(s), got {}",
                self.chunk_index,
                profile.total_shards(),
                self.shards.len()
            )));
        }

        let mut seen_slots = HashSet::with_capacity(self.shards.len());
        for shard in &self.shards {
            if !seen_slots.insert(shard.slot_index) {
                return Err(invalid_manifest_without_object(format!(
                    "chunk {} contains duplicate shard slot {}",
                    self.chunk_index, shard.slot_index
                )));
            }
            shard.validate(profile, self.chunk_index, self.encoded_size)?;
        }

        Ok(())
    }
}

/// Manifest entry for one shard.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardManifest {
    /// Zero-based shard slot index.
    pub slot_index: u8,
    /// Node assigned to this shard slot.
    pub node_id: NodeId,
    /// Shard role in the codec profile.
    pub role: ShardRole,
    /// CRC32 of the shard bytes.
    pub crc32: u32,
    /// SHA-256 of the shard bytes.
    pub sha256: [u8; 32],
    /// Shard size in bytes.
    pub size: u64,
}

impl ShardManifest {
    /// Build a manifest entry from a codec shard and assigned node.
    pub fn from_codec_shard(shard: &s4_codec::Shard, node_id: NodeId) -> Self {
        Self {
            slot_index: shard.slot_index,
            node_id,
            role: shard.role,
            crc32: shard.crc32,
            sha256: shard.sha256,
            size: shard.size,
        }
    }

    /// Validate this entry against the profile and encoded shard size.
    pub fn validate(
        &self,
        profile: &EcProfileSnapshot,
        chunk_index: u32,
        encoded_size: u64,
    ) -> Result<(), EcMetadataError> {
        let expected_role = profile
            .role_for_slot(self.slot_index)
            .map_err(|error| EcMetadataError::Codec(error.to_string()))?;
        if self.role != expected_role {
            return Err(invalid_manifest_without_object(format!(
                "chunk {chunk_index} slot {} has role {:?}, expected {:?}",
                self.slot_index, self.role, expected_role
            )));
        }
        if self.size != encoded_size {
            return Err(invalid_manifest_without_object(format!(
                "chunk {chunk_index} slot {} size {} does not match encoded_size {}",
                self.slot_index, self.size, encoded_size
            )));
        }
        Ok(())
    }

    /// Verify shard bytes against the manifest, not against replica claims.
    pub fn verify_bytes(&self, bytes: &[u8]) -> Result<(), EcMetadataError> {
        let actual_size =
            u64::try_from(bytes.len()).map_err(|_| EcMetadataError::InvalidShard {
                reason: "shard byte length does not fit in u64".to_string(),
            })?;
        if actual_size != self.size {
            return Err(EcMetadataError::InvalidShard {
                reason: format!(
                    "slot {} expected {} byte(s), got {}",
                    self.slot_index, self.size, actual_size
                ),
            });
        }
        let actual_crc32 = crc32fast::hash(bytes);
        if actual_crc32 != self.crc32 {
            return Err(EcMetadataError::InvalidShard {
                reason: format!(
                    "slot {} CRC32 mismatch: expected {}, got {}",
                    self.slot_index, self.crc32, actual_crc32
                ),
            });
        }
        let actual_sha256 = sha256(bytes);
        if actual_sha256 != self.sha256 {
            return Err(EcMetadataError::InvalidShard {
                reason: format!("slot {} SHA-256 mismatch", self.slot_index),
            });
        }
        Ok(())
    }
}

/// Local key for one shard record.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LocalShardKey {
    /// Object version addressed by this local shard.
    pub object: EcObjectVersionKey,
    /// Logical chunk index.
    pub chunk_index: u32,
    /// Shard slot index.
    pub slot_index: u8,
}

impl LocalShardKey {
    /// Build a local shard key.
    pub fn new(
        bucket: impl Into<String>,
        key: impl Into<String>,
        version_id: Option<String>,
        chunk_index: u32,
        slot_index: u8,
    ) -> Self {
        Self {
            object: EcObjectVersionKey::new(bucket, key, version_id),
            chunk_index,
            slot_index,
        }
    }

    /// Validate the local shard key.
    pub fn validate(&self) -> Result<(), EcMetadataError> {
        self.object.validate()
    }
}

/// Node-local lifecycle state for a physical shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LocalShardState {
    /// Shard write is staged but not yet committed.
    Staged,
    /// Shard is durable and may satisfy manifest reads.
    Durable,
    /// Shard is no longer referenced by a live manifest and may be collected.
    GcPending,
}

/// Node-local shard location and integrity metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalShardRecord {
    /// Local volume file ID.
    pub volume_id: u32,
    /// Local byte offset in the volume file.
    pub offset: u64,
    /// Shard size in bytes.
    pub size: u64,
    /// CRC32 of the shard bytes.
    pub crc32: u32,
    /// SHA-256 of the shard bytes.
    pub sha256: [u8; 32],
    /// Local shard lifecycle state.
    pub state: LocalShardState,
    /// Idempotency ID for the operation that wrote this shard.
    pub operation_id: Uuid,
}

impl LocalShardRecord {
    /// Validate the local shard record.
    pub fn validate(&self) -> Result<(), EcMetadataError> {
        if self.size == 0 {
            return Err(EcMetadataError::InvalidShard {
                reason: "local shard size must be greater than 0".to_string(),
            });
        }
        if self.operation_id.is_nil() {
            return Err(EcMetadataError::InvalidShard {
                reason: "local shard operation_id must not be nil".to_string(),
            });
        }
        Ok(())
    }

    /// Verify that the local location metadata agrees with the global manifest.
    ///
    /// This compares identity and integrity fields only. The volume location is
    /// deliberately node-local and is never expected to appear in the manifest.
    pub fn verify_against_manifest(&self, manifest: &ShardManifest) -> Result<(), EcMetadataError> {
        self.validate()?;
        if self.size != manifest.size {
            return Err(EcMetadataError::InvalidShard {
                reason: format!(
                    "local shard size {} does not match manifest size {}",
                    self.size, manifest.size
                ),
            });
        }
        if self.crc32 != manifest.crc32 {
            return Err(EcMetadataError::InvalidShard {
                reason: "local shard CRC32 does not match manifest".to_string(),
            });
        }
        if self.sha256 != manifest.sha256 {
            return Err(EcMetadataError::InvalidShard {
                reason: "local shard SHA-256 does not match manifest".to_string(),
            });
        }
        Ok(())
    }
}

/// Result of an EC manifest metadata read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EcManifestReadResult {
    /// A live manifest was found.
    Present(EcObjectManifest),
    /// A tombstone manifest was found.
    Tombstone(EcObjectManifest),
    /// Quorum confirmed that the object/version is absent.
    Absent,
}

/// One page from an EC metadata keyspace scan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EcMetadataPage<T> {
    /// Entries returned in this page.
    pub entries: Vec<T>,
    /// Opaque cursor to pass to the next page request.
    pub next_cursor: Option<Vec<u8>>,
}

/// Failure class for a staged EC transcode attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EcTranscodeFailureClass {
    /// Retrying the same operation may succeed without operator action.
    Retryable,
    /// Retrying the same target profile/set is not expected to succeed.
    Permanent,
}

/// Durable reason attached to a failed EC transcode job.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EcTranscodeFailureRecord {
    /// Failure class used by retry scheduling and admin surfaces.
    pub class: EcTranscodeFailureClass,
    /// Human-readable failure reason.
    pub reason: String,
}

impl EcTranscodeFailureRecord {
    /// Build a retryable failure record.
    pub fn retryable(reason: impl Into<String>) -> Self {
        Self {
            class: EcTranscodeFailureClass::Retryable,
            reason: reason.into(),
        }
    }

    /// Build a permanent failure record.
    pub fn permanent(reason: impl Into<String>) -> Self {
        Self {
            class: EcTranscodeFailureClass::Permanent,
            reason: reason.into(),
        }
    }
}

/// What caused a staged EC transcode job to exist.
///
/// A client write and a tiering migration run through the same staged
/// pipeline, but they are not the same event to an operator: one is the cost of
/// storing what a user just sent, the other is background work the node chose
/// to do. Keeping them apart lets the queue ceiling from EC2-022 bound only the
/// second, and lets metrics and logs say which kind of work is running.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EcTranscodeOrigin {
    /// The object was written by a client and is being encoded on its way in.
    #[default]
    ClientWrite,
    /// The auto-tiering worker picked a cold object for migration.
    Tiering,
}

impl EcTranscodeOrigin {
    /// Whether this job was created by the auto-tiering worker.
    pub fn is_tiering(self) -> bool {
        matches!(self, Self::Tiering)
    }
}

/// What a transcode job encodes.
///
/// The staged pipeline is the same for both — read the bytes, cut them into
/// shards, commit the manifest, verify, cut over — but the two differ at the
/// ends. A segment's bytes are gathered from the objects packed into it rather
/// than read as one object, and its cutover switches every one of those objects
/// at once rather than a single layout. Naming the target explicitly keeps that
/// difference out of guesswork; the alternative, inferring it from the reserved
/// bucket a segment's key lives in, would make the pipeline depend on the shape
/// of a name.
///
/// Defaulted so that a job built without naming a target encodes an object,
/// which is what every job did before packed segments existed.
///
/// The default does **not** make records written by an older build readable.
/// The queue is bincode, which is positional and not self-describing: a record
/// that ends before this field cannot be decoded at all, and `serde(default)`
/// never sees it. `list_transcode_jobs_page` therefore skips such records
/// instead of failing, which is what actually carries the queue across an
/// upgrade. The same is true of `origin` above.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EcTranscodeTarget {
    /// One client object.
    #[default]
    Object,
    /// A whole packed segment, encoded as a single blob.
    PackedSegment,
}

impl EcTranscodeTarget {
    /// Whether this job encodes a packed segment rather than a single object.
    pub fn is_packed_segment(self) -> bool {
        matches!(self, Self::PackedSegment)
    }
}

/// What removing one object's transcode jobs actually removed.
///
/// The two counts are not the same question. The total says how much queue
/// state the object left behind; the tiering count says how many background
/// migrations were abandoned because the client changed the object, which is
/// the number an operator watches to tell a busy pool from a misconfigured one.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EcTranscodeJobsRemoved {
    /// Jobs removed, whatever created them.
    pub total: usize,
    /// Of those, jobs the auto-tiering worker had queued.
    pub tiering: usize,
}

/// Durable state for one staged EC transcode operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EcTranscodeJobRecord {
    /// Object version being transcoded.
    pub object: EcObjectVersionKey,
    /// Deterministic idempotency ID for this source object and target set.
    pub operation_id: Uuid,
    /// HLC of the RF=3 source object/version.
    pub source_hlc: HlcTimestamp,
    /// Target erasure set.
    pub target_erasure_set_id: u64,
    /// Current placement/transcode state.
    pub state: EcPlacementState,
    /// Number of attempts already made.
    pub attempt: u32,
    /// Original object size.
    pub object_size: u64,
    /// SHA-256 of the RF=3 source object bytes.
    pub source_content_hash_sha256: [u8; 32],
    /// Last update time in nanoseconds since Unix epoch.
    pub updated_at: u64,
    /// Earliest retry time in nanoseconds since Unix epoch.
    pub next_retry_at: Option<u64>,
    /// Failure details, when the job is in a failed state.
    pub failure: Option<EcTranscodeFailureRecord>,
    /// What created this job.
    ///
    /// Defaulted so a job built without naming an origin counts as a client
    /// write, which is what every job was before auto-tiering existed.
    ///
    /// This does not make older records readable — see [`EcTranscodeTarget`].
    #[serde(default)]
    pub origin: EcTranscodeOrigin,
    /// Whether this job encodes one object or a whole packed segment.
    #[serde(default)]
    pub target: EcTranscodeTarget,
}

impl EcTranscodeJobRecord {
    /// Validate this job record before durable write.
    pub fn validate(&self) -> Result<(), EcMetadataError> {
        self.object.validate()?;
        if self.operation_id.is_nil() {
            return Err(EcMetadataError::InvalidLayout {
                reason: "transcode job operation_id must not be nil".to_string(),
            });
        }
        if self.source_hlc.is_zero() {
            return Err(EcMetadataError::InvalidLayout {
                reason: "transcode job source_hlc must not be zero".to_string(),
            });
        }
        Ok(())
    }
}

/// Renewable lease for one active staged EC transcode.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EcTranscodeLeaseRecord {
    /// Operation protected by this lease.
    pub operation_id: Uuid,
    /// Node currently owning the lease.
    pub owner_node_id: NodeId,
    /// Acquisition time in nanoseconds since Unix epoch.
    pub acquired_at: u64,
    /// Last heartbeat time in nanoseconds since Unix epoch.
    pub last_heartbeat_at: u64,
    /// Lease expiration time in nanoseconds since Unix epoch.
    pub expires_at: u64,
}

impl EcTranscodeLeaseRecord {
    /// Validate this lease record before durable write.
    pub fn validate(&self) -> Result<(), EcMetadataError> {
        if self.operation_id.is_nil() {
            return Err(EcMetadataError::InvalidLayout {
                reason: "transcode lease operation_id must not be nil".to_string(),
            });
        }
        if self.expires_at <= self.last_heartbeat_at {
            return Err(EcMetadataError::InvalidLayout {
                reason: "transcode lease expires_at must be after last_heartbeat_at".to_string(),
            });
        }
        Ok(())
    }

    /// True if the lease has not expired at `now`.
    pub fn is_alive_at(&self, now: u64) -> bool {
        self.expires_at > now
    }
}

/// Durable gate for hot RF=3 replica garbage collection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EcHotReplicaGcRecord {
    /// Object version whose RF=3 replicas can eventually be removed.
    pub object: EcObjectVersionKey,
    /// EC operation that won cutover for this object version.
    pub operation_id: Uuid,
    /// HLC of the RF=3 source generation that may be physically purged.
    #[serde(default = "HlcTimestamp::zero")]
    pub source_hlc: HlcTimestamp,
    /// Earliest purge time in nanoseconds since Unix epoch.
    pub eligible_at: u64,
    /// Whether full EC SHA-256 verification has completed.
    pub full_object_sha256_verified: bool,
    /// Actual purge time in nanoseconds since Unix epoch.
    pub purged_at: Option<u64>,
    /// Last update time in nanoseconds since Unix epoch.
    pub updated_at: u64,
}

impl EcHotReplicaGcRecord {
    /// Validate this GC record before durable write.
    pub fn validate(&self) -> Result<(), EcMetadataError> {
        self.object.validate()?;
        if self.operation_id.is_nil() {
            return Err(EcMetadataError::InvalidLayout {
                reason: "hot replica GC operation_id must not be nil".to_string(),
            });
        }
        if self.source_hlc.is_zero() {
            return Err(EcMetadataError::InvalidLayout {
                reason: "hot replica GC source_hlc must not be zero".to_string(),
            });
        }
        Ok(())
    }
}

/// Durable lifecycle state for an orphan EC shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EcOrphanShardState {
    /// Shard is known to be unreferenced but has not passed GC safety gates.
    Candidate,
    /// Shard passed GC safety gates and is being removed locally.
    GcPending,
    /// Shard was physically removed and its metadata can be forgotten.
    Purged,
}

/// Durable key for one orphan EC shard candidate.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EcOrphanShardKey {
    /// Operation that produced the orphan shard.
    pub operation_id: Uuid,
    /// Source object version for the operation.
    pub object: EcObjectVersionKey,
    /// Logical chunk index.
    pub chunk_index: u32,
    /// Shard slot index.
    pub slot_index: u8,
}

impl EcOrphanShardKey {
    /// Build an orphan shard key.
    pub fn new(
        operation_id: Uuid,
        object: EcObjectVersionKey,
        chunk_index: u32,
        slot_index: u8,
    ) -> Self {
        Self {
            operation_id,
            object,
            chunk_index,
            slot_index,
        }
    }

    /// Validate this orphan shard key.
    pub fn validate(&self) -> Result<(), EcMetadataError> {
        self.object.validate()?;
        if self.operation_id.is_nil() {
            return Err(EcMetadataError::InvalidLayout {
                reason: "orphan shard operation_id must not be nil".to_string(),
            });
        }
        Ok(())
    }
}

/// Durable GC record for one orphan EC shard candidate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EcOrphanShardRecord {
    /// Operation that produced the orphan shard.
    pub operation_id: Uuid,
    /// Source object version for the operation.
    pub source_object: EcObjectVersionKey,
    /// Target erasure set for the failed or losing operation.
    pub target_erasure_set_id: u64,
    /// Logical chunk index.
    pub chunk_index: u32,
    /// Shard slot index.
    pub slot_index: u8,
    /// Local physical shard metadata captured before GC.
    pub local_shard: LocalShardRecord,
    /// First observation time in nanoseconds since Unix epoch.
    pub created_at: u64,
    /// Last observed lease acquisition timestamp for this operation.
    pub lease_epoch: u64,
    /// Last observed lease heartbeat timestamp for this operation.
    pub last_lease_seen_at: Option<u64>,
    /// Orphan shard lifecycle state.
    pub state: EcOrphanShardState,
    /// Human-readable reason recorded by scrubber or write path.
    pub reason: String,
}

impl EcOrphanShardRecord {
    /// Build the durable key for this orphan record.
    pub fn key(&self) -> EcOrphanShardKey {
        EcOrphanShardKey::new(
            self.operation_id,
            self.source_object.clone(),
            self.chunk_index,
            self.slot_index,
        )
    }

    /// Validate this orphan record before durable write.
    pub fn validate(&self) -> Result<(), EcMetadataError> {
        self.source_object.validate()?;
        if self.operation_id.is_nil() {
            return Err(EcMetadataError::InvalidLayout {
                reason: "orphan shard operation_id must not be nil".to_string(),
            });
        }
        if self.local_shard.operation_id != self.operation_id {
            return Err(EcMetadataError::InvalidLayout {
                reason: "orphan shard local_shard operation_id must match record operation_id"
                    .to_string(),
            });
        }
        // PoolId(0) is valid in single-pool clusters, so erasure set 0 is valid too.
        if self.created_at == 0 {
            return Err(EcMetadataError::InvalidLayout {
                reason: "orphan shard created_at must be greater than 0".to_string(),
            });
        }
        if self.reason.trim().is_empty() {
            return Err(EcMetadataError::InvalidLayout {
                reason: "orphan shard reason must not be empty".to_string(),
            });
        }
        self.local_shard.validate()
    }

    /// Validate that this record belongs under `key`.
    pub fn validate_for_key(&self, key: &EcOrphanShardKey) -> Result<(), EcMetadataError> {
        self.validate()?;
        key.validate()?;
        if self.operation_id != key.operation_id
            || self.source_object != key.object
            || self.chunk_index != key.chunk_index
            || self.slot_index != key.slot_index
        {
            return Err(EcMetadataError::InvalidLayout {
                reason: "orphan shard record does not match its durable key".to_string(),
            });
        }
        Ok(())
    }
}

/// Durable GC gate for a replicated-small object in an EC bucket.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EcReplicatedSmallGcRecord {
    /// Object version whose explicit RF=3 replicas are deleted.
    pub object: EcObjectVersionKey,
    /// Delete operation that produced the tombstone.
    pub operation_id: Uuid,
    /// Original explicit RF=3 placement.
    pub placement: ReplicatedSmallObjectPlacement,
    /// HLC timestamp of the delete marker.
    pub tombstone_hlc: HlcTimestamp,
    /// Earliest physical purge time in nanoseconds since Unix epoch.
    pub eligible_at: u64,
    /// Actual physical purge time in nanoseconds since Unix epoch.
    pub purged_at: Option<u64>,
    /// Last update time in nanoseconds since Unix epoch.
    pub updated_at: u64,
}

impl EcReplicatedSmallGcRecord {
    /// Validate this small-object GC record before durable write.
    pub fn validate(&self) -> Result<(), EcMetadataError> {
        self.object.validate()?;
        self.placement.validate()?;
        if self.operation_id.is_nil() {
            return Err(EcMetadataError::InvalidLayout {
                reason: "replicated-small GC operation_id must not be nil".to_string(),
            });
        }
        if self.tombstone_hlc.is_zero() {
            return Err(EcMetadataError::InvalidLayout {
                reason: "replicated-small GC tombstone_hlc must not be zero".to_string(),
            });
        }
        Ok(())
    }
}

// The floor a bucket policy may not go under. Defined once, in the edition
// neutral admin contract, so the durable record and the API that accepts it
// cannot drift apart. Decision: EC2-021.
pub use s4_cluster::MIN_TIERING_COLD_AFTER_SECS;

/// Recorded activity of one object version.
///
/// Written by the heat tracker, read by the tiering scanner. The mark is an
/// approximation on purpose: signals are collapsed in memory and flushed in
/// batches, so an object that is read constantly still costs at most a handful
/// of writes per day. Decision: EC2-020.
///
/// The record is node-local. Reads of one object spread across the nodes that
/// serve them, so a single node's mark is a lower bound on activity, never a
/// measure of it; a migration decision has to take the newest mark across the
/// nodes of the set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EcObjectHeatRecord {
    /// Object version this mark belongs to.
    pub object: EcObjectVersionKey,
    /// Last observed access in nanoseconds since Unix epoch.
    pub last_access_nanos: u64,
    /// Reads counted over the trailing window.
    pub window: EcHeatWindow,
    /// How many times this object has already migrated between layouts.
    pub migrations: u32,
    /// Last migration time in nanoseconds since Unix epoch.
    pub last_migration_nanos: Option<u64>,
    /// Last update time in nanoseconds since Unix epoch.
    pub updated_at: u64,
}

impl EcObjectHeatRecord {
    /// Build a first mark for an object that has just been accessed.
    pub fn first_access(object: EcObjectVersionKey, access_nanos: u64, window_nanos: u64) -> Self {
        let mut window = EcHeatWindow::new(window_nanos, access_nanos);
        window.record(access_nanos, 1);
        Self {
            object,
            last_access_nanos: access_nanos,
            window,
            migrations: 0,
            last_migration_nanos: None,
            updated_at: access_nanos,
        }
    }

    /// Validate this activity record before durable write.
    pub fn validate(&self) -> Result<(), EcMetadataError> {
        self.object.validate()?;
        if self.last_access_nanos == 0 {
            return Err(EcMetadataError::InvalidLayout {
                reason: "object heat last_access_nanos must not be zero".to_string(),
            });
        }
        self.window.validate()?;
        if self.window.newest_slot_started_nanos > self.last_access_nanos {
            return Err(EcMetadataError::InvalidLayout {
                reason: "object heat window starts after its last access".to_string(),
            });
        }
        Ok(())
    }
}

/// How many slots one counting window is divided into.
///
/// The count has to slide, not reset: an object read steadily across a window
/// boundary would otherwise never reach a promotion threshold that it plainly
/// deserves, and one read moments after a reset would report an object as
/// untouched. Keeping a timestamp per read would slide exactly and cost
/// unbounded space, so the window is a ring of counters instead: reads land in
/// the newest slot, and slots older than the window fall off the end.
///
/// Eight is the point where the error stops mattering: the count is behind by
/// at most one slot, which is three hours in the default 24-hour window, and
/// the record grows by 32 bytes.
pub const EC_HEAT_WINDOW_SLOTS: usize = 8;

/// Reads counted over a trailing window, as a ring of per-slot counters.
///
/// Self-describing on purpose: the slot width is stored rather than derived
/// from the current configuration, so changing `S4_EC_TIERING_PROMOTE_WINDOW_SECS`
/// re-scales existing records instead of silently reinterpreting their counts
/// as belonging to a window they were never measured over.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EcHeatWindow {
    /// Width of one slot in nanoseconds.
    pub slot_width_nanos: u64,
    /// Start of the newest slot in nanoseconds since Unix epoch.
    pub newest_slot_started_nanos: u64,
    /// Read counts, newest slot first.
    pub slots: Vec<u32>,
}

impl EcHeatWindow {
    /// Build an empty window of the given width, starting at `now_nanos`.
    ///
    /// A window narrower than the slot count degenerates to one nanosecond per
    /// slot rather than to zero, so that arithmetic on it stays defined.
    pub fn new(window_nanos: u64, now_nanos: u64) -> Self {
        let slot_width_nanos = (window_nanos / EC_HEAT_WINDOW_SLOTS as u64).max(1);
        Self {
            slot_width_nanos,
            newest_slot_started_nanos: now_nanos,
            slots: vec![0; EC_HEAT_WINDOW_SLOTS],
        }
    }

    /// Full width of the window this ring covers.
    pub fn width_nanos(&self) -> u64 {
        self.slot_width_nanos.saturating_mul(self.slots.len() as u64)
    }

    /// Add `reads` observed at `now_nanos`.
    ///
    /// An access older than the newest slot is folded into that slot rather
    /// than dropped: marks arrive collapsed and slightly out of order, and
    /// losing them would understate exactly the traffic being measured.
    pub fn record(&mut self, now_nanos: u64, reads: u32) {
        self.advance(now_nanos);
        if let Some(newest) = self.slots.first_mut() {
            *newest = newest.saturating_add(reads);
        }
    }

    /// Reads counted in the window ending at `now_nanos`.
    pub fn reads_at(&self, now_nanos: u64) -> u32 {
        self.reads_within(now_nanos, self.width_nanos())
    }

    /// Reads counted in a trailing window of `window_nanos` ending at `now_nanos`.
    ///
    /// A bucket may ask for a narrower window than the node records over, and
    /// this answers it exactly rather than handing back a wider count: the ring
    /// stores its own slot width, so a shorter window is simply fewer slots.
    /// Asking for a wider one gives what is stored, because reads that fell off
    /// the end are gone.
    pub fn reads_within(&self, now_nanos: u64, window_nanos: u64) -> u32 {
        let elapsed_slots = self.elapsed_slots(now_nanos);
        if elapsed_slots >= self.slots.len() {
            return 0;
        }
        // Rounded up: a window of one and a half slots has to include the whole
        // second slot, because there is no way to tell where inside it the
        // reads landed, and undercounting a promotion is the safer error only
        // until it makes the threshold unreachable.
        let width = self.slot_width_nanos.max(1);
        let wanted_slots = usize::try_from(window_nanos.div_ceil(width)).unwrap_or(usize::MAX);
        let live_slots = (self.slots.len() - elapsed_slots).min(wanted_slots);
        self.slots
            .iter()
            .take(live_slots)
            .fold(0u32, |total, count| total.saturating_add(*count))
    }

    /// Rebuild this window for a different configured width, keeping the total.
    ///
    /// Called when the configured window no longer matches the stored one. The
    /// counts cannot be re-bucketed honestly, so they are collapsed into the
    /// newest slot: the object keeps the credit it earned and loses only the
    /// knowledge of when within the window it earned it.
    pub fn rescale(&mut self, window_nanos: u64, now_nanos: u64) {
        let carried = self.reads_at(now_nanos);
        *self = Self::new(window_nanos, now_nanos);
        if let Some(newest) = self.slots.first_mut() {
            *newest = carried;
        }
    }

    /// Validate this window before durable write.
    pub fn validate(&self) -> Result<(), EcMetadataError> {
        if self.slot_width_nanos == 0 {
            return Err(EcMetadataError::InvalidLayout {
                reason: "object heat window slot width must not be zero".to_string(),
            });
        }
        if self.slots.is_empty() {
            return Err(EcMetadataError::InvalidLayout {
                reason: "object heat window must have at least one slot".to_string(),
            });
        }
        Ok(())
    }

    /// Slide the ring forward so that `now_nanos` falls in the newest slot.
    fn advance(&mut self, now_nanos: u64) {
        let elapsed_slots = self.elapsed_slots(now_nanos);
        if elapsed_slots == 0 {
            return;
        }
        self.newest_slot_started_nanos = self
            .newest_slot_started_nanos
            .saturating_add((elapsed_slots as u64).saturating_mul(self.slot_width_nanos));
        if elapsed_slots >= self.slots.len() {
            self.slots.iter_mut().for_each(|count| *count = 0);
            return;
        }
        self.slots.truncate(self.slots.len() - elapsed_slots);
        self.slots.splice(0..0, std::iter::repeat_n(0, elapsed_slots));
    }

    /// How many whole slots have passed since the newest one started.
    fn elapsed_slots(&self, now_nanos: u64) -> usize {
        let elapsed = now_nanos.saturating_sub(self.newest_slot_started_nanos);
        usize::try_from(elapsed / self.slot_width_nanos.max(1)).unwrap_or(usize::MAX)
    }
}

/// Durable per-bucket tiering policy.
///
/// Every threshold is optional: an unset field means "inherit the global value
/// from `EcTieringConfig`", not "zero". This is what makes the policy a delta
/// over node configuration rather than a second source of truth.
/// Decision: EC2-021.
///
/// This is the storage shape only. The rules that combine a policy with the
/// global configuration and decide what to do with one object live in the
/// tiering domain module, not here.
///
/// There is deliberately no `Default`: a policy without an HLC cannot be
/// ordered against a concurrent write, and [`Self::inheriting`] makes the
/// caller supply one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EcTieringPolicyRecord {
    /// Bucket this policy applies to.
    pub bucket: String,
    /// Whether tiering is enabled for this bucket.
    pub enabled: Option<bool>,
    /// Idle time before an object of this bucket is considered cold.
    pub cold_after_secs: Option<u64>,
    /// Smallest object size this bucket migrates.
    pub min_object_size: Option<u64>,
    /// Key prefixes that never migrate.
    pub exclude_prefixes: Vec<String>,
    /// Object tags that exclude an object from migration.
    pub exclude_tags: HashMap<String, String>,
    /// Reads within the promotion window that return an object to RF=3.
    pub promote_after_reads: Option<u32>,
    /// Width of the promotion counting window.
    pub promote_window_secs: Option<u64>,
    /// HLC of this policy revision.
    ///
    /// Ordering between two concurrent writes is resolved by the storage layer's
    /// own last-writer-wins on the replicated object; this field is what lets a
    /// node compare a revision it just read against the one it has cached, so a
    /// read that raced a write cannot move the bucket backwards.
    pub hlc: HlcTimestamp,
    /// Last update time in nanoseconds since Unix epoch.
    pub updated_at: u64,
}

impl EcTieringPolicyRecord {
    /// Build a policy that overrides nothing and inherits every global value.
    pub fn inheriting(bucket: impl Into<String>, hlc: HlcTimestamp, updated_at: u64) -> Self {
        Self {
            bucket: bucket.into(),
            enabled: None,
            cold_after_secs: None,
            min_object_size: None,
            exclude_prefixes: Vec::new(),
            exclude_tags: HashMap::new(),
            promote_after_reads: None,
            promote_window_secs: None,
            hlc,
            updated_at,
        }
    }

    /// Validate this policy before durable write.
    pub fn validate(&self) -> Result<(), EcMetadataError> {
        if self.bucket.trim().is_empty() {
            return Err(EcMetadataError::InvalidLayout {
                reason: "tiering policy bucket must not be blank".to_string(),
            });
        }
        if self.hlc.is_zero() {
            return Err(EcMetadataError::InvalidLayout {
                reason: "tiering policy hlc must not be zero".to_string(),
            });
        }
        if let Some(cold_after_secs) = self.cold_after_secs {
            if cold_after_secs < MIN_TIERING_COLD_AFTER_SECS {
                return Err(EcMetadataError::InvalidLayout {
                    reason: format!(
                        "tiering policy cold_after_secs must be at least {MIN_TIERING_COLD_AFTER_SECS} seconds"
                    ),
                });
            }
        }
        if self.promote_window_secs == Some(0) {
            return Err(EcMetadataError::InvalidLayout {
                reason: "tiering policy promote_window_secs must be greater than 0".to_string(),
            });
        }
        if self.exclude_prefixes.iter().any(|prefix| prefix.is_empty()) {
            return Err(EcMetadataError::InvalidLayout {
                reason: "tiering policy exclude_prefixes must not contain an empty prefix"
                    .to_string(),
            });
        }
        // The same rule the admin request enforces. This validator is the only
        // gate on a record arriving from another node, so a rule it does not
        // repeat is a rule that can be bypassed — an empty tag name would be
        // listed as an exclusion and match nothing.
        if self.exclude_tags.keys().any(|key| key.is_empty()) {
            return Err(EcMetadataError::InvalidLayout {
                reason: "tiering policy exclude_tags must not contain an empty tag name"
                    .to_string(),
            });
        }
        Ok(())
    }
}

/// What put a promotion job in the queue.
///
/// The distinction is not cosmetic. An automatic promotion is a heuristic and
/// obeys the pause between two migrations of one object; a manual one is an
/// operator who has already decided, and obeys nothing but the guard against
/// overwriting a client's newer write. Decision: EC2-023.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EcPromotionOrigin {
    /// The tiering worker counted enough reads in the window.
    #[default]
    Automatic,
    /// An operator asked for it through the admin API.
    Manual,
}

impl EcPromotionOrigin {
    /// Whether an operator asked for this promotion.
    pub fn is_manual(self) -> bool {
        matches!(self, Self::Manual)
    }
}

/// Lifecycle of one reverse migration.
///
/// The order is the order of the gates in EC2-023: the bytes come back, they
/// are read back, and only then may the shards go. Each step is durable so
/// that a node which dies between two of them resumes rather than restarts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EcPromotionState {
    /// Waiting for a worker to pick it up.
    Queued,
    /// The RF=3 copies are written and the namespace entries are local again.
    HotRestored,
    /// The RF=3 copy has been read back through quorum and matches.
    HotVerified,
    /// The manifest is marked promoted; the shards wait out their grace period.
    ManifestPromoted,
    /// Shards and manifest are gone and the job can be forgotten.
    Completed,
    /// Failed in a way retrying will not fix.
    FailedPermanent,
    /// A client wrote the object while the promotion was in flight.
    Superseded,
}

impl EcPromotionState {
    /// Whether the job still occupies a place in the queue.
    ///
    /// A transient failure does not get a state of its own: the job keeps the
    /// step it reached and carries `attempt`, `failure` and `next_retry_at`
    /// alongside it, so a retry resumes where it stopped instead of decoding
    /// the shards again. The one exception is a copy that did not read back:
    /// only a new restore can put its bytes back, so that job returns to
    /// `Queued` (EC2-026).
    pub fn is_in_flight(self) -> bool {
        match self {
            Self::Queued | Self::HotRestored | Self::HotVerified | Self::ManifestPromoted => true,
            Self::Completed | Self::FailedPermanent | Self::Superseded => false,
        }
    }
}

/// Durable state for one reverse migration from erasure coding back to RF=3.
///
/// Keyed by object, not by operation: an object has at most one promotion worth
/// running, so re-queueing one that is already in flight has to be a no-op
/// rather than a second decode of the same shards. Decision: EC2-023.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EcPromotionJobRecord {
    /// Object version being brought back.
    pub object: EcObjectVersionKey,
    /// Deterministic idempotency ID for this object and EC generation.
    pub operation_id: Uuid,
    /// HLC of the erasure-coded generation being promoted.
    pub source_hlc: HlcTimestamp,
    /// Erasure set the shards belong to.
    pub erasure_set_id: u64,
    /// Current lifecycle state.
    pub state: EcPromotionState,
    /// Attempts already made.
    pub attempt: u32,
    /// Object size in bytes, as the manifest records it.
    pub object_size: u64,
    /// SHA-256 of the complete object, as the manifest records it.
    pub content_hash_sha256: [u8; 32],
    /// What created this job.
    pub origin: EcPromotionOrigin,
    /// Earliest time the shards may be purged, set once the copy is verified.
    pub shards_eligible_at: Option<u64>,
    /// Last update time in nanoseconds since Unix epoch.
    pub updated_at: u64,
    /// Earliest retry time in nanoseconds since Unix epoch.
    pub next_retry_at: Option<u64>,
    /// Why the job failed, when it did.
    pub failure: Option<String>,
}

impl EcPromotionJobRecord {
    /// Validate this promotion job before durable write.
    pub fn validate(&self) -> Result<(), EcMetadataError> {
        self.object.validate()?;
        if self.operation_id.is_nil() {
            return Err(EcMetadataError::InvalidLayout {
                reason: "promotion job operation_id must not be nil".to_string(),
            });
        }
        if self.source_hlc.is_zero() {
            return Err(EcMetadataError::InvalidLayout {
                reason: "promotion job source_hlc must not be zero".to_string(),
            });
        }
        // Versioned objects never enter the tiering pipeline in either
        // direction, and the attach path has no version-aware writer behind it.
        // Refusing here keeps a job that could not be carried out from ever
        // reaching the queue. Decision: EC2-025.
        if self.object.version_id.is_some() {
            return Err(EcMetadataError::InvalidLayout {
                reason: "promotion job must name the current version of an object".to_string(),
            });
        }
        Ok(())
    }
}

/// One erasure set as an operator asked for it.
///
/// The inputs, not the built set. Every node rebuilds the set from its own
/// profile, topology labels and epoch, so "these nodes, as a set" means the same
/// thing everywhere; and the record stays readable the day `ErasureSet` gains a
/// field, which a serialized `ErasureSet` would not.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EcErasureSetSpec {
    /// Identifier issued to the set within its pool.
    pub set_id: u64,
    /// Nodes holding the set's slots, in slot order.
    pub node_ids: Vec<NodeId>,
    /// Lifecycle state the operator left the set in.
    pub state: ErasureSetState,
    /// Creation time in nanoseconds since Unix epoch.
    pub created_at: u64,
}

/// The erasure sets an operator added to a pool.
///
/// The pool's inherited set is deliberately absent: it comes from the node
/// configuration, and a binary that has never seen this record must still build
/// it and read every object EC v1 wrote ([`crate::erasure_coding::ErasureSet::inherited`]).
/// This record only ever adds to that.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EcErasureSetRoster {
    /// Pool these sets belong to.
    pub pool_id: u32,
    /// Added sets, in the order they were added.
    pub sets: Vec<EcErasureSetSpec>,
    /// Lifecycle state of the pool's inherited set.
    ///
    /// The inherited set itself is built from node configuration, so only its
    /// state can be an operator decision. Kept as a field of its own rather than
    /// as a spec with no nodes, because a spec without nodes is not a set.
    pub inherited_state: ErasureSetState,
    /// Nodes the inherited set was built from when the roster was first written.
    ///
    /// A pool may be configured wider than one set, and the inherited set takes
    /// the first slots' worth of nodes in configured order. That order is what
    /// every manifest written so far names, so reordering the pool's nodes would
    /// quietly make those objects unreadable. Recording the list turns that
    /// mistake into a refusal to adopt the roster.
    pub inherited_node_ids: Vec<NodeId>,
    /// HLC of the last change, used to order concurrent edits.
    pub hlc: HlcTimestamp,
    /// Last update time in nanoseconds since Unix epoch.
    pub updated_at: u64,
}

impl EcErasureSetRoster {
    /// An empty roster for a pool that has only its inherited set.
    pub fn empty(pool_id: u32) -> Self {
        Self {
            pool_id,
            sets: Vec::new(),
            inherited_state: ErasureSetState::Active,
            inherited_node_ids: Vec::new(),
            hlc: HlcTimestamp::zero(),
            updated_at: 0,
        }
    }

    /// Validate the roster before durable write.
    pub fn validate(&self) -> Result<(), EcMetadataError> {
        let mut seen = HashSet::with_capacity(self.sets.len());
        for spec in &self.sets {
            if spec.node_ids.is_empty() {
                return Err(EcMetadataError::InvalidErasureSetRoster {
                    reason: format!("erasure set {} has no nodes", spec.set_id),
                });
            }
            let mut nodes = HashSet::with_capacity(spec.node_ids.len());
            for node_id in &spec.node_ids {
                if !nodes.insert(*node_id) {
                    return Err(EcMetadataError::InvalidErasureSetRoster {
                        reason: format!("erasure set {} lists node {node_id} twice", spec.set_id),
                    });
                }
            }
            if !seen.insert(spec.set_id) {
                return Err(EcMetadataError::InvalidErasureSetRoster {
                    reason: format!("erasure set id {} appears twice in the roster", spec.set_id),
                });
            }
        }
        Ok(())
    }

    /// The set with this id, if the roster holds it.
    pub fn get(&self, set_id: u64) -> Option<&EcErasureSetSpec> {
        self.sets.iter().find(|spec| spec.set_id == set_id)
    }
}

/// Layout record format this build writes and reads.
pub const EC_SET_LAYOUT_RECORD_FORMAT_VERSION: u32 = 1;

/// Version of the algorithm that produced an EC v1 layout, as a layout record
/// names it. Diagnostic only: a recorded layout is never recomputed from it.
pub const EC_V1_LAYOUT_ALGORITHM_VERSION: u32 = 1;

/// Version of the LRC group placement of decision EC2-082, as a layout record
/// names it. Diagnostic only, like [`EC_V1_LAYOUT_ALGORITHM_VERSION`]: a set
/// laid out by it keeps its recorded order whatever later versions would
/// choose.
pub const EC_LRC_GROUP_LAYOUT_ALGORITHM_VERSION: u32 = 2;

/// How one erasure set is laid out, fixed when the layout was chosen.
///
/// A set's slot order used to be recomputed from topology labels on every
/// start, so a label change moved slots under objects already written
/// (EC2-086). Once a set has this record its order comes from here verbatim,
/// and labels only decide the layout of sets created later.
///
/// A record of its own, one per set, and not a field of
/// [`EcErasureSetRoster`]: the roster is positional bincode, so a new field
/// there would make every roster written so far unreadable to this build and
/// every roster this build writes unreadable to the previous one (EC2-084,
/// point 3). An old binary never reads this record at all.
///
/// Written through the cluster write path under a key made of the pool and the
/// set id, with a copy in the local cache — the same arrangement as the
/// roster.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EcSetLayoutRecord {
    /// Record format; always [`EC_SET_LAYOUT_RECORD_FORMAT_VERSION`] today.
    ///
    /// First, so a future format can be told apart before anything else is
    /// decoded.
    pub format_version: u32,
    /// Pool the set belongs to.
    pub pool_id: u32,
    /// The set this record lays out.
    pub set_id: u64,
    /// Rule the order below was chosen by.
    pub rule: EcSetLayoutRule,
    /// Topology policy the set was laid out under.
    pub policy: EcTopologyPolicy,
    /// Nodes in slot order: the node of slot `i` at index `i`.
    pub node_ids: Vec<NodeId>,
    /// Why the set does not fully meet its rule, when it was created under
    /// `warn` in breach of it; `None` when it meets it. Always `None` for
    /// [`EcSetLayoutRule::V1`], which lays out whatever it is given.
    pub violation: Option<EcLayoutViolation>,
    /// Version of the layout algorithm that chose the order. For diagnostics.
    pub algorithm_version: u32,
    /// When the layout was recorded, in nanoseconds since the Unix epoch.
    pub created_at: u64,
    /// HLC of the write, which orders two nodes recording the same set at once.
    pub hlc: HlcTimestamp,
}

impl EcSetLayoutRecord {
    /// Record the EC v1 layout `set` already has.
    ///
    /// How a set that predates layout records gets one: the order it is
    /// written with is the order this node computes for it today, so writing
    /// it changes nothing on this node and freezes it for every later start.
    pub fn v1(
        set: &ErasureSet,
        policy: EcTopologyPolicy,
        created_at: u64,
        hlc: HlcTimestamp,
    ) -> Self {
        Self {
            format_version: EC_SET_LAYOUT_RECORD_FORMAT_VERSION,
            pool_id: set.pool_id.as_u32(),
            set_id: set.set_id,
            rule: EcSetLayoutRule::V1,
            policy,
            node_ids: set.slot_assignment.slot_order(),
            violation: None,
            algorithm_version: EC_V1_LAYOUT_ALGORITHM_VERSION,
            created_at,
            hlc,
        }
    }

    /// Record the layout an operator's new set was given.
    ///
    /// Takes the rule, the violation and the algorithm version from `created`
    /// as they are, so the record says exactly how the set was laid out —
    /// including that a set created under `warn` falls short of its rule.
    pub fn new_set(
        created: &EcNewErasureSet,
        policy: EcTopologyPolicy,
        created_at: u64,
        hlc: HlcTimestamp,
    ) -> Self {
        Self {
            format_version: EC_SET_LAYOUT_RECORD_FORMAT_VERSION,
            pool_id: created.set.pool_id.as_u32(),
            set_id: created.set.set_id,
            rule: created.set.layout_rule,
            policy,
            node_ids: created.set.slot_assignment.slot_order(),
            violation: created.violation.clone(),
            algorithm_version: created.algorithm_version,
            created_at,
            hlc,
        }
    }

    /// Validate the record's own structure.
    ///
    /// Whether the nodes are the set's nodes, and whether their number fits
    /// the profile, is checked when the record is applied to a set, because
    /// only the set knows.
    pub fn validate(&self) -> Result<(), EcMetadataError> {
        if self.format_version != EC_SET_LAYOUT_RECORD_FORMAT_VERSION {
            return Err(self.invalid(format!(
                "unsupported format_version {}; this build reads \
                 {EC_SET_LAYOUT_RECORD_FORMAT_VERSION}",
                self.format_version
            )));
        }
        if self.node_ids.is_empty() {
            return Err(self.invalid("names no nodes".to_string()));
        }
        let mut seen = HashSet::with_capacity(self.node_ids.len());
        for node_id in &self.node_ids {
            if !seen.insert(*node_id) {
                return Err(self.invalid(format!("names node {node_id} twice")));
            }
        }
        if self.rule == EcSetLayoutRule::V1 && self.violation.is_some() {
            return Err(self.invalid(
                "records a rule violation for the EC v1 layout, which has no rule to break"
                    .to_string(),
            ));
        }
        Ok(())
    }

    fn invalid(&self, reason: String) -> EcMetadataError {
        EcMetadataError::InvalidSetLayout {
            reason: format!(
                "layout record of erasure set {} in pool {} {reason}",
                self.set_id, self.pool_id
            ),
        }
    }
}

/// Where this node's tiering scan stopped last time.
///
/// The scan is a bounded sweep, not a single pass: one cycle looks at
/// `scan_limit` objects and the next continues from here. Without a durable
/// position a restart would send every cycle back to the first bucket, and the
/// tail of a large catalog would never be examined at all.
///
/// One record per node, under a fixed key: the position is about this node's
/// sweep, not about any object.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EcTieringCursorRecord {
    /// Bucket the next cycle resumes in. Empty means "start from the first".
    pub bucket: String,
    /// Continuation token inside that bucket, if the bucket is unfinished.
    pub continuation: Option<String>,
    /// Last update time in nanoseconds since Unix epoch.
    pub updated_at: u64,
}

/// Where the last tombstone sweep stopped walking this node's manifests.
///
/// The sweep is a walk over every manifest the node holds, and tombstones are a
/// small part of them, so a cycle that started from the beginning every time
/// would re-read the whole keyspace to find the same few. The position is
/// durable for the same reason the tiering one is: a restart must continue the
/// round rather than start it again (decision EC2-202).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EcTombstoneGcCursorRecord {
    /// Continuation token into the manifest keyspace. `None` starts a round.
    pub continuation: Option<Vec<u8>>,
    /// Last update time in nanoseconds since Unix epoch.
    pub updated_at: u64,
    /// Rounds this node has completed since it started keeping the position.
    pub rounds_completed: u64,
}

impl EcTombstoneGcCursorRecord {
    /// The position a node starts a fresh round from.
    pub fn start(updated_at: u64, rounds_completed: u64) -> Self {
        Self {
            continuation: None,
            updated_at,
            rounds_completed,
        }
    }
}

impl EcTieringCursorRecord {
    /// The position a node starts from when it has never scanned.
    pub fn start(updated_at: u64) -> Self {
        Self {
            bucket: String::new(),
            continuation: None,
            updated_at,
        }
    }

    /// Validate this cursor before durable write.
    pub fn validate(&self) -> Result<(), EcMetadataError> {
        // A continuation token without a bucket points nowhere: the next cycle
        // would hand a token from one bucket to whichever bucket comes first.
        if self.bucket.is_empty() && self.continuation.is_some() {
            return Err(EcMetadataError::InvalidLayout {
                reason: "tiering cursor continuation requires a bucket".to_string(),
            });
        }
        Ok(())
    }
}

/// This node's own durable record that it was serving its EC pool.
///
/// One record per node, under a fixed key. It exists because nothing else in
/// the project remembers when a node was last up in wall-clock terms: gossip
/// keeps `Instant`s that do not survive the observer's own restart
/// (decision EC2-141).
///
/// It lives in the EC metadata store rather than beside the identity file on
/// purpose: it describes participation in *this* pool with *this* storage, so
/// it must be destroyed together with the shards it speaks for. A replacement
/// machine has no record, and that absence is the fact the rejoin decision
/// reads.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EcNodePresenceRecord {
    /// The identity that was running.
    pub node_id: NodeId,
    /// The pool it was serving.
    pub pool: String,
    /// When this node first recorded itself in this pool, in nanoseconds.
    pub first_seen_at: u64,
    /// When it last refreshed the record, in nanoseconds.
    pub last_seen_at: u64,
    /// Whether the node still owed the pool shards when the record was written.
    ///
    /// Without this a node held back for a rebuild would clear itself by
    /// restarting: the restart finds a presence record a few seconds old, reads
    /// it as a node that was barely away, and lets it serve. The flag carries
    /// the obligation across restarts until something clears it.
    pub rebuild_pending: bool,
}

impl EcNodePresenceRecord {
    /// The record a node writes the first time it serves a pool.
    pub fn first(
        node_id: NodeId,
        pool: impl Into<String>,
        now: u64,
        rebuild_pending: bool,
    ) -> Self {
        Self {
            node_id,
            pool: pool.into(),
            first_seen_at: now,
            last_seen_at: now,
            rebuild_pending,
        }
    }

    /// The same record moved forward to `now`.
    ///
    /// Never backwards: a clock that stepped back would otherwise make the node
    /// look as if it had been away since the future, and the next start would
    /// read that as a very long absence.
    pub fn refreshed(&self, now: u64, rebuild_pending: bool) -> Self {
        Self {
            last_seen_at: self.last_seen_at.max(now),
            rebuild_pending,
            ..self.clone()
        }
    }

    /// Validate this record before a durable write.
    pub fn validate(&self) -> Result<(), EcMetadataError> {
        if self.pool.trim().is_empty() {
            return Err(EcMetadataError::InvalidLayout {
                reason: "node presence record must name a pool".to_string(),
            });
        }
        if self.last_seen_at < self.first_seen_at {
            return Err(EcMetadataError::InvalidLayout {
                reason: "node presence record was last seen before it was first seen".to_string(),
            });
        }
        Ok(())
    }
}

/// Node-local durable EC metadata store.
pub struct EcMetadataStore {
    db: Database,
    manifests: Keyspace,
    object_layouts: Keyspace,
    local_shards: Keyspace,
    transcode_jobs: Keyspace,
    transcode_leases: Keyspace,
    hot_replica_gc: Keyspace,
    repair_tasks: Keyspace,
    orphan_shards: Keyspace,
    small_object_gc: Keyspace,
    object_heat: Keyspace,
    tiering_policies: Keyspace,
    tiering_cursor: Keyspace,
    promotion_jobs: Keyspace,
    erasure_sets: Keyspace,
    /// Sealed and later packed segments, keyed by segment id.
    ///
    /// An open segment has no record here: it is described entirely by its
    /// journal, so that accepting one small object costs one journal append
    /// rather than a rewrite of the whole index. The record is materialized when
    /// the segment is sealed, and the journal is dropped in the same batch.
    packed_segments: Keyspace,
    /// Append-only journal of open packed segments, keyed by segment and sequence.
    packed_segment_journal: Keyspace,
    /// Objects deleted from a packed segment, keyed by segment and object.
    ///
    /// A set rather than a log, because a deletion is idempotent and carries no
    /// order: any node may record one for any segment, and rebuilding a segment
    /// applies whatever the set holds. That is what lets a delete land on a node
    /// that does not own the open segment.
    packed_segment_dead: Keyspace,
    /// This node's own record of serving the pool, under one fixed key.
    node_presence: Keyspace,
    /// Where the tombstone sweep stopped walking the manifests, under one fixed
    /// key.
    tombstone_gc_cursor: Keyspace,
    /// Small objects whose placement record has not reached a majority of their
    /// replicas, keyed by object.
    ///
    /// Only the shortfalls live here, never the ordinary case, so the sweep
    /// that drains it costs what the outage cost and nothing when there was
    /// none.
    small_placement_hints: Keyspace,
    /// Serializes hint read/compare/write inside blocking tasks, including fsync.
    small_placement_hints_lock: std::sync::Arc<std::sync::Mutex<()>>,
    /// Durable position in the hint queue; a cursor records examination, not delivery.
    small_placement_hint_cursor: Keyspace,
}

impl EcMetadataStore {
    /// Open an EC metadata database at `db_path`.
    pub fn open(db_path: &Path) -> Result<Self, EcMetadataError> {
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|error| EcMetadataError::Database(error.to_string()))?;
        }

        let db = Database::builder(db_path)
            .open()
            .map_err(|error| EcMetadataError::Database(error.to_string()))?;
        let manifests = open_keyspace(&db, "ec_manifests")?;
        let object_layouts = open_keyspace(&db, "ec_object_layouts")?;
        let local_shards = open_keyspace(&db, "ec_shards")?;
        let transcode_jobs = open_keyspace(&db, "ec_transcode_jobs")?;
        let transcode_leases = open_keyspace(&db, "ec_transcode_leases")?;
        let hot_replica_gc = open_keyspace(&db, "ec_hot_replica_gc")?;
        let repair_tasks = open_keyspace(&db, "ec_repair_tasks")?;
        let orphan_shards = open_keyspace(&db, "ec_orphan_shards")?;
        let small_object_gc = open_keyspace(&db, "ec_small_object_gc")?;
        let object_heat = open_keyspace(&db, "ec_object_heat")?;
        let tiering_policies = open_keyspace(&db, "ec_tiering_policies")?;
        let tiering_cursor = open_keyspace(&db, "ec_tiering_cursor")?;
        let promotion_jobs = open_keyspace(&db, "ec_promotion_jobs")?;
        let erasure_sets = open_keyspace(&db, "ec_erasure_sets")?;
        let packed_segments = open_keyspace(&db, "ec_packed_segments")?;
        let packed_segment_journal = open_keyspace(&db, "ec_packed_segment_journal")?;
        let packed_segment_dead = open_keyspace(&db, "ec_packed_segment_dead")?;
        let node_presence = open_keyspace(&db, "ec_node_presence")?;
        let small_placement_hints = open_keyspace(&db, "ec_small_placement_hints")?;
        let small_placement_hint_cursor = open_keyspace(&db, "ec_small_placement_hint_cursor")?;
        let tombstone_gc_cursor = open_keyspace(&db, "ec_tombstone_gc_cursor")?;

        Ok(Self {
            db,
            manifests,
            object_layouts,
            local_shards,
            transcode_jobs,
            transcode_leases,
            hot_replica_gc,
            repair_tasks,
            orphan_shards,
            small_object_gc,
            object_heat,
            tiering_policies,
            tiering_cursor,
            promotion_jobs,
            erasure_sets,
            packed_segments,
            packed_segment_journal,
            packed_segment_dead,
            node_presence,
            small_placement_hints,
            small_placement_hints_lock: std::sync::Arc::new(std::sync::Mutex::new(())),
            small_placement_hint_cursor,
            tombstone_gc_cursor,
        })
    }

    /// Store an object layout.
    pub async fn put_object_layout(
        &self,
        object: &EcObjectVersionKey,
        layout: &ObjectDataLayout,
    ) -> Result<(), EcMetadataError> {
        layout.validate_for_object(object)?;
        let db_key = serialize(object)?;
        let value = serialize(layout)?;
        put_value(self.db.clone(), self.object_layouts.clone(), db_key, value).await
    }

    /// Read an object layout from the local metadata store.
    pub async fn get_object_layout(
        &self,
        object: &EcObjectVersionKey,
    ) -> Result<Option<ObjectDataLayout>, EcMetadataError> {
        object.validate()?;
        get_value(self.object_layouts.clone(), serialize(object)?).await
    }

    /// Delete an object layout from the local metadata store.
    pub async fn delete_object_layout(
        &self,
        object: &EcObjectVersionKey,
    ) -> Result<(), EcMetadataError> {
        object.validate()?;
        delete_value(
            self.db.clone(),
            self.object_layouts.clone(),
            serialize(object)?,
        )
        .await
    }

    /// Record that a small object's placement owes copies to its replicas.
    /// Returns false when a newer generation or retry is already recorded.
    ///
    /// # Errors
    ///
    /// Returns [`EcMetadataError::InvalidLayout`] when the object key or the
    /// placement is not valid.
    pub async fn put_small_placement_hint(
        &self,
        object: &EcObjectVersionKey,
        hint: &EcSmallPlacementHint,
    ) -> Result<bool, EcMetadataError> {
        hint.validate()?;
        let hint = hint.clone();
        self.mutate_small_placement_hint(object, move |current| {
            if current.as_ref().is_some_and(|held| {
                held.placement.hlc > hint.placement.hlc
                    || (held.placement.hlc == hint.placement.hlc
                        && (held.placement != hint.placement || held.attempts >= hint.attempts))
            }) {
                return None;
            }
            Some(Some(hint))
        })
        .await
    }

    async fn mutate_small_placement_hint<F>(
        &self,
        object: &EcObjectVersionKey,
        update: F,
    ) -> Result<bool, EcMetadataError>
    where
        F: FnOnce(Option<EcSmallPlacementHint>) -> Option<Option<EcSmallPlacementHint>>
            + Send
            + 'static,
    {
        object.validate()?;
        let key = serialize(object)?;
        let db = self.db.clone();
        let hints = self.small_placement_hints.clone();
        let lock = std::sync::Arc::clone(&self.small_placement_hints_lock);
        task::spawn_blocking(move || {
            let _guard =
                lock.lock().map_err(|error| EcMetadataError::Database(error.to_string()))?;
            let current = hints
                .get(&key)
                .map_err(|error| EcMetadataError::Database(error.to_string()))?
                .map(|value| deserialize(&value))
                .transpose()?;
            let Some(next) = update(current) else {
                return Ok(false);
            };
            match next {
                Some(hint) => hints.insert(&key, serialize(&hint)?),
                None => hints.remove(&key),
            }
            .map_err(|error| EcMetadataError::Database(error.to_string()))?;
            db.persist(PersistMode::SyncAll)
                .map_err(|error| EcMetadataError::Database(error.to_string()))?;
            Ok(true)
        })
        .await
        .map_err(|error| EcMetadataError::Database(error.to_string()))?
    }

    /// Change only the exact hint snapshot observed before a network operation.
    pub async fn replace_small_placement_hint_if_matches(
        &self,
        object: &EcObjectVersionKey,
        expected: &EcSmallPlacementHint,
        replacement: Option<&EcSmallPlacementHint>,
    ) -> Result<bool, EcMetadataError> {
        if let Some(next) = replacement {
            next.validate()?;
            if next.placement != expected.placement || next.attempts < expected.attempts {
                return Err(EcMetadataError::InvalidLayout {
                    reason: "a hint retry cannot change placement or decrease attempts".into(),
                });
            }
        }
        let expected = expected.clone();
        let replacement = replacement.cloned();
        self.mutate_small_placement_hint(object, move |current| {
            (current.as_ref() == Some(&expected)).then_some(replacement)
        })
        .await
    }

    /// Clear debt only for the placement generation whose write was acknowledged.
    pub async fn clear_small_placement_hint_for_placement(
        &self,
        object: &EcObjectVersionKey,
        placement: &ReplicatedSmallObjectPlacement,
    ) -> Result<bool, EcMetadataError> {
        let placement = placement.clone();
        self.mutate_small_placement_hint(object, move |current| {
            current.is_some_and(|hint| hint.placement == placement).then_some(None)
        })
        .await
    }

    /// A committed delete cannot remove a newer generation's placement debt.
    pub async fn delete_small_placement_hint_through(
        &self,
        object: &EcObjectVersionKey,
        hlc: HlcTimestamp,
    ) -> Result<bool, EcMetadataError> {
        self.mutate_small_placement_hint(object, move |current| {
            current.is_some_and(|hint| hint.placement.hlc <= hlc).then_some(None)
        })
        .await
    }

    /// Read one small-object placement hint.
    ///
    /// # Errors
    ///
    /// Returns [`EcMetadataError::InvalidLayout`] when the object key is not
    /// valid, and [`EcMetadataError::Database`] when the read fails.
    pub async fn get_small_placement_hint(
        &self,
        object: &EcObjectVersionKey,
    ) -> Result<Option<EcSmallPlacementHint>, EcMetadataError> {
        object.validate()?;
        get_value(self.small_placement_hints.clone(), serialize(object)?).await
    }

    /// Forget a small-object placement hint.
    ///
    /// # Errors
    ///
    /// Returns [`EcMetadataError::InvalidLayout`] when the object key is not
    /// valid, and [`EcMetadataError::Database`] when the delete fails.
    pub async fn delete_small_placement_hint(
        &self,
        object: &EcObjectVersionKey,
    ) -> Result<(), EcMetadataError> {
        self.mutate_small_placement_hint(object, |current| current.map(|_| None))
            .await
            .map(|_| ())
    }

    /// List small-object placement hints in storage order up to `max_entries`.
    ///
    /// # Errors
    ///
    /// Returns [`EcMetadataError::Database`] when the scan fails.
    pub async fn list_small_placement_hints(
        &self,
        max_entries: usize,
    ) -> Result<Vec<(EcObjectVersionKey, EcSmallPlacementHint)>, EcMetadataError> {
        self.list_small_placement_hints_page(None, max_entries)
            .await
            .map(|page| page.entries)
    }

    /// List one page without hiding its continuation behind a first-page wrapper.
    pub async fn list_small_placement_hints_page(
        &self,
        cursor: Option<Vec<u8>>,
        max_entries: usize,
    ) -> Result<EcMetadataPage<(EcObjectVersionKey, EcSmallPlacementHint)>, EcMetadataError> {
        list_keyspace_page(
            self.small_placement_hints.clone(),
            cursor,
            max_entries,
            |key_bytes, value_bytes| {
                let object: EcObjectVersionKey = deserialize(&key_bytes)?;
                let hint: EcSmallPlacementHint = deserialize(&value_bytes)?;
                hint.validate()?;
                Ok((object, hint))
            },
        )
        .await
    }

    /// Resume the hint queue after restart; None starts a new round.
    pub async fn get_small_placement_hint_cursor(
        &self,
    ) -> Result<Option<Vec<u8>>, EcMetadataError> {
        get_value::<Option<Vec<u8>>>(self.small_placement_hint_cursor.clone(), b"cursor".to_vec())
            .await
            .map(Option::flatten)
    }

    /// Checkpoint only after processing a page; a crash before this replays it.
    pub async fn put_small_placement_hint_cursor(
        &self,
        cursor: Option<Vec<u8>>,
    ) -> Result<(), EcMetadataError> {
        put_value(
            self.db.clone(),
            self.small_placement_hint_cursor.clone(),
            b"cursor".to_vec(),
            serialize(&cursor)?,
        )
        .await
    }

    /// Remember where the tombstone sweep stopped.
    ///
    /// # Errors
    ///
    /// Returns [`EcMetadataError::Database`] when the write fails.
    pub async fn put_tombstone_gc_cursor(
        &self,
        record: &EcTombstoneGcCursorRecord,
    ) -> Result<(), EcMetadataError> {
        put_value(
            self.db.clone(),
            self.tombstone_gc_cursor.clone(),
            TOMBSTONE_GC_CURSOR_KEY.to_vec(),
            serialize(record)?,
        )
        .await
    }

    /// Where the tombstone sweep stopped, if it has ever run here.
    ///
    /// # Errors
    ///
    /// Returns [`EcMetadataError::Database`] when the read fails.
    pub async fn get_tombstone_gc_cursor(
        &self,
    ) -> Result<Option<EcTombstoneGcCursorRecord>, EcMetadataError> {
        get_value(
            self.tombstone_gc_cursor.clone(),
            TOMBSTONE_GC_CURSOR_KEY.to_vec(),
        )
        .await
    }

    /// Store a packed segment record.
    ///
    /// # Errors
    ///
    /// Returns [`EcMetadataError::InvalidLayout`] when the segment violates its
    /// own invariants, and [`EcMetadataError::Database`] on a storage failure.
    pub async fn put_packed_segment(&self, segment: &PackedSegment) -> Result<(), EcMetadataError> {
        segment.validate().map_err(packed_segment_error)?;
        let value = serialize(segment)?;
        put_value(
            self.db.clone(),
            self.packed_segments.clone(),
            packed_segment_key(segment.segment_id),
            value,
        )
        .await
    }

    /// Read one packed segment record.
    ///
    /// Answers `None` for a segment that is still open: an open segment lives in
    /// the journal until it is sealed.
    ///
    /// # Errors
    ///
    /// Returns [`EcMetadataError::Database`] on a storage failure and
    /// [`EcMetadataError::Serialization`] on an unreadable record.
    pub async fn get_packed_segment(
        &self,
        segment_id: PackedSegmentId,
    ) -> Result<Option<PackedSegment>, EcMetadataError> {
        get_value(self.packed_segments.clone(), packed_segment_key(segment_id)).await
    }

    /// List one page of packed segment records in storage order.
    ///
    /// # Errors
    ///
    /// Returns [`EcMetadataError::Database`] on a storage failure.
    pub async fn list_packed_segments_page(
        &self,
        after_cursor: Option<Vec<u8>>,
        max_entries: usize,
    ) -> Result<EcMetadataPage<PackedSegment>, EcMetadataError> {
        list_keyspace_page(
            self.packed_segments.clone(),
            after_cursor,
            max_entries,
            |_, value_bytes| deserialize(&value_bytes),
        )
        .await
    }

    /// List packed segment records in storage order up to `max_entries`.
    ///
    /// # Errors
    ///
    /// Returns [`EcMetadataError::Database`] on a storage failure.
    pub async fn list_packed_segments(
        &self,
        max_entries: usize,
    ) -> Result<Vec<PackedSegment>, EcMetadataError> {
        self.list_packed_segments_page(None, max_entries).await.map(|page| page.entries)
    }

    /// Delete a packed segment record together with any journal left for it.
    ///
    /// # Errors
    ///
    /// Returns [`EcMetadataError::Database`] on a storage failure.
    pub async fn delete_packed_segment(
        &self,
        segment_id: PackedSegmentId,
    ) -> Result<(), EcMetadataError> {
        let mut writes = vec![BatchWrite::Delete {
            keyspace: self.packed_segments.clone(),
            key: packed_segment_key(segment_id),
        }];
        writes.extend(self.prefixed_deletes(&self.packed_segment_journal, segment_id).await?);
        writes.extend(self.prefixed_deletes(&self.packed_segment_dead, segment_id).await?);
        commit_batch(self.db.clone(), writes).await
    }

    /// Append one packed segment journal record.
    ///
    /// # Errors
    ///
    /// Returns [`EcMetadataError::Database`] on a storage failure.
    pub async fn append_packed_segment_journal(
        &self,
        item: &PackedSegmentJournalItem,
    ) -> Result<(), EcMetadataError> {
        let value = serialize(&item.payload)?;
        put_value(
            self.db.clone(),
            self.packed_segment_journal.clone(),
            packed_segment_journal_key(item.segment_id, item.sequence),
            value,
        )
        .await
    }

    /// Commit an object into an open packed segment.
    ///
    /// The journal record and the object layout land in one atomic batch, so
    /// there is never an object without an index entry, nor an index entry
    /// without an object. Returning `Ok` is what makes the object
    /// acknowledgeable to the client (`EC2-062`).
    ///
    /// # Errors
    ///
    /// Returns [`EcMetadataError::InvalidLayout`] when the placement or the
    /// object key is unusable, and [`EcMetadataError::Database`] on a storage
    /// failure.
    pub async fn commit_packed_object(
        &self,
        object: &EcObjectVersionKey,
        placement: &PackedSegmentObjectPlacement,
        item: &PackedSegmentJournalItem,
    ) -> Result<(), EcMetadataError> {
        let layout = ObjectDataLayout::PackedSegment(placement.clone());
        layout.validate_for_object(object)?;
        if item.segment_id != placement.segment_id {
            return Err(EcMetadataError::InvalidLayout {
                reason: format!(
                    "journal record names segment {} but the layout names {}",
                    item.segment_id, placement.segment_id
                ),
            });
        }

        commit_batch(
            self.db.clone(),
            vec![
                BatchWrite::Put {
                    keyspace: self.packed_segment_journal.clone(),
                    key: packed_segment_journal_key(item.segment_id, item.sequence),
                    value: serialize(&item.payload)?,
                },
                BatchWrite::Put {
                    keyspace: self.object_layouts.clone(),
                    key: serialize(object)?,
                    value: serialize(&layout)?,
                },
            ],
        )
        .await
    }

    /// Materialize a sealed segment and retire the journal that described it.
    ///
    /// From here on the record is authoritative, which is why the journal goes
    /// in the same batch: a segment is described by exactly one of the two, and
    /// never by both.
    ///
    /// # Errors
    ///
    /// Returns [`EcMetadataError::InvalidLayout`] when the segment is not
    /// sealed or violates its invariants, and [`EcMetadataError::Database`] on a
    /// storage failure.
    pub async fn seal_packed_segment(
        &self,
        segment: &PackedSegment,
    ) -> Result<(), EcMetadataError> {
        segment.validate().map_err(packed_segment_error)?;
        if segment.state == PackedSegmentState::Open {
            return Err(EcMetadataError::InvalidLayout {
                reason: format!(
                    "segment {} is still open and has no record to materialize",
                    segment.segment_id
                ),
            });
        }

        // A seal never moves a segment backwards. The in-memory pool that asks
        // for one holds its own copy of the segment, and that copy stops being
        // the truth the moment the segment is encoded or released — so a seal
        // arriving late would otherwise overwrite a `Purged` record with a
        // `Sealed` one, hand the segment back to the encoder, and lose the fact
        // that its hot copies are already gone.
        if let Some(stored) = self.get_packed_segment(segment.segment_id).await? {
            if stored.state != PackedSegmentState::Open && stored.state != segment.state {
                tracing::debug!(
                    segment_id = %segment.segment_id,
                    stored = %stored.state,
                    offered = %segment.state,
                    "ignoring a seal for a segment whose record has moved on"
                );
                return Ok(());
            }
        }

        let mut writes = vec![BatchWrite::Put {
            keyspace: self.packed_segments.clone(),
            key: packed_segment_key(segment.segment_id),
            value: serialize(segment)?,
        }];
        writes
            .extend(self.prefixed_deletes(&self.packed_segment_journal, segment.segment_id).await?);
        commit_batch(self.db.clone(), writes).await
    }

    /// Entries of a segment whose objects still point at it and still have hot copies.
    ///
    /// This is the same predicate [`Self::switch_packed_segment_to_ec`] uses,
    /// exposed so cutover can commit a pointer manifest for exactly the objects
    /// it is about to switch and for no others. An object deleted while the
    /// segment was open is not in this list: committing a manifest for it would
    /// bring it back from the dead.
    ///
    /// # Errors
    ///
    /// Propagates a metadata read failure.
    pub async fn live_packed_segment_entries(
        &self,
        segment: &PackedSegment,
    ) -> Result<Vec<PackedSegmentEntry>, EcMetadataError> {
        let mut live = Vec::with_capacity(segment.entries.len());
        for entry in &segment.entries {
            if self.live_packed_placement(segment, &entry.object).await?.is_some() {
                live.push(entry.clone());
            }
        }
        Ok(live)
    }

    /// The object's placement when it still names this segment and has hot copies.
    async fn live_packed_placement(
        &self,
        segment: &PackedSegment,
        object: &EcObjectVersionKey,
    ) -> Result<Option<PackedSegmentObjectPlacement>, EcMetadataError> {
        let Some(ObjectDataLayout::PackedSegment(placement)) =
            self.get_object_layout(object).await?
        else {
            return Ok(None);
        };
        if placement.segment_id != segment.segment_id || placement.hot_replica_nodes.is_empty() {
            return Ok(None);
        }
        Ok(Some(placement))
    }

    /// Whether the object's layout still points at this segment.
    ///
    /// Unlike [`Self::live_packed_segment_entries`] this says nothing about hot
    /// copies, because a repacked segment has none left: its objects read from
    /// its shards, and the only question the cutover asks is whether they still
    /// read from *this* segment.
    ///
    /// The answer is `false` for an object that was deleted or overwritten
    /// while the repack ran, which is what stops the cutover from writing
    /// anything on its behalf.
    ///
    /// # Errors
    ///
    /// Propagates a metadata read failure.
    pub async fn packed_layout_names_segment(
        &self,
        object: &EcObjectVersionKey,
        segment_id: PackedSegmentId,
    ) -> Result<bool, EcMetadataError> {
        match self.get_object_layout(object).await? {
            Some(ObjectDataLayout::PackedSegment(placement)) => {
                Ok(placement.segment_id == segment_id)
            }
            _ => Ok(false),
        }
    }

    /// Move a segment's whole contents off their hot copies, in one operation.
    ///
    /// Every object of the segment stops naming RF=3 replicas and starts being
    /// served from the segment's shards, and the segment moves to `Purged`. The
    /// batch is the point of the whole method: a segment half of whose objects
    /// read from replicas that the other half's purge is about to delete is a
    /// state that must not be observable, so it is never written.
    ///
    /// Idempotent. An object whose layout is already switched, or which no
    /// longer points at this segment because it was overwritten or deleted
    /// meanwhile, is skipped rather than resurrected.
    ///
    /// Returns the objects whose layout actually changed — the ones whose hot
    /// copies may now be purged.
    ///
    /// # Errors
    ///
    /// Returns [`EcMetadataError::InvalidLayout`] when the segment has not been
    /// encoded yet, and [`EcMetadataError::Database`] on a storage failure.
    pub async fn switch_packed_segment_to_ec(
        &self,
        segment: &PackedSegment,
    ) -> Result<Vec<EcObjectVersionKey>, EcMetadataError> {
        segment.validate().map_err(packed_segment_error)?;
        if segment.state != PackedSegmentState::Encoded {
            return Err(EcMetadataError::InvalidLayout {
                reason: format!(
                    "segment {} is {} and cannot release its hot copies; only an encoded \
                     segment can",
                    segment.segment_id, segment.state
                ),
            });
        }

        let mut writes = Vec::with_capacity(segment.entries.len() + 1);
        let mut switched = Vec::new();
        for entry in &segment.entries {
            let Some(mut placement) = self.live_packed_placement(segment, &entry.object).await?
            else {
                continue;
            };
            placement.hot_replica_nodes.clear();
            let layout = ObjectDataLayout::PackedSegment(placement);
            layout.validate_for_object(&entry.object)?;
            writes.push(BatchWrite::Put {
                keyspace: self.object_layouts.clone(),
                key: serialize(&entry.object)?,
                value: serialize(&layout)?,
            });
            switched.push(entry.object.clone());
        }

        let mut purged = segment.clone();
        purged.transition_to(PackedSegmentState::Purged).map_err(packed_segment_error)?;
        writes.push(BatchWrite::Put {
            keyspace: self.packed_segments.clone(),
            key: packed_segment_key(segment.segment_id),
            value: serialize(&purged)?,
        });

        commit_batch(self.db.clone(), writes).await?;
        Ok(switched)
    }

    /// Object versions whose layout still names this segment.
    ///
    /// The check that stands in front of deleting a segment's shards. A segment
    /// index says which objects the segment was built from; this says which
    /// objects would stop being readable if it went. After a repack the two
    /// differ on purpose — the index is kept so a straggling read can still
    /// resolve an ETag from it, while the layouts have all moved on.
    ///
    /// # Errors
    ///
    /// Propagates a metadata read failure.
    pub async fn packed_segment_referrers(
        &self,
        segment: &PackedSegment,
        max_entries: usize,
    ) -> Result<Vec<EcObjectVersionKey>, EcMetadataError> {
        let mut referrers = Vec::new();
        for entry in segment.entries.iter().take(max_entries) {
            if self.packed_layout_names_segment(&entry.object, segment.segment_id).await? {
                referrers.push(entry.object.clone());
            }
        }
        Ok(referrers)
    }

    /// Switch a repacked segment's objects onto their replacement, in one batch.
    ///
    /// The cutover of phase 5, and the batch is the whole of it. Every moved
    /// object's layout, the replacement's own record and the retirement of the
    /// source all land together or not at all, so no observer ever sees one
    /// version of an object in the new segment while another is still in the
    /// old one — which `ListObjectVersions` would show as a torn list
    /// (`EC2-065`). A repack fills exactly one replacement for this reason.
    ///
    /// The moved objects keep no hot copies: the replacement is already encoded
    /// by the time this runs, and the source's shards answered every read until
    /// this instant. Nothing is ever without a readable placement.
    ///
    /// The source segment is only retired, never deleted here. Its record,
    /// index and shards stay put: a read that resolved its layout just before
    /// this batch is still reading them, and the drain window of `EC2-062` is
    /// what says when they may go.
    ///
    /// An object that stopped pointing at the source while the repack ran is
    /// skipped rather than switched — an overwrite or a delete got there first,
    /// and the newer writer's layout wins. Its room in the replacement is
    /// charged to dead bytes, because the replacement was encoded holding it
    /// and cannot give the room back.
    ///
    /// # Errors
    ///
    /// Returns [`EcMetadataError::InvalidLayout`] when the source has not been
    /// purged, the replacement has not been encoded or does not name the source,
    /// or a move does not match the entry it claims, and
    /// [`EcMetadataError::Database`] on a storage failure.
    pub async fn commit_packed_repack(
        &self,
        source: &PackedSegment,
        destination: &PackedSegment,
        moves: &[PackedRepackMove],
        now: u64,
    ) -> Result<PackedRepackCutover, EcMetadataError> {
        source.validate().map_err(packed_segment_error)?;
        destination.validate().map_err(packed_segment_error)?;
        if source.state != PackedSegmentState::Purged {
            return Err(EcMetadataError::InvalidLayout {
                reason: format!(
                    "segment {} is {} and cannot be repacked; only a purged segment, whose \
                     objects read from its shards alone, has anything to reclaim",
                    source.segment_id, source.state
                ),
            });
        }
        if destination.state != PackedSegmentState::Encoded {
            return Err(EcMetadataError::InvalidLayout {
                reason: format!(
                    "replacement segment {} is {}; its objects may not be switched onto it \
                     before its shards exist",
                    destination.segment_id, destination.state
                ),
            });
        }
        if destination.repacked_from != Some(source.segment_id) {
            return Err(EcMetadataError::InvalidLayout {
                reason: format!(
                    "segment {} is not the replacement of segment {}",
                    destination.segment_id, source.segment_id
                ),
            });
        }
        if destination.erasure_set_id != source.erasure_set_id {
            return Err(EcMetadataError::InvalidLayout {
                reason: format!(
                    "replacement segment {} belongs to erasure set {}, but segment {} belongs \
                     to {}",
                    destination.segment_id,
                    destination.erasure_set_id,
                    source.segment_id,
                    source.erasure_set_id
                ),
            });
        }

        let mut replacement = destination.clone();
        let mut cutover = PackedRepackCutover {
            reclaimed_bytes: source.used_bytes,
            ..PackedRepackCutover::default()
        };
        let mut writes = Vec::with_capacity(moves.len() + 2);

        for moved in moves {
            if moved.placement.segment_id != destination.segment_id {
                return Err(EcMetadataError::InvalidLayout {
                    reason: format!(
                        "{}/{} is moved into segment {}, but the replacement is {}",
                        moved.object.bucket,
                        moved.object.key,
                        moved.placement.segment_id,
                        destination.segment_id
                    ),
                });
            }
            ensure_move_matches_entry(destination, moved)?;

            if !self.packed_layout_names_segment(&moved.object, source.segment_id).await? {
                // Charged, not switched: the replacement was encoded holding
                // this object's bytes, and an encoded container cannot give
                // room back.
                replacement.record_object_deleted(&moved.object).map_err(packed_segment_error)?;
                cutover.abandoned.push(moved.object.clone());
                continue;
            }

            let layout = ObjectDataLayout::PackedSegment(moved.placement.clone());
            layout.validate_for_object(&moved.object)?;
            writes.push(BatchWrite::Put {
                keyspace: self.object_layouts.clone(),
                key: serialize(&moved.object)?,
                value: serialize(&layout)?,
            });
            cutover.switched.push(moved.object.clone());
        }

        if replacement.entries.is_empty() {
            // Nothing survived the race into it. The replacement holds shards
            // describing objects that all moved on, so it is retired here
            // rather than left for the repacker to rediscover.
            replacement.retire(now).map_err(packed_segment_error)?;
            cutover.retired_destinations.push(replacement.segment_id);
        } else {
            replacement
                .transition_to(PackedSegmentState::Purged)
                .map_err(packed_segment_error)?;
        }
        replacement.validate().map_err(packed_segment_error)?;
        writes.push(BatchWrite::Put {
            keyspace: self.packed_segments.clone(),
            key: packed_segment_key(replacement.segment_id),
            value: serialize(&replacement)?,
        });

        let mut retired = source.clone();
        retired.retire(now).map_err(packed_segment_error)?;
        writes.push(BatchWrite::Put {
            keyspace: self.packed_segments.clone(),
            key: packed_segment_key(retired.segment_id),
            value: serialize(&retired)?,
        });

        commit_batch(self.db.clone(), writes).await?;
        Ok(cutover)
    }

    /// Return a segment's objects to RF=3 and retire the segment, in one batch.
    ///
    /// The reverse of packing, and the operator's way back out of the feature.
    /// Every object stops being described by a place inside a container and
    /// starts being described by its own three copies, which is exactly what a
    /// small object looks like when packing was never switched on.
    ///
    /// The batch covers every object and the segment together for the same
    /// reason the repack cutover does: a segment half of whose objects had been
    /// released while the other half still pointed into it would be a state no
    /// reader could interpret.
    ///
    /// The bytes must already be on their replicas before this is called. This
    /// writes metadata only, and metadata that promises three copies of
    /// something that has none is the one mistake an unpack must not make.
    ///
    /// # Errors
    ///
    /// Returns [`EcMetadataError::InvalidLayout`] when the segment is open or
    /// already dead, or a move does not match the segment's index, and
    /// [`EcMetadataError::Database`] on a storage failure.
    pub async fn commit_packed_unpack(
        &self,
        segment: &PackedSegment,
        moves: &[PackedUnpackMove],
        now: u64,
    ) -> Result<PackedUnpackResult, EcMetadataError> {
        segment.validate().map_err(packed_segment_error)?;
        if segment.state == PackedSegmentState::Open {
            return Err(EcMetadataError::InvalidLayout {
                reason: format!(
                    "segment {} is still open and is the write cursor of a live pool; seal it \
                     before unpacking it",
                    segment.segment_id
                ),
            });
        }
        if segment.state == PackedSegmentState::Dead {
            return Err(EcMetadataError::InvalidLayout {
                reason: format!("segment {} is already retired", segment.segment_id),
            });
        }

        let mut result = PackedUnpackResult::default();
        let mut writes = Vec::with_capacity(moves.len() + 1);

        for moved in moves {
            let entry =
                segment.entry_for(&moved.object).ok_or_else(|| EcMetadataError::InvalidLayout {
                    reason: format!(
                        "segment {} does not list {}/{}",
                        segment.segment_id, moved.object.bucket, moved.object.key
                    ),
                })?;
            if moved.placement.size != entry.length
                || moved.placement.content_hash_sha256 != entry.content_hash_sha256
            {
                return Err(EcMetadataError::InvalidLayout {
                    reason: format!(
                        "the unpacked placement of {}/{} describes {} byte(s), but segment {} \
                         lists {}",
                        moved.object.bucket,
                        moved.object.key,
                        moved.placement.size,
                        segment.segment_id,
                        entry.length
                    ),
                });
            }

            if !self.packed_layout_names_segment(&moved.object, segment.segment_id).await? {
                result.skipped.push(moved.object.clone());
                continue;
            }

            let layout = ObjectDataLayout::ReplicatedSmall(moved.placement.clone());
            layout.validate_for_object(&moved.object)?;
            writes.push(BatchWrite::Put {
                keyspace: self.object_layouts.clone(),
                key: serialize(&moved.object)?,
                value: serialize(&layout)?,
            });
            result.unpacked.push(moved.object.clone());
        }

        let mut retired = segment.clone();
        retired.retire(now).map_err(packed_segment_error)?;
        writes.push(BatchWrite::Put {
            keyspace: self.packed_segments.clone(),
            key: packed_segment_key(retired.segment_id),
            value: serialize(&retired)?,
        });

        commit_batch(self.db.clone(), writes).await?;
        Ok(result)
    }

    /// Retire a staged replacement that was never referenced by anything.
    ///
    /// Unlike every other retirement this one throws away a segment with live
    /// entries, and it is allowed to because nothing points at those entries:
    /// a staged replacement holds a description of where objects *would* go,
    /// and until its cutover runs it is the only thing that believes it.
    ///
    /// # Errors
    ///
    /// Returns [`EcMetadataError::InvalidLayout`] when the segment is not a
    /// staged replacement, and [`EcMetadataError::Database`] on a storage
    /// failure.
    pub async fn retire_drifted_replacement(
        &self,
        segment: &PackedSegment,
        now: u64,
    ) -> Result<(), EcMetadataError> {
        if segment.repacked_from.is_none() || segment.state != PackedSegmentState::Sealed {
            return Err(EcMetadataError::InvalidLayout {
                reason: format!(
                    "segment {} is {} and is not a staged replacement; only one of those may be \
                     discarded with its entries intact",
                    segment.segment_id, segment.state
                ),
            });
        }
        let mut retired = segment.clone();
        retired.retire(now).map_err(packed_segment_error)?;
        self.put_packed_segment(&retired).await
    }

    /// Retire a segment nothing points at any more.
    ///
    /// Used for a segment emptied by deletions rather than by a repack: there
    /// is nothing to move, so the cutover of
    /// [`Self::commit_packed_repack`] has nothing to do and only the retirement
    /// is left.
    ///
    /// # Errors
    ///
    /// Returns [`EcMetadataError::InvalidLayout`] when the segment still holds
    /// live entries or is already dead, and [`EcMetadataError::Database`] on a
    /// storage failure.
    pub async fn retire_empty_packed_segment(
        &self,
        segment: &PackedSegment,
        now: u64,
    ) -> Result<(), EcMetadataError> {
        if !segment.entries.is_empty() {
            return Err(EcMetadataError::InvalidLayout {
                reason: format!(
                    "segment {} still holds {} live object(s) and cannot be retired without \
                     moving them",
                    segment.segment_id,
                    segment.entries.len()
                ),
            });
        }
        let mut retired = segment.clone();
        retired.retire(now).map_err(packed_segment_error)?;
        self.put_packed_segment(&retired).await
    }

    /// Charge a deleted object to the segment that holds it.
    ///
    /// Records the deletion and drops the object's layout in one atomic batch,
    /// which is what keeps "the object is gone" and "its bytes are dead" from
    /// ever disagreeing. Works the same whether the segment is open or sealed
    /// and whether or not this node owns it: the deletion is an idempotent set
    /// entry, applied when the segment is next rebuilt.
    ///
    /// Answers `None` when the object is not packed at all.
    ///
    /// # Errors
    ///
    /// Returns [`EcMetadataError::Database`] on a storage failure.
    pub async fn record_packed_object_deleted(
        &self,
        object: &EcObjectVersionKey,
    ) -> Result<Option<PackedSegmentDeletion>, EcMetadataError> {
        let Some(ObjectDataLayout::PackedSegment(placement)) =
            self.get_object_layout(object).await?
        else {
            return Ok(None);
        };
        self.charge_packed_object_deleted(object, &placement).await.map(Some)
    }

    /// Charge a deleted object to its segment, from a placement already in hand.
    ///
    /// The delete path reads the object's placement, commits a tombstone and
    /// only then charges the bytes — and committing the tombstone is what drops
    /// the object's layout. Re-reading the layout at that point finds nothing,
    /// so the charge has to be made from the placement the caller already
    /// resolved. Getting this wrong is invisible until a segment that should be
    /// half dead reports itself pristine and is never repacked.
    ///
    /// # Errors
    ///
    /// Returns [`EcMetadataError::Database`] on a storage failure.
    pub async fn charge_packed_object_deleted(
        &self,
        object: &EcObjectVersionKey,
        placement: &PackedSegmentObjectPlacement,
    ) -> Result<PackedSegmentDeletion, EcMetadataError> {
        commit_batch(
            self.db.clone(),
            vec![
                BatchWrite::Put {
                    keyspace: self.packed_segment_dead.clone(),
                    key: packed_segment_dead_key(placement.segment_id, object)?,
                    value: serialize(object)?,
                },
                // Idempotent: the tombstone commit may already have dropped it.
                BatchWrite::Delete {
                    keyspace: self.object_layouts.clone(),
                    key: serialize(object)?,
                },
            ],
        )
        .await?;

        Ok(PackedSegmentDeletion {
            segment_id: placement.segment_id,
            freed_bytes: placement.length,
        })
    }

    /// Read the objects recorded as deleted from packed segments.
    ///
    /// # Errors
    ///
    /// Returns [`EcMetadataError::Database`] on a storage failure.
    async fn list_packed_segment_dead(
        &self,
        max_entries: usize,
    ) -> Result<Vec<(PackedSegmentId, EcObjectVersionKey)>, EcMetadataError> {
        list_keyspace_page(
            self.packed_segment_dead.clone(),
            None,
            max_entries,
            |key_bytes, value_bytes| {
                let segment_id = decode_packed_segment_dead_key(&key_bytes)?;
                Ok((segment_id, deserialize(&value_bytes)?))
            },
        )
        .await
        .map(|page| page.entries)
    }

    /// Rebuild every packed segment this node knows about.
    ///
    /// Sealed segments come from their records. Open segments come from
    /// replaying their journals, which is what discards a tail that was still
    /// being written when the node died (`EC2-062`). Deletions are applied on
    /// top of both, from the dead set.
    ///
    /// # Errors
    ///
    /// Returns [`EcMetadataError::Database`] on a storage failure.
    pub async fn load_packed_segments(
        &self,
        max_entries: usize,
    ) -> Result<PackedSegmentRecovery, EcMetadataError> {
        let recorded = self.list_packed_segments(max_entries).await?;
        let journal = self.list_packed_segment_journal(max_entries).await?;
        let dead = self.list_packed_segment_dead(max_entries).await?;
        tracing::debug!(
            recorded = recorded.len(),
            journal = journal.len(),
            dead = dead.len(),
            "rebuilding packed segments"
        );

        let mut recovery = recover_segments(journal);
        let materialized: HashSet<PackedSegmentId> =
            recorded.iter().map(|segment| segment.segment_id).collect();
        // A segment with a record is described by that record alone. Any journal
        // still lying around for it is the leftover of a crash between the seal
        // batch and its next compaction, and replaying it would rebuild a state
        // the seal already superseded.
        recovery.segments.retain(|segment| !materialized.contains(&segment.segment_id));
        recovery.discarded.retain(|tail| !materialized.contains(&tail.segment_id));
        recovery
            .rejected
            .retain(|rejected| !materialized.contains(&rejected.segment_id));
        recovery.segments.extend(recorded);

        for (segment_id, object) in dead {
            let Some(segment) =
                recovery.segments.iter_mut().find(|segment| segment.segment_id == segment_id)
            else {
                tracing::debug!(
                    %segment_id,
                    bucket = %object.bucket,
                    key = %object.key,
                    "a deletion names a segment this node has no record of"
                );
                continue;
            };
            // A deletion whose object is not in the segment is not a failure:
            // its append may have been in a tail this replay discarded, or the
            // deletion may already have been folded into a sealed record. The
            // set entry is idempotent, so applying it twice must be harmless.
            if let Err(error) = segment.record_object_deleted(&object) {
                tracing::debug!(
                    %segment_id,
                    bucket = %object.bucket,
                    key = %object.key,
                    %error,
                    "a deletion could not be applied to its segment"
                );
            }
        }

        recovery.segments.sort_by_key(|segment| segment.segment_id);
        Ok(recovery)
    }

    /// Read the packed segment journal, reporting records it cannot decode.
    ///
    /// A record that fails to deserialize comes back as
    /// [`PackedSegmentJournalPayload::Unreadable`] rather than being skipped:
    /// the key survives even when the value does not, so recovery can tell a
    /// torn tail from a missing record.
    async fn list_packed_segment_journal(
        &self,
        max_entries: usize,
    ) -> Result<Vec<PackedSegmentJournalItem>, EcMetadataError> {
        list_keyspace_page(
            self.packed_segment_journal.clone(),
            None,
            max_entries,
            |key_bytes, value_bytes| {
                let (segment_id, sequence) = decode_packed_segment_journal_key(&key_bytes)?;
                let payload = deserialize(&value_bytes).unwrap_or_else(|error| {
                    PackedSegmentJournalPayload::Unreadable {
                        reason: error.to_string(),
                    }
                });
                Ok(PackedSegmentJournalItem {
                    segment_id,
                    sequence,
                    payload,
                })
            },
        )
        .await
        .map(|page| page.entries)
    }

    /// Build the deletes that remove every row a segment owns in one keyspace.
    ///
    /// Both packed-segment side keyspaces put the segment id first, so a prefix
    /// scan finds exactly that segment's rows and nothing else.
    async fn prefixed_deletes(
        &self,
        keyspace: &Keyspace,
        segment_id: PackedSegmentId,
    ) -> Result<Vec<BatchWrite>, EcMetadataError> {
        let scanned = keyspace.clone();
        let prefix = segment_id.as_uuid().into_bytes();
        let keys = task::spawn_blocking(move || {
            let mut keys = Vec::new();
            for guard in scanned.prefix(prefix) {
                let (key_bytes, _) = guard
                    .into_inner()
                    .map_err(|error| EcMetadataError::Database(error.to_string()))?;
                keys.push(key_bytes.to_vec());
            }
            Ok::<_, EcMetadataError>(keys)
        })
        .await
        .map_err(|error| EcMetadataError::Database(error.to_string()))??;

        Ok(keys
            .into_iter()
            .map(|key| BatchWrite::Delete {
                keyspace: keyspace.clone(),
                key,
            })
            .collect())
    }

    /// Store a manifest in the local metadata store.
    pub async fn put_manifest(&self, manifest: &EcObjectManifest) -> Result<(), EcMetadataError> {
        manifest.validate()?;
        let db_key = serialize(&manifest.object_key())?;
        let value = encode_manifest(manifest)?;
        put_value(self.db.clone(), self.manifests.clone(), db_key, value).await
    }

    /// Store a manifest only when the local copy is not already this one.
    ///
    /// A read that resolves a manifest from the quorum caches it here. Doing
    /// that unconditionally turns every read into an fsync — [`Self::put_manifest`]
    /// persists with `SyncAll` — for a write that changes nothing. The local
    /// lookup that decides it costs an LSM read and no disk sync.
    ///
    /// # Errors
    ///
    /// Propagates a storage failure from either the read or the write.
    pub async fn cache_manifest(&self, manifest: &EcObjectManifest) -> Result<(), EcMetadataError> {
        if self.get_manifest(&manifest.object_key()).await?.as_ref() == Some(manifest) {
            return Ok(());
        }
        self.put_manifest(manifest).await
    }

    /// Store an object layout only when the local record is not already this one.
    ///
    /// Same reason as [`Self::cache_manifest`]: a read that learns the layout
    /// from the quorum must not pay an fsync to write back what is already there.
    ///
    /// # Errors
    ///
    /// Propagates a storage failure from either the read or the write.
    pub async fn cache_object_layout(
        &self,
        object: &EcObjectVersionKey,
        layout: &ObjectDataLayout,
    ) -> Result<(), EcMetadataError> {
        if self.get_object_layout(object).await?.as_ref() == Some(layout) {
            return Ok(());
        }
        self.put_object_layout(object, layout).await
    }

    /// Read a manifest from the local metadata store.
    pub async fn get_manifest(
        &self,
        object: &EcObjectVersionKey,
    ) -> Result<Option<EcObjectManifest>, EcMetadataError> {
        object.validate()?;
        let key = serialize(object)?;
        let keyspace = self.manifests.clone();
        task::spawn_blocking(move || match keyspace.get(&key[..]) {
            Ok(Some(bytes)) => decode_manifest(&bytes).map(Some),
            Ok(None) => Ok(None),
            Err(error) => Err(EcMetadataError::Database(error.to_string())),
        })
        .await
        .map_err(|error| EcMetadataError::Database(error.to_string()))?
    }

    /// Delete a manifest from the local metadata store.
    pub async fn delete_manifest(
        &self,
        object: &EcObjectVersionKey,
    ) -> Result<(), EcMetadataError> {
        object.validate()?;
        delete_value(self.db.clone(), self.manifests.clone(), serialize(object)?).await
    }

    /// List locally cached manifests in storage order up to `max_entries`.
    pub async fn list_manifests(
        &self,
        max_entries: usize,
    ) -> Result<Vec<EcObjectManifest>, EcMetadataError> {
        self.list_manifests_page(None, max_entries).await.map(|page| page.entries)
    }

    /// List one page of locally cached manifests in storage order.
    pub async fn list_manifests_page(
        &self,
        after_cursor: Option<Vec<u8>>,
        max_entries: usize,
    ) -> Result<EcMetadataPage<EcObjectManifest>, EcMetadataError> {
        list_keyspace_page(
            self.manifests.clone(),
            after_cursor,
            max_entries,
            |_, value_bytes| decode_manifest(&value_bytes),
        )
        .await
    }

    /// Store a node-local shard location.
    pub async fn put_local_shard(
        &self,
        key: &LocalShardKey,
        record: &LocalShardRecord,
    ) -> Result<(), EcMetadataError> {
        key.validate()?;
        record.validate()?;
        let db_key = serialize(key)?;
        let value = serialize(record)?;
        put_value(self.db.clone(), self.local_shards.clone(), db_key, value).await
    }

    /// Read a node-local shard location.
    pub async fn get_local_shard(
        &self,
        key: &LocalShardKey,
    ) -> Result<Option<LocalShardRecord>, EcMetadataError> {
        key.validate()?;
        get_value(self.local_shards.clone(), serialize(key)?).await
    }

    /// Delete a node-local shard location.
    pub async fn delete_local_shard(&self, key: &LocalShardKey) -> Result<(), EcMetadataError> {
        key.validate()?;
        delete_value(self.db.clone(), self.local_shards.clone(), serialize(key)?).await
    }

    /// List local shard records in storage order up to `max_entries`.
    pub async fn list_local_shards(
        &self,
        max_entries: usize,
    ) -> Result<Vec<(LocalShardKey, LocalShardRecord)>, EcMetadataError> {
        self.list_local_shards_page(None, max_entries).await.map(|page| page.entries)
    }

    /// List one page of local shard records in storage order.
    pub async fn list_local_shards_page(
        &self,
        after_cursor: Option<Vec<u8>>,
        max_entries: usize,
    ) -> Result<EcMetadataPage<(LocalShardKey, LocalShardRecord)>, EcMetadataError> {
        list_keyspace_page(
            self.local_shards.clone(),
            after_cursor,
            max_entries,
            |key_bytes, value_bytes| {
                let key: LocalShardKey = deserialize(&key_bytes)?;
                let record: LocalShardRecord = deserialize(&value_bytes)?;
                Ok((key, record))
            },
        )
        .await
    }

    /// List local shard records for one object/version.
    pub async fn list_local_shards_for_object(
        &self,
        object: &EcObjectVersionKey,
        max_entries: usize,
    ) -> Result<Vec<(LocalShardKey, LocalShardRecord)>, EcMetadataError> {
        object.validate()?;
        if max_entries == 0 {
            return Ok(Vec::new());
        }

        let target = object.clone();
        let local_shards = self.local_shards.clone();

        task::spawn_blocking(move || {
            let mut entries = Vec::new();
            for guard in local_shards.iter() {
                let (key_bytes, value_bytes) = guard
                    .into_inner()
                    .map_err(|error| EcMetadataError::Database(error.to_string()))?;
                let key: LocalShardKey = deserialize(&key_bytes)?;
                if key.object != target {
                    continue;
                }
                let record: LocalShardRecord = deserialize(&value_bytes)?;
                entries.push((key, record));
                if entries.len() >= max_entries {
                    break;
                }
            }
            Ok(entries)
        })
        .await
        .map_err(|error| EcMetadataError::Database(error.to_string()))?
    }

    /// Resolve a manifest using EC metadata read semantics.
    ///
    /// A current local hit may be served. A local miss or stale local manifest
    /// calls `quorum_read`; when quorum returns a manifest or tombstone, the
    /// result is cached locally before returning.
    pub async fn resolve_manifest_with_quorum<F, Fut>(
        &self,
        object: EcObjectVersionKey,
        is_local_current_enough: impl Fn(&EcObjectManifest) -> bool,
        quorum_read: F,
    ) -> Result<EcManifestReadResult, EcMetadataError>
    where
        F: FnOnce(EcObjectVersionKey) -> Fut,
        Fut: Future<Output = Result<EcManifestReadResult, EcMetadataError>>,
    {
        if let Some(local) = self.get_manifest(&object).await? {
            if is_local_current_enough(&local) {
                return Ok(local.into_read_result());
            }
        }

        let quorum_result = quorum_read(object).await?;
        match &quorum_result {
            EcManifestReadResult::Present(manifest) | EcManifestReadResult::Tombstone(manifest) => {
                self.put_manifest(manifest).await?;
            }
            EcManifestReadResult::Absent => {}
        }
        Ok(quorum_result)
    }

    /// Store durable state for one EC transcode job.
    pub async fn put_transcode_job(
        &self,
        job: &EcTranscodeJobRecord,
    ) -> Result<(), EcMetadataError> {
        job.validate()?;
        put_value(
            self.db.clone(),
            self.transcode_jobs.clone(),
            serialize(&job.operation_id)?,
            serialize(job)?,
        )
        .await
    }

    /// Read durable state for one EC transcode job.
    pub async fn get_transcode_job(
        &self,
        operation_id: Uuid,
    ) -> Result<Option<EcTranscodeJobRecord>, EcMetadataError> {
        if operation_id.is_nil() {
            return Err(EcMetadataError::InvalidLayout {
                reason: "transcode job operation_id must not be nil".to_string(),
            });
        }
        get_value(self.transcode_jobs.clone(), serialize(&operation_id)?).await
    }

    /// Delete queued/running transcode job records for one object version.
    pub async fn delete_transcode_jobs_for_object(
        &self,
        object: &EcObjectVersionKey,
        max_entries: usize,
    ) -> Result<EcTranscodeJobsRemoved, EcMetadataError> {
        object.validate()?;
        let mut removed = EcTranscodeJobsRemoved::default();
        if max_entries == 0 {
            return Ok(removed);
        }

        let object = object.clone();
        let mut cursor = None;

        loop {
            let page = self.list_transcode_jobs_page(cursor, max_entries).await?;
            cursor = page.next_cursor;
            let doomed = page
                .entries
                .into_iter()
                .filter(|job| job.object == object)
                .map(|job| (job.operation_id, job.origin))
                .collect::<Vec<_>>();

            for (operation_id, origin) in doomed {
                delete_value(
                    self.db.clone(),
                    self.transcode_jobs.clone(),
                    serialize(&operation_id)?,
                )
                .await?;
                removed.total = removed.total.saturating_add(1);
                if origin.is_tiering() {
                    removed.tiering = removed.tiering.saturating_add(1);
                }
            }

            if cursor.is_none() {
                break;
            }
        }

        Ok(removed)
    }

    /// List durable transcode jobs in storage order up to `max_entries`.
    pub async fn list_transcode_jobs(
        &self,
        max_entries: usize,
    ) -> Result<Vec<EcTranscodeJobRecord>, EcMetadataError> {
        self.list_transcode_jobs_page(None, max_entries).await.map(|page| page.entries)
    }

    /// List one page of durable transcode jobs.
    ///
    /// A record this build cannot decode is skipped rather than failing the
    /// page. The queue is written in bincode, which is not self-describing: a
    /// record written by a build with fewer fields ends early and cannot be
    /// read back, whatever `#[serde(default)]` says. Failing the page over one
    /// such record would stall every transcode on the node until an operator
    /// found it, and the record is worth far less than that — the object it
    /// names is still durable on its RF=3 replicas, and the next write or
    /// tiering scan queues it again.
    pub async fn list_transcode_jobs_page(
        &self,
        after_cursor: Option<Vec<u8>>,
        max_entries: usize,
    ) -> Result<EcMetadataPage<EcTranscodeJobRecord>, EcMetadataError> {
        let page: EcMetadataPage<Option<EcTranscodeJobRecord>> = list_keyspace_page(
            self.transcode_jobs.clone(),
            after_cursor,
            max_entries,
            |_, value_bytes| match deserialize::<EcTranscodeJobRecord>(&value_bytes) {
                Ok(job) => Ok(Some(job)),
                Err(error) => {
                    tracing::warn!(
                        %error,
                        "skipping an unreadable EC transcode job record; it will be re-queued"
                    );
                    Ok(None)
                }
            },
        )
        .await?;
        Ok(EcMetadataPage {
            entries: page.entries.into_iter().flatten().collect(),
            next_cursor: page.next_cursor,
        })
    }

    /// Try to acquire or steal an expired transcode lease.
    pub async fn try_acquire_transcode_lease(
        &self,
        operation_id: Uuid,
        owner_node_id: NodeId,
        now: u64,
        lease_ttl_nanos: u64,
    ) -> Result<bool, EcMetadataError> {
        if operation_id.is_nil() {
            return Err(EcMetadataError::InvalidLayout {
                reason: "transcode lease operation_id must not be nil".to_string(),
            });
        }
        if lease_ttl_nanos == 0 {
            return Err(EcMetadataError::InvalidLayout {
                reason: "transcode lease ttl must be greater than 0".to_string(),
            });
        }

        let db = self.db.clone();
        let leases = self.transcode_leases.clone();
        task::spawn_blocking(move || {
            let db_key = serialize(&operation_id)?;
            if let Some(bytes) = leases
                .get(&db_key[..])
                .map_err(|error| EcMetadataError::Database(error.to_string()))?
            {
                let current: EcTranscodeLeaseRecord = deserialize(&bytes)?;
                if current.is_alive_at(now) {
                    return Ok(false);
                }
            }

            let lease = EcTranscodeLeaseRecord {
                operation_id,
                owner_node_id,
                acquired_at: now,
                last_heartbeat_at: now,
                expires_at: now.saturating_add(lease_ttl_nanos),
            };
            lease.validate()?;
            let value = serialize(&lease)?;
            leases
                .insert(&db_key[..], &value[..])
                .map_err(|error| EcMetadataError::Database(error.to_string()))?;
            db.persist(PersistMode::SyncAll)
                .map_err(|error| EcMetadataError::Database(error.to_string()))?;
            Ok(true)
        })
        .await
        .map_err(|error| EcMetadataError::Database(error.to_string()))?
    }

    /// Renew a live transcode lease owned by `owner_node_id`.
    pub async fn renew_transcode_lease(
        &self,
        operation_id: Uuid,
        owner_node_id: NodeId,
        now: u64,
        lease_ttl_nanos: u64,
    ) -> Result<bool, EcMetadataError> {
        if operation_id.is_nil() {
            return Err(EcMetadataError::InvalidLayout {
                reason: "transcode lease operation_id must not be nil".to_string(),
            });
        }
        if lease_ttl_nanos == 0 {
            return Err(EcMetadataError::InvalidLayout {
                reason: "transcode lease ttl must be greater than 0".to_string(),
            });
        }

        let db = self.db.clone();
        let leases = self.transcode_leases.clone();
        task::spawn_blocking(move || {
            let db_key = serialize(&operation_id)?;
            let Some(bytes) = leases
                .get(&db_key[..])
                .map_err(|error| EcMetadataError::Database(error.to_string()))?
            else {
                return Ok(false);
            };
            let mut lease: EcTranscodeLeaseRecord = deserialize(&bytes)?;
            if lease.owner_node_id != owner_node_id || !lease.is_alive_at(now) {
                return Ok(false);
            }
            lease.last_heartbeat_at = now;
            lease.expires_at = now.saturating_add(lease_ttl_nanos);
            lease.validate()?;
            let value = serialize(&lease)?;
            leases
                .insert(&db_key[..], &value[..])
                .map_err(|error| EcMetadataError::Database(error.to_string()))?;
            db.persist(PersistMode::SyncAll)
                .map_err(|error| EcMetadataError::Database(error.to_string()))?;
            Ok(true)
        })
        .await
        .map_err(|error| EcMetadataError::Database(error.to_string()))?
    }

    /// Release a transcode lease if it is still owned by `owner_node_id`.
    pub async fn release_transcode_lease(
        &self,
        operation_id: Uuid,
        owner_node_id: NodeId,
    ) -> Result<(), EcMetadataError> {
        if operation_id.is_nil() {
            return Err(EcMetadataError::InvalidLayout {
                reason: "transcode lease operation_id must not be nil".to_string(),
            });
        }

        let db = self.db.clone();
        let leases = self.transcode_leases.clone();
        task::spawn_blocking(move || {
            let db_key = serialize(&operation_id)?;
            let should_remove = match leases
                .get(&db_key[..])
                .map_err(|error| EcMetadataError::Database(error.to_string()))?
            {
                Some(bytes) => {
                    let lease: EcTranscodeLeaseRecord = deserialize(&bytes)?;
                    lease.owner_node_id == owner_node_id
                }
                None => false,
            };
            if should_remove {
                leases
                    .remove(&db_key[..])
                    .map_err(|error| EcMetadataError::Database(error.to_string()))?;
                db.persist(PersistMode::SyncAll)
                    .map_err(|error| EcMetadataError::Database(error.to_string()))?;
            }
            Ok(())
        })
        .await
        .map_err(|error| EcMetadataError::Database(error.to_string()))?
    }

    /// Read a transcode lease by operation ID.
    pub async fn get_transcode_lease(
        &self,
        operation_id: Uuid,
    ) -> Result<Option<EcTranscodeLeaseRecord>, EcMetadataError> {
        if operation_id.is_nil() {
            return Err(EcMetadataError::InvalidLayout {
                reason: "transcode lease operation_id must not be nil".to_string(),
            });
        }
        get_value(self.transcode_leases.clone(), serialize(&operation_id)?).await
    }

    /// Store the hot-replica GC gate for one object version.
    pub async fn put_hot_replica_gc(
        &self,
        record: &EcHotReplicaGcRecord,
    ) -> Result<(), EcMetadataError> {
        record.validate()?;
        put_value(
            self.db.clone(),
            self.hot_replica_gc.clone(),
            serialize(&record.object)?,
            serialize(record)?,
        )
        .await
    }

    /// Read the hot-replica GC gate for one object version.
    pub async fn get_hot_replica_gc(
        &self,
        object: &EcObjectVersionKey,
    ) -> Result<Option<EcHotReplicaGcRecord>, EcMetadataError> {
        object.validate()?;
        get_value(self.hot_replica_gc.clone(), serialize(object)?).await
    }

    /// List hot-replica GC gates in storage order up to `max_entries`.
    pub async fn list_hot_replica_gc(
        &self,
        max_entries: usize,
    ) -> Result<Vec<EcHotReplicaGcRecord>, EcMetadataError> {
        self.list_hot_replica_gc_page(None, max_entries).await.map(|page| page.entries)
    }

    /// List one page of hot-replica GC gates.
    pub async fn list_hot_replica_gc_page(
        &self,
        after_cursor: Option<Vec<u8>>,
        max_entries: usize,
    ) -> Result<EcMetadataPage<EcHotReplicaGcRecord>, EcMetadataError> {
        list_keyspace_page(
            self.hot_replica_gc.clone(),
            after_cursor,
            max_entries,
            |_, value_bytes| deserialize(&value_bytes),
        )
        .await
    }

    /// Store durable state for one EC repair task.
    pub async fn put_repair_task(&self, task: &EcRepairTaskRecord) -> Result<(), EcMetadataError> {
        task.validate()?;
        put_value(
            self.db.clone(),
            self.repair_tasks.clone(),
            serialize(&task.repair_id)?,
            serialize(task)?,
        )
        .await
    }

    /// Read durable state for one EC repair task.
    pub async fn get_repair_task(
        &self,
        repair_id: Uuid,
    ) -> Result<Option<EcRepairTaskRecord>, EcMetadataError> {
        if repair_id.is_nil() {
            return Err(EcMetadataError::InvalidLayout {
                reason: "repair task repair_id must not be nil".to_string(),
            });
        }
        get_value(self.repair_tasks.clone(), serialize(&repair_id)?).await
    }

    /// List repair tasks in storage order up to `max_entries`.
    pub async fn list_repair_tasks(
        &self,
        max_entries: usize,
    ) -> Result<Vec<EcRepairTaskRecord>, EcMetadataError> {
        if max_entries == 0 {
            return Ok(Vec::new());
        }

        let repair_tasks = self.repair_tasks.clone();
        task::spawn_blocking(move || {
            let mut tasks = Vec::new();
            for guard in repair_tasks.iter() {
                let (_key_bytes, value_bytes) = guard
                    .into_inner()
                    .map_err(|error| EcMetadataError::Database(error.to_string()))?;
                let task: EcRepairTaskRecord = deserialize(&value_bytes)?;
                tasks.push(task);
                if tasks.len() >= max_entries {
                    break;
                }
            }
            Ok(tasks)
        })
        .await
        .map_err(|error| EcMetadataError::Database(error.to_string()))?
    }

    /// List repair tasks with a specific status up to `max_entries`.
    pub async fn list_repair_tasks_by_status(
        &self,
        status: EcRepairStatus,
        max_entries: usize,
    ) -> Result<Vec<EcRepairTaskRecord>, EcMetadataError> {
        if max_entries == 0 {
            return Ok(Vec::new());
        }

        let repair_tasks = self.repair_tasks.clone();
        task::spawn_blocking(move || {
            let mut tasks = Vec::new();
            for guard in repair_tasks.iter() {
                let (_key_bytes, value_bytes) = guard
                    .into_inner()
                    .map_err(|error| EcMetadataError::Database(error.to_string()))?;
                let task: EcRepairTaskRecord = deserialize(&value_bytes)?;
                if task.status != status {
                    continue;
                }
                tasks.push(task);
                if tasks.len() >= max_entries {
                    break;
                }
            }
            Ok(tasks)
        })
        .await
        .map_err(|error| EcMetadataError::Database(error.to_string()))?
    }

    /// Find the most urgent repair task that is ready to run at `now`.
    ///
    /// Urgency is how many shards of the task's own chunk the pool is missing,
    /// and chunks that are equally damaged keep the order they had before:
    /// oldest first. See decision EC2-160 for the measure and
    /// `RepairSelectionKey` for the order it produces.
    pub async fn find_next_ready_repair_task(
        &self,
        now: u64,
    ) -> Result<Option<EcRepairTaskRecord>, EcMetadataError> {
        let repair_tasks = self.repair_tasks.clone();
        task::spawn_blocking(move || {
            let best = best_ready_repair_task_per_set(&repair_tasks, now)?;
            let Some(urgency) = best.into_values().map(|(urgency, _)| urgency).min() else {
                return Ok(None);
            };
            read_repair_task(&repair_tasks, urgency.repair_id)
        })
        .await
        .map_err(|error| EcMetadataError::Database(error.to_string()))?
    }

    /// Find the most urgent ready repair task of every erasure set, in one scan.
    ///
    /// The queue is one keyspace shared by every set of the pool, so a set with
    /// a long backlog would always hold the best record and be served first
    /// for as long as the backlog lasts. Returning one candidate per set lets
    /// the caller take turns instead; the scan costs exactly what finding a
    /// single candidate costs.
    ///
    /// Urgency therefore orders tasks **inside** a set and never across sets:
    /// a set in a heavy incident takes its turn like any other.
    pub async fn find_next_ready_repair_task_per_set(
        &self,
        now: u64,
    ) -> Result<BTreeMap<u64, EcReadyRepairTasks>, EcMetadataError> {
        let repair_tasks = self.repair_tasks.clone();
        task::spawn_blocking(move || {
            let mut tasks = BTreeMap::new();
            for (erasure_set_id, (urgency, waiting)) in
                best_ready_repair_task_per_set(&repair_tasks, now)?
            {
                let Some(most_urgent) = read_repair_task(&repair_tasks, urgency.repair_id)? else {
                    continue;
                };
                let longest_waiting = if waiting.repair_id == urgency.repair_id {
                    most_urgent.clone()
                } else {
                    match read_repair_task(&repair_tasks, waiting.repair_id)? {
                        Some(task) => task,
                        None => most_urgent.clone(),
                    }
                };
                tasks.insert(
                    erasure_set_id,
                    EcReadyRepairTasks {
                        most_urgent,
                        longest_waiting,
                    },
                );
            }
            Ok(tasks)
        })
        .await
        .map_err(|error| EcMetadataError::Database(error.to_string()))?
    }

    /// Recover stale repair tasks that were left in `Running` by a crashed worker.
    pub async fn recover_stale_running_repair_tasks(
        &self,
        now: u64,
        running_timeout_nanos: u64,
        max_entries: usize,
    ) -> Result<usize, EcMetadataError> {
        if max_entries == 0 {
            return Ok(0);
        }

        let db = self.db.clone();
        let repair_tasks = self.repair_tasks.clone();
        task::spawn_blocking(move || {
            let mut recovered_tasks = Vec::new();
            for guard in repair_tasks.iter() {
                let (key_bytes, value_bytes) = guard
                    .into_inner()
                    .map_err(|error| EcMetadataError::Database(error.to_string()))?;
                let mut task: EcRepairTaskRecord = deserialize(&value_bytes)?;
                if !repair_task_is_stale_running(&task, now, running_timeout_nanos) {
                    continue;
                }

                task.status = EcRepairStatus::FailedRetryable;
                task.reason =
                    "repair worker lease expired; stale running task recovered".to_string();
                task.updated_at = now;
                task.next_retry_at = Some(now);
                task.validate()?;
                recovered_tasks.push((key_bytes.to_vec(), task));
                if recovered_tasks.len() >= max_entries {
                    break;
                }
            }

            for (key_bytes, task) in &recovered_tasks {
                let value = serialize(task)?;
                repair_tasks
                    .insert(&key_bytes[..], &value[..])
                    .map_err(|error| EcMetadataError::Database(error.to_string()))?;
            }

            if !recovered_tasks.is_empty() {
                db.persist(PersistMode::SyncAll)
                    .map_err(|error| EcMetadataError::Database(error.to_string()))?;
            }
            Ok(recovered_tasks.len())
        })
        .await
        .map_err(|error| EcMetadataError::Database(error.to_string()))?
    }

    /// Count repair tasks with statuses that still consume queue capacity.
    pub async fn count_active_repair_tasks(&self) -> Result<usize, EcMetadataError> {
        let repair_tasks = self.repair_tasks.clone();
        task::spawn_blocking(move || {
            let mut count = 0usize;
            for guard in repair_tasks.iter() {
                let (_key_bytes, value_bytes) = guard
                    .into_inner()
                    .map_err(|error| EcMetadataError::Database(error.to_string()))?;
                let task: EcRepairTaskRecord = deserialize(&value_bytes)?;
                if task.status.counts_against_queue_depth() {
                    count = count.saturating_add(1);
                }
            }
            Ok(count)
        })
        .await
        .map_err(|error| EcMetadataError::Database(error.to_string()))?
    }

    /// Store one durable orphan shard candidate.
    pub async fn put_orphan_shard(
        &self,
        key: &EcOrphanShardKey,
        record: &EcOrphanShardRecord,
    ) -> Result<(), EcMetadataError> {
        record.validate_for_key(key)?;
        put_value(
            self.db.clone(),
            self.orphan_shards.clone(),
            serialize(key)?,
            serialize(record)?,
        )
        .await
    }

    /// Read one orphan shard candidate.
    pub async fn get_orphan_shard(
        &self,
        key: &EcOrphanShardKey,
    ) -> Result<Option<EcOrphanShardRecord>, EcMetadataError> {
        key.validate()?;
        get_value(self.orphan_shards.clone(), serialize(key)?).await
    }

    /// Delete one orphan shard candidate.
    pub async fn delete_orphan_shard(&self, key: &EcOrphanShardKey) -> Result<(), EcMetadataError> {
        key.validate()?;
        delete_value(self.db.clone(), self.orphan_shards.clone(), serialize(key)?).await
    }

    /// List orphan shard records in storage order up to `max_entries`.
    pub async fn list_orphan_shards(
        &self,
        max_entries: usize,
    ) -> Result<Vec<(EcOrphanShardKey, EcOrphanShardRecord)>, EcMetadataError> {
        self.list_orphan_shards_page(None, max_entries).await.map(|page| page.entries)
    }

    /// List one page of orphan shard records in storage order.
    pub async fn list_orphan_shards_page(
        &self,
        after_cursor: Option<Vec<u8>>,
        max_entries: usize,
    ) -> Result<EcMetadataPage<(EcOrphanShardKey, EcOrphanShardRecord)>, EcMetadataError> {
        list_keyspace_page(
            self.orphan_shards.clone(),
            after_cursor,
            max_entries,
            |key_bytes, value_bytes| {
                let key: EcOrphanShardKey = deserialize(&key_bytes)?;
                let record: EcOrphanShardRecord = deserialize(&value_bytes)?;
                record.validate_for_key(&key)?;
                Ok((key, record))
            },
        )
        .await
    }

    /// Store a replicated-small physical GC gate.
    pub async fn put_replicated_small_gc(
        &self,
        record: &EcReplicatedSmallGcRecord,
    ) -> Result<(), EcMetadataError> {
        record.validate()?;
        put_value(
            self.db.clone(),
            self.small_object_gc.clone(),
            serialize(&record.object)?,
            serialize(record)?,
        )
        .await
    }

    /// Read a replicated-small physical GC gate.
    pub async fn get_replicated_small_gc(
        &self,
        object: &EcObjectVersionKey,
    ) -> Result<Option<EcReplicatedSmallGcRecord>, EcMetadataError> {
        object.validate()?;
        get_value(self.small_object_gc.clone(), serialize(object)?).await
    }

    /// Delete a replicated-small physical GC gate.
    pub async fn delete_replicated_small_gc(
        &self,
        object: &EcObjectVersionKey,
    ) -> Result<(), EcMetadataError> {
        object.validate()?;
        delete_value(
            self.db.clone(),
            self.small_object_gc.clone(),
            serialize(object)?,
        )
        .await
    }

    /// List replicated-small GC records in storage order up to `max_entries`.
    pub async fn list_replicated_small_gc(
        &self,
        max_entries: usize,
    ) -> Result<Vec<EcReplicatedSmallGcRecord>, EcMetadataError> {
        self.list_replicated_small_gc_page(None, max_entries)
            .await
            .map(|page| page.entries)
    }

    /// List one page of replicated-small GC records in storage order.
    pub async fn list_replicated_small_gc_page(
        &self,
        after_cursor: Option<Vec<u8>>,
        max_entries: usize,
    ) -> Result<EcMetadataPage<EcReplicatedSmallGcRecord>, EcMetadataError> {
        list_keyspace_page(
            self.small_object_gc.clone(),
            after_cursor,
            max_entries,
            |_, value_bytes| {
                let record: EcReplicatedSmallGcRecord = deserialize(&value_bytes)?;
                record.validate()?;
                Ok(record)
            },
        )
        .await
    }

    /// Store the activity mark of one object version.
    pub async fn put_object_heat(
        &self,
        record: &EcObjectHeatRecord,
    ) -> Result<(), EcMetadataError> {
        record.validate()?;
        let db_key = serialize(&record.object)?;
        let value = serialize(record)?;
        put_value(self.db.clone(), self.object_heat.clone(), db_key, value).await
    }

    /// Read the activity mark of one object version.
    ///
    /// A missing mark is not an error: it means no access has been recorded on
    /// this node yet, and the caller treats the object as used at creation time.
    pub async fn get_object_heat(
        &self,
        object: &EcObjectVersionKey,
    ) -> Result<Option<EcObjectHeatRecord>, EcMetadataError> {
        object.validate()?;
        get_value(self.object_heat.clone(), serialize(object)?).await
    }

    /// Delete the activity mark of one object version.
    pub async fn delete_object_heat(
        &self,
        object: &EcObjectVersionKey,
    ) -> Result<(), EcMetadataError> {
        object.validate()?;
        delete_value(
            self.db.clone(),
            self.object_heat.clone(),
            serialize(object)?,
        )
        .await
    }

    /// List one page of activity marks in storage order.
    pub async fn list_object_heat_page(
        &self,
        after_cursor: Option<Vec<u8>>,
        max_entries: usize,
    ) -> Result<EcMetadataPage<EcObjectHeatRecord>, EcMetadataError> {
        list_keyspace_page(
            self.object_heat.clone(),
            after_cursor,
            max_entries,
            |_, value_bytes| {
                let record: EcObjectHeatRecord = deserialize(&value_bytes)?;
                record.validate()?;
                Ok(record)
            },
        )
        .await
    }

    /// Store the tiering policy of one bucket.
    pub async fn put_tiering_policy(
        &self,
        record: &EcTieringPolicyRecord,
    ) -> Result<(), EcMetadataError> {
        record.validate()?;
        let db_key = serialize(&record.bucket)?;
        let value = serialize(record)?;
        put_value(
            self.db.clone(),
            self.tiering_policies.clone(),
            db_key,
            value,
        )
        .await
    }

    /// Read the tiering policy of one bucket.
    ///
    /// A missing policy means the bucket inherits every global value.
    pub async fn get_tiering_policy(
        &self,
        bucket: &str,
    ) -> Result<Option<EcTieringPolicyRecord>, EcMetadataError> {
        get_value(
            self.tiering_policies.clone(),
            serialize(&bucket.to_string())?,
        )
        .await
    }

    /// Delete the tiering policy of one bucket.
    pub async fn delete_tiering_policy(&self, bucket: &str) -> Result<(), EcMetadataError> {
        delete_value(
            self.db.clone(),
            self.tiering_policies.clone(),
            serialize(&bucket.to_string())?,
        )
        .await
    }

    /// List one page of tiering policies in storage order.
    pub async fn list_tiering_policies_page(
        &self,
        after_cursor: Option<Vec<u8>>,
        max_entries: usize,
    ) -> Result<EcMetadataPage<EcTieringPolicyRecord>, EcMetadataError> {
        list_keyspace_page(
            self.tiering_policies.clone(),
            after_cursor,
            max_entries,
            |_, value_bytes| {
                let record: EcTieringPolicyRecord = deserialize(&value_bytes)?;
                record.validate()?;
                Ok(record)
            },
        )
        .await
    }
    /// Cache the pool's erasure-set roster locally.
    ///
    /// A cache, not the source of truth: the roster is committed through the
    /// cluster write path, and this copy is what lets a node come back up with
    /// the sets it had before the quorum answers again.
    pub async fn put_erasure_set_roster(
        &self,
        roster: &EcErasureSetRoster,
    ) -> Result<(), EcMetadataError> {
        roster.validate()?;
        let value = serialize(roster)?;
        put_value(
            self.db.clone(),
            self.erasure_sets.clone(),
            ERASURE_SET_ROSTER_KEY.to_vec(),
            value,
        )
        .await
    }

    /// Read the locally cached erasure-set roster.
    ///
    /// A missing roster means the pool has only the set its configuration
    /// describes, which is every pool until an operator adds one.
    pub async fn get_erasure_set_roster(
        &self,
    ) -> Result<Option<EcErasureSetRoster>, EcMetadataError> {
        get_value(self.erasure_sets.clone(), ERASURE_SET_ROSTER_KEY.to_vec()).await
    }

    /// Cache the layout record of one erasure set locally.
    ///
    /// A cache, as the roster copy is: the record is committed through the
    /// cluster write path first. This copy is what keeps a restarted node on
    /// the recorded layout while the quorum cannot answer yet.
    pub async fn put_set_layout_record(
        &self,
        record: &EcSetLayoutRecord,
    ) -> Result<(), EcMetadataError> {
        record.validate()?;
        let value = serialize(record)?;
        put_value(
            self.db.clone(),
            self.erasure_sets.clone(),
            set_layout_record_key(record.set_id),
            value,
        )
        .await
    }

    /// Read the locally cached layout record of one erasure set.
    ///
    /// `None` means this node has never seen one, which says nothing about
    /// whether the cluster has it.
    pub async fn get_set_layout_record(
        &self,
        set_id: u64,
    ) -> Result<Option<EcSetLayoutRecord>, EcMetadataError> {
        let record: Option<EcSetLayoutRecord> =
            get_value(self.erasure_sets.clone(), set_layout_record_key(set_id)).await?;
        if let Some(record) = &record {
            record.validate()?;
        }
        Ok(record)
    }

    /// Store this node's tiering scan position.
    pub async fn put_tiering_cursor(
        &self,
        record: &EcTieringCursorRecord,
    ) -> Result<(), EcMetadataError> {
        record.validate()?;
        let value = serialize(record)?;
        put_value(
            self.db.clone(),
            self.tiering_cursor.clone(),
            TIERING_CURSOR_KEY.to_vec(),
            value,
        )
        .await
    }

    /// Record that this node is serving its EC pool right now.
    pub async fn put_node_presence(
        &self,
        record: &EcNodePresenceRecord,
    ) -> Result<(), EcMetadataError> {
        record.validate()?;
        let value = serialize(record)?;
        put_value(
            self.db.clone(),
            self.node_presence.clone(),
            NODE_PRESENCE_KEY.to_vec(),
            value,
        )
        .await
    }

    /// Read this node's own record of serving its EC pool.
    ///
    /// `None` means this storage has no memory of ever having served the pool:
    /// either the node has never run here, or its data directory is gone. Both
    /// read the same from here, and telling them apart is the caller's job.
    pub async fn get_node_presence(&self) -> Result<Option<EcNodePresenceRecord>, EcMetadataError> {
        get_value(self.node_presence.clone(), NODE_PRESENCE_KEY.to_vec()).await
    }

    /// Read this node's tiering scan position.
    ///
    /// A missing position means the node has not scanned yet and starts over.
    pub async fn get_tiering_cursor(
        &self,
    ) -> Result<Option<EcTieringCursorRecord>, EcMetadataError> {
        get_value(self.tiering_cursor.clone(), TIERING_CURSOR_KEY.to_vec()).await
    }

    /// Forget this node's tiering scan position.
    pub async fn delete_tiering_cursor(&self) -> Result<(), EcMetadataError> {
        delete_value(
            self.db.clone(),
            self.tiering_cursor.clone(),
            TIERING_CURSOR_KEY.to_vec(),
        )
        .await
    }

    /// Store the reverse migration job of one object.
    pub async fn put_promotion_job(
        &self,
        record: &EcPromotionJobRecord,
    ) -> Result<(), EcMetadataError> {
        record.validate()?;
        let db_key = serialize(&record.object)?;
        let value = serialize(record)?;
        put_value(self.db.clone(), self.promotion_jobs.clone(), db_key, value).await
    }

    /// Read the reverse migration job of one object.
    pub async fn get_promotion_job(
        &self,
        object: &EcObjectVersionKey,
    ) -> Result<Option<EcPromotionJobRecord>, EcMetadataError> {
        object.validate()?;
        get_value(self.promotion_jobs.clone(), serialize(object)?).await
    }

    /// Forget the reverse migration job of one object.
    pub async fn delete_promotion_job(
        &self,
        object: &EcObjectVersionKey,
    ) -> Result<(), EcMetadataError> {
        object.validate()?;
        delete_value(
            self.db.clone(),
            self.promotion_jobs.clone(),
            serialize(object)?,
        )
        .await
    }

    /// List one page of reverse migration jobs in storage order.
    pub async fn list_promotion_jobs_page(
        &self,
        after_cursor: Option<Vec<u8>>,
        max_entries: usize,
    ) -> Result<EcMetadataPage<EcPromotionJobRecord>, EcMetadataError> {
        list_keyspace_page(
            self.promotion_jobs.clone(),
            after_cursor,
            max_entries,
            |_, value_bytes| {
                let record: EcPromotionJobRecord = deserialize(&value_bytes)?;
                record.validate()?;
                Ok(record)
            },
        )
        .await
    }

    /// Count reverse migrations that have not reached a terminal state.
    ///
    /// Counting stops at `ceiling`: the caller only needs to know how much
    /// headroom is left, and a pool with a full queue should not pay for a
    /// whole keyspace walk to be told it has none.
    pub async fn count_in_flight_promotions(
        &self,
        ceiling: usize,
        scan_limit: usize,
    ) -> Result<usize, EcMetadataError> {
        let mut counted = 0usize;
        let mut cursor = None;
        loop {
            let page = self.list_promotion_jobs_page(cursor, scan_limit.max(1)).await?;
            cursor = page.next_cursor;
            for job in page.entries {
                if job.state.is_in_flight() {
                    counted = counted.saturating_add(1);
                    if counted >= ceiling {
                        return Ok(counted);
                    }
                }
            }
            if cursor.is_none() {
                return Ok(counted);
            }
        }
    }
}

/// The single key under which the node's own presence record is stored.
const NODE_PRESENCE_KEY: &[u8] = b"self";

/// The single key under which the node's scan position is stored.
const TIERING_CURSOR_KEY: &[u8] = b"scan";

/// The single key under which the tombstone sweep's position is stored.
const TOMBSTONE_GC_CURSOR_KEY: &[u8] = b"scan";

/// The roster is one record per pool, and a node serves one pool.
const ERASURE_SET_ROSTER_KEY: &[u8] = b"roster";

/// Layout records share the roster's keyspace, one per set, under a prefix the
/// roster key cannot collide with.
const SET_LAYOUT_RECORD_KEY_PREFIX: &[u8] = b"layout/";

fn set_layout_record_key(set_id: u64) -> Vec<u8> {
    let mut key = SET_LAYOUT_RECORD_KEY_PREFIX.to_vec();
    key.extend_from_slice(&set_id.to_be_bytes());
    key
}

/// Identity of one logical chunk, as the repair queue can see it.
///
/// A digest rather than the names themselves, so that counting losses costs
/// the same whatever a client calls its buckets and keys. Nothing durable is
/// built from it: two chunks colliding would order one selection wrongly and
/// leave no trace, which is why the cheap hash is the right one here and the
/// cryptographic one is reserved for the IDs that are written down.
type ChunkDigest = u64;

/// The order two ready repair tasks are served in, smallest key first.
///
/// Fields are compared in declaration order, which is the whole rule: the
/// chunk the pool is closest to losing goes first; chunks equally far from it
/// keep the order the queue had before this key existed, oldest first; and
/// `repair_id` decides nothing but ties, so that one queue always answers the
/// same way twice. Decisions EC2-160 and EC2-161.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct RepairSelectionKey {
    /// Missing shards of the task's chunk, most damaged chunk first.
    losses: std::cmp::Reverse<u32>,
    /// Queue insertion time, oldest first.
    enqueued_at: u64,
    /// Stable tie-break between tasks the first two fields cannot separate.
    repair_id: Uuid,
}

/// The order the starvation guard serves a set's ready tasks in.
///
/// Oldest first, and `repair_id` only to keep the order total. No task queued
/// later can move ahead of an earlier one here, which is what bounds the wait
/// of a task the urgency order keeps passing over (decision EC2-162).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct RepairWaitingKey {
    /// Queue insertion time, oldest first.
    enqueued_at: u64,
    /// Stable tie-break between tasks queued in the same instant.
    repair_id: Uuid,
}

/// One ready task as the queue scan keeps it.
///
/// Deliberately not the record itself: a queue at its depth limit holds
/// thousands of ready tasks, and only the winner of a set is ever read back.
#[derive(Debug, Clone, Copy)]
struct ReadyRepairTask {
    chunk: ChunkDigest,
    erasure_set_id: u64,
    enqueued_at: u64,
    repair_id: Uuid,
}

/// Which chunk a repair task belongs to.
///
/// Losses are counted per chunk, because a chunk is what the codec rebuilds:
/// the object version, the operation that wrote the winning manifest and the
/// chunk index.
fn repair_chunk_digest(task: &EcRepairTaskRecord) -> ChunkDigest {
    let mut hasher = DefaultHasher::new();
    task.object.bucket.hash(&mut hasher);
    task.object.key.hash(&mut hasher);
    task.object.version_id.hash(&mut hasher);
    task.operation_id.hash(&mut hasher);
    task.chunk_index.hash(&mut hasher);
    hasher.finish()
}

/// Scan the durable queue once and name the best ready task of every set.
///
/// The scan reads every record because that is what finding the oldest task
/// already cost. Living tasks of a chunk are counted while they pass, so
/// urgency is known by the end of the same pass and never has to be stored
/// next to the task — a stored count would be a second truth about the chunk,
/// kept in agreement by hand (decision EC2-161).
///
/// Terminal tasks are counted as nothing: a repaired shard is not a loss, and
/// a task closed as beyond repair will not become one on its own. The
/// predicate is the one queue depth is already measured by, so the measure and
/// the metric cannot disagree about what is still live.
fn best_ready_repair_task_per_set(
    repair_tasks: &Keyspace,
    now: u64,
) -> Result<BTreeMap<u64, (RepairSelectionKey, RepairWaitingKey)>, EcMetadataError> {
    let mut losses: HashMap<ChunkDigest, u32> = HashMap::new();
    let mut ready: Vec<ReadyRepairTask> = Vec::new();

    for guard in repair_tasks.iter() {
        let (_key_bytes, value_bytes) = guard
            .into_inner()
            .map_err(|error| EcMetadataError::Database(error.to_string()))?;
        let task: EcRepairTaskRecord = deserialize(&value_bytes)?;
        if task.status.is_terminal() {
            continue;
        }

        let chunk = repair_chunk_digest(&task);
        let count = losses.entry(chunk).or_insert(0);
        *count = count.saturating_add(1);

        if repair_task_is_ready_at(&task, now) {
            ready.push(ReadyRepairTask {
                chunk,
                erasure_set_id: task.erasure_set_id,
                enqueued_at: task.enqueued_at,
                repair_id: task.repair_id,
            });
        }
    }

    let mut best: BTreeMap<u64, (RepairSelectionKey, RepairWaitingKey)> = BTreeMap::new();
    for candidate in ready {
        let urgency = RepairSelectionKey {
            losses: std::cmp::Reverse(losses.get(&candidate.chunk).copied().unwrap_or(1)),
            enqueued_at: candidate.enqueued_at,
            repair_id: candidate.repair_id,
        };
        let waiting = RepairWaitingKey {
            enqueued_at: candidate.enqueued_at,
            repair_id: candidate.repair_id,
        };
        match best.entry(candidate.erasure_set_id) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert((urgency, waiting));
            }
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                let best = entry.get_mut();
                if urgency < best.0 {
                    best.0 = urgency;
                }
                if waiting < best.1 {
                    best.1 = waiting;
                }
            }
        }
    }
    Ok(best)
}

/// Read one repair task by ID inside a blocking scan.
///
/// `None` means the record was gone between the scan and this read. No path
/// deletes repair records, so it stands for "ask again next scan" rather than
/// for an error.
fn read_repair_task(
    repair_tasks: &Keyspace,
    repair_id: Uuid,
) -> Result<Option<EcRepairTaskRecord>, EcMetadataError> {
    let key = serialize(&repair_id)?;
    match repair_tasks.get(&key[..]) {
        Ok(Some(value_bytes)) => deserialize(&value_bytes).map(Some),
        Ok(None) => Ok(None),
        Err(error) => Err(EcMetadataError::Database(error.to_string())),
    }
}

fn repair_task_is_ready_at(task: &EcRepairTaskRecord, now: u64) -> bool {
    matches!(
        task.status,
        EcRepairStatus::Queued | EcRepairStatus::FailedRetryable
    ) && task.next_retry_at.is_none_or(|retry_at| retry_at <= now)
}

fn repair_task_is_stale_running(
    task: &EcRepairTaskRecord,
    now: u64,
    running_timeout_nanos: u64,
) -> bool {
    task.status == EcRepairStatus::Running
        && now.saturating_sub(task.updated_at) >= running_timeout_nanos
}

/// One write inside an atomic metadata batch.
enum BatchWrite {
    Put {
        keyspace: Keyspace,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        keyspace: Keyspace,
        key: Vec<u8>,
    },
}

/// Apply several writes as one durable, all-or-nothing batch.
///
/// This is what lets an object's layout and its segment's index move together.
/// Without it a crash between the two writes would leave either an object no
/// segment claims or a segment claiming an object that does not exist.
async fn commit_batch(db: Database, writes: Vec<BatchWrite>) -> Result<(), EcMetadataError> {
    if writes.is_empty() {
        return Ok(());
    }
    task::spawn_blocking(move || {
        let mut batch = db.batch().durability(Some(PersistMode::SyncAll));
        for write in writes {
            match write {
                BatchWrite::Put {
                    keyspace,
                    key,
                    value,
                } => batch.insert(&keyspace, key, value),
                BatchWrite::Delete { keyspace, key } => batch.remove(&keyspace, key),
            }
        }
        batch.commit().map_err(|error| EcMetadataError::Database(error.to_string()))
    })
    .await
    .map_err(|error| EcMetadataError::Database(error.to_string()))?
}

fn packed_segment_key(segment_id: PackedSegmentId) -> Vec<u8> {
    segment_id.as_uuid().into_bytes().to_vec()
}

/// Journal key: segment id, then sequence in big-endian order.
///
/// Big-endian is what makes the keyspace's byte order the journal's write
/// order, so replay can read a segment's records back in sequence without
/// sorting them.
fn packed_segment_journal_key(segment_id: PackedSegmentId, sequence: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(24);
    key.extend_from_slice(&segment_id.as_uuid().into_bytes());
    key.extend_from_slice(&sequence.to_be_bytes());
    key
}

fn decode_packed_segment_journal_key(
    key: &[u8],
) -> Result<(PackedSegmentId, u64), EcMetadataError> {
    if key.len() != 24 {
        return Err(EcMetadataError::Database(format!(
            "packed segment journal key is {} byte(s), expected 24",
            key.len()
        )));
    }
    let mut uuid_bytes = [0u8; 16];
    uuid_bytes.copy_from_slice(&key[..16]);
    let mut sequence_bytes = [0u8; 8];
    sequence_bytes.copy_from_slice(&key[16..]);
    Ok((
        PackedSegmentId::from_uuid(Uuid::from_bytes(uuid_bytes)),
        u64::from_be_bytes(sequence_bytes),
    ))
}

/// Dead-set key: segment id, then the SHA-256 of the serialized object key.
///
/// Hashing keeps the key a fixed 48 bytes whatever the object key looks like,
/// and makes the row idempotent: recording the same deletion twice writes the
/// same row rather than a second one.
fn packed_segment_dead_key(
    segment_id: PackedSegmentId,
    object: &EcObjectVersionKey,
) -> Result<Vec<u8>, EcMetadataError> {
    let mut key = Vec::with_capacity(48);
    key.extend_from_slice(&segment_id.as_uuid().into_bytes());
    key.extend_from_slice(&sha256(&serialize(object)?));
    Ok(key)
}

fn decode_packed_segment_dead_key(key: &[u8]) -> Result<PackedSegmentId, EcMetadataError> {
    if key.len() != 48 {
        return Err(EcMetadataError::Database(format!(
            "packed segment dead key is {} byte(s), expected 48",
            key.len()
        )));
    }
    let mut uuid_bytes = [0u8; 16];
    uuid_bytes.copy_from_slice(&key[..16]);
    Ok(PackedSegmentId::from_uuid(Uuid::from_bytes(uuid_bytes)))
}

/// Check that a repack move describes the replacement entry it claims.
///
/// The layout and the segment index are two records of the same fact, written
/// by the same batch. If they can disagree, a direct read verifies bytes
/// against one hash while the index promises another, and the mismatch surfaces
/// as corruption long after the repack that caused it.
fn ensure_move_matches_entry(
    destination: &PackedSegment,
    moved: &PackedRepackMove,
) -> Result<(), EcMetadataError> {
    let entry =
        destination
            .entry_for(&moved.object)
            .ok_or_else(|| EcMetadataError::InvalidLayout {
                reason: format!(
                    "replacement segment {} does not list {}/{}",
                    destination.segment_id, moved.object.bucket, moved.object.key
                ),
            })?;
    if moved.placement.offset != entry.offset
        || moved.placement.length != entry.length
        || moved.placement.content_hash_sha256 != entry.content_hash_sha256
        || moved.placement.erasure_set_id != destination.erasure_set_id
    {
        return Err(EcMetadataError::InvalidLayout {
            reason: format!(
                "the move of {}/{} places it at {}+{} but segment {} lists it at {}+{}",
                moved.object.bucket,
                moved.object.key,
                moved.placement.offset,
                moved.placement.length,
                destination.segment_id,
                entry.offset,
                entry.length
            ),
        });
    }
    if !moved.placement.hot_replica_nodes.is_empty() {
        return Err(EcMetadataError::InvalidLayout {
            reason: format!(
                "the move of {}/{} names {} hot replica(s); a repacked object is read from its \
                 replacement's shards and never had hot copies to name",
                moved.object.bucket,
                moved.object.key,
                moved.placement.hot_replica_nodes.len()
            ),
        });
    }
    moved.placement.validate()
}

/// Present a packed-segment invariant failure as a metadata error.
pub(crate) fn packed_segment_error(error: PackedSegmentError) -> EcMetadataError {
    EcMetadataError::InvalidLayout {
        reason: error.to_string(),
    }
}

fn open_keyspace(db: &Database, name: &str) -> Result<Keyspace, EcMetadataError> {
    db.keyspace(name, KeyspaceCreateOptions::default)
        .map_err(|error| EcMetadataError::Database(error.to_string()))
}

async fn put_value(
    db: Database,
    keyspace: Keyspace,
    key: Vec<u8>,
    value: Vec<u8>,
) -> Result<(), EcMetadataError> {
    task::spawn_blocking(move || {
        keyspace
            .insert(&key[..], &value[..])
            .map_err(|error| EcMetadataError::Database(error.to_string()))?;
        db.persist(PersistMode::SyncAll)
            .map_err(|error| EcMetadataError::Database(error.to_string()))?;
        Ok(())
    })
    .await
    .map_err(|error| EcMetadataError::Database(error.to_string()))?
}

async fn get_value<T: DeserializeOwned + Send + 'static>(
    keyspace: Keyspace,
    key: Vec<u8>,
) -> Result<Option<T>, EcMetadataError> {
    task::spawn_blocking(move || match keyspace.get(&key[..]) {
        Ok(Some(bytes)) => deserialize(&bytes).map(Some),
        Ok(None) => Ok(None),
        Err(error) => Err(EcMetadataError::Database(error.to_string())),
    })
    .await
    .map_err(|error| EcMetadataError::Database(error.to_string()))?
}

async fn delete_value(
    db: Database,
    keyspace: Keyspace,
    key: Vec<u8>,
) -> Result<(), EcMetadataError> {
    task::spawn_blocking(move || {
        keyspace
            .remove(&key[..])
            .map_err(|error| EcMetadataError::Database(error.to_string()))?;
        db.persist(PersistMode::SyncAll)
            .map_err(|error| EcMetadataError::Database(error.to_string()))?;
        Ok(())
    })
    .await
    .map_err(|error| EcMetadataError::Database(error.to_string()))?
}

/// Seek directly past the last returned key instead of walking the prefix again.
fn keyspace_page_iter(keyspace: &Keyspace, after_cursor: Option<Vec<u8>>) -> fjall::Iter {
    let lower = after_cursor.map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded);
    keyspace.range((lower, std::ops::Bound::Unbounded))
}

async fn list_keyspace_page<T, F>(
    keyspace: Keyspace,
    after_cursor: Option<Vec<u8>>,
    max_entries: usize,
    decode: F,
) -> Result<EcMetadataPage<T>, EcMetadataError>
where
    T: Send + 'static,
    F: Fn(Vec<u8>, Vec<u8>) -> Result<T, EcMetadataError> + Send + 'static,
{
    if max_entries == 0 {
        return Ok(EcMetadataPage {
            entries: Vec::new(),
            next_cursor: None,
        });
    }

    task::spawn_blocking(move || {
        let mut entries = Vec::new();
        let mut last_returned_key = None;
        let mut next_cursor = None;

        for guard in keyspace_page_iter(&keyspace, after_cursor) {
            let (key_bytes, value_bytes) = guard
                .into_inner()
                .map_err(|error| EcMetadataError::Database(error.to_string()))?;
            let key_vec = key_bytes.to_vec();
            if entries.len() >= max_entries {
                next_cursor = last_returned_key;
                break;
            }

            entries.push(decode(key_vec.clone(), value_bytes.to_vec())?);
            last_returned_key = Some(key_vec);
        }

        Ok(EcMetadataPage {
            entries,
            next_cursor,
        })
    })
    .await
    .map_err(|error| EcMetadataError::Database(error.to_string()))?
}

/// Encode a manifest for durable storage.
///
/// The body is the manifest exactly as every earlier build wrote it — the
/// packed placement is `#[serde(skip)]` and contributes nothing to it. A packed
/// manifest appends the placement after that body, so a classic manifest's
/// bytes are unchanged and a manifest written before this build still decodes.
///
/// This is the only way a manifest may be written. A direct `bincode::serialize`
/// would silently drop the placement and produce a record that claims to be a
/// packed object while saying nothing about where its bytes are.
///
/// # Errors
///
/// Returns [`EcMetadataError::Serialization`] when bincode fails.
pub fn encode_manifest(manifest: &EcObjectManifest) -> Result<Vec<u8>, EcMetadataError> {
    let mut bytes = serialize(manifest)?;
    if let Some(packed) = &manifest.packed {
        bytes.extend_from_slice(&serialize(packed)?);
    }
    Ok(bytes)
}

/// Encode a replicated-small placement for the pool's hidden metadata objects.
///
/// A small object has no manifest — that is the point of the layout — but the
/// decision that it is replicated rather than erasure coded still has to reach
/// the nodes that hold it. The record is written the same way a manifest is,
/// so it survives the loss of the node that made the decision.
pub fn encode_replicated_small_placement(
    placement: &ReplicatedSmallObjectPlacement,
) -> Result<Vec<u8>, EcMetadataError> {
    serialize(placement)
}

/// Decode a placement written by [`encode_replicated_small_placement`].
///
/// # Errors
///
/// Returns [`EcMetadataError::Serialization`] when the bytes do not decode, and
/// [`EcMetadataError::InvalidLayout`] when the placement they hold is not a
/// valid RF=3 small-object placement.
pub fn decode_replicated_small_placement(
    bytes: &[u8],
) -> Result<ReplicatedSmallObjectPlacement, EcMetadataError> {
    let placement: ReplicatedSmallObjectPlacement = deserialize(bytes)?;
    placement.validate()?;
    Ok(placement)
}

/// Decode a manifest written by [`encode_manifest`] or by any earlier build.
///
/// `format_version` is the first field of the body, so the byte stream says
/// which shape follows before anything else is read.
///
/// # Errors
///
/// Returns [`EcMetadataError::Serialization`] when the bytes do not decode, and
/// [`EcMetadataError::InvalidLayout`] when the version is one this build does
/// not write.
pub fn decode_manifest(bytes: &[u8]) -> Result<EcObjectManifest, EcMetadataError> {
    let mut cursor = std::io::Cursor::new(bytes);
    let mut manifest: EcObjectManifest = bincode::deserialize_from(&mut cursor)
        .map_err(|error| EcMetadataError::Serialization(error.to_string()))?;
    match manifest.format_version {
        EC_MANIFEST_FORMAT_VERSION => {}
        EC_PACKED_MANIFEST_FORMAT_VERSION => {
            manifest.packed = Some(
                bincode::deserialize_from(&mut cursor)
                    .map_err(|error| EcMetadataError::Serialization(error.to_string()))?,
            );
        }
        other => {
            return Err(EcMetadataError::InvalidLayout {
                reason: format!(
                    "manifest format_version {other} was written by a newer build; this one \
                     reads {EC_MANIFEST_FORMAT_VERSION} and {EC_PACKED_MANIFEST_FORMAT_VERSION}"
                ),
            });
        }
    }
    Ok(manifest)
}

fn serialize<T: Serialize>(value: &T) -> Result<Vec<u8>, EcMetadataError> {
    bincode::serialize(value).map_err(|error| EcMetadataError::Serialization(error.to_string()))
}

fn deserialize<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, EcMetadataError> {
    bincode::deserialize(bytes).map_err(|error| EcMetadataError::Serialization(error.to_string()))
}

fn validate_object_key(
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
) -> Result<(), EcMetadataError> {
    if bucket.trim().is_empty() {
        return Err(EcMetadataError::InvalidLayout {
            reason: "bucket must not be empty".to_string(),
        });
    }
    if key.is_empty() {
        return Err(EcMetadataError::InvalidLayout {
            reason: "key must not be empty".to_string(),
        });
    }
    if version_id.is_some_and(str::is_empty) {
        return Err(EcMetadataError::InvalidLayout {
            reason: "version_id must not be empty when present".to_string(),
        });
    }
    Ok(())
}

fn ensure_distinct_nodes(nodes: &[NodeId], context: &str) -> Result<(), EcMetadataError> {
    let mut seen = HashSet::with_capacity(nodes.len());
    for node in nodes {
        if !seen.insert(*node) {
            return Err(EcMetadataError::InvalidLayout {
                reason: format!("{context} contains duplicate node {node}"),
            });
        }
    }
    Ok(())
}

fn invalid_manifest_without_object(reason: impl Into<String>) -> EcMetadataError {
    EcMetadataError::InvalidManifest {
        bucket: String::new(),
        key: String::new(),
        reason: reason.into(),
    }
}

fn shard_role_from_slot(role: ShardSlotRole) -> ShardRole {
    match role {
        ShardSlotRole::Data => ShardRole::Data,
        ShardSlotRole::ReedSolomonParity => ShardRole::ReedSolomonParity,
        ShardSlotRole::LrcLocalParity => ShardRole::LrcLocalParity,
        ShardSlotRole::LrcGlobalParity => ShardRole::LrcGlobalParity,
    }
}

fn sha256(bytes: &[u8]) -> [u8; 32] {
    Sha256::new().chain_update(bytes).finalize().into()
}

/// How far along the staged ladder a state is.
///
/// One ranking rather than two: the hidden manifest's HLC is derived from it,
/// which is what makes last-write-wins keep the furthest state, and a transcode
/// that meets its own earlier work reads the same order to tell "already past
/// this" from "something else wrote here".
pub(crate) fn manifest_state_rank(state: EcPlacementState) -> u32 {
    match state {
        EcPlacementState::HotReplicated => 0,
        EcPlacementState::EcTranscodeQueued => 1,
        EcPlacementState::EcEncoding => 2,
        EcPlacementState::EcShardsDurable => 3,
        EcPlacementState::EcManifestCommitted => 4,
        EcPlacementState::EcReadVerified => 5,
        EcPlacementState::EcCommitted => 6,
        EcPlacementState::HotReplicasGcEligible => 7,
        EcPlacementState::HotReplicasPurged => 8,
        // Between the state it replaces and the one it leads to: the marker has
        // to win against the purged manifest at the same HLC, and the promoted
        // manifest has to win against the marker (EC2-026).
        EcPlacementState::Promoting => 9,
        EcPlacementState::Promoted => 10,
        EcPlacementState::EcFailedRetryable => 20,
        EcPlacementState::EcFailedPermanent => 21,
        EcPlacementState::Tombstone => 30,
    }
}

/// HLC used only for the hidden manifest object stored in the hot engine.
pub(crate) fn manifest_storage_hlc(base: HlcTimestamp, state: EcPlacementState) -> HlcTimestamp {
    let rank = manifest_state_rank(state);

    let mut hlc = base;
    match hlc.logical.checked_add(rank) {
        Some(logical) => hlc.logical = logical,
        None => {
            hlc.wall_time = hlc.wall_time.saturating_add(1);
            hlc.logical = rank;
        }
    }
    hlc
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    use super::*;

    #[test]
    fn metadata_page_iterator_seeks_past_late_cursor() {
        let tmp = tempfile::tempdir().unwrap();
        let store = EcMetadataStore::open(tmp.path()).unwrap();
        let keyspace = &store.manifests;
        for index in 0u32..1000 {
            keyspace.insert(index.to_be_bytes(), b"value").unwrap();
        }
        let cursor = 990u32.to_be_bytes().to_vec();
        let mut iter = keyspace_page_iter(keyspace, Some(cursor.clone()));
        let (first, _) = iter.next().unwrap().into_inner().unwrap();
        assert_eq!(
            first.as_ref(),
            991u32.to_be_bytes().as_slice(),
            "the raw page iterator must seek, not enumerate the preceding 991 keys"
        );
        assert_eq!(iter.count(), 8);

        keyspace.remove(&cursor).unwrap();
        let (first, _) =
            keyspace_page_iter(keyspace, Some(cursor)).next().unwrap().into_inner().unwrap();
        assert_eq!(first.as_ref(), 991u32.to_be_bytes().as_slice());
        for end in [999u32, 1000] {
            assert!(
                keyspace_page_iter(keyspace, Some(end.to_be_bytes().to_vec())).next().is_none()
            );
        }
    }

    #[tokio::test]
    async fn metadata_pages_preserve_binary_boundaries_and_deleted_cursor() {
        let tmp = tempfile::tempdir().unwrap();
        let store = EcMetadataStore::open(tmp.path()).unwrap();
        let keyspace = &store.manifests;
        let keys = [vec![0], vec![0, 0], vec![0, 255], vec![255]];
        for key in &keys {
            keyspace.insert(key, b"value").unwrap();
        }
        let first = list_keyspace_page(keyspace.clone(), None, 2, |key, _| Ok(key)).await.unwrap();
        assert_eq!(first.entries, keys[..2]);
        assert_eq!(first.next_cursor, Some(keys[1].clone()));
        keyspace.remove(&keys[1]).unwrap();
        let rest = list_keyspace_page(keyspace.clone(), first.next_cursor, 2, |key, _| Ok(key))
            .await
            .unwrap();
        assert_eq!(rest.entries, keys[2..]);
        assert!(rest.next_cursor.is_none());
        let end = list_keyspace_page(keyspace.clone(), Some(keys[3].clone()), 2, |key, _| Ok(key))
            .await
            .unwrap();
        assert!(end.entries.is_empty());
        assert!(end.next_cursor.is_none());
    }

    #[tokio::test]
    async fn metadata_page_zero_limit_and_empty_keyspace_are_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let store = EcMetadataStore::open(tmp.path()).unwrap();
        let keyspace = &store.manifests;
        for cursor in [None, Some(vec![42])] {
            assert!(keyspace_page_iter(keyspace, cursor.clone()).next().is_none());
            let empty =
                list_keyspace_page(keyspace.clone(), cursor, 1, |key, _| Ok(key)).await.unwrap();
            assert!(empty.entries.is_empty());
            assert!(empty.next_cursor.is_none());
        }
        keyspace.insert([42], b"value").unwrap();
        let zero = list_keyspace_page(keyspace.clone(), None, 0, |key, _| Ok(key)).await.unwrap();
        assert!(zero.entries.is_empty());
        assert!(zero.next_cursor.is_none());
    }

    fn placement_hint_at(wall_time: u64) -> EcSmallPlacementHint {
        EcSmallPlacementHint {
            placement: ReplicatedSmallObjectPlacement::new(
                vec![NodeId(1), NodeId(2), NodeId(3)],
                [7; 32],
                8,
                HlcTimestamp::from_record(wall_time, 0, 1),
            )
            .unwrap(),
            recorded_at: wall_time,
            acks: 0,
            attempts: 1,
        }
    }

    #[tokio::test]
    async fn placement_hint_older_publication_cannot_replace_newer_generation() {
        let tmp = tempfile::tempdir().unwrap();
        let store = EcMetadataStore::open(tmp.path()).unwrap();
        let object = EcObjectVersionKey::new("bucket", "object", None);
        let newer = placement_hint_at(2);
        store.put_small_placement_hint(&object, &newer).await.unwrap();
        store.put_small_placement_hint(&object, &placement_hint_at(1)).await.unwrap();
        assert_eq!(
            store.get_small_placement_hint(&object).await.unwrap(),
            Some(newer)
        );
    }

    #[tokio::test]
    async fn placement_hint_snapshot_cannot_update_or_delete_a_replacement() {
        let tmp = tempfile::tempdir().unwrap();
        let store = EcMetadataStore::open(tmp.path()).unwrap();
        let object = EcObjectVersionKey::new("bucket", "object", None);
        let old = placement_hint_at(1);
        let new = placement_hint_at(2);
        store.put_small_placement_hint(&object, &old).await.unwrap();
        let snapshot = store.get_small_placement_hint(&object).await.unwrap().unwrap();
        store.put_small_placement_hint(&object, &new).await.unwrap();
        assert!(!store
            .replace_small_placement_hint_if_matches(&object, &snapshot, None)
            .await
            .unwrap());
        let mut retried = snapshot.clone();
        retried.attempts += 1;
        assert!(!store
            .replace_small_placement_hint_if_matches(&object, &snapshot, Some(&retried))
            .await
            .unwrap());
        assert!(!store
            .delete_small_placement_hint_through(&object, old.placement.hlc)
            .await
            .unwrap());
        assert_eq!(
            store.get_small_placement_hint(&object).await.unwrap(),
            Some(new.clone())
        );
        let mut retry_new = new.clone();
        retry_new.attempts += 1;
        assert!(store
            .replace_small_placement_hint_if_matches(&object, &new, Some(&retry_new))
            .await
            .unwrap());
        assert!(!store.put_small_placement_hint(&object, &new).await.unwrap());
        assert!(store
            .delete_small_placement_hint_through(&object, new.placement.hlc)
            .await
            .unwrap());
        assert!(store.get_small_placement_hint(&object).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn placement_hint_cursor_reaches_tail_across_restart_and_wraps() {
        let tmp = tempfile::tempdir().unwrap();
        let first_keys = {
            let store = EcMetadataStore::open(tmp.path()).unwrap();
            for index in 0..6 {
                store
                    .put_small_placement_hint(
                        &EcObjectVersionKey::new("bucket", format!("key{index}"), None),
                        &placement_hint_at(1),
                    )
                    .await
                    .unwrap();
            }
            let first = store.list_small_placement_hints_page(None, 2).await.unwrap();
            store.put_small_placement_hint_cursor(first.next_cursor).await.unwrap();
            // The undeliverable head remains; only remove the cursor boundary.
            store
                .replace_small_placement_hint_if_matches(
                    &first.entries[1].0,
                    &first.entries[1].1,
                    None,
                )
                .await
                .unwrap();
            first.entries.into_iter().map(|(key, _)| key).collect::<Vec<_>>()
        };
        let store = EcMetadataStore::open(tmp.path()).unwrap();
        let mut tail_keys = Vec::new();
        loop {
            let cursor = store.get_small_placement_hint_cursor().await.unwrap();
            let page = store.list_small_placement_hints_page(cursor, 2).await.unwrap();
            tail_keys.extend(page.entries.into_iter().map(|(key, _)| key));
            let completed = page.next_cursor.is_none();
            store.put_small_placement_hint_cursor(page.next_cursor).await.unwrap();
            if completed {
                break;
            }
        }
        assert_eq!(tail_keys.len(), 4);
        assert!(tail_keys.iter().all(|key| !first_keys.contains(key)));
        assert!(store.get_small_placement_hint_cursor().await.unwrap().is_none());
        let next_round = store.list_small_placement_hints_page(None, 2).await.unwrap();
        assert_eq!(next_round.entries[0].0, first_keys[0]);
    }
    use crate::ec_packed_segments::{PackedObjectIdentity, PackedSegmentConfig, SealReason};
    use crate::erasure_coding::{
        legacy_pool_set_id, EcCodecKind, EcFailureDomainLevel, EcLayoutViolationReason, EcParity,
        EcProfile, ErasureSetRegistry, TopologyLabels, DEFAULT_CHUNK_SIZE, DEFAULT_MIN_OBJECT_SIZE,
    };

    /// One day in nanoseconds — the default promotion window.
    const DAY_NANOS: u64 = 24 * 3_600 * 1_000_000_000;

    fn promotion_job(key: &str, state: EcPromotionState) -> EcPromotionJobRecord {
        EcPromotionJobRecord {
            object: EcObjectVersionKey::new("bucket", key, None),
            operation_id: Uuid::from_u128(9),
            source_hlc: hlc(node(1)),
            erasure_set_id: 1,
            state,
            attempt: 0,
            object_size: 4096,
            content_hash_sha256: [5; 32],
            origin: EcPromotionOrigin::Automatic,
            shards_eligible_at: None,
            updated_at: 1_000,
            next_retry_at: None,
            failure: None,
        }
    }

    fn node(index: u8) -> NodeId {
        NodeId(u128::from(index))
    }

    fn nodes(count: u8) -> Vec<NodeId> {
        (1..=count).map(node).collect()
    }

    fn hlc(node_id: NodeId) -> HlcTimestamp {
        HlcTimestamp {
            wall_time: 1000,
            logical: 1,
            node_id: node_id.0,
        }
    }

    fn test_operation_id() -> Uuid {
        Uuid::from_u128(0x1234)
    }

    fn shard_bytes(slot_index: u8) -> Vec<u8> {
        vec![slot_index; 8]
    }

    fn manifest(version_id: Option<String>) -> EcObjectManifest {
        let profile = EcProfileSnapshot::rs_small();
        let shards = (0..profile.total_shards())
            .map(|slot_index| {
                let bytes = shard_bytes(slot_index);
                ShardManifest {
                    slot_index,
                    node_id: node(slot_index + 1),
                    role: profile.role_for_slot(slot_index).unwrap(),
                    crc32: crc32fast::hash(&bytes),
                    sha256: sha256(&bytes),
                    size: bytes.len() as u64,
                }
            })
            .collect();

        EcObjectManifest {
            format_version: EC_MANIFEST_FORMAT_VERSION,
            bucket: "photos".to_string(),
            key: "2026/raw.bin".to_string(),
            version_id,
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
            hlc: hlc(node(1)),
            origin_node_id: node(1),
            operation_id: test_operation_id(),
            created_at: 42,
            is_deleted: false,
            packed: None,
        }
    }

    /// The shape a build without packed segments wrote transcode jobs in.
    #[derive(Serialize)]
    struct LegacyTranscodeJobWithoutTarget {
        object: EcObjectVersionKey,
        operation_id: Uuid,
        source_hlc: HlcTimestamp,
        target_erasure_set_id: u64,
        state: EcPlacementState,
        attempt: u32,
        object_size: u64,
        source_content_hash_sha256: [u8; 32],
        updated_at: u64,
        next_retry_at: Option<u64>,
        failure: Option<EcTranscodeFailureRecord>,
        origin: EcTranscodeOrigin,
    }

    /// One stale queue record must not take the node's transcoding down with it.
    ///
    /// bincode reads fields by position, so a record written before `target`
    /// existed cannot be decoded at all. If the listing failed on it, every
    /// transcode on the node would stop after an upgrade until somebody found
    /// the record by hand — while the objects it names are perfectly safe on
    /// their RF=3 replicas and would be re-queued by the next write or scan.
    #[tokio::test]
    async fn an_unreadable_transcode_job_is_skipped_rather_than_fatal() {
        let dir = tempfile::tempdir().unwrap();
        let store = EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap();

        let readable = transcode_job(
            EcObjectVersionKey::new("bucket", "readable", None),
            Uuid::from_u128(1),
        );
        store.put_transcode_job(&readable).await.unwrap();

        let stale_operation_id = Uuid::from_u128(2);
        let stale = LegacyTranscodeJobWithoutTarget {
            object: EcObjectVersionKey::new("bucket", "stale", None),
            operation_id: stale_operation_id,
            source_hlc: hlc(node(1)),
            target_erasure_set_id: 9,
            state: EcPlacementState::EcTranscodeQueued,
            attempt: 0,
            object_size: 16,
            source_content_hash_sha256: [9; 32],
            updated_at: 42,
            next_retry_at: None,
            failure: None,
            origin: EcTranscodeOrigin::ClientWrite,
        };
        store
            .transcode_jobs
            .insert(
                &serialize(&stale_operation_id).unwrap()[..],
                &serialize(&stale).unwrap()[..],
            )
            .unwrap();

        let listed = store.list_transcode_jobs(32).await.unwrap();
        assert_eq!(
            listed.len(),
            1,
            "the readable job must survive the unreadable one"
        );
        assert_eq!(listed[0].operation_id, readable.operation_id);
    }

    fn transcode_job(object: EcObjectVersionKey, operation_id: Uuid) -> EcTranscodeJobRecord {
        EcTranscodeJobRecord {
            object,
            operation_id,
            source_hlc: hlc(node(1)),
            target_erasure_set_id: 9,
            state: EcPlacementState::EcTranscodeQueued,
            attempt: 0,
            object_size: 16,
            source_content_hash_sha256: [9; 32],
            updated_at: 42,
            next_retry_at: None,
            failure: None,
            origin: EcTranscodeOrigin::ClientWrite,
            target: EcTranscodeTarget::Object,
        }
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

    #[test]
    fn replicated_small_requires_explicit_rf3_nodes() {
        let placement = ReplicatedSmallObjectPlacement::new(
            vec![node(1), node(2), node(3)],
            [4; 32],
            128,
            hlc(node(1)),
        )
        .unwrap();
        assert_eq!(placement.replica_nodes.len(), 3);

        let err = ReplicatedSmallObjectPlacement::new(
            vec![node(1), node(1), node(3)],
            [4; 32],
            128,
            hlc(node(1)),
        )
        .unwrap_err();
        assert!(err.to_string().contains("duplicate node"));
    }

    #[test]
    fn orphan_shard_record_accepts_zero_erasure_set_for_pool_zero() {
        let operation_id = test_operation_id();
        let record = EcOrphanShardRecord {
            operation_id,
            source_object: EcObjectVersionKey::new("bucket", "key", None),
            target_erasure_set_id: 0,
            chunk_index: 0,
            slot_index: 0,
            local_shard: LocalShardRecord {
                volume_id: 1,
                offset: 0,
                size: 1,
                crc32: 0,
                sha256: [1; 32],
                state: LocalShardState::Durable,
                operation_id,
            },
            created_at: 1,
            lease_epoch: 0,
            last_lease_seen_at: None,
            state: EcOrphanShardState::Candidate,
            reason: "pool zero erasure set".to_string(),
        };

        record.validate().unwrap();
    }

    #[test]
    fn manifest_storage_hlc_advances_across_state_transitions() {
        let base = hlc(node(1));
        let committed = manifest_storage_hlc(base, EcPlacementState::EcManifestCommitted);
        let read_verified = manifest_storage_hlc(base, EcPlacementState::EcReadVerified);
        let ec_committed = manifest_storage_hlc(base, EcPlacementState::EcCommitted);

        assert!(committed.logical < read_verified.logical);
        assert!(read_verified.logical < ec_committed.logical);
        assert_eq!(committed.wall_time, base.wall_time);
    }

    #[test]
    fn manifest_contains_per_shard_crc32_and_sha256() {
        let manifest = manifest(None);
        manifest.validate().unwrap();
        let shard = &manifest.chunks[0].shards[2];
        shard.verify_bytes(&shard_bytes(2)).unwrap();
        assert!(shard.verify_bytes(b"wrong").is_err());
    }

    #[test]
    fn manifest_validates_against_erasure_set_slot_assignment() {
        let manifest = manifest(None);
        manifest.validate_against_erasure_set(&active_set()).unwrap();
    }

    // -----------------------------------------------------------------------
    // The manifest is the source of truth about its own placement (EC2-086)
    // -----------------------------------------------------------------------

    /// A one-chunk manifest whose shards sit where `set` puts each slot today.
    fn manifest_placed_by(set: &ErasureSet) -> EcObjectManifest {
        let profile = set.profile.codec_snapshot();
        let shards = set
            .slot_assignment
            .slots()
            .iter()
            .map(|slot| {
                let bytes = shard_bytes(slot.slot_index);
                ShardManifest {
                    slot_index: slot.slot_index,
                    node_id: slot.node_id,
                    role: profile.role_for_slot(slot.slot_index).unwrap(),
                    crc32: crc32fast::hash(&bytes),
                    sha256: sha256(&bytes),
                    size: bytes.len() as u64,
                }
            })
            .collect();
        let mut manifest = manifest(None);
        manifest.profile = profile;
        manifest.erasure_set_id = set.set_id;
        manifest.topology_epoch = set.topology_epoch;
        manifest.chunks[0].shards = shards;
        manifest.validate().unwrap();
        manifest
    }

    fn rack_labels(racks: &[(u8, &str)]) -> HashMap<NodeId, TopologyLabels> {
        racks
            .iter()
            .map(|(index, rack)| {
                (
                    node(*index),
                    TopologyLabels {
                        zone: Some("z1".to_string()),
                        rack: Some((*rack).to_string()),
                        host: Some(format!("h{index}")),
                        disk_group: Some("d0".to_string()),
                    },
                )
            })
            .collect()
    }

    fn set_with_labels(profile: EcProfile, labels: &HashMap<NodeId, TopologyLabels>) -> ErasureSet {
        let set_nodes = nodes(profile.total_shards());
        ErasureSet::inherited(
            s4_cluster::PoolId::new(9),
            profile,
            &set_nodes,
            labels,
            EcTopologyPolicy::Warn,
            3,
            1,
        )
        .unwrap()
    }

    fn slots_moved(before: &ErasureSet, after: &ErasureSet) -> usize {
        before
            .slot_assignment
            .slot_order()
            .iter()
            .zip(after.slot_assignment.slot_order())
            .filter(|(was, now)| **was != *now)
            .count()
    }

    /// Regression tests for the defect EC2-086 found: a label change reorders
    /// the slots of a set that has no layout record, and every manifest written
    /// before it used to be refused slot by slot. The two scenarios of that
    /// decision — labels added to an unlabeled pool, and one node moved to
    /// another rack — for an RS and an LRC profile.
    #[test]
    fn a_manifest_written_before_a_label_change_still_validates() {
        let rs_racks = rack_labels(&[
            (1, "r0"),
            (2, "r0"),
            (3, "r0"),
            (4, "r1"),
            (5, "r1"),
            (6, "r1"),
            (7, "r2"),
            (8, "r2"),
            (9, "r2"),
        ]);
        let mut rs_node_moved = rs_racks.clone();
        rs_node_moved.get_mut(&node(1)).unwrap().rack = Some("r1".to_string());

        let lrc_racks = rack_labels(&[
            (1, "r0"),
            (2, "r0"),
            (3, "r0"),
            (4, "r1"),
            (5, "r1"),
            (6, "r1"),
            (7, "r2"),
            (8, "r2"),
            (9, "r3"),
            (10, "r3"),
        ]);
        let mut lrc_node_moved = lrc_racks.clone();
        lrc_node_moved.get_mut(&node(2)).unwrap().rack = Some("r3".to_string());

        let scenarios = [
            (
                "rs_standard: labels added to an unlabeled pool",
                EcProfile::rs_standard(),
                HashMap::new(),
                rs_racks.clone(),
                6,
            ),
            (
                "rs_standard: one node moved to another rack",
                EcProfile::rs_standard(),
                rs_racks,
                rs_node_moved,
                7,
            ),
            (
                "lrc_standard: labels added to an unlabeled pool",
                EcProfile::lrc_standard(),
                HashMap::new(),
                lrc_racks.clone(),
                9,
            ),
            (
                "lrc_standard: one node moved to another rack",
                EcProfile::lrc_standard(),
                lrc_racks,
                lrc_node_moved,
                5,
            ),
        ];

        for (scenario, profile, before, after, expected_moves) in scenarios {
            let written_under = set_with_labels(profile.clone(), &before);
            let read_under = set_with_labels(profile, &after);
            assert_eq!(
                slots_moved(&written_under, &read_under),
                expected_moves,
                "{scenario}: the label change must really move slots"
            );

            let manifest = manifest_placed_by(&written_under);
            manifest
                .validate_against_erasure_set(&read_under)
                .unwrap_or_else(|error| panic!("{scenario}: {error}"));
        }
    }

    /// Membership is still checked: a shard on a node the set does not have is
    /// refused, whatever its slot.
    #[test]
    fn a_manifest_naming_a_node_outside_its_set_is_refused() {
        let set = active_set();
        let mut manifest = manifest_placed_by(&set);
        manifest.chunks[0].shards[3].node_id = node(200);

        let error = manifest.validate_against_erasure_set(&set).unwrap_err();

        assert!(
            error.to_string().contains("not a node of erasure set"),
            "{error}"
        );
    }

    /// A node of another set of the same pool is outside this set too. The
    /// sets of a pool never share a node, so membership keeps a manifest from
    /// sending a read into another set.
    #[test]
    fn a_manifest_naming_a_node_of_another_set_of_the_pool_is_refused() {
        let pool_id = s4_cluster::PoolId::new(9);
        let own = active_set();
        let other_nodes = (7..=12).map(node).collect::<Vec<_>>();
        let other = ErasureSet::active(
            pool_id,
            2,
            own.profile.clone(),
            &other_nodes,
            &HashMap::<NodeId, TopologyLabels>::new(),
            EcTopologyPolicy::Strict,
            3,
            1,
        )
        .unwrap();
        let registry = ErasureSetRegistry::from_sets(pool_id, vec![own.clone(), other]).unwrap();

        let mut manifest = manifest_placed_by(&own);
        manifest.chunks[0].shards[0].node_id = other_nodes[0];
        let resolved = registry.resolve_manifest_set(manifest.erasure_set_id).unwrap();
        let error = manifest.validate_against_erasure_set(resolved).unwrap_err();

        assert!(
            error.to_string().contains("not a node of erasure set"),
            "{error}"
        );
    }

    /// Two shards of one chunk on one node would both be lost with it. The
    /// slot check used to rule this out implicitly; membership alone does not,
    /// so it is checked explicitly.
    #[test]
    fn a_manifest_placing_two_shards_of_a_chunk_on_one_node_is_refused() {
        let set = active_set();
        let mut manifest = manifest_placed_by(&set);
        manifest.chunks[0].shards[1].node_id = manifest.chunks[0].shards[0].node_id;

        let error = manifest.validate_against_erasure_set(&set).unwrap_err();

        assert!(error.to_string().contains("more than one shard"), "{error}");
    }

    // -----------------------------------------------------------------------
    // Roster and layout record encodings (EC2-084, `EC2-ZONE-01-006`)
    // -----------------------------------------------------------------------

    fn reference_roster() -> EcErasureSetRoster {
        EcErasureSetRoster {
            pool_id: 7,
            sets: vec![EcErasureSetSpec {
                set_id: 1,
                node_ids: vec![NodeId(0x11), NodeId(0x12)],
                state: ErasureSetState::ReadOnly,
                created_at: 0x0102,
            }],
            inherited_state: ErasureSetState::Active,
            inherited_node_ids: vec![NodeId(0x21)],
            hlc: HlcTimestamp {
                wall_time: 0x0a0b,
                logical: 3,
                node_id: 0x21,
            },
            updated_at: 0x0c0d,
        }
    }

    /// The bincode encoding of [`reference_roster`], written out by hand from
    /// the field layout rather than produced by the serializer, so a change to
    /// the roster's fields cannot move both sides of the comparison at once.
    #[rustfmt::skip]
    const REFERENCE_ROSTER_BYTES: [u8; 136] = [
        // pool_id: 7
        0x07, 0x00, 0x00, 0x00,
        // sets: one spec
        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        //   set_id: 1
        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        //   node_ids: two nodes
        0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        //     NodeId(0x11)
        0x11, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        //     NodeId(0x12)
        0x12, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        //   state: ReadOnly (variant 2)
        0x02, 0x00, 0x00, 0x00,
        //   created_at: 0x0102
        0x02, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // inherited_state: Active (variant 0)
        0x00, 0x00, 0x00, 0x00,
        // inherited_node_ids: one node
        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        //   NodeId(0x21)
        0x21, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // hlc.wall_time: 0x0a0b
        0x0b, 0x0a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // hlc.logical: 3
        0x03, 0x00, 0x00, 0x00,
        // hlc.node_id: 0x21
        0x21, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // updated_at: 0x0c0d
        0x0d, 0x0c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    ];

    /// The roster is encoded byte for byte as before plan 05. Layout records
    /// went into a record of their own precisely so that this would hold: a
    /// field added here would make every roster already written unreadable.
    #[test]
    fn the_roster_encoding_matches_its_reference_bytes() {
        assert_eq!(
            bincode::serialize(&reference_roster()).unwrap(),
            REFERENCE_ROSTER_BYTES
        );
    }

    /// A roster written before this build decodes to the same roster, through
    /// the same path the node reads its cache with.
    #[tokio::test]
    async fn a_roster_written_before_this_build_still_reads() {
        let decoded: EcErasureSetRoster = deserialize(&REFERENCE_ROSTER_BYTES).unwrap();
        assert_eq!(decoded, reference_roster());

        let dir = tempfile::tempdir().unwrap();
        let store = EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap();
        store
            .erasure_sets
            .insert(ERASURE_SET_ROSTER_KEY, &REFERENCE_ROSTER_BYTES[..])
            .unwrap();
        assert_eq!(
            store.get_erasure_set_roster().await.unwrap(),
            Some(reference_roster())
        );
    }

    fn reference_layout_record() -> EcSetLayoutRecord {
        EcSetLayoutRecord {
            format_version: EC_SET_LAYOUT_RECORD_FORMAT_VERSION,
            pool_id: 7,
            set_id: 1,
            rule: EcSetLayoutRule::LrcGroups {
                level: EcFailureDomainLevel::Rack,
            },
            policy: EcTopologyPolicy::Warn,
            node_ids: vec![NodeId(0x12), NodeId(0x11)],
            violation: Some(EcLayoutViolation {
                reason: EcLayoutViolationReason::TooFewDomains,
                detail: "ab".to_string(),
            }),
            algorithm_version: 2,
            created_at: 0x0102,
            hlc: HlcTimestamp {
                wall_time: 0x0a0b,
                logical: 3,
                node_id: 0x21,
            },
        }
    }

    /// The bincode encoding of [`reference_layout_record`], written out by hand.
    #[rustfmt::skip]
    const REFERENCE_LAYOUT_RECORD_BYTES: [u8; 123] = [
        // format_version: 1
        0x01, 0x00, 0x00, 0x00,
        // pool_id: 7
        0x07, 0x00, 0x00, 0x00,
        // set_id: 1
        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // rule: LrcGroups (variant 1)
        0x01, 0x00, 0x00, 0x00,
        //   level: Rack (variant 1)
        0x01, 0x00, 0x00, 0x00,
        // policy: Warn (variant 1)
        0x01, 0x00, 0x00, 0x00,
        // node_ids: two nodes
        0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        //   NodeId(0x12)
        0x12, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        //   NodeId(0x11)
        0x11, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // violation: Some
        0x01,
        //   reason: TooFewDomains (variant 0)
        0x00, 0x00, 0x00, 0x00,
        //   detail: "ab"
        0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x61, 0x62,
        // algorithm_version: 2
        0x02, 0x00, 0x00, 0x00,
        // created_at: 0x0102
        0x02, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // hlc.wall_time: 0x0a0b
        0x0b, 0x0a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // hlc.logical: 3
        0x03, 0x00, 0x00, 0x00,
        // hlc.node_id: 0x21
        0x21, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    ];

    /// The layout record's format is pinned from its first day, variant
    /// indices included: other nodes, and later builds, read what this one
    /// writes.
    #[test]
    fn the_layout_record_encoding_matches_its_reference_bytes() {
        let record = reference_layout_record();
        record.validate().unwrap();
        assert_eq!(
            bincode::serialize(&record).unwrap(),
            REFERENCE_LAYOUT_RECORD_BYTES
        );
        let decoded: EcSetLayoutRecord = deserialize(&REFERENCE_LAYOUT_RECORD_BYTES).unwrap();
        assert_eq!(decoded, record);
    }

    /// The record of an EC v1 set holds that set's slot order, and survives
    /// the cluster encoding and the local cache unchanged.
    #[tokio::test]
    async fn a_layout_record_round_trips_through_the_local_cache() {
        let set = active_set();
        let record = EcSetLayoutRecord::v1(&set, EcTopologyPolicy::Strict, 5, hlc(node(1)));
        assert_eq!(record.node_ids, set.slot_assignment.slot_order());
        assert_eq!(record.rule, EcSetLayoutRule::V1);
        assert_eq!(record.violation, None);
        assert_eq!(record.algorithm_version, EC_V1_LAYOUT_ALGORITHM_VERSION);
        assert_eq!(
            deserialize::<EcSetLayoutRecord>(&serialize(&record).unwrap()).unwrap(),
            record
        );

        let dir = tempfile::tempdir().unwrap();
        let store = EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap();
        assert_eq!(store.get_set_layout_record(set.set_id).await.unwrap(), None);
        store.put_erasure_set_roster(&reference_roster()).await.unwrap();
        store.put_set_layout_record(&record).await.unwrap();

        assert_eq!(
            store.get_set_layout_record(set.set_id).await.unwrap(),
            Some(record)
        );
        assert_eq!(
            store.get_set_layout_record(set.set_id + 1).await.unwrap(),
            None
        );
        // The records share the roster's keyspace without disturbing it.
        assert_eq!(
            store.get_erasure_set_roster().await.unwrap(),
            Some(reference_roster())
        );
    }

    #[test]
    fn a_layout_record_that_cannot_describe_a_set_is_refused() {
        let valid = EcSetLayoutRecord::v1(&active_set(), EcTopologyPolicy::Strict, 5, hlc(node(1)));
        valid.validate().unwrap();

        let mut future_format = valid.clone();
        future_format.format_version = EC_SET_LAYOUT_RECORD_FORMAT_VERSION + 1;
        let mut no_nodes = valid.clone();
        no_nodes.node_ids.clear();
        let mut repeated_node = valid.clone();
        repeated_node.node_ids[1] = repeated_node.node_ids[0];
        let mut v1_with_violation = valid.clone();
        v1_with_violation.violation = Some(EcLayoutViolation {
            reason: EcLayoutViolationReason::DomainOverloaded,
            detail: "overloaded".to_string(),
        });

        for (case, record, expected) in [
            ("future format", future_format, "unsupported format_version"),
            ("no nodes", no_nodes, "names no nodes"),
            ("repeated node", repeated_node, "twice"),
            (
                "EC v1 with a violation",
                v1_with_violation,
                "no rule to break",
            ),
        ] {
            let error = record.validate().unwrap_err();
            assert!(
                matches!(error, EcMetadataError::InvalidSetLayout { .. }),
                "{case}: {error:?}"
            );
            assert!(error.to_string().contains(expected), "{case}: {error}");
        }
    }

    /// The pool's inherited set as a v2 node builds it via `ErasureSet::inherited`.
    /// Profile, nodes and epoch match `active_set()`; its `set_id` is
    /// `legacy_pool_set_id(pool_id)` — the exact value EC v1 wrote — so a manifest
    /// written before the decoupling still resolves and validates here.
    fn inherited_set() -> ErasureSet {
        let profile = EcProfile {
            name: "ec-rs-small".to_string(),
            codec: EcCodecKind::ReedSolomon,
            k: 4,
            parity: EcParity::ReedSolomon { m: 2 },
            min_object_size: DEFAULT_MIN_OBJECT_SIZE,
            chunk_size: DEFAULT_CHUNK_SIZE,
        };
        let nodes = nodes(profile.total_shards());
        ErasureSet::inherited(
            s4_cluster::PoolId::new(9),
            profile,
            &nodes,
            &HashMap::<NodeId, TopologyLabels>::new(),
            EcTopologyPolicy::Strict,
            3,
            1,
        )
        .unwrap()
    }

    /// A pool holding the inherited set plus one added later.
    fn two_set_registry() -> ErasureSetRegistry {
        let pool_id = s4_cluster::PoolId::new(9);
        let profile = EcProfile {
            name: "ec-rs-small".to_string(),
            codec: EcCodecKind::ReedSolomon,
            k: 4,
            parity: EcParity::ReedSolomon { m: 2 },
            min_object_size: DEFAULT_MIN_OBJECT_SIZE,
            chunk_size: DEFAULT_CHUNK_SIZE,
        };
        let nodes = nodes(profile.total_shards());
        let added = ErasureSet::active(
            pool_id,
            2,
            profile,
            &nodes,
            &HashMap::<NodeId, TopologyLabels>::new(),
            EcTopologyPolicy::Strict,
            3,
            1,
        )
        .unwrap();

        ErasureSetRegistry::from_sets(pool_id, vec![inherited_set(), added]).unwrap()
    }

    /// An object written before `set_id` was decoupled from `pool_id` names the
    /// pool in its manifest. It must still resolve — and still validate — once
    /// the inherited set carries an issued id instead. This is the compatibility
    /// guarantee of EC2-041, and phase 1 is not allowed to close without it.
    #[test]
    fn manifest_written_before_decoupling_resolves_to_inherited_set() {
        let manifest = manifest(None);
        let pool_id = s4_cluster::PoolId::new(9);
        // The fixture carries the value EC v1 derived from the pool.
        assert_eq!(manifest.erasure_set_id, legacy_pool_set_id(pool_id));

        let registry = two_set_registry();
        let inherited = registry.inherited_set().unwrap();
        // The inherited set carries exactly the EC v1 value — that is what keeps
        // v1 binaries and mixed clusters able to read it.
        assert_eq!(inherited.set_id, legacy_pool_set_id(pool_id));
        assert_eq!(inherited.set_id, manifest.erasure_set_id);

        let resolved = registry
            .resolve_manifest_set(manifest.erasure_set_id)
            .expect("a manifest written by EC v1 must still resolve after the decoupling");
        assert_eq!(resolved.set_id, legacy_pool_set_id(pool_id));
        assert!(resolved.inherited);

        manifest.validate_against_erasure_set(resolved).unwrap();
    }

    /// The rule above must be able to fail, or it proves nothing. A manifest
    /// naming a set this pool never issued is refused rather than quietly served
    /// from the inherited set — serving it would read the wrong shards.
    #[test]
    fn manifest_naming_unknown_set_is_not_served_from_inherited_set() {
        let mut manifest = manifest(None);
        manifest.erasure_set_id = 77;

        let registry = two_set_registry();
        assert!(registry.resolve_manifest_set(77).is_none());

        let err = manifest
            .validate_against_erasure_set(registry.inherited_set().unwrap())
            .unwrap_err();
        assert!(
            err.to_string().contains("does not belong"),
            "unexpected error: {err}"
        );
    }

    /// A manifest belonging to the added set must not validate against the
    /// inherited one: the two sets are different placements even while they share
    /// nodes and profile.
    #[test]
    fn manifest_of_added_set_does_not_validate_against_inherited_set() {
        let mut manifest = manifest(None);
        manifest.erasure_set_id = 2;

        let registry = two_set_registry();
        let resolved = registry.resolve_manifest_set(2).unwrap();
        assert_eq!(resolved.set_id, 2);
        manifest.validate_against_erasure_set(resolved).unwrap();

        assert!(manifest
            .validate_against_erasure_set(registry.inherited_set().unwrap())
            .is_err());
    }

    #[test]
    fn manifest_rejects_wrong_shard_role() {
        let mut manifest = manifest(None);
        manifest.chunks[0].shards[0].role = ShardRole::ReedSolomonParity;
        let err = manifest.validate().unwrap_err();
        assert!(err.to_string().contains("expected Data"));
    }

    #[test]
    fn manifest_rejects_encoded_capacity_smaller_than_data_size() {
        let mut manifest = manifest(None);
        manifest.chunks[0].encoded_size = 1;
        for shard in &mut manifest.chunks[0].shards {
            shard.size = 1;
        }

        let err = manifest.validate().unwrap_err();
        assert!(err.to_string().contains("encoded capacity"));
    }

    #[test]
    fn manifest_hash_uses_canonical_user_metadata_order() {
        let mut first = manifest(None);
        first.user_metadata.insert("z-key".to_string(), "z".to_string());
        first.user_metadata.insert("a-key".to_string(), "a".to_string());

        let mut second = manifest(None);
        second.user_metadata.insert("a-key".to_string(), "a".to_string());
        second.user_metadata.insert("z-key".to_string(), "z".to_string());

        assert_eq!(
            first.serialized_sha256().unwrap(),
            second.serialized_sha256().unwrap()
        );
        let reference = EcObjectManifestRef::from_manifest(&first).unwrap();
        assert!(reference.matches_manifest(&second).unwrap());
    }

    #[test]
    fn local_shard_record_is_not_global_manifest_location() {
        let manifest = manifest(Some("v1".to_string()));
        let shard_manifest = &manifest.chunks[0].shards[0];
        let local = LocalShardRecord {
            volume_id: 77,
            offset: 4096,
            size: shard_manifest.size,
            crc32: shard_manifest.crc32,
            sha256: shard_manifest.sha256,
            state: LocalShardState::Durable,
            operation_id: test_operation_id(),
        };

        local.verify_against_manifest(shard_manifest).unwrap();
        assert_eq!(local.volume_id, 77);
        assert_eq!(shard_manifest.node_id, node(1));
    }

    #[tokio::test]
    async fn store_keeps_versioned_manifests_and_local_shards_separate() {
        let dir = tempfile::tempdir().unwrap();
        let store = EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap();

        let m1 = manifest(Some("v1".to_string()));
        let mut m2 = manifest(Some("v2".to_string()));
        m2.content_hash_sha256 = [9; 32];
        store.put_manifest(&m1).await.unwrap();
        store.put_manifest(&m2).await.unwrap();

        let key_v1 = m1.object_key();
        let key_v2 = m2.object_key();
        assert_eq!(
            store.get_manifest(&key_v1).await.unwrap().unwrap().content_hash_sha256,
            [7; 32]
        );
        assert_eq!(
            store.get_manifest(&key_v2).await.unwrap().unwrap().content_hash_sha256,
            [9; 32]
        );

        let shard_key_v1 =
            LocalShardKey::new("photos", "2026/raw.bin", Some("v1".to_string()), 0, 0);
        let shard_key_v2 =
            LocalShardKey::new("photos", "2026/raw.bin", Some("v2".to_string()), 0, 0);
        let record = LocalShardRecord {
            volume_id: 1,
            offset: 10,
            size: 8,
            crc32: 1,
            sha256: [1; 32],
            state: LocalShardState::Durable,
            operation_id: test_operation_id(),
        };
        store.put_local_shard(&shard_key_v1, &record).await.unwrap();
        assert!(store.get_local_shard(&shard_key_v2).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn local_shard_listing_honors_zero_limit() {
        let dir = tempfile::tempdir().unwrap();
        let store = EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap();
        let object = EcObjectVersionKey::new("photos", "2026/raw.bin", Some("v1".to_string()));
        let shard_key = LocalShardKey {
            object: object.clone(),
            chunk_index: 0,
            slot_index: 0,
        };
        let record = LocalShardRecord {
            volume_id: 1,
            offset: 10,
            size: 8,
            crc32: 1,
            sha256: [1; 32],
            state: LocalShardState::Durable,
            operation_id: test_operation_id(),
        };

        store.put_local_shard(&shard_key, &record).await.unwrap();
        assert!(store.list_local_shards_for_object(&object, 0).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn delete_transcode_jobs_for_object_paginates_past_first_page() {
        let dir = tempfile::tempdir().unwrap();
        let store = EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap();

        for index in 0..8u128 {
            let object = EcObjectVersionKey::new("photos", format!("job-{index}.bin"), None);
            let job = transcode_job(object, Uuid::from_u128(index + 1));
            store.put_transcode_job(&job).await.unwrap();
        }

        let ordered = store.list_transcode_jobs(32).await.unwrap();
        assert!(ordered.len() > 1);
        let first_object = ordered.first().unwrap().object.clone();
        let target_job = ordered.last().unwrap().clone();
        assert_ne!(target_job.object, first_object);

        let removed = store.delete_transcode_jobs_for_object(&target_job.object, 1).await.unwrap();

        assert_eq!(removed.total, 1);
        // Every job in this fixture came from a client write.
        assert_eq!(removed.tiering, 0);
        assert!(store.get_transcode_job(target_job.operation_id).await.unwrap().is_none());
        for job in ordered.iter().filter(|job| job.operation_id != target_job.operation_id) {
            assert!(store.get_transcode_job(job.operation_id).await.unwrap().is_some());
        }
    }

    #[tokio::test]
    async fn hot_replica_gc_listing_paginates_past_retained_records() {
        let dir = tempfile::tempdir().unwrap();
        let store = EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap();

        for index in 0..4u128 {
            let record = EcHotReplicaGcRecord {
                object: EcObjectVersionKey::new("photos", format!("object-{index}.bin"), None),
                operation_id: Uuid::from_u128(index + 1),
                source_hlc: hlc(node(1)),
                eligible_at: 100,
                full_object_sha256_verified: true,
                purged_at: (index < 2).then_some(200),
                updated_at: 200,
            };
            store.put_hot_replica_gc(&record).await.unwrap();
        }

        let first = store.list_hot_replica_gc_page(None, 2).await.unwrap();
        assert_eq!(first.entries.len(), 2);
        assert!(first.entries.iter().all(|record| record.purged_at.is_some()));

        let second = store.list_hot_replica_gc_page(first.next_cursor, 2).await.unwrap();
        assert_eq!(second.entries.len(), 2);
        assert!(second.entries.iter().all(|record| record.purged_at.is_none()));
    }

    #[tokio::test]
    async fn live_transcode_lease_is_exclusive_even_for_same_owner() {
        let dir = tempfile::tempdir().unwrap();
        let store = EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap();
        let operation_id = test_operation_id();
        let owner = node(1);

        assert!(store.try_acquire_transcode_lease(operation_id, owner, 100, 100).await.unwrap());
        assert!(!store.try_acquire_transcode_lease(operation_id, owner, 101, 100).await.unwrap());
        assert!(store.renew_transcode_lease(operation_id, owner, 150, 100).await.unwrap());
        assert!(!store
            .try_acquire_transcode_lease(operation_id, node(2), 151, 100)
            .await
            .unwrap());
        assert!(store.try_acquire_transcode_lease(operation_id, owner, 300, 100).await.unwrap());
    }

    #[tokio::test]
    async fn local_metadata_miss_triggers_quorum_read_and_caches_manifest() {
        let dir = tempfile::tempdir().unwrap();
        let store = EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap();
        let object = EcObjectVersionKey::new("photos", "2026/raw.bin", None);
        let quorum_calls = AtomicUsize::new(0);

        let result = store
            .resolve_manifest_with_quorum(
                object.clone(),
                |_| true,
                |requested| {
                    quorum_calls.fetch_add(1, Ordering::SeqCst);
                    async move {
                        assert_eq!(requested, object);
                        Ok(EcManifestReadResult::Present(manifest(None)))
                    }
                },
            )
            .await
            .unwrap();

        assert!(matches!(result, EcManifestReadResult::Present(_)));
        assert_eq!(quorum_calls.load(Ordering::SeqCst), 1);
        assert!(store
            .get_manifest(&EcObjectVersionKey::new("photos", "2026/raw.bin", None))
            .await
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn current_local_manifest_does_not_call_quorum_read() {
        let dir = tempfile::tempdir().unwrap();
        let store = EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap();
        let object = EcObjectVersionKey::new("photos", "2026/raw.bin", None);
        store.put_manifest(&manifest(None)).await.unwrap();
        let quorum_calls = AtomicUsize::new(0);

        let result = store
            .resolve_manifest_with_quorum(
                object,
                |_| true,
                |_| {
                    quorum_calls.fetch_add(1, Ordering::SeqCst);
                    async { Ok(EcManifestReadResult::Absent) }
                },
            )
            .await
            .unwrap();

        assert!(matches!(result, EcManifestReadResult::Present(_)));
        assert_eq!(quorum_calls.load(Ordering::SeqCst), 0);
    }

    /// `ALL` is what recovers the state a reference was taken at, so a state
    /// missing from it would make that reference look like other work.
    #[test]
    fn every_placement_state_is_listed_once() {
        // Exhaustive on purpose: a new variant does not compile here until it
        // is given a place in `ALL`.
        fn position(state: EcPlacementState) -> usize {
            match state {
                EcPlacementState::HotReplicated => 0,
                EcPlacementState::EcTranscodeQueued => 1,
                EcPlacementState::EcEncoding => 2,
                EcPlacementState::EcShardsDurable => 3,
                EcPlacementState::EcManifestCommitted => 4,
                EcPlacementState::EcReadVerified => 5,
                EcPlacementState::EcCommitted => 6,
                EcPlacementState::HotReplicasGcEligible => 7,
                EcPlacementState::HotReplicasPurged => 8,
                EcPlacementState::EcFailedRetryable => 9,
                EcPlacementState::EcFailedPermanent => 10,
                EcPlacementState::Promoted => 11,
                EcPlacementState::Tombstone => 12,
                EcPlacementState::Promoting => 13,
            }
        }
        for (index, state) in EcPlacementState::ALL.into_iter().enumerate() {
            assert_eq!(position(state), index, "{state:?} is out of place in ALL");
        }
    }

    /// Adding the promotion marker moved no state already on disk.
    ///
    /// bincode writes a variant as its position, so a state inserted anywhere
    /// but at the end would make every manifest stored with a later state decode
    /// as a different one — a tombstone as the marker, say.
    #[test]
    fn existing_states_keep_the_bincode_position_they_were_written_with() {
        for (index, state) in EcPlacementState::ALL.into_iter().enumerate() {
            let bytes = serialize(&state).unwrap();
            assert_eq!(bytes, (index as u32).to_le_bytes(), "{state:?}");
            assert_eq!(deserialize::<EcPlacementState>(&bytes).unwrap(), state);
        }
        assert_eq!(
            serialize(&EcPlacementState::Tombstone).unwrap(),
            12u32.to_le_bytes()
        );
        assert_eq!(
            serialize(&EcPlacementState::Promoting).unwrap(),
            13u32.to_le_bytes()
        );
        assert_eq!(EcPlacementState::Promoting.as_str(), "promoting");
    }

    /// The marker outranks the manifest it replaces and is outranked by the one
    /// that replaces it, at the same HLC, in the stored hidden object too.
    ///
    /// The storage order is what decides which write of the same generation
    /// lands; if the marker ranked below the purged manifest it would never
    /// land at all, and reconciliation would never see it (EC2-026).
    #[test]
    fn the_promotion_marker_sits_between_the_purged_and_the_promoted_manifest() {
        let base = HlcTimestamp::from_record(1_000, 0, 1);
        let purged = manifest_storage_hlc(base, EcPlacementState::HotReplicasPurged);
        let promoting = manifest_storage_hlc(base, EcPlacementState::Promoting);
        let promoted = manifest_storage_hlc(base, EcPlacementState::Promoted);
        let tombstone = manifest_storage_hlc(base, EcPlacementState::Tombstone);
        assert!(purged < promoting);
        assert!(promoting < promoted);
        assert!(promoted < tombstone);
    }

    /// A reader that cached the purged manifest keeps reading through erasure
    /// coding once the marker is committed.
    ///
    /// The marker changes nothing but the state, so the cached reference is
    /// recognised as an earlier state of it. Had it moved `created_at` the way
    /// the promoted manifest does, every such reader would fail with "did not
    /// win quorum" for the whole of the promotion.
    #[test]
    fn a_reference_taken_at_the_purge_is_an_earlier_state_of_the_promotion_marker() {
        let mut purged = manifest(None);
        purged.state = EcPlacementState::HotReplicasPurged;
        let reference = EcObjectManifestRef::from_manifest(&purged).unwrap();
        let mut promoting = purged.clone();
        promoting.state = EcPlacementState::Promoting;

        assert!(!reference.matches_manifest(&promoting).unwrap());
        assert!(reference.references_earlier_state_of(&promoting).unwrap());
    }

    /// Every path that asks whether a manifest describes the object's storage
    /// gets the same answer, and the marker is one of those states.
    #[test]
    fn the_promotion_marker_keeps_erasure_coding_authoritative() {
        assert!(EcPlacementState::Promoting.is_ec_authoritative());
        assert!(EcPlacementState::HotReplicasPurged.is_ec_authoritative());
        assert!(!EcPlacementState::Promoted.is_ec_authoritative());
        assert!(!EcPlacementState::Tombstone.is_ec_authoritative());
        assert!(!EcPlacementState::EcReadVerified.is_ec_authoritative());
    }

    /// The hot-copy purge commits the same manifest as `hot_replicas_purged`;
    /// a layout recorded at `ec_committed` must be recognised as that manifest.
    #[test]
    fn a_reference_taken_before_the_purge_is_an_earlier_state_of_the_purged_manifest() {
        let committed = manifest(None);
        let reference = EcObjectManifestRef::from_manifest(&committed).unwrap();
        let mut purged = committed.clone();
        purged.state = EcPlacementState::HotReplicasPurged;

        assert!(!reference.matches_manifest(&purged).unwrap());
        assert!(reference.references_earlier_state_of(&purged).unwrap());
        // The manifest it already matches is not "earlier than itself".
        assert!(!reference.references_earlier_state_of(&committed).unwrap());
    }

    /// Only a step forward of the very same manifest counts.
    #[test]
    fn a_reference_to_other_work_is_not_an_earlier_state() {
        let committed = manifest(None);
        let reference = EcObjectManifestRef::from_manifest(&committed).unwrap();
        let purged = |edit: &dyn Fn(&mut EcObjectManifest)| {
            let mut manifest = committed.clone();
            manifest.state = EcPlacementState::HotReplicasPurged;
            edit(&mut manifest);
            manifest
        };

        // Another operation of the same object.
        let other_operation = purged(&|manifest| manifest.operation_id = Uuid::from_u128(0x9999));
        assert!(!reference.references_earlier_state_of(&other_operation).unwrap());
        // The same operation, but different bytes.
        let other_bytes = purged(&|manifest| manifest.content_hash_sha256 = [9; 32]);
        assert!(!reference.references_earlier_state_of(&other_bytes).unwrap());
        // Another erasure set.
        let other_set = purged(&|manifest| manifest.erasure_set_id += 1);
        assert!(!reference.references_earlier_state_of(&other_set).unwrap());

        // A quorum that is behind the reference is not the reference moved on.
        let mut behind = committed.clone();
        behind.state = EcPlacementState::EcReadVerified;
        assert!(!reference.references_earlier_state_of(&behind).unwrap());
    }

    #[tokio::test]
    async fn object_layout_can_reference_manifest_or_replicated_small_placement() {
        let dir = tempfile::tempdir().unwrap();
        let store = EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap();
        let manifest = manifest(None);
        let object = manifest.object_key();
        let manifest_ref = EcObjectManifestRef::from_manifest(&manifest).unwrap();
        let ec_layout = ObjectDataLayout::ErasureCoded(manifest_ref.clone());

        store.put_object_layout(&object, &ec_layout).await.unwrap();
        assert_eq!(
            store.get_object_layout(&object).await.unwrap(),
            Some(ec_layout)
        );
        assert!(manifest_ref.matches_manifest(&manifest).unwrap());

        let wrong_object = EcObjectVersionKey::new("photos", "other.bin", None);
        let err = store
            .put_object_layout(&wrong_object, &ObjectDataLayout::ErasureCoded(manifest_ref))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("references"));

        let small_object = EcObjectVersionKey::new("photos", "small.txt", None);
        let small_layout = ObjectDataLayout::ReplicatedSmall(
            ReplicatedSmallObjectPlacement::new(
                vec![node(1), node(2), node(3)],
                [1; 32],
                12,
                hlc(node(1)),
            )
            .unwrap(),
        );
        store.put_object_layout(&small_object, &small_layout).await.unwrap();
        assert_eq!(
            store.get_object_layout(&small_object).await.unwrap(),
            Some(small_layout)
        );
    }

    #[tokio::test]
    async fn stale_running_repair_tasks_are_recovered_for_retry() {
        let dir = tempfile::tempdir().unwrap();
        let store = EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap();
        let manifest = manifest(None);
        let shard = &manifest.chunks[0].shards[0];
        let stale_id = Uuid::from_u128(0x100);
        let fresh_id = Uuid::from_u128(0x101);

        let task = |repair_id, updated_at| EcRepairTaskRecord {
            repair_id,
            object: manifest.object_key(),
            operation_id: manifest.operation_id,
            erasure_set_id: manifest.erasure_set_id,
            chunk_index: 0,
            slot_index: shard.slot_index,
            node_id: shard.node_id,
            kind: EcRepairKind::MissingShard,
            status: EcRepairStatus::Running,
            attempts: 1,
            reason: "repair started".to_string(),
            enqueued_at: updated_at,
            updated_at,
            next_retry_at: None,
            failure_domain: None,
        };

        store.put_repair_task(&task(stale_id, 10)).await.unwrap();
        store.put_repair_task(&task(fresh_id, 175)).await.unwrap();
        assert!(store.find_next_ready_repair_task(200).await.unwrap().is_none());

        let recovered = store.recover_stale_running_repair_tasks(200, 100, 10).await.unwrap();
        assert_eq!(recovered, 1);

        let stale = store.get_repair_task(stale_id).await.unwrap().unwrap();
        assert_eq!(stale.status, EcRepairStatus::FailedRetryable);
        assert_eq!(stale.next_retry_at, Some(200));
        assert!(stale.reason.contains("stale running task recovered"));
        let fresh = store.get_repair_task(fresh_id).await.unwrap().unwrap();
        assert_eq!(fresh.status, EcRepairStatus::Running);

        let ready = store.find_next_ready_repair_task(200).await.unwrap().unwrap();
        assert_eq!(ready.repair_id, stale_id);
    }

    /// One queued repair task of one shard slot.
    ///
    /// Tasks sharing `(object, chunk_index)` are the losses of one chunk, which
    /// is what the selection measures.
    fn priority_task(
        object_key: &str,
        chunk_index: u32,
        slot_index: u8,
        erasure_set_id: u64,
        enqueued_at: u64,
    ) -> EcRepairTaskRecord {
        let mut hasher = Sha256::new();
        hasher.update(object_key.as_bytes());
        hasher.update(chunk_index.to_be_bytes());
        hasher.update([slot_index]);
        hasher.update(erasure_set_id.to_be_bytes());
        let digest = hasher.finalize();
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(&digest[..16]);
        let repair_id = Uuid::from_u128(u128::from_be_bytes(bytes) | 1);
        EcRepairTaskRecord {
            repair_id,
            object: EcObjectVersionKey::new("photos", object_key, None),
            operation_id: test_operation_id(),
            erasure_set_id,
            chunk_index,
            slot_index,
            node_id: node(slot_index + 1),
            kind: EcRepairKind::MissingShard,
            status: EcRepairStatus::Queued,
            attempts: 0,
            reason: "repair priority test task".to_string(),
            enqueued_at,
            updated_at: enqueued_at,
            next_retry_at: None,
            failure_domain: None,
        }
    }

    async fn store_with_tasks(dir: &Path, tasks: &[EcRepairTaskRecord]) -> EcMetadataStore {
        let store = EcMetadataStore::open(dir).unwrap();
        for task in tasks {
            store.put_repair_task(task).await.unwrap();
        }
        store
    }

    /// Chunks damaged alike keep the order the queue had before urgency
    /// existed: the oldest ready task wins, whatever else is in the queue.
    #[tokio::test]
    async fn repair_priority_keeps_fifo_when_urgency_is_equal() {
        let dir = tempfile::tempdir().unwrap();
        let single_losses = [
            priority_task("a.bin", 0, 0, 9, 30),
            priority_task("b.bin", 0, 0, 9, 10),
            priority_task("c.bin", 0, 0, 9, 20),
        ];
        let store = store_with_tasks(&dir.path().join("ec-meta"), &single_losses).await;
        let chosen = store.find_next_ready_repair_task(100).await.unwrap().unwrap();
        assert_eq!(chosen.repair_id, single_losses[1].repair_id);

        // The same holds one step up: when every chunk has lost two shards,
        // nothing separates them but age again.
        let double_dir = tempfile::tempdir().unwrap();
        let double_losses = [
            priority_task("a.bin", 0, 0, 9, 30),
            priority_task("a.bin", 0, 1, 9, 31),
            priority_task("b.bin", 0, 0, 9, 15),
            priority_task("b.bin", 0, 1, 9, 16),
        ];
        let store = store_with_tasks(&double_dir.path().join("ec-meta"), &double_losses).await;
        let chosen = store.find_next_ready_repair_task(100).await.unwrap().unwrap();
        assert_eq!(chosen.repair_id, double_losses[2].repair_id);
    }

    /// The point of the plan: the chunk closest to being unrecoverable is
    /// repaired first, even though an older task is waiting.
    #[tokio::test]
    async fn repair_priority_prefers_the_chunk_with_more_losses() {
        let dir = tempfile::tempdir().unwrap();
        let tasks = [
            priority_task("old.bin", 0, 0, 9, 10),
            priority_task("hurt.bin", 0, 0, 9, 20),
            priority_task("hurt.bin", 0, 1, 9, 21),
        ];
        let store = store_with_tasks(&dir.path().join("ec-meta"), &tasks).await;

        let chosen = store.find_next_ready_repair_task(100).await.unwrap().unwrap();
        assert_eq!(chosen.repair_id, tasks[1].repair_id);
        assert_ne!(
            chosen.repair_id, tasks[0].repair_id,
            "the oldest task no longer wins on age alone"
        );
    }

    /// Losses are counted per chunk, because a chunk is what the codec
    /// rebuilds. Two chunks of one object, and one chunk of two objects, are
    /// each other's neighbours in the queue and nothing more.
    #[tokio::test]
    async fn repair_priority_groups_losses_by_chunk_not_by_object() {
        let dir = tempfile::tempdir().unwrap();
        let tasks = [
            priority_task("same.bin", 0, 0, 9, 10),
            priority_task("same.bin", 1, 0, 9, 20),
            priority_task("same.bin", 1, 1, 9, 21),
        ];
        let store = store_with_tasks(&dir.path().join("ec-meta"), &tasks).await;
        let chosen = store.find_next_ready_repair_task(100).await.unwrap().unwrap();
        assert_eq!(
            chosen.repair_id, tasks[1].repair_id,
            "chunk 1 lost two shards"
        );

        // Two objects, one lost shard each: their losses do not add up, so the
        // older task is served first.
        let split_dir = tempfile::tempdir().unwrap();
        let split = [
            priority_task("first.bin", 0, 0, 9, 40),
            priority_task("second.bin", 0, 0, 9, 30),
        ];
        let store = store_with_tasks(&split_dir.path().join("ec-meta"), &split).await;
        let chosen = store.find_next_ready_repair_task(100).await.unwrap().unwrap();
        assert_eq!(chosen.repair_id, split[1].repair_id);
    }

    /// A repaired shard is not a loss, and a task closed as beyond repair will
    /// not become one on its own. Counting them would make the queue's history
    /// look like damage.
    #[tokio::test]
    async fn repair_priority_ignores_terminal_tasks_of_the_same_chunk() {
        let dir = tempfile::tempdir().unwrap();
        let mut history = [
            priority_task("history.bin", 0, 1, 9, 1),
            priority_task("history.bin", 0, 2, 9, 2),
        ];
        history[0].status = EcRepairStatus::Succeeded;
        history[1].status = EcRepairStatus::FailedPermanent;
        let tasks = [
            priority_task("history.bin", 0, 0, 9, 30),
            priority_task("hurt.bin", 0, 0, 9, 40),
            priority_task("hurt.bin", 0, 1, 9, 41),
            history[0].clone(),
            history[1].clone(),
        ];
        let store = store_with_tasks(&dir.path().join("ec-meta"), &tasks).await;

        let chosen = store.find_next_ready_repair_task(100).await.unwrap().unwrap();
        assert_eq!(
            chosen.repair_id, tasks[1].repair_id,
            "the chunk with two live losses wins over one with two finished tasks"
        );
    }

    /// A shard being repaired right now is still a shard the pool does not
    /// have. Counting only what a worker may pick up would understate the
    /// damage of exactly the chunks already being worked on.
    #[tokio::test]
    async fn repair_priority_counts_live_but_unready_siblings() {
        let dir = tempfile::tempdir().unwrap();
        let mut running = priority_task("hurt.bin", 0, 1, 9, 41);
        running.status = EcRepairStatus::Running;
        let mut backing_off = priority_task("hurt.bin", 0, 2, 9, 42);
        backing_off.status = EcRepairStatus::FailedRetryable;
        backing_off.next_retry_at = Some(10_000);
        let tasks = [
            priority_task("old.bin", 0, 0, 9, 10),
            priority_task("hurt.bin", 0, 0, 9, 40),
            running,
            backing_off,
        ];
        let store = store_with_tasks(&dir.path().join("ec-meta"), &tasks).await;

        let chosen = store.find_next_ready_repair_task(100).await.unwrap().unwrap();
        assert_eq!(
            chosen.repair_id, tasks[1].repair_id,
            "three shards of that chunk are missing, two of them not pickable yet"
        );
    }

    /// Urgency orders tasks inside a set. A set in a heavy incident does not
    /// reach into another set's candidate — taking turns between sets is the
    /// caller's rule, and this scan must not undo it.
    #[tokio::test]
    async fn repair_priority_orders_inside_each_set() {
        let dir = tempfile::tempdir().unwrap();
        let tasks = [
            priority_task("quiet.bin", 0, 0, 1, 50),
            priority_task("old.bin", 0, 0, 7, 5),
            priority_task("hurt.bin", 0, 0, 7, 60),
            priority_task("hurt.bin", 0, 1, 7, 61),
        ];
        let store = store_with_tasks(&dir.path().join("ec-meta"), &tasks).await;

        let per_set = store.find_next_ready_repair_task_per_set(100).await.unwrap();
        assert_eq!(per_set.len(), 2);
        assert_eq!(per_set[&1].most_urgent.repair_id, tasks[0].repair_id);
        assert_eq!(
            per_set[&7].most_urgent.repair_id, tasks[2].repair_id,
            "the damaged chunk wins inside its own set, not across sets"
        );
    }

    /// Two tasks the measure and the clock cannot separate still have to be
    /// separated, or one queue would answer differently from one scan to the
    /// next and no test of order could hold.
    #[tokio::test]
    async fn repair_priority_breaks_ties_by_repair_id() {
        let dir = tempfile::tempdir().unwrap();
        let tasks = [
            priority_task("a.bin", 0, 0, 9, 10),
            priority_task("bb.bin", 0, 0, 9, 10),
        ];
        let store = store_with_tasks(&dir.path().join("ec-meta"), &tasks).await;

        let expected = tasks.iter().map(|task| task.repair_id).min().unwrap();
        for _ in 0..3 {
            let chosen = store.find_next_ready_repair_task(100).await.unwrap().unwrap();
            assert_eq!(chosen.repair_id, expected);
        }
    }

    /// The claim decision EC2-160 rests on, put to the codec instead of to a
    /// reader: ordering chunks by how many shards they are missing never puts
    /// a chunk further from unrecoverable ahead of one closer to it.
    ///
    /// `distance` is how many further losses a pattern survives, taken from the
    /// decoder itself. Exhaustive over every loss pattern of every standard
    /// profile — up to 2^14 patterns each.
    #[test]
    fn repair_priority_never_puts_a_safer_chunk_first() {
        for profile in [
            EcProfileSnapshot::rs_small(),
            EcProfileSnapshot::rs_standard(),
            EcProfileSnapshot::rs_dense(),
            EcProfileSnapshot::lrc_small(),
            EcProfileSnapshot::lrc_standard(),
            EcProfileSnapshot::lrc_dense(),
        ] {
            let tolerance = s4_codec::LossTolerance::new(&profile).unwrap();
            let slots = tolerance.total_shards();
            let patterns = 1usize << slots;

            // Supersets are larger numbers, so counting down visits every
            // pattern after the patterns one further loss leads to.
            let mut distance = vec![0u32; patterns];
            for mask in (0..patterns).rev() {
                let lost = (0..slots).map(|slot| mask & (1 << slot) != 0).collect::<Vec<_>>();
                if !tolerance.can_decode(&lost) {
                    continue;
                }
                distance[mask] = (0..slots)
                    .filter(|slot| mask & (1 << slot) == 0)
                    .map(|slot| distance[mask | (1 << slot)].saturating_add(1))
                    .min()
                    .unwrap_or(0);
            }

            let mut nearest = vec![u32::MAX; slots + 1];
            let mut furthest = vec![0u32; slots + 1];
            for (mask, distance) in distance.iter().enumerate() {
                let losses = mask.count_ones() as usize;
                nearest[losses] = nearest[losses].min(*distance);
                furthest[losses] = furthest[losses].max(*distance);
            }

            // Going up in losses, the worst case of this count against the
            // best case of every smaller one.
            let mut safest_smaller_count = u32::MAX;
            for (losses, (furthest, nearest)) in furthest.iter().zip(nearest.iter()).enumerate() {
                assert!(
                    *furthest <= safest_smaller_count,
                    "{}: {losses} losses can sit {furthest} further losses from being lost, \
                     while a smaller loss can sit only {safest_smaller_count}",
                    profile.name
                );
                safest_smaller_count = safest_smaller_count.min(*nearest);
            }
        }
    }

    /// Counting losses rides along with a scan the queue was already paying
    /// for. Measured against that same scan doing nothing but finding the
    /// oldest record, on a queue the size of a whole node's loss.
    #[tokio::test]
    async fn repair_priority_costs_about_what_a_plain_scan_costs() {
        const TASKS: usize = 10_000;

        let dir = tempfile::tempdir().unwrap();
        let store = EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap();
        // Written straight into the keyspace: 10 000 durable puts would fsync
        // 10 000 times and measure the disk instead of the scan.
        for index in 0..TASKS {
            let chunk = (index / 2) as u32;
            let slot = (index % 2) as u8;
            let mut task = priority_task("node-loss.bin", chunk, slot, 9, 1_000 + index as u64);
            // One chunk in five hundred has lost a second shard; the rest have
            // lost one, which is what losing a single node looks like.
            if slot == 1 && !chunk.is_multiple_of(250) {
                task.status = EcRepairStatus::Succeeded;
            }
            store
                .repair_tasks
                .insert(
                    &serialize(&task.repair_id).unwrap()[..],
                    &serialize(&task).unwrap()[..],
                )
                .unwrap();
        }

        let plain_scan = || {
            let mut oldest: Option<EcRepairTaskRecord> = None;
            for guard in store.repair_tasks.iter() {
                let (_key, value) = guard.into_inner().unwrap();
                let task: EcRepairTaskRecord = deserialize(&value).unwrap();
                if !repair_task_is_ready_at(&task, u64::MAX) {
                    continue;
                }
                if oldest.as_ref().is_none_or(|best| task.enqueued_at < best.enqueued_at) {
                    oldest = Some(task);
                }
            }
            oldest
        };

        // Both scans run twice; the faster run of each is compared, so a
        // stolen time slice does not decide the result.
        let mut plain = Duration::MAX;
        let mut prioritised = Duration::MAX;
        for _ in 0..2 {
            let started = Instant::now();
            assert!(plain_scan().is_some());
            plain = plain.min(started.elapsed());

            let started = Instant::now();
            let chosen = store.find_next_ready_repair_task(u64::MAX).await.unwrap().unwrap();
            prioritised = prioritised.min(started.elapsed());
            assert!(
                chosen.chunk_index.is_multiple_of(250),
                "a chunk missing two shards wins"
            );
            assert_eq!(chosen.chunk_index, 0, "and the oldest of those");
        }

        // Visible with `--nocapture`, so that the comparison can be re-measured
        // on any machine instead of trusted from a plan.
        eprintln!("{TASKS} tasks: plain scan {plain:?}, priority scan {prioritised:?}");
        assert!(
            prioritised <= plain * 3 + Duration::from_millis(50),
            "priority scan {prioritised:?} against plain scan {plain:?} over {TASKS} tasks"
        );
    }

    /// The scan position survives a restart and is one record, not one per key.
    #[tokio::test]
    async fn the_node_presence_record_round_trips_and_never_walks_backwards() {
        let dir = tempfile::tempdir().unwrap();
        let store = EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap();
        let node_id = NodeId(7);

        // A store that has never served a pool says so, and that absence is the
        // evidence the rejoin decision reads.
        assert!(store.get_node_presence().await.unwrap().is_none());

        let first = EcNodePresenceRecord::first(node_id, "ec", 1_000, true);
        store.put_node_presence(&first).await.unwrap();
        let read_back = store.get_node_presence().await.unwrap().unwrap();
        assert_eq!(read_back, first);
        assert!(read_back.rebuild_pending);

        // A clock that stepped back must not make the node look as if it had
        // been away since the future.
        let refreshed = read_back.refreshed(500, false);
        assert_eq!(refreshed.last_seen_at, 1_000);
        assert_eq!(refreshed.first_seen_at, 1_000);
        assert!(!refreshed.rebuild_pending);

        store.put_node_presence(&refreshed.refreshed(2_000, false)).await.unwrap();
        let latest = store.get_node_presence().await.unwrap().unwrap();
        assert_eq!(latest.last_seen_at, 2_000);
        assert_eq!(latest.first_seen_at, 1_000);
    }

    /// A record that names no pool, or claims to have been last seen before it
    /// was first seen, is refused before it reaches the store.
    #[tokio::test]
    async fn a_nonsensical_node_presence_record_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let store = EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap();

        let nameless = EcNodePresenceRecord::first(NodeId(7), "  ", 1_000, false);
        assert!(store.put_node_presence(&nameless).await.is_err());

        let mut backwards = EcNodePresenceRecord::first(NodeId(7), "ec", 1_000, false);
        backwards.last_seen_at = 999;
        assert!(store.put_node_presence(&backwards).await.is_err());

        assert!(store.get_node_presence().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn the_tiering_cursor_round_trips_and_stays_single() {
        let dir = tempfile::tempdir().unwrap();
        let store = EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap();

        assert!(store.get_tiering_cursor().await.unwrap().is_none());

        store
            .put_tiering_cursor(&EcTieringCursorRecord {
                bucket: "photos".to_string(),
                continuation: Some("2024/06/img.jpg".to_string()),
                updated_at: 42,
            })
            .await
            .unwrap();
        store
            .put_tiering_cursor(&EcTieringCursorRecord {
                bucket: "videos".to_string(),
                continuation: None,
                updated_at: 43,
            })
            .await
            .unwrap();

        let cursor = store.get_tiering_cursor().await.unwrap().unwrap();
        assert_eq!(cursor.bucket, "videos");
        assert_eq!(cursor.continuation, None);

        store.delete_tiering_cursor().await.unwrap();
        assert!(store.get_tiering_cursor().await.unwrap().is_none());
    }

    /// A continuation token belongs to a bucket, so one without a bucket is
    /// refused: the next cycle would hand it to whichever bucket sorts first.
    #[tokio::test]
    async fn a_tiering_cursor_continuation_requires_its_bucket() {
        let dir = tempfile::tempdir().unwrap();
        let store = EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap();

        let orphaned = EcTieringCursorRecord {
            bucket: String::new(),
            continuation: Some("some-key".to_string()),
            updated_at: 1,
        };

        assert!(orphaned.validate().is_err());
        assert!(store.put_tiering_cursor(&orphaned).await.is_err());
        assert!(EcTieringCursorRecord::start(1).validate().is_ok());
    }

    /// The two keyspaces added by auto-tiering must not see each other's rows.
    ///
    /// They are opened from the same database and their keys are both bincode,
    /// so a mistake in wiring would show up as a policy appearing in the heat
    /// listing rather than as a compile error.
    #[tokio::test]
    async fn heat_and_policy_keyspaces_stay_separate() {
        let dir = tempfile::tempdir().unwrap();
        let store = EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap();
        let object = EcObjectVersionKey::new("bucket", "dir/object.bin", None);

        store
            .put_object_heat(&EcObjectHeatRecord::first_access(
                object.clone(),
                1_700,
                DAY_NANOS,
            ))
            .await
            .unwrap();
        store
            .put_tiering_policy(&EcTieringPolicyRecord::inheriting(
                "bucket",
                hlc(node(1)),
                1_700,
            ))
            .await
            .unwrap();

        let heat = store.list_object_heat_page(None, 16).await.unwrap();
        assert_eq!(heat.entries.len(), 1);
        assert_eq!(heat.entries[0].object, object);

        let policies = store.list_tiering_policies_page(None, 16).await.unwrap();
        assert_eq!(policies.entries.len(), 1);
        assert_eq!(policies.entries[0].bucket, "bucket");
    }

    #[tokio::test]
    async fn object_heat_round_trips_per_version_and_deletes() {
        let dir = tempfile::tempdir().unwrap();
        let store = EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap();
        let current = EcObjectVersionKey::new("bucket", "object.bin", None);
        let versioned = EcObjectVersionKey::new("bucket", "object.bin", Some("v-1".to_string()));

        store
            .put_object_heat(&EcObjectHeatRecord::first_access(
                current.clone(),
                10,
                DAY_NANOS,
            ))
            .await
            .unwrap();
        store
            .put_object_heat(&EcObjectHeatRecord::first_access(
                versioned.clone(),
                20,
                DAY_NANOS,
            ))
            .await
            .unwrap();

        // Two versions of one key are two independent marks. The runtime only
        // tiers the current version today (EC2-025), but the metadata model is
        // already version-keyed, so it will not need a migration later.
        assert_eq!(
            store.get_object_heat(&current).await.unwrap().unwrap().last_access_nanos,
            10
        );
        assert_eq!(
            store.get_object_heat(&versioned).await.unwrap().unwrap().last_access_nanos,
            20
        );

        store.delete_object_heat(&current).await.unwrap();
        assert!(store.get_object_heat(&current).await.unwrap().is_none());
        assert!(store.get_object_heat(&versioned).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn missing_heat_mark_is_not_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let store = EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap();
        let object = EcObjectVersionKey::new("bucket", "never-read.bin", None);

        // An object nobody has read yet has no mark, and the scanner has to be
        // able to tell that apart from an error: it falls back to creation time.
        assert!(store.get_object_heat(&object).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn tiering_policies_are_stored_per_bucket() {
        let dir = tempfile::tempdir().unwrap();
        let store = EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap();

        let mut archive = EcTieringPolicyRecord::inheriting("archive", hlc(node(1)), 5);
        archive.cold_after_secs = Some(7 * 24 * 3_600);
        archive.exclude_prefixes = vec!["keep-hot/".to_string()];
        store.put_tiering_policy(&archive).await.unwrap();
        store
            .put_tiering_policy(&EcTieringPolicyRecord::inheriting(
                "scratch",
                hlc(node(2)),
                6,
            ))
            .await
            .unwrap();

        let stored = store.get_tiering_policy("archive").await.unwrap().unwrap();
        assert_eq!(stored.cold_after_secs, Some(7 * 24 * 3_600));
        assert_eq!(stored.exclude_prefixes, vec!["keep-hot/".to_string()]);
        // An unset field means "inherit", so it must survive as None and not as
        // a zero that the merge would mistake for an explicit override.
        assert_eq!(stored.min_object_size, None);
        assert_eq!(
            store.get_tiering_policy("scratch").await.unwrap().unwrap().cold_after_secs,
            None
        );
        assert!(store.get_tiering_policy("absent").await.unwrap().is_none());

        store.delete_tiering_policy("archive").await.unwrap();
        assert!(store.get_tiering_policy("archive").await.unwrap().is_none());
    }

    #[test]
    fn tiering_policy_refuses_a_cold_after_below_one_day() {
        let mut policy = EcTieringPolicyRecord::inheriting("bucket", hlc(node(1)), 1);
        policy.cold_after_secs = Some(MIN_TIERING_COLD_AFTER_SECS - 1);
        assert!(policy.validate().is_err());

        policy.cold_after_secs = Some(MIN_TIERING_COLD_AFTER_SECS);
        assert!(policy.validate().is_ok());
    }

    #[test]
    fn tiering_policy_refuses_blank_bucket_zero_hlc_and_empty_prefix() {
        let valid = EcTieringPolicyRecord::inheriting("bucket", hlc(node(1)), 1);
        assert!(valid.validate().is_ok());

        let mut blank = valid.clone();
        blank.bucket = "  ".to_string();
        assert!(blank.validate().is_err());

        let mut zero_hlc = valid.clone();
        zero_hlc.hlc = HlcTimestamp::zero();
        assert!(zero_hlc.validate().is_err());

        let mut empty_prefix = valid.clone();
        empty_prefix.exclude_prefixes = vec![String::new()];
        assert!(empty_prefix.validate().is_err());

        let mut empty_tag = valid.clone();
        empty_tag.exclude_tags = HashMap::from([(String::new(), "x".to_string())]);
        assert!(empty_tag.validate().is_err());

        let mut zero_window = valid;
        zero_window.promote_window_secs = Some(0);
        assert!(zero_window.validate().is_err());
    }

    /// The promotion queue is keyed by object and survives a restart.
    #[tokio::test]
    async fn promotion_jobs_round_trip_and_count_only_unfinished_work() {
        let dir = tempfile::tempdir().unwrap();
        let store = EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap();

        let queued = promotion_job("a.bin", EcPromotionState::Queued);
        let finished = promotion_job("b.bin", EcPromotionState::Completed);
        store.put_promotion_job(&queued).await.unwrap();
        store.put_promotion_job(&finished).await.unwrap();

        assert_eq!(
            store.get_promotion_job(&queued.object).await.unwrap(),
            Some(queued.clone())
        );
        // Only unfinished work occupies the queue: a job that reached its end
        // must not hold the ceiling against the next rollback.
        assert_eq!(store.count_in_flight_promotions(16, 8).await.unwrap(), 1);

        // Re-queueing the same object replaces the record rather than adding a
        // second one, which is what makes admission idempotent.
        let mut advanced = queued.clone();
        advanced.state = EcPromotionState::HotRestored;
        store.put_promotion_job(&advanced).await.unwrap();
        let page = store.list_promotion_jobs_page(None, 16).await.unwrap();
        assert_eq!(page.entries.len(), 2);

        store.delete_promotion_job(&queued.object).await.unwrap();
        assert!(store.get_promotion_job(&queued.object).await.unwrap().is_none());
    }

    /// Counting stops at the ceiling rather than walking the whole keyspace.
    #[tokio::test]
    async fn counting_promotions_stops_at_the_ceiling() {
        let dir = tempfile::tempdir().unwrap();
        let store = EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap();
        for index in 0..8 {
            store
                .put_promotion_job(&promotion_job(
                    &format!("object-{index}.bin"),
                    EcPromotionState::Queued,
                ))
                .await
                .unwrap();
        }

        assert_eq!(store.count_in_flight_promotions(3, 2).await.unwrap(), 3);
        assert_eq!(store.count_in_flight_promotions(100, 2).await.unwrap(), 8);
    }

    /// A versioned object never enters the queue.
    ///
    /// The attach path behind a promotion has no version-aware writer, so a job
    /// naming a version could not be carried out. Refusing it at the record
    /// keeps it from ever reaching the queue. Decision EC2-025.
    #[tokio::test]
    async fn a_versioned_promotion_job_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let store = EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap();
        let mut versioned = promotion_job("a.bin", EcPromotionState::Queued);
        versioned.object = EcObjectVersionKey::new("bucket", "a.bin", Some("v-1".to_string()));

        assert!(versioned.validate().is_err());
        assert!(store.put_promotion_job(&versioned).await.is_err());
    }

    #[test]
    fn object_heat_refuses_a_window_that_starts_after_the_last_access() {
        let object = EcObjectVersionKey::new("bucket", "object.bin", None);
        let mut record = EcObjectHeatRecord::first_access(object, 100, DAY_NANOS);
        assert!(record.validate().is_ok());

        record.window.newest_slot_started_nanos = 101;
        assert!(record.validate().is_err());

        record.window.newest_slot_started_nanos = 0;
        record.last_access_nanos = 0;
        assert!(record.validate().is_err());
    }

    /// The count slides instead of resetting.
    ///
    /// A tumbling window drops to zero the moment it rolls over, so an object
    /// read steadily around the boundary reports a fraction of its traffic and
    /// never reaches a promotion threshold it plainly deserves.
    #[test]
    fn the_read_window_slides_rather_than_resetting() {
        let hour = DAY_NANOS / 24;
        let mut window = EcHeatWindow::new(DAY_NANOS, 0);

        // One read every hour for a full day.
        for step in 0..24 {
            window.record(step * hour, 1);
        }
        assert_eq!(window.reads_at(23 * hour), 24);

        // Half a day later, only the reads inside the trailing day count. With
        // eight slots the boundary is three hours wide, which is why the
        // assertion is a range and not an equality.
        let half_day_later = 23 * hour + DAY_NANOS / 2;
        let remaining = window.reads_at(half_day_later);
        assert!(
            (11..=14).contains(&remaining),
            "expected about half the reads to survive, got {remaining}"
        );

        // A full window of silence decays the count to nothing.
        assert_eq!(window.reads_at(23 * hour + 2 * DAY_NANOS), 0);
    }

    /// Reads survive a change of the configured window width.
    ///
    /// Re-bucketing them honestly is impossible, so they collapse into the
    /// newest slot: an object keeps the credit it earned and loses only the
    /// knowledge of when inside the window it earned it.
    #[test]
    fn rescaling_the_window_keeps_the_reads_it_had() {
        let hour = DAY_NANOS / 24;
        let mut window = EcHeatWindow::new(DAY_NANOS, 0);
        window.record(hour, 5);
        window.record(2 * hour, 3);

        window.rescale(DAY_NANOS * 7, 3 * hour);

        assert_eq!(window.width_nanos(), DAY_NANOS * 7);
        assert_eq!(window.reads_at(3 * hour), 8);
        assert!(window.validate().is_ok());
    }

    /// Out-of-order marks land in the newest slot rather than being dropped.
    ///
    /// The tracker collapses signals and flushes them in batches, so a mark can
    /// arrive slightly behind one already recorded. Dropping it would
    /// understate exactly the traffic being measured.
    #[test]
    fn a_late_mark_still_counts() {
        let hour = DAY_NANOS / 24;
        let mut window = EcHeatWindow::new(DAY_NANOS, 0);
        window.record(6 * hour, 2);
        window.record(hour, 4);

        assert_eq!(window.reads_at(6 * hour), 6);
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    enum LegacyObjectDataLayout {
        ReplicatedSmall(ReplicatedSmallObjectPlacement),
        ErasureCoded(EcObjectManifestRef),
    }

    /// Packed segment layout round-trips through bincode serialization.
    #[test]
    fn packed_segment_layout_round_trips() {
        let placement = PackedSegmentObjectPlacement::new(
            PackedSegmentId::new_random(),
            42,
            100,
            2048,
            [0xab; 32],
            hlc(node(1)),
            vec![node(1), node(2), node(3)],
        )
        .unwrap();
        let layout = ObjectDataLayout::PackedSegment(placement);
        let serialized = serialize(&layout).unwrap();
        let deserialized: ObjectDataLayout = deserialize(&serialized).unwrap();
        assert_eq!(deserialized, layout);
    }

    /// Layouts written before packed segments were introduced still deserialize correctly.
    #[test]
    fn layouts_written_before_packed_segments_still_read() {
        let small_placement = ReplicatedSmallObjectPlacement::new(
            vec![node(1), node(2), node(3)],
            [0x11; 32],
            512,
            hlc(node(1)),
        )
        .unwrap();
        let legacy_small = LegacyObjectDataLayout::ReplicatedSmall(small_placement.clone());
        let small_bytes = serialize(&legacy_small).unwrap();
        let modern_small: ObjectDataLayout = deserialize(&small_bytes).unwrap();
        assert_eq!(
            modern_small,
            ObjectDataLayout::ReplicatedSmall(small_placement)
        );

        let manifest_ref = EcObjectManifestRef {
            object: EcObjectVersionKey::new("my-bucket", "my-key", Some("v1".to_string())),
            erasure_set_id: 12,
            manifest_hash_sha256: [0x22; 32],
            hlc: hlc(node(2)),
            operation_id: test_operation_id(),
        };
        let legacy_ec = LegacyObjectDataLayout::ErasureCoded(manifest_ref.clone());
        let ec_bytes = serialize(&legacy_ec).unwrap();
        let modern_ec: ObjectDataLayout = deserialize(&ec_bytes).unwrap();
        assert_eq!(modern_ec, ObjectDataLayout::ErasureCoded(manifest_ref));
    }

    /// A node running code before packed segments rejects the new layout with an error rather than corrupting it.
    #[test]
    fn a_node_without_packed_segments_rejects_the_new_layout() {
        let placement = PackedSegmentObjectPlacement::new(
            PackedSegmentId::new_random(),
            7,
            0,
            1024,
            [0x33; 32],
            hlc(node(1)),
            vec![],
        )
        .unwrap();
        let modern_layout = ObjectDataLayout::PackedSegment(placement);
        let serialized = serialize(&modern_layout).unwrap();
        let result: Result<LegacyObjectDataLayout, _> = deserialize(&serialized);
        match result {
            Ok(unexpected) => panic!(
                "expected legacy deserialization to fail on new variant, but got: {unexpected:?}"
            ),
            Err(err) => {
                let message = err.to_string();
                assert!(
                    !message.is_empty(),
                    "legacy node must reject new layout with an error: {message}"
                );
            }
        }
    }

    /// The variant index order is a format contract: PackedSegment must be variant index 2.
    #[test]
    fn packed_segment_is_the_last_layout_variant() {
        let small = ObjectDataLayout::ReplicatedSmall(ReplicatedSmallObjectPlacement {
            replica_nodes: vec![node(1), node(2), node(3)],
            content_hash_sha256: [1; 32],
            size: 10,
            hlc: hlc(node(1)),
        });
        let ec = ObjectDataLayout::ErasureCoded(EcObjectManifestRef {
            object: EcObjectVersionKey::new("bucket", "key", None),
            erasure_set_id: 1,
            manifest_hash_sha256: [2; 32],
            hlc: hlc(node(1)),
            operation_id: test_operation_id(),
        });
        let packed = ObjectDataLayout::PackedSegment(PackedSegmentObjectPlacement {
            segment_id: PackedSegmentId::new_random(),
            erasure_set_id: 1,
            offset: 0,
            length: 10,
            content_hash_sha256: [3; 32],
            hlc: hlc(node(1)),
            hot_replica_nodes: vec![],
        });

        let bytes_small = serialize(&small).unwrap();
        let bytes_ec = serialize(&ec).unwrap();
        let bytes_packed = serialize(&packed).unwrap();

        assert!(bytes_small.len() >= 4);
        assert!(bytes_ec.len() >= 4);
        assert!(bytes_packed.len() >= 4);

        let idx_small = u32::from_le_bytes(bytes_small[0..4].try_into().unwrap());
        let idx_ec = u32::from_le_bytes(bytes_ec[0..4].try_into().unwrap());
        let idx_packed = u32::from_le_bytes(bytes_packed[0..4].try_into().unwrap());

        assert_eq!(idx_small, 0, "ReplicatedSmall must be variant index 0");
        assert_eq!(idx_ec, 1, "ErasureCoded must be variant index 1");
        assert_eq!(idx_packed, 2, "PackedSegment must be variant index 2");
    }

    /// Packed segment placement validates its segment ID, non-zero length, and overflow.
    #[test]
    fn packed_placement_validates_its_region() {
        let valid_id = PackedSegmentId::new_random();
        let nil_id = PackedSegmentId::from_uuid(Uuid::nil());

        // nil segment_id -> Err
        let p_nil = PackedSegmentObjectPlacement {
            segment_id: nil_id,
            erasure_set_id: 1,
            offset: 0,
            length: 100,
            content_hash_sha256: [1; 32],
            hlc: hlc(node(1)),
            hot_replica_nodes: vec![],
        };
        assert!(p_nil.validate().is_err());

        // length 0 -> Err
        let p_zero = PackedSegmentObjectPlacement {
            segment_id: valid_id,
            erasure_set_id: 1,
            offset: 0,
            length: 0,
            content_hash_sha256: [1; 32],
            hlc: hlc(node(1)),
            hot_replica_nodes: vec![],
        };
        assert!(p_zero.validate().is_err());

        // offset = u64::MAX and length = 1 -> Err (overflow)
        let p_overflow = PackedSegmentObjectPlacement {
            segment_id: valid_id,
            erasure_set_id: 1,
            offset: u64::MAX,
            length: 1,
            content_hash_sha256: [1; 32],
            hlc: hlc(node(1)),
            hot_replica_nodes: vec![],
        };
        assert!(p_overflow.validate().is_err());

        // Normal case -> Ok
        let p_ok = PackedSegmentObjectPlacement {
            segment_id: valid_id,
            erasure_set_id: 1,
            offset: 1024,
            length: 2048,
            content_hash_sha256: [1; 32],
            hlc: hlc(node(1)),
            hot_replica_nodes: vec![],
        };
        assert!(p_ok.validate().is_ok());
    }

    /// Packed segment placement hot replica list accepts only empty or exactly three distinct nodes.
    #[test]
    fn packed_placement_accepts_no_replicas_or_exactly_three() {
        let make = |nodes: Vec<NodeId>| PackedSegmentObjectPlacement {
            segment_id: PackedSegmentId::new_random(),
            erasure_set_id: 1,
            offset: 0,
            length: 100,
            content_hash_sha256: [1; 32],
            hlc: hlc(node(1)),
            hot_replica_nodes: nodes,
        };

        assert!(make(vec![]).validate().is_ok(), "empty list -> Ok");
        assert!(make(vec![node(1)]).validate().is_err(), "1 node -> Err");
        assert!(
            make(vec![node(1), node(2)]).validate().is_err(),
            "2 nodes -> Err"
        );
        assert!(
            make(vec![node(1), node(2), node(3)]).validate().is_ok(),
            "3 distinct nodes -> Ok"
        );
        assert!(
            make(vec![node(1), node(2), node(1)]).validate().is_err(),
            "3 nodes with duplicate -> Err"
        );
        assert!(
            make(vec![node(1), node(2), node(3), node(4)]).validate().is_err(),
            "4 nodes -> Err"
        );
    }

    /// Packed placement hot_placement mirrors object metadata when hot replicas exist, and gives None when empty.
    #[test]
    fn packed_placement_hot_placement_mirrors_the_object() {
        let nodes = vec![node(1), node(2), node(3)];
        let hash = [0x5a; 32];
        let timestamp = hlc(node(2));
        let length = 8192;

        let placement_with_hot = PackedSegmentObjectPlacement::new(
            PackedSegmentId::new_random(),
            5,
            1024,
            length,
            hash,
            timestamp,
            nodes.clone(),
        )
        .unwrap();

        let hot = placement_with_hot.hot_placement();
        assert!(hot.is_some());
        let hot = hot.unwrap();
        assert_eq!(hot.replica_nodes, nodes);
        assert_eq!(hot.content_hash_sha256, hash);
        assert_eq!(hot.size, length);
        assert_eq!(hot.hlc, timestamp);

        let placement_without_hot = PackedSegmentObjectPlacement::new(
            PackedSegmentId::new_random(),
            5,
            1024,
            length,
            hash,
            timestamp,
            vec![],
        )
        .unwrap();

        assert!(placement_without_hot.hot_placement().is_none());
    }

    /// Deriving placement from entry preserves segment ID, erasure set ID, offset, length, hash, and HLC.
    #[test]
    fn packed_placement_from_entry_copies_the_entry() {
        let segment_id = PackedSegmentId::new_random();
        let erasure_set_id = 99;
        let config = crate::ec_packed_segments::PackedSegmentConfig::default();
        let segment = PackedSegment::open(segment_id, erasure_set_id, &config, 1_000);

        let object_key = EcObjectVersionKey::new("bucket", "key.bin", None);
        let offset = 4096;
        let length = 1024;
        let hash = [0x7b; 32];
        let timestamp = hlc(node(1));
        let entry = PackedSegmentEntry {
            object: object_key,
            offset,
            length,
            content_hash_sha256: hash,
            hlc: timestamp,
            etag: String::new(),
            content_type: String::new(),
            user_metadata: std::collections::HashMap::new(),
        };

        let hot_nodes = vec![node(1), node(2), node(3)];
        let placement =
            PackedSegmentObjectPlacement::from_entry(&segment, &entry, hot_nodes.clone()).unwrap();

        assert_eq!(placement.segment_id, segment.segment_id);
        assert_eq!(placement.erasure_set_id, segment.erasure_set_id);
        assert_eq!(placement.offset, entry.offset);
        assert_eq!(placement.length, entry.length);
        assert_eq!(placement.content_hash_sha256, entry.content_hash_sha256);
        assert_eq!(placement.hlc, entry.hlc);
        assert_eq!(placement.hot_replica_nodes, hot_nodes);
    }

    /// ObjectDataLayout validation correctly delegates to the PackedSegment variant and checks object key binding.
    #[test]
    fn object_layout_validation_covers_the_packed_variant() {
        let bad_placement = PackedSegmentObjectPlacement {
            segment_id: PackedSegmentId::from_uuid(Uuid::nil()),
            erasure_set_id: 1,
            offset: 0,
            length: 100,
            content_hash_sha256: [1; 32],
            hlc: hlc(node(1)),
            hot_replica_nodes: vec![],
        };
        let bad_layout = ObjectDataLayout::PackedSegment(bad_placement);
        assert!(bad_layout.validate().is_err());

        let good_placement = PackedSegmentObjectPlacement::new(
            PackedSegmentId::new_random(),
            1,
            0,
            100,
            [1; 32],
            hlc(node(1)),
            vec![],
        )
        .unwrap();
        let good_layout = ObjectDataLayout::PackedSegment(good_placement);
        assert!(good_layout.validate().is_ok());

        let object_key = EcObjectVersionKey::new("valid-bucket", "valid-key", None);
        assert!(good_layout.validate_for_object(&object_key).is_ok());
    }

    /// Unreadable journal entries are returned as discarded tails rather than silently skipped.
    #[tokio::test]
    async fn unreadable_journal_value_is_reported_not_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let store = EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap();

        let segment_id = PackedSegmentId::new_random();
        let open_item = PackedSegmentJournalItem {
            segment_id,
            sequence: 0,
            payload: PackedSegmentJournalPayload::SegmentOpened {
                erasure_set_id: 1,
                target_bytes: 64 * 1024 * 1024,
                max_objects: 4096,
                created_at: 1000,
            },
        };
        store.append_packed_segment_journal(&open_item).await.unwrap();

        let bad_key = packed_segment_journal_key(segment_id, 1);
        store
            .packed_segment_journal
            .insert(&bad_key, b"unreadable garbage payload")
            .unwrap();
        store.db.persist(PersistMode::SyncAll).unwrap();

        let recovery = store.load_packed_segments(1000).await.unwrap();

        let recovered_segment = recovery
            .segments
            .iter()
            .find(|s| s.segment_id == segment_id)
            .expect("opened segment should be returned in recovered segments");
        assert_eq!(recovered_segment.segment_id, segment_id);

        assert!(
            !recovery.discarded.is_empty(),
            "recovery must report non-empty discarded tails"
        );
        let discarded_tail = recovery
            .discarded
            .iter()
            .find(|t| t.segment_id == segment_id)
            .expect("discarded tail for corrupted segment must be present");
        assert_eq!(discarded_tail.from_sequence, 1);
    }

    fn packed_manifest() -> EcObjectManifest {
        let mut manifest = manifest(None);
        manifest.format_version = EC_PACKED_MANIFEST_FORMAT_VERSION;
        manifest.chunks = Vec::new();
        manifest.original_size = 4096;
        manifest.content_hash_sha256 = [3u8; 32];
        manifest.packed = Some(
            PackedSegmentObjectPlacement::new(
                PackedSegmentId::from_uuid(Uuid::from_u128(0x5e6d)),
                manifest.erasure_set_id,
                8192,
                4096,
                [3u8; 32],
                manifest.hlc,
                Vec::new(),
            )
            .unwrap(),
        );
        assert!(manifest.validate().is_ok());
        manifest
    }

    #[test]
    fn classic_manifest_encoding_is_unchanged_by_the_packed_field() {
        // Guarantee that a manifest written by an older build has the same bytes.
        assert_eq!(
            encode_manifest(&manifest(None)).unwrap(),
            bincode::serialize(&manifest(None)).unwrap()
        );
    }

    #[test]
    fn manifest_bytes_written_before_this_build_still_decode() {
        let original = manifest(None);
        let serialized = bincode::serialize(&original).unwrap();
        let decoded = decode_manifest(&serialized).unwrap();
        assert_eq!(decoded, original);
        assert!(decoded.packed.is_none());
    }

    #[test]
    fn packed_manifest_survives_a_round_trip() {
        let original = packed_manifest();
        let decoded = decode_manifest(&encode_manifest(&original).unwrap()).unwrap();
        assert_eq!(decoded, original);
        let packed = decoded.packed.as_ref().unwrap();
        assert_eq!(packed.offset, original.packed.as_ref().unwrap().offset);
        assert_eq!(packed.length, original.packed.as_ref().unwrap().length);
    }

    #[test]
    fn a_packed_manifest_is_longer_than_its_body_and_names_its_own_version() {
        let packed = packed_manifest();
        let encoded_packed = encode_manifest(&packed).unwrap();
        let serialized_body = bincode::serialize(&packed).unwrap();
        assert!(encoded_packed.len() > serialized_body.len());
        assert_eq!(encoded_packed[0], EC_PACKED_MANIFEST_FORMAT_VERSION);

        let classic = manifest(None);
        let encoded_classic = encode_manifest(&classic).unwrap();
        assert_eq!(encoded_classic[0], EC_MANIFEST_FORMAT_VERSION);
    }

    #[test]
    fn the_placement_is_covered_by_the_manifest_hash() {
        let first = packed_manifest();
        let mut second = packed_manifest();
        second.packed = Some(
            PackedSegmentObjectPlacement::new(
                PackedSegmentId::from_uuid(Uuid::from_u128(0x5e6d)),
                second.erasure_set_id,
                12288,
                4096,
                [3u8; 32],
                second.hlc,
                Vec::new(),
            )
            .unwrap(),
        );

        assert_ne!(
            first.serialized_sha256().unwrap(),
            second.serialized_sha256().unwrap()
        );
    }

    #[test]
    fn decode_manifest_rejects_a_format_version_this_build_does_not_know() {
        let mut bytes = encode_manifest(&manifest(None)).unwrap();
        bytes[0] = 7;
        assert!(decode_manifest(&bytes).is_err());
    }

    #[test]
    fn format_version_one_rejects_a_packed_placement() {
        let mut manifest = packed_manifest();
        manifest.format_version = EC_MANIFEST_FORMAT_VERSION;
        assert!(manifest.validate().is_err());
    }

    #[test]
    fn format_version_two_requires_a_packed_placement() {
        let mut manifest = manifest(None);
        manifest.format_version = EC_PACKED_MANIFEST_FORMAT_VERSION;
        assert!(manifest.validate().is_err());
    }

    #[test]
    fn a_packed_manifest_must_not_carry_chunks_of_its_own() {
        let mut packed = packed_manifest();
        packed.chunks = manifest(None).chunks;
        assert!(packed.validate().is_err());
    }

    #[test]
    fn a_packed_manifest_must_agree_with_its_placement() {
        let mut wrong_size = packed_manifest();
        wrong_size.original_size = 4095;
        assert!(wrong_size.validate().is_err());

        let mut wrong_hash = packed_manifest();
        wrong_hash.content_hash_sha256 = [9u8; 32];
        assert!(wrong_hash.validate().is_err());

        let mut wrong_set = packed_manifest();
        wrong_set.erasure_set_id += 1;
        assert!(wrong_set.validate().is_err());
    }

    #[test]
    fn a_tombstone_never_carries_a_packed_placement() {
        let mut manifest = packed_manifest();
        manifest.is_deleted = true;
        manifest.state = EcPlacementState::Tombstone;
        manifest.chunks = Vec::new();
        assert!(manifest.validate().is_err());
    }

    /// A purged segment, an encoded replacement for it, and the moves between.
    ///
    /// The two states a cutover joins: a source whose objects read from its own
    /// shards, and a replacement that already has shards of its own but that
    /// nothing points at yet.
    async fn prepare_repack_fixture(
        objects: usize,
    ) -> (
        tempfile::TempDir,
        EcMetadataStore,
        PackedSegment,
        PackedSegment,
        Vec<PackedRepackMove>,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let store = EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap();
        let config = PackedSegmentConfig::default();
        let mut source = PackedSegment::open(PackedSegmentId::new_random(), 1, &config, 100);

        for index in 1..=objects {
            let object = EcObjectVersionKey::new("bucket", format!("key-{index}"), None);
            let identity =
                PackedObjectIdentity::new(object, [(index % 255) as u8; 32], hlc(node(1)));
            let reservation = source.reserve(1000).unwrap();
            let item = source.journal_append(&reservation, identity).unwrap();
            source.commit(&reservation, &item).unwrap();
        }

        source.seal(100, SealReason::TargetSizeReached).unwrap();
        source.transition_to(PackedSegmentState::Encoded).unwrap();
        source.transition_to(PackedSegmentState::Purged).unwrap();
        store.put_packed_segment(&source).await.unwrap();

        // No hot replicas: a purged segment's objects are served by its shards,
        // which is what makes its dead space unreclaimable without a repack.
        for entry in &source.entries {
            let placement =
                PackedSegmentObjectPlacement::from_entry(&source, entry, Vec::new()).unwrap();
            store
                .put_object_layout(&entry.object, &ObjectDataLayout::PackedSegment(placement))
                .await
                .unwrap();
        }

        let mut destination =
            PackedSegment::open_repack(PackedSegmentId::new_random(), &source, &config, 200);
        for entry in &source.entries {
            let identity = PackedObjectIdentity::new(
                entry.object.clone(),
                entry.content_hash_sha256,
                entry.hlc,
            );
            destination.append_without_journal(identity, entry.length).unwrap();
        }
        destination.seal(300, SealReason::Repacked).unwrap();
        destination.transition_to(PackedSegmentState::Encoded).unwrap();
        store.put_packed_segment(&destination).await.unwrap();

        let moves = destination
            .entries
            .iter()
            .map(|entry| {
                let placement =
                    PackedSegmentObjectPlacement::from_entry(&destination, entry, Vec::new())
                        .unwrap();
                PackedRepackMove {
                    object: entry.object.clone(),
                    placement,
                }
            })
            .collect();

        (dir, store, source, destination, moves)
    }

    #[tokio::test]
    async fn a_repack_cutover_switches_every_object_at_once() {
        let (_dir, store, source, destination, moves) = prepare_repack_fixture(3).await;
        let now = 1000;
        let cutover = store.commit_packed_repack(&source, &destination, &moves, now).await.unwrap();

        assert_eq!(cutover.switched.len(), moves.len());
        assert!(cutover.abandoned.is_empty());
        assert_eq!(cutover.reclaimed_bytes, source.used_bytes);

        for moved in &moves {
            match store.get_object_layout(&moved.object).await.unwrap() {
                Some(ObjectDataLayout::PackedSegment(placement)) => {
                    assert_eq!(placement.segment_id, destination.segment_id);
                }
                other => panic!("expected PackedSegment layout, got {other:?}"),
            }
        }

        let retired = store.get_packed_segment(source.segment_id).await.unwrap().unwrap();
        assert_eq!(retired.state, PackedSegmentState::Dead);
        assert_eq!(retired.dead_at, Some(now));

        let replacement = store.get_packed_segment(destination.segment_id).await.unwrap().unwrap();
        assert_eq!(replacement.state, PackedSegmentState::Purged);
    }

    #[tokio::test]
    async fn a_repack_cutover_leaves_an_object_that_moved_on_where_it_is() {
        let (_dir, store, source, destination, moves) = prepare_repack_fixture(2).await;
        store.delete_object_layout(&moves[0].object).await.unwrap();

        let now = 1000;
        let cutover = store.commit_packed_repack(&source, &destination, &moves, now).await.unwrap();

        assert_eq!(cutover.abandoned, vec![moves[0].object.clone()]);
        assert!(!cutover.switched.contains(&moves[0].object));
        assert!(store.get_object_layout(&moves[0].object).await.unwrap().is_none());

        let replacement = store.get_packed_segment(destination.segment_id).await.unwrap().unwrap();
        assert_eq!(replacement.dead_bytes, moves[0].placement.length);
        assert!(replacement.entry_for(&moves[0].object).is_none());
    }

    #[tokio::test]
    async fn a_repack_cutover_retires_a_replacement_nothing_reached() {
        let (_dir, store, source, destination, moves) = prepare_repack_fixture(2).await;
        for moved in &moves {
            store.delete_object_layout(&moved.object).await.unwrap();
        }

        let now = 1000;
        let cutover = store.commit_packed_repack(&source, &destination, &moves, now).await.unwrap();

        assert_eq!(cutover.retired_destinations, vec![destination.segment_id]);

        let replacement = store.get_packed_segment(destination.segment_id).await.unwrap().unwrap();
        assert_eq!(replacement.state, PackedSegmentState::Dead);
        assert_eq!(replacement.dead_at, Some(now));

        let retired = store.get_packed_segment(source.segment_id).await.unwrap().unwrap();
        assert_eq!(retired.state, PackedSegmentState::Dead);
        assert_eq!(retired.dead_at, Some(now));
    }

    #[tokio::test]
    async fn a_repack_cutover_refuses_a_source_that_is_not_purged() {
        let (_dir, store, mut source, destination, moves) = prepare_repack_fixture(2).await;
        source.state = PackedSegmentState::Encoded;
        let err = store
            .commit_packed_repack(&source, &destination, &moves, 1000)
            .await
            .unwrap_err();
        assert!(matches!(err, EcMetadataError::InvalidLayout { .. }));
    }

    #[tokio::test]
    async fn a_repack_cutover_refuses_a_replacement_that_is_not_encoded() {
        let (_dir, store, source, mut destination, moves) = prepare_repack_fixture(2).await;
        destination.state = PackedSegmentState::Sealed;
        let err = store
            .commit_packed_repack(&source, &destination, &moves, 1000)
            .await
            .unwrap_err();
        assert!(matches!(err, EcMetadataError::InvalidLayout { .. }));
    }

    #[tokio::test]
    async fn a_repack_cutover_refuses_a_replacement_of_another_segment() {
        let (_dir, store, source, mut destination, moves) = prepare_repack_fixture(2).await;
        destination.repacked_from = Some(PackedSegmentId::new_random());
        let err = store
            .commit_packed_repack(&source, &destination, &moves, 1000)
            .await
            .unwrap_err();
        assert!(matches!(err, EcMetadataError::InvalidLayout { .. }));
    }

    #[tokio::test]
    async fn a_repack_cutover_refuses_a_move_that_does_not_match_the_index() {
        let (_dir, store, source, destination, mut moves) = prepare_repack_fixture(2).await;
        moves[0].placement.offset += 1;
        let err = store
            .commit_packed_repack(&source, &destination, &moves, 1000)
            .await
            .unwrap_err();
        assert!(matches!(err, EcMetadataError::InvalidLayout { .. }));
    }

    #[tokio::test]
    async fn a_repack_cutover_refuses_a_move_that_still_names_hot_copies() {
        let (_dir, store, source, destination, mut moves) = prepare_repack_fixture(2).await;
        moves[0].placement.hot_replica_nodes = vec![node(1), node(2), node(3)];
        let err = store
            .commit_packed_repack(&source, &destination, &moves, 1000)
            .await
            .unwrap_err();
        assert!(matches!(err, EcMetadataError::InvalidLayout { .. }));
    }

    #[tokio::test]
    async fn packed_segment_referrers_lists_only_objects_that_still_point_at_the_segment() {
        let (_dir, store, source, destination, moves) = prepare_repack_fixture(3).await;
        let all_objects: Vec<_> = moves.iter().map(|moved| moved.object.clone()).collect();

        let source_refs_before = store.packed_segment_referrers(&source, 10).await.unwrap();
        assert_eq!(source_refs_before, all_objects);

        let dest_refs_before = store.packed_segment_referrers(&destination, 10).await.unwrap();
        assert!(dest_refs_before.is_empty());

        store.commit_packed_repack(&source, &destination, &moves, 1000).await.unwrap();

        let source_refs_after = store.packed_segment_referrers(&source, 10).await.unwrap();
        assert!(source_refs_after.is_empty());

        let dest_refs_after = store.packed_segment_referrers(&destination, 10).await.unwrap();
        assert_eq!(dest_refs_after, all_objects);
    }

    #[tokio::test]
    async fn an_empty_segment_is_retired_without_moving_anything() {
        let dir = tempfile::tempdir().unwrap();
        let store = EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap();
        let config = PackedSegmentConfig::default();
        let mut segment = PackedSegment::open(PackedSegmentId::new_random(), 1, &config, 100);
        segment.seal(200, SealReason::OperatorRequest).unwrap();
        segment.transition_to(PackedSegmentState::Encoded).unwrap();
        segment.transition_to(PackedSegmentState::Purged).unwrap();
        store.put_packed_segment(&segment).await.unwrap();

        let now = 5000;
        store.retire_empty_packed_segment(&segment, now).await.unwrap();

        let retired = store
            .get_packed_segment(segment.segment_id)
            .await
            .unwrap()
            .expect("retired segment must be stored");
        assert_eq!(retired.state, PackedSegmentState::Dead);
        assert_eq!(retired.dead_at, Some(now));
    }

    #[tokio::test]
    async fn a_segment_with_live_objects_is_not_retired_as_empty() {
        let (_dir, store, source, _destination, _moves) = prepare_repack_fixture(2).await;
        let err = store.retire_empty_packed_segment(&source, 5000).await.unwrap_err();
        assert!(matches!(err, EcMetadataError::InvalidLayout { .. }));
    }
}
