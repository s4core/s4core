// Copyright 2026 S4Core Team
//
// Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

//! Enterprise Erasure Coding operations and admin-service implementation.
//!
//! This module is the Phase 9 operational layer. It keeps auto-tiering disabled
//! by default, exposes bounded admin diagnostics, and wires metrics without
//! changing the Community Edition data path.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use s4_cluster::{
    rpc::proto, ClusterError, DistributedListCoordinator, EcAdminAddSetRequest,
    EcAdminApplyTopologyRequest, EcAdminApproveRejoinRequest, EcAdminCodecHookRequest,
    EcAdminDeactivateSetRequest, EcAdminDedupReportRequest, EcAdminObjectRef,
    EcAdminOrphanGcRequest, EcAdminRepairRequest, EcAdminRepairStatusRequest, EcAdminService,
    EcAdminTestHookRequest, EcAdminTieringDryRunRequest, EcAdminTieringPolicyRequest,
    EcAdminTieringPromoteRequest, EcAdminTombstoneGcRequest, EcAdminTranscodePauseRequest,
    EcAdminUnpackSegmentsRequest, EcContentDigest, EcContentDigestPage, EcContentDigestScan,
    EcHeatQuery, EcObjectDeleteCommit, EcObjectHeadRequest, EcObjectHeadResult, EcObjectHeatMark,
    EcObjectPutCommit, EcObjectPutDisposition, EcObjectReadError, EcObjectReadLayout,
    EcObjectReadRequest, EcObjectReadResult, EcObjectRuntime, HlcTimestamp, KeyRange, ListEntry,
    NodeClient, NodeId, NodeIdentityOrigin, QuorumHeadResult, QuorumReadCoordinator,
    QuorumReadResult, QuorumWriteCoordinator, RepairFrontier,
};
use s4_codec::CodecEngineKind;
use s4_core::types::OBJECT_TAGS_METADATA_KEY;
use s4_core::{
    IndexRecord, StorageEngine, EXTERNAL_DATA_FILE_ID, FORCE_PHYSICAL_WRITE_METADATA_KEY,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::io::AsyncReadExt;
use tokio::task::JoinSet;
use uuid::Uuid;

use crate::codec_runtime::{EcCodecCpuConfig, EcCodecCpuLimiter};
use crate::ec_anti_entropy::{
    collect_layout_drift, EcAntiEntropyConfig, EcAntiEntropyError,
    EcAntiEntropyMetadataCoordinator, EcAntiEntropyService, EcLayoutDriftReport, EcLocalShardRead,
    EcLocalShardReadRequest, EcLocalShardReadResponse, EcLocalShardReader,
    EcReconstructionSampleRequest, EcReconstructionSampleResult, EcReconstructionSampleStatus,
    EcReconstructionVerifier, EcShardInventoryAuditReport, EcShardQuarantine,
    EcShardQuarantineRequest, EcShardScrubReport,
};
use crate::ec_dedup::{build_dedup_report, EcDedupScanFacts, EcDedupUnreadNode};
use crate::ec_gc::{
    EcDeadShardPurgeKind, EcDeadShardPurgeRequest, EcDeadShardPurger, EcDeleteOutcome,
    EcDeleteRequest, EcDeletedLayout, EcGcConfig, EcGcCoordinator, EcGcError,
    EcGcMetadataCoordinator, EcOrphanShardObservation, EcPackedSegmentUnpacker,
    EcReplicatedSmallDeleteRequest, EcReplicatedSmallDeleteResult, EcReplicatedSmallDeleter,
    EcReplicatedSmallPurgeRequest, EcReplicatedSmallPurgeResult, EcTombstoneCommitResult,
    EcTombstoneGcReport, EcTombstoneGcSafety, EcTombstonePropagation, EcTombstoneQuorum,
    NoObjectLockRetention, PackedRepackPolicy, PackedSegmentRetentionSource,
};
use crate::ec_metadata::{
    decode_manifest, decode_replicated_small_placement, encode_manifest,
    encode_replicated_small_placement, manifest_storage_hlc, packed_segment_error, ChunkManifest,
    EcErasureSetRoster, EcErasureSetSpec, EcHotReplicaGcRecord, EcManifestReadResult,
    EcMetadataError, EcMetadataStore, EcNodePresenceRecord, EcObjectManifest, EcObjectManifestRef,
    EcObjectVersionKey, EcOrphanShardRecord, EcPlacementState, EcPromotionJobRecord,
    EcPromotionOrigin, EcPromotionState, EcRepairKind, EcRepairStatus, EcRepairTaskRecord,
    EcSetLayoutRecord, EcSmallPlacementHint, EcTieringCursorRecord, EcTieringPolicyRecord,
    EcTombstoneGcCursorRecord, EcTranscodeJobRecord, EcTranscodeJobsRemoved, EcTranscodeOrigin,
    EcTranscodeTarget, LocalShardKey, LocalShardRecord, LocalShardState, ObjectDataLayout,
    PackedSegmentObjectPlacement, ReplicatedSmallObjectPlacement, ShardManifest,
    REPLICATED_SMALL_REPLICA_COUNT,
};
use crate::ec_metrics;
use crate::ec_node_rebuild::{
    EcNodeRebuildInventory, EcNodeRepairQueueOutcome, EcRebuildQueueStop, EcShardPresence,
};
use crate::ec_packed_segments::{
    packed_segment_manifest_key, PackedObjectIdentity, PackedSegment, PackedSegmentAdmission,
    PackedSegmentConfig, PackedSegmentEntry, PackedSegmentId, PackedSegmentJournalItem,
    PackedSegmentJournalPayload, PackedSegmentPool, PackedSegmentState,
};
use crate::ec_placement::{
    EcClusterCapacity, EcErasureSets, EcPlacementError, EcPlacementService, EcSetCapacityReport,
};
use crate::ec_read::{
    EcByteRange, EcHeadObject, EcObjectLayoutReadResult, EcReadCoordinator, EcReadError,
    EcReadLayout, EcReadMetadataCoordinator, EcReadRequest, EcReadShardRequest,
    EcReadShardResponse, EcReplicatedSmallHeadRequest, EcReplicatedSmallReadRequest,
    EcReplicatedSmallReadResult, EcReplicatedSmallReader, EcShardReader, NoopEcReadRepairQueue,
};
use crate::ec_rejoin::{rejoin_decision_label, EcNodeReturn, EcRejoinGate, EcRejoinStatus};
use crate::ec_repair::{
    EcRepairConfig, EcRepairCoordinator, EcRepairError, EcRepairFrontierUpdate,
    EcRepairFrontierUpdater, EcRepairMetadataCoordinator, EcRepairQueueRequest,
    EcRepairShardReadRequest, EcRepairShardReadResponse, EcRepairShardReader,
    EcRepairShardWriteAck, EcRepairShardWriteRequest, EcRepairShardWriter,
};
use crate::ec_runtime::{
    derive_storage_operation_id, deterministic_object_uuid, erasure_set_roster_object_key,
    is_not_found_message, manifest_object_key, manifest_wins, replicated_small_object_key,
    set_layout_record_object_key, sha256, shard_object_key, tiering_policy_object_key,
    EcTranscodeRuntimeDeps, EcTranscodeWorkerConfig, EcTranscodeWorkerRuntime, SYSTEM_BUCKET,
};
use crate::ec_tiering::heat::{self, EcHeatConfig, EcHeatTracker};
use crate::ec_tiering::policy::{
    decide, TieringCandidate, TieringDecision, TieringGlobals, TieringPolicy, TieringSkipReason,
};
use crate::ec_tiering::promote::{
    EcPromotionCycleReport, EcPromotionEnvironment, EcPromotionWorker, EcPromotionWorkerConfig,
};
use crate::ec_tiering::worker::{
    EcTieringCycleReport, EcTieringCycleStop, EcTieringEnvironment, EcTieringWorker,
    EcTieringWorkerConfig, PoolActivity, TieringCatalogEntry, TieringCatalogPage,
};
use crate::ec_transcode::{content_hash_is_absent, deterministic_operation_id, EcTranscodeConfig};
use crate::erasure_coding::{
    EcConfigError, EcNewErasureSet, EcSetLayoutRule, ErasureSet, ErasureSetBuilder,
    ErasureSetRegistry, ErasureSetState, LayoutViolationOrigin, PoolTopologyView,
    SetDomainPlacement, SetLayoutCompliance, FIRST_SET_ID, LRC_GROUP_DOMAIN_ENV,
};

/// Default number of EC metadata records scanned by one admin request.
pub const DEFAULT_EC_ADMIN_SCAN_LIMIT: usize = 4_096;

/// Manifests read per page by pool-wide admin walks.
///
/// The walk's total is `S4_EC_ADMIN_SCAN_LIMIT`; this is only how much of it is
/// held in memory at a time, so that a pool with a large limit is walked in
/// steady steps rather than in one allocation.
const EC_ADMIN_MANIFEST_PAGE: usize = 256;

/// Shard presence questions a rebuild inventory keeps in flight at once.
///
/// Bounded because the node being asked is, by the nature of the question,
/// quite possibly the one already having a bad day.
const EC_SHARD_PROBES_IN_FLIGHT: usize = 32;
/// How often a node re-reads the pool's erasure-set roster, in seconds.
pub const DEFAULT_ERASURE_SET_REFRESH_SECS: u64 = 30;
/// Environment variable overriding [`DEFAULT_ERASURE_SET_REFRESH_SECS`].
pub const ERASURE_SET_REFRESH_SECS_ENV: &str = "S4_EC_SET_REFRESH_SECS";
const EC_MANIFEST_CONTENT_TYPE: &str = "application/vnd.s4.ec.manifest+bin";
const EC_SHARD_CONTENT_TYPE: &str = "application/vnd.s4.ec.shard";
const EC_SMALL_PLACEMENT_CONTENT_TYPE: &str = "application/vnd.s4.ec.small+bin";
const EC_TIERING_POLICY_CONTENT_TYPE: &str = "application/vnd.s4.ec.tiering-policy+bin";
const EC_ERASURE_SET_ROSTER_CONTENT_TYPE: &str = "application/vnd.s4.ec.erasure-sets+bin";
const EC_SET_LAYOUT_CONTENT_TYPE: &str = "application/vnd.s4.ec.erasure-set-layout+bin";
/// Objects fetched per catalog page while a dry run walks a bucket.
const DEFAULT_EC_ADMIN_SCAN_PAGE: usize = 256;
/// Default cold-object age before optional auto-tiering considers an object.
pub const DEFAULT_EC_TIERING_COLD_AFTER_SECS: u64 = 30 * 24 * 3_600;
/// Default accuracy of the access mark kept for one object (decision EC2-020).
const DEFAULT_EC_TIERING_HEAT_RESOLUTION_SECS: u64 = 3_600;
/// Default interval between two flushes of collapsed access signals.
const DEFAULT_EC_TIERING_HEAT_FLUSH_SECS: u64 = 60;
/// Default capacity of the bounded channel that carries access signals.
const DEFAULT_EC_TIERING_HEAT_QUEUE_CAPACITY: usize = 4_096;
/// Most buckets one tiering sweep enumerates.
///
/// A pool with more buckets than this would have its tail never examined; the
/// limit is deliberately far above any realistic bucket count so that hitting
/// it is a signal, not routine.
const MAX_TIERING_BUCKETS: usize = 10_000;
/// Default pause between two tiering scan cycles.
const DEFAULT_EC_TIERING_SCAN_INTERVAL_SECS: u64 = 3_600;
/// Default number of objects one scan cycle may queue for transcode.
const DEFAULT_EC_TIERING_ENQUEUE_PER_CYCLE: usize = 64;
/// Default ceiling of outstanding tiering-originated transcode jobs.
const DEFAULT_EC_TIERING_MAX_QUEUE_DEPTH: usize = 1_024;
/// Default spread applied to the cold-after threshold, in percent.
const DEFAULT_EC_TIERING_JITTER_PERCENT: u8 = 10;
/// Default window used to count reads for reverse tiering.
const DEFAULT_EC_TIERING_PROMOTE_WINDOW_SECS: u64 = 24 * 3_600;
/// Default minimum pause between two migrations of the same object.
const DEFAULT_EC_TIERING_MIN_MIGRATION_INTERVAL_SECS: u64 = 7 * 24 * 3_600;
/// Default number of objects one scan cycle may send back to RF=3.
///
/// Smaller than the forward limit because a promotion is the more expensive
/// direction: it decodes a whole object and then writes three copies of it,
/// where a migration writes shards that add up to half the size.
const DEFAULT_EC_TIERING_PROMOTE_PER_CYCLE: usize = 16;
/// Default ceiling of outstanding reverse migrations.
const DEFAULT_EC_TIERING_MAX_PROMOTION_QUEUE_DEPTH: usize = 256;
/// Default pause between two passes over the reverse migration queue.
const DEFAULT_EC_TIERING_PROMOTE_POLL_SECS: u64 = 60;
/// Default number of reverse migrations advanced by one pass.
const DEFAULT_EC_TIERING_PROMOTE_BATCH_SIZE: usize = 8;
/// Default attempts one reverse migration gets before it is given up on.
const DEFAULT_EC_TIERING_PROMOTE_MAX_ATTEMPTS: u32 = 10;
/// Default pause before a failed reverse migration step is tried again.
const DEFAULT_EC_TIERING_PROMOTE_RETRY_BACKOFF_SECS: u64 = 60;
const DEFAULT_EC_REPAIR_WORKER_POLL_MS: u64 = 1_000;
const DEFAULT_EC_REPAIR_WORKER_BATCH_SIZE: usize = 64;
const DEFAULT_EC_SCRUB_WORKER_INTERVAL_SECS: u64 = 60;
const DEFAULT_EC_AE_WORKER_INTERVAL_SECS: u64 = 60;
/// How often a node looks for deleted objects whose shards it may release.
///
/// Five minutes rather than the anti-entropy minute: the work behind it is a
/// poll of every owner per tombstone, and nothing is waiting on it — the
/// object is already gone from every client's point of view, and what the
/// sweep reclaims is disk.
const DEFAULT_EC_TOMBSTONE_GC_WORKER_INTERVAL_SECS: u64 = 300;
const EC_V1_CI_ACCEPTANCE_ENV: &str = "S4_EC_V1_CI_ACCEPTANCE_PASSED";

/// Environment variable that decides how much of a chunk a serving read pulls.
pub const MINIMAL_READ_ENABLED_ENV: &str = "S4_EC_MINIMAL_READ_ENABLED";

/// Whether a serving read pulls only the `k` data shards it assembles from.
///
/// On by default. It went out on rather than off after the saving had been
/// measured and not before: `n/k` → 1.0 on the bytes and on the round trips of
/// every standard profile, with cross-zone traffic falling in the same
/// proportion (decision EC2-183). Off restores the EC v1 read, which fetches
/// and verifies all `n` shards of every chunk it touches.
///
/// The switch is per node and changes nothing on disk, so a pool may run with
/// it set either way on any of its nodes.
const DEFAULT_EC_MINIMAL_READ_ENABLED: bool = true;

/// Environment variable holding how long "this object has no placement record"
/// is remembered.
pub const SMALL_PLACEMENT_NEGATIVE_TTL_ENV: &str = "S4_EC_SMALL_PLACEMENT_NEGATIVE_TTL_SECS";

/// How long a node remembers that an object has no replicated-small placement.
///
/// Every object the hot path still holds answers "no manifest" on this path, so
/// without a memory of the miss each of its reads would pay the resolve again.
/// A minute is short enough that an object placed elsewhere in the meantime is
/// picked up promptly, and long enough to take the cost off a read loop. `0`
/// turns the memory off (decision EC2-201).
const DEFAULT_EC_SMALL_PLACEMENT_NEGATIVE_TTL_SECS: u64 = 60;

/// Most misses a node remembers at once.
///
/// A bound rather than an eviction policy: the map holds one key and one
/// instant per object, it is only ever consulted on the path that would
/// otherwise fan out, and dropping it costs one resolve.
const EC_SMALL_PLACEMENT_MISS_CAPACITY: usize = 4_096;

/// Optional auto-tiering configuration.
///
/// EC v1 shipped the first three fields as a placeholder. EC v2 plan 02 extends
/// this struct instead of adding a second one, so that the cross-check against
/// `S4_EC_TRANSCODE_ENABLED` and the admin status snapshot keep a single source
/// of truth. Decisions: EC2-020 (activity tracking), EC2-021 (policies),
/// EC2-022 (rate limiting), EC2-023 (reverse tiering).
///
/// Every field added by plan 02 is inert until the phase that reads it ships.
/// Their defaults are chosen so that turning tiering on does not change how a
/// node behaves under user load.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EcTieringConfig {
    /// Whether auto-tiering is enabled.
    pub enabled: bool,
    /// Idle time before an object may be queued for staged EC transcode.
    ///
    /// EC v1 had nothing to compare this against but the object's creation
    /// time. Plan 02 measures it from the last recorded access instead, and
    /// falls back to creation time when no access has been recorded.
    pub cold_after_secs: u64,
    /// Maximum objects examined by one tiering cycle.
    ///
    /// This limits reading. What may be queued is limited separately, because
    /// examining an object is cheap and queueing one is not.
    pub scan_limit: usize,
    /// Whether object access marks are recorded at all.
    ///
    /// Defaults to `enabled`, so a node that does not tier keeps exactly the
    /// behaviour it had before EC v2. Setting it while `enabled` stays false
    /// lets an operator collect access data before the first migration.
    pub heat_enabled: bool,
    /// How much newer an access mark has to be before it is written durably.
    ///
    /// Every metadata write costs an fsync, so an hour of accuracy against a
    /// threshold measured in days is a deliberate trade.
    pub heat_resolution_secs: u64,
    /// Interval between two flushes of collapsed access signals.
    pub heat_flush_interval_secs: u64,
    /// Capacity of the bounded channel that carries access signals.
    ///
    /// A full channel drops signals instead of blocking the S3 response.
    pub heat_queue_capacity: usize,
    /// Pause between two tiering scan cycles.
    pub scan_interval_secs: u64,
    /// Objects one scan cycle may queue for transcode.
    pub enqueue_per_cycle: usize,
    /// Ceiling of outstanding tiering-originated transcode jobs.
    ///
    /// Jobs created by user writes are not counted against this ceiling.
    pub max_queue_depth: usize,
    /// Deterministic spread applied to the cold-after threshold, in percent.
    ///
    /// Keeps a single ingest burst from cooling down all at once.
    pub jitter_percent: u8,
    /// Reads within the promotion window that bring an object back to RF=3.
    ///
    /// Zero disables automatic reverse tiering; the manual admin endpoint keeps
    /// working, because it is the documented rollback for the migration phase.
    pub promote_after_reads: u32,
    /// Width of the window used to count reads for reverse tiering.
    pub promote_window_secs: u64,
    /// Base pause between two migrations of the same object.
    pub min_migration_interval_secs: u64,
    /// Objects one scan cycle may send back to RF=3.
    pub promote_per_cycle: usize,
    /// Ceiling of outstanding reverse migrations.
    ///
    /// Counted apart from the forward queue on purpose: sharing one ceiling
    /// would make the manual rollback impossible exactly when it is needed,
    /// with the forward queue full of the migrations being undone.
    pub max_promotion_queue_depth: usize,
    /// Pause between two passes over the reverse migration queue.
    pub promote_poll_secs: u64,
    /// Reverse migrations advanced by one pass.
    pub promote_batch_size: usize,
    /// Attempts one reverse migration gets before it is given up on.
    pub promote_max_attempts: u32,
    /// Pause before a failed reverse migration step is tried again.
    pub promote_retry_backoff_secs: u64,
    /// Whether new client writes stay on RF=3 replicas instead of being
    /// queued for EC immediately.
    ///
    /// Without this an EC pool encodes every object on its way in, and there is
    /// no replicated population left for auto-tiering to migrate. Turning it on
    /// is what makes the feature mean "set and forget": the object is written
    /// as an ordinary replicated object, and the tiering worker encodes it once
    /// it has gone cold. Defaults to false, so an existing pool keeps encoding
    /// on write exactly as it did before.
    pub defer_new_writes: bool,
}

impl EcTieringConfig {
    /// Parse `S4_EC_TIERING_*` settings.
    pub fn from_env() -> Result<Self, ClusterError> {
        let enabled = parse_optional_bool("S4_EC_TIERING_ENABLED")?.unwrap_or(false);
        let cold_after_secs = parse_optional_u64("S4_EC_TIERING_COLD_AFTER_SECS")?
            .unwrap_or(DEFAULT_EC_TIERING_COLD_AFTER_SECS);
        let scan_limit = parse_optional_usize("S4_EC_TIERING_SCAN_LIMIT")?
            .unwrap_or(DEFAULT_EC_ADMIN_SCAN_LIMIT);
        // The one-day floor from EC2-021 belongs to bucket policies, not here:
        // raising the floor on this variable would stop existing nodes booting.
        if cold_after_secs == 0 {
            return Err(invalid_config(
                "S4_EC_TIERING_COLD_AFTER_SECS",
                "must be greater than 0",
            ));
        }
        if scan_limit == 0 {
            return Err(invalid_config(
                "S4_EC_TIERING_SCAN_LIMIT",
                "must be greater than 0",
            ));
        }

        let heat_enabled = parse_optional_bool("S4_EC_TIERING_HEAT_ENABLED")?.unwrap_or(enabled);
        let heat_resolution_secs = parse_optional_u64("S4_EC_TIERING_HEAT_RESOLUTION_SECS")?
            .unwrap_or(DEFAULT_EC_TIERING_HEAT_RESOLUTION_SECS);
        let heat_flush_interval_secs = parse_optional_u64("S4_EC_TIERING_HEAT_FLUSH_SECS")?
            .unwrap_or(DEFAULT_EC_TIERING_HEAT_FLUSH_SECS);
        let heat_queue_capacity = parse_optional_usize("S4_EC_TIERING_HEAT_QUEUE_CAPACITY")?
            .unwrap_or(DEFAULT_EC_TIERING_HEAT_QUEUE_CAPACITY);
        let scan_interval_secs = parse_optional_u64("S4_EC_TIERING_SCAN_INTERVAL_SECS")?
            .unwrap_or(DEFAULT_EC_TIERING_SCAN_INTERVAL_SECS);
        let max_queue_depth = parse_optional_usize("S4_EC_TIERING_MAX_QUEUE_DEPTH")?
            .unwrap_or(DEFAULT_EC_TIERING_MAX_QUEUE_DEPTH);
        // An explicit per-cycle limit is checked against its ceilings below. The
        // default adapts to them instead, so that a node which already sets a
        // small scan limit keeps starting.
        let explicit_enqueue_per_cycle = parse_optional_usize("S4_EC_TIERING_ENQUEUE_PER_CYCLE")?;
        let enqueue_per_cycle = explicit_enqueue_per_cycle.unwrap_or_else(|| {
            DEFAULT_EC_TIERING_ENQUEUE_PER_CYCLE.min(scan_limit).min(max_queue_depth)
        });
        let jitter_percent = parse_optional_u8("S4_EC_TIERING_JITTER_PERCENT")?
            .unwrap_or(DEFAULT_EC_TIERING_JITTER_PERCENT);
        let promote_after_reads =
            parse_optional_u32("S4_EC_TIERING_PROMOTE_AFTER_READS")?.unwrap_or(0);
        let promote_window_secs = parse_optional_u64("S4_EC_TIERING_PROMOTE_WINDOW_SECS")?
            .unwrap_or(DEFAULT_EC_TIERING_PROMOTE_WINDOW_SECS);
        let min_migration_interval_secs =
            parse_optional_u64("S4_EC_TIERING_MIN_MIGRATION_INTERVAL_SECS")?
                .unwrap_or(DEFAULT_EC_TIERING_MIN_MIGRATION_INTERVAL_SECS);
        let promote_per_cycle = parse_optional_usize("S4_EC_TIERING_PROMOTE_PER_CYCLE")?
            .unwrap_or(DEFAULT_EC_TIERING_PROMOTE_PER_CYCLE);
        let max_promotion_queue_depth =
            parse_optional_usize("S4_EC_TIERING_MAX_PROMOTION_QUEUE_DEPTH")?
                .unwrap_or(DEFAULT_EC_TIERING_MAX_PROMOTION_QUEUE_DEPTH);
        let promote_poll_secs = parse_optional_u64("S4_EC_TIERING_PROMOTE_POLL_SECS")?
            .unwrap_or(DEFAULT_EC_TIERING_PROMOTE_POLL_SECS);
        let promote_batch_size = parse_optional_usize("S4_EC_TIERING_PROMOTE_BATCH_SIZE")?
            .unwrap_or(DEFAULT_EC_TIERING_PROMOTE_BATCH_SIZE);
        let promote_max_attempts = parse_optional_u32("S4_EC_TIERING_PROMOTE_MAX_ATTEMPTS")?
            .unwrap_or(DEFAULT_EC_TIERING_PROMOTE_MAX_ATTEMPTS);
        let promote_retry_backoff_secs =
            parse_optional_u64("S4_EC_TIERING_PROMOTE_RETRY_BACKOFF_SECS")?
                .unwrap_or(DEFAULT_EC_TIERING_PROMOTE_RETRY_BACKOFF_SECS);
        let defer_new_writes =
            parse_optional_bool("S4_EC_TIERING_DEFER_NEW_WRITES")?.unwrap_or(false);

        for (name, value) in [
            ("S4_EC_TIERING_HEAT_RESOLUTION_SECS", heat_resolution_secs),
            ("S4_EC_TIERING_HEAT_FLUSH_SECS", heat_flush_interval_secs),
            ("S4_EC_TIERING_SCAN_INTERVAL_SECS", scan_interval_secs),
            ("S4_EC_TIERING_PROMOTE_WINDOW_SECS", promote_window_secs),
            (
                "S4_EC_TIERING_MIN_MIGRATION_INTERVAL_SECS",
                min_migration_interval_secs,
            ),
            ("S4_EC_TIERING_PROMOTE_POLL_SECS", promote_poll_secs),
            (
                "S4_EC_TIERING_PROMOTE_RETRY_BACKOFF_SECS",
                promote_retry_backoff_secs,
            ),
        ] {
            if value == 0 {
                return Err(invalid_config(name, "must be greater than 0"));
            }
        }
        for (name, value) in [
            ("S4_EC_TIERING_HEAT_QUEUE_CAPACITY", heat_queue_capacity),
            ("S4_EC_TIERING_ENQUEUE_PER_CYCLE", enqueue_per_cycle),
            ("S4_EC_TIERING_MAX_QUEUE_DEPTH", max_queue_depth),
            ("S4_EC_TIERING_PROMOTE_PER_CYCLE", promote_per_cycle),
            (
                "S4_EC_TIERING_MAX_PROMOTION_QUEUE_DEPTH",
                max_promotion_queue_depth,
            ),
            ("S4_EC_TIERING_PROMOTE_BATCH_SIZE", promote_batch_size),
        ] {
            if value == 0 {
                return Err(invalid_config(name, "must be greater than 0"));
            }
        }

        // A retry budget of zero would give up on the first transient error and
        // leave the object holding two full copies of itself.
        if promote_max_attempts == 0 {
            return Err(invalid_config(
                "S4_EC_TIERING_PROMOTE_MAX_ATTEMPTS",
                "must be greater than 0",
            ));
        }
        // Same reasoning as the forward pair: a per-cycle limit above its
        // ceiling is silently clamped, and saying so beats leaving the operator
        // to wonder why the observed rate is lower than the one configured.
        if promote_per_cycle > max_promotion_queue_depth {
            return Err(invalid_config(
                "S4_EC_TIERING_PROMOTE_PER_CYCLE",
                "must not exceed S4_EC_TIERING_MAX_PROMOTION_QUEUE_DEPTH",
            ));
        }
        if jitter_percent > 100 {
            return Err(invalid_config(
                "S4_EC_TIERING_JITTER_PERCENT",
                "must not exceed 100",
            ));
        }
        // Deferring writes without a worker to pick them up would leave every
        // new object replicated forever. That is a durability and cost decision
        // the operator has not made, so it is refused rather than assumed.
        if defer_new_writes && !enabled {
            return Err(invalid_config(
                "S4_EC_TIERING_DEFER_NEW_WRITES",
                "requires S4_EC_TIERING_ENABLED=true, otherwise nothing would ever encode",
            ));
        }
        // Both ceilings would silently clamp the per-cycle value. Refusing to
        // start says which knob is ignored instead of leaving the operator to
        // wonder why the observed rate is lower than the one configured.
        if explicit_enqueue_per_cycle.is_some() {
            if enqueue_per_cycle > max_queue_depth {
                return Err(invalid_config(
                    "S4_EC_TIERING_ENQUEUE_PER_CYCLE",
                    "must not exceed S4_EC_TIERING_MAX_QUEUE_DEPTH",
                ));
            }
            if enqueue_per_cycle > scan_limit {
                return Err(invalid_config(
                    "S4_EC_TIERING_ENQUEUE_PER_CYCLE",
                    "must not exceed S4_EC_TIERING_SCAN_LIMIT",
                ));
            }
        }

        Ok(Self {
            enabled,
            cold_after_secs,
            scan_limit,
            heat_enabled,
            heat_resolution_secs,
            heat_flush_interval_secs,
            heat_queue_capacity,
            scan_interval_secs,
            enqueue_per_cycle,
            max_queue_depth,
            jitter_percent,
            promote_after_reads,
            promote_window_secs,
            min_migration_interval_secs,
            promote_per_cycle,
            max_promotion_queue_depth,
            promote_poll_secs,
            promote_batch_size,
            promote_max_attempts,
            promote_retry_backoff_secs,
            defer_new_writes,
        })
    }

    /// Settings the activity tracker needs, taken from this configuration.
    ///
    /// The tracker does not read `EcTieringConfig` itself: it lives in the
    /// tiering domain module and only needs these five values.
    ///
    /// Note which window this hands over: the node's, not a bucket's. It is the
    /// width the ring of counters is built at, and a bucket policy may ask for
    /// a narrower one. That is answered by reading fewer slots of the same
    /// ring, so a bucket never promotes on reads older than it asked for, and a
    /// bucket asking for a wider window gets what the node actually recorded.
    pub fn heat_config(&self) -> EcHeatConfig {
        EcHeatConfig {
            enabled: self.heat_enabled,
            resolution: Duration::from_secs(self.heat_resolution_secs),
            flush_interval: Duration::from_secs(self.heat_flush_interval_secs),
            queue_capacity: self.heat_queue_capacity,
            window: Duration::from_secs(self.promote_window_secs),
        }
    }
}

impl Default for EcTieringConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            cold_after_secs: DEFAULT_EC_TIERING_COLD_AFTER_SECS,
            scan_limit: DEFAULT_EC_ADMIN_SCAN_LIMIT,
            heat_enabled: false,
            heat_resolution_secs: DEFAULT_EC_TIERING_HEAT_RESOLUTION_SECS,
            heat_flush_interval_secs: DEFAULT_EC_TIERING_HEAT_FLUSH_SECS,
            heat_queue_capacity: DEFAULT_EC_TIERING_HEAT_QUEUE_CAPACITY,
            scan_interval_secs: DEFAULT_EC_TIERING_SCAN_INTERVAL_SECS,
            enqueue_per_cycle: DEFAULT_EC_TIERING_ENQUEUE_PER_CYCLE,
            max_queue_depth: DEFAULT_EC_TIERING_MAX_QUEUE_DEPTH,
            jitter_percent: DEFAULT_EC_TIERING_JITTER_PERCENT,
            promote_after_reads: 0,
            promote_window_secs: DEFAULT_EC_TIERING_PROMOTE_WINDOW_SECS,
            min_migration_interval_secs: DEFAULT_EC_TIERING_MIN_MIGRATION_INTERVAL_SECS,
            promote_per_cycle: DEFAULT_EC_TIERING_PROMOTE_PER_CYCLE,
            max_promotion_queue_depth: DEFAULT_EC_TIERING_MAX_PROMOTION_QUEUE_DEPTH,
            promote_poll_secs: DEFAULT_EC_TIERING_PROMOTE_POLL_SECS,
            promote_batch_size: DEFAULT_EC_TIERING_PROMOTE_BATCH_SIZE,
            promote_max_attempts: DEFAULT_EC_TIERING_PROMOTE_MAX_ATTEMPTS,
            promote_retry_backoff_secs: DEFAULT_EC_TIERING_PROMOTE_RETRY_BACKOFF_SECS,
            defer_new_writes: false,
        }
    }
}

/// Runtime scheduling for EC repair, anti-entropy, and scrub workers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EcIntegrityWorkerConfig {
    /// Whether the repair worker is started.
    pub repair_enabled: bool,
    /// Whether local EC anti-entropy inventory audits are scheduled.
    pub anti_entropy_enabled: bool,
    /// Whether local EC shard scrub cycles are scheduled.
    pub scrub_enabled: bool,
    /// Poll interval for queued repair work.
    pub repair_poll_interval: Duration,
    /// Maximum repair tasks attempted per poll.
    pub repair_batch_size: usize,
    /// Local shard scrub interval.
    pub scrub_interval: Duration,
    /// Local manifest inventory audit interval.
    pub anti_entropy_interval: Duration,
    /// Whether this node releases the shards of deleted objects on a schedule.
    pub tombstone_gc_enabled: bool,
    /// Interval between tombstone GC cycles.
    pub tombstone_gc_interval: Duration,
}

impl EcIntegrityWorkerConfig {
    /// Parse integrity worker settings from environment variables.
    pub fn from_env() -> Result<Self, ClusterError> {
        let repair_enabled = parse_optional_bool("S4_EC_REPAIR_WORKER_ENABLED")?.unwrap_or(true);
        let anti_entropy_enabled = parse_optional_bool("S4_EC_AE_WORKER_ENABLED")?.unwrap_or(true);
        let scrub_enabled = parse_optional_bool("S4_EC_SCRUB_WORKER_ENABLED")?.unwrap_or(true);
        let tombstone_gc_enabled =
            parse_optional_bool("S4_EC_TOMBSTONE_GC_WORKER_ENABLED")?.unwrap_or(true);

        let repair_poll_ms = parse_optional_u64("S4_EC_REPAIR_WORKER_POLL_MS")?
            .unwrap_or(DEFAULT_EC_REPAIR_WORKER_POLL_MS);
        if repair_poll_ms == 0 {
            return Err(invalid_config(
                "S4_EC_REPAIR_WORKER_POLL_MS",
                "must be greater than 0",
            ));
        }

        let repair_batch_size = parse_optional_usize("S4_EC_REPAIR_WORKER_BATCH_SIZE")?
            .unwrap_or(DEFAULT_EC_REPAIR_WORKER_BATCH_SIZE);
        if repair_batch_size == 0 {
            return Err(invalid_config(
                "S4_EC_REPAIR_WORKER_BATCH_SIZE",
                "must be greater than 0",
            ));
        }

        let scrub_interval_secs = parse_optional_u64("S4_EC_SCRUB_WORKER_INTERVAL_SECS")?
            .unwrap_or(DEFAULT_EC_SCRUB_WORKER_INTERVAL_SECS);
        if scrub_interval_secs == 0 {
            return Err(invalid_config(
                "S4_EC_SCRUB_WORKER_INTERVAL_SECS",
                "must be greater than 0",
            ));
        }

        let anti_entropy_interval_secs = parse_optional_u64("S4_EC_AE_WORKER_INTERVAL_SECS")?
            .unwrap_or(DEFAULT_EC_AE_WORKER_INTERVAL_SECS);
        if anti_entropy_interval_secs == 0 {
            return Err(invalid_config(
                "S4_EC_AE_WORKER_INTERVAL_SECS",
                "must be greater than 0",
            ));
        }

        let tombstone_gc_interval_secs =
            parse_optional_u64("S4_EC_TOMBSTONE_GC_WORKER_INTERVAL_SECS")?
                .unwrap_or(DEFAULT_EC_TOMBSTONE_GC_WORKER_INTERVAL_SECS);
        if tombstone_gc_interval_secs == 0 {
            return Err(invalid_config(
                "S4_EC_TOMBSTONE_GC_WORKER_INTERVAL_SECS",
                "must be greater than 0",
            ));
        }

        Ok(Self {
            repair_enabled,
            anti_entropy_enabled,
            scrub_enabled,
            repair_poll_interval: Duration::from_millis(repair_poll_ms),
            repair_batch_size,
            scrub_interval: Duration::from_secs(scrub_interval_secs),
            anti_entropy_interval: Duration::from_secs(anti_entropy_interval_secs),
            tombstone_gc_enabled,
            tombstone_gc_interval: Duration::from_secs(tombstone_gc_interval_secs),
        })
    }
}

impl Default for EcIntegrityWorkerConfig {
    fn default() -> Self {
        Self {
            repair_enabled: true,
            anti_entropy_enabled: true,
            scrub_enabled: true,
            repair_poll_interval: Duration::from_millis(DEFAULT_EC_REPAIR_WORKER_POLL_MS),
            repair_batch_size: DEFAULT_EC_REPAIR_WORKER_BATCH_SIZE,
            scrub_interval: Duration::from_secs(DEFAULT_EC_SCRUB_WORKER_INTERVAL_SECS),
            anti_entropy_interval: Duration::from_secs(DEFAULT_EC_AE_WORKER_INTERVAL_SECS),
            tombstone_gc_enabled: true,
            tombstone_gc_interval: Duration::from_secs(
                DEFAULT_EC_TOMBSTONE_GC_WORKER_INTERVAL_SECS,
            ),
        }
    }
}

/// High-level EC runtime configuration visible to operations.
///
/// Compared by value but not `Eq`: the repack threshold is a fraction, and a
/// fraction has no total equality to derive.
#[derive(Debug, Clone, PartialEq)]
pub struct EcOperationsConfig {
    /// Whether staged transcode workers may run.
    pub transcode_enabled: bool,
    /// Optional auto-tiering policy.
    pub tiering: EcTieringConfig,
    /// Staged transcode configuration.
    pub transcode: EcTranscodeConfig,
    /// Supervised transcode worker runtime configuration.
    pub transcode_worker: EcTranscodeWorkerConfig,
    /// Repair scheduler configuration.
    pub repair: EcRepairConfig,
    /// Supervised repair/scrub worker runtime configuration.
    pub integrity_worker: EcIntegrityWorkerConfig,
    /// Tombstone/orphan GC configuration.
    pub gc: EcGcConfig,
    /// Anti-entropy and scrub configuration.
    pub anti_entropy: EcAntiEntropyConfig,
    /// Codec CPU limiter configuration.
    pub cpu: EcCodecCpuConfig,
    /// Small-object packing configuration.
    pub packed_segments: PackedSegmentConfig,
    /// When a fragmented packed segment becomes worth repacking.
    pub packed_repack: PackedRepackPolicy,
    /// Maximum EC metadata records scanned by one admin request.
    pub admin_scan_limit: usize,
    /// How often a node re-reads the pool's erasure-set roster.
    ///
    /// An operator adds a set on one node; the others learn about it on their
    /// next pass. Until they do they cannot serve that set's objects, so the
    /// interval is the worst-case lag between "the set exists" and "the pool
    /// serves it", not a background tidy-up.
    pub erasure_set_refresh_interval: Duration,
    /// Whether a node held back by the rejoin verdict actually stops serving
    /// EC reads, or only reports that it should.
    ///
    /// The one switch that makes plan 08 reversible: off, every verdict is
    /// still computed and published and nothing is refused.
    pub rejoin_gate_enforced: bool,
    /// Whether a read that serves bytes pulls only the `k` data shards it
    /// assembles from, instead of every shard of the chunk.
    ///
    /// The switch that makes plan 10 reversible. It governs reads that answer a
    /// client — `GET`, promotion, unpacking a segment — and deliberately not
    /// the two that answer a question about the object itself: the scrubber's
    /// reconstruction sample and the admin object check keep fetching all `n`
    /// whatever this is set to, because their verdict is about every shard
    /// (decision EC2-184).
    pub minimal_read_enabled: bool,
    /// How long a node remembers that an object has no replicated-small
    /// placement record, so that a read of an ordinary hot object does not pay
    /// the resolve every time. `Duration::ZERO` turns the memory off.
    pub small_placement_negative_ttl: Duration,
}

impl EcOperationsConfig {
    /// Parse Phase 9 EC operations settings from the environment.
    pub fn from_env() -> Result<Self, ClusterError> {
        let transcode_enabled = parse_optional_bool("S4_EC_TRANSCODE_ENABLED")?.unwrap_or(true);
        let tiering = EcTieringConfig::from_env()?;
        if tiering.enabled && !transcode_enabled {
            return Err(invalid_config(
                "S4_EC_TIERING_ENABLED",
                "auto-tiering requires S4_EC_TRANSCODE_ENABLED=true",
            ));
        }
        let admin_scan_limit =
            parse_optional_usize("S4_EC_ADMIN_SCAN_LIMIT")?.unwrap_or(DEFAULT_EC_ADMIN_SCAN_LIMIT);
        if admin_scan_limit == 0 {
            return Err(invalid_config(
                "S4_EC_ADMIN_SCAN_LIMIT",
                "must be greater than 0",
            ));
        }

        let erasure_set_refresh_secs = parse_optional_u64(ERASURE_SET_REFRESH_SECS_ENV)?
            .unwrap_or(DEFAULT_ERASURE_SET_REFRESH_SECS);
        if erasure_set_refresh_secs == 0 {
            return Err(invalid_config(
                ERASURE_SET_REFRESH_SECS_ENV,
                "must be greater than 0",
            ));
        }

        let gc = EcGcConfig::from_env().map_err(cluster_config_error)?;
        // The promoted manifest is the marker that tells every node to release
        // its shards, and it is removed once they have. Until then the object
        // must not be migrated into erasure coding again: a second migration of
        // the same generation produces a manifest with the same HLC, and at
        // equal HLC the marker outranks it — deliberately, because that is what
        // makes a promotion switch readers at all. The object would end up
        // pointing at a marker for a copy that is being rebuilt underneath it.
        //
        // The pause between two migrations of one object is what keeps the two
        // apart, so it has to outlast the marker. Both defaults satisfy this by
        // a wide margin; the check is here so that a deployment which narrows
        // one of them cannot discover the overlap in production.
        if gc.promoted_grace_secs >= tiering.min_migration_interval_secs {
            return Err(invalid_config(
                "S4_EC_PROMOTED_GC_GRACE_SECS",
                "must be shorter than S4_EC_TIERING_MIN_MIGRATION_INTERVAL_SECS, so that a \
                 promoted object cannot be migrated back into EC while its promotion marker \
                 is still the manifest the pool agrees on",
            ));
        }

        Ok(Self {
            transcode_enabled,
            tiering,
            transcode: EcTranscodeConfig::from_env().map_err(cluster_config_error)?,
            transcode_worker: EcTranscodeWorkerConfig::from_env()?,
            repair: EcRepairConfig::from_env().map_err(cluster_config_error)?,
            integrity_worker: EcIntegrityWorkerConfig::from_env()?,
            gc,
            anti_entropy: EcAntiEntropyConfig::from_env().map_err(cluster_config_error)?,
            cpu: EcCodecCpuConfig::from_env().map_err(cluster_config_error)?,
            packed_segments: PackedSegmentConfig::from_env().map_err(cluster_config_error)?,
            packed_repack: PackedRepackPolicy::from_env().map_err(cluster_config_error)?,
            admin_scan_limit,
            erasure_set_refresh_interval: Duration::from_secs(erasure_set_refresh_secs),
            rejoin_gate_enforced: crate::ec_rejoin::rejoin_gate_enforced_from_env(),
            minimal_read_enabled: parse_optional_bool(MINIMAL_READ_ENABLED_ENV)?
                .unwrap_or(DEFAULT_EC_MINIMAL_READ_ENABLED),
            small_placement_negative_ttl: Duration::from_secs(
                parse_optional_u64(SMALL_PLACEMENT_NEGATIVE_TTL_ENV)?
                    .unwrap_or(DEFAULT_EC_SMALL_PLACEMENT_NEGATIVE_TTL_SECS),
            ),
        })
    }
}

/// Enterprise implementation of the edition-safe EC admin trait.
pub struct EcOperationsService {
    pool_name: String,
    /// Every erasure set of this pool, and the choice of which one a new object
    /// goes to.
    ///
    /// Authoritative for resolving a manifest to the set that holds it, and the
    /// single point where placement is decided. Shared with the tiering worker
    /// so both admission paths walk the same rotation rather than each keeping
    /// its own.
    placement: Arc<EcPlacementService>,
    /// The pool's default set — the inherited one, where EC v1 put everything.
    ///
    /// Still the set the read, repair and garbage-collection paths work
    /// against: adapting those to resolve a set per object is plan 03 phase 3
    /// (`EC2-MSET-03-001`..`003`), and doing it here would blur the phase
    /// boundary. Also the source of pool-wide facts every set shares — the
    /// profile, the pool id, the topology epoch.
    default_set: ErasureSet,
    /// What another set of this pool is built from, when an operator adds one.
    set_builder: ErasureSetBuilder,
    /// Layout records of the pool's sets as this node last knew them, by set id.
    ///
    /// The state a node stays on when a record cannot be read: rebuilding the
    /// set from labels instead would quietly produce a different layout
    /// (decision EC2-084, point 3). A plain mutex, because it is only ever held
    /// to copy the map in or out, never across an await.
    set_layouts: std::sync::Mutex<HashMap<u64, EcSetLayoutRecord>>,
    local_node_id: NodeId,
    /// The pool as this node sees it, from the labels it started with: which
    /// zone and rack every node is in. Degraded reads ask for near shards
    /// first and repair plans by it (plan 05, phase 3).
    topology: Arc<PoolTopologyView>,
    metadata: Arc<EcMetadataStore>,
    codec_limiter: EcCodecCpuLimiter,
    repair: EcRepairCoordinator,
    heat: EcHeatTracker,
    config: EcOperationsConfig,
    read_runtime: OnceLock<EcObjectReadRuntimeDeps>,
    /// Cluster-wide catalog view, used by the tiering surface only.
    ///
    /// A node's local index holds the objects replicated to it, which in a
    /// pool wider than the replication factor is a fraction of the bucket.
    /// Answering "what would tiering do here" from that fraction would report
    /// a number the operator cannot act on, so the question is asked of the
    /// whole pool. Kept apart from the read runtime because no EC v1 path
    /// needs it.
    catalog: OnceLock<Arc<DistributedListCoordinator>>,
    anti_entropy: OnceLock<EcAntiEntropyService>,
    /// Shared with `EcTieringServiceEnvironment` rather than copied into it, so
    /// the worker and the admin health snapshot can never disagree about
    /// whether the pool is uniform — including when the handle is wired after
    /// an environment has already been built.
    membership: Arc<OnceLock<Arc<dyn s4_cluster::ClusterMembershipQuery>>>,
    transcode_worker_started: Arc<AtomicBool>,
    tiering_worker_started: Arc<AtomicBool>,
    promotion_worker_started: Arc<AtomicBool>,
    repair_worker_started: Arc<AtomicBool>,
    anti_entropy_worker_started: Arc<AtomicBool>,
    set_refresher_started: Arc<AtomicBool>,
    packed_segment_maintenance_started: Arc<AtomicBool>,
    /// Open packed segments this node owns, one pool per erasure set.
    ///
    /// Behind a mutex because a segment hands out offsets in order: two writes
    /// racing for the same offset would overlap. Concurrency comes from keeping
    /// several segments open, which the pool arranges, rather than from letting
    /// two appends into one segment.
    packed_segments: Arc<tokio::sync::Mutex<PackedSegmentRuntime>>,
    /// Where the repacker asks whether an object is under Object Lock.
    ///
    /// Empty until a layer that can see both subsystems installs a source. The
    /// EC layer does not depend on `s4-features`, and the answer it falls back
    /// to — nothing is held — is the correct one for a build without Object
    /// Lock (`EC2-065`).
    packed_retention: Arc<OnceLock<Arc<dyn PackedSegmentRetentionSource>>>,
    /// What this node is allowed to do until its shards are back.
    ///
    /// Decided once at startup and then only by an operator, so the read path
    /// pays one atomic load rather than re-deciding per request.
    rejoin: Arc<EcRejoinGate>,
    /// Objects this node has looked for a placement record for and not found.
    ///
    /// Bounded and per node, holding an instant per object; losing it costs one
    /// resolve. A plain mutex, because it is only ever held to look at a map
    /// and never across an await.
    small_placement_misses: Arc<std::sync::Mutex<HashMap<EcObjectVersionKey, Instant>>>,
}

/// One small object as the packing path sees it.
///
/// Grouped rather than passed as loose arguments because these five values
/// always travel together and mean nothing apart.
struct PackedObjectCommit<'a> {
    object: &'a EcObjectVersionKey,
    content_hash_sha256: [u8; 32],
    hlc: HlcTimestamp,
    replica_nodes: &'a [NodeId],
    size: u64,
    /// S3 metadata the segment index has to keep.
    ///
    /// Once the segment is encoded and the object's hot copies are purged,
    /// nothing else on the node describes the object, so a `HEAD` would start
    /// answering differently from before the purge.
    etag: &'a str,
    content_type: &'a str,
    user_metadata: &'a HashMap<String, String>,
}

/// Open packed segments of every erasure set, rebuilt from durable metadata.
///
/// These segments are the write cursor and nothing else. Deletions are not
/// applied to them: a delete writes a durable set entry and can arrive at any
/// node, while these are only the segments this node has open. The dead count a
/// repacker acts on therefore comes from
/// [`EcMetadataStore::load_packed_segments`](crate::ec_metadata::EcMetadataStore::load_packed_segments),
/// which folds that set in, and never from a pool held here.
struct PackedSegmentRuntime {
    config: PackedSegmentConfig,
    loaded: bool,
    pools: HashMap<u64, PackedSegmentPool>,
}

impl PackedSegmentRuntime {
    fn new(config: PackedSegmentConfig) -> Self {
        Self {
            config,
            loaded: false,
            pools: HashMap::new(),
        }
    }

    /// Rebuild the open segments from durable metadata, once per process.
    ///
    /// Lazy rather than done at start-up because a node with packing switched
    /// off must not pay for it, and because the first small write is the first
    /// moment the answer is needed.
    async fn ensure_loaded(
        &mut self,
        metadata: &EcMetadataStore,
        scan_limit: usize,
    ) -> Result<(), ClusterError> {
        if self.loaded {
            return Ok(());
        }
        let recovery =
            metadata.load_packed_segments(scan_limit).await.map_err(cluster_storage_error)?;
        for tail in &recovery.discarded {
            tracing::warn!(
                segment_id = %tail.segment_id,
                from_sequence = tail.from_sequence,
                discarded_records = tail.discarded_records,
                reason = %tail.reason,
                "packed segment journal tail discarded on recovery"
            );
        }
        for rejected in &recovery.rejected {
            tracing::error!(
                segment_id = %rejected.segment_id,
                reason = %rejected.reason,
                "packed segment journal rejected on recovery"
            );
        }

        let mut by_set: HashMap<u64, Vec<PackedSegment>> = HashMap::new();
        for segment in recovery.segments {
            // Only open segments belong in a pool. A sealed one is the transcode
            // pipeline's business and is read from its durable record.
            if segment.state == PackedSegmentState::Open {
                by_set.entry(segment.erasure_set_id).or_default().push(segment);
            }
        }
        for (set_id, segments) in by_set {
            let pool = PackedSegmentPool::from_recovered(set_id, self.config, segments)
                .map_err(|error| cluster_storage_error(packed_segment_error(error)))?;
            self.pools.insert(set_id, pool);
        }
        self.loaded = true;
        Ok(())
    }

    fn pool_for(&mut self, set_id: u64) -> &mut PackedSegmentPool {
        self.pools
            .entry(set_id)
            .or_insert_with(|| PackedSegmentPool::new(set_id, self.config))
    }
}

#[derive(Clone)]
struct EcObjectReadRuntimeDeps {
    read_coordinator: Arc<QuorumReadCoordinator>,
    write_coordinator: Arc<QuorumWriteCoordinator>,
    storage: Arc<dyn StorageEngine>,
    rpc_client: Arc<NodeClient>,
    topology_epoch: u64,
}

/// Why this node could not take the pool's erasure-set roster.
///
/// The three kinds differ in who can fix them, which is what decides whether a
/// node may start without the roster.
#[derive(Debug)]
enum RosterRejection {
    /// This node's own configuration contradicts the pool. No other node will
    /// fix it, and a node serving a pool whose roster it cannot build serves
    /// part of that pool wrongly.
    NodeConfiguration(ClusterError),
    /// The roster the pool published is not one a pool can be built from. Every
    /// node reads the same record, so refusing to start would take the whole
    /// cluster down over it; the node keeps the sets it already had.
    PoolState(ClusterError),
    /// Nothing is wrong with either: the roster could not be read right now.
    Transient(ClusterError),
}

/// What reading the layout record of one erasure set found.
///
/// "Absent" and "unknown" are kept apart on purpose: only an answer that the
/// pool has no record lets a node write its own. Taking a failed read for one
/// would let a node overwrite the record it could not see (decision EC2-084,
/// point 3).
#[derive(Debug)]
enum SetLayoutRead {
    /// The record the pool holds, or this node's copy of it when the node
    /// has no cluster read path to ask.
    Found(EcSetLayoutRecord),
    /// The pool did not produce the record — its replicas answered that they
    /// hold none, or too few answered — while this node keeps a copy.
    ///
    /// "None" is not proof. When a pool grows, the placement ring of the system
    /// bucket moves a key to new owners that never received it: they answer
    /// "none" in good faith, and where only one old owner is left no quorum
    /// forms at all. The copy is taken, and the set's own nodes publish it
    /// again so the new owners get it.
    LocalOnly(EcSetLayoutRecord),
    /// The pool answered that the set has no record, and this node has no copy.
    Absent,
    /// Neither the cluster nor the local copy could say; the reason.
    Unknown(String),
}

/// What a node does with one set once it has asked the pool for the set's
/// layout record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LayoutStep {
    /// Use the pool's record.
    Take,
    /// Use this node's copy and publish it again, so the pool's current
    /// replicas hold it.
    Republish,
    /// Record the layout this node builds from configuration.
    RecordV1,
    /// Keep what this node knows, and ask again at the next refresh.
    Wait,
}

impl LayoutStep {
    /// The step after `read`, for a node that is one of the set's nodes or
    /// not.
    ///
    /// "The pool has no record" makes a node record the EC v1 layout only if
    /// it is a node of the set. That answer is how a set created before
    /// layout records gets one (EC2-086, point 2) — its own nodes upgrade and
    /// record it — but a node added to a grown pool gets the same answer for
    /// every record written before it joined, because the placement ring has
    /// moved those keys to owners that never received them. Such a node is
    /// never a node of an existing set, and letting it record would overwrite
    /// the set's real layout with the one its own labels give; for a set laid
    /// out by the LRC group rule, the rule's layout would be lost. It waits
    /// instead, until a node holding the record publishes it again.
    ///
    /// Publishing a copy again is the set's own nodes' job too: they all keep
    /// one. It is safe because a record does not change after its set is
    /// created, and a copy naming other nodes than the set — the trace of a
    /// losing concurrent add — is never published (EC2-088).
    fn after(read: &SetLayoutRead, node_of_the_set: bool) -> Self {
        match read {
            SetLayoutRead::Found(_) => Self::Take,
            SetLayoutRead::LocalOnly(_) if node_of_the_set => Self::Republish,
            SetLayoutRead::LocalOnly(_) => Self::Take,
            SetLayoutRead::Absent if node_of_the_set => Self::RecordV1,
            SetLayoutRead::Absent | SetLayoutRead::Unknown(_) => Self::Wait,
        }
    }
}

/// Whether a set this node waits on takes no new objects here meanwhile.
///
/// Only a set whose record this node has never known: otherwise the last
/// record known stays in force. Such a set may be laid out by a rule this node
/// cannot see yet, and even an EC v1 set may be recorded with another order
/// than this node's labels give, if the labels changed after it was recorded.
/// So it serves reads — they go by the manifest — and new objects go
/// elsewhere, or stay on their RF=3 replicas until the record is read.
///
/// The pool's inherited set is held only on a node with a cluster read path.
/// Without one there is no record this node could be missing: the layout its
/// configuration gives is the only one there is.
fn held_until_known(inherited: bool, has_cluster_read_path: bool, known: bool) -> bool {
    !known && (!inherited || has_cluster_read_path)
}

/// The layout records a node knows for its pool's sets.
#[derive(Debug, Clone, Default)]
struct SetLayouts {
    /// The record of each set this node has one for, by set id.
    records: HashMap<u64, EcSetLayoutRecord>,
    /// Sets whose record this node has never known and could not learn now
    /// ([`held_until_known`]). Built from configuration, they take no new
    /// objects until their record is read ([`ErasureSet::layout_pending`]).
    unresolved: HashSet<u64>,
}

impl SetLayouts {
    /// Known records, with nothing unresolved.
    fn from_records(records: HashMap<u64, EcSetLayoutRecord>) -> Self {
        Self {
            records,
            unresolved: HashSet::new(),
        }
    }
}

impl RosterRejection {
    fn into_cluster_error(self) -> ClusterError {
        match self {
            Self::NodeConfiguration(error) | Self::PoolState(error) | Self::Transient(error) => {
                error
            }
        }
    }
}

impl EcOperationsService {
    /// Open the node-local EC operations service.
    ///
    /// `sets` is the pool's erasure-set registry. It must hold at least one set
    /// that can take writes, otherwise there is nowhere to place an object and
    /// the node refuses to start rather than failing on the first write.
    pub fn open(
        metadata_path: &Path,
        pool_name: impl Into<String>,
        sets: ErasureSetRegistry,
        set_builder: ErasureSetBuilder,
        local_node_id: NodeId,
    ) -> Result<Self, ClusterError> {
        // Resolve the codec engine before anything else touches data: a node
        // that was explicitly told to use ISA-L and cannot must stop here, not
        // run slower than the operator believes it does. See decision EC2-004.
        crate::ec_codec::ensure_codec_engine_available()
            .map_err(|error| ClusterError::Config(error.to_string()))?;

        let placement = Arc::new(
            EcPlacementService::from_env(sets)
                .map_err(|error| ClusterError::Config(error.to_string()))?,
        );
        let default_set = placement
            .sets()
            .default_set()
            .ok_or_else(|| {
                ClusterError::Config(format!(
                    "EC pool {} has no erasure set that can take writes; a pool must hold at \
                 least one active set",
                    placement.sets().pool_id()
                ))
            })?
            .clone();

        let config = EcOperationsConfig::from_env()?;
        let metadata =
            Arc::new(EcMetadataStore::open(metadata_path).map_err(cluster_storage_error)?);
        let codec_limiter = EcCodecCpuLimiter::new(config.cpu.clone());
        let topology = set_builder.topology_view(local_node_id);
        let repair = EcRepairCoordinator::new(
            Arc::clone(&metadata),
            codec_limiter.clone(),
            config.repair.clone(),
            topology.clone(),
        );

        // Checked here rather than in `from_env` because the floor depends on
        // the profile's `min_object_size`, which the environment does not know.
        config
            .packed_segments
            .validate_for_profile(default_set.profile.min_object_size)
            .map_err(cluster_config_error)?;

        let heat = EcHeatTracker::new(config.tiering.heat_config());
        ec_metrics::init_tiering_metrics();
        ec_metrics::init_layout_metrics();
        ec_metrics::init_repair_traffic_metrics();
        ec_metrics::init_read_traffic_metrics();
        ec_metrics::init_small_placement_metrics();
        ec_metrics::init_repair_throttle_metrics();
        ec_metrics::init_rejoin_metrics();
        let packed_segments = Arc::new(tokio::sync::Mutex::new(PackedSegmentRuntime::new(
            config.packed_segments,
        )));

        Ok(Self {
            pool_name: pool_name.into(),
            placement,
            default_set,
            set_builder,
            set_layouts: std::sync::Mutex::new(HashMap::new()),
            local_node_id,
            topology: Arc::new(topology),
            metadata,
            codec_limiter,
            repair,
            heat,
            config,
            read_runtime: OnceLock::new(),
            catalog: OnceLock::new(),
            anti_entropy: OnceLock::new(),
            membership: Arc::new(OnceLock::new()),
            transcode_worker_started: Arc::new(AtomicBool::new(false)),
            tiering_worker_started: Arc::new(AtomicBool::new(false)),
            promotion_worker_started: Arc::new(AtomicBool::new(false)),
            repair_worker_started: Arc::new(AtomicBool::new(false)),
            anti_entropy_worker_started: Arc::new(AtomicBool::new(false)),
            set_refresher_started: Arc::new(AtomicBool::new(false)),
            packed_segment_maintenance_started: Arc::new(AtomicBool::new(false)),
            packed_segments,
            packed_retention: Arc::new(OnceLock::new()),
            rejoin: Arc::new(EcRejoinGate::new()),
            small_placement_misses: Arc::new(std::sync::Mutex::new(HashMap::new())),
        })
    }

    /// Access the local EC metadata store.
    pub fn metadata(&self) -> &Arc<EcMetadataStore> {
        &self.metadata
    }

    /// Current operations configuration.
    pub fn config(&self) -> &EcOperationsConfig {
        &self.config
    }

    /// Node-wide tiering values a bucket policy may override.
    fn tiering_globals(&self) -> TieringGlobals {
        let tiering = &self.config.tiering;
        TieringGlobals {
            enabled: tiering.enabled,
            cold_after: Duration::from_secs(tiering.cold_after_secs),
            // The profile threshold, not a setting: below it an object is
            // stored replicated-small and erasure coding gains nothing.
            min_object_size: self.default_set.profile.min_object_size,
            // What the transcode worker can actually read in one piece. A
            // candidate above this would only produce retryable failures.
            max_object_size: self.config.transcode_worker.max_source_read_bytes,
            promote_after_reads: tiering.promote_after_reads,
            promote_window: Duration::from_secs(tiering.promote_window_secs),
            min_migration_interval: Duration::from_secs(tiering.min_migration_interval_secs),
            jitter_percent: tiering.jitter_percent,
        }
    }

    /// Read a bucket policy from the cluster, falling back to the local cache.
    ///
    /// The replicated object is authoritative: an operator who wrote a policy
    /// through another node a second ago must see it here. The local keyspace
    /// is a cache of that object, in the same role `ec_manifests` plays for
    /// manifests, and it answers when the cluster read path is unavailable.
    async fn read_tiering_policy(
        &self,
        bucket: &str,
    ) -> Result<Option<EcTieringPolicyRecord>, ClusterError> {
        let Some(deps) = self.read_runtime.get() else {
            return self.cached_tiering_policy(bucket).await;
        };
        read_tiering_policy_through_quorum(&self.metadata, &deps.read_coordinator, bucket).await
    }

    /// Read a bucket policy from this node's cache only.
    pub async fn cached_tiering_policy(
        &self,
        bucket: &str,
    ) -> Result<Option<EcTieringPolicyRecord>, ClusterError> {
        self.metadata.get_tiering_policy(bucket).await.map_err(cluster_storage_error)
    }

    /// Store a bucket policy in the cluster and in the local cache.
    async fn write_tiering_policy(
        &self,
        record: &EcTieringPolicyRecord,
    ) -> Result<(), ClusterError> {
        record.validate().map_err(cluster_storage_error)?;
        let data = bincode::serialize(record).map_err(|error| {
            ClusterError::Storage(format!("tiering policy could not be encoded: {error}"))
        })?;

        if let Some(deps) = self.read_runtime.get() {
            deps.write_coordinator
                .write(
                    SYSTEM_BUCKET,
                    &tiering_policy_object_key(&record.bucket),
                    &data,
                    EC_TIERING_POLICY_CONTENT_TYPE,
                    &HashMap::new(),
                )
                .await?;
        } else {
            // Without the cluster write path the policy would only exist here,
            // and the worker on another node would not honour it. Say so.
            tracing::warn!(
                bucket = %record.bucket,
                "EC cluster write path is not configured; tiering policy stored on this node only"
            );
        }

        self.metadata.put_tiering_policy(record).await.map_err(cluster_storage_error)
    }

    /// Build the tiering worker's view of this node and its pool.
    fn tiering_environment(&self) -> Result<EcTieringServiceEnvironment, ClusterError> {
        let deps = self.read_runtime.get().ok_or_else(|| {
            ClusterError::Config("EC read runtime is not configured on this node".to_string())
        })?;
        let catalog = self.catalog.get().ok_or_else(|| {
            ClusterError::Config("EC tiering catalog is not configured on this node".to_string())
        })?;
        Ok(EcTieringServiceEnvironment {
            local_node_id: self.local_node_id,
            placement: Arc::clone(&self.placement),
            globals: self.tiering_globals(),
            scan_limit: self.config.admin_scan_limit,
            metadata: Arc::clone(&self.metadata),
            catalog: Arc::clone(catalog),
            read_coordinator: Arc::clone(&deps.read_coordinator),
            rpc_client: Arc::clone(&deps.rpc_client),
            membership: Arc::clone(&self.membership),
        })
    }

    // -----------------------------------------------------------------------
    // Erasure sets of a running pool (plan 03 phase 4)
    // -----------------------------------------------------------------------

    /// Whether this node may manage more than one erasure set.
    ///
    /// The gate is the same flag that decides whether the router may spread
    /// objects. With it off, a pool keeps exactly the set its configuration
    /// describes, and the management routes say so instead of quietly creating
    /// a set nothing would ever place into.
    fn ensure_multi_set_enabled(&self, action: &str) -> Result<(), ClusterError> {
        if self.placement.config().multi_set_enabled {
            return Ok(());
        }
        Err(ClusterError::Config(format!(
            "{action} requires {} to be true on this node; with it off the pool keeps the single \
             erasure set its configuration describes",
            crate::erasure_coding::MULTI_SET_ENABLED_ENV
        )))
    }

    /// Whether every configured node of the pool is confirmed to support EC v2.
    ///
    /// This asks about every configured pool node rather than only the nodes
    /// holding slots: the candidates for a new set are precisely the nodes that
    /// hold none yet, so a check limited to active slot holders would allow a
    /// pool to expand onto nodes that cannot decode v2 manifests.
    ///
    /// Fails closed when no membership query handle is wired.
    fn all_pool_nodes_speak_v2(&self) -> bool {
        match self.membership.get() {
            Some(membership) => membership.pool_nodes_support_v2(&self.set_builder.pool_nodes()),
            None => false,
        }
    }

    /// Ensure all configured pool nodes confirm EC v2 support before modifying
    /// multi-set topology.
    fn ensure_pool_speaks_v2(&self, action: &str) -> Result<(), ClusterError> {
        if self.all_pool_nodes_speak_v2() {
            return Ok(());
        }
        Err(ClusterError::Config(format!(
            "{action} is refused while pool {} is mid-upgrade: every node of the pool must \
             confirm EC v2 support (protocol version {}) before the pool can grow, and a node \
             which has not answered counts as not confirmed",
            self.pool_name,
            s4_cluster::EC_V2_MIN_PROTOCOL_VERSION
        )))
    }

    /// Refuse `action` while a node of the pool may still ignore layout records.
    ///
    /// A node below [`s4_cluster::EC_SET_LAYOUT_MIN_PROTOCOL_VERSION`] rebuilds
    /// every set from topology labels. For a set laid out by any rule other
    /// than EC v1 it would get another slot order and write the set's new
    /// objects where the rule does not put them. So a set is laid out by such a
    /// rule only once every node of the pool confirms the version, and the
    /// refusal names the nodes that do not (decision EC2-084, point 4). Fails
    /// closed when no membership handle is wired, like the EC v2 gate.
    fn ensure_pool_reads_set_layouts(&self, action: &str) -> Result<(), ClusterError> {
        let pool_nodes = self.set_builder.pool_nodes();
        // An empty pool is refused explicitly: "no node is unconfirmed" is also
        // the answer about no nodes.
        let unconfirmed = match self.membership.get() {
            Some(membership) if !pool_nodes.is_empty() => membership
                .nodes_below_protocol(&pool_nodes, s4_cluster::EC_SET_LAYOUT_MIN_PROTOCOL_VERSION),
            _ => pool_nodes.clone(),
        };
        if !pool_nodes.is_empty() && unconfirmed.is_empty() {
            return Ok(());
        }
        let reason = if pool_nodes.is_empty() {
            "the pool lists no nodes to check".to_string()
        } else if self.membership.get().is_none() {
            "this node has no view of cluster membership to check the pool's nodes against"
                .to_string()
        } else {
            format!(
                "these node(s) have not confirmed protocol version {}: {}",
                s4_cluster::EC_SET_LAYOUT_MIN_PROTOCOL_VERSION,
                unconfirmed.iter().map(ToString::to_string).collect::<Vec<_>>().join(", ")
            )
        };
        Err(ClusterError::Config(format!(
            "{action} is refused while pool {} may hold nodes that rebuild erasure sets from \
             topology labels instead of reading their layout records; such a node would place the \
             set's new objects where the rule does not put them. {reason}. Upgrade them first — a \
             node which has not answered counts as not confirmed",
            self.pool_name
        )))
    }

    /// Build the set an operator is adding, by the rule this node is
    /// configured with.
    ///
    /// The LRC group placement searches over swaps of slots, which is CPU work
    /// of milliseconds on the standard profiles and more on a wide custom
    /// one, so it runs on the blocking pool rather than on a request worker.
    ///
    /// A refusal under `strict` is counted by its reason before it is returned,
    /// unless `count_refusal` is false. A dry run passes false: it refuses
    /// nothing and creates nothing, while that counter means exactly those two
    /// things (EC2-083).
    async fn build_new_erasure_set(
        &self,
        set_id: u64,
        node_ids: &[NodeId],
        created_at: u64,
        count_refusal: bool,
    ) -> Result<EcNewErasureSet, ClusterError> {
        let builder = self.set_builder.clone();
        let nodes = node_ids.to_vec();
        let built =
            tokio::task::spawn_blocking(move || builder.build_new(set_id, &nodes, created_at))
                .await
                .map_err(|error| {
                    // The task only fails by panicking: a fault of this node,
                    // not of the request, so it answers 500 rather than 400.
                    ClusterError::Storage(format!(
                        "laying out the new erasure set did not finish: {error}"
                    ))
                })?;
        built.map_err(|error| {
            let refused = match &error {
                EcConfigError::LayoutRule(violation) => Some(violation),
                _ => None,
            };
            if let Some(violation) = refused.filter(|_| count_refusal) {
                ec_metrics::record_set_layout_unsatisfied(
                    violation.reason,
                    self.set_builder.rule().policy,
                );
            }
            ClusterError::Config(error.to_string())
        })
    }

    /// Build the pool's registry from the node's configuration, a roster and
    /// the sets' layout records.
    ///
    /// The inherited set always comes from configuration — a node must be able
    /// to build it before it has read anything — and the roster only adds to it
    /// and may change its state. A set that has a layout record in `layouts`
    /// takes its slot order from the record rather than from the labels.
    fn registry_from_roster(
        &self,
        roster: &EcErasureSetRoster,
        layouts: &SetLayouts,
    ) -> Result<ErasureSetRegistry, RosterRejection> {
        // The recorded list is the pool's own memory of which nodes its
        // inherited set was built from. A configuration that reordered or
        // replaced them would build a different set under the same identity, and
        // every object written before the change would stop resolving — so it is
        // refused here rather than discovered as unreadable data.
        let inherited_nodes = self.default_set.slot_assignment.distinct_nodes();
        if !roster.inherited_node_ids.is_empty() && roster.inherited_node_ids != inherited_nodes {
            return Err(RosterRejection::NodeConfiguration(ClusterError::Config(
                format!(
                "pool {} built its inherited erasure set from {:?}, but the pool's roster records \
                 {:?}. The pool's node list must only ever be appended to; restore the original \
                 order before starting this node",
                self.pool_name, inherited_nodes, roster.inherited_node_ids
            ),
            )));
        }

        let mut inherited = self.with_layout_record(&self.default_set, &layouts.records)?;
        if !layouts.records.contains_key(&self.default_set.set_id) {
            inherited.layout_pending = layouts.unresolved.contains(&self.default_set.set_id);
        }
        inherited.state = roster.inherited_state;
        let mut sets = vec![inherited];
        for spec in &roster.sets {
            let mut set = self.roster_set(spec, layouts)?;
            set.state = spec.state;
            sets.push(set);
        }

        // Sets of a pool never share a node: the pool's free space is reported
        // as the sum of its sets', and a node counted twice would promise room
        // that is not there.
        let mut seen_nodes: HashSet<NodeId> = HashSet::new();
        for (idx, set) in sets.iter().enumerate() {
            for node_id in set.slot_assignment.distinct_nodes() {
                if !seen_nodes.insert(node_id) {
                    let prior = sets[..idx]
                        .iter()
                        .find(|s| s.slot_assignment.distinct_nodes().contains(&node_id))
                        .map(|s| s.set_id)
                        .unwrap_or(self.default_set.set_id);
                    return Err(RosterRejection::PoolState(ClusterError::Config(format!(
                        "node {node_id} is in both erasure set {prior} and erasure set {} of pool {}; \
                         the pool's free space is the sum of its sets' and cannot be reported honestly \
                         while a node is counted twice",
                        set.set_id, self.pool_name
                    ))));
                }
            }
        }

        ErasureSetRegistry::from_sets(self.set_builder.pool_id(), sets)
            .map_err(|error| RosterRejection::PoolState(ClusterError::Config(error.to_string())))
    }

    /// Read the pool's roster from the cluster, falling back to the local copy.
    ///
    /// The local copy is a cache, and answering from it is right whenever the
    /// cluster cannot: a node that cannot reach a quorum must keep serving the
    /// sets it already knows about rather than forget them.
    async fn read_erasure_set_roster(&self) -> Result<EcErasureSetRoster, RosterRejection> {
        let pool_id = self.set_builder.pool_id().as_u32();
        let key = erasure_set_roster_object_key(pool_id);
        let cached = || async {
            let record = self
                .metadata
                .get_erasure_set_roster()
                .await
                .map_err(cluster_storage_error)
                .map_err(RosterRejection::Transient)?;
            Ok(record.unwrap_or_else(|| EcErasureSetRoster::empty(pool_id)))
        };

        let Some(deps) = self.read_runtime.get() else {
            return cached().await;
        };
        match deps.read_coordinator.read(SYSTEM_BUCKET, &key).await {
            Ok(result) => {
                let roster: EcErasureSetRoster =
                    bincode::deserialize(&result.data).map_err(|error| {
                        RosterRejection::PoolState(ClusterError::Storage(format!(
                            "EC erasure set roster could not be decoded: {error}"
                        )))
                    })?;
                roster
                    .validate()
                    .map_err(|error| RosterRejection::PoolState(cluster_storage_error(error)))?;
                if roster.pool_id != pool_id {
                    return Err(RosterRejection::PoolState(ClusterError::Storage(format!(
                        "EC erasure set roster names pool {}, this node serves pool {pool_id}",
                        roster.pool_id
                    ))));
                }
                if let Err(error) = self.metadata.put_erasure_set_roster(&roster).await {
                    tracing::warn!(
                        error = %error,
                        "EC erasure set roster cache refresh failed; the cluster copy still answers"
                    );
                }
                Ok(roster)
            }
            Err(ClusterError::ObjectNotFound { .. }) => cached().await,
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    "EC erasure set roster quorum read failed; answering from the local cache"
                );
                cached().await
            }
        }
    }

    /// Commit a roster to the cluster and to this node's cache.
    async fn write_erasure_set_roster(
        &self,
        roster: &EcErasureSetRoster,
    ) -> Result<(), ClusterError> {
        let mut roster = roster.clone();
        // Stamped on every write, so a pool that never had a roster records its
        // inherited nodes the first time an operator touches it.
        roster.inherited_node_ids = self.default_set.slot_assignment.distinct_nodes();
        let roster = &roster;
        roster.validate().map_err(cluster_storage_error)?;
        let data = bincode::serialize(roster).map_err(|error| {
            ClusterError::Storage(format!(
                "EC erasure set roster could not be encoded: {error}"
            ))
        })?;
        let deps = self.read_runtime.get().ok_or_else(|| {
            ClusterError::Config(
                "EC cluster write path is not configured on this node, so an erasure set cannot be \
                 published to the pool"
                    .to_string(),
            )
        })?;
        deps.write_coordinator
            .write(
                SYSTEM_BUCKET,
                &erasure_set_roster_object_key(roster.pool_id),
                &data,
                EC_ERASURE_SET_ROSTER_CONTENT_TYPE,
                &HashMap::new(),
            )
            .await?;
        self.metadata
            .put_erasure_set_roster(roster)
            .await
            .map_err(cluster_storage_error)
    }

    /// `set` laid out as its layout record says, or as built when it has none.
    ///
    /// A record naming other nodes than the set is not applied: it is what a
    /// losing concurrent add leaves behind, or the trace of a configuration
    /// that changed the set's nodes, and taking it would change which nodes the
    /// set's manifests are checked against. The set keeps the layout it was
    /// built with, and the mismatch is logged for an operator rather than
    /// overwritten — two nodes that disagree about a set's nodes would
    /// otherwise take turns rewriting its record.
    fn with_layout_record(
        &self,
        set: &ErasureSet,
        layouts: &HashMap<u64, EcSetLayoutRecord>,
    ) -> Result<ErasureSet, RosterRejection> {
        let Some(record) = layouts.get(&set.set_id) else {
            return Ok(set.clone());
        };
        match self.set_builder.with_recorded_layout(set, record.rule, &record.node_ids) {
            Ok(restored) => Ok(restored),
            Err(EcConfigError::Pool { reason, .. }) => {
                tracing::error!(
                    pool = %self.pool_name,
                    set_id = set.set_id,
                    reason = %reason,
                    "EC erasure set layout record does not fit the set and is not applied; the set \
                     keeps the layout its configuration gives until an operator resolves the \
                     mismatch"
                );
                Ok(set.clone())
            }
            Err(error) => Err(RosterRejection::NodeConfiguration(ClusterError::Config(
                error.to_string(),
            ))),
        }
    }

    /// One set of the roster, laid out as its layout record says.
    ///
    /// A set with a record is built from it directly and judged by the
    /// record's rule, never through the EC v1 layout first: the EC v1 checks
    /// could refuse, at every start, a set laid out by the LRC group rule. A
    /// set without a record — one created before records existed — is built
    /// by the EC v1 rule, which is the rule it was created under. A record
    /// naming other nodes is not applied, as for the inherited set. A set whose
    /// record this node could not learn at all is built from configuration and
    /// held from new writes until it can.
    fn roster_set(
        &self,
        spec: &EcErasureSetSpec,
        layouts: &SetLayouts,
    ) -> Result<ErasureSet, RosterRejection> {
        let node_configuration =
            |error: EcConfigError| RosterRejection::NodeConfiguration(cluster_config_error(error));
        let from_configuration = || {
            self.set_builder
                .build(spec.set_id, &spec.node_ids, spec.created_at)
                .map_err(node_configuration)
        };

        match layouts.records.get(&spec.set_id) {
            Some(record) if ErasureSetBuilder::record_fits(&spec.node_ids, &record.node_ids) => {
                self.set_builder
                    .build_recorded(
                        spec.set_id,
                        &spec.node_ids,
                        spec.created_at,
                        record.rule,
                        &record.node_ids,
                    )
                    .map_err(node_configuration)
            }
            Some(record) => {
                tracing::error!(
                    pool = %self.pool_name,
                    set_id = spec.set_id,
                    recorded = ?record.node_ids,
                    configured = ?spec.node_ids,
                    "EC erasure set layout record names other nodes than the roster gives the set \
                     and is not applied; the set keeps the layout its configuration gives until \
                     an operator resolves the mismatch"
                );
                from_configuration()
            }
            None => {
                let mut set = from_configuration()?;
                set.layout_pending = layouts.unresolved.contains(&spec.set_id);
                Ok(set)
            }
        }
    }

    /// The pool's inherited set as this node serves it now.
    ///
    /// `default_set` is that set as configuration built it at startup. The live
    /// registry may since have taken its layout record, which fixes the slot
    /// order, and the state the roster gives it; the admin surface reports
    /// what the pool actually does.
    fn live_inherited_set(&self) -> ErasureSet {
        self.placement
            .sets()
            .get(self.default_set.set_id)
            .cloned()
            .unwrap_or_else(|| self.default_set.clone())
    }

    /// Layout records this node currently knows, by set id.
    fn known_set_layouts(&self) -> HashMap<u64, EcSetLayoutRecord> {
        self.set_layouts.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).clone()
    }

    /// Read the layout record of one set from the cluster, falling back to the
    /// local copy.
    async fn read_set_layout_record(&self, set_id: u64) -> SetLayoutRead {
        let pool_id = self.set_builder.pool_id().as_u32();
        let cached = match self.metadata.get_set_layout_record(set_id).await {
            Ok(cached) => cached,
            Err(error) => {
                return SetLayoutRead::Unknown(format!(
                    "the local copy could not be read: {error}"
                ));
            }
        };
        // What the cluster failed to produce, this node's copy stands in for.
        let from_cache = |reason: String| match cached.clone() {
            Some(record) => SetLayoutRead::LocalOnly(record),
            None => SetLayoutRead::Unknown(reason),
        };

        let Some(deps) = self.read_runtime.get() else {
            return match cached {
                Some(record) => SetLayoutRead::Found(record),
                None => SetLayoutRead::Unknown(
                    "the cluster read path is not configured on this node".to_string(),
                ),
            };
        };
        let key = set_layout_record_object_key(pool_id, set_id);
        match deps.read_coordinator.read(SYSTEM_BUCKET, &key).await {
            Ok(result) => {
                let record = match bincode::deserialize::<EcSetLayoutRecord>(&result.data) {
                    Ok(record) => record,
                    Err(error) => {
                        return from_cache(format!(
                            "the pool's record could not be decoded: {error}"
                        ))
                    }
                };
                if let Err(error) = record.validate() {
                    return from_cache(format!("the pool's record is invalid: {error}"));
                }
                if record.pool_id != pool_id || record.set_id != set_id {
                    return from_cache(format!(
                        "the pool's record names erasure set {} of pool {}",
                        record.set_id, record.pool_id
                    ));
                }
                if cached.as_ref() != Some(&record) {
                    if let Err(error) = self.metadata.put_set_layout_record(&record).await {
                        tracing::warn!(
                            set_id,
                            error = %error,
                            "EC erasure set layout cache refresh failed; the cluster copy still answers"
                        );
                    }
                }
                SetLayoutRead::Found(record)
            }
            Err(ClusterError::ObjectNotFound { .. }) => match cached {
                Some(record) => SetLayoutRead::LocalOnly(record),
                None => SetLayoutRead::Absent,
            },
            Err(error) => from_cache(format!("the quorum read failed: {error}")),
        }
    }

    /// Commit a layout record to the cluster and to this node's cache.
    async fn write_set_layout_record(
        &self,
        record: &EcSetLayoutRecord,
    ) -> Result<(), ClusterError> {
        record.validate().map_err(cluster_storage_error)?;
        let data = bincode::serialize(record).map_err(|error| {
            ClusterError::Storage(format!(
                "EC erasure set layout record could not be encoded: {error}"
            ))
        })?;
        let deps = self.read_runtime.get().ok_or_else(|| {
            ClusterError::Config(
                "EC cluster write path is not configured on this node, so an erasure set layout \
                 cannot be recorded for the pool"
                    .to_string(),
            )
        })?;
        deps.write_coordinator
            .write(
                SYSTEM_BUCKET,
                &set_layout_record_object_key(record.pool_id, record.set_id),
                &data,
                EC_SET_LAYOUT_CONTENT_TYPE,
                &HashMap::new(),
            )
            .await?;
        self.metadata
            .put_set_layout_record(record)
            .await
            .map_err(cluster_storage_error)?;
        self.set_layouts
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(record.set_id, record.clone());
        Ok(())
    }

    /// Bring this node's layout records of the pool's sets up to date.
    ///
    /// Returns the records the registry is to be built with, and the sets
    /// held from new writes. For the inherited set and every set of the
    /// roster, by [`LayoutStep`]:
    ///
    /// - the pool has a record: it is taken;
    /// - the pool does not produce the record but this node keeps a copy: the
    ///   copy is taken, and a node of the set publishes it again. After the
    ///   pool grows this is what brings the record to the new owners of its
    ///   key, so nodes added with the growth can read it;
    /// - the pool has none and this node has none: a node of the set records
    ///   the layout it already uses. That is how every set gets a record,
    ///   including the ones created before records existed, and from then on
    ///   labels no longer move its slots (decision EC2-086, point 2). Nodes
    ///   started with the same labels record the same layout. When two record
    ///   different ones, the later write wins and the other node takes it at
    ///   its next refresh; what it wrote in between stays readable, because a
    ///   manifest is checked against the set's nodes rather than its layout.
    ///   A node that is not one of the set's nodes waits instead;
    /// - neither the cluster nor the cache can say: the last record this node
    ///   knew stays in force. Falling back to the labels would build another
    ///   layout without anyone deciding to.
    ///
    /// A set this node waits on, and has never known a record for, is held
    /// from new writes ([`held_until_known`]).
    async fn refresh_set_layouts(&self, roster: &EcErasureSetRoster) -> SetLayouts {
        let previous = self.known_set_layouts();
        let mut known = previous.clone();
        let mut unresolved = HashSet::new();
        let has_cluster_read_path = self.read_runtime.get().is_some();

        // Every set with its nodes, and — when this node can build it without
        // a record — the EC v1 set: the layout used while there is none, and
        // the one recorded when the pool has none. A spec that cannot be built
        // by the EC v1 rule still has its record read: a set laid out by
        // another rule is judged by that rule once the record is found. What
        // is wrong with a spec that fits neither is said when the registry is
        // built.
        let mut sets = vec![(
            self.default_set.set_id,
            self.default_set.slot_assignment.distinct_nodes(),
            Some(self.default_set.clone()),
        )];
        for spec in &roster.sets {
            let built = self.set_builder.build(spec.set_id, &spec.node_ids, spec.created_at).ok();
            sets.push((spec.set_id, spec.node_ids.clone(), built));
        }

        for (set_id, nodes, built) in &sets {
            let read = self.read_set_layout_record(*set_id).await;
            let step = LayoutStep::after(&read, nodes.contains(&self.local_node_id));
            match (step, read) {
                (LayoutStep::Take, SetLayoutRead::Found(record)) => {
                    if previous.get(set_id) != Some(&record) {
                        self.warn_if_labels_disagree(*set_id, nodes, built.as_ref(), &record);
                    }
                    known.insert(*set_id, record);
                }
                (LayoutStep::Take, SetLayoutRead::LocalOnly(record)) => {
                    known.insert(*set_id, record);
                }
                (LayoutStep::Republish, SetLayoutRead::LocalOnly(record)) => {
                    // A record's content no longer stops changing once its set
                    // is created: an operator can lay a pool's sets out again
                    // (`EC2-ZONE-04-005`). Republishing writes through the
                    // cluster afresh and so wins by last-writer-wins, which
                    // means an older copy would undo a newer layout instead of
                    // healing the gap it was published to heal (EC2-088,
                    // point 2). A copy older than the newest this node has
                    // accepted is therefore kept to itself.
                    let newer_known =
                        previous.get(set_id).filter(|known| known.hlc > record.hlc).cloned();
                    if let Some(newer) = newer_known {
                        tracing::warn!(
                            pool = %self.pool_name,
                            set_id,
                            "EC erasure set layout record was not published again: this node holds \
                             a newer one, and publishing the older copy would undo a layout the \
                             pool has already accepted"
                        );
                        known.insert(*set_id, newer);
                    } else {
                        // Only a copy that fits the set is worth spreading; one
                        // naming other nodes is reported where it is refused.
                        if ErasureSetBuilder::record_fits(nodes, &record.node_ids) {
                            match self.write_set_layout_record(&record).await {
                                Ok(()) => tracing::info!(
                                    pool = %self.pool_name,
                                    set_id,
                                    "EC erasure set layout record published again: the pool did \
                                     not produce it, as after the pool grew"
                                ),
                                Err(error) => tracing::warn!(
                                    pool = %self.pool_name,
                                    set_id,
                                    error = %error,
                                    "EC erasure set layout record could not be published again; \
                                     retrying at the next refresh"
                                ),
                            }
                        }
                        known.insert(*set_id, record);
                    }
                }
                (LayoutStep::RecordV1, _) => {
                    // A set laid out by a rule always has its record written
                    // before the roster names it (EC2-084, point 3), so a set
                    // without one is an EC v1 set.
                    let Some(set) = built else {
                        continue;
                    };
                    let record = EcSetLayoutRecord::v1(
                        set,
                        self.set_builder.rule().policy,
                        unix_time_nanos(),
                        current_hlc(self.local_node_id),
                    );
                    match self.write_set_layout_record(&record).await {
                        Ok(()) => {
                            tracing::info!(
                                pool = %self.pool_name,
                                set_id,
                                "EC erasure set layout recorded; topology labels no longer move \
                                 its slots"
                            );
                            known.insert(*set_id, record);
                        }
                        Err(error) => tracing::warn!(
                            pool = %self.pool_name,
                            set_id,
                            error = %error,
                            "EC erasure set layout could not be recorded; retrying at the next \
                             refresh"
                        ),
                    }
                }
                (_, read) => {
                    let inherited = *set_id == self.default_set.set_id;
                    let held = held_until_known(
                        inherited,
                        has_cluster_read_path,
                        known.contains_key(set_id),
                    );
                    if held {
                        unresolved.insert(*set_id);
                    }
                    // Without a cluster read path there is nothing to wait
                    // for, and saying so on every refresh would only be noise.
                    if has_cluster_read_path {
                        let why = match read {
                            SetLayoutRead::Unknown(reason) => {
                                format!("the record could not be read: {reason}")
                            }
                            _ => "the pool's replicas hold no record, and only a node of the set \
                                  records one"
                                .to_string(),
                        };
                        if held {
                            tracing::warn!(
                                pool = %self.pool_name,
                                set_id,
                                reason = %why,
                                "EC erasure set layout record is not known to this node; the set \
                                 serves reads but takes no new objects here until the record is \
                                 read"
                            );
                        } else {
                            tracing::warn!(
                                pool = %self.pool_name,
                                set_id,
                                reason = %why,
                                "EC erasure set layout record could not be confirmed; this node \
                                 keeps the layout it knows"
                            );
                        }
                    }
                }
            }
        }

        *self.set_layouts.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) = known.clone();
        SetLayouts {
            records: known,
            unresolved,
        }
    }

    /// Log that a newly taken EC v1 record differs from the layout this
    /// node's labels give.
    ///
    /// Only a question for an EC v1 record: a set laid out by a rule never
    /// had the order the labels give. A record naming other nodes is reported
    /// where it is refused, not here.
    fn warn_if_labels_disagree(
        &self,
        set_id: u64,
        nodes: &[NodeId],
        built: Option<&ErasureSet>,
        record: &EcSetLayoutRecord,
    ) {
        let Some(set) = built else {
            return;
        };
        if record.rule != EcSetLayoutRule::V1
            || !ErasureSetBuilder::record_fits(nodes, &record.node_ids)
        {
            return;
        }
        let computed = set.slot_assignment.slot_order();
        let moved = record
            .node_ids
            .iter()
            .zip(&computed)
            .filter(|(recorded, computed)| recorded != computed)
            .count();
        if moved > 0 {
            tracing::warn!(
                pool = %self.pool_name,
                set_id,
                slots_that_differ = moved,
                "EC erasure set keeps its recorded layout, which differs from the one this \
                 node's topology labels give: the labels changed after the layout was recorded, \
                 or differ between nodes. Nothing is moved"
            );
        }
    }

    /// Bring this node's view of the pool's sets up to date, returning the
    /// classified rejection when it cannot.
    async fn try_refresh_erasure_sets(&self) -> Result<EcErasureSetRoster, RosterRejection> {
        let roster = self.read_erasure_set_roster().await?;
        let layouts = self.refresh_set_layouts(&roster).await;
        let registry = self.registry_from_roster(&roster, &layouts)?;
        if self.placement.replace_sets(registry) {
            tracing::info!(
                pool = %self.pool_name,
                sets = roster.sets.len() + 1,
                "EC pool erasure sets updated"
            );
        }
        Ok(roster)
    }

    /// Bring this node's view of the pool's sets up to date.
    ///
    /// Returns the roster it settled on. Called before every set-management
    /// request so an operator never edits a stale list, and on a timer so a node
    /// that took no requests still learns about a set another node added.
    pub async fn refresh_erasure_sets(&self) -> Result<EcErasureSetRoster, ClusterError> {
        self.try_refresh_erasure_sets()
            .await
            .map_err(RosterRejection::into_cluster_error)
    }

    /// Take the pool's erasure sets at startup, refusing to start only when the
    /// reason this node cannot is its own configuration.
    ///
    /// Reads the cluster once the read path is configured, and only this
    /// node's local copies before that. Startup calls it both ways: first
    /// before any worker starts, so the first object encoded after a restart
    /// already follows the recorded layouts instead of whatever the labels
    /// give today, then again once the cluster can answer.
    pub async fn adopt_erasure_sets_at_startup(&self) -> Result<(), ClusterError> {
        match self.try_refresh_erasure_sets().await {
            Ok(_) => Ok(()),
            Err(RosterRejection::NodeConfiguration(error)) => Err(error),
            Err(RosterRejection::PoolState(error)) => {
                tracing::error!(
                    pool = %self.pool_name,
                    error = %error,
                    "Enterprise EC erasure set roster rejected by pool state; this node starts with the sets its configuration describes"
                );
                Ok(())
            }
            Err(RosterRejection::Transient(error)) => {
                tracing::warn!(
                    pool = %self.pool_name,
                    error = %error,
                    "Enterprise EC erasure set roster could not be read at startup; this node starts with the sets its configuration describes"
                );
                Ok(())
            }
        }
    }

    // -----------------------------------------------------------------------
    // Returning to the pool (plan 08, phase 2)
    // -----------------------------------------------------------------------

    /// Decide what this node may do on its way back into the pool, and act on
    /// the answer.
    ///
    /// Called once per start, after the pool's erasure sets have been taken and
    /// before this node serves a single EC read. The verdict comes from
    /// [`EcRepairCoordinator::evaluate_rejoin`](crate::ec_repair::EcRepairCoordinator::evaluate_rejoin),
    /// which until now no production path called (decision EC2-141); what this
    /// adds is the evidence to ask it about and the state its answer acts on.
    ///
    /// Refreshes this node's presence record on the way out, so the next start
    /// can tell how long this one was away — and, while the node is still held
    /// back, so that restarting it is not mistaken for rebuilding it.
    ///
    /// # Errors
    ///
    /// Returns a storage error when the presence record cannot be read or
    /// written. The verdict itself never fails: a node with no evidence about
    /// its own return is not held back on suspicion.
    pub async fn evaluate_node_rejoin(
        &self,
        identity_origin: NodeIdentityOrigin,
    ) -> Result<EcRejoinStatus, ClusterError> {
        let last_participation =
            self.metadata.get_node_presence().await.map_err(cluster_storage_error)?;
        let evidence = EcNodeReturn {
            identity_origin,
            last_participation,
            owns_slots: self.node_owns_slots(),
        };

        let now = unix_time_nanos();
        let status = self.rejoin.evaluate(
            &self.repair,
            evidence,
            now,
            self.config.rejoin_gate_enforced,
        );
        self.record_node_presence(now, !status.serves_ec_reads()).await?;
        self.publish_rejoin_status(&status, "node evaluated its return to the pool");
        Ok(status)
    }

    /// This node's standing verdict about its own return.
    pub fn rejoin_status(&self) -> EcRejoinStatus {
        self.rejoin.status()
    }

    /// Whether this node may serve EC reads right now.
    fn serves_ec_reads(&self) -> bool {
        self.rejoin.serves_ec_reads()
    }

    /// The refusal a held-back node answers an EC read with.
    ///
    /// `Unavailable` rather than a not-found: the object exists and the pool can
    /// serve it, just not through this node yet. That is a 503 to the client,
    /// which is what makes a retry against another node the right response.
    fn rejoin_read_refusal(&self) -> EcObjectReadError {
        let status = self.rejoin_status();
        EcObjectReadError::Unavailable {
            reason: format!(
                "node {} is not serving erasure-coded reads: its return was decided '{}' \
                 ({}). Rebuild its shards with POST /api/admin/ec/pools/{}/repair and then \
                 POST /api/admin/ec/pools/{}/rejoin/approve",
                self.local_node_id,
                rejoin_decision_label(status.decision),
                status.reason.as_str(),
                self.pool_name,
                self.pool_name,
            ),
        }
    }

    /// Record an operator's statement that this node's return is acceptable.
    ///
    /// The statement names one identity and applies to one return: the verdict
    /// lives only as long as this process, so a restart evaluates the evidence
    /// again and asks again. It is never stored as a setting.
    ///
    /// Named apart from the admin trait's `approve_node_rejoin` on purpose: an
    /// inherent method of the same name would win method resolution inside the
    /// trait impl, and the delegation there would read as recursion to everyone
    /// who has not memorised that rule.
    ///
    /// # Errors
    ///
    /// Returns a configuration error when the request names another node, and a
    /// storage error when the refreshed presence record cannot be written.
    pub async fn apply_rejoin_approval(
        &self,
        node_id: NodeId,
    ) -> Result<EcRejoinStatus, ClusterError> {
        if node_id != self.local_node_id {
            return Err(ClusterError::Config(format!(
                "this node is {}, so it cannot approve the return of {node_id}. A rejoin approval \
                 is addressed to the node that is held back; send it to that node's admin API",
                self.local_node_id
            )));
        }

        let now = unix_time_nanos();
        let status = self.rejoin.approve(&self.repair, now);
        self.record_node_presence(now, !status.serves_ec_reads()).await?;
        self.publish_rejoin_status(&status, "operator approved this node's return to the pool");
        Ok(status)
    }

    /// Whether the pool's layout gives this node any slot to hold.
    ///
    /// A node that holds no slot owes the pool no shards, so nothing about its
    /// return can leave it behind. This is also the honest answer for a machine
    /// that came back with a fresh identity: the pool does not know that name.
    fn node_owns_slots(&self) -> bool {
        self.placement
            .sets()
            .sets()
            .into_iter()
            .any(|set| set.slot_assignment.distinct_nodes().contains(&self.local_node_id))
    }

    /// Write this node's presence record, creating it on the first start.
    async fn record_node_presence(
        &self,
        now: u64,
        rebuild_pending: bool,
    ) -> Result<(), ClusterError> {
        let existing = self.metadata.get_node_presence().await.map_err(cluster_storage_error)?;
        let record = match existing {
            // A record written by another identity belongs to storage this node
            // has taken over; it is replaced rather than moved forward, because
            // its dates are about a node that is gone.
            Some(record) if record.node_id == self.local_node_id => {
                record.refreshed(now, rebuild_pending)
            }
            _ => EcNodePresenceRecord::first(
                self.local_node_id,
                self.pool_name.clone(),
                now,
                rebuild_pending,
            ),
        };
        self.metadata.put_node_presence(&record).await.map_err(cluster_storage_error)
    }

    /// Move this node's presence record forward while it runs.
    ///
    /// Without this the record would only ever say when the node last started,
    /// and a node that ran for a month would look as if it had been away for
    /// one. Called from the erasure-set refresher, which is the one worker that
    /// runs on every node whatever else is switched off.
    async fn refresh_node_presence(&self) {
        let rebuild_pending = !self.serves_ec_reads();
        if let Err(error) = self.record_node_presence(unix_time_nanos(), rebuild_pending).await {
            tracing::warn!(
                pool = %self.pool_name,
                node_id = %self.local_node_id,
                error = %error,
                "EC node presence record could not be refreshed; a long run may read as a long absence"
            );
        }
    }

    /// Publish a verdict to the log and to the metrics.
    fn publish_rejoin_status(&self, status: &EcRejoinStatus, message: &'static str) {
        ec_metrics::set_node_rejoin(
            status.serves_ec_reads(),
            rejoin_decision_label(status.decision),
        );
        if status.serves_ec_reads()
            && status.reason != crate::ec_rejoin::EcRejoinReason::NotEvaluated
        {
            tracing::info!(
                pool = %self.pool_name,
                node_id = %self.local_node_id,
                decision = rejoin_decision_label(status.decision),
                reason = status.reason.as_str(),
                downtime_secs = status.downtime_secs,
                admin_override = status.admin_overrode_decision(),
                "{message}"
            );
        } else if !status.serves_ec_reads() {
            tracing::warn!(
                pool = %self.pool_name,
                node_id = %self.local_node_id,
                decision = rejoin_decision_label(status.decision),
                reason = status.reason.as_str(),
                downtime_secs = status.downtime_secs,
                "{message}; it holds slots it cannot answer for and serves no erasure-coded reads \
                 until its shards are rebuilt"
            );
        }
    }

    /// Install the source the repacker asks about Object Lock retention.
    ///
    /// Called by the layer that can see both EC and Object Lock. Until it is,
    /// and in a build without Object Lock at all, the repacker is told nothing
    /// is held — which is the correct answer there and the safe one anywhere:
    /// a segment without retention repacks normally (`EC2-065`).
    ///
    /// Installed once. A second call is ignored rather than swapping the source
    /// underneath a running sweep.
    pub fn install_packed_retention_source(&self, source: Arc<dyn PackedSegmentRetentionSource>) {
        let _ = self.packed_retention.set(source);
    }

    /// The retention source in force, or the one that reports no holds.
    fn packed_retention_source(&self) -> Arc<dyn PackedSegmentRetentionSource> {
        self.packed_retention
            .get()
            .map(Arc::clone)
            .unwrap_or_else(|| Arc::new(NoObjectLockRetention))
    }

    /// Seal every open packed segment that has waited long enough.
    ///
    /// Returns how many were sealed.
    ///
    /// The size condition seals a segment on the write that overflows it, so a
    /// busy pool never needs this. A quiet one does: without a tick, a segment
    /// that stopped receiving objects stays open until the next small write,
    /// which may never come, and its contents keep costing three copies for as
    /// long as that lasts. The age limit of `EC2-060` is a promise, and this is
    /// what keeps it.
    ///
    /// # Errors
    ///
    /// Returns a storage error when the seal cannot be made durable. The
    /// in-memory pool has already sealed the segment at that point; the next
    /// tick writes it again, which is safe because sealing is idempotent.
    pub async fn seal_expired_packed_segments(&self, now: u64) -> Result<usize, ClusterError> {
        if !self.config.packed_segments.enabled {
            return Ok(0);
        }
        let mut runtime = self.packed_segments.lock().await;
        runtime.ensure_loaded(&self.metadata, self.config.admin_scan_limit).await?;

        let mut sealed_segments = Vec::new();
        for pool in runtime.pools.values_mut() {
            if pool.seal_expired(now).is_empty() {
                continue;
            }
            sealed_segments.extend(pool.take_sealed());
        }
        drop(runtime);

        let mut sealed = 0usize;
        for segment in sealed_segments {
            // The record supersedes the journal, so this one write is the whole
            // seal: after it the segment is described by its index alone.
            self.metadata
                .seal_packed_segment(&segment)
                .await
                .map_err(cluster_storage_error)?;
            tracing::info!(
                segment_id = %segment.segment_id,
                erasure_set_id = segment.erasure_set_id,
                objects = segment.entries.len(),
                bytes = segment.used_bytes,
                reason = ?segment.seal_reason,
                "sealed a packed segment that reached its age limit"
            );
            sealed = sealed.saturating_add(1);
        }
        Ok(sealed)
    }

    /// Start the background task that seals packed segments left waiting.
    ///
    /// Returns `None` when the task is already running or packing is off.
    pub fn start_packed_segment_maintenance(
        self: &Arc<Self>,
    ) -> Option<tokio::task::JoinHandle<()>> {
        if !self.config.packed_segments.enabled {
            return None;
        }
        if self
            .packed_segment_maintenance_started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return None;
        }
        // A quarter of the age limit, so a segment is sealed within 25% of the
        // deadline it was promised rather than at some unrelated cadence.
        let interval = (self.config.packed_segments.max_age / 4)
            .max(Duration::from_secs(5))
            .min(Duration::from_secs(60));
        let service = Arc::clone(self);
        Some(tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                match service.seal_expired_packed_segments(unix_time_nanos()).await {
                    Ok(0) => {}
                    Ok(sealed) => {
                        tracing::debug!(sealed, "packed segment maintenance sealed segments")
                    }
                    Err(error) => {
                        tracing::warn!(%error, "packed segment maintenance cycle failed");
                    }
                }
            }
        }))
    }

    /// Start the background task that keeps this node's set list current.
    ///
    /// Returns `None` when the task is already running. An operator adds a set
    /// on one node; without this, the others would only learn about it on their
    /// next restart, and until then would refuse to read its objects.
    pub fn start_erasure_set_refresher(self: &Arc<Self>) -> Option<tokio::task::JoinHandle<()>> {
        if self
            .set_refresher_started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return None;
        }
        let service = Arc::clone(self);
        let interval = service.config.erasure_set_refresh_interval;
        Some(tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                // The one worker every node runs whatever else is switched off,
                // which is why the presence record rides along here: a node
                // that never refreshed it would read its own uptime as absence
                // on the next start (plan 08, phase 2).
                service.refresh_node_presence().await;
                match service.try_refresh_erasure_sets().await {
                    Ok(_) => {}
                    Err(RosterRejection::NodeConfiguration(error)) => {
                        tracing::error!(
                            pool = %service.pool_name,
                            error = %error,
                            "EC erasure set refresh rejected by node configuration; this node keeps the sets it knows"
                        );
                    }
                    Err(RosterRejection::PoolState(error)) => {
                        tracing::error!(
                            pool = %service.pool_name,
                            error = %error,
                            "EC erasure set refresh rejected by pool state; this node keeps the sets it knows"
                        );
                    }
                    Err(RosterRejection::Transient(error)) => {
                        tracing::warn!(
                            pool = %service.pool_name,
                            error = %error,
                            "EC erasure set refresh failed; this node keeps the sets it knows"
                        );
                    }
                }
            }
        }))
    }

    /// The identifier the next added set takes.
    ///
    /// Dense from [`FIRST_SET_ID`], and never the inherited set's value: that
    /// one carries the pool's EC v1 identity and every manifest written before
    /// the decoupling resolves to it (decision EC2-041).
    fn next_set_id(&self, roster: &EcErasureSetRoster) -> u64 {
        let inherited = self.default_set.set_id;
        let mut candidate = FIRST_SET_ID;
        loop {
            if candidate != inherited && roster.get(candidate).is_none() {
                return candidate;
            }
            candidate = candidate.saturating_add(1);
        }
    }

    /// The pool's sets and their free space, as the admin surface reports them.
    ///
    /// One computation feeds both the per-set lines and the pool total, so the
    /// aggregate can never disagree with its parts (`EC2-MSET-04-007`).
    fn erasure_sets_snapshot(&self) -> Value {
        let sets = self.placement.sets();
        let capacity = self.placement.capacity_report();
        let lines = sets
            .sets()
            .into_iter()
            .map(|set| {
                let line = capacity.sets.iter().find(|line| line.set_id == set.set_id);
                let mut value = self.set_admin_snapshot(set);
                if let Some(object) = value.as_object_mut() {
                    object.insert("capacity".to_string(), set_capacity_admin_snapshot(line));
                }
                value
            })
            .collect::<Vec<_>>();

        // Which pool node holds slots in which set, and which hold none. An
        // operator adding a set has to name its nodes, and this is where the
        // free ones can be seen; without it the endpoint would be unusable
        // without reading the node configuration by hand.
        let owners = self.slot_owners(&sets);
        let unassigned = owners
            .iter()
            .filter(|owner| owner.set_id.is_none())
            .map(|owner| owner.node_id.to_string())
            .collect::<Vec<_>>();
        let assignment = owners.iter().map(SlotOwner::admin_snapshot).collect::<Vec<_>>();

        json!({
            "pool": self.pool_name,
            "pool_id": self.default_set.pool_id.as_u32(),
            "multi_set_enabled": self.placement.config().multi_set_enabled,
            "all_nodes_speak_v2": self.all_pool_nodes_speak_v2(),
            "placement_strategy": self.placement.config().strategy,
            "profile_width": self.set_builder.profile().total_shards(),
            "nodes": assignment,
            "unassigned_nodes": unassigned,
            // Whether the `membership` of each node above was read from a
            // membership view at all. Without one every node reads "unknown",
            // and that is a statement about this node, not about the pool.
            "membership_available": self.membership.get().is_some(),
            "sets": lines,
            "capacity": {
                // The sum of the sets that reported, and how many did. A total
                // taken over an incomplete set list is not the pool's free
                // space, and saying how many contributed is what lets an
                // operator tell the two apart.
                "available_bytes": capacity.available_bytes,
                "sets_measured": capacity.sets_measured,
                "sets_total": capacity.sets_total,
                "full_threshold": capacity.full_threshold,
            },
        })
    }

    /// Every node of the pool, with the set and slots it holds and how the
    /// cluster currently sees it.
    ///
    /// This is what an operator writes down before an outage and reads after
    /// one: it is the only place that puts a node's identity, the slots that
    /// identity owns, and its liveness side by side. A node the pool assigns
    /// slots to but cannot see is a replacement waiting to happen
    /// (`EC2-NREP-01-001`).
    ///
    /// Built from this node's pool roster, which is fixed at startup. That is
    /// deliberate: a machine that came back with a fresh identity is not in it,
    /// and the slots stay where the pool put them — which is exactly the state
    /// an operator has to be able to see (EC2-140).
    fn slot_owners(&self, sets: &ErasureSetRegistry) -> Vec<SlotOwner> {
        let membership = self.membership.get();
        self.set_builder
            .pool_nodes()
            .into_iter()
            .map(|node_id| {
                let holder = sets
                    .sets()
                    .into_iter()
                    .find(|set| set.slot_assignment.distinct_nodes().contains(&node_id));
                let slots = holder
                    .map(|set| {
                        set.slot_assignment
                            .slots()
                            .iter()
                            .filter(|slot| slot.node_id == node_id)
                            .map(|slot| slot.slot_index)
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                SlotOwner {
                    node_id,
                    set_id: holder.map(|set| set.set_id),
                    slots,
                    membership: match membership {
                        None => SlotOwnerMembership::Unknown,
                        Some(membership) => match membership.node_status(node_id) {
                            Some(s4_cluster::NodeStatus::Alive) => SlotOwnerMembership::Alive,
                            Some(s4_cluster::NodeStatus::Suspect) => SlotOwnerMembership::Suspect,
                            Some(s4_cluster::NodeStatus::Dead) => SlotOwnerMembership::Dead,
                            Some(s4_cluster::NodeStatus::Left) => SlotOwnerMembership::Left,
                            None => SlotOwnerMembership::NotAMember,
                        },
                    },
                }
            })
            .collect()
    }

    /// Configure the cluster membership query handle for rolling upgrade checks
    /// and capacity-aware placement.
    ///
    /// Must be called before [`Self::start_tiering_worker`]: until it is, the
    /// pool cannot be confirmed uniform and no migration may start. It is also
    /// the only place the placement router is given a view of node capacity;
    /// without it the router places without capacity data, which is a working
    /// state rather than a broken one.
    pub fn configure_cluster_membership(
        &self,
        membership: Arc<dyn s4_cluster::ClusterMembershipQuery>,
    ) {
        self.placement
            .configure_capacity_source(Arc::new(EcClusterCapacity::new(Arc::clone(&membership))));
        self.placement.configure_membership(Arc::clone(&membership));
        if self.membership.set(membership).is_err() {
            tracing::debug!("Enterprise EC cluster membership was already configured");
        }
    }

    /// One set as the admin surface reports it, with where its slots sit.
    ///
    /// The placement is built from this node's topology labels, which is the
    /// only view a node has; a pool whose nodes are labelled differently
    /// answers differently through each of them.
    fn set_admin_snapshot(&self, set: &ErasureSet) -> Value {
        erasure_set_admin_snapshot(set, Some(&self.set_builder.domain_placement(set)))
    }

    /// How every set of the pool stands against the rule it was laid out by.
    ///
    /// Each set is judged by its own rule and this node's topology labels. A
    /// set created in breach of its rule under `warn` says so in its layout
    /// record, which is how a violator since birth is told apart from one that
    /// drifted (EC2-083, EC2-086 point 3).
    fn set_layout_compliance(&self) -> Vec<(u64, SetLayoutCompliance)> {
        let records = self.known_set_layouts();
        self.placement
            .sets()
            .sets()
            .into_iter()
            .map(|set| {
                let created_in_breach =
                    records.get(&set.set_id).is_some_and(|record| record.violation.is_some());
                (
                    set.set_id,
                    self.set_builder.assess_set_layout(set, created_in_breach),
                )
            })
            .collect()
    }

    /// Judge every set against its own layout rule and publish the result.
    ///
    /// Cheap by construction: it compares the sets this node already holds in
    /// memory against the labels it started with, reading no manifest and no
    /// shard. That is what lets the anti-entropy pass run it every time without
    /// competing with the work that pass exists for (`EC2-ZONE-04-004`).
    ///
    /// Drift is reported and never corrected: the recorded layout stays as it
    /// is, and laying a set out again is an explicit operator action
    /// (EC2-086, point 3).
    fn check_layout_drift(&self) -> EcLayoutDriftReport {
        let assessments = self.set_layout_compliance();
        for (set_id, compliance) in &assessments {
            match compliance {
                SetLayoutCompliance::Violated {
                    origin: LayoutViolationOrigin::Drift,
                    coincidences,
                    detail,
                } => tracing::warn!(
                    pool = %self.pool_name,
                    set_id,
                    coincidences,
                    detail = %detail,
                    "EC erasure set no longer meets the layout rule it was laid out by: the \
                     topology labels changed under it. Nothing is moved, and the set's objects \
                     stay readable; restore the labels, accept the risk, or lay the set out again \
                     with the pool topology endpoint"
                ),
                SetLayoutCompliance::NotJudgeable { detail } => tracing::debug!(
                    pool = %self.pool_name,
                    set_id,
                    detail = %detail,
                    "EC erasure set layout rule cannot be judged from this node's topology labels"
                ),
                _ => {}
            }
        }

        let report = collect_layout_drift(assessments);
        ec_metrics::set_layout_violations(&self.known_set_ids(), &report.violations);
        report
    }

    /// Ids of every erasure set this pool has, in registry order.
    ///
    /// Per-set gauges publish a value for each of them, including the sets with
    /// nothing to report, so a drained queue reads as zero rather than as a
    /// series that stopped moving.
    fn known_set_ids(&self) -> Vec<u64> {
        self.placement.sets().sets().iter().map(|set| set.set_id).collect()
    }

    /// Whether every node in this pool is confirmed to support EC v2 auto-tiering.
    pub fn is_cluster_tiering_supported(&self) -> bool {
        pool_supports_ec_v2(&self.membership, &self.placement.sets())
    }

    /// Give the tiering surface a cluster-wide view of the catalog.
    pub fn configure_tiering_catalog(&self, list_coordinator: Arc<DistributedListCoordinator>) {
        if self.catalog.set(list_coordinator).is_err() {
            tracing::debug!("Enterprise EC tiering catalog was already configured");
        }
    }

    /// Start the background flusher of the object activity tracker.
    ///
    /// Returns `None` when activity tracking is switched off or the flusher is
    /// already running. Until it starts, signals fill the bounded channel and
    /// are then dropped, which costs accuracy and nothing else.
    pub fn start_heat_tracker(&self) -> Option<tokio::task::JoinHandle<()>> {
        self.heat.start_flusher(Arc::clone(&self.metadata))
    }

    /// Start the background auto-tiering migration worker.
    ///
    /// Returns `None` when tiering is switched off or the worker is already
    /// running. It must be started after the read runtime and the catalog are
    /// configured, because the first cycle needs both.
    pub fn start_tiering_worker(self: &Arc<Self>) -> Option<tokio::task::JoinHandle<()>> {
        if !self.config.tiering.enabled {
            return None;
        }
        if self
            .tiering_worker_started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return None;
        }

        // Without a membership handle the rolling-upgrade gate cannot confirm
        // the pool is uniform, so every cycle would stop on `mixed_cluster`. A
        // worker that can never migrate anything is worse than an absent one:
        // it reports itself as wired, and the operator reads a real diagnosis
        // ("the pool is mixed") for a wiring mistake. Say what is actually
        // wrong instead, and leave `tiering_worker_wired` false.
        if self.membership.get().is_none() {
            self.tiering_worker_started.store(false, Ordering::Release);
            tracing::error!(
                "Enterprise EC auto-tiering worker cannot start: cluster membership is not \
                 configured, so the rolling-upgrade gate has nothing to check the pool against"
            );
            return None;
        }

        let environment = match self.tiering_environment() {
            Ok(environment) => environment,
            Err(error) => {
                self.tiering_worker_started.store(false, Ordering::Release);
                tracing::warn!(
                    error = %error,
                    "Enterprise EC auto-tiering worker cannot start on this node"
                );
                return None;
            }
        };
        let worker = EcTieringWorker::new(self.tiering_worker_config(), Arc::new(environment));
        Some(tokio::spawn(worker.run()))
    }

    /// Build the promotion worker's view of this node and its pool.
    fn promotion_environment(&self) -> Result<EcPromotionServiceEnvironment, ClusterError> {
        let deps = self.read_runtime.get().ok_or_else(|| {
            ClusterError::Config("EC read runtime is not configured on this node".to_string())
        })?;
        Ok(EcPromotionServiceEnvironment {
            erasure_set: self.default_set.clone(),
            sets: self.placement.sets_handle(),
            scan_limit: self.config.admin_scan_limit,
            promote_window_nanos: seconds_to_nanos(self.config.tiering.promote_window_secs),
            metadata: Arc::clone(&self.metadata),
            read_coordinator: Arc::clone(&deps.read_coordinator),
            write_coordinator: Arc::clone(&deps.write_coordinator),
            ports: self.read_ports(deps),
            gc: EcGcCoordinator::new(
                Arc::clone(&self.metadata),
                self.config.gc.clone(),
                self.placement.sets_handle(),
            ),
            local_node_id: self.local_node_id,
        })
    }

    /// Start the background reverse migration worker.
    ///
    /// Returns `None` when the worker is already running or the node cannot
    /// build its environment. Deliberately not gated on `tiering.enabled`: the
    /// queue is also what the operator's rollback command writes into, and a
    /// worker that stops draining it the moment the feature is switched off
    /// would take the rollback away exactly when it is needed. Decision
    /// EC2-023.
    pub fn start_promotion_worker(self: &Arc<Self>) -> Option<tokio::task::JoinHandle<()>> {
        if self
            .promotion_worker_started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return None;
        }

        let environment = match self.promotion_environment() {
            Ok(environment) => environment,
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    "Enterprise EC promotion worker cannot start on this node"
                );
                return None;
            }
        };
        let worker = EcPromotionWorker::new(self.promotion_worker_config(), Arc::new(environment));
        Some(tokio::spawn(worker.run()))
    }

    /// Cycle limits the promotion worker runs under.
    fn promotion_worker_config(&self) -> EcPromotionWorkerConfig {
        promotion_worker_config(&self.config.tiering, &self.config.gc)
    }

    /// Cycle limits the migration worker runs under.
    fn tiering_worker_config(&self) -> EcTieringWorkerConfig {
        let tiering = &self.config.tiering;
        EcTieringWorkerConfig {
            enabled: tiering.enabled,
            scan_interval: Duration::from_secs(tiering.scan_interval_secs),
            scan_limit: tiering.scan_limit,
            enqueue_per_cycle: tiering.enqueue_per_cycle,
            max_queue_depth: tiering.max_queue_depth,
            promote_per_cycle: tiering.promote_per_cycle,
            max_promotion_queue_depth: tiering.max_promotion_queue_depth,
        }
    }

    /// Start the supervised EC transcode worker for this EE pool.
    pub fn start_transcode_worker(
        self: &Arc<Self>,
        read_coordinator: Arc<QuorumReadCoordinator>,
        write_coordinator: Arc<QuorumWriteCoordinator>,
        storage: Arc<dyn StorageEngine>,
        rpc_client: Arc<NodeClient>,
        topology_epoch: u64,
    ) -> Result<Option<tokio::task::JoinHandle<()>>, ClusterError> {
        self.configure_read_runtime(
            Arc::clone(&read_coordinator),
            Arc::clone(&write_coordinator),
            Arc::clone(&storage),
            Arc::clone(&rpc_client),
            topology_epoch,
        );

        if !self.config.transcode_enabled {
            return Ok(None);
        }
        if self
            .transcode_worker_started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Ok(None);
        }

        let runtime = EcTranscodeWorkerRuntime::new(EcTranscodeRuntimeDeps {
            local_node_id: self.local_node_id,
            topology_epoch,
            sets: self.placement.sets_handle(),
            default_set: self.default_set.clone(),
            metadata: Arc::clone(&self.metadata),
            read_coordinator,
            write_coordinator,
            storage,
            rpc_client,
            codec_limiter: self.codec_limiter.clone(),
            transcode: self.config.transcode.clone(),
            worker: self.config.transcode_worker.clone(),
            gc: self.config.gc.clone(),
            packed_segments: self.config.packed_segments,
            packed_repack: self.config.packed_repack,
            packed_retention: self.packed_retention_source(),
            scan_limit: self.config.admin_scan_limit,
            tiering_promote_window_nanos: seconds_to_nanos(self.config.tiering.promote_window_secs),
            minimal_read: self.config.minimal_read_enabled,
        });
        let started = Arc::clone(&self.transcode_worker_started);
        let handle = tokio::spawn(async move {
            let _guard = TranscodeWorkerStartedGuard { started };
            runtime.run().await;
        });
        Ok(Some(handle))
    }

    /// Start supervised EC repair, anti-entropy, and scrub workers for this EE pool.
    pub fn start_integrity_workers(
        self: &Arc<Self>,
        read_coordinator: Arc<QuorumReadCoordinator>,
        write_coordinator: Arc<QuorumWriteCoordinator>,
        storage: Arc<dyn StorageEngine>,
        rpc_client: Arc<NodeClient>,
        topology_epoch: u64,
    ) -> Result<Option<tokio::task::JoinHandle<()>>, ClusterError> {
        self.configure_read_runtime(
            Arc::clone(&read_coordinator),
            Arc::clone(&write_coordinator),
            Arc::clone(&storage),
            Arc::clone(&rpc_client),
            topology_epoch,
        );

        if !self.config.integrity_worker.repair_enabled
            && !self.config.integrity_worker.anti_entropy_enabled
            && !self.config.integrity_worker.scrub_enabled
        {
            return Ok(None);
        }
        if self
            .repair_worker_started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Ok(None);
        }
        self.anti_entropy_worker_started.store(
            self.config.integrity_worker.anti_entropy_enabled
                || self.config.integrity_worker.scrub_enabled,
            Ordering::Release,
        );

        let deps = EcObjectReadRuntimeDeps {
            read_coordinator,
            write_coordinator,
            storage,
            rpc_client,
            topology_epoch,
        };
        let runtime = match EcIntegrityWorkerRuntime::new(Arc::clone(self), deps) {
            Ok(runtime) => runtime,
            Err(error) => {
                self.repair_worker_started.store(false, Ordering::Release);
                self.anti_entropy_worker_started.store(false, Ordering::Release);
                return Err(error);
            }
        };
        let repair_started = Arc::clone(&self.repair_worker_started);
        let anti_entropy_started = Arc::clone(&self.anti_entropy_worker_started);
        let handle = tokio::spawn(async move {
            let _guard = IntegrityWorkerStartedGuard {
                repair_started,
                anti_entropy_started,
            };
            runtime.run().await;
        });
        Ok(Some(handle))
    }

    fn configure_read_runtime(
        &self,
        read_coordinator: Arc<QuorumReadCoordinator>,
        write_coordinator: Arc<QuorumWriteCoordinator>,
        storage: Arc<dyn StorageEngine>,
        rpc_client: Arc<NodeClient>,
        topology_epoch: u64,
    ) {
        let read_runtime = EcObjectReadRuntimeDeps {
            read_coordinator,
            write_coordinator,
            storage,
            rpc_client,
            topology_epoch,
        };
        if self.read_runtime.set(read_runtime).is_err() {
            tracing::debug!("Enterprise EC object read runtime was already configured");
        }
    }

    /// Put back namespace entries that erasure-coded objects are missing.
    ///
    /// Two situations produce a manifest with no matching entry in the object
    /// index, and both make the object invisible to every client that
    /// enumerates a bucket while it stays perfectly readable by key:
    ///
    /// 1. The object was cut over to EC before cutover learned to keep the
    ///    entry. Those objects are still out there in existing clusters.
    /// 2. A replica was unreachable when its peers converted their entry, so it
    ///    still holds a hot copy that nothing will reclaim.
    ///
    /// Both are repaired from the manifest, which carries everything a client
    /// can see: size, ETag, content type, user metadata and the HLC generation.
    ///
    /// The pass runs wherever a manifest is known and repairs through the
    /// quorum coordinators, because the node holding a manifest is not
    /// necessarily one of the object's replicas: manifests follow the erasure
    /// set's slots, entries follow the router. Two nodes reaching the same
    /// conclusion is harmless — both repairs are idempotent.
    /// Whether the pool agrees that this object's hot copies are leftovers.
    ///
    /// Asked only before releasing hot copies, because that is the one thing
    /// this pass does that cannot be undone by the next one. A local manifest
    /// is a cache, and the answer it gives about an object being promoted is
    /// stale by construction: the promotion is carried out by one node and
    /// reaches the others through the metadata quorum. The rule itself is
    /// [`quorum_says_hot_copies_are_leftovers`].
    ///
    /// A pool that cannot answer counts as "do not touch": an unreachable
    /// quorum is exactly when this pass should keep its hands off live copies.
    async fn hot_copies_are_leftovers_at_quorum(&self, manifest: &EcObjectManifest) -> bool {
        let Some(deps) = self.read_runtime.get() else {
            return false;
        };
        let ports = self.read_ports(deps);
        let object = manifest.object_key();
        match <EcClusterReadPorts as EcReadMetadataCoordinator>::quorum_read_manifest(
            &ports, &object,
        )
        .await
        {
            Ok(read) => quorum_says_hot_copies_are_leftovers(&read, manifest),
            Err(error) => {
                tracing::debug!(
                    bucket = %manifest.bucket,
                    key = %manifest.key,
                    error = %error,
                    "could not confirm the EC layout before releasing hot copies"
                );
                false
            }
        }
    }

    async fn reconcile_namespace_now(&self) -> Result<EcNamespaceReconcileReport, ClusterError> {
        let deps = self.read_runtime.get().ok_or_else(|| {
            ClusterError::Config("EC read runtime is not configured on this node".to_string())
        })?;
        self.reconcile_namespace_entries(&deps.read_coordinator, &deps.write_coordinator)
            .await
    }

    async fn reconcile_namespace_entries(
        &self,
        read_coordinator: &Arc<QuorumReadCoordinator>,
        write_coordinator: &Arc<QuorumWriteCoordinator>,
    ) -> Result<EcNamespaceReconcileReport, ClusterError> {
        let mut report = EcNamespaceReconcileReport::default();
        let mut cursor = None;

        loop {
            let page = self
                .metadata
                .list_manifests_page(cursor, self.config.anti_entropy.max_manifest_scan_entries)
                .await
                .map_err(cluster_storage_error)?;

            for manifest in page.entries {
                report.manifests_scanned = report.manifests_scanned.saturating_add(1);

                // A deleted or half-written manifest is not an object anybody
                // should be able to list.
                if !crate::ec_anti_entropy::manifest_is_committed_live(&manifest) {
                    continue;
                }
                // Versioned objects never took the hot RF=3 purge path, so they
                // have no entry to repair here.
                if manifest.version_id.is_some() {
                    continue;
                }

                // One object that cannot be repaired right now — a replica is
                // down, a quorum is short — must not stop the pass. The next
                // one runs in an anti-entropy interval, and the object stays
                // exactly as unreachable as it already was.
                let observed = match read_coordinator.head(&manifest.bucket, &manifest.key).await {
                    Ok(head) => observe_entry(&head, &manifest),
                    Err(ClusterError::ObjectNotFound { .. }) => ObservedEntry::Missing,
                    Err(error) => {
                        report.failed = report.failed.saturating_add(1);
                        tracing::warn!(
                            bucket = %manifest.bucket,
                            key = %manifest.key,
                            error = %error,
                            "could not read object state during reconciliation"
                        );
                        continue;
                    }
                };

                match namespace_action(manifest.state, observed) {
                    EcNamespaceAction::Nothing => {
                        report.already_current = report.already_current.saturating_add(1);
                    }
                    EcNamespaceAction::AwaitPurge => {
                        report.awaiting_purge = report.awaiting_purge.saturating_add(1);
                    }
                    EcNamespaceAction::Superseded => {
                        report.superseded = report.superseded.saturating_add(1);
                    }
                    EcNamespaceAction::ReleaseHotCopies => {
                        // Confirmed with the pool before anything is released.
                        // This branch is driven by a manifest cached locally,
                        // and the object may be on its way back to RF=3 since —
                        // by reverse tiering or by an operator's rollback — in
                        // which case those hot copies are the object, not
                        // leftovers. Releasing them would fail the promotion,
                        // or, between its check and its switch, leave readers
                        // on a hot path with nothing behind it (EC2-026).
                        if !self.hot_copies_are_leftovers_at_quorum(&manifest).await {
                            report.already_current = report.already_current.saturating_add(1);
                            tracing::debug!(
                                bucket = %manifest.bucket,
                                key = %manifest.key,
                                "hot copies kept: the pool does not agree they are leftovers"
                            );
                            continue;
                        }
                        match write_coordinator
                            .detach_object_data_if_hlc(
                                &manifest.bucket,
                                &manifest.key,
                                manifest.hlc,
                                manifest.erasure_set_id,
                                manifest.topology_epoch,
                            )
                            .await
                        {
                            Ok(_) => {
                                report.entries_detached = report.entries_detached.saturating_add(1);
                                tracing::info!(
                                    bucket = %manifest.bucket,
                                    key = %manifest.key,
                                    "released hot copies left behind by an unfinished cutover"
                                );
                            }
                            Err(error) => {
                                report.failed = report.failed.saturating_add(1);
                                tracing::warn!(
                                    bucket = %manifest.bucket,
                                    key = %manifest.key,
                                    error = %error,
                                    "could not release hot copies during reconciliation"
                                );
                            }
                        }
                    }
                    EcNamespaceAction::RestoreEntry => {
                        let record = namespace_entry_from_manifest(&manifest);
                        match write_coordinator
                            .restore_namespace_entry(&manifest.bucket, &manifest.key, &record)
                            .await
                        {
                            Ok(restored) if restored > 0 => {
                                report.entries_restored = report.entries_restored.saturating_add(1);
                                tracing::info!(
                                    bucket = %manifest.bucket,
                                    key = %manifest.key,
                                    replicas = restored,
                                    "restored namespace entry for erasure-coded object"
                                );
                            }
                            Ok(_) => {}
                            Err(error) => {
                                report.failed = report.failed.saturating_add(1);
                                tracing::warn!(
                                    bucket = %manifest.bucket,
                                    key = %manifest.key,
                                    error = %error,
                                    "could not restore namespace entry"
                                );
                            }
                        }
                    }
                }
            }

            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }

        Ok(report)
    }

    /// Run one local shard scrub pass now, without waiting for the worker.
    ///
    /// The background worker runs the same pass on its own schedule; this makes
    /// it observable at a chosen moment, which is what an acceptance test needs
    /// in order to state that a mixed-engine cluster produced no false bit rot.
    async fn scrub_now(&self) -> Result<EcShardScrubReport, ClusterError> {
        let (ports, anti_entropy) = self.integrity_ports()?;
        anti_entropy
            .scrub_local_shards(&ports, &self.repair, &ports, &ports)
            .await
            .map_err(cluster_config_error)
    }

    /// Audit every known manifest against local shard inventory, now.
    ///
    /// This is the manifest-inventory half of anti-entropy: it compares what
    /// the manifests say this node should hold against what it actually holds.
    async fn anti_entropy_now(&self) -> Result<EcShardInventoryAuditReport, ClusterError> {
        let (ports, anti_entropy) = self.integrity_ports()?;
        self.audit_all_manifests(&anti_entropy, &ports).await
    }

    /// Audit every manifest on this node and sum the reports.
    ///
    /// Shared with the integrity worker so the two cannot drift: they page
    /// through the same keyspace with the same page size, and a counter added
    /// to the report shows up in both.
    async fn audit_all_manifests(
        &self,
        anti_entropy: &EcAntiEntropyService,
        ports: &EcClusterReadPorts,
    ) -> Result<EcShardInventoryAuditReport, ClusterError> {
        let mut total = EcShardInventoryAuditReport::default();
        let mut cursor = None;

        loop {
            let page = self
                .metadata
                .list_manifests_page(cursor, self.config.anti_entropy.max_manifest_scan_entries)
                .await
                .map_err(cluster_storage_error)?;
            cursor = page.next_cursor;

            for manifest in page.entries {
                let report = anti_entropy
                    .audit_manifest_shard_inventory(&manifest, &self.repair, ports)
                    .await
                    .map_err(cluster_config_error)?;
                total.expected_local_shards =
                    total.expected_local_shards.saturating_add(report.expected_local_shards);
                total.healthy_local_shards =
                    total.healthy_local_shards.saturating_add(report.healthy_local_shards);
                total.missing_local_shards =
                    total.missing_local_shards.saturating_add(report.missing_local_shards);
                total.corrupt_local_shards =
                    total.corrupt_local_shards.saturating_add(report.corrupt_local_shards);
                total.quarantined_shards =
                    total.quarantined_shards.saturating_add(report.quarantined_shards);
                total.quarantine_failures =
                    total.quarantine_failures.saturating_add(report.quarantine_failures);
                total.repairs_queued = total.repairs_queued.saturating_add(report.repairs_queued);
                total.repair_enqueue_failures =
                    total.repair_enqueue_failures.saturating_add(report.repair_enqueue_failures);
            }

            if cursor.is_none() {
                break;
            }
        }
        Ok(total)
    }

    /// Ports and anti-entropy service for an on-demand integrity pass.
    fn integrity_ports(&self) -> Result<(EcClusterReadPorts, EcAntiEntropyService), ClusterError> {
        let deps = self.read_runtime.get().ok_or_else(|| {
            ClusterError::Config("EC read runtime is not configured on this node".to_string())
        })?;
        Ok((self.read_ports(deps), self.anti_entropy_service()?))
    }

    /// The one anti-entropy service of this node.
    ///
    /// It owns the metrics counters, so a second instance built per request
    /// would record real bit rot into an `Arc` nobody publishes.
    fn anti_entropy_service(&self) -> Result<EcAntiEntropyService, ClusterError> {
        if let Some(existing) = self.anti_entropy.get() {
            return Ok(existing.clone());
        }
        let created = EcAntiEntropyService::new(
            self.local_node_id,
            Arc::clone(&self.metadata),
            self.config.anti_entropy.clone(),
        )
        .map_err(cluster_config_error)?;

        // Another thread may win this race; either way everyone ends up using
        // the stored instance, which is what keeps the metrics in one place.
        let _ = self.anti_entropy.set(created);
        self.anti_entropy.get().cloned().ok_or_else(|| {
            ClusterError::Config("EC anti-entropy service is unavailable".to_string())
        })
    }

    fn repair_worker_effective_started(&self) -> bool {
        self.config.integrity_worker.repair_enabled
            && self.repair_worker_started.load(Ordering::Acquire)
    }

    fn anti_entropy_worker_effective_started(&self) -> bool {
        (self.config.integrity_worker.anti_entropy_enabled
            || self.config.integrity_worker.scrub_enabled)
            && self.anti_entropy_worker_started.load(Ordering::Acquire)
    }

    fn ensure_pool(&self, pool: &str) -> Result<(), ClusterError> {
        if pool == self.pool_name
            || pool == self.default_set.pool_id.to_string()
            || pool == self.default_set.pool_id.as_u32().to_string()
        {
            Ok(())
        } else {
            Err(ClusterError::Config(format!(
                "unknown EC pool '{pool}'; this node serves '{}'",
                self.pool_name
            )))
        }
    }

    async fn get_manifest(
        &self,
        object: &EcObjectVersionKey,
    ) -> Result<EcObjectManifest, ClusterError> {
        match self.metadata.get_manifest(object).await.map_err(cluster_storage_error)? {
            Some(manifest) => Ok(manifest),
            None => {
                let Some(deps) = self.read_runtime.get() else {
                    return Err(ClusterError::ObjectNotFound {
                        bucket: object.bucket.clone(),
                        key: object.key.clone(),
                    });
                };
                let ports = self.read_ports(deps);
                match <EcClusterReadPorts as EcReadMetadataCoordinator>::quorum_read_manifest(
                    &ports, object,
                )
                .await
                .map_err(ec_read_cluster_error)?
                {
                    EcManifestReadResult::Present(manifest)
                    | EcManifestReadResult::Tombstone(manifest) => {
                        self.metadata
                            .put_manifest(&manifest)
                            .await
                            .map_err(cluster_storage_error)?;
                        Ok(manifest)
                    }
                    EcManifestReadResult::Absent => Err(ClusterError::ObjectNotFound {
                        bucket: object.bucket.clone(),
                        key: object.key.clone(),
                    }),
                }
            }
        }
    }

    /// Place a small object inside an open packed segment.
    ///
    /// Answers `false` whenever packing does not apply or cannot be arranged,
    /// and the caller then stores the object as `ReplicatedSmall`. That is
    /// always a correct outcome: the object's bytes are already durable on RF=3
    /// replicas either way, and packing only decides how they will eventually be
    /// erasure coded. Nothing here is allowed to cost the client a write.
    async fn pack_small_object(
        &self,
        commit: PackedObjectCommit<'_>,
        now: u64,
    ) -> Result<bool, ClusterError> {
        if !self
            .config
            .packed_segments
            .accepts_object_size(commit.size, self.default_set.profile.min_object_size)
        {
            return Ok(false);
        }
        // Without a full replica set there is nothing to read the object from
        // while its segment is open, and packing it would trade durability for
        // space. That trade is not on offer.
        if commit.replica_nodes.len() != REPLICATED_SMALL_REPLICA_COUNT {
            return Ok(false);
        }
        let Ok(set) = self.placement.select_write_set() else {
            return Ok(false);
        };
        let set_id = set.set_id;

        let mut runtime = self.packed_segments.lock().await;
        runtime.ensure_loaded(&self.metadata, self.config.admin_scan_limit).await?;

        let admission = match runtime.pool_for(set_id).reserve(commit.size, now) {
            Ok(admission) => admission,
            Err(error) => {
                tracing::debug!(
                    bucket = %commit.object.bucket,
                    key = %commit.object.key,
                    erasure_set_id = set_id,
                    %error,
                    "packed segment refused the object; storing it replicated-small"
                );
                return Ok(false);
            }
        };

        let reservation = admission.reservation;
        let outcome =
            self.persist_packed_admission(&commit, set_id, &admission, &mut runtime).await;
        match outcome {
            Ok(item) => {
                runtime
                    .pool_for(set_id)
                    .commit(&reservation, &item)
                    .map_err(|error| cluster_storage_error(packed_segment_error(error)))?;
                Ok(true)
            }
            Err(error) => {
                // The reservation was never acknowledged, so rolling it back
                // loses nothing. The object still goes to RF=3 below.
                let _ = runtime.pool_for(set_id).abort(&reservation);
                tracing::warn!(
                    bucket = %commit.object.bucket,
                    key = %commit.object.key,
                    erasure_set_id = set_id,
                    %error,
                    "could not make the packed placement durable; storing the object \
                     replicated-small"
                );
                Ok(false)
            }
        }
    }

    /// Make everything a packed admission needs durable, in dependency order.
    ///
    /// The segments an admission opened or sealed have to be durable before the
    /// record that points into them, or recovery would replay an append into a
    /// segment it has never heard of. The object's own record goes last, in one
    /// atomic batch with its layout: that batch is what makes the write
    /// acknowledgeable.
    async fn persist_packed_admission(
        &self,
        commit: &PackedObjectCommit<'_>,
        set_id: u64,
        admission: &PackedSegmentAdmission,
        runtime: &mut PackedSegmentRuntime,
    ) -> Result<PackedSegmentJournalItem, EcMetadataError> {
        for item in &admission.prerequisites {
            if matches!(
                item.payload,
                PackedSegmentJournalPayload::SegmentSealed { .. }
            ) {
                // A sealed segment stops being described by its journal and
                // starts being described by its record, in one batch.
                // Taken, not copied: once this record is durable the pool's
                // copy is stale, and a later seal that still held it would
                // write it back over whatever the pipeline has done since.
                let sealed = runtime.pool_for(set_id).take_sealed_segment(item.segment_id);
                if let Some(segment) = sealed {
                    self.metadata.seal_packed_segment(&segment).await?;
                    continue;
                }
            }
            self.metadata.append_packed_segment_journal(item).await?;
        }

        let reservation = admission.reservation;
        let item = runtime
            .pool_for(set_id)
            .journal_append(
                &reservation,
                PackedObjectIdentity::new(
                    commit.object.clone(),
                    commit.content_hash_sha256,
                    commit.hlc,
                )
                .with_s3_metadata(
                    commit.etag,
                    commit.content_type,
                    commit.user_metadata.clone(),
                ),
            )
            .map_err(packed_segment_error)?;
        let placement = PackedSegmentObjectPlacement::new(
            reservation.segment_id,
            set_id,
            reservation.offset,
            reservation.length,
            commit.content_hash_sha256,
            commit.hlc,
            commit.replica_nodes.to_vec(),
        )?;
        self.metadata.commit_packed_object(commit.object, &placement, &item).await?;
        Ok(item)
    }

    async fn clear_object_runtime_metadata(
        &self,
        object: &EcObjectVersionKey,
        preserve_tombstone: bool,
        clear_quorum_manifest: bool,
    ) -> Result<(), ClusterError> {
        if clear_quorum_manifest {
            self.clear_quorum_manifest_if_configured(object).await?;
        }

        if let Some(manifest) =
            self.metadata.get_manifest(object).await.map_err(cluster_storage_error)?
        {
            if !(preserve_tombstone && manifest_is_tombstone(&manifest)) {
                self.metadata.delete_manifest(object).await.map_err(cluster_storage_error)?;
            }
        }
        // A packed layout has to be charged to its segment before it goes: the
        // segment cannot give the bytes back, and the dead count is the only
        // record that they stopped being in use. The call drops the layout
        // itself, atomically with the charge, and answers `None` when the object
        // was not packed at all.
        if self
            .metadata
            .record_packed_object_deleted(object)
            .await
            .map_err(cluster_storage_error)?
            .is_none()
        {
            self.metadata
                .delete_object_layout(object)
                .await
                .map_err(cluster_storage_error)?;
        }
        let removed = self
            .metadata
            .delete_transcode_jobs_for_object(object, self.config.admin_scan_limit)
            .await
            .map_err(cluster_storage_error)?;
        record_abandoned_migrations(object, removed);
        Ok(())
    }

    async fn clear_quorum_manifest_if_configured(
        &self,
        object: &EcObjectVersionKey,
    ) -> Result<(), ClusterError> {
        let Some(deps) = self.read_runtime.get() else {
            return Ok(());
        };
        let ports = self.read_ports(deps);
        ports
            .delete_manifest_quorum(object, current_hlc(self.local_node_id))
            .await
            .map_err(cluster_storage_error)
    }

    /// Read a manifest the way an operator has to see it: from the cluster.
    ///
    /// [`Self::get_manifest`] answers from the local copy first, which is right
    /// for the read path — it is a cache, and a cached manifest decodes the
    /// object just as well. It is wrong for the admin surface. A manifest is
    /// committed through quorum, so on a six-node pool two nodes may legitimately
    /// be behind, and nothing brings a lagging one forward until anti-entropy
    /// reaches it. A node in that position would report an object as still
    /// migrating for as long as anyone asked it, across restarts — which is
    /// exactly what an operator watching a migration would be misled by, and
    /// what makes two nodes behind one load balancer answer differently.
    ///
    /// Falls back to the local copy when the cluster cannot answer, so
    /// inspecting a pool with nodes down keeps working.
    async fn authoritative_manifest(
        &self,
        object: &EcObjectVersionKey,
    ) -> Result<EcObjectManifest, ClusterError> {
        let Some(deps) = self.read_runtime.get() else {
            return self.get_manifest(object).await;
        };
        let ports = self.read_ports(deps);
        match <EcClusterReadPorts as EcReadMetadataCoordinator>::quorum_read_manifest(
            &ports, object,
        )
        .await
        {
            Ok(EcManifestReadResult::Present(manifest))
            | Ok(EcManifestReadResult::Tombstone(manifest)) => {
                // Keep the local copy in step, so the next reader of this node —
                // admin or not — is no longer behind.
                if let Err(error) = self.metadata.put_manifest(&manifest).await {
                    tracing::warn!(
                        bucket = %object.bucket,
                        key = %object.key,
                        error = %error,
                        "EC manifest cache refresh failed; the cluster copy still answers"
                    );
                }
                Ok(manifest)
            }
            // Quorum says the object is not erasure coded. The local copy is
            // then the only thing that could still answer, and `get_manifest`
            // reports the same not-found error if it cannot.
            Ok(EcManifestReadResult::Absent) => self.get_manifest(object).await,
            Err(error) => {
                tracing::warn!(
                    bucket = %object.bucket,
                    key = %object.key,
                    error = %error,
                    "EC manifest quorum read failed; answering from the local copy"
                );
                self.get_manifest(object).await
            }
        }
    }

    fn read_runtime(&self) -> Result<&EcObjectReadRuntimeDeps, EcObjectReadError> {
        self.read_runtime.get().ok_or_else(|| EcObjectReadError::Unavailable {
            reason: "Enterprise EC object read runtime is not configured".to_string(),
        })
    }

    /// A read coordinator that asks for near shards first, narrowed to the
    /// `k` data shards of a chunk unless this node was told otherwise.
    fn ec_reader(&self) -> EcReadCoordinator {
        EcReadCoordinator::new(Arc::clone(&self.metadata), self.codec_limiter.clone())
            .with_topology(Arc::clone(&self.topology))
            .with_minimal_read(self.config.minimal_read_enabled)
    }

    fn read_ports(&self, deps: &EcObjectReadRuntimeDeps) -> EcClusterReadPorts {
        EcClusterReadPorts::new(EcClusterReadPortsDeps {
            local_node_id: self.local_node_id,
            topology: Arc::clone(&self.topology),
            topology_epoch: deps.topology_epoch,
            sets: self.placement.sets(),
            erasure_set: self.default_set.clone(),
            metadata: Arc::clone(&self.metadata),
            codec_limiter: self.codec_limiter.clone(),
            read_coordinator: Arc::clone(&deps.read_coordinator),
            write_coordinator: Arc::clone(&deps.write_coordinator),
            storage: Arc::clone(&deps.storage),
            rpc_client: Arc::clone(&deps.rpc_client),
            minimal_read: self.config.minimal_read_enabled,
        })
    }

    async fn select_read_plan(
        &self,
        object: &EcObjectVersionKey,
        ports: &EcClusterReadPorts,
    ) -> Result<EcObjectReadPlan, EcObjectReadError> {
        if self
            .metadata
            .get_manifest(object)
            .await
            .map_err(ec_metadata_internal)?
            .as_ref()
            .is_some_and(manifest_is_tombstone)
        {
            self.metadata.delete_object_layout(object).await.map_err(ec_metadata_internal)?;
            return Ok(EcObjectReadPlan::Deleted);
        }

        match self.metadata.get_object_layout(object).await.map_err(ec_metadata_internal)? {
            Some(ObjectDataLayout::ReplicatedSmall(_)) => Ok(EcObjectReadPlan::EcMetadata),
            // While the segment is still open the object's real bytes are the
            // RF=3 copies named in the placement, and the read path serves
            // those; the hot quorum behind them catches a stale local record.
            Some(ObjectDataLayout::PackedSegment(placement))
                if placement.hot_placement().is_some() =>
            {
                Ok(EcObjectReadPlan::EcMetadata)
            }
            // After cutover the local record is only a cache. The object's
            // authoritative location is its pointer manifest, exactly as for any
            // other erasure coded object, so it is resolved from the quorum —
            // otherwise a version written through another node would keep being
            // served from this node's stale offset. See decision EC2-068.
            Some(ObjectDataLayout::PackedSegment(_)) => {
                self.select_erasure_coded_read_plan(object, ports, None).await
            }
            Some(ObjectDataLayout::ErasureCoded(reference)) => {
                self.select_erasure_coded_read_plan(object, ports, Some(reference)).await
            }
            None => self.select_erasure_coded_read_plan(object, ports, None).await,
        }
    }

    async fn select_erasure_coded_read_plan(
        &self,
        object: &EcObjectVersionKey,
        ports: &EcClusterReadPorts,
        local_reference: Option<EcObjectManifestRef>,
    ) -> Result<EcObjectReadPlan, EcObjectReadError> {
        match <EcClusterReadPorts as EcReadMetadataCoordinator>::quorum_read_manifest(ports, object)
            .await
            .map_err(map_ec_read_error)?
        {
            EcManifestReadResult::Present(manifest) => {
                self.metadata.cache_manifest(&manifest).await.map_err(ec_metadata_internal)?;
                if !manifest_is_ec_readable(&manifest) {
                    self.metadata
                        .delete_object_layout(object)
                        .await
                        .map_err(ec_metadata_internal)?;
                    return Ok(EcObjectReadPlan::HotReplicated);
                }

                if local_reference.is_none()
                    && self.hot_replicated_is_current_or_newer(object, &manifest, ports).await?
                {
                    return Ok(EcObjectReadPlan::HotReplicated);
                }

                // A packed object's manifest describes a region of a segment
                // rather than chunks of its own, so the layout is built from the
                // placement it carries. Caching it locally is what lets every
                // node serve the object, not only the one that packed it.
                if let Some(packed) = manifest.packed.clone() {
                    self.metadata
                        .cache_object_layout(object, &ObjectDataLayout::PackedSegment(packed))
                        .await
                        .map_err(ec_metadata_internal)?;
                    return Ok(EcObjectReadPlan::EcMetadata);
                }

                if let Some(reference) = local_reference {
                    if reference.matches_manifest(&manifest).map_err(ec_metadata_internal)? {
                        return Ok(EcObjectReadPlan::EcMetadata);
                    }
                }

                let reference =
                    EcObjectManifestRef::from_manifest(&manifest).map_err(ec_metadata_internal)?;
                self.metadata
                    .cache_object_layout(object, &ObjectDataLayout::ErasureCoded(reference))
                    .await
                    .map_err(ec_metadata_internal)?;
                Ok(EcObjectReadPlan::EcMetadata)
            }
            EcManifestReadResult::Tombstone(manifest) => {
                self.metadata.put_manifest(&manifest).await.map_err(ec_metadata_internal)?;
                self.metadata.delete_object_layout(object).await.map_err(ec_metadata_internal)?;
                Ok(EcObjectReadPlan::Deleted)
            }
            // No manifest is the answer for two very different objects: one the
            // pool deliberately placed as replicated-small, and one the hot
            // path is simply still holding. The placement record tells them
            // apart, and a node that has never resolved this object has to ask
            // the pool for it — once. Caching what comes back is what makes
            // every node, and not only the one that took the write, able to say
            // the object was placed deliberately.
            EcManifestReadResult::Absent => {
                // An object this node has already looked for and not found a
                // record for does not get looked for again until the memory
                // expires: on a pool where most small objects are ordinary hot
                // ones, that is the difference between one resolve and one per
                // read (decision EC2-201).
                if self.small_placement_miss_is_remembered(object) {
                    self.metadata
                        .delete_object_layout(object)
                        .await
                        .map_err(ec_metadata_internal)?;
                    return Ok(EcObjectReadPlan::HotReplicated);
                }
                if !self.object_is_small_enough_for_replicated_small(object, ports).await? {
                    self.metadata
                        .delete_object_layout(object)
                        .await
                        .map_err(ec_metadata_internal)?;
                    return Ok(EcObjectReadPlan::HotReplicated);
                }
                match read_replicated_small_placement(ports, object)
                    .await
                    .map_err(map_ec_read_error)?
                {
                    Some(placement) => {
                        self.forget_small_placement_miss(object);
                        self.metadata
                            .cache_object_layout(
                                object,
                                &ObjectDataLayout::ReplicatedSmall(placement),
                            )
                            .await
                            .map_err(ec_metadata_internal)?;
                        Ok(EcObjectReadPlan::EcMetadata)
                    }
                    None => {
                        self.remember_small_placement_miss(object);
                        self.metadata
                            .delete_object_layout(object)
                            .await
                            .map_err(ec_metadata_internal)?;
                        Ok(EcObjectReadPlan::HotReplicated)
                    }
                }
            }
        }
    }

    /// Ask the pool what became of an object whose local record promised bytes
    /// that turned out not to be there.
    ///
    /// A replicated-small record says where the object was put, never whether
    /// it was later deleted: the delete writes a tombstone at the manifest key,
    /// and a node holding only the placement has no reason to look for one
    /// until a read comes back empty. That miss is the moment to look. It costs
    /// nothing on the path that finds the object, and it is what lets every
    /// node — not only the one that took the delete — answer "gone" and stop
    /// carrying a record for an object that no longer exists.
    ///
    /// Any other failure is returned as it came: an unreachable replica is not
    /// evidence of anything, and re-reading the manifest would only add a
    /// second timeout to the first.
    ///
    /// One quorum manifest read and nothing else. Re-running the whole plan
    /// here would also pay a quorum `HEAD` and a placement resolve to answer a
    /// question neither of them bears on: only a tombstone turns "not found"
    /// into "deleted" (decision EC2-201).
    async fn explain_missing_object<M>(
        &self,
        object: &EcObjectVersionKey,
        metadata_coordinator: &M,
        error: EcReadError,
    ) -> EcObjectReadError
    where
        M: EcReadMetadataCoordinator,
    {
        let mapped = map_ec_read_error(error);
        if !matches!(mapped, EcObjectReadError::NotFound { .. }) {
            return mapped;
        }
        let read = metadata_coordinator.quorum_read_manifest(object).await;
        let Ok(EcManifestReadResult::Tombstone(tombstone)) = read else {
            return mapped;
        };
        // Seen once, remembered: the stale local record goes, and the next read
        // of this object answers "gone" without asking the pool again.
        if self.metadata.put_manifest(&tombstone).await.is_ok() {
            let _ = self.metadata.delete_object_layout(object).await;
            self.forget_small_placement_miss(object);
        }
        EcObjectReadError::Deleted {
            bucket: object.bucket.clone(),
            key: object.key.clone(),
            version_id: object.version_id.clone(),
        }
    }

    /// Whether this node looked for this object's placement record recently
    /// and found none.
    fn small_placement_miss_is_remembered(&self, object: &EcObjectVersionKey) -> bool {
        let ttl = self.config.small_placement_negative_ttl;
        if ttl.is_zero() {
            return false;
        }
        let Ok(misses) = self.small_placement_misses.lock() else {
            return false;
        };
        misses.get(object).is_some_and(|seen| seen.elapsed() < ttl)
    }

    /// Remember that this object has no placement record.
    fn remember_small_placement_miss(&self, object: &EcObjectVersionKey) {
        let ttl = self.config.small_placement_negative_ttl;
        if ttl.is_zero() {
            return;
        }
        let Ok(mut misses) = self.small_placement_misses.lock() else {
            return;
        };
        if misses.len() >= EC_SMALL_PLACEMENT_MISS_CAPACITY {
            misses.retain(|_, seen| seen.elapsed() < ttl);
            if misses.len() >= EC_SMALL_PLACEMENT_MISS_CAPACITY {
                // Nothing here is authoritative, and the bound matters more
                // than any particular entry: dropping the lot costs one
                // resolve per object that comes back.
                misses.clear();
            }
        }
        misses.insert(object.clone(), Instant::now());
    }

    /// Forget a remembered miss, because this object now has a record.
    fn forget_small_placement_miss(&self, object: &EcObjectVersionKey) {
        if self.config.small_placement_negative_ttl.is_zero() {
            return;
        }
        if let Ok(mut misses) = self.small_placement_misses.lock() {
            misses.remove(object);
        }
    }

    /// Whether it is worth asking the pool for a replicated-small placement.
    ///
    /// One quorum HEAD before a fan-out read, and it settles both halves of the
    /// question: an object that is not there at all, and one too large to have
    /// been placed as replicated-small, cannot have a placement record. Without
    /// the guard every read of an object the hot path is still holding — and
    /// every 404 — would cost a fan-out that can only ever miss.
    async fn object_is_small_enough_for_replicated_small(
        &self,
        object: &EcObjectVersionKey,
        ports: &EcClusterReadPorts,
    ) -> Result<bool, EcObjectReadError> {
        if object.version_id.is_some() {
            return Ok(false);
        }
        match ports.read_coordinator.head(&object.bucket, &object.key).await {
            Ok(head) => Ok(head.size < self.default_set.profile.min_object_size),
            Err(ClusterError::ObjectNotFound { .. }) => Ok(false),
            // Unreachable replicas are not an answer either way, and this is
            // only deciding whether to ask a further question. The read that
            // follows reports the real failure.
            Err(_) => Ok(false),
        }
    }

    async fn hot_replicated_is_current_or_newer(
        &self,
        object: &EcObjectVersionKey,
        manifest: &EcObjectManifest,
        ports: &EcClusterReadPorts,
    ) -> Result<bool, EcObjectReadError> {
        if object.version_id.is_some() {
            return Ok(false);
        }

        match ports.read_coordinator.head(&object.bucket, &object.key).await {
            // A record marked erasure coded is this same object's namespace
            // entry, left behind by cutover so the object stays listable. It
            // carries the source HLC, so comparing timestamps alone would read
            // it as a hot copy that is just as fresh — and send the reader to
            // replicas that hold no bytes at all.
            Ok(head) if head.erasure_coded => Ok(false),
            Ok(head) => Ok(head.hlc >= manifest.hlc),
            Err(ClusterError::ObjectNotFound { .. }) => Ok(false),
            Err(error) => Err(EcObjectReadError::Unavailable {
                reason: format!(
                    "hot replicated HEAD failed while checking EC manifest freshness: {error}"
                ),
            }),
        }
    }

    async fn read_hot_replicated(
        &self,
        deps: &EcObjectReadRuntimeDeps,
        request: EcObjectReadRequest,
    ) -> Result<EcObjectReadResult, EcObjectReadError> {
        if request.version_id.is_some() {
            return Err(EcObjectReadError::Unavailable {
                reason: "version-specific hot replicated EC reads require a version-aware cluster reader"
                    .to_string(),
            });
        }

        let range = request.range.map(|range| (range.start, range.end));
        let stream = deps
            .read_coordinator
            .read_stream(&request.bucket, &request.key, range)
            .await
            .map_err(|error| EcObjectReadError::from_cluster(error, request.bucket, request.key))?;
        let mut body = Vec::with_capacity(stream.content_length.min(usize::MAX as u64) as usize);
        let mut reader = stream.body;
        reader
            .read_to_end(&mut body)
            .await
            .map_err(|error| EcObjectReadError::Unavailable {
                reason: format!("hot replicated read stream failed: {error}"),
            })?;

        Ok(EcObjectReadResult {
            body: Bytes::from(body),
            head: hot_head_result(
                stream.etag,
                stream.content_type,
                stream.total_size,
                stream.metadata,
                stream.hlc,
                stream.version_id,
            ),
            content_length: stream.content_length,
            content_range: stream.content_range,
            repair_enqueued: false,
        })
    }

    async fn head_hot_replicated(
        &self,
        deps: &EcObjectReadRuntimeDeps,
        request: EcObjectHeadRequest,
    ) -> Result<EcObjectHeadResult, EcObjectReadError> {
        if request.version_id.is_some() {
            return Err(EcObjectReadError::Unavailable {
                reason: "version-specific hot replicated EC HEAD requires a version-aware cluster reader"
                    .to_string(),
            });
        }
        let head = deps
            .read_coordinator
            .head(&request.bucket, &request.key)
            .await
            .map_err(|error| EcObjectReadError::from_cluster(error, request.bucket, request.key))?;
        Ok(hot_head_from_quorum(head))
    }

    /// Reduce this node's manifests to content digests, bounded by the limit.
    ///
    /// One pass over the manifest keyspace. The walk is capped by the node's own
    /// `S4_EC_ADMIN_SCAN_LIMIT` as well as by what the caller asked for, so a
    /// measurement started elsewhere cannot make this node walk its whole store.
    async fn local_content_digest_page(
        &self,
        max_entries: usize,
    ) -> Result<EcContentDigestPage, ClusterError> {
        let capped = max_entries.clamp(1, self.config.admin_scan_limit);
        let page = self
            .metadata
            .list_manifests_page(None, capped)
            .await
            .map_err(cluster_storage_error)?;

        let scanned = page.entries.len();
        let digests = page
            .entries
            .into_iter()
            .filter(manifest_holds_ec_bytes)
            .map(|manifest| EcContentDigest {
                bucket: manifest.bucket,
                key: manifest.key,
                version_id: manifest.version_id,
                content_hash_sha256: manifest.content_hash_sha256,
                original_size: manifest.original_size,
                erasure_set_id: manifest.erasure_set_id,
                packed: manifest.packed.is_some(),
            })
            .collect();

        Ok(EcContentDigestPage {
            digests,
            scanned,
            // Some only when at least one manifest was left unread, so this
            // distinguishes a walk that reached the end from one that stopped.
            truncated: page.next_cursor.is_some(),
        })
    }

    /// Collect content digests from every node of the pool and merge them.
    ///
    /// A node's manifest store holds what it transcoded plus what its read path
    /// resolved, so a repeat whose two objects were transcoded on different
    /// nodes is invisible on both of them. Asking one node and reporting the
    /// answer as the pool's would under-count duplicates by roughly the width of
    /// the pool — and under-counting here produces a "no duplicates" reading
    /// that looks like a safe answer while being no answer at all.
    ///
    /// A peer that cannot be reached, or that is too old to know the call, is
    /// recorded by name with its reason. It is never counted as a node with
    /// nothing to report.
    async fn collect_pool_content_digests(
        &self,
        request: &EcAdminDedupReportRequest,
    ) -> Result<(Vec<EcContentDigest>, EcDedupScanFacts), ClusterError> {
        let capped = request.max_entries.clamp(1, self.config.admin_scan_limit);
        let pool_nodes = pool_nodes(&self.placement.sets());
        let local = self.local_content_digest_page(capped).await?;

        let mut facts = EcDedupScanFacts {
            nodes_total: pool_nodes.len().max(1),
            nodes_answered: 1,
            nodes_unread: Vec::new(),
            scan_limit: capped,
            nodes_truncated: usize::from(local.truncated),
            manifests_scanned: local.scanned,
        };
        let mut digests = local.digests;

        if request.local_only {
            // Asked for on purpose, so the shortfall is named rather than
            // implied: every other node of the pool stays unread, and the report
            // will refuse a verdict because of it.
            for node_id in pool_nodes.iter().filter(|node| **node != self.local_node_id) {
                facts.nodes_unread.push(EcDedupUnreadNode {
                    node_id: node_id.to_string(),
                    reason: "not asked: the request was for this node only".to_string(),
                });
            }
            return Ok((digests, facts));
        }

        let Some(runtime) = self.read_runtime.get() else {
            for node_id in pool_nodes.iter().filter(|node| **node != self.local_node_id) {
                facts.nodes_unread.push(EcDedupUnreadNode {
                    node_id: node_id.to_string(),
                    reason: "this node has no cluster read path configured, so it cannot ask peers"
                        .to_string(),
                });
            }
            return Ok((digests, facts));
        };

        let mut tasks = JoinSet::new();
        for node_id in pool_nodes.iter().copied().filter(|node| *node != self.local_node_id) {
            let rpc_client = Arc::clone(&runtime.rpc_client);
            let coordinator = self.local_node_id;
            tasks.spawn(async move {
                let page =
                    rpc_client.send_scan_ec_content_digests(node_id, coordinator, capped).await;
                (node_id, page)
            });
        }

        while let Some(joined) = tasks.join_next().await {
            match joined {
                Ok((_node_id, Ok(page))) => {
                    facts.nodes_answered += 1;
                    facts.manifests_scanned += page.scanned;
                    facts.nodes_truncated += usize::from(page.truncated);
                    digests.extend(page.digests);
                }
                Ok((node_id, Err(error))) => {
                    tracing::warn!(
                        node_id = %node_id,
                        error = %error,
                        "EC dedup measurement could not read a node's manifests"
                    );
                    facts.nodes_unread.push(EcDedupUnreadNode {
                        node_id: node_id.to_string(),
                        reason: error.to_string(),
                    });
                }
                Err(error) => {
                    // The peer is not identifiable here, so the count still has
                    // to move: a task lost is a node unread.
                    facts.nodes_unread.push(EcDedupUnreadNode {
                        node_id: "unknown".to_string(),
                        reason: format!("digest scan task failed: {error}"),
                    });
                }
            }
        }

        Ok((digests, facts))
    }

    async fn pool_health_snapshot(&self) -> Result<Value, ClusterError> {
        let manifests = self
            .metadata
            .list_manifests(self.config.admin_scan_limit)
            .await
            .map_err(cluster_storage_error)?;
        let local_shards = self
            .metadata
            .list_local_shards(self.config.admin_scan_limit)
            .await
            .map_err(cluster_storage_error)?;
        let transcode_jobs = self
            .metadata
            .list_transcode_jobs(self.config.admin_scan_limit)
            .await
            .map_err(cluster_storage_error)?;
        let repair_tasks = self
            .repair
            .list_repair_tasks(self.config.admin_scan_limit)
            .await
            .map_err(cluster_config_error)?;
        let orphans = self
            .metadata
            .list_orphan_shards(self.config.admin_scan_limit)
            .await
            .map_err(cluster_storage_error)?;
        let hot_replica_gc = self
            .metadata
            .list_hot_replica_gc(self.config.admin_scan_limit)
            .await
            .map_err(cluster_storage_error)?;

        let transcode_counts = transcode_counts(&transcode_jobs);
        let repair_counts = repair_counts(&repair_tasks);
        let now = unix_time_nanos();
        let hot_gc_eligible = hot_replica_gc
            .iter()
            .filter(|record| hot_replica_is_eligible(record, now))
            .count();
        // Saturating at the admission ceiling on purpose. The tiering worker is
        // the other writer of this gauge, and it stops counting once it knows
        // there is no headroom (`outstanding_migrations` returns early at the
        // ceiling), so an unclamped count here would make the same gauge mean
        // two different things depending on which writer ran last. Clamped, the
        // reading is well defined at both ends: below the ceiling it is the
        // exact depth, at the ceiling it means "no headroom", which is the one
        // fact an operator acts on.
        let tiering_queue_depth = transcode_jobs
            .iter()
            .filter(|job| job.origin.is_tiering() && transcode_job_is_in_flight(job.state))
            .count()
            .min(self.config.tiering.max_queue_depth);
        let known_sets = self.known_set_ids();
        ec_metrics::set_transcode_queue_depth(
            &known_sets,
            &transcode_queue_depth_by_set(&transcode_jobs),
        );
        ec_metrics::set_tiering_queue_depth(tiering_queue_depth);
        ec_metrics::set_repair_queue_depth(&known_sets, &repair_queue_depth_by_set(&repair_tasks));
        ec_metrics::set_repair_queue_depth_by_losses(
            &known_sets,
            &repair_queue_depth_by_losses(&repair_tasks),
        );
        ec_metrics::set_orphan_shards(&known_sets, &orphan_count_by_set(&orphans));
        // Published here as well as from the anti-entropy pass, so a node whose
        // integrity worker is switched off still answers with a current figure
        // when it is scraped or its health is read.
        let layout_drift = self.check_layout_drift();

        let transcode_worker_started = self.transcode_worker_started.load(Ordering::Acquire);
        let repair_worker_started = self.repair_worker_effective_started();
        let anti_entropy_worker_started = self.anti_entropy_worker_effective_started();
        let read_runtime_configured = self.read_runtime.get().is_some();
        let release_gates = release_gate_status(
            transcode_worker_started,
            read_runtime_configured,
            repair_worker_started,
            anti_entropy_worker_started,
        );
        // Every set of the pool, not only the default one: a pool whose second
        // set is at risk is not healthy, and reporting it as such would hide
        // exactly the case multi-set pools introduce.
        let sets = self.placement.sets();
        let inherited = self.live_inherited_set();
        let slot_owners = self.slot_owners(&sets);
        let rejoin = self.rejoin_status();
        // What this node holds of what the pool gives it. Zero against a
        // non-empty slot list is a node that is back in membership and answers
        // for nothing — the state `slot_owners` alone cannot show, because from
        // there the node is simply alive again (`EC2-NREP-02-004`).
        let local_shards_held = local_shards.len();
        let any_topology_risk = sets.sets().into_iter().any(|set| !set.topology_risks.is_empty());
        let health = pool_health_verdict(
            any_topology_risk,
            repair_counts.failed_permanent,
            transcode_counts.failed_permanent,
            rejoin.serves_ec_reads(),
        );

        Ok(json!({
            "pool": self.pool_name,
            "health": health,
            "runtime": ec_runtime_status(
                transcode_worker_started,
                read_runtime_configured,
                repair_worker_started,
                anti_entropy_worker_started,
            ),
            // Kept exactly as it was so a pool with one set answers a caller
            // written before multi-set pools existed identically. The per-set
            // view is added alongside rather than replacing it.
            "erasure_set": self.set_admin_snapshot(&inherited),
            "erasure_sets": self.erasure_sets_snapshot(),
            "config": self.config_snapshot(),
            "counts": {
                "manifests_scanned": manifests.len(),
                "local_shards_scanned": local_shards.len(),
                "transcode_jobs_scanned": transcode_jobs.len(),
                "transcode_queue_depth": transcode_counts.queue_depth,
                "repair_tasks_scanned": repair_tasks.len(),
                "orphans_scanned": orphans.len(),
                "hot_replicas_gc_records_scanned": hot_replica_gc.len(),
                "hot_replicas_gc_eligible": hot_gc_eligible,
                // Same value as the `s4_ec_tiering_queue_depth` gauge, so a
                // pool can be diagnosed without a Prometheus scrape.
                "tiering_queue_depth": tiering_queue_depth,
                "tiering_queue_ceiling": self.config.tiering.max_queue_depth
            },
            "transcode": transcode_counts,
            "repair": repair_counts,
            "topology": {
                "health": inherited.topology_health,
                "risks": inherited.topology_risks,
                // Whether the pool's sets still keep the guarantees of the
                // rules they were laid out by, as this node's labels see it.
                // Added beside the two fields that were always here, never in
                // place of one (`EC2-ZONE-04-001`).
                "layout_drift": {
                    "sets_checked": layout_drift.sets_checked,
                    "sets_meeting_rule": layout_drift.sets_meeting_rule,
                    // Every EC v1 set: its rule promises no placement, so
                    // switching the LRC group mode on cannot make it a
                    // violator (EC2-084, point 1).
                    "sets_without_guarantee": layout_drift.sets_without_guarantee,
                    "sets_drifted": layout_drift.sets_drifted,
                    "sets_violated_at_creation": layout_drift.sets_violated_at_creation,
                    "sets_not_judgeable": layout_drift.sets_not_judgeable,
                    "violators": layout_drift.violators()
                }
            },
            // Owners of slots the pool cannot see, reported beside topology and
            // never inside it. "This node is dead" and "this node sits in the
            // wrong failure domain" are both risks to durability and nothing
            // else about them is alike: one is answered by replacing a machine,
            // the other by moving a set. A single list would make an operator
            // read both and act on neither (`EC2-NREP-01-003`).
            //
            // It does not move `health` either. That verdict is defined by
            // topology risk and permanently failed work, and a node that is
            // down is neither; whether a pool serving reads from an incomplete
            // set may still call itself healthy is decided in phase 2, not
            // here.
            "slot_owners": {
                // Without a membership view this node has no opinion about
                // anyone, and `not_seen_alive` is empty because nothing was
                // read — not because everyone is up.
                "membership_available": self.membership.get().is_some(),
                "total": slot_owners.len(),
                "seen_alive": slot_owners
                    .iter()
                    .filter(|owner| owner.membership.is_seen_alive())
                    .count(),
                "not_seen_alive": slot_owners
                    .iter()
                    .filter(|owner| owner.membership.is_seen_not_alive())
                    .map(SlotOwner::admin_snapshot)
                    .collect::<Vec<_>>()
            },
            // What this node was allowed to do on its way back into the pool.
            //
            // Only about the node that answered: a verdict is reached from that
            // node's own storage, and no other node can reach it for it. Every
            // node answers this route for itself, which is also how a scrape of
            // `s4_ec_node_rejoin_serving` is read.
            "rejoin": rejoin_admin_snapshot(
                self.local_node_id,
                &rejoin,
                &slot_owners,
                local_shards_held,
                local_shards_held >= self.config.admin_scan_limit,
            ),
            "release_gates": release_gates,
            "scan_limit": self.config.admin_scan_limit
        }))
    }

    fn config_snapshot(&self) -> Value {
        json!({
            // Machine-readable so an acceptance test can tell a genuinely mixed
            // cluster from an accidentally uniform one. A log line cannot.
            "codec_engine": crate::ec_codec::codec_engine().as_str(),
            "transcode_enabled": self.config.transcode_enabled,
            "inline_write_enabled": self.config.transcode.inline_write_enabled,
            "tiering": self.config.tiering,
            "tiering_heat": {
                "enabled": self.heat.is_enabled(),
                "counters": self.heat.counters()
            },
            "tiering_worker_wired": self.tiering_worker_started.load(Ordering::Acquire),
            "tiering_cluster_supported": self.is_cluster_tiering_supported(),
            // Reported apart from the forward worker because it starts apart
            // from it: reverse tiering also carries the operator's rollback, so
            // it runs even where auto-tiering is switched off.
            "promotion_worker_wired": self.promotion_worker_started.load(Ordering::Acquire),
            "promoted_shard_grace_secs": self.config.gc.promoted_grace_secs,
            // Whether a held-back node actually stops serving, or only says it
            // should. The one switch that makes plan 08 reversible.
            "rejoin_gate_enforced": self.config.rejoin_gate_enforced,
            // Whether a serving read pulls the `k` data shards of a chunk or
            // all `n`. Reported per node because the switch is per node: a
            // pool may run with it set either way on any of its nodes.
            "minimal_read_enabled": self.config.minimal_read_enabled,
            "max_rejoin_downtime_days": self.config.repair.max_rejoin_downtime_days,
            "long_offline_rejoin_policy": match self.config.repair.long_offline_rejoin_policy {
                crate::ec_repair::EcLongOfflineRejoinPolicy::NeedsBootstrap => "needs_bootstrap",
                crate::ec_repair::EcLongOfflineRejoinPolicy::RequireAdminApproval => {
                    "require_admin_approval"
                }
            },
            "s3_object_path_wired": true,
            "http_data_path_wired": true,
            "ec_read_runtime_wired": self.read_runtime.get().is_some(),
            "transcode_worker_wired": self.transcode_worker_started.load(Ordering::Acquire),
            "repair_worker_wired": self.repair_worker_effective_started(),
            "anti_entropy_worker_wired": self.anti_entropy_worker_effective_started(),
            "ec_delete_tombstone_wired": self.read_runtime.get().is_some(),
            "hot_replica_purger_wired": self.transcode_worker_started.load(Ordering::Acquire),
            "orphan_gc_physical_purger_wired": self.read_runtime.get().is_some(),
            "ec_v1_ci_acceptance_attested": ec_v1_ci_acceptance_attested(),
            "ec_v1_ci_acceptance_env": EC_V1_CI_ACCEPTANCE_ENV,
            "max_concurrent_transcodes": self.config.transcode.max_concurrent_transcodes,
            "max_concurrent_inline_writes": self.config.transcode.max_concurrent_inline_writes,
            "transcode_worker_poll_ms": self.config.transcode_worker.poll_interval.as_millis(),
            "transcode_worker_batch_size": self.config.transcode_worker.batch_size,
            "transcode_max_source_bytes": self.config.transcode_worker.max_source_read_bytes,
            "repair_worker_poll_ms": self.config.integrity_worker.repair_poll_interval.as_millis(),
            "repair_worker_batch_size": self.config.integrity_worker.repair_batch_size,
            "scrub_worker_enabled": self.config.integrity_worker.scrub_enabled,
            "scrub_worker_interval_secs": self.config.integrity_worker.scrub_interval.as_secs(),
            "anti_entropy_worker_enabled": self.config.integrity_worker.anti_entropy_enabled,
            "anti_entropy_worker_interval_secs": self.config.integrity_worker.anti_entropy_interval.as_secs(),
            "cpu_workers": self.config.cpu.workers.get(),
            "cpu_budget_percent": self.config.cpu.cpu_budget_percent,
            "max_inflight_encode_bytes": self.config.cpu.max_inflight_encode_bytes,
            "repair_bandwidth_bytes_per_sec": self.config.repair.global_bandwidth_bytes_per_sec,
            "repair_running_timeout_secs": self.config.repair.running_timeout_secs,
            "orphan_min_age_secs": self.config.gc.orphan_min_age_secs,
            "orphan_lease_grace_secs": self.config.gc.orphan_lease_grace_secs,
        })
    }

    /// Walk the pool's manifests and queue what `request` asks for.
    ///
    /// Two limits act here and they are not the same one. `max_tasks` caps what
    /// goes into the repair queue; `S4_EC_ADMIN_SCAN_LIMIT` caps how far the
    /// walk reads. Past the first the walk keeps going and keeps counting, and
    /// that count is the whole point: without it a request that filled its own
    /// quota is indistinguishable from one that finished the pool, and a
    /// whole-node rebuild looks done after its first page (`EC2-NREP-03-001`).
    async fn queue_repairs_for_pool(
        &self,
        request: &EcAdminRepairRequest,
        target_node: NodeId,
        queued: &mut Vec<EcRepairTaskRecord>,
        outcome: &mut EcNodeRepairQueueOutcome,
    ) -> Result<(), ClusterError> {
        if let Some(object) = &request.object {
            let key = object_key_from_admin_ref(object)?;
            let manifest = self.get_manifest(&key).await?;
            outcome.note_manifest();
            return self
                .queue_repairs_for_manifest(&manifest, request, target_node, queued, outcome)
                .await;
        }

        let scan_limit = self.config.admin_scan_limit;
        let mut cursor: Option<Vec<u8>> = None;
        loop {
            let budget = scan_limit.saturating_sub(outcome.manifests_scanned());
            if budget == 0 {
                outcome.mark_truncated();
                outcome.stop_at(EcRebuildQueueStop::ScanLimit);
                break;
            }
            let page = self
                .metadata
                .list_manifests_page(cursor.take(), budget.min(EC_ADMIN_MANIFEST_PAGE))
                .await
                .map_err(cluster_storage_error)?;
            let next_cursor = page.next_cursor;
            for manifest in &page.entries {
                outcome.note_manifest();
                self.queue_repairs_for_manifest(manifest, request, target_node, queued, outcome)
                    .await?;
            }
            match next_cursor {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }
        Ok(())
    }

    async fn queue_repairs_for_manifest(
        &self,
        manifest: &EcObjectManifest,
        request: &EcAdminRepairRequest,
        target_node: NodeId,
        queued: &mut Vec<EcRepairTaskRecord>,
        outcome: &mut EcNodeRepairQueueOutcome,
    ) -> Result<(), ClusterError> {
        for chunk in &manifest.chunks {
            if request.chunk_index.is_some_and(|filter| filter != chunk.chunk_index) {
                continue;
            }
            for shard in &chunk.shards {
                if request.slot_index.is_some_and(|filter| filter != shard.slot_index) {
                    continue;
                }
                if shard.node_id != target_node {
                    continue;
                }

                let should_queue = if target_node == self.local_node_id {
                    self.local_shard_needs_repair(manifest, chunk, shard).await?
                } else {
                    true
                };
                if !should_queue && !is_explicit_single_shard_repair_request(request) {
                    continue;
                }

                // Beyond the task limit, or with the durable queue already
                // full, this shard is work for the next request rather than
                // work that vanishes.
                if queued.len() >= request.max_tasks
                    || outcome.stopped_at() == EcRebuildQueueStop::QueueFull
                {
                    outcome.stop_at(EcRebuildQueueStop::MaxTasks);
                    outcome.note_remaining();
                    continue;
                }

                let result = self
                    .repair
                    .queue_admin_repair(EcRepairQueueRequest {
                        object: manifest.object_key(),
                        operation_id: manifest.operation_id,
                        erasure_set_id: manifest.erasure_set_id,
                        chunk_index: chunk.chunk_index,
                        slot_index: shard.slot_index,
                        node_id: shard.node_id,
                        kind: EcRepairKind::ManualAdmin,
                        reason: if target_node == self.local_node_id {
                            if should_queue {
                                "admin requested repair for missing or mismatched local shard"
                                    .to_string()
                            } else {
                                "admin requested explicit repair for a healthy local shard"
                                    .to_string()
                            }
                        } else {
                            "admin requested repair for explicitly targeted shard".to_string()
                        },
                        failure_domain: None,
                    })
                    .await;
                match result {
                    Ok(task) => {
                        outcome.note_queued(task.created);
                        queued.push(task.task);
                    }
                    // A full queue is backpressure, not a failed request: the
                    // tasks already queued are real work, and refusing the
                    // whole call would throw them away along with the count of
                    // what is left.
                    Err(EcRepairError::QueueFull { limit }) => {
                        tracing::warn!(
                            limit,
                            target_node = %target_node,
                            "EC admin repair stopped queueing: the durable repair queue is full"
                        );
                        outcome.stop_at(EcRebuildQueueStop::QueueFull);
                        outcome.note_remaining();
                    }
                    Err(error) => return Err(cluster_config_error(error)),
                }
            }
        }
        Ok(())
    }

    async fn local_shard_needs_repair(
        &self,
        manifest: &EcObjectManifest,
        chunk: &ChunkManifest,
        shard: &ShardManifest,
    ) -> Result<bool, ClusterError> {
        let local_key = LocalShardKey {
            object: manifest.object_key(),
            chunk_index: chunk.chunk_index,
            slot_index: shard.slot_index,
        };
        let Some(local) =
            self.metadata.get_local_shard(&local_key).await.map_err(cluster_storage_error)?
        else {
            return Ok(true);
        };
        match local.verify_against_manifest(shard) {
            Ok(()) => {}
            Err(_) => {
                ec_metrics::record_shard_hash_mismatch(manifest.erasure_set_id);
                return Ok(true);
            }
        }

        let Some(deps) = self.read_runtime.get() else {
            return Ok(false);
        };
        let hidden_key = shard_object_key(
            &manifest.object_key(),
            manifest.operation_id,
            chunk.chunk_index,
            shard.slot_index,
        );
        // Metadata can still match the manifest when disk bytes are corrupted.
        // Admin repair must verify the physical shard before skipping the task.
        let (data, _) = match deps.storage.get_object(SYSTEM_BUCKET, &hidden_key).await {
            Ok(found) => found,
            Err(s4_core::StorageError::ObjectNotFound { .. }) => return Ok(true),
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    bucket = %manifest.bucket,
                    key = %manifest.key,
                    chunk_index = chunk.chunk_index,
                    slot_index = shard.slot_index,
                    "EC admin repair found unreadable local shard bytes"
                );
                return Ok(true);
            }
        };
        if let Err(error) = shard.verify_bytes(&data) {
            ec_metrics::record_shard_hash_mismatch(manifest.erasure_set_id);
            tracing::warn!(
                error = %error,
                bucket = %manifest.bucket,
                key = %manifest.key,
                chunk_index = chunk.chunk_index,
                slot_index = shard.slot_index,
                "EC admin repair found local shard bytes that do not match manifest"
            );
            return Ok(true);
        }

        Ok(false)
    }

    /// What the pool still owes `node_id`, counted from manifests and shards.
    ///
    /// The independent answer to "has the rebuild of this node finished". It
    /// deliberately never looks at the repair queue: an empty queue is what a
    /// finished rebuild looks like and equally what a rebuild nobody queued
    /// looks like, and telling an operator the first when it is the second is
    /// the one wrong answer that matters here (`EC2-NREP-03-002`).
    ///
    /// Bounded by `S4_EC_ADMIN_SCAN_LIMIT` like every other pool-wide admin
    /// walk. A walk that reaches that limit says so, and the inventory then
    /// refuses a verdict rather than reporting what it managed to read as the
    /// pool.
    async fn node_shard_inventory(
        &self,
        node_id: NodeId,
    ) -> Result<EcNodeRebuildInventory, ClusterError> {
        let slots = self
            .slot_owners(&self.placement.sets())
            .into_iter()
            .find(|owner| owner.node_id == node_id)
            .map(|owner| owner.slots)
            .unwrap_or_default();
        let mut inventory = EcNodeRebuildInventory::new(node_id).with_slots(slots);

        // Built once for the whole walk: a handle per shard would rebuild the
        // pool's roster thousands of times to ask thousands of one-line
        // questions.
        let ports = self.read_runtime.get().map(|deps| self.read_ports(deps));
        let scan_limit = self.config.admin_scan_limit;
        let mut cursor: Option<Vec<u8>> = None;
        loop {
            let budget = scan_limit.saturating_sub(inventory.manifests_scanned());
            if budget == 0 {
                inventory.mark_truncated();
                break;
            }
            let page = self
                .metadata
                .list_manifests_page(cursor.take(), budget.min(EC_ADMIN_MANIFEST_PAGE))
                .await
                .map_err(cluster_storage_error)?;
            let next_cursor = page.next_cursor;

            let mut probes = Vec::new();
            for manifest in &page.entries {
                inventory.note_manifest();
                for (chunk_index, slot_index) in shards_owned_by(manifest, node_id) {
                    probes.push(EcShardProbe {
                        object: manifest.object_key(),
                        operation_id: manifest.operation_id,
                        chunk_index,
                        slot_index,
                    });
                }
            }
            for presence in self.probe_shard_presence(node_id, ports.as_ref(), probes).await {
                inventory.note_shard(presence);
            }

            match next_cursor {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }
        Ok(inventory)
    }

    /// Ask whether `node_id` holds each of `probes`, a bounded batch at a time.
    ///
    /// The cap is what keeps an inventory of a large pool from opening one
    /// request per shard at once against a node that is, by the nature of this
    /// question, quite likely already struggling.
    async fn probe_shard_presence(
        &self,
        node_id: NodeId,
        ports: Option<&EcClusterReadPorts>,
        probes: Vec<EcShardProbe>,
    ) -> Vec<EcShardPresence> {
        let mut found = Vec::with_capacity(probes.len());
        for batch in probes.chunks(EC_SHARD_PROBES_IN_FLIGHT) {
            let mut tasks = JoinSet::new();
            for probe in batch {
                let probe = probe.clone();
                let metadata = Arc::clone(&self.metadata);
                let ports = ports.cloned();
                let local_node_id = self.local_node_id;
                tasks.spawn(async move {
                    shard_presence(node_id, local_node_id, &metadata, ports.as_ref(), &probe).await
                });
            }
            while let Some(joined) = tasks.join_next().await {
                found.push(joined.unwrap_or_else(|error| {
                    // A task that did not finish did not find the shard absent.
                    EcShardPresence::unknown(format!("shard presence check failed: {error}"))
                }));
            }
        }
        found
    }

    async fn rehydrate_local_shard_record(
        &self,
        object: &EcObjectVersionKey,
        chunk_index: u32,
        expected: &ShardManifest,
    ) -> Result<Option<LocalShardRecord>, ClusterError> {
        if expected.node_id != self.local_node_id {
            return Ok(None);
        }
        let Some(deps) = self.read_runtime.get() else {
            return Ok(None);
        };
        let hidden_key = shard_object_key(
            object,
            self.get_manifest(object).await?.operation_id,
            chunk_index,
            expected.slot_index,
        );
        let (data, index) = match deps.storage.get_object(SYSTEM_BUCKET, &hidden_key).await {
            Ok(found) => found,
            Err(s4_core::StorageError::ObjectNotFound { .. }) => return Ok(None),
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    bucket = %object.bucket,
                    key = %object.key,
                    chunk_index,
                    slot_index = expected.slot_index,
                    "EC admin verify could not rehydrate local shard metadata"
                );
                return Ok(None);
            }
        };
        if let Err(error) = expected.verify_bytes(&data) {
            tracing::warn!(
                error = %error,
                bucket = %object.bucket,
                key = %object.key,
                chunk_index,
                slot_index = expected.slot_index,
                "EC admin verify found physical shard bytes that do not match manifest"
            );
            return Ok(None);
        }
        Ok(Some(local_shard_record_from_index(
            &index,
            expected,
            self.get_manifest(object).await?.operation_id,
        )))
    }

    async fn orphan_gc_dry_run(
        &self,
        max_entries: usize,
    ) -> Result<
        (
            Vec<(crate::ec_metadata::EcOrphanShardKey, EcOrphanShardRecord)>,
            usize,
        ),
        ClusterError,
    > {
        let records = self
            .metadata
            .list_orphan_shards(max_entries)
            .await
            .map_err(cluster_storage_error)?;
        let now = unix_time_nanos();
        let eligible = records
            .iter()
            .filter(|(_, record)| self.orphan_record_is_gc_eligible(record, now))
            .count();
        ec_metrics::set_orphan_shards(&self.known_set_ids(), &orphan_count_by_set(&records));
        Ok((records, eligible))
    }

    /// Release the shards of these tombstones, one erasure set at a time.
    ///
    /// One pass per set, because both safety gates are answered by that set's
    /// own owners: a tombstone of another set is retained by this pass and
    /// collected by that set's, rather than judged against nodes that never
    /// held a shard of it.
    async fn sweep_tombstones(
        &self,
        now: u64,
        tombstones: &[EcObjectManifest],
    ) -> Result<EcTombstoneSweep, ClusterError> {
        let deps = self.read_runtime.get().ok_or_else(|| {
            ClusterError::Config(
                "EC tombstone GC requires the EC runtime ports to be configured".to_string(),
            )
        })?;
        let ports = self.read_ports(deps);
        let gc = EcGcCoordinator::new(
            Arc::clone(&self.metadata),
            self.config.gc.clone(),
            self.placement.sets_handle(),
        );

        let registry = self.placement.sets();
        let mut sweep = EcTombstoneSweep::default();
        let mut claimed = vec![false; tombstones.len()];
        for set in registry.sets() {
            let mut mine = Vec::new();
            for (index, tombstone) in tombstones.iter().enumerate() {
                if set.accepts_manifest_set_id(tombstone.erasure_set_id) {
                    claimed[index] = true;
                    mine.push(tombstone.clone());
                }
            }
            if mine.is_empty() {
                continue;
            }
            let owners = set_metadata_nodes(set);
            let poll = poll_tombstone_owners(&ports, &owners, &mine).await;
            sweep.confirmed_by_all = sweep.confirmed_by_all.saturating_add(poll.confirmed_by_all);
            sweep.repaired_owners = sweep.repaired_owners.saturating_add(poll.repaired);
            let report = gc
                .run_tombstone_gc(
                    now,
                    &mine,
                    &ports,
                    &ports,
                    EcTombstoneGcSafety {
                        propagation: &poll.propagation,
                        repair_frontier: &poll.frontier,
                        key_range: &KeyRange::full(),
                        shard_owners: &owners,
                    },
                )
                .await
                .map_err(cluster_gc_error)?;
            sweep.sets_swept = sweep.sets_swept.saturating_add(1);
            add_tombstone_gc_report(&mut sweep.report, &report);
        }

        // A tombstone whose set this pool no longer has is nobody's to judge:
        // the gates are answered by that set's own owners. Counted rather than
        // silently added to the retained, so that "the sweep looked at N and
        // kept M" stays an equation and not an impression.
        sweep.unclaimed = claimed.iter().filter(|claimed| !**claimed).count();
        if sweep.unclaimed > 0 {
            tracing::debug!(
                unclaimed = sweep.unclaimed,
                "EC tombstone sweep found tombstones belonging to no erasure set of this pool"
            );
        }
        Ok(sweep)
    }

    /// Write a small object's placement to the nodes that hold its replicas.
    ///
    /// Best effort in the same sense the hot write path is: a majority of the
    /// replicas has to take it, and a replica that is down learns it from the
    /// pool the next time it resolves the object. Refusing the write outright
    /// would fail a PUT whose bytes are already durable.
    async fn publish_replicated_small_placement(
        &self,
        object: &EcObjectVersionKey,
        placement: &ReplicatedSmallObjectPlacement,
    ) -> Result<(), ClusterError> {
        let Some(deps) = self.read_runtime.get() else {
            return Ok(());
        };
        let ports = self.read_ports(deps);
        let acks = write_replicated_small_placement(&ports, object, placement).await;
        // This object has a record now, whatever the memory says.
        self.forget_small_placement_miss(object);
        self.settle_small_placement(object, placement, acks, 1).await
    }

    /// Clear or record the debt one placement write leaves behind.
    ///
    /// A majority of the replicas has to hold the record for it to survive the
    /// loss of the node that wrote it — which is the whole reason the record
    /// exists. Short of that the write is not failed, because the object's
    /// bytes are already durable and a client is owed its `200`; the shortfall
    /// becomes a hint the sweep settles later (decision EC2-201).
    async fn settle_small_placement(
        &self,
        object: &EcObjectVersionKey,
        placement: &ReplicatedSmallObjectPlacement,
        acks: usize,
        attempts: u32,
    ) -> Result<(), ClusterError> {
        if acks >= small_placement_quorum(placement.replica_nodes.len()) {
            if self
                .metadata
                .clear_small_placement_hint_for_placement(object, placement)
                .await
                .map_err(cluster_storage_error)?
            {
                ec_metrics::record_small_placement_hint(
                    ec_metrics::SmallPlacementHintOutcome::Cleared,
                );
            }
            return Ok(());
        }

        tracing::warn!(
            bucket = %object.bucket,
            key = %object.key,
            acks,
            replicas = placement.replica_nodes.len(),
            attempts,
            "EC replicated-small placement did not reach a majority of its replicas; recorded for redelivery"
        );
        let recorded = self
            .metadata
            .put_small_placement_hint(
                object,
                &EcSmallPlacementHint {
                    placement: placement.clone(),
                    recorded_at: unix_time_nanos(),
                    acks: u8::try_from(acks).unwrap_or(u8::MAX),
                    attempts,
                },
            )
            .await
            .map_err(cluster_storage_error)?;
        if recorded {
            ec_metrics::record_small_placement_hint(
                ec_metrics::SmallPlacementHintOutcome::Recorded,
            );
        }
        Ok(())
    }

    /// Deliver the placement records this node still owes replicas.
    ///
    /// Node-local, like the write that recorded the debt. A hint is settled
    /// when a majority of the object's replicas holds the record, and dropped
    /// when the object it describes is gone — a delete makes the record moot,
    /// and keeping it would be keeping a promise about an object nobody has.
    async fn deliver_small_placement_hints(
        &self,
        max_entries: usize,
    ) -> Result<EcSmallPlacementHintSweep, ClusterError> {
        let mut sweep = EcSmallPlacementHintSweep::default();
        let Some(deps) = self.read_runtime.get() else {
            return Ok(sweep);
        };
        if max_entries == 0 {
            return Ok(sweep);
        }
        let cursor = self
            .metadata
            .get_small_placement_hint_cursor()
            .await
            .map_err(cluster_storage_error)?;
        let page = self
            .metadata
            .list_small_placement_hints_page(cursor, max_entries)
            .await
            .map_err(cluster_storage_error)?;
        let ports = self.read_ports(deps);

        for (object, hint) in page.entries {
            sweep.examined = sweep.examined.saturating_add(1);
            match <EcClusterReadPorts as EcReadMetadataCoordinator>::quorum_read_manifest(
                &ports, &object,
            )
            .await
            {
                // The object was deleted, or has since become erasure coded.
                // Either way the record this hint owes describes something that
                // is no longer there.
                Ok(EcManifestReadResult::Tombstone(manifest))
                | Ok(EcManifestReadResult::Present(manifest))
                    if manifest.hlc >= hint.placement.hlc =>
                {
                    let dropped = self
                        .metadata
                        .replace_small_placement_hint_if_matches(&object, &hint, None)
                        .await
                        .map_err(cluster_storage_error)?;
                    if dropped {
                        ec_metrics::record_small_placement_hint(
                            ec_metrics::SmallPlacementHintOutcome::Dropped,
                        );
                        sweep.dropped = sweep.dropped.saturating_add(1);
                    } else {
                        sweep.deferred = sweep.deferred.saturating_add(1);
                    }
                    continue;
                }
                Ok(_) => {}
                // The pool could not answer. Nothing is decided, and the hint
                // waits for a sweep that can ask.
                Err(error) => {
                    tracing::debug!(
                        bucket = %object.bucket,
                        key = %object.key,
                        %error,
                        "EC placement hint could not be judged; leaving it for the next sweep"
                    );
                    sweep.deferred = sweep.deferred.saturating_add(1);
                    continue;
                }
            }

            let acks = write_replicated_small_placement(&ports, &object, &hint.placement).await;
            let delivered = acks >= small_placement_quorum(hint.placement.replica_nodes.len());
            let mut updated = hint.clone();
            updated.attempts = hint.attempts.saturating_add(1);
            updated.acks = u8::try_from(acks).unwrap_or(u8::MAX);
            let changed = self
                .metadata
                .replace_small_placement_hint_if_matches(
                    &object,
                    &hint,
                    (!delivered).then_some(&updated),
                )
                .await
                .map_err(cluster_storage_error)?;
            if delivered {
                if changed {
                    ec_metrics::record_small_placement_hint(
                        ec_metrics::SmallPlacementHintOutcome::Cleared,
                    );
                    sweep.delivered = sweep.delivered.saturating_add(1);
                } else {
                    sweep.deferred = sweep.deferred.saturating_add(1);
                }
            } else {
                sweep.deferred = sweep.deferred.saturating_add(1);
            }
        }
        self.metadata
            .put_small_placement_hint_cursor(page.next_cursor)
            .await
            .map_err(cluster_storage_error)?;
        Ok(sweep)
    }

    /// Every tombstone manifest this node holds, up to `max_entries`.
    ///
    /// Node-local by design: a tombstone this node does not hold names no
    /// shards it could release, so a sweep that looked further would gather
    /// work it cannot do.
    async fn scan_local_tombstones(
        &self,
        max_entries: usize,
        start: TombstoneScanStart,
    ) -> Result<EcTombstoneScan, ClusterError> {
        let budget = self.config.gc.tombstone_scan_budget;
        let previous = match &start {
            TombstoneScanStart::Cursor(_) => None,
            TombstoneScanStart::WhereTheLastCycleStopped => {
                self.metadata.get_tombstone_gc_cursor().await.map_err(cluster_storage_error)?
            }
        };
        let mut cursor = match &start {
            TombstoneScanStart::Cursor(cursor) => cursor.clone(),
            _ => previous.as_ref().and_then(|record| record.continuation.clone()),
        };
        let mut scan = EcTombstoneScan::default();

        loop {
            let limit = self
                .config
                .gc
                .scan_limit
                .min(budget.saturating_sub(scan.manifests_examined))
                .min(max_entries.saturating_sub(scan.tombstones.len()));
            if limit == 0 {
                break;
            }
            let page = self
                .metadata
                .list_manifests_page(cursor.clone(), limit)
                .await
                .map_err(cluster_storage_error)?;
            cursor = page.next_cursor;
            for manifest in page.entries {
                scan.manifests_examined = scan.manifests_examined.saturating_add(1);
                if manifest.is_deleted || manifest.state == EcPlacementState::Tombstone {
                    scan.tombstones.push(manifest);
                }
            }
            if cursor.is_none() {
                scan.round_completed = true;
                break;
            }
            if scan.tombstones.len() >= max_entries || scan.manifests_examined >= budget {
                break;
            }
        }
        scan.next_cursor = if scan.round_completed {
            None
        } else {
            cursor.clone()
        };

        if matches!(start, TombstoneScanStart::WhereTheLastCycleStopped) {
            let rounds = previous.map(|record| record.rounds_completed).unwrap_or(0);
            self.metadata
                .put_tombstone_gc_cursor(&EcTombstoneGcCursorRecord {
                    continuation: if scan.round_completed { None } else { cursor },
                    updated_at: unix_time_nanos(),
                    rounds_completed: rounds
                        .saturating_add(u64::from(u8::from(scan.round_completed))),
                })
                .await
                .map_err(cluster_storage_error)?;
        }
        Ok(scan)
    }

    fn orphan_record_is_gc_eligible(&self, record: &EcOrphanShardRecord, now: u64) -> bool {
        let min_age = seconds_to_nanos(self.config.gc.orphan_min_age_secs);
        let lease_grace = seconds_to_nanos(self.config.gc.orphan_lease_grace_secs);
        let old_enough = now.saturating_sub(record.created_at) >= min_age;
        let lease_safe = record
            .last_lease_seen_at
            .map(|last_seen| now.saturating_sub(last_seen) >= lease_grace)
            .unwrap_or(true);
        old_enough && lease_safe
    }
}

struct TranscodeWorkerStartedGuard {
    started: Arc<AtomicBool>,
}

impl Drop for TranscodeWorkerStartedGuard {
    fn drop(&mut self) {
        self.started.store(false, Ordering::Release);
    }
}

struct IntegrityWorkerStartedGuard {
    repair_started: Arc<AtomicBool>,
    anti_entropy_started: Arc<AtomicBool>,
}

impl Drop for IntegrityWorkerStartedGuard {
    fn drop(&mut self) {
        self.repair_started.store(false, Ordering::Release);
        self.anti_entropy_started.store(false, Ordering::Release);
    }
}

struct EcIntegrityWorkerRuntime {
    service: Arc<EcOperationsService>,
    ports: EcClusterReadPorts,
    anti_entropy: EcAntiEntropyService,
    config: EcIntegrityWorkerConfig,
}

impl EcIntegrityWorkerRuntime {
    fn new(
        service: Arc<EcOperationsService>,
        deps: EcObjectReadRuntimeDeps,
    ) -> Result<Self, ClusterError> {
        let ports = service.read_ports(&deps);
        let anti_entropy = service.anti_entropy_service()?;
        Ok(Self {
            config: service.config.integrity_worker.clone(),
            service,
            ports,
            anti_entropy,
        })
    }

    async fn run(self) {
        let mut last_scrub = 0u64;
        let mut last_anti_entropy = 0u64;
        let mut last_tombstone_gc = 0u64;

        loop {
            let now = unix_time_nanos();
            match self
                .run_once(
                    now,
                    &mut last_scrub,
                    &mut last_anti_entropy,
                    &mut last_tombstone_gc,
                )
                .await
            {
                Ok(processed) if processed > 0 => {
                    tracing::debug!(processed, "EC integrity worker processed items");
                }
                Ok(_) => {}
                Err(error) => {
                    tracing::warn!(error = %error, "EC integrity worker loop failed");
                }
            }
            tokio::time::sleep(self.config.repair_poll_interval).await;
        }
    }

    async fn run_once(
        &self,
        now: u64,
        last_scrub: &mut u64,
        last_anti_entropy: &mut u64,
        last_tombstone_gc: &mut u64,
    ) -> Result<usize, ClusterError> {
        let mut processed = 0usize;

        if self.config.repair_enabled {
            for _ in 0..self.config.repair_batch_size {
                match self
                    .service
                    .repair
                    .repair_next(&self.ports, &self.ports, &self.ports, &self.ports)
                    .await
                {
                    Ok(Some(report)) => {
                        processed = processed.saturating_add(1);
                        tracing::info!(
                            repair_id = %report.task.repair_id,
                            bucket = %report.task.object.bucket,
                            key = %report.task.object.key,
                            chunk_index = report.task.chunk_index,
                            slot_index = report.task.slot_index,
                            "EC repair task completed"
                        );
                    }
                    Ok(None) => break,
                    Err(error) => {
                        tracing::warn!(error = %error, "EC repair worker attempt failed");
                        processed = processed.saturating_add(1);
                        break;
                    }
                }
            }
        }

        if self.config.anti_entropy_enabled
            && due(now, *last_anti_entropy, self.config.anti_entropy_interval)
        {
            processed = processed.saturating_add(self.run_manifest_inventory_audit().await?);
            processed = processed.saturating_add(self.run_namespace_reconcile().await?);
            // Whether any set has stopped meeting its own layout rule. Compared
            // in memory against this node's labels, so it adds nothing
            // measurable to a pass that has just walked every manifest
            // (`EC2-ZONE-04-004`).
            // Placement records this node still owes replicas. Delivering
            // them is anti-entropy of the same kind the audit above does, so
            // it runs on the same tick; a node with nothing owed pays one
            // empty keyspace read for it.
            let hints = self
                .service
                .deliver_small_placement_hints(self.service.config.gc.scan_limit)
                .await?;
            if hints.examined > 0 {
                tracing::info!(
                    examined = hints.examined,
                    delivered = hints.delivered,
                    dropped = hints.dropped,
                    deferred = hints.deferred,
                    "EC small-object placement records redelivered to their replicas"
                );
                processed = processed.saturating_add(hints.examined);
            }
            let drift = self.service.check_layout_drift();
            if drift.violators() > 0 {
                tracing::debug!(
                    sets_checked = drift.sets_checked,
                    sets_drifted = drift.sets_drifted,
                    sets_violated_at_creation = drift.sets_violated_at_creation,
                    "EC layout drift check found sets that do not meet their layout rule"
                );
            }
            *last_anti_entropy = now;
        }

        if self.config.scrub_enabled && due(now, *last_scrub, self.config.scrub_interval) {
            let report = self
                .anti_entropy
                .scrub_local_shards(&self.ports, &self.service.repair, &self.ports, &self.ports)
                .await
                .map_err(cluster_config_error)?;
            processed = processed
                .saturating_add(report.local_shards_scanned)
                .saturating_add(report.repairs_queued);
            *last_scrub = now;
            tracing::debug!(?report, "EC local shard scrub pass completed");
        }

        if self.config.tombstone_gc_enabled
            && due(now, *last_tombstone_gc, self.config.tombstone_gc_interval)
        {
            processed = processed.saturating_add(self.run_tombstone_gc_cycle(now).await?);
            *last_tombstone_gc = now;
        }

        Ok(processed)
    }

    /// Release the shards of objects this node knows to have been deleted.
    ///
    /// Every node runs it, and for the same reason the promoted sweep does:
    /// the delete is agreed on through the metadata quorum, but the shards are
    /// held node by node, and only their holder can let them go.
    async fn run_tombstone_gc_cycle(&self, now: u64) -> Result<usize, ClusterError> {
        // A slice of the keyspace per cycle, continuing where the last one
        // stopped: the walk is over every manifest this node holds, and
        // tombstones are a small part of them (decision EC2-202).
        let scan = self
            .service
            .scan_local_tombstones(
                self.service.config.gc.scan_limit,
                TombstoneScanStart::WhereTheLastCycleStopped,
            )
            .await?;
        if scan.tombstones.is_empty() {
            return Ok(0);
        }
        let sweep = self.service.sweep_tombstones(now, &scan.tombstones).await?;
        let report = &sweep.report;
        if report.shards_purged > 0 || report.tombstones_retired > 0 {
            tracing::info!(
                shards_purged = report.shards_purged,
                objects = report.tombstones_shards_released,
                retired = report.tombstones_retired,
                manifests_examined = scan.manifests_examined,
                round_completed = scan.round_completed,
                "EC tombstone sweep released shards of deleted objects"
            );
        }
        Ok(usize::try_from(report.shards_purged).unwrap_or(usize::MAX))
    }

    async fn run_manifest_inventory_audit(&self) -> Result<usize, ClusterError> {
        let report = self.service.audit_all_manifests(&self.anti_entropy, &self.ports).await?;
        Ok(report.expected_local_shards.saturating_add(report.repairs_queued))
    }

    /// Repair namespace entries as part of the anti-entropy pass.
    ///
    /// It belongs here rather than in its own worker because it answers the
    /// same question anti-entropy already asks — does what this node holds
    /// match what the manifests say — only about the object index instead of
    /// the shards.
    async fn run_namespace_reconcile(&self) -> Result<usize, ClusterError> {
        let report = self
            .service
            .reconcile_namespace_entries(
                &self.ports.read_coordinator,
                &self.ports.write_coordinator,
            )
            .await?;
        let repaired = report.entries_restored.saturating_add(report.entries_detached);
        if repaired > 0 {
            tracing::info!(?report, "EC namespace reconciliation repaired entries");
        }
        Ok(repaired)
    }
}

/// Refuses an on-demand integrity pass unless test hooks are enabled.
///
/// These endpoints walk every manifest on the node and can queue repairs, so
/// they are as unsuitable for a production cluster as the other `/ec/test/`
/// routes, and are gated the same way.
fn ensure_test_hooks_enabled(operation: &str) -> Result<(), ClusterError> {
    if crate::ec_test_hooks::hooks_enabled() {
        return Ok(());
    }
    Err(ClusterError::Config(format!(
        "on-demand EC {operation} requires EC test hooks; set S4_EC_TEST_HOOKS_ENABLED=true"
    )))
}

/// What one namespace reconciliation pass found and did.
#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct EcNamespaceReconcileReport {
    /// Manifests read from local metadata.
    pub manifests_scanned: usize,
    /// Entries written back for objects that had none.
    pub entries_restored: usize,
    /// Hot copies this node still held and has now released.
    pub entries_detached: usize,
    /// Entries already marked erasure coded.
    pub already_current: usize,
    /// Objects whose hot copies are still supposed to be there, because the
    /// purge has not finished its own checks yet.
    pub awaiting_purge: usize,
    /// Entries left alone because a newer generation exists.
    pub superseded: usize,
    /// Objects that could not be repaired on this pass, for example because a
    /// replica was unreachable. Retried on the next one.
    pub failed: usize,
}

/// What the cluster currently says about an object, reduced to what the
/// reconciliation decision actually depends on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ObservedEntry {
    /// No entry on a read quorum of replicas.
    Missing,
    /// An entry that already names the object erasure coded.
    ErasureCoded,
    /// A hot entry of exactly the generation the manifest was built from.
    HotSameGeneration,
    /// A hot entry of some other generation, so the object was overwritten.
    HotOtherGeneration,
}

/// What reconciliation should do about one object.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EcNamespaceAction {
    /// Leave it alone: the namespace already agrees with the manifest.
    Nothing,
    /// Leave it alone: the hot copies are still supposed to be there.
    AwaitPurge,
    /// Leave it alone: a newer generation exists and the live index wins.
    Superseded,
    /// Release hot copies a finished purge left behind on some replica.
    ReleaseHotCopies,
    /// Write the namespace entry back from the manifest.
    RestoreEntry,
}

/// Reduces a quorum HEAD to the four cases the decision distinguishes.
fn observe_entry(head: &QuorumHeadResult, manifest: &EcObjectManifest) -> ObservedEntry {
    if head.erasure_coded {
        ObservedEntry::ErasureCoded
    } else if head.hlc == manifest.hlc {
        ObservedEntry::HotSameGeneration
    } else {
        ObservedEntry::HotOtherGeneration
    }
}

/// The whole reconciliation decision, with no I/O in it.
///
/// Kept as a pure function on purpose: this is where the pass could do real
/// damage, and a table of two inputs can be checked exhaustively without a
/// cluster, which a function that also talks to two coordinators cannot.
///
/// The state input is what keeps the repair out of the purge's way. Before
/// `HotReplicasPurged` the hot copies are meant to be there — the purger still
/// has a full SHA-256 verification and a grace period to clear — and releasing
/// them here would step over both. That mistake looks harmless right up until
/// a transcode produces a bad result and the hot copies are already gone.
fn namespace_action(state: EcPlacementState, observed: ObservedEntry) -> EcNamespaceAction {
    match observed {
        ObservedEntry::ErasureCoded => EcNamespaceAction::Nothing,
        ObservedEntry::HotOtherGeneration => EcNamespaceAction::Superseded,
        ObservedEntry::Missing => EcNamespaceAction::RestoreEntry,
        ObservedEntry::HotSameGeneration => {
            if state == EcPlacementState::HotReplicasPurged {
                EcNamespaceAction::ReleaseHotCopies
            } else {
                EcNamespaceAction::AwaitPurge
            }
        }
    }
}

/// Whether the quorum manifest lets reconciliation release hot copies.
///
/// Only the exact state a finished hot purge leaves behind, and only for the
/// generation the local manifest describes. Every other answer keeps the copies:
///
/// - `promoting` — reverse tiering has marked the object and is putting these
///   very copies back. Without this case the pass takes them for leftovers,
///   because a restored copy has the generation, layout and cached manifest of
///   one; the promotion then fails its read-back, or has already passed it and
///   switches readers to copies that are gone (EC2-026);
/// - `promoted` — the copies are the object;
/// - an earlier EC state — the purge has not finished its own checks;
/// - another generation, a tombstone, or no manifest — not this pass's object
///   any more.
fn quorum_says_hot_copies_are_leftovers(
    read: &EcManifestReadResult,
    local: &EcObjectManifest,
) -> bool {
    match read {
        EcManifestReadResult::Present(current) => {
            !current.is_deleted
                && current.hlc == local.hlc
                && current.state == EcPlacementState::HotReplicasPurged
        }
        EcManifestReadResult::Tombstone(_) | EcManifestReadResult::Absent => false,
    }
}

/// Rebuilds an object's namespace entry from its EC manifest.
///
/// The manifest is the authority for an erasure-coded object and carries every
/// client-visible field, which is what makes this repair possible at all.
fn namespace_entry_from_manifest(manifest: &EcObjectManifest) -> IndexRecord {
    let mut record = IndexRecord::new(
        EXTERNAL_DATA_FILE_ID,
        0,
        manifest.original_size,
        manifest.content_hash_sha256,
        manifest.etag.clone(),
        manifest.content_type.clone(),
    );
    // Extended rather than replaced: `new` seeds the storage class, and user
    // metadata may legitimately override it.
    record.metadata.extend(manifest.user_metadata.clone());
    record.version_id = manifest.version_id.clone();
    record.hlc_timestamp = manifest.hlc.wall_time;
    record.hlc_logical = manifest.hlc.logical;
    record.origin_node_id = manifest.hlc.node_id;
    // Last-Modified comes from the HLC everywhere else in listings, so it does
    // here too; the wall clock is in milliseconds and the record in nanoseconds.
    record.created_at = manifest.hlc.wall_time.saturating_mul(1_000_000);
    record.modified_at = record.created_at;
    record.into_erasure_coded(manifest.erasure_set_id, manifest.topology_epoch)
}

/// Cycle limits the promotion worker runs under, from the node's configuration.
fn promotion_worker_config(tiering: &EcTieringConfig, gc: &EcGcConfig) -> EcPromotionWorkerConfig {
    EcPromotionWorkerConfig {
        enabled: true,
        poll_interval: Duration::from_secs(tiering.promote_poll_secs),
        batch_size: tiering.promote_batch_size,
        shard_grace: Duration::from_secs(gc.promoted_grace_secs),
        max_retry_attempts: tiering.promote_max_attempts,
        retry_backoff: Duration::from_secs(tiering.promote_retry_backoff_secs),
    }
}

fn due(now: u64, last_run: u64, interval: Duration) -> bool {
    last_run == 0 || now.saturating_sub(last_run) >= duration_to_nanos(interval)
}

fn duration_to_nanos(duration: Duration) -> u64 {
    duration.as_nanos().min(u128::from(u64::MAX)) as u64
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EcObjectReadPlan {
    EcMetadata,
    HotReplicated,
    Deleted,
}

#[derive(Clone)]
struct EcClusterReadPorts {
    local_node_id: NodeId,
    /// The pool as this node sees it; reads through these ports ask for near
    /// shards first.
    topology: Arc<PoolTopologyView>,
    topology_epoch: u64,
    /// The pool's sets as they were when this handle was built.
    ///
    /// A manifest names the set that holds its shards, and that set decides
    /// which nodes answer for it. Keeping the whole registry here is what lets
    /// one handle serve objects of every set instead of only the default one. A
    /// snapshot on purpose: one request must see one roster from start to
    /// finish, and a handle is built per request.
    sets: Arc<ErasureSetRegistry>,
    /// The set this handle is bound to.
    ///
    /// The pool's default set unless a caller rebound the handle to the set of
    /// the object it is working on. It is the set used when there is no
    /// manifest to resolve one from.
    erasure_set: ErasureSet,
    metadata: Arc<EcMetadataStore>,
    codec_limiter: EcCodecCpuLimiter,
    read_coordinator: Arc<QuorumReadCoordinator>,
    write_coordinator: Arc<QuorumWriteCoordinator>,
    storage: Arc<dyn StorageEngine>,
    rpc_client: Arc<NodeClient>,
    /// Metadata nodes of the bound set.
    metadata_nodes: Vec<NodeId>,
    /// Metadata nodes of every set of the pool, by set id, in registry order.
    ///
    /// Computed once so that a manifest read, which does not know its set
    /// before it has the manifest, can fan out over the whole pool without
    /// walking the slot assignments again on every request.
    pool_metadata_nodes: Vec<(u64, Vec<NodeId>)>,
    /// What `S4_EC_MINIMAL_READ_ENABLED` says on this node.
    ///
    /// Carried here rather than read again, so that every read of one request
    /// works to one answer, and so that the verification reads which insist on
    /// the whole chunk can override it in one visible place
    /// (decision EC2-184).
    minimal_read: bool,
}

struct EcClusterReadPortsDeps {
    local_node_id: NodeId,
    topology: Arc<PoolTopologyView>,
    topology_epoch: u64,
    sets: Arc<ErasureSetRegistry>,
    erasure_set: ErasureSet,
    metadata: Arc<EcMetadataStore>,
    codec_limiter: EcCodecCpuLimiter,
    read_coordinator: Arc<QuorumReadCoordinator>,
    write_coordinator: Arc<QuorumWriteCoordinator>,
    storage: Arc<dyn StorageEngine>,
    rpc_client: Arc<NodeClient>,
    minimal_read: bool,
}

/// Whether every set of the pool has reached `required` counted answers.
///
/// Used for the two outcomes that only hold pool-wide: an object is absent when
/// no set has it, and a manifest is removed when no set still serves it. One set
/// alone can decide neither, which is why this is not a single counter.
fn every_set_reached(
    pool_metadata_nodes: &[(u64, Vec<NodeId>)],
    counted: &HashMap<u64, u8>,
    required: u8,
) -> bool {
    // A pool with no sets has nothing to have reached anything. `all` over an
    // empty list is true, which would turn "this node has no pool configured"
    // into "the object does not exist" — the one answer that must never be
    // guessed.
    if pool_metadata_nodes.is_empty() {
        return false;
    }
    pool_metadata_nodes
        .iter()
        .all(|(set_id, _)| counted.get(set_id).copied().unwrap_or(0) >= required)
}

/// Whether every set of the pool has answered for good.
///
/// A set is decided once it has produced `required` matching manifests or
/// `required` absences; nothing still in flight for it can change the outcome.
/// The read stops there instead of draining the stragglers, so a silent node in
/// one set does not add its RPC timeout to reads of every other set.
///
/// Empty pool guarded for the same reason as [`every_set_reached`]: `all` over
/// nothing is true, and "this node has no pool" must not read as a verdict.
fn every_set_decided(pool_metadata_nodes: &[(u64, Vec<NodeId>)], decided: &HashSet<u64>) -> bool {
    if pool_metadata_nodes.is_empty() {
        return false;
    }
    pool_metadata_nodes.iter().all(|(set_id, _)| decided.contains(set_id))
}

/// Refuse a shard read aimed at a node outside the set the manifest names.
///
/// Membership, not "holds this slot in the current layout": the manifest says
/// where its own shards are, and the layout may have been reordered since the
/// object was written (EC2-086, point 1). What this still stops is a manifest
/// sending a read to a node of another set — the sets of a pool never share a
/// node — whose bytes would then fail verification for no visible reason.
fn ensure_shard_node_in_set(
    erasure_set: &ErasureSet,
    slot_index: u8,
    node_id: NodeId,
) -> Result<(), EcReadError> {
    if erasure_set.holds_node(node_id) {
        return Ok(());
    }
    Err(EcReadError::InvalidManifest {
        reason: format!(
            "slot {slot_index} is placed on node {node_id}, which is not a node of erasure set {}",
            erasure_set.set_id
        ),
    })
}

fn set_metadata_nodes(erasure_set: &ErasureSet) -> Vec<NodeId> {
    let mut nodes = erasure_set
        .slot_assignment
        .slots()
        .iter()
        .map(|slot| slot.node_id)
        .collect::<Vec<_>>();
    nodes.sort_unstable_by_key(|node| node.0);
    nodes.dedup();
    nodes
}

impl EcClusterReadPorts {
    fn new(deps: EcClusterReadPortsDeps) -> Self {
        let metadata_nodes = set_metadata_nodes(&deps.erasure_set);
        let pool_metadata_nodes = deps
            .sets
            .sets()
            .into_iter()
            .map(|set| (set.set_id, set_metadata_nodes(set)))
            .collect::<Vec<_>>();

        Self {
            local_node_id: deps.local_node_id,
            topology: deps.topology,
            topology_epoch: deps.topology_epoch,
            sets: deps.sets,
            erasure_set: deps.erasure_set,
            metadata: deps.metadata,
            codec_limiter: deps.codec_limiter,
            read_coordinator: deps.read_coordinator,
            write_coordinator: deps.write_coordinator,
            storage: deps.storage,
            rpc_client: deps.rpc_client,
            metadata_nodes,
            pool_metadata_nodes,
            minimal_read: deps.minimal_read,
        }
    }

    /// A read coordinator that asks for near shards first, narrowed to the
    /// `k` data shards of a chunk unless this node was told otherwise.
    fn ec_reader(&self) -> EcReadCoordinator {
        EcReadCoordinator::new(Arc::clone(&self.metadata), self.codec_limiter.clone())
            .with_topology(Arc::clone(&self.topology))
            .with_minimal_read(self.minimal_read)
    }

    /// The same ports bound to another set of the same pool.
    ///
    /// Cheap enough to do per operation: a short node-id vector and a handful
    /// of `Arc` clones. Handing back the current handle when the set is already
    /// the bound one keeps a single-set pool free of even that.
    fn rebound_to(&self, erasure_set: &ErasureSet) -> Self {
        if erasure_set.set_id == self.erasure_set.set_id {
            return self.clone();
        }
        let mut rebound = self.clone();
        rebound.metadata_nodes = set_metadata_nodes(erasure_set);
        rebound.erasure_set = erasure_set.clone();
        rebound
    }

    /// The set a manifest belongs to, refusing one this pool never issued.
    ///
    /// Falling back to the bound set would serve the manifest from unrelated
    /// nodes, so an unknown id is an error rather than a default.
    fn set_for_manifest_id(&self, manifest_set_id: u64) -> Result<&ErasureSet, EcReadError> {
        self.sets.resolve_manifest_set(manifest_set_id).ok_or_else(|| {
            EcReadError::InvalidManifest {
                reason: format!(
                    "manifest names erasure set {manifest_set_id}, which pool {} does not have",
                    self.sets.pool_id()
                ),
            }
        })
    }

    /// Ports bound to the set that holds `manifest`.
    fn ports_for_manifest(&self, manifest: &EcObjectManifest) -> Result<Self, EcReadError> {
        let set = self.set_for_manifest_id(manifest.erasure_set_id)?;
        Ok(self.rebound_to(set))
    }

    fn ensure_manifest_quorum_size(&self, required_acks: u8) -> Result<(), EcReadError> {
        if usize::from(required_acks) > self.metadata_nodes.len() {
            return Err(EcReadError::Unavailable {
                reason: format!(
                    "manifest quorum requires {required_acks} node(s), only {} available",
                    self.metadata_nodes.len()
                ),
            });
        }
        Ok(())
    }

    /// Every metadata node of the pool, each listed once.
    fn pool_nodes(&self) -> Vec<NodeId> {
        let mut nodes = self
            .pool_metadata_nodes
            .iter()
            .flat_map(|(_, nodes)| nodes.iter().copied())
            .collect::<Vec<_>>();
        nodes.sort_unstable_by_key(|node| node.0);
        nodes.dedup();
        nodes
    }

    /// Whether every set of the pool has reached `required` counted answers.
    fn every_set_reached(&self, counted: &HashMap<u64, u8>, required: u8) -> bool {
        every_set_reached(&self.pool_metadata_nodes, counted, required)
    }

    /// Whether every set of the pool answered "no such manifest" at quorum.
    fn every_set_reports_absent(&self, absent: &HashMap<u64, u8>, required: u8) -> bool {
        self.every_set_reached(absent, required)
    }

    async fn commit_manifest_quorum_count(
        &self,
        manifest: EcObjectManifest,
        required: u8,
    ) -> Result<u8, EcReadError> {
        // Written to the nodes of the set the manifest names, not to the ones
        // this handle happens to be bound to. A manifest committed to another
        // set's nodes would be invisible to every reader of its own set.
        let ports = self.ports_for_manifest(&manifest)?;
        ports.ensure_manifest_quorum_size(required)?;
        manifest.validate_against_erasure_set(&ports.erasure_set)?;

        let key = manifest_object_key(&manifest.object_key());
        let data = encode_manifest(&manifest).map_err(|error| EcReadError::InvalidManifest {
            reason: format!("manifest serialization failed: {error}"),
        })?;
        let metadata = manifest_metadata(&manifest)?;
        let operation_id = derive_storage_operation_id(
            manifest.operation_id,
            b"ec-manifest-write",
            0,
            0,
            manifest.state,
        );
        let mut set = JoinSet::new();

        for node_id in ports.metadata_nodes.clone() {
            let ports = ports.clone();
            let write = EcHiddenObjectWrite {
                node_id,
                key: key.clone(),
                data: data.clone(),
                content_type: EC_MANIFEST_CONTENT_TYPE,
                metadata: metadata.clone(),
                hlc: manifest_storage_hlc(manifest.hlc, manifest.state),
                operation_id,
            };
            set.spawn(async move { write_hidden_object_to_node(&ports, write).await });
        }

        let mut acks = 0u8;
        while let Some(result) = set.join_next().await {
            match result {
                Ok(Ok(())) => {
                    acks = acks.saturating_add(1);
                    if acks >= required {
                        set.abort_all();
                        return Ok(acks);
                    }
                }
                Ok(Err(error)) => {
                    tracing::warn!(error = %error, "EC manifest quorum write candidate failed");
                }
                Err(error) => {
                    tracing::warn!(error = %error, "EC manifest quorum write task failed");
                }
            }
        }

        Ok(acks)
    }

    /// Remove a manifest from every metadata node, not merely from a quorum.
    ///
    /// The quorum-shaped delete below stops as soon as it has enough ACKs and
    /// abandons the rest, which is right when the point is to make the removal
    /// durable. It is wrong for a manifest that must stop existing: a node left
    /// holding it keeps answering with it, and if that manifest outranks what
    /// is written next — as a promotion marker does, being the highest-ranked
    /// state at its generation's clock value — the pool splits and no state
    /// ever reaches a matching quorum again.
    ///
    /// Returns the nodes that did not confirm. An empty list is the only
    /// outcome the caller may treat as done.
    async fn delete_manifest_everywhere(
        &self,
        object: &EcObjectVersionKey,
        hlc: HlcTimestamp,
    ) -> Vec<NodeId> {
        let key = manifest_object_key(object);
        let operation_id = Uuid::new_v4().as_u128();
        let mut set = JoinSet::new();

        // Every set of the pool, not only the bound one. This removal is
        // addressed by object key, so it cannot know which set answers for the
        // object, and a copy left in another set is exactly the split the
        // method exists to prevent.
        for node_id in self.pool_nodes() {
            let ports = self.clone();
            let key = key.clone();
            set.spawn(async move {
                (
                    node_id,
                    delete_hidden_object_from_node(&ports, node_id, key, hlc, operation_id).await,
                )
            });
        }

        let mut unconfirmed = Vec::new();
        while let Some(result) = set.join_next().await {
            match result {
                Ok((_, Ok(()))) => {}
                Ok((node_id, Err(error))) => {
                    tracing::warn!(
                        node_id = %node_id,
                        error = %error,
                        "EC manifest removal was not confirmed by a metadata node"
                    );
                    unconfirmed.push(node_id);
                }
                Err(error) => {
                    tracing::warn!(error = %error, "EC manifest removal task failed");
                    // The node it belonged to is unknown, so it is reported as
                    // the local one: the caller only needs "not all of them".
                    unconfirmed.push(self.local_node_id);
                }
            }
        }
        unconfirmed
    }

    async fn delete_manifest_quorum(
        &self,
        object: &EcObjectVersionKey,
        hlc: HlcTimestamp,
    ) -> Result<(), EcReadError> {
        let required = self.erasure_set.profile.manifest_quorum();
        for (set_id, nodes) in &self.pool_metadata_nodes {
            if usize::from(required) > nodes.len() {
                return Err(EcReadError::Unavailable {
                    reason: format!(
                        "manifest quorum requires {required} node(s), erasure set {set_id} \
                         only has {}",
                        nodes.len()
                    ),
                });
            }
        }

        let key = manifest_object_key(object);
        let operation_id = Uuid::new_v4().as_u128();
        let mut set = JoinSet::new();

        // Addressed by object key like the removal above, so it goes to every
        // set: a quorum inside one set says nothing about the others, and each
        // of them answers for the object on its own.
        for (set_id, nodes) in self.pool_metadata_nodes.clone() {
            for node_id in nodes {
                let ports = self.clone();
                let key = key.clone();
                set.spawn(async move {
                    (
                        set_id,
                        delete_hidden_object_from_node(&ports, node_id, key, hlc, operation_id)
                            .await,
                    )
                });
            }
        }

        let mut acks_by_set: HashMap<u64, u8> = HashMap::new();
        while let Some(result) = set.join_next().await {
            match result {
                Ok((set_id, Ok(()))) => {
                    let acks = acks_by_set.entry(set_id).or_insert(0);
                    *acks = acks.saturating_add(1);
                    if self.every_set_reached(&acks_by_set, required) {
                        set.abort_all();
                        return Ok(());
                    }
                }
                Ok((_, Err(error))) => {
                    tracing::warn!(error = %error, "EC manifest quorum delete candidate failed");
                }
                Err(error) => {
                    tracing::warn!(error = %error, "EC manifest quorum delete task failed");
                }
            }
        }

        let short = self
            .pool_metadata_nodes
            .iter()
            .map(|(set_id, _)| {
                let acks = acks_by_set.get(set_id).copied().unwrap_or(0);
                format!("set {set_id}: {acks}")
            })
            .collect::<Vec<_>>()
            .join(", ");
        Err(EcReadError::Unavailable {
            reason: format!(
                "manifest quorum delete required {required} ACK(s) per erasure set, got {short}"
            ),
        })
    }

    async fn put_local_shard_from_head(
        &self,
        object: &EcObjectVersionKey,
        request: &EcRepairShardWriteRequest,
    ) -> Result<(), EcRepairError> {
        let hidden_key = shard_object_key(
            object,
            request.operation_id,
            request.chunk_index,
            request.expected.slot_index,
        );
        let record =
            self.storage.head_object(SYSTEM_BUCKET, &hidden_key).await.map_err(|error| {
                EcRepairError::ShardWrite {
                    reason: format!("local repaired EC shard HEAD failed: {error}"),
                }
            })?;
        let key = LocalShardKey {
            object: object.clone(),
            chunk_index: request.chunk_index,
            slot_index: request.expected.slot_index,
        };
        let local = local_shard_record_from_index(&record, &request.expected, request.operation_id);
        self.metadata.put_local_shard(&key, &local).await?;
        Ok(())
    }
}

#[async_trait]
impl EcReadMetadataCoordinator for EcClusterReadPorts {
    async fn quorum_read_object_layout(
        &self,
        object: &EcObjectVersionKey,
    ) -> Result<EcObjectLayoutReadResult, EcReadError> {
        if let Some(layout) = self.metadata.get_object_layout(object).await? {
            layout.validate_for_object(object)?;
            match &layout {
                // A replicated-small object has no manifest at all, and a packed
                // object that still holds hot copies is read through them, so in
                // both cases the local record is the whole answer.
                ObjectDataLayout::ReplicatedSmall(_) => {
                    Ok(EcObjectLayoutReadResult::Present(layout))
                }
                ObjectDataLayout::PackedSegment(ref placement)
                    if placement.hot_placement().is_some() =>
                {
                    Ok(EcObjectLayoutReadResult::Present(layout))
                }
                // Once the hot copies are gone the pointer manifest is the
                // authority; the local record is a cache that an overwrite
                // through another node may have left behind.
                ObjectDataLayout::PackedSegment(_) => {
                    match <Self as EcReadMetadataCoordinator>::quorum_read_manifest(self, object)
                        .await?
                    {
                        EcManifestReadResult::Present(manifest) => {
                            self.metadata.cache_manifest(&manifest).await?;
                            Ok(EcObjectLayoutReadResult::Present(layout_from_manifest(
                                &manifest,
                            )?))
                        }
                        EcManifestReadResult::Tombstone(manifest) => {
                            Ok(EcObjectLayoutReadResult::Tombstone(Box::new(manifest)))
                        }
                        EcManifestReadResult::Absent => Ok(EcObjectLayoutReadResult::Absent),
                    }
                }
                ObjectDataLayout::ErasureCoded(reference) => {
                    match <Self as EcReadMetadataCoordinator>::quorum_read_manifest(self, object)
                        .await?
                    {
                        EcManifestReadResult::Present(manifest) => {
                            let current =
                                erasure_coded_layout_for_quorum_manifest(reference, &manifest)?;
                            self.metadata.cache_manifest(&manifest).await?;
                            Ok(EcObjectLayoutReadResult::Present(current))
                        }
                        EcManifestReadResult::Tombstone(manifest) => {
                            Ok(EcObjectLayoutReadResult::Tombstone(Box::new(manifest)))
                        }
                        EcManifestReadResult::Absent => Ok(EcObjectLayoutReadResult::Absent),
                    }
                }
            }
        } else {
            match <Self as EcReadMetadataCoordinator>::quorum_read_manifest(self, object).await? {
                EcManifestReadResult::Present(manifest) => {
                    self.metadata.cache_manifest(&manifest).await?;
                    Ok(EcObjectLayoutReadResult::Present(layout_from_manifest(
                        &manifest,
                    )?))
                }
                EcManifestReadResult::Tombstone(manifest) => {
                    Ok(EcObjectLayoutReadResult::Tombstone(Box::new(manifest)))
                }
                // No manifest is the answer for two very different objects: one
                // the pool deliberately placed as replicated-small, and one the
                // hot path is simply still holding. The placement record is
                // what tells them apart, and a node that has never resolved
                // this object has to ask the pool for it.
                EcManifestReadResult::Absent => {
                    match read_replicated_small_placement(self, object).await? {
                        Some(placement) => {
                            let layout = ObjectDataLayout::ReplicatedSmall(placement);
                            self.metadata.cache_object_layout(object, &layout).await?;
                            Ok(EcObjectLayoutReadResult::Present(layout))
                        }
                        None => Ok(EcObjectLayoutReadResult::Absent),
                    }
                }
            }
        }
    }

    /// Read the winning manifest from the pool's metadata quorum.
    ///
    /// The set that holds an object is written in its manifest, so a reader
    /// cannot know it before the read: the fan-out covers every set of the
    /// pool. Two consequences follow, and both collapse to EC v1 behaviour when
    /// the pool has one set.
    ///
    /// Absence is per set. One set answering "no such object" only says the
    /// object is not in that set, so `Absent` needs every set to say it.
    ///
    /// A pool of several sets also cannot stop at the first quorum. Successive
    /// generations of one object may be admitted into different sets, and each
    /// generation's manifest reaches quorum inside its own set, so the first
    /// quorum to arrive is whichever set answered fastest — not the newest
    /// object. `manifest_wins` picks between the sets' answers instead.
    ///
    /// What the read does *not* wait for is the rest of a set that has already
    /// answered. Each set is decided on its own — `required` matching manifests
    /// or `required` absences — and the read stops as soon as every set has a
    /// verdict. Draining every task instead would let one silent node anywhere
    /// in the pool add its RPC timeout to every read of every set, which is the
    /// isolation between sets that plan 03 exists to provide.
    ///
    /// Quorum counting is per set, not pool-wide: a manifest lives in exactly
    /// one set, so copies found in two sets are two different findings and must
    /// never be added together into a quorum neither set reached.
    async fn quorum_read_manifest(
        &self,
        object: &EcObjectVersionKey,
    ) -> Result<EcManifestReadResult, EcReadError> {
        let required = self.erasure_set.profile.manifest_quorum();
        for (set_id, nodes) in &self.pool_metadata_nodes {
            if usize::from(required) > nodes.len() {
                return Err(EcReadError::Unavailable {
                    reason: format!(
                        "manifest quorum requires {required} node(s), erasure set {set_id} \
                         only has {}",
                        nodes.len()
                    ),
                });
            }
        }

        let key = manifest_object_key(object);
        let operation_id = derive_storage_operation_id(
            deterministic_object_uuid(object),
            b"ec-manifest-read",
            0,
            0,
            EcPlacementState::EcManifestCommitted,
        );
        let mut set = JoinSet::new();

        for (set_id, nodes) in self.pool_metadata_nodes.clone() {
            for node_id in nodes {
                let ports = self.clone();
                let key = key.clone();
                let object = object.clone();
                set.spawn(async move {
                    (
                        set_id,
                        read_manifest_from_node(&ports, node_id, key, object, operation_id).await,
                    )
                });
            }
        }

        let mut absent_by_set: HashMap<u64, u8> = HashMap::new();
        let mut by_hash: BTreeMap<(u64, [u8; 32]), (EcObjectManifest, u8)> = BTreeMap::new();
        // Sets that have answered for good, either way. Once every set is in
        // here nothing still in flight can change the outcome.
        let mut decided: HashSet<u64> = HashSet::new();
        while let Some(result) = set.join_next().await {
            match result {
                Ok((set_id, Ok(ManifestNodeRead::Present(manifest)))) => {
                    let manifest = *manifest;
                    let hash = manifest.serialized_sha256()?;
                    let count = by_hash
                        .entry((set_id, hash))
                        .and_modify(|(_, count)| *count = count.saturating_add(1))
                        .or_insert((manifest, 1))
                        .1;
                    if count >= required {
                        decided.insert(set_id);
                    }
                }
                Ok((set_id, Ok(ManifestNodeRead::Absent))) => {
                    let absent = absent_by_set.entry(set_id).or_insert(0);
                    *absent = absent.saturating_add(1);
                    if *absent >= required {
                        decided.insert(set_id);
                    }
                    if self.every_set_reports_absent(&absent_by_set, required) {
                        set.abort_all();
                        return Ok(EcManifestReadResult::Absent);
                    }
                }
                Ok((_, Err(error))) => {
                    tracing::warn!(error = %error, "EC read manifest quorum candidate failed");
                }
                Err(error) => {
                    tracing::warn!(error = %error, "EC read manifest quorum task failed");
                }
            }

            if every_set_decided(&self.pool_metadata_nodes, &decided) {
                set.abort_all();
                break;
            }
        }

        let mut winner: Option<EcObjectManifest> = None;
        for (_, (manifest, count)) in by_hash {
            if count < required {
                continue;
            }
            winner = match winner {
                None => Some(manifest),
                Some(current) if manifest_wins(&manifest, &current) => Some(manifest),
                Some(current) => Some(current),
            };
        }

        if let Some(manifest) = winner {
            return Ok(manifest.into_read_result());
        }
        if self.every_set_reports_absent(&absent_by_set, required) {
            return Ok(EcManifestReadResult::Absent);
        }

        Err(EcReadError::Unavailable {
            reason: format!("manifest quorum read did not reach {required} matching response(s)"),
        })
    }
}

#[async_trait]
impl EcGcMetadataCoordinator for EcClusterReadPorts {
    async fn quorum_read_object_layout(
        &self,
        object: &EcObjectVersionKey,
    ) -> Result<EcObjectLayoutReadResult, EcGcError> {
        <Self as EcReadMetadataCoordinator>::quorum_read_object_layout(self, object)
            .await
            .map_err(ec_read_to_gc_error)
    }

    async fn retire_tombstone_everywhere(
        &self,
        tombstone: &EcObjectManifest,
    ) -> Result<(), EcGcError> {
        let object = tombstone.object_key();
        let unconfirmed = self.delete_manifest_everywhere(&object, tombstone.hlc).await;
        if !unconfirmed.is_empty() {
            // Every node or none. A tombstone removed from half the pool is a
            // manifest no quorum read can decide either way, which would leave
            // the object answering "unavailable" instead of "gone" and would
            // strand the shards of every owner that had not swept yet.
            return Err(EcGcError::Metadata(
                crate::ec_metadata::EcMetadataError::QuorumRead(format!(
                    "{} metadata node(s) did not confirm removal of the retired tombstone",
                    unconfirmed.len()
                )),
            ));
        }
        tracing::debug!(
            bucket = %object.bucket,
            key = %object.key,
            "EC tombstone retired from every metadata node of the pool"
        );
        Ok(())
    }
}

#[async_trait]
impl EcTombstoneQuorum for EcClusterReadPorts {
    async fn quorum_read_manifest(
        &self,
        object: &EcObjectVersionKey,
    ) -> Result<EcManifestReadResult, EcGcError> {
        <Self as EcReadMetadataCoordinator>::quorum_read_manifest(self, object)
            .await
            .map_err(ec_read_to_gc_error)
    }

    async fn commit_tombstone(
        &self,
        tombstone: EcObjectManifest,
        required_acks: u8,
    ) -> Result<EcTombstoneCommitResult, EcGcError> {
        let acks = self
            .commit_manifest_quorum_count(tombstone, required_acks)
            .await
            .map_err(ec_read_to_gc_error)?;
        Ok(EcTombstoneCommitResult { acks })
    }
}

#[async_trait]
impl EcReplicatedSmallDeleter for EcClusterReadPorts {
    async fn tombstone_replicated_small(
        &self,
        request: EcReplicatedSmallDeleteRequest,
    ) -> Result<EcReplicatedSmallDeleteResult, EcGcError> {
        if request.object.version_id.is_some() {
            return Err(EcGcError::InvalidRequest {
                reason:
                    "version-specific replicated-small delete requires version-aware cluster delete"
                        .to_string(),
            });
        }
        Ok(EcReplicatedSmallDeleteResult {
            acks: request.required_acks,
        })
    }

    async fn purge_replicated_small(
        &self,
        request: EcReplicatedSmallPurgeRequest,
    ) -> Result<EcReplicatedSmallPurgeResult, EcGcError> {
        if request.object.version_id.is_some() {
            return Err(EcGcError::InvalidRequest {
                reason:
                    "version-specific replicated-small purge requires version-aware cluster delete"
                        .to_string(),
            });
        }
        let result = self
            .write_coordinator
            .delete(&request.object.bucket, &request.object.key)
            .await
            .map_err(|error| {
                EcGcError::Metadata(crate::ec_metadata::EcMetadataError::QuorumRead(
                    error.to_string(),
                ))
            })?;
        Ok(EcReplicatedSmallPurgeResult {
            acks: result.replicas_acked,
        })
    }
}

#[async_trait]
impl EcDeadShardPurger for EcClusterReadPorts {
    async fn purge_dead_shard(&self, request: EcDeadShardPurgeRequest) -> Result<(), EcGcError> {
        let hidden_key = shard_object_key(
            &request.key.object,
            request.record.operation_id,
            request.key.chunk_index,
            request.key.slot_index,
        );
        // Every kind removes the same bytes the same way. The distinction is
        // kept because the reason a shard is dead is what an operator needs to
        // see in a log, not because the removal differs.
        match request.kind {
            EcDeadShardPurgeKind::Tombstone
            | EcDeadShardPurgeKind::Orphan
            | EcDeadShardPurgeKind::Promoted => {}
        }
        delete_local_hidden_object(Arc::clone(&self.storage), hidden_key)
            .await
            .map_err(ec_read_to_gc_error)
    }
}

#[async_trait]
impl EcReplicatedSmallReader for EcClusterReadPorts {
    async fn read_replicated_small(
        &self,
        request: EcReplicatedSmallReadRequest,
    ) -> Result<EcReplicatedSmallReadResult, EcReadError> {
        if request.object.version_id.is_some() {
            return Err(EcReadError::Unavailable {
                reason:
                    "version-specific replicated-small reads require a version-aware cluster reader"
                        .to_string(),
            });
        }

        let result = self
            .read_coordinator
            .read(&request.object.bucket, &request.object.key)
            .await
            .map_err(cluster_read_to_ec_error)?;
        if result.size != request.placement.size {
            return Err(EcReadError::ReplicatedSmallVerification {
                reason: format!(
                    "replicated-small size changed from {} to {}",
                    request.placement.size, result.size
                ),
            });
        }
        let QuorumReadResult {
            data,
            etag,
            content_type,
            metadata,
            hlc,
            version_id,
            replicas_responded,
            ..
        } = result;
        let full = Bytes::from(data);
        let actual = sha256(&full);
        if actual != request.placement.content_hash_sha256 {
            return Err(EcReadError::ReplicatedSmallVerification {
                reason:
                    "replicated-small RF=3 read returned bytes that do not match placement SHA-256"
                        .to_string(),
            });
        }

        let (body, content_range) = apply_read_range(full, request.range, request.placement.size)?;
        let content_length = u64::try_from(body.len()).map_err(|_| EcReadError::InvalidRange {
            start: 0,
            end: request.placement.size,
            total: request.placement.size,
        })?;
        let head = EcHeadObject {
            object: request.object,
            etag,
            content_type,
            size: request.placement.size,
            user_metadata: user_metadata(metadata),
            content_hash_sha256: request.placement.content_hash_sha256,
            hlc,
            version_id,
            layout: EcReadLayout::ReplicatedSmall {
                replica_nodes: request.placement.replica_nodes.clone(),
            },
        };

        Ok(EcReplicatedSmallReadResult {
            body,
            head,
            content_length,
            content_range,
            replicas_responded,
        })
    }

    async fn head_replicated_small(
        &self,
        request: EcReplicatedSmallHeadRequest,
    ) -> Result<EcHeadObject, EcReadError> {
        if request.object.version_id.is_some() {
            return Err(EcReadError::Unavailable {
                reason:
                    "version-specific replicated-small HEAD requires a version-aware cluster reader"
                        .to_string(),
            });
        }
        let result = self
            .read_coordinator
            .head(&request.object.bucket, &request.object.key)
            .await
            .map_err(cluster_read_to_ec_error)?;
        if result.size != request.placement.size {
            return Err(EcReadError::ReplicatedSmallVerification {
                reason: format!(
                    "replicated-small HEAD size changed from {} to {}",
                    request.placement.size, result.size
                ),
            });
        }
        Ok(head_from_quorum_head(
            request.object,
            result,
            request.placement.replica_nodes,
            request.placement.content_hash_sha256,
        ))
    }
}

#[async_trait]
impl EcShardReader for EcClusterReadPorts {
    async fn read_shard(
        &self,
        request: EcReadShardRequest,
    ) -> Result<Option<EcReadShardResponse>, EcReadError> {
        let erasure_set = self.set_for_manifest_id(request.erasure_set_id)?;
        ensure_shard_node_in_set(erasure_set, request.slot_index, request.node_id)?;

        let key = shard_object_key(
            &request.object,
            request.operation_id,
            request.chunk_index,
            request.slot_index,
        );
        let operation_id = derive_storage_operation_id(
            request.operation_id,
            b"ec-shard-read",
            request.chunk_index,
            request.slot_index,
            EcPlacementState::EcReadVerified,
        );
        let Some(read) =
            read_hidden_object_from_node(self, request.node_id, key, operation_id).await?
        else {
            return Ok(None);
        };

        let replica_crc32 =
            read.metadata.get("_s4_ec_crc32").and_then(|raw| raw.parse::<u32>().ok());
        Ok(Some(EcReadShardResponse {
            data: read.data,
            replica_crc32,
        }))
    }
}

#[async_trait]
impl EcPackedSegmentUnpacker for EcClusterReadPorts {
    async fn restore_hot_copies(
        &self,
        segment: &PackedSegment,
        entry: &PackedSegmentEntry,
        placement: &PackedSegmentObjectPlacement,
    ) -> Result<Vec<NodeId>, EcGcError> {
        // The segment was never released, so the object's own three copies are
        // still where the write path put them. Rewriting them would spend a
        // full object write to arrive at the bytes that are already there.
        if !placement.hot_replica_nodes.is_empty() {
            return Ok(placement.hot_replica_nodes.clone());
        }
        if entry.object.version_id.is_some() {
            return Err(EcGcError::InvalidRequest {
                reason: "unpacking a specific object version requires a version-aware cluster \
                         writer"
                    .to_string(),
            });
        }

        let reader = self.ec_reader();
        let body = reader
            .read_packed_object_bytes(
                entry.object.clone(),
                placement,
                self,
                &NoopEcReadRepairQueue,
            )
            .await
            .map_err(|error| EcGcError::PackedRepack {
                reason: format!(
                    "reading {}/{} out of segment {} failed: {error}",
                    entry.object.bucket, entry.object.key, segment.segment_id
                ),
            })?;

        // Guarded by the object's write clock, exactly as a promotion is: a
        // client PUT that landed while the unpack ran must not be overwritten
        // by the older bytes this just assembled.
        let restored = self
            .write_coordinator
            .attach_object_data_if_hlc(&entry.object.bucket, &entry.object.key, &body, entry.hlc)
            .await
            .map_err(|error| EcGcError::PackedRepack {
                reason: format!(
                    "restoring the RF=3 copies of {}/{} failed: {error}",
                    entry.object.bucket, entry.object.key
                ),
            })?;
        if restored.replica_nodes.len() < REPLICATED_SMALL_REPLICA_COUNT {
            return Err(EcGcError::PackedRepack {
                reason: format!(
                    "{}/{} was placed on {} replica(s); an unpacked object needs \
                     {REPLICATED_SMALL_REPLICA_COUNT}",
                    entry.object.bucket,
                    entry.object.key,
                    restored.replica_nodes.len()
                ),
            });
        }
        Ok(restored
            .replica_nodes
            .into_iter()
            .take(REPLICATED_SMALL_REPLICA_COUNT)
            .collect())
    }

    async fn publish_unpacked_manifest(
        &self,
        object: &EcObjectVersionKey,
    ) -> Result<(), EcGcError> {
        let manifest = match <Self as EcTombstoneQuorum>::quorum_read_manifest(self, object).await?
        {
            EcManifestReadResult::Present(manifest) => manifest,
            // No manifest describes the object at all — a segment that was
            // never released never committed one. Nothing to withdraw.
            EcManifestReadResult::Tombstone(_) | EcManifestReadResult::Absent => return Ok(()),
        };
        if manifest.state == EcPlacementState::Promoted {
            return Ok(());
        }

        // Promoted is what every node reads as "this object is back on its
        // replicas": the read path refuses to serve a promoted manifest through
        // EC and falls back to the hot copies, whatever placement the manifest
        // still carries. Reusing it means unpacking needs no new state and no
        // new rule in the reader.
        let mut promoted = manifest;
        promoted.state = EcPlacementState::Promoted;
        promoted.origin_node_id = self.local_node_id;
        promoted.created_at = unix_time_nanos();

        let required = self.erasure_set.profile.manifest_quorum();
        let acks =
            self.commit_manifest_quorum_count(promoted, required).await.map_err(|error| {
                EcGcError::PackedRepack {
                    reason: format!(
                        "withdrawing the packed pointer of {}/{} failed: {error}",
                        object.bucket, object.key
                    ),
                }
            })?;
        if acks < required {
            return Err(EcGcError::ManifestQuorum {
                required_acks: required,
                actual_acks: acks,
            });
        }
        Ok(())
    }
}

#[async_trait]
impl EcRepairMetadataCoordinator for EcClusterReadPorts {
    async fn quorum_read_manifest(
        &self,
        object: &EcObjectVersionKey,
    ) -> Result<EcManifestReadResult, EcRepairError> {
        <Self as EcReadMetadataCoordinator>::quorum_read_manifest(self, object)
            .await
            .map_err(ec_read_to_repair_error)
    }
}

#[async_trait]
impl EcAntiEntropyMetadataCoordinator for EcClusterReadPorts {
    async fn quorum_read_manifest(
        &self,
        object: &EcObjectVersionKey,
    ) -> Result<EcManifestReadResult, EcAntiEntropyError> {
        <Self as EcReadMetadataCoordinator>::quorum_read_manifest(self, object)
            .await
            .map_err(|error| {
                EcAntiEntropyError::Metadata(crate::ec_metadata::EcMetadataError::QuorumRead(
                    error.to_string(),
                ))
            })
    }
}

#[async_trait]
impl EcRepairShardReader for EcClusterReadPorts {
    async fn read_repair_shard(
        &self,
        request: EcRepairShardReadRequest,
    ) -> Result<Option<EcRepairShardReadResponse>, EcRepairError> {
        let key = shard_object_key(
            &request.object,
            request.operation_id,
            request.chunk_index,
            request.slot_index,
        );
        let operation_id = derive_storage_operation_id(
            request.operation_id,
            b"ec-repair-shard-read",
            request.chunk_index,
            request.slot_index,
            EcPlacementState::EcReadVerified,
        );
        let read = read_hidden_object_from_node(self, request.node_id, key, operation_id)
            .await
            .map_err(|error| match error {
                EcReadError::Unavailable { reason } => EcRepairError::SourceUnavailable { reason },
                other => ec_read_to_repair_error(other),
            })?;
        Ok(read.map(|read| EcRepairShardReadResponse {
            replica_crc32: read.metadata.get("_s4_ec_crc32").and_then(|raw| raw.parse().ok()),
            data: read.data,
        }))
    }
}

#[async_trait]
impl EcRepairShardWriter for EcClusterReadPorts {
    async fn write_repaired_shard(
        &self,
        request: EcRepairShardWriteRequest,
    ) -> Result<EcRepairShardWriteAck, EcRepairError> {
        let actual_crc32 = crc32fast::hash(&request.data);
        let actual_sha256 = sha256(&request.data);
        if actual_crc32 != request.expected.crc32 || actual_sha256 != request.expected.sha256 {
            return Err(EcRepairError::ShardWrite {
                reason: "repaired shard bytes do not match manifest integrity".to_string(),
            });
        }

        let hidden_key = shard_object_key(
            &request.object,
            request.operation_id,
            request.chunk_index,
            request.expected.slot_index,
        );
        let storage_operation_id = repair_storage_operation_id();
        let hlc = HlcTimestamp::from_record(unix_time_nanos() / 1_000_000, 0, self.local_node_id.0);

        // Do not delete the live key first. Several coordinators can repair the
        // same shard, and a stale delete must not erase a newer repaired copy.
        let write = EcHiddenObjectWrite {
            node_id: request.expected.node_id,
            key: hidden_key,
            data: request.data.to_vec(),
            content_type: EC_SHARD_CONTENT_TYPE,
            metadata: repair_shard_metadata(&request),
            hlc,
            operation_id: storage_operation_id,
        };
        write_hidden_object_to_node(self, write)
            .await
            .map_err(ec_read_to_repair_error)?;

        if request.expected.node_id == self.local_node_id {
            self.put_local_shard_from_head(&request.object, &request).await?;
        }

        Ok(EcRepairShardWriteAck {
            node_id: request.expected.node_id,
            size: request.expected.size,
            crc32: request.expected.crc32,
            sha256: request.expected.sha256,
            durable: true,
            local_index_updated: true,
        })
    }
}

#[async_trait]
impl EcRepairFrontierUpdater for EcClusterReadPorts {
    async fn mark_shard_repaired(
        &self,
        update: EcRepairFrontierUpdate,
    ) -> Result<(), EcRepairError> {
        tracing::debug!(
            repair_id = %update.repair_id,
            bucket = %update.object.bucket,
            key = %update.object.key,
            chunk_index = update.chunk_index,
            slot_index = update.slot_index,
            node_id = %update.node_id,
            "EC repair frontier observed repaired shard"
        );
        Ok(())
    }
}

#[async_trait]
impl s4_cluster::EcTombstoneProbeService for EcOperationsService {
    async fn holds_tombstones(
        &self,
        probes: Vec<s4_cluster::EcTombstoneProbe>,
    ) -> Result<Vec<bool>, ClusterError> {
        let deps = self.read_runtime.get().ok_or_else(|| {
            ClusterError::Config(
                "EC tombstone probe requires the EC runtime ports to be configured".to_string(),
            )
        })?;
        let storage = Arc::clone(&deps.storage);
        let mut held = Vec::with_capacity(probes.len());
        for probe in probes {
            held.push(local_manifest_is_this_tombstone(&storage, &probe).await);
        }
        Ok(held)
    }
}

/// Whether this node's own manifest for the probed object is that exact delete.
///
/// The same comparison [`node_holds_tombstone`] makes remotely, run locally:
/// a tombstone, of this object, from this delete operation, at this HLC.
/// Anything else — including a manifest that cannot be read or decoded — is a
/// no, because the gate asks to be shown the delete rather than to be told
/// nothing stands in its way.
async fn local_manifest_is_this_tombstone(
    storage: &Arc<dyn StorageEngine>,
    probe: &s4_cluster::EcTombstoneProbe,
) -> bool {
    let object = EcObjectVersionKey::new(
        probe.bucket.clone(),
        probe.key.clone(),
        probe.version_id.clone(),
    );
    let key = manifest_object_key(&object);
    let Ok(Some(read)) = read_local_hidden_object(Arc::clone(storage), key).await else {
        return false;
    };
    let Ok(found) = decode_manifest(&read.data) else {
        return false;
    };
    (found.is_deleted || found.state == EcPlacementState::Tombstone)
        && found.object_key() == object
        && found.operation_id.as_u128() == probe.operation_id
        && found.hlc == probe.hlc
}

#[async_trait]
impl EcLocalShardReader for EcClusterReadPorts {
    async fn read_local_shard(
        &self,
        request: EcLocalShardReadRequest,
    ) -> Result<EcLocalShardRead, EcAntiEntropyError> {
        let key = shard_object_key(
            &request.key.object,
            request.record.operation_id,
            request.key.chunk_index,
            request.key.slot_index,
        );
        match self.storage.get_object(SYSTEM_BUCKET, &key).await {
            Ok((data, _)) => Ok(EcLocalShardRead::Present(EcLocalShardReadResponse {
                data: Bytes::from(data),
                blob_header_crc_valid: true,
                blob_header_crc32: Some(request.record.crc32),
            })),
            Err(s4_core::StorageError::ObjectNotFound { .. }) => Ok(EcLocalShardRead::Absent),
            // Bytes were read and refused: they are not the bytes recorded for
            // them. That is a verdict on this shard — rot (decision EC2-185).
            Err(error) if storage_error_is_rot(&error) => {
                tracing::warn!(
                    error = %error,
                    chunk_index = request.key.chunk_index,
                    slot_index = request.key.slot_index,
                    "EC local shard failed its recorded hash during scrub"
                );
                Ok(EcLocalShardRead::Unreadable {
                    reason: error.to_string(),
                })
            }
            // The medium did not answer. Nothing was learned about this shard,
            // so nothing is claimed about it: it is neither condemned nor
            // repaired, and the pass counts the failure against the disk
            // (decision EC2-200).
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    chunk_index = request.key.chunk_index,
                    slot_index = request.key.slot_index,
                    "EC local shard could not be reached during scrub"
                );
                Ok(EcLocalShardRead::MediaUnavailable {
                    reason: error.to_string(),
                })
            }
        }
    }
}

/// Whether a storage error is a verdict on these bytes rather than on the disk.
///
/// Only two are. `ChecksumMismatch` is bytes read and found to differ from the
/// hash recorded for them; `InvalidData` is a blob whose own framing does not
/// hold — a short read against a recorded length, a header that cannot be
/// parsed. Everything else — I/O, the index, a volume that is not there —
/// describes the medium, and a medium is judged by accumulated failures rather
/// than by one object (decision EC2-200).
fn storage_error_is_rot(error: &s4_core::StorageError) -> bool {
    matches!(
        error,
        s4_core::StorageError::ChecksumMismatch { .. } | s4_core::StorageError::InvalidData(_)
    )
}

#[async_trait]
impl EcShardQuarantine for EcClusterReadPorts {
    async fn quarantine_corrupt_shard(
        &self,
        request: EcShardQuarantineRequest,
    ) -> Result<(), EcAntiEntropyError> {
        let key = LocalShardKey {
            object: request.object,
            chunk_index: request.chunk_index,
            slot_index: request.slot_index,
        };
        self.metadata.delete_local_shard(&key).await?;
        Ok(())
    }
}

#[async_trait]
impl EcReconstructionVerifier for EcClusterReadPorts {
    async fn verify_reconstruction_sample(
        &self,
        request: EcReconstructionSampleRequest,
    ) -> Result<EcReconstructionSampleResult, EcAntiEntropyError> {
        // Every shard of the sampled chunks, not just the `k` a serving read
        // needs. This check exists to answer whether the object can still be
        // rebuilt, and a sample that never asks for parity cannot answer it —
        // the scrubber is exactly the side parity verification was handed to
        // (decisions EC2-181 and EC2-184).
        let coordinator = self.ec_reader().with_minimal_read(false);
        let sample_chunks = request.sample_chunks.clone();
        for chunk_index in sample_chunks.iter().copied() {
            let Some((start, end)) = chunk_byte_range(&request.manifest, chunk_index) else {
                return Ok(EcReconstructionSampleResult {
                    status: EcReconstructionSampleStatus::Failed,
                    sampled_chunks: sample_chunks.clone(),
                    reason: Some(format!("manifest has no chunk {chunk_index}")),
                });
            };
            let read = coordinator
                .read(
                    EcReadRequest {
                        object: request.manifest.object_key(),
                        range: Some(EcByteRange::new(start, end)),
                    },
                    self,
                    self,
                    self,
                    &NoopEcReadRepairQueue,
                )
                .await;
            if let Err(error) = read {
                return Ok(EcReconstructionSampleResult {
                    status: EcReconstructionSampleStatus::Failed,
                    sampled_chunks: sample_chunks.clone(),
                    reason: Some(error.to_string()),
                });
            }
        }
        Ok(EcReconstructionSampleResult {
            status: EcReconstructionSampleStatus::Verified,
            sampled_chunks: sample_chunks,
            reason: None,
        })
    }
}

enum ManifestNodeRead {
    Present(Box<EcObjectManifest>),
    Absent,
}

struct HiddenObjectRead {
    data: Bytes,
    metadata: std::collections::HashMap<String, String>,
}

#[derive(Clone)]
struct EcHiddenObjectWrite {
    node_id: NodeId,
    key: String,
    data: Vec<u8>,
    content_type: &'static str,
    metadata: HashMap<String, String>,
    hlc: HlcTimestamp,
    operation_id: u128,
}

type EcReadRangeSlice = (Bytes, Option<(u64, u64, u64)>);

async fn read_manifest_from_node(
    ports: &EcClusterReadPorts,
    node_id: NodeId,
    key: String,
    object: EcObjectVersionKey,
    operation_id: u128,
) -> Result<ManifestNodeRead, EcReadError> {
    let Some(read) = read_hidden_object_from_node(ports, node_id, key, operation_id).await? else {
        return Ok(ManifestNodeRead::Absent);
    };
    let manifest = decode_manifest(&read.data).map_err(|error| EcReadError::InvalidManifest {
        reason: format!("manifest deserialization failed: {error}"),
    })?;
    if manifest.object_key() != object {
        return Err(EcReadError::ManifestMismatch {
            reason: format!(
                "manifest object mismatch: read {}/{} while resolving {}/{}",
                manifest.bucket, manifest.key, object.bucket, object.key
            ),
        });
    }
    // Against the set the manifest itself names, not against whichever set the
    // handle is bound to. A pool of several sets answers for all of them, and
    // validating a set-2 manifest against set 1 would reject a healthy object.
    let erasure_set = ports.set_for_manifest_id(manifest.erasure_set_id)?;
    manifest.validate_against_erasure_set(erasure_set)?;
    Ok(ManifestNodeRead::Present(Box::new(manifest)))
}

/// Which owners have observed each tombstone, asked once before a sweep.
///
/// The gate the GC coordinator applies is per object and it applies it inside
/// its own scan, so the answers are gathered beforehand rather than while the
/// scan holds the page it is walking. One metadata read per owner per
/// tombstone, taken once for the cycle.
#[derive(Debug, Default)]
struct EcTombstoneOwnerPoll {
    confirmed_by_all: HashMap<EcObjectVersionKey, bool>,
}

#[async_trait]
impl EcTombstonePropagation for EcTombstoneOwnerPoll {
    async fn tombstone_propagated(&self, tombstone: &EcObjectManifest) -> Result<bool, EcGcError> {
        // Absent means "this sweep did not ask about it", which is never a
        // reason to release shards.
        Ok(self.confirmed_by_all.get(&tombstone.object_key()).copied().unwrap_or(false))
    }
}

/// Where one tombstone scan begins.
#[derive(Debug, Clone, PartialEq, Eq)]
enum TombstoneScanStart {
    /// From an explicit manual continuation; this never changes durable state.
    Cursor(Option<Vec<u8>>),
    /// From where the last background cycle stopped, so that a cycle costs a
    /// slice of the keyspace and successive cycles cover all of it.
    WhereTheLastCycleStopped,
}

/// What one walk over the manifests turned up.
#[derive(Debug, Default)]
struct EcTombstoneScan {
    /// Tombstones found, capped at what the caller asked for.
    tombstones: Vec<EcObjectManifest>,
    /// Manifests looked at to find them.
    manifests_examined: usize,
    /// Whether this walk reached the end of the keyspace.
    round_completed: bool,
    /// Last metadata key actually examined, when more of the round remains.
    next_cursor: Option<Vec<u8>>,
}

/// How many replicas have to hold a small object's placement record.
///
/// A simple majority of the nodes the record names, so it survives the loss of
/// the node that wrote it — the one failure this record exists to withstand.
fn small_placement_quorum(replicas: usize) -> usize {
    replicas / 2 + 1
}

/// What one pass over the placement hints did.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct EcSmallPlacementHintSweep {
    /// Hints looked at.
    examined: usize,
    /// Hints settled: a majority of the replicas now holds the record.
    delivered: usize,
    /// Hints dropped because the object they describe is gone.
    dropped: usize,
    /// Hints left for a later sweep, undecided or still short of a majority.
    deferred: usize,
}

/// What one tombstone sweep did, across every erasure set it touched.
#[derive(Debug, Default)]
struct EcTombstoneSweep {
    /// Tombstones every owner of their set was confirmed to hold.
    confirmed_by_all: usize,
    /// Owners handed a delete they had never received.
    repaired_owners: usize,
    /// Erasure sets this sweep had tombstones for.
    sets_swept: usize,
    /// Tombstones belonging to no erasure set this pool still has.
    unclaimed: usize,
    /// The per-set reports, added together.
    report: EcTombstoneGcReport,
}

/// Add one set's report into the running total.
fn add_tombstone_gc_report(total: &mut EcTombstoneGcReport, report: &EcTombstoneGcReport) {
    total.tombstones_scanned = total.tombstones_scanned.saturating_add(report.tombstones_scanned);
    total.tombstones_retained =
        total.tombstones_retained.saturating_add(report.tombstones_retained);
    total.tombstones_purged = total.tombstones_purged.saturating_add(report.tombstones_purged);
    total.tombstones_shards_released = total
        .tombstones_shards_released
        .saturating_add(report.tombstones_shards_released);
    total.tombstones_retired = total.tombstones_retired.saturating_add(report.tombstones_retired);
    total.shards_scanned = total.shards_scanned.saturating_add(report.shards_scanned);
    total.shards_purged = total.shards_purged.saturating_add(report.shards_purged);
    total.shard_scan_capped = total.shard_scan_capped.saturating_add(report.shard_scan_capped);
}

/// What one poll of an erasure set's owners established.
struct EcTombstonePollResult {
    propagation: EcTombstoneOwnerPoll,
    frontier: RepairFrontier,
    confirmed_by_all: usize,
    /// Owners that were handed a delete they had never received.
    repaired: usize,
}

/// Ask every owner of a set whether it holds each of these tombstones, and
/// give the delete to the ones that do not.
///
/// Produces both gates the GC sweep needs, from one set of answers. The
/// propagation gate is per object: every owner has this delete. The frontier is
/// per owner: the point up to which that owner has every delete this sweep
/// knows of, which is what makes it a confirmed cut rather than a watermark —
/// an owner that missed an older delete cannot be carried past it by a newer
/// one it happens to hold.
///
/// An owner that was down when the delete was committed never received it, and
/// nothing else would ever hand it over: the commit waits for a quorum and
/// leaves the rest behind. Such an owner holds shards of an object the pool
/// considers gone and a manifest that still calls it live, which is the zombie
/// this whole path exists to prevent — so the sweep writes the tombstone there
/// before it judges anything. Safe because the tombstone is only ever pushed
/// after the pool's own quorum read has confirmed it, and because the write
/// carries the tombstone's HLC: a node that has meanwhile taken a newer
/// generation of the object keeps it.
async fn poll_tombstone_owners(
    ports: &EcClusterReadPorts,
    owners: &[NodeId],
    tombstones: &[EcObjectManifest],
) -> EcTombstonePollResult {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut ordered: Vec<&EcObjectManifest> = tombstones.iter().collect();
    ordered.sort_by(|left, right| {
        left.hlc
            .cmp(&right.hlc)
            .then_with(|| left.bucket.cmp(&right.bucket))
            .then_with(|| left.key.cmp(&right.key))
            .then_with(|| left.version_id.cmp(&right.version_id))
    });

    let mut confirmed_by_all = HashMap::new();
    let mut cuts: HashMap<NodeId, HlcTimestamp> = HashMap::new();
    let mut stalled: HashSet<NodeId> = HashSet::new();
    let mut repaired = 0usize;

    // Every owner is asked once, about every delete this sweep is about, before
    // any of them is judged. The answers are then read per delete below.
    let probe: Arc<dyn EcTombstoneOwnerProbe> = Arc::new(EcClusterTombstoneProbe {
        ports: ports.clone(),
    });
    let answers = poll_tombstones_by_owner_until(probe, owners, &ordered, deadline).await;

    for (index, tombstone) in ordered.iter().copied().enumerate() {
        let mut seen_by = owners
            .iter()
            .copied()
            .filter(|owner| answers.get(owner).is_some_and(|answer| answer.held.contains(&index)))
            .collect::<HashSet<_>>();
        let missing = owners
            .iter()
            .copied()
            .filter(|owner| {
                answers.get(owner).is_some_and(|answer| answer.missing.contains(&index))
            })
            .collect::<Vec<_>>();
        if !missing.is_empty()
            && tokio::time::Instant::now() < deadline
            && tokio::time::timeout_at(deadline, pool_confirms_tombstone(ports, tombstone))
                .await
                .unwrap_or(false)
        {
            for owner in missing {
                if tokio::time::Instant::now() >= deadline {
                    break;
                }
                match tokio::time::timeout_at(
                    deadline,
                    push_tombstone_to_node(ports, owner, tombstone),
                )
                .await
                {
                    Ok(Ok(())) => {
                        repaired = repaired.saturating_add(1);
                        seen_by.insert(owner);
                        tracing::info!(
                            node_id = %owner,
                            bucket = %tombstone.bucket,
                            key = %tombstone.key,
                            "EC tombstone handed to an owner that had not received the delete"
                        );
                    }
                    Ok(Err(error)) => {
                        tracing::debug!(
                            node_id = %owner,
                            bucket = %tombstone.bucket,
                            key = %tombstone.key,
                            %error,
                            "EC tombstone could not be handed to a lagging owner"
                        );
                    }
                    Err(_) => break,
                }
            }
        }

        for owner in owners {
            if seen_by.contains(owner) {
                if !stalled.contains(owner) {
                    cuts.insert(*owner, tombstone.hlc);
                }
            } else {
                stalled.insert(*owner);
            }
        }
        let everywhere = owners.iter().all(|owner| seen_by.contains(owner));
        confirmed_by_all.insert(tombstone.object_key(), everywhere);
    }

    let confirmed = confirmed_by_all.values().filter(|value| **value).count();
    let mut frontier = RepairFrontier::new();
    let range = KeyRange::full();
    for (owner, cut) in cuts {
        frontier.advance(&range, owner, cut);
    }
    EcTombstonePollResult {
        propagation: EcTombstoneOwnerPoll { confirmed_by_all },
        frontier,
        confirmed_by_all: confirmed,
        repaired,
    }
}

/// Which of `owners` hold each of these tombstones, asked once per owner.
///
/// One call per owner carrying every delete this sweep is about, instead of one
/// call per delete per owner: on a cycle of a thousand tombstones over six
/// owners that is six requests rather than six thousand, and the gate is
/// unchanged (decision EC2-202).
///
/// Only an explicit Unimplemented permits the old call. All owners share one
/// ten-second deadline, including the batch request and compatibility calls;
/// unlike blob transfers this maintenance probe must not occupy the integrity
/// worker for minutes. Anything not confirmed by then stays unconfirmed.
#[derive(Debug, Default)]
struct EcOwnerTombstoneAnswers {
    held: HashSet<usize>,
    missing: HashSet<usize>,
}

#[cfg(test)]
async fn poll_tombstones_by_owner(
    probe: Arc<dyn EcTombstoneOwnerProbe>,
    owners: &[NodeId],
    tombstones: &[&EcObjectManifest],
) -> HashMap<NodeId, EcOwnerTombstoneAnswers> {
    poll_tombstones_by_owner_until(
        probe,
        owners,
        tombstones,
        tokio::time::Instant::now() + Duration::from_secs(10),
    )
    .await
}

async fn poll_tombstones_by_owner_until(
    probe: Arc<dyn EcTombstoneOwnerProbe>,
    owners: &[NodeId],
    tombstones: &[&EcObjectManifest],
    deadline: tokio::time::Instant,
) -> HashMap<NodeId, EcOwnerTombstoneAnswers> {
    let owned = tombstones.iter().map(|tombstone| (*tombstone).clone()).collect::<Vec<_>>();
    let mut asked = JoinSet::new();
    for owner in owners.iter().copied() {
        let probe = Arc::clone(&probe);
        let tombstones = owned.clone();
        asked.spawn(async move {
            match tokio::time::timeout_at(deadline, probe.probe_batch(owner, &tombstones)).await {
                Ok(Ok(held)) if held.len() == tombstones.len() => {
                    let mut answer = EcOwnerTombstoneAnswers::default();
                    for (index, held) in held.into_iter().enumerate() {
                        if held { answer.held.insert(index); }
                        else { answer.missing.insert(index); }
                    }
                    (owner, answer)
                }
                Ok(Err(ClusterError::RpcUnimplemented(error))) => {
                    tracing::debug!(
                        node_id = %owner,
                        %error,
                        "peer does not implement the tombstone batch probe; using bounded legacy calls"
                    );
                    let mut answer = EcOwnerTombstoneAnswers::default();
                    for (index, tombstone) in tombstones.iter().enumerate() {
                        if tokio::time::Instant::now() >= deadline {
                            break;
                        }
                        match tokio::time::timeout_at(deadline, probe.probe_one(owner, tombstone)).await {
                            Ok(Some(true)) => { answer.held.insert(index); }
                            Ok(Some(false)) => { answer.missing.insert(index); }
                            Ok(None) | Err(_) => break,
                        }
                    }
                    (owner, answer)
                }
                Ok(Err(error)) => {
                    tracing::debug!(node_id = %owner, %error,
                        "EC tombstone owner could not be confirmed; no legacy retry");
                    (owner, EcOwnerTombstoneAnswers::default())
                }
                Err(_) => {
                    tracing::debug!(node_id = %owner, "EC tombstone probe deadline elapsed");
                    (owner, EcOwnerTombstoneAnswers::default())
                }
                Ok(Ok(_)) => (owner, EcOwnerTombstoneAnswers::default()),
            }
        });
    }

    let mut by_owner = HashMap::new();
    while let Some(result) = asked.join_next().await {
        if let Ok((owner, seen)) = result {
            if !seen.held.is_empty() || !seen.missing.is_empty() {
                by_owner.insert(owner, seen);
            }
        }
    }
    by_owner
}

/// Asks one owner whether it holds each of a list of deletes.
///
/// Behind a trait for one reason: the fallback below it runs exactly once in
/// the life of a cluster — while it is being upgraded — and a branch that can
/// only be reached during an upgrade is a branch nobody ever sees fail. Here it
/// is reachable in a test.
#[async_trait]
trait EcTombstoneOwnerProbe: Send + Sync + 'static {
    /// One request for the whole list, of an owner that knows the call.
    async fn probe_batch(
        &self,
        owner: NodeId,
        tombstones: &[EcObjectManifest],
    ) -> Result<Vec<bool>, ClusterError>;

    /// One request per delete, for an owner that does not.
    /// None means unknown and must not cause a handoff to this owner.
    async fn probe_one(&self, owner: NodeId, tombstone: &EcObjectManifest) -> Option<bool>;
}

/// The probe as the cluster answers it.
struct EcClusterTombstoneProbe {
    ports: EcClusterReadPorts,
}

#[async_trait]
impl EcTombstoneOwnerProbe for EcClusterTombstoneProbe {
    async fn probe_batch(
        &self,
        owner: NodeId,
        tombstones: &[EcObjectManifest],
    ) -> Result<Vec<bool>, ClusterError> {
        let probes = tombstones
            .iter()
            .map(|tombstone| s4_cluster::EcTombstoneProbe {
                bucket: tombstone.bucket.clone(),
                key: tombstone.key.clone(),
                version_id: tombstone.version_id.clone(),
                operation_id: tombstone.operation_id.as_u128(),
                hlc: tombstone.hlc,
            })
            .collect::<Vec<_>>();
        self.ports
            .rpc_client
            .send_probe_ec_tombstones(
                owner,
                self.ports.local_node_id,
                self.ports.topology_epoch,
                &probes,
            )
            .await
    }

    async fn probe_one(&self, owner: NodeId, tombstone: &EcObjectManifest) -> Option<bool> {
        node_holds_tombstone(&self.ports, owner, tombstone).await
    }
}

/// Whether the pool itself agrees this object is deleted by this tombstone.
///
/// Asked before the tombstone is handed to anybody, so a sweep can never
/// spread a delete the rest of the pool does not stand behind.
async fn pool_confirms_tombstone(ports: &EcClusterReadPorts, tombstone: &EcObjectManifest) -> bool {
    match <EcClusterReadPorts as EcReadMetadataCoordinator>::quorum_read_manifest(
        ports,
        &tombstone.object_key(),
    )
    .await
    {
        Ok(EcManifestReadResult::Tombstone(read)) => {
            read.operation_id == tombstone.operation_id && read.hlc == tombstone.hlc
        }
        _ => false,
    }
}

/// Write the tombstone onto one owner that does not have it.
async fn push_tombstone_to_node(
    ports: &EcClusterReadPorts,
    node_id: NodeId,
    tombstone: &EcObjectManifest,
) -> Result<(), EcReadError> {
    let data = encode_manifest(tombstone).map_err(|error| EcReadError::InvalidManifest {
        reason: format!("manifest serialization failed: {error}"),
    })?;
    write_hidden_object_to_node(
        ports,
        EcHiddenObjectWrite {
            node_id,
            key: manifest_object_key(&tombstone.object_key()),
            data,
            content_type: EC_MANIFEST_CONTENT_TYPE,
            metadata: manifest_metadata(tombstone)?,
            hlc: manifest_storage_hlc(tombstone.hlc, tombstone.state),
            operation_id: derive_storage_operation_id(
                tombstone.operation_id,
                b"ec-tombstone-handoff",
                0,
                0,
                tombstone.state,
            ),
        },
    )
    .await
}

/// Write one small object's placement to each of its replica nodes.
///
/// Returns how many of them confirmed it. Addressed to the replicas rather
/// than to the erasure set: these are the nodes that hold the bytes, and the
/// record only has to outlive the node that wrote it.
async fn write_replicated_small_placement(
    ports: &EcClusterReadPorts,
    object: &EcObjectVersionKey,
    placement: &ReplicatedSmallObjectPlacement,
) -> usize {
    let Ok(data) = encode_replicated_small_placement(placement) else {
        return 0;
    };
    let key = replicated_small_object_key(object);
    let operation_id = derive_storage_operation_id(
        deterministic_object_uuid(object),
        b"ec-small-placement-write",
        0,
        0,
        EcPlacementState::EcTranscodeQueued,
    );
    let mut writes = JoinSet::new();
    for node_id in placement.replica_nodes.clone() {
        let ports = ports.clone();
        let write = EcHiddenObjectWrite {
            node_id,
            key: key.clone(),
            data: data.clone(),
            content_type: EC_SMALL_PLACEMENT_CONTENT_TYPE,
            metadata: HashMap::from([("_s4_ec_kind".to_string(), "replicated_small".to_string())]),
            hlc: placement.hlc,
            operation_id,
        };
        writes.spawn(async move { write_hidden_object_to_node(&ports, write).await });
    }
    let mut acks = 0usize;
    while let Some(result) = writes.join_next().await {
        if let Ok(Ok(())) = result {
            acks = acks.saturating_add(1);
        }
    }
    acks
}

/// Take a small object's placement back after its delete was confirmed.
///
/// Best effort: the tombstone manifest is what makes the object gone, and it
/// outranks anything a leftover placement record could say. Removing it keeps
/// the pool from carrying a record for an object that no longer exists.
async fn retract_replicated_small_placement(
    ports: &EcClusterReadPorts,
    object: &EcObjectVersionKey,
    hlc: HlcTimestamp,
) {
    let key = replicated_small_object_key(object);
    let operation_id = derive_storage_operation_id(
        deterministic_object_uuid(object),
        b"ec-small-placement-retract",
        0,
        0,
        EcPlacementState::Tombstone,
    );
    let mut deletes = JoinSet::new();
    for node_id in ports.pool_nodes() {
        let ports = ports.clone();
        let key = key.clone();
        deletes.spawn(async move {
            delete_hidden_object_from_node(&ports, node_id, key, hlc, operation_id).await
        });
    }
    while let Some(result) = deletes.join_next().await {
        if let Ok(Err(error)) = result {
            tracing::debug!(
                bucket = %object.bucket,
                key = %object.key,
                %error,
                "EC replicated-small placement was not retracted from a node"
            );
        }
    }
}

/// Read one small object's placement from the pool, if it has one.
///
/// Asked of every node of the pool, because the reader does not know which
/// three hold the replicas until it has the record. The first decodable answer
/// settles it: the record is written once and never rewritten, so two copies
/// cannot disagree.
async fn read_replicated_small_placement(
    ports: &EcClusterReadPorts,
    object: &EcObjectVersionKey,
) -> Result<Option<ReplicatedSmallObjectPlacement>, EcReadError> {
    // The record is written to the nodes that hold the object, and which nodes
    // those are is a computation over the ring rather than a question anybody
    // has to be asked. So they are asked first, and a pool of any size costs
    // RF requests instead of N (decision EC2-201).
    let replicas = ports
        .read_coordinator
        .replica_nodes(&object.bucket, &object.key)
        .unwrap_or_default();
    Ok(
        resolve_small_placement_with(replicas, ports.pool_nodes(), |nodes| async move {
            read_small_placement_from_nodes(ports, object, &nodes).await
        })
        .await,
    )
}

/// Resolve from owners first, then the remaining pool, using the same read operation.
async fn resolve_small_placement_with<F, Fut>(
    replicas: Vec<NodeId>,
    pool_nodes: Vec<NodeId>,
    mut read: F,
) -> Option<ReplicatedSmallObjectPlacement>
where
    F: FnMut(Vec<NodeId>) -> Fut,
    Fut: std::future::Future<Output = Option<ReplicatedSmallObjectPlacement>>,
{
    if let Some(placement) = read(replicas.clone()).await {
        ec_metrics::record_small_placement_resolve(ec_metrics::SmallPlacementSource::Replicas);
        return Some(placement);
    }

    // Nothing from the owners. Either the object has no placement record at
    // all, or the write that made it never reached them — a node that was down
    // then, a majority that was not met. The fan-out is the fallback for the
    // second case, and it is counted so that a pool living on it is visible
    // rather than merely slow.
    let rest = placement_fallback_nodes(&replicas, pool_nodes);
    if !rest.is_empty() {
        ec_metrics::record_small_placement_fallback_attempt();
    }
    match read(rest).await {
        Some(placement) => {
            tracing::debug!(
                "EC replicated-small placement was not on the object's replicas; the pool had it"
            );
            ec_metrics::record_small_placement_resolve(
                ec_metrics::SmallPlacementSource::PoolFanout,
            );
            Some(placement)
        }
        None => {
            ec_metrics::record_small_placement_resolve(ec_metrics::SmallPlacementSource::Absent);
            None
        }
    }
}

/// The nodes a placement resolve falls back to, once the owners had nothing.
///
/// The rest of the pool, with the owners removed: they have already answered,
/// and asking them twice would make the fallback cost more than the fan-out it
/// replaced. Kept apart from the I/O so that "the owners are asked first, and
/// nobody is asked twice" is a property with a test rather than a comment
/// (decision EC2-201).
fn placement_fallback_nodes(replicas: &[NodeId], pool_nodes: Vec<NodeId>) -> Vec<NodeId> {
    pool_nodes.into_iter().filter(|node_id| !replicas.contains(node_id)).collect()
}

/// Ask `nodes` for this object's placement record, first decodable answer wins.
///
/// The record is written once and never rewritten, so two copies cannot
/// disagree and there is nothing to reconcile between them.
async fn read_small_placement_from_nodes(
    ports: &EcClusterReadPorts,
    object: &EcObjectVersionKey,
    nodes: &[NodeId],
) -> Option<ReplicatedSmallObjectPlacement> {
    if nodes.is_empty() {
        return None;
    }
    let key = replicated_small_object_key(object);
    let operation_id = derive_storage_operation_id(
        deterministic_object_uuid(object),
        b"ec-small-placement-read",
        0,
        0,
        EcPlacementState::EcTranscodeQueued,
    );
    let mut reads = JoinSet::new();
    for node_id in nodes.iter().copied() {
        let ports = ports.clone();
        let key = key.clone();
        reads.spawn(async move {
            read_hidden_object_from_node(&ports, node_id, key, operation_id).await
        });
    }
    let mut found = None;
    while let Some(result) = reads.join_next().await {
        if let Ok(Ok(Some(read))) = result {
            if let Ok(placement) = decode_replicated_small_placement(&read.data) {
                found = Some(placement);
                break;
            }
        }
    }
    reads.shutdown().await;
    found
}

/// Whether `node_id` holds this exact tombstone right now.
///
/// Matched by identity — same object, same delete operation, same HLC — and
/// deliberately not validated against the erasure set the object used to live
/// in. A tombstone carries no chunks and no placement to check, and a set whose
/// topology epoch has moved on since the delete would reject every tombstone
/// written before it.
///
/// Unknown differs from an explicit negative answer: only the latter can
/// trigger a handoff, and neither permits shard release.
async fn node_holds_tombstone(
    ports: &EcClusterReadPorts,
    node_id: NodeId,
    tombstone: &EcObjectManifest,
) -> Option<bool> {
    let object = tombstone.object_key();
    let key = manifest_object_key(&object);
    let operation_id = derive_storage_operation_id(
        deterministic_object_uuid(&object),
        b"ec-manifest-read",
        0,
        0,
        EcPlacementState::EcManifestCommitted,
    );
    let read = match read_hidden_object_from_node(ports, node_id, key, operation_id).await {
        Ok(Some(read)) => read,
        Ok(None) => return Some(false),
        Err(error) => {
            tracing::debug!(
                node_id = %node_id,
                bucket = %object.bucket,
                key = %object.key,
                %error,
                "EC tombstone propagation could not be confirmed by an owner"
            );
            return None;
        }
    };
    let Ok(found) = decode_manifest(&read.data) else {
        return None;
    };
    Some(
        (found.is_deleted || found.state == EcPlacementState::Tombstone)
            && found.object_key() == object
            && found.operation_id == tombstone.operation_id
            && found.hlc == tombstone.hlc,
    )
}

/// One shard a rebuild inventory has to ask about.
#[derive(Debug, Clone)]
struct EcShardProbe {
    object: EcObjectVersionKey,
    operation_id: uuid::Uuid,
    chunk_index: u32,
    slot_index: u8,
}

/// Whether `node_id` holds the shard `probe` names.
///
/// Presence only, never integrity. A shard whose bytes are there but wrong is
/// what `verify` and the scrubber answer for; reading every shard of a pool to
/// answer "is the rebuild finished" would cost a full pool read each time the
/// question is asked, and the question gets asked in a loop.
async fn shard_presence(
    node_id: NodeId,
    local_node_id: NodeId,
    metadata: &EcMetadataStore,
    ports: Option<&EcClusterReadPorts>,
    probe: &EcShardProbe,
) -> EcShardPresence {
    let hidden_key = shard_object_key(
        &probe.object,
        probe.operation_id,
        probe.chunk_index,
        probe.slot_index,
    );

    if node_id == local_node_id {
        // With storage wired the bytes decide, because the local index can lag
        // behind a shard a peer's repair has just written here — and a rebuild
        // held open by a stale index entry would never finish. Without it the
        // index is all this node has, and it is the truth it acts on anyway.
        return match ports {
            Some(ports) => match ports.storage.head_object(SYSTEM_BUCKET, &hidden_key).await {
                Ok(_) => EcShardPresence::Present,
                Err(s4_core::StorageError::ObjectNotFound { .. }) => EcShardPresence::Missing,
                Err(error) => {
                    EcShardPresence::unknown(format!("local shard could not be read: {error}"))
                }
            },
            None => {
                let local_key = LocalShardKey {
                    object: probe.object.clone(),
                    chunk_index: probe.chunk_index,
                    slot_index: probe.slot_index,
                };
                match metadata.get_local_shard(&local_key).await {
                    Ok(Some(_)) => EcShardPresence::Present,
                    Ok(None) => EcShardPresence::Missing,
                    Err(error) => EcShardPresence::unknown(format!(
                        "local shard record could not be read: {error}"
                    )),
                }
            }
        };
    }

    let Some(ports) = ports else {
        return EcShardPresence::unknown(
            "this node has no cluster read path configured, so it cannot ask the shard's owner",
        );
    };
    match head_hidden_object_on_node(ports, node_id, hidden_key).await {
        Ok(true) => EcShardPresence::Present,
        Ok(false) => EcShardPresence::Missing,
        Err(error) => EcShardPresence::unknown(error.to_string()),
    }
}

/// Ask `node_id` whether it holds the hidden object `key`, without reading it.
///
/// A head rather than a read on purpose: an inventory asks this question once
/// per shard of a whole pool, and pulling every shard across the network to
/// learn that it exists would make the answer cost more than the rebuild.
async fn head_hidden_object_on_node(
    ports: &EcClusterReadPorts,
    node_id: NodeId,
    key: String,
) -> Result<bool, EcReadError> {
    let request = proto::HeadObjectRequest {
        bucket: SYSTEM_BUCKET.to_string(),
        key,
        version_id: None,
        topology_epoch: ports.topology_epoch,
    };
    let response = ports.rpc_client.send_head(node_id, request).await.map_err(|error| {
        EcReadError::Unavailable {
            reason: format!("EC shard presence check on {node_id} failed: {error}"),
        }
    })?;
    if response.epoch_mismatch {
        return Err(EcReadError::Unavailable {
            reason: format!("EC shard presence check on {node_id} rejected the topology epoch"),
        });
    }
    // An owner that answered "no" said the shard is gone. One that answered
    // with an error said nothing at all, and the two must not merge: the first
    // is work for repair, the second is a node to look at.
    if !response.exists && !response.error.is_empty() {
        return Err(EcReadError::Unavailable {
            reason: format!(
                "EC shard presence check on {node_id} failed: {}",
                response.error
            ),
        });
    }
    Ok(response.exists)
}

/// Fold the fields of `extra` into `value`, both being JSON objects.
///
/// Used to add fields beside those an admin response already had rather than
/// nesting a second object under it: callers written before this plan keep
/// reading the fields they always read.
fn merge_admin_objects(value: &mut Value, extra: Value) {
    let (Some(target), Some(extra)) = (value.as_object_mut(), extra.as_object()) else {
        return;
    };
    for (name, field) in extra {
        target.insert(name.clone(), field.clone());
    }
}

async fn read_hidden_object_from_node(
    ports: &EcClusterReadPorts,
    node_id: NodeId,
    key: String,
    operation_id: u128,
) -> Result<Option<HiddenObjectRead>, EcReadError> {
    if node_id == ports.local_node_id {
        return read_local_hidden_object(Arc::clone(&ports.storage), key).await;
    }

    read_remote_hidden_object(
        Arc::clone(&ports.rpc_client),
        ports.local_node_id,
        node_id,
        ports.topology_epoch,
        key,
        operation_id,
    )
    .await
}

async fn read_local_hidden_object(
    storage: Arc<dyn StorageEngine>,
    key: String,
) -> Result<Option<HiddenObjectRead>, EcReadError> {
    match storage.get_object(SYSTEM_BUCKET, &key).await {
        Ok((data, record)) => {
            let actual_hash = sha256(&data);
            if record.content_hash != actual_hash {
                return Err(EcReadError::ShardIo {
                    reason: format!("local EC hidden object {key} failed SHA-256 verification"),
                });
            }
            Ok(Some(HiddenObjectRead {
                data: Bytes::from(data),
                metadata: record.metadata,
            }))
        }
        Err(s4_core::StorageError::ObjectNotFound { .. }) => Ok(None),
        Err(error) => Err(EcReadError::ShardIo {
            reason: format!("local EC hidden object read failed: {error}"),
        }),
    }
}

async fn read_remote_hidden_object(
    rpc_client: Arc<NodeClient>,
    local_node_id: NodeId,
    node_id: NodeId,
    topology_epoch: u64,
    key: String,
    operation_id: u128,
) -> Result<Option<HiddenObjectRead>, EcReadError> {
    let request = proto::ReadBlobRequest {
        bucket: SYSTEM_BUCKET.to_string(),
        key,
        version_id: None,
        coordinator_id: local_node_id.to_bytes().to_vec(),
        operation_id: operation_id.to_be_bytes().to_vec(),
        topology_epoch,
        range_start: None,
        range_end: None,
    };
    // A failed call means the node could not be asked at all, which is not
    // the same as a node that answered with an error: repair defers a shard
    // it cannot rebuild for the first reason and closes it for the second.
    let response =
        rpc_client
            .send_read(node_id, request)
            .await
            .map_err(|error| EcReadError::Unavailable {
                reason: format!("remote EC hidden object read from {node_id} failed: {error}"),
            })?;
    if response.epoch_mismatch {
        return Err(EcReadError::Unavailable {
            reason: format!("remote EC hidden object read from {node_id} rejected topology epoch"),
        });
    }
    if !response.success {
        if is_not_found_message(&response.error) {
            return Ok(None);
        }
        return Err(EcReadError::ShardIo {
            reason: format!(
                "remote EC hidden object read from {node_id} failed: {}",
                response.error
            ),
        });
    }
    let expected_hash: [u8; 32] =
        response.content_hash.as_slice().try_into().map_err(|_| EcReadError::ShardIo {
            reason: format!("remote EC hidden object read from {node_id} returned invalid SHA-256"),
        })?;
    let actual_hash = sha256(&response.data);
    if expected_hash != actual_hash {
        return Err(EcReadError::ShardIo {
            reason: format!(
                "remote EC hidden object read from {node_id} returned mismatched SHA-256"
            ),
        });
    }

    Ok(Some(HiddenObjectRead {
        data: Bytes::from(response.data),
        metadata: response.metadata,
    }))
}

async fn write_hidden_object_to_node(
    ports: &EcClusterReadPorts,
    write: EcHiddenObjectWrite,
) -> Result<(), EcReadError> {
    if write.node_id == ports.local_node_id {
        return write_local_hidden_object(Arc::clone(&ports.storage), write).await;
    }

    write_remote_hidden_object(
        Arc::clone(&ports.rpc_client),
        ports.local_node_id,
        ports.topology_epoch,
        write,
    )
    .await
}

async fn write_local_hidden_object(
    storage: Arc<dyn StorageEngine>,
    write: EcHiddenObjectWrite,
) -> Result<(), EcReadError> {
    let mut metadata = write.metadata;
    add_hlc_metadata(&mut metadata, write.hlc, write.operation_id);
    storage
        .put_object(
            SYSTEM_BUCKET,
            &write.key,
            &write.data,
            write.content_type,
            &metadata,
        )
        .await
        .map(|_| ())
        .map_err(|error| EcReadError::ShardIo {
            reason: format!("local EC hidden object write failed: {error}"),
        })
}

async fn write_remote_hidden_object(
    rpc_client: Arc<NodeClient>,
    local_node_id: NodeId,
    topology_epoch: u64,
    write: EcHiddenObjectWrite,
) -> Result<(), EcReadError> {
    let node_id = write.node_id;
    let request = proto::WriteBlobRequest {
        bucket: SYSTEM_BUCKET.to_string(),
        key: write.key,
        content_hash: sha256(&write.data).to_vec(),
        data: write.data,
        content_type: write.content_type.to_string(),
        metadata: write.metadata,
        hlc: Some(write.hlc.to_proto()),
        version_id: None,
        coordinator_id: local_node_id.to_bytes().to_vec(),
        operation_id: write.operation_id.to_be_bytes().to_vec(),
        topology_epoch,
    };
    let response =
        rpc_client
            .send_write(node_id, request)
            .await
            .map_err(|error| EcReadError::ShardIo {
                reason: format!("remote EC hidden object write to {node_id} failed: {error}"),
            })?;
    if response.epoch_mismatch {
        return Err(EcReadError::Unavailable {
            reason: format!("remote EC hidden object write to {node_id} rejected topology epoch"),
        });
    }
    if !response.success {
        return Err(EcReadError::ShardIo {
            reason: format!(
                "remote EC hidden object write to {node_id} failed: {}",
                response.error
            ),
        });
    }
    Ok(())
}

async fn delete_hidden_object_from_node(
    ports: &EcClusterReadPorts,
    node_id: NodeId,
    key: String,
    hlc: HlcTimestamp,
    operation_id: u128,
) -> Result<(), EcReadError> {
    if node_id == ports.local_node_id {
        return delete_local_hidden_object(Arc::clone(&ports.storage), key).await;
    }

    delete_remote_hidden_object(
        Arc::clone(&ports.rpc_client),
        ports.local_node_id,
        node_id,
        ports.topology_epoch,
        key,
        hlc,
        operation_id,
    )
    .await
}

async fn delete_local_hidden_object(
    storage: Arc<dyn StorageEngine>,
    key: String,
) -> Result<(), EcReadError> {
    match storage.delete_object(SYSTEM_BUCKET, &key).await {
        Ok(()) | Err(s4_core::StorageError::ObjectNotFound { .. }) => Ok(()),
        Err(error) => Err(EcReadError::ShardIo {
            reason: format!("local EC hidden object delete failed: {error}"),
        }),
    }
}

async fn delete_remote_hidden_object(
    rpc_client: Arc<NodeClient>,
    local_node_id: NodeId,
    node_id: NodeId,
    topology_epoch: u64,
    key: String,
    hlc: HlcTimestamp,
    operation_id: u128,
) -> Result<(), EcReadError> {
    let request = proto::DeleteBlobRequest {
        bucket: SYSTEM_BUCKET.to_string(),
        key,
        hlc: Some(hlc.to_proto()),
        coordinator_id: local_node_id.to_bytes().to_vec(),
        operation_id: operation_id.to_be_bytes().to_vec(),
        topology_epoch,
        version_id: None,
        expected_source_hlc: None,
    };
    let response =
        rpc_client
            .send_delete(node_id, request)
            .await
            .map_err(|error| EcReadError::ShardIo {
                reason: format!("remote EC hidden object delete from {node_id} failed: {error}"),
            })?;
    if response.epoch_mismatch {
        return Err(EcReadError::Unavailable {
            reason: format!(
                "remote EC hidden object delete from {node_id} rejected topology epoch"
            ),
        });
    }
    if response.success || is_not_found_message(&response.error) {
        return Ok(());
    }
    Err(EcReadError::ShardIo {
        reason: format!(
            "remote EC hidden object delete from {node_id} failed: {}",
            response.error
        ),
    })
}

fn apply_read_range(
    data: Bytes,
    range: Option<EcByteRange>,
    total: u64,
) -> Result<EcReadRangeSlice, EcReadError> {
    let Some(range) = range else {
        return Ok((data, None));
    };
    if range.start > range.end || total == 0 || range.start >= total {
        return Err(EcReadError::InvalidRange {
            start: range.start,
            end: range.end,
            total,
        });
    }
    let end = range.end.min(total - 1);
    let start_usize = usize::try_from(range.start).map_err(|_| EcReadError::InvalidRange {
        start: range.start,
        end,
        total,
    })?;
    let end_exclusive =
        usize::try_from(end.saturating_add(1)).map_err(|_| EcReadError::InvalidRange {
            start: range.start,
            end,
            total,
        })?;
    if end_exclusive > data.len() {
        return Err(EcReadError::InvalidRange {
            start: range.start,
            end,
            total,
        });
    }
    Ok((
        data.slice(start_usize..end_exclusive),
        Some((range.start, end, total)),
    ))
}

fn head_from_quorum_head(
    object: EcObjectVersionKey,
    result: QuorumHeadResult,
    replica_nodes: Vec<NodeId>,
    content_hash_sha256: [u8; 32],
) -> EcHeadObject {
    EcHeadObject {
        object,
        etag: result.etag,
        content_type: result.content_type,
        size: result.size,
        user_metadata: user_metadata(result.metadata),
        content_hash_sha256,
        hlc: result.hlc,
        version_id: result.version_id,
        layout: EcReadLayout::ReplicatedSmall { replica_nodes },
    }
}

fn hot_head_from_quorum(result: QuorumHeadResult) -> EcObjectHeadResult {
    hot_head_result(
        result.etag,
        result.content_type,
        result.size,
        result.metadata,
        result.hlc,
        result.version_id,
    )
}

fn hot_head_result(
    etag: String,
    content_type: String,
    size: u64,
    metadata: std::collections::HashMap<String, String>,
    hlc: HlcTimestamp,
    version_id: Option<String>,
) -> EcObjectHeadResult {
    EcObjectHeadResult {
        etag,
        content_type,
        size,
        metadata: user_metadata(metadata),
        hlc,
        version_id,
        layout: EcObjectReadLayout::HotReplicated,
    }
}

fn ec_head_to_object_head(head: EcHeadObject) -> EcObjectHeadResult {
    EcObjectHeadResult {
        etag: head.etag,
        content_type: head.content_type,
        size: head.size,
        metadata: head.user_metadata,
        hlc: head.hlc,
        version_id: head.version_id,
        layout: match head.layout {
            EcReadLayout::ReplicatedSmall { replica_nodes } => {
                EcObjectReadLayout::ReplicatedSmall { replica_nodes }
            }
            EcReadLayout::PackedSegment {
                segment_id,
                erasure_set_id,
                offset,
                length,
                hot_copies,
            } => EcObjectReadLayout::PackedSegment {
                segment_id,
                erasure_set_id,
                offset,
                length,
                hot_copies,
            },
            EcReadLayout::ErasureCoded {
                erasure_set_id,
                topology_epoch,
                profile_name,
                chunk_count,
            } => EcObjectReadLayout::ErasureCoded {
                erasure_set_id,
                topology_epoch,
                profile_name,
                chunk_count,
            },
        },
    }
}

fn ec_read_result_to_object_result(result: crate::ec_read::EcReadResult) -> EcObjectReadResult {
    EcObjectReadResult {
        body: result.body,
        head: ec_head_to_object_head(result.head),
        content_length: result.content_length,
        content_range: result.content_range,
        repair_enqueued: result.repair_enqueued,
    }
}

/// The object layout a manifest describes.
///
/// A packed manifest carries its placement directly. Any other manifest is
/// referenced by hash, and the chunks inside it are the placement.
fn layout_from_manifest(manifest: &EcObjectManifest) -> Result<ObjectDataLayout, EcMetadataError> {
    match &manifest.packed {
        Some(packed) => Ok(ObjectDataLayout::PackedSegment(packed.clone())),
        None => Ok(ObjectDataLayout::ErasureCoded(
            EcObjectManifestRef::from_manifest(manifest)?,
        )),
    }
}

/// The layout a node's cached erasure-coded reference stands for, checked
/// against the manifest the quorum holds.
///
/// A reference that matches is the answer as it is. Two kinds of reference
/// that do not match are this node's cache being behind, not a divergence, and
/// the quorum's manifest is taken instead — exactly as a read plan does, and
/// as a node without a record would:
///
/// - One taken at an earlier state of the same operation. The hot-copy purge
///   commits the manifest again as `hot_replicas_purged`, and every node that
///   recorded the layout before that — the one that ran the transcode, and any
///   that served a read in between — would otherwise refuse the object.
/// - One of an older generation. An overwrite clears the record only on the
///   node that took the write, so the node that transcoded the previous
///   generation keeps pointing at it once the new one is encoded. A quorum
///   manifest with a later HLC is the newer generation.
///
/// That refusal failed `DELETE` with a 500 and let the namespace entry come
/// back. The layout record is not rewritten here: a read caches what it
/// resolves, and a delete removes the record.
///
/// What still fails as a mismatch is a reference the quorum has not caught up
/// with: a state of the same operation ahead of the quorum's — a transcode
/// between its cutover and its manifest commit — or another manifest at the
/// same or an earlier HLC.
fn erasure_coded_layout_for_quorum_manifest(
    reference: &EcObjectManifestRef,
    manifest: &EcObjectManifest,
) -> Result<ObjectDataLayout, EcReadError> {
    if reference.matches_manifest(manifest)? {
        return Ok(ObjectDataLayout::ErasureCoded(reference.clone()));
    }
    if reference.references_earlier_state_of(manifest)? || manifest.hlc > reference.hlc {
        return Ok(layout_from_manifest(manifest)?);
    }
    Err(EcReadError::ManifestMismatch {
        reason: "local object layout references a manifest that did not win quorum".to_string(),
    })
}

fn manifest_is_ec_readable(manifest: &EcObjectManifest) -> bool {
    !manifest.is_deleted && manifest.state.is_ec_authoritative()
}

fn manifest_is_tombstone(manifest: &EcObjectManifest) -> bool {
    manifest.is_deleted || manifest.state == EcPlacementState::Tombstone
}

#[cfg(test)]
fn tombstone_from_live_manifest(
    live: &EcObjectManifest,
    origin_node_id: NodeId,
    hlc: HlcTimestamp,
    now_nanos: u64,
) -> Result<EcObjectManifest, ClusterError> {
    let mut tombstone = live.clone();
    tombstone.chunks.clear();
    tombstone.state = EcPlacementState::Tombstone;
    tombstone.hlc = hlc;
    tombstone.origin_node_id = origin_node_id;
    tombstone.operation_id = Uuid::new_v4();
    tombstone.created_at = now_nanos;
    tombstone.is_deleted = true;
    tombstone.validate().map_err(cluster_storage_error)?;
    Ok(tombstone)
}

fn manifest_metadata(manifest: &EcObjectManifest) -> Result<HashMap<String, String>, EcReadError> {
    Ok(HashMap::from([
        ("_s4_ec_kind".to_string(), "manifest".to_string()),
        (
            "_s4_ec_operation_id".to_string(),
            manifest.operation_id.to_string(),
        ),
        ("_s4_ec_state".to_string(), format!("{:?}", manifest.state)),
        (
            "_s4_ec_manifest_hash".to_string(),
            hex_encode(manifest.serialized_sha256()?),
        ),
    ]))
}

/// The storage operation id one repair write carries.
///
/// New on every call, and that is the whole point. A replica skips a write
/// whose operation id it has already applied — which is right for a
/// coordinator resending the same write after a timeout, because the RPC
/// client resends this same id with it. It is wrong for a shard that was
/// rebuilt again: the bytes on that node are gone or rotten now, and "already
/// applied" would answer success while the disk keeps what bit rot left.
///
/// Every other EC storage operation keeps its derived id
/// ([`derive_storage_operation_id`]): a transcode writes a shard once per
/// manifest operation, and a repeat of it really is a retry. A repair is the
/// one write that is asked for again and again for the same shard, over the
/// life of the object, and each time it is new work.
fn repair_storage_operation_id() -> u128 {
    Uuid::new_v4().as_u128()
}

fn repair_shard_metadata(request: &EcRepairShardWriteRequest) -> HashMap<String, String> {
    HashMap::from([
        ("_s4_ec_kind".to_string(), "shard".to_string()),
        (
            "_s4_ec_operation_id".to_string(),
            request.operation_id.to_string(),
        ),
        (
            "_s4_ec_repair_id".to_string(),
            request.repair_id.to_string(),
        ),
        (
            "_s4_ec_erasure_set_id".to_string(),
            request.erasure_set_id.to_string(),
        ),
        (
            "_s4_ec_chunk_index".to_string(),
            request.chunk_index.to_string(),
        ),
        (
            "_s4_ec_slot_index".to_string(),
            request.expected.slot_index.to_string(),
        ),
        (
            "_s4_ec_repair_kind".to_string(),
            format!("{:?}", request.kind),
        ),
        (
            "_s4_ec_crc32".to_string(),
            request.expected.crc32.to_string(),
        ),
        (
            "_s4_ec_sha256".to_string(),
            hex_encode(request.expected.sha256),
        ),
        // The bytes are the point of a repair. Without this the write path is
        // free to answer with the blob that already holds this content — the
        // one that rotted — and the shard would stay damaged behind a task
        // reported as succeeded (`s4_core::FORCE_PHYSICAL_WRITE_METADATA_KEY`).
        (
            FORCE_PHYSICAL_WRITE_METADATA_KEY.to_string(),
            "true".to_string(),
        ),
    ])
}

fn local_shard_record_from_index(
    record: &IndexRecord,
    expected: &ShardManifest,
    operation_id: Uuid,
) -> LocalShardRecord {
    LocalShardRecord {
        volume_id: record.file_id,
        offset: record.offset,
        size: expected.size,
        crc32: expected.crc32,
        sha256: expected.sha256,
        state: LocalShardState::Durable,
        operation_id,
    }
}

fn chunk_byte_range(manifest: &EcObjectManifest, chunk_index: u32) -> Option<(u64, u64)> {
    let mut start = 0u64;
    for chunk in &manifest.chunks {
        let end = start.saturating_add(chunk.data_size).saturating_sub(1);
        if chunk.chunk_index == chunk_index {
            return Some((start, end));
        }
        start = start.saturating_add(chunk.data_size);
    }
    None
}

fn add_hlc_metadata(metadata: &mut HashMap<String, String>, hlc: HlcTimestamp, operation_id: u128) {
    metadata.insert("_s4_hlc_wall".to_string(), hlc.wall_time.to_string());
    metadata.insert("_s4_hlc_logical".to_string(), hlc.logical.to_string());
    metadata.insert("_s4_hlc_node_id".to_string(), hlc.node_id.to_string());
    metadata.insert("_s4_operation_id".to_string(), operation_id.to_string());
}

fn hex_encode(bytes: [u8; 32]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn user_metadata(
    metadata: std::collections::HashMap<String, String>,
) -> std::collections::HashMap<String, String> {
    metadata.into_iter().filter(|(key, _)| !key.starts_with("_s4_")).collect()
}

fn cluster_read_to_ec_error(error: ClusterError) -> EcReadError {
    match error {
        ClusterError::ObjectNotFound { bucket, key } => EcReadError::NotFound {
            bucket,
            key,
            version_id: None,
        },
        ClusterError::QuorumNotMet { .. }
        | ClusterError::ReadTimeout(_)
        | ClusterError::EpochMismatch { .. } => EcReadError::Unavailable {
            reason: error.to_string(),
        },
        other => EcReadError::ShardIo {
            reason: other.to_string(),
        },
    }
}

fn map_ec_read_error(error: EcReadError) -> EcObjectReadError {
    match error {
        EcReadError::NotFound {
            bucket,
            key,
            version_id,
        } => EcObjectReadError::NotFound {
            bucket,
            key,
            version_id,
        },
        EcReadError::Deleted {
            bucket,
            key,
            version_id,
        } => EcObjectReadError::Deleted {
            bucket,
            key,
            version_id,
        },
        EcReadError::InvalidRange { start, end, total } => {
            EcObjectReadError::InvalidRange { start, end, total }
        }
        EcReadError::Unavailable { reason } | EcReadError::ShardIo { reason } => {
            EcObjectReadError::Unavailable { reason }
        }
        EcReadError::ManifestMismatch { reason }
        | EcReadError::InvalidManifest { reason }
        | EcReadError::ChunkVerification { reason }
        | EcReadError::ReplicatedSmallVerification { reason } => {
            EcObjectReadError::Internal { reason }
        }
        EcReadError::Metadata(error) => EcObjectReadError::Internal {
            reason: error.to_string(),
        },
        EcReadError::Codec(error) => EcObjectReadError::Internal {
            reason: error.to_string(),
        },
        EcReadError::Cpu(error) => EcObjectReadError::Internal {
            reason: error.to_string(),
        },
    }
}

fn ec_read_to_gc_error(error: EcReadError) -> EcGcError {
    EcGcError::Metadata(crate::ec_metadata::EcMetadataError::QuorumRead(
        error.to_string(),
    ))
}

fn ec_read_to_repair_error(error: EcReadError) -> EcRepairError {
    match error {
        EcReadError::NotFound {
            bucket,
            key,
            version_id,
        } => EcRepairError::NotFound {
            bucket,
            key,
            version_id,
        },
        EcReadError::Deleted {
            bucket,
            key,
            version_id,
        } => EcRepairError::Deleted {
            bucket,
            key,
            version_id,
        },
        EcReadError::ManifestMismatch { reason } => EcRepairError::ManifestMismatch { reason },
        EcReadError::InvalidManifest { reason }
        | EcReadError::ChunkVerification { reason }
        | EcReadError::ReplicatedSmallVerification { reason } => {
            EcRepairError::InvalidManifest { reason }
        }
        EcReadError::ShardIo { reason } | EcReadError::Unavailable { reason } => {
            EcRepairError::ShardRead { reason }
        }
        EcReadError::Metadata(error) => EcRepairError::Metadata(error),
        EcReadError::Codec(error) => EcRepairError::Codec(error),
        EcReadError::Cpu(error) => EcRepairError::Cpu(error),
        EcReadError::InvalidRange { start, end, total } => EcRepairError::ShardRead {
            reason: format!("invalid EC repair read range {start}-{end}/{total}"),
        },
    }
}

fn ec_metadata_internal(error: impl std::fmt::Display) -> EcObjectReadError {
    EcObjectReadError::Internal {
        reason: error.to_string(),
    }
}

#[async_trait]
impl EcContentDigestScan for EcOperationsService {
    async fn local_content_digests(
        &self,
        max_entries: usize,
    ) -> Result<EcContentDigestPage, ClusterError> {
        self.local_content_digest_page(max_entries).await
    }
}

#[async_trait]
impl EcHeatQuery for EcOperationsService {
    async fn local_object_heat(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        window_nanos: u64,
    ) -> Result<Option<EcObjectHeatMark>, ClusterError> {
        let object = EcObjectVersionKey::new(bucket, key, version_id.map(str::to_string));
        // Zero means the caller has no opinion, which is what a node from
        // before bucket-level promotion windows sends. Answering over this
        // node's own window is what it expects.
        let window_nanos = if window_nanos == 0 {
            seconds_to_nanos(self.config.tiering.promote_window_secs)
        } else {
            window_nanos
        };
        Ok(self
            .metadata
            .get_object_heat(&object)
            .await
            .map_err(cluster_storage_error)?
            .map(|record| EcObjectHeatMark {
                last_access_nanos: record.last_access_nanos,
                // Evaluated here, against this node's clock: a sliding window
                // decays with time, and a raw ring would mean nothing to a
                // caller that does not know how wide the slots are.
                window_access_count: record.window.reads_within(unix_time_nanos(), window_nanos),
            }))
    }
}

/// Count migrations dropped because the client changed the object under them.
///
/// A client write or delete removes the queued job for that object, and for a
/// tiering job that is an abandoned migration: work the node chose to do and
/// then had to throw away. Client-write jobs removed the same way are not
/// abandoned migrations — the object simply has a newer generation to encode.
fn record_abandoned_migrations(object: &EcObjectVersionKey, removed: EcTranscodeJobsRemoved) {
    if removed.tiering == 0 {
        return;
    }
    for _ in 0..removed.tiering {
        ec_metrics::record_tiering_cancelled();
    }
    tracing::debug!(
        bucket = %object.bucket,
        key = %object.key,
        migrations = removed.tiering,
        "EC tiering migration abandoned: the object changed"
    );
}

/// Assemble everything the decision engine needs about one object.
///
/// The object's own facts come from a quorum HEAD rather than from the local
/// index, because the node asking may hold no replica of it at all. The
/// activity mark stays local: it is node-local by design, and taking the newest
/// across the pool is the migration worker's job, not this function's.
async fn build_tiering_candidate(
    metadata: &EcMetadataStore,
    bucket: &str,
    key: &str,
    head: &QuorumHeadResult,
    promote_window_nanos: u64,
) -> Result<TieringCandidate, ClusterError> {
    let object = EcObjectVersionKey::new(bucket, key, None);
    let heat = metadata.get_object_heat(&object).await.map_err(cluster_storage_error)?;
    // The HLC carries the write time in milliseconds while the rest of the EC
    // metadata counts nanoseconds; mixing the two silently would make every
    // object look ancient.
    let created_at_nanos = head.hlc.wall_time.saturating_mul(1_000_000);
    // An object with no mark counts as used when it was created, never as used
    // at the epoch: on the first run that is every object there is.
    let last_access_nanos = heat.as_ref().map_or(created_at_nanos, |heat| {
        heat.last_access_nanos.max(created_at_nanos)
    });

    Ok(TieringCandidate {
        object,
        size: head.size,
        // A delete marker never reaches a listing, so an entry that does is a
        // live object.
        is_delete_marker: false,
        erasure_coded: head.erasure_coded,
        tags: metadata_tags(&head.metadata),
        last_access_nanos,
        // Counted over the window the bucket asked for, which may be narrower
        // than the one this node records over. The ring stores its own slot
        // width, so a narrower window is answered exactly rather than by
        // handing back a wider count and hoping it is close enough.
        window_access_count: heat.as_ref().map_or(0, |heat| {
            heat.window.reads_within(unix_time_nanos(), promote_window_nanos)
        }),
        migrations: heat.as_ref().map_or(0, |heat| heat.migrations),
        last_migration_nanos: heat.as_ref().and_then(|heat| heat.last_migration_nanos),
    })
}

/// The tiering worker's view of this node and its pool.
///
/// Holds the handles it needs rather than the operations service itself. The
/// worker outlives a single call and the admin route only ever has a `&self`,
/// so a type that can be built from either is the one that fits both.
#[derive(Clone)]
pub struct EcTieringServiceEnvironment {
    local_node_id: NodeId,
    /// Shared with `EcOperationsService`, not a copy: a migration and a client
    /// write admitted moments apart must walk one rotation, or the two paths
    /// would each fill their own favourite set.
    placement: Arc<EcPlacementService>,
    globals: TieringGlobals,
    scan_limit: usize,
    metadata: Arc<EcMetadataStore>,
    catalog: Arc<DistributedListCoordinator>,
    read_coordinator: Arc<QuorumReadCoordinator>,
    rpc_client: Arc<NodeClient>,
    /// The service's own cell, not a copy of its contents: see the field of the
    /// same name on `EcOperationsService`.
    membership: Arc<OnceLock<Arc<dyn s4_cluster::ClusterMembershipQuery>>>,
}

#[async_trait]
impl EcTieringEnvironment for EcTieringServiceEnvironment {
    async fn list_buckets(&self) -> Result<Vec<String>, ClusterError> {
        let page = self
            .catalog
            .list(
                SYSTEM_BUCKET,
                s4_core::types::BUCKET_MARKER_KEY_PREFIX,
                MAX_TIERING_BUCKETS,
                None,
            )
            .await?;
        let mut buckets = page
            .entries
            .iter()
            .filter_map(|entry| {
                entry
                    .key
                    .strip_prefix(s4_core::types::BUCKET_MARKER_KEY_PREFIX)
                    .map(String::from)
            })
            .collect::<Vec<_>>();
        // Scatter-gather returns the same marker from every node holding it,
        // and the sweep resumes by bucket name, so the order has to be stable.
        buckets.sort();
        buckets.dedup();
        Ok(buckets)
    }

    async fn list_objects(
        &self,
        bucket: &str,
        continuation: Option<&str>,
        limit: usize,
    ) -> Result<TieringCatalogPage, ClusterError> {
        let page = self.catalog.list(bucket, "", limit, continuation).await?;
        Ok(TieringCatalogPage {
            entries: page
                .entries
                .into_iter()
                .map(|entry| TieringCatalogEntry {
                    key: entry.key,
                    size: entry.size,
                })
                .collect(),
            next_continuation: page.is_truncated.then_some(page.next_continuation_token).flatten(),
        })
    }

    async fn effective_policy(&self, bucket: &str) -> Result<TieringPolicy, ClusterError> {
        // Through quorum, once per bucket per cycle.
        //
        // The local copy is a cache, and it is only ever filled by a read on
        // this node. A policy written through any other node would therefore
        // never reach this worker: an operator who narrows a bucket would watch
        // one node honour it and the rest keep migrating by the old rules,
        // indefinitely and silently. This is the read that makes a bucket policy
        // mean the same thing everywhere.
        //
        // The cost is one quorum read per bucket per cycle, and cycles are hours
        // apart. `read_tiering_policy` falls back to the cached copy when the
        // cluster cannot answer, so a partition slows convergence instead of
        // stopping tiering.
        let stored =
            read_tiering_policy_through_quorum(&self.metadata, &self.read_coordinator, bucket)
                .await?;
        Ok(TieringPolicy::resolve(
            bucket,
            &self.globals,
            stored.as_ref(),
        ))
    }

    async fn candidate(
        &self,
        bucket: &str,
        key: &str,
        policy: &TieringPolicy,
    ) -> Result<Option<TieringCandidate>, ClusterError> {
        let head = match self.read_coordinator.head(bucket, key).await {
            Ok(head) => head,
            Err(ClusterError::ObjectNotFound { .. }) => return Ok(None),
            Err(error) => return Err(error),
        };
        build_tiering_candidate(
            &self.metadata,
            bucket,
            key,
            &head,
            duration_to_nanos(policy.promote_window),
        )
        .await
        .map(Some)
    }

    async fn pool_activity(
        &self,
        candidate: &TieringCandidate,
        policy: &TieringPolicy,
    ) -> Result<PoolActivity, ClusterError> {
        let mut activity = PoolActivity {
            newest_access_nanos: candidate.last_access_nanos,
            window_reads: candidate.window_access_count,
        };

        // Every node of the pool, not only the default set's. An object lives
        // in one set, but which one is not known here, and a node that never
        // served it simply has no mark to report.
        for node_id in pool_nodes(&self.placement.sets()) {
            if node_id == self.local_node_id {
                continue;
            }
            let request = proto::ReadObjectHeatRequest {
                bucket: candidate.object.bucket.clone(),
                key: candidate.object.key.clone(),
                version_id: candidate.object.version_id.clone().unwrap_or_default(),
                coordinator_id: self.local_node_id.0.to_be_bytes().to_vec(),
                window_nanos: duration_to_nanos(policy.promote_window),
            };
            match self.rpc_client.send_read_object_heat(node_id, request).await {
                Ok(response) if response.found => {
                    activity.newest_access_nanos =
                        activity.newest_access_nanos.max(response.last_access_nanos);
                    // Summed, not maximised: each node counts only the reads it
                    // served itself, so a client spreading its traffic across
                    // the set would look a fraction as busy under any other
                    // rule, and would never reach a promotion threshold.
                    activity.window_reads =
                        activity.window_reads.saturating_add(response.window_access_count);
                }
                Ok(_) => {}
                // A peer that is down, or too old to know the call, contributes
                // nothing — the same as a peer that never served a read for the
                // object. Refusing to decide instead would stop tiering
                // whenever any node of the set is unavailable, which is a
                // steady state in a large pool rather than an exception. It
                // errs towards leaving the object where it is in both
                // directions: colder for a demotion, quieter for a promotion.
                Err(error) => {
                    tracing::debug!(
                        node = %node_id,
                        bucket = %candidate.object.bucket,
                        key = %candidate.object.key,
                        error = %error,
                        "EC tiering could not read a peer activity mark"
                    );
                }
            }
        }

        Ok(activity)
    }

    async fn enqueue_promotion(&self, candidate: &TieringCandidate) -> Result<bool, ClusterError> {
        enqueue_promotion_job(
            &self.metadata,
            &self.read_coordinator,
            &candidate.object,
            EcPromotionOrigin::Automatic,
        )
        .await
    }

    async fn outstanding_promotions(&self, ceiling: usize) -> Result<usize, ClusterError> {
        self.metadata
            .count_in_flight_promotions(ceiling, self.scan_limit)
            .await
            .map_err(cluster_storage_error)
    }

    async fn enqueue_migration(&self, candidate: &TieringCandidate) -> Result<bool, ClusterError> {
        // Re-read rather than trusting the candidate: between the decision and
        // this point the object may have been overwritten, and queueing the
        // generation that has just been replaced would send the transcode
        // worker after bytes that no longer exist.
        let head = match self
            .read_coordinator
            .head(&candidate.object.bucket, &candidate.object.key)
            .await
        {
            Ok(head) => head,
            Err(ClusterError::ObjectNotFound { .. }) => return Ok(false),
            Err(error) => return Err(error),
        };
        let Some(content_hash_sha256) =
            head.content_hash_sha256.filter(|hash| !content_hash_is_absent(hash))
        else {
            // Two objects arrive here without a usable hash: one held by a node
            // too old to report the field, and one assembled from multipart
            // parts, whose record carries the absent-hash sentinel because
            // nothing ever hashed it whole. Queuing either would hand the
            // transcode worker a source check it cannot pass, so the object
            // keeps its hot replicas instead of failing forever (EC2-204).
            tracing::debug!(
                bucket = %candidate.object.bucket,
                key = %candidate.object.key,
                "EC tiering skipped an object whose content hash the pool did not report"
            );
            return Ok(false);
        };

        if find_admitted_transcode_job(
            &self.metadata,
            &self.placement.sets(),
            &candidate.object,
            head.hlc,
        )
        .await?
        .is_some()
        {
            // Already admitted, by an earlier cycle or by another node. The
            // queue is keyed by object generation, so this is the idempotent
            // case, not a conflict.
            //
            // A *finished* job blocks admission too, and that is deliberate:
            // one generation of an object is encoded at most once. Manifests of
            // a generation are ordered by that generation's clock value and
            // their state, so a second encoding of the same generation cannot
            // be ordered against the first — nor against the marker a promotion
            // leaves in its place. An object that came back to RF=3 therefore
            // waits for a client write before it can be encoded again; until
            // then it stays replicated, which costs space and nothing else.
            // Recorded as a limitation in EC2-023.
            return Ok(false);
        }

        let target_erasure_set_id = match self.placement.select_write_set() {
            Ok(set) => set.set_id,
            // A cold object waits for the next cycle; it is already replicated,
            // and nothing is gained by queueing it before the set's layout
            // record is read.
            Err(error) if error.is_capacity_exhausted() || error.held_set().is_some() => {
                // Not a failure of this object: the pool simply has no room to
                // encode into. The candidate is left where it is and the next
                // scan offers it again, so the work is postponed rather than
                // lost (EC2-043).
                tracing::debug!(
                    bucket = %candidate.object.bucket,
                    key = %candidate.object.key,
                    error = %error,
                    "EC tiering postponed a migration: the pool has no room for it"
                );
                return Ok(false);
            }
            Err(error) => return Err(error.into()),
        };
        let operation_id =
            deterministic_operation_id(&candidate.object, head.hlc, target_erasure_set_id);

        let job = EcTranscodeJobRecord {
            object: candidate.object.clone(),
            operation_id,
            source_hlc: head.hlc,
            target_erasure_set_id,
            target: EcTranscodeTarget::Object,
            state: EcPlacementState::EcTranscodeQueued,
            attempt: 0,
            object_size: head.size,
            source_content_hash_sha256: content_hash_sha256,
            updated_at: unix_time_nanos(),
            next_retry_at: None,
            failure: None,
            origin: EcTranscodeOrigin::Tiering,
        };
        self.metadata.put_transcode_job(&job).await.map_err(cluster_storage_error)?;
        Ok(true)
    }

    async fn outstanding_migrations(&self, ceiling: usize) -> Result<usize, ClusterError> {
        let mut outstanding = 0usize;
        let mut cursor = None;
        loop {
            let page = self
                .metadata
                .list_transcode_jobs_page(cursor, self.scan_limit)
                .await
                .map_err(cluster_storage_error)?;
            for job in page.entries {
                if job.origin.is_tiering() && transcode_job_is_in_flight(job.state) {
                    outstanding += 1;
                    // The caller only needs to know whether there is headroom,
                    // and a pool with a long backlog should not pay for the
                    // rest of the walk to be told there is none.
                    if outstanding >= ceiling {
                        return Ok(outstanding);
                    }
                }
            }
            match page.next_cursor {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }
        Ok(outstanding)
    }

    async fn load_cursor(&self) -> Result<Option<EcTieringCursorRecord>, ClusterError> {
        self.metadata.get_tiering_cursor().await.map_err(cluster_storage_error)
    }

    async fn save_cursor(&self, cursor: &EcTieringCursorRecord) -> Result<(), ClusterError> {
        self.metadata.put_tiering_cursor(cursor).await.map_err(cluster_storage_error)
    }

    async fn cluster_tiering_supported(&self) -> Result<bool, ClusterError> {
        Ok(pool_supports_ec_v2(
            &self.membership,
            &self.placement.sets(),
        ))
    }
}

/// The promotion worker's view of this node and its pool.
///
/// Holds handles rather than the operations service, for the same reason the
/// forward environment does: the worker outlives any single call.
#[derive(Clone)]
pub struct EcPromotionServiceEnvironment {
    erasure_set: ErasureSet,
    scan_limit: usize,
    promote_window_nanos: u64,
    metadata: Arc<EcMetadataStore>,
    read_coordinator: Arc<QuorumReadCoordinator>,
    write_coordinator: Arc<QuorumWriteCoordinator>,
    ports: EcClusterReadPorts,
    gc: EcGcCoordinator,
    local_node_id: NodeId,
    /// Every erasure set of this pool, live.
    ///
    /// A queued reverse migration names the set its object was placed in, and
    /// this worker drains the queue for the whole pool: filtering on one set
    /// would leave every other set's rollbacks sitting there forever. Live
    /// rather than a copy, because a set added to a running pool must not leave
    /// its rollbacks stranded until the next restart.
    sets: Arc<EcErasureSets>,
}

#[async_trait]
impl EcPromotionEnvironment for EcPromotionServiceEnvironment {
    async fn ready_jobs(
        &self,
        now: u64,
        limit: usize,
    ) -> Result<Vec<EcPromotionJobRecord>, ClusterError> {
        let mut ready = Vec::new();
        let mut cursor = None;
        // One roster for the whole scan: a set added halfway through must not
        // make the pass answer two different ways about the same job.
        let sets = self.sets.current();
        loop {
            let page = self
                .metadata
                .list_promotion_jobs_page(cursor, self.scan_limit.max(1))
                .await
                .map_err(cluster_storage_error)?;
            cursor = page.next_cursor;
            for job in page.entries {
                // A job for a set this pool does not have belongs to another
                // pool's keyspace or is corrupt; either way there are no shards
                // here to bring back.
                if sets.resolve_manifest_set(job.erasure_set_id).is_none() {
                    continue;
                }
                if !job.state.is_in_flight() {
                    continue;
                }
                if job.next_retry_at.is_some_and(|retry_at| retry_at > now) {
                    continue;
                }
                ready.push(job);
                if ready.len() >= limit {
                    return Ok(ready);
                }
            }
            if cursor.is_none() {
                return Ok(ready);
            }
        }
    }

    async fn save_job(&self, job: &EcPromotionJobRecord) -> Result<(), ClusterError> {
        self.metadata.put_promotion_job(job).await.map_err(cluster_storage_error)
    }

    async fn delete_job(&self, object: &EcObjectVersionKey) -> Result<(), ClusterError> {
        self.metadata.delete_promotion_job(object).await.map_err(cluster_storage_error)
    }

    async fn read_erasure_coded(
        &self,
        job: &EcPromotionJobRecord,
    ) -> Result<Option<Vec<u8>>, ClusterError> {
        if !self.refresh_local_layout(job).await? {
            return Ok(None);
        }

        let result = self
            .ports
            .ec_reader()
            .read(
                EcReadRequest {
                    object: job.object.clone(),
                    range: None,
                },
                &self.ports,
                &self.ports,
                &self.ports,
                &NoopEcReadRepairQueue,
            )
            .await
            .map_err(|error| ClusterError::Storage(error.to_string()))?;

        // Checked against the manifest before the bytes go anywhere. A decode
        // that produced the wrong object would otherwise be written over three
        // replicas and only noticed by the client.
        if result.body.len() as u64 != job.object_size {
            return Err(ClusterError::Storage(format!(
                "EC decode produced {} byte(s) for an object the manifest records as {}",
                result.body.len(),
                job.object_size
            )));
        }
        if sha256(&result.body) != job.content_hash_sha256 {
            return Err(ClusterError::Storage(
                "EC decode did not match the manifest content hash".to_string(),
            ));
        }
        Ok(Some(result.body.to_vec()))
    }

    async fn mark_promotion_in_flight(
        &self,
        job: &EcPromotionJobRecord,
    ) -> Result<bool, ClusterError> {
        let manifest = match <EcClusterReadPorts as EcTombstoneQuorum>::quorum_read_manifest(
            &self.ports,
            &job.object,
        )
        .await
        .map_err(cluster_gc_error)?
        {
            EcManifestReadResult::Present(manifest) => manifest,
            EcManifestReadResult::Tombstone(_) | EcManifestReadResult::Absent => return Ok(false),
        };
        if manifest.is_deleted || manifest.hlc != job.source_hlc {
            // A client wrote the object; its manifest, if it has one, is not
            // this promotion's to mark.
            return Ok(false);
        }
        match manifest.state {
            // A retry of this step, or another node got further.
            EcPlacementState::Promoting | EcPlacementState::Promoted => return Ok(true),
            state if state.is_ec_authoritative() => {}
            // Nothing readers use through erasure coding, so nothing to bring
            // back from it.
            _ => return Ok(false),
        }

        // The state and nothing else. A reader holding a layout reference taken
        // at the purged manifest recognises this one as a later state of it and
        // keeps reading through erasure coding; a changed `created_at` or origin
        // would make that reference look like somebody else's manifest.
        let mut marker = manifest;
        marker.state = EcPlacementState::Promoting;

        let required = self.erasure_set.profile.manifest_quorum();
        let acks = self
            .ports
            .commit_manifest_quorum_count(marker.clone(), required)
            .await
            .map_err(|error| ClusterError::Storage(error.to_string()))?;
        if acks < required {
            return Err(ClusterError::QuorumNotMet {
                needed: required,
                achieved: acks,
            });
        }

        // Cached locally too: this node's own reconciliation reads its cache,
        // and a promotion that gives up is resumed from the node that holds
        // the mark (see `enqueue_promotion_job`).
        self.metadata.put_manifest(&marker).await.map_err(cluster_storage_error)?;
        Ok(true)
    }

    async fn restore_hot_copy(
        &self,
        job: &EcPromotionJobRecord,
        data: &[u8],
    ) -> Result<bool, ClusterError> {
        // Checked, switched, checked again — the same shape the forward cutover
        // uses, and here for the same reason: a write that landed between the
        // two would otherwise be overwritten by bytes older than itself.
        if !self.generation_unchanged(job).await? {
            return Ok(false);
        }
        let restored = self
            .write_coordinator
            .attach_object_data_if_hlc(&job.object.bucket, &job.object.key, data, job.source_hlc)
            .await?;
        // Write quorum is the bar, the same one every hot write is held to: a
        // replica that never received the object cannot take it back, and the
        // promotion must not wait forever on one that was already behind. The
        // replicas that stay erasure coded no longer break reads — the stream
        // reader skips a replica that holds a namespace entry and no bytes —
        // and anti-entropy converges them like any other divergence.
        if restored.replicas_acked < restored.replicas_total {
            tracing::debug!(
                bucket = %job.object.bucket,
                key = %job.object.key,
                acked = restored.replicas_acked,
                total = restored.replicas_total,
                "EC promotion restored the object on a quorum, not on every replica"
            );
        }
        self.generation_unchanged(job).await
    }

    async fn verify_hot_copy(&self, job: &EcPromotionJobRecord) -> Result<bool, ClusterError> {
        let result = match self.read_coordinator.read(&job.object.bucket, &job.object.key).await {
            Ok(result) => result,
            Err(ClusterError::ObjectNotFound { .. }) => return Ok(false),
            Err(error) => return Err(error),
        };
        Ok(result.size == job.object_size && sha256(&result.data) == job.content_hash_sha256)
    }

    async fn publish_promoted_manifest(
        &self,
        job: &EcPromotionJobRecord,
    ) -> Result<bool, ClusterError> {
        let manifest = match <EcClusterReadPorts as EcTombstoneQuorum>::quorum_read_manifest(
            &self.ports,
            &job.object,
        )
        .await
        .map_err(cluster_gc_error)?
        {
            EcManifestReadResult::Present(manifest) => manifest,
            // A delete won the race. It owns the shards from here, and its own
            // GC path is stricter than this one.
            EcManifestReadResult::Tombstone(_) | EcManifestReadResult::Absent => return Ok(false),
        };
        if manifest.state == EcPlacementState::Promoted {
            // Another node published it, or this is a retry after the commit
            // succeeded and the job write did not.
            return Ok(true);
        }

        let mut promoted = manifest;
        promoted.state = EcPlacementState::Promoted;
        promoted.origin_node_id = self.local_node_id;
        // The grace period the shards get is measured from here, and the
        // tombstone GC path reads the same field for the same purpose.
        promoted.created_at = unix_time_nanos();
        // The HLC is left alone. It ties the manifest to the object generation
        // it describes, and the promoted manifest still describes that
        // generation; what makes it win against the committed one is its state,
        // which outranks every erasure-coded state at the same HLC.

        let required = self.erasure_set.profile.manifest_quorum();
        let acks = self
            .ports
            .commit_manifest_quorum_count(promoted.clone(), required)
            .await
            .map_err(|error| ClusterError::Storage(error.to_string()))?;
        if acks < required {
            return Err(ClusterError::QuorumNotMet {
                needed: required,
                achieved: acks,
            });
        }

        // Cached locally so this node's own sweep can find the shards to
        // release without waiting for a read to refresh the copy.
        self.metadata.put_manifest(&promoted).await.map_err(cluster_storage_error)?;
        self.metadata
            .delete_object_layout(&job.object)
            .await
            .map_err(cluster_storage_error)?;
        Ok(true)
    }

    async fn purge_erasure_coded_copy(
        &self,
        job: &EcPromotionJobRecord,
    ) -> Result<bool, ClusterError> {
        // The marker goes first, and from the whole pool rather than just from
        // here. Left in place it would be the manifest the quorum agrees on for
        // this generation forever, and a later migration of the same generation
        // writes at the same HLC — where the marker outranks it. The object
        // would then resolve to a copy that had been moved out from under it a
        // second time.
        //
        // Removed by this node alone, and only here: it published the marker
        // and the step above has just held it through its grace, so there is no
        // window in which another node deletes a manifest that has meanwhile
        // become somebody else's. Nodes still holding shards read the absence
        // as "already collected" and release them on their own sweep.
        let unconfirmed = self.ports.delete_manifest_everywhere(&job.object, job.source_hlc).await;
        if !unconfirmed.is_empty() {
            // Every node or none. A node still holding the marker answers with
            // it, and because the marker outranks anything written at the same
            // generation, the pool would never agree on a manifest for this
            // object again. Retrying costs a cycle; getting this half-done
            // costs the object.
            return Err(ClusterError::Storage(format!(
                "{} metadata node(s) have not dropped the promotion marker yet",
                unconfirmed.len()
            )));
        }

        // Then this node's own shards. Best effort on purpose: the per-cycle
        // sweep does the same work on every node and may well have got here
        // first, and a step that treated "nothing left to do" as failure would
        // never finish the job that has to remove the marker.
        let report = self
            .gc
            .purge_promoted_object(&job.object, unix_time_nanos(), &self.ports, &self.ports)
            .await
            .map_err(cluster_gc_error)?;
        tracing::debug!(
            bucket = %job.object.bucket,
            key = %job.object.key,
            shards = report.shards_purged,
            "EC promotion released the erasure-coded copy"
        );
        Ok(true)
    }

    async fn record_migration(
        &self,
        object: &EcObjectVersionKey,
        now: u64,
    ) -> Result<(), ClusterError> {
        heat::record_migration(&self.metadata, object, now, self.promote_window_nanos)
            .await
            .map_err(cluster_storage_error)
    }

    async fn sweep_promoted_shards(&self, now: u64) -> Result<usize, ClusterError> {
        let report = self
            .gc
            .run_promoted_gc(now, &self.ports, &self.ports)
            .await
            .map_err(cluster_gc_error)?;
        if report.shards_purged > 0 {
            tracing::debug!(
                shards = report.shards_purged,
                objects = report.promoted_purged,
                "EC promotion sweep released shards of objects the pool has moved back"
            );
        }
        Ok(usize::try_from(report.shards_purged).unwrap_or(usize::MAX))
    }
}

impl EcPromotionServiceEnvironment {
    /// Point this node's cached layout at the manifest the pool agrees on.
    ///
    /// A manifest changes hash every time its state advances — committed, then
    /// GC-eligible, then purged — and a layout reference cached at one of those
    /// points no longer matches the next. The client read path heals that
    /// silently before it reads; this worker asks the read coordinator for the
    /// bytes directly, so it has to heal it first or the read fails against a
    /// manifest that is merely newer than the reference it holds.
    ///
    /// Returns `false` when the pool no longer has an erasure-coded manifest
    /// for the object, which means there is nothing left to bring back.
    async fn refresh_local_layout(&self, job: &EcPromotionJobRecord) -> Result<bool, ClusterError> {
        let read = <EcClusterReadPorts as EcReadMetadataCoordinator>::quorum_read_manifest(
            &self.ports,
            &job.object,
        )
        .await
        .map_err(|error| ClusterError::Storage(error.to_string()))?;

        let Some(manifest) = promotion_source_manifest(read) else {
            return Ok(false);
        };

        let reference =
            EcObjectManifestRef::from_manifest(&manifest).map_err(cluster_storage_error)?;
        self.metadata.put_manifest(&manifest).await.map_err(cluster_storage_error)?;
        self.metadata
            .put_object_layout(&job.object, &ObjectDataLayout::ErasureCoded(reference))
            .await
            .map_err(cluster_storage_error)?;
        Ok(true)
    }

    /// Whether the object is still the generation being promoted.
    ///
    /// Only the generation is compared, before the write and after it. A
    /// different one means a client wrote the object; its write is newer and
    /// stands, and the promotion is dropped. Whether the object is reported as
    /// erasure coded says nothing about that: after the attach it is not, and
    /// a restore repeated after a partial one or after its copy was taken away
    /// finds some replicas hot and some not, with either kind winning the HEAD.
    /// The attach itself is guarded by the generation on every replica and
    /// leaves one that already holds the bytes as it is.
    async fn generation_unchanged(&self, job: &EcPromotionJobRecord) -> Result<bool, ClusterError> {
        match self.read_coordinator.head(&job.object.bucket, &job.object.key).await {
            Ok(head) => Ok(head.hlc == job.source_hlc),
            Err(ClusterError::ObjectNotFound { .. }) => Ok(false),
            Err(error) => Err(error),
        }
    }
}

/// The manifest a promotion should decode from, if there is one.
///
/// `None` means there is nothing left to bring back, and the promotion is
/// dropped rather than retried: a tombstone means the object was deleted, an
/// absent manifest means the pool no longer knows it as erasure coded, and a
/// manifest already in `Promoted` means someone else finished the job.
fn promotion_source_manifest(read: EcManifestReadResult) -> Option<EcObjectManifest> {
    match read {
        EcManifestReadResult::Present(manifest) if manifest.state != EcPlacementState::Promoted => {
            Some(manifest)
        }
        EcManifestReadResult::Present(_)
        | EcManifestReadResult::Tombstone(_)
        | EcManifestReadResult::Absent => None,
    }
}

/// Put one object in the reverse migration queue.
///
/// Shared by the automatic path and the operator's rollback command, because
/// the two differ only in what they are allowed to ignore, never in what they
/// write. The guard is read here rather than taken from the caller: minutes may
/// have passed since the decision, and queueing a generation the pool has
/// already replaced would send the worker after shards that no longer describe
/// the object.
///
/// Returns `false` when there is nothing to promote — the object is not
/// erasure coded, it is gone, or a promotion for it is already in flight.
/// The transcode job already admitted for this object generation, if any.
///
/// A job's operation id is derived from the set it targets, so a pool with
/// several sets has one candidate id per set and a single lookup would only
/// ever find the job that happens to sit in the set chosen this time round.
/// Probing every set is what keeps "one generation is encoded at most once"
/// true once the router is free to answer differently on a later cycle — and
/// what lets an existing job pin the target set of every retry (EC2-042).
///
/// The cost is one point read per set, which is one read in every pool
/// configured today.
async fn find_admitted_transcode_job(
    metadata: &EcMetadataStore,
    sets: &ErasureSetRegistry,
    object: &EcObjectVersionKey,
    source_hlc: HlcTimestamp,
) -> Result<Option<EcTranscodeJobRecord>, ClusterError> {
    for set in sets.sets() {
        let operation_id = deterministic_operation_id(object, source_hlc, set.set_id);
        if let Some(job) =
            metadata.get_transcode_job(operation_id).await.map_err(cluster_storage_error)?
        {
            return Ok(Some(job));
        }
    }
    Ok(None)
}

async fn enqueue_promotion_job(
    metadata: &EcMetadataStore,
    read_coordinator: &QuorumReadCoordinator,
    object: &EcObjectVersionKey,
    origin: EcPromotionOrigin,
) -> Result<bool, ClusterError> {
    if object.version_id.is_some() {
        return Ok(false);
    }

    let head = match read_coordinator.head(&object.bucket, &object.key).await {
        Ok(head) => head,
        Err(ClusterError::ObjectNotFound { .. }) => return Ok(false),
        Err(error) => return Err(error),
    };

    if let Some(existing) =
        metadata.get_promotion_job(object).await.map_err(cluster_storage_error)?
    {
        if existing.state.is_in_flight() && existing.source_hlc == head.hlc {
            // Already admitted, by an earlier cycle or by the operator. The
            // queue is keyed by object, so this is the idempotent case.
            return Ok(false);
        }
    }

    let manifest = match metadata.get_manifest(object).await.map_err(cluster_storage_error)? {
        Some(manifest) => manifest,
        // The manifest is a local cache, refreshed by the read path and by
        // anti-entropy. Without it this node cannot say what the object should
        // decode to, and a promotion that cannot check its own result is worse
        // than one that waits a cycle.
        None => return Ok(false),
    };

    // Only an object whose bytes are actually in the pool has anything to come
    // back from. A hot object asked to be promoted is already where it is
    // going, which is a no-op rather than an error — unless it is hot because a
    // promotion marked it, wrote some of its copies and gave up. The mark
    // cannot be taken back at the same generation, so going on is the only way
    // out of it, and it is the one case the HEAD cannot tell: the replicas that
    // were written answer as hot (EC2-026).
    let resumes_a_marked_promotion =
        manifest.state == EcPlacementState::Promoting && manifest.hlc == head.hlc;
    if !head.erasure_coded && !resumes_a_marked_promotion {
        return Ok(false);
    }

    // The set comes from the object's own manifest, not from the pool's default
    // set. They are the same in a pool of one set and differ the moment there
    // are two, and a job naming the wrong set would send the reverse migration
    // after shards that are not there.
    let erasure_set_id = manifest.erasure_set_id;
    let job = EcPromotionJobRecord {
        object: object.clone(),
        operation_id: deterministic_promotion_id(object, head.hlc, erasure_set_id),
        source_hlc: head.hlc,
        erasure_set_id,
        state: EcPromotionState::Queued,
        attempt: 0,
        object_size: manifest.original_size,
        content_hash_sha256: manifest.content_hash_sha256,
        origin,
        shards_eligible_at: None,
        updated_at: unix_time_nanos(),
        next_retry_at: None,
        failure: None,
    };
    metadata.put_promotion_job(&job).await.map_err(cluster_storage_error)?;
    Ok(true)
}

/// Compute a deterministic reverse migration operation ID.
///
/// A separate domain string from the transcode id, so that promoting the
/// generation a transcode produced can never derive the same value and have the
/// two operations share a lease.
fn deterministic_promotion_id(
    object: &EcObjectVersionKey,
    source_hlc: HlcTimestamp,
    erasure_set_id: u64,
) -> Uuid {
    let mut hasher = Sha256::new();
    hasher.update(b"s4-ec-promotion-v1");
    hasher.update(object.bucket.as_bytes());
    hasher.update([0]);
    hasher.update(object.key.as_bytes());
    hasher.update([0]);
    hasher.update(source_hlc.wall_time.to_be_bytes());
    hasher.update(source_hlc.logical.to_be_bytes());
    hasher.update(source_hlc.node_id.to_be_bytes());
    hasher.update(erasure_set_id.to_be_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    Uuid::from_bytes(bytes)
}

/// Whether a transcode job still occupies a place in the queue.
///
/// A job that failed and may be retried still counts: it will be picked up
/// again, and admitting more work on top of it is what the ceiling exists to
/// prevent.
/// Read a bucket's tiering policy from the cluster, refreshing the local cache.
///
/// Shared by the admin surface and the tiering worker so the two cannot answer
/// differently. The local record is a cache that only a read on this node ever
/// fills, so resolving a policy anywhere else has to go through the cluster —
/// otherwise a policy written through one node would never reach the workers on
/// the others.
///
/// Falls back to the cached copy whenever the cluster cannot answer, including
/// when it has no copy at all: that is the normal answer for a bucket nobody
/// configured, and it is also what a node holding a policy written before the
/// cluster write path existed reports. Deleting the local copy on a read would
/// destroy operator input.
async fn read_tiering_policy_through_quorum(
    metadata: &EcMetadataStore,
    read_coordinator: &QuorumReadCoordinator,
    bucket: &str,
) -> Result<Option<EcTieringPolicyRecord>, ClusterError> {
    let key = tiering_policy_object_key(bucket);
    match read_coordinator.read(SYSTEM_BUCKET, &key).await {
        Ok(result) => {
            let record: EcTieringPolicyRecord =
                bincode::deserialize(&result.data).map_err(|error| {
                    ClusterError::Storage(format!(
                        "tiering policy for {bucket} could not be decoded: {error}"
                    ))
                })?;
            record.validate().map_err(cluster_storage_error)?;
            refresh_cached_tiering_policy(metadata, &record).await;
            Ok(Some(record))
        }
        Err(ClusterError::ObjectNotFound { .. }) => {
            metadata.get_tiering_policy(bucket).await.map_err(cluster_storage_error)
        }
        Err(error) => {
            tracing::warn!(
                bucket = %bucket,
                error = %error,
                "EC tiering policy quorum read failed; answering from the local cache"
            );
            metadata.get_tiering_policy(bucket).await.map_err(cluster_storage_error)
        }
    }
}

/// Keep the local copy in step with what the cluster returned.
///
/// Two rules follow from it being a cache. A cached copy with a newer HLC is not
/// overwritten — a read that raced a write must not move the bucket backwards.
/// And a failure to refresh is logged rather than returned: the caller asked
/// what the policy is, the cluster answered, and losing an optimisation is no
/// reason to fail that read.
async fn refresh_cached_tiering_policy(metadata: &EcMetadataStore, record: &EcTieringPolicyRecord) {
    match metadata.get_tiering_policy(&record.bucket).await {
        Ok(Some(cached)) if cached.hlc > record.hlc => return,
        Ok(_) => {}
        Err(error) => tracing::warn!(
            bucket = %record.bucket,
            error = %error,
            "EC tiering policy cache could not be read; refreshing it anyway"
        ),
    }
    if let Err(error) = metadata.put_tiering_policy(record).await {
        tracing::warn!(
            bucket = %record.bucket,
            error = %error,
            "EC tiering policy cache refresh failed; the cluster copy still answers"
        );
    }
}

/// Whether every node holding a slot in any set of the pool speaks EC v2.
///
/// The single place this question is answered, so the auto-tiering worker and
/// the admin health snapshot cannot drift apart.
///
/// Fail-closed on an unconfigured handle. The only production path that starts
/// the tiering worker wires membership first (`cluster_bootstrap`), and
/// [`EcOperationsService::start_tiering_worker`] refuses to start without it —
/// so an empty cell means something is wrong with the wiring, and answering
/// "yes, migrate" to a question nobody can check would be the worst available
/// answer.
fn pool_supports_ec_v2(
    membership: &OnceLock<Arc<dyn s4_cluster::ClusterMembershipQuery>>,
    sets: &ErasureSetRegistry,
) -> bool {
    match membership.get() {
        Some(membership) => membership.pool_nodes_support_v2(&pool_nodes(sets)),
        None => false,
    }
}

/// Every node holding a slot in any set of the pool, each listed once.
fn pool_nodes(sets: &ErasureSetRegistry) -> Vec<NodeId> {
    sets.distinct_nodes()
}

/// True when the manifest describes bytes erasure coding is currently storing.
///
/// The dedup measurement is a statement about stored bytes, so both ends of the
/// ratio have to hold the same population. A manifest that describes no shards
/// would inflate the denominator; one counted as a duplicate of live content
/// would inflate the numerator. The three excluded groups are:
///
/// - deleted objects and tombstones — nothing is stored;
/// - states before the shards are durable — nothing is stored *yet*, and the
///   transcode may still fail;
/// - `Promoted` and `EcFailedPermanent` — the bytes are back on, or never left,
///   RF=3 replicas, and dedup at the EC tier would not touch them.
fn manifest_holds_ec_bytes(manifest: &EcObjectManifest) -> bool {
    if manifest.is_deleted {
        return false;
    }
    match manifest.state {
        EcPlacementState::EcShardsDurable
        | EcPlacementState::EcManifestCommitted
        | EcPlacementState::EcReadVerified
        | EcPlacementState::EcCommitted
        | EcPlacementState::HotReplicasGcEligible
        | EcPlacementState::HotReplicasPurged
        // The copy coming back is not finished yet; the shards still are the
        // object's storage.
        | EcPlacementState::Promoting => true,
        EcPlacementState::HotReplicated
        | EcPlacementState::EcTranscodeQueued
        | EcPlacementState::EcEncoding
        | EcPlacementState::EcFailedRetryable
        | EcPlacementState::EcFailedPermanent
        | EcPlacementState::Promoted
        | EcPlacementState::Tombstone => false,
    }
}

fn transcode_job_is_in_flight(state: EcPlacementState) -> bool {
    match state {
        EcPlacementState::EcTranscodeQueued
        | EcPlacementState::EcEncoding
        | EcPlacementState::EcShardsDurable
        | EcPlacementState::EcManifestCommitted
        | EcPlacementState::EcReadVerified
        | EcPlacementState::EcFailedRetryable => true,
        EcPlacementState::HotReplicated
        | EcPlacementState::EcCommitted
        | EcPlacementState::HotReplicasGcEligible
        | EcPlacementState::HotReplicasPurged
        | EcPlacementState::EcFailedPermanent
        | EcPlacementState::Promoting
        | EcPlacementState::Promoted
        | EcPlacementState::Tombstone => false,
    }
}

#[async_trait]
impl EcObjectRuntime for EcOperationsService {
    async fn record_put_commit(
        &self,
        commit: EcObjectPutCommit,
    ) -> Result<EcObjectPutDisposition, ClusterError> {
        commit.validate()?;
        let object =
            EcObjectVersionKey::new(commit.bucket.clone(), commit.key.clone(), commit.version_id);
        let now = unix_time_nanos();
        let object_size = commit.size;

        // A fresh write counts as use. Without this an object stored moments
        // before a scan would look as cold as its predecessor and migrate
        // immediately, which is the opposite of what the client just asked for.
        if self.heat.is_enabled() {
            self.heat.record_write(object.clone());
        }

        if object_size < self.default_set.profile.min_object_size {
            self.clear_object_runtime_metadata(&object, false, true).await?;
            let replica_nodes = commit
                .hot_replica_nodes
                .iter()
                .copied()
                .take(REPLICATED_SMALL_REPLICA_COUNT)
                .collect::<Vec<_>>();

            if self
                .pack_small_object(
                    PackedObjectCommit {
                        object: &object,
                        content_hash_sha256: commit.content_hash_sha256,
                        hlc: commit.source_hlc,
                        replica_nodes: &replica_nodes,
                        size: object_size,
                        etag: &commit.etag,
                        content_type: &commit.content_type,
                        user_metadata: &commit.metadata,
                    },
                    now,
                )
                .await?
            {
                return Ok(EcObjectPutDisposition::PackedSegment);
            }

            let placement = ReplicatedSmallObjectPlacement::new(
                replica_nodes,
                commit.content_hash_sha256,
                object_size,
                commit.source_hlc,
            )
            .map_err(cluster_storage_error)?;
            self.metadata
                .put_object_layout(
                    &object,
                    &ObjectDataLayout::ReplicatedSmall(placement.clone()),
                )
                .await
                .map_err(cluster_storage_error)?;
            // And on the nodes that hold the bytes, not only on the one that
            // made the decision. A record that lives on the coordinator alone
            // is exactly the implicit "no EC manifest" fallback this layout
            // exists to replace: lose that node and the other five have nothing
            // that says the object was placed deliberately.
            self.publish_replicated_small_placement(&object, &placement).await?;
            return Ok(EcObjectPutDisposition::ReplicatedSmall);
        }

        // Deferred pools leave the object where the hot write path put it. No
        // EC job, no layout: to every reader it is an ordinary replicated
        // object, and it stays one until the tiering worker finds it cold.
        if self.config.tiering.defer_new_writes {
            self.clear_object_runtime_metadata(&object, false, true).await?;
            return Ok(EcObjectPutDisposition::HotReplicated);
        }

        // A job already admitted for this generation fixes the target set: the
        // choice is made once and survives every retry, so two attempts can
        // never encode one generation into two different sets (EC2-042).
        let admitted = find_admitted_transcode_job(
            &self.metadata,
            &self.placement.sets(),
            &object,
            commit.source_hlc,
        )
        .await?;

        let (target_erasure_set_id, attempt) = match admitted {
            Some(job) => (job.target_erasure_set_id, job.attempt),
            None => match self.placement.select_write_set() {
                Ok(set) => (set.set_id, 0),
                Err(EcPlacementError::LayoutPending { set_id, .. }) => {
                    // The set is there and has room, but this node has not read
                    // its layout record yet. The job is admitted to it now and
                    // the transcode worker leaves it queued until the record is
                    // read, so the object is encoded by the recorded layout
                    // rather than left on its replicas for good (EC2-088,
                    // point 3).
                    tracing::info!(
                        bucket = %object.bucket,
                        key = %object.key,
                        erasure_set_id = set_id,
                        "EC object queued for a set held until its layout record is read"
                    );
                    (set_id, 0)
                }
                Err(error) if error.is_capacity_exhausted() => {
                    // The client's write is already durable on RF=3 replicas,
                    // and EC is an optimisation on top of that. A pool out of
                    // EC capacity therefore costs space, never availability:
                    // the object stays replicated and the tiering worker
                    // encodes it once room appears.
                    tracing::warn!(
                        bucket = %object.bucket,
                        key = %object.key,
                        error = %error,
                        "EC pool has no room for a new object; it stays on its RF=3 replicas"
                    );
                    self.clear_object_runtime_metadata(&object, false, true).await?;
                    return Ok(EcObjectPutDisposition::HotReplicated);
                }
                Err(error) => return Err(error.into()),
            },
        };
        let operation_id =
            deterministic_operation_id(&object, commit.source_hlc, target_erasure_set_id);

        self.clear_object_runtime_metadata(&object, false, true).await?;

        let job = EcTranscodeJobRecord {
            object,
            operation_id,
            source_hlc: commit.source_hlc,
            target_erasure_set_id,
            target: EcTranscodeTarget::Object,
            state: EcPlacementState::EcTranscodeQueued,
            attempt,
            object_size,
            source_content_hash_sha256: commit.content_hash_sha256,
            updated_at: now,
            next_retry_at: None,
            failure: None,
            origin: EcTranscodeOrigin::ClientWrite,
        };
        self.metadata.put_transcode_job(&job).await.map_err(cluster_storage_error)?;

        Ok(EcObjectPutDisposition::TranscodeQueued)
    }

    async fn record_delete_commit(&self, commit: EcObjectDeleteCommit) -> Result<(), ClusterError> {
        commit.validate()?;
        let object = EcObjectVersionKey::new(commit.bucket, commit.key, commit.version_id);

        if let Some(deps) = self.read_runtime.get() {
            let ports = self.read_ports(deps);
            let gc = EcGcCoordinator::new(
                Arc::clone(&self.metadata),
                self.config.gc.clone(),
                self.placement.sets_handle(),
            );
            let outcome = gc
                .delete_object(
                    EcDeleteRequest {
                        object: object.clone(),
                        erasure_set: self.default_set.clone(),
                        delete_hlc: commit.delete_hlc,
                        operation_id: commit.operation_id,
                        origin_node_id: self.local_node_id,
                        now_nanos: unix_time_nanos(),
                    },
                    &ports,
                    &ports,
                )
                .await
                .map_err(cluster_gc_error)?;
            // The placement record outlives the bytes unless it is taken back.
            // Only the small-object layout has one, and only a delete that
            // actually committed a tombstone may remove it: a delete that lost
            // to a newer write must leave the record the winner is using.
            if matches!(
                outcome,
                EcDeleteOutcome::Deleted {
                    layout: EcDeletedLayout::ReplicatedSmall,
                    ..
                }
            ) {
                retract_replicated_small_placement(&ports, &object, commit.delete_hlc).await;
                self.forget_small_placement_miss(&object);
                self.metadata
                    .delete_small_placement_hint_through(&object, commit.delete_hlc)
                    .await
                    .map_err(cluster_storage_error)?;
            }
            trace_delete_outcome(&object, &outcome);
        } else {
            tracing::warn!(
                bucket = %object.bucket,
                key = %object.key,
                "Enterprise EC delete runtime is not configured; clearing local EC metadata only"
            );
            self.clear_object_runtime_metadata(&object, true, false).await?;
        }

        let removed = self
            .metadata
            .delete_transcode_jobs_for_object(&object, self.config.admin_scan_limit)
            .await
            .map_err(cluster_storage_error)?;
        record_abandoned_migrations(&object, removed);

        // A deleted object keeps no activity mark. Without this the keyspace
        // would grow by one row for every object ever read and then removed,
        // and nothing else would ever collect them.
        //
        // One narrow case survives: the cold-EC delete fallback asks the read
        // path for metadata first, so a signal for this object may still be
        // sitting in the tracker's batch and will recreate the row on the next
        // flush. That leaves a single stale row for an object that no longer
        // exists, which the tiering scanner drops when it cannot resolve it.
        self.metadata.delete_object_heat(&object).await.map_err(cluster_storage_error)?;

        // A queued reverse migration of a deleted object has nothing to bring
        // back. It would drop itself on its next cycle, once the guard read
        // returned "gone", but dropping it here saves a decode attempt and
        // keeps the queue depth honest in the meantime.
        self.metadata
            .delete_promotion_job(&object)
            .await
            .map_err(cluster_storage_error)?;
        Ok(())
    }

    async fn read_object(
        &self,
        request: EcObjectReadRequest,
    ) -> Result<EcObjectReadResult, EcObjectReadError> {
        request.validate()?;
        // A node that owes the pool shards it has not rebuilt does not answer
        // for them, and does not answer from its own metadata either: after a
        // long absence that metadata is a picture of a pool that has moved on
        // (plan 08, `EC2-NREP-02-002`).
        if !self.serves_ec_reads() {
            return Err(self.rejoin_read_refusal());
        }
        let deps = self.read_runtime()?;
        let ports = self.read_ports(deps);
        let object = EcObjectVersionKey::new(
            request.bucket.clone(),
            request.key.clone(),
            request.version_id.clone(),
        );

        // Cloned before the plan consumes the key, and only when tracking is on:
        // a node that does not tier must not pay for two allocations per read.
        let heat_key = self.heat.is_enabled().then(|| object.clone());

        let result = match self.select_read_plan(&object, &ports).await? {
            EcObjectReadPlan::HotReplicated => self.read_hot_replicated(deps, request).await,
            EcObjectReadPlan::Deleted => Err(EcObjectReadError::Deleted {
                bucket: object.bucket,
                key: object.key,
                version_id: object.version_id,
            }),
            EcObjectReadPlan::EcMetadata => {
                let range = request.range.map(|range| EcByteRange::new(range.start, range.end));
                let confirm = object.clone();
                match self
                    .ec_reader()
                    .read(
                        EcReadRequest { object, range },
                        &ports,
                        &ports,
                        &ports,
                        &self.repair,
                    )
                    .await
                {
                    Ok(result) => Ok(ec_read_result_to_object_result(result)),
                    Err(error) => Err(self.explain_missing_object(&confirm, &ports, error).await),
                }
            }
        };

        record_read_access(&self.heat, heat_key, result.is_ok());
        result
    }

    async fn head_object(
        &self,
        request: EcObjectHeadRequest,
    ) -> Result<EcObjectHeadResult, EcObjectReadError> {
        request.validate()?;
        // Held back for the same reason a read is: a `HEAD` answered from stale
        // local metadata is the same wrong answer with a smaller body.
        if !self.serves_ec_reads() {
            return Err(self.rejoin_read_refusal());
        }
        let deps = self.read_runtime()?;
        let ports = self.read_ports(deps);
        let object = EcObjectVersionKey::new(
            request.bucket.clone(),
            request.key.clone(),
            request.version_id.clone(),
        );

        let heat_key = self.heat.is_enabled().then(|| object.clone());

        let result = match self.select_read_plan(&object, &ports).await? {
            EcObjectReadPlan::HotReplicated => self.head_hot_replicated(deps, request).await,
            EcObjectReadPlan::Deleted => Err(EcObjectReadError::Deleted {
                bucket: object.bucket,
                key: object.key,
                version_id: object.version_id,
            }),
            EcObjectReadPlan::EcMetadata => {
                let confirm = object.clone();
                match self.ec_reader().head(object, &ports, &ports).await {
                    Ok(head) => Ok(ec_head_to_object_head(head)),
                    Err(error) => Err(self.explain_missing_object(&confirm, &ports, error).await),
                }
            }
        };

        record_read_access(&self.heat, heat_key, result.is_ok());
        result
    }
}

#[async_trait]
impl EcAdminService for EcOperationsService {
    async fn list_pools(&self) -> Result<Value, ClusterError> {
        let transcode_worker_started = self.transcode_worker_started.load(Ordering::Acquire);
        let read_runtime_configured = self.read_runtime.get().is_some();
        let repair_worker_started = self.repair_worker_effective_started();
        let anti_entropy_worker_started = self.anti_entropy_worker_effective_started();
        let inherited = self.live_inherited_set();
        Ok(json!({
            "enabled": true,
            "runtime": ec_runtime_status(
                transcode_worker_started,
                read_runtime_configured,
                repair_worker_started,
                anti_entropy_worker_started,
            ),
            "pools": [{
                "name": self.pool_name,
                "pool_id": inherited.pool_id,
                "erasure_set_id": inherited.set_id,
                "profile": inherited.profile,
                "state": inherited.state,
                "topology_health": inherited.topology_health,
                "topology_risks": inherited.topology_risks,
                "fixed_width_v1": true
            }]
        }))
    }

    async fn pool_health(&self, pool: &str) -> Result<Value, ClusterError> {
        self.ensure_pool(pool)?;
        self.pool_health_snapshot().await
    }

    async fn add_erasure_set(&self, request: EcAdminAddSetRequest) -> Result<Value, ClusterError> {
        request.validate()?;
        self.ensure_pool(&request.pool)?;
        self.ensure_multi_set_enabled("adding an erasure set")?;

        let mut node_ids = Vec::with_capacity(request.node_ids.len());
        for raw in &request.node_ids {
            node_ids.push(parse_node_id(raw)?);
        }

        // Checked before anything is written. A set is fixed at the profile's
        // width — that is what makes a manifest of this pool decodable at all —
        // so a request with the wrong number of nodes is refused here rather
        // than producing a set nothing can encode into.
        let width = usize::from(self.set_builder.profile().total_shards());
        if node_ids.len() != width {
            return Err(ClusterError::Config(format!(
                "erasure set needs exactly {width} node(s) for profile '{}', got {}",
                self.set_builder.profile().name,
                node_ids.len()
            )));
        }

        self.ensure_pool_speaks_v2("adding an erasure set")?;

        // Built on the roster as the cluster has it, not as this node last saw
        // it: two operators adding a set at once must not both be handed the
        // same identifier.
        let roster = self.refresh_erasure_sets().await?;

        // A node already carrying slots in another set of this pool is refused.
        // The model allows a node in several sets (EC2-040) and the placement
        // code handles it, but as an operator action it is a mistake with two
        // consequences worth refusing over: the pool gains no capacity, and its
        // free space is the sum of its sets' — a node counted twice makes that
        // sum promise room that does not exist.
        let taken = self.placement.sets();
        for node_id in &node_ids {
            if let Some(holder) = taken
                .sets()
                .into_iter()
                .find(|set| set.slot_assignment.distinct_nodes().contains(node_id))
            {
                return Err(ClusterError::Config(format!(
                    "node {node_id} already holds slots in erasure set {} of pool {}; a set built \
                     on it would add no capacity and would make the pool's free space count that \
                     node twice",
                    holder.set_id, self.pool_name
                )));
            }
        }
        let set_id = self.next_set_id(&roster);
        let created_at = unix_time_nanos();

        // A set created now is laid out by the rule this node is configured
        // with. Any rule but EC v1 needs every node of the pool to read layout
        // records, or a node that does not would lay the set out by labels.
        if self.set_builder.layout_rule_for_new_set() != EcSetLayoutRule::V1 {
            self.ensure_pool_reads_set_layouts(&format!(
                "adding an erasure set laid out by {LRC_GROUP_DOMAIN_ENV}"
            ))?;
        }
        let policy = self.set_builder.rule().policy;
        let created = self.build_new_erasure_set(set_id, &node_ids, created_at, true).await?;
        let record = EcSetLayoutRecord::new_set(
            &created,
            policy,
            created_at,
            current_hlc(self.local_node_id),
        );

        let mut next = roster.clone();
        next.sets.push(EcErasureSetSpec {
            set_id,
            node_ids: node_ids.clone(),
            state: ErasureSetState::Active,
            created_at,
        });
        next.hlc = current_hlc(self.local_node_id);
        next.updated_at = created_at;

        // Building the whole registry is the validation: it rejects a duplicate
        // id, a node outside the pool, a set of the wrong width and a topology
        // the policy refuses — all before the roster is published.
        let mut layouts = self.known_set_layouts();
        layouts.insert(set_id, record.clone());
        let registry = self
            .registry_from_roster(&next, &SetLayouts::from_records(layouts))
            .map_err(RosterRejection::into_cluster_error)?;
        registry.get(set_id).ok_or_else(|| {
            ClusterError::Config(format!(
                "erasure set {set_id} disappeared while being added"
            ))
        })?;

        // The layout is recorded before the roster makes the set visible. A set
        // in the roster without its record would be rebuilt with the EC v1
        // layout by every node that reads it; a record without a set in the
        // roster is harmless (decision EC2-084, point 3).
        self.write_set_layout_record(&record).await?;
        self.write_erasure_set_roster(&next).await?;

        // Read back before the set is used. The roster is one object under
        // last-writer-wins, so two operators adding a set at the same moment
        // would both take the same free identifier and one write would replace
        // the other. Confirming which one the pool kept turns a set that
        // silently never existed — with objects already placed into it — into a
        // conflict the operator is told about.
        let winner = self.refresh_erasure_sets().await?;
        if winner.get(set_id).filter(|spec| spec.node_ids == node_ids).is_none() {
            return Err(ClusterError::Config(format!(
                "erasure set {set_id} is not the one the pool kept: another node added a set at \
                 the same time. Nothing was placed into yours; list the pool's sets and retry if \
                 the capacity is still needed"
            )));
        }
        // The roster kept this set, so its layout record must be this one too.
        // The record is a separate object under last-writer-wins, and the add
        // that lost the roster may still have won the record; left alone, its
        // record would name nodes the set does not have.
        let kept_layout = self.known_set_layouts().get(&set_id).map(|kept| kept.node_ids.clone());
        if kept_layout.as_ref() != Some(&record.node_ids) {
            self.write_set_layout_record(&record).await?;
            self.refresh_erasure_sets().await?;
        }
        let sets = self.placement.sets();
        let added = sets
            .get(set_id)
            .ok_or_else(|| {
                ClusterError::Config(format!(
                    "erasure set {set_id} disappeared while being added"
                ))
            })?
            .clone();
        tracing::info!(
            pool = %self.pool_name,
            set_id,
            nodes = node_ids.len(),
            layout_rule = ?created.set.layout_rule,
            "erasure set added to the pool"
        );
        if let Some(violation) = &created.violation {
            ec_metrics::record_set_layout_unsatisfied(violation.reason, policy);
            tracing::warn!(
                pool = %self.pool_name,
                set_id,
                reason = violation.reason.as_str(),
                detail = %violation.detail,
                "erasure set added with degraded topology: its nodes cannot meet its layout rule, \
                 and S4_EC_TOPOLOGY_POLICY=warn let it be created anyway"
            );
        }

        Ok(json!({
            "pool": self.pool_name,
            "set": self.set_admin_snapshot(&added),
            "sets_total": sets.len(),
            "layout_violation": created.violation.as_ref().map(|violation| json!({
                "reason": violation.reason.as_str(),
                "detail": &violation.detail,
            })),
        }))
    }

    async fn apply_pool_topology(
        &self,
        request: EcAdminApplyTopologyRequest,
    ) -> Result<Value, ClusterError> {
        request.validate()?;
        self.ensure_pool(&request.pool)?;

        // The rule is this node's configuration, never part of the request: a
        // set must come out the same on every node of the pool. With the mode
        // off there is no rule to apply, and answering "done" would be a yes to
        // something that did not happen.
        let rule = self.set_builder.layout_rule_for_new_set();
        if rule == EcSetLayoutRule::V1 {
            let reason = if self.set_builder.rule().lrc_group_domain.is_none() {
                format!(
                    "this node is configured to lay sets out by the EC v1 rule; set \
                     {LRC_GROUP_DOMAIN_ENV} to rack, zone or host on every node of the pool first"
                )
            } else {
                format!(
                    "profile '{}' has no local groups to spread, so {LRC_GROUP_DOMAIN_ENV} does \
                     not apply to it; every slot of a Reed-Solomon set is equal and which node \
                     holds which does not change what the set survives",
                    self.set_builder.profile().name
                )
            };
            return Err(ClusterError::Config(format!(
                "pool {} has no topology rule to apply: {reason}",
                self.pool_name
            )));
        }

        // A node still laying sets out from labels would give this set another
        // order and place its new objects where the rule does not put them.
        self.ensure_pool_reads_set_layouts("applying a topology rule to a pool")?;

        // Built on the roster as the cluster has it, so a set added elsewhere a
        // moment ago is laid out too rather than silently skipped.
        self.refresh_erasure_sets().await?;
        let sets = self.placement.sets();
        let targets: Vec<ErasureSet> = match request.set_id {
            Some(set_id) => vec![sets
                .get(set_id)
                .ok_or_else(|| {
                    ClusterError::Config(format!(
                        "pool {} has no erasure set {set_id}",
                        self.pool_name
                    ))
                })?
                .clone()],
            None => sets.sets().into_iter().cloned().collect(),
        };

        let policy = self.set_builder.rule().policy;
        let mut changed = Vec::with_capacity(targets.len());
        for set in &targets {
            // A set whose record this node has never read must not be recorded
            // from here: the layout it is serving is the one configuration
            // gives, not the one the pool holds, and writing it would overwrite
            // a record this node has not seen (EC2-088, point 3).
            if set.layout_pending {
                return Err(ClusterError::Config(format!(
                    "erasure set {} of pool {} is still waiting for its layout record on this \
                     node, so its current layout is not known here and must not be replaced. \
                     Retry once the record is read{}",
                    set.set_id,
                    self.pool_name,
                    applied_so_far(&changed)
                )));
            }

            let nodes = set.slot_assignment.distinct_nodes();
            let created_at = unix_time_nanos();
            let created = self
                .build_new_erasure_set(set.set_id, &nodes, created_at, !request.dry_run)
                .await
                .map_err(|error| extend_config_error(error, &changed))?;

            let previous_order = set.slot_assignment.slot_order();
            let new_order = created.set.slot_assignment.slot_order();
            let moved_slots = previous_order
                .iter()
                .zip(&new_order)
                .filter(|(before, after)| before != after)
                .count();

            if !request.dry_run {
                let record = EcSetLayoutRecord::new_set(
                    &created,
                    policy,
                    created_at,
                    current_hlc(self.local_node_id),
                );
                self.write_set_layout_record(&record)
                    .await
                    .map_err(|error| extend_config_error(error, &changed))?;
                tracing::info!(
                    pool = %self.pool_name,
                    set_id = set.set_id,
                    moved_slots,
                    layout_rule = ?created.set.layout_rule,
                    "EC erasure set laid out again by the pool's topology rule on an operator's \
                     request; objects already stored keep their shards and stay readable, and only \
                     new objects follow the new layout"
                );
            }
            // A dry run neither refuses a set nor creates a degraded one, and
            // that counter means exactly those two things (EC2-083). Counting a
            // preview would make repeated dry runs look like a pool failing to
            // lay its sets out.
            if let Some(violation) = created.violation.as_ref().filter(|_| !request.dry_run) {
                ec_metrics::record_set_layout_unsatisfied(violation.reason, policy);
            }

            changed.push(json!({
                "set_id": set.set_id,
                "layout_rule": created.set.layout_rule,
                // How far the set moves. Zero means the rule already had it
                // where it wants it, and the record is rewritten with the same
                // order.
                "moved_slots": moved_slots,
                "slot_order": new_order.iter().map(ToString::to_string).collect::<Vec<_>>(),
                "layout_violation": created.violation.as_ref().map(|violation| json!({
                    "reason": violation.reason.as_str(),
                    "detail": &violation.detail,
                })),
            }));
        }

        if !request.dry_run {
            self.refresh_erasure_sets().await?;

            // Read back before the answer says the rule was applied, the way
            // adding a set does. A layout record is one object under
            // last-writer-wins, so a node that never saw this write — one
            // partitioned throughout it — can still publish its older copy over
            // this one (EC2-088, point 2). Confirming here makes that something
            // the operator is told about now rather than a layout that quietly
            // went back; the drift check reports it afterwards either way.
            let kept = self.known_set_layouts();
            for set in &changed {
                let Some(set_id) = set.get("set_id").and_then(Value::as_u64) else {
                    continue;
                };
                let expected = set
                    .get("slot_order")
                    .and_then(Value::as_array)
                    .map(|order| order.iter().filter_map(Value::as_str).collect::<Vec<_>>())
                    .unwrap_or_default();
                let actual = kept
                    .get(&set_id)
                    .map(|record| {
                        record.node_ids.iter().map(ToString::to_string).collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                if actual.iter().map(String::as_str).collect::<Vec<_>>() != expected {
                    return Err(ClusterError::Config(format!(
                        "erasure set {set_id} of pool {} is not laid out the way this request left \
                         it: another node wrote its layout record at the same time. List the \
                         pool's sets and retry if the layout is still wanted{}",
                        self.pool_name,
                        applied_so_far(&changed)
                    )));
                }
            }
        }

        Ok(json!({
            "pool": self.pool_name,
            "dry_run": request.dry_run,
            "layout_rule": rule,
            "sets": changed,
            // Said here rather than left to the documentation: this is the one
            // thing an operator running it has to know (EC2-084, point 2).
            "note": "Objects already stored keep their shards where they are and stay readable. \
                     Only objects written from now on follow the new layout; moving the existing \
                     ones would be a rewrite, which this command does not do",
        }))
    }

    async fn list_erasure_sets(&self, pool: &str) -> Result<Value, ClusterError> {
        self.ensure_pool(pool)?;
        // Refreshed first so an operator is never shown a list this node is
        // simply behind on. A cluster that cannot answer leaves the cached list
        // in place, which is the honest fallback.
        if let Err(error) = self.refresh_erasure_sets().await {
            tracing::warn!(
                error = %error,
                "EC erasure set list is answering from this node's cached roster"
            );
        }
        Ok(self.erasure_sets_snapshot())
    }

    async fn deactivate_erasure_set(
        &self,
        request: EcAdminDeactivateSetRequest,
    ) -> Result<Value, ClusterError> {
        request.validate()?;
        self.ensure_pool(&request.pool)?;
        self.ensure_multi_set_enabled("deactivating an erasure set")?;

        // Deactivation is not gated on EC v2 upgrade: taking a set out of the
        // write rotation only reduces exposure, and it must stay available
        // mid-upgrade.

        let roster = self.refresh_erasure_sets().await?;
        let sets = self.placement.sets();
        let target = sets.get(request.set_id).ok_or_else(|| {
            ClusterError::Config(format!(
                "pool {} has no erasure set {}",
                self.pool_name, request.set_id
            ))
        })?;
        if !target.accepts_writes() {
            return Ok(json!({
                "pool": self.pool_name,
                "set_id": request.set_id,
                "state": target.state,
                "changed": false,
            }));
        }
        // A pool with nowhere to write is a pool that fails every new object.
        // Deactivating the last writable set is refused rather than accepted and
        // discovered on the next PUT.
        if sets.active_sets().len() <= 1 {
            return Err(ClusterError::Config(format!(
                "erasure set {} is the only set of pool {} still taking writes; deactivating it \
                 would leave the pool nowhere to place a new object",
                request.set_id, self.pool_name
            )));
        }

        let mut next = roster.clone();
        if request.set_id == self.default_set.set_id {
            next.inherited_state = ErasureSetState::ReadOnly;
        } else {
            let spec = next.sets.iter_mut().find(|spec| spec.set_id == request.set_id).ok_or_else(
                || {
                    ClusterError::Config(format!(
                        "erasure set {} is not one this pool can change",
                        request.set_id
                    ))
                },
            )?;
            spec.state = ErasureSetState::ReadOnly;
        }
        next.hlc = current_hlc(self.local_node_id);
        next.updated_at = unix_time_nanos();

        // Built before the write for the same reason the add path builds one: a
        // roster that cannot be turned into a registry must not be published.
        self.registry_from_roster(&next, &SetLayouts::from_records(self.known_set_layouts()))
            .map_err(RosterRejection::into_cluster_error)?;
        self.write_erasure_set_roster(&next).await?;

        // Read back before answering, exactly as adding a set does. The roster
        // is one object under last-writer-wins, so two operators deactivating
        // two different sets at the same moment both write, and one write
        // replaces the other. Without this check the loser is told the set
        // stopped taking objects while it goes on taking them — and the set
        // being drained is usually a set someone is about to switch off.
        let winner = self.refresh_erasure_sets().await?;
        let kept = if request.set_id == self.default_set.set_id {
            winner.inherited_state
        } else {
            winner
                .get(request.set_id)
                .map(|spec| spec.state)
                .unwrap_or(ErasureSetState::Active)
        };
        if kept != ErasureSetState::ReadOnly {
            return Err(ClusterError::Config(format!(
                "erasure set {} of pool {} is {:?} in the roster the pool kept: another node \
                 changed the pool's sets at the same time, and this change was replaced. List the \
                 pool's sets and retry",
                request.set_id, self.pool_name, kept
            )));
        }

        tracing::info!(
            pool = %self.pool_name,
            set_id = request.set_id,
            "erasure set stopped taking new objects; reads and repair continue"
        );

        Ok(json!({
            "pool": self.pool_name,
            "set_id": request.set_id,
            "state": ErasureSetState::ReadOnly,
            "changed": true,
        }))
    }

    async fn failure_matrix(&self, pool: &str) -> Result<Value, ClusterError> {
        self.ensure_pool(pool)?;
        // Built on the portable engine on purpose: the table is derived from the
        // profile alone, so asking the accelerated one would build a generator
        // matrix and a set of ISA-L tables only to throw them away.
        let codec = s4_codec::build_codec(self.default_set.profile.codec_snapshot())
            .map_err(|error| ClusterError::Config(error.to_string()))?;
        // Deliberately node-independent: callers compare this document between
        // nodes byte for byte, so anything that legitimately differs between
        // them — the engine, the node id — belongs in pool health instead.
        Ok(json!({
            "pool": self.pool_name,
            "failure_matrix": codec.failure_matrix()
        }))
    }

    async fn object_manifest(&self, object: EcAdminObjectRef) -> Result<Value, ClusterError> {
        let key = object_key_from_admin_ref(&object)?;
        let manifest = self.authoritative_manifest(&key).await?;
        Ok(json!({
            "object": object,
            "manifest": manifest_admin_snapshot(&manifest)
        }))
    }

    async fn verify_object(&self, object: EcAdminObjectRef) -> Result<Value, ClusterError> {
        let key = object_key_from_admin_ref(&object)?;
        // The quorum's manifest, not this node's copy of it: after an overwrite
        // through another node the local copy is the previous generation, and
        // every check below would be made against an object that is gone.
        let manifest = self.authoritative_manifest(&key).await?;
        // Resolved from the manifest, not taken from the write set: the object
        // may live in any set of the pool, and verifying it against the wrong
        // set would report a healthy object as corrupt.
        let sets = self.placement.sets();
        let resolved_set = sets.resolve_manifest_set(manifest.erasure_set_id).ok_or_else(|| {
            ClusterError::Config(format!(
                "object {}/{} names erasure set {}, which pool {} does not have",
                manifest.bucket,
                manifest.key,
                manifest.erasure_set_id,
                sets.pool_id()
            ))
        })?;
        let manifest_valid = manifest
            .validate_against_erasure_set(resolved_set)
            .map(|_| true)
            .map_err(cluster_storage_error)?;
        let local_records = self
            .metadata
            .list_local_shards_for_object(&key, self.config.admin_scan_limit)
            .await
            .map_err(cluster_storage_error)?;

        let mut checked = 0usize;
        let mut mismatches = Vec::new();
        for (local_key, local) in &local_records {
            let Some(shard) =
                manifest_shard(&manifest, local_key.chunk_index, local_key.slot_index)
            else {
                mismatches.push(json!({
                    "chunk_index": local_key.chunk_index,
                    "slot_index": local_key.slot_index,
                    "reason": "local shard is not referenced by manifest"
                }));
                continue;
            };
            if let Err(error) = local.verify_against_manifest(shard) {
                if let Some(rehydrated) = self
                    .rehydrate_local_shard_record(
                        &manifest.object_key(),
                        local_key.chunk_index,
                        shard,
                    )
                    .await?
                {
                    self.metadata
                        .put_local_shard(local_key, &rehydrated)
                        .await
                        .map_err(cluster_storage_error)?;
                    checked = checked.saturating_add(1);
                } else {
                    ec_metrics::record_shard_hash_mismatch(manifest.erasure_set_id);
                    mismatches.push(json!({
                        "chunk_index": local_key.chunk_index,
                        "slot_index": local_key.slot_index,
                        "reason": error.to_string()
                    }));
                }
            } else {
                checked = checked.saturating_add(1);
            }
        }

        let mut missing_local = Vec::new();
        for (chunk_index, slot_index) in shards_owned_by(&manifest, self.local_node_id) {
            if local_records
                .iter()
                .any(|(key, _)| key.chunk_index == chunk_index && key.slot_index == slot_index)
            {
                continue;
            }
            let Some(shard) = manifest_shard(&manifest, chunk_index, slot_index) else {
                continue;
            };
            let local_key = LocalShardKey {
                object: manifest.object_key(),
                chunk_index,
                slot_index,
            };
            if let Some(rehydrated) =
                self.rehydrate_local_shard_record(&local_key.object, chunk_index, shard).await?
            {
                self.metadata
                    .put_local_shard(&local_key, &rehydrated)
                    .await
                    .map_err(cluster_storage_error)?;
                checked = checked.saturating_add(1);
            } else {
                missing_local.push(json!({
                    "chunk_index": chunk_index,
                    "slot_index": slot_index
                }));
            }
        }
        let status = if mismatches.is_empty() && missing_local.is_empty() {
            "metadata_healthy"
        } else {
            "metadata_degraded"
        };

        let full_data_verify = match self.read_runtime.get() {
            Some(deps) if manifest_is_ec_readable(&manifest) => {
                let ports = self.read_ports(deps);
                match self
                    .ec_reader()
                    // An operator asking to verify an object is asking about
                    // every shard of it, so this read keeps fetching and
                    // checking all `n` even where a serving read now stops at
                    // `k` (decision EC2-184).
                    .with_minimal_read(false)
                    .read(
                        EcReadRequest {
                            object: key.clone(),
                            range: None,
                        },
                        &ports,
                        &ports,
                        &ports,
                        &self.repair,
                    )
                    .await
                {
                    // Checked against the manifest the read resolved, which is
                    // the one the bytes were decoded by. An overwrite landing
                    // between the report above and this read would otherwise
                    // make a healthy object of the new generation look corrupt.
                    Ok(result) => json!({
                        "available": true,
                        "verified": sha256(&result.body) == result.head.content_hash_sha256,
                        "bytes_verified": result.body.len()
                    }),
                    Err(error) => json!({
                        "available": true,
                        "verified": false,
                        "reason": error.to_string()
                    }),
                }
            }
            _ => json!({
                "available": false,
                "reason": "EC read runtime is not configured or manifest is not EC-readable"
            }),
        };

        Ok(json!({
            "object": object,
            "manifest_valid": manifest_valid,
            "local_metadata_verify": {
                "checked_shards": checked,
                "mismatches": mismatches,
                "missing_local_shards": missing_local
            },
            "full_data_verify": full_data_verify,
            "status": status
        }))
    }

    async fn repair_pool(&self, request: EcAdminRepairRequest) -> Result<Value, ClusterError> {
        request.validate()?;
        self.ensure_pool(&request.pool)?;
        let target_node = match &request.node_id {
            Some(raw) => parse_node_id(raw)?,
            None => self.local_node_id,
        };
        let mut queued = Vec::new();
        let mut outcome = EcNodeRepairQueueOutcome::default();
        self.queue_repairs_for_pool(&request, target_node, &mut queued, &mut outcome)
            .await?;

        let mut value = json!({
            "pool": request.pool,
            "target_node_id": target_node.to_string(),
            // Unchanged in name and meaning: the tasks this response names.
            // What this call made of them, and what it left, is added beside
            // it rather than folded into it.
            "queued": queued.len(),
            "tasks": queued.iter().map(repair_task_admin_snapshot).collect::<Vec<_>>()
        });
        merge_admin_objects(&mut value, outcome.admin_snapshot());
        Ok(value)
    }

    async fn repair_status(
        &self,
        request: EcAdminRepairStatusRequest,
    ) -> Result<Value, ClusterError> {
        request.validate()?;
        self.ensure_pool(&request.pool)?;
        let tasks = self
            .repair
            .list_repair_tasks(self.config.admin_scan_limit)
            .await
            .map_err(cluster_config_error)?;
        let mut value = json!({
            "pool": request.pool,
            "counts": repair_counts(&tasks),
            "tasks": tasks.iter().map(repair_task_admin_snapshot).collect::<Vec<_>>()
        });

        if let Some(raw) = &request.node_id {
            let node_id = parse_node_id(raw)?;
            let inventory = self.node_shard_inventory(node_id).await?;
            // Published from here because this is where the count exists: it
            // comes from a walk over the pool's manifests, which is not work a
            // scrape can do. Asking for a node's rebuild status is therefore
            // also how its gauges are refreshed (`EC2-NREP-04-004`).
            ec_metrics::set_node_rebuild(
                &node_id.to_string(),
                inventory.shards_missing(),
                inventory.verdict().as_str(),
            );
            let mut rebuild = inventory.admin_snapshot();
            if let Some(object) = rebuild.as_object_mut() {
                // The queue beside the inventory, never instead of it. An empty
                // queue is what a finished rebuild and an unqueued one look
                // like alike; only the shard count tells them apart
                // (`EC2-NREP-03-002`).
                let owned = tasks.iter().filter(|task| task.node_id == node_id);
                object.insert("tasks".to_string(), json!(repair_counts(owned)));
            }
            if let Some(object) = value.as_object_mut() {
                object.insert("node_rebuild".to_string(), rebuild);
            }
        }
        Ok(value)
    }

    async fn list_orphans(&self, max_entries: usize) -> Result<Value, ClusterError> {
        if max_entries == 0 {
            return Err(invalid_config("max-entries", "must be greater than 0"));
        }
        let records = self
            .metadata
            .list_orphan_shards(max_entries.min(self.config.admin_scan_limit))
            .await
            .map_err(cluster_storage_error)?;
        ec_metrics::set_orphan_shards(&self.known_set_ids(), &orphan_count_by_set(&records));
        Ok(json!({
            "orphans": records,
            "count": records.len(),
            "scan_limit": max_entries.min(self.config.admin_scan_limit)
        }))
    }

    async fn dedup_report(
        &self,
        request: EcAdminDedupReportRequest,
    ) -> Result<Value, ClusterError> {
        request.validate()?;
        let (digests, facts) = self.collect_pool_content_digests(&request).await?;
        let report = build_dedup_report(digests, facts);
        // Rendered through `to_value` rather than the `json!` macro: the macro
        // turns a serialization failure into a panic, and a panic in the admin
        // surface has already reached a cluster once (plan 02, phase 2).
        serde_json::to_value(&report).map_err(|error| {
            ClusterError::Storage(format!("EC dedup report could not be rendered: {error}"))
        })
    }

    async fn approve_node_rejoin(
        &self,
        request: EcAdminApproveRejoinRequest,
    ) -> Result<Value, ClusterError> {
        request.validate()?;
        self.ensure_pool(&request.pool)?;

        let node_id: NodeId = request.node_id.trim().parse().map_err(|error| {
            invalid_config("node_id", format!("is not a node identity: {error}"))
        })?;
        let was_serving = self.serves_ec_reads();
        let status = self.apply_rejoin_approval(node_id).await?;

        let mut value = status.admin_snapshot();
        if let Some(object) = value.as_object_mut() {
            object.insert("pool".to_string(), json!(self.pool_name));
            object.insert("node_id".to_string(), json!(node_id.to_string()));
            // Whether this call is what opened the gate, so that approving a
            // node that was already serving reads as the no-op it is rather
            // than as a repair of something.
            object.insert("changed".to_string(), json!(!was_serving));
        }
        Ok(value)
    }

    async fn unpack_packed_segments(
        &self,
        request: EcAdminUnpackSegmentsRequest,
    ) -> Result<Value, ClusterError> {
        request.validate()?;
        let max_segments = request.max_segments.min(self.config.admin_scan_limit);

        // Rebuilt rather than read as stored, so a segment whose objects were
        // all deleted is seen as empty and not offered as work.
        let segments = self
            .metadata
            .load_packed_segments(self.config.admin_scan_limit)
            .await
            .map_err(cluster_storage_error)?
            .segments;

        let wanted: Option<PackedSegmentId> = match request.segment_id.as_deref() {
            Some(id) => Some(PackedSegmentId::from_uuid(
                Uuid::parse_str(id.trim()).map_err(|error| {
                    invalid_config("segment_id", format!("is not a segment id: {error}"))
                })?,
            )),
            None => None,
        };

        let mut candidates: Vec<PackedSegment> = segments
            .into_iter()
            .filter(|segment| wanted.is_none_or(|id| segment.segment_id == id))
            // An open segment is a live pool's write cursor and a dead one is
            // already on its way out. Neither is unpackable, and saying so is
            // more useful than silently skipping it.
            .filter(|segment| {
                !matches!(
                    segment.state,
                    PackedSegmentState::Open | PackedSegmentState::Dead
                )
            })
            .collect();
        candidates.sort_by_key(|segment| segment.segment_id);
        candidates.truncate(max_segments);

        if let Some(id) = wanted {
            if candidates.is_empty() {
                return Err(ClusterError::Config(format!(
                    "segment {id} is not on this node, or is open or already retired"
                )));
            }
        }

        if request.dry_run {
            return Ok(json!({
                "dry_run": true,
                "segments_eligible": candidates.len(),
                "objects": candidates.iter().map(|s| s.entries.len()).sum::<usize>(),
                "segments": candidates
                    .iter()
                    .map(|segment| json!({
                        "segment_id": segment.segment_id.to_string(),
                        "erasure_set_id": segment.erasure_set_id,
                        "state": segment.state.to_string(),
                        "objects": segment.entries.len(),
                        "used_bytes": segment.used_bytes,
                        "dead_bytes": segment.dead_bytes,
                    }))
                    .collect::<Vec<_>>(),
                "scan_limit": max_segments
            }));
        }

        let deps = self.read_runtime.get().ok_or_else(|| {
            ClusterError::Config(
                "unpacking packed segments requires the EC runtime ports to be configured"
                    .to_string(),
            )
        })?;
        let ports = self.read_ports(deps);
        let gc = EcGcCoordinator::new(
            Arc::clone(&self.metadata),
            self.config.gc.clone(),
            self.placement.sets_handle(),
        );

        let now = unix_time_nanos();
        let mut reports = Vec::with_capacity(candidates.len());
        let mut failures = Vec::new();
        for segment in &candidates {
            match gc.unpack_packed_segment(segment, &ports, now).await {
                Ok(report) => {
                    // A sealed segment may still have a transcode job queued
                    // against it. Encoding a segment nothing points at any more
                    // would spend shards to describe nothing.
                    let removed = self
                        .metadata
                        .delete_transcode_jobs_for_object(
                            &segment.manifest_object_key(),
                            self.config.admin_scan_limit,
                        )
                        .await
                        .map_err(cluster_storage_error)?;
                    reports.push(json!({
                        "segment_id": report.segment_id.to_string(),
                        "erasure_set_id": segment.erasure_set_id,
                        "objects": report.objects,
                        "unpacked": report.unpacked,
                        "restored": report.restored,
                        "kept_hot_copies": report.kept_hot_copies,
                        "skipped": report.skipped,
                        "transcode_jobs_dropped": removed.total,
                    }));
                }
                Err(error) => {
                    // Reported rather than raised: one segment that could not be
                    // unpacked must not hide the ones that were.
                    tracing::warn!(
                        segment_id = %segment.segment_id,
                        %error,
                        "packed segment could not be unpacked"
                    );
                    failures.push(json!({
                        "segment_id": segment.segment_id.to_string(),
                        "error": error.to_string(),
                    }));
                }
            }
        }

        Ok(json!({
            "dry_run": false,
            "segments_unpacked": reports.len(),
            "segments_failed": failures.len(),
            "objects_unpacked": reports
                .iter()
                .filter_map(|report| report.get("unpacked").and_then(Value::as_u64))
                .sum::<u64>(),
            "segments": reports,
            "failures": failures,
            "scan_limit": max_segments
        }))
    }

    async fn gc_orphans(&self, request: EcAdminOrphanGcRequest) -> Result<Value, ClusterError> {
        request.validate()?;
        let max_entries = request.max_entries.min(self.config.admin_scan_limit);
        let (records, eligible) = self.orphan_gc_dry_run(max_entries).await?;
        if request.dry_run {
            return Ok(json!({
                "dry_run": true,
                "mode": "dry_run",
                "orphans_scanned": records.len(),
                "orphans_eligible": eligible,
                "orphans_retained": records.len().saturating_sub(eligible),
                "orphans_purged": 0,
                "scan_limit": max_entries
            }));
        }

        let deps = self.read_runtime.get().ok_or_else(|| {
            ClusterError::Config(
                "EC orphan GC physical purger requires the EC runtime ports to be configured"
                    .to_string(),
            )
        })?;
        let ports = self.read_ports(deps);
        let gc = EcGcCoordinator::new(
            Arc::clone(&self.metadata),
            self.config.gc.clone(),
            self.placement.sets_handle(),
        );
        let report = gc
            .run_orphan_gc_limited(unix_time_nanos(), max_entries, &ports, &ports)
            .await
            .map_err(cluster_gc_error)?;
        Ok(json!({
            "dry_run": false,
            "mode": "physical_purge",
            "orphans_scanned": report.orphans_scanned,
            "orphans_eligible": eligible,
            "orphans_retained": report.orphans_retained,
            "orphans_purged": report.orphans_purged,
            "stale_orphan_records": report.stale_orphan_records,
            "scan_limit": max_entries
        }))
    }

    async fn run_tombstone_gc(
        &self,
        request: EcAdminTombstoneGcRequest,
    ) -> Result<Value, ClusterError> {
        request.validate()?;
        let max_entries = request.max_entries.min(self.config.admin_scan_limit);
        let requested_cursor = request.decoded_cursor()?;
        let now = unix_time_nanos();
        // From the first manifest, not from the background cursor: an operator
        // asking about an object means the object has to be looked at, wherever
        // in the keyspace it sits.
        let scan = self
            .scan_local_tombstones(max_entries, TombstoneScanStart::Cursor(requested_cursor))
            .await?;
        let next_cursor = scan.next_cursor.as_deref().map(EcAdminTombstoneGcRequest::encode_cursor);
        let tombstones = scan.tombstones;
        let grace = seconds_to_nanos(self.config.gc.gc_grace_secs);
        let past_grace = tombstones
            .iter()
            .filter(|tombstone| now.saturating_sub(tombstone.created_at) > grace)
            .count();

        if request.dry_run {
            return Ok(json!({
                "dry_run": true,
                "mode": "dry_run",
                "tombstones_scanned": tombstones.len(),
                "manifests_examined": scan.manifests_examined,
                "next_cursor": next_cursor,
                "round_completed": scan.round_completed,
                "tombstones_past_grace": past_grace,
                "tombstones_retained": tombstones.len().saturating_sub(past_grace),
                "tombstones_confirmed_by_all_owners": 0,
                "tombstone_handoffs_to_lagging_owners": 0,
                "tombstones_unclaimed_by_any_set": 0,
                "tombstones_shards_released": 0,
                "tombstones_retired": 0,
                "tombstones_purged": 0,
                "shards_scanned": 0,
                "shards_purged": 0,
                "shard_scan_capped": 0,
                "sets_swept": 0,
                "scan_limit": max_entries
            }));
        }

        let sweep = self.sweep_tombstones(now, &tombstones).await?;
        let report = &sweep.report;
        if report.shards_purged > 0 || report.tombstones_retired > 0 {
            tracing::info!(
                shards_purged = report.shards_purged,
                objects = report.tombstones_shards_released,
                retired = report.tombstones_retired,
                "EC tombstone GC released shards of deleted objects"
            );
        }

        Ok(json!({
            "dry_run": false,
            "mode": "physical_purge",
            "tombstones_scanned": tombstones.len(),
            "manifests_examined": scan.manifests_examined,
            "next_cursor": next_cursor,
            "round_completed": scan.round_completed,
            "tombstones_past_grace": past_grace,
            "tombstones_confirmed_by_all_owners": sweep.confirmed_by_all,
            "tombstone_handoffs_to_lagging_owners": sweep.repaired_owners,
            // Every tombstone this call looked at is in exactly one of these
            // three: kept for now, its shards released, or belonging to a set
            // this pool no longer has. They add up to `tombstones_scanned`,
            // which is what makes the report readable rather than suggestive.
            "tombstones_retained": report.tombstones_retained,
            "tombstones_unclaimed_by_any_set": sweep.unclaimed,
            "tombstones_shards_released": report.tombstones_shards_released,
            "tombstones_retired": report.tombstones_retired,
            "tombstones_purged": report.tombstones_purged,
            "shards_scanned": report.shards_scanned,
            "shards_purged": report.shards_purged,
            "shard_scan_capped": report.shard_scan_capped,
            "sets_swept": sweep.sets_swept,
            "scan_limit": max_entries
        }))
    }

    async fn test_hook(&self, request: EcAdminTestHookRequest) -> Result<Value, ClusterError> {
        request.validate()?;
        crate::ec_test_hooks::arm_fail_reconstruction_after_shards(
            request.bucket.clone(),
            request.key.clone(),
            request.version_id.clone(),
            request.mode.clone(),
        )?;
        Ok(json!({
            "enabled": true,
            "bucket": request.bucket,
            "key": request.key,
            "version_id": request.version_id,
            "mode": request.mode,
            "armed": true
        }))
    }

    async fn transcode_pause_hook(
        &self,
        request: EcAdminTranscodePauseRequest,
    ) -> Result<Value, ClusterError> {
        request.validate()?;
        ensure_test_hooks_enabled("transcode pause")?;

        let version_id = request.version_id.clone();
        match request.action.as_str() {
            "arm" => crate::ec_test_hooks::arm_transcode_pause(
                request.bucket.clone(),
                request.key.clone(),
                version_id.clone(),
                request.state.clone(),
                &request.mode,
            )?,
            "release" => {
                crate::ec_test_hooks::release_transcode_pause(
                    &request.bucket,
                    &request.key,
                    version_id.as_deref(),
                    &request.state,
                )?;
            }
            "status" => {}
            // Unreachable through `validate`, and refused rather than ignored
            // anyway: an action nobody implements must not come back looking
            // like it was carried out.
            other => {
                return Err(ClusterError::Config(format!(
                    "unsupported EC transcode pause action '{other}'; use arm, status or release"
                )))
            }
        }

        let status = crate::ec_test_hooks::transcode_pause_status(
            &request.bucket,
            &request.key,
            version_id.as_deref(),
            &request.state,
        )?;

        Ok(json!({
            "action": request.action,
            "bucket": request.bucket,
            "key": request.key,
            "version_id": request.version_id,
            "state": request.state,
            "mode": request.mode,
            // Both read from the rule, not from the action that was sent: a
            // status call on a pause nobody armed says so.
            "armed": status.armed,
            "paused": status.paused,
            "status": if status.paused { "paused" } else { "running" },
            "operation_id": status.operation_id,
            // The hook is process-local, so the node answering this is the node
            // holding the pause. The UUID goes under its own name rather than
            // under `worker_node_id`, which callers read as the node's label.
            "worker_node_uuid": status.node_id,
        }))
    }

    async fn scan_orphans(&self, max_entries: usize) -> Result<Value, ClusterError> {
        ensure_test_hooks_enabled("orphan scan")?;
        if max_entries == 0 {
            return Err(invalid_config("max-entries", "must be greater than 0"));
        }
        let limit = max_entries.min(self.config.admin_scan_limit);
        let (_, anti_entropy) = self.integrity_ports()?;
        let candidates =
            anti_entropy.detect_orphan_shards(limit).await.map_err(cluster_config_error)?;

        // Detection on its own changes nothing: the pass that finds these
        // already runs and already counts them, and then the finding is gone
        // with the report. Recording is what lets GC collect them later, and
        // what lets an operator see them at all — the listing endpoint reads
        // the recorded set, not a live sweep.
        let gc = EcGcCoordinator::new(
            Arc::clone(&self.metadata),
            self.config.gc.clone(),
            self.placement.sets_handle(),
        );
        let observed_at = unix_time_nanos();
        let mut recorded = 0usize;
        for candidate in &candidates {
            gc.record_orphan_shard(EcOrphanShardObservation {
                key: candidate.key.clone(),
                record: candidate.record.clone(),
                // Without a manifest nothing can say which set the shard was
                // meant for, so it is attributed to this node's own set rather
                // than to a guess.
                target_erasure_set_id: candidate.erasure_set_id.unwrap_or(self.default_set.set_id),
                observed_at,
                reason: candidate.reason.clone(),
            })
            .await
            .map_err(cluster_config_error)?;
            recorded = recorded.saturating_add(1);
        }

        Ok(json!({
            "scan_limit": limit,
            "candidates_found": candidates.len(),
            "orphans_recorded": recorded,
        }))
    }

    async fn codec_test_hook(
        &self,
        request: EcAdminCodecHookRequest,
    ) -> Result<Value, ClusterError> {
        let engine = crate::ec_codec::codec_engine();

        // Arming this on a pure-Rust node would change nothing, and the caller
        // would read that silence as "the matrix does not matter". Refuse, so
        // the acceptance script reports an unusable hook instead of a passing
        // check that proves nothing.
        if request.enabled && engine != CodecEngineKind::Isal {
            return Err(ClusterError::Config(format!(
                "the foreign-matrix hook only affects the accelerated engine, but this node runs \
                 the '{}' engine",
                engine.as_str()
            )));
        }

        crate::ec_test_hooks::set_foreign_matrix(request.enabled)?;
        Ok(json!({
            "enabled": request.enabled,
            "armed": crate::ec_test_hooks::foreign_matrix_armed(),
            "codec_engine": engine.as_str()
        }))
    }

    async fn run_scrub(&self) -> Result<Value, ClusterError> {
        ensure_test_hooks_enabled("scrub")?;
        let report = self.scrub_now().await?;

        // A mismatch is a shard whose bytes disagree with a recorded hash.
        // `corrupt_shards` is already the total of the three specific counters
        // below — every corruption branch increments both — so summing them
        // would report each damaged shard twice.
        //
        // What this catches in a mixed cluster is a shard written by one node
        // against a manifest committed by another. It cannot catch a node that
        // encoded and recorded with the same wrong matrix: those bytes agree
        // with their own manifest. That case is what the byte-equality
        // comparison in the acceptance script is for, and what
        // `reconstruction_samples_failed` below would eventually surface.
        let mismatches = report.corrupt_shards;

        // Repairs queued for a missing shard are counted apart from repairs
        // queued for disagreeing bytes: a node restarted a moment ago may still
        // be missing a shard, and that must not read as corruption.
        let mismatch_repairs =
            report.repairs_queued.saturating_sub(report.missing_shard_repairs_queued);

        Ok(json!({
            "codec_engine": crate::ec_codec::codec_engine().as_str(),
            "mismatches": mismatches,
            "repairs_scheduled": mismatch_repairs,
            "missing_shard_repairs_scheduled": report.missing_shard_repairs_queued,
            "repair_enqueue_failures": report.repair_enqueue_failures,
            "local_shards_scanned": report.local_shards_scanned,
            "healthy_shards": report.healthy_shards,
            "missing_shards": report.missing_shards,
            "corrupt_shards": report.corrupt_shards,
            "blob_header_crc_failures": report.blob_header_crc_failures,
            "physical_hash_mismatches": report.physical_hash_mismatches,
            "manifest_hash_mismatches": report.manifest_hash_mismatches,
            "physical_read_failures": report.physical_read_failures,
            // Outside `corrupt_shards` on purpose: these shards were not
            // judged at all, because the medium did not answer for them
            // (decision EC2-200). `media_suspect` says the pass stopped for
            // that reason and did not look at the rest of the inventory.
            "media_unavailable": report.media_unavailable,
            "media_suspect": report.media_suspect,
            "quarantined_shards": report.quarantined_shards,
            "quarantine_failures": report.quarantine_failures,
            "reconstruction_samples_verified": report.reconstruction_samples_verified,
            "reconstruction_samples_failed": report.reconstruction_samples_failed
        }))
    }

    async fn run_anti_entropy(&self) -> Result<Value, ClusterError> {
        ensure_test_hooks_enabled("anti-entropy")?;
        let report = self.anti_entropy_now().await?;

        // A divergence is a shard this node holds that does not match what the
        // manifests say it should be. Corrupt counts as divergence; missing is
        // reported on its own, for the same reason as in the scrub above.
        //
        // `repair_enqueue_failures` is reported too: a pass that found problems
        // and failed to queue a single repair would otherwise look like a pass
        // that found nothing.
        Ok(json!({
            "codec_engine": crate::ec_codec::codec_engine().as_str(),
            "divergences": report.corrupt_local_shards,
            "expected_local_shards": report.expected_local_shards,
            "healthy_local_shards": report.healthy_local_shards,
            "missing_local_shards": report.missing_local_shards,
            "quarantined_shards": report.quarantined_shards,
            "quarantine_failures": report.quarantine_failures,
            "repairs_scheduled": report.repairs_queued,
            "repair_enqueue_failures": report.repair_enqueue_failures
        }))
    }

    async fn tiering_policy(&self, bucket: String) -> Result<Value, ClusterError> {
        let stored = self.read_tiering_policy(&bucket).await?;
        let effective = TieringPolicy::resolve(&bucket, &self.tiering_globals(), stored.as_ref());
        Ok(json!({
            "bucket": bucket,
            // What the operator set, and separately what is actually in force
            // after inheritance. Reporting only the second would hide whether a
            // value is the bucket's own or the node's; only the first would
            // leave an unexplained "nothing migrates".
            "policy": stored.as_ref().map(tiering_policy_record_snapshot),
            "effective": tiering_policy_snapshot(&effective)
        }))
    }

    async fn put_tiering_policy(
        &self,
        bucket: String,
        request: EcAdminTieringPolicyRequest,
    ) -> Result<Value, ClusterError> {
        request.validate()?;
        let now = unix_time_nanos();
        let record = EcTieringPolicyRecord {
            bucket: bucket.clone(),
            enabled: request.enabled,
            cold_after_secs: request.cold_after_secs,
            min_object_size: request.min_object_size,
            exclude_prefixes: request.exclude_prefixes,
            exclude_tags: request.exclude_tags,
            promote_after_reads: request.promote_after_reads,
            promote_window_secs: request.promote_window_secs,
            hlc: current_hlc(self.local_node_id),
            updated_at: now,
        };
        self.write_tiering_policy(&record).await?;

        let effective = TieringPolicy::resolve(&bucket, &self.tiering_globals(), Some(&record));
        Ok(json!({
            "bucket": bucket,
            "policy": tiering_policy_record_snapshot(&record),
            "effective": tiering_policy_snapshot(&effective)
        }))
    }

    async fn tiering_dry_run(
        &self,
        request: EcAdminTieringDryRunRequest,
    ) -> Result<Value, ClusterError> {
        request.validate()?;
        let deps = self.read_runtime.get().ok_or_else(|| {
            ClusterError::Config("EC read runtime is not configured on this node".to_string())
        })?;

        let stored = self.read_tiering_policy(&request.bucket).await?;
        let policy =
            TieringPolicy::resolve(&request.bucket, &self.tiering_globals(), stored.as_ref());
        let prefix = request.prefix.clone().unwrap_or_default();
        let max_objects = request.max_objects.min(self.config.admin_scan_limit);
        let now = unix_time_nanos();

        let catalog = self.catalog.get().ok_or_else(|| {
            ClusterError::Config("EC tiering catalog is not configured on this node".to_string())
        })?;

        let mut report = TieringDryRunReport::default();
        let mut samples = Vec::new();
        let mut continuation: Option<String> = None;

        while report.scanned < max_objects {
            let page_size = (max_objects - report.scanned).min(DEFAULT_EC_ADMIN_SCAN_PAGE);
            let page = catalog
                .list(&request.bucket, &prefix, page_size, continuation.as_deref())
                .await?;
            if page.entries.is_empty() {
                break;
            }

            for entry in &page.entries {
                report.scanned += 1;

                // The size bounds are decidable from the listing alone, and in
                // most buckets they rule out the majority of objects. Asking the
                // pool for metadata first would put one quorum HEAD behind every
                // object of the scan limit, which on a large bucket is minutes
                // of wall time inside one admin request.
                if entry.size < policy.min_object_size {
                    let decision = TieringDecision::Skip(TieringSkipReason::TooSmall {
                        size: entry.size,
                        minimum: policy.min_object_size,
                    });
                    report.record(&decision);
                    push_dry_run_sample(&mut samples, request.max_samples, entry, &decision, 0);
                    continue;
                }
                if entry.size > policy.max_object_size {
                    let decision = TieringDecision::Skip(TieringSkipReason::TooLarge {
                        size: entry.size,
                        maximum: policy.max_object_size,
                    });
                    report.record(&decision);
                    push_dry_run_sample(&mut samples, request.max_samples, entry, &decision, 0);
                    continue;
                }

                let head = deps.read_coordinator.head(&request.bucket, &entry.key).await;
                let head = match head {
                    Ok(head) => head,
                    // An object that vanished between the listing and the head
                    // is simply not a candidate. Failing the whole report over
                    // one such race would make the dry run unusable on a busy
                    // bucket.
                    Err(ClusterError::ObjectNotFound { .. }) => continue,
                    Err(error) => return Err(error),
                };
                let candidate = build_tiering_candidate(
                    &self.metadata,
                    &request.bucket,
                    &entry.key,
                    &head,
                    duration_to_nanos(policy.promote_window),
                )
                .await?;
                let decision = decide(&policy, &candidate, now);
                report.record(&decision);
                let idle_secs = now.saturating_sub(candidate.last_access_nanos) / 1_000_000_000;
                push_dry_run_sample(
                    &mut samples,
                    request.max_samples,
                    entry,
                    &decision,
                    idle_secs,
                );
            }

            if !page.is_truncated {
                break;
            }
            continuation = page.next_continuation_token;
            if continuation.is_none() {
                break;
            }
        }

        Ok(json!({
            "bucket": request.bucket,
            "prefix": request.prefix,
            // Bounded by the admin scan limit, and nothing here writes: a dry
            // run an operator is afraid to repeat is not a dry run.
            "scanned": report.scanned,
            "scan_limit": max_objects,
            "would_migrate": report.would_migrate,
            "would_promote": report.would_promote,
            "skipped": report.skipped_counts(),
            "effective": tiering_policy_snapshot(&policy),
            "samples": samples
        }))
    }

    async fn run_tiering_cycle(&self) -> Result<Value, ClusterError> {
        ensure_test_hooks_enabled("tiering scan cycle")?;
        // Deliberately not gated on `tiering.enabled`: a node with the feature
        // off must still answer, so a test can prove that it queues nothing.
        let worker = EcTieringWorker::new(
            self.tiering_worker_config(),
            Arc::new(self.tiering_environment()?),
        );
        let report = worker.run_once(unix_time_nanos()).await?;
        Ok(tiering_cycle_snapshot(&report))
    }

    async fn promote_objects(
        &self,
        request: EcAdminTieringPromoteRequest,
    ) -> Result<Value, ClusterError> {
        request.validate()?;
        let deps = self.read_runtime.get().ok_or_else(|| {
            ClusterError::Config("EC read runtime is not configured on this node".to_string())
        })?;
        let catalog = self.catalog.get().ok_or_else(|| {
            ClusterError::Config("EC tiering catalog is not configured on this node".to_string())
        })?;

        let prefix = request.prefix.clone().unwrap_or_default();
        let mut examined = 0usize;
        let mut queued = Vec::new();
        let mut already_hot = 0usize;
        let mut not_queued = 0usize;
        let mut continuation: Option<String> = None;

        loop {
            let remaining = request.max_objects.saturating_sub(examined);
            if remaining == 0 {
                break;
            }
            let page = catalog
                .list(
                    &request.bucket,
                    &prefix,
                    remaining.min(self.config.admin_scan_limit),
                    continuation.as_deref(),
                )
                .await?;
            let next = page.next_continuation_token.clone();

            for entry in page.entries {
                examined = examined.saturating_add(1);
                // The dry run answers the same question without writing, so an
                // operator can size a rollback before starting it.
                if request.dry_run {
                    match deps.read_coordinator.head(&request.bucket, &entry.key).await {
                        Ok(head) if head.erasure_coded => queued.push(entry.key.clone()),
                        Ok(_) => already_hot = already_hot.saturating_add(1),
                        Err(ClusterError::ObjectNotFound { .. }) => {}
                        Err(error) => return Err(error),
                    }
                    continue;
                }

                let object = EcObjectVersionKey::new(&request.bucket, &entry.key, None);
                if enqueue_promotion_job(
                    &self.metadata,
                    &deps.read_coordinator,
                    &object,
                    EcPromotionOrigin::Manual,
                )
                .await?
                {
                    queued.push(entry.key.clone());
                } else {
                    not_queued = not_queued.saturating_add(1);
                }
            }

            match next {
                Some(token) if !token.is_empty() => continuation = Some(token),
                _ => break,
            }
        }

        if !queued.is_empty() {
            tracing::info!(
                bucket = %request.bucket,
                prefix = %prefix,
                queued = queued.len(),
                dry_run = request.dry_run,
                "EC operator requested a reverse migration"
            );
        }

        Ok(json!({
            "bucket": request.bucket,
            "prefix": prefix,
            "dry_run": request.dry_run,
            "examined": examined,
            "queued": queued.len(),
            // An object that is not erasure coded is already where the operator
            // wants it; one that is, but was not queued, already has a
            // promotion in flight. Neither is a failure, and the two counts are
            // what tells them apart.
            "already_hot": already_hot,
            "not_queued": not_queued,
            "objects": queued,
        }))
    }

    async fn run_promotion_cycle(&self) -> Result<Value, ClusterError> {
        ensure_test_hooks_enabled("promotion cycle")?;
        let worker = EcPromotionWorker::new(
            self.promotion_worker_config(),
            Arc::new(self.promotion_environment()?),
        );
        let report = worker.run_once(unix_time_nanos()).await?;
        Ok(promotion_cycle_snapshot(&report))
    }

    async fn object_heat(&self, object: EcAdminObjectRef) -> Result<Value, ClusterError> {
        ensure_test_hooks_enabled("object activity mark")?;
        let key = object_key_from_admin_ref(&object)?;
        let record = self.metadata.get_object_heat(&key).await.map_err(cluster_storage_error)?;
        Ok(json!({
            "object": object,
            // The mark is node-local, so the answer is about this node only.
            // A caller comparing nodes has to ask each of them.
            "tracking_enabled": self.heat.is_enabled(),
            "counters": self.heat.counters(),
            "heat": record
        }))
    }

    async fn promotion_job(&self, object: EcAdminObjectRef) -> Result<Value, ClusterError> {
        ensure_test_hooks_enabled("reverse migration job")?;
        let key = object_key_from_admin_ref(&object)?;
        let record = self.metadata.get_promotion_job(&key).await.map_err(cluster_storage_error)?;
        Ok(json!({
            "object": object,
            // Node-local, like the queue itself: a promotion is carried out by
            // the node that admitted it, and a caller comparing nodes has to
            // ask each of them.
            "job": record.as_ref().map(promotion_job_admin_snapshot),
            "in_flight": record.as_ref().is_some_and(|job| job.state.is_in_flight()),
        }))
    }

    async fn transcode_job(&self, object: EcAdminObjectRef) -> Result<Value, ClusterError> {
        ensure_test_hooks_enabled("forward migration job")?;
        let key = object_key_from_admin_ref(&object)?;
        // Jobs are keyed by operation id, so finding the one for an object is a
        // scan — the same one `delete_transcode_jobs_for_object` performs. That
        // is acceptable here and nowhere else: this route exists for tests, is
        // gated behind the test hooks flag, and is bounded by the admin scan
        // limit like every other listing surface.
        let mut matching = Vec::new();
        let mut cursor = None;
        loop {
            let page = self
                .metadata
                .list_transcode_jobs_page(cursor, self.config.admin_scan_limit)
                .await
                .map_err(cluster_storage_error)?;
            matching.extend(page.entries.into_iter().filter(|job| job.object == key));
            match page.next_cursor {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }
        // Newest first, so `job` names the attempt a caller is asking about
        // rather than whichever one the keyspace happened to yield first.
        matching.sort_unstable_by_key(|job| std::cmp::Reverse(job.updated_at));
        let in_flight = matching.iter().any(|job| transcode_job_is_in_flight(job.state));
        let current = matching
            .iter()
            .find(|job| transcode_job_is_in_flight(job.state))
            .or_else(|| matching.first());

        Ok(json!({
            "object": object,
            // Node-local, like the queue itself: a migration is carried out by
            // the node that admitted it, and a caller comparing nodes has to
            // ask each of them.
            "job": current.map(transcode_job_admin_snapshot),
            "jobs": matching.iter().map(transcode_job_admin_snapshot).collect::<Vec<_>>(),
            "in_flight": in_flight,
        }))
    }

    async fn reconcile_namespace(&self) -> Result<Value, ClusterError> {
        ensure_test_hooks_enabled("namespace reconciliation")?;
        let report = self.reconcile_namespace_now().await?;
        Ok(json!({
            "manifests_scanned": report.manifests_scanned,
            "entries_restored": report.entries_restored,
            "entries_detached": report.entries_detached,
            "already_current": report.already_current,
            "awaiting_purge": report.awaiting_purge,
            "superseded": report.superseded,
            "failed": report.failed
        }))
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
struct TranscodeCounts {
    queue_depth: usize,
    queued: usize,
    running: usize,
    committed: usize,
    gc_eligible: usize,
    purged: usize,
    failed_retryable: usize,
    failed_permanent: usize,
}

#[derive(Debug, Clone, Copy, Serialize)]
struct RepairCounts {
    queued: usize,
    running: usize,
    succeeded: usize,
    failed_retryable: usize,
    failed_permanent: usize,
}

impl RepairCounts {
    fn queue_depth(self) -> usize {
        self.queued.saturating_add(self.running).saturating_add(self.failed_retryable)
    }
}

fn transcode_counts(jobs: &[EcTranscodeJobRecord]) -> TranscodeCounts {
    let mut counts = TranscodeCounts {
        queue_depth: 0,
        queued: 0,
        running: 0,
        committed: 0,
        gc_eligible: 0,
        purged: 0,
        failed_retryable: 0,
        failed_permanent: 0,
    };
    for job in jobs {
        match job.state {
            EcPlacementState::EcTranscodeQueued => {
                counts.queued = counts.queued.saturating_add(1);
                counts.queue_depth = counts.queue_depth.saturating_add(1);
            }
            EcPlacementState::EcEncoding
            | EcPlacementState::EcShardsDurable
            | EcPlacementState::EcManifestCommitted
            | EcPlacementState::EcReadVerified => {
                counts.running = counts.running.saturating_add(1);
                counts.queue_depth = counts.queue_depth.saturating_add(1);
            }
            EcPlacementState::EcCommitted => {
                counts.committed = counts.committed.saturating_add(1);
            }
            EcPlacementState::HotReplicasGcEligible => {
                counts.gc_eligible = counts.gc_eligible.saturating_add(1);
            }
            EcPlacementState::HotReplicasPurged => {
                counts.purged = counts.purged.saturating_add(1);
            }
            EcPlacementState::EcFailedRetryable => {
                counts.failed_retryable = counts.failed_retryable.saturating_add(1);
                counts.queue_depth = counts.queue_depth.saturating_add(1);
            }
            EcPlacementState::EcFailedPermanent => {
                counts.failed_permanent = counts.failed_permanent.saturating_add(1);
            }
            // None describes a transcode job: one is the state before a job
            // exists, the others are states only a manifest ever reaches.
            EcPlacementState::HotReplicated
            | EcPlacementState::Promoting
            | EcPlacementState::Promoted
            | EcPlacementState::Tombstone => {}
        }
    }
    counts
}

/// Transcode queue depth per erasure set.
///
/// Derived from [`transcode_counts`] one job at a time rather than repeating its
/// list of states, so the per-set gauge and the pool snapshot can never disagree
/// about what counts as queued.
fn transcode_queue_depth_by_set(jobs: &[EcTranscodeJobRecord]) -> HashMap<u64, usize> {
    let mut by_set: HashMap<u64, usize> = HashMap::new();
    for job in jobs {
        if transcode_counts(std::slice::from_ref(job)).queue_depth == 1 {
            let depth = by_set.entry(job.target_erasure_set_id).or_insert(0);
            *depth = depth.saturating_add(1);
        }
    }
    by_set
}

/// Repair queue depth per erasure set, derived from [`repair_counts`] the same way.
fn repair_queue_depth_by_set(tasks: &[EcRepairTaskRecord]) -> HashMap<u64, usize> {
    let mut by_set: HashMap<u64, usize> = HashMap::new();
    for task in tasks {
        if repair_task_is_in_queue(task) {
            let depth = by_set.entry(task.erasure_set_id).or_insert(0);
            *depth = depth.saturating_add(1);
        }
    }
    by_set
}

/// The same queue depth, split by how many shards the task's chunk is missing.
///
/// The measure repairs are ordered by (decision EC2-160), counted over the
/// same tasks and by the same liveness rule as [`repair_queue_depth_by_set`],
/// so the buckets of a set add up to that set's depth. A chunk is what the
/// codec rebuilds, so losses are counted per chunk of a version of an object
/// and never summed across chunks.
fn repair_queue_depth_by_losses(
    tasks: &[EcRepairTaskRecord],
) -> HashMap<u64, HashMap<&'static str, usize>> {
    let mut losses_by_chunk: HashMap<RepairChunkKey<'_>, usize> = HashMap::new();
    for task in tasks.iter().filter(|task| repair_task_is_in_queue(task)) {
        let losses = losses_by_chunk.entry(repair_chunk_key(task)).or_insert(0);
        *losses = losses.saturating_add(1);
    }

    let mut by_set: HashMap<u64, HashMap<&'static str, usize>> = HashMap::new();
    for task in tasks.iter().filter(|task| repair_task_is_in_queue(task)) {
        let losses = losses_by_chunk.get(&repair_chunk_key(task)).copied().unwrap_or(1);
        let bucket = by_set
            .entry(task.erasure_set_id)
            .or_default()
            .entry(ec_metrics::repair_loss_bucket(losses))
            .or_insert(0);
        *bucket = bucket.saturating_add(1);
    }
    by_set
}

/// Whether a task still takes up room in the durable repair queue.
fn repair_task_is_in_queue(task: &EcRepairTaskRecord) -> bool {
    repair_counts(std::slice::from_ref(task)).queue_depth() == 1
}

/// The chunk a repair task belongs to: one version of one object, written by
/// one operation, and one chunk index inside it.
type RepairChunkKey<'a> = (&'a EcObjectVersionKey, Uuid, u32);

fn repair_chunk_key(task: &EcRepairTaskRecord) -> RepairChunkKey<'_> {
    (&task.object, task.operation_id, task.chunk_index)
}

/// Local orphan shard candidates per erasure set.
fn orphan_count_by_set(
    records: &[(crate::ec_metadata::EcOrphanShardKey, EcOrphanShardRecord)],
) -> HashMap<u64, usize> {
    let mut by_set: HashMap<u64, usize> = HashMap::new();
    for (_, record) in records {
        let count = by_set.entry(record.target_erasure_set_id).or_insert(0);
        *count = count.saturating_add(1);
    }
    by_set
}

fn is_explicit_single_shard_repair_request(request: &EcAdminRepairRequest) -> bool {
    request.object.is_some()
        && request.chunk_index.is_some()
        && request.slot_index.is_some()
        && request.node_id.is_some()
}

/// Tally the states of `tasks`.
///
/// Takes anything that yields task references so a caller can count a filtered
/// view — the tasks of one slot owner, say — without copying the queue.
fn repair_counts<'a>(tasks: impl IntoIterator<Item = &'a EcRepairTaskRecord>) -> RepairCounts {
    let mut counts = RepairCounts {
        queued: 0,
        running: 0,
        succeeded: 0,
        failed_retryable: 0,
        failed_permanent: 0,
    };
    for task in tasks {
        match task.status {
            EcRepairStatus::Queued => counts.queued = counts.queued.saturating_add(1),
            EcRepairStatus::Running => counts.running = counts.running.saturating_add(1),
            EcRepairStatus::Succeeded => counts.succeeded = counts.succeeded.saturating_add(1),
            EcRepairStatus::FailedRetryable => {
                counts.failed_retryable = counts.failed_retryable.saturating_add(1);
            }
            EcRepairStatus::FailedPermanent => {
                counts.failed_permanent = counts.failed_permanent.saturating_add(1);
            }
        }
    }
    counts
}

/// Every `(chunk, slot)` of `manifest` the pool puts on `node_id`.
///
/// The pool's own statement of what that identity answers for, and the only
/// source of it: a node's local shard index says what it has, which is the
/// other half of the question a rebuild asks.
fn shards_owned_by(manifest: &EcObjectManifest, node_id: NodeId) -> Vec<(u32, u8)> {
    manifest
        .chunks
        .iter()
        .flat_map(|chunk| {
            chunk.shards.iter().filter_map(move |shard| {
                (shard.node_id == node_id).then_some((chunk.chunk_index, shard.slot_index))
            })
        })
        .collect()
}

fn manifest_shard(
    manifest: &EcObjectManifest,
    chunk_index: u32,
    slot_index: u8,
) -> Option<&ShardManifest> {
    manifest
        .chunks
        .iter()
        .find(|chunk| chunk.chunk_index == chunk_index)
        .and_then(|chunk| chunk.shards.iter().find(|shard| shard.slot_index == slot_index))
}

/// The free-space line for one set, or an honest "not measured".
fn set_capacity_admin_snapshot(line: Option<&EcSetCapacityReport>) -> Value {
    match line.and_then(|line| line.capacity.map(|capacity| (line, capacity))) {
        Some((line, capacity)) => json!({
            "measured": true,
            "available_bytes": capacity.available_bytes,
            "used_ratio": capacity.used_ratio,
            "full": line.full,
        }),
        // Unknown is not zero. A set nobody has reported on has not run out of
        // space, and an operator acts on the two differently.
        None => json!({
            "measured": false,
            "available_bytes": Value::Null,
            "used_ratio": Value::Null,
            "full": false,
        }),
    }
}

/// Names the sets an apply-topology request had already laid out again, for an
/// error raised partway through.
///
/// A request covering several sets writes one record per set, and the sets
/// before the failure keep their new layout. Saying so turns a partial apply
/// into something an operator can finish, instead of a state they have to
/// discover by listing the pool.
fn applied_so_far(changed: &[Value]) -> String {
    if changed.is_empty() {
        return String::new();
    }
    let ids = changed
        .iter()
        .filter_map(|set| set.get("set_id").and_then(Value::as_u64))
        .map(|set_id| set_id.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        ". {} erasure set(s) were already laid out again and keep their new layout: {ids}",
        changed.len()
    )
}

/// Add the partial-apply note to an error raised partway through an apply.
fn extend_config_error(error: ClusterError, changed: &[Value]) -> ClusterError {
    let note = applied_so_far(changed);
    if note.is_empty() {
        return error;
    }
    match error {
        ClusterError::Config(message) => ClusterError::Config(format!("{message}{note}")),
        other => {
            // Every other kind keeps its class: a storage fault must not start
            // answering 400 because it happened to interrupt an apply. The
            // partial apply is logged instead of folded into the message.
            tracing::warn!(
                applied = %note.trim_start_matches(". "),
                error = %other,
                "EC apply-topology stopped partway through the pool's sets"
            );
            other
        }
    }
}

/// One erasure set as the admin surface reports it.
///
/// `placement` adds where the set's slots sit, by failure domain. It is added
/// next to the fields that were always here, never in place of one: a caller
/// written before this release reads exactly what it read before (plan 05,
/// `EC2-ZONE-04-001`). `None` leaves the response in its earlier shape.
/// How the cluster currently sees one node of the pool.
///
/// `Unknown` and `NotAMember` are deliberately different. The first says this
/// node has no membership view and therefore no opinion; the second says the
/// view is there and has no entry for that identity, which is what a node
/// replaced by a machine with a fresh identity looks like. Folding them
/// together would report "the pool cannot see this node" when the truth is
/// "this node cannot see anything" (`EC2-NREP-01-001`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SlotOwnerMembership {
    Alive,
    Suspect,
    Dead,
    Left,
    NotAMember,
    Unknown,
}

impl SlotOwnerMembership {
    fn as_str(self) -> &'static str {
        match self {
            Self::Alive => "alive",
            Self::Suspect => "suspect",
            Self::Dead => "dead",
            Self::Left => "left",
            Self::NotAMember => "not_a_member",
            Self::Unknown => "unknown",
        }
    }

    /// Whether the pool has positive confirmation that this node is up.
    ///
    /// `Unknown` is not "not alive": a node without a membership view knows
    /// nothing about anyone, and reporting that as a dead pool would send an
    /// operator to replace six healthy machines.
    fn is_seen_alive(self) -> bool {
        matches!(self, Self::Alive)
    }

    /// Whether this is a positive statement that the node is not up.
    fn is_seen_not_alive(self) -> bool {
        matches!(
            self,
            Self::Suspect | Self::Dead | Self::Left | Self::NotAMember
        )
    }
}

/// One node of the pool, the slots it owns, and how the cluster sees it.
#[derive(Debug, Clone)]
struct SlotOwner {
    node_id: NodeId,
    /// The set whose slots it holds, or `None` for a node holding none.
    set_id: Option<u64>,
    /// Slot indices within that set, ascending.
    slots: Vec<u8>,
    membership: SlotOwnerMembership,
}

/// The pool's verdict, from the three things that can take it away.
///
/// Split out of the health snapshot so the rule can be read back without
/// building a pool: the third input is new in plan 08 and it is the one an
/// acceptance run has to be able to state on its own.
///
/// The third input is the point of `EC2-NREP-02-004`. A node that came back
/// with its identity and none of its shards is `alive` in membership, holds
/// every slot the pool gives it, and answers for none of them — and before this
/// the verdict went on reading "healthy" through exactly that node. Topology
/// risk and permanently failed work cannot see that state; only the node's own
/// return can.
fn pool_health_verdict(
    any_topology_risk: bool,
    repair_failed_permanent: usize,
    transcode_failed_permanent: usize,
    serving_ec_reads: bool,
) -> &'static str {
    if !any_topology_risk
        && repair_failed_permanent == 0
        && transcode_failed_permanent == 0
        && serving_ec_reads
    {
        "healthy"
    } else {
        "degraded"
    }
}

/// This node's return to the pool, as the health route reports it.
///
/// Deliberately puts three things in one object: the verdict, the slots the
/// pool gives this identity, and how many shards this node actually holds. Each
/// is harmless alone and together they are the whole of the state plan 08 was
/// written for — an owner the pool sees alive that answers for nothing.
///
/// `local_shards_truncated` matters as much as the count: a walk that stopped
/// at the admin scan limit did not count the shards, it counted the limit, and
/// an operator reading "held: 1024" has to know which of the two happened.
fn rejoin_admin_snapshot(
    local_node_id: NodeId,
    status: &EcRejoinStatus,
    slot_owners: &[SlotOwner],
    local_shards_held: usize,
    local_shards_truncated: bool,
) -> Value {
    let own = slot_owners.iter().find(|owner| owner.node_id == local_node_id);
    let mut value = status.admin_snapshot();
    if let Some(object) = value.as_object_mut() {
        object.insert("node_id".to_string(), json!(local_node_id.to_string()));
        object.insert(
            "slots".to_string(),
            json!(own.map(|owner| owner.slots.clone()).unwrap_or_default()),
        );
        object.insert("local_shards_held".to_string(), json!(local_shards_held));
        object.insert(
            "local_shards_truncated".to_string(),
            json!(local_shards_truncated),
        );
    }
    value
}

impl SlotOwner {
    fn admin_snapshot(&self) -> Value {
        json!({
            "node_id": self.node_id.to_string(),
            "set_id": self.set_id,
            "slots": self.slots,
            "membership": self.membership.as_str(),
        })
    }
}

fn erasure_set_admin_snapshot(
    erasure_set: &ErasureSet,
    placement: Option<&SetDomainPlacement>,
) -> Value {
    let slots = erasure_set
        .slot_assignment
        .slots()
        .iter()
        .map(|slot| {
            json!({
                "slot_index": slot.slot_index,
                "node_id": slot.node_id.to_string(),
                "role": slot.role,
                "local_group": slot.local_group,
            })
        })
        .collect::<Vec<_>>();

    let mut value = json!({
        "set_id": erasure_set.set_id,
        "pool_id": erasure_set.pool_id.as_u32(),
        "profile": &erasure_set.profile,
        "slot_assignment": {
            "slots": slots,
        },
        "topology_epoch": erasure_set.topology_epoch,
        "created_epoch": erasure_set.created_epoch,
        "state": erasure_set.state,
        "topology_health": erasure_set.topology_health,
        "topology_risks": &erasure_set.topology_risks,
        "layout_rule": erasure_set.layout_rule,
        "layout_pending": erasure_set.layout_pending,
    });

    if let (Some(placement), Some(object)) = (placement, value.as_object_mut()) {
        object.insert(
            "placement".to_string(),
            json!({
                // The level the domains below are grouped at, and whose labels
                // said so: two nodes with different environments describe the
                // same set differently (EC2-086, point 4).
                "level": placement.level,
                "slots": &placement.slots,
                "domains": &placement.domains,
                "unlabeled_nodes": &placement.unlabeled_nodes,
            }),
        );
    }
    value
}

/// Render one reverse migration job for the admin surface.
///
/// Written out field by field rather than serialized whole, for the same reason
/// the manifest is: node ids and operation ids are 128-bit, and JSON numbers
/// are not. They go out as strings.
fn promotion_job_admin_snapshot(job: &EcPromotionJobRecord) -> Value {
    json!({
        "bucket": &job.object.bucket,
        "key": &job.object.key,
        "operation_id": job.operation_id.to_string(),
        "source_hlc": hlc_admin_snapshot(job.source_hlc),
        "erasure_set_id": job.erasure_set_id,
        "state": job.state,
        "attempt": job.attempt,
        "object_size": job.object_size,
        "origin": job.origin,
        "shards_eligible_at": job.shards_eligible_at,
        "updated_at": job.updated_at,
        "next_retry_at": job.next_retry_at,
        "failure": &job.failure,
    })
}

fn transcode_job_admin_snapshot(job: &EcTranscodeJobRecord) -> Value {
    json!({
        "bucket": &job.object.bucket,
        "key": &job.object.key,
        "operation_id": job.operation_id.to_string(),
        "source_hlc": hlc_admin_snapshot(job.source_hlc),
        "erasure_set_id": job.target_erasure_set_id,
        "state": job.state,
        "attempt": job.attempt,
        "object_size": job.object_size,
        "origin": job.origin,
        "updated_at": job.updated_at,
        "next_retry_at": job.next_retry_at,
        "failure": &job.failure,
    })
}

fn manifest_admin_snapshot(manifest: &EcObjectManifest) -> Value {
    let chunks = manifest.chunks.iter().map(chunk_manifest_admin_snapshot).collect::<Vec<_>>();

    json!({
        "format_version": manifest.format_version,
        "bucket": &manifest.bucket,
        "key": &manifest.key,
        "version_id": &manifest.version_id,
        "original_size": manifest.original_size,
        "content_hash_sha256": manifest.content_hash_sha256,
        "etag": &manifest.etag,
        "content_type": &manifest.content_type,
        "user_metadata": &manifest.user_metadata,
        "profile": &manifest.profile,
        "erasure_set_id": manifest.erasure_set_id,
        "topology_epoch": manifest.topology_epoch,
        "chunks": chunks,
        "state": manifest.state,
        "hlc": hlc_admin_snapshot(manifest.hlc),
        "origin_node_id": manifest.origin_node_id.to_string(),
        "operation_id": manifest.operation_id.to_string(),
        "created_at": manifest.created_at,
        "is_deleted": manifest.is_deleted,
        "packed": manifest.packed.as_ref().map(packed_placement_admin_snapshot),
        // What this manifest costs on disk, per metadata replica. Reported
        // because a packed object's manifest is the only metadata it has, and
        // an operator measuring what packing saves has to be able to count it.
        "encoded_size": encode_manifest(manifest).map(|bytes| bytes.len()).ok(),
    })
}

fn packed_placement_admin_snapshot(placement: &PackedSegmentObjectPlacement) -> Value {
    let segment_manifest = packed_segment_manifest_key(placement.segment_id);
    json!({
        "segment_id": placement.segment_id.to_string(),
        "segment_manifest_bucket": segment_manifest.bucket,
        "segment_manifest_key": segment_manifest.key,
        "erasure_set_id": placement.erasure_set_id,
        "offset": placement.offset,
        "length": placement.length,
        "content_hash_sha256": placement.content_hash_sha256,
        "hot_replica_nodes": placement
            .hot_replica_nodes
            .iter()
            .map(|node| node.to_string())
            .collect::<Vec<_>>(),
    })
}

fn chunk_manifest_admin_snapshot(chunk: &ChunkManifest) -> Value {
    let shards = chunk.shards.iter().map(shard_manifest_admin_snapshot).collect::<Vec<_>>();

    json!({
        "chunk_index": chunk.chunk_index,
        "data_size": chunk.data_size,
        "encoded_size": chunk.encoded_size,
        "chunk_sha256": chunk.chunk_sha256,
        "shards": shards,
    })
}

fn shard_manifest_admin_snapshot(shard: &ShardManifest) -> Value {
    json!({
        "slot_index": shard.slot_index,
        "node_id": shard.node_id.to_string(),
        "role": shard.role,
        "crc32": shard.crc32,
        "sha256": shard.sha256,
        "size": shard.size,
    })
}

fn repair_task_admin_snapshot(task: &EcRepairTaskRecord) -> Value {
    json!({
        "repair_id": task.repair_id.to_string(),
        "object": &task.object,
        "operation_id": task.operation_id.to_string(),
        "erasure_set_id": task.erasure_set_id,
        "chunk_index": task.chunk_index,
        "slot_index": task.slot_index,
        "node_id": task.node_id.to_string(),
        "kind": task.kind,
        "status": task.status,
        "attempts": task.attempts,
        "reason": &task.reason,
        "enqueued_at": task.enqueued_at,
        "updated_at": task.updated_at,
        "next_retry_at": task.next_retry_at,
        "failure_domain": &task.failure_domain,
    })
}

/// The stored bucket policy as JSON.
///
/// Fields are written out one by one rather than handed to `json!` whole. The
/// record carries an HLC, whose node id is a `u128`, and JSON has no such
/// number: serializing the record directly fails at runtime, and inside `json!`
/// that failure is an unwrap. The manifest snapshot above exists for the same
/// reason and renders the HLC the same way.
fn tiering_policy_record_snapshot(record: &EcTieringPolicyRecord) -> Value {
    json!({
        "bucket": &record.bucket,
        "enabled": record.enabled,
        "cold_after_secs": record.cold_after_secs,
        "min_object_size": record.min_object_size,
        "exclude_prefixes": &record.exclude_prefixes,
        "exclude_tags": &record.exclude_tags,
        "promote_after_reads": record.promote_after_reads,
        "promote_window_secs": record.promote_window_secs,
        "hlc": hlc_admin_snapshot(record.hlc),
        "updated_at": record.updated_at,
    })
}

fn hlc_admin_snapshot(hlc: HlcTimestamp) -> Value {
    json!({
        "wall_time": hlc.wall_time,
        "logical": hlc.logical,
        "node_id": NodeId(hlc.node_id).to_string(),
    })
}

fn hot_replica_is_eligible(record: &EcHotReplicaGcRecord, now: u64) -> bool {
    record.purged_at.is_none() && record.full_object_sha256_verified && now >= record.eligible_at
}

fn object_key_from_admin_ref(
    object: &EcAdminObjectRef,
) -> Result<EcObjectVersionKey, ClusterError> {
    object.validate()?;
    Ok(EcObjectVersionKey::new(
        object.bucket.clone(),
        object.key.clone(),
        object.version_id.clone(),
    ))
}

fn parse_node_id(raw: &str) -> Result<NodeId, ClusterError> {
    let uuid = Uuid::parse_str(raw).map_err(|error| {
        ClusterError::Config(format!("invalid EC repair node_id '{raw}': {error}"))
    })?;
    Ok(NodeId(uuid.as_u128()))
}

fn release_gate_status(
    transcode_worker_started: bool,
    read_runtime_configured: bool,
    repair_worker_started: bool,
    anti_entropy_worker_started: bool,
) -> Value {
    let ci_acceptance = if ec_v1_ci_acceptance_attested() {
        "passed"
    } else {
        "pending_ci_acceptance"
    };
    let transcode_worker_runtime = if transcode_worker_started {
        "wired"
    } else {
        "pending_runtime_wiring"
    };
    let repair_worker_runtime = if repair_worker_started {
        "wired"
    } else {
        "pending_runtime_wiring"
    };
    let anti_entropy_worker_runtime = if anti_entropy_worker_started {
        "wired"
    } else {
        "pending_runtime_wiring"
    };
    let s3_read_path = if read_runtime_configured {
        "wired_ec_read"
    } else {
        "wired_hot_rf3"
    };
    let gates = BTreeMap::from([
        ("staged_write", "wired_hot_rf3_metadata_commit"),
        (
            "hot_copy_delete_after_ec_read_verified",
            if transcode_worker_started {
                "wired"
            } else {
                "pending_transcode_worker"
            },
        ),
        (
            "manifest_authoritative_shard_hashes",
            "implemented_in_foundation",
        ),
        (
            "local_metadata_miss_quorum_read",
            "implemented_in_foundation",
        ),
        ("multipart_staged_ec", "wired_hot_rf3_metadata_commit"),
        ("corrupt_shard_no_client_leak", "wired_manifest_authority"),
        ("bounded_repair_gc_resources", "wired_runtime_throttles"),
        ("repair_worker_runtime", repair_worker_runtime),
        ("anti_entropy_scrub_runtime", anti_entropy_worker_runtime),
        ("ec_v1_acceptance_ci", ci_acceptance),
        ("s3_put_object_path", "wired_hot_rf3"),
        ("s3_get_object_path", s3_read_path),
        ("s3_head_object_path", s3_read_path),
        (
            "s3_delete_object_path",
            if read_runtime_configured {
                "wired_ec_tombstone"
            } else {
                "wired_hot_rf3"
            },
        ),
        ("s3_multipart_path", "wired_hot_rf3"),
        ("http_ec_read_write_path", s3_read_path),
        ("transcode_worker_runtime", transcode_worker_runtime),
        (
            "orphan_gc_physical_purger",
            if read_runtime_configured {
                "wired"
            } else {
                "dry_run_only"
            },
        ),
        ("chaos_ci", ci_acceptance),
        ("inline_ec_write", "disabled_by_default"),
        ("auto_tiering", "disabled_by_default"),
    ]);
    json!(gates)
}

fn ec_runtime_status(
    transcode_worker_started: bool,
    read_runtime_configured: bool,
    repair_worker_started: bool,
    anti_entropy_worker_started: bool,
) -> Value {
    let s3_hot_path_wired = true;
    let hot_copy_purger_wired = transcode_worker_started;
    let orphan_gc_physical_purger_wired = read_runtime_configured;
    let runtime_ready = s3_hot_path_wired
        && transcode_worker_started
        && read_runtime_configured
        && repair_worker_started
        && anti_entropy_worker_started
        && hot_copy_purger_wired
        && orphan_gc_physical_purger_wired;
    let ci_acceptance_passed = ec_v1_ci_acceptance_attested();
    let production_ready = runtime_ready && ci_acceptance_passed;
    let blockers = production_readiness_blockers(
        transcode_worker_started,
        read_runtime_configured,
        repair_worker_started,
        anti_entropy_worker_started,
        hot_copy_purger_wired,
        orphan_gc_physical_purger_wired,
        ci_acceptance_passed,
    );
    json!({
        "serving_state": if transcode_worker_started {
            if read_runtime_configured {
                "s3_ec_read_path_with_transcode_worker"
            } else {
                "s3_hot_path_with_transcode_worker"
            }
        } else {
            if read_runtime_configured {
                "s3_ec_read_path_wired"
            } else {
                "s3_hot_path_wired"
            }
        },
        "s3_object_path": "wired_hot_rf3",
        "http_data_path": if read_runtime_configured { "wired_ec_read" } else { "wired_hot_rf3" },
        "ec_read_runtime": if read_runtime_configured { "wired" } else { "not_configured" },
        "transcode_worker": if transcode_worker_started { "wired" } else { "not_started" },
        "repair_worker": if repair_worker_started { "wired" } else { "not_started" },
        "anti_entropy_worker": if anti_entropy_worker_started { "wired" } else { "not_started" },
        "hot_copy_purger": if hot_copy_purger_wired { "wired" } else { "not_started" },
        "orphan_gc": if orphan_gc_physical_purger_wired { "wired_physical_purger" } else { "dry_run_only" },
        "runtime_ready": runtime_ready,
        "ci_acceptance": if ci_acceptance_passed { "passed" } else { "not_attested" },
        "production_ready_blockers": blockers,
        "production_ready": production_ready
    })
}

fn production_readiness_blockers(
    transcode_worker_started: bool,
    read_runtime_configured: bool,
    repair_worker_started: bool,
    anti_entropy_worker_started: bool,
    hot_copy_purger_wired: bool,
    orphan_gc_physical_purger_wired: bool,
    ci_acceptance_passed: bool,
) -> Vec<&'static str> {
    let mut blockers = Vec::new();
    if !transcode_worker_started {
        blockers.push("transcode_worker_not_started");
    }
    if !read_runtime_configured {
        blockers.push("ec_read_runtime_not_configured");
    }
    if !repair_worker_started {
        blockers.push("repair_worker_not_started");
    }
    if !anti_entropy_worker_started {
        blockers.push("anti_entropy_or_scrub_worker_not_started");
    }
    if !hot_copy_purger_wired {
        blockers.push("hot_copy_purger_not_wired");
    }
    if !orphan_gc_physical_purger_wired {
        blockers.push("orphan_gc_physical_purger_not_wired");
    }
    if !ci_acceptance_passed {
        blockers.push("ec_v1_ci_acceptance_not_attested");
    }
    blockers
}

fn ec_v1_ci_acceptance_attested() -> bool {
    option_env!("S4_EC_V1_CI_ACCEPTANCE_PASSED")
        .map(env_value_truthy)
        .unwrap_or(false)
}

fn env_value_truthy(value: &str) -> bool {
    matches!(
        value.to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on" | "passed"
    )
}

fn parse_optional_bool(name: &'static str) -> Result<Option<bool>, ClusterError> {
    match std::env::var(name) {
        Ok(raw) => match raw.to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Ok(Some(true)),
            "0" | "false" | "no" | "off" => Ok(Some(false)),
            other => Err(invalid_config(
                name,
                format!("unknown boolean value '{other}'"),
            )),
        },
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(invalid_config(name, error.to_string())),
    }
}

fn parse_optional_u64(name: &'static str) -> Result<Option<u64>, ClusterError> {
    match std::env::var(name) {
        Ok(raw) => raw
            .parse::<u64>()
            .map(Some)
            .map_err(|error| invalid_config(name, error.to_string())),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(invalid_config(name, error.to_string())),
    }
}

fn parse_optional_u32(name: &'static str) -> Result<Option<u32>, ClusterError> {
    match std::env::var(name) {
        Ok(raw) => raw
            .parse::<u32>()
            .map(Some)
            .map_err(|error| invalid_config(name, error.to_string())),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(invalid_config(name, error.to_string())),
    }
}

fn parse_optional_u8(name: &'static str) -> Result<Option<u8>, ClusterError> {
    match std::env::var(name) {
        Ok(raw) => raw
            .parse::<u8>()
            .map(Some)
            .map_err(|error| invalid_config(name, error.to_string())),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(invalid_config(name, error.to_string())),
    }
}

fn parse_optional_usize(name: &'static str) -> Result<Option<usize>, ClusterError> {
    match std::env::var(name) {
        Ok(raw) => raw
            .parse::<usize>()
            .map(Some)
            .map_err(|error| invalid_config(name, error.to_string())),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(invalid_config(name, error.to_string())),
    }
}

fn invalid_config(name: impl AsRef<str>, reason: impl Into<String>) -> ClusterError {
    ClusterError::Config(format!("invalid {}: {}", name.as_ref(), reason.into()))
}

fn cluster_config_error(error: impl std::fmt::Display) -> ClusterError {
    ClusterError::Config(error.to_string())
}

fn cluster_storage_error(error: impl std::fmt::Display) -> ClusterError {
    ClusterError::Storage(error.to_string())
}

/// Keep "the cluster cannot answer" apart from "storage failed".
///
/// A manifest read that cannot reach quorum is the first, and a caller that is
/// told the second goes looking for a fault instead of for the missing nodes.
fn ec_read_cluster_error(error: EcReadError) -> ClusterError {
    match error {
        EcReadError::Unavailable { reason } => ClusterError::Unavailable(reason),
        other => ClusterError::Storage(other.to_string()),
    }
}

fn cluster_gc_error(error: EcGcError) -> ClusterError {
    match error {
        EcGcError::ManifestQuorum {
            required_acks,
            actual_acks,
        }
        | EcGcError::ReplicatedSmallQuorum {
            required_acks,
            actual_acks,
        } => ClusterError::QuorumNotMet {
            needed: required_acks,
            achieved: actual_acks,
        },
        EcGcError::InvalidConfig { .. } | EcGcError::InvalidRequest { .. } => {
            ClusterError::Config(error.to_string())
        }
        EcGcError::TombstoneNotConfirmed { .. }
        | EcGcError::PackedRepack { .. }
        | EcGcError::Metadata(_) => ClusterError::Storage(error.to_string()),
    }
}

fn trace_delete_outcome(object: &EcObjectVersionKey, outcome: &EcDeleteOutcome) {
    match outcome {
        EcDeleteOutcome::Deleted {
            layout,
            manifest_acks,
            replicated_small_acks,
            ..
        } => tracing::info!(
            bucket = %object.bucket,
            key = %object.key,
            version_id = ?object.version_id,
            layout = ?layout,
            manifest_acks,
            replicated_small_acks = ?replicated_small_acks,
            "EC delete tombstone committed"
        ),
        EcDeleteOutcome::AlreadyDeleted { .. } => tracing::debug!(
            bucket = %object.bucket,
            key = %object.key,
            version_id = ?object.version_id,
            "EC delete observed an existing tombstone"
        ),
        EcDeleteOutcome::NotFound => tracing::debug!(
            bucket = %object.bucket,
            key = %object.key,
            version_id = ?object.version_id,
            "EC delete found no EC layout or manifest"
        ),
        EcDeleteOutcome::SupersededByLive { live_hlc } => tracing::warn!(
            bucket = %object.bucket,
            key = %object.key,
            version_id = ?object.version_id,
            live_hlc = %live_hlc,
            "EC delete tombstone lost to a newer live manifest"
        ),
    }
}

/// Tally of one dry run.
///
/// The skip reasons are counted separately on purpose. "Nothing would migrate"
/// is not an answer an operator can act on; "everything is still warm" and
/// "everything is excluded by a prefix" lead to opposite next steps.
#[derive(Debug, Default)]
struct TieringDryRunReport {
    scanned: usize,
    would_migrate: usize,
    would_promote: usize,
    disabled: usize,
    not_current_version: usize,
    delete_marker: usize,
    already_erasure_coded: usize,
    too_small: usize,
    too_large: usize,
    still_warm: usize,
    excluded: usize,
    recently_migrated: usize,
}

impl TieringDryRunReport {
    fn record(&mut self, decision: &TieringDecision) {
        match decision {
            TieringDecision::Demote => self.would_migrate += 1,
            TieringDecision::Promote => self.would_promote += 1,
            TieringDecision::Skip(reason) => match reason {
                TieringSkipReason::Disabled => self.disabled += 1,
                TieringSkipReason::NotCurrentVersion => self.not_current_version += 1,
                TieringSkipReason::DeleteMarker => self.delete_marker += 1,
                TieringSkipReason::AlreadyErasureCoded => self.already_erasure_coded += 1,
                TieringSkipReason::TooSmall { .. } => self.too_small += 1,
                TieringSkipReason::TooLarge { .. } => self.too_large += 1,
                TieringSkipReason::StillWarm { .. } => self.still_warm += 1,
                TieringSkipReason::Excluded(_) => self.excluded += 1,
                TieringSkipReason::RecentlyMigrated { .. } => self.recently_migrated += 1,
            },
        }
    }

    fn skipped_counts(&self) -> Value {
        json!({
            "disabled": self.disabled,
            "not_current_version": self.not_current_version,
            "delete_marker": self.delete_marker,
            "already_erasure_coded": self.already_erasure_coded,
            "too_small": self.too_small,
            "too_large": self.too_large,
            "still_warm": self.still_warm,
            "excluded": self.excluded,
            "recently_migrated": self.recently_migrated
        })
    }
}

/// Add one per-object line to the dry-run report, up to the requested limit.
fn push_dry_run_sample(
    samples: &mut Vec<Value>,
    max_samples: usize,
    entry: &ListEntry,
    decision: &TieringDecision,
    idle_secs: u64,
) {
    if samples.len() >= max_samples {
        return;
    }
    samples.push(json!({
        "key": entry.key,
        "size": entry.size,
        "idle_secs": idle_secs,
        "decision": decision_label(decision)
    }));
}

fn decision_label(decision: &TieringDecision) -> &'static str {
    match decision {
        TieringDecision::Demote => "migrate",
        TieringDecision::Promote => "promote",
        TieringDecision::Skip(reason) => match reason {
            TieringSkipReason::Disabled => "disabled",
            TieringSkipReason::NotCurrentVersion => "not_current_version",
            TieringSkipReason::DeleteMarker => "delete_marker",
            TieringSkipReason::AlreadyErasureCoded => "already_erasure_coded",
            TieringSkipReason::TooSmall { .. } => "too_small",
            TieringSkipReason::TooLarge { .. } => "too_large",
            TieringSkipReason::StillWarm { .. } => "still_warm",
            TieringSkipReason::Excluded(_) => "excluded",
            TieringSkipReason::RecentlyMigrated { .. } => "recently_migrated",
        },
    }
}

/// The values actually in force for a bucket, as machine-readable JSON.
/// Render one tiering scan cycle for the admin surface.
///
/// Skip reasons stay broken out rather than summed: an operator who sees that
/// nothing migrated needs to know whether it was the thresholds, the
/// exclusions or the sizes, and a single number would not tell them.
fn tiering_cycle_snapshot(report: &EcTieringCycleReport) -> Value {
    json!({
        "scanned": report.scanned,
        "queued": report.queued,
        "queued_bytes": report.queued_bytes,
        // The reverse direction is reported next to the forward one because a
        // cycle does both, and an operator reading only "queued" would think a
        // sweep that spent its whole budget bringing objects back had done
        // nothing at all.
        "promoted": report.promoted,
        "promoted_bytes": report.promoted_bytes,
        "skipped": report.skipped,
        "vanished": report.vanished,
        "kept_warm_by_peer": report.kept_warm_by_peer,
        "skip_reasons": report.skip_reasons,
        "stop": tiering_cycle_stop_name(report.stop),
    })
}

/// Render one reverse migration cycle for the admin surface.
fn promotion_cycle_snapshot(report: &EcPromotionCycleReport) -> Value {
    json!({
        "examined": report.examined,
        "restored": report.restored,
        "promoted": report.promoted,
        "completed": report.completed,
        "superseded": report.superseded,
        "retried": report.retried,
        "failed": report.failed,
        "restored_bytes": report.restored_bytes,
        "swept_shards": report.swept_shards,
        "steps": report.steps,
    })
}

/// Stable name for why a cycle ended.
fn tiering_cycle_stop_name(stop: Option<EcTieringCycleStop>) -> &'static str {
    match stop {
        Some(EcTieringCycleStop::CatalogExhausted) => "catalog_exhausted",
        Some(EcTieringCycleStop::ScanLimit) => "scan_limit",
        Some(EcTieringCycleStop::EnqueueLimit) => "enqueue_limit",
        Some(EcTieringCycleStop::QueueFull) => "queue_full",
        Some(EcTieringCycleStop::PromoteLimit) => "promote_limit",
        Some(EcTieringCycleStop::Disabled) => "disabled",
        Some(EcTieringCycleStop::MixedCluster) => "mixed_cluster",
        None => "incomplete",
    }
}

fn tiering_policy_snapshot(policy: &TieringPolicy) -> Value {
    json!({
        "enabled": policy.enabled,
        "cold_after_secs": policy.cold_after.as_secs(),
        "min_object_size": policy.min_object_size,
        "max_object_size": policy.max_object_size,
        "promote_after_reads": policy.promote_after_reads,
        "promote_window_secs": policy.promote_window.as_secs(),
        "min_migration_interval_secs": policy.min_migration_interval.as_secs(),
        "jitter_percent": policy.jitter_percent,
        "exclude_prefixes": policy
            .exclusions
            .iter()
            .filter_map(|rule| match rule {
                crate::ec_tiering::policy::TieringRule::Prefix(prefix) => Some(prefix.clone()),
                crate::ec_tiering::policy::TieringRule::Tag { .. } => None,
            })
            .collect::<Vec<_>>(),
        "exclude_tags": policy
            .exclusions
            .iter()
            .filter_map(|rule| match rule {
                crate::ec_tiering::policy::TieringRule::Tag { key, value } =>
                    Some((key.clone(), value.clone())),
                crate::ec_tiering::policy::TieringRule::Prefix(_) => None,
            })
            .collect::<HashMap<_, _>>()
    })
}

/// Object tags as the S3 tagging API stores them.
///
/// Tags live as a JSON blob in one reserved metadata key rather than as
/// separate entries, so a policy that excludes by tag has to parse it. A blob
/// that fails to parse is treated as no tags: a malformed value must not make
/// an object migrate or stay by accident, and the safe answer is the one that
/// applies no exclusion the operator did not ask for.
fn metadata_tags(metadata: &HashMap<String, String>) -> HashMap<String, String> {
    metadata
        .get(OBJECT_TAGS_METADATA_KEY)
        .and_then(|raw| serde_json::from_str(raw).ok())
        .unwrap_or_default()
}

/// Send an access signal for a read that actually succeeded.
///
/// A failed or denied read is not use of an object, and counting it would keep
/// alive exactly the objects nobody can read. One client GET of an erasure
/// coded object produces two signals — the API asks for metadata first and the
/// body second — which the tracker collapses into one entry.
fn record_read_access(heat: &EcHeatTracker, key: Option<EcObjectVersionKey>, succeeded: bool) {
    if !succeeded {
        return;
    }
    if let Some(key) = key {
        heat.record_read(key);
    }
}

fn unix_time_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(u128::from(u64::MAX)) as u64
}

fn current_hlc(local_node_id: NodeId) -> HlcTimestamp {
    HlcTimestamp::from_record(unix_time_nanos() / 1_000_000, 0, local_node_id.0)
}

fn seconds_to_nanos(seconds: u64) -> u64 {
    seconds.saturating_mul(1_000_000_000)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{HashMap, HashSet};

    use s4_cluster::{EcObjectRuntime, PoolId};
    use s4_codec::EcProfileSnapshot;

    use crate::ec_placement::{EcCapacitySource, EcNodeCapacity};
    use crate::erasure_coding::{
        legacy_pool_set_id, EcCodecKind, EcFailureDomainLevel, EcParity, EcProfile,
        EcTopologyPolicy, ErasureSet, ErasureSetBuilder, ErasureSetState, ShardSlotAssignment,
        TopologyLabels, FIRST_SET_ID, MULTI_SET_ENABLED_ENV, PLACEMENT_ENV_LOCK,
        PLACEMENT_STRATEGY_ENV,
    };

    /// Every committed state an object can be in while reconciliation looks at
    /// it. `EcManifestCommitted` is the state before cutover finished, and the
    /// three after it are the ones `manifest_is_committed_live` accepts.
    const LIVE_STATES: [EcPlacementState; 4] = [
        EcPlacementState::EcManifestCommitted,
        EcPlacementState::EcCommitted,
        EcPlacementState::HotReplicasGcEligible,
        EcPlacementState::HotReplicasPurged,
    ];

    /// The whole reconciliation decision table, checked exhaustively.
    ///
    /// This is the pass's dangerous half: a wrong answer here either leaves an
    /// object unlistable or takes away hot copies the purge is still relying on.
    #[test]
    fn namespace_action_covers_every_state_and_observation() {
        for state in LIVE_STATES {
            // An entry that already says "erasure coded" is the healthy case,
            // whatever the manifest state.
            assert_eq!(
                namespace_action(state, ObservedEntry::ErasureCoded),
                EcNamespaceAction::Nothing,
                "state {state:?}"
            );

            // A different generation means the object was overwritten after the
            // manifest was written. The live index wins; the shards are GC's.
            assert_eq!(
                namespace_action(state, ObservedEntry::HotOtherGeneration),
                EcNamespaceAction::Superseded,
                "state {state:?}"
            );

            // No entry anywhere: the object exists only as shards, and without
            // an entry no client that enumerates the bucket can see it.
            assert_eq!(
                namespace_action(state, ObservedEntry::Missing),
                EcNamespaceAction::RestoreEntry,
                "state {state:?}"
            );
        }
    }

    /// A promoted object never asks this pass to take its hot copies away.
    ///
    /// After a reverse migration the object is hot copies plus a manifest of
    /// the same generation — the exact shape this pass reads as an unfinished
    /// cutover. It is not one, and releasing them would leave readers, already
    /// switched to the hot path, with nothing behind it. The pass is kept out
    /// of it twice: a promoted manifest is not "committed live" and never
    /// enters the loop, and the release branch confirms the layout with the
    /// pool before acting on a locally cached opinion.
    #[test]
    fn a_promoted_manifest_never_asks_for_hot_copies_to_be_released() {
        for observed in [
            ObservedEntry::ErasureCoded,
            ObservedEntry::HotSameGeneration,
            ObservedEntry::HotOtherGeneration,
            ObservedEntry::Missing,
        ] {
            assert_ne!(
                namespace_action(EcPlacementState::Promoted, observed),
                EcNamespaceAction::ReleaseHotCopies,
                "observed {observed:?}"
            );
        }

        let mut promoted = manifest_with_state(EcPlacementState::Promoted);
        promoted.is_deleted = false;
        assert!(
            !crate::ec_anti_entropy::manifest_is_committed_live(&promoted),
            "a promoted manifest must not be walked by namespace reconciliation at all"
        );
    }

    /// The purge gate, pinned down on its own.
    ///
    /// Hot copies of the manifest's own generation may only be released once
    /// the purge is finished. Before that they are what the purge's SHA-256
    /// verification and grace period still protect, and reconciliation taking
    /// them early would be invisible until a transcode went wrong.
    #[test]
    fn hot_copies_are_released_only_after_the_purge_is_done() {
        assert_eq!(
            namespace_action(
                EcPlacementState::HotReplicasPurged,
                ObservedEntry::HotSameGeneration
            ),
            EcNamespaceAction::ReleaseHotCopies
        );

        for state in [
            EcPlacementState::EcManifestCommitted,
            EcPlacementState::EcCommitted,
            EcPlacementState::HotReplicasGcEligible,
        ] {
            assert_eq!(
                namespace_action(state, ObservedEntry::HotSameGeneration),
                EcNamespaceAction::AwaitPurge,
                "reconciliation must not race the purge in state {state:?}"
            );
        }
    }

    /// The pool's word on whether hot copies are leftovers, case by case.
    ///
    /// Only a purged manifest of the same generation releases them. The case
    /// that matters most is the promotion marker: the local cache still says
    /// "purged", the HEAD sees hot copies of the same generation, and only the
    /// quorum knows they are being put back on purpose (EC2-026).
    #[test]
    fn hot_copies_are_released_only_when_the_quorum_still_says_purged() {
        let local = test_manifest();
        let at_quorum = |state: EcPlacementState| {
            let mut current = local.clone();
            current.state = state;
            EcManifestReadResult::Present(current)
        };

        assert!(quorum_says_hot_copies_are_leftovers(
            &at_quorum(EcPlacementState::HotReplicasPurged),
            &local
        ));

        for state in EcPlacementState::ALL {
            if state == EcPlacementState::HotReplicasPurged {
                continue;
            }
            assert!(
                !quorum_says_hot_copies_are_leftovers(&at_quorum(state), &local),
                "hot copies released while the quorum says {state:?}"
            );
        }

        let mut newer = local.clone();
        newer.hlc.wall_time += 1;
        assert!(!quorum_says_hot_copies_are_leftovers(
            &EcManifestReadResult::Present(newer),
            &local
        ));
        let mut deleted = local.clone();
        deleted.is_deleted = true;
        assert!(!quorum_says_hot_copies_are_leftovers(
            &EcManifestReadResult::Present(deleted.clone()),
            &local
        ));
        assert!(!quorum_says_hot_copies_are_leftovers(
            &EcManifestReadResult::Tombstone(deleted),
            &local
        ));
        assert!(!quorum_says_hot_copies_are_leftovers(
            &EcManifestReadResult::Absent,
            &local
        ));
    }

    /// One erasure-coded object, its replicas and its manifest at quorum, as
    /// the reverse-tiering model below sees them.
    struct ModelPool {
        /// The manifest the metadata quorum agrees on.
        quorum_state: EcPlacementState,
        /// Whether the replicas hold the object's bytes.
        hot_copies: bool,
        jobs: HashMap<String, EcPromotionJobRecord>,
        /// Readers were switched to the hot path while it had no bytes.
        switched_to_nothing: bool,
        /// The shards were released while the replicas had no bytes.
        lost: bool,
    }

    /// The promotion environment of the model: the pool's side of every step,
    /// with the manifest ranking the real pool applies.
    struct ModelPromotionEnvironment {
        pool: std::sync::Mutex<ModelPool>,
    }

    impl ModelPromotionEnvironment {
        fn pool(&self) -> std::sync::MutexGuard<'_, ModelPool> {
            self.pool.lock().expect("model pool")
        }
    }

    #[async_trait]
    impl EcPromotionEnvironment for ModelPromotionEnvironment {
        async fn ready_jobs(
            &self,
            now: u64,
            limit: usize,
        ) -> Result<Vec<EcPromotionJobRecord>, ClusterError> {
            Ok(self
                .pool()
                .jobs
                .values()
                .filter(|job| job.state.is_in_flight())
                .filter(|job| job.next_retry_at.is_none_or(|retry_at| retry_at <= now))
                .take(limit)
                .cloned()
                .collect())
        }

        async fn save_job(&self, job: &EcPromotionJobRecord) -> Result<(), ClusterError> {
            self.pool().jobs.insert(job.object.key.clone(), job.clone());
            Ok(())
        }

        async fn delete_job(&self, object: &EcObjectVersionKey) -> Result<(), ClusterError> {
            self.pool().jobs.remove(&object.key);
            Ok(())
        }

        async fn read_erasure_coded(
            &self,
            job: &EcPromotionJobRecord,
        ) -> Result<Option<Vec<u8>>, ClusterError> {
            let pool = self.pool();
            Ok((pool.quorum_state.is_ec_authoritative()
                && pool.quorum_state != EcPlacementState::Promoted)
                .then(|| vec![7u8; job.object_size as usize]))
        }

        async fn mark_promotion_in_flight(
            &self,
            _job: &EcPromotionJobRecord,
        ) -> Result<bool, ClusterError> {
            let mut pool = self.pool();
            match pool.quorum_state {
                EcPlacementState::Promoting | EcPlacementState::Promoted => Ok(true),
                state if state.is_ec_authoritative() => {
                    pool.quorum_state = EcPlacementState::Promoting;
                    Ok(true)
                }
                _ => Ok(false),
            }
        }

        async fn restore_hot_copy(
            &self,
            _job: &EcPromotionJobRecord,
            _data: &[u8],
        ) -> Result<bool, ClusterError> {
            self.pool().hot_copies = true;
            Ok(true)
        }

        async fn verify_hot_copy(&self, _job: &EcPromotionJobRecord) -> Result<bool, ClusterError> {
            Ok(self.pool().hot_copies)
        }

        async fn publish_promoted_manifest(
            &self,
            _job: &EcPromotionJobRecord,
        ) -> Result<bool, ClusterError> {
            let mut pool = self.pool();
            if !pool.hot_copies {
                pool.switched_to_nothing = true;
            }
            pool.quorum_state = EcPlacementState::Promoted;
            Ok(true)
        }

        async fn purge_erasure_coded_copy(
            &self,
            _job: &EcPromotionJobRecord,
        ) -> Result<bool, ClusterError> {
            let mut pool = self.pool();
            if !pool.hot_copies {
                pool.lost = true;
            }
            Ok(true)
        }

        async fn record_migration(
            &self,
            _object: &EcObjectVersionKey,
            _now: u64,
        ) -> Result<(), ClusterError> {
            Ok(())
        }

        async fn sweep_promoted_shards(&self, _now: u64) -> Result<usize, ClusterError> {
            Ok(0)
        }
    }

    /// One namespace reconciliation pass over the model, through the real
    /// decisions: the HEAD reduction, the action table and the quorum check.
    ///
    /// The node running it still caches the manifest as purged, which is what
    /// the node that ran the transcode holds until something refreshes it.
    fn reconcile_model(pool: &mut ModelPool, cached: &EcObjectManifest) {
        if !crate::ec_anti_entropy::manifest_is_committed_live(cached) {
            return;
        }
        let head = QuorumHeadResult {
            content_hash_sha256: None,
            etag: cached.etag.clone(),
            content_type: cached.content_type.clone(),
            size: cached.original_size,
            metadata: HashMap::new(),
            hlc: cached.hlc,
            version_id: None,
            erasure_coded: !pool.hot_copies,
            replicas_responded: 3,
        };
        if namespace_action(cached.state, observe_entry(&head, cached))
            != EcNamespaceAction::ReleaseHotCopies
        {
            return;
        }
        let mut current = cached.clone();
        current.state = pool.quorum_state;
        if quorum_says_hot_copies_are_leftovers(&EcManifestReadResult::Present(current), cached) {
            pool.hot_copies = false;
        }
    }

    /// Reverse tiering finishes at the production defaults, whatever the phase
    /// between the promotion worker and namespace reconciliation.
    ///
    /// This is the defect EC2-026 records, reproduced with the values a node
    /// starts with: one promotion step per 60-second pass, and a reconciliation
    /// pass every 60 seconds on the node that ran the transcode. Before the
    /// fix, the object's restored copies looked to that pass exactly like a
    /// purge's leftovers and were released on its first run — the promotion
    /// failed its read-back until its budget ran out, or, landing between the
    /// read-back and the switch, sent readers to replicas with no bytes and
    /// then let the shards go.
    ///
    /// The worker and every reconciliation decision are the real ones; only
    /// the I/O is modelled. Every one-second phase offset of the two loops is
    /// tried, so the result does not depend on where their clocks happen to
    /// start.
    #[tokio::test]
    async fn a_promotion_at_production_defaults_survives_namespace_reconciliation() {
        const SECOND: u64 = 1_000_000_000;
        let worker_config =
            promotion_worker_config(&EcTieringConfig::default(), &EcGcConfig::default());
        let promote_poll = worker_config.poll_interval.as_secs();
        let reconcile_every = EcIntegrityWorkerConfig::default().anti_entropy_interval.as_secs();
        let grace = worker_config.shard_grace.as_secs();
        let horizon = grace + 6 * 3_600;

        let cached = test_manifest();
        assert_eq!(cached.state, EcPlacementState::HotReplicasPurged);
        let object = cached.object_key();

        for phase in 0..reconcile_every {
            let job = EcPromotionJobRecord {
                object: object.clone(),
                operation_id: uuid::Uuid::from_u128(1),
                source_hlc: cached.hlc,
                erasure_set_id: cached.erasure_set_id,
                state: EcPromotionState::Queued,
                attempt: 0,
                object_size: 16,
                content_hash_sha256: cached.content_hash_sha256,
                origin: EcPromotionOrigin::Manual,
                shards_eligible_at: None,
                updated_at: 0,
                next_retry_at: None,
                failure: None,
            };
            let environment = Arc::new(ModelPromotionEnvironment {
                pool: std::sync::Mutex::new(ModelPool {
                    quorum_state: EcPlacementState::HotReplicasPurged,
                    hot_copies: false,
                    jobs: HashMap::from([(object.key.clone(), job)]),
                    switched_to_nothing: false,
                    lost: false,
                }),
            });
            let worker = EcPromotionWorker::new(
                worker_config.clone(),
                Arc::clone(&environment) as Arc<dyn EcPromotionEnvironment>,
            );

            let mut finished_at = None;
            for second in 1..=horizon {
                // Reconciliation first when both fall on the same second: of
                // the two orders, it is the one that meets a restored copy.
                if second >= phase && (second - phase) % reconcile_every == 0 {
                    reconcile_model(&mut environment.pool(), &cached);
                }
                if second % promote_poll == 0 {
                    worker.run_once(second * SECOND).await.unwrap();
                }
                if !environment.pool().jobs.contains_key(&object.key) {
                    finished_at = Some(second);
                    break;
                }
            }

            let pool = environment.pool();
            let job = pool.jobs.get(&object.key);
            assert!(
                finished_at.is_some(),
                "phase {phase}s: the promotion did not finish within {horizon}s; job ended as \
                 {:?} after {} attempt(s), last failure {:?}",
                job.map(|job| job.state),
                job.map_or(0, |job| job.attempt),
                job.and_then(|job| job.failure.clone()),
            );
            assert!(
                !pool.switched_to_nothing,
                "phase {phase}s: readers were switched to replicas with no bytes"
            );
            assert!(
                !pool.lost,
                "phase {phase}s: the shards went while the replicas had no bytes"
            );
            assert!(
                pool.hot_copies,
                "phase {phase}s: the object ended without its hot copies"
            );
            assert_eq!(
                pool.quorum_state,
                EcPlacementState::Promoted,
                "phase {phase}s"
            );
        }
    }

    /// The reduction from a quorum HEAD to the four cases the decision knows.
    #[test]
    fn observe_entry_distinguishes_the_four_cases() {
        let manifest = test_manifest();
        let head = |erasure_coded: bool, hlc: HlcTimestamp| QuorumHeadResult {
            content_hash_sha256: None,
            etag: "\"e\"".to_string(),
            content_type: "application/octet-stream".to_string(),
            size: 1,
            metadata: HashMap::new(),
            hlc,
            version_id: None,
            erasure_coded,
            replicas_responded: 3,
        };
        let other_hlc = HlcTimestamp {
            wall_time: manifest.hlc.wall_time + 1,
            logical: 0,
            node_id: manifest.hlc.node_id,
        };

        assert_eq!(
            observe_entry(&head(true, manifest.hlc), &manifest),
            ObservedEntry::ErasureCoded
        );
        assert_eq!(
            observe_entry(&head(false, manifest.hlc), &manifest),
            ObservedEntry::HotSameGeneration
        );
        assert_eq!(
            observe_entry(&head(false, other_hlc), &manifest),
            ObservedEntry::HotOtherGeneration
        );

        // An erasure-coded entry stays that case even when a later generation
        // exists: it is this object's namespace entry either way.
        assert_eq!(
            observe_entry(&head(true, other_hlc), &manifest),
            ObservedEntry::ErasureCoded
        );
    }

    /// The rebuilt entry has to carry everything a client can see, or a restored
    /// object would come back with the wrong size or ETag.
    #[test]
    fn namespace_entry_from_manifest_preserves_client_visible_fields() {
        let mut manifest = test_manifest();
        manifest
            .user_metadata
            .insert("x-amz-meta-owner".to_string(), "acceptance".to_string());

        let record = namespace_entry_from_manifest(&manifest);

        assert!(record.is_erasure_coded());
        assert_eq!(record.file_id, EXTERNAL_DATA_FILE_ID);
        assert_eq!(record.size, manifest.original_size);
        assert_eq!(record.etag, manifest.etag);
        assert_eq!(record.content_type, manifest.content_type);
        assert_eq!(record.content_hash, manifest.content_hash_sha256);
        assert_eq!(
            record.metadata.get("x-amz-meta-owner"),
            Some(&"acceptance".to_string())
        );
        // The generation must survive, or the next cutover check would refuse
        // to touch the record it just wrote.
        assert_eq!(record.hlc_timestamp, manifest.hlc.wall_time);
        assert_eq!(record.hlc_logical, manifest.hlc.logical);
        assert_eq!(record.origin_node_id, manifest.hlc.node_id);
        // Last-Modified comes from the HLC, in nanoseconds.
        assert_eq!(record.modified_at, manifest.hlc.wall_time * 1_000_000);
    }

    /// A purged manifest for the reconciliation tests.
    ///
    /// Only the fields the decision and the rebuilt entry read matter here, so
    /// the chunk list is left empty: nothing under test looks at shards.
    fn test_manifest() -> EcObjectManifest {
        let hlc = HlcTimestamp {
            wall_time: 1_700_000_000_000,
            logical: 4,
            node_id: 11,
        };
        EcObjectManifest {
            format_version: crate::ec_metadata::EC_MANIFEST_FORMAT_VERSION,
            bucket: "bucket".to_string(),
            key: "dir/object.bin".to_string(),
            version_id: None,
            original_size: 4096,
            content_hash_sha256: [9u8; 32],
            etag: "\"manifest-etag\"".to_string(),
            content_type: "application/octet-stream".to_string(),
            user_metadata: HashMap::new(),
            profile: EcProfileSnapshot::rs_small(),
            erasure_set_id: 7,
            topology_epoch: 3,
            chunks: Vec::new(),
            state: EcPlacementState::HotReplicasPurged,
            hlc,
            origin_node_id: NodeId(11),
            operation_id: uuid::Uuid::nil(),
            created_at: 42,
            is_deleted: false,
            packed: None,
        }
    }

    /// Serializes tests that manipulate `S4_EC_TIERING_*`. `set_var`/`remove_var`
    /// affect the whole process, so parallel tests race.
    ///
    /// The lock covers two kinds of caller, and both have to take it: the tests
    /// that set these variables, and every test that opens an
    /// `EcOperationsService`, because opening one parses them. A test that
    /// deliberately sets an invalid value would otherwise break whichever
    /// unrelated test happened to open a service at the same moment.
    static TIERING_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// Clear every `S4_EC_TIERING_*` variable.
    ///
    /// Keys are collected first so the environment is not mutated while it is
    /// being iterated, and the prefix match keeps this correct as plan 02 adds
    /// further variables.
    fn clear_tiering_env() {
        let keys: Vec<String> = std::env::vars()
            .map(|(key, _)| key)
            // The promoted-grace setting is cleared with them: it is not named
            // `S4_EC_TIERING_*`, but it is cross-checked against one of them,
            // so a test that sets it would otherwise leak into the next.
            //
            // The placement and packing settings are cleared for the same
            // reason: opening a service parses them too, so a test that
            // switches multi-set placement or small-object packing on must not
            // leave it on for the next one.
            .filter(|key| {
                key.starts_with("S4_EC_TIERING_")
                    || key.starts_with("S4_EC_PACKED_SEGMENT")
                    || key == "S4_EC_PROMOTED_GC_GRACE_SECS"
                    || key == MULTI_SET_ENABLED_ENV
                    || key == PLACEMENT_STRATEGY_ENV
                    || key == MINIMAL_READ_ENABLED_ENV
            })
            .collect();
        for key in keys {
            std::env::remove_var(key);
        }
    }

    /// RAII guard that serializes tiering env access and clears it on both ends.
    ///
    /// Clearing on drop is the load-bearing part: a test that panics between
    /// `set_var` and its cleanup would otherwise leave the environment dirty for
    /// every test that follows.
    ///
    /// It holds the placement lock as well, because opening a service parses the
    /// placement settings too. The two locks are always taken in this order and
    /// never in the other, so a test module that needs only the placement one
    /// cannot deadlock against this guard.
    struct TieringEnvGuard {
        _lock: std::sync::MutexGuard<'static, ()>,
        _placement_lock: std::sync::MutexGuard<'static, ()>,
    }

    impl TieringEnvGuard {
        fn acquire() -> Self {
            let lock = TIERING_ENV_LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            let placement_lock =
                PLACEMENT_ENV_LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            clear_tiering_env();
            Self {
                _lock: lock,
                _placement_lock: placement_lock,
            }
        }
    }

    impl Drop for TieringEnvGuard {
        fn drop(&mut self) {
            clear_tiering_env();
        }
    }

    /// A small object's placement, for the tests below.
    fn small_placement(nodes: [u128; 3]) -> ReplicatedSmallObjectPlacement {
        ReplicatedSmallObjectPlacement::new(
            nodes.into_iter().map(NodeId).collect(),
            [7u8; 32],
            1_024,
            HlcTimestamp {
                wall_time: 1_700_000_000_000,
                logical: 1,
                node_id: nodes[0],
            },
        )
        .unwrap()
    }

    /// A tombstone manifest for the sweep tests: no chunks, deleted, ordered.
    fn sweep_tombstone(key: &str) -> EcObjectManifest {
        let mut manifest = test_manifest();
        manifest.key = key.to_string();
        manifest.chunks.clear();
        manifest.state = EcPlacementState::Tombstone;
        manifest.is_deleted = true;
        manifest.operation_id = Uuid::from_u128(0x5eed_0000_0000_0000_0000_0000_0000_0001);
        manifest
    }

    /// EC2-202: a cycle walks a slice of the manifests and the next one carries
    /// on from there.
    ///
    /// The sweep looks for tombstones among every manifest the node holds, so
    /// its cost is the walk. Before this each cycle re-read the whole keyspace
    /// — every five minutes, for the life of the node — to find the same few
    /// deletes. Here three cycles cover six manifests two at a time, the round
    /// then closes, and the fourth cycle starts over.
    #[tokio::test]
    async fn the_tombstone_sweep_walks_a_slice_and_the_next_cycle_carries_on() {
        let tmp = tempfile::tempdir().unwrap();
        let service = {
            let _guard = TieringEnvGuard::acquire();
            std::env::set_var("S4_EC_GC_SCAN_LIMIT", "2");
            std::env::set_var("S4_EC_TOMBSTONE_GC_SCAN_BUDGET", "2");
            let service = EcOperationsService::open(
                tmp.path(),
                "ec",
                active_set_registry(1),
                test_set_builder(1),
                NodeId(1),
            )
            .unwrap();
            std::env::remove_var("S4_EC_GC_SCAN_LIMIT");
            std::env::remove_var("S4_EC_TOMBSTONE_GC_SCAN_BUDGET");
            service
        };
        for index in 0..6 {
            service
                .metadata
                .put_manifest(&sweep_tombstone(&format!("deleted-{index}.bin")))
                .await
                .unwrap();
        }

        let mut seen = std::collections::BTreeSet::new();
        let mut examined_per_cycle = Vec::new();
        for _ in 0..3 {
            let scan = service
                .scan_local_tombstones(64, TombstoneScanStart::WhereTheLastCycleStopped)
                .await
                .unwrap();
            examined_per_cycle.push(scan.manifests_examined);
            for tombstone in &scan.tombstones {
                seen.insert(tombstone.key.clone());
            }
        }

        assert_eq!(
            examined_per_cycle,
            vec![2, 2, 2],
            "each cycle walks its budget, not the whole keyspace"
        );
        assert_eq!(
            seen.len(),
            6,
            "three cycles between them covered every delete"
        );

        // The third cycle reached the end, so the round closed and the cursor
        // went back to the start: the walk is a loop, not a one-way scan.
        let cursor = service.metadata.get_tombstone_gc_cursor().await.unwrap().unwrap();
        assert_eq!(cursor.continuation, None);
        assert_eq!(cursor.rounds_completed, 1);

        let fourth = service
            .scan_local_tombstones(64, TombstoneScanStart::WhereTheLastCycleStopped)
            .await
            .unwrap();
        assert_eq!(fourth.tombstones.len(), 2, "the next round starts over");
        assert!(!fourth.round_completed);
    }

    /// An operator asking for a sweep gets the whole keyspace, not the slice the
    /// background cycle happens to be on.
    #[tokio::test]
    async fn an_asked_for_sweep_starts_at_the_first_manifest() {
        let tmp = tempfile::tempdir().unwrap();
        let service = {
            let _guard = TieringEnvGuard::acquire();
            EcOperationsService::open(
                tmp.path(),
                "ec",
                active_set_registry(1),
                test_set_builder(1),
                NodeId(1),
            )
            .unwrap()
        };
        for index in 0..4 {
            service
                .metadata
                .put_manifest(&sweep_tombstone(&format!("deleted-{index}.bin")))
                .await
                .unwrap();
        }

        let asked = service
            .scan_local_tombstones(64, TombstoneScanStart::Cursor(None))
            .await
            .unwrap();

        assert_eq!(asked.tombstones.len(), 4);
        assert!(asked.round_completed);
        // And it left no position behind: the background cycle owns the cursor.
        assert!(service.metadata.get_tombstone_gc_cursor().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn tombstone_scan_never_reads_past_its_manifest_budget() {
        let tmp = tempfile::tempdir().unwrap();
        let service = {
            let _guard = TieringEnvGuard::acquire();
            std::env::set_var("S4_EC_GC_SCAN_LIMIT", "4");
            std::env::set_var("S4_EC_TOMBSTONE_GC_SCAN_BUDGET", "1");
            let service = EcOperationsService::open(
                tmp.path(),
                "ec",
                active_set_registry(1),
                test_set_builder(1),
                NodeId(1),
            )
            .unwrap();
            std::env::remove_var("S4_EC_GC_SCAN_LIMIT");
            std::env::remove_var("S4_EC_TOMBSTONE_GC_SCAN_BUDGET");
            service
        };
        for index in 0..4 {
            service
                .metadata
                .put_manifest(&sweep_tombstone(&format!("deleted-{index}.bin")))
                .await
                .unwrap();
        }

        let first = service
            .scan_local_tombstones(4, TombstoneScanStart::Cursor(None))
            .await
            .unwrap();

        assert_eq!(first.manifests_examined, 1);
        assert_eq!(first.tombstones.len(), 1);
        assert!(first.next_cursor.is_some());
        assert!(!first.round_completed);
    }

    #[tokio::test]
    async fn tombstone_scan_cursor_does_not_skip_candidates_after_max_entries() {
        let tmp = tempfile::tempdir().unwrap();
        let service = EcOperationsService::open(
            tmp.path(),
            "ec",
            active_set_registry(1),
            test_set_builder(1),
            NodeId(1),
        )
        .unwrap();
        for index in 0..4 {
            service
                .metadata
                .put_manifest(&sweep_tombstone(&format!("deleted-{index}.bin")))
                .await
                .unwrap();
        }

        let first = service
            .scan_local_tombstones(2, TombstoneScanStart::Cursor(None))
            .await
            .unwrap();
        let second = service
            .scan_local_tombstones(2, TombstoneScanStart::Cursor(first.next_cursor.clone()))
            .await
            .unwrap();
        let keys = first
            .tombstones
            .iter()
            .chain(&second.tombstones)
            .map(|manifest| manifest.key.as_str())
            .collect::<std::collections::BTreeSet<_>>();

        assert_eq!(keys.len(), 4);
        assert_eq!(first.manifests_examined, 2);
        assert_eq!(second.manifests_examined, 2);
        assert!(second.round_completed);
    }

    #[tokio::test]
    async fn tombstone_scan_crosses_mixed_pages_without_losing_tombstones() {
        let tmp = tempfile::tempdir().unwrap();
        let service = {
            let _guard = TieringEnvGuard::acquire();
            std::env::set_var("S4_EC_GC_SCAN_LIMIT", "4");
            let service = EcOperationsService::open(
                tmp.path(),
                "ec",
                active_set_registry(1),
                test_set_builder(1),
                NodeId(1),
            )
            .unwrap();
            std::env::remove_var("S4_EC_GC_SCAN_LIMIT");
            service
        };
        for index in 0..8 {
            let mut manifest = if index % 2 == 0 {
                sweep_tombstone(&format!("item-{index}"))
            } else {
                let mut live = manifest_with_state(EcPlacementState::HotReplicasPurged);
                live.key = format!("item-{index}");
                live.operation_id =
                    Uuid::from_u128(0x5eed_1000_0000_0000_0000_0000_0000_0000 + index as u128);
                live
            };
            manifest.created_at = u64::try_from(index).unwrap();
            service.metadata.put_manifest(&manifest).await.unwrap();
        }

        let mut cursor = None;
        let mut keys = std::collections::BTreeSet::new();
        let mut examined = 0usize;
        loop {
            let scan = service
                .scan_local_tombstones(2, TombstoneScanStart::Cursor(cursor))
                .await
                .unwrap();
            examined += scan.manifests_examined;
            keys.extend(scan.tombstones.iter().map(|manifest| manifest.key.clone()));
            if scan.round_completed {
                break;
            }
            cursor = scan.next_cursor;
        }

        assert_eq!(
            keys,
            std::collections::BTreeSet::from([
                "item-0".to_string(),
                "item-2".to_string(),
                "item-4".to_string(),
                "item-6".to_string(),
            ])
        );
        assert_eq!(examined, 8);
    }

    #[tokio::test]
    async fn manual_tombstone_cursor_survives_restart_and_deleted_boundary_key() {
        let _guard = TieringEnvGuard::acquire();
        let tmp = tempfile::tempdir().unwrap();
        let first_cursor = {
            let service = EcOperationsService::open(
                tmp.path(),
                "ec",
                active_set_registry(1),
                test_set_builder(1),
                NodeId(1),
            )
            .unwrap();
            for index in 0..4 {
                service
                    .metadata
                    .put_manifest(&sweep_tombstone(&format!("deleted-{index}")))
                    .await
                    .unwrap();
            }
            let first = service
                .scan_local_tombstones(1, TombstoneScanStart::Cursor(None))
                .await
                .unwrap();
            let boundary = first.tombstones[0].object_key();
            service.metadata.delete_manifest(&boundary).await.unwrap();
            first.next_cursor.unwrap()
        };
        let service = EcOperationsService::open(
            tmp.path(),
            "ec",
            active_set_registry(1),
            test_set_builder(1),
            NodeId(1),
        )
        .unwrap();

        let rest = service
            .scan_local_tombstones(4, TombstoneScanStart::Cursor(Some(first_cursor)))
            .await
            .unwrap();

        assert_eq!(rest.tombstones.len(), 3);
        assert!(rest.round_completed);
        assert!(service.metadata.get_tombstone_gc_cursor().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn manual_gc_pages_continue_past_young_tombstones_to_mature_ones() {
        let _guard = TieringEnvGuard::acquire();
        let tmp = tempfile::tempdir().unwrap();
        let service = EcOperationsService::open(
            tmp.path(),
            "ec",
            active_set_registry(1),
            test_set_builder(1),
            NodeId(1),
        )
        .unwrap();
        for index in 0..6 {
            let mut tombstone = sweep_tombstone(&format!("deleted-{index}"));
            tombstone.created_at = if index < 2 { u64::MAX } else { 1 };
            service.metadata.put_manifest(&tombstone).await.unwrap();
        }

        let mut cursor = None;
        let mut scanned = 0u64;
        let mut past_grace = 0u64;
        let mut pages = 0usize;
        loop {
            let page = service
                .run_tombstone_gc(EcAdminTombstoneGcRequest {
                    dry_run: true,
                    max_entries: 2,
                    cursor,
                })
                .await
                .unwrap();
            pages += 1;
            scanned += page["tombstones_scanned"].as_u64().unwrap();
            past_grace += page["tombstones_past_grace"].as_u64().unwrap();
            if page["round_completed"].as_bool().unwrap() {
                assert!(page["next_cursor"].is_null());
                break;
            }
            cursor = Some(page["next_cursor"].as_str().unwrap().to_string());
        }

        assert_eq!(pages, 3);
        assert_eq!(scanned, 6);
        assert_eq!(past_grace, 4);
        assert!(service.metadata.get_tombstone_gc_cursor().await.unwrap().is_none());
    }

    #[test]
    fn malformed_tombstone_gc_cursor_is_rejected_as_a_request_error() {
        let request = EcAdminTombstoneGcRequest {
            dry_run: true,
            max_entries: 1,
            cursor: Some("not-hex".to_string()),
        };

        let error = request.validate().unwrap_err();
        assert!(matches!(error, ClusterError::Config(_)));
        assert!(error.to_string().contains("cursor"));
    }

    /// The walk budget is a number an operator sets, and zero is not one.
    #[test]
    fn the_tombstone_scan_budget_has_to_be_a_number_of_manifests() {
        let _guard = TieringEnvGuard::acquire();
        std::env::remove_var("S4_EC_TOMBSTONE_GC_SCAN_BUDGET");
        assert_eq!(
            EcGcConfig::from_env().unwrap().tombstone_scan_budget,
            crate::ec_gc::DEFAULT_EC_TOMBSTONE_GC_SCAN_BUDGET
        );

        std::env::set_var("S4_EC_TOMBSTONE_GC_SCAN_BUDGET", "0");
        let error = EcGcConfig::from_env().unwrap_err().to_string();
        std::env::remove_var("S4_EC_TOMBSTONE_GC_SCAN_BUDGET");
        assert!(
            error.contains("S4_EC_TOMBSTONE_GC_SCAN_BUDGET"),
            "unexpected error: {error}"
        );
    }

    /// A metadata coordinator that counts how often it was asked.
    ///
    /// The cost of a `404` is the number of quorum reads it takes, and that is
    /// a number only a counting stand can report.
    struct CountingMetadataCoordinator {
        answer: EcManifestReadResult,
        reads: std::sync::atomic::AtomicUsize,
    }

    impl CountingMetadataCoordinator {
        fn new(answer: EcManifestReadResult) -> Self {
            Self {
                answer,
                reads: std::sync::atomic::AtomicUsize::new(0),
            }
        }

        fn reads(&self) -> usize {
            self.reads.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl crate::ec_read::EcReadMetadataCoordinator for CountingMetadataCoordinator {
        async fn quorum_read_object_layout(
            &self,
            _object: &EcObjectVersionKey,
        ) -> Result<crate::ec_read::EcObjectLayoutReadResult, EcReadError> {
            Ok(crate::ec_read::EcObjectLayoutReadResult::Absent)
        }

        async fn quorum_read_manifest(
            &self,
            _object: &EcObjectVersionKey,
        ) -> Result<EcManifestReadResult, EcReadError> {
            self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(self.answer.clone())
        }
    }

    /// EC2-201: a read that came back empty costs one quorum read to explain,
    /// and the explanation is right.
    ///
    /// Before this it re-ran the whole plan: another quorum read, a quorum
    /// `HEAD` and a placement resolve — none of which bear on the only question
    /// being asked, which is whether a tombstone exists.
    #[tokio::test]
    async fn explaining_a_missing_object_costs_one_quorum_read() {
        let tmp = tempfile::tempdir().unwrap();
        let service = {
            let _guard = TieringEnvGuard::acquire();
            EcOperationsService::open(
                tmp.path(),
                "ec",
                active_set_registry(1),
                test_set_builder(1),
                NodeId(1),
            )
            .unwrap()
        };
        let object = EcObjectVersionKey::new("photos".to_string(), "gone.bin".to_string(), None);
        let not_found = || EcReadError::NotFound {
            bucket: object.bucket.clone(),
            key: object.key.clone(),
            version_id: object.version_id.clone(),
        };

        // A tombstone turns "not found" into "deleted", in one read.
        let deleted = CountingMetadataCoordinator::new(EcManifestReadResult::Tombstone(
            sweep_tombstone("gone.bin"),
        ));
        let answer = service.explain_missing_object(&object, &deleted, not_found()).await;
        assert!(matches!(answer, EcObjectReadError::Deleted { .. }));
        assert_eq!(deleted.reads(), 1, "one quorum read, not a second plan");

        // Anything else is left as it came, and still costs one read.
        let absent = CountingMetadataCoordinator::new(EcManifestReadResult::Absent);
        let answer = service.explain_missing_object(&object, &absent, not_found()).await;
        assert!(matches!(answer, EcObjectReadError::NotFound { .. }));
        assert_eq!(absent.reads(), 1);

        // A failure that is not "not found" is not explained at all: nothing is
        // asked of the pool, because nothing about it is in question.
        let unavailable = CountingMetadataCoordinator::new(EcManifestReadResult::Absent);
        let answer = service
            .explain_missing_object(
                &object,
                &unavailable,
                EcReadError::Unavailable {
                    reason: "quorum not met".to_string(),
                },
            )
            .await;
        assert!(!matches!(answer, EcObjectReadError::Deleted { .. }));
        assert_eq!(
            unavailable.reads(),
            0,
            "an unreachable pool is not a delete"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn placement_resolution_counts_fallback_attempts_and_sources() {
        let replicas = vec![NodeId(1), NodeId(2), NodeId(3)];
        let rest = vec![NodeId(4), NodeId(5), NodeId(6)];
        let pool: Vec<_> = replicas.iter().chain(&rest).copied().collect();
        let placement = ReplicatedSmallObjectPlacement::new(
            replicas.clone(),
            [42; 32],
            12,
            HlcTimestamp::from_record(100, 0, 1),
        )
        .unwrap();
        for (case, nodes, answers, expected_calls, expected_source, expected_attempts) in [
            (
                "owners hit",
                pool.clone(),
                vec![Some(placement.clone())],
                vec![replicas.clone()],
                "replicas",
                0,
            ),
            (
                "rest hit",
                pool.clone(),
                vec![None, Some(placement.clone())],
                vec![replicas.clone(), rest.clone()],
                "pool_fanout",
                1,
            ),
            (
                "full miss",
                pool,
                vec![None, None],
                vec![replicas.clone(), rest],
                "absent",
                1,
            ),
            (
                "empty rest",
                replicas.clone(),
                vec![None, None],
                vec![replicas.clone(), vec![]],
                "absent",
                0,
            ),
        ] {
            let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
            let _guard = metrics::set_default_local_recorder(&recorder);
            ec_metrics::init_small_placement_metrics();
            let expected_result = answers.iter().find_map(Clone::clone);
            let mut answers = answers.into_iter();
            let mut calls = Vec::new();
            let result = resolve_small_placement_with(replicas.clone(), nodes, |nodes| {
                calls.push(nodes);
                std::future::ready(answers.next().expect("one answer per expected call"))
            })
            .await;
            assert_eq!(result, expected_result, "{case}");
            assert_eq!(calls, expected_calls, "{case}");
            let rendered = recorder.handle().render();
            let value = |series: &str| -> u64 {
                rendered
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(' ')?;
                        (name == series).then(|| value.parse().unwrap())
                    })
                    .unwrap_or_else(|| panic!("{case}: missing metric {series}: {rendered}"))
            };
            for source in ["replicas", "pool_fanout", "absent"] {
                assert_eq!(
                    value(&format!(
                        "s4_ec_small_placement_resolves_total{{source=\"{source}\"}}"
                    )),
                    u64::from(source == expected_source),
                    "{case}: {source}"
                );
            }
            assert_eq!(
                value("s4_ec_small_placement_fallback_attempts_total"),
                expected_attempts,
                "{case}",
            );
        }
    }

    /// EC2-201: the owners are asked first, and nobody is asked twice.
    ///
    /// The fallback exists for a record that never reached its owners, so it
    /// must cover the rest of the pool — and only the rest: asking the owners
    /// again would make it cost more than the fan-out it replaced.
    #[test]
    fn the_placement_fallback_asks_the_rest_of_the_pool_and_nobody_twice() {
        let replicas = vec![NodeId(1), NodeId(2), NodeId(3)];
        let pool = vec![
            NodeId(1),
            NodeId(2),
            NodeId(3),
            NodeId(4),
            NodeId(5),
            NodeId(6),
        ];

        assert_eq!(
            placement_fallback_nodes(&replicas, pool.clone()),
            vec![NodeId(4), NodeId(5), NodeId(6)]
        );

        // A pool that is only its replicas has no fallback to make: the owners
        // have answered, and there is nobody else to ask.
        assert!(placement_fallback_nodes(&replicas, replicas.clone()).is_empty());

        // And an owner outside this node's view of the pool does not conjure
        // one: the fallback is the pool minus the owners, never the other way.
        assert_eq!(placement_fallback_nodes(&[NodeId(9)], pool.clone()), pool);
    }

    /// An owner that answers the batch call, one that does not, and a count of
    /// what each was asked.
    struct RecordingOwnerProbe {
        /// Owners that refuse the batch call, as a node too old to know it does.
        old_nodes: HashSet<NodeId>,
        unavailable_nodes: HashSet<NodeId>,
        single_delay: Duration,
        batch_delay: Duration,
        unknown_legacy_nodes: HashSet<NodeId>,
        /// Which delete each owner actually holds, by key.
        held: HashMap<NodeId, HashSet<String>>,
        batch_calls: std::sync::Mutex<Vec<NodeId>>,
        single_calls: std::sync::Mutex<Vec<(NodeId, String)>>,
    }

    impl RecordingOwnerProbe {
        fn new(old_nodes: &[NodeId], held: &[(NodeId, &[&str])]) -> Self {
            Self {
                old_nodes: old_nodes.iter().copied().collect(),
                unavailable_nodes: HashSet::new(),
                single_delay: Duration::ZERO,
                batch_delay: Duration::ZERO,
                unknown_legacy_nodes: HashSet::new(),
                held: held
                    .iter()
                    .map(|(owner, keys)| {
                        (*owner, keys.iter().map(|key| (*key).to_string()).collect())
                    })
                    .collect(),
                batch_calls: std::sync::Mutex::new(Vec::new()),
                single_calls: std::sync::Mutex::new(Vec::new()),
            }
        }

        fn holds(&self, owner: NodeId, tombstone: &EcObjectManifest) -> bool {
            self.held.get(&owner).is_some_and(|keys| keys.contains(&tombstone.key))
        }
    }

    #[async_trait]
    impl EcTombstoneOwnerProbe for RecordingOwnerProbe {
        async fn probe_batch(
            &self,
            owner: NodeId,
            tombstones: &[EcObjectManifest],
        ) -> Result<Vec<bool>, ClusterError> {
            self.batch_calls.lock().unwrap().push(owner);
            tokio::time::sleep(self.batch_delay).await;
            if self.unavailable_nodes.contains(&owner) {
                return Err(ClusterError::Rpc("transport unavailable".into()));
            }
            if self.old_nodes.contains(&owner) {
                return Err(ClusterError::RpcUnimplemented(format!(
                    "probe EC tombstones on {owner}: status: Unimplemented"
                )));
            }
            Ok(tombstones.iter().map(|tombstone| self.holds(owner, tombstone)).collect())
        }

        async fn probe_one(&self, owner: NodeId, tombstone: &EcObjectManifest) -> Option<bool> {
            self.single_calls.lock().unwrap().push((owner, tombstone.key.clone()));
            tokio::time::sleep(self.single_delay).await;
            if self.unknown_legacy_nodes.contains(&owner) {
                return None;
            }
            Some(self.holds(owner, tombstone))
        }
    }

    /// EC2-202: one request per owner, and an owner that cannot answer it is
    /// asked the old way — to the same answer.
    ///
    /// The fallback is the path a cluster takes exactly once, while it is being
    /// upgraded, so it is the path least likely to be exercised and most likely
    /// to be wrong. Both halves are checked here against the same expected
    /// result, because "cheaper" is only worth anything if it is also "same".
    #[tokio::test]
    async fn unavailable_tombstone_owners_do_not_trigger_per_delete_fallback() {
        let mut probe = RecordingOwnerProbe::new(&[], &[]);
        probe.unavailable_nodes.insert(NodeId(2));
        let probe = Arc::new(probe);
        let deletes = [sweep_tombstone("a"), sweep_tombstone("b")];
        let answers = poll_tombstones_by_owner(
            Arc::clone(&probe) as Arc<dyn EcTombstoneOwnerProbe>,
            &[NodeId(2)],
            &deletes.iter().collect::<Vec<_>>(),
        )
        .await;
        assert!(
            !answers.contains_key(&NodeId(2)),
            "an unavailable owner must not be confused with an owner that answered not-held"
        );
        assert!(
            probe.single_calls.lock().unwrap().is_empty(),
            "a network failure must not multiply into one RPC per delete"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn tombstone_owners_share_batch_and_legacy_deadline() {
        let mut probe = RecordingOwnerProbe::new(&[NodeId(1), NodeId(2)], &[(NodeId(3), &["a"])]);
        probe.batch_delay = Duration::from_secs(6);
        probe.single_delay = Duration::from_secs(6);
        let probe = Arc::new(probe);
        let deletes = [sweep_tombstone("a"), sweep_tombstone("b")];
        let start = tokio::time::Instant::now();
        let answers = poll_tombstones_by_owner(
            Arc::clone(&probe) as Arc<dyn EcTombstoneOwnerProbe>,
            &[NodeId(1), NodeId(2), NodeId(3)],
            &deletes.iter().collect::<Vec<_>>(),
        )
        .await;
        assert_eq!(start.elapsed(), Duration::from_secs(10));
        assert_eq!(answers.len(), 1);
        assert_eq!(answers[&NodeId(3)].held, HashSet::from([0]));
        assert_eq!(answers[&NodeId(3)].missing, HashSet::from([1]));
        assert_eq!(probe.single_calls.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn unknown_legacy_tombstone_owner_does_not_allow_handoff_or_more_probes() {
        let mut probe = RecordingOwnerProbe::new(&[NodeId(1), NodeId(2)], &[]);
        probe.unknown_legacy_nodes.insert(NodeId(2));
        let probe = Arc::new(probe);
        let deletes = [sweep_tombstone("a"), sweep_tombstone("b")];
        let answers = poll_tombstones_by_owner(
            Arc::clone(&probe) as Arc<dyn EcTombstoneOwnerProbe>,
            &[NodeId(1), NodeId(2)],
            &deletes.iter().collect::<Vec<_>>(),
        )
        .await;
        assert!(!answers.contains_key(&NodeId(2)));
        assert!(answers[&NodeId(1)].held.is_empty());
        assert_eq!(answers[&NodeId(1)].missing, HashSet::from([0, 1]));
        assert_eq!(
            probe
                .single_calls
                .lock()
                .unwrap()
                .iter()
                .filter(|(node, _)| *node == NodeId(2))
                .count(),
            1
        );
    }

    #[tokio::test(start_paused = true)]
    async fn old_tombstone_owner_fallback_has_one_deadline_for_the_entire_batch() {
        let mut probe = RecordingOwnerProbe::new(&[NodeId(2)], &[(NodeId(2), &["a", "b", "c"])]);
        probe.single_delay = Duration::from_secs(6);
        let probe = Arc::new(probe);
        let deletes = [
            sweep_tombstone("a"),
            sweep_tombstone("b"),
            sweep_tombstone("c"),
        ];
        let start = tokio::time::Instant::now();
        let answers = poll_tombstones_by_owner(
            Arc::clone(&probe) as Arc<dyn EcTombstoneOwnerProbe>,
            &[NodeId(2)],
            &deletes.iter().collect::<Vec<_>>(),
        )
        .await;
        assert!(start.elapsed() <= Duration::from_secs(10));
        assert_eq!(answers[&NodeId(2)].held, HashSet::from([0]));
        assert!(answers[&NodeId(2)].missing.is_empty());
        assert_eq!(probe.single_calls.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn owners_are_asked_once_each_and_an_old_owner_is_asked_the_old_way() {
        let deletes =
            ["a.bin", "b.bin", "c.bin"].into_iter().map(sweep_tombstone).collect::<Vec<_>>();
        let ordered = deletes.iter().collect::<Vec<_>>();
        let owners = [NodeId(1), NodeId(2), NodeId(3)];

        // Node 2 is mid-upgrade and refuses the batch call. Every owner holds
        // a different subset, so a mixed-up answer cannot pass unnoticed.
        let probe = Arc::new(RecordingOwnerProbe::new(
            &[NodeId(2)],
            &[
                (NodeId(1), &["a.bin", "b.bin", "c.bin"]),
                (NodeId(2), &["a.bin", "c.bin"]),
                (NodeId(3), &["b.bin"]),
            ],
        ));

        let answers = poll_tombstones_by_owner(
            Arc::clone(&probe) as Arc<dyn EcTombstoneOwnerProbe>,
            &owners,
            &ordered,
        )
        .await;

        assert_eq!(answers[&NodeId(1)].held, HashSet::from([0, 1, 2]));
        assert_eq!(
            answers[&NodeId(2)].held,
            HashSet::from([0, 2]),
            "the old way, same answer"
        );
        assert_eq!(answers[&NodeId(3)].held, HashSet::from([1]));
        assert_eq!(answers[&NodeId(2)].missing, HashSet::from([1]));
        assert_eq!(answers[&NodeId(3)].missing, HashSet::from([0, 2]));

        // Every owner was asked once in a batch — including the one that could
        // not answer it, because that is how it says so.
        let mut batched = probe.batch_calls.lock().unwrap().clone();
        batched.sort_by_key(|node| node.0);
        assert_eq!(batched, vec![NodeId(1), NodeId(2), NodeId(3)]);

        // And only that owner fell back, once per delete.
        let singles = probe.single_calls.lock().unwrap().clone();
        assert_eq!(
            singles.len(),
            3,
            "three deletes, asked one at a time: {singles:?}"
        );
        assert!(
            singles.iter().all(|(owner, _)| *owner == NodeId(2)),
            "an owner that answered the batch must not be asked again: {singles:?}"
        );
    }

    /// EC2-201: a placement record survives the node that wrote it only if a
    /// majority of the replicas holds it.
    #[test]
    fn a_placement_record_needs_a_majority_of_the_replicas() {
        assert_eq!(
            small_placement_quorum(3),
            2,
            "RF=3 tolerates one silent replica"
        );
        assert_eq!(small_placement_quorum(1), 1);
        assert_eq!(small_placement_quorum(2), 2);
        assert_eq!(small_placement_quorum(5), 3);
    }

    /// EC2-201: a write short of a majority records what it owes, and the debt
    /// is cleared the moment it is paid.
    ///
    /// The `PUT` itself is never failed for this: the object's bytes are
    /// already durable on their replicas, and a client is owed its `200`. What
    /// must not happen is the shortfall being forgotten — which is what a lone
    /// `warn!` amounted to before.
    #[tokio::test]
    async fn old_placement_ack_cannot_clear_a_newer_placement_hint() {
        let _guard = TieringEnvGuard::acquire();
        let tmp = tempfile::tempdir().unwrap();
        let service = EcOperationsService::open(
            tmp.path(),
            "ec",
            active_set_registry(1),
            test_set_builder(1),
            NodeId(1),
        )
        .unwrap();
        let object = EcObjectVersionKey::new("bucket", "object", None);
        let mut old = small_placement([1, 2, 3]);
        old.hlc = HlcTimestamp::from_record(1, 0, 1);
        let mut new = old.clone();
        new.hlc = HlcTimestamp::from_record(2, 0, 1);
        service.settle_small_placement(&object, &new, 0, 1).await.unwrap();
        service.settle_small_placement(&object, &old, 2, 1).await.unwrap();
        assert_eq!(
            service
                .metadata
                .get_small_placement_hint(&object)
                .await
                .unwrap()
                .expect("old ack must not clear a newer write's debt")
                .placement,
            new
        );
    }

    #[tokio::test]
    async fn a_placement_write_short_of_a_majority_is_remembered_and_then_settled() {
        let tmp = tempfile::tempdir().unwrap();
        let service = {
            let _guard = TieringEnvGuard::acquire();
            EcOperationsService::open(
                tmp.path(),
                "ec",
                active_set_registry(1),
                test_set_builder(1),
                NodeId(1),
            )
            .unwrap()
        };
        let object = EcObjectVersionKey::new("photos".to_string(), "thumb.png".to_string(), None);
        let placement = small_placement([1, 2, 3]);

        // One replica out of three took the record: not enough to outlive this
        // node, so the debt is written down.
        service.settle_small_placement(&object, &placement, 1, 1).await.unwrap();
        let hint = service.metadata.get_small_placement_hint(&object).await.unwrap();
        let hint = hint.expect("a write short of a majority owes the replicas a copy");
        assert_eq!(hint.acks, 1);
        assert_eq!(hint.attempts, 1);
        assert_eq!(hint.placement, placement);

        // Two of three is a majority, and the debt is gone.
        service.settle_small_placement(&object, &placement, 2, 2).await.unwrap();
        assert!(service.metadata.get_small_placement_hint(&object).await.unwrap().is_none());
    }

    /// A sweep that cannot reach the pool keeps what it owes.
    ///
    /// The debt is durable on purpose: a node restarted before its replicas
    /// came back must still know it owes them a copy, and a sweep with no
    /// cluster ports must not read that as "nothing to do and nothing owed".
    #[tokio::test]
    async fn a_sweep_without_cluster_ports_keeps_the_hints_it_owes() {
        let tmp = tempfile::tempdir().unwrap();
        let service = {
            let _guard = TieringEnvGuard::acquire();
            EcOperationsService::open(
                tmp.path(),
                "ec",
                active_set_registry(1),
                test_set_builder(1),
                NodeId(1),
            )
            .unwrap()
        };
        let object = EcObjectVersionKey::new("photos".to_string(), "thumb.png".to_string(), None);
        service
            .settle_small_placement(&object, &small_placement([1, 2, 3]), 0, 1)
            .await
            .unwrap();

        service
            .metadata
            .put_small_placement_hint_cursor(Some(vec![1, 2, 3]))
            .await
            .unwrap();
        let sweep = service.deliver_small_placement_hints(16).await.unwrap();
        assert_eq!(
            service.metadata.get_small_placement_hint_cursor().await.unwrap(),
            Some(vec![1, 2, 3])
        );

        assert_eq!(
            sweep,
            EcSmallPlacementHintSweep::default(),
            "nothing could be judged"
        );
        assert!(
            service.metadata.get_small_placement_hint(&object).await.unwrap().is_some(),
            "a sweep that could not ask the pool must not drop what it owes"
        );
    }

    /// EC2-201: an object with no placement record is looked for once, not once
    /// per read.
    ///
    /// Every object the hot path still holds answers "no manifest" on this
    /// path. Without the memory each of their reads would pay a quorum `HEAD`
    /// and a resolve that can only ever miss.
    #[tokio::test]
    async fn a_missing_placement_record_is_looked_for_once_until_it_expires() {
        let tmp = tempfile::tempdir().unwrap();
        let service = {
            let _guard = TieringEnvGuard::acquire();
            EcOperationsService::open(
                tmp.path(),
                "ec",
                active_set_registry(1),
                test_set_builder(1),
                NodeId(1),
            )
            .unwrap()
        };
        let object = EcObjectVersionKey::new("photos".to_string(), "hot.bin".to_string(), None);

        assert!(!service.small_placement_miss_is_remembered(&object));
        service.remember_small_placement_miss(&object);
        assert!(service.small_placement_miss_is_remembered(&object));

        // A record appearing for that object forgets the miss at once, so the
        // memory can never outlive what it is about.
        service.forget_small_placement_miss(&object);
        assert!(!service.small_placement_miss_is_remembered(&object));
    }

    /// The memory is off when its window is zero, and defaults to a minute.
    #[test]
    fn the_placement_miss_memory_has_a_window_an_operator_sets() {
        let _guard = TieringEnvGuard::acquire();
        std::env::remove_var(SMALL_PLACEMENT_NEGATIVE_TTL_ENV);
        assert_eq!(
            EcOperationsConfig::from_env().unwrap().small_placement_negative_ttl,
            Duration::from_secs(DEFAULT_EC_SMALL_PLACEMENT_NEGATIVE_TTL_SECS)
        );

        std::env::set_var(SMALL_PLACEMENT_NEGATIVE_TTL_ENV, "0");
        let config = EcOperationsConfig::from_env().unwrap();
        assert!(config.small_placement_negative_ttl.is_zero());
        std::env::remove_var(SMALL_PLACEMENT_NEGATIVE_TTL_ENV);
    }

    /// EC2-200: which storage failures are a verdict on the bytes, and which
    /// are a verdict on the disk.
    ///
    /// The whole difference between the two branches of `read_local_shard`, in
    /// the one place it is decided. Only bytes that were read and found wanting
    /// condemn a shard; a medium that did not answer condemns nothing, because
    /// a node that loses a volume for a minute must not be made to rebuild
    /// everything it held.
    #[test]
    fn only_bytes_that_were_read_and_refused_count_as_rot() {
        use s4_core::StorageError;

        assert!(storage_error_is_rot(&StorageError::ChecksumMismatch {
            key: "__s4_ec_v1/shards/abc".to_string(),
        }));
        assert!(storage_error_is_rot(&StorageError::InvalidData(
            "Short blob read for __s4_ec_v1/shards/abc: read=10, expected=64".to_string(),
        )));

        for medium in [
            StorageError::Io(std::io::Error::other("input/output error")),
            StorageError::Database("index is not responding".to_string()),
            StorageError::VolumeNotFound { volume_id: 7 },
        ] {
            assert!(
                !storage_error_is_rot(&medium),
                "a failing medium is not a corrupt shard: {medium}"
            );
        }
    }

    /// EC2-READ-04-001: the switch that decides how much of a chunk a serving
    /// read pulls, and the value it ships with.
    ///
    /// On by default, and on because the saving was measured first: `n/k` → 1.0
    /// on bytes and on round trips, with the cross-zone share falling in the
    /// same proportion (decision EC2-183). `false` puts the node back on the
    /// EC v1 read, and it does so on that node alone — nothing on disk changes,
    /// so a pool may run with it set either way on any of its nodes.
    #[test]
    fn minimal_read_is_on_unless_the_operator_turns_it_off() {
        let _guard = TieringEnvGuard::acquire();

        assert!(
            EcOperationsConfig::from_env().unwrap().minimal_read_enabled,
            "a node with nothing set must read the `k` data shards"
        );

        for (value, expected) in [("false", false), ("true", true), ("off", false)] {
            std::env::set_var(MINIMAL_READ_ENABLED_ENV, value);
            assert_eq!(
                EcOperationsConfig::from_env().unwrap().minimal_read_enabled,
                expected,
                "{MINIMAL_READ_ENABLED_ENV}={value}"
            );
        }

        // A value nobody can read as yes or no stops the node instead of
        // picking one: which shards a read pulls is not something to guess at.
        std::env::set_var(MINIMAL_READ_ENABLED_ENV, "sometimes");
        let error = EcOperationsConfig::from_env().unwrap_err().to_string();
        assert!(
            error.contains(MINIMAL_READ_ENABLED_ENV),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn tiering_defaults_to_disabled() {
        let _guard = TieringEnvGuard::acquire();
        let config = EcTieringConfig::from_env().unwrap();
        assert!(!config.enabled);
        assert_eq!(config.cold_after_secs, DEFAULT_EC_TIERING_COLD_AFTER_SECS);
        assert_eq!(config, EcTieringConfig::default());
    }

    /// The three variables EC v1 shipped keep their meaning and their defaults.
    ///
    /// This is the compatibility guarantee of `EC2-TIER-00-006`: a deployment
    /// that already sets them must behave exactly as it did before plan 02.
    #[test]
    fn tiering_keeps_the_v1_environment_variables() {
        let _guard = TieringEnvGuard::acquire();
        std::env::set_var("S4_EC_TIERING_ENABLED", "true");
        std::env::set_var("S4_EC_TIERING_COLD_AFTER_SECS", "1209600");
        std::env::set_var("S4_EC_TIERING_SCAN_LIMIT", "2048");

        let config = EcTieringConfig::from_env().unwrap();
        assert!(config.enabled);
        assert_eq!(config.cold_after_secs, 1_209_600);
        assert_eq!(config.scan_limit, 2_048);

        // Everything plan 02 adds falls back to its documented default, so the
        // node keeps the behaviour it had before the fields existed.
        assert_eq!(
            config.heat_resolution_secs,
            DEFAULT_EC_TIERING_HEAT_RESOLUTION_SECS
        );
        assert_eq!(
            config.enqueue_per_cycle,
            DEFAULT_EC_TIERING_ENQUEUE_PER_CYCLE
        );
        assert_eq!(config.max_queue_depth, DEFAULT_EC_TIERING_MAX_QUEUE_DEPTH);
        assert_eq!(config.jitter_percent, DEFAULT_EC_TIERING_JITTER_PERCENT);
        assert_eq!(config.promote_after_reads, 0);
    }

    /// A sub-day cold-after value stays legal in the environment.
    ///
    /// The one-day floor of EC2-021 applies to bucket policies. Enforcing it
    /// here would stop a node that already sets a shorter value from booting,
    /// which phase 0 is not allowed to do.
    #[test]
    fn tiering_environment_allows_a_short_cold_after() {
        let _guard = TieringEnvGuard::acquire();
        std::env::set_var("S4_EC_TIERING_COLD_AFTER_SECS", "60");
        assert_eq!(EcTieringConfig::from_env().unwrap().cold_after_secs, 60);

        std::env::set_var("S4_EC_TIERING_COLD_AFTER_SECS", "0");
        assert!(EcTieringConfig::from_env().is_err());
    }

    /// Activity tracking follows the tiering switch unless asked otherwise.
    ///
    /// Defaulting to `enabled` is what keeps a node that does not tier free of
    /// any new work. Turning it on alone is the supported way to collect access
    /// data before the first migration is ever queued.
    #[test]
    fn tiering_heat_tracking_follows_the_enabled_flag() {
        let _guard = TieringEnvGuard::acquire();
        assert!(!EcTieringConfig::from_env().unwrap().heat_enabled);

        std::env::set_var("S4_EC_TIERING_ENABLED", "true");
        assert!(EcTieringConfig::from_env().unwrap().heat_enabled);

        std::env::set_var("S4_EC_TIERING_ENABLED", "false");
        std::env::set_var("S4_EC_TIERING_HEAT_ENABLED", "true");
        let config = EcTieringConfig::from_env().unwrap();
        assert!(!config.enabled);
        assert!(config.heat_enabled);
    }

    #[test]
    fn tiering_rejects_zero_intervals_and_limits() {
        let _guard = TieringEnvGuard::acquire();
        for name in [
            "S4_EC_TIERING_HEAT_RESOLUTION_SECS",
            "S4_EC_TIERING_HEAT_FLUSH_SECS",
            "S4_EC_TIERING_SCAN_INTERVAL_SECS",
            "S4_EC_TIERING_PROMOTE_WINDOW_SECS",
            "S4_EC_TIERING_MIN_MIGRATION_INTERVAL_SECS",
            "S4_EC_TIERING_HEAT_QUEUE_CAPACITY",
            "S4_EC_TIERING_ENQUEUE_PER_CYCLE",
            "S4_EC_TIERING_MAX_QUEUE_DEPTH",
        ] {
            std::env::set_var(name, "0");
            assert!(
                EcTieringConfig::from_env().is_err(),
                "{name} must be rejected at zero"
            );
            std::env::remove_var(name);
        }
        assert!(EcTieringConfig::from_env().is_ok());
    }

    /// A node that already sets a small scan limit keeps starting.
    ///
    /// The per-cycle default is larger than a deliberately small scan limit, so
    /// checking it the same way as an explicit value would turn an existing
    /// configuration into a boot failure. The default adapts instead.
    #[test]
    fn tiering_default_enqueue_limit_adapts_to_a_smaller_scan_limit() {
        let _guard = TieringEnvGuard::acquire();
        std::env::set_var("S4_EC_TIERING_SCAN_LIMIT", "10");
        let config = EcTieringConfig::from_env().unwrap();
        assert_eq!(config.scan_limit, 10);
        assert_eq!(config.enqueue_per_cycle, 10);

        std::env::set_var("S4_EC_TIERING_SCAN_LIMIT", "4096");
        std::env::set_var("S4_EC_TIERING_MAX_QUEUE_DEPTH", "8");
        let config = EcTieringConfig::from_env().unwrap();
        assert_eq!(config.enqueue_per_cycle, 8);
    }

    /// An explicitly set per-cycle limit above either ceiling is refused.
    ///
    /// Silently clamping a value the operator typed would leave them comparing
    /// a configured rate against a lower observed one with nothing to explain
    /// the difference.
    #[test]
    fn tiering_rejects_an_enqueue_limit_above_its_ceilings() {
        let _guard = TieringEnvGuard::acquire();
        std::env::set_var("S4_EC_TIERING_MAX_QUEUE_DEPTH", "32");
        std::env::set_var("S4_EC_TIERING_ENQUEUE_PER_CYCLE", "64");
        assert!(EcTieringConfig::from_env().is_err());

        std::env::set_var("S4_EC_TIERING_MAX_QUEUE_DEPTH", "1024");
        std::env::set_var("S4_EC_TIERING_SCAN_LIMIT", "16");
        assert!(EcTieringConfig::from_env().is_err());

        std::env::set_var("S4_EC_TIERING_SCAN_LIMIT", "4096");
        assert!(EcTieringConfig::from_env().is_ok());
    }

    #[test]
    fn tiering_rejects_a_jitter_above_one_hundred_percent() {
        let _guard = TieringEnvGuard::acquire();
        std::env::set_var("S4_EC_TIERING_JITTER_PERCENT", "101");
        assert!(EcTieringConfig::from_env().is_err());

        std::env::set_var("S4_EC_TIERING_JITTER_PERCENT", "300");
        assert!(EcTieringConfig::from_env().is_err());

        std::env::set_var("S4_EC_TIERING_JITTER_PERCENT", "100");
        assert_eq!(EcTieringConfig::from_env().unwrap().jitter_percent, 100);
    }

    /// The status snapshot carries the new fields without extra wiring.
    ///
    /// `config_snapshot` embeds the whole struct, so a field added here shows up
    /// in the admin JSON that the acceptance scripts read.
    #[test]
    fn tiering_config_serializes_its_new_fields() {
        let value = serde_json::to_value(EcTieringConfig::default()).unwrap();
        for field in [
            "enabled",
            "cold_after_secs",
            "scan_limit",
            "heat_enabled",
            "heat_resolution_secs",
            "scan_interval_secs",
            "enqueue_per_cycle",
            "max_queue_depth",
            "jitter_percent",
            "promote_after_reads",
            "promote_window_secs",
            "min_migration_interval_secs",
            "promote_per_cycle",
            "max_promotion_queue_depth",
            "promote_poll_secs",
            "promote_batch_size",
            "promote_max_attempts",
            "promote_retry_backoff_secs",
        ] {
            assert!(value.get(field).is_some(), "{field} is missing from status");
        }
    }

    /// Reverse tiering is off by default, and the queue behind it is bounded.
    ///
    /// Turning auto-tiering on must not start moving objects back on its own:
    /// the automatic direction is the more expensive one and needs an operator
    /// watching metrics before it is enabled. Decision EC2-023.
    #[test]
    fn reverse_tiering_is_off_until_a_threshold_is_set() {
        let _guard = TieringEnvGuard::acquire();
        let config = EcTieringConfig::from_env().unwrap();

        assert_eq!(config.promote_after_reads, 0);
        // The queue and its worker exist regardless: they also carry the
        // operator's rollback, which has to work with the feature switched off.
        assert!(config.promote_per_cycle > 0);
        assert!(config.max_promotion_queue_depth >= config.promote_per_cycle);
        assert!(config.promote_max_attempts > 0);
    }

    #[test]
    fn reverse_tiering_rejects_limits_that_would_stall_or_be_clamped() {
        let _guard = TieringEnvGuard::acquire();

        std::env::set_var("S4_EC_TIERING_PROMOTE_MAX_ATTEMPTS", "0");
        assert!(
            EcTieringConfig::from_env().is_err(),
            "a retry budget of zero would abandon a promotion on the first hiccup"
        );
        std::env::remove_var("S4_EC_TIERING_PROMOTE_MAX_ATTEMPTS");

        std::env::set_var("S4_EC_TIERING_PROMOTE_PER_CYCLE", "64");
        std::env::set_var("S4_EC_TIERING_MAX_PROMOTION_QUEUE_DEPTH", "16");
        assert!(
            EcTieringConfig::from_env().is_err(),
            "a per-cycle limit above its ceiling is silently clamped, so it is refused"
        );

        std::env::set_var("S4_EC_TIERING_MAX_PROMOTION_QUEUE_DEPTH", "64");
        assert!(EcTieringConfig::from_env().is_ok());

        for name in [
            "S4_EC_TIERING_PROMOTE_POLL_SECS",
            "S4_EC_TIERING_PROMOTE_BATCH_SIZE",
            "S4_EC_TIERING_PROMOTE_RETRY_BACKOFF_SECS",
        ] {
            std::env::set_var(name, "0");
            assert!(
                EcTieringConfig::from_env().is_err(),
                "{name} must be refused at zero"
            );
            std::env::remove_var(name);
        }
    }

    /// The promotion marker must not outlive the pause that protects it.
    ///
    /// A second migration of the same generation writes a manifest at the same
    /// HLC, where the marker deliberately outranks it. The pause between two
    /// migrations of one object is the only thing keeping the two apart, so a
    /// configuration where the marker lives longer is refused at startup rather
    /// than discovered as an object that stopped reading.
    #[test]
    fn a_promotion_marker_may_not_outlive_the_pause_between_migrations() {
        let _guard = TieringEnvGuard::acquire();
        std::env::set_var("S4_EC_TIERING_MIN_MIGRATION_INTERVAL_SECS", "3600");

        std::env::set_var("S4_EC_PROMOTED_GC_GRACE_SECS", "3600");
        let error = EcOperationsConfig::from_env().unwrap_err().to_string();
        assert!(
            error.contains("S4_EC_PROMOTED_GC_GRACE_SECS"),
            "unexpected error: {error}"
        );

        std::env::set_var("S4_EC_PROMOTED_GC_GRACE_SECS", "7200");
        assert!(EcOperationsConfig::from_env().is_err());

        std::env::set_var("S4_EC_PROMOTED_GC_GRACE_SECS", "3599");
        assert!(EcOperationsConfig::from_env().is_ok());

        // And the shipped defaults satisfy it with room to spare: an hour of
        // grace against a week of pause.
        std::env::remove_var("S4_EC_PROMOTED_GC_GRACE_SECS");
        std::env::remove_var("S4_EC_TIERING_MIN_MIGRATION_INTERVAL_SECS");
        assert!(EcOperationsConfig::from_env().is_ok());
    }

    /// A promotion decodes from the manifest the pool agrees on, or from none.
    ///
    /// The bug this guards against was the absence of the check, not a wrong
    /// branch of it: the worker asked the read coordinator for bytes while its
    /// cached layout still pointed at an older revision of the same manifest,
    /// and every advance of the manifest's state — committed, GC-eligible,
    /// purged — changes its hash. A live manifest is what the promotion
    /// refreshes to; the other three answers mean the work is already gone.
    #[test]
    fn a_promotion_only_decodes_from_a_manifest_the_pool_still_calls_erasure_coded() {
        for state in [
            EcPlacementState::EcCommitted,
            EcPlacementState::HotReplicasGcEligible,
            EcPlacementState::HotReplicasPurged,
        ] {
            let manifest = manifest_with_state(state);
            let source = promotion_source_manifest(EcManifestReadResult::Present(manifest.clone()));
            assert_eq!(
                source.map(|found| found.state),
                Some(state),
                "a live manifest in {state:?} is what a promotion reads from"
            );
        }

        // Already back on the hot path: its shards are what GC is about to take.
        assert!(
            promotion_source_manifest(EcManifestReadResult::Present(manifest_with_state(
                EcPlacementState::Promoted
            )))
            .is_none()
        );

        // Deleted, or the pool no longer knows the object as erasure coded.
        assert!(
            promotion_source_manifest(EcManifestReadResult::Tombstone(manifest_with_state(
                EcPlacementState::Tombstone
            )))
            .is_none()
        );
        assert!(promotion_source_manifest(EcManifestReadResult::Absent).is_none());
    }

    /// The admin view of a reverse migration job survives serialization.
    ///
    /// Node ids and operation ids are 128-bit, and a JSON number is not: a
    /// snapshot that hands the record to `json!` whole panics on the first
    /// object whose HLC carries a real node id.
    #[test]
    fn a_promotion_job_snapshot_renders_its_128_bit_ids() {
        let job = EcPromotionJobRecord {
            object: EcObjectVersionKey::new("bucket", "report.bin", None),
            operation_id: Uuid::from_u128(u128::MAX - 7),
            source_hlc: HlcTimestamp::from_record(1_700, 3, u128::MAX - 1),
            erasure_set_id: 1,
            state: EcPromotionState::HotRestored,
            attempt: 2,
            object_size: 4096,
            content_hash_sha256: [9; 32],
            origin: EcPromotionOrigin::Manual,
            shards_eligible_at: Some(2_000),
            updated_at: 1_800,
            next_retry_at: None,
            failure: Some("peer unreachable".to_string()),
        };

        let value = promotion_job_admin_snapshot(&job);

        assert_eq!(value["state"], json!("hot_restored"));
        assert_eq!(value["origin"], json!("manual"));
        assert_eq!(value["operation_id"], json!(job.operation_id.to_string()));
        assert!(value["source_hlc"]["node_id"].is_string());
        assert_eq!(value["failure"], json!("peer unreachable"));
        // The whole thing has to render, not just the fields asserted above.
        assert!(serde_json::to_string(&value).is_ok());
    }

    #[test]
    fn node_id_parser_accepts_uuid_display() {
        let id = NodeId(42);
        let parsed = parse_node_id(&id.to_string()).unwrap();
        assert_eq!(parsed, id);
    }

    #[test]
    fn transcode_queue_depth_counts_retryable_failures() {
        let object = EcObjectVersionKey::new("b", "k", None);
        let job = EcTranscodeJobRecord {
            object,
            operation_id: Uuid::new_v4(),
            source_hlc: s4_cluster::HlcTimestamp::from_record(1, 1, NodeId(1).0),
            target_erasure_set_id: 9,
            state: EcPlacementState::EcFailedRetryable,
            attempt: 1,
            object_size: 10,
            source_content_hash_sha256: [1u8; 32],
            updated_at: 1,
            next_retry_at: Some(2),
            failure: None,
            origin: EcTranscodeOrigin::ClientWrite,
            target: EcTranscodeTarget::Object,
        };
        let counts = transcode_counts(&[job]);
        assert_eq!(counts.queue_depth, 1);
        assert_eq!(counts.failed_retryable, 1);
    }

    /// Absence is a pool-wide verdict, never one set's.
    ///
    /// A set answering "no such object" only says the object is not in that set.
    /// Concluding `Absent` from it would hide every object of every other set.
    #[test]
    fn absence_needs_every_set_of_the_pool_to_say_so() {
        let inherited = active_set(1);
        let second = additional_set(1);
        let sets = vec![
            (inherited.set_id, set_metadata_nodes(&inherited)),
            (second.set_id, set_metadata_nodes(&second)),
        ];
        let required = 4u8;

        let one_set_only = HashMap::from([(sets[0].0, required)]);
        assert!(!every_set_reached(&sets, &one_set_only, required));

        let short_of_quorum = HashMap::from([(sets[0].0, required), (sets[1].0, required - 1)]);
        assert!(!every_set_reached(&sets, &short_of_quorum, required));

        let both = HashMap::from([(sets[0].0, required), (sets[1].0, required)]);
        assert!(every_set_reached(&sets, &both, required));

        // A pool of one set behaves exactly as EC v1 did.
        assert!(every_set_reached(&sets[..1], &one_set_only, required));

        // A node with no pool configured must not answer "absent" for every
        // object it is asked about.
        assert!(!every_set_reached(&[], &both, required));
    }

    /// A manifest read stops once every set has answered for good.
    ///
    /// Waiting for the rest of an already-decided set is what lets one silent
    /// node put its RPC timeout in front of every read of every other set.
    #[test]
    fn a_manifest_read_stops_when_every_set_has_a_verdict() {
        let inherited = active_set(1);
        let second = additional_set(1);
        let sets = vec![
            (inherited.set_id, set_metadata_nodes(&inherited)),
            (second.set_id, set_metadata_nodes(&second)),
        ];

        let one_set_only = HashSet::from([sets[0].0]);
        assert!(!every_set_decided(&sets, &one_set_only));

        let both = HashSet::from([sets[0].0, sets[1].0]);
        assert!(every_set_decided(&sets, &both));

        // A pool of one set is decided by its only set, which is the EC v1
        // behaviour: the first quorum ends the read.
        assert!(every_set_decided(&sets[..1], &one_set_only));

        // And a node with no pool configured has decided nothing.
        assert!(!every_set_decided(&[], &both));
    }

    /// A shard read may only go to a node of the set the manifest names.
    ///
    /// The two fixtures sit on disjoint nodes, so slot 0 exists in both and is
    /// held by a different node in each. A reader that ignored the set would
    /// happily ask the wrong one.
    #[test]
    fn a_node_of_another_set_cannot_serve_a_slot() {
        let inherited = active_set(1);
        let second = additional_set(1);
        let inherited_slot_zero = inherited.slot_assignment.slots()[0];
        let second_slot_zero = second.slot_assignment.slots()[0];
        assert_ne!(inherited_slot_zero.node_id, second_slot_zero.node_id);

        assert!(ensure_shard_node_in_set(&inherited, 0, inherited_slot_zero.node_id).is_ok());
        assert!(ensure_shard_node_in_set(&second, 0, second_slot_zero.node_id).is_ok());

        let refused =
            ensure_shard_node_in_set(&inherited, 0, second_slot_zero.node_id).unwrap_err();
        assert!(
            refused.to_string().contains("not a node of erasure set"),
            "the refusal must say why: {refused}"
        );
        assert!(ensure_shard_node_in_set(&second, 0, inherited_slot_zero.node_id).is_err());
        // A node that is in no set of the pool at all.
        assert!(ensure_shard_node_in_set(&inherited, 0, NodeId(9_999)).is_err());
    }

    /// A shard written under one layout of the set stays readable after the
    /// layout is reordered — which a label change does to a set without a
    /// layout record. Regression test for EC2-086: the read used to demand that
    /// the node still hold the very slot, and refused every shard whose slot had
    /// moved.
    #[test]
    fn a_shard_stays_readable_after_its_slot_moved_to_another_node_of_the_set() {
        let written = active_set(1);
        let mut reordered_nodes = written.slot_assignment.slot_order();
        reordered_nodes.rotate_left(1);
        let mut relabeled = written.clone();
        relabeled.slot_assignment =
            ShardSlotAssignment::from_slot_order(&written.profile, &reordered_nodes).unwrap();

        for slot in written.slot_assignment.slots() {
            let now_held_by = relabeled.slot_assignment.slots()[usize::from(slot.slot_index)];
            assert_ne!(
                now_held_by.node_id, slot.node_id,
                "the reorder must move every slot"
            );
            ensure_shard_node_in_set(&relabeled, slot.slot_index, slot.node_id)
                .expect("a shard on a node of the set must stay readable");
        }
    }

    /// Queue depth is reported per set, so one set's backlog is not read as
    /// pressure on another.
    #[test]
    fn queue_depths_are_grouped_by_the_set_that_owns_the_work() {
        let job = |set_id: u64, state: EcPlacementState| EcTranscodeJobRecord {
            object: EcObjectVersionKey::new("b", "k", None),
            operation_id: Uuid::new_v4(),
            source_hlc: s4_cluster::HlcTimestamp::from_record(1, 1, NodeId(1).0),
            target_erasure_set_id: set_id,
            state,
            attempt: 1,
            object_size: 10,
            source_content_hash_sha256: [1u8; 32],
            updated_at: 1,
            next_retry_at: None,
            failure: None,
            origin: EcTranscodeOrigin::ClientWrite,
            target: EcTranscodeTarget::Object,
        };
        let jobs = vec![
            job(9, EcPlacementState::EcTranscodeQueued),
            job(9, EcPlacementState::EcEncoding),
            job(1, EcPlacementState::EcTranscodeQueued),
            // Finished work is not queue depth, in either set.
            job(1, EcPlacementState::EcCommitted),
        ];

        let by_set = transcode_queue_depth_by_set(&jobs);

        assert_eq!(by_set.get(&9).copied(), Some(2));
        assert_eq!(by_set.get(&1).copied(), Some(1));
        assert_eq!(by_set.len(), 2);
        // The pool figure is the sum over the label, and it still agrees with
        // the snapshot the admin surface reports.
        assert_eq!(
            by_set.values().sum::<usize>(),
            transcode_counts(&jobs).queue_depth
        );
    }

    /// The same, for the repair queue: the number the isolation rule is read
    /// from.
    #[test]
    fn repair_queue_depth_is_grouped_by_erasure_set() {
        let task = |set_id: u64, status: EcRepairStatus| EcRepairTaskRecord {
            repair_id: Uuid::new_v4(),
            object: EcObjectVersionKey::new("b", "k", None),
            operation_id: Uuid::new_v4(),
            erasure_set_id: set_id,
            chunk_index: 0,
            slot_index: 0,
            node_id: NodeId(1),
            kind: EcRepairKind::MissingShard,
            status,
            attempts: 0,
            reason: String::new(),
            enqueued_at: 1,
            updated_at: 1,
            next_retry_at: None,
            failure_domain: None,
        };
        let tasks = vec![
            task(9, EcRepairStatus::Queued),
            task(9, EcRepairStatus::Running),
            task(9, EcRepairStatus::FailedRetryable),
            task(1, EcRepairStatus::Queued),
            // Done and given up on are not depth.
            task(1, EcRepairStatus::Succeeded),
            task(1, EcRepairStatus::FailedPermanent),
        ];

        let by_set = repair_queue_depth_by_set(&tasks);

        assert_eq!(by_set.get(&9).copied(), Some(3));
        assert_eq!(by_set.get(&1).copied(), Some(1));
        assert_eq!(
            by_set.values().sum::<usize>(),
            repair_counts(&tasks).queue_depth()
        );
    }

    fn repair_write_request() -> crate::ec_repair::EcRepairShardWriteRequest {
        let bytes = vec![7u8; 16];
        crate::ec_repair::EcRepairShardWriteRequest {
            object: EcObjectVersionKey::new("photos", "raw.bin", None),
            repair_id: Uuid::from_u128(9),
            operation_id: Uuid::from_u128(11),
            erasure_set_id: 0,
            chunk_index: 2,
            expected: ShardManifest {
                slot_index: 3,
                node_id: NodeId(4),
                role: s4_codec::ShardRole::Data,
                crc32: crc32fast::hash(&bytes),
                sha256: sha256(&bytes),
                size: bytes.len() as u64,
            },
            data: bytes::Bytes::from(bytes),
            kind: EcRepairKind::MissingShard,
        }
    }

    /// A repair is not a retry. The shard it rebuilds can be lost again next
    /// month, and the write that brings it back must not be mistaken for the
    /// one that brought it back last time: a replica skips a write whose
    /// operation id it has already applied, and it would answer success while
    /// the damaged bytes stayed on the disk.
    #[test]
    fn every_repair_write_carries_an_operation_id_of_its_own() {
        let first = repair_storage_operation_id();
        let second = repair_storage_operation_id();
        assert_ne!(first, second);

        // And not the id the same shard would derive: that one is the same on
        // every repair of this slot, for as long as the object exists.
        let request = repair_write_request();
        let derived = derive_storage_operation_id(
            request.operation_id,
            b"ec-repair-shard-write",
            request.chunk_index,
            request.expected.slot_index,
            EcPlacementState::EcCommitted,
        );
        assert_ne!(first, derived);
        assert_ne!(second, derived);
    }

    /// The second half of the same guarantee: the write path must put the
    /// bytes somewhere of their own rather than point the record at the blob
    /// that already holds this content — which, after bit rot, is the damage.
    #[test]
    fn a_repair_write_asks_for_its_bytes_to_be_materialised() {
        let metadata = repair_shard_metadata(&repair_write_request());

        assert!(
            s4_core::is_force_physical_write(&metadata),
            "a repair write that does not ask to materialise its bytes can be \
             answered by deduplication against the copy it is meant to replace"
        );
    }

    /// The damage split of the same queue: what tells a quiet pool with a tail
    /// of single losses from a pool a shard away from losing data.
    #[test]
    fn repair_queue_depth_is_split_by_the_damage_of_each_chunk() {
        let operation = Uuid::new_v4();
        let task =
            |set_id: u64, key: &str, chunk_index: u32, slot_index: u8, status| EcRepairTaskRecord {
                repair_id: Uuid::new_v4(),
                object: EcObjectVersionKey::new("b", key, None),
                operation_id: operation,
                erasure_set_id: set_id,
                chunk_index,
                slot_index,
                node_id: NodeId(1),
                kind: EcRepairKind::MissingShard,
                status,
                attempts: 0,
                reason: String::new(),
                enqueued_at: 1,
                updated_at: 1,
                next_retry_at: None,
                failure_domain: None,
            };
        let tasks = vec![
            // One chunk missing two shards, and a whole shard of it already
            // rebuilt: history is not damage.
            task(9, "urgent", 0, 0, EcRepairStatus::Queued),
            task(9, "urgent", 0, 1, EcRepairStatus::Running),
            task(9, "urgent", 0, 2, EcRepairStatus::Succeeded),
            // Another chunk of the same object, missing one shard of its own.
            task(9, "urgent", 1, 0, EcRepairStatus::Queued),
            // A quiet neighbour set with a single loss.
            task(1, "calm", 0, 0, EcRepairStatus::FailedRetryable),
        ];

        let by_losses = repair_queue_depth_by_losses(&tasks);

        assert_eq!(by_losses[&9].get("2").copied(), Some(2));
        assert_eq!(by_losses[&9].get("1").copied(), Some(1));
        assert_eq!(by_losses[&1].get("1").copied(), Some(1));
        // Summing a set's buckets gives that set's queue depth back, which is
        // what makes the two gauges readable side by side.
        let depth_by_set = repair_queue_depth_by_set(&tasks);
        for (erasure_set_id, buckets) in &by_losses {
            assert_eq!(
                buckets.values().sum::<usize>(),
                depth_by_set.get(erasure_set_id).copied().unwrap_or(0)
            );
        }
    }

    /// Two objects damaged the same way are two single losses, not one double:
    /// a chunk is what the codec rebuilds, and nothing crosses that boundary.
    #[test]
    fn repair_queue_damage_is_counted_per_chunk_and_not_per_object() {
        let task = |key: &str, operation_id: Uuid| EcRepairTaskRecord {
            repair_id: Uuid::new_v4(),
            object: EcObjectVersionKey::new("b", key, None),
            operation_id,
            erasure_set_id: 9,
            chunk_index: 0,
            slot_index: 0,
            node_id: NodeId(1),
            kind: EcRepairKind::MissingShard,
            status: EcRepairStatus::Queued,
            attempts: 0,
            reason: String::new(),
            enqueued_at: 1,
            updated_at: 1,
            next_retry_at: None,
            failure_domain: None,
        };
        let operation = Uuid::new_v4();
        let tasks = vec![
            task("one", operation),
            task("two", operation),
            // The same object and chunk written again: a task of the earlier
            // operation is about shards no current manifest names.
            task("one", Uuid::new_v4()),
        ];

        let by_losses = repair_queue_depth_by_losses(&tasks);

        assert_eq!(by_losses[&9].get("1").copied(), Some(3));
        assert_eq!(by_losses[&9].get("2"), None);
    }

    #[test]
    fn release_gates_show_s3_hot_path_wiring_without_worker_claims() {
        let gates = release_gate_status(false, false, false, false);
        assert_eq!(gates["http_ec_read_write_path"], json!("wired_hot_rf3"));
        assert_eq!(gates["s3_put_object_path"], json!("wired_hot_rf3"));
        assert_eq!(
            gates["transcode_worker_runtime"],
            json!("pending_runtime_wiring")
        );
        assert_eq!(gates["orphan_gc_physical_purger"], json!("dry_run_only"));
    }

    #[test]
    fn release_gates_report_started_transcode_worker() {
        let gates = release_gate_status(true, false, false, false);
        assert_eq!(gates["transcode_worker_runtime"], json!("wired"));
    }

    #[test]
    fn release_gates_report_ec_read_runtime() {
        let gates = release_gate_status(false, true, false, false);
        assert_eq!(gates["s3_get_object_path"], json!("wired_ec_read"));
        assert_eq!(gates["s3_head_object_path"], json!("wired_ec_read"));
        assert_eq!(gates["http_ec_read_write_path"], json!("wired_ec_read"));
    }

    #[test]
    fn runtime_ready_is_separate_from_production_attestation() {
        let runtime = ec_runtime_status(true, true, true, true);
        assert_eq!(runtime["runtime_ready"], json!(true));
        assert_eq!(runtime["ci_acceptance"], json!("not_attested"));
        assert_eq!(runtime["production_ready"], json!(false));
        assert_eq!(
            runtime["production_ready_blockers"],
            json!(["ec_v1_ci_acceptance_not_attested"])
        );
    }

    #[test]
    fn admin_erasure_set_snapshot_serializes_uuid_node_ids_as_strings() {
        let profile = EcProfile::rs_small();
        let nodes = (0..profile.total_shards())
            .map(|_| NodeId(Uuid::new_v4().as_u128()))
            .collect::<Vec<_>>();
        let erasure_set = ErasureSet::active(
            PoolId::new(9),
            // Kept at the value EC v1 derived from the pool so the fixtures that
            // name `erasure_set_id: 9` keep meaning the same set.
            legacy_pool_set_id(PoolId::new(9)),
            profile,
            &nodes,
            &HashMap::<NodeId, TopologyLabels>::new(),
            EcTopologyPolicy::Strict,
            3,
            1,
        )
        .unwrap();

        let snapshot = erasure_set_admin_snapshot(&erasure_set, None);

        serde_json::to_string(&snapshot).unwrap();
        let slot_node_ids = snapshot["slot_assignment"]["slots"]
            .as_array()
            .unwrap()
            .iter()
            .map(|slot| slot["node_id"].as_str().unwrap())
            .collect::<Vec<_>>();
        assert!(nodes.iter().all(|node_id| {
            let expected = node_id.to_string();
            slot_node_ids.iter().any(|actual| *actual == expected)
        }));
    }

    #[test]
    fn admin_manifest_snapshot_serializes_uuid_node_ids_as_strings() {
        let mut manifest = manifest_with_state(EcPlacementState::EcCommitted);
        manifest.hlc.node_id = Uuid::new_v4().as_u128();
        manifest.origin_node_id = NodeId(Uuid::new_v4().as_u128());
        for shard in manifest.chunks.iter_mut().flat_map(|chunk| &mut chunk.shards) {
            shard.node_id = NodeId(Uuid::new_v4().as_u128());
        }
        let first_shard_node_id = manifest.chunks[0].shards[0].node_id.to_string();

        let snapshot = manifest_admin_snapshot(&manifest);

        serde_json::to_string(&snapshot).unwrap();
        assert_eq!(
            snapshot["origin_node_id"],
            json!(manifest.origin_node_id.to_string())
        );
        assert_eq!(
            snapshot["hlc"]["node_id"],
            json!(NodeId(manifest.hlc.node_id).to_string())
        );
        assert_eq!(
            snapshot["chunks"][0]["shards"][0]["node_id"],
            json!(first_shard_node_id)
        );
    }

    #[test]
    fn read_plan_manifest_readability_rejects_pre_cutover_states() {
        for state in [
            EcPlacementState::HotReplicated,
            EcPlacementState::EcTranscodeQueued,
            EcPlacementState::EcEncoding,
            EcPlacementState::EcShardsDurable,
            EcPlacementState::EcManifestCommitted,
            EcPlacementState::EcReadVerified,
        ] {
            let manifest = manifest_with_state(state);
            assert!(!manifest_is_ec_readable(&manifest), "{state:?}");
        }

        for state in [
            EcPlacementState::EcCommitted,
            EcPlacementState::HotReplicasGcEligible,
            EcPlacementState::HotReplicasPurged,
        ] {
            let manifest = manifest_with_state(state);
            assert!(manifest_is_ec_readable(&manifest), "{state:?}");
        }
    }

    #[test]
    fn delete_tombstone_from_live_manifest_clears_chunks_and_wins_as_deleted() {
        let live = manifest_with_state(EcPlacementState::HotReplicasPurged);
        let tombstone = tombstone_from_live_manifest(
            &live,
            NodeId(99),
            s4_cluster::HlcTimestamp::from_record(20, 0, NodeId(99).0),
            200,
        )
        .unwrap();

        assert!(manifest_is_tombstone(&tombstone));
        assert!(tombstone.chunks.is_empty());
        assert_eq!(tombstone.state, EcPlacementState::Tombstone);
        assert!(tombstone.hlc > live.hlc);
        assert!(matches!(
            tombstone.clone().into_read_result(),
            EcManifestReadResult::Tombstone(read) if read.operation_id == tombstone.operation_id
        ));
    }

    #[tokio::test]
    async fn object_runtime_records_small_object_layout() {
        let (_tmp, service) = test_service(1024);
        let commit = put_commit(10);

        let disposition = service.record_put_commit(commit.clone()).await.unwrap();
        assert_eq!(disposition, EcObjectPutDisposition::ReplicatedSmall);

        let object = EcObjectVersionKey::new(commit.bucket, commit.key, None);
        let layout = service.metadata.get_object_layout(&object).await.unwrap().unwrap();
        match layout {
            ObjectDataLayout::ReplicatedSmall(placement) => {
                assert_eq!(placement.replica_nodes, commit.hot_replica_nodes);
                assert_eq!(placement.content_hash_sha256, commit.content_hash_sha256);
                assert_eq!(placement.size, commit.size);
            }
            other => panic!("expected replicated-small layout, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn object_runtime_packs_a_small_object_when_packing_is_enabled() {
        let (_tmp, service) = packed_test_service(1024);
        let commit = put_commit(10);

        let disposition = service.record_put_commit(commit.clone()).await.unwrap();
        assert_eq!(disposition, EcObjectPutDisposition::PackedSegment);

        let object =
            EcObjectVersionKey::new(commit.bucket.clone(), commit.key.clone(), commit.version_id);
        let layout = service.metadata().get_object_layout(&object).await.unwrap().unwrap();
        match layout {
            ObjectDataLayout::PackedSegment(placement) => {
                assert_eq!(placement.offset, 0);
                assert_eq!(placement.length, commit.size);
                assert_eq!(placement.content_hash_sha256, commit.content_hash_sha256);
                assert_eq!(placement.hot_replica_nodes, commit.hot_replica_nodes);
            }
            other => panic!("expected packed-segment layout, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn overwriting_a_packed_object_charges_the_previous_version() {
        let (_tmp, service) = packed_test_service(1024);
        let commit1 = put_commit(10);
        let disposition1 = service.record_put_commit(commit1.clone()).await.unwrap();
        assert_eq!(disposition1, EcObjectPutDisposition::PackedSegment);

        let mut commit2 = put_commit(25);
        commit2.content_hash_sha256 = [9u8; 32];
        commit2.source_hlc = HlcTimestamp::from_record(20, 0, NodeId(1).0);

        let disposition2 = service.record_put_commit(commit2.clone()).await.unwrap();
        assert_eq!(disposition2, EcObjectPutDisposition::PackedSegment);

        let recovery = service.metadata().load_packed_segments(1000).await.unwrap();
        assert_eq!(recovery.segments.len(), 1);
        assert_eq!(recovery.segments[0].dead_bytes, commit1.size);
    }

    /// A segment that has reached its configured age limit is sealed by the maintenance tick.
    #[tokio::test]
    async fn maintenance_seals_a_segment_that_reached_its_age_limit() {
        use crate::ec_packed_segments::SealReason;

        let (_tmp, service) = packed_test_service(1024);
        let commit = put_commit(10);
        let disposition = service.record_put_commit(commit.clone()).await.unwrap();
        assert_eq!(disposition, EcObjectPutDisposition::PackedSegment);

        // Before the maintenance tick, the open segment has no materialized record.
        let before = service.metadata().list_packed_segments(100).await.unwrap();
        assert!(
            before.is_empty(),
            "open segment must not appear in list_packed_segments"
        );

        // The open segment is reconstructed from the journal.
        let recovery = service.metadata().load_packed_segments(100).await.unwrap();
        assert_eq!(recovery.segments.len(), 1);
        let created_at = recovery.segments[0].created_at;

        let max_age_nanos = service.config().packed_segments.max_age_nanos();
        let tick_time = created_at.saturating_add(max_age_nanos).saturating_add(1);

        let sealed_count = service.seal_expired_packed_segments(tick_time).await.unwrap();
        assert_eq!(
            sealed_count, 1,
            "maintenance tick must seal the expired segment"
        );

        // After the tick, the segment is materialized and sealed.
        let after = service.metadata().list_packed_segments(100).await.unwrap();
        assert_eq!(after.len(), 1, "expected one sealed segment in metadata");
        let segment = &after[0];

        assert_eq!(segment.state, PackedSegmentState::Sealed);
        assert_eq!(segment.sealed_at, Some(tick_time));
        assert_eq!(segment.seal_reason, Some(SealReason::MaxAgeReached));

        let object =
            EcObjectVersionKey::new(commit.bucket.clone(), commit.key.clone(), commit.version_id);
        let entry = segment
            .entry_for(&object)
            .expect("sealed segment must contain the written object");
        assert_eq!(entry.length, commit.size);
        assert_eq!(entry.content_hash_sha256, commit.content_hash_sha256);

        assert!(
            segment.validate().is_ok(),
            "sealed segment validation must succeed"
        );
    }

    /// A fresh segment that has not exceeded its age limit remains open after a maintenance tick.
    #[tokio::test]
    async fn maintenance_leaves_a_fresh_segment_open() {
        let (_tmp, service) = packed_test_service(1024);
        let commit = put_commit(10);
        let disposition = service.record_put_commit(commit.clone()).await.unwrap();
        assert_eq!(disposition, EcObjectPutDisposition::PackedSegment);

        // A freshly opened segment must not be sealed at the current timestamp.
        let now = unix_time_nanos();
        let sealed_count = service.seal_expired_packed_segments(now).await.unwrap();
        assert_eq!(sealed_count, 0, "fresh segment must not be sealed");

        // list_packed_segments must still be empty for an open segment.
        let listed = service.metadata().list_packed_segments(100).await.unwrap();
        assert!(
            listed.is_empty(),
            "open segment must not appear in list_packed_segments"
        );

        // load_packed_segments shows the segment in the Open state with the object.
        let recovery = service.metadata().load_packed_segments(100).await.unwrap();
        assert_eq!(
            recovery.segments.len(),
            1,
            "expected one open segment in recovery"
        );
        let segment = &recovery.segments[0];
        assert_eq!(segment.state, PackedSegmentState::Open);
        assert!(segment.sealed_at.is_none());
        assert!(segment.seal_reason.is_none());

        let object =
            EcObjectVersionKey::new(commit.bucket.clone(), commit.key.clone(), commit.version_id);
        let entry = segment
            .entry_for(&object)
            .expect("open segment must contain the written object");
        assert_eq!(entry.length, commit.size);
        assert_eq!(entry.content_hash_sha256, commit.content_hash_sha256);

        assert!(
            segment.validate().is_ok(),
            "open segment validation must succeed"
        );
    }

    /// When packing is disabled, the maintenance tick returns 0 and creates no segments.
    #[tokio::test]
    async fn maintenance_does_nothing_when_packing_is_off() {
        let (_tmp, service) = test_service(1024);
        assert!(!service.config().packed_segments.enabled);

        // A small write goes to ReplicatedSmall when packing is disabled.
        let commit = put_commit(10);
        let disposition = service.record_put_commit(commit.clone()).await.unwrap();
        assert_eq!(disposition, EcObjectPutDisposition::ReplicatedSmall);

        // Maintenance tick returns 0 regardless of the timestamp passed.
        let sealed_zero = service.seal_expired_packed_segments(0).await.unwrap();
        assert_eq!(sealed_zero, 0);

        let sealed_now = service.seal_expired_packed_segments(unix_time_nanos()).await.unwrap();
        assert_eq!(sealed_now, 0);

        let sealed_future = service.seal_expired_packed_segments(u64::MAX).await.unwrap();
        assert_eq!(sealed_future, 0);

        let listed = service.metadata().list_packed_segments(100).await.unwrap();
        assert!(
            listed.is_empty(),
            "no packed segments must appear when packing is off"
        );

        let recovery = service.metadata().load_packed_segments(100).await.unwrap();
        assert!(
            recovery.segments.is_empty(),
            "no recovered segments when packing is off"
        );
    }

    /// A sealed segment preserves every accepted object with contiguous offsets.
    #[tokio::test]
    async fn a_sealed_segment_keeps_every_object_it_accepted() {
        use crate::ec_packed_segments::SealReason;

        let (_tmp, service) = packed_test_service(1024);

        let object_count = 5;
        let mut expected_commits = Vec::new();
        let mut expected_used_bytes = 0u64;

        for i in 0..object_count {
            let size = 10 + i as u64 * 5;
            let mut commit = put_commit(size);
            commit.bucket = format!("bucket-{}", i % 2);
            commit.key = format!("item-{i}.bin");
            commit.content_hash_sha256 = [(i + 1) as u8; 32];
            commit.source_hlc = HlcTimestamp::from_record(10 + i as u64, 0, NodeId(1).0);

            let disposition = service.record_put_commit(commit.clone()).await.unwrap();
            assert_eq!(disposition, EcObjectPutDisposition::PackedSegment);

            expected_used_bytes = expected_used_bytes.checked_add(size).unwrap();
            expected_commits.push(commit);
        }

        // Before sealing, list_packed_segments is empty.
        let before = service.metadata().list_packed_segments(100).await.unwrap();
        assert!(before.is_empty());

        let recovery = service.metadata().load_packed_segments(100).await.unwrap();
        assert_eq!(recovery.segments.len(), 1);
        let created_at = recovery.segments[0].created_at;

        let max_age_nanos = service.config().packed_segments.max_age_nanos();
        let tick_time = created_at.saturating_add(max_age_nanos).saturating_add(1);

        let sealed_count = service.seal_expired_packed_segments(tick_time).await.unwrap();
        assert_eq!(sealed_count, 1);

        let records = service.metadata().list_packed_segments(100).await.unwrap();
        assert_eq!(records.len(), 1);
        let segment = &records[0];

        assert_eq!(segment.state, PackedSegmentState::Sealed);
        assert_eq!(segment.seal_reason, Some(SealReason::MaxAgeReached));
        assert_eq!(segment.sealed_at, Some(tick_time));

        // The segment must contain exactly five entries.
        assert_eq!(segment.entries.len(), object_count);

        // Offsets must be strictly increasing with no overlaps and no gaps.
        let mut current_offset = 0u64;
        for (i, entry) in segment.entries.iter().enumerate() {
            assert_eq!(
                entry.offset, current_offset,
                "entry {i} offset must match expected consecutive offset (no gaps or overlaps)"
            );
            current_offset = current_offset.checked_add(entry.length).unwrap();
        }

        // Sum of lengths must equal used_bytes.
        let sum_lengths: u64 = segment.entries.iter().map(|entry| entry.length).sum();
        assert_eq!(sum_lengths, segment.used_bytes);
        assert_eq!(segment.used_bytes, expected_used_bytes);

        // dead_bytes == 0.
        assert_eq!(segment.dead_bytes, 0);

        // Every accepted object must be present in the segment.
        for commit in &expected_commits {
            let object = EcObjectVersionKey::new(
                commit.bucket.clone(),
                commit.key.clone(),
                commit.version_id.clone(),
            );
            let entry = segment
                .entry_for(&object)
                .expect("each accepted object must be present in the sealed segment");
            assert_eq!(entry.length, commit.size);
            assert_eq!(entry.content_hash_sha256, commit.content_hash_sha256);
        }

        assert!(
            segment.validate().is_ok(),
            "sealed segment validation must succeed"
        );
    }

    /// Sealing twice in a row with the same timestamp seals nothing the second time.
    #[tokio::test]
    async fn sealing_twice_seals_nothing_the_second_time() {
        use crate::ec_packed_segments::SealReason;

        let (_tmp, service) = packed_test_service(1024);
        let commit = put_commit(10);
        let disposition = service.record_put_commit(commit.clone()).await.unwrap();
        assert_eq!(disposition, EcObjectPutDisposition::PackedSegment);

        let recovery = service.metadata().load_packed_segments(100).await.unwrap();
        assert_eq!(recovery.segments.len(), 1);
        let created_at = recovery.segments[0].created_at;

        let max_age_nanos = service.config().packed_segments.max_age_nanos();
        let tick_time = created_at.saturating_add(max_age_nanos).saturating_add(1);

        // First tick seals the expired segment.
        let first_sealed = service.seal_expired_packed_segments(tick_time).await.unwrap();
        assert_eq!(first_sealed, 1, "first tick must seal 1 expired segment");

        let first_records = service.metadata().list_packed_segments(100).await.unwrap();
        assert_eq!(first_records.len(), 1);
        let first_segment = first_records[0].clone();
        assert_eq!(first_segment.state, PackedSegmentState::Sealed);
        assert_eq!(first_segment.seal_reason, Some(SealReason::MaxAgeReached));

        // Second tick with the exact same timestamp must seal 0 segments.
        let second_sealed = service.seal_expired_packed_segments(tick_time).await.unwrap();
        assert_eq!(second_sealed, 0, "second tick must seal 0 segments");

        let second_records = service.metadata().list_packed_segments(100).await.unwrap();
        assert_eq!(second_records.len(), 1);
        let second_segment = &second_records[0];

        // The segment record must be completely identical.
        assert_eq!(&first_segment, second_segment);
    }

    #[tokio::test]
    async fn object_runtime_queues_large_object_transcode() {
        let (_tmp, service) = test_service(1);
        let commit = put_commit(10);

        let disposition = service.record_put_commit(commit.clone()).await.unwrap();
        assert_eq!(disposition, EcObjectPutDisposition::TranscodeQueued);

        let jobs = service.metadata.list_transcode_jobs(16).await.unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].object.bucket, commit.bucket);
        assert_eq!(jobs[0].object.key, commit.key);
        assert_eq!(jobs[0].state, EcPlacementState::EcTranscodeQueued);
        assert_eq!(jobs[0].object_size, commit.size);
    }

    /// A deferred pool leaves a fresh write exactly as the hot path stored it.
    ///
    /// This is what gives auto-tiering something to migrate. Without it every
    /// object above the profile threshold is encoded on its way in, and the
    /// migration worker never has a cold RF=3 object to find.
    #[tokio::test]
    async fn a_deferred_pool_leaves_a_new_write_on_its_replicas() {
        let _guard = TieringEnvGuard::acquire();
        std::env::set_var("S4_EC_TIERING_ENABLED", "true");
        std::env::set_var("S4_EC_TIERING_DEFER_NEW_WRITES", "true");
        let tmp = tempfile::tempdir().unwrap();
        let service = EcOperationsService::open(
            tmp.path(),
            "ec",
            active_set_registry(1),
            test_set_builder(1),
            NodeId(1),
        )
        .unwrap();
        let commit = put_commit(10);

        let disposition = service.record_put_commit(commit.clone()).await.unwrap();

        assert_eq!(disposition, EcObjectPutDisposition::HotReplicated);
        assert!(service.metadata.list_transcode_jobs(16).await.unwrap().is_empty());
        let object = EcObjectVersionKey::new(commit.bucket, commit.key, None);
        assert!(service.metadata.get_object_layout(&object).await.unwrap().is_none());
    }

    /// Deferring writes with no worker to pick them up is refused, not assumed.
    #[test]
    fn deferring_writes_without_tiering_is_refused() {
        let _guard = TieringEnvGuard::acquire();
        std::env::set_var("S4_EC_TIERING_DEFER_NEW_WRITES", "true");

        let error = EcTieringConfig::from_env().unwrap_err().to_string();

        assert!(
            error.contains("S4_EC_TIERING_DEFER_NEW_WRITES"),
            "the error must name the setting that is wrong: {error}"
        );
        std::env::set_var("S4_EC_TIERING_ENABLED", "true");
        assert!(EcTieringConfig::from_env().unwrap().defer_new_writes);
    }

    /// A pool holding the inherited set and one more, with multi-set placement
    /// switched on.
    ///
    /// The guard covers only the open: that is when the placement settings are
    /// read, and the service keeps its own copy of them afterwards. Clearing the
    /// environment on the way out is what keeps the switch from leaking into the
    /// next test.
    fn multi_set_service(min_object_size: u64) -> (tempfile::TempDir, EcOperationsService) {
        let _guard = TieringEnvGuard::acquire();
        std::env::set_var(MULTI_SET_ENABLED_ENV, "true");
        std::env::set_var(PLACEMENT_STRATEGY_ENV, "round_robin");
        let sets = ErasureSetRegistry::from_sets(
            PoolId::new(9),
            vec![active_set(min_object_size), additional_set(min_object_size)],
        )
        .unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let service =
            EcOperationsService::open(tmp.path(), "ec", sets, test_set_builder(1), NodeId(1))
                .unwrap();
        (tmp, service)
    }

    /// Capacity that answers with a fixed measurement per node.
    #[derive(Default)]
    struct MeasuredNodes {
        by_node: HashMap<NodeId, EcNodeCapacity>,
    }

    impl MeasuredNodes {
        fn new(groups: &[(Vec<NodeId>, u64, u64)]) -> Self {
            let mut by_node = HashMap::new();
            for (nodes, total_bytes, available_bytes) in groups {
                for node_id in nodes {
                    by_node.insert(
                        *node_id,
                        EcNodeCapacity {
                            total_bytes: *total_bytes,
                            available_bytes: *available_bytes,
                            reported_ago: Duration::from_secs(1),
                        },
                    );
                }
            }
            Self { by_node }
        }
    }

    impl EcCapacitySource for MeasuredNodes {
        fn node_capacity(&self, node_id: NodeId) -> Option<EcNodeCapacity> {
            self.by_node.get(&node_id).copied()
        }
    }

    /// Capacity that reports every node of the pool as out of room.
    struct NoRoomAnywhere;

    impl EcCapacitySource for NoRoomAnywhere {
        fn node_capacity(&self, _node_id: NodeId) -> Option<EcNodeCapacity> {
            Some(EcNodeCapacity {
                total_bytes: 400 * 1024 * 1024 * 1024,
                available_bytes: 1024,
                reported_ago: Duration::from_secs(1),
            })
        }
    }

    /// The set placement chose is what the queued job names, and successive
    /// objects spread over the pool instead of piling into one set.
    ///
    /// This is the whole point of multi-set pools: capacity added as a second
    /// set has to receive writes without anyone recreating a bucket.
    #[tokio::test]
    async fn new_objects_spread_over_the_sets_of_the_pool() {
        let (_tmp, service) = multi_set_service(1);
        let known_sets: Vec<u64> =
            service.placement.sets().sets().iter().map(|set| set.set_id).collect();

        for index in 0..4u8 {
            let mut commit = put_commit(10);
            commit.key = format!("object-{index}.bin");
            service.record_put_commit(commit).await.unwrap();
        }

        let jobs = service.metadata.list_transcode_jobs(16).await.unwrap();
        assert_eq!(jobs.len(), 4);
        for job in &jobs {
            assert!(
                known_sets.contains(&job.target_erasure_set_id),
                "a job must name a set the pool actually has: {job:?}"
            );
        }
        let targets: HashSet<u64> = jobs.iter().map(|job| job.target_erasure_set_id).collect();
        assert_eq!(targets.len(), 2, "four objects must reach both sets");
    }

    /// One generation of an object is admitted once, into one set.
    ///
    /// Recording the same commit again must not queue a second job just because
    /// the rotation has moved on: two jobs for one generation would encode it
    /// twice, into different sets, and leave two manifests that cannot be
    /// ordered against each other.
    #[tokio::test]
    async fn a_repeated_commit_keeps_the_set_its_generation_was_admitted_into() {
        let (_tmp, service) = multi_set_service(1);
        let commit = put_commit(10);

        service.record_put_commit(commit.clone()).await.unwrap();
        let first = service.metadata.list_transcode_jobs(16).await.unwrap();
        assert_eq!(first.len(), 1);

        service.record_put_commit(commit).await.unwrap();
        let second = service.metadata.list_transcode_jobs(16).await.unwrap();

        assert_eq!(second.len(), 1, "the generation must not be admitted twice");
        assert_eq!(second[0].operation_id, first[0].operation_id);
        assert_eq!(
            second[0].target_erasure_set_id, first[0].target_erasure_set_id,
            "a retry must not move the object to another set"
        );
    }

    /// A pool with no room costs space, never availability.
    ///
    /// The client's write is already durable on its RF=3 replicas by the time
    /// this runs, so the object stays replicated and no job is queued. Tiering
    /// encodes it later, once an operator adds a set or frees space.
    #[tokio::test]
    async fn a_pool_with_no_room_leaves_a_new_write_on_its_replicas() {
        let (_tmp, service) = multi_set_service(1);
        service.placement.configure_capacity_source(Arc::new(NoRoomAnywhere));
        let commit = put_commit(10);

        let disposition = service.record_put_commit(commit.clone()).await.unwrap();

        assert_eq!(disposition, EcObjectPutDisposition::HotReplicated);
        assert!(service.metadata.list_transcode_jobs(16).await.unwrap().is_empty());
        let object = EcObjectVersionKey::new(commit.bucket, commit.key, None);
        assert!(service.metadata.get_object_layout(&object).await.unwrap().is_none());
    }

    /// A write whose only set with room is held until this node reads the
    /// set's layout record is queued for that set, not left on its replicas
    /// for good: the transcode worker encodes it once the record is read
    /// (EC2-088, point 3). Found by scenario 31: a node that could not read the
    /// record in its first seconds after start kept every object written
    /// through it in that window replicated, with tiering off for ever.
    #[tokio::test]
    async fn a_write_whose_set_is_held_is_queued_for_that_set() {
        let (_tmp, service) = multi_set_service(1);
        let extra = additional_set(1);
        service
            .metadata
            .put_erasure_set_roster(&EcErasureSetRoster {
                pool_id: 9,
                sets: vec![EcErasureSetSpec {
                    set_id: FIRST_SET_ID,
                    node_ids: extra.slot_assignment.distinct_nodes(),
                    state: ErasureSetState::Active,
                    created_at: 1,
                }],
                // The inherited set takes no new objects, so the held set is
                // the only one that could.
                inherited_state: ErasureSetState::ReadOnly,
                inherited_node_ids: Vec::new(),
                hlc: HlcTimestamp::from_record(1, 1, 1),
                updated_at: 1,
            })
            .await
            .unwrap();
        service.refresh_erasure_sets().await.unwrap();
        assert!(service.placement.sets().get(FIRST_SET_ID).unwrap().layout_pending);

        let disposition = service.record_put_commit(put_commit(10)).await.unwrap();

        assert_eq!(disposition, EcObjectPutDisposition::TranscodeQueued);
        let jobs = service.metadata.list_transcode_jobs(16).await.unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].target_erasure_set_id, FIRST_SET_ID);
        assert_eq!(jobs[0].state, EcPlacementState::EcTranscodeQueued);
    }

    /// A client write and a migration are told apart in the durable queue.
    #[tokio::test]
    async fn a_client_write_is_queued_as_a_client_write() {
        let (_tmp, service) = test_service(1);

        service.record_put_commit(put_commit(10)).await.unwrap();

        let jobs = service.metadata.list_transcode_jobs(16).await.unwrap();
        assert_eq!(jobs[0].origin, EcTranscodeOrigin::ClientWrite);
        assert!(!jobs[0].origin.is_tiering());
    }

    /// A job record written before auto-tiering existed still deserializes.
    ///
    /// Every such job came from a client write, which is what the default has
    /// to mean; reading them as migrations would let the tiering queue ceiling
    /// stop a node that has never tiered anything.
    #[test]
    fn a_job_record_without_an_origin_reads_as_a_client_write() {
        let stored = json!({
            "object": {"bucket": "b", "key": "k", "version_id": null},
            "operation_id": Uuid::from_u128(7),
            "source_hlc": {"wall_time": 10, "logical": 0, "node_id": 1},
            "target_erasure_set_id": 1,
            "state": "ec_transcode_queued",
            "attempt": 0,
            "object_size": 1024,
            "source_content_hash_sha256": vec![0u8; 32],
            "updated_at": 1,
            "next_retry_at": null,
            "failure": null
        });

        let job: EcTranscodeJobRecord = serde_json::from_value(stored).unwrap();

        assert_eq!(job.origin, EcTranscodeOrigin::ClientWrite);
    }

    /// A client write over a queued migration is reported as an abandoned one.
    ///
    /// The counter behind this is the only signal an operator has that tiering
    /// is doing work the client keeps invalidating, and it was declared once
    /// without ever being incremented. The assertion is on the count the
    /// deletion reports, because that is what feeds the metric.
    #[tokio::test]
    async fn overwriting_a_queued_migration_is_counted_as_abandoned() {
        let (_tmp, service) = test_service(1);
        let commit = put_commit(10);
        let object = EcObjectVersionKey::new(commit.bucket.clone(), commit.key.clone(), None);

        // Stand in for the tiering worker having queued this object.
        service
            .metadata
            .put_transcode_job(&EcTranscodeJobRecord {
                object: object.clone(),
                operation_id: Uuid::from_u128(4_242),
                source_hlc: commit.source_hlc,
                target_erasure_set_id: service.default_set.set_id,
                state: EcPlacementState::EcTranscodeQueued,
                attempt: 0,
                object_size: commit.size,
                source_content_hash_sha256: commit.content_hash_sha256,
                updated_at: 1,
                next_retry_at: None,
                failure: None,
                origin: EcTranscodeOrigin::Tiering,
                target: EcTranscodeTarget::Object,
            })
            .await
            .unwrap();

        let removed = service.metadata.delete_transcode_jobs_for_object(&object, 64).await.unwrap();

        assert_eq!(removed.total, 1);
        assert_eq!(
            removed.tiering, 1,
            "an abandoned migration must be counted as one"
        );
    }

    /// Deleting an object cancels the migration that was queued for it.
    ///
    /// The plan asks for cancellation on both a concurrent PUT and a concurrent
    /// DELETE. The write path is covered above and end to end; this is the
    /// delete path, which reaches the queue through a different function.
    #[tokio::test]
    async fn deleting_an_object_cancels_its_queued_migration() {
        let (_tmp, service) = test_service(1);
        let commit = put_commit(10);
        let object = EcObjectVersionKey::new(commit.bucket.clone(), commit.key.clone(), None);
        service
            .metadata
            .put_transcode_job(&EcTranscodeJobRecord {
                object: object.clone(),
                operation_id: Uuid::from_u128(4_243),
                source_hlc: commit.source_hlc,
                target_erasure_set_id: service.default_set.set_id,
                state: EcPlacementState::EcTranscodeQueued,
                attempt: 0,
                object_size: commit.size,
                source_content_hash_sha256: commit.content_hash_sha256,
                updated_at: 1,
                next_retry_at: None,
                failure: None,
                origin: EcTranscodeOrigin::Tiering,
                target: EcTranscodeTarget::Object,
            })
            .await
            .unwrap();

        service
            .record_delete_commit(EcObjectDeleteCommit {
                bucket: commit.bucket,
                key: commit.key,
                version_id: None,
                delete_hlc: s4_cluster::HlcTimestamp::from_record(30, 0, NodeId(1).0),
                operation_id: Uuid::from_u128(30),
            })
            .await
            .unwrap();

        assert!(
            service.metadata.list_transcode_jobs(16).await.unwrap().is_empty(),
            "a deleted object must not leave a migration queued behind it"
        );
    }

    /// A client write over its own queued job is not an abandoned migration.
    #[tokio::test]
    async fn overwriting_a_client_write_job_is_not_counted_as_abandoned() {
        let (_tmp, service) = test_service(1);
        let commit = put_commit(10);
        service.record_put_commit(commit.clone()).await.unwrap();
        let object = EcObjectVersionKey::new(commit.bucket, commit.key, None);

        let removed = service.metadata.delete_transcode_jobs_for_object(&object, 64).await.unwrap();

        assert_eq!(removed.total, 1);
        assert_eq!(removed.tiering, 0);
    }

    /// Only unfinished tiering work counts against the admission ceiling.
    #[test]
    fn the_queue_ceiling_counts_unfinished_work_only() {
        for state in [
            EcPlacementState::EcTranscodeQueued,
            EcPlacementState::EcEncoding,
            EcPlacementState::EcShardsDurable,
            EcPlacementState::EcManifestCommitted,
            EcPlacementState::EcReadVerified,
            EcPlacementState::EcFailedRetryable,
        ] {
            assert!(
                transcode_job_is_in_flight(state),
                "{state:?} still occupies the queue"
            );
        }
        for state in [
            EcPlacementState::HotReplicated,
            EcPlacementState::EcCommitted,
            EcPlacementState::HotReplicasGcEligible,
            EcPlacementState::HotReplicasPurged,
            EcPlacementState::EcFailedPermanent,
            EcPlacementState::Tombstone,
        ] {
            assert!(
                !transcode_job_is_in_flight(state),
                "{state:?} is finished with the queue"
            );
        }
    }

    #[tokio::test]
    async fn object_runtime_delete_clears_layout_and_jobs() {
        let (_tmp, service) = test_service(1);
        let commit = put_commit(10);
        service.record_put_commit(commit.clone()).await.unwrap();

        service
            .record_delete_commit(EcObjectDeleteCommit {
                bucket: commit.bucket.clone(),
                key: commit.key.clone(),
                version_id: None,
                delete_hlc: s4_cluster::HlcTimestamp::from_record(20, 0, NodeId(1).0),
                operation_id: Uuid::from_u128(20),
            })
            .await
            .unwrap();

        let object = EcObjectVersionKey::new(commit.bucket, commit.key, None);
        assert!(service.metadata.get_object_layout(&object).await.unwrap().is_none());
        assert!(service.metadata.list_transcode_jobs(16).await.unwrap().is_empty());
    }

    /// A deleted object leaves no activity mark behind.
    ///
    /// Nothing else collects these rows, so keeping them would grow the keyspace
    /// by one entry for every object that was ever read and then removed.
    #[tokio::test]
    async fn object_runtime_delete_clears_the_activity_mark() {
        let (_tmp, service) = test_service(1);
        let commit = put_commit(10);
        let object = EcObjectVersionKey::new(commit.bucket.clone(), commit.key.clone(), None);
        service
            .metadata
            .put_object_heat(&crate::ec_metadata::EcObjectHeatRecord::first_access(
                object.clone(),
                42,
                86_400_000_000_000,
            ))
            .await
            .unwrap();

        service
            .record_delete_commit(EcObjectDeleteCommit {
                bucket: commit.bucket,
                key: commit.key,
                version_id: None,
                delete_hlc: s4_cluster::HlcTimestamp::from_record(20, 0, NodeId(1).0),
                operation_id: Uuid::from_u128(21),
            })
            .await
            .unwrap();

        assert!(service.metadata.get_object_heat(&object).await.unwrap().is_none());
    }

    /// A policy written through the admin surface comes back with both halves:
    /// what was stored and what is actually in force.
    #[tokio::test]
    async fn tiering_policy_round_trips_with_its_effective_values() {
        let (_tmp, service) = test_service_with_node_id(1024, uuid_node_id());
        let request = EcAdminTieringPolicyRequest {
            cold_after_secs: Some(7 * 24 * 3_600),
            exclude_prefixes: vec!["keep-hot/".to_string()],
            ..EcAdminTieringPolicyRequest::default()
        };

        let written = service.put_tiering_policy("archive".to_string(), request).await.unwrap();
        assert_eq!(written["policy"]["cold_after_secs"], json!(7 * 24 * 3_600));

        let read = service.tiering_policy("archive".to_string()).await.unwrap();
        assert_eq!(read["policy"]["cold_after_secs"], json!(7 * 24 * 3_600));
        assert_eq!(read["effective"]["cold_after_secs"], json!(7 * 24 * 3_600));
        assert_eq!(read["effective"]["exclude_prefixes"], json!(["keep-hot/"]));
        // Untouched fields report the node's values, not zeros.
        assert_eq!(
            read["effective"]["min_object_size"],
            json!(service.default_set.profile.min_object_size)
        );

        // It is also in the local keyspace, which is what the tiering worker
        // reads without going to the network.
        assert!(service.cached_tiering_policy("archive").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn a_bucket_without_a_policy_reports_the_inherited_values() {
        let (_tmp, service) = test_service_with_node_id(1024, uuid_node_id());
        let read = service.tiering_policy("plain".to_string()).await.unwrap();

        assert_eq!(read["policy"], Value::Null);
        assert_eq!(
            read["effective"]["cold_after_secs"],
            json!(DEFAULT_EC_TIERING_COLD_AFTER_SECS)
        );
        assert_eq!(read["effective"]["exclude_prefixes"], json!([]));
    }

    /// A threshold shorter than a day turns tiering into a migration generator,
    /// so the admin surface refuses it outright.
    #[tokio::test]
    async fn put_tiering_policy_refuses_dangerous_values() {
        let (_tmp, service) = test_service_with_node_id(1024, uuid_node_id());

        let too_short = EcAdminTieringPolicyRequest {
            cold_after_secs: Some(60),
            ..EcAdminTieringPolicyRequest::default()
        };
        assert!(service.put_tiering_policy("archive".to_string(), too_short).await.is_err());

        let empty_prefix = EcAdminTieringPolicyRequest {
            exclude_prefixes: vec![String::new()],
            ..EcAdminTieringPolicyRequest::default()
        };
        assert!(service.put_tiering_policy("archive".to_string(), empty_prefix).await.is_err());

        // Nothing was stored by either attempt.
        assert!(service.cached_tiering_policy("archive").await.unwrap().is_none());
    }

    /// Writing an empty policy is how a bucket goes back to inheriting.
    #[tokio::test]
    async fn an_empty_policy_clears_every_override() {
        let (_tmp, service) = test_service_with_node_id(1024, uuid_node_id());
        service
            .put_tiering_policy(
                "archive".to_string(),
                EcAdminTieringPolicyRequest {
                    cold_after_secs: Some(7 * 24 * 3_600),
                    ..EcAdminTieringPolicyRequest::default()
                },
            )
            .await
            .unwrap();

        service
            .put_tiering_policy(
                "archive".to_string(),
                EcAdminTieringPolicyRequest::default(),
            )
            .await
            .unwrap();

        let read = service.tiering_policy("archive".to_string()).await.unwrap();
        assert_eq!(read["policy"]["cold_after_secs"], Value::Null);
        assert_eq!(
            read["effective"]["cold_after_secs"],
            json!(DEFAULT_EC_TIERING_COLD_AFTER_SECS)
        );
    }

    #[tokio::test]
    async fn a_dry_run_without_a_read_runtime_says_so() {
        let (_tmp, service) = test_service(1024);
        let error = service
            .tiering_dry_run(EcAdminTieringDryRunRequest {
                bucket: "archive".to_string(),
                prefix: None,
                max_objects: 10,
                max_samples: 4,
            })
            .await
            .unwrap_err();
        // A dry run that silently reports zero on an unwired node would read as
        // "nothing to migrate", which is the wrong conclusion entirely.
        assert!(matches!(error, ClusterError::Config(_)));
    }

    /// A pool that has not been told it may hold several sets must not grow one.
    ///
    /// The refusal names the switch, because "nothing happened" is the worst
    /// possible answer to an operator who has just added nine nodes.
    #[tokio::test]
    async fn adding_a_set_is_refused_while_the_feature_is_off() {
        let (_tmp, service) = test_service(1);
        let request = EcAdminAddSetRequest {
            pool: "ec".to_string(),
            node_ids: additional_set(1)
                .slot_assignment
                .distinct_nodes()
                .iter()
                .map(ToString::to_string)
                .collect(),
        };

        let error = service.add_erasure_set(request).await.unwrap_err().to_string();

        assert!(
            error.contains(MULTI_SET_ENABLED_ENV),
            "the refusal must name the switch that is off: {error}"
        );
    }

    /// The width of a set is fixed by the profile, and the check happens before
    /// anything durable is touched.
    #[tokio::test]
    async fn a_set_of_the_wrong_width_is_refused_before_anything_is_written() {
        let (_tmp, service) = multi_set_service(1);
        let width = usize::from(service.set_builder.profile().total_shards());
        let mut node_ids: Vec<String> = additional_set(1)
            .slot_assignment
            .distinct_nodes()
            .iter()
            .map(ToString::to_string)
            .collect();
        node_ids.pop();

        let error = service
            .add_erasure_set(EcAdminAddSetRequest {
                pool: "ec".to_string(),
                node_ids,
            })
            .await
            .unwrap_err()
            .to_string();

        assert!(
            error.contains(&format!("exactly {width} node(s)")),
            "the refusal must say what width is required: {error}"
        );
        // Nothing was published, and nothing was cached either.
        assert!(service.metadata.get_erasure_set_roster().await.unwrap().is_none());
    }

    /// A set built on nodes another set already uses is refused.
    ///
    /// It would add no capacity, and the pool's free space is the sum of its
    /// sets': a node counted twice makes that sum promise room that is not
    /// there. Refused before anything is written.
    #[tokio::test]
    async fn a_set_on_nodes_another_set_already_uses_is_refused() {
        let (_tmp, service) = multi_set_service(1);
        service
            .configure_cluster_membership(Arc::new(s4_cluster::MockClusterMembership::new(true)));
        let taken: Vec<String> = service
            .default_set
            .slot_assignment
            .distinct_nodes()
            .iter()
            .map(ToString::to_string)
            .collect();

        let error = service
            .add_erasure_set(EcAdminAddSetRequest {
                pool: "ec".to_string(),
                node_ids: taken,
            })
            .await
            .unwrap_err()
            .to_string();

        assert!(
            error.contains("already holds slots in erasure set"),
            "the refusal must name the set already using the node: {error}"
        );
        assert!(service.metadata.get_erasure_set_roster().await.unwrap().is_none());
    }

    /// The pool's free space is the sum of its sets', which is only true while
    /// the sets sit on disjoint nodes.
    ///
    /// Guards the rule the refusal above exists to keep: every set of the pool
    /// is built on nodes no other set holds.
    #[tokio::test]
    async fn the_sets_of_a_pool_never_share_a_node() {
        let (_tmp, service) = multi_set_service(1);
        cache_second_set_roster(&service).await;
        let sets = service.placement.sets();
        assert_eq!(sets.len(), 2);

        let mut seen = std::collections::HashSet::new();
        for set in sets.sets() {
            for node_id in set.slot_assignment.distinct_nodes() {
                assert!(
                    seen.insert(node_id),
                    "node {node_id} is held by more than one set of the pool"
                );
            }
        }
    }

    /// An added set never takes the inherited set's identifier.
    ///
    /// That value carries the pool's EC v1 identity, and every manifest written
    /// before the decoupling resolves to it (decision EC2-041). Handing it to a
    /// second set would make those objects resolve to the wrong nodes.
    #[test]
    fn an_added_set_never_takes_the_inherited_identifier() {
        let (_tmp, service) = test_service(1);
        let inherited = service.default_set.set_id;
        let mut roster = EcErasureSetRoster::empty(9);

        let first = service.next_set_id(&roster);
        assert_ne!(first, inherited);
        roster.sets.push(EcErasureSetSpec {
            set_id: first,
            node_ids: vec![NodeId(1)],
            state: ErasureSetState::Active,
            created_at: 1,
        });

        let second = service.next_set_id(&roster);
        assert_ne!(second, inherited);
        assert_ne!(second, first);
    }

    /// Every node must rebuild the same pool from the same roster.
    ///
    /// The roster carries the operator's inputs, not a serialized set, so this
    /// is where "the same nodes mean the same set everywhere" is checked.
    #[test]
    fn a_roster_rebuilds_the_pool_the_same_way_on_every_node() {
        let (_tmp, service) = test_service(1);
        let extra = additional_set(1);
        let roster = EcErasureSetRoster {
            pool_id: 9,
            sets: vec![EcErasureSetSpec {
                set_id: 7,
                node_ids: extra.slot_assignment.distinct_nodes(),
                state: ErasureSetState::Active,
                created_at: 42,
            }],
            inherited_state: ErasureSetState::Active,
            inherited_node_ids: Vec::new(),
            hlc: HlcTimestamp::from_record(1, 1, 1),
            updated_at: 42,
        };

        let registry = service.registry_from_roster(&roster, &SetLayouts::default()).unwrap();

        assert_eq!(registry.len(), 2);
        let rebuilt = registry.get(7).unwrap();
        assert_eq!(
            rebuilt.slot_assignment.distinct_nodes(),
            extra.slot_assignment.distinct_nodes()
        );
        assert_eq!(rebuilt.profile, service.set_builder.profile().clone());
        assert!(!rebuilt.inherited);
        // The pool's own set comes back untouched apart from its state, which
        // is the only thing the roster may say about it.
        let default_set = registry.get(service.default_set.set_id).unwrap();
        assert_eq!(
            default_set.slot_assignment.distinct_nodes(),
            service.default_set.slot_assignment.distinct_nodes()
        );
        assert_eq!(default_set.inherited, service.default_set.inherited);

        // Twice from the same roster is the same pool, which is what makes two
        // nodes agree.
        assert_eq!(
            registry,
            service.registry_from_roster(&roster, &SetLayouts::default()).unwrap()
        );
    }

    /// A node outside the pool cannot be made into a set.
    #[test]
    fn a_roster_naming_a_node_outside_the_pool_is_refused() {
        let (_tmp, service) = test_service(1);
        let mut node_ids = additional_set(1).slot_assignment.distinct_nodes();
        node_ids[0] = NodeId(9_999);
        let roster = EcErasureSetRoster {
            pool_id: 9,
            sets: vec![EcErasureSetSpec {
                set_id: 7,
                node_ids,
                state: ErasureSetState::Active,
                created_at: 42,
            }],
            inherited_state: ErasureSetState::Active,
            inherited_node_ids: Vec::new(),
            hlc: HlcTimestamp::from_record(1, 1, 1),
            updated_at: 42,
        };

        let rejection = service.registry_from_roster(&roster, &SetLayouts::default()).unwrap_err();

        // A node the pool does not have is this node's configuration against the
        // pool's, which is the kind of rejection a node must not start through.
        assert!(
            matches!(rejection, RosterRejection::NodeConfiguration(_)),
            "a node outside the pool is a node-configuration rejection: {rejection:?}"
        );
        let error = rejection.into_cluster_error().to_string();
        assert!(
            error.contains("is not a node of this pool"),
            "the refusal must say the node is not in the pool: {error}"
        );
    }

    /// Taking the last writable set out of rotation would leave every new object
    /// with nowhere to go, so it is refused rather than discovered on the next
    /// PUT.
    #[tokio::test]
    async fn the_last_set_that_takes_writes_cannot_be_deactivated() {
        let (_tmp, service) = multi_set_service(1);
        // The pool holds two sets and only one of them still takes writes.
        cache_second_set_roster_in_state(&service, ErasureSetState::ReadOnly).await;
        let sets = service.placement.sets();
        assert_eq!(sets.active_sets().len(), 1);
        let survivor = sets.active_sets()[0].set_id;

        let error = service
            .deactivate_erasure_set(EcAdminDeactivateSetRequest {
                pool: "ec".to_string(),
                set_id: survivor,
            })
            .await
            .unwrap_err()
            .to_string();

        assert!(
            error.contains("nowhere to place a new object"),
            "the refusal must say why: {error}"
        );
    }

    /// Deactivating a set the pool does not have is an error, not a silent
    /// success.
    #[tokio::test]
    async fn deactivating_an_unknown_set_is_refused() {
        let (_tmp, service) = multi_set_service(1);
        cache_second_set_roster(&service).await;

        let error = service
            .deactivate_erasure_set(EcAdminDeactivateSetRequest {
                pool: "ec".to_string(),
                set_id: 4_242,
            })
            .await
            .unwrap_err()
            .to_string();

        assert!(
            error.contains("no erasure set 4242"),
            "the refusal must name the set: {error}"
        );
    }

    /// Pool health gains where the set's slots sit and whether the pool's sets
    /// still meet their rules, without disturbing a field that was already
    /// there (`EC2-ZONE-04-001`).
    #[tokio::test]
    async fn pool_health_reports_the_placement_and_the_drift_check() {
        let (_tmp, service) = test_service(1);

        let health = service.pool_health("ec").await.unwrap();

        // The fields that were always here still answer as they did.
        assert!(health["topology"]["health"].is_string());
        assert!(health["topology"]["risks"].is_array());
        assert_eq!(
            health["erasure_set"],
            service.set_admin_snapshot(&service.default_set)
        );

        // The placement is added beside them, never in place of one.
        let placement = &health["erasure_set"]["placement"];
        assert_eq!(placement["level"], json!("rack"));
        assert_eq!(
            placement["slots"].as_array().unwrap().len(),
            usize::from(service.default_set.profile.total_shards())
        );

        // The drift check runs and finds the pool's single EC v1 set, which
        // carries no placement guarantee and so can never be a violator.
        let drift = &health["topology"]["layout_drift"];
        assert_eq!(drift["sets_checked"], json!(1));
        assert_eq!(drift["sets_without_guarantee"], json!(1));
        assert_eq!(drift["sets_drifted"], json!(0));
        assert_eq!(drift["violators"], json!(0));
    }

    /// With the LRC group mode off there is no rule to apply. The refusal names
    /// the variable rather than quietly doing nothing and answering "applied".
    #[tokio::test]
    async fn applying_a_topology_rule_is_refused_while_the_mode_is_off() {
        let (_tmp, service) = test_service(1);

        let error = service
            .apply_pool_topology(EcAdminApplyTopologyRequest {
                pool: "ec".to_string(),
                set_id: None,
                dry_run: false,
            })
            .await
            .unwrap_err()
            .to_string();

        assert!(error.contains("no topology rule to apply"), "{error}");
        assert!(error.contains(LRC_GROUP_DOMAIN_ENV), "{error}");
    }

    /// An unknown pool and an unknown set are both refused by name, before
    /// anything is written.
    #[tokio::test]
    async fn applying_a_topology_rule_refuses_an_unknown_pool() {
        let (_tmp, service) = test_service(1);

        let error = service
            .apply_pool_topology(EcAdminApplyTopologyRequest {
                pool: "not-this-pool".to_string(),
                set_id: None,
                dry_run: false,
            })
            .await
            .unwrap_err()
            .to_string();

        assert!(error.contains("not-this-pool"), "{error}");
    }

    /// A pool with one set answers the health route exactly as it did before
    /// multi-set pools existed.
    ///
    /// The per-set view is added alongside `erasure_set`, never in place of it:
    /// a caller written against EC v1 keeps working.
    #[tokio::test]
    async fn a_single_set_pool_keeps_the_health_response_it_had() {
        let (_tmp, service) = test_service(1);

        let health = service.pool_health("ec").await.unwrap();

        assert_eq!(
            health["erasure_set"],
            service.set_admin_snapshot(&service.default_set)
        );
        // The new view is present and describes the same single set.
        assert_eq!(health["erasure_sets"]["sets"].as_array().unwrap().len(), 1);
        assert_eq!(
            health["erasure_sets"]["sets"][0]["set_id"],
            json!(service.default_set.set_id)
        );
        assert_eq!(health["erasure_sets"]["multi_set_enabled"], json!(false));

        // Listing is a read, not management, so it answers with the feature off
        // too. Refusing it would take away the only way to see that the pool has
        // one set.
        let listing = service.list_erasure_sets("ec").await.unwrap();
        assert_eq!(listing["sets"].as_array().unwrap().len(), 1);
        // The fixture's pool is two sets wide, so the nodes the inherited set
        // does not hold are reported as free — that list is what an operator
        // hands back to create the next set.
        let free = listing["unassigned_nodes"].as_array().unwrap().len();
        let width = usize::from(service.set_builder.profile().total_shards());
        assert_eq!(free, service.set_builder.pool_nodes().len() - width);
    }

    /// The pool's free space is the sum of its sets', because both come from
    /// one computation.
    #[tokio::test]
    async fn the_pool_capacity_total_is_the_sum_of_its_sets() {
        let (_tmp, service) = multi_set_service(1);
        // Through the roster, the way a node that did not serve the request
        // learns about a set: cached locally, then picked up by a refresh.
        cache_second_set_roster(&service).await;
        let sets = service.placement.sets();
        assert_eq!(sets.len(), 2, "the refresh must pick the added set up");
        let first = sets.sets()[0].slot_assignment.distinct_nodes();
        let second = sets.sets()[1].slot_assignment.distinct_nodes();
        const GIB: u64 = 1024 * 1024 * 1024;
        service.placement.configure_capacity_source(Arc::new(MeasuredNodes::new(&[
            (first, 100 * GIB, 30 * GIB),
            (second, 100 * GIB, 70 * GIB),
        ])));

        let listing = service.list_erasure_sets("ec").await.unwrap();
        let report = service.placement.capacity_report();

        assert_eq!(report.sets_total, 2);
        assert_eq!(report.sets_measured, 2);
        assert_eq!(report.available_bytes, 100 * GIB);
        assert_eq!(
            listing["capacity"]["available_bytes"],
            json!(report.available_bytes)
        );
        let per_set: u64 = listing["sets"]
            .as_array()
            .unwrap()
            .iter()
            .map(|set| set["capacity"]["available_bytes"].as_u64().unwrap())
            .sum();
        assert_eq!(per_set, report.available_bytes);
    }

    /// A set nobody has reported on is unknown, not empty.
    /// Put a second set in the local roster cache and adopt it, the way a node
    /// adopts a set an operator added on another node.
    async fn cache_second_set_roster(service: &EcOperationsService) {
        cache_second_set_roster_in_state(service, ErasureSetState::Active).await
    }

    async fn cache_second_set_roster_in_state(
        service: &EcOperationsService,
        state: ErasureSetState,
    ) {
        let extra = additional_set(1);
        // A node that adopted the set knows its layout record, as every node
        // does once it has read it. Without one the set would be held from
        // writes (`a_set_whose_record_is_unknown_takes_no_writes_until_it_is_read`).
        if service.metadata.get_set_layout_record(FIRST_SET_ID).await.unwrap().is_none() {
            service
                .metadata
                .put_set_layout_record(&EcSetLayoutRecord::v1(
                    &extra,
                    EcTopologyPolicy::Strict,
                    1,
                    HlcTimestamp::from_record(1, 1, 1),
                ))
                .await
                .unwrap();
        }
        service
            .metadata
            .put_erasure_set_roster(&EcErasureSetRoster {
                pool_id: 9,
                sets: vec![EcErasureSetSpec {
                    set_id: FIRST_SET_ID,
                    node_ids: extra.slot_assignment.distinct_nodes(),
                    state,
                    created_at: 1,
                }],
                inherited_state: ErasureSetState::Active,
                inherited_node_ids: Vec::new(),
                hlc: HlcTimestamp::from_record(1, 1, 1),
                updated_at: 1,
            })
            .await
            .unwrap();
        service.refresh_erasure_sets().await.unwrap();
    }

    #[tokio::test]
    async fn an_unmeasured_set_is_reported_as_unknown_rather_than_full() {
        let (_tmp, service) = multi_set_service(1);
        cache_second_set_roster(&service).await;

        let listing = service.list_erasure_sets("ec").await.unwrap();

        for set in listing["sets"].as_array().unwrap() {
            assert_eq!(set["capacity"]["measured"], json!(false));
            assert_eq!(set["capacity"]["available_bytes"], Value::Null);
            assert_eq!(set["capacity"]["full"], json!(false));
        }
        assert_eq!(listing["capacity"]["sets_measured"], json!(0));
    }

    /// A membership view with a fixed answer per node.
    ///
    /// A node absent from the map is absent from the view, which is what a
    /// machine replaced by one with a fresh identity looks like to the pool.
    struct TestMembership {
        statuses: HashMap<NodeId, s4_cluster::NodeStatus>,
    }

    impl TestMembership {
        /// Every node of the fixture pool seen alive.
        fn all_alive() -> Self {
            let statuses = test_set_builder(1)
                .pool_nodes()
                .into_iter()
                .map(|node_id| (node_id, s4_cluster::NodeStatus::Alive))
                .collect();
            Self { statuses }
        }

        /// Every node alive except `node_id`, which the view reports as `status`.
        fn all_alive_but(node_id: NodeId, status: s4_cluster::NodeStatus) -> Self {
            let mut view = Self::all_alive();
            view.statuses.insert(node_id, status);
            view
        }

        /// Every node alive except `node_id`, which the view has no entry for.
        fn all_alive_without(node_id: NodeId) -> Self {
            let mut view = Self::all_alive();
            view.statuses.remove(&node_id);
            view
        }
    }

    impl s4_cluster::ClusterMembershipQuery for TestMembership {
        fn pool_nodes_support_v2(&self, _pool_nodes: &[NodeId]) -> bool {
            true
        }

        fn nodes_below_protocol(
            &self,
            _pool_nodes: &[NodeId],
            _min_protocol_version: u32,
        ) -> Vec<NodeId> {
            Vec::new()
        }

        fn node_status(&self, node_id: NodeId) -> Option<s4_cluster::NodeStatus> {
            self.statuses.get(&node_id).copied()
        }
    }

    fn listed_node(listing: &Value, node_id: NodeId) -> &Value {
        listing["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .find(|node| node["node_id"] == json!(node_id.to_string()))
            .unwrap_or_else(|| panic!("node {node_id} is missing from the sets listing"))
    }

    /// The listing has to put identity, slots and liveness side by side: that
    /// is the answer an operator writes down before an outage and reads after
    /// one (`EC2-NREP-01-001`).
    #[tokio::test]
    async fn sets_listing_names_each_node_its_slots_and_how_the_pool_sees_it() {
        let (_tmp, service) = test_service(1);
        service.configure_cluster_membership(Arc::new(TestMembership::all_alive_but(
            NodeId(4),
            s4_cluster::NodeStatus::Dead,
        )));

        let listing = service.list_erasure_sets("ec").await.unwrap();

        assert_eq!(listing["membership_available"], json!(true));
        let holder = listed_node(&listing, NodeId(4));
        assert_eq!(holder["set_id"], json!(service.default_set.set_id));
        assert_eq!(holder["membership"], json!("dead"));
        // Slot 3 is NodeId(4)'s: the fixture set runs NodeId(1)..NodeId(6) over
        // slots 0..5 in order.
        assert_eq!(holder["slots"], json!([3]));

        let neighbour = listed_node(&listing, NodeId(5));
        assert_eq!(neighbour["membership"], json!("alive"));
        assert_eq!(neighbour["slots"], json!([4]));

        // A node of the pool that holds no slot keeps saying so, and now also
        // says it is there to be given some.
        let free = listed_node(&listing, NodeId(7));
        assert_eq!(free["set_id"], Value::Null);
        assert_eq!(free["slots"], json!([]));
        assert_eq!(free["membership"], json!("alive"));
        assert!(listing["unassigned_nodes"]
            .as_array()
            .unwrap()
            .contains(&json!(NodeId(7).to_string())));
    }

    /// A node with no membership view knows nothing about anyone, and the
    /// listing has to say that rather than call the whole pool dead.
    #[tokio::test]
    async fn sets_listing_without_a_membership_view_says_so_instead_of_guessing() {
        let (_tmp, service) = test_service(1);

        let listing = service.list_erasure_sets("ec").await.unwrap();

        assert_eq!(listing["membership_available"], json!(false));
        for node in listing["nodes"].as_array().unwrap() {
            assert_eq!(node["membership"], json!("unknown"));
        }
    }

    /// An owner the pool cannot see is named in pool health, apart from
    /// topology risk, because the two lead to different actions
    /// (`EC2-NREP-01-003`).
    #[tokio::test]
    async fn pool_health_names_the_slot_owner_it_cannot_see() {
        let (_tmp, service) = test_service(1);
        service
            .configure_cluster_membership(Arc::new(TestMembership::all_alive_without(NodeId(4))));

        let health = service.pool_health("ec").await.unwrap();

        let owners = &health["slot_owners"];
        assert_eq!(owners["membership_available"], json!(true));
        let expected_total = service.set_builder.pool_nodes().len();
        assert_eq!(owners["total"], json!(expected_total));
        assert_eq!(owners["seen_alive"], json!(expected_total - 1));

        let missing = owners["not_seen_alive"].as_array().unwrap();
        assert_eq!(missing.len(), 1);
        assert_eq!(missing[0]["node_id"], json!(NodeId(4).to_string()));
        assert_eq!(missing[0]["slots"], json!([3]));
        // Absent from the view, not reported dead by it: the two are different
        // states and the operator acts on them differently.
        assert_eq!(missing[0]["membership"], json!("not_a_member"));

        // The dead owner never becomes a topology risk. The fixture has a risk
        // of its own — its nodes carry no labels — and it still says nothing
        // about a node being down.
        let risks = health["topology"]["risks"].as_array().unwrap();
        assert!(
            !risks.iter().any(|risk| risk
                .as_str()
                .is_some_and(|risk| risk.contains(&NodeId(4).to_string()))),
            "{risks:?}"
        );

        // And it does not move the pool's verdict: the same pool with every
        // owner seen alive answers the same.
        let (_seeing_tmp, seeing) = test_service(1);
        seeing.configure_cluster_membership(Arc::new(TestMembership::all_alive()));
        let seeing_health = seeing.pool_health("ec").await.unwrap();
        assert_eq!(seeing_health["slot_owners"]["not_seen_alive"], json!([]));
        assert_eq!(health["health"], seeing_health["health"]);
    }

    /// Without a membership view the report withholds the answer rather than
    /// reporting an empty list as "everyone is up".
    #[tokio::test]
    async fn pool_health_without_a_membership_view_reports_no_verdict_on_owners() {
        let (_tmp, service) = test_service(1);

        let health = service.pool_health("ec").await.unwrap();

        let owners = &health["slot_owners"];
        assert_eq!(owners["membership_available"], json!(false));
        assert_eq!(owners["seen_alive"], json!(0));
        assert_eq!(owners["not_seen_alive"], json!([]));
        assert_eq!(
            owners["total"],
            json!(service.set_builder.pool_nodes().len())
        );
    }

    // -----------------------------------------------------------------------
    // Returning to the pool (plan 08, phase 2)
    // -----------------------------------------------------------------------

    /// A service on an existing EC metadata directory, as a restart produces.
    fn reopen_service_at(path: &std::path::Path, node_id: NodeId) -> EcOperationsService {
        let _guard = TieringEnvGuard::acquire();
        EcOperationsService::open(
            path,
            "ec",
            active_set_registry(1),
            test_set_builder(1),
            node_id,
        )
        .unwrap()
    }

    fn read_request() -> EcObjectReadRequest {
        EcObjectReadRequest {
            bucket: "bucket".to_string(),
            key: "object.bin".to_string(),
            version_id: None,
            range: None,
        }
    }

    /// The everyday start: an identity this node generated, nothing owed, reads
    /// served exactly as before this gate existed.
    #[tokio::test]
    async fn a_new_node_serves_reads_and_writes_down_that_it_was_here() {
        let (tmp, service) = test_service(1);

        let status = service
            .evaluate_node_rejoin(NodeIdentityOrigin::Generated)
            .await
            .expect("evaluate the return");

        assert_eq!(status.reason, crate::ec_rejoin::EcRejoinReason::NewIdentity);
        assert!(service.serves_ec_reads());

        // The record the next start reads its downtime from.
        let presence = service
            .metadata
            .get_node_presence()
            .await
            .expect("read the presence record")
            .expect("a started node records itself");
        assert_eq!(presence.node_id, NodeId(1));
        assert_eq!(presence.pool, "ec");
        assert!(!presence.rebuild_pending);
        drop(tmp);
    }

    /// A machine that took over a dead node's identity holds slots it cannot
    /// answer for, and neither the read path nor the health route pretends
    /// otherwise (`EC2-NREP-02-002`, `EC2-NREP-02-004`).
    #[tokio::test]
    async fn an_adopted_identity_is_held_out_of_the_read_path_and_health_says_so() {
        let (_tmp, service) = test_service(1);

        let status = service
            .evaluate_node_rejoin(NodeIdentityOrigin::Adopted)
            .await
            .expect("evaluate the return");

        assert_eq!(
            status.decision,
            crate::ec_repair::EcNodeRejoinDecision::NeedsBootstrap
        );
        assert!(!service.serves_ec_reads());

        // Both halves of the read surface refuse, and both say why rather than
        // answering "not found" for an object the pool still holds.
        for reason in [
            match service.read_object(read_request()).await {
                Err(EcObjectReadError::Unavailable { reason }) => reason,
                other => panic!("a held-back node must refuse a read: {other:?}"),
            },
            match service
                .head_object(EcObjectHeadRequest {
                    bucket: "bucket".to_string(),
                    key: "object.bin".to_string(),
                    version_id: None,
                })
                .await
            {
                Err(EcObjectReadError::Unavailable { reason }) => reason,
                other => panic!("a held-back node must refuse a head: {other:?}"),
            },
        ] {
            assert!(reason.contains("needs_bootstrap"), "{reason}");
            assert!(reason.contains("rejoin/approve"), "{reason}");
        }

        // The pool cannot call itself healthy through a node that is not
        // serving its slots, however alive that node looks in membership. The
        // rule itself is checked in
        // `a_node_that_is_not_serving_its_slots_takes_the_pools_verdict_with_it`,
        // because this fixture's pool is degraded for topology reasons too.
        let health = service.pool_health("ec").await.unwrap();
        assert_eq!(health["health"], json!("degraded"));
        let rejoin = &health["rejoin"];
        assert_eq!(rejoin["serving_ec_reads"], json!(false));
        assert_eq!(rejoin["decision"], json!("needs_bootstrap"));
        assert_eq!(rejoin["reason"], json!("storage_adopted"));
        assert_eq!(rejoin["node_id"], json!(NodeId(1).to_string()));
        assert_eq!(rejoin["local_shards_held"], json!(0));
        assert_eq!(rejoin["local_shards_truncated"], json!(false));
        // Slots it owns and shards it holds, side by side: that pairing is the
        // whole of what an operator has to see here.
        assert!(
            !rejoin["slots"].as_array().unwrap().is_empty(),
            "the pool gives this identity slots: {rejoin}"
        );
    }

    /// Restarting a node that still owes shards is not rebuilding it.
    ///
    /// The second start finds an identity on disk and a presence record seconds
    /// old; without the obligation being carried in that record, this is the
    /// shape that would quietly clear the gate.
    #[tokio::test]
    async fn a_held_back_node_is_still_held_after_a_restart() {
        let (tmp, service) = test_service(1);
        service
            .evaluate_node_rejoin(NodeIdentityOrigin::Adopted)
            .await
            .expect("evaluate the return");
        assert!(!service.serves_ec_reads());
        drop(service);

        let restarted = reopen_service_at(tmp.path(), NodeId(1));
        let status = restarted
            .evaluate_node_rejoin(NodeIdentityOrigin::Existing)
            .await
            .expect("evaluate the return again");

        assert_eq!(
            status.decision,
            crate::ec_repair::EcNodeRejoinDecision::NeedsBootstrap
        );
        assert_eq!(
            status.reason,
            crate::ec_rejoin::EcRejoinReason::RebuildPending
        );
        assert!(!restarted.serves_ec_reads());
    }

    /// The operator's way out, and what it is not: a statement about one
    /// identity and one return, never a setting (`EC2-NREP-02-003`).
    #[tokio::test]
    async fn an_operator_approval_puts_this_node_back_in_the_read_path() {
        let (tmp, service) = test_service(1);
        service
            .evaluate_node_rejoin(NodeIdentityOrigin::Adopted)
            .await
            .expect("evaluate the return");

        // Addressed to a node: an approval meant for another one is refused
        // rather than applied to whichever node the request reached.
        let wrong = service
            .approve_node_rejoin(EcAdminApproveRejoinRequest {
                pool: "ec".to_string(),
                node_id: NodeId(2).to_string(),
            })
            .await
            .expect_err("an approval for another node must be refused");
        assert!(
            wrong.to_string().contains(&NodeId(2).to_string()),
            "{wrong}"
        );
        assert!(!service.serves_ec_reads());

        let approved = service
            .approve_node_rejoin(EcAdminApproveRejoinRequest {
                pool: "ec".to_string(),
                node_id: NodeId(1).to_string(),
            })
            .await
            .expect("approve this node's return");
        assert_eq!(approved["changed"], json!(true));
        assert_eq!(approved["serving_ec_reads"], json!(true));
        // The decision has not changed its mind, and the answer says who did.
        assert_eq!(approved["decision"], json!("needs_bootstrap"));
        assert_eq!(approved["admin_override"], json!(true));
        assert!(service.serves_ec_reads());
        assert!(
            service.read_object(read_request()).await.is_err_and(|error| !matches!(
                &error,
                EcObjectReadError::Unavailable { reason } if reason.contains("rejoin/approve")
            ))
        );

        // Approving a node that already serves changes nothing, and says so.
        let again = service
            .approve_node_rejoin(EcAdminApproveRejoinRequest {
                pool: "ec".to_string(),
                node_id: NodeId(1).to_string(),
            })
            .await
            .expect("a second approval is a no-op");
        assert_eq!(again["changed"], json!(false));

        // And it is not a setting: the obligation is cleared for this return,
        // so the next start judges the evidence rather than the approval.
        drop(service);
        let restarted = reopen_service_at(tmp.path(), NodeId(1));
        let status = restarted
            .evaluate_node_rejoin(NodeIdentityOrigin::Existing)
            .await
            .expect("evaluate the return again");
        assert!(status.serves_ec_reads());
        assert!(!status.admin_approved);
    }

    /// With the gate disarmed the verdict is still reached and still published,
    /// and nothing is refused. This is the switch the whole plan rolls back by,
    /// and the shape an acceptance run uses to prove the gate is what refuses.
    #[tokio::test]
    async fn a_disarmed_gate_reports_the_verdict_and_serves_anyway() {
        struct RejoinEnvReset;
        impl Drop for RejoinEnvReset {
            fn drop(&mut self) {
                std::env::remove_var(crate::ec_rejoin::REJOIN_GATE_ENABLED_ENV);
            }
        }

        let service = {
            let _guard = TieringEnvGuard::acquire();
            std::env::set_var(crate::ec_rejoin::REJOIN_GATE_ENABLED_ENV, "false");
            let _reset = RejoinEnvReset;
            let tmp = tempfile::tempdir().unwrap();
            let service = EcOperationsService::open(
                tmp.path(),
                "ec",
                active_set_registry(1),
                test_set_builder(1),
                NodeId(1),
            )
            .unwrap();
            // The directory has to outlive the service, and this test never
            // reopens it.
            std::mem::forget(tmp);
            service
        };

        let status = service
            .evaluate_node_rejoin(NodeIdentityOrigin::Adopted)
            .await
            .expect("evaluate the return");

        assert_eq!(
            status.decision,
            crate::ec_repair::EcNodeRejoinDecision::NeedsBootstrap
        );
        assert!(!status.enforced);
        assert!(service.serves_ec_reads());

        let health = service.pool_health("ec").await.unwrap();
        assert_eq!(health["rejoin"]["enforced"], json!(false));
        assert_eq!(health["rejoin"]["serving_ec_reads"], json!(true));
        assert_eq!(health["rejoin"]["decision"], json!("needs_bootstrap"));
        assert_eq!(health["config"]["rejoin_gate_enforced"], json!(false));
        // The verdict is the one a pool of this shape has anyway: a disarmed
        // gate takes nothing away.
        assert_eq!(
            health["health"],
            json!(pool_health_verdict(true, 0, 0, true))
        );
    }

    /// The rule the pool's verdict follows, with the input plan 08 added.
    ///
    /// Checked apart from a live pool because the fixture pool is degraded for
    /// topology reasons anyway, and an assertion that cannot fail for the
    /// reason it names proves nothing.
    #[test]
    fn a_node_that_is_not_serving_its_slots_takes_the_pools_verdict_with_it() {
        // Nothing wrong anywhere: the only shape that reads healthy.
        assert_eq!(pool_health_verdict(false, 0, 0, true), "healthy");
        // Each of the three older inputs still takes it away on its own.
        assert_eq!(pool_health_verdict(true, 0, 0, true), "degraded");
        assert_eq!(pool_health_verdict(false, 1, 0, true), "degraded");
        assert_eq!(pool_health_verdict(false, 0, 1, true), "degraded");
        // And so does a node that holds slots it cannot answer for, which is
        // the state none of the other three can see.
        assert_eq!(pool_health_verdict(false, 0, 0, false), "degraded");
    }

    /// A node the pool gives no slot owes it nothing, whatever it was told
    /// about its identity.
    #[tokio::test]
    async fn a_node_outside_the_set_layout_is_never_held() {
        let (_tmp, service) = test_service_with_node_id(1, uuid_node_id());

        let status = service
            .evaluate_node_rejoin(NodeIdentityOrigin::Adopted)
            .await
            .expect("evaluate the return");

        assert_eq!(
            status.reason,
            crate::ec_rejoin::EcRejoinReason::NoSlotsOwned
        );
        assert!(service.serves_ec_reads());
    }

    fn test_service(min_object_size: u64) -> (tempfile::TempDir, EcOperationsService) {
        test_service_with_node_id(min_object_size, NodeId(1))
    }

    /// A service whose node id is the size a real one is.
    ///
    /// Node ids are UUIDs, so they fill a `u128` and do not fit in a JSON
    /// number. A test that uses `NodeId(1)` cannot tell a value that is
    /// rendered correctly from one that would fail on a real node, which is
    /// exactly how a panic in the admin surface reached a cluster once.
    fn test_service_with_node_id(
        min_object_size: u64,
        node_id: NodeId,
    ) -> (tempfile::TempDir, EcOperationsService) {
        // Opening a service parses `S4_EC_TIERING_*`, so it has to take the
        // same lock as the tests that set those variables. Without it a test
        // that deliberately sets an invalid value makes every service opened
        // in parallel fail to parse, and the failure lands on whichever test
        // was unlucky.
        let _guard = TieringEnvGuard::acquire();
        let tmp = tempfile::tempdir().unwrap();
        let service = EcOperationsService::open(
            tmp.path(),
            "ec",
            active_set_registry(min_object_size),
            test_set_builder(1),
            node_id,
        )
        .unwrap();
        (tmp, service)
    }

    /// Open a test service with packed segments enabled under the tiering env lock.
    fn packed_test_service(min_object_size: u64) -> (tempfile::TempDir, EcOperationsService) {
        struct PackedEnvReset;
        impl Drop for PackedEnvReset {
            fn drop(&mut self) {
                std::env::remove_var(crate::ec_packed_segments::PACKED_SEGMENTS_ENABLED_ENV);
            }
        }

        let _guard = TieringEnvGuard::acquire();
        std::env::set_var(
            crate::ec_packed_segments::PACKED_SEGMENTS_ENABLED_ENV,
            "true",
        );
        let _reset = PackedEnvReset;
        let tmp = tempfile::tempdir().unwrap();
        let service = EcOperationsService::open(
            tmp.path(),
            "ec",
            active_set_registry(min_object_size),
            test_set_builder(1),
            NodeId(1),
        )
        .unwrap();
        drop(_reset);
        (tmp, service)
    }

    fn uuid_node_id() -> NodeId {
        NodeId(Uuid::new_v4().as_u128())
    }

    /// EC2-READ-04-001: a node says which read it is doing, so that a pool
    /// part way through the switch can be told from one that is not.
    ///
    /// The setting is per node and nothing on disk depends on it, so a mixed
    /// pool is correct by construction — but only readable if each node reports
    /// its own answer.
    #[tokio::test]
    async fn pool_health_reports_whether_this_node_narrows_its_reads() {
        for (setting, expected) in [(None, true), (Some("false"), false), (Some("true"), true)] {
            let tmp = tempfile::tempdir().unwrap();
            let service = {
                let _guard = TieringEnvGuard::acquire();
                if let Some(setting) = setting {
                    std::env::set_var(MINIMAL_READ_ENABLED_ENV, setting);
                }
                EcOperationsService::open(
                    tmp.path(),
                    "ec",
                    active_set_registry(1),
                    test_set_builder(1),
                    NodeId(1),
                )
                .unwrap()
            };

            assert_eq!(
                service.config().minimal_read_enabled,
                expected,
                "{setting:?}"
            );
            let health = service.pool_health("ec").await.unwrap();
            assert_eq!(
                health["config"]["minimal_read_enabled"],
                json!(expected),
                "{setting:?}"
            );
        }
    }

    /// The pool registry holding just `active_set`.
    ///
    /// This is what an EC v1 configuration produces: one set and nothing else,
    /// so a test that opens a service with it exercises the single-set path the
    /// refactor must not change.
    fn active_set_registry(min_object_size: u64) -> ErasureSetRegistry {
        let set = active_set(min_object_size);
        ErasureSetRegistry::from_sets(set.pool_id, vec![set]).unwrap()
    }

    /// A builder for the fixtures' pool, covering the nodes both fixture sets
    /// sit on so either can be rebuilt from a roster.
    fn test_set_builder(min_object_size: u64) -> ErasureSetBuilder {
        let profile = test_profile(min_object_size);
        let labels = (1..=profile.total_shards() * 2)
            .map(|node| (NodeId(u128::from(node)), TopologyLabels::default()))
            .collect::<HashMap<_, _>>();
        let pool_nodes = labels.keys().copied().collect::<Vec<_>>();
        ErasureSetBuilder::new(
            PoolId::new(9),
            "ec",
            profile,
            pool_nodes,
            labels,
            EcTopologyPolicy::Strict,
            3,
        )
    }

    /// A pool with nowhere to put a new object must refuse to start.
    ///
    /// Failing here rather than on the first write is the difference between an
    /// operator reading one clear message at startup and clients receiving
    /// placement errors later. Covers the startup-validation requirement of
    /// `EC2-MSET-01-007`.
    #[tokio::test]
    async fn open_refuses_a_pool_with_no_writable_set() {
        let _guard = TieringEnvGuard::acquire();
        let tmp = tempfile::tempdir().unwrap();

        // An empty registry: the pool has no sets at all.
        let empty = ErasureSetRegistry::new(PoolId::new(9));
        let err =
            EcOperationsService::open(tmp.path(), "ec", empty, test_set_builder(1), NodeId(1))
                .err()
                .expect("a pool with no sets must be refused");
        assert!(
            err.to_string().contains("no erasure set that can take writes"),
            "unexpected error: {err}"
        );

        // A pool whose only set is read-only: it can still serve what it holds,
        // but it cannot accept anything new.
        let mut retired = active_set(1);
        retired.state = ErasureSetState::ReadOnly;
        let registry = ErasureSetRegistry::from_sets(retired.pool_id, vec![retired]).unwrap();
        let err =
            EcOperationsService::open(tmp.path(), "ec", registry, test_set_builder(1), NodeId(1))
                .err()
                .expect("a pool with no writable set must be refused");
        assert!(
            err.to_string().contains("no erasure set that can take writes"),
            "unexpected error: {err}"
        );
    }

    /// A pool whose sets disagree about identity is refused before the service is
    /// built, which is what keeps manifest resolution unambiguous at runtime.
    #[test]
    fn registry_rejects_ambiguous_pool_configuration() {
        let pool_id = PoolId::new(9);
        let width = EcProfile::rs_small().total_shards();
        let nodes = (1..=width).map(|node| NodeId(u128::from(node))).collect::<Vec<_>>();
        let labels = HashMap::<NodeId, TopologyLabels>::new();

        let build = |set_id: u64| {
            ErasureSet::active(
                pool_id,
                set_id,
                EcProfile::rs_small(),
                &nodes,
                &labels,
                EcTopologyPolicy::Strict,
                3,
                1,
            )
            .unwrap()
        };

        // Duplicate ids.
        let err = ErasureSetRegistry::from_sets(pool_id, vec![build(1), build(1)]).unwrap_err();
        assert!(err.to_string().contains("already taken"), "{err}");

        // A set belonging to another pool.
        let foreign = ErasureSet::active(
            PoolId::new(4),
            1,
            EcProfile::rs_small(),
            &nodes,
            &labels,
            EcTopologyPolicy::Strict,
            3,
            1,
        )
        .unwrap();
        let err = ErasureSetRegistry::from_sets(pool_id, vec![foreign]).unwrap_err();
        assert!(err.to_string().contains("belongs to pool"), "{err}");

        // Two sets claiming the pool's inherited identity.
        let first = ErasureSet::inherited(
            pool_id,
            EcProfile::rs_small(),
            &nodes,
            &labels,
            EcTopologyPolicy::Strict,
            3,
            1,
        )
        .unwrap();
        let mut second = build(2);
        second.inherited = true;
        let err = ErasureSetRegistry::from_sets(pool_id, vec![first, second]).unwrap_err();
        assert!(err.to_string().contains("inherited identity"), "{err}");
    }

    fn active_set(min_object_size: u64) -> ErasureSet {
        // Kept at the value EC v1 derived from the pool so the fixtures that
        // name `erasure_set_id: 9` keep meaning the same set.
        set_on_nodes(legacy_pool_set_id(PoolId::new(9)), 1, min_object_size)
    }

    /// A second set of the same pool, on nodes of its own.
    fn additional_set(min_object_size: u64) -> ErasureSet {
        set_on_nodes(
            FIRST_SET_ID,
            1 + test_profile(0).total_shards(),
            min_object_size,
        )
    }

    fn test_profile(min_object_size: u64) -> EcProfile {
        EcProfile {
            name: "test-rs".to_string(),
            codec: EcCodecKind::ReedSolomon,
            k: 4,
            parity: EcParity::ReedSolomon { m: 2 },
            min_object_size,
            chunk_size: 1024,
        }
    }

    fn set_on_nodes(set_id: u64, first_node: u8, min_object_size: u64) -> ErasureSet {
        let profile = test_profile(min_object_size);
        let nodes = (first_node..first_node + profile.total_shards())
            .map(|node| NodeId(u128::from(node)))
            .collect::<Vec<_>>();
        ErasureSet::active(
            PoolId::new(9),
            set_id,
            profile,
            &nodes,
            &HashMap::<NodeId, TopologyLabels>::new(),
            EcTopologyPolicy::Strict,
            3,
            1,
        )
        .unwrap()
    }

    fn put_commit(size: u64) -> EcObjectPutCommit {
        EcObjectPutCommit {
            bucket: "photos".to_string(),
            key: "raw.bin".to_string(),
            version_id: None,
            etag: "etag".to_string(),
            size,
            content_type: "application/octet-stream".to_string(),
            metadata: HashMap::new(),
            hot_replica_nodes: vec![NodeId(1), NodeId(2), NodeId(3)],
            content_hash_sha256: [7u8; 32],
            source_hlc: s4_cluster::HlcTimestamp::from_record(10, 0, NodeId(1).0),
        }
    }

    fn manifest_with_state(state: EcPlacementState) -> EcObjectManifest {
        let profile = EcProfileSnapshot::rs_small();
        let shards = (0..profile.total_shards())
            .map(|slot_index| {
                let bytes = vec![slot_index; 8];
                ShardManifest {
                    slot_index,
                    node_id: NodeId(u128::from(slot_index) + 1),
                    role: profile.role_for_slot(slot_index).unwrap(),
                    crc32: crc32fast::hash(&bytes),
                    sha256: sha256(&bytes),
                    size: bytes.len() as u64,
                }
            })
            .collect();

        EcObjectManifest {
            format_version: crate::ec_metadata::EC_MANIFEST_FORMAT_VERSION,
            bucket: "photos".to_string(),
            key: "raw.bin".to_string(),
            version_id: None,
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
            state,
            hlc: s4_cluster::HlcTimestamp::from_record(10, 0, NodeId(1).0),
            origin_node_id: NodeId(1),
            operation_id: Uuid::from_u128(123),
            created_at: 100,
            is_deleted: false,
            packed: None,
        }
    }

    /// The node that ran the transcode recorded its layout at `ec_committed`;
    /// the hot-copy purge then moved the quorum's manifest on. The record must
    /// resolve to the quorum's manifest instead of refusing the object, which
    /// is what failed `DELETE` on that node with a 500.
    #[test]
    fn a_layout_recorded_before_the_hot_purge_resolves_to_the_purged_manifest() {
        let committed = manifest_with_state(EcPlacementState::EcCommitted);
        let reference = EcObjectManifestRef::from_manifest(&committed).unwrap();
        let purged = manifest_with_state(EcPlacementState::HotReplicasPurged);

        let layout = erasure_coded_layout_for_quorum_manifest(&reference, &purged).unwrap();

        let ObjectDataLayout::ErasureCoded(current) = layout else {
            panic!("an erasure-coded record must stay erasure coded: {layout:?}");
        };
        assert!(current.matches_manifest(&purged).unwrap());
        assert_eq!(current.operation_id, reference.operation_id);
    }

    #[test]
    fn a_layout_that_matches_the_quorum_is_returned_as_it_is() {
        let purged = manifest_with_state(EcPlacementState::HotReplicasPurged);
        let reference = EcObjectManifestRef::from_manifest(&purged).unwrap();

        assert_eq!(
            erasure_coded_layout_for_quorum_manifest(&reference, &purged).unwrap(),
            ObjectDataLayout::ErasureCoded(reference)
        );
    }

    /// The node that transcoded the previous generation keeps its record when
    /// the object is overwritten through another node. Once the new generation
    /// is encoded, the record must resolve to it instead of refusing the object.
    #[test]
    fn a_layout_of_an_older_generation_resolves_to_the_newer_manifest() {
        let older = manifest_with_state(EcPlacementState::HotReplicasPurged);
        let reference = EcObjectManifestRef::from_manifest(&older).unwrap();
        let mut newer = manifest_with_state(EcPlacementState::EcCommitted);
        newer.operation_id = Uuid::from_u128(456);
        newer.content_hash_sha256 = [9; 32];
        newer.etag = "newer-etag".to_string();
        newer.hlc = s4_cluster::HlcTimestamp::from_record(20, 0, NodeId(2).0);

        let layout = erasure_coded_layout_for_quorum_manifest(&reference, &newer).unwrap();

        let ObjectDataLayout::ErasureCoded(current) = layout else {
            panic!("the newer generation is erasure coded: {layout:?}");
        };
        assert!(current.matches_manifest(&newer).unwrap());
        assert_eq!(current.operation_id, newer.operation_id);
    }

    /// Everything the quorum has not caught up with stays a mismatch: another
    /// manifest at the same or an earlier HLC, other bytes of the same
    /// operation, or a state of the same operation ahead of the quorum's.
    #[test]
    fn a_layout_of_other_work_is_still_a_mismatch() {
        let committed = manifest_with_state(EcPlacementState::EcCommitted);
        let reference = EcObjectManifestRef::from_manifest(&committed).unwrap();

        // Same HLC as the reference: not a newer generation.
        let mut other_operation = manifest_with_state(EcPlacementState::HotReplicasPurged);
        other_operation.operation_id = Uuid::from_u128(456);
        // An earlier HLC: the reference is the newer one.
        let mut older_generation = manifest_with_state(EcPlacementState::HotReplicasPurged);
        older_generation.operation_id = Uuid::from_u128(789);
        older_generation.hlc = s4_cluster::HlcTimestamp::from_record(5, 0, NodeId(2).0);
        let mut other_bytes = manifest_with_state(EcPlacementState::HotReplicasPurged);
        other_bytes.etag = "other-etag".to_string();
        let behind = manifest_with_state(EcPlacementState::EcReadVerified);

        for quorum in [other_operation, older_generation, other_bytes, behind] {
            let error = erasure_coded_layout_for_quorum_manifest(&reference, &quorum).unwrap_err();
            assert!(
                matches!(error, EcReadError::ManifestMismatch { .. }),
                "{:?} must not be taken for the recorded layout: {error}",
                quorum.state
            );
        }
    }

    #[test]
    fn test_tiering_cycle_stop_mixed_cluster() {
        assert_eq!(
            tiering_cycle_stop_name(Some(EcTieringCycleStop::MixedCluster)),
            "mixed_cluster"
        );
    }

    /// An unwired service must not claim the pool is uniform.
    ///
    /// This is the fail-closed half of the gate. The verdict is only ever "yes"
    /// on a positive answer from a membership source; nothing configured means
    /// nothing confirmed.
    #[test]
    fn an_unwired_service_reports_the_pool_as_unconfirmed() {
        let temp_dir = tempfile::tempdir().unwrap();
        let service = EcOperationsService::open(
            temp_dir.path(),
            "ec-pool",
            active_set_registry(1024),
            test_set_builder(1),
            NodeId(1),
        )
        .unwrap();

        assert!(!service.is_cluster_tiering_supported());
        assert_eq!(
            service.config_snapshot()["tiering_cluster_supported"],
            false
        );
    }

    /// The service must report whatever the membership source says, and say the
    /// same thing in its health snapshot as it does to the worker.
    ///
    /// The gate's own logic — which node counts as EC v2 capable — lives in
    /// `s4_cluster::GossipHandle` and is tested there against real member
    /// metadata. What is worth pinning here is that the service delegates
    /// rather than deciding, and that both of its readers get one answer.
    #[test]
    fn the_service_and_its_health_snapshot_report_one_verdict() {
        for supported in [true, false] {
            let temp_dir = tempfile::tempdir().unwrap();
            let service = EcOperationsService::open(
                temp_dir.path(),
                "ec-pool",
                active_set_registry(1024),
                test_set_builder(1),
                NodeId(1),
            )
            .unwrap();
            service.configure_cluster_membership(Arc::new(s4_cluster::MockClusterMembership::new(
                supported,
            )));

            assert_eq!(service.is_cluster_tiering_supported(), supported);
            assert_eq!(
                service.config_snapshot()["tiering_cluster_supported"],
                serde_json::Value::Bool(supported)
            );
        }
    }

    /// Wiring membership after the tiering environment was built must still
    /// take effect.
    ///
    /// The environment shares the service's cell instead of copying its
    /// contents, so a later `configure_cluster_membership` reaches the worker
    /// too. Copying would have made startup order load-bearing in a way nothing
    /// would have caught: the health endpoint would report a mixed pool while
    /// the worker happily migrated.
    #[tokio::test]
    async fn membership_wired_after_the_fact_reaches_the_worker() {
        let temp_dir = tempfile::tempdir().unwrap();
        let service = EcOperationsService::open(
            temp_dir.path(),
            "ec-pool",
            active_set_registry(1024),
            test_set_builder(1),
            NodeId(1),
        )
        .unwrap();

        let membership: Arc<OnceLock<Arc<dyn s4_cluster::ClusterMembershipQuery>>> =
            Arc::clone(&service.membership);
        assert!(!pool_supports_ec_v2(&membership, &service.placement.sets()));

        service
            .configure_cluster_membership(Arc::new(s4_cluster::MockClusterMembership::new(true)));

        assert!(pool_supports_ec_v2(&membership, &service.placement.sets()));
        assert!(service.is_cluster_tiering_supported());
    }

    /// A node that has not been given a membership source must not start the
    /// worker, and must not report it as wired.
    #[tokio::test]
    async fn the_tiering_worker_refuses_to_start_without_membership() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut service = EcOperationsService::open(
            temp_dir.path(),
            "ec-pool",
            active_set_registry(1024),
            test_set_builder(1),
            NodeId(1),
        )
        .unwrap();
        service.config.tiering.enabled = true;
        let service = Arc::new(service);

        assert!(service.start_tiering_worker().is_none());
        assert_eq!(service.config_snapshot()["tiering_worker_wired"], false);
    }

    /// Adding an erasure set while the cluster is mid-upgrade would place
    /// objects into sets older nodes cannot read.
    #[tokio::test]
    async fn adding_a_set_is_refused_while_the_pool_is_mid_upgrade() {
        let (_tmp, service) = multi_set_service(1);
        let node_ids: Vec<String> = additional_set(1)
            .slot_assignment
            .distinct_nodes()
            .iter()
            .map(ToString::to_string)
            .collect();

        let error = service
            .add_erasure_set(EcAdminAddSetRequest {
                pool: "ec".to_string(),
                node_ids,
            })
            .await
            .unwrap_err()
            .to_string();

        assert!(
            error.contains("mid-upgrade"),
            "the refusal must name the upgrade: {error}"
        );
        assert!(service.metadata.get_erasure_set_roster().await.unwrap().is_none());
    }

    /// A pool whose nodes all confirm EC v2 support passes the upgrade gate
    /// and allows multi-set operations to proceed.
    #[test]
    fn a_pool_whose_nodes_all_confirm_v2_passes_the_upgrade_gate() {
        let (_tmp, service) = multi_set_service(1);
        service
            .configure_cluster_membership(Arc::new(s4_cluster::MockClusterMembership::new(true)));
        assert!(service.ensure_pool_speaks_v2("adding an erasure set").is_ok());
    }

    /// Deactivating an erasure set only reduces exposure, so it must stay
    /// available mid-upgrade even when no membership view is wired.
    #[tokio::test]
    async fn deactivating_a_set_is_not_blocked_mid_upgrade() {
        let (_tmp, service) = multi_set_service(1);
        let error = service
            .deactivate_erasure_set(EcAdminDeactivateSetRequest {
                pool: "ec".to_string(),
                set_id: 9999,
            })
            .await
            .unwrap_err()
            .to_string();

        assert!(
            error.contains("has no erasure set 9999"),
            "deactivation must fail on validation of the set id, not the upgrade gate: {error}"
        );
    }

    /// The admin set listing must report whether every pool node confirms EC v2
    /// support so operators can track upgrade progress.
    #[tokio::test]
    async fn the_set_listing_says_whether_every_pool_node_speaks_v2() {
        let (_tmp, service) = multi_set_service(1);
        let listing = service.list_erasure_sets("ec").await.unwrap();
        assert_eq!(listing["all_nodes_speak_v2"], false);

        service
            .configure_cluster_membership(Arc::new(s4_cluster::MockClusterMembership::new(true)));
        let listing = service.list_erasure_sets("ec").await.unwrap();
        assert_eq!(listing["all_nodes_speak_v2"], true);
    }

    /// A node whose configuration does not contain a node the pool's roster
    /// requires cannot build the pool and would serve it incompletely.
    /// Refusing to start prevents it from answering InvalidManifest for objects
    /// belonging to sets it could not construct.
    #[tokio::test]
    async fn a_roster_this_node_cannot_build_stops_the_start() {
        let (_tmp, service) = test_service(1);
        let mut node_ids = additional_set(1).slot_assignment.distinct_nodes();
        node_ids[0] = NodeId(9_999);
        service
            .metadata
            .put_erasure_set_roster(&EcErasureSetRoster {
                pool_id: 9,
                sets: vec![EcErasureSetSpec {
                    set_id: FIRST_SET_ID,
                    node_ids,
                    state: ErasureSetState::Active,
                    created_at: 1,
                }],
                inherited_state: ErasureSetState::Active,
                inherited_node_ids: Vec::new(),
                hlc: HlcTimestamp::from_record(1, 1, 1),
                updated_at: 1,
            })
            .await
            .unwrap();

        let error = service.adopt_erasure_sets_at_startup().await.unwrap_err().to_string();

        // Naming the node is what makes the refusal actionable: the operator has
        // to know which entry of the pool's node list this node is missing.
        assert!(
            error.contains(&NodeId(9_999).to_string()),
            "the refusal must name the node this pool does not have: {error}"
        );
    }

    /// Reordering the pool's inherited node list in local configuration would
    /// silently change slot placement and make existing objects unreadable.
    /// The node refuses to start and names the mismatched node lists.
    #[tokio::test]
    async fn a_roster_naming_reordered_inherited_nodes_stops_the_start() {
        let (_tmp, service) = test_service(1);
        let mut reordered = service.default_set.slot_assignment.distinct_nodes();
        reordered.reverse();
        service
            .metadata
            .put_erasure_set_roster(&EcErasureSetRoster {
                pool_id: 9,
                sets: Vec::new(),
                inherited_state: ErasureSetState::Active,
                inherited_node_ids: reordered.clone(),
                hlc: HlcTimestamp::from_record(1, 1, 1),
                updated_at: 1,
            })
            .await
            .unwrap();

        let error = service.adopt_erasure_sets_at_startup().await.unwrap_err().to_string();

        assert!(
            error.contains(&format!("{reordered:?}")),
            "error must name the roster's recorded inherited node list: {error}"
        );
    }

    /// A roster defect in shared state must not take down every node at boot.
    /// The node logs an error, starts anyway, and retains the single set
    /// it constructed from its own valid configuration.
    #[tokio::test]
    async fn a_roster_the_pool_cannot_agree_on_lets_the_node_start() {
        let (_tmp, service) = test_service(1);
        let shared_nodes = service.default_set.slot_assignment.distinct_nodes();
        service
            .metadata
            .put_erasure_set_roster(&EcErasureSetRoster {
                pool_id: 9,
                sets: vec![EcErasureSetSpec {
                    set_id: FIRST_SET_ID,
                    node_ids: shared_nodes,
                    state: ErasureSetState::Active,
                    created_at: 1,
                }],
                inherited_state: ErasureSetState::Active,
                inherited_node_ids: Vec::new(),
                hlc: HlcTimestamp::from_record(1, 1, 1),
                updated_at: 1,
            })
            .await
            .unwrap();

        assert!(service.adopt_erasure_sets_at_startup().await.is_ok());
        assert_eq!(service.placement.sets().len(), 1);
    }

    /// When sets in a roster share a node, refreshing erasure sets returns an
    /// error naming the shared node so capacity double-counting is prevented.
    #[tokio::test]
    async fn sets_sharing_a_node_are_refused_when_the_roster_is_adopted() {
        let (_tmp, service) = test_service(1);
        let shared_nodes = service.default_set.slot_assignment.distinct_nodes();
        let shared_node = shared_nodes[0];
        service
            .metadata
            .put_erasure_set_roster(&EcErasureSetRoster {
                pool_id: 9,
                sets: vec![EcErasureSetSpec {
                    set_id: FIRST_SET_ID,
                    node_ids: shared_nodes,
                    state: ErasureSetState::Active,
                    created_at: 1,
                }],
                inherited_state: ErasureSetState::Active,
                inherited_node_ids: Vec::new(),
                hlc: HlcTimestamp::from_record(1, 1, 1),
                updated_at: 1,
            })
            .await
            .unwrap();

        let error = service.refresh_erasure_sets().await.unwrap_err().to_string();

        assert!(
            error.contains(&shared_node.to_string()),
            "the refusal must name the shared node {shared_node}: {error}"
        );
    }

    // -----------------------------------------------------------------------
    // Layout records of the pool's sets (plan 05, phase 1)
    // -----------------------------------------------------------------------

    /// Nodes 1-6 in three racks of two: the EC v1 spread then orders them
    /// 1, 3, 5, 2, 4, 6, where an unlabeled pool orders them 1-6.
    fn three_rack_labels() -> HashMap<NodeId, TopologyLabels> {
        (1..=6u8)
            .map(|node| {
                (
                    NodeId(u128::from(node)),
                    TopologyLabels {
                        zone: Some("z1".to_string()),
                        rack: Some(format!("r{}", (node - 1) / 2)),
                        host: Some(format!("h{node}")),
                        disk_group: Some("d0".to_string()),
                    },
                )
            })
            .collect()
    }

    /// A single-set service for pool 9 on `dir`, as a node started with
    /// `labels` builds it. Opening two of them on one directory in turn is a
    /// node restarting with other labels.
    fn service_with_labels(
        dir: &Path,
        labels: HashMap<NodeId, TopologyLabels>,
    ) -> EcOperationsService {
        let _guard = TieringEnvGuard::acquire();
        let profile = test_profile(1);
        let set_nodes = (1..=profile.total_shards())
            .map(|node| NodeId(u128::from(node)))
            .collect::<Vec<_>>();
        let inherited = ErasureSet::inherited(
            PoolId::new(9),
            profile.clone(),
            &set_nodes,
            &labels,
            EcTopologyPolicy::Warn,
            3,
            1,
        )
        .unwrap();
        let registry = ErasureSetRegistry::from_sets(PoolId::new(9), vec![inherited]).unwrap();
        let builder = ErasureSetBuilder::new(
            PoolId::new(9),
            "ec",
            profile,
            set_nodes,
            labels,
            EcTopologyPolicy::Warn,
            3,
        );
        EcOperationsService::open(dir, "ec", registry, builder, NodeId(1)).unwrap()
    }

    /// EC2-ZONE-03-001, the operator's path: a repair queued through the admin
    /// API carries the zone of the shard's owner, as the read and
    /// anti-entropy paths do, so the zone repair limits apply to it. Before
    /// plan 05, phase 3 every path queued `failure_domain: None` and those
    /// limits never applied (EC2-085, point 1).
    #[tokio::test]
    async fn an_admin_repair_carries_the_zone_of_the_shards_owner() {
        let dir = tempfile::tempdir().unwrap();
        // Nodes 1-3 in zone "a", 4-6 in zone "b"; the service runs on node 1.
        let labels = (1..=6u8)
            .map(|node| {
                (
                    NodeId(u128::from(node)),
                    TopologyLabels {
                        zone: Some(if node <= 3 { "a" } else { "b" }.to_string()),
                        rack: Some(format!("r{node}")),
                        host: Some(format!("h{node}")),
                        disk_group: Some("d0".to_string()),
                    },
                )
            })
            .collect();
        let service = service_with_labels(dir.path(), labels);
        // Slot `i` of the manifest is on node `i + 1`.
        let manifest = manifest_with_state(EcPlacementState::HotReplicasPurged);
        service.metadata.put_manifest(&manifest).await.unwrap();

        let response = EcAdminService::repair_pool(
            &service,
            EcAdminRepairRequest {
                pool: "ec".to_string(),
                object: Some(EcAdminObjectRef {
                    bucket: manifest.bucket.clone(),
                    key: manifest.key.clone(),
                    version_id: None,
                }),
                chunk_index: Some(0),
                slot_index: Some(4),
                node_id: Some(NodeId(5).to_string()),
                max_tasks: 1,
            },
        )
        .await
        .unwrap();

        assert_eq!(response["queued"], 1, "{response}");
        assert_eq!(response["tasks"][0]["failure_domain"], "b", "{response}");
        let stored = service.repair.list_repair_tasks(10).await.unwrap();
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].kind, EcRepairKind::ManualAdmin);
        assert_eq!(stored[0].failure_domain.as_deref(), Some("b"));
    }

    // -----------------------------------------------------------------------
    // Rebuilding the shards of a replaced node (plan 08, phase 3)
    // -----------------------------------------------------------------------

    /// `count` manifests of the same shape under distinct keys.
    ///
    /// Slot `i` sits on node `i + 1`, as `manifest_with_state` lays them out, so
    /// asking about node 5 asks about slot 4 of every one of them.
    async fn store_manifests(service: &EcOperationsService, count: usize) -> Vec<EcObjectManifest> {
        let mut stored = Vec::with_capacity(count);
        for index in 0..count {
            let mut manifest = manifest_with_state(EcPlacementState::HotReplicasPurged);
            manifest.key = format!("raw-{index}.bin");
            manifest.operation_id = Uuid::from_u128(1_000 + index as u128);
            service.metadata.put_manifest(&manifest).await.unwrap();
            stored.push(manifest);
        }
        stored
    }

    /// Write the local shard index entries a rebuild of this node would produce.
    async fn store_local_shards(service: &EcOperationsService, manifests: &[EcObjectManifest]) {
        for manifest in manifests {
            for (chunk_index, slot_index) in shards_owned_by(manifest, service.local_node_id) {
                let shard = manifest_shard(manifest, chunk_index, slot_index).unwrap();
                let key = LocalShardKey {
                    object: manifest.object_key(),
                    chunk_index,
                    slot_index,
                };
                let record = LocalShardRecord {
                    volume_id: 1,
                    offset: 0,
                    size: shard.size,
                    crc32: shard.crc32,
                    sha256: shard.sha256,
                    state: LocalShardState::Durable,
                    operation_id: manifest.operation_id,
                };
                service.metadata.put_local_shard(&key, &record).await.unwrap();
            }
        }
    }

    fn repair_by_node(node_id: NodeId, max_tasks: usize) -> EcAdminRepairRequest {
        EcAdminRepairRequest {
            pool: "ec".to_string(),
            object: None,
            chunk_index: None,
            slot_index: None,
            node_id: Some(node_id.to_string()),
            max_tasks,
        }
    }

    /// One command, every shard of one owner: the whole point of
    /// `EC2-NREP-03-001`. Before this the operator queued them one slot at a
    /// time, which is a different procedure for every object in the pool.
    #[tokio::test]
    async fn one_request_queues_every_shard_of_a_replaced_node() {
        let dir = tempfile::tempdir().unwrap();
        let service = service_with_labels(dir.path(), three_rack_labels());
        let manifests = store_manifests(&service, 4).await;
        let replaced = NodeId(5);

        let response = EcAdminService::repair_pool(&service, repair_by_node(replaced, 1_024))
            .await
            .unwrap();

        assert_eq!(response["target_node_id"], json!(replaced.to_string()));
        assert_eq!(response["queued"], json!(manifests.len()), "{response}");
        assert_eq!(
            response["newly_queued"],
            json!(manifests.len()),
            "{response}"
        );
        assert_eq!(response["already_queued"], json!(0), "{response}");
        // The walk read the pool to the end, so nothing is left and the
        // remainder is a total rather than a floor.
        assert_eq!(response["remaining"], json!(0), "{response}");
        assert_eq!(response["stopped_at"], json!("end"), "{response}");
        assert_eq!(response["scan_truncated"], json!(false), "{response}");
        assert_eq!(
            response["manifests_scanned"],
            json!(manifests.len()),
            "{response}"
        );

        let stored = service.repair.list_repair_tasks(64).await.unwrap();
        assert_eq!(stored.len(), manifests.len());
        assert!(stored.iter().all(|task| task.node_id == replaced));
    }

    /// Driving a rebuild to the end means repeating the request, so the second
    /// call must report what it found rather than what it made. A queue that
    /// grew by four here would grow without limit over a real rebuild.
    #[tokio::test]
    async fn repeating_a_mass_repair_does_not_double_the_queue() {
        let dir = tempfile::tempdir().unwrap();
        let service = service_with_labels(dir.path(), three_rack_labels());
        let manifests = store_manifests(&service, 4).await;
        let replaced = NodeId(5);

        EcAdminService::repair_pool(&service, repair_by_node(replaced, 1_024))
            .await
            .unwrap();
        let again = EcAdminService::repair_pool(&service, repair_by_node(replaced, 1_024))
            .await
            .unwrap();

        assert_eq!(again["queued"], json!(manifests.len()), "{again}");
        assert_eq!(again["newly_queued"], json!(0), "{again}");
        assert_eq!(again["already_queued"], json!(manifests.len()), "{again}");

        let stored = service.repair.list_repair_tasks(64).await.unwrap();
        assert_eq!(stored.len(), manifests.len(), "the queue must not grow");
    }

    /// A request that filled its own quota has to look different from one that
    /// finished the pool — otherwise a whole-node rebuild reads as done after
    /// its first page.
    #[tokio::test]
    async fn a_request_that_fills_its_task_limit_says_how_much_is_left() {
        let dir = tempfile::tempdir().unwrap();
        let service = service_with_labels(dir.path(), three_rack_labels());
        let manifests = store_manifests(&service, 4).await;
        let replaced = NodeId(5);

        let response = EcAdminService::repair_pool(&service, repair_by_node(replaced, 1))
            .await
            .unwrap();

        assert_eq!(response["queued"], json!(1), "{response}");
        assert_eq!(
            response["remaining"],
            json!(manifests.len() - 1),
            "{response}"
        );
        assert_eq!(response["stopped_at"], json!("max_tasks"), "{response}");
        // The walk still read every manifest, so the remainder above is exact.
        assert_eq!(response["scan_truncated"], json!(false), "{response}");
        assert_eq!(
            response["manifests_scanned"],
            json!(manifests.len()),
            "{response}"
        );
    }

    /// The completion signal, counted from shards and not from the queue
    /// (`EC2-NREP-03-002`). A node that holds none of its shards is not
    /// finished however empty its repair queue happens to be.
    #[tokio::test]
    async fn repair_status_answers_whether_a_nodes_rebuild_has_finished() {
        let dir = tempfile::tempdir().unwrap();
        let service = service_with_labels(dir.path(), three_rack_labels());
        let manifests = store_manifests(&service, 3).await;
        let owner = service.local_node_id;
        let request = EcAdminRepairStatusRequest {
            pool: "ec".to_string(),
            node_id: Some(owner.to_string()),
        };

        // Nothing queued anywhere, and the rebuild is plainly unfinished.
        let before = EcAdminService::repair_status(&service, request.clone()).await.unwrap();
        let rebuild = &before["node_rebuild"];
        assert_eq!(before["counts"]["queued"], json!(0), "{before}");
        assert_eq!(rebuild["node_id"], json!(owner.to_string()), "{rebuild}");
        assert_eq!(
            rebuild["shards_expected"],
            json!(manifests.len()),
            "{rebuild}"
        );
        assert_eq!(
            rebuild["shards_missing"],
            json!(manifests.len()),
            "{rebuild}"
        );
        assert_eq!(rebuild["shards_unknown"], json!(0), "{rebuild}");
        assert_eq!(rebuild["complete"], json!(false), "{rebuild}");
        assert_eq!(
            rebuild["verdict_reason"],
            json!("shards_missing"),
            "{rebuild}"
        );
        assert_eq!(rebuild["scan_truncated"], json!(false), "{rebuild}");

        store_local_shards(&service, &manifests).await;

        let after = EcAdminService::repair_status(&service, request).await.unwrap();
        let rebuild = &after["node_rebuild"];
        assert_eq!(rebuild["shards_missing"], json!(0), "{rebuild}");
        assert_eq!(
            rebuild["shards_present"],
            json!(manifests.len()),
            "{rebuild}"
        );
        assert_eq!(rebuild["complete"], json!(true), "{rebuild}");
        assert_eq!(
            rebuild["verdict_reason"],
            json!("all_shards_present"),
            "{rebuild}"
        );
    }

    /// A shard whose owner cannot be asked was not found present. Answering
    /// "nothing missing" there would hand an operator the one word they act on,
    /// produced by the one case in which it is not known.
    #[tokio::test]
    async fn a_rebuild_verdict_is_withheld_when_the_owner_cannot_be_asked() {
        let dir = tempfile::tempdir().unwrap();
        let service = service_with_labels(dir.path(), three_rack_labels());
        let manifests = store_manifests(&service, 2).await;

        let status = EcAdminService::repair_status(
            &service,
            EcAdminRepairStatusRequest {
                pool: "ec".to_string(),
                node_id: Some(NodeId(5).to_string()),
            },
        )
        .await
        .unwrap();

        let rebuild = &status["node_rebuild"];
        assert_eq!(
            rebuild["shards_unknown"],
            json!(manifests.len()),
            "{rebuild}"
        );
        assert_eq!(rebuild["shards_missing"], json!(0), "{rebuild}");
        assert_eq!(rebuild["complete"], Value::Null, "{rebuild}");
        assert_eq!(
            rebuild["verdict_reason"],
            json!("shards_unanswered"),
            "{rebuild}"
        );
        assert!(
            rebuild["unknown_reason"]
                .as_str()
                .is_some_and(|reason| reason.contains("cluster read path")),
            "{rebuild}"
        );
    }

    /// The route without an owner is the queue report it has always been, and
    /// a caller written before this plan sees exactly what it saw.
    #[tokio::test]
    async fn repair_status_without_an_owner_is_the_report_it_always_was() {
        let dir = tempfile::tempdir().unwrap();
        let service = service_with_labels(dir.path(), three_rack_labels());
        store_manifests(&service, 2).await;

        let status =
            EcAdminService::repair_status(&service, EcAdminRepairStatusRequest::for_pool("ec"))
                .await
                .unwrap();

        assert_eq!(status["pool"], json!("ec"));
        assert!(status.get("counts").is_some(), "{status}");
        assert!(status.get("tasks").is_some(), "{status}");
        assert!(
            status.get("node_rebuild").is_none(),
            "an unasked question must not be answered: {status}"
        );
    }

    /// The queue is reported beside the inventory and scoped to the owner
    /// asked about, so a pool busy repairing someone else does not read as
    /// progress on this node.
    #[tokio::test]
    async fn a_rebuild_report_counts_only_the_queue_of_the_owner_it_is_about() {
        let dir = tempfile::tempdir().unwrap();
        let service = service_with_labels(dir.path(), three_rack_labels());
        store_manifests(&service, 2).await;
        EcAdminService::repair_pool(&service, repair_by_node(NodeId(5), 1_024))
            .await
            .unwrap();

        let status = EcAdminService::repair_status(
            &service,
            EcAdminRepairStatusRequest {
                pool: "ec".to_string(),
                node_id: Some(NodeId(6).to_string()),
            },
        )
        .await
        .unwrap();

        assert_eq!(status["counts"]["queued"], json!(2), "{status}");
        assert_eq!(
            status["node_rebuild"]["tasks"]["queued"],
            json!(0),
            "{status}"
        );
    }

    fn inherited_slot_order(service: &EcOperationsService) -> Vec<NodeId> {
        service
            .placement
            .sets()
            .get(service.default_set.set_id)
            .unwrap()
            .slot_assignment
            .slot_order()
    }

    /// With no record anywhere, the registry is exactly the one EC v1 builds —
    /// a node that has not recorded anything behaves as before plan 05.
    #[tokio::test]
    async fn without_a_layout_record_the_registry_is_the_ec_v1_one() {
        let dir = tempfile::tempdir().unwrap();
        let service = service_with_labels(dir.path(), three_rack_labels());
        let before = service.placement.sets();

        service.refresh_erasure_sets().await.unwrap();

        assert_eq!(*service.placement.sets(), *before);
        assert_eq!(
            service.placement.sets().get(service.default_set.set_id),
            Some(&service.default_set)
        );
    }

    /// A set with a record takes the recorded order verbatim, and the admin
    /// surface reports that order rather than the one configuration built.
    #[tokio::test]
    async fn a_set_is_rebuilt_from_its_layout_record_verbatim() {
        let dir = tempfile::tempdir().unwrap();
        let service = service_with_labels(dir.path(), three_rack_labels());
        let mut recorded = service.default_set.clone();
        let mut order = recorded.slot_assignment.slot_order();
        order.reverse();
        recorded.slot_assignment =
            ShardSlotAssignment::from_slot_order(&recorded.profile, &order).unwrap();
        service
            .metadata
            .put_set_layout_record(&EcSetLayoutRecord::v1(
                &recorded,
                EcTopologyPolicy::Warn,
                1,
                HlcTimestamp::from_record(1, 1, 1),
            ))
            .await
            .unwrap();

        service.refresh_erasure_sets().await.unwrap();

        assert_eq!(inherited_slot_order(&service), order);
        assert_ne!(order, service.default_set.slot_assignment.slot_order());
        let health = service.pool_health("ec").await.unwrap();
        let reported = health["erasure_set"]["slot_assignment"]["slots"]
            .as_array()
            .unwrap()
            .iter()
            .map(|slot| slot["node_id"].as_str().unwrap().to_string())
            .collect::<Vec<_>>();
        assert_eq!(
            reported,
            order.iter().map(ToString::to_string).collect::<Vec<_>>()
        );
    }

    /// The scenario of EC2-086 across a restart: a node records its set's
    /// layout, restarts with other labels, and keeps the recorded layout
    /// instead of the one its new labels give. Not a single slot moves.
    #[tokio::test]
    async fn a_label_change_across_a_restart_moves_no_slot_of_a_recorded_set() {
        let dir = tempfile::tempdir().unwrap();
        let recorded = {
            let before = service_with_labels(dir.path(), HashMap::new());
            let record = EcSetLayoutRecord::v1(
                &before.default_set,
                EcTopologyPolicy::Warn,
                1,
                HlcTimestamp::from_record(1, 1, 1),
            );
            before.metadata.put_set_layout_record(&record).await.unwrap();
            before.refresh_erasure_sets().await.unwrap();
            inherited_slot_order(&before)
        };

        let after = service_with_labels(dir.path(), three_rack_labels());
        // The new labels alone would lay the set out differently...
        assert_ne!(after.default_set.slot_assignment.slot_order(), recorded);
        after.adopt_erasure_sets_at_startup().await.unwrap();

        // ...and the set keeps the recorded layout anyway.
        assert_eq!(inherited_slot_order(&after), recorded);
    }

    /// A record naming a node the set does not have is not applied, and does
    /// not stop the node: the set keeps the layout its configuration gives.
    #[tokio::test]
    async fn a_layout_record_naming_other_nodes_is_not_applied() {
        let dir = tempfile::tempdir().unwrap();
        let service = service_with_labels(dir.path(), three_rack_labels());
        let mut record = EcSetLayoutRecord::v1(
            &service.default_set,
            EcTopologyPolicy::Warn,
            1,
            HlcTimestamp::from_record(1, 1, 1),
        );
        record.node_ids[0] = NodeId(9_999);
        service.metadata.put_set_layout_record(&record).await.unwrap();

        service.adopt_erasure_sets_at_startup().await.unwrap();

        assert_eq!(
            inherited_slot_order(&service),
            service.default_set.slot_assignment.slot_order()
        );
    }

    /// A record that can no longer be read leaves the node on the last layout
    /// it knew — never on a layout rebuilt from labels (EC2-084, point 3).
    #[tokio::test]
    async fn an_unreadable_layout_record_keeps_the_last_known_layout() {
        let dir = tempfile::tempdir().unwrap();
        let service = service_with_labels(dir.path(), three_rack_labels());
        let mut order = service.default_set.slot_assignment.slot_order();
        order.rotate_left(2);
        let mut known = EcSetLayoutRecord::v1(
            &service.default_set,
            EcTopologyPolicy::Warn,
            1,
            HlcTimestamp::from_record(1, 1, 1),
        );
        known.node_ids = order.clone();
        // Known in memory from an earlier refresh; the local copy is gone and
        // this node has no cluster read path, so the read answers "unknown".
        service.set_layouts.lock().unwrap().insert(service.default_set.set_id, known);
        assert!(matches!(
            service.read_set_layout_record(service.default_set.set_id).await,
            SetLayoutRead::Unknown(_)
        ));

        service.refresh_erasure_sets().await.unwrap();

        assert_eq!(inherited_slot_order(&service), order);
    }

    /// Only an answer that the pool has no record counts as "absent". Without
    /// a cluster read path this node cannot know, and says so instead of
    /// claiming the record does not exist.
    #[tokio::test]
    async fn a_node_without_a_cluster_read_path_never_reports_a_record_absent() {
        let dir = tempfile::tempdir().unwrap();
        let service = service_with_labels(dir.path(), HashMap::new());

        assert!(matches!(
            service.read_set_layout_record(service.default_set.set_id).await,
            SetLayoutRead::Unknown(_)
        ));
        // And it cannot write one either, so a refresh records nothing.
        service.refresh_erasure_sets().await.unwrap();
        assert!(service
            .metadata
            .get_set_layout_record(service.default_set.set_id)
            .await
            .unwrap()
            .is_none());
    }

    /// An LRC pool, two sets wide, with the LRC group mode switched on and
    /// multi-set pools enabled; no topology labels and `warn`.
    fn lrc_group_rule_service() -> (tempfile::TempDir, EcOperationsService, Vec<NodeId>) {
        lrc_group_rule_service_with(EcTopologyPolicy::Warn, HashMap::new())
    }

    /// The same pool under `policy`, with `labels` on its nodes. The
    /// inherited set takes nodes 1-7, the spare nodes are 8-14.
    fn lrc_group_rule_service_with(
        policy: EcTopologyPolicy,
        labels: HashMap<NodeId, TopologyLabels>,
    ) -> (tempfile::TempDir, EcOperationsService, Vec<NodeId>) {
        let tmp = tempfile::tempdir().unwrap();
        let (service, spare) = lrc_group_rule_service_on(tmp.path(), policy, labels);
        (tmp, service, spare)
    }

    /// The pool of [`lrc_group_rule_service_with`] opened on `dir`, so two
    /// services on two directories are two nodes of one pool.
    fn lrc_group_rule_service_on(
        dir: &Path,
        policy: EcTopologyPolicy,
        labels: HashMap<NodeId, TopologyLabels>,
    ) -> (EcOperationsService, Vec<NodeId>) {
        let _guard = TieringEnvGuard::acquire();
        std::env::set_var(MULTI_SET_ENABLED_ENV, "true");
        let profile = EcProfile::lrc_small();
        let width = profile.total_shards();
        let pool_nodes = (1..=width * 2).map(|node| NodeId(u128::from(node))).collect::<Vec<_>>();
        let inherited = ErasureSet::inherited(
            PoolId::new(9),
            profile.clone(),
            &pool_nodes[..usize::from(width)],
            &labels,
            policy,
            3,
            1,
        )
        .unwrap();
        let registry = ErasureSetRegistry::from_sets(PoolId::new(9), vec![inherited]).unwrap();
        let builder = ErasureSetBuilder::new(
            PoolId::new(9),
            "ec",
            profile,
            pool_nodes.clone(),
            labels,
            policy,
            3,
        )
        .with_lrc_group_domain(Some(EcFailureDomainLevel::Rack));
        let service = EcOperationsService::open(dir, "ec", registry, builder, NodeId(1)).unwrap();
        std::env::remove_var(MULTI_SET_ENABLED_ENV);
        let spare = pool_nodes[usize::from(width)..].to_vec();
        (service, spare)
    }

    /// Complete labels for the fixture pool: the inherited set's nodes 1-7 in
    /// racks of their own, and the spare nodes 8-14 in `spare_racks`, one
    /// entry per node. Every node is its own host unless `host` says
    /// otherwise.
    fn rule_pool_labels(
        spare_racks: [&str; 7],
        host: impl Fn(u8) -> String,
    ) -> HashMap<NodeId, TopologyLabels> {
        (1..=14u8)
            .map(|node| {
                let rack = if node <= 7 {
                    format!("inherited-{node}")
                } else {
                    spare_racks[usize::from(node - 8)].to_string()
                };
                (
                    NodeId(u128::from(node)),
                    TopologyLabels {
                        zone: Some("z1".to_string()),
                        rack: Some(rack),
                        host: Some(host(node)),
                        disk_group: Some("d0".to_string()),
                    },
                )
            })
            .collect()
    }

    fn add_request(nodes: &[NodeId]) -> EcAdminAddSetRequest {
        EcAdminAddSetRequest {
            pool: "ec".to_string(),
            node_ids: nodes.iter().map(ToString::to_string).collect(),
        }
    }

    /// The gate names the nodes holding it shut, and opens only when every
    /// node of the pool confirms the version (EC2-084, point 4).
    #[tokio::test]
    async fn the_layout_record_gate_waits_for_every_node_of_the_pool() {
        let (_tmp, service, _) = lrc_group_rule_service();

        let unwired = service.ensure_pool_reads_set_layouts("testing").unwrap_err().to_string();
        assert!(
            unwired.contains("no view of cluster membership"),
            "{unwired}"
        );

        service.configure_cluster_membership(Arc::new(
            s4_cluster::MockClusterMembership::with_protocol_version(
                s4_cluster::EC_V2_MIN_PROTOCOL_VERSION,
            ),
        ));
        let mid_upgrade = service.ensure_pool_reads_set_layouts("testing").unwrap_err().to_string();
        assert!(
            mid_upgrade.contains(&s4_cluster::EC_SET_LAYOUT_MIN_PROTOCOL_VERSION.to_string()),
            "{mid_upgrade}"
        );
        for node_id in service.set_builder.pool_nodes() {
            assert!(
                mid_upgrade.contains(&node_id.to_string()),
                "the refusal must name node {node_id}: {mid_upgrade}"
            );
        }
    }

    #[tokio::test]
    async fn the_layout_record_gate_opens_when_every_node_confirms_the_version() {
        let (_tmp, service, _) = lrc_group_rule_service();
        service
            .configure_cluster_membership(Arc::new(s4_cluster::MockClusterMembership::new(true)));

        service.ensure_pool_reads_set_layouts("testing").unwrap();
    }

    /// With the mode on for an LRC profile, a set is not created while a node
    /// of the pool could lay it out by labels, and the operator is told which.
    /// Nothing is written.
    #[tokio::test]
    async fn a_set_by_the_group_rule_is_refused_while_a_pool_node_is_behind() {
        let (_tmp, service, spare) = lrc_group_rule_service();
        service.configure_cluster_membership(Arc::new(
            s4_cluster::MockClusterMembership::with_protocol_version(
                s4_cluster::EC_V2_MIN_PROTOCOL_VERSION,
            ),
        ));

        let error = service.add_erasure_set(add_request(&spare)).await.unwrap_err().to_string();

        assert!(error.contains(LRC_GROUP_DOMAIN_ENV), "{error}");
        assert!(error.contains("layout records"), "{error}");
        assert!(service.metadata.get_erasure_set_roster().await.unwrap().is_none());
        assert!(service.metadata.get_set_layout_record(FIRST_SET_ID).await.unwrap().is_none());
    }

    /// Who records a layout when the pool answers that a set has none: a node
    /// of the set, never a node outside it. A node added to a grown pool gets
    /// that answer for every record written before it joined, and recording
    /// would overwrite the set's real layout (EC2-088). Publishing a copy the
    /// pool did not produce is the set's own nodes' job as well.
    #[test]
    fn only_a_node_of_the_set_records_a_layout_the_pool_lacks() {
        let record = || {
            EcSetLayoutRecord::v1(
                &additional_set(1),
                EcTopologyPolicy::Strict,
                1,
                HlcTimestamp::from_record(1, 1, 1),
            )
        };
        for member in [true, false] {
            assert_eq!(
                LayoutStep::after(&SetLayoutRead::Found(record()), member),
                LayoutStep::Take
            );
            assert_eq!(
                LayoutStep::after(&SetLayoutRead::Unknown("down".to_string()), member),
                LayoutStep::Wait
            );
        }
        assert_eq!(
            LayoutStep::after(&SetLayoutRead::Absent, true),
            LayoutStep::RecordV1
        );
        assert_eq!(
            LayoutStep::after(&SetLayoutRead::Absent, false),
            LayoutStep::Wait
        );
        // A copy the pool did not produce is published again by the set's own
        // nodes, and only taken by the others.
        assert_eq!(
            LayoutStep::after(&SetLayoutRead::LocalOnly(record()), true),
            LayoutStep::Republish
        );
        assert_eq!(
            LayoutStep::after(&SetLayoutRead::LocalOnly(record()), false),
            LayoutStep::Take
        );
    }

    /// A set whose record a node has never known is held from new writes; the
    /// inherited set only on a node with a cluster read path, because without
    /// one there is no record to miss.
    #[test]
    fn a_set_is_held_only_while_its_record_was_never_known() {
        for inherited in [true, false] {
            for read_path in [true, false] {
                assert!(!held_until_known(inherited, read_path, true));
            }
        }
        assert!(held_until_known(false, false, false));
        assert!(held_until_known(false, true, false));
        assert!(held_until_known(true, true, false));
        assert!(!held_until_known(true, false, false));
    }

    /// Under `strict`, spare nodes in two racks cannot hold a local group of
    /// three: the set is refused with the numbers and the reason, before
    /// anything is written (`EC2-ZONE-02-002`).
    #[tokio::test]
    async fn a_set_the_group_rule_cannot_place_is_refused_under_strict() {
        let labels = rule_pool_labels(["a", "a", "a", "a", "b", "b", "b"], |node| {
            format!("h{node}")
        });
        let (_tmp, service, spare) = lrc_group_rule_service_with(EcTopologyPolicy::Strict, labels);
        service
            .configure_cluster_membership(Arc::new(s4_cluster::MockClusterMembership::new(true)));

        let error = service.add_erasure_set(add_request(&spare)).await.unwrap_err();

        assert!(matches!(error, ClusterError::Config(_)), "{error:?}");
        let error = error.to_string();
        assert!(error.contains("over 3 distinct racks"), "{error}");
        assert!(error.contains("in 2 rack(s) (z1/a=4, z1/b=3)"), "{error}");
        assert!(error.contains("S4_EC_TOPOLOGY_POLICY=warn"), "{error}");
        assert!(service.metadata.get_erasure_set_roster().await.unwrap().is_none());
        assert!(service.metadata.get_set_layout_record(FIRST_SET_ID).await.unwrap().is_none());
        assert_eq!(service.placement.sets().len(), 1);
    }

    /// A set the rule can place is laid out by it, recorded with its rule and
    /// algorithm, and every node that reads the record rebuilds exactly that
    /// set — including one whose EC v1 checks would refuse the same nodes
    /// under `strict`: two spare nodes share a host name in different racks,
    /// which the EC v1 checks count as one host.
    #[tokio::test]
    async fn a_set_laid_out_by_the_rule_is_rebuilt_from_its_record_on_every_node() {
        let labels = rule_pool_labels(["a", "a", "b", "b", "c", "c", "d"], |node| {
            if node == 10 {
                "h8".to_string()
            } else {
                format!("h{node}")
            }
        });
        let (_tmp, creator, spare) =
            lrc_group_rule_service_with(EcTopologyPolicy::Strict, labels.clone());
        // The EC v1 path refuses these nodes: by name, h8 is two nodes.
        let v1 = creator.set_builder.build(FIRST_SET_ID, &spare, 5).unwrap_err().to_string();
        assert!(v1.contains("one host per slot"), "{v1}");

        let created = creator.build_new_erasure_set(FIRST_SET_ID, &spare, 5, true).await.unwrap();
        let rule = EcSetLayoutRule::LrcGroups {
            level: EcFailureDomainLevel::Rack,
        };
        assert_eq!(created.set.layout_rule, rule);
        assert_eq!(created.violation, None);
        assert_eq!(
            created.algorithm_version,
            crate::ec_metadata::EC_LRC_GROUP_LAYOUT_ALGORITHM_VERSION
        );
        let record = EcSetLayoutRecord::new_set(
            &created,
            EcTopologyPolicy::Strict,
            5,
            HlcTimestamp::from_record(1, 1, 1),
        );
        record.validate().unwrap();
        assert_eq!(record.rule, rule);
        assert_eq!(record.node_ids, created.set.slot_assignment.slot_order());

        // Another node of the pool, with the same configuration, reads the
        // roster and the record.
        let other_dir = tempfile::tempdir().unwrap();
        let (other, _) =
            lrc_group_rule_service_on(other_dir.path(), EcTopologyPolicy::Strict, labels);
        other.metadata.put_set_layout_record(&record).await.unwrap();
        other
            .metadata
            .put_erasure_set_roster(&EcErasureSetRoster {
                pool_id: 9,
                sets: vec![EcErasureSetSpec {
                    set_id: FIRST_SET_ID,
                    node_ids: spare.clone(),
                    state: ErasureSetState::Active,
                    created_at: 5,
                }],
                inherited_state: ErasureSetState::Active,
                inherited_node_ids: Vec::new(),
                hlc: HlcTimestamp::from_record(1, 1, 1),
                updated_at: 5,
            })
            .await
            .unwrap();
        other.adopt_erasure_sets_at_startup().await.unwrap();

        let rebuilt = other.placement.sets().get(FIRST_SET_ID).cloned().unwrap();
        assert_eq!(rebuilt, created.set);
        assert!(rebuilt.accepts_writes());
        assert_eq!(rebuilt.topology_risks, created.set.topology_risks);
    }

    /// Under `warn` a set the rule cannot fully place is created as close to
    /// the rule as its nodes allow, and says how it falls short: the record
    /// carries the reason, the set is degraded, and its risks name the
    /// numbers.
    #[tokio::test]
    async fn under_warn_a_set_short_of_the_rule_is_created_degraded_and_says_why() {
        let labels = rule_pool_labels(["a", "a", "a", "a", "b", "b", "b"], |node| {
            format!("h{node}")
        });
        let (_tmp, service, spare) = lrc_group_rule_service_with(EcTopologyPolicy::Warn, labels);

        let created = service.build_new_erasure_set(FIRST_SET_ID, &spare, 5, true).await.unwrap();

        let violation = created.violation.clone().unwrap();
        assert_eq!(
            violation.reason,
            crate::erasure_coding::EcLayoutViolationReason::TooFewDomains
        );
        assert!(
            !violation.detail.contains("S4_EC_TOPOLOGY_POLICY"),
            "{}",
            violation.detail
        );
        assert_eq!(
            created.set.topology_health,
            crate::erasure_coding::EcTopologyHealth::TopologyDegraded
        );
        assert!(
            created.set.topology_risks.iter().any(|risk| risk == &violation.detail),
            "{:?}",
            created.set.topology_risks
        );
        let record = EcSetLayoutRecord::new_set(
            &created,
            EcTopologyPolicy::Warn,
            5,
            HlcTimestamp::from_record(1, 1, 1),
        );
        record.validate().unwrap();
        assert_eq!(record.violation, Some(violation));
    }

    /// A set of the roster whose record this node has never read, while the
    /// pool could not say whether it has one, serves reads but takes no new
    /// objects: it may be laid out by a rule this node cannot see yet. Once
    /// the record is read the set takes writes (plan 05, phase 1 gate, point
    /// 7).
    #[tokio::test]
    async fn a_set_whose_record_is_unknown_takes_no_writes_until_it_is_read() {
        let (_tmp, service) = multi_set_service(1);
        let extra = additional_set(1);
        service
            .metadata
            .put_erasure_set_roster(&EcErasureSetRoster {
                pool_id: 9,
                sets: vec![EcErasureSetSpec {
                    set_id: FIRST_SET_ID,
                    node_ids: extra.slot_assignment.distinct_nodes(),
                    state: ErasureSetState::Active,
                    created_at: 1,
                }],
                inherited_state: ErasureSetState::Active,
                inherited_node_ids: Vec::new(),
                hlc: HlcTimestamp::from_record(1, 1, 1),
                updated_at: 1,
            })
            .await
            .unwrap();

        // No local copy and no cluster read path: the record is unknown.
        service.refresh_erasure_sets().await.unwrap();
        let sets = service.placement.sets();
        let held = sets.get(FIRST_SET_ID).unwrap();
        assert!(held.layout_pending);
        assert_eq!(held.state, ErasureSetState::Active);
        assert!(!held.accepts_writes());
        // Without a cluster read path there is no record of the inherited set
        // to miss, so it is not held.
        assert!(sets.get(service.default_set.set_id).unwrap().accepts_writes());
        for _ in 0..8 {
            assert_ne!(
                service.placement.select_write_set().unwrap().set_id,
                FIRST_SET_ID
            );
        }
        let listing = service.list_erasure_sets("ec").await.unwrap();
        let listed = listing["sets"]
            .as_array()
            .unwrap()
            .iter()
            .find(|set| set["set_id"] == json!(FIRST_SET_ID))
            .cloned()
            .unwrap();
        assert_eq!(listed["layout_pending"], json!(true), "{listed}");

        // The record arrives: the set is laid out by it and takes writes.
        service
            .metadata
            .put_set_layout_record(&EcSetLayoutRecord::v1(
                &extra,
                EcTopologyPolicy::Strict,
                1,
                HlcTimestamp::from_record(1, 1, 1),
            ))
            .await
            .unwrap();
        service.refresh_erasure_sets().await.unwrap();
        let known = service.placement.sets().get(FIRST_SET_ID).cloned().unwrap();
        assert!(!known.layout_pending);
        assert!(known.accepts_writes());
    }

    /// A record naming other nodes than the roster gives a set is not applied
    /// to it, and does not stop the node: the set keeps its configured layout.
    #[tokio::test]
    async fn a_roster_set_record_naming_other_nodes_is_not_applied() {
        let (_tmp, service) = multi_set_service(1);
        let extra = additional_set(1);
        let mut record = EcSetLayoutRecord::v1(
            &extra,
            EcTopologyPolicy::Strict,
            1,
            HlcTimestamp::from_record(1, 1, 1),
        );
        record.node_ids.reverse();
        record.node_ids[0] = NodeId(9_999);
        service.metadata.put_set_layout_record(&record).await.unwrap();

        cache_second_set_roster(&service).await;

        let set = service.placement.sets().get(FIRST_SET_ID).cloned().unwrap();
        assert_eq!(set.slot_assignment, extra.slot_assignment);
        assert!(!set.layout_pending);
    }
    // ------------------------------------------------------------------
    // Dedup measurement — plan 07, phase 0
    // ------------------------------------------------------------------

    /// A manifest describing stored EC bytes for one object and content.
    ///
    /// Sized properly rather than patched on top of a fixture: `put_manifest`
    /// validates, and a live manifest has to hold chunks whose data sizes add up
    /// to `original_size` and shards wide enough to carry them.
    fn dedup_manifest(
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        content: u8,
        size: u64,
    ) -> EcObjectManifest {
        let profile = EcProfileSnapshot::rs_small();
        let encoded_size = size.div_ceil(u64::from(profile.k)).max(1);
        let shards = (0..profile.total_shards())
            .map(|slot_index| {
                let bytes = vec![slot_index; encoded_size as usize];
                ShardManifest {
                    slot_index,
                    node_id: NodeId(u128::from(slot_index) + 1),
                    role: profile.role_for_slot(slot_index).unwrap(),
                    crc32: crc32fast::hash(&bytes),
                    sha256: sha256(&bytes),
                    size: encoded_size,
                }
            })
            .collect();

        let mut manifest = manifest_with_state(EcPlacementState::HotReplicasPurged);
        manifest.bucket = bucket.to_string();
        manifest.key = key.to_string();
        manifest.version_id = version_id.map(str::to_string);
        manifest.content_hash_sha256 = [content; 32];
        manifest.original_size = size;
        manifest.chunks = vec![ChunkManifest {
            chunk_index: 0,
            data_size: size,
            encoded_size,
            chunk_sha256: [content; 32],
            shards,
        }];
        manifest.operation_id = Uuid::new_v4();
        manifest
    }

    async fn dedup_report_of(
        service: &EcOperationsService,
        max_entries: usize,
    ) -> serde_json::Value {
        service
            .dedup_report(EcAdminDedupReportRequest {
                max_entries,
                local_only: true,
            })
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn dedup_report_counts_repeats_and_names_an_example() {
        let (_tmp, service) = test_service(1);
        for manifest in [
            dedup_manifest("photos", "a.bin", None, 7, 1_000),
            dedup_manifest("backups", "b.bin", None, 7, 1_000),
            dedup_manifest("photos", "c.bin", None, 8, 1_000),
        ] {
            service.metadata.put_manifest(&manifest).await.unwrap();
        }

        let report = dedup_report_of(&service, 4_096).await;

        assert_eq!(report["totals"]["objects"], json!(3));
        assert_eq!(report["totals"]["distinct_contents"], json!(2));
        assert_eq!(report["totals"]["ec_bytes"], json!(3_000));
        // Two copies of one thousand bytes save one thousand, not two.
        assert_eq!(report["within_set"]["groups"], json!(1));
        assert_eq!(report["within_set"]["redundant_bytes"], json!(1_000));
        assert_eq!(report["by_relation"]["cross_bucket"]["groups"], json!(1));
        let sample = &report["samples"][0];
        assert_eq!(sample["copies"], json!(2));
        assert_eq!(sample["relation"], json!("cross_bucket"));
        assert_eq!(sample["objects"].as_array().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn dedup_report_leaves_out_manifests_that_store_no_ec_bytes() {
        let (_tmp, service) = test_service(1);
        let live = dedup_manifest("photos", "live.bin", None, 7, 1_000);
        let mut deleted = dedup_manifest("photos", "deleted.bin", None, 7, 1_000);
        // A tombstone describes no bytes, so it carries no chunks either.
        deleted.is_deleted = true;
        deleted.state = EcPlacementState::Tombstone;
        deleted.chunks.clear();
        let mut promoted = dedup_manifest("photos", "promoted.bin", None, 7, 1_000);
        promoted.state = EcPlacementState::Promoted;
        let mut queued = dedup_manifest("photos", "queued.bin", None, 7, 1_000);
        queued.state = EcPlacementState::EcTranscodeQueued;
        for manifest in [live, deleted, promoted, queued] {
            service.metadata.put_manifest(&manifest).await.unwrap();
        }

        let report = dedup_report_of(&service, 4_096).await;

        // All four share a content hash, but only one of them is bytes erasure
        // coding stores. Counting the other three would report a saving on data
        // that is not there — the deleted object has none, and the promoted and
        // queued ones are on RF=3 replicas, which dedup at this tier never sees.
        assert_eq!(report["totals"]["manifests_scanned"], json!(4));
        assert_eq!(report["totals"]["objects"], json!(1));
        assert_eq!(report["within_set"]["redundant_bytes"], json!(0));
    }

    #[tokio::test]
    async fn dedup_report_stops_at_the_scan_limit_and_says_so() {
        let (_tmp, service) = test_service(1);
        for index in 0..5 {
            let manifest = dedup_manifest("photos", &format!("object-{index}.bin"), None, 7, 1_000);
            service.metadata.put_manifest(&manifest).await.unwrap();
        }

        let report = dedup_report_of(&service, 2).await;

        assert_eq!(report["totals"]["manifests_scanned"], json!(2));
        assert_eq!(report["scope"]["scan_limit"], json!(2));
        assert_eq!(report["scope"]["nodes_truncated"], json!(1));
        assert_eq!(report["scope"]["complete"], json!(false));
        // A walk that saw a prefix of the store cannot be read as a share of the
        // whole of it, however clean the number looks.
        assert_eq!(report["decision"]["verdict"], json!("inconclusive"));
    }

    #[tokio::test]
    async fn dedup_report_names_every_node_it_did_not_read() {
        let (_tmp, service) = test_service(1);
        service
            .metadata
            .put_manifest(&dedup_manifest("photos", "a.bin", None, 7, 1_000))
            .await
            .unwrap();

        let report = dedup_report_of(&service, 4_096).await;

        // The pool is six nodes wide and this report read one of them, so five
        // are named with a reason. Silence about them would turn a sixth of the
        // pool into a statement about all of it.
        let pool_width = pool_nodes(&service.placement.sets()).len();
        assert_eq!(report["scope"]["nodes_total"], json!(pool_width));
        assert_eq!(report["scope"]["nodes_answered"], json!(1));
        assert_eq!(
            report["scope"]["nodes_unread"].as_array().unwrap().len(),
            pool_width - 1
        );
        assert_eq!(report["scope"]["complete"], json!(false));
        assert_eq!(report["decision"]["verdict"], json!("inconclusive"));
        assert_eq!(report["decision"]["adr"], json!("EC2-121"));
    }

    #[tokio::test]
    async fn dedup_report_writes_nothing() {
        let (_tmp, service) = test_service(1);
        let manifest = dedup_manifest("photos", "a.bin", None, 7, 1_000);
        service.metadata.put_manifest(&manifest).await.unwrap();

        dedup_report_of(&service, 4_096).await;

        // Phase 0 measures and nothing else: the rollback of this phase is
        // deleting the endpoint, which is only true while it touches no data.
        let after = service.metadata.list_manifests(4_096).await.unwrap();
        assert_eq!(after, vec![manifest]);
    }

    #[tokio::test]
    async fn dedup_report_rejects_a_limit_of_zero() {
        let (_tmp, service) = test_service(1);

        let error = service
            .dedup_report(EcAdminDedupReportRequest {
                max_entries: 0,
                local_only: true,
            })
            .await
            .unwrap_err();

        assert!(matches!(error, ClusterError::Config(_)), "got {error:?}");
    }
}
