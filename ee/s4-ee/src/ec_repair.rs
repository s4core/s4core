// Copyright 2026 S4Core Team
//
// Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

//! Erasure Coding v1 repair service foundation.
//!
//! This module owns the Enterprise-only repair boundary for missing and
//! corrupt EC shards. It provides durable queue records, explicit throttling,
//! manifest-authoritative verification, codec-driven shard reconstruction, and
//! rejoin safety policy. The actual storage/RPC integration is injected through
//! small ports so CE data paths remain unchanged.

use std::collections::{BTreeMap, HashMap};
use std::ops::Bound;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use s4_cluster::{HlcTimestamp, NodeId};
use s4_codec::{CodecError, RecoveryPath, Shard};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;

use crate::codec_runtime::{EcCodecCpuLimiter, EcCpuError};
use crate::ec_metadata::{
    ChunkManifest, EcManifestReadResult, EcMetadataError, EcMetadataStore, EcObjectManifest,
    EcObjectVersionKey, EcPlacementState, EcReadyRepairTasks, EcRepairKind, EcRepairStatus,
    EcRepairTaskRecord, ShardManifest,
};
use crate::ec_metrics;
use crate::ec_metrics::EcRepairLimit;
use crate::ec_read::{EcReadError, EcReadRepairKind, EcReadRepairQueue, EcReadRepairTask};
use crate::ec_repair_plan::{
    RepairBytes, RepairLeg, RepairPlanner, RepairSourcePlan, RepairStrategy, RepairTraffic,
    SlotState, TransferScope,
};
use crate::erasure_coding::{NodeDistance, PoolTopologyView};

/// Default maximum repairs running across this node.
pub const DEFAULT_EC_REPAIR_MAX_GLOBAL_CONCURRENCY: usize = 4;
/// Default maximum repairs targeting the same shard owner node.
pub const DEFAULT_EC_REPAIR_MAX_NODE_CONCURRENCY: usize = 1;
/// Default maximum repairs in the same erasure set.
pub const DEFAULT_EC_REPAIR_MAX_SET_CONCURRENCY: usize = 2;
/// Default maximum repairs in the same failure domain.
pub const DEFAULT_EC_REPAIR_MAX_FAILURE_DOMAIN_CONCURRENCY: usize = 1;
/// Default maximum durable repair queue depth.
pub const DEFAULT_EC_REPAIR_MAX_QUEUE_DEPTH: usize = 10_000;
/// Default retry backoff after throttling.
pub const DEFAULT_EC_REPAIR_THROTTLE_RETRY_SECS: u64 = 5;
/// Default initial retry backoff for retryable repair failures.
pub const DEFAULT_EC_REPAIR_INITIAL_BACKOFF_SECS: u64 = 30;
/// Default maximum retry backoff for retryable repair failures.
pub const DEFAULT_EC_REPAIR_MAX_BACKOFF_SECS: u64 = 3_600;
/// Default maximum downtime before incremental rejoin is unsafe.
pub const DEFAULT_EC_REPAIR_MAX_REJOIN_DOWNTIME_DAYS: u64 = 3;
/// Default maximum stale `Running` tasks recovered in one worker pass.
pub const DEFAULT_EC_REPAIR_SCAN_LIMIT: usize = 1_024;
/// Default timeout before a `Running` repair task lease is considered stale.
pub const DEFAULT_EC_REPAIR_RUNNING_TIMEOUT_SECS: u64 = 900;
/// Default age at which a ready repair task starts taking turns of its own.
pub const DEFAULT_EC_REPAIR_PRIORITY_AGE_BOOST_SECS: u64 = 600;
/// Default share of a set's turns given to its longest-waiting ready task.
pub const DEFAULT_EC_REPAIR_PRIORITY_AGE_BOOST_EVERY: usize = 8;

/// Long-offline node policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EcLongOfflineRejoinPolicy {
    /// A long-offline node must bootstrap from scratch.
    NeedsBootstrap,
    /// A long-offline node may use incremental repair only after admin approval.
    RequireAdminApproval,
}

/// EC repair scheduling and safety configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EcRepairConfig {
    /// Maximum repairs running across this node.
    pub max_global_concurrency: usize,
    /// Maximum repairs targeting the same shard owner node.
    pub max_node_concurrency: usize,
    /// Maximum repairs in the same erasure set.
    pub max_erasure_set_concurrency: usize,
    /// Maximum repairs in the same failure domain.
    pub max_failure_domain_concurrency: usize,
    /// Maximum durable repair queue depth.
    pub max_queue_depth: usize,
    /// Optional global repair bandwidth budget in bytes/sec.
    pub global_bandwidth_bytes_per_sec: Option<u64>,
    /// Optional per-failure-domain repair bandwidth budget in bytes/sec.
    pub failure_domain_bandwidth_bytes_per_sec: Option<u64>,
    /// Retry backoff after throttling.
    pub throttle_retry_secs: u64,
    /// Initial retry backoff for retryable repair failures.
    pub initial_backoff_secs: u64,
    /// Maximum retry backoff for retryable repair failures.
    pub max_backoff_secs: u64,
    /// Maximum downtime before incremental rejoin is unsafe.
    pub max_rejoin_downtime_days: u64,
    /// Policy used when a node exceeds the maximum rejoin downtime.
    pub long_offline_rejoin_policy: EcLongOfflineRejoinPolicy,
    /// Maximum stale `Running` tasks recovered in one worker pass.
    ///
    /// The name says "scan limit" because the setting was meant to bound the
    /// walk that picks the next task, and it never has: picking a task reads
    /// the whole queue, which is what counting the losses of a chunk needs
    /// (decision EC2-161). Only the recovery of tasks a crashed worker left
    /// running is bounded by this, and renaming an environment variable
    /// operators already set would cost more than the misleading name does.
    pub repair_scan_limit: usize,
    /// Timeout before a `Running` task is recovered after worker crash.
    pub running_timeout_secs: u64,
    /// Age at which a ready task starts taking turns ahead of more urgent ones.
    ///
    /// Zero switches the starvation guard off and leaves the order to urgency
    /// alone, which is the rollback lever of decision EC2-162.
    pub priority_age_boost_secs: u64,
    /// How often a turn of a set goes to its longest-waiting ready task.
    ///
    /// One turn in this many. Giving the guard every turn instead would undo
    /// the priority in exactly the incident it was written for, where the whole
    /// queue is older than the threshold.
    pub priority_age_boost_every: usize,
}

impl EcRepairConfig {
    /// Parse `S4_EC_REPAIR_*` settings from the environment.
    pub fn from_env() -> Result<Self, EcRepairError> {
        let max_global_concurrency = parse_optional_usize("S4_EC_REPAIR_MAX_GLOBAL_CONCURRENCY")?
            .unwrap_or(DEFAULT_EC_REPAIR_MAX_GLOBAL_CONCURRENCY);
        let max_node_concurrency = parse_optional_usize("S4_EC_REPAIR_MAX_NODE_CONCURRENCY")?
            .unwrap_or(DEFAULT_EC_REPAIR_MAX_NODE_CONCURRENCY);
        let max_erasure_set_concurrency = parse_optional_usize("S4_EC_REPAIR_MAX_SET_CONCURRENCY")?
            .unwrap_or(DEFAULT_EC_REPAIR_MAX_SET_CONCURRENCY);
        let max_failure_domain_concurrency =
            parse_optional_usize("S4_EC_REPAIR_MAX_FAILURE_DOMAIN_CONCURRENCY")?
                .unwrap_or(DEFAULT_EC_REPAIR_MAX_FAILURE_DOMAIN_CONCURRENCY);
        let max_queue_depth = parse_optional_usize("S4_EC_REPAIR_MAX_QUEUE_DEPTH")?
            .unwrap_or(DEFAULT_EC_REPAIR_MAX_QUEUE_DEPTH);

        for (name, value) in [
            (
                "S4_EC_REPAIR_MAX_GLOBAL_CONCURRENCY",
                max_global_concurrency,
            ),
            ("S4_EC_REPAIR_MAX_NODE_CONCURRENCY", max_node_concurrency),
            (
                "S4_EC_REPAIR_MAX_SET_CONCURRENCY",
                max_erasure_set_concurrency,
            ),
            (
                "S4_EC_REPAIR_MAX_FAILURE_DOMAIN_CONCURRENCY",
                max_failure_domain_concurrency,
            ),
            ("S4_EC_REPAIR_MAX_QUEUE_DEPTH", max_queue_depth),
        ] {
            if value == 0 {
                return Err(EcRepairError::InvalidConfig {
                    name,
                    reason: "must be greater than 0".to_string(),
                });
            }
        }

        let throttle_retry_secs = parse_optional_u64("S4_EC_REPAIR_THROTTLE_RETRY_SECS")?
            .unwrap_or(DEFAULT_EC_REPAIR_THROTTLE_RETRY_SECS);
        let initial_backoff_secs = parse_optional_u64("S4_EC_REPAIR_INITIAL_BACKOFF_SECS")?
            .unwrap_or(DEFAULT_EC_REPAIR_INITIAL_BACKOFF_SECS);
        let max_backoff_secs = parse_optional_u64("S4_EC_REPAIR_MAX_BACKOFF_SECS")?
            .unwrap_or(DEFAULT_EC_REPAIR_MAX_BACKOFF_SECS);
        for (name, value) in [
            ("S4_EC_REPAIR_THROTTLE_RETRY_SECS", throttle_retry_secs),
            ("S4_EC_REPAIR_INITIAL_BACKOFF_SECS", initial_backoff_secs),
            ("S4_EC_REPAIR_MAX_BACKOFF_SECS", max_backoff_secs),
        ] {
            if value == 0 {
                return Err(EcRepairError::InvalidConfig {
                    name,
                    reason: "must be greater than 0".to_string(),
                });
            }
        }

        let max_rejoin_downtime_days = parse_optional_u64("S4_MAX_REJOIN_DOWNTIME_DAYS")?
            .unwrap_or(DEFAULT_EC_REPAIR_MAX_REJOIN_DOWNTIME_DAYS);
        let long_offline_rejoin_policy = match std::env::var("S4_EC_LONG_OFFLINE_REJOIN_POLICY")
            .unwrap_or_else(|_| "needs_bootstrap".to_string())
            .as_str()
        {
            "needs_bootstrap" | "bootstrap" => EcLongOfflineRejoinPolicy::NeedsBootstrap,
            "require_admin_approval" | "admin_approval" => {
                EcLongOfflineRejoinPolicy::RequireAdminApproval
            }
            other => {
                return Err(EcRepairError::InvalidConfig {
                        name: "S4_EC_LONG_OFFLINE_REJOIN_POLICY",
                        reason: format!(
                            "unknown value '{other}'; expected needs_bootstrap or require_admin_approval"
                        ),
                    });
            }
        };

        let repair_scan_limit = parse_optional_usize("S4_EC_REPAIR_SCAN_LIMIT")?
            .unwrap_or(DEFAULT_EC_REPAIR_SCAN_LIMIT);
        if repair_scan_limit == 0 {
            return Err(EcRepairError::InvalidConfig {
                name: "S4_EC_REPAIR_SCAN_LIMIT",
                reason: "must be greater than 0".to_string(),
            });
        }
        let running_timeout_secs = parse_optional_u64("S4_EC_REPAIR_RUNNING_TIMEOUT_SECS")?
            .unwrap_or(DEFAULT_EC_REPAIR_RUNNING_TIMEOUT_SECS);
        if running_timeout_secs == 0 {
            return Err(EcRepairError::InvalidConfig {
                name: "S4_EC_REPAIR_RUNNING_TIMEOUT_SECS",
                reason: "must be greater than 0".to_string(),
            });
        }

        let priority_age_boost_secs = parse_optional_u64("S4_EC_REPAIR_PRIORITY_AGE_BOOST_SECS")?
            .unwrap_or(DEFAULT_EC_REPAIR_PRIORITY_AGE_BOOST_SECS);
        let priority_age_boost_every =
            parse_optional_usize("S4_EC_REPAIR_PRIORITY_AGE_BOOST_EVERY")?
                .unwrap_or(DEFAULT_EC_REPAIR_PRIORITY_AGE_BOOST_EVERY);
        if priority_age_boost_every == 0 {
            return Err(EcRepairError::InvalidConfig {
                name: "S4_EC_REPAIR_PRIORITY_AGE_BOOST_EVERY",
                reason: "must be greater than 0; switch the guard off with \
                         S4_EC_REPAIR_PRIORITY_AGE_BOOST_SECS=0 instead"
                    .to_string(),
            });
        }

        Ok(Self {
            max_global_concurrency,
            max_node_concurrency,
            max_erasure_set_concurrency,
            max_failure_domain_concurrency,
            max_queue_depth,
            global_bandwidth_bytes_per_sec: parse_optional_u64(
                "S4_EC_REPAIR_GLOBAL_BANDWIDTH_BYTES_PER_SEC",
            )?
            .filter(|limit| *limit > 0),
            failure_domain_bandwidth_bytes_per_sec: parse_optional_u64(
                "S4_EC_REPAIR_FAILURE_DOMAIN_BANDWIDTH_BYTES_PER_SEC",
            )?
            .filter(|limit| *limit > 0),
            throttle_retry_secs,
            initial_backoff_secs,
            max_backoff_secs,
            max_rejoin_downtime_days,
            long_offline_rejoin_policy,
            repair_scan_limit,
            running_timeout_secs,
            priority_age_boost_secs,
            priority_age_boost_every,
        })
    }

    fn throttle_retry_nanos(&self) -> u64 {
        seconds_to_nanos(self.throttle_retry_secs)
    }

    fn retry_delay_nanos(&self, attempt: u32, repair_id: Uuid) -> u64 {
        let exponent = attempt.saturating_sub(1).min(31);
        let base = self
            .initial_backoff_secs
            .saturating_mul(1u64 << exponent)
            .min(self.max_backoff_secs);
        seconds_to_nanos(base.saturating_add(deterministic_jitter_secs(repair_id, attempt, base)))
    }

    fn running_timeout_nanos(&self) -> u64 {
        seconds_to_nanos(self.running_timeout_secs)
    }

    /// How long a ready task waits before it starts taking turns of its own,
    /// or `None` when the starvation guard is switched off.
    fn age_boost_after_nanos(&self) -> Option<u64> {
        (self.priority_age_boost_secs > 0).then(|| seconds_to_nanos(self.priority_age_boost_secs))
    }
}

impl Default for EcRepairConfig {
    fn default() -> Self {
        Self {
            max_global_concurrency: DEFAULT_EC_REPAIR_MAX_GLOBAL_CONCURRENCY,
            max_node_concurrency: DEFAULT_EC_REPAIR_MAX_NODE_CONCURRENCY,
            max_erasure_set_concurrency: DEFAULT_EC_REPAIR_MAX_SET_CONCURRENCY,
            max_failure_domain_concurrency: DEFAULT_EC_REPAIR_MAX_FAILURE_DOMAIN_CONCURRENCY,
            max_queue_depth: DEFAULT_EC_REPAIR_MAX_QUEUE_DEPTH,
            global_bandwidth_bytes_per_sec: None,
            failure_domain_bandwidth_bytes_per_sec: None,
            throttle_retry_secs: DEFAULT_EC_REPAIR_THROTTLE_RETRY_SECS,
            initial_backoff_secs: DEFAULT_EC_REPAIR_INITIAL_BACKOFF_SECS,
            max_backoff_secs: DEFAULT_EC_REPAIR_MAX_BACKOFF_SECS,
            max_rejoin_downtime_days: DEFAULT_EC_REPAIR_MAX_REJOIN_DOWNTIME_DAYS,
            long_offline_rejoin_policy: EcLongOfflineRejoinPolicy::NeedsBootstrap,
            repair_scan_limit: DEFAULT_EC_REPAIR_SCAN_LIMIT,
            running_timeout_secs: DEFAULT_EC_REPAIR_RUNNING_TIMEOUT_SECS,
            priority_age_boost_secs: DEFAULT_EC_REPAIR_PRIORITY_AGE_BOOST_SECS,
            priority_age_boost_every: DEFAULT_EC_REPAIR_PRIORITY_AGE_BOOST_EVERY,
        }
    }
}

/// Request to queue one shard repair.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EcRepairQueueRequest {
    /// Object version needing repair.
    pub object: EcObjectVersionKey,
    /// Operation that produced the winning manifest.
    pub operation_id: Uuid,
    /// Erasure set that owns the shard.
    pub erasure_set_id: u64,
    /// Logical chunk index.
    pub chunk_index: u32,
    /// Shard slot index.
    pub slot_index: u8,
    /// Assigned shard owner node.
    pub node_id: NodeId,
    /// Repair trigger class.
    pub kind: EcRepairKind,
    /// Human-readable diagnostic reason.
    pub reason: String,
    /// Failure-domain key the repair is throttled under.
    ///
    /// `None` lets the coordinator fill in the zone of the shard's owner, which
    /// is what every production path does (plan 05, phase 3, EC2-085).
    pub failure_domain: Option<String>,
}

impl EcRepairQueueRequest {
    /// Build a queue request from a Phase 5 read-repair task.
    pub fn from_read_repair(task: EcReadRepairTask) -> Self {
        let kind = match task.kind {
            EcReadRepairKind::MissingShard => EcRepairKind::MissingShard,
            EcReadRepairKind::CorruptShard => EcRepairKind::CorruptShard,
        };
        Self {
            object: task.object,
            operation_id: task.operation_id,
            erasure_set_id: task.erasure_set_id,
            chunk_index: task.chunk_index,
            slot_index: task.slot_index,
            node_id: task.node_id,
            kind,
            reason: task.reason,
            failure_domain: None,
        }
    }

    /// Validate queue request fields.
    pub fn validate(&self) -> Result<(), EcRepairError> {
        self.object.validate()?;
        if self.operation_id.is_nil() {
            return Err(EcRepairError::InvalidTask {
                reason: "operation_id must not be nil".to_string(),
            });
        }
        if self.reason.trim().is_empty() {
            return Err(EcRepairError::InvalidTask {
                reason: "reason must not be empty".to_string(),
            });
        }
        if self.failure_domain.as_ref().is_some_and(|domain| domain.trim().is_empty()) {
            return Err(EcRepairError::InvalidTask {
                reason: "failure_domain must not be empty when present".to_string(),
            });
        }
        Ok(())
    }
}

/// Port used by repair workers for quorum-aware manifest reads.
#[async_trait]
pub trait EcRepairMetadataCoordinator: Send + Sync {
    /// Read an EC manifest through quorum metadata semantics.
    async fn quorum_read_manifest(
        &self,
        object: &EcObjectVersionKey,
    ) -> Result<EcManifestReadResult, EcRepairError>;
}

/// Request for a repair source shard candidate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EcRepairShardReadRequest {
    /// Object version addressed by the shard.
    pub object: EcObjectVersionKey,
    /// Operation that produced the winning manifest.
    pub operation_id: Uuid,
    /// Logical chunk index.
    pub chunk_index: u32,
    /// Shard slot index.
    pub slot_index: u8,
    /// Assigned shard owner node.
    pub node_id: NodeId,
}

/// Response for a repair source shard candidate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EcRepairShardReadResponse {
    /// Shard bytes.
    pub data: Bytes,
    /// Replica-provided CRC32, used only for diagnostics.
    pub replica_crc32: Option<u32>,
}

/// Port used by repair workers to read verified source shards.
#[async_trait]
pub trait EcRepairShardReader: Send + Sync {
    /// Read a shard candidate. `Ok(None)` means this candidate is unavailable.
    async fn read_repair_shard(
        &self,
        request: EcRepairShardReadRequest,
    ) -> Result<Option<EcRepairShardReadResponse>, EcRepairError>;
}

/// Request to durably write one repaired shard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EcRepairShardWriteRequest {
    /// Object version addressed by the shard.
    pub object: EcObjectVersionKey,
    /// Repair task ID.
    pub repair_id: Uuid,
    /// Operation that produced the winning manifest.
    pub operation_id: Uuid,
    /// Erasure set that owns the shard.
    pub erasure_set_id: u64,
    /// Logical chunk index.
    pub chunk_index: u32,
    /// Expected manifest entry for the repaired shard.
    pub expected: ShardManifest,
    /// Repaired shard bytes.
    pub data: Bytes,
    /// Repair trigger class.
    pub kind: EcRepairKind,
}

/// Durable write acknowledgement for one repaired shard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EcRepairShardWriteAck {
    /// Node that wrote and indexed the shard.
    pub node_id: NodeId,
    /// Written shard size.
    pub size: u64,
    /// Written shard CRC32.
    pub crc32: u32,
    /// Written shard SHA-256.
    pub sha256: [u8; 32],
    /// True only after bytes are durable on the local node.
    pub durable: bool,
    /// True only after the local `ec_shards` index was updated atomically with the write.
    pub local_index_updated: bool,
}

/// Port used by repair workers to commit a repaired shard.
#[async_trait]
pub trait EcRepairShardWriter: Send + Sync {
    /// Durably write repaired bytes and update the local shard index.
    async fn write_repaired_shard(
        &self,
        request: EcRepairShardWriteRequest,
    ) -> Result<EcRepairShardWriteAck, EcRepairError>;
}

/// Repair frontier update after a shard has been committed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EcRepairFrontierUpdate {
    /// Object version that was repaired.
    pub object: EcObjectVersionKey,
    /// Repair task ID.
    pub repair_id: Uuid,
    /// Operation that produced the winning manifest.
    pub operation_id: Uuid,
    /// Erasure set that owns the shard.
    pub erasure_set_id: u64,
    /// Logical chunk index.
    pub chunk_index: u32,
    /// Shard slot index.
    pub slot_index: u8,
    /// Shard owner node.
    pub node_id: NodeId,
    /// Manifest HLC observed by the repair.
    pub manifest_hlc: HlcTimestamp,
    /// Repair completion time in nanoseconds since Unix epoch.
    pub repaired_at: u64,
}

/// Port used to advance EC repair frontiers.
#[async_trait]
pub trait EcRepairFrontierUpdater: Send + Sync {
    /// Mark one repaired shard as observed by the repair frontier.
    async fn mark_shard_repaired(
        &self,
        update: EcRepairFrontierUpdate,
    ) -> Result<(), EcRepairError>;
}

/// No-op repair frontier updater for tests and partially integrated callers.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopEcRepairFrontierUpdater;

#[async_trait]
impl EcRepairFrontierUpdater for NoopEcRepairFrontierUpdater {
    async fn mark_shard_repaired(
        &self,
        _update: EcRepairFrontierUpdate,
    ) -> Result<(), EcRepairError> {
        Ok(())
    }
}

/// Result of asking for one shard to be repaired.
///
/// One task per shard is live at a time, so asking twice returns the same task
/// and queues nothing. `created` is what tells the two apart, and a whole-node
/// rebuild — a request repeated until nothing is left — needs the difference to
/// report honestly what it did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EcRepairQueueOutcome {
    /// The live task for this shard, new or already there.
    pub task: EcRepairTaskRecord,
    /// Whether this call is what put it in the queue.
    pub created: bool,
}

/// Result of one repair attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EcRepairReport {
    /// Durable repair task after the attempt.
    pub task: EcRepairTaskRecord,
    /// How the shard was rebuilt.
    pub strategy: RepairStrategy,
    /// Source shards read and verified, including any a widened plan did not
    /// end up using.
    pub verified_source_shards: usize,
    /// Source shards that were missing, corrupt or out of reach.
    pub skipped_source_shards: usize,
    /// Bytes written for the repaired shard.
    pub repaired_bytes: u64,
    /// Bytes this repair moved, by leg and zone scope.
    pub transfers: RepairBytes,
    /// Whether the writer acknowledged durable bytes and local index update.
    pub committed: bool,
}

/// Rejoin decision for an EC shard owner node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EcNodeRejoinDecision {
    /// Incremental EC repair is allowed.
    IncrementalRepairAllowed,
    /// Node must bootstrap from scratch before serving EC shards.
    NeedsBootstrap,
    /// Node requires explicit admin approval before incremental repair.
    AdminApprovalRequired,
}

/// EC repair coordinator.
#[derive(Clone)]
pub struct EcRepairCoordinator {
    metadata: Arc<EcMetadataStore>,
    codec_limiter: EcCodecCpuLimiter,
    throttles: EcRepairThrottles,
    /// Erasure set served last, so the next pick starts after it.
    ///
    /// Shared across clones on purpose: every clone drains the same durable
    /// queue, and a per-clone cursor would let two of them serve the same set
    /// in turn while a third set waited.
    last_served_set: Arc<AtomicU64>,
    /// Turns each erasure set has been served, so that one turn in
    /// `priority_age_boost_every` can go to the task that has waited longest.
    ///
    /// Shared across clones for the same reason as `last_served_set`, and kept
    /// in memory only: a restart shifts where the rotation stands and leaves
    /// the waiting bound itself untouched (decision EC2-162).
    turns_by_set: Arc<Mutex<HashMap<u64, u64>>>,
    /// The pool's topology as the node running the repairs sees it.
    ///
    /// Tells which zone each shard's owner is in — the key of the zone limits
    /// — how far every source is, and which bytes cross a zone (plan 05,
    /// phase 3).
    topology: Arc<PoolTopologyView>,
}

impl EcRepairCoordinator {
    /// Create a repair coordinator from explicit dependencies.
    ///
    /// `topology` is the pool as the node running the repairs sees it: repairs
    /// execute on the node that queued them.
    pub fn new(
        metadata: Arc<EcMetadataStore>,
        codec_limiter: EcCodecCpuLimiter,
        config: EcRepairConfig,
        topology: PoolTopologyView,
    ) -> Self {
        Self {
            metadata,
            codec_limiter,
            throttles: EcRepairThrottles::new(config),
            last_served_set: Arc::new(AtomicU64::new(0)),
            turns_by_set: Arc::new(Mutex::new(HashMap::new())),
            topology: Arc::new(topology),
        }
    }

    /// The zone of `node_id`, as the key its repairs are throttled under.
    fn owner_zone(&self, node_id: NodeId) -> Option<String> {
        self.topology.zone_of(node_id).map(|zone| zone.zone().to_string())
    }

    /// How far the node holding `slot` of `chunk` is from this node.
    fn distance_to_slot(&self, chunk: &ChunkManifest, slot: u8) -> NodeDistance {
        chunk
            .shards
            .iter()
            .find(|shard| shard.slot_index == slot)
            .map_or(NodeDistance::Remote, |shard| {
                self.topology.distance_to(shard.node_id)
            })
    }

    /// What reading `sources` of `chunk` to this node costs against the limits.
    fn source_traffic(
        &self,
        chunk: &ChunkManifest,
        sources: &[u8],
        zone: Option<&str>,
    ) -> RepairTraffic {
        let mut traffic = RepairTraffic::default();
        for shard in chunk.shards.iter().filter(|shard| sources.contains(&shard.slot_index)) {
            traffic.add(
                &self.topology,
                zone,
                shard.node_id,
                self.topology.local_node_id(),
                shard.size,
            );
        }
        traffic
    }

    /// Count `bytes` moved from `from` to `to` in the repair's report and in
    /// `s4_ec_repair_bytes_total` alike, so the two cannot disagree.
    fn note_transfer(
        &self,
        transfers: &mut RepairBytes,
        leg: RepairLeg,
        from: NodeId,
        to: NodeId,
        bytes: u64,
    ) {
        let scope = TransferScope::between(&self.topology, from, to);
        transfers.add(leg, scope, bytes);
        ec_metrics::record_repair_bytes(leg, scope, bytes);
    }

    /// Pick the next erasure set to serve, taking turns between them.
    ///
    /// Sets are ordered by id, and the pick is the first one after the set
    /// served last, wrapping around. A set with thousands of queued shards
    /// therefore yields to its neighbour after each task instead of holding the
    /// worker for as long as its backlog lasts. With one set in the pool the
    /// answer is always that set, which is EC v1 behaviour unchanged.
    fn take_turn<'a>(
        &self,
        ready: &'a BTreeMap<u64, EcReadyRepairTasks>,
        now: u64,
    ) -> Option<&'a EcRepairTaskRecord> {
        let last = self.last_served_set.load(Ordering::Acquire);
        let (set_id, candidates) = ready
            .range((Bound::Excluded(last), Bound::Unbounded))
            .next()
            .or_else(|| ready.iter().next())?;
        self.last_served_set.store(*set_id, Ordering::Release);
        Some(self.serve_this_turn(*set_id, candidates, now))
    }

    /// Which of a set's two candidates this turn of the set serves.
    ///
    /// One turn in `priority_age_boost_every` belongs to the task that has
    /// waited longest, and it takes that turn once it has waited longer than
    /// `priority_age_boost_secs`; every other turn goes to the most endangered
    /// chunk. Without such a turn a task of low urgency would be overtaken for
    /// as long as urgent ones keep arriving; with it, the tasks that can still
    /// overtake it are only those already older than it, because the guard's
    /// order is by queue time and nothing queued later can enter ahead of it.
    ///
    /// Giving the guard every turn instead would undo the priority in exactly
    /// the incident it was written for: after an hour of a node's backlog the
    /// whole queue is older than the threshold, and the order would be plain
    /// FIFO again. Decision EC2-162.
    fn serve_this_turn<'a>(
        &self,
        set_id: u64,
        candidates: &'a EcReadyRepairTasks,
        now: u64,
    ) -> &'a EcRepairTaskRecord {
        let Some(boost_after) = self.config().age_boost_after_nanos() else {
            return &candidates.most_urgent;
        };
        let waited = now.saturating_sub(candidates.longest_waiting.enqueued_at);
        if self.is_guard_turn(set_id) && waited >= boost_after {
            &candidates.longest_waiting
        } else {
            &candidates.most_urgent
        }
    }

    /// Whether this serving of `set_id` is the starvation guard's turn.
    ///
    /// Counted per set, so that one set's guard turn is never spent on another
    /// set's queue. A poisoned counter falls back to urgency: the order is
    /// worth less than the repair, and the next turn counts again.
    fn is_guard_turn(&self, set_id: u64) -> bool {
        let every = self.config().priority_age_boost_every.max(1) as u64;
        let Ok(mut turns) = self.turns_by_set.lock() else {
            return false;
        };
        let turn = turns.entry(set_id).or_insert(0);
        *turn = turn.saturating_add(1);
        turn.is_multiple_of(every)
    }

    /// Current repair configuration.
    pub fn config(&self) -> &EcRepairConfig {
        self.throttles.config()
    }

    /// Queue one missing/corrupt shard repair.
    ///
    /// A task without a failure domain gets the zone of the shard's owner,
    /// whichever path queued it — a read, anti-entropy or an operator.
    ///
    /// One task per shard is live at a time: while a repair of this shard is
    /// queued, running or waiting to retry, the existing task is returned. A
    /// finished one does not stand in the way of a new loss, though — see the
    /// private `supersedes` for which finished task a new request replaces.
    pub async fn enqueue_repair(
        &self,
        request: EcRepairQueueRequest,
    ) -> Result<EcRepairTaskRecord, EcRepairError> {
        self.enqueue_repair_reporting(request).await.map(|outcome| outcome.task)
    }

    /// Queue one shard repair and say whether this call is what queued it.
    ///
    /// Same work as [`Self::enqueue_repair`], and the extra word is what a mass
    /// rebuild needs: driving a whole node back is repeating one request until
    /// nothing is left, and a task that was already in the queue must not be
    /// reported as progress made by the call that found it.
    pub async fn enqueue_repair_reporting(
        &self,
        mut request: EcRepairQueueRequest,
    ) -> Result<EcRepairQueueOutcome, EcRepairError> {
        request.validate()?;
        if request.failure_domain.is_none() {
            request.failure_domain = self.owner_zone(request.node_id);
        }
        let repair_id = deterministic_repair_id(&request);
        if let Some(existing) = self.metadata.get_repair_task(repair_id).await? {
            if !supersedes(request.kind, &existing) {
                return Ok(EcRepairQueueOutcome {
                    task: existing,
                    created: false,
                });
            }
            tracing::info!(
                repair_id = %repair_id,
                previous_status = ?existing.status,
                kind = ?request.kind,
                chunk_index = request.chunk_index,
                slot_index = request.slot_index,
                "EC shard queued for repair again after an earlier repair task finished"
            );
        }

        let active = self.metadata.count_active_repair_tasks().await?;
        if active >= self.config().max_queue_depth {
            return Err(EcRepairError::QueueFull {
                limit: self.config().max_queue_depth,
            });
        }

        let now = unix_time_nanos();
        let task = EcRepairTaskRecord {
            repair_id,
            object: request.object,
            operation_id: request.operation_id,
            erasure_set_id: request.erasure_set_id,
            chunk_index: request.chunk_index,
            slot_index: request.slot_index,
            node_id: request.node_id,
            kind: request.kind,
            status: EcRepairStatus::Queued,
            attempts: 0,
            reason: request.reason,
            enqueued_at: now,
            updated_at: now,
            next_retry_at: None,
            failure_domain: request.failure_domain,
        };
        self.metadata.put_repair_task(&task).await?;
        ec_metrics::record_repair_job(task.erasure_set_id);
        Ok(EcRepairQueueOutcome {
            task,
            created: true,
        })
    }

    /// Queue a manual admin-triggered repair.
    pub async fn queue_admin_repair(
        &self,
        mut request: EcRepairQueueRequest,
    ) -> Result<EcRepairQueueOutcome, EcRepairError> {
        request.kind = EcRepairKind::ManualAdmin;
        self.enqueue_repair_reporting(request).await
    }

    /// Inspect a repair task by ID.
    pub async fn get_repair_task(
        &self,
        repair_id: Uuid,
    ) -> Result<Option<EcRepairTaskRecord>, EcRepairError> {
        Ok(self.metadata.get_repair_task(repair_id).await?)
    }

    /// List repair tasks for admin surfaces.
    pub async fn list_repair_tasks(
        &self,
        max_entries: usize,
    ) -> Result<Vec<EcRepairTaskRecord>, EcRepairError> {
        Ok(self.metadata.list_repair_tasks(max_entries).await?)
    }

    /// Repair the next eligible queued or retryable task.
    pub async fn repair_next<M, R, W, F>(
        &self,
        metadata_coordinator: &M,
        shard_reader: &R,
        shard_writer: &W,
        frontier: &F,
    ) -> Result<Option<EcRepairReport>, EcRepairError>
    where
        M: EcRepairMetadataCoordinator,
        R: EcRepairShardReader,
        W: EcRepairShardWriter,
        F: EcRepairFrontierUpdater,
    {
        let now = unix_time_nanos();
        let recovered = self
            .metadata
            .recover_stale_running_repair_tasks(
                now,
                self.config().running_timeout_nanos(),
                self.config().repair_scan_limit,
            )
            .await?;
        if recovered > 0 {
            tracing::warn!(
                recovered,
                "recovered stale EC repair task(s) left running by a previous worker"
            );
        }
        let ready = self.metadata.find_next_ready_repair_task_per_set(now).await?;
        let Some(task) = self.take_turn(&ready, now) else {
            return Ok(None);
        };

        self.repair_task(
            task.repair_id,
            metadata_coordinator,
            shard_reader,
            shard_writer,
            frontier,
        )
        .await
        .map(Some)
    }

    /// Repair one queued task by ID.
    pub async fn repair_task<M, R, W, F>(
        &self,
        repair_id: Uuid,
        metadata_coordinator: &M,
        shard_reader: &R,
        shard_writer: &W,
        frontier: &F,
    ) -> Result<EcRepairReport, EcRepairError>
    where
        M: EcRepairMetadataCoordinator,
        R: EcRepairShardReader,
        W: EcRepairShardWriter,
        F: EcRepairFrontierUpdater,
    {
        let mut task = self
            .metadata
            .get_repair_task(repair_id)
            .await?
            .ok_or(EcRepairError::RepairTaskNotFound { repair_id })?;
        task = self.recover_task_if_stale_running(task).await?;
        ensure_task_can_run(&task)?;

        let manifest = match self.resolve_manifest(&task, metadata_coordinator).await {
            Ok(manifest) => manifest,
            Err(error) => {
                self.mark_task_failed(task, &error).await?;
                return Err(error);
            }
        };
        let chunk = match manifest_chunk(&manifest, task.chunk_index) {
            Ok(chunk) => chunk.clone(),
            Err(error) => {
                self.mark_task_failed(task, &error).await?;
                return Err(error);
            }
        };
        let expected = match manifest_shard(&chunk, task.slot_index) {
            Ok(expected) => expected.clone(),
            Err(error) => {
                self.mark_task_failed(task, &error).await?;
                return Err(error);
            }
        };
        if expected.node_id != task.node_id {
            let error = EcRepairError::ManifestMismatch {
                reason: format!(
                    "task targets node {} but manifest assigns slot {} to {}",
                    task.node_id, task.slot_index, expected.node_id
                ),
            };
            self.mark_task_failed(task, &error).await?;
            return Err(error);
        }

        let planner = match RepairPlanner::new(&manifest.profile) {
            Ok(planner) => planner,
            Err(error) => {
                let error = EcRepairError::Codec(error);
                self.mark_task_failed(task, &error).await?;
                return Err(error);
            }
        };
        // The plan is made before anything is read, from the loss the task
        // names; it is what the limits are charged for up front. A source
        // found lost on the way widens it (`fetch_sources`).
        let first = planner.plan(task.slot_index, &[], |slot| {
            self.distance_to_slot(&chunk, slot)
        });
        let Some(first) = first else {
            let error = EcRepairError::Unrecoverable {
                reason: planner.explain_unrecoverable(task.slot_index, &[]),
                temporary: false,
            };
            self.mark_task_failed(task, &error).await?;
            return Err(error);
        };
        let zone = task.failure_domain.clone().or_else(|| self.owner_zone(task.node_id));
        let reserved_sources = self.source_traffic(&chunk, &first.sources, zone.as_deref());
        let mut write = RepairTraffic::default();
        write.add(
            &self.topology,
            zone.as_deref(),
            self.topology.local_node_id(),
            expected.node_id,
            expected.size,
        );

        let now = unix_time_nanos();
        let _permit = match self.throttles.try_acquire(
            &task,
            zone.as_deref(),
            reserved_sources.plus(write),
            now,
        ) {
            Ok(permit) => permit,
            Err(error) => {
                task.next_retry_at = Some(now.saturating_add(self.config().throttle_retry_nanos()));
                task.updated_at = now;
                self.metadata.put_repair_task(&task).await?;
                return Err(error);
            }
        };

        tracing::debug!(
            repair_id = %task.repair_id,
            chunk_index = task.chunk_index,
            slot_index = task.slot_index,
            strategy = first.strategy.as_str(),
            sources = ?first.sources,
            why = %planner.explain_choice(task.slot_index, &first, &[]),
            "EC repair planned"
        );

        task.status = EcRepairStatus::Running;
        task.attempts = task.attempts.saturating_add(1);
        task.updated_at = now;
        self.metadata.put_repair_task(&task).await?;

        match self
            .repair_running_task(
                task.clone(),
                EcRepairPlan {
                    manifest,
                    chunk,
                    expected,
                    planner,
                    first,
                    zone,
                    reserved_sources,
                },
                shard_reader,
                shard_writer,
                frontier,
            )
            .await
        {
            Ok(report) => Ok(report),
            Err(error) => {
                self.mark_task_failed(task, &error).await?;
                Err(error)
            }
        }
    }

    async fn mark_task_failed(
        &self,
        mut task: EcRepairTaskRecord,
        error: &EcRepairError,
    ) -> Result<(), EcRepairError> {
        task.status = failure_status_for_error(error);
        task.reason = error.to_string();
        task.updated_at = unix_time_nanos();
        let retry_attempt = task.attempts.max(1);
        task.next_retry_at =
            (task.status == EcRepairStatus::FailedRetryable)
                .then_some(task.updated_at.saturating_add(
                    self.config().retry_delay_nanos(retry_attempt, task.repair_id),
                ));
        self.metadata.put_repair_task(&task).await?;
        ec_metrics::record_repair_failure(task.erasure_set_id);
        Ok(())
    }

    async fn recover_task_if_stale_running(
        &self,
        mut task: EcRepairTaskRecord,
    ) -> Result<EcRepairTaskRecord, EcRepairError> {
        let now = unix_time_nanos();
        if task.status == EcRepairStatus::Running
            && now.saturating_sub(task.updated_at) >= self.config().running_timeout_nanos()
        {
            task.status = EcRepairStatus::FailedRetryable;
            task.reason = "repair worker lease expired; stale running task recovered".to_string();
            task.updated_at = now;
            task.next_retry_at = Some(now);
            self.metadata.put_repair_task(&task).await?;
        }
        Ok(task)
    }

    /// Evaluate whether a returning node may use incremental EC repair.
    pub fn evaluate_rejoin(
        &self,
        last_seen_at: u64,
        now: u64,
        admin_approved: bool,
    ) -> EcNodeRejoinDecision {
        if now <= last_seen_at {
            return EcNodeRejoinDecision::IncrementalRepairAllowed;
        }
        let max_downtime = days_to_nanos(self.config().max_rejoin_downtime_days);
        if now.saturating_sub(last_seen_at) <= max_downtime {
            return EcNodeRejoinDecision::IncrementalRepairAllowed;
        }

        match self.config().long_offline_rejoin_policy {
            EcLongOfflineRejoinPolicy::NeedsBootstrap => EcNodeRejoinDecision::NeedsBootstrap,
            EcLongOfflineRejoinPolicy::RequireAdminApproval if admin_approved => {
                EcNodeRejoinDecision::IncrementalRepairAllowed
            }
            EcLongOfflineRejoinPolicy::RequireAdminApproval => {
                EcNodeRejoinDecision::AdminApprovalRequired
            }
        }
    }

    async fn resolve_manifest<M>(
        &self,
        task: &EcRepairTaskRecord,
        metadata_coordinator: &M,
    ) -> Result<EcObjectManifest, EcRepairError>
    where
        M: EcRepairMetadataCoordinator,
    {
        if let Some(local) = self.metadata.get_manifest(&task.object).await? {
            if manifest_matches_task(&local, task) {
                ensure_manifest_repairable(&local)?;
                return Ok(local);
            }
        }

        let quorum_result = metadata_coordinator.quorum_read_manifest(&task.object).await?;
        match &quorum_result {
            EcManifestReadResult::Present(manifest) | EcManifestReadResult::Tombstone(manifest) => {
                self.metadata.put_manifest(manifest).await?;
            }
            EcManifestReadResult::Absent => {}
        }

        match quorum_result {
            EcManifestReadResult::Present(manifest) => {
                if !manifest_matches_task(&manifest, task) {
                    return Err(EcRepairError::ManifestMismatch {
                        reason: "quorum manifest does not match repair task identity".to_string(),
                    });
                }
                ensure_manifest_repairable(&manifest)?;
                Ok(manifest)
            }
            EcManifestReadResult::Tombstone(_) => Err(EcRepairError::Deleted {
                bucket: task.object.bucket.clone(),
                key: task.object.key.clone(),
                version_id: task.object.version_id.clone(),
            }),
            EcManifestReadResult::Absent => Err(EcRepairError::NotFound {
                bucket: task.object.bucket.clone(),
                key: task.object.key.clone(),
                version_id: task.object.version_id.clone(),
            }),
        }
    }

    async fn repair_running_task<R, W, F>(
        &self,
        mut task: EcRepairTaskRecord,
        plan: EcRepairPlan,
        shard_reader: &R,
        shard_writer: &W,
        frontier: &F,
    ) -> Result<EcRepairReport, EcRepairError>
    where
        R: EcRepairShardReader,
        W: EcRepairShardWriter,
        F: EcRepairFrontierUpdater,
    {
        let fetched = self.fetch_sources(&task, &plan, shard_reader).await?;
        // A widened plan read sources the reservation did not cover. The bytes
        // are already moved, so they are charged rather than asked for: the
        // limits then hold the next repairs back until they are paid off.
        self.throttles.charge(
            plan.zone.as_deref(),
            fetched.traffic.beyond(plan.reserved_sources),
            unix_time_nanos(),
        )?;

        let profile = plan.manifest.profile.clone();
        let shard_candidates = fetched.shards;
        let missing_slot = plan.expected.slot_index;
        // How this shard is about to be rebuilt, read off the shards the fetch
        // actually produced. `unified_solver` here means no EC v1 node would
        // have rebuilt it at all, and that is worth saying out loud
        // (decision EC2-103, section 5).
        let recovery_path = plan.planner.repair_path(
            missing_slot,
            &shard_candidates.iter().map(Option::is_none).collect::<Vec<_>>(),
        );
        let repaired = self
            .codec_limiter
            .run_decode_or_repair(move || {
                let codec = crate::ec_codec::build_codec(profile)?;
                codec.repair_shard(&shard_candidates, missing_slot)
            })
            .await?;
        verify_repaired_shard(&plan.expected, &repaired)?;

        let ack = shard_writer
            .write_repaired_shard(EcRepairShardWriteRequest {
                object: plan.manifest.object_key(),
                repair_id: task.repair_id,
                operation_id: task.operation_id,
                erasure_set_id: task.erasure_set_id,
                chunk_index: task.chunk_index,
                expected: plan.expected.clone(),
                data: repaired.data.clone(),
                kind: task.kind,
            })
            .await?;
        let mut transfers = fetched.transfers;
        self.note_transfer(
            &mut transfers,
            RepairLeg::Write,
            self.topology.local_node_id(),
            plan.expected.node_id,
            repaired.data.len() as u64,
        );
        verify_write_ack(&plan.expected, &ack)?;

        let repaired_at = unix_time_nanos();
        frontier
            .mark_shard_repaired(EcRepairFrontierUpdate {
                object: plan.manifest.object_key(),
                repair_id: task.repair_id,
                operation_id: task.operation_id,
                erasure_set_id: task.erasure_set_id,
                chunk_index: task.chunk_index,
                slot_index: task.slot_index,
                node_id: task.node_id,
                manifest_hlc: plan.manifest.hlc,
                repaired_at,
            })
            .await?;

        task.status = EcRepairStatus::Succeeded;
        task.reason = format!(
            "repair completed by {} from {} source shard(s){}",
            fetched.strategy,
            fetched.sources.len(),
            unified_solver_note(recovery_path)
        );
        task.updated_at = repaired_at;
        task.next_retry_at = None;
        self.metadata.put_repair_task(&task).await?;
        ec_metrics::record_repair_success(task.erasure_set_id);
        tracing::info!(
            repair_id = %task.repair_id,
            bucket = %task.object.bucket,
            key = %task.object.key,
            chunk_index = task.chunk_index,
            slot_index = task.slot_index,
            strategy = fetched.strategy.as_str(),
            why = %fetched.why,
            sources = ?fetched.sources,
            source_bytes = transfers.on_leg(RepairLeg::Source),
            cross_zone_bytes = transfers.cross_zone(),
            recovery_path = recovery_path.as_str(),
            "EC shard repaired"
        );
        if recovery_path == RecoveryPath::UnifiedSolver {
            ec_metrics::record_unified_solver_recovery(
                task.erasure_set_id,
                ec_metrics::UnifiedSolverOperation::Repair,
            );
        }

        Ok(EcRepairReport {
            task,
            strategy: fetched.strategy,
            verified_source_shards: fetched.verified,
            skipped_source_shards: fetched.skipped,
            repaired_bytes: plan.expected.size,
            transfers,
            committed: true,
        })
    }

    /// Read the sources of `plan`, widening it one source at a time while any
    /// of them turns out to be lost.
    ///
    /// Shards already read are kept: the next plan counts them as free and
    /// uses them first. The loop ends because every round either completes the
    /// plan or marks at least one more slot lost. When no plan is left, the
    /// error says whether a source that could come back — one out of reach, as
    /// opposed to one that answered missing or corrupt — was among the losses:
    /// that decides whether the task waits or is closed (EC2-085).
    async fn fetch_sources<R>(
        &self,
        task: &EcRepairTaskRecord,
        plan: &EcRepairPlan,
        shard_reader: &R,
    ) -> Result<FetchedSources, EcRepairError>
    where
        R: EcRepairShardReader,
    {
        let total = plan.planner.total_shards();
        let mut states = vec![SlotState::Unread; total];
        let mut shards: Vec<Option<Shard>> = vec![None; total];
        if let Some(state) = states.get_mut(usize::from(task.slot_index)) {
            *state = SlotState::Lost;
        }
        let mut current = plan.first.clone();
        let mut fetched = FetchedSources {
            strategy: current.strategy,
            why: String::new(),
            sources: Vec::new(),
            shards: Vec::new(),
            verified: 0,
            skipped: 0,
            traffic: RepairTraffic::default(),
            transfers: RepairBytes::default(),
        };
        let mut unreachable = 0usize;

        loop {
            for source in current.sources.iter().copied() {
                let index = usize::from(source);
                if states.get(index) != Some(&SlotState::Unread) {
                    continue;
                }
                let expected = manifest_shard(&plan.chunk, source)?;
                let (outcome, received) =
                    read_repair_source(&plan.manifest, &plan.chunk, expected, shard_reader).await?;
                if received > 0 {
                    let local = self.topology.local_node_id();
                    fetched.traffic.add(
                        &self.topology,
                        plan.zone.as_deref(),
                        expected.node_id,
                        local,
                        received,
                    );
                    self.note_transfer(
                        &mut fetched.transfers,
                        RepairLeg::Source,
                        expected.node_id,
                        local,
                        received,
                    );
                }
                match outcome {
                    SourceOutcome::Verified(shard) => {
                        states[index] = SlotState::Fetched;
                        shards[index] = Some(shard);
                        fetched.verified = fetched.verified.saturating_add(1);
                    }
                    SourceOutcome::Lost { reachable } => {
                        states[index] = SlotState::Lost;
                        fetched.skipped = fetched.skipped.saturating_add(1);
                        unreachable = unreachable.saturating_add(usize::from(!reachable));
                    }
                }
            }

            if current
                .sources
                .iter()
                .all(|source| states.get(usize::from(*source)) == Some(&SlotState::Fetched))
            {
                break;
            }
            let next = plan.planner.plan(task.slot_index, &states, |slot| {
                self.distance_to_slot(&plan.chunk, slot)
            });
            let Some(next) = next else {
                let reason = plan.planner.explain_unrecoverable(task.slot_index, &states);
                tracing::warn!(
                    repair_id = %task.repair_id,
                    chunk_index = task.chunk_index,
                    slot_index = task.slot_index,
                    unreachable_sources = unreachable,
                    reason = %reason,
                    "EC shard cannot be rebuilt from the shards left"
                );
                return Err(EcRepairError::Unrecoverable {
                    reason,
                    temporary: unreachable > 0,
                });
            };
            tracing::info!(
                repair_id = %task.repair_id,
                chunk_index = task.chunk_index,
                slot_index = task.slot_index,
                from = current.strategy.as_str(),
                to = next.strategy.as_str(),
                sources = ?next.sources,
                why = %plan.planner.explain_choice(task.slot_index, &next, &states),
                "EC repair source lost; plan widened"
            );
            current = next;
        }

        fetched.strategy = current.strategy;
        fetched.why = plan.planner.explain_choice(task.slot_index, &current, &states);
        // The codec gets exactly the planned sources, so the path it takes is
        // the one the plan chose and the log reports.
        fetched.shards = (0..total)
            .map(|index| {
                let planned = current.sources.iter().any(|source| usize::from(*source) == index);
                if planned {
                    shards[index].take()
                } else {
                    None
                }
            })
            .collect();
        fetched.sources = current.sources;
        Ok(fetched)
    }
}

#[async_trait]
impl EcReadRepairQueue for EcRepairCoordinator {
    async fn enqueue(&self, task: EcReadRepairTask) -> Result<(), EcReadError> {
        self.enqueue_repair(EcRepairQueueRequest::from_read_repair(task))
            .await
            .map(|_| ())
            .map_err(|error| EcReadError::ShardIo {
                reason: error.to_string(),
            })
    }
}

#[derive(Clone)]
struct EcRepairThrottles {
    config: Arc<EcRepairConfig>,
    global: Arc<Semaphore>,
    per_node: Arc<Mutex<HashMap<NodeId, Arc<Semaphore>>>>,
    per_set: Arc<Mutex<HashMap<u64, Arc<Semaphore>>>>,
    per_failure_domain: Arc<Mutex<HashMap<String, Arc<Semaphore>>>>,
    bandwidth: Arc<Mutex<EcRepairBandwidthWindows>>,
}

impl EcRepairThrottles {
    fn new(config: EcRepairConfig) -> Self {
        Self {
            global: Arc::new(Semaphore::new(config.max_global_concurrency)),
            config: Arc::new(config),
            per_node: Arc::new(Mutex::new(HashMap::new())),
            per_set: Arc::new(Mutex::new(HashMap::new())),
            per_failure_domain: Arc::new(Mutex::new(HashMap::new())),
            bandwidth: Arc::new(Mutex::new(EcRepairBandwidthWindows::default())),
        }
    }

    fn config(&self) -> &EcRepairConfig {
        &self.config
    }

    /// Admit one repair that plans to move `traffic`, its owner in `zone`.
    ///
    /// The zone's limits — concurrency and bandwidth alike — only apply to a
    /// repair that moves bytes across the zone's boundary: they guard the
    /// zone's links, and a repair that stays inside the zone does not touch
    /// them. In a pool of one zone nothing crosses, so neither holds a repair
    /// back there.
    fn try_acquire(
        &self,
        task: &EcRepairTaskRecord,
        zone: Option<&str>,
        traffic: RepairTraffic,
        now: u64,
    ) -> Result<EcRepairPermit, EcRepairError> {
        let global = self.global.clone().try_acquire_owned().map_err(|_| {
            throttled(
                EcRepairLimit::GlobalConcurrency,
                "global EC repair concurrency is saturated".to_string(),
            )
        })?;
        let node = semaphore_for(
            &self.per_node,
            task.node_id,
            self.config.max_node_concurrency,
        )?
        .try_acquire_owned()
        .map_err(|_| {
            throttled(
                EcRepairLimit::NodeConcurrency,
                format!(
                    "EC repair concurrency for node {} is saturated",
                    task.node_id
                ),
            )
        })?;
        let erasure_set = semaphore_for(
            &self.per_set,
            task.erasure_set_id,
            self.config.max_erasure_set_concurrency,
        )?
        .try_acquire_owned()
        .map_err(|_| {
            throttled(
                EcRepairLimit::SetConcurrency,
                format!(
                    "EC repair concurrency for erasure set {} is saturated",
                    task.erasure_set_id
                ),
            )
        })?;
        let crossing_zone = zone.filter(|_| traffic.zone_bytes > 0);
        let failure_domain = match crossing_zone {
            Some(domain) => Some(
                semaphore_for(
                    &self.per_failure_domain,
                    domain.to_string(),
                    self.config.max_failure_domain_concurrency,
                )?
                .try_acquire_owned()
                .map_err(|_| {
                    throttled(
                        EcRepairLimit::FailureDomainConcurrency,
                        format!("EC repair concurrency for failure domain '{domain}' is saturated"),
                    )
                })?,
            ),
            None => None,
        };

        self.bandwidth()?.reserve(traffic, zone, now, &self.config)?;

        Ok(EcRepairPermit {
            _global: global,
            _node: node,
            _erasure_set: erasure_set,
            _failure_domain: failure_domain,
        })
    }

    /// Count `traffic` a repair has already moved beyond what it reserved.
    fn charge(
        &self,
        zone: Option<&str>,
        traffic: RepairTraffic,
        now: u64,
    ) -> Result<(), EcRepairError> {
        self.bandwidth()?.charge(traffic, zone, now, &self.config);
        Ok(())
    }

    fn bandwidth(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, EcRepairBandwidthWindows>, EcRepairError> {
        self.bandwidth.lock().map_err(|_| EcRepairError::Throttled {
            reason: "EC repair bandwidth limiter is poisoned".to_string(),
        })
    }
}

/// One repair refused by `limit`, counted and explained.
///
/// Both halves happen here so they cannot drift apart: a refusal that does not
/// reach the counter is a rebuild whose pacing no one can account for.
fn throttled(limit: EcRepairLimit, reason: String) -> EcRepairError {
    ec_metrics::record_repair_throttled(limit);
    EcRepairError::Throttled { reason }
}

struct EcRepairPermit {
    _global: OwnedSemaphorePermit,
    _node: OwnedSemaphorePermit,
    _erasure_set: OwnedSemaphorePermit,
    _failure_domain: Option<OwnedSemaphorePermit>,
}

/// Per-second repair bandwidth budgets.
///
/// Deliberately not split per erasure set. A failure domain is a physical
/// thing — a zone, a rack — and one zone may hold nodes of two sets; giving
/// each set its own budget for that zone would let them together push twice
/// what the operator allowed through it. Sets are kept apart by taking turns in
/// the queue and by the per-set concurrency limit, not by dividing a physical
/// budget.
#[derive(Default)]
struct EcRepairBandwidthWindows {
    global: ByteBudget,
    zones: HashMap<String, ByteBudget>,
}

impl EcRepairBandwidthWindows {
    /// Admit `traffic` if both limits have room for it, and spend it.
    ///
    /// The zone limit only sees the bytes that cross the zone; a repair with
    /// none is admitted whatever the zone has spent.
    fn reserve(
        &mut self,
        traffic: RepairTraffic,
        zone: Option<&str>,
        now: u64,
        config: &EcRepairConfig,
    ) -> Result<(), EcRepairError> {
        let second = now / 1_000_000_000;
        if let Some(limit) = config.global_bandwidth_bytes_per_sec {
            self.global.roll(second, limit);
            if !self.global.admits(traffic.total_bytes, limit) {
                return Err(throttled(
                    EcRepairLimit::GlobalBandwidth,
                    format!("global EC repair bandwidth budget {limit} B/s would be exceeded"),
                ));
            }
        }
        if let (Some(zone), Some(limit)) = (zone, config.failure_domain_bandwidth_bytes_per_sec) {
            if traffic.zone_bytes > 0 {
                let budget = self.zones.entry(zone.to_string()).or_default();
                budget.roll(second, limit);
                if !budget.admits(traffic.zone_bytes, limit) {
                    return Err(throttled(
                        EcRepairLimit::FailureDomainBandwidth,
                        format!(
                            "EC repair bandwidth budget {limit} B/s for failure domain '{zone}' \
                             would be exceeded"
                        ),
                    ));
                }
            }
        }
        self.charge(traffic, zone, now, config);
        Ok(())
    }

    /// Spend `traffic` without asking: bytes already moved.
    fn charge(
        &mut self,
        traffic: RepairTraffic,
        zone: Option<&str>,
        now: u64,
        config: &EcRepairConfig,
    ) {
        let second = now / 1_000_000_000;
        if let Some(limit) = config.global_bandwidth_bytes_per_sec {
            self.global.roll(second, limit);
            self.global.spend(traffic.total_bytes);
        }
        if let (Some(zone), Some(limit)) = (zone, config.failure_domain_bandwidth_bytes_per_sec) {
            if traffic.zone_bytes > 0 {
                let budget = self.zones.entry(zone.to_string()).or_default();
                budget.roll(second, limit);
                budget.spend(traffic.zone_bytes);
            }
        }
    }
}

/// Bytes spent against one per-second limit.
///
/// Spending more than a second's allowance is possible — one repair may move
/// more than the limit, and bytes of a widened plan are charged after the fact
/// — and the excess is carried into the following seconds rather than
/// forgotten at the next one. Admission waits until the carried bytes are paid
/// off, so over time the limit holds on average. A repair larger than the
/// whole allowance is admitted when nothing is outstanding: refusing it would
/// leave it waiting forever for a second with room it can never have.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct ByteBudget {
    second: u64,
    used: u64,
}

impl ByteBudget {
    /// Pay off `limit` bytes for every second since the budget last moved.
    fn roll(&mut self, second: u64, limit: u64) {
        if second > self.second {
            let elapsed = second - self.second;
            self.used = self.used.saturating_sub(limit.saturating_mul(elapsed));
            self.second = second;
        }
    }

    fn admits(&self, bytes: u64, limit: u64) -> bool {
        self.used == 0 || self.used.saturating_add(bytes) <= limit
    }

    fn spend(&mut self, bytes: u64) {
        self.used = self.used.saturating_add(bytes);
    }
}

struct EcRepairPlan {
    manifest: EcObjectManifest,
    chunk: ChunkManifest,
    expected: ShardManifest,
    planner: RepairPlanner,
    /// The plan made before reading anything.
    first: RepairSourcePlan,
    /// Zone of the shard's owner: the key of the zone limits.
    zone: Option<String>,
    /// What reading `first` was reserved as against the limits.
    reserved_sources: RepairTraffic,
}

/// What reading a plan's sources produced.
struct FetchedSources {
    strategy: RepairStrategy,
    /// Why that strategy, for the log.
    why: String,
    /// The slots the shard is rebuilt from.
    sources: Vec<u8>,
    /// The shards of `sources`, at their slot indexes; `None` everywhere else.
    shards: Vec<Option<Shard>>,
    verified: usize,
    skipped: usize,
    /// Source bytes as the limits count them.
    traffic: RepairTraffic,
    /// Source bytes as the metric counts them.
    transfers: RepairBytes,
}

/// What one repair source turned out to be.
enum SourceOutcome {
    /// Its bytes match the manifest.
    Verified(Shard),
    /// Unusable. `reachable` is false when the node could not be asked at all
    /// — the one kind of loss that may be gone by the next attempt.
    Lost { reachable: bool },
}

/// Read one source shard, and tell how many bytes arrived.
///
/// Bytes that arrive and fail verification were still moved, so they count.
async fn read_repair_source<R>(
    manifest: &EcObjectManifest,
    chunk: &ChunkManifest,
    expected: &ShardManifest,
    shard_reader: &R,
) -> Result<(SourceOutcome, u64), EcRepairError>
where
    R: EcRepairShardReader,
{
    let response = match shard_reader
        .read_repair_shard(EcRepairShardReadRequest {
            object: manifest.object_key(),
            operation_id: manifest.operation_id,
            chunk_index: chunk.chunk_index,
            slot_index: expected.slot_index,
            node_id: expected.node_id,
        })
        .await
    {
        Ok(Some(response)) => response,
        Ok(None) => {
            tracing::warn!(
                node_id = %expected.node_id,
                slot_index = expected.slot_index,
                "EC repair source shard is missing"
            );
            return Ok((SourceOutcome::Lost { reachable: true }, 0));
        }
        Err(error) => {
            let reachable = !matches!(error, EcRepairError::SourceUnavailable { .. });
            tracing::warn!(
                error = %error,
                node_id = %expected.node_id,
                slot_index = expected.slot_index,
                reachable,
                "skipping EC repair source shard"
            );
            return Ok((SourceOutcome::Lost { reachable }, 0));
        }
    };
    let received = response.data.len() as u64;

    if let Some(replica_crc32) = response.replica_crc32 {
        if replica_crc32 != expected.crc32 {
            tracing::debug!(
                node_id = %expected.node_id,
                chunk_index = chunk.chunk_index,
                slot_index = expected.slot_index,
                replica_crc32,
                manifest_crc32 = expected.crc32,
                "replica-reported EC repair source CRC32 differs from manifest"
            );
        }
    }

    if let Err(error) = expected.verify_bytes(&response.data) {
        tracing::warn!(
            error = %error,
            node_id = %expected.node_id,
            slot_index = expected.slot_index,
            "skipping corrupt EC repair source shard"
        );
        return Ok((SourceOutcome::Lost { reachable: true }, received));
    }

    let shard = Shard::new(
        expected.slot_index,
        chunk.chunk_index,
        expected.role,
        response.data,
    )?;
    Ok((SourceOutcome::Verified(shard), received))
}

fn ensure_task_can_run(task: &EcRepairTaskRecord) -> Result<(), EcRepairError> {
    match task.status {
        EcRepairStatus::Queued | EcRepairStatus::FailedRetryable => Ok(()),
        status => Err(EcRepairError::RepairTaskNotRunnable {
            repair_id: task.repair_id,
            status,
        }),
    }
}

fn manifest_matches_task(manifest: &EcObjectManifest, task: &EcRepairTaskRecord) -> bool {
    manifest.object_key() == task.object
        && manifest.operation_id == task.operation_id
        && manifest.erasure_set_id == task.erasure_set_id
}

fn ensure_manifest_repairable(manifest: &EcObjectManifest) -> Result<(), EcRepairError> {
    manifest.validate()?;
    if manifest.is_deleted || manifest.state == EcPlacementState::Tombstone {
        return Err(EcRepairError::Deleted {
            bucket: manifest.bucket.clone(),
            key: manifest.key.clone(),
            version_id: manifest.version_id.clone(),
        });
    }
    if !manifest.state.is_ec_authoritative() {
        return Err(EcRepairError::InvalidManifest {
            reason: format!(
                "manifest state {:?} is not repairable by EC repair path",
                manifest.state
            ),
        });
    }
    Ok(())
}

fn manifest_chunk(
    manifest: &EcObjectManifest,
    chunk_index: u32,
) -> Result<&ChunkManifest, EcRepairError> {
    manifest
        .chunks
        .iter()
        .find(|chunk| chunk.chunk_index == chunk_index)
        .ok_or_else(|| EcRepairError::InvalidManifest {
            reason: format!("missing chunk {chunk_index} in manifest"),
        })
}

fn manifest_shard(chunk: &ChunkManifest, slot_index: u8) -> Result<&ShardManifest, EcRepairError> {
    chunk.shards.iter().find(|shard| shard.slot_index == slot_index).ok_or_else(|| {
        EcRepairError::InvalidManifest {
            reason: format!(
                "missing slot {slot_index} in chunk {} manifest",
                chunk.chunk_index
            ),
        }
    })
}

/// What a completed repair says about the unified solver, and nothing when
/// there is nothing to say.
///
/// The reason of a finished task is what an operator reads first, so a shard
/// that came back only because of the full recovery path says so there — next
/// to the strategy and the source count, rather than in a separate place
/// nobody thinks to look (decision EC2-103, section 5).
fn unified_solver_note(path: RecoveryPath) -> &'static str {
    match path {
        RecoveryPath::UnifiedSolver => ", through the unified LRC solver",
        _ => "",
    }
}

fn verify_repaired_shard(expected: &ShardManifest, repaired: &Shard) -> Result<(), EcRepairError> {
    if repaired.slot_index != expected.slot_index {
        return Err(EcRepairError::ShardVerification {
            reason: format!(
                "codec repaired slot {} but manifest expected slot {}",
                repaired.slot_index, expected.slot_index
            ),
        });
    }
    if repaired.role != expected.role {
        return Err(EcRepairError::ShardVerification {
            reason: format!(
                "codec repaired role {:?} but manifest expected {:?}",
                repaired.role, expected.role
            ),
        });
    }
    expected.verify_bytes(&repaired.data)?;
    Ok(())
}

fn verify_write_ack(
    expected: &ShardManifest,
    ack: &EcRepairShardWriteAck,
) -> Result<(), EcRepairError> {
    if ack.node_id != expected.node_id {
        return Err(EcRepairError::ShardWrite {
            reason: format!(
                "writer ack came from node {}, expected {}",
                ack.node_id, expected.node_id
            ),
        });
    }
    if !ack.durable {
        return Err(EcRepairError::ShardWrite {
            reason: "writer did not acknowledge durable shard bytes".to_string(),
        });
    }
    if !ack.local_index_updated {
        return Err(EcRepairError::ShardWrite {
            reason: "writer did not acknowledge local ec_shards index update".to_string(),
        });
    }
    if ack.size != expected.size || ack.crc32 != expected.crc32 || ack.sha256 != expected.sha256 {
        return Err(EcRepairError::ShardWrite {
            reason: "writer ack integrity fields do not match manifest".to_string(),
        });
    }
    Ok(())
}

fn failure_status_for_error(error: &EcRepairError) -> EcRepairStatus {
    match error {
        EcRepairError::NotFound { .. }
        | EcRepairError::Deleted { .. }
        | EcRepairError::ManifestMismatch { .. }
        | EcRepairError::InvalidManifest { .. } => EcRepairStatus::FailedPermanent,
        // Not rebuildable with a source out of reach: the source may be back
        // by the next attempt, so the task waits with backoff (EC2-085).
        EcRepairError::Unrecoverable {
            temporary: true, ..
        } => EcRepairStatus::FailedRetryable,
        // Every source that failed answered — missing or corrupt. Nothing
        // left in the set brings the shard back; an operator can queue it
        // again once something has changed.
        EcRepairError::Unrecoverable {
            temporary: false, ..
        } => EcRepairStatus::FailedPermanent,
        // The plan promised the codec could repair from what it was given, so
        // this is a disagreement between the two, not a lost source; retrying
        // would only fail the same way.
        EcRepairError::Codec(CodecError::NotRecoverable { .. })
        | EcRepairError::Cpu(EcCpuError::Codec(CodecError::NotRecoverable { .. })) => {
            EcRepairStatus::FailedPermanent
        }
        _ => EcRepairStatus::FailedRetryable,
    }
}

/// Whether a new request for a shard replaces the finished task `existing`.
///
/// A repair that succeeded does not stop the next one: the shard can be lost
/// again, and a request after the repair is a new loss. A task closed as
/// beyond repair is reopened only by an operator — nothing an automatic path
/// sees has changed, and reopening it on every read or scrub would repeat the
/// same futile attempt. Everything still live is left alone: its worker is on
/// it already.
fn supersedes(kind: EcRepairKind, existing: &EcRepairTaskRecord) -> bool {
    match existing.status {
        EcRepairStatus::Succeeded => true,
        EcRepairStatus::FailedPermanent => kind == EcRepairKind::ManualAdmin,
        EcRepairStatus::Queued | EcRepairStatus::Running | EcRepairStatus::FailedRetryable => false,
    }
}

fn deterministic_repair_id(request: &EcRepairQueueRequest) -> Uuid {
    let mut hasher = Sha256::new();
    hasher.update(request.object.bucket.as_bytes());
    hasher.update([0]);
    hasher.update(request.object.key.as_bytes());
    hasher.update([0]);
    if let Some(version_id) = &request.object.version_id {
        hasher.update(version_id.as_bytes());
    }
    hasher.update([0]);
    hasher.update(request.operation_id.as_u128().to_be_bytes());
    hasher.update(request.erasure_set_id.to_be_bytes());
    hasher.update(request.chunk_index.to_be_bytes());
    hasher.update([request.slot_index]);
    hasher.update(request.node_id.0.to_be_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    Uuid::from_u128(u128::from_be_bytes(bytes))
}

fn semaphore_for<K>(
    map: &Arc<Mutex<HashMap<K, Arc<Semaphore>>>>,
    key: K,
    permits: usize,
) -> Result<Arc<Semaphore>, EcRepairError>
where
    K: Eq + std::hash::Hash,
{
    let mut guard = map.lock().map_err(|_| EcRepairError::Throttled {
        reason: "EC repair semaphore map is poisoned".to_string(),
    })?;
    Ok(guard.entry(key).or_insert_with(|| Arc::new(Semaphore::new(permits))).clone())
}

fn unix_time_nanos() -> u64 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    nanos.min(u128::from(u64::MAX)) as u64
}

fn seconds_to_nanos(seconds: u64) -> u64 {
    seconds.saturating_mul(1_000_000_000)
}

fn days_to_nanos(days: u64) -> u64 {
    seconds_to_nanos(days.saturating_mul(86_400))
}

fn deterministic_jitter_secs(repair_id: Uuid, attempt: u32, max: u64) -> u64 {
    if max <= 1 {
        return 0;
    }
    let mut bytes = [0u8; 20];
    bytes[..16].copy_from_slice(repair_id.as_bytes());
    bytes[16..].copy_from_slice(&attempt.to_be_bytes());
    let digest = Sha256::new().chain_update(bytes).finalize();
    let mut value = [0u8; 8];
    value.copy_from_slice(&digest[..8]);
    u64::from_be_bytes(value) % max.min(60)
}

fn parse_optional_usize(name: &'static str) -> Result<Option<usize>, EcRepairError> {
    match std::env::var(name) {
        Ok(raw) => raw.parse::<usize>().map(Some).map_err(|error| EcRepairError::InvalidConfig {
            name,
            reason: error.to_string(),
        }),
        Err(_) => Ok(None),
    }
}

fn parse_optional_u64(name: &'static str) -> Result<Option<u64>, EcRepairError> {
    match std::env::var(name) {
        Ok(raw) => raw.parse::<u64>().map(Some).map_err(|error| EcRepairError::InvalidConfig {
            name,
            reason: error.to_string(),
        }),
        Err(_) => Ok(None),
    }
}

/// Errors returned by the EC repair path.
#[derive(Debug, Error)]
pub enum EcRepairError {
    /// Invalid environment configuration.
    #[error("invalid {name}: {reason}")]
    InvalidConfig {
        /// Environment variable name.
        name: &'static str,
        /// Human-readable validation failure.
        reason: String,
    },

    /// Repair task fields are invalid.
    #[error("invalid EC repair task: {reason}")]
    InvalidTask {
        /// Human-readable validation failure.
        reason: String,
    },

    /// Durable repair queue is full.
    #[error("EC repair queue is full at {limit} task(s)")]
    QueueFull {
        /// Configured queue limit.
        limit: usize,
    },

    /// Repair task was not found.
    #[error("EC repair task {repair_id} was not found")]
    RepairTaskNotFound {
        /// Repair task ID.
        repair_id: Uuid,
    },

    /// Repair task is not runnable in its current status.
    #[error("EC repair task {repair_id} is not runnable in status {status:?}")]
    RepairTaskNotRunnable {
        /// Repair task ID.
        repair_id: Uuid,
        /// Current status.
        status: EcRepairStatus,
    },

    /// Repair throttling rejected the attempt.
    #[error("EC repair throttled: {reason}")]
    Throttled {
        /// Human-readable reason.
        reason: String,
    },

    /// The object/version was quorum-confirmed absent.
    #[error("EC repair object not found: {bucket}/{key} version {version_id:?}")]
    NotFound {
        /// Bucket name.
        bucket: String,
        /// Object key.
        key: String,
        /// Version ID.
        version_id: Option<String>,
    },

    /// The object/version was quorum-confirmed deleted.
    #[error("EC repair object is deleted: {bucket}/{key} version {version_id:?}")]
    Deleted {
        /// Bucket name.
        bucket: String,
        /// Object key.
        key: String,
        /// Version ID.
        version_id: Option<String>,
    },

    /// Manifest metadata did not match the repair task.
    #[error("EC repair manifest mismatch: {reason}")]
    ManifestMismatch {
        /// Human-readable reason.
        reason: String,
    },

    /// Manifest metadata is internally inconsistent for repair.
    #[error("invalid EC repair manifest: {reason}")]
    InvalidManifest {
        /// Human-readable reason.
        reason: String,
    },

    /// Repaired shard verification failed.
    #[error("EC repaired shard verification failed: {reason}")]
    ShardVerification {
        /// Human-readable reason.
        reason: String,
    },

    /// Repaired shard write failed or returned an invalid acknowledgement.
    #[error("EC repaired shard write failed: {reason}")]
    ShardWrite {
        /// Human-readable reason.
        reason: String,
    },

    /// Repair source shard read failed.
    #[error("EC repair shard read failed: {reason}")]
    ShardRead {
        /// Human-readable reason.
        reason: String,
    },

    /// A repair source could not be asked at all: its node did not answer.
    ///
    /// Kept apart from [`Self::ShardRead`], where a node answered with an
    /// error, because only this kind of loss can be gone by the next attempt.
    #[error("EC repair source is out of reach: {reason}")]
    SourceUnavailable {
        /// Human-readable reason.
        reason: String,
    },

    /// The shards still usable cannot rebuild the shard.
    #[error("EC shard cannot be rebuilt now: {reason}")]
    Unrecoverable {
        /// Which shards are lost, and whether a whole local group is.
        reason: String,
        /// Whether a source out of reach was among the losses, so that a
        /// later attempt may succeed.
        temporary: bool,
    },

    /// Repair frontier update failed.
    #[error("EC repair frontier update failed: {reason}")]
    Frontier {
        /// Human-readable reason.
        reason: String,
    },

    /// EC metadata operation failed.
    #[error(transparent)]
    Metadata(#[from] EcMetadataError),

    /// Codec operation failed.
    #[error(transparent)]
    Codec(#[from] CodecError),

    /// CPU limiter operation failed.
    #[error(transparent)]
    Cpu(#[from] EcCpuError),
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::{HashMap, HashSet};
    use std::num::NonZeroUsize;
    use std::sync::atomic::{AtomicBool, Ordering};

    use crate::codec_runtime::{EcCodecCpuConfig, EcCodecCpuLimiter};
    use crate::ec_metadata::{ChunkManifest, EC_MANIFEST_FORMAT_VERSION};
    use crate::erasure_coding::TopologyLabels;
    use s4_codec::EcProfileSnapshot;

    fn node(index: u8) -> NodeId {
        NodeId(u128::from(index))
    }

    fn hlc() -> HlcTimestamp {
        HlcTimestamp {
            wall_time: 10,
            logical: 2,
            node_id: 1,
        }
    }

    fn operation_id() -> Uuid {
        Uuid::from_u128(0xabc)
    }

    fn object() -> EcObjectVersionKey {
        EcObjectVersionKey::new("photos", "raw.bin", Some("v1".to_string()))
    }

    /// A coordinator on node 1 of a pool without topology labels.
    fn coordinator(store: Arc<EcMetadataStore>, config: EcRepairConfig) -> EcRepairCoordinator {
        coordinator_on(store, config, PoolTopologyView::unlabeled(node(1)))
    }

    fn coordinator_on(
        store: Arc<EcMetadataStore>,
        config: EcRepairConfig,
        topology: PoolTopologyView,
    ) -> EcRepairCoordinator {
        let cpu = EcCodecCpuLimiter::new(EcCodecCpuConfig {
            workers: NonZeroUsize::new(1).unwrap(),
            cpu_budget_percent: 60,
            max_inflight_encode_bytes: 1024 * 1024,
        });
        EcRepairCoordinator::new(store, cpu, config, topology)
    }

    /// The view of node `local` over the fixture manifests' nodes — the node
    /// of slot `i` is `i + 1` — with each node in the zone `zone_of` names
    /// and a rack of its own.
    fn zoned_view(local: u8, width: u8, zone_of: impl Fn(u8) -> &'static str) -> PoolTopologyView {
        let nodes = (1..=width).map(node).collect::<Vec<_>>();
        let labels = (1..=width)
            .map(|index| {
                (
                    node(index),
                    TopologyLabels {
                        zone: Some(zone_of(index).to_string()),
                        rack: Some(format!("r{index}")),
                        host: Some(format!("h{index}")),
                        disk_group: Some("d0".to_string()),
                    },
                )
            })
            .collect::<HashMap<_, _>>();
        PoolTopologyView::build(node(local), &nodes, &labels)
    }

    fn encoded_manifest(
        mut profile: EcProfileSnapshot,
        data: &[u8],
    ) -> (EcObjectManifest, HashMap<(u32, u8), Bytes>) {
        profile.chunk_size = data.len() as u64;
        let codec = crate::ec_codec::build_codec(profile.clone()).unwrap();
        let encoded = codec.encode_chunk(data, 0).unwrap();
        let mut shard_bytes = HashMap::new();
        let shards = encoded
            .shards
            .iter()
            .map(|shard| {
                shard_bytes.insert((0, shard.slot_index), shard.data.clone());
                ShardManifest {
                    slot_index: shard.slot_index,
                    node_id: node(shard.slot_index + 1),
                    role: shard.role,
                    crc32: shard.crc32,
                    sha256: shard.sha256,
                    size: shard.size,
                }
            })
            .collect();
        let manifest = EcObjectManifest {
            format_version: EC_MANIFEST_FORMAT_VERSION,
            bucket: "photos".to_string(),
            key: "raw.bin".to_string(),
            version_id: Some("v1".to_string()),
            original_size: data.len() as u64,
            content_hash_sha256: sha256(data),
            etag: "\"etag\"".to_string(),
            content_type: "application/octet-stream".to_string(),
            user_metadata: HashMap::new(),
            profile,
            erasure_set_id: 9,
            topology_epoch: 3,
            chunks: vec![ChunkManifest {
                chunk_index: 0,
                data_size: encoded.original_size,
                encoded_size: encoded.shard_size,
                chunk_sha256: sha256(data),
                shards,
            }],
            state: EcPlacementState::EcCommitted,
            hlc: hlc(),
            origin_node_id: node(1),
            operation_id: operation_id(),
            created_at: 42,
            is_deleted: false,
            packed: None,
        };
        manifest.validate().unwrap();
        (manifest, shard_bytes)
    }

    fn sha256(bytes: &[u8]) -> [u8; 32] {
        Sha256::new().chain_update(bytes).finalize().into()
    }

    #[derive(Default)]
    struct MemoryMetadata {
        manifest: Mutex<Option<EcManifestReadResult>>,
    }

    #[async_trait]
    impl EcRepairMetadataCoordinator for MemoryMetadata {
        async fn quorum_read_manifest(
            &self,
            _object: &EcObjectVersionKey,
        ) -> Result<EcManifestReadResult, EcRepairError> {
            Ok(self.manifest.lock().unwrap().clone().unwrap_or(EcManifestReadResult::Absent))
        }
    }

    #[derive(Default)]
    struct MemoryShards {
        shards: Mutex<HashMap<(u32, u8), Bytes>>,
        missing: Mutex<HashSet<(u32, u8)>>,
        /// Slots whose node does not answer at all.
        unreachable: Mutex<HashSet<(u32, u8)>>,
        /// Slots whose node answers with bytes that are not the shard.
        corrupt: Mutex<HashSet<(u32, u8)>>,
        /// Every slot asked for, in order.
        reads: Mutex<Vec<u8>>,
        writes: Mutex<HashMap<(u32, u8), Bytes>>,
        fail_write: AtomicBool,
    }

    impl MemoryShards {
        fn new(shards: HashMap<(u32, u8), Bytes>) -> Self {
            Self {
                shards: Mutex::new(shards),
                ..Self::default()
            }
        }

        fn missing_slots(&self, slots: &[(u32, u8)]) {
            self.missing.lock().unwrap().extend(slots.iter().copied());
        }

        fn unreachable_slots(&self, slots: &[(u32, u8)]) {
            self.unreachable.lock().unwrap().extend(slots.iter().copied());
        }

        fn corrupt_slots(&self, slots: &[(u32, u8)]) {
            self.corrupt.lock().unwrap().extend(slots.iter().copied());
        }

        fn slots_read(&self) -> Vec<u8> {
            self.reads.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl EcRepairShardReader for MemoryShards {
        async fn read_repair_shard(
            &self,
            request: EcRepairShardReadRequest,
        ) -> Result<Option<EcRepairShardReadResponse>, EcRepairError> {
            let at = (request.chunk_index, request.slot_index);
            self.reads.lock().unwrap().push(request.slot_index);
            if self.unreachable.lock().unwrap().contains(&at) {
                return Err(EcRepairError::SourceUnavailable {
                    reason: format!("node {} did not answer", request.node_id),
                });
            }
            if self.missing.lock().unwrap().contains(&at) {
                return Ok(None);
            }
            if self.corrupt.lock().unwrap().contains(&at) {
                let mut data = self.shards.lock().unwrap().get(&at).unwrap().to_vec();
                data[0] ^= 0xff;
                return Ok(Some(EcRepairShardReadResponse {
                    replica_crc32: None,
                    data: Bytes::from(data),
                }));
            }
            let data = self
                .shards
                .lock()
                .unwrap()
                .get(&(request.chunk_index, request.slot_index))
                .cloned();
            Ok(data.map(|data| EcRepairShardReadResponse {
                replica_crc32: Some(crc32fast::hash(&data)),
                data,
            }))
        }
    }

    #[async_trait]
    impl EcRepairShardWriter for MemoryShards {
        async fn write_repaired_shard(
            &self,
            request: EcRepairShardWriteRequest,
        ) -> Result<EcRepairShardWriteAck, EcRepairError> {
            if self.fail_write.load(Ordering::SeqCst) {
                return Err(EcRepairError::ShardWrite {
                    reason: "test writer failure".to_string(),
                });
            }
            self.writes.lock().unwrap().insert(
                (request.chunk_index, request.expected.slot_index),
                request.data.clone(),
            );
            Ok(EcRepairShardWriteAck {
                node_id: request.expected.node_id,
                size: request.expected.size,
                crc32: crc32fast::hash(&request.data),
                sha256: sha256(&request.data),
                durable: true,
                local_index_updated: true,
            })
        }
    }

    #[derive(Default)]
    struct MemoryFrontier {
        updates: Mutex<Vec<EcRepairFrontierUpdate>>,
    }

    #[async_trait]
    impl EcRepairFrontierUpdater for MemoryFrontier {
        async fn mark_shard_repaired(
            &self,
            update: EcRepairFrontierUpdate,
        ) -> Result<(), EcRepairError> {
            self.updates.lock().unwrap().push(update);
            Ok(())
        }
    }

    async fn install_manifest(metadata: &MemoryMetadata, manifest: &EcObjectManifest) {
        *metadata.manifest.lock().unwrap() = Some(EcManifestReadResult::Present(manifest.clone()));
    }

    async fn queue_missing(
        coordinator: &EcRepairCoordinator,
        manifest: &EcObjectManifest,
        slot_index: u8,
    ) -> EcRepairTaskRecord {
        let shard = manifest_shard(&manifest.chunks[0], slot_index).unwrap();
        coordinator
            .enqueue_repair(EcRepairQueueRequest {
                object: manifest.object_key(),
                operation_id: manifest.operation_id,
                erasure_set_id: manifest.erasure_set_id,
                chunk_index: 0,
                slot_index,
                node_id: shard.node_id,
                kind: EcRepairKind::MissingShard,
                reason: "test missing shard".to_string(),
                failure_domain: None,
            })
            .await
            .unwrap()
    }

    fn repair_task_record(
        manifest: &EcObjectManifest,
        slot_index: u8,
        repair_id: Uuid,
        status: EcRepairStatus,
        enqueued_at: u64,
        next_retry_at: Option<u64>,
    ) -> EcRepairTaskRecord {
        let shard = manifest_shard(&manifest.chunks[0], slot_index).unwrap();
        EcRepairTaskRecord {
            repair_id,
            object: manifest.object_key(),
            operation_id: manifest.operation_id,
            erasure_set_id: manifest.erasure_set_id,
            chunk_index: 0,
            slot_index,
            node_id: shard.node_id,
            kind: EcRepairKind::MissingShard,
            status,
            attempts: 0,
            reason: "test repair task".to_string(),
            enqueued_at,
            updated_at: enqueued_at,
            next_retry_at,
            failure_domain: None,
        }
    }

    #[tokio::test]
    async fn rs_missing_shard_repair_writes_expected_bytes() {
        let data = (0..128).map(|idx| (idx % 251) as u8).collect::<Vec<_>>();
        let (manifest, shard_bytes) = encoded_manifest(EcProfileSnapshot::rs_small(), &data);
        let dir = tempfile::tempdir().unwrap();
        let local = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let coordinator = coordinator(local, EcRepairConfig::default());
        let metadata = MemoryMetadata::default();
        install_manifest(&metadata, &manifest).await;
        let shards = MemoryShards::new(shard_bytes.clone());
        shards.missing_slots(&[(0, 1)]);
        let frontier = MemoryFrontier::default();
        let task = queue_missing(&coordinator, &manifest, 1).await;

        let report = coordinator
            .repair_task(task.repair_id, &metadata, &shards, &shards, &frontier)
            .await
            .unwrap();

        assert_eq!(report.task.status, EcRepairStatus::Succeeded);
        assert_eq!(
            shards.writes.lock().unwrap().get(&(0, 1)).unwrap(),
            shard_bytes.get(&(0, 1)).unwrap()
        );
        assert_eq!(frontier.updates.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn lrc_single_data_shard_repair_uses_local_or_global_path() {
        let data = (0..180).map(|idx| ((idx * 17) % 251) as u8).collect::<Vec<_>>();
        let (manifest, shard_bytes) = encoded_manifest(EcProfileSnapshot::lrc_small(), &data);
        let dir = tempfile::tempdir().unwrap();
        let local = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let coordinator = coordinator(local, EcRepairConfig::default());
        let metadata = MemoryMetadata::default();
        install_manifest(&metadata, &manifest).await;
        let shards = MemoryShards::new(shard_bytes.clone());
        shards.missing_slots(&[(0, 0)]);
        let frontier = MemoryFrontier::default();
        let task = queue_missing(&coordinator, &manifest, 0).await;

        coordinator
            .repair_task(task.repair_id, &metadata, &shards, &shards, &frontier)
            .await
            .unwrap();

        assert_eq!(
            shards.writes.lock().unwrap().get(&(0, 0)).unwrap(),
            shard_bytes.get(&(0, 0)).unwrap()
        );
    }

    #[tokio::test]
    async fn read_repair_queue_impl_persists_durable_task() {
        let dir = tempfile::tempdir().unwrap();
        let local = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let coordinator = coordinator_on(
            local.clone(),
            EcRepairConfig::default(),
            zoned_view(1, 6, |index| if index <= 3 { "a" } else { "b" }),
        );
        let task = EcReadRepairTask {
            object: object(),
            operation_id: operation_id(),
            erasure_set_id: 9,
            chunk_index: 0,
            slot_index: 2,
            node_id: node(3),
            kind: EcReadRepairKind::CorruptShard,
            reason: "manifest SHA-256 mismatch".to_string(),
        };

        EcReadRepairQueue::enqueue(&coordinator, task).await.unwrap();

        let tasks = local.list_repair_tasks(10).await.unwrap();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].kind, EcRepairKind::CorruptShard);
        // The read path names no failure domain; the coordinator fills in the
        // zone of the shard's owner, node 3.
        assert_eq!(tasks[0].failure_domain.as_deref(), Some("a"));
    }

    #[tokio::test]
    async fn missing_manifest_marks_repair_permanent_in_queue() {
        let dir = tempfile::tempdir().unwrap();
        let local = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let coordinator = coordinator(local.clone(), EcRepairConfig::default());
        let (manifest, shard_bytes) =
            encoded_manifest(EcProfileSnapshot::rs_small(), b"permanent-miss-test-data");
        let metadata = MemoryMetadata::default();
        let shards = MemoryShards::new(shard_bytes);
        let frontier = MemoryFrontier::default();
        let task = queue_missing(&coordinator, &manifest, 0).await;

        let err = coordinator
            .repair_task(task.repair_id, &metadata, &shards, &shards, &frontier)
            .await
            .unwrap_err();

        assert!(matches!(err, EcRepairError::NotFound { .. }));
        let stored = local.get_repair_task(task.repair_id).await.unwrap().unwrap();
        assert_eq!(stored.status, EcRepairStatus::FailedPermanent);
    }

    #[tokio::test]
    async fn repair_next_scans_past_terminal_and_not_due_records() {
        let data = (0..128).map(|idx| (idx % 251) as u8).collect::<Vec<_>>();
        let (manifest, shard_bytes) = encoded_manifest(EcProfileSnapshot::rs_small(), &data);
        let dir = tempfile::tempdir().unwrap();
        let local = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let config = EcRepairConfig {
            repair_scan_limit: 1,
            ..EcRepairConfig::default()
        };
        let coordinator = coordinator(local.clone(), config);
        let metadata = MemoryMetadata::default();
        install_manifest(&metadata, &manifest).await;
        let shards = MemoryShards::new(shard_bytes);
        shards.missing_slots(&[(0, 0)]);
        let frontier = MemoryFrontier::default();

        let terminal = repair_task_record(
            &manifest,
            1,
            Uuid::from_u128(1),
            EcRepairStatus::Succeeded,
            1,
            None,
        );
        let not_due = repair_task_record(
            &manifest,
            2,
            Uuid::from_u128(2),
            EcRepairStatus::Queued,
            2,
            Some(u64::MAX),
        );
        let ready_id = Uuid::from_u128(u128::MAX);
        let ready = repair_task_record(&manifest, 0, ready_id, EcRepairStatus::Queued, 3, None);
        local.put_repair_task(&terminal).await.unwrap();
        local.put_repair_task(&not_due).await.unwrap();
        local.put_repair_task(&ready).await.unwrap();

        let report = coordinator
            .repair_next(&metadata, &shards, &shards, &frontier)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(report.task.repair_id, ready_id);
        assert_eq!(report.task.status, EcRepairStatus::Succeeded);
    }

    /// A long backlog in one set must not hold the worker away from another.
    ///
    /// The durable queue is one keyspace shared by every set of the pool, and
    /// the oldest task always wins inside a set. Without taking turns, five
    /// older tasks of one set would be served before the newer task of the other
    /// one ever ran — which is the pool-wide behaviour multi-set pools have to
    /// stop.
    #[tokio::test]
    async fn repair_takes_turns_between_erasure_sets() {
        let dir = tempfile::tempdir().unwrap();
        let local = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let coordinator = coordinator(local.clone(), EcRepairConfig::default());
        let (manifest, _) = encoded_manifest(EcProfileSnapshot::rs_small(), b"turns");

        // The backlog: five tasks of the inherited set, all older than the one
        // task waiting in the second set.
        for index in 0..5u128 {
            let mut task = repair_task_record(
                &manifest,
                0,
                Uuid::from_u128(index + 1),
                EcRepairStatus::Queued,
                1 + index as u64,
                None,
            );
            task.erasure_set_id = 9;
            local.put_repair_task(&task).await.unwrap();
        }
        let mut newcomer = repair_task_record(
            &manifest,
            1,
            Uuid::from_u128(100),
            EcRepairStatus::Queued,
            1_000,
            None,
        );
        newcomer.erasure_set_id = 1;
        local.put_repair_task(&newcomer).await.unwrap();

        let ready = local.find_next_ready_repair_task_per_set(2_000).await.unwrap();
        assert_eq!(ready.len(), 2, "one candidate per set was expected");

        // Four picks in a row alternate between the two sets rather than
        // draining the backlog first. The rotation runs in set-id order and
        // starts before the first id, so set 1 opens it — which set opens does
        // not matter, the alternation does.
        let served: Vec<u64> = (0..4)
            .map(|_| coordinator.take_turn(&ready, 2_000).unwrap().erasure_set_id)
            .collect();
        assert_eq!(served, vec![1, 9, 1, 9]);

        // Inside a set the oldest task still wins.
        assert_eq!(ready[&9].most_urgent.repair_id, Uuid::from_u128(1));
    }

    /// The guard the priority order needs: a task nobody is in a hurry to
    /// repair is still repaired, however long the stream of urgent ones lasts.
    ///
    /// One old task of a chunk missing a single shard waits while every
    /// selection brings another chunk missing two. Written as a bound — the old
    /// task is served no later than the guard's own turn — because "eventually"
    /// cannot be told apart from starvation on a finite run (decision EC2-162).
    #[tokio::test]
    async fn repair_priority_bounds_the_wait_of_a_passed_over_task() {
        const EVERY: usize = 4;

        let dir = tempfile::tempdir().unwrap();
        let local = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let config = EcRepairConfig {
            priority_age_boost_secs: 60,
            priority_age_boost_every: EVERY,
            ..EcRepairConfig::default()
        };
        let coordinator = coordinator(local.clone(), config);
        let (manifest, _) = encoded_manifest(EcProfileSnapshot::rs_small(), b"starvation");
        let now = seconds_to_nanos(600);

        let mut old = repair_task_record(
            &manifest,
            5,
            Uuid::from_u128(1),
            EcRepairStatus::Queued,
            0,
            None,
        );
        old.chunk_index = 1_000;
        local.put_repair_task(&old).await.unwrap();

        let mut served = Vec::new();
        for round in 0..EVERY as u128 {
            // Every turn is preceded by a fresh chunk missing two shards, so
            // the urgency order always has something newer to prefer.
            for slot in 0..2u8 {
                let mut urgent = repair_task_record(
                    &manifest,
                    slot,
                    Uuid::from_u128(100 + round * 10 + u128::from(slot)),
                    EcRepairStatus::Queued,
                    now - 1,
                    None,
                );
                urgent.chunk_index = round as u32;
                local.put_repair_task(&urgent).await.unwrap();
            }

            let ready = local.find_next_ready_repair_task_per_set(now).await.unwrap();
            let chosen = coordinator.take_turn(&ready, now).unwrap().clone();
            served.push(chosen.repair_id);

            // A served task is claimed by the worker: still a missing shard of
            // its chunk, no longer something the next turn may pick up.
            let mut claimed = chosen;
            claimed.status = EcRepairStatus::Running;
            claimed.updated_at = now;
            local.put_repair_task(&claimed).await.unwrap();
        }

        assert_eq!(
            served.iter().position(|repair_id| *repair_id == old.repair_id),
            Some(EVERY - 1),
            "the task passed over by urgency is served on the guard's turn, \
             not left to the stream of urgent ones: {served:?}"
        );
    }

    /// One set, one chunk missing two shards and one older task missing a
    /// single shard, with the guard's turn every `every` turns.
    async fn queue_with_a_passed_over_task(
        local: &EcMetadataStore,
        manifest: &EcObjectManifest,
        erasure_set_id: u64,
        now: u64,
        old_enqueued_at: u64,
    ) -> EcRepairTaskRecord {
        let mut old = repair_task_record(
            manifest,
            5,
            Uuid::from_u128(u128::from(erasure_set_id) * 1_000 + 1),
            EcRepairStatus::Queued,
            old_enqueued_at,
            None,
        );
        old.erasure_set_id = erasure_set_id;
        old.chunk_index = 1_000;
        local.put_repair_task(&old).await.unwrap();

        for slot in 0..2u8 {
            let mut urgent = repair_task_record(
                manifest,
                slot,
                Uuid::from_u128(u128::from(erasure_set_id) * 1_000 + 100 + u128::from(slot)),
                EcRepairStatus::Queued,
                now - 1,
                None,
            );
            urgent.erasure_set_id = erasure_set_id;
            urgent.chunk_index = 0;
            local.put_repair_task(&urgent).await.unwrap();
        }
        old
    }

    /// In a healthy queue the guard has nothing to serve: nobody has waited
    /// long enough, its turn comes and goes, and the endangered chunk keeps it.
    #[tokio::test]
    async fn repair_priority_age_boost_waits_for_the_threshold() {
        let dir = tempfile::tempdir().unwrap();
        let local = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let config = EcRepairConfig {
            priority_age_boost_secs: 600,
            priority_age_boost_every: 2,
            ..EcRepairConfig::default()
        };
        let coordinator = coordinator(local.clone(), config);
        let (manifest, _) = encoded_manifest(EcProfileSnapshot::rs_small(), b"threshold");
        let now = seconds_to_nanos(600);

        // Queued a minute ago against a threshold of ten: waiting, but not
        // starving.
        let old =
            queue_with_a_passed_over_task(&local, &manifest, 9, now, now - seconds_to_nanos(60))
                .await;

        let ready = local.find_next_ready_repair_task_per_set(now).await.unwrap();
        assert_eq!(ready[&9].longest_waiting.repair_id, old.repair_id);
        for turn in 1..=4 {
            assert_ne!(
                coordinator.take_turn(&ready, now).unwrap().repair_id,
                old.repair_id,
                "turn {turn} went to a task that has not waited its threshold"
            );
        }
    }

    /// The rollback lever of decision EC2-162: with the threshold at zero the
    /// order is urgency alone, exactly as it was before the guard existed.
    #[tokio::test]
    async fn repair_priority_age_boost_switches_off_at_zero() {
        let dir = tempfile::tempdir().unwrap();
        let local = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let config = EcRepairConfig {
            priority_age_boost_secs: 0,
            priority_age_boost_every: 2,
            ..EcRepairConfig::default()
        };
        let coordinator = coordinator(local.clone(), config);
        let (manifest, _) = encoded_manifest(EcProfileSnapshot::rs_small(), b"switched-off");
        let now = seconds_to_nanos(600);

        let old = queue_with_a_passed_over_task(&local, &manifest, 9, now, 0).await;

        let ready = local.find_next_ready_repair_task_per_set(now).await.unwrap();
        for turn in 1..=6 {
            assert_ne!(
                coordinator.take_turn(&ready, now).unwrap().repair_id,
                old.repair_id,
                "turn {turn} was boosted although the guard is switched off"
            );
        }
    }

    /// The guard's turn belongs to the set it was counted for. Two sets, each
    /// with a task urgency keeps passing over: each gets its own guard turn and
    /// neither spends the other's.
    #[tokio::test]
    async fn repair_priority_age_boost_counts_turns_per_set() {
        let dir = tempfile::tempdir().unwrap();
        let local = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let config = EcRepairConfig {
            priority_age_boost_secs: 60,
            priority_age_boost_every: 2,
            ..EcRepairConfig::default()
        };
        let coordinator = coordinator(local.clone(), config);
        let (manifest, _) = encoded_manifest(EcProfileSnapshot::rs_small(), b"two-sets");
        let now = seconds_to_nanos(600);

        let first = queue_with_a_passed_over_task(&local, &manifest, 1, now, 0).await;
        let ninth = queue_with_a_passed_over_task(&local, &manifest, 9, now, 0).await;

        let ready = local.find_next_ready_repair_task_per_set(now).await.unwrap();
        let served: Vec<(u64, bool)> = (0..4)
            .map(|_| {
                let task = coordinator.take_turn(&ready, now).unwrap();
                let passed_over =
                    task.repair_id == first.repair_id || task.repair_id == ninth.repair_id;
                (task.erasure_set_id, passed_over)
            })
            .collect();

        assert_eq!(
            served,
            vec![(1, false), (9, false), (1, true), (9, true)],
            "each set spends its own second turn on the task it kept passing over"
        );
    }

    /// Urgency orders a set's own queue and reaches no further.
    ///
    /// A set holding a chunk one loss away from being gone still yields to its
    /// neighbour after each task. Applied over the pool instead of inside a
    /// set, priority would hand the whole repair budget to the heaviest
    /// incident — which is the behaviour taking turns was built to stop.
    #[tokio::test]
    async fn repair_priority_does_not_take_turns_from_another_set() {
        let dir = tempfile::tempdir().unwrap();
        let local = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let coordinator = coordinator(local.clone(), EcRepairConfig::default());
        let (manifest, _) = encoded_manifest(EcProfileSnapshot::rs_small(), b"priority");

        // Set 9 lost three shards of one chunk, and queued them after the one
        // lost shard of its other chunk.
        for slot in 0..3u8 {
            let mut task = repair_task_record(
                &manifest,
                slot,
                Uuid::from_u128(u128::from(slot) + 1),
                EcRepairStatus::Queued,
                100 + u64::from(slot),
                None,
            );
            task.erasure_set_id = 9;
            task.chunk_index = 0;
            local.put_repair_task(&task).await.unwrap();
        }
        let mut lonely = repair_task_record(
            &manifest,
            3,
            Uuid::from_u128(50),
            EcRepairStatus::Queued,
            10,
            None,
        );
        lonely.erasure_set_id = 9;
        lonely.chunk_index = 1;
        local.put_repair_task(&lonely).await.unwrap();

        // Set 1 waits with a newer task and nothing urgent about it.
        let mut newcomer = repair_task_record(
            &manifest,
            0,
            Uuid::from_u128(100),
            EcRepairStatus::Queued,
            1_000,
            None,
        );
        newcomer.erasure_set_id = 1;
        newcomer.chunk_index = 2;
        local.put_repair_task(&newcomer).await.unwrap();

        let ready = local.find_next_ready_repair_task_per_set(2_000).await.unwrap();
        assert_eq!(ready.len(), 2, "one candidate per set was expected");
        assert_eq!(
            ready[&9].most_urgent.repair_id,
            Uuid::from_u128(1),
            "inside set 9 the damaged chunk goes before the older single loss"
        );
        assert_eq!(ready[&1].most_urgent.repair_id, newcomer.repair_id);

        let served: Vec<u64> = (0..4)
            .map(|_| coordinator.take_turn(&ready, 2_000).unwrap().erasure_set_id)
            .collect();
        assert_eq!(served, vec![1, 9, 1, 9]);
    }

    /// A pool of one set behaves exactly as EC v1 did: the oldest ready task,
    /// every time.
    #[tokio::test]
    async fn one_set_serves_its_oldest_ready_task() {
        let dir = tempfile::tempdir().unwrap();
        let local = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let coordinator = coordinator(local.clone(), EcRepairConfig::default());
        let (manifest, _) = encoded_manifest(EcProfileSnapshot::rs_small(), b"single");

        let older = repair_task_record(
            &manifest,
            0,
            Uuid::from_u128(1),
            EcRepairStatus::Queued,
            5,
            None,
        );
        let newer = repair_task_record(
            &manifest,
            1,
            Uuid::from_u128(2),
            EcRepairStatus::Queued,
            9,
            None,
        );
        local.put_repair_task(&older).await.unwrap();
        local.put_repair_task(&newer).await.unwrap();

        let ready = local.find_next_ready_repair_task_per_set(2_000).await.unwrap();
        assert_eq!(ready.len(), 1);
        for _ in 0..3 {
            assert_eq!(
                coordinator.take_turn(&ready, 2_000).unwrap().repair_id,
                older.repair_id
            );
        }
    }

    #[test]
    fn concurrency_throttle_does_not_consume_bandwidth_budget() {
        let dir = tempfile::tempdir().unwrap();
        let local = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let config = EcRepairConfig {
            max_node_concurrency: 1,
            global_bandwidth_bytes_per_sec: Some(64),
            ..EcRepairConfig::default()
        };
        let coordinator = coordinator(local, config);
        let (manifest, _shard_bytes) =
            encoded_manifest(EcProfileSnapshot::rs_small(), b"bandwidth-test-data");
        let held = repair_task_record(
            &manifest,
            0,
            Uuid::from_u128(10),
            EcRepairStatus::Queued,
            1,
            None,
        );
        let same_node = repair_task_record(
            &manifest,
            0,
            Uuid::from_u128(11),
            EcRepairStatus::Queued,
            2,
            None,
        );
        let other_node = repair_task_record(
            &manifest,
            1,
            Uuid::from_u128(12),
            EcRepairStatus::Queued,
            3,
            None,
        );

        let bytes = |total_bytes| RepairTraffic {
            total_bytes,
            zone_bytes: 0,
        };
        let _held_permit = coordinator.throttles.try_acquire(&held, None, bytes(0), 1).unwrap();
        let err = match coordinator.throttles.try_acquire(&same_node, None, bytes(64), 1) {
            Ok(_) => panic!("same-node repair should be concurrency-throttled"),
            Err(error) => error,
        };
        assert!(matches!(err, EcRepairError::Throttled { .. }));

        let _other_permit =
            coordinator.throttles.try_acquire(&other_node, None, bytes(64), 1).unwrap();
    }

    #[tokio::test]
    async fn repair_queue_depth_rejects_new_unique_tasks() {
        let dir = tempfile::tempdir().unwrap();
        let local = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let config = EcRepairConfig {
            max_queue_depth: 1,
            ..EcRepairConfig::default()
        };
        let coordinator = coordinator(local, config);
        let (manifest, _shards) =
            encoded_manifest(EcProfileSnapshot::rs_small(), b"queue-depth-test-data");
        queue_missing(&coordinator, &manifest, 0).await;

        let err = coordinator
            .enqueue_repair(EcRepairQueueRequest {
                object: manifest.object_key(),
                operation_id: manifest.operation_id,
                erasure_set_id: manifest.erasure_set_id,
                chunk_index: 0,
                slot_index: 1,
                node_id: node(2),
                kind: EcRepairKind::MissingShard,
                reason: "another missing shard".to_string(),
                failure_domain: None,
            })
            .await
            .unwrap_err();

        assert!(matches!(err, EcRepairError::QueueFull { limit: 1 }));
    }

    #[tokio::test]
    async fn long_offline_rejoin_requires_bootstrap_by_default() {
        let dir = tempfile::tempdir().unwrap();
        let local = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let coordinator = coordinator(local, EcRepairConfig::default());
        let now = days_to_nanos(10);

        assert_eq!(
            coordinator.evaluate_rejoin(0, now, false),
            EcNodeRejoinDecision::NeedsBootstrap
        );
    }

    /// One repair of `slot` of a fresh object: the shard is missing, and
    /// `losses` takes away more before the repair starts.
    struct RepairRun {
        result: Result<EcRepairReport, EcRepairError>,
        shards: MemoryShards,
        original: HashMap<(u32, u8), Bytes>,
        stored: EcRepairTaskRecord,
        shard_size: u64,
        _dir: tempfile::TempDir,
    }

    async fn repair_once(
        profile: EcProfileSnapshot,
        slot: u8,
        topology: PoolTopologyView,
        losses: impl FnOnce(&MemoryShards),
    ) -> RepairRun {
        let data = (0..600).map(|idx| ((idx * 31) % 251) as u8).collect::<Vec<_>>();
        let (manifest, original) = encoded_manifest(profile, &data);
        let dir = tempfile::tempdir().unwrap();
        let local = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let coordinator = coordinator_on(local.clone(), EcRepairConfig::default(), topology);
        let metadata = MemoryMetadata::default();
        install_manifest(&metadata, &manifest).await;
        let shards = MemoryShards::new(original.clone());
        shards.missing_slots(&[(0, slot)]);
        losses(&shards);
        let task = queue_missing(&coordinator, &manifest, slot).await;

        let result = coordinator
            .repair_task(
                task.repair_id,
                &metadata,
                &shards,
                &shards,
                &MemoryFrontier::default(),
            )
            .await;

        let stored = local.get_repair_task(task.repair_id).await.unwrap().unwrap();
        RepairRun {
            result,
            shards,
            original,
            stored,
            shard_size: manifest.chunks[0].encoded_size,
            _dir: dir,
        }
    }

    fn slots(list: &[u8]) -> Vec<(u32, u8)> {
        list.iter().map(|slot| (0, *slot)).collect()
    }

    /// EC2-ZONE-03-007: where the losses are decides the strategy, and the
    /// repair reads exactly the shards it names — the rest of the group for
    /// one loss in it, `k` shards otherwise, one source more whenever a
    /// source is gone. The rebuilt bytes are the original ones every time.
    #[tokio::test]
    async fn the_strategy_follows_where_the_losses_are() {
        use RepairStrategy::{Global, LocalGroup};
        // lrc_small: groups {0, 1 | 4} and {2, 3 | 5}, global 6.
        // lrc_standard: groups {0, 1, 2 | 6} and {3, 4, 5 | 7}, globals 8, 9.
        // lrc_dense: groups {0-4 | 10} and {5-9 | 11}, globals 12, 13.
        // rs_small: 4 data, 2 parity.
        /// Profile, slot to repair, other shards absent, expected strategy,
        /// expected slots read in order.
        type Case = (
            EcProfileSnapshot,
            u8,
            &'static [u8],
            RepairStrategy,
            &'static [u8],
        );
        let cases: [Case; 11] = [
            (
                EcProfileSnapshot::lrc_standard(),
                0,
                &[],
                LocalGroup { group: 0 },
                &[1, 2, 6],
            ),
            (
                EcProfileSnapshot::lrc_standard(),
                4,
                &[],
                LocalGroup { group: 1 },
                &[3, 5, 7],
            ),
            (
                EcProfileSnapshot::lrc_standard(),
                6,
                &[],
                LocalGroup { group: 0 },
                &[0, 1, 2],
            ),
            (
                EcProfileSnapshot::lrc_standard(),
                9,
                &[],
                Global,
                &[0, 1, 2, 3, 4, 5],
            ),
            // A loss in the other group changes nothing for this one.
            (
                EcProfileSnapshot::lrc_standard(),
                0,
                &[7],
                LocalGroup { group: 0 },
                &[1, 2, 6],
            ),
            // A second loss in the group: the local attempt stops at it and
            // the repair goes global, keeping slots 2 and 6 it already has.
            // The local parity is one equation of the unified system, so with
            // two holes in the group it still counts towards the `k` and the
            // repair reads one shard fewer than EC v1 did (EC2-101).
            (
                EcProfileSnapshot::lrc_standard(),
                0,
                &[1],
                Global,
                &[1, 2, 6, 3, 4, 5, 8],
            ),
            (
                EcProfileSnapshot::lrc_small(),
                0,
                &[],
                LocalGroup { group: 0 },
                &[1, 4],
            ),
            (
                EcProfileSnapshot::lrc_small(),
                5,
                &[],
                LocalGroup { group: 1 },
                &[2, 3],
            ),
            (
                EcProfileSnapshot::lrc_dense(),
                7,
                &[],
                LocalGroup { group: 1 },
                &[5, 6, 8, 9, 11],
            ),
            (EcProfileSnapshot::rs_small(), 0, &[], Global, &[1, 2, 3, 4]),
            (
                EcProfileSnapshot::rs_small(),
                0,
                &[2],
                Global,
                &[1, 2, 3, 4, 5],
            ),
        ];

        for (profile, slot, absent, strategy, reads) in cases {
            let name = profile.name.clone();
            let run = repair_once(
                profile,
                slot,
                PoolTopologyView::unlabeled(node(1)),
                |shards| {
                    shards.missing_slots(&slots(absent));
                },
            )
            .await;

            let report = run.result.unwrap_or_else(|error| panic!("{name} slot {slot}: {error}"));
            assert_eq!(
                report.strategy, strategy,
                "{name} slot {slot} absent {absent:?}"
            );
            assert_eq!(
                run.shards.slots_read(),
                reads,
                "{name} slot {slot} absent {absent:?}"
            );
            // Only a loss the repair ran into counts: slot 7 in the fifth
            // case is never asked for.
            let ran_into = absent.iter().filter(|lost| reads.contains(lost)).count();
            assert_eq!(report.skipped_source_shards, ran_into, "{name} slot {slot}");
            assert_eq!(
                run.shards.writes.lock().unwrap().get(&(0, slot)),
                run.original.get(&(0, slot)),
                "{name} slot {slot}"
            );
            assert!(
                run.stored.reason.contains(&strategy.to_string()),
                "{name}: the task names its strategy: {}",
                run.stored.reason
            );
        }
    }

    /// A whole local group gone is beyond any strategy. The repair stops as
    /// soon as it knows, without reading the rest of the chunk, closes the
    /// task and names the group — when every lost source answered.
    #[tokio::test]
    async fn a_whole_group_lost_is_reported_and_closed() {
        let run = repair_once(
            EcProfileSnapshot::lrc_standard(),
            0,
            PoolTopologyView::unlabeled(node(1)),
            |shards| shards.missing_slots(&slots(&[1, 2, 6])),
        )
        .await;

        let error = run.result.unwrap_err();
        assert!(
            matches!(
                error,
                EcRepairError::Unrecoverable {
                    temporary: false,
                    ..
                }
            ),
            "{error}"
        );
        assert!(error.to_string().contains("local group(s) [0]"), "{error}");
        assert_eq!(run.shards.slots_read(), vec![1, 2, 6]);
        assert_eq!(run.stored.status, EcRepairStatus::FailedPermanent);
        assert!(run.shards.writes.lock().unwrap().is_empty());
    }

    /// The same losses, but one of the sources is on a node that did not
    /// answer: it may be back next time, so the task waits with backoff
    /// instead of being closed (EC2-085, "Что ещё обнаружено").
    #[tokio::test]
    async fn a_source_out_of_reach_defers_the_task_instead_of_closing_it() {
        let run = repair_once(
            EcProfileSnapshot::lrc_standard(),
            0,
            PoolTopologyView::unlabeled(node(1)),
            |shards| {
                shards.unreachable_slots(&slots(&[1]));
                shards.missing_slots(&slots(&[2, 6]));
            },
        )
        .await;

        let error = run.result.unwrap_err();
        assert!(
            matches!(
                error,
                EcRepairError::Unrecoverable {
                    temporary: true,
                    ..
                }
            ),
            "{error}"
        );
        assert_eq!(run.stored.status, EcRepairStatus::FailedRetryable);
        assert!(run.stored.next_retry_at.is_some());
    }

    /// Bytes that arrive and fail verification were still moved: they count
    /// in the report and the metric, and the source counts as lost.
    #[tokio::test]
    async fn a_corrupt_source_is_lost_but_its_bytes_count() {
        let run = repair_once(
            EcProfileSnapshot::lrc_standard(),
            0,
            PoolTopologyView::unlabeled(node(1)),
            |shards| shards.corrupt_slots(&slots(&[1])),
        )
        .await;

        let report = run.result.unwrap();
        assert_eq!(report.strategy, RepairStrategy::Global);
        assert_eq!(report.skipped_source_shards, 1);
        assert_eq!(report.verified_source_shards, 6);
        let reads = run.shards.slots_read().len() as u64;
        assert_eq!(reads, 7);
        assert_eq!(
            report.transfers.on_leg(RepairLeg::Source),
            reads * run.shard_size
        );
    }

    /// Exit gate of plan 05, phase 3, measured rather than expected: a local
    /// repair of a group lying in one zone moves no byte across zones — three
    /// sources from the group's zone and the write to the owner — while a
    /// global repair of the same chunk from the same node does cross.
    #[tokio::test]
    async fn a_local_repair_inside_one_zone_moves_nothing_across_zones() {
        // Group 0 — slots 0, 1, 2 and 6, nodes 1, 2, 3 and 7 — is in zone
        // "a"; every other node is in zone "b".
        let zone_of = |index: u8| {
            if matches!(index, 1 | 2 | 3 | 7) {
                "a"
            } else {
                "b"
            }
        };

        // Repaired on node 1, the owner of slot 0 — as when anti-entropy finds
        // its own shard missing.
        let local = repair_once(
            EcProfileSnapshot::lrc_standard(),
            0,
            zoned_view(1, 10, zone_of),
            |_| {},
        )
        .await;
        let report = local.result.unwrap();
        let shard = local.shard_size;
        assert_eq!(report.strategy, RepairStrategy::LocalGroup { group: 0 });
        assert_eq!(
            report.transfers.get(RepairLeg::Source, TransferScope::SameZone),
            3 * shard
        );
        assert_eq!(
            report.transfers.get(RepairLeg::Write, TransferScope::Local),
            shard
        );
        assert_eq!(report.transfers.cross_zone(), 0);
        assert_eq!(local.stored.failure_domain.as_deref(), Some("a"));

        // A global parity shard of zone "b" rebuilt from node 1: zone "a" can
        // give only three data shards the decode uses, so three come from
        // "b", and the rebuilt shard goes back to "b".
        let global = repair_once(
            EcProfileSnapshot::lrc_standard(),
            8,
            zoned_view(1, 10, zone_of),
            |_| {},
        )
        .await;
        let report = global.result.unwrap();
        assert_eq!(report.strategy, RepairStrategy::Global);
        assert_eq!(global.shards.slots_read(), vec![0, 1, 2, 3, 4, 5]);
        assert_eq!(
            report.transfers.get(RepairLeg::Source, TransferScope::CrossZone),
            3 * shard
        );
        assert_eq!(
            report.transfers.get(RepairLeg::Write, TransferScope::CrossZone),
            shard
        );
        assert_eq!(global.stored.failure_domain.as_deref(), Some("b"));
    }

    /// Finding 2 of plan 05, phase 0: a shard repaired once was never repaired
    /// again — a second request returned the old `Succeeded` task. A new loss
    /// after a repair now gets a new repair.
    #[tokio::test]
    async fn a_shard_repaired_once_is_repaired_again_when_lost_again() {
        let data = (0..128).map(|idx| (idx % 251) as u8).collect::<Vec<_>>();
        let (manifest, shard_bytes) = encoded_manifest(EcProfileSnapshot::rs_small(), &data);
        let dir = tempfile::tempdir().unwrap();
        let local = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let coordinator = coordinator(local.clone(), EcRepairConfig::default());
        let metadata = MemoryMetadata::default();
        install_manifest(&metadata, &manifest).await;
        let shards = MemoryShards::new(shard_bytes);
        shards.missing_slots(&[(0, 1)]);
        let frontier = MemoryFrontier::default();

        let first = queue_missing(&coordinator, &manifest, 1).await;
        coordinator
            .repair_task(first.repair_id, &metadata, &shards, &shards, &frontier)
            .await
            .unwrap();

        let again = queue_missing(&coordinator, &manifest, 1).await;
        assert_eq!(again.repair_id, first.repair_id);
        assert_eq!(again.status, EcRepairStatus::Queued);
        assert_eq!(again.attempts, 0);
        let report = coordinator
            .repair_task(again.repair_id, &metadata, &shards, &shards, &frontier)
            .await
            .unwrap();
        assert_eq!(report.task.status, EcRepairStatus::Succeeded);
        assert_eq!(frontier.updates.lock().unwrap().len(), 2);
    }

    /// A task still live is never duplicated, and a task closed as beyond
    /// repair is reopened by an operator only: nothing a read or a scrub sees
    /// has changed, and reopening it for them would repeat the same attempt.
    #[tokio::test]
    async fn only_an_operator_reopens_a_task_closed_as_beyond_repair() {
        let dir = tempfile::tempdir().unwrap();
        let local = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let coordinator = coordinator(local.clone(), EcRepairConfig::default());
        let (manifest, _) = encoded_manifest(EcProfileSnapshot::rs_small(), b"beyond repair");
        let queued = queue_missing(&coordinator, &manifest, 0).await;

        // Live: a second request returns the same task untouched.
        let mut running = queued.clone();
        running.status = EcRepairStatus::Running;
        running.attempts = 3;
        local.put_repair_task(&running).await.unwrap();
        assert_eq!(queue_missing(&coordinator, &manifest, 0).await, running);

        let mut closed = queued.clone();
        closed.status = EcRepairStatus::FailedPermanent;
        local.put_repair_task(&closed).await.unwrap();
        assert_eq!(queue_missing(&coordinator, &manifest, 0).await, closed);

        let shard = manifest_shard(&manifest.chunks[0], 0).unwrap();
        let reopened = coordinator
            .queue_admin_repair(EcRepairQueueRequest {
                object: manifest.object_key(),
                operation_id: manifest.operation_id,
                erasure_set_id: manifest.erasure_set_id,
                chunk_index: 0,
                slot_index: 0,
                node_id: shard.node_id,
                kind: EcRepairKind::MissingShard,
                reason: "operator asked".to_string(),
                failure_domain: None,
            })
            .await
            .unwrap();
        // Reopening a closed task is work this call did, so it reports as made
        // rather than found.
        assert!(reopened.created);
        let reopened = reopened.task;
        assert_eq!(reopened.repair_id, queued.repair_id);
        assert_eq!(reopened.status, EcRepairStatus::Queued);
        assert_eq!(reopened.kind, EcRepairKind::ManualAdmin);
    }

    /// The zone limits guard the zone's links. A repair that crosses the zone
    /// is held by them; one that stays inside passes even while the zone's
    /// budget is spent. A repair larger than the whole budget runs when
    /// nothing is outstanding, and what it overspent is paid off over the
    /// following seconds before the next crossing repair is let through.
    #[test]
    fn a_zone_limit_holds_only_repairs_that_cross_the_zone() {
        let dir = tempfile::tempdir().unwrap();
        let local = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let config = EcRepairConfig {
            failure_domain_bandwidth_bytes_per_sec: Some(100),
            ..EcRepairConfig::default()
        };
        let coordinator = coordinator(local, config);
        let (manifest, _) = encoded_manifest(EcProfileSnapshot::rs_small(), b"zone limits");
        let task = |slot| {
            repair_task_record(
                &manifest,
                slot,
                Uuid::from_u128(u128::from(slot) + 1),
                EcRepairStatus::Queued,
                1,
                None,
            )
        };
        let crossing = RepairTraffic {
            total_bytes: 300,
            zone_bytes: 150,
        };
        let inside = RepairTraffic {
            total_bytes: 300,
            zone_bytes: 0,
        };
        let second = 1_000_000_000u64;
        let throttles = &coordinator.throttles;

        let held = throttles.try_acquire(&task(0), Some("a"), crossing, 0).unwrap();
        // One crossing repair at a time per zone.
        assert!(throttles.try_acquire(&task(1), Some("a"), crossing, 0).is_err());
        // A repair that stays inside the zone is not held by it.
        drop(throttles.try_acquire(&task(2), Some("a"), inside, 0).unwrap());
        drop(held);

        // 150 bytes spent against 100 a second: refused now, and a second
        // later with 50 still outstanding...
        let refused = throttles.try_acquire(&task(1), Some("a"), crossing, 0).err().unwrap();
        assert!(
            refused.to_string().contains("failure domain 'a'"),
            "{refused}"
        );
        assert!(throttles.try_acquire(&task(1), Some("a"), crossing, second).is_err());
        // ...admitted once it is paid off.
        drop(throttles.try_acquire(&task(1), Some("a"), crossing, 2 * second).unwrap());
        // Another zone has its own budget.
        drop(throttles.try_acquire(&task(3), Some("b"), crossing, 2 * second).unwrap());
    }

    /// Bytes charged after the fact — the sources of a widened plan — hold
    /// the next repairs back just as reserved ones do.
    #[test]
    fn bytes_charged_after_the_fact_count_against_the_limits() {
        let dir = tempfile::tempdir().unwrap();
        let local = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let config = EcRepairConfig {
            global_bandwidth_bytes_per_sec: Some(100),
            ..EcRepairConfig::default()
        };
        let coordinator = coordinator(local, config);
        let (manifest, _) = encoded_manifest(EcProfileSnapshot::rs_small(), b"charged");
        let task = repair_task_record(
            &manifest,
            0,
            Uuid::from_u128(9),
            EcRepairStatus::Queued,
            1,
            None,
        );
        let small = RepairTraffic {
            total_bytes: 10,
            zone_bytes: 0,
        };

        drop(coordinator.throttles.try_acquire(&task, None, small, 0).unwrap());
        coordinator
            .throttles
            .charge(
                None,
                RepairTraffic {
                    total_bytes: 95,
                    zone_bytes: 0,
                },
                0,
            )
            .unwrap();
        assert!(coordinator.throttles.try_acquire(&task, None, small, 0).is_err());
    }

    /// Rebuilding a whole node is the heaviest repair load a pool ever carries,
    /// and its weight is the number of queued tasks. That number must buy
    /// nothing: the bandwidth budget admits the same bytes a second whether one
    /// shard is waiting or ten thousand (`EC2-NREP-03-003`).
    #[test]
    fn a_whole_node_rebuild_cannot_outrun_the_bandwidth_budget() {
        let dir = tempfile::tempdir().unwrap();
        let local = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let limit = 300u64;
        let config = EcRepairConfig {
            global_bandwidth_bytes_per_sec: Some(limit),
            ..EcRepairConfig::default()
        };
        let coordinator = coordinator(local, config);
        let (manifest, _) = encoded_manifest(EcProfileSnapshot::rs_small(), b"whole node rebuild");
        let repair = RepairTraffic {
            total_bytes: 100,
            zone_bytes: 0,
        };
        let second = 1_000_000_000u64;

        // A queue far deeper than the budget, as a replaced node produces.
        let queued = (0..64u128)
            .map(|index| {
                repair_task_record(
                    &manifest,
                    0,
                    Uuid::from_u128(index + 1),
                    EcRepairStatus::Queued,
                    1,
                    None,
                )
            })
            .collect::<Vec<_>>();

        let mut admitted_bytes = 0u64;
        for now in [0, second, 2 * second] {
            let mut this_second = 0u64;
            for task in &queued {
                // Permits are released at once, so nothing here is held back by
                // concurrency: the bytes are the only thing under test.
                if let Ok(permit) = coordinator.throttles.try_acquire(task, None, repair, now) {
                    drop(permit);
                    this_second = this_second.saturating_add(repair.total_bytes);
                }
            }
            assert!(
                this_second <= limit,
                "a deep queue moved {this_second} B in one second against a {limit} B/s budget"
            );
            admitted_bytes = admitted_bytes.saturating_add(this_second);
        }

        // And the budget is spent rather than merely not exceeded: a rebuild
        // that made no progress would pass the assertion above too.
        assert_eq!(admitted_bytes, 3 * limit);
    }

    /// Every task of a whole-node rebuild names the same shard owner, so the
    /// per-owner concurrency limit is the one that paces it. A queue of any
    /// depth may not put more repairs on that node at once than the operator
    /// allowed.
    #[test]
    fn the_owner_limit_paces_a_rebuild_however_deep_its_queue() {
        let dir = tempfile::tempdir().unwrap();
        let local = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let allowed = 2usize;
        let config = EcRepairConfig {
            max_node_concurrency: allowed,
            // Out of the way: this test is about the owner limit alone.
            max_global_concurrency: 64,
            max_erasure_set_concurrency: 64,
            ..EcRepairConfig::default()
        };
        let coordinator = coordinator(local, config);
        let (manifest, _) = encoded_manifest(EcProfileSnapshot::rs_small(), b"owner limit");
        let traffic = RepairTraffic::default();
        let task = |index: u128| {
            repair_task_record(
                &manifest,
                0,
                Uuid::from_u128(index + 1),
                EcRepairStatus::Queued,
                1,
                None,
            )
        };

        let mut held = Vec::new();
        for index in 0..64u128 {
            if let Ok(permit) = coordinator.throttles.try_acquire(&task(index), None, traffic, 0) {
                held.push(permit);
            }
        }

        assert_eq!(
            held.len(),
            allowed,
            "a deep queue must not put more repairs on one owner than the limit allows"
        );
        // Finishing one lets exactly one more in, which is what makes the limit
        // a pace rather than a ceiling a rebuild stops at.
        drop(held.pop());
        assert!(coordinator.throttles.try_acquire(&task(99), None, traffic, 0).is_ok());
    }

    #[test]
    fn a_byte_budget_carries_what_one_second_overspent() {
        let mut budget = ByteBudget::default();
        budget.roll(10, 100);
        assert!(
            budget.admits(250, 100),
            "an idle budget admits one oversized repair"
        );
        budget.spend(250);
        assert!(!budget.admits(1, 100));
        budget.roll(11, 100);
        assert_eq!(budget.used, 150);
        assert!(!budget.admits(1, 100));
        budget.roll(12, 100);
        assert_eq!(budget.used, 50);
        assert!(budget.admits(50, 100));
        assert!(!budget.admits(51, 100));
        // A clock that steps back pays nothing off.
        budget.roll(5, 100);
        assert_eq!(budget.used, 50);
    }
}
