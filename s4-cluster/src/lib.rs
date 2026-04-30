// Copyright 2026 S4Core Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! S4 Cluster — membership, gossip, quorum operations, and node identity.
//!
//! This crate provides the foundation for S4's distributed mode:
//!
//! - **Node Identity** — persistent UUID-based node IDs
//! - **Cluster Configuration** — declarative pool-based topology
//! - **SWIM Gossip** — failure detection and metadata propagation
//! - **gRPC** — inter-node RPC for data/metadata replication
//! - **Data Placement** — key-to-replica routing via hash ring or full replication
//! - **Quorum Write** — coordinator fans out writes to W replicas (Phase 4)
//! - **Quorum Read** — digest-based quorum read with read repair (Phase 5)
//! - **Distributed LIST** — scatter-gather LIST across pool nodes (Phase 5)
//! - **Read Repair** — async repair of stale replicas on read (Phase 5)
//! - **Anti-Entropy** — background Merkle tree exchange for consistency (Phase 7)
//! - **Merkle Tree** — persistent hash tree for divergence detection (Phase 7)
//! - **Repair Frontier** — per-replica reconciliation markers for GC (Phase 7)
//! - **Distributed GC** — tombstone lifecycle, purge, zombie guard (Phase 8)
//! - **Dedup GC** — mark-sweep GC for per-node dedup entries (Phase 8)
//! - **Replica Handlers** — local write/read/head/list handlers (Phase 4-5)
//! - **Hinted Handoff** — temporary storage for offline replicas (Phase 4)
//! - **HLC** — Hybrid Logical Clock for causal ordering (Phase 4)
//! - **Conflict Resolution** — LWW by HLC for deterministic conflict resolution (Phase 6)
//! - **DVV** — Dotted Version Vectors, prepared for post-MVP (Phase 6)
//! - **Idempotency** — operation deduplication via LRU + journal (Phase 4)
//! - **Data Scrubber** — background CRC32 integrity verification with healing (Phase 9)
//! - **Deep Verify** — SHA-256 content hash verification utility (Phase 9)
//!
//! # Quick Start
//!
//! ```ignore
//! use s4_cluster::{GossipService, GossipConfig, GossipIdentity, NodeId, NodeMeta};
//!
//! let node_id = get_or_create_node_id(data_dir)?;
//! let (handle, events) = GossipService::start(config, identity, meta).await?;
//! ```

pub mod capability;
pub mod clock;
pub mod cluster_metrics;
pub mod config;
pub mod conflict;
pub mod coordinator;
pub mod dedup;
pub mod error;
pub mod gc;
pub mod gossip;
pub mod hints;
pub mod idempotency;
pub mod identity;
pub mod lifecycle;
pub mod placement;
pub mod protocol;
pub mod repair;
pub mod replica;
pub mod rpc;
pub mod scrubber;
pub mod traits;

/// Backward-compatible re-export of HLC types.
///
/// The canonical location is [`clock::hlc`], but existing code may use
/// `crate::hlc::*`. This module alias preserves that path.
pub mod hlc {
    pub use crate::clock::hlc::*;
}

// Re-export key types for convenience.
pub use capability::ClusterLimits;
pub use clock::hlc::{HlcTimestamp, HybridClock};
pub use config::AntiEntropyConfig;
pub use config::{ClusterConfig, PoolConfig};
pub use conflict::{resolve_conflict, ConflictResolution};
pub use coordinator::{
    DistributedListCoordinator, DistributedListResult, ListEntry, ListTimeouts, QuorumHeadResult,
    QuorumMultipartCompleteResult, QuorumMultipartCreateResult, QuorumMultipartPartResult,
    QuorumReadCoordinator, QuorumReadResult, QuorumReadStreamResult, QuorumWriteCoordinator,
    ReadTimeouts, WriteTimeouts,
};
pub use dedup::{DedupGcConfig, DistributedDedupGc, SweepStats};
pub use error::ClusterError;
pub use gc::{
    can_purge_tombstone, DistributedGcService, GcConfig, RejoinDecision, TombstonePolicy,
    ZombieGuard,
};
pub use gossip::{ClusterEvent, GossipConfig, GossipHandle, GossipService, NodeState};
pub use hints::{HintedHandoffConfig, HintedHandoffManager};
pub use idempotency::IdempotencyTracker;
pub use identity::{
    get_or_create_node_id, Edition, GossipIdentity, NodeAddr, NodeCapacity, NodeId, NodeMeta,
    NodeStatus, PoolId,
};
pub use placement::{
    BucketPlacement, ConsistentHashRing, PlacementDecision, PlacementRouter, PlacementStrategy,
};
pub use repair::{
    AntiEntropyMetrics, AntiEntropyService, AntiEntropySnapshot, KeyRange, MerkleTree,
    ReadRepairService, RepairFrontier,
};
pub use replica::{
    MultipartReplicaStorage, ReplicaMultipartHandler, ReplicaReadHandler, ReplicaWriteHandler,
};
pub use rpc::{NodeClient, NodeClientConfig, NodeRpcServer, RpcServerConfig, TlsConfig};
pub use scrubber::{
    deep_verify::{deep_verify_object, VerifyResult},
    metrics::{ScrubberMetrics, ScrubberMetricsSnapshot},
    DataScrubber, HealingContext, ScrubCycleStats, ScrubberConfig,
};
pub use traits::{
    AuditEvent, AuditLogger, ClusterServices, DeepScrubber, NodeReplacer, NoopAuditLogger,
    NoopDeepScrubber, NoopNodeReplacer, NoopRollingUpgrader, RollingUpgrader,
};

// Phase 10 re-exports.
pub use cluster_metrics::{ClusterHealthStatus, HealthLevel, NodeHealthInfo, RepairStatus};
pub use lifecycle::{
    graceful_shutdown, InFlightGuard, NodeLifecycle, DEFAULT_DRAIN_TIMEOUT,
    GOSSIP_PROPAGATION_DELAY,
};
pub use protocol::{
    check_compatibility, is_compatible, MIN_COMPATIBLE_VERSION, PROTOCOL_VERSION, S4_VERSION,
};
