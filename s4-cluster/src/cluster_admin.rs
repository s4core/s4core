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

//! What the cluster routes of the admin API answer, and where it comes from.
//!
//! The routes read the state the metrics are written from: the pool's
//! membership counted by [`PoolRoster::statuses`], and the atomics of
//! anti-entropy, tombstone GC and the scrubber, which are incremented next to
//! the metric each of them mirrors. A route and a scrape of the same node
//! therefore agree; scenario 55 checks that on a live cluster.
//!
//! [`ClusterAdminService`] is injected into the API the way
//! [`crate::EcAdminService`] is: [`NoopClusterAdminService`] by default, which
//! answers as a node that is not part of a cluster, and [`NodeClusterAdmin`]
//! built by cluster bootstrap.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::cluster_metrics::{MemberStatus, PoolRoster};
use crate::gc::service::GcMetrics;
use crate::gossip::GossipHandle;
use crate::identity::{NodeMeta, NodeStatus};
use crate::metadata_placement::{PlacementStatus, QuorumShape};
use crate::protocol::{PROTOCOL_VERSION, S4_VERSION};
use crate::rebalance::PoolGrowth;
use crate::repair::{AntiEntropyMetrics, ConfigAdoptionSnapshot, ConfigAdoptionStatus};
use crate::scrubber::metrics::{ScrubberMetrics, ScrubberMetricsSnapshot};

/// What the cluster routes of the admin API report.
///
/// Every method answers `None` on a node that is not part of a cluster; the
/// route then answers as a single node does.
pub trait ClusterAdminService: Send + Sync + 'static {
    /// The pool's health as this node sees it.
    fn cluster_health(&self) -> Option<ClusterHealthStatus>;

    /// The pool and its members as this node sees them.
    fn topology(&self) -> Option<ClusterTopology>;

    /// This node.
    fn node_health(&self) -> Option<NodeHealthInfo>;

    /// Anti-entropy and tombstone GC on this node.
    fn repair_status(&self) -> Option<RepairStatus>;

    /// The background scrubber of this node.
    fn scrubber_status(&self) -> Option<ScrubberStatus>;

    /// The pool's growth on this node: its state, and the pause of the move
    /// of its keys. `None` on a node that is not part of a cluster.
    fn pool_growth(&self) -> Option<Arc<PoolGrowth>> {
        None
    }
}

/// The service of a node that is not part of a cluster.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopClusterAdminService;

impl ClusterAdminService for NoopClusterAdminService {
    fn cluster_health(&self) -> Option<ClusterHealthStatus> {
        None
    }

    fn topology(&self) -> Option<ClusterTopology> {
        None
    }

    fn node_health(&self) -> Option<NodeHealthInfo> {
        None
    }

    fn repair_status(&self) -> Option<RepairStatus> {
        None
    }

    fn scrubber_status(&self) -> Option<ScrubberStatus> {
        None
    }
}

// ---------------------------------------------------------------------------
// Answers
// ---------------------------------------------------------------------------

/// The pool's health as one node sees it.
///
/// The counts are those of `s4_cluster_nodes` on the same node: every member
/// `S4_POOL_NODES` configures, this node included, and a member this node has
/// never heard from counted as dead.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterHealthStatus {
    /// Overall health, by [`health_level`].
    pub status: HealthLevel,
    /// Configured members of the pool.
    pub nodes_total: u64,
    /// Members alive.
    pub nodes_alive: u64,
    /// Members suspect.
    pub nodes_suspect: u64,
    /// Members dead, or never heard from.
    pub nodes_dead: u64,
    /// Members that left.
    pub nodes_left: u64,
    /// Topology epoch of this node.
    pub epoch: u64,
    /// Protocol version of this node.
    pub protocol_version: u32,
    /// S4 version of this node.
    pub s4_version: String,
    /// The pool.
    pub pool: String,
    /// Copies of each replicated key.
    pub replication_factor: u8,
    /// Replicas a write waits for.
    pub write_quorum: u8,
    /// Replicas a read waits for.
    pub read_quorum: u8,
    /// Where the move of the pool's bucket metadata to more replicas stands
    /// on this node.
    pub bucket_metadata: Option<BucketMetadataHealth>,
    /// This node's progress adopting its own bucket configurations.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bucket_config_adoption: Option<ConfigAdoptionSnapshot>,
    /// Whether IAM changes wait for the pool to take over the IAM some node
    /// kept of its own; such a pool is at least `degraded`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub iam_adoption: Option<IamAdoptionHealth>,
    /// Whether the live nodes of the pool are configured alike for IAM; a
    /// pool where they are not is `critical`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub iam_config: Option<IamConfigHealth>,
    /// Single-to-cluster obligations still awaiting every placement owner.
    #[serde(default)]
    pub migration_pending_records: u64,
}

/// The IAM configuration of the live nodes of the pool compared, as this
/// node sees it (`s4_iam_config_mismatch`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IamConfigHealth {
    /// Whether every live node announces the same root, JWT secret and IAM
    /// encryption key.
    pub consistent: bool,
    /// The settings that differ, with the nodes that differ.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub mismatches: Vec<crate::iam_config::IamConfigMismatch>,
}

/// The pool taking over the IAM its nodes kept of their own, as this node
/// sees it (`s4_iam_adoption_pending`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IamAdoptionHealth {
    /// Whether IAM changes are refused until it is taken over.
    pub pending: bool,
    /// Why, while they are.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// The move of a pool's bucket markers and configurations from the replicas
/// of its data to as many as the pool survives failures for, as this node is
/// at it. A pool with nothing to move is settled from the start, with `from`
/// and `to` its data's shape.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BucketMetadataHealth {
    /// The stage, 0 to 3 (`s4_bucket_metadata_stage`).
    pub stage: u8,
    /// `legacy`, `dual_write`, `read_new` or `settled`.
    pub stage_name: String,
    /// The layout the move starts from.
    pub from: PlacementShape,
    /// The layout it ends at (`s4_bucket_metadata_replication_factor`).
    pub to: PlacementShape,
    /// Whether this node brought what it held to the new replicas.
    pub backfill_complete: bool,
    /// The nodes the next stage waits for, and why; empty once settled.
    pub held_by: Vec<PlacementHoldout>,
    /// How long the next stage has waited, in seconds; `null` when it does
    /// not wait.
    pub held_for_secs: Option<u64>,
}

/// Replicas and quorums of one layout of the bucket metadata.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct PlacementShape {
    /// Replicas of each key.
    pub replication_factor: u8,
    /// Acknowledgements a write needs.
    pub write_quorum: u8,
    /// Answers a read needs.
    pub read_quorum: u8,
}

impl From<QuorumShape> for PlacementShape {
    fn from(shape: QuorumShape) -> Self {
        Self {
            replication_factor: shape.replication_factor,
            write_quorum: shape.write_quorum,
            read_quorum: shape.read_quorum,
        }
    }
}

/// A node the next stage of the move waits for.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PlacementHoldout {
    /// Its name in `S4_POOL_NODES`, or its identity when it has none.
    pub node: String,
    /// Why the gate waits for it.
    pub reason: String,
}

/// High-level health classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HealthLevel {
    /// Every member is alive.
    Healthy,
    /// Some member is not alive, but every key still has enough live replicas
    /// for both quorums.
    Degraded,
    /// Some key may not reach a quorum: writes or reads of it fail.
    Critical,
}

/// The health of a pool where `not_alive` members are not alive.
///
/// A key has `replication_factor` replicas and stays readable and writable
/// while no more than `replication_factor - max(write_quorum, read_quorum)` of
/// them are down. In a pool larger than the replication factor the members
/// that are down may be replicas of one key, so the pool is critical as soon as
/// more than that many members are down — the worst case, which in a pool of
/// three is the only case.
pub fn health_level(
    not_alive: u64,
    replication_factor: u8,
    write_quorum: u8,
    read_quorum: u8,
) -> HealthLevel {
    let spare = replication_factor.saturating_sub(write_quorum.max(read_quorum));
    if not_alive == 0 {
        HealthLevel::Healthy
    } else if not_alive <= u64::from(spare) {
        HealthLevel::Degraded
    } else {
        HealthLevel::Critical
    }
}

/// The pools a node knows and their members.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterTopology {
    /// Always `cluster`; a single node answers `standalone`.
    pub mode: String,
    /// Topology epoch of this node.
    pub epoch: u64,
    /// The pools.
    pub pools: Vec<PoolTopology>,
}

/// One pool of the topology.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PoolTopology {
    /// Pool identifier.
    pub pool_id: u32,
    /// Pool name.
    pub name: String,
    /// `standard` (replicated) or `erasure_coded`.
    pub storage_class: s4_core::types::StorageClass,
    /// Copies of each replicated key.
    pub replication_factor: u8,
    /// Replicas a write waits for.
    pub write_quorum: u8,
    /// Replicas a read waits for.
    pub read_quorum: u8,
    /// Every configured member, in the order `S4_POOL_NODES` lists them.
    pub nodes: Vec<NodeTopology>,
}

/// One member of a pool as this node sees it.
///
/// Addresses and versions are what the member last announced over gossip;
/// they are absent for a member this node has never heard from.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeTopology {
    /// Its name in `S4_POOL_NODES`; the `peer` label of the metrics.
    pub name: String,
    /// Its identity.
    pub node_id: String,
    /// `alive`, `suspect`, `dead` or `left`, as `s4_cluster_nodes` counts it.
    pub status: String,
    /// Whether it is the node answering.
    pub local: bool,
    /// gRPC address.
    pub grpc_addr: Option<String>,
    /// HTTP address.
    pub http_addr: Option<String>,
    /// Protocol version.
    pub protocol_version: Option<u32>,
    /// S4 version.
    pub s4_version: Option<String>,
}

/// The node answering.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeHealthInfo {
    /// Its identity, as `<data dir>/node_id` holds it.
    pub node_id: String,
    /// The name it announces (`S4_NODE_ID`, or its identity).
    pub node_name: String,
    /// Always `alive`: it is answering.
    pub status: String,
    /// Pool identifier.
    pub pool_id: u32,
    /// Pool name.
    pub pool: String,
    /// Protocol version.
    pub protocol_version: u32,
    /// S4 version.
    pub s4_version: String,
    /// gRPC address.
    pub grpc_addr: String,
    /// HTTP address.
    pub http_addr: String,
}

/// Anti-entropy and tombstone GC on this node, since it started.
///
/// Each field is the metric named beside it, read from the same counter.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RepairStatus {
    /// Keys anti-entropy found behind on this node
    /// (`s4_anti_entropy_divergent_keys_total`).
    pub divergent_keys: u64,
    /// Repair frontier lag at the end of the last round, in seconds
    /// (`s4_repair_frontier_lag_seconds`).
    pub repair_frontier_lag_secs: f64,
    /// Keys repaired (`s4_anti_entropy_repairs_total{outcome="ok"}`).
    pub total_repairs: u64,
    /// When a key was last repaired, RFC 3339; `null` before the first.
    pub last_repair: Option<String>,
    /// Anti-entropy rounds that settled every divergent key
    /// (`s4_anti_entropy_rounds_total{outcome="complete"}`).
    pub rounds_completed: u64,
    /// Keys whose repair failed
    /// (`s4_anti_entropy_repairs_total{outcome="failed"}`).
    pub repairs_failed: u64,
    /// Keys left as they were because their newest copy may have missed a
    /// delete (`s4_anti_entropy_repairs_total{outcome="ambiguous"}`).
    pub repairs_ambiguous: u64,
    /// Anti-entropy rounds by how they ended
    /// (`s4_anti_entropy_rounds_total{outcome}`).
    pub rounds: RoundCounts,
    /// Whether this node starts anti-entropy rounds
    /// (`S4_ANTI_ENTROPY_ENABLED`). A node that does not still answers the
    /// rounds of its peers, and removes no tombstones.
    pub anti_entropy_enabled: bool,
    /// Tombstones after the last GC pass (`s4_tombstones_active`).
    pub tombstones_active: u64,
    /// Tombstones purged (`s4_tombstones_purged_total`).
    pub tombstones_purged: u64,
}

/// Anti-entropy rounds of this node by how they ended.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct RoundCounts {
    /// Every leaf compared and every divergent key settled.
    pub complete: u64,
    /// The round ran but left keys for the next one.
    pub incomplete: u64,
    /// The pool was not ready for rounds, or no peer was alive.
    pub skipped: u64,
}

/// The background scrubber of this node, since it started.
#[derive(Debug, Clone, Serialize)]
pub struct ScrubberStatus {
    /// Always `cluster`; a single node answers `standalone`.
    pub mode: &'static str,
    /// Always `true`: every cluster node runs it.
    pub background_scrubber: bool,
    /// What the scrubber does.
    pub message: &'static str,
    /// Progress of the current full scan, 0 to 1
    /// (`s4_scrubber_scan_progress`).
    pub scan_progress: f64,
    /// The scrubber's counters; `blobs_scanned_total`,
    /// `corruptions_found_total` and `corruptions_healed_total` are the
    /// `s4_scrubber_*` counters of the same names.
    #[serde(flatten)]
    pub counters: ScrubberMetricsSnapshot,
}

/// What `/scrubber/status` says the background scrubber does.
pub const SCRUBBER_MESSAGE: &str = "The background scrubber verifies every blob of this node \
     against its CRC32 and heals a corrupted one from a replica. \
     POST /api/admin/scrubber/run checks every volume now.";

// ---------------------------------------------------------------------------
// The implementation on a cluster node
// ---------------------------------------------------------------------------

/// The pool a node belongs to, as configured.
#[derive(Debug, Clone)]
pub struct PoolShape {
    /// Pool name and members, as the membership metrics count them.
    pub roster: PoolRoster,
    /// Pool identifier.
    pub pool_id: u32,
    /// Replicated or erasure coded.
    pub storage_class: s4_core::types::StorageClass,
    /// Copies of each replicated key.
    pub replication_factor: u8,
    /// Replicas a write waits for.
    pub write_quorum: u8,
    /// Replicas a read waits for.
    pub read_quorum: u8,
    /// Topology epoch.
    pub epoch: u64,
}

/// The cluster admin service of a cluster node.
pub struct NodeClusterAdmin {
    gossip: Arc<GossipHandle>,
    local: NodeMeta,
    pool: PoolShape,
    anti_entropy: Arc<AntiEntropyMetrics>,
    gc: Arc<GcMetrics>,
    scrubber: Arc<ScrubberMetrics>,
    bucket_metadata: Option<Arc<PlacementStatus>>,
    config_adoption: Option<ConfigAdoptionStatus>,
    iam_adoption: Option<crate::iam_adoption::IamAdoptionGate>,
    iam_config: Option<Arc<crate::iam_config::IamConfigCheck>>,
    pool_growth: Option<Arc<PoolGrowth>>,
    migration: Option<Arc<crate::migration::MigrationStatus>>,
}

impl NodeClusterAdmin {
    /// The service of the node `local`, reading membership from `gossip` and
    /// the counters of the services that write the metrics.
    pub fn new(
        gossip: Arc<GossipHandle>,
        local: NodeMeta,
        pool: PoolShape,
        anti_entropy: Arc<AntiEntropyMetrics>,
        gc: Arc<GcMetrics>,
        scrubber: Arc<ScrubberMetrics>,
    ) -> Self {
        Self {
            gossip,
            local,
            pool,
            anti_entropy,
            gc,
            scrubber,
            bucket_metadata: None,
            config_adoption: None,
            iam_adoption: None,
            iam_config: None,
            pool_growth: None,
            migration: None,
        }
    }

    /// The same service, reporting local bucket configuration adoption.
    pub fn with_config_adoption(mut self, status: ConfigAdoptionStatus) -> Self {
        self.config_adoption = Some(status);
        self
    }

    /// The same health service, reporting durable migration obligations.
    pub fn with_migration(mut self, migration: Arc<crate::migration::MigrationStatus>) -> Self {
        self.migration = Some(migration);
        self
    }

    /// The same service, reporting the pool's growth from `growth`.
    pub fn with_pool_growth(mut self, growth: Arc<PoolGrowth>) -> Self {
        self.pool_growth = Some(growth);
        self
    }

    /// The same service, reporting whether the pool is configured alike for
    /// IAM by `check`.
    pub fn with_iam_config(mut self, check: Arc<crate::iam_config::IamConfigCheck>) -> Self {
        self.iam_config = Some(check);
        self
    }

    fn iam_config_health(&self) -> Option<IamConfigHealth> {
        let mismatches = self.iam_config.as_ref()?.mismatches();
        Some(IamConfigHealth {
            consistent: mismatches.is_empty(),
            mismatches,
        })
    }

    /// The same service, reporting the pool's adoption of IAM by `gate`.
    pub fn with_iam_adoption(mut self, gate: crate::iam_adoption::IamAdoptionGate) -> Self {
        self.iam_adoption = Some(gate);
        self
    }

    fn iam_adoption_health(&self) -> Option<IamAdoptionHealth> {
        let gate = self.iam_adoption.as_ref()?;
        let pending = !gate.is_open();
        Some(IamAdoptionHealth {
            pending,
            reason: pending.then(|| {
                "IAM changes are refused until every node of the pool announces that it keeps no \
                 IAM users of its own: see GET /api/admin/iam/adoption"
                    .to_string()
            }),
        })
    }

    /// The same service, reporting the move of the bucket metadata from
    /// `status`.
    pub fn with_bucket_metadata(mut self, status: Arc<PlacementStatus>) -> Self {
        self.bucket_metadata = Some(status);
        self
    }

    fn bucket_metadata_health(&self) -> Option<BucketMetadataHealth> {
        let snapshot = self.bucket_metadata.as_ref()?.snapshot(std::time::Instant::now());
        let held_by = snapshot
            .holdouts
            .iter()
            .map(|holdout| PlacementHoldout {
                node: self
                    .pool
                    .roster
                    .name_of(holdout.node)
                    .map_or_else(|| holdout.node.to_string(), str::to_string),
                reason: holdout.reason.to_string(),
            })
            .collect();
        Some(BucketMetadataHealth {
            stage: snapshot.record.stage.number(),
            stage_name: snapshot.record.stage.as_str().to_string(),
            from: snapshot.record.from.into(),
            to: snapshot.record.to.into(),
            backfill_complete: snapshot.record.backfill_complete,
            held_by,
            held_for_secs: snapshot.held_for.map(|held| held.as_secs()),
        })
    }
}

/// `None` for the empty strings of the placeholder metadata gossip holds for a
/// member whose own announcement has not arrived yet.
fn announced(value: &str) -> Option<String> {
    (!value.is_empty()).then(|| value.to_string())
}

impl ClusterAdminService for NodeClusterAdmin {
    fn cluster_health(&self) -> Option<ClusterHealthStatus> {
        let members = self.gossip.members();
        let statuses: Vec<NodeStatus> =
            self.pool.roster.statuses(&members).map(|member| member.status).collect();
        let count =
            |wanted: NodeStatus| statuses.iter().filter(|status| **status == wanted).count() as u64;
        let alive = count(NodeStatus::Alive);
        let total = statuses.len() as u64;
        let iam_adoption = self.iam_adoption_health();
        let mut status = health_level(
            total - alive,
            self.pool.replication_factor,
            self.pool.write_quorum,
            self.pool.read_quorum,
        );
        if status == HealthLevel::Healthy && iam_adoption.as_ref().is_some_and(|iam| iam.pending) {
            status = HealthLevel::Degraded;
        }
        let iam_config = self.iam_config_health();
        if iam_config.as_ref().is_some_and(|config| !config.consistent) {
            status = HealthLevel::Critical;
        }
        Some(ClusterHealthStatus {
            status,
            nodes_total: total,
            nodes_alive: alive,
            nodes_suspect: count(NodeStatus::Suspect),
            nodes_dead: count(NodeStatus::Dead),
            nodes_left: count(NodeStatus::Left),
            epoch: self.pool.epoch,
            protocol_version: PROTOCOL_VERSION,
            s4_version: S4_VERSION.to_string(),
            pool: self.pool.roster.pool().to_string(),
            replication_factor: self.pool.replication_factor,
            write_quorum: self.pool.write_quorum,
            read_quorum: self.pool.read_quorum,
            bucket_metadata: self.bucket_metadata_health(),
            bucket_config_adoption: self
                .config_adoption
                .as_ref()
                .map(ConfigAdoptionStatus::snapshot),
            iam_adoption,
            iam_config,
            migration_pending_records: self.migration.as_ref().map_or(0, |status| status.pending()),
        })
    }

    fn topology(&self) -> Option<ClusterTopology> {
        let states = self.gossip.members();
        let nodes = self
            .pool
            .roster
            .statuses(&states)
            .map(|member: MemberStatus<'_>| {
                let meta = if member.local {
                    Some(&self.local)
                } else {
                    states.get(&member.node_id).map(|state| &state.meta)
                };
                NodeTopology {
                    name: member.name.to_string(),
                    node_id: member.node_id.to_string(),
                    status: member.status.as_str().to_string(),
                    local: member.local,
                    grpc_addr: meta.map(|meta| meta.addr.grpc_addr.to_string()),
                    http_addr: meta.map(|meta| meta.addr.http_addr.to_string()),
                    protocol_version: meta
                        .filter(|meta| !meta.s4_version.is_empty())
                        .map(|meta| meta.protocol_version),
                    s4_version: meta.and_then(|meta| announced(&meta.s4_version)),
                }
            })
            .collect();
        Some(ClusterTopology {
            mode: "cluster".to_string(),
            epoch: self.pool.epoch,
            pools: vec![PoolTopology {
                pool_id: self.pool.pool_id,
                name: self.pool.roster.pool().to_string(),
                storage_class: self.pool.storage_class,
                replication_factor: self.pool.replication_factor,
                write_quorum: self.pool.write_quorum,
                read_quorum: self.pool.read_quorum,
                nodes,
            }],
        })
    }

    fn node_health(&self) -> Option<NodeHealthInfo> {
        Some(NodeHealthInfo {
            node_id: self.local.node_id.to_string(),
            node_name: self.local.node_name.clone(),
            status: NodeStatus::Alive.as_str().to_string(),
            pool_id: self.local.pool_id.0,
            pool: self.pool.roster.pool().to_string(),
            protocol_version: PROTOCOL_VERSION,
            s4_version: S4_VERSION.to_string(),
            grpc_addr: self.local.addr.grpc_addr.to_string(),
            http_addr: self.local.addr.http_addr.to_string(),
        })
    }

    fn repair_status(&self) -> Option<RepairStatus> {
        let anti_entropy = self.anti_entropy.snapshot();
        let last_repair = (anti_entropy.last_repair_unix_ms > 0)
            .then(|| {
                chrono::DateTime::from_timestamp_millis(anti_entropy.last_repair_unix_ms as i64)
            })
            .flatten()
            .map(|at| at.to_rfc3339_opts(chrono::SecondsFormat::Millis, true));
        Some(RepairStatus {
            divergent_keys: anti_entropy.divergent_keys_found,
            repair_frontier_lag_secs: anti_entropy.frontier_lag_secs,
            total_repairs: anti_entropy.repairs_completed,
            last_repair,
            rounds_completed: anti_entropy.rounds_completed,
            repairs_failed: anti_entropy.repairs_failed,
            repairs_ambiguous: anti_entropy.repairs_ambiguous,
            rounds: RoundCounts {
                complete: anti_entropy.rounds_completed,
                incomplete: anti_entropy.rounds_incomplete,
                skipped: anti_entropy.rounds_skipped,
            },
            anti_entropy_enabled: anti_entropy.enabled,
            tombstones_active: self.gc.tombstones_active.load(Ordering::Relaxed),
            tombstones_purged: self.gc.tombstones_purged.load(Ordering::Relaxed),
        })
    }

    fn pool_growth(&self) -> Option<Arc<PoolGrowth>> {
        self.pool_growth.clone()
    }

    fn scrubber_status(&self) -> Option<ScrubberStatus> {
        let counters = self.scrubber.snapshot();
        Some(ScrubberStatus {
            mode: "cluster",
            background_scrubber: true,
            message: SCRUBBER_MESSAGE,
            scan_progress: counters.scan_progress_pct as f64 / 100.0,
            counters,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster_metrics::publish_membership;
    use crate::gossip::NodeState;
    use crate::identity::{Edition, NodeAddr, NodeCapacity, NodeId, PoolId};
    use crate::test_metrics::TestMetrics;
    use std::collections::HashMap;
    use std::net::SocketAddr;
    use std::sync::atomic::AtomicU64;

    fn meta(node: u128, version: &str) -> NodeMeta {
        let port = 9000 + node as u16;
        NodeMeta {
            node_id: NodeId(node),
            node_name: format!("announced-{node}"),
            addr: NodeAddr {
                grpc_addr: SocketAddr::from(([127, 0, 0, 1], port + 100)),
                http_addr: SocketAddr::from(([127, 0, 0, 1], port)),
                gossip_addr: SocketAddr::from(([127, 0, 0, 1], port + 100)),
            },
            pool_id: PoolId::new(0),
            topology_epoch: 1,
            protocol_version: 3,
            capacity: NodeCapacity::default(),
            started_at: 0,
            s4_version: version.to_string(),
            edition: Edition::Community,
        }
    }

    fn state(node: u128, status: NodeStatus) -> NodeState {
        NodeState {
            meta: meta(node, "1.1.1"),
            status,
            last_seen: std::time::Instant::now(),
        }
    }

    /// Node 1 answers, in a pool of `size` named `node-1` …; `members` is what
    /// its gossip holds.
    fn admin(size: u128, members: HashMap<NodeId, NodeState>) -> (NodeClusterAdmin, PoolRoster) {
        let roster = PoolRoster::new(
            "pool-1",
            NodeId(1),
            (1..=size).map(|node| (NodeId(node), format!("node-{node}"))),
        );
        let admin = NodeClusterAdmin::new(
            Arc::new(GossipHandle::new_for_testing(members)),
            meta(1, S4_VERSION),
            PoolShape {
                roster: roster.clone(),
                pool_id: 0,
                storage_class: s4_core::types::StorageClass::Standard,
                replication_factor: 3,
                write_quorum: 2,
                read_quorum: 2,
                epoch: 1,
            },
            Arc::new(AntiEntropyMetrics::default()),
            Arc::new(GcMetrics::default()),
            Arc::new(ScrubberMetrics::default()),
        );
        (admin, roster)
    }

    #[test]
    fn health_reports_the_initial_migration_pending_count() {
        let (admin, _) = admin(3, HashMap::new());
        let migration = Arc::new(crate::migration::MigrationStatus::default());
        migration.pending_is(17);
        let health = admin.with_migration(migration).cluster_health().unwrap();
        assert_eq!(health.migration_pending_records, 17);
        let serialized = serde_json::to_value(&health).unwrap();
        assert_eq!(serialized["migration_pending_records"], 17);
    }

    #[test]
    fn health_reports_local_bucket_config_adoption() {
        let (admin, _) = admin(3, HashMap::new());
        let status = ConfigAdoptionStatus::default();
        let admin = admin.with_config_adoption(status.clone());
        let health = || admin.cluster_health().unwrap().bucket_config_adoption.unwrap();
        assert_eq!(health().phase, crate::ConfigAdoptionPhase::Starting);
        status.waiting(Some(2), vec!["owner unavailable".into()]);
        assert_eq!(health(), status.snapshot());
        status.mark_ready();
        assert_eq!(health(), status.snapshot());
        let mut old = serde_json::to_value(admin.cluster_health().unwrap()).unwrap();
        old.as_object_mut().unwrap().remove("bucket_config_adoption");
        assert!(serde_json::from_value::<ClusterHealthStatus>(old)
            .unwrap()
            .bucket_config_adoption
            .is_none());
    }

    #[test]
    fn health_follows_the_quorum_every_key_can_still_reach() {
        use HealthLevel::{Critical, Degraded, Healthy};
        // RF=3, W=R=2: one replica of a key may be down.
        assert_eq!(health_level(0, 3, 2, 2), Healthy);
        assert_eq!(health_level(1, 3, 2, 2), Degraded);
        assert_eq!(health_level(2, 3, 2, 2), Critical);
        assert_eq!(health_level(3, 3, 2, 2), Critical);
        // The larger quorum decides: reads of every replica cannot lose one.
        assert_eq!(health_level(1, 3, 1, 3), Critical);
        assert_eq!(health_level(1, 3, 3, 1), Critical);
        // RF=5, W=R=3: two may be down, wherever they are.
        assert_eq!(health_level(2, 5, 3, 3), Degraded);
        assert_eq!(health_level(3, 5, 3, 3), Critical);
        // One copy: any member down takes its keys with it.
        assert_eq!(health_level(1, 1, 1, 1), Critical);
    }

    /// The route and the metrics count one member map by one rule, so they
    /// agree on every status — including a member gossip never heard from.
    #[tokio::test]
    async fn health_counts_what_the_membership_metrics_count() {
        let metrics = TestMetrics::install();
        // Node 2 alive, node 3 dead, node 4 never heard from.
        let members = HashMap::from([
            (NodeId(2), state(2, NodeStatus::Alive)),
            (NodeId(3), state(3, NodeStatus::Dead)),
        ]);
        let (admin, roster) = admin(4, members.clone());
        publish_membership(&roster, &members);

        let health = admin.cluster_health().expect("a cluster node answers");
        for (status, count) in [
            ("alive", health.nodes_alive),
            ("suspect", health.nodes_suspect),
            ("dead", health.nodes_dead),
            ("left", health.nodes_left),
        ] {
            assert_eq!(
                metrics.value(
                    "s4_cluster_nodes",
                    &[("pool", "pool-1"), ("status", status)]
                ),
                Some(count as f64),
                "{status}"
            );
        }
        assert_eq!(
            (health.nodes_total, health.nodes_alive, health.nodes_dead),
            (4, 2, 2)
        );
        assert_eq!(health.status, HealthLevel::Critical, "two of four are down");
        assert_eq!(health.pool, "pool-1");
        assert_eq!(
            (
                health.replication_factor,
                health.write_quorum,
                health.read_quorum
            ),
            (3, 2, 2)
        );
        assert_eq!(health.epoch, 1);
    }

    #[test]
    fn one_member_down_of_three_is_degraded_and_none_is_healthy() {
        let (one_down, _) = admin(
            3,
            HashMap::from([
                (NodeId(2), state(2, NodeStatus::Alive)),
                (NodeId(3), state(3, NodeStatus::Dead)),
            ]),
        );
        assert_eq!(
            one_down.cluster_health().unwrap().status,
            HealthLevel::Degraded
        );

        let (all_up, _) = admin(
            3,
            HashMap::from([
                (NodeId(2), state(2, NodeStatus::Alive)),
                (NodeId(3), state(3, NodeStatus::Alive)),
            ]),
        );
        assert_eq!(
            all_up.cluster_health().unwrap().status,
            HealthLevel::Healthy
        );
    }

    /// A pool whose IAM some node keeps of its own is degraded, with the
    /// reason, until the gate opens; a pool with a member down stays as its
    /// members make it.
    #[test]
    fn a_pool_waiting_for_its_iam_to_be_taken_over_is_degraded() {
        let members = HashMap::from([
            (NodeId(2), state(2, NodeStatus::Alive)),
            (NodeId(3), state(3, NodeStatus::Alive)),
        ]);
        let gate = crate::iam_adoption::IamAdoptionGate::default();
        let (admin, _) = admin(3, members);
        let admin = admin.with_iam_adoption(gate.clone());
        let health = admin.cluster_health().unwrap();
        assert_eq!(health.status, HealthLevel::Degraded);
        let iam = health.iam_adoption.unwrap();
        assert!(iam.pending);
        assert!(iam.reason.unwrap().contains("/api/admin/iam/adoption"));

        gate.open();
        let health = admin.cluster_health().unwrap();
        assert_eq!(health.status, HealthLevel::Healthy);
        assert_eq!(
            health.iam_adoption,
            Some(IamAdoptionHealth {
                pending: false,
                reason: None
            })
        );
    }

    /// A pool whose live nodes are configured differently for IAM is
    /// critical, naming the node that differs; a node that is down is not
    /// compared, and a pool configured alike is healthy.
    #[test]
    fn a_pool_configured_differently_for_iam_is_critical() {
        use crate::iam_config::{IamConfigCheck, IamConfigKind, IamConfigPrint};
        let members = HashMap::from([
            (NodeId(1), state(1, NodeStatus::Alive)),
            (NodeId(2), state(2, NodeStatus::Alive)),
            (NodeId(3), state(3, NodeStatus::Alive)),
        ]);
        let gossip = Arc::new(GossipHandle::new_for_testing(members));
        let (admin, roster) = admin(3, HashMap::new());
        let mut admin = admin;
        admin.gossip = gossip.clone();
        let check = Arc::new(IamConfigCheck::new(
            NodeId(1),
            vec![NodeId(1), NodeId(2), NodeId(3)],
            roster,
            gossip.clone(),
        ));
        let admin = admin.with_iam_config(check.clone());
        let alike = IamConfigPrint {
            root: Some([1; 16]),
            jwt_secret: Some([2; 16]),
            encryption_key: Some([3; 16]),
        };
        for node in 1..=3 {
            gossip.set_iam_config_for_testing(NodeId(node), Some(alike));
        }
        let health = admin.cluster_health().unwrap();
        assert_eq!(health.status, HealthLevel::Healthy);
        assert!(health.iam_config.as_ref().unwrap().consistent);
        assert!(check.pool_shares_encryption_key());

        let other_root = IamConfigPrint {
            root: Some([9; 16]),
            ..alike
        };
        gossip.set_iam_config_for_testing(NodeId(3), Some(other_root));
        let health = admin.cluster_health().unwrap();
        assert_eq!(health.status, HealthLevel::Critical);
        let config = health.iam_config.unwrap();
        assert!(!config.consistent);
        assert_eq!(config.mismatches.len(), 1);
        assert_eq!(config.mismatches[0].kind, IamConfigKind::Root);
        assert_eq!(config.mismatches[0].nodes, vec!["node-3".to_string()]);
        assert!(check.pool_shares_encryption_key());

        let other_key = IamConfigPrint {
            encryption_key: Some([8; 16]),
            ..alike
        };
        gossip.set_iam_config_for_testing(NodeId(3), Some(other_key));
        assert!(!check.pool_shares_encryption_key());
    }

    #[test]
    fn topology_lists_every_configured_member_as_gossip_last_heard_it() {
        let mut placeholder = state(3, NodeStatus::Alive);
        placeholder.meta.s4_version.clear();
        let (admin, _) = admin(
            4,
            HashMap::from([
                (NodeId(2), state(2, NodeStatus::Dead)),
                (NodeId(3), placeholder),
                // Gossip knows a node the pool does not configure.
                (NodeId(9), state(9, NodeStatus::Alive)),
            ]),
        );
        let topology = admin.topology().expect("a cluster node answers");
        assert_eq!((topology.mode.as_str(), topology.epoch), ("cluster", 1));
        let [pool] = topology.pools.as_slice() else {
            panic!("one pool: {topology:?}");
        };
        assert_eq!((pool.pool_id, pool.name.as_str()), (0, "pool-1"));

        let nodes: Vec<_> = pool
            .nodes
            .iter()
            .map(|node| (node.name.as_str(), node.status.as_str(), node.local))
            .collect();
        assert_eq!(
            nodes,
            [
                ("node-1", "alive", true),
                ("node-2", "dead", false),
                ("node-3", "alive", false),
                ("node-4", "dead", false),
            ]
        );
        let [local, dead, placeholder, unheard] = pool.nodes.as_slice() else {
            unreachable!()
        };
        assert_eq!(local.node_id, NodeId(1).to_string());
        assert_eq!(local.s4_version.as_deref(), Some(S4_VERSION));
        assert_eq!(dead.http_addr.as_deref(), Some("127.0.0.1:9002"));
        assert_eq!(dead.grpc_addr.as_deref(), Some("127.0.0.1:9102"));
        assert_eq!(dead.s4_version.as_deref(), Some("1.1.1"));
        assert_eq!(dead.protocol_version, Some(3));
        assert_eq!(
            (
                placeholder.s4_version.as_deref(),
                placeholder.protocol_version
            ),
            (None, None),
            "a member that has not announced itself has no version yet"
        );
        assert_eq!(
            (unheard.grpc_addr.as_deref(), unheard.s4_version.as_deref()),
            (None, None)
        );
        assert_eq!(unheard.node_id, NodeId(4).to_string());
    }

    #[test]
    fn node_health_is_the_node_answering() {
        let (admin, _) = admin(3, HashMap::new());
        let node = admin.node_health().expect("a cluster node answers");
        assert_eq!(node.node_id, NodeId(1).to_string());
        assert_eq!(node.node_name, "announced-1");
        assert_eq!(
            (node.status.as_str(), node.pool_id, node.pool.as_str()),
            ("alive", 0, "pool-1")
        );
        assert_eq!(
            (node.grpc_addr.as_str(), node.http_addr.as_str()),
            ("127.0.0.1:9101", "127.0.0.1:9001")
        );
        assert_eq!(
            (node.protocol_version, node.s4_version.as_str()),
            (PROTOCOL_VERSION, S4_VERSION)
        );
    }

    #[test]
    fn repair_status_reads_the_counters_the_metrics_mirror() {
        let (admin, _) = admin(3, HashMap::new());
        let status = admin.repair_status().expect("a cluster node answers");
        assert_eq!(
            status,
            RepairStatus {
                divergent_keys: 0,
                repair_frontier_lag_secs: 0.0,
                total_repairs: 0,
                last_repair: None,
                rounds_completed: 0,
                repairs_failed: 0,
                repairs_ambiguous: 0,
                rounds: RoundCounts::default(),
                anti_entropy_enabled: false,
                tombstones_active: 0,
                tombstones_purged: 0,
            }
        );

        let anti_entropy = Arc::new(AntiEntropyMetrics::default());
        let set = |counter: &AtomicU64, value| counter.store(value, Ordering::Relaxed);
        set(&anti_entropy.rounds_completed, 7);
        set(&anti_entropy.divergent_keys_found, 5);
        set(&anti_entropy.repairs_completed, 4);
        set(&anti_entropy.repairs_failed, 1);
        set(&anti_entropy.repairs_ambiguous, 2);
        set(&anti_entropy.rounds_incomplete, 3);
        set(&anti_entropy.rounds_skipped, 4);
        anti_entropy.enabled.store(true, Ordering::Relaxed);
        set(&anti_entropy.frontier_lag_secs_bits, 12.5f64.to_bits());
        set(&anti_entropy.last_repair_unix_ms, 1_700_000_000_123);
        let gc = Arc::new(GcMetrics::default());
        set(&gc.tombstones_active, 3);
        set(&gc.tombstones_purged, 9);
        set(&gc.tombstones_scanned, 100);
        let admin = NodeClusterAdmin {
            anti_entropy,
            gc,
            ..admin
        };
        assert_eq!(
            admin.repair_status().unwrap(),
            RepairStatus {
                divergent_keys: 5,
                repair_frontier_lag_secs: 12.5,
                total_repairs: 4,
                last_repair: Some("2023-11-14T22:13:20.123Z".to_string()),
                rounds_completed: 7,
                repairs_failed: 1,
                repairs_ambiguous: 2,
                rounds: RoundCounts {
                    complete: 7,
                    incomplete: 3,
                    skipped: 4,
                },
                anti_entropy_enabled: true,
                tombstones_active: 3,
                tombstones_purged: 9,
            }
        );
    }

    #[test]
    fn scrubber_status_reads_the_scrubber_counters() {
        let scrubber = Arc::new(ScrubberMetrics::default());
        scrubber.blobs_scanned_total.store(40, Ordering::Relaxed);
        scrubber.corruptions_found_total.store(2, Ordering::Relaxed);
        scrubber.corruptions_healed_total.store(1, Ordering::Relaxed);
        scrubber.scan_progress_pct.store(25, Ordering::Relaxed);
        let (admin, _) = admin(3, HashMap::new());
        let admin = NodeClusterAdmin { scrubber, ..admin };

        let json = serde_json::to_value(admin.scrubber_status().unwrap()).unwrap();
        assert_eq!(json["mode"], "cluster");
        assert_eq!(json["background_scrubber"], true);
        assert_eq!(json["scan_progress"], 0.25);
        assert_eq!(json["blobs_scanned_total"], 40);
        assert_eq!(json["corruptions_found_total"], 2);
        assert_eq!(json["corruptions_healed_total"], 1);
    }

    #[test]
    fn a_single_node_has_nothing_to_report() {
        let noop = NoopClusterAdminService;
        assert!(noop.cluster_health().is_none());
        assert!(noop.topology().is_none());
        assert!(noop.node_health().is_none());
        assert!(noop.repair_status().is_none());
        assert!(noop.scrubber_status().is_none());
    }

    #[test]
    fn health_level_serializes_in_lowercase() {
        assert_eq!(
            serde_json::to_value(HealthLevel::Healthy).unwrap(),
            "healthy"
        );
        assert_eq!(
            serde_json::to_value(HealthLevel::Degraded).unwrap(),
            "degraded"
        );
        assert_eq!(
            serde_json::to_value(HealthLevel::Critical).unwrap(),
            "critical"
        );
    }
}
