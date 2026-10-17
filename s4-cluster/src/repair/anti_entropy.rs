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

//! Anti-entropy rounds: replicas of a pool converge without a client.
//!
//! Once per interval a node compares itself with one live peer of its pool,
//! the peers taken in turn in an order shuffled at start, so every peer gets a
//! round once per "peers × interval". The pair compares the keys both of them
//! replicate by placement ([`PairScope`]), by the version digests the storage
//! keeps for every replicated key in the same batch as the record:
//!
//! 1. both sides compute the 16 384 leaves of their tree in one pass over the
//!    digests; the peer sends its whole vector ([`AntiEntropyResponder`]);
//! 2. for each leaf that differs, the keys and digests of both sides are read
//!    a page at a time and compared; a key whose digests differ, or that one
//!    side lacks, is divergent;
//! 3. each divergent key is settled (`Reconciler::settle`): its version is
//!    read from every replica, the pair's answers required, and every replica
//!    that answered and is behind is brought to the newest version — a copy
//!    with its HLC or a tombstone with its HLC. Up to
//!    `S4_ANTI_ENTROPY_MAX_KEYS_PER_ROUND` keys a round; the rest wait.
//!
//! A round is `complete` when every leaf was compared and every divergent key
//! settled; `incomplete` when the peer stopped answering, a key was not
//! settled or ambiguous, or the key limit was reached; `skipped` when a node
//! of the pool has not announced protocol 5 yet or no peer is alive.
//!
//! A complete round sets the peer's cut of the repair frontier to the moment
//! the round started, less the window in which the write of a delete may
//! still bring its tombstone here (replica write timeout and clock skew): the
//! peer has everything with an HLC before the cut that reached this node so.
//! Tombstone GC removes a tombstone once the cut of every other replica of its
//! key is past it ([`DistributedGcService`](crate::gc::DistributedGcService)).
//!
//! A node with `S4_ANTI_ENTROPY_ENABLED=false` starts no rounds but answers
//! the rounds of its peers. The service also holds what the rounds share with
//! the rest of the node: the repair frontier tombstone GC reads, and the
//! counters `/cluster/repair-status` reports.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rand::seq::SliceRandom;
use s4_core::StorageEngine;
use tokio::sync::{watch, RwLock};
use tracing::{debug, info, warn};

use crate::clock::hlc::{now_millis, HlcTimestamp};
use crate::cluster_metrics::{
    record_anti_entropy_key, record_anti_entropy_round, record_repair_frontier_lag,
    KeyRepairOutcome, RoundOutcome,
};
use crate::config::AntiEntropyConfig;
use crate::error::ClusterError;
use crate::gc::tombstone::arrival_window;
use crate::gossip::GossipHandle;
use crate::identity::{NodeId, NodeStatus, PoolId};
use crate::metadata_placement::Stage;
use crate::metadata_placement::StageCell;
use crate::placement::{
    BucketMetadataMove, ClassLayout, GenerationPhase, GenerationRanges, PlacementStrategy,
    PoolLayout, ReplicationClass,
};
use crate::protocol::IAM_REPLICATION_MIN_PROTOCOL_VERSION;
use crate::repair::frontier::RepairFrontier;
use crate::repair::pair_tree::{
    leaf_keys, leaf_of, pair_leaves, LeafCursor, LeafHash, PairScope, LEAF_COUNT, LEAF_HASH_LEN,
};
use crate::repair::reconcile::{KeyRef, Reconciler, Settled};
use crate::rpc::proto;
use crate::rpc::NodeClient;
use s4_core::storage::VERSION_DIGESTS_FORMAT;

/// Lowest protocol version every live node of the pool must announce before
/// rounds start: the version since which every node gives the same keys a
/// digest. A node of protocol 5 keeps no digests of bucket configurations, a
/// node of protocol 7 none of the IAM records of version 2, and the leaves of
/// a pair across that line would differ wherever one lies.
const ROUNDS_MIN_PROTOCOL_VERSION: u32 = IAM_REPLICATION_MIN_PROTOCOL_VERSION;

/// Most keys one page of `AntiEntropyKeys` carries.
pub const MAX_KEYS_PER_PAGE: u32 = 1000;

/// Leaves asked about in one `AntiEntropyKeys` request.
const LEAVES_PER_REQUEST: usize = 64;

/// Metrics collected by the anti-entropy service.
#[derive(Debug, Default)]
pub struct AntiEntropyMetrics {
    /// Rounds that compared every leaf and settled every divergent key.
    pub rounds_completed: AtomicU64,
    /// Rounds that ran but did not settle every divergent key.
    pub rounds_incomplete: AtomicU64,
    /// Rounds skipped: the pool was not ready, or no peer was alive.
    pub rounds_skipped: AtomicU64,
    /// Keys found in different versions on this node and its peer. Each of
    /// them is repaired, failed or ambiguous.
    pub divergent_keys_found: AtomicU64,
    /// Divergent keys every replica that answered is at the newest version of.
    pub repairs_completed: AtomicU64,
    /// Divergent keys a replica was not brought to the newest version of.
    pub repairs_failed: AtomicU64,
    /// Divergent keys whose newest copy may have missed a delete.
    pub repairs_ambiguous: AtomicU64,
    /// Total duration of all rounds that ran, in milliseconds.
    pub total_repair_duration_ms: AtomicU64,
    /// The repair frontier lag at the end of the last round, in seconds, as
    /// the bits of an `f64`: the value `s4_repair_frontier_lag_seconds` was
    /// set to. Zero before the first round.
    pub frontier_lag_secs_bits: AtomicU64,
    /// When a key was last repaired, in Unix milliseconds; zero before the
    /// first repair.
    pub last_repair_unix_ms: AtomicU64,
    /// Whether this node starts rounds (`S4_ANTI_ENTROPY_ENABLED`).
    pub enabled: AtomicBool,
}

impl AntiEntropyMetrics {
    /// Create a new metrics instance.
    pub fn new() -> Self {
        Self::default()
    }

    /// Snapshot the current metrics.
    pub fn snapshot(&self) -> AntiEntropySnapshot {
        AntiEntropySnapshot {
            rounds_completed: self.rounds_completed.load(Ordering::Relaxed),
            rounds_incomplete: self.rounds_incomplete.load(Ordering::Relaxed),
            rounds_skipped: self.rounds_skipped.load(Ordering::Relaxed),
            divergent_keys_found: self.divergent_keys_found.load(Ordering::Relaxed),
            repairs_completed: self.repairs_completed.load(Ordering::Relaxed),
            repairs_failed: self.repairs_failed.load(Ordering::Relaxed),
            repairs_ambiguous: self.repairs_ambiguous.load(Ordering::Relaxed),
            total_repair_duration_ms: self.total_repair_duration_ms.load(Ordering::Relaxed),
            frontier_lag_secs: f64::from_bits(self.frontier_lag_secs_bits.load(Ordering::Relaxed)),
            last_repair_unix_ms: self.last_repair_unix_ms.load(Ordering::Relaxed),
            enabled: self.enabled.load(Ordering::Relaxed),
        }
    }

    fn round(&self, outcome: RoundOutcome, took: Duration) {
        let counter = match outcome {
            RoundOutcome::Complete => &self.rounds_completed,
            RoundOutcome::Incomplete => &self.rounds_incomplete,
            RoundOutcome::Skipped => &self.rounds_skipped,
        };
        counter.fetch_add(1, Ordering::Relaxed);
        if outcome != RoundOutcome::Skipped {
            let millis = u64::try_from(took.as_millis()).unwrap_or(u64::MAX);
            self.total_repair_duration_ms.fetch_add(millis, Ordering::Relaxed);
        }
        record_anti_entropy_round(outcome, took);
    }

    fn key(&self, outcome: KeyRepairOutcome) {
        self.divergent_keys_found.fetch_add(1, Ordering::Relaxed);
        let counter = match outcome {
            KeyRepairOutcome::Ok => {
                self.last_repair_unix_ms.store(now_millis(), Ordering::Relaxed);
                &self.repairs_completed
            }
            KeyRepairOutcome::Failed => &self.repairs_failed,
            KeyRepairOutcome::Ambiguous => &self.repairs_ambiguous,
        };
        counter.fetch_add(1, Ordering::Relaxed);
        record_anti_entropy_key(outcome);
    }
}

/// Point-in-time snapshot of anti-entropy metrics.
#[derive(Debug, Clone, Default)]
pub struct AntiEntropySnapshot {
    /// Rounds that settled every divergent key.
    pub rounds_completed: u64,
    /// Rounds that ran but did not settle every divergent key.
    pub rounds_incomplete: u64,
    /// Rounds skipped.
    pub rounds_skipped: u64,
    /// Total divergent keys discovered.
    pub divergent_keys_found: u64,
    /// Divergent keys settled.
    pub repairs_completed: u64,
    /// Divergent keys not settled.
    pub repairs_failed: u64,
    /// Divergent keys whose newest copy may have missed a delete.
    pub repairs_ambiguous: u64,
    /// Total duration of the rounds that ran, in milliseconds.
    pub total_repair_duration_ms: u64,
    /// The repair frontier lag at the end of the last round, in seconds.
    pub frontier_lag_secs: f64,
    /// When a key was last repaired, in Unix milliseconds; zero before the
    /// first repair.
    pub last_repair_unix_ms: u64,
    /// Whether this node starts rounds.
    pub enabled: bool,
}

/// The repair frontier and the counters of anti-entropy.
#[derive(Debug, Default)]
pub struct AntiEntropyService {
    repair_frontier: Arc<RwLock<RepairFrontier>>,
    metrics: Arc<AntiEntropyMetrics>,
}

impl AntiEntropyService {
    /// Create a service with an empty frontier and counters at zero.
    pub fn new() -> Self {
        Self::default()
    }

    /// Get a reference to the metrics.
    pub fn metrics(&self) -> &Arc<AntiEntropyMetrics> {
        &self.metrics
    }

    /// Get a reference to the repair frontier.
    pub fn repair_frontier(&self) -> &Arc<RwLock<RepairFrontier>> {
        &self.repair_frontier
    }
}

// ---------------------------------------------------------------------------
// The answering side
// ---------------------------------------------------------------------------

/// A pool growing to a successor generation, as anti-entropy sees it (plan
/// ERASURE-CODING-FIXING-POOL-GROWTH-REBALANCE.md, `EPGR-04-2`): until the
/// pool settled here, pairs compare by the current ring; from then on by the
/// successor's, with its bucket metadata layout. Two nodes that announce
/// different current generations compare nothing: their pairs' keys differ.
pub struct AntiEntropyGrowth {
    ranges: Arc<GenerationRanges>,
    data: PlacementStrategy,
    bucket_metadata: Arc<BucketMetadataMove>,
    gossip: Arc<GossipHandle>,
    local_node_id: NodeId,
}

impl AntiEntropyGrowth {
    /// The growth whose arcs are `ranges` to the `successor` layout, with
    /// generations as `gossip` announces them for node `local_node_id`.
    pub fn new(
        ranges: Arc<GenerationRanges>,
        successor: &PoolLayout,
        gossip: Arc<GossipHandle>,
        local_node_id: NodeId,
    ) -> Self {
        let metadata = successor.class(ReplicationClass::BucketMetadata).clone();
        Self {
            ranges,
            data: successor.data().strategy().clone(),
            bucket_metadata: Arc::new(BucketMetadataMove::new(
                metadata.clone(),
                metadata,
                StageCell::new(Stage::Settled),
            )),
            gossip,
            local_node_id,
        }
    }

    fn settled(&self) -> bool {
        self.ranges.phase() == GenerationPhase::Finished
    }

    /// Why this node and `peer` compare nothing now, if they do not: they
    /// announce different current generations.
    fn apart_from(&self, peer: NodeId) -> Option<String> {
        let ours = self.gossip.pool_generation(&self.local_node_id)?;
        let theirs = self.gossip.pool_generation(&peer)?;
        (ours.current != theirs.current).then(|| {
            format!(
                "{peer} announces pool generation {} and this node {}: their pairs differ until \
                 both settle on the same one",
                theirs.current.epoch, ours.current.epoch
            )
        })
    }
}

/// The ring and bucket metadata layout a pair compares by now.
fn pair_placement<'a>(
    growth: Option<&'a AntiEntropyGrowth>,
    placement: &'a PlacementStrategy,
    bucket_metadata: Option<&'a BucketMetadataMove>,
) -> (&'a PlacementStrategy, Option<&'a BucketMetadataMove>) {
    match growth {
        Some(growth) if growth.settled() => (&growth.data, Some(&*growth.bucket_metadata)),
        _ => (placement, bucket_metadata),
    }
}

/// What this node answers the rounds of its peers with: the leaves and keys
/// of the tree it shares with the caller. It answers whether or not it starts
/// rounds itself.
pub struct AntiEntropyResponder {
    storage: Arc<dyn StorageEngine>,
    placement: PlacementStrategy,
    /// The pool's growth to a successor generation, once known.
    growth: std::sync::OnceLock<AntiEntropyGrowth>,
    /// The move of the pool's bucket metadata, whose layouts the caller's
    /// scope usually names.
    bucket_metadata: Option<Arc<BucketMetadataMove>>,
    local_node_id: NodeId,
    pool_id: PoolId,
    topology_epoch: u64,
}

impl AntiEntropyResponder {
    /// Answers for node `local_node_id` of pool `pool_id`, placed by
    /// `placement`, from `storage`, on `topology_epoch`.
    pub fn new(
        storage: Arc<dyn StorageEngine>,
        placement: PlacementStrategy,
        local_node_id: NodeId,
        pool_id: PoolId,
        topology_epoch: u64,
    ) -> Self {
        Self {
            storage,
            placement,
            growth: std::sync::OnceLock::new(),
            bucket_metadata: None,
            local_node_id,
            pool_id,
            topology_epoch,
        }
    }

    /// Answers by the successor's ring once the pool's `growth` settled here;
    /// set once, after the responder began answering.
    pub fn set_growth(&self, growth: AntiEntropyGrowth) {
        let _ = self.growth.set(growth);
    }

    /// The same responder in a pool that moves its bucket metadata as
    /// `moving` says.
    pub fn with_bucket_metadata_move(mut self, moving: BucketMetadataMove) -> Self {
        self.bucket_metadata = Some(Arc::new(moving));
        self
    }

    /// The tree this node shares with the caller, which compares the bucket
    /// metadata as `bucket_metadata` says, or why there is none.
    fn scope(
        &self,
        pool_id: u32,
        peer_id: &[u8],
        bucket_metadata: Option<&proto::BucketMetadataScope>,
    ) -> Result<PairScope, String> {
        if pool_id != self.pool_id.as_u32() {
            return Err(format!(
                "this node is in pool {}, not in pool {pool_id}",
                self.pool_id
            ));
        }
        let peer = NodeId::from_bytes(peer_id).map_err(|e| e.to_string())?;
        let (placement, moving) = pair_placement(
            self.growth.get(),
            &self.placement,
            self.bucket_metadata.as_deref(),
        );
        if peer == self.local_node_id || !placement.all_nodes().contains(&peer) {
            return Err(format!(
                "{peer} is not another node of pool {}",
                self.pool_id
            ));
        }
        if let Some(why) = self.growth.get().and_then(|growth| growth.apart_from(peer)) {
            return Err(why);
        }
        let bucket_metadata = bucket_metadata_placement(bucket_metadata, placement, moving)?;
        Ok(PairScope::by_class(
            placement,
            bucket_metadata.as_ref(),
            self.local_node_id,
            peer,
        ))
    }

    fn other_epoch(&self, epoch: u64) -> bool {
        epoch != 0 && epoch != self.topology_epoch
    }

    /// Why a caller whose digests are of `format` gets no answer, if it does
    /// not: its tree is built over another set of keys, and the keys where
    /// the two differ are ones the two nodes do not both replicate. A node of
    /// protocol 5 sends no format, which reads as 0.
    fn other_format(format: u32) -> Option<String> {
        (format != VERSION_DIGESTS_FORMAT).then(|| {
            format!(
                "the caller keeps version digests of format {format}, this node of format \
                 {VERSION_DIGESTS_FORMAT}: rounds between them wait for the upgrade to end"
            )
        })
    }

    /// Answers `AntiEntropyLeaves`.
    pub async fn leaves(
        &self,
        request: &proto::AntiEntropyLeavesRequest,
    ) -> proto::AntiEntropyLeavesResponse {
        let mut response = proto::AntiEntropyLeavesResponse::default();
        if self.other_epoch(request.topology_epoch) {
            response.epoch_mismatch = true;
            return response;
        }
        if let Some(why) = Self::other_format(request.digests_format) {
            response.error = why;
            return response;
        }
        let leaves = match self.scope(
            request.pool_id,
            &request.peer_id,
            request.bucket_metadata.as_ref(),
        ) {
            Ok(scope) => pair_leaves(&*self.storage, &scope).await.map_err(|e| e.to_string()),
            Err(why) => Err(why),
        };
        match leaves {
            Ok(leaves) => response.leaves = leaves.concat(),
            Err(why) => response.error = why,
        }
        response
    }

    /// Answers `AntiEntropyKeys`.
    pub async fn keys(
        &self,
        request: &proto::AntiEntropyKeysRequest,
    ) -> proto::AntiEntropyKeysResponse {
        let mut response = proto::AntiEntropyKeysResponse::default();
        if self.other_epoch(request.topology_epoch) {
            response.epoch_mismatch = true;
            return response;
        }
        if let Some(why) = Self::other_format(request.digests_format) {
            response.error = why;
            return response;
        }
        let scope = match self.scope(
            request.pool_id,
            &request.peer_id,
            request.bucket_metadata.as_ref(),
        ) {
            Ok(scope) => scope,
            Err(why) => {
                response.error = why;
                return response;
            }
        };
        if request.leaves.windows(2).any(|pair| pair[0] >= pair[1])
            || request.leaves.iter().any(|leaf| *leaf as usize >= LEAF_COUNT)
        {
            response.error = "the leaves asked about are not ascending leaves".into();
            return response;
        }
        let cursor = request.cursor.as_ref().map(|cursor| LeafCursor {
            leaf: cursor.leaf,
            after: cursor.after_key.clone(),
        });
        if let Some(cursor) = &cursor {
            let leaf_asked = request.leaves.binary_search(&cursor.leaf).is_ok();
            let key_in_leaf = cursor
                .after
                .as_deref()
                .is_none_or(|after| leaf_of(s4_core::ring_position_of(after)) == cursor.leaf);
            if !leaf_asked || !key_in_leaf {
                response.error = "the cursor is not in the leaves asked about".into();
                return response;
            }
        }
        let limit = request.max_keys.clamp(1, MAX_KEYS_PER_PAGE) as usize;
        match leaf_keys(&*self.storage, &scope, &request.leaves, cursor, limit).await {
            Ok((keys, next)) => {
                response.keys = keys
                    .into_iter()
                    .map(|entry| proto::AntiEntropyKeyDigest {
                        leaf: leaf_of(entry.ring_position),
                        key: entry.key,
                        digest: entry.digest.encode().to_vec(),
                    })
                    .collect();
                response.next = next.map(|next| proto::AntiEntropyCursor {
                    leaf: next.leaf,
                    after_key: next.after,
                });
            }
            Err(e) => response.error = e.to_string(),
        }
        response
    }
}

/// How a round compares the bucket metadata of a pool that moves them as
/// `moving` says, at this node's stage (plan BUCKET-METADATA-REPLICATION.md,
/// decision 7): by the old layout at stage 0, not at all at stages 1 and 2,
/// where the backfill brings them to the new replicas, and by the new layout
/// once settled. `None` — like the data — for a pool that does not move them.
fn bucket_metadata_scope(
    moving: Option<&BucketMetadataMove>,
) -> Option<proto::BucketMetadataScope> {
    let moving = moving?;
    let placed = |layout: &ClassLayout| proto::BucketMetadataScope {
        excluded: false,
        replication_factor: u32::from(layout.replication_factor()),
    };
    Some(match moving.stage_cell().stage() {
        Stage::Legacy => placed(moving.old()),
        Stage::DualWrite | Stage::ReadNew => proto::BucketMetadataScope {
            excluded: true,
            replication_factor: 0,
        },
        Stage::Settled => placed(moving.new_layout()),
    })
}

/// The placement of the bucket metadata that `scope` asks a pair to compare
/// them by, on the ring of `data`; `None` when they are left out. A layout
/// of `moving`, or the data's, is taken as it is; another replication factor
/// is placed on the same ring.
fn bucket_metadata_placement(
    scope: Option<&proto::BucketMetadataScope>,
    data: &PlacementStrategy,
    moving: Option<&BucketMetadataMove>,
) -> Result<Option<PlacementStrategy>, String> {
    let Some(scope) = scope else {
        return Ok(Some(data.clone()));
    };
    if scope.excluded {
        return Ok(None);
    }
    let rf = u8::try_from(scope.replication_factor)
        .ok()
        .filter(|rf| *rf > 0 && usize::from(*rf) <= data.node_count())
        .ok_or_else(|| {
            format!(
                "bucket metadata on {} replicas cannot be placed on the {} node(s) of the pool",
                scope.replication_factor,
                data.node_count()
            )
        })?;
    let known = moving
        .into_iter()
        .flat_map(|moving| [moving.old().strategy(), moving.new_layout().strategy()])
        .chain([data])
        .find(|placement| placement.replication_factor() == rf);
    match known {
        Some(placement) => Ok(Some(placement.clone())),
        None => PlacementStrategy::new(data.all_nodes().to_vec(), rf)
            .map(Some)
            .map_err(|e| e.to_string()),
    }
}

// ---------------------------------------------------------------------------
// The rounds
// ---------------------------------------------------------------------------

/// Everything a round needs, and the order its peers are taken in.
pub struct AntiEntropyRounds {
    service: Arc<AntiEntropyService>,
    config: AntiEntropyConfig,
    /// A tombstone older than this is not carried to a replica without the key.
    gc_grace: Duration,
    storage: Arc<dyn StorageEngine>,
    placement: PlacementStrategy,
    /// The move of the pool's bucket metadata, if it moves them.
    bucket_metadata: Option<Arc<BucketMetadataMove>>,
    /// The pool's growth to a successor generation, if it grows.
    growth: Option<AntiEntropyGrowth>,
    local_node_id: NodeId,
    pool_id: PoolId,
    topology_epoch: u64,
    rpc_client: Arc<NodeClient>,
    gossip: Arc<GossipHandle>,
    reconciler: Reconciler,
    /// The peers in the order they are taken, and the next one's place.
    turn: Mutex<(Vec<NodeId>, usize)>,
    /// Per peer, the place `(leaf, key)` of the last divergent key a round
    /// with it took when its budget ran out: the next round with that peer
    /// goes on from there, around the ring.
    resume: Mutex<HashMap<NodeId, (u32, String)>>,
    /// When these rounds began, in Unix milliseconds: the frontier lag of a
    /// peer no round has completed with counts from here.
    began_ms: u64,
    /// How far before its start a complete round sets the peer's cut: the
    /// window in which a tombstone may still land on this node through the
    /// write of its delete ([`arrival_window`]).
    cut_margin: Duration,
    /// Called between the leaves and the keys of a round.
    #[cfg(test)]
    after_leaves: Option<Box<dyn Fn() + Send + Sync>>,
}

/// The keys a round compares, and how it asks the peer to compare the
/// bucket metadata.
struct RoundScope {
    pair: PairScope,
    bucket_metadata: Option<proto::BucketMetadataScope>,
}

/// The peer a round is to take.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NextPeer {
    /// Alive, with anti-entropy.
    Ready(NodeId),
    /// Alive, and its protocol not known yet: it keeps its turn.
    NotAnnounced(NodeId),
    /// No peer is alive.
    None,
}

/// Which keys of a pass over divergent leaves are taken.
#[derive(Debug, Clone, Copy)]
enum KeyWindow<'a> {
    /// Every key.
    All,
    /// In leaf `.0`, only the keys after `.1`; every key of the other leaves.
    After(u32, &'a str),
    /// The keys up to `.1`, in leaf `.0`, the only leaf of its pass.
    UpTo(u32, &'a str),
}

impl KeyWindow<'_> {
    fn takes(&self, leaf: u32, key: &str) -> bool {
        match *self {
            Self::All => true,
            Self::After(from, after) => leaf != from || key > after,
            Self::UpTo(at, up_to) => leaf == at && key <= up_to,
        }
    }
}

/// How a round ended and what it found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoundReport {
    /// How the round ended.
    pub outcome: RoundEnd,
    /// The peer it compared this node with.
    pub peer: Option<NodeId>,
    /// Leaves that differed.
    pub divergent_leaves: usize,
    /// Divergent keys settled, failed and ambiguous.
    pub repaired: u64,
    /// Divergent keys not settled.
    pub failed: u64,
    /// Divergent keys whose newest copy may have missed a delete.
    pub ambiguous: u64,
}

/// How a round ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoundEnd {
    /// Every leaf compared and every divergent key settled.
    Complete,
    /// The round ran but left something for the next one, and why.
    Incomplete(String),
    /// The round did not run, and why.
    Skipped(String),
}

impl AntiEntropyRounds {
    /// Rounds of node `local_node_id` of pool `pool_id` placed by
    /// `placement`, comparing `storage` with its peers through `rpc_client`
    /// and settling keys through `reconciler`.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        service: Arc<AntiEntropyService>,
        config: AntiEntropyConfig,
        gc_grace: Duration,
        storage: Arc<dyn StorageEngine>,
        placement: PlacementStrategy,
        local_node_id: NodeId,
        pool_id: PoolId,
        topology_epoch: u64,
        rpc_client: Arc<NodeClient>,
        gossip: Arc<GossipHandle>,
        reconciler: Reconciler,
    ) -> Self {
        let mut peers: Vec<NodeId> = placement
            .all_nodes()
            .iter()
            .copied()
            .filter(|node| *node != local_node_id)
            .collect();
        peers.shuffle(&mut rand::rng());
        service.metrics().enabled.store(config.enabled, Ordering::Relaxed);
        Self {
            service,
            config,
            gc_grace,
            storage,
            placement,
            bucket_metadata: None,
            growth: None,
            local_node_id,
            pool_id,
            topology_epoch,
            rpc_client,
            gossip,
            reconciler,
            turn: Mutex::new((peers, 0)),
            resume: Mutex::new(HashMap::new()),
            began_ms: now_millis(),
            cut_margin: arrival_window(crate::WriteTimeouts::default().replica_write_timeout),
            #[cfg(test)]
            after_leaves: None,
        }
    }

    /// The same rounds in a pool that moves its bucket metadata as `moving`
    /// says: each round compares them by the layout of this node's stage.
    pub fn with_bucket_metadata_move(mut self, moving: BucketMetadataMove) -> Self {
        self.bucket_metadata = Some(Arc::new(moving));
        self
    }

    /// The same rounds in a pool growing as `growth` says: once it settled
    /// here, rounds take the successor's nodes and compare by its ring.
    pub fn with_growth(mut self, growth: AntiEntropyGrowth) -> Self {
        self.growth = Some(growth);
        self
    }

    /// The same rounds, setting a peer's cut `margin` before the start of a
    /// complete round: the arrival window of the write coordinator's replica
    /// write timeout ([`arrival_window`]).
    pub fn with_cut_margin(mut self, margin: Duration) -> Self {
        self.cut_margin = margin;
        self
    }

    /// The same rounds, calling `hook` between the leaves and the keys of
    /// each round.
    #[cfg(test)]
    fn after_leaves(mut self, hook: impl Fn() + Send + Sync + 'static) -> Self {
        self.after_leaves = Some(Box::new(hook));
        self
    }

    /// The same rounds, taking `peer` first.
    #[cfg(test)]
    fn turning_to(self, peer: NodeId) -> Self {
        {
            let mut turn = self.turn.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            let place = turn.0.iter().position(|node| *node == peer).unwrap_or(0);
            turn.1 = place;
        }
        self
    }

    /// Runs a round every interval until `shutdown` turns true. A node with
    /// rounds switched off returns at once: it only answers its peers.
    pub async fn run(self: Arc<Self>, mut shutdown: watch::Receiver<bool>) {
        if !self.config.enabled {
            return;
        }
        let interval = Duration::from_secs(self.config.interval_secs.max(1));
        let mut ticks = tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = ticks.tick() => {
                    self.round().await;
                }
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return;
                    }
                }
            }
        }
    }

    /// Runs one round and records it. A complete round moves the peer's cut
    /// of the repair frontier to the moment the round started, less the
    /// arrival window: a tombstone with an HLC before the cut that the write
    /// of its delete brought here was here when the round started, so the
    /// peer has it, or something newer.
    pub async fn round(&self) -> RoundReport {
        let started = Instant::now();
        let margin = u64::try_from(self.cut_margin.as_millis()).unwrap_or(u64::MAX);
        let cut = HlcTimestamp::from_record(now_millis().saturating_sub(margin), 0, 0);
        let mut report = RoundReport {
            outcome: RoundEnd::Complete,
            peer: None,
            divergent_leaves: 0,
            repaired: 0,
            failed: 0,
            ambiguous: 0,
        };
        report.outcome = self.round_with(&mut report).await;
        if let (RoundEnd::Complete, Some(peer)) = (&report.outcome, report.peer) {
            self.service.repair_frontier().write().await.advance_peer(peer, cut);
        }
        self.record_frontier_lag().await;
        let took = started.elapsed();
        let outcome = match &report.outcome {
            RoundEnd::Complete => RoundOutcome::Complete,
            RoundEnd::Incomplete(_) => RoundOutcome::Incomplete,
            RoundEnd::Skipped(_) => RoundOutcome::Skipped,
        };
        self.service.metrics().round(outcome, took);
        let peer = report.peer.map(|peer| self.rpc_client.peer_label(peer));
        match &report.outcome {
            RoundEnd::Skipped(why) => debug!(%why, "anti-entropy round skipped"),
            RoundEnd::Complete if report.divergent_leaves == 0 => {
                debug!(
                    ?peer,
                    took_ms = took.as_millis() as u64,
                    "anti-entropy round: in step"
                )
            }
            RoundEnd::Complete => info!(
                ?peer,
                divergent_leaves = report.divergent_leaves,
                repaired = report.repaired,
                took_ms = took.as_millis() as u64,
                "anti-entropy round complete"
            ),
            RoundEnd::Incomplete(why) => info!(
                ?peer,
                divergent_leaves = report.divergent_leaves,
                repaired = report.repaired,
                failed = report.failed,
                ambiguous = report.ambiguous,
                %why,
                took_ms = took.as_millis() as u64,
                "anti-entropy round incomplete; the rest is left for the next rounds"
            ),
        }
        report
    }

    /// Records how far behind now the oldest cut over the pool's peers is.
    async fn record_frontier_lag(&self) {
        let peers = self.turn_order();
        let lag_ms = self.service.repair_frontier().read().await.lag_millis(
            &peers,
            self.began_ms,
            now_millis(),
        );
        let seconds = lag_ms as f64 / 1000.0;
        self.service
            .metrics()
            .frontier_lag_secs_bits
            .store(seconds.to_bits(), Ordering::Relaxed);
        record_repair_frontier_lag(seconds);
    }

    async fn round_with(&self, report: &mut RoundReport) -> RoundEnd {
        // A live node that announced an older protocol means a rolling
        // upgrade is under way: rounds wait for it to end. A node gossip has
        // not seen, or that announced nothing yet, does not hold them up — its
        // replicas are settled as ones that did not answer.
        let upgrading: Vec<NodeId> = self
            .turn_order()
            .into_iter()
            .filter(|node| {
                self.gossip.get_node(node).is_some_and(|state| {
                    matches!(state.status, NodeStatus::Alive | NodeStatus::Suspect)
                        && (1..ROUNDS_MIN_PROTOCOL_VERSION).contains(&state.meta.protocol_version)
                })
            })
            .collect();
        if !upgrading.is_empty() {
            return RoundEnd::Skipped(format!(
                "nodes {upgrading:?} of the pool announced a protocol older than \
                 {ROUNDS_MIN_PROTOCOL_VERSION}"
            ));
        }
        let peer = match self.next_live_peer() {
            NextPeer::Ready(peer) => peer,
            NextPeer::NotAnnounced(peer) => {
                return RoundEnd::Skipped(format!(
                    "{peer}, the next peer in turn, has not announced its protocol yet"
                ))
            }
            NextPeer::None => return RoundEnd::Skipped("no peer of the pool is alive".into()),
        };
        report.peer = Some(peer);
        if let Some(why) = self.growth.as_ref().and_then(|growth| growth.apart_from(peer)) {
            return RoundEnd::Skipped(why);
        }
        // Chosen once for the round: the peer compares by the same scope.
        let (ring, moving) = pair_placement(
            self.growth.as_ref(),
            &self.placement,
            self.bucket_metadata.as_deref(),
        );
        let bucket_metadata = bucket_metadata_scope(moving);
        let placement = match bucket_metadata_placement(bucket_metadata.as_ref(), ring, moving) {
            Ok(placement) => placement,
            Err(why) => return RoundEnd::Skipped(why),
        };
        let scope = RoundScope {
            pair: PairScope::by_class(ring, placement.as_ref(), self.local_node_id, peer),
            bucket_metadata,
        };

        let (local, remote) = tokio::join!(
            pair_leaves(&*self.storage, &scope.pair),
            self.peer_leaves(peer, &scope)
        );
        let local = match local {
            Ok(leaves) => leaves,
            Err(e) => return RoundEnd::Incomplete(format!("local leaves: {e}")),
        };
        let remote = match remote {
            Ok(leaves) => leaves,
            Err(e) => return RoundEnd::Incomplete(format!("leaves of the peer: {e}")),
        };
        #[cfg(test)]
        if let Some(hook) = &self.after_leaves {
            hook();
        }
        let divergent: Vec<u32> = (0..LEAF_COUNT)
            .filter(|leaf| local[*leaf] != remote[*leaf])
            .map(|leaf| leaf as u32)
            .collect();
        report.divergent_leaves = divergent.len();
        if divergent.is_empty() {
            return RoundEnd::Complete;
        }

        let budget = self.config.max_keys_per_round.max(1) as usize;
        let resume = self
            .resume
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&peer)
            .cloned();
        let (keys, all_compared) =
            match self.divergent_keys(peer, &scope, &divergent, budget, resume.as_ref()).await {
                Ok(found) => found,
                Err(e) => return RoundEnd::Incomplete(format!("keys of the peer: {e}")),
            };
        {
            let mut resume = self.resume.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            match keys.last() {
                Some(last) if !all_compared => {
                    resume.insert(peer, last.clone());
                }
                _ => {
                    resume.remove(&peer);
                }
            }
        }

        let pair = [self.local_node_id, peer];
        let mut first_failure = None;
        for (_, full_key) in &keys {
            let Some((bucket, key)) = full_key.split_once('/') else {
                continue;
            };
            let settled =
                self.reconciler.settle(KeyRef { bucket, key }, &pair, self.gc_grace).await;
            let outcome = match settled {
                Settled::InStep => {
                    report.repaired += 1;
                    KeyRepairOutcome::Ok
                }
                Settled::Ambiguous(why) => {
                    report.ambiguous += 1;
                    warn!(
                        bucket, key, %why,
                        "anti-entropy left a key as it is: its newest copy may have missed a delete"
                    );
                    first_failure.get_or_insert(format!("{full_key} is ambiguous"));
                    KeyRepairOutcome::Ambiguous
                }
                Settled::Failed(why) => {
                    report.failed += 1;
                    debug!(bucket, key, %why, "anti-entropy could not settle a key this round");
                    first_failure.get_or_insert(format!("{full_key}: {why}"));
                    KeyRepairOutcome::Failed
                }
                Settled::NotOurs => continue,
            };
            self.service.metrics().key(outcome);
        }
        if let Some(why) = first_failure {
            return RoundEnd::Incomplete(why);
        }
        if !all_compared {
            return RoundEnd::Incomplete(format!(
                "more divergent keys than the {budget} a round settles"
            ));
        }
        RoundEnd::Complete
    }

    /// The pool's other nodes, in the order rounds take them.
    fn turn_order(&self) -> Vec<NodeId> {
        self.turn.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).0.clone()
    }

    /// The next peer in turn that gossip holds alive. One that announced a
    /// protocol with anti-entropy is taken, and the turn moves past it. One
    /// whose metadata has not arrived yet — a node gossip has just seen come
    /// back — keeps its turn: the round is skipped rather than taken with
    /// another peer, so that rounds keep visiting every peer in turn.
    fn next_live_peer(&self) -> NextPeer {
        let mut turn = self.turn.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        // A pool that settled on its successor takes the successor's nodes.
        let (ring, _) = pair_placement(self.growth.as_ref(), &self.placement, None);
        let members: Vec<NodeId> = ring
            .all_nodes()
            .iter()
            .copied()
            .filter(|node| *node != self.local_node_id)
            .collect();
        if turn.0.len() != members.len() || !members.iter().all(|node| turn.0.contains(node)) {
            let mut peers = members;
            peers.shuffle(&mut rand::rng());
            *turn = (peers, 0);
        }
        let (peers, next) = &mut *turn;
        let count = peers.len();
        for step in 0..count {
            let place = (*next + step) % count;
            let peer = peers[place];
            let Some(state) = self.gossip.get_node(&peer) else {
                continue;
            };
            if state.status != NodeStatus::Alive || self.rpc_client.is_down(peer) {
                continue;
            }
            if state.meta.protocol_version < ROUNDS_MIN_PROTOCOL_VERSION {
                *next = place;
                return NextPeer::NotAnnounced(peer);
            }
            *next = (place + 1) % count;
            return NextPeer::Ready(peer);
        }
        NextPeer::None
    }

    async fn peer_leaves(
        &self,
        peer: NodeId,
        scope: &RoundScope,
    ) -> Result<Vec<LeafHash>, ClusterError> {
        let response = self
            .rpc_client
            .send_anti_entropy_leaves(
                peer,
                proto::AntiEntropyLeavesRequest {
                    pool_id: self.pool_id.as_u32(),
                    peer_id: self.local_node_id.to_bytes().to_vec(),
                    topology_epoch: self.topology_epoch,
                    digests_format: VERSION_DIGESTS_FORMAT,
                    bucket_metadata: scope.bucket_metadata,
                },
            )
            .await?;
        if response.epoch_mismatch {
            return Err(ClusterError::Unavailable(
                "the peer is on another topology epoch".into(),
            ));
        }
        if !response.error.is_empty() {
            return Err(ClusterError::Rpc(response.error));
        }
        if response.leaves.len() != LEAF_COUNT * LEAF_HASH_LEN {
            return Err(ClusterError::Rpc(format!(
                "the peer sent {} byte(s) of leaves, expected {}",
                response.leaves.len(),
                LEAF_COUNT * LEAF_HASH_LEN
            )));
        }
        Ok(response.leaves.as_chunks::<LEAF_HASH_LEN>().0.to_vec())
    }

    /// The keys of `divergent` leaves (ascending) whose versions differ
    /// between this node and `peer`, with their leaves, at most `budget` of
    /// them; and whether every divergent key was compared. Taken in ring
    /// order from just after `resume` — the last key the previous round with
    /// this peer took — around the ring and back to it, so that keys that do
    /// not settle never keep the rest of the ring from its turn. Leaf by
    /// leaf, so that only one leaf's keys of each side are held at a time.
    async fn divergent_keys(
        &self,
        peer: NodeId,
        scope: &RoundScope,
        divergent: &[u32],
        budget: usize,
        resume: Option<&(u32, String)>,
    ) -> Result<(Vec<(u32, String)>, bool), ClusterError> {
        let passes: Vec<(Vec<u32>, KeyWindow<'_>)> = match resume {
            None => vec![(divergent.to_vec(), KeyWindow::All)],
            Some((from, after)) => {
                let split = divergent.partition_point(|leaf| leaf < from);
                let back: Vec<u32> = divergent
                    .get(split)
                    .filter(|leaf| *leaf == from)
                    .into_iter()
                    .copied()
                    .collect();
                vec![
                    (divergent[split..].to_vec(), KeyWindow::After(*from, after)),
                    (divergent[..split].to_vec(), KeyWindow::All),
                    (back, KeyWindow::UpTo(*from, after)),
                ]
            }
        };
        let mut found = Vec::new();
        for (leaves, window) in &passes {
            for chunk in leaves.chunks(LEAVES_PER_REQUEST) {
                let mut remote = PeerKeys::new(chunk, scope.bucket_metadata);
                for &leaf in chunk {
                    for key in self.differing_keys(&mut remote, peer, scope, leaf).await? {
                        if !window.takes(leaf, &key) {
                            continue;
                        }
                        if found.len() == budget {
                            return Ok((found, false));
                        }
                        found.push((leaf, key));
                    }
                }
            }
        }
        Ok((found, true))
    }

    /// The keys of `leaf` whose versions differ between this node and the
    /// peer, sorted.
    async fn differing_keys(
        &self,
        remote: &mut PeerKeys<'_>,
        peer: NodeId,
        scope: &RoundScope,
        leaf: u32,
    ) -> Result<Vec<String>, ClusterError> {
        let theirs = remote.leaf(self, peer, leaf).await?;
        let mut ours = BTreeMap::new();
        let mut cursor = None;
        loop {
            let (page, next) = leaf_keys(
                &*self.storage,
                &scope.pair,
                &[leaf],
                cursor,
                MAX_KEYS_PER_PAGE as usize,
            )
            .await?;
            ours.extend(page.into_iter().map(|entry| (entry.key, entry.digest.encode().to_vec())));
            match next {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }
        let mut differing: Vec<String> = theirs
            .iter()
            .filter(|(key, digest)| ours.get(*key) != Some(*digest))
            .map(|(key, _)| key.clone())
            .collect();
        differing.extend(ours.keys().filter(|key| !theirs.contains_key(*key)).cloned());
        differing.sort_unstable();
        Ok(differing)
    }
}

/// The keys of a chunk of leaves on the peer, read a page at a time and
/// handed out leaf by leaf.
struct PeerKeys<'a> {
    leaves: &'a [u32],
    /// How the round compares the bucket metadata.
    bucket_metadata: Option<proto::BucketMetadataScope>,
    cursor: Option<proto::AntiEntropyCursor>,
    /// Keys read but not handed out yet, in ring order.
    read: VecDeque<proto::AntiEntropyKeyDigest>,
    done: bool,
}

impl<'a> PeerKeys<'a> {
    fn new(leaves: &'a [u32], bucket_metadata: Option<proto::BucketMetadataScope>) -> Self {
        Self {
            leaves,
            bucket_metadata,
            cursor: None,
            read: VecDeque::new(),
            done: false,
        }
    }

    /// The peer's keys and digests in `leaf`: pages are read until one goes
    /// past it, since they come in ring order.
    async fn leaf(
        &mut self,
        rounds: &AntiEntropyRounds,
        peer: NodeId,
        leaf: u32,
    ) -> Result<BTreeMap<String, Vec<u8>>, ClusterError> {
        while !self.done && self.read.back().is_none_or(|last| last.leaf <= leaf) {
            let response = rounds
                .rpc_client
                .send_anti_entropy_keys(
                    peer,
                    proto::AntiEntropyKeysRequest {
                        pool_id: rounds.pool_id.as_u32(),
                        peer_id: rounds.local_node_id.to_bytes().to_vec(),
                        topology_epoch: rounds.topology_epoch,
                        leaves: self.leaves.to_vec(),
                        cursor: self.cursor.clone(),
                        max_keys: MAX_KEYS_PER_PAGE,
                        digests_format: VERSION_DIGESTS_FORMAT,
                        bucket_metadata: self.bucket_metadata,
                    },
                )
                .await?;
            if response.epoch_mismatch {
                return Err(ClusterError::Unavailable(
                    "the peer is on another topology epoch".into(),
                ));
            }
            if !response.error.is_empty() {
                return Err(ClusterError::Rpc(response.error));
            }
            self.read.extend(response.keys);
            self.cursor = response.next;
            self.done = self.cursor.is_none();
        }
        let mut keys = BTreeMap::new();
        while self.read.front().is_some_and(|front| front.leaf <= leaf) {
            if let Some(entry) = self.read.pop_front() {
                if entry.leaf == leaf {
                    keys.insert(entry.key, entry.digest);
                }
            }
        }
        Ok(keys)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::NodeId;
    use crate::repair::merkle::KeyRange;

    #[test]
    fn metrics_default_zero() {
        let metrics = AntiEntropyMetrics::new();
        let snap = metrics.snapshot();
        assert_eq!(snap.rounds_completed, 0);
        assert_eq!(snap.divergent_keys_found, 0);
        assert_eq!(snap.repairs_completed, 0);
        assert_eq!(snap.repairs_failed, 0);
    }

    // ---- Bucket metadata in the tree of a pair ----

    /// A storage of a six-node pool holding 200 objects and 200 bucket
    /// markers, every node the same.
    async fn storage_of_every_key() -> (Arc<dyn StorageEngine>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let storage = s4_core::storage::BitcaskStorageEngine::open(
            dir.path().join("volumes"),
            dir.path().join("metadata"),
            10 * 1024 * 1024,
            4096,
            false,
            s4_core::storage::IndexOptions {
                version_digests: true,
            },
        )
        .await
        .unwrap();
        let metadata = crate::coordinator::test_pool::hlc_metadata(at(10));
        for i in 0..200 {
            storage
                .put_object("bucket", &format!("k{i}"), b"v", "text/plain", &metadata)
                .await
                .unwrap();
            storage
                .put_object(
                    "__system__",
                    &format!("{}b{i}", s4_core::types::BUCKET_MARKER_KEY_PREFIX),
                    b"1",
                    "application/octet-stream",
                    &metadata,
                )
                .await
                .unwrap();
        }
        (Arc::new(storage), dir)
    }

    /// Two nodes of a pool that moves its bucket metadata from three
    /// replicas to five are at neighbouring stages. Whichever starts the
    /// round, both compute their leaves by the scope it chose, and the leaves
    /// match: each answers by the caller's scope, not by its own stage. The
    /// scopes of stages 0, 1 and 3 give three different trees (task
    /// BMR-04-002).
    #[tokio::test]
    async fn nodes_at_neighbouring_stages_compute_the_same_leaves() {
        use crate::metadata_placement::{QuorumShape, Stage, StageCell};

        let nodes: Vec<NodeId> = (1..=6).map(NodeId).collect();
        let data = PlacementStrategy::new(nodes.clone(), 3).unwrap();
        let moving_at = |stage: Stage| {
            let layout = |shape: QuorumShape| {
                ClassLayout::new(
                    PlacementStrategy::new(nodes.clone(), shape.replication_factor).unwrap(),
                    shape.write_quorum,
                    shape.read_quorum,
                )
                .unwrap()
            };
            BucketMetadataMove::new(
                layout(QuorumShape::majority(3)),
                layout(QuorumShape::majority(5)),
                StageCell::new(stage),
            )
        };
        let (storage_1, _dir_1) = storage_of_every_key().await;
        let (storage_2, _dir_2) = storage_of_every_key().await;
        let responder = |storage: &Arc<dyn StorageEngine>, node: u128, stage: Stage| {
            AntiEntropyResponder::new(
                storage.clone(),
                data.clone(),
                NodeId(node),
                PoolId::new(1),
                0,
            )
            .with_bucket_metadata_move(moving_at(stage))
        };
        let request = |caller: u128, scope: Option<proto::BucketMetadataScope>| {
            proto::AntiEntropyLeavesRequest {
                pool_id: 1,
                peer_id: NodeId(caller).to_bytes().to_vec(),
                topology_epoch: 0,
                digests_format: VERSION_DIGESTS_FORMAT,
                bucket_metadata: scope,
            }
        };
        let mut trees = Vec::new();
        for pair in Stage::ALL.windows(2) {
            let (first, second) = (pair[0], pair[1]);
            let node_1 = responder(&storage_1, 1, first);
            let node_2 = responder(&storage_2, 2, second);
            for caller_stage in [first, second] {
                let scope = bucket_metadata_scope(Some(&moving_at(caller_stage)));
                let on_1 = node_1.leaves(&request(2, scope)).await;
                let on_2 = node_2.leaves(&request(1, scope)).await;
                assert!(on_1.error.is_empty() && on_2.error.is_empty(), "{on_1:?}");
                assert_eq!(
                    on_1.leaves, on_2.leaves,
                    "{first} and {second}, scope of {caller_stage}"
                );
                trees.push((caller_stage, on_1.leaves));
            }
        }
        let tree_of = |stage: Stage| {
            trees
                .iter()
                .find(|(at, _)| *at == stage)
                .map(|(_, leaves)| leaves.clone())
                .unwrap()
        };
        assert_eq!(tree_of(Stage::DualWrite), tree_of(Stage::ReadNew));
        assert_ne!(tree_of(Stage::Legacy), tree_of(Stage::DualWrite));
        assert_ne!(tree_of(Stage::Settled), tree_of(Stage::DualWrite));
        assert_ne!(tree_of(Stage::Legacy), tree_of(Stage::Settled));

        // A caller of an earlier release names no scope: the bucket metadata
        // are compared like the data, which is the old layout.
        let old = responder(&storage_1, 1, Stage::Legacy).leaves(&request(2, None)).await;
        assert_eq!(old.leaves, tree_of(Stage::Legacy));
        // A layout the pool cannot hold is refused.
        let wide = proto::BucketMetadataScope {
            excluded: false,
            replication_factor: 7,
        };
        let refused =
            responder(&storage_1, 1, Stage::Settled).leaves(&request(2, Some(wide))).await;
        assert!(refused.error.contains("cannot be placed"), "{refused:?}");
    }

    // ---- Rounds on a three-node pool ----

    use crate::coordinator::test_pool::{at, Replica, TestPool};
    use crate::gc::{DistributedGcService, GcConfig, TombstonePolicy};
    use crate::repair::pair_tree::leaf_range;
    use s4_core::storage::VersionDigestEntry;

    fn config(max_keys_per_round: u32) -> AntiEntropyConfig {
        AntiEntropyConfig {
            enabled: true,
            interval_secs: 1,
            max_keys_per_round,
        }
    }

    /// Rounds of node 1 of `pool`, counting into `service`, taking node 2
    /// first.
    fn rounds_counted(
        pool: &TestPool,
        config: AntiEntropyConfig,
        service: Arc<AntiEntropyService>,
    ) -> AntiEntropyRounds {
        rounds_keeping(pool, config, service, Duration::MAX)
    }

    /// Rounds of node 1 of `pool`, counting into `service`, carrying no
    /// tombstone older than `gc_grace` to a replica without the key, taking
    /// node 2 first.
    fn rounds_keeping(
        pool: &TestPool,
        config: AntiEntropyConfig,
        service: Arc<AntiEntropyService>,
        gc_grace: Duration,
    ) -> AntiEntropyRounds {
        AntiEntropyRounds::new(
            service,
            config,
            gc_grace,
            pool.storage(1).clone(),
            pool.placement(),
            NodeId(1),
            pool.pool_id(),
            pool.epoch(),
            pool.rpc_client.clone(),
            pool.gossip.clone(),
            pool.reconciler(),
        )
        .turning_to(NodeId(2))
    }

    /// Rounds of node 1 of `pool`, taking node 2 first.
    fn rounds(pool: &TestPool, config: AntiEntropyConfig) -> AntiEntropyRounds {
        rounds_counted(pool, config, Arc::new(AntiEntropyService::new()))
    }

    async fn digests(pool: &TestPool, node: u128) -> Vec<VersionDigestEntry> {
        pool.storage(node).index_db().version_digests().await.unwrap()
    }

    /// Node 2 missed a write, an overwrite and a delete; node 1 missed a
    /// write node 2 took. One round settles all four, every node ends up
    /// with the same versions, and the next round finds nothing.
    async fn diverged(pool: &TestPool) {
        for node in 1..=3 {
            pool.create_bucket(node, at(1)).await;
            pool.put(node, "kept", b"v", at(10)).await;
            pool.put(node, "overwritten", b"v1", at(10)).await;
            pool.put(node, "deleted", b"v", at(10)).await;
        }
        for node in [1, 3] {
            pool.put(node, "new", b"new", at(20)).await;
            pool.put(node, "overwritten", b"v2", at(20)).await;
            pool.delete(node, "deleted", at(30)).await;
        }
        for node in [2, 3] {
            pool.put(node, "theirs", b"theirs", at(20)).await;
        }
    }

    #[tokio::test]
    async fn missed_writes_and_deletes_converge_in_one_round_both_ways() {
        let pool = TestPool::start(Replica::Up, Replica::Up, 21140).await;
        diverged(&pool).await;
        let rounds = rounds(&pool, config(1000));

        let report = rounds.round().await;
        assert_eq!(report.outcome, RoundEnd::Complete);
        assert_eq!(report.peer, Some(NodeId(2)));
        assert_eq!(
            (report.repaired, report.failed, report.ambiguous),
            (4, 0, 0)
        );
        let on_3 = digests(&pool, 3).await;
        assert_eq!(digests(&pool, 1).await, on_3);
        assert_eq!(digests(&pool, 2).await, on_3);

        let again = rounds.turning_to(NodeId(2)).round().await;
        assert_eq!(again.outcome, RoundEnd::Complete);
        assert_eq!(again.divergent_leaves, 0);
    }

    /// Bucket configurations are replicated keys, placed with the bucket
    /// markers: a round settles a configuration a replica missed, an older
    /// one it kept and a delete it missed, as it settles an object. The rest
    /// of `__system__` has no digest and is left alone.
    #[tokio::test]
    async fn a_round_settles_bucket_configurations_and_nothing_else_of_the_system_bucket() {
        let pool = TestPool::start(Replica::Up, Replica::Up, 21288).await;
        let cors = "__s4_bucket_cors_bucket";
        let lifecycle = "__s4_bucket_lifecycle_bucket";
        let policy = "__s4_bucket_policy_bucket";
        for node in 1..=3 {
            pool.create_bucket(node, at(1)).await;
            pool.put_system(node, policy, b"{\"old\":true}", at(10)).await;
        }
        for node in [1, 3] {
            pool.put_system(node, cors, b"<CORSConfiguration/>", at(20)).await;
            pool.put_system(node, lifecycle, b"<new/>", at(30)).await;
            pool.delete_system(node, policy, at(40)).await;
        }
        pool.put_system(2, lifecycle, b"<old/>", at(20)).await;
        let ec_key = "__s4_ec_v1/tiering/bucket";
        pool.put_system(1, ec_key, b"{}", at(50)).await;

        let report = rounds(&pool, config(1000)).round().await;
        assert_eq!(report.outcome, RoundEnd::Complete);
        assert_eq!(
            (report.repaired, report.failed, report.ambiguous),
            (3, 0, 0)
        );
        assert_eq!(digests(&pool, 2).await, digests(&pool, 1).await);
        let on_2 = pool.storage(2);
        assert_eq!(
            on_2.get_object("__system__", cors).await.unwrap().0,
            b"<CORSConfiguration/>"
        );
        assert_eq!(
            on_2.get_object("__system__", lifecycle).await.unwrap().0,
            b"<new/>"
        );
        assert!(matches!(
            on_2.key_version("__system__", policy).await.unwrap(),
            s4_core::KeyVersion::Tombstone(_)
        ));
        assert!(matches!(
            on_2.key_version("__system__", ec_key).await.unwrap(),
            s4_core::KeyVersion::Absent
        ));
    }

    #[tokio::test]
    async fn a_round_counts_what_it_found_and_how_it_ended() {
        let pool = TestPool::start(Replica::Up, Replica::Up, 21164).await;
        diverged(&pool).await;
        let metrics = crate::test_metrics::TestMetrics::install();
        let service = Arc::new(AntiEntropyService::new());
        let rounds = rounds_counted(&pool, config(1000), service.clone());
        rounds.round().await;
        rounds.turning_to(NodeId(2)).round().await;

        let snapshot = service.metrics().snapshot();
        assert_eq!(
            (snapshot.rounds_completed, snapshot.rounds_incomplete),
            (2, 0)
        );
        assert_eq!(
            (snapshot.divergent_keys_found, snapshot.repairs_completed),
            (4, 4)
        );
        assert!(snapshot.last_repair_unix_ms > 0);
        let rounds_total = "s4_anti_entropy_rounds_total";
        assert_eq!(
            metrics.value(rounds_total, &[("outcome", "complete")]),
            Some(2.0)
        );
        assert_eq!(
            metrics.value("s4_anti_entropy_divergent_keys_total", &[]),
            Some(4.0)
        );
        assert_eq!(
            metrics.value("s4_anti_entropy_repairs_total", &[("outcome", "ok")]),
            Some(4.0)
        );
        assert_eq!(
            metrics.value("s4_anti_entropy_round_duration_seconds_count", &[]),
            Some(2.0)
        );
    }

    #[tokio::test]
    async fn a_round_settles_at_most_its_key_limit() {
        let pool = TestPool::start(Replica::Up, Replica::Up, 21144).await;
        diverged(&pool).await;
        let service = Arc::new(AntiEntropyService::new());
        let rounds = rounds_counted(&pool, config(3), service.clone());

        let first = rounds.round().await;
        assert!(
            matches!(first.outcome, RoundEnd::Incomplete(_)),
            "{first:?}"
        );
        assert_eq!(first.repaired, 3);
        let rounds = rounds.turning_to(NodeId(2));
        let second = rounds.round().await;
        assert_eq!(second.outcome, RoundEnd::Complete);
        assert_eq!(second.repaired, 1);
        let rounds = rounds.turning_to(NodeId(2));
        assert_eq!(rounds.round().await.divergent_leaves, 0);

        let snapshot = service.metrics().snapshot();
        assert_eq!(
            (
                snapshot.rounds_completed,
                snapshot.rounds_incomplete,
                snapshot.rounds_skipped
            ),
            (2, 1, 0)
        );
        assert_eq!(
            (snapshot.divergent_keys_found, snapshot.repairs_completed),
            (4, 4)
        );
    }

    #[tokio::test]
    async fn a_pool_with_a_node_below_protocol_6_starts_no_round() {
        let pool = TestPool::start(Replica::Up, Replica::Up, 21148).await;
        diverged(&pool).await;
        // A build of the branch with tombstones but without rounds.
        pool.announce_protocol(3, crate::protocol::TOMBSTONE_MIN_PROTOCOL_VERSION);
        let service = Arc::new(AntiEntropyService::new());
        let rounds = rounds_counted(&pool, config(1000), service.clone());
        let before = digests(&pool, 2).await;

        let report = rounds.round().await;
        assert!(matches!(report.outcome, RoundEnd::Skipped(_)), "{report:?}");
        assert_eq!(digests(&pool, 2).await, before);
        assert_eq!(service.metrics().snapshot().rounds_skipped, 1);

        // Protocol 5 answers rounds, but gives bucket configurations no
        // digest: rounds still wait.
        pool.announce_protocol(3, crate::protocol::ANTI_ENTROPY_MIN_PROTOCOL_VERSION);
        let report = rounds.round().await;
        assert!(matches!(report.outcome, RoundEnd::Skipped(_)), "{report:?}");
        assert_eq!(digests(&pool, 2).await, before);

        // Once it announces the protocol of replicated configurations, rounds run.
        pool.announce_protocol(3, ROUNDS_MIN_PROTOCOL_VERSION);
        assert_eq!(rounds.round().await.outcome, RoundEnd::Complete);
    }

    /// Keys `count` of the pool's bucket that a round takes first, in its
    /// order: by leaf, then by key.
    fn keys_in_round_order(count: usize) -> Vec<String> {
        let mut names: Vec<String> = (0..200).map(|i| format!("key-{i:03}")).collect();
        names.sort_by_key(|name| {
            let full = format!("bucket/{name}");
            (leaf_of(s4_core::ring_position_of(&full)), full)
        });
        names.truncate(count);
        names
    }

    /// Keys that never settle — ambiguous ones — do not keep the rest of the
    /// ring from its turn: the next round goes on past the last key the
    /// previous one took. Before, every round took the same first keys of
    /// the ring and the key past them was never repaired.
    #[tokio::test]
    async fn keys_that_never_settle_do_not_starve_the_rest_of_the_ring() {
        // Node 3 deleted keys before it kept tombstones, up to 50.
        let pool = TestPool::start_with_horizons(
            Replica::Up,
            Replica::Up,
            21200,
            [None, None, Some(at(50))],
        )
        .await;
        let keys = keys_in_round_order(3);
        for node in 1..=3 {
            pool.create_bucket(node, at(1)).await;
        }
        // Lone copies older than node 3's horizon: ambiguous for ever.
        pool.put(2, &keys[0], b"lone", at(40)).await;
        pool.put(2, &keys[1], b"lone", at(40)).await;
        // A write node 2 missed, past them in the ring.
        pool.put(1, &keys[2], b"missed", at(60)).await;
        pool.put(3, &keys[2], b"missed", at(60)).await;
        let rounds = rounds(&pool, config(2));

        let first = rounds.round().await;
        assert_eq!((first.ambiguous, first.repaired), (2, 0), "{first:?}");
        let rounds = rounds.turning_to(NodeId(2));
        let second = rounds.round().await;
        assert!(
            matches!(second.outcome, RoundEnd::Incomplete(_)),
            "{second:?}"
        );
        assert_eq!(second.repaired, 1, "{second:?}");
        let on_2 = pool.storage(2).get_object("bucket", &keys[2]).await.unwrap().0;
        assert_eq!(on_2, b"missed");
    }

    /// A node of the pool gossip has never seen, or one that announced no
    /// protocol yet, does not hold up the rounds of the nodes that are up: its
    /// replicas are the ones that did not answer. A live node that announced
    /// an older protocol does.
    #[tokio::test]
    async fn only_a_live_node_that_announced_an_older_protocol_holds_rounds_up() {
        let pool = TestPool::start(Replica::Up, Replica::Up, 21204).await;
        for node in 1..=3 {
            pool.create_bucket(node, at(1)).await;
        }
        pool.put(1, "k", b"v", at(10)).await;
        pool.put(3, "k", b"v", at(10)).await;
        let state = pool.gossip.get_node(&NodeId(3)).unwrap();

        pool.gossip.remove_member_for_testing(NodeId(3));
        let report = rounds(&pool, config(1000)).round().await;
        assert_eq!(report.outcome, RoundEnd::Complete, "never seen: {report:?}");
        assert_eq!(
            pool.storage(2).get_object("bucket", "k").await.unwrap().0,
            b"v"
        );

        let mut silent = state.clone();
        silent.meta.protocol_version = 0;
        pool.gossip.set_member_for_testing(NodeId(3), silent);
        let report = rounds(&pool, config(1000)).round().await;
        assert!(
            !matches!(report.outcome, RoundEnd::Skipped(_)),
            "announced nothing: {report:?}"
        );
        // Its own turn is kept, not handed to another peer: the round waits.
        let turning_to_3 = rounds(&pool, config(1000)).turning_to(NodeId(3));
        let report = turning_to_3.round().await;
        assert!(matches!(report.outcome, RoundEnd::Skipped(_)), "{report:?}");
        assert_eq!(report.peer, None);
        pool.announce_protocol(3, ROUNDS_MIN_PROTOCOL_VERSION);
        assert_eq!(turning_to_3.round().await.peer, Some(NodeId(3)));

        let mut dead_and_old = state.clone();
        dead_and_old.meta.protocol_version = ROUNDS_MIN_PROTOCOL_VERSION - 1;
        dead_and_old.status = NodeStatus::Dead;
        pool.gossip.set_member_for_testing(NodeId(3), dead_and_old);
        let report = rounds(&pool, config(1000)).round().await;
        assert!(
            !matches!(report.outcome, RoundEnd::Skipped(_)),
            "dead and old: {report:?}"
        );

        pool.gossip.set_member_for_testing(NodeId(3), state);
        pool.announce_protocol(3, ROUNDS_MIN_PROTOCOL_VERSION - 1);
        let report = rounds(&pool, config(1000)).round().await;
        assert!(
            matches!(report.outcome, RoundEnd::Skipped(_)),
            "alive and old: {report:?}"
        );
    }

    #[tokio::test]
    async fn without_a_live_peer_a_round_is_skipped() {
        let pool = TestPool::start(Replica::Down, Replica::Down, 21152).await;
        let report = rounds(&pool, config(1000)).round().await;
        assert!(matches!(report.outcome, RoundEnd::Skipped(_)), "{report:?}");
        assert_eq!(report.peer, None);
    }

    /// Node 2 starts no rounds of its own, yet answers node 1's, which
    /// completes; and with no round of its own its GC removes no tombstone.
    #[tokio::test]
    async fn a_node_that_starts_no_rounds_answers_its_peers_and_purges_nothing() {
        let pool = TestPool::start(Replica::Up, Replica::Up, 21156).await;
        diverged(&pool).await;

        let switched_off = Arc::new(AntiEntropyService::new());
        let node_2 = Arc::new(AntiEntropyRounds::new(
            switched_off.clone(),
            AntiEntropyConfig {
                enabled: false,
                ..config(1000)
            },
            Duration::MAX,
            pool.storage(2).clone(),
            pool.placement(),
            NodeId(2),
            pool.pool_id(),
            pool.epoch(),
            pool.rpc_client.clone(),
            pool.gossip.clone(),
            pool.reconciler(),
        ));
        let (_stop, shutdown) = watch::channel(false);
        tokio::time::timeout(Duration::from_secs(5), node_2.run(shutdown))
            .await
            .expect("a node with rounds switched off returns at once");
        assert!(!switched_off.metrics().snapshot().enabled);

        assert_eq!(
            rounds(&pool, config(1000)).round().await.outcome,
            RoundEnd::Complete
        );
        let snapshot = switched_off.metrics().snapshot();
        assert_eq!(
            snapshot.rounds_completed + snapshot.rounds_incomplete + snapshot.rounds_skipped,
            0
        );

        let gc = DistributedGcService::new(
            gc_config(Duration::ZERO),
            pool.storage(2).clone(),
            switched_off.repair_frontier().clone(),
            pool.layout(),
            NodeId(2),
        );
        gc.run_purge_cycle().await.unwrap();
        assert!(matches!(
            pool.storage(2).key_version("bucket", "deleted").await.unwrap(),
            s4_core::KeyVersion::Tombstone(_)
        ));
    }

    /// Over gRPC the peer's leaves are the ones computed from its storage,
    /// and pages of its keys cover the leaves asked about without a gap or a
    /// repeat.
    #[tokio::test]
    async fn the_peer_answers_with_its_leaves_and_its_keys_page_by_page() {
        let pool = TestPool::start(Replica::Up, Replica::Up, 21160).await;
        for n in 0..300 {
            pool.put(2, &format!("k{n}"), b"v", at(10 + n)).await;
        }
        let scope = PairScope::new(pool.placement(), NodeId(2), NodeId(1));
        let local = pair_leaves(&**pool.storage(2), &scope).await.unwrap();
        let request = proto::AntiEntropyLeavesRequest {
            pool_id: pool.pool_id().as_u32(),
            peer_id: NodeId(1).to_bytes().to_vec(),
            topology_epoch: pool.epoch(),
            digests_format: VERSION_DIGESTS_FORMAT,
            bucket_metadata: None,
        };
        let answer = pool.rpc_client.send_anti_entropy_leaves(NodeId(2), request).await.unwrap();
        assert_eq!(answer.leaves, local.concat());

        let all = digests(&pool, 2).await;
        let mut leaves: Vec<u32> = all.iter().map(|entry| leaf_of(entry.ring_position)).collect();
        leaves.dedup();
        let leaves: Vec<u32> = leaves.into_iter().step_by(2).collect();
        let wanted: Vec<(String, Vec<u8>)> = all
            .iter()
            .filter(|entry| leaves.contains(&leaf_of(entry.ring_position)))
            .map(|entry| (entry.key.clone(), entry.digest.encode().to_vec()))
            .collect();
        let mut paged = Vec::new();
        let mut cursor = None;
        loop {
            let page = pool
                .rpc_client
                .send_anti_entropy_keys(
                    NodeId(2),
                    proto::AntiEntropyKeysRequest {
                        pool_id: pool.pool_id().as_u32(),
                        peer_id: NodeId(1).to_bytes().to_vec(),
                        topology_epoch: pool.epoch(),
                        leaves: leaves.clone(),
                        cursor: cursor.clone(),
                        max_keys: 5,
                        digests_format: VERSION_DIGESTS_FORMAT,
                        bucket_metadata: None,
                    },
                )
                .await
                .unwrap();
            assert!(page.error.is_empty(), "{}", page.error);
            assert!(page.keys.len() <= 5);
            for key in &page.keys {
                let (from, to) = leaf_range(key.leaf);
                let position = s4_core::ring_position_of(&key.key);
                assert!(position >= from && to.is_none_or(|to| position < to));
            }
            paged.extend(page.keys.into_iter().map(|key| (key.key, key.digest)));
            match page.next {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }
        assert_eq!(paged, wanted);

        // A cursor outside the leaves asked about, or naming a key of another
        // leaf, is refused rather than answered with keys of other leaves.
        let skipped = all
            .iter()
            .find(|entry| !leaves.contains(&leaf_of(entry.ring_position)))
            .unwrap();
        for cursor in [
            proto::AntiEntropyCursor {
                leaf: leaf_of(skipped.ring_position),
                after_key: None,
            },
            proto::AntiEntropyCursor {
                leaf: leaves[leaves.len() - 1],
                after_key: Some(skipped.key.clone()),
            },
        ] {
            let refused = pool
                .rpc_client
                .send_anti_entropy_keys(
                    NodeId(2),
                    proto::AntiEntropyKeysRequest {
                        pool_id: pool.pool_id().as_u32(),
                        peer_id: NodeId(1).to_bytes().to_vec(),
                        topology_epoch: pool.epoch(),
                        leaves: leaves.clone(),
                        cursor: Some(cursor.clone()),
                        max_keys: 5,
                        digests_format: VERSION_DIGESTS_FORMAT,
                        bucket_metadata: None,
                    },
                )
                .await
                .unwrap();
            assert!(
                refused.keys.is_empty() && !refused.error.is_empty(),
                "{cursor:?}: {refused:?}"
            );
        }

        // A caller that is no node of the pool gets no tree.
        let stranger = proto::AntiEntropyLeavesRequest {
            peer_id: NodeId(9).to_bytes().to_vec(),
            ..request_for(&pool)
        };
        let refused = pool.rpc_client.send_anti_entropy_leaves(NodeId(2), stranger).await.unwrap();
        assert!(refused.leaves.is_empty() && !refused.error.is_empty());

        // A caller whose digests are of another format — a node of protocol
        // 5 sends none — gets neither leaves nor keys: its tree is over
        // another set of keys.
        for format in [0, VERSION_DIGESTS_FORMAT - 1] {
            let leaves_of_old = proto::AntiEntropyLeavesRequest {
                digests_format: format,
                bucket_metadata: None,
                ..request_for(&pool)
            };
            let refused = pool
                .rpc_client
                .send_anti_entropy_leaves(NodeId(2), leaves_of_old)
                .await
                .unwrap();
            assert!(
                refused.leaves.is_empty() && refused.error.contains("format"),
                "{format}: {refused:?}"
            );
            let refused = pool
                .rpc_client
                .send_anti_entropy_keys(
                    NodeId(2),
                    proto::AntiEntropyKeysRequest {
                        pool_id: pool.pool_id().as_u32(),
                        peer_id: NodeId(1).to_bytes().to_vec(),
                        topology_epoch: pool.epoch(),
                        leaves: leaves.clone(),
                        cursor: None,
                        max_keys: 5,
                        digests_format: format,
                        bucket_metadata: None,
                    },
                )
                .await
                .unwrap();
            assert!(
                refused.keys.is_empty() && refused.error.contains("format"),
                "{format}: {refused:?}"
            );
        }
    }

    fn request_for(pool: &TestPool) -> proto::AntiEntropyLeavesRequest {
        proto::AntiEntropyLeavesRequest {
            pool_id: pool.pool_id().as_u32(),
            peer_id: NodeId(1).to_bytes().to_vec(),
            topology_epoch: pool.epoch(),
            digests_format: VERSION_DIGESTS_FORMAT,
            bucket_metadata: None,
        }
    }

    fn gc_config(gc_grace: Duration) -> GcConfig {
        GcConfig {
            enabled: true,
            interval: Duration::from_secs(3600),
            tombstone_policy: TombstonePolicy {
                gc_grace,
                max_rejoin_downtime: Duration::ZERO,
                repair_turn: Duration::ZERO,
                write_settle: Duration::ZERO,
            },
        }
    }

    async fn cut_of(service: &AntiEntropyService, peer: u128) -> Option<HlcTimestamp> {
        service.repair_frontier().read().await.peer_cut(&NodeId(peer))
    }

    /// A complete round sets the peer's cut to the moment it started less the
    /// arrival window, a round in step included; the cut of a peer no round was taken with stays
    /// unset; a repeated round moves the cut on.
    #[tokio::test]
    async fn a_complete_round_sets_the_peers_cut_to_its_start() {
        let pool = TestPool::start(Replica::Up, Replica::Up, 21252).await;
        diverged(&pool).await;
        let service = Arc::new(AntiEntropyService::new());
        let rounds = rounds_counted(&pool, config(1000), service.clone());

        let window = 60_500;
        let before = now_millis();
        assert_eq!(rounds.round().await.outcome, RoundEnd::Complete);
        let first = cut_of(&service, 2).await.expect("a cut of node 2");
        assert!(
            first.wall_time >= before - window && first.wall_time <= now_millis() - window,
            "{first:?}: the start of the round less the arrival window"
        );
        assert_eq!(cut_of(&service, 3).await, None);

        tokio::time::sleep(Duration::from_millis(5)).await;
        let rounds = rounds.turning_to(NodeId(2));
        let again = rounds.round().await;
        assert_eq!(
            (again.outcome, again.divergent_leaves),
            (RoundEnd::Complete, 0)
        );
        let second = cut_of(&service, 2).await.unwrap();
        assert!(second > first, "{second:?} after {first:?}");
    }

    /// A round that left keys for the next one sets no cut: the peer may
    /// still lack what this node holds.
    #[tokio::test]
    async fn an_incomplete_round_sets_no_cut() {
        let pool = TestPool::start(Replica::Up, Replica::Up, 21256).await;
        diverged(&pool).await;
        let service = Arc::new(AntiEntropyService::new());
        let rounds = rounds_counted(&pool, config(3), service.clone());

        let first = rounds.round().await;
        assert!(
            matches!(first.outcome, RoundEnd::Incomplete(_)),
            "{first:?}"
        );
        assert_eq!(cut_of(&service, 2).await, None);

        let rounds = rounds.turning_to(NodeId(2));
        assert_eq!(rounds.round().await.outcome, RoundEnd::Complete);
        assert!(cut_of(&service, 2).await.is_some());
    }

    /// The peer answers with its leaves, then stops answering before its
    /// keys: the round is incomplete and the peer gets no cut.
    #[tokio::test]
    async fn a_peer_that_stops_answering_amid_the_keys_gets_no_cut() {
        let pool = TestPool::start(Replica::Up, Replica::Up, 21260).await;
        diverged(&pool).await;
        let service = Arc::new(AntiEntropyService::new());
        let gossip = pool.gossip.clone();
        let mut gone = gossip.get_node(&NodeId(2)).unwrap();
        gone.status = NodeStatus::Dead;
        let rounds = rounds_counted(&pool, config(1000), service.clone())
            .after_leaves(move || gossip.set_member_for_testing(NodeId(2), gone.clone()));

        let report = rounds.round().await;
        assert_eq!(report.peer, Some(NodeId(2)));
        assert!(report.divergent_leaves > 0);
        assert!(
            matches!(report.outcome, RoundEnd::Incomplete(_)),
            "{report:?}"
        );
        assert_eq!(cut_of(&service, 2).await, None);
    }

    /// The frontier lag is the age of the oldest cut over the pool's peers;
    /// a peer no round has completed with counts from when the rounds began.
    #[tokio::test]
    async fn the_frontier_lag_counts_over_every_peer_of_the_pool() {
        let pool = TestPool::start(Replica::Up, Replica::Up, 21264).await;
        let metrics = crate::test_metrics::TestMetrics::install();
        let service = Arc::new(AntiEntropyService::new());
        let began = now_millis();
        // Without the arrival window, so that a cut is younger than the start
        // of the rounds and the two rules can be told apart.
        let rounds =
            rounds_counted(&pool, config(1000), service.clone()).with_cut_margin(Duration::ZERO);
        tokio::time::sleep(Duration::from_millis(300)).await;

        // Node 3 has no cut yet: the lag runs from the start of the rounds.
        assert_eq!(rounds.round().await.outcome, RoundEnd::Complete);
        let lag = service.metrics().snapshot().frontier_lag_secs;
        let since_began = (now_millis() - began) as f64 / 1000.0;
        assert!((0.3..=since_began).contains(&lag), "{lag} of {since_began}");
        assert_eq!(
            metrics.value("s4_repair_frontier_lag_seconds", &[]),
            Some(lag)
        );

        // Both peers have cuts: the lag is the age of the older.
        let rounds = rounds.turning_to(NodeId(3));
        assert_eq!(rounds.round().await.outcome, RoundEnd::Complete);
        let lag = service.metrics().snapshot().frontier_lag_secs;
        let oldest = cut_of(&service, 2).await.unwrap().min(cut_of(&service, 3).await.unwrap());
        let age = (now_millis() - oldest.wall_time) as f64 / 1000.0;
        // The cut, not the start of the rounds: the oldest cut is past it.
        assert!(oldest.wall_time >= began + 300, "{oldest:?}");
        assert!(lag > 0.0 && lag <= age, "{lag}, the oldest cut {age}s old");
    }

    /// Node 3 missed a delete. Node 1 holds the tombstone until a complete
    /// round with each of its peers, node 3 among them, then removes it; and
    /// the next rounds bring nothing back, though nodes 2 and 3 still hold
    /// their tombstones.
    #[tokio::test]
    async fn a_tombstone_goes_once_every_other_replica_has_it_and_the_key_stays_deleted() {
        let pool = TestPool::start(Replica::Up, Replica::Up, 21268).await;
        for node in 1..=3 {
            pool.create_bucket(node, at(1)).await;
            pool.put(node, "deleted", b"v", at(10)).await;
        }
        pool.delete(1, "deleted", at(30)).await;
        pool.delete(2, "deleted", at(30)).await;

        let gc_grace = Duration::from_secs(1);
        let service = Arc::new(AntiEntropyService::new());
        let rounds = rounds_keeping(&pool, config(1000), service.clone(), gc_grace);
        let gc = DistributedGcService::new(
            gc_config(gc_grace),
            pool.storage(1).clone(),
            service.repair_frontier().clone(),
            pool.layout(),
            NodeId(1),
        );
        let on = |node: u128| pool.storage(node).key_version("bucket", "deleted");

        assert_eq!(rounds.round().await.outcome, RoundEnd::Complete);
        assert_eq!(gc.run_purge_cycle().await.unwrap().purged, 0);
        assert!(matches!(
            on(1).await.unwrap(),
            s4_core::KeyVersion::Tombstone(_)
        ));

        let rounds = rounds.turning_to(NodeId(3));
        let with_3 = rounds.round().await;
        assert_eq!((with_3.outcome, with_3.repaired), (RoundEnd::Complete, 1));
        assert!(matches!(
            on(3).await.unwrap(),
            s4_core::KeyVersion::Tombstone(_)
        ));
        assert_eq!(gc.run_purge_cycle().await.unwrap().purged, 1);
        assert!(matches!(on(1).await.unwrap(), s4_core::KeyVersion::Absent));

        let mut rounds = rounds;
        for peer in [2, 3] {
            rounds = rounds.turning_to(NodeId(peer));
            let report = rounds.round().await;
            assert_eq!(report.outcome, RoundEnd::Complete, "with node {peer}");
        }
        assert!(matches!(on(1).await.unwrap(), s4_core::KeyVersion::Absent));
        for node in [2, 3] {
            assert!(matches!(
                on(node).await.unwrap(),
                s4_core::KeyVersion::Tombstone(_)
            ));
        }
        let head = pool.read_coordinator().head("bucket", "deleted").await;
        assert!(
            matches!(head, Err(ClusterError::ObjectNotFound { .. })),
            "{head:?}"
        );
    }

    /// A tombstone the write of its delete brings to node 1 after its rounds
    /// with both peers started — late, but with an HLC before their start, as
    /// a slow replica write or a coordinator whose clock is behind leaves it —
    /// was in neither round, so the peers' cuts must not cover it: node 1
    /// keeps it, though it is older than the grace period. Before, a cut at
    /// the start of the round covered it and node 1 removed it.
    #[tokio::test]
    async fn a_tombstone_that_arrived_after_the_rounds_started_is_kept() {
        let pool = TestPool::start(Replica::Up, Replica::Up, 21284).await;
        for node in 1..=3 {
            pool.create_bucket(node, at(1)).await;
        }
        let service = Arc::new(AntiEntropyService::new());
        let started = now_millis();
        let mut rounds = rounds_counted(&pool, config(1000), service.clone());
        for peer in [2, 3] {
            rounds = rounds.turning_to(NodeId(peer));
            assert_eq!(rounds.round().await.outcome, RoundEnd::Complete);
        }
        // Issued 30 s before the rounds started, inside the arrival window.
        let late = HlcTimestamp::from_record(started - 30_000, 0, 7);
        pool.delete(1, "late", late).await;
        let gc = DistributedGcService::new(
            gc_config(Duration::from_secs(1)),
            pool.storage(1).clone(),
            service.repair_frontier().clone(),
            pool.layout(),
            NodeId(1),
        );

        assert_eq!(gc.run_purge_cycle().await.unwrap().purged, 0);
        let held = pool.storage(1).key_version("bucket", "late").await.unwrap();
        assert!(
            matches!(held, s4_core::KeyVersion::Tombstone(_)),
            "{held:?}"
        );
    }

    /// GC of node `node` of `pool` that holds every other replica past now:
    /// the frontier as a slip of it would leave it.
    async fn purge_as_if_every_peer_had_it(pool: &TestPool, node: u128) -> u64 {
        let mut frontier = RepairFrontier::new();
        for peer in (1..=3).filter(|peer| *peer != node) {
            frontier.advance_peer(NodeId(peer), HlcTimestamp::from_record(now_millis(), 0, 0));
        }
        DistributedGcService::new(
            gc_config(Duration::from_secs(1)),
            pool.storage(node).clone(),
            Arc::new(RwLock::new(frontier)),
            pool.layout(),
            NodeId(node),
        )
        .run_purge_cycle()
        .await
        .unwrap()
        .purged
    }

    /// Nodes 1 and 2 removed the tombstone of `lone` while node 3 still holds
    /// the copy the delete outranked: the frontier slipped. Their absence no
    /// longer proves the copy outlived every delete — they report the
    /// tombstone they removed as their horizon — so the lone copy is
    /// ambiguous and goes nowhere. Before, their horizon stayed zero and the
    /// round spread the deleted object back to them. A copy two replicas hold
    /// is confirmed as before and is brought to the third.
    #[tokio::test]
    async fn a_lone_copy_older_than_a_removed_tombstone_is_not_spread() {
        let pool = TestPool::start(Replica::Up, Replica::Up, 21280).await;
        for node in 1..=3 {
            pool.create_bucket(node, at(1)).await;
            pool.put(node, "lone", b"deleted", at(10)).await;
        }
        pool.delete(1, "lone", at(30)).await;
        pool.delete(2, "lone", at(30)).await;
        pool.put(2, "pair", b"kept", at(10)).await;
        pool.put(3, "pair", b"kept", at(10)).await;
        pool.delete(1, "pair", at(30)).await;
        assert_eq!(purge_as_if_every_peer_had_it(&pool, 1).await, 2);
        assert_eq!(purge_as_if_every_peer_had_it(&pool, 2).await, 1);

        let service = Arc::new(AntiEntropyService::new());
        let report = rounds_counted(&pool, config(1000), service.clone())
            .turning_to(NodeId(3))
            .round()
            .await;
        assert_eq!((report.ambiguous, report.repaired), (1, 1), "{report:?}");
        for node in [1, 2] {
            let held = pool.storage(node).key_version("bucket", "lone").await.unwrap();
            assert!(
                matches!(held, s4_core::KeyVersion::Absent),
                "node {node}: {held:?}"
            );
        }
        assert_eq!(
            pool.storage(1).get_object("bucket", "pair").await.unwrap().0,
            b"kept"
        );
    }

    #[tokio::test]
    async fn repair_frontier_integration() {
        let service = AntiEntropyService::new();

        let range = KeyRange::full();
        let hlc = HlcTimestamp {
            wall_time: 1000,
            logical: 0,
            node_id: 0,
        };

        {
            let mut frontier = service.repair_frontier.write().await;
            frontier.advance(&range, NodeId(2), hlc);
        }

        let frontier = service.repair_frontier.read().await;
        assert_eq!(frontier.get_cut(&range, &NodeId(2)), Some(hlc));
    }
}
