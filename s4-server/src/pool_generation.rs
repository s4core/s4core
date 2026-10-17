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

//! Bootstrap durable pool generations before constructing any placement, and
//! run the pool's growth on this node: the driver that begins the move,
//! verifies and cuts over its ranges and settles the pool, the move of this
//! node's keys, and the clean-up of the copies it no longer owns (plan
//! ERASURE-CODING-FIXING-POOL-GROWTH-REBALANCE.md).

use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::Context;
use s4_cluster::metadata_placement::Stage;
use s4_cluster::placement::GenerationPhase;
use s4_cluster::pool_generation::{
    self, GenerationAnnouncement, GenerationPeer, PoolGenerationRecord,
};
use s4_cluster::rebalance::space::{self, StoredBytes};
use s4_cluster::rebalance::{
    FinishedMark, ForeignCopies, ForeignMode, ForeignParts, ForeignProgress, MultipartTransfer,
    PoolTransition, ProgressStore, QuorumLoad, QuorumRecords, RangeVerifier, RebalanceConfig,
    RebalanceParts, RebalanceService, TransitionConfig, TransitionDriver,
};
use s4_cluster::rpc::NodeClient;
use s4_cluster::{
    ClusterError, GossipHandle, NodeId, NodeStatus, PoolId, QuorumReadCoordinator,
    QuorumWriteCoordinator, PROTOCOL_VERSION,
};
use s4_cluster::{LocalReplica, MultipartReplicaStorage, PlacementRouter, Reconciler, WithHolder};
use s4_core::storage::IndexDb;
use s4_core::StorageEngine;
use tokio::sync::watch;
use tracing::{info, warn};

/// The refusal of a start whose roster cannot reproduce the generation `peer`
/// announces, worded for the operator who has to act on it.
///
/// The usual cause is a machine that took a pool node's address with an
/// identity of its own: a blank data directory generates one. The pool goes on
/// placing that node's data, and an erasure-coded pool its slots, on the
/// identity it recorded, so the start is refused rather than joined under a
/// name nothing is placed on. When gossip still knows the recorded identity the
/// error names it; otherwise it says where to read it.
fn roster_refused(
    error: ClusterError,
    announcement: &GenerationAnnouncement,
    peer: NodeId,
    gossip: &GossipHandle,
    local: NodeId,
    pool: PoolId,
    configured: &[NodeId],
) -> anyhow::Error {
    let epoch = announcement.current.epoch;
    let owners = announcement.current.node_count;
    let Some(position) = configured.iter().position(|node| *node == local) else {
        return error.into();
    };
    if usize::try_from(owners).map_or(true, |owners| configured.len() < owners) {
        return error.into();
    }
    if position as u64 >= owners {
        return anyhow::anyhow!(
            "{error}. The pool's generation {epoch}, announced by node {peer}, has {owners} \
             owner(s), and the identities at the first {owners} address(es) of S4_POOL_NODES \
             are not those owners in that order. Every node of the pool must list the same \
             addresses in the same order, with new nodes appended at the end; a machine that \
             replaces an owner takes its identity through S4_EXPECTED_NODE_IDENTITY"
        );
    }
    let candidates: Vec<NodeId> =
        gossip.members().into_keys().filter(|node| !configured.contains(node)).collect();
    match announcement.holder_of(pool, configured, position, candidates) {
        Some(holder) => anyhow::anyhow!(
            "{error}. This node is {local}, but the pool's generation {epoch}, announced by \
             node {peer}, holds its place in S4_POOL_NODES (position {}) for node {holder}. \
             A machine that replaces that node must take its identity: start it on an empty \
             data directory with S4_EXPECTED_NODE_IDENTITY={holder}. A new identity is refused \
             because the pool goes on placing that node's data on {holder}, which no machine \
             would then hold",
            position + 1
        ),
        None => anyhow::anyhow!(
            "{error}. This node is {local}, and the pool's generation {epoch}, announced by \
             node {peer}, does not hold its place in S4_POOL_NODES (position {}) for this \
             identity, or another address of S4_POOL_NODES answers with an identity the pool \
             does not know. If this machine replaces a pool node, start it on an empty data \
             directory with S4_EXPECTED_NODE_IDENTITY=<identity of that node>: any surviving \
             node lists the pool's identities at GET /api/admin/cluster/topology. If \
             S4_POOL_NODES on this node lists the addresses in another order than on the rest \
             of the pool, restore that order",
            position + 1
        ),
    }
}

/// Chooses the durable ring before using the configured, potentially grown
/// roster. New nodes learn the old ordered prefix from their peers' tails.
#[allow(clippy::too_many_arguments)]
pub async fn start(
    index: &IndexDb,
    gossip: &GossipHandle,
    local: NodeId,
    pool: PoolId,
    configured: &[NodeId],
    stored: Option<PoolGenerationRecord>,
    previously_known: &[NodeId],
) -> anyhow::Result<PoolGenerationRecord> {
    anyhow::ensure!(
        configured.contains(&local),
        "local node is absent from the configured pool roster"
    );
    if let Some(stored) = stored {
        anyhow::ensure!(
            stored.pool_id == pool,
            "stored generation belongs to a different pool"
        );
        let stored = adopt_settled_growth(index, gossip, local, configured, stored).await?;
        let stored = adopt_begun_growth(index, gossip, local, configured, stored).await?;
        let record = stored.configure(configured.to_vec())?;
        let expected = GenerationAnnouncement::of(&record, false);
        for peer in configured.iter().filter(|node| **node != local) {
            if let Some(announced) = gossip.pool_generation(peer).filter(|a| !a.bootstrapping) {
                anyhow::ensure!(announced.current == expected.current,
                    "pool members announced conflicting current generations; refusing to guess ownership");
                if let (Some(a), Some(b)) = (announced.proposed, expected.proposed) {
                    anyhow::ensure!(
                        a == b,
                        "a different pool growth is already pending on node {peer}"
                    );
                }
                anyhow::ensure!(
                    !announced.moving || expected.moving,
                    "pool growth already began on node {peer}; refusing to roll back its state"
                );
            }
        }
        pool_generation::store(index, &record).await?;
        gossip.announce_pool_generation(GenerationAnnouncement::of(&record, false));
        return Ok(record);
    }

    if !previously_known.is_empty() {
        anyhow::ensure!(previously_known.len() == configured.len()
            && previously_known.iter().all(|id| configured.contains(id)),
            "pool generation is unknown and S4_POOL_NODES changed since the previous start: first upgrade on the old roster to record its generation, then append nodes");
    }
    let initial = PoolGenerationRecord::initial(pool, configured.to_vec())?;
    gossip.announce_pool_generation(GenerationAnnouncement::of(&initial, true));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let record = loop {
        let members = gossip.members();
        let mut durable = Vec::new();
        let mut tentative = vec![initial.clone()];
        let mut all_announced = true;
        let mut legacy = false;
        for node in configured.iter().filter(|node| **node != local) {
            match gossip.pool_generation(node) {
                Some(announcement) => {
                    let resolved = announcement.resolve(pool, configured).map_err(|error| {
                        roster_refused(error, &announcement, *node, gossip, local, pool, configured)
                    })?;
                    if !announcement.bootstrapping {
                        durable.push(resolved);
                    } else {
                        tentative.push(resolved);
                    }
                }
                None => match members.get(node) {
                    Some(state)
                        if !state.meta.s4_version.is_empty()
                            && state.meta.protocol_version
                                < s4_cluster::protocol::POOL_GENERATION_MIN_PROTOCOL_VERSION =>
                    {
                        legacy = true
                    }
                    _ => all_announced = false,
                },
            }
        }
        if let Some(record) = choose_record(&durable)? {
            break record.configure(configured.to_vec())?;
        }
        if all_announced {
            anyhow::ensure!(!legacy || !previously_known.is_empty(),
                "pool generation is unknown: this new node must wait until existing pool members record their generation at protocol 9 before appending S4_POOL_NODES");
            // A blank node can join while older owners are recording their
            // first generation. Their shorter, exact prefix wins over the
            // joining node's tentative enlarged roster.
            let oldest = tentative
                .into_iter()
                .min_by_key(|r| r.current.nodes.len())
                .context("no initial generation claim")?;
            break oldest.configure(configured.to_vec())?;
        }
        anyhow::ensure!(tokio::time::Instant::now() < deadline,
            "pool generation is unknown: waiting for the existing owners' generation announcements; refusing to build a ring from an unconfirmed roster");
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    pool_generation::store(index, &record).await?;
    gossip.announce_pool_generation(GenerationAnnouncement::of(&record, false));
    Ok(record)
}

/// A node that stopped while its pool moved, and whose peers announce that
/// the pool settled on the successor since, settles too before it places
/// anything: the pool settles only after every member applied every range,
/// so nothing is left for this node to do in the move.
async fn adopt_settled_growth(
    index: &IndexDb,
    gossip: &GossipHandle,
    local: NodeId,
    configured: &[NodeId],
    stored: PoolGenerationRecord,
) -> anyhow::Result<PoolGenerationRecord> {
    if !stored.moving {
        return Ok(stored);
    }
    let finished = stored.finish()?;
    let settled = GenerationAnnouncement::of(&finished, false);
    let announced = configured.iter().filter(|node| **node != local).any(|node| {
        gossip.pool_generation(node).is_some_and(|a| {
            !a.bootstrapping && !a.moving && a.proposed.is_none() && a.current == settled.current
        })
    });
    if !announced {
        return Ok(stored);
    }
    let to_epoch = finished.current.epoch;
    let mark = FinishedMark {
        format: 1,
        finished_at: s4_cluster::HlcTimestamp::from_record(
            s4_cluster::clock::hlc::now_millis(),
            0,
            local.0,
        ),
    };
    index
        .store_rebalance_entry(&FinishedMark::record_name(to_epoch), mark.encode()?)
        .await?;
    pool_generation::store(index, &finished).await?;
    info!(
        epoch = to_epoch,
        "the pool settled on its grown generation while this node was away; it settles too"
    );
    Ok(finished)
}

/// A node that stopped while its pool's growth was prepared, and whose peers
/// announce that the move of that same growth began since, begins it too
/// before it places anything: the move began once every member announced the
/// grown generation, this node included, and the members that began it
/// already write to both generations and fence the earlier one. Refusing to
/// start, as a node whose state would roll the move back, would keep it out
/// of the pool for good, and the move — which needs every member — with it.
async fn adopt_begun_growth(
    index: &IndexDb,
    gossip: &GossipHandle,
    local: NodeId,
    configured: &[NodeId],
    stored: PoolGenerationRecord,
) -> anyhow::Result<PoolGenerationRecord> {
    if stored.moving || stored.proposed.is_none() {
        return Ok(stored);
    }
    let prepared = GenerationAnnouncement::of(&stored, false);
    let began = configured.iter().filter(|node| **node != local).any(|node| {
        gossip.pool_generation(node).is_some_and(|a| {
            !a.bootstrapping
                && a.moving
                && a.current == prepared.current
                && a.proposed.is_some()
                && a.proposed == prepared.proposed
        })
    });
    if !began {
        return Ok(stored);
    }
    let begun = stored.begin()?;
    pool_generation::store(index, &begun).await?;
    info!(
        epoch = begun.proposed.as_ref().map_or(0, |next| next.epoch),
        "the pool's move began while this node was away; it begins too"
    );
    Ok(begun)
}

/// The progress name of the hand-over of copies a node kept from before
/// pool generations (`EPGR-04-4`), and of their removal after it.
const FORMER_OWNER_HAND_OVER: &str = "legacy/hand-over";
const FORMER_OWNER_CLEAN_UP: &str = "legacy/clean-up";

/// Marks the hand-over of the copies this node may keep from a pool grown
/// before pool generations: it runs once, from this start on, until done.
pub async fn mark_former_owner_pass(store: &dyn ProgressStore) -> anyhow::Result<()> {
    if ForeignProgress::load(store, FORMER_OWNER_HAND_OVER).await?.is_none() {
        store.save(FORMER_OWNER_HAND_OVER, ForeignProgress::default().encode()?).await?;
    }
    Ok(())
}

/// A new coordinator inherits the old owners' settled bucket metadata layout.
/// Starting from `Legacy` would read the wrong replicas after an earlier RF
/// move. An ongoing metadata move finishes on the old owners before growth.
pub async fn inherit_bucket_metadata(
    index: &IndexDb,
    gossip: &GossipHandle,
    local: NodeId,
    generation: &PoolGenerationRecord,
) -> anyhow::Result<()> {
    use s4_cluster::metadata_placement::store;
    if generation.current.nodes.contains(&local) || store::load(index).await?.is_some() {
        return Ok(());
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let members = gossip.members();
        let mut settled = None;
        let agreed = generation.current.nodes.iter().all(|node| {
            if !members.get(node).is_some_and(|peer| peer.status == NodeStatus::Alive) {
                return false;
            }
            let Some(record) = gossip
                .bucket_metadata_placement(node)
                .filter(|record| record.stage == Stage::Settled)
            else {
                return false;
            };
            if settled.is_some_and(|known| known != record) {
                return false;
            }
            settled = Some(record);
            true
        });
        if let Some(record) = settled.filter(|_| agreed) {
            store::store(index, &record).await?;
            return Ok(());
        }
        anyhow::ensure!(tokio::time::Instant::now() < deadline,
            "pool growth: waiting for the old owners to announce identical settled bucket metadata placement; refusing to guess the layout");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn choose_record(records: &[PoolGenerationRecord]) -> anyhow::Result<Option<PoolGenerationRecord>> {
    let mut chosen: Option<PoolGenerationRecord> = None;
    for record in records {
        record.validate()?;
        if let Some(current) = &chosen {
            anyhow::ensure!(current.current == record.current && current.pool_id == record.pool_id,
                "pool members announced conflicting current generations; refusing to guess ownership");
            if let (Some(a), Some(b)) = (&current.proposed, &record.proposed) {
                anyhow::ensure!(
                    a == b,
                    "pool members announced conflicting growth proposals"
                );
            }
        }
        if chosen.as_ref().is_none_or(|c| {
            (record.moving, record.proposed.is_some()) > (c.moving, c.proposed.is_some())
        }) {
            chosen = Some(record.clone());
        }
    }
    Ok(chosen)
}

/// The generation gate as this node's gossip view sees it. Readiness alone
/// never switches the active ring; the migration driver must also be ready.
pub fn gate(
    record: &PoolGenerationRecord,
    gossip: &GossipHandle,
    local: NodeId,
) -> Result<(), ClusterError> {
    let members = gossip.members();
    record.gate(|node| {
        let (alive, protocol) = if node == local {
            (true, PROTOCOL_VERSION)
        } else {
            let peer = members.get(&node)?;
            (peer.status == NodeStatus::Alive, peer.meta.protocol_version)
        };
        Some(GenerationPeer {
            alive,
            protocol,
            announcement: gossip.pool_generation(&node),
            metadata_settled: gossip
                .bucket_metadata_placement(&node)
                .is_some_and(|r| r.stage == Stage::Settled),
        })
    })
}

/// How long a publication waits for the owners gossip holds alive to serve.
const OWNERS_READY_TIMEOUT: Duration = Duration::from_secs(30);

/// How long one readiness probe of an owner may take.
const OWNER_PROBE_CONNECT_TIMEOUT: Duration = Duration::from_secs(1);
const OWNER_PROBE_REQUEST_TIMEOUT: Duration = Duration::from_secs(2);

/// Waits until every owner of `owners` that gossip holds alive serves its gRPC
/// and answers `Health` with its own identity, for at most
/// `timeout` ([`OWNERS_READY_TIMEOUT`] when publishing). Returns the owners
/// that did not.
///
/// Gossip holds a node alive before its gRPC server listens: a pool started
/// together sees every member over gossip first. Traffic sent in that moment
/// fails, and its failures, hints and retries are all the pool's metrics show
/// of an ordinary start — every node of a pool restarted together would report
/// peer errors and store hints for a peer that was never down. So the node's
/// own traffic waits for the peers to serve, as a node of other stores waits
/// before it counts a peer in. An owner gossip does not hold alive is not waited
/// for: it is down, and the quorum goes on without it. The probe is not a call
/// of the cluster's client and is not counted as one.
pub(crate) async fn owners_serving(
    owners: &[NodeId],
    local: NodeId,
    gossip: &GossipHandle,
    timeout: Duration,
) -> Vec<NodeId> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let members = gossip.members();
        let mut probes = tokio::task::JoinSet::new();
        for owner in owners.iter().filter(|owner| **owner != local) {
            let Some(state) = members.get(owner).filter(|s| s.status == NodeStatus::Alive) else {
                continue;
            };
            let (owner, addr) = (*owner, state.meta.addr.grpc_addr);
            probes.spawn(async move {
                let answer = s4_cluster::probe_node_identity(
                    addr,
                    OWNER_PROBE_CONNECT_TIMEOUT,
                    OWNER_PROBE_REQUEST_TIMEOUT,
                    None,
                )
                .await;
                (owner, matches!(answer, Ok(id) if id == owner))
            });
        }
        let mut waiting = Vec::new();
        // A probe that panicked answered nothing; every owner is asked again.
        let mut unanswered = false;
        while let Some(probe) = probes.join_next().await {
            match probe {
                Ok((_, true)) => {}
                Ok((owner, false)) => waiting.push(owner),
                Err(_) => unanswered = true,
            }
        }
        if (waiting.is_empty() && !unanswered) || tokio::time::Instant::now() >= deadline {
            return waiting;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Publishes generation state with an HLC through the existing quorum, once
/// the owners gossip holds alive serve (`owners_serving`). Retried in the
/// background so a whole-pool restart needs no peer's RPC to have started
/// first. Preparing a successor does not change placement; starting its move
/// requires the future joint-placement driver.
pub async fn publish(
    record: PoolGenerationRecord,
    gossip: Arc<GossipHandle>,
    local: NodeId,
    read: Arc<QuorumReadCoordinator>,
    write: Arc<QuorumWriteCoordinator>,
    mut shutdown: watch::Receiver<bool>,
) {
    tokio::select! {
        _ = shutdown.wait_for(|stop| *stop) => return,
        waiting = owners_serving(&record.current.nodes, local, &gossip, OWNERS_READY_TIMEOUT) => {
            if !waiting.is_empty() {
                let waiting: Vec<String> = waiting.iter().map(ToString::to_string).collect();
                warn!(owners = %waiting.join(", "), timeout_secs = OWNERS_READY_TIMEOUT.as_secs(),
                    "pool generation published although owners gossip holds alive do not serve yet");
            }
        }
    }
    let key = pool_generation::object_key(record.pool_id);
    let data = match record.encode() {
        Ok(data) => data,
        Err(error) => {
            warn!(%error, "pool generation cannot be encoded");
            return;
        }
    };
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    loop {
        tokio::select! {
            _ = shutdown.changed() => if *shutdown.borrow() { return; },
            _ = interval.tick() => {
                let publish = match read.read("__system__", &key).await {
                    Ok(result) => {
                        match PoolGenerationRecord::decode(&result.data) {
                            Ok(found) if found == record => {
                                info!(epoch = record.current.epoch, "Pool generation already published through quorum");
                                return;
                            }
                            // An owner not yet restarted with the new roster
                            // must not undo another owner's prepared growth.
                            Ok(found) if found.pool_id == record.pool_id
                                && found.current == record.current
                                && !record.moving && record.proposed.is_none() => return,
                            Ok(found) if found.pool_id == record.pool_id
                                && found.current == record.current && !found.moving
                                && found.proposed.is_none() => true,
                            Ok(_) | Err(_) => { warn!("replicated pool generation conflicts with local ownership; refusing to overwrite it"); return; }
                        }
                    }
                    Err(ClusterError::ObjectNotFound { .. }) => true,
                    Err(error) => { warn!(%error, "waiting for quorum before publishing pool generation"); false },
                };
                if publish {
                    match write.write("__system__", &key, &data, "application/json", &HashMap::new()).await {
                        Ok(_) => { info!(epoch = record.current.epoch, "Pool generation published through quorum"); return; }
                        Err(error) => warn!(%error, "pool generation quorum publication deferred"),
                    }
                }
            }
        }
    }
}

/// Placement config keeps the old ring until joint placement can run.
pub fn placement_config(
    config: &s4_cluster::ClusterConfig,
    record: &PoolGenerationRecord,
) -> anyhow::Result<s4_cluster::ClusterConfig> {
    let mut active = config.clone();
    let pool = active
        .pools
        .iter_mut()
        .find(|p| p.pool_id == record.pool_id)
        .context("pool generation names an unconfigured pool")?;
    anyhow::ensure!(
        pool.nodes.len() >= record.current.nodes.len(),
        "configured roster omits current owners"
    );
    pool.nodes.truncate(record.current.nodes.len());
    active.epoch = record.current.epoch;
    Ok(active)
}

/// Whether every node the growth appends has room for its share of the pool
/// with `min_free_ratio` of its disk left free (plan
/// ERASURE-CODING-FIXING-POOL-GROWTH-REBALANCE.md, `EPGR-03-5`). The share is
/// estimated from what this node of the current ring stores now.
///
/// # Errors
///
/// Why the move cannot begin yet: a new node without a reported disk, or one
/// without room, or this node's own storage not measured.
pub fn space_gate(
    record: &PoolGenerationRecord,
    gossip: &GossipHandle,
    stored: &dyn StoredBytes,
    min_free_ratio: f64,
) -> Result<(), String> {
    use s4_cluster::ClusterMembershipQuery;
    let Some(next) = &record.proposed else {
        return Ok(());
    };
    let stored = stored
        .stored_bytes()
        .ok_or_else(|| "this node's stored bytes could not be measured".to_string())?;
    let share = space::share_after_growth(stored, record.current.nodes.len(), next.nodes.len());
    let new_members: Vec<_> = next
        .nodes
        .iter()
        .filter(|node| !record.current.nodes.contains(node))
        .map(|node| {
            (
                *node,
                gossip.node_capacity(*node).map(|report| report.capacity),
            )
        })
        .collect();
    space::check_before_begin(&new_members, share, min_free_ratio)
}

/// Everything the growth of the pool needs on this node.
pub struct GrowthParts {
    /// This node's part in the move.
    pub transition: Arc<PoolTransition>,
    /// Placement of both generations.
    pub router: Arc<PlacementRouter>,
    /// Calls to the other members.
    pub rpc_client: Arc<NodeClient>,
    /// Membership and the members' announcements.
    pub gossip: Arc<GossipHandle>,
    /// Quorum reads: the replicated records, and the versions of keys.
    pub read: Arc<QuorumReadCoordinator>,
    /// Quorum writes of the replicated records.
    pub write: Arc<QuorumWriteCoordinator>,
    /// This node's storage.
    pub storage: Arc<dyn StorageEngine>,
    /// This node's multipart uploads.
    pub multipart_storage: Arc<dyn MultipartReplicaStorage>,
    /// This node's replica, written without the network.
    pub local_replica: LocalReplica,
    /// Where the progress of the move is kept.
    pub store: Arc<dyn ProgressStore>,
    /// What this node stores: the new members need room for their share.
    pub stored_bytes: Arc<dyn StoredBytes>,
    /// The limits of the move.
    pub rebalance: RebalanceConfig,
    /// Tombstone grace period.
    pub gc_grace: Duration,
    /// How long a write placed by the earlier generation may still land.
    pub settle: Duration,
    /// How often the driver looks at the pool.
    pub poll: Duration,
    /// The topology epoch of replica calls.
    pub topology_epoch: u64,
    /// Stops everything the growth runs.
    pub shutdown: watch::Receiver<bool>,
}

/// Starts the driver of the pool's growth on this node: it begins the move
/// once every member is ready (the generation gate, then room on the new
/// members), runs the move of this node's keys from then on, and verifies,
/// cuts over and settles the pool when this node leads.
///
/// # Errors
///
/// Invalid limits of the move.
pub fn start_growth(parts: GrowthParts) -> anyhow::Result<()> {
    parts.rebalance.validate()?;
    let local = parts.transition.local();
    // A coordinator or a hint asks this node to hand an upload over.
    parts.transition.set_uploads(Arc::new(MultipartTransfer::new(
        parts.multipart_storage.clone(),
        parts.rpc_client.clone(),
        parts.router.clone(),
        local,
        parts.topology_epoch,
    )));
    let reconciler = || {
        Reconciler::new(
            parts.read.clone(),
            parts.rpc_client.clone(),
            parts.gossip.clone(),
            local,
            parts.topology_epoch,
        )
        .with_local_replica(parts.local_replica.clone())
    };
    let verifier = RangeVerifier::new(
        parts.transition.clone(),
        parts.router.clone(),
        parts.rpc_client.clone(),
        parts.gossip.clone(),
        Arc::new(QuorumRecords::new(parts.read.clone(), parts.write.clone())),
        parts.read.clone(),
        Arc::new(reconciler()),
        parts.storage.clone(),
        TransitionConfig {
            settle: parts.settle,
            poll: parts.poll,
            page_size: parts.rebalance.page_size.max(64),
            concurrent_ranges: parts.rebalance.concurrent_ranges,
            gc_grace: parts.gc_grace,
            ..TransitionConfig::default()
        },
    );
    let gate = {
        let gossip = parts.gossip.clone();
        let stored = parts.stored_bytes.clone();
        let min_free_ratio = parts.rebalance.min_free_ratio;
        Arc::new(move |record: &PoolGenerationRecord| {
            gate(record, &gossip, local)
                .map_err(|e| e.to_string())
                .and_then(|()| space_gate(record, &gossip, stored.as_ref(), min_free_ratio))
        })
    };
    let on_begin = {
        let started = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let transition = parts.transition.clone();
        let router = parts.router.clone();
        let read = parts.read.clone();
        let rpc_client = parts.rpc_client.clone();
        let gossip = parts.gossip.clone();
        let storage = parts.storage.clone();
        let multipart_storage = parts.multipart_storage.clone();
        let local_replica = parts.local_replica.clone();
        let store = parts.store.clone();
        // The records without digests wait as long as the verification does.
        let config = RebalanceConfig {
            settle: parts.settle,
            ..parts.rebalance.clone()
        };
        let gc_grace = parts.gc_grace;
        let topology_epoch = parts.topology_epoch;
        let shutdown = parts.shutdown.clone();
        Arc::new(move || {
            if started.swap(true, std::sync::atomic::Ordering::SeqCst) {
                return;
            }
            let service = RebalanceService::new(RebalanceParts {
                record: transition.record(),
                local,
                topology_epoch,
                router: router.clone(),
                ranges: transition.ranges().clone(),
                storage: storage.clone(),
                source: read.clone(),
                local_replica: local_replica.clone(),
                multipart: Some(MultipartTransfer::new(
                    multipart_storage.clone(),
                    rpc_client.clone(),
                    router.clone(),
                    local,
                    topology_epoch,
                )),
                rpc_client: rpc_client.clone(),
                gossip: gossip.clone(),
                store: store.clone(),
                load: Arc::new(QuorumLoad),
                config: config.clone(),
                gc_grace,
            });
            match service {
                Ok(service) => {
                    transition.set_rebalance(service.handle());
                    tokio::spawn(service.run(shutdown.clone()));
                }
                Err(error) => warn!(%error, "the move of this node's keys could not start"),
            }
        })
    };
    let driver = TransitionDriver::new(verifier, gate, on_begin);
    tokio::spawn(driver.run(parts.shutdown.clone()));
    tokio::spawn(log_growth(parts.transition.clone(), parts.shutdown.clone()));
    Ok(())
}

/// Logs the milestones of the pool's growth on this node: why it waits, its
/// beginning, the fencing of the earlier generation, the hand-over of this
/// node's keys, every tenth range cut over, and the settling.
async fn log_growth(transition: Arc<PoolTransition>, mut shutdown: watch::Receiver<bool>) {
    let mut phase = None;
    let mut waiting: Option<String> = None;
    let mut fenced = false;
    let mut handed = false;
    let mut cut_over = 0;
    loop {
        let now = transition.phase();
        if phase != Some(now) {
            match now {
                GenerationPhase::Prepared => {}
                GenerationPhase::Moving => info!(
                    from_epoch = transition.source_epoch(),
                    to_epoch = transition.to_epoch(),
                    "Pool growth began: writes reach the quorums of both generations, reads follow each range's cut-over"
                ),
                GenerationPhase::Finished => {
                    info!(
                        epoch = transition.to_epoch(),
                        nodes = transition.record().current.nodes.len(),
                        "Pool growth finished: the grown generation alone places every key"
                    );
                    return;
                }
            }
            phase = Some(now);
        }
        let reason = transition.waiting();
        if reason != waiting {
            if let Some(reason) = &reason {
                info!(%reason, "Pool growth waits");
            }
            waiting = reason;
        }
        if !fenced && transition.fences_earlier_generation() && now == GenerationPhase::Moving {
            fenced = true;
            info!("Every member began the pool's move: coordinators of the earlier generation are fenced");
        }
        if !handed && now == GenerationPhase::Moving && transition.handed_over() {
            handed = true;
            let moved = transition.rebalance().map(|handle| handle.status());
            info!(
                keys_moved = moved.as_ref().map_or(0, |s| s.keys_moved),
                bytes_moved = moved.as_ref().map_or(0, |s| s.bytes_moved),
                "This node handed over what it holds of the earlier generation"
            );
        }
        let counts = transition.ranges().counts();
        if counts.cut_over / 10 != cut_over / 10 {
            info!(
                cut_over = counts.cut_over,
                total = counts.total(),
                "Ranges of the pool's growth read through the grown generation"
            );
        }
        cut_over = counts.cut_over;
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(1)) => {}
            changed = shutdown.changed() => if changed.is_err() || *shutdown.borrow() { return; },
        }
    }
}

/// Everything the clean-up of this node's foreign copies needs.
pub struct CleanUpParts {
    /// The durable generation this node started with.
    pub record: PoolGenerationRecord,
    /// The growth that runs on this node, if one does.
    pub transition: Option<Arc<PoolTransition>>,
    /// This node.
    pub local: NodeId,
    /// Placement: who owns each key now.
    pub router: Arc<PlacementRouter>,
    /// Calls to the owners.
    pub rpc_client: Arc<NodeClient>,
    /// Membership and the members' announcements.
    pub gossip: Arc<GossipHandle>,
    /// Quorum reads of the versions of keys.
    pub read: Arc<QuorumReadCoordinator>,
    /// This node's storage.
    pub storage: Arc<dyn StorageEngine>,
    /// This node's multipart uploads.
    pub multipart_storage: Arc<dyn MultipartReplicaStorage>,
    /// This node's replica, written without the network.
    pub local_replica: LocalReplica,
    /// Where the progress is kept.
    pub store: Arc<dyn ProgressStore>,
    /// Rates of the move, which the clean-up keeps to as well.
    pub rebalance: RebalanceConfig,
    /// Tombstone grace period: copies stay this long after the pool settled.
    pub gc_grace: Duration,
    /// The topology epoch of replica calls.
    pub topology_epoch: u64,
}

impl CleanUpParts {
    /// A pass over this node's foreign copies under `name`. With
    /// `unconfirmed_after`, a clean-up removes the copies the owners do not
    /// confirm once they are that old (after a growth that settled).
    pub(crate) fn copies(
        &self,
        name: &str,
        mode: ForeignMode,
        unconfirmed_after: Option<Duration>,
    ) -> ForeignCopies {
        let reconciler = Reconciler::new(
            Arc::new(WithHolder::new(self.read.clone(), self.local)),
            self.rpc_client.clone(),
            self.gossip.clone(),
            self.local,
            self.topology_epoch,
        )
        .with_local_replica(self.local_replica.clone());
        ForeignCopies::new(ForeignParts {
            local: self.local,
            router: self.router.clone(),
            storage: self.storage.clone(),
            reconciler,
            multipart: Some(MultipartTransfer::new(
                self.multipart_storage.clone(),
                self.rpc_client.clone(),
                self.router.clone(),
                self.local,
                self.topology_epoch,
            )),
            rpc_client: self.rpc_client.clone(),
            topology_epoch: self.topology_epoch,
            store: self.store.clone(),
            name: name.to_string(),
            mode,
            load: Arc::new(QuorumLoad),
            max_keys_per_sec: u64::from(self.rebalance.max_keys_per_sec),
            page_size: self.rebalance.page_size,
            retry_after: self.rebalance.retry_after,
            gc_grace: self.gc_grace,
            unconfirmed_after,
        })
    }

    /// Why the copies of the members of `record` are not removed yet: a
    /// member that does not announce the same settled generation as this
    /// node, or the grace period since `since_ms` not over.
    fn ready_to_remove(&self, record: &PoolGenerationRecord, since_ms: u64) -> Result<(), String> {
        let ours = GenerationAnnouncement::of(record, false);
        for node in &record.current.nodes {
            let settled = self.gossip.pool_generation(node).is_some_and(|announced| {
                !announced.bootstrapping
                    && !announced.moving
                    && announced.proposed.is_none()
                    && announced.current == ours.current
            });
            if !settled {
                return Err(format!(
                    "waiting for node {node} to announce the settled generation {}",
                    record.current.epoch
                ));
            }
        }
        let grace = u64::try_from(self.gc_grace.as_millis()).unwrap_or(u64::MAX);
        let due = since_ms.saturating_add(grace);
        let now = s4_cluster::clock::hlc::now_millis();
        if now < due {
            return Err(format!(
                "copies of keys this node no longer owns stay {} s more, the grace period",
                (due - now).div_ceil(1000)
            ));
        }
        Ok(())
    }
}

/// Hands over, then removes after the grace period, the copies of keys
/// this node no longer owns: those a pool grown before generations left
/// here (`EPGR-04-4`), and those of a growth that settled (`EPGR-04-3`).
pub async fn clean_up_foreign_copies(parts: CleanUpParts, mut shutdown: watch::Receiver<bool>) {
    let load = |name: &'static str| {
        let store = parts.store.clone();
        async move { ForeignProgress::load(store.as_ref(), name).await }
    };
    // A pool grown before generations: hand over, then remove.
    match load(FORMER_OWNER_HAND_OVER).await {
        Ok(Some(progress)) => {
            let handed = if progress.phase == s4_cluster::rebalance::progress::Phase::Done {
                Some(progress)
            } else {
                info!("handing over the copies this node may keep of keys a pool grown before generations gave to others");
                parts
                    .copies(FORMER_OWNER_HAND_OVER, ForeignMode::HandOver, None)
                    .run(|| Ok(()), shutdown.clone())
                    .await
            };
            let Some(handed) = handed else {
                return;
            };
            if handed.foreign > 0 {
                info!(
                    foreign = handed.foreign,
                    handed = handed.handed,
                    kept = handed.kept,
                    "Copies kept from before pool generations were handed over to their owners"
                );
            }
            let since = handed.done_at.unwrap_or_else(s4_cluster::clock::hlc::now_millis);
            // The record as it stands now, not as this node started: a node
            // that joined a growth starts without a record of its own, marks
            // this pass, and may remove its copies once the growth settled —
            // waiting on the record of its start would wait for ever, and keep
            // it from the clean-up of every later growth.
            let ready = || {
                let record = match &parts.transition {
                    Some(transition) if transition.phase() != GenerationPhase::Finished => {
                        return Err("a growth of the pool is prepared or runs".to_string());
                    }
                    Some(transition) => transition.record(),
                    None => parts.record.clone(),
                };
                if record.proposed.is_some() {
                    return Err("a growth of the pool is prepared or runs".to_string());
                }
                parts.ready_to_remove(&record, since)
            };
            match parts
                .copies(FORMER_OWNER_CLEAN_UP, ForeignMode::CleanUp, None)
                .run(logged(ready), shutdown.clone())
                .await
            {
                Some(cleaned) => info!(
                    removed = cleaned.removed,
                    kept = cleaned.kept,
                    "Copies kept from before pool generations of keys others own were removed"
                ),
                None => return,
            }
        }
        Ok(None) => {}
        Err(error) => {
            warn!(%error, "the hand-over of copies from before pool generations could not be read")
        }
    }

    // A growth that settled: in this run, or before this start.
    let epoch = match &parts.transition {
        Some(transition) => {
            loop {
                if transition.phase() == GenerationPhase::Finished {
                    break;
                }
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(5)) => {}
                    changed = shutdown.changed() => if changed.is_err() || *shutdown.borrow() { return; },
                }
            }
            transition.to_epoch()
        }
        None => parts.record.current.epoch,
    };
    let mark = match FinishedMark::load(parts.store.as_ref(), epoch).await {
        Ok(Some(mark)) => mark,
        Ok(None) => return,
        Err(error) => {
            warn!(%error, "the moment the pool settled could not be read; copies it no longer gives this node stay");
            return;
        }
    };
    let record = match &parts.transition {
        Some(transition) => transition.record(),
        None => parts.record.clone(),
    };
    let since = mark.finished_at.wall_time;
    let name = format!("{epoch}/clean-up");
    let cleaned = parts
        .copies(&name, ForeignMode::CleanUp, Some(parts.gc_grace))
        .run(logged(|| parts.ready_to_remove(&record, since)), shutdown)
        .await;
    if let Some(cleaned) = cleaned {
        info!(
            epoch,
            removed = cleaned.removed,
            kept = cleaned.kept,
            "Copies of keys the grown generation does not give this node were removed"
        );
    }
}

/// `ready`, logging each new reason it gives for waiting.
fn logged(
    ready: impl Fn() -> Result<(), String> + Send + Sync,
) -> impl Fn() -> Result<(), String> + Send + Sync {
    let last = std::sync::Mutex::new(None::<String>);
    move || {
        let result = ready();
        let reason = result.as_ref().err().cloned();
        let mut last = last.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if *last != reason {
            if let Some(reason) = &reason {
                info!(%reason, "The removal of copies this node no longer owns waits");
            }
            *last = reason;
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use s4_cluster::{
        BucketPlacement, ClusterConfig, Edition, NodeAddr, NodeCapacity, NodeMeta, NodeState,
        PlacementRouter, PoolConfig,
    };
    use s4_core::{storage::IndexOptions, types::StorageClass};
    use std::time::Instant;

    fn nodes(n: u128) -> Vec<NodeId> {
        (0..n).map(NodeId).collect()
    }
    /// A growth waits until every appended node reported a disk with room
    /// for its share — half of what a node of three holds when the pool
    /// doubles — and the free ratio on top of it.
    #[test]
    fn growth_waits_for_room_on_every_new_node() {
        struct Stored(u64);
        impl StoredBytes for Stored {
            fn stored_bytes(&self) -> Option<u64> {
                Some(self.0)
            }
        }
        let record = stable().configure(nodes(6)).unwrap();
        let gossip = gossip(6);
        let waiting = space_gate(&record, &gossip, &Stored(600), 0.1).unwrap_err();
        assert!(waiting.contains("report"), "{waiting}");
        let disk = |gossip: &GossipHandle, node: u128, used: u64| {
            let mut state = gossip.get_node(&NodeId(node)).unwrap();
            state.meta.capacity = NodeCapacity {
                total_bytes: 1000,
                used_bytes: used,
                volume_count: 0,
                object_count: 0,
            };
            gossip.set_member_for_testing(NodeId(node), state);
        };
        for node in 3..6 {
            disk(&gossip, node, 500);
        }
        assert_eq!(space_gate(&record, &gossip, &Stored(600), 0.1), Ok(()));
        disk(&gossip, 5, 700);
        let full = space_gate(&record, &gossip, &Stored(600), 0.1).unwrap_err();
        assert!(full.contains(&NodeId(5).to_string()), "{full}");
        assert_eq!(space_gate(&stable(), &gossip, &Stored(600), 0.1), Ok(()));
    }

    fn stable() -> PoolGenerationRecord {
        PoolGenerationRecord::initial(PoolId(0), nodes(3)).unwrap()
    }
    fn gossip(n: u128) -> GossipHandle {
        GossipHandle::new_for_testing(
            (1..n)
                .map(|id| {
                    let addr = "127.0.0.1:1".parse().unwrap();
                    (
                        NodeId(id),
                        NodeState {
                            meta: NodeMeta {
                                node_id: NodeId(id),
                                node_name: id.to_string(),
                                addr: NodeAddr {
                                    grpc_addr: addr,
                                    http_addr: addr,
                                    gossip_addr: addr,
                                },
                                pool_id: PoolId(0),
                                topology_epoch: 1,
                                protocol_version: PROTOCOL_VERSION,
                                capacity: NodeCapacity::default(),
                                started_at: 0,
                                s4_version: "test".into(),
                                edition: Edition::Enterprise,
                            },
                            status: NodeStatus::Alive,
                            last_seen: Instant::now(),
                        },
                    )
                })
                .collect(),
        )
    }
    fn index(dir: &std::path::Path) -> IndexDb {
        IndexDb::open(
            dir,
            IndexOptions {
                version_digests: true,
            },
        )
        .unwrap()
    }

    #[tokio::test]
    async fn a_blank_node_learns_the_old_ring_before_constructing_placement() {
        let dir = tempfile::tempdir().unwrap();
        let index = index(&dir.path().join("index"));
        let gossip = gossip(6);
        gossip.set_pool_generation_for_testing(
            NodeId(1),
            Some(GenerationAnnouncement::of(&stable(), false)),
        );
        let record = start(&index, &gossip, NodeId(0), PoolId(0), &nodes(6), None, &[])
            .await
            .unwrap();
        assert_eq!(record, stable().configure(nodes(6)).unwrap());
        assert_eq!(
            pool_generation::load(&index).await.unwrap(),
            Some(record.clone())
        );
        assert_eq!(
            gossip.pool_generation(&NodeId(0)),
            Some(GenerationAnnouncement::of(&record, false))
        );
    }

    /// A node that stopped while the pool moved, and whose peers announce
    /// the pool settled since, starts settled on the grown generation, with
    /// the moment it settled kept for the clean-up's grace period. Without
    /// such an announcement it starts moving, as it stopped.
    #[tokio::test]
    async fn a_node_back_after_the_pool_settled_starts_settled() {
        let dir = tempfile::tempdir().unwrap();
        let index = index(&dir.path().join("index"));
        let moving = stable().configure(nodes(6)).unwrap().begin().unwrap();
        pool_generation::store(&index, &moving).await.unwrap();
        let gossip = gossip(6);
        let resumed = start(
            &index,
            &gossip,
            NodeId(0),
            PoolId(0),
            &nodes(6),
            Some(moving.clone()),
            &nodes(3),
        )
        .await
        .unwrap();
        assert_eq!(resumed, moving);
        let settled = moving.finish().unwrap();
        gossip.set_pool_generation_for_testing(
            NodeId(4),
            Some(GenerationAnnouncement::of(&settled, false)),
        );
        let record = start(
            &index,
            &gossip,
            NodeId(0),
            PoolId(0),
            &nodes(6),
            Some(moving),
            &nodes(6),
        )
        .await
        .unwrap();
        assert_eq!(record, settled);
        assert_eq!(pool_generation::load(&index).await.unwrap(), Some(settled));
        let store = s4_cluster::rebalance::IndexProgressStore::new(Arc::new(index));
        assert!(FinishedMark::load(&store, 2).await.unwrap().is_some());
    }

    /// A node that stopped while the growth was prepared, and whose peers
    /// announce that the move of that growth began since, starts moving:
    /// before, it refused to start, as one rolling the move back, and stayed
    /// out of the pool and the move for good (scenario 61, group D). Peers
    /// still prepared leave it prepared, and a peer moving to another
    /// generation is refused.
    #[tokio::test]
    async fn a_node_back_after_the_move_began_begins_too() {
        let dir = tempfile::tempdir().unwrap();
        let index = index(&dir.path().join("index"));
        let prepared = stable().configure(nodes(6)).unwrap();
        pool_generation::store(&index, &prepared).await.unwrap();
        let gossip = gossip(6);
        gossip.set_pool_generation_for_testing(
            NodeId(4),
            Some(GenerationAnnouncement::of(&prepared, false)),
        );
        let still = start(
            &index,
            &gossip,
            NodeId(5),
            PoolId(0),
            &nodes(6),
            Some(prepared.clone()),
            &nodes(6),
        )
        .await
        .unwrap();
        assert_eq!(still, prepared);

        let moving = prepared.begin().unwrap();
        gossip.set_pool_generation_for_testing(
            NodeId(4),
            Some(GenerationAnnouncement::of(&moving, false)),
        );
        let record = start(
            &index,
            &gossip,
            NodeId(5),
            PoolId(0),
            &nodes(6),
            Some(prepared.clone()),
            &nodes(6),
        )
        .await
        .unwrap();
        assert_eq!(record, moving);
        assert_eq!(pool_generation::load(&index).await.unwrap(), Some(moving));

        let other = stable().configure(nodes(5)).unwrap().begin().unwrap();
        gossip.set_pool_generation_for_testing(
            NodeId(4),
            Some(GenerationAnnouncement::of(&other, false)),
        );
        assert!(start(
            &index,
            &gossip,
            NodeId(5),
            PoolId(0),
            &nodes(6),
            Some(prepared),
            &nodes(6),
        )
        .await
        .is_err());
    }

    #[tokio::test]
    async fn a_saved_generation_can_restart_while_new_nodes_are_down() {
        let dir = tempfile::tempdir().unwrap();
        let index = index(&dir.path().join("index"));
        let gossip = gossip(1);
        let record = start(
            &index,
            &gossip,
            NodeId(0),
            PoolId(0),
            &nodes(6),
            Some(stable()),
            &nodes(3),
        )
        .await
        .unwrap();
        assert_eq!(record.current, stable().current);
        assert!(gate(&record, &gossip, NodeId(0)).is_err());
        assert!(start(
            &index,
            &gossip,
            NodeId(0),
            PoolId(0),
            &nodes(9),
            Some(record.clone()),
            &nodes(3)
        )
        .await
        .is_err());
        assert_eq!(pool_generation::load(&index).await.unwrap(), Some(record));
    }

    #[tokio::test]
    async fn first_upgrade_cannot_install_a_grown_ring_while_legacy_owners_remain() {
        let dir = tempfile::tempdir().unwrap();
        let index = index(&dir.path().join("index"));
        let gossip = gossip(6);
        for id in 1..3 {
            let mut peer = gossip.get_node(&NodeId(id)).unwrap();
            peer.meta.protocol_version = 8;
            gossip.set_member_for_testing(NodeId(id), peer);
        }
        for id in 3..6 {
            let initial = PoolGenerationRecord::initial(PoolId(0), nodes(6)).unwrap();
            gossip.set_pool_generation_for_testing(
                NodeId(id),
                Some(GenerationAnnouncement::of(&initial, true)),
            );
        }
        assert!(start(
            &index,
            &gossip,
            NodeId(0),
            PoolId(0),
            &nodes(6),
            None,
            &nodes(3)
        )
        .await
        .is_err());
        assert!(pool_generation::load(&index).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn fresh_nodes_with_different_start_orders_agree_on_the_old_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let index = index(&dir.path().join("index"));
        let gossip = gossip(6);
        for id in 1..6 {
            let claim = if id < 3 {
                stable()
            } else {
                PoolGenerationRecord::initial(PoolId(0), nodes(6)).unwrap()
            };
            gossip.set_pool_generation_for_testing(
                NodeId(id),
                Some(GenerationAnnouncement::of(&claim, true)),
            );
        }
        let record = start(&index, &gossip, NodeId(0), PoolId(0), &nodes(6), None, &[])
            .await
            .unwrap();
        assert_eq!(record.current, stable().current);
    }

    #[test]
    fn conflicting_durable_proposals_are_refused_without_selecting_by_hlc() {
        let six = stable().configure(nodes(6)).unwrap();
        let nine = stable().configure(nodes(9)).unwrap();
        assert!(choose_record(&[six.clone(), nine]).is_err());
        assert_eq!(choose_record(&[stable(), six.clone()]).unwrap(), Some(six));
    }

    #[tokio::test]
    async fn a_saved_owner_cannot_propose_a_second_growth_announced_by_a_peer() {
        let dir = tempfile::tempdir().unwrap();
        let index = index(&dir.path().join("index"));
        let gossip = gossip(9);
        let first = stable().configure(nodes(6)).unwrap();
        gossip.set_pool_generation_for_testing(
            NodeId(1),
            Some(GenerationAnnouncement::of(&first, false)),
        );
        assert!(start(
            &index,
            &gossip,
            NodeId(0),
            PoolId(0),
            &nodes(9),
            Some(stable()),
            &nodes(3)
        )
        .await
        .is_err());
        assert!(pool_generation::load(&index).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn a_new_coordinator_inherits_settled_metadata_instead_of_starting_at_legacy() {
        use s4_cluster::metadata_placement::{store, PlacementRecord, QuorumShape};
        let dir = tempfile::tempdir().unwrap();
        let index = index(&dir.path().join("index"));
        let gossip = gossip(7);
        let current: Vec<_> = (1..7).map(NodeId).collect();
        let mut configured = current.clone();
        configured.push(NodeId(0));
        let growing = PoolGenerationRecord::initial(PoolId(0), current.clone())
            .unwrap()
            .configure(configured)
            .unwrap();
        let settled = PlacementRecord::settled(QuorumShape::majority(5));
        for node in current {
            gossip.set_bucket_metadata_placement_for_testing(node, Some(settled));
        }
        inherit_bucket_metadata(&index, &gossip, NodeId(0), &growing).await.unwrap();
        let resumed = crate::bucket_metadata_placement::start(
            &index,
            &gossip,
            QuorumShape::majority(3),
            QuorumShape::majority(5),
        )
        .await
        .unwrap();
        assert_eq!(resumed, settled);
        assert_eq!(store::load(&index).await.unwrap(), Some(settled));
    }

    #[tokio::test]
    async fn a_new_coordinator_waits_for_metadata_owners_to_finish_their_move() {
        use s4_cluster::metadata_placement::{store, PlacementRecord, QuorumShape};
        let dir = tempfile::tempdir().unwrap();
        let index = index(&dir.path().join("index"));
        let gossip = gossip(4);
        let growing =
            PoolGenerationRecord::initial(PoolId(0), vec![NodeId(1), NodeId(2), NodeId(3)])
                .unwrap()
                .configure(vec![NodeId(1), NodeId(2), NodeId(3), NodeId(0)])
                .unwrap();
        for id in 1..4 {
            let record = if id == 3 {
                PlacementRecord::starting(QuorumShape::majority(1), QuorumShape::majority(3))
            } else {
                PlacementRecord::settled(QuorumShape::majority(3))
            };
            gossip.set_bucket_metadata_placement_for_testing(NodeId(id), Some(record));
        }
        assert!(tokio::time::timeout(
            Duration::from_millis(150),
            inherit_bucket_metadata(&index, &gossip, NodeId(0), &growing)
        )
        .await
        .is_err());
        assert!(store::load(&index).await.unwrap().is_none());
    }

    #[test]
    fn a_new_coordinator_routes_every_replication_class_to_old_owners() {
        let addr = "127.0.0.1:1".parse().unwrap();
        let config = ClusterConfig {
            pools: vec![PoolConfig {
                pool_id: PoolId(0),
                name: "pool".into(),
                nodes: vec![
                    NodeAddr {
                        grpc_addr: addr,
                        http_addr: addr,
                        gossip_addr: addr
                    };
                    6
                ],
                replication_factor: Some(3),
                write_quorum: Some(2),
                read_quorum: Some(2),
                storage_class: StorageClass::Standard,
            }],
            ..ClusterConfig::default()
        };
        let growing = stable().configure(nodes(6)).unwrap();
        let active = placement_config(&config, &growing).unwrap();
        let router = PlacementRouter::from_pool_node_ids(
            &active,
            NodeId(5),
            BucketPlacement::new(),
            &HashMap::from([(PoolId(0), nodes(3))]),
        )
        .unwrap();
        for (bucket, key) in [
            ("photos", "cat"),
            ("__system__", "__s4_bucket_marker_photos"),
            ("__system__", "__s4_bucket_cors_photos"),
            ("__system__", "__s4_ec_v1/pools/0/erasure-sets"),
            ("__system__", "__s4_iam_v2_user_alice"),
        ] {
            let decision = router.route(bucket, key).unwrap();
            assert_eq!(decision.replicas.len(), 3);
            assert!(decision.replicas.iter().all(|node| growing.current.nodes.contains(node)));
        }
    }
}
