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

//! SWIM gossip layer built on the [`foca`] crate.
//!
//! The gossip service provides:
//!
//! - **Membership discovery** — nodes find each other via seed addresses
//!   and the SWIM protocol's protocol dissemination.
//! - **Failure detection** — `Alive → Suspect → Dead` transitions with
//!   configurable timeouts (default: suspect after 5 s, dead after 30 s).
//! - **Metadata propagation** — each node periodically broadcasts its
//!   [`NodeMeta`] (capacity, version, pool membership) piggybacked on
//!   SWIM protocol messages.
//!
//! # Architecture
//!
//! ```text
//! ┌──────────────────────────────────────────────┐
//! │               GossipService                  │
//! │                                              │
//! │  ┌────────┐   ┌───────────┐   ┌──────────┐  │
//! │  │  Foca   │◄─│ UDP Socket│──►│  Timers  │  │
//! │  │ (SWIM)  │  └───────────┘   └──────────┘  │
//! │  └────┬───┘                                  │
//! │       │  Notifications                       │
//! │       ▼                                      │
//! │  ClusterEvent channel ──► consumer           │
//! └──────────────────────────────────────────────┘
//! ```

use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use bytes::Bytes;
use foca::{AccumulatingRuntime, BroadcastHandler, Foca, Invalidates, PostcardCodec, Timer};
use rand::rngs::SmallRng;
use rand::SeedableRng;
use serde::{Deserialize, Serialize};
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tracing::{debug, info, trace, warn};

use crate::cluster_metrics::{self, PoolRoster};
use crate::error::ClusterError;
use crate::iam_catch_up::IamAnnouncement;
use crate::iam_config::IamConfigPrint;
use crate::identity::{GossipIdentity, NodeId, NodeMeta, NodeStatus};
use crate::metadata_placement::{PeerPlacement, PlacementRecord, PlacementView};
use crate::pool_generation::GenerationAnnouncement;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Events emitted by the gossip layer when cluster membership changes.
#[derive(Debug)]
pub enum ClusterEvent {
    /// A new node has been discovered and is alive.
    Joined(NodeId, Box<NodeMeta>),
    /// A node has left the cluster (graceful or declared dead).
    Left(NodeId),
    /// A node is suspected of being unreachable.
    Suspected(NodeId),
    /// A previously-suspected node has recovered.
    Recovered(NodeId),
}

/// Snapshot of a remote node's state as observed locally.
#[derive(Debug, Clone)]
pub struct NodeState {
    /// Latest metadata received from the node.
    pub meta: NodeMeta,
    /// Current liveness status.
    pub status: NodeStatus,
    /// Wall-clock time when we last heard from this node.
    pub last_seen: Instant,
}

// ---------------------------------------------------------------------------
// Gossip configuration
// ---------------------------------------------------------------------------

/// Tuning knobs for the gossip layer.
#[derive(Debug, Clone)]
pub struct GossipConfig {
    /// UDP address to bind the gossip socket to.
    pub bind_addr: SocketAddr,
    /// Seed node gossip addresses for initial discovery.
    pub seeds: Vec<SocketAddr>,
    /// How often a node re-announces itself to the seeds it does not see alive.
    ///
    /// An announcement is a UDP datagram: if the seed's socket is not bound
    /// yet, it is silently lost, and nothing else would ever bring this node
    /// into the cluster. Only a seed receives announcements, so no peer would
    /// discover it either. The same announcements bring the two sides of a
    /// healed network split back together.
    pub seed_reannounce_interval: Duration,
    /// How often to broadcast local metadata (default: 30 s).
    pub meta_broadcast_interval: Duration,
    /// Maximum UDP packet size (default: 1400 bytes to stay under MTU).
    pub max_packet_size: usize,
}

impl Default for GossipConfig {
    fn default() -> Self {
        Self {
            bind_addr: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 9200)),
            seeds: Vec::new(),
            meta_broadcast_interval: Duration::from_secs(30),
            // Short, because it only sends while a seed is missing: one
            // datagram per missing seed, none once the whole pool is up.
            seed_reannounce_interval: Duration::from_secs(1),
            max_packet_size: 1400,
        }
    }
}

// ---------------------------------------------------------------------------
// Internal: metadata broadcast types
// ---------------------------------------------------------------------------

/// Wrapper sent via foca's custom broadcast channel.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct MetaBroadcast {
    node_id: NodeId,
    meta: NodeMeta,
    /// Monotonically increasing version so receivers can discard stale data.
    version: u64,
}

/// How recent one metadata broadcast is, against the other broadcasts of the
/// same node.
///
/// The version alone cannot answer that. It counts broadcasts inside one
/// process and starts again at zero when the node restarts, so a node that has
/// been up for a month leaves its peers remembering a version its next run
/// needs a month of broadcasts to reach — and until it got there, every
/// broadcast from the live process would read as stale and be dropped. The
/// address, the protocol version and the capacity a peer holds for that node
/// would then stay at whatever the previous run last said, which is how a
/// restarted node keeps being addressed at the IP it no longer has.
///
/// The generation is the node's process start time (`NodeMeta::started_at`,
/// epoch seconds), which orders runs the way the version orders broadcasts
/// inside a run. It is already on the wire, so nothing about the broadcast
/// format changes and a node from an older build is ordered by it just the
/// same.
///
/// A clock that goes backwards over a restart breaks the order, exactly as it
/// breaks foca's own incarnation comparison; that limit is real and is not
/// hidden here. Two runs that start inside the same second share a generation
/// and fall back to comparing versions, which resolves itself after a couple of
/// broadcasts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct MetaStamp {
    /// Process start time of the node that sent the broadcast, epoch seconds.
    generation: u64,
    /// Broadcast counter inside that process.
    version: u64,
}

/// What a metadata broadcast carries after [`MetaBroadcast`]. A node of an
/// older release decodes the broadcast with `postcard::from_bytes`, which
/// stops at the end of what it knows and ignores what follows, and foca
/// relays a broadcast as the bytes it received: the tail reaches every node
/// unchanged. A node that sent none — an older release — reads as the default.
///
/// On the wire the tail is a sequence of parts, each added by a release after
/// the parts before it and never changed: [`AdoptionTail`] (protocol 6), then
/// [`PlacementTail`], then [`IamTail`] (protocol 8). A node reads the parts it
/// knows and ignores the rest, and a part missing reads as its default.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct MetaTail {
    /// The node adopted the bucket configurations it stored before protocol
    /// 6 (`ConfigAdoption`).
    bucket_configs_adopted: bool,
    /// Where the node is in the move of its pool's bucket metadata, `None`
    /// from a node that announces none — of an earlier release, or before
    /// its first record.
    bucket_metadata_placement: Option<PlacementRecord>,
    /// The state of the node's copy of IAM, `None` from a node that
    /// announces none — of an earlier release, or before it read its copy.
    iam: Option<IamAnnouncement>,
    /// The node keeps no IAM of its own that its pool has not taken over
    /// (plan CLUSTER-IAM-REPLICATION.md, decision 8).
    iam_adopted: bool,
    /// Fingerprints of the node's IAM configuration, `None` from a node that
    /// announces none (plan CLUSTER-IAM-REPLICATION.md, decision 6).
    iam_config: Option<IamConfigPrint>,
    /// Durable pool generations (protocol 9), separate from topology in use.
    pool_generation: Option<GenerationAnnouncement>,
}

/// The first part of [`MetaTail`] on the wire.
#[derive(Debug, Serialize, Deserialize)]
struct AdoptionTail {
    bucket_configs_adopted: bool,
}

/// The second part of [`MetaTail`] on the wire.
#[derive(Debug, Serialize, Deserialize)]
struct PlacementTail {
    bucket_metadata_placement: Option<PlacementRecord>,
}

/// The third part of [`MetaTail`] on the wire.
#[derive(Debug, Serialize, Deserialize)]
struct IamTail {
    iam: Option<IamAnnouncement>,
    iam_adopted: bool,
    iam_config: Option<IamConfigPrint>,
}

/// The fourth, fixed-size part of the metadata tail (protocol 9).
#[derive(Debug, Serialize, Deserialize)]
struct GenerationTail {
    pool_generation: Option<GenerationAnnouncement>,
}

/// The bytes of a metadata broadcast: [`MetaBroadcast`], then [`MetaTail`].
fn encode_meta(broadcast: &MetaBroadcast, tail: MetaTail) -> postcard::Result<Vec<u8>> {
    let mut data = postcard::to_allocvec(broadcast)?;
    data.extend(postcard::to_allocvec(&AdoptionTail {
        bucket_configs_adopted: tail.bucket_configs_adopted,
    })?);
    data.extend(postcard::to_allocvec(&PlacementTail {
        bucket_metadata_placement: tail.bucket_metadata_placement,
    })?);
    data.extend(postcard::to_allocvec(&IamTail {
        iam: tail.iam,
        iam_adopted: tail.iam_adopted,
        iam_config: tail.iam_config,
    })?);
    data.extend(postcard::to_allocvec(&GenerationTail {
        pool_generation: tail.pool_generation,
    })?);
    Ok(data)
}

/// A metadata broadcast and its tail: a part of the tail the sender put none
/// of, or one this release cannot read, reads as its default, and so does
/// every part after it. What the broadcast itself says — the node's
/// protocol, capacity and addresses — counts whatever a later release adds
/// after it.
fn decode_meta(data: &[u8]) -> postcard::Result<(MetaBroadcast, MetaTail)> {
    let (broadcast, rest) = postcard::take_from_bytes::<MetaBroadcast>(data)?;
    let mut tail = MetaTail::default();
    if rest.is_empty() {
        return Ok((broadcast, tail));
    }
    let not_understood = |e: postcard::Error| {
        debug!(node_id = %broadcast.node_id, error = %e, "metadata tail not understood, ignored");
    };
    let rest = match postcard::take_from_bytes::<AdoptionTail>(rest) {
        Ok((adoption, rest)) => {
            tail.bucket_configs_adopted = adoption.bucket_configs_adopted;
            rest
        }
        Err(e) => {
            not_understood(e);
            return Ok((broadcast, tail));
        }
    };
    if rest.is_empty() {
        return Ok((broadcast, tail));
    }
    let rest = match postcard::take_from_bytes::<PlacementTail>(rest) {
        Ok((placement, rest)) => {
            tail.bucket_metadata_placement = placement.bucket_metadata_placement;
            rest
        }
        Err(e) => {
            not_understood(e);
            return Ok((broadcast, tail));
        }
    };
    if rest.is_empty() {
        return Ok((broadcast, tail));
    }
    let rest = match postcard::take_from_bytes::<IamTail>(rest) {
        Ok((iam, rest)) => {
            tail.iam = iam.iam;
            tail.iam_adopted = iam.iam_adopted;
            tail.iam_config = iam.iam_config;
            rest
        }
        Err(e) => {
            not_understood(e);
            return Ok((broadcast, tail));
        }
    };
    if !rest.is_empty() {
        match postcard::take_from_bytes::<GenerationTail>(rest) {
            Ok((generation, _)) => tail.pool_generation = generation.pool_generation,
            Err(e) => not_understood(e),
        }
    }
    Ok((broadcast, tail))
}

/// Key used by foca to deduplicate and invalidate stale broadcasts.
#[derive(Debug, Clone, PartialEq, Eq)]
struct MetaBroadcastKey {
    node_id: NodeId,
    stamp: MetaStamp,
}

impl Invalidates for MetaBroadcastKey {
    /// A newer broadcast for the same node supersedes older ones.
    fn invalidates(&self, other: &Self) -> bool {
        self.node_id == other.node_id && self.stamp >= other.stamp
    }
}

/// Handler that processes incoming metadata broadcasts.
struct MetaHandler {
    /// Most recent stamp seen per node — used to drop stale broadcasts.
    seen: HashMap<NodeId, MetaStamp>,
    /// Channel for forwarding new metadata to the async run-loop.
    meta_tx: mpsc::UnboundedSender<(NodeId, NodeMeta, MetaTail)>,
}

impl BroadcastHandler<GossipIdentity> for MetaHandler {
    type Key = MetaBroadcastKey;
    type Error = MetaHandlerError;

    fn receive_item(
        &mut self,
        data: &[u8],
        _sender: Option<&GossipIdentity>,
    ) -> Result<Option<Self::Key>, Self::Error> {
        let (broadcast, tail) = decode_meta(data).map_err(|e| MetaHandlerError(e.to_string()))?;

        let stamp = MetaStamp {
            generation: broadcast.meta.started_at,
            version: broadcast.version,
        };
        if self.seen.get(&broadcast.node_id).is_some_and(|seen| stamp <= *seen) {
            return Ok(None); // stale: an earlier broadcast, or an earlier run
        }

        self.seen.insert(broadcast.node_id, stamp);
        // Forward to async loop; if the receiver is dropped we just discard.
        let _ = self.meta_tx.send((broadcast.node_id, broadcast.meta, tail));

        Ok(Some(MetaBroadcastKey {
            node_id: broadcast.node_id,
            stamp,
        }))
    }
}

/// Error type for [`MetaHandler`], required by [`BroadcastHandler`].
#[derive(Debug)]
struct MetaHandlerError(String);

impl std::fmt::Display for MetaHandlerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "metadata broadcast error: {}", self.0)
    }
}

impl std::error::Error for MetaHandlerError {}

// ---------------------------------------------------------------------------
// Type aliases
// ---------------------------------------------------------------------------

type S4Foca = Foca<GossipIdentity, PostcardCodec, SmallRng, MetaHandler>;
type S4Runtime = AccumulatingRuntime<GossipIdentity>;

// ---------------------------------------------------------------------------
// Runtime output processing
// ---------------------------------------------------------------------------

/// Collected outputs from one foca interaction.
struct RuntimeOutputs {
    to_send: Vec<(SocketAddr, Bytes)>,
    to_schedule: Vec<(Duration, Timer<GossipIdentity>)>,
    notifications: Vec<foca::OwnedNotification<GossipIdentity>>,
}

fn drain_runtime(runtime: &mut S4Runtime) -> RuntimeOutputs {
    let mut to_send = Vec::new();
    while let Some((identity, data)) = runtime.to_send() {
        to_send.push((foca::Identity::addr(&identity), data));
    }

    let mut to_schedule = Vec::new();
    while let Some((duration, timer)) = runtime.to_schedule() {
        to_schedule.push((duration, timer));
    }

    let mut notifications = Vec::new();
    while let Some(notification) = runtime.to_notify() {
        notifications.push(notification);
    }

    RuntimeOutputs {
        to_send,
        to_schedule,
        notifications,
    }
}

// ---------------------------------------------------------------------------
// GossipService
// ---------------------------------------------------------------------------

/// This node's own storage capacity, as the gossip loop last measured it.
///
/// Kept beside the member map rather than in it, because the local node never
/// appears there: membership is built from what other nodes announce, and a node
/// does not announce itself to itself. Placement asks about every node of an
/// erasure set — this one included — so without this the local node would always
/// read as unmeasured, and capacity-aware placement could never engage in a pool
/// that contains it.
#[derive(Debug, Clone)]
struct LocalCapacity {
    capacity: crate::identity::NodeCapacity,
    measured_at: Instant,
}

/// The pool whose membership this node reports in its metrics, once bootstrap
/// has named it.
///
/// The lock also orders the reports: each one reads the member map only after
/// taking it, so the report of a later change can never be overwritten by that
/// of an earlier one.
type RosterCell = std::sync::Arc<Mutex<Option<PoolRoster>>>;

/// Reports the membership of the roster, if one is set, from the member map as
/// it is now.
fn publish_membership(
    roster: &RosterCell,
    members: &std::sync::Arc<std::sync::RwLock<HashMap<NodeId, NodeState>>>,
) {
    let roster = roster.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(roster) = roster.as_ref() {
        let members = members.read().unwrap_or_else(|poisoned| poisoned.into_inner());
        cluster_metrics::publish_membership(roster, &members);
    }
}

/// Handle returned by [`GossipService::start`] for querying cluster state.
#[derive(Clone)]
pub struct GossipHandle {
    peer_changes: std::sync::Arc<PeerChanges>,
    local_pool_id: crate::identity::PoolId,
    /// Current view of all known cluster members.
    members: std::sync::Arc<std::sync::RwLock<HashMap<NodeId, NodeState>>>,
    /// Identity of the node this handle belongs to.
    local_node_id: NodeId,
    /// This node's own capacity, refreshed by the gossip loop.
    local_capacity: std::sync::Arc<std::sync::RwLock<LocalCapacity>>,
    /// The pool whose membership the gossip loop reports.
    roster: RosterCell,
    /// The peers whose latest metadata says they adopted their bucket
    /// configurations.
    adopted_peers: AdoptedPeers,
    /// Whether this node adopted its own; announced in its metadata.
    adopted_locally: std::sync::Arc<std::sync::atomic::AtomicBool>,
    /// The latest bucket metadata placement each peer announced.
    placement_peers: PlacementPeers,
    /// This node's bucket metadata placement; announced in its metadata.
    placement_local: PlacementLocal,
    /// The latest state of IAM each peer announced.
    iam_peers: IamPeers,
    /// The state of this node's copy of IAM; announced in its metadata.
    iam_local: IamLocal,
    /// The peers whose latest metadata says they keep no IAM of their own.
    iam_adopted_peers: AdoptedPeers,
    /// Whether this node keeps no IAM of its own; announced in its metadata.
    iam_adopted_local: std::sync::Arc<std::sync::atomic::AtomicBool>,
    /// The fingerprints of IAM configuration each peer announced.
    iam_config_peers: IamConfigPeers,
    /// This node's; announced in its metadata.
    iam_config_local: IamConfigLocal,
    generation_peers: GenerationPeers,
    generation_local: GenerationLocal,
    /// Wakes the gossip loop to broadcast this node's metadata at once.
    announce: std::sync::Arc<tokio::sync::Notify>,
}

/// The peers whose latest metadata says they adopted their bucket
/// configurations.
type AdoptedPeers = std::sync::Arc<std::sync::RwLock<std::collections::HashSet<NodeId>>>;

/// The latest bucket metadata placement each peer announced.
type PlacementPeers = std::sync::Arc<std::sync::RwLock<HashMap<NodeId, PlacementRecord>>>;

/// This node's bucket metadata placement, once it has one.
type PlacementLocal = std::sync::Arc<std::sync::RwLock<Option<PlacementRecord>>>;

/// The latest state of IAM each peer announced.
type IamPeers = std::sync::Arc<std::sync::RwLock<HashMap<NodeId, IamAnnouncement>>>;

/// The state of this node's copy of IAM, once it has read it.
type IamLocal = std::sync::Arc<std::sync::RwLock<Option<IamAnnouncement>>>;

/// The fingerprints of IAM configuration each peer announced.
type IamConfigPeers = std::sync::Arc<std::sync::RwLock<HashMap<NodeId, IamConfigPrint>>>;

/// This node's fingerprints of IAM configuration, once set.
type IamConfigLocal = std::sync::Arc<std::sync::RwLock<Option<IamConfigPrint>>>;
type GenerationPeers = std::sync::Arc<std::sync::RwLock<HashMap<NodeId, GenerationAnnouncement>>>;
type GenerationLocal = std::sync::Arc<std::sync::RwLock<Option<GenerationAnnouncement>>>;

/// A projection deliberately omitting capacity, IAM and last-seen timestamps.
#[derive(PartialEq, Eq)]
struct AdoptionPeer {
    node_id: NodeId,
    pool_id: crate::identity::PoolId,
    status: NodeStatus,
    addr: crate::identity::NodeAddr,
    protocol_version: u32,
    s4_version: String,
    topology_epoch: u64,
    started_at: u64,
    identity: Option<GossipIdentity>,
    placement: Option<PlacementRecord>,
    generation: Option<GenerationAnnouncement>,
    adopted: bool,
}

#[derive(Default)]
struct PublishedPeers {
    identities: IdentityTable,
    peers: HashMap<NodeId, AdoptionPeer>,
    local_placement: Option<PlacementRecord>,
    local_generation: Option<GenerationAnnouncement>,
}

struct PeerChanges {
    changed: tokio::sync::watch::Sender<()>,
    /// Serializes snapshot construction and publication, including handle calls.
    published: Mutex<PublishedPeers>,
}

impl Default for PeerChanges {
    fn default() -> Self {
        Self {
            changed: tokio::sync::watch::channel(()).0,
            published: Mutex::default(),
        }
    }
}

impl GossipHandle {
    /// Subscribes to adoption-relevant changes in this node's pool.
    ///
    /// Independent subscribers retain changes received during an adoption pass.
    /// Multiple changes may coalesce into one pending notification.
    pub fn subscribe_peer_changes(&self) -> tokio::sync::watch::Receiver<()> {
        self.publish_peer_changes(None);
        self.peer_changes.changed.subscribe()
    }

    /// Reads current state only after taking the publication lock: a later
    /// change can never be overwritten by an older snapshot from another caller.
    fn publish_peer_changes(&self, identities: Option<&IdentityTable>) {
        let mut published = self.peer_changes.published.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(identities) = identities {
            published.identities.clone_from(identities);
        }
        let roster = self.roster.lock().unwrap_or_else(|p| p.into_inner());
        let members = self.members.read().unwrap_or_else(|p| p.into_inner());
        let placement = self.placement_peers.read().unwrap_or_else(|p| p.into_inner());
        let generation = self.generation_peers.read().unwrap_or_else(|p| p.into_inner());
        let adopted = self.adopted_peers.read().unwrap_or_else(|p| p.into_inner());
        let peers = members
            .iter()
            .filter(|(node, state)| {
                state.meta.pool_id == self.local_pool_id
                    || roster.as_ref().is_some_and(|roster| roster.name_of(**node).is_some())
            })
            .map(|(node, state)| {
                (
                    *node,
                    AdoptionPeer {
                        node_id: state.meta.node_id,
                        pool_id: state.meta.pool_id,
                        status: state.status,
                        addr: state.meta.addr.clone(),
                        protocol_version: state.meta.protocol_version,
                        s4_version: state.meta.s4_version.clone(),
                        topology_epoch: state.meta.topology_epoch,
                        started_at: state.meta.started_at,
                        identity: published.identities.get(node).cloned(),
                        placement: placement.get(node).copied(),
                        generation: generation.get(node).copied(),
                        adopted: adopted.contains(node),
                    },
                )
            })
            .collect();
        let local_placement = *self.placement_local.read().unwrap_or_else(|p| p.into_inner());
        let local_generation = *self.generation_local.read().unwrap_or_else(|p| p.into_inner());
        if published.peers != peers
            || published.local_placement != local_placement
            || published.local_generation != local_generation
        {
            published.peers = peers;
            published.local_placement = local_placement;
            published.local_generation = local_generation;
            // Unlike Notify, watch retains a separate unseen version for every
            // subscriber. send_replace also records the change with no receivers.
            self.peer_changes.changed.send_replace(());
        }
    }

    /// Announces the generations this node durably knows; a bootstrap claim
    /// is tentative and never opens the growth gate.
    pub fn announce_pool_generation(&self, generation: GenerationAnnouncement) {
        *self.generation_local.write().unwrap_or_else(|p| p.into_inner()) = Some(generation);
        self.publish_peer_changes(None);
        self.announce.notify_one();
    }

    /// Latest generation announcement, or none from a pre-protocol-9 peer.
    pub fn pool_generation(&self, node: &NodeId) -> Option<GenerationAnnouncement> {
        if *node == self.local_node_id {
            return *self.generation_local.read().unwrap_or_else(|p| p.into_inner());
        }
        self.generation_peers
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .get(node)
            .copied()
    }

    /// Installs a peer's generation announcement for tests.
    #[cfg(any(test, feature = "test-util"))]
    pub fn set_pool_generation_for_testing(
        &self,
        node: NodeId,
        generation: Option<GenerationAnnouncement>,
    ) {
        record_generation(&self.generation_peers, node, generation);
        self.publish_peer_changes(None);
    }

    /// Records that this node adopted the bucket configurations it stored
    /// before protocol 6, and announces it to the pool at once.
    pub fn announce_bucket_configs_adopted(&self) {
        self.adopted_locally.store(true, std::sync::atomic::Ordering::Release);
        self.announce.notify_one();
    }

    /// Whether `node` adopted its bucket configurations: this node by what it
    /// announced, a peer by its latest metadata. A peer of a release that
    /// does not announce it has not.
    pub fn bucket_configs_adopted(&self, node: &NodeId) -> bool {
        if *node == self.local_node_id {
            return self.adopted_locally.load(std::sync::atomic::Ordering::Acquire);
        }
        self.adopted_peers
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .contains(node)
    }

    /// Has the metadata of `node` say whether it adopted its bucket
    /// configurations, as its broadcast would.
    pub fn set_bucket_configs_adopted_for_testing(&self, node: NodeId, adopted: bool) {
        record_adoption(&self.adopted_peers, node, adopted);
        self.publish_peer_changes(None);
    }

    /// Records where this node is in the move of its pool's bucket metadata,
    /// and announces it to the pool at once.
    pub fn announce_bucket_metadata_placement(&self, record: PlacementRecord) {
        *self.placement_local.write().unwrap_or_else(|poisoned| poisoned.into_inner()) =
            Some(record);
        self.publish_peer_changes(None);
        self.announce.notify_one();
    }

    /// Where `node` is in the move of its pool's bucket metadata: this node
    /// by what it announced, a peer by its latest metadata. `None` for a
    /// peer that announced none, a node of an earlier release among them.
    pub fn bucket_metadata_placement(&self, node: &NodeId) -> Option<PlacementRecord> {
        if *node == self.local_node_id {
            return *self.placement_local.read().unwrap_or_else(|poisoned| poisoned.into_inner());
        }
        self.placement_peers
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(node)
            .copied()
    }

    /// Has the metadata of `node` announce `record`, as its broadcast would.
    pub fn set_bucket_metadata_placement_for_testing(
        &self,
        node: NodeId,
        record: Option<PlacementRecord>,
    ) {
        record_placement(&self.placement_peers, node, record);
        self.publish_peer_changes(None);
    }

    /// Records the state of this node's copy of IAM and announces it to the
    /// pool at once, unless it is what the node announced already.
    pub fn announce_iam(&self, state: IamAnnouncement) {
        {
            let mut local = self.iam_local.write().unwrap_or_else(|poisoned| poisoned.into_inner());
            if *local == Some(state) {
                return;
            }
            *local = Some(state);
        }
        self.announce.notify_one();
    }

    /// The state of the copy of IAM `node` holds: this node's by what it
    /// announced, a peer's by its latest metadata. `None` for a peer that
    /// announced none, a node of an earlier release among them.
    pub fn iam_announcement(&self, node: &NodeId) -> Option<IamAnnouncement> {
        if *node == self.local_node_id {
            return *self.iam_local.read().unwrap_or_else(|poisoned| poisoned.into_inner());
        }
        self.iam_peers
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(node)
            .copied()
    }

    /// Has the metadata of `node` announce `state`, as its broadcast would.
    pub fn set_iam_announcement_for_testing(&self, node: NodeId, state: Option<IamAnnouncement>) {
        record_iam(&self.iam_peers, node, state);
    }

    /// Records whether this node keeps no IAM of its own that its pool has
    /// not taken over, and announces it to the pool at once when it changed.
    pub fn announce_iam_adopted(&self, adopted: bool) {
        let before = self.iam_adopted_local.swap(adopted, std::sync::atomic::Ordering::AcqRel);
        if before != adopted {
            self.announce.notify_one();
        }
    }

    /// Whether `node` keeps no IAM of its own: this node by what it
    /// announced, a peer by its latest metadata. A peer of a release that
    /// does not announce it keeps some.
    pub fn iam_adopted(&self, node: &NodeId) -> bool {
        if *node == self.local_node_id {
            return self.iam_adopted_local.load(std::sync::atomic::Ordering::Acquire);
        }
        self.iam_adopted_peers
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .contains(node)
    }

    /// Has the metadata of `node` say whether it keeps IAM of its own, as its
    /// broadcast would.
    pub fn set_iam_adopted_for_testing(&self, node: NodeId, adopted: bool) {
        record_adoption(&self.iam_adopted_peers, node, adopted);
    }

    /// Records the fingerprints of this node's IAM configuration and
    /// announces them to the pool at once, unless they are what the node
    /// announced already.
    pub fn announce_iam_config(&self, print: IamConfigPrint) {
        {
            let mut local =
                self.iam_config_local.write().unwrap_or_else(|poisoned| poisoned.into_inner());
            if *local == Some(print) {
                return;
            }
            *local = Some(print);
        }
        self.announce.notify_one();
    }

    /// The fingerprints of the IAM configuration of `node`: this node's by
    /// what it announced, a peer's by its latest metadata. `None` for a peer
    /// that announced none, a node of an earlier release among them.
    pub fn iam_config(&self, node: &NodeId) -> Option<IamConfigPrint> {
        if *node == self.local_node_id {
            return *self.iam_config_local.read().unwrap_or_else(|poisoned| poisoned.into_inner());
        }
        self.iam_config_peers
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(node)
            .copied()
    }

    /// Has the metadata of `node` announce `print`, as its broadcast would.
    pub fn set_iam_config_for_testing(&self, node: NodeId, print: Option<IamConfigPrint>) {
        record_iam_config(&self.iam_config_peers, node, print);
    }

    /// Starts reporting the membership of `roster` in `s4_cluster_nodes` and
    /// `s4_cluster_peer_up`: right away, and again after every change gossip
    /// makes to the member map.
    ///
    /// Called once bootstrap has matched every configured member to its
    /// identity; before that, the map cannot be read against the pool.
    pub fn report_membership(&self, roster: PoolRoster) {
        *self.roster.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(roster);
        publish_membership(&self.roster, &self.members);
        self.publish_peer_changes(None);
    }

    /// Return a snapshot of all known member states.
    pub fn members(&self) -> HashMap<NodeId, NodeState> {
        self.members.read().unwrap_or_else(|poisoned| poisoned.into_inner()).clone()
    }

    /// The address a node currently answers gRPC on, as gossip last heard it.
    ///
    /// Separate from [`Self::get_node`] because callers on the RPC path ask this
    /// on every single call — a restarted node can change address at any moment,
    /// and a connection pool that only asks when it has nothing cached keeps
    /// talking to the old address forever. Cloning a whole [`NodeState`], and
    /// with it the node's name and version strings, to read one `SocketAddr`
    /// would put an allocation on that path for nothing.
    pub fn peer_grpc_addr(&self, node_id: &NodeId) -> Option<SocketAddr> {
        self.members
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(node_id)
            .map(|state| state.meta.addr.grpc_addr)
    }

    /// How this node currently sees a peer's liveness.
    ///
    /// The cheap counterpart of [`Self::get_node`], for the same reason as
    /// [`Self::peer_grpc_addr`]: the RPC path asks this before every call, and
    /// cloning a `NodeState` to read one enum copies the peer's name and
    /// version strings with it.
    pub fn peer_status(&self, node_id: &NodeId) -> Option<NodeStatus> {
        self.members
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(node_id)
            .map(|state| state.status)
    }

    /// Look up a specific node's state.
    pub fn get_node(&self, node_id: &NodeId) -> Option<NodeState> {
        self.members
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(node_id)
            .cloned()
    }

    /// Replace one member entry of a handle built for a test.
    ///
    /// Behind the same feature gate as [`Self::new_for_testing`] and for the
    /// same reason: a production binary must not be able to write a membership
    /// view by hand. A test that moves a node to another address needs it,
    /// because that is the event the connection pool has to notice.
    #[cfg(any(test, feature = "test-util"))]
    pub fn set_member_for_testing(&self, node_id: NodeId, state: NodeState) {
        self.members
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(node_id, state);
        self.publish_peer_changes(None);
    }

    /// Drop one member entry of a handle built for a test: a node this view
    /// has never seen, as after a restart while it was down.
    #[cfg(any(test, feature = "test-util"))]
    pub fn remove_member_for_testing(&self, node_id: NodeId) {
        self.members
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&node_id);
        self.publish_peer_changes(None);
    }

    /// Create a `GossipHandle` with pre-populated members (for testing).
    ///
    /// Behind the `test-util` feature so a release build cannot fabricate a
    /// membership view: the EC v2 rolling-upgrade gate reads this map, and a
    /// handcrafted one would answer it with whatever the caller wanted.
    #[cfg(any(test, feature = "test-util"))]
    pub fn new_for_testing(members: HashMap<NodeId, NodeState>) -> Self {
        Self {
            peer_changes: std::sync::Arc::default(),
            local_pool_id: members
                .values()
                .next()
                .map_or(crate::PoolId(0), |state| state.meta.pool_id),
            members: std::sync::Arc::new(std::sync::RwLock::new(members)),
            // No local node: every member of this handle is a peer, so a
            // capacity question about the local id falls through to the map and
            // finds nothing, exactly as it would before the first measurement.
            local_node_id: NodeId(0),
            local_capacity: std::sync::Arc::new(std::sync::RwLock::new(LocalCapacity {
                capacity: crate::identity::NodeCapacity::default(),
                measured_at: Instant::now(),
            })),
            roster: RosterCell::default(),
            adopted_peers: AdoptedPeers::default(),
            adopted_locally: std::sync::Arc::default(),
            placement_peers: PlacementPeers::default(),
            placement_local: PlacementLocal::default(),
            iam_peers: IamPeers::default(),
            iam_local: IamLocal::default(),
            iam_adopted_peers: AdoptedPeers::default(),
            iam_adopted_local: std::sync::Arc::default(),
            iam_config_peers: IamConfigPeers::default(),
            iam_config_local: IamConfigLocal::default(),
            generation_peers: GenerationPeers::default(),
            generation_local: GenerationLocal::default(),
            announce: std::sync::Arc::default(),
        }
    }

    /// Return the number of currently-known alive members.
    pub fn alive_count(&self) -> usize {
        self.members
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .values()
            .filter(|s| s.status == NodeStatus::Alive)
            .count()
    }
}

/// Query interface for cluster node membership and capabilities.
///
/// Exists so that crates which must not depend on gossip internals — EE erasure
/// coding among them — can ask a capability question without taking a type
/// dependency on [`GossipHandle`].
pub trait ClusterMembershipQuery: Send + Sync + 'static {
    /// Whether every node in `pool_nodes` is confirmed to speak EC v2.
    ///
    /// "Confirmed" is the operative word: anything short of a positive answer
    /// from that node itself — it has not joined gossip, it has joined but its
    /// metadata has not arrived yet, it reports an older protocol — counts as
    /// unsupported. The caller uses this to decide whether a migration that
    /// older nodes cannot follow is safe to start, so an unknown node has to
    /// read the same as an old one.
    fn pool_nodes_support_v2(&self, pool_nodes: &[NodeId]) -> bool;

    /// The nodes of `pool_nodes` not confirmed to speak at least
    /// `min_protocol_version`, in the order given.
    ///
    /// "Confirmed" means what it means for [`Self::pool_nodes_support_v2`]: a
    /// node that has not joined, has not broadcast its metadata yet, or reports
    /// an older protocol is listed. A caller that names the nodes in a refusal
    /// uses this rather than a plain verdict.
    ///
    /// An empty answer confirms the pool only when `pool_nodes` is not empty:
    /// "no node is unconfirmed" is also the answer about no nodes, so a caller
    /// has to refuse an empty pool itself.
    ///
    /// Defaults to every node, so a membership view that cannot tell keeps a
    /// capability closed rather than open.
    fn nodes_below_protocol(
        &self,
        pool_nodes: &[NodeId],
        _min_protocol_version: u32,
    ) -> Vec<NodeId> {
        pool_nodes.to_vec()
    }

    /// What a node last told the cluster about its storage, and how old that is.
    ///
    /// Defaults to "nothing known". A membership view without capacity reports
    /// is not a fault: callers that place data by free space are required to
    /// degrade to a capacity-blind rule rather than refuse work, so an absent
    /// answer costs balance and never availability.
    fn node_capacity(&self, _node_id: NodeId) -> Option<NodeCapacityReport> {
        None
    }

    /// How this view currently sees one node, or `None` when it has no entry
    /// for it.
    ///
    /// `None` means "this view cannot tell", and a caller must never render it
    /// as "dead": for a node that never joined and for a node this view does
    /// not track at all, the answer is the same, and only the caller knows
    /// whether it had a membership source in the first place. Report the two
    /// apart; an operator acts differently on "the pool cannot see this node"
    /// than on "this node cannot see anything".
    ///
    /// Defaults to `None` so a view that does not track liveness says so
    /// rather than inventing a status.
    fn node_status(&self, _node_id: NodeId) -> Option<NodeStatus> {
        None
    }
}

/// One node's reported storage capacity, with the age of the report.
///
/// The age is measured from the last time this node heard from that one, which
/// bounds — from our side — how stale the numbers can be. It is not the moment
/// the remote node measured its filesystem, and it cannot be: only the sender
/// knows that, and it does not broadcast it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeCapacityReport {
    /// Capacity figures as the node broadcast them.
    pub capacity: crate::identity::NodeCapacity,
    /// How long ago this node last heard from that one.
    pub age: Duration,
}

impl ClusterMembershipQuery for GossipHandle {
    /// Rolling-upgrade guard (EC2-TIER-05-004).
    ///
    /// The whole body lives in the trait impl rather than in an inherent method
    /// of the same name: an inherent `pool_nodes_support_v2` would win method
    /// resolution here, so a trait impl delegating to `self.…` reads as
    /// recursion to everyone who has not memorised that rule, and becomes real
    /// recursion the day the inherent method is renamed.
    fn pool_nodes_support_v2(&self, pool_nodes: &[NodeId]) -> bool {
        if pool_nodes.is_empty() {
            // Not "every node in an empty set qualifies" — an empty pool means
            // the caller does not know its own membership, and a guard that
            // opens on missing input is not a guard.
            return false;
        }

        // Read under the lock instead of cloning the map: this runs on every
        // tiering cycle and every admin health poll, and `members()` clones a
        // `NodeState` per member.
        let members = self.members.read().unwrap_or_else(|poisoned| poisoned.into_inner());
        pool_nodes.iter().all(|id| {
            members.get(id).is_some_and(|state| {
                confirms_protocol(state, crate::protocol::EC_V2_MIN_PROTOCOL_VERSION)
            })
        })
    }

    fn nodes_below_protocol(
        &self,
        pool_nodes: &[NodeId],
        min_protocol_version: u32,
    ) -> Vec<NodeId> {
        let members = self.members.read().unwrap_or_else(|poisoned| poisoned.into_inner());
        pool_nodes
            .iter()
            .filter(|id| {
                !members
                    .get(id)
                    .is_some_and(|state| confirms_protocol(state, min_protocol_version))
            })
            .copied()
            .collect()
    }

    fn node_capacity(&self, node_id: NodeId) -> Option<NodeCapacityReport> {
        if node_id == self.local_node_id {
            let local = self.local_capacity.read().unwrap_or_else(|poisoned| poisoned.into_inner());
            return Some(NodeCapacityReport {
                capacity: local.capacity.clone(),
                age: local.measured_at.elapsed(),
            });
        }

        let members = self.members.read().unwrap_or_else(|poisoned| poisoned.into_inner());
        members.get(&node_id).map(|state| NodeCapacityReport {
            capacity: state.meta.capacity.clone(),
            age: state.last_seen.elapsed(),
        })
    }

    fn node_status(&self, node_id: NodeId) -> Option<NodeStatus> {
        // The local node is never its own member: it is the observer, and it is
        // answering, so it is alive. Without this it would read as the one
        // status that means "the pool cannot see it".
        if node_id == self.local_node_id {
            return Some(NodeStatus::Alive);
        }

        let members = self.members.read().unwrap_or_else(|poisoned| poisoned.into_inner());
        members.get(&node_id).map(|state| state.status)
    }
}

/// Whether a member has confirmed that it speaks at least `min_protocol_version`.
///
/// An empty version string is the placeholder a member carries between joining
/// gossip and its first metadata broadcast, so it confirms nothing.
fn confirms_protocol(state: &NodeState, min_protocol_version: u32) -> bool {
    !state.meta.s4_version.is_empty() && state.meta.protocol_version >= min_protocol_version
}

impl PlacementView for GossipHandle {
    fn peer_placement(&self, node: NodeId, min_protocol: u32) -> PeerPlacement {
        let Some(state) = self.get_node(&node) else {
            return PeerPlacement::Unknown;
        };
        if state.status != NodeStatus::Alive {
            return PeerPlacement::Down;
        }
        if !confirms_protocol(&state, min_protocol) {
            return PeerPlacement::BelowProtocol {
                confirmed: (!state.meta.s4_version.is_empty())
                    .then_some(state.meta.protocol_version),
            };
        }
        match self.bucket_metadata_placement(&node) {
            Some(record) => PeerPlacement::Announced(record),
            None => PeerPlacement::Silent,
        }
    }
}

/// A [`ClusterMembershipQuery`] that answers with a fixed verdict.
///
/// Behind the `test-util` feature: its whole purpose is to make the EC v2
/// rolling-upgrade gate return a constant, which is exactly what a production
/// binary must not be able to do.
#[cfg(any(test, feature = "test-util"))]
#[derive(Debug, Clone, Default)]
pub struct MockClusterMembership {
    /// The verdict every query gets, regardless of the nodes asked about.
    pub supports_v2: bool,
    /// The protocol version every node is taken to have confirmed. `0`, the
    /// default, confirms nothing.
    pub protocol_version: u32,
}

#[cfg(any(test, feature = "test-util"))]
impl MockClusterMembership {
    /// Create a membership query that always answers `supports_v2`.
    ///
    /// A pool that supports EC v2 is taken to be fully upgraded, so its nodes
    /// confirm this binary's protocol version; one that does not confirms only
    /// the oldest version still spoken.
    pub fn new(supports_v2: bool) -> Self {
        Self {
            supports_v2,
            protocol_version: if supports_v2 {
                crate::protocol::PROTOCOL_VERSION
            } else {
                crate::protocol::MIN_COMPATIBLE_VERSION
            },
        }
    }

    /// Create a membership query whose every node confirms `protocol_version`.
    pub fn with_protocol_version(protocol_version: u32) -> Self {
        Self {
            supports_v2: protocol_version >= crate::protocol::EC_V2_MIN_PROTOCOL_VERSION,
            protocol_version,
        }
    }
}

#[cfg(any(test, feature = "test-util"))]
impl ClusterMembershipQuery for MockClusterMembership {
    fn pool_nodes_support_v2(&self, _pool_nodes: &[NodeId]) -> bool {
        self.supports_v2
    }

    fn nodes_below_protocol(
        &self,
        pool_nodes: &[NodeId],
        min_protocol_version: u32,
    ) -> Vec<NodeId> {
        if self.protocol_version >= min_protocol_version {
            Vec::new()
        } else {
            pool_nodes.to_vec()
        }
    }
}

/// SWIM gossip service for cluster membership and failure detection.
pub struct GossipService;

impl GossipService {
    /// Start the gossip service.
    ///
    /// Binds a UDP socket, creates the foca instance, and spawns the
    /// background run-loop. Returns a [`GossipHandle`] for querying
    /// cluster state and an event receiver for reacting to membership
    /// changes.
    ///
    /// The background task runs until the returned [`GossipHandle`] is
    /// dropped or the tokio runtime shuts down.
    /// `capacity` is how this node measures its own storage. `None` leaves the
    /// capacity in `local_meta` as it is and never refreshes it, which is what a
    /// test wants and what a storage node must not do: a node that never
    /// re-measures reports numbers that age out of the freshness window, and
    /// capacity-aware placement then treats it as unmeasured.
    pub async fn start(
        gossip_config: GossipConfig,
        local_identity: GossipIdentity,
        local_meta: NodeMeta,
        capacity: Option<std::sync::Arc<dyn crate::capacity::NodeCapacityProvider>>,
    ) -> Result<(GossipHandle, mpsc::Receiver<ClusterEvent>), ClusterError> {
        let socket = UdpSocket::bind(gossip_config.bind_addr).await?;
        Self::start_on(socket, gossip_config, local_identity, local_meta, capacity).await
    }

    /// [`Self::start`] on a socket that is already bound. Tests hand over the
    /// socket they reserved the address with: closing it and binding the same
    /// port again leaves a moment in which a test running beside it can take
    /// that port.
    async fn start_on(
        socket: UdpSocket,
        gossip_config: GossipConfig,
        local_identity: GossipIdentity,
        local_meta: NodeMeta,
        capacity: Option<std::sync::Arc<dyn crate::capacity::NodeCapacityProvider>>,
    ) -> Result<(GossipHandle, mpsc::Receiver<ClusterEvent>), ClusterError> {
        let local_addr = socket.local_addr()?;
        info!(%local_addr, node_id = %local_identity.node_id, "gossip service starting");

        let members: std::sync::Arc<std::sync::RwLock<HashMap<NodeId, NodeState>>> =
            std::sync::Arc::new(std::sync::RwLock::new(HashMap::new()));

        let (event_tx, event_rx) = mpsc::channel(256);
        let (meta_tx, meta_rx) = mpsc::unbounded_channel();

        let foca_config = build_foca_config(gossip_config.max_packet_size);

        let handler = MetaHandler {
            seen: HashMap::new(),
            meta_tx,
        };

        let rng = SmallRng::from_os_rng();
        let foca = Foca::with_custom_broadcast(
            local_identity.clone(),
            foca_config,
            rng,
            PostcardCodec,
            handler,
        );

        let local_capacity = std::sync::Arc::new(std::sync::RwLock::new(LocalCapacity {
            capacity: local_meta.capacity.clone(),
            measured_at: Instant::now(),
        }));

        let roster = RosterCell::default();
        let adopted_peers = AdoptedPeers::default();
        let adopted_locally = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let placement_peers = PlacementPeers::default();
        let placement_local = PlacementLocal::default();
        let iam_peers = IamPeers::default();
        let iam_local = IamLocal::default();
        let iam_adopted_peers = AdoptedPeers::default();
        let iam_adopted_local = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let iam_config_peers = IamConfigPeers::default();
        let iam_config_local = IamConfigLocal::default();
        let generation_peers = GenerationPeers::default();
        let generation_local = GenerationLocal::default();
        let announce = std::sync::Arc::new(tokio::sync::Notify::new());
        let handle = GossipHandle {
            peer_changes: std::sync::Arc::default(),
            local_pool_id: local_meta.pool_id,
            members: members.clone(),
            local_node_id: local_identity.node_id,
            local_capacity: local_capacity.clone(),
            roster: roster.clone(),
            adopted_peers: adopted_peers.clone(),
            adopted_locally: adopted_locally.clone(),
            placement_peers: placement_peers.clone(),
            placement_local: placement_local.clone(),
            iam_peers: iam_peers.clone(),
            iam_local: iam_local.clone(),
            iam_adopted_peers: iam_adopted_peers.clone(),
            iam_adopted_local: iam_adopted_local.clone(),
            iam_config_peers: iam_config_peers.clone(),
            iam_config_local: iam_config_local.clone(),
            generation_peers: generation_peers.clone(),
            generation_local: generation_local.clone(),
            announce: announce.clone(),
        };

        tokio::spawn(run_loop(
            handle.clone(),
            socket,
            Mutex::new(foca),
            members,
            roster,
            event_tx,
            meta_rx,
            local_identity,
            local_meta,
            capacity,
            local_capacity,
            gossip_config.seeds,
            gossip_config.meta_broadcast_interval,
            gossip_config.seed_reannounce_interval,
            LocalAdoption {
                peers: adopted_peers,
                local: adopted_locally,
                placement_peers,
                placement_local,
                iam_peers,
                iam_local,
                iam_adopted_peers,
                iam_adopted_local,
                iam_config_peers,
                iam_config_local,
                generation_peers,
                generation_local,
                announce,
            },
        ));

        Ok((handle, event_rx))
    }
}

/// Build a foca SWIM configuration tuned for LAN clusters.
fn build_foca_config(max_packet_size: usize) -> foca::Config {
    let mut config = foca::Config::simple();

    // SWIM timing parameters from the plan.
    config.probe_period = Duration::from_secs(1);
    config.probe_rtt = Duration::from_millis(500);
    config.suspect_to_down_after = Duration::from_secs(5);
    config.remove_down_after = Duration::from_secs(30);

    // Plain SWIM never tells a member it was declared down: its peers just
    // drop what it sends. A node that stalled longer than the suspicion
    // timeout — a stopped process, a frozen VM, a long pause — resumes still
    // believing it is a member, gets no answers, declares its peers down in
    // turn, and the two sides never speak again until one restarts. With this,
    // a peer answers such a node that it is down; the node renews its identity
    // (`GossipIdentity::renew`) and joins with the next incarnation.
    config.notify_down_members = true;

    if let Ok(size) = max_packet_size.try_into() {
        config.max_packet_size = size;
    }

    config
}

// ---------------------------------------------------------------------------
// Background run-loop
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
async fn run_loop(
    peer_view: GossipHandle,
    socket: UdpSocket,
    foca: Mutex<S4Foca>,
    members: std::sync::Arc<std::sync::RwLock<HashMap<NodeId, NodeState>>>,
    roster: RosterCell,
    event_tx: mpsc::Sender<ClusterEvent>,
    mut meta_rx: mpsc::UnboundedReceiver<(NodeId, NodeMeta, MetaTail)>,
    mut local_identity: GossipIdentity,
    mut local_meta: NodeMeta,
    capacity: Option<std::sync::Arc<dyn crate::capacity::NodeCapacityProvider>>,
    local_capacity: std::sync::Arc<std::sync::RwLock<LocalCapacity>>,
    seeds: Vec<SocketAddr>,
    meta_broadcast_interval: Duration,
    seed_reannounce_interval: Duration,
    adoption: LocalAdoption,
) {
    let mut recv_buf = vec![0u8; 65535];
    let (timer_tx, mut timer_rx) = mpsc::unbounded_channel::<Timer<GossipIdentity>>();

    // Seeded with this node's own identity, so that the address it answers on
    // is held by somebody from the first packet onwards: a record that claims
    // this node's address for another id is then refused rather than believed.
    let mut identities: IdentityTable =
        HashMap::from([(local_identity.node_id, local_identity.clone())]);

    let mut meta_version: u64 = 0;
    let mut meta_interval = tokio::time::interval(meta_broadcast_interval);
    meta_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    // Announce to seed nodes.
    announce_to_seeds(&foca, &seeds, &local_identity, &socket, &timer_tx).await;
    info!(seeds = seeds.len(), "announced to seed nodes");

    // ...and keep announcing to every seed this node does not see alive.
    //
    // An announcement is one UDP datagram: sent while the seed's socket is not
    // bound yet — which is ordinary when a whole cluster starts at once, or
    // during a rolling restart — it is silently lost. Nothing else would bring
    // this node in, because only a seed receives announcements, so no peer
    // would ever discover it.
    //
    // And once a network split has healed, nothing else would bring the two
    // sides back together: each has declared the other dead, and nobody sends
    // anything to a member it holds dead. A seed that still holds this node
    // dead answers the announcement by telling it so (`notify_down_members`),
    // and the node rejoins under a renewed identity; one that has forgotten it
    // takes it as an ordinary join.
    let mut reannounce_interval = tokio::time::interval(seed_reannounce_interval);
    reannounce_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    reannounce_interval.tick().await;

    // Broadcast our own metadata immediately, with a fresh measurement in it —
    // a node that announced zeros and only corrected itself on the first tick
    // would spend that window looking unmeasured to every peer.
    refresh_local_capacity(capacity.as_deref(), &mut local_meta, &local_capacity);
    broadcast_local_meta(&foca, &local_meta, adoption.tail(), &mut meta_version);

    loop {
        tokio::select! {
            // --- Receive UDP gossip packet ---
            result = socket.recv_from(&mut recv_buf) => {
                match result {
                    Ok((len, _src)) => {
                        let outputs = {
                            let mut foca = lock_foca(&foca);
                            let mut runtime = AccumulatingRuntime::new();
                            if let Err(e) = foca.handle_data(&recv_buf[..len], &mut runtime) {
                                debug!(error = %e, "foca handle_data error");
                            }
                            drain_runtime(&mut runtime)
                        };
                        if process_outputs(
                            outputs,
                            &socket,
                            &timer_tx,
                            &members,
                            &roster,
                            &event_tx,
                            &mut local_identity,
                            &mut identities,
                            &peer_view,
                        )
                        .await
                        {
                            broadcast_local_meta(&foca, &local_meta, adoption.tail(), &mut meta_version);
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, "gossip socket recv error");
                    }
                }
            }

            // --- Timer fired ---
            Some(timer) = timer_rx.recv() => {
                let outputs = {
                    let mut foca = lock_foca(&foca);
                    let mut runtime = AccumulatingRuntime::new();
                    if let Err(e) = foca.handle_timer(timer, &mut runtime) {
                        debug!(error = %e, "foca handle_timer error");
                    }
                    drain_runtime(&mut runtime)
                };
                if process_outputs(
                    outputs,
                    &socket,
                    &timer_tx,
                    &members,
                    &roster,
                    &event_tx,
                    &mut local_identity,
                    &mut identities,
                    &peer_view,
                )
                .await
                {
                    broadcast_local_meta(&foca, &local_meta, adoption.tail(), &mut meta_version);
                }
            }

            // --- Received metadata from broadcast handler ---
            Some((node_id, meta, tail)) = meta_rx.recv() => {
                if node_id != local_identity.node_id {
                    record_adoption(&adoption.peers, node_id, tail.bucket_configs_adopted);
                    record_placement(
                        &adoption.placement_peers,
                        node_id,
                        tail.bucket_metadata_placement,
                    );
                    record_iam(&adoption.iam_peers, node_id, tail.iam);
                    record_adoption(&adoption.iam_adopted_peers, node_id, tail.iam_adopted);
                    record_iam_config(&adoption.iam_config_peers, node_id, tail.iam_config);
                    record_generation(&adoption.generation_peers, node_id, tail.pool_generation);
                }
                update_member_meta(&members, &roster, &local_identity, node_id, meta);
                peer_view.publish_peer_changes(Some(&identities));
            }

            // --- This node has something new to announce ---
            _ = adoption.announce.notified() => {
                if let Some(generation) = *adoption.generation_local.read().unwrap_or_else(|p| p.into_inner()) {
                    local_meta.topology_epoch = generation.current.epoch;
                }
                broadcast_local_meta(&foca, &local_meta, adoption.tail(), &mut meta_version);
            }

            // --- Periodic local metadata broadcast ---
            _ = meta_interval.tick() => {
                // Measured here and nowhere else. The other broadcast sites fire
                // on gossip traffic, which arrives far more often than the disk
                // changes size; re-measuring there would spend syscalls to learn
                // the same number.
                refresh_local_capacity(capacity.as_deref(), &mut local_meta, &local_capacity);
                broadcast_local_meta(&foca, &local_meta, adoption.tail(), &mut meta_version);
            }

            // --- Re-announce to the seeds this node does not see alive ---
            _ = reannounce_interval.tick() => {
                let missing = seeds_not_seen_alive(&members, &local_identity, &seeds);
                if !missing.is_empty() {
                    announce_to_seeds(&foca, &missing, &local_identity, &socket, &timer_tx).await;
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

/// The seeds no live peer of this node answers at: every seed but this node's
/// own address while it knows no one, none once the whole pool is up.
///
/// A seed is matched by the gossip address a live member has. The local node
/// is always in the map — foca reports it as a member — and never counts.
///
/// A poisoned lock yields no seed: the announcement is a best-effort nudge, and
/// a node whose membership state is broken has a larger problem than a missed
/// datagram.
fn seeds_not_seen_alive(
    members: &std::sync::Arc<std::sync::RwLock<HashMap<NodeId, NodeState>>>,
    local_identity: &GossipIdentity,
    seeds: &[SocketAddr],
) -> Vec<SocketAddr> {
    let Ok(members) = members.read() else {
        return Vec::new();
    };
    seeds
        .iter()
        .filter(|seed| **seed != local_identity.gossip_addr)
        .filter(|seed| {
            !members.iter().any(|(node_id, state)| {
                *node_id != local_identity.node_id
                    && state.status == NodeStatus::Alive
                    && state.meta.addr.gossip_addr == **seed
            })
        })
        .copied()
        .collect()
}

fn lock_foca(foca: &Mutex<S4Foca>) -> std::sync::MutexGuard<'_, S4Foca> {
    foca.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

async fn announce_to_seeds(
    foca: &Mutex<S4Foca>,
    seeds: &[SocketAddr],
    local_identity: &GossipIdentity,
    socket: &UdpSocket,
    timer_tx: &mpsc::UnboundedSender<Timer<GossipIdentity>>,
) {
    for (i, seed_addr) in seeds.iter().enumerate() {
        // Skip ourselves.
        if *seed_addr == local_identity.gossip_addr {
            continue;
        }

        // Use a placeholder identity for the seed — foca only needs the
        // address for routing.  The real identity will be learned from
        // the seed's response.
        let seed_identity = GossipIdentity {
            node_id: NodeId(i as u128 + 1),
            gossip_addr: *seed_addr,
            incarnation: 0,
        };

        let outputs = {
            let mut foca = lock_foca(foca);
            let mut runtime = AccumulatingRuntime::new();
            if let Err(e) = foca.announce(seed_identity, &mut runtime) {
                warn!(seed = %seed_addr, error = %e, "failed to announce to seed");
                continue;
            }
            drain_runtime(&mut runtime)
        };

        // Send the announcement packets.
        for (addr, data) in &outputs.to_send {
            if let Err(e) = socket.send_to(data, addr).await {
                warn!(addr = %addr, error = %e, "failed to send announcement");
            }
        }

        // Schedule any timers.
        for (duration, timer) in outputs.to_schedule {
            schedule_timer(timer_tx, duration, timer);
        }

        debug!(seed = %seed_addr, "announced to seed node");
    }
}

/// Re-measure this node's storage and put the result where both the cluster and
/// the local reader will see it.
///
/// Two destinations, because they answer different questions: `meta` is what
/// peers receive on the next broadcast, and `local_capacity` is what this node's
/// own placement reads without waiting for a round trip it would never make.
///
/// With no provider the numbers stand as they are and only their age advances.
/// That is deliberate: a caller that did not supply a way to measure has not
/// promised fresh data, and pretending the untouched value was just measured
/// would be a lie that placement would act on.
fn refresh_local_capacity(
    capacity: Option<&dyn crate::capacity::NodeCapacityProvider>,
    meta: &mut NodeMeta,
    local_capacity: &std::sync::Arc<std::sync::RwLock<LocalCapacity>>,
) {
    let Some(provider) = capacity else {
        return;
    };

    let measured = provider.measure();
    cluster_metrics::set_node_disk(measured.total_bytes, measured.used_bytes);
    meta.capacity = measured.clone();
    let mut local = local_capacity.write().unwrap_or_else(|poisoned| poisoned.into_inner());
    *local = LocalCapacity {
        capacity: measured,
        measured_at: Instant::now(),
    };
}

/// What the gossip loop keeps of the tail of the metadata: of adoption, the
/// peers that announced it and whether this node did; of the bucket metadata
/// placement and of IAM, what each peer and this node announced.
struct LocalAdoption {
    peers: AdoptedPeers,
    local: std::sync::Arc<std::sync::atomic::AtomicBool>,
    placement_peers: PlacementPeers,
    placement_local: PlacementLocal,
    iam_peers: IamPeers,
    iam_local: IamLocal,
    iam_adopted_peers: AdoptedPeers,
    iam_adopted_local: std::sync::Arc<std::sync::atomic::AtomicBool>,
    iam_config_peers: IamConfigPeers,
    iam_config_local: IamConfigLocal,
    generation_peers: GenerationPeers,
    generation_local: GenerationLocal,
    announce: std::sync::Arc<tokio::sync::Notify>,
}

impl LocalAdoption {
    /// The tail of this node's next metadata broadcast.
    fn tail(&self) -> MetaTail {
        MetaTail {
            pool_generation: *self.generation_local.read().unwrap_or_else(|p| p.into_inner()),
            bucket_configs_adopted: self.local.load(std::sync::atomic::Ordering::Acquire),
            bucket_metadata_placement: *self
                .placement_local
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
            iam: *self.iam_local.read().unwrap_or_else(|poisoned| poisoned.into_inner()),
            iam_adopted: self.iam_adopted_local.load(std::sync::atomic::Ordering::Acquire),
            iam_config: *self
                .iam_config_local
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        }
    }
}

fn record_generation(
    peers: &GenerationPeers,
    node: NodeId,
    generation: Option<GenerationAnnouncement>,
) {
    let mut peers = peers.write().unwrap_or_else(|p| p.into_inner());
    match generation {
        Some(generation) => peers.insert(node, generation),
        None => peers.remove(&node),
    };
}

/// Records what the latest metadata of `node` says of its IAM configuration.
fn record_iam_config(peers: &IamConfigPeers, node: NodeId, print: Option<IamConfigPrint>) {
    let mut peers = peers.write().unwrap_or_else(|poisoned| poisoned.into_inner());
    match print {
        Some(print) => peers.insert(node, print),
        None => peers.remove(&node),
    };
}

/// Records what the latest metadata of `node` says of its copy of IAM.
fn record_iam(peers: &IamPeers, node: NodeId, state: Option<IamAnnouncement>) {
    let mut peers = peers.write().unwrap_or_else(|poisoned| poisoned.into_inner());
    match state {
        Some(state) => peers.insert(node, state),
        None => peers.remove(&node),
    };
}

/// Records what the latest metadata of `node` says of its bucket metadata
/// placement.
fn record_placement(peers: &PlacementPeers, node: NodeId, record: Option<PlacementRecord>) {
    let mut peers = peers.write().unwrap_or_else(|poisoned| poisoned.into_inner());
    match record {
        Some(record) => peers.insert(node, record),
        None => peers.remove(&node),
    };
}

/// Records what the latest metadata of `node` says of its adoption.
fn record_adoption(peers: &AdoptedPeers, node: NodeId, adopted: bool) {
    let mut peers = peers.write().unwrap_or_else(|poisoned| poisoned.into_inner());
    if adopted {
        peers.insert(node);
    } else {
        peers.remove(&node);
    }
}

fn broadcast_local_meta(foca: &Mutex<S4Foca>, meta: &NodeMeta, tail: MetaTail, version: &mut u64) {
    *version += 1;
    let broadcast = MetaBroadcast {
        node_id: meta.node_id,
        meta: meta.clone(),
        version: *version,
    };

    match encode_meta(&broadcast, tail) {
        Ok(data) => {
            let mut foca = lock_foca(foca);
            if let Err(e) = foca.add_broadcast(&data) {
                debug!(error = %e, "failed to add metadata broadcast");
            }
        }
        Err(e) => {
            warn!(error = %e, "failed to serialize local metadata");
        }
    }
}

/// Returns `true` if a node this process had never seen before appeared, or
/// a node it knew came back as a new run — restarted.
///
/// The caller answers that by rebroadcasting its own metadata: a fresh member
/// starts out knowing nothing but node ids, and would otherwise wait up to
/// `meta_broadcast_interval` before it could tell an EC v2 peer from an old
/// one. Every existing member reacting to the same join is what makes the new
/// node's view complete in one round instead of thirty seconds. A restarted
/// node knows as little as a fresh one: until the tails of its peers reach
/// it, it cannot confirm its copy of IAM and refuses IAM credentials (plan
/// CLUSTER-IAM-REPLICATION.md, decision 4) — in a quiet pool, where nothing
/// else is broadcast, for most of that interval.
#[allow(clippy::too_many_arguments)]
async fn process_outputs(
    outputs: RuntimeOutputs,
    socket: &UdpSocket,
    timer_tx: &mpsc::UnboundedSender<Timer<GossipIdentity>>,
    members: &std::sync::Arc<std::sync::RwLock<HashMap<NodeId, NodeState>>>,
    roster: &RosterCell,
    event_tx: &mpsc::Sender<ClusterEvent>,
    local_identity: &mut GossipIdentity,
    identities: &mut IdentityTable,
    peer_view: &GossipHandle,
) -> bool {
    // Send UDP packets.
    for (addr, data) in &outputs.to_send {
        trace!(addr = %addr, len = data.len(), "sending gossip packet");
        if let Err(e) = socket.send_to(data, addr).await {
            warn!(addr = %addr, error = %e, "gossip send failed");
        }
    }

    // Schedule timers.
    for (duration, timer) in outputs.to_schedule {
        schedule_timer(timer_tx, duration, timer);
    }

    // Process notifications.
    let mut new_member = false;
    for notification in outputs.notifications {
        new_member |= handle_notification(
            notification,
            members,
            roster,
            event_tx,
            local_identity,
            identities,
            peer_view,
        )
        .await;
    }
    new_member
}

fn schedule_timer(
    tx: &mpsc::UnboundedSender<Timer<GossipIdentity>>,
    duration: Duration,
    timer: Timer<GossipIdentity>,
) {
    let tx = tx.clone();
    tokio::spawn(async move {
        tokio::time::sleep(duration).await;
        let _ = tx.send(timer);
    });
}

/// What this node currently believes each peer's gossip identity to be.
///
/// foca keys its members by address, not by node id, so one node id can sit in
/// its table twice: once at the address the node used before a restart, and
/// once at the address it uses now. Both halves produce notifications, and the
/// old half's death notice arrives after the new half's join. Read against this
/// table, that late notice is what it actually is — news about a process that
/// has ended — instead of news about the node, which is how a node that had
/// just come back was being buried by its own ghost.
///
/// The table stays inside the gossip run-loop. Nothing outside it needs an
/// identity: callers ask where a node answers, and that is its metadata.
type IdentityTable = HashMap<NodeId, GossipIdentity>;

/// Whether `incoming` names a run this node has already moved on from.
///
/// The comparison is foca's own: a strictly higher incarnation is a later run,
/// and on a tie the identity already held stays. Incarnation is the node's start
/// time in milliseconds, so a node that comes back always outranks the run it
/// replaces — unless the clock went backwards over the restart, which is as
/// undecidable here as it is inside foca.
fn identity_is_superseded(known: Option<&GossipIdentity>, incoming: &GossipIdentity) -> bool {
    known.is_some_and(|known| known != incoming && incoming.incarnation <= known.incarnation)
}

/// The entry a node gets before it has said anything about itself.
///
/// Every address in it is the gossip address, because that is the only address
/// an identity carries. It stands in until the node's own metadata broadcast
/// arrives, and reports `protocol_version: 0` — nothing confirmed — rather than
/// a guess.
fn placeholder_meta(identity: &GossipIdentity) -> NodeMeta {
    NodeMeta {
        node_id: identity.node_id,
        node_name: format!("node-{}", &identity.node_id.to_string()[..8]),
        addr: crate::identity::NodeAddr {
            grpc_addr: identity.gossip_addr,
            http_addr: identity.gossip_addr,
            gossip_addr: identity.gossip_addr,
        },
        pool_id: crate::identity::PoolId(0),
        topology_epoch: 0,
        protocol_version: 0,
        capacity: crate::identity::NodeCapacity {
            total_bytes: 0,
            used_bytes: 0,
            volume_count: 0,
            object_count: 0,
        },
        started_at: 0,
        s4_version: String::new(),
        edition: crate::identity::Edition::Community,
    }
}

/// Record that a node is present under `identity`, and say whether this process
/// had ever seen that node before.
///
/// Metadata that arrived earlier is kept, because a join notification and the
/// node's own metadata broadcast race and the broadcast often wins. Overwriting
/// it with the placeholder would not merely lose detail: the placeholder reports
/// `protocol_version: 0`, which reads as "no EC v2 support" to the
/// rolling-upgrade gate, and it would stand until the node's next metadata tick.
///
/// Kept, that is, while it is still about the address the node answers on. Once
/// that address changes, everything the old record said about reaching the node
/// is wrong, and the address it names may by now belong to a different node —
/// so the record goes back to the placeholder and the node's own broadcast
/// fills it in again.
fn note_member_present(
    members: &std::sync::Arc<std::sync::RwLock<HashMap<NodeId, NodeState>>>,
    identity: &GossipIdentity,
    status: NodeStatus,
) -> (NodeMeta, bool) {
    let mut m = members.write().unwrap_or_else(|p| p.into_inner());
    match m.get_mut(&identity.node_id) {
        Some(state) => {
            if state.meta.addr.gossip_addr != identity.gossip_addr {
                state.meta = placeholder_meta(identity);
            }
            state.status = status;
            state.last_seen = Instant::now();
            (state.meta.clone(), false)
        }
        None => {
            let meta = placeholder_meta(identity);
            m.insert(
                identity.node_id,
                NodeState {
                    meta: meta.clone(),
                    status,
                    last_seen: Instant::now(),
                },
            );
            (meta, true)
        }
    }
}

/// Returns `true` when the notification introduced a node this process had not
/// seen before, or a new run of one it had. See [`process_outputs`] for what
/// the caller does with that.
async fn handle_notification(
    notification: foca::OwnedNotification<GossipIdentity>,
    members: &std::sync::Arc<std::sync::RwLock<HashMap<NodeId, NodeState>>>,
    roster: &RosterCell,
    event_tx: &mpsc::Sender<ClusterEvent>,
    local_identity: &mut GossipIdentity,
    identities: &mut IdentityTable,
    peer_view: &GossipHandle,
) -> bool {
    match notification {
        foca::OwnedNotification::MemberUp(identity) => {
            if is_this_node(local_identity, &identity) {
                return false;
            }
            if identity_is_superseded(identities.get(&identity.node_id), &identity) {
                debug!(
                    node_id = %identity.node_id,
                    addr = %identity.gossip_addr,
                    "ignoring a join from a run this node has already moved on from"
                );
                return false;
            }

            let node_id = identity.node_id;
            info!(%node_id, addr = %identity.gossip_addr, "node joined cluster");
            let new_run = identities.get(&node_id) != Some(&identity);
            identities.insert(node_id, identity.clone());
            let (announced_meta, first_sighting) =
                note_member_present(members, &identity, NodeStatus::Alive);
            publish_membership(roster, members);
            peer_view.publish_peer_changes(Some(identities));

            let _ = event_tx.send(ClusterEvent::Joined(node_id, Box::new(announced_meta))).await;
            first_sighting || new_run
        }

        foca::OwnedNotification::MemberDown(identity) => {
            if is_this_node(local_identity, &identity) {
                return false;
            }
            // A death notice is about one identity, and only the identity this
            // node currently tracks can speak for the node. The other one is a
            // process that has already been replaced — burying the node on its
            // behalf is what made a restarted node unreachable for the rest of
            // the cluster's life.
            if identities.get(&identity.node_id) != Some(&identity) {
                debug!(
                    node_id = %identity.node_id,
                    addr = %identity.gossip_addr,
                    "ignoring a death notice for a run this node no longer tracks"
                );
                return false;
            }

            let node_id = identity.node_id;
            info!(%node_id, "node left cluster");

            {
                let mut m = members.write().unwrap_or_else(|p| p.into_inner());
                if let Some(state) = m.get_mut(&node_id) {
                    state.status = NodeStatus::Dead;
                    state.last_seen = Instant::now();
                }
            }
            publish_membership(roster, members);
            peer_view.publish_peer_changes(Some(identities));

            let _ = event_tx.send(ClusterEvent::Left(node_id)).await;
            false
        }

        foca::OwnedNotification::Active => {
            info!("gossip: this node is now active in the cluster");
            false
        }

        foca::OwnedNotification::Idle => {
            debug!("gossip: this node is idle (no known active members)");
            false
        }

        foca::OwnedNotification::Defunct => {
            warn!("gossip: this node has been declared defunct by the cluster");
            false
        }

        foca::OwnedNotification::Rejoin(new_identity) => {
            info!(
                incarnation = new_identity.incarnation,
                "gossip: rejoined cluster with new incarnation"
            );
            // Our own incarnation changed, so every peer's view of us is stale,
            // and so is every rule here that compares an identity against this
            // node's own.
            identities.insert(new_identity.node_id, new_identity.clone());
            *local_identity = new_identity;
            peer_view.publish_peer_changes(Some(identities));
            true
        }

        foca::OwnedNotification::Rename(old, new) => {
            // An address answers for one process at a time, so this says two
            // things: the node behind `new` is where `old` used to be, and
            // whoever `old` was is no longer there. foca sends no join with it
            // when the address was already answering — which is precisely the
            // case of a restarted node landing on an address its cluster still
            // considers live.
            info!(
                old_id = %old.node_id,
                new_id = %new.node_id,
                addr = %new.gossip_addr,
                "gossip: the node answering an address was replaced"
            );

            let displaced_status = members
                .read()
                .unwrap_or_else(|p| p.into_inner())
                .get(&old.node_id)
                .map(|state| state.status);

            let mut news = false;
            if !is_this_node(local_identity, &new)
                && !identity_is_superseded(identities.get(&new.node_id), &new)
            {
                let new_run = identities.get(&new.node_id) != Some(&new);
                identities.insert(new.node_id, new.clone());
                // The member slot keeps the liveness it had: foca reports a
                // rename on its own only when the replacement changes nothing
                // about whether that address answers.
                let (announced_meta, first_sighting) = note_member_present(
                    members,
                    &new,
                    displaced_status.unwrap_or(NodeStatus::Alive),
                );
                publish_membership(roster, members);
                peer_view.publish_peer_changes(Some(identities));
                let _ = event_tx
                    .send(ClusterEvent::Joined(new.node_id, Box::new(announced_meta)))
                    .await;
                news = first_sighting || new_run;
            }

            // The displaced node is not dead — it is unlocatable. Saying it is
            // Dead would be a claim this node cannot support, and leaving it
            // Alive at an address another node now owns would send this node's
            // RPCs to the wrong server. Forgetting where it is says exactly
            // what is known, and a join puts it back the moment it answers
            // somewhere.
            if old.node_id != new.node_id
                && !is_this_node(local_identity, &old)
                && identities.get(&old.node_id) == Some(&old)
            {
                identities.remove(&old.node_id);
                members.write().unwrap_or_else(|p| p.into_inner()).remove(&old.node_id);
                publish_membership(roster, members);
                peer_view.publish_peer_changes(Some(identities));
                let _ = event_tx.send(ClusterEvent::Left(old.node_id)).await;
            }

            news
        }
    }
}

/// Whether a notification is about this node rather than about a peer.
///
/// A node is not its own peer, so nothing here acts on one. The interesting
/// half is the ghost: after a restart on a different address, the cluster still
/// gossips the previous run's identity, which carries this node's id and some
/// other address. Treated as a peer, its death notice is what marks this node
/// dead in its own membership view.
fn is_this_node(local_identity: &GossipIdentity, identity: &GossipIdentity) -> bool {
    if identity.node_id != local_identity.node_id {
        return false;
    }
    if identity != local_identity {
        debug!(
            addr = %identity.gossip_addr,
            incarnation = identity.incarnation,
            "ignoring gossip about a previous run of this node"
        );
    }
    true
}

fn update_member_meta(
    members: &std::sync::Arc<std::sync::RwLock<HashMap<NodeId, NodeState>>>,
    roster: &RosterCell,
    local_identity: &GossipIdentity,
    node_id: NodeId,
    meta: NodeMeta,
) {
    // A node's own broadcast comes back through this handler, so the sender is
    // not necessarily a peer — but no peer is at this node's address, and a
    // record claiming it would send this node's RPCs to itself.
    //
    // Only this node's own address is judged here. Which of two remote nodes
    // holds an address is settled by the identity notifications, and a record
    // that disagrees with them is corrected by the next one. Refusing it
    // instead would be worse than the stale address it avoids: a refused
    // record is not re-sent until that node's next metadata tick, and until
    // then the node is missing from every answer this view gives — including
    // the one the rolling-upgrade gate reads.
    if node_id != local_identity.node_id && meta.addr.gossip_addr == local_identity.gossip_addr {
        debug!(
            %node_id,
            addr = %meta.addr.gossip_addr,
            "ignoring metadata that claims the address this node answers on"
        );
        return;
    }

    let mut m = members.write().unwrap_or_else(|p| p.into_inner());
    if let Some(state) = m.get_mut(&node_id) {
        state.meta = meta;
        state.last_seen = Instant::now();
        trace!(%node_id, "updated member metadata");
    } else {
        // Metadata arrived before MemberUp notification — store it anyway.
        m.insert(
            node_id,
            NodeState {
                meta,
                status: NodeStatus::Alive,
                last_seen: Instant::now(),
            },
        );
        trace!(%node_id, "stored metadata for not-yet-joined node");
        // A new member is a new status to count; new metadata of a known one
        // changes no count.
        drop(m);
        publish_membership(roster, members);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{
        EC_SET_LAYOUT_MIN_PROTOCOL_VERSION, EC_V2_MIN_PROTOCOL_VERSION, MIN_COMPATIBLE_VERSION,
        PROTOCOL_VERSION,
    };
    use std::net::{Ipv4Addr, SocketAddrV4};

    fn localhost(port: u16) -> SocketAddr {
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port))
    }

    /// A loopback address for a gossip node, with the socket that holds it.
    ///
    /// The socket is handed to the node rather than closed and bound again: in
    /// between, a test running beside this one may bind port 0 and be given the
    /// very same port. A std socket, so a node on a runtime of its own can take
    /// it there.
    fn reserve() -> (std::net::UdpSocket, SocketAddr) {
        let socket = std::net::UdpSocket::bind(localhost(0)).unwrap();
        socket.set_nonblocking(true).unwrap();
        let addr = socket.local_addr().unwrap();
        (socket, addr)
    }

    /// Starts a gossip node on a socket from [`reserve`].
    async fn start_reserved(
        socket: std::net::UdpSocket,
        config: GossipConfig,
        identity: GossipIdentity,
        meta: NodeMeta,
    ) -> (GossipHandle, mpsc::Receiver<ClusterEvent>) {
        let socket = UdpSocket::from_std(socket).unwrap();
        GossipService::start_on(socket, config, identity, meta, None).await.unwrap()
    }

    /// A handle over a member map the test keeps a grip on.
    ///
    /// It has no local node: every id these tests ask about is a peer, so
    /// capacity questions fall through to the map exactly as they do before a
    /// real node's first measurement.
    fn test_handle(
        members: std::sync::Arc<std::sync::RwLock<HashMap<NodeId, NodeState>>>,
    ) -> GossipHandle {
        GossipHandle {
            peer_changes: std::sync::Arc::default(),
            local_pool_id: crate::identity::PoolId(1),
            members,
            local_node_id: NodeId(0),
            local_capacity: std::sync::Arc::new(std::sync::RwLock::new(LocalCapacity {
                capacity: crate::identity::NodeCapacity::default(),
                measured_at: Instant::now(),
            })),
            roster: RosterCell::default(),
            adopted_peers: AdoptedPeers::default(),
            adopted_locally: std::sync::Arc::default(),
            placement_peers: PlacementPeers::default(),
            placement_local: PlacementLocal::default(),
            iam_peers: IamPeers::default(),
            iam_local: IamLocal::default(),
            iam_adopted_peers: AdoptedPeers::default(),
            iam_adopted_local: std::sync::Arc::default(),
            iam_config_peers: IamConfigPeers::default(),
            iam_config_local: IamConfigLocal::default(),
            generation_peers: GenerationPeers::default(),
            generation_local: GenerationLocal::default(),
            announce: std::sync::Arc::default(),
        }
    }

    #[tokio::test]
    async fn peer_changes_notify_independent_subscribers_and_preserve_unseen_updates() {
        let mut view = View::new(identity(NodeId(10), 9000, 1));
        let handle = view.handle();
        let mut first = handle.subscribe_peer_changes();
        let mut second = handle.subscribe_peer_changes();
        view.meta_arrived(test_meta(NodeId(20), 9200));
        assert!(
            first.has_changed().unwrap(),
            "metadata arrival must wake adoption"
        );
        first.changed().await.unwrap();
        assert!(
            second.has_changed().unwrap(),
            "subscribers must not steal updates"
        );
        second.changed().await.unwrap();
        let peer = identity(NodeId(20), 9200, 1);
        view.notify(foca::OwnedNotification::MemberUp(peer.clone())).await;
        first.borrow_and_update();
        second.borrow_and_update();
        view.notify(foca::OwnedNotification::MemberDown(peer.clone())).await;
        view.notify(foca::OwnedNotification::MemberUp(peer)).await;
        assert!(
            first.has_changed().unwrap(),
            "changes during a pass must remain pending"
        );
        assert!(second.has_changed().unwrap());
        first.changed().await.unwrap();
        assert!(
            !first.has_changed().unwrap(),
            "multiple changes coalesce into one pending version"
        );
    }

    #[tokio::test]
    async fn peer_changes_ignore_capacity_iam_repeats_and_other_pools() {
        let mut view = View::new(identity(NodeId(10), 9000, 1));
        let handle = view.handle();
        let mut peer = test_meta(NodeId(20), 9200);
        view.meta_arrived(peer.clone());
        let changes = handle.subscribe_peer_changes();
        for count in 1..5 {
            peer.capacity.object_count = count;
            view.meta_arrived(peer.clone());
            handle.set_iam_adopted_for_testing(peer.node_id, count % 2 == 0);
        }
        let mut foreign = test_meta(NodeId(30), 9300);
        foreign.pool_id = crate::PoolId(2);
        view.meta_arrived(foreign);
        view.notify(foca::OwnedNotification::MemberUp(identity(
            NodeId(30),
            9300,
            1,
        )))
        .await;
        let shape = crate::metadata_placement::QuorumShape::majority(3);
        handle.set_bucket_metadata_placement_for_testing(
            NodeId(30),
            Some(PlacementRecord::starting(shape, shape)),
        );
        let generation = crate::pool_generation::PoolGenerationRecord::initial(
            crate::PoolId(2),
            vec![NodeId(30)],
        )
        .unwrap();
        handle.set_pool_generation_for_testing(
            NodeId(30),
            Some(GenerationAnnouncement::of(&generation, false)),
        );
        assert!(!changes.has_changed().unwrap());
    }

    #[tokio::test]
    async fn peer_changes_notify_protocol_address_identity_and_returning_owner() {
        let mut view = View::new(identity(NodeId(10), 9000, 1));
        let handle = view.handle();
        handle.report_membership(roster(NodeId(10), &[NodeId(20)]));
        let mut changes = handle.subscribe_peer_changes();
        let old = identity(NodeId(20), 9200, 1);
        view.notify(foca::OwnedNotification::MemberUp(old.clone())).await;
        assert!(
            changes.has_changed().unwrap(),
            "configured owner placeholder matters"
        );
        changes.borrow_and_update();
        let mut peer = test_meta(NodeId(20), 9200);
        view.meta_arrived(peer.clone());
        changes.borrow_and_update();
        peer.protocol_version += 1;
        view.meta_arrived(peer.clone());
        assert!(
            changes.has_changed().unwrap(),
            "protocol confirmation matters"
        );
        changes.borrow_and_update();
        peer.addr.grpc_addr = localhost(9201);
        view.meta_arrived(peer);
        assert!(changes.has_changed().unwrap(), "owner RPC address matters");
        changes.borrow_and_update();
        view.notify(foca::OwnedNotification::MemberUp(identity(
            NodeId(20),
            9200,
            2,
        )))
        .await;
        assert!(
            changes.has_changed().unwrap(),
            "a new incarnation at the same address matters"
        );
        changes.borrow_and_update();
        // A stale death notice must neither change the state nor wake adoption.
        view.notify(foca::OwnedNotification::MemberDown(old)).await;
        assert!(!changes.has_changed().unwrap());
        let returning = identity(NodeId(20), 9200, 2);
        view.notify(foca::OwnedNotification::MemberDown(returning.clone())).await;
        assert!(changes.has_changed().unwrap());
        changes.borrow_and_update();
        view.notify(foca::OwnedNotification::MemberUp(returning)).await;
        assert!(
            changes.has_changed().unwrap(),
            "the owner returning must wake adoption"
        );
    }

    #[test]
    fn peer_changes_notify_placement_and_generation_without_duplicates() {
        let view = View::new(identity(NodeId(10), 9000, 1));
        let handle = view.handle();
        view.meta_arrived(test_meta(NodeId(20), 9200));
        let mut changes = handle.subscribe_peer_changes();
        let shape = crate::metadata_placement::QuorumShape::majority(3);
        let placement = PlacementRecord::starting(shape, shape);
        handle.set_bucket_metadata_placement_for_testing(NodeId(20), Some(placement));
        assert!(changes.has_changed().unwrap());
        changes.borrow_and_update();
        handle.set_bucket_metadata_placement_for_testing(NodeId(20), Some(placement));
        assert!(!changes.has_changed().unwrap());
        let record = crate::pool_generation::PoolGenerationRecord::initial(
            crate::PoolId(1),
            vec![NodeId(10), NodeId(20)],
        )
        .unwrap();
        let generation = GenerationAnnouncement::of(&record, false);
        handle.set_pool_generation_for_testing(NodeId(20), Some(generation));
        assert!(changes.has_changed().unwrap());
        changes.borrow_and_update();
        handle.set_pool_generation_for_testing(NodeId(20), Some(generation));
        assert!(!changes.has_changed().unwrap());
        handle.announce_bucket_metadata_placement(placement);
        assert!(
            changes.has_changed().unwrap(),
            "local placement matters too"
        );
        changes.borrow_and_update();
        handle.announce_pool_generation(generation);
        assert!(
            changes.has_changed().unwrap(),
            "local generation matters too"
        );
        changes.borrow_and_update();
        handle.announce_pool_generation(generation);
        handle.announce_bucket_metadata_placement(placement);
        assert!(
            !changes.has_changed().unwrap(),
            "identical local announcements are quiet"
        );
        handle.set_pool_generation_for_testing(NodeId(20), None);
        assert!(
            changes.has_changed().unwrap(),
            "withdrawn generation matters"
        );
    }

    #[tokio::test]
    async fn peer_changes_follow_live_metadata_and_placement_broadcasts() {
        let (socket_a, addr_a) = reserve();
        let (socket_b, addr_b) = reserve();
        let id_a = NodeId(100);
        let id_b = NodeId(200);
        let (a, _events_a) = start_reserved(
            socket_a,
            GossipConfig {
                bind_addr: addr_a,
                seeds: vec![addr_b],
                meta_broadcast_interval: Duration::from_millis(100),
                ..Default::default()
            },
            identity(id_a, addr_a.port(), 1),
            test_meta(id_a, addr_a.port()),
        )
        .await;
        let mut changes = a.subscribe_peer_changes();
        let (b, _events_b) = start_reserved(
            socket_b,
            GossipConfig {
                bind_addr: addr_b,
                seeds: vec![addr_a],
                meta_broadcast_interval: Duration::from_millis(100),
                ..Default::default()
            },
            identity(id_b, addr_b.port(), 1),
            test_meta(id_b, addr_b.port()),
        )
        .await;
        tokio::time::timeout(Duration::from_secs(10), async {
            while a.members().get(&id_b).is_none_or(|state| state.meta.protocol_version == 0) {
                changes.changed().await.unwrap();
            }
        })
        .await
        .expect("live metadata must wake adoption");
        changes.borrow_and_update();
        let shape = crate::metadata_placement::QuorumShape::majority(3);
        let placement = PlacementRecord::starting(shape, shape);
        b.announce_bucket_metadata_placement(placement);
        tokio::time::timeout(Duration::from_secs(10), async {
            while a.bucket_metadata_placement(&id_b) != Some(placement) {
                changes.changed().await.unwrap();
            }
        })
        .await
        .expect("live placement must wake adoption");
        changes.borrow_and_update();
        let generation = crate::pool_generation::PoolGenerationRecord::initial(
            crate::PoolId(1),
            vec![id_a, id_b],
        )
        .unwrap();
        let announcement = GenerationAnnouncement::of(&generation, false);
        b.announce_pool_generation(announcement);
        tokio::time::timeout(Duration::from_secs(10), async {
            while a.pool_generation(&id_b) != Some(announcement) {
                changes.changed().await.unwrap();
            }
        })
        .await
        .expect("live generation must wake adoption");
    }

    fn stamp(generation: u64, version: u64) -> MetaStamp {
        MetaStamp {
            generation,
            version,
        }
    }

    fn test_meta(node_id: NodeId, port: u16) -> NodeMeta {
        NodeMeta {
            node_id,
            node_name: "test-node".into(),
            addr: crate::identity::NodeAddr {
                grpc_addr: localhost(port),
                http_addr: localhost(port.saturating_add(1000)),
                gossip_addr: localhost(port),
            },
            pool_id: crate::identity::PoolId(1),
            topology_epoch: 1,
            protocol_version: 1,
            capacity: crate::identity::NodeCapacity {
                total_bytes: 1_000_000,
                used_bytes: 0,
                volume_count: 0,
                object_count: 0,
            },
            started_at: 1000,
            s4_version: "0.1.0".into(),
            edition: crate::identity::Edition::Community,
        }
    }

    /// A capacity provider that hands back numbers the test chose.
    struct StubCapacity(crate::identity::NodeCapacity);

    impl crate::capacity::NodeCapacityProvider for StubCapacity {
        fn measure(&self) -> crate::identity::NodeCapacity {
            self.0.clone()
        }
    }

    #[test]
    fn meta_broadcast_key_invalidates_same_node_older_version() {
        let node = NodeId::generate();
        let new_key = MetaBroadcastKey {
            node_id: node,
            stamp: stamp(1, 5),
        };
        let old_key = MetaBroadcastKey {
            node_id: node,
            stamp: stamp(1, 3),
        };
        assert!(new_key.invalidates(&old_key));
    }

    #[test]
    fn meta_broadcast_key_does_not_invalidate_different_node() {
        let a = MetaBroadcastKey {
            node_id: NodeId::generate(),
            stamp: stamp(1, 5),
        };
        let b = MetaBroadcastKey {
            node_id: NodeId::generate(),
            stamp: stamp(1, 3),
        };
        assert!(!a.invalidates(&b));
    }

    #[test]
    fn meta_broadcast_key_invalidates_same_version() {
        let node = NodeId::generate();
        let a = MetaBroadcastKey {
            node_id: node,
            stamp: stamp(1, 3),
        };
        let b = MetaBroadcastKey {
            node_id: node,
            stamp: stamp(1, 3),
        };
        assert!(a.invalidates(&b));
    }

    #[test]
    fn meta_handler_accepts_new_broadcast() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut handler = MetaHandler {
            seen: HashMap::new(),
            meta_tx: tx,
        };

        let node_id = NodeId::generate();
        let meta = test_meta(node_id, 9200);
        let broadcast = MetaBroadcast {
            node_id,
            meta: meta.clone(),
            version: 1,
        };
        let data = postcard::to_allocvec(&broadcast).unwrap();

        let key = handler.receive_item(&data, None).unwrap();
        assert!(key.is_some(), "new broadcast should return Some(key)");

        let (received_id, received_meta, _) = rx.try_recv().unwrap();
        assert_eq!(received_id, node_id);
        assert_eq!(received_meta, meta);
    }

    #[test]
    fn meta_handler_rejects_stale_broadcast() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let mut handler = MetaHandler {
            seen: HashMap::new(),
            meta_tx: tx,
        };

        let node_id = NodeId::generate();
        let meta = test_meta(node_id, 9200);

        // First: accept version 5
        let b1 = MetaBroadcast {
            node_id,
            meta: meta.clone(),
            version: 5,
        };
        let data1 = postcard::to_allocvec(&b1).unwrap();
        assert!(handler.receive_item(&data1, None).unwrap().is_some());

        // Second: reject version 3 (stale)
        let b2 = MetaBroadcast {
            node_id,
            meta,
            version: 3,
        };
        let data2 = postcard::to_allocvec(&b2).unwrap();
        assert!(handler.receive_item(&data2, None).unwrap().is_none());
    }

    /// A node that restarts starts counting its broadcasts from zero again.
    /// Compared on the version alone, everything the live process says reads as
    /// stale to every peer that stayed up — for as many broadcasts as the
    /// previous run had made, which after a month of uptime is a month of them.
    /// The peers would go on addressing the node at the IP it no longer has.
    #[test]
    fn metadata_from_a_later_run_is_accepted_although_its_version_restarted() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut handler = MetaHandler {
            seen: HashMap::new(),
            meta_tx: tx,
        };

        let node_id = NodeId::generate();
        let mut long_running = test_meta(node_id, 9200);
        long_running.started_at = 1000;
        let earlier = MetaBroadcast {
            node_id,
            meta: long_running,
            version: 86_400,
        };
        assert!(handler
            .receive_item(&postcard::to_allocvec(&earlier).unwrap(), None)
            .unwrap()
            .is_some());
        let _ = rx.try_recv();

        let mut restarted = test_meta(node_id, 9300);
        restarted.started_at = 2000;
        let later = MetaBroadcast {
            node_id,
            meta: restarted.clone(),
            version: 1,
        };
        assert!(handler
            .receive_item(&postcard::to_allocvec(&later).unwrap(), None)
            .unwrap()
            .is_some());

        let (received_id, received_meta, _) = rx.try_recv().unwrap();
        assert_eq!(received_id, node_id);
        assert_eq!(received_meta, restarted);
    }

    /// The ordering has to hold the other way round too: a broadcast of the
    /// previous run, relayed late, must not overwrite what the live one said.
    #[test]
    fn metadata_from_a_run_that_ended_is_dropped_however_high_its_version() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let mut handler = MetaHandler {
            seen: HashMap::new(),
            meta_tx: tx,
        };

        let node_id = NodeId::generate();
        let mut restarted = test_meta(node_id, 9300);
        restarted.started_at = 2000;
        let later = MetaBroadcast {
            node_id,
            meta: restarted,
            version: 1,
        };
        assert!(handler
            .receive_item(&postcard::to_allocvec(&later).unwrap(), None)
            .unwrap()
            .is_some());

        let mut long_running = test_meta(node_id, 9200);
        long_running.started_at = 1000;
        let earlier = MetaBroadcast {
            node_id,
            meta: long_running,
            version: 86_400,
        };
        assert!(handler
            .receive_item(&postcard::to_allocvec(&earlier).unwrap(), None)
            .unwrap()
            .is_none());
    }

    #[test]
    fn meta_broadcast_round_trip_serialization() {
        let node_id = NodeId::generate();
        let broadcast = MetaBroadcast {
            node_id,
            meta: test_meta(node_id, 9200),
            version: 42,
        };

        let encoded = postcard::to_allocvec(&broadcast).unwrap();
        let decoded: MetaBroadcast = postcard::from_bytes(&encoded).unwrap();

        assert_eq!(decoded.node_id, broadcast.node_id);
        assert_eq!(decoded.version, broadcast.version);
        assert_eq!(decoded.meta, broadcast.meta);
    }

    #[test]
    fn pool_generation_tail_preserves_every_older_tail() {
        use crate::pool_generation::{GenerationAnnouncement, PoolGenerationRecord};
        let record = PoolGenerationRecord::initial(crate::PoolId(0), vec![NodeId(1)])
            .unwrap()
            .configure(vec![NodeId(1), NodeId(2)])
            .unwrap();
        let broadcast = MetaBroadcast {
            node_id: NodeId(1),
            meta: test_meta(NodeId(1), 9200),
            version: 42,
        };
        let tail = MetaTail {
            pool_generation: Some(GenerationAnnouncement::of(&record, false)),
            ..MetaTail::default()
        };
        let bytes = encode_meta(&broadcast, tail).unwrap();
        let (_, rest) = postcard::take_from_bytes::<MetaBroadcast>(&bytes).unwrap();
        let (_, rest) = postcard::take_from_bytes::<AdoptionTail>(rest).unwrap();
        let (_, rest) = postcard::take_from_bytes::<PlacementTail>(rest).unwrap();
        let (_, generation) = postcard::take_from_bytes::<IamTail>(rest).unwrap();
        assert_eq!(decode_meta(&bytes).unwrap().1, tail);
        let old_len = bytes.len() - generation.len();
        assert_eq!(
            decode_meta(&bytes[..old_len]).unwrap().1.pool_generation,
            None
        );
        // An unreadable future tail does not discard protocol-8 information.
        let mut damaged = bytes[..old_len].to_vec();
        damaged.extend([0xff; 12]);
        assert_eq!(decode_meta(&damaged).unwrap().1.pool_generation, None);
        assert!(postcard::from_bytes::<MetaBroadcast>(&bytes).is_ok());
    }

    /// The tail of a metadata broadcast is invisible to a node of an older
    /// release, which decodes `MetaBroadcast` alone and stops there; a
    /// broadcast from such a node, which has no tail, reads as not adopted;
    /// and a tail that says adopted is read as sent.
    #[test]
    fn the_adoption_tail_passes_older_nodes_by() {
        let node_id = NodeId::generate();
        let broadcast = MetaBroadcast {
            node_id,
            meta: test_meta(node_id, 9200),
            version: 7,
        };
        let adopted = MetaTail {
            pool_generation: None,
            bucket_configs_adopted: true,
            bucket_metadata_placement: None,
            iam: None,
            iam_adopted: false,
            iam_config: None,
        };
        let encoded = encode_meta(&broadcast, adopted).unwrap();

        let as_older: MetaBroadcast = postcard::from_bytes(&encoded).unwrap();
        assert_eq!(as_older.meta, broadcast.meta);
        assert_eq!(as_older.version, 7);

        let (decoded, tail) = decode_meta(&encoded).unwrap();
        assert_eq!(
            (decoded.meta, decoded.version, tail),
            (broadcast.meta.clone(), 7, adopted)
        );

        let from_older = postcard::to_allocvec(&broadcast).unwrap();
        let (decoded, tail) = decode_meta(&from_older).unwrap();
        assert_eq!((decoded.version, tail), (7, MetaTail::default()));
        assert!(!tail.bucket_configs_adopted);

        // A tail this release cannot read leaves the broadcast as it is.
        let mut from_later = postcard::to_allocvec(&broadcast).unwrap();
        from_later.push(0xFF);
        let (decoded, tail) = decode_meta(&from_later).unwrap();
        assert_eq!(
            (decoded.meta, decoded.version, tail),
            (broadcast.meta, 7, MetaTail::default())
        );
    }

    /// The IAM part of the tail comes after the placement part: a node of an
    /// earlier release reads up to the placement and stops there, its own
    /// broadcast reads as no IAM, and an IAM part this release cannot read
    /// keeps what came before it.
    #[test]
    fn the_iam_tail_passes_earlier_nodes_by() {
        let node_id = NodeId::generate();
        let broadcast = MetaBroadcast {
            node_id,
            meta: test_meta(node_id, 9200),
            version: 7,
        };
        let iam = IamAnnouncement {
            epoch: crate::hlc::HlcTimestamp::from_record(1_700_000_000_000, 3, 9),
            digest: [7; 16],
        };
        let tail = MetaTail {
            pool_generation: None,
            bucket_configs_adopted: true,
            bucket_metadata_placement: None,
            iam: Some(iam),
            iam_adopted: true,
            iam_config: Some(IamConfigPrint {
                root: Some([1; 16]),
                jwt_secret: None,
                encryption_key: Some([3; 16]),
            }),
        };
        let encoded = encode_meta(&broadcast, tail).unwrap();
        assert_eq!(decode_meta(&encoded).unwrap().1, tail);

        // A node of the previous release: the broadcast, adoption, placement.
        let (_, rest) = postcard::take_from_bytes::<MetaBroadcast>(&encoded).unwrap();
        let (adoption, rest) = postcard::take_from_bytes::<AdoptionTail>(rest).unwrap();
        let (placement, _) = postcard::take_from_bytes::<PlacementTail>(rest).unwrap();
        assert!(adoption.bucket_configs_adopted);
        assert_eq!(placement.bucket_metadata_placement, None);

        // A broadcast of the previous release.
        let mut earlier = postcard::to_allocvec(&broadcast).unwrap();
        earlier.extend(
            postcard::to_allocvec(&AdoptionTail {
                bucket_configs_adopted: true,
            })
            .unwrap(),
        );
        earlier.extend(
            postcard::to_allocvec(&PlacementTail {
                bucket_metadata_placement: None,
            })
            .unwrap(),
        );
        let without_iam = MetaTail {
            iam: None,
            iam_adopted: false,
            iam_config: None,
            ..tail
        };
        assert_eq!(decode_meta(&earlier).unwrap().1, without_iam);

        // An IAM part this release cannot read.
        let mut later = earlier.clone();
        later.extend([1, 0xFF]);
        assert_eq!(decode_meta(&later).unwrap().1, without_iam);
    }

    /// A node announces the state of its copy of IAM once per change, and
    /// reads its peers' from their metadata.
    #[test]
    fn the_copy_of_iam_is_announced_and_read_back() {
        let handle = test_handle(std::sync::Arc::default());
        let peer = NodeId(5);
        let state = IamAnnouncement {
            epoch: crate::hlc::HlcTimestamp::from_record(10, 0, 1),
            digest: [1; 16],
        };
        assert_eq!(handle.iam_announcement(&NodeId(0)), None);
        handle.announce_iam(state);
        assert_eq!(handle.iam_announcement(&NodeId(0)), Some(state));

        assert_eq!(handle.iam_announcement(&peer), None);
        handle.set_iam_announcement_for_testing(peer, Some(state));
        assert_eq!(handle.iam_announcement(&peer), Some(state));
        handle.set_iam_announcement_for_testing(peer, None);
        assert_eq!(handle.iam_announcement(&peer), None);
    }

    /// A node announces the fingerprints of its IAM configuration once per
    /// change, and reads its peers' from their metadata.
    #[test]
    fn the_iam_configuration_is_announced_and_read_back() {
        let handle = test_handle(std::sync::Arc::default());
        let peer = NodeId(5);
        let print = IamConfigPrint {
            root: Some([1; 16]),
            jwt_secret: Some([2; 16]),
            encryption_key: None,
        };
        assert_eq!(handle.iam_config(&NodeId(0)), None);
        handle.announce_iam_config(print);
        assert_eq!(handle.iam_config(&NodeId(0)), Some(print));
        assert_eq!(handle.iam_config(&peer), None);
        handle.set_iam_config_for_testing(peer, Some(print));
        assert_eq!(handle.iam_config(&peer), Some(print));
        handle.set_iam_config_for_testing(peer, None);
        assert_eq!(handle.iam_config(&peer), None);
    }

    /// A node announces whether it keeps IAM of its own, once per change,
    /// and reads its peers' from their metadata; a peer that said nothing
    /// keeps some.
    #[test]
    fn the_adoption_of_iam_is_announced_and_read_back() {
        let handle = test_handle(std::sync::Arc::default());
        let peer = NodeId(5);
        assert!(!handle.iam_adopted(&NodeId(0)));
        handle.announce_iam_adopted(true);
        assert!(handle.iam_adopted(&NodeId(0)));
        assert!(!handle.iam_adopted(&peer));
        handle.set_iam_adopted_for_testing(peer, true);
        assert!(handle.iam_adopted(&peer));
        handle.set_iam_adopted_for_testing(peer, false);
        assert!(!handle.iam_adopted(&peer));
    }

    /// The placement part of the tail comes after the adoption part: a node
    /// of protocol 6 reads its adoption flag and stops there, a broadcast of
    /// protocol 6 reads as no placement with its adoption kept, and a
    /// placement part this release cannot read keeps the adoption flag.
    #[test]
    fn the_placement_tail_passes_protocol_6_by() {
        let node_id = NodeId::generate();
        let broadcast = MetaBroadcast {
            node_id,
            meta: test_meta(node_id, 9200),
            version: 7,
        };
        let s3 = crate::metadata_placement::QuorumShape::majority(3);
        let record = PlacementRecord {
            stage: crate::metadata_placement::Stage::DualWrite,
            backfill_complete: true,
            ..PlacementRecord::starting(s3, crate::metadata_placement::QuorumShape::majority(5))
        };
        let tail = MetaTail {
            pool_generation: None,
            bucket_configs_adopted: true,
            bucket_metadata_placement: Some(record),
            iam: None,
            iam_adopted: false,
            iam_config: None,
        };
        let encoded = encode_meta(&broadcast, tail).unwrap();
        assert_eq!(decode_meta(&encoded).unwrap().1, tail);

        // A node of protocol 6: the broadcast, then its own tail, and stop.
        #[derive(Deserialize)]
        struct Protocol6Tail {
            bucket_configs_adopted: bool,
        }
        let (as_6, rest) = postcard::take_from_bytes::<MetaBroadcast>(&encoded).unwrap();
        assert_eq!(as_6.meta, broadcast.meta);
        let as_6_tail: Protocol6Tail = postcard::from_bytes(rest).unwrap();
        assert!(as_6_tail.bucket_configs_adopted);

        // A broadcast of protocol 6.
        let mut from_6 = postcard::to_allocvec(&broadcast).unwrap();
        from_6.extend(postcard::to_allocvec(&true).unwrap());
        let (_, tail_6) = decode_meta(&from_6).unwrap();
        assert_eq!(
            tail_6,
            MetaTail {
                pool_generation: None,
                bucket_configs_adopted: true,
                bucket_metadata_placement: None,
                iam: None,
                iam_adopted: false,
                iam_config: None,
            }
        );

        // A placement part this release cannot read.
        let mut later = from_6.clone();
        later.extend([1, 0xFF, 0xFF]);
        assert_eq!(decode_meta(&later).unwrap().1, tail_6);
    }

    /// The gate's view of a peer: unknown, down, below the protocol, silent,
    /// or what it announced.
    #[test]
    fn the_placement_view_reads_the_member_map() {
        let members = std::sync::Arc::new(std::sync::RwLock::new(HashMap::new()));
        let handle = test_handle(members.clone());
        let min = crate::protocol::BUCKET_METADATA_MIN_PROTOCOL_VERSION;
        let state = |protocol_version: u32, status: NodeStatus, s4_version: &str| NodeState {
            meta: NodeMeta {
                protocol_version,
                s4_version: s4_version.into(),
                ..test_meta(NodeId(1), 9200)
            },
            status,
            last_seen: Instant::now(),
        };
        {
            let mut m = members.write().unwrap();
            m.insert(NodeId(2), state(min, NodeStatus::Dead, "1.0.0"));
            m.insert(NodeId(3), state(min - 1, NodeStatus::Alive, "1.0.0"));
            m.insert(NodeId(4), state(0, NodeStatus::Alive, ""));
            m.insert(NodeId(5), state(min, NodeStatus::Alive, "1.0.0"));
            m.insert(NodeId(6), state(min, NodeStatus::Alive, "1.0.0"));
        }
        let record = PlacementRecord::starting(
            crate::metadata_placement::QuorumShape::majority(3),
            crate::metadata_placement::QuorumShape::majority(5),
        );
        handle.set_bucket_metadata_placement_for_testing(NodeId(6), Some(record));
        assert_eq!(
            handle.peer_placement(NodeId(1), min),
            PeerPlacement::Unknown
        );
        assert_eq!(handle.peer_placement(NodeId(2), min), PeerPlacement::Down);
        assert_eq!(
            handle.peer_placement(NodeId(3), min),
            PeerPlacement::BelowProtocol {
                confirmed: Some(min - 1)
            }
        );
        assert_eq!(
            handle.peer_placement(NodeId(4), min),
            PeerPlacement::BelowProtocol { confirmed: None }
        );
        assert_eq!(handle.peer_placement(NodeId(5), min), PeerPlacement::Silent);
        assert_eq!(
            handle.peer_placement(NodeId(6), min),
            PeerPlacement::Announced(record)
        );
        handle.set_bucket_metadata_placement_for_testing(NodeId(6), None);
        assert_eq!(handle.peer_placement(NodeId(6), min), PeerPlacement::Silent);

        // This node answers by what it announced.
        assert_eq!(handle.bucket_metadata_placement(&NodeId(0)), None);
        handle.announce_bucket_metadata_placement(record);
        assert_eq!(handle.bucket_metadata_placement(&NodeId(0)), Some(record));
    }

    /// What a peer's latest metadata says of its adoption is what the handle
    /// answers; this node answers by what it announced.
    #[test]
    fn the_handle_answers_adoption_as_the_latest_metadata_says() {
        let handle = test_handle(std::sync::Arc::default());
        let peer = NodeId(8);
        assert!(!handle.bucket_configs_adopted(&peer));
        record_adoption(&handle.adopted_peers, peer, true);
        assert!(handle.bucket_configs_adopted(&peer));
        record_adoption(&handle.adopted_peers, peer, false);
        assert!(
            !handle.bucket_configs_adopted(&peer),
            "a restart that has not adopted yet"
        );

        let local = handle.local_node_id;
        assert!(!handle.bucket_configs_adopted(&local));
        handle.announce_bucket_configs_adopted();
        assert!(handle.bucket_configs_adopted(&local));
    }

    #[test]
    fn gossip_identity_serde_round_trip() {
        let identity = GossipIdentity {
            node_id: NodeId::generate(),
            gossip_addr: localhost(9200),
            incarnation: 7,
        };

        let encoded = postcard::to_allocvec(&identity).unwrap();
        let decoded: GossipIdentity = postcard::from_bytes(&encoded).unwrap();

        assert_eq!(decoded, identity);
    }

    #[test]
    fn foca_config_has_expected_timing() {
        let config = build_foca_config(1400);
        assert_eq!(config.probe_period, Duration::from_secs(1));
        assert_eq!(config.suspect_to_down_after, Duration::from_secs(5));
        assert_eq!(config.remove_down_after, Duration::from_secs(30));
        assert!(config.notify_down_members);
    }

    #[tokio::test]
    async fn gossip_service_starts_and_binds() {
        let node_id = NodeId::generate();
        let bind_addr = localhost(0); // OS-assigned port
        let gossip_config = GossipConfig {
            bind_addr,
            seeds: Vec::new(),
            ..Default::default()
        };
        let identity = GossipIdentity {
            node_id,
            gossip_addr: bind_addr,
            incarnation: 0,
        };
        let meta = test_meta(node_id, 0);

        let result = GossipService::start(gossip_config, identity, meta, None).await;
        assert!(result.is_ok(), "gossip service should start successfully");

        let (handle, _rx) = result.unwrap();
        assert_eq!(handle.alive_count(), 0, "no peers yet");
    }

    /// A node must appear in its own membership view, carrying its real
    /// metadata.
    ///
    /// The EC v2 gate asks about every node in the pool, and the local node is
    /// one of them: if a node were absent from its own view, the gate would
    /// read the pool as mixed forever and auto-tiering would never run
    /// anywhere. Self-membership is a side effect of `Foca::add_broadcast`
    /// passing the payload through our own `BroadcastHandler`, which is not
    /// obvious from our code alone — hence a test that pins it.
    #[tokio::test]
    async fn a_node_appears_in_its_own_membership_view() {
        let node_id = NodeId::generate();
        let (socket, bind_addr) = reserve();

        let mut meta = test_meta(node_id, bind_addr.port());
        meta.protocol_version = PROTOCOL_VERSION;
        meta.s4_version = "0.1.0".into();

        let (handle, _rx) = start_reserved(
            socket,
            GossipConfig {
                bind_addr,
                seeds: Vec::new(),
                ..Default::default()
            },
            GossipIdentity {
                node_id,
                gossip_addr: bind_addr,
                incarnation: 0,
            },
            meta,
        )
        .await;

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if handle.get_node(&node_id).is_some() {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "a node never learned its own metadata"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        let state = handle.get_node(&node_id).unwrap();
        assert_eq!(state.meta.protocol_version, PROTOCOL_VERSION);
        assert!(
            handle.pool_nodes_support_v2(&[node_id]),
            "a single-node pool of this binary must be EC v2 capable"
        );
    }

    #[tokio::test]
    async fn two_nodes_discover_each_other() {
        // Node A
        let id_a = NodeId::generate();
        let (socket_a, addr_a) = reserve();

        let config_a = GossipConfig {
            bind_addr: addr_a,
            seeds: Vec::new(),
            ..Default::default()
        };
        let identity_a = GossipIdentity {
            node_id: id_a,
            gossip_addr: addr_a,
            incarnation: 0,
        };

        let (handle_a, mut rx_a) = start_reserved(
            socket_a,
            config_a,
            identity_a,
            test_meta(id_a, addr_a.port()),
        )
        .await;

        // Node B (seeds = [addr_a])
        let id_b = NodeId::generate();
        let (socket_b, addr_b) = reserve();

        let config_b = GossipConfig {
            bind_addr: addr_b,
            seeds: vec![addr_a],
            ..Default::default()
        };
        let identity_b = GossipIdentity {
            node_id: id_b,
            gossip_addr: addr_b,
            incarnation: 0,
        };

        let (handle_b, mut rx_b) = start_reserved(
            socket_b,
            config_b,
            identity_b,
            test_meta(id_b, addr_b.port()),
        )
        .await;

        // Wait for discovery (with timeout).
        let deadline = tokio::time::sleep(Duration::from_secs(10));
        tokio::pin!(deadline);

        let mut a_saw_b = false;
        let mut b_saw_a = false;

        loop {
            if a_saw_b && b_saw_a {
                break;
            }

            tokio::select! {
                _ = &mut deadline => {
                    panic!(
                        "timeout waiting for discovery (a_saw_b={a_saw_b}, b_saw_a={b_saw_a})"
                    );
                }
                Some(event) = rx_a.recv() => {
                    if let ClusterEvent::Joined(nid, _) = event {
                        if nid == id_b { a_saw_b = true; }
                    }
                }
                Some(event) = rx_b.recv() => {
                    if let ClusterEvent::Joined(nid, _) = event {
                        if nid == id_a { b_saw_a = true; }
                    }
                }
            }
        }

        assert!(handle_a.alive_count() >= 1);
        assert!(handle_b.alive_count() >= 1);
    }

    /// A node that adopted its bucket configurations announces it at once: its
    /// peer learns it well before the next periodic metadata broadcast.
    #[tokio::test]
    async fn an_adoption_announced_reaches_the_peer_at_once() {
        let start = |seeds: Vec<SocketAddr>| async move {
            let id = NodeId::generate();
            let (socket, addr) = reserve();
            let config = GossipConfig {
                bind_addr: addr,
                seeds,
                meta_broadcast_interval: Duration::from_secs(3600),
                ..Default::default()
            };
            let identity = GossipIdentity {
                node_id: id,
                gossip_addr: addr,
                incarnation: 0,
            };
            let (handle, rx) =
                start_reserved(socket, config, identity, test_meta(id, addr.port())).await;
            (id, addr, handle, rx)
        };
        let (id_a, addr_a, handle_a, _rx_a) = start(Vec::new()).await;
        let (_id_b, _addr_b, handle_b, _rx_b) = start(vec![addr_a]).await;

        let deadline = Instant::now() + Duration::from_secs(10);
        while handle_b.get_node(&id_a).is_none_or(|state| state.meta.node_id != id_a) {
            assert!(
                Instant::now() < deadline,
                "node B never saw node A's metadata"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(!handle_b.bucket_configs_adopted(&id_a));

        handle_a.announce_bucket_configs_adopted();
        assert!(handle_a.bucket_configs_adopted(&id_a));
        let deadline = Instant::now() + Duration::from_secs(10);
        while !handle_b.bucket_configs_adopted(&id_a) {
            assert!(
                Instant::now() < deadline,
                "node B never learned that node A adopted"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// A node whose seed is not listening yet still joins once it comes up.
    ///
    /// This is the ordinary case when a whole cluster starts at once, or during
    /// a rolling restart: the announcement is a single UDP datagram, and one
    /// sent at a socket nobody has bound is silently lost. Without a retry the
    /// node stays isolated for the life of the process — and stays invisible to
    /// everyone else too, because only a seed receives announcements.
    /// The EC v2 gate asks whether every node of a pool has confirmed its
    /// protocol, and a node confirms it only through its metadata broadcast.
    /// A pool whose nodes all seed from each other must reach that state on its
    /// own, quickly and without a restart — the tiering worker asks a couple of
    /// seconds after start, and a pool that cannot answer pauses migrations.
    #[tokio::test]
    async fn a_pool_that_starts_together_confirms_every_protocol() {
        const NODES: usize = 6;

        let mut ids = Vec::new();
        let mut addrs = Vec::new();
        let mut sockets = Vec::new();
        for _ in 0..NODES {
            let (socket, addr) = reserve();
            addrs.push(addr);
            sockets.push(socket);
            ids.push(NodeId::generate());
        }

        let mut handles = Vec::new();
        for (idx, socket) in sockets.into_iter().enumerate() {
            let mut meta = test_meta(ids[idx], addrs[idx].port());
            meta.addr.gossip_addr = addrs[idx];
            meta.addr.grpc_addr = addrs[idx];
            meta.protocol_version = PROTOCOL_VERSION;
            // Every node seeds from the whole pool, its own address included,
            // exactly as the cluster bootstrap passes S4_SEEDS.
            let config = GossipConfig {
                bind_addr: addrs[idx],
                seeds: addrs.clone(),
                ..Default::default()
            };
            let identity = GossipIdentity {
                node_id: ids[idx],
                gossip_addr: addrs[idx],
                incarnation: 1_700_000_000_000 + idx as u64,
            };
            let (handle, _rx) = start_reserved(socket, config, identity, meta).await;
            handles.push(handle);
        }

        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let confirmed = handles.iter().all(|handle| handle.pool_nodes_support_v2(&ids));
            if confirmed {
                break;
            }
            if Instant::now() >= deadline {
                for (idx, handle) in handles.iter().enumerate() {
                    let missing = handle.nodes_below_protocol(&ids, EC_V2_MIN_PROTOCOL_VERSION);
                    let seen: Vec<_> = handle
                        .members()
                        .iter()
                        .map(|(id, state)| {
                            (
                                id.to_string()[..8].to_string(),
                                state.meta.protocol_version,
                                state.meta.addr.gossip_addr,
                                state.status,
                            )
                        })
                        .collect();
                    eprintln!(
                        "node {idx}: missing {} of {NODES}, view {seen:?}",
                        missing.len()
                    );
                }
                panic!("a pool that starts together never confirmed its own protocol");
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    #[tokio::test]
    async fn a_node_joins_a_seed_that_starts_after_it() {
        // Reserve both addresses first, so the late seed starts on the exact
        // address the early node was told to announce to.
        let (socket_a, addr_a) = reserve();
        let (socket_b, addr_b) = reserve();

        // Node B starts first and announces into the void.
        let id_b = NodeId::generate();
        let (handle_b, mut rx_b) = start_reserved(
            socket_b,
            GossipConfig {
                bind_addr: addr_b,
                seeds: vec![addr_a],
                ..Default::default()
            },
            GossipIdentity {
                node_id: id_b,
                gossip_addr: addr_b,
                incarnation: 0,
            },
            test_meta(id_b, addr_b.port()),
        )
        .await;

        // Long enough that the one-shot announcement is definitely spent. The
        // local node counts itself as a member, so the check is for peers.
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(
            handle_b.members().contains_key(&id_b),
            "a node must appear in its own membership view"
        );
        assert_eq!(
            handle_b.members().len(),
            1,
            "node B must know no peer while its seed is not up"
        );

        // The seed's address was held, not closed, so what B announced while the
        // seed was down waits in that socket. Thrown away here: the seed must be
        // reached by what B sends once it is up, as a seed that was really down
        // would be.
        let mut stale = [0u8; 65_536];
        while socket_a.recv_from(&mut stale).is_ok() {}

        let id_a = NodeId::generate();
        let (handle_a, _rx_a) = start_reserved(
            socket_a,
            GossipConfig {
                bind_addr: addr_a,
                seeds: Vec::new(),
                ..Default::default()
            },
            GossipIdentity {
                node_id: id_a,
                gossip_addr: addr_a,
                incarnation: 0,
            },
            test_meta(id_a, addr_a.port()),
        )
        .await;

        let deadline = tokio::time::sleep(Duration::from_secs(15));
        tokio::pin!(deadline);
        loop {
            tokio::select! {
                _ = &mut deadline => {
                    panic!("node B never joined a seed that started after it");
                }
                Some(event) = rx_b.recv() => {
                    if matches!(event, ClusterEvent::Joined(nid, _) if nid == id_a) {
                        break;
                    }
                }
            }
        }

        assert!(handle_b.members().contains_key(&id_a));
        assert!(
            handle_a.members().contains_key(&id_b),
            "the seed must see the late joiner too"
        );
    }

    /// Starts a gossip node on a runtime and a thread of its own. While the
    /// returned flag is set the whole runtime stands still — timers, socket,
    /// replies — the way SIGSTOP stops a process, and its peers go on without
    /// it. The thread ends once the test drops the flag.
    fn start_pausable_node(
        socket: std::net::UdpSocket,
        config: GossipConfig,
        identity: GossipIdentity,
        meta: NodeMeta,
    ) -> (GossipHandle, std::sync::Arc<std::sync::atomic::AtomicBool>) {
        let paused = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let node_paused = paused.clone();
        let (handle_tx, handle_rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let runtime =
                tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
            runtime.block_on(async move {
                let (handle, _) = start_reserved(socket, config, identity, meta).await;
                handle_tx.send(handle).unwrap();
                while std::sync::Arc::strong_count(&node_paused) > 1 {
                    // Blocks this thread, and with it every task of the node.
                    while node_paused.load(std::sync::atomic::Ordering::SeqCst)
                        && std::sync::Arc::strong_count(&node_paused) > 1
                    {
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            });
        });
        (handle_rx.recv().unwrap(), paused)
    }

    /// What node `viewer` holds node `of` to be.
    fn status_of(
        handles: &[GossipHandle],
        ids: &[NodeId],
        viewer: usize,
        of: usize,
    ) -> Option<NodeStatus> {
        handles[viewer].members().get(&ids[of]).map(|s| s.status)
    }

    /// Whether every node holds every other one alive.
    fn pool_is_whole(handles: &[GossipHandle], ids: &[NodeId]) -> bool {
        (0..ids.len()).all(|viewer| {
            (0..ids.len())
                .filter(|of| *of != viewer)
                .all(|of| status_of(handles, ids, viewer, of) == Some(NodeStatus::Alive))
        })
    }

    /// Polls `done` until it holds; after `within`, fails with `what` and what
    /// every node holds every node to be.
    async fn wait_until(
        what: &str,
        within: Duration,
        handles: &[GossipHandle],
        ids: &[NodeId],
        done: impl Fn() -> bool,
    ) {
        let deadline = Instant::now() + within;
        while !done() {
            let views: Vec<Vec<_>> = (0..ids.len())
                .map(|viewer| {
                    (0..ids.len()).map(|of| status_of(handles, ids, viewer, of)).collect()
                })
                .collect();
            assert!(Instant::now() < deadline, "{what}; views: {views:?}");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// A node that stalls until its peers declare it dead — a process stopped
    /// with SIGSTOP, a frozen VM — comes back into the pool on its own once it
    /// runs again. Nothing else would bring it back: its peers drop whatever it
    /// sends, it gets no answers and declares them dead in turn, and the pool
    /// stays split until one of the nodes restarts.
    #[tokio::test]
    async fn a_node_paused_until_declared_dead_rejoins_when_it_resumes() {
        const NODES: usize = 3;
        const PAUSED: usize = 1;
        // Node 0 runs on this thread and reports its view of the pool.
        let metrics = crate::test_metrics::TestMetrics::install();

        let mut ids = Vec::new();
        let mut addrs = Vec::new();
        let mut sockets = Vec::new();
        for _ in 0..NODES {
            let (socket, addr) = reserve();
            addrs.push(addr);
            sockets.push(socket);
            ids.push(NodeId::generate());
        }

        let mut handles = Vec::new();
        let mut paused = None;
        for (idx, socket) in sockets.into_iter().enumerate() {
            let config = GossipConfig {
                bind_addr: addrs[idx],
                seeds: addrs.clone(),
                ..Default::default()
            };
            let identity = GossipIdentity {
                node_id: ids[idx],
                gossip_addr: addrs[idx],
                incarnation: 1_700_000_000_000 + idx as u64,
            };
            let meta = test_meta(ids[idx], addrs[idx].port());
            if idx == PAUSED {
                let (handle, node_paused) = start_pausable_node(socket, config, identity, meta);
                handles.push(handle);
                paused = Some(node_paused);
            } else {
                let (handle, _) = start_reserved(socket, config, identity, meta).await;
                handles.push(handle);
            }
        }
        let paused = paused.unwrap();
        let (handles, ids) = (&handles[..], &ids[..]);

        wait_until(
            "the pool never formed",
            Duration::from_secs(10),
            handles,
            ids,
            || pool_is_whole(handles, ids),
        )
        .await;
        handles[0].report_membership(roster(ids[0], &ids[1..]));
        assert_eq!(
            node_counts(&metrics),
            vec![Some(3.0), Some(0.0), Some(0.0), Some(0.0)]
        );

        paused.store(true, std::sync::atomic::Ordering::SeqCst);
        wait_until(
            "the running nodes never declared the paused one dead",
            Duration::from_secs(20),
            handles,
            ids,
            || {
                (0..NODES)
                    .filter(|viewer| *viewer != PAUSED)
                    .all(|viewer| status_of(handles, ids, viewer, PAUSED) == Some(NodeStatus::Dead))
            },
        )
        .await;
        assert_eq!(
            node_counts(&metrics),
            vec![Some(2.0), Some(0.0), Some(1.0), Some(0.0)]
        );
        assert_eq!(peer_up(&metrics, "node-2"), Some(0.0));
        assert_eq!(peer_up(&metrics, "node-3"), Some(1.0));
        paused.store(false, std::sync::atomic::Ordering::SeqCst);

        let resumed = Instant::now();
        wait_until(
            "the paused node never came back after it resumed",
            Duration::from_secs(30),
            handles,
            ids,
            || pool_is_whole(handles, ids),
        )
        .await;
        eprintln!(
            "the pool was whole again {:?} after the node resumed",
            resumed.elapsed()
        );
        assert_eq!(
            node_counts(&metrics),
            vec![Some(3.0), Some(0.0), Some(0.0), Some(0.0)]
        );
        assert_eq!(peer_up(&metrics, "node-2"), Some(1.0));
    }

    /// Forwards what reaches `relay` to the socket of `node`, except, while
    /// `split` holds, a datagram between `cut_off` and any other node: a
    /// network the test can cut one node off from, and heal.
    fn spawn_relay(
        relay: UdpSocket,
        node: SocketAddr,
        cut_off: SocketAddr,
        split: std::sync::Arc<std::sync::atomic::AtomicBool>,
    ) {
        tokio::spawn(async move {
            let mut buf = vec![0u8; 65_536];
            while let Ok((len, from)) = relay.recv_from(&mut buf).await {
                if split.load(std::sync::atomic::Ordering::SeqCst)
                    && (from == cut_off) != (node == cut_off)
                {
                    continue;
                }
                let _ = relay.send_to(&buf[..len], node).await;
            }
        });
    }

    /// The two sides of a network split find each other again once it heals.
    /// While it lasts each side declares the other dead, and nobody sends
    /// anything to a member it holds dead — a healed network alone changes
    /// nothing, and a node cut off for longer than the suspicion timeout would
    /// stay out of the pool until it restarted.
    #[tokio::test]
    async fn a_node_cut_off_by_a_network_split_rejoins_once_it_heals() {
        const NODES: usize = 3;
        const CUT_OFF: usize = 2;

        // Every node is known to its peers only by its relay's address.
        let split = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut nodes = Vec::new();
        let mut sockets = Vec::new();
        let mut relays = Vec::new();
        for _ in 0..NODES {
            let (socket, addr) = reserve();
            nodes.push(addr);
            sockets.push(socket);
            relays.push(UdpSocket::bind(localhost(0)).await.unwrap());
        }
        let relay_addrs: Vec<_> = relays.iter().map(|r| r.local_addr().unwrap()).collect();
        for (relay, node) in relays.into_iter().zip(&nodes) {
            spawn_relay(relay, *node, nodes[CUT_OFF], split.clone());
        }

        let ids: Vec<_> = (0..NODES).map(|_| NodeId::generate()).collect();
        let mut handles = Vec::new();
        for (idx, socket) in sockets.into_iter().enumerate() {
            let config = GossipConfig {
                bind_addr: nodes[idx],
                seeds: relay_addrs.clone(),
                ..Default::default()
            };
            let identity = GossipIdentity {
                node_id: ids[idx],
                gossip_addr: relay_addrs[idx],
                incarnation: 1_700_000_000_000 + idx as u64,
            };
            let meta = test_meta(ids[idx], relay_addrs[idx].port());
            let (handle, _) = start_reserved(socket, config, identity, meta).await;
            handles.push(handle);
        }
        let (handles, ids) = (&handles[..], &ids[..]);

        wait_until(
            "the pool never formed",
            Duration::from_secs(10),
            handles,
            ids,
            || pool_is_whole(handles, ids),
        )
        .await;

        split.store(true, std::sync::atomic::Ordering::SeqCst);
        wait_until(
            "the two sides never declared each other dead",
            Duration::from_secs(20),
            handles,
            ids,
            || {
                (0..NODES).filter(|node| *node != CUT_OFF).all(|node| {
                    status_of(handles, ids, node, CUT_OFF) == Some(NodeStatus::Dead)
                        && status_of(handles, ids, CUT_OFF, node) == Some(NodeStatus::Dead)
                })
            },
        )
        .await;
        // Held past every suspicion timer still running. foca sends a member it
        // declares dead a notice of it, and one sent after the heal would bring
        // the pool back together by luck — a split that outlasts the timers,
        // as a real one does, gets no such notice.
        tokio::time::sleep(Duration::from_secs(6)).await;
        split.store(false, std::sync::atomic::Ordering::SeqCst);

        let healed = Instant::now();
        wait_until(
            "the split never closed after the network healed",
            Duration::from_secs(30),
            handles,
            ids,
            || pool_is_whole(handles, ids),
        )
        .await;
        eprintln!(
            "the pool was whole again {:?} after the network healed",
            healed.elapsed()
        );
    }

    /// A node announces itself to exactly the seeds no live peer answers at.
    #[test]
    fn a_node_announces_to_the_seeds_it_does_not_see_alive() {
        let local = identity(NodeId(1), 9001, 1);
        let seeds = [
            localhost(9001),
            localhost(9002),
            localhost(9003),
            localhost(9004),
        ];
        let members = std::sync::Arc::new(std::sync::RwLock::new(HashMap::new()));
        {
            let mut m = members.write().unwrap();
            for (node_id, port, status) in [
                (1, 9001, NodeStatus::Alive),
                (2, 9002, NodeStatus::Alive),
                (3, 9003, NodeStatus::Dead),
            ] {
                m.insert(
                    NodeId(node_id),
                    NodeState {
                        meta: test_meta(NodeId(node_id), port),
                        status,
                        last_seen: Instant::now(),
                    },
                );
            }
        }

        assert_eq!(
            seeds_not_seen_alive(&members, &local, &seeds),
            vec![localhost(9003), localhost(9004)],
            "a seed held dead or never heard from is announced to, a live one and this node are not"
        );

        members.write().unwrap().retain(|node_id, _| *node_id == NodeId(1));
        assert_eq!(
            seeds_not_seen_alive(&members, &local, &seeds),
            vec![localhost(9002), localhost(9003), localhost(9004)],
            "a node that knows no one announces to every seed but itself"
        );
    }

    #[test]
    fn update_member_meta_creates_entry_for_unknown_node() {
        let members = std::sync::Arc::new(std::sync::RwLock::new(HashMap::new()));
        let node_id = NodeId::generate();
        let meta = test_meta(node_id, 9200);

        update_member_meta(
            &members,
            &RosterCell::default(),
            &identity(NodeId(1), 9000, 1),
            node_id,
            meta.clone(),
        );

        let m = members.read().unwrap();
        let state = m.get(&node_id).unwrap();
        assert_eq!(state.status, NodeStatus::Alive);
        assert_eq!(state.meta.node_id, node_id);
    }

    #[test]
    fn update_member_meta_updates_existing_entry() {
        let members = std::sync::Arc::new(std::sync::RwLock::new(HashMap::new()));
        let node_id = NodeId::generate();

        // Insert initial state.
        {
            let mut m = members.write().unwrap();
            m.insert(
                node_id,
                NodeState {
                    meta: test_meta(node_id, 9200),
                    status: NodeStatus::Suspect,
                    last_seen: Instant::now(),
                },
            );
        }

        // Update with new metadata.
        let mut updated_meta = test_meta(node_id, 9200);
        updated_meta.capacity.used_bytes = 500_000;
        update_member_meta(
            &members,
            &RosterCell::default(),
            &identity(NodeId(1), 9000, 1),
            node_id,
            updated_meta.clone(),
        );

        let m = members.read().unwrap();
        let state = m.get(&node_id).unwrap();
        // Status should remain unchanged (only meta updates).
        assert_eq!(state.status, NodeStatus::Suspect);
        assert_eq!(state.meta.capacity.used_bytes, 500_000);
    }

    #[test]
    fn gossip_handle_alive_count() {
        let members = std::sync::Arc::new(std::sync::RwLock::new(HashMap::new()));
        let handle = test_handle(members.clone());

        assert_eq!(handle.alive_count(), 0);

        let id1 = NodeId::generate();
        let id2 = NodeId::generate();
        {
            let mut m = members.write().unwrap();
            m.insert(
                id1,
                NodeState {
                    meta: test_meta(id1, 9200),
                    status: NodeStatus::Alive,
                    last_seen: Instant::now(),
                },
            );
            m.insert(
                id2,
                NodeState {
                    meta: test_meta(id2, 9201),
                    status: NodeStatus::Dead,
                    last_seen: Instant::now(),
                },
            );
        }

        assert_eq!(handle.alive_count(), 1);
    }

    #[test]
    fn gossip_handle_get_node() {
        let members = std::sync::Arc::new(std::sync::RwLock::new(HashMap::new()));
        let handle = test_handle(members.clone());

        let id = NodeId::generate();
        assert!(handle.get_node(&id).is_none());

        {
            let mut m = members.write().unwrap();
            m.insert(
                id,
                NodeState {
                    meta: test_meta(id, 9200),
                    status: NodeStatus::Alive,
                    last_seen: Instant::now(),
                },
            );
        }

        let state = handle.get_node(&id).unwrap();
        assert_eq!(state.meta.node_id, id);
    }

    // -----------------------------------------------------------------------
    // EC v2 rolling-upgrade gate
    // -----------------------------------------------------------------------

    /// Build a handle whose members carry the given `(protocol_version, s4_version)`.
    fn handle_with_versions(specs: &[(NodeId, u32, &str)]) -> GossipHandle {
        let mut members = HashMap::new();
        for (id, protocol_version, s4_version) in specs {
            let mut meta = test_meta(*id, 9200);
            meta.protocol_version = *protocol_version;
            meta.s4_version = (*s4_version).to_string();
            members.insert(
                *id,
                NodeState {
                    meta,
                    status: NodeStatus::Alive,
                    last_seen: Instant::now(),
                },
            );
        }
        GossipHandle::new_for_testing(members)
    }

    #[test]
    fn a_pool_of_v2_nodes_supports_ec_v2() {
        let a = NodeId::generate();
        let b = NodeId::generate();
        let handle = handle_with_versions(&[(a, 2, "0.1.0"), (b, 2, "0.1.0")]);

        assert!(handle.pool_nodes_support_v2(&[a, b]));
    }

    /// A newer node than this binary still supports v2: the capability floor is
    /// a minimum, not an equality.
    #[test]
    fn a_newer_node_still_counts_as_v2_capable() {
        let a = NodeId::generate();
        let handle = handle_with_versions(&[(a, EC_V2_MIN_PROTOCOL_VERSION + 7, "9.9.9")]);

        assert!(handle.pool_nodes_support_v2(&[a]));
    }

    /// The case the gate exists for: one node in the pool is still on v1.
    #[test]
    fn one_v1_node_makes_the_whole_pool_unsupported() {
        let upgraded = NodeId::generate();
        let old = NodeId::generate();
        let handle = handle_with_versions(&[(upgraded, 2, "0.1.0"), (old, 1, "0.0.9")]);

        assert!(!handle.pool_nodes_support_v2(&[upgraded, old]));
        // ...and the upgraded node on its own is still fine, so the verdict
        // really is about the pool and not about the handle.
        assert!(handle.pool_nodes_support_v2(&[upgraded]));
    }

    /// A member that has joined gossip but whose metadata has not arrived yet
    /// carries the placeholder from `handle_notification`. It must not be read
    /// as a v2 node, and it must not be read as absent either.
    #[test]
    fn a_member_with_placeholder_metadata_is_not_yet_v2() {
        let joining = NodeId::generate();
        let handle = handle_with_versions(&[(joining, 0, "")]);

        assert!(!handle.pool_nodes_support_v2(&[joining]));
    }

    /// Version string present but protocol still below the floor, and the
    /// reverse. Either half alone is not a confirmation.
    #[test]
    fn both_halves_of_the_confirmation_are_required() {
        let no_version = NodeId::generate();
        let old_protocol = NodeId::generate();
        let handle = handle_with_versions(&[(no_version, 2, ""), (old_protocol, 1, "0.1.0")]);

        assert!(!handle.pool_nodes_support_v2(&[no_version]));
        assert!(!handle.pool_nodes_support_v2(&[old_protocol]));
    }

    /// A pool node that has not joined gossip at all is unknown, and unknown
    /// has to read the same as unsupported.
    #[test]
    fn a_node_missing_from_gossip_makes_the_pool_unsupported() {
        let present = NodeId::generate();
        let never_joined = NodeId::generate();
        let handle = handle_with_versions(&[(present, 2, "0.1.0")]);

        assert!(!handle.pool_nodes_support_v2(&[present, never_joined]));
    }

    /// A pool node that has been declared down keeps its metadata, so it does
    /// not turn a uniform pool into a mixed one. Tiering pausing every time a
    /// node reboots would make the feature unusable, and a dead node cannot
    /// misread a migration it is not part of.
    #[test]
    fn a_dead_node_does_not_make_the_pool_look_mixed() {
        let alive = NodeId::generate();
        let dead = NodeId::generate();
        let handle = handle_with_versions(&[(alive, 2, "0.1.0"), (dead, 2, "0.1.0")]);
        {
            let mut m = handle.members.write().unwrap();
            m.get_mut(&dead).unwrap().status = NodeStatus::Dead;
        }

        assert!(handle.pool_nodes_support_v2(&[alive, dead]));
    }

    /// An empty pool means the caller does not know its own membership.
    #[test]
    fn an_empty_pool_is_not_supported() {
        let handle = handle_with_versions(&[]);

        assert!(!handle.pool_nodes_support_v2(&[]));
    }

    #[test]
    fn the_mock_answers_with_its_fixed_verdict() {
        let id = NodeId::generate();
        assert!(MockClusterMembership::new(true).pool_nodes_support_v2(&[id]));
        assert!(!MockClusterMembership::new(false).pool_nodes_support_v2(&[id]));
        // Default is the closed verdict, so a forgotten field cannot open the
        // gate.
        assert!(!MockClusterMembership::default().pool_nodes_support_v2(&[id]));
    }

    // -----------------------------------------------------------------------
    // Erasure-set layout record gate (EC2-084, point 4)
    // -----------------------------------------------------------------------

    /// The nodes below a protocol floor are named, so a refusal can say which
    /// ones are holding the pool back.
    #[test]
    fn nodes_below_a_protocol_floor_are_named() {
        let current = NodeId::generate();
        let ec_v2_only = NodeId::generate();
        let handle = handle_with_versions(&[
            (current, EC_SET_LAYOUT_MIN_PROTOCOL_VERSION, "0.2.0"),
            (ec_v2_only, EC_V2_MIN_PROTOCOL_VERSION, "0.1.0"),
        ]);

        assert_eq!(
            handle.nodes_below_protocol(&[current, ec_v2_only], EC_SET_LAYOUT_MIN_PROTOCOL_VERSION),
            vec![ec_v2_only]
        );
        // The same EC v2 node passes the EC v2 gate: the two floors are
        // separate questions.
        assert!(handle.pool_nodes_support_v2(&[current, ec_v2_only]));
        assert!(handle
            .nodes_below_protocol(&[current, ec_v2_only], EC_V2_MIN_PROTOCOL_VERSION)
            .is_empty());
    }

    /// Unknown reads as unconfirmed here too: a node missing from gossip and a
    /// node whose metadata has not arrived are both named.
    #[test]
    fn unknown_nodes_are_below_every_protocol_floor() {
        let joining = NodeId::generate();
        let never_joined = NodeId::generate();
        let handle = handle_with_versions(&[(joining, 0, "")]);

        assert_eq!(
            handle.nodes_below_protocol(&[joining, never_joined], MIN_COMPATIBLE_VERSION),
            vec![joining, never_joined]
        );
    }

    #[test]
    fn the_mock_confirms_the_protocol_version_it_was_given() {
        let id = NodeId::generate();
        assert!(MockClusterMembership::new(true)
            .nodes_below_protocol(&[id], EC_SET_LAYOUT_MIN_PROTOCOL_VERSION)
            .is_empty());
        assert_eq!(
            MockClusterMembership::with_protocol_version(EC_V2_MIN_PROTOCOL_VERSION)
                .nodes_below_protocol(&[id], EC_SET_LAYOUT_MIN_PROTOCOL_VERSION),
            vec![id]
        );
        assert!(
            MockClusterMembership::with_protocol_version(EC_V2_MIN_PROTOCOL_VERSION)
                .pool_nodes_support_v2(&[id])
        );
        assert_eq!(
            MockClusterMembership::default()
                .nodes_below_protocol(&[id], EC_SET_LAYOUT_MIN_PROTOCOL_VERSION),
            vec![id]
        );
    }

    // -----------------------------------------------------------------------
    // Membership bookkeeping
    // -----------------------------------------------------------------------

    fn identity(node_id: NodeId, port: u16, incarnation: u64) -> GossipIdentity {
        GossipIdentity {
            node_id,
            gossip_addr: localhost(port),
            incarnation,
        }
    }

    /// One node's gossip view, held the way the run-loop holds it: the member
    /// map, this node's own identity, and what it believes every other node's
    /// identity to be. Notifications take the same path they take in the loop.
    struct View {
        peer_view: GossipHandle,
        members: std::sync::Arc<std::sync::RwLock<HashMap<NodeId, NodeState>>>,
        roster: RosterCell,
        local: GossipIdentity,
        identities: IdentityTable,
        event_tx: mpsc::Sender<ClusterEvent>,
        events: mpsc::Receiver<ClusterEvent>,
    }

    impl View {
        fn new(local: GossipIdentity) -> Self {
            let (event_tx, events) = mpsc::channel(32);
            let members = std::sync::Arc::new(std::sync::RwLock::new(HashMap::new()));
            let roster = RosterCell::default();
            let mut peer_view = test_handle(members.clone());
            peer_view.local_node_id = local.node_id;
            peer_view.roster = roster.clone();
            Self {
                peer_view,
                members,
                roster,
                identities: IdentityTable::from([(local.node_id, local.clone())]),
                local,
                event_tx,
                events,
            }
        }

        async fn notify(&mut self, notification: foca::OwnedNotification<GossipIdentity>) -> bool {
            handle_notification(
                notification,
                &self.members,
                &self.roster,
                &self.event_tx,
                &mut self.local,
                &mut self.identities,
                &self.peer_view,
            )
            .await
        }

        fn meta_arrived(&self, meta: NodeMeta) {
            update_member_meta(&self.members, &self.roster, &self.local, meta.node_id, meta);
            self.peer_view.publish_peer_changes(Some(&self.identities));
        }

        fn state(&self, node_id: NodeId) -> Option<NodeState> {
            self.members.read().unwrap().get(&node_id).cloned()
        }

        fn status(&self, node_id: NodeId) -> Option<NodeStatus> {
            self.state(node_id).map(|state| state.status)
        }

        fn grpc_port(&self, node_id: NodeId) -> Option<u16> {
            self.state(node_id).map(|state| state.meta.addr.grpc_addr.port())
        }

        /// A handle over this view, as bootstrap holds one over the loop's.
        fn handle(&self) -> GossipHandle {
            self.peer_view.clone()
        }
    }

    /// The Cluster EE crash-recovery failure, in one sequence.
    ///
    /// A node restarts, lands on an address another member of its own cluster
    /// was using, and that member has meanwhile moved elsewhere. foca reports
    /// the takeover as a rename and — because the address never stopped
    /// answering — sends no join with it. Then the address the restarted node
    /// left behind is declared down, carrying its node id.
    ///
    /// Before this was handled, the rename was logged and dropped, so the
    /// returned node was never recorded as reachable, and the death notice for
    /// the address it had left marked it Dead by node id. Every peer then
    /// refused to talk to it for the rest of the cluster's life: "node … is
    /// Dead, skipping RPC", until the EC object it held could not be read.
    #[tokio::test]
    async fn a_node_that_comes_back_on_another_members_address_stays_reachable() {
        let returning = NodeId::generate();
        let displaced = NodeId::generate();
        let mut view = View::new(identity(NodeId::generate(), 9000, 100));

        view.notify(foca::OwnedNotification::MemberUp(identity(
            returning, 9001, 10,
        )))
        .await;
        view.notify(foca::OwnedNotification::MemberUp(identity(
            displaced, 9002, 11,
        )))
        .await;

        // The returning node takes over the address the displaced one held.
        view.notify(foca::OwnedNotification::Rename(
            identity(displaced, 9002, 11),
            identity(returning, 9002, 20),
        ))
        .await;

        assert_eq!(view.status(returning), Some(NodeStatus::Alive));
        assert_eq!(
            view.grpc_port(returning),
            Some(9002),
            "the returned node is addressed where it answers now"
        );
        assert!(
            view.state(displaced).is_none(),
            "the displaced node is not at that address any more, and this view does not know where it is"
        );

        // The address the returning node left behind is finally declared down.
        view.notify(foca::OwnedNotification::MemberDown(identity(
            returning, 9001, 10,
        )))
        .await;

        assert_eq!(
            view.status(returning),
            Some(NodeStatus::Alive),
            "a death notice for the previous run buried the live node"
        );
        assert_eq!(view.grpc_port(returning), Some(9002));
    }

    /// The same defect without the address takeover: the node comes back on a
    /// free address, so foca reports a plain join, and only afterwards declares
    /// the address it left.
    #[tokio::test]
    async fn a_death_notice_from_the_previous_run_does_not_bury_the_returned_node() {
        let node_id = NodeId::generate();
        let mut view = View::new(identity(NodeId::generate(), 9000, 100));

        view.notify(foca::OwnedNotification::MemberUp(identity(
            node_id, 9001, 10,
        )))
        .await;
        view.notify(foca::OwnedNotification::MemberUp(identity(
            node_id, 9002, 20,
        )))
        .await;
        view.notify(foca::OwnedNotification::MemberDown(identity(
            node_id, 9001, 10,
        )))
        .await;

        assert_eq!(view.status(node_id), Some(NodeStatus::Alive));
        assert_eq!(view.grpc_port(node_id), Some(9002));
    }

    /// A death notice for the run this view does track still buries the node:
    /// ignoring the stale ones must not cost failure detection.
    #[tokio::test]
    async fn a_death_notice_for_the_current_run_still_counts() {
        let node_id = NodeId::generate();
        let mut view = View::new(identity(NodeId::generate(), 9000, 100));

        view.notify(foca::OwnedNotification::MemberUp(identity(
            node_id, 9002, 20,
        )))
        .await;
        view.notify(foca::OwnedNotification::MemberDown(identity(
            node_id, 9002, 20,
        )))
        .await;

        assert_eq!(view.status(node_id), Some(NodeStatus::Dead));
    }

    /// A rumour about a run that has ended can still arrive as a join. Acting
    /// on it would move the node back to an address nothing answers on.
    #[tokio::test]
    async fn a_join_from_a_run_that_has_ended_is_ignored() {
        let node_id = NodeId::generate();
        let mut view = View::new(identity(NodeId::generate(), 9000, 100));

        view.notify(foca::OwnedNotification::MemberUp(identity(
            node_id, 9002, 20,
        )))
        .await;
        view.notify(foca::OwnedNotification::MemberUp(identity(
            node_id, 9001, 10,
        )))
        .await;

        assert_eq!(view.grpc_port(node_id), Some(9002));
        assert_eq!(
            view.identities.get(&node_id).map(|id| id.gossip_addr.port()),
            Some(9002),
            "the ended run was taken for the current one"
        );
    }

    /// Metadata describes an endpoint. Once the node answers somewhere else,
    /// the addresses in the old record are wrong, and the one it names may
    /// already belong to another node — so they are dropped rather than kept.
    #[tokio::test]
    async fn a_node_that_moved_is_not_addressed_at_its_old_endpoint() {
        let node_id = NodeId::generate();
        let mut view = View::new(identity(NodeId::generate(), 9000, 100));

        view.notify(foca::OwnedNotification::MemberUp(identity(
            node_id, 9001, 10,
        )))
        .await;
        view.meta_arrived(test_meta(node_id, 9001));
        assert_eq!(view.grpc_port(node_id), Some(9001));

        view.notify(foca::OwnedNotification::MemberUp(identity(
            node_id, 9002, 20,
        )))
        .await;
        assert_eq!(view.grpc_port(node_id), Some(9002));

        view.meta_arrived(test_meta(node_id, 9002));
        assert_eq!(view.grpc_port(node_id), Some(9002));
        assert_eq!(view.state(node_id).unwrap().meta.s4_version, "0.1.0");
    }

    /// After a restart on a new address, the cluster goes on gossiping the
    /// previous run for a while. It carries this node's own id, so read as a
    /// peer its death notice is what marks this node dead in its own view.
    #[tokio::test]
    async fn a_node_ignores_the_ghost_of_its_own_previous_run() {
        let node_id = NodeId::generate();
        let mut view = View::new(identity(node_id, 9000, 100));
        // A node's own broadcast comes back through the same handler, which is
        // how its own entry gets there in the first place.
        view.meta_arrived(test_meta(node_id, 9000));

        view.notify(foca::OwnedNotification::MemberUp(identity(
            node_id, 8999, 10,
        )))
        .await;
        view.notify(foca::OwnedNotification::MemberDown(identity(
            node_id, 8999, 10,
        )))
        .await;

        assert_eq!(
            view.status(node_id),
            Some(NodeStatus::Alive),
            "this node declared itself dead on behalf of its previous run"
        );
        assert_eq!(view.grpc_port(node_id), Some(9000));
    }

    /// Nothing but this node is at this node's address, so a record that claims
    /// it for another node is a record from a run that has since moved. Stored,
    /// it would send that node's RPCs back to this one.
    #[tokio::test]
    async fn metadata_claiming_this_nodes_own_address_is_ignored() {
        let local = NodeId::generate();
        let moved = NodeId::generate();
        let view = View::new(identity(local, 9000, 100));

        view.meta_arrived(test_meta(local, 9000));
        view.meta_arrived(test_meta(moved, 9000));

        assert!(view.state(moved).is_none());
        assert_eq!(view.grpc_port(local), Some(9000));
    }

    /// Two remote nodes disagreeing about an address is settled by the identity
    /// notifications, not by refusing to hear one of them: a refused record is
    /// not re-sent until that node's next metadata tick, and until then the node
    /// is missing from every answer this view gives — the rolling-upgrade gate
    /// among them.
    #[tokio::test]
    async fn metadata_from_a_peer_is_stored_even_where_this_view_expects_another() {
        let present = NodeId::generate();
        let moved = NodeId::generate();
        let mut view = View::new(identity(NodeId::generate(), 9000, 100));

        view.notify(foca::OwnedNotification::MemberUp(identity(
            present, 9001, 10,
        )))
        .await;
        view.meta_arrived(test_meta(moved, 9001));

        assert_eq!(
            view.state(moved).map(|state| state.meta.s4_version),
            Some("0.1.0".to_string())
        );
    }

    /// `MemberUp` must not overwrite metadata that already arrived.
    ///
    /// The join notification and the node's metadata broadcast race, and the
    /// broadcast often wins. Replacing real metadata with the placeholder would
    /// report `protocol_version: 0` — "not EC v2 capable" — and, because
    /// `MetaHandler` drops a re-sent broadcast of the same version as stale,
    /// that wrong answer would stand until the peer's next metadata tick.
    #[tokio::test]
    async fn member_up_keeps_metadata_that_already_arrived() {
        let node_id = NodeId::generate();
        let mut view = View::new(identity(NodeId::generate(), 9000, 100));

        let mut meta = test_meta(node_id, 9200);
        meta.protocol_version = 2;
        view.meta_arrived(meta);

        let news = view
            .notify(foca::OwnedNotification::MemberUp(identity(
                node_id, 9200, 0,
            )))
            .await;

        assert!(
            news,
            "a run this node did not track yet is told this node's metadata"
        );
        let state = view.state(node_id).unwrap();
        assert_eq!(
            state.meta.protocol_version, 2,
            "real metadata was overwritten"
        );
        assert_eq!(state.meta.s4_version, "0.1.0");
        assert_eq!(state.status, NodeStatus::Alive);

        let handle = test_handle(view.members.clone());
        assert!(handle.pool_nodes_support_v2(&[node_id]));

        match view.events.try_recv().unwrap() {
            ClusterEvent::Joined(id, announced) => {
                assert_eq!(id, node_id);
                assert_eq!(
                    announced.protocol_version, 2,
                    "the event announced the placeholder"
                );
            }
            other => panic!("expected Joined, got {other:?}"),
        }
    }

    /// A node that comes back as a new run — restarted — knows nothing of its
    /// peers, as a fresh one: the join is answered with this node's metadata,
    /// so that the tails it needs — its copy of IAM confirmed among them —
    /// reach it in one round rather than at the next periodic broadcast. A
    /// join of the run already tracked is not.
    #[tokio::test]
    async fn a_node_back_as_a_new_run_is_told_this_nodes_metadata() {
        let node_id = NodeId::generate();
        let mut view = View::new(identity(NodeId::generate(), 9000, 100));
        assert!(
            view.notify(foca::OwnedNotification::MemberUp(identity(
                node_id, 9200, 10
            )))
            .await
        );
        view.meta_arrived(test_meta(node_id, 9200));
        assert!(
            !view
                .notify(foca::OwnedNotification::MemberUp(identity(
                    node_id, 9200, 10
                )))
                .await,
            "the same run again is no news"
        );
        assert!(
            view.notify(foca::OwnedNotification::MemberUp(identity(
                node_id, 9200, 20
            )))
            .await,
            "a restarted node is told this node's metadata"
        );
        assert!(
            !view
                .notify(foca::OwnedNotification::MemberUp(identity(
                    node_id, 9200, 15
                )))
                .await,
            "a run this node has moved on from is ignored"
        );
    }

    /// A node nobody has heard from still gets an entry, so that failure
    /// detection has something to mark down — but a placeholder one.
    #[tokio::test]
    async fn member_up_inserts_a_placeholder_for_an_unknown_node() {
        let node_id = NodeId::generate();
        let mut view = View::new(identity(NodeId::generate(), 9000, 100));

        let first_sighting = view
            .notify(foca::OwnedNotification::MemberUp(identity(
                node_id, 9200, 0,
            )))
            .await;

        assert!(first_sighting, "an unknown node is a first sighting");
        let state = view.state(node_id).unwrap();
        assert_eq!(state.meta.protocol_version, 0);
        assert!(state.meta.s4_version.is_empty());

        let handle = test_handle(view.members.clone());
        assert!(!handle.pool_nodes_support_v2(&[node_id]));
    }

    /// Going down and coming back must not lose the metadata either.
    #[tokio::test]
    async fn a_flapping_member_keeps_its_capabilities() {
        let node_id = NodeId::generate();
        let mut view = View::new(identity(NodeId::generate(), 9000, 100));

        let mut meta = test_meta(node_id, 9200);
        meta.protocol_version = 2;
        view.meta_arrived(meta);

        view.notify(foca::OwnedNotification::MemberUp(identity(
            node_id, 9200, 0,
        )))
        .await;
        view.notify(foca::OwnedNotification::MemberDown(identity(
            node_id, 9200, 0,
        )))
        .await;
        view.notify(foca::OwnedNotification::MemberUp(identity(
            node_id, 9200, 0,
        )))
        .await;

        let handle = test_handle(view.members.clone());
        assert!(handle.pool_nodes_support_v2(&[node_id]));
        assert_eq!(handle.get_node(&node_id).unwrap().status, NodeStatus::Alive);
    }

    /// A renewed identity for the same node is an address change at most — the
    /// member stays whoever it was, with the liveness it had.
    #[tokio::test]
    async fn a_rename_of_the_same_node_keeps_it_alive() {
        let node_id = NodeId::generate();
        let mut view = View::new(identity(NodeId::generate(), 9000, 100));

        view.notify(foca::OwnedNotification::MemberUp(identity(
            node_id, 9001, 10,
        )))
        .await;
        view.notify(foca::OwnedNotification::Rename(
            identity(node_id, 9001, 10),
            identity(node_id, 9001, 11),
        ))
        .await;

        assert_eq!(view.status(node_id), Some(NodeStatus::Alive));
        assert_eq!(view.grpc_port(node_id), Some(9001));
        assert_eq!(
            view.identities.get(&node_id).map(|id| id.incarnation),
            Some(11),
            "the renewed identity is the one this view now tracks"
        );
    }

    /// An address taken over while its previous holder was already down leaves
    /// that holder down: a rename says where a node is not, never that another
    /// node is well.
    #[tokio::test]
    async fn a_rename_inherits_the_state_of_the_address_it_replaces() {
        let taking = NodeId::generate();
        let leaving = NodeId::generate();
        let mut view = View::new(identity(NodeId::generate(), 9000, 100));

        view.notify(foca::OwnedNotification::MemberUp(identity(
            leaving, 9001, 10,
        )))
        .await;
        view.notify(foca::OwnedNotification::MemberDown(identity(
            leaving, 9001, 10,
        )))
        .await;
        view.notify(foca::OwnedNotification::Rename(
            identity(leaving, 9001, 10),
            identity(taking, 9001, 20),
        ))
        .await;

        assert_eq!(
            view.status(taking),
            Some(NodeStatus::Dead),
            "foca reports a join separately when the address starts answering"
        );

        view.notify(foca::OwnedNotification::MemberUp(identity(
            taking, 9001, 20,
        )))
        .await;
        assert_eq!(view.status(taking), Some(NodeStatus::Alive));
    }

    // -----------------------------------------------------------------------
    // Membership metrics
    // -----------------------------------------------------------------------

    /// `s4_cluster_nodes` of pool-1 by status, in the order alive, suspect,
    /// dead, left; `None` for a series that is not there.
    fn node_counts(metrics: &crate::test_metrics::TestMetrics) -> Vec<Option<f64>> {
        ["alive", "suspect", "dead", "left"]
            .iter()
            .map(|status| {
                metrics.value(
                    "s4_cluster_nodes",
                    &[("pool", "pool-1"), ("status", status)],
                )
            })
            .collect()
    }

    fn peer_up(metrics: &crate::test_metrics::TestMetrics, peer: &str) -> Option<f64> {
        metrics.value("s4_cluster_peer_up", &[("peer", peer)])
    }

    /// The roster of pool-1: this node as node-1 and its peers as node-2, ….
    fn roster(local: NodeId, peers: &[NodeId]) -> PoolRoster {
        let members = std::iter::once(local)
            .chain(peers.iter().copied())
            .enumerate()
            .map(|(idx, node_id)| (node_id, format!("node-{}", idx + 1)));
        PoolRoster::new("pool-1", local, members)
    }

    /// Alive → Dead → Alive shows in the metrics as each notification lands,
    /// not at the next scrape of some other state. Suspect and Left are never
    /// set by this code (plan EXTENDED-METRICS.md, context item 7); their rows
    /// are there, at zero, so a dashboard needs no special case for them.
    #[tokio::test]
    async fn membership_metrics_follow_a_peer_down_and_back() {
        let metrics = crate::test_metrics::TestMetrics::install();
        let local = NodeId::generate();
        let (up, flapping) = (NodeId::generate(), NodeId::generate());
        let mut view = View::new(identity(local, 9000, 100));
        view.notify(foca::OwnedNotification::MemberUp(identity(up, 9001, 10))).await;
        view.notify(foca::OwnedNotification::MemberUp(identity(
            flapping, 9002, 10,
        )))
        .await;
        assert_eq!(
            node_counts(&metrics),
            vec![None; 4],
            "nothing is reported before the pool is named"
        );

        view.handle().report_membership(roster(local, &[up, flapping]));
        assert_eq!(
            node_counts(&metrics),
            vec![Some(3.0), Some(0.0), Some(0.0), Some(0.0)]
        );
        assert_eq!(peer_up(&metrics, "node-2"), Some(1.0));
        assert_eq!(peer_up(&metrics, "node-3"), Some(1.0));
        assert_eq!(
            peer_up(&metrics, "node-1"),
            None,
            "a node is not its own peer"
        );

        view.notify(foca::OwnedNotification::MemberDown(identity(
            flapping, 9002, 10,
        )))
        .await;
        assert_eq!(
            node_counts(&metrics),
            vec![Some(2.0), Some(0.0), Some(1.0), Some(0.0)]
        );
        assert_eq!(peer_up(&metrics, "node-3"), Some(0.0));
        assert_eq!(peer_up(&metrics, "node-2"), Some(1.0));

        view.notify(foca::OwnedNotification::MemberUp(identity(
            flapping, 9002, 11,
        )))
        .await;
        assert_eq!(
            node_counts(&metrics),
            vec![Some(3.0), Some(0.0), Some(0.0), Some(0.0)]
        );
        assert_eq!(peer_up(&metrics, "node-3"), Some(1.0));
    }

    /// A configured member this node has never heard from is down as far as
    /// this node can tell, and the pool's counts still add up to its size.
    #[tokio::test]
    async fn a_member_never_heard_from_counts_as_dead() {
        let metrics = crate::test_metrics::TestMetrics::install();
        let local = NodeId::generate();
        let (heard, silent) = (NodeId::generate(), NodeId::generate());
        let mut view = View::new(identity(local, 9000, 100));
        view.notify(foca::OwnedNotification::MemberUp(identity(heard, 9001, 10))).await;

        view.handle().report_membership(roster(local, &[heard, silent]));
        assert_eq!(
            node_counts(&metrics),
            vec![Some(2.0), Some(0.0), Some(1.0), Some(0.0)]
        );
        assert_eq!(peer_up(&metrics, "node-3"), Some(0.0));

        // Its metadata can arrive before its join; either makes it a member.
        view.meta_arrived(test_meta(silent, 9002));
        assert_eq!(
            node_counts(&metrics),
            vec![Some(3.0), Some(0.0), Some(0.0), Some(0.0)]
        );
        assert_eq!(peer_up(&metrics, "node-3"), Some(1.0));
    }

    /// Membership is counted by node, so a node that came back on another
    /// address is one member, and so is a node whose previous run is still
    /// being declared down. A node outside the pool is not counted at all.
    #[tokio::test]
    async fn a_node_is_counted_once_and_only_inside_its_pool() {
        let metrics = crate::test_metrics::TestMetrics::install();
        let local = NodeId::generate();
        let (moving, stranger) = (NodeId::generate(), NodeId::generate());
        let mut view = View::new(identity(local, 9000, 100));
        view.handle().report_membership(roster(local, &[moving]));

        view.notify(foca::OwnedNotification::MemberUp(identity(
            moving, 9001, 10,
        )))
        .await;
        view.notify(foca::OwnedNotification::MemberUp(identity(
            moving, 9005, 20,
        )))
        .await;
        view.notify(foca::OwnedNotification::MemberDown(identity(
            moving, 9001, 10,
        )))
        .await;
        view.notify(foca::OwnedNotification::MemberUp(identity(
            stranger, 9007, 10,
        )))
        .await;

        assert_eq!(view.grpc_port(moving), Some(9005));
        assert_eq!(
            node_counts(&metrics),
            vec![Some(2.0), Some(0.0), Some(0.0), Some(0.0)]
        );
        assert_eq!(peer_up(&metrics, "node-2"), Some(1.0));
        assert_eq!(
            peer_up(&metrics, &stranger.to_string()),
            None,
            "a node outside the pool got a series"
        );
    }

    // -----------------------------------------------------------------------
    // Local capacity tracking
    // -----------------------------------------------------------------------

    /// Capacity measurements are needed by two distinct readers: peers that
    /// receive it in the next broadcast, and local placement which reads it
    /// directly. Updating one without the other would cause them to drift apart.
    #[test]
    fn refresh_local_capacity_updates_the_broadcast_the_local_view_and_the_metrics() {
        let metrics = crate::test_metrics::TestMetrics::install();
        let mut meta = test_meta(NodeId(1), 9300);
        let cell = std::sync::Arc::new(std::sync::RwLock::new(LocalCapacity {
            capacity: meta.capacity.clone(),
            measured_at: Instant::now(),
        }));
        let provider = StubCapacity(crate::identity::NodeCapacity {
            total_bytes: 900,
            used_bytes: 400,
            volume_count: 0,
            object_count: 0,
        });
        refresh_local_capacity(Some(&provider), &mut meta, &cell);

        assert_eq!(meta.capacity.total_bytes, 900);
        assert_eq!(meta.capacity.used_bytes, 400);

        let local = cell.read().unwrap();
        assert_eq!(local.capacity.total_bytes, 900);
        assert_eq!(local.capacity.used_bytes, 400);

        // The metrics show the same measurement the peers receive.
        assert_eq!(metrics.value("s4_node_disk_total_bytes", &[]), Some(900.0));
        assert_eq!(metrics.value("s4_node_disk_used_bytes", &[]), Some(400.0));
    }

    /// A caller that supplied no way to measure promised no fresh data. Passing
    /// off untouched numbers as freshly measured would be a falsehood that
    /// capacity-aware placement would rely on.
    #[test]
    fn refresh_local_capacity_without_a_provider_leaves_the_numbers_alone() {
        let metrics = crate::test_metrics::TestMetrics::install();
        let mut meta = test_meta(NodeId(1), 9300);
        let cell = std::sync::Arc::new(std::sync::RwLock::new(LocalCapacity {
            capacity: meta.capacity.clone(),
            measured_at: Instant::now(),
        }));

        refresh_local_capacity(None, &mut meta, &cell);

        assert_eq!(meta.capacity.total_bytes, 1_000_000);
        assert_eq!(meta.capacity.used_bytes, 0);

        let local = cell.read().unwrap();
        assert_eq!(local.capacity.total_bytes, 1_000_000);
        assert_eq!(local.capacity.used_bytes, 0);
        assert_eq!(
            metrics.value("s4_node_disk_total_bytes", &[]),
            None,
            "nothing was measured"
        );
    }

    /// A node never appears in its own membership map, which is built from peer
    /// announcements. Placement queries every erasure set node including this one;
    /// without answering for itself, capacity-aware placement would never engage.
    #[test]
    fn the_local_node_answers_a_capacity_question_about_itself() {
        let local = NodeId(7);
        let other = NodeId(8);
        let handle = GossipHandle {
            peer_changes: std::sync::Arc::default(),
            local_pool_id: crate::PoolId(1),
            members: std::sync::Arc::new(std::sync::RwLock::new(HashMap::new())),
            local_node_id: local,
            local_capacity: std::sync::Arc::new(std::sync::RwLock::new(LocalCapacity {
                capacity: crate::identity::NodeCapacity {
                    total_bytes: 500,
                    used_bytes: 100,
                    volume_count: 0,
                    object_count: 0,
                },
                measured_at: Instant::now(),
            })),
            roster: RosterCell::default(),
            adopted_peers: AdoptedPeers::default(),
            adopted_locally: std::sync::Arc::default(),
            placement_peers: PlacementPeers::default(),
            placement_local: PlacementLocal::default(),
            iam_peers: IamPeers::default(),
            iam_local: IamLocal::default(),
            iam_adopted_peers: AdoptedPeers::default(),
            iam_adopted_local: std::sync::Arc::default(),
            iam_config_peers: IamConfigPeers::default(),
            iam_config_local: IamConfigLocal::default(),
            generation_peers: GenerationPeers::default(),
            generation_local: GenerationLocal::default(),
            announce: std::sync::Arc::default(),
        };

        let report = handle.node_capacity(local).unwrap();
        assert_eq!(report.capacity.total_bytes, 500);
        assert_eq!(report.capacity.available_bytes(), 400);
        assert!(report.age < std::time::Duration::from_secs(5));
        assert!(handle.node_capacity(other).is_none());
    }
}
