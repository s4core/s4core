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

use crate::error::ClusterError;
use crate::identity::{GossipIdentity, NodeId, NodeMeta, NodeStatus};

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

/// Key used by foca to deduplicate and invalidate stale broadcasts.
#[derive(Debug, Clone, PartialEq, Eq)]
struct MetaBroadcastKey {
    node_id: NodeId,
    version: u64,
}

impl Invalidates for MetaBroadcastKey {
    /// A newer broadcast for the same node supersedes older ones.
    fn invalidates(&self, other: &Self) -> bool {
        self.node_id == other.node_id && self.version >= other.version
    }
}

/// Handler that processes incoming metadata broadcasts.
struct MetaHandler {
    /// Highest version seen per node — used to drop stale broadcasts.
    seen_versions: HashMap<NodeId, u64>,
    /// Channel for forwarding new metadata to the async run-loop.
    meta_tx: mpsc::UnboundedSender<(NodeId, NodeMeta)>,
}

impl BroadcastHandler<GossipIdentity> for MetaHandler {
    type Key = MetaBroadcastKey;
    type Error = MetaHandlerError;

    fn receive_item(
        &mut self,
        data: &[u8],
        _sender: Option<&GossipIdentity>,
    ) -> Result<Option<Self::Key>, Self::Error> {
        let broadcast: MetaBroadcast =
            postcard::from_bytes(data).map_err(|e| MetaHandlerError(e.to_string()))?;

        let current = self.seen_versions.get(&broadcast.node_id).copied().unwrap_or(0);
        if broadcast.version <= current {
            return Ok(None); // stale
        }

        self.seen_versions.insert(broadcast.node_id, broadcast.version);
        // Forward to async loop; if the receiver is dropped we just discard.
        let _ = self.meta_tx.send((broadcast.node_id, broadcast.meta));

        Ok(Some(MetaBroadcastKey {
            node_id: broadcast.node_id,
            version: broadcast.version,
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

/// Handle returned by [`GossipService::start`] for querying cluster state.
pub struct GossipHandle {
    /// Current view of all known cluster members.
    members: std::sync::Arc<std::sync::RwLock<HashMap<NodeId, NodeState>>>,
}

impl GossipHandle {
    /// Return a snapshot of all known member states.
    pub fn members(&self) -> HashMap<NodeId, NodeState> {
        self.members.read().unwrap_or_else(|poisoned| poisoned.into_inner()).clone()
    }

    /// Look up a specific node's state.
    pub fn get_node(&self, node_id: &NodeId) -> Option<NodeState> {
        self.members
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(node_id)
            .cloned()
    }

    /// Create a `GossipHandle` with pre-populated members (for testing).
    #[cfg(test)]
    pub fn new_for_testing(members: HashMap<NodeId, NodeState>) -> Self {
        Self {
            members: std::sync::Arc::new(std::sync::RwLock::new(members)),
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
    pub async fn start(
        gossip_config: GossipConfig,
        local_identity: GossipIdentity,
        local_meta: NodeMeta,
    ) -> Result<(GossipHandle, mpsc::Receiver<ClusterEvent>), ClusterError> {
        let socket = UdpSocket::bind(gossip_config.bind_addr).await?;
        let local_addr = socket.local_addr()?;
        info!(%local_addr, node_id = %local_identity.node_id, "gossip service starting");

        let members: std::sync::Arc<std::sync::RwLock<HashMap<NodeId, NodeState>>> =
            std::sync::Arc::new(std::sync::RwLock::new(HashMap::new()));

        let (event_tx, event_rx) = mpsc::channel(256);
        let (meta_tx, meta_rx) = mpsc::unbounded_channel();

        let foca_config = build_foca_config(gossip_config.max_packet_size);

        let handler = MetaHandler {
            seen_versions: HashMap::new(),
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

        let handle = GossipHandle {
            members: members.clone(),
        };

        tokio::spawn(run_loop(
            socket,
            Mutex::new(foca),
            members,
            event_tx,
            meta_rx,
            local_identity,
            local_meta,
            gossip_config.seeds,
            gossip_config.meta_broadcast_interval,
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
    socket: UdpSocket,
    foca: Mutex<S4Foca>,
    members: std::sync::Arc<std::sync::RwLock<HashMap<NodeId, NodeState>>>,
    event_tx: mpsc::Sender<ClusterEvent>,
    mut meta_rx: mpsc::UnboundedReceiver<(NodeId, NodeMeta)>,
    local_identity: GossipIdentity,
    local_meta: NodeMeta,
    seeds: Vec<SocketAddr>,
    meta_broadcast_interval: Duration,
) {
    let mut recv_buf = vec![0u8; 65535];
    let (timer_tx, mut timer_rx) = mpsc::unbounded_channel::<Timer<GossipIdentity>>();

    let mut meta_version: u64 = 0;
    let mut meta_interval = tokio::time::interval(meta_broadcast_interval);
    meta_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    // Announce to seed nodes.
    announce_to_seeds(&foca, &seeds, &local_identity, &socket, &timer_tx).await;

    // Broadcast our own metadata immediately.
    broadcast_local_meta(&foca, &local_meta, &mut meta_version);

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
                        process_outputs(outputs, &socket, &timer_tx, &members, &event_tx).await;
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
                process_outputs(outputs, &socket, &timer_tx, &members, &event_tx).await;
            }

            // --- Received metadata from broadcast handler ---
            Some((node_id, meta)) = meta_rx.recv() => {
                update_member_meta(&members, node_id, meta);
            }

            // --- Periodic local metadata broadcast ---
            _ = meta_interval.tick() => {
                broadcast_local_meta(&foca, &local_meta, &mut meta_version);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

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

        info!(seed = %seed_addr, "announced to seed node");
    }
}

fn broadcast_local_meta(foca: &Mutex<S4Foca>, meta: &NodeMeta, version: &mut u64) {
    *version += 1;
    let broadcast = MetaBroadcast {
        node_id: meta.node_id,
        meta: meta.clone(),
        version: *version,
    };

    match postcard::to_allocvec(&broadcast) {
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

async fn process_outputs(
    outputs: RuntimeOutputs,
    socket: &UdpSocket,
    timer_tx: &mpsc::UnboundedSender<Timer<GossipIdentity>>,
    members: &std::sync::Arc<std::sync::RwLock<HashMap<NodeId, NodeState>>>,
    event_tx: &mpsc::Sender<ClusterEvent>,
) {
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
    for notification in outputs.notifications {
        handle_notification(notification, members, event_tx).await;
    }
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

async fn handle_notification(
    notification: foca::OwnedNotification<GossipIdentity>,
    members: &std::sync::Arc<std::sync::RwLock<HashMap<NodeId, NodeState>>>,
    event_tx: &mpsc::Sender<ClusterEvent>,
) {
    match notification {
        foca::OwnedNotification::MemberUp(identity) => {
            let node_id = identity.node_id;
            info!(%node_id, addr = %identity.gossip_addr, "node joined cluster");

            let placeholder_meta = NodeMeta {
                node_id,
                node_name: format!("node-{}", &node_id.to_string()[..8]),
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
            };

            let state = NodeState {
                meta: placeholder_meta.clone(),
                status: NodeStatus::Alive,
                last_seen: Instant::now(),
            };

            {
                let mut m = members.write().unwrap_or_else(|p| p.into_inner());
                m.insert(node_id, state);
            }

            let _ = event_tx.send(ClusterEvent::Joined(node_id, Box::new(placeholder_meta))).await;
        }

        foca::OwnedNotification::MemberDown(identity) => {
            let node_id = identity.node_id;
            info!(%node_id, "node left cluster");

            {
                let mut m = members.write().unwrap_or_else(|p| p.into_inner());
                if let Some(state) = m.get_mut(&node_id) {
                    state.status = NodeStatus::Dead;
                    state.last_seen = Instant::now();
                }
            }

            let _ = event_tx.send(ClusterEvent::Left(node_id)).await;
        }

        foca::OwnedNotification::Active => {
            info!("gossip: this node is now active in the cluster");
        }

        foca::OwnedNotification::Idle => {
            debug!("gossip: this node is idle (no known active members)");
        }

        foca::OwnedNotification::Defunct => {
            warn!("gossip: this node has been declared defunct by the cluster");
        }

        foca::OwnedNotification::Rejoin(new_identity) => {
            info!(
                incarnation = new_identity.incarnation,
                "gossip: rejoined cluster with new incarnation"
            );
        }

        foca::OwnedNotification::Rename(old, new) => {
            let old_id = old.node_id;
            let new_id = new.node_id;
            debug!(%old_id, %new_id, "gossip: member identity renamed");
        }
    }
}

fn update_member_meta(
    members: &std::sync::Arc<std::sync::RwLock<HashMap<NodeId, NodeState>>>,
    node_id: NodeId,
    meta: NodeMeta,
) {
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
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, SocketAddrV4};

    fn localhost(port: u16) -> SocketAddr {
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port))
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

    #[test]
    fn meta_broadcast_key_invalidates_same_node_older_version() {
        let node = NodeId::generate();
        let new_key = MetaBroadcastKey {
            node_id: node,
            version: 5,
        };
        let old_key = MetaBroadcastKey {
            node_id: node,
            version: 3,
        };
        assert!(new_key.invalidates(&old_key));
    }

    #[test]
    fn meta_broadcast_key_does_not_invalidate_different_node() {
        let a = MetaBroadcastKey {
            node_id: NodeId::generate(),
            version: 5,
        };
        let b = MetaBroadcastKey {
            node_id: NodeId::generate(),
            version: 3,
        };
        assert!(!a.invalidates(&b));
    }

    #[test]
    fn meta_broadcast_key_invalidates_same_version() {
        let node = NodeId::generate();
        let a = MetaBroadcastKey {
            node_id: node,
            version: 3,
        };
        let b = MetaBroadcastKey {
            node_id: node,
            version: 3,
        };
        assert!(a.invalidates(&b));
    }

    #[test]
    fn meta_handler_accepts_new_broadcast() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut handler = MetaHandler {
            seen_versions: HashMap::new(),
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

        let (received_id, received_meta) = rx.try_recv().unwrap();
        assert_eq!(received_id, node_id);
        assert_eq!(received_meta, meta);
    }

    #[test]
    fn meta_handler_rejects_stale_broadcast() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let mut handler = MetaHandler {
            seen_versions: HashMap::new(),
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

        let result = GossipService::start(gossip_config, identity, meta).await;
        assert!(result.is_ok(), "gossip service should start successfully");

        let (handle, _rx) = result.unwrap();
        assert_eq!(handle.alive_count(), 0, "no peers yet");
    }

    #[tokio::test]
    async fn two_nodes_discover_each_other() {
        // Node A
        let id_a = NodeId::generate();
        let socket_a = UdpSocket::bind(localhost(0)).await.unwrap();
        let addr_a = socket_a.local_addr().unwrap();
        drop(socket_a); // Release so GossipService can bind

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

        let (handle_a, mut rx_a) =
            GossipService::start(config_a, identity_a, test_meta(id_a, addr_a.port()))
                .await
                .unwrap();

        // Node B (seeds = [addr_a])
        let id_b = NodeId::generate();
        let socket_b = UdpSocket::bind(localhost(0)).await.unwrap();
        let addr_b = socket_b.local_addr().unwrap();
        drop(socket_b);

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

        let (handle_b, mut rx_b) =
            GossipService::start(config_b, identity_b, test_meta(id_b, addr_b.port()))
                .await
                .unwrap();

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

    #[test]
    fn update_member_meta_creates_entry_for_unknown_node() {
        let members = std::sync::Arc::new(std::sync::RwLock::new(HashMap::new()));
        let node_id = NodeId::generate();
        let meta = test_meta(node_id, 9200);

        update_member_meta(&members, node_id, meta.clone());

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
        update_member_meta(&members, node_id, updated_meta.clone());

        let m = members.read().unwrap();
        let state = m.get(&node_id).unwrap();
        // Status should remain unchanged (only meta updates).
        assert_eq!(state.status, NodeStatus::Suspect);
        assert_eq!(state.meta.capacity.used_bytes, 500_000);
    }

    #[test]
    fn gossip_handle_alive_count() {
        let members = std::sync::Arc::new(std::sync::RwLock::new(HashMap::new()));
        let handle = GossipHandle {
            members: members.clone(),
        };

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
        let handle = GossipHandle {
            members: members.clone(),
        };

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
}
