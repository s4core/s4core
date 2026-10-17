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

//! A three-node pool on real storage, for the coordinators' tests.
//!
//! Node 1 is the coordinating node and reads its own storage directly. Nodes
//! 2 and 3 are replicas behind real gRPC servers, each on a
//! `BitcaskStorageEngine` of its own and answering anti-entropy rounds, or
//! held dead by gossip and never asked.
//! One bucket, [`BUCKET`], lives in the pool beside `__system__`: RF=3, W=R=2.
//! A pool started on `base_port` listens on `base_port + 2` and
//! `base_port + 3` and nothing else. Tests run in parallel, and a server that
//! cannot bind its port fails in the background without a word: a pool on a
//! port another pool holds would send its requests to that pool's replicas.
//! So a pool claims its ports for as long as it lives ([`PortClaim`]), and a
//! second pool on them fails at once, naming the port.

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex};

use s4_core::storage::{BitcaskStorageEngine, EtagRank};
use s4_core::types::StorageClass;
use s4_core::StorageEngine;

use crate::config::{ClusterConfig, PoolConfig};
use crate::coordinator::{
    DistributedListCoordinator, ListTimeouts, QuorumReadCoordinator, QuorumWriteCoordinator,
    ReadTimeouts, WriteTimeouts,
};
use crate::gossip::{GossipHandle, NodeState};
use crate::hints::{HintedHandoffConfig, HintedHandoffManager};
use crate::hlc::{HlcTimestamp, HybridClock};
use crate::idempotency::IdempotencyTracker;
use crate::identity::{Edition, NodeAddr, NodeCapacity, NodeId, NodeMeta, NodeStatus, PoolId};
use crate::metadata_placement::{QuorumShape, Stage, StageCell};
use crate::placement::bucket_placement::BucketPlacement;
use crate::placement::{
    BucketMetadataMove, ClassLayout, PlacementRouter, PlacementStrategy, PoolLayout,
};
use crate::repair::anti_entropy::AntiEntropyResponder;
use crate::repair::reconcile::{LocalReplica, Reconciler};
use crate::repair::ReadRepairService;
use crate::replica::{ReplicaReadHandler, ReplicaWriteHandler};
use crate::rpc::server::{NodeRpcServer, RpcServerConfig, RpcServerHandle};
use crate::rpc::{proto, NodeClient, NodeClientConfig};

/// The ports the pools alive in this test process listen on.
static PORTS_IN_USE: Mutex<BTreeSet<u16>> = Mutex::new(BTreeSet::new());

/// The ports one pool listens on, held for as long as it lives.
struct PortClaim(Vec<u16>);

impl PortClaim {
    /// Claims the ports of a pool on `base_port`.
    ///
    /// # Panics
    ///
    /// When another pool alive in this process holds one of them: two tests
    /// were given overlapping ports, and the second would talk to the first's
    /// replicas.
    fn take(base_port: u16) -> Self {
        let ports = vec![base_port + 2, base_port + 3];
        let mut in_use = PORTS_IN_USE.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(taken) = ports.iter().find(|port| in_use.contains(port)) {
            panic!(
                "a test pool on base port {base_port} needs port {taken}, which another test pool \
                 of this run holds: give the tests ports that do not overlap"
            );
        }
        in_use.extend(&ports);
        Self(ports)
    }
}

impl Drop for PortClaim {
    fn drop(&mut self) {
        let mut in_use = PORTS_IN_USE.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        for port in &self.0 {
            in_use.remove(port);
        }
    }
}

/// The bucket every test pool holds.
pub(crate) const BUCKET: &str = "bucket";

/// Whether a replica of the pool runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Replica {
    /// Serves reads and writes over gRPC and is alive to gossip.
    Up,
    /// Has no server, and gossip holds it dead, so it is never asked.
    Down,
    /// Alive to gossip, and takes connections on its port without ever
    /// answering: a node too loaded to reply, which gossip has not caught.
    Silent,
}

/// A port that takes connections and holds them without a word, until the
/// pool is dropped.
struct SilentListener(tokio::task::JoinHandle<()>);

impl SilentListener {
    async fn bind(addr: std::net::SocketAddr) -> Self {
        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        Self(tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((socket, _)) = listener.accept().await {
                held.push(socket);
            }
        }))
    }
}

impl Drop for SilentListener {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// A move of the pool's bucket metadata from `from` to `to`, each node at
/// its own stage: node 1's router and the replica handlers of nodes 2 and 3
/// place and fence by `stages[n - 1]`.
#[derive(Clone, Copy, Debug)]
pub(crate) struct TestMove {
    pub(crate) from: QuorumShape,
    pub(crate) to: QuorumShape,
    pub(crate) stages: [Stage; 3],
}

/// Node 1 of a three-node pool, with nodes 2 and 3 as [`Replica`] says.
pub(crate) struct TestPool {
    config: ClusterConfig,
    /// Storage of nodes 1, 2 and 3, in that order. A replica that is down
    /// has one too, holding whatever a test writes to it.
    storages: [Arc<BitcaskStorageEngine>; 3],
    router: Arc<PlacementRouter>,
    pub(crate) rpc_client: Arc<NodeClient>,
    pub(crate) gossip: Arc<GossipHandle>,
    /// The stage of each node, when the pool moves its bucket metadata.
    stage_cells: Option<[StageCell; 3]>,
    _servers: Vec<RpcServerHandle>,
    _silent: Vec<SilentListener>,
    _dirs: Vec<tempfile::TempDir>,
    /// Released last, once the servers are gone.
    _ports: PortClaim,
}

/// An HLC issued at `wall` by a node that is none of the pool's: a version
/// that compares by its wall time alone.
pub(crate) fn at(wall: u64) -> HlcTimestamp {
    HlcTimestamp::from_record(wall, 0, 7)
}

/// The rank of the ETag a replica gives `data` written by a client: its MD5.
pub(crate) fn etag_of(data: &[u8]) -> EtagRank {
    EtagRank::of(&format!("{:x}", md5::compute(data)))
}

/// The metadata that stamps a write with `hlc`, as a replica reads it.
pub(crate) fn hlc_metadata(hlc: HlcTimestamp) -> HashMap<String, String> {
    HashMap::from([
        ("_s4_hlc_wall".to_string(), hlc.wall_time.to_string()),
        ("_s4_hlc_logical".to_string(), hlc.logical.to_string()),
        ("_s4_hlc_node_id".to_string(), hlc.node_id.to_string()),
    ])
}

/// Writes `data` under `key` straight into `storage`, stamped with `hlc`, as
/// a replica stores a replicated write. The user metadata `owner: test`
/// travels with it.
pub(crate) async fn put_with_hlc(
    storage: &BitcaskStorageEngine,
    key: &str,
    data: &[u8],
    hlc: HlcTimestamp,
) {
    let metadata = HashMap::from([
        ("_s4_hlc_wall".to_string(), hlc.wall_time.to_string()),
        ("_s4_hlc_logical".to_string(), hlc.logical.to_string()),
        ("_s4_hlc_node_id".to_string(), hlc.node_id.to_string()),
        ("owner".to_string(), "test".to_string()),
    ]);
    storage.put_object(BUCKET, key, data, "text/plain", &metadata).await.unwrap();
}

/// A node's storage as a cluster node opens it, with version digests and
/// the tombstone horizon settled: zero for an index the node created with
/// them, or `horizon` for one that held data from before tombstones.
async fn open_storage(
    horizon: Option<HlcTimestamp>,
) -> (Arc<BitcaskStorageEngine>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let open = |version_digests| {
        BitcaskStorageEngine::open(
            dir.path().join("volumes"),
            dir.path().join("metadata"),
            10 * 1024 * 1024,
            4096,
            false,
            s4_core::storage::IndexOptions { version_digests },
        )
    };
    if horizon.is_some() {
        drop(open(false).await.unwrap());
    }
    let storage = open(true).await.unwrap();
    let settled = storage
        .index_db()
        .settle_tombstone_horizon(horizon.unwrap_or_else(|| at(1)).into())
        .await
        .unwrap();
    assert_eq!(
        HlcTimestamp::from(settled),
        horizon.unwrap_or_else(HlcTimestamp::zero)
    );
    (Arc::new(storage), dir)
}

impl TestPool {
    /// Starts the pool with nodes 2 and 3 as given, each node's storage
    /// created with tombstones (horizon zero).
    pub(crate) async fn start(node_2: Replica, node_3: Replica, base_port: u16) -> Self {
        Self::start_with_horizons(node_2, node_3, base_port, [None; 3]).await
    }

    /// Starts the pool with nodes 2 and 3 as given, and node `n`'s storage
    /// holding data from before tombstones up to `horizons[n - 1]`, or none.
    pub(crate) async fn start_with_horizons(
        node_2: Replica,
        node_3: Replica,
        base_port: u16,
        horizons: [Option<HlcTimestamp>; 3],
    ) -> Self {
        Self::build(node_2, node_3, base_port, horizons, None).await
    }

    /// Starts the pool with nodes 2 and 3 as given, moving its bucket
    /// metadata as `moving` says.
    pub(crate) async fn start_moving(
        node_2: Replica,
        node_3: Replica,
        base_port: u16,
        moving: TestMove,
    ) -> Self {
        Self::build(node_2, node_3, base_port, [None; 3], Some(moving)).await
    }

    async fn build(
        node_2: Replica,
        node_3: Replica,
        base_port: u16,
        horizons: [Option<HlcTimestamp>; 3],
        moving: Option<TestMove>,
    ) -> Self {
        let ports = PortClaim::take(base_port);
        let stage_cells = moving.map(|moving| moving.stages.map(StageCell::new));
        let layout_of = |shape: QuorumShape| {
            ClassLayout::new(
                PlacementStrategy::new((1..=3).map(NodeId).collect(), shape.replication_factor)
                    .unwrap_or_else(|e| panic!("three nodes at {shape}: {e}")),
                shape.write_quorum,
                shape.read_quorum,
            )
            .unwrap_or_else(|e| panic!("{shape}: {e}"))
        };
        let addr = |n: u128| NodeAddr {
            grpc_addr: format!("127.0.0.1:{}", base_port + n as u16).parse().unwrap(),
            http_addr: format!("127.0.0.1:{}", 9000 + n).parse().unwrap(),
            gossip_addr: format!("127.0.0.1:{}", 9200 + n).parse().unwrap(),
        };
        let pool_id = PoolId::new(1);
        let config = ClusterConfig {
            pools: vec![PoolConfig {
                pool_id,
                name: "pool-1".to_string(),
                nodes: (1..=3).map(addr).collect(),
                replication_factor: Some(3),
                write_quorum: Some(2),
                read_quorum: Some(2),
                storage_class: StorageClass::Standard,
            }],
            default_replication_factor: 3,
            default_write_quorum: 2,
            default_read_quorum: 2,
            ..ClusterConfig::default()
        };
        let meta = |n: u128| NodeMeta {
            node_id: NodeId(n),
            node_name: format!("node-{n}"),
            addr: addr(n),
            pool_id,
            topology_epoch: config.epoch,
            protocol_version: crate::protocol::PROTOCOL_VERSION,
            capacity: NodeCapacity::default(),
            started_at: 0,
            s4_version: "0.1.0".into(),
            edition: Edition::Community,
        };

        let mut storages = Vec::with_capacity(3);
        let mut dirs = Vec::with_capacity(3);
        for horizon in horizons {
            let (storage, dir) = open_storage(horizon).await;
            storages.push(storage);
            dirs.push(dir);
        }
        let storages: [Arc<BitcaskStorageEngine>; 3] =
            storages.try_into().unwrap_or_else(|_| unreachable!("three storages opened"));

        let placement = || {
            PlacementStrategy::new((1..=3).map(NodeId).collect(), 3)
                .unwrap_or_else(|e| panic!("three nodes at RF=3: {e}"))
        };
        let mut servers = Vec::new();
        let mut silent = Vec::new();
        let mut members = HashMap::new();
        for (n, replica) in [(2u128, node_2), (3, node_3)] {
            members.insert(
                NodeId(n),
                NodeState {
                    meta: meta(n),
                    status: match replica {
                        Replica::Up | Replica::Silent => NodeStatus::Alive,
                        Replica::Down => NodeStatus::Dead,
                    },
                    last_seen: std::time::Instant::now(),
                },
            );
            match replica {
                Replica::Down => continue,
                Replica::Silent => {
                    silent.push(SilentListener::bind(addr(n).grpc_addr).await);
                    continue;
                }
                Replica::Up => {}
            }
            let storage = storages[n as usize - 1].clone();
            let mut write_handler = ReplicaWriteHandler::new(
                storage.clone(),
                Arc::new(IdempotencyTracker::new()),
                config.epoch,
            );
            if let Some(cells) = &stage_cells {
                write_handler =
                    write_handler.with_bucket_metadata_stage(cells[n as usize - 1].clone());
            }
            let server = NodeRpcServer::new(
                NodeId(n),
                Arc::new(meta(n)),
                Arc::new(GossipHandle::new_for_testing(HashMap::new())),
            )
            .with_replica_handler(Arc::new(write_handler))
            .with_read_handler(Arc::new(ReplicaReadHandler::new(
                storage.clone(),
                config.epoch,
            )))
            .with_anti_entropy(Arc::new(AntiEntropyResponder::new(
                storage,
                placement(),
                NodeId(n),
                pool_id,
                config.epoch,
            )))
            .start(RpcServerConfig {
                bind_addr: addr(n).grpc_addr,
                ..Default::default()
            })
            .await
            .unwrap();
            servers.push(server);
        }

        let gossip = Arc::new(GossipHandle::new_for_testing(members));
        let rpc_client = Arc::new(NodeClient::new(gossip.clone(), NodeClientConfig::default()));
        let resolver: HashMap<_, _> = (1..=3).map(|n| (addr(n).grpc_addr, NodeId(n))).collect();
        let placement = BucketPlacement::from_entries([
            (BUCKET.to_string(), pool_id),
            ("__system__".to_string(), pool_id),
        ]);
        let mut router = PlacementRouter::new(&config, NodeId(1), placement, &resolver).unwrap();
        if let (Some(moving), Some(cells)) = (moving, &stage_cells) {
            router = router
                .with_bucket_metadata_move(
                    pool_id,
                    BucketMetadataMove::new(
                        layout_of(moving.from),
                        layout_of(moving.to),
                        cells[0].clone(),
                    ),
                )
                .unwrap();
        }
        let router = Arc::new(router);
        Self {
            config,
            storages,
            router,
            rpc_client,
            gossip,
            stage_cells,
            _servers: servers,
            _silent: silent,
            _dirs: dirs,
            _ports: ports,
        }
    }

    /// Has gossip report that node `node` speaks protocol `version`, as a
    /// node of an older release announces itself.
    pub(crate) fn announce_protocol(&self, node: u128, version: u32) {
        let mut state = self.gossip.get_node(&NodeId(node)).unwrap();
        state.meta.protocol_version = version;
        self.gossip.set_member_for_testing(NodeId(node), state);
    }

    /// Router shared by the pool's real coordinators and migration worker.
    pub(crate) fn router(&self) -> Arc<PlacementRouter> {
        self.router.clone()
    }

    /// The pool's placement: every node replicates every key.
    pub(crate) fn placement(&self) -> PlacementStrategy {
        PlacementStrategy::new((1..=3).map(NodeId).collect(), 3)
            .unwrap_or_else(|e| panic!("three nodes at RF=3: {e}"))
    }

    /// The pool's placement of every replication class, as its router has it.
    pub(crate) fn layout(&self) -> PoolLayout {
        self.router
            .layout(&self.pool_id())
            .cloned()
            .unwrap_or_else(|| panic!("the router places the test pool"))
    }

    /// The stage of node `node`, 1 to 3, in a pool started moving.
    pub(crate) fn stage_cell(&self, node: u128) -> &StageCell {
        &self
            .stage_cells
            .as_ref()
            .unwrap_or_else(|| panic!("the pool was started moving"))[node as usize - 1]
    }

    /// The pool's identifier.
    pub(crate) fn pool_id(&self) -> PoolId {
        self.config.pools[0].pool_id
    }

    /// The pool's topology epoch.
    pub(crate) fn epoch(&self) -> u64 {
        self.config.epoch
    }

    /// The storage of node `node`, 1 to 3.
    pub(crate) fn storage(&self, node: u128) -> &Arc<BitcaskStorageEngine> {
        &self.storages[node as usize - 1]
    }

    /// A read coordinator on node 1.
    pub(crate) fn read_coordinator(&self) -> QuorumReadCoordinator {
        self.read_coordinator_with(ReadTimeouts::default())
    }

    /// A read coordinator on node 1 that waits for replicas as `timeouts` say.
    pub(crate) fn read_coordinator_with(&self, timeouts: ReadTimeouts) -> QuorumReadCoordinator {
        QuorumReadCoordinator::new(
            NodeId(1),
            self.storage(1).clone(),
            self.rpc_client.clone(),
            self.router.clone(),
            Arc::new(HybridClock::new(NodeId(1))),
            Arc::new(ReadRepairService::start_with_capacity(
                self.rpc_client.clone(),
                16,
            )),
            &self.config,
            timeouts,
        )
    }

    /// Settles keys from node 1, reaching node 1 itself without the network.
    pub(crate) fn reconciler(&self) -> Reconciler {
        self.reconciler_reading(self.read_coordinator())
    }

    /// Settles keys from node 1, reading their versions through `source`.
    pub(crate) fn reconciler_reading(&self, source: QuorumReadCoordinator) -> Reconciler {
        self.reconciler_reading_source(Arc::new(source))
    }

    /// Settles keys from node 1, reading their versions through any `source`.
    pub(crate) fn reconciler_reading_source(
        &self,
        source: Arc<dyn crate::repair::reconcile::KeyStateSource>,
    ) -> Reconciler {
        let storage = self.storage(1).clone();
        Reconciler::new(
            source,
            self.rpc_client.clone(),
            self.gossip.clone(),
            NodeId(1),
            self.config.epoch,
        )
        .with_local_replica(LocalReplica {
            write: Arc::new(ReplicaWriteHandler::new(
                storage.clone(),
                Arc::new(IdempotencyTracker::new()),
                self.config.epoch,
            )),
            read: Arc::new(ReplicaReadHandler::new(storage, self.config.epoch)),
        })
    }

    /// A write coordinator on node 1, storing its hints under `hints_dir`.
    pub(crate) fn write_coordinator(&self, hints_dir: &std::path::Path) -> QuorumWriteCoordinator {
        QuorumWriteCoordinator::new(
            NodeId(1),
            self.storage(1).clone(),
            self.rpc_client.clone(),
            self.router.clone(),
            Arc::new(HybridClock::new(NodeId(1))),
            Arc::new(HintedHandoffManager::new(
                HintedHandoffConfig::new(hints_dir),
                self.rpc_client.clone(),
            )),
            &self.config,
            WriteTimeouts::default(),
        )
    }

    /// A list coordinator on node 1.
    pub(crate) fn list_coordinator(&self) -> DistributedListCoordinator {
        DistributedListCoordinator::new(
            NodeId(1),
            self.storage(1).clone(),
            self.rpc_client.clone(),
            self.router.clone(),
            self.config.epoch,
            ListTimeouts::default(),
        )
        .with_read_coordinator(Arc::new(self.read_coordinator()))
    }

    /// Node `node` takes a replicated write of `key`, as [`put_with_hlc`]
    /// stores it.
    pub(crate) async fn put(&self, node: u128, key: &str, data: &[u8], hlc: HlcTimestamp) {
        put_with_hlc(self.storage(node), key, data, hlc).await;
    }

    /// Node `node` takes a copy of `key` that a repair wrote at `hlc` with
    /// the ETag `etag` of its version. With `hlc` made up (node 0) it is the
    /// copy of a record written without an HLC.
    pub(crate) async fn put_with_etag(
        &self,
        node: u128,
        key: &str,
        data: &[u8],
        hlc: HlcTimestamp,
        etag: &str,
    ) {
        let handler = ReplicaWriteHandler::new(
            self.storage(node).clone(),
            Arc::new(IdempotencyTracker::new()),
            self.config.epoch,
        );
        let response = handler
            .handle_write(&proto::WriteBlobRequest {
                verify_content: false,
                placement_generation: 0,
                bucket: BUCKET.to_string(),
                key: key.to_string(),
                data: data.to_vec(),
                content_hash: Vec::new(),
                content_type: "text/plain".to_string(),
                metadata: HashMap::new(),
                hlc: Some(hlc.to_proto()),
                version_id: None,
                coordinator_id: NodeId(1).to_bytes().to_vec(),
                operation_id: uuid::Uuid::new_v4().as_u128().to_be_bytes().to_vec(),
                topology_epoch: self.config.epoch,
                etag: etag.to_string(),
                bucket_metadata_stage: 0,
                repair_copy: false,
                iam_lease: None,
            })
            .await
            .unwrap();
        assert!(
            response.success,
            "node {node} refused the copy: {}",
            response.error
        );
    }

    /// Node `node` holds `key` as a multipart upload of two parts completes
    /// it, at `hlc`, with the multipart ETag `etag`: a record that lists its
    /// parts and holds no content hash of the whole. Returns the object's
    /// bytes.
    pub(crate) async fn complete_multipart(
        &self,
        node: u128,
        key: &str,
        etag: &str,
        hlc: HlcTimestamp,
    ) -> Vec<u8> {
        let storage = self.storage(node);
        let metadata = HashMap::from([
            ("_s4_hlc_wall".to_string(), hlc.wall_time.to_string()),
            ("_s4_hlc_logical".to_string(), hlc.logical.to_string()),
            ("_s4_hlc_node_id".to_string(), hlc.node_id.to_string()),
        ]);
        let upload_id = format!("upload-{key}");
        storage
            .create_multipart_session(&upload_id, BUCKET, key, "text/plain", &metadata)
            .await
            .unwrap();
        let mut parts = Vec::new();
        let mut whole = Vec::new();
        for number in 1..=2u32 {
            let part = vec![b'a' + number as u8; 700];
            whole.extend_from_slice(&part);
            let uploaded = storage
                .upload_part_streaming(
                    &upload_id,
                    number,
                    Box::new(std::io::Cursor::new(part)),
                    700,
                )
                .await
                .unwrap();
            parts.push(uploaded.record);
        }
        storage.mark_session_completing(&upload_id).await.unwrap();
        storage
            .complete_multipart_native(
                BUCKET,
                key,
                &upload_id,
                &parts,
                etag,
                "text/plain",
                &metadata,
            )
            .await
            .unwrap();
        whole
    }

    /// Node `node` takes the marker of [`BUCKET`] written at `hlc`, as a
    /// replica takes a replicated CreateBucket.
    pub(crate) async fn create_bucket(&self, node: u128, hlc: HlcTimestamp) {
        let marker = format!("{}{BUCKET}", s4_core::types::BUCKET_MARKER_KEY_PREFIX);
        self.put_system(node, &marker, b"1", hlc).await;
    }

    /// Node `node` takes a replicated write of `key` in `__system__`, stamped
    /// with `hlc`.
    pub(crate) async fn put_system(&self, node: u128, key: &str, data: &[u8], hlc: HlcTimestamp) {
        self.storage(node)
            .put_object(
                "__system__",
                key,
                data,
                "application/octet-stream",
                &hlc_metadata(hlc),
            )
            .await
            .unwrap();
    }

    /// Node `node` takes the delete of `key` issued at `hlc`, as a replica
    /// takes the `DeleteBlob` a client's delete sends it: through the replica
    /// write handler, whether the node runs or not.
    pub(crate) async fn delete(&self, node: u128, key: &str, hlc: HlcTimestamp) {
        self.delete_in(node, BUCKET, key, hlc).await;
    }

    /// Node `node` takes the delete of `key` in `__system__` issued at `hlc`,
    /// as [`Self::delete`] takes one in the pool's bucket.
    pub(crate) async fn delete_system(&self, node: u128, key: &str, hlc: HlcTimestamp) {
        self.delete_in(node, "__system__", key, hlc).await;
    }

    async fn delete_in(&self, node: u128, bucket: &str, key: &str, hlc: HlcTimestamp) {
        let handler = ReplicaWriteHandler::new(
            self.storage(node).clone(),
            Arc::new(IdempotencyTracker::new()),
            self.config.epoch,
        );
        let response = handler
            .handle_delete(&proto::DeleteBlobRequest {
                placement_generation: 0,
                bucket: bucket.to_string(),
                key: key.to_string(),
                hlc: Some(hlc.to_proto()),
                coordinator_id: NodeId(1).to_bytes().to_vec(),
                operation_id: uuid::Uuid::new_v4().as_u128().to_be_bytes().to_vec(),
                topology_epoch: self.config.epoch,
                version_id: None,
                expected_source_hlc: None,
                bucket_metadata_stage: 0,
                repair_copy: false,
                iam_lease: None,
            })
            .await
            .unwrap();
        assert!(
            response.success,
            "node {node} refused the delete: {}",
            response.error
        );
    }
}

/// A pool's ports are its own while it lives: a pool whose ports overlap
/// them fails at once, and they are free again once it is gone. Ports far
/// from any pool's, so no test running beside this one holds them.
#[test]
fn a_port_two_pools_would_share_is_refused_until_the_first_is_gone() {
    let first = PortClaim::take(29900);
    let overlapping = std::panic::catch_unwind(|| PortClaim::take(29901));
    let message = overlapping
        .err()
        .and_then(|panic| panic.downcast_ref::<String>().cloned())
        .expect("a pool on a port another holds is refused");
    assert!(message.contains("port 29903"), "{message}");
    let beside = PortClaim::take(29904);
    drop(first);
    let again = PortClaim::take(29901);
    drop((beside, again));
}

#[tokio::test]
async fn stable_head_and_streaming_get_do_not_schedule_read_repair() {
    let pool = TestPool::start(Replica::Up, Replica::Down, 32660).await;
    let repair = Arc::new(ReadRepairService::start_with_local(
        pool.rpc_client.clone(),
        NodeId(1),
        pool.storage(1).clone(),
        pool.epoch(),
    ));
    let read = QuorumReadCoordinator::new(
        NodeId(1),
        pool.storage(1).clone(),
        pool.rpc_client.clone(),
        pool.router.clone(),
        Arc::new(HybridClock::new(NodeId(1))),
        repair.clone(),
        &pool.config,
        ReadTimeouts::default(),
    );
    for key in ["head", "stream", "deleted-head", "deleted-stream"] {
        pool.put(1, key, b"old", at(100)).await;
        pool.put(2, key, b"new", at(200)).await;
    }
    pool.delete(2, "deleted-head", at(300)).await;
    pool.delete(2, "deleted-stream", at(300)).await;

    assert_eq!(read.head(BUCKET, "head").await.unwrap().hlc, at(200));
    drop(read.read_stream(BUCKET, "stream", None).await.unwrap());
    assert!(matches!(
        read.head(BUCKET, "deleted-head").await,
        Err(crate::ClusterError::ObjectNotFound { .. })
    ));
    assert!(matches!(
        read.read_stream(BUCKET, "deleted-stream", None).await,
        Err(crate::ClusterError::ObjectNotFound { .. })
    ));
    assert_eq!(repair.repairs_initiated(), 0);
    assert_eq!(
        pool.storage(1).get_object(BUCKET, "head").await.unwrap().0,
        b"old"
    );
    assert!(pool.storage(1).get_object(BUCKET, "deleted-head").await.is_ok());
}

/// A key overwritten between the digests of a read and its fetch: every
/// source took a newer version, and the read serves it instead of answering
/// 503 — a GET as a full read and as a stream.
#[tokio::test]
async fn a_read_whose_source_took_a_newer_version_serves_it() {
    let pool = TestPool::start(Replica::Up, Replica::Up, 32670).await;
    for node in 1..=3 {
        pool.put(node, "hot", b"first", at(100)).await;
        pool.put(node, "hot-stream", b"first", at(100)).await;
    }
    let read = pool.read_coordinator();
    let overwrite = |key: &'static str| {
        let storages: Vec<_> = (1..=3).map(|node| pool.storage(node).clone()).collect();
        Box::pin(async move {
            for storage in storages {
                put_with_hlc(&storage, key, b"second", at(200)).await;
            }
        })
    };

    read.before_next_fetch(overwrite("hot"));
    let result = read.read(BUCKET, "hot").await.unwrap();
    assert_eq!(result.data, b"second");
    assert_eq!(result.hlc, at(200));

    read.before_next_fetch(overwrite("hot-stream"));
    let mut stream = read.read_stream(BUCKET, "hot-stream", None).await.unwrap();
    let mut body = Vec::new();
    tokio::io::AsyncReadExt::read_to_end(&mut stream.body, &mut body).await.unwrap();
    assert_eq!(body, b"second");
    assert_eq!(stream.hlc, at(200));
}

/// An object every replica holds as an erasure-coded entry has no bytes a
/// replicated read could serve: both read paths say so with their own error,
/// which a caller that took it for a hot object a moment before tells apart
/// and reads through the EC runtime.
#[tokio::test]
async fn a_read_of_an_object_moved_into_its_set_says_so() {
    let pool = TestPool::start(Replica::Up, Replica::Up, 32680).await;
    for node in 1..=3 {
        pool.put(node, "moved", b"hot bytes", at(100)).await;
        assert!(pool
            .storage(node)
            .detach_object_data_if_hlc(
                BUCKET,
                "moved",
                100,
                0,
                7,
                s4_core::types::ObjectLayout::ErasureCoded {
                    erasure_set_id: 11,
                    topology_epoch: 1,
                },
            )
            .await
            .unwrap());
    }
    let read = pool.read_coordinator();
    assert!(
        matches!(
            read.read(BUCKET, "moved").await,
            Err(crate::ClusterError::ErasureCodedOnly { .. })
        ),
        "full read"
    );
    assert!(
        matches!(
            read.read_stream(BUCKET, "moved", None).await,
            Err(crate::ClusterError::ErasureCodedOnly { .. })
        ),
        "streamed read"
    );
}

/// Additional mutation paths that must preserve both generation quorums.
#[cfg(test)]
mod pool_growth_mutation_paths {
    use super::*;
    use crate::pool_generation::PoolGenerationRecord;
    use s4_core::types::{IndexRecord, ObjectLayout};
    use std::time::Duration;

    struct GrowingPool {
        storages: Vec<Arc<BitcaskStorageEngine>>,
        router: Arc<PlacementRouter>,
        coordinator: QuorumWriteCoordinator,
        down: Vec<u128>,
        _servers: Vec<RpcServerHandle>,
        _dirs: Vec<tempfile::TempDir>,
        _ports: PortClaim,
    }

    impl GrowingPool {
        async fn start(base_port: u16, down: &[u128]) -> Self {
            Self::start_for_local(base_port, down, NodeId(1)).await
        }

        async fn start_for_local(base_port: u16, down: &[u128], local: NodeId) -> Self {
            let ports: Vec<_> = (2..=6).map(|node| base_port + node).collect();
            {
                let mut used = PORTS_IN_USE.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                assert!(ports.iter().all(|port| !used.contains(port)));
                used.extend(&ports);
            }
            let ports = PortClaim(ports);
            let addr = |node| NodeAddr {
                grpc_addr: format!("127.0.0.1:{}", base_port + node as u16).parse().unwrap(),
                http_addr: format!("127.0.0.1:{}", 9000 + node).parse().unwrap(),
                gossip_addr: format!("127.0.0.1:{}", 9200 + node).parse().unwrap(),
            };
            let pool_id = PoolId(1);
            let config = ClusterConfig {
                pools: vec![PoolConfig {
                    pool_id,
                    name: "joint-mutations".into(),
                    nodes: (1..=3).map(addr).collect(),
                    replication_factor: Some(3),
                    write_quorum: Some(2),
                    read_quorum: Some(2),
                    storage_class: StorageClass::Standard,
                }],
                ..ClusterConfig::default()
            };
            let meta = |node| NodeMeta {
                node_id: NodeId(node),
                node_name: format!("growth-{node}"),
                addr: addr(node),
                pool_id,
                topology_epoch: config.epoch,
                protocol_version: crate::PROTOCOL_VERSION,
                capacity: NodeCapacity::default(),
                started_at: 0,
                s4_version: "test".into(),
                edition: Edition::Community,
            };
            let mut storages = Vec::new();
            let mut dirs = Vec::new();
            let mut servers = Vec::new();
            let mut members = HashMap::new();
            for node in 1..=6 {
                let (storage, dir) = open_storage(None).await;
                if node > 1 {
                    members.insert(
                        NodeId(node),
                        NodeState {
                            meta: meta(node),
                            status: if down.contains(&node) {
                                NodeStatus::Dead
                            } else {
                                NodeStatus::Alive
                            },
                            last_seen: std::time::Instant::now(),
                        },
                    );
                    if !down.contains(&node) {
                        servers.push(
                            NodeRpcServer::new(
                                NodeId(node),
                                Arc::new(meta(node)),
                                Arc::new(GossipHandle::new_for_testing(HashMap::new())),
                            )
                            .with_replica_handler(Arc::new(
                                ReplicaWriteHandler::new(
                                    storage.clone(),
                                    Arc::new(IdempotencyTracker::new()),
                                    config.epoch,
                                )
                                .with_placement_generation(2),
                            ))
                            .start(RpcServerConfig {
                                bind_addr: addr(node).grpc_addr,
                                ..Default::default()
                            })
                            .await
                            .unwrap(),
                        );
                    }
                }
                storages.push(storage);
                dirs.push(dir);
            }
            let gossip = Arc::new(GossipHandle::new_for_testing(members));
            let client = Arc::new(NodeClient::new(
                gossip,
                NodeClientConfig {
                    max_retries: 0,
                    connect_timeout: Duration::from_secs(2),
                    ..Default::default()
                },
            ));
            let placement = BucketPlacement::from_entries([(BUCKET.to_string(), pool_id)]);
            let record = PoolGenerationRecord::initial(pool_id, (1..=3).map(NodeId).collect())
                .unwrap()
                .configure((1..=6).map(NodeId).collect())
                .unwrap()
                .begin()
                .unwrap();
            let router = Arc::new(
                PlacementRouter::from_pool_node_ids(
                    &config,
                    local,
                    placement,
                    &HashMap::from([(pool_id, (1..=3).map(NodeId).collect())]),
                )
                .unwrap()
                .with_pool_generation(&record)
                .unwrap(),
            );
            let hint_dir = tempfile::tempdir().unwrap();
            let hints = Arc::new(HintedHandoffManager::new(
                HintedHandoffConfig::new(hint_dir.path()),
                client.clone(),
            ));
            dirs.push(hint_dir);
            let coordinator = QuorumWriteCoordinator::new(
                local,
                storages[0].clone(),
                client,
                router.clone(),
                Arc::new(HybridClock::new(local)),
                hints,
                &config,
                WriteTimeouts {
                    quorum_timeout: Duration::from_secs(10),
                    replica_write_timeout: Duration::from_secs(5),
                },
            )
            .with_placement_generation(2);
            Self {
                storages,
                router,
                coordinator,
                down: down.to_vec(),
                _servers: servers,
                _dirs: dirs,
                _ports: ports,
            }
        }

        fn key(&self, operation: &str) -> String {
            let key = (0..10_000)
                .map(|n| format!("{operation}-{n}"))
                .find(|key| {
                    self.router.route(BUCKET, key).unwrap().write_rule.requirements()[1]
                        .nodes
                        .iter()
                        .all(|node| node.0 > 3)
                })
                .unwrap();
            let decision = self.router.route(BUCKET, &key).unwrap();
            assert!(decision.write_rule.requirements()[0].nodes.contains(&NodeId(1)));
            assert!(!decision.write_rule.requirements()[1].nodes.contains(&NodeId(1)));
            key
        }

        async fn seed(&self, key: &str, detached: bool) {
            for storage in &self.storages {
                put_with_hlc(storage, key, b"hot bytes", at(100)).await;
                if detached {
                    assert!(storage
                        .detach_object_data_if_hlc(
                            BUCKET,
                            key,
                            100,
                            0,
                            7,
                            ObjectLayout::ErasureCoded {
                                erasure_set_id: 11,
                                topology_epoch: 1
                            }
                        )
                        .await
                        .unwrap());
                }
            }
        }

        async fn assert_stored_jointly(&self, key: &str, detached: bool) {
            let decision = self.router.route(BUCKET, key).unwrap();
            for _ in 0..200 {
                let mut held = Vec::new();
                for (index, storage) in self.storages.iter().enumerate() {
                    let node = index as u128 + 1;
                    if self.down.contains(&node) {
                        continue;
                    }
                    let matches = if detached {
                        storage
                            .head_object(BUCKET, key)
                            .await
                            .is_ok_and(|record| record.is_erasure_coded())
                    } else {
                        storage
                            .get_object(BUCKET, key)
                            .await
                            .is_ok_and(|(bytes, _)| bytes == b"hot bytes")
                    };
                    if matches {
                        held.push(NodeId(node));
                    }
                }
                if decision.write_rule.met(&held) {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            panic!("{key}: mutation is missing from one generation's quorum");
        }
    }

    fn restored_record() -> IndexRecord {
        let mut record = IndexRecord::new(
            u32::MAX,
            0,
            9,
            [7; 32],
            "restored".into(),
            "text/plain".into(),
        );
        record.hlc_timestamp = 100;
        record.origin_node_id = 7;
        record.into_erasure_coded(11, 1)
    }

    #[tokio::test]
    async fn pool_growth_detach_attach_restore_and_remote_replication_store_both_quorums() {
        // The local ACK is needed by the old group; only remote nodes 4 and 5
        // can satisfy the disjoint new group. Neither third replica is alive.
        let pool = GrowingPool::start(32600, &[3, 6]).await;
        let detach = pool.key("detach");
        pool.seed(&detach, false).await;
        pool.coordinator
            .detach_object_data_if_hlc(BUCKET, &detach, at(100), 11, 1)
            .await
            .unwrap();
        pool.assert_stored_jointly(&detach, true).await;

        let attach = pool.key("attach");
        pool.seed(&attach, true).await;
        pool.coordinator
            .attach_object_data_if_hlc(BUCKET, &attach, b"hot bytes", at(100))
            .await
            .unwrap();
        pool.assert_stored_jointly(&attach, false).await;

        let restore = pool.key("restore");
        assert!(
            pool.coordinator
                .restore_namespace_entry(BUCKET, &restore, &restored_record())
                .await
                .unwrap()
                >= 4
        );
        pool.assert_stored_jointly(&restore, true).await;

        let remote = pool.key("remote");
        put_with_hlc(&pool.storages[0], &remote, b"hot bytes", at(100)).await;
        let result = pool
            .coordinator
            .replicate_to_remotes(BUCKET, &remote, b"hot bytes", "text/plain", &HashMap::new())
            .await
            .unwrap();
        assert!(result.replicas_acked >= 4);
        pool.assert_stored_jointly(&remote, false).await;
    }

    #[tokio::test]
    async fn pool_growth_mutation_paths_cannot_spend_the_local_ack_in_the_new_group() {
        // All old owners and exactly one new owner are available. Counting
        // node 1 toward both requirements would incorrectly confirm every path.
        let pool = GrowingPool::start(32620, &[5, 6]).await;
        let detach = pool.key("detach-denied");
        pool.seed(&detach, false).await;
        let detached = pool
            .coordinator
            .detach_object_data_if_hlc(BUCKET, &detach, at(100), 11, 1)
            .await;
        assert!(
            matches!(detached, Err(crate::ClusterError::QuorumNotMet { .. })),
            "{detached:?}"
        );

        let attach = pool.key("attach-denied");
        pool.seed(&attach, true).await;
        let attached = pool
            .coordinator
            .attach_object_data_if_hlc(BUCKET, &attach, b"hot bytes", at(100))
            .await;
        assert!(
            matches!(attached, Err(crate::ClusterError::QuorumNotMet { .. })),
            "{attached:?}"
        );

        let restore = pool.key("restore-denied");
        let restored = pool
            .coordinator
            .restore_namespace_entry(BUCKET, &restore, &restored_record())
            .await;
        assert!(
            matches!(restored, Err(crate::ClusterError::QuorumNotMet { .. })),
            "{restored:?}"
        );

        let remote = pool.key("remote-denied");
        put_with_hlc(&pool.storages[0], &remote, b"hot bytes", at(100)).await;
        let replicated = pool
            .coordinator
            .replicate_to_remotes(BUCKET, &remote, b"hot bytes", "text/plain", &HashMap::new())
            .await;
        assert!(
            matches!(replicated, Err(crate::ClusterError::QuorumNotMet { .. })),
            "{replicated:?}"
        );
    }

    #[tokio::test]
    async fn pool_growth_remote_replication_does_not_count_a_learner_local_copy() {
        // Learner 7 holds bytes but belongs to neither requirement. Owner 1
        // has no server or gossip entry; owner 3 is down. Only old owner 2
        // and new owners 4/5 can acknowledge, leaving the old W=2 unmet.
        let pool = GrowingPool::start_for_local(32640, &[3, 6], NodeId(7)).await;
        let key = pool.key("learner-remote-denied");
        let decision = pool.router.route(BUCKET, &key).unwrap();
        assert!(decision
            .write_rule
            .requirements()
            .iter()
            .all(|requirement| !requirement.nodes.contains(&NodeId(7))));
        put_with_hlc(&pool.storages[0], &key, b"hot bytes", at(100)).await;
        let result = pool
            .coordinator
            .replicate_to_remotes(BUCKET, &key, b"hot bytes", "text/plain", &HashMap::new())
            .await;
        assert!(
            matches!(result, Err(crate::ClusterError::QuorumNotMet { .. })),
            "{result:?}"
        );
    }
}
