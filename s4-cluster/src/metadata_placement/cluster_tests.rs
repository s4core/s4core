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

//! The move on a pool of six nodes in one process, over real storage and
//! gRPC (plan BUCKET-METADATA-REPLICATION.md, tasks BMR-04-001 and
//! BMR-04-004).
//!
//! The pool places its data on three replicas of a ring, W = R = 2, and
//! moves its bucket metadata to five replicas of the same ring, W = R = 3 —
//! the move of an erasure-coded pool of six nodes. Every node has its own
//! storage, replica handlers, router, coordinators and stage; the nodes
//! announce their records on one board that stands for gossip.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use s4_core::storage::BitcaskStorageEngine;
use s4_core::types::{StorageClass, BUCKET_MARKER_KEY_PREFIX};
use s4_core::{KeyVersion, StorageEngine};

use crate::config::{ClusterConfig, PoolConfig};
use crate::coordinator::test_pool::hlc_metadata;
use crate::coordinator::{
    QuorumReadCoordinator, QuorumWriteCoordinator, ReadTimeouts, WriteTimeouts,
};
use crate::error::ClusterError;
use crate::gossip::{GossipHandle, NodeState};
use crate::hints::{HintedHandoffConfig, HintedHandoffManager};
use crate::hlc::{HlcTimestamp, HybridClock};
use crate::idempotency::IdempotencyTracker;
use crate::identity::{Edition, NodeAddr, NodeCapacity, NodeId, NodeMeta, NodeStatus, PoolId};
use crate::metadata_placement::{
    Backfill, BackfillPass, BackfillReport, PeerPlacement, PlacementBoard, PlacementRecord,
    PlacementStatus, PlacementView, QuorumShape, Stage, StageCell, StageDriver,
};
use crate::placement::bucket_placement::BucketPlacement;
use crate::placement::{BucketMetadataMove, ClassLayout, PlacementRouter, PlacementStrategy};
use crate::protocol::{BUCKET_METADATA_MIN_PROTOCOL_VERSION, PROTOCOL_VERSION};
use crate::repair::reconcile::{LocalReplica, Reconciler};
use crate::repair::ReadRepairService;
use crate::replica::{ReplicaReadHandler, ReplicaWriteHandler};
use crate::rpc::server::{NodeRpcServer, RpcServerConfig, RpcServerHandle};
use crate::rpc::{NodeClient, NodeClientConfig};

const NODES: u128 = 6;
const SYSTEM: &str = "__system__";
const S3: QuorumShape = QuorumShape {
    replication_factor: 3,
    write_quorum: 2,
    read_quorum: 2,
};
const S5: QuorumShape = QuorumShape {
    replication_factor: 5,
    write_quorum: 3,
    read_quorum: 3,
};

/// An HLC no node of the pool issued, at `wall`.
fn at(wall: u64) -> HlcTimestamp {
    HlcTimestamp::from_record(wall, 0, 7)
}

fn marker(bucket: &str) -> String {
    format!("{BUCKET_MARKER_KEY_PREFIX}{bucket}")
}

/// What the nodes announced, standing for gossip; a node gossip holds down
/// reads as down.
struct Board {
    gossip: Arc<GossipHandle>,
    records: Mutex<HashMap<NodeId, PlacementRecord>>,
}

/// The board as one node uses it.
struct NodeBoard {
    board: Arc<Board>,
    node: NodeId,
}

impl PlacementView for NodeBoard {
    fn peer_placement(&self, node: NodeId, _min_protocol: u32) -> PeerPlacement {
        match self.board.gossip.get_node(&node) {
            None => PeerPlacement::Unknown,
            Some(state) if state.status != NodeStatus::Alive => PeerPlacement::Down,
            Some(_) => match self.board.record(node) {
                Some(record) => PeerPlacement::Announced(record),
                None => PeerPlacement::Silent,
            },
        }
    }
}

impl PlacementBoard for NodeBoard {
    fn announce(&self, record: PlacementRecord) {
        self.board
            .records
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(self.node, record);
    }
}

impl Board {
    fn record(&self, node: NodeId) -> Option<PlacementRecord> {
        self.records
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&node)
            .copied()
    }
}

/// One node of the pool.
struct Node {
    storage: Arc<BitcaskStorageEngine>,
    stage: StageCell,
    write_handler: Arc<ReplicaWriteHandler>,
    read_handler: Arc<ReplicaReadHandler>,
    router: Arc<PlacementRouter>,
    rpc_client: Arc<NodeClient>,
    server: Option<RpcServerHandle>,
    _dir: tempfile::TempDir,
}

/// Six nodes moving their bucket metadata from three replicas to five.
struct MovingPool {
    config: ClusterConfig,
    gossip: Arc<GossipHandle>,
    board: Arc<Board>,
    old: ClassLayout,
    new: ClassLayout,
    nodes: Vec<Node>,
    base_port: u16,
    _hints: tempfile::TempDir,
}

fn ids() -> Vec<NodeId> {
    (1..=NODES).map(NodeId).collect()
}

fn layout(shape: QuorumShape) -> ClassLayout {
    ClassLayout::new(
        PlacementStrategy::new(ids(), shape.replication_factor).unwrap(),
        shape.write_quorum,
        shape.read_quorum,
    )
    .unwrap()
}

impl MovingPool {
    /// The pool with every node at `stage`, each node's storage holding data
    /// from before tombstones up to `horizon`: an absence there proves no
    /// delete older than it.
    async fn start(base_port: u16, stage: Stage, horizon: HlcTimestamp) -> Self {
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
                nodes: (1..=NODES).map(addr).collect(),
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
            protocol_version: PROTOCOL_VERSION,
            capacity: NodeCapacity::default(),
            started_at: 0,
            s4_version: "0.1.0".into(),
            edition: Edition::Enterprise,
        };
        let members = (1..=NODES)
            .map(|n| {
                (
                    NodeId(n),
                    NodeState {
                        meta: meta(n),
                        status: NodeStatus::Alive,
                        last_seen: std::time::Instant::now(),
                    },
                )
            })
            .collect();
        let gossip = Arc::new(GossipHandle::new_for_testing(members));
        let resolver: HashMap<_, _> = (1..=NODES).map(|n| (addr(n).grpc_addr, NodeId(n))).collect();
        let record = PlacementRecord {
            stage,
            ..PlacementRecord::starting(S3, S5)
        };
        let board = Arc::new(Board {
            gossip: gossip.clone(),
            records: Mutex::new(ids().into_iter().map(|node| (node, record)).collect()),
        });

        let mut nodes = Vec::new();
        for n in 1..=NODES {
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
            drop(open(false).await.unwrap());
            let storage = open(true).await.unwrap();
            storage.index_db().settle_tombstone_horizon(horizon.into()).await.unwrap();
            crate::metadata_placement::store::store(storage.index_db(), &record)
                .await
                .unwrap();
            let storage = Arc::new(storage);
            let cell = StageCell::new(stage);
            let write_handler = Arc::new(
                ReplicaWriteHandler::new(
                    storage.clone(),
                    Arc::new(IdempotencyTracker::new()),
                    config.epoch,
                )
                .with_bucket_metadata_stage(cell.clone()),
            );
            let read_handler = Arc::new(ReplicaReadHandler::new(storage.clone(), config.epoch));
            let router = PlacementRouter::new(
                &config,
                NodeId(n),
                BucketPlacement::from_entries([(SYSTEM.to_string(), pool_id)]),
                &resolver,
            )
            .unwrap()
            .with_bucket_metadata_move(
                pool_id,
                BucketMetadataMove::new(layout(S3), layout(S5), cell.clone()),
            )
            .unwrap();
            nodes.push(Node {
                storage,
                stage: cell,
                write_handler,
                read_handler,
                router: Arc::new(router),
                rpc_client: Arc::new(NodeClient::new(gossip.clone(), NodeClientConfig::default())),
                server: None,
                _dir: dir,
            });
        }
        let mut pool = Self {
            config,
            gossip,
            board,
            old: layout(S3),
            new: layout(S5),
            nodes,
            base_port,
            _hints: tempfile::tempdir().unwrap(),
        };
        for n in 1..=NODES {
            pool.start_node(n).await;
        }
        pool
    }

    fn node(&self, n: u128) -> &Node {
        &self.nodes[n as usize - 1]
    }

    /// Starts node `n`'s server, and has gossip hold it alive.
    async fn start_node(&mut self, n: u128) {
        let mut state = self.gossip.get_node(&NodeId(n)).unwrap();
        let node = &self.nodes[n as usize - 1];
        let server = NodeRpcServer::new(
            NodeId(n),
            Arc::new(state.meta.clone()),
            Arc::new(GossipHandle::new_for_testing(HashMap::new())),
        )
        .with_replica_handler(node.write_handler.clone())
        .with_read_handler(node.read_handler.clone())
        .start(RpcServerConfig {
            bind_addr: format!("127.0.0.1:{}", self.base_port + n as u16).parse().unwrap(),
            ..Default::default()
        })
        .await
        .unwrap();
        self.nodes[n as usize - 1].server = Some(server);
        state.status = NodeStatus::Alive;
        self.gossip.set_member_for_testing(NodeId(n), state);
    }

    /// Stops node `n`'s server, and has gossip hold it dead.
    async fn stop_node(&mut self, n: u128) {
        if let Some(server) = self.nodes[n as usize - 1].server.take() {
            server.shutdown().await.unwrap();
        }
        let mut state = self.gossip.get_node(&NodeId(n)).unwrap();
        state.status = NodeStatus::Dead;
        self.gossip.set_member_for_testing(NodeId(n), state);
    }

    fn read_coordinator(&self, n: u128) -> QuorumReadCoordinator {
        let node = self.node(n);
        QuorumReadCoordinator::new(
            NodeId(n),
            node.storage.clone(),
            node.rpc_client.clone(),
            node.router.clone(),
            Arc::new(HybridClock::new(NodeId(n))),
            Arc::new(ReadRepairService::start_with_capacity(
                node.rpc_client.clone(),
                16,
            )),
            &self.config,
            ReadTimeouts::default(),
        )
    }

    fn write_coordinator(&self, n: u128) -> QuorumWriteCoordinator {
        let node = self.node(n);
        QuorumWriteCoordinator::new(
            NodeId(n),
            node.storage.clone(),
            node.rpc_client.clone(),
            node.router.clone(),
            Arc::new(HybridClock::new(NodeId(n))),
            Arc::new(HintedHandoffManager::new(
                HintedHandoffConfig::new(self._hints.path().join(format!("node-{n}"))),
                node.rpc_client.clone(),
            )),
            &self.config,
            WriteTimeouts::default(),
        )
    }

    fn backfill(&self, n: u128) -> Backfill {
        let node = self.node(n);
        let reconciler = Reconciler::new(
            Arc::new(self.read_coordinator(n)),
            node.rpc_client.clone(),
            self.gossip.clone(),
            NodeId(n),
            self.config.epoch,
        )
        .with_local_replica(LocalReplica {
            write: node.write_handler.clone(),
            read: node.read_handler.clone(),
        });
        Backfill::new(node.storage.clone(), reconciler).reading_pages_of(7)
    }

    /// The driver of node `n`, from the record it stored.
    async fn driver(&self, n: u128) -> StageDriver {
        let node = self.node(n);
        let record = crate::metadata_placement::store::load(node.storage.index_db())
            .await
            .unwrap()
            .unwrap();
        StageDriver::new(
            ids(),
            NodeId(n),
            record,
            Some(node.stage.clone()),
            Arc::new(NodeBoard {
                board: self.board.clone(),
                node: NodeId(n),
            }),
            node.storage.index_db().clone(),
            Some(Arc::new(self.backfill(n)) as Arc<dyn BackfillPass>),
            PROTOCOL_VERSION,
            BUCKET_METADATA_MIN_PROTOCOL_VERSION,
        )
        .with_intervals(Duration::from_millis(50), Duration::from_millis(200))
    }

    /// The old replicas of `key` in `__system__`, then the new ones it gains.
    fn replicas_of(&self, key: &str) -> (Vec<NodeId>, Vec<NodeId>) {
        let full_key = format!("{SYSTEM}/{key}");
        let old = self.old.strategy().get_replicas(&full_key);
        let added = self
            .new
            .strategy()
            .get_replicas(&full_key)
            .into_iter()
            .filter(|node| !old.contains(node))
            .collect();
        (old, added)
    }

    /// Node `node` holds `key` as a replicated write at `hlc` left it.
    async fn put(&self, node: NodeId, key: &str, hlc: HlcTimestamp) {
        self.node(node.0)
            .storage
            .put_object(
                SYSTEM,
                key,
                b"1",
                "application/octet-stream",
                &hlc_metadata(hlc),
            )
            .await
            .unwrap();
    }

    /// Node `node` holds the tombstone of a delete of `key` at `hlc`.
    async fn tombstone(&self, node: NodeId, key: &str, hlc: HlcTimestamp) {
        self.node(node.0)
            .storage
            .tombstone_object(SYSTEM, key, hlc.wall_time, hlc.logical, hlc.node_id)
            .await
            .unwrap();
    }

    /// What `node` holds of `key`: the wall time of a copy or of a tombstone.
    async fn held(&self, node: NodeId, key: &str) -> Held {
        match self.node(node.0).storage.key_version(SYSTEM, key).await.unwrap() {
            KeyVersion::Live(record) => Held::Copy(record.hlc_timestamp),
            KeyVersion::Tombstone(record) => Held::Tombstone(record.hlc_timestamp),
            KeyVersion::Absent => Held::Nothing,
        }
    }

    /// Whether bucket `bucket` is confirmed through node `n`: `Ok(true)` for a
    /// marker, `Ok(false)` for none, `Err` when the quorum cannot tell.
    async fn bucket_exists(&self, n: u128, bucket: &str) -> Result<bool, ClusterError> {
        match self.read_coordinator(n).head(SYSTEM, &marker(bucket)).await {
            Ok(_) => Ok(true),
            Err(ClusterError::ObjectNotFound { .. }) => Ok(false),
            Err(e) => Err(e),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Held {
    Copy(u64),
    Tombstone(u64),
    Nothing,
}

/// A key of `__system__` whose marker name starts with `prefix`, picked so
/// that `pick` holds of its old and added replicas.
fn key_where(
    pool: &MovingPool,
    prefix: &str,
    pick: impl Fn(&[NodeId], &[NodeId]) -> bool,
) -> String {
    (0..10_000)
        .map(|i| marker(&format!("{prefix}{i}")))
        .find(|key| {
            let (old, added) = pool.replicas_of(key);
            pick(&old, &added)
        })
        .unwrap_or_else(|| panic!("no key for {prefix}"))
}

fn total(reports: &[BackfillReport]) -> BackfillReport {
    let mut sum = BackfillReport::default();
    for report in reports {
        sum.copied += report.copied;
        sum.already += report.already;
        sum.unconfirmed += report.unconfirmed;
        sum.failed += report.failed;
        sum.dropped += report.dropped;
        if sum.first_failure.is_none() {
            sum.first_failure.clone_from(&report.first_failure);
        }
    }
    sum
}

/// The backfill brings a marker held by the three old replicas, or by two
/// of them, to all five new ones, and the newest tombstone of a deleted
/// bucket too, over a live copy a replica kept. A marker on one old replica
/// — a write that did not reach its quorum — stays there, and so does one
/// on two new replicas alone: the old layout's rule counts its own
/// replicas. A pass with a new replica down is not complete, and the next
/// one is (task BMR-04-001).
/// A marker an earlier release made up on a node outside the old layout —
/// stamped with its modification time, so it ranks above the bucket's real
/// marker, or outlives a delete that left no tombstone — and one this
/// release makes up at the oldest HLC are removed by the backfill of their
/// node once the new layout makes it a replica; the bucket that exists gets
/// its real marker there, the deleted one stays deleted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_backfill_removes_markers_made_up_outside_the_old_layout() {
    let pool = MovingPool::start(21660, Stage::DualWrite, at(50)).await;
    let any = |_: &[NodeId], _: &[NodeId]| true;
    let live = key_where(&pool, "live", any);
    let gone = key_where(&pool, "gone", any);
    let (old, added) = pool.replicas_of(&live);
    for node in &old {
        pool.put(*node, &live, at(40)).await;
    }
    // Stamped with nothing: the record carries no HLC of its own and reads
    // by its modification time, far above 40.
    pool.node(added[0].0)
        .storage
        .put_object(
            SYSTEM,
            &live,
            b"1",
            "application/octet-stream",
            &HashMap::new(),
        )
        .await
        .unwrap();
    pool.put(added[1], &live, HlcTimestamp::from_record(1, 0, 0)).await;
    // The delete of an earlier release left no tombstone on the old replicas.
    let (_, added_gone) = pool.replicas_of(&gone);
    pool.node(added_gone[0].0)
        .storage
        .put_object(
            SYSTEM,
            &gone,
            b"1",
            "application/octet-stream",
            &HashMap::new(),
        )
        .await
        .unwrap();

    let mut reports = Vec::new();
    for n in 1..=NODES {
        let report = pool.backfill(n).pass().await;
        assert!(report.complete(), "node {n}: {report:?}");
        reports.push(report);
    }
    let first = total(&reports);
    assert_eq!((first.dropped, first.unconfirmed), (3, 0), "{first:?}");
    for node in old.iter().chain(&added) {
        assert_eq!(
            pool.held(*node, &live).await,
            Held::Copy(40),
            "{live} on {node}"
        );
    }
    let (old_gone, added_gone) = pool.replicas_of(&gone);
    for node in old_gone.iter().chain(&added_gone) {
        assert_eq!(
            pool.held(*node, &gone).await,
            Held::Nothing,
            "{gone} on {node}"
        );
    }
    let mut reports = Vec::new();
    for n in 1..=NODES {
        reports.push(pool.backfill(n).pass().await);
    }
    let second = total(&reports);
    assert_eq!(
        (second.copied, second.dropped, second.failed),
        (0, 0, 0),
        "{second:?}"
    );
}

#[tokio::test]
async fn the_backfill_brings_what_the_old_layout_confirms_to_every_new_replica() {
    // Every node held data from before tombstones up to 50: a lone copy
    // older than that may have missed a delete.
    let mut pool = MovingPool::start(21620, Stage::DualWrite, at(50)).await;
    let any = |_: &[NodeId], _: &[NodeId]| true;
    let three = key_where(&pool, "three", any);
    let two = key_where(&pool, "two", any);
    let one = key_where(&pool, "one", any);
    let new_only = key_where(&pool, "new-only", any);
    let deleted = key_where(&pool, "deleted", any);
    let (old, added) = pool.replicas_of(&three);
    for node in &old {
        pool.put(*node, &three, at(40)).await;
    }
    let (old, _) = pool.replicas_of(&two);
    for node in &old[1..] {
        pool.put(*node, &two, at(40)).await;
    }
    let (old, _) = pool.replicas_of(&one);
    pool.put(old[0], &one, at(40)).await;
    let (_, added_new_only) = pool.replicas_of(&new_only);
    for node in &added_new_only {
        pool.put(*node, &new_only, at(40)).await;
    }
    let (old_deleted, _) = pool.replicas_of(&deleted);
    pool.put(old_deleted[0], &deleted, at(40)).await;
    for node in &old_deleted[1..] {
        pool.tombstone(*node, &deleted, at(45)).await;
    }

    // One of the new replicas of the first marker is down: the pass that
    // reaches it is not complete.
    pool.stop_node(added[0].0).await;
    let mut reports = Vec::new();
    for n in 1..=NODES {
        if n != added[0].0 {
            reports.push(pool.backfill(n).pass().await);
        }
    }
    let first = total(&reports);
    assert!(first.failed > 0 && !first.complete(), "{first:?}");
    pool.start_node(added[0].0).await;

    let mut reports = Vec::new();
    for n in 1..=NODES {
        let report = pool.backfill(n).pass().await;
        assert!(report.complete(), "node {n}: {report:?}");
        reports.push(report);
    }
    let second = total(&reports);
    // The lone marker on its holder, the new-only one on each of its two.
    assert_eq!(second.unconfirmed, 3, "{second:?}");

    for key in [&three, &two] {
        let (old, added) = pool.replicas_of(key);
        for node in old.iter().chain(&added) {
            assert_eq!(
                pool.held(*node, key).await,
                Held::Copy(40),
                "{key} on {node}"
            );
        }
    }
    let (old, added) = pool.replicas_of(&deleted);
    for node in old.iter().chain(&added) {
        assert_eq!(
            pool.held(*node, &deleted).await,
            Held::Tombstone(45),
            "on {node}"
        );
    }
    let (old, added) = pool.replicas_of(&one);
    assert_eq!(pool.held(old[0], &one).await, Held::Copy(40));
    for node in old[1..].iter().chain(&added) {
        assert_eq!(
            pool.held(*node, &one).await,
            Held::Nothing,
            "{one} on {node}"
        );
    }
    let (old, _) = pool.replicas_of(&new_only);
    for node in &old {
        assert_eq!(
            pool.held(*node, &new_only).await,
            Held::Nothing,
            "on {node}"
        );
    }
    // Nothing more to bring: a third pass finds every key in step.
    let mut reports = Vec::new();
    for n in 1..=NODES {
        reports.push(pool.backfill(n).pass().await);
    }
    let third = total(&reports);
    assert_eq!((third.copied, third.failed), (0, 0), "{third:?}");
}

/// What the writer was told of one bucket.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Acked {
    Created,
    Deleted,
}

/// Creates and deletes buckets through every node in turn until `stop`
/// turns true; what each acknowledged write left. A bucket one of whose
/// writes failed is left out: either outcome is right for it.
async fn write_while_moving(
    pool: Arc<MovingPool>,
    stop: tokio::sync::watch::Receiver<bool>,
) -> HashMap<String, Acked> {
    let mut acked = HashMap::new();
    let mut failed = std::collections::HashSet::new();
    let mut i = 0u128;
    while !*stop.borrow() {
        let n = i % NODES + 1;
        let bucket = format!("live-{i}");
        let coordinator = pool.write_coordinator(n);
        let created = coordinator
            .write(
                SYSTEM,
                &marker(&bucket),
                b"1",
                "application/octet-stream",
                &HashMap::new(),
            )
            .await;
        match created {
            Ok(_) => {
                acked.insert(bucket.clone(), Acked::Created);
            }
            Err(_) => {
                failed.insert(bucket.clone());
            }
        }
        if i.is_multiple_of(3) && created.is_ok() {
            match pool.write_coordinator(n % NODES + 1).delete(SYSTEM, &marker(&bucket)).await {
                Ok(_) => {
                    acked.insert(bucket.clone(), Acked::Deleted);
                }
                Err(_) => {
                    failed.insert(bucket.clone());
                }
            }
        }
        i += 1;
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    acked.retain(|bucket, _| !failed.contains(bucket));
    acked
}

/// The whole move on six nodes: buckets created and deleted at stage 0 —
/// some left on two old replicas only, some deleted with a stale copy left on
/// one — buckets created and deleted while the pool moves, every node taking the
/// stages by itself, the backfill included, up to stage 3. Before the move,
/// two nodes down leave some bucket unconfirmed; after it, every pair of
/// nodes down leaves every bucket as its acknowledged writes left it (task
/// BMR-04-004).
#[tokio::test]
async fn the_pool_moves_by_itself_and_then_survives_any_two_nodes_down() {
    let mut pool = MovingPool::start(21640, Stage::Legacy, at(1)).await;
    let mut expected = HashMap::new();
    for i in 0..40u128 {
        let bucket = format!("b{i}");
        pool.write_coordinator(i % NODES + 1)
            .write(
                SYSTEM,
                &marker(&bucket),
                b"1",
                "application/octet-stream",
                &HashMap::new(),
            )
            .await
            .unwrap();
        expected.insert(bucket, Acked::Created);
    }
    for i in [3u128, 17, 29] {
        let bucket = format!("b{i}");
        pool.write_coordinator((i + 1) % NODES + 1)
            .delete(SYSTEM, &marker(&bucket))
            .await
            .unwrap();
        expected.insert(bucket, Acked::Deleted);
    }

    // Written before the move as a confirmed write that missed one replica
    // leaves it: on two old replicas. And deleted while one replica was
    // away: the tombstone on two, the stale copy on the third. Without the
    // backfill the first are lost, and the second come back, once reads take
    // the new replicas with the two holders down.
    for i in 0..10u64 {
        let partial = format!("partial-{i}");
        let (old, _) = pool.replicas_of(&marker(&partial));
        for node in &old[1..] {
            pool.put(*node, &marker(&partial), at(1_000 + i)).await;
        }
        expected.insert(partial, Acked::Created);
        let zombie = format!("zombie-{i}");
        let (old, _) = pool.replicas_of(&marker(&zombie));
        pool.put(old[0], &marker(&zombie), at(1_000 + i)).await;
        for node in &old[1..] {
            pool.tombstone(*node, &marker(&zombie), at(2_000 + i)).await;
        }
        expected.insert(zombie, Acked::Deleted);
    }

    // At stage 0 the two old replicas but one of b0 down leave it
    // unconfirmed: the defect the move removes.
    let (old, _) = pool.replicas_of(&marker("b0"));
    let live = ids().into_iter().find(|node| !old[..2].contains(node)).unwrap();
    pool.stop_node(old[0].0).await;
    pool.stop_node(old[1].0).await;
    assert!(pool.bucket_exists(live.0, "b0").await.is_err());
    pool.start_node(old[0].0).await;
    pool.start_node(old[1].0).await;

    let pool = Arc::new(pool);
    let (stop_drivers, drivers_stop) = tokio::sync::watch::channel(false);
    let mut drivers = Vec::new();
    let mut statuses = Vec::new();
    for n in 1..=NODES {
        let driver = pool.driver(n).await;
        let status = Arc::new(PlacementStatus::new(
            "pool-1",
            PlacementRecord::starting(S3, S5),
        ));
        statuses.push(status.clone());
        drivers.push(tokio::spawn(
            driver.with_status(status).run(drivers_stop.clone()),
        ));
    }
    let (stop_writer, writer_stop) = tokio::sync::watch::channel(false);
    let writer = tokio::spawn(write_while_moving(pool.clone(), writer_stop));

    tokio::time::timeout(Duration::from_secs(60), async {
        while (1..=NODES)
            .any(|n| pool.board.record(NodeId(n)).map(|r| r.stage) != Some(Stage::Settled))
        {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("every node settles the move");
    for driver in drivers {
        tokio::time::timeout(Duration::from_secs(5), driver)
            .await
            .expect("a settled driver returns")
            .unwrap();
    }
    drop(stop_drivers);
    // A few writes at stage 3 as well.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let _ = stop_writer.send(true);
    let during = writer.await.unwrap();
    assert!(
        during.len() >= 10,
        "the writer wrote while the pool moved: {during:?}"
    );
    expected.extend(during);
    for status in &statuses {
        let snapshot = status.snapshot(std::time::Instant::now());
        assert_eq!(snapshot.record.stage, Stage::Settled, "{snapshot:?}");
        assert!(snapshot.record.backfill_complete, "{snapshot:?}");
        assert!(snapshot.holdouts.is_empty(), "{snapshot:?}");
        assert_eq!(snapshot.held_for, None, "{snapshot:?}");
    }
    for n in 1..=NODES {
        assert_eq!(pool.node(n).stage.stage(), Stage::Settled);
        let stored = crate::metadata_placement::store::load(pool.node(n).storage.index_db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            stored,
            PlacementRecord {
                stage: Stage::Settled,
                backfill_complete: true,
                ..PlacementRecord::starting(S3, S5)
            }
        );
    }

    let mut pool = Arc::into_inner(pool).expect("the writer and the drivers ended");
    for a in 1..=NODES {
        for b in a + 1..=NODES {
            pool.stop_node(a).await;
            pool.stop_node(b).await;
            let live: Vec<u128> = (1..=NODES).filter(|n| *n != a && *n != b).collect();
            for (i, (bucket, acked)) in expected.iter().enumerate() {
                let through = live[i % live.len()];
                let exists = pool.bucket_exists(through, bucket).await;
                assert_eq!(
                    exists.as_ref().ok(),
                    Some(&(*acked == Acked::Created)),
                    "{bucket} ({acked:?}) through node {through} with nodes {a} and {b} down: \
                     {exists:?}"
                );
            }
            pool.start_node(a).await;
            pool.start_node(b).await;
        }
    }
}
