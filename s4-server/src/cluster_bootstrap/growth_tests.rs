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

//! A pool's growth from beginning to end (plan
//! ERASURE-CODING-FIXING-POOL-GROWTH-REBALANCE.md, phase 4) on six nodes,
//! each a real `NodeRpcServer` on Bitcask with its own router, quorum
//! coordinators and part in the growth, wired as bootstrap wires them
//! ([`crate::pool_generation::start_growth`] and
//! [`crate::pool_generation::clean_up_foreign_copies`]). Nodes 1–3 are the
//! old generation (RF=3, W=R=2), nodes 1–6 the new one. Gossip is simulated:
//! a task copies every node's announcement of its generation to the others.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use s4_cluster::metadata_placement::{PlacementRecord, QuorumShape};
use s4_cluster::placement::GenerationPhase;
use s4_cluster::placement_fence::PlacementFloor;
use s4_cluster::pool_generation::{self, GenerationAnnouncement, PoolGenerationRecord};
use s4_cluster::rebalance::{
    IndexProgressStore, PoolTransition, ProgressStore, RangeCheck, RangeVerifier, RebalanceConfig,
    TransitionConfig, TransitionParts, VolumeFiles,
};
use s4_cluster::rpc::server::RpcServerHandle;
use s4_cluster::{
    BucketPlacement, ClusterConfig, Edition, GossipHandle, HintedHandoffConfig,
    HintedHandoffManager, HybridClock, IdempotencyTracker, LocalReplica, NodeAddr, NodeCapacity,
    NodeClient, NodeClientConfig, NodeId, NodeMeta, NodeRpcServer, NodeState, NodeStatus,
    PlacementRouter, PoolConfig, PoolId, PoolMoveSlot, QuorumReadCoordinator,
    QuorumWriteCoordinator, ReadTimeouts, Reconciler, ReplicaMultipartHandler, ReplicaReadHandler,
    ReplicaWriteHandler, RpcServerConfig, WriteTimeouts,
};
use s4_core::storage::BitcaskStorageEngine;
use s4_core::types::StorageClass;
use s4_core::{KeyVersion, StorageEngine};
use tokio::sync::{watch, RwLock};

use super::SharedStorageAdapter;

const POOL: PoolId = PoolId(0);
const BUCKET: &str = "bucket";

fn addr(base: u16, n: u128) -> NodeAddr {
    NodeAddr {
        grpc_addr: format!("127.0.0.1:{}", base + n as u16).parse().unwrap(),
        http_addr: format!("127.0.0.1:{}", base + 100 + n as u16).parse().unwrap(),
        gossip_addr: format!("127.0.0.1:{}", base + 200 + n as u16).parse().unwrap(),
    }
}

fn config(base: u16) -> ClusterConfig {
    ClusterConfig {
        pools: vec![PoolConfig {
            pool_id: POOL,
            name: "pool".into(),
            nodes: (1..=3).map(|n| addr(base, n)).collect(),
            replication_factor: Some(3),
            write_quorum: Some(2),
            read_quorum: Some(2),
            storage_class: StorageClass::Standard,
        }],
        default_replication_factor: 3,
        default_write_quorum: 2,
        default_read_quorum: 2,
        ..ClusterConfig::default()
    }
}

fn prepared() -> PoolGenerationRecord {
    PoolGenerationRecord::initial(POOL, (1..=3).map(NodeId).collect())
        .unwrap()
        .configure((1..=6).map(NodeId).collect())
        .unwrap()
}

fn member(base: u16, n: u128) -> NodeState {
    NodeState {
        meta: NodeMeta {
            node_id: NodeId(n),
            node_name: format!("node-{n}"),
            addr: addr(base, n),
            pool_id: POOL,
            topology_epoch: 1,
            protocol_version: s4_cluster::PROTOCOL_VERSION,
            capacity: NodeCapacity {
                total_bytes: 1 << 40,
                used_bytes: 0,
                volume_count: 0,
                object_count: 0,
            },
            started_at: 0,
            s4_version: "test".into(),
            edition: Edition::Enterprise,
        },
        status: NodeStatus::Alive,
        last_seen: std::time::Instant::now(),
    }
}

/// One node: its storage, placement, coordinators and part in the growth.
struct Node {
    id: NodeId,
    storage: Arc<RwLock<BitcaskStorageEngine>>,
    adapter: Arc<SharedStorageAdapter>,
    router: Arc<PlacementRouter>,
    gossip: Arc<GossipHandle>,
    rpc_client: Arc<NodeClient>,
    read: Arc<QuorumReadCoordinator>,
    write: Arc<QuorumWriteCoordinator>,
    replica_write: Arc<ReplicaWriteHandler>,
    replica_read: Arc<ReplicaReadHandler>,
    transition: Arc<PoolTransition>,
    store: Arc<dyn ProgressStore>,
    _server: RpcServerHandle,
}

struct Pool {
    nodes: Vec<Node>,
    shutdown: watch::Sender<bool>,
    _dirs: Vec<tempfile::TempDir>,
}

impl Pool {
    fn node(&self, n: u128) -> &Node {
        &self.nodes[n as usize - 1]
    }
}

async fn pool(base: u16) -> Pool {
    let config = config(base);
    let record = prepared();
    let (shutdown, _) = watch::channel(false);
    let mut dirs = Vec::new();
    let mut nodes = Vec::new();
    for n in 1..=6u128 {
        let dir = tempfile::tempdir().unwrap();
        let engine = BitcaskStorageEngine::open(
            dir.path().join("volumes"),
            dir.path().join("metadata_db"),
            10 * 1024 * 1024,
            4096,
            false,
            s4_core::storage::IndexOptions {
                version_digests: true,
            },
        )
        .await
        .unwrap();
        let index = engine.index_db().clone();
        let storage = Arc::new(RwLock::new(engine));
        // As bootstrap settles it: zero for a node created keeping tombstones.
        super::settle_tombstone_horizon(&storage, &HybridClock::new(NodeId(n)))
            .await
            .unwrap();
        let adapter = Arc::new(SharedStorageAdapter::new(storage.clone()));
        pool_generation::store(&index, &record).await.unwrap();
        let gossip = Arc::new(GossipHandle::new_for_testing(
            (1..=6u128).filter(|m| *m != n).map(|m| (NodeId(m), member(base, m))).collect(),
        ));
        for m in 1..=6u128 {
            gossip.set_bucket_metadata_placement_for_testing(
                NodeId(m),
                Some(PlacementRecord::settled(QuorumShape::majority(3))),
            );
        }
        gossip.announce_pool_generation(GenerationAnnouncement::of(&record, false));
        let rpc_client = Arc::new(NodeClient::new(gossip.clone(), NodeClientConfig::default()));
        let router = Arc::new(
            PlacementRouter::from_pool_node_ids(
                &config,
                NodeId(n),
                BucketPlacement::new(),
                &HashMap::from([(POOL, (1..=3).map(NodeId).collect())]),
            )
            .unwrap()
            .with_pool_generation(&record)
            .unwrap(),
        );
        let floor = PlacementFloor::new(0);
        let clock = Arc::new(HybridClock::new(NodeId(n)));
        let hints_dir = tempfile::tempdir().unwrap();
        let hints = Arc::new(HintedHandoffManager::new(
            HintedHandoffConfig::new(hints_dir.path()),
            rpc_client.clone(),
        ));
        dirs.push(hints_dir);
        let replica_write = Arc::new(
            ReplicaWriteHandler::new(adapter.clone(), Arc::new(IdempotencyTracker::new()), 1)
                .with_placement_floor(floor.clone()),
        );
        let replica_read = Arc::new(ReplicaReadHandler::new(adapter.clone(), 1));
        let slot = PoolMoveSlot::default();
        let server = NodeRpcServer::new(NodeId(n), Arc::new(member(base, n).meta), gossip.clone())
            .with_pool_move(slot.clone())
            .with_replica_handler(replica_write.clone())
            .with_read_handler(replica_read.clone())
            .with_multipart_handler(Arc::new(
                ReplicaMultipartHandler::new(
                    adapter.clone(),
                    Arc::new(IdempotencyTracker::new()),
                    1,
                )
                .with_placement_floor(floor.clone()),
            ))
            .start(RpcServerConfig {
                bind_addr: addr(base, n).grpc_addr,
                ..Default::default()
            })
            .await
            .unwrap();
        let read = Arc::new(QuorumReadCoordinator::new(
            NodeId(n),
            adapter.clone(),
            rpc_client.clone(),
            router.clone(),
            clock.clone(),
            Arc::new(s4_cluster::ReadRepairService::start_with_local(
                rpc_client.clone(),
                NodeId(n),
                adapter.clone(),
                config.epoch,
            )),
            &config,
            ReadTimeouts::default(),
        ));
        let write = Arc::new(
            QuorumWriteCoordinator::new(
                NodeId(n),
                adapter.clone(),
                rpc_client.clone(),
                router.clone(),
                clock.clone(),
                hints.clone(),
                &config,
                WriteTimeouts {
                    quorum_timeout: Duration::from_secs(5),
                    replica_write_timeout: Duration::from_secs(2),
                },
            )
            .with_multipart_storage(adapter.clone())
            .with_placement_floor(floor.clone()),
        );
        let store: Arc<dyn ProgressStore> = Arc::new(IndexProgressStore::new(index.clone()));
        let transition = Arc::new(
            PoolTransition::new(TransitionParts {
                record: record.clone(),
                local: NodeId(n),
                ranges: router.generation_ranges(&POOL).unwrap().clone(),
                floor,
                index,
                store: store.clone(),
                gossip: gossip.clone(),
                hints: Some(hints),
                clock,
            })
            .unwrap(),
        );
        transition.restore().await.unwrap();
        let _ = slot.set(transition.clone());
        dirs.push(dir);
        nodes.push(Node {
            id: NodeId(n),
            storage,
            adapter,
            router,
            gossip,
            rpc_client,
            read,
            write,
            replica_write,
            replica_read,
            transition,
            store,
            _server: server,
        });
    }
    // Gossip: every node's own announcement reaches the others.
    let handles: Vec<Arc<GossipHandle>> = nodes.iter().map(|node| node.gossip.clone()).collect();
    let mut stop = shutdown.subscribe();
    tokio::spawn(async move {
        loop {
            for (index, from) in handles.iter().enumerate() {
                let announced = from.pool_generation(&NodeId(0));
                for to in &handles {
                    to.set_pool_generation_for_testing(NodeId(index as u128 + 1), announced);
                }
            }
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(50)) => {}
                _ = stop.changed() => return,
            }
        }
    });
    Pool {
        nodes,
        shutdown,
        _dirs: dirs,
    }
}

fn rebalance_config() -> RebalanceConfig {
    RebalanceConfig {
        page_size: 64,
        retry_after: Duration::from_millis(200),
        settle: Duration::from_millis(200),
        ..RebalanceConfig::default()
    }
}

/// Starts the growth and the clean-up on every node, as bootstrap does.
fn start_growth(pool: &Pool, gc_grace: Duration) {
    for node in &pool.nodes {
        let local_replica = LocalReplica {
            write: node.replica_write.clone(),
            read: node.replica_read.clone(),
        };
        crate::pool_generation::start_growth(crate::pool_generation::GrowthParts {
            transition: node.transition.clone(),
            router: node.router.clone(),
            rpc_client: node.rpc_client.clone(),
            gossip: node.gossip.clone(),
            read: node.read.clone(),
            write: node.write.clone(),
            storage: node.adapter.clone(),
            multipart_storage: node.adapter.clone(),
            local_replica: local_replica.clone(),
            store: node.store.clone(),
            stored_bytes: Arc::new(VolumeFiles::new(std::env::temp_dir())),
            rebalance: rebalance_config(),
            gc_grace,
            settle: Duration::from_millis(300),
            poll: Duration::from_millis(250),
            topology_epoch: 1,
            shutdown: pool.shutdown.subscribe(),
        })
        .unwrap();
        tokio::spawn(crate::pool_generation::clean_up_foreign_copies(
            crate::pool_generation::CleanUpParts {
                record: node.transition.record(),
                transition: Some(node.transition.clone()),
                local: node.id,
                router: node.router.clone(),
                rpc_client: node.rpc_client.clone(),
                gossip: node.gossip.clone(),
                read: node.read.clone(),
                storage: node.adapter.clone(),
                multipart_storage: node.adapter.clone(),
                local_replica,
                store: node.store.clone(),
                rebalance: rebalance_config(),
                gc_grace,
                topology_epoch: 1,
            },
            pool.shutdown.subscribe(),
        ));
    }
}

async fn wait_for(what: &str, timeout: Duration, mut check: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + timeout;
    while !check() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn version(node: &Node, key: &str) -> KeyVersion {
    node.storage.read().await.key_version(BUCKET, key).await.unwrap()
}

/// Runs passes of `copies` until one is done, as `ForeignCopies::run` does
/// without its wait: under the load of the whole test suite an RPC to an
/// owner may time out, and the key it failed is taken by the next pass. A
/// key failed and taken again counts once more in `foreign`.
async fn pass_until_done(
    copies: &s4_cluster::rebalance::ForeignCopies,
    shutdown: &watch::Receiver<bool>,
) -> s4_cluster::rebalance::ForeignProgress {
    let mut progress = copies.pass(shutdown).await.unwrap().unwrap();
    for _ in 0..10 {
        if progress.phase == s4_cluster::rebalance::progress::Phase::Done {
            return progress;
        }
        progress = copies.pass(shutdown).await.unwrap().unwrap();
    }
    assert_eq!(
        progress.phase,
        s4_cluster::rebalance::progress::Phase::Done,
        "the pass still fails after ten: {:?}",
        progress.last_error
    );
    progress
}

fn hlc_metadata(wall: u64) -> HashMap<String, String> {
    HashMap::from([
        ("_s4_hlc_wall".to_string(), wall.to_string()),
        ("_s4_hlc_logical".to_string(), "0".to_string()),
        ("_s4_hlc_node_id".to_string(), "7".to_string()),
    ])
}

fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

/// The whole growth: written and deleted keys before it and during it, and
/// a key whose newest copy is one a failed write left on a single old owner.
/// The move begins by itself once every member is ready, every range is
/// verified and cut over, the pool passes the barrier and settles on six
/// nodes; every confirmed write reads back through a new node, the deleted
/// keys stay deleted, and after the grace period the old owners keep no copy
/// of a key they no longer own.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_pool_grows_by_itself_and_every_confirmed_write_survives() {
    let pool = pool(30100).await;
    let writer = pool.node(1);
    for i in 0..120 {
        writer
            .write
            .write(
                BUCKET,
                &format!("before-{i}"),
                format!("v{i}").as_bytes(),
                "text/plain",
                &HashMap::new(),
            )
            .await
            .unwrap();
    }
    for i in 0..10 {
        writer.write.delete(BUCKET, &format!("before-{i}")).await.unwrap();
    }
    // Confirmed on all three old owners, then a newer copy that a failed
    // write left on node 2 alone.
    let confirmed = now_millis() - 10_000;
    for node in 1..=3 {
        pool.node(node)
            .storage
            .read()
            .await
            .put_object(
                BUCKET,
                "half-written",
                b"confirmed",
                "text/plain",
                &hlc_metadata(confirmed),
            )
            .await
            .unwrap();
    }
    pool.node(2)
        .storage
        .read()
        .await
        .put_object(
            BUCKET,
            "half-written",
            b"failed",
            "text/plain",
            &hlc_metadata(confirmed + 1),
        )
        .await
        .unwrap();

    let started = std::time::Instant::now();
    start_growth(&pool, Duration::from_secs(2));
    // Writes while the pool moves, through an old and a new node.
    wait_for("the move to begin", Duration::from_secs(30), || {
        pool.nodes
            .iter()
            .all(|node| node.transition.phase() != GenerationPhase::Prepared)
    })
    .await;
    for i in 0..40 {
        let through = pool.node(if i % 2 == 0 { 1 } else { 5 });
        through
            .write
            .write(
                BUCKET,
                &format!("during-{i}"),
                format!("d{i}").as_bytes(),
                "text/plain",
                &HashMap::new(),
            )
            .await
            .unwrap();
    }
    pool.node(5).write.delete(BUCKET, "before-50").await.unwrap();

    wait_for(
        "the pool to settle on six nodes",
        Duration::from_secs(180),
        || {
            pool.nodes
                .iter()
                .all(|node| node.transition.phase() == GenerationPhase::Finished)
        },
    )
    .await;
    for node in &pool.nodes {
        let durable = pool_generation::load(&node.storage.read().await.index_db().clone())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(durable.current.epoch, 2, "node {}", node.id);
        assert_eq!(durable.current.nodes.len(), 6);
        assert!(durable.proposed.is_none() && !durable.moving);
    }

    // Every confirmed write reads back through a new node; deletes hold.
    let reader = pool.node(6);
    for i in 0..120 {
        let key = format!("before-{i}");
        let read = reader.read.read(BUCKET, &key).await;
        if i < 10 || i == 50 {
            assert!(read.is_err(), "{key} came back");
        } else {
            assert_eq!(read.unwrap().data, format!("v{i}").as_bytes(), "{key}");
        }
    }
    for i in 0..40 {
        let key = format!("during-{i}");
        assert_eq!(
            reader.read.read(BUCKET, &key).await.unwrap().data,
            format!("d{i}").as_bytes()
        );
    }
    // The confirmed version, or the newer one the failed write left: a
    // failed write may still take effect, as a read repair spreads it too.
    let half = reader.read.read(BUCKET, "half-written").await.unwrap();
    assert!(
        half.data == b"confirmed".to_vec() || half.data == b"failed".to_vec(),
        "{:?}",
        String::from_utf8_lossy(&half.data)
    );
    eprintln!("settled and verified in {:?}", started.elapsed());

    // After the grace period the old owners keep only what they own.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    'cleaned: loop {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for the clean-up of the former owners"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
        for node in &pool.nodes[..3] {
            for i in 10..120 {
                let key = format!("before-{i}");
                let owners = node.router.route(BUCKET, &key).unwrap().replicas;
                if !owners.contains(&node.id)
                    && !matches!(version(node, &key).await, KeyVersion::Absent)
                {
                    continue 'cleaned;
                }
            }
        }
        break;
    }
    eprintln!("former owners cleaned up in {:?}", started.elapsed());
    // And the owners still hold every key.
    for i in 10..120 {
        let key = format!("before-{i}");
        let owners = reader.router.route(BUCKET, &key).unwrap().replicas;
        for owner in owners {
            assert!(
                !matches!(version(pool.node(owner.0), &key).await, KeyVersion::Absent),
                "{key} missing on its owner {owner}"
            );
        }
    }
    let _ = pool.shutdown.send(true);
}

/// The range verifier of `leader`.
fn verifier_of(leader: &Node) -> RangeVerifier {
    RangeVerifier::new(
        leader.transition.clone(),
        leader.router.clone(),
        leader.rpc_client.clone(),
        leader.gossip.clone(),
        Arc::new(s4_cluster::rebalance::QuorumRecords::new(
            leader.read.clone(),
            leader.write.clone(),
        )),
        leader.read.clone(),
        Arc::new(
            Reconciler::new(
                leader.read.clone(),
                leader.rpc_client.clone(),
                leader.gossip.clone(),
                leader.id,
                1,
            )
            .with_local_replica(LocalReplica {
                write: leader.replica_write.clone(),
                read: leader.replica_read.clone(),
            }),
        ),
        leader.adapter.clone(),
        TransitionConfig::default(),
    )
}

/// Writes `data` with `etag` at `wall` to `node` as a repair would: the
/// version's HLC and ETag as given.
async fn put_version(node: &Node, key: &str, data: &[u8], wall: u64, etag: &str) {
    let response = node
        .replica_write
        .handle_write(&s4_cluster::rpc::proto::WriteBlobRequest {
            bucket: BUCKET.to_string(),
            key: key.to_string(),
            data: data.to_vec(),
            content_type: "text/plain".to_string(),
            hlc: Some(s4_cluster::HlcTimestamp::from_record(wall, 0, 7).to_proto()),
            operation_id: {
                let mut id = [0u8; 16];
                id[..8].copy_from_slice(&(node.id.0 as u64).to_be_bytes());
                id[8..].copy_from_slice(&wall.to_be_bytes());
                id.to_vec()
            },
            topology_epoch: 1,
            etag: etag.to_string(),
            repair_copy: true,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(response.success, "{}", response.error);
}

/// A range whose new owner lacks a key the old owners confirmed is not
/// verified: the verifier brings the key to it, and only the next pass
/// verifies the range. Cutting it over without the comparison would leave
/// the key unreadable through the successor.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_range_whose_new_owner_lacks_a_key_is_not_verified() {
    let pool = pool(30300).await;
    for node in &pool.nodes {
        node.transition.begin().await.unwrap();
    }
    // A key whose new owners include a node of the successor alone.
    let key = (0..10_000)
        .map(|i| format!("lacking-{i}"))
        .find(|key| {
            let decision = pool.node(1).router.route(BUCKET, key).unwrap();
            decision.write_rule.requirements()[1].nodes.iter().any(|node| node.0 > 3)
        })
        .unwrap();
    for node in 1..=3 {
        pool.node(node)
            .storage
            .read()
            .await
            .put_object(
                BUCKET,
                &key,
                b"old",
                "text/plain",
                &hlc_metadata(now_millis()),
            )
            .await
            .unwrap();
    }
    let leader = pool.node(1);
    let verifier = verifier_of(leader);
    let ranges = leader.transition.ranges().clone();
    let range = ranges.range_at(s4_core::ring_position_of(&format!("{BUCKET}/{key}")));
    let first = verifier.verify_range(&range, &Default::default()).await;
    assert!(
        matches!(&first, RangeCheck::NotYet(why) if why.contains("behind")),
        "{first:?}"
    );
    let new_owners = pool.node(1).router.route(BUCKET, &key).unwrap().write_rule.requirements()[1]
        .nodes
        .clone();
    for owner in &new_owners {
        assert!(
            matches!(version(pool.node(owner.0), &key).await, KeyVersion::Live(_)),
            "the verifier did not bring the key to {owner}"
        );
    }
    assert_eq!(
        verifier.verify_range(&range, &Default::default()).await,
        RangeCheck::Verified
    );
    // Hints for a new owner of the range hold it back too.
    let held = std::collections::HashSet::from([new_owners[0]]);
    assert!(matches!(
        verifier.verify_range(&range, &held).await,
        RangeCheck::NotYet(why) if why.contains("hints")
    ));
    let _ = pool.shutdown.send(true);
}

/// Two versions of a key with one HLC and two ETags — as replicas of a
/// release that computed the ETag of a multipart object differently keep
/// them — cannot be ordered by their digests: the old owners confirm one, the
/// new owners hold the other, which the total order of versions puts above
/// it. The range is verified: there is nothing to bring, and waiting for the
/// digests to agree would hold the growth forever.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_key_the_digests_cannot_order_does_not_hold_its_range_back() {
    let pool = pool(30340).await;
    for node in &pool.nodes {
        node.transition.begin().await.unwrap();
    }
    // A key whose new owners hold at most one of the old ones: two old owners
    // that are not new owners confirm the version the new owners lack.
    let key = (0..10_000)
        .map(|i| format!("unordered-{i}"))
        .find(|key| {
            let decision = pool.node(1).router.route(BUCKET, key).unwrap();
            decision.write_rule.requirements()[1]
                .nodes
                .iter()
                .filter(|node| node.0 <= 3)
                .count()
                <= 1
        })
        .unwrap();
    let new_owners = pool.node(1).router.route(BUCKET, &key).unwrap().write_rule.requirements()[1]
        .nodes
        .clone();
    let (mut higher, mut lower) = ("\"aaaa\"".to_string(), "\"bbbb\"".to_string());
    if s4_core::storage::EtagRank::of(&higher) < s4_core::storage::EtagRank::of(&lower) {
        std::mem::swap(&mut higher, &mut lower);
    }
    let wall = now_millis();
    for old in 1..=3 {
        if !new_owners.iter().any(|node| node.0 == old) {
            put_version(pool.node(old), &key, b"lower", wall, &lower).await;
        }
    }
    for owner in &new_owners {
        put_version(pool.node(owner.0), &key, b"higher", wall, &higher).await;
    }
    let verifier = verifier_of(pool.node(1));
    let ranges = pool.node(1).transition.ranges().clone();
    let range = ranges.range_at(s4_core::ring_position_of(&format!("{BUCKET}/{key}")));
    assert_eq!(
        verifier.verify_range(&range, &Default::default()).await,
        RangeCheck::Verified
    );
    let _ = pool.shutdown.send(true);
}

/// A node holds copies of keys placement gives to others, as a pool grown
/// before generations left them: the hand-over brings them to their owners,
/// which held nothing, and keeps them here; the clean-up removes them here
/// once the owners hold them, a tombstone too. A copy this node owns stays.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_former_owners_hand_over_and_then_remove_what_they_no_longer_own() {
    use s4_cluster::rebalance::{ForeignMode, ForeignProgress};
    let pool = pool(30500).await;
    for node in &pool.nodes {
        node.transition.adopt_finish().await.unwrap();
    }
    let local = pool.node(1);
    let not_owned = |prefix: &str| {
        (0..10_000)
            .map(|i| format!("{prefix}-{i}"))
            .find(|key| !local.router.route(BUCKET, key).unwrap().replicas.contains(&local.id))
            .unwrap()
    };
    let foreign = not_owned("foreign");
    let deleted = not_owned("deleted");
    let owned = (0..10_000)
        .map(|i| format!("owned-{i}"))
        .find(|key| local.router.route(BUCKET, key).unwrap().replicas.contains(&local.id))
        .unwrap();
    let storage = local.storage.read().await;
    storage
        .put_object(
            BUCKET,
            &foreign,
            b"kept here",
            "text/plain",
            &hlc_metadata(now_millis()),
        )
        .await
        .unwrap();
    storage
        .put_object(
            BUCKET,
            &owned,
            b"mine",
            "text/plain",
            &hlc_metadata(now_millis()),
        )
        .await
        .unwrap();
    storage.tombstone_object(BUCKET, &deleted, now_millis(), 0, 7).await.unwrap();
    drop(storage);

    let parts = crate::pool_generation::CleanUpParts {
        record: local.transition.record(),
        transition: None,
        local: local.id,
        router: local.router.clone(),
        rpc_client: local.rpc_client.clone(),
        gossip: local.gossip.clone(),
        read: local.read.clone(),
        storage: local.adapter.clone(),
        multipart_storage: local.adapter.clone(),
        local_replica: LocalReplica {
            write: local.replica_write.clone(),
            read: local.replica_read.clone(),
        },
        store: local.store.clone(),
        rebalance: rebalance_config(),
        gc_grace: Duration::from_secs(3600),
        topology_epoch: 1,
    };
    let (_stop, shutdown) = watch::channel(false);
    let handed = pass_until_done(
        &parts.copies("legacy/hand-over", ForeignMode::HandOver, None),
        &shutdown,
    )
    .await;
    assert!(handed.foreign >= 2, "{handed:?}");
    assert_eq!(handed.removed, 0, "{handed:?}");
    let owners = |key: &str| local.router.route(BUCKET, key).unwrap().replicas;
    for owner in owners(&foreign) {
        assert_eq!(
            pool.node(owner.0)
                .storage
                .read()
                .await
                .get_object(BUCKET, &foreign)
                .await
                .unwrap()
                .0,
            b"kept here"
        );
    }
    for owner in owners(&deleted) {
        assert!(matches!(
            version(pool.node(owner.0), &deleted).await,
            KeyVersion::Tombstone(_)
        ));
    }
    assert!(matches!(
        version(local, &foreign).await,
        KeyVersion::Live(_)
    ));

    let cleaned = pass_until_done(
        &parts.copies("legacy/clean-up", ForeignMode::CleanUp, None),
        &shutdown,
    )
    .await;
    assert!(cleaned.foreign >= 2, "{cleaned:?}");
    assert_eq!((cleaned.removed, cleaned.kept), (2, 0), "{cleaned:?}");
    assert!(matches!(version(local, &foreign).await, KeyVersion::Absent));
    assert!(matches!(version(local, &deleted).await, KeyVersion::Absent));
    assert!(matches!(version(local, &owned).await, KeyVersion::Live(_)));
    for owner in owners(&foreign) {
        assert!(matches!(
            version(pool.node(owner.0), &foreign).await,
            KeyVersion::Live(_)
        ));
    }
    // A pass that is done stays done across a restart.
    let stored = ForeignProgress::load(local.store.as_ref(), "legacy/clean-up")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.phase, s4_cluster::rebalance::progress::Phase::Done);
    let _ = pool.shutdown.send(true);
}

/// After a growth that settled, a former owner's copy its owners neither hold
/// nor outrank goes once it is older than the grace period: it was deleted
/// after the pool settled and its tombstone collected on the owners, or it
/// was never confirmed. Kept, the listing showed the deleted object for ever
/// (scenario 61, group A). A younger one is weighed again on a later pass,
/// and neither reaches the owners. The pass of a pool grown before
/// generations keeps such copies: there a former owner may hold the only one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn after_a_settled_growth_a_former_owner_removes_copies_its_owners_left_behind() {
    use s4_cluster::rebalance::ForeignMode;
    let pool = pool(30520).await;
    for node in &pool.nodes {
        node.transition.adopt_finish().await.unwrap();
    }
    let local = pool.node(1);
    let not_owned = |prefix: &str| {
        (0..10_000)
            .map(|i| format!("{prefix}-{i}"))
            .find(|key| !local.router.route(BUCKET, key).unwrap().replicas.contains(&local.id))
            .unwrap()
    };
    let deleted_since = not_owned("deleted-since");
    let young = not_owned("young");
    let storage = local.storage.read().await;
    storage
        .put_object(
            BUCKET,
            &deleted_since,
            b"deleted on the owners since",
            "text/plain",
            &hlc_metadata(now_millis() - 7_200_000),
        )
        .await
        .unwrap();
    storage
        .put_object(
            BUCKET,
            &young,
            b"written two minutes ago",
            "text/plain",
            &hlc_metadata(now_millis() - 120_000),
        )
        .await
        .unwrap();
    drop(storage);
    // Every other node collected a delete newer than both copies: its
    // absence of a key is a delete it may have taken, not a write it missed.
    let collected = now_millis() - 60_000;
    for node in pool.nodes.iter().filter(|node| node.id != local.id) {
        let storage = node.storage.read().await;
        storage.tombstone_object(BUCKET, "collected", collected, 0, 7).await.unwrap();
        let removed = storage
            .gc_tombstones(&[s4_core::storage::TombstoneRef {
                full_key: format!("{BUCKET}/collected"),
                hlc: (collected, 0, 7),
            }])
            .await
            .unwrap();
        assert_eq!(removed, 1);
    }
    let parts = crate::pool_generation::CleanUpParts {
        record: local.transition.record(),
        transition: None,
        local: local.id,
        router: local.router.clone(),
        rpc_client: local.rpc_client.clone(),
        gossip: local.gossip.clone(),
        read: local.read.clone(),
        storage: local.adapter.clone(),
        multipart_storage: local.adapter.clone(),
        local_replica: LocalReplica {
            write: local.replica_write.clone(),
            read: local.replica_read.clone(),
        },
        store: local.store.clone(),
        rebalance: rebalance_config(),
        gc_grace: Duration::from_secs(3600),
        topology_epoch: 1,
    };
    let (_stop, shutdown) = watch::channel(false);
    let owners = |key: &str| local.router.route(BUCKET, key).unwrap().replicas;

    // The pass of a pool grown before generations keeps both.
    let kept = pass_until_done(
        &parts.copies("legacy/clean-up", ForeignMode::CleanUp, None),
        &shutdown,
    )
    .await;
    assert!(kept.foreign >= 2, "{kept:?}");
    assert_eq!((kept.removed, kept.kept), (0, 2), "{kept:?}");

    let cleaned = parts
        .copies(
            "2/clean-up",
            ForeignMode::CleanUp,
            Some(Duration::from_secs(3600)),
        )
        .pass(&shutdown)
        .await
        .unwrap()
        .unwrap();
    // The young copy is tried once more after the walk, and counted again.
    assert_eq!((cleaned.removed, cleaned.kept), (1, 0));
    assert_ne!(
        cleaned.phase,
        s4_cluster::rebalance::progress::Phase::Done,
        "the young copy is weighed again later"
    );
    assert!(matches!(
        version(local, &deleted_since).await,
        KeyVersion::Absent
    ));
    assert!(matches!(version(local, &young).await, KeyVersion::Live(_)));
    for key in [&deleted_since, &young] {
        for owner in owners(key) {
            assert!(
                matches!(version(pool.node(owner.0), key).await, KeyVersion::Absent),
                "{key} reached its owner {owner}"
            );
        }
    }
    let _ = pool.shutdown.send(true);
}

/// The hand-over of a pool grown before generations gives the owners a copy
/// they cannot confirm: they ran a release that deleted a copy outright, so
/// their absence of the key — behind a horizon newer than the copy — may be a
/// delete or a write they never got. Keeping it would lose for good an object
/// the clients were told is stored (scenario 61, group F). The clean-up then
/// removes it from the former owner, as the owners hold it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_hand_over_of_a_legacy_pool_gives_the_owners_what_they_cannot_confirm() {
    use s4_cluster::rebalance::ForeignMode;
    let pool = pool(30560).await;
    for node in &pool.nodes {
        node.transition.adopt_finish().await.unwrap();
    }
    let local = pool.node(1);
    let key = (0..10_000)
        .map(|i| format!("legacy-{i}"))
        .find(|key| !local.router.route(BUCKET, key).unwrap().replicas.contains(&local.id))
        .unwrap();
    local
        .storage
        .read()
        .await
        .put_object(
            BUCKET,
            &key,
            b"only on the former owner",
            "text/plain",
            &hlc_metadata(now_millis() - 7_200_000),
        )
        .await
        .unwrap();
    let horizon = now_millis() - 60_000;
    for node in pool.nodes.iter().filter(|node| node.id != local.id) {
        let storage = node.storage.read().await;
        storage.tombstone_object(BUCKET, "collected", horizon, 0, 7).await.unwrap();
        storage
            .gc_tombstones(&[s4_core::storage::TombstoneRef {
                full_key: format!("{BUCKET}/collected"),
                hlc: (horizon, 0, 7),
            }])
            .await
            .unwrap();
    }
    let parts = crate::pool_generation::CleanUpParts {
        record: local.transition.record(),
        transition: None,
        local: local.id,
        router: local.router.clone(),
        rpc_client: local.rpc_client.clone(),
        gossip: local.gossip.clone(),
        read: local.read.clone(),
        storage: local.adapter.clone(),
        multipart_storage: local.adapter.clone(),
        local_replica: LocalReplica {
            write: local.replica_write.clone(),
            read: local.replica_read.clone(),
        },
        store: local.store.clone(),
        rebalance: rebalance_config(),
        gc_grace: Duration::from_secs(3600),
        topology_epoch: 1,
    };
    let (_stop, shutdown) = watch::channel(false);
    let handed = pass_until_done(
        &parts.copies("legacy/hand-over", ForeignMode::HandOver, None),
        &shutdown,
    )
    .await;
    assert!(handed.foreign >= 1, "{handed:?}");
    assert_eq!((handed.handed, handed.kept), (1, 0), "{handed:?}");
    for owner in local.router.route(BUCKET, &key).unwrap().replicas {
        assert_eq!(
            pool.node(owner.0)
                .storage
                .read()
                .await
                .get_object(BUCKET, &key)
                .await
                .unwrap()
                .0,
            b"only on the former owner"
        );
    }
    let cleaned = pass_until_done(
        &parts.copies("legacy/clean-up", ForeignMode::CleanUp, None),
        &shutdown,
    )
    .await;
    assert_eq!((cleaned.removed, cleaned.kept), (1, 0), "{cleaned:?}");
    assert!(matches!(version(local, &key).await, KeyVersion::Absent));
    let _ = pool.shutdown.send(true);
}

fn foreign_metadata_parts(local: &Node) -> crate::pool_generation::CleanUpParts {
    crate::pool_generation::CleanUpParts {
        record: local.transition.record(),
        transition: None,
        local: local.id,
        router: local.router.clone(),
        rpc_client: local.rpc_client.clone(),
        gossip: local.gossip.clone(),
        read: local.read.clone(),
        storage: local.adapter.clone(),
        multipart_storage: local.adapter.clone(),
        local_replica: LocalReplica {
            write: local.replica_write.clone(),
            read: local.replica_read.clone(),
        },
        store: local.store.clone(),
        rebalance: rebalance_config(),
        gc_grace: Duration::from_secs(3600),
        topology_epoch: 1,
    }
}

fn foreign_metadata_key(local: &Node, prefix: &str) -> String {
    (0..10_000)
        .map(|n| format!("{prefix}foreign-{n}"))
        .find(|key| !local.router.route("__system__", key).unwrap().replicas.contains(&local.id))
        .unwrap()
}

async fn unknown_delete_history(node: &Node, wall: u64) {
    let storage = node.storage.read().await;
    storage.tombstone_object("__system__", "collected", wall, 0, 7).await.unwrap();
    let removed = storage
        .gc_tombstones(&[s4_core::storage::TombstoneRef {
            full_key: "__system__/collected".into(),
            hlc: (wall, 0, 7),
        }])
        .await
        .unwrap();
    assert_eq!(removed, 1);
}

/// A legacy replica manufactured this marker for an object it stored. It is
/// no evidence that the bucket exists, whether its owners have collected the
/// delete or still hold a tombstone older than the manufactured timestamp.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn legacy_handover_keeps_a_made_up_foreign_bucket_marker() {
    use s4_cluster::rebalance::ForeignMode;
    for (tombstone, port) in [(false, 31260), (true, 31280)] {
        let pool = pool(port).await;
        for node in &pool.nodes {
            node.transition.adopt_finish().await.unwrap();
        }
        let local = pool.node(1);
        let key = foreign_metadata_key(local, "__s4_bucket_marker_");
        local
            .storage
            .read()
            .await
            .put_object("__system__", &key, b"1", "text/plain", &HashMap::new())
            .await
            .unwrap();
        let original = local.storage.read().await.head_object("__system__", &key).await.unwrap();
        let made_up = original.reported_hlc().0;
        let owners = local.router.route("__system__", &key).unwrap().replicas;
        for owner in &owners {
            if tombstone {
                pool.node(owner.0)
                    .storage
                    .read()
                    .await
                    .tombstone_object("__system__", &key, made_up - 1000, 0, 7)
                    .await
                    .unwrap();
            } else {
                unknown_delete_history(pool.node(owner.0), made_up + 1000).await;
            }
        }
        let parts = foreign_metadata_parts(local);
        let (_stop, shutdown) = watch::channel(false);
        let progress = pass_until_done(
            &parts.copies("legacy/hand-over", ForeignMode::HandOver, None),
            &shutdown,
        )
        .await;
        assert_eq!((progress.handed, progress.kept), (0, 1), "{progress:?}");
        for owner in owners {
            let held = pool
                .node(owner.0)
                .storage
                .read()
                .await
                .key_version("__system__", &key)
                .await
                .unwrap();
            if tombstone {
                assert!(
                    matches!(held, KeyVersion::Tombstone(ref record) if record.hlc_timestamp == made_up - 1000),
                    "{held:?}"
                );
            } else {
                assert!(
                    matches!(held, KeyVersion::Absent),
                    "foreign marker resurrected on {owner}: {held:?}"
                );
            }
        }
        let kept = local.storage.read().await.head_object("__system__", &key).await.unwrap();
        assert_eq!(kept.reported_hlc(), original.reported_hlc());
        let _ = pool.shutdown.send(true);
    }
}

/// After a verified growth, an invented marker outside its owners must not
/// live forever. Legacy hand-over still keeps it; clean-up waits for its age
/// and never propagates it to the actual owners.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn settled_growth_cleans_an_aged_made_up_foreign_bucket_marker() {
    use s4_cluster::rebalance::ForeignMode;
    let pool = pool(31920).await;
    for node in &pool.nodes {
        node.transition.adopt_finish().await.unwrap();
    }
    let local = pool.node(1);
    let key = foreign_metadata_key(local, "__s4_bucket_marker_");
    local
        .storage
        .read()
        .await
        .put_object("__system__", &key, b"1", "text/plain", &HashMap::new())
        .await
        .unwrap();
    let owners = local.router.route("__system__", &key).unwrap().replicas;
    let parts = foreign_metadata_parts(local);
    let (_stop, shutdown) = watch::channel(false);
    let young = parts
        .copies(
            "young/clean-up",
            ForeignMode::CleanUp,
            Some(Duration::from_secs(3600)),
        )
        .pass(&shutdown)
        .await
        .unwrap()
        .unwrap();
    assert_ne!(
        young.phase,
        s4_cluster::rebalance::progress::Phase::Done,
        "{young:?}"
    );
    assert!(local.storage.read().await.head_object("__system__", &key).await.is_ok());
    let cleaned = pass_until_done(
        &parts.copies(
            "settled/clean-up",
            ForeignMode::CleanUp,
            Some(Duration::ZERO),
        ),
        &shutdown,
    )
    .await;
    assert_eq!((cleaned.removed, cleaned.kept), (1, 0), "{cleaned:?}");
    assert!(matches!(
        local.storage.read().await.key_version("__system__", &key).await.unwrap(),
        KeyVersion::Absent
    ));
    for owner in owners {
        assert!(matches!(
            pool.node(owner.0)
                .storage
                .read()
                .await
                .key_version("__system__", &key)
                .await
                .unwrap(),
            KeyVersion::Absent
        ));
    }
    let _ = pool.shutdown.send(true);
}

/// The previous implementation persisted Done/kept>0, so a restart of that
/// epoch must reconsider its markers instead of trusting the obsolete pass.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn saved_cleanup_with_kept_markers_is_rechecked_after_the_upgrade() {
    use s4_cluster::rebalance::progress::Phase;
    use s4_cluster::rebalance::{ForeignMode, ForeignProgress};
    let pool = pool(31940).await;
    for node in &pool.nodes {
        node.transition.adopt_finish().await.unwrap();
    }
    let local = pool.node(1);
    let key = foreign_metadata_key(local, "__s4_bucket_marker_");
    local
        .storage
        .read()
        .await
        .put_object("__system__", &key, b"1", "text/plain", &HashMap::new())
        .await
        .unwrap();
    let parts = foreign_metadata_parts(local);
    let old = ForeignProgress {
        phase: Phase::Done,
        scanned: 1,
        foreign: 1,
        kept: 1,
        done_at: Some(s4_cluster::clock::hlc::now_millis()),
        ..ForeignProgress::default()
    };
    parts.store.save("saved/clean-up", old.encode().unwrap()).await.unwrap();
    let (_stop, shutdown) = watch::channel(false);
    for (name, mode, policy) in [
        ("saved/legacy-clean-up", ForeignMode::CleanUp, None),
        (
            "saved/hand-over",
            ForeignMode::HandOver,
            Some(Duration::ZERO),
        ),
    ] {
        parts.store.save(name, old.encode().unwrap()).await.unwrap();
        let unchanged =
            parts.copies(name, mode, policy).run(|| Ok(()), shutdown.clone()).await.unwrap();
        assert_eq!((unchanged.removed, unchanged.kept), (0, 1));
        assert!(local.storage.read().await.head_object("__system__", &key).await.is_ok());
    }
    let cleaned = parts
        .copies("saved/clean-up", ForeignMode::CleanUp, Some(Duration::ZERO))
        .run(|| Ok(()), shutdown)
        .await
        .unwrap();
    assert_eq!((cleaned.removed, cleaned.kept), (1, 0), "{cleaned:?}");
    assert!(matches!(
        local.storage.read().await.key_version("__system__", &key).await.unwrap(),
        KeyVersion::Absent
    ));
    let _ = pool.shutdown.send(true);
}

/// Unknown history does not authorise the legacy recovery override for
/// bucket metadata. A configuration could have been deleted and collected;
/// its own adoption path is responsible for deciding which legacy copy lives.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn legacy_handover_keeps_unconfirmed_bucket_metadata() {
    use s4_cluster::rebalance::ForeignMode;
    let pool = pool(31640).await;
    for node in &pool.nodes {
        node.transition.adopt_finish().await.unwrap();
    }
    let local = pool.node(1);
    let mut keys = Vec::new();
    for prefix in ["__s4_bucket_marker_", "__s4_bucket_cors_"] {
        let key = foreign_metadata_key(local, prefix);
        local
            .storage
            .read()
            .await
            .put_object(
                "__system__",
                &key,
                b"old metadata",
                "text/plain",
                &hlc_metadata(now_millis() - 7_200_000),
            )
            .await
            .unwrap();
        keys.push(key);
    }
    for node in &pool.nodes {
        if node.id != local.id {
            unknown_delete_history(node, now_millis() - 60_000).await;
        }
    }
    let parts = foreign_metadata_parts(local);
    let (_stop, shutdown) = watch::channel(false);
    let progress = pass_until_done(
        &parts.copies("legacy/hand-over", ForeignMode::HandOver, None),
        &shutdown,
    )
    .await;
    assert_eq!((progress.handed, progress.kept), (0, 2), "{progress:?}");
    for key in keys {
        for owner in local.router.route("__system__", &key).unwrap().replicas {
            let held = pool
                .node(owner.0)
                .storage
                .read()
                .await
                .key_version("__system__", &key)
                .await
                .unwrap();
            assert!(
                matches!(held, KeyVersion::Absent),
                "unconfirmed metadata resurrected on {owner}: {held:?}"
            );
        }
        assert!(local.storage.read().await.head_object("__system__", &key).await.is_ok());
    }
    let _ = pool.shutdown.send(true);
}

/// A confirmed real marker and configuration still reach every owner and
/// can subsequently leave their former holder; protecting unknown history
/// must not suppress all metadata hand-over.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn legacy_handover_transfers_confirmed_bucket_metadata() {
    use s4_cluster::rebalance::ForeignMode;
    let pool = pool(31660).await;
    for node in &pool.nodes {
        node.transition.adopt_finish().await.unwrap();
    }
    let local = pool.node(1);
    for node in &pool.nodes {
        unknown_delete_history(node, now_millis() - 60_000).await;
    }
    let mut keys = Vec::new();
    for prefix in ["__s4_bucket_marker_", "__s4_bucket_cors_"] {
        let key = foreign_metadata_key(local, prefix);
        let owners = local.router.route("__system__", &key).unwrap().replicas;
        let metadata = hlc_metadata(now_millis() - 7_200_000);
        for node in std::iter::once(local).chain(owners[..2].iter().map(|owner| pool.node(owner.0)))
        {
            node.storage
                .read()
                .await
                .put_object(
                    "__system__",
                    &key,
                    b"confirmed metadata",
                    "text/plain",
                    &metadata,
                )
                .await
                .unwrap();
        }
        keys.push(key);
    }
    let parts = foreign_metadata_parts(local);
    let (_stop, shutdown) = watch::channel(false);
    let progress = pass_until_done(
        &parts.copies("legacy/hand-over", ForeignMode::HandOver, None),
        &shutdown,
    )
    .await;
    assert_eq!((progress.handed, progress.kept), (2, 0), "{progress:?}");
    for key in &keys {
        for owner in local.router.route("__system__", key).unwrap().replicas {
            assert_eq!(
                pool.node(owner.0)
                    .storage
                    .read()
                    .await
                    .get_object("__system__", key)
                    .await
                    .unwrap()
                    .0,
                b"confirmed metadata"
            );
        }
    }
    let progress = pass_until_done(
        &parts.copies("legacy/clean-up", ForeignMode::CleanUp, None),
        &shutdown,
    )
    .await;
    assert_eq!(progress.removed, 2, "{progress:?}");
    for key in keys {
        assert!(matches!(
            local.storage.read().await.key_version("__system__", &key).await.unwrap(),
            KeyVersion::Absent
        ));
    }
    let _ = pool.shutdown.send(true);
}

/// A settled pool lists only what the owners of each key hold: the copy a
/// former owner keeps until its clean-up — here of a key deleted since and
/// collected on its owners — is not listed, as no read of the key returns it,
/// while an owner's object is.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_settled_pool_lists_no_copy_a_former_owner_keeps() {
    let pool = pool(30540).await;
    for node in &pool.nodes {
        node.transition.adopt_finish().await.unwrap();
    }
    let local = pool.node(1);
    let owned_by = |owned: bool, prefix: &str| {
        (0..10_000)
            .map(|i| format!("{prefix}-{i}"))
            .find(|key| {
                local.router.route(BUCKET, key).unwrap().replicas.contains(&local.id) == owned
            })
            .unwrap()
    };
    let left_behind = owned_by(false, "left-behind");
    let listed = owned_by(true, "listed");
    local
        .storage
        .read()
        .await
        .put_object(
            BUCKET,
            &left_behind,
            b"old",
            "text/plain",
            &hlc_metadata(now_millis()),
        )
        .await
        .unwrap();
    for owner in local.router.route(BUCKET, &listed).unwrap().replicas {
        pool.node(owner.0)
            .storage
            .read()
            .await
            .put_object(
                BUCKET,
                &listed,
                b"live",
                "text/plain",
                &hlc_metadata(now_millis()),
            )
            .await
            .unwrap();
    }
    let list = s4_cluster::DistributedListCoordinator::new(
        local.id,
        local.adapter.clone(),
        local.rpc_client.clone(),
        local.router.clone(),
        1,
        s4_cluster::ListTimeouts::default(),
    );
    let keys: Vec<String> = list
        .list(BUCKET, "", 1000, None)
        .await
        .unwrap()
        .entries
        .into_iter()
        .map(|entry| entry.key)
        .collect();
    assert_eq!(keys, vec![listed]);
    let _ = pool.shutdown.send(true);
}

/// What every node reports it stores, for the estimate of what is left.
struct FixedBytes(u64);

impl s4_cluster::rebalance::StoredBytes for FixedBytes {
    fn stored_bytes(&self) -> Option<u64> {
        Some(self.0)
    }
}

/// The growth of `node` as the admin API sees it, as bootstrap builds it.
fn growth_of(node: &Node) -> s4_cluster::rebalance::PoolGrowth {
    s4_cluster::rebalance::PoolGrowth::new(s4_cluster::rebalance::PoolGrowthParts {
        pool: "pool".into(),
        record: node.transition.record(),
        transition: Some(node.transition.clone()),
        router: node.router.clone(),
        stored_bytes: Arc::new(FixedBytes(1_000_000)),
        records: Arc::new(s4_cluster::rebalance::QuorumRecords::new(
            node.read.clone(),
            node.write.clone(),
        )),
    })
}

/// An operator's pause is the pool's: paused through a new node before the
/// growth began, every old node begins the move paused and hands over
/// nothing, so no range is verified and the pool keeps moving; resumed
/// through another node, the pool settles on six nodes and every key reads
/// back. The report follows each step.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_pause_holds_the_move_on_every_member_until_it_is_resumed() {
    use s4_cluster::rebalance::{GrowthControlError, GrowthState};

    let pool = pool(30700).await;
    for i in 0..60 {
        pool.node(1)
            .write
            .write(
                BUCKET,
                &format!("key-{i}"),
                format!("v{i}").as_bytes(),
                "text/plain",
                &HashMap::new(),
            )
            .await
            .unwrap();
    }
    let prepared = growth_of(pool.node(4)).report();
    assert_eq!(prepared.state, GrowthState::Prepared);
    assert_eq!(
        (prepared.generation.epoch, prepared.generation.nodes),
        (1, 3)
    );
    let next = prepared.moving_to.unwrap();
    assert_eq!((next.epoch, next.nodes), (2, 6));
    assert!(
        prepared.transfer.is_none(),
        "node 4 holds nothing of the old generation"
    );

    let paused = growth_of(pool.node(4)).pause(Some("night window".into())).await.unwrap();
    assert_eq!(paused.paused.as_deref(), Some("night window"));

    start_growth(&pool, Duration::from_secs(2));
    wait_for(
        "the move to begin everywhere",
        Duration::from_secs(30),
        || pool.nodes.iter().all(|node| node.transition.phase() == GenerationPhase::Moving),
    )
    .await;
    wait_for(
        "every node to take the pause",
        Duration::from_secs(10),
        || {
            pool.nodes
                .iter()
                .all(|node| node.transition.paused().as_deref() == Some("night window"))
        },
    )
    .await;
    // Long enough for an unpaused move of 60 keys to end several times over.
    tokio::time::sleep(Duration::from_secs(4)).await;
    for node in &pool.nodes[..3] {
        let report = growth_of(node).report();
        assert_eq!(report.state, GrowthState::Moving, "node {}", node.id);
        assert_eq!(report.paused.as_deref(), Some("night window"));
        assert_eq!(report.ranges.verified + report.ranges.cut_over, 0);
        let transfer = report.transfer.expect("an old node hands over");
        assert!(!transfer.handed_over);
        assert_eq!(
            transfer.keys_scanned, 0,
            "node {} moved while paused",
            node.id
        );
        assert_eq!(transfer.remaining_bytes, 1_000_000);
        assert_eq!(transfer.eta_secs, None);
    }
    for node in &pool.nodes[3..] {
        assert!(growth_of(node).report().transfer.is_none());
    }

    let resumed = growth_of(pool.node(2)).resume().await.unwrap();
    assert_eq!(resumed.paused, None);
    wait_for(
        "the pool to settle on six nodes",
        Duration::from_secs(180),
        || {
            pool.nodes
                .iter()
                .all(|node| node.transition.phase() == GenerationPhase::Finished)
        },
    )
    .await;
    for node in &pool.nodes {
        let report = growth_of(node).report();
        assert_eq!(report.state, GrowthState::Stable);
        assert_eq!((report.generation.epoch, report.generation.nodes), (2, 6));
        assert_eq!(report.ranges.total, 0);
        assert!(report.transfer.is_none());
    }
    for i in 0..60 {
        assert_eq!(
            pool.node(6).read.read(BUCKET, &format!("key-{i}")).await.unwrap().data,
            format!("v{i}").as_bytes()
        );
    }
    assert!(matches!(
        growth_of(pool.node(1)).pause(None).await,
        Err(GrowthControlError::NotGrowing(_))
    ));
    let _ = pool.shutdown.send(true);
}
