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

//! A multipart completion through the write coordinator of node 1 of a
//! three-node pool (RF=3, W=R=2) whose other replicas are real
//! `NodeRpcServer`s on Bitcask, each behind the adapter bootstrap uses: the
//! completion is issued with an HLC above every version of the key the
//! inspection saw.

use std::collections::HashMap;
use std::sync::Arc;

use s4_cluster::rpc::server::RpcServerHandle;
use s4_cluster::{
    BucketPlacement, ClusterConfig, Edition, GossipHandle, HintedHandoffConfig,
    HintedHandoffManager, HlcTimestamp, HybridClock, IdempotencyTracker, NodeAddr, NodeCapacity,
    NodeClient, NodeClientConfig, NodeId, NodeMeta, NodeRpcServer, NodeState, NodeStatus,
    PlacementRouter, PoolConfig, PoolId, QuorumWriteCoordinator, ReplicaMultipartHandler,
    ReplicaReadHandler, ReplicaWriteHandler, RpcServerConfig, WriteTimeouts,
};
use s4_core::storage::BitcaskStorageEngine;
use s4_core::types::StorageClass;
use s4_core::{KeyVersion, StorageEngine};
use tokio::io::AsyncReadExt;
use tokio::sync::RwLock;

use super::SharedStorageAdapter;

/// The storage of each of the three nodes, and node 1's write coordinator.
struct Pool {
    storages: Vec<Arc<RwLock<BitcaskStorageEngine>>>,
    coordinator: QuorumWriteCoordinator,
    router: Arc<PlacementRouter>,
    rpc_client: Arc<NodeClient>,
    hints: Arc<HintedHandoffManager>,
    hints_dir: std::path::PathBuf,
    gossip: Arc<GossipHandle>,
    config: ClusterConfig,
    _servers: Vec<RpcServerHandle>,
    _dirs: Vec<tempfile::TempDir>,
}

async fn pool(base_port: u16) -> Pool {
    pool_with_generation(base_port, false, &[]).await
}

async fn pool_with_generation(base_port: u16, growing: bool, down: &[u128]) -> Pool {
    let node_count = if growing { 6 } else { 3 };
    let addr = |n: u128| NodeAddr {
        grpc_addr: format!("127.0.0.1:{}", base_port + n as u16).parse().unwrap(),
        http_addr: format!("127.0.0.1:{}", 9000 + n).parse().unwrap(),
        gossip_addr: format!("127.0.0.1:{}", 9200 + n).parse().unwrap(),
    };
    let pool_id = PoolId::new(0);
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
        protocol_version: s4_cluster::PROTOCOL_VERSION,
        capacity: NodeCapacity::default(),
        started_at: 0,
        s4_version: "0.1.0".into(),
        edition: Edition::Community,
    };

    let mut dirs = Vec::new();
    let mut storages = Vec::new();
    let mut servers = Vec::new();
    for n in 1..=node_count {
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
        let storage = Arc::new(RwLock::new(engine));
        dirs.push(dir);
        if n > 1 && !down.contains(&n) {
            let adapter = Arc::new(SharedStorageAdapter::new(storage.clone()));
            let server = NodeRpcServer::new(
                NodeId(n),
                Arc::new(meta(n)),
                Arc::new(GossipHandle::new_for_testing(HashMap::new())),
            )
            .with_replica_handler(Arc::new(
                ReplicaWriteHandler::new(
                    adapter.clone(),
                    Arc::new(IdempotencyTracker::new()),
                    config.epoch,
                )
                .with_placement_generation(if growing { 2 } else { 0 }),
            ))
            .with_read_handler(Arc::new(ReplicaReadHandler::new(
                adapter.clone(),
                config.epoch,
            )))
            .with_multipart_handler(Arc::new(
                ReplicaMultipartHandler::new(
                    adapter,
                    Arc::new(IdempotencyTracker::new()),
                    config.epoch,
                )
                .with_placement_generation(if growing { 2 } else { 0 }),
            ))
            .start(RpcServerConfig {
                bind_addr: addr(n).grpc_addr,
                ..Default::default()
            })
            .await
            .unwrap();
            servers.push(server);
        }
        storages.push(storage);
    }

    let members = (2..=node_count)
        .map(|n| {
            (
                NodeId(n),
                NodeState {
                    meta: meta(n),
                    status: if down.contains(&n) {
                        NodeStatus::Dead
                    } else {
                        NodeStatus::Alive
                    },
                    last_seen: std::time::Instant::now(),
                },
            )
        })
        .collect();
    let gossip = Arc::new(GossipHandle::new_for_testing(members));
    let rpc_client = Arc::new(NodeClient::new(gossip.clone(), NodeClientConfig::default()));
    for n in 2..=node_count {
        if down.contains(&n) {
            continue;
        }
        let node_id = NodeId(n);
        let mut connected = false;
        let mut last_error = None;
        for _ in 0..10 {
            let result = rpc_client
                .send_head(
                    node_id,
                    s4_cluster::rpc::proto::HeadObjectRequest {
                        bucket: "bucket".into(),
                        key: "fixture-readiness".into(),
                        topology_epoch: config.epoch,
                        ..Default::default()
                    },
                )
                .await;
            if result.is_ok() {
                connected = true;
                break;
            }
            last_error = result.err();
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        assert!(
            connected,
            "fixture node {node_id} never accepted a connection: {last_error:?}"
        );
    }
    let resolver: HashMap<_, _> = (1..=3).map(|n| (addr(n).grpc_addr, NodeId(n))).collect();
    let mut router =
        PlacementRouter::new(&config, NodeId(1), BucketPlacement::new(), &resolver).unwrap();
    if growing {
        let record = s4_cluster::pool_generation::PoolGenerationRecord::initial(
            pool_id,
            (1..=3).map(NodeId).collect(),
        )
        .unwrap()
        .configure((1..=6).map(NodeId).collect())
        .unwrap()
        .begin()
        .unwrap();
        router = router.with_pool_generation(&record).unwrap();
    }
    let router = Arc::new(router);
    let local = Arc::new(SharedStorageAdapter::new(storages[0].clone()));
    let hints_dir = tempfile::tempdir().unwrap();
    let hints_path = hints_dir.path().join("hints");
    let hints = Arc::new(HintedHandoffManager::new(
        HintedHandoffConfig::new(hints_dir.path()),
        rpc_client.clone(),
    ));
    let coordinator = QuorumWriteCoordinator::new(
        NodeId(1),
        local.clone(),
        rpc_client.clone(),
        router.clone(),
        Arc::new(HybridClock::new(NodeId(1))),
        hints.clone(),
        &config,
        if growing {
            WriteTimeouts {
                quorum_timeout: std::time::Duration::from_secs(2),
                replica_write_timeout: std::time::Duration::from_secs(1),
            }
        } else {
            WriteTimeouts::default()
        },
    )
    .with_multipart_storage(local);
    dirs.push(hints_dir);
    Pool {
        storages,
        coordinator,
        router,
        rpc_client,
        hints,
        hints_dir: hints_path,
        gossip,
        config: config.clone(),
        _servers: servers,
        _dirs: dirs,
    }
}

/// A completion that follows a delete issued by a clock an hour ahead of
/// node 1's is published above that delete on every replica, not turned
/// away by it: the inspection reports the tombstone's HLC, and the
/// coordinator issues the completion above it.
#[tokio::test]
async fn a_completion_after_a_delete_from_a_clock_ahead_is_published_above_it() {
    let pool = pool(21076).await;
    let coordinator = &pool.coordinator;
    coordinator
        .create_multipart_upload("upload", "bucket", "key", "text/plain", &HashMap::new())
        .await
        .unwrap();
    let part = coordinator
        .upload_multipart_part("bucket", "key", "upload", 1, b"the only part".to_vec())
        .await
        .unwrap();

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let ahead = now + 3_600_000;
    for storage in &pool.storages {
        assert!(storage
            .read()
            .await
            .tombstone_object("bucket", "key", ahead, 0, 7)
            .await
            .unwrap());
    }

    let completed = coordinator
        .complete_multipart_upload("bucket", "key", "upload", &[(1, part.etag)])
        .await
        .unwrap();
    let deleted = HlcTimestamp::from_record(ahead, 0, 7);
    assert!(
        completed.hlc > deleted,
        "{} is not above {deleted}",
        completed.hlc
    );
    for (n, storage) in pool.storages.iter().enumerate() {
        let mut version = storage.read().await.key_version("bucket", "key").await.unwrap();
        for _ in 0..100 {
            if matches!(version, KeyVersion::Live(_)) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            version = storage.read().await.key_version("bucket", "key").await.unwrap();
        }
        assert!(
            matches!(
                &version,
                KeyVersion::Live(record) if HlcTimestamp::of_record(record) == completed.hlc
            ),
            "node {}: {version:?}",
            n + 1
        );
    }
}

/// A replica of a stable pool that misses a multipart operation gets no
/// multipart hint: the hint log of a stable pool stays readable by the
/// release before, which knows none, so a rollback loses no hint. While the
/// pool grows, the missed operation is hinted.
#[tokio::test]
async fn multipart_hints_are_written_only_while_the_pool_grows() {
    let stable = pool_with_generation(31340, false, &[3]).await;
    stable
        .coordinator
        .create_multipart_upload("upload", "bucket", "key", "text/plain", &HashMap::new())
        .await
        .unwrap();
    stable
        .coordinator
        .upload_multipart_part("bucket", "key", "upload", 1, b"part".to_vec())
        .await
        .unwrap();
    stable
        .coordinator
        .abort_multipart_upload("bucket", "key", "upload")
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert_eq!(
        hint_records(&stable, 3),
        0,
        "a stable pool wrote a multipart hint"
    );

    let growing = pool_with_generation(31520, true, &[6]).await;
    let key = (0..10_000)
        .map(|n| format!("growth-hint-{n}"))
        .find(|key| {
            let d = growing.router.route("bucket", key).unwrap();
            d.replicas.contains(&NodeId(6))
                && d.write_rule.requirements().iter().all(|requirement| {
                    requirement.nodes.iter().filter(|node| node.0 != 6).count()
                        >= usize::from(requirement.count)
                })
        })
        .unwrap();
    growing
        .coordinator
        .create_multipart_upload("upload", "bucket", &key, "text/plain", &HashMap::new())
        .await
        .unwrap();
    let mut written = 0;
    for _ in 0..50 {
        written = hint_records(&growing, 6);
        if written > 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert_eq!(written, 1, "a growing pool did not hint the missed create");
}

/// The old group alone cannot confirm any kind of multipart mutation.
#[tokio::test]
async fn pool_growth_multipart_requires_acknowledgements_from_both_generations() {
    let pool = pool_with_generation(31200, true, &[4, 5, 6]).await;
    let key = (0..10_000)
        .map(|n| format!("growth-{n}"))
        .find(|key| {
            let d = pool.router.route("bucket", key).unwrap();
            d.write_rule.requirements().last().unwrap().nodes.iter().all(|n| n.0 > 3)
        })
        .unwrap();
    let error = pool
        .coordinator
        .create_multipart_upload("upload", "bucket", &key, "text/plain", &HashMap::new())
        .await;
    assert!(
        matches!(error, Err(s4_cluster::ClusterError::QuorumNotMet { .. })),
        "{error:?}"
    );
}

/// While a pool grows, every read path — GET, a streamed GET and HEAD —
/// repairs an old owner that answered with an older version, and leaves the
/// new owners it did not ask to the move: a repair per read and owner would
/// copy the object on every read, past the move's limits.
#[tokio::test]
async fn pool_growth_reads_repair_stale_answers_and_leave_new_owners_to_the_move() {
    let pool = pool_with_generation(31220, true, &[]).await;
    let keys: Vec<String> = ["repair", "stream-repair", "head-repair"]
        .iter()
        .map(|prefix| {
            (0..10_000)
                .map(|n| format!("{prefix}-{n}"))
                .find(|key| {
                    pool.router.route("bucket", key).unwrap().write_rule.requirements()[1]
                        .nodes
                        .iter()
                        .all(|n| n.0 > 3)
                })
                .unwrap()
        })
        .collect();
    let metadata = |wall: u64| {
        HashMap::from([
            ("_s4_hlc_wall".to_string(), wall.to_string()),
            ("_s4_hlc_logical".to_string(), "0".to_string()),
            ("_s4_hlc_node_id".to_string(), "1".to_string()),
        ])
    };
    // Old owners 1 and 2 hold the newest version, old owner 3 an older one.
    for key in &keys {
        for (index, storage) in pool.storages[..3].iter().enumerate() {
            let (body, wall): (&[u8], u64) = if index < 2 {
                (b"newest body", 100)
            } else {
                (b"older", 50)
            };
            storage
                .read()
                .await
                .put_object("bucket", key, body, "text/plain", &metadata(wall))
                .await
                .unwrap();
        }
    }
    let local = Arc::new(SharedStorageAdapter::new(pool.storages[0].clone()));
    let repair = Arc::new(
        s4_cluster::repair::read_repair::ReadRepairService::start_with_local(
            pool.rpc_client.clone(),
            NodeId(1),
            local.clone(),
            ClusterConfig::default().epoch,
        ),
    );
    let read = s4_cluster::QuorumReadCoordinator::new(
        NodeId(1),
        local,
        pool.rpc_client.clone(),
        pool.router.clone(),
        Arc::new(HybridClock::new(NodeId(1))),
        repair.clone(),
        &ClusterConfig::default(),
        s4_cluster::ReadTimeouts::default(),
    );
    // The three replicas answer only when the third is asked as well: a
    // read quorum of two may skip it, so each path is read until it is.
    let stale_answered = |key: &str| {
        let storage = pool.storages[2].clone();
        let key = key.to_string();
        async move {
            for _ in 0..100 {
                if storage.read().await.get_object("bucket", &key).await.unwrap().0
                    == b"newest body"
                {
                    return true;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            false
        }
    };
    assert_eq!(
        read.read("bucket", &keys[0]).await.unwrap().data,
        b"newest body"
    );
    assert!(
        stale_answered(&keys[0]).await,
        "GET did not repair the stale old owner"
    );
    let mut streamed = read.read_stream("bucket", &keys[1], Some((0, 5))).await.unwrap();
    let mut ranged = Vec::new();
    streamed.body.read_to_end(&mut ranged).await.unwrap();
    assert_eq!(ranged, b"newest");
    assert!(
        stale_answered(&keys[1]).await,
        "a streamed GET did not repair the stale old owner"
    );
    assert_eq!(read.head("bucket", &keys[2]).await.unwrap().size, 11);
    assert!(
        stale_answered(&keys[2]).await,
        "HEAD did not repair the stale old owner"
    );
    assert_eq!(
        repair.repairs_initiated(),
        3,
        "one repair per read: the stale old owner"
    );
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    for key in &keys {
        for storage in &pool.storages[3..] {
            assert!(
                storage.read().await.get_object("bucket", key).await.is_err(),
                "a read copied {key} to a new owner: that is the move's"
            );
        }
    }
}

fn disjoint_growth_key(pool: &Pool) -> String {
    (0..10_000)
        .map(|n| format!("native-growth-{n}"))
        .find(|key| {
            pool.router.route("bucket", key).unwrap().write_rule.requirements()[1]
                .nodes
                .iter()
                .all(|node| node.0 > 3)
        })
        .unwrap()
}

fn set_down(pool: &Pool, nodes: &[u128]) {
    for &id in nodes {
        let mut state = pool.gossip.get_node(&NodeId(id)).unwrap();
        state.status = NodeStatus::Dead;
        pool.gossip.set_member_for_testing(NodeId(id), state);
    }
}

/// Count durable framed records without depending on the private hint codec.
fn hint_records(pool: &Pool, target: u128) -> usize {
    let mut count = 0;
    let Ok(files) = std::fs::read_dir(pool.hints_dir.join(target.to_string())) else {
        return 0;
    };
    for file in files {
        let file = file.unwrap();
        let bytes = std::fs::read(file.path()).unwrap();
        let mut offset = 0;
        while offset < bytes.len() {
            assert!(offset + 8 <= bytes.len(), "incomplete hint header");
            let len = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 8 + len;
            assert!(offset <= bytes.len(), "incomplete hint body");
            count += 1;
        }
    }
    count
}

#[derive(Clone, Copy)]
enum NativeState {
    Session,
    Part,
    Completed,
    Aborted,
}

async fn assert_native_quorums(pool: &Pool, key: &str, upload: &str, state: NativeState) {
    for _ in 0..200 {
        let mut held = [0; 2];
        for (index, storage) in pool.storages.iter().enumerate() {
            if index > 0
                && pool.gossip.get_node(&NodeId(index as u128 + 1)).unwrap().status
                    != NodeStatus::Alive
            {
                continue;
            }
            let storage = storage.read().await;
            let matches = match state {
                NativeState::Session => storage.get_multipart_session(upload).await.is_ok(),
                NativeState::Part => storage
                    .list_multipart_parts(upload)
                    .unwrap()
                    .iter()
                    .any(|part| part.part_number == 1 && part.size == 12),
                NativeState::Completed => storage
                    .get_object("bucket", key)
                    .await
                    .is_ok_and(|(body, _)| body == b"native bytes"),
                NativeState::Aborted => {
                    storage.get_multipart_session(upload).await.is_err()
                        && storage.list_multipart_parts(upload).unwrap().is_empty()
                }
            };
            held[usize::from(index >= 3)] += usize::from(matches);
        }
        if held.iter().all(|count| *count >= 2) {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("native state absent from a generation quorum: upload={upload}");
}

async fn assert_generation_quorums_hold(pool: &Pool, bucket: &str, key: &str, live: bool) {
    let decision = pool.router.route(bucket, key).unwrap();
    for requirement in decision.write_rule.requirements() {
        let mut held = 0usize;
        for node in &requirement.nodes {
            let version = pool.storages[node.0 as usize - 1]
                .read()
                .await
                .key_version(bucket, key)
                .await
                .unwrap();
            held += usize::from(matches!(version, KeyVersion::Live(_)) == live);
        }
        assert!(
            held >= usize::from(requirement.count),
            "{bucket}/{key}: generation quorum needs {}, observed {held} with live={live}",
            requirement.count
        );
    }
}

/// Data, bucket metadata and cluster-control records all cross the same
/// old/new quorum boundary. The individual reserved key families are listed
/// explicitly so adding a special route cannot silently bypass the rule.
#[tokio::test]
async fn pool_growth_ring_mutations_land_on_both_generation_quorums() {
    let pool = pool_with_generation(31760, true, &[]).await;
    let cases = [
        ("bucket", "ordinary-object"),
        ("__system__", "__s4_bucket_marker_phase2"),
        ("__system__", "__s4_bucket_cors_phase2"),
        ("__system__", "__s4_bucket_lifecycle_phase2"),
        ("__system__", "__s4_bucket_policy_phase2"),
        ("__system__", "__s4_bucket_encryption_phase2"),
        ("__system__", "__s4_bucket_versioning_phase2"),
        ("__system__", "__s4_bucket_object_lock_phase2"),
        ("__system__", "__s4_ec_v1/tiering/phase2"),
        ("__system__", "__s4_ec_v1/pools/0/erasure-sets"),
        ("__system__", "__s4_ec_v1/pools/0/set-layouts/phase2"),
        ("__system__", "__s4_pool_generation_v1/0"),
        ("__system__", "__s4_iam_v2_user_phase2"),
    ];

    for (bucket, key) in cases {
        pool.coordinator
            .write(
                bucket,
                key,
                b"phase 2",
                "application/octet-stream",
                &HashMap::new(),
            )
            .await
            .unwrap();
        assert_generation_quorums_hold(&pool, bucket, key, true).await;

        pool.coordinator.delete(bucket, key).await.unwrap();
        assert_generation_quorums_hold(&pool, bucket, key, false).await;
    }
}

#[tokio::test]
async fn pool_growth_native_mutations_persist_on_both_quorums_and_hint_both_groups() {
    let pool = pool_with_generation(31300, true, &[]).await;
    let key = disjoint_growth_key(&pool);
    set_down(&pool, &[3, 6]);
    for upload in ["complete-growth", "abort-growth"] {
        pool.coordinator
            .create_multipart_upload(upload, "bucket", &key, "text/plain", &HashMap::new())
            .await
            .unwrap();
        assert_native_quorums(&pool, &key, upload, NativeState::Session).await;
        let part = pool
            .coordinator
            .upload_multipart_part("bucket", &key, upload, 1, b"native bytes".to_vec())
            .await
            .unwrap();
        assert_native_quorums(&pool, &key, upload, NativeState::Part).await;
        if upload == "complete-growth" {
            pool.coordinator
                .complete_multipart_upload("bucket", &key, upload, &[(1, part.etag)])
                .await
                .unwrap();
            assert_native_quorums(&pool, &key, upload, NativeState::Completed).await;
        } else {
            pool.coordinator.abort_multipart_upload("bucket", &key, upload).await.unwrap();
            assert_native_quorums(&pool, &key, upload, NativeState::Aborted).await;
        }
    }
    for _ in 0..200 {
        if [3, 6].iter().all(|target| hint_records(&pool, *target) >= 6) {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!(
        "native misses must leave durable hints in both generations: old={}, new={}",
        hint_records(&pool, 3),
        hint_records(&pool, 6)
    );
}

#[tokio::test]
async fn pool_growth_native_part_complete_abort_refuse_old_only_acknowledgements() {
    let pool = pool_with_generation(31320, true, &[]).await;
    let key = disjoint_growth_key(&pool);
    let mut completion = None;
    for upload in ["part-failure", "complete-failure", "abort-failure"] {
        pool.coordinator
            .create_multipart_upload(upload, "bucket", &key, "text/plain", &HashMap::new())
            .await
            .unwrap();
        assert_native_quorums(&pool, &key, upload, NativeState::Session).await;
        if upload != "part-failure" {
            let part = pool
                .coordinator
                .upload_multipart_part("bucket", &key, upload, 1, b"native bytes".to_vec())
                .await
                .unwrap();
            assert_native_quorums(&pool, &key, upload, NativeState::Part).await;
            if upload == "complete-failure" {
                completion = Some(part.etag);
            }
        }
    }
    set_down(&pool, &[4, 5, 6]);
    let part = pool
        .coordinator
        .upload_multipart_part("bucket", &key, "part-failure", 1, b"native bytes".to_vec())
        .await;
    let complete = pool
        .coordinator
        .complete_multipart_upload(
            "bucket",
            &key,
            "complete-failure",
            &[(1, completion.unwrap())],
        )
        .await;
    let abort = pool.coordinator.abort_multipart_upload("bucket", &key, "abort-failure").await;
    assert!(
        matches!(part, Err(s4_cluster::ClusterError::QuorumNotMet { .. })),
        "{part:?}"
    );
    assert!(
        matches!(complete, Err(s4_cluster::ClusterError::QuorumNotMet { .. })),
        "{complete:?}"
    );
    assert!(
        matches!(abort, Err(s4_cluster::ClusterError::QuorumNotMet { .. })),
        "{abort:?}"
    );
}

#[tokio::test]
async fn pool_growth_stale_coordinator_cannot_confirm_or_hint_fenced_mutations() {
    let pool = pool_with_generation(31740, true, &[]).await;
    let config = ClusterConfig {
        pools: vec![pool.router.pool_config(&PoolId(0)).unwrap().clone()],
        ..ClusterConfig::default()
    };
    let router = Arc::new(
        PlacementRouter::from_pool_node_ids(
            &config,
            NodeId(1),
            BucketPlacement::new(),
            &HashMap::from([(PoolId(0), (1..=3).map(NodeId).collect())]),
        )
        .unwrap(),
    );
    let local = Arc::new(SharedStorageAdapter::new(pool.storages[0].clone()));
    let stale = QuorumWriteCoordinator::new(
        NodeId(1),
        local.clone(),
        pool.rpc_client.clone(),
        router,
        Arc::new(HybridClock::new(NodeId(1))),
        pool.hints.clone(),
        &config,
        WriteTimeouts {
            quorum_timeout: std::time::Duration::from_secs(6),
            replica_write_timeout: std::time::Duration::from_secs(5),
        },
    )
    .with_multipart_storage(local);
    let written = stale
        .write(
            "bucket",
            "stale-object",
            b"stale",
            "text/plain",
            &HashMap::new(),
        )
        .await;
    let created = stale
        .create_multipart_upload(
            "stale-upload",
            "bucket",
            "stale-native",
            "text/plain",
            &HashMap::new(),
        )
        .await;
    assert!(written.is_err(), "generation0 must not reach W=2");
    assert!(created.is_err(), "generation0 multipart must not reach W=2");
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    for target in 2..=6 {
        assert_eq!(
            hint_records(&pool, target),
            0,
            "fencing is not a transient replication miss"
        );
    }
    for storage in &pool.storages[1..3] {
        assert!(storage.read().await.get_object("bucket", "stale-object").await.is_err());
        assert!(storage.read().await.get_multipart_session("stale-upload").await.is_err());
    }
}

mod rebalance;
