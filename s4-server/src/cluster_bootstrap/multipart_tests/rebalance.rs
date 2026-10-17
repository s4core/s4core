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

//! The move of a grown pool's keys (plan
//! ERASURE-CODING-FIXING-POOL-GROWTH-REBALANCE.md, phase 3) on the pool of
//! the parent module: nodes 1–3 are the old generation, 1–6 the new one,
//! each a real `NodeRpcServer` on Bitcask; the move runs on node 1, which
//! holds everything the old owners were given before the growth.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use s4_cluster::rebalance::{
    MemoryProgressStore, MultipartTransfer, ProgressStore, QuorumLoad, RebalanceConfig,
    RebalanceParts, RebalanceService,
};
use s4_cluster::{
    ClusterConfig, HintDelivery, HintedHandoffConfig, HintedHandoffManager, HlcTimestamp,
    HybridClock, IdempotencyTracker, LocalReplica, NodeCapacity, NodeId, PoolId,
    ReplicaReadHandler, ReplicaWriteHandler,
};
use s4_core::types::IndexRecord;
use s4_core::{KeyVersion, StorageEngine};
use tokio::sync::watch;

use super::{pool_with_generation, Pool, SharedStorageAdapter};

fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

fn hlc_metadata(wall: u64) -> HashMap<String, String> {
    HashMap::from([
        ("_s4_hlc_wall".to_string(), wall.to_string()),
        ("_s4_hlc_logical".to_string(), "0".to_string()),
        ("_s4_hlc_node_id".to_string(), "7".to_string()),
    ])
}

/// Every node but node 1 reports `used` of 1 000 000 bytes taken.
fn report_disks(pool: &Pool, used: &[(u128, u64)]) {
    for n in 2..=6u128 {
        let mut state = pool.gossip.get_node(&NodeId(n)).unwrap();
        let taken = used.iter().find(|(node, _)| *node == n).map_or(100_000, |(_, used)| *used);
        state.meta.capacity = NodeCapacity {
            total_bytes: 1_000_000,
            used_bytes: taken,
            volume_count: 0,
            object_count: 0,
        };
        pool.gossip.set_member_for_testing(NodeId(n), state);
    }
}

fn config(page_size: usize) -> RebalanceConfig {
    RebalanceConfig {
        page_size,
        retry_after: Duration::from_millis(100),
        settle: Duration::ZERO,
        ..RebalanceConfig::default()
    }
}

/// The move of node 1 of `pool`, keeping its progress in `store`, every
/// member having announced the move.
fn service(
    pool: &Pool,
    store: Arc<dyn ProgressStore>,
    config: RebalanceConfig,
) -> RebalanceService {
    let record = s4_cluster::pool_generation::PoolGenerationRecord::initial(
        PoolId::new(0),
        (1..=3).map(NodeId).collect(),
    )
    .unwrap()
    .configure((1..=6).map(NodeId).collect())
    .unwrap()
    .begin()
    .unwrap();
    for n in 2..=6 {
        pool.gossip.set_pool_generation_for_testing(
            NodeId(n),
            Some(s4_cluster::pool_generation::GenerationAnnouncement::of(
                &record, false,
            )),
        );
    }
    service_with(pool, store, config)
}

/// The move of node 1 of `pool`, keeping its progress in `store`, with the
/// members' announcements as gossip holds them.
fn service_with(
    pool: &Pool,
    store: Arc<dyn ProgressStore>,
    config: RebalanceConfig,
) -> RebalanceService {
    let epoch = ClusterConfig::default().epoch;
    let local = Arc::new(SharedStorageAdapter::new(pool.storages[0].clone()));
    let source = s4_cluster::QuorumReadCoordinator::new(
        NodeId(1),
        local.clone(),
        pool.rpc_client.clone(),
        pool.router.clone(),
        Arc::new(HybridClock::new(NodeId(1))),
        Arc::new(s4_cluster::ReadRepairService::start_with_local(
            pool.rpc_client.clone(),
            NodeId(1),
            local.clone(),
            epoch,
        )),
        &ClusterConfig::default(),
        s4_cluster::ReadTimeouts::default(),
    );
    let record = s4_cluster::pool_generation::PoolGenerationRecord::initial(
        PoolId::new(0),
        (1..=3).map(NodeId).collect(),
    )
    .unwrap()
    .configure((1..=6).map(NodeId).collect())
    .unwrap()
    .begin()
    .unwrap();
    RebalanceService::new(RebalanceParts {
        record,
        local: NodeId(1),
        topology_epoch: epoch,
        router: pool.router.clone(),
        ranges: pool.router.generation_ranges(&PoolId::new(0)).unwrap().clone(),
        storage: local.clone(),
        source: Arc::new(source),
        local_replica: LocalReplica {
            write: Arc::new(
                ReplicaWriteHandler::new(local.clone(), Arc::new(IdempotencyTracker::new()), epoch)
                    .with_placement_generation(2),
            ),
            read: Arc::new(ReplicaReadHandler::new(local.clone(), epoch)),
        },
        multipart: Some(transfer(pool)),
        rpc_client: pool.rpc_client.clone(),
        gossip: pool.gossip.clone(),
        store,
        load: Arc::new(QuorumLoad),
        config,
        gc_grace: Duration::from_secs(86_400),
    })
    .unwrap()
}

fn transfer(pool: &Pool) -> MultipartTransfer {
    MultipartTransfer::new(
        Arc::new(SharedStorageAdapter::new(pool.storages[0].clone())),
        pool.rpc_client.clone(),
        pool.router.clone(),
        NodeId(1),
        ClusterConfig::default().epoch,
    )
}

async fn run_to_end(service: RebalanceService) {
    let (_stop, shutdown) = watch::channel(false);
    tokio::time::timeout(Duration::from_secs(120), service.run(shutdown))
        .await
        .expect("the move did not end");
}

/// The new owners of `bucket/key`: the second requirement of its joint rule.
fn new_owners(pool: &Pool, bucket: &str, key: &str) -> Vec<NodeId> {
    let decision = pool.router.route(bucket, key).unwrap();
    decision.write_rule.requirements()[1].nodes.clone()
}

async fn version(pool: &Pool, node: NodeId, bucket: &str, key: &str) -> KeyVersion {
    pool.storages[node.0 as usize - 1]
        .read()
        .await
        .key_version(bucket, key)
        .await
        .unwrap()
}

/// Puts `bucket/key` on the three old owners at HLC `wall`.
async fn seed(pool: &Pool, bucket: &str, key: &str, body: &[u8], wall: u64) {
    for storage in &pool.storages[..3] {
        storage
            .read()
            .await
            .put_object(bucket, key, body, "text/plain", &hlc_metadata(wall))
            .await
            .unwrap();
    }
}

fn ec_record(wall: u64) -> IndexRecord {
    let mut record = IndexRecord::new(
        u32::MAX,
        0,
        9,
        [7; 32],
        "erasure-coded".into(),
        "text/plain".into(),
    );
    record.hlc_timestamp = wall;
    record.origin_node_id = 7;
    record.into_erasure_coded(11, 1)
}

/// Every family of the plan's table that the ring places reaches the new
/// owners of its key as the old owners hold it: objects, a tombstone,
/// bucket metadata, an IAM record of version 2, the records of `__system__`
/// without digests and an erasure-coded object's namespace entry. Each arc
/// moved to `Streaming`, and the move reports what it carried.
#[tokio::test]
async fn pool_growth_move_carries_every_family_to_the_new_owners() {
    let pool = pool_with_generation(31420, true, &[]).await;
    report_disks(&pool, &[]);
    let objects: Vec<String> = (0..40).map(|n| format!("object-{n}")).collect();
    for key in &objects {
        seed(&pool, "bucket", key, key.as_bytes(), 100).await;
    }
    let deleted_at = now_millis();
    seed(&pool, "bucket", "deleted", b"before the delete", 100).await;
    for storage in &pool.storages[..3] {
        storage
            .read()
            .await
            .tombstone_object("bucket", "deleted", deleted_at, 0, 7)
            .await
            .unwrap();
    }
    let system = [
        "__s4_bucket_marker_photos",
        "__s4_bucket_cors_photos",
        "__s4_iam_v2_user_9",
        "__s4_ec_v1/tiering/photos",
        "__s4_ec_v1/pools/0/erasure-sets",
        "__s4_pool_generation_v1/0",
    ];
    for key in system {
        seed(&pool, "__system__", key, key.as_bytes(), 100).await;
    }
    for storage in &pool.storages[..3] {
        storage
            .read()
            .await
            .restore_erasure_coded_entry("bucket", "erasure-coded", ec_record(100))
            .await
            .unwrap();
    }

    let store: Arc<dyn ProgressStore> = Arc::new(MemoryProgressStore::default());
    let service = service(&pool, store, config(16));
    let handle = service.handle();
    run_to_end(service).await;

    for key in &objects {
        for node in new_owners(&pool, "bucket", key) {
            match version(&pool, node, "bucket", key).await {
                KeyVersion::Live(record) => {
                    assert_eq!(record.hlc_timestamp, 100, "{key} on {node}")
                }
                other => panic!("{key} on {node}: {other:?}"),
            }
            let (body, _) = pool.storages[node.0 as usize - 1]
                .read()
                .await
                .get_object("bucket", key)
                .await
                .unwrap();
            assert_eq!(body, key.as_bytes());
        }
    }
    for node in new_owners(&pool, "bucket", "deleted") {
        assert!(
            matches!(
                version(&pool, node, "bucket", "deleted").await,
                KeyVersion::Tombstone(_)
            ),
            "the tombstone did not reach {node}"
        );
    }
    for key in system {
        for node in new_owners(&pool, "__system__", key) {
            assert!(
                matches!(
                    version(&pool, node, "__system__", key).await,
                    KeyVersion::Live(_)
                ),
                "{key} did not reach {node}"
            );
        }
    }
    for node in new_owners(&pool, "bucket", "erasure-coded") {
        match version(&pool, node, "bucket", "erasure-coded").await {
            KeyVersion::Live(record) => assert!(record.is_erasure_coded(), "{node}"),
            other => panic!("the namespace entry did not reach {node}: {other:?}"),
        }
    }
    let ranges = pool.router.generation_ranges(&PoolId::new(0)).unwrap().snapshot();
    assert!(ranges
        .iter()
        .all(|range| range.state == s4_cluster::placement::RangeState::Streaming));
    let status = handle.status();
    assert_eq!(status.items_done, status.items_total, "{status:?}");
    assert_eq!(status.ranges_done, ranges.len());
    assert!(
        status.keys_moved > 0 && status.bytes_moved > 0,
        "{status:?}"
    );
    assert!(status.errors.is_empty(), "{status:?}");
}

/// A move stopped part way continues from its cursors: the keys it read
/// before the stop are not read again, so the two runs together read every
/// key exactly once. A pause is kept across the restart, and nothing moves
/// until the move is resumed.
#[tokio::test]
async fn pool_growth_move_continues_from_its_cursor_and_keeps_a_pause() {
    let pool = pool_with_generation(31360, true, &[]).await;
    report_disks(&pool, &[]);
    for n in 0..60 {
        let key = format!("cursor-{n}");
        seed(&pool, "bucket", &key, key.as_bytes(), 100).await;
    }
    let digests = pool.storages[0].read().await.index_db().version_digests().await.unwrap().len();
    // The objects, and none of the records of `__system__` without digests.
    let expected = digests as u64;
    let store: Arc<dyn ProgressStore> = Arc::new(MemoryProgressStore::default());

    let first = service(
        &pool,
        store.clone(),
        RebalanceConfig {
            max_keys_per_sec: 20,
            ..config(4)
        },
    );
    let handle = first.handle();
    let (stop, shutdown) = watch::channel(false);
    let running = tokio::spawn(first.run(shutdown));
    for _ in 0..500 {
        if handle.status().keys_scanned >= 8 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    stop.send(true).unwrap();
    running.await.unwrap();
    let read_before = handle.status().keys_scanned;
    assert!(
        read_before >= 8 && read_before < expected,
        "{read_before} of {expected}"
    );

    handle.pause("operator").await.unwrap();
    let second = service(&pool, store.clone(), config(4));
    second.restore().await.unwrap();
    let handle = second.handle();
    assert_eq!(handle.status().paused.as_deref(), Some("operator"));
    assert_eq!(handle.status().keys_scanned, read_before);
    let (_stop, shutdown) = watch::channel(false);
    let running = tokio::spawn(second.run(shutdown));
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        handle.status().keys_scanned,
        read_before,
        "a paused move read keys"
    );
    handle.resume().await.unwrap();
    tokio::time::timeout(Duration::from_secs(120), running).await.unwrap().unwrap();
    let status = handle.status();
    assert_eq!(status.keys_scanned, expected, "{status:?}");
    assert_eq!(status.items_done, status.items_total);
    assert_eq!(status.paused, None);
}

/// A receiver whose free share is below the ratio the move keeps is sent
/// nothing: the ranges sending to it wait with the reason named, and the
/// others go on. Once it has room again, they finish.
#[tokio::test]
async fn pool_growth_move_waits_for_room_on_a_full_receiver() {
    let pool = pool_with_generation(31380, true, &[]).await;
    report_disks(&pool, &[(4, 960_000)]);
    let keys: Vec<String> = (0..30).map(|n| format!("room-{n}")).collect();
    for key in &keys {
        seed(&pool, "bucket", key, key.as_bytes(), 100).await;
    }
    let store: Arc<dyn ProgressStore> = Arc::new(MemoryProgressStore::default());
    let service = service(&pool, store, config(16));
    let handle = service.handle();
    let (_stop, shutdown) = watch::channel(false);
    let running = tokio::spawn(service.run(shutdown));
    let mut reason = None;
    for _ in 0..500 {
        reason = handle.status().waiting.values().find(|why| why.contains("below")).cloned();
        if reason.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let reason = reason.expect("no range waits for the full receiver");
    assert!(reason.contains(&NodeId(4).to_string()), "{reason}");
    for key in keys.iter().filter(|key| new_owners(&pool, "bucket", key).contains(&NodeId(4))) {
        assert!(
            matches!(
                version(&pool, NodeId(4), "bucket", key).await,
                KeyVersion::Absent
            ),
            "{key} reached the full receiver"
        );
    }
    report_disks(&pool, &[]);
    tokio::time::timeout(Duration::from_secs(120), running).await.unwrap().unwrap();
    for key in &keys {
        for node in new_owners(&pool, "bucket", key) {
            assert!(matches!(
                version(&pool, node, "bucket", key).await,
                KeyVersion::Live(_)
            ));
        }
    }
    assert!(handle.status().waiting.is_empty());
}

const PART: usize = 5 * 1024 * 1024;

/// Opens `upload` of `key` on the three old owners with one part of five
/// MiB, as a client did before the growth; the part's ETag.
async fn open_before_growth(pool: &Pool, key: &str, upload: &str) -> String {
    let body = vec![b'p'; PART];
    let mut etag = String::new();
    for storage in &pool.storages[..3] {
        let storage = storage.read().await;
        storage
            .create_multipart_session(upload, "bucket", key, "text/plain", &HashMap::new())
            .await
            .unwrap();
        etag = storage
            .upload_part_streaming(
                upload,
                1,
                Box::new(std::io::Cursor::new(body.clone())),
                PART as u64,
            )
            .await
            .unwrap()
            .etag;
    }
    etag
}

/// An upload begun before the growth cannot take a part through the joint
/// quorum while the new owners lack it and the coordinator cannot hand it
/// over (503, not 404); once the move handed it over, its
/// next part and its completion go through, and the object reads back from
/// the new owners. An upload handed over and then aborted is gone from both
/// generations. A multipart hint is delivered by handing the upload over.
#[tokio::test]
async fn pool_growth_upload_begun_before_the_growth_completes_and_aborts_through_it() {
    let pool = pool_with_generation(31400, true, &[]).await;
    report_disks(&pool, &[]);
    let key = super::disjoint_growth_key(&pool);
    let first_etag = open_before_growth(&pool, &key, "upload-before").await;
    let aborted_key = format!("{key}-aborted");
    open_before_growth(&pool, &aborted_key, "upload-aborted").await;

    // A coordinator that cannot hand the upload over: the part fails, and
    // the client is told to try again — not that the upload does not exist.
    let early = pool
        .coordinator
        .upload_multipart_part("bucket", &key, "upload-before", 2, b"tail".to_vec())
        .await;
    assert!(
        matches!(early, Err(s4_cluster::ClusterError::QuorumNotMet { .. })),
        "the new owners took a part of an upload they lack, or it was not found: {early:?}"
    );

    let store: Arc<dyn ProgressStore> = Arc::new(MemoryProgressStore::default());
    run_to_end(service(&pool, store, config(16))).await;

    let second = pool
        .coordinator
        .upload_multipart_part("bucket", &key, "upload-before", 2, b"tail".to_vec())
        .await
        .unwrap();
    pool.coordinator
        .complete_multipart_upload(
            "bucket",
            &key,
            "upload-before",
            &[(1, first_etag), (2, second.etag)],
        )
        .await
        .unwrap();
    let mut expected = vec![b'p'; PART];
    expected.extend_from_slice(b"tail");
    for node in new_owners(&pool, "bucket", &key) {
        let (body, _) = pool.storages[node.0 as usize - 1]
            .read()
            .await
            .get_object("bucket", &key)
            .await
            .unwrap();
        assert!(body == expected, "the completed object differs on {node}");
    }

    pool.coordinator
        .abort_multipart_upload("bucket", &aborted_key, "upload-aborted")
        .await
        .unwrap();
    for (index, storage) in pool.storages.iter().enumerate() {
        let storage = storage.read().await;
        assert!(
            storage.get_multipart_session("upload-aborted").await.is_err()
                && storage.list_multipart_parts("upload-aborted").unwrap().is_empty(),
            "the aborted upload stayed on node {}",
            index + 1
        );
    }

    let hinted_key = format!("{key}-hinted");
    open_before_growth(&pool, &hinted_key, "upload-hinted").await;
    let target = new_owners(&pool, "bucket", &hinted_key)[0];
    let dir = tempfile::tempdir().unwrap();
    let manager = HintedHandoffManager::new(
        HintedHandoffConfig::new(dir.path()),
        pool.rpc_client.clone(),
    );
    manager
        .store(
            target,
            s4_cluster::hints::HintEntry::multipart(
                "bucket",
                &hinted_key,
                "upload-hinted",
                HlcTimestamp::from_record(now_millis(), 0, 1),
            ),
        )
        .await;
    let delivery = HintDelivery::new(
        Arc::new(s4_cluster::QuorumReadCoordinator::new(
            NodeId(1),
            Arc::new(SharedStorageAdapter::new(pool.storages[0].clone())),
            pool.rpc_client.clone(),
            pool.router.clone(),
            Arc::new(HybridClock::new(NodeId(1))),
            Arc::new(s4_cluster::ReadRepairService::start(
                pool.rpc_client.clone(),
            )),
            &ClusterConfig::default(),
            s4_cluster::ReadTimeouts::default(),
        )),
        pool.rpc_client.clone(),
        pool.gossip.clone(),
        NodeId(1),
        ClusterConfig::default().epoch,
    )
    .with_multipart_transfer(Arc::new(transfer(&pool)));
    manager.deliver(&delivery).await;
    let storage = pool.storages[target.0 as usize - 1].read().await;
    assert!(storage.get_multipart_session("upload-hinted").await.is_ok());
    assert_eq!(
        storage.list_multipart_parts("upload-hinted").unwrap().len(),
        1
    );
}

/// A multipart hint held by a node with no copy of its upload — a
/// coordinator that is not a replica of the key — comes to what the replicas
/// hold: an upload fewer than a write quorum of them hold open was completed
/// or aborted — one replica that missed that keeps a stale session — and its
/// hint goes; one the target holds needs nothing; one a write quorum holds
/// open waits for them to hand it over. A hint that waited for a
/// completed upload kept the ranges of its target from being verified, and a
/// growing pool from settling, until it expired (scenario 61, group A).
#[tokio::test]
async fn pool_growth_multipart_hint_of_an_upload_no_replica_holds_open_goes() {
    let pool = pool_with_generation(31440, true, &[]).await;
    report_disks(&pool, &[]);
    let key = super::disjoint_growth_key(&pool);
    let target = new_owners(&pool, "bucket", &key)[0];
    let other = new_owners(&pool, "bucket", &key)[1];
    let hlc = || HlcTimestamp::from_record(now_millis(), 0, 1);
    let delivery = || {
        HintDelivery::new(
            Arc::new(s4_cluster::QuorumReadCoordinator::new(
                NodeId(1),
                Arc::new(SharedStorageAdapter::new(pool.storages[0].clone())),
                pool.rpc_client.clone(),
                pool.router.clone(),
                Arc::new(HybridClock::new(NodeId(1))),
                Arc::new(s4_cluster::ReadRepairService::start(
                    pool.rpc_client.clone(),
                )),
                &ClusterConfig::default(),
                s4_cluster::ReadTimeouts::default(),
            )),
            pool.rpc_client.clone(),
            pool.gossip.clone(),
            NodeId(1),
            ClusterConfig::default().epoch,
        )
        .with_multipart_transfer(Arc::new(transfer(&pool)))
    };
    let open_on = |node: NodeId, upload: &'static str| {
        let storage = pool.storages[node.0 as usize - 1].clone();
        let key = key.clone();
        async move {
            storage
                .read()
                .await
                .create_multipart_session(upload, "bucket", &key, "text/plain", &HashMap::new())
                .await
                .unwrap();
        }
    };
    let dir = tempfile::tempdir().unwrap();
    let manager = HintedHandoffManager::new(
        HintedHandoffConfig::new(dir.path()),
        pool.rpc_client.clone(),
    );
    let hint =
        |upload: &str| s4_cluster::hints::HintEntry::multipart("bucket", &key, upload, hlc());

    // Completed or aborted everywhere: no replica holds it.
    manager.store(target, hint("upload-gone")).await;
    manager.deliver(&delivery()).await;
    assert!(
        manager.targets_with_hints().await.is_empty(),
        "the hint of an upload no replica holds is still kept"
    );

    // Open on one replica only, which missed the completion: a stale session,
    // and the hint goes.
    open_on(other, "upload-stale").await;
    manager.store(target, hint("upload-stale")).await;
    manager.deliver(&delivery()).await;
    assert!(
        manager.targets_with_hints().await.is_empty(),
        "a session one replica kept after the completion holds the hint"
    );

    // Open on a write quorum of the old owners: the hint waits for them to
    // hand it over.
    open_on(NodeId(2), "upload-elsewhere").await;
    open_on(NodeId(3), "upload-elsewhere").await;
    manager.store(target, hint("upload-elsewhere")).await;
    manager.deliver(&delivery()).await;
    assert_eq!(manager.targets_with_hints().await, vec![target]);

    // Once the target holds it, the hint is done.
    open_on(target, "upload-elsewhere").await;
    manager.deliver(&delivery()).await;
    assert!(manager.targets_with_hints().await.is_empty());
    assert!(pool.storages[0]
        .read()
        .await
        .get_multipart_session("upload-elsewhere")
        .await
        .is_err());
}

/// A coordinator that writes a part of an upload begun before the growth,
/// or completes it, first hands it to the new owners that lack it — session
/// and parts, from a replica holding it open — so neither waits for the move
/// nor fails with `NoSuchUpload`. The object reads back from the new owners.
/// An upload that was aborted is not handed over: the operation answers
/// `NoSuchUpload` as it would.
#[tokio::test]
async fn pool_growth_coordinator_hands_an_upload_over_before_its_part_and_completion() {
    let pool = pool_with_generation(31460, true, &[]).await;
    let key = super::disjoint_growth_key(&pool);
    let first_etag = open_before_growth(&pool, &key, "upload-early").await;
    let coordinator = s4_cluster::QuorumWriteCoordinator::new(
        NodeId(1),
        Arc::new(SharedStorageAdapter::new(pool.storages[0].clone())),
        pool.rpc_client.clone(),
        pool.router.clone(),
        Arc::new(HybridClock::new(NodeId(1))),
        pool.hints.clone(),
        &pool.config,
        s4_cluster::WriteTimeouts::default(),
    )
    .with_multipart_storage(Arc::new(SharedStorageAdapter::new(
        pool.storages[0].clone(),
    )))
    .with_upload_transfer(Arc::new(transfer(&pool)));

    let second = coordinator
        .upload_multipart_part("bucket", &key, "upload-early", 2, b"tail".to_vec())
        .await
        .expect("a part of an upload begun before the growth");
    for node in new_owners(&pool, "bucket", &key) {
        let storage = pool.storages[node.0 as usize - 1].read().await;
        assert_eq!(
            storage.list_multipart_parts("upload-early").unwrap().len(),
            2,
            "node {node} did not get the upload with both parts"
        );
    }
    coordinator
        .complete_multipart_upload(
            "bucket",
            &key,
            "upload-early",
            &[(1, first_etag), (2, second.etag)],
        )
        .await
        .expect("the completion of an upload begun before the growth");
    let mut expected = vec![b'p'; PART];
    expected.extend_from_slice(b"tail");
    for node in new_owners(&pool, "bucket", &key) {
        let (body, _) = pool.storages[node.0 as usize - 1]
            .read()
            .await
            .get_object("bucket", &key)
            .await
            .unwrap();
        assert!(body == expected, "the completed object differs on {node}");
    }

    // Aborted on the old owners: nothing to hand over, and the part is told
    // the upload does not exist.
    let gone_key = format!("{key}-gone");
    open_before_growth(&pool, &gone_key, "upload-gone").await;
    for storage in &pool.storages[..3] {
        storage.read().await.abort_multipart_native("upload-gone").await.unwrap();
    }
    let gone = coordinator
        .upload_multipart_part("bucket", &gone_key, "upload-gone", 2, b"tail".to_vec())
        .await;
    assert!(
        matches!(
            gone,
            Err(s4_cluster::ClusterError::MultipartUploadNotFound { .. })
        ),
        "{gone:?}"
    );
}

/// A session a missed abort left on one replica is stale: the multipart
/// hint for that replica aborts it there instead of counting it delivered,
/// and the move does not hand it to the new owners.
#[tokio::test]
async fn pool_growth_a_stale_session_is_aborted_and_not_handed_over() {
    let pool = pool_with_generation(31480, true, &[]).await;
    report_disks(&pool, &[]);
    let key = super::disjoint_growth_key(&pool);
    open_before_growth(&pool, &key, "upload-stale").await;
    // The abort reached nodes 2 and 3 and missed node 1.
    for storage in &pool.storages[1..3] {
        storage.read().await.abort_multipart_native("upload-stale").await.unwrap();
    }
    let transfer = transfer(&pool);
    assert_eq!(
        transfer.standing("bucket", &key, "upload-stale").await,
        s4_cluster::rebalance::UploadStanding::Closed {
            stale: vec![NodeId(1)]
        }
    );
    let session = transfer.local_session("upload-stale").await.unwrap();
    assert_eq!(
        transfer.hand_over(&session, None).await,
        s4_cluster::rebalance::UploadHandedOver::Gone
    );
    for node in new_owners(&pool, "bucket", &key) {
        let storage = pool.storages[node.0 as usize - 1].read().await;
        assert!(
            storage.get_multipart_session("upload-stale").await.is_err(),
            "the stale session reached new owner {node}"
        );
    }

    // Another upload whose abort missed node 2: the hint for node 2 is
    // delivered by aborting the stale session there.
    let hinted_key = format!("{key}-hinted");
    open_before_growth(&pool, &hinted_key, "upload-hinted-stale").await;
    for index in [0, 2] {
        pool.storages[index]
            .read()
            .await
            .abort_multipart_native("upload-hinted-stale")
            .await
            .unwrap();
    }
    let dir = tempfile::tempdir().unwrap();
    let manager = HintedHandoffManager::new(
        HintedHandoffConfig::new(dir.path()),
        pool.rpc_client.clone(),
    );
    manager
        .store(
            NodeId(2),
            s4_cluster::hints::HintEntry::multipart(
                "bucket",
                &hinted_key,
                "upload-hinted-stale",
                HlcTimestamp::from_record(now_millis(), 0, 1),
            ),
        )
        .await;
    let delivery = HintDelivery::new(
        Arc::new(s4_cluster::QuorumReadCoordinator::new(
            NodeId(1),
            Arc::new(SharedStorageAdapter::new(pool.storages[0].clone())),
            pool.rpc_client.clone(),
            pool.router.clone(),
            Arc::new(HybridClock::new(NodeId(1))),
            Arc::new(s4_cluster::ReadRepairService::start(
                pool.rpc_client.clone(),
            )),
            &ClusterConfig::default(),
            s4_cluster::ReadTimeouts::default(),
        )),
        pool.rpc_client.clone(),
        pool.gossip.clone(),
        NodeId(1),
        ClusterConfig::default().epoch,
    )
    .with_multipart_transfer(Arc::new(transfer));
    manager.deliver(&delivery).await;
    assert!(
        pool.storages[1]
            .read()
            .await
            .get_multipart_session("upload-hinted-stale")
            .await
            .is_err(),
        "the stale session stayed on the replica the abort missed"
    );
    assert!(manager.targets_with_hints().await.is_empty());
}

/// The records of `__system__` without digests and the open uploads are
/// handed over once, and no verification compares them before their ranges
/// cut over: while a member has not announced the move, it may still write
/// one by the earlier generation, to the old owners alone, after this node's
/// pass. So they wait for every member; the arcs do not.
#[tokio::test]
async fn pool_growth_records_without_digests_wait_for_every_member_to_begin() {
    let pool = pool_with_generation(31500, true, &[]).await;
    report_disks(&pool, &[]);
    let store: Arc<dyn ProgressStore> = Arc::new(MemoryProgressStore::default());
    let service = service(&pool, store.clone(), config(16));
    // Node 6 has not begun the move yet.
    pool.gossip.set_pool_generation_for_testing(NodeId(6), None);
    let handle = service.handle();
    let (stop, shutdown) = watch::channel(false);
    let running = tokio::spawn(service.run(shutdown));
    let mut status = handle.status();
    for _ in 0..200 {
        status = handle.status();
        if status.items_done + 2 == status.items_total
            && status.waiting.values().filter(|why| why.contains("node")).count() == 2
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        status.items_done + 2,
        status.items_total,
        "only the two items without digests wait: {status:?}"
    );
    assert!(
        status.waiting.values().all(|why| why.contains("to begin the move")),
        "{status:?}"
    );
    let _ = stop.send(true);
    let _ = running.await;

    let record = s4_cluster::pool_generation::PoolGenerationRecord::initial(
        PoolId::new(0),
        (1..=3).map(NodeId).collect(),
    )
    .unwrap()
    .configure((1..=6).map(NodeId).collect())
    .unwrap()
    .begin()
    .unwrap();
    pool.gossip.set_pool_generation_for_testing(
        NodeId(6),
        Some(s4_cluster::pool_generation::GenerationAnnouncement::of(
            &record, false,
        )),
    );
    let again = service_with(&pool, store, config(16));
    let handle = again.handle();
    run_to_end(again).await;
    let status = handle.status();
    assert_eq!(status.items_done, status.items_total, "{status:?}");
}
