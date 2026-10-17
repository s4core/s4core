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

use super::*;
use crate::coordinator::test_pool::{Replica, TestPool, BUCKET};
use crate::protocol::{PROTOCOL_VERSION, S4_VERSION};
use s4_core::{ObjectStream, StorageEngine, StorageError};
use std::collections::HashMap;

struct PoolStore {
    storage: Arc<BitcaskStorageEngine>,
    completion_scans: AtomicU64,
}

#[async_trait::async_trait]
impl MigrationStore for PoolStore {
    fn index(&self) -> Arc<IndexDb> {
        self.storage.index_db().clone()
    }
    async fn open(
        &self,
        bucket: &str,
        key: &str,
        expected: &IndexRecord,
    ) -> Result<ObjectStream, StorageError> {
        self.storage.open_legacy_migration_stream(bucket, key, expected).await
    }
    async fn finalize(
        &self,
        full: &str,
        original: &IndexRecord,
        observed: Option<&IndexRecord>,
        keep: bool,
    ) -> Result<bool, StorageError> {
        finalize_store(&self.storage, full, original, observed, keep).await
    }
    async fn complete(&self) -> Result<bool, StorageError> {
        self.completion_scans.fetch_add(1, Ordering::Relaxed);
        complete_store(&self.storage).await
    }
}

fn worker(pool: &TestPool) -> MigrationWorker {
    for node in [2, 3] {
        let mut state = pool.gossip.get_node(&NodeId(node)).unwrap();
        state.meta.s4_version = S4_VERSION.into();
        pool.gossip.set_member_for_testing(NodeId(node), state);
    }
    MigrationWorker::new(MigrationParts {
        storage: Arc::new(PoolStore {
            storage: pool.storage(1).clone(),
            completion_scans: AtomicU64::new(0),
        }),
        router: pool.router(),
        rpc: pool.rpc_client.clone(),
        gossip: pool.gossip.clone(),
        reconciler: pool.reconciler(),
        local: NodeId(1),
        members: vec![NodeId(1), NodeId(2), NodeId(3)],
        epoch: pool.epoch(),
        concurrency: 2,
    })
    .unwrap()
}

#[tokio::test]
async fn a_trusted_completion_skips_the_final_index_scan_on_startup() {
    for (trusted, port) in [(false, 15960), (true, 15970)] {
        let pool = TestPool::start(Replica::Up, Replica::Up, port).await;
        let index = pool.storage(1).index_db();
        if trusted {
            index.mark_legacy_migration_complete().await.unwrap();
        }
        let store = Arc::new(PoolStore {
            storage: pool.storage(1).clone(),
            completion_scans: AtomicU64::new(0),
        });
        let mut migration = worker(&pool);
        migration.parts.storage = store.clone();
        migration.initialize().await.unwrap();
        assert_eq!(migration.status().pending(), 0);
        let (_sender, shutdown) = watch::channel(false);
        migration.run(shutdown).await;
        assert_eq!(
            store.completion_scans.load(Ordering::Relaxed),
            u64::from(!trusted),
            "a trusted completion must not repeat the full index scan"
        );
        assert!(index.legacy_migration_complete().await.unwrap());
    }
}

#[tokio::test]
async fn pending_is_counted_before_the_gate_and_copy_keeps_the_original_generation() {
    let pool = TestPool::start(Replica::Up, Replica::Up, 24900).await;
    pool.storage(1)
        .put_object(
            "__system__",
            "__s4_bucket_marker_bucket",
            b"1",
            "application/octet-stream",
            &HashMap::new(),
        )
        .await
        .unwrap();
    let metadata = HashMap::from([
        ("owner".into(), "original".into()),
        ("_s4_storage_class".into(), "STANDARD_IA".into()),
    ]);
    pool.storage(1)
        .put_object(BUCKET, "legacy", b"single bytes", "text/custom", &metadata)
        .await
        .unwrap();
    let original = pool.storage(1).head_object(BUCKET, "legacy").await.unwrap();
    let migration = worker(&pool);
    migration.initialize().await.unwrap();
    assert_eq!(migration.status().pending(), 2);
    assert_eq!(migration.status().migrated(), 0);
    assert!(migration.start_gate().is_ok());
    let (_, shutdown) = watch::channel(false);
    assert!(migration.pass(&shutdown).await.unwrap());
    assert_eq!(migration.status().pending(), 0);
    assert_eq!(migration.status().migrated(), 2);
    for node in 1..=3 {
        let (bytes, record) = pool.storage(node).get_object(BUCKET, "legacy").await.unwrap();
        assert_eq!(bytes, b"single bytes");
        assert_eq!(record.etag, original.etag);
        assert_eq!(record.content_type, original.content_type);
        assert_eq!(
            record.metadata.get("_s4_storage_class"),
            original.metadata.get("_s4_storage_class")
        );
        assert_eq!(record.metadata.get("owner"), Some(&"original".to_string()));
        assert_eq!(
            HlcTimestamp::of_record(&record),
            HlcTimestamp::of_record(&original)
        );
        assert_eq!(record.origin_node_id, 0);
    }
    let restarted = worker(&pool);
    restarted.initialize().await.unwrap();
    assert_eq!(restarted.status().pending(), 0);
    assert_eq!(restarted.status().migrated(), 0);
    assert!(restarted.pass(&shutdown).await.unwrap());
}

#[tokio::test]
async fn a_missing_owner_keeps_the_source_unstamped_and_restart_resumes_it() {
    let pool = TestPool::start(Replica::Up, Replica::Up, 24910).await;
    pool.storage(1)
        .put_object(
            BUCKET,
            "resume",
            b"single bytes",
            "text/plain",
            &HashMap::new(),
        )
        .await
        .unwrap();
    let original = pool.storage(1).head_object(BUCKET, "resume").await.unwrap();
    let migration = worker(&pool);
    migration.initialize().await.unwrap();
    let mut state = pool.gossip.get_node(&NodeId(3)).unwrap();
    state.status = NodeStatus::Dead;
    pool.gossip.set_member_for_testing(NodeId(3), state.clone());
    let (_, shutdown) = watch::channel(false);
    assert!(!migration.pass(&shutdown).await.unwrap());
    assert_eq!(migration.status().pending(), 1);
    assert_eq!(
        pool.storage(1).head_object(BUCKET, "resume").await.unwrap(),
        original
    );
    let restarted = worker(&pool);
    restarted.initialize().await.unwrap();
    assert_eq!(restarted.status().pending(), 1);
    state.status = NodeStatus::Alive;
    pool.gossip.set_member_for_testing(NodeId(3), state);
    assert!(restarted.pass(&shutdown).await.unwrap());
    assert_eq!(restarted.status().migrated(), 1);
}

#[tokio::test]
async fn the_start_gate_rejects_unknown_mixed_and_dead_members() {
    let pool = TestPool::start(Replica::Up, Replica::Up, 24920).await;
    let migration = worker(&pool);
    assert!(migration.start_gate().is_ok());
    let original = pool.gossip.get_node(&NodeId(2)).unwrap();
    for (version, protocol, status) in [
        ("", PROTOCOL_VERSION, NodeStatus::Alive),
        ("other", PROTOCOL_VERSION, NodeStatus::Alive),
        (S4_VERSION, 0, NodeStatus::Alive),
        (S4_VERSION, PROTOCOL_VERSION, NodeStatus::Dead),
    ] {
        let mut state = original.clone();
        state.meta.s4_version = version.into();
        state.meta.protocol_version = protocol;
        state.status = status;
        pool.gossip.set_member_for_testing(NodeId(2), state);
        assert!(migration.start_gate().is_err());
    }
}

#[tokio::test]
async fn large_and_composite_objects_use_the_stream_and_preserve_multipart_etag() {
    let pool = TestPool::start(Replica::Up, Replica::Up, 24930).await;
    let bytes = vec![42; 24 * 1024 * 1024];
    pool.storage(1)
        .put_object(BUCKET, "large", &bytes, "text/plain", &HashMap::new())
        .await
        .unwrap();
    let multipart_etag = "\"123456abcdef-2\"";
    let composite = pool.complete_multipart(1, "parts", multipart_etag, HlcTimestamp::zero()).await;
    let record = pool.storage(1).head_object(BUCKET, "parts").await.unwrap();
    assert!(record.is_composite());
    assert_eq!(record.content_hash, [0; 32]);
    let migration = worker(&pool);
    migration.initialize().await.unwrap();
    let (_, shutdown) = watch::channel(false);
    assert!(migration.pass(&shutdown).await.unwrap());
    for node in 2..=3 {
        assert_eq!(
            pool.storage(node).get_object(BUCKET, "large").await.unwrap().0,
            bytes
        );
        let (body, record) = pool.storage(node).get_object(BUCKET, "parts").await.unwrap();
        assert_eq!(body, composite);
        assert_eq!(record.etag, multipart_etag);
    }
}

struct CountingReader {
    reads: Arc<AtomicU64>,
    remaining: usize,
    error_at_eof: bool,
}
impl AsyncRead for CountingReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        if self.remaining == 0 && self.error_at_eof {
            return Poll::Ready(Err(std::io::Error::other("source checksum failure at EOF")));
        }
        let size = self.remaining.min(buf.remaining());
        buf.put_slice(&vec![7; size]);
        self.remaining -= size;
        Poll::Ready(Ok(()))
    }
}
#[tokio::test]
async fn the_bounded_stream_producer_obeys_backpressure_and_checks_eof_errors() {
    let reads = Arc::new(AtomicU64::new(0));
    let header = proto::WriteBlobHeader {
        total_size: (CHUNK_SIZE * 8) as u64,
        ..Default::default()
    };
    let (tx, mut rx) = mpsc::channel(2);
    let producer = tokio::spawn(produce_stream(
        Box::new(CountingReader {
            reads: reads.clone(),
            remaining: CHUNK_SIZE * 8,
            error_at_eof: false,
        }),
        header,
        tx,
    ));
    for _ in 0..100 {
        if rx.len() == 2 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(rx.len(), 2);
    assert!(
        reads.load(Ordering::Relaxed) <= 2,
        "producer read past its bounded channel"
    );
    let mut bytes = 0;
    while let Some(chunk) = rx.recv().await {
        if let Some(proto::write_blob_chunk::Payload::Data(data)) = chunk.payload {
            bytes += data.len();
            assert!(data.len() <= CHUNK_SIZE);
        }
    }
    producer.await.unwrap().unwrap();
    assert_eq!(bytes, CHUNK_SIZE * 8);
    let (tx, mut rx) = mpsc::channel(2);
    let producer = tokio::spawn(produce_stream(
        Box::new(CountingReader {
            reads,
            remaining: 1,
            error_at_eof: true,
        }),
        proto::WriteBlobHeader {
            total_size: 1,
            ..Default::default()
        },
        tx,
    ));
    while rx.recv().await.is_some() {}
    assert!(producer.await.unwrap().unwrap_err().to_string().contains("EOF"));
}

struct ChangeAtCas {
    storage: Arc<BitcaskStorageEngine>,
    delete: bool,
    changed: std::sync::atomic::AtomicBool,
}
#[async_trait::async_trait]
impl MigrationStore for ChangeAtCas {
    fn index(&self) -> Arc<IndexDb> {
        self.storage.index_db().clone()
    }
    async fn open(
        &self,
        bucket: &str,
        key: &str,
        expected: &IndexRecord,
    ) -> Result<ObjectStream, StorageError> {
        self.storage.open_legacy_migration_stream(bucket, key, expected).await
    }
    async fn finalize(
        &self,
        full: &str,
        original: &IndexRecord,
        observed: Option<&IndexRecord>,
        keep: bool,
    ) -> Result<bool, StorageError> {
        if !self.changed.swap(true, Ordering::Relaxed) {
            let (bucket, key) = full.split_once('/').unwrap();
            let hlc =
                HlcTimestamp::from_record(HlcTimestamp::of_record(original).wall_time + 100, 0, 7);
            if self.delete {
                self.storage
                    .tombstone_object(bucket, key, hlc.wall_time, hlc.logical, hlc.node_id)
                    .await?;
            } else {
                self.storage
                    .put_object(
                        bucket,
                        key,
                        b"winning client bytes",
                        "text/client",
                        &crate::coordinator::test_pool::hlc_metadata(hlc),
                    )
                    .await?;
            }
        }
        finalize_store(&self.storage, full, original, observed, keep).await
    }
    async fn complete(&self) -> Result<bool, StorageError> {
        complete_store(&self.storage).await
    }
}
#[tokio::test]
async fn a_client_write_at_the_final_cas_keeps_the_obligation_until_all_owners_settle() {
    superseded_at_cas(false, 24940).await;
}
#[tokio::test]
async fn a_client_delete_at_the_final_cas_reaches_all_owners_without_resurrection() {
    superseded_at_cas(true, 24950).await;
}
async fn superseded_at_cas(delete: bool, port: u16) {
    let pool = TestPool::start(Replica::Up, Replica::Up, port).await;
    pool.storage(1)
        .put_object(
            BUCKET,
            "race",
            b"old single bytes",
            "text/plain",
            &HashMap::new(),
        )
        .await
        .unwrap();
    let mut migration = worker(&pool);
    migration.parts.storage = Arc::new(ChangeAtCas {
        storage: pool.storage(1).clone(),
        delete,
        changed: std::sync::atomic::AtomicBool::new(false),
    });
    migration.initialize().await.unwrap();
    let (_, shutdown) = watch::channel(false);
    assert!(!migration.pass(&shutdown).await.unwrap());
    assert_eq!(migration.status().pending(), 1);
    assert_eq!(migration.status().migrated(), 0);
    // New worker reconstructs the obligation even though source is no longer HLC0.
    let restarted = worker(&pool);
    restarted.initialize().await.unwrap();
    assert_eq!(restarted.status().pending(), 1);
    assert!(restarted.pass(&shutdown).await.unwrap());
    assert_eq!(restarted.status().superseded(), 1);
    for node in 1..=3 {
        if delete {
            assert!(pool
                .storage(node)
                .index_db()
                .get("bucket/race")
                .await
                .unwrap()
                .unwrap()
                .is_tombstone());
        } else {
            assert_eq!(
                pool.storage(node).get_object(BUCKET, "race").await.unwrap().0,
                b"winning client bytes"
            );
        }
    }
}

#[tokio::test]
async fn shutdown_cancels_a_silent_owner_and_zero_wall_stays_pending() {
    let pool = TestPool::start(Replica::Up, Replica::Silent, 24960).await;
    pool.storage(1)
        .put_object(BUCKET, "silent", b"bytes", "text/plain", &HashMap::new())
        .await
        .unwrap();
    let migration = worker(&pool);
    migration.initialize().await.unwrap();
    let (tx, shutdown) = watch::channel(false);
    let task = tokio::spawn(migration.run(shutdown));
    tokio::time::sleep(Duration::from_millis(20)).await;
    tx.send(true).unwrap();
    tokio::time::timeout(Duration::from_millis(500), task).await.unwrap().unwrap();
    assert!(pool
        .storage(1)
        .head_object(BUCKET, "silent")
        .await
        .unwrap()
        .hlc_order()
        .is_none());
}

struct FivePool {
    storages: Vec<Arc<BitcaskStorageEngine>>,
    migration: MigrationWorker,
    _servers: Vec<crate::rpc::server::RpcServerHandle>,
    _dirs: Vec<tempfile::TempDir>,
}
impl FivePool {
    async fn start(base: u16) -> Self {
        use crate::gossip::NodeState;
        use crate::{
            BucketPlacement, ClusterConfig, Edition, HybridClock, IdempotencyTracker, LocalReplica,
            NodeAddr, NodeCapacity, NodeClientConfig, NodeMeta, NodeRpcServer, PoolConfig, PoolId,
            QuorumReadCoordinator, ReadRepairService, ReadTimeouts, ReplicaReadHandler,
            ReplicaWriteHandler, RpcServerConfig, WithHolder,
        };
        let address = |node: u128| {
            let addr = format!("127.0.0.1:{}", base + node as u16).parse().unwrap();
            NodeAddr {
                grpc_addr: addr,
                http_addr: addr,
                gossip_addr: addr,
            }
        };
        let pool_id = PoolId(1);
        let config = ClusterConfig {
            pools: vec![PoolConfig {
                pool_id,
                name: "migration-five".into(),
                nodes: (1..=5).map(address).collect(),
                replication_factor: Some(3),
                write_quorum: Some(2),
                read_quorum: Some(2),
                storage_class: s4_core::types::StorageClass::Standard,
            }],
            ..Default::default()
        };
        let meta = |node| NodeMeta {
            node_id: NodeId(node),
            node_name: format!("migration-{node}"),
            addr: address(node),
            pool_id,
            topology_epoch: config.epoch,
            protocol_version: PROTOCOL_VERSION,
            capacity: NodeCapacity::default(),
            started_at: 0,
            s4_version: S4_VERSION.into(),
            edition: Edition::Community,
        };
        let mut storages = Vec::new();
        let mut dirs = Vec::new();
        let mut servers = Vec::new();
        let mut members = HashMap::new();
        for node in 1..=5 {
            let dir = tempfile::tempdir().unwrap();
            let storage = Arc::new(
                BitcaskStorageEngine::open(
                    dir.path().join("volumes"),
                    dir.path().join("metadata"),
                    32 * 1024 * 1024,
                    4096,
                    true,
                    s4_core::storage::IndexOptions {
                        version_digests: true,
                    },
                )
                .await
                .unwrap(),
            );
            storage
                .index_db()
                .settle_tombstone_horizon(HlcTimestamp::zero().into())
                .await
                .unwrap();
            if node > 1 {
                members.insert(
                    NodeId(node),
                    NodeState {
                        meta: meta(node),
                        status: NodeStatus::Alive,
                        last_seen: std::time::Instant::now(),
                    },
                );
                servers.push(
                    NodeRpcServer::new(
                        NodeId(node),
                        Arc::new(meta(node)),
                        Arc::new(GossipHandle::new_for_testing(HashMap::new())),
                    )
                    .with_replica_handler(Arc::new(ReplicaWriteHandler::new(
                        storage.clone(),
                        Arc::new(IdempotencyTracker::new()),
                        config.epoch,
                    )))
                    .with_read_handler(Arc::new(ReplicaReadHandler::new(
                        storage.clone(),
                        config.epoch,
                    )))
                    .start(RpcServerConfig {
                        bind_addr: address(node).grpc_addr,
                        ..Default::default()
                    })
                    .await
                    .unwrap(),
                );
            }
            storages.push(storage);
            dirs.push(dir);
        }
        let gossip = Arc::new(GossipHandle::new_for_testing(members));
        let rpc = Arc::new(NodeClient::new(
            gossip.clone(),
            NodeClientConfig {
                max_retries: 0,
                connect_timeout: Duration::from_secs(1),
                ..Default::default()
            },
        ));
        let router = Arc::new(
            PlacementRouter::from_pool_node_ids(
                &config,
                NodeId(1),
                BucketPlacement::from_entries([
                    (BUCKET.to_string(), pool_id),
                    ("__system__".to_string(), pool_id),
                ]),
                &HashMap::from([(pool_id, (1..=5).map(NodeId).collect())]),
            )
            .unwrap(),
        );
        let reader = Arc::new(QuorumReadCoordinator::new(
            NodeId(1),
            storages[0].clone(),
            rpc.clone(),
            router.clone(),
            Arc::new(HybridClock::new(NodeId(1))),
            Arc::new(ReadRepairService::start_with_capacity(rpc.clone(), 16)),
            &config,
            ReadTimeouts::default(),
        ));
        let reconciler = Reconciler::new(
            Arc::new(WithHolder::new(reader, NodeId(1))),
            rpc.clone(),
            gossip.clone(),
            NodeId(1),
            config.epoch,
        )
        .with_local_replica(LocalReplica {
            write: Arc::new(ReplicaWriteHandler::new(
                storages[0].clone(),
                Arc::new(IdempotencyTracker::new()),
                config.epoch,
            )),
            read: Arc::new(ReplicaReadHandler::new(storages[0].clone(), config.epoch)),
        });
        let migration = MigrationWorker::new(MigrationParts {
            storage: Arc::new(PoolStore {
                storage: storages[0].clone(),
                completion_scans: AtomicU64::new(0),
            }),
            router,
            rpc,
            gossip,
            reconciler,
            local: NodeId(1),
            members: (1..=5).map(NodeId).collect(),
            epoch: config.epoch,
            concurrency: 2,
        })
        .unwrap();
        Self {
            storages,
            migration,
            _servers: servers,
            _dirs: dirs,
        }
    }
    fn key(&self, prefix: &str, predicate: impl Fn(&[NodeId]) -> bool) -> String {
        (0..10000)
            .map(|n| format!("{prefix}-{n}"))
            .find(|key| {
                predicate(&self.migration.parts.router.route(BUCKET, key).unwrap().replicas)
            })
            .unwrap()
    }
}

#[tokio::test]
async fn in_five_nodes_an_offline_owner_blocks_only_its_keys_and_foreign_cleanup_waits_for_all() {
    let pool = FivePool::start(24970).await;
    let blocked = pool.key("blocked", |owners| {
        !owners.contains(&NodeId(1)) && owners.contains(&NodeId(5))
    });
    let independent = pool.key("independent", |owners| !owners.contains(&NodeId(5)));
    for key in [&blocked, &independent] {
        pool.storages[0]
            .put_object(BUCKET, key, b"single bytes", "text/plain", &HashMap::new())
            .await
            .unwrap();
    }
    pool.migration.initialize().await.unwrap();
    assert!(pool.migration.start_gate().is_ok());
    let mut state = pool.migration.parts.gossip.get_node(&NodeId(5)).unwrap();
    state.status = NodeStatus::Dead;
    pool.migration.parts.gossip.set_member_for_testing(NodeId(5), state.clone());
    let (_, shutdown) = watch::channel(false);
    assert!(!pool.migration.pass(&shutdown).await.unwrap());
    assert_eq!(pool.migration.status().pending(), 1);
    assert_eq!(pool.migration.status().migrated(), 1);
    assert!(pool.storages[0]
        .head_object(BUCKET, &blocked)
        .await
        .unwrap()
        .hlc_order()
        .is_none());
    state.status = NodeStatus::Alive;
    pool.migration.parts.gossip.set_member_for_testing(NodeId(5), state);
    assert!(pool.migration.pass(&shutdown).await.unwrap());
    assert!(pool.storages[0]
        .index_db()
        .get(&format!("bucket/{blocked}"))
        .await
        .unwrap()
        .is_none());
    for owner in pool.migration.parts.router.route(BUCKET, &blocked).unwrap().replicas {
        assert_eq!(
            pool.storages[owner.0 as usize - 1]
                .get_object(BUCKET, &blocked)
                .await
                .unwrap()
                .0,
            b"single bytes"
        );
    }
}

#[tokio::test]
async fn foreign_object_and_bucket_marker_cannot_resurrect_after_owners_purge_their_delete() {
    use s4_core::storage::TombstoneRef;
    let pool = FivePool::start(24980).await;
    let object = pool.key("purged", |owners| !owners.contains(&NodeId(1)));
    let marker = (0..10000)
        .map(|n| format!("__s4_bucket_marker_deleted-{n}"))
        .find(|key| {
            !pool
                .migration
                .parts
                .router
                .route("__system__", key)
                .unwrap()
                .replicas
                .contains(&NodeId(1))
        })
        .unwrap();
    for (bucket, key) in [(BUCKET, object.as_str()), ("__system__", marker.as_str())] {
        pool.storages[0]
            .put_object(
                bucket,
                key,
                b"old single bytes",
                "text/plain",
                &HashMap::new(),
            )
            .await
            .unwrap();
        let original = pool.storages[0].head_object(bucket, key).await.unwrap();
        let deleted =
            HlcTimestamp::from_record(HlcTimestamp::of_record(&original).wall_time + 100, 0, 7);
        for owner in pool.migration.parts.router.route(bucket, key).unwrap().replicas {
            let storage = &pool.storages[owner.0 as usize - 1];
            assert!(storage.tombstone_object(bucket, key, deleted.wall_time, 0, 7).await.unwrap());
            assert_eq!(
                storage
                    .gc_tombstones(&[TombstoneRef {
                        full_key: format!("{bucket}/{key}"),
                        hlc: deleted.as_tuple()
                    }])
                    .await
                    .unwrap(),
                1
            );
        }
    }
    // Intent does not authorize overriding the current owners' GC history.
    pool.migration.initialize().await.unwrap();
    let (_, shutdown) = watch::channel(false);
    assert!(!pool.migration.pass(&shutdown).await.unwrap());
    assert_eq!(pool.migration.status().pending(), 2);
    assert_eq!(pool.migration.status().migrated(), 0);
    for (bucket, key) in [(BUCKET, object.as_str()), ("__system__", marker.as_str())] {
        assert!(pool.storages[0].head_object(bucket, key).await.unwrap().hlc_order().is_none());
        for owner in pool.migration.parts.router.route(bucket, key).unwrap().replicas {
            assert!(pool.storages[owner.0 as usize - 1]
                .index_db()
                .get(&format!("{bucket}/{key}"))
                .await
                .unwrap()
                .is_none());
        }
    }
    assert!(!pool.storages[0].index_db().legacy_migration_complete().await.unwrap());
}

#[tokio::test]
async fn foreign_superseded_source_is_removed_without_storing_the_repaired_version_here() {
    let pool = FivePool::start(24990).await;
    let key = pool.key("superseded", |owners| !owners.contains(&NodeId(1)));
    pool.storages[0]
        .put_object(
            BUCKET,
            &key,
            b"old single bytes",
            "text/plain",
            &HashMap::new(),
        )
        .await
        .unwrap();
    pool.migration.initialize().await.unwrap();
    let original = pool.storages[0].head_object(BUCKET, &key).await.unwrap();
    let newest =
        HlcTimestamp::from_record(HlcTimestamp::of_record(&original).wall_time + 100, 0, 7);
    let owners = pool.migration.parts.router.route(BUCKET, &key).unwrap().replicas;
    for owner in owners.iter().take(2) {
        pool.storages[owner.0 as usize - 1]
            .put_object(
                BUCKET,
                &key,
                b"winning client bytes",
                "text/client",
                &crate::coordinator::test_pool::hlc_metadata(newest),
            )
            .await
            .unwrap();
    }
    let (_, shutdown) = watch::channel(false);
    assert!(pool.migration.pass(&shutdown).await.unwrap());
    assert_eq!(pool.migration.status().superseded(), 1);
    assert!(pool.storages[0]
        .index_db()
        .get(&format!("bucket/{key}"))
        .await
        .unwrap()
        .is_none());
    for owner in owners {
        assert_eq!(
            pool.storages[owner.0 as usize - 1].get_object(BUCKET, &key).await.unwrap().0,
            b"winning client bytes"
        );
    }
}

#[tokio::test]
async fn an_obligation_superseded_by_a_delete_finishes_after_all_tombstones_are_purged() {
    let pool = TestPool::start(Replica::Up, Replica::Up, 25000).await;
    pool.storage(1)
        .put_object(
            BUCKET,
            "purged-debt",
            b"old bytes",
            "text/plain",
            &HashMap::new(),
        )
        .await
        .unwrap();
    let migration = worker(&pool);
    migration.initialize().await.unwrap();
    let source = pool.storage(1).head_object(BUCKET, "purged-debt").await.unwrap();
    let deleted = HlcTimestamp::from_record(HlcTimestamp::of_record(&source).wall_time + 100, 0, 7);
    for node in 1..=3 {
        pool.storage(node)
            .tombstone_object(BUCKET, "purged-debt", deleted.wall_time, 0, 7)
            .await
            .unwrap();
        pool.storage(node)
            .gc_tombstones(&[s4_core::storage::TombstoneRef {
                full_key: "bucket/purged-debt".into(),
                hlc: deleted.as_tuple(),
            }])
            .await
            .unwrap();
    }
    let restarted = worker(&pool);
    restarted.initialize().await.unwrap();
    assert_eq!(restarted.status().pending(), 1);
    let (_, shutdown) = watch::channel(false);
    assert!(restarted.pass(&shutdown).await.unwrap());
    assert_eq!(restarted.status().superseded(), 1);
}

#[tokio::test]
async fn source_only_uncertain_restart_resumes_with_owners_zero_history() {
    use crate::{
        ClusterConfig, HybridClock, IdempotencyTracker, LocalReplica, QuorumReadCoordinator,
        ReadRepairService, ReadTimeouts, ReplicaReadHandler, ReplicaWriteHandler, WithHolder,
    };
    let pool = FivePool::start(25010).await;
    let key = pool.key("source-restart", |owners| !owners.contains(&NodeId(1)));
    pool.storages[0]
        .put_object(
            BUCKET,
            &key,
            b"original single bytes",
            "text/plain",
            &HashMap::new(),
        )
        .await
        .unwrap();
    pool.migration.initialize().await.unwrap();
    let FivePool {
        mut storages,
        migration,
        _servers,
        _dirs,
    } = pool;
    let router = migration.parts.router.clone();
    let rpc = migration.parts.rpc.clone();
    let gossip = migration.parts.gossip.clone();
    let epoch = migration.parts.epoch;
    drop(migration);
    let source = storages.remove(0);
    let engine = Arc::try_unwrap(source).ok().expect("all source handles released");
    drop(engine); // No shutdown seal: source history is deliberately uncertain.
    let source = Arc::new(
        BitcaskStorageEngine::open(
            _dirs[0].path().join("volumes"),
            _dirs[0].path().join("metadata"),
            32 * 1024 * 1024,
            4096,
            true,
            s4_core::storage::IndexOptions {
                version_digests: true,
            },
        )
        .await
        .unwrap(),
    );
    source
        .index_db()
        .settle_tombstone_horizon(HlcTimestamp::from_record(9000000000000, 0, 7).into())
        .await
        .unwrap();
    storages.insert(0, source.clone());
    let reader = Arc::new(QuorumReadCoordinator::new(
        NodeId(1),
        source.clone(),
        rpc.clone(),
        router.clone(),
        Arc::new(HybridClock::new(NodeId(1))),
        Arc::new(ReadRepairService::start_with_capacity(rpc.clone(), 16)),
        &ClusterConfig {
            epoch,
            ..Default::default()
        },
        ReadTimeouts::default(),
    ));
    let reconciler = Reconciler::new(
        Arc::new(WithHolder::new(reader, NodeId(1))),
        rpc.clone(),
        gossip.clone(),
        NodeId(1),
        epoch,
    )
    .with_local_replica(LocalReplica {
        write: Arc::new(ReplicaWriteHandler::new(
            source.clone(),
            Arc::new(IdempotencyTracker::new()),
            epoch,
        )),
        read: Arc::new(ReplicaReadHandler::new(source.clone(), epoch)),
    });
    let restarted = MigrationWorker::new(MigrationParts {
        storage: Arc::new(PoolStore {
            storage: source.clone(),
            completion_scans: AtomicU64::new(0),
        }),
        router,
        rpc,
        gossip,
        reconciler,
        local: NodeId(1),
        members: (1..=5).map(NodeId).collect(),
        epoch,
        concurrency: 2,
    })
    .unwrap();
    restarted.initialize().await.unwrap();
    assert_eq!(restarted.status().pending(), 1);
    let (_, shutdown) = watch::channel(false);
    assert!(restarted.pass(&shutdown).await.unwrap());
    assert_eq!(restarted.status().migrated(), 1);
    assert!(source.index_db().get(&format!("bucket/{key}")).await.unwrap().is_none());
    drop(_servers);
}

#[tokio::test]
async fn unknown_owner_history_stays_pending_until_current_owners_confirm_the_exact_copy() {
    use crate::coordinator::test_pool::at;
    let pool = TestPool::start_with_horizons(
        Replica::Up,
        Replica::Up,
        25020,
        [None, Some(at(9000000000000)), Some(at(9000000000000))],
    )
    .await;
    pool.storage(1)
        .put_object(
            BUCKET,
            "unknown-history",
            b"old copy",
            "text/plain",
            &HashMap::new(),
        )
        .await
        .unwrap();
    let original = pool.storage(1).head_object(BUCKET, "unknown-history").await.unwrap();
    let migration = worker(&pool);
    migration.initialize().await.unwrap();
    let (_, shutdown) = watch::channel(false);
    assert!(!migration.pass(&shutdown).await.unwrap());
    assert_eq!(migration.status().pending(), 1);
    for node in [2, 3] {
        assert!(pool
            .storage(node)
            .index_db()
            .get("bucket/unknown-history")
            .await
            .unwrap()
            .is_none());
        pool.put_with_etag(
            node,
            "unknown-history",
            b"old copy",
            HlcTimestamp::of_record(&original),
            &original.etag,
        )
        .await;
    }
    assert!(migration.pass(&shutdown).await.unwrap());
}

#[tokio::test]
async fn zero_legacy_wall_does_not_get_a_fresh_clock_or_block_an_independent_key() {
    let pool = TestPool::start(Replica::Up, Replica::Up, 25030).await;
    for key in ["ancient", "ordinary"] {
        pool.storage(1)
            .put_object(BUCKET, key, b"preserved", "text/plain", &HashMap::new())
            .await
            .unwrap();
    }
    let mut ancient = pool.storage(1).head_object(BUCKET, "ancient").await.unwrap();
    ancient.modified_at = 999_999;
    pool.storage(1).index_db().put("bucket/ancient", &ancient).await.unwrap();
    let migration = worker(&pool);
    migration.initialize().await.unwrap();
    let (_, shutdown) = watch::channel(false);
    assert!(!migration.pass(&shutdown).await.unwrap());
    assert_eq!(migration.status().pending(), 1);
    assert_eq!(migration.status().migrated(), 1);
    assert_eq!(
        pool.storage(1).head_object(BUCKET, "ancient").await.unwrap(),
        ancient
    );
    for node in [2, 3] {
        assert!(pool.storage(node).index_db().get("bucket/ancient").await.unwrap().is_none());
    }
}

struct RejectBufferedLocal {
    inner: Arc<dyn crate::repair::reconcile::KeyStateSource>,
}
#[async_trait::async_trait]
impl crate::repair::reconcile::KeyStateSource for RejectBufferedLocal {
    async fn newest_version(
        &self,
        bucket: &str,
        key: &str,
        target: NodeId,
    ) -> Result<NewestVersion, ClusterError> {
        self.inner.newest_version(bucket, key, target).await
    }
    async fn key_versions(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<crate::repair::reconcile::KeyVersions, ClusterError> {
        self.inner.key_versions(bucket, key).await
    }
    async fn read_copy(
        &self,
        bucket: &str,
        key: &str,
        node: NodeId,
    ) -> Result<crate::repair::reconcile::StoredCopy, ClusterError> {
        if node == NodeId(1) {
            return Err(ClusterError::Unavailable(
                "local migration source must stream".into(),
            ));
        }
        self.inner.read_copy(bucket, key, node).await
    }
}
#[tokio::test]
async fn changed_local_snapshots_stream_without_buffered_repair_or_double_accounting() {
    let pool = TestPool::start(Replica::Up, Replica::Up, 25040).await;
    for key in ["relocated", "client-large"] {
        pool.storage(1)
            .put_object(
                BUCKET,
                key,
                &vec![12; 24 * 1024 * 1024],
                "text/plain",
                &HashMap::new(),
            )
            .await
            .unwrap();
    }
    let mut migration = worker(&pool);
    migration.parts.reconciler = pool.reconciler_reading_source(Arc::new(RejectBufferedLocal {
        inner: Arc::new(pool.read_coordinator()),
    }));
    migration.initialize().await.unwrap();
    let mut physical = pool.storage(1).head_object(BUCKET, "relocated").await.unwrap();
    physical.created_at += 1; // Same VersionRank, different full storage snapshot, as after compaction.
    pool.storage(1).index_db().put("bucket/relocated", &physical).await.unwrap();
    let original = pool.storage(1).head_object(BUCKET, "client-large").await.unwrap();
    pool.put(
        1,
        "client-large",
        &vec![13; 24 * 1024 * 1024],
        HlcTimestamp::from_record(HlcTimestamp::of_record(&original).wall_time + 100, 0, 7),
    )
    .await;
    let (_, shutdown) = watch::channel(false);
    assert!(migration.pass(&shutdown).await.unwrap());
    assert_eq!(migration.status().migrated(), 1);
    assert_eq!(migration.status().superseded(), 1);
    assert_eq!(migration.status().pending(), 0);
    for node in 1..=3 {
        assert_eq!(
            pool.storage(node).get_object(BUCKET, "relocated").await.unwrap().0,
            vec![12; 24 * 1024 * 1024]
        );
        assert_eq!(
            pool.storage(node).get_object(BUCKET, "client-large").await.unwrap().0,
            vec![13; 24 * 1024 * 1024]
        );
    }
}
