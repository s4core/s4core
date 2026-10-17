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
use async_trait::async_trait;
use s4_cluster::replica::ReplicaWriteHandler;
use s4_cluster::rpc::server::{NodeRpcServer, RpcServerConfig, RpcServerHandle};
use s4_cluster::rpc::{NodeClient, NodeClientConfig};
use s4_cluster::{
    BucketPlacement, ClusterConfig, EcObjectDeleteCommit, EcObjectPutCommit,
    EcObjectPutDisposition, EcObjectRuntime, Edition, GossipHandle, HintedHandoffConfig,
    HintedHandoffManager, HlcTimestamp, HybridClock, IdempotencyTracker, NodeAddr, NodeCapacity,
    NodeId, NodeMeta, NodeState, NodeStatus, PlacementRouter, PoolConfig, PoolId, WriteTimeouts,
};
use s4_core::types::StorageClass;
use s4_core::{BitcaskStorageEngine, KeyVersion};
use std::sync::{Arc, Mutex};
use tempfile::TempDir;

#[derive(Default)]
struct ColdEcRuntime {
    deletes: Mutex<Vec<EcObjectDeleteCommit>>,
    refuse_commit: bool,
}

#[async_trait]
impl EcObjectRuntime for ColdEcRuntime {
    async fn record_put_commit(
        &self,
        _: EcObjectPutCommit,
    ) -> Result<EcObjectPutDisposition, ClusterError> {
        panic!("delete must not invoke PUT");
    }

    async fn record_delete_commit(&self, commit: EcObjectDeleteCommit) -> Result<(), ClusterError> {
        self.deletes.lock().unwrap().push(commit);
        if self.refuse_commit {
            Err(ClusterError::QuorumNotMet {
                needed: 4,
                achieved: 3,
            })
        } else {
            Ok(())
        }
    }

    async fn read_object(
        &self,
        _: EcObjectReadRequest,
    ) -> Result<EcObjectReadResult, EcObjectReadError> {
        panic!("delete must not invoke GET");
    }

    // A cached cold layout can answer HEAD even when two namespace owners
    // are down. It must never substitute for the failed namespace DELETE.
    async fn head_object(
        &self,
        _: EcObjectHeadRequest,
    ) -> Result<EcObjectHeadResult, EcObjectReadError> {
        Ok(EcObjectHeadResult {
            etag: "cold-object".into(),
            content_type: "application/octet-stream".into(),
            size: 4 * 1024 * 1024,
            metadata: HashMap::new(),
            hlc: HlcTimestamp::from_record(1, 0, 1),
            version_id: None,
            layout: EcObjectReadLayout::ErasureCoded {
                erasure_set_id: 0,
                topology_epoch: 1,
                profile_name: "rs-4-2".into(),
                chunk_count: 1,
            },
        })
    }
}

struct Fixture {
    state: AppState,
    coordinator: Arc<QuorumWriteCoordinator>,
    storage: Arc<BitcaskStorageEngine>,
    ec: Arc<ColdEcRuntime>,
    _server: Option<RpcServerHandle>,
    _dir: TempDir,
}

impl Fixture {
    async fn new(namespace_quorum: bool, ec_pool: bool, refuse_commit: bool) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let storage = Arc::new(
            BitcaskStorageEngine::new(
                dir.path().join("volumes"),
                dir.path().join("metadata"),
                10 * 1024 * 1024,
                4096,
                false,
            )
            .await
            .unwrap(),
        );
        storage
            .put_object("bucket", "cold", b"stub", "text/plain", &HashMap::new())
            .await
            .unwrap();
        let local = NodeId(1);
        let pool = PoolId::new(1);
        let mut server = None;
        let mut members = HashMap::new();
        let mut peer_addr = ([127, 0, 0, 1], 0).into();
        if namespace_quorum {
            let reservation = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            peer_addr = reservation.local_addr().unwrap();
            let meta = NodeMeta {
                node_id: NodeId(2),
                node_name: "peer".into(),
                addr: NodeAddr {
                    grpc_addr: peer_addr,
                    http_addr: peer_addr,
                    gossip_addr: peer_addr,
                },
                pool_id: pool,
                topology_epoch: ClusterConfig::default().epoch,
                protocol_version: s4_cluster::protocol::PROTOCOL_VERSION,
                capacity: NodeCapacity::default(),
                started_at: 0,
                s4_version: "test".into(),
                edition: Edition::Community,
            };
            let peer_storage = Arc::new(
                BitcaskStorageEngine::new(
                    dir.path().join("peer-volumes"),
                    dir.path().join("peer-metadata"),
                    10 * 1024 * 1024,
                    4096,
                    false,
                )
                .await
                .unwrap(),
            );
            let handler = ReplicaWriteHandler::new(
                peer_storage,
                Arc::new(IdempotencyTracker::new()),
                meta.topology_epoch,
            );
            let rpc_server = NodeRpcServer::new(
                NodeId(2),
                Arc::new(meta.clone()),
                Arc::new(GossipHandle::new_for_testing(Default::default())),
            )
            .with_replica_handler(Arc::new(handler));
            drop(reservation);
            // start binds synchronously and fails loudly if the port was taken.
            server = Some(
                rpc_server
                    .start(RpcServerConfig {
                        bind_addr: peer_addr,
                        ..Default::default()
                    })
                    .await
                    .unwrap(),
            );
            members.insert(
                NodeId(2),
                NodeState {
                    meta,
                    status: NodeStatus::Alive,
                    last_seen: std::time::Instant::now(),
                },
            );
        }
        let addr = |n: u16| NodeAddr {
            grpc_addr: if n == 2 {
                peer_addr
            } else {
                ([127, 0, 0, 1], 19400 + n).into()
            },
            http_addr: ([127, 0, 0, 1], 19500 + n).into(),
            gossip_addr: ([127, 0, 0, 1], 19600 + n).into(),
        };
        let config = ClusterConfig {
            pools: vec![PoolConfig {
                pool_id: pool,
                name: "pool".into(),
                nodes: (1..=3).map(addr).collect(),
                replication_factor: Some(3),
                write_quorum: Some(2),
                read_quorum: Some(2),
                storage_class: StorageClass::Standard,
            }],
            ..ClusterConfig::default()
        };
        let router = PlacementRouter::from_pool_node_ids(
            &config,
            local,
            BucketPlacement::from_entries([("bucket".into(), pool)]),
            &HashMap::from([(pool, vec![local, NodeId(2), NodeId(3)])]),
        )
        .unwrap();
        // Only the live gRPC peer can ACK. The other peer is absent.
        let gossip = Arc::new(GossipHandle::new_for_testing(members));
        let rpc = Arc::new(NodeClient::new(gossip, NodeClientConfig::default()));
        let hints = Arc::new(HintedHandoffManager::new(
            HintedHandoffConfig::new(dir.path().join("hints")),
            rpc.clone(),
        ));
        let coordinator = Arc::new(QuorumWriteCoordinator::new(
            local,
            storage.clone(),
            rpc,
            Arc::new(router),
            Arc::new(HybridClock::new(local)),
            hints,
            &config,
            WriteTimeouts::default(),
        ));
        let api_storage = BitcaskStorageEngine::new(
            dir.path().join("api-volumes"),
            dir.path().join("api-metadata"),
            10 * 1024 * 1024,
            4096,
            false,
        )
        .await
        .unwrap();
        let ec = Arc::new(ColdEcRuntime {
            refuse_commit,
            ..Default::default()
        });
        let mut state = AppState::new(api_storage, "test".into(), "test-secret".into(), dir.path())
            .await
            .with_default_bucket_storage_class(if ec_pool {
                StorageClass::ErasureCoded
            } else {
                StorageClass::Standard
            })
            .with_ec_object_runtime(ec.clone());
        state.write_coordinator = Some(coordinator.clone());
        Self {
            state,
            coordinator,
            storage,
            ec,
            _server: server,
            _dir: dir,
        }
    }
}

#[tokio::test]
async fn cold_ec_head_cannot_acknowledge_a_failed_namespace_delete() {
    let f = Fixture::new(false, true, false).await;
    let response = delete_object_in_cluster(&f.state, &f.coordinator, "bucket", "cold").await;
    assert_eq!(
        response.err().map(|r| r.status()),
        Some(StatusCode::SERVICE_UNAVAILABLE)
    );
    assert!(
        f.ec.deletes.lock().unwrap().is_empty(),
        "failed namespace quorum must not commit an EC tombstone"
    );
}

#[tokio::test]
async fn ec_delete_uses_the_acknowledged_namespace_tombstone_hlc() {
    let f = Fixture::new(true, true, false).await;
    delete_object_in_cluster(&f.state, &f.coordinator, "bucket", "cold")
        .await
        .unwrap();
    let KeyVersion::Tombstone(record) = f.storage.key_version("bucket", "cold").await.unwrap()
    else {
        panic!("successful namespace delete must leave a tombstone");
    };
    let commits = f.ec.deletes.lock().unwrap();
    assert_eq!(commits.len(), 1);
    assert_eq!(commits[0].delete_hlc, HlcTimestamp::of_record(&record));
    assert!(!commits[0].operation_id.is_nil());
    assert_eq!((&*commits[0].bucket, &*commits[0].key), ("bucket", "cold"));
    assert_eq!(commits[0].version_id, None);
}

#[tokio::test]
async fn failed_ec_tombstone_commit_is_not_success() {
    let f = Fixture::new(true, true, true).await;
    let response = delete_object_in_cluster(&f.state, &f.coordinator, "bucket", "cold").await;
    assert_eq!(
        response.err().map(|r| r.status()),
        Some(StatusCode::SERVICE_UNAVAILABLE)
    );
    assert_eq!(f.ec.deletes.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn replicated_pool_delete_does_not_call_ec_runtime() {
    let f = Fixture::new(true, false, true).await;
    delete_object_in_cluster(&f.state, &f.coordinator, "bucket", "cold")
        .await
        .unwrap();
    assert!(f.ec.deletes.lock().unwrap().is_empty());
}

#[tokio::test]
async fn batch_delete_reports_namespace_quorum_failure_per_key() {
    use http_body_util::BodyExt;

    let f = Fixture::new(false, true, false).await;
    let response = crate::handlers::bucket::delete_objects(
        State(f.state.clone()),
        Path("bucket".into()),
        HeaderMap::new(),
        Bytes::from_static(b"<Delete><Object><Key>cold</Key></Object></Delete>"),
    )
    .await
    .into_response();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let xml = std::str::from_utf8(&body).unwrap();
    assert!(xml.contains("<Error>"), "{xml}");
    assert!(xml.contains("<Code>ServiceUnavailable</Code>"), "{xml}");
    assert!(!xml.contains("<Deleted>"), "{xml}");
    assert!(f.ec.deletes.lock().unwrap().is_empty());
}
