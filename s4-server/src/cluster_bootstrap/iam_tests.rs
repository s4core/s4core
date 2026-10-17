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

//! IAM in cluster mode (plan CLUSTER-IAM-REPLICATION.md, phase 2): three
//! nodes, each with its API, its coordinators and a `NodeRpcServer` on
//! Bitcask behind the adapter a cluster node uses. A user changed through one
//! node is one record on every node — written through a majority, the rest
//! told at once — and each node finds it in its own directory, so a login, a
//! token and an access key hold through every node. A delete leaves a
//! tombstone everywhere; a change without a majority is refused.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use s4_api::{create_router, AppState};
use s4_cluster::rpc::server::RpcServerHandle;
use s4_cluster::{
    BucketPlacement, ClusterConfig, DistributedListCoordinator, Edition, GossipHandle,
    HintedHandoffConfig, HintedHandoffManager, HybridClock, IamAdoptionCollector, IamAdoptionGate,
    IamCatchUp, IamCatchUpConfig, IamCopy, IamFreshness, IamLeaseConfig, IamLeaseHolder,
    IamLeaseKeeper, IamOwnUsers, IamStatusSource, IdempotencyTracker, ListTimeouts, LocalReplica,
    NodeAddr, NodeCapacity, NodeClient, NodeClientConfig, NodeId, NodeMeta, NodeRpcServer,
    NodeState, NodeStatus, PlacementRouter, PoolConfig, PoolId, PoolRoster, QuorumReadCoordinator,
    QuorumWriteCoordinator, ReadRepairService, ReadTimeouts, Reconciler, ReplicaReadHandler,
    ReplicaWriteHandler, RpcServerConfig, WriteTimeouts, IAM_REPLICATION_MIN_PROTOCOL_VERSION,
};
use s4_core::storage::engine::StorageEngine;
use s4_core::storage::BitcaskStorageEngine;
use s4_core::types::StorageClass;
use s4_features::iam::models::Role;
use s4_features::iam::{RootAccount, USER_KEY_PREFIX};
use tower::ServiceExt;

use super::SharedStorageAdapter;

const JWT_SECRET: &str = "cluster-iam-test-secret-of-32-bytes-or-more";
const ROOT_PASSWORD: &str = "Root-Pass-12345";

/// One node of the pool.
struct Node {
    state: AppState,
    /// What this node knows of the others.
    gossip: Arc<GossipHandle>,
    server: Option<RpcServerHandle>,
    /// This node's grants of the lease of IAM changes.
    keeper: Arc<IamLeaseKeeper>,
    /// Takes the lease for the changes through this node.
    iam_lease: Arc<IamLeaseHolder>,
    /// This node's copy of IAM as the pool compares it.
    copy: Arc<IamCopy>,
    /// Keeps the copy current; ticked by hand, gossip being the tests'.
    catch_up: Arc<IamCatchUp>,
    /// Whether a majority agrees with the copy.
    freshness: Arc<IamFreshness>,
    /// Opens IAM changes once no node keeps IAM of its own; `None` for a
    /// node built without it.
    adoption: Option<Arc<crate::iam_adoption::IamAdoptionWorker>>,
}

/// Three nodes, RF=3, W=R=2: the IAM records are on all three, written and
/// read through two.
struct Pool {
    nodes: Vec<Node>,
    _dirs: Vec<tempfile::TempDir>,
}

fn addr(base_port: u16, n: u128) -> NodeAddr {
    NodeAddr {
        grpc_addr: format!("127.0.0.1:{}", base_port + n as u16).parse().unwrap(),
        http_addr: format!("127.0.0.1:{}", 9000 + n).parse().unwrap(),
        gossip_addr: format!("127.0.0.1:{}", 9200 + n).parse().unwrap(),
    }
}

fn config(base_port: u16) -> ClusterConfig {
    ClusterConfig {
        pools: vec![PoolConfig {
            pool_id: PoolId::new(0),
            name: "pool-1".to_string(),
            nodes: (1..=3).map(|n| addr(base_port, n)).collect(),
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

fn meta(base_port: u16, n: u128, protocol_version: u32) -> NodeMeta {
    NodeMeta {
        node_id: NodeId(n),
        node_name: format!("node-{n}"),
        addr: addr(base_port, n),
        pool_id: PoolId::new(0),
        topology_epoch: config(base_port).epoch,
        protocol_version,
        capacity: NodeCapacity::default(),
        started_at: 0,
        s4_version: "0.1.0".into(),
        edition: Edition::Community,
    }
}

fn member(base_port: u16, n: u128, protocol_version: u32, status: NodeStatus) -> NodeState {
    NodeState {
        meta: meta(base_port, n, protocol_version),
        status,
        last_seen: std::time::Instant::now(),
    }
}

async fn pool(base_port: u16) -> Pool {
    pool_with_lease(base_port, IamLeaseConfig::default()).await
}

/// The pool, its nodes holding the lease of IAM changes as `lease_config`
/// says; credentials are checked whatever the copy of IAM.
async fn pool_with_lease(base_port: u16, lease_config: IamLeaseConfig) -> Pool {
    pool_with(base_port, lease_config, None, false).await
}

/// The pool, each node checking IAM credentials only while a majority agrees
/// with its copy of IAM, for at most `max_staleness` after it last did.
async fn gated_pool(base_port: u16, max_staleness: Duration) -> Pool {
    pool_with(
        base_port,
        IamLeaseConfig::default(),
        Some(max_staleness),
        false,
    )
    .await
}

/// The pool, each node changing IAM only once no node of it keeps IAM of its
/// own, and answering the report and the decision of its adoption.
async fn adopting_pool(base_port: u16) -> Pool {
    pool_with(base_port, IamLeaseConfig::default(), None, true).await
}

/// The pool, gated as `gate` says when it is some.
async fn pool_with(
    base_port: u16,
    lease_config: IamLeaseConfig,
    gate: Option<Duration>,
    adopting: bool,
) -> Pool {
    let config = config(base_port);
    let resolver: HashMap<_, _> =
        (1..=3).map(|n| (addr(base_port, n).grpc_addr, NodeId(n))).collect();
    let mut nodes = Vec::new();
    let mut dirs = Vec::new();
    for n in 1..=3u128 {
        let dir = tempfile::tempdir().unwrap();
        let engine = BitcaskStorageEngine::new(
            dir.path().join("volumes"),
            dir.path().join("metadata_db"),
            10 * 1024 * 1024,
            4096,
            false,
        )
        .await
        .unwrap();
        let state = AppState::new(
            engine,
            "test-access-key".to_string(),
            "test-secret-key".to_string(),
            dir.path(),
        )
        .await
        .with_jwt_secret(JWT_SECRET)
        .with_root_account(RootAccount::new("root", ROOT_PASSWORD, JWT_SECRET).unwrap())
        .with_iam_node_name(&format!("node-{n}"));
        // Every node seals secrets with the key derived from the shared JWT
        // secret, as a node started with S4_JWT_SECRET does.
        state
            .iam_storage
            .use_secret_keys(s4_features::iam::SecretKeySettings {
                jwt_secret: Some(JWT_SECRET.to_string()),
                ..Default::default()
            })
            .await
            .unwrap();
        let copy = Arc::new(IamCopy::new(Arc::new(SharedStorageAdapter::new(
            state.storage.clone(),
        ))));
        copy.load().await.unwrap();
        let storage: Arc<dyn StorageEngine> = Arc::new(
            SharedStorageAdapter::new(state.storage.clone())
                .with_iam_directory(Some(state.iam_storage.directory()))
                .with_iam_copy(copy.clone()),
        );
        let members = (1..=3u128)
            .filter(|peer| *peer != n)
            .map(|peer| {
                (
                    NodeId(peer),
                    member(
                        base_port,
                        peer,
                        s4_cluster::PROTOCOL_VERSION,
                        NodeStatus::Alive,
                    ),
                )
            })
            .collect();
        let gossip = Arc::new(GossipHandle::new_for_testing(members));
        let rpc_client = Arc::new(NodeClient::new(gossip.clone(), NodeClientConfig::default()));
        let router = Arc::new(
            PlacementRouter::new(&config, NodeId(n), BucketPlacement::new(), &resolver).unwrap(),
        );
        let clock = Arc::new(HybridClock::new(NodeId(n)));
        let hints = Arc::new(HintedHandoffManager::new(
            HintedHandoffConfig::new(dir.path()),
            rpc_client.clone(),
        ));
        let keeper = Arc::new(IamLeaseKeeper::open(storage.clone()).await.unwrap());
        let iam_lease = Arc::new(IamLeaseHolder::new(
            NodeId(n),
            keeper.clone(),
            rpc_client.clone(),
            router.clone(),
            clock.clone(),
            lease_config,
        ));
        let write = QuorumWriteCoordinator::new(
            NodeId(n),
            storage.clone(),
            rpc_client.clone(),
            router.clone(),
            clock.clone(),
            hints,
            &config,
            WriteTimeouts {
                quorum_timeout: Duration::from_secs(5),
                replica_write_timeout: Duration::from_secs(5),
            },
        )
        .with_iam_lease(keeper.clone());
        let read = Arc::new(QuorumReadCoordinator::new(
            NodeId(n),
            storage.clone(),
            rpc_client.clone(),
            router.clone(),
            clock,
            Arc::new(ReadRepairService::start_with_capacity(
                rpc_client.clone(),
                16,
            )),
            &config,
            ReadTimeouts::default(),
        ));
        let list = DistributedListCoordinator::new(
            NodeId(n),
            storage.clone(),
            rpc_client.clone(),
            router.clone(),
            config.epoch,
            ListTimeouts::default(),
        );
        let replica_write = Arc::new(
            ReplicaWriteHandler::new(
                storage.clone(),
                Arc::new(IdempotencyTracker::new()),
                config.epoch,
            )
            .with_iam_lease(keeper.clone()),
        );
        let replica_read = Arc::new(ReplicaReadHandler::new(storage.clone(), config.epoch));
        let freshness = Arc::new(IamFreshness::new(gate.unwrap_or(Duration::from_secs(30))));
        let reconciler = Reconciler::new(
            read.clone(),
            rpc_client.clone(),
            gossip.clone(),
            NodeId(n),
            config.epoch,
        )
        .with_local_replica(LocalReplica {
            write: replica_write.clone(),
            read: replica_read.clone(),
        });
        let own_users = Arc::new(IamOwnUsers::new(storage.clone()).with_sealer(Arc::new(
            super::DirectorySealer(state.iam_storage.directory()),
        )));
        let collector = Arc::new(IamAdoptionCollector::new(
            NodeId(n),
            PoolRoster::new(
                "pool-1",
                NodeId(n),
                (1..=3u128).map(|peer| (NodeId(peer), format!("node-{peer}"))),
            ),
            (1..=3u128).map(NodeId).collect(),
            own_users.clone(),
            rpc_client.clone(),
        ));
        let catch_up = Arc::new(IamCatchUp::new(
            NodeId(n),
            copy.clone(),
            freshness.clone(),
            gossip.clone(),
            rpc_client,
            router,
            Arc::new(reconciler),
            Duration::from_secs(7 * 24 * 3600),
            IamCatchUpConfig {
                retry: Duration::from_millis(50),
                ..IamCatchUpConfig::default()
            },
        ));
        let server = NodeRpcServer::new(
            NodeId(n),
            Arc::new(meta(base_port, n, s4_cluster::PROTOCOL_VERSION)),
            gossip.clone(),
        )
        .with_replica_handler(replica_write.clone())
        .with_read_handler(replica_read.clone())
        .with_iam_lease(keeper.clone())
        .with_iam_copy(copy.clone())
        .with_iam_own_users(own_users)
        .start(RpcServerConfig {
            bind_addr: addr(base_port, n).grpc_addr,
            ..Default::default()
        })
        .await
        .unwrap();
        let mut state = state.with_cluster_coordinators(
            Arc::new(write),
            read,
            Arc::new(list),
            iam_lease.clone(),
        );
        if gate.is_some() {
            state = state.with_iam_freshness(freshness.clone());
        }
        let mut adoption = None;
        let adoption_gate = if adopting {
            IamAdoptionGate::default()
        } else {
            IamAdoptionGate::opened()
        };
        state = state.with_iam_status(Arc::new(IamStatusSource::new(
            NodeId(n),
            PoolRoster::new(
                "pool-1",
                NodeId(n),
                (1..=3u128).map(|peer| (NodeId(peer), format!("node-{peer}"))),
            ),
            catch_up.clone(),
            freshness.clone(),
            keeper.clone(),
            adoption_gate.clone(),
        )));
        if adopting {
            state = state.with_iam_adoption(s4_api::cluster_iam::ClusterIamAdoption::new(
                collector,
                adoption_gate.clone(),
            ));
            adoption = Some(Arc::new(crate::iam_adoption::IamAdoptionWorker::new(
                state.iam_storage.clone(),
                Some("root".to_string()),
                NodeId(n),
                (1..=3u128).map(NodeId).collect(),
                gossip.clone(),
                adoption_gate,
                None,
            )));
        }
        nodes.push(Node {
            state,
            gossip,
            server: Some(server),
            keeper,
            iam_lease,
            copy,
            catch_up,
            freshness,
            adoption,
        });
        dirs.push(dir);
    }
    Pool { nodes, _dirs: dirs }
}

impl Pool {
    fn node(&self, n: usize) -> &Node {
        &self.nodes[n - 1]
    }

    /// Stops node `n`'s server, and every other node sees it dead.
    async fn stop(&mut self, base_port: u16, n: usize) {
        if let Some(server) = self.nodes[n - 1].server.take() {
            server.shutdown().await.unwrap();
        }
        for (i, node) in self.nodes.iter().enumerate() {
            if i + 1 != n {
                node.gossip.set_member_for_testing(
                    NodeId(n as u128),
                    member(
                        base_port,
                        n as u128,
                        s4_cluster::PROTOCOL_VERSION,
                        NodeStatus::Dead,
                    ),
                );
            }
        }
    }
}

/// One request through a node's router: the status and the body as JSON.
async fn send(
    state: &AppState,
    method: &str,
    uri: &str,
    token: Option<&str>,
    body: &str,
) -> (StatusCode, serde_json::Value) {
    let mut request = Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json");
    if let Some(token) = token {
        request = request.header("authorization", format!("Bearer {token}"));
    }
    let response = create_router(state.clone())
        .oneshot(request.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), 1 << 20).await.unwrap();
    let json = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
    (status, json)
}

/// A token of `username` from node `node`.
async fn login(state: &AppState, username: &str, password: &str) -> (StatusCode, String) {
    let (status, body) = send(
        state,
        "POST",
        "/api/admin/login",
        None,
        &format!(r#"{{"username":"{username}","password":"{password}"}}"#),
    )
    .await;
    (
        status,
        body["token"].as_str().unwrap_or_default().to_string(),
    )
}

/// Whether `check` holds within a few seconds: a write waits for two nodes,
/// and the third is told a moment later.
async fn eventually<F, Fut>(check: F) -> bool
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    for _ in 0..100 {
        if check().await {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    false
}

/// A user created through node 1 logs in through every node, its token from
/// node 1 is accepted by every node, its S3 keys are found by every node, and
/// a change of its role through node 2 is the role every node knows.
#[tokio::test]
async fn a_user_changed_through_one_node_holds_through_every_node() {
    let pool = pool(22301).await;
    let (status, root) = login(&pool.node(1).state, "root", ROOT_PASSWORD).await;
    assert_eq!(status, StatusCode::OK);

    let (status, created) = send(
        &pool.node(1).state,
        "POST",
        "/api/admin/users",
        Some(&root),
        r#"{"username":"alice","password":"Alice-Pass-123","role":"SuperUser"}"#,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{created}");
    let alice_id = created["id"].as_str().unwrap().to_string();
    let (status, keys) = send(
        &pool.node(1).state,
        "POST",
        &format!("/api/admin/users/{alice_id}/credentials"),
        Some(&root),
        "",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{keys}");
    let access_key = keys["access_key"].as_str().unwrap().to_string();
    let secret_key = keys["secret_key"].as_str().unwrap().to_string();

    let (_, alice_token) = login(&pool.node(1).state, "alice", "Alice-Pass-123").await;
    for n in 1..=3 {
        let state = &pool.node(n).state;
        assert!(
            eventually(|| async { state.iam_storage.get_user(&alice_id).await.unwrap().is_some() })
                .await,
            "node-{n} never learned of alice"
        );
        assert_eq!(
            login(state, "alice", "Alice-Pass-123").await.0,
            StatusCode::OK
        );
        let (status, _) = send(state, "GET", "/api/admin/users", Some(&alice_token), "").await;
        assert_eq!(status, StatusCode::OK, "node-{n} refused alice's token");
        let user = state
            .iam_storage
            .verify_s3_credentials(&access_key, &secret_key)
            .await
            .unwrap()
            .expect("the access key is known");
        assert_eq!(user.id, alice_id);
    }

    // A change through node 2: every node knows the new role, and the token
    // issued before it is refused everywhere.
    let (status, _) = send(
        &pool.node(2).state,
        "PUT",
        &format!("/api/admin/users/{alice_id}"),
        Some(&root),
        r#"{"role":"Reader"}"#,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    for n in 1..=3 {
        let state = &pool.node(n).state;
        assert!(
            eventually(|| async {
                state.iam_storage.get_user(&alice_id).await.unwrap().unwrap().role == Role::Reader
            })
            .await,
            "node-{n} kept alice's old role"
        );
        let (status, _) = send(state, "GET", "/api/admin/users", Some(&alice_token), "").await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "node-{n}");
    }
}

/// A user deleted through node 3 leaves a tombstone of its one record on
/// every node, and no node finds it, its name or its key any more.
#[tokio::test]
async fn a_user_deleted_through_one_node_is_gone_from_every_node() {
    let pool = pool(22311).await;
    let iam = &pool.node(1).state.iam_storage;
    let bob = iam
        .create_user("bob".into(), "Bob-Pass-1234".into(), Role::Writer)
        .await
        .unwrap();
    let keys = iam.generate_s3_credentials(&bob.id).await.unwrap();
    let key = format!("{USER_KEY_PREFIX}{}", bob.id);
    for n in 1..=3 {
        let state = &pool.node(n).state;
        assert!(
            eventually(|| async {
                state
                    .iam_storage
                    .get_user_by_access_key(&keys.access_key)
                    .await
                    .unwrap()
                    .is_some()
            })
            .await
        );
    }

    pool.node(3).state.iam_storage.delete_user(&bob.id).await.unwrap();

    for n in 1..=3 {
        let state = &pool.node(n).state;
        assert!(
            eventually(|| async { state.iam_storage.get_user(&bob.id).await.unwrap().is_none() })
                .await,
            "node-{n} still knows bob"
        );
        assert!(state.iam_storage.get_user_by_username("bob").await.unwrap().is_none());
        assert!(state
            .iam_storage
            .get_user_by_access_key(&keys.access_key)
            .await
            .unwrap()
            .is_none());
        let engine = state.storage.read().await;
        let version = engine.key_version("__system__", &key).await.unwrap();
        assert!(
            version.record().is_some_and(|record| record.is_delete_marker),
            "node-{n} holds no tombstone of bob's record"
        );
    }
}

/// A name taken through one node is refused through another: every node
/// checks against its own copy, which the write reached.
#[tokio::test]
async fn a_name_taken_through_one_node_is_refused_through_the_others() {
    let pool = pool(22321).await;
    let (_, root) = login(&pool.node(1).state, "root", ROOT_PASSWORD).await;
    let body = r#"{"username":"dup_user","password":"Dup-Pass-12345","role":"Reader"}"#;
    let (status, created) = send(
        &pool.node(1).state,
        "POST",
        "/api/admin/users",
        Some(&root),
        body,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let id = created["id"].as_str().unwrap().to_string();

    for n in 2..=3 {
        let state = &pool.node(n).state;
        assert!(
            eventually(|| async { state.iam_storage.get_user(&id).await.unwrap().is_some() }).await
        );
        let (status, body) = send(
            state,
            "POST",
            "/api/admin/users",
            Some(&root),
            r#"{"username":"dup_user","password":"Other-Pass-999","role":"SuperUser"}"#,
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT, "node-{n}: {body}");
        assert_eq!(body["code"], "UserAlreadyExists");
    }
    for n in 1..=3 {
        let users = pool.node(n).state.iam_storage.list_users().await.unwrap();
        assert_eq!(
            users.iter().filter(|user| user.username == "dup_user").count(),
            1,
            "node-{n}"
        );
    }
}

/// With one node down a change still reaches a majority; with two down it is
/// refused with 503 and changes nothing, while credentials are still checked
/// against the remaining node's own copy.
#[tokio::test]
async fn a_change_needs_a_majority_of_the_pool() {
    let base_port = 22331;
    let mut pool = pool(base_port).await;
    let (_, root) = login(&pool.node(1).state, "root", ROOT_PASSWORD).await;
    pool.node(1)
        .state
        .iam_storage
        .create_user("carol".into(), "Carol-Pass-123".into(), Role::Writer)
        .await
        .unwrap();

    pool.stop(base_port, 3).await;
    let (status, _) = send(
        &pool.node(1).state,
        "POST",
        "/api/admin/users",
        Some(&root),
        r#"{"username":"dave","password":"Dave-Pass-1234","role":"Writer"}"#,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "two of three nodes are a majority");

    pool.stop(base_port, 2).await;
    let (status, body) = send(
        &pool.node(1).state,
        "POST",
        "/api/admin/users",
        Some(&root),
        r#"{"username":"erin","password":"Erin-Pass-1234","role":"Writer"}"#,
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    assert_eq!(body["code"], "ServiceUnavailable");
    assert_eq!(
        login(&pool.node(1).state, "carol", "Carol-Pass-123").await.0,
        StatusCode::OK
    );
    assert_eq!(
        login(&pool.node(1).state, "dave", "Dave-Pass-1234").await.0,
        StatusCode::OK
    );
}

/// A node that announced a protocol below the one of the cluster's control
/// state is not sent its records: it would keep them without a digest and
/// delete them without a tombstone. The other two are a majority.
#[tokio::test]
async fn a_node_of_an_earlier_protocol_is_not_sent_the_records() {
    let base_port = 22341;
    let pool = pool(base_port).await;
    pool.node(1).gossip.set_member_for_testing(
        NodeId(3),
        member(
            base_port,
            3,
            IAM_REPLICATION_MIN_PROTOCOL_VERSION - 1,
            NodeStatus::Alive,
        ),
    );

    let frank = pool
        .node(1)
        .state
        .iam_storage
        .create_user("frank".into(), "Frank-Pass-12".into(), Role::Writer)
        .await
        .unwrap();
    let key = format!("{USER_KEY_PREFIX}{}", frank.id);
    let held = |n: usize| {
        let storage = pool.node(n).state.storage.clone();
        let key = key.clone();
        async move { storage.read().await.head_object("__system__", &key).await.is_ok() }
    };
    assert!(held(1).await);
    assert!(held(2).await);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !held(3).await,
        "a node of an earlier protocol got the record"
    );
}

/// Root's state is the pool's: sessions of root revoked through node 1 end on
/// every node, and a node's start reads the state the pool holds.
#[tokio::test]
async fn root_sessions_revoked_through_one_node_end_on_every_node() {
    let pool = pool(22351).await;
    for n in 1..=3 {
        pool.node(n).state.auth_service.start_root().await.unwrap();
    }
    let (_, token) = login(&pool.node(2).state, "root", ROOT_PASSWORD).await;
    for n in 1..=3 {
        let (status, _) = send(
            &pool.node(n).state,
            "GET",
            "/api/admin/users",
            Some(&token),
            "",
        )
        .await;
        assert_eq!(status, StatusCode::OK, "node-{n}");
    }

    let root_id = RootAccount::id_for("root");
    let (status, _) = send(
        &pool.node(1).state,
        "POST",
        &format!("/api/admin/users/{root_id}/sessions/revoke"),
        Some(&token),
        "",
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    for n in 1..=3 {
        let state = &pool.node(n).state;
        assert!(
            eventually(|| async {
                state.iam_storage.root_state().await.unwrap().credential_version == 1
            })
            .await,
            "node-{n} kept root's old credential version"
        );
        let (status, _) = send(state, "GET", "/api/admin/users", Some(&token), "").await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "node-{n}");
    }

    // A restart of node 3 with the same configuration keeps the version.
    let (state, ended) = pool.node(3).state.auth_service.start_root().await.unwrap().unwrap();
    assert_eq!(state.credential_version, 1);
    assert!(!ended);
}

/// A new root password rolled through the nodes ends root's sessions once:
/// the first node restarted with it raises the pool's credential version, the
/// next finds the change recorded and raises nothing, and a restart with an
/// unchanged configuration reads only the node's own copy.
#[tokio::test]
async fn a_new_root_password_rolled_through_the_nodes_ends_root_sessions_once() {
    let pool = pool(22371).await;
    for n in 1..=3 {
        let (state, ended) = pool.node(n).state.auth_service.start_root().await.unwrap().unwrap();
        assert_eq!((state.credential_version, ended), (0, false), "node-{n}");
    }
    let (_, old_token) = login(&pool.node(1).state, "root", ROOT_PASSWORD).await;

    let new_root = RootAccount::new("root", "Root-New-Pass-678", JWT_SECRET).unwrap();
    let restarted =
        |n: usize| (*pool.node(n).state.auth_service).clone().with_root(new_root.clone());
    let (state, ended) = restarted(1).start_root().await.unwrap().unwrap();
    assert_eq!((state.credential_version, ended), (1, true));
    for n in 1..=3 {
        let state = &pool.node(n).state;
        assert!(
            eventually(|| async {
                state.iam_storage.root_state().await.unwrap().credential_version == 1
            })
            .await,
            "node-{n}"
        );
        let (status, _) = send(state, "GET", "/api/admin/users", Some(&old_token), "").await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "node-{n}");
    }

    let (state, ended) = restarted(2).start_root().await.unwrap().unwrap();
    assert_eq!((state.credential_version, ended), (1, false));
    let (state, ended) = restarted(1).start_root().await.unwrap().unwrap();
    assert_eq!((state.credential_version, ended), (1, false));
}

/// The directory of a node learns of a record a peer's write brought, not
/// only of its own changes: written straight into node 2's storage through
/// its adapter, as a replica handler does.
#[tokio::test]
async fn a_record_a_peer_wrote_is_in_the_directory_at_once() {
    let pool = pool(22361).await;
    let node = pool.node(2);
    let user = serde_json::json!({
        "id": "u-peer",
        "username": "grace",
        "password_hash": "x",
        "role": "Reader",
        "access_key": "S4AKGRACE",
        "secret_key": "s",
        "secret_key_hash": "h",
        "created_at": "2026-01-01T00:00:00Z",
        "updated_at": "2026-01-01T00:00:00Z",
        "is_active": true,
        "credential_version": 0
    });
    let adapter = SharedStorageAdapter::new(node.state.storage.clone())
        .with_iam_directory(Some(node.state.iam_storage.directory()));
    let metadata = HashMap::from([
        ("_s4_hlc_wall".to_string(), "1000".to_string()),
        ("_s4_hlc_logical".to_string(), "0".to_string()),
        ("_s4_hlc_node_id".to_string(), "1".to_string()),
    ]);
    // Load the directory first, so the record arrives as a change.
    assert!(node.state.iam_storage.list_users().await.unwrap().is_empty());
    adapter
        .put_object_outcome(
            "__system__",
            &format!("{USER_KEY_PREFIX}u-peer"),
            user.to_string().as_bytes(),
            "application/json",
            &metadata,
        )
        .await
        .unwrap();
    let found = node.state.iam_storage.get_user_by_access_key("S4AKGRACE").await.unwrap();
    assert_eq!(found.map(|user| user.id).as_deref(), Some("u-peer"));

    assert!(adapter
        .tombstone_object(
            "__system__",
            &format!("{USER_KEY_PREFIX}u-peer"),
            2000,
            0,
            1
        )
        .await
        .unwrap());
    assert!(node.state.iam_storage.get_user("u-peer").await.unwrap().is_none());
}

/// One name created at once through every node, again and again: exactly
/// one creation succeeds, the others are refused with `UserAlreadyExists`,
/// and every node finds the one that succeeded under the name once it has
/// caught up with the pool (plan CLUSTER-IAM-REPLICATION.md, decisions 3 and
/// 4). The winner waits for two nodes only: its copy to the third may arrive
/// after the next holder read that node under a later epoch, and is fenced
/// there — the node catches up from the others, not from a hint half a
/// minute later.
#[tokio::test]
async fn one_name_created_at_once_through_every_node_is_one_user() {
    let pool = pool(22381).await;
    for round in 0..8 {
        let name = format!("race_{round}");
        let attempt = |n: usize| {
            let iam = pool.node(n).state.iam_storage.clone();
            let name = name.clone();
            async move { iam.create_user(name, format!("Pass-of-node-{n}"), Role::Writer).await }
        };
        let (first, second, third) = tokio::join!(attempt(1), attempt(2), attempt(3));
        let results = [first, second, third];
        let created: Vec<_> = results.iter().filter_map(|r| r.as_ref().ok()).collect();
        assert_eq!(created.len(), 1, "round {round}: {results:?}");
        assert!(
            results
                .iter()
                .filter(|r| r.is_err())
                .all(|r| matches!(r, Err(s4_features::iam::IamError::UserAlreadyExists))),
            "round {round}: {results:?}"
        );
        let winner = created[0].id.clone();
        for n in 1..=3 {
            let iam = &pool.node(n).state.iam_storage;
            assert!(
                pool.settle_until(|| async {
                    iam.get_user_by_username(&name).await.unwrap().map(|u| u.id)
                        == Some(winner.clone())
                })
                .await,
                "round {round}: node-{n} does not find the created user under its name"
            );
        }
    }
}

/// A new password through node 1 and a new role through node 2, at once:
/// both confirmed changes hold, on every node — neither overwrote the other.
#[tokio::test]
async fn two_changes_of_one_user_through_two_nodes_both_hold() {
    let pool = pool(22391).await;
    for round in 0..5 {
        let user = pool
            .node(1)
            .state
            .iam_storage
            .create_user(
                format!("both_{round}"),
                "Old-Pass-1234".into(),
                Role::Writer,
            )
            .await
            .unwrap();
        let password = pool.node(1).state.iam_storage.update_user(
            &user.id,
            Some("New-Pass-1234".into()),
            None,
            None,
        );
        let role =
            pool.node(2)
                .state
                .iam_storage
                .update_user(&user.id, None, Some(Role::Reader), None);
        let (password, role) = tokio::join!(password, role);
        password.unwrap();
        role.unwrap();
        for n in 1..=3 {
            let iam = &pool.node(n).state.iam_storage;
            assert!(
                eventually(|| async {
                    let user = iam.get_user(&user.id).await.unwrap().unwrap();
                    user.role == Role::Reader
                        && iam.verify_password(&user.username, "New-Pass-1234").await.unwrap()
                        && user.credential_version == 2
                })
                .await,
                "round {round}: node-{n} lost one of the two changes"
            );
        }
    }
}

/// While node 1 holds the lease, a change through node 2 waits; past its
/// wait it is refused with 503 and `Retry-After`. Released, the change goes
/// through.
#[tokio::test]
async fn a_change_waits_for_the_lease_and_is_refused_with_retry_after() {
    let pool = pool_with_lease(
        22401,
        IamLeaseConfig {
            duration: Duration::from_secs(5),
            wait: Duration::from_millis(800),
        },
    )
    .await;
    let (_, root) = login(&pool.node(2).state, "root", ROOT_PASSWORD).await;
    let held = pool.node(1).iam_lease.acquire().await.unwrap();

    let started = std::time::Instant::now();
    let response = create_router(pool.node(2).state.clone())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/admin/users")
                .header("content-type", "application/json")
                .header("authorization", format!("Bearer {root}"))
                .body(Body::from(
                    r#"{"username":"waiter","password":"Waiter-Pass-1","role":"Writer"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(
        started.elapsed() >= Duration::from_millis(800),
        "it did not wait"
    );
    let retry_after: u64 = response
        .headers()
        .get("retry-after")
        .expect("Retry-After")
        .to_str()
        .unwrap()
        .parse()
        .unwrap();
    assert!((1..=5).contains(&retry_after), "{retry_after}");

    pool.node(1).iam_lease.release(held).await;
    let (status, body) = send(
        &pool.node(2).state,
        "POST",
        "/api/admin/users",
        Some(&root),
        r#"{"username":"waiter","password":"Waiter-Pass-1","role":"Writer"}"#,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

/// `GET /api/admin/iam/status` tells what the node holds: `starting` until
/// a majority agrees with its copy, then every node agreeing, the epoch of
/// the change through node 1 on each, and, while a change is under way, the
/// node holding the lease — as every node that granted it sees it.
#[tokio::test]
async fn the_iam_status_names_the_agreement_of_the_pool_and_the_lease_holder() {
    let pool = gated_pool(22451, Duration::from_secs(30)).await;
    let (_, root) = login(&pool.node(1).state, "root", ROOT_PASSWORD).await;
    let (status, before) = send(
        &pool.node(2).state,
        "GET",
        "/api/admin/iam/status",
        Some(&root),
        "",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{before}");
    assert_eq!(before["mode"], "cluster");
    assert_eq!(before["credentials"]["state"], "starting");
    assert_eq!(before["credentials"]["max_staleness_secs"], 30);
    assert_eq!(before["adoption"], "adopted");

    create_with_keys(&pool, &root, "stella").await;
    assert!(
        pool.settle_until(|| async {
            pool.nodes.iter().all(|node| node.freshness.check().is_ok())
        })
        .await
    );
    let mut epochs = Vec::new();
    for n in 1..=3 {
        let (status, body) = send(
            &pool.node(n).state,
            "GET",
            "/api/admin/iam/status",
            Some(&root),
            "",
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["node"], format!("node-{n}"));
        assert_eq!(body["credentials"]["state"], "serving", "node-{n}: {body}");
        assert_eq!(
            (body["agreeing"].as_u64(), body["majority"].as_u64()),
            (Some(3), Some(2))
        );
        let nodes = body["nodes"].as_array().unwrap();
        assert_eq!(nodes.len(), 3);
        assert!(nodes.iter().all(|node| node["agrees"] == true && node["epoch_behind_ms"] == 0));
        assert_eq!(nodes.iter().filter(|node| node["local"] == true).count(), 1);
        assert!(body["lease"]["max_epoch"].as_u64().unwrap() >= 2, "{body}");
        assert!(body["lease"]["held"].is_null(), "released: {body}");
        epochs.push(body["epoch"]["wall_ms"].as_u64().unwrap());
    }
    assert!(
        epochs.iter().all(|epoch| *epoch == epochs[0] && *epoch > 0),
        "{epochs:?}"
    );

    let held = pool.node(1).iam_lease.acquire().await.unwrap();
    for n in 1..=3 {
        let (_, body) = send(
            &pool.node(n).state,
            "GET",
            "/api/admin/iam/status",
            Some(&root),
            "",
        )
        .await;
        assert_eq!(
            body["lease"]["held"]["holder"], "node-1",
            "node-{n}: {body}"
        );
        assert_eq!(
            body["lease"]["held"]["epoch"].as_u64(),
            Some(held.fence().epoch)
        );
    }
    pool.node(1).iam_lease.release(held).await;

    let (_, reader) = {
        let (status, body) = send(
            &pool.node(1).state,
            "POST",
            "/api/admin/users",
            Some(&root),
            r#"{"username":"rita","password":"Rita-Pass-123","role":"Reader"}"#,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        login(&pool.node(1).state, "rita", "Rita-Pass-123").await
    };
    let (status, _) = send(
        &pool.node(1).state,
        "GET",
        "/api/admin/iam/status",
        Some(&reader),
        "",
    )
    .await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "a Reader is not shown the status"
    );
}

/// Node 1 takes the lease and stalls past it; node 2 takes the next epoch
/// and changes IAM. Node 1's write under its old epoch is refused by every
/// node — the two that took the later epoch, and node 1's own, whose grant
/// ran out — and lands nowhere.
#[tokio::test]
async fn a_holder_whose_lease_passed_on_writes_nothing() {
    let pool = pool_with_lease(
        22411,
        IamLeaseConfig {
            duration: Duration::from_secs(1),
            wait: Duration::from_secs(5),
        },
    )
    .await;
    let stale = pool.node(1).iam_lease.acquire().await.unwrap();
    let fence = stale.fence();
    // Paused: its grants run out, and node 2 changes IAM under the next epoch.
    tokio::time::sleep(Duration::from_millis(1_200)).await;
    pool.node(2)
        .state
        .iam_storage
        .create_user("fresh".into(), "Fresh-Pass-12".into(), Role::Writer)
        .await
        .unwrap();
    for n in 1..=3 {
        assert!(pool.node(n).keeper.max_epoch().await > fence.epoch || n == 1);
    }

    let key = format!("{USER_KEY_PREFIX}stale-write");
    let written = pool
        .node(1)
        .state
        .write_coordinator
        .as_ref()
        .unwrap()
        .write_fenced(
            "__system__",
            &key,
            b"{}",
            "application/json",
            &HashMap::new(),
            fence,
        )
        .await;
    assert!(written.is_err(), "the stale holder's write was confirmed");
    tokio::time::sleep(Duration::from_millis(300)).await;
    for n in 1..=3 {
        let storage = pool.node(n).state.storage.clone();
        assert!(
            storage.read().await.head_object("__system__", &key).await.is_err(),
            "node-{n} took the stale holder's write"
        );
    }
    drop(stale);
}

// ---------------------------------------------------------------------------
// Phase 4: credentials checked against the node's copy once a majority agrees
// ---------------------------------------------------------------------------

impl Pool {
    /// One round of gossip and catch-up: every node hears what each other
    /// node announces of its copy of IAM, then compares its own with them.
    fn gossip_round(&self) {
        for (i, from) in self.nodes.iter().enumerate() {
            let announced = from.copy.announcement();
            for (j, to) in self.nodes.iter().enumerate() {
                if i != j {
                    to.gossip.set_iam_announcement_for_testing(NodeId(i as u128 + 1), announced);
                }
            }
        }
        for node in &self.nodes {
            node.catch_up.tick();
        }
    }

    /// Gossip rounds until `check` holds, or a few seconds pass.
    async fn settle_until<F, Fut>(&self, check: F) -> bool
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = bool>,
    {
        for _ in 0..150 {
            self.gossip_round();
            if check().await {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
        false
    }

    /// How node `n` sees node `peer`: alive or dead.
    fn sees(&self, base_port: u16, n: usize, peer: usize, status: NodeStatus) {
        self.node(n).gossip.set_member_for_testing(
            NodeId(peer as u128),
            member(
                base_port,
                peer as u128,
                s4_cluster::PROTOCOL_VERSION,
                status,
            ),
        );
    }
}

/// Creates `username` through node 1 as root, with S3 credentials; its id
/// and access key.
async fn create_with_keys(pool: &Pool, root: &str, username: &str) -> (String, String) {
    let (status, created) = send(
        &pool.node(1).state,
        "POST",
        "/api/admin/users",
        Some(root),
        &format!(r#"{{"username":"{username}","password":"Pass-{username}-123","role":"Writer"}}"#),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{created}");
    let id = created["id"].as_str().unwrap().to_string();
    let (status, keys) = send(
        &pool.node(1).state,
        "POST",
        &format!("/api/admin/users/{id}/credentials"),
        Some(root),
        "",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{keys}");
    (id, keys["access_key"].as_str().unwrap().to_string())
}

/// Until a majority of the pool agrees with its copy, a node refuses IAM
/// credentials as unavailable — a login, a token, an access key — while
/// root works; once it agrees, they are checked. No node is asked anything
/// while the copies agree: gossip carries the comparison.
#[tokio::test]
async fn iam_credentials_wait_until_a_majority_agrees_with_the_copy() {
    let pool = gated_pool(22421, Duration::from_secs(30)).await;
    let (status, root) = login(&pool.node(1).state, "root", ROOT_PASSWORD).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "root logs in while the gate is closed"
    );
    let (id, access_key) = create_with_keys(&pool, &root, "alice").await;

    let node = &pool.node(1).state;
    assert_eq!(
        login(node, "alice", "Pass-alice-123").await.0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert!(matches!(
        node.iam_storage.get_user_by_access_key_with_secret(&access_key).await,
        Err(s4_features::iam::IamError::NotServing(
            s4_features::iam::NotServing::Starting
        ))
    ));
    let (status, _) = send(node, "GET", "/api/admin/users", Some(&root), "").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "root's token works while the gate is closed"
    );

    assert!(
        pool.settle_until(|| async {
            pool.nodes.iter().all(|node| node.freshness.check().is_ok())
        })
        .await,
        "the copies agree and every gate opens"
    );
    for n in 1..=3 {
        let state = &pool.node(n).state;
        let (status, token) = login(state, "alice", "Pass-alice-123").await;
        assert_eq!(status, StatusCode::OK, "node-{n}");
        let (status, _) = send(state, "POST", "/api/admin/logout", Some(&token), "").await;
        assert_eq!(
            status,
            StatusCode::NO_CONTENT,
            "node-{n} checks alice's token"
        );
        let user = state
            .iam_storage
            .get_user_by_access_key_with_secret(&access_key)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(user.id, id);
    }
}

/// A node that missed changes — a user created, another deleted — while the
/// others held it dead refuses IAM credentials until it has caught up with
/// them: the deleted user never logs in through it, and the created one
/// does once it agrees. Node 3 compares nothing before it comes back, as a
/// node that has just restarted.
#[tokio::test]
async fn a_node_that_missed_changes_catches_up_before_it_checks_credentials() {
    let base_port = 22431;
    let pool = gated_pool(base_port, Duration::from_secs(30)).await;
    let (_, root) = login(&pool.node(1).state, "root", ROOT_PASSWORD).await;
    let (erin, _) = create_with_keys(&pool, &root, "erin").await;
    assert!(
        eventually(|| async {
            pool.node(3).state.iam_storage.get_user(&erin).await.unwrap().is_some()
        })
        .await
    );

    // Node 3 is down for the others: changes go to nodes 1 and 2 only.
    pool.sees(base_port, 1, 3, NodeStatus::Dead);
    pool.sees(base_port, 2, 3, NodeStatus::Dead);
    let (dave, _) = create_with_keys(&pool, &root, "dave").await;
    let (status, _) = send(
        &pool.node(1).state,
        "DELETE",
        &format!("/api/admin/users/{erin}"),
        Some(&root),
        "",
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let lagging = &pool.node(3).state;
    assert!(lagging.iam_storage.get_user(&dave).await.unwrap().is_none());
    assert!(lagging.iam_storage.get_user(&erin).await.unwrap().is_some());
    assert_eq!(
        login(lagging, "erin", "Pass-erin-123").await.0,
        StatusCode::SERVICE_UNAVAILABLE,
        "the deleted user is not let in by a node that may not know of the delete"
    );

    pool.sees(base_port, 1, 3, NodeStatus::Alive);
    pool.sees(base_port, 2, 3, NodeStatus::Alive);
    assert!(
        pool.settle_until(|| async { pool.node(3).freshness.check().is_ok() }).await,
        "node 3 catches up and its gate opens"
    );
    assert!(lagging.iam_storage.get_user(&dave).await.unwrap().is_some());
    assert!(lagging.iam_storage.get_user(&erin).await.unwrap().is_none());
    assert_eq!(
        login(lagging, "dave", "Pass-dave-123").await.0,
        StatusCode::OK
    );
    assert_eq!(
        login(lagging, "erin", "Pass-erin-123").await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        pool.node(3).copy.announcement(),
        pool.node(1).copy.announcement(),
        "the copies agree"
    );
}

/// A node cut off from the majority checks IAM credentials for as long as
/// the staleness bound, then refuses them as unavailable — root still logs
/// in — and checks them again once the majority agrees with it.
#[tokio::test]
async fn a_node_cut_off_refuses_iam_credentials_past_the_staleness_bound() {
    let base_port = 22441;
    let pool = gated_pool(base_port, Duration::from_millis(600)).await;
    let (_, root) = login(&pool.node(1).state, "root", ROOT_PASSWORD).await;
    create_with_keys(&pool, &root, "alice").await;
    assert!(
        pool.settle_until(|| async {
            pool.nodes.iter().all(|node| node.freshness.check().is_ok())
        })
        .await
    );

    pool.sees(base_port, 1, 2, NodeStatus::Dead);
    pool.sees(base_port, 1, 3, NodeStatus::Dead);
    let cut_off = &pool.node(1).state;
    pool.gossip_round();
    assert_eq!(
        login(cut_off, "alice", "Pass-alice-123").await.0,
        StatusCode::OK,
        "within the bound the copy is trusted"
    );
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert_eq!(
        login(cut_off, "alice", "Pass-alice-123").await.0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        pool.node(1).freshness.check(),
        Err(s4_cluster::IamRefusal::Stale)
    );
    assert_eq!(
        login(cut_off, "root", ROOT_PASSWORD).await.0,
        StatusCode::OK
    );
    // This fixture drives catch-up manually: the wait and root's password
    // check ran without another round, so even the healthy peers are stale.
    assert_eq!(
        pool.node(2).freshness.check(),
        Err(s4_cluster::IamRefusal::Stale)
    );
    pool.gossip_round();
    // The healthy majority confirms again; the isolated node still cannot.
    assert_eq!(
        pool.node(1).freshness.check(),
        Err(s4_cluster::IamRefusal::Stale)
    );
    assert_eq!(
        login(&pool.node(2).state, "alice", "Pass-alice-123").await.0,
        StatusCode::OK
    );

    pool.sees(base_port, 1, 2, NodeStatus::Alive);
    pool.sees(base_port, 1, 3, NodeStatus::Alive);
    assert!(pool.settle_until(|| async { pool.node(1).freshness.check().is_ok() }).await);
    assert_eq!(
        login(&pool.node(1).state, "alice", "Pass-alice-123").await.0,
        StatusCode::OK
    );
}

// ---------------------------------------------------------------------------
// Phase 5: the pool takes over the IAM its nodes kept of their own
// ---------------------------------------------------------------------------

impl Pool {
    /// One look of every node's adoption worker, then every node hears what
    /// each announced: whether it keeps IAM of its own.
    async fn adoption_round(&self) {
        for node in &self.nodes {
            node.adoption.as_ref().expect("an adopting pool").tick().await;
        }
        for (i, from) in self.nodes.iter().enumerate() {
            // A test handle announces as node 0, whatever node it serves.
            let adopted = from.gossip.iam_adopted(&NodeId(0));
            for to in &self.nodes {
                to.gossip.set_iam_adopted_for_testing(NodeId(i as u128 + 1), adopted);
            }
        }
    }

    /// Adoption rounds until `check` holds, or a few seconds pass.
    async fn adopt_until<F, Fut>(&self, check: F) -> bool
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = bool>,
    {
        for _ in 0..150 {
            self.adoption_round().await;
            if check().await {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
        false
    }
}

/// A user record an earlier release stored on one node, with its indexes.
async fn store_own(state: &AppState, id: &str, username: &str, role: Role, minutes_ago: i64) {
    let at = chrono::Utc::now() - chrono::Duration::minutes(minutes_ago);
    let hasher = s4_features::iam::PasswordHasher::new();
    let has_keys = username != "root";
    let user = s4_features::iam::User {
        id: id.to_string(),
        username: username.to_string(),
        password_hash: hasher.hash_password(&format!("Pass-{id}-123")).unwrap(),
        role,
        access_key: has_keys.then(|| format!("S4AK{id}")),
        secret_key: has_keys.then(|| format!("secret-{id}")),
        secret_key_sealed: None,
        secret_key_hash: has_keys.then(|| hasher.hash_password(&format!("secret-{id}")).unwrap()),
        created_at: at,
        updated_at: at,
        is_active: true,
        credential_version: 0,
        created_epoch: 0,
    };
    let storage = state.storage.read().await;
    for (key, data) in [
        (
            format!("__s4_iam_user_{id}"),
            serde_json::to_vec(&user).unwrap(),
        ),
        (
            format!("__s4_iam_username_{username}"),
            id.as_bytes().to_vec(),
        ),
    ] {
        storage
            .put_object(
                s4_core::types::SYSTEM_BUCKET,
                &key,
                &data,
                "application/json",
                &HashMap::new(),
            )
            .await
            .unwrap();
    }
}

/// Three nodes of an earlier release: alice and bob on node 1, another bob
/// — a Reader — and carol on node 2, only a stored root without keys on node
/// 3. Until the operator decides, the nodes with users serve them as before,
/// and no node changes IAM; the report names every user with its nodes and
/// conflicts. A decision on a report the nodes no longer answer is refused.
/// `mode=majority` takes bob over — the bob created first keeps the name,
/// the other is renamed and disabled — and leaves alice and carol out: every
/// node drops what it kept of its own once the decision reaches it, the gates
/// open, and every node serves the same users and changes them.
#[tokio::test]
async fn the_pool_takes_over_what_its_nodes_kept_as_the_operator_decides() {
    let pool = adopting_pool(22501).await;
    store_own(&pool.node(1).state, "a-1", "alice", Role::Writer, 50).await;
    store_own(&pool.node(1).state, "b-1", "bob", Role::Writer, 40).await;
    store_own(&pool.node(2).state, "b-2", "bob", Role::Reader, 30).await;
    store_own(&pool.node(2).state, "c-2", "carol", Role::Writer, 20).await;
    store_own(&pool.node(3).state, "r-3", "root", Role::SuperUser, 60).await;
    for n in 1..=2 {
        pool.node(n).state.iam_storage.serve_legacy().await.unwrap();
    }
    let (status, root) = login(&pool.node(1).state, "root", ROOT_PASSWORD).await;
    assert_eq!(status, StatusCode::OK);

    // Before the decision.
    for _ in 0..3 {
        pool.adoption_round().await;
    }
    assert_eq!(
        login(&pool.node(1).state, "alice", "Pass-a-1-123").await.0,
        StatusCode::OK,
        "node 1 serves its own users as before"
    );
    for n in 1..=3 {
        let (status, body) = send(
            &pool.node(n).state,
            "POST",
            "/api/admin/users",
            Some(&root),
            r#"{"username":"dave","password":"Dave-Pass-123","role":"Writer"}"#,
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT, "node-{n}: {body}");
        assert_eq!(body["code"], "IamAdoptionPending");
    }
    let (status, report) = send(
        &pool.node(3).state,
        "GET",
        "/api/admin/iam/adoption",
        Some(&root),
        "",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{report}");
    assert_eq!(report["state"], "pending");
    assert_eq!(report["complete"], true);
    assert_eq!(report["node_serves_own_iam"], false);
    let names: Vec<&str> = report["users"]
        .as_array()
        .unwrap()
        .iter()
        .map(|name| name["username"].as_str().unwrap())
        .collect();
    assert_eq!(
        names,
        ["alice", "bob", "carol"],
        "the stored root is no user"
    );
    assert_eq!(report["users"][1]["on_majority"], true);
    assert_eq!(
        report["users"][1]["conflicts"],
        serde_json::json!(["ids", "roles"])
    );
    assert_eq!(report["users"][0]["nodes"], serde_json::json!(["node-1"]));
    assert!(
        !report.to_string().contains("secret-"),
        "no secret is reported"
    );
    let report_id = report["report_id"].as_str().unwrap().to_string();

    // A node's records change after the report was read.
    store_own(&pool.node(2).state, "e-2", "erin", Role::Reader, 10).await;
    let decide = |report_id: &str| format!(r#"{{"report_id":"{report_id}","mode":"majority"}}"#);
    let (status, body) = send(
        &pool.node(3).state,
        "POST",
        "/api/admin/iam/adoption",
        Some(&root),
        &decide(&report_id),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["code"], "IamAdoptionReportChanged");
    let (_, report) = send(
        &pool.node(3).state,
        "GET",
        "/api/admin/iam/adoption",
        Some(&root),
        "",
    )
    .await;
    let report_id = report["report_id"].as_str().unwrap().to_string();

    let (status, decided) = send(
        &pool.node(3).state,
        "POST",
        "/api/admin/iam/adoption",
        Some(&root),
        &decide(&report_id),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{decided}");
    let adopted: Vec<(&str, &str)> = decided["adopted"]
        .as_array()
        .unwrap()
        .iter()
        .map(|user| {
            (
                user["id"].as_str().unwrap(),
                user["username"].as_str().unwrap(),
            )
        })
        .collect();
    assert_eq!(adopted, [("b-1", "bob"), ("b-2", "bob~b-2")]);
    assert_eq!(decided["left_out"].as_array().unwrap().len(), 3);

    assert!(
        pool.adopt_until(|| async {
            pool.nodes.iter().all(|node| node.state.iam_storage.changes_open())
        })
        .await,
        "every node drops its own and opens IAM changes"
    );
    for n in 1..=3 {
        let state = &pool.node(n).state;
        assert!(
            eventually(|| async { state.iam_storage.legacy_records().await.unwrap().is_empty() })
                .await,
            "node-{n} dropped every record of an earlier release"
        );
        assert_eq!(
            login(state, "bob", "Pass-b-1-123").await.0,
            StatusCode::OK,
            "node-{n}"
        );
        for (username, password) in [
            ("bob", "Pass-b-2-123"),
            ("alice", "Pass-a-1-123"),
            ("carol", "Pass-c-2-123"),
            ("bob~b-2", "Pass-b-2-123"),
        ] {
            assert_eq!(
                login(state, username, password).await.0,
                StatusCode::UNAUTHORIZED,
                "node-{n}: {username}"
            );
        }
        let renamed = state.iam_storage.get_user_by_username("bob~b-2").await.unwrap().unwrap();
        assert!(!renamed.is_active);
        // Both secrets are sealed: b-1's on the node that kept it, b-2's on
        // the node it was sent from.
        for id in ["b-1", "b-2"] {
            let (record, _) = state
                .storage
                .read()
                .await
                .get_object(
                    s4_core::types::SYSTEM_BUCKET,
                    &format!("{USER_KEY_PREFIX}{id}"),
                )
                .await
                .unwrap();
            assert!(
                !String::from_utf8_lossy(&record).contains(&format!("secret-{id}")),
                "node-{n}: the secret of {id} is sealed"
            );
        }
        assert!(state
            .iam_storage
            .verify_s3_credentials("S4AKb-1", "secret-b-1")
            .await
            .unwrap()
            .is_some());
        let (_, report) = send(state, "GET", "/api/admin/iam/adoption", Some(&root), "").await;
        assert_eq!(report["state"], "adopted", "node-{n}");
        assert_eq!(report["decision"]["report_id"], report_id.as_str());
        assert!(report["users"].as_array().unwrap().is_empty());
    }

    let (status, body) = send(
        &pool.node(2).state,
        "POST",
        "/api/admin/users",
        Some(&root),
        r#"{"username":"dave","password":"Dave-Pass-123","role":"Writer"}"#,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (_, report) = send(
        &pool.node(1).state,
        "GET",
        "/api/admin/iam/adoption",
        Some(&root),
        "",
    )
    .await;
    let (status, body) = send(
        &pool.node(1).state,
        "POST",
        "/api/admin/iam/adoption",
        Some(&root),
        &decide(report["report_id"].as_str().unwrap()),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["code"], "IamAdoptionNotPending");

    // The decision is in the audit log of every node.
    for n in 1..=3 {
        let state = &pool.node(n).state;
        assert!(
            eventually(|| async {
                state
                    .iam_storage
                    .audit_page(None, 100)
                    .await
                    .unwrap()
                    .entries
                    .iter()
                    .any(|entry| {
                        entry.action == s4_features::iam::AuditAction::IamAdopted
                            && entry.through == "node-3"
                            && entry.details["mode"] == "majority"
                    })
            })
            .await,
            "node-{n}"
        );
    }
}

/// A pool whose nodes keep no IAM of their own refuses changes only until
/// every node announced so, then opens by itself; a node that does not
/// answer leaves a report incomplete, and a decision on it is refused.
#[tokio::test]
async fn a_pool_with_nothing_of_its_own_opens_by_itself() {
    let base_port = 22511;
    let mut pool = adopting_pool(base_port).await;
    let (_, root) = login(&pool.node(1).state, "root", ROOT_PASSWORD).await;
    let create = r#"{"username":"fay","password":"Fay-Pass-1234","role":"Writer"}"#;
    let (status, _) = send(
        &pool.node(1).state,
        "POST",
        "/api/admin/users",
        Some(&root),
        create,
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "before every node announced");

    assert!(
        pool.adopt_until(|| async {
            pool.nodes.iter().all(|node| node.state.iam_storage.changes_open())
        })
        .await
    );
    let (status, body) = send(
        &pool.node(1).state,
        "POST",
        "/api/admin/users",
        Some(&root),
        create,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (_, report) = send(
        &pool.node(2).state,
        "GET",
        "/api/admin/iam/adoption",
        Some(&root),
        "",
    )
    .await;
    assert_eq!(report["state"], "adopted");
    assert_eq!(report["complete"], true);
    assert!(report["decision"].is_null());

    // Node 3 stops answering: the report says so, and nothing is decided.
    pool.stop(base_port, 3).await;
    store_own(&pool.node(2).state, "g-2", "gus", Role::Writer, 5).await;
    let (_, report) = send(
        &pool.node(2).state,
        "GET",
        "/api/admin/iam/adoption",
        Some(&root),
        "",
    )
    .await;
    assert_eq!(report["complete"], false, "{report}");
    assert_eq!(report["nodes"][2]["answered"], false);
    let (status, body) = send(
        &pool.node(2).state,
        "POST",
        "/api/admin/iam/adoption",
        Some(&root),
        &format!(
            r#"{{"report_id":"{}","mode":"union"}}"#,
            report["report_id"].as_str().unwrap()
        ),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    assert_eq!(body["code"], "IamAdoptionIncomplete");
}

/// The secret of S3 credentials issued through one node is on every node
/// sealed, never in clear text, and every node opens it to check a
/// signature; the user records and the audit entries of the change are on
/// every node, the entries naming the node the change went through.
#[tokio::test]
async fn a_secret_is_sealed_on_every_node_and_every_change_is_audited_on_every_node() {
    let pool = pool(22701).await;
    let (status, root) = login(&pool.node(1).state, "root", ROOT_PASSWORD).await;
    assert_eq!(status, StatusCode::OK);
    let (status, user) = send(
        &pool.node(1).state,
        "POST",
        "/api/admin/users",
        Some(&root),
        r#"{"username":"sealy","password":"Sealy-Pass-123","role":"Writer"}"#,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{user}");
    let id = user["id"].as_str().unwrap().to_string();
    let (status, keys) = send(
        &pool.node(2).state,
        "POST",
        &format!("/api/admin/users/{id}/credentials"),
        Some(&root),
        "",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{keys}");
    let access_key = keys["access_key"].as_str().unwrap().to_string();
    let secret = keys["secret_key"].as_str().unwrap().to_string();

    for n in 1..=3 {
        let state = &pool.node(n).state;
        assert!(
            eventually(|| async {
                state
                    .iam_storage
                    .get_user_by_access_key_with_secret(&access_key)
                    .await
                    .ok()
                    .flatten()
                    .is_some_and(|user| user.secret_key.as_deref() == Some(secret.as_str()))
            })
            .await,
            "node-{n} opens the secret"
        );
        let (record, _) = state
            .storage
            .read()
            .await
            .get_object(
                s4_core::types::SYSTEM_BUCKET,
                &format!("{USER_KEY_PREFIX}{id}"),
            )
            .await
            .unwrap();
        assert!(
            !String::from_utf8_lossy(&record).contains(&secret),
            "node-{n} holds the secret sealed"
        );
        assert!(
            eventually(|| async {
                let log = state.iam_storage.audit_page(None, 100).await.unwrap();
                let made: Vec<(s4_features::iam::AuditAction, &str)> = log
                    .entries
                    .iter()
                    .map(|entry| (entry.action, entry.through.as_str()))
                    .collect();
                made == [
                    (s4_features::iam::AuditAction::UserCreated, "node-1"),
                    (s4_features::iam::AuditAction::CredentialsIssued, "node-2"),
                ] && log.entries.iter().all(|entry| entry.epoch > 0 && entry.actor.name == "root")
                    && log.entries[0].epoch < log.entries[1].epoch
            })
            .await,
            "node-{n} holds the audit log"
        );
    }
}
