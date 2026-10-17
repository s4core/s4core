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

//! Buckets in cluster mode, through the S3 and admin routes of node 1 of a
//! three-node pool (RF=3, W=R=2) whose other replicas are real
//! `NodeRpcServer`s on Bitcask: a bucket exists when a read quorum of its
//! marker's replicas says so, and nothing is written to, listed in or deleted
//! from a bucket that does not.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use s4_api::{create_router, AppState};
use s4_cluster::rpc::server::RpcServerHandle;
use s4_cluster::{
    BucketPlacement, ClusterConfig, ConfigReadGate, DistributedListCoordinator, Edition,
    GossipHandle, HintedHandoffConfig, HintedHandoffManager, HybridClock, IdempotencyTracker,
    ListTimeouts, NodeAddr, NodeCapacity, NodeClient, NodeClientConfig, NodeId, NodeMeta,
    NodeRpcServer, NodeState, NodeStatus, PlacementRouter, PoolConfig, PoolId,
    QuorumReadCoordinator, QuorumWriteCoordinator, ReadRepairService, ReadTimeouts,
    ReplicaReadHandler, ReplicaWriteHandler, RpcServerConfig, WriteTimeouts,
};
use s4_core::storage::engine::StorageEngine;
use s4_core::storage::BitcaskStorageEngine;
use s4_core::types::StorageClass;
use s4_features::iam::models::Role;
use tower::ServiceExt;

use super::SharedStorageAdapter;

/// Node 1's API over a pool whose replicas 2 and 3 run, or are held dead.
struct Pool {
    state: AppState,
    /// Storages of node 2 and node 3.
    peers: Vec<Arc<BitcaskStorageEngine>>,
    /// Node 1's view of the pool.
    gossip: Arc<GossipHandle>,
    _servers: Vec<RpcServerHandle>,
    _dirs: Vec<tempfile::TempDir>,
}

async fn bitcask(dir: &std::path::Path, version_digests: bool) -> BitcaskStorageEngine {
    BitcaskStorageEngine::open(
        dir.join("volumes"),
        dir.join("metadata_db"),
        10 * 1024 * 1024,
        4096,
        false,
        s4_core::storage::IndexOptions { version_digests },
    )
    .await
    .expect("storage engine")
}

/// S3 requests go unsigned, as in the API's own tests; the admin routes still
/// take a JWT.
static DISABLE_S3_AUTH: std::sync::Once = std::sync::Once::new();

async fn pool(base_port: u16, peers_run: bool) -> Pool {
    pool_with_digests(base_port, peers_run, false).await
}

async fn pool_with_digests(base_port: u16, peers_run: bool, version_digests: bool) -> Pool {
    DISABLE_S3_AUTH.call_once(|| std::env::set_var("S4_DISABLE_AUTH", "1"));
    let addr = |n: u128| NodeAddr {
        grpc_addr: format!("127.0.0.1:{}", base_port + n as u16).parse().unwrap(),
        http_addr: format!("127.0.0.1:{}", 9000 + n).parse().unwrap(),
        gossip_addr: format!("127.0.0.1:{}", 9200 + n).parse().unwrap(),
    };
    // The only pool of a node, as bootstrap configures it: every bucket is in it.
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
    let mut peers = Vec::new();
    let mut servers = Vec::new();
    for n in 2..=3u128 {
        let dir = tempfile::tempdir().unwrap();
        let storage = Arc::new(bitcask(dir.path(), version_digests).await);
        if version_digests {
            storage
                .index_db()
                .settle_tombstone_horizon(s4_core::storage::TombstoneHorizon {
                    hlc_timestamp: 0,
                    hlc_logical: 0,
                    origin_node_id: 0,
                })
                .await
                .unwrap();
        }
        dirs.push(dir);
        if peers_run {
            let server = NodeRpcServer::new(
                NodeId(n),
                Arc::new(meta(n)),
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
                bind_addr: addr(n).grpc_addr,
                ..Default::default()
            })
            .await
            .unwrap();
            servers.push(server);
        }
        peers.push(storage);
    }

    let dir = tempfile::tempdir().unwrap();
    let state = AppState::new(
        bitcask(dir.path(), version_digests).await,
        "test-access-key".to_string(),
        "test-secret-key".to_string(),
        dir.path(),
    )
    .await;
    let storage: Arc<dyn StorageEngine> =
        Arc::new(SharedStorageAdapter::new(state.storage.clone()));
    let members = (2..=3u128)
        .map(|n| {
            let status = if peers_run {
                NodeStatus::Alive
            } else {
                NodeStatus::Dead
            };
            (
                NodeId(n),
                NodeState {
                    meta: meta(n),
                    status,
                    last_seen: std::time::Instant::now(),
                },
            )
        })
        .collect();
    let gossip = Arc::new(GossipHandle::new_for_testing(members));
    let rpc_client = Arc::new(NodeClient::new(gossip.clone(), NodeClientConfig::default()));
    let resolver: HashMap<_, _> = (1..=3).map(|n| (addr(n).grpc_addr, NodeId(n))).collect();
    let router = Arc::new(
        PlacementRouter::new(&config, NodeId(1), BucketPlacement::new(), &resolver).unwrap(),
    );
    let clock = Arc::new(HybridClock::new(NodeId(1)));
    let hints = Arc::new(HintedHandoffManager::new(
        HintedHandoffConfig::new(dir.path()),
        rpc_client.clone(),
    ));
    let iam_lease = Arc::new(s4_cluster::IamLeaseHolder::new(
        NodeId(1),
        Arc::new(s4_cluster::IamLeaseKeeper::open(storage.clone()).await.unwrap()),
        rpc_client.clone(),
        router.clone(),
        clock.clone(),
        s4_cluster::IamLeaseConfig::default(),
    ));
    let write = QuorumWriteCoordinator::new(
        NodeId(1),
        storage.clone(),
        rpc_client.clone(),
        router.clone(),
        clock.clone(),
        hints,
        &config,
        WriteTimeouts::default(),
    );
    let read = QuorumReadCoordinator::new(
        NodeId(1),
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
    );
    let list = DistributedListCoordinator::new(
        NodeId(1),
        storage,
        rpc_client.clone(),
        router,
        config.epoch,
        ListTimeouts::default(),
    );
    // The pool adopted its configurations: they are read through the quorums.
    let state = state
        .with_cluster_coordinators(Arc::new(write), Arc::new(read), Arc::new(list), iam_lease)
        .with_config_read_gate(ConfigReadGate::opened());
    dirs.push(dir);
    Pool {
        state,
        peers,
        gossip,
        _servers: servers,
        _dirs: dirs,
    }
}

/// A publication waits for the owners gossip holds alive until they serve, and
/// for nobody else: an owner gossip holds alive whose gRPC does not listen is
/// waited for until the time runs out and named, one gossip holds dead is not
/// waited for at all.
#[tokio::test]
async fn a_publication_waits_for_the_owners_gossip_holds_alive_to_serve() {
    use crate::pool_generation::owners_serving;
    let up = pool(29960, true).await;
    let owners = [NodeId(1), NodeId(2), NodeId(3)];
    let started = std::time::Instant::now();
    assert!(
        owners_serving(&owners, NodeId(1), &up.gossip, Duration::from_secs(5))
            .await
            .is_empty()
    );
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "serving owners must not be waited for"
    );

    // An owner gossip holds alive at an address where nothing listens.
    let mut silent = up.gossip.get_node(&NodeId(3)).unwrap();
    silent.meta.addr.grpc_addr = "127.0.0.1:29999".parse().unwrap();
    up.gossip.set_member_for_testing(NodeId(3), silent.clone());
    let started = std::time::Instant::now();
    assert_eq!(
        owners_serving(&owners, NodeId(1), &up.gossip, Duration::from_millis(600)).await,
        vec![NodeId(3)]
    );
    assert!(
        started.elapsed() >= Duration::from_millis(600),
        "a silent owner is waited for"
    );

    // The same owner held dead is down: the quorum goes on without it.
    silent.status = NodeStatus::Dead;
    up.gossip.set_member_for_testing(NodeId(3), silent);
    let started = std::time::Instant::now();
    assert!(
        owners_serving(&owners, NodeId(1), &up.gossip, Duration::from_secs(5))
            .await
            .is_empty()
    );
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "a dead owner must not be waited for"
    );
}

#[tokio::test]
async fn pool_generation_is_published_with_an_hlc_to_the_old_write_quorum() {
    use s4_cluster::pool_generation::{object_key, PoolGenerationRecord};
    async fn publish(pool: &Pool, record: PoolGenerationRecord) {
        let (_shutdown, shutdown_rx) = tokio::sync::watch::channel(false);
        tokio::time::timeout(
            Duration::from_secs(10),
            crate::pool_generation::publish(
                record,
                pool.gossip.clone(),
                NodeId(1),
                pool.state.read_coordinator.clone().unwrap(),
                pool.state.write_coordinator.clone().unwrap(),
                shutdown_rx,
            ),
        )
        .await
        .unwrap();
    }
    let pool = pool(29930, true).await;
    let stable =
        PoolGenerationRecord::initial(PoolId(0), vec![NodeId(1), NodeId(2), NodeId(3)]).unwrap();
    let growing = stable.configure((1..=6).map(NodeId).collect()).unwrap();
    publish(&pool, stable.clone()).await;
    publish(&pool, growing.clone()).await;
    let key = object_key(PoolId(0));
    let result = pool
        .state
        .read_coordinator
        .as_ref()
        .unwrap()
        .read("__system__", &key)
        .await
        .unwrap();
    assert_eq!(PoolGenerationRecord::decode(&result.data).unwrap(), growing);
    assert!(!result.hlc.is_zero());
    let mut holders = 0;
    for peer in &pool.peers {
        if let Ok(object) = peer.get_object("__system__", &key).await {
            if PoolGenerationRecord::decode(&object.0).unwrap() == growing {
                holders += 1;
            }
        }
    }
    assert!(
        holders >= 1,
        "the local copy alone cannot confirm a write quorum of two"
    );
    publish(&pool, stable.clone()).await;
    publish(
        &pool,
        stable.configure((1..=9).map(NodeId).collect()).unwrap(),
    )
    .await;
    let result = pool
        .state
        .read_coordinator
        .as_ref()
        .unwrap()
        .read("__system__", &key)
        .await
        .unwrap();
    assert_eq!(
        PoolGenerationRecord::decode(&result.data).unwrap(),
        growing,
        "neither an old coordinator nor a conflicting growth may replace the pending successor"
    );
}

/// One request through node 1's router: the status and the body.
async fn send(
    state: &AppState,
    method: &str,
    uri: &str,
    headers: &[(&str, &str)],
    body: &str,
) -> (StatusCode, String) {
    let mut request = Request::builder().method(method).uri(uri);
    for (name, value) in headers {
        request = request.header(*name, *value);
    }
    let response = create_router(state.clone())
        .oneshot(request.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), 1 << 20).await.unwrap();
    (status, String::from_utf8_lossy(&bytes).into_owned())
}

/// One request of a table: method, URI, headers, and the body or the expected
/// S3 error code.
type Case<'a> = (&'a str, &'a str, &'a [(&'a str, &'a str)], &'a str);

/// The S3 error code of an answer.
fn code(body: &str) -> &str {
    body.split("<Code>")
        .nth(1)
        .and_then(|rest| rest.split("</Code>").next())
        .unwrap_or("")
}

const MARKER_PREFIX: &str = "__s4_bucket_marker_";

/// Whether every one of `storages` holds the key, given a moment for the
/// replica a write did not wait for.
async fn eventually_on_all(
    storages: &[Arc<BitcaskStorageEngine>],
    bucket: &str,
    key: &str,
) -> bool {
    for _ in 0..100 {
        let mut all = true;
        for storage in storages {
            all &= storage.head_object(bucket, key).await.is_ok();
        }
        if all {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    false
}

/// Whether none of `storages` holds the key, given the same moment.
async fn eventually_on_none(
    storages: &[Arc<BitcaskStorageEngine>],
    bucket: &str,
    key: &str,
) -> bool {
    for _ in 0..100 {
        let mut none = true;
        for storage in storages {
            none &= storage.head_object(bucket, key).await.is_err();
        }
        if none {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    false
}

/// The `Authorization` header of a SuperUser logged in on node 1.
///
/// The user is written to node 1's own copy of IAM, as if the pool had
/// confirmed it before any peer went down: a change of IAM needs a majority
/// of the pool, and the tests here are about buckets, some with both peers
/// down.
async fn admin_bearer(state: &AppState) -> String {
    let own_copy = s4_features::iam::IamStorage::new(Arc::new(SharedStorageAdapter::new(
        state.storage.clone(),
    )));
    let admin = own_copy
        .create_user("admin".into(), "password123".into(), Role::SuperUser)
        .await
        .unwrap();
    state.iam_storage.directory().refresh(&admin.id).await.unwrap();
    let (status, login) = send(
        state,
        "POST",
        "/api/admin/login",
        &[("content-type", "application/json")],
        r#"{"username":"admin","password":"password123"}"#,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{login}");
    let token: serde_json::Value = serde_json::from_str(&login).unwrap();
    format!("Bearer {}", token["token"].as_str().unwrap())
}

/// Writes `data` as `bucket/key` straight into one replica's storage, with
/// the HLC a coordinator would have given it: a copy the other replicas, node
/// 1 among them, never received.
async fn held_only_by(
    storage: &BitcaskStorageEngine,
    bucket: &str,
    key: &str,
    data: &[u8],
    wall: u64,
) {
    let metadata = HashMap::from([
        ("_s4_hlc_wall".to_string(), wall.to_string()),
        ("_s4_hlc_logical".to_string(), "0".to_string()),
        ("_s4_hlc_node_id".to_string(), "2".to_string()),
    ]);
    storage.put_object(bucket, key, data, "text/plain", &metadata).await.unwrap();
}

async fn node_1_holds(state: &AppState, bucket: &str, key: &str) -> bool {
    state.storage.read().await.head_object(bucket, key).await.is_ok()
}

/// Every request that writes to, lists or deletes from a bucket answers
/// `NoSuchBucket` when no quorum knows the bucket, through every path, and
/// leaves no trace of it on any replica: no object and no marker.
#[tokio::test]
async fn requests_into_a_missing_bucket_answer_no_such_bucket_and_write_nothing() {
    let pool = pool(19940, true).await;
    let state = &pool.state;
    assert_eq!(send(state, "PUT", "/real", &[], "").await.0, StatusCode::OK);
    assert_eq!(
        send(state, "PUT", "/real/source.txt", &[], "source").await.0,
        StatusCode::OK
    );

    let tagging = "<Tagging><TagSet><Tag><Key>k</Key><Value>v</Value></Tag></TagSet></Tagging>";
    let delete = "<Delete><Object><Key>a.txt</Key></Object></Delete>";
    let cors = "<CORSConfiguration><CORSRule><AllowedMethod>GET</AllowedMethod>\
                <AllowedOrigin>*</AllowedOrigin></CORSRule></CORSConfiguration>";
    let requests: [Case; 9] = [
        ("PUT", "/ghost/a.txt", &[], "payload"),
        (
            "PUT",
            "/ghost/copy.txt",
            &[("x-amz-copy-source", "/real/source.txt")],
            "",
        ),
        ("POST", "/ghost/upload.bin?uploads", &[], ""),
        ("PUT", "/ghost/a.txt?tagging", &[], tagging),
        ("DELETE", "/ghost/a.txt", &[], ""),
        ("POST", "/ghost?delete", &[], delete),
        ("GET", "/ghost?list-type=2", &[], ""),
        ("GET", "/ghost/a.txt", &[], ""),
        ("PUT", "/ghost?cors", &[], cors),
    ];
    for (method, uri, headers, body) in requests {
        let (status, answer) = send(state, method, uri, headers, body).await;
        assert_eq!(
            (status, code(&answer)),
            (StatusCode::NOT_FOUND, "NoSuchBucket"),
            "{method} {uri}: {answer}"
        );
    }

    let marker = format!("{MARKER_PREFIX}ghost");
    for key in ["a.txt", "copy.txt", "upload.bin"] {
        assert!(
            !node_1_holds(state, "ghost", key).await,
            "node 1 holds {key}"
        );
        assert!(
            eventually_on_none(&pool.peers, "ghost", key).await,
            "a replica holds {key}"
        );
    }
    assert!(!node_1_holds(state, "__system__", &marker).await);
    assert!(eventually_on_none(&pool.peers, "__system__", &marker).await);
    let (_, buckets) = send(state, "GET", "/", &[], "").await;
    assert!(buckets.contains("<Name>real</Name>"), "{buckets}");
    assert!(!buckets.contains("ghost"), "{buckets}");
}

/// A read of one object asks for its bucket only when it finds nothing: a
/// missing key in an existing bucket is `NoSuchKey`, any key of a missing
/// bucket `NoSuchBucket`, for GET and for the source of a CopyObject alike.
#[tokio::test]
async fn a_missing_key_is_no_such_key_and_a_missing_bucket_no_such_bucket() {
    let pool = pool(19944, true).await;
    let state = &pool.state;
    assert_eq!(send(state, "PUT", "/real", &[], "").await.0, StatusCode::OK);

    let cases: [Case; 4] = [
        ("GET", "/real/missing.txt", &[], "NoSuchKey"),
        ("GET", "/ghost/missing.txt", &[], "NoSuchBucket"),
        (
            "PUT",
            "/real/copy.txt",
            &[("x-amz-copy-source", "/real/missing.txt")],
            "NoSuchKey",
        ),
        (
            "PUT",
            "/real/copy.txt",
            &[("x-amz-copy-source", "/ghost/missing.txt")],
            "NoSuchBucket",
        ),
    ];
    for (method, uri, headers, expected) in cases {
        let (status, answer) = send(state, method, uri, headers, "").await;
        assert_eq!(
            (status, code(&answer)),
            (StatusCode::NOT_FOUND, expected),
            "{method} {uri} {headers:?}: {answer}"
        );
    }
}

/// A bucket created through node 1 is on every replica, and objects written
/// into it reach them all.
#[tokio::test]
async fn a_bucket_created_through_the_quorum_is_on_every_replica() {
    let pool = pool(19948, true).await;
    let state = &pool.state;
    assert_eq!(send(state, "PUT", "/real", &[], "").await.0, StatusCode::OK);
    assert!(eventually_on_all(&pool.peers, "__system__", &format!("{MARKER_PREFIX}real")).await);
    assert_eq!(
        send(state, "HEAD", "/real", &[], "").await.0,
        StatusCode::OK
    );
    assert_eq!(
        send(state, "PUT", "/real/object.txt", &[], "data").await.0,
        StatusCode::OK
    );
    assert!(eventually_on_all(&pool.peers, "real", "object.txt").await);
    assert!(node_1_holds(state, "real", "object.txt").await);
}

/// DeleteObjects deletes every key it names on every replica, as DeleteObject
/// deletes one. A delete on node 1 alone would leave two copies for a quorum
/// read to bring back.
#[tokio::test]
async fn delete_objects_deletes_on_every_replica() {
    let pool = pool(19952, true).await;
    let state = &pool.state;
    assert_eq!(send(state, "PUT", "/real", &[], "").await.0, StatusCode::OK);
    for key in ["a.txt", "b.txt", "kept.txt"] {
        assert_eq!(
            send(state, "PUT", &format!("/real/{key}"), &[], key).await.0,
            StatusCode::OK
        );
        assert!(eventually_on_all(&pool.peers, "real", key).await, "{key}");
    }

    let delete = "<Delete><Object><Key>a.txt</Key></Object><Object><Key>b.txt</Key></Object>\
                  </Delete>";
    let (status, answer) = send(state, "POST", "/real?delete", &[], delete).await;
    assert_eq!(status, StatusCode::OK, "{answer}");
    assert!(answer.contains("<Key>a.txt</Key>") && answer.contains("<Key>b.txt</Key>"));
    assert!(!answer.contains("<Error>"), "{answer}");

    for key in ["a.txt", "b.txt"] {
        assert!(
            eventually_on_none(&pool.peers, "real", key).await,
            "{key} left on a replica"
        );
        assert!(!node_1_holds(state, "real", key).await);
        let (status, answer) = send(state, "GET", &format!("/real/{key}"), &[], "").await;
        assert_eq!(
            (status, code(&answer)),
            (StatusCode::NOT_FOUND, "NoSuchKey")
        );
    }
    assert!(eventually_on_all(&pool.peers, "real", "kept.txt").await);
}

/// The admin API creates and deletes a bucket as the S3 API does, through the
/// quorums: the marker reaches every replica, a bucket with objects is not
/// deleted without `force`, and with it the objects and the marker leave every
/// replica.
#[tokio::test]
async fn admin_bucket_create_and_delete_go_through_the_quorums() {
    let pool = pool(19956, true).await;
    let state = &pool.state;
    let bearer = admin_bearer(state).await;
    let auth = [("authorization", bearer.as_str())];

    let (status, answer) = send(state, "PUT", "/api/admin/buckets/adm", &auth, "").await;
    assert_eq!(status, StatusCode::OK, "{answer}");
    let marker = format!("{MARKER_PREFIX}adm");
    assert!(eventually_on_all(&pool.peers, "__system__", &marker).await);
    assert_eq!(
        send(state, "PUT", "/api/admin/buckets/adm", &auth, "").await.0,
        StatusCode::CONFLICT
    );
    assert_eq!(
        send(state, "PUT", "/adm/object.txt", &[], "data").await.0,
        StatusCode::OK
    );
    assert!(eventually_on_all(&pool.peers, "adm", "object.txt").await);

    assert_eq!(
        send(state, "DELETE", "/api/admin/buckets/adm", &auth, "").await.0,
        StatusCode::CONFLICT
    );
    assert_eq!(
        send(
            state,
            "DELETE",
            "/api/admin/buckets/adm?force=true",
            &auth,
            ""
        )
        .await
        .0,
        StatusCode::NO_CONTENT
    );
    assert!(eventually_on_none(&pool.peers, "adm", "object.txt").await);
    assert!(eventually_on_none(&pool.peers, "__system__", &marker).await);
    assert!(!node_1_holds(state, "adm", "object.txt").await);
    assert_eq!(
        send(state, "HEAD", "/adm", &[], "").await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        send(state, "DELETE", "/api/admin/buckets/adm", &auth, "").await.0,
        StatusCode::NOT_FOUND
    );
}

/// With the other replicas dead, no read quorum can vouch for the bucket even
/// though node 1 holds its marker: a write is refused with 503 before it
/// starts, and nothing lands on node 1.
#[tokio::test]
async fn a_bucket_no_quorum_vouches_for_takes_no_write() {
    let pool = pool(19960, false).await;
    let state = &pool.state;
    state
        .storage
        .read()
        .await
        .put_object(
            "__system__",
            &format!("{MARKER_PREFIX}real"),
            b"1",
            "application/octet-stream",
            &HashMap::new(),
        )
        .await
        .unwrap();

    let (status, answer) = send(state, "PUT", "/real/object.txt", &[], "data").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{answer}");
    assert!(!node_1_holds(state, "real", "object.txt").await);
}

/// The statistics of the admin API are the pool's, not node 1's: a bucket
/// and an object only the other replicas hold are counted, and an object whose
/// replicas differ counts once, at the size of its newest version.
#[tokio::test]
async fn bucket_stats_count_the_pool_not_the_node() {
    let pool = pool(19964, true).await;
    let state = &pool.state;
    let bearer = admin_bearer(state).await;
    let auth = [("authorization", bearer.as_str())];
    assert_eq!(
        send(state, "PUT", "/stats", &[], "").await.0,
        StatusCode::OK
    );
    for (key, data) in [("a.txt", "aaaaa"), ("b.txt", "bbb")] {
        assert_eq!(
            send(state, "PUT", &format!("/stats/{key}"), &[], data).await.0,
            StatusCode::OK
        );
        assert!(eventually_on_all(&pool.peers, "stats", key).await, "{key}");
    }
    let later = u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
        + 60_000;
    held_only_by(&pool.peers[0], "stats", "only-on-2.txt", b"seven!!", later).await;
    held_only_by(&pool.peers[1], "stats", "a.txt", b"newer version", later).await;
    for peer in &pool.peers {
        held_only_by(
            peer,
            "__system__",
            &format!("{MARKER_PREFIX}remote"),
            b"1",
            later,
        )
        .await;
        held_only_by(peer, "remote", "x.bin", b"four", later).await;
    }

    let (status, answer) = send(state, "GET", "/api/admin/bucket-stats", &auth, "").await;
    assert_eq!(status, StatusCode::OK, "{answer}");
    let stats: serde_json::Value = serde_json::from_str(&answer).unwrap();
    let bucket = |name: &str| {
        stats["buckets"]
            .as_array()
            .unwrap()
            .iter()
            .find(|b| b["name"] == name)
            .cloned()
            .unwrap_or_else(|| panic!("{name} missing from {stats}"))
    };
    assert_eq!(bucket("stats")["objects_count"], 3, "{stats}");
    assert_eq!(bucket("stats")["storage_used_bytes"], 13 + 3 + 7, "{stats}");
    assert_eq!(bucket("remote")["objects_count"], 1, "{stats}");
    assert_eq!(bucket("remote")["storage_used_bytes"], 4, "{stats}");
    assert_eq!(stats["buckets"].as_array().unwrap().len(), 2, "{stats}");
    assert_eq!(stats["total_storage_bytes"], 27, "{stats}");
    assert_eq!(stats["complete"], true, "{stats}");
}

/// With the other replicas dead, node 1 can only count what it holds, and
/// the statistics say they are incomplete instead of passing for the pool's.
#[tokio::test]
async fn bucket_stats_say_when_a_node_did_not_answer() {
    let pool = pool(19968, false).await;
    let state = &pool.state;
    let bearer = admin_bearer(state).await;
    {
        let storage = state.storage.read().await;
        storage
            .put_object(
                "__system__",
                &format!("{MARKER_PREFIX}lonely"),
                b"1",
                "application/octet-stream",
                &HashMap::new(),
            )
            .await
            .unwrap();
        storage
            .put_object("lonely", "x.bin", b"abc", "text/plain", &HashMap::new())
            .await
            .unwrap();
    }

    let (status, answer) = send(
        state,
        "GET",
        "/api/admin/bucket-stats",
        &[("authorization", bearer.as_str())],
        "",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{answer}");
    let stats: serde_json::Value = serde_json::from_str(&answer).unwrap();
    assert_eq!(stats["buckets"][0]["name"], "lonely", "{stats}");
    assert_eq!(stats["buckets"][0]["objects_count"], 1, "{stats}");
    assert_eq!(stats["complete"], false, "{stats}");
}

/// Seconds since the epoch in milliseconds, a minute ahead: an HLC newer than
/// any write the test sent through node 1.
fn a_minute_ahead() -> u64 {
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap();
    u64::try_from(now.as_millis()).unwrap() + 60_000
}

/// `/api/stats` counts the pool's buckets and objects — those only the other
/// replicas hold too — and reports the node's own deduplication. The count is
/// reused for a few seconds, so an object written meanwhile shows up later,
/// but creating a bucket through the node counts again at once.
#[tokio::test]
async fn api_stats_count_the_pool_and_the_node_s_own_dedup() {
    let pool = pool(19972, true).await;
    let state = &pool.state;
    assert_eq!(
        send(state, "PUT", "/counted", &[], "").await.0,
        StatusCode::OK
    );
    for (key, data) in [("a.txt", "aaaaa"), ("b.txt", "bbb")] {
        assert_eq!(
            send(state, "PUT", &format!("/counted/{key}"), &[], data).await.0,
            StatusCode::OK
        );
        assert!(
            eventually_on_all(&pool.peers, "counted", key).await,
            "{key}"
        );
    }
    let later = a_minute_ahead();
    held_only_by(
        &pool.peers[0],
        "counted",
        "only-on-2.txt",
        b"seven!!",
        later,
    )
    .await;
    for peer in &pool.peers {
        held_only_by(
            peer,
            "__system__",
            &format!("{MARKER_PREFIX}remote"),
            b"1",
            later,
        )
        .await;
        held_only_by(peer, "remote", "x.bin", b"four", later).await;
    }

    let stats = |answer: (StatusCode, String)| {
        assert_eq!(answer.0, StatusCode::OK, "{}", answer.1);
        serde_json::from_str::<serde_json::Value>(&answer.1).unwrap()
    };
    let first = stats(send(state, "GET", "/api/stats", &[], "").await);
    assert_eq!(first["buckets_count"], 2, "{first}");
    assert_eq!(first["objects_count"], 4, "{first}");
    assert_eq!(first["storage_used_bytes"], 5 + 3 + 7 + 4, "{first}");
    assert_eq!(first["complete"], true, "{first}");
    let (unique, references) = state.storage.read().await.dedup_stats().await.unwrap();
    assert_eq!(first["dedup_unique_blobs"], unique, "{first}");
    assert_eq!(first["dedup_total_references"], references, "{first}");

    assert_eq!(
        send(state, "PUT", "/counted/c.txt", &[], "cc").await.0,
        StatusCode::OK
    );
    let kept = stats(send(state, "GET", "/api/stats", &[], "").await);
    assert_eq!(
        kept["objects_count"], 4,
        "counted again within the reuse period: {kept}"
    );

    assert_eq!(
        send(state, "PUT", "/fresh", &[], "").await.0,
        StatusCode::OK
    );
    let recounted = stats(send(state, "GET", "/api/stats", &[], "").await);
    assert_eq!(recounted["buckets_count"], 3, "{recounted}");
    assert_eq!(recounted["objects_count"], 5, "{recounted}");
}

/// The admin API lists a bucket from every node of the pool: an object only
/// another replica holds is listed, so is a bucket only the other replicas
/// know, and a bucket no quorum knows is `NoSuchBucket`.
#[tokio::test]
async fn admin_listing_lists_the_pool() {
    let pool = pool(19976, true).await;
    let state = &pool.state;
    let bearer = admin_bearer(state).await;
    let auth = [("authorization", bearer.as_str())];
    assert_eq!(
        send(state, "PUT", "/browse", &[], "").await.0,
        StatusCode::OK
    );
    assert_eq!(
        send(state, "PUT", "/browse/a.txt", &[], "a").await.0,
        StatusCode::OK
    );
    assert!(eventually_on_all(&pool.peers, "browse", "a.txt").await);
    let later = a_minute_ahead();
    held_only_by(&pool.peers[0], "browse", "only-on-2.txt", b"two", later).await;
    for peer in &pool.peers {
        held_only_by(
            peer,
            "__system__",
            &format!("{MARKER_PREFIX}remote"),
            b"1",
            later,
        )
        .await;
        held_only_by(peer, "remote", "x.bin", b"four", later).await;
    }

    let keys = |answer: &serde_json::Value| -> Vec<String> {
        answer["objects"]
            .as_array()
            .unwrap()
            .iter()
            .map(|object| object["key"].as_str().unwrap().to_string())
            .collect()
    };
    let (status, answer) = send(state, "GET", "/api/admin/buckets/browse/objects", &auth, "").await;
    assert_eq!(status, StatusCode::OK, "{answer}");
    let answer: serde_json::Value = serde_json::from_str(&answer).unwrap();
    assert_eq!(keys(&answer), ["a.txt", "only-on-2.txt"], "{answer}");
    assert_eq!(answer["complete"], true, "{answer}");

    let (status, answer) = send(state, "GET", "/api/admin/buckets/remote/objects", &auth, "").await;
    assert_eq!(status, StatusCode::OK, "{answer}");
    let answer: serde_json::Value = serde_json::from_str(&answer).unwrap();
    assert_eq!(keys(&answer), ["x.bin"], "{answer}");

    let (status, answer) = send(state, "GET", "/api/admin/buckets/ghost/objects", &auth, "").await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{answer}");
    assert!(answer.contains("NoSuchBucket"), "{answer}");
}

/// Cluster mode writes and deletes objects with no versions and no retention
/// checks, so every request that turns on versioning or Object Lock answers
/// `501 NotImplemented` instead of a `200` nothing would keep, and leaves
/// nothing behind: no bucket, no object, no configuration on any replica.
#[tokio::test]
async fn versioning_and_object_lock_are_refused_and_write_nothing() {
    let pool = pool(19980, true).await;
    let state = &pool.state;
    assert_eq!(send(state, "PUT", "/real", &[], "").await.0, StatusCode::OK);
    assert_eq!(
        send(state, "PUT", "/real/source.txt", &[], "source").await.0,
        StatusCode::OK
    );

    let versioning = "<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>";
    let suspended = "<VersioningConfiguration><Status>Suspended</Status></VersioningConfiguration>";
    let lock = "<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled>\
                </ObjectLockConfiguration>";
    let retention = "<Retention><Mode>COMPLIANCE</Mode>\
                     <RetainUntilDate>2099-01-01T00:00:00Z</RetainUntilDate></Retention>";
    let hold = "<LegalHold><Status>ON</Status></LegalHold>";
    let compliance: &[(&str, &str)] = &[
        ("x-amz-object-lock-mode", "COMPLIANCE"),
        (
            "x-amz-object-lock-retain-until-date",
            "2099-01-01T00:00:00Z",
        ),
    ];
    let requests: [Case; 12] = [
        ("PUT", "/real?versioning", &[], versioning),
        ("PUT", "/real?versioning", &[], suspended),
        ("PUT", "/real?object-lock", &[], lock),
        (
            "PUT",
            "/locked",
            &[("x-amz-bucket-object-lock-enabled", "true")],
            "",
        ),
        ("PUT", "/real/locked.txt", compliance, "payload"),
        (
            "PUT",
            "/real/held.txt",
            &[("x-amz-object-lock-legal-hold", "ON")],
            "payload",
        ),
        (
            "PUT",
            "/real/copy.txt",
            &[
                ("x-amz-copy-source", "/real/source.txt"),
                ("x-amz-object-lock-legal-hold", "ON"),
            ],
            "",
        ),
        ("POST", "/real/upload.bin?uploads", compliance, ""),
        ("PUT", "/real/source.txt?retention", &[], retention),
        ("GET", "/real/source.txt?retention", &[], ""),
        ("PUT", "/real/source.txt?legal-hold", &[], hold),
        ("GET", "/real/source.txt?legal-hold", &[], ""),
    ];
    for (method, uri, headers, body) in requests {
        let (status, answer) = send(state, method, uri, headers, body).await;
        assert_eq!(
            (status, code(&answer)),
            (StatusCode::NOT_IMPLEMENTED, "NotImplemented"),
            "{method} {uri} {headers:?}: {answer}"
        );
        assert!(
            answer.contains("not supported in cluster mode"),
            "{method} {uri}: {answer}"
        );
    }

    for (bucket, key) in [
        ("__system__", format!("{MARKER_PREFIX}locked")),
        ("__system__", "__s4_bucket_versioning_real".to_string()),
        ("__system__", "__s4_bucket_object_lock_real".to_string()),
        ("__system__", "__s4_bucket_versioning_locked".to_string()),
        ("__system__", "__s4_bucket_object_lock_locked".to_string()),
        ("real", "locked.txt".to_string()),
        ("real", "held.txt".to_string()),
        ("real", "copy.txt".to_string()),
        ("real", "upload.bin".to_string()),
    ] {
        assert!(
            !node_1_holds(state, bucket, &key).await,
            "node 1 holds {bucket}/{key}"
        );
        assert!(
            eventually_on_none(&pool.peers, bucket, &key).await,
            "a replica holds {bucket}/{key}"
        );
    }
    assert_eq!(
        send(state, "HEAD", "/locked", &[], "").await.0,
        StatusCode::NOT_FOUND
    );

    // Without Object Lock headers the same writes go on as before.
    assert_eq!(
        send(state, "PUT", "/plain", &[], "").await.0,
        StatusCode::OK
    );
    assert_eq!(
        send(state, "PUT", "/real/plain.txt", &[], "payload").await.0,
        StatusCode::OK
    );
}

/// A node that stored versioning and Object Lock before cluster mode refused
/// them answers as cluster mode behaves: versioning never turned on, no
/// Object Lock configuration. A missing bucket is still `NoSuchBucket`.
#[tokio::test]
async fn stored_versioning_and_object_lock_read_as_never_turned_on() {
    let pool = pool(19984, true).await;
    let state = &pool.state;
    assert_eq!(send(state, "PUT", "/old", &[], "").await.0, StatusCode::OK);
    let lock = s4_features::object_lock::object_lock_to_xml(
        &s4_features::object_lock::ObjectLockConfiguration {
            object_lock_enabled: true,
            default_retention: None,
        },
    );
    for (key, value) in [
        ("__s4_bucket_versioning_old", "Enabled"),
        ("__s4_bucket_object_lock_old", lock.as_str()),
    ] {
        state
            .storage
            .read()
            .await
            .put_object(
                "__system__",
                key,
                value.as_bytes(),
                "text/plain",
                &HashMap::new(),
            )
            .await
            .unwrap();
    }

    let (status, answer) = send(state, "GET", "/old?versioning", &[], "").await;
    assert_eq!(status, StatusCode::OK, "{answer}");
    assert!(answer.contains("<VersioningConfiguration"), "{answer}");
    assert!(!answer.contains("<Status>"), "{answer}");

    let (status, answer) = send(state, "GET", "/old?object-lock", &[], "").await;
    assert_eq!(
        (status, code(&answer)),
        (
            StatusCode::NOT_FOUND,
            "ObjectLockConfigurationNotFoundError"
        ),
        "{answer}"
    );

    for uri in ["/ghost?versioning", "/ghost?object-lock"] {
        let (status, answer) = send(state, "GET", uri, &[], "").await;
        assert_eq!(
            (status, code(&answer)),
            (StatusCode::NOT_FOUND, "NoSuchBucket"),
            "{uri}: {answer}"
        );
    }
}

/// DeleteBucket leaves a tombstone of each of the bucket's six configuration
/// keys on every replica, set or not, and a configuration write older than
/// the delete — one that missed it — does not bring the setting back.
#[tokio::test]
async fn delete_bucket_leaves_tombstones_of_its_configurations() {
    let pool = pool(19988, true).await;
    let state = &pool.state;
    assert_eq!(send(state, "PUT", "/gone", &[], "").await.0, StatusCode::OK);
    let cors_key = "__s4_bucket_cors_gone";
    for peer in &pool.peers {
        held_only_by(peer, "__system__", cors_key, b"<CORSConfiguration/>", 10).await;
    }
    assert_eq!(
        send(state, "DELETE", "/gone", &[], "").await.0,
        StatusCode::NO_CONTENT
    );

    let mut storages: Vec<&BitcaskStorageEngine> = pool.peers.iter().map(Arc::as_ref).collect();
    let node_1 = state.storage.read().await;
    storages.push(&node_1);
    for key in s4_core::types::BUCKET_CONFIG_KEY_PREFIXES.map(|prefix| format!("{prefix}gone")) {
        for (n, storage) in storages.iter().enumerate() {
            let mut tombstone = false;
            for _ in 0..100 {
                if matches!(
                    storage.key_version("__system__", &key).await.unwrap(),
                    s4_core::KeyVersion::Tombstone(_)
                ) {
                    tombstone = true;
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            assert!(tombstone, "storage {n} holds no tombstone of {key}");
        }
    }

    held_only_by(
        &pool.peers[0],
        "__system__",
        cors_key,
        b"<CORSConfiguration/>",
        20,
    )
    .await;
    assert!(matches!(
        pool.peers[0].key_version("__system__", cors_key).await.unwrap(),
        s4_core::KeyVersion::Tombstone(_)
    ));
}

/// One configuration of a table: the query, a body to put, what the GET
/// of it contains, the status of the PUT, and the error code once it is gone.
struct Setting {
    query: &'static str,
    body: &'static str,
    shows: &'static str,
    put_status: StatusCode,
    missing: &'static str,
}

/// CORS, lifecycle, policy and encryption: the configurations cluster mode
/// stores through the quorums.
fn settings() -> [Setting; 4] {
    [
        Setting {
            query: "cors",
            body: "<CORSConfiguration><CORSRule><AllowedMethod>GET</AllowedMethod>\
                   <AllowedOrigin>https://one.example</AllowedOrigin></CORSRule>\
                   </CORSConfiguration>",
            shows: "https://one.example",
            put_status: StatusCode::OK,
            missing: "NoSuchCORSConfiguration",
        },
        Setting {
            query: "lifecycle",
            body: "<LifecycleConfiguration><Rule><ID>logs</ID><Status>Enabled</Status>\
                   <Filter><Prefix>logs/</Prefix></Filter><Expiration><Days>7</Days>\
                   </Expiration></Rule></LifecycleConfiguration>",
            shows: "<Days>7</Days>",
            put_status: StatusCode::OK,
            missing: "NoSuchLifecycleConfiguration",
        },
        Setting {
            query: "policy",
            body: r#"{"Version":"2012-10-17","Statement":[{"Sid":"one"}]}"#,
            shows: r#""Sid":"one""#,
            put_status: StatusCode::NO_CONTENT,
            missing: "NoSuchBucketPolicy",
        },
        Setting {
            query: "encryption",
            body: "<ServerSideEncryptionConfiguration><Rule><ApplyServerSideEncryptionByDefault>\
                   <SSEAlgorithm>aws:kms</SSEAlgorithm></ApplyServerSideEncryptionByDefault>\
                   </Rule></ServerSideEncryptionConfiguration>",
            shows: "aws:kms",
            put_status: StatusCode::OK,
            missing: "ServerSideEncryptionConfigurationNotFoundError",
        },
    ]
}

/// The `__system__` key of a configuration of `bucket`.
fn setting_key(query: &str, bucket: &str) -> String {
    format!("__s4_bucket_{query}_{bucket}")
}

/// Each configuration written through node 1 lands on every replica; one
/// another node wrote later — held by the other replicas only — is what node
/// 1 reads; a delete through node 1 leaves a tombstone on every replica, and
/// the configuration is gone.
#[tokio::test]
async fn a_configuration_is_written_read_and_deleted_through_the_quorums() {
    let pool = pool(19992, true).await;
    let state = &pool.state;
    assert_eq!(send(state, "PUT", "/conf", &[], "").await.0, StatusCode::OK);

    for setting in settings() {
        let uri = format!("/conf?{}", setting.query);
        let key = setting_key(setting.query, "conf");
        let (status, answer) = send(state, "PUT", &uri, &[], setting.body).await;
        assert_eq!(status, setting.put_status, "{uri}: {answer}");
        assert!(node_1_holds(state, "__system__", &key).await, "{key}");
        assert!(
            eventually_on_all(&pool.peers, "__system__", &key).await,
            "{key}"
        );
        let (status, answer) = send(state, "GET", &uri, &[], "").await;
        assert_eq!(status, StatusCode::OK, "{uri}: {answer}");
        assert!(answer.contains(setting.shows), "{uri}: {answer}");

        for peer in &pool.peers {
            held_only_by(
                peer,
                "__system__",
                &key,
                b"written elsewhere",
                a_minute_ahead(),
            )
            .await;
        }
        let (status, answer) = send(state, "GET", &uri, &[], "").await;
        assert_eq!(
            (status, answer.as_str()),
            (StatusCode::OK, "written elsewhere"),
            "{uri}"
        );

        assert_eq!(
            send(state, "DELETE", &uri, &[], "").await.0,
            StatusCode::NO_CONTENT,
            "{uri}"
        );
        let (status, answer) = send(state, "GET", &uri, &[], "").await;
        assert_eq!(
            (status, code(&answer)),
            (StatusCode::NOT_FOUND, setting.missing),
            "{uri}: {answer}"
        );
        for (n, peer) in pool.peers.iter().enumerate() {
            let mut tombstone = false;
            for _ in 0..100 {
                if matches!(
                    peer.key_version("__system__", &key).await.unwrap(),
                    s4_core::KeyVersion::Tombstone(_)
                ) {
                    tombstone = true;
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            assert!(tombstone, "replica {} holds no tombstone of {key}", n + 2);
        }
    }

    for setting in settings() {
        let uri = format!("/missing?{}", setting.query);
        for method in ["GET", "PUT", "DELETE"] {
            let (status, answer) = send(state, method, &uri, &[], setting.body).await;
            assert_eq!(
                (status, code(&answer)),
                (StatusCode::NOT_FOUND, "NoSuchBucket"),
                "{method} {uri}: {answer}"
            );
        }
        let key = setting_key(setting.query, "missing");
        assert!(!node_1_holds(state, "__system__", &key).await, "{key}");
        assert!(
            eventually_on_none(&pool.peers, "__system__", &key).await,
            "{key}"
        );
    }
}

/// A configuration older than the bucket's marker was left by an earlier
/// bucket of the same name and reads as not set; a bucket deleted and
/// created again starts without the configurations of the first one.
#[tokio::test]
async fn a_bucket_created_again_starts_without_the_old_configuration() {
    let pool = pool(19996, true).await;
    let state = &pool.state;
    assert_eq!(
        send(state, "PUT", "/again", &[], "").await.0,
        StatusCode::OK
    );
    for peer in &pool.peers {
        held_only_by(peer, "__system__", "__s4_bucket_cors_again", b"<old/>", 10).await;
    }
    let (status, answer) = send(state, "GET", "/again?cors", &[], "").await;
    assert_eq!(
        (status, code(&answer)),
        (StatusCode::NOT_FOUND, "NoSuchCORSConfiguration"),
        "{answer}"
    );

    for setting in settings() {
        let uri = format!("/again?{}", setting.query);
        assert_eq!(
            send(state, "PUT", &uri, &[], setting.body).await.0,
            setting.put_status,
            "{uri}"
        );
    }
    assert_eq!(
        send(state, "DELETE", "/again", &[], "").await.0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        send(state, "PUT", "/again", &[], "").await.0,
        StatusCode::OK
    );
    for setting in settings() {
        let uri = format!("/again?{}", setting.query);
        let (status, answer) = send(state, "GET", &uri, &[], "").await;
        assert_eq!(
            (status, code(&answer)),
            (StatusCode::NOT_FOUND, setting.missing),
            "{uri}: {answer}"
        );
    }
}

/// PutBucketX that checked the bucket before DeleteBucket and wrote after
/// it, deterministically: the write step of the handler — a quorum write of
/// the key — runs after the delete, once through node 1 and once as a node
/// whose clock runs a minute ahead wrote it. The deleted bucket has no
/// configuration to show, and the bucket created again under its name does
/// not inherit either.
#[tokio::test]
async fn a_configuration_written_after_delete_bucket_is_not_inherited() {
    let pool = pool(20000, true).await;
    let state = &pool.state;
    assert_eq!(
        send(state, "PUT", "/raced", &[], "").await.0,
        StatusCode::OK
    );
    assert_eq!(
        send(state, "DELETE", "/raced", &[], "").await.0,
        StatusCode::NO_CONTENT
    );

    let write = state.write_coordinator.as_ref().unwrap();
    write
        .write(
            "__system__",
            "__s4_bucket_cors_raced",
            settings()[0].body.as_bytes(),
            "application/xml",
            &HashMap::new(),
        )
        .await
        .unwrap();
    for peer in &pool.peers {
        held_only_by(
            peer,
            "__system__",
            "__s4_bucket_lifecycle_raced",
            settings()[1].body.as_bytes(),
            a_minute_ahead(),
        )
        .await;
    }
    let (status, answer) = send(state, "GET", "/raced?cors", &[], "").await;
    assert_eq!(
        (status, code(&answer)),
        (StatusCode::NOT_FOUND, "NoSuchBucket"),
        "{answer}"
    );

    assert_eq!(
        send(state, "PUT", "/raced", &[], "").await.0,
        StatusCode::OK
    );
    for (query, missing) in [
        ("cors", "NoSuchCORSConfiguration"),
        ("lifecycle", "NoSuchLifecycleConfiguration"),
    ] {
        let (status, answer) = send(state, "GET", &format!("/raced?{query}"), &[], "").await;
        assert_eq!(
            (status, code(&answer)),
            (StatusCode::NOT_FOUND, missing),
            "{query}: {answer}"
        );
    }

    // The new bucket takes a configuration of its own as any other.
    let (status, answer) = send(state, "PUT", "/raced?cors", &[], settings()[0].body).await;
    assert_eq!(status, StatusCode::OK, "{answer}");
    let (status, answer) = send(state, "GET", "/raced?cors", &[], "").await;
    assert_eq!(status, StatusCode::OK, "{answer}");
}

/// With the other replicas dead no quorum can vouch for the bucket or the
/// configuration: each request answers 503, and none writes on node 1.
#[tokio::test]
async fn a_configuration_no_quorum_vouches_for_answers_503() {
    let pool = pool(20004, false).await;
    let state = &pool.state;
    let node_1 = state.storage.read().await;
    node_1
        .put_object(
            "__system__",
            &format!("{MARKER_PREFIX}alone"),
            b"1",
            "application/octet-stream",
            &HashMap::new(),
        )
        .await
        .unwrap();
    node_1
        .put_object(
            "__system__",
            "__s4_bucket_policy_alone",
            b"{}",
            "application/json",
            &HashMap::new(),
        )
        .await
        .unwrap();
    drop(node_1);

    for setting in settings() {
        let uri = format!("/alone?{}", setting.query);
        for method in ["PUT", "GET", "DELETE"] {
            let (status, answer) = send(state, method, &uri, &[], setting.body).await;
            assert_eq!(
                status,
                StatusCode::SERVICE_UNAVAILABLE,
                "{method} {uri}: {answer}"
            );
        }
    }
    for query in ["cors", "lifecycle", "encryption"] {
        let key = setting_key(query, "alone");
        assert!(!node_1_holds(state, "__system__", &key).await, "{key}");
    }
    assert!(node_1_holds(state, "__system__", "__s4_bucket_policy_alone").await);
}

/// Until the pool adopted its configurations a configuration a node holds
/// itself is what it answers, as an earlier release did — the bucket is still
/// checked through the quorum — unless a replica holds a newer value; one it
/// does not hold is read through the quorums. Once the gate opens, every one
/// is read through the quorums.
#[tokio::test]
async fn configurations_are_read_through_the_quorums_once_the_gate_opens() {
    let pool = pool(20008, true).await;
    let gate = ConfigReadGate::default();
    let state = &pool.state.clone().with_config_read_gate(gate.clone());
    assert_eq!(
        send(state, "PUT", "/gated", &[], "").await.0,
        StatusCode::OK
    );
    state
        .storage
        .read()
        .await
        .put_object(
            "__system__",
            "__s4_bucket_cors_gated",
            b"<local/>",
            "application/xml",
            &HashMap::new(),
        )
        .await
        .unwrap();

    let (status, answer) = send(state, "GET", "/gated?cors", &[], "").await;
    assert_eq!((status, answer.as_str()), (StatusCode::OK, "<local/>"));
    // Set again through the quorums since, on the other replicas.
    for peer in &pool.peers {
        held_only_by(
            peer,
            "__system__",
            "__s4_bucket_cors_gated",
            b"<quorum/>",
            a_minute_ahead(),
        )
        .await;
    }
    let (status, answer) = send(state, "GET", "/gated?cors", &[], "").await;
    assert_eq!((status, answer.as_str()), (StatusCode::OK, "<quorum/>"));
    // Adoption moved node 1's copy of the policy to the replicas of its key.
    for peer in &pool.peers {
        held_only_by(
            peer,
            "__system__",
            "__s4_bucket_policy_gated",
            b"{\"moved\":true}",
            a_minute_ahead(),
        )
        .await;
    }
    let (status, answer) = send(state, "GET", "/gated?policy", &[], "").await;
    assert_eq!(
        (status, answer.as_str()),
        (StatusCode::OK, "{\"moved\":true}")
    );
    let (status, answer) = send(state, "GET", "/gated?encryption", &[], "").await;
    assert_eq!(
        (status, code(&answer)),
        (
            StatusCode::NOT_FOUND,
            "ServerSideEncryptionConfigurationNotFoundError"
        ),
        "{answer}"
    );
    let (status, answer) = send(state, "GET", "/missing?cors", &[], "").await;
    assert_eq!(
        (status, code(&answer)),
        (StatusCode::NOT_FOUND, "NoSuchBucket"),
        "{answer}"
    );

    gate.open();
    let (status, answer) = send(state, "GET", "/gated?cors", &[], "").await;
    assert_eq!((status, answer.as_str()), (StatusCode::OK, "<quorum/>"));
}

/// The gate opens only when every node of the pool announced in gossip that
/// it adopted its configurations: this node by its own announcement, the
/// others by their latest metadata. A node gossip holds dead, or one that
/// announced a protocol older than 6, has not; none of it asks a peer.
#[tokio::test]
async fn the_pool_has_adopted_when_every_node_announced_it() {
    let up = pool(20012, true).await;
    // The handle's own node is node 0: node 1 of the harness reads as a peer.
    let nodes = [NodeId(0), NodeId(2), NodeId(3)];
    let adopted = || crate::config_adoption::pool_adopted(&nodes, NodeId(0), &up.gossip);
    assert!(!adopted());
    up.gossip.announce_bucket_configs_adopted();
    assert!(!adopted());
    up.gossip.set_bucket_configs_adopted_for_testing(NodeId(2), true);
    assert!(!adopted());
    up.gossip.set_bucket_configs_adopted_for_testing(NodeId(3), true);
    assert!(adopted());

    // A node of protocol 5 keeps configurations on single nodes.
    let mut older = up.gossip.get_node(&NodeId(3)).unwrap();
    older.meta.protocol_version = 5;
    up.gossip.set_member_for_testing(NodeId(3), older);
    assert!(!adopted());

    // A restart that has not adopted yet announces so.
    let mut restarted = up.gossip.get_node(&NodeId(3)).unwrap();
    restarted.meta.protocol_version = s4_cluster::PROTOCOL_VERSION;
    up.gossip.set_member_for_testing(NodeId(3), restarted);
    assert!(adopted());
    up.gossip.set_bucket_configs_adopted_for_testing(NodeId(3), false);
    assert!(!adopted());

    let down = pool(20016, false).await;
    down.gossip.announce_bucket_configs_adopted();
    for peer in [NodeId(2), NodeId(3)] {
        down.gossip.set_bucket_configs_adopted_for_testing(peer, true);
    }
    assert!(
        !crate::config_adoption::pool_adopted(&nodes, NodeId(0), &down.gossip),
        "nodes gossip holds dead"
    );
}

fn adoption_worker_pass(pool: &Pool) -> s4_cluster::ConfigAdoption {
    adoption_worker_with_source(pool, pool.state.read_coordinator.clone().unwrap())
}

fn adoption_worker_with_source(
    pool: &Pool,
    source: Arc<dyn s4_cluster::KeyStateSource>,
) -> s4_cluster::ConfigAdoption {
    let storage: Arc<dyn StorageEngine> =
        Arc::new(SharedStorageAdapter::new(pool.state.storage.clone()));
    s4_cluster::ConfigAdoption::new(
        storage,
        s4_cluster::Reconciler::new(
            source,
            Arc::new(NodeClient::new(
                pool.gossip.clone(),
                NodeClientConfig::default(),
            )),
            pool.gossip.clone(),
            NodeId(1),
            1,
        ),
        NodeId(1),
    )
}

/// Failure to persist completion cannot announce successful adoption.
#[tokio::test]
async fn adoption_worker_does_not_announce_when_completion_cannot_be_saved() {
    let pool = pool(20100, false).await;
    // This fixture deliberately opens its index without version digests:
    // durable completion returns a real storage error.
    let index = pool.state.storage.read().await.index_db().clone();
    let (passes, mut observed) = tokio::sync::watch::channel(0u64);
    let (shutdown, shutdown_rx) = tokio::sync::watch::channel(false);
    let status = s4_cluster::ConfigAdoptionStatus::default();
    let worker = tokio::spawn(crate::config_adoption::adopt(
        adoption_worker_pass(&pool),
        index,
        pool.gossip.clone(),
        status.clone(),
        passes,
        shutdown_rx,
    ));
    tokio::time::timeout(Duration::from_secs(5), observed.changed())
        .await
        .unwrap()
        .unwrap();
    // Let the completion attempt finish, rather than racing its announcement.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let announced = pool.gossip.bucket_configs_adopted(&NodeId(0));
    shutdown.send(true).unwrap_or(());
    worker.await.unwrap();
    assert!(
        !announced,
        "failed durable completion must keep adoption pending"
    );
    assert_eq!(
        status.snapshot().phase,
        s4_cluster::ConfigAdoptionPhase::Pending
    );
    assert_eq!(status.snapshot().pending_records, Some(0));
    assert!(status.snapshot().reasons[0].contains("save adoption completion"));
}

async fn adoption_worker_index() -> (tempfile::TempDir, Arc<s4_core::storage::IndexDb>) {
    let dir = tempfile::tempdir().unwrap();
    let index = Arc::new(
        s4_core::storage::IndexDb::open(
            &dir.path().join("index"),
            s4_core::storage::IndexOptions {
                version_digests: true,
            },
        )
        .unwrap(),
    );
    (dir, index)
}

async fn adoption_worker_cors(pool: &Pool) {
    assert_eq!(
        send(&pool.state, "PUT", "/adopting", &[], "").await.0,
        StatusCode::OK
    );
    pool.state
        .storage
        .read()
        .await
        .put_object(
            "__system__",
            "__s4_bucket_cors_adopting",
            b"<single/>",
            "application/xml",
            &HashMap::new(),
        )
        .await
        .unwrap();
}

fn adoption_worker_protocol(pool: &Pool, protocol: u32) {
    for node in [NodeId(2), NodeId(3)] {
        let mut member = pool.gossip.get_node(&node).unwrap();
        member.meta.protocol_version = protocol;
        pool.gossip.set_member_for_testing(node, member);
    }
}

#[tokio::test]
async fn adoption_worker_retries_pending_cors_when_owner_metadata_arrives() {
    let pool = pool_with_digests(20104, true, true).await;
    adoption_worker_cors(&pool).await;
    adoption_worker_protocol(&pool, 0);
    let (_dir, index) = adoption_worker_index().await;
    index.mark_bucket_config_quorum_reads().await.unwrap();
    assert!(!adoption_worker_pass(&pool).after_quorum_reads().pass().await.is_complete());
    let (passes, mut observed) = tokio::sync::watch::channel(0u64);
    let (shutdown, shutdown_rx) = tokio::sync::watch::channel(false);
    let status = s4_cluster::ConfigAdoptionStatus::default();
    assert_eq!(
        status.snapshot().phase,
        s4_cluster::ConfigAdoptionPhase::Starting
    );
    let mut worker = tokio::spawn(crate::config_adoption::adopt(
        adoption_worker_pass(&pool),
        index.clone(),
        pool.gossip.clone(),
        status.clone(),
        passes,
        shutdown_rx,
    ));
    tokio::time::timeout(Duration::from_secs(5), observed.changed())
        .await
        .unwrap()
        .unwrap();
    assert!(
        !pool.gossip.bucket_configs_adopted(&NodeId(0)),
        "pending CORS cannot be ready"
    );
    assert_eq!(
        status.snapshot().phase,
        s4_cluster::ConfigAdoptionPhase::Pending
    );
    assert_eq!(status.snapshot().pending_records, Some(1));
    adoption_worker_protocol(&pool, s4_cluster::PROTOCOL_VERSION);
    let completed = tokio::time::timeout(Duration::from_secs(5), &mut worker).await;
    shutdown.send(true).unwrap_or(());
    let finished_early = completed.is_ok();
    if completed.is_err() {
        worker.await.unwrap();
    }
    assert!(
        finished_early,
        "owner metadata must wake adoption before its 30-second timer"
    );
    assert!(index.bucket_configs_adopted().await.unwrap());
    assert_eq!(
        status.snapshot().phase,
        s4_cluster::ConfigAdoptionPhase::Ready
    );
    for peer in &pool.peers {
        assert_eq!(
            peer.get_object("__system__", "__s4_bucket_cors_adopting").await.unwrap().0,
            b"<single/>"
        );
    }
}

/// Holds the first failed owner read in flight so metadata can arrive before
/// the first pass ends. Later reads use the real quorum coordinator.
struct AdoptionOwnerRead {
    inner: Arc<QuorumReadCoordinator>,
    first: std::sync::atomic::AtomicBool,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

#[async_trait::async_trait]
impl s4_cluster::KeyStateSource for AdoptionOwnerRead {
    async fn newest_version(
        &self,
        bucket: &str,
        key: &str,
        target: NodeId,
    ) -> Result<s4_cluster::repair::reconcile::NewestVersion, s4_cluster::ClusterError> {
        self.inner.newest_version(bucket, key, target).await
    }

    async fn key_versions(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<s4_cluster::repair::reconcile::KeyVersions, s4_cluster::ClusterError> {
        if !self.first.swap(true, std::sync::atomic::Ordering::SeqCst) {
            self.entered.notify_one();
            self.release.notified().await;
            return Err(s4_cluster::ClusterError::Unavailable(
                "owner metadata not available when the read started".into(),
            ));
        }
        self.inner.key_versions(bucket, key).await
    }

    async fn read_copy(
        &self,
        bucket: &str,
        key: &str,
        node: NodeId,
    ) -> Result<s4_cluster::repair::reconcile::StoredCopy, s4_cluster::ClusterError> {
        self.inner.read_copy(bucket, key, node).await
    }
}

#[tokio::test]
async fn adoption_worker_preserves_metadata_arriving_during_a_pass() {
    let pool = pool_with_digests(20108, true, true).await;
    adoption_worker_cors(&pool).await;
    let (_dir, index) = adoption_worker_index().await;
    let source = Arc::new(AdoptionOwnerRead {
        inner: pool.state.read_coordinator.clone().unwrap(),
        first: std::sync::atomic::AtomicBool::new(false),
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
    });
    let (passes, mut observed) = tokio::sync::watch::channel(0u64);
    let (shutdown, shutdown_rx) = tokio::sync::watch::channel(false);
    let status = s4_cluster::ConfigAdoptionStatus::default();
    assert_eq!(
        status.snapshot().phase,
        s4_cluster::ConfigAdoptionPhase::Starting
    );
    let mut worker = tokio::spawn(crate::config_adoption::adopt(
        adoption_worker_with_source(&pool, source.clone()),
        index.clone(),
        pool.gossip.clone(),
        status.clone(),
        passes,
        shutdown_rx,
    ));
    tokio::time::timeout(Duration::from_secs(5), source.entered.notified())
        .await
        .unwrap();
    let mut member = pool.gossip.get_node(&NodeId(2)).unwrap();
    member.meta.protocol_version += 1;
    pool.gossip.set_member_for_testing(NodeId(2), member);
    source.release.notify_one();
    tokio::time::timeout(Duration::from_secs(5), observed.changed())
        .await
        .unwrap()
        .unwrap();
    let completed = tokio::time::timeout(Duration::from_secs(5), &mut worker).await;
    shutdown.send(true).unwrap_or(());
    if completed.is_err() {
        worker.await.unwrap();
    }
    completed
        .expect("metadata received during the failed pass must cause another pass")
        .expect("the adoption worker must complete without panicking");
    assert!(index.bucket_configs_adopted().await.unwrap());
    assert_eq!(
        status.snapshot().phase,
        s4_cluster::ConfigAdoptionPhase::Ready
    );
}

#[tokio::test]
async fn adoption_worker_empty_directory_and_trusted_restart_are_ready_without_peers() {
    let pool = pool(20112, false).await;
    let (dir, index) = adoption_worker_index().await;
    let status = s4_cluster::ConfigAdoptionStatus::default();
    let (passes, observed) = tokio::sync::watch::channel(0u64);
    let (_shutdown, shutdown_rx) = tokio::sync::watch::channel(false);
    tokio::time::timeout(
        Duration::from_secs(5),
        crate::config_adoption::adopt(
            adoption_worker_pass(&pool),
            index.clone(),
            pool.gossip.clone(),
            status.clone(),
            passes,
            shutdown_rx,
        ),
    )
    .await
    .unwrap();
    assert_eq!(
        status.snapshot().phase,
        s4_cluster::ConfigAdoptionPhase::Ready
    );
    assert_eq!(*observed.borrow(), 1);
    assert!(index.bucket_configs_adopted().await.unwrap());
    index.seal_version_digests().unwrap();
    drop(index);
    let index = Arc::new(
        s4_core::storage::IndexDb::open(
            &dir.path().join("index"),
            s4_core::storage::IndexOptions {
                version_digests: true,
            },
        )
        .unwrap(),
    );
    assert!(index.bucket_configs_adopted().await.unwrap());

    // A trusted completion must not scan storage or contact unavailable peers.
    let _locked = pool.state.storage.write().await;
    let restarted = s4_cluster::ConfigAdoptionStatus::default();
    let (passes, observed) = tokio::sync::watch::channel(0u64);
    let (_shutdown, shutdown_rx) = tokio::sync::watch::channel(false);
    tokio::time::timeout(
        Duration::from_secs(1),
        crate::config_adoption::adopt(
            adoption_worker_pass(&pool),
            index,
            pool.gossip.clone(),
            restarted.clone(),
            passes,
            shutdown_rx,
        ),
    )
    .await
    .unwrap();
    assert_eq!(
        restarted.snapshot().phase,
        s4_cluster::ConfigAdoptionPhase::Ready
    );
    assert_eq!(*observed.borrow(), 1);
    // The returned worker cannot restart work when peer metadata changes.
    adoption_worker_protocol(&pool, 0);
    assert_eq!(*observed.borrow(), 1);
}

#[tokio::test]
async fn adoption_worker_coalesces_owner_changes_and_limits_pass_frequency() {
    let pool = pool(20116, false).await;
    for n in 0..7 {
        pool.state
            .storage
            .read()
            .await
            .put_object(
                "__system__",
                &format!("__s4_bucket_cors_pending{n}"),
                b"<pending/>",
                "application/xml",
                &HashMap::new(),
            )
            .await
            .unwrap();
    }
    let (_dir, index) = adoption_worker_index().await;
    let (passes, mut observed) = tokio::sync::watch::channel(0u64);
    let (shutdown, shutdown_rx) = tokio::sync::watch::channel(false);
    let status = s4_cluster::ConfigAdoptionStatus::default();
    let started = tokio::time::Instant::now();
    let worker = tokio::spawn(crate::config_adoption::adopt(
        adoption_worker_pass(&pool),
        index.clone(),
        pool.gossip.clone(),
        status.clone(),
        passes,
        shutdown_rx,
    ));
    tokio::time::timeout(Duration::from_secs(5), observed.changed())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status.snapshot().pending_records, Some(7));
    assert_eq!(status.snapshot().reasons.len(), 5);
    for protocol in 10..110 {
        adoption_worker_protocol(&pool, protocol);
    }
    tokio::time::timeout(Duration::from_secs(5), observed.changed())
        .await
        .unwrap()
        .unwrap();
    assert!(
        started.elapsed() >= Duration::from_secs(1),
        "pass starts must be at least a second apart"
    );
    assert_eq!(
        *observed.borrow_and_update(),
        2,
        "a metadata burst coalesces into one retry"
    );
    // Allow the one-second rate limiter and another second for a real pass
    // to finish: identical metadata must not trigger a third pass.
    adoption_worker_protocol(&pool, 109);
    assert!(tokio::time::timeout(Duration::from_secs(2), observed.changed()).await.is_err());
    assert!(!pool.gossip.bucket_configs_adopted(&NodeId(0)));
    assert!(!index.bucket_configs_adopted().await.unwrap());
    shutdown.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(1), worker).await.unwrap().unwrap();
}

#[tokio::test]
async fn adoption_worker_shutdown_cancels_an_in_flight_pass() {
    let pool = pool_with_digests(20120, true, true).await;
    adoption_worker_cors(&pool).await;
    let (_dir, index) = adoption_worker_index().await;
    let source = Arc::new(AdoptionOwnerRead {
        inner: pool.state.read_coordinator.clone().unwrap(),
        first: std::sync::atomic::AtomicBool::new(false),
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
    });
    let (passes, observed) = tokio::sync::watch::channel(0u64);
    let (shutdown, shutdown_rx) = tokio::sync::watch::channel(false);
    let status = s4_cluster::ConfigAdoptionStatus::default();
    let worker = tokio::spawn(crate::config_adoption::adopt(
        adoption_worker_with_source(&pool, source.clone()),
        index.clone(),
        pool.gossip.clone(),
        status.clone(),
        passes,
        shutdown_rx,
    ));
    tokio::time::timeout(Duration::from_secs(5), source.entered.notified())
        .await
        .unwrap();
    shutdown.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(1), worker).await.unwrap().unwrap();
    assert_eq!(*observed.borrow(), 0);
    assert_eq!(
        status.snapshot().phase,
        s4_cluster::ConfigAdoptionPhase::Starting
    );
    assert!(!index.bucket_configs_adopted().await.unwrap());
    assert!(!pool.gossip.bucket_configs_adopted(&NodeId(0)));
}

/// The local legacy copy is evidence from a former owner, never confirmation
/// from one of the current owners (nodes 2 and 3).
struct ForeignAdoptionSource(Arc<QuorumReadCoordinator>);

#[async_trait::async_trait]
impl s4_cluster::KeyStateSource for ForeignAdoptionSource {
    async fn newest_version(
        &self,
        bucket: &str,
        key: &str,
        target: NodeId,
    ) -> Result<s4_cluster::repair::reconcile::NewestVersion, s4_cluster::ClusterError> {
        self.0.newest_version(bucket, key, target).await
    }
    async fn key_versions(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<s4_cluster::repair::reconcile::KeyVersions, s4_cluster::ClusterError> {
        let mut versions = self.0.key_versions(bucket, key).await?;
        versions.replicas.retain(|node| *node != NodeId(1));
        versions.answers.retain(|(node, _)| *node != NodeId(1));
        versions.unanswered.retain(|(node, _)| *node != NodeId(1));
        versions.confirming_replicas.retain(|node| *node != NodeId(1));
        Ok(versions)
    }
    async fn read_copy(
        &self,
        bucket: &str,
        key: &str,
        node: NodeId,
    ) -> Result<s4_cluster::repair::reconcile::StoredCopy, s4_cluster::ClusterError> {
        self.0.read_copy(bucket, key, node).await
    }
}

#[tokio::test]
async fn adoption_worker_does_not_resurrect_a_collected_delete() {
    let pool = pool_with_digests(20124, true, true).await;
    adoption_worker_cors(&pool).await;
    let key = "__s4_bucket_cors_adopting";
    let record = pool.state.storage.read().await.get_object("__system__", key).await.unwrap().1;
    let deleted = s4_cluster::HlcTimestamp::of_record(&record).wall_time + 1;
    for peer in &pool.peers {
        assert!(peer.tombstone_object("__system__", key, deleted, 0, 2).await.unwrap());
        assert_eq!(
            peer.gc_tombstones(&[s4_core::storage::TombstoneRef {
                full_key: format!("__system__/{key}"),
                hlc: (deleted, 0, 2),
            }])
            .await
            .unwrap(),
            1
        );
    }
    let (_dir, index) = adoption_worker_index().await;
    index.mark_bucket_config_quorum_reads().await.unwrap();
    let source = Arc::new(ForeignAdoptionSource(
        pool.state.read_coordinator.clone().unwrap(),
    ));
    let (passes, mut observed) = tokio::sync::watch::channel(0u64);
    let (shutdown, shutdown_rx) = tokio::sync::watch::channel(false);
    let status = s4_cluster::ConfigAdoptionStatus::default();
    let worker = tokio::spawn(crate::config_adoption::adopt(
        adoption_worker_with_source(&pool, source),
        index.clone(),
        pool.gossip.clone(),
        status.clone(),
        passes,
        shutdown_rx,
    ));
    tokio::time::timeout(Duration::from_secs(5), observed.changed())
        .await
        .unwrap()
        .unwrap();
    shutdown.send(true).unwrap();
    worker.await.unwrap();
    assert_eq!(
        status.snapshot().phase,
        s4_cluster::ConfigAdoptionPhase::Pending
    );
    assert_eq!(status.snapshot().pending_records, Some(1));
    assert!(status.snapshot().reasons[0].contains("collected delete"));
    assert!(!index.bucket_configs_adopted().await.unwrap());
    assert!(!pool.gossip.bucket_configs_adopted(&NodeId(0)));
    assert!(node_1_holds(&pool.state, "__system__", key).await);
    for peer in &pool.peers {
        assert!(peer.get_object("__system__", key).await.is_err());
    }
}

/// The gate, once the pool adopted its configurations, is recorded and opens
/// at the next start at once — though a node of the pool is down then and
/// would keep it closed otherwise. A gate already open needs no watching.
#[tokio::test]
async fn an_open_gate_stays_open_across_restarts() {
    let dir = tempfile::tempdir().unwrap();
    let storage = BitcaskStorageEngine::open(
        dir.path().join("volumes"),
        dir.path().join("metadata_db"),
        1024 * 1024,
        4096,
        false,
        s4_core::storage::IndexOptions {
            version_digests: true,
        },
    )
    .await
    .unwrap();
    let index = storage.index_db().clone();
    assert!(!crate::config_adoption::initial_gate(&index).await.is_open());

    let up = pool(20028, true).await;
    let nodes = vec![NodeId(0), NodeId(2), NodeId(3)];
    up.gossip.announce_bucket_configs_adopted();
    for peer in [NodeId(2), NodeId(3)] {
        up.gossip.set_bucket_configs_adopted_for_testing(peer, true);
    }
    let (_shutdown, shutdown_rx) = tokio::sync::watch::channel(false);
    let gate = ConfigReadGate::default();
    tokio::time::timeout(
        Duration::from_secs(5),
        crate::config_adoption::open_gate(
            nodes.clone(),
            NodeId(0),
            up.gossip.clone(),
            gate.clone(),
            index.clone(),
            shutdown_rx.clone(),
        ),
    )
    .await
    .unwrap();
    assert!(gate.is_open());
    assert!(index.bucket_config_quorum_reads().await.unwrap());

    // Node 3 is down: the pool would not count as adopted.
    up.gossip.set_bucket_configs_adopted_for_testing(NodeId(3), false);
    assert!(!crate::config_adoption::pool_adopted(
        &nodes,
        NodeId(0),
        &up.gossip
    ));
    let restarted = crate::config_adoption::initial_gate(&index).await;
    assert!(restarted.is_open());
    tokio::time::timeout(
        Duration::from_secs(5),
        crate::config_adoption::open_gate(
            nodes,
            NodeId(0),
            up.gossip.clone(),
            restarted,
            index,
            shutdown_rx,
        ),
    )
    .await
    .expect("an open gate is not watched");
}

/// A crash before this node recorded the whole pool's cutover cannot turn
/// previously adopted local copies into first-time legacy writes again.
#[tokio::test]
async fn a_restart_does_not_reopen_local_fallback_after_local_adoption() {
    let dir = tempfile::tempdir().unwrap();
    for first in [true, false] {
        let storage = BitcaskStorageEngine::open(
            dir.path().join("volumes"),
            dir.path().join("metadata_db"),
            1024 * 1024,
            4096,
            false,
            s4_core::storage::IndexOptions {
                version_digests: true,
            },
        )
        .await
        .unwrap();
        let index = storage.index_db();
        if first {
            index.mark_bucket_configs_adopted().await.unwrap();
            assert!(!index.bucket_config_quorum_reads().await.unwrap());
            // No seal: the process stopped before the pool's cutover.
        } else {
            assert!(!index.bucket_configs_adopted().await.unwrap());
            assert!(
                crate::config_adoption::initial_gate(index).await.is_open(),
                "an uncertain restart cannot allow previously adopted copies as local fallback"
            );
        }
    }
}

/// A node expires its copies of a bucket's objects by the rules the pool
/// holds, though it holds neither the bucket's marker nor the rules — the
/// marker on the nodes the placement gives it, the rules written through
/// another node. Until the pool adopted its configurations the rules the node
/// holds itself are weighed too: they count when nothing newer is held and
/// they are of the bucket's generation. A bucket without rules keeps its
/// objects.
#[tokio::test]
async fn a_node_expires_its_copies_by_the_rules_the_pool_holds() {
    let pool = pool(20020, true).await;
    let state = &pool.state;
    for path in [
        "/aging",
        "/aging/old.log",
        "/moved",
        "/moved/old.log",
        "/kept",
        "/kept/old.log",
        "/own",
        "/own/old.log",
        "/stale",
        "/stale/old.log",
    ] {
        assert_eq!(
            send(state, "PUT", path, &[], "bytes").await.0,
            StatusCode::OK,
            "{path}"
        );
    }
    // More buckets than a page of the worker's listing of markers.
    for n in 0..12 {
        let path = format!("/empty-{n:02}");
        assert_eq!(
            send(state, "PUT", &path, &[], "").await.0,
            StatusCode::OK,
            "{path}"
        );
    }
    let rule = |status: &str| {
        format!(
            "<LifecycleConfiguration><Rule><ID>all</ID><Status>{status}</Status>\
             <Filter><Prefix></Prefix></Filter><Expiration><Days>0</Days></Expiration>\
             </Rule></LifecycleConfiguration>"
        )
    };
    let marker = format!("{MARKER_PREFIX}aging");
    {
        // Node 1 is no replica of the marker: it holds no copy of it.
        let storage = state.storage.read().await;
        let record = storage.head_object("__system__", &marker).await.unwrap();
        assert!(storage
            .delete_object_if_hlc(
                "__system__",
                &marker,
                record.hlc_timestamp,
                record.hlc_logical,
                record.origin_node_id,
            )
            .await
            .unwrap());
        // An earlier release left node 1 rules of its own: for `aging` one
        // that expires nothing, replaced through the quorums since; for `own`
        // and `stale` one that expires everything, the only copy of it.
        for (bucket, status) in [
            ("aging", "Disabled"),
            ("own", "Enabled"),
            ("stale", "Enabled"),
        ] {
            storage
                .put_object(
                    "__system__",
                    &format!("__s4_bucket_lifecycle_{bucket}"),
                    rule(status).as_bytes(),
                    "application/xml",
                    &HashMap::new(),
                )
                .await
                .unwrap();
        }
        // `stale` was deleted and created again since: node 1's rule is of
        // the bucket before.
        held_only_by(
            &storage,
            "__system__",
            &format!("{MARKER_PREFIX}stale"),
            b"1",
            a_minute_ahead(),
        )
        .await;
    }
    for peer in &pool.peers {
        held_only_by(
            peer,
            "__system__",
            &format!("{MARKER_PREFIX}stale"),
            b"1",
            a_minute_ahead(),
        )
        .await;
    }
    for bucket in ["aging", "moved"] {
        for peer in &pool.peers {
            held_only_by(
                peer,
                "__system__",
                &format!("__s4_bucket_lifecycle_{bucket}"),
                rule("Enabled").as_bytes(),
                a_minute_ahead() + 1,
            )
            .await;
        }
    }
    let config = crate::config::LifecycleConfig {
        enabled: true,
        interval_hours: 24,
        dry_run: false,
        interval_secs: None,
        days_ahead: 0,
    };
    let gate = ConfigReadGate::default();
    let worker = crate::LifecycleWorker::new(state.storage.clone(), config)
        .with_cluster_clock(Arc::new(HybridClock::new(NodeId(1))))
        .with_cluster_buckets(
            state.list_coordinator.clone().unwrap(),
            state.read_coordinator.clone().unwrap(),
            gate.clone(),
        );

    let expired = |bucket: &'static str| async move {
        matches!(
            state.storage.read().await.key_version(bucket, "old.log").await.unwrap(),
            s4_core::KeyVersion::Tombstone(_)
        )
    };

    let before_the_gate = worker.evaluate_once().await.unwrap();
    assert_eq!(
        (
            before_the_gate.buckets_processed,
            before_the_gate.objects_deleted
        ),
        (17, 3)
    );
    assert!(
        expired("aging").await,
        "the quorums' rule, newer than the node's own"
    );
    assert!(
        expired("moved").await,
        "the quorums' rule, the node holding none"
    );
    assert!(
        expired("own").await,
        "the node's own rule, the only copy of it"
    );
    assert!(
        !expired("stale").await,
        "the node's own rule is of the bucket before"
    );

    gate.open();
    let stats = worker.evaluate_once().await.unwrap();
    assert_eq!((stats.buckets_processed, stats.objects_deleted), (17, 0));
    assert!(!expired("stale").await);
    assert!(state.storage.read().await.head_object("kept", "old.log").await.is_ok());
}
