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
    BucketPlacement, ClusterConfig, DistributedListCoordinator, Edition, GossipHandle,
    HintedHandoffConfig, HintedHandoffManager, HybridClock, IdempotencyTracker, ListTimeouts,
    NodeAddr, NodeCapacity, NodeClient, NodeClientConfig, NodeId, NodeMeta, NodeRpcServer,
    NodeState, NodeStatus, PlacementRouter, PoolConfig, PoolId, QuorumReadCoordinator,
    QuorumWriteCoordinator, ReadRepairService, ReadTimeouts, ReplicaReadHandler,
    ReplicaWriteHandler, RpcServerConfig, WriteTimeouts,
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
    _servers: Vec<RpcServerHandle>,
    _dirs: Vec<tempfile::TempDir>,
}

async fn bitcask(dir: &std::path::Path) -> BitcaskStorageEngine {
    BitcaskStorageEngine::new(
        dir.join("volumes"),
        dir.join("metadata_db"),
        10 * 1024 * 1024,
        4096,
        false,
    )
    .await
    .expect("storage engine")
}

/// S3 requests go unsigned, as in the API's own tests; the admin routes still
/// take a JWT.
static DISABLE_S3_AUTH: std::sync::Once = std::sync::Once::new();

async fn pool(base_port: u16, peers_run: bool) -> Pool {
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
        let storage = Arc::new(bitcask(dir.path()).await);
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
        bitcask(dir.path()).await,
        "test-access-key".to_string(),
        "test-secret-key".to_string(),
        dir.path(),
    )
    .await;
    let storage: Arc<dyn StorageEngine> = Arc::new(SharedStorageAdapter(state.storage.clone()));
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
    let rpc_client = Arc::new(NodeClient::new(gossip, NodeClientConfig::default()));
    let resolver: HashMap<_, _> = (1..=3).map(|n| (addr(n).grpc_addr, NodeId(n))).collect();
    let router = Arc::new(
        PlacementRouter::new(&config, NodeId(1), BucketPlacement::new(), &resolver).unwrap(),
    );
    let clock = Arc::new(HybridClock::new(NodeId(1)));
    let hints = Arc::new(HintedHandoffManager::new(
        HintedHandoffConfig::new(dir.path()),
        rpc_client.clone(),
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
        rpc_client,
        router,
        config.epoch,
        ListTimeouts::default(),
    );
    let state = state.with_cluster_coordinators(Arc::new(write), Arc::new(read), Arc::new(list));
    dirs.push(dir);
    Pool {
        state,
        peers,
        _servers: servers,
        _dirs: dirs,
    }
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
async fn admin_bearer(state: &AppState) -> String {
    state
        .iam_storage
        .create_user("admin".into(), "password123".into(), Role::SuperUser)
        .await
        .unwrap();
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
