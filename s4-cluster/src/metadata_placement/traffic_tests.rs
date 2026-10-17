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

//! The stages applied to traffic, on a pool of three nodes over real
//! storage and gRPC (plan BUCKET-METADATA-REPLICATION.md, tasks BMR-03-001
//! and BMR-03-003).
//!
//! The pool's bucket metadata move from one replica to all three, read and
//! written through one, then two: the new replicas include the old one as
//! they do on a real ring, and each stage writes and reads by the rules of
//! [`Stage::rules`]. A write meets one of its two conditions and not the
//! other, and fails; a read takes the replicas of its stage.

use std::collections::HashMap;

use s4_core::StorageEngine;

use crate::coordinator::test_pool::{Replica, TestMove, TestPool};
use crate::error::ClusterError;
use crate::identity::NodeId;
use crate::metadata_placement::{QuorumShape, Stage};
use crate::rpc::proto;

const ONE: QuorumShape = QuorumShape {
    replication_factor: 1,
    write_quorum: 1,
    read_quorum: 1,
};
const THREE: QuorumShape = QuorumShape {
    replication_factor: 3,
    write_quorum: 2,
    read_quorum: 2,
};

fn moving(stages: [Stage; 3]) -> TestMove {
    TestMove {
        from: ONE,
        to: THREE,
        stages,
    }
}

/// A bucket marker whose one old replica is `node`.
fn marker_held_by(pool: &TestPool, node: u128) -> String {
    let layout = pool.layout();
    let old = layout.bucket_metadata_move().expect("the pool moves").old();
    (0..10_000)
        .map(|i| format!("{}b{i}", s4_core::types::BUCKET_MARKER_KEY_PREFIX))
        .find(|key| old.strategy().get_replicas(&format!("__system__/{key}")) == [NodeId(node)])
        .unwrap_or_else(|| panic!("no marker of node {node}"))
}

async fn write(pool: &TestPool, key: &str) -> Result<(), ClusterError> {
    let hints = tempfile::tempdir().unwrap();
    pool.write_coordinator(hints.path())
        .write(
            "__system__",
            key,
            b"1",
            "application/octet-stream",
            &HashMap::new(),
        )
        .await
        .map(|_| ())
}

fn is_quorum_not_met(result: &Result<(), ClusterError>) -> bool {
    matches!(result, Err(ClusterError::QuorumNotMet { .. }))
}

/// Nodes 2 and 3 down: a marker whose old replica is node 1 meets the old
/// write quorum alone. Stage 0 takes the write; stages 1 to 3 need two of
/// the three new replicas too, and refuse it.
#[tokio::test]
async fn a_write_without_the_new_quorum_fails_from_stage_one() {
    for (stage, port) in Stage::ALL.into_iter().zip([21400, 21404, 21408, 21412]) {
        let pool = TestPool::start_moving(
            Replica::Down,
            Replica::Down,
            port,
            moving([stage, stage, stage]),
        )
        .await;
        let key = marker_held_by(&pool, 1);
        let result = write(&pool, &key).await;
        if stage == Stage::Legacy {
            assert!(result.is_ok(), "{stage}: {result:?}");
        } else {
            assert!(is_quorum_not_met(&result), "{stage}: {result:?}");
        }
    }
}

/// Node 2 down: a marker whose old replica is node 2 meets the new write
/// quorum through nodes 1 and 3 and not the old one. Stages 0 to 2 need
/// the old replica, and refuse the write; stage 3 takes it.
#[tokio::test]
async fn a_write_without_the_old_quorum_fails_until_stage_three() {
    for (stage, port) in Stage::ALL.into_iter().zip([21416, 21420, 21424, 21428]) {
        let pool =
            TestPool::start_moving(Replica::Down, Replica::Up, port, moving([stage; 3])).await;
        let key = marker_held_by(&pool, 2);
        let result = write(&pool, &key).await;
        if stage == Stage::Settled {
            assert!(result.is_ok(), "{stage}: {result:?}");
            assert!(pool.storage(3).head_object("__system__", &key).await.is_ok());
        } else {
            assert!(is_quorum_not_met(&result), "{stage}: {result:?}");
        }
    }
}

/// A write at stages 1 and 2 reaches every replica of both layouts; at
/// stage 0 only the old one.
#[tokio::test]
async fn writes_reach_the_replicas_of_their_stage() {
    for (stage, port, reached) in [
        (Stage::Legacy, 21432, [false, true, false]),
        (Stage::DualWrite, 21436, [true, true, true]),
        (Stage::ReadNew, 21440, [true, true, true]),
    ] {
        let pool = TestPool::start_moving(Replica::Up, Replica::Up, port, moving([stage; 3])).await;
        let key = marker_held_by(&pool, 2);
        write(&pool, &key).await.unwrap();
        // Writes go on to the replicas past the quorum in the background.
        for _ in 0..100 {
            let mut held = [false; 3];
            for node in 1..=3u128 {
                held[node as usize - 1] =
                    pool.storage(node).head_object("__system__", &key).await.is_ok();
            }
            if held == reached || stage == Stage::Legacy && held[1] {
                assert_eq!(held, reached, "{stage}");
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    }
}

/// The old replica holds one version of a marker, the two others a newer
/// one. Stages 0 and 1 read the old replica's, stages 2 and 3 the newest of
/// the new replicas'.
#[tokio::test]
async fn a_read_takes_the_replicas_of_its_stage() {
    for (stage, port, expected) in [
        (Stage::Legacy, 21444, b"old"),
        (Stage::DualWrite, 21448, b"old"),
        (Stage::ReadNew, 21452, b"new"),
        (Stage::Settled, 21456, b"new"),
    ] {
        let pool = TestPool::start_moving(Replica::Up, Replica::Up, port, moving([stage; 3])).await;
        let key = marker_held_by(&pool, 1);
        pool.put_system(1, &key, b"old", crate::coordinator::test_pool::at(1_000)).await;
        for node in [2, 3] {
            pool.put_system(node, &key, b"new", crate::coordinator::test_pool::at(2_000))
                .await;
        }
        let read = pool.read_coordinator().read("__system__", &key).await.unwrap();
        assert_eq!(read.data, expected, "{stage}");
    }
}

/// Replicas at stage 2 refuse a client's write of a coordinator at stage 0
/// — one of an earlier release, which sends no stage — and the marker is
/// not written; they take a repair copy, and a write placed by stage 1.
#[tokio::test]
async fn a_replica_fences_a_coordinator_two_stages_behind() {
    let metrics = crate::test_metrics::TestMetrics::install();
    let pool = TestPool::start_moving(
        Replica::Up,
        Replica::Up,
        21460,
        moving([Stage::Legacy, Stage::ReadNew, Stage::ReadNew]),
    )
    .await;
    let key = marker_held_by(&pool, 2);
    let result = write(&pool, &key).await;
    assert!(is_quorum_not_met(&result), "{result:?}");
    assert!(
        pool.storage(2).head_object("__system__", &key).await.is_err(),
        "the fenced write left nothing"
    );

    let request = |stage: u32, repair_copy: bool| proto::WriteBlobRequest {
        verify_content: false,
        placement_generation: 0,
        bucket: "__system__".to_string(),
        key: key.clone(),
        data: b"1".to_vec(),
        content_hash: Vec::new(),
        content_type: "application/octet-stream".to_string(),
        metadata: HashMap::new(),
        hlc: Some(crate::coordinator::test_pool::at(5_000).to_proto()),
        version_id: None,
        coordinator_id: NodeId(1).to_bytes().to_vec(),
        operation_id: uuid::Uuid::new_v4().as_u128().to_be_bytes().to_vec(),
        topology_epoch: pool.epoch(),
        etag: String::new(),
        bucket_metadata_stage: stage,
        repair_copy,
        iam_lease: None,
    };
    let fenced_before = metrics
        .value("s4_bucket_metadata_fenced_writes_total", &[])
        .expect("the coordinator's write was fenced");
    assert!(fenced_before >= 1.0, "{}", metrics.render());
    let fenced = pool.rpc_client.send_write(NodeId(2), request(0, false)).await.unwrap();
    assert!(!fenced.success && fenced.stage_fenced, "{fenced:?}");
    let repaired = pool.rpc_client.send_write(NodeId(3), request(0, true)).await.unwrap();
    assert!(repaired.success && !repaired.stage_fenced, "{repaired:?}");
    let neighbour = pool.rpc_client.send_write(NodeId(2), request(1, false)).await.unwrap();
    assert!(neighbour.success, "{neighbour:?}");
    assert!(pool.storage(2).head_object("__system__", &key).await.is_ok());
    // Each fenced write is counted; the repair copy and the write of
    // stage 1 are not.
    assert_eq!(
        metrics.value("s4_bucket_metadata_fenced_writes_total", &[]),
        Some(fenced_before + 1.0),
        "{}",
        metrics.render()
    );

    // A key that is not bucket metadata is never fenced.
    let object = proto::WriteBlobRequest {
        bucket: crate::coordinator::test_pool::BUCKET.to_string(),
        key: "object".to_string(),
        ..request(0, false)
    };
    let taken = pool.rpc_client.send_write(NodeId(2), object).await.unwrap();
    assert!(taken.success, "{taken:?}");
    assert_eq!(pool.stage_cell(2).stage(), Stage::ReadNew);
}
