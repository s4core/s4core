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

//! Where this node is in the move of its pool's bucket metadata to the
//! replicas the pool's failures call for (plan
//! BUCKET-METADATA-REPLICATION.md, decision 4).
//!
//! [`start`] reads the record this node stored, goes on from it towards the
//! target this start derived, stores the result and announces it in gossip.
//! [`advance`] then takes the next stage whenever its gate opens, through the
//! driver of the stages ([`StageDriver`]): it stores the new stage before it
//! announces it, so a stage another node saw announced survives a restart of
//! this one, and at stage 1 it brings the bucket metadata stored before the
//! move to the new replicas (the backfill). The record travels with the
//! metadata gossip spreads anyway.
//!
//! While the pool moves its bucket metadata, [`layout_move`] gives the router
//! and the replica handlers the two layouts and this node's stage
//! ([`StageCell`]): a key is placed, and a write of it fenced, by the stage
//! the node is at. A change of stage takes the new stage at once for the
//! writes that begin after it, and is stored and announced only once the
//! writes begun at the previous stage ended. A pool with nothing to move is
//! settled from the start and places its bucket metadata like its data.

use std::sync::Arc;

use s4_cluster::metadata_placement::{
    store, BackfillPass, PlacementNote, PlacementRecord, PlacementStatus, QuorumShape, StageCell,
    StageDriver,
};
use s4_cluster::placement::{BucketMetadataMove, ClassLayout};
use s4_cluster::{
    GossipHandle, NodeId, PlacementStrategy, BUCKET_METADATA_MIN_PROTOCOL_VERSION, PROTOCOL_VERSION,
};
use s4_core::storage::IndexDb;
use tokio::sync::watch;
use tracing::{info, warn};

/// The record this node starts with: the one it stored, gone on towards
/// `target` ([`PlacementRecord::resume`]), stored and announced.
///
/// # Errors
///
/// The stored record cannot be read, or the new one cannot be stored: a node
/// that does not know its stage must not serve, since its stage decides
/// which replicas it reads.
pub async fn start(
    index_db: &IndexDb,
    gossip: &GossipHandle,
    data: QuorumShape,
    target: QuorumShape,
) -> anyhow::Result<PlacementRecord> {
    let stored = store::load(index_db)
        .await
        .map_err(|e| anyhow::anyhow!("bucket metadata placement: {e}"))?;
    let (record, note) = PlacementRecord::resume(stored, data, target);
    match note {
        None => {}
        Some(PlacementNote::TargetDeferred { target }) => warn!(
            from = %record.from,
            to = %record.to,
            stage = %record.stage,
            target = %target,
            "The move of the bucket metadata under way finishes first; the pool moves on to the \
             new target once it settled"
        ),
        Some(PlacementNote::LoweringRefused { target }) => warn!(
            placed = %record.to,
            target = %target,
            "The pool keeps its bucket metadata on more replicas than the target: fewer \
             replicas are not supported"
        ),
    }
    store::store(index_db, &record)
        .await
        .map_err(|e| anyhow::anyhow!("bucket metadata placement: {e}"))?;
    gossip.announce_bucket_metadata_placement(record);
    info!(
        from = %record.from,
        to = %record.to,
        stage = %record.stage,
        backfill_complete = record.backfill_complete,
        "Bucket metadata placement"
    );
    Ok(record)
}

/// The two layouts of the pool's bucket metadata and this node's stage, as
/// `record` places them on `nodes`; `None` when the pool places them like its
/// `data`, with nothing to move.
///
/// # Errors
///
/// A shape of the record the pool's nodes cannot hold.
pub fn layout_move(
    record: &PlacementRecord,
    data: QuorumShape,
    nodes: &[NodeId],
) -> anyhow::Result<Option<BucketMetadataMove>> {
    if record.from == record.to && record.to == data {
        return Ok(None);
    }
    let layout = |shape: QuorumShape| -> anyhow::Result<ClassLayout> {
        let strategy = PlacementStrategy::new(nodes.to_vec(), shape.replication_factor)
            .map_err(|e| anyhow::anyhow!("bucket metadata placement {shape}: {e}"))?;
        ClassLayout::new(strategy, shape.write_quorum, shape.read_quorum)
            .map_err(|e| anyhow::anyhow!("bucket metadata placement {shape}: {e}"))
    };
    Ok(Some(BucketMetadataMove::new(
        layout(record.from)?,
        layout(record.to)?,
        StageCell::new(record.stage),
    )))
}

/// Takes the next stage of `record` each time its gate opens for `local` in
/// `pool`, until the move settled ([`StageDriver`]): in `stage` first, where
/// the router and the replicas read it, then — once the writes of the
/// previous stage ended — in `cluster_meta` and in gossip. At stage 1 it runs
/// `backfill` until a pass is complete. Where it stands goes to `status`,
/// which the metrics and the admin API read. Stops early when `shutdown`
/// turns true.
#[allow(clippy::too_many_arguments)]
pub async fn advance(
    pool: Vec<NodeId>,
    local: NodeId,
    gossip: Arc<GossipHandle>,
    index_db: Arc<IndexDb>,
    record: PlacementRecord,
    stage_cell: Option<StageCell>,
    backfill: Option<Arc<dyn BackfillPass>>,
    status: Option<Arc<PlacementStatus>>,
    shutdown: watch::Receiver<bool>,
) {
    let driver = StageDriver::new(
        pool,
        local,
        record,
        stage_cell,
        gossip,
        index_db,
        backfill,
        PROTOCOL_VERSION,
        BUCKET_METADATA_MIN_PROTOCOL_VERSION,
    );
    match status {
        Some(status) => driver.with_status(status),
        None => driver,
    }
    .run(shutdown)
    .await;
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use s4_cluster::metadata_placement::{BackfillReport, Stage, GATE_INTERVAL};
    use s4_cluster::{Edition, NodeAddr, NodeCapacity, NodeMeta, NodeState, NodeStatus, PoolId};
    use s4_core::storage::{BitcaskStorageEngine, IndexOptions};

    use super::*;

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

    async fn index(dir: &std::path::Path) -> Arc<IndexDb> {
        let storage = BitcaskStorageEngine::open(
            dir.join("volumes"),
            dir.join("metadata_db"),
            1024 * 1024,
            4096,
            false,
            IndexOptions {
                version_digests: true,
            },
        )
        .await
        .unwrap();
        storage.index_db().clone()
    }

    /// The handle of node 0, with peers 1 to 5 alive and speaking `protocol`.
    fn pool_gossip(protocol: u32) -> Arc<GossipHandle> {
        let members = (1..=5u128)
            .map(|n| {
                let meta = NodeMeta {
                    node_id: NodeId(n),
                    node_name: format!("node-{n}"),
                    addr: NodeAddr {
                        grpc_addr: format!("127.0.0.1:{}", 9100 + n).parse().unwrap(),
                        http_addr: format!("127.0.0.1:{}", 9000 + n).parse().unwrap(),
                        gossip_addr: format!("127.0.0.1:{}", 9200 + n).parse().unwrap(),
                    },
                    pool_id: PoolId::new(0),
                    topology_epoch: 1,
                    protocol_version: protocol,
                    capacity: NodeCapacity::default(),
                    started_at: 0,
                    s4_version: "1.0.0".into(),
                    edition: Edition::Enterprise,
                };
                (
                    NodeId(n),
                    NodeState {
                        meta,
                        status: NodeStatus::Alive,
                        last_seen: std::time::Instant::now(),
                    },
                )
            })
            .collect::<HashMap<_, _>>();
        Arc::new(GossipHandle::new_for_testing(members))
    }

    fn pool() -> Vec<NodeId> {
        (0..=5u128).map(NodeId).collect()
    }

    /// A pool whose bucket metadata already has the replicas the pool's
    /// failures call for is settled from the start, and nothing is watched.
    #[tokio::test]
    async fn a_pool_with_nothing_to_move_is_settled_at_once() {
        let dir = tempfile::tempdir().unwrap();
        let index = index(dir.path()).await;
        let gossip = pool_gossip(PROTOCOL_VERSION);
        let record = start(&index, &gossip, S3, S3).await.unwrap();
        assert_eq!(record, PlacementRecord::settled(S3));
        assert!(layout_move(&record, S3, &pool()).unwrap().is_none());
        assert_eq!(store::load(&index).await.unwrap(), Some(record));
        assert_eq!(gossip.bucket_metadata_placement(&NodeId(0)), Some(record));
        let (_shutdown, shutdown_rx) = watch::channel(false);
        tokio::time::timeout(
            Duration::from_secs(5),
            advance(
                pool(),
                NodeId(0),
                gossip,
                index,
                record,
                None,
                None,
                None,
                shutdown_rx,
            ),
        )
        .await
        .expect("a settled pool is not watched");
    }

    /// A pool that moves its bucket metadata takes stage 1 once every node
    /// announced stage 0 at protocol 7: the stage reaches the router's cell
    /// first, then the disk and gossip. It stops there until its backfill
    /// finished.
    #[tokio::test]
    async fn a_moving_pool_takes_stage_one_and_waits_there_for_the_backfill() {
        let dir = tempfile::tempdir().unwrap();
        let index = index(dir.path()).await;
        let gossip = pool_gossip(BUCKET_METADATA_MIN_PROTOCOL_VERSION);
        let record = start(&index, &gossip, S3, S5).await.unwrap();
        assert_eq!(record, PlacementRecord::starting(S3, S5));
        let moving = layout_move(&record, S3, &pool()).unwrap().expect("the pool moves");
        let cell = moving.stage_cell().clone();
        assert_eq!(cell.stage(), Stage::Legacy);
        for n in 1..=5 {
            gossip.set_bucket_metadata_placement_for_testing(NodeId(n), Some(record));
        }
        let (_shutdown, shutdown_rx) = watch::channel(false);
        let watching = tokio::time::timeout(
            GATE_INTERVAL * 3,
            advance(
                pool(),
                NodeId(0),
                gossip.clone(),
                index.clone(),
                record,
                Some(cell.clone()),
                None,
                None,
                shutdown_rx,
            ),
        )
        .await;
        assert!(watching.is_err(), "stage 2 waits for the backfill");
        assert_eq!(
            store::load(&index).await.unwrap().map(|r| r.backfill_complete),
            Some(false)
        );
        let dual = PlacementRecord {
            stage: Stage::DualWrite,
            ..record
        };
        assert_eq!(cell.stage(), Stage::DualWrite);
        assert_eq!(store::load(&index).await.unwrap(), Some(dual));
        assert_eq!(gossip.bucket_metadata_placement(&NodeId(0)), Some(dual));

        // A restart goes on from stage 1, a new target waiting for its end.
        let restarted = pool_gossip(BUCKET_METADATA_MIN_PROTOCOL_VERSION);
        assert_eq!(start(&index, &restarted, S3, S5).await.unwrap(), dual);
        let higher = QuorumShape::majority(6);
        assert_eq!(start(&index, &restarted, S3, higher).await.unwrap(), dual);
        let moving = layout_move(&dual, S3, &pool()).unwrap().unwrap();
        assert_eq!(moving.stage_cell().stage(), Stage::DualWrite);
    }

    /// A pool with a node of protocol 6 does not leave stage 0.
    #[tokio::test]
    async fn an_earlier_release_keeps_the_pool_at_stage_zero() {
        let dir = tempfile::tempdir().unwrap();
        let index = index(dir.path()).await;
        let gossip = pool_gossip(BUCKET_METADATA_MIN_PROTOCOL_VERSION - 1);
        let record = start(&index, &gossip, S3, S5).await.unwrap();
        let cell = StageCell::new(record.stage);
        let (_shutdown, shutdown_rx) = watch::channel(false);
        let watching = tokio::time::timeout(
            GATE_INTERVAL * 3,
            advance(
                pool(),
                NodeId(0),
                gossip.clone(),
                index.clone(),
                record,
                Some(cell.clone()),
                None,
                None,
                shutdown_rx,
            ),
        )
        .await;
        assert!(watching.is_err());
        assert_eq!(cell.stage(), Stage::Legacy);
        assert_eq!(store::load(&index).await.unwrap(), Some(record));
    }

    /// A write placed by stage 0 keeps stage 1 from being stored and
    /// announced until it ends, though the router places new writes by
    /// stage 1 at once (task BMR-03-002).
    #[tokio::test]
    async fn stage_one_is_announced_only_after_the_writes_of_stage_zero() {
        let dir = tempfile::tempdir().unwrap();
        let index = index(dir.path()).await;
        let gossip = pool_gossip(BUCKET_METADATA_MIN_PROTOCOL_VERSION);
        let record = start(&index, &gossip, S3, S5).await.unwrap();
        for n in 1..=5 {
            gossip.set_bucket_metadata_placement_for_testing(NodeId(n), Some(record));
        }
        let cell = StageCell::new(Stage::Legacy);
        let write = cell.enter();
        let (_shutdown, shutdown_rx) = watch::channel(false);
        let driver = tokio::spawn(advance(
            pool(),
            NodeId(0),
            gossip.clone(),
            index.clone(),
            record,
            Some(cell.clone()),
            None,
            None,
            shutdown_rx,
        ));
        tokio::time::sleep(GATE_INTERVAL * 2).await;
        assert_eq!(
            cell.stage(),
            Stage::DualWrite,
            "new writes are placed by stage 1"
        );
        assert_eq!(cell.enter().stage(), Stage::DualWrite);
        assert_eq!(
            store::load(&index).await.unwrap(),
            Some(record),
            "not stored yet"
        );
        assert_eq!(gossip.bucket_metadata_placement(&NodeId(0)), Some(record));
        drop(write);
        let dual = PlacementRecord {
            stage: Stage::DualWrite,
            ..record
        };
        tokio::time::timeout(Duration::from_secs(5), async {
            while gossip.bucket_metadata_placement(&NodeId(0)) != Some(dual) {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("stage 1 is announced once the stage 0 write ended");
        assert_eq!(store::load(&index).await.unwrap(), Some(dual));
        driver.abort();
    }

    /// A backfill that brings everything at once.
    #[derive(Default)]
    struct CountedBackfill(AtomicUsize);

    #[async_trait::async_trait]
    impl BackfillPass for CountedBackfill {
        async fn pass(&self) -> BackfillReport {
            self.0.fetch_add(1, Ordering::SeqCst);
            BackfillReport::default()
        }
    }

    /// Every other node already settled the move: this node takes stage 1,
    /// runs its backfill once every node announced stage 1, stores and
    /// announces that it finished, and goes on to stage 3. The settled
    /// record is on disk and in gossip.
    #[tokio::test]
    async fn a_backfilled_node_goes_on_to_the_end_of_the_move() {
        let dir = tempfile::tempdir().unwrap();
        let index = index(dir.path()).await;
        let gossip = pool_gossip(BUCKET_METADATA_MIN_PROTOCOL_VERSION);
        let record = start(&index, &gossip, S3, S5).await.unwrap();
        let settled = PlacementRecord {
            stage: Stage::Settled,
            backfill_complete: true,
            ..record
        };
        for n in 1..=5 {
            gossip.set_bucket_metadata_placement_for_testing(NodeId(n), Some(settled));
        }
        let cell = StageCell::new(Stage::Legacy);
        let backfill = Arc::new(CountedBackfill::default());
        let (_shutdown, shutdown_rx) = watch::channel(false);
        tokio::time::timeout(
            GATE_INTERVAL * 10,
            advance(
                pool(),
                NodeId(0),
                gossip.clone(),
                index.clone(),
                record,
                Some(cell.clone()),
                Some(backfill.clone()),
                None,
                shutdown_rx,
            ),
        )
        .await
        .expect("the move settles");
        assert_eq!(backfill.0.load(Ordering::SeqCst), 1);
        assert_eq!(cell.stage(), Stage::Settled);
        assert_eq!(store::load(&index).await.unwrap(), Some(settled));
        assert_eq!(gossip.bucket_metadata_placement(&NodeId(0)), Some(settled));
    }
}
