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

//! Background tombstone GC of a replicated pool.
//!
//! Each pass walks this node's version digests a page at a time and, for
//! every tombstone, checks:
//!
//! - its age: older than `gc_grace`;
//! - the repair frontier: the cut of every other replica of its key, by the
//!   pool's placement, is past its HLC ([`can_purge_tombstone`]).
//!
//! The tombstones of a page that pass are removed through the storage engine
//! in one batch, each only if its key still holds that very tombstone
//! ([`StorageEngine::gc_tombstones`]): a write or delete that reached the
//! key since the page was read is never removed in its place. The keys'
//! version digests go in the same batch, and so does the raised tombstone
//! purge horizon: from then on the node reports that a delete up to it may
//! have left nothing here.
//!
//! # Design Notes
//!
//! - Cuts move only in this node's own anti-entropy rounds. A node with
//!   rounds switched off, or one whose peer is down, removes no tombstone of
//!   the keys it shares with that peer, however long that lasts.
//! - Distributed safety is achieved through `gc_grace + repair frontier`,
//!   not through synchronized compaction between nodes.
//! - While the pool moves its bucket metadata to new replicas, at stages 1
//!   and 2, their tombstones are not removed; once the move settled, one goes
//!   only when the cut of every other replica is past the moment this node
//!   took stage 3 as well ([`PoolLayout::tombstone_keep`], plan
//!   BUCKET-METADATA-REPLICATION.md, decision 7).
//! - While the pool moves to a grown generation every tombstone stays; once
//!   it settled, the successor places the keys and only rounds begun since
//!   count ([`DistributedGcService::with_generation`]).

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use s4_core::storage::{TombstoneRef, VersionKind};
use s4_core::StorageEngine;
use tokio::sync::{watch, RwLock};
use tracing::{debug, error, info, warn};

use crate::clock::hlc::{self, HlcTimestamp};
use crate::gc::tombstone::{can_purge_tombstone, GcConfig};
use crate::identity::NodeId;
use crate::placement::{GenerationPhase, GenerationRanges, PoolLayout, TombstoneKeep};
use crate::repair::frontier::RepairFrontier;

/// Version digests read per page of a pass.
const SCAN_PAGE: usize = 4096;

/// Metrics emitted by the GC service.
#[derive(Debug, Default)]
pub struct GcMetrics {
    /// Total number of purge cycles completed.
    pub cycles_completed: AtomicU64,
    /// Total number of tombstones scanned across all cycles.
    pub tombstones_scanned: AtomicU64,
    /// Total number of tombstones successfully purged.
    pub tombstones_purged: AtomicU64,
    /// Number of tombstones currently active (last scan count).
    pub tombstones_active: AtomicU64,
    /// Number of purge errors encountered.
    pub purge_errors: AtomicU64,
}

/// Background service for distributed tombstone garbage collection.
///
/// The service scans for tombstones periodically and purges those that
/// satisfy both the age and repair frontier conditions.
pub struct DistributedGcService {
    config: GcConfig,
    storage: Arc<dyn StorageEngine>,
    repair_frontier: Arc<RwLock<RepairFrontier>>,
    placement: PoolLayout,
    local_node_id: NodeId,
    metrics: Arc<GcMetrics>,
    running: AtomicBool,
    /// Version digests read per page.
    page: usize,
    /// The arcs of a pool growing to a successor generation, and the
    /// successor's layout: while the move runs every tombstone stays; once
    /// the pool settled, the successor places the keys and only rounds begun
    /// since count.
    growth: Option<(Arc<GenerationRanges>, PoolLayout)>,
}

impl DistributedGcService {
    /// Tombstone GC of node `local_node_id` of a pool placed by `placement`.
    ///
    /// # Arguments
    ///
    /// * `config` — GC configuration (interval, tombstone policy)
    /// * `storage` — the node's storage, whose tombstones it removes
    /// * `repair_frontier` — the cuts this node's anti-entropy rounds set
    /// * `placement` — which nodes replicate each key, by its replication class
    /// * `local_node_id` — this node, which is not in the condition
    pub fn new(
        config: GcConfig,
        storage: Arc<dyn StorageEngine>,
        repair_frontier: Arc<RwLock<RepairFrontier>>,
        placement: PoolLayout,
        local_node_id: NodeId,
    ) -> Self {
        Self {
            config,
            storage,
            repair_frontier,
            placement,
            local_node_id,
            metrics: Arc::new(GcMetrics::default()),
            running: AtomicBool::new(false),
            page: SCAN_PAGE,
            growth: None,
        }
    }

    /// The same service for a pool growing to a successor generation,
    /// whose arcs are `ranges` and whose layout is `successor` (plan
    /// ERASURE-CODING-FIXING-POOL-GROWTH-REBALANCE.md, `EPGR-03-4`).
    ///
    /// - While the move runs, every tombstone stays. A new owner that has not
    ///   received a delete yet could only learn it from the tombstone, and
    ///   anti-entropy rounds compare the old owners alone: not only in the
    ///   arcs not cut over, but in those cut over too, where a new owner that
    ///   missed a later delete is compared by no round.
    /// - Once the pool settled here, the successor places the keys, and a
    ///   tombstone goes only when the cut of every other replica is past the
    ///   moment the pool settled as well: only rounds begun since compared
    ///   the successor's replicas.
    pub fn with_generation(mut self, ranges: Arc<GenerationRanges>, successor: PoolLayout) -> Self {
        self.growth = Some((ranges, successor));
        self
    }

    /// The same service, reading `page` digests at a time.
    #[cfg(test)]
    fn reading_pages_of(mut self, page: usize) -> Self {
        self.page = page;
        self
    }

    /// Returns a reference to the GC metrics.
    pub fn metrics(&self) -> &Arc<GcMetrics> {
        &self.metrics
    }

    /// Run the GC service loop until shutdown is signaled.
    ///
    /// The service runs a purge cycle every `config.interval`, then sleeps.
    /// It gracefully stops when `shutdown` receives `true`.
    pub async fn run(&self, mut shutdown: watch::Receiver<bool>) {
        if !self.config.enabled {
            info!("distributed GC service is disabled, exiting");
            return;
        }

        if let Err(e) = self.config.validate() {
            error!(error = %e, "invalid GC config, service will not start");
            return;
        }

        self.running.store(true, Ordering::SeqCst);
        info!(
            interval_secs = self.config.interval.as_secs(),
            gc_grace_secs = self.config.tombstone_policy.gc_grace.as_secs(),
            "distributed GC service started"
        );

        loop {
            tokio::select! {
                _ = tokio::time::sleep(self.config.interval) => {
                    match self.run_purge_cycle().await {
                        Ok(stats) if stats.purged > 0 => info!(
                            scanned = stats.scanned,
                            purged = stats.purged,
                            remaining = stats.remaining,
                            "GC purge cycle completed"
                        ),
                        Ok(stats) => debug!(
                            scanned = stats.scanned,
                            remaining = stats.remaining,
                            "GC purge cycle completed"
                        ),
                        Err(e) => {
                            error!(error = %e, "GC purge cycle failed");
                            self.metrics.purge_errors.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        info!("distributed GC service shutting down");
                        break;
                    }
                }
            }
        }

        self.running.store(false, Ordering::SeqCst);
    }

    /// Run a single purge cycle.
    ///
    /// Returns statistics about the cycle for logging and metrics. The
    /// tombstones each page removes are counted as soon as it removed them; a
    /// cycle that fails part way returns the error and leaves the count of
    /// active tombstones as the last complete cycle set it.
    pub async fn run_purge_cycle(&self) -> Result<PurgeCycleStats, String> {
        let now = hlc::now_millis();
        let gc_grace = self.config.tombstone_policy.gc_grace;

        let mut scanned = 0u64;
        let mut purged = 0u64;
        let mut after: Option<String> = None;
        loop {
            let page =
                match self.storage.version_digests_in(0, None, after.as_deref(), self.page).await {
                    Ok(page) => page,
                    Err(e) => {
                        self.metrics.tombstones_scanned.fetch_add(scanned, Ordering::Relaxed);
                        return Err(format!(
                            "version digest scan failed after {purged} tombstone(s) were \
                         removed: {e}"
                        ));
                    }
                };

            let mut due = Vec::new();
            let (placement, settled_at, held_by_move) = match &self.growth {
                Some((ranges, successor)) => match ranges.phase() {
                    GenerationPhase::Prepared => (&self.placement, None, false),
                    GenerationPhase::Moving => (&self.placement, None, true),
                    GenerationPhase::Finished => (successor, ranges.finished_at(), false),
                },
                None => (&self.placement, None, false),
            };
            {
                let frontier = self.repair_frontier.read().await;
                for entry in &page {
                    if entry.digest.kind != VersionKind::Tombstone {
                        continue;
                    }
                    scanned += 1;
                    let tombstone = HlcTimestamp::from_record(
                        entry.digest.hlc_timestamp,
                        entry.digest.hlc_logical,
                        entry.digest.origin_node_id,
                    );
                    // The tombstones of bucket metadata moving to new
                    // replicas stay while writes reach both layouts: the
                    // new replicas are compared by no round yet, and one
                    // that missed the delete could only learn it from them.
                    // Once settled, only the rounds begun since compared
                    // them on the new replicas.
                    if held_by_move {
                        continue;
                    }
                    let compared_since = match placement.tombstone_keep(&entry.key) {
                        TombstoneKeep::Proven => settled_at,
                        TombstoneKeep::Held => continue,
                        TombstoneKeep::ComparedSince(since) => {
                            Some(settled_at.map_or(since, |settled| {
                                if (settled.wall_time, settled.logical)
                                    > (since.wall_time, since.logical)
                                {
                                    settled
                                } else {
                                    since
                                }
                            }))
                        }
                    };
                    let others: Vec<NodeId> = placement
                        .replicas_at(&entry.key, entry.ring_position)
                        .into_iter()
                        .filter(|node| *node != self.local_node_id)
                        .collect();
                    let compared = compared_since.is_none_or(|since| {
                        others.iter().all(|node| {
                            frontier.peer_cut(node).is_some_and(|cut| {
                                (cut.wall_time, cut.logical) > (since.wall_time, since.logical)
                            })
                        })
                    });
                    if compared && can_purge_tombstone(tombstone, now, gc_grace, &frontier, &others)
                    {
                        due.push(TombstoneRef {
                            full_key: entry.key.clone(),
                            hlc: (tombstone.wall_time, tombstone.logical, tombstone.node_id),
                        });
                    }
                }
            }

            // One lock, one batch and one sync to disk for the page. A key
            // that took a write or another delete since the page was read
            // holds something else now, and stays.
            if !due.is_empty() {
                match self.storage.gc_tombstones(&due).await {
                    Ok(removed) => {
                        purged += removed;
                        self.metrics.tombstones_purged.fetch_add(removed, Ordering::Relaxed);
                        crate::cluster_metrics::record_tombstones_purged(removed);
                        debug!(due = due.len(), removed, "tombstones purged");
                    }
                    Err(e) => {
                        warn!(due = due.len(), error = %e, "failed to purge a page of tombstones");
                        self.metrics.purge_errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }

            match page.last() {
                Some(last) if page.len() == self.page => after = Some(last.key.clone()),
                _ => break,
            }
        }

        let remaining = scanned.saturating_sub(purged);
        self.metrics.tombstones_scanned.fetch_add(scanned, Ordering::Relaxed);
        self.metrics.tombstones_active.store(remaining, Ordering::Relaxed);
        self.metrics.cycles_completed.fetch_add(1, Ordering::Relaxed);
        crate::cluster_metrics::record_tombstones_active(remaining);

        Ok(PurgeCycleStats {
            scanned,
            purged,
            remaining,
        })
    }

    /// Check if the service loop is currently running.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }
}

/// Statistics from a single GC purge cycle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PurgeCycleStats {
    /// Number of tombstones scanned.
    pub scanned: u64,
    /// Number of tombstones purged.
    pub purged: u64,
    /// Number of tombstones remaining (not yet eligible).
    pub remaining: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordinator::test_pool::{at, put_with_hlc, Replica, TestPool, BUCKET};
    use crate::gc::tombstone::TombstonePolicy;
    use s4_core::KeyVersion;
    use std::time::Duration;

    fn config(gc_grace: Duration) -> GcConfig {
        GcConfig {
            enabled: true,
            interval: Duration::from_secs(1),
            tombstone_policy: TombstonePolicy {
                gc_grace,
                max_rejoin_downtime: Duration::ZERO,
                repair_turn: Duration::ZERO,
                write_settle: Duration::ZERO,
            },
        }
    }

    /// GC of node 1 of `pool` with the cuts of `cuts`.
    fn gc_of_node_1(pool: &TestPool, gc_grace: Duration, cuts: &[u128]) -> DistributedGcService {
        let mut frontier = RepairFrontier::new();
        for peer in cuts {
            frontier.advance_peer(
                NodeId(*peer),
                HlcTimestamp::from_record(hlc::now_millis(), 0, 0),
            );
        }
        DistributedGcService::new(
            config(gc_grace),
            pool.storage(1).clone(),
            Arc::new(RwLock::new(frontier)),
            pool.layout(),
            NodeId(1),
        )
    }

    async fn version(pool: &TestPool, node: u128, key: &str) -> KeyVersion {
        pool.storage(node).key_version(BUCKET, key).await.unwrap()
    }

    /// Node 1 holds an old tombstone of `deleted`, a young one of `young`,
    /// a live `kept`, and the tombstone of a deleted bucket marker.
    async fn tombstones_on_node_1(pool: &TestPool) {
        pool.create_bucket(1, at(1)).await;
        put_with_hlc(pool.storage(1), "kept", b"v", at(10)).await;
        put_with_hlc(pool.storage(1), "deleted", b"v", at(10)).await;
        pool.delete(1, "deleted", at(20)).await;
        pool.delete(
            1,
            "young",
            HlcTimestamp::from_record(hlc::now_millis(), 0, 7),
        )
        .await;
        pool.storage(1)
            .tombstone_object("__system__", "__s4_bucket_marker_gone", 30, 0, 7)
            .await
            .unwrap();
    }

    /// While the pool moves to a grown generation, every tombstone stays
    /// even with every cut past it, also once every arc cut over. Once the
    /// pool settled, the successor's replicas decide, by cuts past the moment
    /// it settled: the cuts of the earlier rounds hold nothing back, and a
    /// replica of the successor without a cut holds its tombstones.
    #[tokio::test]
    async fn a_growing_pool_keeps_tombstones_until_rounds_compared_the_successor() {
        use crate::placement::RangeState;
        let pool = TestPool::start(Replica::Down, Replica::Down, 21232).await;
        tombstones_on_node_1(&pool).await;
        let old: Vec<NodeId> = (1..=3).map(NodeId).collect();
        let new: Vec<NodeId> = (1..=6).map(NodeId).collect();
        let ranges = Arc::new(GenerationRanges::new(&old, &new).unwrap());
        let successor =
            PoolLayout::uniform(crate::placement::ClassLayout::majority_of(new.clone()));
        let gc = |cuts: &[u128]| {
            gc_of_node_1(&pool, Duration::from_secs(3600), cuts)
                .with_generation(ranges.clone(), successor.clone())
        };

        ranges.begin();
        let held = gc(&[2, 3, 4, 5, 6]);
        let stats = held.run_purge_cycle().await.unwrap();
        assert_eq!((stats.scanned, stats.purged, stats.remaining), (3, 0, 3));
        for range in ranges.snapshot() {
            ranges.advance_to(range.upper_inclusive, RangeState::CutOver).unwrap();
        }
        let stats = held.run_purge_cycle().await.unwrap();
        assert_eq!((stats.scanned, stats.purged, stats.remaining), (3, 0, 3));

        tokio::time::sleep(Duration::from_millis(5)).await;
        ranges.finish(HlcTimestamp::from_record(hlc::now_millis(), 0, 1)).unwrap();
        tokio::time::sleep(Duration::from_millis(5)).await;
        // Cuts from before the pool settled count for nothing.
        let stats = held.run_purge_cycle().await.unwrap();
        assert_eq!((stats.scanned, stats.purged, stats.remaining), (3, 0, 3));
        // Rounds since with every replica of the successor but one.
        let stats = gc(&[2, 3, 4, 5]).run_purge_cycle().await.unwrap();
        assert_eq!((stats.scanned, stats.purged, stats.remaining), (3, 0, 3));
        let stats = gc(&[2, 3, 4, 5, 6]).run_purge_cycle().await.unwrap();
        assert_eq!((stats.scanned, stats.purged, stats.remaining), (3, 2, 1));
    }

    /// A replica without a cut holds the tombstone; the node itself is not in
    /// the condition. Before, GC asked for a cut of every replica of the pool,
    /// itself among them, and removed nothing.
    #[tokio::test]
    async fn a_replica_without_a_cut_holds_the_tombstone_and_the_node_itself_is_no_replica_to_wait_for(
    ) {
        let pool = TestPool::start(Replica::Down, Replica::Down, 21240).await;
        tombstones_on_node_1(&pool).await;

        let only_2 = gc_of_node_1(&pool, Duration::from_secs(3600), &[2]);
        let stats = only_2.run_purge_cycle().await.unwrap();
        assert_eq!((stats.scanned, stats.purged, stats.remaining), (3, 0, 3));
        assert!(matches!(
            version(&pool, 1, "deleted").await,
            KeyVersion::Tombstone(_)
        ));

        let both = gc_of_node_1(&pool, Duration::from_secs(3600), &[2, 3]);
        let stats = both.run_purge_cycle().await.unwrap();
        // The old tombstone and the bucket marker's go, the young one stays.
        assert_eq!((stats.scanned, stats.purged, stats.remaining), (3, 2, 1));
        assert!(matches!(
            version(&pool, 1, "deleted").await,
            KeyVersion::Absent
        ));
        assert!(matches!(
            version(&pool, 1, "young").await,
            KeyVersion::Tombstone(_)
        ));
        assert!(matches!(
            version(&pool, 1, "kept").await,
            KeyVersion::Live(_)
        ));
        let marker = pool
            .storage(1)
            .key_version("__system__", "__s4_bucket_marker_gone")
            .await
            .unwrap();
        assert!(matches!(marker, KeyVersion::Absent), "{marker:?}");

        // Their digests went in the same batch: the digests still describe
        // exactly what the index holds.
        let index = pool.storage(1).index_db();
        assert!(index.verify_version_digests().await.unwrap().is_consistent());
        let keys: Vec<String> = index
            .version_digests()
            .await
            .unwrap()
            .into_iter()
            .map(|entry| entry.key)
            .collect();
        assert!(!keys.contains(&"bucket/deleted".to_string()), "{keys:?}");
        assert!(keys.contains(&"bucket/young".to_string()), "{keys:?}");
    }

    /// The tombstones of moving bucket metadata stay at stages 1 and 2
    /// whatever the cuts; once settled, they go only when every cut is past
    /// the moment this node took stage 3. A tombstone of a data key goes by
    /// its cuts alone at every stage (task BMR-04-003).
    #[tokio::test]
    async fn bucket_metadata_tombstones_wait_for_rounds_on_their_new_replicas() {
        use crate::coordinator::test_pool::TestMove;
        use crate::metadata_placement::{QuorumShape, Stage};

        let shape = |rf| QuorumShape::majority(rf);
        for (stage, port) in Stage::ALL.into_iter().zip([21600u16, 21604, 21608, 21612]) {
            let pool = TestPool::start_moving(
                Replica::Down,
                Replica::Down,
                port,
                TestMove {
                    from: shape(1),
                    to: shape(3),
                    stages: [stage; 3],
                },
            )
            .await;
            let settled_since = pool.stage_cell(1).settled_since();
            assert_eq!(settled_since.is_some(), stage == Stage::Settled);
            let gc_with_cuts_at = |wall: u64| {
                let mut frontier = RepairFrontier::new();
                for peer in [2, 3] {
                    frontier.advance_peer(NodeId(peer), HlcTimestamp::from_record(wall, 0, 0));
                }
                DistributedGcService::new(
                    config(Duration::from_secs(3600)),
                    pool.storage(1).clone(),
                    Arc::new(RwLock::new(frontier)),
                    pool.layout(),
                    NodeId(1),
                )
            };
            let tombstones = || async {
                pool.delete(1, "deleted", at(20)).await;
                pool.storage(1)
                    .tombstone_object("__system__", "__s4_bucket_marker_gone", 30, 0, 7)
                    .await
                    .unwrap();
            };
            let marker_gone = || async {
                matches!(
                    pool.storage(1)
                        .key_version("__system__", "__s4_bucket_marker_gone")
                        .await
                        .unwrap(),
                    KeyVersion::Absent
                )
            };

            // Cuts past the tombstones, from before stage 3 was taken.
            tombstones().await;
            let before = settled_since.map_or(1_000, |since| since.wall_time - 1);
            gc_with_cuts_at(before).run_purge_cycle().await.unwrap();
            assert!(
                matches!(version(&pool, 1, "deleted").await, KeyVersion::Absent),
                "{stage}: a data tombstone goes by its cuts"
            );
            assert_eq!(marker_gone().await, stage == Stage::Legacy, "{stage}");

            // Cuts from after it.
            tombstones().await;
            gc_with_cuts_at(hlc::now_millis() + 1_000).run_purge_cycle().await.unwrap();
            assert_eq!(
                marker_gone().await,
                matches!(stage, Stage::Legacy | Stage::Settled),
                "{stage}"
            );
        }
    }

    fn due(key: &str, wall: u64) -> TombstoneRef {
        TombstoneRef {
            full_key: format!("{BUCKET}/{key}"),
            hlc: (wall, 0, 7),
        }
    }

    /// Of a page of tombstones found due, only those whose keys still hold
    /// them as found go: not a key a newer write or delete replaced since,
    /// nor one that holds nothing.
    #[tokio::test]
    async fn only_the_tombstones_found_are_removed() {
        let pool = TestPool::start(Replica::Down, Replica::Down, 21244).await;
        pool.create_bucket(1, at(1)).await;
        pool.delete(1, "old", at(20)).await;
        pool.delete(1, "rewritten", at(20)).await;
        pool.delete(1, "deleted-again", at(20)).await;
        put_with_hlc(pool.storage(1), "rewritten", b"new", at(30)).await;
        pool.delete(1, "deleted-again", at(30)).await;

        let storage = pool.storage(1);
        let page = [
            due("old", 20),
            due("rewritten", 20),
            due("deleted-again", 20),
            due("never-there", 20),
        ];
        assert_eq!(storage.gc_tombstones(&page).await.unwrap(), 1);
        assert!(matches!(version(&pool, 1, "old").await, KeyVersion::Absent));
        assert!(matches!(
            version(&pool, 1, "rewritten").await,
            KeyVersion::Live(_)
        ));
        match version(&pool, 1, "deleted-again").await {
            KeyVersion::Tombstone(record) => assert_eq!(record.hlc_timestamp, 30),
            other => panic!("{other:?}"),
        }
        assert_eq!(
            storage.gc_tombstones(&[due("deleted-again", 30)]).await.unwrap(),
            1
        );
        assert!(matches!(
            version(&pool, 1, "deleted-again").await,
            KeyVersion::Absent
        ));
        // Nothing found, nothing written: the purge horizon stays where the
        // removals put it.
        assert_eq!(storage.gc_tombstones(&[due("old", 20)]).await.unwrap(), 0);
    }

    /// A node that removed tombstones reports the newest of them as its
    /// horizon: a key it holds nothing for may have had a delete up to there.
    /// The horizon only rises.
    #[tokio::test]
    async fn removing_tombstones_raises_the_horizon_the_node_reports() {
        let pool = TestPool::start(Replica::Down, Replica::Down, 21272).await;
        pool.create_bucket(1, at(1)).await;
        pool.delete(1, "a", at(20)).await;
        pool.delete(1, "b", at(30)).await;
        pool.delete(1, "c", at(25)).await;
        let storage = pool.storage(1);
        assert_eq!(
            storage.tombstone_horizon().await.unwrap(),
            Some(s4_core::storage::TombstoneHorizon::ZERO)
        );

        storage.gc_tombstones(&[due("a", 20), due("b", 30)]).await.unwrap();
        let reported = storage.tombstone_horizon().await.unwrap().unwrap();
        assert_eq!(HlcTimestamp::from(reported), at(30));
        storage.gc_tombstones(&[due("c", 25)]).await.unwrap();
        let reported = storage.tombstone_horizon().await.unwrap().unwrap();
        assert_eq!(
            HlcTimestamp::from(reported),
            at(30),
            "the horizon never falls"
        );
        // The settled horizon the start log reports is unchanged.
        assert_eq!(
            storage.index_db().tombstone_horizon().await.unwrap(),
            Some(s4_core::storage::TombstoneHorizon::ZERO)
        );
    }

    /// Reads digests from `inner`, `pages` pages of them and then fails.
    struct FailingAfter {
        inner: Arc<s4_core::BitcaskStorageEngine>,
        pages: std::sync::atomic::AtomicUsize,
    }

    #[async_trait::async_trait]
    impl StorageEngine for FailingAfter {
        async fn version_digests_in(
            &self,
            from: u64,
            to: Option<u64>,
            after: Option<&str>,
            limit: usize,
        ) -> Result<Vec<s4_core::storage::VersionDigestEntry>, s4_core::StorageError> {
            if self.pages.fetch_sub(1, Ordering::SeqCst) == 0 {
                return Err(s4_core::StorageError::Database("injected".into()));
            }
            self.inner.version_digests_in(from, to, after, limit).await
        }
        async fn gc_tombstones(
            &self,
            tombstones: &[TombstoneRef],
        ) -> Result<u64, s4_core::StorageError> {
            self.inner.gc_tombstones(tombstones).await
        }
        async fn put_object(
            &self,
            _: &str,
            _: &str,
            _: &[u8],
            _: &str,
            _: &std::collections::HashMap<String, String>,
        ) -> Result<String, s4_core::StorageError> {
            unreachable!("GC writes no object")
        }
        async fn get_object(
            &self,
            _: &str,
            _: &str,
        ) -> Result<(Vec<u8>, s4_core::IndexRecord), s4_core::StorageError> {
            unreachable!("GC reads no object")
        }
        async fn delete_object(&self, _: &str, _: &str) -> Result<(), s4_core::StorageError> {
            unreachable!("GC deletes no object")
        }
        async fn head_object(
            &self,
            _: &str,
            _: &str,
        ) -> Result<s4_core::IndexRecord, s4_core::StorageError> {
            unreachable!("GC reads no object")
        }
        async fn list_objects(
            &self,
            _: &str,
            _: &str,
            _: usize,
        ) -> Result<Vec<(String, s4_core::IndexRecord)>, s4_core::StorageError> {
            unreachable!("GC lists nothing")
        }
        async fn list_objects_after(
            &self,
            _: &str,
            _: &str,
            _: &str,
            _: usize,
        ) -> Result<Vec<(String, s4_core::IndexRecord)>, s4_core::StorageError> {
            unreachable!("GC lists nothing")
        }
        async fn put_object_versioned(
            &self,
            _: &str,
            _: &str,
            _: &[u8],
            _: &str,
            _: &std::collections::HashMap<String, String>,
            _: s4_core::VersioningStatus,
        ) -> Result<(String, Option<String>), s4_core::StorageError> {
            unreachable!("GC writes no object")
        }
        async fn get_object_version(
            &self,
            _: &str,
            _: &str,
            _: &str,
        ) -> Result<(Vec<u8>, s4_core::IndexRecord), s4_core::StorageError> {
            unreachable!("GC reads no object")
        }
        async fn delete_object_versioned(
            &self,
            _: &str,
            _: &str,
            _: Option<&str>,
            _: s4_core::VersioningStatus,
        ) -> Result<s4_core::DeleteResult, s4_core::StorageError> {
            unreachable!("GC deletes no object")
        }
        async fn head_object_version(
            &self,
            _: &str,
            _: &str,
            _: &str,
        ) -> Result<s4_core::IndexRecord, s4_core::StorageError> {
            unreachable!("GC reads no object")
        }
        async fn list_object_versions(
            &self,
            _: &str,
            _: &str,
            _: Option<&str>,
            _: Option<&str>,
            _: usize,
        ) -> Result<s4_core::ListVersionsResult, s4_core::StorageError> {
            unreachable!("GC lists nothing")
        }
        #[allow(clippy::too_many_arguments)]
        async fn put_object_with_retention(
            &self,
            _: &str,
            _: &str,
            _: &[u8],
            _: &str,
            _: &std::collections::HashMap<String, String>,
            _: s4_core::VersioningStatus,
            _: Option<s4_core::types::DefaultRetention>,
        ) -> Result<(String, Option<String>), s4_core::StorageError> {
            unreachable!("GC writes no object")
        }
        async fn update_object_metadata(
            &self,
            _: &str,
            _: &str,
            _: &str,
            _: s4_core::IndexRecord,
        ) -> Result<(), s4_core::StorageError> {
            unreachable!("GC writes no object")
        }
        async fn open_object_stream(
            &self,
            _: &str,
            _: &str,
            _: s4_core::ReadOptions,
        ) -> Result<s4_core::ObjectStream, s4_core::StorageError> {
            unreachable!("GC reads no object")
        }
        async fn open_object_version_stream(
            &self,
            _: &str,
            _: &str,
            _: &str,
            _: s4_core::ReadOptions,
        ) -> Result<s4_core::ObjectStream, s4_core::StorageError> {
            unreachable!("GC reads no object")
        }
        #[allow(clippy::too_many_arguments)]
        async fn put_object_streaming(
            &self,
            _: &str,
            _: &str,
            _: Box<dyn tokio::io::AsyncRead + Unpin + Send>,
            _: u64,
            _: &str,
            _: &std::collections::HashMap<String, String>,
            _: &str,
        ) -> Result<s4_core::StreamingPutResult, s4_core::StorageError> {
            unreachable!("GC writes no object")
        }
    }

    /// A pass that fails on its second page still counts what its first page
    /// removed, and leaves the count of active tombstones as the last complete
    /// pass set it.
    #[tokio::test]
    async fn a_pass_that_fails_part_way_counts_what_it_removed() {
        let metrics = crate::test_metrics::TestMetrics::install();
        let pool = TestPool::start(Replica::Down, Replica::Down, 21276).await;
        pool.create_bucket(1, at(1)).await;
        for n in 0..6 {
            pool.delete(1, &format!("k{n}"), at(20 + n)).await;
        }
        let mut frontier = RepairFrontier::new();
        for peer in [2, 3] {
            frontier.advance_peer(
                NodeId(peer),
                HlcTimestamp::from_record(hlc::now_millis(), 0, 0),
            );
        }
        let failing = Arc::new(FailingAfter {
            inner: pool.storage(1).clone(),
            pages: std::sync::atomic::AtomicUsize::new(1),
        });
        let service = DistributedGcService::new(
            config(Duration::from_secs(1)),
            failing,
            Arc::new(RwLock::new(frontier)),
            pool.layout(),
            NodeId(1),
        )
        .reading_pages_of(4);

        assert!(service.run_purge_cycle().await.is_err());
        // The first page holds the bucket marker and three tombstones.
        let counters = service.metrics();
        assert_eq!(counters.tombstones_purged.load(Ordering::Relaxed), 3);
        assert_eq!(metrics.value("s4_tombstones_purged_total", &[]), Some(3.0));
        assert_eq!(counters.cycles_completed.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.value("s4_tombstones_active", &[]), None);
    }

    /// A pass reports the tombstones it purged, adding up over passes, and
    /// the ones it left, as they stand after the latest pass.
    #[tokio::test]
    async fn a_pass_reports_what_it_purged_and_what_is_left() {
        let metrics = crate::test_metrics::TestMetrics::install();
        let pool = TestPool::start(Replica::Down, Replica::Down, 21248).await;
        pool.create_bucket(1, at(1)).await;
        pool.delete(1, "ancient", at(1000)).await;
        pool.delete(
            1,
            "young",
            HlcTimestamp::from_record(hlc::now_millis(), 0, 7),
        )
        .await;
        let service = gc_of_node_1(&pool, Duration::from_secs(3600), &[2, 3]);

        service.run_purge_cycle().await.unwrap();
        assert_eq!(metrics.value("s4_tombstones_purged_total", &[]), Some(1.0));
        assert_eq!(metrics.value("s4_tombstones_active", &[]), Some(1.0));

        service.run_purge_cycle().await.unwrap();
        assert_eq!(metrics.value("s4_tombstones_purged_total", &[]), Some(1.0));
        assert_eq!(metrics.value("s4_tombstones_active", &[]), Some(1.0));

        // The admin API reads these counters, not the metrics.
        let counters = service.metrics();
        assert_eq!(
            (
                counters.tombstones_purged.load(Ordering::Relaxed),
                counters.tombstones_active.load(Ordering::Relaxed),
                counters.cycles_completed.load(Ordering::Relaxed),
            ),
            (1, 1, 2)
        );
    }
}
