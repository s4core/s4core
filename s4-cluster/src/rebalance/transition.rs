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

//! This node's part in its pool's move to a grown generation (plan
//! ERASURE-CODING-FIXING-POOL-GROWTH-REBALANCE.md, phase 4).
//!
//! The node goes through three steps, each stored before it takes effect:
//!
//! 1. **Begin** — the durable generation becomes `moving`, writes reach the
//!    quorums of both rings; once every member announced the move, the
//!    placement floor rises to the successor's epoch (a coordinator of the
//!    earlier generation is fenced).
//! 2. **Cut-over** of each range — the set of ranges cut over is stored in
//!    `cluster_meta` first, then the router reads them through the
//!    successor. A restart reads them back before serving, so a node never
//!    returns to reading a range through the old owners.
//! 3. **Finish** — once every range cut over everywhere and the common
//!    barrier passed, the durable generation becomes `Stable(g + 1)` and the
//!    successor alone places every key.
//!
//! The node answers `PoolMove` with where it stands ([`PoolTransition::status`])
//! and applies what the request hands it: ranges cut over, or the pool's
//! settling.

use std::sync::{Arc, RwLock};

use s4_core::storage::IndexDb;
use serde::{Deserialize, Serialize};
use tracing::{debug, info};

use super::progress::{MoveControl, ProgressStore};
use super::service::RebalanceHandle;
use crate::clock::hlc::{HlcTimestamp, HybridClock};
use crate::placement::{GenerationPhase, GenerationRanges, RangeState};
use crate::placement_fence::PlacementFloor;
use crate::pool_generation::{self, CutOverRecord, GenerationAnnouncement, PoolGenerationRecord};
use crate::rpc::proto;
use crate::{ClusterError, GossipHandle, HintedHandoffManager, NodeId};

/// When this node settled on the successor, kept in `cluster_meta`: the
/// clean-up of the former owners' copies counts its grace period from it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FinishedMark {
    /// Encoding version.
    pub format: u32,
    /// The moment, by this node's clock.
    pub finished_at: HlcTimestamp,
}

impl FinishedMark {
    /// The name the mark of the move to `to_epoch` is stored under.
    pub fn record_name(to_epoch: u64) -> String {
        format!("{to_epoch}/finished")
    }

    /// Encoded mark.
    pub fn encode(&self) -> Result<Vec<u8>, ClusterError> {
        serde_json::to_vec(self).map_err(|e| ClusterError::Codec(e.to_string()))
    }

    /// Decodes a mark.
    pub fn decode(bytes: &[u8]) -> Result<Self, ClusterError> {
        serde_json::from_slice(bytes)
            .map_err(|e| ClusterError::Codec(format!("finished mark: {e}")))
    }

    /// The mark of the move to `to_epoch` in `store`, if the node settled.
    pub async fn load(
        store: &dyn ProgressStore,
        to_epoch: u64,
    ) -> Result<Option<Self>, ClusterError> {
        let name = Self::record_name(to_epoch);
        for (stored, bytes) in store.load().await? {
            if stored == name {
                return Self::decode(&bytes).map(Some);
            }
        }
        Ok(None)
    }
}

/// The name the ranges this node applied as cut over are stored under.
pub fn applied_cut_over_name(to_epoch: u64) -> String {
    format!("{to_epoch}/cut-over")
}

/// Everything this node's part in the move needs.
pub struct TransitionParts {
    /// The durable generation now: prepared or moving, with its successor.
    pub record: PoolGenerationRecord,
    /// This node.
    pub local: NodeId,
    /// The arcs of the union ring, shared with the router.
    pub ranges: Arc<GenerationRanges>,
    /// The floor this node's replicas and write coordinator fence by.
    pub floor: PlacementFloor,
    /// Where the durable generation lives.
    pub index: Arc<IndexDb>,
    /// Where the ranges applied and the moment of settling are kept.
    pub store: Arc<dyn ProgressStore>,
    /// Where the generation is announced.
    pub gossip: Arc<GossipHandle>,
    /// This node's hints, reported to the verifier.
    pub hints: Option<Arc<HintedHandoffManager>>,
    /// This node's clock.
    pub clock: Arc<HybridClock>,
}

/// This node's part in the move; see the module documentation.
pub struct PoolTransition {
    local: NodeId,
    /// The move's identity: the generation it began from, with its successor.
    origin: PoolGenerationRecord,
    record: RwLock<PoolGenerationRecord>,
    ranges: Arc<GenerationRanges>,
    floor: PlacementFloor,
    index: Arc<IndexDb>,
    store: Arc<dyn ProgressStore>,
    gossip: Arc<GossipHandle>,
    hints: Option<Arc<HintedHandoffManager>>,
    clock: Arc<HybridClock>,
    rebalance: RwLock<Option<RebalanceHandle>>,
    /// Hands this node's open uploads over when a coordinator or a hint asks.
    uploads: RwLock<Option<Arc<super::MultipartTransfer>>>,
    waiting: RwLock<Option<String>>,
    paused: RwLock<Option<String>>,
    steps: tokio::sync::Mutex<()>,
}

impl PoolTransition {
    /// This node's part in the move `parts.record` prepares or runs.
    ///
    /// # Errors
    ///
    /// A record without a successor.
    pub fn new(parts: TransitionParts) -> Result<Self, ClusterError> {
        parts.record.validate()?;
        if parts.record.proposed.is_none() {
            return Err(ClusterError::Config(
                "a pool transition needs a prepared successor generation".into(),
            ));
        }
        let mut origin = parts.record.clone();
        origin.moving = false;
        Ok(Self {
            local: parts.local,
            origin,
            record: RwLock::new(parts.record),
            ranges: parts.ranges,
            floor: parts.floor,
            index: parts.index,
            store: parts.store,
            gossip: parts.gossip,
            hints: parts.hints,
            clock: parts.clock,
            rebalance: RwLock::new(None),
            uploads: RwLock::new(None),
            waiting: RwLock::new(None),
            paused: RwLock::new(None),
            steps: tokio::sync::Mutex::new(()),
        })
    }

    /// Epoch the keys move from.
    pub fn source_epoch(&self) -> u64 {
        self.origin.current.epoch
    }

    /// Epoch the keys move to.
    pub fn to_epoch(&self) -> u64 {
        self.origin.proposed.as_ref().map_or(0, |next| next.epoch)
    }

    /// The generation the move began from, with its successor.
    pub fn origin(&self) -> &PoolGenerationRecord {
        &self.origin
    }

    /// The durable generation now.
    pub fn record(&self) -> PoolGenerationRecord {
        self.record.read().unwrap_or_else(std::sync::PoisonError::into_inner).clone()
    }

    /// Where this node stands.
    pub fn phase(&self) -> GenerationPhase {
        self.ranges.phase()
    }

    /// The arcs of the union ring.
    pub fn ranges(&self) -> &Arc<GenerationRanges> {
        &self.ranges
    }

    /// This node.
    pub fn local(&self) -> NodeId {
        self.local
    }

    /// The move of this node's keys, once it runs, for its progress.
    pub fn set_rebalance(&self, handle: RebalanceHandle) {
        *self.rebalance.write().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(handle);
    }

    /// Hands this node's open uploads over when a coordinator or a hint asks
    /// through `PoolMove`.
    pub fn set_uploads(&self, transfer: Arc<super::MultipartTransfer>) {
        *self.uploads.write().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(transfer);
    }

    /// The move of this node's keys, if it was started.
    pub fn rebalance(&self) -> Option<RebalanceHandle> {
        self.rebalance.read().unwrap_or_else(std::sync::PoisonError::into_inner).clone()
    }

    /// Reads back what an earlier run of this node stored: every range it
    /// applied as cut over reads through the successor before the node
    /// serves anything. The earlier generation is fenced again at once only
    /// when some range was cut over here, which happens only after every
    /// member began; otherwise a member may still be prepared and write by
    /// the earlier generation, and the driver fences once every member
    /// announced the move ([`Self::fence_earlier_generation`]).
    ///
    /// # Errors
    ///
    /// The stored progress could not be read.
    pub async fn restore(&self) -> Result<(), ClusterError> {
        let control = MoveControl::record_name(self.to_epoch());
        for (stored, bytes) in self.store.load().await? {
            if stored == control {
                let control = MoveControl::decode(&bytes)?;
                if control.pool_id == self.origin.pool_id
                    && control.from_epoch == self.source_epoch()
                {
                    *self.paused.write().unwrap_or_else(std::sync::PoisonError::into_inner) =
                        control.paused;
                }
            }
        }
        let record = self.record();
        if !record.moving {
            return Ok(());
        }
        self.ranges.begin();
        let name = applied_cut_over_name(self.to_epoch());
        for (stored, bytes) in self.store.load().await? {
            if stored != name {
                continue;
            }
            let applied = CutOverRecord::decode(&bytes)?;
            for upper in &applied.ranges {
                self.ranges.advance_to(*upper, RangeState::CutOver)?;
            }
            if !applied.ranges.is_empty() {
                self.floor.raise(self.to_epoch());
            }
            info!(
                cut_over = applied.ranges.len(),
                "the ranges this node read through the successor before its restart read through it again"
            );
        }
        Ok(())
    }

    /// Begins the move on this node: stored as `moving`, the placement floor
    /// raised, writes to both generations, announced. `false` when it had
    /// begun already.
    ///
    /// # Errors
    ///
    /// The generation could not be stored.
    pub async fn begin(&self) -> Result<bool, ClusterError> {
        let _step = self.steps.lock().await;
        if self.phase() != GenerationPhase::Prepared {
            return Ok(false);
        }
        let moving = self.record().begin()?;
        pool_generation::store(&self.index, &moving).await?;
        self.ranges.begin();
        *self.record.write().unwrap_or_else(std::sync::PoisonError::into_inner) = moving.clone();
        self.gossip.announce_pool_generation(GenerationAnnouncement::of(&moving, false));
        debug!(
            from_epoch = self.source_epoch(),
            to_epoch = self.to_epoch(),
            "pool growth began: writes reach the quorums of both generations"
        );
        Ok(true)
    }

    /// Fences coordinators of the earlier generation from this node's
    /// replicas and write coordinator. Called once every member announced
    /// the move: until then a member that has not begun yet still writes by
    /// the earlier generation, and fencing it would fail its clients' writes
    /// for nothing — no range is verified before every member began.
    pub fn fence_earlier_generation(&self) {
        if self.floor.get() < self.to_epoch() {
            self.floor.raise(self.to_epoch());
            debug!(
                to_epoch = self.to_epoch(),
                "every member began the move: coordinators of the earlier generation are fenced"
            );
        }
    }

    /// Whether this node fences coordinators of the earlier generation.
    pub fn fences_earlier_generation(&self) -> bool {
        self.floor.get() >= self.to_epoch()
    }

    /// Why the move waits on this node now, as its driver last saw it.
    pub fn waiting(&self) -> Option<String> {
        self.waiting.read().unwrap_or_else(std::sync::PoisonError::into_inner).clone()
    }

    /// Records why the move waits, `None` when it does not.
    pub fn set_waiting(&self, reason: Option<String>) {
        *self.waiting.write().unwrap_or_else(std::sync::PoisonError::into_inner) = reason;
    }

    /// Why an operator paused the move of this node's keys, while it is.
    pub fn paused(&self) -> Option<String> {
        match self.rebalance() {
            Some(handle) => handle.status().paused,
            None => self.paused.read().unwrap_or_else(std::sync::PoisonError::into_inner).clone(),
        }
    }

    /// Pauses the move of this node's keys for `paused`, or resumes it with
    /// `None`, as the pool's pause record says. A move that has not started
    /// here yet starts paused: the pause is stored where it reads its own.
    /// Returns whether anything changed.
    ///
    /// # Errors
    ///
    /// The pause could not be stored.
    pub async fn apply_pause(&self, paused: Option<String>) -> Result<bool, ClusterError> {
        if self.paused() == paused {
            return Ok(false);
        }
        match (self.rebalance(), &paused) {
            (Some(handle), Some(reason)) => handle.pause(reason).await?,
            (Some(handle), None) => handle.resume().await?,
            (None, _) => {
                let control = MoveControl {
                    format: 1,
                    pool_id: self.origin.pool_id,
                    from_epoch: self.source_epoch(),
                    to_epoch: self.to_epoch(),
                    paused: paused.clone(),
                };
                self.store
                    .save(
                        &MoveControl::record_name(self.to_epoch()),
                        control.encode()?,
                    )
                    .await?;
            }
        }
        *self.paused.write().unwrap_or_else(std::sync::PoisonError::into_inner) = paused;
        Ok(true)
    }

    /// Applies `uppers` as cut over: stored first, then read through the
    /// successor. Returns how many ranges were new here.
    ///
    /// # Errors
    ///
    /// A move that has not begun here, an unknown range, or the store failed.
    pub async fn apply_cut_over(&self, uppers: &[u64]) -> Result<usize, ClusterError> {
        let _step = self.steps.lock().await;
        match self.phase() {
            GenerationPhase::Finished => return Ok(0),
            GenerationPhase::Prepared => {
                return Err(ClusterError::Placement(
                    "ranges cut over before this node began the move".into(),
                ))
            }
            GenerationPhase::Moving => {}
        }
        let known: std::collections::BTreeSet<u64> =
            self.ranges.snapshot().iter().map(|range| range.upper_inclusive).collect();
        if let Some(unknown) = uppers.iter().find(|upper| !known.contains(upper)) {
            return Err(ClusterError::Placement(format!(
                "range ending at {unknown} is not an arc of this move"
            )));
        }
        let mut applied = CutOverRecord::of(&self.origin)?;
        applied.merge(self.ranges.cut_over_ranges());
        if !applied.merge(uppers.iter().copied()) {
            return Ok(0);
        }
        self.store
            .save(&applied_cut_over_name(self.to_epoch()), applied.encode()?)
            .await?;
        let before = self.ranges.counts().cut_over;
        for upper in uppers {
            self.ranges.advance_to(*upper, RangeState::CutOver)?;
        }
        let counts = self.ranges.counts();
        let new = counts.cut_over - before;
        if before / 10 != counts.cut_over / 10 || counts.cut_over == counts.total() {
            debug!(
                cut_over = counts.cut_over,
                total = counts.total(),
                "ranges of the pool's growth read through the successor generation"
            );
        }
        Ok(new)
    }

    /// Marks `uppers` verified here; a range further along stays.
    pub fn mark_verified(&self, uppers: &[u64]) {
        for upper in uppers {
            let _ = self.ranges.advance_to(*upper, RangeState::Verified);
        }
    }

    /// Settles this node on the successor: stored as `Stable(g + 1)` with the
    /// moment, then placed by the successor alone, then announced. `false`
    /// when it had settled already.
    ///
    /// # Errors
    ///
    /// A range not cut over here, a move that has not begun, or the store
    /// failed.
    pub async fn finish(&self) -> Result<bool, ClusterError> {
        let _step = self.steps.lock().await;
        if self.phase() == GenerationPhase::Finished {
            return Ok(false);
        }
        if self.phase() != GenerationPhase::Moving || !self.ranges.all_cut_over() {
            return Err(ClusterError::Placement(
                "the pool settles on its successor only once every range cut over".into(),
            ));
        }
        let finished = self.record().finish()?;
        let at = self.clock.now();
        self.store
            .save(
                &FinishedMark::record_name(self.to_epoch()),
                FinishedMark {
                    format: 1,
                    finished_at: at,
                }
                .encode()?,
            )
            .await?;
        pool_generation::store(&self.index, &finished).await?;
        self.ranges.finish(at)?;
        *self.record.write().unwrap_or_else(std::sync::PoisonError::into_inner) = finished.clone();
        self.gossip
            .announce_pool_generation(GenerationAnnouncement::of(&finished, false));
        debug!(
            epoch = finished.current.epoch,
            nodes = finished.current.nodes.len(),
            "pool growth finished: the successor generation alone places every key"
        );
        Ok(true)
    }

    /// Applies every range as cut over, then settles: the replicated
    /// generation says the pool settled, which it does only after every node
    /// applied every range.
    ///
    /// # Errors
    ///
    /// As [`Self::apply_cut_over`] and [`Self::finish`].
    pub async fn adopt_finish(&self) -> Result<bool, ClusterError> {
        if self.phase() == GenerationPhase::Prepared {
            self.begin().await?;
        }
        let every: Vec<u64> =
            self.ranges.snapshot().iter().map(|range| range.upper_inclusive).collect();
        self.apply_cut_over(&every).await?;
        self.finish().await
    }

    /// Whether this node handed over everything it holds of the old
    /// generation; a node of the successor alone holds nothing to hand.
    pub fn handed_over(&self) -> bool {
        if self.phase() == GenerationPhase::Finished
            || !self.origin.current.nodes.contains(&self.local)
        {
            return true;
        }
        self.rebalance().is_some_and(|handle| {
            let status = handle.status();
            status.items_total > 0 && status.items_done == status.items_total
        })
    }

    /// Where this node stands, as `PoolMove` reports it.
    pub async fn status(&self) -> proto::PoolMoveResponse {
        let counts = self.ranges.counts();
        let hint_targets = match &self.hints {
            Some(hints) => hints
                .targets_with_hints()
                .await
                .into_iter()
                .map(|node| node.to_bytes().to_vec())
                .collect(),
            None => Vec::new(),
        };
        proto::PoolMoveResponse {
            phase: match self.phase() {
                GenerationPhase::Prepared => 0,
                GenerationPhase::Moving => 1,
                GenerationPhase::Finished => 2,
            },
            ranges_total: counts.total() as u64,
            ranges_cut_over: counts.cut_over as u64,
            old_reads_in_flight: self.ranges.old_reads_in_flight(),
            handed_over: self.handed_over(),
            hint_targets,
            error: String::new(),
            upload_outcome: 0,
            upload_error: String::new(),
        }
    }
}

#[async_trait::async_trait]
impl crate::rpc::PoolMoveResponder for PoolTransition {
    async fn pool_move(&self, request: proto::PoolMoveRequest) -> proto::PoolMoveResponse {
        // A hand-over of an upload is asked by a coordinator or a hint, which
        // know the pool of the key and not the epochs of its move.
        if let Some(upload) = &request.hand_over_upload {
            let transfer =
                self.uploads.read().unwrap_or_else(std::sync::PoisonError::into_inner).clone();
            let (upload_outcome, upload_error) = match transfer {
                _ if request.pool_id != self.origin.pool_id.as_u32() => (
                    3,
                    format!(
                        "this node grows pool {}, not {}",
                        self.origin.pool_id, request.pool_id
                    ),
                ),
                Some(transfer) => transfer.answer_hand_over(upload).await.outcome(),
                None => (3, "this node hands no uploads over".into()),
            };
            return proto::PoolMoveResponse {
                upload_outcome,
                upload_error,
                ..self.status().await
            };
        }
        if request.pool_id != self.origin.pool_id.as_u32()
            || request.from_epoch != self.source_epoch()
            || request.to_epoch != self.to_epoch()
        {
            return proto::PoolMoveResponse {
                error: format!(
                    "this node moves pool {} from epoch {} to {}, not pool {} from {} to {}",
                    self.origin.pool_id,
                    self.source_epoch(),
                    self.to_epoch(),
                    request.pool_id,
                    request.from_epoch,
                    request.to_epoch
                ),
                ..proto::PoolMoveResponse::default()
            };
        }
        let applied = if request.finished {
            self.adopt_finish().await.map(|_| ())
        } else if !request.cut_over.is_empty() {
            self.apply_cut_over(&request.cut_over).await.map(|_| ())
        } else {
            Ok(())
        };
        let mut status = self.status().await;
        if let Err(e) = applied {
            status.error = e.to_string();
        }
        status
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rebalance::MemoryProgressStore;
    use crate::rpc::PoolMoveResponder;
    use crate::{PlacementRouter, PoolId};
    use s4_core::storage::IndexOptions;

    fn prepared() -> PoolGenerationRecord {
        PoolGenerationRecord::initial(PoolId(0), (1..=3).map(NodeId).collect())
            .unwrap()
            .configure((1..=6).map(NodeId).collect())
            .unwrap()
    }

    /// A router of node 1 with `record` attached, and its arcs.
    fn ranges_of(record: &PoolGenerationRecord) -> Arc<GenerationRanges> {
        let config = crate::ClusterConfig {
            pools: vec![crate::PoolConfig {
                pool_id: PoolId(0),
                name: "pool".into(),
                nodes: (1..=3u16)
                    .map(|n| crate::NodeAddr {
                        grpc_addr: format!("127.0.0.1:{}", 7000 + n).parse().unwrap(),
                        http_addr: format!("127.0.0.1:{}", 7100 + n).parse().unwrap(),
                        gossip_addr: format!("127.0.0.1:{}", 7200 + n).parse().unwrap(),
                    })
                    .collect(),
                replication_factor: Some(3),
                write_quorum: Some(2),
                read_quorum: Some(2),
                storage_class: s4_core::types::StorageClass::Standard,
            }],
            ..crate::ClusterConfig::default()
        };
        PlacementRouter::from_pool_node_ids(
            &config,
            NodeId(1),
            crate::BucketPlacement::new(),
            &std::collections::HashMap::from([(PoolId(0), (1..=3).map(NodeId).collect())]),
        )
        .unwrap()
        .with_pool_generation(record)
        .unwrap()
        .generation_ranges(&PoolId(0))
        .unwrap()
        .clone()
    }

    struct Node {
        transition: PoolTransition,
        index: Arc<IndexDb>,
        floor: PlacementFloor,
        _dir: tempfile::TempDir,
    }

    fn node(
        record: PoolGenerationRecord,
        store: Arc<dyn ProgressStore>,
        dir: Option<tempfile::TempDir>,
    ) -> Node {
        let dir = dir.unwrap_or_else(|| tempfile::tempdir().unwrap());
        let index = Arc::new(
            IndexDb::open(
                dir.path(),
                IndexOptions {
                    version_digests: true,
                },
            )
            .unwrap(),
        );
        let floor = PlacementFloor::new(0);
        let transition = PoolTransition::new(TransitionParts {
            ranges: ranges_of(&record),
            record,
            local: NodeId(1),
            floor: floor.clone(),
            index: index.clone(),
            store,
            gossip: Arc::new(GossipHandle::new_for_testing(Default::default())),
            hints: None,
            clock: Arc::new(HybridClock::new(NodeId(1))),
        })
        .unwrap();
        Node {
            transition,
            index,
            floor,
            _dir: dir,
        }
    }

    /// Beginning stores the move, fences the earlier generation and
    /// announces it; ranges are applied only once it began, and the pool
    /// settles only once every one of them cut over here.
    #[tokio::test]
    async fn the_steps_of_a_node_are_stored_and_ordered() {
        let store: Arc<dyn ProgressStore> = Arc::new(MemoryProgressStore::default());
        let node = node(prepared(), store.clone(), None);
        let transition = &node.transition;
        let first = transition.ranges().snapshot()[0].upper_inclusive;
        assert!(transition.apply_cut_over(&[first]).await.is_err());
        assert!(transition.finish().await.is_err());
        assert!(transition.begin().await.unwrap());
        assert!(!transition.begin().await.unwrap());
        assert_eq!(node.floor.get(), 0, "fenced only once every member began");
        transition.fence_earlier_generation();
        assert_eq!(node.floor.get(), 2);
        assert!(transition.fences_earlier_generation());
        assert!(pool_generation::load(&node.index).await.unwrap().unwrap().moving);
        assert_eq!(transition.apply_cut_over(&[first]).await.unwrap(), 1);
        assert_eq!(transition.apply_cut_over(&[first]).await.unwrap(), 0);
        assert!(transition.apply_cut_over(&[1]).await.is_err());
        assert!(transition.finish().await.is_err());
        assert!(transition.adopt_finish().await.unwrap());
        assert_eq!(transition.phase(), GenerationPhase::Finished);
        let durable = pool_generation::load(&node.index).await.unwrap().unwrap();
        assert_eq!(durable, prepared().begin().unwrap().finish().unwrap());
        assert!(FinishedMark::load(store.as_ref(), 2).await.unwrap().is_some());
        assert_eq!(transition.apply_cut_over(&[first]).await.unwrap(), 0);
        assert!(transition.handed_over());
    }

    /// After a restart the ranges a node read through the successor read
    /// through it again before it serves anything, and the floor is raised.
    #[tokio::test]
    async fn a_restarted_node_reads_again_through_the_successor_what_it_cut_over() {
        let store: Arc<dyn ProgressStore> = Arc::new(MemoryProgressStore::default());
        let before = node(prepared(), store.clone(), None);
        before.transition.begin().await.unwrap();
        let cut: Vec<u64> = before
            .transition
            .ranges()
            .snapshot()
            .iter()
            .take(5)
            .map(|range| range.upper_inclusive)
            .collect();
        before.transition.apply_cut_over(&cut).await.unwrap();
        let moving = pool_generation::load(&before.index).await.unwrap().unwrap();
        let after = node(moving, store, None);
        assert_eq!(after.transition.ranges().counts().cut_over, 0);
        after.transition.restore().await.unwrap();
        assert_eq!(after.transition.phase(), GenerationPhase::Moving);
        assert_eq!(after.floor.get(), 2);
        assert_eq!(after.transition.ranges().cut_over_ranges(), {
            let mut cut = cut.clone();
            cut.sort_unstable();
            cut
        });
    }

    /// A node restarted in the move before any range cut over does not fence
    /// the earlier generation at once: a member may still be prepared and
    /// write by it. The driver fences once every member announced the move.
    #[tokio::test]
    async fn a_node_restarted_before_any_cut_over_leaves_fencing_to_the_driver() {
        let store: Arc<dyn ProgressStore> = Arc::new(MemoryProgressStore::default());
        let before = node(prepared(), store.clone(), None);
        before.transition.begin().await.unwrap();
        let moving = pool_generation::load(&before.index).await.unwrap().unwrap();
        let after = node(moving, store, None);
        after.transition.restore().await.unwrap();
        assert_eq!(after.transition.phase(), GenerationPhase::Moving);
        assert_eq!(
            after.floor.get(),
            0,
            "no member is fenced before every one began"
        );
        after.transition.fence_earlier_generation();
        assert_eq!(after.floor.get(), 2);
    }

    /// `PoolMove` answers only for this move, and hands the node the ranges
    /// and the settling it carries.
    #[tokio::test]
    async fn pool_move_answers_for_its_own_move_only() {
        let store: Arc<dyn ProgressStore> = Arc::new(MemoryProgressStore::default());
        let node = node(prepared().begin().unwrap(), store, None);
        node.transition.restore().await.unwrap();
        let request = proto::PoolMoveRequest {
            pool_id: 0,
            from_epoch: 1,
            to_epoch: 2,
            ..proto::PoolMoveRequest::default()
        };
        let other = node
            .transition
            .pool_move(proto::PoolMoveRequest {
                to_epoch: 3,
                ..request.clone()
            })
            .await;
        assert!(!other.error.is_empty());
        let status = node.transition.pool_move(request.clone()).await;
        assert_eq!((status.phase, status.ranges_cut_over), (1, 0));
        assert!(
            !status.handed_over,
            "node 1 of the old generation has not moved its keys"
        );
        let first = node.transition.ranges().snapshot()[0].upper_inclusive;
        let status = node
            .transition
            .pool_move(proto::PoolMoveRequest {
                cut_over: vec![first],
                ..request.clone()
            })
            .await;
        assert_eq!((status.ranges_cut_over, status.error.as_str()), (1, ""));
        let status = node
            .transition
            .pool_move(proto::PoolMoveRequest {
                finished: true,
                ..request
            })
            .await;
        assert_eq!(status.phase, 2);
        assert_eq!(status.ranges_cut_over, status.ranges_total);
    }
}
