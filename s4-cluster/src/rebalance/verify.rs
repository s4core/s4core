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

//! The driver of a pool's growth: its beginning, the verification and
//! cut-over of every range, the common barrier and the pool's settling (plan
//! ERASURE-CODING-FIXING-POOL-GROWTH-REBALANCE.md, phase 4).
//!
//! Every node runs a [`TransitionDriver`]:
//!
//! - **Prepared**: the move begins once the readiness gate opens (every
//!   member of the grown pool announced both generations at protocol 9, its
//!   bucket metadata settled, the new members have room), or as soon as a
//!   member announces that it began.
//! - **Moving**: the node applies the ranges the replicated cut-over record
//!   names, and settles once the replicated generation says the pool did.
//! - The member with the lowest identity among those alive verifies the
//!   ranges ([`RangeVerifier`]); a second one by a split view only repeats
//!   the same checks and merges the same record.
//!
//! **Verification of a range.** It begins once every member announced the
//! move for longer than a write placed by the earlier generation may take,
//! and once every member handed over what it holds of the old generation.
//! Every node owning the range by either generation, in any class, reports
//! its version digests of the range, a page at a time, merged in ring order.
//! For every key, the newest version that as many old owners as a confirmed
//! write hold, or have gone past, must be held — or something newer — by
//! every new owner; a tombstone older than `gc_grace` may also be absent.
//! A key that falls short is brought to its new owners and the range is
//! verified again later. A range is verified only while no node keeps a hint
//! for a new owner of the classes it holds.
//!
//! **Cut-over.** Verified ranges are merged into the replicated cut-over
//! record (`__system__`, written through the joint quorum with an HLC), then
//! handed to every member, which stores them before reading them through the
//! successor. Members that missed the hand-over read the record.
//!
//! **Barrier and settling.** Once every range cut over, the pool settles
//! when every member reports every range read through the successor and no
//! placement decision reading through the old owners in flight. The
//! replicated generation record becomes `Stable(g + 1)`, and every member
//! settles: from then on writes need the successor's quorum alone.

use std::collections::{BTreeMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::StreamExt;
use s4_core::storage::{VersionDigest, VersionKind};
use s4_core::types::SYSTEM_BUCKET;
use s4_core::StorageEngine;
use tokio::sync::watch;
use tokio::time::Instant;
use tracing::{debug, info, warn};

use super::progress::WorkItem;
use super::transition::PoolTransition;
use crate::clock::hlc::now_millis;
use crate::identity::NodeStatus;
use crate::placement::{GenerationOwners, GenerationPhase, GenerationRange, ReplicationClass};
use crate::pool_generation::{
    self, CutOverRecord, GenerationAnnouncement, PauseRecord, PoolGenerationRecord,
};
use crate::repair::reconcile::{newest_confirmed, KeyRef, KeyStateSource};
use crate::repair::Reconciler;
use crate::rpc::{proto, NodeClient, PoolMoveResponder};
use crate::{
    ClusterError, GossipHandle, NodeId, PlacementRouter, QuorumReadCoordinator,
    QuorumWriteCoordinator,
};

/// Records of `__system__` read and written through the pool's quorums.
#[async_trait::async_trait]
pub trait ReplicatedRecords: Send + Sync {
    /// The record at `key`, `None` when there is none.
    async fn read(&self, key: &str) -> Result<Option<Vec<u8>>, ClusterError>;

    /// Writes the record at `key`.
    async fn write(&self, key: &str, data: Vec<u8>) -> Result<(), ClusterError>;
}

/// [`ReplicatedRecords`] through this node's quorum coordinators.
pub struct QuorumRecords {
    read: Arc<QuorumReadCoordinator>,
    write: Arc<QuorumWriteCoordinator>,
}

impl QuorumRecords {
    /// Reads through `read`, writes through `write`.
    pub fn new(read: Arc<QuorumReadCoordinator>, write: Arc<QuorumWriteCoordinator>) -> Self {
        Self { read, write }
    }
}

#[async_trait::async_trait]
impl ReplicatedRecords for QuorumRecords {
    async fn read(&self, key: &str) -> Result<Option<Vec<u8>>, ClusterError> {
        match self.read.read(SYSTEM_BUCKET, key).await {
            Ok(result) => Ok(Some(result.data)),
            Err(ClusterError::ObjectNotFound { .. }) => Ok(None),
            Err(e) => Err(e),
        }
    }

    async fn write(&self, key: &str, data: Vec<u8>) -> Result<(), ClusterError> {
        self.write
            .write(
                SYSTEM_BUCKET,
                key,
                &data,
                "application/json",
                &std::collections::HashMap::new(),
            )
            .await
            .map(|_| ())
    }
}

/// How the driver and the verifier pace themselves.
#[derive(Debug, Clone)]
pub struct TransitionConfig {
    /// How often the driver looks at the pool.
    pub poll: Duration,
    /// How long after every member began the verification waits: the
    /// longest a write placed by the earlier generation may still land.
    pub settle: Duration,
    /// Version digests asked of a node per page.
    pub page_size: usize,
    /// Ranges verified at the same time.
    pub concurrent_ranges: usize,
    /// Keys brought to their new owners per range and pass, at most.
    pub repairs_per_range: usize,
    /// Ranges verified between two publications of the cut-over record.
    pub publish_every: usize,
    /// How long one call to a member may take.
    pub call_timeout: Duration,
    /// A tombstone older than this may be absent from a new owner.
    pub gc_grace: Duration,
}

impl Default for TransitionConfig {
    fn default() -> Self {
        Self {
            poll: Duration::from_secs(2),
            settle: Duration::from_secs(10),
            page_size: 512,
            concurrent_ranges: 2,
            repairs_per_range: 256,
            publish_every: 32,
            call_timeout: Duration::from_secs(30),
            gc_grace: Duration::from_secs(86_400),
        }
    }
}

/// What the verification of one range found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RangeCheck {
    /// Every new owner holds what the old owners confirmed.
    Verified,
    /// Not now, and why: a node that did not answer, keys brought to their
    /// new owners, hints waiting.
    NotYet(String),
}

/// What one key of a range needs ([`check_key`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyCheck {
    /// Every new owner holds the confirmed version, or something newer.
    Covered,
    /// These new owners do not.
    Behind(Vec<NodeId>),
}

/// Whether the new owners of a key hold what its old owners confirmed.
///
/// `held` is what each node that answered holds of the key. The confirmed
/// version is the newest that at least `owners.old_write_quorum` old owners
/// hold or have gone past: every write the old generation confirmed is at or
/// below it. A new owner covers it when its version
/// [covers](VersionDigest::covers) it; a tombstone older than `gc_grace`
/// (at `now_ms`) may also be absent, as the move does not carry one to a
/// replica without the key.
pub fn check_key(
    owners: &GenerationOwners,
    held: &[(NodeId, VersionDigest)],
    now_ms: u64,
    gc_grace: Duration,
) -> KeyCheck {
    let of = |node: &NodeId| held.iter().find(|(n, _)| n == node).map(|(_, d)| *d);
    let old: Vec<VersionDigest> = owners.old.iter().filter_map(of).collect();
    let quorum = usize::from(owners.old_write_quorum).max(1);
    let confirmed = old
        .iter()
        .filter(|candidate| old.iter().filter(|other| other.covers(candidate)).count() >= quorum)
        .fold(None::<VersionDigest>, |best, candidate| match best {
            Some(best) if best.covers(candidate) => Some(best),
            _ => Some(*candidate),
        });
    let Some(confirmed) = confirmed else {
        return KeyCheck::Covered;
    };
    let grace = u64::try_from(gc_grace.as_millis()).unwrap_or(u64::MAX);
    let old_tombstone = confirmed.kind == VersionKind::Tombstone
        && now_ms.saturating_sub(confirmed.hlc_timestamp) > grace;
    let behind: Vec<NodeId> = owners
        .new
        .iter()
        .copied()
        .filter(|node| match of(node) {
            Some(version) => !version.covers(&confirmed),
            None => !old_tombstone,
        })
        .collect();
    if behind.is_empty() {
        KeyCheck::Covered
    } else {
        KeyCheck::Behind(behind)
    }
}

/// The pages of every node asked, merged in ring order.
struct NodePages {
    node: NodeId,
    after: Option<String>,
    exhausted: bool,
    buffer: VecDeque<(u64, String, VersionDigest)>,
}

/// Verifies ranges and drives the cut-over, the barrier and the settling
/// from the node that leads.
pub struct RangeVerifier {
    transition: Arc<PoolTransition>,
    router: Arc<PlacementRouter>,
    rpc_client: Arc<NodeClient>,
    gossip: Arc<GossipHandle>,
    records: Arc<dyn ReplicatedRecords>,
    source: Arc<dyn KeyStateSource>,
    reconciler: Arc<Reconciler>,
    storage: Arc<dyn StorageEngine>,
    config: TransitionConfig,
    all_began_at: Mutex<Option<Instant>>,
}

/// The node's statuses, by member.
type Statuses = BTreeMap<u128, (NodeId, proto::PoolMoveResponse)>;

impl RangeVerifier {
    /// The verifier of `transition`'s move.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        transition: Arc<PoolTransition>,
        router: Arc<PlacementRouter>,
        rpc_client: Arc<NodeClient>,
        gossip: Arc<GossipHandle>,
        records: Arc<dyn ReplicatedRecords>,
        source: Arc<dyn KeyStateSource>,
        reconciler: Arc<Reconciler>,
        storage: Arc<dyn StorageEngine>,
        config: TransitionConfig,
    ) -> Self {
        Self {
            transition,
            router,
            rpc_client,
            gossip,
            records,
            source,
            reconciler,
            storage,
            config,
            all_began_at: Mutex::new(None),
        }
    }

    fn members(&self) -> Vec<NodeId> {
        self.transition
            .origin()
            .proposed
            .as_ref()
            .map_or_else(Vec::new, |next| next.nodes.clone())
    }

    fn request(&self) -> proto::PoolMoveRequest {
        proto::PoolMoveRequest {
            pool_id: self.transition.origin().pool_id.as_u32(),
            from_epoch: self.transition.source_epoch(),
            to_epoch: self.transition.to_epoch(),
            ..proto::PoolMoveRequest::default()
        }
    }

    /// Asks `node` with `request`, this node without the network.
    async fn ask(
        &self,
        node: NodeId,
        request: proto::PoolMoveRequest,
    ) -> Result<proto::PoolMoveResponse, String> {
        if node == self.transition.local() {
            return Ok(self.transition.pool_move(request).await);
        }
        match tokio::time::timeout(
            self.config.call_timeout,
            self.rpc_client.send_pool_move(node, request),
        )
        .await
        {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(e)) => Err(e.to_string()),
            Err(_) => Err("did not answer in time".into()),
        }
    }

    async fn asked(&self, node: NodeId) -> (NodeId, Result<proto::PoolMoveResponse, String>) {
        (node, self.ask(node, self.request()).await)
    }

    async fn checked(&self, range: &GenerationRange, hints: &HashSet<NodeId>) -> (u64, RangeCheck) {
        (range.upper_inclusive, self.verify_range(range, hints).await)
    }

    /// Every member's status, or the first that did not give one.
    async fn statuses(&self) -> Result<Statuses, String> {
        let mut asks = Vec::new();
        for node in self.members() {
            asks.push(self.asked(node));
        }
        let asked = futures::future::join_all(asks).await;
        let mut statuses = Statuses::new();
        for (node, answer) in asked {
            let status = answer.map_err(|why| format!("node {node} did not report: {why}"))?;
            if !status.error.is_empty() {
                return Err(format!("node {node}: {}", status.error));
            }
            statuses.insert(node.0, (node, status));
        }
        Ok(statuses)
    }

    /// Whether every member announced the move began, as both generations
    /// this node knows.
    fn first_not_begun(&self) -> Option<NodeId> {
        let expected = GenerationAnnouncement::of(&self.transition.record(), false);
        self.members().into_iter().find(|node| {
            !self.gossip.pool_generation(node).is_some_and(|announced| {
                announced.moving
                    && announced.current == expected.current
                    && announced.proposed == expected.proposed
            })
        })
    }

    /// The replicated cut-over record, empty when none was written.
    async fn cut_over_record(&self) -> Result<CutOverRecord, String> {
        let origin = self.transition.origin();
        let key = pool_generation::cut_over_key(origin.pool_id, self.transition.to_epoch());
        let empty = CutOverRecord::of(origin).map_err(|e| e.to_string())?;
        match self.records.read(&key).await {
            Ok(Some(bytes)) => {
                let record = CutOverRecord::decode(&bytes).map_err(|e| e.to_string())?;
                Ok(if record.belongs_to(origin) {
                    record
                } else {
                    empty
                })
            }
            Ok(None) => Ok(empty),
            Err(e) => Err(format!("the cut-over record could not be read: {e}")),
        }
    }

    /// One pass of the leader: why the move waits, `None` once the pool
    /// settled.
    pub async fn step(&self) -> Option<String> {
        if self.transition.phase() != GenerationPhase::Moving {
            return None;
        }
        if let Some(node) = self.first_not_begun() {
            *self.all_began_at.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = None;
            return Some(format!("waiting for node {node} to begin the move"));
        }
        let began = *self
            .all_began_at
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get_or_insert_with(Instant::now);
        if began.elapsed() < self.config.settle {
            return Some(
                "every member began the move; waiting for writes placed by the earlier \
                 generation to end"
                    .into(),
            );
        }
        let statuses = match self.statuses().await {
            Ok(statuses) => statuses,
            Err(why) => return Some(why),
        };
        for (node, status) in statuses.values() {
            if status.phase == 0 {
                return Some(format!("waiting for node {node} to begin the move"));
            }
            if !status.handed_over {
                return Some(format!(
                    "waiting for node {node} to hand over what it holds of the old generation"
                ));
            }
        }
        let mut record = match self.cut_over_record().await {
            Ok(record) => record,
            Err(why) => return Some(why),
        };
        if let Err(e) = self.transition.apply_cut_over(&record.ranges).await {
            return Some(format!(
                "the cut-over record could not be applied here: {e}"
            ));
        }
        let cut: HashSet<u64> = record.ranges.iter().copied().collect();
        let pending: Vec<GenerationRange> = self
            .transition
            .ranges()
            .snapshot()
            .into_iter()
            .filter(|range| !cut.contains(&range.upper_inclusive))
            .collect();
        if pending.is_empty() {
            return self.settle_pool(&statuses).await;
        }
        let hints: HashSet<NodeId> = statuses
            .values()
            .flat_map(|(_, status)| status.hint_targets.iter())
            .filter_map(|bytes| NodeId::from_bytes(bytes).ok())
            .collect();
        let mut waiting = None;
        for chunk in pending.chunks(self.config.publish_every.max(1)) {
            let mut checks = Vec::with_capacity(chunk.len());
            for range in chunk {
                checks.push(self.checked(range, &hints));
            }
            let checked: Vec<(u64, RangeCheck)> = futures::stream::iter(checks)
                .buffer_unordered(self.config.concurrent_ranges.max(1))
                .collect()
                .await;
            let mut verified = Vec::new();
            for (upper, check) in checked {
                match check {
                    RangeCheck::Verified => verified.push(upper),
                    RangeCheck::NotYet(why) => {
                        waiting.get_or_insert(why);
                    }
                }
            }
            if verified.is_empty() {
                continue;
            }
            self.transition.mark_verified(&verified);
            if let Err(why) = self.publish(&mut record, &verified).await {
                return Some(why);
            }
        }
        waiting.or_else(|| Some("ranges were cut over; the barrier follows".into()))
    }

    /// Merges `verified` into the replicated record, hands them to every
    /// member and applies them here.
    async fn publish(&self, record: &mut CutOverRecord, verified: &[u64]) -> Result<(), String> {
        let origin = self.transition.origin();
        let key = pool_generation::cut_over_key(origin.pool_id, self.transition.to_epoch());
        // Another verifier may have published meanwhile: merge what it wrote.
        if let Ok(current) = self.cut_over_record().await {
            record.merge(current.ranges);
        }
        record.merge(verified.iter().copied());
        let bytes = record.encode().map_err(|e| e.to_string())?;
        self.records
            .write(&key, bytes)
            .await
            .map_err(|e| format!("the cut-over record could not be written: {e}"))?;
        let request = proto::PoolMoveRequest {
            cut_over: verified.to_vec(),
            ..self.request()
        };
        for node in self.members() {
            if let Err(why) = self.ask(node, request.clone()).await {
                warn!(%node, %why, "a member did not take the ranges cut over; it reads them from the record");
            }
        }
        Ok(())
    }

    /// The common barrier, then the pool's settling.
    async fn settle_pool(&self, statuses: &Statuses) -> Option<String> {
        for (node, status) in statuses.values() {
            if status.ranges_cut_over < status.ranges_total || status.ranges_total == 0 {
                return Some(format!(
                    "waiting for node {node} to read every range through the successor ({} of {})",
                    status.ranges_cut_over, status.ranges_total
                ));
            }
            if status.old_reads_in_flight > 0 {
                return Some(format!(
                    "waiting for {} request(s) on node {node} placed by the old owners to end",
                    status.old_reads_in_flight
                ));
            }
        }
        let finished = match self.transition.record().finish() {
            Ok(finished) => finished,
            Err(e) => return Some(e.to_string()),
        };
        let bytes = match finished.encode() {
            Ok(bytes) => bytes,
            Err(e) => return Some(e.to_string()),
        };
        let key = pool_generation::object_key(finished.pool_id);
        if let Err(e) = self.records.write(&key, bytes).await {
            return Some(format!("the settled generation could not be written: {e}"));
        }
        info!(
            epoch = finished.current.epoch,
            "every member reads the successor and no request placed by the old owners is in \
             flight: the pool settles on its successor"
        );
        let request = proto::PoolMoveRequest {
            finished: true,
            ..self.request()
        };
        for node in self.members() {
            if let Err(why) = self.ask(node, request.clone()).await {
                warn!(%node, %why, "a member did not take the pool's settling; it reads it from the record");
            }
        }
        None
    }

    /// Verifies `range` ([`check_key`] for every key), bringing keys that
    /// fall short to their new owners.
    pub async fn verify_range(
        &self,
        range: &GenerationRange,
        hints: &HashSet<NodeId>,
    ) -> RangeCheck {
        let pool = self.transition.origin().pool_id;
        let classes = [
            ReplicationClass::Data,
            ReplicationClass::BucketMetadata,
            ReplicationClass::ClusterControl,
        ];
        let mut owners = Vec::new();
        for class in classes {
            match self.router.generation_owners(&pool, class, range.upper_inclusive) {
                Some(found) => owners.push((class, found)),
                None => return RangeCheck::NotYet("the pool has no successor placement".into()),
            }
        }
        let mut asked: Vec<NodeId> = owners
            .iter()
            .flat_map(|(_, owners)| owners.old.iter().chain(owners.new.iter()).copied())
            .collect();
        asked.sort_unstable_by_key(|node| node.0);
        asked.dedup();
        let now = now_millis();
        let mut behind: Vec<(String, ReplicationClass)> = Vec::new();
        let mut present: HashSet<ReplicationClass> = HashSet::new();
        for (from, to) in WorkItem::of_range(range).segments() {
            let mut pages: Vec<NodePages> = asked
                .iter()
                .map(|node| NodePages {
                    node: *node,
                    after: None,
                    exhausted: false,
                    buffer: VecDeque::new(),
                })
                .collect();
            loop {
                for page in pages.iter_mut().filter(|p| p.buffer.is_empty() && !p.exhausted) {
                    if let Err(why) = self.fill(page, from, to).await {
                        return RangeCheck::NotYet(why);
                    }
                }
                if pages.iter().all(|p| p.exhausted && p.buffer.is_empty()) {
                    break;
                }
                // Every key up to the smallest last key of a page that may go
                // on has been read from every node.
                let bound = pages
                    .iter()
                    .filter(|p| !p.exhausted)
                    .filter_map(|p| {
                        p.buffer.back().map(|(position, key, _)| (*position, key.clone()))
                    })
                    .min();
                let mut keys: BTreeMap<(u64, String), Vec<(NodeId, VersionDigest)>> =
                    BTreeMap::new();
                for page in &mut pages {
                    while let Some((position, key, _)) = page.buffer.front() {
                        if bound
                            .as_ref()
                            .is_some_and(|bound| (*position, key) > (bound.0, &bound.1))
                        {
                            break;
                        }
                        let Some((position, key, digest)) = page.buffer.pop_front() else {
                            break;
                        };
                        keys.entry((position, key)).or_default().push((page.node, digest));
                    }
                }
                for ((_, key), held) in keys {
                    let class = ReplicationClass::of_full_key(&key);
                    present.insert(class);
                    let Some((_, class_owners)) = owners.iter().find(|(c, _)| *c == class) else {
                        continue;
                    };
                    if let KeyCheck::Behind(_) =
                        check_key(class_owners, &held, now, self.config.gc_grace)
                    {
                        behind.push((key, class));
                    }
                }
            }
        }
        if !behind.is_empty() {
            let total = behind.len();
            let mut brought = 0;
            // Keys the digests could not order — one HLC and kind, two ETags —
            // that the total order of versions finds covered on every new
            // owner: nothing to bring.
            let mut covered = 0;
            let mut last_error = None;
            for (key, class) in behind.iter().take(self.config.repairs_per_range) {
                let Some((_, class_owners)) = owners.iter().find(|(c, _)| c == class) else {
                    continue;
                };
                match self.bring(key, class_owners).await {
                    Ok(0) => covered += 1,
                    Ok(count) => brought += count,
                    Err(why) => last_error = Some(why),
                }
            }
            if covered < total {
                return RangeCheck::NotYet(format!(
                    "range ending at {}: {} key(s) behind on the new owners, {brought} \
                     copy(ies) brought{}",
                    range.upper_inclusive,
                    total - covered,
                    last_error.map(|why| format!("; {why}")).unwrap_or_default()
                ));
            }
        }
        for (class, class_owners) in &owners {
            if !present.contains(class) {
                continue;
            }
            if let Some(node) = class_owners.new.iter().find(|node| hints.contains(node)) {
                return RangeCheck::NotYet(format!(
                    "range ending at {}: hints for its new owner {node} wait to be delivered",
                    range.upper_inclusive
                ));
            }
        }
        RangeCheck::Verified
    }

    /// Reads the next page of `page.node` into its buffer; this node's own
    /// from its storage.
    async fn fill(&self, page: &mut NodePages, from: u64, to: Option<u64>) -> Result<(), String> {
        if page.node == self.transition.local() {
            let entries = self
                .storage
                .version_digests_in(from, to, page.after.as_deref(), self.config.page_size)
                .await
                .map_err(|e| format!("this node's digests: {e}"))?;
            page.exhausted = entries.len() < self.config.page_size;
            for entry in entries {
                page.after = Some(entry.key.clone());
                page.buffer.push_back((entry.ring_position, entry.key, entry.digest));
            }
            return Ok(());
        }
        let request = proto::RangeDigestsRequest {
            pool_id: self.transition.origin().pool_id.as_u32(),
            from,
            to,
            after_key: page.after.clone(),
            max_keys: u32::try_from(self.config.page_size).unwrap_or(u32::MAX),
        };
        let response = match tokio::time::timeout(
            self.config.call_timeout,
            self.rpc_client.send_range_digests(page.node, request),
        )
        .await
        {
            Ok(Ok(response)) if response.error.is_empty() => response,
            Ok(Ok(response)) => return Err(format!("node {}: {}", page.node, response.error)),
            Ok(Err(e)) => {
                return Err(format!(
                    "node {} did not report its digests: {e}",
                    page.node
                ))
            }
            Err(_) => {
                return Err(format!(
                    "node {} did not report its digests in time",
                    page.node
                ))
            }
        };
        page.exhausted = response.keys.len() < self.config.page_size;
        for entry in response.keys {
            let digest = VersionDigest::decode(&entry.digest)
                .map_err(|e| format!("node {}: {e}", page.node))?;
            page.after = Some(entry.key.clone());
            page.buffer.push_back((entry.ring_position, entry.key, digest));
        }
        Ok(())
    }

    /// Brings the new owners of `full_key` to the version its old owners
    /// confirmed; how many took it. Zero when, by the total order of
    /// versions ([`s4_core::storage::VersionRank`]), every new owner holds it
    /// or something newer, or no version is confirmed: the key is covered,
    /// even where its digests alone could not tell.
    async fn bring(&self, full_key: &str, owners: &GenerationOwners) -> Result<usize, String> {
        let (bucket, key) = full_key
            .split_once('/')
            .ok_or_else(|| format!("key {full_key} has no bucket"))?;
        let versions = self
            .source
            .key_versions(bucket, key)
            .await
            .map_err(|e| format!("{full_key}: {e}"))?;
        let Some(confirmed) = newest_confirmed(
            &versions.answers,
            &owners.old,
            usize::from(owners.old_write_quorum),
        ) else {
            return Ok(0);
        };
        if confirmed.kind == VersionKind::ErasureCoded {
            return Err(format!(
                "{full_key}: an erasure-coded entry missing on a new owner waits for the move of \
                 its namespace entry"
            ));
        }
        self.reconciler
            .bring_confirmed(KeyRef { bucket, key }, &versions, confirmed, &owners.new)
            .await
            .map_err(|why| format!("{full_key}: {why}"))
    }
}

/// Why the move cannot begin yet, if it cannot: the readiness gate.
pub type BeginGate = Arc<dyn Fn(&PoolGenerationRecord) -> Result<(), String> + Send + Sync>;

/// Every node's driver of the pool's growth; see the module documentation.
pub struct TransitionDriver {
    transition: Arc<PoolTransition>,
    verifier: RangeVerifier,
    records: Arc<dyn ReplicatedRecords>,
    gossip: Arc<GossipHandle>,
    gate: BeginGate,
    on_begin: Arc<dyn Fn() + Send + Sync>,
    poll: Duration,
}

impl TransitionDriver {
    /// The driver of `verifier`'s move: it begins once `gate` passes, and
    /// calls `on_begin` once the move runs on this node (also when it was
    /// running before a restart).
    pub fn new(
        verifier: RangeVerifier,
        gate: BeginGate,
        on_begin: Arc<dyn Fn() + Send + Sync>,
    ) -> Self {
        Self {
            transition: verifier.transition.clone(),
            records: verifier.records.clone(),
            gossip: verifier.gossip.clone(),
            poll: verifier.config.poll,
            verifier,
            gate,
            on_begin,
        }
    }

    /// Runs until the pool settled on its successor or `shutdown`.
    pub async fn run(self, mut shutdown: watch::Receiver<bool>) {
        let mut ticks = tokio::time::interval(self.poll);
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut last: Option<String> = None;
        let mut started = false;
        loop {
            tokio::select! {
                _ = ticks.tick() => {}
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return;
                    }
                    continue;
                }
            }
            if self.transition.phase() != GenerationPhase::Finished {
                self.sync_pause().await;
            }
            let reason = match self.transition.phase() {
                GenerationPhase::Prepared => self.try_begin().await,
                GenerationPhase::Moving => {
                    if !started {
                        started = true;
                        (self.on_begin)();
                    }
                    self.moving().await
                }
                GenerationPhase::Finished => return,
            };
            if reason != last {
                if let Some(reason) = &reason {
                    debug!(%reason, "pool growth waits");
                }
                self.transition.set_waiting(reason.clone());
                last = reason;
            }
        }
    }

    /// Pauses or resumes the move of this node's keys as the pool's pause
    /// record says; a record that cannot be read leaves things as they are.
    async fn sync_pause(&self) {
        let origin = self.transition.origin().clone();
        let key = pool_generation::pause_key(origin.pool_id, self.transition.to_epoch());
        let wanted = match self.records.read(&key).await {
            Ok(Some(bytes)) => match PauseRecord::decode(&bytes) {
                Ok(record) if record.belongs_to(&origin) => record.paused,
                Ok(_) => None,
                Err(e) => {
                    warn!(error = %e, "the pool's pause record could not be decoded");
                    return;
                }
            },
            Ok(None) => None,
            Err(e) => {
                debug!(error = %e, "the pool's pause record could not be read");
                return;
            }
        };
        match self.transition.apply_pause(wanted.clone()).await {
            Ok(true) => match &wanted {
                Some(reason) => {
                    info!(%reason, "Pool growth paused: this node moves no keys until it is resumed")
                }
                None => info!("Pool growth resumed: this node moves its keys again"),
            },
            Ok(false) => {}
            Err(e) => warn!(error = %e, "the pool's pause could not be applied here"),
        }
    }

    /// Begins the move once the gate passes or a member began it.
    async fn try_begin(&self) -> Option<String> {
        let record = self.transition.record();
        let expected = GenerationAnnouncement::of(&record, false);
        let next = record.proposed.as_ref()?;
        let begun_elsewhere = next.nodes.iter().any(|node| {
            *node != self.transition.local()
                && self.gossip.pool_generation(node).is_some_and(|announced| {
                    announced.moving
                        && announced.current == expected.current
                        && announced.proposed == expected.proposed
                })
        });
        if !begun_elsewhere {
            if let Err(reason) = (self.gate)(&record) {
                return Some(reason);
            }
        }
        match self.transition.begin().await {
            Ok(_) => None,
            Err(e) => Some(format!("the move could not begin: {e}")),
        }
    }

    /// Applies the replicated state, and leads when this node is the leader.
    async fn moving(&self) -> Option<String> {
        if self.verifier.first_not_begun().is_none() {
            self.transition.fence_earlier_generation();
        }
        let origin = self.transition.origin().clone();
        match self.records.read(&pool_generation::object_key(origin.pool_id)).await {
            Ok(Some(bytes)) => {
                if let Ok(found) = PoolGenerationRecord::decode(&bytes) {
                    if self.transition.record().finishes_into(&found) {
                        return match self.transition.adopt_finish().await {
                            Ok(_) => None,
                            Err(e) => Some(format!("the pool settled, but not here yet: {e}")),
                        };
                    }
                }
            }
            Ok(None) => {}
            Err(e) => warn!(error = %e, "the replicated pool generation could not be read"),
        }
        match self.verifier.cut_over_record().await {
            Ok(record) if !record.ranges.is_empty() => {
                if let Err(e) = self.transition.apply_cut_over(&record.ranges).await {
                    warn!(error = %e, "the replicated cut-over record could not be applied");
                }
            }
            Ok(_) => {}
            Err(why) => warn!(%why, "the replicated cut-over record could not be read"),
        }
        if self.leader() != self.transition.local() {
            return None;
        }
        self.verifier.step().await
    }

    /// The member that verifies: the lowest identity among those alive.
    fn leader(&self) -> NodeId {
        let local = self.transition.local();
        self.verifier
            .members()
            .into_iter()
            .filter(|node| {
                *node == local
                    || self
                        .gossip
                        .get_node(node)
                        .is_some_and(|state| state.status == NodeStatus::Alive)
            })
            .min_by_key(|node| node.0)
            .unwrap_or(local)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn digest(wall: u64, kind: VersionKind, etag: u8) -> VersionDigest {
        VersionDigest {
            hlc_timestamp: wall,
            hlc_logical: 0,
            origin_node_id: 1,
            etag_hash: [etag; 32],
            kind,
        }
    }

    fn owners() -> GenerationOwners {
        GenerationOwners {
            old: vec![NodeId(1), NodeId(2), NodeId(3)],
            old_write_quorum: 2,
            new: vec![NodeId(1), NodeId(4), NodeId(5)],
        }
    }

    const GRACE: Duration = Duration::from_secs(100);

    /// A version the old owners confirmed must be on every new owner; one
    /// that a failed write left on a single old owner is not required.
    #[test]
    fn the_new_owners_must_hold_what_the_old_owners_confirmed() {
        let v1 = digest(1_000, VersionKind::Live, 1);
        let v2 = digest(2_000, VersionKind::Live, 2);
        let held = vec![(NodeId(1), v1), (NodeId(2), v1), (NodeId(3), v1)];
        assert_eq!(
            check_key(&owners(), &held, 5_000, GRACE),
            KeyCheck::Behind(vec![NodeId(4), NodeId(5)])
        );
        let mut moved = held.clone();
        moved.extend([(NodeId(4), v1), (NodeId(5), v2)]);
        assert_eq!(
            check_key(&owners(), &moved, 5_000, GRACE),
            KeyCheck::Covered
        );
        // v2 is on one old owner only: v1 is what was confirmed.
        let failed = vec![
            (NodeId(1), v2),
            (NodeId(2), v1),
            (NodeId(3), v1),
            (NodeId(4), v1),
            (NodeId(5), v1),
        ];
        assert_eq!(
            check_key(&owners(), &failed, 5_000, GRACE),
            KeyCheck::Covered
        );
        // Two old owners passed v1: the newer of them counts.
        let confirmed = vec![
            (NodeId(1), v2),
            (NodeId(2), v2),
            (NodeId(3), v1),
            (NodeId(4), v1),
            (NodeId(5), v2),
        ];
        assert_eq!(
            check_key(&owners(), &confirmed, 5_000, GRACE),
            KeyCheck::Behind(vec![NodeId(4)])
        );
    }

    /// A tombstone the old owners confirmed must reach the new owners while
    /// it is young; once older than the grace period, an absence covers it,
    /// but an older copy never does.
    #[test]
    fn a_confirmed_delete_must_reach_the_new_owners() {
        let live = digest(1_000, VersionKind::Live, 1);
        let deleted = digest(2_000, VersionKind::Tombstone, 0);
        let held = vec![
            (NodeId(1), deleted),
            (NodeId(2), deleted),
            (NodeId(3), live),
            (NodeId(4), live),
        ];
        assert_eq!(
            check_key(&owners(), &held, 5_000, GRACE),
            KeyCheck::Behind(vec![NodeId(4), NodeId(5)])
        );
        assert_eq!(
            check_key(&owners(), &held, 200_000, GRACE),
            KeyCheck::Behind(vec![NodeId(4)])
        );
    }

    /// A key the old owners never held is the new owners' own.
    #[test]
    fn a_key_only_the_new_owners_hold_is_covered() {
        let held = vec![(NodeId(4), digest(1_000, VersionKind::Live, 1))];
        assert_eq!(check_key(&owners(), &held, 5_000, GRACE), KeyCheck::Covered);
    }
}
