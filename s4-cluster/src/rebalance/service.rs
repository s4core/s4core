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

//! The move of this node's keys to the new owners of a grown pool.
//!
//! Runs on every node of the old generation once the pool is moving. Each
//! work item ([`WorkItem`]) is walked a page at a time; every key is settled
//! across both generations as anti-entropy settles a key
//! (`Reconciler::settle_counting`) — newer wins, an older version never
//! overwrites a newer one — with this node and the key's new owners required
//! to answer. The other old owners are asked too and give their versions
//! when they answer; one that is down holds up nothing, and the newest
//! version it may hold reaches the new owners when its own move runs.
//!
//! A key of an erasure-coded object is moved as its namespace entry
//! (`RestoreNamespaceEntry`): its shards stay on their set.
//!
//! The progress of every item is stored after every page
//! ([`super::progress`]), so a restart of either side, or of the whole pool,
//! continues where it stopped. The move keeps to its rates
//! ([`RateLimiter`]), gives way to the clients' traffic ([`give_way`]),
//! sends nothing to a receiver short of space (the item waits, its reason
//! named) and stops at a pause until it is resumed; a pause is stored and
//! outlives a restart.
//!
//! Moving a range moves its arc from `Pending` to `Streaming` in the shared
//! [`GenerationRanges`]. A range this node finished is done here; comparing
//! both generations (`Verified`) and the cut-over are the verifier's
//! ([`super::verify`]).

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use s4_core::storage::VersionKind;
use s4_core::types::SYSTEM_BUCKET;
use s4_core::{KeyVersion, StorageEngine};
use tokio::sync::watch;
use tokio::time::Instant;
use tracing::{debug, error, info, warn};

use super::config::RebalanceConfig;
use super::families::SYSTEM_RECORD_PREFIXES;
use super::limiter::{give_way, ForegroundLoad, RateLimiter};
use super::multipart::{MultipartTransfer, UploadHandedOver};
use super::progress::{ItemProgress, MoveControl, Phase, ProgressStore, WorkItem};
use super::space::check_receiver;
use crate::gossip::ClusterMembershipQuery;
use crate::placement::{GenerationRanges, RangeState};
use crate::pool_generation::{GenerationAnnouncement, PoolGenerationRecord};
use crate::repair::reconcile::{KeyRef, KeyStateSource, Settled};
use crate::repair::{CopyMeter, LocalReplica, Reconciler};
use crate::rpc::{proto, NodeClient};
use crate::{ClusterError, GossipHandle, NodeId, PlacementRouter};

/// Everything the move of one node needs.
pub struct RebalanceParts {
    /// The pool's generations; the move runs only while `moving`.
    pub record: PoolGenerationRecord,
    /// This node.
    pub local: NodeId,
    /// The topology epoch of replica calls.
    pub topology_epoch: u64,
    /// Placement of both generations.
    pub router: Arc<PlacementRouter>,
    /// The arcs of the union ring, shared with the router.
    pub ranges: Arc<GenerationRanges>,
    /// This node's storage: its digests and its records.
    pub storage: Arc<dyn StorageEngine>,
    /// Where the versions of a key are read: the quorum read coordinator.
    pub source: Arc<dyn KeyStateSource>,
    /// This node's replica, written without the network.
    pub local_replica: LocalReplica,
    /// This node's open uploads, when it takes multipart writes.
    pub multipart: Option<MultipartTransfer>,
    /// Calls to the other nodes.
    pub rpc_client: Arc<NodeClient>,
    /// Membership, protocols and the receivers' capacity.
    pub gossip: Arc<GossipHandle>,
    /// Where the progress is kept.
    pub store: Arc<dyn ProgressStore>,
    /// How busy the clients keep this node.
    pub load: Arc<dyn ForegroundLoad>,
    /// Rates and limits.
    pub config: RebalanceConfig,
    /// Tombstones older than this are not carried to a replica without the key.
    pub gc_grace: Duration,
}

/// What became of one key.
enum KeyMoved {
    /// A replica received a version: `bytes` of copies.
    Moved { bytes: u64 },
    /// Every replica already held its newest version.
    InStep,
    /// Not this move's: not placed by the moving ring, or not held here.
    Skipped,
    /// A receiver is short of space: the item waits.
    NoRoom(String),
    /// Not settled now; tried again after the pass.
    Failed { reason: &'static str, why: String },
}

/// How a walk over an item ended.
enum ItemEnd {
    Done,
    Later(String),
    Stopped,
}

/// The state of the move on this node, as the admin API and the metrics read
/// it.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RebalanceStatus {
    /// Epoch of the generation the keys move from.
    pub from_epoch: u64,
    /// Epoch of the generation the keys move to.
    pub to_epoch: u64,
    /// Why the move is paused, while it is.
    pub paused: Option<String>,
    /// Arcs of the union ring.
    pub ranges_total: usize,
    /// Arcs this node handed over in full.
    pub ranges_done: usize,
    /// Every item: the arcs, the records of `__system__`, the uploads.
    pub items_total: usize,
    /// Items handed over in full.
    pub items_done: usize,
    /// Keys read.
    pub keys_scanned: u64,
    /// Keys some replica received a version of.
    pub keys_moved: u64,
    /// Bytes of copies delivered.
    pub bytes_moved: u64,
    /// Items waiting, and why: a receiver short of space, keys to retry.
    pub waiting: BTreeMap<String, String>,
    /// Items among `waiting` that wait for a receiver short of space.
    pub waiting_for_room: usize,
    /// Keys that could not be settled, by reason.
    pub errors: BTreeMap<String, u64>,
}

struct Inner {
    record: PoolGenerationRecord,
    local: NodeId,
    topology_epoch: u64,
    router: Arc<PlacementRouter>,
    ranges: Arc<GenerationRanges>,
    storage: Arc<dyn StorageEngine>,
    source: Arc<dyn KeyStateSource>,
    local_replica: LocalReplica,
    multipart: Option<MultipartTransfer>,
    rpc_client: Arc<NodeClient>,
    gossip: Arc<GossipHandle>,
    store: Arc<dyn ProgressStore>,
    load: Arc<dyn ForegroundLoad>,
    config: RebalanceConfig,
    gc_grace: Duration,
    keys: RateLimiter,
    bytes: RateLimiter,
    progress: Mutex<BTreeMap<WorkItem, ItemProgress>>,
    waiting: Mutex<BTreeMap<WorkItem, String>>,
    no_room: Mutex<std::collections::BTreeSet<WorkItem>>,
    errors: Mutex<BTreeMap<&'static str, u64>>,
    paused: watch::Sender<Option<String>>,
    /// Since when every member announced the move, as this node saw it.
    all_began_at: Mutex<Option<Instant>>,
}

/// The move of this node's keys; [`Self::handle`] pauses, resumes and
/// reports it.
pub struct RebalanceService {
    inner: Arc<Inner>,
}

/// Pauses, resumes and reports the move.
#[derive(Clone)]
pub struct RebalanceHandle {
    inner: Arc<Inner>,
}

impl RebalanceService {
    /// The move described by `parts`.
    ///
    /// # Errors
    ///
    /// A pool that is not moving, or invalid limits.
    pub fn new(parts: RebalanceParts) -> Result<Self, ClusterError> {
        parts.record.validate()?;
        if !parts.record.moving || parts.record.proposed.is_none() {
            return Err(ClusterError::Config(
                "the move of a pool's keys runs only while the pool is moving".into(),
            ));
        }
        parts.config.validate()?;
        let (paused, _) = watch::channel(None);
        Ok(Self {
            inner: Arc::new(Inner {
                keys: RateLimiter::per_second(u64::from(parts.config.max_keys_per_sec)),
                bytes: RateLimiter::per_second(parts.config.max_bytes_per_sec),
                record: parts.record,
                local: parts.local,
                topology_epoch: parts.topology_epoch,
                router: parts.router,
                ranges: parts.ranges,
                storage: parts.storage,
                source: parts.source,
                local_replica: parts.local_replica,
                multipart: parts.multipart,
                rpc_client: parts.rpc_client,
                gossip: parts.gossip,
                store: parts.store,
                load: parts.load,
                config: parts.config,
                gc_grace: parts.gc_grace,
                progress: Mutex::new(BTreeMap::new()),
                waiting: Mutex::new(BTreeMap::new()),
                no_room: Mutex::new(std::collections::BTreeSet::new()),
                errors: Mutex::new(BTreeMap::new()),
                paused,
                all_began_at: Mutex::new(None),
            }),
        })
    }

    /// Pauses, resumes and reports this move.
    pub fn handle(&self) -> RebalanceHandle {
        RebalanceHandle {
            inner: self.inner.clone(),
        }
    }

    /// Loads the stored progress and pause; the arcs this node began move to
    /// `Streaming`.
    ///
    /// # Errors
    ///
    /// The progress could not be read.
    pub async fn restore(&self) -> Result<(), ClusterError> {
        self.inner.restore().await
    }

    /// Moves every item, `concurrent_ranges` at a time, until each is done
    /// or `shutdown` is signalled.
    pub async fn run(self, shutdown: watch::Receiver<bool>) {
        let inner = self.inner;
        if let Err(e) = inner.restore().await {
            error!(error = %e, "the move of the pool's keys could not read its progress; it does not run");
            return;
        }
        if !inner.record.current.nodes.contains(&inner.local) {
            info!("this node joins the grown pool: it holds no keys of the old generation to hand over");
            return;
        }
        let items = inner.items();
        let pending: VecDeque<(WorkItem, Instant)> = {
            let progress = inner.progress.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            items
                .iter()
                .filter(|item| !progress.get(item).is_some_and(ItemProgress::is_done))
                .map(|item| (*item, Instant::now()))
                .collect()
        };
        info!(
            from_epoch = inner.record.current.epoch,
            to_epoch = inner.to_epoch(),
            items = items.len(),
            remaining = pending.len(),
            "moving this node's keys to the grown pool's new owners"
        );
        let queue = Arc::new(Mutex::new(pending));
        let mut workers = tokio::task::JoinSet::new();
        for _ in 0..inner.config.concurrent_ranges {
            workers.spawn(worker(inner.clone(), queue.clone(), shutdown.clone()));
        }
        while workers.join_next().await.is_some() {}
        let left = queue.lock().unwrap_or_else(std::sync::PoisonError::into_inner).len();
        if left == 0 && !*shutdown.borrow() {
            let status = RebalanceHandle {
                inner: inner.clone(),
            }
            .status();
            info!(
                keys_moved = status.keys_moved,
                bytes_moved = status.bytes_moved,
                "this node handed every range it holds to the new owners; the ranges wait for verification and cut-over"
            );
        }
    }
}

/// Takes items from `queue` until it is empty or `shutdown` is signalled.
async fn worker(
    inner: Arc<Inner>,
    queue: Arc<Mutex<VecDeque<(WorkItem, Instant)>>>,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        if *shutdown.borrow() || !inner.wait_unpaused(&mut shutdown).await {
            return;
        }
        let next = queue.lock().unwrap_or_else(std::sync::PoisonError::into_inner).pop_front();
        let Some((item, ready_at)) = next else {
            return;
        };
        if ready_at > Instant::now() {
            tokio::select! {
                _ = tokio::time::sleep_until(ready_at) => {}
                _ = shutdown.changed() => {}
            }
            if *shutdown.borrow() {
                return;
            }
        }
        match inner.walk(item, &shutdown).await {
            ItemEnd::Done => inner.finished(item),
            ItemEnd::Later(why) => {
                debug!(item = %item.record_name(inner.to_epoch()), %why, "an item waits for its next walk");
                queue
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push_back((item, Instant::now() + inner.config.retry_after));
            }
            ItemEnd::Stopped => {
                queue
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push_front((item, Instant::now()));
            }
        }
    }
}

impl Inner {
    fn to_epoch(&self) -> u64 {
        self.record.proposed.as_ref().map_or(0, |next| next.epoch)
    }

    /// Why a write placed by the earlier generation may still come, if it
    /// may: a member has not announced the move, or did so less than
    /// `settle` ago. Such a write reaches the old owners only. The arcs
    /// catch it, as the verification compares their digests before they
    /// cut over; the records of `__system__` without digests and the open
    /// uploads are handed over once, so they are handed over only after.
    fn writes_of_the_earlier_generation(&self) -> Option<String> {
        let expected = GenerationAnnouncement::of(&self.record, false);
        let next = self.record.proposed.as_ref()?;
        let lagging = next.nodes.iter().copied().find(|node| {
            *node != self.local
                && !self.gossip.pool_generation(node).is_some_and(|announced| {
                    announced.moving
                        && announced.current == expected.current
                        && announced.proposed == expected.proposed
                })
        });
        let mut since = self.all_began_at.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(node) = lagging {
            *since = None;
            return Some(format!(
                "waiting for node {node} to begin the move: a record without digests it writes \
                 meanwhile reaches the old owners only"
            ));
        }
        let began = *since.get_or_insert_with(Instant::now);
        (began.elapsed() < self.config.settle).then(|| {
            "every member began the move; waiting for writes placed by the earlier generation \
             to end before records without digests are handed over"
                .to_string()
        })
    }

    /// Every item of the move: the arcs in ring order, then the records of
    /// `__system__`, then the uploads.
    fn items(&self) -> Vec<WorkItem> {
        let mut items: Vec<WorkItem> =
            self.ranges.snapshot().iter().map(WorkItem::of_range).collect();
        items.push(WorkItem::SystemRecords);
        if self.multipart.is_some() {
            items.push(WorkItem::MultipartUploads);
        }
        items
    }

    async fn restore(&self) -> Result<(), ClusterError> {
        let to = self.to_epoch();
        let prefix = format!("{to}/");
        let mut stored = BTreeMap::new();
        let mut paused = None;
        for (name, record) in self.store.load().await? {
            let Some(rest) = name.strip_prefix(&prefix) else {
                continue;
            };
            if rest == "control" {
                let control = MoveControl::decode(&record)?;
                if control.pool_id == self.record.pool_id
                    && control.from_epoch == self.record.current.epoch
                {
                    paused = control.paused;
                }
                continue;
            }
            stored.insert(name, record);
        }
        let mut progress = BTreeMap::new();
        for item in self.items() {
            // The other records of the move (the ranges cut over, the moment
            // the pool settled) are not an item's.
            if let Some(found) = stored.remove(&item.record_name(to)) {
                let found = ItemProgress::decode(&found)?;
                if let WorkItem::Range {
                    upper_inclusive, ..
                } = item
                {
                    if found.passes > 0 {
                        let _ = self.ranges.transition(upper_inclusive, RangeState::Streaming);
                    }
                }
                progress.insert(item, found);
            }
        }
        *self.progress.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = progress;
        if paused.is_some() {
            info!(reason = ?paused, "the move of the pool's keys stays paused, as it was stopped");
        }
        self.paused.send_replace(paused);
        Ok(())
    }

    /// Waits while the move is paused; `false` once `shutdown` is signalled.
    async fn wait_unpaused(&self, shutdown: &mut watch::Receiver<bool>) -> bool {
        let mut paused = self.paused.subscribe();
        loop {
            if paused.borrow_and_update().is_none() {
                return true;
            }
            tokio::select! {
                _ = paused.changed() => {}
                _ = shutdown.changed() => if *shutdown.borrow() { return false; },
            }
        }
    }

    fn is_paused(&self) -> bool {
        self.paused.borrow().is_some()
    }

    fn progress_of(&self, item: WorkItem) -> ItemProgress {
        self.progress
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&item)
            .cloned()
            .unwrap_or_default()
    }

    async fn save(&self, item: WorkItem, progress: &ItemProgress) -> Result<(), ClusterError> {
        self.store.save(&item.record_name(self.to_epoch()), progress.encode()?).await?;
        self.progress
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(item, progress.clone());
        Ok(())
    }

    fn count_error(&self, reason: &'static str, count: u64) {
        if count > 0 {
            crate::cluster_metrics::record_rebalance_errors(reason, count);
            *self
                .errors
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .entry(reason)
                .or_default() += count;
        }
    }

    /// Records that `item` waits for a receiver short of space.
    fn set_no_room(&self, item: WorkItem, why: String) {
        self.set_waiting(item, Some(why));
        self.no_room
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(item);
    }

    fn set_waiting(&self, item: WorkItem, why: Option<String>) {
        self.no_room
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&item);
        let mut waiting = self.waiting.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        match why {
            Some(why) => waiting.insert(item, why),
            None => waiting.remove(&item),
        };
    }

    fn finished(&self, item: WorkItem) {
        self.set_waiting(item, None);
        if let WorkItem::Range { .. } = item {
            let done = self
                .progress
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .iter()
                .filter(|(item, progress)| {
                    matches!(item, WorkItem::Range { .. }) && progress.is_done()
                })
                .count();
            if done % 10 == 0 {
                info!(ranges_done = done, "ranges handed over to the new owners");
            }
        }
    }

    /// Walks `item` until it is done, has to wait, or the move stops.
    async fn walk(&self, item: WorkItem, shutdown: &watch::Receiver<bool>) -> ItemEnd {
        if let WorkItem::Range {
            upper_inclusive, ..
        } = item
        {
            let _ = self.ranges.transition(upper_inclusive, RangeState::Streaming);
        }
        if matches!(item, WorkItem::SystemRecords | WorkItem::MultipartUploads) {
            if let Some(why) = self.writes_of_the_earlier_generation() {
                self.set_waiting(item, Some(why.clone()));
                return ItemEnd::Later(why);
            }
        }
        // A new walk waits for nothing yet: what it meets says why it does.
        self.set_waiting(item, None);
        let meter = Arc::new(CopyMeter::default());
        let reconciler = Reconciler::new(
            self.source.clone(),
            self.rpc_client.clone(),
            self.gossip.clone(),
            self.local,
            self.topology_epoch,
        )
        .with_local_replica(self.local_replica.clone())
        .with_copy_meter(meter.clone());
        let mut progress = self.progress_of(item);
        if progress.passes == 0 {
            progress.passes = 1;
        }
        let stopped = || *shutdown.borrow() || self.is_paused();
        loop {
            if stopped() {
                return self.stop(item, &progress).await;
            }
            match progress.phase.clone() {
                Phase::Done => {
                    if let Err(e) = self.save(item, &progress).await {
                        return ItemEnd::Later(e.to_string());
                    }
                    return ItemEnd::Done;
                }
                Phase::Scan { segment, after } => {
                    let page = match self.page(item, segment, after.as_deref()).await {
                        Ok(page) => page,
                        Err(e) => return ItemEnd::Later(e.to_string()),
                    };
                    let Some(page) = page else {
                        progress.end_scan();
                        if let Err(e) = self.save(item, &progress).await {
                            return ItemEnd::Later(e.to_string());
                        }
                        if let Some(why) = walked_again(&progress) {
                            return ItemEnd::Later(why);
                        }
                        continue;
                    };
                    let full = page.len() == self.config.page_size;
                    for (key, kind) in &page {
                        if stopped() {
                            return self.stop(item, &progress).await;
                        }
                        match self.move_key(item, &reconciler, &meter, key, *kind).await {
                            KeyMoved::NoRoom(why) => {
                                self.set_no_room(item, why.clone());
                                let _ = self.save(item, &progress).await;
                                return ItemEnd::Later(why);
                            }
                            outcome => self.account(&mut progress, key, outcome),
                        }
                        progress.phase = Phase::Scan {
                            segment,
                            after: Some(key.clone()),
                        };
                    }
                    if !full {
                        progress.phase = Phase::Scan {
                            segment: segment + 1,
                            after: None,
                        };
                        if !self.has_segment(item, segment + 1) {
                            progress.end_scan();
                        }
                    }
                    if let Err(e) = self.save(item, &progress).await {
                        return ItemEnd::Later(e.to_string());
                    }
                    if let Some(why) = walked_again(&progress) {
                        return ItemEnd::Later(why);
                    }
                }
                Phase::Retry => {
                    let mut still = Vec::new();
                    let keys = progress.retry.clone();
                    for (index, key) in keys.iter().enumerate() {
                        if stopped() {
                            still.extend(keys[index..].iter().cloned());
                            break;
                        }
                        let kind = self.kind_of(item, key).await;
                        match self.move_key(item, &reconciler, &meter, key, kind).await {
                            KeyMoved::NoRoom(why) => {
                                self.set_no_room(item, why);
                                still.extend(keys[index..].iter().cloned());
                                break;
                            }
                            KeyMoved::Failed { reason, why } => {
                                self.count_error(reason, 1);
                                progress.last_error = Some(why);
                                still.push(key.clone());
                            }
                            outcome => {
                                let mut scratch = ItemProgress::default();
                                self.account(&mut scratch, key, outcome);
                                progress.moved += scratch.moved;
                                progress.bytes += scratch.bytes;
                            }
                        }
                    }
                    progress.end_retry(still);
                    if let Err(e) = self.save(item, &progress).await {
                        return ItemEnd::Later(e.to_string());
                    }
                    if !progress.is_done() {
                        let why = progress
                            .last_error
                            .clone()
                            .unwrap_or_else(|| "keys wait to be settled again".into());
                        self.set_waiting(
                            item,
                            Some(format!("{} key(s) to retry: {why}", progress.retry.len())),
                        );
                        return ItemEnd::Later(why);
                    }
                }
            }
        }
    }

    async fn stop(&self, item: WorkItem, progress: &ItemProgress) -> ItemEnd {
        if let Err(e) = self.save(item, progress).await {
            warn!(error = %e, "the progress of a stopped item could not be stored; it repeats its last page");
        }
        ItemEnd::Stopped
    }

    fn account(&self, progress: &mut ItemProgress, key: &str, outcome: KeyMoved) {
        progress.scanned += 1;
        match outcome {
            KeyMoved::Moved { bytes } => {
                crate::cluster_metrics::record_rebalance_key_moved(bytes);
                progress.moved += 1;
                progress.bytes += bytes;
            }
            KeyMoved::InStep | KeyMoved::Skipped | KeyMoved::NoRoom(_) => {}
            KeyMoved::Failed { reason, why } => {
                self.count_error(reason, 1);
                progress.failed(key, why);
            }
        }
    }

    fn has_segment(&self, item: WorkItem, segment: u32) -> bool {
        let count = match item {
            WorkItem::Range { .. } => item.segments().len(),
            WorkItem::SystemRecords => SYSTEM_RECORD_PREFIXES.len(),
            WorkItem::MultipartUploads => 1,
        };
        (segment as usize) < count
    }

    /// The next page of `item`'s keys in `segment` after `after`, with the
    /// kind of the version this node holds; `None` past the segments.
    async fn page(
        &self,
        item: WorkItem,
        segment: u32,
        after: Option<&str>,
    ) -> Result<Option<Vec<(String, VersionKind)>>, ClusterError> {
        let limit = self.config.page_size;
        let storage_error = |e: s4_core::StorageError| ClusterError::Storage(e.to_string());
        match item {
            WorkItem::Range { .. } => {
                let Some((from, to)) = item.segments().get(segment as usize).copied() else {
                    return Ok(None);
                };
                let page = self
                    .storage
                    .version_digests_in(from, to, after, limit)
                    .await
                    .map_err(storage_error)?;
                Ok(Some(
                    page.into_iter().map(|entry| (entry.key, entry.digest.kind)).collect(),
                ))
            }
            WorkItem::SystemRecords => {
                let Some(prefix) = SYSTEM_RECORD_PREFIXES.get(segment as usize) else {
                    return Ok(None);
                };
                let after = after
                    .and_then(|full| full.strip_prefix(&format!("{SYSTEM_BUCKET}/")))
                    .unwrap_or("");
                let page = self
                    .storage
                    .list_objects_after(SYSTEM_BUCKET, prefix, after, limit)
                    .await
                    .map_err(storage_error)?;
                Ok(Some(
                    page.into_iter()
                        .map(|(key, _)| (format!("{SYSTEM_BUCKET}/{key}"), VersionKind::Live))
                        .collect(),
                ))
            }
            WorkItem::MultipartUploads => {
                let Some(multipart) = &self.multipart else {
                    return Ok(None);
                };
                if segment > 0 {
                    return Ok(None);
                }
                let sessions = multipart.sessions_after(after, limit).await?;
                Ok(Some(
                    sessions
                        .into_iter()
                        .map(|session| (session.upload_id, VersionKind::Live))
                        .collect(),
                ))
            }
        }
    }

    /// The kind of version this node holds of a key tried again.
    async fn kind_of(&self, item: WorkItem, key: &str) -> VersionKind {
        if !matches!(item, WorkItem::Range { .. }) {
            return VersionKind::Live;
        }
        let Some((bucket, name)) = key.split_once('/') else {
            return VersionKind::Live;
        };
        match self.storage.key_version(bucket, name).await {
            Ok(KeyVersion::Live(record)) if record.is_erasure_coded() => VersionKind::ErasureCoded,
            Ok(KeyVersion::Tombstone(_)) => VersionKind::Tombstone,
            _ => VersionKind::Live,
        }
    }

    /// Brings the replicas of `key` of `item` to its newest version.
    async fn move_key(
        &self,
        item: WorkItem,
        reconciler: &Reconciler,
        meter: &CopyMeter,
        key: &str,
        kind: VersionKind,
    ) -> KeyMoved {
        if let WorkItem::MultipartUploads = item {
            return self.move_upload(key).await;
        }
        let Some((bucket, name)) = key.split_once('/') else {
            return KeyMoved::Skipped;
        };
        let decision = match self.router.route(bucket, name) {
            Ok(decision) => decision,
            Err(e) => {
                return KeyMoved::Failed {
                    reason: "placement",
                    why: e.to_string(),
                }
            }
        };
        let requirements = decision.write_rule.requirements();
        let [_, new] = requirements else {
            return KeyMoved::Skipped;
        };
        let new_owners = new.nodes.clone();
        if let Err(why) = self.room_on(&new_owners) {
            return KeyMoved::NoRoom(why);
        }
        give_way(
            self.load.as_ref(),
            self.config.yield_at_in_flight,
            Duration::from_secs(1),
        )
        .await;
        self.keys.spend(1).await;
        let mut required = new_owners.clone();
        if decision.replicas.contains(&self.local) && !required.contains(&self.local) {
            required.push(self.local);
        }
        let bytes_before = meter.bytes();
        let refusals_before = meter.checksum_refusals();
        let (settled, brought) = reconciler
            .settle_counting(KeyRef { bucket, key: name }, Some(&required), self.gc_grace)
            .await;
        let bytes = meter.bytes().saturating_sub(bytes_before);
        self.bytes.spend(bytes).await;
        self.count_error(
            "checksum",
            meter.checksum_refusals().saturating_sub(refusals_before),
        );
        match settled {
            Settled::InStep if brought > 0 || bytes > 0 => KeyMoved::Moved { bytes },
            Settled::InStep => KeyMoved::InStep,
            Settled::NotOurs if kind == VersionKind::ErasureCoded => {
                self.move_namespace_entry(bucket, name, &new_owners, decision.placement_generation)
                    .await
            }
            Settled::NotOurs => KeyMoved::Skipped,
            // The newest copy is one whose write may have failed: the new
            // owners still take the newest version the old owners confirmed.
            Settled::Ambiguous(why) => {
                match self.bring_confirmed(reconciler, bucket, name, &new_owners).await {
                    Ok(brought) => {
                        let bytes = meter.bytes().saturating_sub(bytes_before);
                        if brought > 0 || bytes > 0 {
                            KeyMoved::Moved { bytes }
                        } else {
                            KeyMoved::InStep
                        }
                    }
                    Err(more) => KeyMoved::Failed {
                        reason: "ambiguous",
                        why: format!("{why}; {more}"),
                    },
                }
            }
            Settled::Failed(why) => KeyMoved::Failed {
                reason: if why.contains(crate::replica::CHECKSUM_MISMATCH) {
                    "checksum"
                } else {
                    "unavailable"
                },
                why,
            },
        }
    }

    /// Brings the new owners of `bucket/key` to the newest version its old
    /// owners confirmed, whatever newer copy a failed write left.
    async fn bring_confirmed(
        &self,
        reconciler: &Reconciler,
        bucket: &str,
        key: &str,
        new_owners: &[NodeId],
    ) -> Result<usize, String> {
        let versions = self.source.key_versions(bucket, key).await.map_err(|e| e.to_string())?;
        let Some(newest) = crate::repair::reconcile::newest_confirmed(
            &versions.answers,
            &versions.confirming_replicas,
            versions.confirming,
        ) else {
            return Err("no version is held by as many old owners as a confirmed write".into());
        };
        reconciler
            .bring_confirmed(KeyRef { bucket, key }, &versions, newest, new_owners)
            .await
    }

    /// Whether every receiver among `nodes` has room now.
    fn room_on(&self, nodes: &[NodeId]) -> Result<(), String> {
        for node in nodes.iter().filter(|node| **node != self.local) {
            let capacity = self.gossip.node_capacity(*node).map(|report| report.capacity);
            check_receiver(*node, capacity.as_ref(), self.config.min_free_ratio)?;
        }
        Ok(())
    }

    /// Gives the new owners the namespace entry of an erasure-coded object
    /// this node holds; one that holds an entry already keeps it.
    async fn move_namespace_entry(
        &self,
        bucket: &str,
        key: &str,
        new_owners: &[NodeId],
        placement_generation: u64,
    ) -> KeyMoved {
        let record = match self.storage.key_version(bucket, key).await {
            Ok(KeyVersion::Live(record)) if record.is_erasure_coded() => record,
            Ok(_) => return KeyMoved::Skipped,
            Err(e) => {
                return KeyMoved::Failed {
                    reason: "storage",
                    why: e.to_string(),
                }
            }
        };
        let encoded = match bincode::serialize(&record) {
            Ok(encoded) => encoded,
            Err(e) => {
                return KeyMoved::Failed {
                    reason: "storage",
                    why: e.to_string(),
                }
            }
        };
        let mut restored = false;
        for node in new_owners.iter().filter(|node| **node != self.local) {
            let request = proto::RestoreNamespaceEntryRequest {
                bucket: bucket.to_string(),
                key: key.to_string(),
                record: encoded.clone(),
                coordinator_id: self.local.to_bytes().to_vec(),
                operation_id: uuid::Uuid::new_v4().as_u128().to_be_bytes().to_vec(),
                topology_epoch: self.topology_epoch,
                placement_generation,
            };
            match self.rpc_client.send_restore_namespace_entry(*node, request).await {
                Ok(response) if response.success => restored |= response.restored,
                Ok(response) => {
                    return KeyMoved::Failed {
                        reason: "unavailable",
                        why: format!("replica {node}: {}", response.error),
                    }
                }
                Err(e) => {
                    return KeyMoved::Failed {
                        reason: "unavailable",
                        why: format!("replica {node}: {e}"),
                    }
                }
            }
        }
        if restored {
            KeyMoved::Moved { bytes: 0 }
        } else {
            KeyMoved::InStep
        }
    }

    /// Hands the open upload `upload_id` to the replicas of its key.
    async fn move_upload(&self, upload_id: &str) -> KeyMoved {
        let Some(multipart) = &self.multipart else {
            return KeyMoved::Skipped;
        };
        let Some(session) = multipart.local_session(upload_id).await else {
            return KeyMoved::Skipped;
        };
        if let Ok(decision) = self.router.route(&session.bucket, &session.key) {
            if let [_, new] = decision.write_rule.requirements() {
                if let Err(why) = self.room_on(&new.nodes) {
                    return KeyMoved::NoRoom(why);
                }
            }
        }
        give_way(
            self.load.as_ref(),
            self.config.yield_at_in_flight,
            Duration::from_secs(1),
        )
        .await;
        self.keys.spend(1).await;
        match multipart.hand_over(&session, None).await {
            UploadHandedOver::InStep { parts: 0, .. } => KeyMoved::InStep,
            UploadHandedOver::InStep { bytes, .. } => {
                self.bytes.spend(bytes).await;
                KeyMoved::Moved { bytes }
            }
            UploadHandedOver::Gone => KeyMoved::Skipped,
            UploadHandedOver::Failed(why) => KeyMoved::Failed {
                reason: "unavailable",
                why,
            },
            UploadHandedOver::Damaged(why) => KeyMoved::Failed {
                reason: "checksum",
                why,
            },
        }
    }
}

/// Why an item waits before it is walked again from its start: more of its
/// keys failed than are kept to be retried. `None` while its pass goes on.
fn walked_again(progress: &ItemProgress) -> Option<String> {
    let restarted = progress.passes > 1
        && progress.phase
            == Phase::Scan {
                segment: 0,
                after: None,
            };
    restarted.then(|| {
        format!(
            "more keys failed than are kept to retry; the item is walked again: {}",
            progress.last_error.as_deref().unwrap_or("no reason recorded")
        )
    })
}

impl RebalanceHandle {
    /// Pauses the move; it stays paused across restarts until resumed.
    ///
    /// # Errors
    ///
    /// The pause could not be stored.
    pub async fn pause(&self, reason: &str) -> Result<(), ClusterError> {
        self.store_control(Some(reason.to_string())).await?;
        info!(%reason, "the move of the pool's keys is paused");
        self.inner.paused.send_replace(Some(reason.to_string()));
        Ok(())
    }

    /// Resumes a paused move.
    ///
    /// # Errors
    ///
    /// The resumption could not be stored.
    pub async fn resume(&self) -> Result<(), ClusterError> {
        self.store_control(None).await?;
        info!("the move of the pool's keys resumes");
        self.inner.paused.send_replace(None);
        Ok(())
    }

    async fn store_control(&self, paused: Option<String>) -> Result<(), ClusterError> {
        let control = MoveControl {
            format: 1,
            pool_id: self.inner.record.pool_id,
            from_epoch: self.inner.record.current.epoch,
            to_epoch: self.inner.to_epoch(),
            paused,
        };
        self.inner
            .store
            .save(
                &MoveControl::record_name(self.inner.to_epoch()),
                control.encode()?,
            )
            .await
    }

    /// The arcs this node handed over in full, by their upper endpoints.
    pub fn ranges_handed_over(&self) -> std::collections::HashSet<u64> {
        self.inner
            .progress
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .filter_map(|(item, progress)| match item {
                WorkItem::Range {
                    upper_inclusive, ..
                } if progress.is_done() => Some(*upper_inclusive),
                _ => None,
            })
            .collect()
    }

    /// The state of the move on this node now.
    pub fn status(&self) -> RebalanceStatus {
        let inner = &self.inner;
        let items = inner.items();
        let progress = inner.progress.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut status = RebalanceStatus {
            from_epoch: inner.record.current.epoch,
            to_epoch: inner.to_epoch(),
            paused: inner.paused.borrow().clone(),
            items_total: items.len(),
            ranges_total: items
                .iter()
                .filter(|item| matches!(item, WorkItem::Range { .. }))
                .count(),
            ..RebalanceStatus::default()
        };
        for item in &items {
            let Some(item_progress) = progress.get(item) else {
                continue;
            };
            status.keys_scanned += item_progress.scanned;
            status.keys_moved += item_progress.moved;
            status.bytes_moved += item_progress.bytes;
            if item_progress.is_done() {
                status.items_done += 1;
                if matches!(item, WorkItem::Range { .. }) {
                    status.ranges_done += 1;
                }
            }
        }
        status.waiting = inner
            .waiting
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .map(|(item, why)| (item.record_name(status.to_epoch), why.clone()))
            .collect();
        status.waiting_for_room =
            inner.no_room.lock().unwrap_or_else(std::sync::PoisonError::into_inner).len();
        status.errors = inner
            .errors
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .map(|(reason, count)| ((*reason).to_string(), *count))
            .collect();
        status
    }
}
