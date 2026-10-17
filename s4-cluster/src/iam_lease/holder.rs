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

//! The node a change of IAM came to, taking the lease for it.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use rand::Rng;
use s4_core::types::SYSTEM_BUCKET;
use tokio::sync::{Mutex, OwnedMutexGuard};
use tracing::{debug, warn};

use super::keeper::{ClassRecord, IamLeaseKeeper, Refusal};
use super::ledger::{Grant, LeaseFence};
use crate::coordinator::takes_class;
use crate::hlc::{HlcTimestamp, HybridClock};
use crate::identity::NodeId;
use crate::placement::{PlacementRouter, WriteRule};
use crate::rpc::{proto, NodeClient};

/// A key of the cluster's control state: where it is placed is where the
/// lease is asked for, by the same majority its writes take.
pub(crate) const LEASE_ROUTE_KEY: &str = "__s4_iam_v2_lease";

/// How long a node gives a peer to answer about the lease.
const ASK_TIMEOUT: Duration = Duration::from_secs(2);

/// Records one read under the lease asks a node for at a time.
const READ_PAGE: u32 = 500;

/// How the lease of IAM changes is held.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IamLeaseConfig {
    /// How long a node grants the lease for (`S4_IAM_LEASE_SECS`).
    pub duration: Duration,
    /// How long a change waits for a lease another node holds
    /// (`S4_IAM_LEASE_WAIT_SECS`) before it is refused.
    pub wait: Duration,
}

impl Default for IamLeaseConfig {
    fn default() -> Self {
        Self {
            duration: Duration::from_secs(5),
            wait: Duration::from_secs(10),
        }
    }
}

impl IamLeaseConfig {
    /// How long before its lease runs out by its own clock a holder stops
    /// writing. The grants started when the peers answered, after the holder
    /// asked, so they run out later; the margin covers clocks that run at
    /// slightly different rates.
    fn margin(&self) -> Duration {
        (self.duration / 10).max(Duration::from_millis(100)).min(self.duration / 2)
    }
}

/// Why a change of IAM got no lease.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IamLeaseError {
    /// Another node held the lease for the whole wait: retry after
    /// `retry_after`.
    Busy {
        /// How long the lease it saw is still held.
        retry_after: Duration,
    },
    /// Too few nodes of the pool answered, or the lease ran out.
    Unavailable(String),
}

impl std::fmt::Display for IamLeaseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Busy { retry_after } => write!(
                f,
                "another change of IAM is in progress; retry in {} s",
                retry_after.as_secs().max(1)
            ),
            Self::Unavailable(reason) => write!(f, "{reason}"),
        }
    }
}

/// What a holder holds between taking and releasing the lease.
#[derive(Debug, Clone, Copy)]
struct Held {
    fence: LeaseFence,
    valid_until: Instant,
}

/// The lease of IAM changes, held by this node for one change. Release it
/// with [`IamLeaseHolder::release`]; dropped, it runs out on the other nodes
/// by itself.
pub struct IamMutationLease {
    held: StdMutex<Held>,
    /// The nodes asked for it, which are told of its release.
    asked: Vec<NodeId>,
    /// Holds this node's other changes until this one is over.
    _turn: OwnedMutexGuard<()>,
}

impl std::fmt::Debug for IamMutationLease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IamMutationLease").field("fence", &self.fence()).finish()
    }
}

impl IamMutationLease {
    fn held(&self) -> Held {
        *self.held.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// The epoch and HLC window the change's writes are made under.
    pub fn fence(&self) -> LeaseFence {
        self.held().fence
    }

    /// How long this node may still write under it.
    pub fn remaining(&self) -> Duration {
        self.held().valid_until.saturating_duration_since(Instant::now())
    }
}

/// What a read under the lease found.
#[derive(Debug, Clone, Default)]
pub struct LeaseRead {
    /// The newest version of each key, tombstones included.
    pub records: Vec<ClassRecord>,
    /// The keys whose newest version fewer than a majority of the pool hold.
    pub unsettled: BTreeSet<String>,
}

/// One node's answer about the lease.
#[derive(Debug, Clone, Copy)]
enum Answer {
    Grant(Grant, u64),
    /// No answer, or one that granted nothing for a fault.
    Silent,
}

/// Takes the lease of IAM changes for the changes that come to this node.
pub struct IamLeaseHolder {
    local: NodeId,
    keeper: Arc<IamLeaseKeeper>,
    rpc: Arc<NodeClient>,
    router: Arc<PlacementRouter>,
    clock: Arc<HybridClock>,
    config: IamLeaseConfig,
    /// One change of this node at a time: the lease is the node's, not the
    /// request's.
    turn: Arc<Mutex<()>>,
    /// The last epoch this node asked for. A new change never asks for it
    /// again: a node that granted it and was not told of its release would
    /// take the request for a renewal of the change before.
    last_epoch: std::sync::atomic::AtomicU64,
}

impl IamLeaseHolder {
    /// The holder of node `local`, which grants through `keeper` itself and
    /// asks its peers through `rpc`.
    pub fn new(
        local: NodeId,
        keeper: Arc<IamLeaseKeeper>,
        rpc: Arc<NodeClient>,
        router: Arc<PlacementRouter>,
        clock: Arc<HybridClock>,
        config: IamLeaseConfig,
    ) -> Self {
        Self {
            local,
            keeper,
            rpc,
            router,
            clock,
            config,
            turn: Arc::new(Mutex::new(())),
            last_epoch: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// How the lease is held.
    pub fn config(&self) -> IamLeaseConfig {
        self.config
    }

    /// The nodes of the class and the majority of them a lease needs: the
    /// nodes its writes go to, and their write rule.
    fn nodes(&self) -> Result<(Vec<NodeId>, WriteRule), IamLeaseError> {
        let decision = self
            .router
            .route(SYSTEM_BUCKET, LEASE_ROUTE_KEY)
            .map_err(|e| IamLeaseError::Unavailable(e.to_string()))?;
        let nodes = decision
            .replicas
            .iter()
            .copied()
            .filter(|node| takes_class(&self.rpc, self.local, *node, decision.class))
            .collect();
        Ok((nodes, decision.write_rule))
    }

    /// A ceiling `duration` past this node's clock: a wall time, no node.
    fn ceiling(&self) -> HlcTimestamp {
        let duration_ms = u64::try_from(self.config.duration.as_millis()).unwrap_or(u64::MAX);
        HlcTimestamp::from_record(self.clock.now().wall_time.saturating_add(duration_ms), 0, 0)
    }

    /// Asks every node of `nodes` for `epoch`, at once.
    async fn ask(
        &self,
        nodes: &[NodeId],
        epoch: u64,
        ceiling: HlcTimestamp,
    ) -> Vec<(NodeId, Answer)> {
        let duration_ms = u64::try_from(self.config.duration.as_millis()).unwrap_or(u64::MAX);
        let asks = nodes.iter().map(|&node| async move {
            if node == self.local {
                let answer = match self
                    .keeper
                    .acquire(self.local, epoch, ceiling, self.config.duration)
                    .await
                {
                    Ok((grant, max_epoch)) => Answer::Grant(grant, max_epoch),
                    Err(error) => {
                        warn!(%error, "this node could not keep its grant of the IAM lease");
                        Answer::Silent
                    }
                };
                return (node, answer);
            }
            let request = proto::IamLeaseAcquireRequest {
                holder: self.local.to_bytes().to_vec(),
                epoch,
                ceiling: Some(ceiling.to_proto()),
                duration_ms,
            };
            let answer = match tokio::time::timeout(
                ASK_TIMEOUT,
                self.rpc.send_iam_lease_acquire(node, request),
            )
            .await
            {
                Ok(Ok(response)) if response.error.is_empty() => {
                    let grant = if response.granted {
                        Grant::Granted {
                            floor: super::fence_from_proto_hlc(response.floor.as_ref()),
                        }
                    } else if response.busy {
                        Grant::Busy {
                            holder: NodeId(0),
                            remaining_ms: response.busy_remaining_ms,
                        }
                    } else {
                        Grant::Stale
                    };
                    Answer::Grant(grant, response.max_epoch)
                }
                Ok(Ok(response)) => {
                    debug!(%node, error = %response.error, "peer kept no grant of the IAM lease");
                    Answer::Silent
                }
                Ok(Err(error)) => {
                    debug!(%node, %error, "peer did not answer about the IAM lease");
                    Answer::Silent
                }
                Err(_) => Answer::Silent,
            };
            (node, answer)
        });
        futures::future::join_all(asks).await
    }

    /// Tells `nodes` that `epoch` is released, its writes at or below
    /// `closing`. A node not told lets the grant run out.
    async fn release_epoch(&self, nodes: &[NodeId], epoch: u64, closing: HlcTimestamp) {
        let releases = nodes.iter().map(|&node| async move {
            if node == self.local {
                if let Err(error) = self.keeper.release(self.local, epoch, closing).await {
                    warn!(%error, "this node could not keep the release of the IAM lease");
                }
                return;
            }
            let request = proto::IamLeaseReleaseRequest {
                holder: self.local.to_bytes().to_vec(),
                epoch,
                closing: Some(closing.to_proto()),
            };
            if let Ok(Err(error)) =
                tokio::time::timeout(ASK_TIMEOUT, self.rpc.send_iam_lease_release(node, request))
                    .await
            {
                debug!(%node, %error, "peer not told of the release of the IAM lease");
            }
        });
        futures::future::join_all(releases).await;
    }

    /// Takes the lease for one change: the next epoch, granted by a majority
    /// of the pool. Waits while another node holds it, up to the configured
    /// wait.
    ///
    /// # Errors
    ///
    /// `Busy` when another node held it for the whole wait; `Unavailable`
    /// when too few nodes of the pool answer.
    pub async fn acquire(&self) -> Result<IamMutationLease, IamLeaseError> {
        let deadline = Instant::now() + self.config.wait;
        let turn = tokio::time::timeout(self.config.wait, self.turn.clone().lock_owned())
            .await
            .map_err(|_| IamLeaseError::Busy {
                retry_after: self.config.duration,
            })?;
        let mut epoch = self
            .keeper
            .max_epoch()
            .await
            .max(self.last_epoch.load(std::sync::atomic::Ordering::SeqCst))
            + 1;
        loop {
            self.last_epoch.fetch_max(epoch, std::sync::atomic::Ordering::SeqCst);
            let (nodes, rule) = self.nodes()?;
            let asked_at = Instant::now();
            let ceiling = self.ceiling();
            let answers = self.ask(&nodes, epoch, ceiling).await;

            let mut granted = Vec::new();
            let mut answered = Vec::new();
            let mut floor = HlcTimestamp::zero();
            let mut busy_for: Option<Duration> = None;
            let mut max_seen = epoch;
            for (node, answer) in &answers {
                let Answer::Grant(grant, max_epoch) = answer else {
                    continue;
                };
                answered.push(*node);
                max_seen = max_seen.max(*max_epoch);
                match grant {
                    Grant::Granted { floor: theirs } => {
                        granted.push(*node);
                        floor = floor.max(*theirs);
                    }
                    Grant::Busy { remaining_ms, .. } => {
                        let remaining = Duration::from_millis(*remaining_ms);
                        busy_for = Some(busy_for.map_or(remaining, |held| held.max(remaining)));
                    }
                    Grant::Stale => {}
                }
            }

            if rule.met(&granted) {
                // Every write of this epoch goes above every write an earlier
                // one may have made.
                self.clock.observe(&floor);
                if self.clock.now() < ceiling {
                    let valid_until = asked_at + self.config.duration - self.config.margin();
                    debug!(epoch, %floor, %ceiling, "IAM lease taken");
                    return Ok(IamMutationLease {
                        held: StdMutex::new(Held {
                            fence: LeaseFence {
                                epoch,
                                holder: self.local,
                                floor,
                                ceiling,
                            },
                            valid_until,
                        }),
                        asked: nodes,
                        _turn: turn,
                    });
                }
                // An earlier holder's clock ran far ahead of this one: the
                // floor is above this epoch's ceiling. The next epoch's is
                // set past it.
                debug!(epoch, %floor, %ceiling, "IAM lease floor above its ceiling, asking again");
            }
            if !granted.is_empty() {
                self.release_epoch(&granted, epoch, HlcTimestamp::zero()).await;
            }
            if !rule.met(&answered) {
                return Err(IamLeaseError::Unavailable(format!(
                    "IAM changes need a majority of the pool: {} of {} nodes answered",
                    answered.len(),
                    nodes.len()
                )));
            }
            epoch = max_seen + 1;
            let now = Instant::now();
            if now >= deadline {
                return Err(IamLeaseError::Busy {
                    retry_after: busy_for.unwrap_or(self.config.duration),
                });
            }
            // Another holder: wait for its grant to run out or be released;
            // a race for the same epoch: step aside for a moment.
            let jitter = Duration::from_millis(rand::rng().random_range(5..50));
            let pause = busy_for
                .map_or(jitter, |held| held.min(Duration::from_millis(200)) + jitter)
                .min(deadline - now);
            tokio::time::sleep(pause).await;
        }
    }

    /// Every record of `node` under `prefix`, read under `fence`; `None` when
    /// the node did not answer every page or refused the epoch.
    async fn read_node(
        &self,
        node: NodeId,
        fence: LeaseFence,
        prefix: &str,
    ) -> Option<Vec<ClassRecord>> {
        let mut records = Vec::new();
        let mut start_after = String::new();
        loop {
            let (page, truncated) = if node == self.local {
                let after = Some(start_after.as_str()).filter(|key| !key.is_empty());
                match self.keeper.read(Some(&fence), prefix, after, READ_PAGE as usize).await {
                    Ok(page) => page,
                    Err(Refusal::Fenced(reason)) => {
                        warn!(%reason, "this node refused its own read under the IAM lease");
                        return None;
                    }
                    Err(refusal) => {
                        warn!(%refusal, "this node could not read IAM under the lease");
                        return None;
                    }
                }
            } else {
                let request = proto::IamLeaseReadRequest {
                    fence: Some(fence.to_proto()),
                    prefix: prefix.to_string(),
                    start_after: start_after.clone(),
                    max_keys: READ_PAGE,
                };
                let response = match tokio::time::timeout(
                    ASK_TIMEOUT,
                    self.rpc.send_iam_lease_read(node, request),
                )
                .await
                {
                    Ok(Ok(response)) if response.error.is_empty() => response,
                    Ok(Ok(response)) => {
                        debug!(%node, error = %response.error, "peer refused a read under the IAM lease");
                        return None;
                    }
                    Ok(Err(error)) => {
                        debug!(%node, %error, "peer did not answer a read under the IAM lease");
                        return None;
                    }
                    Err(_) => return None,
                };
                let page = response
                    .records
                    .into_iter()
                    .map(|record| ClassRecord {
                        data: (!record.tombstone).then_some(record.data),
                        hlc: super::fence_from_proto_hlc(record.hlc.as_ref()),
                        key: record.key,
                        etag: record.etag,
                    })
                    .collect();
                (page, response.is_truncated)
            };
            let page: Vec<ClassRecord> = page;
            if let Some(last) = page.last() {
                start_after = last.key.clone();
            }
            let empty = page.is_empty();
            records.extend(page);
            if !truncated || empty {
                return Some(records);
            }
        }
    }

    /// The records of the cluster's control state under `prefix` as a
    /// majority of the pool holds them, read under `lease`: the newest
    /// version of each key any of those nodes holds, tombstones included,
    /// and which of them are held by fewer than a majority of the pool.
    ///
    /// Every node that answers takes the lease's epoch first and refuses the
    /// writes of earlier ones from then on, so the answer holds every change
    /// confirmed before it, and no change of an earlier lease can be
    /// confirmed after it. A version on fewer than a majority is one a change
    /// never confirmed; another read may not see it, so the caller writes it
    /// again under its own lease before it acts on it.
    ///
    /// # Errors
    ///
    /// `Unavailable` when fewer than a majority answered.
    pub async fn read(
        &self,
        lease: &IamMutationLease,
        prefix: &str,
    ) -> Result<LeaseRead, IamLeaseError> {
        let fence = lease.fence();
        let (_, rule) = self.nodes()?;
        let answers = futures::future::join_all(
            lease
                .asked
                .iter()
                .map(|&node| async move { (node, self.read_node(node, fence, prefix).await) }),
        )
        .await;
        let mut answered = Vec::new();
        let mut newest: BTreeMap<String, ClassRecord> = BTreeMap::new();
        let mut held_by: BTreeMap<String, Vec<(NodeId, ClassRecord)>> = BTreeMap::new();
        for (node, records) in answers {
            let Some(records) = records else {
                continue;
            };
            answered.push(node);
            for record in records {
                match newest.get(&record.key) {
                    Some(held) if held.rank() >= record.rank() => {}
                    _ => {
                        newest.insert(record.key.clone(), record.clone());
                    }
                }
                held_by.entry(record.key.clone()).or_default().push((node, record));
            }
        }
        if !rule.met(&answered) {
            return Err(IamLeaseError::Unavailable(format!(
                "IAM changes need a majority of the pool: {} of {} nodes answered the read",
                answered.len(),
                lease.asked.len()
            )));
        }
        let unsettled = newest
            .iter()
            .filter(|(key, record)| {
                let holders: Vec<NodeId> = held_by
                    .get(*key)
                    .into_iter()
                    .flatten()
                    .filter(|(_, held)| held.rank() == record.rank())
                    .map(|(node, _)| *node)
                    .collect();
                !rule.met(&holders)
            })
            .map(|(key, _)| key.clone())
            .collect();
        Ok(LeaseRead {
            records: newest.into_values().collect(),
            unsettled,
        })
    }

    /// Renews `lease` once less than half its duration is left, so that a
    /// change that takes long still writes inside it.
    ///
    /// # Errors
    ///
    /// `Unavailable` when a majority did not renew it: the change must stop.
    pub async fn ensure_time(&self, lease: &IamMutationLease) -> Result<(), IamLeaseError> {
        if lease.remaining() >= self.config.duration / 2 {
            return Ok(());
        }
        let fence = lease.fence();
        if lease.remaining().is_zero() {
            return Err(IamLeaseError::Unavailable(format!(
                "IAM lease epoch {} ran out before the change was written",
                fence.epoch
            )));
        }
        let (_, rule) = self.nodes()?;
        let asked_at = Instant::now();
        let ceiling = self.ceiling().max(fence.ceiling);
        let renewed: Vec<NodeId> = self
            .ask(&lease.asked, fence.epoch, ceiling)
            .await
            .into_iter()
            .filter_map(|(node, answer)| {
                matches!(answer, Answer::Grant(Grant::Granted { .. }, _)).then_some(node)
            })
            .collect();
        if !rule.met(&renewed) || lease.remaining().is_zero() {
            return Err(IamLeaseError::Unavailable(format!(
                "IAM lease epoch {} not renewed by a majority of the pool",
                fence.epoch
            )));
        }
        let mut held = lease.held.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        held.fence.ceiling = ceiling;
        held.valid_until = asked_at + self.config.duration - self.config.margin();
        Ok(())
    }

    /// Releases `lease`: the next change, through any node, may take it at
    /// once. Every write it made is at or below the HLC of the release.
    pub async fn release(&self, lease: IamMutationLease) {
        let fence = lease.fence();
        let closing = self.clock.now().min(fence.ceiling);
        self.release_epoch(&lease.asked, fence.epoch, closing).await;
        debug!(epoch = fence.epoch, "IAM lease released");
    }
}
