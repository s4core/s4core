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

//! Keeping this node's copy of IAM current with the pool, and telling how
//! current it is.

use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use s4_core::types::SYSTEM_BUCKET;
use tokio::sync::watch;
use tracing::{debug, info, warn};

use super::copy::{ClassVersion, IamCopy};
use super::freshness::IamFreshness;
use super::IamAnnouncement;
use crate::gossip::GossipHandle;
use crate::hlc::HlcTimestamp;
use crate::iam_lease::holder::LEASE_ROUTE_KEY;
use crate::identity::{NodeId, NodeStatus};
use crate::placement::PlacementRouter;
use crate::repair::reconcile::{KeyRef, Reconciler, Settled};
use crate::rpc::{proto, NodeClient};

/// How this node keeps its copy of IAM current.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IamCatchUpConfig {
    /// How often the copy is compared with what the peers announce.
    pub tick: Duration,
    /// How long a peer has to answer one page of its versions.
    pub call_timeout: Duration,
    /// How long after catching up with a peer — or failing to — the node
    /// waits before it asks the same peer again.
    pub retry: Duration,
    /// Versions asked of a peer at a time.
    pub page: u32,
    /// How long after reading its copy the node waits before it compares
    /// it with the pool: zero, but for scenario 62's test hook
    /// (`S4_IAM_TEST_CATCH_UP_DELAY_MS`), which holds a node that has just
    /// started at its gate — closed — long enough to be asked.
    pub start_delay: Duration,
}

impl Default for IamCatchUpConfig {
    fn default() -> Self {
        Self {
            tick: Duration::from_millis(250),
            call_timeout: Duration::from_secs(5),
            retry: Duration::from_secs(1),
            page: 500,
            start_delay: Duration::ZERO,
        }
    }
}

/// Where this node stands with one peer.
#[derive(Debug, Clone, Copy, Default)]
struct PeerCatchUp {
    /// The fingerprint the peer's versions had when this node last caught up
    /// with them: while the peer still announces it, this node holds every
    /// version the peer does.
    caught_up_with: Option<[u8; 16]>,
    /// When this node began to read the versions it last caught up with:
    /// it holds every version the peer held then, whatever the peer took
    /// since.
    caught_up_at: Option<Instant>,
    /// A catch-up with the peer is under way.
    running: bool,
    /// Not asked again before this.
    not_before: Option<Instant>,
}

/// What a peer looks like to this node at one comparison.
#[derive(Debug, Clone, Copy)]
struct PeerView {
    node: NodeId,
    alive: bool,
    announced: Option<IamAnnouncement>,
    caught_up_with: Option<[u8; 16]>,
    caught_up_at: Option<Instant>,
}

/// What one comparison found.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Assessment {
    /// Nodes of the pool, this one included, that agree with its copy.
    agreeing: usize,
    /// Live peers whose copy differs, to catch up with.
    differing: Vec<NodeId>,
    /// The moment a majority of the pool, this node included, confirms the
    /// copy as of; `None` when no majority does.
    confirmed_at: Option<Instant>,
}

/// Compares `ours` with each peer at `now`: a live peer agrees when it
/// announces the same fingerprint, or the one this node last caught up
/// with; a live peer that announces another differs, and is caught up with.
/// A peer that is not alive, or announces nothing — of an earlier release,
/// or before it read its copy — neither agrees nor is asked.
///
/// The copy is confirmed as of the moment a majority vouches for it: an
/// agreeing peer and this node vouch for `now`, a differing peer this node
/// caught up with for the moment it began to read that peer's versions —
/// the copy holds every version the peer held then. A change of IAM keeps
/// the copies apart only until the next catch-up, so a pool that changes IAM
/// without pause, whose copies seldom agree at the moment they are
/// compared, still confirms them about once a catch-up.
fn assess(
    ours: &IamAnnouncement,
    local_counts: bool,
    peers: &[PeerView],
    majority: usize,
    now: Instant,
) -> Assessment {
    let mut assessment = Assessment {
        agreeing: usize::from(local_counts),
        differing: Vec::new(),
        confirmed_at: None,
    };
    let mut vouched = Vec::new();
    if local_counts {
        vouched.push(now);
    }
    for peer in peers.iter().filter(|peer| peer.alive) {
        let Some(theirs) = peer.announced else {
            continue;
        };
        if theirs.digest == ours.digest || peer.caught_up_with == Some(theirs.digest) {
            assessment.agreeing += 1;
            vouched.push(now);
        } else {
            assessment.differing.push(peer.node);
            vouched.extend(peer.caught_up_at);
        }
    }
    // The newest moments a majority vouches for; the copy is confirmed as
    // of the oldest of them.
    vouched.sort_unstable_by(|a, b| b.cmp(a));
    assessment.confirmed_at = majority.checked_sub(1).and_then(|n| vouched.get(n)).copied();
    assessment
}

/// How one node of the pool stands with this node's copy of IAM.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IamNodeView {
    /// The node.
    pub node: NodeId,
    /// Whether it is this node.
    pub local: bool,
    /// Its liveness as gossip holds it; `None` when gossip holds nothing.
    pub status: Option<NodeStatus>,
    /// What it announces of its copy; `None` before it announced anything,
    /// or for a node of an earlier release.
    pub announced: Option<IamAnnouncement>,
    /// Whether it counts towards the majority that confirms this node's
    /// copy: alive, and announcing the same fingerprint or one this node
    /// caught up with.
    pub agrees: bool,
}

/// How the nodes of the pool stand with this node's copy of IAM.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IamPoolView {
    /// This node's copy.
    pub ours: IamAnnouncement,
    /// How many nodes, this one included, confirm the copy.
    pub majority: usize,
    /// Every node of the pool, in the order the pool is placed.
    pub nodes: Vec<IamNodeView>,
}

/// Keeps this node's copy of IAM current with its pool and confirms, in
/// [`IamFreshness`], how current it is (plan CLUSTER-IAM-REPLICATION.md,
/// decision 4).
pub struct IamCatchUp {
    local: NodeId,
    copy: Arc<IamCopy>,
    freshness: Arc<IamFreshness>,
    gossip: Arc<GossipHandle>,
    rpc: Arc<NodeClient>,
    router: Arc<PlacementRouter>,
    reconciler: Arc<Reconciler>,
    gc_grace: Duration,
    config: IamCatchUpConfig,
    peers: StdMutex<HashMap<NodeId, PeerCatchUp>>,
    started: Instant,
}

impl IamCatchUp {
    /// Node `local` keeping `copy` current with its pool — the nodes
    /// `router` places the cluster's control state on — and confirming it in
    /// `freshness`. Peers are compared by what `gossip` holds of them, asked
    /// through `rpc`, and a key one holds newer is settled by `reconciler`.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        local: NodeId,
        copy: Arc<IamCopy>,
        freshness: Arc<IamFreshness>,
        gossip: Arc<GossipHandle>,
        rpc: Arc<NodeClient>,
        router: Arc<PlacementRouter>,
        reconciler: Arc<Reconciler>,
        gc_grace: Duration,
        config: IamCatchUpConfig,
    ) -> Self {
        Self {
            local,
            copy,
            freshness,
            gossip,
            rpc,
            router,
            reconciler,
            gc_grace,
            config,
            peers: StdMutex::new(HashMap::new()),
            started: Instant::now(),
        }
    }

    /// Reads the copy, then compares it with the pool every tick until
    /// `shutdown`.
    pub async fn run(self: Arc<Self>, mut shutdown: watch::Receiver<bool>) {
        loop {
            match self.copy.load().await {
                Ok(announcement) => {
                    self.gossip.announce_iam(announcement);
                    info!(
                        epoch = %announcement.epoch,
                        "read this node's copy of IAM; IAM credentials are checked once a \
                         majority of the pool agrees with it"
                    );
                    break;
                }
                Err(error) => warn!(%error, "could not read this node's copy of IAM; retrying"),
            }
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                _ = shutdown.changed() => return,
            }
        }
        if !self.config.start_delay.is_zero() {
            warn!(
                delay_ms = self.config.start_delay.as_millis() as u64,
                "IAM test hook: this node compares its copy of IAM with the pool only after a delay"
            );
            tokio::select! {
                _ = tokio::time::sleep(self.config.start_delay) => {}
                _ = shutdown.changed() => return,
            }
        }
        let mut ticker = tokio::time::interval(self.config.tick);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut open = false;
        loop {
            tokio::select! {
                _ = ticker.tick() => {}
                _ = shutdown.changed() => return,
            }
            self.tick();
            let now_open = self.freshness.check().is_ok();
            if now_open != open {
                if now_open {
                    info!("a majority of the pool agrees with this node's copy of IAM: IAM credentials are checked");
                } else {
                    warn!(
                        since_confirmed = ?self.freshness.since_confirmed(),
                        "no majority of the pool has agreed with this node's copy of IAM for \
                         longer than S4_IAM_MAX_STALENESS_SECS: IAM credentials are refused (503)"
                    );
                }
                open = now_open;
            }
        }
    }

    /// The nodes of the pool the cluster's control state is kept on.
    fn nodes(&self) -> Option<Vec<NodeId>> {
        match self.router.route(SYSTEM_BUCKET, LEASE_ROUTE_KEY) {
            Ok(decision) => Some(decision.replicas),
            Err(error) => {
                debug!(%error, "the nodes of the cluster's control state cannot be placed");
                None
            }
        }
    }

    /// How every node of the pool stands with this node's copy now, by the
    /// rule a comparison confirms the copy by; `None` before the copy was
    /// read or while the pool cannot be placed. Asks nothing of any peer.
    pub fn pool_view(&self) -> Option<IamPoolView> {
        let ours = self.copy.announcement()?;
        let nodes = self.nodes()?;
        let peers = self.peers.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let nodes = nodes
            .iter()
            .map(|&node| {
                if node == self.local {
                    return IamNodeView {
                        node,
                        local: true,
                        status: Some(NodeStatus::Alive),
                        announced: Some(ours),
                        agrees: true,
                    };
                }
                let view = PeerView {
                    node,
                    alive: self.gossip.peer_status(&node) == Some(NodeStatus::Alive),
                    announced: self.gossip.iam_announcement(&node),
                    caught_up_with: peers.get(&node).and_then(|peer| peer.caught_up_with),
                    caught_up_at: None,
                };
                IamNodeView {
                    node,
                    local: false,
                    status: self.gossip.peer_status(&node),
                    announced: view.announced,
                    agrees: assess(&ours, false, &[view], 1, Instant::now()).agreeing == 1,
                }
            })
            .collect::<Vec<_>>();
        Some(IamPoolView {
            ours,
            majority: nodes.len() / 2 + 1,
            nodes,
        })
    }

    /// One comparison: announces the copy, confirms it when a majority of the
    /// pool agrees with it, and starts catching up with every live peer whose
    /// copy differs.
    pub fn tick(self: &Arc<Self>) {
        let Some(ours) = self.copy.announcement() else {
            return;
        };
        self.gossip.announce_iam(ours);
        let Some(nodes) = self.nodes() else {
            return;
        };
        let majority = nodes.len() / 2 + 1;
        let now = Instant::now();
        let mut peers = self.peers.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let views: Vec<PeerView> = nodes
            .iter()
            .filter(|node| **node != self.local)
            .map(|&node| PeerView {
                node,
                alive: self.gossip.peer_status(&node) == Some(NodeStatus::Alive),
                announced: self.gossip.iam_announcement(&node),
                caught_up_with: peers.get(&node).and_then(|peer| peer.caught_up_with),
                caught_up_at: peers.get(&node).and_then(|peer| peer.caught_up_at),
            })
            .collect();
        let assessment = assess(&ours, nodes.contains(&self.local), &views, majority, now);
        if let Some(at) = assessment.confirmed_at {
            self.freshness.confirm(at);
        }
        for node in assessment.differing {
            let peer = peers.entry(node).or_default();
            if peer.running || peer.not_before.is_some_and(|not_before| now < not_before) {
                continue;
            }
            peer.running = true;
            tokio::spawn(self.clone().catch_up_with(node));
        }
        drop(peers);
        let lag = self.freshness.since_confirmed().unwrap_or_else(|| self.started.elapsed());
        crate::cluster_metrics::set_iam_copy(ours.epoch.wall_time, lag);
    }

    /// Catches up with `peer` and records how it went.
    async fn catch_up_with(self: Arc<Self>, peer: NodeId) {
        let outcome = self.settle_with(peer).await;
        let mut peers = self.peers.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let state = peers.entry(peer).or_default();
        state.running = false;
        state.not_before = Some(Instant::now() + self.config.retry);
        match outcome {
            Ok((digest, settled, began)) => {
                state.caught_up_with = Some(digest);
                state.caught_up_at = Some(began);
                if settled > 0 {
                    info!(%peer, keys = settled, "caught this node's copy of IAM up with a peer");
                }
            }
            Err(why) => {
                debug!(%peer, %why, "could not catch this node's copy of IAM up with a peer");
            }
        }
    }

    /// Reads the versions `peer` holds and settles every key this node holds
    /// in an older version, or not at all; the fingerprint the peer's
    /// versions had, how many keys were settled, and when the reading
    /// began: every version the peer held then this node holds now, as
    /// versions only move forward.
    async fn settle_with(&self, peer: NodeId) -> Result<([u8; 16], usize, Instant), String> {
        let began = Instant::now();
        let mut digest = None;
        let mut behind = Vec::new();
        let mut start_after = String::new();
        loop {
            let request = proto::IamVersionsRequest {
                start_after: start_after.clone(),
                max_keys: self.config.page,
            };
            let response = tokio::time::timeout(
                self.config.call_timeout,
                self.rpc.send_iam_versions(peer, request),
            )
            .await
            .map_err(|_| format!("no answer in {:?}", self.config.call_timeout))?
            .map_err(|error| error.to_string())?;
            if !response.error.is_empty() {
                return Err(response.error);
            }
            if digest.is_none() {
                let first: [u8; 16] = response
                    .digest
                    .as_slice()
                    .try_into()
                    .map_err(|_| format!("a fingerprint of {} bytes", response.digest.len()))?;
                digest = Some(first);
            }
            let truncated = response.is_truncated;
            let last = response.versions.last().map(|version| version.key.clone());
            for version in response.versions {
                let theirs = ClassVersion {
                    hlc: version
                        .hlc
                        .as_ref()
                        .map(HlcTimestamp::from_proto)
                        .unwrap_or_else(HlcTimestamp::zero),
                    key: version.key,
                    tombstone: version.tombstone,
                    etag: version.etag,
                };
                let ours = self.copy.version_of(&theirs.key).await.map_err(|e| e.to_string())?;
                if theirs.is_missing_from(ours.as_ref()) {
                    behind.push(theirs.key);
                }
            }
            match last {
                Some(last) if truncated => start_after = last,
                _ => break,
            }
        }
        let digest = digest.ok_or_else(|| "no fingerprint".to_string())?;
        let learner = !self
            .nodes()
            .ok_or_else(|| "control-state owners are unknown".to_string())?
            .contains(&self.local);
        for key in &behind {
            let key = KeyRef {
                bucket: SYSTEM_BUCKET,
                key,
            };
            let settled = if learner {
                self.reconciler.settle_local_learner(key, peer).await
            } else {
                self.reconciler.settle(key, &[self.local, peer], self.gc_grace).await
            };
            match settled {
                Settled::InStep => {}
                Settled::NotOurs => {
                    return Err(format!(
                        "{}: IAM copy was not settled on this node",
                        key.key
                    ))
                }
                Settled::Ambiguous(why) | Settled::Failed(why) => {
                    return Err(format!("{}: {why}", key.key));
                }
            }
        }
        Ok((digest, behind.len(), began))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn announced(digest: u8) -> IamAnnouncement {
        IamAnnouncement {
            epoch: HlcTimestamp::from_record(u64::from(digest), 0, 1),
            digest: [digest; 16],
        }
    }

    fn peer(node: u128, alive: bool, digest: Option<u8>, caught_up: Option<u8>) -> PeerView {
        PeerView {
            node: NodeId(node),
            alive,
            announced: digest.map(announced),
            caught_up_with: caught_up.map(|digest| [digest; 16]),
            caught_up_at: None,
        }
    }

    /// A peer this node caught up with at `at`, which has changed since.
    fn caught_up_at(node: u128, digest: u8, at: Instant) -> PeerView {
        PeerView {
            caught_up_at: Some(at),
            ..peer(node, true, Some(digest), Some(digest.wrapping_sub(1)))
        }
    }

    #[test]
    fn a_live_peer_with_the_same_copy_agrees() {
        let ours = announced(1);
        let now = Instant::now();
        let found = assess(
            &ours,
            true,
            &[peer(2, true, Some(1), None), peer(3, true, Some(1), None)],
            2,
            now,
        );
        assert_eq!(
            found,
            Assessment {
                agreeing: 3,
                differing: vec![],
                confirmed_at: Some(now),
            }
        );
    }

    #[test]
    fn a_peer_that_is_down_or_silent_neither_agrees_nor_is_asked() {
        let ours = announced(1);
        let found = assess(
            &ours,
            true,
            &[
                peer(2, false, Some(1), None),
                peer(3, true, None, None),
                PeerView {
                    caught_up_at: Some(Instant::now()),
                    ..peer(4, false, Some(9), None)
                },
            ],
            3,
            Instant::now(),
        );
        assert_eq!(
            found,
            Assessment {
                agreeing: 1,
                differing: vec![],
                confirmed_at: None,
            }
        );
    }

    /// A peer that announces another copy is caught up with; once it was,
    /// it agrees for as long as it announces the copy caught up with.
    #[test]
    fn a_peer_with_another_copy_agrees_once_caught_up_with() {
        let ours = announced(1);
        let now = Instant::now();
        let differing = assess(&ours, true, &[peer(2, true, Some(5), None)], 2, now);
        assert_eq!(differing.agreeing, 1);
        assert_eq!(differing.differing, vec![NodeId(2)]);
        assert_eq!(differing.confirmed_at, None);

        let caught_up = assess(&ours, true, &[peer(2, true, Some(5), Some(5))], 2, now);
        assert_eq!(caught_up.agreeing, 2);
        assert!(caught_up.differing.is_empty());
        assert_eq!(caught_up.confirmed_at, Some(now));

        // The peer changed since: caught up with again.
        let changed = assess(&ours, true, &[peer(2, true, Some(6), Some(5))], 2, now);
        assert_eq!(changed.agreeing, 1);
        assert_eq!(changed.differing, vec![NodeId(2)]);
    }

    /// While IAM changes without pause, the copies seldom agree at the
    /// moment they are compared: each differing peer this node caught up
    /// with vouches for the copy as of the moment the catch-up began, and the
    /// copy is confirmed as of the oldest moment a majority vouches for.
    #[test]
    fn a_peer_caught_up_with_vouches_for_the_moment_the_catch_up_began() {
        let ours = announced(1);
        let now = Instant::now();
        let a_second_ago = now - Duration::from_secs(1);
        let two_seconds_ago = now - Duration::from_secs(2);
        let peers = [
            caught_up_at(2, 7, a_second_ago),
            caught_up_at(3, 8, two_seconds_ago),
            peer(4, true, Some(9), None),
            peer(5, true, Some(1), None),
        ];
        let found = assess(&ours, true, &peers, 3, now);
        assert_eq!(found.agreeing, 2, "this node and node 5");
        assert_eq!(found.differing, vec![NodeId(2), NodeId(3), NodeId(4)]);
        assert_eq!(found.confirmed_at, Some(a_second_ago));
        let found = assess(&ours, true, &peers, 4, now);
        assert_eq!(found.confirmed_at, Some(two_seconds_ago));
        assert_eq!(assess(&ours, true, &peers, 5, now).confirmed_at, None);
    }

    #[test]
    fn a_node_outside_the_pool_does_not_count_itself() {
        let now = Instant::now();
        let found = assess(
            &announced(1),
            false,
            &[peer(2, true, Some(1), None)],
            2,
            now,
        );
        assert_eq!(found.agreeing, 1);
        assert_eq!(found.confirmed_at, None);
    }
}
