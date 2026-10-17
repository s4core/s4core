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

//! Settling one key: its replicas are brought to its newest version.
//!
//! Hint delivery and anti-entropy settle keys the same way, here. The
//! versions of a key are read from its replicas ([`KeyStateSource`]), the
//! newest one is picked by HLC, a tombstone included ([`newest_of`]), and each
//! replica behind it is brought to it (`Reconciler::bring`): a copy is
//! written with its own HLC (in chunks when it is large), a tombstone is sent
//! as a delete at its HLC. The replica's LWW keeps anything newer it took
//! meanwhile, so settling a key again, or in any order, is safe.
//!
//! A copy is spread to a replica without the key only when that cannot bring
//! a deleted key back: every replica that reported the key absent keeps
//! tombstones from a horizon older than the copy, or as many replicas hold
//! the copy as a confirmed write leaves (plan ANTI-ENTROPY.md, decision 7).
//! Otherwise the key is ambiguous and nothing is written.
//!
//! Anti-entropy settles a key with `Reconciler::settle`: every answering
//! replica is brought to the newest version, and a tombstone older than
//! `gc_grace` is not carried to a replica without the key, whose absence then
//! hides no live copy. Hint delivery brings one target to the newest version
//! of the other replicas, and adds what only a hint knows (see `hints`).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use s4_core::storage::{EtagRank, VersionKind, VersionRank};

use crate::clock::hlc::now_millis;
use crate::error::ClusterError;
use crate::gossip::GossipHandle;
use crate::hlc::HlcTimestamp;
use crate::identity::NodeId;
use crate::protocol::{
    CONDITIONAL_DELETE_MIN_PROTOCOL_VERSION, PROTOCOL_VERSION, TOMBSTONE_MIN_PROTOCOL_VERSION,
};
use crate::replica::{ReplicaReadHandler, ReplicaWriteHandler};
use crate::rpc::proto;
use crate::rpc::NodeClient;

/// The newest version of a key on the replicas asked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NewestVersion {
    /// `node` holds a copy of it.
    Copy {
        /// Its HLC.
        hlc: HlcTimestamp,
        /// The rank of its ETag, which orders copies of one HLC.
        etag: EtagRank,
        /// A replica that holds it.
        node: NodeId,
        /// Whether the copy may be spread: every replica that reported the
        /// key absent keeps tombstones from a horizon older than the copy, or
        /// at least as many replicas hold exactly this version as a confirmed
        /// write leaves. A copy that is neither may be one that missed a
        /// delete the absent replicas took before they kept tombstones.
        confirmed: bool,
    },
    /// The key was deleted: the newest version is a tombstone.
    Tombstone {
        /// The HLC of the delete.
        hlc: HlcTimestamp,
    },
    /// It is erasure coded: no replica holds a copy of its bytes.
    ErasureCoded {
        /// Its HLC.
        hlc: HlcTimestamp,
    },
    /// None of the replicas asked holds the key.
    Absent,
    /// The target is no longer a replica of the key, or it is the only one:
    /// there is nothing to bring it to.
    NotAReplica,
}

/// One copy of an object, as a replica holds it.
#[derive(Debug, Clone)]
pub struct StoredCopy {
    /// Its HLC, made up for a record written without one
    /// ([`HlcTimestamp::of_record`]); `None` when the replica reported no HLC
    /// at all, which no LWW could weigh against another.
    pub hlc: Option<HlcTimestamp>,
    /// The object's bytes.
    pub data: Vec<u8>,
    /// The ETag of the version, which the copy keeps on the replica it is
    /// written to; empty when the source did not report one.
    pub etag: String,
    /// SHA-256 of the bytes.
    pub content_hash: Vec<u8>,
    /// MIME type.
    pub content_type: String,
    /// Metadata as the replica stores it.
    pub metadata: HashMap<String, String>,
    /// Version ID, if any.
    pub version_id: Option<String>,
}

/// What one replica holds of a key, as it reported it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaHolds {
    /// A version: an object, its tombstone, or the namespace entry of an
    /// erasure-coded object.
    Version {
        /// Its HLC; made up for a record written without one.
        hlc: HlcTimestamp,
        /// The version is the key's tombstone.
        tombstone: bool,
        /// The version is an erasure-coded object's namespace entry.
        erasure_coded: bool,
        /// The rank of its ETag; [`EtagRank::NONE`] for a tombstone.
        etag: EtagRank,
    },
    /// Nothing for the key.
    Nothing {
        /// The replica's tombstone horizon: every delete it took since left a
        /// tombstone. `None` for one that keeps no tombstones or reported no
        /// horizon, whose absence proves nothing about any delete.
        horizon: Option<HlcTimestamp>,
    },
}

impl ReplicaHolds {
    /// The rank of the version held, `None` for nothing.
    pub fn rank(&self) -> Option<VersionRank> {
        match *self {
            Self::Version {
                hlc,
                tombstone,
                erasure_coded,
                etag,
            } => Some(VersionRank {
                hlc: hlc.as_tuple(),
                kind: kind_of(tombstone, erasure_coded),
                etag: if tombstone { EtagRank::NONE } else { etag },
            }),
            Self::Nothing { .. } => None,
        }
    }
}

impl NewestVersion {
    /// The rank of a version a replica is brought to: a copy or a
    /// tombstone. `None` for anything else, which no replica is brought to.
    pub fn rank(&self) -> Option<VersionRank> {
        match *self {
            Self::Copy { hlc, etag, .. } => Some(VersionRank {
                hlc: hlc.as_tuple(),
                kind: VersionKind::Live,
                etag,
            }),
            Self::Tombstone { hlc } => Some(VersionRank {
                hlc: hlc.as_tuple(),
                kind: VersionKind::Tombstone,
                etag: EtagRank::NONE,
            }),
            Self::ErasureCoded { .. } | Self::Absent | Self::NotAReplica => None,
        }
    }
}

/// The kind of a version reported as a tombstone or an erasure-coded entry,
/// or neither.
fn kind_of(tombstone: bool, erasure_coded: bool) -> VersionKind {
    if tombstone {
        VersionKind::Tombstone
    } else if erasure_coded {
        VersionKind::ErasureCoded
    } else {
        VersionKind::Live
    }
}

/// The rank of what a replica's answer to a HEAD says it holds, `None` for
/// nothing.
pub(crate) fn rank_of_head(head: &proto::HeadObjectResponse) -> Option<VersionRank> {
    let hlc = head.hlc.as_ref().map(HlcTimestamp::from_proto)?;
    if head.exists {
        let kind = kind_of(false, head.erasure_coded);
        Some(VersionRank::new(hlc.as_tuple(), kind, &head.etag))
    } else if head.tombstone {
        Some(VersionRank::new(hlc.as_tuple(), VersionKind::Tombstone, ""))
    } else {
        None
    }
}

/// What the replicas of a key reported, read by [`KeyStateSource::key_versions`].
#[derive(Debug, Clone, Default)]
pub struct KeyVersions {
    /// The key's replicas, as placement has them now.
    pub replicas: Vec<NodeId>,
    /// The replicas that answered, and what they hold.
    pub answers: Vec<(NodeId, ReplicaHolds)>,
    /// The replicas that did not answer, and why.
    pub unanswered: Vec<(NodeId, String)>,
    /// How many copies a confirmed write leaves on the replicas other than
    /// one that missed it: the write quorum, at most the replicas but one.
    pub confirming: usize,
    /// The replicas whose copies count towards `confirming`: those of the
    /// layout reads take. While a pool moves its bucket metadata they may be
    /// fewer than `replicas` — at stage 1 a write is confirmed by the old
    /// layout's quorum, and a copy on the new replicas alone is one whose
    /// write failed (plan BUCKET-METADATA-REPLICATION.md, decision 5).
    pub confirming_replicas: Vec<NodeId>,
}

impl KeyVersions {
    /// The newest version the answers hold ([`newest_of_among`]).
    pub fn newest(&self) -> NewestVersion {
        newest_of_among(&self.answers, self.confirming, &self.confirming_replicas)
    }
}

/// Where the current state of a key is read.
#[async_trait::async_trait]
pub trait KeyStateSource: Send + Sync {
    /// The newest version of `bucket/key` among its replicas other than
    /// `target`. Every one of them has to answer: one that does not makes this
    /// an error, never "absent", because it may be the one holding the newest
    /// version. Reads nothing into the replicas and repairs nothing.
    async fn newest_version(
        &self,
        bucket: &str,
        key: &str,
        target: NodeId,
    ) -> Result<NewestVersion, ClusterError>;

    /// What every replica of `bucket/key` holds; a replica that does not
    /// answer is listed as such rather than failing the whole read. `Err`
    /// when the key cannot be placed.
    async fn key_versions(&self, bucket: &str, key: &str) -> Result<KeyVersions, ClusterError>;

    /// The copy of `bucket/key` that `node` holds.
    async fn read_copy(
        &self,
        bucket: &str,
        key: &str,
        node: NodeId,
    ) -> Result<StoredCopy, ClusterError>;
}

/// The newest of the versions in `answers` by [`VersionRank`]: by HLC, a
/// tombstone included, and at one HLC a tombstone over an erasure-coded entry
/// over a copy, copies ordered by their ETags.
///
/// At a tie of HLCs, one replica holding the tombstone is enough for the
/// version to count as deleted, and one holding only the erasure-coded record
/// for it to count as erasure coded: the object is being moved out of
/// replication, and a copy is not to be spread again.
///
/// A copy is confirmed when every replica that holds nothing keeps tombstones
/// from a horizon older than the copy, or when at least `confirming` replicas
/// hold exactly it. An absent replica without such a horizon may be one that
/// took a delete the copy missed before it kept tombstones, and only a copy on
/// as many replicas as a confirmed write leaves is known to be newer than any
/// delete that was confirmed. A replica whose horizon is older than the copy
/// and has no key took no delete of it: that delete would have left a
/// tombstone (plan ANTI-ENTROPY.md, decision 7).
pub fn newest_of(answers: &[(NodeId, ReplicaHolds)], confirming: usize) -> NewestVersion {
    let every: Vec<NodeId> = answers.iter().map(|(node, _)| *node).collect();
    newest_of_among(answers, confirming, &every)
}

/// [`newest_of`], counting towards `confirming` only the copies held by
/// `counted`: the replicas of the layout reads take.
pub fn newest_of_among(
    answers: &[(NodeId, ReplicaHolds)],
    confirming: usize,
    counted: &[NodeId],
) -> NewestVersion {
    let ranked = || answers.iter().filter_map(|(node, holds)| Some((*node, holds.rank()?)));
    let Some(newest) = ranked().map(|(_, rank)| rank).max() else {
        return NewestVersion::Absent;
    };
    let (wall, logical, node_id) = newest.hlc;
    let hlc = HlcTimestamp::from_record(wall, logical, node_id);
    match newest.kind {
        VersionKind::Tombstone => NewestVersion::Tombstone { hlc },
        VersionKind::ErasureCoded => NewestVersion::ErasureCoded { hlc },
        VersionKind::Live => {
            let holding: Vec<NodeId> =
                ranked().filter(|(_, rank)| *rank == newest).map(|(node, _)| node).collect();
            let Some(&node) = holding.first() else {
                return NewestVersion::Absent;
            };
            let counted_holding = holding.iter().filter(|node| counted.contains(node)).count();
            NewestVersion::Copy {
                hlc,
                etag: newest.etag,
                node,
                confirmed: answers
                    .iter()
                    .filter_map(|(_, holds)| match holds {
                        ReplicaHolds::Nothing { horizon } => Some(*horizon),
                        ReplicaHolds::Version { .. } => None,
                    })
                    .all(|horizon| horizon.is_some_and(|horizon| horizon < hlc))
                    || counted_holding >= confirming,
            }
        }
    }
}

/// The newest version that at least `quorum` of `counted` hold, or have
/// gone past: what a confirmed write left at the least. A version held by
/// fewer is one whose write may have failed, and does not count. `None` when
/// no version is held that widely.
pub(crate) fn newest_confirmed(
    answers: &[(NodeId, ReplicaHolds)],
    counted: &[NodeId],
    quorum: usize,
) -> Option<VersionRank> {
    let ranks: Vec<VersionRank> = answers
        .iter()
        .filter(|(node, _)| counted.contains(node))
        .filter_map(|(_, holds)| holds.rank())
        .collect();
    ranks
        .iter()
        .copied()
        .filter(|candidate| ranks.iter().filter(|held| *held >= candidate).count() >= quorum.max(1))
        .max()
}

/// Whether a replica holding `holds` is behind `newest`: it holds nothing, or
/// a version of a lower rank.
fn is_behind(holds: ReplicaHolds, newest: NewestVersion) -> bool {
    let Some(newest) = newest.rank() else {
        return false;
    };
    holds.rank().is_none_or(|held| held < newest)
}

/// What became of bringing one replica to a key's newest version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Outcome {
    /// The target is at the key's current state now, or already was.
    Delivered,
    /// The key has left replication — erasure coded, or placed elsewhere.
    Superseded,
    /// The target refused, or cannot take, what it would be sent.
    Rejected(String),
    /// The replicas disagree in a way only tombstones could settle: the
    /// newest copy may be one that missed a delete. Nothing was written.
    Ambiguous(String),
    /// The key cannot be settled now; it waits for a later pass.
    Later(String),
}

/// What became of settling one key across its replicas.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Settled {
    /// Every replica that answered is at the key's newest version: it was
    /// brought there, or already was.
    InStep,
    /// The newest copy may be one that missed a delete; nothing was written.
    Ambiguous(String),
    /// A replica was not brought to the newest version, or did not answer.
    Failed(String),
    /// The key is not anti-entropy's to settle: it is erasure coded, or the
    /// pair asked about is no longer among its replicas.
    NotOurs,
}

/// This node's own replica, reached without the network: the handlers its
/// gRPC server answers peers with.
#[derive(Clone)]
pub struct LocalReplica {
    /// Takes writes and deletes.
    pub write: Arc<ReplicaWriteHandler>,
    /// Answers HEAD.
    pub read: Arc<ReplicaReadHandler>,
}

/// What the copies a [`Reconciler`] wrote add up to: the bytes delivered,
/// and the copies a target refused because their bytes did not match their
/// SHA-256.
#[derive(Debug, Default)]
pub struct CopyMeter {
    bytes: std::sync::atomic::AtomicU64,
    checksum_refusals: std::sync::atomic::AtomicU64,
}

impl CopyMeter {
    /// Bytes of the copies delivered so far.
    pub fn bytes(&self) -> u64 {
        self.bytes.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Copies a target refused for a checksum mismatch so far.
    pub fn checksum_refusals(&self) -> u64 {
        self.checksum_refusals.load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// Migration streams its own current snapshot directly. If that source
/// changes while ordinary repair chooses a version, retry rather than reading
/// the entire local object through the buffered repair interface.
struct WithoutBufferedSource {
    inner: Arc<dyn KeyStateSource>,
    source: NodeId,
}
#[async_trait::async_trait]
impl KeyStateSource for WithoutBufferedSource {
    async fn newest_version(
        &self,
        bucket: &str,
        key: &str,
        target: NodeId,
    ) -> Result<NewestVersion, ClusterError> {
        self.inner.newest_version(bucket, key, target).await
    }
    async fn key_versions(&self, bucket: &str, key: &str) -> Result<KeyVersions, ClusterError> {
        self.inner.key_versions(bucket, key).await
    }
    async fn read_copy(
        &self,
        bucket: &str,
        key: &str,
        node: NodeId,
    ) -> Result<StoredCopy, ClusterError> {
        if node == self.source {
            return Err(ClusterError::Unavailable(
                "local migration source changed; retry through its bounded stream".into(),
            ));
        }
        self.inner.read_copy(bucket, key, node).await
    }
}

/// Brings replicas of a key to its newest version.
pub struct Reconciler {
    source: Arc<dyn KeyStateSource>,
    rpc_client: Arc<NodeClient>,
    gossip: Arc<GossipHandle>,
    local_node_id: NodeId,
    local: Option<LocalReplica>,
    topology_epoch: u64,
    /// How long one call to a replica may take.
    call_timeout: Duration,
    /// Where the copies written are counted, if anywhere.
    meter: Option<Arc<CopyMeter>>,
}

/// The key being settled.
#[derive(Debug, Clone, Copy)]
pub(crate) struct KeyRef<'a> {
    pub(crate) bucket: &'a str,
    pub(crate) key: &'a str,
}

impl Reconciler {
    /// Reads current states from `source` and reaches replicas through
    /// `rpc_client`, as node `local_node_id` on `topology_epoch`.
    pub fn new(
        source: Arc<dyn KeyStateSource>,
        rpc_client: Arc<NodeClient>,
        gossip: Arc<GossipHandle>,
        local_node_id: NodeId,
        topology_epoch: u64,
    ) -> Self {
        Self {
            source,
            rpc_client,
            gossip,
            local_node_id,
            local: None,
            topology_epoch,
            call_timeout: Duration::from_secs(60),
            meter: None,
        }
    }

    pub(crate) fn without_buffered_source(mut self, source: NodeId) -> Self {
        self.source = Arc::new(WithoutBufferedSource {
            inner: self.source,
            source,
        });
        self
    }

    /// The same, counting the copies it writes in `meter`.
    pub fn with_copy_meter(mut self, meter: Arc<CopyMeter>) -> Self {
        self.meter = Some(meter);
        self
    }

    /// The same, able to bring this node's own replica too.
    pub fn with_local_replica(mut self, local: LocalReplica) -> Self {
        self.local = Some(local);
        self
    }

    pub(crate) fn source(&self) -> &Arc<dyn KeyStateSource> {
        &self.source
    }

    /// The protocol `target` announced. `Err` while it has announced none: a
    /// node that has just come back is known by gossip a moment before its
    /// metadata arrives.
    pub(crate) fn protocol_of(&self, target: NodeId) -> Result<u32, ClusterError> {
        if target == self.local_node_id {
            return Ok(PROTOCOL_VERSION);
        }
        match self.gossip.get_node(&target).map(|state| state.meta.protocol_version) {
            Some(0) | None => Err(ClusterError::Unavailable(format!(
                "{target} has not announced its protocol yet"
            ))),
            Some(version) => Ok(version),
        }
    }

    /// Whether `target` understands a delete of one exact generation; a node
    /// that does not would take it for an unconditional delete.
    fn takes_conditional_deletes(&self, target: NodeId) -> Result<bool, ClusterError> {
        Ok(self.protocol_of(target)? >= CONDITIONAL_DELETE_MIN_PROTOCOL_VERSION)
    }

    /// Settles `bucket/key` for anti-entropy: reads its version on every
    /// replica, and brings each one that answered and is behind to the
    /// newest. Every node of `required` has to answer; the others are settled
    /// if they do. A tombstone older than `gc_grace` is not carried to a
    /// replica without the key.
    pub(crate) async fn settle(
        &self,
        key: KeyRef<'_>,
        required: &[NodeId],
        gc_grace: Duration,
    ) -> Settled {
        self.settle_counting(key, Some(required), gc_grace).await.0
    }

    /// Copies control state to a joining coordinator without counting it as
    /// an owner. Only the old owners' quorum confirms the source; the peer
    /// whose version page is being adopted must itself answer.
    pub(crate) async fn settle_local_learner(&self, key: KeyRef<'_>, peer: NodeId) -> Settled {
        if !s4_core::types::is_cluster_control(key.bucket, key.key) {
            return Settled::Failed("only cluster control state may be copied to a learner".into());
        }
        let versions = match self.source.key_versions(key.bucket, key.key).await {
            Ok(versions) => versions,
            Err(error) => return Settled::Failed(error.to_string()),
        };
        let answered = |node| versions.answers.iter().any(|(id, _)| *id == node);
        let count = versions.confirming_replicas.iter().filter(|id| answered(**id)).count();
        if !versions.replicas.contains(&peer)
            || !answered(peer)
            || versions.confirming == 0
            || count < versions.confirming
        {
            return Settled::Failed(
                "waiting for the source peer and the old control-state quorum".into(),
            );
        }
        let newest = versions.newest();
        let hlc = match newest {
            NewestVersion::Copy {
                hlc,
                confirmed: true,
                ..
            }
            | NewestVersion::Tombstone { hlc } => hlc,
            _ => {
                return Settled::Failed(
                    "the old owners did not confirm the learner's source version".into(),
                )
            }
        };
        match self.bring(self.local_node_id, key, newest, hlc).await {
            Ok(Outcome::Delivered) => Settled::InStep,
            Ok(Outcome::Ambiguous(why)) => Settled::Ambiguous(why),
            Ok(Outcome::Rejected(why) | Outcome::Later(why)) => Settled::Failed(why),
            Ok(Outcome::Superseded) => {
                Settled::Failed("control-state learner copy was superseded".into())
            }
            Err(error) => Settled::Failed(error.to_string()),
        }
    }

    /// Settles `bucket/key` as [`Self::settle`] does, every replica of the
    /// key required when `required` is `None`; and how many replicas were
    /// brought to a version they did not hold.
    pub(crate) async fn settle_counting(
        &self,
        key: KeyRef<'_>,
        required: Option<&[NodeId]>,
        gc_grace: Duration,
    ) -> (Settled, usize) {
        self.settle_targets(key, required, None, gc_grace).await
    }

    /// Settles every current owner while a foreign holder remains evidence
    /// only. It never creates a new replica on a migration source outside
    /// placement, and uses the same history confirmation as ordinary repair.
    pub(crate) async fn settle_owners_counting(
        &self,
        key: KeyRef<'_>,
        owners: &[NodeId],
        gc_grace: Duration,
    ) -> (Settled, usize) {
        self.settle_targets(key, Some(owners), Some(owners), gc_grace).await
    }

    async fn settle_targets(
        &self,
        key: KeyRef<'_>,
        required: Option<&[NodeId]>,
        targets: Option<&[NodeId]>,
        gc_grace: Duration,
    ) -> (Settled, usize) {
        let versions = match self.source.key_versions(key.bucket, key.key).await {
            Ok(versions) => versions,
            Err(e) => return (Settled::Failed(e.to_string()), 0),
        };
        let required = required.unwrap_or(&versions.replicas);
        if !required.iter().all(|node| versions.replicas.contains(node)) {
            return (Settled::NotOurs, 0);
        }
        if let Some((node, why)) =
            versions.unanswered.iter().find(|(node, _)| required.contains(node))
        {
            return (
                Settled::Failed(format!("replica {node} did not answer: {why}")),
                0,
            );
        }
        let newest = versions.newest();
        let missed = match newest {
            NewestVersion::Copy { hlc, .. } | NewestVersion::Tombstone { hlc } => hlc,
            NewestVersion::ErasureCoded { .. } | NewestVersion::NotAReplica => {
                return (Settled::NotOurs, 0)
            }
            NewestVersion::Absent => return (Settled::InStep, 0),
        };
        // Only a confirmed copy or a tombstone is written; an unconfirmed
        // copy is only checked against.
        let writes = matches!(
            newest,
            NewestVersion::Copy {
                confirmed: true,
                ..
            } | NewestVersion::Tombstone { .. }
        );
        let mut brought = 0;
        // A tombstone this old may already be gone from the replicas that
        // kept it: an absence beside it hides no live copy.
        let carried_to_absence = match newest {
            NewestVersion::Tombstone { hlc } => {
                let grace = u64::try_from(gc_grace.as_millis()).unwrap_or(u64::MAX);
                now_millis().saturating_sub(hlc.wall_time) <= grace
            }
            _ => true,
        };
        let mut failed = None;
        let mut ambiguous = None;
        let mut superseded = false;
        // Every replica holding the newest version can hand its copy over:
        // one whose copy is refused as damaged is followed by the next.
        let newest_rank = newest.rank();
        let holders: Vec<NodeId> = versions
            .answers
            .iter()
            .filter(|(_, holds)| holds.rank().is_some() && holds.rank() == newest_rank)
            .map(|(node, _)| *node)
            .collect();
        for (target, holds) in &versions.answers {
            if targets.is_some_and(|targets| !targets.contains(target)) {
                continue;
            }
            if !is_behind(*holds, newest) {
                continue;
            }
            if matches!(holds, ReplicaHolds::Nothing { .. }) && !carried_to_absence {
                continue;
            }
            match self.bring_from(*target, key, newest, missed, &holders).await {
                Ok(Outcome::Delivered) => {
                    if writes {
                        brought += 1;
                    }
                }
                // This replica's record is the EC runtime's to remove; the
                // other replicas behind are still brought to the newest.
                Ok(Outcome::Superseded) => superseded = true,
                Ok(Outcome::Ambiguous(why)) => {
                    ambiguous.get_or_insert(why);
                }
                Ok(Outcome::Rejected(why) | Outcome::Later(why)) => {
                    failed.get_or_insert(format!("replica {target}: {why}"));
                }
                Err(e) => {
                    failed.get_or_insert(format!("replica {target}: {e}"));
                }
            }
        }
        let settled = match (failed, ambiguous) {
            (Some(why), _) => Settled::Failed(why),
            (None, Some(why)) => Settled::Ambiguous(why),
            (None, None) if superseded => Settled::NotOurs,
            (None, None) => Settled::InStep,
        };
        (settled, brought)
    }

    /// Brings each of `targets` behind `newest` — the newest version of
    /// `key` its old owners confirmed ([`newest_confirmed`]) — to it, taken
    /// from a replica that holds exactly that version. For a key whose newest
    /// copy is one whose write may have failed: anti-entropy leaves such a key
    /// ambiguous, but a range of a growing pool cannot move without the
    /// confirmed version on its new owners. A target's last-writer-wins keeps
    /// whatever newer version it holds. Returns how many targets took it.
    ///
    /// # Errors
    ///
    /// Why a target was not brought: an erasure-coded entry (its move is
    /// the namespace entry's), no holder could hand the version over, or the
    /// target refused it.
    pub(crate) async fn bring_confirmed(
        &self,
        key: KeyRef<'_>,
        versions: &KeyVersions,
        newest: VersionRank,
        targets: &[NodeId],
    ) -> Result<usize, String> {
        let holds_of = |node: NodeId| {
            versions
                .answers
                .iter()
                .find(|(answered, _)| *answered == node)
                .and_then(|(_, holds)| holds.rank())
        };
        let holders: Vec<NodeId> = versions
            .answers
            .iter()
            .filter(|(_, holds)| holds.rank() == Some(newest))
            .map(|(node, _)| *node)
            .collect();
        let (wall, logical, origin) = newest.hlc;
        let hlc = HlcTimestamp::from_record(wall, logical, origin);
        let mut brought = 0;
        for target in targets {
            if holds_of(*target).is_some_and(|held| held >= newest) {
                continue;
            }
            let outcome = match newest.kind {
                VersionKind::ErasureCoded => {
                    return Err("the confirmed version is an erasure-coded entry".into())
                }
                VersionKind::Tombstone => self
                    .deliver_tombstone(*target, key, hlc)
                    .await
                    .map_err(|e| format!("replica {target}: {e}"))?,
                VersionKind::Live => {
                    let mut outcome =
                        Outcome::Later("no replica could hand the confirmed version over".into());
                    for source in holders.iter().filter(|holder| *holder != target) {
                        let copy = match self.source.read_copy(key.bucket, key.key, *source).await {
                            Ok(copy) => copy,
                            Err(e) => {
                                outcome = Outcome::Later(e.to_string());
                                continue;
                            }
                        };
                        // The source may have taken a newer version since it
                        // answered: only the confirmed one is spread here.
                        if copy.hlc != Some(hlc) {
                            outcome = Outcome::Later(format!(
                                "replica {source} no longer holds the confirmed version"
                            ));
                            continue;
                        }
                        outcome = self
                            .write_copy(*target, key, copy)
                            .await
                            .map_err(|e| format!("replica {target}: {e}"))?;
                        if matches!(outcome, Outcome::Delivered) {
                            break;
                        }
                    }
                    outcome
                }
            };
            match outcome {
                Outcome::Delivered => brought += 1,
                Outcome::Superseded => {}
                Outcome::Rejected(why) | Outcome::Later(why) | Outcome::Ambiguous(why) => {
                    return Err(format!("replica {target}: {why}"))
                }
            }
        }
        Ok(brought)
    }

    /// Brings `target` to `newest`, the newest version of `key` on the other
    /// replicas. When no other replica holds the key, `missed` — the HLC of
    /// the operation the target missed — bounds what the target's record may
    /// still be removed for. `Err` means the target itself did not answer.
    pub(crate) async fn bring(
        &self,
        target: NodeId,
        key: KeyRef<'_>,
        newest: NewestVersion,
        missed: HlcTimestamp,
    ) -> Result<Outcome, ClusterError> {
        self.bring_from(target, key, newest, missed, &[]).await
    }

    /// [`Self::bring`], a confirmed copy taken from the replica `newest`
    /// names or, when that one cannot hand it over or its copy is refused as
    /// damaged, from the next of `holders` — replicas holding the same
    /// version.
    pub(crate) async fn bring_from(
        &self,
        target: NodeId,
        key: KeyRef<'_>,
        newest: NewestVersion,
        missed: HlcTimestamp,
        holders: &[NodeId],
    ) -> Result<Outcome, ClusterError> {
        match newest {
            NewestVersion::NotAReplica | NewestVersion::ErasureCoded { .. } => {
                Ok(Outcome::Superseded)
            }
            NewestVersion::Copy {
                node,
                confirmed: true,
                ..
            } => {
                let mut sources = vec![node];
                sources
                    .extend(holders.iter().filter(|holder| **holder != node && **holder != target));
                let mut outcome = Outcome::Later("no replica could hand its copy over".into());
                for source in sources {
                    outcome = match self.source.read_copy(key.bucket, key.key, source).await {
                        Ok(copy) => self.write_copy(target, key, copy).await?,
                        Err(e) => Outcome::Later(e.to_string()),
                    };
                    let damaged = matches!(&outcome, Outcome::Rejected(why)
                        if why.starts_with(crate::replica::CHECKSUM_MISMATCH));
                    if !damaged && !matches!(outcome, Outcome::Later(_)) {
                        break;
                    }
                }
                Ok(outcome)
            }
            NewestVersion::Copy {
                hlc,
                etag,
                confirmed: false,
                ..
            } => {
                let newest = VersionRank {
                    hlc: hlc.as_tuple(),
                    kind: VersionKind::Live,
                    etag,
                };
                self.settle_unconfirmed(target, key, newest).await
            }
            NewestVersion::Tombstone { hlc } => self.deliver_tombstone(target, key, hlc).await,
            NewestVersion::Absent => self.remove_stale(target, key, missed).await,
        }
    }

    /// The newest version is the tombstone of a delete at `deleted`. A target
    /// that keeps tombstones takes it, and its LWW keeps a write newer than
    /// the delete. An older one would delete whatever it holds: its record
    /// goes only if it is no newer than the delete, and only by its exact HLC.
    /// A record of the target reported with a made-up HLC newer than the
    /// delete is left alone, as on that path: the target's LWW does not weigh
    /// a record stored without an HLC against a tombstone. One no newer than
    /// the delete gives way to the tombstone, which stays to turn away an
    /// older copy arriving later. An erasure-coded record is left to the EC
    /// runtime.
    async fn deliver_tombstone(
        &self,
        target: NodeId,
        key: KeyRef<'_>,
        deleted: HlcTimestamp,
    ) -> Result<Outcome, ClusterError> {
        if self.protocol_of(target)? < TOMBSTONE_MIN_PROTOCOL_VERSION {
            return self.remove_stale(target, key, deleted).await;
        }
        let head = self.head(target, key).await?;
        if head.epoch_mismatch {
            return Ok(other_epoch());
        }
        let made_up_and_newer = head
            .hlc
            .as_ref()
            .map(HlcTimestamp::from_proto)
            .is_some_and(|held| held.is_made_up() && held > deleted);
        if head.exists && (made_up_and_newer || head.erasure_coded) {
            return self.remove_stale(target, key, deleted).await;
        }
        let request = proto::DeleteBlobRequest {
            placement_generation: 0,
            bucket: key.bucket.to_string(),
            key: key.key.to_string(),
            hlc: Some(deleted.to_proto()),
            coordinator_id: self.local_node_id.to_bytes().to_vec(),
            operation_id: uuid::Uuid::new_v4().as_u128().to_be_bytes().to_vec(),
            topology_epoch: self.topology_epoch,
            version_id: None,
            expected_source_hlc: None,
            bucket_metadata_stage: 0,
            repair_copy: true,
            iam_lease: None,
        };
        let response = self.send_delete(target, request).await?;
        Ok(delete_outcome(&response))
    }

    /// The newest copy is held by fewer replicas than a confirmed write
    /// leaves behind, while another, one without such a horizon, has no key:
    /// it may be a copy that missed a delete the others took, and spreading it
    /// would bring the deleted key back. Nothing is written. The target counts
    /// as current when it already holds that version or one ranked higher —
    /// its own tombstone included — and the key as ambiguous otherwise.
    async fn settle_unconfirmed(
        &self,
        target: NodeId,
        key: KeyRef<'_>,
        newest: VersionRank,
    ) -> Result<Outcome, ClusterError> {
        let head = self.head(target, key).await?;
        if head.epoch_mismatch {
            return Ok(other_epoch());
        }
        // A record without an HLC is reported with a made-up one, node 0.
        let held = rank_of_head(&head).filter(|held| held.hlc.2 != 0);
        Ok(match held {
            Some(held) if held >= newest => Outcome::Delivered,
            _ => Outcome::Ambiguous(
                "the newest copy is on fewer replicas than a confirmed write leaves, and another \
                 replica has no key: it may have missed a delete"
                    .into(),
            ),
        })
    }

    /// Writes `copy` to `target` as a repair writes one, with its own HLC and
    /// ETag, whatever the other replicas hold: for a caller that has already
    /// weighed the copy against them (the adoption of bucket configurations).
    pub(crate) async fn deliver_copy(
        &self,
        target: NodeId,
        key: KeyRef<'_>,
        copy: StoredCopy,
    ) -> Result<Outcome, ClusterError> {
        self.write_copy(target, key, copy).await
    }

    async fn write_copy(
        &self,
        target: NodeId,
        key: KeyRef<'_>,
        copy: StoredCopy,
    ) -> Result<Outcome, ClusterError> {
        let Some(hlc) = copy.hlc else {
            // The target's LWW weighs HLCs: a copy without one has no place
            // among the target's versions.
            return Ok(Outcome::Rejected("the current copy carries no HLC".into()));
        };
        if hlc.is_made_up() {
            // The target's LWW weighs only a record that has an HLC, and would
            // let this copy over the target's own record without one whatever
            // its rank. Ranked as a quorum read ranks them, a target holding
            // this version or one ranked higher is current.
            let head = self.head(target, key).await?;
            if head.epoch_mismatch {
                return Ok(other_epoch());
            }
            let copied = VersionRank::new(hlc.as_tuple(), VersionKind::Live, &copy.etag);
            if rank_of_head(&head).is_some_and(|held| held >= copied) {
                return Ok(Outcome::Delivered);
            }
        }
        let request = proto::WriteBlobRequest {
            verify_content: true,
            placement_generation: 0,
            bucket: key.bucket.to_string(),
            key: key.key.to_string(),
            data: copy.data,
            content_hash: copy.content_hash,
            content_type: copy.content_type,
            metadata: copy.metadata,
            etag: copy.etag,
            hlc: Some(hlc.to_proto()),
            version_id: copy.version_id,
            coordinator_id: self.local_node_id.to_bytes().to_vec(),
            operation_id: uuid::Uuid::new_v4().as_u128().to_be_bytes().to_vec(),
            topology_epoch: self.topology_epoch,
            bucket_metadata_stage: 0,
            repair_copy: true,
            iam_lease: None,
        };
        let bytes = request.data.len() as u64;
        let response = self.send_write(target, request).await?;
        if let Some(meter) = &self.meter {
            use std::sync::atomic::Ordering;
            if response.success {
                meter.bytes.fetch_add(bytes, Ordering::Relaxed);
            } else if response.error.starts_with(crate::replica::CHECKSUM_MISMATCH) {
                meter.checksum_refusals.fetch_add(1, Ordering::Relaxed);
            }
        }
        Ok(if response.success {
            Outcome::Delivered
        } else if response.epoch_mismatch {
            other_epoch()
        } else {
            Outcome::Rejected(response.error)
        })
    }

    /// The key is gone from the other replicas: the target's record goes too,
    /// if it is no newer than `bound` — the operation the target missed, or
    /// the tombstone the others hold.
    pub(crate) async fn remove_stale(
        &self,
        target: NodeId,
        key: KeyRef<'_>,
        bound: HlcTimestamp,
    ) -> Result<Outcome, ClusterError> {
        let head = self.head(target, key).await?;
        if head.epoch_mismatch {
            return Ok(other_epoch());
        }
        if !head.exists {
            return Ok(Outcome::Delivered);
        }
        if head.erasure_coded {
            return Ok(Outcome::Superseded);
        }
        let Some(generation) = head.hlc.as_ref().map(HlcTimestamp::from_proto) else {
            return Ok(Outcome::Rejected("the target reported no HLC".into()));
        };
        if generation.is_made_up() {
            return self.remove_made_up(target, key, generation, bound).await;
        }
        if generation > bound {
            // Written after the operation it missed: the target is current.
            return Ok(Outcome::Delivered);
        }
        self.remove_generation(target, key, generation, bound).await
    }

    /// The target's record is reported with a made-up HLC. Either it is a
    /// copy repaired from a record written without an HLC, which stores the
    /// made-up one and is removed by it like any other generation, or it
    /// stores none, and the zero HLC matches it — and only it, so a write that
    /// reaches the target meanwhile, carrying an HLC, is not touched. The
    /// replica answers a conditional delete the same way whether it matched or
    /// not: a second look tells whether the record is gone. A made-up HLC
    /// orders a record only roughly; one newer than the missed operation is
    /// left alone and not counted as current.
    pub(crate) async fn remove_made_up(
        &self,
        target: NodeId,
        key: KeyRef<'_>,
        generation: HlcTimestamp,
        bound: HlcTimestamp,
    ) -> Result<Outcome, ClusterError> {
        if generation > bound {
            return Ok(Outcome::Rejected(
                "the target's record has a made-up HLC newer than the operation it missed".into(),
            ));
        }
        for stored in [generation, HlcTimestamp::zero()] {
            match self.remove_generation(target, key, stored, bound).await? {
                Outcome::Delivered => {}
                other => return Ok(other),
            }
            let head = self.head(target, key).await?;
            if head.epoch_mismatch {
                return Ok(other_epoch());
            }
            let left =
                head.exists && head.hlc.as_ref().map(HlcTimestamp::from_proto) == Some(generation);
            if !left {
                return Ok(Outcome::Delivered);
            }
        }
        Ok(Outcome::Rejected(
            "the target's record without an HLC stayed after a delete matching it".into(),
        ))
    }

    /// Removes the target's record if it is exactly `generation`, by a delete
    /// issued at `issued`; a replica deletes by the generation alone.
    pub(crate) async fn remove_generation(
        &self,
        target: NodeId,
        key: KeyRef<'_>,
        generation: HlcTimestamp,
        issued: HlcTimestamp,
    ) -> Result<Outcome, ClusterError> {
        if !self.takes_conditional_deletes(target)? {
            return Ok(Outcome::Rejected(
                "the target is too old to delete one generation only".into(),
            ));
        }
        let request = proto::DeleteBlobRequest {
            placement_generation: 0,
            bucket: key.bucket.to_string(),
            key: key.key.to_string(),
            hlc: Some(issued.to_proto()),
            coordinator_id: self.local_node_id.to_bytes().to_vec(),
            operation_id: uuid::Uuid::new_v4().as_u128().to_be_bytes().to_vec(),
            topology_epoch: self.topology_epoch,
            version_id: None,
            expected_source_hlc: Some(generation.to_proto()),
            bucket_metadata_stage: 0,
            repair_copy: true,
            iam_lease: None,
        };
        let response = self.send_delete(target, request).await?;
        Ok(delete_outcome(&response))
    }

    pub(crate) async fn head(
        &self,
        target: NodeId,
        key: KeyRef<'_>,
    ) -> Result<proto::HeadObjectResponse, ClusterError> {
        let request = proto::HeadObjectRequest {
            bucket: key.bucket.to_string(),
            key: key.key.to_string(),
            version_id: None,
            topology_epoch: self.topology_epoch,
        };
        match self.local_of(target) {
            Some(local) => self.call(local.read.handle_head(&request)).await,
            None => self.call(self.rpc_client.send_head(target, request)).await,
        }
    }

    async fn send_write(
        &self,
        target: NodeId,
        request: proto::WriteBlobRequest,
    ) -> Result<proto::WriteBlobResponse, ClusterError> {
        match self.local_of(target) {
            Some(local) => self.call(local.write.handle_write(&request)).await,
            None => self.call(self.rpc_client.send_write(target, request)).await,
        }
    }

    async fn send_delete(
        &self,
        target: NodeId,
        request: proto::DeleteBlobRequest,
    ) -> Result<proto::DeleteBlobResponse, ClusterError> {
        match self.local_of(target) {
            Some(local) => self.call(local.write.handle_delete(&request)).await,
            None => self.call(self.rpc_client.send_delete(target, request)).await,
        }
    }

    /// This node's own replica, when `target` is this node and it can be
    /// reached without the network.
    fn local_of(&self, target: NodeId) -> Option<&LocalReplica> {
        self.local.as_ref().filter(|_| target == self.local_node_id)
    }

    async fn call<T>(
        &self,
        call: impl std::future::Future<Output = Result<T, ClusterError>>,
    ) -> Result<T, ClusterError> {
        tokio::time::timeout(self.call_timeout, call)
            .await
            .map_err(|_| ClusterError::WriteTimeout(self.call_timeout))?
    }
}

fn other_epoch() -> Outcome {
    Outcome::Rejected("the target is on another topology epoch".into())
}

fn delete_outcome(response: &proto::DeleteBlobResponse) -> Outcome {
    if response.success {
        Outcome::Delivered
    } else if response.epoch_mismatch {
        other_epoch()
    } else {
        Outcome::Rejected(response.error.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordinator::test_pool::{at, Replica, TestPool, BUCKET};
    use s4_core::{KeyVersion, StorageEngine};
    use std::collections::HashMap;

    /// The old control-state owners are 2 and 3; node 1 is a joining learner.
    struct OldOwners(crate::QuorumReadCoordinator);

    #[async_trait::async_trait]
    impl KeyStateSource for OldOwners {
        async fn newest_version(
            &self,
            bucket: &str,
            key: &str,
            target: NodeId,
        ) -> Result<NewestVersion, ClusterError> {
            self.0.newest_version(bucket, key, target).await
        }
        async fn key_versions(&self, bucket: &str, key: &str) -> Result<KeyVersions, ClusterError> {
            let mut versions = self.0.key_versions(bucket, key).await?;
            versions.replicas.retain(|n| *n != NodeId(1));
            versions.confirming_replicas.retain(|n| *n != NodeId(1));
            versions.answers.retain(|(n, _)| *n != NodeId(1));
            versions.unanswered.retain(|(n, _)| *n != NodeId(1));
            Ok(versions)
        }
        async fn read_copy(
            &self,
            bucket: &str,
            key: &str,
            node: NodeId,
        ) -> Result<StoredCopy, ClusterError> {
            self.0.read_copy(bucket, key, node).await
        }
    }

    fn learner(pool: &TestPool) -> Reconciler {
        Reconciler::new(
            Arc::new(OldOwners(pool.read_coordinator())),
            pool.rpc_client.clone(),
            pool.gossip.clone(),
            NodeId(1),
            pool.epoch(),
        )
        .with_local_replica(LocalReplica {
            write: Arc::new(ReplicaWriteHandler::new(
                pool.storage(1).clone(),
                Arc::new(crate::IdempotencyTracker::new()),
                pool.epoch(),
            )),
            read: Arc::new(ReplicaReadHandler::new(
                pool.storage(1).clone(),
                pool.epoch(),
            )),
        })
    }

    /// The confirmed version is the newest that as many replicas as a
    /// confirmed write hold or have gone past; one replica alone does not
    /// confirm anything, and replicas outside `counted` count for nothing.
    #[test]
    fn the_newest_confirmed_version_needs_a_write_quorum_behind_it() {
        let live = |wall: u64| ReplicaHolds::Version {
            hlc: HlcTimestamp::from_record(wall, 0, 7),
            tombstone: false,
            erasure_coded: false,
            etag: EtagRank::of(&format!("etag-{wall}")),
        };
        let deleted = |wall: u64| ReplicaHolds::Version {
            hlc: HlcTimestamp::from_record(wall, 0, 7),
            tombstone: true,
            erasure_coded: false,
            etag: EtagRank::NONE,
        };
        let counted = [NodeId(1), NodeId(2), NodeId(3)];
        let answers = vec![
            (NodeId(1), live(10)),
            (NodeId(2), live(20)),
            (NodeId(3), live(10)),
            (NodeId(4), live(30)),
            (NodeId(5), live(30)),
        ];
        let confirmed = newest_confirmed(&answers, &counted, 2).unwrap();
        assert_eq!(confirmed.hlc.0, 10);
        assert_eq!(newest_confirmed(&answers, &counted, 1).unwrap().hlc.0, 20);
        assert_eq!(newest_confirmed(&answers, &counted, 4), None);
        let passed = vec![
            (NodeId(1), live(10)),
            (NodeId(2), deleted(20)),
            (NodeId(3), live(30)),
        ];
        // Two replicas hold 20 or newer: the delete is confirmed.
        let confirmed = newest_confirmed(&passed, &counted, 2).unwrap();
        assert_eq!(
            (confirmed.hlc.0, confirmed.kind),
            (20, VersionKind::Tombstone)
        );
    }

    /// A replica behind the confirmed version is brought to it from a
    /// replica that holds exactly it, whatever newer copy a failed write
    /// left elsewhere; a replica already past it is left alone.
    #[tokio::test]
    async fn the_confirmed_version_is_brought_past_a_newer_failed_write() {
        let pool = TestPool::start(Replica::Up, Replica::Up, 29960).await;
        crate::coordinator::test_pool::put_with_hlc(pool.storage(2), "key", b"confirmed", at(10))
            .await;
        crate::coordinator::test_pool::put_with_hlc(pool.storage(3), "key", b"confirmed", at(10))
            .await;
        crate::coordinator::test_pool::put_with_hlc(pool.storage(2), "key", b"failed", at(20))
            .await;
        let reconciler = learner(&pool);
        let versions = pool.read_coordinator().key_versions(BUCKET, "key").await.unwrap();
        let confirmed =
            newest_confirmed(&versions.answers, &[NodeId(1), NodeId(2), NodeId(3)], 2).unwrap();
        assert_eq!(confirmed.hlc.0, at(10).wall_time);
        let key = KeyRef {
            bucket: BUCKET,
            key: "key",
        };
        let brought = reconciler
            .bring_confirmed(key, &versions, confirmed, &[NodeId(1), NodeId(2)])
            .await
            .unwrap();
        assert_eq!(brought, 1);
        assert_eq!(
            pool.storage(1).get_object(BUCKET, "key").await.unwrap().0,
            b"confirmed"
        );
        assert_eq!(
            pool.storage(2).get_object(BUCKET, "key").await.unwrap().0,
            b"failed"
        );
    }

    #[tokio::test]
    async fn a_control_state_learner_receives_live_records_and_tombstones_without_voting() {
        let pool = TestPool::start(Replica::Up, Replica::Up, 29940).await;
        let name = "__s4_iam_v2_user_alice";
        let key = KeyRef {
            bucket: "__system__",
            key: name,
        };
        for node in [2, 3] {
            pool.put_system(node, name, b"alice", at(10)).await;
        }
        let learner = learner(&pool);
        assert_eq!(
            learner.settle(key, &[NodeId(1), NodeId(2)], FOREVER).await,
            Settled::NotOurs
        );
        assert_eq!(
            learner.settle_local_learner(key, NodeId(2)).await,
            Settled::InStep
        );
        assert_eq!(
            pool.storage(1).get_object("__system__", name).await.unwrap().0,
            b"alice"
        );
        for node in [2, 3] {
            pool.delete_system(node, name, at(20)).await;
        }
        assert_eq!(
            learner.settle_local_learner(key, NodeId(3)).await,
            Settled::InStep
        );
        assert!(matches!(
            pool.storage(1).key_version("__system__", name).await.unwrap(),
            KeyVersion::Tombstone { .. }
        ));
    }

    #[tokio::test]
    async fn a_control_state_learner_waits_when_the_source_peer_or_quorum_is_missing() {
        let pool = TestPool::start(Replica::Up, Replica::Down, 29944).await;
        let name = "__s4_iam_v2_user_alice";
        pool.put_system(2, name, b"alice", at(10)).await;
        let key = KeyRef {
            bucket: "__system__",
            key: name,
        };
        assert!(matches!(
            learner(&pool).settle_local_learner(key, NodeId(3)).await,
            Settled::Failed(_)
        ));
        assert!(matches!(
            learner(&pool).settle_local_learner(key, NodeId(2)).await,
            Settled::Failed(_)
        ));
        assert!(pool.storage(1).get_object("__system__", name).await.is_err());
    }

    /// No tombstone is too old to carry.
    const FOREVER: Duration = Duration::MAX;

    fn reconciler(pool: &TestPool) -> Reconciler {
        pool.reconciler()
    }

    fn key(name: &str) -> KeyRef<'_> {
        KeyRef {
            bucket: BUCKET,
            key: name,
        }
    }

    /// What node `node` holds for `name`: the HLC wall time and bytes of a
    /// copy, or the wall time of a tombstone.
    #[derive(Debug, PartialEq, Eq)]
    enum Held {
        Copy(u64, Vec<u8>),
        Tombstone(u64),
        Nothing,
    }

    async fn held(pool: &TestPool, node: u128, name: &str) -> Held {
        let storage = pool.storage(node);
        match storage.key_version(BUCKET, name).await.unwrap() {
            KeyVersion::Live(record) => {
                let (data, _) = storage.get_object(BUCKET, name).await.unwrap();
                Held::Copy(record.hlc_timestamp, data)
            }
            KeyVersion::Tombstone(record) => Held::Tombstone(record.hlc_timestamp),
            KeyVersion::Absent => Held::Nothing,
        }
    }

    const PAIR: [NodeId; 2] = [NodeId(1), NodeId(2)];

    #[tokio::test]
    async fn the_newest_copy_reaches_every_replica_behind_it() {
        let pool = TestPool::start(Replica::Up, Replica::Up, 21100).await;
        // On the peer only; node 1 has nothing, node 3 an older copy.
        pool.put(2, "new", b"v2", at(20)).await;
        pool.put(3, "new", b"v1", at(10)).await;
        // On node 1 only, newer than node 2's tombstone.
        pool.put(1, "back", b"again", at(40)).await;
        pool.delete(2, "back", at(30)).await;

        let reconciler = reconciler(&pool);
        for name in ["new", "back"] {
            assert_eq!(
                reconciler.settle(key(name), &PAIR, FOREVER).await,
                Settled::InStep
            );
        }
        for node in 1..=3 {
            assert_eq!(
                held(&pool, node, "new").await,
                Held::Copy(20, b"v2".to_vec())
            );
            assert_eq!(
                held(&pool, node, "back").await,
                Held::Copy(40, b"again".to_vec())
            );
        }
    }

    /// Hands over the first copy it is asked for damaged — one byte changed,
    /// its SHA-256 kept — and every later one intact.
    struct DamagesFirstCopy {
        inner: crate::QuorumReadCoordinator,
        damaged: std::sync::atomic::AtomicBool,
    }

    #[async_trait::async_trait]
    impl KeyStateSource for DamagesFirstCopy {
        async fn newest_version(
            &self,
            bucket: &str,
            key: &str,
            target: NodeId,
        ) -> Result<NewestVersion, ClusterError> {
            self.inner.newest_version(bucket, key, target).await
        }
        async fn key_versions(&self, bucket: &str, key: &str) -> Result<KeyVersions, ClusterError> {
            self.inner.key_versions(bucket, key).await
        }
        async fn read_copy(
            &self,
            bucket: &str,
            key: &str,
            node: NodeId,
        ) -> Result<StoredCopy, ClusterError> {
            let mut copy = self.inner.read_copy(bucket, key, node).await?;
            if !self.damaged.swap(true, std::sync::atomic::Ordering::SeqCst) {
                copy.data[0] ^= 0xff;
            }
            Ok(copy)
        }
    }

    /// A copy damaged on its way is refused by the replica it is brought to,
    /// and the copy of another replica holding the same version is brought
    /// instead: the target ends with the version's bytes, never the damaged
    /// ones, and the refusal is counted.
    #[tokio::test]
    async fn a_damaged_copy_is_refused_and_taken_from_another_holder() {
        let pool = TestPool::start(Replica::Up, Replica::Up, 21132).await;
        pool.put(2, "copy", b"intact", at(20)).await;
        pool.put(3, "copy", b"intact", at(20)).await;
        let meter = Arc::new(CopyMeter::default());
        let reconciler = pool
            .reconciler_reading_source(Arc::new(DamagesFirstCopy {
                inner: pool.read_coordinator(),
                damaged: std::sync::atomic::AtomicBool::new(false),
            }))
            .with_copy_meter(meter.clone());
        assert_eq!(
            reconciler.settle(key("copy"), &PAIR, FOREVER).await,
            Settled::InStep
        );
        assert_eq!(
            held(&pool, 1, "copy").await,
            Held::Copy(20, b"intact".to_vec())
        );
        assert_eq!(meter.checksum_refusals(), 1);
        assert_eq!(meter.bytes(), b"intact".len() as u64);
    }

    /// A copy of an object completed by a multipart upload is written to a
    /// replica as one blob, and keeps the ETag of its version: the replicas
    /// agree on it and on the version digest.
    #[tokio::test]
    async fn a_multipart_object_reaches_a_replica_as_the_same_version() {
        let pool = TestPool::start(Replica::Up, Replica::Up, 21128).await;
        let etag = "\"9b2cf535f27731c974343645a3985328-2\"";
        let bytes = pool.complete_multipart(2, "parts", etag, at(20)).await;

        assert_eq!(
            reconciler(&pool).settle(key("parts"), &PAIR, FOREVER).await,
            Settled::InStep
        );
        let digest_of = |entries: Vec<s4_core::storage::VersionDigestEntry>| {
            entries
                .into_iter()
                .find(|entry| entry.key == "bucket/parts")
                .map(|entry| entry.digest)
        };
        let on_2 = digest_of(pool.storage(2).index_db().version_digests().await.unwrap());
        assert!(on_2.is_some());
        for node in [1, 3] {
            assert_eq!(
                held(&pool, node, "parts").await,
                Held::Copy(20, bytes.clone())
            );
            let record = pool.storage(node).head_object(BUCKET, "parts").await.unwrap();
            assert_eq!(record.etag, etag, "node {node}");
            let digests = pool.storage(node).index_db().version_digests().await.unwrap();
            assert_eq!(digest_of(digests), on_2, "node {node}");
        }
    }

    #[tokio::test]
    async fn the_newest_tombstone_reaches_every_replica_behind_it() {
        let pool = TestPool::start(Replica::Up, Replica::Up, 21104).await;
        // Deleted on the peer; node 1 kept the copy, node 3 never had it.
        pool.put(1, "gone", b"v1", at(10)).await;
        pool.delete(2, "gone", at(20)).await;
        // Deleted on node 1; the peer and node 3 kept older copies.
        pool.put(2, "mine", b"v1", at(10)).await;
        pool.put(3, "mine", b"v1", at(10)).await;
        pool.delete(1, "mine", at(30)).await;

        let reconciler = reconciler(&pool);
        for name in ["gone", "mine"] {
            assert_eq!(
                reconciler.settle(key(name), &PAIR, FOREVER).await,
                Settled::InStep
            );
        }
        for node in 1..=3 {
            assert_eq!(held(&pool, node, "gone").await, Held::Tombstone(20));
            assert_eq!(held(&pool, node, "mine").await, Held::Tombstone(30));
        }
    }

    #[tokio::test]
    async fn a_tombstone_past_gc_grace_is_not_carried_where_the_key_is_absent() {
        let pool = TestPool::start(Replica::Up, Replica::Up, 21108).await;
        let hour = 3_600_000;
        let now = now_millis();
        let old = HlcTimestamp::from_record(now - 2 * hour, 0, 7);
        let fresh = HlcTimestamp::from_record(now, 0, 7);
        // Node 1 still holds a copy older than the old tombstone.
        pool.put(
            1,
            "old",
            b"v1",
            HlcTimestamp::from_record(now - 3 * hour, 0, 7),
        )
        .await;
        pool.delete(2, "old", old).await;
        pool.delete(2, "fresh", fresh).await;

        let reconciler = reconciler(&pool);
        let grace = Duration::from_secs(3600);
        for name in ["old", "fresh"] {
            assert_eq!(
                reconciler.settle(key(name), &PAIR, grace).await,
                Settled::InStep
            );
        }
        // The stale copy goes; the absences stay absences.
        assert_eq!(held(&pool, 1, "old").await, Held::Tombstone(old.wall_time));
        assert_eq!(held(&pool, 3, "old").await, Held::Nothing);
        for node in [1, 3] {
            assert_eq!(
                held(&pool, node, "fresh").await,
                Held::Tombstone(fresh.wall_time)
            );
        }
    }

    #[tokio::test]
    async fn a_lone_copy_older_than_an_absent_replicas_horizon_is_ambiguous() {
        // Node 3 deleted keys before it kept tombstones, up to 50.
        let pool = TestPool::start_with_horizons(
            Replica::Up,
            Replica::Up,
            21112,
            [None, None, Some(at(50))],
        )
        .await;
        // Older than node 3's horizon, on one replica: it may have missed a
        // delete node 3 took. Nothing is written.
        pool.put(2, "lone", b"v", at(40)).await;
        // Newer than the horizon: node 3 would have kept that delete.
        pool.put(2, "newer", b"v", at(60)).await;
        // Older, but on two replicas: as many as a confirmed write leaves.
        pool.put(1, "pair", b"v", at(40)).await;
        pool.put(2, "pair", b"v", at(40)).await;

        let reconciler = reconciler(&pool);
        assert!(matches!(
            reconciler.settle(key("lone"), &PAIR, FOREVER).await,
            Settled::Ambiguous(_)
        ));
        for node in [1, 3] {
            assert_eq!(held(&pool, node, "lone").await, Held::Nothing);
        }
        for name in ["newer", "pair"] {
            assert_eq!(
                reconciler.settle(key(name), &PAIR, FOREVER).await,
                Settled::InStep
            );
        }
        assert_eq!(held(&pool, 3, "newer").await, Held::Copy(60, b"v".to_vec()));
        assert_eq!(held(&pool, 1, "newer").await, Held::Copy(60, b"v".to_vec()));
        assert_eq!(held(&pool, 3, "pair").await, Held::Copy(40, b"v".to_vec()));
    }

    #[tokio::test]
    async fn a_replica_outside_the_pair_that_does_not_answer_is_left_for_later() {
        let pool = TestPool::start(Replica::Up, Replica::Down, 21116).await;
        pool.put(2, "k", b"v", at(20)).await;

        let reconciler = reconciler(&pool);
        assert_eq!(
            reconciler.settle(key("k"), &PAIR, FOREVER).await,
            Settled::InStep
        );
        assert_eq!(held(&pool, 1, "k").await, Held::Copy(20, b"v".to_vec()));
        assert_eq!(held(&pool, 3, "k").await, Held::Nothing);

        // A pair member that does not answer fails the key.
        assert!(matches!(
            reconciler.settle(key("k"), &[NodeId(1), NodeId(3)], FOREVER).await,
            Settled::Failed(_)
        ));
    }

    #[tokio::test]
    async fn replicas_in_step_are_left_as_they_are() {
        let pool = TestPool::start(Replica::Up, Replica::Up, 21120).await;
        for node in 1..=3 {
            pool.put(node, "same", b"v", at(20)).await;
            pool.delete(node, "deleted", at(30)).await;
        }
        let before = pool.storage(2).index_db().version_digests().await.unwrap();

        let reconciler = reconciler(&pool);
        for name in ["same", "deleted", "never-written"] {
            assert_eq!(
                reconciler.settle(key(name), &PAIR, FOREVER).await,
                Settled::InStep
            );
        }
        assert_eq!(
            pool.storage(2).index_db().version_digests().await.unwrap(),
            before
        );
        assert_eq!(held(&pool, 1, "never-written").await, Held::Nothing);
    }

    #[tokio::test]
    async fn a_key_the_pair_does_not_replicate_is_not_settled() {
        let pool = TestPool::start(Replica::Up, Replica::Up, 21124).await;
        pool.put(2, "k", b"v", at(20)).await;
        let reconciler = reconciler(&pool);
        assert_eq!(
            reconciler.settle(key("k"), &[NodeId(1), NodeId(9)], FOREVER).await,
            Settled::NotOurs
        );
        assert_eq!(held(&pool, 1, "k").await, Held::Nothing);
    }

    /// A copy of 8 MiB or more goes to a remote replica as a stream, and
    /// keeps the ETag of its version there as a unary copy does: the stream's
    /// header carries it. Before, it took the ETag of its bytes, a version of
    /// another rank than its source's.
    #[tokio::test]
    async fn a_streamed_copy_keeps_the_etag_of_its_version() {
        let pool = TestPool::start(Replica::Up, Replica::Up, 21464).await;
        let etag = "9b2cf535f27731c974343645a3985328-2";
        let bytes: Vec<u8> =
            (0..crate::rpc::STREAMING_THRESHOLD + 1024).map(|i| (i % 251) as u8).collect();
        pool.put_with_etag(2, "large", &bytes, at(20), etag).await;

        assert_eq!(
            reconciler(&pool).settle(key("large"), &PAIR, FOREVER).await,
            Settled::InStep
        );
        let on_2 = etag_and_digest(&pool, 2, "large").await;
        assert_eq!(on_2.0, etag);
        for node in [1, 3] {
            assert_eq!(
                held(&pool, node, "large").await,
                Held::Copy(20, bytes.clone())
            );
            assert_eq!(
                etag_and_digest(&pool, node, "large").await,
                on_2,
                "node {node}"
            );
        }
    }

    /// The ETag node `node` holds for `name`, and its version digest.
    async fn etag_and_digest(
        pool: &TestPool,
        node: u128,
        name: &str,
    ) -> (String, Option<s4_core::storage::VersionDigest>) {
        let record = pool.storage(node).head_object(BUCKET, name).await.unwrap();
        let digest = pool
            .storage(node)
            .index_db()
            .version_digests()
            .await
            .unwrap()
            .into_iter()
            .find(|entry| entry.key == format!("{BUCKET}/{name}"))
            .map(|entry| entry.digest);
        (record.etag, digest)
    }

    /// Two copies of one HLC with different ETags — a copy stored without
    /// its source's ETag holds the MD5 of its bytes — are two versions, not
    /// one: the multipart ETag outranks it, and every replica takes it, from
    /// either side of the pair. Before, both were taken for the same version
    /// while their digests differed for ever.
    #[tokio::test]
    async fn copies_of_one_hlc_with_different_etags_settle_on_the_higher_rank() {
        let multipart = "9b2cf535f27731c974343645a3985328-2";
        for (holder, port) in [(1u128, 21170), (2, 21174)] {
            let pool = TestPool::start(Replica::Up, Replica::Up, port).await;
            for node in 1..=3 {
                if node == holder {
                    pool.put_with_etag(node, "k", b"same bytes", at(20), multipart).await;
                } else {
                    pool.put(node, "k", b"same bytes", at(20)).await;
                }
            }
            assert_eq!(
                reconciler(&pool).settle(key("k"), &PAIR, FOREVER).await,
                Settled::InStep
            );
            let expected = etag_and_digest(&pool, holder, "k").await;
            assert_eq!(expected.0, multipart);
            for node in 1..=3 {
                assert_eq!(
                    etag_and_digest(&pool, node, "k").await,
                    expected,
                    "node {node}, the multipart copy on node {holder}"
                );
            }
        }
    }

    /// Two different records written without an HLC in the same millisecond
    /// get the same made-up HLC. Ordered by their ETags, the replicas settle
    /// on one of them, and a record without an HLC and the copy repaired from
    /// it hold the same digest.
    #[tokio::test]
    async fn records_with_one_made_up_hlc_settle_on_one_of_them() {
        let pool = TestPool::start(Replica::Up, Replica::Up, 21178).await;
        let made_up = HlcTimestamp::from_record(1_700_000_000_000, 0, 0);
        pool.put_with_etag(1, "k", b"written on node 1", made_up, "").await;
        pool.put_with_etag(2, "k", b"written on node 2", made_up, "").await;
        let winner = [b"written on node 1".as_slice(), b"written on node 2"]
            .into_iter()
            .max_by_key(|data| crate::coordinator::test_pool::etag_of(data))
            .unwrap()
            .to_vec();

        let reconciler = reconciler(&pool);
        assert_eq!(
            reconciler.settle(key("k"), &PAIR, FOREVER).await,
            Settled::InStep
        );
        for node in 1..=3 {
            assert_eq!(
                held(&pool, node, "k").await,
                Held::Copy(made_up.wall_time, winner.clone()),
                "node {node}"
            );
        }
        assert_eq!(
            etag_and_digest(&pool, 1, "k").await,
            etag_and_digest(&pool, 2, "k").await
        );

        // A record stored without any HLC and its repaired copy are one
        // version: the digest records the made-up HLC for both.
        pool.storage(1)
            .put_object(
                BUCKET,
                "plain",
                b"single mode",
                "text/plain",
                &HashMap::new(),
            )
            .await
            .unwrap();
        assert_eq!(
            reconciler.settle(key("plain"), &PAIR, FOREVER).await,
            Settled::InStep
        );
        let original = etag_and_digest(&pool, 1, "plain").await;
        assert!(original.1.is_some());
        for node in [2, 3] {
            assert_eq!(
                etag_and_digest(&pool, node, "plain").await,
                original,
                "node {node}"
            );
        }
    }

    /// A replica whose record moved to erasure coding is left to the EC
    /// runtime, and the others behind the tombstone still take it, whichever
    /// of them answers first.
    #[tokio::test]
    async fn an_erasure_coded_replica_does_not_stop_the_others_from_taking_the_tombstone() {
        for (coded, live, port) in [(2u128, 3u128, 21182), (3, 2, 21186)] {
            let pool = TestPool::start(Replica::Up, Replica::Up, port).await;
            pool.put(coded, "k", b"v10", at(10)).await;
            assert!(pool
                .storage(coded)
                .detach_object_data_if_hlc(
                    BUCKET,
                    "k",
                    10,
                    0,
                    7,
                    s4_core::types::ObjectLayout::ErasureCoded {
                        erasure_set_id: 1,
                        topology_epoch: 1,
                    },
                )
                .await
                .unwrap());
            pool.put(live, "k", b"v5", at(5)).await;
            pool.delete(1, "k", at(20)).await;

            let settled = reconciler(&pool).settle(key("k"), &PAIR, FOREVER).await;
            assert_eq!(settled, Settled::NotOurs, "coded on node {coded}");
            assert_eq!(
                held(&pool, live, "k").await,
                Held::Tombstone(20),
                "coded on node {coded}"
            );
        }
    }

    /// A replica outside the pair that takes the call and never answers
    /// costs only its own answer: the pair is settled once the replica read
    /// timeout gives it up, though that is longer than the quorum timeout.
    #[tokio::test]
    async fn a_silent_replica_outside_the_pair_does_not_keep_the_pair_from_settling() {
        let pool = TestPool::start(Replica::Up, Replica::Silent, 21190).await;
        pool.put(1, "k", b"v", at(20)).await;
        let source = pool.read_coordinator_with(crate::coordinator::ReadTimeouts {
            quorum_timeout: Duration::from_millis(200),
            replica_read_timeout: Duration::from_millis(1500),
        });
        let reconciler = pool.reconciler_reading(source);

        assert_eq!(
            reconciler.settle(key("k"), &PAIR, FOREVER).await,
            Settled::InStep
        );
        assert_eq!(held(&pool, 2, "k").await, Held::Copy(20, b"v".to_vec()));
    }

    #[test]
    fn a_replica_is_behind_a_tombstone_of_its_own_version() {
        let copy = |hlc| ReplicaHolds::Version {
            hlc,
            tombstone: false,
            erasure_coded: false,
            etag: EtagRank::of("e"),
        };
        let tombstone = NewestVersion::Tombstone { hlc: at(20) };
        assert!(is_behind(copy(at(20)), tombstone));
        assert!(is_behind(copy(at(10)), tombstone));
        assert!(!is_behind(copy(at(30)), tombstone));
        assert!(is_behind(
            ReplicaHolds::Nothing { horizon: None },
            tombstone
        ));
        let newest = NewestVersion::Copy {
            hlc: at(20),
            etag: EtagRank::of("e"),
            node: NodeId(2),
            confirmed: true,
        };
        assert!(!is_behind(copy(at(20)), newest));
        assert!(is_behind(copy(at(19)), newest));
        assert!(!is_behind(
            copy(at(20)),
            NewestVersion::ErasureCoded { hlc: at(30) }
        ));
    }
}
