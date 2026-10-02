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

//! Hinted handoff for replicas that missed a write.
//!
//! When a write or a delete does not land on a replica — the replica refused
//! it, timed out, or gossip holds it dead — the coordinator notes it here: this
//! key on this replica may be behind, and the operation it missed had this HLC.
//! A hint carries no object data and replays no operation. Delivering it brings
//! the replica to the key's current state on the other replicas, the way read
//! repair does, so the hints of one key may be delivered in any order and more
//! than once. The one exception is the purge of a single generation's hot copy,
//! which is safe to repeat by itself and is kept as that operation.
//!
//! A replica that answers late but successfully gets no hint, and neither does
//! the local node.
//!
//! # Storage
//!
//! One append-only log per target under `{hints_dir}/{target}/`, split into
//! segments `{seq}.log`. A record is `[u32 length][u32 CRC32][bincode entry]`,
//! both numbers little-endian; a tail cut short by a crash is cut off at
//! startup. Stores that arrive together are written together with one fsync.
//! The per-target limit is checked against the length of the log, never by
//! walking a directory.
//!
//! A new segment starts once the open one spans a quarter of the TTL, so a
//! segment expires as a whole and is removed by unlinking it: a hint is kept at
//! most a quarter of the TTL longer than the TTL, plus one maintenance pass.
//!
//! Hints are best effort. One that is lost — the coordinator stopped before
//! writing it, the log was full, it expired — leaves the key to anti-entropy.
//!
//! # Delivery
//!
//! Every maintenance pass closes the open segment of each target gossip holds
//! alive and delivers its closed segments in order; new hints go to the next
//! segment meanwhile. For each hint the newest version of the key is read from
//! every replica except the target ([`KeyStateSource`]) and the target is
//! brought to it: the copy is written to it with its own HLC, so the target's
//! LWW keeps anything newer it already has; a key that no replica holds any
//! more is removed from the target only if the target's record is no newer
//! than the operation it missed, and only by that record's exact HLC.
//!
//! A record written without an HLC is ordered by the HLC every node makes up
//! for it, as a quorum read orders it ([`HlcTimestamp::of_record`]). Its copy
//! is written with that HLC, but only to a target that holds nothing newer: a
//! target's own record without an HLC is not weighed by its LWW. The target's
//! record without one is removed by a conditional delete on the zero HLC, which
//! matches only a record that still stores none.
//!
//! Without tombstones a replica that took a delete simply has no key, which
//! cannot be told from one that never got the write. So a copy is spread only
//! when that cannot matter — no other replica reported the key absent, or as
//! many of them hold the copy as a confirmed write leaves behind. Otherwise the
//! copy may be one that missed a delete the absent replicas took, and writing
//! it would bring the deleted key back: nothing is written, and the hint is
//! dropped as `ambiguous` unless the target already holds that version.
//!
//! A hint whose key cannot be settled now — another replica did not answer,
//! or the write it names may still be landing — waits for a later pass
//! without holding up the others. A segment is removed, and its hints counted,
//! once each of them is settled or carried over into a new segment; a pass
//! that delivered or dropped nothing in it, expiry aside, leaves it as it is.
//! Only a target that stops answering stops the pass over its hints. Repeating
//! any of this is safe, because delivery only ever brings the target to the
//! current state.
//!
//! One window stays open, the same one read repair has: a key deleted between
//! reading its current state and writing it to the target comes back on the
//! target. Closing it takes tombstones with an HLC on the replicas.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;
use tokio::sync::watch;
use tracing::{debug, error, info, warn};

use crate::clock::hlc::now_millis;
use crate::cluster_metrics::{
    record_hints_delivered, record_hints_dropped, record_hints_stored, set_hints_pending,
    HintDropReason,
};
use crate::error::ClusterError;
use crate::gossip::GossipHandle;
use crate::hlc::HlcTimestamp;
use crate::identity::{NodeId, NodeStatus};
use crate::protocol::CONDITIONAL_DELETE_MIN_PROTOCOL_VERSION;
use crate::rpc::proto;
use crate::rpc::NodeClient;

/// Default hint TTL: 3 hours.
const DEFAULT_HINT_TTL: Duration = Duration::from_secs(3 * 3600);

/// Default maximum size of the hint log of one target: 1 GiB.
const DEFAULT_MAX_HINTS_SIZE: u64 = 1024 * 1024 * 1024;

/// Default pause between two maintenance passes.
const DEFAULT_MAINTENANCE_INTERVAL: Duration = Duration::from_secs(30);

/// A segment this large is closed however young it is, so that no single file
/// grows to the whole per-target limit.
const MAX_SEGMENT_BYTES: u64 = 64 * 1024 * 1024;

/// The length and the CRC32 in front of every record.
const RECORD_HEADER_LEN: usize = 8;

/// A record longer than this is damage, not a hint: an entry holds a bucket
/// name and a key, which S3 bounds far below it. A hint this long is refused
/// when it is written ([`encode_record`]), so reading never meets one.
const MAX_RECORD_LEN: usize = 64 * 1024;

/// Extension of a log segment.
const SEGMENT_EXTENSION: &str = "log";

/// Extension of a hint file of the earlier format, one file per operation.
const LEGACY_EXTENSION: &str = "hint";

/// One hint: a key on the target replica that may be behind.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HintEntry {
    /// Bucket of the key.
    pub bucket: String,
    /// The key.
    pub key: String,
    /// HLC of the operation the target missed.
    pub hlc: HlcTimestamp,
    /// What delivering the hint has to do.
    pub kind: HintKind,
    /// When the hint was taken, in Unix milliseconds. The TTL counts from here.
    pub created_at: u64,
}

/// What delivering a hint has to do.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HintKind {
    /// A write the target missed: the key on the target is to be brought to
    /// its current state on the other replicas.
    Written,
    /// A delete the target missed. Brought to the current state like a write,
    /// with one difference: a copy on the other replicas older than the delete
    /// is one that missed it too, and the key counts as gone.
    Deleted,
    /// A purge of one generation's hot copy the target missed: the target's
    /// record is to be removed if it is exactly this generation, and kept
    /// otherwise.
    Purge {
        /// The generation to remove.
        expected: HlcTimestamp,
    },
}

impl HintEntry {
    /// A hint, taken now, for a write of `bucket/key` at `hlc`.
    pub fn written(bucket: &str, key: &str, hlc: HlcTimestamp) -> Self {
        Self::taken_now(bucket, key, hlc, HintKind::Written)
    }

    /// A hint, taken now, for a delete of `bucket/key` at `hlc`.
    pub fn deleted(bucket: &str, key: &str, hlc: HlcTimestamp) -> Self {
        Self::taken_now(bucket, key, hlc, HintKind::Deleted)
    }

    /// A hint, taken now, for the purge of generation `expected` of
    /// `bucket/key`, decided at `hlc`.
    pub fn purge(bucket: &str, key: &str, hlc: HlcTimestamp, expected: HlcTimestamp) -> Self {
        Self::taken_now(bucket, key, hlc, HintKind::Purge { expected })
    }

    fn taken_now(bucket: &str, key: &str, hlc: HlcTimestamp, kind: HintKind) -> Self {
        Self {
            bucket: bucket.to_string(),
            key: key.to_string(),
            hlc,
            kind,
            created_at: now_millis(),
        }
    }
}

/// The newest version of a key on its replicas other than a hint's target.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NewestVersion {
    /// `node` holds a copy of it.
    Copy {
        /// Its HLC.
        hlc: HlcTimestamp,
        /// A replica that holds it.
        node: NodeId,
        /// Whether the copy may be spread: no other replica reported the key
        /// absent, or at least as many of them hold exactly this version as
        /// the write quorum asks for (all of them, when the write quorum
        /// counts the target too). A copy that is neither may be one that
        /// missed a delete the absent replicas took.
        confirmed: bool,
    },
    /// It is erasure coded: no replica holds a copy of its bytes.
    ErasureCoded {
        /// Its HLC.
        hlc: HlcTimestamp,
    },
    /// None of the other replicas holds the key.
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
    /// SHA-256 of the bytes.
    pub content_hash: Vec<u8>,
    /// MIME type.
    pub content_type: String,
    /// Metadata as the replica stores it.
    pub metadata: HashMap<String, String>,
    /// Version ID, if any.
    pub version_id: Option<String>,
}

/// Where hint delivery learns the current state of a key.
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

    /// The copy of `bucket/key` that `node` holds.
    async fn read_copy(
        &self,
        bucket: &str,
        key: &str,
        node: NodeId,
    ) -> Result<StoredCopy, ClusterError>;
}

/// What delivering hints needs besides the log: where the current state of a
/// key is read, and how targets are reached.
pub struct HintDelivery {
    source: Arc<dyn KeyStateSource>,
    rpc_client: Arc<NodeClient>,
    gossip: Arc<GossipHandle>,
    local_node_id: NodeId,
    topology_epoch: u64,
    /// How long one call to a target may take before the pass gives up on it.
    call_timeout: Duration,
    /// How long after a hint the write it names may still be landing on the
    /// other replicas: the coordinator's `replica_write_timeout`.
    settle_time: Duration,
}

/// What became of one hint handed to its target.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Outcome {
    /// The target is at the key's current state now, or already was.
    Delivered,
    /// The key has left replication — erasure coded, or placed elsewhere.
    Superseded,
    /// The target refused, or cannot take, what delivery would send it.
    Rejected(String),
    /// The other replicas disagree in a way only tombstones could settle:
    /// the newest copy may be one that missed a delete. Nothing was written.
    Ambiguous(String),
    /// The key cannot be settled now; the hint waits for a later pass.
    Later(String),
}

impl HintDelivery {
    /// Delivery that reads current states from `source` and reaches targets
    /// through `rpc_client`, as node `local_node_id` on `topology_epoch`.
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
            topology_epoch,
            call_timeout: Duration::from_secs(60),
            settle_time: crate::WriteTimeouts::default().replica_write_timeout,
        }
    }

    /// The same delivery for coordinators whose replica writes may take up to
    /// `replica_write_timeout`: until then, a write a hint names may still be
    /// landing on the other replicas.
    pub fn with_settle_time(mut self, replica_write_timeout: Duration) -> Self {
        self.settle_time = replica_write_timeout;
        self
    }

    fn is_alive(&self, target: NodeId) -> bool {
        self.gossip
            .get_node(&target)
            .is_some_and(|state| state.status == NodeStatus::Alive)
    }

    /// Whether `target` understands a delete of one exact generation; a node
    /// that does not would take it for an unconditional delete. `Err` while
    /// the target has not announced its protocol yet — a node that has just
    /// come back is known by gossip a moment before its metadata arrives.
    fn takes_conditional_deletes(&self, target: NodeId) -> Result<bool, ClusterError> {
        match self.gossip.get_node(&target).map(|state| state.meta.protocol_version) {
            Some(0) | None => Err(ClusterError::Unavailable(format!(
                "{target} has not announced its protocol yet"
            ))),
            Some(version) => Ok(version >= CONDITIONAL_DELETE_MIN_PROTOCOL_VERSION),
        }
    }

    /// Brings `target` to the current state of `hint`'s key. `Err` means the
    /// target itself did not answer, and the pass over its hints stops; a key
    /// the other replicas cannot settle now is [`Outcome::Later`].
    async fn deliver(&self, target: NodeId, hint: &HintEntry) -> Result<Outcome, ClusterError> {
        if let HintKind::Purge { expected } = hint.kind {
            return self.remove_generation(target, hint, expected).await;
        }
        let newest = match self.source.newest_version(&hint.bucket, &hint.key, target).await {
            Ok(newest) => newest,
            // The replica that did not answer may hold the newest version.
            Err(e) => return Ok(Outcome::Later(e.to_string())),
        };
        let newest = match (hint.kind, newest) {
            // Older than the delete the target missed: a copy that missed it
            // too, and the key is gone.
            (
                HintKind::Deleted,
                NewestVersion::Copy { hlc, .. } | NewestVersion::ErasureCoded { hlc },
            ) if hlc < hint.hlc => NewestVersion::Absent,
            (_, newest) => newest,
        };
        if self.may_still_be_landing(hint, newest) {
            return Ok(Outcome::Later(
                "the write it names may still be landing on the other replicas".into(),
            ));
        }
        match newest {
            NewestVersion::NotAReplica | NewestVersion::ErasureCoded { .. } => {
                Ok(Outcome::Superseded)
            }
            NewestVersion::Copy {
                node,
                confirmed: true,
                ..
            } => match self.source.read_copy(&hint.bucket, &hint.key, node).await {
                Ok(copy) => self.write_copy(target, hint, copy).await,
                Err(e) => Ok(Outcome::Later(e.to_string())),
            },
            NewestVersion::Copy {
                hlc,
                confirmed: false,
                ..
            } => self.settle_unconfirmed(target, hint, hlc).await,
            NewestVersion::Absent => self.remove_stale(target, hint).await,
        }
    }

    /// Whether the write `hint` names may not have reached the other replicas
    /// yet. A replica that fails at once is hinted at once, while the write
    /// goes on to the others for up to the settle time; read before it lands,
    /// the key would look older than the write, and the target would be
    /// brought to a state that misses it.
    fn may_still_be_landing(&self, hint: &HintEntry, newest: NewestVersion) -> bool {
        if hint.kind != HintKind::Written {
            return false;
        }
        let older = match newest {
            NewestVersion::Absent => true,
            NewestVersion::Copy { hlc, .. } | NewestVersion::ErasureCoded { hlc } => hlc < hint.hlc,
            NewestVersion::NotAReplica => false,
        };
        let settle = u64::try_from(self.settle_time.as_millis()).unwrap_or(u64::MAX);
        older && now_millis() < hint.created_at.saturating_add(settle)
    }

    /// The newest copy is held by fewer of the other replicas than a
    /// confirmed write leaves behind, while another of them has no key: it
    /// may be a copy that missed a delete the others took, and spreading it
    /// would bring the deleted key back. Nothing is written. The hint counts
    /// as delivered when the target already holds that version or a newer
    /// one, and as ambiguous otherwise.
    async fn settle_unconfirmed(
        &self,
        target: NodeId,
        hint: &HintEntry,
        newest: HlcTimestamp,
    ) -> Result<Outcome, ClusterError> {
        let head = self.head(target, hint).await?;
        if head.epoch_mismatch {
            return Ok(Outcome::Rejected(
                "the target is on another topology epoch".into(),
            ));
        }
        let held = head
            .hlc
            .as_ref()
            .filter(|_| head.exists)
            .map(HlcTimestamp::from_proto)
            // A record without an HLC is reported with a made-up one.
            .filter(|held| !held.is_made_up());
        Ok(match held {
            Some(held) if held >= newest => Outcome::Delivered,
            _ => Outcome::Ambiguous(
                "the newest copy is on fewer replicas than a confirmed write leaves, and another \
                 replica has no key: it may have missed a delete"
                    .into(),
            ),
        })
    }

    async fn head(
        &self,
        target: NodeId,
        hint: &HintEntry,
    ) -> Result<proto::HeadObjectResponse, ClusterError> {
        let request = proto::HeadObjectRequest {
            bucket: hint.bucket.clone(),
            key: hint.key.clone(),
            version_id: None,
            topology_epoch: self.topology_epoch,
        };
        self.call(self.rpc_client.send_head(target, request)).await
    }

    async fn write_copy(
        &self,
        target: NodeId,
        hint: &HintEntry,
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
            // its age. Ordered as a quorum read orders them, a target holding
            // this version or a newer one is current.
            let head = self.head(target, hint).await?;
            if head.epoch_mismatch {
                return Ok(Outcome::Rejected(
                    "the target is on another topology epoch".into(),
                ));
            }
            let held = head.hlc.as_ref().filter(|_| head.exists).map(HlcTimestamp::from_proto);
            if held.is_some_and(|held| held >= hlc) {
                return Ok(Outcome::Delivered);
            }
        }
        let request = proto::WriteBlobRequest {
            bucket: hint.bucket.clone(),
            key: hint.key.clone(),
            data: copy.data,
            content_hash: copy.content_hash,
            content_type: copy.content_type,
            metadata: copy.metadata,
            hlc: Some(hlc.to_proto()),
            version_id: copy.version_id,
            coordinator_id: self.local_node_id.to_bytes().to_vec(),
            operation_id: uuid::Uuid::new_v4().as_u128().to_be_bytes().to_vec(),
            topology_epoch: self.topology_epoch,
        };
        let response = self.call(self.rpc_client.send_write(target, request)).await?;
        Ok(if response.success {
            Outcome::Delivered
        } else if response.epoch_mismatch {
            Outcome::Rejected("the target is on another topology epoch".into())
        } else {
            Outcome::Rejected(response.error)
        })
    }

    /// The key is gone from the other replicas: the target's record goes too,
    /// if it is no newer than the operation the target missed.
    async fn remove_stale(
        &self,
        target: NodeId,
        hint: &HintEntry,
    ) -> Result<Outcome, ClusterError> {
        let head = self.head(target, hint).await?;
        if head.epoch_mismatch {
            return Ok(Outcome::Rejected(
                "the target is on another topology epoch".into(),
            ));
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
            return self.remove_made_up(target, hint, generation).await;
        }
        if generation > hint.hlc {
            // Written after the operation it missed: the target is current.
            return Ok(Outcome::Delivered);
        }
        self.remove_generation(target, hint, generation).await
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
    async fn remove_made_up(
        &self,
        target: NodeId,
        hint: &HintEntry,
        generation: HlcTimestamp,
    ) -> Result<Outcome, ClusterError> {
        if generation > hint.hlc {
            return Ok(Outcome::Rejected(
                "the target's record has a made-up HLC newer than the operation it missed".into(),
            ));
        }
        for stored in [generation, HlcTimestamp::zero()] {
            match self.remove_generation(target, hint, stored).await? {
                Outcome::Delivered => {}
                other => return Ok(other),
            }
            let head = self.head(target, hint).await?;
            if head.epoch_mismatch {
                return Ok(Outcome::Rejected(
                    "the target is on another topology epoch".into(),
                ));
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

    /// Removes the target's record if it is exactly `generation`.
    async fn remove_generation(
        &self,
        target: NodeId,
        hint: &HintEntry,
        generation: HlcTimestamp,
    ) -> Result<Outcome, ClusterError> {
        if !self.takes_conditional_deletes(target)? {
            return Ok(Outcome::Rejected(
                "the target is too old to delete one generation only".into(),
            ));
        }
        let request = proto::DeleteBlobRequest {
            bucket: hint.bucket.clone(),
            key: hint.key.clone(),
            hlc: Some(hint.hlc.to_proto()),
            coordinator_id: self.local_node_id.to_bytes().to_vec(),
            operation_id: uuid::Uuid::new_v4().as_u128().to_be_bytes().to_vec(),
            topology_epoch: self.topology_epoch,
            version_id: None,
            expected_source_hlc: Some(generation.to_proto()),
        };
        let response = self.call(self.rpc_client.send_delete(target, request)).await?;
        Ok(if response.success {
            Outcome::Delivered
        } else if response.epoch_mismatch {
            Outcome::Rejected("the target is on another topology epoch".into())
        } else {
            Outcome::Rejected(response.error)
        })
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

/// Configuration for the hinted handoff subsystem.
#[derive(Debug, Clone)]
pub struct HintedHandoffConfig {
    /// Directory where hints are stored.
    pub hints_dir: PathBuf,
    /// How long a hint is kept (`S4_HINT_TTL_HOURS`, default: 3 hours). After
    /// that the key is left to anti-entropy. Zero keeps no hints at all.
    pub hint_ttl: Duration,
    /// Maximum size of the hint log of one target (default: 1 GiB).
    pub max_hints_size: u64,
    /// Pause between two maintenance passes (default: 30 s).
    pub maintenance_interval: Duration,
}

impl HintedHandoffConfig {
    /// Create a config with the given data directory.
    /// Hints will be stored in `{data_dir}/hints/`.
    pub fn new(data_dir: impl AsRef<Path>) -> Self {
        Self {
            hints_dir: data_dir.as_ref().join("hints"),
            hint_ttl: DEFAULT_HINT_TTL,
            max_hints_size: DEFAULT_MAX_HINTS_SIZE,
            maintenance_interval: DEFAULT_MAINTENANCE_INTERVAL,
        }
    }

    /// The same config with hints kept for `ttl`.
    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.hint_ttl = ttl;
        self
    }

    fn ttl_millis(&self) -> u64 {
        u64::try_from(self.hint_ttl.as_millis()).unwrap_or(u64::MAX)
    }

    /// How much time one segment may span: a quarter of the TTL.
    fn segment_span_millis(&self) -> u64 {
        (self.ttl_millis() / 4).max(1)
    }
}

/// Keeps the hints of this node for the replicas that missed a write.
pub struct HintedHandoffManager {
    config: HintedHandoffConfig,
    /// Names peers for the metrics.
    rpc_client: Arc<NodeClient>,
    /// Hints handed to [`store`](Self::store) and not yet written.
    queue: Mutex<Queue>,
    /// The ticket of the last hint a written batch took care of.
    written: AtomicU64,
    /// The logs on disk. Whoever holds this writes the whole queue.
    logs: tokio::sync::Mutex<HintLogs>,
    /// Batches written, which is fsyncs made, for the tests to count.
    #[cfg(test)]
    batches: AtomicU64,
}

/// Hints waiting for the writer, in the order they arrived.
#[derive(Default)]
struct Queue {
    /// Tickets handed out so far; the last one is this number.
    issued: u64,
    hints: Vec<(NodeId, HintEntry)>,
}

impl HintedHandoffManager {
    /// Create a new manager.
    ///
    /// What an earlier run left on disk is read here: each target's log, cut
    /// back to its last whole record, so `s4_hints_pending` starts from what is
    /// actually waiting. Hint files of the earlier format are removed: that
    /// format was never delivered and carried no HLC, and the keys it named are
    /// left to anti-entropy.
    pub fn new(config: HintedHandoffConfig, rpc_client: Arc<NodeClient>) -> Self {
        let (logs, legacy) = HintLogs::load(&config.hints_dir);
        if legacy > 0 {
            info!(
                removed = legacy,
                "removed hint files of the earlier format; the keys they named are left to anti-entropy"
            );
        }
        for (target, log) in &logs.targets {
            set_hints_pending(&rpc_client.peer_label(*target), log.pending());
        }
        Self {
            config,
            rpc_client,
            queue: Mutex::new(Queue::default()),
            written: AtomicU64::new(0),
            logs: tokio::sync::Mutex::new(logs),
            #[cfg(test)]
            batches: AtomicU64::new(0),
        }
    }

    /// Notes that `hint.key` on `target` may be behind.
    ///
    /// Returns once the hint is on disk, or once it is known not to be: the log
    /// of `target` was full, or writing failed. Either is counted or logged
    /// here — a lost hint leaves the key to anti-entropy, and no caller could
    /// do more about it.
    ///
    /// Hints stored at the same time are written together with one fsync.
    pub async fn store(&self, target: NodeId, hint: HintEntry) {
        if self.config.hint_ttl.is_zero() {
            // Kept for no time at all, it would expire as it is written.
            record_hints_dropped(HintDropReason::Expired, 1);
            return;
        }

        let ticket = {
            let mut queue = self.queue.lock().unwrap_or_else(|p| p.into_inner());
            queue.issued += 1;
            queue.hints.push((target, hint));
            queue.issued
        };

        let mut logs = self.logs.lock().await;
        if self.written.load(Ordering::Acquire) >= ticket {
            // A batch written while this store waited already held its hint.
            return;
        }
        let (batch, last) = {
            let mut queue = self.queue.lock().unwrap_or_else(|p| p.into_inner());
            (std::mem::take(&mut queue.hints), queue.issued)
        };

        let mut by_target: HashMap<NodeId, Vec<HintEntry>> = HashMap::new();
        for (target, hint) in batch {
            by_target.entry(target).or_default().push(hint);
        }
        for (target, hints) in by_target {
            let appended = logs.append(target, &hints, &self.config, Append::New).await;
            self.publish(target, &appended, &logs);
        }
        #[cfg(test)]
        self.batches.fetch_add(1, Ordering::Relaxed);
        self.written.store(last, Ordering::Release);
    }

    /// [`store`](Self::store) on a task of its own, for a caller that must not
    /// wait for the fsync.
    pub fn store_in_background(self: &Arc<Self>, target: NodeId, hint: HintEntry) {
        let this = Arc::clone(self);
        tokio::spawn(async move { this.store(target, hint).await });
    }

    fn publish(&self, target: NodeId, appended: &Appended, logs: &HintLogs) {
        let peer = self.rpc_client.peer_label(target);
        record_hints_stored(appended.stored);
        if appended.full > 0 {
            record_hints_dropped(HintDropReason::Full, appended.full);
            warn!(
                %peer,
                dropped = appended.full,
                limit = self.config.max_hints_size,
                "hint log for the peer is full, hints dropped"
            );
        }
        if let Some(e) = &appended.error {
            error!(
                %peer,
                lost = appended.lost,
                error = %e,
                "could not write hints; their keys are left to anti-entropy"
            );
        }
        set_hints_pending(&peer, logs.pending(target));
    }

    /// Removes every segment whose newest hint has outlived the TTL, counting
    /// its hints as dropped. A segment that could not be removed is logged and
    /// tried again on the next pass.
    pub async fn remove_expired(&self) {
        let now = now_millis();
        let ttl = self.config.ttl_millis();
        let mut logs = self.logs.lock().await;
        for (target, log) in logs.targets.iter_mut() {
            let expired: Vec<u64> = log
                .segments
                .iter()
                .filter(|(_, segment)| segment.newest.saturating_add(ttl) <= now)
                .map(|(seq, _)| *seq)
                .collect();
            if expired.is_empty() {
                continue;
            }
            let open_seq = log.open_seq();
            let mut dropped = 0;
            for seq in expired {
                if Some(seq) == open_seq {
                    log.open = None;
                }
                match tokio::fs::remove_file(log.segment_path(seq)).await {
                    Ok(()) => {}
                    Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                    Err(e) => {
                        warn!(
                            path = %log.segment_path(seq).display(),
                            error = %e,
                            "could not remove an expired hint segment"
                        );
                        continue;
                    }
                }
                if let Some(segment) = log.segments.remove(&seq) {
                    dropped += segment.entries;
                }
            }
            record_hints_dropped(HintDropReason::Expired, dropped);
            let peer = self.rpc_client.peer_label(*target);
            debug!(%peer, dropped, "expired hints removed");
            set_hints_pending(&peer, log.pending());
            if log.segments.is_empty() {
                // Only an empty directory goes; anything else in it stays.
                let _ = tokio::fs::remove_dir(&log.dir).await;
            }
        }
        logs.targets.retain(|_, log| !log.segments.is_empty());
    }

    /// Every `maintenance_interval`, starting at once, removes expired hints
    /// and, given a `delivery`, delivers the rest to the targets gossip holds
    /// alive — until `shutdown` turns `true` or its sender is dropped.
    pub async fn run_maintenance(
        self: Arc<Self>,
        delivery: Option<HintDelivery>,
        mut shutdown: watch::Receiver<bool>,
    ) {
        info!(
            ttl_secs = self.config.hint_ttl.as_secs(),
            delivers = delivery.is_some(),
            "hinted handoff maintenance started"
        );
        let mut tick = tokio::time::interval(self.config.maintenance_interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = tick.tick() => {
                    self.remove_expired().await;
                    if let Some(delivery) = &delivery {
                        self.deliver(delivery).await;
                    }
                }
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        break;
                    }
                }
            }
        }
        info!("hinted handoff maintenance stopped");
    }

    /// Delivers every closed segment of every target gossip holds alive,
    /// closing the open segments first so that new hints go to the next one.
    /// A target that stops answering is left for the next pass.
    pub async fn deliver(&self, delivery: &HintDelivery) {
        let targets: Vec<(NodeId, Vec<u64>)> = {
            let mut logs = self.logs.lock().await;
            logs.targets
                .iter_mut()
                .filter(|(target, _)| delivery.is_alive(**target))
                .map(|(target, log)| {
                    log.open = None;
                    (*target, log.segments.keys().copied().collect())
                })
                .collect()
        };
        for (target, segments) in targets {
            let mut delivered_keys = HashSet::new();
            for seq in segments {
                if let Err(e) =
                    self.deliver_segment(delivery, target, seq, &mut delivered_keys).await
                {
                    info!(
                        peer = %self.rpc_client.peer_label(target),
                        error = %e,
                        "hint delivery stopped; the rest is delivered on the next pass"
                    );
                    break;
                }
            }
        }
    }

    /// Delivers one closed segment and removes it.
    ///
    /// Nothing is counted until the segment is gone: its hints are settled
    /// one by one, and the ones that cannot be settled now are first carried
    /// over into a new segment, keeping their age, so that removing this one
    /// loses nothing. A segment in which nothing was delivered or dropped —
    /// expiry aside, which happens to its hints a few at a time — is left as
    /// it is rather than written out again on every pass while, say, another
    /// replica is down; its expired hints are counted when it goes. When the
    /// target stops answering, its hints not reached yet are carried over the
    /// same way and the error ends the pass over this target.
    async fn deliver_segment(
        &self,
        delivery: &HintDelivery,
        target: NodeId,
        seq: u64,
        delivered_keys: &mut HashSet<(String, String)>,
    ) -> Result<(), ClusterError> {
        let path = {
            let logs = self.logs.lock().await;
            match logs.targets.get(&target) {
                Some(log) if log.segments.contains_key(&seq) => log.segment_path(seq),
                // Removed since the pass began: expired.
                _ => return Ok(()),
            }
        };
        let bytes = match tokio::fs::read(&path).await {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == io::ErrorKind::NotFound => Vec::new(),
            Err(e) => return Err(e.into()),
        };
        let (hints, _) = read_records(&bytes);

        let now = now_millis();
        let ttl = self.config.ttl_millis();
        let peer = self.rpc_client.peer_label(target);
        let mut counts = SegmentCounts::default();
        let mut later: Vec<HintEntry> = Vec::new();
        let mut why_later = None;
        let mut stopped = None;
        for (index, hint) in hints.iter().enumerate() {
            if hint.created_at.saturating_add(ttl) <= now {
                counts.expired += 1;
                continue;
            }
            let key = (hint.bucket.clone(), hint.key.clone());
            let repeats_a_key =
                !matches!(hint.kind, HintKind::Purge { .. }) && delivered_keys.contains(&key);
            if repeats_a_key {
                // Already brought to a state newer than this hint's operation.
                counts.delivered += 1;
                continue;
            }
            let outcome = match delivery.deliver(target, hint).await {
                Ok(outcome) => outcome,
                Err(e) => {
                    stopped = Some((index, e));
                    break;
                }
            };
            match outcome {
                Outcome::Delivered => {
                    counts.delivered += 1;
                    if delivered_keys.len() >= MAX_KEYS_REMEMBERED {
                        delivered_keys.clear();
                    }
                    delivered_keys.insert(key);
                }
                Outcome::Superseded => counts.superseded += 1,
                Outcome::Rejected(reason) => {
                    counts.rejected += 1;
                    warn!(
                        %peer,
                        bucket = %hint.bucket,
                        key = %hint.key,
                        %reason,
                        "hint not delivered: the target cannot take it; its key is left to anti-entropy"
                    );
                }
                Outcome::Ambiguous(reason) => {
                    counts.ambiguous += 1;
                    warn!(
                        %peer,
                        bucket = %hint.bucket,
                        key = %hint.key,
                        %reason,
                        "hint not delivered: the replicas disagree about the key; it is left to read repair and anti-entropy"
                    );
                }
                Outcome::Later(reason) => {
                    why_later.get_or_insert(reason);
                    later.push(hint.clone());
                }
            }
        }
        if let Some((index, _)) = &stopped {
            later.extend_from_slice(&hints[*index..]);
        }
        if counts.progress() == 0 && !later.is_empty() {
            debug!(
                %peer,
                waiting = later.len(),
                reason = why_later.as_deref().unwrap_or("the target stopped answering"),
                "no hint of the segment could be settled; it waits for the next pass as it is"
            );
            return stopped.map_or(Ok(()), |(_, e)| Err(e));
        }

        let mut logs = self.logs.lock().await;
        if !later.is_empty() {
            let carried = logs.append(target, &later, &self.config, Append::CarriedOver).await;
            if let Some(e) = carried.error {
                // The segment stays whole, so nothing is lost; what did reach
                // the new segment is delivered twice, which is harmless, and
                // the counts are off by as much. So it is after a carry-over
                // whose old segment could not be removed below.
                set_hints_pending(&peer, logs.pending(target));
                return Err(e.into());
            }
            debug!(
                %peer,
                waiting = later.len(),
                reason = why_later.as_deref().unwrap_or("the target stopped answering"),
                "hints that cannot be settled now carried over to the next pass"
            );
        }
        match tokio::fs::remove_file(&path).await {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        record_hints_delivered(counts.delivered);
        record_hints_dropped(HintDropReason::Expired, counts.expired);
        record_hints_dropped(HintDropReason::Superseded, counts.superseded);
        record_hints_dropped(HintDropReason::Rejected, counts.rejected);
        record_hints_dropped(HintDropReason::Ambiguous, counts.ambiguous);
        if let Some(log) = logs.targets.get_mut(&target) {
            log.segments.remove(&seq);
            set_hints_pending(&peer, log.pending());
            if log.segments.is_empty() {
                let _ = tokio::fs::remove_dir(&log.dir).await;
                logs.targets.remove(&target);
            }
        }
        if counts.delivered > 0 {
            debug!(%peer, delivered = counts.delivered, "hints delivered");
        }
        stopped.map_or(Ok(()), |(_, e)| Err(e))
    }

    /// Hints waiting on disk for `target`.
    #[cfg(test)]
    pub(crate) async fn pending(&self, target: NodeId) -> u64 {
        self.logs.lock().await.pending(target)
    }

    /// The hints on disk for `target`, oldest segment first.
    #[cfg(test)]
    pub(crate) async fn hints_for(&self, target: NodeId) -> Vec<HintEntry> {
        let logs = self.logs.lock().await;
        let Some(log) = logs.targets.get(&target) else {
            return Vec::new();
        };
        log.segments
            .keys()
            .filter_map(|seq| std::fs::read(log.segment_path(*seq)).ok())
            .flat_map(|bytes| read_records(&bytes).0)
            .collect()
    }
}

/// How many keys one pass remembers as delivered, so that later hints for the
/// same key cost nothing. Forgetting only costs a repeated delivery.
const MAX_KEYS_REMEMBERED: usize = 100_000;

/// What became of the hints of one delivered segment.
#[derive(Default)]
struct SegmentCounts {
    delivered: u64,
    expired: u64,
    superseded: u64,
    rejected: u64,
    ambiguous: u64,
}

impl SegmentCounts {
    /// Hints delivered or dropped for a reason other than their age: what
    /// makes writing the rest of a segment out again worth it.
    fn progress(&self) -> u64 {
        self.delivered + self.superseded + self.rejected + self.ambiguous
    }
}

/// Where a batch of hints is appended to a target's log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Append {
    /// Hints just stored: into the open segment while it is young enough, and
    /// only as far as the per-target limit allows.
    New,
    /// Hints a delivery could not settle, moved out of a segment about to be
    /// removed. They were within the limit already, so it is not applied
    /// again, and they start a segment of their own: one that expires by
    /// their age, not by that of the hints stored after them.
    CarriedOver,
}

/// What became of one batch of hints for one target.
#[derive(Default)]
struct Appended {
    /// Hints now on disk.
    stored: u64,
    /// Hints the log had no room for.
    full: u64,
    /// Hints lost to `error`.
    lost: u64,
    error: Option<io::Error>,
}

/// The hint logs of every target.
struct HintLogs {
    dir: PathBuf,
    targets: HashMap<NodeId, TargetLog>,
}

/// The hint log of one target: its segments, oldest first.
struct TargetLog {
    dir: PathBuf,
    segments: BTreeMap<u64, Segment>,
    /// The last segment, while appends may still go to it.
    open: Option<tokio::fs::File>,
}

/// What one segment holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Segment {
    entries: u64,
    bytes: u64,
    /// `created_at` of its oldest hint.
    oldest: u64,
    /// `created_at` of its newest hint.
    newest: u64,
}

impl Segment {
    fn empty() -> Self {
        Self {
            entries: 0,
            bytes: 0,
            oldest: u64::MAX,
            newest: 0,
        }
    }

    fn add(&mut self, created_at: u64, bytes: u64) {
        self.entries += 1;
        self.bytes += bytes;
        self.oldest = self.oldest.min(created_at);
        self.newest = self.newest.max(created_at);
    }

    fn merge(&mut self, other: &Segment) {
        self.entries += other.entries;
        self.bytes += other.bytes;
        self.oldest = self.oldest.min(other.oldest);
        self.newest = self.newest.max(other.newest);
    }

    /// The segment of `entries`, read from `bytes` bytes of a file.
    fn of(entries: &[HintEntry], bytes: u64) -> Self {
        let mut segment = Self::empty();
        for entry in entries {
            segment.add(entry.created_at, 0);
        }
        segment.bytes = bytes;
        segment
    }
}

impl HintLogs {
    /// Reads what an earlier run left under `dir`: each target's segments, cut
    /// back to their last whole record. Hint files of the earlier format are
    /// removed; their number is returned.
    fn load(dir: &Path) -> (Self, u64) {
        let mut logs = Self {
            dir: dir.to_path_buf(),
            targets: HashMap::new(),
        };
        let mut legacy = 0u64;
        let Ok(target_dirs) = std::fs::read_dir(dir) else {
            return (logs, legacy);
        };
        for target_dir in target_dirs.flatten() {
            let Some(target) =
                target_dir.file_name().to_str().and_then(|name| name.parse::<u128>().ok())
            else {
                continue;
            };
            let mut log = TargetLog::new(target_dir.path());
            let Ok(files) = std::fs::read_dir(target_dir.path()) else {
                continue;
            };
            for file in files.flatten() {
                let path = file.path();
                match path.extension().and_then(|e| e.to_str()) {
                    Some(LEGACY_EXTENSION) => {
                        if std::fs::remove_file(&path).is_ok() {
                            legacy += 1;
                        }
                    }
                    Some(SEGMENT_EXTENSION) => {
                        let Some(seq) =
                            path.file_stem().and_then(|s| s.to_str()).and_then(|s| s.parse().ok())
                        else {
                            continue;
                        };
                        if let Some(segment) = load_segment(&path) {
                            log.segments.insert(seq, segment);
                        }
                    }
                    _ => {}
                }
            }
            if log.segments.is_empty() {
                let _ = std::fs::remove_dir(target_dir.path());
            } else {
                logs.targets.insert(NodeId(target), log);
            }
        }
        (logs, legacy)
    }

    fn pending(&self, target: NodeId) -> u64 {
        self.targets.get(&target).map_or(0, TargetLog::pending)
    }

    /// Appends `hints` to the log of `target` with one write and one fsync.
    async fn append(
        &mut self,
        target: NodeId,
        hints: &[HintEntry],
        config: &HintedHandoffConfig,
        mode: Append,
    ) -> Appended {
        let log = self
            .targets
            .entry(target)
            .or_insert_with(|| TargetLog::new(self.dir.join(target.0.to_string())));
        let mut appended = Appended::default();
        let Some(first) = hints.iter().map(|hint| hint.created_at).min() else {
            return appended;
        };

        if mode == Append::CarriedOver || log.wants_new_segment(first, config.segment_span_millis())
        {
            if let Err(e) = log.start_segment().await {
                appended.lost = hints.len() as u64;
                appended.error = Some(e);
                return appended;
            }
        }

        let mut buf = Vec::new();
        let mut added = Segment::empty();
        for hint in hints {
            let record = match encode_record(hint) {
                Ok(record) => record,
                Err(e) => {
                    appended.lost += 1;
                    appended.error = Some(e);
                    continue;
                }
            };
            let len = record.len() as u64;
            if mode == Append::New && log.bytes() + added.bytes + len > config.max_hints_size {
                appended.full += 1;
                continue;
            }
            added.add(hint.created_at, len);
            buf.extend_from_slice(&record);
        }
        if buf.is_empty() {
            return appended;
        }

        let Some(seq) = log.open_seq() else {
            appended.lost += added.entries;
            appended.error = Some(io::Error::other("no open hint segment"));
            return appended;
        };
        let written = match log.open.as_mut() {
            Some(file) => write_and_sync(file, &buf).await,
            None => Err(io::Error::other("no open hint segment")),
        };
        match written {
            Ok(()) => {
                if let Some(segment) = log.segments.get_mut(&seq) {
                    segment.merge(&added);
                }
                appended.stored = added.entries;
            }
            Err(e) => {
                // Part of the batch may have reached the file: the file says
                // what did, once a torn record is cut off its end.
                log.open = None;
                let path = log.segment_path(seq);
                let before = log.segments.get(&seq).map_or(0, |segment| segment.entries);
                let after =
                    tokio::task::spawn_blocking(move || load_segment(&path)).await.ok().flatten();
                let now_on_disk = after.map_or(0, |segment| segment.entries);
                match after {
                    Some(segment) => {
                        log.segments.insert(seq, segment);
                    }
                    None => {
                        log.segments.remove(&seq);
                    }
                }
                appended.stored = now_on_disk.saturating_sub(before);
                appended.lost += added.entries.saturating_sub(appended.stored);
                appended.error = Some(e);
            }
        }
        appended
    }
}

impl TargetLog {
    fn new(dir: PathBuf) -> Self {
        Self {
            dir,
            segments: BTreeMap::new(),
            open: None,
        }
    }

    fn pending(&self) -> u64 {
        self.segments.values().map(|segment| segment.entries).sum()
    }

    fn bytes(&self) -> u64 {
        self.segments.values().map(|segment| segment.bytes).sum()
    }

    fn segment_path(&self, seq: u64) -> PathBuf {
        self.dir.join(format!("{seq:020}.{SEGMENT_EXTENSION}"))
    }

    /// The segment appends go to, if one is open.
    fn open_seq(&self) -> Option<u64> {
        self.open.as_ref()?;
        self.segments.last_key_value().map(|(seq, _)| *seq)
    }

    /// Whether a hint taken at `created_at` has to start a new segment: none is
    /// open, or the open one is full or already spans `span` milliseconds.
    fn wants_new_segment(&self, created_at: u64, span: u64) -> bool {
        match (self.open_seq(), self.segments.last_key_value()) {
            (Some(_), Some((_, segment))) => {
                segment.bytes >= MAX_SEGMENT_BYTES
                    || created_at >= segment.oldest.saturating_add(span)
            }
            _ => true,
        }
    }

    async fn start_segment(&mut self) -> io::Result<()> {
        self.open = None;
        tokio::fs::create_dir_all(&self.dir).await?;
        let seq = self.segments.last_key_value().map_or(1, |(seq, _)| seq + 1);
        let file = tokio::fs::OpenOptions::new()
            .create_new(true)
            .append(true)
            .open(self.segment_path(seq))
            .await?;
        sync_dir(&self.dir).await;
        self.segments.insert(seq, Segment::empty());
        self.open = Some(file);
        Ok(())
    }
}

/// One hint as a record of the log. A hint longer than a record may be is
/// refused here: written, it would stop every reading of the segment at it
/// and hide the hints behind it.
fn encode_record(hint: &HintEntry) -> io::Result<Vec<u8>> {
    let payload = bincode::serialize(hint).map_err(io::Error::other)?;
    if payload.len() > MAX_RECORD_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "a hint for a {}-byte key is longer than a hint record may be ({MAX_RECORD_LEN} bytes)",
                hint.key.len()
            ),
        ));
    }
    let len = u32::try_from(payload.len()).map_err(io::Error::other)?;
    let mut record = Vec::with_capacity(RECORD_HEADER_LEN + payload.len());
    record.extend_from_slice(&len.to_le_bytes());
    record.extend_from_slice(&crc32fast::hash(&payload).to_le_bytes());
    record.extend_from_slice(&payload);
    Ok(record)
}

/// The hints of a segment's bytes, and how many bytes they fill. Reading stops
/// at the first record that is cut short, too long, fails its CRC or does not
/// decode: what follows it is not a hint.
fn read_records(bytes: &[u8]) -> (Vec<HintEntry>, usize) {
    let mut entries = Vec::new();
    let mut pos = 0;
    while let Some(header) = bytes.get(pos..pos + RECORD_HEADER_LEN) {
        let len = u32::from_le_bytes([header[0], header[1], header[2], header[3]]) as usize;
        let crc = u32::from_le_bytes([header[4], header[5], header[6], header[7]]);
        if len == 0 || len > MAX_RECORD_LEN {
            break;
        }
        let start = pos + RECORD_HEADER_LEN;
        let Some(payload) = bytes.get(start..start + len) else {
            break;
        };
        if crc32fast::hash(payload) != crc {
            break;
        }
        let Ok(entry) = bincode::deserialize::<HintEntry>(payload) else {
            break;
        };
        entries.push(entry);
        pos = start + len;
    }
    (entries, pos)
}

/// Reads one segment file, cutting it back to its last whole record. A file
/// with no hint left in it is removed, and `None` returned.
fn load_segment(path: &Path) -> Option<Segment> {
    let bytes = std::fs::read(path).ok()?;
    let (entries, valid) = read_records(&bytes);
    if valid < bytes.len() {
        warn!(
            path = %path.display(),
            kept = entries.len(),
            cut_bytes = bytes.len() - valid,
            "hint segment ends in a damaged record; cut back to the last whole one"
        );
        let cut = std::fs::OpenOptions::new()
            .write(true)
            .open(path)
            .and_then(|file| file.set_len(valid as u64).and_then(|()| file.sync_all()));
        if let Err(e) = cut {
            warn!(path = %path.display(), error = %e, "could not cut the damaged hint segment");
        }
    }
    if entries.is_empty() {
        let _ = std::fs::remove_file(path);
        return None;
    }
    Some(Segment::of(&entries, valid as u64))
}

async fn write_and_sync(file: &mut tokio::fs::File, buf: &[u8]) -> io::Result<()> {
    file.write_all(buf).await?;
    file.sync_data().await
}

/// Makes a new file's name durable. Best effort: not every platform can open a
/// directory, and a hint is not worth failing over.
async fn sync_dir(dir: &Path) {
    if let Ok(dir) = tokio::fs::File::open(dir).await {
        let _ = dir.sync_all().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster_metrics::PeerNames;
    use crate::gossip::GossipHandle;
    use crate::test_metrics::TestMetrics;

    const PENDING: &str = "s4_hints_pending";
    const STORED: &str = "s4_hints_stored_total";
    const DROPPED: &str = "s4_hints_dropped_total";

    fn config(dir: &Path) -> HintedHandoffConfig {
        HintedHandoffConfig::new(dir)
    }

    /// A manager over `config` whose targets 42 and 43 are named for metrics.
    fn manager(config: HintedHandoffConfig) -> HintedHandoffManager {
        let names = Arc::new(PeerNames::default());
        names.set([
            (NodeId(42), "node-42".to_string()),
            (NodeId(43), "node-43".to_string()),
        ]);
        let gossip = Arc::new(GossipHandle::new_for_testing(HashMap::new()));
        let rpc_client = Arc::new(
            NodeClient::new(gossip, crate::rpc::NodeClientConfig::default()).with_peer_names(names),
        );
        HintedHandoffManager::new(config, rpc_client)
    }

    fn hlc(wall: u64) -> HlcTimestamp {
        HlcTimestamp::from_record(wall, 0, 1)
    }

    fn hint(key: &str) -> HintEntry {
        HintEntry::written("bucket", key, hlc(10))
    }

    /// Every hint the log of `target` holds, oldest segment first.
    fn read_log(config: &HintedHandoffConfig, target: u128) -> Vec<HintEntry> {
        let dir = config.hints_dir.join(target.to_string());
        let mut paths: Vec<_> = std::fs::read_dir(dir)
            .map(|files| files.flatten().map(|file| file.path()).collect())
            .unwrap_or_default();
        paths.sort();
        paths
            .iter()
            .filter(|path| path.extension().and_then(|e| e.to_str()) == Some(SEGMENT_EXTENSION))
            .flat_map(|path| read_records(&std::fs::read(path).unwrap()).0)
            .collect()
    }

    fn segment_files(config: &HintedHandoffConfig, target: u128) -> Vec<PathBuf> {
        let mut paths: Vec<_> = std::fs::read_dir(config.hints_dir.join(target.to_string()))
            .map(|files| files.flatten().map(|file| file.path()).collect())
            .unwrap_or_default();
        paths.sort();
        paths
    }

    #[tokio::test]
    async fn hints_are_read_back_as_they_were_written() {
        let tmp = tempfile::tempdir().unwrap();
        let config = config(tmp.path());
        let manager = manager(config.clone());
        let purge = HintEntry::purge("bucket", "cold", hlc(30), hlc(20));

        manager.store(NodeId(42), hint("a")).await;
        manager.store(NodeId(42), purge.clone()).await;
        manager.store(NodeId(43), hint("b")).await;

        let for_42 = read_log(&config, 42);
        assert_eq!(for_42.len(), 2);
        assert_eq!(for_42[0].key, "a");
        assert_eq!(for_42[0].kind, HintKind::Written);
        assert_eq!(for_42[1], purge);
        assert_eq!(read_log(&config, 43).len(), 1);
        assert_eq!(manager.pending(NodeId(42)).await, 2);
    }

    #[tokio::test]
    async fn pending_hints_are_counted_from_the_log_after_a_restart() {
        let tmp = tempfile::tempdir().unwrap();
        {
            let metrics = TestMetrics::install();
            let manager = manager(config(tmp.path()));
            manager.store(NodeId(42), hint("a")).await;
            manager.store(NodeId(42), hint("b")).await;
            manager.store(NodeId(43), hint("c")).await;

            assert_eq!(metrics.value(STORED, &[]), Some(3.0));
            assert_eq!(metrics.value(PENDING, &[("peer", "node-42")]), Some(2.0));
            assert_eq!(metrics.value(PENDING, &[("peer", "node-43")]), Some(1.0));
        }

        // A recorder that saw no hint being stored: the count comes from disk.
        let metrics = TestMetrics::install();
        let restarted = manager(config(tmp.path()));
        assert_eq!(metrics.value(PENDING, &[("peer", "node-42")]), Some(2.0));
        assert_eq!(metrics.value(PENDING, &[("peer", "node-43")]), Some(1.0));
        assert_eq!(metrics.value(STORED, &[]), None);

        // And the restarted log takes new hints after the old ones.
        restarted.store(NodeId(42), hint("d")).await;
        let keys: Vec<_> = read_log(&config(tmp.path()), 42).into_iter().map(|h| h.key).collect();
        assert_eq!(keys, ["a", "b", "d"]);
    }

    /// A crash can leave half a record at the end of a segment, and a damaged
    /// disk a record that fails its checksum. Neither is a hint, and neither
    /// may hide the whole records in front of it or the ones written later.
    #[tokio::test]
    async fn a_damaged_tail_is_cut_back_to_the_last_whole_record() {
        let tmp = tempfile::tempdir().unwrap();
        let config = config(tmp.path());
        {
            let manager = manager(config.clone());
            manager.store(NodeId(42), hint("a")).await;
            manager.store(NodeId(42), hint("b")).await;
        }
        let segment = segment_files(&config, 42).pop().unwrap();
        let whole = std::fs::metadata(&segment).unwrap().len();

        // A record whose checksum is wrong, then half of a header.
        let mut damaged = encode_record(&hint("c")).unwrap();
        damaged[4] ^= 0xFF;
        damaged.extend_from_slice(&[9, 0, 0]);
        {
            use std::io::Write;
            let mut file = std::fs::OpenOptions::new().append(true).open(&segment).unwrap();
            file.write_all(&damaged).unwrap();
        }

        let metrics = TestMetrics::install();
        let restarted = manager(config.clone());
        assert_eq!(metrics.value(PENDING, &[("peer", "node-42")]), Some(2.0));
        assert_eq!(std::fs::metadata(&segment).unwrap().len(), whole);

        restarted.store(NodeId(42), hint("d")).await;
        let keys: Vec<_> = read_log(&config, 42).into_iter().map(|h| h.key).collect();
        assert_eq!(keys, ["a", "b", "d"]);
    }

    /// The limit is the length of the log, and nothing else in the directory
    /// counts towards it: a stray file as large as the limit changes nothing.
    #[tokio::test]
    async fn a_full_log_drops_the_hint_and_the_limit_counts_only_the_log() {
        let metrics = TestMetrics::install();
        let tmp = tempfile::tempdir().unwrap();
        let record = encode_record(&hint("a")).unwrap().len() as u64;
        let config = HintedHandoffConfig {
            max_hints_size: record,
            ..config(tmp.path())
        };
        let target_dir = config.hints_dir.join("42");
        std::fs::create_dir_all(&target_dir).unwrap();
        std::fs::write(target_dir.join("stray.bin"), vec![0u8; record as usize * 4]).unwrap();
        let manager = manager(config);

        manager.store(NodeId(42), hint("a")).await;
        manager.store(NodeId(42), hint("b")).await;

        assert_eq!(metrics.value(STORED, &[]), Some(1.0));
        assert_eq!(metrics.value(DROPPED, &[("reason", "full")]), Some(1.0));
        assert_eq!(metrics.value(PENDING, &[("peer", "node-42")]), Some(1.0));
    }

    /// Stores that arrive while a batch is being written wait for it, and the
    /// next batch takes all of them: one fsync for the lot, and every hint
    /// written exactly once.
    #[tokio::test]
    async fn stores_that_arrive_together_are_written_together() {
        let tmp = tempfile::tempdir().unwrap();
        let config = config(tmp.path());
        let manager = manager(config.clone());
        let keys: Vec<String> = (0..50).map(|i| format!("key-{i}")).collect();

        let manager = Arc::new(manager);
        let mut stores = tokio::task::JoinSet::new();
        for key in &keys {
            let (manager, hint) = (manager.clone(), hint(key));
            stores.spawn(async move { manager.store(NodeId(42), hint).await });
        }
        while let Some(stored) = stores.join_next().await {
            stored.unwrap();
        }

        let mut written: Vec<_> = read_log(&config, 42).into_iter().map(|h| h.key).collect();
        written.sort();
        let mut expected = keys.clone();
        expected.sort();
        assert_eq!(written, expected);
        let batches = manager.batches.load(Ordering::Relaxed);
        assert!(batches <= 2, "50 stores took {batches} batches");
    }

    /// A segment closes once it spans a quarter of the TTL, so hints that
    /// expire go as a whole segment while younger ones stay.
    #[tokio::test]
    async fn expired_segments_are_removed_and_younger_hints_stay() {
        let metrics = TestMetrics::install();
        let tmp = tempfile::tempdir().unwrap();
        let config = config(tmp.path()).with_ttl(Duration::from_secs(60));
        let manager = manager(config.clone());
        let taken_long_ago = |key: &str| HintEntry {
            created_at: now_millis() - 120_000,
            ..hint(key)
        };

        manager.store(NodeId(42), taken_long_ago("old-1")).await;
        manager.store(NodeId(42), taken_long_ago("old-2")).await;
        manager.store(NodeId(43), taken_long_ago("old-3")).await;
        manager.store(NodeId(42), hint("young")).await;
        assert_eq!(segment_files(&config, 42).len(), 2);

        manager.remove_expired().await;

        assert_eq!(metrics.value(DROPPED, &[("reason", "expired")]), Some(3.0));
        assert_eq!(metrics.value(PENDING, &[("peer", "node-42")]), Some(1.0));
        // Still exported, at zero: the peer has nothing waiting any more.
        assert_eq!(metrics.value(PENDING, &[("peer", "node-43")]), Some(0.0));
        let keys: Vec<_> = read_log(&config, 42).into_iter().map(|h| h.key).collect();
        assert_eq!(keys, ["young"]);
        assert!(!config.hints_dir.join("43").exists());

        // The segment that expired was the open one for node 43: the next
        // hint starts a new one rather than writing through a stale handle.
        manager.store(NodeId(43), hint("again")).await;
        assert_eq!(read_log(&config, 43).len(), 1);
    }

    #[tokio::test]
    async fn maintenance_removes_expired_hints_until_it_is_stopped() {
        let tmp = tempfile::tempdir().unwrap();
        let config = HintedHandoffConfig {
            maintenance_interval: Duration::from_millis(20),
            ..config(tmp.path()).with_ttl(Duration::from_millis(40))
        };
        let manager = Arc::new(manager(config));
        let (stop, stopped) = watch::channel(false);
        let loop_task = tokio::spawn(manager.clone().run_maintenance(None, stopped));

        manager.store(NodeId(42), hint("a")).await;
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while manager.pending(NodeId(42)).await > 0 {
            assert!(
                std::time::Instant::now() < deadline,
                "the expired hint was never removed"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        stop.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(5), loop_task)
            .await
            .expect("the loop stops when told")
            .unwrap();
        manager.store(NodeId(42), hint("b")).await;
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(
            manager.pending(NodeId(42)).await,
            1,
            "a stopped loop removes nothing"
        );
    }

    /// Hints of the earlier format were never delivered and carry no HLC:
    /// they are removed at startup, without touching the log or the metrics.
    #[tokio::test]
    async fn hint_files_of_the_earlier_format_are_removed_at_startup() {
        let tmp = tempfile::tempdir().unwrap();
        let config = config(tmp.path());
        {
            let manager = manager(config.clone());
            manager.store(NodeId(42), hint("a")).await;
        }
        let target_dir = config.hints_dir.join("42");
        std::fs::write(target_dir.join("0123abcd.hint"), b"old").unwrap();
        let only_old = config.hints_dir.join("43");
        std::fs::create_dir_all(&only_old).unwrap();
        std::fs::write(only_old.join("4567cdef.hint"), b"old").unwrap();

        let metrics = TestMetrics::install();
        let _restarted = manager(config.clone());

        assert!(!target_dir.join("0123abcd.hint").exists());
        assert!(!only_old.exists());
        assert_eq!(metrics.value(PENDING, &[("peer", "node-42")]), Some(1.0));
        assert_eq!(metrics.value(PENDING, &[("peer", "node-43")]), None);
        assert_eq!(metrics.value(DROPPED, &[("reason", "expired")]), None);
    }

    #[tokio::test]
    async fn a_zero_ttl_keeps_no_hints() {
        let metrics = TestMetrics::install();
        let tmp = tempfile::tempdir().unwrap();
        let config = config(tmp.path()).with_ttl(Duration::ZERO);
        let manager = manager(config.clone());

        manager.store(NodeId(42), hint("a")).await;

        assert!(!config.hints_dir.exists());
        assert_eq!(metrics.value(DROPPED, &[("reason", "expired")]), Some(1.0));
        assert_eq!(metrics.value(STORED, &[]), None);
    }

    // ---- Delivery ----

    use crate::gossip::NodeState;
    use crate::identity::{Edition, NodeAddr, NodeCapacity, NodeMeta, PoolId};
    use crate::replica::{ReplicaReadHandler, ReplicaWriteHandler};
    use crate::rpc::server::{NodeRpcServer, RpcServerConfig, RpcServerHandle};
    use s4_core::storage::BitcaskStorageEngine;
    use s4_core::StorageEngine;

    const TARGET: NodeId = NodeId(42);
    const DELIVERED: &str = "s4_hints_delivered_total";

    /// Where the delivery tests read the current state of a key: whatever
    /// the test put there.
    #[derive(Default)]
    struct ScriptedSource {
        /// `None` answers as a replica that did not answer.
        newest: Mutex<Option<NewestVersion>>,
        /// Keys asked about as if a replica did not answer, whatever `newest`.
        unanswered: Mutex<HashSet<String>>,
        copy: Mutex<Option<StoredCopy>>,
        copies_read: AtomicU64,
        /// A hint stored the first time the source is asked, as if written
        /// while delivery runs.
        store_meanwhile: Mutex<Option<(Arc<HintedHandoffManager>, HintEntry)>>,
        /// When asked about this key, gossip starts holding the target dead,
        /// as if it had stopped in the middle of the pass.
        target_stops_at: Mutex<Option<(String, Arc<GossipHandle>, NodeState)>>,
    }

    impl ScriptedSource {
        fn answering(newest: NewestVersion) -> Arc<Self> {
            let source = Self::default();
            *source.newest.lock().unwrap() = Some(newest);
            Arc::new(source)
        }

        fn with_copy(self: Arc<Self>, copy: StoredCopy) -> Arc<Self> {
            *self.copy.lock().unwrap() = Some(copy);
            self
        }
    }

    #[async_trait::async_trait]
    impl KeyStateSource for ScriptedSource {
        async fn newest_version(
            &self,
            _bucket: &str,
            key: &str,
            _target: NodeId,
        ) -> Result<NewestVersion, ClusterError> {
            let meanwhile = self.store_meanwhile.lock().unwrap().take();
            if let Some((manager, hint)) = meanwhile {
                manager.store(TARGET, hint).await;
            }
            let mut stops = self.target_stops_at.lock().unwrap();
            if stops.as_ref().is_some_and(|(at, _, _)| at == key) {
                if let Some((_, gossip, mut state)) = stops.take() {
                    state.status = NodeStatus::Dead;
                    gossip.set_member_for_testing(TARGET, state);
                }
            }
            drop(stops);
            if self.unanswered.lock().unwrap().contains(key) {
                return Err(ClusterError::Unavailable("replica 7 did not answer".into()));
            }
            let newest = *self.newest.lock().unwrap();
            newest.ok_or_else(|| ClusterError::Unavailable("replica 7 did not answer".into()))
        }

        async fn read_copy(
            &self,
            _bucket: &str,
            _key: &str,
            _node: NodeId,
        ) -> Result<StoredCopy, ClusterError> {
            self.copies_read.fetch_add(1, Ordering::Relaxed);
            let copy = self.copy.lock().unwrap().clone();
            copy.ok_or_else(|| ClusterError::Unavailable("no copy".into()))
        }
    }

    /// The target of the delivery tests: a replica on real storage, reached
    /// over gRPC, taking writes, deletes and HEADs on `epoch`.
    struct Target {
        storage: Arc<BitcaskStorageEngine>,
        state: NodeState,
        _server: RpcServerHandle,
        _dir: tempfile::TempDir,
    }

    impl Target {
        async fn start(port: u16, epoch: u64) -> Self {
            let dir = tempfile::tempdir().unwrap();
            let meta = NodeMeta {
                node_id: TARGET,
                node_name: "node-42".into(),
                addr: NodeAddr {
                    grpc_addr: format!("127.0.0.1:{port}").parse().unwrap(),
                    http_addr: "127.0.0.1:9000".parse().unwrap(),
                    gossip_addr: "127.0.0.1:9200".parse().unwrap(),
                },
                pool_id: PoolId::new(1),
                topology_epoch: epoch,
                protocol_version: crate::protocol::PROTOCOL_VERSION,
                capacity: NodeCapacity::default(),
                started_at: 0,
                s4_version: "0.1.0".into(),
                edition: Edition::Community,
            };
            let storage = Arc::new(
                BitcaskStorageEngine::new(
                    dir.path().join("volumes"),
                    dir.path().join("metadata"),
                    10 * 1024 * 1024,
                    4096,
                    false,
                )
                .await
                .unwrap(),
            );
            let server = NodeRpcServer::new(
                TARGET,
                Arc::new(meta.clone()),
                Arc::new(GossipHandle::new_for_testing(HashMap::new())),
            )
            .with_replica_handler(Arc::new(ReplicaWriteHandler::new(
                storage.clone(),
                Arc::new(crate::idempotency::IdempotencyTracker::new()),
                epoch,
            )))
            .with_read_handler(Arc::new(ReplicaReadHandler::new(storage.clone(), epoch)))
            .start(RpcServerConfig {
                bind_addr: meta.addr.grpc_addr,
                ..Default::default()
            })
            .await
            .unwrap();
            let state = NodeState {
                meta,
                status: NodeStatus::Alive,
                last_seen: std::time::Instant::now(),
            };
            Self {
                storage,
                state,
                _server: server,
                _dir: dir,
            }
        }

        /// Puts `data` for `key` on the target as a replica write at `hlc`.
        async fn holds(&self, key: &str, data: &[u8], hlc: HlcTimestamp) {
            let metadata = HashMap::from([
                ("_s4_hlc_wall".to_string(), hlc.wall_time.to_string()),
                ("_s4_hlc_logical".to_string(), hlc.logical.to_string()),
                ("_s4_hlc_node_id".to_string(), hlc.node_id.to_string()),
            ]);
            self.storage
                .put_object("bucket", key, data, "text/plain", &metadata)
                .await
                .unwrap();
        }

        /// The HLC and bytes of what the target holds for `key`.
        async fn copy_of(&self, key: &str) -> Option<(u64, Vec<u8>)> {
            let (data, record) = self.storage.get_object("bucket", key).await.ok()?;
            Some((record.hlc_timestamp, data))
        }
    }

    /// A manager and a delivery reaching `target` as gossip describes it,
    /// reading current states from `source`, on topology epoch 1.
    fn delivering(
        dir: &Path,
        target: NodeState,
        source: Arc<dyn KeyStateSource>,
    ) -> (Arc<HintedHandoffManager>, HintDelivery) {
        let (delivery, gossip) = delivery(target, source);
        let names = Arc::new(PeerNames::default());
        names.set([(TARGET, "node-42".to_string())]);
        let rpc_client = Arc::new(
            NodeClient::new(gossip, crate::rpc::NodeClientConfig::default()).with_peer_names(names),
        );
        let manager = Arc::new(HintedHandoffManager::new(config(dir), rpc_client));
        (manager, delivery)
    }

    /// A delivery reaching `target` as gossip describes it, reading current
    /// states from `source`, on topology epoch 1; and that gossip.
    fn delivery(
        target: NodeState,
        source: Arc<dyn KeyStateSource>,
    ) -> (HintDelivery, Arc<GossipHandle>) {
        let gossip = Arc::new(GossipHandle::new_for_testing(HashMap::from([(
            TARGET, target,
        )])));
        let rpc_client = Arc::new(NodeClient::new(
            gossip.clone(),
            crate::rpc::NodeClientConfig {
                max_retries: 0,
                ..crate::rpc::NodeClientConfig::default()
            },
        ));
        let delivery = HintDelivery::new(source, rpc_client, gossip.clone(), NodeId(1), 1);
        (delivery, gossip)
    }

    fn copy(data: &[u8], hlc: HlcTimestamp) -> StoredCopy {
        StoredCopy {
            hlc: Some(hlc),
            data: data.to_vec(),
            content_hash: vec![0u8; 32],
            content_type: "text/plain".into(),
            metadata: HashMap::new(),
            version_id: None,
        }
    }

    fn at(wall: u64) -> HlcTimestamp {
        HlcTimestamp::from_record(wall, 0, 7)
    }

    /// Every hint stored is pending, delivered or dropped, and nothing else.
    fn assert_hints_add_up(metrics: &TestMetrics) {
        let pending = metrics.value(PENDING, &[("peer", "node-42")]).unwrap_or(0.0);
        let stored = metrics.value(STORED, &[]).unwrap_or(0.0);
        let delivered = metrics.value(DELIVERED, &[]).unwrap_or(0.0);
        let dropped = metrics.sum(DROPPED, &[]);
        assert_eq!(
            pending,
            stored - delivered - dropped,
            "{}",
            metrics.render()
        );
    }

    #[tokio::test]
    async fn the_current_copy_is_written_to_the_target_with_its_own_hlc() {
        let metrics = TestMetrics::install();
        let tmp = tempfile::tempdir().unwrap();
        let target = Target::start(19791, 1).await;
        let source = ScriptedSource::answering(NewestVersion::Copy {
            hlc: at(20),
            node: NodeId(7),
            confirmed: true,
        })
        .with_copy(copy(b"version 20", at(20)));
        let (manager, delivery) = delivering(tmp.path(), target.state.clone(), source);

        manager.store(TARGET, HintEntry::written("bucket", "key", at(20))).await;
        manager.deliver(&delivery).await;

        assert_eq!(
            target.copy_of("key").await,
            Some((20, b"version 20".to_vec()))
        );
        assert_eq!(metrics.value(DELIVERED, &[]), Some(1.0));
        assert_eq!(metrics.value(PENDING, &[("peer", "node-42")]), Some(0.0));
        assert!(segment_files(&config(tmp.path()), 42).is_empty());
        assert_hints_add_up(&metrics);
    }

    /// The copy goes with its HLC, so a target that got something newer
    /// directly in the meantime keeps it.
    #[tokio::test]
    async fn a_target_holding_something_newer_keeps_it() {
        let metrics = TestMetrics::install();
        let tmp = tempfile::tempdir().unwrap();
        let target = Target::start(19792, 1).await;
        target.holds("key", b"version 30", at(30)).await;
        let source = ScriptedSource::answering(NewestVersion::Copy {
            hlc: at(20),
            node: NodeId(7),
            confirmed: true,
        })
        .with_copy(copy(b"version 20", at(20)));
        let (manager, delivery) = delivering(tmp.path(), target.state.clone(), source);

        manager.store(TARGET, HintEntry::written("bucket", "key", at(20))).await;
        manager.deliver(&delivery).await;

        assert_eq!(
            target.copy_of("key").await,
            Some((30, b"version 30".to_vec()))
        );
        assert_eq!(metrics.value(DELIVERED, &[]), Some(1.0));
        assert_hints_add_up(&metrics);
    }

    /// A key no other replica holds goes from the target too — unless the
    /// target's record is newer than the operation it missed.
    #[tokio::test]
    async fn a_key_gone_elsewhere_goes_from_the_target_unless_the_target_is_newer() {
        let metrics = TestMetrics::install();
        let tmp = tempfile::tempdir().unwrap();
        let target = Target::start(19793, 1).await;
        target.holds("stale", b"version 10", at(10)).await;
        target.holds("newer", b"version 30", at(30)).await;
        let source = ScriptedSource::answering(NewestVersion::Absent);
        let (manager, delivery) = delivering(tmp.path(), target.state.clone(), source);

        manager.store(TARGET, HintEntry::deleted("bucket", "stale", at(15))).await;
        manager.store(TARGET, HintEntry::deleted("bucket", "newer", at(15))).await;
        manager.store(TARGET, HintEntry::deleted("bucket", "never-there", at(15))).await;
        manager.deliver(&delivery).await;

        assert_eq!(target.copy_of("stale").await, None);
        assert_eq!(
            target.copy_of("newer").await,
            Some((30, b"version 30".to_vec()))
        );
        assert_eq!(metrics.value(DELIVERED, &[]), Some(3.0));
        assert_hints_add_up(&metrics);
    }

    /// A copy older than the delete the target missed missed the delete too:
    /// the key is gone, and the stale copy is not spread to the target.
    #[tokio::test]
    async fn a_copy_older_than_a_missed_delete_counts_as_gone() {
        let tmp = tempfile::tempdir().unwrap();
        let target = Target::start(19794, 1).await;
        target.holds("key", b"version 20", at(20)).await;
        let source = ScriptedSource::answering(NewestVersion::Copy {
            hlc: at(20),
            node: NodeId(7),
            confirmed: true,
        })
        .with_copy(copy(b"version 20", at(20)));
        let (manager, delivery) = delivering(tmp.path(), target.state.clone(), source.clone());

        manager.store(TARGET, HintEntry::deleted("bucket", "key", at(25))).await;
        manager.deliver(&delivery).await;

        assert_eq!(target.copy_of("key").await, None);
        assert_eq!(source.copies_read.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn an_erasure_coded_key_is_superseded_and_the_target_left_alone() {
        let metrics = TestMetrics::install();
        let tmp = tempfile::tempdir().unwrap();
        let target = Target::start(19795, 1).await;
        target.holds("key", b"hot copy", at(10)).await;
        let source = ScriptedSource::answering(NewestVersion::ErasureCoded { hlc: at(20) });
        let (manager, delivery) = delivering(tmp.path(), target.state.clone(), source);

        manager.store(TARGET, HintEntry::written("bucket", "key", at(20))).await;
        manager.deliver(&delivery).await;

        assert_eq!(
            target.copy_of("key").await,
            Some((10, b"hot copy".to_vec()))
        );
        assert_eq!(
            metrics.value(DROPPED, &[("reason", "superseded")]),
            Some(1.0)
        );
        assert_eq!(metrics.value(DELIVERED, &[]), None);
        assert_hints_add_up(&metrics);
    }

    #[tokio::test]
    async fn a_purge_removes_exactly_its_generation() {
        let metrics = TestMetrics::install();
        let tmp = tempfile::tempdir().unwrap();
        let target = Target::start(19796, 1).await;
        target.holds("purged", b"generation 10", at(10)).await;
        target.holds("rewritten", b"generation 11", at(11)).await;
        let source = Arc::new(ScriptedSource::default());
        let (manager, delivery) = delivering(tmp.path(), target.state.clone(), source);

        manager
            .store(TARGET, HintEntry::purge("bucket", "purged", at(12), at(10)))
            .await;
        manager
            .store(
                TARGET,
                HintEntry::purge("bucket", "rewritten", at(12), at(10)),
            )
            .await;
        manager.deliver(&delivery).await;

        assert_eq!(target.copy_of("purged").await, None);
        assert_eq!(
            target.copy_of("rewritten").await,
            Some((11, b"generation 11".to_vec()))
        );
        assert_eq!(metrics.value(DELIVERED, &[]), Some(2.0));
    }

    /// A target that refuses — here, one on another topology epoch — will
    /// not take the hint however often it is sent.
    #[tokio::test]
    async fn a_refusing_target_drops_the_hint_as_rejected() {
        let metrics = TestMetrics::install();
        let tmp = tempfile::tempdir().unwrap();
        let target = Target::start(19797, 2).await;
        let source = ScriptedSource::answering(NewestVersion::Copy {
            hlc: at(20),
            node: NodeId(7),
            confirmed: true,
        })
        .with_copy(copy(b"version 20", at(20)));
        let (manager, delivery) = delivering(tmp.path(), target.state.clone(), source);

        manager.store(TARGET, HintEntry::written("bucket", "key", at(20))).await;
        manager.deliver(&delivery).await;

        assert_eq!(target.copy_of("key").await, None);
        assert_eq!(metrics.value(DROPPED, &[("reason", "rejected")]), Some(1.0));
        assert_eq!(metrics.value(PENDING, &[("peer", "node-42")]), Some(0.0));
        assert_hints_add_up(&metrics);
    }

    /// A replica that does not answer may hold the newest version: the
    /// segment stays whole, nothing is counted, and the next pass delivers.
    #[tokio::test]
    async fn a_replica_that_does_not_answer_leaves_the_segment_for_the_next_pass() {
        let metrics = TestMetrics::install();
        let tmp = tempfile::tempdir().unwrap();
        let target = Target::start(19798, 1).await;
        let source = Arc::new(ScriptedSource::default());
        let (manager, delivery) = delivering(tmp.path(), target.state.clone(), source.clone());

        manager.store(TARGET, HintEntry::written("bucket", "key", at(20))).await;
        let segment = segment_files(&config(tmp.path()), 42).pop().unwrap();
        let bytes = std::fs::read(&segment).unwrap();
        manager.deliver(&delivery).await;

        assert_eq!(metrics.value(PENDING, &[("peer", "node-42")]), Some(1.0));
        assert_eq!(metrics.value(DELIVERED, &[]), None);
        assert_eq!(metrics.sum(DROPPED, &[]), 0.0);
        // Nothing in it could be settled: the very same file, not a copy.
        assert_eq!(
            segment_files(&config(tmp.path()), 42),
            std::slice::from_ref(&segment)
        );
        assert_eq!(std::fs::read(&segment).unwrap(), bytes);

        *source.newest.lock().unwrap() = Some(NewestVersion::Copy {
            hlc: at(20),
            node: NodeId(7),
            confirmed: true,
        });
        *source.copy.lock().unwrap() = Some(copy(b"version 20", at(20)));
        manager.deliver(&delivery).await;

        assert_eq!(
            target.copy_of("key").await,
            Some((20, b"version 20".to_vec()))
        );
        assert_eq!(metrics.value(DELIVERED, &[]), Some(1.0));
        assert_hints_add_up(&metrics);
    }

    /// A node from before conditional deletes would take one for a plain
    /// delete: it is not sent one.
    #[tokio::test]
    async fn a_target_too_old_for_a_conditional_delete_is_not_sent_one() {
        let metrics = TestMetrics::install();
        let tmp = tempfile::tempdir().unwrap();
        let target = Target::start(19799, 1).await;
        target.holds("key", b"version 10", at(10)).await;
        let mut old = target.state.clone();
        old.meta.protocol_version = CONDITIONAL_DELETE_MIN_PROTOCOL_VERSION - 1;
        let source = ScriptedSource::answering(NewestVersion::Absent);
        let (manager, delivery) = delivering(tmp.path(), old, source);

        manager.store(TARGET, HintEntry::deleted("bucket", "key", at(15))).await;
        manager.deliver(&delivery).await;

        assert_eq!(
            target.copy_of("key").await,
            Some((10, b"version 10".to_vec()))
        );
        assert_eq!(metrics.value(DROPPED, &[("reason", "rejected")]), Some(1.0));
    }

    #[tokio::test]
    async fn hints_for_a_target_gossip_holds_dead_wait() {
        let metrics = TestMetrics::install();
        let tmp = tempfile::tempdir().unwrap();
        let target = Target::start(19800, 1).await;
        let mut dead = target.state.clone();
        dead.status = NodeStatus::Dead;
        let source = ScriptedSource::answering(NewestVersion::Copy {
            hlc: at(20),
            node: NodeId(7),
            confirmed: true,
        })
        .with_copy(copy(b"version 20", at(20)));
        let (manager, delivery) = delivering(tmp.path(), dead, source.clone());

        manager.store(TARGET, HintEntry::written("bucket", "key", at(20))).await;
        manager.deliver(&delivery).await;

        assert_eq!(target.copy_of("key").await, None);
        assert_eq!(source.copies_read.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.value(PENDING, &[("peer", "node-42")]), Some(1.0));
    }

    /// Hints for one key delivered once: the first brought the target to a
    /// state newer than all of them.
    #[tokio::test]
    async fn repeated_hints_for_a_key_cost_one_delivery() {
        let metrics = TestMetrics::install();
        let tmp = tempfile::tempdir().unwrap();
        let target = Target::start(19801, 1).await;
        let source = ScriptedSource::answering(NewestVersion::Copy {
            hlc: at(30),
            node: NodeId(7),
            confirmed: true,
        })
        .with_copy(copy(b"version 30", at(30)));
        let (manager, delivery) = delivering(tmp.path(), target.state.clone(), source.clone());

        for wall in [10, 20, 30] {
            manager.store(TARGET, HintEntry::written("bucket", "key", at(wall))).await;
        }
        manager.deliver(&delivery).await;

        assert_eq!(source.copies_read.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.value(DELIVERED, &[]), Some(3.0));
        assert_eq!(
            target.copy_of("key").await,
            Some((30, b"version 30".to_vec()))
        );
    }

    #[tokio::test]
    async fn a_hint_past_its_ttl_is_dropped_rather_than_delivered() {
        let metrics = TestMetrics::install();
        let tmp = tempfile::tempdir().unwrap();
        let target = Target::start(19802, 1).await;
        let source = ScriptedSource::answering(NewestVersion::Copy {
            hlc: at(20),
            node: NodeId(7),
            confirmed: true,
        })
        .with_copy(copy(b"version 20", at(20)));
        let (manager, delivery) = delivering(tmp.path(), target.state.clone(), source.clone());
        let old = HintEntry {
            created_at: now_millis() - DEFAULT_HINT_TTL.as_millis() as u64 - 1,
            ..HintEntry::written("bucket", "key", at(20))
        };

        manager.store(TARGET, old).await;
        manager.deliver(&delivery).await;

        assert_eq!(source.copies_read.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.value(DROPPED, &[("reason", "expired")]), Some(1.0));
        assert_hints_add_up(&metrics);
    }

    /// Delivery closes the open segment before it starts: a hint stored while
    /// it runs goes to the next segment, is not lost, and is not counted as
    /// delivered by a pass that never saw it.
    #[tokio::test]
    async fn a_hint_stored_during_delivery_waits_for_the_next_pass() {
        let metrics = TestMetrics::install();
        let tmp = tempfile::tempdir().unwrap();
        let target = Target::start(19803, 1).await;
        let source = ScriptedSource::answering(NewestVersion::Copy {
            hlc: at(20),
            node: NodeId(7),
            confirmed: true,
        })
        .with_copy(copy(b"version 20", at(20)));
        let (manager, delivery) = delivering(tmp.path(), target.state.clone(), source.clone());
        *source.store_meanwhile.lock().unwrap() = Some((
            manager.clone(),
            HintEntry::written("bucket", "later", at(20)),
        ));

        manager.store(TARGET, HintEntry::written("bucket", "key", at(20))).await;
        manager.deliver(&delivery).await;

        assert_eq!(metrics.value(DELIVERED, &[]), Some(1.0));
        assert_eq!(metrics.value(PENDING, &[("peer", "node-42")]), Some(1.0));
        let waiting: Vec<_> =
            read_log(&config(tmp.path()), 42).into_iter().map(|h| h.key).collect();
        assert_eq!(waiting, ["later"]);

        manager.deliver(&delivery).await;
        assert_eq!(metrics.value(DELIVERED, &[]), Some(2.0));
        assert_eq!(metrics.value(PENDING, &[("peer", "node-42")]), Some(0.0));
        assert_hints_add_up(&metrics);
    }

    /// A node stopped in the middle of a delivery left its segment whole; the
    /// next run counts it from disk and delivers it.
    #[tokio::test]
    async fn a_segment_a_stopped_node_left_is_delivered_after_the_restart() {
        let tmp = tempfile::tempdir().unwrap();
        let target = Target::start(19804, 1).await;
        {
            let (manager, delivery) = delivering(
                tmp.path(),
                target.state.clone(),
                Arc::new(ScriptedSource::default()),
            );
            manager.store(TARGET, HintEntry::written("bucket", "key", at(20))).await;
            manager.deliver(&delivery).await;
            assert_eq!(manager.pending(TARGET).await, 1);
        }

        let metrics = TestMetrics::install();
        let source = ScriptedSource::answering(NewestVersion::Copy {
            hlc: at(20),
            node: NodeId(7),
            confirmed: true,
        })
        .with_copy(copy(b"version 20", at(20)));
        let (restarted, delivery) = delivering(tmp.path(), target.state.clone(), source);
        assert_eq!(metrics.value(PENDING, &[("peer", "node-42")]), Some(1.0));
        restarted.deliver(&delivery).await;

        assert_eq!(
            target.copy_of("key").await,
            Some((20, b"version 20".to_vec()))
        );
        assert_eq!(metrics.value(DELIVERED, &[]), Some(1.0));
        assert_eq!(metrics.value(PENDING, &[("peer", "node-42")]), Some(0.0));
    }

    #[tokio::test]
    async fn maintenance_delivers_on_its_own() {
        let tmp = tempfile::tempdir().unwrap();
        let target = Target::start(19805, 1).await;
        let source = ScriptedSource::answering(NewestVersion::Copy {
            hlc: at(20),
            node: NodeId(7),
            confirmed: true,
        })
        .with_copy(copy(b"version 20", at(20)));
        let names = Arc::new(PeerNames::default());
        let gossip = Arc::new(GossipHandle::new_for_testing(HashMap::from([(
            TARGET,
            target.state.clone(),
        )])));
        let rpc_client = Arc::new(
            NodeClient::new(gossip.clone(), crate::rpc::NodeClientConfig::default())
                .with_peer_names(names),
        );
        let config = HintedHandoffConfig {
            maintenance_interval: Duration::from_millis(20),
            ..config(tmp.path())
        };
        let manager = Arc::new(HintedHandoffManager::new(config, rpc_client.clone()));
        let delivery = HintDelivery::new(source, rpc_client, gossip, NodeId(1), 1);
        let (_stop, stopped) = watch::channel(false);
        tokio::spawn(manager.clone().run_maintenance(Some(delivery), stopped));

        manager.store(TARGET, HintEntry::written("bucket", "key", at(20))).await;
        // The target has the copy a moment before its segment is removed.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while manager.pending(TARGET).await > 0 {
            assert!(
                std::time::Instant::now() < deadline,
                "the hint was never delivered"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            target.copy_of("key").await,
            Some((20, b"version 20".to_vec()))
        );
    }

    /// A record written without an HLC — stored before its node joined the
    /// cluster — is reported with one made up from its modification time. One
    /// no newer than the delete the target missed is removed by the zero HLC,
    /// which matches only a record that still stores none; a newer one orders
    /// only roughly, and is left alone.
    #[tokio::test]
    async fn a_target_record_without_an_hlc_is_removed_by_the_delete_it_missed() {
        let metrics = TestMetrics::install();
        let tmp = tempfile::tempdir().unwrap();
        let target = Target::start(19806, 1).await;
        for key in ["older", "newer"] {
            target
                .storage
                .put_object(
                    "bucket",
                    key,
                    b"written before HLCs",
                    "text/plain",
                    &HashMap::new(),
                )
                .await
                .unwrap();
        }
        let source = ScriptedSource::answering(NewestVersion::Absent);
        let (manager, delivery) = delivering(tmp.path(), target.state.clone(), source);

        manager
            .store(
                TARGET,
                HintEntry::deleted("bucket", "older", at(u64::MAX / 2)),
            )
            .await;
        manager.store(TARGET, HintEntry::deleted("bucket", "newer", at(1))).await;
        manager.deliver(&delivery).await;

        assert_eq!(target.copy_of("older").await, None);
        assert!(target.copy_of("newer").await.is_some());
        assert_eq!(metrics.value(DELIVERED, &[]), Some(1.0));
        assert_eq!(metrics.value(DROPPED, &[("reason", "rejected")]), Some(1.0));
        assert_hints_add_up(&metrics);
    }

    /// A copy read repair took from a record written without an HLC stores
    /// the made-up one, node 0. It is removed by that HLC like any other
    /// generation when it is no newer than the delete the target missed; a
    /// newer one orders only roughly, and is left alone.
    #[tokio::test]
    async fn a_repaired_copy_with_a_made_up_hlc_is_removed_by_the_delete_it_missed() {
        let metrics = TestMetrics::install();
        let tmp = tempfile::tempdir().unwrap();
        let target = Target::start(19872, 1).await;
        let made_up = |wall| HlcTimestamp::from_record(wall, 0, 0);
        target.holds("older", b"repaired at 20", made_up(20)).await;
        target.holds("newer", b"repaired at 40", made_up(40)).await;
        let source = ScriptedSource::answering(NewestVersion::Absent);
        let (manager, delivery) = delivering(tmp.path(), target.state.clone(), source);

        for key in ["older", "newer"] {
            manager.store(TARGET, HintEntry::deleted("bucket", key, at(30))).await;
        }
        manager.deliver(&delivery).await;

        assert_eq!(target.copy_of("older").await, None);
        assert_eq!(
            target.copy_of("newer").await,
            Some((40, b"repaired at 40".to_vec()))
        );
        assert_eq!(metrics.value(DELIVERED, &[]), Some(1.0));
        assert_eq!(metrics.value(DROPPED, &[("reason", "rejected")]), Some(1.0));
        assert_hints_add_up(&metrics);
    }

    /// A copy of a record written without an HLC carries the made-up one,
    /// which the target's LWW weighs against a record that has an HLC, but
    /// not against the target's own record without one. It is written only
    /// where the target holds nothing as new, ordered as a quorum read orders
    /// them, and keeps node 0 there.
    #[tokio::test]
    async fn a_copy_with_a_made_up_hlc_is_written_only_where_nothing_newer_is_held() {
        let metrics = TestMetrics::install();
        let tmp = tempfile::tempdir().unwrap();
        let target = Target::start(19873, 1).await;
        let made_up = HlcTimestamp::from_record(30, 0, 0);
        target.holds("older", b"version 20", at(20)).await;
        target.holds("newer", b"version 40", at(40)).await;
        target
            .storage
            .put_object(
                "bucket",
                "own",
                b"own, no hlc",
                "text/plain",
                &HashMap::new(),
            )
            .await
            .unwrap();
        let source = ScriptedSource::answering(NewestVersion::Copy {
            hlc: made_up,
            node: NodeId(7),
            confirmed: true,
        })
        .with_copy(copy(b"made up at 30", made_up));
        let (manager, delivery) = delivering(tmp.path(), target.state.clone(), source);

        for key in ["absent", "older", "newer", "own"] {
            manager.store(TARGET, HintEntry::written("bucket", key, at(20))).await;
        }
        manager.deliver(&delivery).await;

        for key in ["absent", "older"] {
            let record = target.storage.head_object("bucket", key).await.unwrap();
            assert_eq!(HlcTimestamp::of_record(&record), made_up, "{key}");
        }
        assert_eq!(
            target.copy_of("newer").await,
            Some((40, b"version 40".to_vec()))
        );
        assert_eq!(
            target.copy_of("own").await,
            Some((0, b"own, no hlc".to_vec()))
        );
        assert_eq!(metrics.value(DELIVERED, &[]), Some(4.0));
        assert_hints_add_up(&metrics);
    }

    /// The target's LWW weighs HLCs: a copy without one has no place among
    /// the target's versions, and is not written.
    #[tokio::test]
    async fn a_copy_without_an_hlc_is_not_written_over_the_target() {
        let metrics = TestMetrics::install();
        let tmp = tempfile::tempdir().unwrap();
        let target = Target::start(19807, 1).await;
        target.holds("key", b"version 30", at(30)).await;
        let source = ScriptedSource::answering(NewestVersion::Copy {
            hlc: at(20),
            node: NodeId(7),
            confirmed: true,
        })
        .with_copy(StoredCopy {
            hlc: None,
            ..copy(b"no hlc", at(20))
        });
        let (manager, delivery) = delivering(tmp.path(), target.state.clone(), source);

        manager.store(TARGET, HintEntry::written("bucket", "key", at(20))).await;
        manager.deliver(&delivery).await;

        assert_eq!(
            target.copy_of("key").await,
            Some((30, b"version 30".to_vec()))
        );
        assert_eq!(metrics.value(DROPPED, &[("reason", "rejected")]), Some(1.0));
    }

    /// A target known to gossip before its metadata arrived has announced no
    /// protocol: whether it takes a conditional delete is not known yet, and
    /// the hint waits for the next pass rather than being given up.
    #[tokio::test]
    async fn a_target_that_has_not_announced_its_protocol_is_tried_again() {
        let metrics = TestMetrics::install();
        let tmp = tempfile::tempdir().unwrap();
        let target = Target::start(19808, 1).await;
        target.holds("key", b"version 10", at(10)).await;
        let mut unannounced = target.state.clone();
        unannounced.meta.protocol_version = 0;
        let source = ScriptedSource::answering(NewestVersion::Absent);
        let (manager, delivery) = delivering(tmp.path(), unannounced, source);

        manager.store(TARGET, HintEntry::deleted("bucket", "key", at(15))).await;
        manager.deliver(&delivery).await;

        assert!(target.copy_of("key").await.is_some());
        assert_eq!(metrics.sum(DROPPED, &[]), 0.0);
        assert_eq!(metrics.value(PENDING, &[("peer", "node-42")]), Some(1.0));
    }

    // ---- A copy that may have missed a delete ----

    /// A copy the other replicas do not confirm may be one that missed a
    /// delete: it is not read, not written, and the hint is ambiguous.
    #[tokio::test]
    async fn an_unconfirmed_copy_is_not_spread() {
        let metrics = TestMetrics::install();
        let tmp = tempfile::tempdir().unwrap();
        let target = Target::start(19860, 1).await;
        let source = ScriptedSource::answering(NewestVersion::Copy {
            hlc: at(20),
            node: NodeId(7),
            confirmed: false,
        })
        .with_copy(copy(b"version 20", at(20)));
        let (manager, delivery) = delivering(tmp.path(), target.state.clone(), source.clone());

        manager.store(TARGET, HintEntry::written("bucket", "key", at(20))).await;
        manager.deliver(&delivery).await;

        assert_eq!(target.copy_of("key").await, None);
        assert_eq!(source.copies_read.load(Ordering::Relaxed), 0);
        assert_eq!(
            metrics.value(DROPPED, &[("reason", "ambiguous")]),
            Some(1.0)
        );
        assert_eq!(metrics.value(PENDING, &[("peer", "node-42")]), Some(0.0));
        assert_hints_add_up(&metrics);
    }

    /// The same copy is no question for a target that already holds it, or
    /// something newer: the target is current.
    #[tokio::test]
    async fn an_unconfirmed_copy_the_target_already_holds_is_delivered() {
        let metrics = TestMetrics::install();
        let tmp = tempfile::tempdir().unwrap();
        let target = Target::start(19861, 1).await;
        target.holds("same", b"version 20", at(20)).await;
        target.holds("newer", b"version 30", at(30)).await;
        let source = ScriptedSource::answering(NewestVersion::Copy {
            hlc: at(20),
            node: NodeId(7),
            confirmed: false,
        });
        let (manager, delivery) = delivering(tmp.path(), target.state.clone(), source);

        manager.store(TARGET, HintEntry::written("bucket", "same", at(20))).await;
        manager.store(TARGET, HintEntry::deleted("bucket", "newer", at(15))).await;
        manager.deliver(&delivery).await;

        assert_eq!(metrics.value(DELIVERED, &[]), Some(2.0));
        assert_eq!(metrics.sum(DROPPED, &[]), 0.0);
        assert_eq!(
            target.copy_of("newer").await,
            Some((30, b"version 30".to_vec()))
        );
        assert_hints_add_up(&metrics);
    }

    // ---- Hints that cannot be settled now ----

    /// A key the other replicas cannot settle now waits for a later pass,
    /// keeping its age, and the hints behind it are delivered meanwhile.
    #[tokio::test]
    async fn a_key_that_cannot_be_settled_now_does_not_hold_up_the_others() {
        let metrics = TestMetrics::install();
        let tmp = tempfile::tempdir().unwrap();
        let target = Target::start(19862, 1).await;
        let source = ScriptedSource::answering(NewestVersion::Copy {
            hlc: at(20),
            node: NodeId(7),
            confirmed: true,
        })
        .with_copy(copy(b"version 20", at(20)));
        source.unanswered.lock().unwrap().insert("stuck".to_string());
        let (manager, delivery) = delivering(tmp.path(), target.state.clone(), source.clone());

        manager.store(TARGET, HintEntry::written("bucket", "stuck", at(20))).await;
        manager.store(TARGET, HintEntry::written("bucket", "free", at(20))).await;
        let first = segment_files(&config(tmp.path()), 42);
        let stuck = read_log(&config(tmp.path()), 42).remove(0);
        manager.deliver(&delivery).await;

        assert_eq!(
            target.copy_of("free").await,
            Some((20, b"version 20".to_vec()))
        );
        assert_eq!(target.copy_of("stuck").await, None);
        assert_eq!(metrics.value(DELIVERED, &[]), Some(1.0));
        assert_eq!(metrics.value(PENDING, &[("peer", "node-42")]), Some(1.0));
        assert_eq!(read_log(&config(tmp.path()), 42), [stuck]);
        assert_ne!(segment_files(&config(tmp.path()), 42), first);
        assert_hints_add_up(&metrics);

        source.unanswered.lock().unwrap().clear();
        manager.deliver(&delivery).await;

        assert_eq!(
            target.copy_of("stuck").await,
            Some((20, b"version 20".to_vec()))
        );
        assert_eq!(metrics.value(DELIVERED, &[]), Some(2.0));
        assert_eq!(metrics.value(PENDING, &[("peer", "node-42")]), Some(0.0));
        assert!(segment_files(&config(tmp.path()), 42).is_empty());
        assert_hints_add_up(&metrics);
    }

    /// Hints of a segment expire a few at a time. While the rest cannot be
    /// settled — another replica down for hours — expiry alone does not get
    /// the segment written out again on every pass; it is counted once the
    /// segment goes.
    #[tokio::test]
    async fn expired_hints_alone_do_not_get_a_segment_rewritten() {
        let metrics = TestMetrics::install();
        let tmp = tempfile::tempdir().unwrap();
        let target = Target::start(19866, 1).await;
        let source = ScriptedSource::answering(NewestVersion::Copy {
            hlc: at(20),
            node: NodeId(7),
            confirmed: true,
        })
        .with_copy(copy(b"version 20", at(20)));
        source.unanswered.lock().unwrap().insert("waiting".to_string());
        let (manager, delivery) = delivering(tmp.path(), target.state.clone(), source.clone());
        let expired = HintEntry {
            created_at: now_millis() - DEFAULT_HINT_TTL.as_millis() as u64 - 1,
            ..HintEntry::written("bucket", "old", at(20))
        };

        manager.store(TARGET, HintEntry::written("bucket", "waiting", at(20))).await;
        manager.store(TARGET, expired).await;
        let segment = segment_files(&config(tmp.path()), 42);
        assert_eq!(segment.len(), 1, "both hints in one segment");
        let bytes = std::fs::read(&segment[0]).unwrap();
        manager.deliver(&delivery).await;

        assert_eq!(segment_files(&config(tmp.path()), 42), segment);
        assert_eq!(std::fs::read(&segment[0]).unwrap(), bytes);
        assert_eq!(metrics.sum(DROPPED, &[]), 0.0);
        assert_eq!(metrics.value(PENDING, &[("peer", "node-42")]), Some(2.0));

        source.unanswered.lock().unwrap().clear();
        manager.deliver(&delivery).await;
        assert_eq!(metrics.value(DELIVERED, &[]), Some(1.0));
        assert_eq!(metrics.value(DROPPED, &[("reason", "expired")]), Some(1.0));
        assert_eq!(metrics.value(PENDING, &[("peer", "node-42")]), Some(0.0));
        assert_hints_add_up(&metrics);
    }

    /// Hints carried over were within the limit already: moving them while
    /// their old segment still counts must not drop them as if the log were
    /// full.
    #[tokio::test]
    async fn carried_over_hints_are_not_dropped_by_the_limit() {
        let metrics = TestMetrics::install();
        let tmp = tempfile::tempdir().unwrap();
        let target = Target::start(19865, 1).await;
        let source = ScriptedSource::answering(NewestVersion::Copy {
            hlc: at(20),
            node: NodeId(7),
            confirmed: true,
        })
        .with_copy(copy(b"version 20", at(20)));
        source.unanswered.lock().unwrap().insert("stuck".to_string());
        let record = encode_record(&HintEntry::written("bucket", "stuck", at(20))).unwrap().len();
        let config = HintedHandoffConfig {
            max_hints_size: 2 * record as u64,
            ..config(tmp.path())
        };
        let manager = manager(config.clone());
        let (delivery, _) = delivery(target.state.clone(), source);

        manager.store(TARGET, HintEntry::written("bucket", "stuck", at(20))).await;
        manager.store(TARGET, HintEntry::written("bucket", "free", at(20))).await;
        manager.deliver(&delivery).await;

        assert_eq!(metrics.value(DROPPED, &[("reason", "full")]), None);
        let waiting: Vec<_> = read_log(&config, 42).into_iter().map(|h| h.key).collect();
        assert_eq!(waiting, ["stuck"]);
        assert_hints_add_up(&metrics);
    }

    /// A target that stops answering ends the pass, and what the pass settled
    /// before that stays settled: only the hints not reached are left.
    #[tokio::test]
    async fn a_target_that_stops_answering_keeps_what_was_settled() {
        let metrics = TestMetrics::install();
        let tmp = tempfile::tempdir().unwrap();
        let target = Target::start(19863, 1).await;
        let source = ScriptedSource::answering(NewestVersion::Copy {
            hlc: at(20),
            node: NodeId(7),
            confirmed: true,
        })
        .with_copy(copy(b"version 20", at(20)));
        let (manager, delivery) = delivering(tmp.path(), target.state.clone(), source.clone());
        *source.target_stops_at.lock().unwrap() = Some((
            "b".to_string(),
            delivery.gossip.clone(),
            target.state.clone(),
        ));

        for key in ["a", "b", "c"] {
            manager.store(TARGET, HintEntry::written("bucket", key, at(20))).await;
        }
        manager.deliver(&delivery).await;

        assert_eq!(
            target.copy_of("a").await,
            Some((20, b"version 20".to_vec()))
        );
        assert_eq!(metrics.value(DELIVERED, &[]), Some(1.0));
        let waiting: Vec<_> =
            read_log(&config(tmp.path()), 42).into_iter().map(|h| h.key).collect();
        assert_eq!(waiting, ["b", "c"]);
        assert_eq!(metrics.value(PENDING, &[("peer", "node-42")]), Some(2.0));
        assert_hints_add_up(&metrics);
    }

    /// A replica that fails at once is hinted at once, while the write goes
    /// on to the others. Until the settle time has passed, a write hint whose
    /// key looks older than the write on the other replicas waits.
    #[tokio::test]
    async fn a_write_hint_waits_while_its_write_may_still_be_landing() {
        let metrics = TestMetrics::install();
        let tmp = tempfile::tempdir().unwrap();
        let target = Target::start(19864, 1).await;
        let source = ScriptedSource::answering(NewestVersion::Absent);
        let (manager, delivery) = delivering(tmp.path(), target.state.clone(), source.clone());
        let taken_long_ago = HintEntry {
            created_at: now_millis() - 120_000,
            ..HintEntry::written("bucket", "settled", at(20))
        };

        manager.store(TARGET, HintEntry::written("bucket", "landing", at(20))).await;
        manager.store(TARGET, taken_long_ago).await;
        manager.deliver(&delivery).await;

        assert_eq!(metrics.value(DELIVERED, &[]), Some(1.0));
        let waiting: Vec<_> =
            read_log(&config(tmp.path()), 42).into_iter().map(|h| h.key).collect();
        assert_eq!(waiting, ["landing"]);

        // An older copy is no proof either.
        *source.newest.lock().unwrap() = Some(NewestVersion::Copy {
            hlc: at(10),
            node: NodeId(7),
            confirmed: true,
        });
        manager.deliver(&delivery).await;
        assert_eq!(source.copies_read.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.value(PENDING, &[("peer", "node-42")]), Some(1.0));

        // Once the write cannot be landing any more, the hint is delivered.
        let (settled, _) = delivery_settling_at_once(target.state.clone(), source.clone());
        *source.newest.lock().unwrap() = Some(NewestVersion::Absent);
        manager.deliver(&settled).await;
        assert_eq!(metrics.value(DELIVERED, &[]), Some(2.0));
        assert_eq!(metrics.value(PENDING, &[("peer", "node-42")]), Some(0.0));
        assert_hints_add_up(&metrics);
    }

    fn delivery_settling_at_once(
        target: NodeState,
        source: Arc<dyn KeyStateSource>,
    ) -> (HintDelivery, Arc<GossipHandle>) {
        let (delivery, gossip) = delivery(target, source);
        (delivery.with_settle_time(Duration::ZERO), gossip)
    }

    /// A hint longer than a record may be would stop every reading of its
    /// segment and hide the hints behind it: it is refused when written.
    #[tokio::test]
    async fn a_hint_too_long_for_a_record_is_refused_and_hides_nothing() {
        let metrics = TestMetrics::install();
        let tmp = tempfile::tempdir().unwrap();
        let config = config(tmp.path());
        let manager = manager(config.clone());
        let long_key = "k".repeat(MAX_RECORD_LEN + 1);

        manager
            .store(NodeId(42), HintEntry::written("bucket", &long_key, hlc(10)))
            .await;
        manager.store(NodeId(42), hint("after")).await;

        let keys: Vec<_> = read_log(&config, 42).into_iter().map(|h| h.key).collect();
        assert_eq!(keys, ["after"]);
        assert_eq!(metrics.value(STORED, &[]), Some(1.0));
        assert_eq!(metrics.value(PENDING, &[("peer", "node-42")]), Some(1.0));
        drop(manager);
        let restarted = self::manager(config);
        assert_eq!(restarted.pending(NodeId(42)).await, 1);
    }
}
