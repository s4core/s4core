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

//! Quorum read coordinator with digest-based optimization.
//!
//! # Read Flow
//!
//! ```text
//! 1. Determine placement (pool + replicas)
//! 2. Send HeadObject to ALL replicas in parallel (digest-based)
//! 3. Wait until the answers decide the read (`decide`)
//! 4a. If versions match (same HLC + ETag):
//!     Read full data from nearest/first replica
//! 4b. If versions differ:
//!     Read full data from node with latest version (LWW by HLC)
//!     Trigger async read repair on stale replica(s)
//! 4c. If the latest version is a tombstone, or no replica holds one:
//!     Return 404; a full read repairs copies older than the tombstone
//! 5. If too few replicas answered: return 503
//! ```
//!
//! A tombstone is a version like an object: a replica reports it with the
//! HLC of the delete, and it outranks an older copy another replica missed
//! the delete with.
//!
//! # Digest-Based Reads
//!
//! Instead of fetching full object data from all replicas, we first
//! request only the digest (via HeadObject). This saves bandwidth:
//! R digests (~100 bytes each) + 1 full read instead of R full copies.

use std::collections::HashMap;
use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use bytes::Bytes;
use s4_core::storage::{VersionKind, VersionRank};
use s4_core::{KeyVersion, ReadOptions, StorageEngine};
use tokio::io::{AsyncRead, ReadBuf};
use tokio_stream::StreamExt;
use tokio_util::io::StreamReader;
use tracing::{debug, error, warn};

use crate::cluster_metrics::{observe_quorum, QuorumOperation};
use crate::config::ClusterConfig;
use crate::error::ClusterError;
use crate::hlc::{HlcTimestamp, HybridClock};
use crate::identity::NodeId;
use crate::placement::{PlacementDecision, PlacementRouter};
use crate::repair::read_repair::{Repair, RepairTask};
use crate::repair::reconcile::{
    self, KeyStateSource, KeyVersions, NewestVersion, ReplicaHolds, StoredCopy,
};
use crate::repair::ReadRepairService;
use crate::rpc::proto;
use crate::rpc::NodeClient;

/// Digest of an object version, used for comparing replicas
/// without transferring full data.
#[derive(Debug, Clone)]
struct ObjectDigest {
    node_id: NodeId,
    hlc: HlcTimestamp,
    etag: String,
    size: u64,
    content_type: String,
    version_id: Option<String>,
    metadata: HashMap<String, String>,
    /// The replica holds a namespace entry for an erasure-coded object, not a
    /// copy of its bytes.
    erasure_coded: bool,
    /// SHA-256 of the object content, absent when the replica did not report
    /// one. A node from before the field existed reports none.
    content_hash: Option<[u8; 32]>,
    /// The version is the key's tombstone: the replica holds the delete, at
    /// `hlc`, and no object.
    tombstone: bool,
}

/// Read a 32-byte content hash out of a wire field.
///
/// Anything other than exactly 32 bytes means the peer did not send one, which
/// is the honest answer from a node that predates the field.
fn content_hash_from_bytes(bytes: &[u8]) -> Option<[u8; 32]> {
    <[u8; 32]>::try_from(bytes).ok()
}

impl ObjectDigest {
    /// The digest of a record replica `node_id` holds.
    fn of_record(node_id: NodeId, record: s4_core::IndexRecord) -> Self {
        Self {
            node_id,
            erasure_coded: record.is_erasure_coded(),
            hlc: HlcTimestamp::of_record(&record),
            content_hash: Some(record.content_hash),
            etag: record.etag,
            size: record.size,
            content_type: record.content_type,
            version_id: record.version_id,
            metadata: record.metadata,
            tombstone: false,
        }
    }

    /// The digest of a tombstone replica `node_id` holds, issued at `hlc`.
    fn of_tombstone(node_id: NodeId, hlc: HlcTimestamp) -> Self {
        Self {
            node_id,
            hlc,
            etag: String::new(),
            size: 0,
            content_type: String::new(),
            version_id: None,
            metadata: HashMap::new(),
            erasure_coded: false,
            content_hash: None,
            tombstone: true,
        }
    }

    /// The rank of the version this digest describes: what a read, a
    /// listing, anti-entropy and the replicas' own LWW order versions by.
    fn rank(&self) -> VersionRank {
        let kind = if self.tombstone {
            VersionKind::Tombstone
        } else if self.erasure_coded {
            VersionKind::ErasureCoded
        } else {
            VersionKind::Live
        };
        VersionRank::new(self.hlc.as_tuple(), kind, &self.etag)
    }

    /// What the replica holds, as anti-entropy and hint delivery weigh it.
    fn holds(&self) -> ReplicaHolds {
        ReplicaHolds::Version {
            hlc: self.hlc,
            tombstone: self.tombstone,
            erasure_coded: self.erasure_coded,
            etag: self.rank().etag,
        }
    }

    fn same_object_version(&self, other: &Self) -> bool {
        self.hlc == other.hlc
            && self.tombstone == other.tombstone
            && self.etag == other.etag
            && self.size == other.size
            && self.version_id == other.version_id
    }
}

/// What a replica's answer to a HEAD says it holds.
///
/// An answer that carries an error is an error, never an absence: a replica
/// whose storage failed reports `exists: false` beside the error.
fn digest_of_head(node_id: NodeId, response: proto::HeadObjectResponse) -> DigestResult {
    let hlc = response.hlc.as_ref().map(HlcTimestamp::from_proto);
    if response.exists {
        return DigestResult::Success(ObjectDigest {
            node_id,
            hlc: hlc.unwrap_or_else(HlcTimestamp::zero),
            etag: response.etag,
            size: response.content_length,
            content_type: response.content_type,
            version_id: response.version_id,
            erasure_coded: response.erasure_coded,
            metadata: response.metadata,
            content_hash: content_hash_from_bytes(&response.content_hash),
            tombstone: false,
        });
    }
    if response.epoch_mismatch {
        return DigestResult::EpochMismatch(node_id);
    }
    if response.tombstone {
        return match hlc {
            Some(hlc) => DigestResult::Success(ObjectDigest::of_tombstone(node_id, hlc)),
            None => DigestResult::Error(node_id, "a tombstone reported without an HLC".into()),
        };
    }
    if !response.error.is_empty() {
        return DigestResult::Error(node_id, response.error);
    }
    DigestResult::NotFound(
        node_id,
        response.tombstone_horizon.as_ref().map(HlcTimestamp::from_proto),
    )
}

/// The versions a quorum read collected, and how many replicas answered
/// that they hold nothing for the key.
#[derive(Debug, Default)]
struct CollectedDigests {
    digests: Vec<ObjectDigest>,
    absent: usize,
}

impl CollectedDigests {
    fn record(&mut self, bucket: &str, key: &str, result: DigestResult) {
        match result {
            DigestResult::Success(digest) => {
                debug!(
                    bucket, key, node_id = %digest.node_id, hlc = ?digest.hlc,
                    tombstone = digest.tombstone, content_hash = ?digest.content_hash,
                    "digest replica returned a version"
                );
                self.digests.push(digest);
            }
            DigestResult::NotFound(node_id, horizon) => {
                self.absent += 1;
                debug!(
                    bucket, key, %node_id, ?horizon,
                    "object not found on replica during digest collection"
                );
            }
            DigestResult::Timeout(node_id) => {
                warn!(bucket, key, %node_id, "digest request timed out");
            }
            DigestResult::Error(node_id, err) => {
                warn!(bucket, key, %node_id, error = %err, "digest request failed");
            }
            DigestResult::EpochMismatch(node_id) => {
                warn!(bucket, key, %node_id, "epoch mismatch during digest collection");
            }
        }
    }
}

/// The digests of a read decided to serve an object, and the index of its
/// newest version among them.
struct ServedDigests {
    digests: Vec<ObjectDigest>,
    winner: usize,
}

/// What the versions a quorum read collected decide.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReadDecision {
    /// The newest version, at this index, is an object: serve it.
    Serve(usize),
    /// The key holds no object: the newest version is the tombstone at this
    /// index, or no replica that answered holds a version.
    NotFound(Option<usize>),
    /// Too few replicas answered to decide.
    QuorumNotMet,
}

/// Decides a quorum read over `digests`, with `absent` more replicas holding
/// nothing for the key. The newest version is the one ranked highest
/// ([`VersionRank`]), as the replicas' own LWW ranks them.
///
/// A newest version that is an object is served once `read_quorum` replicas
/// hold a version. A replica that holds nothing does not count toward that:
/// it may have taken a delete from before tombstones, which left nothing to
/// weigh. A newest version that is a tombstone, or no version at all, is
/// "not found" once `read_quorum` replicas answered: an object written after
/// the delete by a write quorum would be on one of them.
fn decide(digests: &[ObjectDigest], absent: usize, read_quorum: u8) -> ReadDecision {
    let read_quorum = usize::from(read_quorum);
    let newest = digests.iter().enumerate().max_by_key(|(_, digest)| digest.rank());
    match newest.map(|(newest, _)| newest) {
        Some(newest) if !digests[newest].tombstone => {
            if digests.len() >= read_quorum {
                ReadDecision::Serve(newest)
            } else {
                ReadDecision::QuorumNotMet
            }
        }
        newest if digests.len() + absent >= read_quorum => ReadDecision::NotFound(newest),
        _ => ReadDecision::QuorumNotMet,
    }
}

/// Result of a quorum read operation.
#[derive(Debug, Clone)]
pub struct QuorumReadResult {
    /// Object data.
    pub data: Vec<u8>,
    /// ETag of the returned object.
    pub etag: String,
    /// Content type.
    pub content_type: String,
    /// Object size in bytes.
    pub size: u64,
    /// User-defined metadata.
    pub metadata: HashMap<String, String>,
    /// HLC timestamp of the returned version.
    pub hlc: HlcTimestamp,
    /// Version ID (if versioning enabled).
    pub version_id: Option<String>,
    /// Number of replicas that responded.
    pub replicas_responded: u8,
    /// Whether a read repair was triggered.
    pub repair_triggered: bool,
}

/// Result of a quorum HEAD operation.
#[derive(Debug, Clone)]
pub struct QuorumHeadResult {
    /// ETag of the object.
    pub etag: String,
    /// Content type.
    pub content_type: String,
    /// Object size in bytes.
    pub size: u64,
    /// User-defined metadata.
    pub metadata: HashMap<String, String>,
    /// HLC timestamp.
    pub hlc: HlcTimestamp,
    /// Version ID.
    pub version_id: Option<String>,
    /// SHA-256 of the object content, when the winning replica reported one.
    ///
    /// A node too old to send it leaves this empty, so a caller comparing
    /// hashes must treat `None` as "unknown", never as a mismatch.
    pub content_hash_sha256: Option<[u8; 32]>,
    /// True when the winning record describes an erasure-coded object.
    ///
    /// Such a record carries the object's metadata but none of its bytes: the
    /// data lives as shards in the pool. A caller must not read it as a hot
    /// replica, and must not mistake it for a newer overwrite of one.
    pub erasure_coded: bool,
    /// Number of replicas that responded.
    pub replicas_responded: u8,
}

/// Result of a quorum streaming read.
pub struct QuorumReadStreamResult {
    /// Streaming body from the selected replica.
    pub body: Box<dyn tokio::io::AsyncRead + Unpin + Send>,
    /// ETag of the selected object version.
    pub etag: String,
    /// Content type.
    pub content_type: String,
    /// User-defined metadata.
    pub metadata: HashMap<String, String>,
    /// HLC timestamp of the selected version.
    pub hlc: HlcTimestamp,
    /// Version ID (if versioning enabled).
    pub version_id: Option<String>,
    /// Full object size.
    pub total_size: u64,
    /// Response body size; lower than total when range is set.
    pub content_length: u64,
    /// Content range `(start, end, total)` for partial reads.
    pub content_range: Option<(u64, u64, u64)>,
    /// Number of replicas that responded during digest quorum.
    pub replicas_responded: u8,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct StreamSourceKey {
    node_id: NodeId,
    bucket: String,
    key: String,
}

#[derive(Clone, Default)]
struct StreamFailureTracker {
    entries: Arc<Mutex<HashMap<StreamSourceKey, Instant>>>,
}

impl StreamFailureTracker {
    const QUARANTINE_TTL: Duration = Duration::from_secs(300);

    fn is_quarantined(&self, node_id: NodeId, bucket: &str, key: &str) -> bool {
        let source = StreamSourceKey {
            node_id,
            bucket: bucket.to_string(),
            key: key.to_string(),
        };
        let now = Instant::now();

        let Ok(mut entries) = self.entries.lock() else {
            return false;
        };

        match entries.get(&source).copied() {
            Some(until) if until > now => true,
            Some(_) => {
                entries.remove(&source);
                false
            }
            None => false,
        }
    }

    fn mark_failed(&self, source: StreamSourceKey) {
        let Ok(mut entries) = self.entries.lock() else {
            return;
        };
        entries.insert(source, Instant::now() + Self::QUARANTINE_TTL);
    }
}

struct FailureMarkingReader {
    inner: Box<dyn AsyncRead + Unpin + Send>,
    tracker: StreamFailureTracker,
    source: StreamSourceKey,
    marked: bool,
}

impl FailureMarkingReader {
    fn new(
        inner: Box<dyn AsyncRead + Unpin + Send>,
        tracker: StreamFailureTracker,
        source: StreamSourceKey,
    ) -> Self {
        Self {
            inner,
            tracker,
            source,
            marked: false,
        }
    }
}

impl AsyncRead for FailureMarkingReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match Pin::new(&mut self.inner).poll_read(cx, buf) {
            Poll::Ready(Err(err)) => {
                if !self.marked {
                    self.marked = true;
                    self.tracker.mark_failed(self.source.clone());
                    warn!(
                        node_id = %self.source.node_id,
                        bucket = %self.source.bucket,
                        key = %self.source.key,
                        error = %err,
                        "stream body read failed; temporarily quarantining replica for this object"
                    );
                }
                Poll::Ready(Err(err))
            }
            other => other,
        }
    }
}

/// Timeout configuration for read operations.
#[derive(Debug, Clone)]
pub struct ReadTimeouts {
    /// Maximum time to wait for R replica digest responses.
    pub quorum_timeout: Duration,
    /// Maximum time for a single replica read.
    pub replica_read_timeout: Duration,
}

impl Default for ReadTimeouts {
    fn default() -> Self {
        Self {
            quorum_timeout: Duration::from_secs(5),
            replica_read_timeout: Duration::from_secs(10),
        }
    }
}

/// Why a read's selected source was refused: it took another version after
/// its digest was selected.
const SOURCE_CHANGED: &str = "read source changed since its digest was selected";

/// How many times a read selects its version anew when the source changes
/// under it — a write of the key landing between the digests and the fetch.
const READ_ATTEMPTS: usize = 3;

fn source_changed() -> ClusterError {
    ClusterError::Unavailable(SOURCE_CHANGED.into())
}

fn is_source_changed(error: &ClusterError) -> bool {
    matches!(error, ClusterError::Unavailable(why) if why == SOURCE_CHANGED)
}

/// Before protocol 5 ReadBlob omitted the ETag. Bind its bytes and complete
/// metadata to a HEAD of the same source before taking that opaque ETag.
/// Insufficient evidence or a write between the requests requires reselection.
fn legacy_read_etag(
    read: &proto::ReadBlobResponse,
    head: &proto::HeadObjectResponse,
) -> Result<String, ClusterError> {
    use sha2::{Digest, Sha256};

    let Some(hlc) = read.hlc.as_ref() else {
        return Err(source_changed());
    };
    if !read.success
        || !read.error.is_empty()
        || read.epoch_mismatch
        || read.tombstone
        || !head.exists
        || !head.error.is_empty()
        || head.epoch_mismatch
        || head.tombstone
        || head.erasure_coded
        || head.etag.is_empty()
        || hlc.wall_time == 0
        || hlc.node_id.len() != 16
        || head.hlc.as_ref() != Some(hlc)
        || read.data.len() as u64 != read.content_length
        || read.content_length != head.content_length
        || read.version_id != head.version_id
        || read.content_type != head.content_type
        || read.metadata != head.metadata
    {
        return Err(source_changed());
    }
    let hash: [u8; 32] = Sha256::digest(&read.data).into();
    if content_hash_from_bytes(&read.content_hash) != Some(hash)
        || content_hash_from_bytes(&head.content_hash) != Some(hash)
    {
        return Err(source_changed());
    }
    Ok(head.etag.clone())
}

/// The largest object a read repair fetches from its source: the local
/// target assembles the copy in memory before its last-writer-wins applies
/// it, as a remote receiver does. A larger stale copy is left to
/// anti-entropy and, in a growing pool, to the move.
pub(crate) const READ_REPAIR_FETCH_MAX_BYTES: u64 = 64 * 1024 * 1024;

/// Replicas that answered with a copy older than the winner, and replicas of
/// bucket metadata moving between two layouts that the read did not ask.
///
/// While the pool grows (`growing`), owners the read did not ask are not
/// among them: the move brings every key to its new owners at its own pace
/// and limits, and the verification brings what is still missing before a
/// range cuts over. A read repair for each of them on every read would copy
/// the object once per read and owner, past those limits.
fn copy_repair_targets(
    decision: &PlacementDecision,
    digests: &[ObjectDigest],
    winner: usize,
    growing: bool,
) -> Vec<NodeId> {
    let latest = &digests[winner];
    if latest.erasure_coded {
        return Vec::new();
    }
    let mut targets: Vec<_> = digests
        .iter()
        .filter(|digest| digest.rank() < latest.rank() && !digest.erasure_coded)
        .map(|digest| digest.node_id)
        .collect();
    for node in &decision.replicas {
        if !growing
            && !decision.read_replicas.contains(node)
            && !digests.iter().any(|digest| digest.node_id == *node)
            && !targets.contains(node)
        {
            targets.push(*node);
        }
    }
    targets
}

/// Quorum read coordinator.
///
/// Orchestrates reads across replicas using a digest-based two-phase
/// approach to minimize network I/O.
pub struct QuorumReadCoordinator {
    local_node_id: NodeId,
    storage: Arc<dyn StorageEngine>,
    rpc_client: Arc<NodeClient>,
    router: Arc<PlacementRouter>,
    clock: Arc<HybridClock>,
    read_repair: Arc<ReadRepairService>,
    timeouts: ReadTimeouts,
    topology_epoch: u64,
    stream_failures: StreamFailureTracker,
    /// Runs once between the digests and the fetch of the next read: a write
    /// landing there, for tests of the race.
    #[cfg(test)]
    before_fetch: std::sync::Mutex<Option<futures::future::BoxFuture<'static, ()>>>,
}

impl QuorumReadCoordinator {
    /// Create a new quorum read coordinator.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        local_node_id: NodeId,
        storage: Arc<dyn StorageEngine>,
        rpc_client: Arc<NodeClient>,
        router: Arc<PlacementRouter>,
        clock: Arc<HybridClock>,
        read_repair: Arc<ReadRepairService>,
        cluster_config: &ClusterConfig,
        timeouts: ReadTimeouts,
    ) -> Self {
        Self {
            local_node_id,
            storage,
            rpc_client,
            router,
            clock,
            read_repair,
            timeouts,
            topology_epoch: cluster_config.epoch,
            stream_failures: StreamFailureTracker::default(),
            #[cfg(test)]
            before_fetch: std::sync::Mutex::new(None),
        }
    }

    /// Runs `step` once, between the digests and the fetch of the next read.
    #[cfg(test)]
    pub(crate) fn before_next_fetch(&self, step: futures::future::BoxFuture<'static, ()>) {
        *self.before_fetch.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(step);
    }

    #[cfg(test)]
    async fn run_before_fetch(&self) {
        let step = self
            .before_fetch
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(step) = step {
            step.await;
        }
    }

    #[cfg(not(test))]
    async fn run_before_fetch(&self) {}

    /// The replicas a read of `decision` asks: its read replicas that take
    /// the key's class ([`super::takes_class`]).
    fn asked(&self, decision: &PlacementDecision) -> Vec<NodeId> {
        decision
            .read_replicas
            .iter()
            .copied()
            .filter(|node| {
                super::takes_class(&self.rpc_client, self.local_node_id, *node, decision.class)
            })
            .collect()
    }

    /// Execute a quorum read (GET object).
    ///
    /// Uses digest-based two-phase read: first HEAD from R replicas,
    /// then full GET from the replica with the latest version.
    pub async fn read(&self, bucket: &str, key: &str) -> Result<QuorumReadResult, ClusterError> {
        observe_quorum(QuorumOperation::Read, self.read_inner(bucket, key)).await
    }

    /// A read whose selected source took another version before its bytes
    /// were fetched selects its version anew: the key was overwritten during
    /// the read, and the newer version is as good an answer.
    async fn read_inner(&self, bucket: &str, key: &str) -> Result<QuorumReadResult, ClusterError> {
        let mut attempt = 1;
        loop {
            match self.read_once(bucket, key).await {
                Err(e) if is_source_changed(&e) && attempt < READ_ATTEMPTS => {
                    debug!(%bucket, %key, attempt, "the read's source changed; selecting again");
                    attempt += 1;
                }
                outcome => return outcome,
            }
        }
    }

    async fn read_once(&self, bucket: &str, key: &str) -> Result<QuorumReadResult, ClusterError> {
        let decision = self.router.route(bucket, key)?;
        let replicas = &decision.read_replicas;

        // Single-node bypass: read directly, no quorum needed
        if replicas.len() == 1 && replicas[0] == self.local_node_id {
            return self.read_single_node(bucket, key).await;
        }
        let replicas = &self.asked(&decision);

        // Phase 1: Collect digests from all replicas, and find the latest
        // version by HLC (LWW conflict resolution)
        let collected = self.collect_digests(bucket, key, replicas, decision.read_quorum).await?;
        let digests = collected.digests;
        let winner_idx = match decide(&digests, collected.absent, decision.read_quorum) {
            ReadDecision::Serve(winner_idx) => winner_idx,
            ReadDecision::NotFound(tombstone) => {
                if let Some(tombstone) = tombstone {
                    self.clock.observe(&digests[tombstone].hlc);
                    self.repair_stale_copies(bucket, key, &digests, tombstone, &decision);
                }
                return Err(ClusterError::ObjectNotFound {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                });
            }
            ReadDecision::QuorumNotMet => {
                return Err(ClusterError::QuorumNotMet {
                    needed: decision.read_quorum,
                    achieved: digests.len() as u8,
                })
            }
        };
        let latest = &digests[winner_idx];

        // Detect stale replicas for read repair; a tombstone older than the
        // object is one of them
        let repair_nodes = copy_repair_targets(
            &decision,
            &digests,
            winner_idx,
            self.router.pool_generation_moving(&decision.pool_id),
        );
        let repair_triggered = !repair_nodes.is_empty();

        self.run_before_fetch().await;
        // Phase 2: Fetch full data from the node with the latest version.
        // If the winner has corrupted/unavailable data, fall back to other
        // replicas that reported the same HLC (e.g. bit-rot on one node).
        let (data, full_metadata) = {
            let candidates = candidate_order(&digests, winner_idx, self.local_node_id);
            let byteless = digests.iter().filter(|digest| digest.erasure_coded).count();

            let mut last_err = None;
            let mut changed = false;
            let mut result = None;
            for idx in candidates {
                match self.fetch_selected_object(bucket, key, &digests[idx]).await {
                    Ok(r) => {
                        result = Some(r);
                        break;
                    }
                    Err(e) => {
                        changed |= is_source_changed(&e);
                        warn!(
                            bucket = %bucket,
                            key = %key,
                            node_id = %digests[idx].node_id,
                            error = %e,
                            "fetch from replica failed, trying next"
                        );
                        last_err = Some(e);
                    }
                }
            }
            match result {
                Some(fetched) => fetched,
                // Every replica answered the HEAD and none of them holds the
                // bytes: the object lives in an erasure-coded pool and has to
                // be read through the EC runtime. Reported as itself, because
                // the alternatives — "not found" or a panic on an empty
                // candidate list — both describe an object that is intact.
                None if byteless > 0 && last_err.is_none() => {
                    return Err(ClusterError::ErasureCodedOnly {
                        bucket: bucket.to_string(),
                        key: key.to_string(),
                        replicas: byteless,
                    })
                }
                None if changed => return Err(source_changed()),
                None => {
                    return Err(last_err.unwrap_or_else(|| ClusterError::ObjectNotFound {
                        bucket: bucket.to_string(),
                        key: key.to_string(),
                    }))
                }
            }
        };

        // The source may have served a version newer than the one selected.
        let served;
        let latest = if HlcTimestamp::of_record(&full_metadata) == latest.hlc
            && full_metadata.etag == latest.etag
        {
            latest
        } else {
            served = ObjectDigest::of_record(latest.node_id, full_metadata.clone());
            &served
        };

        // Phase 3: Trigger async read repair for stale replicas
        if repair_triggered {
            for target_node in repair_nodes {
                self.read_repair.submit(RepairTask {
                    target_node,
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                    hlc: latest.hlc,
                    topology_epoch: self.topology_epoch,
                    repair: Repair::Copy {
                        data: data.clone(),
                        etag: latest.etag.clone(),
                        content_hash: full_metadata.content_hash.to_vec(),
                        content_type: full_metadata.content_type.clone(),
                        metadata: full_metadata.metadata.clone(),
                        version_id: full_metadata.version_id.clone(),
                    },
                });
            }
        }

        // Observe the latest HLC to advance our local clock
        self.clock.observe(&latest.hlc);

        Ok(QuorumReadResult {
            data,
            etag: latest.etag.clone(),
            content_type: full_metadata.content_type,
            size: latest.size,
            metadata: full_metadata.metadata,
            hlc: latest.hlc,
            version_id: latest.version_id.clone(),
            replicas_responded: digests.len() as u8,
            repair_triggered,
        })
    }

    /// Execute a quorum read and return a streaming body from the selected replica.
    ///
    /// This keeps memory bounded for server-side copy and other internal data
    /// paths that need bytes but do not need the whole object materialized.
    pub async fn read_stream(
        &self,
        bucket: &str,
        key: &str,
        range: Option<(u64, u64)>,
    ) -> Result<QuorumReadStreamResult, ClusterError> {
        observe_quorum(
            QuorumOperation::Read,
            self.read_stream_inner(bucket, key, range),
        )
        .await
    }

    /// As [`Self::read_inner`]: a stream whose selected source changed
    /// selects its version anew.
    async fn read_stream_inner(
        &self,
        bucket: &str,
        key: &str,
        range: Option<(u64, u64)>,
    ) -> Result<QuorumReadStreamResult, ClusterError> {
        let mut attempt = 1;
        loop {
            match self.read_stream_once(bucket, key, range).await {
                Err(e) if is_source_changed(&e) && attempt < READ_ATTEMPTS => {
                    debug!(%bucket, %key, attempt, "the stream's source changed; selecting again");
                    attempt += 1;
                }
                outcome => return outcome,
            }
        }
    }

    async fn read_stream_once(
        &self,
        bucket: &str,
        key: &str,
        range: Option<(u64, u64)>,
    ) -> Result<QuorumReadStreamResult, ClusterError> {
        let decision = self.router.route(bucket, key)?;
        let replicas = &decision.read_replicas;

        if replicas.len() == 1 && replicas[0] == self.local_node_id {
            return self.open_stream_from_replica(bucket, key, self.local_node_id, range).await;
        }

        let digests = self.decided_digests(bucket, key, &decision).await?;
        let (digests, winner_idx) = (digests.digests, digests.winner);

        let candidates = candidate_order(&digests, winner_idx, self.local_node_id);
        self.run_before_fetch().await;

        let skipped_erasure_coded = digests.iter().filter(|digest| digest.erasure_coded).count();
        let mut last_err = None;
        let mut changed = false;
        let mut skipped_quarantined = false;
        for idx in candidates {
            let node_id = digests[idx].node_id;
            if self.stream_failures.is_quarantined(node_id, bucket, key) {
                skipped_quarantined = true;
                warn!(
                    bucket = %bucket,
                    key = %key,
                    node_id = %node_id,
                    "skipping temporarily quarantined stream source"
                );
                continue;
            }

            match self.open_selected_stream(bucket, key, &digests[idx], range).await {
                Ok(mut stream) => {
                    // A newer version than the one selected keeps what its
                    // source said of it; the selected one is described as the
                    // digests say.
                    let selected =
                        stream.hlc == digests[idx].hlc && stream.etag == digests[idx].etag;
                    if selected {
                        stream.content_type = digests[idx].content_type.clone();
                        stream.metadata = digests[idx].metadata.clone();
                        stream.version_id = digests[idx].version_id.clone();
                    }
                    stream.replicas_responded = digests.len() as u8;
                    stream.body = Box::new(FailureMarkingReader::new(
                        stream.body,
                        self.stream_failures.clone(),
                        StreamSourceKey {
                            node_id,
                            bucket: bucket.to_string(),
                            key: key.to_string(),
                        },
                    ));
                    self.clock.observe(&stream.hlc);
                    if selected {
                        self.repair_from_source(bucket, key, &decision, &digests, idx);
                    }
                    return Ok(stream);
                }
                Err(e) => {
                    changed |= is_source_changed(&e);
                    if is_integrity_error(&e) {
                        self.stream_failures.mark_failed(StreamSourceKey {
                            node_id,
                            bucket: bucket.to_string(),
                            key: key.to_string(),
                        });
                    }
                    warn!(
                        bucket = %bucket,
                        key = %key,
                        node_id = %node_id,
                        error = %e,
                        "stream open from replica failed, trying next"
                    );
                    last_err = Some(e);
                }
            }
        }

        if changed {
            return Err(source_changed());
        }
        if skipped_quarantined && last_err.is_none() {
            return Err(ClusterError::Storage(format!(
                "all candidate stream sources are temporarily quarantined for {bucket}/{key}"
            )));
        }
        // Every replica holds a namespace entry and none of them the bytes:
        // the object lives in an erasure-coded pool and has to be read through
        // the EC runtime. Saying so beats the "not found" below, which would
        // send the caller looking for an object that is perfectly intact.
        if skipped_erasure_coded > 0 && last_err.is_none() {
            return Err(ClusterError::ErasureCodedOnly {
                bucket: bucket.to_string(),
                key: key.to_string(),
                replicas: skipped_erasure_coded,
            });
        }

        Err(last_err.unwrap_or_else(|| ClusterError::ObjectNotFound {
            bucket: bucket.to_string(),
            key: key.to_string(),
        }))
    }

    /// The nodes that hold this object's replicas, without reading anything.
    ///
    /// The same placement decision every read makes, exposed on its own so that
    /// a caller looking for metadata written beside an object can address the
    /// nodes that hold it instead of asking the whole pool. Answering "who has
    /// this object" is a computation over the ring, not an I/O.
    ///
    /// # Errors
    ///
    /// Returns [`ClusterError::Placement`] when the bucket belongs to no pool.
    pub fn replica_nodes(&self, bucket: &str, key: &str) -> Result<Vec<NodeId>, ClusterError> {
        Ok(self.router.route(bucket, key)?.replicas)
    }

    /// Execute a quorum HEAD (metadata only, no data transfer).
    pub async fn head(&self, bucket: &str, key: &str) -> Result<QuorumHeadResult, ClusterError> {
        observe_quorum(QuorumOperation::Head, self.head_inner(bucket, key)).await
    }

    pub(super) async fn head_inner(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<QuorumHeadResult, ClusterError> {
        let decision = self.router.route(bucket, key)?;
        let replicas = &decision.read_replicas;

        // Single-node bypass
        if replicas.len() == 1 && replicas[0] == self.local_node_id {
            return self.head_single_node(bucket, key).await;
        }
        let digests = self.decided_digests(bucket, key, &decision).await?;
        let latest = &digests.digests[digests.winner];
        self.repair_from_source(bucket, key, &decision, &digests.digests, digests.winner);

        self.clock.observe(&latest.hlc);

        Ok(QuorumHeadResult {
            etag: latest.etag.clone(),
            content_type: latest.content_type.clone(),
            size: latest.size,
            metadata: latest.metadata.clone(),
            hlc: latest.hlc,
            version_id: latest.version_id.clone(),
            content_hash_sha256: latest.content_hash,
            erasure_coded: latest.erasure_coded,
            replicas_responded: digests.digests.len() as u8,
        })
    }

    /// Collects the digests of `bucket/key` and decides the read, scheduling
    /// tombstone repair during a moving generation: the newest object to serve,
    /// `ObjectNotFound` when it is a tombstone or there is none, and
    /// `QuorumNotMet` when fewer than `read_quorum` replicas answered.
    async fn decided_digests(
        &self,
        bucket: &str,
        key: &str,
        decision: &PlacementDecision,
    ) -> Result<ServedDigests, ClusterError> {
        let read_quorum = decision.read_quorum;
        let replicas = self.asked(decision);
        let collected = self.collect_digests(bucket, key, &replicas, read_quorum).await?;
        match decide(&collected.digests, collected.absent, read_quorum) {
            ReadDecision::Serve(winner) => Ok(ServedDigests {
                digests: collected.digests,
                winner,
            }),
            ReadDecision::NotFound(tombstone) => {
                if let Some(tombstone) = tombstone {
                    self.clock.observe(&collected.digests[tombstone].hlc);
                    if self.router.pool_generation_moving(&decision.pool_id) {
                        self.repair_stale_copies(
                            bucket,
                            key,
                            &collected.digests,
                            tombstone,
                            decision,
                        );
                    }
                }
                Err(ClusterError::ObjectNotFound {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                })
            }
            ReadDecision::QuorumNotMet => Err(ClusterError::QuorumNotMet {
                needed: read_quorum,
                achieved: collected.digests.len() as u8,
            }),
        }
    }

    fn repair_from_source(
        &self,
        bucket: &str,
        key: &str,
        decision: &PlacementDecision,
        digests: &[ObjectDigest],
        winner: usize,
    ) {
        let latest = &digests[winner];
        if !self.router.pool_generation_moving(&decision.pool_id)
            || latest.erasure_coded
            || latest.size > READ_REPAIR_FETCH_MAX_BYTES
        {
            return;
        }
        for target_node in copy_repair_targets(decision, digests, winner, true) {
            self.read_repair.submit(RepairTask {
                target_node,
                bucket: bucket.to_string(),
                key: key.to_string(),
                hlc: latest.hlc,
                topology_epoch: self.topology_epoch,
                repair: Repair::Fetch {
                    source_node: latest.node_id,
                    etag: latest.etag.clone(),
                },
            });
        }
    }

    /// Brings every replica whose digest shows a copy older than the
    /// tombstone at `tombstone` to the delete. An erasure-coded record is
    /// left alone: its shards are the EC runtime's to remove.
    fn repair_stale_copies(
        &self,
        bucket: &str,
        key: &str,
        digests: &[ObjectDigest],
        tombstone: usize,
        decision: &PlacementDecision,
    ) {
        let deleted = digests[tombstone].hlc;
        let deleted_rank = digests[tombstone].rank();
        let mut targets: Vec<_> = digests
            .iter()
            .filter(|digest| {
                !digest.tombstone && !digest.erasure_coded && digest.rank() < deleted_rank
            })
            .map(|digest| (digest.node_id, digest.hlc))
            .collect();
        let growing = self.router.pool_generation_moving(&decision.pool_id);
        for node in &decision.replicas {
            if !growing
                && !decision.read_replicas.contains(node)
                && !digests.iter().any(|digest| digest.node_id == *node)
                && !targets.iter().any(|(target, _)| target == node)
            {
                targets.push((*node, HlcTimestamp::zero()));
            }
        }
        for (target_node, stale) in targets {
            self.read_repair.submit(RepairTask {
                target_node,
                bucket: bucket.to_string(),
                key: key.to_string(),
                hlc: deleted,
                topology_epoch: self.topology_epoch,
                repair: Repair::Delete { stale },
            });
        }
    }

    /// Single-node read bypass.
    async fn read_single_node(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<QuorumReadResult, ClusterError> {
        let (data, record) = self.storage.get_object(bucket, key).await.map_err(|e| match e {
            s4_core::StorageError::ObjectNotFound { .. } => ClusterError::ObjectNotFound {
                bucket: bucket.to_string(),
                key: key.to_string(),
            },
            other => ClusterError::Storage(other.to_string()),
        })?;

        let hlc = HlcTimestamp::of_record(&record);
        // As a quorum read does: what this node writes after the read is
        // newer than what it read, though another node's coordinator wrote it.
        self.clock.observe(&hlc);

        debug!(
            bucket = %bucket,
            key = %key,
            etag = %record.etag,
            "single-node read completed"
        );

        Ok(QuorumReadResult {
            data,
            etag: record.etag,
            content_type: record.content_type,
            size: record.size,
            metadata: record.metadata,
            hlc,
            version_id: record.version_id,
            replicas_responded: 1,
            repair_triggered: false,
        })
    }

    /// Single-node HEAD bypass.
    async fn head_single_node(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<QuorumHeadResult, ClusterError> {
        let record = self.storage.head_object(bucket, key).await.map_err(|e| match e {
            s4_core::StorageError::ObjectNotFound { .. } => ClusterError::ObjectNotFound {
                bucket: bucket.to_string(),
                key: key.to_string(),
            },
            other => ClusterError::Storage(other.to_string()),
        })?;

        let hlc = HlcTimestamp::of_record(&record);
        self.clock.observe(&hlc);

        let erasure_coded = record.is_erasure_coded();
        Ok(QuorumHeadResult {
            content_hash_sha256: Some(record.content_hash),
            etag: record.etag,
            content_type: record.content_type,
            size: record.size,
            erasure_coded,
            metadata: record.metadata,
            hlc,
            version_id: record.version_id,
            replicas_responded: 1,
        })
    }

    /// Phase 1: Collect digests (HEAD) from all replicas, until they decide
    /// a read of `read_quorum` ([`decide`]) or every replica answered.
    async fn collect_digests(
        &self,
        bucket: &str,
        key: &str,
        replicas: &[NodeId],
        read_quorum: u8,
    ) -> Result<CollectedDigests, ClusterError> {
        debug!(
            coordinator = %self.local_node_id,
            epoch = self.topology_epoch,
            bucket,
            key,
            ?replicas,
            needed = read_quorum,
            "digest collection started"
        );
        // Own the actual replica futures. Wrapping already-spawned JoinHandles
        // would detach those tasks when this quorum read is cancelled.
        let mut set = tokio::task::JoinSet::new();
        for &node_id in replicas {
            set.spawn(self.digest_task(node_id, bucket, key));
        }

        // Wait until the answers decide the read, with timeout
        let mut collected = CollectedDigests::default();
        let deadline = tokio::time::Instant::now() + self.timeouts.quorum_timeout;

        loop {
            tokio::select! {
                maybe_result = set.join_next() => {
                    match maybe_result {
                        Some(Ok(result)) => collected.record(bucket, key, result),
                        Some(Err(e)) => {
                            error!(bucket, key, error = %e, "join error in digest collection");
                        }
                        None => break,
                    }
                    if decide(&collected.digests, collected.absent, read_quorum)
                        != ReadDecision::QuorumNotMet
                    {
                        // Decided — drain remaining responses briefly to
                        // improve LWW conflict resolution accuracy when
                        // concurrent writes are in flight.
                        let drain_until = digest_drain_deadline(deadline);
                        while let Ok(Some(result)) =
                            tokio::time::timeout_at(drain_until, set.join_next()).await
                        {
                            if let Ok(result) = result {
                                collected.record(bucket, key, result);
                            }
                        }
                        break;
                    }
                }
                _ = tokio::time::sleep_until(deadline) => {
                    warn!(
                        bucket, key,
                        got = collected.digests.len(),
                        absent = collected.absent,
                        needed = read_quorum,
                        "digest collection timed out"
                    );
                    break;
                }
            }
        }

        debug!(
            bucket,
            key,
            versions = collected.digests.len(),
            not_found = collected.absent,
            needed = read_quorum,
            "digest collection completed"
        );
        Ok(collected)
    }

    /// What replica `node_id` holds of `bucket/key`, as a task that owns
    /// everything it needs.
    fn digest_task(
        &self,
        node_id: NodeId,
        bucket: &str,
        key: &str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = DigestResult> + Send + 'static>> {
        if node_id == self.local_node_id {
            let storage = self.storage.clone();
            let bucket = bucket.to_string();
            let key = key.to_string();
            let local_id = self.local_node_id;
            let timeout_dur = self.timeouts.replica_read_timeout;

            Box::pin(async move {
                let result =
                    tokio::time::timeout(timeout_dur, storage.key_version(&bucket, &key)).await;

                match result {
                    Ok(Ok(KeyVersion::Live(record))) => {
                        DigestResult::Success(ObjectDigest::of_record(local_id, record))
                    }
                    Ok(Ok(KeyVersion::Tombstone(record))) => DigestResult::Success(
                        ObjectDigest::of_tombstone(local_id, HlcTimestamp::of_record(&record)),
                    ),
                    Ok(Ok(KeyVersion::Absent)) => match storage.tombstone_horizon().await {
                        Ok(horizon) => {
                            DigestResult::NotFound(local_id, horizon.map(HlcTimestamp::from))
                        }
                        Err(e) => DigestResult::Error(local_id, e.to_string()),
                    },
                    Ok(Err(e)) => DigestResult::Error(local_id, e.to_string()),
                    Err(_) => DigestResult::Timeout(local_id),
                }
            })
        } else {
            let client = self.rpc_client.clone();
            let request = proto::HeadObjectRequest {
                bucket: bucket.to_string(),
                key: key.to_string(),
                version_id: None,
                topology_epoch: self.topology_epoch,
            };
            let timeout_dur = self.timeouts.replica_read_timeout;

            Box::pin(async move {
                let result =
                    tokio::time::timeout(timeout_dur, client.send_head(node_id, request)).await;

                match result {
                    Ok(Ok(resp)) => digest_of_head(node_id, resp),
                    Ok(Err(e)) => DigestResult::Error(node_id, e.to_string()),
                    Err(_) => DigestResult::Timeout(node_id),
                }
            })
        }
    }

    /// The newest version of `bucket/key` among its replicas other than
    /// `target`, for hint delivery.
    ///
    /// Unlike a quorum read it asks every one of those replicas and needs
    /// every answer: a replica that did not answer may hold the newest
    /// version, and taking the others for the current state would bring the
    /// target to an older one. One that gossip holds down is not asked at all,
    /// and makes the answer an error at once. It counts no quorum operation and
    /// repairs nothing — deciding who is behind is the caller's job.
    async fn newest_version_except(
        &self,
        bucket: &str,
        key: &str,
        target: NodeId,
    ) -> Result<NewestVersion, ClusterError> {
        let decision = self.router.route(bucket, key)?;
        if !decision.replicas.contains(&target) || decision.replicas.len() < 2 {
            return Ok(NewestVersion::NotAReplica);
        }
        let others: Vec<NodeId> =
            decision.replicas.iter().copied().filter(|node_id| *node_id != target).collect();
        if let Some(down) = others
            .iter()
            .find(|node_id| **node_id != self.local_node_id && self.rpc_client.is_down(**node_id))
        {
            return Err(ClusterError::Unavailable(format!("replica {down} is down")));
        }
        let (answers, unanswered) = self.replica_holdings(bucket, key, &others).await;
        if let Some((_, why)) = unanswered.into_iter().next() {
            return Err(ClusterError::Unavailable(why));
        }
        // A confirmed write not on the target is on this many of the others
        // that reads take: while bucket metadata moves, a write is confirmed
        // by the quorum of the layout reads take.
        let counted: Vec<NodeId> = others
            .iter()
            .copied()
            .filter(|node_id| decision.read_replicas.contains(node_id))
            .collect();
        let confirming = usize::from(decision.write_quorum).min(counted.len());
        Ok(reconcile::newest_of_among(&answers, confirming, &counted))
    }

    /// What every replica of `bucket/key` holds, for anti-entropy. A replica
    /// gossip holds down is not asked and is listed as not answering.
    async fn all_key_versions(&self, bucket: &str, key: &str) -> Result<KeyVersions, ClusterError> {
        let decision = self.router.route(bucket, key)?;
        let (up, down): (Vec<NodeId>, Vec<NodeId>) =
            decision.replicas.iter().copied().partition(|node_id| {
                *node_id == self.local_node_id || !self.rpc_client.is_down(*node_id)
            });
        let (answers, mut unanswered) = self.replica_holdings(bucket, key, &up).await;
        unanswered.extend(down.into_iter().map(|node| (node, format!("replica {node} is down"))));
        // A confirmed write that missed one replica is on this many others of
        // the layout reads take.
        let confirming = usize::from(decision.write_quorum)
            .min(decision.read_replicas.len().saturating_sub(1))
            .max(1);
        Ok(KeyVersions {
            replicas: decision.replicas,
            answers,
            unanswered,
            confirming,
            confirming_replicas: decision.read_replicas,
        })
    }

    /// [`Self::all_key_versions`], with `holder` asked as well when it is not
    /// a replica of the key: a former owner whose copy is to be handed over.
    /// Its copy is weighed with the others but never counts towards a
    /// confirmed write: placement no longer names it.
    pub(crate) async fn key_versions_with_holder(
        &self,
        bucket: &str,
        key: &str,
        holder: NodeId,
    ) -> Result<KeyVersions, ClusterError> {
        let mut versions = self.all_key_versions(bucket, key).await?;
        if !versions.replicas.contains(&holder) {
            let (answers, unanswered) = self.replica_holdings(bucket, key, &[holder]).await;
            versions.replicas.push(holder);
            versions.answers.extend(answers);
            versions.unanswered.extend(unanswered);
        }
        Ok(versions)
    }

    /// What each of `nodes` holds of `bucket/key`, asked all at once: the
    /// replicas that answered, and those that did not with why.
    ///
    /// Each replica has the replica read timeout to answer, and one that has
    /// not by then is listed as not answering: the answers of the others
    /// stand. A slow replica never costs the whole read, which would leave
    /// the replicas that did answer unsettled with it.
    async fn replica_holdings(
        &self,
        bucket: &str,
        key: &str,
        nodes: &[NodeId],
    ) -> (Vec<(NodeId, ReplicaHolds)>, Vec<(NodeId, String)>) {
        let mut set = tokio::task::JoinSet::new();
        for &node_id in nodes {
            set.spawn(self.digest_task(node_id, bucket, key));
        }
        // Every task gives up on its own after the replica read timeout; this
        // bounds one that does not.
        let deadline =
            tokio::time::Instant::now() + self.timeouts.replica_read_timeout + HOLDINGS_GRACE;
        let mut pending: Vec<NodeId> = nodes.to_vec();
        let mut answers = Vec::new();
        let mut unanswered = Vec::new();
        while let Ok(Some(answer)) = tokio::time::timeout_at(deadline, set.join_next()).await {
            // A task that failed leaves its replica pending.
            let Ok(result) = answer else {
                continue;
            };
            pending.retain(|node_id| *node_id != result.node_id());
            match result {
                DigestResult::Success(digest) => answers.push((digest.node_id, digest.holds())),
                // A replica that keeps no tombstones, or reports no horizon,
                // took deletes that left nothing to weigh.
                DigestResult::NotFound(node_id, horizon) => answers.push((
                    node_id,
                    ReplicaHolds::Nothing {
                        horizon: horizon.filter(|_| self.keeps_tombstones(node_id)),
                    },
                )),
                DigestResult::Timeout(node_id) => {
                    unanswered.push((node_id, format!("replica {node_id} did not answer in time")))
                }
                DigestResult::Error(node_id, error) => {
                    unanswered.push((node_id, format!("replica {node_id}: {error}")))
                }
                DigestResult::EpochMismatch(node_id) => unanswered.push((
                    node_id,
                    format!("replica {node_id} is on another topology epoch"),
                )),
            }
        }
        unanswered.extend(
            pending
                .into_iter()
                .map(|node_id| (node_id, format!("replica {node_id} did not answer in time"))),
        );
        (answers, unanswered)
    }

    /// Whether `node_id` keeps tombstones: this node does, a peer if it
    /// announced a protocol that has them. One that has announced none yet is
    /// taken for one that does not.
    fn keeps_tombstones(&self, node_id: NodeId) -> bool {
        node_id == self.local_node_id
            || self
                .rpc_client
                .protocol_version(node_id)
                .is_some_and(|version| version >= crate::protocol::TOMBSTONE_MIN_PROTOCOL_VERSION)
    }

    /// Phase 2: Fetch full object data from a specific replica.
    ///
    /// The source may have taken a newer version since its digest was
    /// selected — the key overwritten during the read. That version is served:
    /// the read would have selected it had it asked a moment later, as it
    /// selects the newest version among the answers. A version that is not
    /// newer than the one selected, yet another, is refused
    /// ([`source_changed`]), and so are bytes that do not match their record.
    async fn fetch_selected_object(
        &self,
        bucket: &str,
        key: &str,
        selected: &ObjectDigest,
    ) -> Result<(Vec<u8>, s4_core::IndexRecord), ClusterError> {
        let (data, record) = self.fetch_full_object(bucket, key, selected.node_id).await?;
        let same = HlcTimestamp::of_record(&record) == selected.hlc
            && record.etag == selected.etag
            && record.size == selected.size
            && record.version_id == selected.version_id;
        let newer = !record.is_erasure_coded()
            && VersionRank::new(
                HlcTimestamp::of_record(&record).as_tuple(),
                VersionKind::Live,
                &record.etag,
            ) > selected.rank();
        if !(same || newer) || data.len() as u64 != record.size {
            return Err(source_changed());
        }
        Ok((data, record))
    }

    async fn open_selected_stream(
        &self,
        bucket: &str,
        key: &str,
        selected: &ObjectDigest,
        range: Option<(u64, u64)>,
    ) -> Result<QuorumReadStreamResult, ClusterError> {
        let stream = self.open_stream_from_replica(bucket, key, selected.node_id, range).await?;
        let same = stream.hlc == selected.hlc
            && stream.etag == selected.etag
            && stream.total_size == selected.size
            && stream.version_id == selected.version_id;
        // A newer version the source took since its digest is served, as
        // `fetch_selected_object` does.
        let newer = VersionRank::new(stream.hlc.as_tuple(), VersionKind::Live, &stream.etag)
            > selected.rank();
        if !(same || newer) {
            return Err(source_changed());
        }
        Ok(stream)
    }

    async fn fetch_full_object(
        &self,
        bucket: &str,
        key: &str,
        node_id: NodeId,
    ) -> Result<(Vec<u8>, s4_core::IndexRecord), ClusterError> {
        if node_id == self.local_node_id {
            return self
                .storage
                .get_object(bucket, key)
                .await
                .map_err(|e| ClusterError::Storage(e.to_string()));
        }

        let request = proto::ReadBlobRequest {
            bucket: bucket.to_string(),
            key: key.to_string(),
            version_id: None,
            coordinator_id: self.local_node_id.to_bytes().to_vec(),
            operation_id: uuid::Uuid::new_v4().as_u128().to_be_bytes().to_vec(),
            topology_epoch: self.topology_epoch,
            range_start: None,
            range_end: None,
        };

        let deadline = tokio::time::Instant::now() + self.timeouts.replica_read_timeout;
        let mut resp =
            tokio::time::timeout_at(deadline, self.rpc_client.send_read(node_id, request))
                .await
                .map_err(|_| ClusterError::ReadTimeout(self.timeouts.replica_read_timeout))?
                .map_err(|e| ClusterError::Rpc(format!("full read from {node_id}: {e}")))?;

        if !resp.success {
            return Err(ClusterError::Storage(format!(
                "remote read from {node_id} failed: {}",
                resp.error
            )));
        }

        if resp.etag.is_empty() {
            let head = tokio::time::timeout_at(
                deadline,
                self.rpc_client.send_head(
                    node_id,
                    proto::HeadObjectRequest {
                        bucket: bucket.to_string(),
                        key: key.to_string(),
                        version_id: None,
                        topology_epoch: self.topology_epoch,
                    },
                ),
            )
            .await
            .map_err(|_| ClusterError::ReadTimeout(self.timeouts.replica_read_timeout))??;
            resp.etag = legacy_read_etag(&resp, &head)?;
            if tokio::time::Instant::now() >= deadline {
                return Err(ClusterError::ReadTimeout(
                    self.timeouts.replica_read_timeout,
                ));
            }
        }

        let hlc = resp
            .hlc
            .as_ref()
            .map(HlcTimestamp::from_proto)
            .unwrap_or_else(HlcTimestamp::zero);

        let content_hash: [u8; 32] = resp.content_hash.try_into().unwrap_or([0u8; 32]);

        let record = s4_core::IndexRecord {
            file_id: 0,
            offset: 0,
            size: resp.content_length,
            etag: resp.etag,
            content_type: resp.content_type,
            metadata: resp.metadata,
            content_hash,
            created_at: hlc.wall_time * 1_000_000, // millis → nanos (IndexRecord uses nanos)
            modified_at: hlc.wall_time * 1_000_000, // millis → nanos
            version_id: resp.version_id,
            is_delete_marker: false,
            retention_mode: None,
            retain_until_timestamp: None,
            legal_hold: false,
            layout: None,
            pool_id: 0,
            hlc_timestamp: hlc.wall_time,
            hlc_logical: hlc.logical,
            origin_node_id: hlc.node_id,
            operation_id: 0,
        };

        Ok((resp.data, record))
    }

    async fn open_stream_from_replica(
        &self,
        bucket: &str,
        key: &str,
        node_id: NodeId,
        range: Option<(u64, u64)>,
    ) -> Result<QuorumReadStreamResult, ClusterError> {
        if node_id == self.local_node_id {
            let object_stream = self
                .storage
                .open_object_stream(bucket, key, ReadOptions { range })
                .await
                .map_err(|e| match e {
                    s4_core::StorageError::ObjectNotFound { .. } => ClusterError::ObjectNotFound {
                        bucket: bucket.to_string(),
                        key: key.to_string(),
                    },
                    other => ClusterError::Storage(other.to_string()),
                })?;

            return Ok(QuorumReadStreamResult {
                hlc: HlcTimestamp::of_record(&object_stream.record),
                body: object_stream.body,
                etag: object_stream.record.etag,
                content_type: object_stream.record.content_type,
                metadata: object_stream.record.metadata,
                version_id: object_stream.record.version_id,
                total_size: object_stream.total_size,
                content_length: object_stream.content_length,
                content_range: object_stream.content_range,
                replicas_responded: 1,
            });
        }

        let request = proto::ReadBlobRequest {
            bucket: bucket.to_string(),
            key: key.to_string(),
            version_id: None,
            coordinator_id: self.local_node_id.to_bytes().to_vec(),
            operation_id: uuid::Uuid::new_v4().as_u128().to_be_bytes().to_vec(),
            topology_epoch: self.topology_epoch,
            range_start: range.map(|(start, _)| start),
            range_end: range.map(|(_, end)| end),
        };

        let mut stream = tokio::time::timeout(
            self.timeouts.replica_read_timeout,
            self.rpc_client.send_read_stream(node_id, request),
        )
        .await
        .map_err(|_| ClusterError::ReadTimeout(self.timeouts.replica_read_timeout))?
        .map_err(|e| ClusterError::Rpc(format!("stream read from {node_id}: {e}")))?;

        let first = tokio::time::timeout(self.timeouts.replica_read_timeout, stream.next())
            .await
            .map_err(|_| ClusterError::ReadTimeout(self.timeouts.replica_read_timeout))?
            .ok_or_else(|| ClusterError::Rpc(format!("stream read from {node_id}: empty stream")))?
            .map_err(|e| ClusterError::Rpc(format!("stream read from {node_id}: {e}")))?;

        let header = match first.payload {
            Some(proto::read_blob_chunk::Payload::Header(header)) => header,
            _ => {
                return Err(ClusterError::Rpc(format!(
                    "stream read from {node_id}: first message was not a header"
                )));
            }
        };

        let total_size = header.total_size;
        let content_length = header.content_length;
        let content_range = match (header.range_start, header.range_end) {
            (Some(start), Some(end)) => Some((start, end, total_size)),
            (None, None) => None,
            _ => {
                return Err(ClusterError::Rpc(format!(
                    "stream read from {node_id}: incomplete range metadata"
                )));
            }
        };

        let body_stream = stream.map(move |item| match item {
            Ok(chunk) => match chunk.payload {
                Some(proto::read_blob_chunk::Payload::Data(data)) => Ok(Bytes::from(data)),
                Some(proto::read_blob_chunk::Payload::Header(_)) => Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "duplicate ReadBlobStream header",
                )),
                None => Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "empty ReadBlobStream payload",
                )),
            },
            Err(status) => Err(io::Error::other(format!(
                "ReadBlobStream transport error: {status}"
            ))),
        });

        Ok(QuorumReadStreamResult {
            body: Box::new(StreamReader::new(body_stream)),
            etag: header.etag,
            content_type: header.content_type,
            metadata: header.metadata,
            hlc: header
                .hlc
                .as_ref()
                .map(HlcTimestamp::from_proto)
                .unwrap_or_else(HlcTimestamp::zero),
            version_id: header.version_id,
            total_size,
            content_length,
            content_range,
            replicas_responded: 1,
        })
    }
}

fn candidate_order(
    digests: &[ObjectDigest],
    winner_idx: usize,
    local_node_id: NodeId,
) -> Vec<usize> {
    let latest = &digests[winner_idx];
    // A replica whose record is a namespace entry for an erasure-coded object
    // holds no bytes to hand over. It answers the HEAD, so it looks like a
    // source, and the stream it opens fails on its first item — past the point
    // where another replica can be tried. It is dropped here rather than in
    // either caller, because "which replicas can serve these bytes" is the
    // question this function exists to answer.
    // A tombstone holds no bytes either. Nor does a replica of an older
    // version hold the bytes of this one: served in its place, they would be
    // an object the newest version replaced, or one deleted since, and read
    // repair would spread them under the newest version's HLC.
    let mut candidates: Vec<usize> = (0..digests.len())
        .filter(|&idx| {
            !digests[idx].erasure_coded
                && !digests[idx].tombstone
                && digests[idx].same_object_version(latest)
        })
        .collect();

    candidates.sort_by(|&a, &b| {
        let rank_a = candidate_rank(&digests[a], a, winner_idx, latest, local_node_id);
        let rank_b = candidate_rank(&digests[b], b, winner_idx, latest, local_node_id);

        rank_a
            .cmp(&rank_b)
            .then_with(|| digests[b].hlc.cmp(&digests[a].hlc))
            .then_with(|| digests[a].node_id.0.cmp(&digests[b].node_id.0))
    });

    candidates
}

fn candidate_rank(
    digest: &ObjectDigest,
    idx: usize,
    winner_idx: usize,
    latest: &ObjectDigest,
    local_node_id: NodeId,
) -> u8 {
    if digest.node_id == local_node_id && digest.same_object_version(latest) {
        0
    } else if idx == winner_idx {
        1
    } else {
        2
    }
}

fn is_integrity_error(error: &ClusterError) -> bool {
    match error {
        ClusterError::Storage(message) | ClusterError::Rpc(message) => {
            let message = message.to_ascii_lowercase();
            message.contains("checksum")
                || message.contains("crc")
                || message.contains("corrupt")
                || message.contains("integrity")
                || message.contains("data loss")
                || message.contains("dataloss")
        }
        ClusterError::Io(err) => err.kind() == io::ErrorKind::InvalidData,
        _ => false,
    }
}

/// The newest of `digests`, given that other replicas with the tombstone
/// horizons `absent_horizons` hold no key, as [`reconcile::newest_of`] picks
/// it.
#[cfg(test)]
fn newest_of(
    digests: &[ObjectDigest],
    absent_horizons: &[Option<HlcTimestamp>],
    confirming: usize,
) -> NewestVersion {
    let answers: Vec<(NodeId, ReplicaHolds)> = digests
        .iter()
        .map(|digest| (digest.node_id, digest.holds()))
        .chain(
            absent_horizons
                .iter()
                .map(|horizon| (NodeId(0), ReplicaHolds::Nothing { horizon: *horizon })),
        )
        .collect();
    reconcile::newest_of(&answers, confirming)
}

/// The versions of a key as its replicas and one more node hold them: a
/// node handing over or removing the copies of keys it does not own
/// (`QuorumReadCoordinator::key_versions_with_holder`).
pub struct WithHolder {
    inner: Arc<QuorumReadCoordinator>,
    holder: NodeId,
}

impl WithHolder {
    /// Versions read through `inner`, `holder` asked too.
    pub fn new(inner: Arc<QuorumReadCoordinator>, holder: NodeId) -> Self {
        Self { inner, holder }
    }
}

#[async_trait::async_trait]
impl KeyStateSource for WithHolder {
    async fn newest_version(
        &self,
        bucket: &str,
        key: &str,
        target: NodeId,
    ) -> Result<NewestVersion, ClusterError> {
        self.inner.newest_version(bucket, key, target).await
    }

    async fn key_versions(&self, bucket: &str, key: &str) -> Result<KeyVersions, ClusterError> {
        self.inner.key_versions_with_holder(bucket, key, self.holder).await
    }

    async fn read_copy(
        &self,
        bucket: &str,
        key: &str,
        node: NodeId,
    ) -> Result<StoredCopy, ClusterError> {
        self.inner.read_copy(bucket, key, node).await
    }
}

#[async_trait::async_trait]
impl KeyStateSource for QuorumReadCoordinator {
    async fn newest_version(
        &self,
        bucket: &str,
        key: &str,
        target: NodeId,
    ) -> Result<NewestVersion, ClusterError> {
        self.newest_version_except(bucket, key, target).await
    }

    async fn key_versions(&self, bucket: &str, key: &str) -> Result<KeyVersions, ClusterError> {
        self.all_key_versions(bucket, key).await
    }

    async fn read_copy(
        &self,
        bucket: &str,
        key: &str,
        node: NodeId,
    ) -> Result<StoredCopy, ClusterError> {
        let (data, record) = self.fetch_full_object(bucket, key, node).await?;
        // A record written without an HLC carries the one every node makes up
        // for it, with node 0: read locally, from a replica's answer, or from
        // a copy repaired from such a record. A replica that answered with no
        // HLC at all leaves a zero one, and the copy carries none.
        let hlc = Some(HlcTimestamp::of_record(&record)).filter(|hlc| hlc.wall_time > 0);
        Ok(StoredCopy {
            hlc,
            data,
            etag: record.etag,
            content_hash: record.content_hash.to_vec(),
            content_type: record.content_type,
            metadata: record.metadata,
            version_id: record.version_id,
        })
    }
}

/// Short grace period for late digests after reaching the read quorum.
fn digest_drain_deadline(quorum_deadline: tokio::time::Instant) -> tokio::time::Instant {
    quorum_deadline.min(tokio::time::Instant::now() + Duration::from_millis(50))
}

/// Internal result type for digest collection.
#[derive(Debug)]
#[allow(dead_code)]
enum DigestResult {
    Success(ObjectDigest),
    /// The replica holds nothing for the key; its tombstone horizon, if it
    /// reported one.
    NotFound(NodeId, Option<HlcTimestamp>),
    Timeout(NodeId),
    Error(NodeId, String),
    EpochMismatch(NodeId),
}

impl DigestResult {
    /// The replica that gave this answer.
    fn node_id(&self) -> NodeId {
        match self {
            Self::Success(digest) => digest.node_id,
            Self::NotFound(node_id, _)
            | Self::Timeout(node_id)
            | Self::Error(node_id, _)
            | Self::EpochMismatch(node_id) => *node_id,
        }
    }
}

/// How much longer than the replica read timeout the versions of a key are
/// waited for: each replica's own task gives up at the timeout.
const HOLDINGS_GRACE: Duration = Duration::from_secs(1);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gossip::GossipHandle;
    use crate::placement::PlacementRouter;
    use crate::rpc::NodeClientConfig;

    fn legacy_read_snapshot() -> (proto::ReadBlobResponse, proto::HeadObjectResponse) {
        use sha2::{Digest, Sha256};

        let data = b"legacy multipart bytes".to_vec();
        let hash = Sha256::digest(&data).to_vec();
        let hlc = HlcTimestamp::from_record(20, 0, 0).to_proto();
        let response = proto::ReadBlobResponse {
            success: true,
            content_length: data.len() as u64,
            data,
            content_hash: hash.clone(),
            hlc: Some(hlc.clone()),
            content_type: "text/custom".into(),
            metadata: HashMap::from([("original".into(), "metadata".into())]),
            ..Default::default()
        };
        let head = proto::HeadObjectResponse {
            exists: true,
            content_length: response.content_length,
            content_hash: hash,
            hlc: Some(hlc),
            etag: "\"9b2cf535f27731c974343645a3985328-2\"".into(),
            content_type: response.content_type.clone(),
            metadata: response.metadata.clone(),
            ..Default::default()
        };
        (response, head)
    }

    #[test]
    fn legacy_read_recovers_the_original_multipart_etag_from_the_same_snapshot() {
        let (response, head) = legacy_read_snapshot();
        assert!(response.etag.is_empty());
        assert_eq!(legacy_read_etag(&response, &head).unwrap(), head.etag);
    }

    #[test]
    fn legacy_read_refuses_an_unconfirmed_or_changed_snapshot() {
        use sha2::Digest;

        let (response, head) = legacy_read_snapshot();
        let mut cases = Vec::new();
        macro_rules! changed {
            ($name:literal, $read:ident, $head:ident, $change:block) => {{
                let mut pair = (response.clone(), head.clone());
                let ($read, $head) = (&mut pair.0, &mut pair.1);
                $change
                cases.push(($name, pair.0, pair.1));
            }};
        }
        changed!("same HLC, different bytes", read, other, {
            read.data[0] ^= 1;
            other.content_hash = sha2::Sha256::digest(&read.data).to_vec();
        });
        changed!("different HLC", read, _other, {
            read.hlc.as_mut().unwrap().wall_time += 1;
        });
        changed!("different logical clock", _read, other, {
            other.hlc.as_mut().unwrap().logical += 1;
        });
        changed!("missing HLC", read, other, {
            read.hlc = None;
            other.hlc = None;
        });
        changed!("zero HLC", read, other, {
            read.hlc = Some(HlcTimestamp::zero().to_proto());
            other.hlc = read.hlc.clone();
        });
        changed!("malformed origin", read, other, {
            read.hlc.as_mut().unwrap().node_id.clear();
            other.hlc = read.hlc.clone();
        });
        changed!("missing hashes", read, other, {
            read.content_hash.clear();
            other.content_hash.clear();
        });
        changed!("wrong hashes", read, other, {
            read.content_hash = vec![0; 32];
            other.content_hash = read.content_hash.clone();
        });
        changed!("actual size", read, other, {
            read.content_length += 1;
            other.content_length = read.content_length;
        });
        changed!("declared size", _read, other, {
            other.content_length += 2;
        });
        changed!("version", _read, other, {
            other.version_id = Some("new".into());
        });
        changed!("metadata", _read, other, {
            other.metadata.insert("new".into(), "value".into());
        });
        changed!("content type", _read, other, {
            other.content_type = "application/xml".into();
        });
        changed!("read error", read, _other, {
            read.error = "storage failed".into();
        });
        changed!("missing ETag", _read, other, {
            other.etag.clear();
        });
        changed!("read tombstone", read, _other, {
            read.tombstone = true;
        });
        changed!("head tombstone", _read, other, {
            other.tombstone = true;
        });
        changed!("deleted", _read, other, {
            other.exists = false;
        });
        changed!("read epoch mismatch", read, _other, {
            read.epoch_mismatch = true;
        });
        changed!("head epoch mismatch", _read, other, {
            other.epoch_mismatch = true;
        });
        changed!("failed read", read, _other, {
            read.success = false;
        });
        changed!("head error", _read, other, {
            other.error = "storage failed".into();
        });
        changed!("EC namespace", _read, other, {
            other.erasure_coded = true;
        });
        for (name, response, head) in cases {
            assert!(
                is_source_changed(&legacy_read_etag(&response, &head).unwrap_err()),
                "{name}"
            );
        }
    }

    async fn legacy_rpc_source(
        omit_etag: bool,
        delay: Duration,
        port: u16,
    ) -> (
        crate::coordinator::test_pool::TestPool,
        Arc<MockStorage>,
        crate::rpc::server::RpcServerHandle,
    ) {
        use crate::coordinator::test_pool::{Replica, TestPool};
        use crate::identity::NodeStatus;
        use crate::replica::ReplicaReadHandler;
        use crate::rpc::{NodeRpcServer, RpcServerConfig};

        let pool = TestPool::start(Replica::Down, Replica::Down, port).await;
        let mut member = pool.gossip.get_node(&NodeId(2)).unwrap();
        member.status = NodeStatus::Alive;
        member.meta.protocol_version = if omit_etag {
            3
        } else {
            crate::PROTOCOL_VERSION
        };
        let meta = Arc::new(member.meta.clone());
        pool.gossip.set_member_for_testing(NodeId(2), member);
        let (response, head) = legacy_read_snapshot();
        let hlc = response.hlc.as_ref().unwrap();
        let record = s4_core::IndexRecord {
            etag: head.etag,
            size: response.content_length,
            content_hash: response.content_hash.try_into().unwrap(),
            hlc_timestamp: hlc.wall_time,
            hlc_logical: hlc.logical,
            origin_node_id: 0,
            content_type: response.content_type,
            metadata: response.metadata,
            ..ahead_record()
        };
        let storage = Arc::new(MockStorage {
            pending_head: None,
            read_snapshot: Some(ReadSnapshot {
                record,
                data: response.data,
                omit_etag,
                delay,
                heads: std::sync::atomic::AtomicUsize::new(0),
            }),
        });
        let server = NodeRpcServer::new(NodeId(2), meta.clone(), pool.gossip.clone())
            .with_read_handler(Arc::new(ReplicaReadHandler::new(
                storage.clone(),
                pool.epoch(),
            )))
            .start(RpcServerConfig {
                bind_addr: meta.addr.grpc_addr,
                ..Default::default()
            })
            .await
            .unwrap();
        (pool, storage, server)
    }

    #[tokio::test]
    async fn legacy_read_over_rpc_preserves_etag_in_selected_reads_and_repair_copies() {
        use std::sync::atomic::Ordering;

        for legacy in [true, false] {
            let (pool, storage, server) = legacy_rpc_source(legacy, Duration::ZERO, 21572).await;
            let source = storage.read_snapshot.as_ref().unwrap();
            let selected = ObjectDigest::of_record(NodeId(2), source.record.clone());
            let coordinator = pool.read_coordinator();
            let (bytes, record) = coordinator
                .fetch_selected_object("bucket", "multipart", &selected)
                .await
                .unwrap();
            assert_eq!(bytes, source.data);
            assert_eq!(record.etag, selected.etag);
            let copy = coordinator.read_copy("bucket", "multipart", NodeId(2)).await.unwrap();
            assert_eq!(copy.data, source.data);
            assert_eq!(copy.etag, selected.etag);
            assert_eq!(copy.hlc, Some(selected.hlc));
            assert_eq!(
                source.heads.load(Ordering::SeqCst),
                if legacy { 2 } else { 0 }
            );
            server.shutdown().await.unwrap();
        }
    }

    #[tokio::test]
    async fn legacy_read_over_rpc_shares_one_deadline_with_its_confirming_head() {
        let (pool, storage, server) =
            legacy_rpc_source(true, Duration::from_millis(1200), 21576).await;
        // Exclude connection setup from the timing being exercised.
        pool.rpc_client.send_health(NodeId(2)).await.unwrap();
        let coordinator = pool.read_coordinator_with(ReadTimeouts {
            replica_read_timeout: Duration::from_secs(2),
            ..ReadTimeouts::default()
        });
        let outcome = tokio::time::timeout(
            Duration::from_secs(5),
            coordinator.read_copy("bucket", "multipart", NodeId(2)),
        )
        .await
        .unwrap();
        assert!(
            matches!(outcome, Err(ClusterError::ReadTimeout(_))),
            "{outcome:?}"
        );
        assert_eq!(
            storage
                .read_snapshot
                .as_ref()
                .unwrap()
                .heads
                .load(std::sync::atomic::Ordering::SeqCst),
            1,
            "the GET must finish and reach HEAD before the shared deadline expires"
        );
        server.shutdown().await.unwrap();
    }

    fn setup_single_node() -> (QuorumReadCoordinator, tempfile::TempDir) {
        let tmp = tempfile::tempdir().unwrap();
        let node_id = NodeId(1);
        let gossip = Arc::new(GossipHandle::new_for_testing(Default::default()));
        let rpc_client = Arc::new(NodeClient::new(gossip.clone(), NodeClientConfig::default()));
        let router = Arc::new(PlacementRouter::single_node(node_id));
        let clock = Arc::new(HybridClock::new(node_id));
        let read_repair = Arc::new(ReadRepairService::start_with_capacity(
            rpc_client.clone(),
            16,
        ));
        let storage = Arc::new(MockStorage::with_data());

        let config = ClusterConfig::default();
        let coordinator = QuorumReadCoordinator::new(
            node_id,
            storage,
            rpc_client,
            router,
            clock,
            read_repair,
            &config,
            ReadTimeouts::default(),
        );

        (coordinator, tmp)
    }

    async fn selected_read_race() -> (QuorumReadCoordinator, ObjectDigest, tempfile::TempDir) {
        let (mut coordinator, tmp) = setup_single_node();
        let storage = Arc::new(
            s4_core::storage::BitcaskStorageEngine::new(
                tmp.path().join("volumes"),
                tmp.path().join("metadata"),
                10 * 1024 * 1024,
                4096,
                false,
            )
            .await
            .unwrap(),
        );
        let metadata = |wall: u64| {
            HashMap::from([
                ("_s4_hlc_wall".into(), wall.to_string()),
                ("_s4_hlc_logical".into(), "0".into()),
                ("_s4_hlc_node_id".into(), "1".into()),
            ])
        };
        storage
            .put_object("bucket", "racing", b"old", "text/plain", &metadata(20))
            .await
            .unwrap();
        let selected = ObjectDigest::of_record(
            NodeId(1),
            storage.head_object("bucket", "racing").await.unwrap(),
        );
        storage
            .put_object("bucket", "racing", b"new-body", "text/plain", &metadata(30))
            .await
            .unwrap();
        coordinator.storage = storage;
        (coordinator, selected, tmp)
    }

    /// A source that took a newer version since its digest serves it; one
    /// whose version is not newer than the one selected, yet another, is
    /// refused.
    #[tokio::test]
    async fn selected_full_read_serves_a_newer_version_and_refuses_another() {
        let (coordinator, selected, _tmp) = selected_read_race().await;
        let (data, record) =
            coordinator.fetch_selected_object("bucket", "racing", &selected).await.unwrap();
        assert_eq!(data, b"new-body");
        assert_eq!(HlcTimestamp::of_record(&record).wall_time, 30);
        let mut ahead = selected.clone();
        ahead.hlc = HlcTimestamp::from_record(40, 0, 1);
        assert!(coordinator.fetch_selected_object("bucket", "racing", &ahead).await.is_err());
        assert_eq!(coordinator.read_repair.repairs_initiated(), 0);
    }

    #[tokio::test]
    async fn selected_stream_read_serves_a_newer_version_and_refuses_another() {
        let (coordinator, selected, _tmp) = selected_read_race().await;
        let stream = coordinator
            .open_selected_stream("bucket", "racing", &selected, None)
            .await
            .unwrap();
        assert_eq!(stream.hlc.wall_time, 30);
        assert_eq!(stream.total_size, 8);
        let mut ahead = selected.clone();
        ahead.hlc = HlcTimestamp::from_record(40, 0, 1);
        assert!(coordinator
            .open_selected_stream("bucket", "racing", &ahead, None)
            .await
            .is_err());
        assert_eq!(coordinator.read_repair.repairs_initiated(), 0);
    }

    #[tokio::test]
    async fn single_node_read_returns_data() {
        let (coordinator, _tmp) = setup_single_node();
        let result = coordinator.read("test-bucket", "hello.txt").await.unwrap();
        assert_eq!(result.data, b"Hello S4!");
        assert_eq!(result.etag, "\"mock-etag\"");
        assert_eq!(result.replicas_responded, 1);
        assert!(!result.repair_triggered);
    }

    #[tokio::test]
    async fn single_node_read_not_found() {
        let (coordinator, _tmp) = setup_single_node();
        let err = coordinator.read("test-bucket", "missing.txt").await.unwrap_err();
        assert!(matches!(err, ClusterError::ObjectNotFound { .. }));
    }

    #[tokio::test]
    async fn single_node_head_returns_metadata() {
        let (coordinator, _tmp) = setup_single_node();
        let result = coordinator.head("test-bucket", "hello.txt").await.unwrap();
        assert_eq!(result.etag, "\"mock-etag\"");
        assert_eq!(result.size, 9);
        assert_eq!(result.replicas_responded, 1);
    }

    /// A copy whose HLC would be zero carries none: written with it, it would
    /// pass the target's LWW unweighed. The mock's record has no HLC and was
    /// modified within the first millisecond, as a copy built from a replica
    /// that reported no HLC is.
    #[tokio::test]
    async fn a_copy_with_a_zero_hlc_carries_none() {
        let (coordinator, _tmp) = setup_single_node();
        let copy = coordinator.read_copy("test-bucket", "hello.txt", NodeId(1)).await.unwrap();
        assert_eq!(copy.hlc, None);
        assert_eq!(copy.data, b"Hello S4!");
    }

    /// The single-node paths report a record written without an HLC as every
    /// other path does: with a made-up HLC, node 0.
    #[tokio::test]
    async fn single_node_reads_report_a_made_up_hlc_for_a_record_without_one() {
        let (coordinator, _tmp) = setup_single_node();
        let read = coordinator.read("test-bucket", "hello.txt").await.unwrap();
        let head = coordinator.head("test-bucket", "hello.txt").await.unwrap();
        assert!(read.hlc.is_made_up(), "{:?}", read.hlc);
        assert!(head.hlc.is_made_up(), "{:?}", head.hlc);
    }

    /// The single-node paths move this node's clock past what they read, as a
    /// quorum read does: a write this node makes after reading a record
    /// another node's coordinator wrote is newer than it, even when that
    /// node's clock runs ahead. A bucket configuration relies on it to be
    /// newer than the marker its write checked.
    #[tokio::test]
    async fn single_node_reads_move_the_clock_past_what_they_read() {
        let (coordinator, _tmp) = setup_single_node();
        let head = coordinator.head("test-bucket", AHEAD_KEY).await.unwrap();
        assert!(coordinator.clock.now() > head.hlc);

        let (coordinator, _tmp) = setup_single_node();
        let read = coordinator.read("test-bucket", AHEAD_KEY).await.unwrap();
        assert!(coordinator.clock.now() > read.hlc);
    }

    #[tokio::test]
    async fn single_node_head_not_found() {
        let (coordinator, _tmp) = setup_single_node();
        let err = coordinator.head("test-bucket", "missing.txt").await.unwrap_err();
        assert!(matches!(err, ClusterError::ObjectNotFound { .. }));
    }

    #[tokio::test]
    async fn local_digest_storage_error_is_not_absence() {
        let (coordinator, _tmp) = setup_single_node();
        let replicas = [coordinator.local_node_id];
        let missing = coordinator.collect_digests("bucket", "missing.txt", &replicas, 1).await;
        assert!(
            matches!(&missing, Ok(collected) if collected.digests.is_empty() && collected.absent == 1),
            "{missing:?}"
        );

        let failed = coordinator.collect_digests("bucket", "unreadable.txt", &replicas, 1).await;
        assert!(
            matches!(&failed, Ok(collected) if collected.digests.is_empty() && collected.absent == 0),
            "a failed storage read must not count toward an absence quorum: {failed:?}"
        );
    }

    async fn assert_pending_digest_is_cancelled(cancel_caller: bool) {
        let (mut coordinator, _tmp) = setup_single_node();
        let started = Arc::new(tokio::sync::Notify::new());
        let dropped = Arc::new(tokio::sync::Notify::new());
        coordinator.storage = Arc::new(MockStorage {
            pending_head: Some((started.clone(), dropped.clone())),
            read_snapshot: None,
        });
        coordinator.timeouts.quorum_timeout = Duration::from_millis(200);
        coordinator.timeouts.replica_read_timeout = Duration::from_secs(10);
        let read = tokio::spawn(async move {
            coordinator.collect_digests("bucket", "pending.txt", &[NodeId(1)], 1).await
        });
        started.notified().await;
        if cancel_caller {
            read.abort();
            assert!(read.await.unwrap_err().is_cancelled());
        } else {
            let collected = read.await.unwrap().unwrap();
            assert!(collected.digests.is_empty() && collected.absent == 0);
        }
        assert!(
            tokio::time::timeout(Duration::from_secs(1), dropped.notified()).await.is_ok(),
            "the in-flight replica read outlived its quorum operation"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn cancelling_digest_collection_cancels_replica_tasks() {
        assert_pending_digest_is_cancelled(true).await;
    }

    #[tokio::test(start_paused = true)]
    async fn digest_deadline_cancels_replica_tasks() {
        assert_pending_digest_is_cancelled(false).await;
    }

    #[tokio::test(start_paused = true)]
    async fn digest_drain_never_extends_the_quorum_deadline() {
        let now = tokio::time::Instant::now();
        let deadline = now + Duration::from_millis(10);
        assert_eq!(digest_drain_deadline(deadline), deadline);
        assert_eq!(
            digest_drain_deadline(now + Duration::from_secs(5)),
            now + Duration::from_millis(50)
        );
        assert_eq!(digest_drain_deadline(now), now);
    }

    #[test]
    fn read_timeouts_default() {
        let t = ReadTimeouts::default();
        assert_eq!(t.quorum_timeout, Duration::from_secs(5));
        assert_eq!(t.replica_read_timeout, Duration::from_secs(10));
    }

    #[test]
    fn object_digest_ordering_by_hlc() {
        let d1 = ObjectDigest {
            node_id: NodeId(1),
            hlc: HlcTimestamp {
                wall_time: 1000,
                logical: 0,
                node_id: 1,
            },
            etag: "\"a\"".into(),
            size: 10,
            content_type: "text/plain".into(),
            version_id: None,
            metadata: HashMap::new(),
            content_hash: None,
            erasure_coded: false,
            tombstone: false,
        };
        let d2 = ObjectDigest {
            node_id: NodeId(2),
            hlc: HlcTimestamp {
                wall_time: 2000,
                logical: 0,
                node_id: 2,
            },
            etag: "\"b\"".into(),
            size: 20,
            content_type: "text/plain".into(),
            version_id: None,
            metadata: HashMap::new(),
            content_hash: None,
            erasure_coded: false,
            tombstone: false,
        };

        let digests = [d1, d2];
        let hlc_list: Vec<HlcTimestamp> = digests.iter().map(|d| d.hlc).collect();
        let winner_idx = crate::conflict::select_winner(&hlc_list).unwrap();
        assert_eq!(digests[winner_idx].etag, "\"b\"");
    }

    /// A replica that holds only a namespace entry is not a source of bytes.
    ///
    /// It answers the HEAD like any other replica, so it looks like one. The
    /// stream it opens then fails on its first item, past the point where
    /// another replica can be tried — which is why it has to be dropped before
    /// the choice is made rather than after it fails.
    #[test]
    fn candidate_order_drops_replicas_that_hold_no_bytes() {
        let hlc = HlcTimestamp {
            wall_time: 2000,
            logical: 0,
            node_id: 1,
        };
        let digest = |node: u128, erasure_coded: bool| ObjectDigest {
            node_id: NodeId(node),
            hlc,
            etag: "\"same\"".into(),
            size: 64,
            content_type: "application/octet-stream".into(),
            version_id: None,
            metadata: HashMap::new(),
            content_hash: None,
            erasure_coded,
            tombstone: false,
        };

        let digests = [digest(1, true), digest(2, false), digest(3, true)];
        let order = candidate_order(&digests, 0, NodeId(1));

        assert_eq!(
            order,
            vec![1],
            "only the replica that still holds the bytes may be streamed from"
        );

        // Every replica erasure coded: nothing to choose, and the caller has to
        // say so rather than report the object missing.
        let all_coded = [digest(1, true), digest(2, true)];
        assert!(candidate_order(&all_coded, 0, NodeId(1)).is_empty());
    }

    #[test]
    fn candidate_order_prefers_local_replica_with_latest_digest() {
        let latest_hlc = HlcTimestamp {
            wall_time: 2000,
            logical: 0,
            node_id: 1,
        };
        let stale_hlc = HlcTimestamp {
            wall_time: 1000,
            logical: 0,
            node_id: 3,
        };
        let digests = [
            ObjectDigest {
                node_id: NodeId(1),
                hlc: latest_hlc,
                etag: "\"latest\"".into(),
                size: 64,
                content_type: "application/octet-stream".into(),
                version_id: None,
                metadata: HashMap::new(),
                content_hash: None,
                erasure_coded: false,
                tombstone: false,
            },
            ObjectDigest {
                node_id: NodeId(2),
                hlc: latest_hlc,
                etag: "\"latest\"".into(),
                size: 64,
                content_type: "application/octet-stream".into(),
                version_id: None,
                metadata: HashMap::new(),
                content_hash: None,
                erasure_coded: false,
                tombstone: false,
            },
            ObjectDigest {
                node_id: NodeId(3),
                hlc: stale_hlc,
                etag: "\"stale\"".into(),
                size: 64,
                content_type: "application/octet-stream".into(),
                version_id: None,
                metadata: HashMap::new(),
                content_hash: None,
                erasure_coded: false,
                tombstone: false,
            },
        ];

        let order = candidate_order(&digests, 0, NodeId(2));

        assert_eq!(
            order,
            vec![1, 0],
            "a replica of an older version is no source"
        );
    }

    /// When the replica of the newest version cannot hand its bytes over,
    /// neither can a replica of an older version or one that holds a
    /// tombstone: the read fails rather than serve what was replaced or
    /// deleted.
    #[test]
    fn candidate_order_never_falls_back_to_an_older_version() {
        let at = |wall_time: u64, node: u128| HlcTimestamp {
            wall_time,
            logical: 0,
            node_id: node as u64 as u128,
        };
        let live = |node: u128, wall: u64| ObjectDigest {
            node_id: NodeId(node),
            hlc: at(wall, node),
            etag: format!("\"v{wall}\""),
            size: 64,
            content_type: "application/octet-stream".into(),
            version_id: None,
            metadata: HashMap::new(),
            content_hash: None,
            erasure_coded: false,
            tombstone: false,
        };
        let digests = [
            live(1, 30),
            ObjectDigest::of_tombstone(NodeId(2), at(20, 2)),
            live(3, 10),
        ];

        assert_eq!(candidate_order(&digests, 0, NodeId(3)), vec![0]);
    }

    #[test]
    fn stream_failure_tracker_quarantines_marked_source() {
        let tracker = StreamFailureTracker::default();
        let source = StreamSourceKey {
            node_id: NodeId(2),
            bucket: "bucket".into(),
            key: "key".into(),
        };

        assert!(!tracker.is_quarantined(source.node_id, &source.bucket, &source.key));
        tracker.mark_failed(source.clone());
        assert!(tracker.is_quarantined(source.node_id, &source.bucket, &source.key));
    }

    struct MockStorage {
        pending_head: Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>,
        read_snapshot: Option<ReadSnapshot>,
    }

    struct ReadSnapshot {
        record: s4_core::IndexRecord,
        data: Vec<u8>,
        omit_etag: bool,
        delay: Duration,
        heads: std::sync::atomic::AtomicUsize,
    }

    /// A key of the mock written by another node's coordinator whose clock
    /// runs a minute ahead of this one.
    const AHEAD_KEY: &str = "ahead.txt";

    fn ahead_record() -> s4_core::IndexRecord {
        let wall = crate::clock::hlc::now_millis() + 60_000;
        s4_core::IndexRecord {
            file_id: 1,
            offset: 0,
            size: 5,
            etag: "\"ahead-etag\"".to_string(),
            content_type: "text/plain".to_string(),
            metadata: HashMap::new(),
            content_hash: [0u8; 32],
            created_at: 1000,
            modified_at: 2000,
            version_id: None,
            is_delete_marker: false,
            retention_mode: None,
            retain_until_timestamp: None,
            legal_hold: false,
            layout: None,
            pool_id: 0,
            hlc_timestamp: wall,
            hlc_logical: 3,
            origin_node_id: 2,
            operation_id: 0,
        }
    }

    struct NotifyOnDrop(Arc<tokio::sync::Notify>);

    impl Drop for NotifyOnDrop {
        fn drop(&mut self) {
            self.0.notify_one();
        }
    }

    impl MockStorage {
        fn with_data() -> Self {
            MockStorage {
                pending_head: None,
                read_snapshot: None,
            }
        }
    }

    #[async_trait::async_trait]
    impl StorageEngine for MockStorage {
        async fn put_object(
            &self,
            _b: &str,
            _k: &str,
            _d: &[u8],
            _ct: &str,
            _m: &HashMap<String, String>,
        ) -> Result<String, s4_core::StorageError> {
            Ok("\"mock-etag\"".into())
        }
        async fn get_object(
            &self,
            _b: &str,
            k: &str,
        ) -> Result<(Vec<u8>, s4_core::IndexRecord), s4_core::StorageError> {
            if let Some(snapshot) = &self.read_snapshot {
                tokio::time::sleep(snapshot.delay).await;
                let mut record = snapshot.record.clone();
                if snapshot.omit_etag {
                    record.etag.clear();
                }
                return Ok((snapshot.data.clone(), record));
            }
            if k == "hello.txt" {
                Ok((
                    b"Hello S4!".to_vec(),
                    s4_core::IndexRecord {
                        file_id: 1,
                        offset: 0,
                        size: 9,
                        etag: "\"mock-etag\"".to_string(),
                        content_type: "text/plain".to_string(),
                        metadata: HashMap::new(),
                        content_hash: [0u8; 32],
                        created_at: 1000,
                        modified_at: 2000,
                        version_id: None,
                        is_delete_marker: false,
                        retention_mode: None,
                        retain_until_timestamp: None,
                        legal_hold: false,
                        layout: None,
                        pool_id: 0,
                        hlc_timestamp: 0,
                        hlc_logical: 0,
                        origin_node_id: 0,
                        operation_id: 0,
                    },
                ))
            } else if k == AHEAD_KEY {
                Ok((b"ahead".to_vec(), ahead_record()))
            } else {
                Err(s4_core::StorageError::ObjectNotFound { key: k.into() })
            }
        }
        async fn delete_object(&self, _b: &str, _k: &str) -> Result<(), s4_core::StorageError> {
            Ok(())
        }
        async fn head_object(
            &self,
            _b: &str,
            k: &str,
        ) -> Result<s4_core::IndexRecord, s4_core::StorageError> {
            if let Some(snapshot) = &self.read_snapshot {
                snapshot.heads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                tokio::time::sleep(snapshot.delay).await;
                return Ok(snapshot.record.clone());
            }
            if let Some((started, dropped)) = &self.pending_head {
                let _notify = NotifyOnDrop(dropped.clone());
                started.notify_one();
                std::future::pending::<()>().await;
            }
            if k == "unreadable.txt" {
                return Err(s4_core::StorageError::Database(
                    "injected database failure".into(),
                ));
            }
            if k == AHEAD_KEY {
                return Ok(ahead_record());
            }
            if k == "hello.txt" {
                Ok(s4_core::IndexRecord {
                    file_id: 1,
                    offset: 0,
                    size: 9,
                    etag: "\"mock-etag\"".to_string(),
                    content_type: "text/plain".to_string(),
                    metadata: HashMap::new(),
                    content_hash: [0u8; 32],
                    created_at: 1000,
                    modified_at: 2000,
                    version_id: None,
                    is_delete_marker: false,
                    retention_mode: None,
                    retain_until_timestamp: None,
                    legal_hold: false,
                    layout: None,
                    pool_id: 0,
                    hlc_timestamp: 0,
                    hlc_logical: 0,
                    origin_node_id: 0,
                    operation_id: 0,
                })
            } else {
                Err(s4_core::StorageError::ObjectNotFound { key: k.into() })
            }
        }
        async fn list_objects(
            &self,
            _b: &str,
            _p: &str,
            _m: usize,
        ) -> Result<Vec<(String, s4_core::IndexRecord)>, s4_core::StorageError> {
            Ok(vec![])
        }
        async fn list_objects_after(
            &self,
            _b: &str,
            _p: &str,
            _s: &str,
            _m: usize,
        ) -> Result<Vec<(String, s4_core::IndexRecord)>, s4_core::StorageError> {
            Ok(vec![])
        }
        async fn put_object_versioned(
            &self,
            _b: &str,
            _k: &str,
            _d: &[u8],
            _ct: &str,
            _m: &HashMap<String, String>,
            _vs: s4_core::VersioningStatus,
        ) -> Result<(String, Option<String>), s4_core::StorageError> {
            Ok(("\"mock-etag\"".into(), None))
        }
        async fn get_object_version(
            &self,
            _b: &str,
            _k: &str,
            _v: &str,
        ) -> Result<(Vec<u8>, s4_core::IndexRecord), s4_core::StorageError> {
            Err(s4_core::StorageError::ObjectNotFound { key: "m".into() })
        }
        async fn delete_object_versioned(
            &self,
            _b: &str,
            _k: &str,
            _v: Option<&str>,
            _vs: s4_core::VersioningStatus,
        ) -> Result<s4_core::DeleteResult, s4_core::StorageError> {
            Ok(s4_core::DeleteResult {
                delete_marker: false,
                version_id: None,
            })
        }
        async fn head_object_version(
            &self,
            _b: &str,
            _k: &str,
            _v: &str,
        ) -> Result<s4_core::IndexRecord, s4_core::StorageError> {
            Err(s4_core::StorageError::ObjectNotFound { key: "m".into() })
        }
        async fn list_object_versions(
            &self,
            _b: &str,
            _p: &str,
            _km: Option<&str>,
            _vm: Option<&str>,
            _m: usize,
        ) -> Result<s4_core::ListVersionsResult, s4_core::StorageError> {
            Ok(Default::default())
        }
        #[allow(clippy::too_many_arguments)]
        async fn put_object_with_retention(
            &self,
            _b: &str,
            _k: &str,
            _d: &[u8],
            _ct: &str,
            _m: &HashMap<String, String>,
            _vs: s4_core::VersioningStatus,
            _dr: Option<s4_core::types::DefaultRetention>,
        ) -> Result<(String, Option<String>), s4_core::StorageError> {
            Ok(("\"mock-etag\"".into(), None))
        }
        async fn update_object_metadata(
            &self,
            _b: &str,
            _k: &str,
            _v: &str,
            _r: s4_core::IndexRecord,
        ) -> Result<(), s4_core::StorageError> {
            Ok(())
        }
        async fn open_object_stream(
            &self,
            _b: &str,
            _k: &str,
            _opts: s4_core::ReadOptions,
        ) -> Result<s4_core::ObjectStream, s4_core::StorageError> {
            Err(s4_core::StorageError::ObjectNotFound { key: "m".into() })
        }
        async fn open_object_version_stream(
            &self,
            _b: &str,
            _k: &str,
            _v: &str,
            _opts: s4_core::ReadOptions,
        ) -> Result<s4_core::ObjectStream, s4_core::StorageError> {
            Err(s4_core::StorageError::ObjectNotFound { key: "m".into() })
        }
        async fn put_object_streaming(
            &self,
            _b: &str,
            _k: &str,
            _r: Box<dyn tokio::io::AsyncRead + Unpin + Send>,
            _cl: u64,
            _ct: &str,
            _m: &HashMap<String, String>,
            _e: &str,
        ) -> Result<s4_core::StreamingPutResult, s4_core::StorageError> {
            Ok(s4_core::StreamingPutResult {
                content_hash: [0u8; 32],
                crc32: 0,
                bytes_written: 0,
                etag: "\"mock-etag\"".into(),
            })
        }
    }

    // ---- Quorum operations, as the metrics count them ----

    #[tokio::test]
    async fn reads_and_heads_count_an_absent_object_apart_from_a_failure() {
        let metrics = crate::test_metrics::TestMetrics::install();
        let (coordinator, _tmp) = setup_single_node();
        let ops = "s4_quorum_operations_total";

        coordinator.read("bucket", "hello.txt").await.expect("found");
        assert!(matches!(
            coordinator.read("bucket", "missing.txt").await,
            Err(ClusterError::ObjectNotFound { .. })
        ));
        coordinator.head("bucket", "hello.txt").await.expect("found");
        assert!(coordinator.head("bucket", "missing.txt").await.is_err());

        for operation in ["read", "head"] {
            assert_eq!(
                metrics.value(
                    ops,
                    &[
                        ("operation", operation),
                        ("outcome", "ok"),
                        ("origin", "system")
                    ]
                ),
                Some(1.0)
            );
            assert_eq!(
                metrics.value(
                    ops,
                    &[
                        ("operation", operation),
                        ("outcome", "not_found"),
                        ("origin", "system")
                    ]
                ),
                Some(1.0)
            );
            assert_eq!(
                metrics.value(
                    "s4_quorum_duration_seconds_count",
                    &[("operation", operation), ("origin", "system")]
                ),
                Some(2.0)
            );
        }
        assert_eq!(metrics.sum(ops, &[("outcome", "error")]), 0.0);
    }

    // ---- The newest version, as hint delivery reads it ----

    mod newest_version {
        use super::*;
        use crate::coordinator::test_pool::{at, etag_of, put_with_hlc, Replica, TestPool};
        use crate::test_metrics::TestMetrics;
        use s4_core::storage::BitcaskStorageEngine;

        const TARGET: NodeId = NodeId(3);

        /// Node 1 of a three-node pool, RF=3, W=R=2, on real storage. Node 2
        /// is a replica on real storage that answers or is held dead. Node 3
        /// is the hint's target: held dead and never asked, or, for the tests
        /// that deliver, a replica on real storage that takes writes.
        struct Pool {
            coordinator: Arc<QuorumReadCoordinator>,
            local: Arc<BitcaskStorageEngine>,
            node_2: Arc<BitcaskStorageEngine>,
            /// The target's storage, when it runs.
            target: Option<Arc<BitcaskStorageEngine>>,
            rpc_client: Arc<NodeClient>,
            gossip: Arc<GossipHandle>,
            stand: TestPool,
        }

        async fn pool(node_2_answers: bool, base_port: u16) -> Pool {
            build_pool(node_2_answers, false, base_port).await
        }

        /// The pool with node 2 and the target both up.
        async fn pool_with_target(base_port: u16) -> Pool {
            build_pool(true, true, base_port).await
        }

        async fn build_pool(node_2_answers: bool, target_runs: bool, base_port: u16) -> Pool {
            let runs = |up: bool| if up { Replica::Up } else { Replica::Down };
            let stand = TestPool::start(runs(node_2_answers), runs(target_runs), base_port).await;
            Pool {
                coordinator: Arc::new(stand.read_coordinator()),
                local: stand.storage(1).clone(),
                node_2: stand.storage(2).clone(),
                target: target_runs.then(|| stand.storage(3).clone()),
                rpc_client: stand.rpc_client.clone(),
                gossip: stand.gossip.clone(),
                stand,
            }
        }

        async fn put(storage: &BitcaskStorageEngine, data: &[u8], hlc: HlcTimestamp) {
            put_with_hlc(storage, "key", data, hlc).await;
        }

        /// The newest version among the replicas other than the target, and
        /// its copy as that replica holds it — found without a quorum
        /// operation or a read repair being counted.
        #[tokio::test]
        async fn the_newest_version_is_the_newest_among_the_other_replicas() {
            let metrics = TestMetrics::install();
            let pool = pool(true, 19810).await;
            put(&pool.local, b"version 10", at(10)).await;
            put(&pool.node_2, b"version 20", at(20)).await;

            let newest = pool.coordinator.newest_version("bucket", "key", TARGET).await.unwrap();
            assert_eq!(
                newest,
                NewestVersion::Copy {
                    hlc: at(20),
                    etag: etag_of(b"version 20"),
                    node: NodeId(2),
                    confirmed: true,
                }
            );
            let copy = pool.coordinator.read_copy("bucket", "key", NodeId(2)).await.unwrap();
            assert_eq!(copy.hlc, Some(at(20)));
            assert_eq!(copy.data, b"version 20");
            assert_eq!(copy.metadata.get("owner").map(String::as_str), Some("test"));

            put(&pool.local, b"version 30", at(30)).await;
            let newest = pool.coordinator.newest_version("bucket", "key", TARGET).await.unwrap();
            assert_eq!(
                newest,
                NewestVersion::Copy {
                    hlc: at(30),
                    etag: etag_of(b"version 30"),
                    node: NodeId(1),
                    confirmed: true,
                }
            );
            let copy = pool.coordinator.read_copy("bucket", "key", NodeId(1)).await.unwrap();
            assert_eq!(
                (copy.hlc, copy.data),
                (Some(at(30)), b"version 30".to_vec())
            );

            assert_eq!(metrics.value("s4_quorum_operations_total", &[]), None);
            assert_eq!(metrics.sum("s4_quorum_operations_total", &[]), 0.0);
            assert_eq!(metrics.sum("s4_read_repairs_total", &[]), 0.0);
        }

        /// A copy one replica holds while the other has no key is confirmed
        /// when that replica keeps tombstones: a delete it took would be
        /// there as one. A replica that keeps none may have taken a delete the
        /// copy missed: with W=2 a confirmed write not on the target is then
        /// on both other replicas.
        #[tokio::test]
        async fn a_copy_beside_a_replica_without_the_key_is_confirmed_if_that_replica_keeps_tombstones(
        ) {
            let pool = pool(true, 19826).await;
            put(&pool.local, b"version 20", at(20)).await;
            let copy = |confirmed| NewestVersion::Copy {
                hlc: at(20),
                etag: etag_of(b"version 20"),
                node: NodeId(1),
                confirmed,
            };

            let newest = pool.coordinator.newest_version("bucket", "key", TARGET).await.unwrap();
            assert_eq!(newest, copy(true));

            pool.stand
                .announce_protocol(2, crate::protocol::TOMBSTONE_MIN_PROTOCOL_VERSION - 1);
            let newest = pool.coordinator.newest_version("bucket", "key", TARGET).await.unwrap();
            assert_eq!(newest, copy(false));

            put(&pool.node_2, b"version 20", at(20)).await;
            let newest = pool.coordinator.newest_version("bucket", "key", TARGET).await.unwrap();
            assert!(
                matches!(
                    newest,
                    NewestVersion::Copy {
                        confirmed: true,
                        ..
                    }
                ),
                "{newest:?}"
            );
        }

        /// A replica without the key confirms the copy beside it only when
        /// its tombstone horizon is older than the copy: a delete it took
        /// before the horizon may have left nothing, and the copy may be the
        /// one that missed it. So for the coordinating node's own storage.
        #[tokio::test]
        async fn a_copy_beside_a_replica_without_the_key_is_confirmed_only_past_its_horizon() {
            for (holder, horizons, confirmed, port) in [
                (1, [None, Some(at(10)), None], true, 19884),
                (1, [None, Some(at(30)), None], false, 19888),
                (2, [Some(at(30)), None, None], false, 19892),
            ] {
                let stand =
                    TestPool::start_with_horizons(Replica::Up, Replica::Down, port, horizons).await;
                put(stand.storage(holder), b"version 20", at(20)).await;
                let newest =
                    stand.read_coordinator().newest_version("bucket", "key", TARGET).await.unwrap();
                assert_eq!(
                    newest,
                    NewestVersion::Copy {
                        hlc: at(20),
                        etag: etag_of(b"version 20"),
                        node: NodeId(holder),
                        confirmed,
                    },
                    "horizons {horizons:?}"
                );
            }
        }

        #[tokio::test]
        async fn a_key_no_other_replica_holds_is_absent() {
            let pool = pool(true, 19814).await;
            let newest = pool.coordinator.newest_version("bucket", "key", TARGET).await.unwrap();
            assert_eq!(newest, NewestVersion::Absent);
        }

        /// The replica that did not answer may be the one holding the newest
        /// version: that is an error, never "absent". One gossip holds dead is
        /// not even asked, so delivery that waits for it costs no call.
        #[tokio::test]
        async fn a_replica_that_does_not_answer_is_an_error_not_an_absence() {
            let metrics = TestMetrics::install();
            let pool = pool(false, 19818).await;
            put(&pool.local, b"version 10", at(10)).await;
            let result = pool.coordinator.newest_version("bucket", "key", TARGET).await;
            assert!(result.is_err(), "{result:?}");
            assert_eq!(
                metrics.sum("s4_peer_requests_total", &[]),
                0.0,
                "{}",
                metrics.render()
            );
        }

        #[tokio::test]
        async fn a_node_that_is_not_a_replica_of_the_key_has_nothing_to_catch_up() {
            let pool = pool(true, 19822).await;
            let newest = pool.coordinator.newest_version("bucket", "key", NodeId(9)).await.unwrap();
            assert_eq!(newest, NewestVersion::NotAReplica);
        }

        /// A record written without an HLC is copied with the one every node
        /// makes up for it, node 0, whether it is read locally or from a
        /// replica.
        #[tokio::test]
        async fn a_copy_written_without_an_hlc_carries_the_made_up_one() {
            let pool = pool(true, 19852).await;
            for (node, storage) in [(NodeId(1), &pool.local), (NodeId(2), &pool.node_2)] {
                storage
                    .put_object("bucket", "key", b"no hlc", "text/plain", &HashMap::new())
                    .await
                    .unwrap();
                let copy = pool.coordinator.read_copy("bucket", "key", node).await.unwrap();
                assert_eq!(
                    copy.hlc,
                    Some(made_up_hlc(storage).await),
                    "read from {node}"
                );
                assert_eq!(copy.data, b"no hlc");
            }
        }

        /// The HLC made up for a record written without one, node 0, as every
        /// replica reports it.
        async fn made_up_hlc(storage: &BitcaskStorageEngine) -> HlcTimestamp {
            let record = storage.head_object("bucket", "key").await.unwrap();
            HlcTimestamp::from_record(record.modified_at / 1_000_000, 0, 0)
        }

        /// A record written without an HLC is reported with the same made-up
        /// HLC whether it is read locally or from a replica, whole or as a
        /// stream: digests from different coordinators compare it alike.
        #[tokio::test]
        async fn a_record_without_an_hlc_is_reported_alike_on_every_path() {
            let pool = pool(true, 19856).await;
            for (node, storage) in [(NodeId(1), &pool.local), (NodeId(2), &pool.node_2)] {
                storage
                    .put_object("bucket", "key", b"no hlc", "text/plain", &HashMap::new())
                    .await
                    .unwrap();
                let made_up = made_up_hlc(storage).await;

                match pool.coordinator.digest_task(node, "bucket", "key").await {
                    DigestResult::Success(digest) => {
                        assert_eq!(digest.hlc, made_up, "digest from {node}")
                    }
                    other => panic!("digest from {node}: {other:?}"),
                }
                let stream = pool
                    .coordinator
                    .open_stream_from_replica("bucket", "key", node, None)
                    .await
                    .unwrap();
                assert_eq!(stream.hlc, made_up, "stream from {node}");
            }
        }

        /// Read repair writes the newest version with its HLC. Taken from a
        /// record written without one, that is the made-up HLC, node 0: the
        /// repaired copies are still told apart from a write the cluster
        /// ordered, and delivery copies them as made up.
        #[tokio::test]
        async fn a_copy_repaired_from_a_record_without_an_hlc_stays_made_up() {
            let pool = pool_with_target(19868).await;
            let target = pool.target.clone().unwrap();
            pool.local
                .put_object("bucket", "key", b"no hlc", "text/plain", &HashMap::new())
                .await
                .unwrap();
            put(&pool.node_2, b"version 10", at(10)).await;
            put(&target, b"version 10", at(10)).await;
            let made_up = made_up_hlc(&pool.local).await;

            let read = pool.coordinator.read("bucket", "key").await.unwrap();
            assert_eq!((read.hlc, read.data), (made_up, b"no hlc".to_vec()));
            assert!(read.repair_triggered);

            for (node, storage) in [(NodeId(2), &pool.node_2), (TARGET, &target)] {
                let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
                while hlc_on(storage).await == Some(10) {
                    assert!(
                        tokio::time::Instant::now() < deadline,
                        "{node} was not repaired"
                    );
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                let record = storage.head_object("bucket", "key").await.unwrap();
                assert_eq!(HlcTimestamp::of_record(&record), made_up, "{node}");
            }
            let copy = pool.coordinator.read_copy("bucket", "key", NodeId(2)).await.unwrap();
            assert_eq!((copy.hlc, copy.data), (Some(made_up), b"no hlc".to_vec()));
        }

        // ---- Hint delivery over real replicas ----

        use crate::hints::{HintDelivery, HintEntry, HintedHandoffConfig, HintedHandoffManager};

        /// A manager storing hints for the pool's target and a delivery that
        /// reads current states through the pool's coordinator.
        fn delivering(pool: &Pool, dir: &std::path::Path) -> (HintedHandoffManager, HintDelivery) {
            let manager =
                HintedHandoffManager::new(HintedHandoffConfig::new(dir), pool.rpc_client.clone());
            let delivery = HintDelivery::new(
                pool.coordinator.clone(),
                pool.rpc_client.clone(),
                pool.gossip.clone(),
                NodeId(1),
                1,
            );
            (manager, delivery)
        }

        async fn hlc_on(storage: &BitcaskStorageEngine) -> Option<u64> {
            storage
                .head_object("bucket", "key")
                .await
                .ok()
                .map(|record| record.hlc_timestamp)
        }

        /// The target missed a write that node 1 and node 2 took. Before the
        /// hint is delivered the key is deleted through node 2, which the
        /// target takes and node 1 misses. Both keep the delete as a
        /// tombstone newer than node 1's copy: the newest version is the
        /// delete, the target already holds it, and the deleted key does not
        /// come back.
        #[tokio::test]
        async fn a_copy_that_missed_a_delete_is_not_brought_back_by_a_hint() {
            let metrics = TestMetrics::install();
            let tmp = tempfile::tempdir().unwrap();
            let pool = pool_with_target(19840).await;
            let target = pool.target.clone().unwrap();
            put(&pool.local, b"version 20", at(20)).await;
            pool.stand.delete(2, "key", at(30)).await;
            pool.stand.delete(3, "key", at(30)).await;
            let (manager, delivery) = delivering(&pool, tmp.path());

            manager.store(TARGET, HintEntry::written("bucket", "key", at(20))).await;
            manager.deliver(&delivery).await;

            assert_eq!(hlc_on(&target).await, None, "the deleted key came back");
            assert_eq!(tombstone_on(&target).await, Some(at(30)));
            assert_eq!(metrics.value("s4_hints_delivered_total", &[]), Some(1.0));
            assert_eq!(metrics.sum("s4_hints_dropped_total", &[]), 0.0);
            assert_eq!(manager.pending(TARGET).await, 0);
        }

        /// The same, with node 2 from before tombstones: it took the delete
        /// and has no key, which cannot be told from a replica that never got
        /// the write. Node 1's copy may be one that missed the delete: it is
        /// not spread, and the hint is dropped as ambiguous.
        #[tokio::test]
        async fn a_copy_beside_a_replica_before_tombstones_without_the_key_is_not_spread() {
            let metrics = TestMetrics::install();
            let tmp = tempfile::tempdir().unwrap();
            let pool = pool_with_target(21032).await;
            let target = pool.target.clone().unwrap();
            put(&pool.local, b"version 20", at(20)).await;
            pool.stand
                .announce_protocol(2, crate::protocol::TOMBSTONE_MIN_PROTOCOL_VERSION - 1);
            let (manager, delivery) = delivering(&pool, tmp.path());

            manager.store(TARGET, HintEntry::written("bucket", "key", at(20))).await;
            manager.deliver(&delivery).await;

            assert_eq!(hlc_on(&target).await, None, "the deleted key came back");
            assert_eq!(
                metrics.value("s4_hints_dropped_total", &[("reason", "ambiguous")]),
                Some(1.0),
                "{}",
                metrics.render()
            );
            assert_eq!(manager.pending(TARGET).await, 0);
        }

        /// A copy one replica holds, beside a replica that keeps tombstones
        /// and holds neither the key nor a tombstone, is the newest version:
        /// no delete is in play, and it reaches the target.
        #[tokio::test]
        async fn a_copy_beside_a_replica_with_neither_the_key_nor_a_tombstone_is_delivered() {
            let metrics = TestMetrics::install();
            let tmp = tempfile::tempdir().unwrap();
            let pool = pool_with_target(21036).await;
            let target = pool.target.clone().unwrap();
            put(&pool.local, b"version 20", at(20)).await;
            let (manager, delivery) = delivering(&pool, tmp.path());

            manager.store(TARGET, HintEntry::written("bucket", "key", at(20))).await;
            manager.deliver(&delivery).await;

            assert_eq!(hlc_on(&target).await, Some(20));
            assert_eq!(metrics.value("s4_hints_delivered_total", &[]), Some(1.0));
            assert_eq!(metrics.sum("s4_hints_dropped_total", &[]), 0.0);
        }

        /// A delete the target missed reaches it as the tombstone the other
        /// replicas keep. A target from before tombstones loses its copy
        /// instead, by exactly the copy's generation.
        #[tokio::test]
        async fn a_delete_hint_brings_its_tombstone_to_the_target() {
            let metrics = TestMetrics::install();
            let tmp = tempfile::tempdir().unwrap();
            let pool = pool_with_target(21040).await;
            let target = pool.target.clone().unwrap();
            for key in ["current", "old-release"] {
                pool.stand.put(3, key, b"version 10", at(10)).await;
                pool.stand.delete(1, key, at(20)).await;
                pool.stand.delete(2, key, at(20)).await;
            }
            let (manager, delivery) = delivering(&pool, tmp.path());

            manager.store(TARGET, HintEntry::deleted("bucket", "current", at(20))).await;
            manager.deliver(&delivery).await;
            assert!(matches!(
                target.key_version("bucket", "current").await.unwrap(),
                s4_core::KeyVersion::Tombstone(record) if HlcTimestamp::of_record(&record) == at(20)
            ));

            pool.stand
                .announce_protocol(3, crate::protocol::TOMBSTONE_MIN_PROTOCOL_VERSION - 1);
            manager.store(TARGET, HintEntry::deleted("bucket", "old-release", at(20))).await;
            manager.deliver(&delivery).await;
            assert!(matches!(
                target.key_version("bucket", "old-release").await.unwrap(),
                s4_core::KeyVersion::Absent
            ));

            assert_eq!(metrics.value("s4_hints_delivered_total", &[]), Some(2.0));
            assert_eq!(metrics.sum("s4_hints_dropped_total", &[]), 0.0);
        }

        /// A delete of a key that keeps no tombstone — erasure-coding
        /// metadata in `__system__` — reaches the target as before: a record no newer
        /// than the delete goes by its exact generation, no tombstone is left,
        /// and a record written after the delete stays.
        #[tokio::test]
        async fn a_delete_hint_of_a_system_record_removes_it_without_a_tombstone() {
            let metrics = TestMetrics::install();
            let tmp = tempfile::tempdir().unwrap();
            let pool = pool_with_target(21048).await;
            let target = pool.target.clone().unwrap();
            for (key, wall) in [
                ("__s4_ec_v1/tiering/older", 10),
                ("__s4_ec_v1/tiering/newer", 30),
            ] {
                let metadata = HashMap::from([
                    ("_s4_hlc_wall".to_string(), wall.to_string()),
                    ("_s4_hlc_logical".to_string(), "0".to_string()),
                    ("_s4_hlc_node_id".to_string(), "7".to_string()),
                ]);
                target
                    .put_object("__system__", key, b"{}", "application/json", &metadata)
                    .await
                    .unwrap();
            }
            let (manager, delivery) = delivering(&pool, tmp.path());

            for key in ["__s4_ec_v1/tiering/older", "__s4_ec_v1/tiering/newer"] {
                manager.store(TARGET, HintEntry::deleted("__system__", key, at(20))).await;
            }
            manager.deliver(&delivery).await;

            assert!(matches!(
                target.key_version("__system__", "__s4_ec_v1/tiering/older").await.unwrap(),
                s4_core::KeyVersion::Absent
            ));
            assert!(matches!(
                target.key_version("__system__", "__s4_ec_v1/tiering/newer").await.unwrap(),
                s4_core::KeyVersion::Live(record) if record.hlc_timestamp == 30
            ));
            assert_eq!(metrics.value("s4_hints_delivered_total", &[]), Some(2.0));
        }

        /// The current state of the key, read through the pool, except that
        /// the key is deleted on the target right after its copy is read:
        /// the delete lands between the read and the write that delivers it.
        struct DeletedMeanwhile {
            pool: Arc<QuorumReadCoordinator>,
            target: Arc<BitcaskStorageEngine>,
            deleted: HlcTimestamp,
        }

        #[async_trait::async_trait]
        impl KeyStateSource for DeletedMeanwhile {
            async fn newest_version(
                &self,
                bucket: &str,
                key: &str,
                target: NodeId,
            ) -> Result<NewestVersion, ClusterError> {
                self.pool.newest_version(bucket, key, target).await
            }

            async fn key_versions(
                &self,
                bucket: &str,
                key: &str,
            ) -> Result<KeyVersions, ClusterError> {
                self.pool.key_versions(bucket, key).await
            }

            async fn read_copy(
                &self,
                bucket: &str,
                key: &str,
                node: NodeId,
            ) -> Result<StoredCopy, ClusterError> {
                let copy = self.pool.read_copy(bucket, key, node).await?;
                let deleted = self.deleted;
                self.target
                    .tombstone_object(
                        bucket,
                        key,
                        deleted.wall_time,
                        deleted.logical,
                        deleted.node_id,
                    )
                    .await
                    .unwrap();
                Ok(copy)
            }
        }

        /// The window hinted handoff shared with read repair: a key deleted
        /// between reading its newest copy and writing it to the target. The
        /// target took the delete as a tombstone newer than the copy, its LWW
        /// refuses the copy, and the deleted key does not come back.
        #[tokio::test]
        async fn a_key_deleted_while_its_copy_is_on_the_way_does_not_come_back() {
            let tmp = tempfile::tempdir().unwrap();
            let pool = pool_with_target(21044).await;
            let target = pool.target.clone().unwrap();
            put(&pool.local, b"version 20", at(20)).await;
            put(&pool.node_2, b"version 20", at(20)).await;
            let manager = HintedHandoffManager::new(
                HintedHandoffConfig::new(tmp.path()),
                pool.rpc_client.clone(),
            );
            let delivery = HintDelivery::new(
                Arc::new(DeletedMeanwhile {
                    pool: pool.coordinator.clone(),
                    target: target.clone(),
                    deleted: at(30),
                }),
                pool.rpc_client.clone(),
                pool.gossip.clone(),
                NodeId(1),
                1,
            );

            manager.store(TARGET, HintEntry::written("bucket", "key", at(20))).await;
            manager.deliver(&delivery).await;

            assert_eq!(hlc_on(&target).await, None, "the deleted key came back");
            assert_eq!(tombstone_on(&target).await, Some(at(30)));
        }

        async fn tombstone_on(storage: &BitcaskStorageEngine) -> Option<HlcTimestamp> {
            match storage.key_version("bucket", "key").await.unwrap() {
                s4_core::KeyVersion::Tombstone(record) => Some(HlcTimestamp::of_record(&record)),
                _ => None,
            }
        }

        /// The same hint once both other replicas hold the copy: a confirmed
        /// write, spread to the target.
        #[tokio::test]
        async fn a_copy_both_other_replicas_hold_is_delivered() {
            let metrics = TestMetrics::install();
            let tmp = tempfile::tempdir().unwrap();
            let pool = pool_with_target(19844).await;
            let target = pool.target.clone().unwrap();
            put(&pool.local, b"version 20", at(20)).await;
            put(&pool.node_2, b"version 20", at(20)).await;
            let (manager, delivery) = delivering(&pool, tmp.path());

            manager.store(TARGET, HintEntry::written("bucket", "key", at(20))).await;
            manager.deliver(&delivery).await;

            assert_eq!(hlc_on(&target).await, Some(20));
            assert_eq!(metrics.value("s4_hints_delivered_total", &[]), Some(1.0));
            assert_eq!(metrics.sum("s4_hints_dropped_total", &[]), 0.0);
        }

        /// Every other replica holds some version: no delete is in play, and
        /// the newest is spread even though only one of them holds it.
        #[tokio::test]
        async fn the_newest_copy_is_delivered_when_no_replica_lacks_the_key() {
            let tmp = tempfile::tempdir().unwrap();
            let pool = pool_with_target(19848).await;
            let target = pool.target.clone().unwrap();
            put(&pool.local, b"version 30", at(30)).await;
            put(&pool.node_2, b"version 20", at(20)).await;
            let (manager, delivery) = delivering(&pool, tmp.path());

            manager.store(TARGET, HintEntry::written("bucket", "key", at(20))).await;
            manager.deliver(&delivery).await;

            assert_eq!(hlc_on(&target).await, Some(30));
        }

        /// A key stored before the cluster formed lives only on the node that
        /// had it. Deleted through the cluster while that node was down, it is
        /// removed from the node once it is back: its record stores no HLC,
        /// and the delete on the zero HLC matches it.
        #[tokio::test]
        async fn a_key_deleted_while_its_only_holder_was_down_is_removed_from_it() {
            let metrics = TestMetrics::install();
            let tmp = tempfile::tempdir().unwrap();
            let pool = pool_with_target(19876).await;
            let target = pool.target.clone().unwrap();
            target
                .put_object(
                    "bucket",
                    "key",
                    b"before the cluster",
                    "text/plain",
                    &HashMap::new(),
                )
                .await
                .unwrap();
            let deleted_at =
                HlcTimestamp::from_record(made_up_hlc(&target).await.wall_time + 1, 0, 7);
            let (manager, delivery) = delivering(&pool, tmp.path());

            manager.store(TARGET, HintEntry::deleted("bucket", "key", deleted_at)).await;
            manager.deliver(&delivery).await;

            assert!(target.head_object("bucket", "key").await.is_err());
            assert_eq!(metrics.value("s4_hints_delivered_total", &[]), Some(1.0));
            assert_eq!(metrics.sum("s4_hints_dropped_total", &[]), 0.0);
        }

        /// The newest version among the other replicas is a record written
        /// without an HLC, and no replica lacks the key: its copy reaches the
        /// target with the made-up HLC, as read repair would bring it.
        #[tokio::test]
        async fn a_copy_without_an_hlc_reaches_a_replica_that_lacks_the_key() {
            let tmp = tempfile::tempdir().unwrap();
            let pool = pool_with_target(19880).await;
            let target = pool.target.clone().unwrap();
            put(&pool.node_2, b"version 10", at(10)).await;
            pool.local
                .put_object("bucket", "key", b"no hlc", "text/plain", &HashMap::new())
                .await
                .unwrap();
            let made_up = made_up_hlc(&pool.local).await;
            let (manager, delivery) = delivering(&pool, tmp.path());

            manager.store(TARGET, HintEntry::written("bucket", "key", at(10))).await;
            manager.deliver(&delivery).await;

            let (data, record) = target.get_object("bucket", "key").await.unwrap();
            assert_eq!(
                (HlcTimestamp::of_record(&record), data),
                (made_up, b"no hlc".to_vec())
            );
        }

        fn digest(node: u128, wall: u64, erasure_coded: bool) -> ObjectDigest {
            ObjectDigest {
                node_id: NodeId(node),
                hlc: at(wall),
                etag: String::new(),
                size: 0,
                content_type: String::new(),
                version_id: None,
                metadata: HashMap::new(),
                erasure_coded,
                content_hash: None,
                tombstone: false,
            }
        }

        /// One replica holding the erasure-coded record of the newest version
        /// is enough: the object is leaving replication, and no copy of it
        /// is to be spread again.
        #[test]
        fn an_erasure_coded_newest_version_is_reported_as_such() {
            assert_eq!(
                newest_of(&[digest(1, 10, false), digest(2, 20, true)], &[], 2),
                NewestVersion::ErasureCoded { hlc: at(20) }
            );
            assert_eq!(
                newest_of(&[digest(1, 20, false), digest(2, 20, true)], &[], 2),
                NewestVersion::ErasureCoded { hlc: at(20) }
            );
            assert_eq!(
                newest_of(&[digest(1, 30, false), digest(2, 20, true)], &[], 2),
                NewestVersion::Copy {
                    hlc: at(30),
                    etag: s4_core::storage::EtagRank::of(""),
                    node: NodeId(1),
                    confirmed: true,
                }
            );
            assert_eq!(newest_of(&[], &[None, None], 2), NewestVersion::Absent);
        }

        /// A copy is confirmed when every replica that lacks the key keeps
        /// tombstones from before the copy, or when as many replicas hold it
        /// as a confirmed write leaves behind.
        #[test]
        fn a_copy_is_confirmed_by_the_write_quorum_or_by_no_replica_lacking_the_key() {
            let confirmed =
                |digests: &[ObjectDigest], absent: &[Option<HlcTimestamp>], confirming| {
                    match newest_of(digests, absent, confirming) {
                        NewestVersion::Copy { confirmed, .. } => confirmed,
                        other => panic!("not a copy: {other:?}"),
                    }
                };
            // RF=3, W=2: two other replicas, a confirmed write is on both.
            assert!(confirmed(
                &[digest(1, 20, false), digest(2, 20, false)],
                &[],
                2
            ));
            assert!(!confirmed(&[digest(1, 20, false)], &[None], 2));
            assert!(confirmed(
                &[digest(1, 30, false), digest(2, 20, false)],
                &[],
                2
            ));
            // RF=5, W=3: four other replicas, a confirmed write is on three.
            let three = [
                digest(1, 20, false),
                digest(2, 20, false),
                digest(3, 20, false),
            ];
            assert!(confirmed(&three, &[None], 3));
            assert!(!confirmed(&three[..2], &[None, None], 3));
            assert!(!confirmed(
                &[three[0].clone(), three[1].clone(), digest(3, 10, false)],
                &[None],
                3
            ));
        }

        /// A replica without the key confirms the copy only when it keeps
        /// tombstones from before the copy was written: a delete of the copy
        /// would have left one there. Its delete from before its horizon may
        /// have left nothing (plan ANTI-ENTROPY.md, decision 7).
        #[test]
        fn a_replica_without_the_key_confirms_only_a_copy_newer_than_its_horizon() {
            let copy = [digest(1, 20, false)];
            let confirmed = |absent: &[Option<HlcTimestamp>]| match newest_of(&copy, absent, 2) {
                NewestVersion::Copy { confirmed, .. } => confirmed,
                other => panic!("not a copy: {other:?}"),
            };
            assert!(confirmed(&[Some(HlcTimestamp::zero())]));
            assert!(confirmed(&[Some(at(10))]));
            assert!(!confirmed(&[Some(at(20))]), "a horizon at the copy itself");
            assert!(!confirmed(&[Some(at(30))]));
            assert!(!confirmed(&[None]));
            assert!(!confirmed(&[Some(at(10)), Some(at(30))]));
        }
    }

    // ---- A deleted key, as every read path answers it ----

    mod deleted_keys {
        use super::*;
        use crate::coordinator::test_pool::{at, Replica, TestPool, BUCKET};
        use s4_core::KeyVersion;

        /// What every read path of `coordinator` answers for `key` that is
        /// not "not found": the whole read, the stream and the HEAD.
        async fn found(coordinator: &QuorumReadCoordinator, key: &str) -> Vec<String> {
            let read = coordinator.read(BUCKET, key).await.map(|read| read.data.len() as u64);
            let stream =
                coordinator.read_stream(BUCKET, key, None).await.map(|stream| stream.total_size);
            let head = coordinator.head(BUCKET, key).await.map(|head| head.size);
            [("read", read), ("read_stream", stream), ("head", head)]
                .into_iter()
                .filter(|(_, answer)| !matches!(answer, Err(ClusterError::ObjectNotFound { .. })))
                .map(|(path, answer)| match answer {
                    Ok(size) => format!("{key}: {path} served {size} bytes"),
                    Err(error) => format!("{key}: {path} failed with {error:?}"),
                })
                .collect()
        }

        /// A replica that missed both the creation and the delete of a bucket
        /// makes its marker up when a late write into the bucket reaches it.
        /// That marker is older than the delete the other replicas hold, so
        /// the bucket stays deleted: HEAD of its marker finds nothing, and
        /// the listing of markers does not show it.
        #[tokio::test]
        async fn a_marker_a_replica_makes_up_does_not_bring_a_deleted_bucket_back() {
            let pool = TestPool::start(Replica::Up, Replica::Up, 21060).await;
            let marker = format!("{}{BUCKET}", s4_core::types::BUCKET_MARKER_KEY_PREFIX);
            for node in [1, 2] {
                assert!(pool
                    .storage(node)
                    .tombstone_object("__system__", &marker, 20, 0, 1)
                    .await
                    .unwrap());
            }
            let handler = crate::replica::ReplicaWriteHandler::new(
                pool.storage(3).clone(),
                Arc::new(crate::idempotency::IdempotencyTracker::new()),
                1,
            );
            let late_write = proto::WriteBlobRequest {
                bucket: BUCKET.to_string(),
                key: "late-write".to_string(),
                data: b"bytes".to_vec(),
                hlc: Some(at(15).to_proto()),
                operation_id: uuid::Uuid::new_v4().as_u128().to_be_bytes().to_vec(),
                topology_epoch: 1,
                ..Default::default()
            };
            assert!(handler.handle_write(&late_write).await.unwrap().success);
            assert!(matches!(
                pool.storage(3).key_version("__system__", &marker).await.unwrap(),
                KeyVersion::Live(_)
            ));

            let head = pool.read_coordinator().head("__system__", &marker).await;
            assert!(
                matches!(head, Err(ClusterError::ObjectNotFound { .. })),
                "{head:?}"
            );
            let listed = pool
                .list_coordinator()
                .list(
                    "__system__",
                    s4_core::types::BUCKET_MARKER_KEY_PREFIX,
                    100,
                    None,
                )
                .await
                .unwrap();
            assert!(listed.entries.is_empty(), "{:?}", listed.entries);
        }

        /// The delete reached two replicas of three and the third kept its
        /// copy, wherever that copy is: the delete is the newest version of
        /// the key, and every read path answers that the object is not there.
        /// Without a trace of the delete the two replicas that took it hold
        /// nothing, the old copy is the only version that answers, and the
        /// read fails on its quorum instead.
        #[tokio::test]
        async fn a_key_deleted_on_two_replicas_is_not_found_beside_the_copy_that_missed_it() {
            let pool = TestPool::start(Replica::Up, Replica::Up, 21000).await;
            let coordinator = pool.read_coordinator();
            let mut found_deleted = Vec::new();
            for kept_by in 1..=3 {
                let key = format!("kept-by-{kept_by}");
                for node in 1..=3 {
                    pool.put(node, &key, b"version 10", at(10)).await;
                }
                for node in (1..=3).filter(|node| *node != kept_by) {
                    pool.delete(node, &key, at(20)).await;
                }
                found_deleted.extend(found(&coordinator, &key).await);
            }
            assert!(found_deleted.is_empty(), "{found_deleted:#?}");
        }

        /// The delete reached one replica of three, and two kept their
        /// copies: the delete is still the newest version. The replica that
        /// took it is the coordinating node, which every read through it
        /// hears; a quorum read hears a remote replica only when it is among
        /// the first R to answer. Without a trace of the delete the two old
        /// copies are all that answers, and the deleted object is served.
        #[tokio::test]
        async fn a_key_deleted_on_one_replica_is_not_found_beside_two_copies_that_missed_it() {
            let pool = TestPool::start(Replica::Up, Replica::Up, 21004).await;
            let coordinator = pool.read_coordinator();
            for node in 1..=3 {
                pool.put(node, "key", b"version 10", at(10)).await;
            }
            pool.delete(1, "key", at(20)).await;
            let found_deleted = found(&coordinator, "key").await;
            assert!(found_deleted.is_empty(), "{found_deleted:#?}");
        }

        /// What node `node` of `pool` holds for `key`, once `settled` says it
        /// is what the test waits for; read repair runs after the read.
        async fn settled_version(
            pool: &TestPool,
            node: u128,
            key: &str,
            settled: impl Fn(&KeyVersion) -> bool,
        ) -> KeyVersion {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
            loop {
                let version = pool.storage(node).key_version(BUCKET, key).await.unwrap();
                if settled(&version) || tokio::time::Instant::now() >= deadline {
                    return version;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }

        /// A read that finds a tombstone newer than a replica's copy answers
        /// "not found" and brings that replica to the delete. A replica that
        /// keeps tombstones takes the tombstone; one before protocol 4 loses
        /// its copy by exactly the copy's generation, the only delete that
        /// cannot take a newer write along. Each is one read repair. Node 3 is
        /// down, so node 2's copy is always among the versions the read
        /// weighs.
        #[tokio::test]
        async fn a_read_brings_a_copy_older_than_its_tombstone_to_the_delete() {
            let metrics = crate::test_metrics::TestMetrics::install();
            let pool = TestPool::start(Replica::Up, Replica::Down, 21020).await;
            let coordinator = pool.read_coordinator();
            for key in ["current", "old-release"] {
                pool.put(1, key, b"version 10", at(10)).await;
                pool.put(2, key, b"version 10", at(10)).await;
                pool.delete(1, key, at(20)).await;
            }

            let read = coordinator.read(BUCKET, "current").await;
            assert!(
                matches!(read, Err(ClusterError::ObjectNotFound { .. })),
                "{read:?}"
            );
            let repaired = settled_version(&pool, 2, "current", |version| {
                matches!(version, KeyVersion::Tombstone(_))
            })
            .await;
            assert!(
                matches!(&repaired, KeyVersion::Tombstone(record) if HlcTimestamp::of_record(record) == at(20)),
                "{repaired:?}"
            );

            pool.announce_protocol(2, 3);
            let read = coordinator.read(BUCKET, "old-release").await;
            assert!(
                matches!(read, Err(ClusterError::ObjectNotFound { .. })),
                "{read:?}"
            );
            let repaired = settled_version(&pool, 2, "old-release", |version| {
                matches!(version, KeyVersion::Absent)
            })
            .await;
            assert!(matches!(repaired, KeyVersion::Absent), "{repaired:?}");

            // The replica applies a repair before the repair hears back and
            // counts it.
            let repairs = "s4_read_repairs_total";
            let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
            while metrics.value(repairs, &[("outcome", "ok")]) != Some(2.0)
                && tokio::time::Instant::now() < deadline
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            assert_eq!(
                metrics.value(repairs, &[("outcome", "ok")]),
                Some(2.0),
                "{}",
                metrics.render()
            );
            assert_eq!(metrics.value(repairs, &[("outcome", "failed")]), None);
        }

        /// Two copies of one HLC with different ETags: the read serves the one
        /// ranked higher — the multipart ETag — whichever replica holds it,
        /// and read repair brings the other replica to it, which its LWW now
        /// takes. Before, the read served whichever answered last, and the
        /// repair was turned away on every read.
        #[tokio::test]
        async fn a_read_serves_the_copy_ranked_higher_and_repairs_the_other() {
            let multipart = "9b2cf535f27731c974343645a3985328-2";
            // Read repair reaches a replica through its server, which node 1,
            // the coordinator here, does not run: the other copies are on
            // nodes 2 and 3.
            for (holder, other, port) in [(2u128, 3u128, 21208), (3, 2, 21212)] {
                let pool = TestPool::start(Replica::Up, Replica::Up, port).await;
                pool.put_with_etag(holder, "key", b"same bytes", at(20), multipart).await;
                pool.put(other, "key", b"same bytes", at(20)).await;
                pool.put(1, "key", b"same bytes", at(20)).await;
                let coordinator = pool.read_coordinator();

                let read = coordinator.read(BUCKET, "key").await.unwrap();
                assert_eq!(read.etag, multipart, "multipart copy on node {holder}");
                assert!(read.repair_triggered);
                let head = coordinator.head(BUCKET, "key").await.unwrap();
                assert_eq!(head.etag, multipart);

                let repaired = settled_version(&pool, other, "key", |version| {
                    matches!(version, KeyVersion::Live(record) if record.etag == multipart)
                })
                .await;
                assert!(
                    matches!(&repaired, KeyVersion::Live(record) if record.etag == multipart),
                    "node {other}: {repaired:?}"
                );
            }
        }

        /// A copy written after the delete is the newest version: it is
        /// served, and the replica still holding the tombstone takes it.
        #[tokio::test]
        async fn a_copy_newer_than_a_tombstone_is_served_and_replaces_it() {
            let pool = TestPool::start(Replica::Up, Replica::Down, 21024).await;
            let coordinator = pool.read_coordinator();
            pool.put(2, "key", b"version 10", at(10)).await;
            pool.delete(2, "key", at(20)).await;
            pool.put(1, "key", b"version 30", at(30)).await;

            let read = coordinator.read(BUCKET, "key").await.unwrap();
            assert_eq!(
                (read.data.as_slice(), read.hlc),
                (&b"version 30"[..], at(30))
            );
            assert!(read.repair_triggered);
            let head = coordinator.head(BUCKET, "key").await.unwrap();
            assert_eq!(head.hlc, at(30));

            let repaired = settled_version(&pool, 2, "key", |version| {
                matches!(version, KeyVersion::Live(_))
            })
            .await;
            assert!(
                matches!(&repaired, KeyVersion::Live(record) if HlcTimestamp::of_record(record) == at(30)),
                "{repaired:?}"
            );
        }

        fn version(node: u128, wall: u64, tombstone: bool) -> ObjectDigest {
            let hlc = at(wall);
            if tombstone {
                ObjectDigest::of_tombstone(NodeId(node), hlc)
            } else {
                ObjectDigest {
                    tombstone: false,
                    ..ObjectDigest::of_tombstone(NodeId(node), hlc)
                }
            }
        }

        /// The newest version decides, a tombstone included. An object needs
        /// R replicas holding a version; "not found" needs R replicas that
        /// answered at all, since a replica without the key may have taken a
        /// delete from before tombstones but never holds a newer object.
        #[test]
        fn the_newest_version_decides_a_quorum_read() {
            const LIVE: bool = false;
            const TOMBSTONE: bool = true;
            let decision = |digests: &[ObjectDigest], absent| decide(digests, absent, 2);

            assert_eq!(
                decision(&[version(1, 10, LIVE), version(2, 20, TOMBSTONE)], 0),
                ReadDecision::NotFound(Some(1))
            );
            assert_eq!(
                decision(&[version(1, 30, LIVE), version(2, 20, TOMBSTONE)], 0),
                ReadDecision::Serve(0)
            );
            assert_eq!(
                decision(&[version(1, 20, TOMBSTONE)], 1),
                ReadDecision::NotFound(Some(0))
            );
            assert_eq!(decision(&[], 2), ReadDecision::NotFound(None));
            assert_eq!(
                decision(&[version(1, 10, LIVE)], 1),
                ReadDecision::QuorumNotMet
            );
            assert_eq!(
                decision(&[version(1, 20, TOMBSTONE)], 0),
                ReadDecision::QuorumNotMet
            );
            assert_eq!(decision(&[], 1), ReadDecision::QuorumNotMet);
        }

        /// A replica's HEAD answer is a version, an absence, or a failure;
        /// one that carries an error is a failure even though it says the
        /// key does not exist, and one from before tombstones is an absence.
        #[test]
        fn a_head_answer_is_a_version_an_absence_or_a_failure() {
            let node = NodeId(2);
            let tombstone = proto::HeadObjectResponse {
                tombstone: true,
                hlc: Some(at(20).to_proto()),
                ..Default::default()
            };
            assert!(matches!(
                digest_of_head(node, tombstone),
                DigestResult::Success(digest) if digest.tombstone && digest.hlc == at(20)
            ));
            let object = proto::HeadObjectResponse {
                exists: true,
                hlc: Some(at(10).to_proto()),
                content_length: 7,
                ..Default::default()
            };
            assert!(matches!(
                digest_of_head(node, object),
                DigestResult::Success(digest) if !digest.tombstone && digest.size == 7
            ));
            assert!(matches!(
                digest_of_head(node, proto::HeadObjectResponse::default()),
                DigestResult::NotFound(..)
            ));
            let failed = proto::HeadObjectResponse {
                error: "storage error: disk refuses".into(),
                ..Default::default()
            };
            assert!(matches!(
                digest_of_head(node, failed),
                DigestResult::Error(..)
            ));
            let unordered = proto::HeadObjectResponse {
                tombstone: true,
                ..Default::default()
            };
            assert!(matches!(
                digest_of_head(node, unordered),
                DigestResult::Error(..)
            ));
            let other_epoch = proto::HeadObjectResponse {
                epoch_mismatch: true,
                error: "epoch mismatch".into(),
                ..Default::default()
            };
            assert!(matches!(
                digest_of_head(node, other_epoch),
                DigestResult::EpochMismatch(_)
            ));
        }
    }
}
