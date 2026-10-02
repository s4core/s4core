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
//! 3. Wait for R=2 responses
//! 4a. If versions match (same HLC + ETag):
//!     Read full data from nearest/first replica
//! 4b. If versions differ:
//!     Read full data from node with latest version (LWW by HLC)
//!     Trigger async read repair on stale replica(s)
//! 5. If <R responses: return 503
//! ```
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
use s4_core::{ReadOptions, StorageEngine};
use tokio::io::{AsyncRead, ReadBuf};
use tokio_stream::StreamExt;
use tokio_util::io::StreamReader;
use tracing::{debug, error, warn};

use crate::cluster_metrics::{observe_quorum, QuorumOperation};
use crate::config::ClusterConfig;
use crate::conflict;
use crate::error::ClusterError;
use crate::hints::{KeyStateSource, NewestVersion, StoredCopy};
use crate::hlc::{HlcTimestamp, HybridClock};
use crate::identity::NodeId;
use crate::placement::PlacementRouter;
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
}

/// Read a 32-byte content hash out of a wire field.
///
/// Anything other than exactly 32 bytes means the peer did not send one, which
/// is the honest answer from a node that predates the field.
fn content_hash_from_bytes(bytes: &[u8]) -> Option<[u8; 32]> {
    <[u8; 32]>::try_from(bytes).ok()
}

impl ObjectDigest {
    fn same_object_version(&self, other: &Self) -> bool {
        self.hlc == other.hlc
            && self.etag == other.etag
            && self.size == other.size
            && self.version_id == other.version_id
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
    read_quorum: u8,
    /// Replicas a write waits for: how many copies a confirmed write leaves.
    write_quorum: u8,
    stream_failures: StreamFailureTracker,
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
            read_quorum: cluster_config.default_read_quorum,
            write_quorum: cluster_config.default_write_quorum,
            stream_failures: StreamFailureTracker::default(),
        }
    }

    /// Execute a quorum read (GET object).
    ///
    /// Uses digest-based two-phase read: first HEAD from R replicas,
    /// then full GET from the replica with the latest version.
    pub async fn read(&self, bucket: &str, key: &str) -> Result<QuorumReadResult, ClusterError> {
        observe_quorum(QuorumOperation::Read, self.read_inner(bucket, key)).await
    }

    async fn read_inner(&self, bucket: &str, key: &str) -> Result<QuorumReadResult, ClusterError> {
        let decision = self.router.route(bucket, key)?;
        let replicas = &decision.replicas;

        // Single-node bypass: read directly, no quorum needed
        if replicas.len() == 1 && replicas[0] == self.local_node_id {
            return self.read_single_node(bucket, key).await;
        }

        // Phase 1: Collect digests from all replicas
        let digests = self.collect_digests(bucket, key, replicas).await?;

        if (digests.len() as u8) < self.read_quorum {
            return Err(ClusterError::QuorumNotMet {
                needed: self.read_quorum,
                achieved: digests.len() as u8,
            });
        }

        // Find the latest version by HLC (LWW conflict resolution)
        let hlc_list: Vec<HlcTimestamp> = digests.iter().map(|d| d.hlc).collect();
        let winner_idx =
            conflict::select_winner(&hlc_list).expect("digests is non-empty after quorum check");
        let latest = &digests[winner_idx];

        // Detect stale replicas for read repair
        let stale_nodes: Vec<&ObjectDigest> = digests
            .iter()
            .filter(|d| d.etag != latest.etag || d.hlc != latest.hlc)
            .collect();

        let repair_triggered = !stale_nodes.is_empty();

        // Phase 2: Fetch full data from the node with the latest version.
        // If the winner has corrupted/unavailable data, fall back to other
        // replicas that reported the same HLC (e.g. bit-rot on one node).
        let (data, full_metadata) = {
            let candidates = candidate_order(&digests, winner_idx, self.local_node_id);
            let byteless = digests.len() - candidates.len();

            let mut last_err = None;
            let mut result = None;
            for idx in candidates {
                match self.fetch_full_object(bucket, key, digests[idx].node_id).await {
                    Ok(r) => {
                        result = Some(r);
                        break;
                    }
                    Err(e) => {
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
                    return Err(ClusterError::Storage(format!(
                        "{bucket}/{key} is erasure coded on all {byteless} replica(s) that \
                         answered; its bytes must be read through the EC runtime"
                    )))
                }
                None => {
                    return Err(last_err.unwrap_or_else(|| ClusterError::ObjectNotFound {
                        bucket: bucket.to_string(),
                        key: key.to_string(),
                    }))
                }
            }
        };

        // Phase 3: Trigger async read repair for stale replicas
        if repair_triggered {
            for stale in &stale_nodes {
                debug!(
                    bucket = %bucket,
                    key = %key,
                    stale_node = %stale.node_id,
                    stale_etag = %stale.etag,
                    latest_etag = %latest.etag,
                    "triggering read repair"
                );

                let hlc_proto = latest.hlc.to_proto();
                self.read_repair.submit(crate::repair::read_repair::RepairTask {
                    target_node: stale.node_id,
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                    data: data.clone(),
                    content_hash: full_metadata.content_hash.to_vec(),
                    content_type: full_metadata.content_type.clone(),
                    metadata: full_metadata.metadata.clone(),
                    hlc: hlc_proto,
                    version_id: full_metadata.version_id.clone(),
                    topology_epoch: self.topology_epoch,
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

    async fn read_stream_inner(
        &self,
        bucket: &str,
        key: &str,
        range: Option<(u64, u64)>,
    ) -> Result<QuorumReadStreamResult, ClusterError> {
        let decision = self.router.route(bucket, key)?;
        let replicas = &decision.replicas;

        if replicas.len() == 1 && replicas[0] == self.local_node_id {
            return self.open_stream_from_replica(bucket, key, self.local_node_id, range).await;
        }

        let digests = self.collect_digests(bucket, key, replicas).await?;

        if (digests.len() as u8) < self.read_quorum {
            return Err(ClusterError::QuorumNotMet {
                needed: self.read_quorum,
                achieved: digests.len() as u8,
            });
        }

        let hlc_list: Vec<HlcTimestamp> = digests.iter().map(|d| d.hlc).collect();
        let winner_idx =
            conflict::select_winner(&hlc_list).expect("digests is non-empty after quorum check");

        let candidates = candidate_order(&digests, winner_idx, self.local_node_id);

        let skipped_erasure_coded = digests.len() - candidates.len();
        let mut last_err = None;
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

            match self.open_stream_from_replica(bucket, key, digests[idx].node_id, range).await {
                Ok(mut stream) => {
                    stream.etag = digests[idx].etag.clone();
                    stream.content_type = digests[idx].content_type.clone();
                    stream.metadata = digests[idx].metadata.clone();
                    stream.hlc = digests[idx].hlc;
                    stream.version_id = digests[idx].version_id.clone();
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
                    self.clock.observe(&digests[idx].hlc);
                    return Ok(stream);
                }
                Err(e) => {
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
            return Err(ClusterError::Storage(format!(
                "{bucket}/{key} is erasure coded on all {skipped_erasure_coded} replica(s) that \
                 answered; its bytes must be read through the EC runtime"
            )));
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

    async fn head_inner(&self, bucket: &str, key: &str) -> Result<QuorumHeadResult, ClusterError> {
        let decision = self.router.route(bucket, key)?;
        let replicas = &decision.replicas;

        // Single-node bypass
        if replicas.len() == 1 && replicas[0] == self.local_node_id {
            return self.head_single_node(bucket, key).await;
        }

        let digests = self.collect_digests(bucket, key, replicas).await?;

        if (digests.len() as u8) < self.read_quorum {
            return Err(ClusterError::QuorumNotMet {
                needed: self.read_quorum,
                achieved: digests.len() as u8,
            });
        }

        let hlc_list: Vec<HlcTimestamp> = digests.iter().map(|d| d.hlc).collect();
        let winner_idx =
            conflict::select_winner(&hlc_list).expect("digests is non-empty after quorum check");
        let latest = &digests[winner_idx];

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
            replicas_responded: digests.len() as u8,
        })
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

    /// Phase 1: Collect digests (HEAD) from all replicas, wait for R responses.
    async fn collect_digests(
        &self,
        bucket: &str,
        key: &str,
        replicas: &[NodeId],
    ) -> Result<Vec<ObjectDigest>, ClusterError> {
        debug!(
            coordinator = %self.local_node_id,
            epoch = self.topology_epoch,
            bucket,
            key,
            ?replicas,
            needed = self.read_quorum,
            "digest collection started"
        );
        // Own the actual replica futures. Wrapping already-spawned JoinHandles
        // would detach those tasks when this quorum read is cancelled.
        let mut set = tokio::task::JoinSet::new();
        for &node_id in replicas {
            set.spawn(self.digest_task(node_id, bucket, key));
        }

        // Wait for R responses with timeout
        let mut digests = Vec::new();
        let mut not_found_count = 0u8;
        let deadline = tokio::time::Instant::now() + self.timeouts.quorum_timeout;

        loop {
            tokio::select! {
                maybe_result = set.join_next() => {
                    match maybe_result {
                        Some(Ok(DigestResult::Success(digest))) => {
                            debug!(
                                bucket, key, node_id = %digest.node_id,
                                hlc = ?digest.hlc, content_hash = ?digest.content_hash,
                                "digest replica returned present"
                            );
                            digests.push(digest);
                            if digests.len() as u8 >= self.read_quorum {
                                // Quorum met — drain remaining responses briefly
                                // to improve LWW conflict resolution accuracy
                                // when concurrent writes are in flight.
                                let drain_until = digest_drain_deadline(deadline);
                                while let Ok(Some(result)) = tokio::time::timeout_at(
                                    drain_until,
                                    set.join_next(),
                                )
                                .await
                                {
                                    if let Ok(DigestResult::Success(extra)) = result {
                                        debug!(
                                            bucket, key, node_id = %extra.node_id,
                                            hlc = ?extra.hlc, content_hash = ?extra.content_hash,
                                            "digest replica returned present during drain"
                                        );
                                        digests.push(extra);
                                    }
                                }
                                break;
                            }
                        }
                        Some(Ok(DigestResult::NotFound(node_id, reason))) => {
                            not_found_count += 1;
                            debug!(
                                bucket, key, %node_id, %reason,
                                "object not found on replica during digest collection"
                            );
                        }
                        Some(Ok(DigestResult::Timeout(node_id))) => {
                            warn!(bucket, key, %node_id, "digest request timed out");
                        }
                        Some(Ok(DigestResult::Error(node_id, err))) => {
                            warn!(bucket, key, %node_id, error = %err, "digest request failed");
                        }
                        Some(Ok(DigestResult::EpochMismatch(node_id))) => {
                            warn!(bucket, key, %node_id, "epoch mismatch during digest collection");
                        }
                        Some(Err(e)) => {
                            error!(bucket, key, error = %e, "join error in digest collection");
                        }
                        None => break,
                    }
                }
                _ = tokio::time::sleep_until(deadline) => {
                    warn!(
                        bucket, key,
                        got = digests.len(),
                        needed = self.read_quorum,
                        "digest collection timed out"
                    );
                    break;
                }
            }
        }

        debug!(
            bucket,
            key,
            present = digests.len(),
            not_found = not_found_count,
            needed = self.read_quorum,
            "digest collection completed"
        );
        // If all replicas said "not found" and we have quorum of not-found,
        // return ObjectNotFound instead of QuorumNotMet.
        if digests.is_empty() && not_found_count >= self.read_quorum {
            return Err(ClusterError::ObjectNotFound {
                bucket: bucket.to_string(),
                key: key.to_string(),
            });
        }

        Ok(digests)
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
                    tokio::time::timeout(timeout_dur, storage.head_object(&bucket, &key)).await;

                match result {
                    Ok(Ok(record)) => DigestResult::Success(ObjectDigest {
                        node_id: local_id,
                        erasure_coded: record.is_erasure_coded(),
                        hlc: HlcTimestamp::of_record(&record),
                        content_hash: Some(record.content_hash),
                        etag: record.etag,
                        size: record.size,
                        content_type: record.content_type,
                        version_id: record.version_id,
                        metadata: record.metadata,
                    }),
                    Ok(Err(e @ s4_core::StorageError::ObjectNotFound { .. })) => {
                        DigestResult::NotFound(local_id, e.to_string())
                    }
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
                    Ok(Ok(resp)) if resp.exists => {
                        let hlc = resp
                            .hlc
                            .map(|h| HlcTimestamp::from_proto(&h))
                            .unwrap_or_else(HlcTimestamp::zero);

                        DigestResult::Success(ObjectDigest {
                            node_id,
                            hlc,
                            etag: resp.etag,
                            size: resp.content_length,
                            content_type: resp.content_type,
                            version_id: resp.version_id,
                            erasure_coded: resp.erasure_coded,
                            metadata: resp.metadata,
                            content_hash: content_hash_from_bytes(&resp.content_hash),
                        })
                    }
                    Ok(Ok(resp)) if resp.epoch_mismatch => DigestResult::EpochMismatch(node_id),
                    Ok(Ok(_)) => DigestResult::NotFound(node_id, "object not found".into()),
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
        let mut set = tokio::task::JoinSet::new();
        for &node_id in &others {
            set.spawn(self.digest_task(node_id, bucket, key));
        }

        let deadline = tokio::time::Instant::now() + self.timeouts.quorum_timeout;
        let mut digests = Vec::new();
        let mut absent = 0;
        loop {
            let answer = tokio::time::timeout_at(deadline, set.join_next())
                .await
                .map_err(|_| ClusterError::ReadTimeout(self.timeouts.quorum_timeout))?;
            match answer {
                None => break,
                Some(Ok(DigestResult::Success(digest))) => digests.push(digest),
                Some(Ok(DigestResult::NotFound(..))) => absent += 1,
                Some(Ok(DigestResult::Timeout(node_id))) => {
                    return Err(ClusterError::Unavailable(format!(
                        "replica {node_id} did not answer in time"
                    )))
                }
                Some(Ok(DigestResult::Error(node_id, error))) => {
                    return Err(ClusterError::Unavailable(format!(
                        "replica {node_id}: {error}"
                    )))
                }
                Some(Ok(DigestResult::EpochMismatch(node_id))) => {
                    return Err(ClusterError::Unavailable(format!(
                        "replica {node_id} is on another topology epoch"
                    )))
                }
                Some(Err(e)) => {
                    return Err(ClusterError::Unavailable(format!(
                        "digest task failed: {e}"
                    )))
                }
            }
        }
        // A confirmed write not on the target is on this many of the others.
        let confirming = usize::from(self.write_quorum).min(others.len());
        Ok(newest_of(&digests, absent, confirming))
    }

    /// Phase 2: Fetch full object data from a specific replica.
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

        let resp = tokio::time::timeout(
            self.timeouts.replica_read_timeout,
            self.rpc_client.send_read(node_id, request),
        )
        .await
        .map_err(|_| ClusterError::ReadTimeout(self.timeouts.replica_read_timeout))?
        .map_err(|e| ClusterError::Rpc(format!("full read from {node_id}: {e}")))?;

        if !resp.success {
            return Err(ClusterError::Storage(format!(
                "remote read from {node_id} failed: {}",
                resp.error
            )));
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
            etag: String::new(),
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
            etag: String::new(),
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
    let mut candidates: Vec<usize> =
        (0..digests.len()).filter(|&idx| !digests[idx].erasure_coded).collect();

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
    } else if digest.same_object_version(latest) {
        2
    } else {
        3
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

/// The newest of `digests` by HLC, given that `absent` other replicas hold no
/// key and that a confirmed write leaves `confirming` copies on them.
///
/// At a tie, one replica holding only the erasure-coded record is enough for
/// the version to count as erasure coded: the object is being moved out of
/// replication, and a copy is not to be spread again.
///
/// A copy is confirmed when no replica is absent, or when at least
/// `confirming` replicas hold exactly it. Without tombstones an absent replica
/// may be one that took a delete the copy missed, and only a copy on as many
/// replicas as a confirmed write leaves is known to be newer than any delete
/// that was confirmed.
fn newest_of(digests: &[ObjectDigest], absent: usize, confirming: usize) -> NewestVersion {
    let Some(newest) = digests.iter().map(|digest| digest.hlc).max() else {
        return NewestVersion::Absent;
    };
    let at_newest = || digests.iter().filter(move |digest| digest.hlc == newest);
    if at_newest().any(|digest| digest.erasure_coded) {
        return NewestVersion::ErasureCoded { hlc: newest };
    }
    match at_newest().next() {
        Some(digest) => NewestVersion::Copy {
            hlc: newest,
            node: digest.node_id,
            confirmed: absent == 0 || at_newest().count() >= confirming,
        },
        None => NewestVersion::Absent,
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
    NotFound(NodeId, String),
    Timeout(NodeId),
    Error(NodeId, String),
    EpochMismatch(NodeId),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gossip::GossipHandle;
    use crate::placement::PlacementRouter;
    use crate::rpc::NodeClientConfig;

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

    #[tokio::test]
    async fn single_node_head_not_found() {
        let (coordinator, _tmp) = setup_single_node();
        let err = coordinator.head("test-bucket", "missing.txt").await.unwrap_err();
        assert!(matches!(err, ClusterError::ObjectNotFound { .. }));
    }

    #[tokio::test]
    async fn local_digest_storage_error_is_not_absence() {
        let (mut coordinator, _tmp) = setup_single_node();
        coordinator.read_quorum = 1;
        let replicas = [coordinator.local_node_id];
        let missing = coordinator.collect_digests("bucket", "missing.txt", &replicas).await;
        assert!(matches!(missing, Err(ClusterError::ObjectNotFound { .. })));

        let failed = coordinator.collect_digests("bucket", "unreadable.txt", &replicas).await;
        assert!(
            matches!(&failed, Ok(digests) if digests.is_empty()),
            "a failed storage read must not count toward an absence quorum: {failed:?}"
        );
    }

    async fn assert_pending_digest_is_cancelled(cancel_caller: bool) {
        let (mut coordinator, _tmp) = setup_single_node();
        let started = Arc::new(tokio::sync::Notify::new());
        let dropped = Arc::new(tokio::sync::Notify::new());
        coordinator.storage = Arc::new(MockStorage {
            pending_head: Some((started.clone(), dropped.clone())),
        });
        coordinator.read_quorum = 1;
        coordinator.timeouts.quorum_timeout = Duration::from_millis(200);
        coordinator.timeouts.replica_read_timeout = Duration::from_secs(10);
        let read = tokio::spawn(async move {
            coordinator.collect_digests("bucket", "pending.txt", &[NodeId(1)]).await
        });
        started.notified().await;
        if cancel_caller {
            read.abort();
            assert!(read.await.unwrap_err().is_cancelled());
        } else {
            assert!(read.await.unwrap().unwrap().is_empty());
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
            },
        ];

        let order = candidate_order(&digests, 0, NodeId(2));

        assert_eq!(order, vec![1, 0, 2]);
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
    }

    struct NotifyOnDrop(Arc<tokio::sync::Notify>);

    impl Drop for NotifyOnDrop {
        fn drop(&mut self) {
            self.0.notify_one();
        }
    }

    impl MockStorage {
        fn with_data() -> Self {
            MockStorage { pending_head: None }
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
                metrics.value(ops, &[("operation", operation), ("outcome", "ok")]),
                Some(1.0)
            );
            assert_eq!(
                metrics.value(ops, &[("operation", operation), ("outcome", "not_found")]),
                Some(1.0)
            );
            assert_eq!(
                metrics.value(
                    "s4_quorum_duration_seconds_count",
                    &[("operation", operation)]
                ),
                Some(2.0)
            );
        }
        assert_eq!(metrics.sum(ops, &[("outcome", "error")]), 0.0);
    }

    // ---- The newest version, as hint delivery reads it ----

    mod newest_version {
        use super::*;
        use crate::config::PoolConfig;
        use crate::gossip::NodeState;
        use crate::identity::{Edition, NodeAddr, NodeCapacity, NodeMeta, NodeStatus, PoolId};
        use crate::placement::bucket_placement::BucketPlacement;
        use crate::replica::ReplicaReadHandler;
        use crate::rpc::server::{NodeRpcServer, RpcServerConfig, RpcServerHandle};
        use crate::test_metrics::TestMetrics;
        use s4_core::storage::BitcaskStorageEngine;
        use s4_core::types::StorageClass;

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
            _servers: Vec<RpcServerHandle>,
            _dirs: Vec<tempfile::TempDir>,
        }

        async fn storage() -> (Arc<BitcaskStorageEngine>, tempfile::TempDir) {
            let dir = tempfile::tempdir().unwrap();
            let storage = BitcaskStorageEngine::new(
                dir.path().join("volumes"),
                dir.path().join("metadata"),
                10 * 1024 * 1024,
                4096,
                false,
            )
            .await
            .unwrap();
            (Arc::new(storage), dir)
        }

        async fn pool(node_2_answers: bool, base_port: u16) -> Pool {
            build_pool(node_2_answers, false, base_port).await
        }

        /// The pool with node 2 and the target both up.
        async fn pool_with_target(base_port: u16) -> Pool {
            build_pool(true, true, base_port).await
        }

        async fn build_pool(node_2_answers: bool, target_runs: bool, base_port: u16) -> Pool {
            let addr = |n: u128| NodeAddr {
                grpc_addr: format!("127.0.0.1:{}", base_port + n as u16).parse().unwrap(),
                http_addr: format!("127.0.0.1:{}", 9000 + n).parse().unwrap(),
                gossip_addr: format!("127.0.0.1:{}", 9200 + n).parse().unwrap(),
            };
            let pool_id = PoolId::new(1);
            let config = ClusterConfig {
                pools: vec![PoolConfig {
                    pool_id,
                    name: "pool-1".to_string(),
                    nodes: (1..=3).map(addr).collect(),
                    replication_factor: Some(3),
                    write_quorum: Some(2),
                    read_quorum: Some(2),
                    storage_class: StorageClass::Standard,
                }],
                default_replication_factor: 3,
                default_write_quorum: 2,
                default_read_quorum: 2,
                ..ClusterConfig::default()
            };
            let meta = |n: u128| NodeMeta {
                node_id: NodeId(n),
                node_name: format!("node-{n}"),
                addr: addr(n),
                pool_id,
                topology_epoch: config.epoch,
                protocol_version: crate::protocol::PROTOCOL_VERSION,
                capacity: NodeCapacity::default(),
                started_at: 0,
                s4_version: "0.1.0".into(),
                edition: Edition::Community,
            };
            let state = |n: u128, alive: bool| NodeState {
                meta: meta(n),
                status: if alive {
                    NodeStatus::Alive
                } else {
                    NodeStatus::Dead
                },
                last_seen: std::time::Instant::now(),
            };
            let serve = |n: u128, storage: Arc<BitcaskStorageEngine>| {
                NodeRpcServer::new(
                    NodeId(n),
                    Arc::new(meta(n)),
                    Arc::new(GossipHandle::new_for_testing(HashMap::new())),
                )
                .with_replica_handler(Arc::new(crate::replica::ReplicaWriteHandler::new(
                    storage.clone(),
                    Arc::new(crate::idempotency::IdempotencyTracker::new()),
                    config.epoch,
                )))
                .with_read_handler(Arc::new(ReplicaReadHandler::new(storage, config.epoch)))
                .start(RpcServerConfig {
                    bind_addr: addr(n).grpc_addr,
                    ..Default::default()
                })
            };

            let (local, local_dir) = storage().await;
            let (node_2, node_2_dir) = storage().await;
            let mut dirs = vec![local_dir, node_2_dir];
            let mut servers = Vec::new();
            if node_2_answers {
                servers.push(serve(2, node_2.clone()).await.unwrap());
            }
            let target = if target_runs {
                let (target, target_dir) = storage().await;
                dirs.push(target_dir);
                servers.push(serve(3, target.clone()).await.unwrap());
                Some(target)
            } else {
                None
            };

            let members = HashMap::from([
                (NodeId(2), state(2, node_2_answers)),
                (TARGET, state(3, target_runs)),
            ]);
            let gossip = Arc::new(GossipHandle::new_for_testing(members));
            let rpc_client = Arc::new(NodeClient::new(gossip.clone(), NodeClientConfig::default()));
            let resolver: HashMap<_, _> = (1..=3).map(|n| (addr(n).grpc_addr, NodeId(n))).collect();
            let placement = BucketPlacement::from_entries([("bucket".to_string(), pool_id)]);
            let router =
                Arc::new(PlacementRouter::new(&config, NodeId(1), placement, &resolver).unwrap());
            let coordinator = Arc::new(QuorumReadCoordinator::new(
                NodeId(1),
                local.clone(),
                rpc_client.clone(),
                router,
                Arc::new(HybridClock::new(NodeId(1))),
                Arc::new(ReadRepairService::start_with_capacity(
                    rpc_client.clone(),
                    16,
                )),
                &config,
                ReadTimeouts::default(),
            ));
            Pool {
                coordinator,
                local,
                node_2,
                target,
                rpc_client,
                gossip,
                _servers: servers,
                _dirs: dirs,
            }
        }

        fn at(wall: u64) -> HlcTimestamp {
            HlcTimestamp::from_record(wall, 0, 7)
        }

        async fn put(storage: &BitcaskStorageEngine, data: &[u8], hlc: HlcTimestamp) {
            let metadata = HashMap::from([
                ("_s4_hlc_wall".to_string(), hlc.wall_time.to_string()),
                ("_s4_hlc_logical".to_string(), hlc.logical.to_string()),
                ("_s4_hlc_node_id".to_string(), hlc.node_id.to_string()),
                ("owner".to_string(), "test".to_string()),
            ]);
            storage
                .put_object("bucket", "key", data, "text/plain", &metadata)
                .await
                .unwrap();
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

        /// A copy one replica holds while the other has no key may be one
        /// that missed a delete: with W=2 a confirmed write not on the target
        /// is on both other replicas.
        #[tokio::test]
        async fn a_copy_beside_a_replica_without_the_key_is_unconfirmed() {
            let pool = pool(true, 19826).await;
            put(&pool.local, b"version 20", at(20)).await;

            let newest = pool.coordinator.newest_version("bucket", "key", TARGET).await.unwrap();
            assert_eq!(
                newest,
                NewestVersion::Copy {
                    hlc: at(20),
                    node: NodeId(1),
                    confirmed: false,
                }
            );

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
        /// target takes and node 1 misses. Node 1's copy is the newest left,
        /// and spreading it would bring the deleted key back on the target:
        /// it is not spread, and the hint is dropped as ambiguous.
        #[tokio::test]
        async fn a_copy_that_missed_a_delete_is_not_brought_back_by_a_hint() {
            let metrics = TestMetrics::install();
            let tmp = tempfile::tempdir().unwrap();
            let pool = pool_with_target(19840).await;
            let target = pool.target.clone().unwrap();
            put(&pool.local, b"version 20", at(20)).await;
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
            }
        }

        /// One replica holding the erasure-coded record of the newest version
        /// is enough: the object is leaving replication, and no copy of it
        /// is to be spread again.
        #[test]
        fn an_erasure_coded_newest_version_is_reported_as_such() {
            assert_eq!(
                newest_of(&[digest(1, 10, false), digest(2, 20, true)], 0, 2),
                NewestVersion::ErasureCoded { hlc: at(20) }
            );
            assert_eq!(
                newest_of(&[digest(1, 20, false), digest(2, 20, true)], 0, 2),
                NewestVersion::ErasureCoded { hlc: at(20) }
            );
            assert_eq!(
                newest_of(&[digest(1, 30, false), digest(2, 20, true)], 0, 2),
                NewestVersion::Copy {
                    hlc: at(30),
                    node: NodeId(1),
                    confirmed: true,
                }
            );
            assert_eq!(newest_of(&[], 2, 2), NewestVersion::Absent);
        }

        /// A copy is confirmed when no replica lacks the key, or when as many
        /// replicas hold it as a confirmed write leaves behind.
        #[test]
        fn a_copy_is_confirmed_by_the_write_quorum_or_by_no_replica_lacking_the_key() {
            let confirmed = |digests: &[ObjectDigest], absent, confirming| match newest_of(
                digests, absent, confirming,
            ) {
                NewestVersion::Copy { confirmed, .. } => confirmed,
                other => panic!("not a copy: {other:?}"),
            };
            // RF=3, W=2: two other replicas, a confirmed write is on both.
            assert!(confirmed(
                &[digest(1, 20, false), digest(2, 20, false)],
                0,
                2
            ));
            assert!(!confirmed(&[digest(1, 20, false)], 1, 2));
            assert!(confirmed(
                &[digest(1, 30, false), digest(2, 20, false)],
                0,
                2
            ));
            // RF=5, W=3: four other replicas, a confirmed write is on three.
            let three = [
                digest(1, 20, false),
                digest(2, 20, false),
                digest(3, 20, false),
            ];
            assert!(confirmed(&three, 1, 3));
            assert!(!confirmed(&three[..2], 2, 3));
            assert!(!confirmed(
                &[three[0].clone(), three[1].clone(), digest(3, 10, false)],
                1,
                3
            ));
        }
    }
}
