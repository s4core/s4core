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

use crate::config::ClusterConfig;
use crate::conflict;
use crate::error::ClusterError;
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
            stream_failures: StreamFailureTracker::default(),
        }
    }

    /// Execute a quorum read (GET object).
    ///
    /// Uses digest-based two-phase read: first HEAD from R replicas,
    /// then full GET from the replica with the latest version.
    pub async fn read(&self, bucket: &str, key: &str) -> Result<QuorumReadResult, ClusterError> {
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
            result.ok_or_else(|| last_err.unwrap())?
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

        Err(last_err.unwrap_or_else(|| ClusterError::ObjectNotFound {
            bucket: bucket.to_string(),
            key: key.to_string(),
        }))
    }

    /// Execute a quorum HEAD (metadata only, no data transfer).
    pub async fn head(&self, bucket: &str, key: &str) -> Result<QuorumHeadResult, ClusterError> {
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

        let hlc = if record.hlc_timestamp > 0 {
            HlcTimestamp {
                wall_time: record.hlc_timestamp,
                logical: record.hlc_logical,
                node_id: record.origin_node_id,
            }
        } else {
            HlcTimestamp {
                wall_time: record.modified_at / 1_000_000, // nanos → millis (HLC uses millis)
                logical: 0,
                node_id: self.local_node_id.0,
            }
        };

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

        let hlc = if record.hlc_timestamp > 0 {
            HlcTimestamp {
                wall_time: record.hlc_timestamp,
                logical: record.hlc_logical,
                node_id: record.origin_node_id,
            }
        } else {
            HlcTimestamp {
                wall_time: record.modified_at / 1_000_000, // nanos → millis (HLC uses millis)
                logical: 0,
                node_id: self.local_node_id.0,
            }
        };

        Ok(QuorumHeadResult {
            etag: record.etag,
            content_type: record.content_type,
            size: record.size,
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
        let mut handles = Vec::with_capacity(replicas.len());

        for &node_id in replicas {
            let handle = if node_id == self.local_node_id {
                let storage = self.storage.clone();
                let bucket = bucket.to_string();
                let key = key.to_string();
                let local_id = self.local_node_id;
                let timeout_dur = self.timeouts.replica_read_timeout;

                tokio::spawn(async move {
                    let result =
                        tokio::time::timeout(timeout_dur, storage.head_object(&bucket, &key)).await;

                    match result {
                        Ok(Ok(record)) => DigestResult::Success(ObjectDigest {
                            node_id: local_id,
                            hlc: if record.hlc_timestamp > 0 {
                                HlcTimestamp {
                                    wall_time: record.hlc_timestamp,
                                    logical: record.hlc_logical,
                                    node_id: record.origin_node_id,
                                }
                            } else {
                                HlcTimestamp {
                                    wall_time: record.modified_at / 1_000_000, // nanos → millis
                                    logical: 0,
                                    node_id: local_id.0,
                                }
                            },
                            etag: record.etag,
                            size: record.size,
                            content_type: record.content_type,
                            version_id: record.version_id,
                            metadata: record.metadata,
                        }),
                        Ok(Err(e)) => DigestResult::NotFound(local_id, e.to_string()),
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

                tokio::spawn(async move {
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
                                metadata: resp.metadata,
                            })
                        }
                        Ok(Ok(resp)) if resp.epoch_mismatch => DigestResult::EpochMismatch(node_id),
                        Ok(Ok(_)) => DigestResult::NotFound(node_id, "object not found".into()),
                        Ok(Err(e)) => DigestResult::Error(node_id, e.to_string()),
                        Err(_) => DigestResult::Timeout(node_id),
                    }
                })
            };

            handles.push(handle);
        }

        // Wait for R responses with timeout
        let mut digests = Vec::new();
        let mut not_found_count = 0u8;
        let mut set = tokio::task::JoinSet::new();
        for handle in handles {
            set.spawn(handle);
        }

        let deadline = tokio::time::Instant::now() + self.timeouts.quorum_timeout;

        loop {
            tokio::select! {
                maybe_result = set.join_next() => {
                    match maybe_result {
                        Some(Ok(Ok(DigestResult::Success(digest)))) => {
                            digests.push(digest);
                            if digests.len() as u8 >= self.read_quorum {
                                // Quorum met — drain remaining responses briefly
                                // to improve LWW conflict resolution accuracy
                                // when concurrent writes are in flight.
                                let drain_until = tokio::time::Instant::now()
                                    + std::time::Duration::from_millis(50);
                                while let Ok(Some(result)) = tokio::time::timeout_at(
                                    drain_until,
                                    set.join_next(),
                                )
                                .await
                                {
                                    if let Ok(Ok(DigestResult::Success(extra))) = result {
                                        digests.push(extra);
                                    }
                                }
                                break;
                            }
                        }
                        Some(Ok(Ok(DigestResult::NotFound(node_id, _)))) => {
                            not_found_count += 1;
                            debug!(
                                %node_id,
                                "object not found on replica during digest collection"
                            );
                        }
                        Some(Ok(Ok(DigestResult::Timeout(node_id)))) => {
                            warn!(%node_id, "digest request timed out");
                        }
                        Some(Ok(Ok(DigestResult::Error(node_id, err)))) => {
                            warn!(%node_id, error = %err, "digest request failed");
                        }
                        Some(Ok(Ok(DigestResult::EpochMismatch(node_id)))) => {
                            warn!(%node_id, "epoch mismatch during digest collection");
                        }
                        Some(Ok(Err(e))) => {
                            error!(error = %e, "digest task panicked");
                        }
                        Some(Err(e)) => {
                            error!(error = %e, "join error in digest collection");
                        }
                        None => break,
                    }
                }
                _ = tokio::time::sleep_until(deadline) => {
                    warn!(
                        got = digests.len(),
                        needed = self.read_quorum,
                        "digest collection timed out"
                    );
                    break;
                }
            }
        }

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
                body: object_stream.body,
                etag: object_stream.record.etag,
                content_type: object_stream.record.content_type,
                metadata: object_stream.record.metadata,
                hlc: if object_stream.record.hlc_timestamp > 0 {
                    HlcTimestamp {
                        wall_time: object_stream.record.hlc_timestamp,
                        logical: object_stream.record.hlc_logical,
                        node_id: object_stream.record.origin_node_id,
                    }
                } else {
                    HlcTimestamp {
                        wall_time: object_stream.record.modified_at / 1_000_000,
                        logical: 0,
                        node_id: self.local_node_id.0,
                    }
                },
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
    let mut candidates: Vec<usize> = (0..digests.len()).collect();

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

    #[tokio::test]
    async fn single_node_head_not_found() {
        let (coordinator, _tmp) = setup_single_node();
        let err = coordinator.head("test-bucket", "missing.txt").await.unwrap_err();
        assert!(matches!(err, ClusterError::ObjectNotFound { .. }));
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
        };

        let digests = [d1, d2];
        let hlc_list: Vec<HlcTimestamp> = digests.iter().map(|d| d.hlc).collect();
        let winner_idx = crate::conflict::select_winner(&hlc_list).unwrap();
        assert_eq!(digests[winner_idx].etag, "\"b\"");
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
            },
            ObjectDigest {
                node_id: NodeId(2),
                hlc: latest_hlc,
                etag: "\"latest\"".into(),
                size: 64,
                content_type: "application/octet-stream".into(),
                version_id: None,
                metadata: HashMap::new(),
            },
            ObjectDigest {
                node_id: NodeId(3),
                hlc: stale_hlc,
                etag: "\"stale\"".into(),
                size: 64,
                content_type: "application/octet-stream".into(),
                version_id: None,
                metadata: HashMap::new(),
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

    struct MockStorage;

    impl MockStorage {
        fn with_data() -> Self {
            MockStorage
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
}
