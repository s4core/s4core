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

//! gRPC client pool for inter-node communication.
//!
//! [`NodeClient`] maintains persistent gRPC connections to peer nodes,
//! discovers addresses via the gossip layer, and provides retry logic
//! with exponential backoff.
//!
//! # Retry Policy
//!
//! - Max retries: 3
//! - Backoff: 100ms -> 200ms -> 400ms (exponential)
//! - Dead nodes (per gossip) are not retried — fail immediately
//! - Non-retryable errors (InvalidArgument, Unauthenticated) are not retried

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use tokio_stream::Stream;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Response, Status};
use tracing::{debug, trace, warn};

use crate::cluster_metrics::{PeerCall, PeerNames, PeerOutcome};
use crate::ec_object::{EcContentDigest, EcContentDigestPage, EcTombstoneProbe};
use crate::error::ClusterError;
use crate::gossip::GossipHandle;
use crate::identity::{NodeId, NodeStatus};
use crate::rpc::proto::s4_node_client::S4NodeClient;
use crate::rpc::proto::{self};
use crate::rpc::tls::TlsConfig;
use crate::rpc::{DEFAULT_CHUNK_SIZE, DEFAULT_MAX_MESSAGE_SIZE, STREAMING_THRESHOLD};

/// Why a retried call gave up.
enum RetryFailure {
    /// The call never reached the peer: it is known dead, or unreachable.
    Cluster(ClusterError),
    /// The peer answered with a status, after `attempts` tries.
    Rpc { attempts: u32, status: Box<Status> },
}

/// How much a streamed read reserves up front for a body it has not seen.
///
/// The peer's announced length is not trusted as an allocation size, the same
/// way a streamed write does not trust the header it is sent. Beyond this the
/// buffer grows with the bytes that actually arrive.
const MAX_STREAMED_READ_RESERVE: usize = DEFAULT_MAX_MESSAGE_SIZE;

impl RetryFailure {
    /// The error a caller that does not inspect the status reports.
    fn into_cluster_error(self, node_id: NodeId) -> ClusterError {
        match self {
            Self::Cluster(error) => error,
            Self::Rpc { attempts, status } => ClusterError::Rpc(format!(
                "RPC to {node_id} failed after {attempts} attempts: {status}"
            )),
        }
    }
}

/// Split a blob write into the header and body chunks the stream carries.
///
/// The header goes first and repeats every field of the unary request, so the
/// replica can rebuild it exactly. An empty body still produces a header and
/// no data chunk, which the replica accepts as a zero-length object.
fn write_blob_chunks(
    request: &proto::WriteBlobRequest,
    chunk_size: usize,
) -> Vec<proto::WriteBlobChunk> {
    use proto::write_blob_chunk::Payload;

    let mut chunks = Vec::with_capacity(1 + request.data.len().div_ceil(chunk_size));
    chunks.push(proto::WriteBlobChunk {
        payload: Some(Payload::Header(proto::WriteBlobHeader {
            bucket: request.bucket.clone(),
            key: request.key.clone(),
            content_hash: request.content_hash.clone(),
            content_type: request.content_type.clone(),
            metadata: request.metadata.clone(),
            hlc: request.hlc.clone(),
            version_id: request.version_id.clone(),
            coordinator_id: request.coordinator_id.clone(),
            operation_id: request.operation_id.clone(),
            topology_epoch: request.topology_epoch,
            total_size: request.data.len() as u64,
        })),
    });
    for piece in request.data.chunks(chunk_size) {
        chunks.push(proto::WriteBlobChunk {
            payload: Some(Payload::Data(piece.to_vec())),
        });
    }
    chunks
}

/// Configuration for the gRPC client pool.
#[derive(Debug, Clone)]
pub struct NodeClientConfig {
    /// Connection timeout for establishing new gRPC channels.
    pub connect_timeout: Duration,
    /// Per-request timeout.
    pub request_timeout: Duration,
    /// Maximum number of retry attempts for failed requests.
    pub max_retries: u32,
    /// Initial retry backoff duration (doubles each attempt).
    pub initial_backoff: Duration,
    /// Maximum inbound/outbound message size.
    pub max_message_size: usize,
    /// Body size of one chunk of a streamed blob write.
    ///
    /// Only the transfer is split; the replica still commits the object in one
    /// durable write. Smaller chunks cost more round trips, larger ones more
    /// memory per in-flight message, and every one of them must stay well
    /// under [`max_message_size`](Self::max_message_size).
    pub write_chunk_size: usize,
    /// Optional TLS configuration for connecting to peers.
    pub tls: Option<TlsConfig>,
}

impl Default for NodeClientConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(10 * 60),
            max_retries: 3,
            initial_backoff: Duration::from_millis(100),
            max_message_size: DEFAULT_MAX_MESSAGE_SIZE,
            write_chunk_size: DEFAULT_CHUNK_SIZE,
            tls: None,
        }
    }
}

/// gRPC client pool for communicating with peer nodes.
///
/// Maintains persistent connections keyed by [`NodeId`]. Connections are
/// lazily created on first use and cached for subsequent requests.
/// Node addresses are resolved via the [`GossipHandle`].
pub struct NodeClient {
    connections: DashMap<NodeId, CachedConnection>,
    gossip: Arc<GossipHandle>,
    config: NodeClientConfig,
    peer_names: Arc<PeerNames>,
}

/// A pooled connection, together with the address it was opened to.
///
/// The address is what makes the entry safe to reuse. A node keeps its id
/// across a restart but not necessarily its address — a container that comes
/// back on a different IP is the ordinary case — and the address it leaves
/// behind is often taken over by another node of the same cluster. A pool keyed
/// by node id alone would go on sending that node's requests to whoever holds
/// its old address.
#[derive(Clone)]
struct CachedConnection {
    addr: SocketAddr,
    client: S4NodeClient<Channel>,
}

impl NodeClient {
    /// Create a new client pool.
    pub fn new(gossip: Arc<GossipHandle>, config: NodeClientConfig) -> Self {
        Self {
            connections: DashMap::new(),
            gossip,
            config,
            peer_names: Arc::new(PeerNames::default()),
        }
    }

    /// Label calls in metrics with these peer names.
    ///
    /// The names are shared rather than copied: bootstrap creates the client
    /// before it has matched the configured pool to identities, and fills the
    /// names in afterwards.
    pub fn with_peer_names(mut self, peer_names: Arc<PeerNames>) -> Self {
        self.peer_names = peer_names;
        self
    }

    /// The `peer` label of `node_id` in metrics.
    pub(crate) fn peer_label(&self, node_id: NodeId) -> String {
        self.peer_names.label(node_id)
    }

    /// Opens one call to `node_id` that bypasses the retry loop.
    ///
    /// Skipped when `check_alive` is set and gossip holds the node dead; an
    /// error when no connection can be had. Either way the call is recorded
    /// here and the caller only returns the error.
    async fn open_call(
        &self,
        node_id: NodeId,
        check_alive: bool,
    ) -> Result<(PeerCall, S4NodeClient<Channel>), ClusterError> {
        let call = PeerCall::start(self.peer_label(node_id));
        if check_alive {
            if let Err(error) = self.check_node_alive(node_id) {
                call.finish(PeerOutcome::Skipped);
                return Err(error);
            }
        }
        match self.get_or_connect(node_id).await {
            Ok(client) => Ok((call, client)),
            Err(error) => {
                call.finish(PeerOutcome::Error);
                Err(error)
            }
        }
    }

    /// Send a `WriteBlob` request to a specific node with retry.
    ///
    /// A body at or above [`STREAMING_THRESHOLD`] goes over `WriteBlobStream`
    /// instead: one message has to fit under
    /// [`NodeClientConfig::max_message_size`], and an object larger than that
    /// would otherwise fail the replica write and, with it, the quorum. The
    /// replica reassembles the chunks and takes the same durable path either
    /// way, so which transport was used is not visible in the result.
    ///
    /// A peer too old to serve the streaming method answers `Unimplemented`;
    /// the write is then retried unary, which is exactly what that peer could
    /// do before. It fails for a body above its message limit, as it did
    /// before — a mixed-version cluster loses no ground here.
    pub async fn send_write(
        &self,
        node_id: NodeId,
        request: proto::WriteBlobRequest,
    ) -> Result<proto::WriteBlobResponse, ClusterError> {
        if request.data.len() >= STREAMING_THRESHOLD {
            match self.send_write_chunked(node_id, &request).await {
                Ok(response) => return Ok(response),
                Err(RetryFailure::Rpc { status, .. })
                    if status.code() == tonic::Code::Unimplemented =>
                {
                    debug!(
                        %node_id,
                        "peer does not serve streaming blob writes; falling back to a unary write"
                    );
                }
                Err(failure) => return Err(failure.into_cluster_error(node_id)),
            }
        }

        self.send_write_unary(node_id, request).await
    }

    /// Send a `WriteBlob` request in one message.
    async fn send_write_unary(
        &self,
        node_id: NodeId,
        request: proto::WriteBlobRequest,
    ) -> Result<proto::WriteBlobResponse, ClusterError> {
        self.with_retry(node_id, |mut client| {
            let req = request.clone();
            async move { client.write_blob(req).await }
        })
        .await
    }

    /// Send a `WriteBlob` request as a header followed by body chunks.
    ///
    /// The stream is built afresh for every attempt: a stream is consumed by
    /// the call that sends it, so a retry cannot reuse the previous one.
    async fn send_write_chunked(
        &self,
        node_id: NodeId,
        request: &proto::WriteBlobRequest,
    ) -> Result<proto::WriteBlobResponse, RetryFailure> {
        let chunk_size = self.config.write_chunk_size.max(1);
        self.with_retry_status(node_id, |mut client| {
            let chunks = write_blob_chunks(request, chunk_size);
            async move { client.write_blob_stream(Request::new(tokio_stream::iter(chunks))).await }
        })
        .await
    }

    /// Send a `ReadBlob` request to a specific node with retry.
    /// Send a `ReadBlob` request to a specific node with retry.
    ///
    /// Unlike a write, a read cannot know the size before it asks: the answer
    /// is what carries the object. So the unary call is made first, and an
    /// object too large for one message — the peer packs it, this side refuses
    /// to decode it with `OutOfRange` — is fetched again over `ReadBlobStream`
    /// and reassembled here. Small objects, which are nearly all of them, keep
    /// the single round trip they had.
    pub async fn send_read(
        &self,
        node_id: NodeId,
        request: proto::ReadBlobRequest,
    ) -> Result<proto::ReadBlobResponse, ClusterError> {
        match self
            .with_retry_status(node_id, |mut client| {
                let req = request.clone();
                async move { client.read_blob(req).await }
            })
            .await
        {
            Ok(response) => Ok(response),
            Err(RetryFailure::Rpc { status, .. })
                if status.code() == tonic::Code::OutOfRange
                    || status.code() == tonic::Code::Unimplemented =>
            {
                debug!(
                    %node_id,
                    status = %status,
                    "object does not fit one message; reading it as a stream"
                );
                self.send_read_chunked(node_id, request).await
            }
            Err(failure) => Err(failure.into_cluster_error(node_id)),
        }
    }

    /// Fetch a blob over `ReadBlobStream` and rebuild the unary response.
    ///
    /// The peer sends a header and then the body in chunks. What comes back is
    /// the same [`proto::ReadBlobResponse`] the unary call would have produced,
    /// so no caller has to know which transport was used.
    async fn send_read_chunked(
        &self,
        node_id: NodeId,
        request: proto::ReadBlobRequest,
    ) -> Result<proto::ReadBlobResponse, ClusterError> {
        use proto::read_blob_chunk::Payload;

        let mut stream = self.send_read_stream(node_id, request).await?;
        let rpc_error =
            |reason: String| ClusterError::Rpc(format!("streamed read from {node_id}: {reason}"));

        let mut header: Option<proto::ReadBlobHeader> = None;
        let mut data: Vec<u8> = Vec::new();
        while let Some(chunk) =
            stream.message().await.map_err(|status| rpc_error(status.to_string()))?
        {
            match chunk.payload {
                Some(Payload::Header(incoming)) => {
                    if header.is_some() {
                        return Err(rpc_error("a second header arrived".to_string()));
                    }
                    data.reserve(
                        usize::try_from(incoming.content_length)
                            .unwrap_or(usize::MAX)
                            .min(MAX_STREAMED_READ_RESERVE),
                    );
                    header = Some(incoming);
                }
                Some(Payload::Data(bytes)) => {
                    if header.is_none() {
                        return Err(rpc_error("body arrived before the header".to_string()));
                    }
                    data.extend_from_slice(&bytes);
                }
                None => return Err(rpc_error("a chunk carried no payload".to_string())),
            }
        }

        let header = header.ok_or_else(|| rpc_error("no header arrived".to_string()))?;
        if data.len() as u64 != header.content_length {
            return Err(rpc_error(format!(
                "stream ended after {} byte(s) but announced {}",
                data.len(),
                header.content_length
            )));
        }

        Ok(proto::ReadBlobResponse {
            success: true,
            error: String::new(),
            data,
            content_hash: header.content_hash,
            content_type: header.content_type,
            metadata: header.metadata,
            hlc: header.hlc,
            version_id: header.version_id,
            content_length: header.content_length,
            epoch_mismatch: false,
        })
    }

    /// Send a `DeleteBlob` request to a specific node with retry.
    pub async fn send_delete(
        &self,
        node_id: NodeId,
        request: proto::DeleteBlobRequest,
    ) -> Result<proto::DeleteBlobResponse, ClusterError> {
        self.with_retry(node_id, |mut client| {
            let req = request.clone();
            async move { client.delete_blob(req).await }
        })
        .await
    }

    /// Send a `DetachObjectData` request to a specific node with retry.
    pub async fn send_detach_object_data(
        &self,
        node_id: NodeId,
        request: proto::DetachObjectDataRequest,
    ) -> Result<proto::DetachObjectDataResponse, ClusterError> {
        self.with_retry(node_id, |mut client| {
            let req = request.clone();
            async move { client.detach_object_data(req).await }
        })
        .await
    }

    /// Send an `AttachObjectData` request to a specific node with retry.
    pub async fn send_attach_object_data(
        &self,
        node_id: NodeId,
        request: proto::AttachObjectDataRequest,
    ) -> Result<proto::AttachObjectDataResponse, ClusterError> {
        self.with_retry(node_id, |mut client| {
            let req = request.clone();
            async move { client.attach_object_data(req).await }
        })
        .await
    }

    /// Ask one node for its local activity mark of an object.
    ///
    /// No retry: the answer is advisory, the caller asks several nodes, and a
    /// slow peer must not hold up a background scan.
    pub async fn send_read_object_heat(
        &self,
        node_id: NodeId,
        request: proto::ReadObjectHeatRequest,
    ) -> Result<proto::ReadObjectHeatResponse, ClusterError> {
        let (call, mut client) = self.open_call(node_id, true).await?;
        let mut req = Request::new(request);
        req.set_timeout(self.config.request_timeout);
        finish_call(call, client.read_object_heat(req).await)
            .map_err(|e| ClusterError::Rpc(format!("read object heat from {node_id}: {e}")))
    }

    /// Ask one node for the EC content digests it holds locally.
    ///
    /// No retry, for the same reason as the activity mark above: the caller asks
    /// every node of the pool and a slow peer must not hold up an admin request.
    /// Unlike the activity mark, a node that does not answer is **not** treated
    /// as a node with nothing to report — the caller counts it as unread and
    /// says so in the report, because "no duplicates" and "not asked" lead to
    /// opposite decisions.
    ///
    /// Ask one node whether it holds each of these deletes.
    ///
    /// One call per owner instead of one per delete per owner. The answers come
    /// back in the order asked, and a node that could not be asked at all is an
    /// error rather than a row of `false`: the caller decides what an
    /// unreachable owner means, and for the sweep's gate it means "not shown to
    /// hold it" (decision EC2-202).
    ///
    /// # Errors
    ///
    /// Returns [`ClusterError::Rpc`] when the peer is unreachable, refuses the
    /// call, or answers with a different number of answers than probes asked.
    /// A node too old to know the call answers `Unimplemented`, which surfaces
    /// here as [`ClusterError::RpcUnimplemented`] and is the caller's signal to ask about
    /// each delete on its own.
    pub async fn send_probe_ec_tombstones(
        &self,
        node_id: NodeId,
        coordinator: NodeId,
        topology_epoch: u64,
        probes: &[EcTombstoneProbe],
    ) -> Result<Vec<bool>, ClusterError> {
        let (call, mut client) = self.open_call(node_id, true).await?;
        let mut req = Request::new(proto::ProbeEcTombstonesRequest {
            coordinator_id: coordinator.to_bytes().to_vec(),
            topology_epoch,
            probes: probes
                .iter()
                .map(|probe| proto::EcTombstoneProbe {
                    bucket: probe.bucket.clone(),
                    key: probe.key.clone(),
                    version_id: probe.version_id.clone().unwrap_or_default(),
                    operation_id: probe.operation_id.to_be_bytes().to_vec(),
                    hlc_wall_time: probe.hlc.wall_time,
                    hlc_logical: probe.hlc.logical,
                    hlc_node_id: probe.hlc.node_id.to_be_bytes().to_vec(),
                })
                .collect(),
        });
        req.set_timeout(self.config.request_timeout);
        let response = finish_call(call, client.probe_ec_tombstones(req).await)
            .map_err(|status| tombstone_probe_error(node_id, *status))?;
        if !response.success {
            return Err(ClusterError::Rpc(format!(
                "probe EC tombstones on {node_id}: {}",
                response.error
            )));
        }
        if response.held.len() != probes.len() {
            return Err(ClusterError::Rpc(format!(
                "probe EC tombstones on {node_id}: {} answer(s) for {} probe(s)",
                response.held.len(),
                probes.len()
            )));
        }
        Ok(response.held)
    }

    /// # Errors
    ///
    /// Returns [`ClusterError::Rpc`] when the peer is unreachable, refuses the
    /// call, or answers with a digest whose content hash is not 32 bytes.
    pub async fn send_scan_ec_content_digests(
        &self,
        node_id: NodeId,
        coordinator: NodeId,
        max_entries: usize,
    ) -> Result<EcContentDigestPage, ClusterError> {
        let (call, mut client) = self.open_call(node_id, true).await?;
        let mut req = Request::new(proto::ScanEcContentDigestsRequest {
            max_entries: u32::try_from(max_entries).unwrap_or(u32::MAX),
            coordinator_id: coordinator.to_bytes().to_vec(),
        });
        req.set_timeout(self.config.request_timeout);
        let response = finish_call(call, client.scan_ec_content_digests(req).await)
            .map_err(|e| ClusterError::Rpc(format!("scan EC content digests on {node_id}: {e}")))?;
        if !response.success {
            return Err(ClusterError::Rpc(format!(
                "scan EC content digests on {node_id}: {}",
                response.error
            )));
        }

        let mut digests = Vec::with_capacity(response.digests.len());
        for digest in response.digests {
            let content_hash_sha256: [u8; 32] =
                digest.content_hash_sha256.as_slice().try_into().map_err(|_| {
                    ClusterError::Rpc(format!(
                        "scan EC content digests on {node_id}: content hash is {} bytes, not 32",
                        digest.content_hash_sha256.len()
                    ))
                })?;
            digests.push(EcContentDigest {
                bucket: digest.bucket,
                key: digest.key,
                version_id: (!digest.version_id.is_empty()).then_some(digest.version_id),
                content_hash_sha256,
                original_size: digest.original_size,
                erasure_set_id: digest.erasure_set_id,
                packed: digest.packed,
            });
        }

        Ok(EcContentDigestPage {
            digests,
            scanned: response.scanned as usize,
            truncated: response.truncated,
        })
    }

    /// Send a `RestoreNamespaceEntry` request to a specific node with retry.
    pub async fn send_restore_namespace_entry(
        &self,
        node_id: NodeId,
        request: proto::RestoreNamespaceEntryRequest,
    ) -> Result<proto::RestoreNamespaceEntryResponse, ClusterError> {
        self.with_retry(node_id, |mut client| {
            let req = request.clone();
            async move { client.restore_namespace_entry(req).await }
        })
        .await
    }

    /// Send a multipart create-session request to a specific node with retry.
    pub async fn send_multipart_create(
        &self,
        node_id: NodeId,
        request: proto::CreateMultipartUploadReplicaRequest,
    ) -> Result<proto::CreateMultipartUploadReplicaResponse, ClusterError> {
        self.with_retry(node_id, |mut client| {
            let req = request.clone();
            async move { client.create_multipart_upload_replica(req).await }
        })
        .await
    }

    /// Send a streaming multipart upload-part request to a specific node.
    pub async fn send_multipart_upload_part_stream(
        &self,
        node_id: NodeId,
        stream: impl Stream<Item = proto::UploadMultipartPartReplicaChunk> + Send + 'static,
    ) -> Result<proto::UploadMultipartPartReplicaResponse, ClusterError> {
        let (call, mut client) = self.open_call(node_id, true).await?;
        let mut request = Request::new(stream);
        request.set_timeout(self.config.request_timeout);
        finish_call(call, client.upload_multipart_part_replica(request).await)
            .map_err(|e| ClusterError::Rpc(format!("multipart part stream to {node_id}: {e}")))
    }

    /// Inspect multipart session and part metadata on a specific node.
    pub async fn send_multipart_inspect(
        &self,
        node_id: NodeId,
        request: proto::InspectMultipartUploadReplicaRequest,
    ) -> Result<proto::InspectMultipartUploadReplicaResponse, ClusterError> {
        self.with_retry(node_id, |mut client| {
            let req = request.clone();
            async move { client.inspect_multipart_upload_replica(req).await }
        })
        .await
    }

    /// Send a multipart complete request to a specific node with retry.
    pub async fn send_multipart_complete(
        &self,
        node_id: NodeId,
        request: proto::CompleteMultipartUploadReplicaRequest,
    ) -> Result<proto::CompleteMultipartUploadReplicaResponse, ClusterError> {
        self.with_retry(node_id, |mut client| {
            let req = request.clone();
            async move { client.complete_multipart_upload_replica(req).await }
        })
        .await
    }

    /// Send a multipart abort request to a specific node with retry.
    pub async fn send_multipart_abort(
        &self,
        node_id: NodeId,
        request: proto::AbortMultipartUploadReplicaRequest,
    ) -> Result<proto::AbortMultipartUploadReplicaResponse, ClusterError> {
        self.with_retry(node_id, |mut client| {
            let req = request.clone();
            async move { client.abort_multipart_upload_replica(req).await }
        })
        .await
    }

    /// Send a `WriteMetadata` request to a specific node with retry.
    pub async fn send_write_metadata(
        &self,
        node_id: NodeId,
        request: proto::WriteMetadataRequest,
    ) -> Result<proto::WriteMetadataResponse, ClusterError> {
        self.with_retry(node_id, |mut client| {
            let req = request.clone();
            async move { client.write_metadata(req).await }
        })
        .await
    }

    /// Send a `ReadMetadata` request to a specific node with retry.
    pub async fn send_read_metadata(
        &self,
        node_id: NodeId,
        request: proto::ReadMetadataRequest,
    ) -> Result<proto::ReadMetadataResponse, ClusterError> {
        self.with_retry(node_id, |mut client| {
            let req = request.clone();
            async move { client.read_metadata(req).await }
        })
        .await
    }

    /// Send a `HeadObject` request to a specific node with retry.
    pub async fn send_head(
        &self,
        node_id: NodeId,
        request: proto::HeadObjectRequest,
    ) -> Result<proto::HeadObjectResponse, ClusterError> {
        self.with_retry(node_id, |mut client| {
            let req = request.clone();
            async move { client.head_object(req).await }
        })
        .await
    }

    /// Send a `ListObjects` request to a specific node with retry.
    pub async fn send_list_objects(
        &self,
        node_id: NodeId,
        request: proto::ListObjectsRequest,
    ) -> Result<proto::ListObjectsResponse, ClusterError> {
        self.with_retry(node_id, |mut client| {
            let req = request.clone();
            async move { client.list_objects(req).await }
        })
        .await
    }

    /// Send a `Health` request to a specific node with retry.
    pub async fn send_health(
        &self,
        node_id: NodeId,
    ) -> Result<proto::HealthResponse, ClusterError> {
        self.with_retry(node_id, |mut client| async move {
            client.health(proto::HealthRequest {}).await
        })
        .await
    }

    /// Send a `DeliverHint` request to a specific node with retry.
    pub async fn send_hint(
        &self,
        node_id: NodeId,
        request: proto::HintRequest,
    ) -> Result<proto::HintResponse, ClusterError> {
        self.with_retry(node_id, |mut client| {
            let req = request.clone();
            async move { client.deliver_hint(req).await }
        })
        .await
    }

    /// Send an `ExchangeMerkleDigest` request to a specific node with retry.
    pub async fn send_merkle_digest(
        &self,
        node_id: NodeId,
        request: proto::MerkleDigestRequest,
    ) -> Result<proto::MerkleDigestResponse, ClusterError> {
        self.with_retry(node_id, |mut client| {
            let req = request.clone();
            async move { client.exchange_merkle_digest(req).await }
        })
        .await
    }

    /// Send a `RepairSync` request and return the response stream.
    pub async fn send_repair_sync(
        &self,
        node_id: NodeId,
        request: proto::RepairSyncRequest,
    ) -> Result<tonic::Streaming<proto::RepairSyncResponse>, ClusterError> {
        let (call, mut client) = self.open_call(node_id, false).await?;
        finish_call(call, client.repair_sync(request).await)
            .map_err(|e| ClusterError::Rpc(format!("repair sync to {node_id}: {e}")))
    }

    /// Start a streaming write to a node (for objects > 8MB).
    pub async fn send_write_stream(
        &self,
        node_id: NodeId,
        stream: impl Stream<Item = proto::WriteBlobChunk> + Send + 'static,
    ) -> Result<proto::WriteBlobResponse, ClusterError> {
        let (call, mut client) = self.open_call(node_id, false).await?;
        finish_call(call, client.write_blob_stream(Request::new(stream)).await)
            .map_err(|e| ClusterError::Rpc(format!("streaming write to {node_id}: {e}")))
    }

    /// Start a streaming read from a node (for objects > 8MB).
    pub async fn send_read_stream(
        &self,
        node_id: NodeId,
        request: proto::ReadBlobRequest,
    ) -> Result<tonic::Streaming<proto::ReadBlobChunk>, ClusterError> {
        let (call, mut client) = self.open_call(node_id, false).await?;
        finish_call(call, client.read_blob_stream(request).await)
            .map_err(|e| ClusterError::Rpc(format!("streaming read from {node_id}: {e}")))
    }

    /// Remove a cached connection (e.g., when a node is declared dead).
    pub fn disconnect(&self, node_id: &NodeId) {
        if self.connections.remove(node_id).is_some() {
            debug!(%node_id, "removed cached gRPC connection");
        }
    }

    /// Return the number of cached connections.
    pub fn connection_count(&self) -> usize {
        self.connections.len()
    }

    // ========================================================================
    // Internal helpers
    // ========================================================================

    /// Execute an RPC with retry and exponential backoff.
    async fn with_retry<F, Fut, R>(&self, node_id: NodeId, make_call: F) -> Result<R, ClusterError>
    where
        F: Fn(S4NodeClient<Channel>) -> Fut,
        Fut: std::future::Future<Output = Result<Response<R>, Status>>,
    {
        self.with_retry_status(node_id, make_call)
            .await
            .map_err(|failure| match failure {
                RetryFailure::Cluster(error) => error,
                RetryFailure::Rpc { attempts, status } => ClusterError::Rpc(format!(
                    "RPC to {node_id} failed after {attempts} attempts: {status}"
                )),
            })
    }

    /// [`with_retry`](Self::with_retry), keeping the gRPC status of the call
    /// that finally failed.
    ///
    /// A caller that has to tell one refusal from another — a peer that does
    /// not serve a method from a peer that could not answer — needs the code,
    /// and reading it back out of a formatted message would be guesswork.
    async fn with_retry_status<F, Fut, R>(
        &self,
        node_id: NodeId,
        make_call: F,
    ) -> Result<R, RetryFailure>
    where
        F: Fn(S4NodeClient<Channel>) -> Fut,
        Fut: std::future::Future<Output = Result<Response<R>, Status>>,
    {
        // One call however many attempts it takes: what the caller asked for
        // and how it ended, not how many times the transport tried.
        let call = PeerCall::start(self.peer_label(node_id));

        // Fast-fail if gossip says the node is dead.
        if let Err(error) = self.check_node_alive(node_id) {
            call.finish(PeerOutcome::Skipped);
            return Err(RetryFailure::Cluster(error));
        }

        let mut backoff = self.config.initial_backoff;

        for attempt in 0..=self.config.max_retries {
            let client = match self.get_or_connect(node_id).await {
                Ok(client) => client,
                Err(error) => {
                    call.finish(PeerOutcome::Error);
                    return Err(RetryFailure::Cluster(error));
                }
            };

            match make_call(client).await {
                Ok(response) => {
                    call.finish(PeerOutcome::Ok);
                    return Ok(response.into_inner());
                }
                Err(status) => {
                    if !is_retryable(&status) || attempt == self.config.max_retries {
                        // Evict bad connections on transport errors.
                        if is_connection_error(&status) {
                            self.disconnect(&node_id);
                        }
                        call.finish(peer_outcome(&status));
                        return Err(RetryFailure::Rpc {
                            attempts: attempt + 1,
                            status: Box::new(status),
                        });
                    }

                    warn!(
                        %node_id,
                        attempt = attempt + 1,
                        max = self.config.max_retries,
                        status = %status,
                        backoff_ms = backoff.as_millis(),
                        "retrying RPC"
                    );

                    // Evict on connection-level errors before retry.
                    if is_connection_error(&status) {
                        self.disconnect(&node_id);
                    }

                    tokio::time::sleep(backoff).await;
                    backoff *= 2;
                }
            }
        }

        unreachable!("loop always returns")
    }

    /// Get a cached connection or create a new one.
    ///
    /// The address gossip currently reports is resolved first, on every call,
    /// and a cached connection is reused only while it still points there. That
    /// is one read of the membership map per RPC; the alternative is to keep
    /// talking to an address the node left, which on a cluster that reassigns
    /// addresses means talking to a different node entirely.
    ///
    /// It does not make a channel already handed out follow the node: tonic
    /// reconnects a clone to the address it was created with. The pool stops
    /// issuing the stale one, and calls in flight fail as they would against
    /// any peer that went away.
    async fn get_or_connect(&self, node_id: NodeId) -> Result<S4NodeClient<Channel>, ClusterError> {
        let addr = self.resolve_grpc_addr(node_id)?;

        // Fast path: return cached connection.
        if let Some(cached) = self.connections.get(&node_id) {
            if cached.addr == addr {
                trace!(%node_id, "reusing cached gRPC connection");
                return Ok(cached.client.clone());
            }
            let stale = cached.addr;
            drop(cached);
            self.connections.remove(&node_id);
            debug!(
                %node_id,
                %stale,
                %addr,
                "node answers on another address; dropping the cached gRPC connection"
            );
        }

        let channel = self.connect_to(addr).await?;
        let client = S4NodeClient::new(channel)
            .max_decoding_message_size(self.config.max_message_size)
            .max_encoding_message_size(self.config.max_message_size);

        self.connections.insert(
            node_id,
            CachedConnection {
                addr,
                client: client.clone(),
            },
        );
        debug!(%node_id, %addr, "established gRPC connection");
        Ok(client)
    }

    /// Create a gRPC channel to the given address.
    async fn connect_to(&self, addr: SocketAddr) -> Result<Channel, ClusterError> {
        connect_channel(
            addr,
            self.config.connect_timeout,
            self.config.request_timeout,
            self.config.tls.as_ref(),
        )
        .await
    }

    /// Resolve the gRPC address for a node via gossip.
    fn resolve_grpc_addr(&self, node_id: NodeId) -> Result<SocketAddr, ClusterError> {
        self.gossip.peer_grpc_addr(&node_id).ok_or_else(|| {
            ClusterError::Rpc(format!("node {node_id} not found in gossip membership"))
        })
    }

    /// Fast-fail if the node is known to be dead.
    fn check_node_alive(&self, node_id: NodeId) -> Result<(), ClusterError> {
        if let Some(status) = self.gossip.peer_status(&node_id) {
            if matches!(status, NodeStatus::Dead | NodeStatus::Left) {
                self.disconnect(&node_id);
                return Err(ClusterError::Rpc(format!(
                    "node {node_id} is {status:?}, skipping RPC"
                )));
            }
        }
        Ok(())
    }

    /// Whether gossip holds `node_id` dead or gone, so that a call to it would
    /// be skipped without being made.
    pub(crate) fn is_down(&self, node_id: NodeId) -> bool {
        matches!(
            self.gossip.peer_status(&node_id),
            Some(NodeStatus::Dead | NodeStatus::Left)
        )
    }
}

/// Open a gRPC channel to one address.
///
/// Shared by the connection pool and by the callers that have an address but no
/// membership yet, which is the case for every check that has to run before this
/// node joins gossip.
async fn connect_channel(
    addr: SocketAddr,
    connect_timeout: Duration,
    request_timeout: Duration,
    tls: Option<&TlsConfig>,
) -> Result<Channel, ClusterError> {
    let scheme = if tls.is_some() { "https" } else { "http" };
    let uri = format!("{scheme}://{addr}");

    let mut endpoint = Endpoint::from_shared(uri)
        .map_err(|e| ClusterError::Rpc(format!("invalid endpoint: {e}")))?
        .connect_timeout(connect_timeout)
        .timeout(request_timeout);

    if let Some(tls) = tls {
        let tls_cfg = tls.client_config()?;
        endpoint = endpoint
            .tls_config(tls_cfg)
            .map_err(|e| ClusterError::Tls(format!("client TLS config error: {e}")))?;
    }

    endpoint
        .connect()
        .await
        .map_err(|e| ClusterError::Rpc(format!("failed to connect to {addr}: {e}")))
}

/// Ask whoever is listening at `addr` which identity it answers for.
///
/// Deliberately free of the connection pool and of gossip: its one caller runs
/// before this node has joined the cluster, when there is no membership to
/// resolve an address from and no identity of our own to announce yet. It opens
/// one channel, asks `Health`, and drops the channel.
///
/// An error means only that this address did not answer — a peer that is down,
/// starting, or not there at all. It is never evidence about who owns an
/// identity, so a caller must treat it as "no answer" rather than as "nobody
/// holds it".
pub async fn probe_node_identity(
    addr: SocketAddr,
    connect_timeout: Duration,
    request_timeout: Duration,
    tls: Option<&TlsConfig>,
) -> Result<NodeId, ClusterError> {
    let channel = connect_channel(addr, connect_timeout, request_timeout, tls).await?;
    let response = S4NodeClient::new(channel)
        .health(proto::HealthRequest {})
        .await
        .map_err(|status| ClusterError::Rpc(format!("health probe to {addr} failed: {status}")))?
        .into_inner();
    NodeId::from_bytes(&response.node_id)
}

/// Determine if a gRPC status code is retryable.
fn is_retryable(status: &Status) -> bool {
    matches!(
        status.code(),
        tonic::Code::Unavailable
            | tonic::Code::DeadlineExceeded
            | tonic::Code::ResourceExhausted
            | tonic::Code::Aborted
            | tonic::Code::Internal
            | tonic::Code::Unknown
    )
}

/// Determine if a gRPC status indicates a connection-level error.
fn is_connection_error(status: &Status) -> bool {
    matches!(
        status.code(),
        tonic::Code::Unavailable | tonic::Code::Unknown
    )
}

/// How a call that failed with `status` is counted.
///
/// tonic reports its own request timeout as `Cancelled` with the message
/// "Timeout expired"; a deadline the peer enforced is `DeadlineExceeded`.
/// Both mean the peer did not answer in time. Anything else is an error.
fn peer_outcome(status: &Status) -> PeerOutcome {
    match status.code() {
        tonic::Code::DeadlineExceeded => PeerOutcome::Timeout,
        tonic::Code::Cancelled if status.message() == "Timeout expired" => PeerOutcome::Timeout,
        _ => PeerOutcome::Error,
    }
}

/// Records how a call opened with [`NodeClient::open_call`] ended.
fn finish_call<T>(call: PeerCall, result: Result<Response<T>, Status>) -> Result<T, Box<Status>> {
    match result {
        Ok(response) => {
            call.finish(PeerOutcome::Ok);
            Ok(response.into_inner())
        }
        Err(status) => {
            call.finish(peer_outcome(&status));
            Err(Box::new(status))
        }
    }
}

fn tombstone_probe_error(node_id: NodeId, status: tonic::Status) -> ClusterError {
    let message = format!("probe EC tombstones on {node_id}: {status}");
    if status.code() == tonic::Code::Unimplemented {
        ClusterError::RpcUnimplemented(message)
    } else {
        ClusterError::Rpc(message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gossip::{GossipHandle, NodeState};
    use crate::identity::{NodeAddr, NodeCapacity, NodeMeta, PoolId};
    use std::collections::HashMap;

    #[test]
    fn only_unimplemented_tombstone_probe_enables_legacy_fallback() {
        for code in [
            tonic::Code::Unimplemented,
            tonic::Code::Unavailable,
            tonic::Code::DeadlineExceeded,
            tonic::Code::FailedPrecondition,
            tonic::Code::Internal,
        ] {
            let error = tombstone_probe_error(NodeId(1), tonic::Status::new(code, "Unimplemented"));
            assert_eq!(
                matches!(error, ClusterError::RpcUnimplemented(_)),
                code == tonic::Code::Unimplemented,
                "code {code:?}"
            );
        }
    }

    fn test_node(node_id: NodeId, grpc_port: u16) -> NodeState {
        NodeState {
            meta: NodeMeta {
                node_id,
                node_name: "test-node".into(),
                addr: NodeAddr {
                    grpc_addr: SocketAddr::from(([127, 0, 0, 1], grpc_port)),
                    http_addr: SocketAddr::from(([127, 0, 0, 1], 9000)),
                    gossip_addr: SocketAddr::from(([127, 0, 0, 1], 9200)),
                },
                pool_id: PoolId::new(1),
                topology_epoch: 1,
                protocol_version: 1,
                capacity: NodeCapacity::default(),
                started_at: 0,
                s4_version: "0.1.0".into(),
                edition: crate::identity::Edition::Community,
            },
            status: NodeStatus::Alive,
            last_seen: std::time::Instant::now(),
        }
    }

    fn dead_node(node_id: NodeId) -> NodeState {
        let mut state = test_node(node_id, 9100);
        state.status = NodeStatus::Dead;
        state
    }

    fn blob_request(size: usize) -> proto::WriteBlobRequest {
        proto::WriteBlobRequest {
            bucket: "bucket".into(),
            key: "key".into(),
            data: (0..size).map(|idx| (idx % 251) as u8).collect(),
            content_hash: vec![7u8; 32],
            content_type: "application/octet-stream".into(),
            metadata: HashMap::from([("k".to_string(), "v".to_string())]),
            hlc: Some(crate::hlc::HlcTimestamp::from_record(5, 6, 7).to_proto()),
            version_id: Some("v1".into()),
            coordinator_id: vec![1u8; 16],
            operation_id: vec![2u8; 16],
            topology_epoch: 42,
        }
    }

    /// The header goes first and carries every field of the unary request, and
    /// the body follows it split at the chunk size — the last piece being
    /// whatever is left over.
    #[test]
    fn a_streamed_write_is_a_header_then_the_body_in_pieces() {
        use proto::write_blob_chunk::Payload;

        let request = blob_request(1000);
        let chunks = write_blob_chunks(&request, 256);

        assert_eq!(
            chunks.len(),
            1 + 4,
            "one header plus ceil(1000 / 256) pieces"
        );
        let Some(Payload::Header(header)) = &chunks[0].payload else {
            panic!("the first chunk must be the header");
        };
        assert_eq!(header.total_size, 1000);
        assert_eq!(header.bucket, request.bucket);
        assert_eq!(header.key, request.key);
        assert_eq!(header.content_hash, request.content_hash);
        assert_eq!(header.content_type, request.content_type);
        assert_eq!(header.metadata, request.metadata);
        assert_eq!(header.hlc, request.hlc);
        assert_eq!(header.version_id, request.version_id);
        assert_eq!(header.coordinator_id, request.coordinator_id);
        assert_eq!(header.operation_id, request.operation_id);
        assert_eq!(header.topology_epoch, request.topology_epoch);

        let body = chunks[1..]
            .iter()
            .map(|chunk| match &chunk.payload {
                Some(Payload::Data(bytes)) => bytes.clone(),
                _ => panic!("every chunk after the header must be body"),
            })
            .collect::<Vec<_>>();
        assert_eq!(
            body.iter().map(Vec::len).collect::<Vec<_>>(),
            vec![256, 256, 256, 232]
        );
        assert_eq!(
            body.concat(),
            request.data,
            "the body must survive the split"
        );
    }

    /// An object of no bytes is still an object: it produces a header and
    /// nothing else, and the header says so.
    #[test]
    fn an_empty_body_still_sends_its_header() {
        use proto::write_blob_chunk::Payload;

        let chunks = write_blob_chunks(&blob_request(0), 256);
        assert_eq!(chunks.len(), 1);
        let Some(Payload::Header(header)) = &chunks[0].payload else {
            panic!("the only chunk must be the header");
        };
        assert_eq!(header.total_size, 0);
    }

    /// Every piece has to fit a message with room to spare for the header
    /// fields that ride along, or the split would not have helped. Checked at
    /// compile time: these are constants, and a build that breaks the relation
    /// must not produce a binary.
    #[test]
    fn a_piece_of_the_body_is_far_below_the_message_limit() {
        const {
            assert!(
                DEFAULT_CHUNK_SIZE < DEFAULT_MAX_MESSAGE_SIZE / 4,
                "a body piece must leave room for the rest of the message"
            )
        };
        const {
            assert!(
                STREAMING_THRESHOLD < DEFAULT_MAX_MESSAGE_SIZE,
                "the unary path must still carry everything below the threshold"
            )
        };
    }

    #[test]
    fn is_retryable_categorization() {
        assert!(is_retryable(&Status::unavailable("down")));
        assert!(is_retryable(&Status::deadline_exceeded("slow")));
        assert!(is_retryable(&Status::resource_exhausted("busy")));
        assert!(is_retryable(&Status::aborted("conflict")));
        assert!(is_retryable(&Status::internal("oops")));

        assert!(!is_retryable(&Status::invalid_argument("bad")));
        assert!(!is_retryable(&Status::not_found("missing")));
        assert!(!is_retryable(&Status::permission_denied("no")));
        assert!(!is_retryable(&Status::unimplemented("nope")));
    }

    #[test]
    fn dead_node_fails_immediately() {
        let node_id = NodeId::generate();
        let mut members = HashMap::new();
        members.insert(node_id, dead_node(node_id));

        let gossip = Arc::new(GossipHandle::new_for_testing(members));
        let client = NodeClient::new(gossip, NodeClientConfig::default());

        let result = client.check_node_alive(node_id);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Dead"));
    }

    #[test]
    fn alive_node_passes_check() {
        let node_id = NodeId::generate();
        let mut members = HashMap::new();
        members.insert(node_id, test_node(node_id, 9100));

        let gossip = Arc::new(GossipHandle::new_for_testing(members));
        let client = NodeClient::new(gossip, NodeClientConfig::default());

        assert!(client.check_node_alive(node_id).is_ok());
    }

    #[test]
    fn unknown_node_passes_alive_check() {
        // If gossip doesn't know the node, we let the connection attempt proceed.
        // This handles bootstrap scenarios where gossip hasn't converged yet.
        let gossip = Arc::new(GossipHandle::new_for_testing(HashMap::new()));
        let client = NodeClient::new(gossip, NodeClientConfig::default());

        let unknown_id = NodeId::generate();
        assert!(client.check_node_alive(unknown_id).is_ok());
    }

    #[test]
    fn resolve_addr_for_known_node() {
        let node_id = NodeId::generate();
        let port = 19150;
        let mut members = HashMap::new();
        members.insert(node_id, test_node(node_id, port));

        let gossip = Arc::new(GossipHandle::new_for_testing(members));
        let client = NodeClient::new(gossip, NodeClientConfig::default());

        let addr = client.resolve_grpc_addr(node_id).unwrap();
        assert_eq!(addr.port(), port);
    }

    #[test]
    fn resolve_addr_for_unknown_node_fails() {
        let gossip = Arc::new(GossipHandle::new_for_testing(HashMap::new()));
        let client = NodeClient::new(gossip, NodeClientConfig::default());

        let result = client.resolve_grpc_addr(NodeId::generate());
        assert!(result.is_err());
    }

    #[test]
    fn disconnect_removes_cached_connection() {
        let gossip = Arc::new(GossipHandle::new_for_testing(HashMap::new()));
        let client = NodeClient::new(gossip, NodeClientConfig::default());

        assert_eq!(client.connection_count(), 0);

        // Disconnect a non-existent connection is a no-op.
        client.disconnect(&NodeId::generate());
        assert_eq!(client.connection_count(), 0);
    }

    #[tokio::test]
    async fn health_check_to_running_server() {
        use crate::rpc::server::{NodeRpcServer, RpcServerConfig};

        let node_id = NodeId::generate();
        let meta = Arc::new(NodeMeta {
            node_id,
            node_name: "health-test-node".into(),
            addr: NodeAddr {
                grpc_addr: "127.0.0.1:19160".parse().unwrap(),
                http_addr: "127.0.0.1:19000".parse().unwrap(),
                gossip_addr: "127.0.0.1:19200".parse().unwrap(),
            },
            pool_id: PoolId::new(1),
            topology_epoch: 1,
            protocol_version: 1,
            capacity: NodeCapacity {
                total_bytes: 500_000_000,
                used_bytes: 50_000_000,
                volume_count: 3,
                object_count: 500,
            },
            started_at: 1700000000,
            s4_version: "0.1.0".into(),
            edition: crate::identity::Edition::Community,
        });

        // Start a server for the node.
        let server_gossip = Arc::new(GossipHandle::new_for_testing(HashMap::new()));
        let server = NodeRpcServer::new(node_id, meta.clone(), server_gossip);
        let server_handle = server
            .start(RpcServerConfig {
                bind_addr: "127.0.0.1:19160".parse().unwrap(),
                ..Default::default()
            })
            .await
            .unwrap();

        // Create a client that knows about this node.
        let mut members = HashMap::new();
        members.insert(
            node_id,
            NodeState {
                meta: (*meta).clone(),
                status: NodeStatus::Alive,
                last_seen: std::time::Instant::now(),
            },
        );
        let client_gossip = Arc::new(GossipHandle::new_for_testing(members));
        let client = NodeClient::new(client_gossip, NodeClientConfig::default());

        // Call health via the client pool.
        let health = client.send_health(node_id).await.unwrap();
        assert_eq!(health.node_name, "health-test-node");
        assert_eq!(health.pool_id, 1);
        assert_eq!(health.status, proto::NodeHealthStatus::Healthy as i32);

        server_handle.shutdown().await.unwrap();
    }

    /// A node keeps its id across a restart; it does not keep its address. The
    /// pool has to notice, or it goes on sending that node's requests to the
    /// address it left — which on a cluster that reassigns addresses is another
    /// node of the same cluster, answering as itself.
    #[tokio::test]
    async fn a_node_that_moved_is_not_reached_through_the_cached_connection() {
        use crate::rpc::server::{NodeRpcServer, RpcServerConfig};

        let node_id = NodeId::generate();
        let before: SocketAddr = "127.0.0.1:19170".parse().unwrap();
        let after: SocketAddr = "127.0.0.1:19171".parse().unwrap();

        let meta_at = |addr: SocketAddr, name: &str| NodeMeta {
            node_id,
            node_name: name.into(),
            addr: NodeAddr {
                grpc_addr: addr,
                http_addr: "127.0.0.1:19002".parse().unwrap(),
                gossip_addr: addr,
            },
            pool_id: PoolId::new(1),
            topology_epoch: 1,
            protocol_version: 1,
            capacity: NodeCapacity::default(),
            started_at: 1_700_000_000,
            s4_version: "0.1.0".into(),
            edition: crate::identity::Edition::Community,
        };

        let mut servers = Vec::new();
        for (addr, name) in [
            (before, "the-run-that-ended"),
            (after, "the-run-that-answers"),
        ] {
            let server = NodeRpcServer::new(
                node_id,
                Arc::new(meta_at(addr, name)),
                Arc::new(GossipHandle::new_for_testing(HashMap::new())),
            );
            servers.push(
                server
                    .start(RpcServerConfig {
                        bind_addr: addr,
                        ..Default::default()
                    })
                    .await
                    .unwrap(),
            );
        }

        let mut members = HashMap::new();
        members.insert(
            node_id,
            NodeState {
                meta: meta_at(before, "the-run-that-ended"),
                status: NodeStatus::Alive,
                last_seen: std::time::Instant::now(),
            },
        );
        let gossip = Arc::new(GossipHandle::new_for_testing(members));
        let client = NodeClient::new(gossip.clone(), NodeClientConfig::default());

        let health = client.send_health(node_id).await.unwrap();
        assert_eq!(health.node_name, "the-run-that-ended");
        assert_eq!(client.connection_count(), 1);

        gossip.set_member_for_testing(
            node_id,
            NodeState {
                meta: meta_at(after, "the-run-that-answers"),
                status: NodeStatus::Alive,
                last_seen: std::time::Instant::now(),
            },
        );

        let health = client.send_health(node_id).await.unwrap();
        assert_eq!(
            health.node_name, "the-run-that-answers",
            "the pool kept the connection to the address the node had left"
        );

        for server in servers {
            server.shutdown().await.unwrap();
        }
    }

    #[tokio::test]
    async fn identity_probe_names_the_node_listening_at_an_address() {
        use crate::rpc::server::{NodeRpcServer, RpcServerConfig};

        let bind_addr: SocketAddr = "127.0.0.1:19161".parse().unwrap();
        let node_id = NodeId::generate();
        let meta = Arc::new(NodeMeta {
            node_id,
            node_name: "identity-probe-node".into(),
            addr: NodeAddr {
                grpc_addr: bind_addr,
                http_addr: "127.0.0.1:19001".parse().unwrap(),
                gossip_addr: bind_addr,
            },
            pool_id: PoolId::new(1),
            topology_epoch: 1,
            protocol_version: 1,
            capacity: NodeCapacity::default(),
            started_at: 1_700_000_000,
            s4_version: "0.1.0".into(),
            edition: crate::identity::Edition::Community,
        });

        // Nothing is listening yet: the probe has no answer, and that is not an
        // answer about who owns the identity either.
        let before = probe_node_identity(
            bind_addr,
            Duration::from_millis(500),
            Duration::from_secs(2),
            None,
        )
        .await;
        assert!(before.is_err(), "an unused address must not name a node");

        let server = NodeRpcServer::new(
            node_id,
            meta,
            Arc::new(GossipHandle::new_for_testing(HashMap::new())),
        );
        let server_handle = server
            .start(RpcServerConfig {
                bind_addr,
                ..Default::default()
            })
            .await
            .unwrap();

        // The probe needs neither gossip nor a connection pool: a replacement
        // node runs it before it has either.
        let answered = probe_node_identity(
            bind_addr,
            Duration::from_millis(500),
            Duration::from_secs(2),
            None,
        )
        .await
        .unwrap();
        assert_eq!(answered, node_id);

        server_handle.shutdown().await.unwrap();
    }

    // ---- Calls to peers, as the metrics count them ----

    use crate::cluster_metrics::PeerNames;
    use crate::test_metrics::TestMetrics;

    /// A client whose gossip knows `members`, with each peer named for metrics.
    fn named_client(
        members: Vec<(NodeId, NodeState, &str)>,
        config: NodeClientConfig,
    ) -> NodeClient {
        let names = Arc::new(PeerNames::default());
        names.set(members.iter().map(|(id, _, name)| (*id, name.to_string())));
        let gossip = Arc::new(GossipHandle::new_for_testing(
            members.into_iter().map(|(id, state, _)| (id, state)).collect(),
        ));
        NodeClient::new(gossip, config).with_peer_names(names)
    }

    fn quick_config() -> NodeClientConfig {
        NodeClientConfig {
            connect_timeout: Duration::from_millis(500),
            max_retries: 2,
            initial_backoff: Duration::from_millis(1),
            ..NodeClientConfig::default()
        }
    }

    /// A port nothing listens on.
    fn closed_port() -> u16 {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        listener.local_addr().expect("addr").port()
    }

    /// A listener that accepts connections and never says a word.
    async fn silent_peer() -> u16 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let port = listener.local_addr().expect("addr").port();
        tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((socket, _)) = listener.accept().await {
                held.push(socket);
            }
        });
        port
    }

    const CALLS: &str = "s4_peer_requests_total";
    const DURATION_COUNT: &str = "s4_peer_request_duration_seconds_count";

    #[tokio::test]
    async fn a_dead_peer_is_skipped_and_counted_against_that_peer_only() {
        let metrics = TestMetrics::install();
        let dead = NodeId::generate();
        let alive = NodeId::generate();
        let client = named_client(
            vec![
                (dead, dead_node(dead), "node-dead"),
                (alive, test_node(alive, closed_port()), "node-alive"),
            ],
            quick_config(),
        );

        assert!(client.send_health(dead).await.is_err());

        assert_eq!(
            metrics.value(CALLS, &[("peer", "node-dead"), ("outcome", "skipped")]),
            Some(1.0)
        );
        assert_eq!(metrics.sum(CALLS, &[("peer", "node-alive")]), 0.0);
        // No call was made, so no time was spent on one.
        assert_eq!(
            metrics.value(DURATION_COUNT, &[("peer", "node-dead")]),
            None
        );
    }

    #[tokio::test]
    async fn a_refused_connection_is_one_error_however_many_retries() {
        let metrics = TestMetrics::install();
        let peer = NodeId::generate();
        let client = named_client(
            vec![(peer, test_node(peer, closed_port()), "node-2")],
            quick_config(),
        );

        assert!(client.send_health(peer).await.is_err());

        assert_eq!(
            metrics.value(CALLS, &[("peer", "node-2"), ("outcome", "error")]),
            Some(1.0)
        );
        assert_eq!(metrics.sum(CALLS, &[]), 1.0);
        assert_eq!(
            metrics.value(DURATION_COUNT, &[("peer", "node-2")]),
            Some(1.0)
        );
    }

    #[tokio::test]
    async fn an_answered_call_is_ok_and_timed() {
        use crate::rpc::server::{NodeRpcServer, RpcServerConfig};

        let metrics = TestMetrics::install();
        let peer = NodeId::generate();
        let state = test_node(peer, 19180);
        let server = NodeRpcServer::new(
            peer,
            Arc::new(state.meta.clone()),
            Arc::new(GossipHandle::new_for_testing(HashMap::new())),
        );
        let handle = server
            .start(RpcServerConfig {
                bind_addr: state.meta.addr.grpc_addr,
                ..Default::default()
            })
            .await
            .expect("server starts");
        let client = named_client(vec![(peer, state, "node-3")], quick_config());

        client.send_health(peer).await.expect("health answered");
        client.send_health(peer).await.expect("health answered");

        assert_eq!(
            metrics.value(CALLS, &[("peer", "node-3"), ("outcome", "ok")]),
            Some(2.0)
        );
        assert_eq!(
            metrics.value(DURATION_COUNT, &[("peer", "node-3")]),
            Some(2.0)
        );
        handle.shutdown().await.expect("server stops");
    }

    #[tokio::test]
    async fn a_peer_that_takes_the_call_and_never_answers_times_out() {
        let metrics = TestMetrics::install();
        let peer = NodeId::generate();
        let client = named_client(
            vec![(peer, test_node(peer, silent_peer().await), "node-slow")],
            NodeClientConfig {
                request_timeout: Duration::from_millis(200),
                ..quick_config()
            },
        );

        assert!(client.send_health(peer).await.is_err());

        assert_eq!(
            metrics.value(CALLS, &[("peer", "node-slow"), ("outcome", "timeout")]),
            Some(1.0),
            "{}",
            metrics.render()
        );
        assert_eq!(
            metrics.value(DURATION_COUNT, &[("peer", "node-slow")]),
            Some(1.0)
        );
    }

    #[tokio::test]
    async fn a_call_nobody_waits_for_any_more_is_cancelled() {
        let metrics = TestMetrics::install();
        let peer = NodeId::generate();
        let client = named_client(
            vec![(peer, test_node(peer, silent_peer().await), "node-slow")],
            quick_config(),
        );

        // The caller's own deadline, as a coordinator has around every
        // replica call: the call is dropped while the peer still says nothing.
        let abandoned =
            tokio::time::timeout(Duration::from_millis(200), client.send_health(peer)).await;
        assert!(abandoned.is_err(), "the caller gave up first");

        assert_eq!(
            metrics.value(CALLS, &[("peer", "node-slow"), ("outcome", "cancelled")]),
            Some(1.0),
            "{}",
            metrics.render()
        );
        // A call that never ended has no duration.
        assert_eq!(
            metrics.value(DURATION_COUNT, &[("peer", "node-slow")]),
            None
        );
    }

    #[tokio::test]
    async fn a_peer_outside_the_named_pool_is_labelled_by_its_identity() {
        let metrics = TestMetrics::install();
        let stranger = NodeId::generate();
        let gossip =
            GossipHandle::new_for_testing(HashMap::from([(stranger, dead_node(stranger))]));
        let client = NodeClient::new(Arc::new(gossip), quick_config());

        assert!(client.send_health(stranger).await.is_err());

        assert_eq!(
            metrics.value(
                CALLS,
                &[
                    ("peer", stranger.to_string().as_str()),
                    ("outcome", "skipped")
                ]
            ),
            Some(1.0)
        );
    }
}
