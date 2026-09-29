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

//! gRPC server for inter-node communication.
//!
//! Each node in the cluster runs a [`NodeRpcServer`] that accepts requests
//! from coordinator nodes and executes local storage operations.
//!
//! In Phase 4, WriteBlob and DeleteBlob are wired to the storage engine
//! via [`ReplicaWriteHandler`]. Other data plane RPCs remain unimplemented
//! until Phase 5+.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::AsyncReadExt;
use tokio::sync::watch;
use tonic::transport::Server;
use tonic::{Request, Response, Status};
use tracing::{error, info};

use crate::ec_object::{
    EcContentDigest, EcContentDigestScan, EcHeatQuery, EcTombstoneProbe, EcTombstoneProbeService,
};
use crate::error::ClusterError;
use crate::gossip::GossipHandle;
use crate::identity::{NodeId, NodeMeta};
use crate::repair::anti_entropy::AntiEntropyService;
use crate::replica::{ReplicaMultipartHandler, ReplicaReadHandler, ReplicaWriteHandler};
use crate::rpc::proto::{self, s4_node_server};
use crate::rpc::tls::TlsConfig;
use crate::rpc::DEFAULT_MAX_MESSAGE_SIZE;

/// Configuration for the gRPC server.
#[derive(Debug, Clone)]
pub struct RpcServerConfig {
    /// Address to bind the gRPC server to.
    pub bind_addr: SocketAddr,
    /// Optional TLS configuration for encrypted inter-node traffic.
    pub tls: Option<TlsConfig>,
    /// Maximum inbound message size (default: 16 MB).
    pub max_message_size: usize,
    /// Graceful shutdown timeout.
    pub shutdown_timeout: Duration,
}

impl Default for RpcServerConfig {
    fn default() -> Self {
        Self {
            bind_addr: ([0, 0, 0, 0], crate::rpc::DEFAULT_GRPC_PORT).into(),
            tls: None,
            max_message_size: DEFAULT_MAX_MESSAGE_SIZE,
            shutdown_timeout: Duration::from_secs(10),
        }
    }
}

/// gRPC server for inter-node communication.
///
/// Accepts requests from coordinator nodes and executes local operations.
/// The server is started via [`NodeRpcServer::start`] which spawns a
/// background task and returns a shutdown handle.
pub struct NodeRpcServer {
    node_id: NodeId,
    node_meta: Arc<NodeMeta>,
    gossip_handle: Arc<GossipHandle>,
    replica_handler: Option<Arc<ReplicaWriteHandler>>,
    multipart_handler: Option<Arc<ReplicaMultipartHandler>>,
    read_handler: Option<Arc<ReplicaReadHandler>>,
    anti_entropy: Option<Arc<AntiEntropyService>>,
    heat_query: Option<Arc<dyn EcHeatQuery>>,
    content_digest_scan: Option<Arc<dyn EcContentDigestScan>>,
    tombstone_probe: Option<Arc<dyn EcTombstoneProbeService>>,
    started_at: std::time::Instant,
}

impl NodeRpcServer {
    /// Create a new gRPC server instance without a storage engine.
    ///
    /// WriteBlob and DeleteBlob will return `Unimplemented`. Use
    /// [`with_replica_handler`](Self::with_replica_handler) to wire up
    /// the storage engine for Phase 4+ operations.
    pub fn new(
        node_id: NodeId,
        node_meta: Arc<NodeMeta>,
        gossip_handle: Arc<GossipHandle>,
    ) -> Self {
        Self {
            node_id,
            node_meta,
            gossip_handle,
            replica_handler: None,
            multipart_handler: None,
            read_handler: None,
            heat_query: None,
            content_digest_scan: None,
            tombstone_probe: None,
            anti_entropy: None,
            started_at: std::time::Instant::now(),
        }
    }

    /// Attach a replica write handler for Phase 4 data plane operations.
    ///
    /// When set, WriteBlob and DeleteBlob requests will be processed
    /// by the storage engine instead of returning `Unimplemented`.
    pub fn with_replica_handler(mut self, handler: Arc<ReplicaWriteHandler>) -> Self {
        self.replica_handler = Some(handler);
        self
    }

    /// Attach a multipart replica handler for native multipart quorum operations.
    pub fn with_multipart_handler(mut self, handler: Arc<ReplicaMultipartHandler>) -> Self {
        self.multipart_handler = Some(handler);
        self
    }

    /// Attach a replica read handler for Phase 5 data plane operations.
    ///
    /// When set, ReadBlob, HeadObject, and ListObjects requests will be
    /// processed by the storage engine instead of returning `Unimplemented`.
    pub fn with_read_handler(mut self, handler: Arc<ReplicaReadHandler>) -> Self {
        self.read_handler = Some(handler);
        self
    }

    /// Attach the source of this node's erasure-coding activity marks.
    ///
    /// Only an Enterprise node running an EC pool has any; without it the node
    /// answers `Unimplemented`, which peers read as "no mark here".
    pub fn with_heat_query(mut self, query: Arc<dyn EcHeatQuery>) -> Self {
        self.heat_query = Some(query);
        self
    }

    /// Attach the source of this node's EC content digests.
    ///
    /// Only an Enterprise node running an EC pool has manifests to report;
    /// without it the node answers `Unimplemented`, which the dedup measurement
    /// counts as a node it could not read rather than as a node with no
    /// duplicates.
    pub fn with_content_digest_scan(mut self, scan: Arc<dyn EcContentDigestScan>) -> Self {
        self.content_digest_scan = Some(scan);
        self
    }

    /// Attach the answer to "do you hold these deletes?".
    ///
    /// Only an Enterprise node running an EC pool holds tombstone manifests;
    /// without it the node answers `Unimplemented`, and the sweep asks about
    /// each delete on its own instead.
    pub fn with_tombstone_probe(mut self, probe: Arc<dyn EcTombstoneProbeService>) -> Self {
        self.tombstone_probe = Some(probe);
        self
    }

    /// Attach an anti-entropy service for Phase 7 Merkle exchange.
    ///
    /// When set, ExchangeMerkleDigest and RepairSync requests will be
    /// handled by the anti-entropy service.
    pub fn with_anti_entropy(mut self, service: Arc<AntiEntropyService>) -> Self {
        self.anti_entropy = Some(service);
        self
    }

    /// Start the gRPC server in a background task.
    ///
    /// Returns a [`RpcServerHandle`] that can be used to trigger graceful shutdown.
    /// The server binds to `config.bind_addr` and serves the S4Node service.
    pub async fn start(self, config: RpcServerConfig) -> Result<RpcServerHandle, ClusterError> {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let bind_addr = config.bind_addr;
        let max_message_size = config.max_message_size;
        let tls_config = config.tls.clone();
        let shutdown_timeout = config.shutdown_timeout;

        let svc = s4_node_server::S4NodeServer::new(self)
            .max_decoding_message_size(max_message_size)
            .max_encoding_message_size(max_message_size);

        let join_handle = tokio::spawn(async move {
            let mut builder = Server::builder();

            if let Some(tls) = tls_config {
                let tls_cfg = tls.server_config().map_err(|e| {
                    ClusterError::Tls(format!("failed to build server TLS config: {e}"))
                })?;
                builder = builder
                    .tls_config(tls_cfg)
                    .map_err(|e| ClusterError::Tls(format!("invalid TLS config: {e}")))?;
            }

            let mut shutdown_rx = shutdown_rx;
            info!(addr = %bind_addr, "gRPC server starting");

            builder
                .add_service(svc)
                .serve_with_shutdown(bind_addr, async move {
                    let _ = shutdown_rx.wait_for(|&v| v).await;
                    info!("gRPC server received shutdown signal");
                })
                .await
                .map_err(|e| ClusterError::Rpc(format!("gRPC server error: {e}")))?;

            info!("gRPC server stopped");
            Ok::<(), ClusterError>(())
        });

        // Give the server a moment to bind.
        tokio::time::sleep(Duration::from_millis(50)).await;
        info!(addr = %bind_addr, "gRPC server started");

        Ok(RpcServerHandle {
            shutdown_tx,
            join_handle,
            shutdown_timeout,
        })
    }
}

fn delete_request_from_hint(hint_req: &proto::HintRequest) -> proto::DeleteBlobRequest {
    proto::DeleteBlobRequest {
        bucket: hint_req.bucket.clone(),
        key: hint_req.key.clone(),
        hlc: hint_req.hlc.clone(),
        coordinator_id: Vec::new(),
        operation_id: hint_req.operation_id.clone(),
        topology_epoch: 0,
        version_id: hint_req.version_id.clone(),
        expected_source_hlc: hint_req.expected_source_hlc.clone(),
    }
}

#[tonic::async_trait]
impl s4_node_server::S4Node for NodeRpcServer {
    // ========================================================================
    // Cluster management — fully implemented in Phase 2
    // ========================================================================

    async fn health(
        &self,
        _request: Request<proto::HealthRequest>,
    ) -> Result<Response<proto::HealthResponse>, Status> {
        let members = self.gossip_handle.members();
        let alive_count = members
            .values()
            .filter(|s| matches!(s.status, crate::identity::NodeStatus::Alive))
            .count();

        let capacity = Some(proto::NodeCapacityInfo {
            total_bytes: self.node_meta.capacity.total_bytes,
            used_bytes: self.node_meta.capacity.used_bytes,
            volume_count: self.node_meta.capacity.volume_count,
            object_count: self.node_meta.capacity.object_count,
        });

        let response = proto::HealthResponse {
            status: proto::NodeHealthStatus::Healthy.into(),
            node_id: self.node_id.to_bytes().to_vec(),
            node_name: self.node_meta.node_name.clone(),
            pool_id: self.node_meta.pool_id.as_u32(),
            uptime_seconds: self.started_at.elapsed().as_secs(),
            topology_epoch: self.node_meta.topology_epoch,
            protocol_version: self.node_meta.protocol_version,
            s4_version: self.node_meta.s4_version.clone(),
            capacity,
            known_members: members.len() as u32,
            alive_members: alive_count as u32,
        };

        Ok(Response::new(response))
    }

    // ========================================================================
    // Data plane — Phase 4: WriteBlob and DeleteBlob wired to storage engine
    // ========================================================================

    async fn write_blob(
        &self,
        request: Request<proto::WriteBlobRequest>,
    ) -> Result<Response<proto::WriteBlobResponse>, Status> {
        let Some(handler) = &self.replica_handler else {
            return Err(Status::unimplemented(
                "WriteBlob requires a storage engine (not configured)",
            ));
        };

        match handler.handle_write(request.get_ref()).await {
            Ok(response) => Ok(Response::new(response)),
            Err(e) => {
                error!(error = %e, "replica write failed");
                Ok(Response::new(proto::WriteBlobResponse {
                    success: false,
                    error: e.to_string(),
                    local_sequence: 0,
                    epoch_mismatch: false,
                }))
            }
        }
    }

    async fn read_blob(
        &self,
        request: Request<proto::ReadBlobRequest>,
    ) -> Result<Response<proto::ReadBlobResponse>, Status> {
        let Some(handler) = &self.read_handler else {
            return Err(Status::unimplemented(
                "ReadBlob requires a storage engine (not configured)",
            ));
        };

        match handler.handle_read(request.get_ref()).await {
            Ok(response) => Ok(Response::new(response)),
            Err(e) => {
                error!(error = %e, "replica read failed");
                Ok(Response::new(proto::ReadBlobResponse {
                    success: false,
                    error: e.to_string(),
                    data: Vec::new(),
                    content_hash: Vec::new(),
                    content_type: String::new(),
                    metadata: Default::default(),
                    hlc: None,
                    version_id: None,
                    content_length: 0,
                    epoch_mismatch: false,
                }))
            }
        }
    }

    async fn delete_blob(
        &self,
        request: Request<proto::DeleteBlobRequest>,
    ) -> Result<Response<proto::DeleteBlobResponse>, Status> {
        let Some(handler) = &self.replica_handler else {
            return Err(Status::unimplemented(
                "DeleteBlob requires a storage engine (not configured)",
            ));
        };

        match handler.handle_delete(request.get_ref()).await {
            Ok(response) => Ok(Response::new(response)),
            Err(e) => {
                error!(error = %e, "replica delete failed");
                Ok(Response::new(proto::DeleteBlobResponse {
                    success: false,
                    error: e.to_string(),
                    local_sequence: 0,
                    epoch_mismatch: false,
                }))
            }
        }
    }

    async fn detach_object_data(
        &self,
        request: Request<proto::DetachObjectDataRequest>,
    ) -> Result<Response<proto::DetachObjectDataResponse>, Status> {
        let Some(handler) = &self.replica_handler else {
            return Err(Status::unimplemented(
                "DetachObjectData requires a storage engine (not configured)",
            ));
        };

        match handler.handle_detach_object_data(request.get_ref()).await {
            Ok(response) => Ok(Response::new(response)),
            Err(e) => {
                error!(error = %e, "replica detach failed");
                Ok(Response::new(proto::DetachObjectDataResponse {
                    success: false,
                    error: e.to_string(),
                    detached: false,
                    epoch_mismatch: false,
                }))
            }
        }
    }

    async fn attach_object_data(
        &self,
        request: Request<proto::AttachObjectDataRequest>,
    ) -> Result<Response<proto::AttachObjectDataResponse>, Status> {
        let Some(handler) = &self.replica_handler else {
            return Err(Status::unimplemented(
                "AttachObjectData requires a storage engine (not configured)",
            ));
        };

        match handler.handle_attach_object_data(request.get_ref()).await {
            Ok(response) => Ok(Response::new(response)),
            Err(e) => {
                error!(error = %e, "replica attach failed");
                Ok(Response::new(proto::AttachObjectDataResponse {
                    success: false,
                    error: e.to_string(),
                    attached: false,
                    epoch_mismatch: false,
                }))
            }
        }
    }

    async fn restore_namespace_entry(
        &self,
        request: Request<proto::RestoreNamespaceEntryRequest>,
    ) -> Result<Response<proto::RestoreNamespaceEntryResponse>, Status> {
        let Some(handler) = &self.replica_handler else {
            return Err(Status::unimplemented(
                "RestoreNamespaceEntry requires a storage engine (not configured)",
            ));
        };

        match handler.handle_restore_namespace_entry(request.get_ref()).await {
            Ok(response) => Ok(Response::new(response)),
            Err(e) => {
                error!(error = %e, "replica namespace restore failed");
                Ok(Response::new(proto::RestoreNamespaceEntryResponse {
                    success: false,
                    error: e.to_string(),
                    restored: false,
                    epoch_mismatch: false,
                }))
            }
        }
    }

    async fn create_multipart_upload_replica(
        &self,
        request: Request<proto::CreateMultipartUploadReplicaRequest>,
    ) -> Result<Response<proto::CreateMultipartUploadReplicaResponse>, Status> {
        let Some(handler) = &self.multipart_handler else {
            return Err(Status::unimplemented(
                "CreateMultipartUploadReplica requires a multipart handler",
            ));
        };

        match handler.handle_create(request.get_ref()).await {
            Ok(response) => Ok(Response::new(response)),
            Err(e) => {
                error!(error = %e, "replica multipart create failed");
                Ok(Response::new(proto::CreateMultipartUploadReplicaResponse {
                    success: false,
                    error: e.to_string(),
                    epoch_mismatch: false,
                }))
            }
        }
    }

    async fn upload_multipart_part_replica(
        &self,
        request: Request<tonic::Streaming<proto::UploadMultipartPartReplicaChunk>>,
    ) -> Result<Response<proto::UploadMultipartPartReplicaResponse>, Status> {
        let Some(handler) = &self.multipart_handler else {
            return Err(Status::unimplemented(
                "UploadMultipartPartReplica requires a multipart handler",
            ));
        };

        match handler.handle_upload_part(request.into_inner()).await {
            Ok(response) => Ok(Response::new(response)),
            Err(e) => {
                error!(error = %e, "replica multipart part failed");
                Ok(Response::new(proto::UploadMultipartPartReplicaResponse {
                    success: false,
                    error: e.to_string(),
                    etag: String::new(),
                    part_record: Vec::new(),
                    epoch_mismatch: false,
                }))
            }
        }
    }

    async fn inspect_multipart_upload_replica(
        &self,
        request: Request<proto::InspectMultipartUploadReplicaRequest>,
    ) -> Result<Response<proto::InspectMultipartUploadReplicaResponse>, Status> {
        let Some(handler) = &self.multipart_handler else {
            return Err(Status::unimplemented(
                "InspectMultipartUploadReplica requires a multipart handler",
            ));
        };

        match handler.handle_inspect(request.get_ref()).await {
            Ok(response) => Ok(Response::new(response)),
            Err(e) => {
                error!(error = %e, "replica multipart inspect failed");
                Ok(Response::new(
                    proto::InspectMultipartUploadReplicaResponse {
                        success: false,
                        error: e.to_string(),
                        session: Vec::new(),
                        part_records: Vec::new(),
                        epoch_mismatch: false,
                    },
                ))
            }
        }
    }

    async fn complete_multipart_upload_replica(
        &self,
        request: Request<proto::CompleteMultipartUploadReplicaRequest>,
    ) -> Result<Response<proto::CompleteMultipartUploadReplicaResponse>, Status> {
        let Some(handler) = &self.multipart_handler else {
            return Err(Status::unimplemented(
                "CompleteMultipartUploadReplica requires a multipart handler",
            ));
        };

        match handler.handle_complete(request.get_ref()).await {
            Ok(response) => Ok(Response::new(response)),
            Err(e) => {
                error!(error = %e, "replica multipart complete failed");
                Ok(Response::new(
                    proto::CompleteMultipartUploadReplicaResponse {
                        success: false,
                        error: e.to_string(),
                        index_record: Vec::new(),
                        epoch_mismatch: false,
                    },
                ))
            }
        }
    }

    async fn abort_multipart_upload_replica(
        &self,
        request: Request<proto::AbortMultipartUploadReplicaRequest>,
    ) -> Result<Response<proto::AbortMultipartUploadReplicaResponse>, Status> {
        let Some(handler) = &self.multipart_handler else {
            return Err(Status::unimplemented(
                "AbortMultipartUploadReplica requires a multipart handler",
            ));
        };

        match handler.handle_abort(request.get_ref()).await {
            Ok(response) => Ok(Response::new(response)),
            Err(e) => {
                error!(error = %e, "replica multipart abort failed");
                Ok(Response::new(proto::AbortMultipartUploadReplicaResponse {
                    success: false,
                    error: e.to_string(),
                    epoch_mismatch: false,
                }))
            }
        }
    }

    // ========================================================================
    // Streaming data plane
    //
    // A body that does not fit one message travels in chunks. Both directions
    // end at the same handlers the unary methods use, so an object is written
    // and read the same way whichever transport carried it.
    // ========================================================================

    /// Accept a blob the coordinator sends as a stream of chunks.
    ///
    /// The object body is carried in chunks rather than in one message, so an
    /// object larger than the unary message limit can still be replicated. The
    /// stream is reassembled here and handed to the very same
    /// [`ReplicaWriteHandler::handle_write`] the unary path uses: the epoch
    /// check, the idempotency cache, the HLC stamping and the durable ACK
    /// boundary must not have a second implementation that can drift from it.
    async fn write_blob_stream(
        &self,
        request: Request<tonic::Streaming<proto::WriteBlobChunk>>,
    ) -> Result<Response<proto::WriteBlobResponse>, Status> {
        let Some(handler) = &self.replica_handler else {
            return Err(Status::unimplemented(
                "WriteBlobStream requires a storage engine (not configured)",
            ));
        };

        let mut stream = request.into_inner();
        let mut blob = StreamedBlob::default();
        while let Some(chunk) = stream.message().await? {
            blob.push(chunk)?;
        }
        let assembled = blob.finish()?;

        match handler.handle_write(&assembled).await {
            Ok(response) => Ok(Response::new(response)),
            Err(e) => {
                error!(error = %e, "streamed replica write failed");
                Ok(Response::new(proto::WriteBlobResponse {
                    success: false,
                    error: e.to_string(),
                    local_sequence: 0,
                    epoch_mismatch: false,
                }))
            }
        }
    }

    type ReadBlobStreamStream =
        tokio_stream::wrappers::ReceiverStream<Result<proto::ReadBlobChunk, Status>>;

    async fn read_blob_stream(
        &self,
        request: Request<proto::ReadBlobRequest>,
    ) -> Result<Response<Self::ReadBlobStreamStream>, Status> {
        let Some(handler) = &self.read_handler else {
            return Err(Status::unimplemented(
                "ReadBlobStream requires a storage engine (not configured)",
            ));
        };

        let object_stream = handler
            .open_read_stream(request.get_ref())
            .await
            .map_err(cluster_error_to_status)?;

        let record = object_stream.record.clone();
        let hlc = if record.hlc_timestamp > 0 {
            crate::hlc::HlcTimestamp {
                wall_time: record.hlc_timestamp,
                logical: record.hlc_logical,
                node_id: record.origin_node_id,
            }
        } else {
            crate::hlc::HlcTimestamp {
                wall_time: record.modified_at / 1_000_000,
                logical: 0,
                node_id: self.node_id.0,
            }
        };
        let header = proto::ReadBlobChunk {
            payload: Some(proto::read_blob_chunk::Payload::Header(
                proto::ReadBlobHeader {
                    content_hash: record.content_hash.to_vec(),
                    content_type: record.content_type,
                    metadata: record.metadata,
                    hlc: Some(hlc.to_proto()),
                    version_id: record.version_id,
                    content_length: object_stream.content_length,
                    total_size: object_stream.total_size,
                    range_start: object_stream.content_range.map(|(start, _, _)| start),
                    range_end: object_stream.content_range.map(|(_, end, _)| end),
                },
            )),
        };

        let (tx, rx) = tokio::sync::mpsc::channel(16);
        tokio::spawn(async move {
            if tx.send(Ok(header)).await.is_err() {
                return;
            }

            let mut reader = object_stream.body;
            let mut buf = vec![0u8; 1024 * 1024];
            loop {
                match reader.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(n) => {
                        let chunk = proto::ReadBlobChunk {
                            payload: Some(proto::read_blob_chunk::Payload::Data(buf[..n].to_vec())),
                        };
                        if tx.send(Ok(chunk)).await.is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        let _ = tx
                            .send(Err(Status::internal(format!("stream read failed: {e}"))))
                            .await;
                        break;
                    }
                }
            }
        });

        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(
            rx,
        )))
    }

    // ========================================================================
    // Metadata plane — Phase 4+
    // ========================================================================

    async fn write_metadata(
        &self,
        _request: Request<proto::WriteMetadataRequest>,
    ) -> Result<Response<proto::WriteMetadataResponse>, Status> {
        Err(Status::unimplemented(
            "WriteMetadata will be available after Phase 4",
        ))
    }

    async fn read_metadata(
        &self,
        _request: Request<proto::ReadMetadataRequest>,
    ) -> Result<Response<proto::ReadMetadataResponse>, Status> {
        // ReadMetadata is used for the anti-entropy system (Phase 7).
        // For Phase 5, HeadObject provides the metadata-only path.
        Err(Status::unimplemented(
            "ReadMetadata will be available after Phase 7 (anti-entropy)",
        ))
    }

    async fn head_object(
        &self,
        request: Request<proto::HeadObjectRequest>,
    ) -> Result<Response<proto::HeadObjectResponse>, Status> {
        let Some(handler) = &self.read_handler else {
            return Err(Status::unimplemented(
                "HeadObject requires a storage engine (not configured)",
            ));
        };

        match handler.handle_head(request.get_ref()).await {
            Ok(response) => Ok(Response::new(response)),
            Err(e) => {
                error!(error = %e, "replica head failed");
                Ok(Response::new(proto::HeadObjectResponse {
                    exists: false,
                    error: e.to_string(),
                    content_type: String::new(),
                    content_length: 0,
                    erasure_coded: false,
                    metadata: Default::default(),
                    hlc: None,
                    version_id: None,
                    etag: String::new(),
                    epoch_mismatch: false,
                    content_hash: Vec::new(),
                }))
            }
        }
    }

    async fn list_objects(
        &self,
        request: Request<proto::ListObjectsRequest>,
    ) -> Result<Response<proto::ListObjectsResponse>, Status> {
        let Some(handler) = &self.read_handler else {
            return Err(Status::unimplemented(
                "ListObjects requires a storage engine (not configured)",
            ));
        };

        match handler.handle_list(request.get_ref()).await {
            Ok(response) => Ok(Response::new(response)),
            Err(e) => {
                error!(error = %e, "replica list failed");
                Ok(Response::new(proto::ListObjectsResponse {
                    objects: Vec::new(),
                    common_prefixes: Vec::new(),
                    is_truncated: false,
                    next_continuation_token: String::new(),
                    error: e.to_string(),
                    epoch_mismatch: false,
                }))
            }
        }
    }

    async fn read_object_heat(
        &self,
        request: Request<proto::ReadObjectHeatRequest>,
    ) -> Result<Response<proto::ReadObjectHeatResponse>, Status> {
        let Some(query) = &self.heat_query else {
            return Err(Status::unimplemented(
                "ReadObjectHeat requires an Enterprise erasure-coded pool (not configured)",
            ));
        };

        let request = request.into_inner();
        let version_id = (!request.version_id.is_empty()).then_some(request.version_id.as_str());
        match query
            .local_object_heat(
                &request.bucket,
                &request.key,
                version_id,
                request.window_nanos,
            )
            .await
        {
            Ok(Some(mark)) => Ok(Response::new(proto::ReadObjectHeatResponse {
                success: true,
                error: String::new(),
                found: true,
                last_access_nanos: mark.last_access_nanos,
                window_access_count: mark.window_access_count,
            })),
            Ok(None) => Ok(Response::new(proto::ReadObjectHeatResponse {
                success: true,
                error: String::new(),
                found: false,
                last_access_nanos: 0,
                window_access_count: 0,
            })),
            Err(error) => {
                error!(error = %error, "replica object heat read failed");
                Ok(Response::new(proto::ReadObjectHeatResponse {
                    success: false,
                    error: error.to_string(),
                    found: false,
                    last_access_nanos: 0,
                    window_access_count: 0,
                }))
            }
        }
    }

    async fn scan_ec_content_digests(
        &self,
        request: Request<proto::ScanEcContentDigestsRequest>,
    ) -> Result<Response<proto::ScanEcContentDigestsResponse>, Status> {
        let Some(scan) = &self.content_digest_scan else {
            return Err(Status::unimplemented(
                "ScanEcContentDigests requires an Enterprise erasure-coded pool (not configured)",
            ));
        };

        let request = request.into_inner();
        let max_entries = request.max_entries as usize;
        match scan.local_content_digests(max_entries).await {
            Ok(page) => Ok(Response::new(proto::ScanEcContentDigestsResponse {
                success: true,
                error: String::new(),
                digests: page.digests.into_iter().map(content_digest_to_proto).collect(),
                scanned: u32::try_from(page.scanned).unwrap_or(u32::MAX),
                truncated: page.truncated,
            })),
            Err(error) => {
                error!(error = %error, "local EC content digest scan failed");
                Ok(Response::new(proto::ScanEcContentDigestsResponse {
                    success: false,
                    error: error.to_string(),
                    digests: Vec::new(),
                    scanned: 0,
                    truncated: false,
                }))
            }
        }
    }

    async fn probe_ec_tombstones(
        &self,
        request: Request<proto::ProbeEcTombstonesRequest>,
    ) -> Result<Response<proto::ProbeEcTombstonesResponse>, Status> {
        let Some(probe_service) = &self.tombstone_probe else {
            return Err(Status::unimplemented(
                "ProbeEcTombstones requires an Enterprise erasure-coded pool (not configured)",
            ));
        };

        let request = request.into_inner();
        let mut probes = Vec::with_capacity(request.probes.len());
        for probe in request.probes {
            let operation_id = probe
                .operation_id
                .as_slice()
                .try_into()
                .map(u128::from_be_bytes)
                .map_err(|_| Status::invalid_argument("operation_id must be 16 bytes"))?;
            let hlc_node_id = probe
                .hlc_node_id
                .as_slice()
                .try_into()
                .map(u128::from_be_bytes)
                .map_err(|_| Status::invalid_argument("hlc_node_id must be 16 bytes"))?;
            probes.push(EcTombstoneProbe {
                bucket: probe.bucket,
                key: probe.key,
                version_id: Some(probe.version_id).filter(|id| !id.is_empty()),
                operation_id,
                hlc: crate::hlc::HlcTimestamp {
                    wall_time: probe.hlc_wall_time,
                    logical: probe.hlc_logical,
                    node_id: hlc_node_id,
                },
            });
        }

        let asked = probes.len();
        match probe_service.holds_tombstones(probes).await {
            Ok(held) => Ok(Response::new(proto::ProbeEcTombstonesResponse {
                success: true,
                error: String::new(),
                held,
            })),
            Err(error) => {
                error!(error = %error, "EC tombstone probe failed");
                Ok(Response::new(proto::ProbeEcTombstonesResponse {
                    success: false,
                    error: error.to_string(),
                    // A node that could not answer has not been shown to hold
                    // anything, which is what the gate asks.
                    held: vec![false; asked],
                }))
            }
        }
    }

    // ========================================================================
    // Repair / Anti-entropy — Phase 7
    // ========================================================================

    async fn exchange_merkle_digest(
        &self,
        request: Request<proto::MerkleDigestRequest>,
    ) -> Result<Response<proto::MerkleDigestResponse>, Status> {
        let Some(ae) = &self.anti_entropy else {
            return Err(Status::unimplemented(
                "ExchangeMerkleDigest requires anti-entropy service (not configured)",
            ));
        };

        let response = ae.handle_merkle_exchange(request.get_ref()).await;
        Ok(Response::new(response))
    }

    type RepairSyncStream =
        tokio_stream::wrappers::ReceiverStream<Result<proto::RepairSyncResponse, Status>>;

    async fn repair_sync(
        &self,
        request: Request<proto::RepairSyncRequest>,
    ) -> Result<Response<Self::RepairSyncStream>, Status> {
        let Some(handler) = &self.read_handler else {
            return Err(Status::unimplemented(
                "RepairSync requires a storage engine (not configured)",
            ));
        };

        let req = request.into_inner();
        let (tx, rx) = tokio::sync::mpsc::channel(64);
        let read_handler = handler.clone();

        tokio::spawn(async move {
            for key in req.keys {
                // Parse "bucket/key" or just "key" for range-based repair.
                let (bucket, obj_key) = match key.split_once('/') {
                    Some((b, k)) => (b.to_string(), k.to_string()),
                    None => (req.bucket.clone(), key.clone()),
                };

                let read_req = proto::ReadBlobRequest {
                    bucket: bucket.clone(),
                    key: obj_key.clone(),
                    version_id: None,
                    coordinator_id: Vec::new(),
                    operation_id: Vec::new(),
                    topology_epoch: req.topology_epoch,
                    range_start: None,
                    range_end: None,
                };

                match read_handler.handle_read(&read_req).await {
                    Ok(resp) if resp.success => {
                        let _ = tx
                            .send(Ok(proto::RepairSyncResponse {
                                key: format!("{bucket}/{obj_key}"),
                                data: resp.data,
                                record: Vec::new(),
                                hlc: resp.hlc,
                                version_id: resp.version_id,
                                content_hash: resp.content_hash,
                                content_type: resp.content_type,
                                metadata: resp.metadata,
                            }))
                            .await;
                    }
                    Ok(_) | Err(_) => {
                        // Key not found or error — skip it.
                    }
                }
            }
        });

        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(
            rx,
        )))
    }

    // ========================================================================
    // Snapshot streaming — Phase 7+
    // ========================================================================

    type StreamSnapshotStream =
        tokio_stream::wrappers::ReceiverStream<Result<proto::SnapshotChunk, Status>>;

    async fn stream_snapshot(
        &self,
        _request: Request<proto::SnapshotRequest>,
    ) -> Result<Response<Self::StreamSnapshotStream>, Status> {
        Err(Status::unimplemented(
            "StreamSnapshot will be available after Phase 7",
        ))
    }

    // ========================================================================
    // Hinted handoff — Phase 4 (delivery via DeliverHint)
    // ========================================================================

    async fn deliver_hint(
        &self,
        request: Request<proto::HintRequest>,
    ) -> Result<Response<proto::HintResponse>, Status> {
        let Some(handler) = &self.replica_handler else {
            return Err(Status::unimplemented(
                "DeliverHint requires a storage engine (not configured)",
            ));
        };

        let hint_req = request.get_ref();

        // Convert hint to the original data-plane request and apply it.
        if hint_req.data.is_empty() && hint_req.content_type.is_empty() {
            let delete_req = delete_request_from_hint(hint_req);
            return match handler.handle_delete(&delete_req).await {
                Ok(resp) if resp.success => Ok(Response::new(proto::HintResponse {
                    accepted: true,
                    error: String::new(),
                })),
                Ok(resp) => Ok(Response::new(proto::HintResponse {
                    accepted: false,
                    error: resp.error,
                })),
                Err(e) => Ok(Response::new(proto::HintResponse {
                    accepted: false,
                    error: e.to_string(),
                })),
            };
        }

        // Reconstruct a WriteBlobRequest from the hint
        let write_req = proto::WriteBlobRequest {
            bucket: hint_req.bucket.clone(),
            key: hint_req.key.clone(),
            data: hint_req.data.clone(),
            content_hash: hint_req.content_hash.clone(),
            content_type: hint_req.content_type.clone(),
            metadata: hint_req.metadata.clone(),
            hlc: hint_req.hlc.clone(),
            version_id: hint_req.version_id.clone(),
            coordinator_id: Vec::new(),
            operation_id: hint_req.operation_id.clone(),
            topology_epoch: 0, // Hints bypass epoch check
        };

        match handler.handle_write(&write_req).await {
            Ok(resp) if resp.success => Ok(Response::new(proto::HintResponse {
                accepted: true,
                error: String::new(),
            })),
            Ok(resp) => Ok(Response::new(proto::HintResponse {
                accepted: false,
                error: resp.error,
            })),
            Err(e) => Ok(Response::new(proto::HintResponse {
                accepted: false,
                error: e.to_string(),
            })),
        }
    }
}

/// Reassemble a streamed blob into the request the unary write path takes.
///
/// The stream is a header followed by the body in chunks, so the shape is
/// checked before any of it is trusted: the header comes first and only once,
/// and the body is accepted only up to the length the header declared. A
/// stream that sends more bytes than it announced, or ends before it delivers
/// them, is refused rather than written — a short write would otherwise be
/// acknowledged as durable.
/// What was wrong with a stream of blob chunks.
///
/// Every one of these is the sender's fault, so the boundary reports them all
/// as `InvalidArgument`. They are named rather than formatted at the point of
/// failure so that a test can state which fault it expects.
#[derive(Debug, PartialEq, Eq)]
enum StreamedBlobError {
    /// The stream ended without ever sending a header.
    MissingHeader,
    /// A second header arrived; one stream carries one object.
    DuplicateHeader,
    /// Body arrived before the header that describes it.
    BodyBeforeHeader,
    /// A chunk was neither header nor body.
    EmptyPayload,
    /// More body arrived than the header announced.
    TooMuchBody { received: u64, announced: u64 },
    /// The stream ended before the announced body had arrived.
    TooLittleBody { received: u64, announced: u64 },
}

impl std::fmt::Display for StreamedBlobError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingHeader => write!(formatter, "streamed blob carries no header"),
            Self::DuplicateHeader => write!(formatter, "streamed blob carries a second header"),
            Self::BodyBeforeHeader => {
                write!(formatter, "streamed blob sent body before its header")
            }
            Self::EmptyPayload => {
                write!(formatter, "streamed blob carries a chunk with no payload")
            }
            Self::TooMuchBody {
                received,
                announced,
            } => write!(
                formatter,
                "streamed blob sent {received} byte(s) but announced {announced}"
            ),
            Self::TooLittleBody {
                received,
                announced,
            } => write!(
                formatter,
                "streamed blob ended after {received} byte(s) but announced {announced}"
            ),
        }
    }
}

impl From<StreamedBlobError> for Status {
    fn from(error: StreamedBlobError) -> Self {
        Status::invalid_argument(error.to_string())
    }
}

/// A blob being rebuilt from the chunks of one `WriteBlobStream` call.
///
/// Holds the header until the body has arrived in full, so that nothing is
/// written on a stream that turns out to be malformed.
#[derive(Default)]
struct StreamedBlob {
    header: Option<proto::WriteBlobHeader>,
    data: Vec<u8>,
}

impl StreamedBlob {
    /// Take the next chunk of the stream.
    fn push(&mut self, chunk: proto::WriteBlobChunk) -> Result<(), StreamedBlobError> {
        use proto::write_blob_chunk::Payload;

        match chunk.payload {
            Some(Payload::Header(incoming)) => {
                if self.header.is_some() {
                    return Err(StreamedBlobError::DuplicateHeader);
                }
                // The declared length decides how much body is accepted, so it
                // is never used as an allocation size on its own: a peer could
                // otherwise announce any number it liked and make this node
                // reserve it. The buffer starts at what a single unary message
                // could have carried and grows with the bytes that arrive.
                let reserve = usize::try_from(incoming.total_size)
                    .unwrap_or(usize::MAX)
                    .min(DEFAULT_MAX_MESSAGE_SIZE);
                self.data.reserve(reserve);
                self.header = Some(incoming);
                Ok(())
            }
            Some(Payload::Data(bytes)) => {
                let Some(header) = self.header.as_ref() else {
                    return Err(StreamedBlobError::BodyBeforeHeader);
                };
                let received = self.data.len().saturating_add(bytes.len()) as u64;
                if received > header.total_size {
                    return Err(StreamedBlobError::TooMuchBody {
                        received,
                        announced: header.total_size,
                    });
                }
                self.data.extend_from_slice(&bytes);
                Ok(())
            }
            None => Err(StreamedBlobError::EmptyPayload),
        }
    }

    /// The request the unary write path takes, once the stream has ended.
    fn finish(self) -> Result<proto::WriteBlobRequest, StreamedBlobError> {
        let Some(header) = self.header else {
            return Err(StreamedBlobError::MissingHeader);
        };
        if self.data.len() as u64 != header.total_size {
            return Err(StreamedBlobError::TooLittleBody {
                received: self.data.len() as u64,
                announced: header.total_size,
            });
        }

        Ok(proto::WriteBlobRequest {
            bucket: header.bucket,
            key: header.key,
            data: self.data,
            content_hash: header.content_hash,
            content_type: header.content_type,
            metadata: header.metadata,
            hlc: header.hlc,
            version_id: header.version_id,
            coordinator_id: header.coordinator_id,
            operation_id: header.operation_id,
            topology_epoch: header.topology_epoch,
        })
    }
}

fn content_digest_to_proto(digest: EcContentDigest) -> proto::EcContentDigest {
    proto::EcContentDigest {
        bucket: digest.bucket,
        key: digest.key,
        version_id: digest.version_id.unwrap_or_default(),
        content_hash_sha256: digest.content_hash_sha256.to_vec(),
        original_size: digest.original_size,
        erasure_set_id: digest.erasure_set_id,
        packed: digest.packed,
    }
}

fn cluster_error_to_status(error: ClusterError) -> Status {
    match error {
        ClusterError::ObjectNotFound { .. } => Status::not_found(error.to_string()),
        ClusterError::EpochMismatch { .. } => Status::failed_precondition(error.to_string()),
        ClusterError::QuorumNotMet { .. } => Status::unavailable(error.to_string()),
        ClusterError::ReadTimeout(_) | ClusterError::WriteTimeout(_) => {
            Status::deadline_exceeded(error.to_string())
        }
        ClusterError::Storage(message) if is_storage_integrity_error(&message) => {
            Status::data_loss(format!("storage error: {message}"))
        }
        _ => Status::internal(error.to_string()),
    }
}

fn is_storage_integrity_error(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    message.contains("checksum")
        || message.contains("crc")
        || message.contains("corrupt")
        || message.contains("integrity")
}

/// Handle for controlling the running gRPC server.
pub struct RpcServerHandle {
    shutdown_tx: watch::Sender<bool>,
    join_handle: tokio::task::JoinHandle<Result<(), ClusterError>>,
    shutdown_timeout: Duration,
}

impl RpcServerHandle {
    /// Trigger graceful shutdown.
    ///
    /// Signals the server to stop accepting new requests, waits for in-flight
    /// requests to complete (up to the configured timeout), then stops.
    pub async fn shutdown(self) -> Result<(), ClusterError> {
        info!("initiating gRPC server shutdown");
        let _ = self.shutdown_tx.send(true);

        match tokio::time::timeout(self.shutdown_timeout, self.join_handle).await {
            Ok(Ok(result)) => result,
            Ok(Err(e)) => {
                error!("gRPC server task panicked: {e}");
                Err(ClusterError::Rpc("server task panicked".into()))
            }
            Err(_) => {
                error!(
                    timeout_secs = self.shutdown_timeout.as_secs(),
                    "gRPC server shutdown timed out"
                );
                Err(ClusterError::Rpc("shutdown timed out".into()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::{NodeAddr, NodeCapacity, PoolId};
    use crate::rpc::proto::s4_node_client::S4NodeClient;
    use std::collections::HashMap;
    use std::net::SocketAddr;

    fn test_node_meta(node_id: NodeId) -> NodeMeta {
        NodeMeta {
            node_id,
            node_name: "test-node".into(),
            addr: NodeAddr {
                grpc_addr: "127.0.0.1:19100".parse().unwrap(),
                http_addr: "127.0.0.1:19000".parse().unwrap(),
                gossip_addr: "127.0.0.1:19200".parse().unwrap(),
            },
            pool_id: PoolId::new(1),
            topology_epoch: 1,
            protocol_version: crate::protocol::PROTOCOL_VERSION,
            capacity: NodeCapacity {
                total_bytes: 1_000_000_000,
                used_bytes: 100_000_000,
                volume_count: 5,
                object_count: 1000,
            },
            started_at: 1700000000,
            s4_version: "0.1.0".into(),
            edition: crate::identity::Edition::Community,
        }
    }

    fn test_gossip_handle() -> GossipHandle {
        GossipHandle::new_for_testing(HashMap::new())
    }

    #[test]
    fn delete_hint_replay_request_preserves_generation_guard() {
        let hlc = proto::HybridTimestamp {
            wall_time: 100,
            logical: 1,
            node_id: vec![1; 16],
        };
        let expected_source_hlc = proto::HybridTimestamp {
            wall_time: 90,
            logical: 3,
            node_id: vec![2; 16],
        };
        let operation_id = uuid::Uuid::new_v4().as_u128().to_be_bytes().to_vec();
        let hint = proto::HintRequest {
            target_node_id: vec![3; 16],
            bucket: "bucket".to_string(),
            key: "key".to_string(),
            data: Vec::new(),
            content_hash: Vec::new(),
            hlc: Some(hlc.clone()),
            operation_id: operation_id.clone(),
            version_id: Some("version".to_string()),
            metadata: HashMap::new(),
            content_type: String::new(),
            expected_source_hlc: Some(expected_source_hlc.clone()),
        };

        let request = delete_request_from_hint(&hint);

        assert_eq!(request.bucket, "bucket");
        assert_eq!(request.key, "key");
        assert_eq!(request.hlc, Some(hlc));
        assert_eq!(request.operation_id, operation_id);
        assert_eq!(request.topology_epoch, 0);
        assert_eq!(request.version_id, Some("version".to_string()));
        assert_eq!(request.expected_source_hlc, Some(expected_source_hlc));
    }

    fn blob_header(total_size: u64) -> proto::WriteBlobChunk {
        proto::WriteBlobChunk {
            payload: Some(proto::write_blob_chunk::Payload::Header(
                proto::WriteBlobHeader {
                    bucket: "bucket".into(),
                    key: "key".into(),
                    content_hash: vec![9u8; 32],
                    content_type: "text/plain".into(),
                    metadata: HashMap::from([("a".to_string(), "b".to_string())]),
                    hlc: None,
                    version_id: Some("v7".into()),
                    coordinator_id: vec![3u8; 16],
                    operation_id: vec![4u8; 16],
                    topology_epoch: 11,
                    total_size,
                },
            )),
        }
    }

    fn blob_body(bytes: &[u8]) -> proto::WriteBlobChunk {
        proto::WriteBlobChunk {
            payload: Some(proto::write_blob_chunk::Payload::Data(bytes.to_vec())),
        }
    }

    fn assemble(
        chunks: Vec<proto::WriteBlobChunk>,
    ) -> Result<proto::WriteBlobRequest, StreamedBlobError> {
        let mut blob = StreamedBlob::default();
        for chunk in chunks {
            blob.push(chunk)?;
        }
        blob.finish()
    }

    /// The body is put back together in order, and every field of the header
    /// reaches the request the durable write path takes.
    #[test]
    fn a_well_formed_stream_rebuilds_the_unary_request() {
        let request = assemble(vec![
            blob_header(6),
            blob_body(b"abc"),
            blob_body(b"de"),
            blob_body(b"f"),
        ])
        .expect("a well-formed stream is accepted");

        assert_eq!(request.data, b"abcdef");
        assert_eq!(request.bucket, "bucket");
        assert_eq!(request.key, "key");
        assert_eq!(request.content_hash, vec![9u8; 32]);
        assert_eq!(request.content_type, "text/plain");
        assert_eq!(
            request.metadata,
            HashMap::from([("a".to_string(), "b".to_string())])
        );
        assert_eq!(request.version_id, Some("v7".to_string()));
        assert_eq!(request.coordinator_id, vec![3u8; 16]);
        assert_eq!(request.operation_id, vec![4u8; 16]);
        assert_eq!(request.topology_epoch, 11);
    }

    /// A zero-length object is a header and nothing more.
    #[test]
    fn a_stream_of_no_body_is_an_empty_object() {
        let request = assemble(vec![blob_header(0)]).expect("an empty body is an object too");
        assert!(request.data.is_empty());
    }

    /// Every malformed shape is refused, and nothing reaches the write path.
    /// A short stream matters most: acknowledging it would report a truncated
    /// object as durable.
    #[test]
    fn a_malformed_stream_is_refused_rather_than_written() {
        for (name, chunks, expected) in [
            (
                "no header at all",
                vec![blob_body(b"abc")],
                StreamedBlobError::BodyBeforeHeader,
            ),
            (
                "body before the header",
                vec![blob_body(b"abc"), blob_header(3)],
                StreamedBlobError::BodyBeforeHeader,
            ),
            (
                "a second header",
                vec![blob_header(3), blob_header(3)],
                StreamedBlobError::DuplicateHeader,
            ),
            (
                "more body than announced",
                vec![blob_header(2), blob_body(b"abc")],
                StreamedBlobError::TooMuchBody {
                    received: 3,
                    announced: 2,
                },
            ),
            (
                "less body than announced",
                vec![blob_header(9), blob_body(b"abc")],
                StreamedBlobError::TooLittleBody {
                    received: 3,
                    announced: 9,
                },
            ),
            ("an empty stream", vec![], StreamedBlobError::MissingHeader),
            (
                "a chunk with no payload",
                vec![blob_header(3), proto::WriteBlobChunk { payload: None }],
                StreamedBlobError::EmptyPayload,
            ),
        ] {
            let error = assemble(chunks).expect_err(&format!("{name} must be refused"));
            assert_eq!(error, expected, "{name} must say what was wrong");
            assert_eq!(
                Status::from(error).code(),
                tonic::Code::InvalidArgument,
                "{name} is the sender's fault, not this node's"
            );
        }
    }

    /// The announced length must not become an allocation on its own: a peer
    /// claiming a body of gigabytes reserves nothing until bytes arrive, and
    /// the bytes it then sends are refused for exceeding what it announced.
    #[test]
    fn an_absurd_announced_length_reserves_nothing_and_is_refused() {
        let mut blob = StreamedBlob::default();
        blob.push(blob_header(u64::MAX)).expect("the header alone is not yet a fault");
        assert!(
            blob.data.capacity() <= DEFAULT_MAX_MESSAGE_SIZE,
            "the buffer must not grow to the announced length"
        );
        blob.push(blob_body(b"abc")).expect("a small body is within the announcement");
        assert_eq!(
            blob.finish().expect_err("a body shorter than announced is refused"),
            StreamedBlobError::TooLittleBody {
                received: 3,
                announced: u64::MAX,
            }
        );
    }

    #[tokio::test]
    async fn server_starts_and_stops() {
        let node_id = NodeId::generate();
        let meta = Arc::new(test_node_meta(node_id));
        let gossip = Arc::new(test_gossip_handle());

        let server = NodeRpcServer::new(node_id, meta, gossip);

        let addr: SocketAddr = "127.0.0.1:19101".parse().unwrap();
        let config = RpcServerConfig {
            bind_addr: addr,
            ..Default::default()
        };

        let handle = server.start(config).await.unwrap();
        handle.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn health_endpoint_responds() {
        let node_id = NodeId::generate();
        let meta = Arc::new(test_node_meta(node_id));
        let gossip = Arc::new(test_gossip_handle());

        let server = NodeRpcServer::new(node_id, meta.clone(), gossip);

        let addr: SocketAddr = "127.0.0.1:19102".parse().unwrap();
        let config = RpcServerConfig {
            bind_addr: addr,
            ..Default::default()
        };

        let handle = server.start(config).await.unwrap();

        // Connect and call Health.
        let mut client = S4NodeClient::connect(format!("http://{addr}")).await.unwrap();

        let response = client.health(proto::HealthRequest {}).await.unwrap().into_inner();

        assert_eq!(response.status, proto::NodeHealthStatus::Healthy as i32);
        assert_eq!(response.node_name, "test-node");
        assert_eq!(response.pool_id, 1);
        assert_eq!(response.protocol_version, crate::protocol::PROTOCOL_VERSION);
        assert_eq!(response.s4_version, "0.1.0");
        assert!(response.uptime_seconds < 10);

        let cap = response.capacity.unwrap();
        assert_eq!(cap.total_bytes, 1_000_000_000);
        assert_eq!(cap.used_bytes, 100_000_000);
        assert_eq!(cap.volume_count, 5);
        assert_eq!(cap.object_count, 1000);

        handle.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn write_blob_without_handler_returns_unimplemented() {
        let node_id = NodeId::generate();
        let meta = Arc::new(test_node_meta(node_id));
        let gossip = Arc::new(test_gossip_handle());

        let server = NodeRpcServer::new(node_id, meta, gossip);

        let addr: SocketAddr = "127.0.0.1:19103".parse().unwrap();
        let config = RpcServerConfig {
            bind_addr: addr,
            ..Default::default()
        };

        let handle = server.start(config).await.unwrap();

        let mut client = S4NodeClient::connect(format!("http://{addr}")).await.unwrap();

        // WriteBlob should be unimplemented without handler.
        let err = client.write_blob(proto::WriteBlobRequest::default()).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unimplemented);

        // So is the streaming form, and with the same code: that is what tells
        // a coordinator to fall back to a unary write rather than give up.
        let err = client
            .write_blob_stream(tokio_stream::iter(Vec::<proto::WriteBlobChunk>::new()))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unimplemented);

        // ReadBlob should still be unimplemented.
        let err = client.read_blob(proto::ReadBlobRequest::default()).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unimplemented);

        // DeleteBlob should be unimplemented without handler.
        let err = client.delete_blob(proto::DeleteBlobRequest::default()).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unimplemented);

        // HeadObject should be unimplemented.
        let err = client.head_object(proto::HeadObjectRequest::default()).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unimplemented);

        // DeliverHint should be unimplemented without handler.
        let err = client.deliver_hint(proto::HintRequest::default()).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unimplemented);

        handle.shutdown().await.unwrap();
    }
}
