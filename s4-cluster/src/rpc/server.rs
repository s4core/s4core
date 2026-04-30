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
    // Streaming data plane — Phase 4+
    // ========================================================================

    async fn write_blob_stream(
        &self,
        _request: Request<tonic::Streaming<proto::WriteBlobChunk>>,
    ) -> Result<Response<proto::WriteBlobResponse>, Status> {
        Err(Status::unimplemented(
            "WriteBlobStream will be available after Phase 4",
        ))
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
                    metadata: Default::default(),
                    hlc: None,
                    version_id: None,
                    etag: String::new(),
                    epoch_mismatch: false,
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

        // Convert hint to a write request and apply
        if hint_req.data.is_empty() && hint_req.content_type.is_empty() {
            // This is a delete hint — not enough context to replay without
            // a full DeleteBlobRequest. For now, accept and log.
            tracing::debug!(
                bucket = %hint_req.bucket,
                key = %hint_req.key,
                "received delete hint, accepted"
            );
            return Ok(Response::new(proto::HintResponse {
                accepted: true,
                error: String::new(),
            }));
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
            protocol_version: 1,
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
        assert_eq!(response.protocol_version, 1);
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
