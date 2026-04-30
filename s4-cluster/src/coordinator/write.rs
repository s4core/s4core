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

//! Quorum write coordinator.
//!
//! Fans out PUT/DELETE operations to all replicas in parallel, waits for
//! W=2 ACKs (configurable), and stores hints for unreachable replicas.
//!
//! # Single-Node Bypass
//!
//! When there is only one replica (the local node), the coordinator writes
//! directly to the local storage engine without any gRPC overhead.
//!
//! # Write Flow
//!
//! ```text
//! 1. Determine placement (pool + replicas)
//! 2. Generate operation_id (UUID v4) + HLC timestamp
//! 3. Fan out to ALL replicas in parallel:
//!    - Local replica: direct storage engine call
//!    - Remote replicas: gRPC WriteBlobRequest
//! 4. Wait for W responses (tokio::select on FuturesUnordered)
//! 5. If W met: return success + ETag
//! 6. If W not met after timeout: return 503
//! 7. For unreachable replicas: store hint for later delivery
//! ```

use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use s4_core::types::composite::{
    MultipartPartRecord, MultipartUploadSession, MultipartUploadState,
};
use s4_core::StorageEngine;
use sha2::{Digest, Sha256};
use tokio::time::timeout;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::{Stream, StreamExt};
use tokio_util::io::StreamReader;
use tracing::{debug, error, info, warn};

use crate::config::ClusterConfig;
use crate::error::ClusterError;
use crate::hints::{Hint, HintOperation, HintedHandoffManager};
use crate::hlc::{HlcTimestamp, HybridClock};
use crate::identity::NodeId;
use crate::placement::PlacementRouter;
use crate::replica::MultipartReplicaStorage;
use crate::rpc::proto;
use crate::rpc::NodeClient;

const MULTIPART_RPC_CHUNK_SIZE: usize = 1024 * 1024;
const MULTIPART_TRANSFER_TIMEOUT: Duration = Duration::from_secs(10 * 60);

/// Result of a quorum write operation.
#[derive(Debug, Clone)]
pub struct QuorumWriteResult {
    /// ETag of the written object (from the first successful replica).
    pub etag: String,
    /// Number of replicas that acknowledged the write.
    pub replicas_acked: u8,
    /// Total number of replicas targeted.
    pub replicas_total: u8,
    /// HLC timestamp assigned to this write.
    pub hlc: HlcTimestamp,
}

/// Result of a quorum delete operation.
#[derive(Debug, Clone)]
pub struct QuorumDeleteResult {
    /// Number of replicas that acknowledged the delete.
    pub replicas_acked: u8,
    /// Total number of replicas targeted.
    pub replicas_total: u8,
}

/// Result of a quorum multipart create operation.
#[derive(Debug, Clone)]
pub struct QuorumMultipartCreateResult {
    /// Number of replicas that acknowledged the operation.
    pub replicas_acked: u8,
    /// Total number of replicas targeted.
    pub replicas_total: u8,
}

/// Result of a quorum multipart upload-part operation.
#[derive(Debug, Clone)]
pub struct QuorumMultipartPartResult {
    /// ETag of the uploaded part.
    pub etag: String,
    /// Durable part record from one successful replica.
    pub part_record: MultipartPartRecord,
    /// Number of replicas that acknowledged the operation.
    pub replicas_acked: u8,
    /// Total number of replicas targeted.
    pub replicas_total: u8,
}

/// Result of a quorum multipart complete operation.
#[derive(Debug, Clone)]
pub struct QuorumMultipartCompleteResult {
    /// S3-compatible multipart ETag of the completed object.
    pub s3_etag: String,
    /// Total logical object size.
    pub total_size: u64,
    /// Number of replicas that acknowledged the operation.
    pub replicas_acked: u8,
    /// Total number of replicas targeted.
    pub replicas_total: u8,
    /// HLC timestamp assigned to the completed object.
    pub hlc: HlcTimestamp,
}

#[derive(Debug, Clone)]
struct MultipartCompletePlan {
    selected_parts: Vec<MultipartPartRecord>,
    s3_etag: String,
    total_size: u64,
    content_type: String,
    metadata: HashMap<String, String>,
}

#[derive(Debug, Clone)]
struct MultipartReplicaInspection {
    node_id: NodeId,
    session: MultipartUploadSession,
    parts: Vec<MultipartPartRecord>,
}

/// Timeout configuration for write operations.
#[derive(Debug, Clone)]
pub struct WriteTimeouts {
    /// Maximum time to wait for W replica ACKs.
    pub quorum_timeout: Duration,
    /// Maximum time for a single replica write.
    pub replica_write_timeout: Duration,
}

impl Default for WriteTimeouts {
    fn default() -> Self {
        Self {
            quorum_timeout: Duration::from_secs(60),
            replica_write_timeout: Duration::from_secs(60),
        }
    }
}

/// Quorum write coordinator.
///
/// Orchestrates writes across replicas, ensures quorum, and handles
/// hinted handoff for unreachable nodes.
pub struct QuorumWriteCoordinator {
    local_node_id: NodeId,
    storage: Arc<dyn StorageEngine>,
    multipart_storage: Option<Arc<dyn MultipartReplicaStorage>>,
    rpc_client: Arc<NodeClient>,
    router: Arc<PlacementRouter>,
    clock: Arc<HybridClock>,
    hints: Arc<HintedHandoffManager>,
    timeouts: WriteTimeouts,
    topology_epoch: u64,
    write_quorum: u8,
}

impl QuorumWriteCoordinator {
    /// Create a new quorum write coordinator.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        local_node_id: NodeId,
        storage: Arc<dyn StorageEngine>,
        rpc_client: Arc<NodeClient>,
        router: Arc<PlacementRouter>,
        clock: Arc<HybridClock>,
        hints: Arc<HintedHandoffManager>,
        cluster_config: &ClusterConfig,
        timeouts: WriteTimeouts,
    ) -> Self {
        Self {
            local_node_id,
            storage,
            multipart_storage: None,
            rpc_client,
            router,
            clock,
            hints,
            timeouts,
            topology_epoch: cluster_config.epoch,
            write_quorum: cluster_config.default_write_quorum,
        }
    }

    /// Attach native multipart storage for cluster-mode multipart quorum writes.
    pub fn with_multipart_storage(mut self, storage: Arc<dyn MultipartReplicaStorage>) -> Self {
        self.multipart_storage = Some(storage);
        self
    }

    /// Execute a quorum write (PUT object).
    ///
    /// Fans out to all replicas, waits for W ACKs, stores hints for
    /// unreachable replicas.
    pub async fn write(
        &self,
        bucket: &str,
        key: &str,
        data: &[u8],
        content_type: &str,
        metadata: &HashMap<String, String>,
    ) -> Result<QuorumWriteResult, ClusterError> {
        let decision = self.router.route(bucket, key)?;
        let replicas = &decision.replicas;

        // Single-node bypass: write directly, no quorum needed
        if replicas.len() == 1 && replicas[0] == self.local_node_id {
            return self.write_single_node(bucket, key, data, content_type, metadata).await;
        }

        let operation_id = uuid::Uuid::new_v4().as_u128();
        let hlc = self.clock.now();
        let content_hash = compute_sha256(data);

        let replicas_total = replicas.len() as u8;

        // Fan out to ALL replicas in parallel
        let mut handles = Vec::with_capacity(replicas.len());

        for &node_id in replicas {
            let handle = if node_id == self.local_node_id {
                // Local write: direct storage engine call
                let storage = self.storage.clone();
                let bucket = bucket.to_string();
                let key = key.to_string();
                let data = data.to_vec();
                let content_type = content_type.to_string();
                // Pass HLC via reserved metadata keys — BitcaskStorageEngine
                // extracts them during put_object and stamps the IndexRecord
                // atomically in the same batch (no race with concurrent writes).
                let mut metadata_with_hlc = metadata.clone();
                metadata_with_hlc.insert("_s4_hlc_wall".to_string(), hlc.wall_time.to_string());
                metadata_with_hlc.insert("_s4_hlc_logical".to_string(), hlc.logical.to_string());
                metadata_with_hlc.insert("_s4_hlc_node_id".to_string(), hlc.node_id.to_string());
                metadata_with_hlc.insert("_s4_operation_id".to_string(), operation_id.to_string());
                let timeout_dur = self.timeouts.replica_write_timeout;

                tokio::spawn(async move {
                    let result = timeout(
                        timeout_dur,
                        storage.put_object(&bucket, &key, &data, &content_type, &metadata_with_hlc),
                    )
                    .await;

                    match result {
                        Ok(Ok(etag)) => ReplicaResult::Success { node_id, etag },
                        Ok(Err(e)) => ReplicaResult::Failure {
                            node_id,
                            error: format!("local storage error: {e}"),
                        },
                        Err(_) => ReplicaResult::Failure {
                            node_id,
                            error: "local write timeout".into(),
                        },
                    }
                })
            } else {
                // Remote write: gRPC
                let client = self.rpc_client.clone();
                let request = proto::WriteBlobRequest {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                    data: data.to_vec(),
                    content_hash: content_hash.to_vec(),
                    content_type: content_type.to_string(),
                    metadata: metadata.clone(),
                    hlc: Some(hlc.to_proto()),
                    version_id: None,
                    coordinator_id: self.local_node_id.to_bytes().to_vec(),
                    operation_id: operation_id.to_be_bytes().to_vec(),
                    topology_epoch: self.topology_epoch,
                };
                let timeout_dur = self.timeouts.replica_write_timeout;

                tokio::spawn(async move {
                    let result = timeout(timeout_dur, client.send_write(node_id, request)).await;

                    match result {
                        Ok(Ok(resp)) if resp.success => ReplicaResult::Success {
                            node_id,
                            etag: String::new(),
                        },
                        Ok(Ok(resp)) if resp.epoch_mismatch => {
                            ReplicaResult::EpochMismatch { node_id }
                        }
                        Ok(Ok(resp)) => ReplicaResult::Failure {
                            node_id,
                            error: resp.error,
                        },
                        Ok(Err(e)) => ReplicaResult::Failure {
                            node_id,
                            error: e.to_string(),
                        },
                        Err(_) => ReplicaResult::Failure {
                            node_id,
                            error: "replica write timeout".into(),
                        },
                    }
                })
            };

            handles.push(handle);
        }

        // Wait for W responses with quorum timeout
        let results = self.collect_quorum_results(handles, self.write_quorum).await?;

        let acked: Vec<_> = results.iter().filter(|r| r.is_success()).collect();

        if (acked.len() as u8) < self.write_quorum {
            return Err(ClusterError::QuorumNotMet {
                needed: self.write_quorum,
                achieved: acked.len() as u8,
            });
        }

        // Extract ETag from the first successful response
        let etag = acked
            .iter()
            .find_map(|r| match r {
                ReplicaResult::Success { etag, .. } if !etag.is_empty() => Some(etag.clone()),
                _ => None,
            })
            .unwrap_or_default();

        // Store hints for every replica that did not ACK before the client
        // quorum response. Slow replicas may still finish successfully; replay
        // uses the same operation id and is idempotent.
        let hint_nodes = unacked_replica_nodes(replicas, &results, None);
        for node_id in &hint_nodes {
            let hint = Hint {
                target_node: node_id.0,
                operation: HintOperation::Write {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                    data: data.to_vec(),
                    content_hash: content_hash.to_vec(),
                    content_type: content_type.to_string(),
                    metadata: metadata.clone(),
                    operation_id: operation_id.to_be_bytes().to_vec(),
                    version_id: None,
                    topology_epoch: self.topology_epoch,
                },
                created_at: crate::hlc::now_millis(),
                ttl_ms: Duration::from_secs(3 * 3600).as_millis() as u64,
            };

            let hints = self.hints.clone();
            let node_id = *node_id;
            tokio::spawn(async move {
                if let Err(e) = hints.store_hint(hint).await {
                    error!(
                        target_node = %node_id,
                        error = %e,
                        "failed to store hint"
                    );
                }
            });
        }

        info!(
            bucket = %bucket,
            key = %key,
            acked = acked.len(),
            hinted = hint_nodes.len(),
            "quorum write completed"
        );

        Ok(QuorumWriteResult {
            etag,
            replicas_acked: acked.len() as u8,
            replicas_total,
            hlc,
        })
    }

    /// Execute a quorum delete (DELETE object).
    pub async fn delete(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<QuorumDeleteResult, ClusterError> {
        let decision = self.router.route(bucket, key)?;
        let replicas = &decision.replicas;

        // Single-node bypass
        if replicas.len() == 1 && replicas[0] == self.local_node_id {
            self.storage
                .delete_object(bucket, key)
                .await
                .map_err(|e| ClusterError::Storage(e.to_string()))?;
            return Ok(QuorumDeleteResult {
                replicas_acked: 1,
                replicas_total: 1,
            });
        }

        let operation_id = uuid::Uuid::new_v4().as_u128();
        let hlc = self.clock.now();
        let replicas_total = replicas.len() as u8;

        let mut handles = Vec::with_capacity(replicas.len());

        for &node_id in replicas {
            let handle = if node_id == self.local_node_id {
                let storage = self.storage.clone();
                let bucket = bucket.to_string();
                let key = key.to_string();
                let timeout_dur = self.timeouts.replica_write_timeout;

                tokio::spawn(async move {
                    let result = timeout(timeout_dur, storage.delete_object(&bucket, &key)).await;
                    match result {
                        Ok(Ok(())) => ReplicaResult::Success {
                            node_id,
                            etag: String::new(),
                        },
                        Ok(Err(e)) => ReplicaResult::Failure {
                            node_id,
                            error: format!("local delete error: {e}"),
                        },
                        Err(_) => ReplicaResult::Failure {
                            node_id,
                            error: "local delete timeout".into(),
                        },
                    }
                })
            } else {
                let client = self.rpc_client.clone();
                let request = proto::DeleteBlobRequest {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                    hlc: Some(hlc.to_proto()),
                    coordinator_id: self.local_node_id.to_bytes().to_vec(),
                    operation_id: operation_id.to_be_bytes().to_vec(),
                    topology_epoch: self.topology_epoch,
                    version_id: None,
                };
                let timeout_dur = self.timeouts.replica_write_timeout;

                tokio::spawn(async move {
                    let result = timeout(timeout_dur, client.send_delete(node_id, request)).await;
                    match result {
                        Ok(Ok(resp)) if resp.success => ReplicaResult::Success {
                            node_id,
                            etag: String::new(),
                        },
                        Ok(Ok(resp)) if resp.epoch_mismatch => {
                            ReplicaResult::EpochMismatch { node_id }
                        }
                        Ok(Ok(resp)) => ReplicaResult::Failure {
                            node_id,
                            error: resp.error,
                        },
                        Ok(Err(e)) => ReplicaResult::Failure {
                            node_id,
                            error: e.to_string(),
                        },
                        Err(_) => ReplicaResult::Failure {
                            node_id,
                            error: "replica delete timeout".into(),
                        },
                    }
                })
            };

            handles.push(handle);
        }

        let results = self.collect_quorum_results(handles, self.write_quorum).await?;

        let acked = results.iter().filter(|r| r.is_success()).count() as u8;

        if acked < self.write_quorum {
            return Err(ClusterError::QuorumNotMet {
                needed: self.write_quorum,
                achieved: acked,
            });
        }

        // Store hints for every replica that did not ACK before quorum.
        let hint_nodes = unacked_replica_nodes(replicas, &results, None);
        for node_id in &hint_nodes {
            let hint = Hint {
                target_node: node_id.0,
                operation: HintOperation::Delete {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                    operation_id: operation_id.to_be_bytes().to_vec(),
                    version_id: None,
                    topology_epoch: self.topology_epoch,
                },
                created_at: crate::hlc::now_millis(),
                ttl_ms: Duration::from_secs(3 * 3600).as_millis() as u64,
            };

            let hints = self.hints.clone();
            let node_id = *node_id;
            tokio::spawn(async move {
                if let Err(e) = hints.store_hint(hint).await {
                    error!(target_node = %node_id, error = %e, "failed to store delete hint");
                }
            });
        }

        Ok(QuorumDeleteResult {
            replicas_acked: acked,
            replicas_total,
        })
    }

    /// Create a native multipart upload session on a quorum of replicas.
    pub async fn create_multipart_upload(
        &self,
        upload_id: &str,
        bucket: &str,
        key: &str,
        content_type: &str,
        metadata: &HashMap<String, String>,
    ) -> Result<QuorumMultipartCreateResult, ClusterError> {
        let decision = self.router.route(bucket, key)?;
        let replicas = &decision.replicas;
        let operation_id = uuid::Uuid::new_v4().as_u128();
        let replicas_total = replicas.len() as u8;
        let mut handles = Vec::with_capacity(replicas.len());

        for &node_id in replicas {
            let handle = if node_id == self.local_node_id {
                let storage = self.multipart_storage.clone().ok_or_else(|| {
                    ClusterError::Config("multipart storage not configured".into())
                })?;
                let upload_id = upload_id.to_string();
                let bucket = bucket.to_string();
                let key = key.to_string();
                let content_type = content_type.to_string();
                let metadata = metadata.clone();
                let timeout_dur = self.timeouts.replica_write_timeout;

                tokio::spawn(async move {
                    let result = timeout(
                        timeout_dur,
                        storage.create_multipart_session(
                            &upload_id,
                            &bucket,
                            &key,
                            &content_type,
                            &metadata,
                        ),
                    )
                    .await;
                    match result {
                        Ok(Ok(())) => MultipartReplicaResult::Success {
                            node_id,
                            etag: String::new(),
                            part_record: None,
                        },
                        Ok(Err(e)) => MultipartReplicaResult::Failure {
                            node_id,
                            error: e.to_string(),
                        },
                        Err(_) => MultipartReplicaResult::Failure {
                            node_id,
                            error: "local multipart create timeout".into(),
                        },
                    }
                })
            } else {
                let client = self.rpc_client.clone();
                let request = proto::CreateMultipartUploadReplicaRequest {
                    upload_id: upload_id.to_string(),
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                    content_type: content_type.to_string(),
                    metadata: metadata.clone(),
                    coordinator_id: self.local_node_id.to_bytes().to_vec(),
                    operation_id: operation_id.to_be_bytes().to_vec(),
                    topology_epoch: self.topology_epoch,
                };
                let timeout_dur = self.timeouts.replica_write_timeout;

                tokio::spawn(async move {
                    let result =
                        timeout(timeout_dur, client.send_multipart_create(node_id, request)).await;
                    match result {
                        Ok(Ok(resp)) if resp.success => MultipartReplicaResult::Success {
                            node_id,
                            etag: String::new(),
                            part_record: None,
                        },
                        Ok(Ok(resp)) if resp.epoch_mismatch => {
                            MultipartReplicaResult::EpochMismatch { node_id }
                        }
                        Ok(Ok(resp)) => MultipartReplicaResult::Failure {
                            node_id,
                            error: resp.error,
                        },
                        Ok(Err(e)) => MultipartReplicaResult::Failure {
                            node_id,
                            error: e.to_string(),
                        },
                        Err(_) => MultipartReplicaResult::Failure {
                            node_id,
                            error: "replica multipart create timeout".into(),
                        },
                    }
                })
            };
            handles.push(handle);
        }

        let results = self
            .collect_multipart_results(handles, self.write_quorum, self.timeouts.quorum_timeout)
            .await?;
        let acked = results.iter().filter(|r| r.is_success()).count() as u8;
        if acked < self.write_quorum {
            return Err(ClusterError::QuorumNotMet {
                needed: self.write_quorum,
                achieved: acked,
            });
        }

        Ok(QuorumMultipartCreateResult {
            replicas_acked: acked,
            replicas_total,
        })
    }

    /// Upload one native multipart part to a quorum of replicas.
    pub async fn upload_multipart_part(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: u32,
        data: Vec<u8>,
    ) -> Result<QuorumMultipartPartResult, ClusterError> {
        let content_length = data.len() as u64;
        let stream = tokio_stream::iter(
            split_bytes_for_stream(Bytes::from(data))
                .into_iter()
                .map(Ok::<Bytes, io::Error>),
        );
        self.upload_multipart_part_streaming(
            bucket,
            key,
            upload_id,
            part_number,
            stream,
            content_length,
        )
        .await
    }

    /// Upload one native multipart part to a quorum of replicas from a stream.
    pub async fn upload_multipart_part_streaming<S>(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: u32,
        body_stream: S,
        content_length: u64,
    ) -> Result<QuorumMultipartPartResult, ClusterError>
    where
        S: Stream<Item = Result<Bytes, io::Error>> + Send + 'static,
    {
        let decision = self.router.route(bucket, key)?;
        let replicas = &decision.replicas;
        let operation_id = uuid::Uuid::new_v4().as_u128();
        let replicas_total = replicas.len() as u8;
        let mut handles = Vec::with_capacity(replicas.len());
        let mut local_senders = Vec::new();
        let mut remote_senders = Vec::new();
        let timeout_dur = self.multipart_transfer_timeout(content_length);

        for &node_id in replicas {
            let handle = if node_id == self.local_node_id {
                let storage = self.multipart_storage.clone().ok_or_else(|| {
                    ClusterError::Config("multipart storage not configured".into())
                })?;
                let upload_id = upload_id.to_string();
                let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, io::Error>>(16);
                local_senders.push(tx);

                tokio::spawn(async move {
                    let session =
                        timeout(timeout_dur, storage.get_multipart_session(&upload_id)).await;
                    match session {
                        Ok(Ok(_)) => {}
                        Ok(Err(_)) => {
                            return MultipartReplicaResult::Failure {
                                node_id,
                                error: "NoSuchUpload".to_string(),
                            };
                        }
                        Err(_) => {
                            return MultipartReplicaResult::Failure {
                                node_id,
                                error: "local multipart session lookup timeout".into(),
                            };
                        }
                    }

                    let reader = StreamReader::new(ReceiverStream::new(rx));
                    let result = timeout(
                        timeout_dur,
                        storage.upload_multipart_part_streaming(
                            &upload_id,
                            part_number,
                            Box::new(reader),
                            content_length,
                        ),
                    )
                    .await;
                    match result {
                        Ok(Ok(part)) => MultipartReplicaResult::Success {
                            node_id,
                            etag: part.etag_md5_hex.clone(),
                            part_record: Some(part),
                        },
                        Ok(Err(e)) => MultipartReplicaResult::Failure {
                            node_id,
                            error: e.to_string(),
                        },
                        Err(_) => MultipartReplicaResult::Failure {
                            node_id,
                            error: "local multipart part timeout".into(),
                        },
                    }
                })
            } else {
                let client = self.rpc_client.clone();
                let (tx, rx) =
                    tokio::sync::mpsc::channel::<proto::UploadMultipartPartReplicaChunk>(16);
                let header = proto::UploadMultipartPartReplicaChunk {
                    payload: Some(proto::upload_multipart_part_replica_chunk::Payload::Header(
                        proto::UploadMultipartPartReplicaHeader {
                            upload_id: upload_id.to_string(),
                            part_number,
                            coordinator_id: self.local_node_id.to_bytes().to_vec(),
                            operation_id: operation_id.to_be_bytes().to_vec(),
                            topology_epoch: self.topology_epoch,
                            total_size: content_length,
                        },
                    )),
                };
                if tx.send(header).await.is_err() {
                    return Err(ClusterError::Rpc(format!(
                        "failed to initialize multipart stream to {node_id}"
                    )));
                }
                remote_senders.push(tx);

                tokio::spawn(async move {
                    let result = timeout(
                        timeout_dur,
                        client.send_multipart_upload_part_stream(node_id, ReceiverStream::new(rx)),
                    )
                    .await;
                    match result {
                        Ok(Ok(resp)) if resp.success => {
                            let part_record = if resp.part_record.is_empty() {
                                None
                            } else {
                                match bincode::deserialize::<MultipartPartRecord>(&resp.part_record)
                                {
                                    Ok(part) => Some(part),
                                    Err(e) => {
                                        return MultipartReplicaResult::Failure {
                                            node_id,
                                            error: format!("part record decode error: {e}"),
                                        };
                                    }
                                }
                            };
                            MultipartReplicaResult::Success {
                                node_id,
                                etag: resp.etag,
                                part_record,
                            }
                        }
                        Ok(Ok(resp)) if resp.epoch_mismatch => {
                            MultipartReplicaResult::EpochMismatch { node_id }
                        }
                        Ok(Ok(resp)) => MultipartReplicaResult::Failure {
                            node_id,
                            error: resp.error,
                        },
                        Ok(Err(e)) => MultipartReplicaResult::Failure {
                            node_id,
                            error: e.to_string(),
                        },
                        Err(_) => MultipartReplicaResult::Failure {
                            node_id,
                            error: "replica multipart part timeout".into(),
                        },
                    }
                })
            };
            handles.push(handle);
        }

        tokio::pin!(body_stream);
        let mut pending = BytesMut::new();
        while let Some(chunk) = body_stream.next().await {
            let chunk = chunk?;
            if chunk.is_empty() {
                continue;
            }
            pending.extend_from_slice(&chunk);
            while pending.len() >= MULTIPART_RPC_CHUNK_SIZE {
                let piece = pending.split_to(MULTIPART_RPC_CHUNK_SIZE).freeze();
                fanout_multipart_piece(piece, &local_senders, &remote_senders).await;
            }
        }
        if !pending.is_empty() {
            fanout_multipart_piece(pending.freeze(), &local_senders, &remote_senders).await;
        }
        drop(local_senders);
        drop(remote_senders);

        let results =
            self.collect_multipart_results(handles, self.write_quorum, timeout_dur).await?;
        let acked: Vec<_> = results.iter().filter(|r| r.is_success()).collect();
        if (acked.len() as u8) < self.write_quorum {
            if results.iter().any(|r| r.is_no_such_upload()) {
                return Err(ClusterError::MultipartUploadNotFound {
                    upload_id: upload_id.to_string(),
                });
            }
            return Err(ClusterError::QuorumNotMet {
                needed: self.write_quorum,
                achieved: acked.len() as u8,
            });
        }

        let etag = acked
            .iter()
            .find_map(|r| r.etag().filter(|e| !e.is_empty()).map(str::to_string))
            .ok_or_else(|| ClusterError::Storage("multipart part succeeded without ETag".into()))?;

        let part_record = acked.iter().find_map(|r| r.part_record().cloned()).ok_or_else(|| {
            ClusterError::Storage("multipart part succeeded without part record".into())
        })?;

        Ok(QuorumMultipartPartResult {
            etag,
            part_record,
            replicas_acked: acked.len() as u8,
            replicas_total,
        })
    }

    async fn prepare_multipart_complete(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        manifest_parts: &[(u32, String)],
    ) -> Result<MultipartCompletePlan, ClusterError> {
        let inspections = self.inspect_multipart_upload(bucket, key, upload_id).await?;

        let mut valid_plans = Vec::new();
        let mut first_validation_error = None;
        for inspection in &inspections {
            if inspection.session.state != MultipartUploadState::Open {
                if first_validation_error.is_none() {
                    first_validation_error = Some(ClusterError::InvalidMultipartPart {
                        upload_id: upload_id.to_string(),
                        message: format!("upload is not open on {}", inspection.node_id),
                    });
                }
                continue;
            }

            match validate_cluster_manifest_parts(upload_id, manifest_parts, &inspection.parts) {
                Ok(selected_parts) => {
                    let s3_etag = compute_native_multipart_etag(&selected_parts);
                    let total_size = selected_parts.iter().map(|p| p.size).sum();
                    valid_plans.push(MultipartCompletePlan {
                        selected_parts,
                        s3_etag,
                        total_size,
                        content_type: inspection.session.content_type.clone(),
                        metadata: inspection.session.metadata.clone(),
                    });
                }
                Err(e) => {
                    if first_validation_error.is_none() {
                        first_validation_error = Some(e);
                    }
                }
            }
        }

        if (valid_plans.len() as u8) < self.write_quorum {
            if inspections.is_empty() {
                return Err(ClusterError::MultipartUploadNotFound {
                    upload_id: upload_id.to_string(),
                });
            }
            if valid_plans.is_empty() {
                return Err(first_validation_error.unwrap_or_else(|| {
                    ClusterError::InvalidMultipartPart {
                        upload_id: upload_id.to_string(),
                        message: "no replica has a valid manifest".to_string(),
                    }
                }));
            }
            return Err(ClusterError::QuorumNotMet {
                needed: self.write_quorum,
                achieved: valid_plans.len() as u8,
            });
        }

        Ok(valid_plans.remove(0))
    }

    async fn inspect_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<Vec<MultipartReplicaInspection>, ClusterError> {
        let decision = self.router.route(bucket, key)?;
        let replicas = &decision.replicas;
        let operation_id = uuid::Uuid::new_v4().as_u128();
        let mut handles = Vec::with_capacity(replicas.len());

        for &node_id in replicas {
            let handle = if node_id == self.local_node_id {
                let storage = self.multipart_storage.clone().ok_or_else(|| {
                    ClusterError::Config("multipart storage not configured".into())
                })?;
                let bucket = bucket.to_string();
                let key = key.to_string();
                let upload_id = upload_id.to_string();
                let timeout_dur = self.timeouts.replica_write_timeout;

                tokio::spawn(async move {
                    let session =
                        timeout(timeout_dur, storage.get_multipart_session(&upload_id)).await;
                    let session = match session {
                        Ok(Ok(session)) => session,
                        Ok(Err(_)) => {
                            return MultipartInspectResult::Failure {
                                node_id,
                                error: "NoSuchUpload".to_string(),
                            };
                        }
                        Err(_) => {
                            return MultipartInspectResult::Failure {
                                node_id,
                                error: "local multipart inspect session timeout".into(),
                            };
                        }
                    };

                    if session.bucket != bucket || session.key != key {
                        return MultipartInspectResult::Failure {
                            node_id,
                            error: "NoSuchUpload".to_string(),
                        };
                    }

                    let parts =
                        timeout(timeout_dur, storage.list_multipart_parts(&upload_id)).await;
                    match parts {
                        Ok(Ok(parts)) => {
                            MultipartInspectResult::Success(MultipartReplicaInspection {
                                node_id,
                                session,
                                parts,
                            })
                        }
                        Ok(Err(e)) => MultipartInspectResult::Failure {
                            node_id,
                            error: e.to_string(),
                        },
                        Err(_) => MultipartInspectResult::Failure {
                            node_id,
                            error: "local multipart inspect parts timeout".into(),
                        },
                    }
                })
            } else {
                let client = self.rpc_client.clone();
                let request = proto::InspectMultipartUploadReplicaRequest {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                    upload_id: upload_id.to_string(),
                    coordinator_id: self.local_node_id.to_bytes().to_vec(),
                    operation_id: operation_id.to_be_bytes().to_vec(),
                    topology_epoch: self.topology_epoch,
                };
                let timeout_dur = self.timeouts.replica_write_timeout;

                tokio::spawn(async move {
                    let result =
                        timeout(timeout_dur, client.send_multipart_inspect(node_id, request)).await;
                    match result {
                        Ok(Ok(resp)) if resp.success => {
                            let session =
                                match bincode::deserialize::<MultipartUploadSession>(&resp.session)
                                {
                                    Ok(session) => session,
                                    Err(e) => {
                                        return MultipartInspectResult::Failure {
                                            node_id,
                                            error: format!("session decode error: {e}"),
                                        };
                                    }
                                };
                            let mut parts = Vec::with_capacity(resp.part_records.len());
                            for raw in &resp.part_records {
                                match bincode::deserialize::<MultipartPartRecord>(raw) {
                                    Ok(part) => parts.push(part),
                                    Err(e) => {
                                        return MultipartInspectResult::Failure {
                                            node_id,
                                            error: format!("part record decode error: {e}"),
                                        };
                                    }
                                }
                            }
                            MultipartInspectResult::Success(MultipartReplicaInspection {
                                node_id,
                                session,
                                parts,
                            })
                        }
                        Ok(Ok(resp)) if resp.epoch_mismatch => {
                            MultipartInspectResult::EpochMismatch { node_id }
                        }
                        Ok(Ok(resp)) => MultipartInspectResult::Failure {
                            node_id,
                            error: resp.error,
                        },
                        Ok(Err(e)) => MultipartInspectResult::Failure {
                            node_id,
                            error: e.to_string(),
                        },
                        Err(_) => MultipartInspectResult::Failure {
                            node_id,
                            error: "replica multipart inspect timeout".into(),
                        },
                    }
                })
            };
            handles.push(handle);
        }

        let results = self.collect_multipart_inspections(handles, self.write_quorum).await?;
        let success_count = results.iter().filter(|result| result.is_success()).count() as u8;
        if success_count < self.write_quorum {
            if results.iter().any(|result| !result.is_success() && !result.is_no_such_upload()) {
                return Err(ClusterError::QuorumNotMet {
                    needed: self.write_quorum,
                    achieved: success_count,
                });
            }
            return Err(ClusterError::MultipartUploadNotFound {
                upload_id: upload_id.to_string(),
            });
        }

        let successes: Vec<_> = results
            .into_iter()
            .filter_map(|result| match result {
                MultipartInspectResult::Success(inspection) => Some(inspection),
                _ => None,
            })
            .collect();

        Ok(successes)
    }

    /// Complete a native multipart upload on a quorum of replicas.
    #[allow(clippy::too_many_arguments)]
    pub async fn complete_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        manifest_parts: &[(u32, String)],
    ) -> Result<QuorumMultipartCompleteResult, ClusterError> {
        let plan = self.prepare_multipart_complete(bucket, key, upload_id, manifest_parts).await?;
        let decision = self.router.route(bucket, key)?;
        let replicas = &decision.replicas;
        let operation_id = uuid::Uuid::new_v4().as_u128();
        let hlc = self.clock.now();
        let replicas_total = replicas.len() as u8;

        let selected_part_records: Vec<Vec<u8>> = plan
            .selected_parts
            .iter()
            .map(|p| bincode::serialize(p).map_err(|e| ClusterError::Codec(e.to_string())))
            .collect::<Result<_, _>>()?;

        let mut handles = Vec::with_capacity(replicas.len());

        for &node_id in replicas {
            let handle = if node_id == self.local_node_id {
                let storage = self.multipart_storage.clone().ok_or_else(|| {
                    ClusterError::Config("multipart storage not configured".into())
                })?;
                let bucket = bucket.to_string();
                let key = key.to_string();
                let upload_id = upload_id.to_string();
                let selected_parts = plan.selected_parts.clone();
                let s3_etag = plan.s3_etag.clone();
                let content_type = plan.content_type.clone();
                let mut metadata = plan.metadata.clone();
                metadata.insert("_s4_hlc_wall".to_string(), hlc.wall_time.to_string());
                metadata.insert("_s4_hlc_logical".to_string(), hlc.logical.to_string());
                metadata.insert("_s4_hlc_node_id".to_string(), hlc.node_id.to_string());
                metadata.insert("_s4_operation_id".to_string(), operation_id.to_string());
                let timeout_dur = self.timeouts.replica_write_timeout;

                tokio::spawn(async move {
                    let session =
                        timeout(timeout_dur, storage.mark_session_completing(&upload_id)).await;
                    let session = match session {
                        Ok(Ok(session)) => session,
                        Ok(Err(_)) => {
                            return MultipartReplicaResult::Failure {
                                node_id,
                                error: "NoSuchUpload".to_string(),
                            };
                        }
                        Err(_) => {
                            return MultipartReplicaResult::Failure {
                                node_id,
                                error: "local multipart complete mark timeout".into(),
                            };
                        }
                    };

                    let result = timeout(
                        timeout_dur,
                        storage.complete_multipart_native(
                            &bucket,
                            &key,
                            &upload_id,
                            &selected_parts,
                            &s3_etag,
                            &content_type,
                            &metadata,
                        ),
                    )
                    .await;

                    match result {
                        Ok(Ok(_record)) => MultipartReplicaResult::Success {
                            node_id,
                            etag: String::new(),
                            part_record: None,
                        },
                        Ok(Err(e)) => {
                            let _ = storage.revert_session_to_open(&session.upload_id).await;
                            MultipartReplicaResult::Failure {
                                node_id,
                                error: e.to_string(),
                            }
                        }
                        Err(_) => {
                            let _ = storage.revert_session_to_open(&session.upload_id).await;
                            MultipartReplicaResult::Failure {
                                node_id,
                                error: "local multipart complete timeout".into(),
                            }
                        }
                    }
                })
            } else {
                let client = self.rpc_client.clone();
                let request = proto::CompleteMultipartUploadReplicaRequest {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                    upload_id: upload_id.to_string(),
                    selected_part_records: selected_part_records.clone(),
                    s3_etag: plan.s3_etag.clone(),
                    content_type: plan.content_type.clone(),
                    metadata: plan.metadata.clone(),
                    hlc: Some(hlc.to_proto()),
                    coordinator_id: self.local_node_id.to_bytes().to_vec(),
                    operation_id: operation_id.to_be_bytes().to_vec(),
                    topology_epoch: self.topology_epoch,
                };
                let timeout_dur = self.timeouts.replica_write_timeout;

                tokio::spawn(async move {
                    let result = timeout(
                        timeout_dur,
                        client.send_multipart_complete(node_id, request),
                    )
                    .await;
                    match result {
                        Ok(Ok(resp)) if resp.success => MultipartReplicaResult::Success {
                            node_id,
                            etag: String::new(),
                            part_record: None,
                        },
                        Ok(Ok(resp)) if resp.epoch_mismatch => {
                            MultipartReplicaResult::EpochMismatch { node_id }
                        }
                        Ok(Ok(resp)) => MultipartReplicaResult::Failure {
                            node_id,
                            error: resp.error,
                        },
                        Ok(Err(e)) => MultipartReplicaResult::Failure {
                            node_id,
                            error: e.to_string(),
                        },
                        Err(_) => MultipartReplicaResult::Failure {
                            node_id,
                            error: "replica multipart complete timeout".into(),
                        },
                    }
                })
            };
            handles.push(handle);
        }

        let results = self
            .collect_multipart_results(handles, self.write_quorum, self.timeouts.quorum_timeout)
            .await?;
        let acked = results.iter().filter(|r| r.is_success()).count() as u8;
        if acked < self.write_quorum {
            if results.iter().any(|r| r.is_no_such_upload()) {
                return Err(ClusterError::MultipartUploadNotFound {
                    upload_id: upload_id.to_string(),
                });
            }
            return Err(ClusterError::QuorumNotMet {
                needed: self.write_quorum,
                achieved: acked,
            });
        }

        Ok(QuorumMultipartCompleteResult {
            s3_etag: plan.s3_etag,
            total_size: plan.total_size,
            replicas_acked: acked,
            replicas_total,
            hlc,
        })
    }

    /// Abort a native multipart upload on a quorum of replicas.
    pub async fn abort_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<QuorumMultipartCreateResult, ClusterError> {
        let decision = self.router.route(bucket, key)?;
        let replicas = &decision.replicas;
        let operation_id = uuid::Uuid::new_v4().as_u128();
        let replicas_total = replicas.len() as u8;
        let mut handles = Vec::with_capacity(replicas.len());

        for &node_id in replicas {
            let handle = if node_id == self.local_node_id {
                let storage = self.multipart_storage.clone().ok_or_else(|| {
                    ClusterError::Config("multipart storage not configured".into())
                })?;
                let upload_id = upload_id.to_string();
                let timeout_dur = self.timeouts.replica_write_timeout;

                tokio::spawn(async move {
                    let session =
                        timeout(timeout_dur, storage.get_multipart_session(&upload_id)).await;
                    match session {
                        Ok(Ok(session)) => {
                            if session.state
                                == s4_core::types::composite::MultipartUploadState::Completing
                            {
                                return MultipartReplicaResult::Failure {
                                    node_id,
                                    error: "Cannot abort upload while CompleteMultipartUpload is in progress"
                                        .to_string(),
                                };
                            }
                            match timeout(timeout_dur, storage.abort_multipart_native(&upload_id))
                                .await
                            {
                                Ok(Ok(())) => MultipartReplicaResult::Success {
                                    node_id,
                                    etag: String::new(),
                                    part_record: None,
                                },
                                Ok(Err(e)) => MultipartReplicaResult::Failure {
                                    node_id,
                                    error: e.to_string(),
                                },
                                Err(_) => MultipartReplicaResult::Failure {
                                    node_id,
                                    error: "local multipart abort timeout".into(),
                                },
                            }
                        }
                        Ok(Err(_)) => MultipartReplicaResult::Success {
                            node_id,
                            etag: String::new(),
                            part_record: None,
                        },
                        Err(_) => MultipartReplicaResult::Failure {
                            node_id,
                            error: "local multipart abort lookup timeout".into(),
                        },
                    }
                })
            } else {
                let client = self.rpc_client.clone();
                let request = proto::AbortMultipartUploadReplicaRequest {
                    upload_id: upload_id.to_string(),
                    coordinator_id: self.local_node_id.to_bytes().to_vec(),
                    operation_id: operation_id.to_be_bytes().to_vec(),
                    topology_epoch: self.topology_epoch,
                };
                let timeout_dur = self.timeouts.replica_write_timeout;

                tokio::spawn(async move {
                    let result =
                        timeout(timeout_dur, client.send_multipart_abort(node_id, request)).await;
                    match result {
                        Ok(Ok(resp)) if resp.success => MultipartReplicaResult::Success {
                            node_id,
                            etag: String::new(),
                            part_record: None,
                        },
                        Ok(Ok(resp)) if resp.epoch_mismatch => {
                            MultipartReplicaResult::EpochMismatch { node_id }
                        }
                        Ok(Ok(resp)) => MultipartReplicaResult::Failure {
                            node_id,
                            error: resp.error,
                        },
                        Ok(Err(e)) => MultipartReplicaResult::Failure {
                            node_id,
                            error: e.to_string(),
                        },
                        Err(_) => MultipartReplicaResult::Failure {
                            node_id,
                            error: "replica multipart abort timeout".into(),
                        },
                    }
                })
            };
            handles.push(handle);
        }

        let results = self
            .collect_multipart_results(handles, self.write_quorum, self.timeouts.quorum_timeout)
            .await?;
        let acked = results.iter().filter(|r| r.is_success()).count() as u8;
        if acked < self.write_quorum {
            return Err(ClusterError::QuorumNotMet {
                needed: self.write_quorum,
                achieved: acked,
            });
        }

        Ok(QuorumMultipartCreateResult {
            replicas_acked: acked,
            replicas_total,
        })
    }

    /// Single-node write bypass — no quorum, no gRPC, no hints.
    async fn write_single_node(
        &self,
        bucket: &str,
        key: &str,
        data: &[u8],
        content_type: &str,
        metadata: &HashMap<String, String>,
    ) -> Result<QuorumWriteResult, ClusterError> {
        let hlc = self.clock.now();
        let operation_id = uuid::Uuid::new_v4().as_u128();
        // Pass HLC via reserved metadata keys for atomic stamping
        let mut metadata_with_hlc = metadata.clone();
        metadata_with_hlc.insert("_s4_hlc_wall".to_string(), hlc.wall_time.to_string());
        metadata_with_hlc.insert("_s4_hlc_logical".to_string(), hlc.logical.to_string());
        metadata_with_hlc.insert("_s4_hlc_node_id".to_string(), hlc.node_id.to_string());
        metadata_with_hlc.insert("_s4_operation_id".to_string(), operation_id.to_string());
        let etag = self
            .storage
            .put_object(bucket, key, data, content_type, &metadata_with_hlc)
            .await
            .map_err(|e| ClusterError::Storage(e.to_string()))?;

        debug!(
            bucket = %bucket,
            key = %key,
            etag = %etag,
            "single-node write completed"
        );

        Ok(QuorumWriteResult {
            etag,
            replicas_acked: 1,
            replicas_total: 1,
            hlc,
        })
    }

    /// Replicate data to remote replicas only (skip local node).
    ///
    /// Used by CompleteMultipartUpload: the local node already has the correct
    /// composite manifest from `complete_multipart_native()`, so we only need
    /// to send the assembled data to remote replicas. The local node is counted
    /// as 1 ACK toward the quorum (W), so we need W-1 remote ACKs.
    pub async fn replicate_to_remotes(
        &self,
        bucket: &str,
        key: &str,
        data: &[u8],
        content_type: &str,
        metadata: &HashMap<String, String>,
    ) -> Result<QuorumWriteResult, ClusterError> {
        let decision = self.router.route(bucket, key)?;
        let replicas = &decision.replicas;

        // Single-node: nothing to replicate
        if replicas.len() == 1 && replicas[0] == self.local_node_id {
            return Ok(QuorumWriteResult {
                etag: String::new(),
                replicas_acked: 1,
                replicas_total: 1,
                hlc: self.clock.now(),
            });
        }

        let operation_id = uuid::Uuid::new_v4().as_u128();
        let hlc = self.clock.now();
        let content_hash = compute_sha256(data);

        let replicas_total = replicas.len() as u8;

        // Fan out to REMOTE replicas only (skip local)
        let mut handles = Vec::with_capacity(replicas.len());

        for &node_id in replicas {
            if node_id == self.local_node_id {
                continue; // Skip local — already has the data
            }

            let client = self.rpc_client.clone();
            let request = proto::WriteBlobRequest {
                bucket: bucket.to_string(),
                key: key.to_string(),
                data: data.to_vec(),
                content_hash: content_hash.to_vec(),
                content_type: content_type.to_string(),
                metadata: metadata.clone(),
                hlc: Some(hlc.to_proto()),
                version_id: None,
                coordinator_id: self.local_node_id.to_bytes().to_vec(),
                operation_id: operation_id.to_be_bytes().to_vec(),
                topology_epoch: self.topology_epoch,
            };
            let timeout_dur = self.timeouts.replica_write_timeout;

            let handle = tokio::spawn(async move {
                let result = timeout(timeout_dur, client.send_write(node_id, request)).await;

                match result {
                    Ok(Ok(resp)) if resp.success => ReplicaResult::Success {
                        node_id,
                        etag: String::new(),
                    },
                    Ok(Ok(resp)) if resp.epoch_mismatch => ReplicaResult::EpochMismatch { node_id },
                    Ok(Ok(resp)) => ReplicaResult::Failure {
                        node_id,
                        error: resp.error,
                    },
                    Ok(Err(e)) => ReplicaResult::Failure {
                        node_id,
                        error: e.to_string(),
                    },
                    Err(_) => ReplicaResult::Failure {
                        node_id,
                        error: "replica write timeout".into(),
                    },
                }
            });

            handles.push(handle);
        }

        // We need W-1 remote ACKs (local counts as 1)
        let remote_quorum = self.write_quorum.saturating_sub(1).max(1);
        let results = self.collect_quorum_results(handles, remote_quorum).await?;

        let acked: Vec<_> = results.iter().filter(|r| r.is_success()).collect();

        if (acked.len() as u8) < remote_quorum {
            return Err(ClusterError::QuorumNotMet {
                needed: self.write_quorum,
                achieved: acked.len() as u8 + 1, // +1 for local
            });
        }

        // Store hints for remote replicas that did not ACK before quorum.
        let hint_nodes = unacked_replica_nodes(replicas, &results, Some(self.local_node_id));
        for node_id in &hint_nodes {
            let hint = Hint {
                target_node: node_id.0,
                operation: HintOperation::Write {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                    data: data.to_vec(),
                    content_hash: content_hash.to_vec(),
                    content_type: content_type.to_string(),
                    metadata: metadata.clone(),
                    operation_id: operation_id.to_be_bytes().to_vec(),
                    version_id: None,
                    topology_epoch: self.topology_epoch,
                },
                created_at: crate::hlc::now_millis(),
                ttl_ms: Duration::from_secs(3 * 3600).as_millis() as u64,
            };

            let hints = self.hints.clone();
            let node_id = *node_id;
            tokio::spawn(async move {
                if let Err(e) = hints.store_hint(hint).await {
                    error!(
                        target_node = %node_id,
                        error = %e,
                        "failed to store hint for multipart replication"
                    );
                }
            });
        }

        info!(
            bucket = %bucket,
            key = %key,
            acked = acked.len() + 1, // +1 for local
            hinted = hint_nodes.len(),
            "multipart remote replication completed"
        );

        Ok(QuorumWriteResult {
            etag: String::new(),
            replicas_acked: acked.len() as u8 + 1, // +1 for local
            replicas_total,
            hlc,
        })
    }

    /// Collect results from spawned tasks, returning as soon as we have
    /// enough for quorum OR all tasks complete.
    async fn collect_quorum_results(
        &self,
        handles: Vec<tokio::task::JoinHandle<ReplicaResult>>,
        quorum: u8,
    ) -> Result<Vec<ReplicaResult>, ClusterError> {
        let total = handles.len();
        let mut results = Vec::with_capacity(total);
        let mut success_count = 0u8;

        let (tx, mut rx) = tokio::sync::mpsc::channel(total);
        for handle in handles {
            let tx = tx.clone();
            tokio::spawn(async move {
                match handle.await {
                    Ok(result) => {
                        let _ = tx.send(result).await;
                    }
                    Err(e) => {
                        error!(error = %e, "join error in quorum collection");
                    }
                }
            });
        }
        drop(tx);

        let deadline = tokio::time::Instant::now() + self.timeouts.quorum_timeout;

        loop {
            tokio::select! {
                maybe_result = rx.recv() => {
                    match maybe_result {
                        Some(result) => {
                            if result.is_success() {
                                success_count += 1;
                            }
                            results.push(result);

                            if success_count >= quorum {
                                break;
                            }
                        }
                        None => break, // All tasks completed
                    }
                }
                _ = tokio::time::sleep_until(deadline) => {
                    warn!(
                        success = success_count,
                        needed = quorum,
                        pending = total - results.len(),
                        "quorum timeout reached"
                    );
                    break;
                }
            }
        }

        Ok(results)
    }

    async fn collect_multipart_results(
        &self,
        handles: Vec<tokio::task::JoinHandle<MultipartReplicaResult>>,
        quorum: u8,
        wait_timeout: Duration,
    ) -> Result<Vec<MultipartReplicaResult>, ClusterError> {
        let total = handles.len();
        let mut results = Vec::with_capacity(total);
        let mut success_count = 0u8;

        let (tx, mut rx) = tokio::sync::mpsc::channel(total);
        for handle in handles {
            let tx = tx.clone();
            tokio::spawn(async move {
                match handle.await {
                    Ok(result) => {
                        let _ = tx.send(result).await;
                    }
                    Err(e) => {
                        error!(error = %e, "join error in multipart quorum collection");
                    }
                }
            });
        }
        drop(tx);

        let deadline = tokio::time::Instant::now() + wait_timeout;

        loop {
            tokio::select! {
                maybe_result = rx.recv() => {
                    match maybe_result {
                        Some(result) => {
                            if result.is_success() {
                                success_count += 1;
                            }
                            results.push(result);

                            if success_count >= quorum {
                                break;
                            }
                        }
                        None => break,
                    }
                }
                _ = tokio::time::sleep_until(deadline) => {
                    warn!(
                        success = success_count,
                        needed = quorum,
                        pending = total - results.len(),
                        "multipart quorum timeout reached"
                    );
                    break;
                }
            }
        }

        Ok(results)
    }

    async fn collect_multipart_inspections(
        &self,
        handles: Vec<tokio::task::JoinHandle<MultipartInspectResult>>,
        quorum: u8,
    ) -> Result<Vec<MultipartInspectResult>, ClusterError> {
        let total = handles.len();
        let mut results = Vec::with_capacity(total);
        let mut success_count = 0u8;

        let mut set = tokio::task::JoinSet::new();
        for handle in handles {
            set.spawn(handle);
        }

        let deadline = tokio::time::Instant::now() + self.timeouts.quorum_timeout;

        loop {
            tokio::select! {
                maybe_result = set.join_next() => {
                    match maybe_result {
                        Some(Ok(Ok(result))) => {
                            if result.is_success() {
                                success_count += 1;
                            }
                            results.push(result);

                        }
                        Some(Ok(Err(e))) => {
                            error!(error = %e, "multipart inspect task panicked");
                        }
                        Some(Err(e)) => {
                            error!(error = %e, "join error in multipart inspect collection");
                        }
                        None => break,
                    }
                }
                _ = tokio::time::sleep_until(deadline) => {
                    warn!(
                        success = success_count,
                        needed = quorum,
                        pending = total - results.len(),
                        "multipart inspect quorum timeout reached"
                    );
                    break;
                }
            }
        }

        Ok(results)
    }

    fn multipart_transfer_timeout(&self, _content_length: u64) -> Duration {
        if self.timeouts.replica_write_timeout > MULTIPART_TRANSFER_TIMEOUT {
            self.timeouts.replica_write_timeout
        } else {
            MULTIPART_TRANSFER_TIMEOUT
        }
    }
}

/// Result from a single replica operation.
#[derive(Debug)]
#[allow(dead_code)]
enum ReplicaResult {
    Success { node_id: NodeId, etag: String },
    Failure { node_id: NodeId, error: String },
    EpochMismatch { node_id: NodeId },
}

#[derive(Debug)]
enum MultipartReplicaResult {
    Success {
        node_id: NodeId,
        etag: String,
        part_record: Option<MultipartPartRecord>,
    },
    Failure {
        node_id: NodeId,
        error: String,
    },
    EpochMismatch {
        node_id: NodeId,
    },
}

#[derive(Debug)]
#[allow(dead_code)]
enum MultipartInspectResult {
    Success(MultipartReplicaInspection),
    Failure { node_id: NodeId, error: String },
    EpochMismatch { node_id: NodeId },
}

impl MultipartInspectResult {
    fn is_success(&self) -> bool {
        matches!(self, MultipartInspectResult::Success(_))
    }

    fn is_no_such_upload(&self) -> bool {
        matches!(
            self,
            MultipartInspectResult::Failure { error, .. } if error.contains("NoSuchUpload")
        )
    }
}

impl MultipartReplicaResult {
    fn is_success(&self) -> bool {
        matches!(self, MultipartReplicaResult::Success { .. })
    }

    fn is_no_such_upload(&self) -> bool {
        matches!(
            self,
            MultipartReplicaResult::Failure { error, .. } if error.contains("NoSuchUpload")
        )
    }

    fn etag(&self) -> Option<&str> {
        match self {
            MultipartReplicaResult::Success { etag, .. } => Some(etag.as_str()),
            _ => None,
        }
    }

    fn part_record(&self) -> Option<&MultipartPartRecord> {
        match self {
            MultipartReplicaResult::Success { part_record, .. } => part_record.as_ref(),
            _ => None,
        }
    }

    #[allow(dead_code)]
    fn node_id(&self) -> NodeId {
        match self {
            MultipartReplicaResult::Success { node_id, .. }
            | MultipartReplicaResult::Failure { node_id, .. }
            | MultipartReplicaResult::EpochMismatch { node_id } => *node_id,
        }
    }
}

impl ReplicaResult {
    fn is_success(&self) -> bool {
        matches!(self, ReplicaResult::Success { .. })
    }

    fn node_id(&self) -> NodeId {
        match self {
            ReplicaResult::Success { node_id, .. }
            | ReplicaResult::Failure { node_id, .. }
            | ReplicaResult::EpochMismatch { node_id } => *node_id,
        }
    }
}

fn compute_sha256(data: &[u8]) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hasher.finalize().to_vec()
}

fn unacked_replica_nodes(
    replicas: &[NodeId],
    results: &[ReplicaResult],
    exclude: Option<NodeId>,
) -> Vec<NodeId> {
    let acked_nodes: Vec<NodeId> = results
        .iter()
        .filter(|result| result.is_success())
        .map(ReplicaResult::node_id)
        .collect();

    replicas
        .iter()
        .copied()
        .filter(|node_id| Some(*node_id) != exclude && !acked_nodes.contains(node_id))
        .collect()
}

fn compute_native_multipart_etag(parts: &[MultipartPartRecord]) -> String {
    let mut ctx = md5::Context::new();
    for part in parts {
        ctx.consume(part.etag_md5_bytes);
    }
    let hash = ctx.compute();
    format!("{:x}-{}", hash, parts.len())
}

fn validate_cluster_manifest_parts(
    upload_id: &str,
    manifest_parts: &[(u32, String)],
    all_parts: &[MultipartPartRecord],
) -> Result<Vec<MultipartPartRecord>, ClusterError> {
    let mut selected_parts = Vec::with_capacity(manifest_parts.len());
    for (part_number, etag) in manifest_parts {
        let Some(stored) = all_parts.iter().find(|part| part.part_number == *part_number) else {
            return Err(ClusterError::InvalidMultipartPart {
                upload_id: upload_id.to_string(),
                message: format!("part {part_number} is missing on replica"),
            });
        };
        if stored.etag_md5_hex != *etag {
            return Err(ClusterError::InvalidMultipartPart {
                upload_id: upload_id.to_string(),
                message: format!(
                    "part {part_number} ETag mismatch: manifest={etag}, stored={}",
                    stored.etag_md5_hex
                ),
            });
        }
        selected_parts.push(stored.clone());
    }

    if selected_parts.is_empty() {
        return Err(ClusterError::InvalidMultipartPart {
            upload_id: upload_id.to_string(),
            message: "no selected parts".to_string(),
        });
    }

    const MIN_PART_SIZE: u64 = 5 * 1024 * 1024;
    if selected_parts.len() > 1 {
        for part in &selected_parts[..selected_parts.len() - 1] {
            if part.size < MIN_PART_SIZE {
                return Err(ClusterError::MultipartPartTooSmall {
                    part_number: part.part_number,
                    size: part.size,
                });
            }
        }
    }

    Ok(selected_parts)
}

fn split_bytes_for_stream(bytes: Bytes) -> Vec<Bytes> {
    if bytes.len() <= MULTIPART_RPC_CHUNK_SIZE {
        return vec![bytes];
    }

    let mut chunks = Vec::with_capacity(bytes.len().div_ceil(MULTIPART_RPC_CHUNK_SIZE));
    let mut remaining = bytes;
    while !remaining.is_empty() {
        let next_len = remaining.len().min(MULTIPART_RPC_CHUNK_SIZE);
        chunks.push(remaining.split_to(next_len));
    }
    chunks
}

async fn fanout_multipart_piece(
    piece: Bytes,
    local_senders: &[tokio::sync::mpsc::Sender<Result<Bytes, io::Error>>],
    remote_senders: &[tokio::sync::mpsc::Sender<proto::UploadMultipartPartReplicaChunk>],
) {
    for sender in local_senders {
        let _ = sender.send(Ok(piece.clone())).await;
    }

    if remote_senders.is_empty() {
        return;
    }

    let payload = piece.to_vec();
    for sender in remote_senders {
        let msg = proto::UploadMultipartPartReplicaChunk {
            payload: Some(proto::upload_multipart_part_replica_chunk::Payload::Data(
                payload.clone(),
            )),
        };
        let _ = sender.send(msg).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gossip::GossipHandle;
    use crate::hints::HintedHandoffConfig;
    use crate::placement::PlacementRouter;
    use crate::rpc::NodeClientConfig;

    fn setup_single_node() -> (QuorumWriteCoordinator, tempfile::TempDir) {
        let tmp = tempfile::tempdir().unwrap();
        let node_id = NodeId(1);
        let gossip = Arc::new(GossipHandle::new_for_testing(Default::default()));
        let rpc_client = Arc::new(NodeClient::new(gossip.clone(), NodeClientConfig::default()));
        let router = Arc::new(PlacementRouter::single_node(node_id));
        let clock = Arc::new(HybridClock::new(node_id));
        let hint_config = HintedHandoffConfig::new(tmp.path());
        let hints = Arc::new(HintedHandoffManager::new(
            hint_config,
            gossip,
            rpc_client.clone(),
        ));
        let storage = Arc::new(MockStorage);

        let config = ClusterConfig::default();
        let coordinator = QuorumWriteCoordinator::new(
            node_id,
            storage,
            rpc_client,
            router,
            clock,
            hints,
            &config,
            WriteTimeouts::default(),
        );

        (coordinator, tmp)
    }

    #[tokio::test]
    async fn single_node_write_bypasses_quorum() {
        let (coordinator, _tmp) = setup_single_node();
        let result = coordinator
            .write(
                "test-bucket",
                "key.txt",
                b"hello",
                "text/plain",
                &HashMap::new(),
            )
            .await
            .unwrap();

        assert_eq!(result.replicas_acked, 1);
        assert_eq!(result.replicas_total, 1);
        assert_eq!(result.etag, "\"mock-etag\"");
    }

    #[tokio::test]
    async fn single_node_delete_bypasses_quorum() {
        let (coordinator, _tmp) = setup_single_node();
        let result = coordinator.delete("test-bucket", "key.txt").await.unwrap();
        assert_eq!(result.replicas_acked, 1);
        assert_eq!(result.replicas_total, 1);
    }

    #[test]
    fn compute_sha256_produces_32_bytes() {
        let hash = compute_sha256(b"hello world");
        assert_eq!(hash.len(), 32);
    }

    #[test]
    fn write_timeouts_default() {
        let t = WriteTimeouts::default();
        assert_eq!(t.quorum_timeout, Duration::from_secs(60));
        assert_eq!(t.replica_write_timeout, Duration::from_secs(60));
    }

    #[tokio::test]
    async fn collect_quorum_results_returns_after_quorum() {
        let (coordinator, _tmp) = setup_single_node();
        let handles = vec![
            tokio::spawn(async {
                ReplicaResult::Success {
                    node_id: NodeId(1),
                    etag: "etag-1".to_string(),
                }
            }),
            tokio::spawn(async {
                ReplicaResult::Success {
                    node_id: NodeId(2),
                    etag: "etag-2".to_string(),
                }
            }),
            tokio::spawn(async {
                tokio::time::sleep(Duration::from_secs(60)).await;
                ReplicaResult::Success {
                    node_id: NodeId(3),
                    etag: "etag-3".to_string(),
                }
            }),
        ];

        let results = tokio::time::timeout(
            Duration::from_secs(1),
            coordinator.collect_quorum_results(handles, 2),
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(results.iter().filter(|r| r.is_success()).count(), 2);
    }

    #[tokio::test]
    async fn collect_multipart_results_accepts_any_quorum() {
        let (coordinator, _tmp) = setup_single_node();
        let handles = vec![
            tokio::spawn(async {
                MultipartReplicaResult::Failure {
                    node_id: NodeId(1),
                    error: "temporary failure".to_string(),
                }
            }),
            tokio::spawn(async {
                MultipartReplicaResult::Success {
                    node_id: NodeId(2),
                    etag: "etag-2".to_string(),
                    part_record: None,
                }
            }),
            tokio::spawn(async {
                MultipartReplicaResult::Success {
                    node_id: NodeId(3),
                    etag: "etag-3".to_string(),
                    part_record: None,
                }
            }),
        ];

        let results = tokio::time::timeout(
            Duration::from_secs(1),
            coordinator.collect_multipart_results(handles, 2, Duration::from_secs(60)),
        )
        .await
        .unwrap()
        .unwrap();

        let success_nodes: Vec<_> = results
            .iter()
            .filter(|r| r.is_success())
            .map(MultipartReplicaResult::node_id)
            .collect();
        assert_eq!(success_nodes, vec![NodeId(2), NodeId(3)]);
    }

    #[test]
    fn unacked_replica_nodes_includes_failed_and_pending_nodes() {
        let replicas = [NodeId(1), NodeId(2), NodeId(3)];
        let results = vec![
            ReplicaResult::Failure {
                node_id: NodeId(1),
                error: "down".to_string(),
            },
            ReplicaResult::Success {
                node_id: NodeId(2),
                etag: "etag".to_string(),
            },
        ];

        assert_eq!(
            unacked_replica_nodes(&replicas, &results, None),
            vec![NodeId(1), NodeId(3)]
        );
    }

    #[tokio::test]
    async fn collect_multipart_inspections_waits_beyond_first_success_quorum() {
        let (coordinator, _tmp) = setup_single_node();
        let handles = vec![
            tokio::spawn(async {
                MultipartInspectResult::Success(sample_inspection(NodeId(1), Vec::new()))
            }),
            tokio::spawn(async {
                MultipartInspectResult::Success(sample_inspection(NodeId(2), Vec::new()))
            }),
            tokio::spawn(async {
                tokio::time::sleep(Duration::from_millis(50)).await;
                MultipartInspectResult::Success(sample_inspection(NodeId(3), Vec::new()))
            }),
        ];

        let results = tokio::time::timeout(
            Duration::from_secs(1),
            coordinator.collect_multipart_inspections(handles, 2),
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(results.iter().filter(|r| r.is_success()).count(), 3);
    }

    #[test]
    fn validate_cluster_manifest_rejects_missing_part() {
        let err = validate_cluster_manifest_parts("upload-1", &[(1, "etag".to_string())], &[])
            .unwrap_err();
        assert!(matches!(err, ClusterError::InvalidMultipartPart { .. }));
    }

    #[test]
    fn validate_cluster_manifest_rejects_small_non_final_part() {
        let parts = vec![
            sample_part("upload-1", 1, 1024, [1u8; 16]),
            sample_part("upload-1", 2, 5 * 1024 * 1024, [2u8; 16]),
        ];
        let manifest = vec![
            (1, parts[0].etag_md5_hex.clone()),
            (2, parts[1].etag_md5_hex.clone()),
        ];

        let err = validate_cluster_manifest_parts("upload-1", &manifest, &parts).unwrap_err();
        assert!(matches!(err, ClusterError::MultipartPartTooSmall { .. }));
    }

    fn sample_part(
        upload_id: &str,
        part_number: u32,
        size: u64,
        etag_md5_bytes: [u8; 16],
    ) -> MultipartPartRecord {
        let physical_hash = [part_number as u8; 32];
        MultipartPartRecord {
            upload_id: upload_id.to_string(),
            part_number,
            size,
            etag_md5_hex: format!("{part_number:032x}"),
            etag_md5_bytes,
            physical_hash,
            blob_id: s4_core::types::composite::BlobId::from_content_hash(physical_hash),
            crc32: part_number,
            created_at: 0,
        }
    }

    fn sample_inspection(
        node_id: NodeId,
        parts: Vec<MultipartPartRecord>,
    ) -> MultipartReplicaInspection {
        MultipartReplicaInspection {
            node_id,
            session: MultipartUploadSession {
                upload_id: "upload-1".to_string(),
                bucket: "test-bucket".to_string(),
                key: "object.bin".to_string(),
                content_type: "application/octet-stream".to_string(),
                metadata: HashMap::new(),
                state: MultipartUploadState::Open,
                created_at: 0,
                updated_at: 0,
            },
            parts,
        }
    }

    /// Minimal mock for unit tests.
    struct MockStorage;

    #[async_trait::async_trait]
    impl StorageEngine for MockStorage {
        async fn put_object(
            &self,
            _bucket: &str,
            _key: &str,
            _data: &[u8],
            _content_type: &str,
            _metadata: &HashMap<String, String>,
        ) -> Result<String, s4_core::StorageError> {
            Ok("\"mock-etag\"".into())
        }
        async fn get_object(
            &self,
            _b: &str,
            _k: &str,
        ) -> Result<(Vec<u8>, s4_core::IndexRecord), s4_core::StorageError> {
            Err(s4_core::StorageError::ObjectNotFound { key: "mock".into() })
        }
        async fn delete_object(&self, _b: &str, _k: &str) -> Result<(), s4_core::StorageError> {
            Ok(())
        }
        async fn head_object(
            &self,
            _b: &str,
            _k: &str,
        ) -> Result<s4_core::IndexRecord, s4_core::StorageError> {
            Err(s4_core::StorageError::ObjectNotFound { key: "mock".into() })
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
            Err(s4_core::StorageError::ObjectNotFound { key: "mock".into() })
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
            Err(s4_core::StorageError::ObjectNotFound { key: "mock".into() })
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
            Err(s4_core::StorageError::ObjectNotFound { key: "mock".into() })
        }
        async fn open_object_version_stream(
            &self,
            _b: &str,
            _k: &str,
            _v: &str,
            _opts: s4_core::ReadOptions,
        ) -> Result<s4_core::ObjectStream, s4_core::StorageError> {
            Err(s4_core::StorageError::ObjectNotFound { key: "mock".into() })
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
