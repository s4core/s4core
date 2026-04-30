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

//! Replica-side handler for native multipart quorum operations.

use std::collections::HashMap;
use std::io;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use s4_core::types::composite::{MultipartPartRecord, MultipartUploadSession};
use s4_core::{IndexRecord, StorageError};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tokio_util::io::StreamReader;
use tracing::{debug, warn};

use crate::error::ClusterError;
use crate::idempotency::IdempotencyTracker;
use crate::rpc::proto;

/// Storage operations required for native multipart replica RPCs.
///
/// This is intentionally narrower than `StorageEngine`: multipart state lives in
/// dedicated fjall keyspaces and is not part of the public object API.
#[async_trait]
pub trait MultipartReplicaStorage: Send + Sync {
    /// Create a durable multipart session.
    async fn create_multipart_session(
        &self,
        upload_id: &str,
        bucket: &str,
        key: &str,
        content_type: &str,
        metadata: &HashMap<String, String>,
    ) -> Result<(), StorageError>;

    /// Read a durable multipart session.
    async fn get_multipart_session(
        &self,
        upload_id: &str,
    ) -> Result<MultipartUploadSession, StorageError>;

    /// Store one multipart part from a stream.
    async fn upload_multipart_part_streaming(
        &self,
        upload_id: &str,
        part_number: u32,
        reader: Box<dyn tokio::io::AsyncRead + Unpin + Send>,
        content_length: u64,
    ) -> Result<MultipartPartRecord, StorageError>;

    /// List durable multipart part records.
    async fn list_multipart_parts(
        &self,
        upload_id: &str,
    ) -> Result<Vec<MultipartPartRecord>, StorageError>;

    /// Mark a session as completing.
    async fn mark_session_completing(
        &self,
        upload_id: &str,
    ) -> Result<MultipartUploadSession, StorageError>;

    /// Revert a session from completing back to open.
    async fn revert_session_to_open(&self, upload_id: &str) -> Result<(), StorageError>;

    /// Complete a native multipart upload.
    #[allow(clippy::too_many_arguments)]
    async fn complete_multipart_native(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        selected_parts: &[MultipartPartRecord],
        s3_etag: &str,
        content_type: &str,
        metadata: &HashMap<String, String>,
    ) -> Result<IndexRecord, StorageError>;

    /// Abort a native multipart upload.
    async fn abort_multipart_native(&self, upload_id: &str) -> Result<(), StorageError>;
}

/// Handles native multipart operations on a replica node.
pub struct ReplicaMultipartHandler {
    storage: Arc<dyn MultipartReplicaStorage>,
    idempotency: Arc<IdempotencyTracker>,
    topology_epoch: u64,
}

impl ReplicaMultipartHandler {
    /// Create a new multipart replica handler.
    pub fn new(
        storage: Arc<dyn MultipartReplicaStorage>,
        idempotency: Arc<IdempotencyTracker>,
        topology_epoch: u64,
    ) -> Self {
        Self {
            storage,
            idempotency,
            topology_epoch,
        }
    }

    /// Handle a create-session request.
    pub async fn handle_create(
        &self,
        request: &proto::CreateMultipartUploadReplicaRequest,
    ) -> Result<proto::CreateMultipartUploadReplicaResponse, ClusterError> {
        if let Some(err) = self.check_create_epoch(request.topology_epoch) {
            return Ok(err);
        }

        let operation_id = parse_operation_id(&request.operation_id)?;
        if self.idempotency.is_duplicate(operation_id, |_key| false) {
            debug!(operation_id = %uuid::Uuid::from_u128(operation_id), "duplicate multipart create, skipping");
            return Ok(proto::CreateMultipartUploadReplicaResponse {
                success: true,
                error: String::new(),
                epoch_mismatch: false,
            });
        }

        self.storage
            .create_multipart_session(
                &request.upload_id,
                &request.bucket,
                &request.key,
                &request.content_type,
                &request.metadata,
            )
            .await
            .map_err(|e| ClusterError::Storage(e.to_string()))?;
        self.idempotency.record(operation_id);

        Ok(proto::CreateMultipartUploadReplicaResponse {
            success: true,
            error: String::new(),
            epoch_mismatch: false,
        })
    }

    /// Handle a streaming upload-part request.
    pub async fn handle_upload_part(
        &self,
        mut stream: tonic::Streaming<proto::UploadMultipartPartReplicaChunk>,
    ) -> Result<proto::UploadMultipartPartReplicaResponse, ClusterError> {
        let first = stream
            .next()
            .await
            .ok_or_else(|| ClusterError::Rpc("multipart part stream missing header".into()))?
            .map_err(|e| ClusterError::Rpc(format!("multipart part stream error: {e}")))?;

        let header = match first.payload {
            Some(proto::upload_multipart_part_replica_chunk::Payload::Header(header)) => header,
            _ => {
                return Err(ClusterError::Rpc(
                    "multipart part stream first message must be a header".into(),
                ));
            }
        };

        if let Some(err) = self.check_upload_part_epoch(header.topology_epoch) {
            return Ok(err);
        }

        let operation_id = parse_operation_id(&header.operation_id)?;
        if self.idempotency.is_duplicate(operation_id, |_key| false) {
            debug!(operation_id = %uuid::Uuid::from_u128(operation_id), "duplicate multipart part, skipping");
            return Ok(proto::UploadMultipartPartReplicaResponse {
                success: true,
                error: String::new(),
                etag: String::new(),
                part_record: Vec::new(),
                epoch_mismatch: false,
            });
        }

        if self.storage.get_multipart_session(&header.upload_id).await.is_err() {
            return Ok(proto::UploadMultipartPartReplicaResponse {
                success: false,
                error: "NoSuchUpload".to_string(),
                etag: String::new(),
                part_record: Vec::new(),
                epoch_mismatch: false,
            });
        }

        let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, io::Error>>(16);
        let pump = tokio::spawn(async move {
            while let Some(chunk) = stream.next().await {
                let chunk = chunk
                    .map_err(|e| io::Error::other(format!("multipart part stream error: {e}")))?;
                match chunk.payload {
                    Some(proto::upload_multipart_part_replica_chunk::Payload::Data(data)) => {
                        if tx.send(Ok(Bytes::from(data))).await.is_err() {
                            return Err(io::Error::new(
                                io::ErrorKind::BrokenPipe,
                                "multipart part storage reader closed",
                            ));
                        }
                    }
                    Some(proto::upload_multipart_part_replica_chunk::Payload::Header(_)) => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "multipart part stream contains duplicate header",
                        ));
                    }
                    None => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "multipart part stream contains empty payload",
                        ));
                    }
                }
            }
            Ok::<(), io::Error>(())
        });

        let reader_stream = ReceiverStream::new(rx);
        let reader = StreamReader::new(reader_stream);
        let part = self
            .storage
            .upload_multipart_part_streaming(
                &header.upload_id,
                header.part_number,
                Box::new(reader),
                header.total_size,
            )
            .await
            .map_err(|e| ClusterError::Storage(e.to_string()))?;

        match pump.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(ClusterError::Io(e)),
            Err(e) => {
                return Err(ClusterError::Rpc(format!(
                    "multipart part stream pump failed: {e}"
                )));
            }
        }

        self.idempotency.record(operation_id);

        let part_record =
            bincode::serialize(&part).map_err(|e| ClusterError::Codec(e.to_string()))?;

        Ok(proto::UploadMultipartPartReplicaResponse {
            success: true,
            error: String::new(),
            etag: part.etag_md5_hex,
            part_record,
            epoch_mismatch: false,
        })
    }

    /// Handle a metadata inspection request used by the coordinator before complete.
    pub async fn handle_inspect(
        &self,
        request: &proto::InspectMultipartUploadReplicaRequest,
    ) -> Result<proto::InspectMultipartUploadReplicaResponse, ClusterError> {
        if let Some(err) = self.check_inspect_epoch(request.topology_epoch) {
            return Ok(err);
        }

        let session = match self.storage.get_multipart_session(&request.upload_id).await {
            Ok(session) => session,
            Err(_) => {
                return Ok(proto::InspectMultipartUploadReplicaResponse {
                    success: false,
                    error: "NoSuchUpload".to_string(),
                    session: Vec::new(),
                    part_records: Vec::new(),
                    epoch_mismatch: false,
                });
            }
        };

        if session.bucket != request.bucket || session.key != request.key {
            return Ok(proto::InspectMultipartUploadReplicaResponse {
                success: false,
                error: "NoSuchUpload".to_string(),
                session: Vec::new(),
                part_records: Vec::new(),
                epoch_mismatch: false,
            });
        }

        let session_bytes =
            bincode::serialize(&session).map_err(|e| ClusterError::Codec(e.to_string()))?;
        let parts = self
            .storage
            .list_multipart_parts(&request.upload_id)
            .await
            .map_err(|e| ClusterError::Storage(e.to_string()))?;
        let part_records = parts
            .iter()
            .map(|p| bincode::serialize(p).map_err(|e| ClusterError::Codec(e.to_string())))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(proto::InspectMultipartUploadReplicaResponse {
            success: true,
            error: String::new(),
            session: session_bytes,
            part_records,
            epoch_mismatch: false,
        })
    }

    /// Handle a complete request.
    pub async fn handle_complete(
        &self,
        request: &proto::CompleteMultipartUploadReplicaRequest,
    ) -> Result<proto::CompleteMultipartUploadReplicaResponse, ClusterError> {
        if let Some(err) = self.check_complete_epoch(request.topology_epoch) {
            return Ok(err);
        }

        let operation_id = parse_operation_id(&request.operation_id)?;
        if self.idempotency.is_duplicate(operation_id, |_key| false) {
            debug!(operation_id = %uuid::Uuid::from_u128(operation_id), "duplicate multipart complete, skipping");
            return Ok(proto::CompleteMultipartUploadReplicaResponse {
                success: true,
                error: String::new(),
                index_record: Vec::new(),
                epoch_mismatch: false,
            });
        }

        let mut selected_parts = Vec::with_capacity(request.selected_part_records.len());
        for raw in &request.selected_part_records {
            let part: MultipartPartRecord =
                bincode::deserialize(raw).map_err(|e| ClusterError::Codec(e.to_string()))?;
            selected_parts.push(part);
        }

        if let Err(message) =
            self.validate_selected_parts(&request.upload_id, &selected_parts).await
        {
            return Ok(proto::CompleteMultipartUploadReplicaResponse {
                success: false,
                error: format!("InvalidPart: {message}"),
                index_record: Vec::new(),
                epoch_mismatch: false,
            });
        }

        let session = match self.storage.mark_session_completing(&request.upload_id).await {
            Ok(session) => session,
            Err(_) => {
                return Ok(proto::CompleteMultipartUploadReplicaResponse {
                    success: false,
                    error: "NoSuchUpload".to_string(),
                    index_record: Vec::new(),
                    epoch_mismatch: false,
                });
            }
        };

        let mut metadata = request.metadata.clone();
        if let Some(hlc_proto) = &request.hlc {
            let hlc = crate::hlc::HlcTimestamp::from_proto(hlc_proto);
            metadata.insert("_s4_hlc_wall".to_string(), hlc.wall_time.to_string());
            metadata.insert("_s4_hlc_logical".to_string(), hlc.logical.to_string());
            metadata.insert("_s4_hlc_node_id".to_string(), hlc.node_id.to_string());
            metadata.insert("_s4_operation_id".to_string(), operation_id.to_string());
        }

        let result = self
            .storage
            .complete_multipart_native(
                &request.bucket,
                &request.key,
                &request.upload_id,
                &selected_parts,
                &request.s3_etag,
                &request.content_type,
                &metadata,
            )
            .await;

        let record = match result {
            Ok(record) => record,
            Err(e) => {
                let _ = self.storage.revert_session_to_open(&session.upload_id).await;
                return Err(ClusterError::Storage(e.to_string()));
            }
        };

        self.idempotency.record(operation_id);
        let index_record =
            bincode::serialize(&record).map_err(|e| ClusterError::Codec(e.to_string()))?;

        Ok(proto::CompleteMultipartUploadReplicaResponse {
            success: true,
            error: String::new(),
            index_record,
            epoch_mismatch: false,
        })
    }

    /// Handle an abort request.
    pub async fn handle_abort(
        &self,
        request: &proto::AbortMultipartUploadReplicaRequest,
    ) -> Result<proto::AbortMultipartUploadReplicaResponse, ClusterError> {
        if let Some(err) = self.check_abort_epoch(request.topology_epoch) {
            return Ok(err);
        }

        let operation_id = parse_operation_id(&request.operation_id)?;
        if self.idempotency.is_duplicate(operation_id, |_key| false) {
            debug!(operation_id = %uuid::Uuid::from_u128(operation_id), "duplicate multipart abort, skipping");
            return Ok(proto::AbortMultipartUploadReplicaResponse {
                success: true,
                error: String::new(),
                epoch_mismatch: false,
            });
        }

        if let Ok(session) = self.storage.get_multipart_session(&request.upload_id).await {
            if session.state == s4_core::types::composite::MultipartUploadState::Completing {
                return Ok(proto::AbortMultipartUploadReplicaResponse {
                    success: false,
                    error: "Cannot abort upload while CompleteMultipartUpload is in progress"
                        .to_string(),
                    epoch_mismatch: false,
                });
            }
            self.storage
                .abort_multipart_native(&request.upload_id)
                .await
                .map_err(|e| ClusterError::Storage(e.to_string()))?;
        }

        self.idempotency.record(operation_id);
        Ok(proto::AbortMultipartUploadReplicaResponse {
            success: true,
            error: String::new(),
            epoch_mismatch: false,
        })
    }

    fn check_create_epoch(
        &self,
        request_epoch: u64,
    ) -> Option<proto::CreateMultipartUploadReplicaResponse> {
        if request_epoch != 0 && request_epoch != self.topology_epoch {
            warn!(
                local_epoch = self.topology_epoch,
                request_epoch, "epoch mismatch on multipart create"
            );
            return Some(proto::CreateMultipartUploadReplicaResponse {
                success: false,
                error: format!(
                    "epoch mismatch: local={}, request={}",
                    self.topology_epoch, request_epoch
                ),
                epoch_mismatch: true,
            });
        }
        None
    }

    fn check_upload_part_epoch(
        &self,
        request_epoch: u64,
    ) -> Option<proto::UploadMultipartPartReplicaResponse> {
        if request_epoch != 0 && request_epoch != self.topology_epoch {
            warn!(
                local_epoch = self.topology_epoch,
                request_epoch, "epoch mismatch on multipart part"
            );
            return Some(proto::UploadMultipartPartReplicaResponse {
                success: false,
                error: format!(
                    "epoch mismatch: local={}, request={}",
                    self.topology_epoch, request_epoch
                ),
                etag: String::new(),
                part_record: Vec::new(),
                epoch_mismatch: true,
            });
        }
        None
    }

    fn check_inspect_epoch(
        &self,
        request_epoch: u64,
    ) -> Option<proto::InspectMultipartUploadReplicaResponse> {
        if request_epoch != 0 && request_epoch != self.topology_epoch {
            warn!(
                local_epoch = self.topology_epoch,
                request_epoch, "epoch mismatch on multipart inspect"
            );
            return Some(proto::InspectMultipartUploadReplicaResponse {
                success: false,
                error: format!(
                    "epoch mismatch: local={}, request={}",
                    self.topology_epoch, request_epoch
                ),
                session: Vec::new(),
                part_records: Vec::new(),
                epoch_mismatch: true,
            });
        }
        None
    }

    fn check_complete_epoch(
        &self,
        request_epoch: u64,
    ) -> Option<proto::CompleteMultipartUploadReplicaResponse> {
        if request_epoch != 0 && request_epoch != self.topology_epoch {
            warn!(
                local_epoch = self.topology_epoch,
                request_epoch, "epoch mismatch on multipart complete"
            );
            return Some(proto::CompleteMultipartUploadReplicaResponse {
                success: false,
                error: format!(
                    "epoch mismatch: local={}, request={}",
                    self.topology_epoch, request_epoch
                ),
                index_record: Vec::new(),
                epoch_mismatch: true,
            });
        }
        None
    }

    fn check_abort_epoch(
        &self,
        request_epoch: u64,
    ) -> Option<proto::AbortMultipartUploadReplicaResponse> {
        if request_epoch != 0 && request_epoch != self.topology_epoch {
            warn!(
                local_epoch = self.topology_epoch,
                request_epoch, "epoch mismatch on multipart abort"
            );
            return Some(proto::AbortMultipartUploadReplicaResponse {
                success: false,
                error: format!(
                    "epoch mismatch: local={}, request={}",
                    self.topology_epoch, request_epoch
                ),
                epoch_mismatch: true,
            });
        }
        None
    }

    async fn validate_selected_parts(
        &self,
        upload_id: &str,
        selected_parts: &[MultipartPartRecord],
    ) -> Result<(), String> {
        let local_parts = self
            .storage
            .list_multipart_parts(upload_id)
            .await
            .map_err(|e| format!("failed to list local parts: {e}"))?;

        for selected in selected_parts {
            let Some(local) =
                local_parts.iter().find(|part| part.part_number == selected.part_number)
            else {
                return Err(format!("part {} is missing locally", selected.part_number));
            };

            if local.etag_md5_hex != selected.etag_md5_hex
                || local.size != selected.size
                || local.physical_hash != selected.physical_hash
                || local.blob_id != selected.blob_id
                || local.crc32 != selected.crc32
            {
                return Err(format!(
                    "part {} metadata does not match local durable record",
                    selected.part_number
                ));
            }
        }

        Ok(())
    }
}

fn parse_operation_id(bytes: &[u8]) -> Result<u128, ClusterError> {
    if bytes.len() != 16 {
        return Err(ClusterError::Codec(format!(
            "operation_id must be 16 bytes, got {}",
            bytes.len()
        )));
    }
    Ok(u128::from_be_bytes(bytes.try_into().map_err(|_| {
        ClusterError::Codec("invalid operation_id".to_string())
    })?))
}
