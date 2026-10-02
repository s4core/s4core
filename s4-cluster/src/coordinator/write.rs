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
use s4_core::types::{
    composite::{MultipartPartRecord, MultipartUploadSession, MultipartUploadState},
    ObjectLayout, S4_STORAGE_CLASS_METADATA_KEY,
};
use s4_core::{IndexRecord, StorageEngine};
use sha2::{Digest, Sha256};
use tokio::time::timeout;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::{Stream, StreamExt};
use tokio_util::io::StreamReader;
use tracing::{debug, error, info, warn};

use crate::cluster_metrics::{observe_quorum, QuorumOperation};
use crate::config::ClusterConfig;
use crate::error::ClusterError;
use crate::hints::{HintEntry, HintedHandoffManager};
use crate::hlc::{HlcTimestamp, HybridClock};
use crate::identity::{NodeId, PoolId};
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
    /// Replica nodes targeted by the placement router.
    pub replica_nodes: Vec<NodeId>,
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
    /// Replica nodes targeted by the placement router.
    pub replica_nodes: Vec<NodeId>,
    /// HLC timestamp assigned to this delete.
    pub hlc: HlcTimestamp,
    /// Idempotency ID assigned to this delete.
    pub operation_id: uuid::Uuid,
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

/// Result of a quorum multipart ListParts operation.
#[derive(Debug, Clone)]
pub struct QuorumMultipartListPartsResult {
    /// Durable part records selected from a quorum-visible replica.
    pub parts: Vec<MultipartPartRecord>,
    /// Number of replicas that returned upload metadata.
    pub replicas_responded: u8,
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
    /// Replica nodes targeted by the placement router.
    pub replica_nodes: Vec<NodeId>,
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

    fn metadata_with_pool_storage_class(
        &self,
        pool_id: PoolId,
        metadata: &HashMap<String, String>,
    ) -> HashMap<String, String> {
        let mut metadata = metadata.clone();
        if let Some(pool) = self.router.pool_config(&pool_id) {
            metadata.insert(
                S4_STORAGE_CLASS_METADATA_KEY.to_string(),
                pool.storage_class.as_metadata_value().to_string(),
            );
        }
        metadata
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
        observe_quorum(
            QuorumOperation::Write,
            self.write_inner(bucket, key, data, content_type, metadata),
        )
        .await
    }

    async fn write_inner(
        &self,
        bucket: &str,
        key: &str,
        data: &[u8],
        content_type: &str,
        metadata: &HashMap<String, String>,
    ) -> Result<QuorumWriteResult, ClusterError> {
        let decision = self.router.route(bucket, key)?;
        let metadata = self.metadata_with_pool_storage_class(decision.pool_id, metadata);
        let replicas = &decision.replicas;

        // Single-node bypass: write directly, no quorum needed
        if replicas.len() == 1 && replicas[0] == self.local_node_id {
            return self.write_single_node(bucket, key, data, content_type, &metadata).await;
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
                let hints = self.hints.clone();
                let hint = HintEntry::written(bucket, key, hlc);

                tokio::spawn(async move {
                    let result = timeout(timeout_dur, client.send_write(node_id, request)).await;

                    let outcome = match result {
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
                    };
                    hint_if_failed(&hints, &outcome, hint);
                    outcome
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

        info!(
            bucket = %bucket,
            key = %key,
            acked = acked.len(),
            "quorum write completed"
        );

        Ok(QuorumWriteResult {
            etag,
            replicas_acked: acked.len() as u8,
            replicas_total,
            replica_nodes: replicas.clone(),
            hlc,
        })
    }

    /// Execute a quorum delete (DELETE object).
    pub async fn delete(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<QuorumDeleteResult, ClusterError> {
        observe_quorum(
            QuorumOperation::Delete,
            self.delete_with_expected_hlc(bucket, key, None),
        )
        .await
    }

    /// Execute a quorum physical purge only if replicas still store the expected HLC.
    pub async fn delete_if_hlc(
        &self,
        bucket: &str,
        key: &str,
        expected_source_hlc: HlcTimestamp,
    ) -> Result<QuorumDeleteResult, ClusterError> {
        observe_quorum(
            QuorumOperation::Delete,
            self.delete_with_expected_hlc(bucket, key, Some(expected_source_hlc)),
        )
        .await
    }

    /// Allocate a coordinator HLC for metadata-only delete commits.
    pub fn next_delete_hlc(&self) -> HlcTimestamp {
        self.clock.now()
    }

    async fn delete_with_expected_hlc(
        &self,
        bucket: &str,
        key: &str,
        expected_source_hlc: Option<HlcTimestamp>,
    ) -> Result<QuorumDeleteResult, ClusterError> {
        let decision = self.router.route(bucket, key)?;
        let replicas = &decision.replicas;
        let operation_uuid = uuid::Uuid::new_v4();
        let operation_id = operation_uuid.as_u128();
        let hlc = self.clock.now();

        // Single-node bypass
        if replicas.len() == 1 && replicas[0] == self.local_node_id {
            if let Some(expected) = expected_source_hlc {
                self.storage
                    .delete_object_if_hlc(
                        bucket,
                        key,
                        expected.wall_time,
                        expected.logical,
                        expected.node_id,
                    )
                    .await
                    .map_err(|e| ClusterError::Storage(e.to_string()))?;
            } else {
                self.storage
                    .delete_object(bucket, key)
                    .await
                    .map_err(|e| ClusterError::Storage(e.to_string()))?;
            }
            return Ok(QuorumDeleteResult {
                replicas_acked: 1,
                replicas_total: 1,
                replica_nodes: replicas.clone(),
                hlc,
                operation_id: operation_uuid,
            });
        }

        let replicas_total = replicas.len() as u8;

        let mut handles = Vec::with_capacity(replicas.len());

        for &node_id in replicas {
            let handle = if node_id == self.local_node_id {
                let storage = self.storage.clone();
                let bucket = bucket.to_string();
                let key = key.to_string();
                let timeout_dur = self.timeouts.replica_write_timeout;

                tokio::spawn(async move {
                    let result = if let Some(expected) = expected_source_hlc {
                        timeout(
                            timeout_dur,
                            storage.delete_object_if_hlc(
                                &bucket,
                                &key,
                                expected.wall_time,
                                expected.logical,
                                expected.node_id,
                            ),
                        )
                        .await
                        .map(|inner| inner.map(|_| ()))
                    } else {
                        timeout(timeout_dur, storage.delete_object(&bucket, &key)).await
                    };
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
                    expected_source_hlc: expected_source_hlc.map(|hlc| hlc.to_proto()),
                };
                let timeout_dur = self.timeouts.replica_write_timeout;
                let hints = self.hints.clone();
                let hint = match expected_source_hlc {
                    Some(expected) => HintEntry::purge(bucket, key, hlc, expected),
                    None => HintEntry::deleted(bucket, key, hlc),
                };

                tokio::spawn(async move {
                    let result = timeout(timeout_dur, client.send_delete(node_id, request)).await;
                    let outcome = match result {
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
                    };
                    hint_if_failed(&hints, &outcome, hint);
                    outcome
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

        Ok(QuorumDeleteResult {
            replicas_acked: acked,
            replicas_total,
            replica_nodes: replicas.clone(),
            hlc,
            operation_id: operation_uuid,
        })
    }

    /// Release the replicas' local copies of an object, keeping it in the
    /// namespace with an erasure-coded layout.
    ///
    /// This is the cutover step of a hot-to-EC migration. It is a write, not a
    /// delete: the object keeps its size, ETag, metadata and its place in every
    /// listing, and only the bytes move. Deleting instead — which is what this
    /// replaces — left the object readable by key but invisible to anything
    /// that enumerates a bucket, including lifecycle rules.
    ///
    /// Guarded by `expected_source_hlc`, so an object overwritten while the
    /// transcode was in flight is left untouched on every replica.
    ///
    /// A replica that does not ACK keeps its hot copy: nothing is lost and the
    /// object stays listable there, but that node's space is reclaimed later,
    /// by EC reconciliation rather than by a hint.
    pub async fn detach_object_data_if_hlc(
        &self,
        bucket: &str,
        key: &str,
        expected_source_hlc: HlcTimestamp,
        erasure_set_id: u64,
        ec_topology_epoch: u64,
    ) -> Result<QuorumDeleteResult, ClusterError> {
        observe_quorum(
            QuorumOperation::Write,
            self.detach_object_data_if_hlc_inner(
                bucket,
                key,
                expected_source_hlc,
                erasure_set_id,
                ec_topology_epoch,
            ),
        )
        .await
    }

    async fn detach_object_data_if_hlc_inner(
        &self,
        bucket: &str,
        key: &str,
        expected_source_hlc: HlcTimestamp,
        erasure_set_id: u64,
        ec_topology_epoch: u64,
    ) -> Result<QuorumDeleteResult, ClusterError> {
        let layout = ObjectLayout::ErasureCoded {
            erasure_set_id,
            topology_epoch: ec_topology_epoch,
        };
        let decision = self.router.route(bucket, key)?;
        let replicas = &decision.replicas;
        let operation_uuid = uuid::Uuid::new_v4();
        let operation_id = operation_uuid.as_u128();
        let hlc = self.clock.now();

        // Single-node bypass
        if replicas.len() == 1 && replicas[0] == self.local_node_id {
            self.storage
                .detach_object_data_if_hlc(
                    bucket,
                    key,
                    expected_source_hlc.wall_time,
                    expected_source_hlc.logical,
                    expected_source_hlc.node_id,
                    layout,
                )
                .await
                .map_err(|e| ClusterError::Storage(e.to_string()))?;
            return Ok(QuorumDeleteResult {
                replicas_acked: 1,
                replicas_total: 1,
                replica_nodes: replicas.clone(),
                hlc,
                operation_id: operation_uuid,
            });
        }

        let replicas_total = replicas.len() as u8;
        let mut handles = Vec::with_capacity(replicas.len());

        for &node_id in replicas {
            let handle = if node_id == self.local_node_id {
                let storage = self.storage.clone();
                let bucket = bucket.to_string();
                let key = key.to_string();
                let timeout_dur = self.timeouts.replica_write_timeout;
                let layout = layout.clone();

                tokio::spawn(async move {
                    let result = timeout(
                        timeout_dur,
                        storage.detach_object_data_if_hlc(
                            &bucket,
                            &key,
                            expected_source_hlc.wall_time,
                            expected_source_hlc.logical,
                            expected_source_hlc.node_id,
                            layout,
                        ),
                    )
                    .await;

                    match result {
                        Ok(Ok(_)) => ReplicaResult::Success {
                            node_id,
                            etag: String::new(),
                        },
                        Ok(Err(e)) => ReplicaResult::Failure {
                            node_id,
                            error: format!("local detach error: {e}"),
                        },
                        Err(_) => ReplicaResult::Failure {
                            node_id,
                            error: "local detach timeout".into(),
                        },
                    }
                })
            } else {
                let client = self.rpc_client.clone();
                let request = proto::DetachObjectDataRequest {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                    coordinator_id: self.local_node_id.to_bytes().to_vec(),
                    operation_id: operation_id.to_be_bytes().to_vec(),
                    topology_epoch: self.topology_epoch,
                    expected_source_hlc: Some(expected_source_hlc.to_proto()),
                    erasure_set_id,
                    ec_topology_epoch,
                };
                let timeout_dur = self.timeouts.replica_write_timeout;

                tokio::spawn(async move {
                    let result = timeout(
                        timeout_dur,
                        client.send_detach_object_data(node_id, request),
                    )
                    .await;
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
                            error: "replica detach timeout".into(),
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

        Ok(QuorumDeleteResult {
            replicas_acked: acked,
            replicas_total,
            replica_nodes: replicas.clone(),
            hlc,
            operation_id: operation_uuid,
        })
    }

    /// Put the object's bytes back on its replicas, undoing a detach.
    ///
    /// This is the cutover step of an EC-to-hot promotion. `data` is the object
    /// decoded from its shards once, by the caller, and handed to every replica
    /// unchanged, so all of them store the same blob and the object keeps its
    /// size, ETag, metadata, timestamps and generation.
    ///
    /// Guarded by `expected_source_hlc` for a stronger reason than its mirror:
    /// a client PUT that landed while the shards were being decoded is newer
    /// than these bytes, and writing them over it would roll the object back.
    ///
    /// `replicas_acked` counts only the replicas that actually took the bytes,
    /// not the ones that merely answered. The caller is expected to compare it
    /// against `replicas_total` and treat anything less as unfinished: unlike
    /// the detach, where a replica that kept its copy still serves the object,
    /// a replica left erasure coded answers a reader on the hot path with "these
    /// bytes are shards in the pool". Reaching write quorum is enough to make
    /// the object durable but not enough to make it readable everywhere.
    ///
    /// # Errors
    ///
    /// Returns [`ClusterError::QuorumNotMet`] when fewer than the write quorum
    /// of replicas took the bytes.
    pub async fn attach_object_data_if_hlc(
        &self,
        bucket: &str,
        key: &str,
        data: &[u8],
        expected_source_hlc: HlcTimestamp,
    ) -> Result<QuorumDeleteResult, ClusterError> {
        observe_quorum(
            QuorumOperation::Write,
            self.attach_object_data_if_hlc_inner(bucket, key, data, expected_source_hlc),
        )
        .await
    }

    async fn attach_object_data_if_hlc_inner(
        &self,
        bucket: &str,
        key: &str,
        data: &[u8],
        expected_source_hlc: HlcTimestamp,
    ) -> Result<QuorumDeleteResult, ClusterError> {
        let decision = self.router.route(bucket, key)?;
        let replicas = &decision.replicas;
        let operation_uuid = uuid::Uuid::new_v4();
        let operation_id = operation_uuid.as_u128();
        let hlc = self.clock.now();

        // Single-node bypass
        if replicas.len() == 1 && replicas[0] == self.local_node_id {
            self.storage
                .attach_object_data_if_hlc(
                    bucket,
                    key,
                    expected_source_hlc.wall_time,
                    expected_source_hlc.logical,
                    expected_source_hlc.node_id,
                    data,
                )
                .await
                .map_err(|e| ClusterError::Storage(e.to_string()))?;
            return Ok(QuorumDeleteResult {
                replicas_acked: 1,
                replicas_total: 1,
                replica_nodes: replicas.clone(),
                hlc,
                operation_id: operation_uuid,
            });
        }

        let replicas_total = replicas.len() as u8;
        let mut handles = Vec::with_capacity(replicas.len());

        for &node_id in replicas {
            let handle = if node_id == self.local_node_id {
                let storage = self.storage.clone();
                let bucket = bucket.to_string();
                let key = key.to_string();
                let data = data.to_vec();
                let timeout_dur = self.timeouts.replica_write_timeout;

                tokio::spawn(async move {
                    let result = timeout(
                        timeout_dur,
                        storage.attach_object_data_if_hlc(
                            &bucket,
                            &key,
                            expected_source_hlc.wall_time,
                            expected_source_hlc.logical,
                            expected_source_hlc.node_id,
                            &data,
                        ),
                    )
                    .await;

                    match result {
                        // Only a replica that actually took the bytes counts.
                        // One that answered "the generation does not match" is
                        // still serving the object out of the pool, and a
                        // promotion that went ahead without it would leave a
                        // replica answering "these bytes are shards" to a
                        // reader sent down the hot path.
                        Ok(Ok(true)) => ReplicaResult::Success {
                            node_id,
                            etag: String::new(),
                        },
                        Ok(Ok(false)) => ReplicaResult::Failure {
                            node_id,
                            error: "local attach found no matching generation".into(),
                        },
                        Ok(Err(e)) => ReplicaResult::Failure {
                            node_id,
                            error: format!("local attach error: {e}"),
                        },
                        Err(_) => ReplicaResult::Failure {
                            node_id,
                            error: "local attach timeout".into(),
                        },
                    }
                })
            } else {
                let client = self.rpc_client.clone();
                let request = proto::AttachObjectDataRequest {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                    coordinator_id: self.local_node_id.to_bytes().to_vec(),
                    operation_id: operation_id.to_be_bytes().to_vec(),
                    topology_epoch: self.topology_epoch,
                    expected_source_hlc: Some(expected_source_hlc.to_proto()),
                    data: data.to_vec(),
                };
                let timeout_dur = self.timeouts.replica_write_timeout;

                tokio::spawn(async move {
                    let result = timeout(
                        timeout_dur,
                        client.send_attach_object_data(node_id, request),
                    )
                    .await;
                    match result {
                        // As above: success is not enough, the replica has to
                        // have taken the bytes.
                        Ok(Ok(resp)) if resp.success && resp.attached => ReplicaResult::Success {
                            node_id,
                            etag: String::new(),
                        },
                        Ok(Ok(resp)) if resp.epoch_mismatch => {
                            ReplicaResult::EpochMismatch { node_id }
                        }
                        Ok(Ok(resp)) if resp.success => ReplicaResult::Failure {
                            node_id,
                            error: "replica attach found no matching generation".into(),
                        },
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
                            error: "replica attach timeout".into(),
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

        Ok(QuorumDeleteResult {
            replicas_acked: acked,
            replicas_total,
            replica_nodes: replicas.clone(),
            hlc,
            operation_id: operation_uuid,
        })
    }

    /// Put back the namespace entry of an erasure-coded object on its replicas.
    ///
    /// Repair path for objects whose entry was lost — by a cutover from before
    /// layout migration existed, or by a replica that was down when its peers
    /// converted theirs. The record is rebuilt from the EC manifest by the
    /// caller; replicas that already have an entry keep it.
    ///
    /// Issued as a quorum operation because the caller is whichever node holds
    /// the manifest, and that is not necessarily one of the object's replicas.
    ///
    /// # Errors
    ///
    /// Returns [`ClusterError::QuorumNotMet`] when fewer than the write quorum
    /// of replicas accepted the entry.
    pub async fn restore_namespace_entry(
        &self,
        bucket: &str,
        key: &str,
        record: &IndexRecord,
    ) -> Result<usize, ClusterError> {
        observe_quorum(
            QuorumOperation::Write,
            self.restore_namespace_entry_inner(bucket, key, record),
        )
        .await
    }

    async fn restore_namespace_entry_inner(
        &self,
        bucket: &str,
        key: &str,
        record: &IndexRecord,
    ) -> Result<usize, ClusterError> {
        let decision = self.router.route(bucket, key)?;
        let replicas = &decision.replicas;
        let operation_id = uuid::Uuid::new_v4().as_u128();
        let encoded = bincode::serialize(record)
            .map_err(|error| ClusterError::Storage(format!("cannot encode entry: {error}")))?;

        let mut handles = Vec::with_capacity(replicas.len());
        let mut restored = 0usize;

        for &node_id in replicas {
            let handle = if node_id == self.local_node_id {
                let storage = self.storage.clone();
                let bucket = bucket.to_string();
                let key = key.to_string();
                let record = record.clone();
                let timeout_dur = self.timeouts.replica_write_timeout;

                tokio::spawn(async move {
                    match timeout(
                        timeout_dur,
                        storage.restore_erasure_coded_entry(&bucket, &key, record),
                    )
                    .await
                    {
                        Ok(Ok(written)) => (
                            ReplicaResult::Success {
                                node_id,
                                etag: String::new(),
                            },
                            written,
                        ),
                        Ok(Err(e)) => (
                            ReplicaResult::Failure {
                                node_id,
                                error: format!("local namespace restore error: {e}"),
                            },
                            false,
                        ),
                        Err(_) => (
                            ReplicaResult::Failure {
                                node_id,
                                error: "local namespace restore timeout".into(),
                            },
                            false,
                        ),
                    }
                })
            } else {
                let client = self.rpc_client.clone();
                let request = proto::RestoreNamespaceEntryRequest {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                    record: encoded.clone(),
                    coordinator_id: self.local_node_id.to_bytes().to_vec(),
                    operation_id: operation_id.to_be_bytes().to_vec(),
                    topology_epoch: self.topology_epoch,
                };
                let timeout_dur = self.timeouts.replica_write_timeout;

                tokio::spawn(async move {
                    match timeout(
                        timeout_dur,
                        client.send_restore_namespace_entry(node_id, request),
                    )
                    .await
                    {
                        Ok(Ok(resp)) if resp.success => (
                            ReplicaResult::Success {
                                node_id,
                                etag: String::new(),
                            },
                            resp.restored,
                        ),
                        Ok(Ok(resp)) if resp.epoch_mismatch => {
                            (ReplicaResult::EpochMismatch { node_id }, false)
                        }
                        Ok(Ok(resp)) => (
                            ReplicaResult::Failure {
                                node_id,
                                error: resp.error,
                            },
                            false,
                        ),
                        Ok(Err(e)) => (
                            ReplicaResult::Failure {
                                node_id,
                                error: e.to_string(),
                            },
                            false,
                        ),
                        Err(_) => (
                            ReplicaResult::Failure {
                                node_id,
                                error: "replica namespace restore timeout".into(),
                            },
                            false,
                        ),
                    }
                })
            };

            handles.push(handle);
        }

        let mut acked = 0u8;
        for handle in handles {
            match handle.await {
                Ok((result, written)) => {
                    if result.is_success() {
                        acked = acked.saturating_add(1);
                    }
                    if written {
                        restored = restored.saturating_add(1);
                    }
                }
                Err(error) => {
                    error!(error = %error, "namespace restore task panicked");
                }
            }
        }

        if acked < self.write_quorum {
            return Err(ClusterError::QuorumNotMet {
                needed: self.write_quorum,
                achieved: acked,
            });
        }

        Ok(restored)
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
        observe_quorum(
            QuorumOperation::Multipart,
            self.create_multipart_upload_inner(upload_id, bucket, key, content_type, metadata),
        )
        .await
    }

    async fn create_multipart_upload_inner(
        &self,
        upload_id: &str,
        bucket: &str,
        key: &str,
        content_type: &str,
        metadata: &HashMap<String, String>,
    ) -> Result<QuorumMultipartCreateResult, ClusterError> {
        let decision = self.router.route(bucket, key)?;
        let metadata = self.metadata_with_pool_storage_class(decision.pool_id, metadata);
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
        observe_quorum(
            QuorumOperation::Multipart,
            self.upload_multipart_part_streaming_inner(
                bucket,
                key,
                upload_id,
                part_number,
                body_stream,
                content_length,
            ),
        )
        .await
    }

    async fn upload_multipart_part_streaming_inner<S>(
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

    /// List multipart parts through the same no-sticky replica inspection path
    /// used by complete. The selected response is a quorum-visible snapshot,
    /// not local-node state.
    pub async fn list_multipart_parts(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<QuorumMultipartListPartsResult, ClusterError> {
        observe_quorum(
            QuorumOperation::Multipart,
            self.list_multipart_parts_inner(bucket, key, upload_id),
        )
        .await
    }

    async fn list_multipart_parts_inner(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<QuorumMultipartListPartsResult, ClusterError> {
        let replicas_total = self.router.route(bucket, key)?.replicas.len() as u8;
        let inspections = self.inspect_multipart_upload(bucket, key, upload_id).await?;
        let replicas_responded = inspections.len() as u8;

        if inspections.is_empty() {
            return Err(ClusterError::MultipartUploadNotFound {
                upload_id: upload_id.to_string(),
            });
        }
        if replicas_responded < self.write_quorum {
            return Err(ClusterError::QuorumNotMet {
                needed: self.write_quorum,
                achieved: replicas_responded,
            });
        }

        let mut selected = inspections
            .into_iter()
            .max_by_key(|inspection| inspection.parts.len())
            .ok_or_else(|| ClusterError::MultipartUploadNotFound {
                upload_id: upload_id.to_string(),
            })?;
        selected.parts.sort_by_key(|part| part.part_number);

        Ok(QuorumMultipartListPartsResult {
            parts: selected.parts,
            replicas_responded,
            replicas_total,
        })
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
        observe_quorum(
            QuorumOperation::Multipart,
            self.complete_multipart_upload_inner(bucket, key, upload_id, manifest_parts),
        )
        .await
    }

    async fn complete_multipart_upload_inner(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        manifest_parts: &[(u32, String)],
    ) -> Result<QuorumMultipartCompleteResult, ClusterError> {
        let mut plan =
            self.prepare_multipart_complete(bucket, key, upload_id, manifest_parts).await?;
        let decision = self.router.route(bucket, key)?;
        plan.metadata = self.metadata_with_pool_storage_class(decision.pool_id, &plan.metadata);
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
            replica_nodes: replicas.clone(),
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
        observe_quorum(
            QuorumOperation::Multipart,
            self.abort_multipart_upload_inner(bucket, key, upload_id),
        )
        .await
    }

    async fn abort_multipart_upload_inner(
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
            replica_nodes: vec![self.local_node_id],
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
        observe_quorum(
            QuorumOperation::Write,
            self.replicate_to_remotes_inner(bucket, key, data, content_type, metadata),
        )
        .await
    }

    async fn replicate_to_remotes_inner(
        &self,
        bucket: &str,
        key: &str,
        data: &[u8],
        content_type: &str,
        metadata: &HashMap<String, String>,
    ) -> Result<QuorumWriteResult, ClusterError> {
        let decision = self.router.route(bucket, key)?;
        let metadata = self.metadata_with_pool_storage_class(decision.pool_id, metadata);
        let replicas = &decision.replicas;

        // Single-node: nothing to replicate
        if replicas.len() == 1 && replicas[0] == self.local_node_id {
            return Ok(QuorumWriteResult {
                etag: String::new(),
                replicas_acked: 1,
                replicas_total: 1,
                replica_nodes: vec![self.local_node_id],
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
            let hints = self.hints.clone();
            let hint = HintEntry::written(bucket, key, hlc);

            let handle = tokio::spawn(async move {
                let result = timeout(timeout_dur, client.send_write(node_id, request)).await;

                let outcome = match result {
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
                };
                hint_if_failed(&hints, &outcome, hint);
                outcome
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

        info!(
            bucket = %bucket,
            key = %key,
            acked = acked.len() + 1, // +1 for local
            "multipart remote replication completed"
        );

        Ok(QuorumWriteResult {
            etag: String::new(),
            replicas_acked: acked.len() as u8 + 1, // +1 for local
            replicas_total,
            replica_nodes: replicas.clone(),
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
}

fn compute_sha256(data: &[u8]) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hasher.finalize().to_vec()
}

/// Leaves `hint` for a remote replica whose own write or delete did not land.
///
/// Decided by the replica's own outcome, whenever it arrives: a replica that
/// answers after the client already has its quorum is not behind, and one
/// that fails or times out is, whether or not the quorum was met without it.
/// An epoch mismatch is not hinted — delivery would meet the same check — and
/// the local replica never gets here: its write goes on after the client's
/// answer, and a failing local disk is not something a retry would fix.
fn hint_if_failed(hints: &Arc<HintedHandoffManager>, outcome: &ReplicaResult, hint: HintEntry) {
    if let ReplicaResult::Failure { node_id, .. } = outcome {
        hints.store_in_background(*node_id, hint);
    }
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
    use crate::config::PoolConfig;
    use crate::gossip::GossipHandle;
    use crate::hints::HintedHandoffConfig;
    use crate::identity::NodeAddr;
    use crate::placement::bucket_placement::BucketPlacement;
    use crate::placement::PlacementRouter;
    use crate::rpc::NodeClientConfig;
    use s4_core::types::StorageClass;

    fn setup_single_node() -> (QuorumWriteCoordinator, tempfile::TempDir) {
        let tmp = tempfile::tempdir().unwrap();
        let node_id = NodeId(1);
        let gossip = Arc::new(GossipHandle::new_for_testing(Default::default()));
        let rpc_client = Arc::new(NodeClient::new(gossip.clone(), NodeClientConfig::default()));
        let router = Arc::new(PlacementRouter::single_node(node_id));
        let clock = Arc::new(HybridClock::new(node_id));
        let hint_config = HintedHandoffConfig::new(tmp.path());
        let hints = Arc::new(HintedHandoffManager::new(hint_config, rpc_client.clone()));
        let storage = Arc::new(MockStorage::default());

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

    fn setup_pool_storage_class(
        storage_class: StorageClass,
    ) -> (QuorumWriteCoordinator, tempfile::TempDir, PoolId) {
        let tmp = tempfile::tempdir().unwrap();
        let node_id = NodeId(1);
        let pool_id = PoolId::new(7);
        let grpc_addr = "127.0.0.1:19001".parse().unwrap();
        let gossip = Arc::new(GossipHandle::new_for_testing(Default::default()));
        let rpc_client = Arc::new(NodeClient::new(gossip.clone(), NodeClientConfig::default()));
        let clock = Arc::new(HybridClock::new(node_id));
        let hint_config = HintedHandoffConfig::new(tmp.path());
        let hints = Arc::new(HintedHandoffManager::new(hint_config, rpc_client.clone()));
        let storage = Arc::new(MockStorage::default());
        let config = ClusterConfig {
            pools: vec![PoolConfig {
                pool_id,
                name: "ec-pool".to_string(),
                nodes: vec![NodeAddr {
                    grpc_addr,
                    http_addr: "127.0.0.1:9001".parse().unwrap(),
                    gossip_addr: "127.0.0.1:19002".parse().unwrap(),
                }],
                replication_factor: Some(1),
                write_quorum: Some(1),
                read_quorum: Some(1),
                storage_class,
            }],
            default_replication_factor: 1,
            default_write_quorum: 1,
            default_read_quorum: 1,
            ..ClusterConfig::default()
        };
        let resolver = HashMap::from([(grpc_addr, node_id)]);
        let router = Arc::new(
            PlacementRouter::new(&config, node_id, BucketPlacement::new(), &resolver).unwrap(),
        );

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

        (coordinator, tmp, pool_id)
    }

    #[test]
    fn metadata_with_pool_storage_class_overrides_reserved_key() {
        let (coordinator, _tmp, pool_id) = setup_pool_storage_class(StorageClass::ErasureCoded);
        let metadata = HashMap::from([
            (
                S4_STORAGE_CLASS_METADATA_KEY.to_string(),
                StorageClass::Standard.as_metadata_value().to_string(),
            ),
            ("x-amz-meta-owner".to_string(), "alice".to_string()),
        ]);

        let metadata = coordinator.metadata_with_pool_storage_class(pool_id, &metadata);

        assert_eq!(
            metadata.get(S4_STORAGE_CLASS_METADATA_KEY).map(String::as_str),
            Some(StorageClass::ErasureCoded.as_metadata_value())
        );
        assert_eq!(
            metadata.get("x-amz-meta-owner").map(String::as_str),
            Some("alice")
        );
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
    /// Storage that takes every write and delete, or, when it `refuses`,
    /// none of them.
    #[derive(Default)]
    struct MockStorage {
        refuses: bool,
    }

    impl MockStorage {
        fn refusing() -> Self {
            Self { refuses: true }
        }

        fn check(&self) -> Result<(), s4_core::StorageError> {
            if self.refuses {
                return Err(s4_core::StorageError::Io(std::io::Error::other(
                    "disk refuses",
                )));
            }
            Ok(())
        }
    }

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
            self.check()?;
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
            self.check()
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

    // ---- Quorum operations, as the metrics count them ----

    use crate::test_metrics::TestMetrics;

    const OPS: &str = "s4_quorum_operations_total";

    /// Node 1 of a three-node pool, RF=3 and W=2, whose two peers gossip holds
    /// dead: a write reaches the local replica and nothing else.
    fn setup_three_nodes_two_dead() -> (QuorumWriteCoordinator, tempfile::TempDir) {
        use crate::gossip::NodeState;
        use crate::identity::{NodeCapacity, NodeMeta, NodeStatus};

        let tmp = tempfile::tempdir().unwrap();
        let local = NodeId(1);
        let addr = |n: u128| NodeAddr {
            grpc_addr: format!("127.0.0.1:{}", 19400 + n).parse().unwrap(),
            http_addr: format!("127.0.0.1:{}", 19500 + n).parse().unwrap(),
            gossip_addr: format!("127.0.0.1:{}", 19600 + n).parse().unwrap(),
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
        let resolver: HashMap<_, _> = (1..=3).map(|n| (addr(n).grpc_addr, NodeId(n))).collect();
        let placement = BucketPlacement::from_entries([("bucket".to_string(), pool_id)]);
        let router = Arc::new(PlacementRouter::new(&config, local, placement, &resolver).unwrap());
        let members = (2..=3)
            .map(|n| {
                let state = NodeState {
                    meta: NodeMeta {
                        node_id: NodeId(n),
                        node_name: format!("node-{n}"),
                        addr: addr(n),
                        pool_id,
                        topology_epoch: 1,
                        protocol_version: 1,
                        capacity: NodeCapacity::default(),
                        started_at: 0,
                        s4_version: "0.1.0".into(),
                        edition: crate::identity::Edition::Community,
                    },
                    status: NodeStatus::Dead,
                    last_seen: std::time::Instant::now(),
                };
                (NodeId(n), state)
            })
            .collect();
        let gossip = Arc::new(GossipHandle::new_for_testing(members));
        let rpc_client = Arc::new(NodeClient::new(gossip.clone(), NodeClientConfig::default()));
        let hints = Arc::new(HintedHandoffManager::new(
            HintedHandoffConfig::new(tmp.path()),
            rpc_client.clone(),
        ));
        let coordinator = QuorumWriteCoordinator::new(
            local,
            Arc::new(MockStorage::default()),
            rpc_client,
            router,
            Arc::new(HybridClock::new(local)),
            hints,
            &config,
            WriteTimeouts::default(),
        );
        (coordinator, tmp)
    }

    #[tokio::test]
    async fn each_operation_is_counted_once_with_its_outcome_and_duration() {
        let metrics = TestMetrics::install();
        let (coordinator, _tmp) = setup_single_node();

        coordinator
            .write("bucket", "key", b"data", "text/plain", &HashMap::new())
            .await
            .expect("single-node write");
        coordinator.delete("bucket", "key").await.expect("single-node delete");
        // The unary part upload hands its body to the streaming one: one
        // multipart operation, not two, whatever its outcome.
        let _ = coordinator
            .upload_multipart_part("bucket", "key", "upload", 1, b"part".to_vec())
            .await;

        assert_eq!(
            metrics.value(OPS, &[("operation", "write"), ("outcome", "ok")]),
            Some(1.0)
        );
        assert_eq!(
            metrics.value(OPS, &[("operation", "delete"), ("outcome", "ok")]),
            Some(1.0)
        );
        assert_eq!(metrics.sum(OPS, &[("operation", "multipart")]), 1.0);
        assert_eq!(metrics.sum(OPS, &[]), 3.0);
        assert_eq!(
            metrics.value(
                "s4_quorum_duration_seconds_count",
                &[("operation", "write")]
            ),
            Some(1.0)
        );
    }

    #[tokio::test]
    async fn a_write_below_the_write_quorum_is_quorum_not_met_and_never_ok() {
        let metrics = TestMetrics::install();
        let (coordinator, _tmp) = setup_three_nodes_two_dead();

        let result =
            coordinator.write("bucket", "key", b"data", "text/plain", &HashMap::new()).await;

        assert!(
            matches!(
                result,
                Err(ClusterError::QuorumNotMet {
                    needed: 2,
                    achieved: 1
                })
            ),
            "{result:?}"
        );
        assert_eq!(
            metrics.value(
                OPS,
                &[("operation", "write"), ("outcome", "quorum_not_met")]
            ),
            Some(1.0)
        );
        assert_eq!(
            metrics.value(OPS, &[("operation", "write"), ("outcome", "ok")]),
            None
        );
        // The replicas that failed are the two dead peers, each counted once.
        for peer in [NodeId(2), NodeId(3)] {
            assert_eq!(
                metrics.value(
                    "s4_peer_requests_total",
                    &[("peer", peer.to_string().as_str()), ("outcome", "skipped")]
                ),
                Some(1.0),
                "{}",
                metrics.render()
            );
        }
    }

    // ---- Hints, left by each replica's own outcome ----

    use crate::hints::{HintEntry, HintKind};

    /// How a peer of the test pool answers a write or a delete.
    #[derive(Clone, Copy)]
    enum Peer {
        /// Takes it and answers at once.
        Answers,
        /// Takes it and answers after this long: after the client already
        /// has its quorum.
        AnswersAfter(Duration),
        /// Runs on another topology epoch and refuses it for that.
        OtherEpoch,
        /// Held dead by gossip: skipped at once.
        Dead,
    }

    /// Node 1 of a three-node pool, RF=3 and W=2, with the local storage
    /// given and nodes 2 and 3 answering as `peers` says.
    struct TestPool {
        coordinator: QuorumWriteCoordinator,
        hints: Arc<HintedHandoffManager>,
        _servers: Vec<crate::rpc::server::RpcServerHandle>,
        _tmp: tempfile::TempDir,
    }

    /// Listens on `listen` and forwards every connection to `to`, but only
    /// after `delay`: a replica that takes the write and answers late.
    async fn start_slow_proxy(
        listen: std::net::SocketAddr,
        to: std::net::SocketAddr,
        delay: Duration,
    ) {
        let listener = tokio::net::TcpListener::bind(listen).await.unwrap();
        tokio::spawn(async move {
            while let Ok((mut inbound, _)) = listener.accept().await {
                tokio::spawn(async move {
                    tokio::time::sleep(delay).await;
                    if let Ok(mut outbound) = tokio::net::TcpStream::connect(to).await {
                        let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
                    }
                });
            }
        });
    }

    async fn setup_pool(local: MockStorage, peers: [Peer; 2], base_port: u16) -> TestPool {
        use crate::gossip::NodeState;
        use crate::identity::{NodeCapacity, NodeMeta, NodeStatus};
        use crate::replica::ReplicaWriteHandler;
        use crate::rpc::server::{NodeRpcServer, RpcServerConfig};

        let tmp = tempfile::tempdir().unwrap();
        let local_id = NodeId(1);
        // A test listens on `base_port + 1..=6` and nothing else: gRPC of
        // nodes 1 to 3, then the proxies of late peers. The HTTP and gossip
        // addresses are never opened.
        let grpc_port = |n: u128| base_port + n as u16;
        let proxied_port = |n: u128| base_port + 3 + n as u16;
        let addr = |n: u128| NodeAddr {
            grpc_addr: format!("127.0.0.1:{}", grpc_port(n)).parse().unwrap(),
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
            protocol_version: 1,
            capacity: NodeCapacity::default(),
            started_at: 0,
            s4_version: "0.1.0".into(),
            edition: crate::identity::Edition::Community,
        };

        let mut servers = Vec::new();
        let mut members = HashMap::new();
        for (n, peer) in (2..=3).zip(peers) {
            let status = match peer {
                Peer::Dead => NodeStatus::Dead,
                _ => NodeStatus::Alive,
            };
            members.insert(
                NodeId(n),
                NodeState {
                    meta: meta(n),
                    status,
                    last_seen: std::time::Instant::now(),
                },
            );
            if let Peer::Dead = peer {
                continue;
            }
            let epoch = match peer {
                Peer::OtherEpoch => config.epoch + 1,
                _ => config.epoch,
            };
            // A late peer is reached through a proxy on its advertised port.
            let listen = match peer {
                Peer::AnswersAfter(_) => format!("127.0.0.1:{}", proxied_port(n)).parse().unwrap(),
                _ => addr(n).grpc_addr,
            };
            let server = NodeRpcServer::new(
                NodeId(n),
                Arc::new(meta(n)),
                Arc::new(GossipHandle::new_for_testing(HashMap::new())),
            )
            .with_replica_handler(Arc::new(ReplicaWriteHandler::new(
                Arc::new(MockStorage::default()),
                Arc::new(crate::idempotency::IdempotencyTracker::new()),
                epoch,
            )));
            servers.push(
                server
                    .start(RpcServerConfig {
                        bind_addr: listen,
                        ..Default::default()
                    })
                    .await
                    .unwrap(),
            );
            if let Peer::AnswersAfter(delay) = peer {
                start_slow_proxy(addr(n).grpc_addr, listen, delay).await;
            }
        }

        let resolver: HashMap<_, _> = (1..=3).map(|n| (addr(n).grpc_addr, NodeId(n))).collect();
        let placement = BucketPlacement::from_entries([("bucket".to_string(), pool_id)]);
        let router =
            Arc::new(PlacementRouter::new(&config, local_id, placement, &resolver).unwrap());
        let gossip = Arc::new(GossipHandle::new_for_testing(members));
        let rpc_client = Arc::new(NodeClient::new(gossip, NodeClientConfig::default()));
        let hints = Arc::new(HintedHandoffManager::new(
            HintedHandoffConfig::new(tmp.path()),
            rpc_client.clone(),
        ));
        let coordinator = QuorumWriteCoordinator::new(
            local_id,
            Arc::new(local),
            rpc_client,
            router,
            Arc::new(HybridClock::new(local_id)),
            hints.clone(),
            &config,
            WriteTimeouts::default(),
        );
        TestPool {
            coordinator,
            hints,
            _servers: servers,
            _tmp: tmp,
        }
    }

    impl TestPool {
        /// The hints for `node` once `count` of them have landed; hints are
        /// written on tasks of their own, after the replica's outcome.
        async fn hints_once(&self, node: u128, count: usize) -> Vec<HintEntry> {
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            loop {
                let hints = self.hints.hints_for(NodeId(node)).await;
                if hints.len() >= count || std::time::Instant::now() >= deadline {
                    return hints;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }

        async fn no_hints_anywhere(&self) -> bool {
            for node in 1..=3 {
                if self.hints.pending(NodeId(node)).await > 0 {
                    return false;
                }
            }
            true
        }
    }

    /// The slowest replica used to be hinted on every write, even when it
    /// took the write a moment after the client had its answer.
    #[tokio::test]
    async fn a_replica_that_answers_after_the_quorum_gets_no_hint() {
        let late = Duration::from_millis(400);
        let pool = setup_pool(
            MockStorage::default(),
            [Peer::Answers, Peer::AnswersAfter(late)],
            19750,
        )
        .await;

        let started = std::time::Instant::now();
        pool.coordinator
            .write("bucket", "key", b"data", "text/plain", &HashMap::new())
            .await
            .expect("the local node and node 2 make the quorum");
        assert!(
            started.elapsed() < late,
            "the client must have its answer before node 3 answers"
        );

        tokio::time::sleep(late * 2).await;
        assert!(
            pool.no_hints_anywhere().await,
            "no replica missed the write"
        );
    }

    #[tokio::test]
    async fn a_replica_that_misses_a_write_gets_one_hint_with_its_hlc() {
        let pool = setup_pool(MockStorage::default(), [Peer::Answers, Peer::Dead], 19758).await;

        let written = pool
            .coordinator
            .write("bucket", "key", b"data", "text/plain", &HashMap::new())
            .await
            .expect("the local node and node 2 make the quorum");

        let hints = pool.hints_once(3, 1).await;
        assert_eq!(hints.len(), 1, "{hints:?}");
        assert_eq!(
            (hints[0].bucket.as_str(), hints[0].key.as_str()),
            ("bucket", "key")
        );
        assert_eq!(hints[0].kind, HintKind::Written);
        assert_eq!(hints[0].hlc, written.hlc);
        assert_eq!(pool.hints.pending(NodeId(1)).await, 0);
        assert_eq!(pool.hints.pending(NodeId(2)).await, 0);
    }

    /// The local node is never its own hint target: a write it could not
    /// make is not one a retry from here would make either.
    #[tokio::test]
    async fn the_local_node_never_hints_itself() {
        let pool = setup_pool(
            MockStorage::refusing(),
            [Peer::Answers, Peer::Answers],
            19766,
        )
        .await;

        pool.coordinator
            .write("bucket", "key", b"data", "text/plain", &HashMap::new())
            .await
            .expect("nodes 2 and 3 make the quorum");
        pool.coordinator
            .delete("bucket", "key")
            .await
            .expect("nodes 2 and 3 make the quorum");

        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(pool.no_hints_anywhere().await);
    }

    /// A replica on another topology epoch refuses for that reason, and
    /// delivery would meet the same check: no hint.
    #[tokio::test]
    async fn a_replica_on_another_epoch_gets_no_hint() {
        let pool = setup_pool(
            MockStorage::default(),
            [Peer::Answers, Peer::OtherEpoch],
            19774,
        )
        .await;

        pool.coordinator
            .write("bucket", "key", b"data", "text/plain", &HashMap::new())
            .await
            .expect("the local node and node 2 make the quorum");

        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(pool.no_hints_anywhere().await);
    }

    #[tokio::test]
    async fn deletes_purges_and_multipart_replication_hint_the_replica_that_missed_them() {
        let pool = setup_pool(MockStorage::default(), [Peer::Answers, Peer::Dead], 19782).await;
        let generation = HlcTimestamp::from_record(5, 0, 1);

        let deleted = pool.coordinator.delete("bucket", "gone").await.expect("delete");
        let purged = pool
            .coordinator
            .delete_if_hlc("bucket", "cold", generation)
            .await
            .expect("purge");
        let replicated = pool
            .coordinator
            .replicate_to_remotes(
                "bucket",
                "assembled",
                b"data",
                "text/plain",
                &HashMap::new(),
            )
            .await
            .expect("node 2 makes the remote quorum");

        let mut hints = pool.hints_once(3, 3).await;
        hints.sort_by(|a, b| a.key.cmp(&b.key));
        let got: Vec<_> = hints.iter().map(|h| (h.key.as_str(), h.kind, h.hlc)).collect();
        assert_eq!(
            got,
            [
                ("assembled", HintKind::Written, replicated.hlc),
                (
                    "cold",
                    HintKind::Purge {
                        expected: generation
                    },
                    purged.hlc
                ),
                ("gone", HintKind::Deleted, deleted.hlc),
            ]
        );
        assert_eq!(pool.hints.pending(NodeId(1)).await, 0);
        assert_eq!(pool.hints.pending(NodeId(2)).await, 0);
    }
}
