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

//! Replica-side write handler for quorum writes.
//!
//! Processes `WriteBlobRequest` and `DeleteBlobRequest` from the
//! coordinator. Each successful response is a **durable ACK**: the data
//! has been written to the volume file, the metadata index updated, and
//! fsync completed before the response is sent.
//!
//! # Idempotency
//!
//! Every request carries an `operation_id`. The handler checks the
//! [`IdempotencyTracker`] before writing. If the operation was already
//! applied, it returns success without re-writing.
//!
//! # Epoch Check
//!
//! Requests from a coordinator with a different topology epoch are
//! rejected with `epoch_mismatch = true`. This prevents stale
//! coordinators from writing to replicas that have moved to a new
//! topology.

use std::collections::HashMap;
use std::sync::Arc;

use s4_core::StorageEngine;
use tracing::{debug, warn};

use crate::error::ClusterError;
use crate::idempotency::IdempotencyTracker;
use crate::repair::AntiEntropyService;
use crate::rpc::proto;

/// Handles write operations on a replica node.
///
/// The handler holds references to the local storage engine and the
/// idempotency tracker. It is shared across all gRPC handler tasks.
pub struct ReplicaWriteHandler {
    storage: Arc<dyn StorageEngine>,
    idempotency: Arc<IdempotencyTracker>,
    topology_epoch: u64,
    anti_entropy: Option<Arc<AntiEntropyService>>,
}

impl ReplicaWriteHandler {
    /// Create a new replica write handler.
    pub fn new(
        storage: Arc<dyn StorageEngine>,
        idempotency: Arc<IdempotencyTracker>,
        topology_epoch: u64,
    ) -> Self {
        Self {
            storage,
            idempotency,
            topology_epoch,
            anti_entropy: None,
        }
    }

    /// Attach an anti-entropy service for Merkle tree updates on writes.
    pub fn with_anti_entropy(mut self, ae: Arc<AntiEntropyService>) -> Self {
        self.anti_entropy = Some(ae);
        self
    }

    /// Handle a `WriteBlobRequest` from the coordinator.
    ///
    /// # Durable ACK Boundary
    ///
    /// Success means ALL of the following are complete:
    /// 1. Blob written to volume file
    /// 2. IndexRecord + JournalEntry + DedupEntry committed in atomic batch
    /// 3. fsync completed (data on disk, not in page cache)
    pub async fn handle_write(
        &self,
        request: &proto::WriteBlobRequest,
    ) -> Result<proto::WriteBlobResponse, ClusterError> {
        // 1. Epoch check
        if let Some(err) = self.check_epoch(request.topology_epoch) {
            return Ok(err);
        }

        // 2. Parse operation_id
        let operation_id = parse_operation_id(&request.operation_id)?;

        // 3. Idempotency check
        if self.idempotency.is_duplicate(operation_id, |_key| {
            // In a full implementation, this would look up the journal keyspace.
            // For now, we rely on the LRU cache + StorageEngine's own
            // idempotency (same content_hash = same dedup entry).
            false
        }) {
            debug!(operation_id = %uuid::Uuid::from_u128(operation_id), "duplicate write, skipping");
            return Ok(proto::WriteBlobResponse {
                success: true,
                error: String::new(),
                local_sequence: 0,
                epoch_mismatch: false,
            });
        }

        // 4. Defense-in-depth: ensure bucket marker exists locally.
        // Handles the race where a PutObject arrives at a replica before the
        // bucket marker replication (from CreateBucket) completes via hinted handoff.
        if request.bucket != "__system__" {
            let marker_key = format!("__s4_bucket_marker_{}", request.bucket);
            if self.storage.head_object("__system__", &marker_key).await.is_err() {
                debug!(
                    bucket = %request.bucket,
                    "auto-creating bucket marker on replica (defense-in-depth)"
                );
                let empty_meta = HashMap::new();
                let _ = self
                    .storage
                    .put_object(
                        "__system__",
                        &marker_key,
                        b"1",
                        "application/octet-stream",
                        &empty_meta,
                    )
                    .await;
            }
        }

        // 5. Parse metadata from the request, injecting HLC fields as reserved
        // metadata keys so that put_object stamps the IndexRecord atomically
        // in the same batch (no race with concurrent writes).
        let mut metadata: HashMap<String, String> = request.metadata.clone();
        let content_type = &request.content_type;

        if let Some(hlc_proto) = &request.hlc {
            let hlc = crate::hlc::HlcTimestamp::from_proto(hlc_proto);
            metadata.insert("_s4_hlc_wall".to_string(), hlc.wall_time.to_string());
            metadata.insert("_s4_hlc_logical".to_string(), hlc.logical.to_string());
            metadata.insert("_s4_hlc_node_id".to_string(), hlc.node_id.to_string());
            metadata.insert("_s4_operation_id".to_string(), operation_id.to_string());
        }

        // 6. Write to local storage engine (volume + index + journal + fsync)
        // HLC is stamped atomically via the reserved _s4_* metadata keys.
        let etag = self
            .storage
            .put_object(
                &request.bucket,
                &request.key,
                &request.data,
                content_type,
                &metadata,
            )
            .await
            .map_err(|e| ClusterError::Storage(e.to_string()))?;

        // 7. Record in idempotency cache
        self.idempotency.record(operation_id);

        // 8. Update anti-entropy Merkle tree + key registry
        if let Some(ae) = &self.anti_entropy {
            let content_hash: [u8; 32] =
                request.content_hash.as_slice().try_into().unwrap_or([0u8; 32]);
            ae.register_key(&request.bucket, &request.key, content_hash).await;
        }

        debug!(
            bucket = %request.bucket,
            key = %request.key,
            etag = %etag,
            operation_id = %uuid::Uuid::from_u128(operation_id),
            "replica write completed"
        );

        Ok(proto::WriteBlobResponse {
            success: true,
            error: String::new(),
            local_sequence: 0,
            epoch_mismatch: false,
        })
    }

    /// Handle a `DeleteBlobRequest` from the coordinator.
    pub async fn handle_delete(
        &self,
        request: &proto::DeleteBlobRequest,
    ) -> Result<proto::DeleteBlobResponse, ClusterError> {
        // 1. Epoch check
        if let Some(err) = self.check_delete_epoch(request.topology_epoch) {
            return Ok(err);
        }

        // 2. Parse operation_id
        let operation_id = parse_operation_id(&request.operation_id)?;

        // 3. Idempotency check
        if self.idempotency.is_duplicate(operation_id, |_key| false) {
            debug!(operation_id = %uuid::Uuid::from_u128(operation_id), "duplicate delete, skipping");
            return Ok(proto::DeleteBlobResponse {
                success: true,
                error: String::new(),
                local_sequence: 0,
                epoch_mismatch: false,
            });
        }

        // 4. Capture content_hash before delete for Merkle tree update
        let content_hash = self
            .storage
            .head_object(&request.bucket, &request.key)
            .await
            .map(|r| r.content_hash)
            .ok();

        // 5. Delete from local storage engine
        self.storage
            .delete_object(&request.bucket, &request.key)
            .await
            .map_err(|e| ClusterError::Storage(e.to_string()))?;

        // 6. Record in idempotency cache
        self.idempotency.record(operation_id);

        // 7. Update anti-entropy Merkle tree + key registry
        if let (Some(ae), Some(hash)) = (&self.anti_entropy, content_hash) {
            ae.unregister_key(&request.bucket, &request.key, hash).await;
        }

        debug!(
            bucket = %request.bucket,
            key = %request.key,
            operation_id = %uuid::Uuid::from_u128(operation_id),
            "replica delete completed"
        );

        Ok(proto::DeleteBlobResponse {
            success: true,
            error: String::new(),
            local_sequence: 0,
            epoch_mismatch: false,
        })
    }

    fn check_epoch(&self, request_epoch: u64) -> Option<proto::WriteBlobResponse> {
        if request_epoch != 0 && request_epoch != self.topology_epoch {
            warn!(
                local_epoch = self.topology_epoch,
                request_epoch, "epoch mismatch on replica write"
            );
            return Some(proto::WriteBlobResponse {
                success: false,
                error: format!(
                    "epoch mismatch: local={}, request={}",
                    self.topology_epoch, request_epoch
                ),
                local_sequence: 0,
                epoch_mismatch: true,
            });
        }
        None
    }

    fn check_delete_epoch(&self, request_epoch: u64) -> Option<proto::DeleteBlobResponse> {
        if request_epoch != 0 && request_epoch != self.topology_epoch {
            warn!(
                local_epoch = self.topology_epoch,
                request_epoch, "epoch mismatch on replica delete"
            );
            return Some(proto::DeleteBlobResponse {
                success: false,
                error: format!(
                    "epoch mismatch: local={}, request={}",
                    self.topology_epoch, request_epoch
                ),
                local_sequence: 0,
                epoch_mismatch: true,
            });
        }
        None
    }
}

/// Parse a 16-byte operation_id from the protobuf bytes field.
fn parse_operation_id(bytes: &[u8]) -> Result<u128, ClusterError> {
    if bytes.len() != 16 {
        return Err(ClusterError::Codec(format!(
            "operation_id must be 16 bytes, got {}",
            bytes.len()
        )));
    }
    Ok(u128::from_be_bytes(
        bytes.try_into().expect("length checked above"),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_operation_id_valid() {
        let id: u128 = 42;
        let bytes = id.to_be_bytes();
        assert_eq!(parse_operation_id(&bytes).unwrap(), 42);
    }

    #[test]
    fn parse_operation_id_wrong_length() {
        assert!(parse_operation_id(&[1, 2, 3]).is_err());
    }

    #[test]
    fn epoch_check_passes_for_matching_epoch() {
        let handler = ReplicaWriteHandler::new(
            Arc::new(MockStorage),
            Arc::new(IdempotencyTracker::new()),
            5,
        );
        assert!(handler.check_epoch(5).is_none());
        assert!(handler.check_epoch(0).is_none()); // 0 = epoch not set
    }

    #[test]
    fn epoch_check_fails_for_mismatched_epoch() {
        let handler = ReplicaWriteHandler::new(
            Arc::new(MockStorage),
            Arc::new(IdempotencyTracker::new()),
            5,
        );
        let resp = handler.check_epoch(3).unwrap();
        assert!(!resp.success);
        assert!(resp.epoch_mismatch);
    }

    /// Minimal mock for unit tests that don't need actual storage.
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
