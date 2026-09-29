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
//! One kind of write is exempt: a **healing write**, which carries
//! [`s4_core::FORCE_PHYSICAL_WRITE_METADATA_KEY`] and exists precisely because the
//! bytes on this node are damaged or gone. It is never a retry of an earlier
//! write, so "already applied" is not an answer it can be given.
//!
//! # Epoch Check
//!
//! Requests from a coordinator with a different topology epoch are
//! rejected with `epoch_mismatch = true`. This prevents stale
//! coordinators from writing to replicas that have moved to a new
//! topology.

use std::collections::HashMap;
use std::sync::Arc;

use s4_core::{is_force_physical_write, StorageEngine};
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
        //
        // A healing write is exempt. The tracker answers one question — "has
        // an operation with this id been applied here?" — and for a retry of
        // the same write that answer is what saves the disk a second copy. A
        // heal asks a different question: the bytes this node holds are gone
        // or rotten, and they have to be written again. Answering it from the
        // tracker would report success while the damage stays where it was,
        // which is how a shard repaired once could never be repaired twice.
        let heals_bytes = is_force_physical_write(&request.metadata);
        if !heals_bytes
            && self.idempotency.is_duplicate(operation_id, |_key| {
                // In a full implementation, this would look up the journal keyspace.
                // For now, we rely on the LRU cache + StorageEngine's own
                // idempotency (same content_hash = same dedup entry).
                false
            })
        {
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
        let head = self.storage.head_object(&request.bucket, &request.key).await.ok();
        let content_hash = head.as_ref().map(|record| record.content_hash);

        // 5. Delete from local storage engine
        let deleted = if let Some(expected) =
            request.expected_source_hlc.as_ref().map(crate::hlc::HlcTimestamp::from_proto)
        {
            self.storage
                .delete_object_if_hlc(
                    &request.bucket,
                    &request.key,
                    expected.wall_time,
                    expected.logical,
                    expected.node_id,
                )
                .await
                .map_err(|e| ClusterError::Storage(e.to_string()))?
        } else {
            self.storage
                .delete_object(&request.bucket, &request.key)
                .await
                .map_err(|e| ClusterError::Storage(e.to_string()))?;
            true
        };

        // 6. Record in idempotency cache
        self.idempotency.record(operation_id);

        // 7. Update anti-entropy Merkle tree + key registry
        if let (true, Some(ae), Some(hash)) = (deleted, &self.anti_entropy, content_hash) {
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

    /// Handle a `DetachObjectDataRequest` from the coordinator.
    ///
    /// Releases this replica's copy of the bytes and leaves the object in the
    /// namespace with an erasure-coded layout. The object stays listable; only
    /// its data moved.
    pub async fn handle_detach_object_data(
        &self,
        request: &proto::DetachObjectDataRequest,
    ) -> Result<proto::DetachObjectDataResponse, ClusterError> {
        if request.topology_epoch != 0 && request.topology_epoch != self.topology_epoch {
            warn!(
                local_epoch = self.topology_epoch,
                request_epoch = request.topology_epoch,
                "epoch mismatch on replica detach"
            );
            return Ok(proto::DetachObjectDataResponse {
                success: false,
                error: format!(
                    "epoch mismatch: local={}, request={}",
                    self.topology_epoch, request.topology_epoch
                ),
                detached: false,
                epoch_mismatch: true,
            });
        }

        let operation_id = parse_operation_id(&request.operation_id)?;

        // The generation guard is what makes this safe against a concurrent
        // overwrite, so a request without one is malformed rather than lenient.
        let Some(expected) =
            request.expected_source_hlc.as_ref().map(crate::hlc::HlcTimestamp::from_proto)
        else {
            return Ok(proto::DetachObjectDataResponse {
                success: false,
                error: "detach requires expected_source_hlc".to_string(),
                detached: false,
                epoch_mismatch: false,
            });
        };

        // Captured before the change: anti-entropy tracks locally stored bytes,
        // and after the detach this replica no longer stores any.
        let content_hash = self
            .storage
            .head_object(&request.bucket, &request.key)
            .await
            .ok()
            .map(|record| record.content_hash);

        let detached = self
            .storage
            .detach_object_data_if_hlc(
                &request.bucket,
                &request.key,
                expected.wall_time,
                expected.logical,
                expected.node_id,
                s4_core::types::ObjectLayout::ErasureCoded {
                    erasure_set_id: request.erasure_set_id,
                    topology_epoch: request.ec_topology_epoch,
                },
            )
            .await
            .map_err(|e| ClusterError::Storage(e.to_string()))?;

        if detached {
            self.idempotency.record(operation_id);
        }

        // Hot anti-entropy must stop expecting these bytes here. Integrity of an
        // erasure-coded object is the EC scrubber's job, and leaving the key
        // registered would have the hot repair path chase a copy that no node
        // holds any more.
        if let (true, Some(ae), Some(hash)) = (detached, &self.anti_entropy, content_hash) {
            ae.unregister_key(&request.bucket, &request.key, hash).await;
        }

        debug!(
            bucket = %request.bucket,
            key = %request.key,
            detached,
            operation_id = %uuid::Uuid::from_u128(operation_id),
            "replica detach completed"
        );

        Ok(proto::DetachObjectDataResponse {
            success: true,
            error: String::new(),
            detached,
            epoch_mismatch: false,
        })
    }

    /// Handle an `AttachObjectDataRequest` from the coordinator.
    ///
    /// Writes this replica's copy of the bytes back and takes the object out of
    /// the erasure-coded layout. The reverse of
    /// [`handle_detach_object_data`](Self::handle_detach_object_data), used when
    /// auto-tiering promotes an object that became hot again.
    pub async fn handle_attach_object_data(
        &self,
        request: &proto::AttachObjectDataRequest,
    ) -> Result<proto::AttachObjectDataResponse, ClusterError> {
        if request.topology_epoch != 0 && request.topology_epoch != self.topology_epoch {
            warn!(
                local_epoch = self.topology_epoch,
                request_epoch = request.topology_epoch,
                "epoch mismatch on replica attach"
            );
            return Ok(proto::AttachObjectDataResponse {
                success: false,
                error: format!(
                    "epoch mismatch: local={}, request={}",
                    self.topology_epoch, request.topology_epoch
                ),
                attached: false,
                epoch_mismatch: true,
            });
        }

        let operation_id = parse_operation_id(&request.operation_id)?;

        // Same reasoning as the detach: without a generation guard the request
        // could overwrite a client's newer write with decoded stale bytes, so a
        // request that omits it is malformed rather than lenient.
        let Some(expected) =
            request.expected_source_hlc.as_ref().map(crate::hlc::HlcTimestamp::from_proto)
        else {
            return Ok(proto::AttachObjectDataResponse {
                success: false,
                error: "attach requires expected_source_hlc".to_string(),
                attached: false,
                epoch_mismatch: false,
            });
        };

        let attached = self
            .storage
            .attach_object_data_if_hlc(
                &request.bucket,
                &request.key,
                expected.wall_time,
                expected.logical,
                expected.node_id,
                &request.data,
            )
            .await
            .map_err(|e| ClusterError::Storage(e.to_string()))?;

        if attached {
            self.idempotency.record(operation_id);
        }

        // Hot anti-entropy has to expect these bytes here again. Read after the
        // change, not before: the hash is the one the storage engine settled on
        // for the blob it wrote, and registering anything else would have the
        // repair path chase a copy nobody holds.
        if attached {
            if let Some(ae) = &self.anti_entropy {
                match self.storage.head_object(&request.bucket, &request.key).await {
                    Ok(record) => {
                        ae.register_key(&request.bucket, &request.key, record.content_hash).await;
                    }
                    Err(error) => warn!(
                        bucket = %request.bucket,
                        key = %request.key,
                        error = %error,
                        "attached object could not be re-registered with anti-entropy"
                    ),
                }
            }
        }

        debug!(
            bucket = %request.bucket,
            key = %request.key,
            attached,
            operation_id = %uuid::Uuid::from_u128(operation_id),
            "replica attach completed"
        );

        Ok(proto::AttachObjectDataResponse {
            success: true,
            error: String::new(),
            attached,
            epoch_mismatch: false,
        })
    }

    /// Handle a `RestoreNamespaceEntryRequest` from the coordinator.
    ///
    /// Repair path: writes back the namespace entry of an erasure-coded object
    /// that lost one. An entry the replica already holds always wins, because
    /// the index is live truth and the manifest it was rebuilt from may be old.
    pub async fn handle_restore_namespace_entry(
        &self,
        request: &proto::RestoreNamespaceEntryRequest,
    ) -> Result<proto::RestoreNamespaceEntryResponse, ClusterError> {
        if request.topology_epoch != 0 && request.topology_epoch != self.topology_epoch {
            warn!(
                local_epoch = self.topology_epoch,
                request_epoch = request.topology_epoch,
                "epoch mismatch on replica namespace restore"
            );
            return Ok(proto::RestoreNamespaceEntryResponse {
                success: false,
                error: format!(
                    "epoch mismatch: local={}, request={}",
                    self.topology_epoch, request.topology_epoch
                ),
                restored: false,
                epoch_mismatch: true,
            });
        }

        let record: s4_core::types::IndexRecord =
            bincode::deserialize(&request.record).map_err(|error| {
                ClusterError::Storage(format!("namespace entry is not a valid record: {error}"))
            })?;

        let restored = self
            .storage
            .restore_erasure_coded_entry(&request.bucket, &request.key, record)
            .await
            .map_err(|e| ClusterError::Storage(e.to_string()))?;

        debug!(
            bucket = %request.bucket,
            key = %request.key,
            restored,
            "replica namespace restore completed"
        );

        Ok(proto::RestoreNamespaceEntryResponse {
            success: true,
            error: String::new(),
            restored,
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

    use s4_core::FORCE_PHYSICAL_WRITE_METADATA_KEY;

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

    fn write_request(
        operation_id: u128,
        metadata: HashMap<String, String>,
    ) -> proto::WriteBlobRequest {
        proto::WriteBlobRequest {
            bucket: "__system__".to_string(),
            key: "shard".to_string(),
            data: b"shard bytes".to_vec(),
            content_hash: vec![0u8; 32],
            content_type: "application/octet-stream".to_string(),
            metadata,
            hlc: None,
            version_id: None,
            coordinator_id: Vec::new(),
            operation_id: operation_id.to_be_bytes().to_vec(),
            topology_epoch: 0,
        }
    }

    /// The contract the tracker exists for: a coordinator that resends the
    /// same write after a timeout must not cost a second copy on disk.
    #[tokio::test]
    async fn a_resent_write_is_applied_once() {
        let storage = Arc::new(MockStorage::default());
        let handler = ReplicaWriteHandler::new(
            Arc::clone(&storage) as Arc<dyn StorageEngine>,
            Arc::new(IdempotencyTracker::new()),
            0,
        );
        let request = write_request(7, HashMap::new());

        for _ in 0..2 {
            let response = handler.handle_write(&request).await.unwrap();
            assert!(response.success);
        }

        assert_eq!(storage.puts(), 1, "a retry of one write wrote twice");
    }

    /// And the exception that keeps a repair honest: a shard rebuilt a second
    /// time carries the same work to the same node, and the tracker must not
    /// answer for the disk. Without this, a shard that rots twice is repaired
    /// once and reported as repaired twice.
    #[tokio::test]
    async fn a_healing_write_is_applied_even_when_its_operation_id_is_known() {
        let storage = Arc::new(MockStorage::default());
        let handler = ReplicaWriteHandler::new(
            Arc::clone(&storage) as Arc<dyn StorageEngine>,
            Arc::new(IdempotencyTracker::new()),
            0,
        );
        let healing = HashMap::from([(
            FORCE_PHYSICAL_WRITE_METADATA_KEY.to_string(),
            "true".to_string(),
        )]);
        let request = write_request(7, healing);

        for _ in 0..2 {
            let response = handler.handle_write(&request).await.unwrap();
            assert!(response.success);
        }

        assert_eq!(
            storage.puts(),
            2,
            "the second heal was answered from the idempotency tracker instead of the disk"
        );
    }

    #[test]
    fn epoch_check_passes_for_matching_epoch() {
        let handler = ReplicaWriteHandler::new(
            Arc::new(MockStorage::default()),
            Arc::new(IdempotencyTracker::new()),
            5,
        );
        assert!(handler.check_epoch(5).is_none());
        assert!(handler.check_epoch(0).is_none()); // 0 = epoch not set
    }

    #[test]
    fn epoch_check_fails_for_mismatched_epoch() {
        let handler = ReplicaWriteHandler::new(
            Arc::new(MockStorage::default()),
            Arc::new(IdempotencyTracker::new()),
            5,
        );
        let resp = handler.check_epoch(3).unwrap();
        assert!(!resp.success);
        assert!(resp.epoch_mismatch);
    }

    /// Minimal mock for unit tests that don't need actual storage.
    ///
    /// Counts the writes that reached the engine, because that is the question
    /// the idempotency tests ask: not what the handler answered, but what it
    /// actually wrote.
    #[derive(Default)]
    struct MockStorage {
        puts: std::sync::atomic::AtomicUsize,
    }

    impl MockStorage {
        fn puts(&self) -> usize {
            self.puts.load(std::sync::atomic::Ordering::Relaxed)
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
            self.puts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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
