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

use s4_core::{is_force_physical_write, KeyVersion, PutOutcome, StorageEngine};
use tracing::{debug, warn};

use s4_core::types::{is_bucket_metadata, is_cluster_control};

use crate::error::ClusterError;
use crate::hlc::HlcTimestamp;
use crate::iam_lease::{Admitted, IamLeaseKeeper, LeaseFence, Refusal};
use crate::idempotency::IdempotencyTracker;
use crate::metadata_placement::StageCell;
use crate::rpc::proto;

/// Handles write operations on a replica node.
///
/// The handler holds references to the local storage engine and the
/// idempotency tracker. It is shared across all gRPC handler tasks.
pub struct ReplicaWriteHandler {
    storage: Arc<dyn StorageEngine>,
    idempotency: Arc<IdempotencyTracker>,
    topology_epoch: u64,
    placement_generation: crate::placement_fence::PlacementFloor,
    /// This node's stage while the pool moves its bucket metadata: what the
    /// writes of bucket metadata are fenced by.
    bucket_metadata_stage: Option<StageCell>,
    /// This node's grants of the lease of IAM changes: what the writes of the
    /// cluster's control state are fenced by.
    iam_lease: Option<Arc<IamLeaseKeeper>>,
}

/// The HLC of a bucket marker a replica makes up for a write into a bucket it
/// holds no marker of: older than any HLC a node issues, and from no node.
pub(crate) const MADE_UP_MARKER_HLC: crate::hlc::HlcTimestamp = crate::hlc::HlcTimestamp {
    wall_time: 1,
    logical: 0,
    node_id: 0,
};

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
            placement_generation: crate::placement_fence::PlacementFloor::default(),
            bucket_metadata_stage: None,
            iam_lease: None,
        }
    }

    /// Set the minimum pool placement generation admitted by this handler.
    /// Zero preserves legacy admission.
    pub fn with_placement_generation(mut self, minimum: u64) -> Self {
        self.placement_generation = crate::placement_fence::PlacementFloor::new(minimum);
        self
    }

    /// The handler admitting what `floor` admits, as it rises when the
    /// pool's move begins.
    pub fn with_placement_floor(mut self, floor: crate::placement_fence::PlacementFloor) -> Self {
        self.placement_generation = floor;
        self
    }

    /// This handler fencing the writes of the cluster's control state by the
    /// lease of IAM changes `keeper` keeps (plan CLUSTER-IAM-REPLICATION.md,
    /// decision 3).
    pub fn with_iam_lease(mut self, keeper: Arc<IamLeaseKeeper>) -> Self {
        self.iam_lease = Some(keeper);
        self
    }

    /// Whether this replica takes a client's write of `bucket/key` at `hlc`
    /// under `fence`: a write of the cluster's control state needs the
    /// current lease of IAM changes. A repair copy settles a version the pool
    /// already ordered, and is not fenced. Hold the answer until the write is
    /// in storage.
    async fn admit(
        &self,
        bucket: &str,
        key: &str,
        repair_copy: bool,
        fence: Option<&proto::IamLeaseFence>,
        hlc: Option<&proto::HybridTimestamp>,
    ) -> Result<Option<Admitted>, Refusal> {
        let Some(keeper) = &self.iam_lease else {
            return Ok(None);
        };
        if repair_copy || !is_cluster_control(bucket, key) {
            return Ok(None);
        }
        let fence = fence.and_then(LeaseFence::from_proto);
        let hlc = hlc.map(HlcTimestamp::from_proto).unwrap_or_else(HlcTimestamp::zero);
        keeper.admit(fence.as_ref(), hlc).await.map(Some)
    }

    /// This handler fencing the writes of bucket metadata by the stage in
    /// `stage`, while the pool moves them (plan
    /// BUCKET-METADATA-REPLICATION.md, decision 6).
    pub fn with_bucket_metadata_stage(mut self, stage: StageCell) -> Self {
        self.bucket_metadata_stage = Some(stage);
        self
    }

    /// Why this replica refuses a write of `bucket/key` placed by
    /// `request_stage`, if it does: the key is bucket metadata, the pool
    /// moves them, and the write is a client's from a stage too old.
    fn fenced(
        &self,
        bucket: &str,
        key: &str,
        request_stage: u32,
        repair_copy: bool,
    ) -> Option<String> {
        let stage = self.bucket_metadata_stage.as_ref()?.stage();
        if !is_bucket_metadata(bucket, key) || !stage.fences(request_stage, repair_copy) {
            return None;
        }
        crate::cluster_metrics::record_bucket_metadata_fenced_write();
        warn!(
            %bucket,
            %key,
            request_stage,
            stage = %stage,
            "refused a write of bucket metadata placed by a stage too old: its coordinator \
             is of an earlier release or behind the pool"
        );
        Some(format!(
            "bucket metadata stage fenced: this replica is at stage {}, the write was placed \
             by stage {request_stage}",
            stage.number()
        ))
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
        crate::placement_fence::check(
            self.placement_generation.get(),
            request.placement_generation,
            &request.bucket,
            &request.key,
            request.repair_copy,
            request.hlc.as_ref(),
        )?;
        // 1. Epoch check
        if let Some(err) = self.check_epoch(request.topology_epoch) {
            return Ok(err);
        }
        if let Some(error) = self.fenced(
            &request.bucket,
            &request.key,
            request.bucket_metadata_stage,
            request.repair_copy,
        ) {
            return Ok(proto::WriteBlobResponse {
                success: false,
                error,
                local_sequence: 0,
                epoch_mismatch: false,
                superseded_by: None,
                stage_fenced: true,
                lease_fenced: false,
            });
        }
        // A copy checked before it is stored: one damaged on its way is never
        // taken for the version it claims to be.
        if let Some(error) = super::content_mismatch(request) {
            warn!(bucket = %request.bucket, key = %request.key, "refused a copy whose bytes do not match its SHA-256");
            return Ok(proto::WriteBlobResponse {
                success: false,
                error,
                local_sequence: 0,
                epoch_mismatch: false,
                superseded_by: None,
                stage_fenced: false,
                lease_fenced: false,
            });
        }
        // Held until the write is in storage: this node grants no later epoch
        // of the lease before it lands.
        let _admitted = match self
            .admit(
                &request.bucket,
                &request.key,
                request.repair_copy,
                request.iam_lease.as_ref(),
                request.hlc.as_ref(),
            )
            .await
        {
            Ok(admitted) => admitted,
            Err(refusal) => {
                return Ok(proto::WriteBlobResponse {
                    success: false,
                    lease_fenced: matches!(refusal, Refusal::Fenced(_)),
                    error: refusal.to_string(),
                    local_sequence: 0,
                    epoch_mismatch: false,
                    superseded_by: None,
                    stage_fenced: false,
                })
            }
        };

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
                superseded_by: None,
                stage_fenced: false,
                lease_fenced: false,
            });
        }

        // 4. Defense-in-depth: ensure bucket marker exists locally.
        // Handles the race where a PutObject arrives at a replica before the
        // bucket marker replication (from CreateBucket) completes via hinted handoff.
        // A marker this replica holds as a tombstone is not made up again: the
        // bucket was deleted, and a bucket created again since comes with a
        // marker newer than the tombstone anyway. The marker made up here is
        // the oldest version there can be (`MADE_UP_MARKER_HLC`): every marker
        // CreateBucket writes and every tombstone DeleteBucket leaves is newer,
        // so it never outvotes them in a quorum read or a listing. Written
        // without an HLC it would be read as made up at its write time, newer
        // than a delete this replica missed.
        if request.bucket != "__system__" {
            let marker_key = format!("__s4_bucket_marker_{}", request.bucket);
            if !matches!(
                self.storage.key_version("__system__", &marker_key).await,
                Ok(KeyVersion::Live(_) | KeyVersion::Tombstone(_))
            ) {
                debug!(
                    bucket = %request.bucket,
                    "auto-creating bucket marker on replica (defense-in-depth)"
                );
                let oldest = HashMap::from([
                    (
                        "_s4_hlc_wall".to_string(),
                        MADE_UP_MARKER_HLC.wall_time.to_string(),
                    ),
                    (
                        "_s4_hlc_logical".to_string(),
                        MADE_UP_MARKER_HLC.logical.to_string(),
                    ),
                    (
                        "_s4_hlc_node_id".to_string(),
                        MADE_UP_MARKER_HLC.node_id.to_string(),
                    ),
                ]);
                let _ = self
                    .storage
                    .put_object(
                        "__system__",
                        &marker_key,
                        b"1",
                        "application/octet-stream",
                        &oldest,
                    )
                    .await;
            }
        }

        // 5. Parse metadata from the request, injecting HLC fields as reserved
        // metadata keys so that put_object stamps the IndexRecord atomically
        // in the same batch (no race with concurrent writes).
        // The ETag a repair copies travels in its own field, never in the
        // metadata: a key of that name there came from the object's stored
        // metadata, and is neither an instruction nor the client's.
        let mut metadata: HashMap<String, String> = request.metadata.clone();
        metadata.remove(s4_core::REPAIRED_ETAG_METADATA_KEY);
        let content_type = &request.content_type;

        if let Some(hlc_proto) = &request.hlc {
            let hlc = crate::hlc::HlcTimestamp::from_proto(hlc_proto);
            metadata.insert("_s4_hlc_wall".to_string(), hlc.wall_time.to_string());
            metadata.insert("_s4_hlc_logical".to_string(), hlc.logical.to_string());
            metadata.insert("_s4_hlc_node_id".to_string(), hlc.node_id.to_string());
            metadata.insert("_s4_operation_id".to_string(), operation_id.to_string());
            if !request.etag.is_empty() {
                metadata.insert(
                    s4_core::REPAIRED_ETAG_METADATA_KEY.to_string(),
                    request.etag.clone(),
                );
            }
        }

        // 6. Write to local storage engine (volume + index + journal + fsync)
        // HLC is stamped atomically via the reserved _s4_* metadata keys. A
        // newer version of the key outranks the write; it is reported, so
        // that the coordinator can raise a client's write above it.
        let outcome = self
            .storage
            .put_object_outcome(
                &request.bucket,
                &request.key,
                &request.data,
                content_type,
                &metadata,
            )
            .await
            .map_err(|e| ClusterError::Storage(e.to_string()))?;
        let superseded_by = match &outcome {
            PutOutcome::Stored { .. } => None,
            PutOutcome::Superseded { by } => Some(HlcTimestamp::of_record(by).to_proto()),
        };

        // 7. Record in idempotency cache
        self.idempotency.record(operation_id);

        debug!(
            bucket = %request.bucket,
            key = %request.key,
            superseded = superseded_by.is_some(),
            operation_id = %uuid::Uuid::from_u128(operation_id),
            "replica write completed"
        );

        Ok(proto::WriteBlobResponse {
            success: true,
            error: String::new(),
            local_sequence: 0,
            epoch_mismatch: false,
            superseded_by,
            stage_fenced: false,
            lease_fenced: false,
        })
    }

    /// Handle a `DeleteBlobRequest` from the coordinator.
    pub async fn handle_delete(
        &self,
        request: &proto::DeleteBlobRequest,
    ) -> Result<proto::DeleteBlobResponse, ClusterError> {
        crate::placement_fence::check(
            self.placement_generation.get(),
            request.placement_generation,
            &request.bucket,
            &request.key,
            request.repair_copy,
            request.hlc.as_ref(),
        )?;
        // 1. Epoch check
        if let Some(err) = self.check_delete_epoch(request.topology_epoch) {
            return Ok(err);
        }
        if let Some(error) = self.fenced(
            &request.bucket,
            &request.key,
            request.bucket_metadata_stage,
            request.repair_copy,
        ) {
            return Ok(proto::DeleteBlobResponse {
                success: false,
                error,
                local_sequence: 0,
                epoch_mismatch: false,
                superseded_by: None,
                stage_fenced: true,
                lease_fenced: false,
            });
        }
        // A purge removes a generation the pool settled: no change of IAM.
        let _admitted = if request.expected_source_hlc.is_some() {
            None
        } else {
            match self
                .admit(
                    &request.bucket,
                    &request.key,
                    request.repair_copy,
                    request.iam_lease.as_ref(),
                    request.hlc.as_ref(),
                )
                .await
            {
                Ok(admitted) => admitted,
                Err(refusal) => {
                    return Ok(proto::DeleteBlobResponse {
                        success: false,
                        lease_fenced: matches!(refusal, Refusal::Fenced(_)),
                        error: refusal.to_string(),
                        local_sequence: 0,
                        epoch_mismatch: false,
                        superseded_by: None,
                        stage_fenced: false,
                    })
                }
            }
        };

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
                superseded_by: None,
                stage_fenced: false,
                lease_fenced: false,
            });
        }

        // 4. Delete from local storage engine: one generation exactly when the
        // request names it (a purge), otherwise by leaving a tombstone at the
        // delete's HLC, which a write older than the delete cannot replace —
        // for the keys that keep tombstones. The others, and a request
        // without an HLC, which no coordinator of this binary sends, remove
        // the record as deletes did before tombstones.
        let hlc = request.hlc.as_ref().map(crate::hlc::HlcTimestamp::from_proto);
        let mut superseded_by = None;
        match (
            request.expected_source_hlc.as_ref().map(crate::hlc::HlcTimestamp::from_proto),
            hlc,
        ) {
            (Some(expected), _) => {
                self.storage
                    .delete_object_if_hlc(
                        &request.bucket,
                        &request.key,
                        expected.wall_time,
                        expected.logical,
                        expected.node_id,
                    )
                    .await
                    .map_err(|e| ClusterError::Storage(e.to_string()))?;
            }
            (None, Some(hlc))
                if hlc.wall_time > 0
                    && super::delete_leaves_tombstone(&request.bucket, &request.key) =>
            {
                superseded_by =
                    super::leave_tombstone(&*self.storage, &request.bucket, &request.key, hlc)
                        .await
                        .map_err(|e| ClusterError::Storage(e.to_string()))?;
            }
            (None, _) => {
                self.storage
                    .delete_object(&request.bucket, &request.key)
                    .await
                    .map_err(|e| ClusterError::Storage(e.to_string()))?;
            }
        }

        // 5. Record in idempotency cache
        self.idempotency.record(operation_id);

        debug!(
            bucket = %request.bucket,
            key = %request.key,
            operation_id = %uuid::Uuid::from_u128(operation_id),
            superseded = superseded_by.is_some(),
            "replica delete completed"
        );

        Ok(proto::DeleteBlobResponse {
            success: true,
            error: String::new(),
            local_sequence: 0,
            epoch_mismatch: false,
            superseded_by: superseded_by.map(HlcTimestamp::to_proto),
            stage_fenced: false,
            lease_fenced: false,
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
        crate::placement_fence::check(
            self.placement_generation.get(),
            request.placement_generation,
            &request.bucket,
            &request.key,
            false,
            None,
        )?;
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
        crate::placement_fence::check(
            self.placement_generation.get(),
            request.placement_generation,
            &request.bucket,
            &request.key,
            false,
            None,
        )?;
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
        crate::placement_fence::check(
            self.placement_generation.get(),
            request.placement_generation,
            &request.bucket,
            &request.key,
            false,
            None,
        )?;
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
                superseded_by: None,
                stage_fenced: false,
                lease_fenced: false,
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
                superseded_by: None,
                stage_fenced: false,
                lease_fenced: false,
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
            verify_content: false,
            placement_generation: 0,
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
            etag: String::new(),
            bucket_metadata_stage: 0,
            repair_copy: false,
            iam_lease: None,
        }
    }

    /// A copy sent to be checked is stored only when its bytes match the
    /// SHA-256 it came with; one without a whole-object hash, or not sent to
    /// be checked, is stored as before.
    #[tokio::test]
    async fn a_checked_copy_whose_bytes_differ_from_its_hash_is_refused_before_storage() {
        use sha2::Digest;
        let storage = Arc::new(MockStorage::default());
        let handler =
            ReplicaWriteHandler::new(storage.clone(), Arc::new(IdempotencyTracker::new()), 0);
        let mut request = write_request(90, HashMap::new());
        request.verify_content = true;
        request.content_hash = sha2::Sha256::digest(b"other bytes").to_vec();
        let refused = handler.handle_write(&request).await.unwrap();
        assert!(!refused.success);
        assert!(
            refused.error.starts_with(crate::replica::CHECKSUM_MISMATCH),
            "{}",
            refused.error
        );
        assert_eq!(storage.puts(), 0);

        for (operation, verify, hash) in [
            (91, true, sha2::Sha256::digest(b"shard bytes").to_vec()),
            (92, true, vec![0u8; 32]),
            (93, false, sha2::Sha256::digest(b"other bytes").to_vec()),
        ] {
            let mut request = write_request(operation, HashMap::new());
            request.verify_content = verify;
            request.content_hash = hash;
            assert!(handler.handle_write(&request).await.unwrap().success);
        }
        assert_eq!(storage.puts(), 3);
    }

    #[tokio::test]
    async fn placement_fence_precedes_idempotency_and_requires_repair_hlc() {
        let metrics = crate::test_metrics::TestMetrics::install();
        let storage = Arc::new(MockStorage::default());
        let tracker = Arc::new(IdempotencyTracker::new());
        let handler = ReplicaWriteHandler::new(storage.clone(), tracker.clone(), 0)
            .with_placement_generation(2);
        tracker.record(77);
        let mut request = write_request(77, HashMap::new());
        for repair in [false, true] {
            request.repair_copy = repair;
            assert!(matches!(
                handler.handle_write(&request).await,
                Err(ClusterError::PlacementGenerationFenced { .. })
            ));
        }
        request.repair_copy = false;
        request.placement_generation = 2;
        assert!(handler.handle_write(&request).await.unwrap().success);
        request.operation_id = 78u128.to_be_bytes().to_vec();
        request.placement_generation = 0;
        request.repair_copy = true;
        request.hlc = Some(HlcTimestamp::from_record(10, 0, 7).to_proto());
        assert!(handler.handle_write(&request).await.unwrap().success);
        assert_eq!(
            metrics.value("s4_pool_generation_fenced_writes_total", &[]),
            Some(2.0)
        );
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

    /// The ETag a repair copies arrives in its own field and is kept only
    /// with an HLC; a key of that name in the metadata — stored metadata a
    /// copy carried along — is neither kept as the client's metadata nor
    /// taken as the ETag.
    #[tokio::test]
    async fn the_etag_of_a_repair_comes_only_from_its_own_field() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Arc::new(
            s4_core::BitcaskStorageEngine::new(
                dir.path().join("volumes"),
                dir.path().join("metadata"),
                1024 * 1024,
                4096,
                false,
            )
            .await
            .unwrap(),
        );
        let handler = ReplicaWriteHandler::new(
            Arc::clone(&storage) as Arc<dyn StorageEngine>,
            Arc::new(IdempotencyTracker::new()),
            0,
        );
        let md5 = format!("{:x}", md5::compute(b"two parts"));
        let multipart = "9b2cf535f27731c974343645a3985328-2";
        let hlc = crate::hlc::HlcTimestamp::from_record(20, 0, 7);
        let forged = HashMap::from([(
            s4_core::REPAIRED_ETAG_METADATA_KEY.to_string(),
            "forged".to_string(),
        )]);
        for (operation_id, (key, metadata, etag, with_hlc, expected)) in (1u128..).zip([
            ("in-metadata", forged.clone(), "", true, md5.as_str()),
            ("in-field", HashMap::new(), multipart, true, multipart),
            (
                "without-hlc",
                HashMap::new(),
                multipart,
                false,
                md5.as_str(),
            ),
        ]) {
            let request = proto::WriteBlobRequest {
                placement_generation: 0,
                bucket: "bucket".to_string(),
                key: key.to_string(),
                data: b"two parts".to_vec(),
                content_type: "text/plain".to_string(),
                metadata,
                hlc: with_hlc.then(|| hlc.to_proto()),
                etag: etag.to_string(),
                ..write_request(operation_id, HashMap::new())
            };
            assert!(
                handler.handle_write(&request).await.unwrap().success,
                "{key}"
            );
            let record = storage.head_object("bucket", key).await.unwrap();
            assert_eq!(record.etag, expected, "{key}");
            assert!(
                !record.metadata.contains_key(s4_core::REPAIRED_ETAG_METADATA_KEY),
                "{key}"
            );
        }
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

    // ---- Deletes on real storage ----

    mod on_real_storage {
        use super::*;
        use crate::hlc::HlcTimestamp;
        use s4_core::storage::BitcaskStorageEngine;

        async fn handler() -> (
            ReplicaWriteHandler,
            Arc<BitcaskStorageEngine>,
            tempfile::TempDir,
        ) {
            let dir = tempfile::tempdir().unwrap();
            let storage = Arc::new(
                BitcaskStorageEngine::new(
                    dir.path().join("volumes"),
                    dir.path().join("metadata"),
                    10 * 1024 * 1024,
                    4096,
                    false,
                )
                .await
                .unwrap(),
            );
            let handler =
                ReplicaWriteHandler::new(storage.clone(), Arc::new(IdempotencyTracker::new()), 1);
            (handler, storage, dir)
        }

        fn at(wall: u64) -> HlcTimestamp {
            HlcTimestamp::from_record(wall, 0, 7)
        }

        async fn put(storage: &BitcaskStorageEngine, key: &str, hlc: HlcTimestamp) {
            let metadata = HashMap::from([
                ("_s4_hlc_wall".to_string(), hlc.wall_time.to_string()),
                ("_s4_hlc_logical".to_string(), hlc.logical.to_string()),
                ("_s4_hlc_node_id".to_string(), hlc.node_id.to_string()),
            ]);
            storage
                .put_object("bucket", key, b"bytes", "text/plain", &metadata)
                .await
                .unwrap();
        }

        fn delete(
            key: &str,
            hlc: Option<HlcTimestamp>,
            expected: Option<HlcTimestamp>,
        ) -> proto::DeleteBlobRequest {
            proto::DeleteBlobRequest {
                placement_generation: 0,
                bucket: "bucket".to_string(),
                key: key.to_string(),
                hlc: hlc.map(HlcTimestamp::to_proto),
                coordinator_id: Vec::new(),
                operation_id: uuid::Uuid::new_v4().as_u128().to_be_bytes().to_vec(),
                topology_epoch: 1,
                version_id: None,
                expected_source_hlc: expected.map(HlcTimestamp::to_proto),
                bucket_metadata_stage: 0,
                repair_copy: false,
                iam_lease: None,
            }
        }

        async fn version(storage: &BitcaskStorageEngine, key: &str) -> Option<(bool, u64)> {
            match storage.key_version("bucket", key).await.unwrap() {
                KeyVersion::Live(record) => Some((false, record.hlc_timestamp)),
                KeyVersion::Tombstone(record) => Some((true, record.hlc_timestamp)),
                KeyVersion::Absent => None,
            }
        }

        /// A delete with an HLC leaves a tombstone at that HLC, also for a
        /// key the replica never held, and leaves alone a write newer than
        /// it. A purge naming a generation removes exactly that generation.
        /// A request without an HLC removes the record as before.
        #[tokio::test]
        async fn a_delete_leaves_a_tombstone_and_a_purge_removes_its_generation() {
            let (handler, storage, _dir) = handler().await;
            put(&storage, "deleted", at(10)).await;
            put(&storage, "newer", at(30)).await;
            put(&storage, "purged", at(10)).await;
            put(&storage, "legacy", at(10)).await;

            for request in [
                delete("deleted", Some(at(20)), None),
                delete("never-held", Some(at(20)), None),
                delete("newer", Some(at(20)), None),
                delete("purged", Some(at(20)), Some(at(10))),
                delete("legacy", None, None),
            ] {
                let response = handler.handle_delete(&request).await.unwrap();
                assert!(response.success, "{}: {}", request.key, response.error);
            }

            assert_eq!(version(&storage, "deleted").await, Some((true, 20)));
            assert_eq!(version(&storage, "never-held").await, Some((true, 20)));
            assert_eq!(version(&storage, "newer").await, Some((false, 30)));
            assert_eq!(version(&storage, "purged").await, None);
            assert_eq!(version(&storage, "legacy").await, None);
        }

        /// A write into a bucket whose marker this replica holds as a
        /// tombstone does not make the marker up again: the bucket stays
        /// deleted here. A replica that has no marker at all makes it up,
        /// as before.
        #[tokio::test]
        async fn a_write_does_not_bring_back_the_marker_of_a_deleted_bucket() {
            let (handler, storage, _dir) = handler().await;
            assert!(storage
                .tombstone_object("__system__", "__s4_bucket_marker_deleted", 30, 0, 7)
                .await
                .unwrap());
            for bucket in ["deleted", "unknown"] {
                let request = proto::WriteBlobRequest {
                    bucket: bucket.to_string(),
                    key: "late-write".to_string(),
                    data: b"bytes".to_vec(),
                    hlc: Some(at(20).to_proto()),
                    operation_id: uuid::Uuid::new_v4().as_u128().to_be_bytes().to_vec(),
                    topology_epoch: 1,
                    ..Default::default()
                };
                assert!(handler.handle_write(&request).await.unwrap().success);
            }

            assert!(matches!(
                storage.key_version("__system__", "__s4_bucket_marker_deleted").await.unwrap(),
                KeyVersion::Tombstone(_)
            ));
            assert!(matches!(
                storage.key_version("__system__", "__s4_bucket_marker_unknown").await.unwrap(),
                KeyVersion::Live(record)
                    if crate::hlc::HlcTimestamp::of_record(&record) == MADE_UP_MARKER_HLC
            ));
        }

        /// In `__system__` only a bucket marker keeps a tombstone. Any other
        /// record there is deleted as before, even by a delete at the HLC of
        /// the record itself — the way erasure coding removes its metadata,
        /// which a tombstone at that HLC would leave in place.
        #[tokio::test]
        async fn in_the_system_bucket_only_a_bucket_marker_keeps_a_tombstone() {
            let (handler, storage, _dir) = handler().await;
            let metadata = |hlc: HlcTimestamp| {
                HashMap::from([
                    ("_s4_hlc_wall".to_string(), hlc.wall_time.to_string()),
                    ("_s4_hlc_logical".to_string(), hlc.logical.to_string()),
                    ("_s4_hlc_node_id".to_string(), hlc.node_id.to_string()),
                ])
            };
            for key in ["__s4_bucket_marker_gone", "__s4_ec_manifest_object"] {
                storage
                    .put_object(
                        "__system__",
                        key,
                        b"record",
                        "text/plain",
                        &metadata(at(20)),
                    )
                    .await
                    .unwrap();
                let mut request = delete(key, Some(at(20)), None);
                request.bucket = "__system__".to_string();
                if key.starts_with("__s4_bucket_marker_") {
                    request.hlc = Some(at(30).to_proto());
                }
                assert!(handler.handle_delete(&request).await.unwrap().success);
            }

            assert!(matches!(
                storage.key_version("__system__", "__s4_bucket_marker_gone").await.unwrap(),
                KeyVersion::Tombstone(record) if record.hlc_timestamp == 30
            ));
            assert!(matches!(
                storage.key_version("__system__", "__s4_ec_manifest_object").await.unwrap(),
                KeyVersion::Absent
            ));
        }
    }
}
