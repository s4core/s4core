// Copyright 2026 S4Core Team
//
// Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

//! Erasure Coding v1 read path.
//!
//! This module owns the Enterprise-only EC read semantics: quorum-aware
//! metadata resolution, manifest-authoritative shard verification, fast-path
//! data assembly, degraded codec decode, range trimming after full chunk
//! reconstruction, HEAD metadata, and repair enqueueing for bad shard
//! candidates.
//!
//! A healthy chunk is read from the `k` data shards it is assembled from, and
//! parity is fetched only where one of them is missing or fails its manifest
//! hash (decisions `EC2-180` and `EC2-182`). A chunk a range only clips is
//! narrowed once more, to the data shards the range falls in, and is verified
//! by their own hashes rather than by the chunk hash there is no whole chunk
//! for (decision `EC2-181`). Verification paths — the
//! scrubber's reconstruction sample and the admin object check — ask for the
//! whole chunk instead, through
//! [`with_minimal_read`](crate::ec_read::EcReadCoordinator::with_minimal_read),
//! because their answer is about every shard rather than about the bytes a
//! client asked for. `S4_EC_MINIMAL_READ_ENABLED` puts a node back on the EC v1
//! read, and `s4_ec_read_shard_bytes_total` is what the difference is read off.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use s4_cluster::{HlcTimestamp, NodeId};
use s4_codec::{CodecError, RecoveryPath, Shard, ShardRole};
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

use crate::codec_runtime::{EcCodecCpuLimiter, EcCpuError};
use crate::ec_metadata::{
    ChunkManifest, EcManifestReadResult, EcMetadataError, EcMetadataStore, EcObjectManifest,
    EcObjectManifestRef, EcObjectVersionKey, EcPlacementState, ObjectDataLayout,
    PackedSegmentObjectPlacement, ReplicatedSmallObjectPlacement, ShardManifest,
};
use crate::ec_metrics;
use crate::ec_packed_segments::{
    packed_segment_manifest_key, PackedSegment, PackedSegmentEntry, PackedSegmentId,
};
use crate::ec_repair_plan::TransferScope;
use crate::erasure_coding::PoolTopologyView;

/// Inclusive byte range used by EC reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EcByteRange {
    /// First byte to return.
    pub start: u64,
    /// Last byte to return, inclusive.
    pub end: u64,
}

impl EcByteRange {
    /// Build an inclusive byte range.
    pub fn new(start: u64, end: u64) -> Self {
        Self { start, end }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct EffectiveRange {
    start: u64,
    end: u64,
    total: u64,
}

impl EffectiveRange {
    fn len(self) -> u64 {
        if self.total == 0 {
            return 0;
        }
        self.end.saturating_sub(self.start).saturating_add(1)
    }

    fn end_exclusive(self) -> u64 {
        if self.total == 0 {
            return 0;
        }
        self.end.saturating_add(1)
    }
}

/// Request for reading an EC-bucket object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EcReadRequest {
    /// Object version to read.
    pub object: EcObjectVersionKey,
    /// Optional inclusive byte range.
    pub range: Option<EcByteRange>,
}

impl EcReadRequest {
    /// Build a full-object read request.
    pub fn full(object: EcObjectVersionKey) -> Self {
        Self {
            object,
            range: None,
        }
    }

    /// Build a range read request.
    pub fn range(object: EcObjectVersionKey, range: EcByteRange) -> Self {
        Self {
            object,
            range: Some(range),
        }
    }
}

/// Read layout exposed in HEAD/read metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EcReadLayout {
    /// Explicit RF=3 small-object placement inside an EC pool.
    ReplicatedSmall {
        /// Replica nodes that should be used by the RF=3 path.
        replica_nodes: Vec<NodeId>,
    },
    /// Object stored inside a packed segment.
    PackedSegment {
        /// Segment holding the object.
        segment_id: Uuid,
        /// Erasure set that owns the segment's shards.
        erasure_set_id: u64,
        /// Offset of the object from the start of the segment.
        offset: u64,
        /// Object length in bytes.
        length: u64,
        /// Whether hot RF=3 copies still back the object.
        ///
        /// While they do, the object is served from them and the segment is
        /// only a record of where it will sit. Once they are gone the segment's
        /// shards are the object.
        hot_copies: bool,
    },
    /// Erasure-coded object manifest layout.
    ErasureCoded {
        /// Erasure set that owns the shard slots.
        erasure_set_id: u64,
        /// Topology epoch used when the manifest was written.
        topology_epoch: u64,
        /// Codec profile name.
        profile_name: String,
        /// Number of logical chunks in the manifest.
        chunk_count: usize,
    },
}

/// Object metadata returned by EC HEAD and attached to read results.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EcHeadObject {
    /// Object version key.
    pub object: EcObjectVersionKey,
    /// S3 ETag.
    pub etag: String,
    /// Content-Type.
    pub content_type: String,
    /// Full logical object size.
    pub size: u64,
    /// User-defined metadata.
    pub user_metadata: HashMap<String, String>,
    /// Content SHA-256 from durable metadata.
    pub content_hash_sha256: [u8; 32],
    /// HLC timestamp used for conflict ordering.
    pub hlc: HlcTimestamp,
    /// Version ID when versioning is enabled.
    pub version_id: Option<String>,
    /// Physical EC-bucket data layout.
    pub layout: EcReadLayout,
}

/// Result of a full in-memory EC read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EcReadResult {
    /// Returned response body.
    pub body: Bytes,
    /// Object metadata.
    pub head: EcHeadObject,
    /// Response body size.
    pub content_length: u64,
    /// HTTP-style content range `(start, end, total)` for range reads.
    pub content_range: Option<(u64, u64, u64)>,
    /// Number of chunks the read had to rebuild whole.
    pub chunks_reconstructed: usize,
    /// Number of chunks that required codec decode.
    pub degraded_chunks: usize,
    /// Whether at least one repair task was enqueued.
    pub repair_enqueued: bool,
}

/// Result of a chunk-streaming EC read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EcReadStreamReport {
    /// Object metadata.
    pub head: EcHeadObject,
    /// Response body size.
    pub content_length: u64,
    /// HTTP-style content range `(start, end, total)` for range reads.
    pub content_range: Option<(u64, u64, u64)>,
    /// Number of chunks the read had to rebuild whole.
    pub chunks_reconstructed: usize,
    /// Number of chunks that required codec decode.
    pub degraded_chunks: usize,
    /// Whether at least one repair task was enqueued.
    pub repair_enqueued: bool,
}

/// Sink used by streaming EC reads.
#[async_trait]
pub trait EcReadSink: Send {
    /// Accept one already verified and range-trimmed chunk fragment.
    async fn write_chunk(&mut self, chunk_index: u32, data: Bytes) -> Result<(), EcReadError>;
}

#[derive(Default)]
struct BytesReadSink {
    body: BytesMut,
}

#[async_trait]
impl EcReadSink for BytesReadSink {
    async fn write_chunk(&mut self, _chunk_index: u32, data: Bytes) -> Result<(), EcReadError> {
        self.body.extend_from_slice(&data);
        Ok(())
    }
}

/// Quorum result for object-layout metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EcObjectLayoutReadResult {
    /// A live object layout was found.
    Present(ObjectDataLayout),
    /// A quorum-visible tombstone was found.
    Tombstone(Box<EcObjectManifest>),
    /// Quorum confirmed the object/version is absent.
    Absent,
}

/// Port used by the EC read path for quorum-aware metadata reads.
#[async_trait]
pub trait EcReadMetadataCoordinator: Send + Sync {
    /// Read object layout through quorum metadata semantics.
    async fn quorum_read_object_layout(
        &self,
        object: &EcObjectVersionKey,
    ) -> Result<EcObjectLayoutReadResult, EcReadError>;

    /// Read an EC manifest through quorum metadata semantics.
    async fn quorum_read_manifest(
        &self,
        object: &EcObjectVersionKey,
    ) -> Result<EcManifestReadResult, EcReadError>;
}

/// RF=3 small-object read request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EcReplicatedSmallReadRequest {
    /// Object version to read.
    pub object: EcObjectVersionKey,
    /// Explicit RF=3 placement metadata.
    pub placement: ReplicatedSmallObjectPlacement,
    /// Optional inclusive range.
    pub range: Option<EcByteRange>,
}

/// RF=3 small-object HEAD request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EcReplicatedSmallHeadRequest {
    /// Object version to inspect.
    pub object: EcObjectVersionKey,
    /// Explicit RF=3 placement metadata.
    pub placement: ReplicatedSmallObjectPlacement,
}

/// Result returned by the RF=3 small-object read port.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EcReplicatedSmallReadResult {
    /// Response body.
    pub body: Bytes,
    /// Object metadata.
    pub head: EcHeadObject,
    /// Response body size.
    pub content_length: u64,
    /// HTTP-style content range `(start, end, total)` for range reads.
    pub content_range: Option<(u64, u64, u64)>,
    /// RF replicas that responded.
    pub replicas_responded: u8,
}

/// Port used to serve explicit replicated-small layouts through RF=3.
#[async_trait]
pub trait EcReplicatedSmallReader: Send + Sync {
    /// Read a small object from its explicit RF=3 placement.
    async fn read_replicated_small(
        &self,
        request: EcReplicatedSmallReadRequest,
    ) -> Result<EcReplicatedSmallReadResult, EcReadError>;

    /// HEAD a small object from its explicit RF=3 placement.
    async fn head_replicated_small(
        &self,
        request: EcReplicatedSmallHeadRequest,
    ) -> Result<EcHeadObject, EcReadError>;
}

/// Request for one shard candidate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EcReadShardRequest {
    /// Object version addressed by the shard.
    pub object: EcObjectVersionKey,
    /// Operation that produced the winning manifest.
    pub operation_id: Uuid,
    /// Erasure set the manifest places this shard in.
    ///
    /// A pool may hold several sets, and a slot index only means something
    /// inside one of them. Carrying the set on the request lets the reader
    /// refuse a candidate whose node does not hold that slot in that set,
    /// instead of quietly fetching bytes from another set's node.
    pub erasure_set_id: u64,
    /// Logical chunk index.
    pub chunk_index: u32,
    /// Shard slot index.
    pub slot_index: u8,
    /// Assigned node.
    pub node_id: NodeId,
}

/// Replica response for one shard candidate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EcReadShardResponse {
    /// Shard bytes.
    pub data: Bytes,
    /// Replica-provided CRC32, used only for diagnostics.
    pub replica_crc32: Option<u32>,
}

/// Port used by the EC read path to fetch shard candidates.
#[async_trait]
pub trait EcShardReader: Send + Sync {
    /// Read a shard candidate. `Ok(None)` means this candidate is unavailable.
    async fn read_shard(
        &self,
        request: EcReadShardRequest,
    ) -> Result<Option<EcReadShardResponse>, EcReadError>;

    /// Read one byte range of a shard candidate.
    ///
    /// Used by the packed-segment direct read, which wants a few kilobytes out
    /// of a shard rather than the whole thing (`EC2-063`).
    ///
    /// The default implementation reads the whole shard and slices it. That is
    /// correct but buys nothing: an implementation that can ask its storage for
    /// a range should override this, and the difference shows up as the byte
    /// count the direct read is measured by. Correct by default, cheap when the
    /// backing store can be.
    async fn read_shard_range(
        &self,
        request: EcReadShardRequest,
        offset: u64,
        length: u64,
    ) -> Result<Option<Bytes>, EcReadError> {
        let Some(response) = self.read_shard(request).await? else {
            return Ok(None);
        };
        slice_bytes(&response.data, offset, offset.saturating_add(length)).map(Some)
    }
}

/// Repair task kind produced by Phase 5 reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EcReadRepairKind {
    /// Shard was missing or unreachable during a read.
    MissingShard,
    /// Shard bytes failed manifest-authoritative verification.
    CorruptShard,
}

/// Repair task enqueued when a shard cannot be trusted for serving.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EcReadRepairTask {
    /// Object version needing repair.
    pub object: EcObjectVersionKey,
    /// Operation that produced the winning manifest.
    pub operation_id: Uuid,
    /// Erasure set that owns the shard.
    pub erasure_set_id: u64,
    /// Logical chunk index.
    pub chunk_index: u32,
    /// Shard slot index.
    pub slot_index: u8,
    /// Assigned node.
    pub node_id: NodeId,
    /// Repair reason class.
    pub kind: EcReadRepairKind,
    /// Human-readable diagnostic reason.
    pub reason: String,
}

/// Port used to enqueue read-repair work.
#[async_trait]
pub trait EcReadRepairQueue: Send + Sync {
    /// Enqueue one read-repair task.
    async fn enqueue(&self, task: EcReadRepairTask) -> Result<(), EcReadError>;
}

/// No-op repair queue for tests and callers that only want read semantics.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopEcReadRepairQueue;

#[async_trait]
impl EcReadRepairQueue for NoopEcReadRepairQueue {
    async fn enqueue(&self, _task: EcReadRepairTask) -> Result<(), EcReadError> {
        Ok(())
    }
}

/// Enterprise EC read coordinator.
#[derive(Clone)]
pub struct EcReadCoordinator {
    metadata: Arc<EcMetadataStore>,
    codec_limiter: EcCodecCpuLimiter,
    /// The pool as the reading node sees it, when known: shards are asked for
    /// nearest first.
    topology: Option<Arc<PoolTopologyView>>,
    /// Whether a healthy chunk is read from its `k` data shards alone.
    minimal_read: bool,
}

impl EcReadCoordinator {
    /// Create an EC read coordinator.
    pub fn new(metadata: Arc<EcMetadataStore>, codec_limiter: EcCodecCpuLimiter) -> Self {
        Self {
            metadata,
            codec_limiter,
            topology: None,
            minimal_read: true,
        }
    }

    /// The same coordinator, asking for the shards of a chunk nearest first as
    /// `topology` ranks them: this node, its rack, its zone, the rest.
    ///
    /// Distance decides the order inside a group of shards and which shard is
    /// topped up when one is missing, but not which group a read starts from:
    /// that is the data shards (decision EC2-182). Without a topology the order
    /// is the slot order.
    pub fn with_topology(mut self, topology: Arc<PoolTopologyView>) -> Self {
        self.topology = Some(topology);
        self
    }

    /// The same coordinator, reading either the `k` data shards a healthy chunk
    /// is assembled from or every shard of that chunk.
    ///
    /// `false` restores the EC v1 read, in which all `n` shards are fetched and
    /// verified on every read. That read also found corruption in a parity
    /// shard it did not otherwise need; with minimal read on, finding it belongs
    /// to the scrubber, which already verifies every shard a node owns against
    /// the same manifest hash (decision EC2-181).
    pub fn with_minimal_read(mut self, enabled: bool) -> Self {
        self.minimal_read = enabled;
        self
    }

    /// Where bytes coming from `node_id` travelled from, as the read traffic
    /// counters split them.
    ///
    /// A coordinator without a topology does not know its own place in the
    /// pool, let alone the shard owner's, so its traffic is `unknown_zone`
    /// rather than guessed at. The totals stay right either way: the scopes
    /// sum to every byte the read pulled.
    fn transfer_scope(&self, node_id: NodeId) -> TransferScope {
        match &self.topology {
            Some(topology) => TransferScope::between(topology, topology.local_node_id(), node_id),
            None => TransferScope::UnknownZone,
        }
    }

    /// The shards of `chunk` in the order they are asked for.
    fn request_order<'a>(&self, chunk: &'a ChunkManifest) -> Vec<&'a ShardManifest> {
        let mut shards = chunk.shards.iter().collect::<Vec<_>>();
        if let Some(topology) = &self.topology {
            shards.sort_by_key(|shard| (topology.distance_to(shard.node_id), shard.slot_index));
        }
        shards
    }

    /// The shards of `chunk` grouped by when the read needs them.
    ///
    /// A healthy chunk is the concatenation of its `k` data shards, so parity
    /// earns its round trip only once a data shard is missing or fails its
    /// manifest hash (decision EC2-180). Both groups keep the topological order
    /// of [`Self::request_order`], so the nearest data shard is asked first and,
    /// when the chunk has to be topped up, so is the nearest parity.
    ///
    /// With minimal read off the first group is the whole chunk and the top-up
    /// group is empty, which is the EC v1 read exactly.
    fn read_order<'a>(&self, chunk: &'a ChunkManifest) -> ShardRequestOrder<'a> {
        let ordered = self.request_order(chunk);
        if !self.minimal_read {
            return ShardRequestOrder {
                first: ordered,
                top_up: Vec::new(),
            };
        }
        let (first, top_up) = ordered.into_iter().partition(|shard| shard.role == ShardRole::Data);
        ShardRequestOrder { first, top_up }
    }

    /// Read an object into memory.
    pub async fn read<M, R, S, Q>(
        &self,
        request: EcReadRequest,
        metadata_coordinator: &M,
        small_reader: &R,
        shard_reader: &S,
        repair_queue: &Q,
    ) -> Result<EcReadResult, EcReadError>
    where
        M: EcReadMetadataCoordinator,
        R: EcReplicatedSmallReader,
        S: EcShardReader,
        Q: EcReadRepairQueue,
    {
        let mut sink = BytesReadSink::default();
        let report = self
            .read_to_sink(
                request,
                metadata_coordinator,
                small_reader,
                shard_reader,
                repair_queue,
                &mut sink,
            )
            .await?;

        Ok(EcReadResult {
            body: sink.body.freeze(),
            head: report.head,
            content_length: report.content_length,
            content_range: report.content_range,
            chunks_reconstructed: report.chunks_reconstructed,
            degraded_chunks: report.degraded_chunks,
            repair_enqueued: report.repair_enqueued,
        })
    }

    /// Read an object chunk-by-chunk into a caller-provided sink.
    pub async fn read_to_sink<M, R, S, Q, K>(
        &self,
        request: EcReadRequest,
        metadata_coordinator: &M,
        small_reader: &R,
        shard_reader: &S,
        repair_queue: &Q,
        sink: &mut K,
    ) -> Result<EcReadStreamReport, EcReadError>
    where
        M: EcReadMetadataCoordinator,
        R: EcReplicatedSmallReader,
        S: EcShardReader,
        Q: EcReadRepairQueue,
        K: EcReadSink,
    {
        request.object.validate()?;
        let layout = self.resolve_object_layout(&request.object, metadata_coordinator).await?;

        match layout {
            ObjectDataLayout::ReplicatedSmall(placement) => {
                self.read_replicated_small(request, placement, small_reader, sink).await
            }
            ObjectDataLayout::PackedSegment(placement) => match placement.hot_placement() {
                // The segment is not encoded yet, so the object's bytes are
                // still the RF=3 copies the write path produced.
                Some(hot) => {
                    let mut report =
                        self.read_replicated_small(request, hot, small_reader, sink).await?;
                    report.head.layout = packed_read_layout(&placement);
                    Ok(report)
                }
                None => {
                    self.read_packed_from_shards(
                        request,
                        placement,
                        metadata_coordinator,
                        shard_reader,
                        repair_queue,
                        sink,
                    )
                    .await
                }
            },
            ObjectDataLayout::ErasureCoded(reference) => {
                let manifest = self
                    .resolve_manifest(&request.object, &reference, metadata_coordinator)
                    .await?;
                self.read_erasure_coded(request, manifest, shard_reader, repair_queue, sink)
                    .await
            }
        }
    }

    /// Return object metadata without fetching EC shards.
    pub async fn head<M, R>(
        &self,
        object: EcObjectVersionKey,
        metadata_coordinator: &M,
        small_reader: &R,
    ) -> Result<EcHeadObject, EcReadError>
    where
        M: EcReadMetadataCoordinator,
        R: EcReplicatedSmallReader,
    {
        object.validate()?;
        let layout = self.resolve_object_layout(&object, metadata_coordinator).await?;
        match layout {
            ObjectDataLayout::ReplicatedSmall(placement) => {
                small_reader
                    .head_replicated_small(EcReplicatedSmallHeadRequest { object, placement })
                    .await
            }
            ObjectDataLayout::PackedSegment(placement) => match placement.hot_placement() {
                Some(hot) => {
                    let mut head = small_reader
                        .head_replicated_small(EcReplicatedSmallHeadRequest {
                            object,
                            placement: hot,
                        })
                        .await?;
                    head.layout = packed_read_layout(&placement);
                    Ok(head)
                }
                None => {
                    let facts = self
                        .resolve_packed_facts(&object, &placement, metadata_coordinator)
                        .await?;
                    Ok(packed_head(object, &placement, &facts))
                }
            },
            ObjectDataLayout::ErasureCoded(reference) => {
                let manifest =
                    self.resolve_manifest(&object, &reference, metadata_coordinator).await?;
                Ok(head_from_manifest(&manifest))
            }
        }
    }

    async fn resolve_object_layout<M>(
        &self,
        object: &EcObjectVersionKey,
        metadata_coordinator: &M,
    ) -> Result<ObjectDataLayout, EcReadError>
    where
        M: EcReadMetadataCoordinator,
    {
        // A local object-layout hit is only a cache. Cluster-visible delete or
        // overwrite state must come from quorum metadata before GET/HEAD can
        // authorize bytes for a client.
        match metadata_coordinator.quorum_read_object_layout(object).await? {
            EcObjectLayoutReadResult::Present(layout) => {
                layout.validate_for_object(object)?;
                self.metadata.cache_object_layout(object, &layout).await?;
                Ok(layout)
            }
            EcObjectLayoutReadResult::Tombstone(manifest) => {
                self.metadata.cache_manifest(&manifest).await?;
                self.metadata.delete_object_layout(object).await?;
                Err(EcReadError::Deleted {
                    bucket: object.bucket.clone(),
                    key: object.key.clone(),
                    version_id: object.version_id.clone(),
                })
            }
            EcObjectLayoutReadResult::Absent => {
                self.metadata.delete_object_layout(object).await?;
                self.metadata.delete_manifest(object).await?;
                Err(EcReadError::NotFound {
                    bucket: object.bucket.clone(),
                    key: object.key.clone(),
                    version_id: object.version_id.clone(),
                })
            }
        }
    }

    async fn resolve_manifest<M>(
        &self,
        object: &EcObjectVersionKey,
        reference: &EcObjectManifestRef,
        metadata_coordinator: &M,
    ) -> Result<EcObjectManifest, EcReadError>
    where
        M: EcReadMetadataCoordinator,
    {
        reference.validate()?;
        if reference.object != *object {
            return Err(EcReadError::ManifestMismatch {
                reason: format!(
                    "layout references {}/{} but read requested {}/{}",
                    reference.object.bucket, reference.object.key, object.bucket, object.key
                ),
            });
        }

        if let Some(local) = self.metadata.get_manifest(object).await? {
            if reference.matches_manifest(&local)? {
                return manifest_from_read_result(object, local.into_read_result());
            }
        }

        let quorum_result = metadata_coordinator.quorum_read_manifest(object).await?;
        match &quorum_result {
            EcManifestReadResult::Present(manifest) | EcManifestReadResult::Tombstone(manifest) => {
                self.metadata.put_manifest(manifest).await?;
            }
            EcManifestReadResult::Absent => {}
        }

        match quorum_result {
            EcManifestReadResult::Present(manifest) => {
                if !reference.matches_manifest(&manifest)? {
                    return Err(EcReadError::ManifestMismatch {
                        reason: "quorum read returned a manifest that does not match object layout"
                            .to_string(),
                    });
                }
                ensure_manifest_readable(&manifest)?;
                Ok(manifest)
            }
            EcManifestReadResult::Tombstone(_) => Err(EcReadError::Deleted {
                bucket: object.bucket.clone(),
                key: object.key.clone(),
                version_id: object.version_id.clone(),
            }),
            EcManifestReadResult::Absent => Err(EcReadError::NotFound {
                bucket: object.bucket.clone(),
                key: object.key.clone(),
                version_id: object.version_id.clone(),
            }),
        }
    }

    async fn read_replicated_small<R, K>(
        &self,
        request: EcReadRequest,
        placement: ReplicatedSmallObjectPlacement,
        small_reader: &R,
        sink: &mut K,
    ) -> Result<EcReadStreamReport, EcReadError>
    where
        R: EcReplicatedSmallReader,
        K: EcReadSink,
    {
        let EcReadRequest { object, range } = request;
        let result = small_reader
            .read_replicated_small(EcReplicatedSmallReadRequest {
                object,
                placement: placement.clone(),
                range,
            })
            .await?;

        if range.is_none() {
            let actual = sha256(&result.body);
            if actual != placement.content_hash_sha256 {
                return Err(EcReadError::ReplicatedSmallVerification {
                    reason: "replicated-small RF=3 read returned bytes that do not match placement SHA-256"
                        .to_string(),
                });
            }
        }

        sink.write_chunk(0, result.body).await?;
        Ok(EcReadStreamReport {
            head: result.head,
            content_length: result.content_length,
            content_range: result.content_range,
            chunks_reconstructed: 0,
            degraded_chunks: 0,
            repair_enqueued: false,
        })
    }

    /// Read one packed object out of its segment's shards, ignoring hot copies.
    ///
    /// The gate that stands in front of a hot-copy purge. An ordinary read would
    /// answer "is this object readable", and while the hot copies exist it would
    /// answer yes without ever touching a shard — which is precisely the
    /// question that must not be asked before deleting them. This forces the
    /// shard path and reports what it took, so a segment that can only be served
    /// degraded is visible as such rather than passing quietly.
    ///
    /// # Errors
    ///
    /// Returns whatever the shard path returns. A failure here means the segment
    /// is not yet safe to release, and the caller must leave the hot copies
    /// alone.
    pub async fn probe_packed_object<S, Q>(
        &self,
        object: EcObjectVersionKey,
        placement: &PackedSegmentObjectPlacement,
        shard_reader: &S,
        repair_queue: &Q,
    ) -> Result<EcReadStreamReport, EcReadError>
    where
        S: EcShardReader,
        Q: EcReadRepairQueue,
    {
        let (segment, entry) = self.resolve_packed_entry(&object, placement).await?;
        let facts = PackedObjectFacts::from_entry(&entry);
        // Local only, and on purpose. The probe runs on the node that just
        // committed the segment's manifest, so a manifest missing here is a
        // reason to keep the hot copies rather than to go looking for it.
        let manifest = self
            .metadata
            .get_manifest(&segment.manifest_object_key())
            .await?
            .ok_or_else(|| EcReadError::Unavailable {
                reason: format!(
                    "packed segment {} has no local manifest to probe against",
                    segment.segment_id
                ),
            })?;

        let mut from_shards = placement.clone();
        from_shards.hot_replica_nodes.clear();
        let mut sink = BytesReadSink::default();
        self.serve_packed_object(
            EcReadRequest::full(object),
            &from_shards,
            &facts,
            &manifest,
            shard_reader,
            repair_queue,
            &mut sink,
        )
        .await
    }

    /// Read one packed object's whole content out of its segment's shards.
    ///
    /// The probe above with its result kept. Used when the bytes themselves are
    /// wanted rather than the fact that they can be produced — a repack reads
    /// its objects this way, because a repacked object's only placement is the
    /// segment it is being moved out of.
    ///
    /// # Errors
    ///
    /// Returns whatever the shard path returns. Unlike an ordinary read this
    /// never falls back to hot copies, because by construction there are none.
    pub async fn read_packed_object_bytes<S, Q>(
        &self,
        object: EcObjectVersionKey,
        placement: &PackedSegmentObjectPlacement,
        shard_reader: &S,
        repair_queue: &Q,
    ) -> Result<Bytes, EcReadError>
    where
        S: EcShardReader,
        Q: EcReadRepairQueue,
    {
        let (segment, entry) = self.resolve_packed_entry(&object, placement).await?;
        let facts = PackedObjectFacts::from_entry(&entry);
        let manifest = self
            .metadata
            .get_manifest(&segment.manifest_object_key())
            .await?
            .ok_or_else(|| EcReadError::Unavailable {
                reason: format!(
                    "packed segment {} has no local manifest to read from",
                    segment.segment_id
                ),
            })?;

        let mut from_shards = placement.clone();
        from_shards.hot_replica_nodes.clear();
        let mut sink = BytesReadSink::default();
        self.serve_packed_object(
            EcReadRequest::full(object),
            &from_shards,
            &facts,
            &manifest,
            shard_reader,
            repair_queue,
            &mut sink,
        )
        .await?;
        Ok(sink.body.freeze())
    }

    /// Serve a packed object whose hot copies are gone, from the segment's shards.
    ///
    /// Tries the direct read of `EC2-063` first and falls back to rebuilding the
    /// chunks the object spans. The fallback is a genuine last resort, not an
    /// equivalent path: it reads whole chunks, which is exactly the read
    /// amplification this plan exists to avoid, so it is counted.
    async fn read_packed_from_shards<M, S, Q, K>(
        &self,
        request: EcReadRequest,
        placement: PackedSegmentObjectPlacement,
        metadata_coordinator: &M,
        shard_reader: &S,
        repair_queue: &Q,
        sink: &mut K,
    ) -> Result<EcReadStreamReport, EcReadError>
    where
        M: EcReadMetadataCoordinator,
        S: EcShardReader,
        Q: EcReadRepairQueue,
        K: EcReadSink,
    {
        let facts = self
            .resolve_packed_facts(&request.object, &placement, metadata_coordinator)
            .await?;
        let manifest = self
            .resolve_segment_manifest(placement.segment_id, metadata_coordinator)
            .await?;
        self.serve_packed_object(
            request,
            &placement,
            &facts,
            &manifest,
            shard_reader,
            repair_queue,
            sink,
        )
        .await
    }

    /// Serve a packed object from a segment whose manifest is already resolved.
    ///
    /// Split from the resolution above because the two callers find the manifest
    /// differently: a client read may have to ask the pool for it, while the
    /// pre-purge probe insists on the local copy.
    #[allow(clippy::too_many_arguments)]
    async fn serve_packed_object<S, Q, K>(
        &self,
        request: EcReadRequest,
        placement: &PackedSegmentObjectPlacement,
        facts: &PackedObjectFacts,
        manifest: &EcObjectManifest,
        shard_reader: &S,
        repair_queue: &Q,
        sink: &mut K,
    ) -> Result<EcReadStreamReport, EcReadError>
    where
        S: EcShardReader,
        Q: EcReadRepairQueue,
        K: EcReadSink,
    {
        let effective_range = resolve_range(request.range, placement.length)?;
        let head = packed_head(request.object.clone(), placement, facts);

        // The whole object is fetched even for a range request. A packed object
        // is below `min_object_size` by definition, so this costs at most that
        // much, and it is what lets the object's own SHA-256 verify the bytes:
        // a partial read has nothing to check itself against.
        let direct = self.read_packed_direct(placement, manifest, shard_reader).await;
        let (body, degraded_chunks, chunks_reconstructed, repair_enqueued) = match direct {
            Ok(body) => {
                ec_metrics::record_packed_direct_read(manifest.erasure_set_id);
                (body, 0, 0, false)
            }
            Err(error) => {
                tracing::info!(
                    bucket = %request.object.bucket,
                    key = %request.object.key,
                    segment_id = %placement.segment_id,
                    %error,
                    "packed direct read failed; rebuilding the chunks the object spans"
                );
                ec_metrics::record_packed_degraded_read(manifest.erasure_set_id);
                let (body, report) = self
                    .read_packed_degraded(placement, manifest, shard_reader, repair_queue)
                    .await?;
                verify_packed_object(placement, &body)?;
                (
                    body,
                    report.degraded_chunks.max(1),
                    report.chunks_reconstructed,
                    report.repair_enqueued,
                )
            }
        };

        let trimmed = slice_bytes(
            &body,
            effective_range.start,
            effective_range.end_exclusive(),
        )?;
        sink.write_chunk(0, trimmed).await?;
        Ok(EcReadStreamReport {
            head,
            content_length: effective_range.len(),
            content_range: request.range.map(|_| {
                (
                    effective_range.start,
                    effective_range.end,
                    effective_range.total,
                )
            }),
            chunks_reconstructed,
            degraded_chunks,
            repair_enqueued,
        })
    }

    /// Read a packed object from the data shards that hold it, without decoding.
    ///
    /// # Errors
    ///
    /// Any failure here is a reason to fall back, not to fail the read: an
    /// unavailable shard, a manifest the arithmetic cannot address, or bytes
    /// that do not hash to what the index says they must.
    async fn read_packed_direct<S>(
        &self,
        placement: &PackedSegmentObjectPlacement,
        manifest: &EcObjectManifest,
        shard_reader: &S,
    ) -> Result<Bytes, EcReadError>
    where
        S: EcShardReader,
    {
        let spans = locate_packed_object(manifest, placement.offset, placement.length)?;
        let capacity = usize::try_from(placement.length).unwrap_or(0);
        let mut body = BytesMut::with_capacity(capacity);

        for span in &spans {
            let node_id = shard_owner(manifest, span.chunk_index, span.slot_index)?.0;
            let request = EcReadShardRequest {
                object: manifest.object_key(),
                operation_id: manifest.operation_id,
                erasure_set_id: manifest.erasure_set_id,
                chunk_index: span.chunk_index,
                slot_index: span.slot_index,
                node_id,
            };
            let scope = self.transfer_scope(node_id);
            let answer =
                shard_reader.read_shard_range(request, span.offset_in_shard, span.length).await;
            let moved = match &answer {
                Ok(Some(bytes)) => bytes.len() as u64,
                Ok(None) | Err(_) => 0,
            };
            ec_metrics::record_read_shard_fetch(scope, moved);
            let fetched = answer?.ok_or_else(|| EcReadError::Unavailable {
                reason: format!(
                    "data shard {} of chunk {} is unavailable",
                    span.slot_index, span.chunk_index
                ),
            })?;
            if fetched.len() as u64 != span.length {
                return Err(EcReadError::Unavailable {
                    reason: format!(
                        "data shard {} of chunk {} returned {} byte(s), expected {}",
                        span.slot_index,
                        span.chunk_index,
                        fetched.len(),
                        span.length
                    ),
                });
            }
            body.extend_from_slice(&fetched);
        }

        let body = body.freeze();
        verify_packed_object(placement, &body)?;
        Ok(body)
    }

    /// Rebuild the chunks a packed object spans and cut the object out of them.
    async fn read_packed_degraded<S, Q>(
        &self,
        placement: &PackedSegmentObjectPlacement,
        manifest: &EcObjectManifest,
        shard_reader: &S,
        repair_queue: &Q,
    ) -> Result<(Bytes, EcReadStreamReport), EcReadError>
    where
        S: EcShardReader,
        Q: EcReadRepairQueue,
    {
        let end = placement
            .offset
            .checked_add(placement.length)
            .and_then(|end| end.checked_sub(1))
            .ok_or_else(|| EcReadError::InvalidManifest {
                reason: format!(
                    "packed object at {}+{} overflows the segment",
                    placement.offset, placement.length
                ),
            })?;
        let segment_request = EcReadRequest::range(
            manifest.object_key(),
            EcByteRange::new(placement.offset, end),
        );
        let mut collected = BytesReadSink::default();
        let report = self
            .read_erasure_coded(
                segment_request,
                manifest.clone(),
                shard_reader,
                repair_queue,
                &mut collected,
            )
            .await?;
        Ok((collected.body.freeze(), report))
    }

    /// Find the segment record and this object's entry in it.
    async fn resolve_packed_entry(
        &self,
        object: &EcObjectVersionKey,
        placement: &PackedSegmentObjectPlacement,
    ) -> Result<(PackedSegment, PackedSegmentEntry), EcReadError> {
        let segment =
            self.metadata.get_packed_segment(placement.segment_id).await?.ok_or_else(|| {
                EcReadError::Unavailable {
                    reason: format!(
                        "packed segment {} is not known to this node",
                        placement.segment_id
                    ),
                }
            })?;
        let entry = segment.entry_for(object).cloned().ok_or_else(|| EcReadError::Unavailable {
            reason: format!(
                "packed segment {} does not list {}/{}",
                placement.segment_id, object.bucket, object.key
            ),
        })?;
        Ok((segment, entry))
    }

    /// The object's ETag, content type and user metadata after the hot copies are gone.
    ///
    /// The segment's index entry is preferred when this node has it, because it
    /// costs nothing: the node that packed the object holds the whole segment
    /// record locally. Every other node reads the object's pointer manifest,
    /// which carries the same facts and is quorum-durable — that is what makes a
    /// packed object readable from the whole cluster rather than from its writer
    /// alone (decision EC2-068).
    async fn resolve_packed_facts<M>(
        &self,
        object: &EcObjectVersionKey,
        placement: &PackedSegmentObjectPlacement,
        metadata_coordinator: &M,
    ) -> Result<PackedObjectFacts, EcReadError>
    where
        M: EcReadMetadataCoordinator,
    {
        if let Some(segment) = self.metadata.get_packed_segment(placement.segment_id).await? {
            if let Some(entry) = segment.entry_for(object) {
                return Ok(PackedObjectFacts::from_entry(entry));
            }
        }

        if let Some(local) = self.metadata.get_manifest(object).await? {
            if local.packed.is_some() && !local.is_deleted {
                return Ok(PackedObjectFacts::from_manifest(&local));
            }
        }

        match metadata_coordinator.quorum_read_manifest(object).await? {
            EcManifestReadResult::Present(manifest) if manifest.packed.is_some() => {
                self.metadata.cache_manifest(&manifest).await?;
                Ok(PackedObjectFacts::from_manifest(&manifest))
            }
            _ => Err(EcReadError::Unavailable {
                reason: format!(
                    "{}/{} is packed into segment {} but no manifest describes it",
                    object.bucket, object.key, placement.segment_id
                ),
            }),
        }
    }

    /// Read the manifest that describes a segment's shards.
    async fn resolve_segment_manifest<M>(
        &self,
        segment_id: PackedSegmentId,
        metadata_coordinator: &M,
    ) -> Result<EcObjectManifest, EcReadError>
    where
        M: EcReadMetadataCoordinator,
    {
        let key = packed_segment_manifest_key(segment_id);
        if let Some(local) = self.metadata.get_manifest(&key).await? {
            if !local.is_deleted && local.state != EcPlacementState::Tombstone {
                return Ok(local);
            }
        }
        match metadata_coordinator.quorum_read_manifest(&key).await? {
            EcManifestReadResult::Present(manifest) => {
                self.metadata.cache_manifest(&manifest).await?;
                Ok(manifest)
            }
            EcManifestReadResult::Tombstone(_) | EcManifestReadResult::Absent => {
                Err(EcReadError::Unavailable {
                    reason: format!(
                        "packed segment {segment_id} has no manifest, so its shards cannot be \
                         located"
                    ),
                })
            }
        }
    }

    async fn read_erasure_coded<S, Q, K>(
        &self,
        request: EcReadRequest,
        manifest: EcObjectManifest,
        shard_reader: &S,
        repair_queue: &Q,
        sink: &mut K,
    ) -> Result<EcReadStreamReport, EcReadError>
    where
        S: EcShardReader,
        Q: EcReadRepairQueue,
        K: EcReadSink,
    {
        manifest.validate()?;
        ensure_manifest_readable(&manifest)?;
        let effective_range = resolve_range(request.range, manifest.original_size)?;
        let head = head_from_manifest(&manifest);
        // Worked out before anything is fetched, so what the read asks for and
        // what the answer needs cannot drift apart (decision EC2-180). Chunks
        // the range does not touch are absent from the plan and stay untouched.
        let plan = plan_effective_read(&manifest, effective_range)?;
        let mut chunks_reconstructed = 0usize;
        let mut degraded_chunks = 0usize;
        let mut repair_enqueued = false;

        for chunk_plan in &plan.chunks {
            let chunk = usize::try_from(chunk_plan.chunk_index)
                .ok()
                .and_then(|index| manifest.chunks.get(index))
                .ok_or_else(|| EcReadError::InvalidManifest {
                    reason: format!("manifest has no chunk {}", chunk_plan.chunk_index),
                })?;

            let chunk_read = self
                .read_chunk(&manifest, chunk, chunk_plan, shard_reader, repair_queue)
                .await?;
            if chunk_read.rebuilt {
                chunks_reconstructed = chunks_reconstructed.saturating_add(1);
            }
            if chunk_read.degraded {
                degraded_chunks = degraded_chunks.saturating_add(1);
                ec_metrics::record_degraded_read(manifest.erasure_set_id);
            }
            if chunk_read.recovery_path == RecoveryPath::UnifiedSolver {
                ec_metrics::record_unified_solver_recovery(
                    manifest.erasure_set_id,
                    ec_metrics::UnifiedSolverOperation::Read,
                );
                tracing::info!(
                    bucket = %manifest.bucket,
                    key = %manifest.key,
                    erasure_set_id = manifest.erasure_set_id,
                    chunk_index = chunk.chunk_index,
                    recovery_path = RecoveryPath::UnifiedSolver.as_str(),
                    "EC chunk rebuilt by the unified LRC solver: no EC v1 path reached this loss"
                );
            }
            repair_enqueued |= chunk_read.repair_enqueued;

            sink.write_chunk(chunk.chunk_index, chunk_read.bytes).await?;
        }

        Ok(EcReadStreamReport {
            head,
            content_length: effective_range.len(),
            content_range: request.range.map(|_| {
                (
                    effective_range.start,
                    effective_range.end,
                    effective_range.total,
                )
            }),
            chunks_reconstructed,
            degraded_chunks,
            repair_enqueued,
        })
    }

    /// The bytes one chunk contributes to the answer.
    ///
    /// A chunk the range covers whole is rebuilt from its `k` data shards and
    /// checked against `chunk_sha256`. A chunk the range only clips is read from
    /// the data shards the range actually falls in, each checked against its own
    /// `ShardManifest::sha256`, because there is no whole chunk to run the chunk
    /// hash on (decision EC2-181).
    ///
    /// Losing one of those shards puts the chunk back on the first path instead
    /// of inventing a partial recovery: the client gets the same bytes, only
    /// dearer (decision EC2-180). The shards already fetched are kept, so the
    /// fallback asks only for what it still needs.
    async fn read_chunk<S, Q>(
        &self,
        manifest: &EcObjectManifest,
        chunk: &ChunkManifest,
        plan: &ChunkReadPlan,
        shard_reader: &S,
        repair_queue: &Q,
    ) -> Result<EcChunkReadOutcome, EcReadError>
    where
        S: EcShardReader,
        Q: EcReadRepairQueue,
    {
        let mut fetched = FetchedShards::new(usize::from(manifest.profile.total_shards()));

        // With minimal read off every chunk the range touches is rebuilt whole
        // from all `n` shards and then trimmed, which is the EC v1 read.
        if self.minimal_read && plan.coverage == ChunkCoverage::Partial {
            if let Some(bytes) = self
                .read_planned_spans(
                    manifest,
                    chunk,
                    plan,
                    &mut fetched,
                    shard_reader,
                    repair_queue,
                )
                .await?
            {
                return Ok(EcChunkReadOutcome {
                    bytes,
                    rebuilt: false,
                    degraded: false,
                    repair_enqueued: fetched.repair_enqueued,
                    recovery_path: RecoveryPath::Direct,
                });
            }
        }

        let mut outcome = self
            .reconstruct_chunk(manifest, chunk, &mut fetched, shard_reader, repair_queue)
            .await?;
        outcome.bytes = slice_bytes(&outcome.bytes, plan.wanted_from, plan.wanted_to)?;
        Ok(outcome)
    }

    /// Fetch the data shards `plan` names and cut its spans out of them.
    ///
    /// `None` when one of them did not arrive or did not match the manifest. The
    /// shards fetched here stay in `fetched`, so rebuilding the chunk afterwards
    /// asks only for what is still missing.
    async fn read_planned_spans<S, Q>(
        &self,
        manifest: &EcObjectManifest,
        chunk: &ChunkManifest,
        plan: &ChunkReadPlan,
        fetched: &mut FetchedShards,
        shard_reader: &S,
        repair_queue: &Q,
    ) -> Result<Option<Bytes>, EcReadError>
    where
        S: EcShardReader,
        Q: EcReadRepairQueue,
    {
        let wanted = self
            .request_order(chunk)
            .into_iter()
            .filter(|shard| plan.slots.contains(&shard.slot_index))
            .collect::<Vec<_>>();
        self.fetch_verified_shards(
            manifest,
            chunk,
            &wanted,
            fetched,
            shard_reader,
            repair_queue,
        )
        .await?;
        if !plan.slots.iter().all(|slot| fetched.holds(*slot)) {
            return Ok(None);
        }

        let capacity =
            usize::try_from(plan.wanted_to.saturating_sub(plan.wanted_from)).unwrap_or_default();
        let mut bytes = BytesMut::with_capacity(capacity);
        for span in &plan.spans {
            let shard = fetched
                .shards
                .get(usize::from(span.slot_index))
                .and_then(Option::as_ref)
                .ok_or_else(|| EcReadError::InvalidManifest {
                reason: format!(
                    "chunk {} slot {} is outside profile shard vector",
                    chunk.chunk_index, span.slot_index
                ),
            })?;
            let end = span.offset_in_shard.saturating_add(span.length);
            bytes.extend_from_slice(&slice_bytes(&shard.data, span.offset_in_shard, end)?);
        }
        Ok(Some(bytes.freeze()))
    }

    async fn reconstruct_chunk<S, Q>(
        &self,
        manifest: &EcObjectManifest,
        chunk: &ChunkManifest,
        fetched: &mut FetchedShards,
        shard_reader: &S,
        repair_queue: &Q,
    ) -> Result<EcChunkReadOutcome, EcReadError>
    where
        S: EcShardReader,
        Q: EcReadRepairQueue,
    {
        let order = self.read_order(chunk);
        self.fetch_verified_shards(
            manifest,
            chunk,
            &order.first,
            fetched,
            shard_reader,
            repair_queue,
        )
        .await?;

        if let Some(bytes) =
            fast_path_assemble_data_shards(&manifest.profile, chunk, &fetched.shards)?
        {
            verify_chunk_hash(chunk, &bytes)?;
            return Ok(EcChunkReadOutcome {
                bytes,
                rebuilt: true,
                degraded: false,
                repair_enqueued: fetched.repair_enqueued,
                recovery_path: RecoveryPath::Direct,
            });
        }

        // A data shard was missing or failed its manifest hash, so this chunk
        // goes back to the read it had before: the rest of the chunk is fetched
        // nearest first and the codec rebuilds it (decision EC2-180). The
        // neighbouring chunks of the same object are not affected — every chunk
        // decides this for itself.
        if !order.top_up.is_empty() {
            self.fetch_verified_shards(
                manifest,
                chunk,
                &order.top_up,
                fetched,
                shard_reader,
                repair_queue,
            )
            .await?;
        }

        if fetched.verified_shards < usize::from(manifest.profile.read_minimum_normal()) {
            return Err(EcReadError::Unavailable {
                reason: format!(
                    "chunk {} has only {} verified shard(s), need at least {}",
                    chunk.chunk_index,
                    fetched.verified_shards,
                    manifest.profile.read_minimum_normal()
                ),
            });
        }

        let profile = manifest.profile.clone();
        // Cloned, not moved: the accumulator is the caller's. One refcount per slot.
        let shard_candidates = fetched.shards.clone();
        let data_size = chunk.data_size;
        // Which slots the codec is about to work from, in the shape the codec
        // answers about. Taken from the shards themselves rather than from the
        // manifest: what the read could not fetch is what the decode has to
        // get past, whatever the cluster believes it still holds.
        let lost = shard_candidates.iter().map(Option::is_none).collect::<Vec<_>>();
        let (decoded, recovery_path) = self
            .codec_limiter
            .run_decode_or_repair(move || {
                let codec = crate::ec_codec::build_codec(profile)?;
                let bytes = codec.decode_chunk(&shard_candidates, data_size)?;
                // Asked of the codec that just did the work, so the path
                // reported is the path taken and not a second opinion about it.
                Ok((bytes, codec.recovery_path(&lost)))
            })
            .await?;
        verify_chunk_hash(chunk, &decoded)?;
        Ok(EcChunkReadOutcome {
            bytes: decoded,
            rebuilt: true,
            degraded: true,
            repair_enqueued: fetched.repair_enqueued,
            recovery_path,
        })
    }

    /// Fetch `wanted` and add to `fetched` the shards that pass their manifest
    /// hash.
    ///
    /// What happens to a shard does not depend on which pass asked for it: a
    /// shard that is unavailable, unreadable or does not match the manifest is
    /// counted, queued for repair and left out of the set, exactly as before.
    /// The read narrows which shards are asked for, not what a bad one costs.
    async fn fetch_verified_shards<S, Q>(
        &self,
        manifest: &EcObjectManifest,
        chunk: &ChunkManifest,
        wanted: &[&ShardManifest],
        fetched: &mut FetchedShards,
        shard_reader: &S,
        repair_queue: &Q,
    ) -> Result<(), EcReadError>
    where
        S: EcShardReader,
        Q: EcReadRepairQueue,
    {
        for expected in wanted.iter().copied() {
            if fetched.claim(expected.slot_index) {
                continue;
            }
            let request = EcReadShardRequest {
                object: manifest.object_key(),
                operation_id: manifest.operation_id,
                erasure_set_id: manifest.erasure_set_id,
                chunk_index: chunk.chunk_index,
                slot_index: expected.slot_index,
                node_id: expected.node_id,
            };

            let scope = self.transfer_scope(expected.node_id);
            let response = match shard_reader.read_shard(request).await {
                Ok(Some(response)) => {
                    // Counted before the manifest hash is checked, for the same
                    // reason repair counts a source that answered: the bytes
                    // crossed the link whatever they turn out to be. The two
                    // branches below count a fetch and no bytes, because a read
                    // that was handed nothing has nothing to measure.
                    ec_metrics::record_read_shard_fetch(scope, response.data.len() as u64);
                    response
                }
                Ok(None) => {
                    ec_metrics::record_read_shard_fetch(scope, 0);
                    fetched.repair_enqueued |= self
                        .enqueue_repair(
                            manifest,
                            chunk,
                            expected,
                            EcReadRepairKind::MissingShard,
                            "shard candidate was unavailable",
                            repair_queue,
                        )
                        .await;
                    continue;
                }
                Err(error) => {
                    ec_metrics::record_read_shard_fetch(scope, 0);
                    ec_metrics::record_shard_hash_mismatch(manifest.erasure_set_id);
                    let reason = error.to_string();
                    fetched.repair_enqueued |= self
                        .enqueue_repair(
                            manifest,
                            chunk,
                            expected,
                            EcReadRepairKind::MissingShard,
                            reason,
                            repair_queue,
                        )
                        .await;
                    continue;
                }
            };

            if let Some(replica_crc32) = response.replica_crc32 {
                if replica_crc32 != expected.crc32 {
                    tracing::debug!(
                        node_id = %expected.node_id,
                        chunk_index = chunk.chunk_index,
                        slot_index = expected.slot_index,
                        replica_crc32,
                        manifest_crc32 = expected.crc32,
                        "replica-reported EC shard CRC32 differs from manifest"
                    );
                }
            }

            if let Err(error) = expected.verify_bytes(&response.data) {
                ec_metrics::record_shard_hash_mismatch(manifest.erasure_set_id);
                fetched.repair_enqueued |= self
                    .enqueue_repair(
                        manifest,
                        chunk,
                        expected,
                        EcReadRepairKind::CorruptShard,
                        error.to_string(),
                        repair_queue,
                    )
                    .await;
                continue;
            }

            let shard = Shard::new(
                expected.slot_index,
                chunk.chunk_index,
                expected.role,
                response.data,
            )?;
            let slot = usize::from(expected.slot_index);
            let Some(entry) = fetched.shards.get_mut(slot) else {
                return Err(EcReadError::InvalidManifest {
                    reason: format!(
                        "chunk {} slot {} is outside profile shard vector",
                        chunk.chunk_index, expected.slot_index
                    ),
                });
            };
            *entry = Some(shard);
            fetched.verified_shards = fetched.verified_shards.saturating_add(1);
        }

        Ok(())
    }

    async fn enqueue_repair<Q>(
        &self,
        manifest: &EcObjectManifest,
        chunk: &ChunkManifest,
        shard: &ShardManifest,
        kind: EcReadRepairKind,
        reason: impl Into<String>,
        repair_queue: &Q,
    ) -> bool
    where
        Q: EcReadRepairQueue,
    {
        let task = EcReadRepairTask {
            object: manifest.object_key(),
            operation_id: manifest.operation_id,
            erasure_set_id: manifest.erasure_set_id,
            chunk_index: chunk.chunk_index,
            slot_index: shard.slot_index,
            node_id: shard.node_id,
            kind,
            reason: reason.into(),
        };
        if let Err(error) = repair_queue.enqueue(task).await {
            tracing::warn!(
                error = %error,
                "failed to enqueue EC read repair task"
            );
            return false;
        }
        true
    }
}

/// The shards of one chunk in the order they are asked for, grouped by the pass
/// that asks for them.
struct ShardRequestOrder<'a> {
    /// Asked for first, and on a healthy chunk the only shards asked for at all.
    first: Vec<&'a ShardManifest>,
    /// Asked for only when the first group could not assemble the chunk.
    top_up: Vec<&'a ShardManifest>,
}

#[derive(Debug, Clone)]
struct FetchedShards {
    shards: Vec<Option<Shard>>,
    /// Slots already asked for, whatever the answer was.
    ///
    /// A read can approach the same chunk twice — the shards a range falls in
    /// first, the whole chunk after one of them is lost — and a slot that
    /// already failed must not be asked for, or paid for, a second time.
    asked: Vec<bool>,
    verified_shards: usize,
    repair_enqueued: bool,
}

impl FetchedShards {
    /// An empty set with one slot per shard of the profile.
    fn new(total_shards: usize) -> Self {
        Self {
            shards: vec![None; total_shards],
            asked: vec![false; total_shards],
            verified_shards: 0,
            repair_enqueued: false,
        }
    }

    /// Reserve this slot for a request.
    ///
    /// `true` means it was reserved already and must not be asked for a second
    /// time.
    fn claim(&mut self, slot_index: u8) -> bool {
        match self.asked.get_mut(usize::from(slot_index)) {
            Some(asked) if *asked => true,
            Some(asked) => {
                *asked = true;
                false
            }
            // Outside the profile's shard vector: left to the fetch loop, which
            // turns it into an invalid-manifest error rather than a silent skip.
            None => false,
        }
    }

    fn holds(&self, slot_index: u8) -> bool {
        self.shards.get(usize::from(slot_index)).is_some_and(Option::is_some)
    }
}

#[derive(Debug, Clone)]
struct EcChunkReadOutcome {
    bytes: Bytes,
    /// Whether the whole chunk had to be rebuilt to answer this read. False for
    /// a range served straight out of the data shards it falls in.
    rebuilt: bool,
    degraded: bool,
    repair_enqueued: bool,
    /// How the chunk came back. `Direct` when every data shard was there;
    /// otherwise whichever path the codec took, which is how a recovery that
    /// only the unified solver reached gets counted instead of passing in
    /// silence (decision EC2-103, section 5).
    recovery_path: RecoveryPath,
}

fn manifest_from_read_result(
    object: &EcObjectVersionKey,
    result: EcManifestReadResult,
) -> Result<EcObjectManifest, EcReadError> {
    match result {
        EcManifestReadResult::Present(manifest) => {
            ensure_manifest_readable(&manifest)?;
            Ok(manifest)
        }
        EcManifestReadResult::Tombstone(_) => Err(EcReadError::Deleted {
            bucket: object.bucket.clone(),
            key: object.key.clone(),
            version_id: object.version_id.clone(),
        }),
        EcManifestReadResult::Absent => Err(EcReadError::NotFound {
            bucket: object.bucket.clone(),
            key: object.key.clone(),
            version_id: object.version_id.clone(),
        }),
    }
}

fn ensure_manifest_readable(manifest: &EcObjectManifest) -> Result<(), EcReadError> {
    manifest.validate()?;
    if manifest.is_deleted || manifest.state == EcPlacementState::Tombstone {
        return Err(EcReadError::Deleted {
            bucket: manifest.bucket.clone(),
            key: manifest.key.clone(),
            version_id: manifest.version_id.clone(),
        });
    }
    if !manifest.state.is_ec_authoritative() {
        return Err(EcReadError::Unavailable {
            reason: format!(
                "manifest state {:?} is not readable by EC path",
                manifest.state
            ),
        });
    }
    Ok(())
}

fn head_from_manifest(manifest: &EcObjectManifest) -> EcHeadObject {
    EcHeadObject {
        object: manifest.object_key(),
        etag: manifest.etag.clone(),
        content_type: manifest.content_type.clone(),
        size: manifest.original_size,
        user_metadata: manifest.user_metadata.clone(),
        content_hash_sha256: manifest.content_hash_sha256,
        hlc: manifest.hlc,
        version_id: manifest.version_id.clone(),
        layout: EcReadLayout::ErasureCoded {
            erasure_set_id: manifest.erasure_set_id,
            topology_epoch: manifest.topology_epoch,
            profile_name: manifest.profile.name.clone(),
            chunk_count: manifest.chunks.len(),
        },
    }
}

fn resolve_range(range: Option<EcByteRange>, total: u64) -> Result<EffectiveRange, EcReadError> {
    match range {
        Some(range) => {
            if range.start > range.end || total == 0 || range.start >= total {
                return Err(EcReadError::InvalidRange {
                    start: range.start,
                    end: range.end,
                    total,
                });
            }
            Ok(EffectiveRange {
                start: range.start,
                end: range.end.min(total - 1),
                total,
            })
        }
        None => {
            if total == 0 {
                Ok(EffectiveRange {
                    start: 0,
                    end: 0,
                    total,
                })
            } else {
                Ok(EffectiveRange {
                    start: 0,
                    end: total - 1,
                    total,
                })
            }
        }
    }
}

fn ranges_intersect(left_start: u64, left_end: u64, right_start: u64, right_end: u64) -> bool {
    left_start < right_end && right_start < left_end
}

/// Check assembled bytes against the hash the segment index recorded.
///
/// End to end, and deliberately stronger than a shard hash: it catches bit rot
/// on disk the same way, but it also catches a mistake in the addressing
/// arithmetic, which a shard hash cannot — the shard would be perfectly intact
/// and simply the wrong one.
fn verify_packed_object(
    placement: &PackedSegmentObjectPlacement,
    body: &Bytes,
) -> Result<(), EcReadError> {
    if body.len() as u64 != placement.length {
        return Err(EcReadError::ChunkVerification {
            reason: format!(
                "packed object in segment {} assembled {} byte(s), expected {}",
                placement.segment_id,
                body.len(),
                placement.length
            ),
        });
    }
    if sha256(body) != placement.content_hash_sha256 {
        return Err(EcReadError::ChunkVerification {
            reason: format!(
                "packed object in segment {} does not match its recorded SHA-256",
                placement.segment_id
            ),
        });
    }
    Ok(())
}

/// The read layout reported for a packed object.
fn packed_read_layout(placement: &PackedSegmentObjectPlacement) -> EcReadLayout {
    EcReadLayout::PackedSegment {
        segment_id: placement.segment_id.as_uuid(),
        erasure_set_id: placement.erasure_set_id,
        offset: placement.offset,
        length: placement.length,
        hot_copies: !placement.hot_replica_nodes.is_empty(),
    }
}

/// The S3 facts about a packed object that its placement does not carry.
///
/// Size, content hash and clock are all in the placement; these three are not,
/// and after the hot copies are purged they live in two places: the segment's
/// index entry on the node that packed the object, and the object's own pointer
/// manifest everywhere. Both are the same facts, which is why the read path
/// takes either.
#[derive(Debug, Clone, PartialEq, Eq)]
struct PackedObjectFacts {
    etag: String,
    content_type: String,
    user_metadata: HashMap<String, String>,
}

impl PackedObjectFacts {
    fn from_entry(entry: &PackedSegmentEntry) -> Self {
        Self {
            etag: entry.etag.clone(),
            content_type: entry.content_type.clone(),
            user_metadata: entry.user_metadata.clone(),
        }
    }

    fn from_manifest(manifest: &EcObjectManifest) -> Self {
        Self {
            etag: manifest.etag.clone(),
            content_type: manifest.content_type.clone(),
            user_metadata: manifest.user_metadata.clone(),
        }
    }
}

/// HEAD metadata for a packed object whose hot copies are gone.
fn packed_head(
    object: EcObjectVersionKey,
    placement: &PackedSegmentObjectPlacement,
    facts: &PackedObjectFacts,
) -> EcHeadObject {
    EcHeadObject {
        version_id: object.version_id.clone(),
        object,
        etag: facts.etag.clone(),
        content_type: facts.content_type.clone(),
        size: placement.length,
        user_metadata: facts.user_metadata.clone(),
        content_hash_sha256: placement.content_hash_sha256,
        hlc: placement.hlc,
        layout: packed_read_layout(placement),
    }
}

/// One contiguous run of a packed object's bytes inside one data shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PackedShardSpan {
    /// Logical chunk of the segment this run belongs to.
    pub chunk_index: u32,
    /// Data-shard slot holding the run.
    pub slot_index: u8,
    /// Offset of the run from the start of that shard.
    pub offset_in_shard: u64,
    /// Length of the run.
    pub length: u64,
}

/// Work out which shard bytes hold `[offset, offset + length)` of a segment.
///
/// This is the whole of `EC2-063` expressed as arithmetic. It is sound only
/// because both codecs split a chunk into `k` **contiguous** blocks
/// (`split_data_shards` in `ee/s4-codec/src/rs.rs`, which `LrcCodec` also uses),
/// so byte `i` of a chunk sits at offset `i % shard_size` of data shard
/// `i / shard_size`, and data shard ordinals are their slot indices.
///
/// Chunk boundaries come from the manifest's own `data_size` values rather than
/// from multiplying by the profile's chunk size: the last chunk is short, and a
/// reader that assumes otherwise reads the wrong bytes from a valid manifest.
///
/// # Errors
///
/// Returns [`EcReadError::InvalidManifest`] when the manifest does not describe
/// a segment this arithmetic can address — in particular when a chunk's
/// `encoded_size` is not the splitter's block size, which would silently move
/// every byte after it.
fn locate_packed_object(
    manifest: &EcObjectManifest,
    offset: u64,
    length: u64,
) -> Result<Vec<PackedShardSpan>, EcReadError> {
    if length == 0 {
        return Ok(Vec::new());
    }
    let k = u64::from(manifest.profile.k);
    if k == 0 {
        return Err(EcReadError::InvalidManifest {
            reason: "profile declares zero data shards".to_string(),
        });
    }

    let mut spans = Vec::new();
    let mut chunk_start = 0u64;
    let mut cursor = offset;
    let mut remaining = length;

    for chunk in &manifest.chunks {
        let chunk_end = chunk_start.checked_add(chunk.data_size).ok_or_else(|| {
            EcReadError::InvalidManifest {
                reason: format!("chunk {} logical offset overflowed", chunk.chunk_index),
            }
        })?;
        if remaining == 0 {
            break;
        }
        if cursor >= chunk_end {
            chunk_start = chunk_end;
            continue;
        }

        // The splitter's block size, checked rather than assumed. Every offset
        // below is computed from it, so a manifest that disagrees would produce
        // confidently wrong bytes.
        let shard_size = chunk.data_size.div_ceil(k);
        if chunk.encoded_size != shard_size {
            return Err(EcReadError::InvalidManifest {
                reason: format!(
                    "chunk {} declares shard size {} but {} data byte(s) over k={k} split into {}",
                    chunk.chunk_index, chunk.encoded_size, chunk.data_size, shard_size
                ),
            });
        }

        let mut offset_in_chunk = cursor.saturating_sub(chunk_start);
        while remaining > 0 && offset_in_chunk < chunk.data_size {
            let ordinal = offset_in_chunk / shard_size;
            let slot_index = u8::try_from(ordinal).map_err(|_| EcReadError::InvalidManifest {
                reason: format!(
                    "chunk {} data shard {ordinal} does not fit in u8",
                    chunk.chunk_index
                ),
            })?;
            if u64::from(slot_index) >= k {
                return Err(EcReadError::InvalidManifest {
                    reason: format!(
                        "chunk {} offset {offset_in_chunk} lands on data shard {ordinal}, outside k={k}",
                        chunk.chunk_index
                    ),
                });
            }

            let shard_start = ordinal.saturating_mul(shard_size);
            // The last data shard is padded; only the bytes below `data_size`
            // are the object's.
            let shard_real_len = shard_size.min(chunk.data_size.saturating_sub(shard_start));
            let offset_in_shard = offset_in_chunk - shard_start;
            let take = remaining.min(shard_real_len.saturating_sub(offset_in_shard));
            if take == 0 {
                return Err(EcReadError::InvalidManifest {
                    reason: format!(
                        "chunk {} yielded no bytes at offset {offset_in_chunk}",
                        chunk.chunk_index
                    ),
                });
            }

            spans.push(PackedShardSpan {
                chunk_index: chunk.chunk_index,
                slot_index,
                offset_in_shard,
                length: take,
            });
            offset_in_chunk += take;
            remaining -= take;
        }

        cursor = chunk_start.saturating_add(offset_in_chunk);
        chunk_start = chunk_end;
    }

    if remaining > 0 {
        return Err(EcReadError::InvalidManifest {
            reason: format!(
                "object at {offset}+{length} runs {remaining} byte(s) past the end of the segment"
            ),
        });
    }
    Ok(spans)
}

/// How much of one chunk a read needs, and therefore what verifies it.
///
/// The line is drawn by coverage, not by the shape of the request: a long range
/// swallows its middle chunks whole and clips only the two at its edges, so
/// those middle chunks are verified exactly as a full `GET` verifies them
/// (decision `EC2-181`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkCoverage {
    /// Every byte of the chunk is wanted. The read fetches all `k` data shards,
    /// assembles the chunk by concatenation and checks the result against
    /// `ChunkManifest::chunk_sha256`.
    Full,
    /// Only part of the chunk is wanted. The read fetches just the data shards
    /// the plan names and checks each against its own `ShardManifest::sha256`:
    /// the chunk hash has no whole chunk to run on.
    Partial,
}

/// The bytes one read takes out of one data shard.
///
/// The shard itself is still fetched whole. `ShardManifest::sha256` covers the
/// whole shard, so bytes cut out of a partial fetch could not be verified at
/// all (decision `EC2-181`, which is also why [`EcShardReader::read_shard_range`]
/// stays out of this path). The span says which of those bytes belong to the
/// answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardReadSpan {
    /// Data shard slot the bytes come from.
    pub slot_index: u8,
    /// Offset of the first wanted byte inside that shard.
    pub offset_in_shard: u64,
    /// How many bytes are wanted from it.
    pub length: u64,
}

/// What one read has to do with one chunk it touches.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkReadPlan {
    /// Chunk this entry is about.
    pub chunk_index: u32,
    /// How much of the chunk the read needs, which decides how it is verified.
    pub coverage: ChunkCoverage,
    /// Offset of the first byte of the chunk the read answers with.
    pub wanted_from: u64,
    /// One past the offset of the last byte of the chunk the read answers with.
    ///
    /// `spans` is this window cut up by data shard; the window itself is what a
    /// chunk that had to be rebuilt whole is trimmed to.
    pub wanted_to: u64,
    /// Data slots to fetch, ascending and each named once.
    ///
    /// On [`ChunkCoverage::Full`] this is every data slot of the profile,
    /// including one that holds nothing but padding: assembly concatenates all
    /// `k`. On [`ChunkCoverage::Partial`] it is exactly the slots `spans` names.
    pub slots: Vec<u8>,
    /// Which bytes of the answer come from which slot, in object order.
    pub spans: Vec<ShardReadSpan>,
}

/// What one read has to fetch, chunk by chunk, in object order.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReadPlan {
    /// Chunks the range touches, ascending. Chunks outside it are absent.
    pub chunks: Vec<ChunkReadPlan>,
}

impl ReadPlan {
    /// Data shards this plan fetches in total.
    ///
    /// This is the number the saving is measured in: on a healthy full read it
    /// is `k` per chunk rather than `n`.
    #[must_use]
    pub fn shard_reads(&self) -> usize {
        self.chunks.iter().map(|chunk| chunk.slots.len()).sum()
    }

    /// Object bytes the spans name in total, which is the length of the answer.
    #[must_use]
    pub fn bytes(&self) -> u64 {
        self.chunks
            .iter()
            .flat_map(|chunk| chunk.spans.iter())
            .map(|span| span.length)
            .sum()
    }
}

/// Work out which data shards a read of `range` needs, without any I/O.
///
/// This is the read side of the arithmetic `locate_packed_object` already does
/// for packed segments, and it is sound for the same reason: both codecs split a
/// chunk into `k` **contiguous** blocks (`split_data_shards` in
/// `ee/s4-codec/src/rs.rs`, which `LrcCodec` uses too), and the ordinal of a data
/// shard is its slot index. So byte `i` of a chunk sits at offset
/// `i % shard_size` of data shard `i / shard_size`, and the shards that cover a
/// range can be named before anything is fetched.
///
/// `None` for `range` plans the whole object. The range is resolved exactly as
/// the read path resolves it, so a plan and the read it describes cannot
/// disagree about which bytes were asked for.
///
/// Nothing here is taken on trust that can be checked: the shard size comes from
/// the manifest and is checked against `data_size.div_ceil(k)`, slot indices are
/// checked against `k`, and the finished plan is checked to cover the requested
/// range exactly. A manifest that argues with itself is rejected rather than
/// allowed to produce confidently wrong bytes — which matters more here than
/// elsewhere, because a partial read is verified by shard hashes, and a shard
/// hash cannot notice that the wrong shard was read (decision `EC2-181`).
///
/// # Errors
///
/// Returns [`EcReadError::InvalidRange`] when the range lies outside the object,
/// and [`EcReadError::InvalidManifest`] when the manifest cannot be addressed by
/// this arithmetic.
pub fn plan_read(
    manifest: &EcObjectManifest,
    range: Option<EcByteRange>,
) -> Result<ReadPlan, EcReadError> {
    let effective = resolve_range(range, manifest.original_size)?;
    plan_effective_read(manifest, effective)
}

fn plan_effective_read(
    manifest: &EcObjectManifest,
    range: EffectiveRange,
) -> Result<ReadPlan, EcReadError> {
    let k = u64::from(manifest.profile.k);
    if k == 0 {
        return Err(EcReadError::InvalidManifest {
            reason: "profile declares zero data shards".to_string(),
        });
    }

    let range_end = range.end_exclusive();
    let mut chunks = Vec::new();
    let mut next_chunk_start = 0u64;

    for chunk in &manifest.chunks {
        let chunk_start = next_chunk_start;
        let chunk_end = chunk_start.checked_add(chunk.data_size).ok_or_else(|| {
            EcReadError::InvalidManifest {
                reason: format!("chunk {} logical offset overflowed", chunk.chunk_index),
            }
        })?;
        next_chunk_start = chunk_end;

        if !ranges_intersect(chunk_start, chunk_end, range.start, range_end) {
            continue;
        }

        let shard_size = data_shard_size(chunk, k)?;
        let wanted_from = range.start.max(chunk_start) - chunk_start;
        let wanted_to = range_end.min(chunk_end) - chunk_start;
        chunks.push(plan_chunk(chunk, k, shard_size, wanted_from, wanted_to)?);
    }

    let plan = ReadPlan { chunks };
    // The chunks are supposed to sum to the object, and `EcObjectManifest`
    // validation says they do. Checking it here as well costs a walk over a
    // handful of spans and turns "the manifest lied about its own length" into a
    // refusal instead of a short answer.
    if plan.bytes() != range.len() {
        return Err(EcReadError::InvalidManifest {
            reason: format!(
                "chunks cover {} of the {} requested byte(s)",
                plan.bytes(),
                range.len()
            ),
        });
    }
    Ok(plan)
}

/// Plan the data shards covering `[wanted_from, wanted_to)` inside one chunk.
fn plan_chunk(
    chunk: &ChunkManifest,
    k: u64,
    shard_size: u64,
    wanted_from: u64,
    wanted_to: u64,
) -> Result<ChunkReadPlan, EcReadError> {
    // The one guard that keeps spans out of padding. The last data shard of a
    // chunk is padded up to the shard size, and that padding is not the
    // object's; holding the window inside `data_size` here bounds every span
    // below, so none of them has to clamp again.
    if wanted_to <= wanted_from || wanted_to > chunk.data_size {
        return Err(EcReadError::InvalidManifest {
            reason: format!(
                "chunk {} was asked for {wanted_from}..{wanted_to} of {} byte(s)",
                chunk.chunk_index, chunk.data_size
            ),
        });
    }

    let first = wanted_from / shard_size;
    let last = (wanted_to - 1) / shard_size;
    let mut spans = Vec::with_capacity(usize::try_from(last - first + 1).unwrap_or(0));

    for ordinal in first..=last {
        let slot_index = data_slot_index(chunk.chunk_index, ordinal, k)?;
        let shard_start =
            ordinal.checked_mul(shard_size).ok_or_else(|| EcReadError::InvalidManifest {
                reason: format!(
                    "chunk {} data shard {ordinal} starts past the end of u64",
                    chunk.chunk_index
                ),
            })?;
        let offset_in_shard = wanted_from.saturating_sub(shard_start);
        let take_to = wanted_to.min(shard_start.saturating_add(shard_size)) - shard_start;
        let length =
            take_to
                .checked_sub(offset_in_shard)
                .filter(|length| *length > 0)
                .ok_or_else(|| EcReadError::InvalidManifest {
                    reason: format!(
                        "chunk {} yields no bytes from data shard {ordinal}",
                        chunk.chunk_index
                    ),
                })?;
        spans.push(ShardReadSpan {
            slot_index,
            offset_in_shard,
            length,
        });
    }

    let coverage = if wanted_from == 0 && wanted_to == chunk.data_size {
        ChunkCoverage::Full
    } else {
        ChunkCoverage::Partial
    };
    let slots = match coverage {
        // Assembly concatenates every data shard, so all `k` are fetched even
        // when the last of them is padding from end to end — which happens when
        // a short chunk has fewer bytes than it has data slots.
        ChunkCoverage::Full => (0..k)
            .map(|ordinal| data_slot_index(chunk.chunk_index, ordinal, k))
            .collect::<Result<Vec<_>, _>>()?,
        ChunkCoverage::Partial => spans.iter().map(|span| span.slot_index).collect(),
    };

    Ok(ChunkReadPlan {
        chunk_index: chunk.chunk_index,
        coverage,
        wanted_from,
        wanted_to,
        slots,
        spans,
    })
}

/// The splitter's block size for one chunk, checked instead of assumed.
///
/// Every offset in a read plan is derived from it, and `ChunkManifest::validate`
/// only asks that `encoded_size * k` covers `data_size`. A manifest declaring a
/// larger block would stay formally valid and move every byte after it, so it is
/// refused here — the same guard `locate_packed_object` carries for the same
/// reason (decision `EC2-063`).
fn data_shard_size(chunk: &ChunkManifest, k: u64) -> Result<u64, EcReadError> {
    let shard_size = chunk.data_size.div_ceil(k);
    if shard_size == 0 {
        return Err(EcReadError::InvalidManifest {
            reason: format!("chunk {} declares no data bytes", chunk.chunk_index),
        });
    }
    if chunk.encoded_size != shard_size {
        return Err(EcReadError::InvalidManifest {
            reason: format!(
                "chunk {} declares shard size {} but {} data byte(s) over k={k} split into {}",
                chunk.chunk_index, chunk.encoded_size, chunk.data_size, shard_size
            ),
        });
    }
    Ok(shard_size)
}

/// The slot holding the `ordinal`-th data shard, which is the ordinal itself.
///
/// True because slots `0..k` are `ShardRole::Data` in both codec families
/// (`EcProfileSnapshot::role_for_slot`). Checked rather than assumed, so a
/// profile that ever stopped being systematic would fail loudly instead of
/// serving parity bytes as data.
fn data_slot_index(chunk_index: u32, ordinal: u64, k: u64) -> Result<u8, EcReadError> {
    if ordinal >= k {
        return Err(EcReadError::InvalidManifest {
            reason: format!("chunk {chunk_index} addresses data shard {ordinal}, outside k={k}"),
        });
    }
    u8::try_from(ordinal).map_err(|_| EcReadError::InvalidManifest {
        reason: format!("chunk {chunk_index} data shard {ordinal} does not fit in u8"),
    })
}

/// The node the manifest places one shard slot of one chunk on.
fn shard_owner(
    manifest: &EcObjectManifest,
    chunk_index: u32,
    slot_index: u8,
) -> Result<(NodeId, &ShardManifest), EcReadError> {
    let chunk = manifest
        .chunks
        .iter()
        .find(|chunk| chunk.chunk_index == chunk_index)
        .ok_or_else(|| EcReadError::InvalidManifest {
            reason: format!("manifest has no chunk {chunk_index}"),
        })?;
    let shard =
        chunk
            .shards
            .iter()
            .find(|shard| shard.slot_index == slot_index)
            .ok_or_else(|| EcReadError::InvalidManifest {
                reason: format!("chunk {chunk_index} has no shard slot {slot_index}"),
            })?;
    if shard.role != ShardRole::Data {
        return Err(EcReadError::InvalidManifest {
            reason: format!(
                "chunk {chunk_index} slot {slot_index} is {:?}, not a data shard",
                shard.role
            ),
        });
    }
    Ok((shard.node_id, shard))
}

fn slice_bytes(bytes: &Bytes, start: u64, end: u64) -> Result<Bytes, EcReadError> {
    let start = usize::try_from(start).map_err(|_| EcReadError::InvalidRange {
        start,
        end,
        total: bytes.len() as u64,
    })?;
    let end = usize::try_from(end).map_err(|_| EcReadError::InvalidRange {
        start: start as u64,
        end,
        total: bytes.len() as u64,
    })?;
    if start > end || end > bytes.len() {
        return Err(EcReadError::InvalidRange {
            start: start as u64,
            end: end as u64,
            total: bytes.len() as u64,
        });
    }
    Ok(bytes.slice(start..end))
}

fn fast_path_assemble_data_shards(
    profile: &s4_codec::EcProfileSnapshot,
    chunk: &ChunkManifest,
    shards: &[Option<Shard>],
) -> Result<Option<Bytes>, EcReadError> {
    let mut data_shards = Vec::with_capacity(usize::from(profile.k));
    for shard_manifest in &chunk.shards {
        let Some(data_index) = data_index_for_shard(profile, shard_manifest)? else {
            continue;
        };
        let Some(shard) =
            shards.get(usize::from(shard_manifest.slot_index)).and_then(Option::as_ref)
        else {
            continue;
        };
        data_shards.push((data_index, shard.data.clone()));
    }

    if data_shards.len() != usize::from(profile.k) {
        return Ok(None);
    }

    data_shards.sort_by_key(|(data_index, _)| *data_index);
    let target_len =
        usize::try_from(chunk.data_size).map_err(|_| EcReadError::InvalidManifest {
            reason: format!(
                "chunk {} data_size does not fit in usize",
                chunk.chunk_index
            ),
        })?;
    let mut assembled = Vec::with_capacity(target_len);
    for (_, shard_bytes) in data_shards {
        if assembled.len() >= target_len {
            break;
        }
        let remaining = target_len - assembled.len();
        assembled.extend_from_slice(&shard_bytes[..remaining.min(shard_bytes.len())]);
    }
    if assembled.len() != target_len {
        return Err(EcReadError::InvalidManifest {
            reason: format!(
                "chunk {} data shards assembled {} byte(s), expected {}",
                chunk.chunk_index,
                assembled.len(),
                target_len
            ),
        });
    }
    Ok(Some(Bytes::from(assembled)))
}

fn data_index_for_shard(
    profile: &s4_codec::EcProfileSnapshot,
    shard: &ShardManifest,
) -> Result<Option<u8>, EcReadError> {
    if shard.role != ShardRole::Data {
        return Ok(None);
    }
    if shard.slot_index >= profile.k {
        return Err(EcReadError::InvalidManifest {
            reason: format!(
                "data shard slot {} is outside k={}",
                shard.slot_index, profile.k
            ),
        });
    }
    Ok(Some(shard.slot_index))
}

fn verify_chunk_hash(chunk: &ChunkManifest, bytes: &[u8]) -> Result<(), EcReadError> {
    let actual = sha256(bytes);
    if actual != chunk.chunk_sha256 {
        return Err(EcReadError::ChunkVerification {
            reason: format!(
                "chunk {} SHA-256 mismatch after reconstruction",
                chunk.chunk_index
            ),
        });
    }
    Ok(())
}

fn sha256(bytes: &[u8]) -> [u8; 32] {
    Sha256::new().chain_update(bytes).finalize().into()
}

/// Errors returned by the EC read path.
#[derive(Debug, Error)]
pub enum EcReadError {
    /// The object/version was quorum-confirmed absent.
    #[error("EC object not found: {bucket}/{key} version {version_id:?}")]
    NotFound {
        /// Bucket name.
        bucket: String,
        /// Object key.
        key: String,
        /// Version ID.
        version_id: Option<String>,
    },

    /// The object/version was quorum-confirmed deleted.
    #[error("EC object is deleted: {bucket}/{key} version {version_id:?}")]
    Deleted {
        /// Bucket name.
        bucket: String,
        /// Object key.
        key: String,
        /// Version ID.
        version_id: Option<String>,
    },

    /// The byte range cannot be satisfied.
    #[error("invalid EC read range {start}-{end} for object size {total}")]
    InvalidRange {
        /// Requested start.
        start: u64,
        /// Requested end.
        end: u64,
        /// Total object size.
        total: u64,
    },

    /// EC manifest or layout did not match durable references.
    #[error("EC manifest mismatch: {reason}")]
    ManifestMismatch {
        /// Human-readable reason.
        reason: String,
    },

    /// Manifest metadata is internally inconsistent.
    #[error("invalid EC read manifest: {reason}")]
    InvalidManifest {
        /// Human-readable reason.
        reason: String,
    },

    /// Not enough verified shards are available for a read.
    #[error("EC read unavailable: {reason}")]
    Unavailable {
        /// Human-readable reason.
        reason: String,
    },

    /// A shard IO operation failed.
    #[error("EC shard read failed: {reason}")]
    ShardIo {
        /// Human-readable reason.
        reason: String,
    },

    /// Reconstructed chunk bytes did not match the manifest.
    #[error("EC chunk verification failed: {reason}")]
    ChunkVerification {
        /// Human-readable reason.
        reason: String,
    },

    /// RF=3 small-object bytes did not match explicit placement metadata.
    #[error("EC replicated-small verification failed: {reason}")]
    ReplicatedSmallVerification {
        /// Human-readable reason.
        reason: String,
    },

    /// The object's RF=3 copies hold a version written after the
    /// replicated-small placement was recorded: the object was overwritten,
    /// and the placement describes the version before. The copies are the
    /// object; the placement is stale and the object is read as a hot one.
    #[error(
        "EC replicated-small placement of version {placement_hlc:?} is superseded by version \
         {current_hlc:?}"
    )]
    ReplicatedSmallSuperseded {
        /// The version the placement was recorded for.
        placement_hlc: HlcTimestamp,
        /// The version the copies hold now.
        current_hlc: HlcTimestamp,
    },

    /// EC metadata operation failed.
    #[error(transparent)]
    Metadata(#[from] EcMetadataError),

    /// Codec operation failed.
    #[error(transparent)]
    Codec(#[from] CodecError),

    /// CPU limiter operation failed.
    #[error(transparent)]
    Cpu(#[from] EcCpuError),
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::{HashMap, HashSet};
    use std::num::NonZeroUsize;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Mutex;
    use std::time::Duration;

    use crate::codec_runtime::{EcCodecCpuConfig, EcCodecCpuLimiter};
    use crate::ec_packed_segments::{
        packed_segment_manifest_key, PackedObjectIdentity, PackedSegmentConfig, PackedSegmentId,
        SealReason,
    };
    use s4_codec::{EcParity, EcProfileSnapshot};

    fn node(index: u8) -> NodeId {
        NodeId(u128::from(index))
    }

    fn hlc() -> HlcTimestamp {
        HlcTimestamp {
            wall_time: 10,
            logical: 2,
            node_id: 1,
        }
    }

    fn operation_id() -> Uuid {
        Uuid::from_u128(0xabc)
    }

    fn object() -> EcObjectVersionKey {
        EcObjectVersionKey::new("photos", "raw.bin", Some("v1".to_string()))
    }

    fn coordinator(store: Arc<EcMetadataStore>) -> EcReadCoordinator {
        let cpu = EcCodecCpuLimiter::new(EcCodecCpuConfig {
            workers: NonZeroUsize::new(1).unwrap(),
            cpu_budget_percent: 60,
            max_inflight_encode_bytes: 1024 * 1024,
        });
        EcReadCoordinator::new(store, cpu)
    }

    fn encoded_manifest(
        profile: EcProfileSnapshot,
        data: &[u8],
    ) -> (EcObjectManifest, HashMap<(u32, u8), Bytes>) {
        let codec = crate::ec_codec::build_codec(profile.clone()).unwrap();
        let chunk_size = usize::try_from(profile.chunk_size).unwrap();
        let mut chunks = Vec::new();
        let mut shard_bytes = HashMap::new();
        for (chunk_index, bytes) in data.chunks(chunk_size).enumerate() {
            let chunk_index = u32::try_from(chunk_index).unwrap();
            let encoded = codec.encode_chunk(bytes, chunk_index).unwrap();
            let shards = encoded
                .shards
                .iter()
                .map(|shard| {
                    shard_bytes.insert((chunk_index, shard.slot_index), shard.data.clone());
                    ShardManifest {
                        slot_index: shard.slot_index,
                        node_id: node(shard.slot_index + 1),
                        role: shard.role,
                        crc32: shard.crc32,
                        sha256: shard.sha256,
                        size: shard.size,
                    }
                })
                .collect();
            chunks.push(ChunkManifest {
                chunk_index,
                data_size: encoded.original_size,
                encoded_size: encoded.shard_size,
                chunk_sha256: sha256(bytes),
                shards,
            });
        }

        let manifest = EcObjectManifest {
            format_version: crate::ec_metadata::EC_MANIFEST_FORMAT_VERSION,
            bucket: "photos".to_string(),
            key: "raw.bin".to_string(),
            version_id: Some("v1".to_string()),
            original_size: data.len() as u64,
            content_hash_sha256: sha256(data),
            etag: "\"etag\"".to_string(),
            content_type: "application/octet-stream".to_string(),
            user_metadata: HashMap::from([("x-amz-meta-owner".to_string(), "qa".to_string())]),
            profile,
            erasure_set_id: 9,
            topology_epoch: 3,
            chunks,
            state: EcPlacementState::EcCommitted,
            hlc: hlc(),
            origin_node_id: node(1),
            operation_id: operation_id(),
            created_at: 42,
            is_deleted: false,
            packed: None,
        };
        manifest.validate().unwrap();
        (manifest, shard_bytes)
    }

    fn rs_profile(chunk_size: u64) -> EcProfileSnapshot {
        let mut profile = EcProfileSnapshot::rs_small();
        profile.chunk_size = chunk_size;
        profile
    }

    fn lrc_profile(chunk_size: u64) -> EcProfileSnapshot {
        let mut profile = EcProfileSnapshot::lrc_small();
        profile.chunk_size = chunk_size;
        profile
    }

    /// A `k = 6` profile, so the read-plan checks do not all run on `k = 4`.
    fn rs_standard_profile(chunk_size: u64) -> EcProfileSnapshot {
        let mut profile = EcProfileSnapshot::rs_standard();
        profile.chunk_size = chunk_size;
        profile
    }

    #[derive(Default)]
    struct MemoryMetadata {
        layout: Mutex<Option<EcObjectLayoutReadResult>>,
        manifest: Mutex<Option<EcManifestReadResult>>,
        layout_reads: AtomicUsize,
        manifest_reads: AtomicUsize,
    }

    #[async_trait]
    impl EcReadMetadataCoordinator for MemoryMetadata {
        async fn quorum_read_object_layout(
            &self,
            _object: &EcObjectVersionKey,
        ) -> Result<EcObjectLayoutReadResult, EcReadError> {
            self.layout_reads.fetch_add(1, Ordering::SeqCst);
            Ok(self.layout.lock().unwrap().clone().unwrap_or(EcObjectLayoutReadResult::Absent))
        }

        async fn quorum_read_manifest(
            &self,
            _object: &EcObjectVersionKey,
        ) -> Result<EcManifestReadResult, EcReadError> {
            self.manifest_reads.fetch_add(1, Ordering::SeqCst);
            Ok(self.manifest.lock().unwrap().clone().unwrap_or(EcManifestReadResult::Absent))
        }
    }

    #[derive(Default)]
    struct MemoryShardReader {
        shards: Mutex<HashMap<(u32, u8), Bytes>>,
        missing: Mutex<HashSet<(u32, u8)>>,
        corrupt: Mutex<HashSet<(u32, u8)>>,
        reads: AtomicUsize,
        bytes_returned: AtomicUsize,
        requests: Mutex<Vec<EcReadShardRequest>>,
        /// What one fetch costs, standing in for a network round trip.
        ///
        /// Zero unless a test asks for it. Under a paused clock the runtime
        /// advances time instead of waiting, so a read making `f` fetches one
        /// after another takes exactly `f` hops of test time: the round-trip
        /// part of read latency, measured rather than reasoned about.
        fetch_delay: Mutex<Duration>,
    }

    impl MemoryShardReader {
        fn new(shards: HashMap<(u32, u8), Bytes>) -> Self {
            Self {
                shards: Mutex::new(shards),
                ..Self::default()
            }
        }

        /// The same reader, with every fetch costing one `delay`.
        fn with_fetch_delay(self, delay: Duration) -> Self {
            *self.fetch_delay.lock().unwrap() = delay;
            self
        }

        async fn pay_for_a_round_trip(&self) {
            let delay = *self.fetch_delay.lock().unwrap();
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
        }

        fn missing_slots(&self, missing: &[(u32, u8)]) {
            self.missing.lock().unwrap().extend(missing.iter().copied());
        }

        fn corrupt_slots(&self, corrupt: &[(u32, u8)]) {
            self.corrupt.lock().unwrap().extend(corrupt.iter().copied());
        }

        fn bytes_returned(&self) -> usize {
            self.bytes_returned.load(Ordering::SeqCst)
        }

        /// Every request in the order it was made, duplicates kept.
        fn all_requests(&self) -> Vec<(u32, u8)> {
            self.requests
                .lock()
                .unwrap()
                .iter()
                .map(|request| (request.chunk_index, request.slot_index))
                .collect()
        }

        fn slots_requested(&self) -> Vec<(u32, u8)> {
            let requests = self.requests.lock().unwrap();
            let mut seen = HashSet::new();
            let mut result = Vec::new();
            for req in requests.iter() {
                let pair = (req.chunk_index, req.slot_index);
                if seen.insert(pair) {
                    result.push(pair);
                }
            }
            result
        }
    }

    #[async_trait]
    impl EcShardReader for MemoryShardReader {
        async fn read_shard(
            &self,
            request: EcReadShardRequest,
        ) -> Result<Option<EcReadShardResponse>, EcReadError> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            self.requests.lock().unwrap().push(request.clone());
            self.pay_for_a_round_trip().await;
            if self
                .missing
                .lock()
                .unwrap()
                .contains(&(request.chunk_index, request.slot_index))
            {
                return Ok(None);
            }
            let Some(mut data) = self
                .shards
                .lock()
                .unwrap()
                .get(&(request.chunk_index, request.slot_index))
                .cloned()
            else {
                return Ok(None);
            };
            if self
                .corrupt
                .lock()
                .unwrap()
                .contains(&(request.chunk_index, request.slot_index))
            {
                let mut bytes = data.to_vec();
                bytes[0] ^= 0xff;
                data = Bytes::from(bytes);
            }
            self.bytes_returned.fetch_add(data.len(), Ordering::SeqCst);
            Ok(Some(EcReadShardResponse {
                replica_crc32: Some(crc32fast::hash(&data)),
                data,
            }))
        }

        async fn read_shard_range(
            &self,
            request: EcReadShardRequest,
            offset: u64,
            length: u64,
        ) -> Result<Option<Bytes>, EcReadError> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            self.requests.lock().unwrap().push(request.clone());
            self.pay_for_a_round_trip().await;
            if self
                .missing
                .lock()
                .unwrap()
                .contains(&(request.chunk_index, request.slot_index))
            {
                return Ok(None);
            }
            let Some(data) = self
                .shards
                .lock()
                .unwrap()
                .get(&(request.chunk_index, request.slot_index))
                .cloned()
            else {
                return Ok(None);
            };
            let mut sliced = slice_bytes(&data, offset, offset.saturating_add(length))?;
            let corrupt = self
                .corrupt
                .lock()
                .unwrap()
                .contains(&(request.chunk_index, request.slot_index));
            if corrupt && !sliced.is_empty() {
                let mut bytes = sliced.to_vec();
                bytes[0] ^= 0xff;
                sliced = Bytes::from(bytes);
            }
            self.bytes_returned.fetch_add(sliced.len(), Ordering::SeqCst);
            Ok(Some(sliced))
        }
    }

    #[derive(Default)]
    struct MemoryRepairQueue {
        tasks: Mutex<Vec<EcReadRepairTask>>,
        fail: AtomicBool,
    }

    #[async_trait]
    impl EcReadRepairQueue for MemoryRepairQueue {
        async fn enqueue(&self, task: EcReadRepairTask) -> Result<(), EcReadError> {
            if self.fail.load(Ordering::SeqCst) {
                return Err(EcReadError::ShardIo {
                    reason: "repair queue unavailable".to_string(),
                });
            }
            self.tasks.lock().unwrap().push(task);
            Ok(())
        }
    }

    /// Records what the read counts, so a claim about a metric can be checked
    /// instead of described.
    ///
    /// Installed with `metrics::set_default_local_recorder`, which is
    /// thread-local: the rest of the suite keeps whatever recorder it had, and
    /// two tests counting at once do not see each other.
    #[derive(Default)]
    struct CountingRecorder {
        counters: Arc<Mutex<HashMap<CounterSeries, u64>>>,
    }

    impl CountingRecorder {
        /// Every series of `name`, whatever labels it carries, added up.
        fn count(&self, name: &str) -> u64 {
            self.sum(name, &[])
        }

        /// The series of `name` that carry `labels`, added up. A metric split
        /// by scope is read one scope at a time, which is what makes "fewer
        /// bytes crossed a zone" a number rather than an inference.
        fn count_where(&self, name: &str, labels: &[(&str, &str)]) -> u64 {
            self.sum(name, labels)
        }

        fn sum(&self, name: &str, labels: &[(&str, &str)]) -> u64 {
            self.counters
                .lock()
                .unwrap()
                .iter()
                .filter(|(series, _)| series.name == name && series.carries(labels))
                .map(|(_, value)| *value)
                .sum()
        }
    }

    /// One counter series: the metric name and the labels it was recorded with.
    #[derive(Clone, PartialEq, Eq, Hash)]
    struct CounterSeries {
        name: String,
        labels: Vec<(String, String)>,
    }

    impl CounterSeries {
        fn from_key(key: &metrics::Key) -> Self {
            Self {
                name: key.name().to_string(),
                labels: key
                    .labels()
                    .map(|label| (label.key().to_string(), label.value().to_string()))
                    .collect(),
            }
        }

        fn carries(&self, labels: &[(&str, &str)]) -> bool {
            labels.iter().all(|(key, value)| {
                self.labels
                    .iter()
                    .any(|(have_key, have_value)| have_key == key && have_value == value)
            })
        }
    }

    struct CountedMetric {
        series: CounterSeries,
        counters: Arc<Mutex<HashMap<CounterSeries, u64>>>,
    }

    impl metrics::CounterFn for CountedMetric {
        fn increment(&self, value: u64) {
            let mut counters = self.counters.lock().unwrap();
            *counters.entry(self.series.clone()).or_default() += value;
        }

        fn absolute(&self, value: u64) {
            let mut counters = self.counters.lock().unwrap();
            let entry = counters.entry(self.series.clone()).or_default();
            *entry = (*entry).max(value);
        }
    }

    impl metrics::Recorder for CountingRecorder {
        fn describe_counter(
            &self,
            _key: metrics::KeyName,
            _unit: Option<metrics::Unit>,
            _description: metrics::SharedString,
        ) {
        }

        fn describe_gauge(
            &self,
            _key: metrics::KeyName,
            _unit: Option<metrics::Unit>,
            _description: metrics::SharedString,
        ) {
        }

        fn describe_histogram(
            &self,
            _key: metrics::KeyName,
            _unit: Option<metrics::Unit>,
            _description: metrics::SharedString,
        ) {
        }

        fn register_counter(
            &self,
            key: &metrics::Key,
            _metadata: &metrics::Metadata<'_>,
        ) -> metrics::Counter {
            metrics::Counter::from_arc(Arc::new(CountedMetric {
                series: CounterSeries::from_key(key),
                counters: Arc::clone(&self.counters),
            }))
        }

        fn register_gauge(
            &self,
            _key: &metrics::Key,
            _metadata: &metrics::Metadata<'_>,
        ) -> metrics::Gauge {
            metrics::Gauge::noop()
        }

        fn register_histogram(
            &self,
            _key: &metrics::Key,
            _metadata: &metrics::Metadata<'_>,
        ) -> metrics::Histogram {
            metrics::Histogram::noop()
        }
    }

    #[derive(Default)]
    struct MemorySmallReader {
        body: Mutex<Bytes>,
    }

    #[async_trait]
    impl EcReplicatedSmallReader for MemorySmallReader {
        async fn read_replicated_small(
            &self,
            request: EcReplicatedSmallReadRequest,
        ) -> Result<EcReplicatedSmallReadResult, EcReadError> {
            let body = self.body.lock().unwrap().clone();
            let effective = resolve_range(request.range, request.placement.size)?;
            let data = if request.range.is_some() {
                slice_bytes(&body, effective.start, effective.end_exclusive())?
            } else {
                body
            };
            Ok(EcReplicatedSmallReadResult {
                content_length: data.len() as u64,
                content_range: request
                    .range
                    .map(|_| (effective.start, effective.end, effective.total)),
                head: small_head(&request.object, &request.placement),
                body: data,
                replicas_responded: 2,
            })
        }

        async fn head_replicated_small(
            &self,
            request: EcReplicatedSmallHeadRequest,
        ) -> Result<EcHeadObject, EcReadError> {
            Ok(small_head(&request.object, &request.placement))
        }
    }

    fn small_head(
        object: &EcObjectVersionKey,
        placement: &ReplicatedSmallObjectPlacement,
    ) -> EcHeadObject {
        EcHeadObject {
            object: object.clone(),
            etag: "\"small\"".to_string(),
            content_type: "application/octet-stream".to_string(),
            size: placement.size,
            user_metadata: HashMap::new(),
            content_hash_sha256: placement.content_hash_sha256,
            hlc: placement.hlc,
            version_id: object.version_id.clone(),
            layout: EcReadLayout::ReplicatedSmall {
                replica_nodes: placement.replica_nodes.clone(),
            },
        }
    }

    async fn install_quorum_manifest(
        metadata: &MemoryMetadata,
        manifest: &EcObjectManifest,
    ) -> EcObjectManifestRef {
        let reference = EcObjectManifestRef::from_manifest(manifest).unwrap();
        *metadata.layout.lock().unwrap() = Some(EcObjectLayoutReadResult::Present(
            ObjectDataLayout::ErasureCoded(reference.clone()),
        ));
        *metadata.manifest.lock().unwrap() = Some(EcManifestReadResult::Present(manifest.clone()));
        reference
    }

    fn install_quorum_layout(metadata: &MemoryMetadata, layout: ObjectDataLayout) {
        *metadata.layout.lock().unwrap() = Some(EcObjectLayoutReadResult::Present(layout));
    }

    /// Every shard request must name the set the manifest placed the object in.
    ///
    /// A pool may hold several sets, and a slot index only identifies a node
    /// inside one of them. Without the set on the request the reader could not
    /// tell a healthy candidate from a node that holds the same slot number
    /// somewhere else, so this is the fact the whole multi-set read path rests
    /// on.
    #[tokio::test]
    async fn shard_requests_name_the_set_the_manifest_placed_the_object_in() {
        let data = (0..128).map(|idx| (idx % 251) as u8).collect::<Vec<_>>();
        let (mut manifest, shard_bytes) = encoded_manifest(rs_profile(17), &data);
        manifest.erasure_set_id = 4_242;
        let dir = tempfile::tempdir().unwrap();
        let local = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let coordinator = coordinator(local);
        let metadata = MemoryMetadata::default();
        install_quorum_manifest(&metadata, &manifest).await;
        let small = MemorySmallReader::default();
        let shards = MemoryShardReader::new(shard_bytes);
        let repairs = MemoryRepairQueue::default();

        let result = coordinator
            .read(
                EcReadRequest::full(object()),
                &metadata,
                &small,
                &shards,
                &repairs,
            )
            .await
            .unwrap();

        assert_eq!(result.body.as_ref(), data.as_slice());
        let requests = shards.requests.lock().unwrap().clone();
        assert!(!requests.is_empty(), "the read fetched no shard at all");
        for request in &requests {
            assert_eq!(
                request.erasure_set_id, 4_242,
                "shard request for chunk {} slot {} named the wrong set",
                request.chunk_index, request.slot_index
            );
        }

        // The node on each request is the one the manifest assigned to that
        // slot, which is what sends the read to this set's nodes and to no
        // other set's. A healthy read asks only for data shards, so those are
        // the slots the check walks (decision EC2-180).
        for chunk in &manifest.chunks {
            for shard in chunk.shards.iter().filter(|shard| shard.role == ShardRole::Data) {
                assert!(
                    requests.iter().any(|request| request.chunk_index == chunk.chunk_index
                        && request.slot_index == shard.slot_index
                        && request.node_id == shard.node_id),
                    "no request went to the node holding chunk {} slot {}",
                    chunk.chunk_index,
                    shard.slot_index
                );
            }
        }
    }

    #[tokio::test]
    async fn local_layout_miss_uses_quorum_before_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let local = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let coordinator = coordinator(local);
        let metadata = MemoryMetadata::default();
        let small = MemorySmallReader::default();
        let shards = MemoryShardReader::default();
        let repairs = MemoryRepairQueue::default();

        let err = coordinator
            .read(
                EcReadRequest::full(object()),
                &metadata,
                &small,
                &shards,
                &repairs,
            )
            .await
            .unwrap_err();

        assert!(matches!(err, EcReadError::NotFound { .. }));
        assert_eq!(metadata.layout_reads.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn local_manifest_miss_uses_quorum_and_reads_ec_object() {
        let data = (0..128).map(|idx| (idx % 251) as u8).collect::<Vec<_>>();
        let (manifest, shard_bytes) = encoded_manifest(rs_profile(17), &data);
        let dir = tempfile::tempdir().unwrap();
        let local = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let coordinator = coordinator(local);
        let metadata = MemoryMetadata::default();
        install_quorum_manifest(&metadata, &manifest).await;
        let small = MemorySmallReader::default();
        let shards = MemoryShardReader::new(shard_bytes);
        let repairs = MemoryRepairQueue::default();

        let result = coordinator
            .read(
                EcReadRequest::full(object()),
                &metadata,
                &small,
                &shards,
                &repairs,
            )
            .await
            .unwrap();

        assert_eq!(result.body.as_ref(), data.as_slice());
        assert_eq!(metadata.layout_reads.load(Ordering::SeqCst), 1);
        assert_eq!(metadata.manifest_reads.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn cached_layout_is_not_served_when_quorum_confirms_absent() {
        let data = (0..64).map(|idx| (idx % 251) as u8).collect::<Vec<_>>();
        let (manifest, shard_bytes) = encoded_manifest(rs_profile(64), &data);
        let reference = EcObjectManifestRef::from_manifest(&manifest).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let local = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        local
            .put_object_layout(&object(), &ObjectDataLayout::ErasureCoded(reference))
            .await
            .unwrap();
        local.put_manifest(&manifest).await.unwrap();
        let coordinator = coordinator(local.clone());
        let metadata = MemoryMetadata::default();
        let small = MemorySmallReader::default();
        let shards = MemoryShardReader::new(shard_bytes);
        let repairs = MemoryRepairQueue::default();

        let err = coordinator
            .read(
                EcReadRequest::full(object()),
                &metadata,
                &small,
                &shards,
                &repairs,
            )
            .await
            .unwrap_err();

        assert!(matches!(err, EcReadError::NotFound { .. }));
        assert_eq!(metadata.layout_reads.load(Ordering::SeqCst), 1);
        assert_eq!(shards.reads.load(Ordering::SeqCst), 0);
        assert!(local.get_object_layout(&object()).await.unwrap().is_none());
        assert!(local.get_manifest(&object()).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn fast_path_uses_data_slot_order_and_leaves_parity_alone() {
        let data = (0..64).map(|idx| (idx % 251) as u8).collect::<Vec<_>>();
        let (mut manifest, shard_bytes) = encoded_manifest(rs_profile(64), &data);
        manifest.chunks[0].shards.reverse();
        manifest.validate().unwrap();
        let reference = EcObjectManifestRef::from_manifest(&manifest).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let local = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        local
            .put_object_layout(
                &object(),
                &ObjectDataLayout::ErasureCoded(reference.clone()),
            )
            .await
            .unwrap();
        local.put_manifest(&manifest).await.unwrap();
        let coordinator = coordinator(local);
        let metadata = MemoryMetadata::default();
        install_quorum_manifest(&metadata, &manifest).await;
        let small = MemorySmallReader::default();
        let shards = MemoryShardReader::new(shard_bytes);
        shards.corrupt_slots(&[(0, manifest.profile.k)]);
        let repairs = MemoryRepairQueue::default();

        let result = coordinator
            .read(
                EcReadRequest::full(object()),
                &metadata,
                &small,
                &shards,
                &repairs,
            )
            .await
            .unwrap();

        assert_eq!(result.body.as_ref(), data.as_slice());
        assert_eq!(result.degraded_chunks, 0);

        // The corrupt shard is parity, and a healthy chunk is assembled without
        // it, so the read never sees it: nothing is queued and nothing is
        // counted. Finding that corruption is the scrubber's job, which checks
        // every shard a node owns against the same manifest hash
        // (decision EC2-181).
        assert!(!result.repair_enqueued);
        assert!(repairs.tasks.lock().unwrap().is_empty());
        // Asked for in the manifest's own order, which this test reversed —
        // the set is what matters here, and assembly puts it back in data-slot
        // order regardless.
        let mut requested = shards.slots_requested();
        requested.sort_unstable();
        assert_eq!(
            requested,
            (0..manifest.profile.k).map(|slot| (0, slot)).collect::<Vec<_>>(),
            "a healthy chunk must be read from its data slots and nothing else"
        );
    }

    #[tokio::test]
    async fn corrupt_data_shard_is_skipped_and_repaired_before_degraded_decode() {
        let data = (0..128).map(|idx| (idx % 251) as u8).collect::<Vec<_>>();
        let (manifest, shard_bytes) = encoded_manifest(rs_profile(128), &data);
        let reference = EcObjectManifestRef::from_manifest(&manifest).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let local = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        local
            .put_object_layout(&object(), &ObjectDataLayout::ErasureCoded(reference))
            .await
            .unwrap();
        local.put_manifest(&manifest).await.unwrap();
        let coordinator = coordinator(local);
        let metadata = MemoryMetadata::default();
        install_quorum_manifest(&metadata, &manifest).await;
        let small = MemorySmallReader::default();
        let shards = MemoryShardReader::new(shard_bytes);
        shards.corrupt_slots(&[(0, 1)]);
        let repairs = MemoryRepairQueue::default();

        let result = coordinator
            .read(
                EcReadRequest::full(object()),
                &metadata,
                &small,
                &shards,
                &repairs,
            )
            .await
            .unwrap();

        assert_eq!(result.body.as_ref(), data.as_slice());
        assert_eq!(result.degraded_chunks, 1);
        let tasks = repairs.tasks.lock().unwrap();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].kind, EcReadRepairKind::CorruptShard);
        assert_eq!(tasks[0].slot_index, 1);
    }

    /// EC2-ZONE-03-002 and EC2-READ-02-002: a degraded read asks for the data
    /// shards of a chunk nearest first — this node, its rack, its zone, the
    /// rest; slot order inside each — and tops up with the rest of the chunk,
    /// in the same order, only once a data shard is missing. The missing shard
    /// is found and queued for repair as before. Without a topology the order
    /// is the slot order; with minimal read off it is the EC v1 order, every
    /// shard of the chunk nearest first.
    #[tokio::test]
    async fn a_degraded_read_asks_for_the_nearest_shards_first() {
        use crate::erasure_coding::{PoolTopologyView, TopologyLabels};

        let data = (0..128).map(|idx| (idx % 251) as u8).collect::<Vec<_>>();
        // Slot `i` is on node `i + 1`.
        let (manifest, shard_bytes) = encoded_manifest(rs_profile(128), &data);
        let reference = EcObjectManifestRef::from_manifest(&manifest).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let local = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        local
            .put_object_layout(&object(), &ObjectDataLayout::ErasureCoded(reference))
            .await
            .unwrap();
        local.put_manifest(&manifest).await.unwrap();
        let metadata = MemoryMetadata::default();
        install_quorum_manifest(&metadata, &manifest).await;

        // The reader is node 5. Node 6 shares its rack, node 3 its zone;
        // nodes 1, 2 and 4 are in another zone.
        let placement = [
            (1, "far", "r1"),
            (2, "far", "r2"),
            (3, "near", "r3"),
            (4, "far", "r4"),
            (5, "near", "r5"),
            (6, "near", "r5"),
        ];
        let labels = placement
            .iter()
            .map(|(index, zone, rack)| {
                (
                    node(*index),
                    TopologyLabels {
                        zone: Some((*zone).to_string()),
                        rack: Some((*rack).to_string()),
                        host: Some(format!("h{index}")),
                        disk_group: Some("d0".to_string()),
                    },
                )
            })
            .collect::<HashMap<_, _>>();
        let nodes = placement.iter().map(|(index, _, _)| node(*index)).collect::<Vec<_>>();
        let view = Arc::new(PoolTopologyView::build(node(5), &nodes, &labels));

        for (reader, expected) in [
            // Data shards nearest first (slot 2 sits on node 3, this node's
            // zone), then the parity the lost shard forces, nearest first too.
            (
                coordinator(Arc::clone(&local)).with_topology(Arc::clone(&view)),
                vec![2, 0, 1, 3, 4, 5],
            ),
            (coordinator(Arc::clone(&local)), vec![0, 1, 2, 3, 4, 5]),
            // Minimal read off: the EC v1 order, every shard of the chunk by
            // distance, parity included whether or not it is needed.
            (
                coordinator(Arc::clone(&local))
                    .with_topology(Arc::clone(&view))
                    .with_minimal_read(false),
                vec![4, 5, 2, 0, 1, 3],
            ),
            (
                coordinator(Arc::clone(&local)).with_minimal_read(false),
                vec![0, 1, 2, 3, 4, 5],
            ),
        ] {
            let shards = MemoryShardReader::new(shard_bytes.clone());
            shards.missing_slots(&[(0, 1)]);
            let repairs = MemoryRepairQueue::default();

            let result = reader
                .read(
                    EcReadRequest::full(object()),
                    &metadata,
                    &MemorySmallReader::default(),
                    &shards,
                    &repairs,
                )
                .await
                .unwrap();

            assert_eq!(result.body.as_ref(), data.as_slice());
            assert_eq!(result.degraded_chunks, 1);
            let order =
                shards.slots_requested().into_iter().map(|(_, slot)| slot).collect::<Vec<_>>();
            assert_eq!(order, expected);
            let tasks = repairs.tasks.lock().unwrap();
            assert_eq!(tasks.len(), 1);
            assert_eq!(tasks[0].slot_index, 1);
        }
    }

    #[tokio::test]
    async fn repair_enqueued_report_requires_successful_enqueue() {
        let data = (0..128).map(|idx| (idx % 251) as u8).collect::<Vec<_>>();
        let (manifest, shard_bytes) = encoded_manifest(rs_profile(128), &data);
        let reference = EcObjectManifestRef::from_manifest(&manifest).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let local = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        local
            .put_object_layout(&object(), &ObjectDataLayout::ErasureCoded(reference))
            .await
            .unwrap();
        local.put_manifest(&manifest).await.unwrap();
        let coordinator = coordinator(local);
        let metadata = MemoryMetadata::default();
        install_quorum_manifest(&metadata, &manifest).await;
        let small = MemorySmallReader::default();
        let shards = MemoryShardReader::new(shard_bytes);
        shards.corrupt_slots(&[(0, 1)]);
        let repairs = MemoryRepairQueue::default();
        repairs.fail.store(true, Ordering::SeqCst);

        let result = coordinator
            .read(
                EcReadRequest::full(object()),
                &metadata,
                &small,
                &shards,
                &repairs,
            )
            .await
            .unwrap();

        assert_eq!(result.body.as_ref(), data.as_slice());
        assert_eq!(result.degraded_chunks, 1);
        assert!(!result.repair_enqueued);
        assert!(repairs.tasks.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn rs_degraded_read_works_for_every_k_of_n_pattern() {
        let data = (0..256).map(|idx| (idx % 251) as u8).collect::<Vec<_>>();
        let profile = rs_profile(256);
        let total = profile.total_shards();
        let parity = match profile.parity {
            EcParity::ReedSolomon { m } => m,
            EcParity::Lrc { .. } => unreachable!(),
        };
        let (manifest, shard_bytes) = encoded_manifest(profile, &data);

        for first in 0..total {
            for second in (first + 1)..total {
                if parity != 2 {
                    continue;
                }
                let dir = tempfile::tempdir().unwrap();
                let local = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
                let reference = EcObjectManifestRef::from_manifest(&manifest).unwrap();
                local
                    .put_object_layout(&object(), &ObjectDataLayout::ErasureCoded(reference))
                    .await
                    .unwrap();
                local.put_manifest(&manifest).await.unwrap();
                let coordinator = coordinator(local);
                let metadata = MemoryMetadata::default();
                install_quorum_manifest(&metadata, &manifest).await;
                let small = MemorySmallReader::default();
                let shards = MemoryShardReader::new(shard_bytes.clone());
                shards.missing_slots(&[(0, first), (0, second)]);
                let repairs = MemoryRepairQueue::default();

                let result = coordinator
                    .read(
                        EcReadRequest::full(object()),
                        &metadata,
                        &small,
                        &shards,
                        &repairs,
                    )
                    .await
                    .unwrap();

                assert_eq!(result.body.as_ref(), data.as_slice());
                assert!(result.degraded_chunks <= 1);
            }
        }
    }

    #[tokio::test]
    async fn lrc_degraded_read_matches_documented_failure_matrix() {
        let data = (0..180).map(|idx| ((idx * 17) % 251) as u8).collect::<Vec<_>>();
        let profile = lrc_profile(180);
        let codec = crate::ec_codec::build_codec(profile.clone()).unwrap();
        let matrix = codec.failure_matrix();
        let (manifest, shard_bytes) = encoded_manifest(profile, &data);

        for entry in matrix.entries {
            let dir = tempfile::tempdir().unwrap();
            let local = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
            let reference = EcObjectManifestRef::from_manifest(&manifest).unwrap();
            local
                .put_object_layout(&object(), &ObjectDataLayout::ErasureCoded(reference))
                .await
                .unwrap();
            local.put_manifest(&manifest).await.unwrap();
            let coordinator = coordinator(local);
            let metadata = MemoryMetadata::default();
            install_quorum_manifest(&metadata, &manifest).await;
            let small = MemorySmallReader::default();
            let shards = MemoryShardReader::new(shard_bytes.clone());
            let missing = entry.missing_slots.iter().map(|slot| (0, *slot)).collect::<Vec<_>>();
            shards.missing_slots(&missing);
            let repairs = MemoryRepairQueue::default();

            let result = coordinator
                .read(
                    EcReadRequest::full(object()),
                    &metadata,
                    &small,
                    &shards,
                    &repairs,
                )
                .await;

            assert_eq!(
                result.is_ok(),
                entry.recoverable,
                "pattern {} failed expectation",
                entry.label
            );
        }
    }

    /// The six standard profiles, which every matrix test has to cover.
    fn standard_profiles(chunk_size: u64) -> Vec<EcProfileSnapshot> {
        [
            EcProfileSnapshot::rs_small(),
            EcProfileSnapshot::rs_standard(),
            EcProfileSnapshot::rs_dense(),
            EcProfileSnapshot::lrc_small(),
            EcProfileSnapshot::lrc_standard(),
            EcProfileSnapshot::lrc_dense(),
        ]
        .into_iter()
        .map(|mut profile| {
            profile.chunk_size = chunk_size;
            profile
        })
        .collect()
    }

    /// An encoded object, its manifest in the local store and its shards in a
    /// reader — everything a read test needs but the coordinator.
    struct ReadFixture {
        data: Vec<u8>,
        manifest: EcObjectManifest,
        shard_bytes: HashMap<(u32, u8), Bytes>,
        local: Arc<EcMetadataStore>,
        metadata: MemoryMetadata,
        small: MemorySmallReader,
        shards: MemoryShardReader,
        repairs: MemoryRepairQueue,
        _dir: tempfile::TempDir,
    }

    impl ReadFixture {
        async fn new(profile: EcProfileSnapshot, data: Vec<u8>) -> Self {
            let (manifest, shard_bytes) = encoded_manifest(profile, &data);
            let reference = EcObjectManifestRef::from_manifest(&manifest).unwrap();
            let dir = tempfile::tempdir().unwrap();
            let local = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
            local
                .put_object_layout(&object(), &ObjectDataLayout::ErasureCoded(reference))
                .await
                .unwrap();
            local.put_manifest(&manifest).await.unwrap();
            let metadata = MemoryMetadata::default();
            install_quorum_manifest(&metadata, &manifest).await;

            Self {
                data,
                shards: MemoryShardReader::new(shard_bytes.clone()),
                manifest,
                shard_bytes,
                local,
                metadata,
                small: MemorySmallReader::default(),
                repairs: MemoryRepairQueue::default(),
                _dir: dir,
            }
        }

        fn coordinator(&self) -> EcReadCoordinator {
            coordinator(Arc::clone(&self.local))
        }

        /// A shard reader of its own, so one fixture can serve two reads that
        /// have to be counted apart.
        fn fresh_shards(&self) -> MemoryShardReader {
            MemoryShardReader::new(self.shard_bytes.clone())
        }

        async fn read(&self, reader: &EcReadCoordinator) -> EcReadResult {
            self.read_with(reader, &self.shards, &self.repairs).await
        }

        async fn read_with(
            &self,
            reader: &EcReadCoordinator,
            shards: &MemoryShardReader,
            repairs: &MemoryRepairQueue,
        ) -> EcReadResult {
            self.try_read(reader, None, shards, repairs).await.unwrap()
        }

        async fn try_read(
            &self,
            reader: &EcReadCoordinator,
            range: Option<EcByteRange>,
            shards: &MemoryShardReader,
            repairs: &MemoryRepairQueue,
        ) -> Result<EcReadResult, EcReadError> {
            let request = match range {
                Some(range) => EcReadRequest::range(object(), range),
                None => EcReadRequest::full(object()),
            };
            reader.read(request, &self.metadata, &self.small, shards, repairs).await
        }
    }

    /// Everything the comparison in `EC2-READ-02-003` looks at for one chunk.
    #[derive(Debug, PartialEq, Eq)]
    struct ChunkReadFacts {
        outcome: Result<(Bytes, bool, RecoveryPath), String>,
        repairs: Vec<(u8, EcReadRepairKind)>,
    }

    async fn read_one_chunk(
        reader: &EcReadCoordinator,
        manifest: &EcObjectManifest,
        chunk: &ChunkManifest,
        shards: &MemoryShardReader,
    ) -> ChunkReadFacts {
        let repairs = MemoryRepairQueue::default();
        let mut fetched = FetchedShards::new(usize::from(manifest.profile.total_shards()));
        let outcome =
            reader.reconstruct_chunk(manifest, chunk, &mut fetched, shards, &repairs).await;
        let mut queued = repairs
            .tasks
            .lock()
            .unwrap()
            .iter()
            .map(|task| (task.slot_index, task.kind))
            .collect::<Vec<_>>();
        queued.sort_by_key(|(slot_index, _)| *slot_index);

        ChunkReadFacts {
            outcome: outcome
                .map(|read| (read.bytes, read.degraded, read.recovery_path))
                .map_err(|error| error.to_string()),
            repairs: queued,
        }
    }

    /// EC2-READ-02-001: a read with nothing wrong with it fetches the `k` data
    /// shards of every chunk it touches and nothing else.
    ///
    /// The count is asserted against `k` rather than bounded by `n`: "no more
    /// than `n`" was true of the read this replaces and would prove nothing.
    #[tokio::test]
    async fn a_healthy_read_fetches_exactly_k_data_shards_per_chunk() {
        // RS(6,3) over 30-byte chunks: three chunks, the last one short.
        let data = (0..77).map(|idx| (idx % 251) as u8).collect::<Vec<_>>();
        let fix = ReadFixture::new(rs_standard_profile(30), data).await;
        let k = usize::from(fix.manifest.profile.k);
        let chunks = fix.manifest.chunks.len();
        assert_eq!(chunks, 3, "the fixture is meant to span several chunks");

        let result = fix.read(&fix.coordinator()).await;

        assert_eq!(result.body.as_ref(), fix.data.as_slice());
        assert_eq!(result.degraded_chunks, 0);
        assert!(!result.repair_enqueued);
        assert!(fix.repairs.tasks.lock().unwrap().is_empty());

        assert_eq!(fix.shards.reads.load(Ordering::SeqCst), k * chunks);
        let expected_slots = (0..chunks)
            .flat_map(|chunk| (0..k).map(move |slot| (chunk as u32, slot as u8)))
            .collect::<Vec<_>>();
        assert_eq!(fix.shards.slots_requested(), expected_slots);

        // The same number the phase 1 planner works out without any I/O, which
        // is what makes it the number the saving is measured in.
        assert_eq!(
            plan_read(&fix.manifest, None).unwrap().shard_reads(),
            k * chunks
        );
    }

    /// EC2-READ-02-001: what the narrowing is worth, measured rather than
    /// claimed. On every standard profile a healthy read moves `k / n` of the
    /// bytes the EC v1 read moved and returns the same object byte for byte.
    #[tokio::test]
    async fn a_healthy_read_moves_k_of_n_bytes_on_every_standard_profile() {
        let data = (0..500).map(|idx| ((idx * 13) % 251) as u8).collect::<Vec<_>>();

        for profile in standard_profiles(240) {
            let name = profile.name.clone();
            let fix = ReadFixture::new(profile, data.clone()).await;
            let k = u64::from(fix.manifest.profile.k);
            let total = u64::from(fix.manifest.profile.total_shards());

            let minimal_shards = fix.fresh_shards();
            let minimal = fix
                .read_with(
                    &fix.coordinator(),
                    &minimal_shards,
                    &MemoryRepairQueue::default(),
                )
                .await;
            let ec_v1_shards = fix.fresh_shards();
            let ec_v1 = fix
                .read_with(
                    &fix.coordinator().with_minimal_read(false),
                    &ec_v1_shards,
                    &MemoryRepairQueue::default(),
                )
                .await;

            assert_eq!(minimal.body, ec_v1.body, "profile {name} changed the body");
            assert_eq!(minimal.body.as_ref(), data.as_slice());
            assert_eq!(minimal.degraded_chunks, 0);
            assert_eq!(ec_v1.degraded_chunks, 0);

            // Cross-multiplied so the ratio is exact: n/k is 1.5 on RS(6,3) and
            // 1.75 on LRC(4,2,1), and neither is a float here.
            let moved = minimal_shards.bytes_returned() as u64;
            let moved_v1 = ec_v1_shards.bytes_returned() as u64;
            assert_eq!(
                moved * total,
                moved_v1 * k,
                "profile {name} moved {moved} byte(s) against {moved_v1}, which is not k/n"
            );
            let reads = minimal_shards.reads.load(Ordering::SeqCst) as u64;
            let reads_v1 = ec_v1_shards.reads.load(Ordering::SeqCst) as u64;
            assert_eq!(
                reads * total,
                reads_v1 * k,
                "profile {name} made {reads} shard request(s) against {reads_v1}"
            );
        }
    }

    /// EC2-READ-02-002: a chunk that lost a data shard tops up with the rest of
    /// its shards and decodes inside the same read — the client asks once — and
    /// the chunk next to it stays on the fast path, because the set of shards to
    /// fetch is decided per chunk.
    #[tokio::test]
    async fn a_lost_data_shard_tops_up_only_the_chunk_that_lost_it() {
        // RS(4,2) over 16-byte chunks: two chunks, one of them short a shard.
        let data = (0..32).map(|idx| (idx % 251) as u8).collect::<Vec<_>>();
        let fix = ReadFixture::new(rs_profile(16), data).await;
        let k = fix.manifest.profile.k;
        let total = fix.manifest.profile.total_shards();
        fix.shards.missing_slots(&[(0, 1)]);

        let result = fix.read(&fix.coordinator()).await;

        assert_eq!(result.body.as_ref(), fix.data.as_slice());
        assert_eq!(result.chunks_reconstructed, 2);
        assert_eq!(result.degraded_chunks, 1);

        let requested = fix.shards.slots_requested();
        let touched =
            |chunk_index: u32| requested.iter().filter(|(chunk, _)| *chunk == chunk_index).count();
        assert_eq!(
            touched(0),
            usize::from(total),
            "the chunk that lost a shard falls back to the whole chunk"
        );
        assert_eq!(
            touched(1),
            usize::from(k),
            "the healthy chunk must not pay for its neighbour"
        );
        assert!(
            requested.iter().filter(|(chunk, _)| *chunk == 1).all(|(_, slot)| *slot < k),
            "no parity of the healthy chunk may be asked for"
        );

        let tasks = fix.repairs.tasks.lock().unwrap();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].chunk_index, 0);
        assert_eq!(tasks[0].slot_index, 1);
        assert_eq!(tasks[0].kind, EcReadRepairKind::MissingShard);
    }

    /// EC2-READ-02-003: narrowing the healthy read did not move the line
    /// between what rebuilds and what does not.
    ///
    /// On every standard profile and every pattern of its failure matrix, a
    /// chunk comes back with the same bytes, the same `degraded` flag and the
    /// same `recovery_path` as under the read that fetched all `n` shards. The
    /// matrix itself is the second check: a pattern the codec calls recoverable
    /// is read, and one it does not is refused.
    ///
    /// Repair tasks are where the two reads are allowed to differ, and the test
    /// pins exactly where. A pattern that costs a data shard makes the read
    /// fetch the whole chunk, so it sees and queues everything the EC v1 read
    /// saw. A pattern confined to parity leaves the chunk assemblable, so the
    /// narrowed read never asks for the lost shards and queues nothing — that
    /// loss is the scrubber's to find, which it does for every shard a node
    /// owns, parity included (decision EC2-181).
    #[tokio::test]
    async fn degraded_reads_are_unchanged_on_every_standard_profile() {
        let data = (0..240).map(|idx| ((idx * 7) % 251) as u8).collect::<Vec<_>>();

        for profile in standard_profiles(240) {
            let name = profile.name.clone();
            let codec = crate::ec_codec::build_codec(profile.clone()).unwrap();
            let matrix = codec.failure_matrix();
            let fix = ReadFixture::new(profile, data.clone()).await;
            let chunk = &fix.manifest.chunks[0];
            let total = fix.manifest.profile.total_shards();
            let k = fix.manifest.profile.k;
            let mut rebuilt = 0usize;

            // The failure matrix is read with its documented verdict, but on an
            // RS profile it names only two patterns and neither of them is a
            // recoverable loss of a data shard — which is the case this test
            // exists for. So every single and every double loss is read on top
            // of it, with the two reads answering for each other.
            let mut patterns = matrix
                .entries
                .iter()
                .map(|entry| {
                    (
                        entry.label.clone(),
                        entry.missing_slots.clone(),
                        Some(entry.recoverable),
                    )
                })
                .collect::<Vec<_>>();
            for first in 0..total {
                patterns.push((format!("lost-{first}"), vec![first], None));
                for second in (first + 1)..total {
                    patterns.push((format!("lost-{first}-{second}"), vec![first, second], None));
                }
            }

            for (label, missing_slots, documented) in &patterns {
                let missing = missing_slots.iter().map(|slot| (0, *slot)).collect::<Vec<_>>();

                let minimal_shards = fix.fresh_shards();
                minimal_shards.missing_slots(&missing);
                let minimal =
                    read_one_chunk(&fix.coordinator(), &fix.manifest, chunk, &minimal_shards).await;

                let ec_v1_shards = fix.fresh_shards();
                ec_v1_shards.missing_slots(&missing);
                let ec_v1 = read_one_chunk(
                    &fix.coordinator().with_minimal_read(false),
                    &fix.manifest,
                    chunk,
                    &ec_v1_shards,
                )
                .await;

                assert_eq!(
                    minimal.outcome, ec_v1.outcome,
                    "profile {name} pattern {label} reads differently now"
                );
                if let Some(recoverable) = documented {
                    assert_eq!(
                        minimal.outcome.is_ok(),
                        *recoverable,
                        "profile {name} pattern {label} disagrees with the failure matrix"
                    );
                }

                if missing_slots.iter().any(|slot| *slot < k) {
                    assert_eq!(
                        minimal.repairs, ec_v1.repairs,
                        "profile {name} pattern {label} lost a data shard and must report every loss"
                    );
                } else {
                    assert!(
                        minimal.repairs.is_empty(),
                        "profile {name} pattern {label} touches no data shard, so the read has nothing to report: {:?}",
                        minimal.repairs
                    );
                    assert!(
                        ec_v1.repairs.iter().all(|(slot, _)| *slot >= k),
                        "profile {name} pattern {label} was supposed to be confined to parity: {:?}",
                        ec_v1.repairs
                    );
                }
                if let Ok((bytes, degraded, _)) = &minimal.outcome {
                    assert_eq!(
                        bytes.as_ref(),
                        &data[..usize::try_from(chunk.data_size).unwrap()]
                    );
                    if *degraded {
                        rebuilt += 1;
                    }
                }
            }

            assert!(
                rebuilt > 0,
                "profile {name} never actually rebuilt a chunk, so the comparison proved nothing"
            );
        }
    }

    /// EC2-READ-02-004: a data shard that fails its manifest hash is still
    /// counted and still queued for repair.
    ///
    /// Fetching fewer shards must not turn into losing the signal from the ones
    /// still fetched, so the counter is read rather than assumed.
    #[tokio::test]
    async fn a_corrupt_data_shard_is_still_counted_and_queued_for_repair() {
        let data = (0..128).map(|idx| (idx % 251) as u8).collect::<Vec<_>>();
        let fix = ReadFixture::new(rs_profile(128), data).await;
        fix.shards.corrupt_slots(&[(0, 1)]);

        let recorder = CountingRecorder::default();
        let guard = metrics::set_default_local_recorder(&recorder);
        let result = fix.read(&fix.coordinator()).await;
        drop(guard);

        assert_eq!(result.body.as_ref(), fix.data.as_slice());
        assert_eq!(result.degraded_chunks, 1);
        assert!(result.repair_enqueued);

        let tasks = fix.repairs.tasks.lock().unwrap();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].slot_index, 1);
        assert_eq!(tasks[0].kind, EcReadRepairKind::CorruptShard);

        assert_eq!(
            recorder.count(ec_metrics::SHARD_HASH_MISMATCH_TOTAL),
            1,
            "the corrupt data shard was not counted"
        );
        assert_eq!(
            recorder.count(ec_metrics::READ_DEGRADED_TOTAL),
            1,
            "the chunk it forced to decode was not counted as degraded"
        );
    }

    /// One row of the range-shape table in `EC2-READ-03-001`.
    struct RangeCase {
        label: &'static str,
        range: Option<EcByteRange>,
        /// Slots the read must ask for, in the order it must ask for them.
        slots: Vec<(u32, u8)>,
        /// Chunks the range covers whole, which are the only ones rebuilt.
        rebuilt: usize,
        /// Chunks the range touches at all.
        touched: usize,
    }

    impl RangeCase {
        fn new(
            label: &'static str,
            range: Option<(u64, u64)>,
            slots: Vec<(u32, u8)>,
            rebuilt: usize,
            touched: usize,
        ) -> Self {
            Self {
                label,
                range: range.map(|(start, end)| EcByteRange::new(start, end)),
                slots,
                rebuilt,
                touched,
            }
        }
    }

    /// EC2-READ-03-001: a range read fetches the data shards it falls in, and no
    /// others.
    ///
    /// The slots are spelled out per case as well as compared with the phase 1
    /// planner, so the test says what the answer should be instead of only
    /// agreeing with whatever the planner produced. Alongside each case runs the
    /// same read with minimal read off, which is the chunk-at-a-time read this
    /// replaces: same bytes, `n` shards per chunk touched.
    #[tokio::test]
    async fn a_range_read_fetches_only_the_shards_it_falls_in() {
        // RS(4,2) over 16-byte chunks: two chunks, four data slots of four bytes
        // each, so every boundary in the plan lands on a round number.
        let data = (0..32).map(|idx| (idx % 251) as u8).collect::<Vec<_>>();
        let fix = ReadFixture::new(rs_profile(16), data).await;
        let k = usize::from(fix.manifest.profile.k);
        let total = usize::from(fix.manifest.profile.total_shards());

        let whole_object = (0..2)
            .flat_map(|chunk| (0..k).map(move |slot| (chunk as u32, slot as u8)))
            .collect::<Vec<_>>();
        let cases = [
            RangeCase::new("inside one shard", Some((5, 6)), vec![(0, 1)], 0, 1),
            RangeCase::new(
                "across two shards",
                Some((3, 4)),
                vec![(0, 0), (0, 1)],
                0,
                1,
            ),
            RangeCase::new(
                "across two chunks",
                Some((15, 16)),
                vec![(0, 3), (1, 0)],
                0,
                2,
            ),
            RangeCase::new(
                "one chunk whole and one byte of the next",
                Some((0, 16)),
                vec![(0, 0), (0, 1), (0, 2), (0, 3), (1, 0)],
                1,
                2,
            ),
            RangeCase::new("the whole object", None, whole_object, 2, 2),
        ];

        for case in cases {
            let RangeCase {
                label,
                range,
                slots: expected_slots,
                rebuilt,
                touched,
            } = case;
            let expected_body = match range {
                Some(range) => &fix.data[range.start as usize..=range.end as usize],
                None => fix.data.as_slice(),
            };

            let shards = fix.fresh_shards();
            let repairs = MemoryRepairQueue::default();
            let result = fix.try_read(&fix.coordinator(), range, &shards, &repairs).await.unwrap();

            assert_eq!(result.body.as_ref(), expected_body, "{label}");
            assert_eq!(result.content_length, expected_body.len() as u64, "{label}");
            assert_eq!(
                result.content_range,
                range.map(|range| (range.start, range.end, fix.data.len() as u64)),
                "{label}"
            );
            assert_eq!(result.degraded_chunks, 0, "{label}");
            assert!(!result.repair_enqueued, "{label}");
            assert_eq!(shards.slots_requested(), expected_slots, "{label}");
            assert_eq!(
                shards.reads.load(Ordering::SeqCst),
                expected_slots.len(),
                "{label}"
            );
            assert_eq!(
                result.chunks_reconstructed, rebuilt,
                "{label}: only a chunk the range covers whole is rebuilt"
            );

            let plan = plan_read(&fix.manifest, range).unwrap();
            assert_eq!(plan.shard_reads(), expected_slots.len(), "{label}");

            // The read this replaces: every chunk the range touches rebuilt from
            // all `n` shards, then trimmed.
            let ec_v1_shards = fix.fresh_shards();
            let ec_v1 = fix
                .try_read(
                    &fix.coordinator().with_minimal_read(false),
                    range,
                    &ec_v1_shards,
                    &MemoryRepairQueue::default(),
                )
                .await
                .unwrap();
            assert_eq!(ec_v1.body, result.body, "{label}");
            assert_eq!(
                ec_v1_shards.reads.load(Ordering::SeqCst),
                total * touched,
                "{label}"
            );
            assert_eq!(ec_v1.chunks_reconstructed, touched, "{label}");
        }
    }

    /// EC2-READ-03-002: a range read never answers with bytes that failed their
    /// shard hash.
    ///
    /// Both halves are needed for "refused" to mean refused. Where the chunk can
    /// still be rebuilt the client gets the right bytes and the bad shard goes to
    /// repair; where it cannot, the read fails instead of handing over what it
    /// could not verify.
    #[tokio::test]
    async fn a_range_read_refuses_bytes_that_failed_their_shard_hash() {
        let data = (0..32).map(|idx| ((idx * 5) % 251) as u8).collect::<Vec<_>>();
        let fix = ReadFixture::new(rs_profile(16), data).await;
        let range = EcByteRange::new(5, 6);

        let shards = fix.fresh_shards();
        shards.corrupt_slots(&[(0, 1)]);
        let repairs = MemoryRepairQueue::default();
        let recorder = CountingRecorder::default();
        let guard = metrics::set_default_local_recorder(&recorder);
        let result =
            fix.try_read(&fix.coordinator(), Some(range), &shards, &repairs).await.unwrap();
        drop(guard);

        assert_eq!(result.body.as_ref(), &fix.data[5..=6]);
        assert_eq!(
            result.chunks_reconstructed, 1,
            "the chunk had to be rebuilt"
        );
        assert_eq!(result.degraded_chunks, 1);
        assert_eq!(recorder.count(ec_metrics::SHARD_HASH_MISMATCH_TOTAL), 1);
        {
            let tasks = repairs.tasks.lock().unwrap();
            assert_eq!(tasks.len(), 1);
            assert_eq!(tasks[0].slot_index, 1);
            assert_eq!(tasks[0].kind, EcReadRepairKind::CorruptShard);
        }

        // Nothing left to rebuild the chunk from: the corrupt bytes are still
        // not an answer.
        let shards = fix.fresh_shards();
        shards.corrupt_slots(&[(0, 1)]);
        shards.missing_slots(&[(0, 4), (0, 5)]);
        let error = fix
            .try_read(
                &fix.coordinator(),
                Some(range),
                &shards,
                &MemoryRepairQueue::default(),
            )
            .await
            .unwrap_err();
        assert!(
            matches!(error, EcReadError::Unavailable { .. }),
            "expected the read to fail, got {error:?}"
        );
    }

    /// EC2-READ-03-003: a shard the range falls in going missing lifts the whole
    /// chunk — the path phase 2 already proved — and nothing is asked for twice.
    #[tokio::test]
    async fn a_range_read_that_loses_a_covering_shard_lifts_the_whole_chunk_once() {
        let data = (0..32).map(|idx| ((idx * 3) % 251) as u8).collect::<Vec<_>>();
        let fix = ReadFixture::new(rs_profile(16), data).await;
        let total = usize::from(fix.manifest.profile.total_shards());
        fix.shards.missing_slots(&[(0, 1)]);

        let result = fix
            .try_read(
                &fix.coordinator(),
                Some(EcByteRange::new(5, 6)),
                &fix.shards,
                &fix.repairs,
            )
            .await
            .unwrap();

        assert_eq!(result.body.as_ref(), &fix.data[5..=6]);
        assert_eq!(result.chunks_reconstructed, 1);
        assert_eq!(result.degraded_chunks, 1);

        // Slot 1 is asked for by the range and never again by the fallback, which
        // picks up the shards it already has.
        assert_eq!(
            fix.shards.all_requests(),
            vec![(0, 1), (0, 0), (0, 2), (0, 3), (0, 4), (0, 5)]
        );
        assert_eq!(fix.shards.reads.load(Ordering::SeqCst), total);

        let tasks = fix.repairs.tasks.lock().unwrap();
        assert_eq!(tasks.len(), 1, "the lost shard must be reported once");
        assert_eq!(tasks[0].slot_index, 1);
        assert_eq!(tasks[0].kind, EcReadRepairKind::MissingShard);
    }

    /// The requests the read this plan replaces would have made: every shard of
    /// every chunk the range touches, in the order the manifest lists them.
    ///
    /// Worked out from the manifest rather than taken from the code under test,
    /// so that "off is the EC v1 read" is a comparison against a reference and
    /// not a restatement of what the reader happens to do.
    fn ec_v1_expected_requests(
        manifest: &EcObjectManifest,
        range: Option<EcByteRange>,
    ) -> Vec<(u32, u8)> {
        let effective = resolve_range(range, manifest.original_size).unwrap();
        let mut offset = 0u64;
        let mut expected = Vec::new();
        for chunk in &manifest.chunks {
            let start = offset;
            offset += chunk.data_size;
            if !ranges_intersect(start, offset, effective.start, effective.end_exclusive()) {
                continue;
            }
            expected.extend(chunk.shards.iter().map(|shard| (chunk.chunk_index, shard.slot_index)));
        }
        expected
    }

    /// EC2-READ-04-001: with minimal read off a read is the EC v1 read again —
    /// every shard of every chunk it touches, fetched and verified, and the
    /// range trimmed out of the rebuilt chunks afterwards.
    ///
    /// Checked by counting rather than by describing, and against a reference
    /// built from the manifest: "no more than `n`" was true of the read this
    /// replaces too, and would prove nothing about the switch.
    #[tokio::test]
    async fn minimal_read_off_asks_for_exactly_what_the_ec_v1_read_asked_for() {
        let data = (0..500).map(|idx| ((idx * 7) % 251) as u8).collect::<Vec<_>>();

        for profile in standard_profiles(240) {
            let name = profile.name.clone();
            let fix = ReadFixture::new(profile, data.clone()).await;
            assert_eq!(fix.manifest.chunks.len(), 3, "{name}");

            let cases = [
                ("the whole object", None),
                ("a range inside one chunk", Some(EcByteRange::new(250, 260))),
                (
                    "a range across two chunks",
                    Some(EcByteRange::new(239, 241)),
                ),
                (
                    "one byte of the short last chunk",
                    Some(EcByteRange::new(490, 490)),
                ),
            ];

            for (label, range) in cases {
                let expected_body = match range {
                    Some(range) => &data[range.start as usize..=range.end as usize],
                    None => data.as_slice(),
                };

                let shards = fix.fresh_shards();
                let result = fix
                    .try_read(
                        &fix.coordinator().with_minimal_read(false),
                        range,
                        &shards,
                        &MemoryRepairQueue::default(),
                    )
                    .await
                    .unwrap();

                let expected_requests = ec_v1_expected_requests(&fix.manifest, range);
                assert_eq!(result.body.as_ref(), expected_body, "{name}: {label}");
                assert_eq!(shards.all_requests(), expected_requests, "{name}: {label}");
                assert_eq!(
                    result.chunks_reconstructed,
                    expected_requests.len() / usize::from(fix.manifest.profile.total_shards()),
                    "{name}: {label}: every chunk the range touches is rebuilt whole"
                );
                assert_eq!(result.degraded_chunks, 0, "{name}: {label}");

                // And the same read with the switch on answers with the same
                // bytes off strictly fewer shards — the saving the switch turns
                // off, in the one number both settings can be compared on.
                let narrowed_shards = fix.fresh_shards();
                let narrowed = fix
                    .try_read(
                        &fix.coordinator(),
                        range,
                        &narrowed_shards,
                        &MemoryRepairQueue::default(),
                    )
                    .await
                    .unwrap();
                assert_eq!(narrowed.body, result.body, "{name}: {label}");
                assert!(
                    narrowed_shards.reads.load(Ordering::SeqCst) < expected_requests.len(),
                    "{name}: {label}: the switch saved nothing"
                );
            }
        }
    }

    /// What one read cost, as the read traffic counters split it.
    #[derive(Debug, Default, PartialEq, Eq)]
    struct ReadTraffic {
        fetches: u64,
        bytes: u64,
        cross_zone_fetches: u64,
        cross_zone_bytes: u64,
    }

    impl ReadTraffic {
        /// What the counters say, read back out of `recorder`.
        fn measured(recorder: &CountingRecorder) -> Self {
            let cross = [("scope", "cross_zone")];
            Self {
                fetches: recorder.count(ec_metrics::READ_SHARD_FETCHES_TOTAL),
                bytes: recorder.count(ec_metrics::READ_SHARD_BYTES_TOTAL),
                cross_zone_fetches: recorder
                    .count_where(ec_metrics::READ_SHARD_FETCHES_TOTAL, &cross),
                cross_zone_bytes: recorder.count_where(ec_metrics::READ_SHARD_BYTES_TOTAL, &cross),
            }
        }

        /// What the stand says those requests had to cost, worked out from the
        /// manifest and the labels rather than from the counters, so that the
        /// measurement is checked against the pool and not against itself.
        fn expected(
            manifest: &EcObjectManifest,
            view: &PoolTopologyView,
            requests: &[(u32, u8)],
        ) -> Self {
            let mut traffic = Self::default();
            for (chunk_index, slot_index) in requests.iter().copied() {
                let chunk = manifest
                    .chunks
                    .iter()
                    .find(|chunk| chunk.chunk_index == chunk_index)
                    .expect("the request names a chunk of this manifest");
                let shard = chunk
                    .shards
                    .iter()
                    .find(|shard| shard.slot_index == slot_index)
                    .expect("the request names a slot of this chunk");
                traffic.fetches += 1;
                traffic.bytes += shard.size;
                if TransferScope::between(view, view.local_node_id(), shard.node_id)
                    == TransferScope::CrossZone
                {
                    traffic.cross_zone_fetches += 1;
                    traffic.cross_zone_bytes += shard.size;
                }
            }
            traffic
        }
    }

    /// A pool of three zones holding the slots of `manifest` in turn, read from
    /// the node that owns slot 0.
    ///
    /// Dealt round robin on purpose: that is how plans 05 and 08 spread a set,
    /// so every zone ends up with both data and parity and neither kind is
    /// nearer the reader than the other by construction.
    fn three_zone_view(manifest: &EcObjectManifest) -> PoolTopologyView {
        use crate::erasure_coding::TopologyLabels;

        let shards = &manifest.chunks[0].shards;
        let labels = shards
            .iter()
            .map(|shard| {
                (
                    shard.node_id,
                    TopologyLabels {
                        zone: Some(format!("z{}", shard.slot_index % 3)),
                        rack: Some(format!("r{}", shard.slot_index)),
                        host: Some(format!("h{}", shard.slot_index)),
                        disk_group: Some("d0".to_string()),
                    },
                )
            })
            .collect::<HashMap<_, _>>();
        let nodes = shards.iter().map(|shard| shard.node_id).collect::<Vec<_>>();
        let reader = shards
            .iter()
            .find(|shard| shard.slot_index == 0)
            .expect("every chunk has a slot 0")
            .node_id;
        PoolTopologyView::build(reader, &nodes, &labels)
    }

    /// EC2-READ-04-002: what the narrowed read saves, measured on a stand of
    /// three zones rather than asserted from the design.
    ///
    /// Three numbers, because three different things are saved: bytes off the
    /// links, round trips out of the latency chain, and the share of both that
    /// crosses a zone boundary — which decision EC2-182 owes an answer for,
    /// having chosen data shards over near ones.
    #[tokio::test]
    async fn minimal_read_moves_fewer_bytes_and_fewer_of_them_cross_a_zone() {
        let data = (0..500).map(|idx| ((idx * 11) % 251) as u8).collect::<Vec<_>>();
        let fix = ReadFixture::new(rs_standard_profile(240), data.clone()).await;
        let view = Arc::new(three_zone_view(&fix.manifest));
        let k = u64::from(fix.manifest.profile.k);
        let total = u64::from(fix.manifest.profile.total_shards());

        for (label, range) in [
            ("GET of the whole object", None),
            // Two short ranges, because where a range lands decides what it
            // saves: the first falls in a shard this node holds itself, the
            // second in a shard two zones away. Only the second can show the
            // far links going quiet.
            (
                "range of 8 bytes in a shard held here",
                Some(EcByteRange::new(250, 257)),
            ),
            (
                "range of 8 bytes in a shard two zones away",
                Some(EcByteRange::new(330, 337)),
            ),
        ] {
            let expected_body = match range {
                Some(range) => &data[range.start as usize..=range.end as usize],
                None => data.as_slice(),
            };

            let mut measured = Vec::new();
            for minimal_read in [true, false] {
                let reader = fix
                    .coordinator()
                    .with_topology(Arc::clone(&view))
                    .with_minimal_read(minimal_read);
                let shards = fix.fresh_shards();
                let recorder = CountingRecorder::default();
                let guard = metrics::set_default_local_recorder(&recorder);
                let result = fix
                    .try_read(&reader, range, &shards, &MemoryRepairQueue::default())
                    .await
                    .unwrap();
                drop(guard);

                assert_eq!(result.body.as_ref(), expected_body, "{label}");
                assert_eq!(result.degraded_chunks, 0, "{label}");

                let traffic = ReadTraffic::measured(&recorder);
                assert_eq!(
                    traffic,
                    ReadTraffic::expected(&fix.manifest, &view, &shards.all_requests()),
                    "{label}, minimal read {minimal_read}: the counters disagree with the stand"
                );
                assert_eq!(
                    traffic.bytes,
                    shards.bytes_returned() as u64,
                    "{label}, minimal read {minimal_read}: the counter missed bytes that moved"
                );
                measured.push(traffic);
            }

            let (narrowed, ec_v1) = (&measured[0], &measured[1]);
            println!(
                "{label}: bytes {} -> {} ({} -> {} cross-zone), fetches {} -> {} ({} -> {} cross-zone)",
                ec_v1.bytes,
                narrowed.bytes,
                ec_v1.cross_zone_bytes,
                narrowed.cross_zone_bytes,
                ec_v1.fetches,
                narrowed.fetches,
                ec_v1.cross_zone_fetches,
                narrowed.cross_zone_fetches,
            );

            assert!(
                narrowed.cross_zone_bytes < ec_v1.cross_zone_bytes,
                "{label}: cross-zone traffic did not fall"
            );
            assert!(
                narrowed.cross_zone_fetches < ec_v1.cross_zone_fetches,
                "{label}: cross-zone round trips did not fall"
            );

            if range.is_none() {
                // A whole object is `k` shards of every chunk against `n`, so
                // the ratio is exactly `k/n` — cross-multiplied, because a set
                // dealt evenly across the zones saves in the same proportion on
                // the far links as on all of them, and 6/9 is not a float here.
                assert_eq!(narrowed.bytes * total, ec_v1.bytes * k, "{label}");
                assert_eq!(narrowed.fetches * total, ec_v1.fetches * k, "{label}");
                assert_eq!(
                    narrowed.cross_zone_bytes * total,
                    ec_v1.cross_zone_bytes * k,
                    "{label}"
                );
                assert_eq!(
                    narrowed.cross_zone_fetches * total,
                    ec_v1.cross_zone_fetches * k,
                    "{label}"
                );
            } else {
                // A short range is one shard against every shard of the chunk
                // it falls in: the saving no ratio of `k` to `n` describes.
                assert_eq!(narrowed.fetches, 1, "{label}");
                assert_eq!(ec_v1.fetches, total, "{label}");
            }
        }
    }

    /// EC2-READ-04-002: the round trips the narrowed read no longer makes come
    /// out of the latency chain, not only off the links.
    ///
    /// Shards are fetched one after another, so the saving is `n - k` hops per
    /// chunk. Measured under a paused clock with one hop costing a fixed delay:
    /// the runtime advances time instead of waiting, so the figure is the
    /// round-trip part of read latency exactly, with no wall clock to be flaky
    /// about. What a hop really costs, and what the rest of the latency is made
    /// of, is for `scripts/51-ec-minimal-read-test.sh` on a live cluster.
    #[tokio::test(start_paused = true)]
    async fn minimal_read_takes_the_round_trips_it_saves_out_of_the_latency_chain() {
        const HOP: Duration = Duration::from_millis(2);

        let data = (0..500).map(|idx| ((idx * 3) % 251) as u8).collect::<Vec<_>>();
        let fix = ReadFixture::new(rs_standard_profile(240), data.clone()).await;
        let k = u32::from(fix.manifest.profile.k);
        let total = u32::from(fix.manifest.profile.total_shards());
        let chunks = u32::try_from(fix.manifest.chunks.len()).unwrap();

        let mut elapsed = Vec::new();
        for minimal_read in [true, false] {
            let shards = fix.fresh_shards().with_fetch_delay(HOP);
            let started = tokio::time::Instant::now();
            let result = fix
                .try_read(
                    &fix.coordinator().with_minimal_read(minimal_read),
                    None,
                    &shards,
                    &MemoryRepairQueue::default(),
                )
                .await
                .unwrap();
            let took = started.elapsed();

            assert_eq!(result.body.as_ref(), data.as_slice());
            elapsed.push(took);
        }

        let (narrowed, ec_v1) = (elapsed[0], elapsed[1]);
        println!("GET of the whole object: {ec_v1:?} -> {narrowed:?} at {HOP:?} per hop");
        assert_eq!(narrowed, HOP * k * chunks);
        assert_eq!(ec_v1, HOP * total * chunks);
        assert_eq!(narrowed * total, ec_v1 * k);
    }

    #[tokio::test]
    async fn head_covers_ec_and_replicated_small_without_fetching_shards() {
        let data = (0..64).map(|idx| (idx % 251) as u8).collect::<Vec<_>>();
        let (manifest, shard_bytes) = encoded_manifest(rs_profile(64), &data);
        let reference = EcObjectManifestRef::from_manifest(&manifest).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let local = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        local
            .put_object_layout(&object(), &ObjectDataLayout::ErasureCoded(reference))
            .await
            .unwrap();
        local.put_manifest(&manifest).await.unwrap();
        let coordinator = coordinator(local.clone());
        let metadata = MemoryMetadata::default();
        install_quorum_manifest(&metadata, &manifest).await;
        let small = MemorySmallReader::default();

        let head = coordinator.head(object(), &metadata, &small).await.unwrap();
        assert_eq!(head.size, data.len() as u64);
        assert!(matches!(head.layout, EcReadLayout::ErasureCoded { .. }));

        let shards = MemoryShardReader::new(shard_bytes);
        assert_eq!(shards.reads.load(Ordering::SeqCst), 0);

        let small_body = Bytes::from_static(b"tiny");
        *small.body.lock().unwrap() = small_body.clone();
        let placement = ReplicatedSmallObjectPlacement::new(
            vec![node(1), node(2), node(3)],
            sha256(&small_body),
            small_body.len() as u64,
            hlc(),
        )
        .unwrap();
        let small_layout = ObjectDataLayout::ReplicatedSmall(placement);
        local.put_object_layout(&object(), &small_layout).await.unwrap();
        install_quorum_layout(&metadata, small_layout);

        let head = coordinator.head(object(), &metadata, &small).await.unwrap();
        assert_eq!(head.size, small_body.len() as u64);
        assert!(matches!(head.layout, EcReadLayout::ReplicatedSmall { .. }));
    }

    #[test]
    fn packed_object_inside_one_shard() {
        let data = (0..128).map(|idx| (idx % 251) as u8).collect::<Vec<_>>();
        let (manifest, _) = encoded_manifest(rs_profile(64), &data);
        // chunk_size = 64, k = 4 -> shard_size = 16.
        // Chunk 0 slot 1 spans logical offsets [16..32).
        // Choose object at offset 20 with length 8 (offsets 20..28, strictly inside slot 1).
        let spans = locate_packed_object(&manifest, 20, 8).expect("locate should succeed");
        assert_eq!(spans.len(), 1);
        assert_eq!(
            spans[0],
            PackedShardSpan {
                chunk_index: 0,
                slot_index: 1,
                offset_in_shard: 4,
                length: 8,
            }
        );
    }

    #[test]
    fn packed_object_crosses_a_shard_boundary() {
        let data = (0..128).map(|idx| (idx % 251) as u8).collect::<Vec<_>>();
        let (manifest, _) = encoded_manifest(rs_profile(64), &data);
        // Chunk 0 slot 0 spans [0..16), slot 1 spans [16..32).
        // Object at offset 10 with length 10 covers [10..16) in slot 0 and [16..20) in slot 1.
        let spans = locate_packed_object(&manifest, 10, 10).expect("locate should succeed");
        assert_eq!(spans.len(), 2);
        assert_eq!(
            spans[0],
            PackedShardSpan {
                chunk_index: 0,
                slot_index: 0,
                offset_in_shard: 10,
                length: 6,
            }
        );
        assert_eq!(
            spans[1],
            PackedShardSpan {
                chunk_index: 0,
                slot_index: 1,
                offset_in_shard: 0,
                length: 4,
            }
        );
        assert_eq!(spans[1].offset_in_shard, 0);
        let total_length: u64 = spans.iter().map(|s| s.length).sum();
        assert_eq!(total_length, 10);
    }

    #[test]
    fn packed_object_crosses_a_chunk_boundary() {
        let data = (0..128).map(|idx| (idx % 251) as u8).collect::<Vec<_>>();
        let (manifest, _) = encoded_manifest(rs_profile(64), &data);
        // Chunk 0 spans [0..64), last data shard is slot 3 covering [48..64).
        // Chunk 1 spans [64..128), first data shard is slot 0 covering [0..16) relative to chunk 1.
        // Object at offset 60 with length 10 covers [60..64) in chunk 0 and [64..70) in chunk 1.
        let spans = locate_packed_object(&manifest, 60, 10).expect("locate should succeed");
        assert_eq!(spans.len(), 2);
        assert_ne!(spans[0].chunk_index, spans[1].chunk_index);
        assert_eq!(
            spans[0],
            PackedShardSpan {
                chunk_index: 0,
                slot_index: 3,
                offset_in_shard: 12,
                length: 4,
            }
        );
        assert_eq!(
            spans[1],
            PackedShardSpan {
                chunk_index: 1,
                slot_index: 0,
                offset_in_shard: 0,
                length: 6,
            }
        );
        let total_length: u64 = spans.iter().map(|s| s.length).sum();
        assert_eq!(total_length, 10);
    }

    #[test]
    fn packed_object_in_the_last_short_chunk() {
        // chunk_size = 64, data = 94 bytes (chunk 0 is 64 bytes, chunk 1 is 30 bytes).
        // For chunk 1, k=4 -> shard_size = ceil(30 / 4) = 8.
        // Data shards 0, 1, 2 hold 8 real bytes each (total 24).
        // Data shard 3 holds 6 real bytes (offset 24..30) and 2 zero-padding bytes.
        let data = (0..94).map(|idx| (idx % 251) as u8).collect::<Vec<_>>();
        let (manifest, _) = encoded_manifest(rs_profile(64), &data);

        // Object placed in the short chunk covering [74..94) (length 20).
        // In chunk 1, this corresponds to [10..30), ending at the segment boundary.
        let spans = locate_packed_object(&manifest, 74, 20).expect("locate should succeed");
        let total_length: u64 = spans.iter().map(|s| s.length).sum();
        assert_eq!(total_length, 20);

        // Verify the spans do not step into zero-padding bytes.
        let last_span = spans.last().expect("must have at least one span");
        assert_eq!(last_span.chunk_index, 1);
        assert_eq!(last_span.slot_index, 3);
        assert_eq!(last_span.offset_in_shard, 0);
        assert_eq!(last_span.length, 6);
    }

    #[test]
    fn spans_reassemble_every_object_in_the_segment() {
        // Deterministic pseudo-random number generator (LCG) without external crates.
        struct SimpleLcg(u64);
        impl SimpleLcg {
            fn next_u64(&mut self) -> u64 {
                self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1);
                self.0
            }
            fn gen_range(&mut self, low: u64, high: u64) -> u64 {
                if low >= high {
                    low
                } else {
                    low + (self.next_u64() % (high - low))
                }
            }
        }

        let mut rng = SimpleLcg(0x0123_4567_89ab_cdef);
        let blob_len = 250usize;
        let mut blob = vec![0u8; blob_len];
        for b in &mut blob {
            *b = (rng.next_u64() & 0xff) as u8;
        }

        let chunk_size = 64u64;
        let (manifest, shard_bytes) = encoded_manifest(rs_profile(chunk_size), &blob);

        let mut cases = vec![
            // Offset 0: the whole segment, one byte, exactly one shard, exactly one chunk.
            (0u64, blob_len as u64),
            (0u64, 1u64),
            (0u64, 16u64),
            (0u64, chunk_size),
            // The end of the segment, including the whole of the last short chunk.
            ((blob_len - 1) as u64, 1u64),
            ((blob_len - 10) as u64, 10u64),
            (192u64, 58u64),
        ];

        // 1-byte objects for every possible offset in the segment:
        for offset in 0..blob_len as u64 {
            cases.push((offset, 1u64));
        }

        // Shard-sized and chunk-sized objects:
        for chunk_idx in 0..3 {
            let base = chunk_idx * chunk_size;
            cases.push((base, chunk_size));
            for slot_idx in 0..4 {
                cases.push((base + slot_idx * 16, 16u64));
            }
        }

        // Boundary crossers:
        cases.push((15u64, 2u64));
        cases.push((63u64, 2u64));
        cases.push((127u64, 2u64));
        cases.push((60u64, 10u64));
        cases.push((48u64, 32u64));

        // Random pairs:
        for _ in 0..200 {
            let offset = rng.gen_range(0, blob_len as u64);
            let max_length = (blob_len as u64) - offset;
            let length = rng.gen_range(1, max_length + 1);
            cases.push((offset, length));
        }

        for (offset, length) in cases {
            let spans = locate_packed_object(&manifest, offset, length)
                .unwrap_or_else(|err| panic!("locate failed for ({offset}, {length}): {err}"));
            let mut reassembled = Vec::with_capacity(length as usize);
            for span in spans {
                let shard =
                    shard_bytes.get(&(span.chunk_index, span.slot_index)).unwrap_or_else(|| {
                        panic!(
                            "missing shard for chunk {} slot {}",
                            span.chunk_index, span.slot_index
                        )
                    });
                let start = span.offset_in_shard as usize;
                let end = start + span.length as usize;
                assert!(
                    end <= shard.len(),
                    "span {start}..{end} exceeds shard length {} (chunk {} slot {})",
                    shard.len(),
                    span.chunk_index,
                    span.slot_index
                );
                reassembled.extend_from_slice(&shard[start..end]);
            }
            assert_eq!(
                reassembled.as_slice(),
                &blob[offset as usize..(offset + length) as usize],
                "reassembly mismatch at offset {offset}, length {length}"
            );
        }
    }

    #[test]
    fn locator_rejects_a_shard_size_that_is_not_the_split_size() {
        let data = (0..128).map(|idx| (idx % 251) as u8).collect::<Vec<_>>();
        let (mut manifest, _) = encoded_manifest(rs_profile(64), &data);
        manifest.chunks[0].encoded_size += 1;
        let result = locate_packed_object(&manifest, 10, 20);
        assert!(matches!(result, Err(EcReadError::InvalidManifest { .. })));
    }

    #[test]
    fn locator_rejects_an_object_past_the_end_of_the_segment() {
        let data = (0..128).map(|idx| (idx % 251) as u8).collect::<Vec<_>>();
        let (manifest, _) = encoded_manifest(rs_profile(64), &data);
        // Total segment data length is 128 bytes.
        let result1 = locate_packed_object(&manifest, 120, 10);
        assert!(matches!(result1, Err(EcReadError::InvalidManifest { .. })));

        let result2 = locate_packed_object(&manifest, 128, 1);
        assert!(matches!(result2, Err(EcReadError::InvalidManifest { .. })));

        let result3 = locate_packed_object(&manifest, 200, 5);
        assert!(matches!(result3, Err(EcReadError::InvalidManifest { .. })));
    }

    #[test]
    fn locator_returns_no_spans_for_a_zero_length_object() {
        let data = (0..128).map(|idx| (idx % 251) as u8).collect::<Vec<_>>();
        let (manifest, _) = encoded_manifest(rs_profile(64), &data);
        let spans =
            locate_packed_object(&manifest, 20, 0).expect("zero-length object should succeed");
        assert!(spans.is_empty());
        let spans_at_zero =
            locate_packed_object(&manifest, 0, 0).expect("zero-length at offset 0 should succeed");
        assert!(spans_at_zero.is_empty());
    }

    struct PackedSegmentFixture {
        _dir: tempfile::TempDir,
        _store: Arc<EcMetadataStore>,
        coordinator: EcReadCoordinator,
        metadata: MemoryMetadata,
        small: MemorySmallReader,
        shards: MemoryShardReader,
        repairs: MemoryRepairQueue,
        blob: Vec<u8>,
        manifest: EcObjectManifest,
        segment: PackedSegment,
        obj_single_shard: EcObjectVersionKey,
        obj_cross_shard: EcObjectVersionKey,
        obj_cross_chunk: EcObjectVersionKey,
        obj_last_chunk: EcObjectVersionKey,
    }

    impl PackedSegmentFixture {
        fn placement_for(&self, object: &EcObjectVersionKey) -> PackedSegmentObjectPlacement {
            let entry = self.segment.entry_for(object).expect("entry must exist");
            PackedSegmentObjectPlacement::from_entry(&self.segment, entry, Vec::new())
                .expect("placement must be valid")
        }

        fn set_layout_for(&self, object: &EcObjectVersionKey) {
            let placement = self.placement_for(object);
            install_quorum_layout(&self.metadata, ObjectDataLayout::PackedSegment(placement));
        }

        async fn read_full(
            &self,
            object: &EcObjectVersionKey,
        ) -> Result<EcReadResult, EcReadError> {
            self.set_layout_for(object);
            self.coordinator
                .read(
                    EcReadRequest::full(object.clone()),
                    &self.metadata,
                    &self.small,
                    &self.shards,
                    &self.repairs,
                )
                .await
        }
    }

    async fn setup_packed_segment_fixture() -> PackedSegmentFixture {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let coordinator = coordinator(store.clone());
        let metadata = MemoryMetadata::default();
        let small = MemorySmallReader::default();
        let repairs = MemoryRepairQueue::default();

        let blob_len = 250usize;
        let blob = (0..blob_len).map(|idx| (idx % 251) as u8).collect::<Vec<_>>();
        let chunk_size = 64u64;
        let profile = rs_profile(chunk_size);
        let segment_id = PackedSegmentId::new_random();

        let (mut manifest, shard_bytes) = encoded_manifest(profile, &blob);
        let segment_key = packed_segment_manifest_key(segment_id);
        manifest.bucket = segment_key.bucket.clone();
        manifest.key = segment_key.key.clone();
        manifest.version_id = None;
        manifest.validate().unwrap();

        store.put_manifest(&manifest).await.unwrap();

        let obj_single_shard = EcObjectVersionKey::new("photos", "single-shard.bin", None);
        let obj_cross_shard = EcObjectVersionKey::new("photos", "cross-shard.bin", None);
        let obj_filler_1 = EcObjectVersionKey::new("photos", "filler-1.bin", None);
        let obj_cross_chunk = EcObjectVersionKey::new("photos", "cross-chunk.bin", None);
        let obj_filler_2 = EcObjectVersionKey::new("photos", "filler-2.bin", None);
        let obj_last_chunk = EcObjectVersionKey::new("photos", "last-chunk.bin", None);

        // Partition the 250-byte blob into contiguous non-overlapping objects:
        // [0..10): inside chunk 0, slot 0 (covers 0..16)
        // [10..20): across slot 0 (10..16) and slot 1 (16..20) of chunk 0
        // [20..58): filler up to chunk boundary
        // [58..70): across chunk 0 slot 3 (58..64) and chunk 1 slot 0 (64..70)
        // [70..200): filler into chunk 3
        // [200..250): inside chunk 3 (192..250), the short chunk
        let specs: Vec<(EcObjectVersionKey, u64, u64)> = vec![
            (obj_single_shard.clone(), 0, 10),
            (obj_cross_shard.clone(), 10, 10),
            (obj_filler_1, 20, 38),
            (obj_cross_chunk.clone(), 58, 12),
            (obj_filler_2, 70, 130),
            (obj_last_chunk.clone(), 200, 50),
        ];

        let config = PackedSegmentConfig {
            enabled: true,
            target_bytes: blob_len as u64,
            max_objects: 100,
            ..PackedSegmentConfig::default()
        };
        let mut segment = PackedSegment::open(segment_id, manifest.erasure_set_id, &config, 1_000);

        for (obj_key, offset, length) in specs {
            let reservation = segment.reserve(length).unwrap();
            let slice = &blob[offset as usize..(offset + length) as usize];
            let identity = PackedObjectIdentity::new(obj_key, sha256(slice), hlc())
                .with_s3_metadata("\"etag\"", "application/octet-stream", HashMap::new());
            let item = segment.journal_append(&reservation, identity).unwrap();
            segment.commit(&reservation, &item).unwrap();
        }

        segment.seal(2_000, SealReason::TargetSizeReached).unwrap();
        segment.validate().unwrap();
        store.put_packed_segment(&segment).await.unwrap();

        let shards = MemoryShardReader::new(shard_bytes);

        PackedSegmentFixture {
            _dir: dir,
            _store: store,
            coordinator,
            metadata,
            small,
            shards,
            repairs,
            blob,
            manifest,
            segment,
            obj_single_shard,
            obj_cross_shard,
            obj_cross_chunk,
            obj_last_chunk,
        }
    }

    #[tokio::test]
    async fn a_packed_object_reads_byte_for_byte_from_its_segment() {
        let fix = setup_packed_segment_fixture().await;

        // 1. Inside a single shard (chunk 0, slot 0: [0..10))
        let res_single = fix.read_full(&fix.obj_single_shard).await.unwrap();
        assert_eq!(res_single.body.as_ref(), &fix.blob[0..10]);
        assert_eq!(res_single.content_length, 10);

        // 2. Crosses a shard boundary (chunk 0, slot 0 [10..16) and slot 1 [16..20))
        let res_cross_shard = fix.read_full(&fix.obj_cross_shard).await.unwrap();
        assert_eq!(res_cross_shard.body.as_ref(), &fix.blob[10..20]);
        assert_eq!(res_cross_shard.content_length, 10);

        // 3. Crosses a chunk boundary (chunk 0 slot 3 [58..64) and chunk 1 slot 0 [64..70))
        let res_cross_chunk = fix.read_full(&fix.obj_cross_chunk).await.unwrap();
        assert_eq!(res_cross_chunk.body.as_ref(), &fix.blob[58..70]);
        assert_eq!(res_cross_chunk.content_length, 12);

        // 4. In the last incomplete chunk (chunk 3: [200..250))
        let res_last_chunk = fix.read_full(&fix.obj_last_chunk).await.unwrap();
        assert_eq!(res_last_chunk.body.as_ref(), &fix.blob[200..250]);
        assert_eq!(res_last_chunk.content_length, 50);
    }

    #[tokio::test]
    async fn direct_read_touches_only_the_shards_that_hold_the_object() {
        let k = 4u8; // profile k for rs_small()

        // 1. Single shard object: touches only chunk 0 slot 0
        {
            let fix = setup_packed_segment_fixture().await;
            fix.read_full(&fix.obj_single_shard).await.unwrap();
            let requested = fix.shards.slots_requested();
            assert_eq!(
                requested,
                vec![(0, 0)],
                "single shard object must only touch (0, 0)"
            );
            assert!(
                requested.iter().all(|&(_, slot)| slot < k),
                "must not touch any parity slot (k={k})"
            );
        }

        // 2. Cross-shard object: touches only chunk 0 slots 0 and 1
        {
            let fix = setup_packed_segment_fixture().await;
            fix.read_full(&fix.obj_cross_shard).await.unwrap();
            let requested = fix.shards.slots_requested();
            assert_eq!(
                requested,
                vec![(0, 0), (0, 1)],
                "cross shard object must only touch (0, 0) and (0, 1)"
            );
            assert!(
                requested.iter().all(|&(_, slot)| slot < k),
                "must not touch any parity slot (k={k})"
            );
        }

        // 3. Cross-chunk object: touches only chunk 0 slot 3 and chunk 1 slot 0
        {
            let fix = setup_packed_segment_fixture().await;
            fix.read_full(&fix.obj_cross_chunk).await.unwrap();
            let requested = fix.shards.slots_requested();
            assert_eq!(
                requested,
                vec![(0, 3), (1, 0)],
                "cross chunk object must only touch (0, 3) and (1, 0)"
            );
            assert!(
                requested.iter().all(|&(_, slot)| slot < k),
                "must not touch any parity slot (k={k})"
            );
        }

        // 4. Last incomplete chunk object: touches only data slots of chunk 3
        {
            let fix = setup_packed_segment_fixture().await;
            fix.read_full(&fix.obj_last_chunk).await.unwrap();
            let requested = fix.shards.slots_requested();
            assert_eq!(
                requested,
                vec![(3, 0), (3, 1), (3, 2), (3, 3)],
                "last chunk object must touch exactly the data slots it spans"
            );
            assert!(
                requested.iter().all(|&(_, slot)| slot < k),
                "must not touch any parity slot (k={k})"
            );
        }
    }

    #[tokio::test]
    async fn direct_read_reads_exactly_the_object_size() {
        let chunk_size = 64usize;

        // 1. Single shard 10-byte object
        let fix1 = setup_packed_segment_fixture().await;
        let res1 = fix1.read_full(&fix1.obj_single_shard).await.unwrap();
        assert_eq!(res1.body.len(), 10);
        let bytes1 = fix1.shards.bytes_returned();
        assert_eq!(
            bytes1, 10,
            "shards.bytes_returned() ({bytes1}) must equal object size (10)"
        );
        assert!(
            bytes1 < chunk_size,
            "shards.bytes_returned() ({bytes1}) must be strictly less than chunk size ({chunk_size})"
        );
        let ratio1 = (chunk_size as f64) / (bytes1 as f64);
        assert!(
            chunk_size >= bytes1 * 6,
            "bytes returned ({bytes1}) is {ratio1:.1}x smaller than chunk size ({chunk_size}), which is several times smaller"
        );

        // 2. Cross-chunk 12-byte object (spans 2 chunks, total chunk size 128)
        let fix2 = setup_packed_segment_fixture().await;
        let res2 = fix2.read_full(&fix2.obj_cross_chunk).await.unwrap();
        assert_eq!(res2.body.len(), 12);
        let bytes2 = fix2.shards.bytes_returned();
        assert_eq!(
            bytes2, 12,
            "shards.bytes_returned() ({bytes2}) must equal object size (12)"
        );
        assert!(
            bytes2 < chunk_size,
            "shards.bytes_returned() ({bytes2}) must be strictly less than chunk size ({chunk_size})"
        );
        let ratio2 = (chunk_size as f64) / (bytes2 as f64);
        assert!(
            chunk_size >= bytes2 * 5,
            "bytes returned ({bytes2}) is {ratio2:.1}x smaller than chunk size ({chunk_size}), which is several times smaller"
        );
    }

    #[tokio::test]
    async fn direct_read_does_not_rebuild_any_chunk() {
        let fix = setup_packed_segment_fixture().await;

        // 1. Normal direct read report: zero reconstructed chunks, zero degraded chunks
        let result = fix.read_full(&fix.obj_single_shard).await.unwrap();
        assert_eq!(
            result.chunks_reconstructed, 0,
            "direct read must not reconstruct any chunk"
        );
        assert_eq!(
            result.degraded_chunks, 0,
            "direct read must not decode any degraded chunk"
        );
        assert!(!result.repair_enqueued);

        let result_cross = fix.read_full(&fix.obj_cross_chunk).await.unwrap();
        assert_eq!(result_cross.chunks_reconstructed, 0);
        assert_eq!(result_cross.degraded_chunks, 0);

        // 2. Contrast with fallback: when direct read fails (corrupt data shard),
        // fallback to full chunk decode is triggered: chunks_reconstructed > 0 and degraded_chunks > 0.
        let fallback_fix = setup_packed_segment_fixture().await;
        fallback_fix.shards.corrupt_slots(&[(0, 0)]);
        let fallback_res = fallback_fix.read_full(&fallback_fix.obj_single_shard).await.unwrap();
        assert_eq!(fallback_res.body.as_ref(), &fallback_fix.blob[0..10]);
        assert!(
            fallback_res.chunks_reconstructed > 0,
            "fallback must reconstruct chunks (got {})",
            fallback_res.chunks_reconstructed
        );
        assert!(
            fallback_res.degraded_chunks > 0,
            "fallback must record degraded chunks (got {})",
            fallback_res.degraded_chunks
        );

        // 3. When segment record is missing: returns Err(EcReadError::Unavailable),
        // NOT a 404 NotFound and NOT a quiet empty response.
        let unknown_segment_id = PackedSegmentId::new_random();
        let unknown_placement = PackedSegmentObjectPlacement::new(
            unknown_segment_id,
            fix.manifest.erasure_set_id,
            0,
            10,
            sha256(&fix.blob[0..10]),
            hlc(),
            Vec::new(),
        )
        .unwrap();
        let unknown_obj = EcObjectVersionKey::new("photos", "unknown-segment.bin", None);
        install_quorum_layout(
            &fix.metadata,
            ObjectDataLayout::PackedSegment(unknown_placement),
        );
        let err = fix
            .coordinator
            .read(
                EcReadRequest::full(unknown_obj),
                &fix.metadata,
                &fix.small,
                &fix.shards,
                &fix.repairs,
            )
            .await
            .unwrap_err();

        assert!(
            matches!(err, EcReadError::Unavailable { .. }),
            "expected EcReadError::Unavailable for unknown segment, got {err:?}"
        );
        assert!(
            !matches!(err, EcReadError::NotFound { .. }),
            "must NOT return 404 NotFound when segment record is missing"
        );

        // 4. When shards are unrecoverable (all chunk 0 shards missing):
        // returns Err(EcReadError::Unavailable), NOT 404 and NOT a quiet empty response.
        let unavail_fix = setup_packed_segment_fixture().await;
        unavail_fix
            .shards
            .missing_slots(&[(0, 0), (0, 1), (0, 2), (0, 3), (0, 4), (0, 5)]);
        let err = unavail_fix.read_full(&unavail_fix.obj_single_shard).await.unwrap_err();
        assert!(
            matches!(err, EcReadError::Unavailable { .. }),
            "expected EcReadError::Unavailable when shards cannot be reconstructed, got {err:?}"
        );
        assert!(
            !matches!(err, EcReadError::NotFound { .. }),
            "must NOT return 404 NotFound when shards are unrecoverable"
        );
    }

    #[tokio::test]
    async fn a_missing_data_shard_falls_back_to_rebuilding_the_chunk() {
        let fix = setup_packed_segment_fixture().await;
        // In rs_profile(64), chunk_size=64, k=4, shard_size=16.
        // obj_single_shard is at offset 0 with length 10, located in chunk 0 slot 0.
        let slot = (0, 0);
        fix.shards.missing_slots(&[slot]);

        let result = fix.read_full(&fix.obj_single_shard).await.unwrap();

        assert_eq!(result.body.as_ref(), &fix.blob[0..10]);
        assert!(result.degraded_chunks >= 1);
        assert!(
            fix.shards.bytes_returned() > 10,
            "bytes_returned ({}) must be strictly greater than object length (10)",
            fix.shards.bytes_returned()
        );
    }

    #[tokio::test]
    async fn corrupt_shard_bytes_fall_back_and_still_return_the_right_object() {
        let fix = setup_packed_segment_fixture().await;
        fix.shards.corrupt_slots(&[(0, 0)]);

        let result = fix.read_full(&fix.obj_single_shard).await.unwrap();

        assert_eq!(result.body.as_ref(), &fix.blob[0..10]);
        assert!(result.degraded_chunks >= 1);
    }

    #[tokio::test]
    async fn range_request_inside_a_packed_object_returns_the_right_slice() {
        let fix = setup_packed_segment_fixture().await;
        fix.set_layout_for(&fix.obj_cross_chunk);

        // obj_cross_chunk spans offset 58 with length 12 (offsets 58..70 in blob).
        // 1. Middle range inside the object (offsets 3..=8, length 6).
        let res_mid = fix
            .coordinator
            .read(
                EcReadRequest::range(fix.obj_cross_chunk.clone(), EcByteRange::new(3, 8)),
                &fix.metadata,
                &fix.small,
                &fix.shards,
                &fix.repairs,
            )
            .await
            .unwrap();
        assert_eq!(res_mid.body.as_ref(), &fix.blob[61..67]);
        assert_eq!(res_mid.content_length, 6);
        assert_eq!(res_mid.content_range, Some((3, 8, 12)));

        // 2. Very beginning of the object (first byte, offset 0..=0, length 1).
        let res_first = fix
            .coordinator
            .read(
                EcReadRequest::range(fix.obj_cross_chunk.clone(), EcByteRange::new(0, 0)),
                &fix.metadata,
                &fix.small,
                &fix.shards,
                &fix.repairs,
            )
            .await
            .unwrap();
        assert_eq!(res_first.body.as_ref(), &fix.blob[58..59]);
        assert_eq!(res_first.content_length, 1);
        assert_eq!(res_first.content_range, Some((0, 0, 12)));

        // 3. Very end of the object (last byte, offset 11..=11, length 1).
        let res_last = fix
            .coordinator
            .read(
                EcReadRequest::range(fix.obj_cross_chunk.clone(), EcByteRange::new(11, 11)),
                &fix.metadata,
                &fix.small,
                &fix.shards,
                &fix.repairs,
            )
            .await
            .unwrap();
        assert_eq!(res_last.body.as_ref(), &fix.blob[69..70]);
        assert_eq!(res_last.content_length, 1);
        assert_eq!(res_last.content_range, Some((11, 11, 12)));
    }

    #[tokio::test]
    async fn head_of_a_purged_packed_object_keeps_its_etag_and_content_type() {
        let fix = setup_packed_segment_fixture().await;
        let placement = fix.placement_for(&fix.obj_single_shard);
        assert!(placement.hot_replica_nodes.is_empty());
        fix.set_layout_for(&fix.obj_single_shard);

        let head = fix
            .coordinator
            .head(fix.obj_single_shard.clone(), &fix.metadata, &fix.small)
            .await
            .unwrap();

        assert_eq!(head.etag, "\"etag\"");
        assert_eq!(head.content_type, "application/octet-stream");
        assert_eq!(head.size, placement.length);
        assert_eq!(
            head.content_hash_sha256,
            sha256(
                &fix.blob
                    [placement.offset as usize..(placement.offset + placement.length) as usize]
            )
        );
        assert_eq!(
            head.layout,
            EcReadLayout::PackedSegment {
                segment_id: placement.segment_id.as_uuid(),
                erasure_set_id: placement.erasure_set_id,
                offset: placement.offset,
                length: placement.length,
                hot_copies: false,
            }
        );
    }

    #[tokio::test]
    async fn a_packed_object_with_hot_copies_is_served_from_them() {
        let fix = setup_packed_segment_fixture().await;
        let mut placement = fix.placement_for(&fix.obj_single_shard);
        placement.hot_replica_nodes = vec![node(1), node(2), node(3)];
        placement.validate().unwrap();

        // Configure small reader's body to match the object's SHA-256 for RF=3 verification.
        let expected_bytes =
            &fix.blob[placement.offset as usize..(placement.offset + placement.length) as usize];
        *fix.small.body.lock().unwrap() = Bytes::copy_from_slice(expected_bytes);

        install_quorum_layout(
            &fix.metadata,
            ObjectDataLayout::PackedSegment(placement.clone()),
        );

        let result = fix
            .coordinator
            .read(
                EcReadRequest::full(fix.obj_single_shard.clone()),
                &fix.metadata,
                &fix.small,
                &fix.shards,
                &fix.repairs,
            )
            .await
            .unwrap();

        assert_eq!(
            fix.shards.reads.load(Ordering::SeqCst),
            0,
            "shards must not be read when hot copies exist"
        );
        assert_eq!(
            fix.shards.bytes_returned(),
            0,
            "no shard bytes must be returned when hot copies exist"
        );
        assert_eq!(
            result.head.layout,
            EcReadLayout::PackedSegment {
                segment_id: placement.segment_id.as_uuid(),
                erasure_set_id: placement.erasure_set_id,
                offset: placement.offset,
                length: placement.length,
                hot_copies: true,
            }
        );
    }

    #[tokio::test]
    async fn a_packed_object_whose_segment_record_is_missing_fails_loudly() {
        let dir = tempfile::tempdir().unwrap();
        let local = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let coordinator = coordinator(local);
        let metadata = MemoryMetadata::default();
        let small = MemorySmallReader::default();
        let shards = MemoryShardReader::default();
        let repairs = MemoryRepairQueue::default();

        let segment_id = PackedSegmentId::new_random();
        let obj = EcObjectVersionKey::new("photos", "missing-segment.bin", None);
        let placement = PackedSegmentObjectPlacement::new(
            segment_id,
            42,
            0,
            10,
            sha256(b"0123456789"),
            hlc(),
            Vec::new(),
        )
        .unwrap();
        install_quorum_layout(&metadata, ObjectDataLayout::PackedSegment(placement));

        let err = coordinator
            .read(
                EcReadRequest::full(obj),
                &metadata,
                &small,
                &shards,
                &repairs,
            )
            .await
            .unwrap_err();

        match &err {
            EcReadError::Unavailable { reason } => {
                assert!(
                    reason.contains(&segment_id.to_string()),
                    "error reason '{reason}' must contain segment_id '{segment_id}'"
                );
            }
            other => panic!("expected EcReadError::Unavailable, got {other:?}"),
        }

        assert!(
            !matches!(err, EcReadError::NotFound { .. }),
            "must NOT return 404 NotFound when segment record is missing"
        );
    }

    /// The plan's spans as the packed locator would have written them, so the
    /// two planners can be compared entry for entry.
    fn read_plan_spans(plan: &ReadPlan) -> Vec<PackedShardSpan> {
        plan.chunks
            .iter()
            .flat_map(|chunk| {
                chunk.spans.iter().map(|span| PackedShardSpan {
                    chunk_index: chunk.chunk_index,
                    slot_index: span.slot_index,
                    offset_in_shard: span.offset_in_shard,
                    length: span.length,
                })
            })
            .collect()
    }

    fn read_plan_for(manifest: &EcObjectManifest, start: u64, end: u64) -> ReadPlan {
        plan_read(manifest, Some(EcByteRange::new(start, end))).expect("plan should succeed")
    }

    /// 128 bytes over `rs_small`: two chunks of 64, `k = 4`, shard size 16.
    fn two_full_chunks() -> EcObjectManifest {
        let data = (0..128).map(|idx| (idx % 251) as u8).collect::<Vec<_>>();
        encoded_manifest(rs_profile(64), &data).0
    }

    #[test]
    fn a_read_plan_inside_one_shard_names_one_slot() {
        let manifest = two_full_chunks();
        // Chunk 0 slot 1 holds logical offsets [16..32); [20..28) is strictly inside it.
        let plan = read_plan_for(&manifest, 20, 27);

        assert_eq!(plan.chunks.len(), 1);
        assert_eq!(plan.chunks[0].chunk_index, 0);
        assert_eq!(plan.chunks[0].coverage, ChunkCoverage::Partial);
        assert_eq!(plan.chunks[0].slots, vec![1]);
        assert_eq!(
            plan.chunks[0].spans,
            vec![ShardReadSpan {
                slot_index: 1,
                offset_in_shard: 4,
                length: 8,
            }]
        );
        assert_eq!(plan.shard_reads(), 1);
        assert_eq!(plan.bytes(), 8);
    }

    #[test]
    fn a_read_plan_across_two_shards_names_both() {
        let manifest = two_full_chunks();
        // Slot 0 holds [0..16), slot 1 holds [16..32); [10..20) straddles them.
        let plan = read_plan_for(&manifest, 10, 19);

        assert_eq!(plan.chunks.len(), 1);
        assert_eq!(plan.chunks[0].coverage, ChunkCoverage::Partial);
        assert_eq!(plan.chunks[0].slots, vec![0, 1]);
        assert_eq!(
            plan.chunks[0].spans,
            vec![
                ShardReadSpan {
                    slot_index: 0,
                    offset_in_shard: 10,
                    length: 6,
                },
                ShardReadSpan {
                    slot_index: 1,
                    offset_in_shard: 0,
                    length: 4,
                },
            ]
        );
        assert_eq!(plan.shard_reads(), 2);
        assert_eq!(plan.bytes(), 10);
    }

    #[test]
    fn a_read_plan_across_two_chunks_splits_at_the_chunk_boundary() {
        let manifest = two_full_chunks();
        // [60..70) is the tail of chunk 0 slot 3 and the head of chunk 1 slot 0.
        let plan = read_plan_for(&manifest, 60, 69);

        assert_eq!(plan.chunks.len(), 2);
        assert_eq!(plan.chunks[0].chunk_index, 0);
        assert_eq!(plan.chunks[1].chunk_index, 1);
        assert_eq!(plan.chunks[0].coverage, ChunkCoverage::Partial);
        assert_eq!(plan.chunks[1].coverage, ChunkCoverage::Partial);
        assert_eq!(
            plan.chunks[0].spans,
            vec![ShardReadSpan {
                slot_index: 3,
                offset_in_shard: 12,
                length: 4,
            }]
        );
        assert_eq!(
            plan.chunks[1].spans,
            vec![ShardReadSpan {
                slot_index: 0,
                offset_in_shard: 0,
                length: 6,
            }]
        );
        assert_eq!(plan.shard_reads(), 2);
        assert_eq!(plan.bytes(), 10);
    }

    #[test]
    fn a_read_plan_for_the_whole_object_covers_every_chunk_in_full() {
        let manifest = two_full_chunks();
        let plan = plan_read(&manifest, None).expect("plan should succeed");

        assert_eq!(plan.chunks.len(), 2);
        for chunk in &plan.chunks {
            assert_eq!(chunk.coverage, ChunkCoverage::Full);
            assert_eq!(chunk.slots, vec![0, 1, 2, 3]);
            assert_eq!(chunk.spans.len(), 4);
            for (ordinal, span) in chunk.spans.iter().enumerate() {
                assert_eq!(span.slot_index, ordinal as u8);
                assert_eq!(span.offset_in_shard, 0);
                assert_eq!(span.length, 16);
            }
        }
        // `k` reads per chunk, not `n`: the whole point of EC2-180.
        assert_eq!(plan.shard_reads(), usize::from(manifest.profile.k) * 2);
        assert_eq!(plan.bytes(), 128);
    }

    #[test]
    fn a_read_plan_for_one_byte_reads_one_shard() {
        let manifest = two_full_chunks();
        // Byte 33 is the second byte of chunk 0 slot 2, which holds [32..48).
        let plan = read_plan_for(&manifest, 33, 33);

        assert_eq!(plan.shard_reads(), 1);
        assert_eq!(plan.chunks[0].coverage, ChunkCoverage::Partial);
        assert_eq!(
            plan.chunks[0].spans,
            vec![ShardReadSpan {
                slot_index: 2,
                offset_in_shard: 1,
                length: 1,
            }]
        );
    }

    #[test]
    fn a_read_plan_for_the_last_byte_stops_at_the_last_real_shard() {
        let manifest = two_full_chunks();
        let plan = read_plan_for(&manifest, 127, 127);

        assert_eq!(plan.chunks.len(), 1);
        assert_eq!(plan.chunks[0].chunk_index, 1);
        assert_eq!(
            plan.chunks[0].spans,
            vec![ShardReadSpan {
                slot_index: 3,
                offset_in_shard: 15,
                length: 1,
            }]
        );
    }

    #[test]
    fn a_read_plan_marks_a_fully_covered_chunk_full_and_its_neighbour_partial() {
        let manifest = two_full_chunks();

        let exact = read_plan_for(&manifest, 0, 63);
        assert_eq!(exact.chunks.len(), 1);
        assert_eq!(exact.chunks[0].coverage, ChunkCoverage::Full);
        assert_eq!(exact.chunks[0].slots, vec![0, 1, 2, 3]);

        // A range that swallows chunk 0 and clips chunk 1 verifies chunk 0 by
        // its chunk hash and chunk 1 by shard hashes (EC2-181).
        let spilling = read_plan_for(&manifest, 0, 70);
        assert_eq!(spilling.chunks.len(), 2);
        assert_eq!(spilling.chunks[0].coverage, ChunkCoverage::Full);
        assert_eq!(spilling.chunks[1].coverage, ChunkCoverage::Partial);
        assert_eq!(spilling.chunks[1].slots, vec![0]);
    }

    #[test]
    fn a_read_plan_never_names_padding_past_the_end_of_a_chunk() {
        // 94 bytes over chunk_size 64: chunk 1 holds 30 bytes, so with k = 4 its
        // shard size is 8 and its last data shard holds 6 real bytes of 8.
        let data = (0..94).map(|idx| (idx % 251) as u8).collect::<Vec<_>>();
        let (manifest, _) = encoded_manifest(rs_profile(64), &data);

        let tail = read_plan_for(&manifest, 86, 93);
        assert_eq!(tail.chunks.len(), 1);
        assert_eq!(tail.chunks[0].chunk_index, 1);
        assert_eq!(
            tail.chunks[0].spans,
            vec![
                ShardReadSpan {
                    slot_index: 2,
                    offset_in_shard: 6,
                    length: 2,
                },
                ShardReadSpan {
                    slot_index: 3,
                    offset_in_shard: 0,
                    length: 6,
                },
            ]
        );

        // The padded shard is 8 bytes long and only 6 of them belong to the
        // object; a plan that reached the other two would hand back padding.
        let whole = plan_read(&manifest, None).expect("plan should succeed");
        let last = whole.chunks.last().expect("second chunk");
        assert_eq!(last.coverage, ChunkCoverage::Full);
        assert_eq!(
            last.spans.last(),
            Some(&ShardReadSpan {
                slot_index: 3,
                offset_in_shard: 0,
                length: 6,
            })
        );
        assert_eq!(whole.bytes(), 94);
    }

    #[test]
    fn a_read_plan_for_a_full_chunk_fetches_a_data_slot_that_is_only_padding() {
        // 69 bytes over chunk_size 64: chunk 1 holds 5 bytes, so with k = 4 its
        // shard size is 2 and slot 3 is padding from end to end.
        let data = (0..69).map(|idx| (idx % 251) as u8).collect::<Vec<_>>();
        let (manifest, _) = encoded_manifest(rs_profile(64), &data);
        let plan = plan_read(&manifest, None).expect("plan should succeed");

        let last = plan.chunks.last().expect("second chunk");
        assert_eq!(last.coverage, ChunkCoverage::Full);
        // Assembly needs every data slot, padding included, so all `k` are
        // fetched even though only three of them carry object bytes.
        assert_eq!(last.slots, vec![0, 1, 2, 3]);
        assert_eq!(
            last.spans,
            vec![
                ShardReadSpan {
                    slot_index: 0,
                    offset_in_shard: 0,
                    length: 2,
                },
                ShardReadSpan {
                    slot_index: 1,
                    offset_in_shard: 0,
                    length: 2,
                },
                ShardReadSpan {
                    slot_index: 2,
                    offset_in_shard: 0,
                    length: 1,
                },
            ]
        );
        assert_eq!(plan.bytes(), 69);
    }

    #[test]
    fn a_read_plan_rejects_a_manifest_whose_shard_size_disagrees_with_its_chunk() {
        let mut manifest = two_full_chunks();
        // Formally valid — `ChunkManifest::validate` only asks that `encoded_size
        // * k` covers `data_size` — but every offset below it would be wrong.
        manifest.chunks[0].encoded_size = 20;

        let error = plan_read(&manifest, None).expect_err("plan must refuse this manifest");
        match error {
            EcReadError::InvalidManifest { reason } => {
                assert!(
                    reason.contains("20") && reason.contains("16"),
                    "reason '{reason}' must name both the declared and the derived shard size"
                );
            }
            other => panic!("expected InvalidManifest, got {other:?}"),
        }
    }

    #[test]
    fn a_read_plan_and_the_packed_locator_agree_on_every_range() {
        // Two independently written planners compared on every range of every
        // fixture: agreement is the only runtime-free check an addressing
        // mistake has (EC2-181).
        let fixtures = [
            (rs_profile(10), 23usize),
            (rs_profile(64), 128),
            (rs_profile(64), 94),
            (rs_profile(64), 69),
            (lrc_profile(12), 30),
            (rs_standard_profile(12), 31),
        ];

        for (profile, size) in fixtures {
            let data = (0..size).map(|idx| (idx % 251) as u8).collect::<Vec<_>>();
            let (manifest, _) = encoded_manifest(profile.clone(), &data);
            let total = size as u64;

            for start in 0..total {
                for end in start..total {
                    let length = end - start + 1;
                    let plan = read_plan_for(&manifest, start, end);
                    let located = locate_packed_object(&manifest, start, length)
                        .expect("locator should succeed");
                    assert_eq!(
                        read_plan_spans(&plan),
                        located,
                        "planners disagree on {start}..={end} of a {size}-byte object in '{}'",
                        profile.name
                    );
                }
            }
        }
    }

    #[test]
    fn a_read_plan_spans_always_add_up_to_the_requested_range() {
        let fixtures = [
            (rs_profile(10), 23usize),
            (rs_profile(64), 94),
            (lrc_profile(12), 30),
            (rs_standard_profile(12), 31),
        ];

        for (profile, size) in fixtures {
            let data = (0..size).map(|idx| (idx % 251) as u8).collect::<Vec<_>>();
            let (manifest, _) = encoded_manifest(profile.clone(), &data);
            let k = u64::from(manifest.profile.k);
            let total = size as u64;

            for start in 0..total {
                for end in start..total {
                    let plan = read_plan_for(&manifest, start, end);
                    assert_eq!(
                        plan.bytes(),
                        end - start + 1,
                        "plan for {start}..={end} of '{}' does not add up",
                        profile.name
                    );

                    for chunk in &plan.chunks {
                        let manifest_chunk = manifest
                            .chunks
                            .iter()
                            .find(|candidate| candidate.chunk_index == chunk.chunk_index)
                            .expect("planned chunk must exist in the manifest");
                        let shard_size = manifest_chunk.encoded_size;

                        // Slots are ascending, named once, and inside `k`.
                        let mut previous = None;
                        for slot in &chunk.slots {
                            assert!(previous < Some(*slot), "slots must ascend without repeats");
                            previous = Some(*slot);
                            assert!(u64::from(*slot) < k);
                        }
                        if chunk.coverage == ChunkCoverage::Full {
                            assert_eq!(chunk.slots.len(), usize::from(manifest.profile.k));
                        } else {
                            assert_eq!(
                                chunk.slots,
                                chunk.spans.iter().map(|span| span.slot_index).collect::<Vec<_>>()
                            );
                        }

                        for span in &chunk.spans {
                            assert!(span.length > 0);
                            let shard_start = u64::from(span.slot_index) * shard_size;
                            let real_len = shard_size.min(manifest_chunk.data_size - shard_start);
                            assert!(
                                span.offset_in_shard + span.length <= real_len,
                                "span {span:?} reaches into padding of chunk {}",
                                chunk.chunk_index
                            );
                        }
                    }
                }
            }
        }
    }
}
