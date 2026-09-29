// Copyright 2026 S4Core Team
//
// Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

//! Enterprise EC transcode runtime wiring.
//!
//! The staged transcode algorithm lives in `ec_transcode`. This module connects
//! that algorithm to the live cluster: hot RF=3 reads, durable shard writes over
//! storage/RPC, quorum-readable manifest objects, and local EC metadata cutover.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use s4_cluster::rpc::proto;
use s4_cluster::{
    ClusterError, HlcTimestamp, NodeClient, NodeId, QuorumReadCoordinator, QuorumWriteCoordinator,
};
use s4_core::{IndexRecord, StorageEngine};
use sha2::{Digest, Sha256};
use tokio::task::JoinSet;
use uuid::Uuid;

use crate::codec_runtime::EcCodecCpuLimiter;
use crate::ec_gc::{
    EcGcConfig, EcGcCoordinator, EcGcError, EcOrphanShardObservation, EcPackedHotCopyPurger,
    EcPackedPointerCommitter, EcPackedProbeOutcome, EcPackedSegmentProbe, EcPackedSegmentStats,
    EcTombstoneCommitResult, EcTombstoneQuorum, PackedRepackPolicy, PackedSegmentRetentionSource,
};
use crate::ec_metadata::{
    decode_manifest, encode_manifest, manifest_storage_hlc, EcHotReplicaGcRecord,
    EcManifestReadResult, EcMetadataError, EcMetadataStore, EcObjectManifest, EcObjectManifestRef,
    EcObjectVersionKey, EcPlacementState, EcTranscodeFailureClass, EcTranscodeFailureRecord,
    EcTranscodeJobRecord, EcTranscodeOrigin, EcTranscodeTarget, LocalShardKey, LocalShardRecord,
    LocalShardState, ObjectDataLayout, PackedSegmentObjectPlacement,
};
use crate::ec_metrics;
use crate::ec_packed_segments::{
    packed_segment_id_from_manifest_key, PackedSegment, PackedSegmentConfig, PackedSegmentEntry,
    PackedSegmentId, PackedSegmentState,
};
use crate::ec_placement::EcErasureSets;
use crate::ec_read::{
    EcReadCoordinator, EcReadError, EcReadShardRequest, EcReadShardResponse, EcShardReader,
    NoopEcReadRepairQueue,
};
use crate::ec_tiering::heat;
use crate::ec_transcode::{
    content_hash_is_absent, deterministic_operation_id, EcCutoverGuard, EcCutoverOutcome,
    EcHotReplicaPurgeDecision, EcManifestCommitResult, EcManifestQuorum, EcObjectCutover,
    EcShardReadRequest, EcShardStore, EcShardWriteAck, EcShardWriteRequest, EcSourceObject,
    EcStagedTranscodeWorker, EcTranscodeConfig, EcTranscodeError, EcTranscodeOutcome,
    EcTranscodeRequest, EcWriteBackpressure,
};
use crate::erasure_coding::{ErasureSet, ErasureSetRegistry};

pub(crate) const SYSTEM_BUCKET: &str = "__system__";
const EC_SHARD_CONTENT_TYPE: &str = "application/vnd.s4.ec.shard";
const EC_MANIFEST_CONTENT_TYPE: &str = "application/vnd.s4.ec.manifest+bin";
const DEFAULT_WORKER_POLL_INTERVAL_MS: u64 = 1_000;
const DEFAULT_WORKER_BATCH_SIZE: usize = 64;
const DEFAULT_MAX_SOURCE_READ_BYTES: u64 = 1_073_741_824;

/// Configuration for the supervised EC transcode runtime loop.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EcTranscodeWorkerConfig {
    /// Poll interval for queued and recoverable transcode jobs.
    pub poll_interval: Duration,
    /// Maximum jobs selected per loop before concurrency limiting.
    pub batch_size: usize,
    /// Maximum hot object size this runtime will materialize for the v1 encoder.
    pub max_source_read_bytes: u64,
}

impl EcTranscodeWorkerConfig {
    /// Parse worker runtime settings from environment variables.
    pub fn from_env() -> Result<Self, ClusterError> {
        let poll_interval_ms = parse_optional_u64("S4_EC_TRANSCODE_WORKER_POLL_MS")?
            .unwrap_or(DEFAULT_WORKER_POLL_INTERVAL_MS);
        if poll_interval_ms == 0 {
            return Err(invalid_config(
                "S4_EC_TRANSCODE_WORKER_POLL_MS",
                "must be greater than 0",
            ));
        }

        let batch_size = parse_optional_usize("S4_EC_TRANSCODE_WORKER_BATCH_SIZE")?
            .unwrap_or(DEFAULT_WORKER_BATCH_SIZE);
        if batch_size == 0 {
            return Err(invalid_config(
                "S4_EC_TRANSCODE_WORKER_BATCH_SIZE",
                "must be greater than 0",
            ));
        }

        let max_source_read_bytes = parse_optional_u64("S4_EC_TRANSCODE_MAX_SOURCE_BYTES")?
            .unwrap_or(DEFAULT_MAX_SOURCE_READ_BYTES);
        if max_source_read_bytes == 0 {
            return Err(invalid_config(
                "S4_EC_TRANSCODE_MAX_SOURCE_BYTES",
                "must be greater than 0",
            ));
        }

        Ok(Self {
            poll_interval: Duration::from_millis(poll_interval_ms),
            batch_size,
            max_source_read_bytes,
        })
    }
}

impl Default for EcTranscodeWorkerConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_millis(DEFAULT_WORKER_POLL_INTERVAL_MS),
            batch_size: DEFAULT_WORKER_BATCH_SIZE,
            max_source_read_bytes: DEFAULT_MAX_SOURCE_READ_BYTES,
        }
    }
}

/// Dependencies required by the live transcode worker.
#[derive(Clone)]
pub struct EcTranscodeRuntimeDeps {
    /// Local node ID.
    pub local_node_id: NodeId,
    /// Cluster topology epoch used for replica RPCs.
    pub topology_epoch: u64,
    /// Every erasure set of this pool, live.
    ///
    /// A job names the set it was admitted into, and the worker encodes into
    /// that one. Every node of the pool sees every job and the operation lease
    /// decides who runs it, so no node is the only one that can drain a set.
    /// Live rather than a copy, because an operator can add a set to a running
    /// pool and a worker holding a startup snapshot would refuse its jobs
    /// forever.
    pub sets: Arc<EcErasureSets>,
    /// The pool's default set.
    ///
    /// Only the set the worker's ports start out bound to. Every path that acts
    /// on a particular object rebinds them to that object's own set first, so
    /// this is a starting point rather than a choice.
    pub default_set: ErasureSet,
    /// Node-local EC metadata store.
    pub metadata: Arc<EcMetadataStore>,
    /// Hot RF=3 quorum reader.
    pub read_coordinator: Arc<QuorumReadCoordinator>,
    /// Hot RF=3 quorum writer/deleter.
    pub write_coordinator: Arc<QuorumWriteCoordinator>,
    /// Local storage engine for assigned local shards and manifest copies.
    pub storage: Arc<dyn StorageEngine>,
    /// Inter-node RPC client.
    pub rpc_client: Arc<NodeClient>,
    /// Codec worker limiter.
    pub codec_limiter: EcCodecCpuLimiter,
    /// Staged transcode settings.
    pub transcode: EcTranscodeConfig,
    /// Runtime loop settings.
    pub worker: EcTranscodeWorkerConfig,
    /// Tombstone/orphan GC settings.
    pub gc: EcGcConfig,
    /// Small-object packing settings, used when a repack opens a replacement.
    pub packed_segments: PackedSegmentConfig,
    /// When a fragmented packed segment becomes worth repacking.
    pub packed_repack: PackedRepackPolicy,
    /// Where the repacker asks whether an object is under Object Lock.
    pub packed_retention: Arc<dyn PackedSegmentRetentionSource>,
    /// Maximum records scanned while looking for work.
    pub scan_limit: usize,
    /// Width of the tiering read-counting window, in nanoseconds.
    ///
    /// Needed only to shape an activity record this runtime may have to create
    /// when it records a migration against an object nobody has read here.
    pub tiering_promote_window_nanos: u64,
    /// What `S4_EC_MINIMAL_READ_ENABLED` says on this node.
    ///
    /// Reading an object back out of a segment — to repack it or to unpack it —
    /// serves the object's own bytes, so it follows the same switch a client
    /// read does rather than keeping a second policy of its own.
    pub minimal_read: bool,
}

/// Supervised runtime that drains durable EC transcode jobs.
#[derive(Clone)]
pub struct EcTranscodeWorkerRuntime {
    local_node_id: NodeId,
    sets: Arc<EcErasureSets>,
    metadata: Arc<EcMetadataStore>,
    read_coordinator: Arc<QuorumReadCoordinator>,
    staged_worker: EcStagedTranscodeWorker,
    gc: EcGcCoordinator,
    ports: EcClusterTranscodePorts,
    transcode_config: EcTranscodeConfig,
    worker_config: EcTranscodeWorkerConfig,
    packed_config: PackedSegmentConfig,
    packed_repack: PackedRepackPolicy,
    packed_retention: Arc<dyn PackedSegmentRetentionSource>,
    scan_limit: usize,
    promote_window_nanos: u64,
    minimal_read: bool,
}

impl EcTranscodeWorkerRuntime {
    /// Build a live transcode runtime from cluster dependencies.
    pub fn new(deps: EcTranscodeRuntimeDeps) -> Self {
        let backpressure = EcWriteBackpressure::new(deps.transcode.clone());
        let staged_worker = EcStagedTranscodeWorker::new(
            Arc::clone(&deps.metadata),
            deps.codec_limiter,
            backpressure,
        );
        let ports = EcClusterTranscodePorts::new(EcClusterTranscodePortsDeps {
            local_node_id: deps.local_node_id,
            topology_epoch: deps.topology_epoch,
            sets: deps.sets.current(),
            erasure_set: deps.default_set,
            metadata: Arc::clone(&deps.metadata),
            read_coordinator: Arc::clone(&deps.read_coordinator),
            write_coordinator: Arc::clone(&deps.write_coordinator),
            storage: deps.storage,
            rpc_client: deps.rpc_client,
        });
        let gc = EcGcCoordinator::new(Arc::clone(&deps.metadata), deps.gc, Arc::clone(&deps.sets));

        Self {
            local_node_id: deps.local_node_id,
            sets: deps.sets,
            metadata: deps.metadata,
            read_coordinator: deps.read_coordinator,
            staged_worker,
            gc,
            ports,
            transcode_config: deps.transcode,
            worker_config: deps.worker,
            packed_config: deps.packed_segments,
            packed_repack: deps.packed_repack,
            packed_retention: deps.packed_retention,
            scan_limit: deps.scan_limit,
            promote_window_nanos: deps.tiering_promote_window_nanos,
            minimal_read: deps.minimal_read,
        }
    }

    /// Run the worker until the task is aborted by the server lifecycle.
    pub async fn run(self) {
        loop {
            match self.run_once().await {
                Ok(processed) if processed > 0 => {
                    tracing::debug!(processed, "EC transcode worker processed jobs");
                }
                Ok(_) => {}
                Err(error) => {
                    tracing::warn!(error = %error, "EC transcode worker loop failed");
                }
            }
            tokio::time::sleep(self.worker_config.poll_interval).await;
        }
    }

    /// Run one bounded scan/process cycle.
    pub async fn run_once(&self) -> Result<usize, EcTranscodeError> {
        let now = unix_time_nanos();
        let max_jobs = self
            .worker_config
            .batch_size
            .min(self.transcode_config.max_concurrent_transcodes)
            .max(1);
        // Before looking for work, make sure the work that has nobody to file it
        // is filed. A sealed segment is queued by this worker or by no one.
        match self.admit_sealed_packed_segments(now, max_jobs).await {
            Ok(0) => {}
            Ok(admitted) => tracing::debug!(admitted, "queued sealed packed segments"),
            Err(error) => {
                tracing::warn!(%error, "could not queue sealed packed segments this cycle");
            }
        }

        let jobs = self.find_eligible_jobs(now, max_jobs).await?;
        let mut processed = 0usize;

        let mut set = JoinSet::new();
        for job in jobs {
            let runtime = self.clone();
            set.spawn(async move { runtime.process_job(job).await });
        }

        while let Some(result) = set.join_next().await {
            processed = processed.saturating_add(1);
            match result {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    tracing::warn!(error = %error, "EC transcode job failed");
                }
                Err(error) => {
                    tracing::warn!(error = %error, "EC transcode task join failed");
                }
            }
        }

        processed = processed.saturating_add(self.process_hot_replica_gc(now, max_jobs).await?);
        match self.release_encoded_packed_segments(max_jobs).await {
            Ok(0) => {}
            Ok(released) => {
                processed = processed.saturating_add(released);
                tracing::debug!(released, "released packed segments");
            }
            Err(error) => {
                tracing::warn!(%error, "could not release packed segments this cycle");
            }
        }
        match self.reclaim_packed_segment_space(now, max_jobs).await {
            Ok(0) => {}
            Ok(reclaimed) => {
                processed = processed.saturating_add(reclaimed);
                tracing::debug!(reclaimed, "advanced packed segment repacking");
            }
            Err(error) => {
                tracing::warn!(%error, "could not repack packed segments this cycle");
            }
        }
        Ok(processed)
    }

    /// Publish what the analyzer saw, so an operator can see it too.
    ///
    /// Every set the pool has is published, including the ones with no segments
    /// at all: a series that stops being written keeps its last value forever,
    /// so a set whose segments were all retired would otherwise look busy for
    /// good.
    fn publish_packed_segment_stats(&self, stats: &EcPackedSegmentStats) {
        let known_sets: Vec<u64> =
            self.sets.current().sets().iter().map(|set| set.set_id).collect();
        ec_metrics::set_packed_segments_by_state(&known_sets, &stats.segments_by_state);
        ec_metrics::set_packed_segment_used_bytes(&known_sets, &stats.used_bytes);
        ec_metrics::set_packed_segment_dead_bytes(&known_sets, &stats.dead_bytes);
        ec_metrics::set_packed_segment_dead_ratio(&known_sets, &stats.dead_ratio);
        ec_metrics::set_packed_repack_queue_depth(&known_sets, &stats.repack_queue);
    }

    /// Move the repack of every fragmented segment one step forward.
    ///
    /// A repack is three steps spread over cycles, because the middle one is
    /// the transcode pipeline and takes as long as an encode takes. Staging
    /// describes a replacement, the pipeline encodes it, and the cutover
    /// switches the objects onto it and retires the source. Each step is picked
    /// up from durable state, so a node that restarts between two of them
    /// resumes rather than starts again.
    ///
    /// Retiring drained segments is done here too, on the same records, so that
    /// the whole lifecycle of a segment past its usefulness lives in one place.
    async fn reclaim_packed_segment_space(
        &self,
        now: u64,
        max_segments: usize,
    ) -> Result<usize, EcTranscodeError> {
        if !self.packed_config.enabled {
            return Ok(0);
        }

        let plan = self
            .gc
            .plan_packed_repacks(
                &self.packed_repack,
                self.packed_retention.as_ref(),
                max_segments,
            )
            .await
            .map_err(|error| {
                EcTranscodeError::Metadata(EcMetadataError::Database(error.to_string()))
            })?;
        self.publish_packed_segment_stats(&plan.stats);
        tracing::debug!(
            scanned = plan.scanned,
            fragmented = plan.fragmented.len(),
            pending_cutover = plan.pending_cutover.len(),
            pending_encode = plan.pending_encode.len(),
            empty = plan.empty.len(),
            blocked_by_retention = plan.blocked_by_retention,
            states = ?plan.stats.segments_by_state,
            dead_bytes = ?plan.stats.dead_bytes,
            "packed segment analyzer pass"
        );
        let mut advanced = 0usize;

        // Cutovers first: they finish work already paid for, and finishing one
        // is what lets the source segment's space actually come back.
        for destination in &plan.pending_cutover {
            let Some(erasure_set) =
                self.sets.current().resolve_manifest_set(destination.erasure_set_id).cloned()
            else {
                continue;
            };
            let ports = self.packed_segment_ports(&erasure_set);
            match self.gc.complete_packed_repack(destination, &ports, &ports, now).await {
                Ok(report) => {
                    tracing::info!(
                        source_id = %report.source_id,
                        destination_id = %report.destination_id,
                        moved = report.moved,
                        abandoned = report.abandoned,
                        degraded_probes = report.degraded_probes,
                        reclaimed_bytes = report.reclaimed_bytes,
                        "packed segment repack completed"
                    );
                    advanced = advanced.saturating_add(1);
                }
                Err(error) => {
                    // Nothing moved. The source still serves every object it
                    // holds, and the next cycle tries again.
                    tracing::warn!(
                        segment_id = %destination.segment_id,
                        %error,
                        "repacked segment is not ready to take over reads yet"
                    );
                }
            }
        }

        // A replacement whose objects moved on cannot be encoded and would
        // retry for ever. Checked before staging anything new, so the source it
        // came from becomes a candidate again in the same pass.
        for destination in &plan.pending_encode {
            match self.gc.discard_drifted_replacement(destination, now).await {
                Ok(true) => {
                    if let Err(error) = self
                        .metadata
                        .delete_transcode_jobs_for_object(
                            &destination.manifest_object_key(),
                            self.scan_limit,
                        )
                        .await
                    {
                        tracing::warn!(
                            segment_id = %destination.segment_id,
                            %error,
                            "discarded replacement kept its transcode job"
                        );
                    }
                    advanced = advanced.saturating_add(1);
                }
                Ok(false) => {}
                Err(error) => tracing::warn!(
                    segment_id = %destination.segment_id,
                    %error,
                    "could not check whether a staged replacement is still current"
                ),
            }
        }

        for segment in &plan.empty {
            if let Err(error) = self.gc.retire_empty_packed_segment(segment, now).await {
                tracing::warn!(
                    segment_id = %segment.segment_id,
                    %error,
                    "emptied packed segment could not be retired this cycle"
                );
                continue;
            }
            advanced = advanced.saturating_add(1);
        }

        for segment in &plan.fragmented {
            match self.gc.repack_packed_segment(segment, &self.packed_config, now).await {
                Ok(_) => advanced = advanced.saturating_add(1),
                Err(error) => tracing::warn!(
                    segment_id = %segment.segment_id,
                    %error,
                    "fragmented packed segment could not be staged for repacking"
                ),
            }
        }

        let ports = self.packed_segment_ports(&self.ports.erasure_set.clone());
        let retire_hlc = HlcTimestamp::from_record(unix_time_millis(), 0, self.local_node_id.0);
        match self
            .gc
            .retire_drained_packed_segments(
                now,
                retire_hlc,
                self.local_node_id,
                &ports,
                max_segments,
            )
            .await
        {
            Ok(report) => advanced = advanced.saturating_add(report.retired),
            Err(error) => {
                tracing::warn!(%error, "could not retire drained packed segments this cycle")
            }
        }

        Ok(advanced)
    }

    /// Queue every sealed packed segment that is not already being encoded.
    ///
    /// A segment is sealed by the write path and then has nobody to speak for
    /// it: no client write follows it, and the tiering scanner walks objects,
    /// not containers. This is the step that hands it to the pipeline, and
    /// without it a sealed segment would sit forever at three copies —
    /// correct, readable, and costing exactly what packing was meant to save.
    ///
    /// Idempotent: the operation id is derived from the segment and its
    /// generation, so a segment already queued is recognised rather than queued
    /// twice.
    async fn admit_sealed_packed_segments(
        &self,
        now: u64,
        max_segments: usize,
    ) -> Result<usize, EcTranscodeError> {
        let segments = self.metadata.list_packed_segments(self.scan_limit.max(1)).await?;
        let mut admitted = 0usize;

        for segment in segments {
            if admitted >= max_segments {
                break;
            }
            if segment.state != PackedSegmentState::Sealed || segment.entries.is_empty() {
                continue;
            }
            if self.sets.current().resolve_manifest_set(segment.erasure_set_id).is_none() {
                continue;
            }
            let Some(source_hlc) = segment.entries.iter().map(|entry| entry.hlc).max() else {
                continue;
            };

            let object = segment.manifest_object_key();
            let operation_id =
                deterministic_operation_id(&object, source_hlc, segment.erasure_set_id);
            if self.metadata.get_transcode_job(operation_id).await?.is_some() {
                continue;
            }

            let job = EcTranscodeJobRecord {
                object,
                operation_id,
                source_hlc,
                target_erasure_set_id: segment.erasure_set_id,
                target: EcTranscodeTarget::PackedSegment,
                state: EcPlacementState::EcTranscodeQueued,
                attempt: 0,
                object_size: segment.used_bytes,
                // Not known until the objects are read back, and deliberately
                // not guessed: `EcSourceObject::for_packed_segment` checks every
                // object against the hash the segment index recorded, which is
                // a stronger statement than one hash over the whole blob.
                source_content_hash_sha256: [0u8; 32],
                updated_at: now,
                next_retry_at: None,
                failure: None,
                origin: EcTranscodeOrigin::ClientWrite,
            };
            self.metadata.put_transcode_job(&job).await?;
            tracing::info!(
                segment_id = %segment.segment_id,
                erasure_set_id = segment.erasure_set_id,
                objects = segment.entries.len(),
                bytes = segment.used_bytes,
                "queued a sealed packed segment for encoding"
            );
            admitted = admitted.saturating_add(1);
        }
        Ok(admitted)
    }

    /// Release every encoded segment whose objects EC can already serve.
    ///
    /// The step that actually makes packing pay: until it runs, a segment's
    /// objects exist twice — once in the shards and once on their RF=3 replicas
    /// — and the pool is using more space than before it was packed, not less.
    async fn release_encoded_packed_segments(
        &self,
        max_segments: usize,
    ) -> Result<usize, EcTranscodeError> {
        let segments = self.metadata.list_packed_segments(self.scan_limit.max(1)).await?;
        let mut released = 0usize;

        for segment in segments {
            if released >= max_segments {
                break;
            }
            if segment.state != PackedSegmentState::Encoded {
                continue;
            }
            if segment.repacked_from.is_some() {
                // A repack replacement reaches `Encoded` too, but its objects
                // are still reading from the segment it replaces and have no
                // hot copies to release. Its cutover belongs to the repacker.
                continue;
            }
            let Some(erasure_set) =
                self.sets.current().resolve_manifest_set(segment.erasure_set_id).cloned()
            else {
                continue;
            };

            let release = self.packed_segment_ports(&erasure_set);
            match self
                .gc
                .release_packed_segment_hot_copies(&segment, &release, &release, &release)
                .await
            {
                Ok(report) => {
                    tracing::info!(
                        segment_id = %segment.segment_id,
                        probed = report.probed,
                        degraded_probes = report.degraded_probes,
                        committed = report.committed,
                        switched = report.switched,
                        purged = report.purged,
                        "released a packed segment's hot copies"
                    );
                    released = released.saturating_add(1);
                }
                Err(error) => {
                    // Not a failure of the segment: the data is intact and still
                    // has its hot copies. The next cycle tries again.
                    tracing::warn!(
                        segment_id = %segment.segment_id,
                        %error,
                        "packed segment is not ready to release its hot copies yet"
                    );
                }
            }
        }
        Ok(released)
    }

    /// Gather a sealed segment's objects back into the blob that gets encoded.
    ///
    /// Each object is read through the same hot quorum path a single-object
    /// transcode uses, so a segment costs no new read machinery — only the
    /// assembly, which `EcSourceObject::for_packed_segment` performs and
    /// verifies.
    async fn read_packed_segment_source(
        &self,
        job: &EcTranscodeJobRecord,
        now: u64,
    ) -> Result<EcSourceObject, EcTranscodeError> {
        let segment_id = packed_segment_id_from_manifest_key(&job.object).map_err(|error| {
            EcTranscodeError::InvalidSource {
                reason: error.to_string(),
            }
        })?;
        let segment = self.metadata.get_packed_segment(segment_id).await?.ok_or_else(|| {
            EcTranscodeError::InvalidSource {
                reason: format!("packed segment {segment_id} has no record to encode"),
            }
        })?;

        let mut parts = Vec::with_capacity(segment.entries.len());
        for entry in &segment.entries {
            // A repacked object has no hot copies: it is being moved out of
            // another segment's shards, which are still the only place its
            // bytes exist. Everything else is a fresh client write, whose bytes
            // are the RF=3 copies the write path produced.
            let packed_source = match self.metadata.get_object_layout(&entry.object).await? {
                Some(ObjectDataLayout::PackedSegment(placement))
                    if placement.hot_replica_nodes.is_empty() =>
                {
                    Some(placement)
                }
                _ => None,
            };

            let data = match packed_source {
                Some(placement) => {
                    self.read_packed_object_from_shards(&entry.object, &placement).await?
                }
                None => {
                    if entry.object.version_id.is_some() {
                        return Err(EcTranscodeError::InvalidSource {
                            reason: "version-specific hot source reads require a version-aware \
                                     cluster reader"
                                .to_string(),
                        });
                    }
                    self.read_coordinator
                        .read(&entry.object.bucket, &entry.object.key)
                        .await
                        .map_err(|error| EcTranscodeError::ShardIo {
                            reason: format!(
                                "hot RF=3 read of {}/{} failed while gathering segment \
                                 {segment_id}: {error}",
                                entry.object.bucket, entry.object.key
                            ),
                        })?
                        .data
                }
            };
            parts.push((entry.object.clone(), data));
        }

        EcSourceObject::for_packed_segment(&segment, &parts, self.local_node_id, now)
    }

    /// Read one packed object out of the shards of the segment it still lives in.
    ///
    /// The read a repack depends on. Its object points at the segment being
    /// repacked, which is purged: the shards are the only copy, and this is the
    /// path that assembles bytes from them.
    async fn read_packed_object_from_shards(
        &self,
        object: &EcObjectVersionKey,
        placement: &PackedSegmentObjectPlacement,
    ) -> Result<Vec<u8>, EcTranscodeError> {
        let erasure_set = self
            .sets
            .current()
            .resolve_manifest_set(placement.erasure_set_id)
            .cloned()
            .ok_or_else(|| EcTranscodeError::InvalidSource {
                reason: format!(
                    "segment {} names erasure set {}, which this node does not have",
                    placement.segment_id, placement.erasure_set_id
                ),
            })?;
        let ports = self.packed_segment_ports(&erasure_set);
        ports
            .reader
            .read_packed_object_bytes(object.clone(), placement, &ports, &NoopEcReadRepairQueue)
            .await
            .map(|body| body.to_vec())
            .map_err(|error| EcTranscodeError::ShardIo {
                reason: format!(
                    "reading {}/{} out of segment {} failed: {error}",
                    object.bucket, object.key, placement.segment_id
                ),
            })
    }

    /// The ports every packed-segment operation of one erasure set shares.
    fn packed_segment_ports(&self, erasure_set: &ErasureSet) -> PackedSegmentReleasePorts {
        PackedSegmentReleasePorts {
            reader: EcReadCoordinator::new(
                Arc::clone(&self.metadata),
                self.staged_worker.codec_limiter(),
            )
            .with_minimal_read(self.minimal_read),
            ports: self.ports_for(erasure_set),
        }
    }

    /// Cluster ports bound to one erasure set.
    ///
    /// Shard writes, manifest quorum and manifest validation are all
    /// set-specific, so a job encoding into a second set needs ports pointing
    /// at that set's nodes. Rebuilding them costs a short node-id vector and a
    /// few `Arc` clones — nothing beside the encoding that follows — and the
    /// pool's default set, which is every job in a pool of one set, is handed
    /// back without rebuilding anything.
    fn ports_for(&self, set: &ErasureSet) -> EcClusterTranscodePorts {
        self.ports.rebuilt(self.sets.current(), set.clone())
    }

    /// Ports on the current roster, bound to the pool's default set.
    ///
    /// Used by the paths that have to read a manifest before they know which
    /// set it belongs to.
    fn ports(&self) -> EcClusterTranscodePorts {
        self.ports.rebuilt(self.sets.current(), self.ports.erasure_set.clone())
    }

    async fn find_eligible_jobs(
        &self,
        now: u64,
        max_jobs: usize,
    ) -> Result<Vec<EcTranscodeJobRecord>, EcTranscodeError> {
        let mut selected = Vec::new();
        let mut cursor = None;
        let page_size = self.scan_limit.max(1).min(self.worker_config.batch_size.max(1));
        // One roster for the whole scan, so a set added halfway through does not
        // make the pass answer two different ways about the same job.
        let sets = self.sets.current();

        loop {
            let page = self.metadata.list_transcode_jobs_page(cursor, page_size).await?;
            cursor = page.next_cursor;

            for job in page.entries {
                // A job for a set this pool does not have belongs to another
                // pool's keyspace or is corrupt; either way this worker has no
                // slots to write its shards to. A job for a set held until this
                // node reads its layout record waits, queued, until it is read.
                if !matches!(
                    job_target(&sets, job.target_erasure_set_id),
                    JobTarget::Ready(_)
                ) {
                    continue;
                }
                if self.job_is_eligible(&job, now).await? {
                    selected.push(job);
                    if selected.len() >= max_jobs {
                        return Ok(selected);
                    }
                }
            }

            if cursor.is_none() {
                break;
            }
        }

        Ok(selected)
    }

    async fn job_is_eligible(
        &self,
        job: &EcTranscodeJobRecord,
        now: u64,
    ) -> Result<bool, EcTranscodeError> {
        Ok(match job.state {
            EcPlacementState::EcTranscodeQueued => true,
            EcPlacementState::EcFailedRetryable => {
                job.next_retry_at.map(|retry_at| retry_at <= now).unwrap_or(true)
            }
            EcPlacementState::EcEncoding
            | EcPlacementState::EcShardsDurable
            | EcPlacementState::EcManifestCommitted
            | EcPlacementState::EcReadVerified => self.lease_is_recoverable(job, now).await?,
            EcPlacementState::HotReplicated
            | EcPlacementState::EcCommitted
            | EcPlacementState::HotReplicasGcEligible
            | EcPlacementState::HotReplicasPurged
            | EcPlacementState::EcFailedPermanent
            | EcPlacementState::Promoting
            | EcPlacementState::Promoted
            | EcPlacementState::Tombstone => false,
        })
    }

    async fn lease_is_recoverable(
        &self,
        job: &EcTranscodeJobRecord,
        now: u64,
    ) -> Result<bool, EcTranscodeError> {
        let Some(lease) = self.metadata.get_transcode_lease(job.operation_id).await? else {
            return Ok(true);
        };
        Ok(!lease.is_alive_at(now))
    }

    async fn process_job(&self, job: EcTranscodeJobRecord) -> Result<(), EcTranscodeError> {
        let now = unix_time_nanos();

        // The set was chosen when the job was admitted and is read back here
        // rather than chosen again: a retry has to encode into the same set as
        // the first attempt, or one generation would end up with two manifests
        // naming different sets (EC2-042). Resolved before anything expensive
        // happens — reading a whole object for a job with nowhere to go would
        // be work thrown away.
        let sets = self.sets.current();
        let erasure_set = match job_target(&sets, job.target_erasure_set_id) {
            JobTarget::Ready(set) => set.clone(),
            // `find_eligible_jobs` filters both of these out, so reaching them
            // means the roster changed between the scan and now.
            JobTarget::Held => {
                tracing::debug!(
                    bucket = %job.object.bucket,
                    key = %job.object.key,
                    erasure_set_id = job.target_erasure_set_id,
                    "EC transcode job waits until its set's layout record is read"
                );
                return Ok(());
            }
            JobTarget::Unknown => {
                // Nothing to encode into, and nothing this worker can fix.
                tracing::warn!(
                    bucket = %job.object.bucket,
                    key = %job.object.key,
                    erasure_set_id = job.target_erasure_set_id,
                    "skipping an EC transcode job for a set this pool does not have"
                );
                return Ok(());
            }
        };

        if job.object_size > self.worker_config.max_source_read_bytes {
            let reason = format!(
                "source object exceeds S4_EC_TRANSCODE_MAX_SOURCE_BYTES ({} > {})",
                job.object_size, self.worker_config.max_source_read_bytes
            );
            self.record_runtime_failure(
                job.clone(),
                EcTranscodeFailureClass::Retryable,
                reason.clone(),
                now,
            )
            .await?;
            self.record_local_orphan_candidates(&job, reason).await?;
            return Ok(());
        }

        let source = match self.read_hot_source(&job).await {
            Ok(source) => source,
            Err(error) => {
                let class = runtime_failure_class(&error);
                let reason = error.to_string();
                // Recorded on the job and retried, which without a line here is
                // a job that fails forever in complete silence.
                tracing::warn!(
                    bucket = %job.object.bucket,
                    key = %job.object.key,
                    attempt = job.attempt,
                    class = ?class,
                    %reason,
                    "EC transcode could not read its source"
                );
                self.record_runtime_failure(job.clone(), class, reason.clone(), now).await?;
                self.record_local_orphan_candidates(&job, reason).await?;
                return Ok(());
            }
        };

        if source.source_hlc != job.source_hlc {
            let class = if source.source_hlc.is_zero() {
                EcTranscodeFailureClass::Retryable
            } else {
                EcTranscodeFailureClass::Permanent
            };
            self.record_runtime_failure(
                job.clone(),
                class,
                "hot RF=3 source version changed before EC transcode".to_string(),
                now,
            )
            .await?;
            self.record_local_orphan_candidates(
                &job,
                "hot RF=3 source version changed before EC transcode".to_string(),
            )
            .await?;
            return Ok(());
        }

        let ports = self.ports_for(&erasure_set);

        let job_origin = job.origin;
        let job_target = job.target;
        let request = EcTranscodeRequest {
            source,
            erasure_set,
            owner_node_id: self.local_node_id,
            now_nanos: now,
            origin: job_origin,
            target: job_target,
        };

        let outcome = match self.staged_worker.transcode(request, &ports, &ports, &ports).await {
            Ok(outcome) => outcome,
            Err(error) => {
                self.record_local_orphan_candidates(&job, error.to_string()).await?;
                return Err(error);
            }
        };

        match outcome {
            EcTranscodeOutcome::Committed { operation_id, .. } => {
                tracing::info!(operation_id = %operation_id, "EC transcode committed");
                // Only a migration counts as a layout change. A client write
                // encoded on its way in never had another layout to leave, and
                // counting it would make the pause between migrations fire on
                // objects that have never migrated at all.
                if job_origin.is_tiering() {
                    if let Err(error) = heat::record_migration(
                        &self.metadata,
                        &job.object,
                        now,
                        self.promote_window_nanos,
                    )
                    .await
                    {
                        // The object is erasure coded either way. Losing the
                        // mark only means the pause before it may move again is
                        // shorter than intended.
                        tracing::warn!(
                            bucket = %job.object.bucket,
                            key = %job.object.key,
                            error = %error,
                            "EC migration could not be recorded against the activity mark"
                        );
                    }
                }
            }
            EcTranscodeOutcome::PackedSegmentEncoded {
                operation_id,
                ref manifest,
            } => {
                tracing::info!(
                    operation_id = %operation_id,
                    segment = %manifest.key,
                    chunks = manifest.chunks.len(),
                    "EC packed segment encoded; its objects still read from their hot copies"
                );
            }
            EcTranscodeOutcome::SkippedSmall { object, size, .. } => {
                tracing::debug!(
                    bucket = %object.bucket,
                    key = %object.key,
                    size,
                    "EC transcode skipped replicated-small object"
                );
            }
            EcTranscodeOutcome::Superseded { operation_id } => {
                tracing::debug!(operation_id = %operation_id, "EC transcode superseded");
                // A client wrote while this ran. For a client-write job that is
                // ordinary — the newer generation has its own job. For a
                // migration it is work the node chose to do and then had to
                // throw away, which is what the counter is for.
                if job_origin.is_tiering() {
                    ec_metrics::record_tiering_cancelled();
                }
            }
        }

        Ok(())
    }

    async fn process_hot_replica_gc(
        &self,
        now: u64,
        max_records: usize,
    ) -> Result<usize, EcTranscodeError> {
        let mut processed = 0usize;
        let mut cursor = None;
        let page_size = self.scan_limit.max(1);
        // One roster and one set of ports for the whole cycle: rebuilding them
        // per record would allocate a node vector for every hot copy examined.
        let sets = self.sets.current();
        let ports = self.ports.rebuilt(Arc::clone(&sets), self.ports.erasure_set.clone());

        loop {
            let page = self.metadata.list_hot_replica_gc_page(cursor, page_size).await?;
            for record in page.entries {
                if record.source_hlc.is_zero() {
                    tracing::warn!(
                        bucket = %record.object.bucket,
                        key = %record.object.key,
                        operation_id = %record.operation_id,
                        "skipping hot-replica GC record without source generation"
                    );
                    continue;
                }
                let manifest = match ports.quorum_read_manifest(&record.object).await? {
                    EcManifestReadResult::Present(manifest)
                        if manifest.operation_id == record.operation_id =>
                    {
                        manifest
                    }
                    EcManifestReadResult::Present(_) => continue,
                    EcManifestReadResult::Tombstone(_) | EcManifestReadResult::Absent => continue,
                };

                // Bound to the set that holds this object, because the purge
                // stamps the set id onto the namespace entry it detaches. The
                // default set would be the wrong stamp for an object placed in
                // any other one.
                let Some(erasure_set) = sets.resolve_manifest_set(manifest.erasure_set_id).cloned()
                else {
                    tracing::warn!(
                        bucket = %record.object.bucket,
                        key = %record.object.key,
                        erasure_set_id = manifest.erasure_set_id,
                        "skipping hot-replica GC for a set this pool does not have"
                    );
                    continue;
                };
                let set_ports = ports.rebuilt(Arc::clone(&sets), erasure_set.clone());

                let decision = self
                    .staged_worker
                    .maybe_purge_hot_replicas(
                        record,
                        manifest.state,
                        erasure_set.set_id,
                        now,
                        &set_ports,
                    )
                    .await?;
                match decision {
                    EcHotReplicaPurgeDecision::Purged => {
                        self.mark_hot_replica_purge_committed(manifest, now).await?;
                        processed = processed.saturating_add(1);
                        if processed >= max_records.max(1) {
                            return Ok(processed);
                        }
                    }
                    EcHotReplicaPurgeDecision::AlreadyPurged => {
                        self.mark_hot_replica_purge_committed(manifest, now).await?;
                    }
                    EcHotReplicaPurgeDecision::ManualMode
                    | EcHotReplicaPurgeDecision::GracePeriod
                    | EcHotReplicaPurgeDecision::NeedsFullVerification
                    | EcHotReplicaPurgeDecision::NotCommitted => {}
                }
            }

            match page.next_cursor {
                Some(next_cursor) => cursor = Some(next_cursor),
                None => break,
            }
        }

        Ok(processed)
    }

    async fn mark_hot_replica_purge_committed(
        &self,
        mut manifest: EcObjectManifest,
        now: u64,
    ) -> Result<(), EcTranscodeError> {
        if manifest.state != EcPlacementState::HotReplicasPurged {
            manifest.state = EcPlacementState::HotReplicasPurged;
            let required_acks = manifest.profile.manifest_quorum();
            let commit = self.ports().commit_manifest(manifest.clone(), required_acks).await?;
            if commit.acks < required_acks {
                ec_metrics::record_manifest_quorum_failure(manifest.erasure_set_id);
                return Err(EcTranscodeError::ManifestQuorum {
                    reason: format!(
                        "hot-replica purge marker requires {required_acks} ACK(s), got {}",
                        commit.acks
                    ),
                });
            }
            self.metadata.put_manifest(&manifest).await?;
        }

        if let Some(mut job) = self.metadata.get_transcode_job(manifest.operation_id).await? {
            if job.state != EcPlacementState::HotReplicasPurged {
                job.state = EcPlacementState::HotReplicasPurged;
                job.updated_at = now;
                job.next_retry_at = None;
                job.failure = None;
                self.metadata.put_transcode_job(&job).await?;
            }
        }
        Ok(())
    }

    async fn read_hot_source(
        &self,
        job: &EcTranscodeJobRecord,
    ) -> Result<EcSourceObject, EcTranscodeError> {
        if job.target.is_packed_segment() {
            return self.read_packed_segment_source(job, unix_time_nanos()).await;
        }
        if job.object.version_id.is_some() {
            return Err(EcTranscodeError::InvalidSource {
                reason: "version-specific hot source reads require a version-aware cluster reader"
                    .to_string(),
            });
        }

        let result =
            self.read_coordinator.read(&job.object.bucket, &job.object.key).await.map_err(
                |error| EcTranscodeError::ShardIo {
                    reason: format!("hot RF=3 source read failed: {error}"),
                },
            )?;
        if result.size != job.object_size {
            return Err(EcTranscodeError::InvalidSource {
                reason: format!(
                    "hot RF=3 source size changed from {} to {}",
                    job.object_size, result.size
                ),
            });
        }
        let actual_hash = sha256(&result.data);
        if actual_hash != job.source_content_hash_sha256 {
            return Err(EcTranscodeError::SourceHashMismatch);
        }

        let user_metadata = result
            .metadata
            .into_iter()
            .filter(|(key, _)| !key.starts_with("_s4_"))
            .collect::<HashMap<_, _>>();

        Ok(EcSourceObject {
            object: job.object.clone(),
            data: result.data,
            content_hash_sha256: actual_hash,
            etag: result.etag,
            content_type: result.content_type,
            user_metadata,
            source_hlc: result.hlc,
            origin_node_id: NodeId(result.hlc.node_id),
            created_at: result.hlc.wall_time.saturating_mul(1_000_000),
        })
    }

    async fn record_runtime_failure(
        &self,
        mut job: EcTranscodeJobRecord,
        class: EcTranscodeFailureClass,
        reason: String,
        now: u64,
    ) -> Result<(), EcTranscodeError> {
        let attempt = job.attempt.saturating_add(1);
        let exhausted = class == EcTranscodeFailureClass::Retryable
            && attempt >= self.transcode_config.max_retry_attempts;
        let final_class = if exhausted {
            EcTranscodeFailureClass::Permanent
        } else {
            class
        };
        job.attempt = attempt;
        job.updated_at = now;
        job.state = match final_class {
            EcTranscodeFailureClass::Retryable => EcPlacementState::EcFailedRetryable,
            EcTranscodeFailureClass::Permanent => EcPlacementState::EcFailedPermanent,
        };
        job.next_retry_at =
            if final_class == EcTranscodeFailureClass::Retryable {
                Some(now.saturating_add(
                    self.transcode_config.retry_delay_nanos(attempt, job.operation_id),
                ))
            } else {
                None
            };
        job.failure = Some(EcTranscodeFailureRecord {
            class: final_class,
            reason: if exhausted {
                format!("retry budget exhausted after {attempt} attempt(s): {reason}")
            } else {
                reason
            },
        });
        self.metadata.put_transcode_job(&job).await?;
        Ok(())
    }

    /// Record the shards a failed transcode left behind as orphan candidates.
    ///
    /// The timestamp is taken here, when the shards are seen to be orphaned,
    /// and deliberately not passed in by the caller. Orphan GC retains a
    /// candidate for `orphan_min_age_secs` after it is observed, which is what
    /// keeps it from collecting the shards of a transcode that is merely slow.
    /// Callers hold the time their job *started*, and a throttled transcode can
    /// run for longer than that window — stamping a record with the start time
    /// would hand GC a candidate that is already past its grace the moment it
    /// is written, and the protection would exist only on paper.
    async fn record_local_orphan_candidates(
        &self,
        job: &EcTranscodeJobRecord,
        reason: String,
    ) -> Result<(), EcTranscodeError> {
        let now = unix_time_nanos();
        let local_shards =
            self.metadata.list_local_shards_for_object(&job.object, self.scan_limit).await?;
        for (key, record) in local_shards {
            if record.operation_id != job.operation_id {
                continue;
            }
            self.gc
                .record_orphan_shard(EcOrphanShardObservation {
                    key,
                    record,
                    target_erasure_set_id: job.target_erasure_set_id,
                    observed_at: now,
                    reason: format!("transcode failed before EC cutover: {reason}"),
                })
                .await
                .map_err(|error| {
                    EcTranscodeError::Metadata(EcMetadataError::QuorumRead(error.to_string()))
                })?;
        }
        Ok(())
    }
}

#[derive(Clone)]
struct EcClusterTranscodePorts {
    local_node_id: NodeId,
    topology_epoch: u64,
    /// The pool's sets as they were when these ports were built.
    ///
    /// A snapshot on purpose: one operation must see one roster from start to
    /// finish. The worker rebuilds the ports from the live roster for each piece
    /// of work, so a set added meanwhile is picked up on the next one.
    sets: Arc<ErasureSetRegistry>,
    erasure_set: ErasureSet,
    metadata: Arc<EcMetadataStore>,
    read_coordinator: Arc<QuorumReadCoordinator>,
    write_coordinator: Arc<QuorumWriteCoordinator>,
    storage: Arc<dyn StorageEngine>,
    rpc_client: Arc<NodeClient>,
    metadata_nodes: Vec<NodeId>,
    /// Metadata nodes of every set of the pool, by set id, in registry order.
    pool_metadata_nodes: Vec<(u64, Vec<NodeId>)>,
}

struct EcClusterTranscodePortsDeps {
    local_node_id: NodeId,
    topology_epoch: u64,
    sets: Arc<ErasureSetRegistry>,
    erasure_set: ErasureSet,
    metadata: Arc<EcMetadataStore>,
    read_coordinator: Arc<QuorumReadCoordinator>,
    write_coordinator: Arc<QuorumWriteCoordinator>,
    storage: Arc<dyn StorageEngine>,
    rpc_client: Arc<NodeClient>,
}

fn transcode_set_metadata_nodes(erasure_set: &ErasureSet) -> Vec<NodeId> {
    let mut nodes = erasure_set
        .slot_assignment
        .slots()
        .iter()
        .map(|slot| slot.node_id)
        .collect::<Vec<_>>();
    nodes.sort_unstable_by_key(|node| node.0);
    nodes.dedup();
    nodes
}

struct EcHiddenObjectWrite {
    node_id: NodeId,
    key: String,
    data: Vec<u8>,
    content_type: &'static str,
    metadata: HashMap<String, String>,
    hlc: HlcTimestamp,
    operation_id: u128,
}

impl EcClusterTranscodePorts {
    /// The same ports, on the given roster and bound to the given set.
    fn rebuilt(&self, sets: Arc<ErasureSetRegistry>, erasure_set: ErasureSet) -> Self {
        Self::new(EcClusterTranscodePortsDeps {
            local_node_id: self.local_node_id,
            topology_epoch: self.topology_epoch,
            sets,
            erasure_set,
            metadata: Arc::clone(&self.metadata),
            read_coordinator: Arc::clone(&self.read_coordinator),
            write_coordinator: Arc::clone(&self.write_coordinator),
            storage: Arc::clone(&self.storage),
            rpc_client: Arc::clone(&self.rpc_client),
        })
    }

    fn new(deps: EcClusterTranscodePortsDeps) -> Self {
        let metadata_nodes = transcode_set_metadata_nodes(&deps.erasure_set);
        let pool_metadata_nodes = deps
            .sets
            .sets()
            .into_iter()
            .map(|set| (set.set_id, transcode_set_metadata_nodes(set)))
            .collect::<Vec<_>>();

        Self {
            local_node_id: deps.local_node_id,
            topology_epoch: deps.topology_epoch,
            sets: deps.sets,
            erasure_set: deps.erasure_set,
            metadata: deps.metadata,
            read_coordinator: deps.read_coordinator,
            write_coordinator: deps.write_coordinator,
            storage: deps.storage,
            rpc_client: deps.rpc_client,
            metadata_nodes,
            pool_metadata_nodes,
        }
    }

    /// The set a manifest belongs to, refusing one this pool never issued.
    fn set_for_manifest_id(&self, manifest_set_id: u64) -> Result<&ErasureSet, EcTranscodeError> {
        self.sets.resolve_manifest_set(manifest_set_id).ok_or_else(|| {
            EcTranscodeError::ManifestQuorum {
                reason: format!(
                    "manifest names erasure set {manifest_set_id}, which pool {} does not have",
                    self.sets.pool_id()
                ),
            }
        })
    }

    /// Whether every set of the pool answered "no such manifest" at quorum.
    fn every_set_reports_absent(&self, absent: &HashMap<u64, u8>, required: u8) -> bool {
        // An empty pool has reached nothing. `all` over an empty list is true,
        // which would report every object as absent on a node whose pool is not
        // configured.
        !self.pool_metadata_nodes.is_empty()
            && self
                .pool_metadata_nodes
                .iter()
                .all(|(set_id, _)| absent.get(set_id).copied().unwrap_or(0) >= required)
    }

    async fn write_object_to_node(
        &self,
        write: EcHiddenObjectWrite,
    ) -> Result<Option<IndexRecord>, EcTranscodeError> {
        if write.node_id == self.local_node_id {
            let record = write_local_object(
                Arc::clone(&self.storage),
                write.key,
                write.data,
                write.content_type,
                write.metadata,
                write.hlc,
                write.operation_id,
            )
            .await?;
            return Ok(Some(record));
        }

        write_remote_object(
            Arc::clone(&self.rpc_client),
            self.local_node_id,
            self.topology_epoch,
            write,
        )
        .await?;
        Ok(None)
    }

    async fn read_object_from_node(
        &self,
        node_id: NodeId,
        key: String,
        operation_id: u128,
    ) -> Result<Vec<u8>, EcTranscodeError> {
        if node_id == self.local_node_id {
            return read_local_object(Arc::clone(&self.storage), key).await;
        }

        read_remote_object(
            Arc::clone(&self.rpc_client),
            self.local_node_id,
            node_id,
            self.topology_epoch,
            key,
            operation_id,
        )
        .await
    }

    fn ensure_manifest_quorum_size(&self, required_acks: u8) -> Result<(), EcTranscodeError> {
        if usize::from(required_acks) > self.metadata_nodes.len() {
            return Err(EcTranscodeError::ManifestQuorum {
                reason: format!(
                    "manifest quorum requires {required_acks} node(s), only {} available",
                    self.metadata_nodes.len()
                ),
            });
        }
        Ok(())
    }
}

#[async_trait]
impl EcShardStore for EcClusterTranscodePorts {
    async fn write_shard(
        &self,
        request: EcShardWriteRequest,
    ) -> Result<EcShardWriteAck, EcTranscodeError> {
        let actual_crc32 = crc32fast::hash(&request.data);
        let actual_sha256 = sha256(&request.data);
        if actual_crc32 != request.crc32 || actual_sha256 != request.sha256 {
            return Err(EcTranscodeError::ShardVerification {
                reason: format!(
                    "encoded shard chunk {} slot {} does not match requested hash",
                    request.chunk_index, request.slot_index
                ),
            });
        }

        let size = u64::try_from(request.data.len()).map_err(|_| EcTranscodeError::ShardIo {
            reason: "shard size does not fit in u64".to_string(),
        })?;
        let key = shard_object_key(
            &request.object,
            request.operation_id,
            request.chunk_index,
            request.slot_index,
        );
        let operation_id = derive_storage_operation_id(
            request.operation_id,
            b"ec-shard-write",
            request.chunk_index,
            request.slot_index,
            EcPlacementState::EcShardsDurable,
        );
        let metadata = shard_metadata(&request);
        let local_record = self
            .write_object_to_node(EcHiddenObjectWrite {
                node_id: request.node_id,
                key,
                data: request.data,
                content_type: EC_SHARD_CONTENT_TYPE,
                metadata,
                hlc: HlcTimestamp::from_record(unix_time_millis(), 0, self.local_node_id.0),
                operation_id,
            })
            .await?;

        if let Some(record) = local_record {
            let key = LocalShardKey {
                object: request.object.clone(),
                chunk_index: request.chunk_index,
                slot_index: request.slot_index,
            };
            let record = LocalShardRecord {
                volume_id: record.file_id,
                offset: record.offset,
                size,
                crc32: request.crc32,
                sha256: request.sha256,
                state: LocalShardState::Durable,
                operation_id: request.operation_id,
            };
            self.metadata.put_local_shard(&key, &record).await?;
        }

        Ok(EcShardWriteAck {
            node_id: request.node_id,
            chunk_index: request.chunk_index,
            slot_index: request.slot_index,
            size,
            crc32: request.crc32,
            sha256: request.sha256,
        })
    }

    async fn read_shard(&self, request: EcShardReadRequest) -> Result<Vec<u8>, EcTranscodeError> {
        let key = shard_object_key(
            &request.object,
            request.operation_id,
            request.chunk_index,
            request.slot_index,
        );
        let operation_id = derive_storage_operation_id(
            request.operation_id,
            b"ec-shard-read",
            request.chunk_index,
            request.slot_index,
            EcPlacementState::EcReadVerified,
        );
        self.read_object_from_node(request.node_id, key, operation_id).await
    }
}

#[async_trait]
impl EcManifestQuorum for EcClusterTranscodePorts {
    async fn commit_manifest(
        &self,
        manifest: EcObjectManifest,
        required_acks: u8,
    ) -> Result<EcManifestCommitResult, EcTranscodeError> {
        // Written to the nodes of the set the manifest names, not to the ones
        // this handle happens to be bound to.
        let bound = self.rebuilt(
            Arc::clone(&self.sets),
            self.set_for_manifest_id(manifest.erasure_set_id)?.clone(),
        );
        bound.ensure_manifest_quorum_size(required_acks)?;
        manifest
            .validate_against_erasure_set(&bound.erasure_set)
            .map_err(EcTranscodeError::Metadata)?;

        let bytes =
            encode_manifest(&manifest).map_err(|error| EcTranscodeError::ManifestQuorum {
                reason: format!("manifest serialization failed: {error}"),
            })?;
        let key = manifest_object_key(&manifest.object_key());
        let metadata = manifest_metadata(&manifest)?;
        let mut set = JoinSet::new();

        for node_id in bound.metadata_nodes.clone() {
            let ports = bound.clone();
            let key = key.clone();
            let data = bytes.clone();
            let metadata = metadata.clone();
            let operation_id = derive_storage_operation_id(
                manifest.operation_id,
                b"ec-manifest-write",
                0,
                0,
                manifest.state,
            );
            let hlc = manifest_storage_hlc(manifest.hlc, manifest.state);
            set.spawn(async move {
                ports
                    .write_object_to_node(EcHiddenObjectWrite {
                        node_id,
                        key,
                        data,
                        content_type: EC_MANIFEST_CONTENT_TYPE,
                        metadata,
                        hlc,
                        operation_id,
                    })
                    .await
                    .map(|_| node_id)
            });
        }

        let mut acks = 0u8;
        while let Some(result) = set.join_next().await {
            match result {
                Ok(Ok(node_id)) => {
                    acks = acks.saturating_add(1);
                    tracing::debug!(node_id = %node_id, state = ?manifest.state, "EC manifest ACK");
                }
                Ok(Err(error)) => {
                    tracing::warn!(error = %error, state = ?manifest.state, "EC manifest write failed");
                }
                Err(error) => {
                    tracing::warn!(error = %error, state = ?manifest.state, "EC manifest write task failed");
                }
            }
        }

        Ok(EcManifestCommitResult { acks })
    }

    async fn quorum_read_manifest(
        &self,
        object: &EcObjectVersionKey,
    ) -> Result<EcManifestReadResult, EcTranscodeError> {
        let required = self.erasure_set.profile.manifest_quorum();
        for (set_id, nodes) in &self.pool_metadata_nodes {
            if usize::from(required) > nodes.len() {
                return Err(EcTranscodeError::ManifestQuorum {
                    reason: format!(
                        "manifest quorum requires {required} node(s), erasure set {set_id} \
                         only has {}",
                        nodes.len()
                    ),
                });
            }
        }
        let key = manifest_object_key(object);
        let operation_id = derive_storage_operation_id(
            deterministic_object_uuid(object),
            b"ec-manifest-read",
            0,
            0,
            EcPlacementState::EcManifestCommitted,
        );
        let mut set = JoinSet::new();

        // Over every set of the pool: the object's set is written in the
        // manifest, so it cannot be known before the manifest is in hand.
        for (set_id, nodes) in self.pool_metadata_nodes.clone() {
            for node_id in nodes {
                let ports = self.clone();
                let key = key.clone();
                let object = object.clone();
                set.spawn(async move {
                    (
                        set_id,
                        read_manifest_from_node(ports, node_id, key, object, operation_id).await,
                    )
                });
            }
        }

        let mut absent_by_set: HashMap<u64, u8> = HashMap::new();
        let mut by_hash: BTreeMap<[u8; 32], (EcObjectManifest, u8)> = BTreeMap::new();
        while let Some(result) = set.join_next().await {
            match result {
                Ok((_, Ok(ManifestNodeRead::Present(manifest)))) => {
                    let manifest = *manifest;
                    let hash = manifest.serialized_sha256()?;
                    by_hash
                        .entry(hash)
                        .and_modify(|(_, count)| *count = count.saturating_add(1))
                        .or_insert((manifest, 1));
                }
                Ok((set_id, Ok(ManifestNodeRead::Absent))) => {
                    let absent = absent_by_set.entry(set_id).or_insert(0);
                    *absent = absent.saturating_add(1);
                }
                Ok((_, Err(error))) => {
                    tracing::warn!(error = %error, "EC manifest read failed");
                }
                Err(error) => {
                    tracing::warn!(error = %error, "EC manifest read task failed");
                }
            }
        }

        let mut winner = None;
        for (_, (manifest, count)) in by_hash {
            if count < required {
                continue;
            }
            winner = match winner {
                None => Some(manifest),
                Some(current) if manifest_wins(&manifest, &current) => Some(manifest),
                Some(current) => Some(current),
            };
        }

        if let Some(manifest) = winner {
            return Ok(manifest.into_read_result());
        }
        if self.every_set_reports_absent(&absent_by_set, required) {
            return Ok(EcManifestReadResult::Absent);
        }

        Err(EcTranscodeError::ManifestQuorum {
            reason: format!("manifest quorum read did not reach {required} matching response(s)"),
        })
    }
}

#[async_trait]
impl EcObjectCutover for EcClusterTranscodePorts {
    async fn cutover_to_ec(
        &self,
        guard: EcCutoverGuard,
        manifest_ref: EcObjectManifestRef,
    ) -> Result<EcCutoverOutcome, EcTranscodeError> {
        let object = manifest_ref.object.clone();
        let head = match self.read_coordinator.head(&object.bucket, &object.key).await {
            Ok(head) => head,
            Err(ClusterError::ObjectNotFound { .. }) => return Ok(EcCutoverOutcome::Superseded),
            Err(error) => {
                return Err(EcTranscodeError::ShardIo {
                    reason: format!("hot RF=3 source HEAD before cutover failed: {error}"),
                });
            }
        };
        if let Some(reason) = cutover_guard_broken(&guard, &head) {
            tracing::debug!(
                bucket = %object.bucket,
                key = %object.key,
                reason,
                "EC cutover refused: the object is no longer what was encoded"
            );
            return Ok(EcCutoverOutcome::Superseded);
        }

        self.metadata
            .put_object_layout(
                &object,
                &ObjectDataLayout::ErasureCoded(manifest_ref.clone()),
            )
            .await?;

        // The same check again, after the switch. A write that landed between
        // the two would otherwise leave the layout pointing at shards of a
        // generation the pool has already replaced.
        let after = match self.read_coordinator.head(&object.bucket, &object.key).await {
            Ok(head) => head,
            Err(ClusterError::ObjectNotFound { .. }) => return Ok(EcCutoverOutcome::Superseded),
            Err(error) => {
                return Err(EcTranscodeError::ShardIo {
                    reason: format!("hot RF=3 source HEAD after cutover failed: {error}"),
                });
            }
        };
        if let Some(reason) = cutover_guard_broken(&guard, &after) {
            tracing::debug!(
                bucket = %object.bucket,
                key = %object.key,
                reason,
                "EC cutover rolled back: the object changed while the layout was switched"
            );
            if layout_is_manifest(&self.metadata, &object, manifest_ref.operation_id).await? {
                self.metadata.delete_object_layout(&object).await?;
            }
            return Ok(EcCutoverOutcome::Superseded);
        }

        Ok(EcCutoverOutcome::Committed)
    }

    async fn mark_hot_replicas_gc_eligible(
        &self,
        record: EcHotReplicaGcRecord,
    ) -> Result<(), EcTranscodeError> {
        self.metadata.put_hot_replica_gc(&record).await?;
        Ok(())
    }

    async fn purge_hot_replicas(
        &self,
        object: &EcObjectVersionKey,
        expected_source_hlc: HlcTimestamp,
        operation_id: Uuid,
    ) -> Result<(), EcTranscodeError> {
        if object.version_id.is_some() {
            return Err(EcTranscodeError::ShardIo {
                reason:
                    "version-specific hot RF=3 purge requires a version-aware write coordinator"
                        .to_string(),
            });
        }

        // Releases the hot copies but keeps the object in the namespace, now
        // marked as erasure coded. Deleting it instead would reclaim the same
        // space and make the object vanish from every listing, lifecycle scan
        // and bucket statistic, while GET and HEAD kept working — an object
        // that exists but cannot be found.
        self.write_coordinator
            .detach_object_data_if_hlc(
                &object.bucket,
                &object.key,
                expected_source_hlc,
                self.erasure_set.set_id,
                self.erasure_set.topology_epoch,
            )
            .await
            .map(|_| ())
            .map_err(|error| EcTranscodeError::ShardIo {
                reason: format!(
                    "hot RF=3 physical purge for {}/{} operation {} failed: {error}",
                    object.bucket, object.key, operation_id
                ),
            })
    }
}

/// The ports a packed-segment release needs, over the ones the worker has.
///
/// Two small adaptations, and nothing more: the EC read path asks for shards
/// through a different trait than the transcode path writes them through, and
/// releasing hot copies is the same detach the single-object purge performs.
#[derive(Clone)]
struct PackedSegmentReleasePorts {
    reader: EcReadCoordinator,
    ports: EcClusterTranscodePorts,
}

#[async_trait]
impl EcShardReader for PackedSegmentReleasePorts {
    async fn read_shard(
        &self,
        request: EcReadShardRequest,
    ) -> Result<Option<EcReadShardResponse>, EcReadError> {
        let read = EcShardStore::read_shard(
            &self.ports,
            EcShardReadRequest {
                object: request.object,
                operation_id: request.operation_id,
                chunk_index: request.chunk_index,
                slot_index: request.slot_index,
                node_id: request.node_id,
            },
        )
        .await;
        match read {
            Ok(data) => Ok(Some(EcReadShardResponse {
                replica_crc32: Some(crc32fast::hash(&data)),
                data: Bytes::from(data),
            })),
            // A shard this node cannot fetch is an unavailable candidate, not a
            // failed probe: the read path decides whether the rest is enough.
            Err(error) => {
                tracing::debug!(%error, "packed segment probe could not fetch a shard");
                Ok(None)
            }
        }
    }
}

#[async_trait]
impl EcPackedSegmentProbe for PackedSegmentReleasePorts {
    async fn probe_packed_object(
        &self,
        segment_id: PackedSegmentId,
        entry: &PackedSegmentEntry,
    ) -> Result<EcPackedProbeOutcome, EcGcError> {
        let placement = PackedSegmentObjectPlacement::new(
            segment_id,
            self.ports.erasure_set.set_id,
            entry.offset,
            entry.length,
            entry.content_hash_sha256,
            entry.hlc,
            Vec::new(),
        )
        .map_err(EcGcError::Metadata)?;

        let report = self
            .reader
            .probe_packed_object(
                entry.object.clone(),
                &placement,
                self,
                &NoopEcReadRepairQueue,
            )
            .await
            .map_err(|error| EcGcError::InvalidRequest {
                reason: format!(
                    "{}/{} is not yet readable from segment {segment_id}: {error}",
                    entry.object.bucket, entry.object.key
                ),
            })?;

        Ok(EcPackedProbeOutcome {
            degraded: report.degraded_chunks > 0,
        })
    }
}

#[async_trait]
impl EcTombstoneQuorum for PackedSegmentReleasePorts {
    async fn quorum_read_manifest(
        &self,
        object: &EcObjectVersionKey,
    ) -> Result<EcManifestReadResult, EcGcError> {
        <EcClusterTranscodePorts as EcManifestQuorum>::quorum_read_manifest(&self.ports, object)
            .await
            .map_err(|error| EcGcError::InvalidRequest {
                reason: error.to_string(),
            })
    }

    async fn commit_tombstone(
        &self,
        tombstone: EcObjectManifest,
        required_acks: u8,
    ) -> Result<EcTombstoneCommitResult, EcGcError> {
        let commit = <EcClusterTranscodePorts as EcManifestQuorum>::commit_manifest(
            &self.ports,
            tombstone,
            required_acks,
        )
        .await
        .map_err(|error| EcGcError::InvalidRequest {
            reason: error.to_string(),
        })?;
        Ok(EcTombstoneCommitResult { acks: commit.acks })
    }
}

#[async_trait]
impl EcPackedPointerCommitter for PackedSegmentReleasePorts {
    async fn commit_packed_pointer(
        &self,
        segment: &PackedSegment,
        entry: &PackedSegmentEntry,
    ) -> Result<(), EcGcError> {
        let set = &self.ports.erasure_set;
        // Derived from the object, its write clock and the segment it is being
        // pointed at, so a retried commit is byte-identical and the quorum
        // keeps agreeing on one manifest hash — while a move to a different
        // segment is a different operation.
        let operation_id = deterministic_operation_id(
            &entry.object,
            entry.hlc,
            segment.erasure_set_id ^ u64::from(segment.segment_id.as_uuid().as_u128() as u32),
        );
        let mut manifest = EcObjectManifest::packed_pointer(
            segment,
            entry,
            set.profile.codec_snapshot(),
            set.topology_epoch,
            self.ports.local_node_id,
            operation_id,
        )
        .map_err(EcGcError::Metadata)?;

        // A repacked object already has a pointer, and the new one has to beat
        // it. Manifests are ordered by the object's write clock plus a rank for
        // their state, so two pointers to the same generation of the same
        // object tie — and the replacement silently loses, leaving the cluster
        // reading a segment that is about to be tombstoned. Stepping one tick
        // past whatever is there makes the move strictly newer, however many
        // times the object has been moved before. The placement inside the
        // manifest keeps the object's real write clock, so deletes and
        // overwrites still compare against the generation and not against this.
        if let EcManifestReadResult::Present(current) =
            <Self as EcTombstoneQuorum>::quorum_read_manifest(self, &entry.object).await?
        {
            if current.hlc >= manifest.hlc {
                manifest.hlc = HlcTimestamp {
                    wall_time: current.hlc.wall_time,
                    logical: current.hlc.logical.saturating_add(1),
                    node_id: current.hlc.node_id,
                };
            }
        }

        let required_acks = set.profile.manifest_quorum();
        let result =
            self.ports.commit_manifest(manifest, required_acks).await.map_err(|error| {
                EcGcError::InvalidRequest {
                    reason: format!(
                        "committing the packed pointer for {}/{} failed: {error}",
                        entry.object.bucket, entry.object.key
                    ),
                }
            })?;
        if result.acks < required_acks {
            return Err(EcGcError::InvalidRequest {
                reason: format!(
                    "packed pointer for {}/{} reached {} of {required_acks} required ack(s)",
                    entry.object.bucket, entry.object.key, result.acks
                ),
            });
        }
        Ok(())
    }
}

#[async_trait]
impl EcPackedHotCopyPurger for PackedSegmentReleasePorts {
    async fn purge_packed_hot_copies(
        &self,
        object: &EcObjectVersionKey,
        expected_source_hlc: HlcTimestamp,
    ) -> Result<(), EcGcError> {
        EcObjectCutover::purge_hot_replicas(&self.ports, object, expected_source_hlc, Uuid::nil())
            .await
            .map_err(|error| EcGcError::InvalidRequest {
                reason: format!(
                    "releasing the hot copies of {}/{} failed: {error}",
                    object.bucket, object.key
                ),
            })
    }
}

/// Why the object is no longer the one that was encoded, if it is not.
///
/// The content hash is compared only when both sides have one. A node from
/// before erasure coding carried the field answers without it, and an object
/// assembled from multipart parts has no whole-object hash at all: its record
/// carries the absent-hash sentinel. Reading either as a mismatch would refuse
/// a cutover that nothing is wrong with — for the composite object it would
/// refuse every cutover it ever attempts. The other three facts are always
/// available, and each of them changes whenever the bytes do.
fn cutover_guard_broken(
    guard: &EcCutoverGuard,
    head: &s4_cluster::QuorumHeadResult,
) -> Option<&'static str> {
    if head.hlc != guard.source_hlc {
        return Some("generation changed");
    }
    if head.size != guard.size {
        return Some("size changed");
    }
    if head.etag != guard.etag {
        return Some("etag changed");
    }
    match head.content_hash_sha256 {
        Some(hash)
            if !content_hash_is_absent(&hash)
                && !content_hash_is_absent(&guard.content_hash_sha256)
                && hash != guard.content_hash_sha256 =>
        {
            Some("content hash changed")
        }
        _ => None,
    }
}

enum ManifestNodeRead {
    Present(Box<EcObjectManifest>),
    Absent,
}

async fn read_manifest_from_node(
    ports: EcClusterTranscodePorts,
    node_id: NodeId,
    key: String,
    object: EcObjectVersionKey,
    operation_id: u128,
) -> Result<ManifestNodeRead, EcTranscodeError> {
    let bytes = match ports.read_object_from_node(node_id, key, operation_id).await {
        Ok(bytes) => bytes,
        Err(EcTranscodeError::ShardIo { reason }) if is_not_found_message(&reason) => {
            return Ok(ManifestNodeRead::Absent);
        }
        Err(error) => return Err(error),
    };
    let manifest = decode_manifest(&bytes).map_err(|error| EcTranscodeError::ManifestQuorum {
        reason: format!("manifest deserialization failed: {error}"),
    })?;
    if manifest.object_key() != object {
        return Err(EcTranscodeError::ManifestQuorum {
            reason: format!(
                "manifest object mismatch: read {}/{} while resolving {}/{}",
                manifest.bucket, manifest.key, object.bucket, object.key
            ),
        });
    }
    // Against the set the manifest names, so a pool of several sets can read
    // back everything it holds rather than only the bound set's objects.
    let erasure_set = ports.set_for_manifest_id(manifest.erasure_set_id)?;
    manifest.validate_against_erasure_set(erasure_set)?;
    Ok(ManifestNodeRead::Present(Box::new(manifest)))
}

async fn write_local_object(
    storage: Arc<dyn StorageEngine>,
    key: String,
    data: Vec<u8>,
    content_type: &'static str,
    mut metadata: HashMap<String, String>,
    hlc: HlcTimestamp,
    operation_id: u128,
) -> Result<IndexRecord, EcTranscodeError> {
    add_hlc_metadata(&mut metadata, hlc, operation_id);
    storage
        .put_object(SYSTEM_BUCKET, &key, &data, content_type, &metadata)
        .await
        .map_err(|error| EcTranscodeError::ShardIo {
            reason: format!("local EC object write failed: {error}"),
        })?;
    storage
        .head_object(SYSTEM_BUCKET, &key)
        .await
        .map_err(|error| EcTranscodeError::ShardIo {
            reason: format!("local EC object HEAD after write failed: {error}"),
        })
}

async fn write_remote_object(
    rpc_client: Arc<NodeClient>,
    local_node_id: NodeId,
    topology_epoch: u64,
    write: EcHiddenObjectWrite,
) -> Result<(), EcTranscodeError> {
    let node_id = write.node_id;
    let request = proto::WriteBlobRequest {
        bucket: SYSTEM_BUCKET.to_string(),
        key: write.key,
        content_hash: sha256(&write.data).to_vec(),
        data: write.data,
        content_type: write.content_type.to_string(),
        metadata: write.metadata,
        hlc: Some(write.hlc.to_proto()),
        version_id: None,
        coordinator_id: local_node_id.to_bytes().to_vec(),
        operation_id: write.operation_id.to_be_bytes().to_vec(),
        topology_epoch,
    };
    let response = rpc_client.send_write(node_id, request).await.map_err(|error| {
        EcTranscodeError::ShardIo {
            reason: format!("remote EC object write to {node_id} failed: {error}"),
        }
    })?;
    if response.epoch_mismatch {
        return Err(EcTranscodeError::ShardIo {
            reason: format!("remote EC object write to {node_id} rejected topology epoch"),
        });
    }
    if !response.success {
        return Err(EcTranscodeError::ShardIo {
            reason: format!(
                "remote EC object write to {node_id} failed: {}",
                response.error
            ),
        });
    }
    Ok(())
}

async fn read_local_object(
    storage: Arc<dyn StorageEngine>,
    key: String,
) -> Result<Vec<u8>, EcTranscodeError> {
    storage
        .get_object(SYSTEM_BUCKET, &key)
        .await
        .map(|(data, _)| data)
        .map_err(|error| EcTranscodeError::ShardIo {
            reason: format!("local EC object read failed: {error}"),
        })
}

async fn read_remote_object(
    rpc_client: Arc<NodeClient>,
    local_node_id: NodeId,
    node_id: NodeId,
    topology_epoch: u64,
    key: String,
    operation_id: u128,
) -> Result<Vec<u8>, EcTranscodeError> {
    let request = proto::ReadBlobRequest {
        bucket: SYSTEM_BUCKET.to_string(),
        key,
        version_id: None,
        coordinator_id: local_node_id.to_bytes().to_vec(),
        operation_id: operation_id.to_be_bytes().to_vec(),
        topology_epoch,
        range_start: None,
        range_end: None,
    };
    let response = rpc_client.send_read(node_id, request).await.map_err(|error| {
        EcTranscodeError::ShardIo {
            reason: format!("remote EC object read from {node_id} failed: {error}"),
        }
    })?;
    if response.epoch_mismatch {
        return Err(EcTranscodeError::ShardIo {
            reason: format!("remote EC object read from {node_id} rejected topology epoch"),
        });
    }
    if !response.success {
        return Err(EcTranscodeError::ShardIo {
            reason: format!(
                "remote EC object read from {node_id} failed: {}",
                response.error
            ),
        });
    }
    let expected_hash: [u8; 32] = response.content_hash.as_slice().try_into().map_err(|_| {
        EcTranscodeError::ShardVerification {
            reason: format!("remote EC object read from {node_id} returned invalid SHA-256"),
        }
    })?;
    let actual_hash = sha256(&response.data);
    if expected_hash != actual_hash {
        return Err(EcTranscodeError::ShardVerification {
            reason: format!("remote EC object read from {node_id} returned mismatched SHA-256"),
        });
    }
    Ok(response.data)
}

async fn layout_is_manifest(
    metadata: &EcMetadataStore,
    object: &EcObjectVersionKey,
    operation_id: Uuid,
) -> Result<bool, EcTranscodeError> {
    Ok(matches!(
        metadata.get_object_layout(object).await?,
        Some(ObjectDataLayout::ErasureCoded(reference)) if reference.operation_id == operation_id
    ))
}

fn shard_metadata(request: &EcShardWriteRequest) -> HashMap<String, String> {
    HashMap::from([
        ("_s4_ec_kind".to_string(), "shard".to_string()),
        (
            "_s4_ec_operation_id".to_string(),
            request.operation_id.to_string(),
        ),
        (
            "_s4_ec_chunk_index".to_string(),
            request.chunk_index.to_string(),
        ),
        (
            "_s4_ec_slot_index".to_string(),
            request.slot_index.to_string(),
        ),
        ("_s4_ec_crc32".to_string(), request.crc32.to_string()),
        ("_s4_ec_sha256".to_string(), hex_encode(request.sha256)),
    ])
}

fn manifest_metadata(
    manifest: &EcObjectManifest,
) -> Result<HashMap<String, String>, EcTranscodeError> {
    Ok(HashMap::from([
        ("_s4_ec_kind".to_string(), "manifest".to_string()),
        (
            "_s4_ec_operation_id".to_string(),
            manifest.operation_id.to_string(),
        ),
        ("_s4_ec_state".to_string(), format!("{:?}", manifest.state)),
        (
            "_s4_ec_manifest_hash".to_string(),
            hex_encode(manifest.serialized_sha256()?),
        ),
    ]))
}

fn add_hlc_metadata(metadata: &mut HashMap<String, String>, hlc: HlcTimestamp, operation_id: u128) {
    metadata.insert("_s4_hlc_wall".to_string(), hlc.wall_time.to_string());
    metadata.insert("_s4_hlc_logical".to_string(), hlc.logical.to_string());
    metadata.insert("_s4_hlc_node_id".to_string(), hlc.node_id.to_string());
    metadata.insert("_s4_operation_id".to_string(), operation_id.to_string());
}

pub(crate) fn shard_object_key(
    object: &EcObjectVersionKey,
    operation_id: Uuid,
    chunk_index: u32,
    slot_index: u8,
) -> String {
    format!(
        "__s4_ec_v1/shards/{}/{}/chunk_{:08x}/slot_{:03}",
        object_hash(object),
        operation_id,
        chunk_index,
        slot_index
    )
}

/// Key of the replicated object that holds one bucket's tiering policy.
///
/// The policy lives in the same hidden bucket as manifests and is written the
/// same way, because it has the same requirements: any node must be able to
/// read what an operator set through any other node. Decision EC2-021. The
/// bucket name is used verbatim rather than hashed — S3 already constrains it,
/// and a readable key is worth a great deal when something has to be inspected
/// by hand.
pub(crate) fn tiering_policy_object_key(bucket: &str) -> String {
    format!("__s4_ec_v1/tiering/{bucket}")
}

/// Where a pool's erasure-set roster lives in the hidden system bucket.
///
/// One object per pool, written through the cluster write path like a tiering
/// policy, so every node of the pool converges on the same set list without
/// anybody restarting.
pub(crate) fn erasure_set_roster_object_key(pool_id: u32) -> String {
    format!("__s4_ec_v1/pools/{pool_id}/erasure-sets")
}

/// Where the layout record of one erasure set lives in the hidden system bucket.
///
/// One object per set, next to the pool's roster and written the same way
/// (decision EC2-084).
pub(crate) fn set_layout_record_object_key(pool_id: u32, set_id: u64) -> String {
    format!("__s4_ec_v1/pools/{pool_id}/set-layouts/{set_id}")
}

pub(crate) fn manifest_object_key(object: &EcObjectVersionKey) -> String {
    format!("__s4_ec_v1/manifests/{}", object_hash(object))
}

/// Where the explicit replicated-small placement of one object lives.
///
/// Its own key rather than the manifest's: a small object has no manifest, and
/// a delete writes a tombstone at the manifest key. Sharing one key would make
/// the two records overwrite each other.
pub(crate) fn replicated_small_object_key(object: &EcObjectVersionKey) -> String {
    format!("__s4_ec_v1/small/{}", object_hash(object))
}

fn object_hash(object: &EcObjectVersionKey) -> String {
    let mut hasher = Sha256::new();
    hash_string(&mut hasher, &object.bucket);
    hash_string(&mut hasher, &object.key);
    match &object.version_id {
        Some(version_id) => {
            hasher.update([1]);
            hash_string(&mut hasher, version_id);
        }
        None => hasher.update([0]),
    }
    hex_encode(hasher.finalize())
}

pub(crate) fn deterministic_object_uuid(object: &EcObjectVersionKey) -> Uuid {
    let mut hasher = Sha256::new();
    hasher.update(b"s4-ec-object-v1");
    hash_string(&mut hasher, &object.bucket);
    hash_string(&mut hasher, &object.key);
    match &object.version_id {
        Some(version_id) => {
            hasher.update([1]);
            hash_string(&mut hasher, version_id);
        }
        None => hasher.update([0]),
    }
    let digest = hasher.finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    Uuid::from_bytes(bytes)
}

pub(crate) fn derive_storage_operation_id(
    base: Uuid,
    kind: &[u8],
    chunk_index: u32,
    slot_index: u8,
    state: EcPlacementState,
) -> u128 {
    let mut hasher = Sha256::new();
    hasher.update(b"s4-ec-storage-op-v1");
    hasher.update(base.as_bytes());
    hasher.update((kind.len() as u64).to_be_bytes());
    hasher.update(kind);
    hasher.update(chunk_index.to_be_bytes());
    hasher.update([slot_index]);
    hasher.update(format!("{:?}", state).as_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    u128::from_be_bytes(bytes)
}

pub(crate) fn manifest_wins(left: &EcObjectManifest, right: &EcObjectManifest) -> bool {
    let left_hlc = (left.hlc.wall_time, left.hlc.logical, left.hlc.node_id);
    let right_hlc = (right.hlc.wall_time, right.hlc.logical, right.hlc.node_id);
    left_hlc > right_hlc
        || (left_hlc == right_hlc && state_rank(left.state) > state_rank(right.state))
}

fn state_rank(state: EcPlacementState) -> u8 {
    match state {
        EcPlacementState::HotReplicated => 0,
        EcPlacementState::EcTranscodeQueued => 1,
        EcPlacementState::EcEncoding => 2,
        EcPlacementState::EcShardsDurable => 3,
        EcPlacementState::EcManifestCommitted => 4,
        EcPlacementState::EcReadVerified => 5,
        EcPlacementState::EcCommitted => 6,
        EcPlacementState::HotReplicasGcEligible => 7,
        EcPlacementState::HotReplicasPurged => 8,
        // Above the purged manifest, so that the marker wins against it at the
        // same HLC and every node learns that the hot copies coming back belong
        // to the object. Below the promoted manifest, which replaces the
        // marker the same way (EC2-026).
        EcPlacementState::Promoting => 9,
        // Above every EC state, so that a promotion committed at the same HLC
        // as the manifest it replaces wins: the bytes are back in RF=3, and a
        // reader that picked the older copy would go looking for shards that
        // GC is about to remove. Below the tombstone, because a delete beats a
        // move in any order.
        EcPlacementState::Promoted => 10,
        EcPlacementState::EcFailedRetryable => 11,
        EcPlacementState::EcFailedPermanent => 12,
        EcPlacementState::Tombstone => 13,
    }
}

fn runtime_failure_class(error: &EcTranscodeError) -> EcTranscodeFailureClass {
    match error {
        EcTranscodeError::InvalidConfig { .. }
        | EcTranscodeError::InlineWriteDisabled
        | EcTranscodeError::InvalidSource { .. }
        | EcTranscodeError::SourceHashMismatch
        | EcTranscodeError::InvalidErasureSet { .. }
        | EcTranscodeError::ReconstructionProbe { .. }
        | EcTranscodeError::Codec(_) => EcTranscodeFailureClass::Permanent,
        EcTranscodeError::Backpressure { .. }
        | EcTranscodeError::LeaseBusy { .. }
        | EcTranscodeError::ShardIo { .. }
        | EcTranscodeError::ShardVerification { .. }
        | EcTranscodeError::ManifestQuorum { .. }
        | EcTranscodeError::Metadata(_)
        | EcTranscodeError::Cpu(_) => EcTranscodeFailureClass::Retryable,
    }
}

pub(crate) fn is_not_found_message(reason: &str) -> bool {
    let lower = reason.to_ascii_lowercase();
    lower.contains("not found") || lower.contains("objectnotfound")
}

fn hash_string(hasher: &mut Sha256, value: &str) {
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value.as_bytes());
}

pub(crate) fn sha256(bytes: &[u8]) -> [u8; 32] {
    Sha256::new().chain_update(bytes).finalize().into()
}

fn hex_encode(bytes: impl AsRef<[u8]>) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let bytes = bytes.as_ref();
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[usize::from(byte >> 4)] as char);
        out.push(HEX[usize::from(byte & 0x0f)] as char);
    }
    out
}

pub(crate) fn unix_time_nanos() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(u128::from(u64::MAX)) as u64
}

fn unix_time_millis() -> u64 {
    unix_time_nanos() / 1_000_000
}

fn parse_optional_u64(name: &'static str) -> Result<Option<u64>, ClusterError> {
    match std::env::var(name) {
        Ok(raw) => raw
            .parse::<u64>()
            .map(Some)
            .map_err(|error| invalid_config(name, error.to_string())),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(invalid_config(name, error.to_string())),
    }
}

fn parse_optional_usize(name: &'static str) -> Result<Option<usize>, ClusterError> {
    match std::env::var(name) {
        Ok(raw) => raw
            .parse::<usize>()
            .map(Some)
            .map_err(|error| invalid_config(name, error.to_string())),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(invalid_config(name, error.to_string())),
    }
}

fn invalid_config(name: impl AsRef<str>, reason: impl Into<String>) -> ClusterError {
    ClusterError::Config(format!("invalid {}: {}", name.as_ref(), reason.into()))
}

/// Where the target set of a transcode job stands on this node.
#[derive(Debug, PartialEq, Eq)]
enum JobTarget<'a> {
    /// The pool has no such set.
    Unknown,
    /// Held until this node reads the set's layout record. Encoding now would
    /// place shards by a layout the pool may not have, so the job waits
    /// (EC2-088, point 3).
    Held,
    /// The set to encode into.
    Ready(&'a ErasureSet),
}

fn job_target(sets: &ErasureSetRegistry, set_id: u64) -> JobTarget<'_> {
    match sets.resolve_manifest_set(set_id) {
        None => JobTarget::Unknown,
        Some(set) if set.layout_pending => JobTarget::Held,
        Some(set) => JobTarget::Ready(set),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ec_transcode::ABSENT_CONTENT_HASH_SHA256;

    /// A job for a set this node holds until it reads the set's layout record
    /// waits; the same job is ready once the record is read, and a job for a
    /// set the pool does not have never is.
    #[test]
    fn a_job_waits_while_its_set_is_held_for_its_layout_record() {
        use crate::erasure_coding::{EcProfile, EcTopologyPolicy};
        use s4_cluster::{NodeId, PoolId};

        let pool = PoolId::new(3);
        let nodes = (1..=6u128).map(NodeId).collect::<Vec<_>>();
        let mut set = ErasureSet::inherited(
            pool,
            EcProfile::rs_small(),
            &nodes,
            &HashMap::new(),
            EcTopologyPolicy::Warn,
            1,
            1,
        )
        .unwrap();
        let set_id = set.set_id;

        set.layout_pending = true;
        let held = ErasureSetRegistry::from_sets(pool, vec![set.clone()]).unwrap();
        assert_eq!(job_target(&held, set_id), JobTarget::Held);

        set.layout_pending = false;
        let known = ErasureSetRegistry::from_sets(pool, vec![set]).unwrap();
        assert!(
            matches!(job_target(&known, set_id), JobTarget::Ready(found) if found.set_id == set_id)
        );
        assert_eq!(job_target(&known, set_id + 100), JobTarget::Unknown);
    }

    fn guard() -> EcCutoverGuard {
        EcCutoverGuard {
            source_hlc: HlcTimestamp::from_record(100, 0, 1),
            content_hash_sha256: [7u8; 32],
            etag: "\"deadbeef\"".to_string(),
            size: 8 * 1024 * 1024,
        }
    }

    fn head_matching(guard: &EcCutoverGuard) -> s4_cluster::QuorumHeadResult {
        s4_cluster::QuorumHeadResult {
            etag: guard.etag.clone(),
            content_type: "application/octet-stream".to_string(),
            size: guard.size,
            metadata: HashMap::new(),
            hlc: guard.source_hlc,
            version_id: None,
            content_hash_sha256: Some(guard.content_hash_sha256),
            erasure_coded: false,
            replicas_responded: 3,
        }
    }

    #[test]
    fn an_unchanged_object_passes_the_cutover_guard() {
        let guard = guard();
        assert_eq!(cutover_guard_broken(&guard, &head_matching(&guard)), None);
    }

    #[test]
    fn every_way_the_object_can_change_breaks_the_cutover_guard() {
        let guard = guard();

        let mut newer_generation = head_matching(&guard);
        newer_generation.hlc = HlcTimestamp::from_record(101, 0, 1);
        assert_eq!(
            cutover_guard_broken(&guard, &newer_generation),
            Some("generation changed")
        );

        let mut resized = head_matching(&guard);
        resized.size += 1;
        assert_eq!(cutover_guard_broken(&guard, &resized), Some("size changed"));

        let mut retagged = head_matching(&guard);
        retagged.etag = "\"cafe\"".to_string();
        assert_eq!(
            cutover_guard_broken(&guard, &retagged),
            Some("etag changed")
        );

        let mut rewritten = head_matching(&guard);
        rewritten.content_hash_sha256 = Some([8u8; 32]);
        assert_eq!(
            cutover_guard_broken(&guard, &rewritten),
            Some("content hash changed")
        );
    }

    /// An object assembled from multipart parts has no whole-object hash.
    ///
    /// Its record carries the absent-hash sentinel, so comparing against it
    /// compares nothing. Before this was read as "unknown", every multipart
    /// object failed its cutover on a hash nobody had computed, and failed it
    /// again on every retry.
    #[test]
    fn a_composite_object_without_a_whole_object_hash_passes_the_cutover_guard() {
        let guard = guard();
        let mut head = head_matching(&guard);
        head.content_hash_sha256 = Some(ABSENT_CONTENT_HASH_SHA256);

        assert_eq!(cutover_guard_broken(&guard, &head), None);
    }

    /// The same silence, from the other side.
    ///
    /// A transcode queued from a source without a whole-object hash carries
    /// the sentinel in its guard, and the pool answering with a real hash does
    /// not make the two comparable.
    #[test]
    fn a_guard_without_a_whole_object_hash_compares_nothing() {
        let mut guard = guard();
        guard.content_hash_sha256 = ABSENT_CONTENT_HASH_SHA256;
        let head = {
            let mut head = head_matching(&guard);
            head.content_hash_sha256 = Some([8u8; 32]);
            head
        };

        assert_eq!(cutover_guard_broken(&guard, &head), None);
    }

    /// A pool that reports no hash is not a pool reporting a wrong one.
    ///
    /// Only a node from before the field existed answers without it. Reading
    /// that silence as a mismatch would stop every cutover in a cluster that is
    /// halfway through an upgrade, which is exactly when it must keep working.
    #[test]
    fn a_missing_content_hash_does_not_break_the_cutover_guard() {
        let guard = guard();
        let mut head = head_matching(&guard);
        head.content_hash_sha256 = None;

        assert_eq!(cutover_guard_broken(&guard, &head), None);
    }

    #[test]
    fn storage_operation_ids_are_unique_per_shard_and_state() {
        let base = Uuid::from_u128(7);
        let first = derive_storage_operation_id(
            base,
            b"ec-shard-write",
            0,
            1,
            EcPlacementState::EcShardsDurable,
        );
        let second = derive_storage_operation_id(
            base,
            b"ec-shard-write",
            0,
            2,
            EcPlacementState::EcShardsDurable,
        );
        let manifest = derive_storage_operation_id(
            base,
            b"ec-manifest-write",
            0,
            0,
            EcPlacementState::EcCommitted,
        );

        assert_ne!(first, second);
        assert_ne!(first, manifest);
    }

    #[test]
    fn hidden_keys_do_not_embed_user_key_text() {
        let object = EcObjectVersionKey::new("bucket", "tenant/raw path.bin", Some("v1".into()));
        let shard = shard_object_key(&object, Uuid::from_u128(9), 12, 3);
        let manifest = manifest_object_key(&object);

        assert!(shard.starts_with("__s4_ec_v1/shards/"));
        assert!(manifest.starts_with("__s4_ec_v1/manifests/"));
        assert!(!shard.contains("tenant/raw"));
        assert!(!manifest.contains("bucket"));
    }
}
