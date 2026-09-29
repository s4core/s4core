// Copyright 2026 S4Core Team
//
// Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

//! Erasure Coding v1 staged write/transcode path.
//!
//! Client writes are still acknowledged by the existing RF=3 quorum path. This
//! module starts after that boundary: it turns a durable hot object into EC
//! shards, commits a quorum-readable manifest, verifies reconstruction, and
//! only then allows the object layout to cut over to EC.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use s4_cluster::{HlcTimestamp, NodeId};
use s4_codec::{CodecError, EcParity, EncodedChunk, RecoveryPath, Shard, ShardRole};
use s4_core::types::IndexRecord;
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;

use crate::codec_runtime::{EcCodecCpuLimiter, EcCpuError};
use crate::ec_metadata::manifest_state_rank;
use crate::ec_metadata::{
    ChunkManifest, EcHotReplicaGcRecord, EcManifestReadResult, EcMetadataError, EcMetadataStore,
    EcObjectManifest, EcObjectManifestRef, EcObjectVersionKey, EcPlacementState,
    EcTranscodeFailureClass, EcTranscodeFailureRecord, EcTranscodeJobRecord, EcTranscodeOrigin,
    EcTranscodeTarget, ObjectDataLayout, ReplicatedSmallObjectPlacement, ShardManifest,
    EC_MANIFEST_FORMAT_VERSION, REPLICATED_SMALL_REPLICA_COUNT,
};
use crate::ec_metrics;
use crate::ec_packed_segments::{
    packed_segment_id_from_manifest_key, PackedSegment, PackedSegmentState,
};
use crate::erasure_coding::{ErasureSet, ErasureSetState};

/// Default maximum number of concurrent staged transcodes per node.
pub const DEFAULT_EC_MAX_CONCURRENT_TRANSCODES: usize = 2;
/// Default maximum number of concurrent inline EC writes.
pub const DEFAULT_EC_MAX_CONCURRENT_INLINE_WRITES: usize = 0;
/// Default lease TTL for an active transcode.
pub const DEFAULT_EC_TRANSCODE_LEASE_SECS: u64 = 300;
/// Default upper bound for full reconstruction probing policy.
pub const DEFAULT_EC_FULL_VERIFY_MAX_BYTES: u64 = 1_073_741_824;
/// Default number of deterministic chunk probes for large objects.
pub const DEFAULT_EC_RECONSTRUCTION_PROBE_CHUNKS: usize = 3;
/// Default hot RF=3 replica GC grace period.
pub const DEFAULT_EC_HOT_REPLICA_GC_GRACE_SECS: u64 = 86_400;
/// Default retry budget for a staged transcode.
pub const DEFAULT_EC_MAX_RETRY_ATTEMPTS: u32 = 10;
/// Default initial retry backoff.
pub const DEFAULT_EC_INITIAL_BACKOFF_SECS: u64 = 30;
/// Default maximum retry backoff.
pub const DEFAULT_EC_MAX_BACKOFF_SECS: u64 = 3_600;

/// Hot RF=3 replica purge mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EcHotPurgeMode {
    /// Purge automatically after the grace and verification gates pass.
    Auto,
    /// Require an admin trigger after the object is GC eligible.
    Manual,
}

/// EC staged transcode configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EcTranscodeConfig {
    /// Maximum staged transcodes allowed to run concurrently.
    pub max_concurrent_transcodes: usize,
    /// Maximum inline EC writes allowed to run concurrently.
    pub max_concurrent_inline_writes: usize,
    /// Whether inline EC writes are enabled.
    pub inline_write_enabled: bool,
    /// Active transcode lease TTL.
    pub transcode_lease_secs: u64,
    /// Objects at or below this size probe every chunk.
    pub full_verify_max_bytes: u64,
    /// Number of deterministic probes for larger objects.
    pub reconstruction_probe_chunks: usize,
    /// Hot RF=3 replica GC grace period.
    pub hot_replica_gc_grace_secs: u64,
    /// Hot RF=3 replica purge mode.
    pub hot_purge_mode: EcHotPurgeMode,
    /// Optional approximate transcode bandwidth cap in bytes/sec.
    pub transcode_bandwidth_limit: Option<u64>,
    /// Maximum retry attempts before a retryable failure becomes permanent.
    pub max_retry_attempts: u32,
    /// Initial retry backoff.
    pub initial_backoff_secs: u64,
    /// Maximum retry backoff.
    pub max_backoff_secs: u64,
}

impl EcTranscodeConfig {
    /// Parse Phase 4 `S4_EC_*` settings from the environment.
    pub fn from_env() -> Result<Self, EcTranscodeError> {
        let max_concurrent_transcodes = parse_optional_usize("S4_EC_MAX_CONCURRENT_TRANSCODES")?
            .unwrap_or(DEFAULT_EC_MAX_CONCURRENT_TRANSCODES);
        if max_concurrent_transcodes == 0 {
            return Err(invalid_config(
                "S4_EC_MAX_CONCURRENT_TRANSCODES",
                "must be greater than 0",
            ));
        }

        let max_concurrent_inline_writes =
            parse_optional_usize("S4_EC_MAX_CONCURRENT_INLINE_WRITES")?
                .unwrap_or(DEFAULT_EC_MAX_CONCURRENT_INLINE_WRITES);
        let inline_write_enabled =
            parse_optional_bool("S4_EC_INLINE_WRITE_ENABLED")?.unwrap_or(false);
        if inline_write_enabled && max_concurrent_inline_writes == 0 {
            return Err(invalid_config(
                "S4_EC_MAX_CONCURRENT_INLINE_WRITES",
                "must be greater than 0 when inline EC writes are enabled",
            ));
        }

        let transcode_lease_secs = parse_optional_u64("S4_EC_TRANSCODE_LEASE_SECS")?
            .unwrap_or(DEFAULT_EC_TRANSCODE_LEASE_SECS);
        if transcode_lease_secs == 0 {
            return Err(invalid_config(
                "S4_EC_TRANSCODE_LEASE_SECS",
                "must be greater than 0",
            ));
        }

        let full_verify_max_bytes = parse_optional_u64("S4_EC_FULL_VERIFY_MAX_BYTES")?
            .unwrap_or(DEFAULT_EC_FULL_VERIFY_MAX_BYTES);
        if full_verify_max_bytes == 0 {
            return Err(invalid_config(
                "S4_EC_FULL_VERIFY_MAX_BYTES",
                "must be greater than 0",
            ));
        }

        let reconstruction_probe_chunks =
            parse_optional_usize("S4_EC_RECONSTRUCTION_PROBE_CHUNKS")?
                .unwrap_or(DEFAULT_EC_RECONSTRUCTION_PROBE_CHUNKS);
        if reconstruction_probe_chunks == 0 {
            return Err(invalid_config(
                "S4_EC_RECONSTRUCTION_PROBE_CHUNKS",
                "must be greater than 0",
            ));
        }

        let hot_replica_gc_grace_secs = parse_optional_u64("S4_EC_HOT_REPLICA_GC_GRACE_SECS")?
            .unwrap_or(DEFAULT_EC_HOT_REPLICA_GC_GRACE_SECS);
        let hot_purge_mode = match std::env::var("S4_EC_HOT_PURGE_MODE")
            .unwrap_or_else(|_| "auto".to_string())
            .as_str()
        {
            "auto" => EcHotPurgeMode::Auto,
            "manual" => EcHotPurgeMode::Manual,
            other => {
                return Err(invalid_config(
                    "S4_EC_HOT_PURGE_MODE",
                    format!("unknown value '{other}'; expected auto or manual"),
                ));
            }
        };

        let transcode_bandwidth_limit =
            parse_optional_u64("S4_EC_TRANSCODE_BANDWIDTH_LIMIT")?.filter(|limit| *limit > 0);
        let max_retry_attempts = parse_optional_u32("S4_EC_MAX_RETRY_ATTEMPTS")?
            .unwrap_or(DEFAULT_EC_MAX_RETRY_ATTEMPTS);
        if max_retry_attempts == 0 {
            return Err(invalid_config(
                "S4_EC_MAX_RETRY_ATTEMPTS",
                "must be greater than 0",
            ));
        }
        let initial_backoff_secs = parse_optional_u64("S4_EC_INITIAL_BACKOFF_SECS")?
            .unwrap_or(DEFAULT_EC_INITIAL_BACKOFF_SECS);
        let max_backoff_secs =
            parse_optional_u64("S4_EC_MAX_BACKOFF_SECS")?.unwrap_or(DEFAULT_EC_MAX_BACKOFF_SECS);
        if initial_backoff_secs == 0 || max_backoff_secs == 0 {
            return Err(invalid_config(
                "S4_EC_*_BACKOFF_SECS",
                "backoff values must be greater than 0",
            ));
        }

        Ok(Self {
            max_concurrent_transcodes,
            max_concurrent_inline_writes,
            inline_write_enabled,
            transcode_lease_secs,
            full_verify_max_bytes,
            reconstruction_probe_chunks,
            hot_replica_gc_grace_secs,
            hot_purge_mode,
            transcode_bandwidth_limit,
            max_retry_attempts,
            initial_backoff_secs,
            max_backoff_secs,
        })
    }

    /// Active transcode lease TTL in nanoseconds.
    pub fn lease_ttl_nanos(&self) -> u64 {
        seconds_to_nanos(self.transcode_lease_secs)
    }

    /// Hot RF=3 replica GC grace period in nanoseconds.
    pub fn hot_replica_gc_grace_nanos(&self) -> u64 {
        seconds_to_nanos(self.hot_replica_gc_grace_secs)
    }

    /// Deterministic retry delay for a transcode attempt in nanoseconds.
    pub fn retry_delay_nanos(&self, attempt: u32, operation_id: Uuid) -> u64 {
        let exponent = attempt.saturating_sub(1).min(31);
        let base = self
            .initial_backoff_secs
            .saturating_mul(1u64 << exponent)
            .min(self.max_backoff_secs);
        let jitter = deterministic_jitter_secs(operation_id, attempt, base);
        seconds_to_nanos(base.saturating_add(jitter))
    }
}

impl Default for EcTranscodeConfig {
    fn default() -> Self {
        Self {
            max_concurrent_transcodes: DEFAULT_EC_MAX_CONCURRENT_TRANSCODES,
            max_concurrent_inline_writes: DEFAULT_EC_MAX_CONCURRENT_INLINE_WRITES,
            inline_write_enabled: false,
            transcode_lease_secs: DEFAULT_EC_TRANSCODE_LEASE_SECS,
            full_verify_max_bytes: DEFAULT_EC_FULL_VERIFY_MAX_BYTES,
            reconstruction_probe_chunks: DEFAULT_EC_RECONSTRUCTION_PROBE_CHUNKS,
            hot_replica_gc_grace_secs: DEFAULT_EC_HOT_REPLICA_GC_GRACE_SECS,
            hot_purge_mode: EcHotPurgeMode::Auto,
            transcode_bandwidth_limit: None,
            max_retry_attempts: DEFAULT_EC_MAX_RETRY_ATTEMPTS,
            initial_backoff_secs: DEFAULT_EC_INITIAL_BACKOFF_SECS,
            max_backoff_secs: DEFAULT_EC_MAX_BACKOFF_SECS,
        }
    }
}

/// EC write backpressure semaphores.
#[derive(Debug, Clone)]
pub struct EcWriteBackpressure {
    transcodes: Arc<Semaphore>,
    inline_writes: Arc<Semaphore>,
    config: EcTranscodeConfig,
}

impl EcWriteBackpressure {
    /// Build backpressure controls from config.
    pub fn new(config: EcTranscodeConfig) -> Self {
        Self {
            transcodes: Arc::new(Semaphore::new(config.max_concurrent_transcodes)),
            inline_writes: Arc::new(Semaphore::new(config.max_concurrent_inline_writes)),
            config,
        }
    }

    /// Current configuration.
    pub fn config(&self) -> &EcTranscodeConfig {
        &self.config
    }

    async fn acquire_transcode(&self) -> Result<OwnedSemaphorePermit, EcTranscodeError> {
        self.transcodes
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| EcTranscodeError::Backpressure {
                reason: "EC transcode semaphore is closed".to_string(),
            })
    }

    /// Acquire an inline-write permit for one erasure set.
    ///
    /// Inline EC remains disabled by default. The set is taken as an argument
    /// only so the counters name the set the write was headed for.
    pub async fn acquire_inline_write(
        &self,
        erasure_set_id: u64,
    ) -> Result<OwnedSemaphorePermit, EcTranscodeError> {
        if !self.config.inline_write_enabled {
            ec_metrics::record_inline_write_rejected(erasure_set_id);
            return Err(EcTranscodeError::InlineWriteDisabled);
        }
        let permit = self.inline_writes.clone().acquire_owned().await.map_err(|_| {
            ec_metrics::record_inline_write_rejected(erasure_set_id);
            EcTranscodeError::Backpressure {
                reason: "EC inline-write semaphore is closed".to_string(),
            }
        })?;
        ec_metrics::record_inline_write(erasure_set_id);
        Ok(permit)
    }
}

/// RF=3 source object that is eligible for staged EC transcode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EcSourceObject {
    /// Object version key.
    pub object: EcObjectVersionKey,
    /// Full logical object bytes read from the hot RF=3 path.
    pub data: Vec<u8>,
    /// SHA-256 of the full logical object.
    pub content_hash_sha256: [u8; 32],
    /// S3 ETag of the source object.
    pub etag: String,
    /// Content-Type of the source object.
    pub content_type: String,
    /// User-defined metadata copied into the EC manifest.
    pub user_metadata: HashMap<String, String>,
    /// HLC timestamp of the RF=3 source object/version.
    pub source_hlc: HlcTimestamp,
    /// Node that wrote the winning RF=3 source object/version.
    pub origin_node_id: NodeId,
    /// Source creation time in nanoseconds since Unix epoch.
    pub created_at: u64,
}

impl EcSourceObject {
    /// Assemble a sealed packed segment into the blob the pipeline encodes.
    ///
    /// The segment is encoded as one object, so the bytes have to be laid out
    /// exactly where the index says they are. That includes the gaps: a segment
    /// whose objects were deleted after sealing has holes, and they are filled
    /// with zeroes rather than closed up. Closing them would move every object
    /// after the hole and invalidate layouts already written — that is a repack,
    /// which is a different operation with a different safety story
    /// (`EC2-061`). The dead bytes are encoded and reclaimed later, by the
    /// repacker, which is exactly what the dead-byte count exists to schedule.
    ///
    /// `parts` must give the bytes of every live entry, in index order. Each is
    /// checked against the length and SHA-256 the index recorded, because this
    /// is the last point at which the object's own bytes are still available to
    /// check against: after this they are shards.
    ///
    /// # Errors
    ///
    /// Returns [`EcTranscodeError::InvalidSource`] when the segment is not
    /// sealed, holds nothing to encode, or the supplied bytes do not match what
    /// the index says they must be.
    pub fn for_packed_segment(
        segment: &PackedSegment,
        parts: &[(EcObjectVersionKey, Vec<u8>)],
        origin_node_id: NodeId,
        created_at: u64,
    ) -> Result<Self, EcTranscodeError> {
        if segment.state != PackedSegmentState::Sealed {
            return Err(EcTranscodeError::InvalidSource {
                reason: format!(
                    "segment {} is {} and cannot be encoded; only a sealed segment can",
                    segment.segment_id, segment.state
                ),
            });
        }
        if segment.entries.is_empty() {
            return Err(EcTranscodeError::InvalidSource {
                reason: format!(
                    "segment {} holds no live object; it should be dropped, not encoded",
                    segment.segment_id
                ),
            });
        }
        if parts.len() != segment.entries.len() {
            return Err(EcTranscodeError::InvalidSource {
                reason: format!(
                    "segment {} has {} live entries but {} part(s) were supplied",
                    segment.segment_id,
                    segment.entries.len(),
                    parts.len()
                ),
            });
        }

        let blob_len =
            usize::try_from(segment.used_bytes).map_err(|_| EcTranscodeError::InvalidSource {
                reason: format!(
                    "segment {} extent {} does not fit in usize",
                    segment.segment_id, segment.used_bytes
                ),
            })?;
        let mut blob = vec![0u8; blob_len];

        for (entry, (object, bytes)) in segment.entries.iter().zip(parts) {
            if &entry.object != object {
                return Err(EcTranscodeError::InvalidSource {
                    reason: format!(
                        "segment {} expects {}/{} at offset {} but was given {}/{}",
                        segment.segment_id,
                        entry.object.bucket,
                        entry.object.key,
                        entry.offset,
                        object.bucket,
                        object.key
                    ),
                });
            }
            if bytes.len() as u64 != entry.length {
                return Err(EcTranscodeError::InvalidSource {
                    reason: format!(
                        "{}/{} is {} byte(s) but the segment index says {}",
                        object.bucket,
                        object.key,
                        bytes.len(),
                        entry.length
                    ),
                });
            }
            if sha256(bytes) != entry.content_hash_sha256 {
                return Err(EcTranscodeError::InvalidSource {
                    reason: format!(
                        "{}/{} does not match the SHA-256 the segment index recorded",
                        object.bucket, object.key
                    ),
                });
            }
            let start =
                usize::try_from(entry.offset).map_err(|_| EcTranscodeError::InvalidSource {
                    reason: format!("offset {} does not fit in usize", entry.offset),
                })?;
            let end =
                start.checked_add(bytes.len()).filter(|end| *end <= blob_len).ok_or_else(|| {
                    EcTranscodeError::InvalidSource {
                        reason: format!(
                            "{}/{} runs past the {} byte extent of segment {}",
                            object.bucket, object.key, blob_len, segment.segment_id
                        ),
                    }
                })?;
            blob[start..end].copy_from_slice(bytes);
        }

        // The newest write the segment holds. A real HLC from a real write, so
        // the segment orders after every object inside it rather than carrying
        // an invented timestamp.
        let source_hlc = segment.entries.iter().map(|entry| entry.hlc).max().ok_or_else(|| {
            EcTranscodeError::InvalidSource {
                reason: format!(
                    "segment {} has no entry to take an HLC from",
                    segment.segment_id
                ),
            }
        })?;
        let content_hash_sha256 = sha256(&blob);

        Ok(Self {
            object: segment.manifest_object_key(),
            data: blob,
            content_hash_sha256,
            // Internal: a segment is never handed to a client, so this ETag is
            // for operator diagnostics, not for an S3 response. Each object's
            // own ETag lives in the segment index.
            etag: format!("\"{}\"", hex_lower(&content_hash_sha256)),
            content_type: "application/octet-stream".to_string(),
            user_metadata: HashMap::new(),
            source_hlc,
            origin_node_id,
            created_at,
        })
    }

    /// Build a source object from an S4 index record and bytes.
    pub fn from_index_record(
        bucket: impl Into<String>,
        key: impl Into<String>,
        version_id: Option<String>,
        data: Vec<u8>,
        record: &IndexRecord,
    ) -> Self {
        let user_metadata = record
            .metadata
            .iter()
            .filter(|(key, _)| !key.starts_with("_s4_"))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        Self {
            object: EcObjectVersionKey::new(
                bucket,
                key,
                version_id.or_else(|| record.version_id.clone()),
            ),
            data,
            content_hash_sha256: record.content_hash,
            etag: record.etag.clone(),
            content_type: record.content_type.clone(),
            user_metadata,
            source_hlc: HlcTimestamp::from_record(
                record.hlc_timestamp,
                record.hlc_logical,
                record.origin_node_id,
            ),
            origin_node_id: NodeId(record.origin_node_id),
            created_at: record.created_at,
        }
    }

    /// Original object size.
    pub fn size(&self) -> Result<u64, EcTranscodeError> {
        u64::try_from(self.data.len()).map_err(|_| EcTranscodeError::InvalidSource {
            reason: "source object size does not fit in u64".to_string(),
        })
    }

    /// Validate source bytes and metadata before EC work starts.
    pub fn validate(&self) -> Result<(), EcTranscodeError> {
        self.object.validate()?;
        if self.source_hlc.is_zero() {
            return Err(EcTranscodeError::InvalidSource {
                reason: "source_hlc must not be zero".to_string(),
            });
        }
        let actual_hash = sha256(&self.data);
        if actual_hash != self.content_hash_sha256 {
            return Err(EcTranscodeError::SourceHashMismatch);
        }
        Ok(())
    }
}

/// Request for one staged transcode attempt.
#[derive(Debug, Clone)]
pub struct EcTranscodeRequest {
    /// RF=3 source object.
    pub source: EcSourceObject,
    /// Target active erasure set.
    pub erasure_set: ErasureSet,
    /// Node attempting the transcode.
    pub owner_node_id: NodeId,
    /// Current time in nanoseconds since Unix epoch.
    pub now_nanos: u64,
    /// What created the job this attempt belongs to.
    ///
    /// Carried through the request rather than re-read from the store on every
    /// state transition: one staged transcode writes the job record half a
    /// dozen times, and each of those would otherwise cost an extra read just
    /// to keep a field the caller already knows.
    pub origin: EcTranscodeOrigin,
    /// Whether this attempt encodes one object or a whole packed segment.
    ///
    /// Carried for the same reason as `origin`, and used at exactly one point:
    /// the cutover. A segment has no single object layout to guard, so it ends
    /// the pipeline by marking itself encoded and leaves switching its contents
    /// to `EcMetadataStore::switch_packed_segment_to_ec`.
    pub target: EcTranscodeTarget,
}

impl EcTranscodeRequest {
    /// Validate request-level invariants.
    pub fn validate(&self) -> Result<(), EcTranscodeError> {
        self.source.validate()?;
        if self.erasure_set.state != ErasureSetState::Active {
            return Err(EcTranscodeError::InvalidErasureSet {
                reason: "target erasure set must be Active".to_string(),
            });
        }
        self.erasure_set.profile.validate().map_err(|error| {
            EcTranscodeError::InvalidErasureSet {
                reason: error.to_string(),
            }
        })?;
        if self.now_nanos == 0 {
            return Err(EcTranscodeError::InvalidSource {
                reason: "now_nanos must be greater than 0".to_string(),
            });
        }
        Ok(())
    }
}

/// Request to write one EC shard to its assigned node.
#[derive(Debug, Clone)]
pub struct EcShardWriteRequest {
    /// Object version addressed by the shard.
    pub object: EcObjectVersionKey,
    /// Operation that produced the shard.
    pub operation_id: Uuid,
    /// Logical chunk index.
    pub chunk_index: u32,
    /// Shard slot index.
    pub slot_index: u8,
    /// Assigned node.
    pub node_id: NodeId,
    /// Shard role.
    pub role: ShardRole,
    /// Shard bytes.
    pub data: Vec<u8>,
    /// Expected CRC32.
    pub crc32: u32,
    /// Expected SHA-256.
    pub sha256: [u8; 32],
}

/// Durable ACK from a shard write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EcShardWriteAck {
    /// Node that accepted the shard.
    pub node_id: NodeId,
    /// Logical chunk index.
    pub chunk_index: u32,
    /// Shard slot index.
    pub slot_index: u8,
    /// ACKed shard size.
    pub size: u64,
    /// ACKed CRC32.
    pub crc32: u32,
    /// ACKed SHA-256.
    pub sha256: [u8; 32],
}

/// Request to read one EC shard for verification.
#[derive(Debug, Clone)]
pub struct EcShardReadRequest {
    /// Object version addressed by the shard.
    pub object: EcObjectVersionKey,
    /// Operation that produced the shard.
    pub operation_id: Uuid,
    /// Logical chunk index.
    pub chunk_index: u32,
    /// Shard slot index.
    pub slot_index: u8,
    /// Assigned node.
    pub node_id: NodeId,
}

/// Port used by the transcode worker to write and verify shards.
#[async_trait]
pub trait EcShardStore: Send + Sync {
    /// Write one shard durably to its assigned node.
    async fn write_shard(
        &self,
        request: EcShardWriteRequest,
    ) -> Result<EcShardWriteAck, EcTranscodeError>;

    /// Read one shard back for manifest-based verification.
    async fn read_shard(&self, request: EcShardReadRequest) -> Result<Vec<u8>, EcTranscodeError>;
}

/// Manifest quorum commit result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EcManifestCommitResult {
    /// Number of metadata replicas that ACKed the commit.
    pub acks: u8,
}

/// Port used by the transcode worker for global manifest metadata.
#[async_trait]
pub trait EcManifestQuorum: Send + Sync {
    /// Commit a manifest through the metadata quorum path.
    async fn commit_manifest(
        &self,
        manifest: EcObjectManifest,
        required_acks: u8,
    ) -> Result<EcManifestCommitResult, EcTranscodeError>;

    /// Read a manifest through quorum metadata semantics.
    async fn quorum_read_manifest(
        &self,
        object: &EcObjectVersionKey,
    ) -> Result<EcManifestReadResult, EcTranscodeError>;
}

/// Result of atomically cutting object metadata over to EC.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum EcCutoverOutcome {
    /// The expected RF=3 source version still won and EC is now authoritative.
    #[default]
    Committed,
    /// A newer concurrent PUT won; the produced shards are orphan candidates.
    Superseded,
}

/// What the RF=3 object must still be for a cutover to be safe.
///
/// The generation clock alone answers "has anything been written since?", and
/// for a client write that is enough, because the encoder was handed the bytes
/// it is encoding. An auto-tiering migration is different: it reads the object
/// itself, minutes or hours after deciding to migrate it, so the cutover states
/// what it believes the object to be and refuses to switch the layout if the
/// pool no longer agrees.
/// The content hash a record carries when it has none.
///
/// An object assembled from multipart parts is stored as a manifest of its
/// segments, and hashing the whole of it is work the write path does not do.
/// Its record carries zeros where a single blob carries a SHA-256, so the
/// value is a sentinel for "not computed" rather than a hash anything can be
/// compared against. Every EC guard has to read it the way it reads a field a
/// node was too old to send (decision EC2-204).
pub const ABSENT_CONTENT_HASH_SHA256: [u8; 32] = [0u8; 32];

/// Whether a reported content hash is the "no whole-object hash" sentinel.
pub fn content_hash_is_absent(hash: &[u8; 32]) -> bool {
    *hash == ABSENT_CONTENT_HASH_SHA256
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EcCutoverGuard {
    /// Generation the encoded shards were produced from.
    pub source_hlc: HlcTimestamp,
    /// SHA-256 of the bytes that were encoded.
    pub content_hash_sha256: [u8; 32],
    /// ETag the object had when it was read.
    pub etag: String,
    /// Size the object had when it was read.
    pub size: u64,
}

/// Port used for object metadata cutover and hot replica purge.
#[async_trait]
pub trait EcObjectCutover: Send + Sync {
    /// Atomically move object layout to the EC manifest if the guard still holds.
    async fn cutover_to_ec(
        &self,
        guard: EcCutoverGuard,
        manifest_ref: EcObjectManifestRef,
    ) -> Result<EcCutoverOutcome, EcTranscodeError>;

    /// Persist/admin-notify that hot RF=3 replicas are GC eligible.
    async fn mark_hot_replicas_gc_eligible(
        &self,
        record: EcHotReplicaGcRecord,
    ) -> Result<(), EcTranscodeError>;

    /// Physically purge hot RF=3 replicas after all gates pass.
    async fn purge_hot_replicas(
        &self,
        object: &EcObjectVersionKey,
        expected_source_hlc: HlcTimestamp,
        operation_id: Uuid,
    ) -> Result<(), EcTranscodeError>;
}

/// Staged transcode outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EcTranscodeOutcome {
    /// Object is below the profile threshold and should remain replicated-small.
    SkippedSmall {
        /// Object version that was skipped.
        object: EcObjectVersionKey,
        /// Object size.
        size: u64,
        /// Profile threshold.
        min_object_size: u64,
    },
    /// EC cutover succeeded and hot replicas are GC eligible after grace.
    Committed {
        /// Operation that produced the winning EC layout.
        operation_id: Uuid,
        /// Final committed manifest.
        manifest: Box<EcObjectManifest>,
        /// Hot-replica GC gate.
        hot_replica_gc: EcHotReplicaGcRecord,
    },
    /// A concurrent PUT won before cutover.
    Superseded {
        /// Losing operation.
        operation_id: Uuid,
    },
    /// A packed segment was encoded and verified.
    ///
    /// Distinct from [`Self::Committed`] because nothing was cut over: the
    /// objects inside the segment still read from their hot copies, and stay
    /// that way until each has been read back out of EC.
    PackedSegmentEncoded {
        /// Operation that produced the segment's manifest.
        operation_id: Uuid,
        /// Final committed manifest of the segment.
        manifest: Box<EcObjectManifest>,
    },
}

/// Verification report for an EC manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EcVerificationReport {
    /// Number of chunks whose reconstruction probe succeeded.
    pub probed_chunks: usize,
    /// Number of shard byte streams verified against the manifest.
    pub verified_shards: usize,
    /// Whether full-object SHA-256 was verified through EC decode.
    pub full_object_sha256_verified: bool,
}

/// Hot-replica purge decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EcHotReplicaPurgeDecision {
    /// Purge was completed and recorded.
    Purged,
    /// Manual mode requires an admin trigger.
    ManualMode,
    /// Grace period has not elapsed yet.
    GracePeriod,
    /// Full EC SHA-256 verification has not completed yet.
    NeedsFullVerification,
    /// Object is not in `EcCommitted` state.
    NotCommitted,
    /// The GC record was already purged.
    AlreadyPurged,
}

/// Phase 4 staged transcode worker.
#[derive(Clone)]
pub struct EcStagedTranscodeWorker {
    metadata: Arc<EcMetadataStore>,
    codec_limiter: EcCodecCpuLimiter,
    backpressure: EcWriteBackpressure,
    config: EcTranscodeConfig,
}

impl EcStagedTranscodeWorker {
    /// Create a staged transcode worker.
    pub fn new(
        metadata: Arc<EcMetadataStore>,
        codec_limiter: EcCodecCpuLimiter,
        backpressure: EcWriteBackpressure,
    ) -> Self {
        let config = backpressure.config().clone();
        Self {
            metadata,
            codec_limiter,
            backpressure,
            config,
        }
    }

    /// Current worker configuration.
    pub fn config(&self) -> &EcTranscodeConfig {
        &self.config
    }

    /// Transcode one already-durable RF=3 source object to EC.
    pub async fn transcode<S, M, C>(
        &self,
        request: EcTranscodeRequest,
        shard_store: &S,
        manifest_quorum: &M,
        cutover: &C,
    ) -> Result<EcTranscodeOutcome, EcTranscodeError>
    where
        S: EcShardStore,
        M: EcManifestQuorum,
        C: EcObjectCutover,
    {
        request.source.object.validate()?;
        if request.source.source_hlc.is_zero() {
            return Err(EcTranscodeError::InvalidSource {
                reason: "source_hlc must not be zero".to_string(),
            });
        }
        if request.now_nanos == 0 {
            return Err(EcTranscodeError::InvalidSource {
                reason: "now_nanos must be greater than 0".to_string(),
            });
        }
        let operation_id = deterministic_operation_id(
            &request.source.object,
            request.source.source_hlc,
            request.erasure_set.set_id,
        );
        ec_metrics::record_transcode_job(request.erasure_set.set_id);
        let attempt = self.next_attempt(operation_id).await?;
        if let Err(error) = request.validate() {
            self.record_failure(&request, operation_id, attempt, &error).await?;
            ec_metrics::record_transcode_failure(request.erasure_set.set_id);
            return Err(error);
        }

        let source_size = request.source.size()?;
        // The threshold decides whether one client object is worth coding on
        // its own. A packed segment is the answer to objects that were not, and
        // holding it to the same rule would refuse to code the very thing that
        // exists to make them codeable — leaving its objects at RF=3 for good
        // and writing a replicated-small layout against a key that is a
        // container rather than an object.
        if !request.target.is_packed_segment()
            && source_size < request.erasure_set.profile.min_object_size
        {
            let replica_nodes = request
                .erasure_set
                .slot_assignment
                .slots()
                .iter()
                .take(REPLICATED_SMALL_REPLICA_COUNT)
                .map(|slot| slot.node_id)
                .collect::<Vec<_>>();
            let placement = ReplicatedSmallObjectPlacement::new(
                replica_nodes,
                request.source.content_hash_sha256,
                source_size,
                request.source.source_hlc,
            )?;
            self.metadata
                .put_object_layout(
                    &request.source.object,
                    &ObjectDataLayout::ReplicatedSmall(placement),
                )
                .await?;
            return Ok(EcTranscodeOutcome::SkippedSmall {
                object: request.source.object.clone(),
                size: source_size,
                min_object_size: request.erasure_set.profile.min_object_size,
            });
        }

        let _permit = self.backpressure.acquire_transcode().await?;
        let acquired = self
            .metadata
            .try_acquire_transcode_lease(
                operation_id,
                request.owner_node_id,
                request.now_nanos,
                self.config.lease_ttl_nanos(),
            )
            .await?;
        if !acquired {
            return Err(EcTranscodeError::LeaseBusy { operation_id });
        }
        let heartbeat = self.start_lease_heartbeat(operation_id, request.owner_node_id);

        let result = self
            .transcode_with_lease(
                &request,
                operation_id,
                attempt,
                shard_store,
                manifest_quorum,
                cutover,
            )
            .await;

        heartbeat.abort();
        let _ = heartbeat.await;
        if let Err(error) = &result {
            self.record_failure(&request, operation_id, attempt, error).await?;
            ec_metrics::record_transcode_failure(request.erasure_set.set_id);
        }
        self.metadata
            .release_transcode_lease(operation_id, request.owner_node_id)
            .await?;
        result
    }

    /// Verify a committed manifest by reading shards back and decoding through the codec.
    pub async fn verify_manifest<S>(
        &self,
        manifest: &EcObjectManifest,
        shard_store: &S,
    ) -> Result<EcVerificationReport, EcTranscodeError>
    where
        S: EcShardStore,
    {
        manifest.validate()?;
        let codec_profile = manifest.profile.clone();
        let probe_indices = selected_probe_indices(
            manifest.original_size,
            self.config.full_verify_max_bytes,
            self.config.reconstruction_probe_chunks,
            &manifest.content_hash_sha256,
            manifest.chunks.len(),
        );

        let mut object_hasher = Sha256::new();
        let mut probed_chunks = 0usize;
        let mut verified_shards = 0usize;

        for chunk in &manifest.chunks {
            let verified = self.read_verified_chunk_shards(manifest, chunk, shard_store).await?;
            verified_shards = verified_shards.saturating_add(verified.iter().flatten().count());

            let decode_shards = verified.clone();
            let data_size = chunk.data_size;
            let profile_for_decode = codec_profile.clone();
            let decoded = self
                .codec_limiter
                .run_decode_or_repair(move || {
                    let codec = crate::ec_codec::build_codec(profile_for_decode)?;
                    codec.decode_chunk(&decode_shards, data_size)
                })
                .await?;
            if sha256(&decoded) != chunk.chunk_sha256 {
                return Err(EcTranscodeError::ReconstructionProbe {
                    reason: format!("chunk {} decoded SHA-256 mismatch", chunk.chunk_index),
                });
            }
            object_hasher.update(&decoded);

            if probe_indices.contains(&chunk.chunk_index) {
                self.probe_chunk(manifest, chunk.chunk_index, &verified).await?;
                probed_chunks = probed_chunks.saturating_add(1);
            }
        }

        let actual: [u8; 32] = object_hasher.finalize().into();
        if actual != manifest.content_hash_sha256 {
            return Err(EcTranscodeError::ReconstructionProbe {
                reason: "full-object EC SHA-256 verification failed".to_string(),
            });
        }

        Ok(EcVerificationReport {
            probed_chunks,
            verified_shards,
            full_object_sha256_verified: true,
        })
    }

    /// Try to purge hot replicas after EC cutover.
    ///
    /// `erasure_set_id` comes from the manifest the caller already resolved
    /// rather than from the GC record: the record is durable and predates
    /// multi-set pools, and widening it would change bytes already on disk.
    pub async fn maybe_purge_hot_replicas<C>(
        &self,
        mut record: EcHotReplicaGcRecord,
        manifest_state: EcPlacementState,
        erasure_set_id: u64,
        now_nanos: u64,
        cutover: &C,
    ) -> Result<EcHotReplicaPurgeDecision, EcTranscodeError>
    where
        C: EcObjectCutover,
    {
        record.validate()?;
        if record.purged_at.is_some() {
            return Ok(EcHotReplicaPurgeDecision::AlreadyPurged);
        }
        if self.config.hot_purge_mode == EcHotPurgeMode::Manual {
            return Ok(EcHotReplicaPurgeDecision::ManualMode);
        }
        if manifest_state != EcPlacementState::EcCommitted {
            return Ok(EcHotReplicaPurgeDecision::NotCommitted);
        }
        if !record.full_object_sha256_verified {
            return Ok(EcHotReplicaPurgeDecision::NeedsFullVerification);
        }
        if now_nanos < record.eligible_at {
            return Ok(EcHotReplicaPurgeDecision::GracePeriod);
        }

        cutover
            .purge_hot_replicas(&record.object, record.source_hlc, record.operation_id)
            .await?;
        record.purged_at = Some(now_nanos);
        record.updated_at = now_nanos;
        self.metadata.put_hot_replica_gc(&record).await?;
        ec_metrics::record_hot_replicas_purged(erasure_set_id);
        Ok(EcHotReplicaPurgeDecision::Purged)
    }

    /// The CPU limiter this worker encodes and decodes under.
    ///
    /// Shared with the packed-segment release probe so that reading a segment
    /// back out of EC is charged to the same budget as encoding it was, rather
    /// than opening a second, unbounded one beside it.
    pub fn codec_limiter(&self) -> EcCodecCpuLimiter {
        self.codec_limiter.clone()
    }

    async fn transcode_with_lease<S, M, C>(
        &self,
        request: &EcTranscodeRequest,
        operation_id: Uuid,
        attempt: u32,
        shard_store: &S,
        manifest_quorum: &M,
        cutover: &C,
    ) -> Result<EcTranscodeOutcome, EcTranscodeError>
    where
        S: EcShardStore,
        M: EcManifestQuorum,
        C: EcObjectCutover,
    {
        self.put_job_state(
            request,
            operation_id,
            attempt,
            EcPlacementState::EcTranscodeQueued,
            None,
            None,
        )
        .await?;
        self.put_job_state(
            request,
            operation_id,
            attempt,
            EcPlacementState::EcEncoding,
            None,
            None,
        )
        .await?;

        let mut manifest = self.encode_and_write_shards(request, operation_id, shard_store).await?;

        self.put_job_state(
            request,
            operation_id,
            attempt,
            EcPlacementState::EcShardsDurable,
            None,
            None,
        )
        .await?;

        if crate::ec_test_hooks::take_fail_reconstruction_after_shards(&request.source.object) {
            return Err(EcTranscodeError::ReconstructionProbe {
                reason:
                    "deterministic EE test hook failed reconstruction after durable shard write"
                        .to_string(),
            });
        }

        self.commit_manifest_state(
            &mut manifest,
            EcPlacementState::EcManifestCommitted,
            manifest_quorum,
        )
        .await?;
        self.put_job_state(
            request,
            operation_id,
            attempt,
            EcPlacementState::EcManifestCommitted,
            None,
            None,
        )
        .await?;

        let verification = self.verify_manifest(&manifest, shard_store).await?;
        if !verification.full_object_sha256_verified {
            return Err(EcTranscodeError::ReconstructionProbe {
                reason: "full-object SHA-256 verification did not complete".to_string(),
            });
        }

        self.commit_manifest_state(
            &mut manifest,
            EcPlacementState::EcReadVerified,
            manifest_quorum,
        )
        .await?;
        self.put_job_state(
            request,
            operation_id,
            attempt,
            EcPlacementState::EcReadVerified,
            None,
            None,
        )
        .await?;

        let mut committed_manifest = manifest.clone();
        committed_manifest.state = EcPlacementState::EcCommitted;
        committed_manifest.validate()?;
        let manifest_ref = EcObjectManifestRef::from_manifest(&committed_manifest)?;

        // A segment ends the pipeline here. There is no single object layout to
        // guard and nothing to cut over yet: the objects inside it keep reading
        // from their hot copies until every one of them has been read back out
        // of EC. Releasing them is `EC2-PACK-04-004`, and it is deliberately not
        // this worker's business.
        if request.target.is_packed_segment() {
            manifest = committed_manifest;
            self.commit_manifest_state(
                &mut manifest,
                EcPlacementState::EcCommitted,
                manifest_quorum,
            )
            .await?;
            self.mark_packed_segment_encoded(&request.source.object).await?;
            self.put_job_state(
                request,
                operation_id,
                attempt,
                EcPlacementState::EcCommitted,
                None,
                None,
            )
            .await?;
            return Ok(EcTranscodeOutcome::PackedSegmentEncoded {
                operation_id,
                manifest: Box::new(manifest),
            });
        }

        let guard = EcCutoverGuard {
            source_hlc: request.source.source_hlc,
            content_hash_sha256: request.source.content_hash_sha256,
            etag: request.source.etag.clone(),
            size: request.source.size()?,
        };
        match cutover.cutover_to_ec(guard, manifest_ref.clone()).await? {
            EcCutoverOutcome::Superseded => {
                self.put_job_state(
                    request,
                    operation_id,
                    attempt,
                    EcPlacementState::EcFailedRetryable,
                    Some(EcTranscodeFailureRecord::retryable(
                        "concurrent PUT won before EC cutover",
                    )),
                    Some(
                        request
                            .now_nanos
                            .saturating_add(self.config.retry_delay_nanos(attempt, operation_id)),
                    ),
                )
                .await?;
                return Ok(EcTranscodeOutcome::Superseded { operation_id });
            }
            EcCutoverOutcome::Committed => {}
        }

        manifest = committed_manifest;
        self.commit_manifest_state(
            &mut manifest,
            EcPlacementState::EcCommitted,
            manifest_quorum,
        )
        .await?;

        self.metadata
            .put_object_layout(
                &request.source.object,
                &crate::ec_metadata::ObjectDataLayout::ErasureCoded(manifest_ref),
            )
            .await?;

        let hot_replica_gc = EcHotReplicaGcRecord {
            object: request.source.object.clone(),
            operation_id,
            source_hlc: request.source.source_hlc,
            eligible_at: request.now_nanos.saturating_add(self.config.hot_replica_gc_grace_nanos()),
            full_object_sha256_verified: true,
            purged_at: None,
            updated_at: request.now_nanos,
        };
        self.metadata.put_hot_replica_gc(&hot_replica_gc).await?;
        cutover.mark_hot_replicas_gc_eligible(hot_replica_gc.clone()).await?;
        ec_metrics::record_hot_replicas_gc_eligible(manifest.erasure_set_id);
        self.put_job_state(
            request,
            operation_id,
            attempt,
            EcPlacementState::HotReplicasGcEligible,
            None,
            None,
        )
        .await?;

        Ok(EcTranscodeOutcome::Committed {
            operation_id,
            manifest: Box::new(manifest),
            hot_replica_gc,
        })
    }

    /// Record that a segment's shards are durable and verified.
    ///
    /// Split out so the pipeline's segment branch reads as one step, and so the
    /// state machine of `EC2-060` is enforced in one place: only a sealed
    /// segment can become encoded.
    async fn mark_packed_segment_encoded(
        &self,
        segment_key: &EcObjectVersionKey,
    ) -> Result<(), EcTranscodeError> {
        let segment_id = packed_segment_id_from_manifest_key(segment_key).map_err(|error| {
            EcTranscodeError::InvalidSource {
                reason: error.to_string(),
            }
        })?;
        let mut segment = self.metadata.get_packed_segment(segment_id).await?.ok_or_else(|| {
            EcTranscodeError::InvalidSource {
                reason: format!("packed segment {segment_id} has no record to mark encoded"),
            }
        })?;
        if segment.state == PackedSegmentState::Encoded {
            return Ok(());
        }
        segment.transition_to(PackedSegmentState::Encoded).map_err(|error| {
            EcTranscodeError::InvalidSource {
                reason: error.to_string(),
            }
        })?;
        self.metadata.put_packed_segment(&segment).await?;
        Ok(())
    }

    async fn encode_and_write_shards<S>(
        &self,
        request: &EcTranscodeRequest,
        operation_id: Uuid,
        shard_store: &S,
    ) -> Result<EcObjectManifest, EcTranscodeError>
    where
        S: EcShardStore,
    {
        let profile = request.erasure_set.profile.codec_snapshot();
        let chunk_size = usize::try_from(profile.chunk_size).map_err(|_| {
            EcTranscodeError::InvalidErasureSet {
                reason: "profile chunk_size does not fit in usize".to_string(),
            }
        })?;
        let mut chunks = Vec::new();

        for (chunk_index, chunk_bytes) in request.source.data.chunks(chunk_size).enumerate() {
            let chunk_index =
                u32::try_from(chunk_index).map_err(|_| EcTranscodeError::InvalidSource {
                    reason: "object has more chunks than u32 chunk_index supports".to_string(),
                })?;
            let chunk_vec = chunk_bytes.to_vec();
            let input_len =
                u64::try_from(chunk_vec.len()).map_err(|_| EcTranscodeError::InvalidSource {
                    reason: "chunk length does not fit in u64".to_string(),
                })?;
            let profile_for_encode = profile.clone();
            let encoded = self
                .codec_limiter
                .run_encode(input_len, move || {
                    // The only encode path of the runtime, and therefore the
                    // only one the foreign-matrix test hook may touch.
                    let codec = crate::ec_codec::build_encoding_codec(profile_for_encode)?;
                    codec.encode_chunk(&chunk_vec, chunk_index)
                })
                .await?;
            let chunk_manifest =
                self.write_encoded_chunk(request, operation_id, &encoded, shard_store).await?;
            chunks.push(chunk_manifest);
        }

        let original_size = request.source.size()?;
        Ok(EcObjectManifest {
            format_version: EC_MANIFEST_FORMAT_VERSION,
            bucket: request.source.object.bucket.clone(),
            key: request.source.object.key.clone(),
            version_id: request.source.object.version_id.clone(),
            original_size,
            content_hash_sha256: request.source.content_hash_sha256,
            etag: request.source.etag.clone(),
            content_type: request.source.content_type.clone(),
            user_metadata: request.source.user_metadata.clone(),
            profile,
            erasure_set_id: request.erasure_set.set_id,
            topology_epoch: request.erasure_set.topology_epoch,
            chunks,
            state: EcPlacementState::EcShardsDurable,
            hlc: request.source.source_hlc,
            origin_node_id: request.source.origin_node_id,
            operation_id,
            created_at: request.source.created_at,
            is_deleted: false,
            packed: None,
        })
    }

    async fn write_encoded_chunk<S>(
        &self,
        request: &EcTranscodeRequest,
        operation_id: Uuid,
        encoded: &EncodedChunk,
        shard_store: &S,
    ) -> Result<ChunkManifest, EcTranscodeError>
    where
        S: EcShardStore,
    {
        let mut shard_manifests = Vec::with_capacity(encoded.shards.len());
        for shard in &encoded.shards {
            let slot = request
                .erasure_set
                .slot_assignment
                .slots()
                .iter()
                .find(|slot| slot.slot_index == shard.slot_index)
                .ok_or_else(|| EcTranscodeError::InvalidErasureSet {
                    reason: format!("missing shard slot {}", shard.slot_index),
                })?;
            let shard_manifest = ShardManifest::from_codec_shard(shard, slot.node_id);
            let data = shard.data.to_vec();
            self.throttle_bytes(shard.size).await;
            let ack = shard_store
                .write_shard(EcShardWriteRequest {
                    object: request.source.object.clone(),
                    operation_id,
                    chunk_index: encoded.chunk_index,
                    slot_index: shard.slot_index,
                    node_id: slot.node_id,
                    role: shard.role,
                    data,
                    crc32: shard.crc32,
                    sha256: shard.sha256,
                })
                .await?;
            verify_shard_ack(&ack, &shard_manifest)?;
            shard_manifests.push(shard_manifest);
        }

        let chunk_bytes = &request.source.data[chunk_range(request, encoded)?];
        Ok(ChunkManifest {
            chunk_index: encoded.chunk_index,
            data_size: encoded.original_size,
            encoded_size: encoded.shard_size,
            chunk_sha256: sha256(chunk_bytes),
            shards: shard_manifests,
        })
    }

    /// Whether `read` is this same transcode, already past the step being
    /// written.
    ///
    /// Compared with the state normalised away, so the only difference allowed
    /// is how far along the ladder quorum is — the shards, the operation and
    /// every other field still have to be identical.
    fn manifest_is_same_work_further_on(
        &self,
        writing: &EcObjectManifest,
        read: &EcObjectManifest,
    ) -> Result<bool, EcTranscodeError> {
        if read.operation_id != writing.operation_id {
            return Ok(false);
        }
        if manifest_state_rank(read.state) <= manifest_state_rank(writing.state) {
            return Ok(false);
        }
        let mut as_read = writing.clone();
        as_read.state = read.state;
        Ok(EcObjectManifestRef::from_manifest(&as_read)?.matches_manifest(read)?)
    }

    async fn commit_manifest_state<M>(
        &self,
        manifest: &mut EcObjectManifest,
        state: EcPlacementState,
        manifest_quorum: &M,
    ) -> Result<(), EcTranscodeError>
    where
        M: EcManifestQuorum,
    {
        manifest.state = state;
        manifest.validate()?;
        let required_acks = manifest.profile.manifest_quorum();
        let commit = manifest_quorum.commit_manifest(manifest.clone(), required_acks).await?;
        if commit.acks < required_acks {
            ec_metrics::record_manifest_quorum_failure(manifest.erasure_set_id);
            return Err(EcTranscodeError::ManifestQuorum {
                reason: format!(
                    "manifest quorum requires {required_acks} ACK(s), got {}",
                    commit.acks
                ),
            });
        }
        self.metadata.put_manifest(manifest).await?;
        match manifest_quorum.quorum_read_manifest(&manifest.object_key()).await? {
            EcManifestReadResult::Present(read) => {
                let expected = EcObjectManifestRef::from_manifest(manifest)?;
                if !expected.matches_manifest(&read)? {
                    // A transcode that crashed between two steps and was
                    // retried meets its own work here. The hidden manifest is
                    // stored under an HLC derived from the state's rank, so
                    // quorum keeps the furthest state it ever saw: writing an
                    // earlier step again cannot take, and the read back cannot
                    // equal what was just sent. Treating that as divergence
                    // fails the job, and every retry after it, leaving the
                    // object at that state for good — which is what a crash
                    // right before the cutover used to do.
                    //
                    // So the same operation found further along its own ladder
                    // is not divergence: the pipeline adopts what quorum holds
                    // and goes on from there. Everything but the state must
                    // still match exactly, and the state must be ahead — a step
                    // back, another operation, or different bytes stay errors.
                    if self.manifest_is_same_work_further_on(manifest, &read)? {
                        *manifest = read;
                        return Ok(());
                    }
                    ec_metrics::record_manifest_quorum_failure(manifest.erasure_set_id);
                    return Err(EcTranscodeError::ManifestQuorum {
                        reason: "quorum read returned a divergent manifest".to_string(),
                    });
                }
            }
            EcManifestReadResult::Tombstone(_) => {
                ec_metrics::record_manifest_quorum_failure(manifest.erasure_set_id);
                return Err(EcTranscodeError::ManifestQuorum {
                    reason: "quorum read returned a tombstone during EC cutover".to_string(),
                });
            }
            EcManifestReadResult::Absent => {
                ec_metrics::record_manifest_quorum_failure(manifest.erasure_set_id);
                return Err(EcTranscodeError::ManifestQuorum {
                    reason: "quorum read did not find the committed manifest".to_string(),
                });
            }
        }
        Ok(())
    }

    async fn read_verified_chunk_shards<S>(
        &self,
        manifest: &EcObjectManifest,
        chunk: &ChunkManifest,
        shard_store: &S,
    ) -> Result<Vec<Option<Shard>>, EcTranscodeError>
    where
        S: EcShardStore,
    {
        let mut shards = vec![None; usize::from(manifest.profile.total_shards())];
        for shard_manifest in &chunk.shards {
            let bytes = shard_store
                .read_shard(EcShardReadRequest {
                    object: manifest.object_key(),
                    operation_id: manifest.operation_id,
                    chunk_index: chunk.chunk_index,
                    slot_index: shard_manifest.slot_index,
                    node_id: shard_manifest.node_id,
                })
                .await?;
            shard_manifest.verify_bytes(&bytes)?;
            let shard = Shard::new(
                shard_manifest.slot_index,
                chunk.chunk_index,
                shard_manifest.role,
                Bytes::from(bytes),
            )?;
            shards[usize::from(shard_manifest.slot_index)] = Some(shard);
        }
        Ok(shards)
    }

    async fn probe_chunk(
        &self,
        manifest: &EcObjectManifest,
        chunk_index: u32,
        verified: &[Option<Shard>],
    ) -> Result<(), EcTranscodeError> {
        let chunk = manifest
            .chunks
            .iter()
            .find(|chunk| chunk.chunk_index == chunk_index)
            .ok_or_else(|| EcTranscodeError::ReconstructionProbe {
                reason: format!("missing chunk {chunk_index} in manifest"),
            })?;
        let patterns = probe_loss_patterns(manifest);
        for missing_slots in patterns {
            let mut probe = verified.to_vec();
            for slot in missing_slots {
                let Some(entry) = probe.get_mut(usize::from(slot)) else {
                    return Err(EcTranscodeError::ReconstructionProbe {
                        reason: format!("probe slot {slot} is outside shard vector"),
                    });
                };
                *entry = None;
            }
            let profile = manifest.profile.clone();
            let data_size = chunk.data_size;
            let decoded = self
                .codec_limiter
                .run_decode_or_repair(move || {
                    // Part of the write: this probe reconstructs the shards the
                    // transcode just produced, so it has to use the same matrix
                    // they were produced with, foreign-matrix hook included.
                    let codec = crate::ec_codec::build_encoding_codec(profile)?;
                    codec.decode_chunk(&probe, data_size)
                })
                .await?;
            if sha256(&decoded) != chunk.chunk_sha256 {
                return Err(EcTranscodeError::ReconstructionProbe {
                    reason: format!("chunk {chunk_index} probe decoded wrong bytes"),
                });
            }
        }
        Ok(())
    }

    async fn next_attempt(&self, operation_id: Uuid) -> Result<u32, EcTranscodeError> {
        Ok(self
            .metadata
            .get_transcode_job(operation_id)
            .await?
            .map_or(1, |job| job.attempt.saturating_add(1)))
    }

    async fn put_job_state(
        &self,
        request: &EcTranscodeRequest,
        operation_id: Uuid,
        attempt: u32,
        state: EcPlacementState,
        failure: Option<EcTranscodeFailureRecord>,
        next_retry_at: Option<u64>,
    ) -> Result<(), EcTranscodeError> {
        let job = EcTranscodeJobRecord {
            object: request.source.object.clone(),
            operation_id,
            source_hlc: request.source.source_hlc,
            target_erasure_set_id: request.erasure_set.set_id,
            state,
            attempt,
            object_size: request.source.size()?,
            source_content_hash_sha256: request.source.content_hash_sha256,
            updated_at: request.now_nanos,
            next_retry_at,
            failure,
            origin: request.origin,
            target: request.target,
        };
        self.metadata.put_transcode_job(&job).await?;

        // The staged states are too short-lived to observe from outside, so the
        // crash-safety scenarios ask the pipeline to stop at a named one. The
        // call returns immediately unless a pause is armed, which is the only
        // path a build without EC test hooks ever takes; it is placed after the
        // job record is written so what the scenario sees is the state it asked
        // to stop at.
        crate::ec_test_hooks::pause_if_armed(
            &request.source.object,
            state.as_str(),
            &operation_id.to_string(),
            &request.owner_node_id.to_string(),
        )
        .await;

        Ok(())
    }

    async fn record_failure(
        &self,
        request: &EcTranscodeRequest,
        operation_id: Uuid,
        attempt: u32,
        error: &EcTranscodeError,
    ) -> Result<(), EcTranscodeError> {
        let class = error.failure_class();
        let exhausted = class == EcTranscodeFailureClass::Retryable
            && attempt >= self.config.max_retry_attempts;
        let final_class = if exhausted {
            EcTranscodeFailureClass::Permanent
        } else {
            class
        };
        let state = match final_class {
            EcTranscodeFailureClass::Retryable => EcPlacementState::EcFailedRetryable,
            EcTranscodeFailureClass::Permanent => EcPlacementState::EcFailedPermanent,
        };
        let reason = if exhausted {
            format!("retry budget exhausted after {attempt} attempt(s): {error}")
        } else {
            error.to_string()
        };
        let next_retry_at = if final_class == EcTranscodeFailureClass::Retryable {
            Some(
                request
                    .now_nanos
                    .saturating_add(self.config.retry_delay_nanos(attempt, operation_id)),
            )
        } else {
            None
        };
        self.put_job_state(
            request,
            operation_id,
            attempt,
            state,
            Some(EcTranscodeFailureRecord {
                class: final_class,
                reason,
            }),
            next_retry_at,
        )
        .await
    }

    async fn throttle_bytes(&self, bytes: u64) {
        let Some(limit) = self.config.transcode_bandwidth_limit else {
            return;
        };
        if limit == 0 || bytes == 0 {
            return;
        }
        let nanos = bytes.saturating_mul(1_000_000_000).div_ceil(limit);
        tokio::time::sleep(Duration::from_nanos(nanos)).await;
    }

    fn start_lease_heartbeat(
        &self,
        operation_id: Uuid,
        owner_node_id: NodeId,
    ) -> tokio::task::JoinHandle<()> {
        let metadata = Arc::clone(&self.metadata);
        let ttl_nanos = self.config.lease_ttl_nanos();
        let interval = Duration::from_nanos((ttl_nanos / 3).max(1));
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                let now = unix_time_nanos();
                match metadata
                    .renew_transcode_lease(operation_id, owner_node_id, now, ttl_nanos)
                    .await
                {
                    Ok(true) => {}
                    Ok(false) => {
                        tracing::warn!(
                            operation_id = %operation_id,
                            owner_node_id = %owner_node_id,
                            "EC transcode lease heartbeat lost ownership"
                        );
                        break;
                    }
                    Err(error) => {
                        tracing::warn!(
                            operation_id = %operation_id,
                            owner_node_id = %owner_node_id,
                            error = %error,
                            "EC transcode lease heartbeat failed"
                        );
                        break;
                    }
                }
            }
        })
    }
}

/// Compute a deterministic EC transcode operation ID.
pub fn deterministic_operation_id(
    object: &EcObjectVersionKey,
    source_hlc: HlcTimestamp,
    target_erasure_set_id: u64,
) -> Uuid {
    let mut hasher = Sha256::new();
    hasher.update(b"s4-ec-transcode-v1");
    hash_string(&mut hasher, &object.bucket);
    hash_string(&mut hasher, &object.key);
    match &object.version_id {
        Some(version_id) => {
            hasher.update([1]);
            hash_string(&mut hasher, version_id);
        }
        None => hasher.update([0]),
    }
    hasher.update(source_hlc.wall_time.to_be_bytes());
    hasher.update(source_hlc.logical.to_be_bytes());
    hasher.update(source_hlc.node_id.to_be_bytes());
    hasher.update(target_erasure_set_id.to_be_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    Uuid::from_bytes(bytes)
}

fn verify_shard_ack(
    ack: &EcShardWriteAck,
    manifest: &ShardManifest,
) -> Result<(), EcTranscodeError> {
    if ack.node_id != manifest.node_id
        || ack.slot_index != manifest.slot_index
        || ack.size != manifest.size
        || ack.crc32 != manifest.crc32
        || ack.sha256 != manifest.sha256
    {
        return Err(EcTranscodeError::ShardVerification {
            reason: format!(
                "durable ACK for chunk {} slot {} does not match manifest",
                ack.chunk_index, ack.slot_index
            ),
        });
    }
    Ok(())
}

fn selected_probe_indices(
    original_size: u64,
    full_verify_max_bytes: u64,
    extra_probes: usize,
    content_hash: &[u8; 32],
    chunk_count: usize,
) -> BTreeSet<u32> {
    let mut selected = BTreeSet::new();
    if chunk_count == 0 {
        return selected;
    }
    if original_size <= full_verify_max_bytes {
        for index in 0..chunk_count {
            if let Ok(index) = u32::try_from(index) {
                selected.insert(index);
            }
        }
        return selected;
    }

    selected.insert(0);
    if let Ok(last) = u32::try_from(chunk_count - 1) {
        selected.insert(last);
    }

    let mut seed = u64::from_be_bytes(content_hash[..8].try_into().unwrap_or([0; 8]));
    let target = chunk_count.min(extra_probes.saturating_add(2));
    while selected.len() < target {
        seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
        let index = (seed % chunk_count as u64) as u32;
        selected.insert(index);
    }
    selected
}

fn probe_loss_patterns(manifest: &EcObjectManifest) -> Vec<Vec<u8>> {
    match manifest.profile.parity {
        EcParity::ReedSolomon { m } => {
            vec![deterministic_missing_slots(
                manifest.profile.total_shards(),
                m,
                &manifest.content_hash_sha256,
            )]
        }
        EcParity::Lrc { .. } => {
            let Ok(codec) = crate::ec_codec::build_codec(manifest.profile.clone()) else {
                return Vec::new();
            };
            // Fast-path patterns only. The probe exists to show that the
            // shards just written fit back together, and it decodes up to
            // three chunks of every transcoded object; adding the solver's
            // patterns would charge the write path for a recovery no write
            // ever performs (decision EC2-103, section 6).
            codec
                .failure_matrix()
                .entries
                .into_iter()
                .filter(|entry| {
                    entry.recoverable && entry.recovery_path != RecoveryPath::UnifiedSolver
                })
                .map(|entry| entry.missing_slots)
                .collect()
        }
    }
}

fn deterministic_missing_slots(
    total_shards: u8,
    missing_count: u8,
    seed_bytes: &[u8; 32],
) -> Vec<u8> {
    let mut selected = BTreeSet::new();
    let mut seed = u64::from_be_bytes(seed_bytes[8..16].try_into().unwrap_or([0; 8]));
    while selected.len() < usize::from(missing_count).min(usize::from(total_shards)) {
        seed = seed.wrapping_mul(1_442_695_040_888_963_407).wrapping_add(11);
        selected.insert((seed % u64::from(total_shards)) as u8);
    }
    selected.into_iter().collect()
}

fn chunk_range(
    request: &EcTranscodeRequest,
    encoded: &EncodedChunk,
) -> Result<std::ops::Range<usize>, EcTranscodeError> {
    let chunk_size = usize::try_from(request.erasure_set.profile.chunk_size).map_err(|_| {
        EcTranscodeError::InvalidErasureSet {
            reason: "profile chunk_size does not fit in usize".to_string(),
        }
    })?;
    let start = usize::try_from(encoded.chunk_index)
        .ok()
        .and_then(|index| index.checked_mul(chunk_size))
        .ok_or_else(|| EcTranscodeError::InvalidSource {
            reason: "chunk range start overflowed".to_string(),
        })?;
    let data_size =
        usize::try_from(encoded.original_size).map_err(|_| EcTranscodeError::InvalidSource {
            reason: "encoded original_size does not fit in usize".to_string(),
        })?;
    let end = start.checked_add(data_size).ok_or_else(|| EcTranscodeError::InvalidSource {
        reason: "chunk range end overflowed".to_string(),
    })?;
    Ok(start..end)
}

fn hash_string(hasher: &mut Sha256, value: &str) {
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value.as_bytes());
}

/// Lower-case hex, for an internal identifier a person may have to read.
fn hex_lower(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn sha256(bytes: &[u8]) -> [u8; 32] {
    Sha256::new().chain_update(bytes).finalize().into()
}

fn seconds_to_nanos(seconds: u64) -> u64 {
    seconds.saturating_mul(1_000_000_000)
}

fn unix_time_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| {
            duration
                .as_secs()
                .saturating_mul(1_000_000_000)
                .saturating_add(u64::from(duration.subsec_nanos()))
        })
        .unwrap_or(0)
}

fn deterministic_jitter_secs(operation_id: Uuid, attempt: u32, base_secs: u64) -> u64 {
    if base_secs <= 1 {
        return 0;
    }
    let mut hasher = Sha256::new();
    hasher.update(operation_id.as_bytes());
    hasher.update(attempt.to_be_bytes());
    let digest = hasher.finalize();
    let jitter_space = (base_secs / 2).max(1);
    u64::from_be_bytes(digest[..8].try_into().unwrap_or([0; 8])) % jitter_space
}

fn invalid_config(name: &'static str, reason: impl Into<String>) -> EcTranscodeError {
    EcTranscodeError::InvalidConfig {
        name,
        reason: reason.into(),
    }
}

fn parse_optional_bool(name: &'static str) -> Result<Option<bool>, EcTranscodeError> {
    match std::env::var(name) {
        Ok(raw) => match raw.as_str() {
            "1" | "true" | "TRUE" | "yes" | "YES" => Ok(Some(true)),
            "0" | "false" | "FALSE" | "no" | "NO" => Ok(Some(false)),
            _ => Err(invalid_config(name, "must be a boolean")),
        },
        Err(_) => Ok(None),
    }
}

fn parse_optional_u32(name: &'static str) -> Result<Option<u32>, EcTranscodeError> {
    match std::env::var(name) {
        Ok(raw) => raw
            .parse::<u32>()
            .map(Some)
            .map_err(|error| invalid_config(name, error.to_string())),
        Err(_) => Ok(None),
    }
}

fn parse_optional_u64(name: &'static str) -> Result<Option<u64>, EcTranscodeError> {
    match std::env::var(name) {
        Ok(raw) => raw
            .parse::<u64>()
            .map(Some)
            .map_err(|error| invalid_config(name, error.to_string())),
        Err(_) => Ok(None),
    }
}

fn parse_optional_usize(name: &'static str) -> Result<Option<usize>, EcTranscodeError> {
    match std::env::var(name) {
        Ok(raw) => raw
            .parse::<usize>()
            .map(Some)
            .map_err(|error| invalid_config(name, error.to_string())),
        Err(_) => Ok(None),
    }
}

/// Errors from the staged EC transcode path.
#[derive(Debug, Error)]
pub enum EcTranscodeError {
    /// Invalid environment or worker configuration.
    #[error("invalid {name}: {reason}")]
    InvalidConfig {
        /// Environment variable or setting name.
        name: &'static str,
        /// Human-readable validation failure.
        reason: String,
    },

    /// Inline EC write is disabled.
    #[error("inline EC write is disabled")]
    InlineWriteDisabled,

    /// EC worker rejected work before it could overrun memory/CPU.
    #[error("EC transcode backpressure: {reason}")]
    Backpressure {
        /// Human-readable backpressure reason.
        reason: String,
    },

    /// Another worker owns the active transcode lease.
    #[error("EC transcode lease is busy for operation {operation_id}")]
    LeaseBusy {
        /// Operation whose lease is already owned.
        operation_id: Uuid,
    },

    /// Invalid hot RF=3 source object.
    #[error("invalid EC source object: {reason}")]
    InvalidSource {
        /// Human-readable validation failure.
        reason: String,
    },

    /// Source bytes no longer match the RF=3 source metadata hash.
    #[error("source object SHA-256 does not match RF=3 metadata")]
    SourceHashMismatch,

    /// Target erasure set is invalid for writes.
    #[error("invalid EC erasure set: {reason}")]
    InvalidErasureSet {
        /// Human-readable validation failure.
        reason: String,
    },

    /// Shard write or read failed.
    #[error("EC shard IO failed: {reason}")]
    ShardIo {
        /// Human-readable IO failure.
        reason: String,
    },

    /// Shard bytes or ACK did not match the manifest.
    #[error("EC shard verification failed: {reason}")]
    ShardVerification {
        /// Human-readable verification failure.
        reason: String,
    },

    /// Manifest quorum commit/read failed.
    #[error("EC manifest quorum failed: {reason}")]
    ManifestQuorum {
        /// Human-readable quorum failure.
        reason: String,
    },

    /// Reconstruction probe or full EC SHA-256 verification failed.
    #[error("EC reconstruction verification failed: {reason}")]
    ReconstructionProbe {
        /// Human-readable verification failure.
        reason: String,
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

impl EcTranscodeError {
    fn failure_class(&self) -> EcTranscodeFailureClass {
        match self {
            Self::InvalidConfig { .. }
            | Self::InlineWriteDisabled
            | Self::InvalidSource { .. }
            | Self::SourceHashMismatch
            | Self::InvalidErasureSet { .. }
            | Self::ReconstructionProbe { .. }
            | Self::Codec(_) => EcTranscodeFailureClass::Permanent,
            Self::Backpressure { .. }
            | Self::LeaseBusy { .. }
            | Self::ShardIo { .. }
            | Self::ShardVerification { .. }
            | Self::ManifestQuorum { .. }
            | Self::Metadata(_)
            | Self::Cpu(_) => EcTranscodeFailureClass::Retryable,
        }
    }
}

#[cfg(test)]
mod tests {
    /// The sentinel is a value, not a hash, and has to read as one.
    #[test]
    fn the_absent_content_hash_is_told_apart_from_a_real_one() {
        assert!(content_hash_is_absent(&ABSENT_CONTENT_HASH_SHA256));
        assert!(content_hash_is_absent(&[0u8; 32]));
        assert!(!content_hash_is_absent(&[1u8; 32]));
    }

    use super::*;
    use std::sync::Mutex;

    use serde::Serialize;

    use crate::codec_runtime::{EcCodecCpuConfig, EcCodecCpuLimiter};
    use crate::ec_packed_segments::{
        PackedObjectIdentity, PackedSegmentConfig, PackedSegmentId, SealReason,
    };
    use crate::erasure_coding::{
        legacy_pool_set_id, EcCodecKind as EeCodecKind, EcParity as EeParity, EcProfile,
        EcTopologyPolicy, TopologyLabels, FIRST_SET_ID,
    };
    use s4_cluster::PoolId;
    use std::num::NonZeroUsize;

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

    fn test_profile() -> EcProfile {
        EcProfile {
            name: "test-rs".to_string(),
            codec: EeCodecKind::ReedSolomon,
            k: 4,
            parity: EeParity::ReedSolomon { m: 2 },
            min_object_size: 1,
            chunk_size: 17,
        }
    }

    /// A set of the pool at the profile's full width, over `width` nodes
    /// starting at `first_node`.
    fn set_on_nodes(set_id: u64, first_node: u8) -> ErasureSet {
        let profile = test_profile();
        let nodes = (first_node..first_node + profile.total_shards()).map(node).collect::<Vec<_>>();
        ErasureSet::active(
            PoolId::new(9),
            set_id,
            profile,
            &nodes,
            &HashMap::<NodeId, TopologyLabels>::new(),
            EcTopologyPolicy::Strict,
            3,
            1,
        )
        .unwrap()
    }

    fn active_set() -> ErasureSet {
        // Kept at the value EC v1 derived from the pool so the fixtures that
        // name `erasure_set_id: 9` keep meaning the same set.
        set_on_nodes(legacy_pool_set_id(PoolId::new(9)), 1)
    }

    /// A second set of the same pool, on nodes of its own.
    fn additional_set() -> ErasureSet {
        set_on_nodes(FIRST_SET_ID, 1 + test_profile().total_shards())
    }

    fn source(data: Vec<u8>) -> EcSourceObject {
        EcSourceObject {
            object: EcObjectVersionKey::new("photos", "raw.bin", Some("v1".to_string())),
            content_hash_sha256: sha256(&data),
            data,
            etag: "\"etag\"".to_string(),
            content_type: "application/octet-stream".to_string(),
            user_metadata: HashMap::new(),
            source_hlc: hlc(),
            origin_node_id: node(1),
            created_at: 10,
        }
    }

    fn worker(store: Arc<EcMetadataStore>) -> EcStagedTranscodeWorker {
        let config = EcTranscodeConfig {
            full_verify_max_bytes: 32,
            hot_replica_gc_grace_secs: 10,
            ..EcTranscodeConfig::default()
        };
        let backpressure = EcWriteBackpressure::new(config);
        let cpu = EcCodecCpuLimiter::new(EcCodecCpuConfig {
            workers: NonZeroUsize::new(1).unwrap(),
            cpu_budget_percent: 60,
            max_inflight_encode_bytes: 1024 * 1024,
        });
        EcStagedTranscodeWorker::new(store, cpu, backpressure)
    }

    #[derive(Default)]
    struct MemoryShardStore {
        shards: Mutex<HashMap<(u32, u8), Vec<u8>>>,
        corrupt_on_read: Mutex<Option<(u32, u8)>>,
    }

    #[async_trait]
    impl EcShardStore for MemoryShardStore {
        async fn write_shard(
            &self,
            request: EcShardWriteRequest,
        ) -> Result<EcShardWriteAck, EcTranscodeError> {
            let size = u64::try_from(request.data.len()).unwrap();
            self.shards
                .lock()
                .unwrap()
                .insert((request.chunk_index, request.slot_index), request.data);
            Ok(EcShardWriteAck {
                node_id: request.node_id,
                chunk_index: request.chunk_index,
                slot_index: request.slot_index,
                size,
                crc32: request.crc32,
                sha256: request.sha256,
            })
        }

        async fn read_shard(
            &self,
            request: EcShardReadRequest,
        ) -> Result<Vec<u8>, EcTranscodeError> {
            let mut data = self
                .shards
                .lock()
                .unwrap()
                .get(&(request.chunk_index, request.slot_index))
                .cloned()
                .ok_or_else(|| EcTranscodeError::ShardIo {
                    reason: "missing test shard".to_string(),
                })?;
            if *self.corrupt_on_read.lock().unwrap()
                == Some((request.chunk_index, request.slot_index))
            {
                data[0] ^= 0xff;
            }
            Ok(data)
        }
    }

    #[derive(Default)]
    struct MemoryManifestQuorum {
        manifest: Mutex<Option<EcObjectManifest>>,
        commits: Mutex<Vec<EcPlacementState>>,
        forced_acks: Mutex<Option<u8>>,
    }

    #[async_trait]
    impl EcManifestQuorum for MemoryManifestQuorum {
        async fn commit_manifest(
            &self,
            manifest: EcObjectManifest,
            required_acks: u8,
        ) -> Result<EcManifestCommitResult, EcTranscodeError> {
            self.commits.lock().unwrap().push(manifest.state);
            *self.manifest.lock().unwrap() = Some(manifest);
            Ok(EcManifestCommitResult {
                acks: self.forced_acks.lock().unwrap().unwrap_or(required_acks),
            })
        }

        async fn quorum_read_manifest(
            &self,
            _object: &EcObjectVersionKey,
        ) -> Result<EcManifestReadResult, EcTranscodeError> {
            Ok(self
                .manifest
                .lock()
                .unwrap()
                .clone()
                .map(EcManifestReadResult::Present)
                .unwrap_or(EcManifestReadResult::Absent))
        }
    }

    #[derive(Default)]
    struct MemoryCutover {
        outcome: Mutex<EcCutoverOutcome>,
        cutover_ref: Mutex<Option<EcObjectManifestRef>>,
        gc_records: Mutex<Vec<EcHotReplicaGcRecord>>,
        purged: Mutex<Vec<EcObjectVersionKey>>,
    }

    #[async_trait]
    impl EcObjectCutover for MemoryCutover {
        async fn cutover_to_ec(
            &self,
            _guard: EcCutoverGuard,
            manifest_ref: EcObjectManifestRef,
        ) -> Result<EcCutoverOutcome, EcTranscodeError> {
            *self.cutover_ref.lock().unwrap() = Some(manifest_ref);
            Ok(*self.outcome.lock().unwrap())
        }

        async fn mark_hot_replicas_gc_eligible(
            &self,
            record: EcHotReplicaGcRecord,
        ) -> Result<(), EcTranscodeError> {
            self.gc_records.lock().unwrap().push(record);
            Ok(())
        }

        async fn purge_hot_replicas(
            &self,
            object: &EcObjectVersionKey,
            _expected_source_hlc: HlcTimestamp,
            _operation_id: Uuid,
        ) -> Result<(), EcTranscodeError> {
            self.purged.lock().unwrap().push(object.clone());
            Ok(())
        }
    }

    #[test]
    fn deterministic_operation_id_uses_source_hlc_and_target_set() {
        let object = EcObjectVersionKey::new("b", "k", Some("v1".to_string()));
        let first = deterministic_operation_id(&object, hlc(), 9);
        let second = deterministic_operation_id(&object, hlc(), 9);
        assert_eq!(first, second);

        let mut newer = hlc();
        newer.logical += 1;
        assert_ne!(first, deterministic_operation_id(&object, newer, 9));
        assert_ne!(first, deterministic_operation_id(&object, hlc(), 10));
    }

    #[tokio::test]
    async fn staged_transcode_commits_after_verification_and_sets_gc_gate() {
        let dir = tempfile::tempdir().unwrap();
        let metadata = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let worker = worker(metadata.clone());
        let shard_store = MemoryShardStore::default();
        let manifest_quorum = MemoryManifestQuorum::default();
        let cutover = MemoryCutover::default();
        let request = EcTranscodeRequest {
            source: source((0..128).map(|idx| (idx % 251) as u8).collect()),
            erasure_set: active_set(),
            owner_node_id: node(1),
            origin: EcTranscodeOrigin::ClientWrite,
            target: EcTranscodeTarget::Object,
            now_nanos: 100,
        };

        let outcome = worker
            .transcode(request.clone(), &shard_store, &manifest_quorum, &cutover)
            .await
            .unwrap();

        let EcTranscodeOutcome::Committed {
            operation_id,
            manifest,
            hot_replica_gc,
        } = outcome
        else {
            panic!("expected committed outcome");
        };
        assert_eq!(manifest.state, EcPlacementState::EcCommitted);
        assert_eq!(hot_replica_gc.eligible_at, 10_000_000_100);
        assert_eq!(hot_replica_gc.source_hlc, request.source.source_hlc);
        assert!(hot_replica_gc.full_object_sha256_verified);
        assert_eq!(
            manifest_quorum.commits.lock().unwrap().as_slice(),
            [
                EcPlacementState::EcManifestCommitted,
                EcPlacementState::EcReadVerified,
                EcPlacementState::EcCommitted
            ]
        );

        let job = metadata.get_transcode_job(operation_id).await.unwrap().unwrap();
        assert_eq!(job.state, EcPlacementState::HotReplicasGcEligible);
        assert!(job.failure.is_none());
        assert!(metadata.get_object_layout(&request.source.object).await.unwrap().is_some());
    }

    /// The set placement chose is what the manifest carries, and it is the only
    /// set the manifest validates against. Without that an object placed into a
    /// second set would be verified, repaired and scrubbed against the nodes of
    /// the first one — every shard would read as missing.
    #[tokio::test]
    async fn transcode_records_the_chosen_set_in_the_manifest() {
        let dir = tempfile::tempdir().unwrap();
        let metadata = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let worker = worker(metadata.clone());
        let shard_store = MemoryShardStore::default();
        let manifest_quorum = MemoryManifestQuorum::default();
        let cutover = MemoryCutover::default();
        let target = additional_set();
        let request = EcTranscodeRequest {
            source: source((0..128).map(|idx| (idx % 251) as u8).collect()),
            erasure_set: target.clone(),
            owner_node_id: node(1),
            origin: EcTranscodeOrigin::ClientWrite,
            target: EcTranscodeTarget::Object,
            now_nanos: 100,
        };

        let outcome = worker
            .transcode(request, &shard_store, &manifest_quorum, &cutover)
            .await
            .unwrap();

        let EcTranscodeOutcome::Committed { manifest, .. } = outcome else {
            panic!("expected committed outcome");
        };
        assert_eq!(manifest.erasure_set_id, target.set_id);
        assert_ne!(manifest.erasure_set_id, active_set().set_id);

        // Read back from the committed copy rather than from the value in hand:
        // the field has to survive the commit, not merely be set before it.
        let stored = manifest_quorum.manifest.lock().unwrap().clone().unwrap();
        assert_eq!(stored.erasure_set_id, target.set_id);
        stored.validate_against_erasure_set(&target).unwrap();
        assert!(stored.validate_against_erasure_set(&active_set()).is_err());
    }

    #[tokio::test]
    async fn superseded_transcode_does_not_publish_committed_manifest() {
        let dir = tempfile::tempdir().unwrap();
        let metadata = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let worker = worker(metadata.clone());
        let shard_store = MemoryShardStore::default();
        let manifest_quorum = MemoryManifestQuorum::default();
        let cutover = MemoryCutover::default();
        *cutover.outcome.lock().unwrap() = EcCutoverOutcome::Superseded;
        let request = EcTranscodeRequest {
            source: source((0..128).map(|idx| (idx % 251) as u8).collect()),
            erasure_set: active_set(),
            owner_node_id: node(1),
            origin: EcTranscodeOrigin::ClientWrite,
            target: EcTranscodeTarget::Object,
            now_nanos: 100,
        };
        let operation_id = deterministic_operation_id(
            &request.source.object,
            request.source.source_hlc,
            request.erasure_set.set_id,
        );

        let outcome = worker
            .transcode(request.clone(), &shard_store, &manifest_quorum, &cutover)
            .await
            .unwrap();

        assert_eq!(outcome, EcTranscodeOutcome::Superseded { operation_id });
        assert_eq!(
            manifest_quorum.commits.lock().unwrap().as_slice(),
            [
                EcPlacementState::EcManifestCommitted,
                EcPlacementState::EcReadVerified
            ]
        );
        assert_eq!(
            metadata.get_manifest(&request.source.object).await.unwrap().unwrap().state,
            EcPlacementState::EcReadVerified
        );
        assert!(metadata.get_object_layout(&request.source.object).await.unwrap().is_none());

        let job = metadata.get_transcode_job(operation_id).await.unwrap().unwrap();
        assert_eq!(job.state, EcPlacementState::EcFailedRetryable);
    }

    #[tokio::test]
    async fn zero_byte_object_skips_to_replicated_small_path() {
        let dir = tempfile::tempdir().unwrap();
        let metadata = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let worker = worker(metadata.clone());
        let shard_store = MemoryShardStore::default();
        let manifest_quorum = MemoryManifestQuorum::default();
        let cutover = MemoryCutover::default();
        let request = EcTranscodeRequest {
            source: source(Vec::new()),
            erasure_set: active_set(),
            owner_node_id: node(1),
            origin: EcTranscodeOrigin::ClientWrite,
            target: EcTranscodeTarget::Object,
            now_nanos: 100,
        };
        let operation_id = deterministic_operation_id(
            &request.source.object,
            request.source.source_hlc,
            request.erasure_set.set_id,
        );

        let outcome = worker
            .transcode(request, &shard_store, &manifest_quorum, &cutover)
            .await
            .unwrap();

        assert_eq!(
            outcome,
            EcTranscodeOutcome::SkippedSmall {
                object: EcObjectVersionKey::new("photos", "raw.bin", Some("v1".to_string())),
                size: 0,
                min_object_size: 1
            }
        );
        assert!(manifest_quorum.commits.lock().unwrap().is_empty());
        assert!(metadata.get_transcode_job(operation_id).await.unwrap().is_none());
        let layout = metadata
            .get_object_layout(&EcObjectVersionKey::new(
                "photos",
                "raw.bin",
                Some("v1".to_string()),
            ))
            .await
            .unwrap()
            .unwrap();
        let ObjectDataLayout::ReplicatedSmall(placement) = layout else {
            panic!("small object should persist replicated-small layout");
        };
        assert_eq!(placement.replica_nodes, vec![node(1), node(2), node(3)]);
        assert_eq!(placement.size, 0);
    }

    #[tokio::test]
    async fn source_hash_mismatch_is_permanent_failure() {
        let dir = tempfile::tempdir().unwrap();
        let metadata = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let worker = worker(metadata.clone());
        let shard_store = MemoryShardStore::default();
        let manifest_quorum = MemoryManifestQuorum::default();
        let cutover = MemoryCutover::default();
        let mut source = source(vec![1, 2, 3, 4, 5, 6]);
        source.content_hash_sha256 = [9; 32];
        let request = EcTranscodeRequest {
            source,
            erasure_set: active_set(),
            owner_node_id: node(1),
            origin: EcTranscodeOrigin::ClientWrite,
            target: EcTranscodeTarget::Object,
            now_nanos: 100,
        };
        let operation_id = deterministic_operation_id(
            &request.source.object,
            request.source.source_hlc,
            request.erasure_set.set_id,
        );

        let err = worker
            .transcode(request, &shard_store, &manifest_quorum, &cutover)
            .await
            .unwrap_err();

        assert!(matches!(err, EcTranscodeError::SourceHashMismatch));
        assert!(manifest_quorum.commits.lock().unwrap().is_empty());
        let job = metadata.get_transcode_job(operation_id).await.unwrap().unwrap();
        assert_eq!(job.state, EcPlacementState::EcFailedPermanent);
        assert_eq!(
            job.failure.unwrap().class,
            EcTranscodeFailureClass::Permanent
        );
    }

    #[tokio::test]
    async fn manifest_quorum_failure_records_retryable_state() {
        let dir = tempfile::tempdir().unwrap();
        let metadata = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let worker = worker(metadata.clone());
        let shard_store = MemoryShardStore::default();
        let manifest_quorum = MemoryManifestQuorum::default();
        *manifest_quorum.forced_acks.lock().unwrap() = Some(1);
        let cutover = MemoryCutover::default();
        let request = EcTranscodeRequest {
            source: source((0..64).map(|idx| (idx % 251) as u8).collect()),
            erasure_set: active_set(),
            owner_node_id: node(1),
            origin: EcTranscodeOrigin::ClientWrite,
            target: EcTranscodeTarget::Object,
            now_nanos: 100,
        };
        let operation_id = deterministic_operation_id(
            &request.source.object,
            request.source.source_hlc,
            request.erasure_set.set_id,
        );

        let err = worker
            .transcode(request, &shard_store, &manifest_quorum, &cutover)
            .await
            .unwrap_err();

        assert!(matches!(err, EcTranscodeError::ManifestQuorum { .. }));
        let job = metadata.get_transcode_job(operation_id).await.unwrap().unwrap();
        assert_eq!(job.state, EcPlacementState::EcFailedRetryable);
        assert_eq!(
            job.failure.unwrap().class,
            EcTranscodeFailureClass::Retryable
        );
        assert!(job.next_retry_at.is_some());
    }

    #[tokio::test]
    async fn hot_replica_purge_requires_verification_grace_and_committed_state() {
        let dir = tempfile::tempdir().unwrap();
        let metadata = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let worker = worker(metadata);
        let cutover = MemoryCutover::default();
        let object = EcObjectVersionKey::new("b", "k", None);
        let operation_id = deterministic_operation_id(&object, hlc(), 9);
        let mut record = EcHotReplicaGcRecord {
            object: object.clone(),
            operation_id,
            source_hlc: hlc(),
            eligible_at: 200,
            full_object_sha256_verified: false,
            purged_at: None,
            updated_at: 100,
        };

        let decision = worker
            .maybe_purge_hot_replicas(
                record.clone(),
                EcPlacementState::EcCommitted,
                9,
                300,
                &cutover,
            )
            .await
            .unwrap();
        assert_eq!(decision, EcHotReplicaPurgeDecision::NeedsFullVerification);

        record.full_object_sha256_verified = true;
        let decision = worker
            .maybe_purge_hot_replicas(
                record.clone(),
                EcPlacementState::EcCommitted,
                9,
                150,
                &cutover,
            )
            .await
            .unwrap();
        assert_eq!(decision, EcHotReplicaPurgeDecision::GracePeriod);

        let decision = worker
            .maybe_purge_hot_replicas(record, EcPlacementState::EcCommitted, 9, 300, &cutover)
            .await
            .unwrap();
        assert_eq!(decision, EcHotReplicaPurgeDecision::Purged);
        assert_eq!(cutover.purged.lock().unwrap().as_slice(), [object]);
    }

    fn deterministic_bytes(len: usize, shift: usize) -> Vec<u8> {
        (0..len).map(|idx| ((idx + shift * 37) % 251) as u8).collect()
    }

    fn test_packed_segment_config(target_bytes: u64) -> PackedSegmentConfig {
        PackedSegmentConfig {
            enabled: true,
            target_bytes: target_bytes.max(1024 * 1024),
            max_age: Duration::from_secs(300),
            max_objects: 100,
            max_open_segments: 4,
        }
    }

    fn build_open_segment(
        specs: &[(EcObjectVersionKey, usize)],
    ) -> (PackedSegment, Vec<(EcObjectVersionKey, Vec<u8>)>) {
        let total_bytes: u64 = specs.iter().map(|(_, len)| *len as u64).sum();
        let config = test_packed_segment_config(total_bytes);
        let mut segment = PackedSegment::open(PackedSegmentId::new_random(), 1, &config, 1000);
        let mut parts = Vec::new();
        for (i, (key, len)) in specs.iter().enumerate() {
            let bytes = deterministic_bytes(*len, i + 1);
            let hash = sha256(&bytes);
            let hlc_offset = match i {
                0 => 100,
                1 => 500,
                2 => 200,
                other => (other as u64 + 1) * 50,
            };
            let hlc = HlcTimestamp {
                wall_time: 1000 + hlc_offset,
                logical: 1,
                node_id: 1,
            };
            let reservation = segment.reserve(*len as u64).unwrap();
            let item = segment
                .journal_append(
                    &reservation,
                    PackedObjectIdentity::new(key.clone(), hash, hlc),
                )
                .unwrap();
            segment.commit(&reservation, &item).unwrap();
            parts.push((key.clone(), bytes));
        }
        (segment, parts)
    }

    fn build_sealed_segment(
        specs: &[(EcObjectVersionKey, usize)],
    ) -> (PackedSegment, Vec<(EcObjectVersionKey, Vec<u8>)>) {
        let (mut segment, parts) = build_open_segment(specs);
        segment.seal(2000, SealReason::TargetSizeReached).unwrap();
        (segment, parts)
    }

    #[test]
    fn a_sealed_segment_becomes_one_blob_the_pipeline_can_encode() {
        let specs = vec![
            (EcObjectVersionKey::new("bucket-a", "obj-1", None), 64),
            (EcObjectVersionKey::new("bucket-a", "obj-2", None), 128),
            (EcObjectVersionKey::new("bucket-a", "obj-3", None), 96),
        ];
        let (segment, parts) = build_sealed_segment(&specs);

        let source = EcSourceObject::for_packed_segment(&segment, &parts, node(1), 5000).unwrap();

        assert_eq!(source.object, segment.manifest_object_key());
        assert_eq!(source.data.len() as u64, segment.used_bytes);
        assert_eq!(source.content_hash_sha256, sha256(&source.data));
        assert_eq!(source.content_type, "application/octet-stream");
        assert_eq!(source.origin_node_id, node(1));
        assert_eq!(source.created_at, 5000);

        for (entry, (_, bytes)) in segment.entries.iter().zip(&parts) {
            let start = entry.offset as usize;
            let end = start + entry.length as usize;
            assert_eq!(&source.data[start..end], bytes.as_slice());
        }

        let max_hlc = segment.entries.iter().map(|e| e.hlc).max().unwrap();
        assert_eq!(source.source_hlc, max_hlc);
        assert_eq!(source.source_hlc.wall_time, 1500);
    }

    #[test]
    fn a_hole_left_by_a_deleted_object_is_encoded_as_zeroes() {
        let specs = vec![
            (EcObjectVersionKey::new("bucket-a", "obj-1", None), 64),
            (EcObjectVersionKey::new("bucket-a", "obj-2", None), 128),
            (EcObjectVersionKey::new("bucket-a", "obj-3", None), 96),
        ];
        let (mut segment, parts) = build_sealed_segment(&specs);
        let used_bytes_before = segment.used_bytes;
        assert_eq!(used_bytes_before, 64 + 128 + 96);

        let deleted_key = specs[1].0.clone();
        segment.record_object_deleted(&deleted_key).unwrap();

        assert_eq!(segment.used_bytes, used_bytes_before);
        assert_eq!(segment.dead_bytes, 128);
        assert_eq!(segment.entries.len(), 2);

        let live_parts: Vec<_> = parts.into_iter().filter(|(k, _)| k != &deleted_key).collect();
        assert_eq!(live_parts.len(), 2);

        let source =
            EcSourceObject::for_packed_segment(&segment, &live_parts, node(1), 5000).unwrap();

        assert_eq!(source.data.len() as u64, segment.used_bytes);
        assert_eq!(source.data.len() as u64, used_bytes_before);

        assert_eq!(&source.data[0..64], live_parts[0].1.as_slice());
        assert_eq!(&source.data[64..192], &[0u8; 128]);
        assert_eq!(&source.data[192..288], live_parts[1].1.as_slice());
        assert_eq!(source.content_hash_sha256, sha256(&source.data));
    }

    #[test]
    fn an_open_segment_cannot_be_encoded() {
        let specs = vec![(EcObjectVersionKey::new("bucket-a", "obj-1", None), 64)];
        let (segment, parts) = build_open_segment(&specs);
        assert_eq!(segment.state, PackedSegmentState::Open);

        let err = EcSourceObject::for_packed_segment(&segment, &parts, node(1), 5000).unwrap_err();
        match err {
            EcTranscodeError::InvalidSource { reason } => {
                assert!(
                    reason.contains("open"),
                    "error message '{reason}' must contain state 'open'"
                );
            }
            other => panic!("expected InvalidSource, got {other:?}"),
        }
    }

    #[test]
    fn a_segment_with_no_live_object_is_not_encoded() {
        let specs = vec![(EcObjectVersionKey::new("bucket-a", "obj-1", None), 64)];
        let (mut segment, _) = build_sealed_segment(&specs);
        segment.record_object_deleted(&specs[0].0).unwrap();
        assert!(segment.entries.is_empty());

        let err = EcSourceObject::for_packed_segment(&segment, &[], node(1), 5000).unwrap_err();
        match err {
            EcTranscodeError::InvalidSource { reason } => {
                assert!(
                    reason.contains("dropped, not encoded"),
                    "error message '{reason}' must state that segment should be dropped, not encoded"
                );
            }
            other => panic!("expected InvalidSource, got {other:?}"),
        }
    }

    #[test]
    fn a_missing_part_is_refused() {
        let specs = vec![
            (EcObjectVersionKey::new("bucket-a", "obj-1", None), 64),
            (EcObjectVersionKey::new("bucket-a", "obj-2", None), 128),
        ];
        let (segment, parts) = build_sealed_segment(&specs);
        assert_eq!(segment.entries.len(), 2);

        let missing_parts = parts[..1].to_vec();
        let err = EcSourceObject::for_packed_segment(&segment, &missing_parts, node(1), 5000)
            .unwrap_err();
        match err {
            EcTranscodeError::InvalidSource { reason } => {
                assert!(
                    reason.contains('2') && reason.contains('1'),
                    "error message '{reason}' must mention both live entries (2) and supplied parts (1)"
                );
            }
            other => panic!("expected InvalidSource, got {other:?}"),
        }
    }

    #[test]
    fn a_part_of_the_wrong_length_is_refused() {
        let specs = vec![(EcObjectVersionKey::new("bucket-a", "obj-1", None), 64)];
        let (segment, mut parts) = build_sealed_segment(&specs);

        parts[0].1.push(0x42);

        let err = EcSourceObject::for_packed_segment(&segment, &parts, node(1), 5000).unwrap_err();
        match err {
            EcTranscodeError::InvalidSource { reason } => {
                assert!(
                    reason.contains("65") && reason.contains("64"),
                    "error message '{reason}' must mention actual and expected lengths"
                );
            }
            other => panic!("expected InvalidSource, got {other:?}"),
        }
    }

    #[test]
    fn a_part_that_does_not_match_its_recorded_hash_is_refused() {
        let specs = vec![(EcObjectVersionKey::new("bucket-a", "obj-1", None), 64)];
        let (segment, mut parts) = build_sealed_segment(&specs);

        parts[0].1[0] ^= 0xff;

        let err = EcSourceObject::for_packed_segment(&segment, &parts, node(1), 5000).unwrap_err();
        match err {
            EcTranscodeError::InvalidSource { reason } => {
                assert!(
                    reason.contains("SHA-256"),
                    "error message '{reason}' must mention SHA-256 mismatch"
                );
            }
            other => panic!("expected InvalidSource, got {other:?}"),
        }
    }

    #[test]
    fn parts_given_out_of_order_are_refused() {
        let specs = vec![
            (EcObjectVersionKey::new("bucket-a", "obj-1", None), 64),
            (EcObjectVersionKey::new("bucket-a", "obj-2", None), 128),
        ];
        let (segment, mut parts) = build_sealed_segment(&specs);
        parts.swap(0, 1);

        let err = EcSourceObject::for_packed_segment(&segment, &parts, node(1), 5000).unwrap_err();
        match err {
            EcTranscodeError::InvalidSource { reason } => {
                assert!(
                    reason.contains("expects") && reason.contains("was given"),
                    "error message '{reason}' must mention order mismatch"
                );
            }
            other => panic!("expected InvalidSource, got {other:?}"),
        }
    }

    #[derive(Serialize)]
    struct LegacyEcTranscodeJobRecordWithoutTarget {
        object: EcObjectVersionKey,
        operation_id: Uuid,
        source_hlc: HlcTimestamp,
        target_erasure_set_id: u64,
        state: EcPlacementState,
        attempt: u32,
        object_size: u64,
        source_content_hash_sha256: [u8; 32],
        updated_at: u64,
        next_retry_at: Option<u64>,
        failure: Option<EcTranscodeFailureRecord>,
        origin: EcTranscodeOrigin,
    }

    /// What a record written by an older build actually does, pinned down.
    ///
    /// `#[serde(default)]` is not backward compatibility here. The queue is
    /// bincode, which reads fields by position and not by name, so a record that
    /// ends before `target` cannot be decoded at all and the default never gets
    /// a chance to apply. The compatibility that does exist lives in
    /// `list_transcode_jobs_page`, which skips such a record instead of failing
    /// the page; this test states the underlying fact so that nobody adds a
    /// field believing the attribute carries old records across.
    #[test]
    fn a_transcode_job_written_before_packed_segments_cannot_be_decoded() {
        let legacy = LegacyEcTranscodeJobRecordWithoutTarget {
            object: EcObjectVersionKey::new("bucket-a", "obj-1", None),
            operation_id: Uuid::new_v4(),
            source_hlc: hlc(),
            target_erasure_set_id: 1,
            state: EcPlacementState::EcTranscodeQueued,
            attempt: 0,
            object_size: 1024,
            source_content_hash_sha256: [7u8; 32],
            updated_at: 100,
            next_retry_at: None,
            failure: None,
            origin: EcTranscodeOrigin::ClientWrite,
        };

        let bytes = bincode::serialize(&legacy).unwrap();
        let decoded: Result<EcTranscodeJobRecord, _> = bincode::deserialize(&bytes);
        assert!(
            decoded.is_err(),
            "bincode cannot default a trailing field; if this ever succeeds, the skip in \
             list_transcode_jobs_page can be revisited"
        );

        // A record this build wrote round-trips, and a job built without naming
        // a target is an object job.
        let modern = EcTranscodeJobRecord {
            object: legacy.object.clone(),
            operation_id: legacy.operation_id,
            source_hlc: legacy.source_hlc,
            target_erasure_set_id: legacy.target_erasure_set_id,
            state: legacy.state,
            attempt: legacy.attempt,
            object_size: legacy.object_size,
            source_content_hash_sha256: legacy.source_content_hash_sha256,
            updated_at: legacy.updated_at,
            next_retry_at: legacy.next_retry_at,
            failure: legacy.failure.clone(),
            origin: legacy.origin,
            target: EcTranscodeTarget::default(),
        };
        let round_tripped: EcTranscodeJobRecord =
            bincode::deserialize(&bincode::serialize(&modern).unwrap()).unwrap();
        assert_eq!(round_tripped.target, EcTranscodeTarget::Object);
        assert!(!round_tripped.target.is_packed_segment());
    }

    #[tokio::test]
    async fn a_packed_segment_is_encoded_however_small_it_is() {
        let dir = tempfile::tempdir().unwrap();
        let metadata = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let worker = worker(metadata.clone());
        let shard_store = MemoryShardStore::default();
        let manifest_quorum = MemoryManifestQuorum::default();
        let cutover = MemoryCutover::default();

        // A segment sealed on its age limit, or one a repack built out of the
        // few survivors of a fragmented one, easily comes out smaller than the
        // threshold a single object is judged by. Refusing to encode it would
        // leave its objects replicated for good — which is the cost packing
        // exists to remove — and would write a replicated-small layout against
        // a key that names a container rather than an object.
        let specs = vec![
            (EcObjectVersionKey::new("bucket-a", "obj-1", None), 64),
            (EcObjectVersionKey::new("bucket-a", "obj-2", None), 96),
        ];
        let (segment, parts) = build_sealed_segment(&specs);
        metadata.put_packed_segment(&segment).await.unwrap();
        let source = EcSourceObject::for_packed_segment(&segment, &parts, node(1), 5000).unwrap();

        let mut erasure_set = active_set();
        erasure_set.profile.min_object_size = 1024;
        assert!(source.size().unwrap() < erasure_set.profile.min_object_size);

        let request = EcTranscodeRequest {
            source,
            erasure_set,
            owner_node_id: node(1),
            origin: EcTranscodeOrigin::ClientWrite,
            target: EcTranscodeTarget::PackedSegment,
            now_nanos: 100,
        };

        let outcome = worker
            .transcode(request, &shard_store, &manifest_quorum, &cutover)
            .await
            .unwrap();

        assert!(
            matches!(outcome, EcTranscodeOutcome::PackedSegmentEncoded { .. }),
            "a packed segment must be encoded whatever its size, got {outcome:?}"
        );
    }
}
