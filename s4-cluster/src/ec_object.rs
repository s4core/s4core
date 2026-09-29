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

//! Edition-safe Erasure Coding object-runtime boundary.
//!
//! The S3 API layer is Apache-licensed and must not depend on Enterprise
//! implementation types. This module defines the small runtime hooks the API
//! layer calls after a normal S3 operation has reached the RF=3 hot durability
//! boundary in an EC pool. Enterprise code records EC layout/transcode state
//! behind this trait; CE uses the noop implementation.

use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::error::ClusterError;
use crate::hlc::HlcTimestamp;
use uuid::Uuid;

/// S3 object write that has already reached the hot RF=3 durability boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EcObjectPutCommit {
    /// Bucket name.
    pub bucket: String,
    /// Object key.
    pub key: String,
    /// Optional version ID.
    #[serde(default)]
    pub version_id: Option<String>,
    /// ETag observed by the write coordinator.
    ///
    /// This can be empty when the hot RF quorum is satisfied entirely by
    /// remote replicas, because remote write ACKs do not currently return an
    /// ETag. EC durability uses the content hash and HLC below instead.
    pub etag: String,
    /// Object size in bytes.
    pub size: u64,
    /// Object content type.
    pub content_type: String,
    /// User-visible and internal metadata attached to the object.
    pub metadata: HashMap<String, String>,
    /// Hot RF replica nodes that accepted or are assigned to the object.
    pub hot_replica_nodes: Vec<crate::identity::NodeId>,
    /// SHA-256 of the complete hot RF=3 object bytes.
    pub content_hash_sha256: [u8; 32],
    /// HLC assigned by the hot RF=3 write coordinator.
    pub source_hlc: HlcTimestamp,
}

impl EcObjectPutCommit {
    /// Validate the committed write before handing it to an EC implementation.
    pub fn validate(&self) -> Result<(), ClusterError> {
        if self.bucket.trim().is_empty() {
            return Err(ClusterError::Config(
                "EC object put bucket must not be empty".to_string(),
            ));
        }
        if self.key.is_empty() {
            return Err(ClusterError::Config(
                "EC object put key must not be empty".to_string(),
            ));
        }
        if self.source_hlc.is_zero() {
            return Err(ClusterError::Config(
                "EC object put source_hlc must not be zero".to_string(),
            ));
        }
        if self.hot_replica_nodes.is_empty() {
            return Err(ClusterError::Config(
                "EC object put hot_replica_nodes must not be empty".to_string(),
            ));
        }
        let distinct_nodes =
            self.hot_replica_nodes.iter().copied().collect::<std::collections::HashSet<_>>();
        if distinct_nodes.len() != self.hot_replica_nodes.len() {
            return Err(ClusterError::Config(
                "EC object put hot_replica_nodes must be distinct".to_string(),
            ));
        }
        Ok(())
    }
}

/// S3 object delete that has already reached the hot RF=3 quorum boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EcObjectDeleteCommit {
    /// Bucket name.
    pub bucket: String,
    /// Object key.
    pub key: String,
    /// Optional version ID.
    #[serde(default)]
    pub version_id: Option<String>,
    /// HLC assigned by the hot RF=3 delete coordinator.
    #[serde(default = "HlcTimestamp::zero")]
    pub delete_hlc: HlcTimestamp,
    /// Idempotency ID assigned by the hot RF=3 delete coordinator.
    #[serde(default = "Uuid::nil")]
    pub operation_id: Uuid,
}

impl EcObjectDeleteCommit {
    /// Validate the committed delete before handing it to an EC implementation.
    pub fn validate(&self) -> Result<(), ClusterError> {
        if self.bucket.trim().is_empty() {
            return Err(ClusterError::Config(
                "EC object delete bucket must not be empty".to_string(),
            ));
        }
        if self.key.is_empty() {
            return Err(ClusterError::Config(
                "EC object delete key must not be empty".to_string(),
            ));
        }
        if self.delete_hlc.is_zero() {
            return Err(ClusterError::Config(
                "EC object delete_hlc must not be zero".to_string(),
            ));
        }
        if self.operation_id.is_nil() {
            return Err(ClusterError::Config(
                "EC object delete operation_id must not be nil".to_string(),
            ));
        }
        Ok(())
    }
}

/// Result of recording EC metadata for a committed S3 object write.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EcObjectPutDisposition {
    /// Object is below the EC threshold and remains explicitly replicated.
    ReplicatedSmall,
    /// Object is queued for staged EC transcode.
    TranscodeQueued,
    /// Object stays on its RF=3 replicas for now.
    ///
    /// Reported when the pool defers encoding to auto-tiering: the object is a
    /// perfectly ordinary replicated object until it goes cold, and only then
    /// does the tiering worker queue it. Nothing about the object is different
    /// from the client's point of view.
    HotReplicated,
    /// Object was placed inside a packed segment.
    ///
    /// Its bytes are still the RF=3 copies the hot write path produced; the
    /// segment records where they will sit once it is sealed and erasure coded
    /// as a whole. Nothing about the object is different from the client's
    /// point of view.
    PackedSegment,
}

/// Inclusive byte range requested from an EC object.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct EcObjectByteRange {
    /// First byte to return.
    pub start: u64,
    /// Last byte to return, inclusive.
    pub end: u64,
}

impl EcObjectByteRange {
    /// Create an inclusive byte range.
    pub fn new(start: u64, end: u64) -> Self {
        Self { start, end }
    }
}

/// S3 object read request for an EC pool.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EcObjectReadRequest {
    /// Bucket name.
    pub bucket: String,
    /// Object key.
    pub key: String,
    /// Optional version ID.
    #[serde(default)]
    pub version_id: Option<String>,
    /// Optional inclusive range.
    #[serde(default)]
    pub range: Option<EcObjectByteRange>,
}

impl EcObjectReadRequest {
    /// Validate a read request before handing it to an EC implementation.
    pub fn validate(&self) -> Result<(), EcObjectReadError> {
        validate_object_ref(&self.bucket, &self.key)?;
        if let Some(range) = self.range {
            if range.start > range.end {
                return Err(EcObjectReadError::InvalidRange {
                    start: range.start,
                    end: range.end,
                    total: 0,
                });
            }
        }
        Ok(())
    }
}

/// S3 object HEAD request for an EC pool.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EcObjectHeadRequest {
    /// Bucket name.
    pub bucket: String,
    /// Object key.
    pub key: String,
    /// Optional version ID.
    #[serde(default)]
    pub version_id: Option<String>,
}

impl EcObjectHeadRequest {
    /// Validate a HEAD request before handing it to an EC implementation.
    pub fn validate(&self) -> Result<(), EcObjectReadError> {
        validate_object_ref(&self.bucket, &self.key)
    }
}

/// Durable EC layout used to serve a read.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EcObjectReadLayout {
    /// Object is still served through the hot RF=3 quorum path.
    HotReplicated,
    /// Object is explicitly replicated as a small object inside an EC pool.
    ReplicatedSmall {
        /// Replica nodes that own the hot RF=3 copies.
        replica_nodes: Vec<crate::identity::NodeId>,
    },
    /// Object is stored inside a packed segment.
    PackedSegment {
        /// Segment holding the object.
        segment_id: uuid::Uuid,
        /// Erasure set that owns the segment's shards.
        erasure_set_id: u64,
        /// Offset of the object from the start of the segment.
        offset: u64,
        /// Object length in bytes.
        length: u64,
        /// Whether hot RF=3 copies still back the object.
        hot_copies: bool,
    },
    /// Object is served from an erasure-coded manifest.
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

/// Metadata returned by EC HEAD and attached to EC reads.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EcObjectHeadResult {
    /// S3 ETag.
    pub etag: String,
    /// Content-Type.
    pub content_type: String,
    /// Full logical object size.
    pub size: u64,
    /// User-defined and internal metadata attached to the selected object.
    pub metadata: HashMap<String, String>,
    /// HLC timestamp used for Last-Modified and conflict ordering.
    pub hlc: HlcTimestamp,
    /// Version ID when versioning is enabled.
    #[serde(default)]
    pub version_id: Option<String>,
    /// Physical layout selected by the EC runtime.
    pub layout: EcObjectReadLayout,
}

/// Body and metadata returned by an EC object read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EcObjectReadResult {
    /// Returned response body.
    pub body: Bytes,
    /// Object metadata.
    pub head: EcObjectHeadResult,
    /// Response body size.
    pub content_length: u64,
    /// HTTP-style content range `(start, end, total)` for range reads.
    pub content_range: Option<(u64, u64, u64)>,
    /// Whether at least one EC repair task was enqueued.
    pub repair_enqueued: bool,
}

/// Errors returned by the edition-safe EC read boundary.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum EcObjectReadError {
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

    /// The requested byte range cannot be satisfied.
    #[error("invalid EC object read range {start}-{end} for object size {total}")]
    InvalidRange {
        /// Requested start.
        start: u64,
        /// Requested end.
        end: u64,
        /// Full object size.
        total: u64,
    },

    /// Not enough verified data is available to serve the read.
    #[error("EC object read unavailable: {reason}")]
    Unavailable {
        /// Human-readable reason.
        reason: String,
    },

    /// Requested feature requires Enterprise Edition with EC enabled.
    #[error("enterprise feature required: {0}")]
    EnterpriseRequired(String),

    /// Internal invariant or implementation failure.
    #[error("internal EC object read error: {reason}")]
    Internal {
        /// Human-readable reason.
        reason: String,
    },
}

impl EcObjectReadError {
    /// Convert cluster-level read failures into stable EC read boundary errors.
    pub fn from_cluster(error: ClusterError, bucket: String, key: String) -> Self {
        match error {
            ClusterError::ObjectNotFound { bucket, key } => Self::NotFound {
                bucket,
                key,
                version_id: None,
            },
            other @ (ClusterError::QuorumNotMet { .. } | ClusterError::ReadTimeout(_)) => {
                Self::Unavailable {
                    reason: other.to_string(),
                }
            }
            ClusterError::EnterpriseRequired(message) => Self::EnterpriseRequired(message),
            other @ ClusterError::EpochMismatch { .. } => Self::Unavailable {
                reason: other.to_string(),
            },
            other => Self::Internal {
                reason: format!("{bucket}/{key}: {other}"),
            },
        }
    }

    /// A timeout-oriented unavailable error.
    pub fn timeout(timeout: Duration) -> Self {
        Self::Unavailable {
            reason: format!("read timeout after {timeout:?}"),
        }
    }
}

/// Runtime hooks called by normal S3 object handlers for EC pools.
#[async_trait]
pub trait EcObjectRuntime: Send + Sync + 'static {
    /// Record EC metadata after a hot RF=3 object write has succeeded.
    async fn record_put_commit(
        &self,
        commit: EcObjectPutCommit,
    ) -> Result<EcObjectPutDisposition, ClusterError>;

    /// Record EC metadata cleanup/tombstone intent after a hot RF=3 delete.
    async fn record_delete_commit(&self, commit: EcObjectDeleteCommit) -> Result<(), ClusterError>;

    /// Read an object through the EC runtime.
    async fn read_object(
        &self,
        request: EcObjectReadRequest,
    ) -> Result<EcObjectReadResult, EcObjectReadError>;

    /// Return object metadata through the EC runtime.
    async fn head_object(
        &self,
        request: EcObjectHeadRequest,
    ) -> Result<EcObjectHeadResult, EcObjectReadError>;
}

/// One node's record of when an object was last used.
///
/// The read count is evaluated by the node that answers, over its own trailing
/// window and against its own clock. A window has no single start once it
/// slides, so there is nothing meaningful to hand the caller except the number
/// itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct EcObjectHeatMark {
    /// Last observed access in nanoseconds since the Unix epoch.
    pub last_access_nanos: u64,
    /// Reads this node served within the trailing window.
    pub window_access_count: u32,
}

/// Serves this node's activity marks to peers over the inter-node RPC.
///
/// Auto-tiering records an access on the node that served it, so no node knows
/// on its own when an object was last used across the pool. This is the pull
/// side of that question; the answer is advisory, and a node that cannot
/// answer is treated as one that has never seen the object.
#[async_trait]
pub trait EcHeatQuery: Send + Sync + 'static {
    /// This node's mark for one object version, if it has one.
    ///
    /// `window_nanos` is the trailing width the caller wants the read count
    /// over; zero means this node's own configured window. A bucket may ask for
    /// a narrower one than the node records, and answering over the node's
    /// width would then count reads the bucket has already forgotten.
    async fn local_object_heat(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        window_nanos: u64,
    ) -> Result<Option<EcObjectHeatMark>, ClusterError>;
}

/// One live EC object, reduced to what the dedup measurement needs.
///
/// Deliberately not the manifest: the measurement only ever groups by content
/// hash and sums sizes, and a manifest carries chunk and shard tables that
/// would multiply the answer's size by three orders of magnitude for nothing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EcContentDigest {
    /// Bucket of the object this manifest describes.
    pub bucket: String,
    /// Key of the object this manifest describes.
    pub key: String,
    /// Version ID, or `None` for an unversioned object.
    pub version_id: Option<String>,
    /// SHA-256 of the object's content, as the manifest records it.
    pub content_hash_sha256: [u8; 32],
    /// Object size before encoding — the bytes a repeat would save.
    pub original_size: u64,
    /// Erasure set holding the shards. Dedup cannot cross this boundary.
    pub erasure_set_id: u64,
    /// True when the object's bytes live inside a packed segment.
    ///
    /// Those bytes already share one encoded segment with other objects, so
    /// sharing shard sets between manifests would save nothing on them.
    pub packed: bool,
}

/// One node's answer to the dedup measurement.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EcContentDigestPage {
    /// Digests of the live EC objects this node knows.
    pub digests: Vec<EcContentDigest>,
    /// Manifests read, including those dropped as describing no EC bytes.
    pub scanned: usize,
    /// True when the walk stopped at the limit rather than at the end.
    pub truncated: bool,
}

/// Serves this node's EC content digests to peers.
///
/// A node's manifest store holds what it transcoded plus what its read path
/// resolved, so on its own it sees a fraction of the pool — and a repeat split
/// across two nodes is invisible on both. This is the pull side of the dedup
/// measurement: the coordinating node asks every peer and merges the answers.
#[async_trait]
pub trait EcContentDigestScan: Send + Sync + 'static {
    /// This node's live EC objects, at most `max_entries` manifests walked.
    ///
    /// The implementation clamps `max_entries` to its own admin scan limit, so
    /// one measurement cannot make a peer walk its whole store.
    async fn local_content_digests(
        &self,
        max_entries: usize,
    ) -> Result<EcContentDigestPage, ClusterError>;
}

/// One delete, as the tombstone sweep identifies it.
///
/// Same object, same delete operation, same HLC. A tombstone names no shards, so
/// there is nothing else to match on and nothing else worth carrying.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EcTombstoneProbe {
    /// Bucket of the deleted object.
    pub bucket: String,
    /// Key of the deleted object.
    pub key: String,
    /// Version of the deleted object, when it had one.
    pub version_id: Option<String>,
    /// The delete operation this tombstone belongs to.
    pub operation_id: u128,
    /// HLC of the delete.
    pub hlc: HlcTimestamp,
}

/// Answers "do you hold this delete?" for a batch of deletes at once.
///
/// The sweep needs the answer per delete per owner before it may release a
/// single shard. One call per delete makes that a round trip per delete; one
/// call per owner makes it a round trip per owner, and the gate is unchanged —
/// anything that is not a clear yes stays a no.
#[async_trait]
pub trait EcTombstoneProbeService: Send + Sync + 'static {
    /// Whether this node holds each of these deletes, in the order asked.
    async fn holds_tombstones(
        &self,
        probes: Vec<EcTombstoneProbe>,
    ) -> Result<Vec<bool>, ClusterError>;
}

/// CE/default implementation for EC object runtime hooks.
#[derive(Debug, Default)]
pub struct NoopEcObjectRuntime;

#[async_trait]
impl EcObjectRuntime for NoopEcObjectRuntime {
    async fn record_put_commit(
        &self,
        _commit: EcObjectPutCommit,
    ) -> Result<EcObjectPutDisposition, ClusterError> {
        Err(ec_required())
    }

    async fn record_delete_commit(
        &self,
        _commit: EcObjectDeleteCommit,
    ) -> Result<(), ClusterError> {
        Err(ec_required())
    }

    async fn read_object(
        &self,
        _request: EcObjectReadRequest,
    ) -> Result<EcObjectReadResult, EcObjectReadError> {
        Err(ec_read_required())
    }

    async fn head_object(
        &self,
        _request: EcObjectHeadRequest,
    ) -> Result<EcObjectHeadResult, EcObjectReadError> {
        Err(ec_read_required())
    }
}

fn ec_required() -> ClusterError {
    ClusterError::EnterpriseRequired(
        "erasure coding object operations require Enterprise Edition with an EC license"
            .to_string(),
    )
}

fn ec_read_required() -> EcObjectReadError {
    EcObjectReadError::EnterpriseRequired(
        "erasure coding object reads require Enterprise Edition with an EC license".to_string(),
    )
}

fn validate_object_ref(bucket: &str, key: &str) -> Result<(), EcObjectReadError> {
    if bucket.trim().is_empty() {
        return Err(EcObjectReadError::Internal {
            reason: "EC object bucket must not be empty".to_string(),
        });
    }
    if key.is_empty() {
        return Err(EcObjectReadError::Internal {
            reason: "EC object key must not be empty".to_string(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::NodeId;

    fn hlc() -> HlcTimestamp {
        HlcTimestamp {
            wall_time: 1,
            logical: 0,
            node_id: NodeId(7).0,
        }
    }

    #[test]
    fn put_commit_rejects_empty_key() {
        let commit = EcObjectPutCommit {
            bucket: "bucket".to_string(),
            key: String::new(),
            version_id: None,
            etag: "etag".to_string(),
            size: 1,
            content_type: "application/octet-stream".to_string(),
            metadata: HashMap::new(),
            hot_replica_nodes: vec![NodeId(1), NodeId(2), NodeId(3)],
            content_hash_sha256: [1; 32],
            source_hlc: hlc(),
        };
        assert!(commit.validate().is_err());
    }

    #[test]
    fn put_commit_allows_empty_etag_from_remote_only_quorum() {
        let commit = EcObjectPutCommit {
            bucket: "bucket".to_string(),
            key: "key".to_string(),
            version_id: None,
            etag: String::new(),
            size: 1,
            content_type: "application/octet-stream".to_string(),
            metadata: HashMap::new(),
            hot_replica_nodes: vec![NodeId(1), NodeId(2), NodeId(3)],
            content_hash_sha256: [1; 32],
            source_hlc: hlc(),
        };
        assert!(commit.validate().is_ok());
    }

    #[tokio::test]
    async fn noop_runtime_returns_enterprise_required() {
        let runtime = NoopEcObjectRuntime;
        let commit = EcObjectDeleteCommit {
            bucket: "bucket".to_string(),
            key: "key".to_string(),
            version_id: None,
            delete_hlc: hlc(),
            operation_id: Uuid::from_u128(9),
        };
        let err = runtime.record_delete_commit(commit).await.unwrap_err();
        assert!(matches!(err, ClusterError::EnterpriseRequired(_)));
    }

    #[test]
    fn read_request_rejects_inverted_range() {
        let request = EcObjectReadRequest {
            bucket: "bucket".to_string(),
            key: "key".to_string(),
            version_id: None,
            range: Some(EcObjectByteRange::new(10, 2)),
        };
        assert!(matches!(
            request.validate(),
            Err(EcObjectReadError::InvalidRange { .. })
        ));
    }

    #[tokio::test]
    async fn noop_read_runtime_returns_enterprise_required() {
        let runtime = NoopEcObjectRuntime;
        let request = EcObjectHeadRequest {
            bucket: "bucket".to_string(),
            key: "key".to_string(),
            version_id: None,
        };
        let err = runtime.head_object(request).await.unwrap_err();
        assert!(matches!(err, EcObjectReadError::EnterpriseRequired(_)));
    }
}
