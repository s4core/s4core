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

//! Edition-safe Erasure Coding admin boundary.
//!
//! The HTTP API lives in Apache-licensed crates, while the implementation of
//! EC operations lives in Enterprise Edition crates. This module keeps that
//! boundary explicit: CE can expose stable routes that return an Enterprise
//! requirement, and EE injects a concrete implementation at startup.

use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::ClusterError;

/// Default upper bound for one pool-repair admin request.
pub const DEFAULT_EC_ADMIN_REPAIR_MAX_TASKS: usize = 1_024;

/// Object/version addressed by EC admin operations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EcAdminObjectRef {
    /// Bucket name.
    pub bucket: String,
    /// Object key.
    pub key: String,
    /// Optional version ID.
    #[serde(default)]
    pub version_id: Option<String>,
}

impl EcAdminObjectRef {
    /// Validate the object reference before handing it to an implementation.
    pub fn validate(&self) -> Result<(), ClusterError> {
        if self.bucket.trim().is_empty() {
            return Err(ClusterError::Config(
                "EC admin object bucket must not be empty".to_string(),
            ));
        }
        if self.key.is_empty() {
            return Err(ClusterError::Config(
                "EC admin object key must not be empty".to_string(),
            ));
        }
        Ok(())
    }
}

/// Request for pool-level EC repair scheduling.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EcAdminRepairRequest {
    /// Pool name or numeric pool ID.
    pub pool: String,
    /// Optional object scope. When omitted, the implementation scans manifests
    /// and queues bounded local repairs for this node.
    #[serde(default)]
    pub object: Option<EcAdminObjectRef>,
    /// Optional chunk filter.
    #[serde(default)]
    pub chunk_index: Option<u32>,
    /// Optional shard-slot filter.
    #[serde(default)]
    pub slot_index: Option<u8>,
    /// Optional node ID filter, formatted with the normal `NodeId` display.
    #[serde(default)]
    pub node_id: Option<String>,
    /// Maximum tasks that may be queued by this request.
    #[serde(default = "default_repair_max_tasks")]
    pub max_tasks: usize,
}

impl EcAdminRepairRequest {
    /// Validate the repair request.
    pub fn validate(&self) -> Result<(), ClusterError> {
        if self.pool.trim().is_empty() {
            return Err(ClusterError::Config(
                "EC admin repair pool must not be empty".to_string(),
            ));
        }
        if let Some(object) = &self.object {
            object.validate()?;
        }
        if self.max_tasks == 0 {
            return Err(ClusterError::Config(
                "EC admin repair max_tasks must be greater than 0".to_string(),
            ));
        }
        Ok(())
    }
}

impl Default for EcAdminRepairRequest {
    fn default() -> Self {
        Self {
            pool: String::new(),
            object: None,
            chunk_index: None,
            slot_index: None,
            node_id: None,
            max_tasks: DEFAULT_EC_ADMIN_REPAIR_MAX_TASKS,
        }
    }
}

/// Request for the repair queue status of a pool.
///
/// `node_id` turns the queue report into a rebuild report: it asks how many
/// shards of that one owner the pool still considers missing. That question
/// cannot be answered from the queue — an empty queue is also what a rebuild
/// nobody queued looks like — so the implementation answers it by inventory.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EcAdminRepairStatusRequest {
    /// Pool name or numeric pool ID.
    pub pool: String,
    /// Optional slot owner whose rebuild progress is asked about, formatted
    /// with the normal `NodeId` display.
    #[serde(default)]
    pub node_id: Option<String>,
}

impl EcAdminRepairStatusRequest {
    /// Build a request for the pool's repair queue alone.
    pub fn for_pool(pool: impl Into<String>) -> Self {
        Self {
            pool: pool.into(),
            node_id: None,
        }
    }

    /// Validate the repair status request.
    ///
    /// # Errors
    ///
    /// Returns [`ClusterError::Config`] when the request names no pool, or
    /// names a slot owner with a blank identity.
    pub fn validate(&self) -> Result<(), ClusterError> {
        if self.pool.trim().is_empty() {
            return Err(ClusterError::Config(
                "EC admin repair status pool must not be empty".to_string(),
            ));
        }
        if self.node_id.as_ref().is_some_and(|id| id.trim().is_empty()) {
            return Err(ClusterError::Config(
                "EC admin repair status node_id must not be blank".to_string(),
            ));
        }
        Ok(())
    }
}

/// Request for orphan-shard GC from the admin API.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EcAdminOrphanGcRequest {
    /// When true, only report candidates. No physical shard metadata is changed.
    #[serde(default)]
    pub dry_run: bool,
    /// Maximum orphan records scanned by this request.
    #[serde(default = "default_repair_max_tasks")]
    pub max_entries: usize,
}

impl EcAdminOrphanGcRequest {
    /// Validate the orphan GC request.
    pub fn validate(&self) -> Result<(), ClusterError> {
        if self.max_entries == 0 {
            return Err(ClusterError::Config(
                "EC orphan GC max_entries must be greater than 0".to_string(),
            ));
        }
        Ok(())
    }
}

impl Default for EcAdminOrphanGcRequest {
    fn default() -> Self {
        Self {
            dry_run: false,
            max_entries: DEFAULT_EC_ADMIN_REPAIR_MAX_TASKS,
        }
    }
}

/// Request to run one EC tombstone GC cycle on this node.
///
/// Node-local on purpose. Shards belong to the node that holds them, so only
/// that node can release them; what is cluster-wide is the retirement of the
/// tombstone itself, and the sweep does that in one step once its own gates
/// pass. An operator sweeping a whole pool asks every node in turn.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EcAdminTombstoneGcRequest {
    /// When true, only report what the gates would allow. Nothing is removed.
    #[serde(default)]
    pub dry_run: bool,
    /// Maximum tombstones examined by this request.
    #[serde(default = "default_repair_max_tasks")]
    pub max_entries: usize,
    /// Opaque continuation returned by the preceding manual GC page.
    #[serde(default)]
    pub cursor: Option<String>,
}

impl EcAdminTombstoneGcRequest {
    /// Validate the tombstone GC request.
    pub fn validate(&self) -> Result<(), ClusterError> {
        if self.max_entries == 0 {
            return Err(ClusterError::Config(
                "EC tombstone GC max_entries must be greater than 0".to_string(),
            ));
        }
        self.decoded_cursor()?;
        Ok(())
    }

    /// Decode and validate the opaque metadata cursor before any store access.
    pub fn decoded_cursor(&self) -> Result<Option<Vec<u8>>, ClusterError> {
        const MAX_CURSOR_HEX_LEN: usize = 32 * 1024;
        let Some(cursor) = self.cursor.as_deref() else {
            return Ok(None);
        };
        if cursor.is_empty() || cursor.len() > MAX_CURSOR_HEX_LEN || cursor.len() % 2 != 0 {
            return Err(ClusterError::Config(
                "EC tombstone GC cursor is malformed".to_string(),
            ));
        }
        cursor
            .as_bytes()
            .as_chunks::<2>()
            .0
            .iter()
            .map(|pair| {
                let high = decode_hex_digit(pair[0])?;
                let low = decode_hex_digit(pair[1])?;
                Ok((high << 4) | low)
            })
            .collect::<Result<Vec<_>, _>>()
            .map(Some)
    }

    /// Encode a metadata-store key as an opaque cursor for a manual response.
    pub fn encode_cursor(cursor: &[u8]) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut encoded = String::with_capacity(cursor.len().saturating_mul(2));
        for byte in cursor {
            encoded.push(char::from(HEX[usize::from(byte >> 4)]));
            encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
        }
        encoded
    }
}

fn decode_hex_digit(digit: u8) -> Result<u8, ClusterError> {
    match digit {
        b'0'..=b'9' => Ok(digit - b'0'),
        b'a'..=b'f' => Ok(digit - b'a' + 10),
        b'A'..=b'F' => Ok(digit - b'A' + 10),
        _ => Err(ClusterError::Config(
            "EC tombstone GC cursor is malformed".to_string(),
        )),
    }
}

impl Default for EcAdminTombstoneGcRequest {
    fn default() -> Self {
        Self {
            dry_run: false,
            max_entries: DEFAULT_EC_ADMIN_REPAIR_MAX_TASKS,
            cursor: None,
        }
    }
}

/// Request to return packed segments to ordinary RF=3 replication.
///
/// The rollback path of packed segments. Switching packing off stops new
/// segments but leaves the existing ones alone, so an operator who wants out of
/// the feature has to unpack what is already there.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EcAdminUnpackSegmentsRequest {
    /// One segment to unpack. When absent, every segment that can be is.
    #[serde(default)]
    pub segment_id: Option<String>,
    /// When true, only report what would be unpacked. Nothing is changed.
    #[serde(default)]
    pub dry_run: bool,
    /// Maximum segments unpacked by this request.
    #[serde(default = "default_repair_max_tasks")]
    pub max_segments: usize,
}

impl EcAdminUnpackSegmentsRequest {
    /// Validate the unpack request.
    ///
    /// # Errors
    ///
    /// Returns [`ClusterError::Config`] when the request could not act on
    /// anything.
    pub fn validate(&self) -> Result<(), ClusterError> {
        if self.max_segments == 0 {
            return Err(ClusterError::Config(
                "EC unpack max_segments must be greater than 0".to_string(),
            ));
        }
        if self.segment_id.as_ref().is_some_and(|id| id.trim().is_empty()) {
            return Err(ClusterError::Config(
                "EC unpack segment_id must not be blank".to_string(),
            ));
        }
        Ok(())
    }
}

impl Default for EcAdminUnpackSegmentsRequest {
    fn default() -> Self {
        Self {
            segment_id: None,
            dry_run: false,
            max_segments: DEFAULT_EC_ADMIN_REPAIR_MAX_TASKS,
        }
    }
}

/// Smallest cold-after value an operator may set on a bucket, in seconds.
///
/// A shorter threshold turns auto-tiering into a migration generator. The
/// node-wide environment variable keeps a looser rule of its own so that
/// deployments which already set a small value still start.
pub const MIN_TIERING_COLD_AFTER_SECS: u64 = 24 * 3_600;

/// Bucket tiering policy submitted by an operator.
///
/// Every threshold is optional and an unset one means "inherit the node value",
/// never "zero". That is what makes a policy a delta over configuration rather
/// than a second source of truth.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EcAdminTieringPolicyRequest {
    /// Whether tiering is enabled for this bucket.
    #[serde(default)]
    pub enabled: Option<bool>,
    /// Idle time before an object of this bucket is considered cold.
    #[serde(default)]
    pub cold_after_secs: Option<u64>,
    /// Smallest object size this bucket migrates.
    #[serde(default)]
    pub min_object_size: Option<u64>,
    /// Key prefixes that never migrate.
    #[serde(default)]
    pub exclude_prefixes: Vec<String>,
    /// Object tags that exclude an object from migration.
    #[serde(default)]
    pub exclude_tags: HashMap<String, String>,
    /// Reads within the promotion window that return an object to RF=3.
    #[serde(default)]
    pub promote_after_reads: Option<u32>,
    /// Width of the promotion counting window.
    #[serde(default)]
    pub promote_window_secs: Option<u64>,
}

impl EcAdminTieringPolicyRequest {
    /// Reject values that would make tiering harmful rather than merely wrong.
    pub fn validate(&self) -> Result<(), ClusterError> {
        if let Some(cold_after_secs) = self.cold_after_secs {
            if cold_after_secs < MIN_TIERING_COLD_AFTER_SECS {
                return Err(ClusterError::Config(format!(
                    "EC tiering cold_after_secs must be at least {MIN_TIERING_COLD_AFTER_SECS} seconds"
                )));
            }
        }
        if self.promote_window_secs == Some(0) {
            return Err(ClusterError::Config(
                "EC tiering promote_window_secs must be greater than 0".to_string(),
            ));
        }
        if self.exclude_prefixes.iter().any(|prefix| prefix.is_empty()) {
            return Err(ClusterError::Config(
                "EC tiering exclude_prefixes must not contain an empty prefix".to_string(),
            ));
        }
        if self.exclude_tags.keys().any(|key| key.is_empty()) {
            return Err(ClusterError::Config(
                "EC tiering exclude_tags must not contain an empty tag name".to_string(),
            ));
        }
        Ok(())
    }
}

/// Request to bring erasure-coded objects back to RF=3.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EcAdminTieringPromoteRequest {
    /// Bucket holding the objects to bring back.
    ///
    /// Filled in from the request path, which is where the bucket is named. A
    /// body may leave it out, and a body that sets it is overwritten: two
    /// answers to the same question is one too many.
    #[serde(default)]
    pub bucket: String,
    /// Only promote objects under this key prefix.
    #[serde(default)]
    pub prefix: Option<String>,
    /// Maximum objects examined by this request.
    #[serde(default = "default_repair_max_tasks")]
    pub max_objects: usize,
    /// Report what would be queued without queueing anything.
    #[serde(default)]
    pub dry_run: bool,
}

impl Default for EcAdminTieringPromoteRequest {
    fn default() -> Self {
        Self {
            bucket: String::new(),
            prefix: None,
            max_objects: default_repair_max_tasks(),
            dry_run: false,
        }
    }
}

impl EcAdminTieringPromoteRequest {
    /// Reject a request that would examine nothing or name no bucket.
    pub fn validate(&self) -> Result<(), ClusterError> {
        if self.bucket.trim().is_empty() {
            return Err(ClusterError::Config(
                "EC tiering promote requires a bucket".to_string(),
            ));
        }
        if self.max_objects == 0 {
            return Err(ClusterError::Config(
                "EC tiering promote max_objects must be greater than 0".to_string(),
            ));
        }
        Ok(())
    }
}

/// Request for a tiering dry run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EcAdminTieringDryRunRequest {
    /// Bucket to examine.
    pub bucket: String,
    /// Only look at objects under this key prefix.
    #[serde(default)]
    pub prefix: Option<String>,
    /// Maximum objects examined by this request.
    #[serde(default = "default_repair_max_tasks")]
    pub max_objects: usize,
    /// How many per-object decisions to include in the report.
    #[serde(default = "default_dry_run_samples")]
    pub max_samples: usize,
}

impl EcAdminTieringDryRunRequest {
    /// Validate the dry-run request.
    pub fn validate(&self) -> Result<(), ClusterError> {
        if self.bucket.trim().is_empty() {
            return Err(ClusterError::Config(
                "EC tiering dry run requires a bucket".to_string(),
            ));
        }
        if self.max_objects == 0 {
            return Err(ClusterError::Config(
                "EC tiering dry run max_objects must be greater than 0".to_string(),
            ));
        }
        Ok(())
    }
}

/// Request for the erasure-coding duplicate-content measurement.
///
/// Phase 0 of plan 07 and nothing more: it reads manifests, writes nothing, and
/// answers whether dedup at the erasure-coding tier would save anything beyond
/// what content-addressable dedup on the write path already saves.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EcAdminDedupReportRequest {
    /// Manifests one node may walk. Each node clamps it to its own scan limit.
    #[serde(default = "default_repair_max_tasks")]
    pub max_entries: usize,
    /// Measure only this node's manifests and do not ask the pool.
    ///
    /// A node holds the manifests it transcoded plus the ones its read path
    /// resolved, so on its own it sees a fraction of the pool and a repeat split
    /// across two nodes is invisible on both. The pool-wide walk is therefore
    /// the default, and this flag exists for looking at one node's own share —
    /// it produces a report that is explicitly marked as deciding nothing.
    #[serde(default)]
    pub local_only: bool,
}

impl EcAdminDedupReportRequest {
    /// Validate the measurement request.
    ///
    /// # Errors
    ///
    /// Returns [`ClusterError::Config`] when the request could not read anything.
    pub fn validate(&self) -> Result<(), ClusterError> {
        if self.max_entries == 0 {
            return Err(ClusterError::Config(
                "EC dedup report max_entries must be greater than 0".to_string(),
            ));
        }
        Ok(())
    }
}

impl Default for EcAdminDedupReportRequest {
    fn default() -> Self {
        Self {
            max_entries: DEFAULT_EC_ADMIN_REPAIR_MAX_TASKS,
            local_only: false,
        }
    }
}

/// Request for explicitly enabled EC test hooks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EcAdminTestHookRequest {
    /// Target bucket.
    pub bucket: String,
    /// Target object key.
    pub key: String,
    /// Optional object version ID.
    #[serde(default)]
    pub version_id: Option<String>,
    /// Hook mode. Implementations decide which modes are accepted.
    pub mode: String,
}

/// Arm, inspect or release the transcode pause test hook.
///
/// The staged transcode states are too short-lived to observe from outside, so
/// the crash-safety scenarios need the pipeline to stop at a named state and
/// stay there until they say otherwise. Test-only: refused unless EC test hooks
/// are enabled for the process.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EcAdminTranscodePauseRequest {
    /// `arm`, `status` or `release`.
    pub action: String,
    /// Target bucket.
    pub bucket: String,
    /// Target object key.
    pub key: String,
    /// Optional object version ID.
    #[serde(default)]
    pub version_id: Option<String>,
    /// Transcode state to stop at, in the manifest's own spelling, for example
    /// `ec_encoding` or `ec_shards_durable`.
    pub state: String,
    /// Hook mode. Only `pause_once` is accepted.
    pub mode: String,
}

impl EcAdminTranscodePauseRequest {
    /// Validate the pause request before handing it to an implementation.
    ///
    /// # Errors
    ///
    /// Returns [`ClusterError::Config`] when a field is empty or unsupported.
    pub fn validate(&self) -> Result<(), ClusterError> {
        match self.action.as_str() {
            "arm" | "status" | "release" => {}
            other => {
                return Err(ClusterError::Config(format!(
                    "unsupported EC transcode pause action '{other}'; use arm, status or release"
                )))
            }
        }
        if self.bucket.trim().is_empty() {
            return Err(ClusterError::Config(
                "EC transcode pause bucket must not be empty".to_string(),
            ));
        }
        if self.key.is_empty() {
            return Err(ClusterError::Config(
                "EC transcode pause key must not be empty".to_string(),
            ));
        }
        if self.state.trim().is_empty() {
            return Err(ClusterError::Config(
                "EC transcode pause state must not be empty".to_string(),
            ));
        }
        if self.mode != "pause_once" {
            return Err(ClusterError::Config(format!(
                "unsupported EC transcode pause mode '{}'; use pause_once",
                self.mode
            )));
        }
        Ok(())
    }
}

impl EcAdminTestHookRequest {
    /// Validate the test-hook request before handing it to an implementation.
    pub fn validate(&self) -> Result<(), ClusterError> {
        if self.bucket.trim().is_empty() {
            return Err(ClusterError::Config(
                "EC test hook bucket must not be empty".to_string(),
            ));
        }
        if self.key.is_empty() {
            return Err(ClusterError::Config(
                "EC test hook key must not be empty".to_string(),
            ));
        }
        if self.mode.trim().is_empty() {
            return Err(ClusterError::Config(
                "EC test hook mode must not be empty".to_string(),
            ));
        }
        Ok(())
    }
}

/// Request for the codec test hook that forces a foreign generator matrix.
///
/// Unlike the object-scoped hooks this one is node-wide: it changes how the
/// accelerated engine encodes every chunk until it is disarmed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct EcAdminCodecHookRequest {
    /// Arm the hook when true, disarm it when false.
    pub enabled: bool,
}

/// Request to add an erasure set to an existing pool.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EcAdminAddSetRequest {
    /// Pool the set is added to.
    pub pool: String,
    /// Nodes that will hold the new set's slots, in slot order.
    ///
    /// Node ids are given the way every other EC admin route takes them: the
    /// display form of the id.
    pub node_ids: Vec<String>,
}

impl EcAdminAddSetRequest {
    /// Validate the add-set request before passing it to an implementation.
    pub fn validate(&self) -> Result<(), ClusterError> {
        if self.pool.trim().is_empty() {
            return Err(ClusterError::Config(
                "EC admin add-set pool must not be empty".to_string(),
            ));
        }
        if self.node_ids.is_empty() {
            return Err(ClusterError::Config(
                "EC admin add-set node_ids must not be empty".to_string(),
            ));
        }
        let mut seen = HashSet::with_capacity(self.node_ids.len());
        for node_id in &self.node_ids {
            let trimmed = node_id.trim();
            if trimmed.is_empty() {
                return Err(ClusterError::Config(
                    "EC admin add-set node_ids must not contain empty or blank node IDs"
                        .to_string(),
                ));
            }
            if !seen.insert(trimmed) {
                return Err(ClusterError::Config(format!(
                    "EC admin add-set node_ids contains duplicate node ID: {trimmed}"
                )));
            }
        }
        Ok(())
    }
}

/// Request to stop an erasure set taking new objects.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EcAdminDeactivateSetRequest {
    /// Pool the set belongs to.
    pub pool: String,
    /// Set to deactivate.
    pub set_id: u64,
}

impl EcAdminDeactivateSetRequest {
    /// Validate the deactivate-set request before passing it to an implementation.
    pub fn validate(&self) -> Result<(), ClusterError> {
        if self.pool.trim().is_empty() {
            return Err(ClusterError::Config(
                "EC admin deactivate-set pool must not be empty".to_string(),
            ));
        }
        Ok(())
    }
}

/// Request to accept a node's return to its pool.
///
/// The identity is part of the request and not taken from the connection on
/// purpose: an approval is addressed to one node about one return, and an
/// operator who reaches the wrong node's admin API must be told so rather than
/// have their approval land wherever the request happened to arrive.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EcAdminApproveRejoinRequest {
    /// Pool the node belongs to.
    pub pool: String,
    /// The identity whose return is being accepted, in its display form.
    pub node_id: String,
}

impl EcAdminApproveRejoinRequest {
    /// Validate the approve-rejoin request before passing it to an implementation.
    pub fn validate(&self) -> Result<(), ClusterError> {
        if self.pool.trim().is_empty() {
            return Err(ClusterError::Config(
                "EC admin rejoin approval pool must not be empty".to_string(),
            ));
        }
        if self.node_id.trim().is_empty() {
            return Err(ClusterError::Config(
                "EC admin rejoin approval must name the node whose return it accepts".to_string(),
            ));
        }
        Ok(())
    }
}

/// Request to lay an existing pool's sets out by this node's topology rule.
///
/// The rule is node configuration, never part of the request: a set must come
/// out the same on every node, and an operator sending a level with the request
/// could lay one set out by a rule the pool's nodes are not configured for.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EcAdminApplyTopologyRequest {
    /// Pool whose sets are to be laid out again.
    pub pool: String,
    /// The one set to apply the rule to, or every set of the pool when absent.
    pub set_id: Option<u64>,
    /// Report what would change and write nothing.
    #[serde(default)]
    pub dry_run: bool,
}

impl EcAdminApplyTopologyRequest {
    /// Validate the apply-topology request before passing it to an implementation.
    pub fn validate(&self) -> Result<(), ClusterError> {
        if self.pool.trim().is_empty() {
            return Err(ClusterError::Config(
                "EC admin apply-topology pool must not be empty".to_string(),
            ));
        }
        Ok(())
    }
}

/// Admin operations exposed by the EC runtime.
#[async_trait]
pub trait EcAdminService: Send + Sync + 'static {
    /// List EC pools known to this node.
    async fn list_pools(&self) -> Result<Value, ClusterError>;

    /// Return one EC pool health snapshot.
    async fn pool_health(&self, pool: &str) -> Result<Value, ClusterError>;

    /// Return the failure matrix the pool's codec documents.
    ///
    /// Every engine must publish the same one: a node that overclaimed what it
    /// can recover would keep serving reads it cannot actually reconstruct.
    async fn failure_matrix(&self, pool: &str) -> Result<Value, ClusterError>;

    /// Return a manifest for an EC object/version.
    async fn object_manifest(&self, object: EcAdminObjectRef) -> Result<Value, ClusterError>;

    /// Verify admin-visible EC metadata and local shard records for an object.
    async fn verify_object(&self, object: EcAdminObjectRef) -> Result<Value, ClusterError>;

    /// Queue bounded EC repair work for a pool or object scope.
    async fn repair_pool(&self, request: EcAdminRepairRequest) -> Result<Value, ClusterError>;

    /// Return repair queue status for a pool, and the rebuild progress of one
    /// slot owner when the request names one.
    async fn repair_status(
        &self,
        request: EcAdminRepairStatusRequest,
    ) -> Result<Value, ClusterError>;

    /// List local orphan EC shard candidates.
    async fn list_orphans(&self, max_entries: usize) -> Result<Value, ClusterError>;

    /// Run or dry-run local orphan shard GC.
    async fn gc_orphans(&self, request: EcAdminOrphanGcRequest) -> Result<Value, ClusterError>;

    /// Run one EC tombstone GC cycle on this node.
    ///
    /// Releases the shards of objects whose delete has finished propagating,
    /// and retires the tombstone itself pool-wide once this node's gates pass.
    /// The background sweep does the same work on a schedule; this makes it
    /// happen at a chosen moment, which is what an acceptance scenario needs in
    /// order to assert on the result rather than on a timer.
    async fn run_tombstone_gc(
        &self,
        request: EcAdminTombstoneGcRequest,
    ) -> Result<Value, ClusterError>;

    /// Return packed segments to ordinary RF=3 replication.
    ///
    /// Undoes packing for the segments it touches: their objects go back to
    /// three copies of their own and the segments are retired. This is what
    /// makes switching the feature off reversible, and it is deliberately an
    /// explicit operator action rather than something a background worker
    /// decides to do.
    async fn unpack_packed_segments(
        &self,
        request: EcAdminUnpackSegmentsRequest,
    ) -> Result<Value, ClusterError>;

    /// Execute an explicitly enabled EC test hook.
    async fn test_hook(&self, request: EcAdminTestHookRequest) -> Result<Value, ClusterError>;

    /// Arm, inspect or release the transcode pause test hook.
    async fn transcode_pause_hook(
        &self,
        request: EcAdminTranscodePauseRequest,
    ) -> Result<Value, ClusterError>;

    /// Sweep this node's shards for ones no live manifest references, and
    /// record what it finds as orphan candidates.
    ///
    /// The background scrub already looks for them; this makes the same look
    /// happen at a chosen moment, which is what an acceptance scenario needs
    /// to assert on the result rather than on a schedule.
    async fn scan_orphans(&self, max_entries: usize) -> Result<Value, ClusterError>;

    /// Arm or disarm the foreign generator matrix used to self-check byte
    /// equality between codec engines.
    async fn codec_test_hook(
        &self,
        request: EcAdminCodecHookRequest,
    ) -> Result<Value, ClusterError>;

    /// Run one local shard scrub pass now and report what it found.
    async fn run_scrub(&self) -> Result<Value, ClusterError>;

    /// Audit local shard inventory against every manifest now.
    async fn run_anti_entropy(&self) -> Result<Value, ClusterError>;

    /// Put back namespace entries that erasure-coded objects are missing, now.
    ///
    /// The background pass does the same work on the anti-entropy schedule;
    /// this makes it observable at a chosen moment, which is what an acceptance
    /// test needs in order to state that an object lost before the fix is
    /// listable again.
    async fn reconcile_namespace(&self) -> Result<Value, ClusterError>;

    /// Read the effective tiering policy of one bucket.
    ///
    /// Returns both what is stored for the bucket and the values actually in
    /// force after inheritance. Without the second half an operator cannot tell
    /// why an object did not migrate, and would look for the answer in the
    /// worker instead of in an inherited value.
    async fn tiering_policy(&self, bucket: String) -> Result<Value, ClusterError>;

    /// Store the tiering policy of one bucket.
    async fn put_tiering_policy(
        &self,
        bucket: String,
        request: EcAdminTieringPolicyRequest,
    ) -> Result<Value, ClusterError>;

    /// Report what tiering would do right now, changing nothing.
    async fn tiering_dry_run(
        &self,
        request: EcAdminTieringDryRunRequest,
    ) -> Result<Value, ClusterError>;

    /// Read the recorded activity mark of one object on this node.
    ///
    /// Activity marks are node-local and deliberately invisible to S3 clients,
    /// so this is the only way an acceptance test can state that a read did
    /// reach durable metadata within one flush interval.
    async fn object_heat(&self, object: EcAdminObjectRef) -> Result<Value, ClusterError>;

    /// Run one auto-tiering scan cycle immediately and report what it did.
    ///
    /// The background worker does the same on its interval, which is measured
    /// in hours; this makes the result observable at a chosen moment.
    async fn run_tiering_cycle(&self) -> Result<Value, ClusterError>;

    /// Queue erasure-coded objects under a prefix to come back to RF=3.
    ///
    /// The documented rollback for auto-tiering, so it works whether or not
    /// automatic reverse tiering is switched on, and whether or not tiering
    /// itself is: an operator who turned the feature off during an incident
    /// must not lose the way to undo what it did.
    async fn promote_objects(
        &self,
        request: EcAdminTieringPromoteRequest,
    ) -> Result<Value, ClusterError>;

    /// Advance the reverse migration queue immediately and report what it did.
    ///
    /// The background worker does the same on its interval; this makes the
    /// result observable at a chosen moment.
    async fn run_promotion_cycle(&self) -> Result<Value, ClusterError>;

    /// Read this node's reverse migration job for one object, if it has one.
    ///
    /// The queue is invisible to S3 clients and, until the promoted manifest is
    /// published, invisible to every other admin route as well. Without this an
    /// acceptance test cannot tell a promotion that was never queued from one
    /// that is queued and waiting.
    async fn promotion_job(&self, object: EcAdminObjectRef) -> Result<Value, ClusterError>;

    /// Read this node's forward migration job for one object, if it has one.
    ///
    /// The counterpart of [`Self::promotion_job`], and needed for the same
    /// reason. A staged transcode publishes no manifest until it commits, so
    /// between admission and commit the object looks — to every other admin
    /// route — exactly like one that was never queued. Without this an
    /// acceptance test cannot establish that a migration is actually running,
    /// which is the precondition for any statement about interrupting one.
    async fn transcode_job(&self, object: EcAdminObjectRef) -> Result<Value, ClusterError>;

    /// Add an erasure set to an existing pool.
    ///
    /// POST /api/admin/ec/pools/{pool}/sets
    async fn add_erasure_set(&self, request: EcAdminAddSetRequest) -> Result<Value, ClusterError>;

    /// List all erasure sets belonging to an EC pool.
    ///
    /// GET /api/admin/ec/pools/{pool}/sets
    async fn list_erasure_sets(&self, pool: &str) -> Result<Value, ClusterError>;

    /// Stop an erasure set from taking new objects.
    ///
    /// POST /api/admin/ec/pools/{pool}/sets/{set_id}/deactivate
    async fn deactivate_erasure_set(
        &self,
        request: EcAdminDeactivateSetRequest,
    ) -> Result<Value, ClusterError>;

    /// Lay a pool's erasure sets out again by this node's topology rule.
    ///
    /// POST /api/admin/ec/pools/{pool}/topology
    ///
    /// An explicit operator action and never a consequence of switching the
    /// rule on: only sets created after the switch, and sets named here, are
    /// laid out by it (EC2-084, point 2). Objects already stored keep their
    /// shards where they are and stay readable; only new objects follow the new
    /// layout.
    async fn apply_pool_topology(
        &self,
        request: EcAdminApplyTopologyRequest,
    ) -> Result<Value, ClusterError>;

    /// Accept a node's return to its pool after a long absence or a rebuild.
    ///
    /// POST /api/admin/ec/pools/{pool}/rejoin/approve
    ///
    /// Answered by the node that is held back, about itself: the verdict was
    /// reached from that node's own storage. It applies to this return only —
    /// a restart evaluates the evidence again and asks again.
    async fn approve_node_rejoin(
        &self,
        request: EcAdminApproveRejoinRequest,
    ) -> Result<Value, ClusterError>;

    /// Measure how much erasure-coded content is stored more than once.
    ///
    /// Read-only. The answer decides whether plan 07 is worth carrying out at
    /// all, so the report says how much of the pool it covered and refuses a
    /// verdict when that is less than all of it.
    async fn dedup_report(&self, request: EcAdminDedupReportRequest)
        -> Result<Value, ClusterError>;
}

/// CE/default implementation for EC admin routes.
#[derive(Debug, Default)]
pub struct NoopEcAdminService;

#[async_trait]
impl EcAdminService for NoopEcAdminService {
    async fn list_pools(&self) -> Result<Value, ClusterError> {
        Err(ec_required())
    }

    async fn pool_health(&self, _pool: &str) -> Result<Value, ClusterError> {
        Err(ec_required())
    }

    async fn failure_matrix(&self, _pool: &str) -> Result<Value, ClusterError> {
        Err(ec_required())
    }

    async fn object_manifest(&self, _object: EcAdminObjectRef) -> Result<Value, ClusterError> {
        Err(ec_required())
    }

    async fn verify_object(&self, _object: EcAdminObjectRef) -> Result<Value, ClusterError> {
        Err(ec_required())
    }

    async fn repair_pool(&self, _request: EcAdminRepairRequest) -> Result<Value, ClusterError> {
        Err(ec_required())
    }

    async fn repair_status(
        &self,
        _request: EcAdminRepairStatusRequest,
    ) -> Result<Value, ClusterError> {
        Err(ec_required())
    }

    async fn list_orphans(&self, _max_entries: usize) -> Result<Value, ClusterError> {
        Err(ec_required())
    }

    async fn gc_orphans(&self, _request: EcAdminOrphanGcRequest) -> Result<Value, ClusterError> {
        Err(ec_required())
    }

    async fn run_tombstone_gc(
        &self,
        _request: EcAdminTombstoneGcRequest,
    ) -> Result<Value, ClusterError> {
        Err(ec_required())
    }

    async fn unpack_packed_segments(
        &self,
        _request: EcAdminUnpackSegmentsRequest,
    ) -> Result<Value, ClusterError> {
        Err(ec_required())
    }

    async fn test_hook(&self, _request: EcAdminTestHookRequest) -> Result<Value, ClusterError> {
        Err(ec_required())
    }

    async fn transcode_pause_hook(
        &self,
        _request: EcAdminTranscodePauseRequest,
    ) -> Result<Value, ClusterError> {
        Err(ec_required())
    }

    async fn scan_orphans(&self, _max_entries: usize) -> Result<Value, ClusterError> {
        Err(ec_required())
    }

    async fn codec_test_hook(
        &self,
        _request: EcAdminCodecHookRequest,
    ) -> Result<Value, ClusterError> {
        Err(ec_required())
    }

    async fn run_scrub(&self) -> Result<Value, ClusterError> {
        Err(ec_required())
    }

    async fn run_anti_entropy(&self) -> Result<Value, ClusterError> {
        Err(ec_required())
    }

    async fn reconcile_namespace(&self) -> Result<Value, ClusterError> {
        Err(ec_required())
    }

    async fn tiering_policy(&self, _bucket: String) -> Result<Value, ClusterError> {
        Err(ec_required())
    }

    async fn put_tiering_policy(
        &self,
        _bucket: String,
        _request: EcAdminTieringPolicyRequest,
    ) -> Result<Value, ClusterError> {
        Err(ec_required())
    }

    async fn tiering_dry_run(
        &self,
        _request: EcAdminTieringDryRunRequest,
    ) -> Result<Value, ClusterError> {
        Err(ec_required())
    }

    async fn run_tiering_cycle(&self) -> Result<Value, ClusterError> {
        Err(ec_required())
    }

    async fn promote_objects(
        &self,
        _request: EcAdminTieringPromoteRequest,
    ) -> Result<Value, ClusterError> {
        Err(ec_required())
    }

    async fn run_promotion_cycle(&self) -> Result<Value, ClusterError> {
        Err(ec_required())
    }

    async fn promotion_job(&self, _object: EcAdminObjectRef) -> Result<Value, ClusterError> {
        Err(ec_required())
    }

    async fn transcode_job(&self, _object: EcAdminObjectRef) -> Result<Value, ClusterError> {
        Err(ec_required())
    }

    async fn object_heat(&self, _object: EcAdminObjectRef) -> Result<Value, ClusterError> {
        Err(ec_required())
    }

    async fn add_erasure_set(&self, _request: EcAdminAddSetRequest) -> Result<Value, ClusterError> {
        Err(ec_required())
    }

    async fn list_erasure_sets(&self, _pool: &str) -> Result<Value, ClusterError> {
        Err(ec_required())
    }

    async fn deactivate_erasure_set(
        &self,
        _request: EcAdminDeactivateSetRequest,
    ) -> Result<Value, ClusterError> {
        Err(ec_required())
    }

    async fn apply_pool_topology(
        &self,
        _request: EcAdminApplyTopologyRequest,
    ) -> Result<Value, ClusterError> {
        Err(ec_required())
    }

    async fn approve_node_rejoin(
        &self,
        _request: EcAdminApproveRejoinRequest,
    ) -> Result<Value, ClusterError> {
        Err(ec_required())
    }

    async fn dedup_report(
        &self,
        _request: EcAdminDedupReportRequest,
    ) -> Result<Value, ClusterError> {
        Err(ec_required())
    }
}

fn default_repair_max_tasks() -> usize {
    DEFAULT_EC_ADMIN_REPAIR_MAX_TASKS
}

fn default_dry_run_samples() -> usize {
    32
}

fn ec_required() -> ClusterError {
    ClusterError::EnterpriseRequired(
        "erasure coding admin operations require Enterprise Edition with an EC license".to_string(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repair_request_rejects_empty_pool() {
        let request = EcAdminRepairRequest::default();
        assert!(request.validate().is_err());
    }

    /// A status request without an owner is the queue report the route has
    /// always answered; one with a blank owner is an operator who meant to name
    /// a node and did not, and answering the pool-wide report there would look
    /// like an answer about that node.
    #[test]
    fn repair_status_request_takes_a_pool_and_optionally_one_owner() {
        assert!(EcAdminRepairStatusRequest::for_pool("ec").validate().is_ok());
        assert!(EcAdminRepairStatusRequest::for_pool("  ").validate().is_err());

        let owner = EcAdminRepairStatusRequest {
            pool: "ec".to_string(),
            node_id: Some("0d3bb4d8-0000-0000-0000-000000000000".to_string()),
        };
        assert!(owner.validate().is_ok());

        let blank_owner = EcAdminRepairStatusRequest {
            pool: "ec".to_string(),
            node_id: Some("   ".to_string()),
        };
        assert!(blank_owner.validate().is_err());
    }

    /// The pool has to be named, blank included: a request without one names no
    /// pool to lay out.
    #[test]
    fn apply_topology_request_rejects_an_empty_pool() {
        for pool in ["", "   "] {
            let request = EcAdminApplyTopologyRequest {
                pool: pool.to_string(),
                set_id: None,
                dry_run: false,
            };
            assert!(
                request.validate().is_err(),
                "pool {pool:?} should not be accepted"
            );
        }
    }

    /// Everything else is optional: no set id means every set of the pool, and
    /// a dry run happens only when it is asked for.
    #[test]
    fn apply_topology_request_accepts_a_pool_on_its_own() {
        let request = EcAdminApplyTopologyRequest {
            pool: "ec".to_string(),
            set_id: None,
            dry_run: false,
        };

        assert!(request.validate().is_ok());
        assert_eq!(request.set_id, None);
        assert!(!request.dry_run);
    }

    #[test]
    fn dedup_report_request_defaults_to_asking_the_whole_pool() {
        let request = EcAdminDedupReportRequest::default();

        // A node sees only the manifests it transcoded or read, so a repeat
        // split across two nodes is invisible on both. Defaulting to one node
        // would make the common call answer a different question than it looks
        // like it answers.
        assert!(!request.local_only);
        assert!(request.validate().is_ok());
    }

    #[test]
    fn dedup_report_request_rejects_a_limit_of_zero() {
        let request = EcAdminDedupReportRequest {
            max_entries: 0,
            local_only: false,
        };

        assert!(request.validate().is_err());
    }

    #[tokio::test]
    async fn noop_service_rejects_the_dedup_report() {
        let service = NoopEcAdminService;

        let err = service.dedup_report(EcAdminDedupReportRequest::default()).await.unwrap_err();

        assert!(matches!(err, ClusterError::EnterpriseRequired(_)));
    }

    #[test]
    fn object_ref_rejects_empty_key() {
        let object = EcAdminObjectRef {
            bucket: "bucket".to_string(),
            key: String::new(),
            version_id: None,
        };
        assert!(object.validate().is_err());
    }

    #[tokio::test]
    async fn noop_service_returns_enterprise_required() {
        let service = NoopEcAdminService;
        let err = service.list_pools().await.unwrap_err();
        assert!(matches!(err, ClusterError::EnterpriseRequired(_)));
    }

    #[test]
    fn add_set_request_accepts_valid() {
        let request = EcAdminAddSetRequest {
            pool: "pool-0".to_string(),
            node_ids: vec!["node-1".to_string(), "node-2".to_string()],
        };
        assert!(request.validate().is_ok());
    }

    #[test]
    fn add_set_request_rejects_empty_pool() {
        let request = EcAdminAddSetRequest {
            pool: String::new(),
            node_ids: vec!["node-1".to_string()],
        };
        let err = request.validate().unwrap_err();
        assert!(matches!(err, ClusterError::Config(_)));
        assert!(
            err.to_string().contains("pool must not be empty"),
            "unexpected error message: {err}"
        );

        let request_blank = EcAdminAddSetRequest {
            pool: "   ".to_string(),
            node_ids: vec!["node-1".to_string()],
        };
        let err_blank = request_blank.validate().unwrap_err();
        assert!(matches!(err_blank, ClusterError::Config(_)));
        assert!(
            err_blank.to_string().contains("pool must not be empty"),
            "unexpected error message: {err_blank}"
        );
    }

    #[test]
    fn add_set_request_rejects_empty_node_ids() {
        let request = EcAdminAddSetRequest {
            pool: "pool-0".to_string(),
            node_ids: vec![],
        };
        let err = request.validate().unwrap_err();
        assert!(matches!(err, ClusterError::Config(_)));
        assert!(
            err.to_string().contains("node_ids must not be empty"),
            "unexpected error message: {err}"
        );
    }

    #[test]
    fn add_set_request_rejects_empty_or_blank_node_id() {
        let request_empty = EcAdminAddSetRequest {
            pool: "pool-0".to_string(),
            node_ids: vec!["node-1".to_string(), String::new()],
        };
        let err_empty = request_empty.validate().unwrap_err();
        assert!(matches!(err_empty, ClusterError::Config(_)));
        assert!(
            err_empty.to_string().contains("empty or blank"),
            "unexpected error message: {err_empty}"
        );

        let request_blank = EcAdminAddSetRequest {
            pool: "pool-0".to_string(),
            node_ids: vec!["   ".to_string(), "node-2".to_string()],
        };
        let err_blank = request_blank.validate().unwrap_err();
        assert!(matches!(err_blank, ClusterError::Config(_)));
        assert!(
            err_blank.to_string().contains("empty or blank"),
            "unexpected error message: {err_blank}"
        );
    }

    #[test]
    fn add_set_request_rejects_duplicate_node_ids() {
        let request = EcAdminAddSetRequest {
            pool: "pool-0".to_string(),
            node_ids: vec![
                "node-1".to_string(),
                "node-2".to_string(),
                "node-1".to_string(),
            ],
        };
        let err = request.validate().unwrap_err();
        assert!(matches!(err, ClusterError::Config(_)));
        assert!(
            err.to_string().contains("duplicate node ID: node-1"),
            "unexpected error message: {err}"
        );

        // Also ensure whitespace variations of duplicate IDs are detected.
        let request_trimmed_dup = EcAdminAddSetRequest {
            pool: "pool-0".to_string(),
            node_ids: vec!["node-1".to_string(), " node-1 ".to_string()],
        };
        let err_trimmed_dup = request_trimmed_dup.validate().unwrap_err();
        assert!(matches!(err_trimmed_dup, ClusterError::Config(_)));
        assert!(
            err_trimmed_dup.to_string().contains("duplicate node ID: node-1"),
            "unexpected error message: {err_trimmed_dup}"
        );
    }

    #[test]
    fn deactivate_set_request_accepts_valid() {
        let request = EcAdminDeactivateSetRequest {
            pool: "pool-0".to_string(),
            set_id: 1,
        };
        assert!(request.validate().is_ok());

        let request_zero = EcAdminDeactivateSetRequest {
            pool: "pool-0".to_string(),
            set_id: 0,
        };
        assert!(request_zero.validate().is_ok());
    }

    #[test]
    fn deactivate_set_request_rejects_empty_pool() {
        let request = EcAdminDeactivateSetRequest {
            pool: String::new(),
            set_id: 1,
        };
        let err = request.validate().unwrap_err();
        assert!(matches!(err, ClusterError::Config(_)));
        assert!(
            err.to_string().contains("pool must not be empty"),
            "unexpected error message: {err}"
        );

        let request_blank = EcAdminDeactivateSetRequest {
            pool: "   ".to_string(),
            set_id: 1,
        };
        let err_blank = request_blank.validate().unwrap_err();
        assert!(matches!(err_blank, ClusterError::Config(_)));
        assert!(
            err_blank.to_string().contains("pool must not be empty"),
            "unexpected error message: {err_blank}"
        );
    }

    #[tokio::test]
    async fn noop_service_rejects_erasure_set_operations() {
        let service = NoopEcAdminService;
        let add_err = service
            .add_erasure_set(EcAdminAddSetRequest {
                pool: "pool-0".to_string(),
                node_ids: vec!["node-1".to_string()],
            })
            .await
            .unwrap_err();
        assert!(matches!(add_err, ClusterError::EnterpriseRequired(_)));

        let list_err = service.list_erasure_sets("pool-0").await.unwrap_err();
        assert!(matches!(list_err, ClusterError::EnterpriseRequired(_)));

        let deact_err = service
            .deactivate_erasure_set(EcAdminDeactivateSetRequest {
                pool: "pool-0".to_string(),
                set_id: 1,
            })
            .await
            .unwrap_err();
        assert!(matches!(deact_err, ClusterError::EnterpriseRequired(_)));
    }
}
#[test]
fn tombstone_gc_request_cursor_round_trips_and_defaults_to_absent() {
    let cursor = vec![0, 1, 0x7f, 0x80, 0xff];
    let request = EcAdminTombstoneGcRequest {
        dry_run: true,
        max_entries: 1,
        cursor: Some(EcAdminTombstoneGcRequest::encode_cursor(&cursor)),
    };

    assert_eq!(request.decoded_cursor().unwrap(), Some(cursor));
    let defaulted: EcAdminTombstoneGcRequest =
        serde_json::from_str(r#"{"dry_run":true,"max_entries":1}"#).unwrap();
    assert_eq!(defaulted.cursor, None);
}

#[test]
fn tombstone_gc_request_rejects_malformed_and_oversized_cursors() {
    for cursor in ["", "0", "gg"] {
        let request = EcAdminTombstoneGcRequest {
            dry_run: true,
            max_entries: 1,
            cursor: Some(cursor.to_string()),
        };
        assert!(matches!(request.validate(), Err(ClusterError::Config(_))));
    }

    let oversized = EcAdminTombstoneGcRequest {
        dry_run: true,
        max_entries: 1,
        cursor: Some("00".repeat(16 * 1024 + 1)),
    };
    assert!(matches!(oversized.validate(), Err(ClusterError::Config(_))));
}
