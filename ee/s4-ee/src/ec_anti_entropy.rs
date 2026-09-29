// Copyright 2026 S4Core Team
//
// Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

//! Erasure Coding v1 anti-entropy and scrubbing.
//!
//! This module owns the Enterprise-only Phase 7 boundary. It builds Merkle
//! fingerprints for EC manifests, reconciles manifest divergence through
//! quorum metadata reads, audits node-local shard inventory against the
//! manifest authority, reports orphan candidates without deleting them, and
//! verifies physical shard bytes during scrub cycles.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use s4_cluster::repair::merkle::{record_hash, ring_position as merkle_ring_position};
use s4_cluster::repair::{KeyRange, MerkleTree};
use s4_cluster::NodeId;
use thiserror::Error;

use crate::ec_metadata::{
    EcManifestReadResult, EcMetadataError, EcMetadataStore, EcObjectManifest, EcObjectVersionKey,
    EcPlacementState, EcRepairKind, EcRepairTaskRecord, LocalShardKey, LocalShardRecord,
    LocalShardState, ShardManifest,
};
use crate::ec_repair::{EcRepairCoordinator, EcRepairError, EcRepairQueueRequest};
use crate::erasure_coding::{LayoutViolationOrigin, SetLayoutCompliance};

/// Default EC manifest Merkle tree depth.
pub const DEFAULT_EC_AE_MERKLE_DEPTH: u8 = 15;
/// Maximum EC manifest Merkle tree depth accepted from configuration.
pub const MAX_EC_AE_MERKLE_DEPTH: u8 = 20;
/// Default maximum local manifests scanned in one anti-entropy pass.
pub const DEFAULT_EC_AE_MAX_MANIFEST_SCAN_ENTRIES: usize = 10_000;
/// Default maximum local shard inventory entries scanned in one scrub pass.
pub const DEFAULT_EC_AE_MAX_SHARD_SCAN_ENTRIES: usize = 10_000;
/// Default maximum reconstructed chunk samples per committed object.
pub const DEFAULT_EC_SCRUB_RECONSTRUCTION_SAMPLE_CHUNKS: usize = 3;
/// Share of a scrub pass that may fail on the medium before the pass stops and
/// calls the medium suspect instead of judging shards one by one.
///
/// A quarter of the shards a pass looked at is far above what rot produces and
/// far below a healthy disk's noise (decision EC2-200).
pub const DEFAULT_EC_SCRUB_MEDIA_FAILURE_RATIO: f64 = 0.25;
/// Fewest media failures a pass needs before the ratio above means anything.
///
/// One failure out of two shards is not evidence about a disk. This is the same
/// reason a volume scanner counts failures before condemning a volume rather
/// than acting on the first one.
const EC_SCRUB_MEDIA_FAILURE_MIN_SAMPLES: usize = 4;

/// What one layout-drift check found about a pool's erasure sets.
///
/// The check asks one question of every set: does the layout it is recorded
/// with still keep the guarantee of the rule it was laid out by, under the
/// topology labels this node holds now? A node moved between racks is the usual
/// way the answer changes.
///
/// Drift is detected and reported, never corrected: the recorded layout stays
/// as it is, and moving a set to another layout is an explicit operator action
/// (EC2-086, point 3). The check reads no shards and no manifests — it compares
/// the sets this node already has in memory against its labels — which is what
/// makes it cheap enough to run inside the anti-entropy pass.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EcLayoutDriftReport {
    /// Sets examined.
    pub sets_checked: usize,
    /// Sets whose rule makes no placement guarantee, so nothing can breach it.
    /// Every EC v1 set is counted here.
    pub sets_without_guarantee: usize,
    /// Sets keeping the guarantee of their rule.
    pub sets_meeting_rule: usize,
    /// Sets that kept their rule when they were laid out and do not now.
    pub sets_drifted: usize,
    /// Sets created in breach of their rule under `warn`: violators since
    /// birth, which is not drift.
    pub sets_violated_at_creation: usize,
    /// Sets whose rule cannot be judged from this node's labels.
    pub sets_not_judgeable: usize,
    /// Every set breaching its rule now, and where the breach came from.
    pub violations: HashMap<u64, LayoutViolationOrigin>,
}

impl EcLayoutDriftReport {
    /// Sets breaching their rule now, whatever the breach came from.
    pub fn violators(&self) -> usize {
        self.violations.len()
    }
}

/// Fold each set's compliance into one report.
///
/// Takes the verdicts rather than computing them, so this stays free of the
/// erasure-set registry: judging a set against its own rule is the builder's
/// job, and it holds the labels to do it with.
pub fn collect_layout_drift(
    assessments: impl IntoIterator<Item = (u64, SetLayoutCompliance)>,
) -> EcLayoutDriftReport {
    let mut report = EcLayoutDriftReport::default();
    for (set_id, compliance) in assessments {
        report.sets_checked = report.sets_checked.saturating_add(1);
        match compliance {
            SetLayoutCompliance::NoGuarantee => {
                report.sets_without_guarantee = report.sets_without_guarantee.saturating_add(1);
            }
            SetLayoutCompliance::Meets => {
                report.sets_meeting_rule = report.sets_meeting_rule.saturating_add(1);
            }
            SetLayoutCompliance::NotJudgeable { .. } => {
                report.sets_not_judgeable = report.sets_not_judgeable.saturating_add(1);
            }
            SetLayoutCompliance::Violated { origin, .. } => {
                match origin {
                    LayoutViolationOrigin::Drift => {
                        report.sets_drifted = report.sets_drifted.saturating_add(1);
                    }
                    LayoutViolationOrigin::Creation => {
                        report.sets_violated_at_creation =
                            report.sets_violated_at_creation.saturating_add(1);
                    }
                }
                report.violations.insert(set_id, origin);
            }
        }
    }
    report
}

/// EC anti-entropy and scrubber configuration.
///
/// Compared by value but not `Eq`: the media failure ratio is a fraction, and a
/// fraction has no total equality to derive.
#[derive(Debug, Clone, PartialEq)]
pub struct EcAntiEntropyConfig {
    /// Merkle depth used for manifest fingerprints.
    pub merkle_depth: u8,
    /// Maximum manifests scanned while building a local manifest tree.
    pub max_manifest_scan_entries: usize,
    /// Maximum local shard records scanned by one scrub cycle.
    pub max_shard_scan_entries: usize,
    /// Maximum chunks sampled per object during reconstruction verification.
    pub reconstruction_sample_chunks: usize,
    /// Share of a pass that may fail on the medium before the pass stops.
    ///
    /// Above this — and above the floor of `EC_SCRUB_MEDIA_FAILURE_MIN_SAMPLES`
    /// failures a pass needs before a share means anything — the finding is
    /// about the disk, not about the shards, so the pass says so and stops
    /// rather than working through an inventory it cannot read (decision
    /// EC2-200). The floor is not configurable, which is why it is named here
    /// rather than linked: it is an implementation detail of the pass.
    pub media_failure_ratio: f64,
}

impl EcAntiEntropyConfig {
    /// Parse `S4_EC_AE_*` and `S4_EC_SCRUB_*` settings from the environment.
    pub fn from_env() -> Result<Self, EcAntiEntropyError> {
        let config = Self {
            merkle_depth: parse_optional_u8("S4_EC_AE_MERKLE_DEPTH")?
                .unwrap_or(DEFAULT_EC_AE_MERKLE_DEPTH),
            max_manifest_scan_entries: parse_optional_usize("S4_EC_AE_MAX_MANIFEST_SCAN_ENTRIES")?
                .unwrap_or(DEFAULT_EC_AE_MAX_MANIFEST_SCAN_ENTRIES),
            max_shard_scan_entries: parse_optional_usize("S4_EC_AE_MAX_SHARD_SCAN_ENTRIES")?
                .unwrap_or(DEFAULT_EC_AE_MAX_SHARD_SCAN_ENTRIES),
            reconstruction_sample_chunks: parse_optional_usize(
                "S4_EC_SCRUB_RECONSTRUCTION_SAMPLE_CHUNKS",
            )?
            .unwrap_or(DEFAULT_EC_SCRUB_RECONSTRUCTION_SAMPLE_CHUNKS),
            media_failure_ratio: parse_optional_f64("S4_EC_SCRUB_MEDIA_FAILURE_RATIO")?
                .unwrap_or(DEFAULT_EC_SCRUB_MEDIA_FAILURE_RATIO),
        };
        config.validate()?;
        Ok(config)
    }

    /// Validate configuration values.
    pub fn validate(&self) -> Result<(), EcAntiEntropyError> {
        if self.merkle_depth == 0 || self.merkle_depth > MAX_EC_AE_MERKLE_DEPTH {
            return Err(EcAntiEntropyError::InvalidConfig {
                name: "S4_EC_AE_MERKLE_DEPTH",
                reason: format!("must be between 1 and {MAX_EC_AE_MERKLE_DEPTH}"),
            });
        }
        for (name, value) in [
            (
                "S4_EC_AE_MAX_MANIFEST_SCAN_ENTRIES",
                self.max_manifest_scan_entries,
            ),
            (
                "S4_EC_AE_MAX_SHARD_SCAN_ENTRIES",
                self.max_shard_scan_entries,
            ),
        ] {
            if value == 0 {
                return Err(EcAntiEntropyError::InvalidConfig {
                    name,
                    reason: "must be greater than 0".to_string(),
                });
            }
        }
        if !(self.media_failure_ratio > 0.0 && self.media_failure_ratio <= 1.0) {
            return Err(EcAntiEntropyError::InvalidConfig {
                name: "S4_EC_SCRUB_MEDIA_FAILURE_RATIO",
                reason: "must be greater than 0 and at most 1".to_string(),
            });
        }
        Ok(())
    }
}

impl Default for EcAntiEntropyConfig {
    fn default() -> Self {
        Self {
            merkle_depth: DEFAULT_EC_AE_MERKLE_DEPTH,
            max_manifest_scan_entries: DEFAULT_EC_AE_MAX_MANIFEST_SCAN_ENTRIES,
            max_shard_scan_entries: DEFAULT_EC_AE_MAX_SHARD_SCAN_ENTRIES,
            reconstruction_sample_chunks: DEFAULT_EC_SCRUB_RECONSTRUCTION_SAMPLE_CHUNKS,
            media_failure_ratio: DEFAULT_EC_SCRUB_MEDIA_FAILURE_RATIO,
        }
    }
}

/// Metrics collected by EC anti-entropy and scrubbing.
#[derive(Debug, Default)]
pub struct EcAntiEntropyMetrics {
    /// Manifest Merkle mismatches detected.
    pub manifest_mismatches_total: AtomicU64,
    /// Local manifests updated from quorum metadata after divergence.
    pub manifest_repairs_total: AtomicU64,
    /// Local shard records missing for manifest-owned slots.
    pub shard_missing_total: AtomicU64,
    /// Shard inventory or bytes failed manifest-authoritative verification.
    pub shard_corrupt_total: AtomicU64,
    /// Local shards that no live manifest references.
    pub orphan_shards_total: AtomicU64,
    /// Repair tasks successfully enqueued by anti-entropy or scrubbing.
    pub repair_enqueues_total: AtomicU64,
    /// Repair enqueue attempts that failed.
    pub repair_enqueue_failures_total: AtomicU64,
    /// BlobHeader CRC verification failures reported by the physical reader.
    pub blob_header_crc_failures_total: AtomicU64,
    /// Physical bytes failed the local shard record hash.
    pub physical_hash_mismatches_total: AtomicU64,
    /// Physical bytes failed the manifest shard hash.
    pub manifest_hash_mismatches_total: AtomicU64,
    /// Shards this node holds a durable record for and could not produce.
    pub physical_read_failures_total: AtomicU64,
    /// Shard reads the medium did not answer at all (decision EC2-200).
    pub media_unavailable_total: AtomicU64,
    /// EC object reconstruction samples that failed.
    pub reconstruction_sample_failures_total: AtomicU64,
}

impl EcAntiEntropyMetrics {
    /// Return a point-in-time metrics snapshot.
    pub fn snapshot(&self) -> EcAntiEntropyMetricsSnapshot {
        EcAntiEntropyMetricsSnapshot {
            manifest_mismatches_total: load(&self.manifest_mismatches_total),
            manifest_repairs_total: load(&self.manifest_repairs_total),
            shard_missing_total: load(&self.shard_missing_total),
            shard_corrupt_total: load(&self.shard_corrupt_total),
            orphan_shards_total: load(&self.orphan_shards_total),
            repair_enqueues_total: load(&self.repair_enqueues_total),
            repair_enqueue_failures_total: load(&self.repair_enqueue_failures_total),
            blob_header_crc_failures_total: load(&self.blob_header_crc_failures_total),
            physical_hash_mismatches_total: load(&self.physical_hash_mismatches_total),
            manifest_hash_mismatches_total: load(&self.manifest_hash_mismatches_total),
            physical_read_failures_total: load(&self.physical_read_failures_total),
            media_unavailable_total: load(&self.media_unavailable_total),
            reconstruction_sample_failures_total: load(&self.reconstruction_sample_failures_total),
        }
    }
}

/// Point-in-time EC anti-entropy metrics snapshot.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EcAntiEntropyMetricsSnapshot {
    /// Manifest Merkle mismatches detected.
    pub manifest_mismatches_total: u64,
    /// Local manifests updated from quorum metadata after divergence.
    pub manifest_repairs_total: u64,
    /// Local shard records missing for manifest-owned slots.
    pub shard_missing_total: u64,
    /// Shard inventory or bytes failed manifest-authoritative verification.
    pub shard_corrupt_total: u64,
    /// Local shards that no live manifest references.
    pub orphan_shards_total: u64,
    /// Repair tasks successfully enqueued by anti-entropy or scrubbing.
    pub repair_enqueues_total: u64,
    /// Repair enqueue attempts that failed.
    pub repair_enqueue_failures_total: u64,
    /// BlobHeader CRC verification failures reported by the physical reader.
    pub blob_header_crc_failures_total: u64,
    /// Physical bytes failed the local shard record hash.
    pub physical_hash_mismatches_total: u64,
    /// Physical bytes failed the manifest shard hash.
    pub manifest_hash_mismatches_total: u64,
    /// Shards this node holds a durable record for and could not produce.
    pub physical_read_failures_total: u64,
    /// Shard reads the medium did not answer at all (decision EC2-200).
    pub media_unavailable_total: u64,
    /// EC object reconstruction samples that failed.
    pub reconstruction_sample_failures_total: u64,
}

/// One manifest fingerprint entry in the manifest Merkle tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EcManifestTreeEntry {
    /// Object version represented by this manifest or tombstone.
    pub object: EcObjectVersionKey,
    /// Ring position used by the Merkle tree.
    pub ring_position: u64,
    /// Manifest/tombstone record hash inserted into the tree.
    pub record_hash: [u8; 32],
    /// SHA-256 of the canonical serialized manifest.
    pub manifest_hash_sha256: [u8; 32],
    /// True if this entry represents a tombstone.
    pub is_tombstone: bool,
}

/// Manifest Merkle snapshot exchanged between EC metadata peers.
#[derive(Debug, Clone)]
pub struct EcManifestTreeSnapshot {
    /// Erasure set whose manifests this snapshot covers.
    pub erasure_set_id: u64,
    /// Key range covered by this snapshot.
    pub key_range: KeyRange,
    /// Merkle tree depth.
    pub merkle_depth: u8,
    /// Root hash for quick equality checks.
    pub root_hash: [u8; 32],
    /// Node hashes indexed by flat Merkle node index.
    pub node_hashes: HashMap<usize, [u8; 32]>,
    /// Manifest entries known by the sender for this snapshot.
    pub entries: Vec<EcManifestTreeEntry>,
    tree: MerkleTree,
}

impl EcManifestTreeSnapshot {
    /// Build a manifest tree snapshot from local manifest records for a specific erasure set.
    pub fn from_manifests(
        erasure_set_id: u64,
        manifests: Vec<EcObjectManifest>,
        merkle_depth: u8,
    ) -> Result<Self, EcAntiEntropyError> {
        let key_range = KeyRange::full();
        let mut entries = Vec::with_capacity(manifests.len());
        let mut merkle_entries = Vec::with_capacity(manifests.len());

        for manifest in manifests {
            // Skip manifests belonging to other erasure sets; each set maintains its own tree.
            if manifest.erasure_set_id != erasure_set_id {
                continue;
            }
            manifest.validate()?;
            let object = manifest.object_key();
            let ring_position = manifest_ring_position(&object);
            let manifest_hash_sha256 = manifest.serialized_sha256()?;
            let record_hash = record_hash(
                &manifest_tree_key(&object),
                &manifest_hash_sha256,
                manifest.hlc.wall_time,
                manifest.hlc.logical,
            );
            merkle_entries.push((ring_position, record_hash));
            entries.push(EcManifestTreeEntry {
                object,
                ring_position,
                record_hash,
                manifest_hash_sha256,
                is_tombstone: manifest.is_deleted || manifest.state == EcPlacementState::Tombstone,
            });
        }

        entries.sort_unstable_by(|left, right| {
            manifest_tree_key(&left.object).cmp(&manifest_tree_key(&right.object))
        });
        let tree = MerkleTree::build(&merkle_entries, key_range, merkle_depth);
        let root_hash = tree.root_hash();
        let node_hashes = all_node_hashes(&tree);

        Ok(Self {
            erasure_set_id,
            key_range,
            merkle_depth,
            root_hash,
            node_hashes,
            entries,
            tree,
        })
    }

    /// Find leaf ranges that differ between this local snapshot and a remote snapshot.
    pub fn divergent_ranges_against(
        &self,
        remote: &Self,
    ) -> Result<Vec<KeyRange>, EcAntiEntropyError> {
        if self.erasure_set_id != remote.erasure_set_id {
            return Err(EcAntiEntropyError::ErasureSetMismatch {
                local: self.erasure_set_id,
                remote: remote.erasure_set_id,
            });
        }
        if self.merkle_depth != remote.merkle_depth || self.key_range != remote.key_range {
            return Ok(vec![self.key_range]);
        }
        if self.root_hash == remote.root_hash {
            return Ok(Vec::new());
        }
        Ok(self.tree.find_divergent_ranges(&remote.node_hashes))
    }
}

/// Manifest reconciliation result for one remote comparison.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EcManifestReconcileReport {
    /// True when local and remote root hashes matched.
    pub roots_matched: bool,
    /// Divergent Merkle leaf ranges found.
    pub divergent_ranges: usize,
    /// Object versions selected for quorum reconciliation.
    pub candidate_objects: usize,
    /// Live manifests cached or refreshed locally.
    pub present_manifests_cached: usize,
    /// Tombstone manifests cached or refreshed locally.
    pub tombstones_cached: usize,
    /// Object versions quorum-confirmed absent and removed from local cache.
    pub absent_confirmed: usize,
    /// Local manifest records whose hash changed after quorum read.
    pub manifests_updated: usize,
}

/// Local shard inventory audit result for one manifest.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EcShardInventoryAuditReport {
    /// Local shard slots expected from the manifest for this node.
    pub expected_local_shards: usize,
    /// Expected local shard records that match manifest metadata.
    pub healthy_local_shards: usize,
    /// Expected local shard records that are missing or not durable.
    pub missing_local_shards: usize,
    /// Local shard records that disagree with the manifest.
    pub corrupt_local_shards: usize,
    /// Corrupt shard records sent to quarantine.
    pub quarantined_shards: usize,
    /// Quarantine attempts that failed.
    pub quarantine_failures: usize,
    /// Repair tasks successfully queued.
    pub repairs_queued: usize,
    /// Repair queue attempts that failed.
    pub repair_enqueue_failures: usize,
}

/// Shard scrub cycle result.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EcShardScrubReport {
    /// Local shard records scanned.
    pub local_shards_scanned: usize,
    /// Shards whose physical bytes matched both local metadata and manifest.
    pub healthy_shards: usize,
    /// Missing physical shards or non-durable local records.
    pub missing_shards: usize,
    /// Corrupt physical shards.
    pub corrupt_shards: usize,
    /// BlobHeader CRC failures.
    pub blob_header_crc_failures: usize,
    /// Physical bytes mismatched the local shard record hash.
    pub physical_hash_mismatches: usize,
    /// Physical bytes mismatched the manifest hash.
    pub manifest_hash_mismatches: usize,
    /// Shards this node holds a durable record for and could not produce.
    ///
    /// Counted inside `corrupt_shards` like the three above it: a node that
    /// cannot hand over bytes it claims to hold has lost them, whether the
    /// storage layer refused them for failing their own hash or could not read
    /// them at all.
    pub physical_read_failures: usize,
    /// Shards the medium did not answer for.
    ///
    /// Deliberately outside `corrupt_shards`: nothing was learned about these
    /// bytes, so nothing is claimed about them. They are neither quarantined
    /// nor repaired, and the next pass asks again (decision EC2-200).
    pub media_unavailable: usize,
    /// Whether this pass stopped because the medium, rather than the shards,
    /// looks to be at fault.
    ///
    /// Set when media failures reach both their floor and their share of the
    /// pass. A pass that stopped this way judged no further shard and skipped
    /// its reconstruction sample, whose failure on a dead medium would be the
    /// same false signal.
    pub media_suspect: bool,
    /// Orphan shard candidates found and retained for later GC policy.
    pub orphan_candidates: Vec<EcOrphanShardCandidate>,
    /// Repair tasks successfully queued.
    pub repairs_queued: usize,
    /// Of those, the ones queued for a shard that was missing rather than
    /// corrupt. A caller watching for silent corruption has to tell the two
    /// apart: a missing shard is a placement problem, not disagreeing bytes.
    pub missing_shard_repairs_queued: usize,
    /// Repair queue attempts that failed.
    pub repair_enqueue_failures: usize,
    /// Corrupt shards sent to quarantine.
    pub quarantined_shards: usize,
    /// Quarantine attempts that failed.
    pub quarantine_failures: usize,
    /// Committed object reconstruction probes that passed.
    pub reconstruction_samples_verified: usize,
    /// Committed object reconstruction probes that failed.
    pub reconstruction_samples_failed: usize,
    /// Reconstruction probes skipped by the verifier.
    pub reconstruction_samples_skipped: usize,
}

/// Local shard that has no live manifest reference.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EcOrphanShardCandidate {
    /// Local shard key.
    pub key: LocalShardKey,
    /// Local shard record.
    pub record: LocalShardRecord,
    /// Erasure set the manifest named, when the pass had a manifest to read it
    /// from.
    ///
    /// `None` for the one case where there is no manifest at all, which is also
    /// the case where nothing can say which set the shard was meant for.
    pub erasure_set_id: Option<u64>,
    /// Human-readable reason. GC must re-check leases and quorum state later.
    pub reason: String,
}

struct MissingShardFinding<'a> {
    manifest: &'a EcObjectManifest,
    chunk_index: u32,
    expected: &'a ShardManifest,
    reason: String,
}

struct CorruptShardFinding<'a> {
    manifest: &'a EcObjectManifest,
    chunk_index: u32,
    expected: &'a ShardManifest,
    observed: EcObservedShardIntegrity,
    reason: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CorruptShardAction {
    repair_queued: bool,
    quarantined: bool,
}

/// Observed integrity fields for a corrupt shard report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EcObservedShardIntegrity {
    /// Observed byte length.
    pub size: u64,
    /// Observed CRC32.
    pub crc32: u32,
    /// Observed SHA-256.
    pub sha256: [u8; 32],
}

impl EcObservedShardIntegrity {
    fn from_record(record: &LocalShardRecord) -> Self {
        Self {
            size: record.size,
            crc32: record.crc32,
            sha256: record.sha256,
        }
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, EcAntiEntropyError> {
        let size = u64::try_from(bytes.len()).map_err(|_| EcAntiEntropyError::ShardRead {
            reason: "shard byte length does not fit in u64".to_string(),
        })?;
        Ok(Self {
            size,
            crc32: crc32fast::hash(bytes),
            sha256: sha256(bytes),
        })
    }
}

/// Request to quarantine a corrupt local shard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EcShardQuarantineRequest {
    /// Object version addressed by the shard.
    pub object: EcObjectVersionKey,
    /// Operation that produced the winning manifest.
    pub operation_id: uuid::Uuid,
    /// Erasure set that owns the shard.
    pub erasure_set_id: u64,
    /// Logical chunk index.
    pub chunk_index: u32,
    /// Shard slot index.
    pub slot_index: u8,
    /// Shard owner node.
    pub node_id: NodeId,
    /// Expected manifest integrity.
    pub expected: ShardManifest,
    /// Observed local integrity.
    pub observed: EcObservedShardIntegrity,
    /// Human-readable reason.
    pub reason: String,
}

/// Port used to quarantine corrupt local shard data.
#[async_trait]
pub trait EcShardQuarantine: Send + Sync {
    /// Quarantine a corrupt shard so it is not served as a valid candidate.
    async fn quarantine_corrupt_shard(
        &self,
        request: EcShardQuarantineRequest,
    ) -> Result<(), EcAntiEntropyError>;
}

/// No-op quarantine implementation for tests and partial integrations.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopEcShardQuarantine;

#[async_trait]
impl EcShardQuarantine for NoopEcShardQuarantine {
    async fn quarantine_corrupt_shard(
        &self,
        _request: EcShardQuarantineRequest,
    ) -> Result<(), EcAntiEntropyError> {
        Ok(())
    }
}

/// Request for a local physical shard read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EcLocalShardReadRequest {
    /// Local shard key.
    pub key: LocalShardKey,
    /// Node-local volume/offset metadata.
    pub record: LocalShardRecord,
}

/// Response from a local physical shard read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EcLocalShardReadResponse {
    /// Shard bytes read from local volume storage.
    pub data: Bytes,
    /// Whether the storage layer verified the BlobHeader CRC.
    pub blob_header_crc_valid: bool,
    /// BlobHeader CRC value reported by the storage layer, for diagnostics.
    pub blob_header_crc32: Option<u32>,
}

/// What a node-local physical shard read found.
///
/// Four outcomes, because four different things are being said about the same
/// shard. A shard the node does not hold is a placement problem: the bytes were
/// never written here, or have already been collected. A shard the node holds a
/// durable record for and whose bytes do not match what was recorded for them
/// is rot: the storage layer read them and refused them (decision EC2-185). A
/// medium that did not answer at all says nothing about these bytes, and is
/// therefore not a verdict on the shard (decision EC2-200).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EcLocalShardRead {
    /// The bytes were read and can be checked.
    Present(EcLocalShardReadResponse),
    /// This node holds no such shard.
    Absent,
    /// The bytes are there and they are not the bytes this node recorded.
    Unreadable {
        /// What the storage layer said, kept for the quarantine record and the
        /// operator reading it afterwards.
        reason: String,
    },
    /// The medium did not answer, so nothing was learned about these bytes.
    ///
    /// A detached volume, an exhausted descriptor table, an index that is not
    /// responding. The shard stays the node's, uncondemned, and the next pass
    /// asks again.
    MediaUnavailable {
        /// What the storage layer said, kept for the operator reading the pass.
        reason: String,
    },
}

/// Port used by the EC scrubber to read node-local physical shard bytes.
#[async_trait]
pub trait EcLocalShardReader: Send + Sync {
    /// Read a local shard.
    async fn read_local_shard(
        &self,
        request: EcLocalShardReadRequest,
    ) -> Result<EcLocalShardRead, EcAntiEntropyError>;
}

/// Request for committed-object reconstruction sample verification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EcReconstructionSampleRequest {
    /// Manifest to verify through the normal EC read/decode path.
    pub manifest: EcObjectManifest,
    /// Chunk indexes selected for this probe.
    pub sample_chunks: Vec<u32>,
}

/// Reconstruction sample outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EcReconstructionSampleStatus {
    /// The verifier reconstructed and verified the selected samples.
    Verified,
    /// The verifier found a reconstruction or hash failure.
    Failed,
    /// Verification was intentionally skipped by a partial integration.
    Skipped,
}

/// Result of a committed-object reconstruction sample.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EcReconstructionSampleResult {
    /// Verification status.
    pub status: EcReconstructionSampleStatus,
    /// Chunks actually sampled by the verifier.
    pub sampled_chunks: Vec<u32>,
    /// Human-readable diagnostic reason for failed or skipped samples.
    pub reason: Option<String>,
}

/// Port used by scrubber integration to verify EC reconstruction samples.
#[async_trait]
pub trait EcReconstructionVerifier: Send + Sync {
    /// Verify selected chunks through the EC read/decode path.
    async fn verify_reconstruction_sample(
        &self,
        request: EcReconstructionSampleRequest,
    ) -> Result<EcReconstructionSampleResult, EcAntiEntropyError>;
}

/// No-op reconstruction verifier for partial integrations.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopEcReconstructionVerifier;

#[async_trait]
impl EcReconstructionVerifier for NoopEcReconstructionVerifier {
    async fn verify_reconstruction_sample(
        &self,
        request: EcReconstructionSampleRequest,
    ) -> Result<EcReconstructionSampleResult, EcAntiEntropyError> {
        Ok(EcReconstructionSampleResult {
            status: EcReconstructionSampleStatus::Skipped,
            sampled_chunks: request.sample_chunks,
            reason: Some("EC reconstruction verifier is not configured".to_string()),
        })
    }
}

/// Port used for quorum-aware manifest reads during divergence reconciliation.
#[async_trait]
pub trait EcAntiEntropyMetadataCoordinator: Send + Sync {
    /// Read an EC manifest through quorum metadata semantics.
    async fn quorum_read_manifest(
        &self,
        object: &EcObjectVersionKey,
    ) -> Result<EcManifestReadResult, EcAntiEntropyError>;
}

/// Port used by anti-entropy and scrubbing to queue shard repairs.
#[async_trait]
pub trait EcAntiEntropyRepairQueue: Send + Sync {
    /// Queue a repair task for an anti-entropy or scrubber finding.
    async fn enqueue_anti_entropy_repair(
        &self,
        request: EcRepairQueueRequest,
    ) -> Result<EcRepairTaskRecord, EcAntiEntropyError>;
}

#[async_trait]
impl EcAntiEntropyRepairQueue for EcRepairCoordinator {
    async fn enqueue_anti_entropy_repair(
        &self,
        request: EcRepairQueueRequest,
    ) -> Result<EcRepairTaskRecord, EcAntiEntropyError> {
        Ok(EcRepairCoordinator::enqueue_repair(self, request).await?)
    }
}

/// EC anti-entropy and scrub coordinator.
#[derive(Clone)]
pub struct EcAntiEntropyService {
    local_node_id: NodeId,
    metadata: Arc<EcMetadataStore>,
    config: EcAntiEntropyConfig,
    metrics: Arc<EcAntiEntropyMetrics>,
}

impl EcAntiEntropyService {
    /// Create an EC anti-entropy service.
    pub fn new(
        local_node_id: NodeId,
        metadata: Arc<EcMetadataStore>,
        config: EcAntiEntropyConfig,
    ) -> Result<Self, EcAntiEntropyError> {
        config.validate()?;
        Ok(Self {
            local_node_id,
            metadata,
            config,
            metrics: Arc::new(EcAntiEntropyMetrics::default()),
        })
    }

    /// Return the local node ID used for local shard ownership checks.
    pub fn local_node_id(&self) -> NodeId {
        self.local_node_id
    }

    /// Return the active configuration.
    pub fn config(&self) -> &EcAntiEntropyConfig {
        &self.config
    }

    /// Return shared anti-entropy metrics.
    pub fn metrics(&self) -> &Arc<EcAntiEntropyMetrics> {
        &self.metrics
    }

    /// Build a local manifest Merkle snapshot for a specific erasure set from the EC metadata store.
    pub async fn build_manifest_tree_snapshot(
        &self,
        erasure_set_id: u64,
    ) -> Result<EcManifestTreeSnapshot, EcAntiEntropyError> {
        let manifests = self.collect_all_manifests().await?;
        EcManifestTreeSnapshot::from_manifests(erasure_set_id, manifests, self.config.merkle_depth)
    }

    /// Build local manifest Merkle snapshots for several erasure sets at once.
    ///
    /// Scans the metadata store once and splits what it read by set, so a pool
    /// of several sets costs one pass rather than one pass per set.
    pub async fn build_manifest_tree_snapshots(
        &self,
        erasure_set_ids: &[u64],
    ) -> Result<Vec<EcManifestTreeSnapshot>, EcAntiEntropyError> {
        let mut by_set: HashMap<u64, Vec<EcObjectManifest>> =
            erasure_set_ids.iter().map(|set_id| (*set_id, Vec::new())).collect();
        for manifest in self.collect_all_manifests().await? {
            if let Some(bucket) = by_set.get_mut(&manifest.erasure_set_id) {
                bucket.push(manifest);
            }
        }

        let mut snapshots = Vec::with_capacity(erasure_set_ids.len());
        for erasure_set_id in erasure_set_ids {
            let manifests = by_set.remove(erasure_set_id).unwrap_or_default();
            snapshots.push(EcManifestTreeSnapshot::from_manifests(
                *erasure_set_id,
                manifests,
                self.config.merkle_depth,
            )?);
        }
        Ok(snapshots)
    }

    /// Reconcile manifest divergence against a remote manifest tree snapshot.
    pub async fn reconcile_manifest_divergence<M>(
        &self,
        remote: &EcManifestTreeSnapshot,
        metadata_coordinator: &M,
    ) -> Result<EcManifestReconcileReport, EcAntiEntropyError>
    where
        M: EcAntiEntropyMetadataCoordinator,
    {
        let local = self.build_manifest_tree_snapshot(remote.erasure_set_id).await?;
        let divergent_ranges = local.divergent_ranges_against(remote)?;
        let mut report = EcManifestReconcileReport {
            roots_matched: divergent_ranges.is_empty(),
            divergent_ranges: divergent_ranges.len(),
            ..EcManifestReconcileReport::default()
        };
        if divergent_ranges.is_empty() {
            return Ok(report);
        }

        let candidates = candidate_manifest_objects(&local, remote, &divergent_ranges);
        report.candidate_objects = candidates.len();
        increment(
            &self.metrics.manifest_mismatches_total,
            candidates.len() as u64,
        );

        for object in candidates {
            let before = self.metadata.get_manifest(&object).await?;
            let before_hash = optional_manifest_hash(before.as_ref())?;
            let quorum_result = metadata_coordinator.quorum_read_manifest(&object).await?;
            match quorum_result {
                EcManifestReadResult::Present(manifest) => {
                    let after_hash = manifest.serialized_sha256()?;
                    self.metadata.put_manifest(&manifest).await?;
                    report.present_manifests_cached =
                        report.present_manifests_cached.saturating_add(1);
                    if before_hash != Some(after_hash) {
                        report.manifests_updated = report.manifests_updated.saturating_add(1);
                        increment(&self.metrics.manifest_repairs_total, 1);
                    }
                }
                EcManifestReadResult::Tombstone(manifest) => {
                    let after_hash = manifest.serialized_sha256()?;
                    self.metadata.put_manifest(&manifest).await?;
                    report.tombstones_cached = report.tombstones_cached.saturating_add(1);
                    if before_hash != Some(after_hash) {
                        report.manifests_updated = report.manifests_updated.saturating_add(1);
                        increment(&self.metrics.manifest_repairs_total, 1);
                    }
                }
                EcManifestReadResult::Absent => {
                    self.metadata.delete_manifest(&object).await?;
                    report.absent_confirmed = report.absent_confirmed.saturating_add(1);
                    if before_hash.is_some() {
                        report.manifests_updated = report.manifests_updated.saturating_add(1);
                        increment(&self.metrics.manifest_repairs_total, 1);
                    }
                }
            }
        }

        Ok(report)
    }

    /// Audit local shard inventory for one manifest and queue needed repairs.
    pub async fn audit_manifest_shard_inventory<Q, Z>(
        &self,
        manifest: &EcObjectManifest,
        repair_queue: &Q,
        quarantine: &Z,
    ) -> Result<EcShardInventoryAuditReport, EcAntiEntropyError>
    where
        Q: EcAntiEntropyRepairQueue,
        Z: EcShardQuarantine,
    {
        manifest.validate()?;
        let mut report = EcShardInventoryAuditReport::default();
        if !manifest_is_committed_live(manifest) {
            return Ok(report);
        }

        for (chunk_index, expected) in local_shards_in_manifest(manifest, self.local_node_id) {
            report.expected_local_shards = report.expected_local_shards.saturating_add(1);
            let key = LocalShardKey {
                object: manifest.object_key(),
                chunk_index,
                slot_index: expected.slot_index,
            };

            let Some(record) = self.metadata.get_local_shard(&key).await? else {
                let repair_queued = self
                    .note_missing_shard(
                        MissingShardFinding {
                            manifest,
                            chunk_index,
                            expected,
                            reason: "anti-entropy found no local shard record".to_string(),
                        },
                        repair_queue,
                    )
                    .await;
                record_repair_enqueue(
                    repair_queued,
                    &mut report.repairs_queued,
                    &mut report.repair_enqueue_failures,
                );
                report.missing_local_shards = report.missing_local_shards.saturating_add(1);
                continue;
            };

            if record.state != LocalShardState::Durable {
                let repair_queued = self
                    .note_missing_shard(
                        MissingShardFinding {
                            manifest,
                            chunk_index,
                            expected,
                            reason: "anti-entropy found a local shard record that is not durable"
                                .to_string(),
                        },
                        repair_queue,
                    )
                    .await;
                record_repair_enqueue(
                    repair_queued,
                    &mut report.repairs_queued,
                    &mut report.repair_enqueue_failures,
                );
                report.missing_local_shards = report.missing_local_shards.saturating_add(1);
                continue;
            }

            let record_error = if record.operation_id != manifest.operation_id {
                Some("local shard operation_id does not match manifest".to_string())
            } else {
                record.verify_against_manifest(expected).err().map(|error| error.to_string())
            };

            if let Some(reason) = record_error {
                let action = self
                    .note_corrupt_shard(
                        CorruptShardFinding {
                            manifest,
                            chunk_index,
                            expected,
                            observed: EcObservedShardIntegrity::from_record(&record),
                            reason,
                        },
                        repair_queue,
                        quarantine,
                    )
                    .await;
                record_corrupt_action(
                    action,
                    &mut report.repairs_queued,
                    &mut report.repair_enqueue_failures,
                    &mut report.quarantined_shards,
                    &mut report.quarantine_failures,
                );
                report.corrupt_local_shards = report.corrupt_local_shards.saturating_add(1);
                continue;
            }

            report.healthy_local_shards = report.healthy_local_shards.saturating_add(1);
        }

        Ok(report)
    }

    /// Detect local orphan shard candidates without deleting them.
    pub async fn detect_orphan_shards(
        &self,
        max_entries: usize,
    ) -> Result<Vec<EcOrphanShardCandidate>, EcAntiEntropyError> {
        let entries = self.metadata.list_local_shards(max_entries).await?;
        let mut candidates = Vec::new();
        for (key, record) in entries {
            if let Some((reason, erasure_set_id)) = self.orphan_reason(&key, &record).await? {
                candidates.push(EcOrphanShardCandidate {
                    key,
                    record,
                    erasure_set_id,
                    reason,
                });
            }
        }
        increment(&self.metrics.orphan_shards_total, candidates.len() as u64);
        Ok(candidates)
    }

    /// Run one local EC shard scrub pass.
    pub async fn scrub_local_shards<R, Q, Z, V>(
        &self,
        shard_reader: &R,
        repair_queue: &Q,
        quarantine: &Z,
        reconstruction_verifier: &V,
    ) -> Result<EcShardScrubReport, EcAntiEntropyError>
    where
        R: EcLocalShardReader,
        Q: EcAntiEntropyRepairQueue,
        Z: EcShardQuarantine,
        V: EcReconstructionVerifier,
    {
        let mut report = EcShardScrubReport::default();
        let mut cursor = None;

        loop {
            let page = self
                .metadata
                .list_local_shards_page(cursor, self.config.max_shard_scan_entries)
                .await?;
            let has_next = page.next_cursor.is_some();
            cursor = page.next_cursor;

            for (key, record) in page.entries {
                report.local_shards_scanned = report.local_shards_scanned.saturating_add(1);
                let Some(manifest) = self.metadata.get_manifest(&key.object).await? else {
                    self.push_orphan(
                        &mut report,
                        key,
                        record,
                        None,
                        "no local manifest references shard",
                    );
                    continue;
                };
                if !manifest_is_committed_live(&manifest) {
                    self.push_orphan(
                        &mut report,
                        key,
                        record,
                        Some(manifest.erasure_set_id),
                        "local manifest is not a committed live EC layout",
                    );
                    continue;
                }
                if record.operation_id != manifest.operation_id {
                    self.push_orphan(
                        &mut report,
                        key,
                        record,
                        Some(manifest.erasure_set_id),
                        "local shard operation_id does not match live manifest",
                    );
                    continue;
                }

                let Some(expected) = expected_manifest_shard(
                    &manifest,
                    key.chunk_index,
                    key.slot_index,
                    self.local_node_id,
                ) else {
                    self.push_orphan(
                        &mut report,
                        key,
                        record,
                        Some(manifest.erasure_set_id),
                        "live manifest does not assign this shard slot to the local node",
                    );
                    continue;
                };
                let expected = expected.clone();

                if record.state != LocalShardState::Durable {
                    let repair_queued = self
                        .note_missing_shard(
                            MissingShardFinding {
                                manifest: &manifest,
                                chunk_index: key.chunk_index,
                                expected: &expected,
                                reason: "scrubber found a local shard record that is not durable"
                                    .to_string(),
                            },
                            repair_queue,
                        )
                        .await;
                    record_repair_enqueue(
                        repair_queued,
                        &mut report.repairs_queued,
                        &mut report.repair_enqueue_failures,
                    );
                    if repair_queued {
                        report.missing_shard_repairs_queued =
                            report.missing_shard_repairs_queued.saturating_add(1);
                    }
                    report.missing_shards = report.missing_shards.saturating_add(1);
                    continue;
                }

                if let Err(error) = record.verify_against_manifest(&expected) {
                    let action = self
                        .note_corrupt_shard(
                            CorruptShardFinding {
                                manifest: &manifest,
                                chunk_index: key.chunk_index,
                                expected: &expected,
                                observed: EcObservedShardIntegrity::from_record(&record),
                                reason: format!(
                                    "local shard record differs from manifest: {error}"
                                ),
                            },
                            repair_queue,
                            quarantine,
                        )
                        .await;
                    record_corrupt_action(
                        action,
                        &mut report.repairs_queued,
                        &mut report.repair_enqueue_failures,
                        &mut report.quarantined_shards,
                        &mut report.quarantine_failures,
                    );
                    report.corrupt_shards = report.corrupt_shards.saturating_add(1);
                    report.manifest_hash_mismatches =
                        report.manifest_hash_mismatches.saturating_add(1);
                    increment(&self.metrics.manifest_hash_mismatches_total, 1);
                    continue;
                }

                let read = shard_reader
                    .read_local_shard(EcLocalShardReadRequest {
                        key: key.clone(),
                        record: record.clone(),
                    })
                    .await?;
                let read = match read {
                    EcLocalShardRead::Present(read) => read,
                    EcLocalShardRead::Absent => {
                        let repair_queued = self
                            .note_missing_shard(
                                MissingShardFinding {
                                    manifest: &manifest,
                                    chunk_index: key.chunk_index,
                                    expected: &expected,
                                    reason: "scrubber found no local physical shard bytes"
                                        .to_string(),
                                },
                                repair_queue,
                            )
                            .await;
                        record_repair_enqueue(
                            repair_queued,
                            &mut report.repairs_queued,
                            &mut report.repair_enqueue_failures,
                        );
                        if repair_queued {
                            report.missing_shard_repairs_queued =
                                report.missing_shard_repairs_queued.saturating_add(1);
                        }
                        report.missing_shards = report.missing_shards.saturating_add(1);
                        continue;
                    }
                    // The record says this node holds the shard, durably, at a
                    // known offset — and the node cannot produce it. That is
                    // rot, and it is treated as rot: quarantined so the node
                    // stops claiming the shard, counted as corruption, and
                    // repaired as a corrupt shard rather than a missing one
                    // (decision EC2-185). `observed` is what the node last
                    // recorded, because there are no bytes left to observe.
                    EcLocalShardRead::Unreadable { reason } => {
                        let action = self
                            .note_corrupt_shard(
                                CorruptShardFinding {
                                    manifest: &manifest,
                                    chunk_index: key.chunk_index,
                                    expected: &expected,
                                    observed: EcObservedShardIntegrity::from_record(&record),
                                    reason: format!(
                                        "scrubber could not read local physical shard bytes: \
                                         {reason}"
                                    ),
                                },
                                repair_queue,
                                quarantine,
                            )
                            .await;
                        record_corrupt_action(
                            action,
                            &mut report.repairs_queued,
                            &mut report.repair_enqueue_failures,
                            &mut report.quarantined_shards,
                            &mut report.quarantine_failures,
                        );
                        report.corrupt_shards = report.corrupt_shards.saturating_add(1);
                        report.physical_read_failures =
                            report.physical_read_failures.saturating_add(1);
                        increment(&self.metrics.physical_read_failures_total, 1);
                        continue;
                    }
                    // The medium did not answer. That is a statement about the
                    // disk and none at all about this shard, so the shard is
                    // left where it is: not quarantined, not counted as
                    // corruption, not repaired. The next pass asks again
                    // (decision EC2-200).
                    EcLocalShardRead::MediaUnavailable { reason } => {
                        report.media_unavailable = report.media_unavailable.saturating_add(1);
                        increment(&self.metrics.media_unavailable_total, 1);
                        tracing::debug!(
                            chunk_index = key.chunk_index,
                            slot_index = key.slot_index,
                            reason,
                            "EC scrub could not reach the medium holding a local shard"
                        );
                        if self.media_failure_stops_the_pass(&report) {
                            report.media_suspect = true;
                            tracing::warn!(
                                media_unavailable = report.media_unavailable,
                                scanned = report.local_shards_scanned,
                                reason,
                                "EC scrub stopped: the medium, not the shards, is what is failing"
                            );
                            // Returned before the reconstruction sample, which
                            // reads shards of its own: on a medium this far
                            // gone its failure would say nothing either.
                            return Ok(report);
                        }
                        continue;
                    }
                };

                let observed = EcObservedShardIntegrity::from_bytes(&read.data)?;
                if !read.blob_header_crc_valid {
                    let action = self
                        .note_corrupt_shard(
                            CorruptShardFinding {
                                manifest: &manifest,
                                chunk_index: key.chunk_index,
                                expected: &expected,
                                observed,
                                reason: "scrubber found invalid BlobHeader CRC".to_string(),
                            },
                            repair_queue,
                            quarantine,
                        )
                        .await;
                    record_corrupt_action(
                        action,
                        &mut report.repairs_queued,
                        &mut report.repair_enqueue_failures,
                        &mut report.quarantined_shards,
                        &mut report.quarantine_failures,
                    );
                    report.corrupt_shards = report.corrupt_shards.saturating_add(1);
                    report.blob_header_crc_failures =
                        report.blob_header_crc_failures.saturating_add(1);
                    increment(&self.metrics.blob_header_crc_failures_total, 1);
                    continue;
                }

                if observed.size != record.size
                    || observed.crc32 != record.crc32
                    || observed.sha256 != record.sha256
                {
                    let action = self
                        .note_corrupt_shard(
                            CorruptShardFinding {
                                manifest: &manifest,
                                chunk_index: key.chunk_index,
                                expected: &expected,
                                observed,
                                reason: "scrubber physical bytes differ from local shard record"
                                    .to_string(),
                            },
                            repair_queue,
                            quarantine,
                        )
                        .await;
                    record_corrupt_action(
                        action,
                        &mut report.repairs_queued,
                        &mut report.repair_enqueue_failures,
                        &mut report.quarantined_shards,
                        &mut report.quarantine_failures,
                    );
                    report.corrupt_shards = report.corrupt_shards.saturating_add(1);
                    report.physical_hash_mismatches =
                        report.physical_hash_mismatches.saturating_add(1);
                    increment(&self.metrics.physical_hash_mismatches_total, 1);
                    continue;
                }

                if let Err(error) = expected.verify_bytes(&read.data) {
                    let observed = EcObservedShardIntegrity::from_bytes(&read.data)?;
                    let action = self
                        .note_corrupt_shard(
                            CorruptShardFinding {
                                manifest: &manifest,
                                chunk_index: key.chunk_index,
                                expected: &expected,
                                observed,
                                reason: format!(
                                    "scrubber physical bytes differ from manifest: {error}"
                                ),
                            },
                            repair_queue,
                            quarantine,
                        )
                        .await;
                    record_corrupt_action(
                        action,
                        &mut report.repairs_queued,
                        &mut report.repair_enqueue_failures,
                        &mut report.quarantined_shards,
                        &mut report.quarantine_failures,
                    );
                    report.corrupt_shards = report.corrupt_shards.saturating_add(1);
                    report.manifest_hash_mismatches =
                        report.manifest_hash_mismatches.saturating_add(1);
                    increment(&self.metrics.manifest_hash_mismatches_total, 1);
                    continue;
                }

                report.healthy_shards = report.healthy_shards.saturating_add(1);
            }

            if !has_next {
                break;
            }
        }

        self.verify_committed_manifest_samples(reconstruction_verifier, &mut report)
            .await?;
        Ok(report)
    }

    /// Whether this pass has seen enough media failures to stop judging shards.
    ///
    /// Both conditions are needed. The floor keeps one failure among two shards
    /// from condemning a disk; the share keeps a pass over a large healthy
    /// inventory from stopping on a handful of unlucky reads.
    fn media_failure_stops_the_pass(&self, report: &EcShardScrubReport) -> bool {
        if report.media_unavailable < EC_SCRUB_MEDIA_FAILURE_MIN_SAMPLES {
            return false;
        }
        let scanned = report.local_shards_scanned as f64;
        report.media_unavailable as f64 >= scanned * self.config.media_failure_ratio
    }

    async fn collect_all_manifests(&self) -> Result<Vec<EcObjectManifest>, EcAntiEntropyError> {
        let mut manifests = Vec::new();
        let mut cursor = None;

        loop {
            let page = self
                .metadata
                .list_manifests_page(cursor, self.config.max_manifest_scan_entries)
                .await?;
            manifests.extend(page.entries);
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }

        Ok(manifests)
    }

    async fn verify_committed_manifest_samples<V>(
        &self,
        verifier: &V,
        report: &mut EcShardScrubReport,
    ) -> Result<(), EcAntiEntropyError>
    where
        V: EcReconstructionVerifier,
    {
        if self.config.reconstruction_sample_chunks == 0 {
            return Ok(());
        }

        let manifests = self.collect_all_manifests().await?;
        for manifest in manifests {
            if !manifest_is_committed_live(&manifest) {
                continue;
            }
            let sample_chunks =
                reconstruction_sample_chunks(&manifest, self.config.reconstruction_sample_chunks);
            if sample_chunks.is_empty() {
                continue;
            }
            let result = verifier
                .verify_reconstruction_sample(EcReconstructionSampleRequest {
                    manifest,
                    sample_chunks,
                })
                .await?;
            match result.status {
                EcReconstructionSampleStatus::Verified => {
                    report.reconstruction_samples_verified =
                        report.reconstruction_samples_verified.saturating_add(1);
                }
                EcReconstructionSampleStatus::Failed => {
                    report.reconstruction_samples_failed =
                        report.reconstruction_samples_failed.saturating_add(1);
                    increment(&self.metrics.reconstruction_sample_failures_total, 1);
                }
                EcReconstructionSampleStatus::Skipped => {
                    report.reconstruction_samples_skipped =
                        report.reconstruction_samples_skipped.saturating_add(1);
                }
            }
        }
        Ok(())
    }

    async fn orphan_reason(
        &self,
        key: &LocalShardKey,
        record: &LocalShardRecord,
    ) -> Result<Option<(String, Option<u64>)>, EcAntiEntropyError> {
        let Some(manifest) = self.metadata.get_manifest(&key.object).await? else {
            return Ok(Some((
                "no local live manifest references shard".to_string(),
                None,
            )));
        };
        let set_id = Some(manifest.erasure_set_id);
        if !manifest_is_committed_live(&manifest) {
            return Ok(Some((
                "local manifest is not a committed live EC layout".to_string(),
                set_id,
            )));
        }
        if record.operation_id != manifest.operation_id {
            return Ok(Some((
                "local shard operation_id does not match live manifest".to_string(),
                set_id,
            )));
        }
        let Some(expected) = expected_manifest_shard(
            &manifest,
            key.chunk_index,
            key.slot_index,
            self.local_node_id,
        ) else {
            return Ok(Some((
                "live manifest does not assign this shard slot to the local node".to_string(),
                set_id,
            )));
        };
        match record.verify_against_manifest(expected) {
            Ok(()) => Ok(None),
            Err(_) => Ok(None),
        }
    }

    fn push_orphan(
        &self,
        report: &mut EcShardScrubReport,
        key: LocalShardKey,
        record: LocalShardRecord,
        erasure_set_id: Option<u64>,
        reason: impl Into<String>,
    ) {
        report.orphan_candidates.push(EcOrphanShardCandidate {
            key,
            record,
            erasure_set_id,
            reason: reason.into(),
        });
        increment(&self.metrics.orphan_shards_total, 1);
    }

    async fn note_missing_shard<Q>(
        &self,
        finding: MissingShardFinding<'_>,
        repair_queue: &Q,
    ) -> bool
    where
        Q: EcAntiEntropyRepairQueue,
    {
        increment(&self.metrics.shard_missing_total, 1);
        self.enqueue_repair(
            repair_queue,
            finding.manifest,
            finding.chunk_index,
            finding.expected,
            EcRepairKind::MissingShard,
            finding.reason,
        )
        .await
    }

    async fn note_corrupt_shard<Q, Z>(
        &self,
        finding: CorruptShardFinding<'_>,
        repair_queue: &Q,
        quarantine: &Z,
    ) -> CorruptShardAction
    where
        Q: EcAntiEntropyRepairQueue,
        Z: EcShardQuarantine,
    {
        increment(&self.metrics.shard_corrupt_total, 1);
        let quarantined = match quarantine
            .quarantine_corrupt_shard(EcShardQuarantineRequest {
                object: finding.manifest.object_key(),
                operation_id: finding.manifest.operation_id,
                erasure_set_id: finding.manifest.erasure_set_id,
                chunk_index: finding.chunk_index,
                slot_index: finding.expected.slot_index,
                node_id: finding.expected.node_id,
                expected: finding.expected.clone(),
                observed: finding.observed,
                reason: finding.reason.clone(),
            })
            .await
        {
            Ok(()) => true,
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    chunk_index = finding.chunk_index,
                    slot_index = finding.expected.slot_index,
                    "failed to quarantine corrupt EC shard"
                );
                false
            }
        };

        let repair_queued = self
            .enqueue_repair(
                repair_queue,
                finding.manifest,
                finding.chunk_index,
                finding.expected,
                EcRepairKind::CorruptShard,
                finding.reason,
            )
            .await;
        CorruptShardAction {
            repair_queued,
            quarantined,
        }
    }

    async fn enqueue_repair<Q>(
        &self,
        repair_queue: &Q,
        manifest: &EcObjectManifest,
        chunk_index: u32,
        expected: &ShardManifest,
        kind: EcRepairKind,
        reason: String,
    ) -> bool
    where
        Q: EcAntiEntropyRepairQueue,
    {
        let request = EcRepairQueueRequest {
            object: manifest.object_key(),
            operation_id: manifest.operation_id,
            erasure_set_id: manifest.erasure_set_id,
            chunk_index,
            slot_index: expected.slot_index,
            node_id: expected.node_id,
            kind,
            reason,
            failure_domain: None,
        };
        match repair_queue.enqueue_anti_entropy_repair(request).await {
            Ok(_) => {
                increment(&self.metrics.repair_enqueues_total, 1);
                true
            }
            Err(error) => {
                increment(&self.metrics.repair_enqueue_failures_total, 1);
                tracing::warn!(error = %error, "failed to enqueue EC repair from anti-entropy");
                false
            }
        }
    }
}

fn candidate_manifest_objects(
    local: &EcManifestTreeSnapshot,
    remote: &EcManifestTreeSnapshot,
    ranges: &[KeyRange],
) -> Vec<EcObjectVersionKey> {
    let mut seen = HashSet::new();
    let mut candidates = Vec::new();
    for entry in local.entries.iter().chain(remote.entries.iter()) {
        if !ranges.iter().any(|range| range.contains(entry.ring_position)) {
            continue;
        }
        if seen.insert(entry.object.clone()) {
            candidates.push(entry.object.clone());
        }
    }
    candidates.sort_unstable_by(manifest_object_cmp);
    candidates
}

fn record_repair_enqueue(
    repair_queued: bool,
    repairs_queued: &mut usize,
    repair_enqueue_failures: &mut usize,
) {
    if repair_queued {
        *repairs_queued = repairs_queued.saturating_add(1);
    } else {
        *repair_enqueue_failures = repair_enqueue_failures.saturating_add(1);
    }
}

fn record_corrupt_action(
    action: CorruptShardAction,
    repairs_queued: &mut usize,
    repair_enqueue_failures: &mut usize,
    quarantined_shards: &mut usize,
    quarantine_failures: &mut usize,
) {
    record_repair_enqueue(
        action.repair_queued,
        repairs_queued,
        repair_enqueue_failures,
    );
    if action.quarantined {
        *quarantined_shards = quarantined_shards.saturating_add(1);
    } else {
        *quarantine_failures = quarantine_failures.saturating_add(1);
    }
}

fn manifest_object_cmp(
    left: &EcObjectVersionKey,
    right: &EcObjectVersionKey,
) -> std::cmp::Ordering {
    left.bucket
        .cmp(&right.bucket)
        .then(left.key.cmp(&right.key))
        .then(left.version_id.cmp(&right.version_id))
}

fn all_node_hashes(tree: &MerkleTree) -> HashMap<usize, [u8; 32]> {
    let mut hashes = HashMap::new();
    for level in 0..tree.depth() {
        for (index, hash) in tree.hashes_at_level(level) {
            hashes.insert(index, hash);
        }
    }
    hashes
}

fn optional_manifest_hash(
    manifest: Option<&EcObjectManifest>,
) -> Result<Option<[u8; 32]>, EcAntiEntropyError> {
    manifest
        .map(EcObjectManifest::serialized_sha256)
        .transpose()
        .map_err(Into::into)
}

fn local_shards_in_manifest(
    manifest: &EcObjectManifest,
    local_node_id: NodeId,
) -> Vec<(u32, &ShardManifest)> {
    manifest
        .chunks
        .iter()
        .flat_map(|chunk| {
            chunk
                .shards
                .iter()
                .filter(move |shard| shard.node_id == local_node_id)
                .map(move |shard| (chunk.chunk_index, shard))
        })
        .collect()
}

fn expected_manifest_shard(
    manifest: &EcObjectManifest,
    chunk_index: u32,
    slot_index: u8,
    local_node_id: NodeId,
) -> Option<&ShardManifest> {
    manifest
        .chunks
        .iter()
        .find(|chunk| chunk.chunk_index == chunk_index)?
        .shards
        .iter()
        .find(|shard| shard.slot_index == slot_index && shard.node_id == local_node_id)
}

pub(crate) fn manifest_is_committed_live(manifest: &EcObjectManifest) -> bool {
    !manifest.is_deleted && manifest.state.is_ec_authoritative()
}

fn manifest_tree_key(object: &EcObjectVersionKey) -> String {
    match &object.version_id {
        Some(version_id) => format!("{}/{}?versionId={version_id}", object.bucket, object.key),
        None => format!("{}/{}", object.bucket, object.key),
    }
}

fn manifest_ring_position(object: &EcObjectVersionKey) -> u64 {
    let versioned_key = match &object.version_id {
        Some(version_id) => format!("{}?versionId={version_id}", object.key),
        None => object.key.clone(),
    };
    merkle_ring_position(&object.bucket, &versioned_key)
}

fn reconstruction_sample_chunks(manifest: &EcObjectManifest, limit: usize) -> Vec<u32> {
    if limit == 0 || manifest.chunks.is_empty() {
        return Vec::new();
    }

    let mut samples = Vec::new();
    push_unique(&mut samples, 0, limit);
    if let Some(last) = manifest.chunks.last() {
        push_unique(&mut samples, last.chunk_index, limit);
    }

    let chunk_count = manifest.chunks.len();
    let mut seed = u64::from_be_bytes([
        manifest.content_hash_sha256[0],
        manifest.content_hash_sha256[1],
        manifest.content_hash_sha256[2],
        manifest.content_hash_sha256[3],
        manifest.content_hash_sha256[4],
        manifest.content_hash_sha256[5],
        manifest.content_hash_sha256[6],
        manifest.content_hash_sha256[7],
    ]);
    while samples.len() < limit && samples.len() < chunk_count {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let index = (seed % chunk_count as u64) as usize;
        if let Some(chunk) = manifest.chunks.get(index) {
            push_unique(&mut samples, chunk.chunk_index, limit);
        } else {
            break;
        }
    }
    samples
}

fn push_unique(samples: &mut Vec<u32>, chunk_index: u32, limit: usize) {
    if samples.len() < limit && !samples.contains(&chunk_index) {
        samples.push(chunk_index);
    }
}

fn sha256(bytes: &[u8]) -> [u8; 32] {
    use sha2::{Digest, Sha256};

    Sha256::new().chain_update(bytes).finalize().into()
}

fn parse_optional_usize(name: &'static str) -> Result<Option<usize>, EcAntiEntropyError> {
    match std::env::var(name) {
        Ok(raw) => {
            raw.parse::<usize>()
                .map(Some)
                .map_err(|error| EcAntiEntropyError::InvalidConfig {
                    name,
                    reason: error.to_string(),
                })
        }
        Err(_) => Ok(None),
    }
}

fn parse_optional_f64(name: &'static str) -> Result<Option<f64>, EcAntiEntropyError> {
    match std::env::var(name) {
        Ok(raw) => {
            raw.parse::<f64>().map(Some).map_err(|error| EcAntiEntropyError::InvalidConfig {
                name,
                reason: error.to_string(),
            })
        }
        Err(_) => Ok(None),
    }
}

fn parse_optional_u8(name: &'static str) -> Result<Option<u8>, EcAntiEntropyError> {
    match std::env::var(name) {
        Ok(raw) => raw.parse::<u8>().map(Some).map_err(|error| EcAntiEntropyError::InvalidConfig {
            name,
            reason: error.to_string(),
        }),
        Err(_) => Ok(None),
    }
}

fn load(counter: &AtomicU64) -> u64 {
    counter.load(Ordering::Relaxed)
}

fn increment(counter: &AtomicU64, value: u64) {
    counter.fetch_add(value, Ordering::Relaxed);
}

/// Errors returned by EC anti-entropy and scrubber code.
#[derive(Debug, Error)]
pub enum EcAntiEntropyError {
    /// Invalid environment configuration.
    #[error("invalid {name}: {reason}")]
    InvalidConfig {
        /// Environment variable name.
        name: &'static str,
        /// Human-readable validation failure.
        reason: String,
    },

    /// Local physical shard read failed.
    #[error("EC shard read failed: {reason}")]
    ShardRead {
        /// Human-readable reason.
        reason: String,
    },

    /// Quarantine operation failed.
    #[error("EC shard quarantine failed: {reason}")]
    Quarantine {
        /// Human-readable reason.
        reason: String,
    },

    /// Reconstruction sample verification failed to run.
    #[error("EC reconstruction sample failed: {reason}")]
    Reconstruction {
        /// Human-readable reason.
        reason: String,
    },

    /// Manifest Merkle trees belong to different erasure sets and cannot be compared.
    #[error(
        "manifest tree for erasure set {local} cannot be compared against a tree for erasure set {remote}"
    )]
    ErasureSetMismatch {
        /// Local erasure set identifier.
        local: u64,
        /// Remote erasure set identifier.
        remote: u64,
    },

    /// EC metadata operation failed.
    #[error(transparent)]
    Metadata(#[from] EcMetadataError),

    /// EC repair queue operation failed.
    #[error(transparent)]
    Repair(#[from] EcRepairError),
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Mutex;

    use crate::codec_runtime::{EcCodecCpuConfig, EcCodecCpuLimiter};
    use crate::ec_metadata::{ChunkManifest, EcMetadataStore, EC_MANIFEST_FORMAT_VERSION};
    use crate::ec_repair::{EcRepairConfig, EcRepairCoordinator};
    use crate::erasure_coding::{PoolTopologyView, TopologyLabels};
    use s4_cluster::HlcTimestamp;
    use s4_codec::EcProfileSnapshot;

    fn node(index: u8) -> NodeId {
        NodeId(u128::from(index))
    }

    fn hlc(wall_time: u64) -> HlcTimestamp {
        HlcTimestamp {
            wall_time,
            logical: 1,
            node_id: 1,
        }
    }

    fn operation_id() -> uuid::Uuid {
        uuid::Uuid::from_u128(0xabc)
    }

    fn encoded_manifest(data: &[u8]) -> (EcObjectManifest, HashMap<(u32, u8), Bytes>) {
        let mut profile = EcProfileSnapshot::rs_small();
        profile.chunk_size = data.len() as u64;
        let codec = crate::ec_codec::build_codec(profile.clone()).unwrap();
        let encoded = codec.encode_chunk(data, 0).unwrap();
        let mut shard_bytes = HashMap::new();
        let shards = encoded
            .shards
            .iter()
            .map(|shard| {
                shard_bytes.insert((0, shard.slot_index), shard.data.clone());
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
        let manifest = EcObjectManifest {
            format_version: EC_MANIFEST_FORMAT_VERSION,
            bucket: "photos".to_string(),
            key: "raw.bin".to_string(),
            version_id: Some("v1".to_string()),
            original_size: data.len() as u64,
            content_hash_sha256: sha256(data),
            etag: "\"etag\"".to_string(),
            content_type: "application/octet-stream".to_string(),
            user_metadata: HashMap::new(),
            profile,
            erasure_set_id: 9,
            topology_epoch: 3,
            chunks: vec![ChunkManifest {
                chunk_index: 0,
                data_size: encoded.original_size,
                encoded_size: encoded.shard_size,
                chunk_sha256: sha256(data),
                shards,
            }],
            state: EcPlacementState::EcCommitted,
            hlc: hlc(10),
            origin_node_id: node(1),
            operation_id: operation_id(),
            created_at: 42,
            is_deleted: false,
            packed: None,
        };
        manifest.validate().unwrap();
        (manifest, shard_bytes)
    }

    fn service(local_node_id: NodeId, store: Arc<EcMetadataStore>) -> EcAntiEntropyService {
        service_with_page_size(local_node_id, store, 100)
    }

    fn service_with_page_size(
        local_node_id: NodeId,
        store: Arc<EcMetadataStore>,
        page_size: usize,
    ) -> EcAntiEntropyService {
        EcAntiEntropyService::new(
            local_node_id,
            store,
            EcAntiEntropyConfig {
                merkle_depth: 4,
                max_manifest_scan_entries: page_size,
                max_shard_scan_entries: page_size,
                reconstruction_sample_chunks: 2,
                media_failure_ratio: DEFAULT_EC_SCRUB_MEDIA_FAILURE_RATIO,
            },
        )
        .unwrap()
    }

    /// A repair queue on node 1, in a pool whose nodes 1-3 are in zone "a"
    /// and 4-6 in zone "b".
    fn repair_queue(store: Arc<EcMetadataStore>) -> EcRepairCoordinator {
        let cpu = EcCodecCpuLimiter::new(EcCodecCpuConfig {
            workers: std::num::NonZeroUsize::new(1).unwrap(),
            cpu_budget_percent: 60,
            max_inflight_encode_bytes: 1024 * 1024,
        });
        let nodes = (1..=6).map(node).collect::<Vec<_>>();
        let labels = nodes
            .iter()
            .map(|node_id| {
                let zone = if node_id.0 <= 3 { "a" } else { "b" };
                (
                    *node_id,
                    TopologyLabels {
                        zone: Some(zone.to_string()),
                        rack: Some(format!("r{}", node_id.0)),
                        host: Some(format!("h{}", node_id.0)),
                        disk_group: Some("d0".to_string()),
                    },
                )
            })
            .collect::<HashMap<_, _>>();
        EcRepairCoordinator::new(
            store,
            cpu,
            EcRepairConfig::default(),
            PoolTopologyView::build(node(1), &nodes, &labels),
        )
    }

    fn local_record(
        manifest: &EcObjectManifest,
        slot_index: u8,
        bytes: &[u8],
    ) -> (LocalShardKey, LocalShardRecord) {
        let key = LocalShardKey {
            object: manifest.object_key(),
            chunk_index: 0,
            slot_index,
        };
        let record = LocalShardRecord {
            volume_id: 1,
            offset: u64::from(slot_index) * 1024,
            size: bytes.len() as u64,
            crc32: crc32fast::hash(bytes),
            sha256: sha256(bytes),
            state: LocalShardState::Durable,
            operation_id: manifest.operation_id,
        };
        (key, record)
    }

    fn retarget_manifest(
        manifest: &mut EcObjectManifest,
        key: &str,
        version_id: Option<&str>,
        wall_time: u64,
    ) {
        manifest.key = key.to_string();
        manifest.version_id = version_id.map(str::to_string);
        manifest.hlc = hlc(wall_time);
        manifest.validate().unwrap();
    }

    #[derive(Default)]
    struct MemoryMetadata {
        result: Mutex<Option<EcManifestReadResult>>,
    }

    #[async_trait]
    impl EcAntiEntropyMetadataCoordinator for MemoryMetadata {
        async fn quorum_read_manifest(
            &self,
            _object: &EcObjectVersionKey,
        ) -> Result<EcManifestReadResult, EcAntiEntropyError> {
            Ok(self.result.lock().unwrap().clone().unwrap_or(EcManifestReadResult::Absent))
        }
    }

    #[derive(Default)]
    struct RecordingQuarantine {
        requests: Mutex<Vec<EcShardQuarantineRequest>>,
    }

    #[async_trait]
    impl EcShardQuarantine for RecordingQuarantine {
        async fn quarantine_corrupt_shard(
            &self,
            request: EcShardQuarantineRequest,
        ) -> Result<(), EcAntiEntropyError> {
            self.requests.lock().unwrap().push(request);
            Ok(())
        }
    }

    #[derive(Default)]
    struct MemoryLocalShardReader {
        responses: Mutex<HashMap<LocalShardKey, EcLocalShardRead>>,
    }

    impl MemoryLocalShardReader {
        fn holds(&self, key: &LocalShardKey, response: EcLocalShardReadResponse) {
            self.responses
                .lock()
                .unwrap()
                .insert(key.clone(), EcLocalShardRead::Present(response));
        }

        fn refuses(&self, key: &LocalShardKey, reason: &str) {
            self.responses.lock().unwrap().insert(
                key.clone(),
                EcLocalShardRead::Unreadable {
                    reason: reason.to_string(),
                },
            );
        }

        /// The medium did not answer for this shard at all.
        fn unreachable(&self, key: &LocalShardKey, reason: &str) {
            self.responses.lock().unwrap().insert(
                key.clone(),
                EcLocalShardRead::MediaUnavailable {
                    reason: reason.to_string(),
                },
            );
        }
    }

    #[async_trait]
    impl EcLocalShardReader for MemoryLocalShardReader {
        async fn read_local_shard(
            &self,
            request: EcLocalShardReadRequest,
        ) -> Result<EcLocalShardRead, EcAntiEntropyError> {
            Ok(self
                .responses
                .lock()
                .unwrap()
                .get(&request.key)
                .cloned()
                .unwrap_or(EcLocalShardRead::Absent))
        }
    }

    #[tokio::test]
    async fn manifest_merkle_divergence_updates_local_manifest_from_quorum() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let service = service(node(1), store.clone());
        let (mut stale, _) = encoded_manifest(b"hello erasure coding");
        stale.hlc = hlc(1);
        stale.etag = "\"old\"".to_string();
        stale.validate().unwrap();
        let mut winning = stale.clone();
        winning.hlc = hlc(20);
        winning.etag = "\"new\"".to_string();
        winning.validate().unwrap();
        store.put_manifest(&stale).await.unwrap();

        let remote = EcManifestTreeSnapshot::from_manifests(
            winning.erasure_set_id,
            vec![winning.clone()],
            4,
        )
        .unwrap();
        let metadata = MemoryMetadata::default();
        *metadata.result.lock().unwrap() = Some(EcManifestReadResult::Present(winning.clone()));

        let report = service.reconcile_manifest_divergence(&remote, &metadata).await.unwrap();

        assert!(!report.roots_matched);
        assert_eq!(report.candidate_objects, 1);
        assert_eq!(report.manifests_updated, 1);
        assert_eq!(
            store.get_manifest(&winning.object_key()).await.unwrap(),
            Some(winning)
        );
        let metrics = service.metrics().snapshot();
        assert_eq!(metrics.manifest_mismatches_total, 1);
        assert_eq!(metrics.manifest_repairs_total, 1);
    }

    #[tokio::test]
    async fn remote_only_manifest_is_cached_through_quorum_not_treated_as_delete() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let service = service(node(1), store.clone());
        let (manifest, _) = encoded_manifest(b"remote object");
        let remote = EcManifestTreeSnapshot::from_manifests(
            manifest.erasure_set_id,
            vec![manifest.clone()],
            4,
        )
        .unwrap();
        let metadata = MemoryMetadata::default();
        *metadata.result.lock().unwrap() = Some(EcManifestReadResult::Present(manifest.clone()));

        let report = service.reconcile_manifest_divergence(&remote, &metadata).await.unwrap();

        assert_eq!(report.present_manifests_cached, 1);
        assert_eq!(
            store.get_manifest(&manifest.object_key()).await.unwrap(),
            Some(manifest)
        );
    }

    #[tokio::test]
    async fn manifest_tree_snapshot_scans_all_pages() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let service = service_with_page_size(node(1), store.clone(), 1);
        let (mut first, _) = encoded_manifest(b"first manifest page");
        retarget_manifest(&mut first, "a.bin", Some("v1"), 10);
        let (mut second, _) = encoded_manifest(b"second manifest page");
        retarget_manifest(&mut second, "z.bin", Some("v1"), 20);
        store.put_manifest(&first).await.unwrap();
        store.put_manifest(&second).await.unwrap();

        let snapshot = service.build_manifest_tree_snapshot(first.erasure_set_id).await.unwrap();

        assert_eq!(snapshot.entries.len(), 2);
        assert!(snapshot.entries.iter().any(|entry| entry.object == first.object_key()));
        assert!(snapshot.entries.iter().any(|entry| entry.object == second.object_key()));
    }

    #[tokio::test]
    async fn manifest_tree_covers_only_its_own_set() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let service = service(node(1), store.clone());
        let (mut first, _) = encoded_manifest(b"first set 10");
        first.erasure_set_id = 10;
        retarget_manifest(&mut first, "first.bin", Some("v1"), 10);
        let (mut second, _) = encoded_manifest(b"second set 20");
        second.erasure_set_id = 20;
        retarget_manifest(&mut second, "second.bin", Some("v1"), 20);
        store.put_manifest(&first).await.unwrap();
        store.put_manifest(&second).await.unwrap();

        let snapshot = service.build_manifest_tree_snapshot(10).await.unwrap();

        assert_eq!(snapshot.erasure_set_id, 10);
        assert_eq!(snapshot.entries.len(), 1);
        assert_eq!(snapshot.entries[0].object, first.object_key());
    }

    #[tokio::test]
    async fn trees_of_different_sets_refuse_comparison() {
        let (mut manifest_a, _) = encoded_manifest(b"payload a");
        manifest_a.erasure_set_id = 11;
        manifest_a.validate().unwrap();
        let (mut manifest_b, _) = encoded_manifest(b"payload b");
        manifest_b.erasure_set_id = 22;
        manifest_b.validate().unwrap();

        let snap_a = EcManifestTreeSnapshot::from_manifests(11, vec![manifest_a], 4).unwrap();
        let snap_b = EcManifestTreeSnapshot::from_manifests(22, vec![manifest_b], 4).unwrap();

        let err = snap_a.divergent_ranges_against(&snap_b).unwrap_err();
        match err {
            EcAntiEntropyError::ErasureSetMismatch { local, remote } => {
                assert_eq!(local, 11);
                assert_eq!(remote, 22);
            }
            other => panic!("expected ErasureSetMismatch, got {other:?}"),
        }
        let msg = err.to_string();
        assert!(
            msg.contains("11"),
            "error message should contain local id: {msg}"
        );
        assert!(
            msg.contains("22"),
            "error message should contain remote id: {msg}"
        );
    }

    #[tokio::test]
    async fn one_scan_builds_a_snapshot_per_set() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let service = service(node(1), store.clone());
        let (mut manifest_a1, _) = encoded_manifest(b"a1");
        manifest_a1.erasure_set_id = 100;
        retarget_manifest(&mut manifest_a1, "a1.bin", Some("v1"), 10);
        let (mut manifest_a2, _) = encoded_manifest(b"a2");
        manifest_a2.erasure_set_id = 100;
        retarget_manifest(&mut manifest_a2, "a2.bin", Some("v1"), 20);
        let (mut manifest_b1, _) = encoded_manifest(b"b1");
        manifest_b1.erasure_set_id = 200;
        retarget_manifest(&mut manifest_b1, "b1.bin", Some("v1"), 30);

        store.put_manifest(&manifest_a1).await.unwrap();
        store.put_manifest(&manifest_a2).await.unwrap();
        store.put_manifest(&manifest_b1).await.unwrap();

        let snapshots = service.build_manifest_tree_snapshots(&[100, 200]).await.unwrap();

        assert_eq!(snapshots.len(), 2);
        assert_eq!(snapshots[0].erasure_set_id, 100);
        assert_eq!(snapshots[0].entries.len(), 2);
        assert_eq!(snapshots[1].erasure_set_id, 200);
        assert_eq!(snapshots[1].entries.len(), 1);
    }

    /// The task carries the zone of the shard's owner, which is what the zone
    /// repair limits are keyed by (plan 05, phase 3).
    #[tokio::test]
    async fn missing_local_shard_queues_repair() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let service = service(node(1), store.clone());
        let repair = repair_queue(store.clone());
        let (manifest, _) = encoded_manifest(b"repair me");
        store.put_manifest(&manifest).await.unwrap();

        let report = service
            .audit_manifest_shard_inventory(&manifest, &repair, &NoopEcShardQuarantine)
            .await
            .unwrap();

        assert_eq!(report.expected_local_shards, 1);
        assert_eq!(report.missing_local_shards, 1);
        assert_eq!(report.repairs_queued, 1);
        let tasks = repair.list_repair_tasks(10).await.unwrap();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].kind, EcRepairKind::MissingShard);
        assert_eq!(tasks[0].failure_domain.as_deref(), Some("a"));
    }

    #[tokio::test]
    async fn corrupt_local_shard_is_quarantined_and_queued_for_repair() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let service = service(node(1), store.clone());
        let repair = repair_queue(store.clone());
        let quarantine = RecordingQuarantine::default();
        let (manifest, shard_bytes) = encoded_manifest(b"corrupt me");
        store.put_manifest(&manifest).await.unwrap();
        let bytes = shard_bytes.get(&(0, 0)).unwrap();
        let (key, mut record) = local_record(&manifest, 0, bytes);
        record.crc32 = record.crc32.wrapping_add(1);
        store.put_local_shard(&key, &record).await.unwrap();

        let report = service
            .audit_manifest_shard_inventory(&manifest, &repair, &quarantine)
            .await
            .unwrap();

        assert_eq!(report.corrupt_local_shards, 1);
        assert_eq!(report.quarantined_shards, 1);
        assert_eq!(quarantine.requests.lock().unwrap().len(), 1);
        let tasks = repair.list_repair_tasks(10).await.unwrap();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].kind, EcRepairKind::CorruptShard);
    }

    #[tokio::test]
    async fn orphan_shard_is_reported_but_not_deleted() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let service = service(node(1), store.clone());
        let (manifest, shard_bytes) = encoded_manifest(b"orphan bytes");
        let bytes = shard_bytes.get(&(0, 0)).unwrap();
        let (key, record) = local_record(&manifest, 0, bytes);
        store.put_local_shard(&key, &record).await.unwrap();

        let orphans = service.detect_orphan_shards(10).await.unwrap();

        assert_eq!(orphans.len(), 1);
        assert!(store.get_local_shard(&key).await.unwrap().is_some());
        assert_eq!(service.metrics().snapshot().orphan_shards_total, 1);
    }

    #[tokio::test]
    async fn scrubber_scans_shards_after_first_page() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let service = service_with_page_size(node(1), store.clone(), 1);
        let repair = repair_queue(store.clone());
        let quarantine = RecordingQuarantine::default();
        let reader = MemoryLocalShardReader::default();
        let (mut live, live_shards) = encoded_manifest(b"live paged shard");
        retarget_manifest(&mut live, "a.bin", Some("v1"), 10);
        store.put_manifest(&live).await.unwrap();
        let live_bytes = live_shards.get(&(0, 0)).unwrap();
        let (live_key, live_record) = local_record(&live, 0, live_bytes);
        store.put_local_shard(&live_key, &live_record).await.unwrap();
        reader.holds(
            &live_key,
            EcLocalShardReadResponse {
                data: live_bytes.clone(),
                blob_header_crc_valid: true,
                blob_header_crc32: Some(crc32fast::hash(live_bytes)),
            },
        );

        let (mut orphan_manifest, orphan_shards) = encoded_manifest(b"orphan paged shard");
        retarget_manifest(&mut orphan_manifest, "z.bin", Some("v1"), 20);
        let orphan_bytes = orphan_shards.get(&(0, 0)).unwrap();
        let (orphan_key, orphan_record) = local_record(&orphan_manifest, 0, orphan_bytes);
        store.put_local_shard(&orphan_key, &orphan_record).await.unwrap();

        let report = service
            .scrub_local_shards(&reader, &repair, &quarantine, &NoopEcReconstructionVerifier)
            .await
            .unwrap();

        assert_eq!(report.local_shards_scanned, 2);
        assert_eq!(report.healthy_shards, 1);
        assert_eq!(report.orphan_candidates.len(), 1);
        assert_eq!(report.orphan_candidates[0].key, orphan_key);
    }

    #[tokio::test]
    async fn scrubber_blob_header_crc_failure_quarantines_and_repairs() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let service = service(node(1), store.clone());
        let repair = repair_queue(store.clone());
        let quarantine = RecordingQuarantine::default();
        let reader = MemoryLocalShardReader::default();
        let (manifest, shard_bytes) = encoded_manifest(b"scrub bytes");
        store.put_manifest(&manifest).await.unwrap();
        let bytes = shard_bytes.get(&(0, 0)).unwrap();
        let (key, record) = local_record(&manifest, 0, bytes);
        store.put_local_shard(&key, &record).await.unwrap();
        reader.holds(
            &key,
            EcLocalShardReadResponse {
                data: bytes.clone(),
                blob_header_crc_valid: false,
                blob_header_crc32: Some(crc32fast::hash(bytes)),
            },
        );

        let report = service
            .scrub_local_shards(&reader, &repair, &quarantine, &NoopEcReconstructionVerifier)
            .await
            .unwrap();

        assert_eq!(report.blob_header_crc_failures, 1);
        assert_eq!(report.corrupt_shards, 1);
        assert_eq!(report.repairs_queued, 1);
        assert_eq!(quarantine.requests.lock().unwrap().len(), 1);
        assert!(store.get_local_shard(&key).await.unwrap().is_some());
    }

    /// EC2-185: a shard this node holds a durable record for and cannot produce
    /// is rot, and is treated as rot.
    ///
    /// The distinction is the whole point. Before it, such a shard was reported
    /// as missing: no corruption counter moved, the node went on claiming it
    /// held the shard, and an operator watching for bit rot saw a placement
    /// problem instead. All three are checked here, because all three were
    /// wrong together.
    #[tokio::test]
    async fn scrubber_treats_a_shard_it_cannot_read_as_rot_rather_than_absence() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let service = service(node(1), store.clone());
        let repair = repair_queue(store.clone());
        let quarantine = RecordingQuarantine::default();
        let reader = MemoryLocalShardReader::default();
        let (manifest, shard_bytes) = encoded_manifest(b"rotted shard bytes");
        store.put_manifest(&manifest).await.unwrap();
        let bytes = shard_bytes.get(&(0, 0)).unwrap();
        let (key, record) = local_record(&manifest, 0, bytes);
        store.put_local_shard(&key, &record).await.unwrap();
        reader.refuses(&key, "local EC hidden object failed SHA-256 verification");

        let report = service
            .scrub_local_shards(&reader, &repair, &quarantine, &NoopEcReconstructionVerifier)
            .await
            .unwrap();

        assert_eq!(report.local_shards_scanned, 1);
        assert_eq!(report.corrupt_shards, 1);
        assert_eq!(report.physical_read_failures, 1);
        assert_eq!(report.missing_shards, 0, "rot must not read as absence");
        assert_eq!(report.missing_shard_repairs_queued, 0);
        assert_eq!(report.repairs_queued, 1);

        // Quarantined, with what the storage layer said carried into the record
        // an operator reads afterwards.
        let quarantined = quarantine.requests.lock().unwrap().clone();
        assert_eq!(quarantined.len(), 1);
        assert_eq!(quarantined[0].chunk_index, 0);
        assert_eq!(quarantined[0].slot_index, 0);
        assert!(
            quarantined[0].reason.contains("SHA-256"),
            "the quarantine record does not say why: {}",
            quarantined[0].reason
        );

        // And the repair is queued as a corrupt shard, which is what tells a
        // repair scheduler that bytes were lost rather than never written.
        let tasks = store.list_repair_tasks(16).await.unwrap();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].kind, EcRepairKind::CorruptShard);
    }

    /// A shard this node simply does not hold stays a missing shard.
    ///
    /// The other half of EC2-185: widening rot must not swallow absence, or a
    /// node that was never given a shard would be reported as having lost one.
    #[tokio::test]
    async fn scrubber_treats_a_shard_it_does_not_hold_as_missing() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let service = service(node(1), store.clone());
        let repair = repair_queue(store.clone());
        let quarantine = RecordingQuarantine::default();
        let reader = MemoryLocalShardReader::default();
        let (manifest, shard_bytes) = encoded_manifest(b"absent shard bytes");
        store.put_manifest(&manifest).await.unwrap();
        let bytes = shard_bytes.get(&(0, 0)).unwrap();
        let (key, record) = local_record(&manifest, 0, bytes);
        store.put_local_shard(&key, &record).await.unwrap();

        let report = service
            .scrub_local_shards(&reader, &repair, &quarantine, &NoopEcReconstructionVerifier)
            .await
            .unwrap();

        assert_eq!(report.missing_shards, 1);
        assert_eq!(report.missing_shard_repairs_queued, 1);
        assert_eq!(report.corrupt_shards, 0);
        assert_eq!(report.physical_read_failures, 0);
        assert!(quarantine.requests.lock().unwrap().is_empty());

        let tasks = store.list_repair_tasks(16).await.unwrap();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].kind, EcRepairKind::MissingShard);
    }

    /// EC2-200: a medium that did not answer is not a verdict on the shard.
    ///
    /// Nothing was learned about these bytes, so nothing is claimed about them:
    /// no quarantine, no corruption counter, no repair, and the node goes on
    /// holding the shard. Before this the same reply cost the node its record
    /// and put a `CorruptShard` repair on the wire — which is how a detached
    /// volume turned into a rebuild of everything it held.
    #[tokio::test]
    async fn scrubber_leaves_a_shard_alone_when_the_medium_did_not_answer() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let service = service(node(1), store.clone());
        let repair = repair_queue(store.clone());
        let quarantine = RecordingQuarantine::default();
        let reader = MemoryLocalShardReader::default();
        let (manifest, shard_bytes) = encoded_manifest(b"unreachable shard bytes");
        store.put_manifest(&manifest).await.unwrap();
        let bytes = shard_bytes.get(&(0, 0)).unwrap();
        let (key, record) = local_record(&manifest, 0, bytes);
        store.put_local_shard(&key, &record).await.unwrap();
        reader.unreachable(&key, "IO error: input/output error");

        let report = service
            .scrub_local_shards(&reader, &repair, &quarantine, &NoopEcReconstructionVerifier)
            .await
            .unwrap();

        assert_eq!(report.local_shards_scanned, 1);
        assert_eq!(report.media_unavailable, 1);
        assert!(!report.media_suspect, "one failure is not a failing disk");
        assert_eq!(
            report.corrupt_shards, 0,
            "an unread shard is not a corrupt one"
        );
        assert_eq!(report.physical_read_failures, 0);
        assert_eq!(
            report.missing_shards, 0,
            "an unread shard is not an absent one"
        );
        assert_eq!(report.repairs_queued, 0);
        assert_eq!(
            service.metrics.media_unavailable_total.load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            service.metrics.shard_corrupt_total.load(Ordering::Relaxed),
            0
        );

        // The node still holds the shard, and nothing was asked of the pool.
        assert!(store.get_local_shard(&key).await.unwrap().is_some());
        assert!(quarantine.requests.lock().unwrap().is_empty());
        assert!(store.list_repair_tasks(16).await.unwrap().is_empty());
    }

    /// EC2-200: enough media failures stop the pass instead of emptying the
    /// node's inventory one shard at a time.
    ///
    /// The pass says what it found — the medium — and looks no further, because
    /// every further answer would come from the same disk. This is the rule a
    /// volume scanner applies to a volume rather than to each block it holds.
    #[tokio::test]
    async fn scrub_stops_and_calls_the_medium_suspect_when_failures_pile_up() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let service = service(node(1), store.clone());
        let repair = repair_queue(store.clone());
        let quarantine = RecordingQuarantine::default();
        let reader = MemoryLocalShardReader::default();

        for index in 0..8u8 {
            let (mut manifest, shard_bytes) = encoded_manifest(&[b'a' + index; 32]);
            // Its own object, so the pass has eight shard records to walk and
            // not one written over seven times.
            manifest.key = format!("raw-{index}.bin");
            store.put_manifest(&manifest).await.unwrap();
            let bytes = shard_bytes.get(&(0, 0)).unwrap();
            let (key, record) = local_record(&manifest, 0, bytes);
            store.put_local_shard(&key, &record).await.unwrap();
            reader.unreachable(&key, "IO error: input/output error");
        }

        let report = service
            .scrub_local_shards(&reader, &repair, &quarantine, &NoopEcReconstructionVerifier)
            .await
            .unwrap();

        assert!(
            report.media_suspect,
            "a disk answering nothing is the finding"
        );
        assert_eq!(
            report.media_unavailable, EC_SCRUB_MEDIA_FAILURE_MIN_SAMPLES,
            "the pass stopped at the floor instead of walking the inventory"
        );
        assert_eq!(
            report.local_shards_scanned,
            EC_SCRUB_MEDIA_FAILURE_MIN_SAMPLES
        );
        assert_eq!(report.corrupt_shards, 0);
        assert_eq!(report.missing_shards, 0);
        assert_eq!(report.repairs_queued, 0);
        assert!(quarantine.requests.lock().unwrap().is_empty());
        assert!(store.list_repair_tasks(16).await.unwrap().is_empty());
    }

    /// EC2-200, the other side: a few unlucky reads over a healthy inventory
    /// are not a failing disk, and the pass finishes.
    #[tokio::test]
    async fn a_few_media_failures_do_not_condemn_a_disk() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let service = service(node(1), store.clone());
        let repair = repair_queue(store.clone());
        let quarantine = RecordingQuarantine::default();
        let reader = MemoryLocalShardReader::default();

        for index in 0..12u8 {
            let (mut manifest, shard_bytes) = encoded_manifest(&[b'a' + index; 48]);
            manifest.key = format!("raw-{index}.bin");
            store.put_manifest(&manifest).await.unwrap();
            let bytes = shard_bytes.get(&(0, 0)).unwrap();
            let (key, record) = local_record(&manifest, 0, bytes);
            store.put_local_shard(&key, &record).await.unwrap();
            if index < 3 {
                reader.unreachable(&key, "IO error: input/output error");
            } else {
                reader.holds(
                    &key,
                    EcLocalShardReadResponse {
                        data: bytes.clone(),
                        blob_header_crc_valid: true,
                        blob_header_crc32: Some(crc32fast::hash(bytes)),
                    },
                );
            }
        }

        let report = service
            .scrub_local_shards(&reader, &repair, &quarantine, &NoopEcReconstructionVerifier)
            .await
            .unwrap();

        assert!(!report.media_suspect);
        assert_eq!(report.local_shards_scanned, 12, "the pass ran to the end");
        assert_eq!(report.media_unavailable, 3);
        assert_eq!(report.healthy_shards, 9);
        assert_eq!(report.corrupt_shards, 0);
        assert!(store.list_repair_tasks(16).await.unwrap().is_empty());
    }

    /// The share of a pass that may fail on the medium is a fraction of it, and
    /// a configuration that is not one stops the node instead of being guessed.
    #[test]
    fn the_media_failure_ratio_has_to_be_a_fraction() {
        let mut config = EcAntiEntropyConfig::default();
        assert_eq!(
            config.media_failure_ratio,
            DEFAULT_EC_SCRUB_MEDIA_FAILURE_RATIO
        );
        assert!(config.validate().is_ok());

        for bad in [0.0, -0.5, 1.5] {
            config.media_failure_ratio = bad;
            let error = config.validate().unwrap_err().to_string();
            assert!(
                error.contains("S4_EC_SCRUB_MEDIA_FAILURE_RATIO"),
                "unexpected error for {bad}: {error}"
            );
        }
    }

    /// Every verdict lands in its own counter, and only a real breach becomes a
    /// violation the metric publishes.
    #[test]
    fn the_drift_report_counts_each_verdict_and_publishes_only_breaches() {
        let report = collect_layout_drift([
            (1, SetLayoutCompliance::NoGuarantee),
            (2, SetLayoutCompliance::Meets),
            (
                3,
                SetLayoutCompliance::Violated {
                    origin: LayoutViolationOrigin::Drift,
                    coincidences: 2,
                    detail: "local group 0 has 2 of its 4 members in rack 'z1/r1'".to_string(),
                },
            ),
            (
                4,
                SetLayoutCompliance::Violated {
                    origin: LayoutViolationOrigin::Creation,
                    coincidences: 1,
                    detail: "created under warn in breach of its rule".to_string(),
                },
            ),
            (
                5,
                SetLayoutCompliance::NotJudgeable {
                    detail: "the labels are incomplete".to_string(),
                },
            ),
        ]);

        assert_eq!(report.sets_checked, 5);
        assert_eq!(report.sets_without_guarantee, 1);
        assert_eq!(report.sets_meeting_rule, 1);
        assert_eq!(report.sets_drifted, 1);
        assert_eq!(report.sets_violated_at_creation, 1);
        assert_eq!(report.sets_not_judgeable, 1);
        assert_eq!(report.violators(), 2);
        assert_eq!(
            report.violations.get(&3),
            Some(&LayoutViolationOrigin::Drift)
        );
        assert_eq!(
            report.violations.get(&4),
            Some(&LayoutViolationOrigin::Creation)
        );
    }

    /// A pool of EC v1 sets has no violators, whatever its labels do: an EC v1
    /// set carries no placement guarantee, so switching the LRC group mode on
    /// cannot make one a violator (EC2-084, point 1).
    #[test]
    fn a_pool_of_ec_v1_sets_reports_no_violators() {
        let report = collect_layout_drift([
            (1, SetLayoutCompliance::NoGuarantee),
            (2, SetLayoutCompliance::NoGuarantee),
        ]);

        assert_eq!(report.sets_checked, 2);
        assert_eq!(report.sets_without_guarantee, 2);
        assert_eq!(report.violators(), 0);
        assert!(report.violations.is_empty());
    }

    /// An unjudgeable set is not a violator: an unknown answer must not be
    /// counted as a known breach.
    #[test]
    fn an_unjudgeable_set_is_not_counted_as_a_violator() {
        let report = collect_layout_drift([(
            7,
            SetLayoutCompliance::NotJudgeable {
                detail: "no node of the set carries labels".to_string(),
            },
        )]);

        assert_eq!(report.sets_checked, 1);
        assert_eq!(report.sets_not_judgeable, 1);
        assert_eq!(report.violators(), 0);
    }
}
