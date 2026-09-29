// Copyright 2026 S4Core Team
//
// Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

//! An object written before a topology label change stays whole after it.
//!
//! Regression tests for decision EC2-086. A set without a layout record is laid
//! out from topology labels, so a label change reorders its slots, and every
//! manifest written before used to be refused because its shards no longer sat
//! on "their" slot's node. Now a manifest is checked against the set's nodes,
//! not its current layout.
//!
//! The quorum and shard fakes below apply exactly the checks the cluster ports
//! apply — `validate_against_erasure_set` on every manifest a node returns, and
//! `ErasureSet::holds_node` before a shard read — so the read path, anti-entropy
//! and garbage collection see relabeled objects the way a real node does. The
//! same scenario on a running cluster is `scripts/45-ec-topology-lrc-test.sh`.
//!
//! The last test is the other half of that promise, for decision EC2-084: a set
//! created before the LRC group rule existed keeps its layout, its judgement and
//! its objects when the node it runs on switches the rule on.

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use s4_cluster::{HlcTimestamp, NodeId, PoolId};
use s4_ee::codec_runtime::{EcCodecCpuConfig, EcCodecCpuLimiter};
use s4_ee::ec_anti_entropy::{
    EcAntiEntropyConfig, EcAntiEntropyError, EcAntiEntropyMetadataCoordinator,
    EcAntiEntropyService, EcManifestTreeSnapshot, NoopEcShardQuarantine,
};
use s4_ee::ec_codec::build_codec;
use s4_ee::ec_gc::{
    EcDeadShardPurgeRequest, EcDeadShardPurger, EcGcConfig, EcGcCoordinator, EcGcError,
    EcGcMetadataCoordinator, EcOrphanShardObservation, EcTombstoneCommitResult, EcTombstoneQuorum,
};
use s4_ee::ec_metadata::{
    ChunkManifest, EcManifestReadResult, EcMetadataStore, EcObjectManifest, EcObjectManifestRef,
    EcObjectVersionKey, EcPlacementState, LocalShardKey, LocalShardRecord, LocalShardState,
    ObjectDataLayout, ShardManifest, EC_MANIFEST_FORMAT_VERSION,
};
use s4_ee::ec_placement::EcErasureSets;
use s4_ee::ec_read::{
    EcHeadObject, EcObjectLayoutReadResult, EcReadCoordinator, EcReadError,
    EcReadMetadataCoordinator, EcReadRepairQueue, EcReadRepairTask, EcReadRequest,
    EcReadShardRequest, EcReadShardResponse, EcReplicatedSmallHeadRequest,
    EcReplicatedSmallReadRequest, EcReplicatedSmallReadResult, EcReplicatedSmallReader,
    EcShardReader,
};
use s4_ee::ec_repair::{EcRepairConfig, EcRepairCoordinator};
use s4_ee::erasure_coding::{
    EcFailureDomainLevel, EcProfile, EcSetLayoutRule, EcTopologyPolicy, ErasureSet,
    ErasureSetBuilder, ErasureSetRegistry, PoolTopologyView, SetLayoutCompliance, TopologyLabels,
};
use sha2::{Digest, Sha256};
use uuid::Uuid;

/// The pool every production node serves, as `cluster_bootstrap` configures it.
fn pool() -> PoolId {
    PoolId::new(0)
}
const OPERATION: u128 = 0x5eed;

fn node(index: u8) -> NodeId {
    NodeId(u128::from(index))
}

fn sha256(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

fn codec_limiter() -> EcCodecCpuLimiter {
    EcCodecCpuLimiter::new(EcCodecCpuConfig {
        workers: NonZeroUsize::new(1).unwrap(),
        cpu_budget_percent: 60,
        max_inflight_encode_bytes: 1024 * 1024,
    })
}

fn object() -> EcObjectVersionKey {
    EcObjectVersionKey::new("photos", "before-the-label-change.bin", None)
}

/// Complete labels filling racks with consecutive nodes, one host per node.
fn racks(nodes: &[NodeId], per_rack: usize) -> HashMap<NodeId, TopologyLabels> {
    nodes
        .iter()
        .enumerate()
        .map(|(idx, node_id)| {
            (
                *node_id,
                TopologyLabels {
                    zone: Some("z1".to_string()),
                    rack: Some(format!("r{}", idx / per_rack)),
                    host: Some(format!("h{idx}")),
                    disk_group: Some("d0".to_string()),
                },
            )
        })
        .collect()
}

/// The inherited set of `profile`, as a node with `labels` builds it.
fn set_with_labels(profile: &EcProfile, labels: &HashMap<NodeId, TopologyLabels>) -> ErasureSet {
    let nodes = (1..=profile.total_shards()).map(node).collect::<Vec<_>>();
    ErasureSet::inherited(
        pool(),
        profile.clone(),
        &nodes,
        labels,
        EcTopologyPolicy::Warn,
        1,
        1,
    )
    .unwrap()
}

/// One set before and after the pool's nodes got labels: the same nodes,
/// another slot order. That is exactly what a label change does to a set that
/// has no layout record.
fn relabeled(profile: &EcProfile) -> (ErasureSet, ErasureSet) {
    let before = set_with_labels(profile, &HashMap::new());
    let nodes = before.slot_assignment.distinct_nodes();
    let after = set_with_labels(profile, &racks(&nodes, 3));
    let moved = before
        .slot_assignment
        .slot_order()
        .iter()
        .zip(after.slot_assignment.slot_order())
        .filter(|(was, now)| **was != *now)
        .count();
    assert!(
        moved > 0,
        "{}: the label change must move slots, or this proves nothing",
        profile.name
    );
    (before, after)
}

/// Small chunks, so one object spans several.
fn profile_with_small_chunks(mut profile: EcProfile) -> EcProfile {
    profile.chunk_size = 1024;
    profile
}

/// An object encoded into `set` as it was laid out when the object was
/// written: its manifest, and every shard's bytes by chunk and slot.
fn write_object(set: &ErasureSet, data: &[u8]) -> (EcObjectManifest, HashMap<(u32, u8), Bytes>) {
    let snapshot = set.profile.codec_snapshot();
    let codec = build_codec(snapshot.clone()).unwrap();
    let chunk_size = usize::try_from(snapshot.chunk_size).unwrap();
    let mut chunks = Vec::new();
    let mut shards_by_slot = HashMap::new();
    for (chunk_index, bytes) in data.chunks(chunk_size).enumerate() {
        let chunk_index = u32::try_from(chunk_index).unwrap();
        let encoded = codec.encode_chunk(bytes, chunk_index).unwrap();
        let shards = encoded
            .shards
            .iter()
            .map(|shard| {
                shards_by_slot.insert((chunk_index, shard.slot_index), shard.data.clone());
                let slot = set.slot_assignment.slots()[usize::from(shard.slot_index)];
                ShardManifest::from_codec_shard(shard, slot.node_id)
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
        format_version: EC_MANIFEST_FORMAT_VERSION,
        bucket: object().bucket,
        key: object().key,
        version_id: None,
        original_size: data.len() as u64,
        content_hash_sha256: sha256(data),
        etag: "\"etag\"".to_string(),
        content_type: "application/octet-stream".to_string(),
        user_metadata: HashMap::new(),
        profile: snapshot,
        erasure_set_id: set.set_id,
        topology_epoch: set.topology_epoch,
        chunks,
        state: EcPlacementState::HotReplicasPurged,
        hlc: HlcTimestamp::from_record(10, 0, 1),
        origin_node_id: node(1),
        operation_id: Uuid::from_u128(OPERATION),
        created_at: 42,
        is_deleted: false,
        packed: None,
    };
    manifest.validate_against_erasure_set(set).unwrap();
    (manifest, shards_by_slot)
}

fn payload() -> Vec<u8> {
    (0..5_000u32).map(|idx| (idx % 251) as u8).collect()
}

/// The pool's metadata quorum as a node sees it after the label change.
///
/// Every manifest passes `validate_against_erasure_set` against the current set
/// before it is returned — what `read_manifest_from_node` does for each node's
/// answer — and a manifest that fails it is an error, which is how a failed
/// check reaches the quorum read in production.
struct QuorumAfterTheChange {
    set: ErasureSet,
    manifest: EcObjectManifest,
}

impl QuorumAfterTheChange {
    fn checked_manifest(&self) -> Result<EcManifestReadResult, String> {
        self.manifest
            .validate_against_erasure_set(&self.set)
            .map(|()| EcManifestReadResult::Present(self.manifest.clone()))
            .map_err(|error| error.to_string())
    }
}

#[async_trait]
impl EcReadMetadataCoordinator for QuorumAfterTheChange {
    async fn quorum_read_object_layout(
        &self,
        _object: &EcObjectVersionKey,
    ) -> Result<EcObjectLayoutReadResult, EcReadError> {
        Ok(EcObjectLayoutReadResult::Present(
            ObjectDataLayout::ErasureCoded(
                EcObjectManifestRef::from_manifest(&self.manifest).unwrap(),
            ),
        ))
    }

    async fn quorum_read_manifest(
        &self,
        _object: &EcObjectVersionKey,
    ) -> Result<EcManifestReadResult, EcReadError> {
        self.checked_manifest()
            .map_err(|reason| EcReadError::InvalidManifest { reason })
    }
}

#[async_trait]
impl EcAntiEntropyMetadataCoordinator for QuorumAfterTheChange {
    async fn quorum_read_manifest(
        &self,
        _object: &EcObjectVersionKey,
    ) -> Result<EcManifestReadResult, EcAntiEntropyError> {
        self.checked_manifest().map_err(|reason| {
            EcAntiEntropyError::Metadata(s4_ee::ec_metadata::EcMetadataError::QuorumRead(reason))
        })
    }
}

#[async_trait]
impl EcTombstoneQuorum for QuorumAfterTheChange {
    async fn quorum_read_manifest(
        &self,
        _object: &EcObjectVersionKey,
    ) -> Result<EcManifestReadResult, EcGcError> {
        self.checked_manifest().map_err(|reason| {
            EcGcError::Metadata(s4_ee::ec_metadata::EcMetadataError::QuorumRead(reason))
        })
    }

    async fn commit_tombstone(
        &self,
        _tombstone: EcObjectManifest,
        _required_acks: u8,
    ) -> Result<EcTombstoneCommitResult, EcGcError> {
        panic!("nothing in these tests deletes the object");
    }
}

#[async_trait]
impl EcGcMetadataCoordinator for QuorumAfterTheChange {
    async fn quorum_read_object_layout(
        &self,
        object: &EcObjectVersionKey,
    ) -> Result<EcObjectLayoutReadResult, EcGcError> {
        <Self as EcReadMetadataCoordinator>::quorum_read_object_layout(self, object)
            .await
            .map_err(|error| EcGcError::InvalidRequest {
                reason: error.to_string(),
            })
    }

    async fn retire_tombstone_everywhere(
        &self,
        _tombstone: &EcObjectManifest,
    ) -> Result<(), EcGcError> {
        Ok(())
    }
}

/// Shard storage after the label change: every shard where it was written,
/// read through the membership check a real node applies before a shard read.
struct ShardsAfterTheChange {
    set: ErasureSet,
    shards: HashMap<(u32, u8), Bytes>,
    placed_on: HashMap<(u32, u8), NodeId>,
    refused: Mutex<Vec<EcReadShardRequest>>,
}

#[async_trait]
impl EcShardReader for ShardsAfterTheChange {
    async fn read_shard(
        &self,
        request: EcReadShardRequest,
    ) -> Result<Option<EcReadShardResponse>, EcReadError> {
        if !self.set.holds_node(request.node_id) {
            self.refused.lock().unwrap().push(request.clone());
            return Err(EcReadError::InvalidManifest {
                reason: format!("node {} is not a node of the set", request.node_id),
            });
        }
        let key = (request.chunk_index, request.slot_index);
        // The bytes live on the node the manifest names, and only there.
        if self.placed_on.get(&key) != Some(&request.node_id) {
            return Ok(None);
        }
        Ok(self.shards.get(&key).map(|data| EcReadShardResponse {
            data: data.clone(),
            replica_crc32: Some(crc32fast::hash(data)),
        }))
    }
}

/// Every shard's node, taken from the manifest the object was written with.
fn placement(manifest: &EcObjectManifest) -> HashMap<(u32, u8), NodeId> {
    manifest
        .chunks
        .iter()
        .flat_map(|chunk| {
            chunk
                .shards
                .iter()
                .map(|shard| ((chunk.chunk_index, shard.slot_index), shard.node_id))
        })
        .collect()
}

#[derive(Default)]
struct RecordingRepairQueue {
    tasks: Mutex<Vec<EcReadRepairTask>>,
}

#[async_trait]
impl EcReadRepairQueue for RecordingRepairQueue {
    async fn enqueue(&self, task: EcReadRepairTask) -> Result<(), EcReadError> {
        self.tasks.lock().unwrap().push(task);
        Ok(())
    }
}

/// The objects here are erasure coded; a replicated-small read would be a bug.
struct NoSmallObjects;

#[async_trait]
impl EcReplicatedSmallReader for NoSmallObjects {
    async fn read_replicated_small(
        &self,
        _request: EcReplicatedSmallReadRequest,
    ) -> Result<EcReplicatedSmallReadResult, EcReadError> {
        panic!("an erasure-coded object must not be read as replicated-small");
    }

    async fn head_replicated_small(
        &self,
        _request: EcReplicatedSmallHeadRequest,
    ) -> Result<EcHeadObject, EcReadError> {
        panic!("an erasure-coded object must not be read as replicated-small");
    }
}

/// The object reads in full, from every shard where it was written, with no
/// degraded decode and no repair work queued for shards that are whole. Before
/// EC2-086 each shard whose slot had moved was refused and queued for repair.
#[tokio::test]
async fn an_object_written_before_a_label_change_reads_in_full_after_it() {
    for profile in [EcProfile::rs_standard(), EcProfile::lrc_standard()] {
        let profile = profile_with_small_chunks(profile);
        let (written_under, read_under) = relabeled(&profile);
        let data = payload();
        let (manifest, shards) = write_object(&written_under, &data);

        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let reader = EcReadCoordinator::new(store, codec_limiter());
        let quorum = QuorumAfterTheChange {
            set: read_under.clone(),
            manifest: manifest.clone(),
        };
        let shard_reader = ShardsAfterTheChange {
            set: read_under,
            placed_on: placement(&manifest),
            shards,
            refused: Mutex::new(Vec::new()),
        };
        let repairs = RecordingRepairQueue::default();

        let read = reader
            .read(
                EcReadRequest::full(object()),
                &quorum,
                &NoSmallObjects,
                &shard_reader,
                &repairs,
            )
            .await
            .unwrap_or_else(|error| panic!("{}: {error}", profile.name));

        assert_eq!(read.body.as_ref(), data.as_slice(), "{}", profile.name);
        assert_eq!(read.degraded_chunks, 0, "{}", profile.name);
        assert!(!read.repair_enqueued, "{}", profile.name);
        assert!(repairs.tasks.lock().unwrap().is_empty(), "{}", profile.name);
        assert!(
            shard_reader.refused.lock().unwrap().is_empty(),
            "{}",
            profile.name
        );
    }
}

/// Anti-entropy on every node of the set, after the label change: the
/// manifest a peer lacks is confirmed through the quorum and kept, and no
/// node finds a shard to repair or quarantine.
#[tokio::test]
async fn anti_entropy_leaves_an_object_written_before_a_label_change_alone() {
    for profile in [EcProfile::rs_standard(), EcProfile::lrc_standard()] {
        let profile = profile_with_small_chunks(profile);
        let (written_under, read_under) = relabeled(&profile);
        let data = payload();
        let (manifest, shards) = write_object(&written_under, &data);
        let quorum = QuorumAfterTheChange {
            set: read_under.clone(),
            manifest: manifest.clone(),
        };

        for node_id in read_under.slot_assignment.distinct_nodes() {
            let dir = tempfile::tempdir().unwrap();
            let store = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
            store.put_manifest(&manifest).await.unwrap();
            for chunk in &manifest.chunks {
                for shard in chunk.shards.iter().filter(|shard| shard.node_id == node_id) {
                    let bytes = &shards[&(chunk.chunk_index, shard.slot_index)];
                    store
                        .put_local_shard(
                            &LocalShardKey {
                                object: object(),
                                chunk_index: chunk.chunk_index,
                                slot_index: shard.slot_index,
                            },
                            &LocalShardRecord {
                                volume_id: 1,
                                offset: u64::from(shard.slot_index) * 4096,
                                size: bytes.len() as u64,
                                crc32: crc32fast::hash(bytes),
                                sha256: sha256(bytes),
                                state: LocalShardState::Durable,
                                operation_id: manifest.operation_id,
                            },
                        )
                        .await
                        .unwrap();
                }
            }
            let anti_entropy =
                EcAntiEntropyService::new(node_id, store.clone(), EcAntiEntropyConfig::default())
                    .unwrap();
            let repairs = EcRepairCoordinator::new(
                store.clone(),
                codec_limiter(),
                EcRepairConfig::default(),
                PoolTopologyView::unlabeled(node_id),
            );

            // A peer without the manifest disagrees with this node's tree.
            let peer_without_it = EcManifestTreeSnapshot::from_manifests(
                manifest.erasure_set_id,
                Vec::new(),
                anti_entropy.config().merkle_depth,
            )
            .unwrap();
            let reconciled = anti_entropy
                .reconcile_manifest_divergence(&peer_without_it, &quorum)
                .await
                .unwrap_or_else(|error| panic!("{} on {node_id}: {error}", profile.name));
            assert_eq!(reconciled.present_manifests_cached, 1);
            assert_eq!(reconciled.manifests_updated, 0);
            assert_eq!(reconciled.absent_confirmed, 0);
            assert_eq!(
                store.get_manifest(&object()).await.unwrap(),
                Some(manifest.clone())
            );

            let audit = anti_entropy
                .audit_manifest_shard_inventory(&manifest, &repairs, &NoopEcShardQuarantine)
                .await
                .unwrap();
            assert!(audit.expected_local_shards > 0, "{node_id} holds shards");
            assert_eq!(audit.missing_local_shards, 0);
            assert_eq!(audit.corrupt_local_shards, 0);
            assert_eq!(audit.repairs_queued, 0);
            assert!(repairs.list_repair_tasks(16).await.unwrap().is_empty());
        }
    }
}

#[derive(Default)]
struct RecordingPurger {
    purged: Mutex<Vec<EcDeadShardPurgeRequest>>,
}

#[async_trait]
impl EcDeadShardPurger for RecordingPurger {
    async fn purge_dead_shard(&self, request: EcDeadShardPurgeRequest) -> Result<(), EcGcError> {
        self.purged.lock().unwrap().push(request);
        Ok(())
    }
}

/// Orphan collection after the label change: a leftover shard record of the
/// object's own operation is kept, because the committed manifest still
/// references it — the check that used to fail once the slots had moved.
#[tokio::test]
async fn orphan_gc_keeps_the_shards_of_an_object_written_before_a_label_change() {
    for profile in [EcProfile::rs_standard(), EcProfile::lrc_standard()] {
        let profile = profile_with_small_chunks(profile);
        let (written_under, read_under) = relabeled(&profile);
        let (manifest, _) = write_object(&written_under, &payload());
        let quorum = QuorumAfterTheChange {
            set: read_under.clone(),
            manifest: manifest.clone(),
        };

        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let registry = ErasureSetRegistry::from_sets(pool(), vec![read_under]).unwrap();
        let gc = EcGcCoordinator::new(
            store.clone(),
            EcGcConfig {
                orphan_min_age_secs: 1,
                orphan_lease_grace_secs: 1,
                transcode_lease_secs: 1,
                ..EcGcConfig::default()
            },
            Arc::new(EcErasureSets::new(registry)),
        );
        let key = LocalShardKey {
            object: object(),
            chunk_index: 0,
            slot_index: 0,
        };
        let record = LocalShardRecord {
            volume_id: 1,
            offset: 0,
            size: manifest.chunks[0].encoded_size,
            crc32: manifest.chunks[0].shards[0].crc32,
            sha256: manifest.chunks[0].shards[0].sha256,
            state: LocalShardState::Durable,
            operation_id: manifest.operation_id,
        };
        store.put_local_shard(&key, &record).await.unwrap();
        gc.record_orphan_shard(EcOrphanShardObservation {
            key: key.clone(),
            record,
            target_erasure_set_id: manifest.erasure_set_id,
            observed_at: 1_000_000_000,
            reason: "left behind by a retried transcode".to_string(),
        })
        .await
        .unwrap();
        let purger = RecordingPurger::default();

        // Long after every grace period has passed.
        let report = gc
            .run_orphan_gc(1_000_000_000_000, &quorum, &purger)
            .await
            .unwrap_or_else(|error| panic!("{}: {error}", profile.name));

        assert_eq!(report.orphans_retained, 1, "{}", profile.name);
        assert_eq!(report.orphans_purged, 0, "{}", profile.name);
        assert!(purger.purged.lock().unwrap().is_empty());
        assert!(store.get_local_shard(&key).await.unwrap().is_some());
    }
}

/// A set created before the rule existed is untouched by switching it on.
///
/// What an operator does on upgrade: set `S4_EC_LRC_GROUP_DOMAIN` on a pool
/// whose sets were laid out by the EC v1 rule. Those sets are rebuilt from their
/// layout record under the rule they were created with, not laid out again by
/// the new one; a rule they never carried cannot be breached, so they do not
/// become violators; and the objects in them read whole. That is
/// `EC2-ZONE-05-004` at the integration level — the same pool on a running
/// cluster is phase 12 onwards of `scripts/45-ec-topology-lrc-test.sh`.
#[tokio::test]
async fn a_set_created_before_the_rule_is_untouched_by_switching_it_on() {
    for profile in [EcProfile::rs_standard(), EcProfile::lrc_standard()] {
        let profile = profile_with_small_chunks(profile);
        let nodes = (1..=profile.total_shards()).map(node).collect::<Vec<_>>();
        let labels = racks(&nodes, 3);

        // The set as the release before this one built it, and an object in it.
        let before = set_with_labels(&profile, &labels);
        let data = payload();
        let (manifest, shards) = write_object(&before, &data);

        // The same pool after the upgrade: same nodes, same labels, rule on.
        let builder = ErasureSetBuilder::new(
            pool(),
            "ec-pool",
            profile.clone(),
            nodes.clone(),
            labels.clone(),
            EcTopologyPolicy::Warn,
            1,
        )
        .with_lrc_group_domain(Some(EcFailureDomainLevel::Rack));

        let recorded = before.slot_assignment.slot_order();
        let restored = builder
            .with_recorded_layout(&before, EcSetLayoutRule::V1, &recorded)
            .unwrap_or_else(|error| panic!("{}: {error}", profile.name));

        assert_eq!(
            restored.slot_assignment.slot_order(),
            recorded,
            "{}: switching the level on moved a slot of a set created before it",
            profile.name
        );
        assert_eq!(
            restored.layout_rule,
            EcSetLayoutRule::V1,
            "{}",
            profile.name
        );
        assert_eq!(
            builder.assess_set_layout(&restored, false),
            SetLayoutCompliance::NoGuarantee,
            "{}: a set is judged by the rule it was laid out by and no other",
            profile.name
        );

        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let reader = EcReadCoordinator::new(store, codec_limiter());
        let quorum = QuorumAfterTheChange {
            set: restored.clone(),
            manifest: manifest.clone(),
        };
        let shard_reader = ShardsAfterTheChange {
            set: restored,
            placed_on: placement(&manifest),
            shards,
            refused: Mutex::new(Vec::new()),
        };
        let repairs = RecordingRepairQueue::default();

        let read = reader
            .read(
                EcReadRequest::full(object()),
                &quorum,
                &NoSmallObjects,
                &shard_reader,
                &repairs,
            )
            .await
            .unwrap_or_else(|error| panic!("{}: {error}", profile.name));

        assert_eq!(read.body.as_ref(), data.as_slice(), "{}", profile.name);
        assert_eq!(read.degraded_chunks, 0, "{}", profile.name);
        assert!(repairs.tasks.lock().unwrap().is_empty(), "{}", profile.name);
        assert!(
            shard_reader.refused.lock().unwrap().is_empty(),
            "{}",
            profile.name
        );
    }
}
