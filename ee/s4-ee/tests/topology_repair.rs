// Copyright 2026 S4Core Team
//
// Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

//! Repair and degraded reads of a set laid out by the LRC group rule, when a
//! node or a whole rack loses its shards (plan 05, phase 3).
//!
//! The set is built exactly as a node builds it: `ErasureSetBuilder::build_new`
//! with `S4_EC_LRC_GROUP_DOMAIN=rack`, over ten nodes in five racks of two. The
//! rule puts every member of a local group in its own rack, so a rack takes at
//! most one member of each group — and every shard it held should come back by
//! XOR inside its group, reading the rest of the group and nothing else, except
//! a global parity shard, which is decoded. The fakes below stand in for the
//! cluster: a shard lives on the node the manifest names, and a lost node
//! answers that it has none.

use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use s4_cluster::{HlcTimestamp, NodeId, PoolId};
use s4_ee::codec_runtime::{EcCodecCpuConfig, EcCodecCpuLimiter};
use s4_ee::ec_codec::build_codec;
use s4_ee::ec_metadata::{
    ChunkManifest, EcManifestReadResult, EcMetadataStore, EcObjectManifest, EcObjectManifestRef,
    EcObjectVersionKey, EcPlacementState, EcRepairKind, EcRepairStatus, ObjectDataLayout,
    ShardManifest, EC_MANIFEST_FORMAT_VERSION,
};
use s4_ee::ec_read::{
    EcHeadObject, EcObjectLayoutReadResult, EcReadCoordinator, EcReadError,
    EcReadMetadataCoordinator, EcReadRepairQueue, EcReadRepairTask, EcReadRequest,
    EcReadShardRequest, EcReadShardResponse, EcReplicatedSmallHeadRequest,
    EcReplicatedSmallReadRequest, EcReplicatedSmallReadResult, EcReplicatedSmallReader,
    EcShardReader,
};
use s4_ee::ec_repair::{
    EcRepairConfig, EcRepairCoordinator, EcRepairError, EcRepairFrontierUpdate,
    EcRepairFrontierUpdater, EcRepairMetadataCoordinator, EcRepairQueueRequest,
    EcRepairShardReadRequest, EcRepairShardReadResponse, EcRepairShardReader,
    EcRepairShardWriteAck, EcRepairShardWriteRequest, EcRepairShardWriter,
};
use s4_ee::ec_repair_plan::{RepairLeg, RepairStrategy, TransferScope};
use s4_ee::erasure_coding::{
    EcFailureDomainLevel, EcProfile, EcTopologyPolicy, ErasureSet, ErasureSetBuilder,
    PoolTopologyView, TopologyLabels,
};
use sha2::{Digest, Sha256};
use uuid::Uuid;

const RACKS: usize = 5;

fn node(index: usize) -> NodeId {
    NodeId(u128::try_from(index).unwrap())
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
    EcObjectVersionKey::new("photos", "laid-out-by-the-rule.bin", None)
}

/// Nodes 1-10 in one zone, two to a rack: nodes 1 and 2 in rack r0, 3 and 4 in
/// r1, and so on.
fn pool() -> (Vec<NodeId>, HashMap<NodeId, TopologyLabels>) {
    let nodes = (1..=2 * RACKS).map(node).collect::<Vec<_>>();
    let labels = nodes
        .iter()
        .enumerate()
        .map(|(index, node_id)| {
            (
                *node_id,
                TopologyLabels {
                    zone: Some("z1".to_string()),
                    rack: Some(format!("r{}", index / 2)),
                    host: Some(format!("h{index}")),
                    disk_group: Some("d0".to_string()),
                },
            )
        })
        .collect();
    (nodes, labels)
}

/// The `lrc_standard` set a node adds on the whole pool with the group rule on.
fn rule_set() -> ErasureSet {
    let (nodes, labels) = pool();
    let mut profile = EcProfile::lrc_standard();
    profile.chunk_size = 1024;
    let builder = ErasureSetBuilder::new(
        PoolId::new(0),
        "ec",
        profile,
        nodes.clone(),
        labels,
        EcTopologyPolicy::Strict,
        1,
    )
    .with_lrc_group_domain(Some(EcFailureDomainLevel::Rack));
    let created = builder.build_new(1, &nodes, 1).unwrap();
    assert!(created.violation.is_none(), "{:?}", created.violation);
    created.set
}

fn rack_of(node_id: NodeId) -> usize {
    usize::try_from(node_id.0 - 1).unwrap() / 2
}

/// An object of several chunks encoded into `set`: its manifest, and every
/// shard's bytes by chunk and slot.
fn write_object(set: &ErasureSet) -> (EcObjectManifest, HashMap<(u32, u8), Bytes>, Vec<u8>) {
    let data = (0..5_000u32).map(|idx| ((idx * 13) % 251) as u8).collect::<Vec<_>>();
    let snapshot = set.profile.codec_snapshot();
    let codec = build_codec(snapshot.clone()).unwrap();
    let chunk_size = usize::try_from(snapshot.chunk_size).unwrap();
    let mut chunks = Vec::new();
    let mut shards = HashMap::new();
    for (chunk_index, bytes) in data.chunks(chunk_size).enumerate() {
        let chunk_index = u32::try_from(chunk_index).unwrap();
        let encoded = codec.encode_chunk(bytes, chunk_index).unwrap();
        let manifests = encoded
            .shards
            .iter()
            .map(|shard| {
                shards.insert((chunk_index, shard.slot_index), shard.data.clone());
                let slot = set.slot_assignment.slots()[usize::from(shard.slot_index)];
                ShardManifest::from_codec_shard(shard, slot.node_id)
            })
            .collect();
        chunks.push(ChunkManifest {
            chunk_index,
            data_size: encoded.original_size,
            encoded_size: encoded.shard_size,
            chunk_sha256: sha256(bytes),
            shards: manifests,
        });
    }
    let manifest = EcObjectManifest {
        format_version: EC_MANIFEST_FORMAT_VERSION,
        bucket: object().bucket,
        key: object().key,
        version_id: None,
        original_size: data.len() as u64,
        content_hash_sha256: sha256(&data),
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
        operation_id: Uuid::from_u128(0x7e57),
        created_at: 42,
        is_deleted: false,
        packed: None,
    };
    manifest.validate_against_erasure_set(set).unwrap();
    (manifest, shards, data)
}

/// The pool's shards, on the nodes the manifest names. A node in `emptied`
/// came back without its shards and answers that it has none; a node in
/// `unreachable` does not answer at all.
struct Cluster {
    manifest: EcObjectManifest,
    shards: Mutex<HashMap<(u32, u8), Bytes>>,
    placed_on: HashMap<(u32, u8), NodeId>,
    emptied: HashSet<NodeId>,
    unreachable: HashSet<NodeId>,
    /// Every repair source asked for, by chunk and slot.
    repair_reads: Mutex<Vec<(u32, u8)>>,
}

impl Cluster {
    fn new(manifest: &EcObjectManifest, shards: HashMap<(u32, u8), Bytes>) -> Self {
        let placed_on = manifest
            .chunks
            .iter()
            .flat_map(|chunk| {
                chunk
                    .shards
                    .iter()
                    .map(|shard| ((chunk.chunk_index, shard.slot_index), shard.node_id))
            })
            .collect();
        Self {
            manifest: manifest.clone(),
            shards: Mutex::new(shards),
            placed_on,
            emptied: HashSet::new(),
            unreachable: HashSet::new(),
            repair_reads: Mutex::new(Vec::new()),
        }
    }

    /// What `node_id` answers for a shard: its bytes, "none", or nothing.
    fn answer(&self, key: (u32, u8), node_id: NodeId) -> Result<Option<Bytes>, String> {
        if self.unreachable.contains(&node_id) {
            return Err(format!("node {node_id} does not answer"));
        }
        if self.emptied.contains(&node_id) || self.placed_on.get(&key) != Some(&node_id) {
            return Ok(None);
        }
        Ok(self.shards.lock().unwrap().get(&key).cloned())
    }
}

#[async_trait]
impl EcRepairMetadataCoordinator for Cluster {
    async fn quorum_read_manifest(
        &self,
        _object: &EcObjectVersionKey,
    ) -> Result<EcManifestReadResult, EcRepairError> {
        Ok(EcManifestReadResult::Present(self.manifest.clone()))
    }
}

#[async_trait]
impl EcRepairShardReader for Cluster {
    async fn read_repair_shard(
        &self,
        request: EcRepairShardReadRequest,
    ) -> Result<Option<EcRepairShardReadResponse>, EcRepairError> {
        let key = (request.chunk_index, request.slot_index);
        self.repair_reads.lock().unwrap().push(key);
        match self.answer(key, request.node_id) {
            Ok(data) => Ok(data.map(|data| EcRepairShardReadResponse {
                replica_crc32: Some(crc32fast::hash(&data)),
                data,
            })),
            Err(reason) => Err(EcRepairError::SourceUnavailable { reason }),
        }
    }
}

/// Repaired shards land back on their node, which then holds them again.
struct Writer<'a> {
    cluster: &'a Cluster,
    written: Mutex<Vec<(u32, u8)>>,
}

#[async_trait]
impl EcRepairShardWriter for Writer<'_> {
    async fn write_repaired_shard(
        &self,
        request: EcRepairShardWriteRequest,
    ) -> Result<EcRepairShardWriteAck, EcRepairError> {
        let key = (request.chunk_index, request.expected.slot_index);
        assert_eq!(
            self.cluster.shards.lock().unwrap().get(&key),
            Some(&request.data),
            "the repaired shard of chunk {} slot {} is not the original",
            key.0,
            key.1
        );
        self.written.lock().unwrap().push(key);
        Ok(EcRepairShardWriteAck {
            node_id: request.expected.node_id,
            size: request.expected.size,
            crc32: request.expected.crc32,
            sha256: request.expected.sha256,
            durable: true,
            local_index_updated: true,
        })
    }
}

struct NoFrontier;

#[async_trait]
impl EcRepairFrontierUpdater for NoFrontier {
    async fn mark_shard_repaired(
        &self,
        _update: EcRepairFrontierUpdate,
    ) -> Result<(), EcRepairError> {
        Ok(())
    }
}

/// What happened to every shard `lost_nodes` held once their repairs ran.
struct Outcome {
    strategies: HashMap<(u32, u8), RepairStrategy>,
    sources_read: HashMap<(u32, u8), usize>,
    cross_zone_bytes: u64,
    source_bytes: u64,
    /// Sources read times the shard size of their chunk — the last chunk of
    /// the object is shorter, and so are its shards.
    expected_source_bytes: u64,
}

/// Queue a repair of every shard the nodes in `lost` held, on the node that
/// owns it — as each node's anti-entropy does when it comes back empty — and
/// run them all.
async fn repair_everything_on(lost: &[NodeId]) -> Outcome {
    let set = rule_set();
    let (manifest, shards, _) = write_object(&set);
    let mut cluster = Cluster::new(&manifest, shards);
    cluster.emptied = lost.iter().copied().collect();
    let (nodes, labels) = pool();
    let dir = tempfile::tempdir().unwrap();

    let mut outcome = Outcome {
        strategies: HashMap::new(),
        sources_read: HashMap::new(),
        cross_zone_bytes: 0,
        source_bytes: 0,
        expected_source_bytes: 0,
    };
    for owner in lost {
        let store =
            Arc::new(EcMetadataStore::open(&dir.path().join(format!("node-{}", owner.0))).unwrap());
        let coordinator = EcRepairCoordinator::new(
            store,
            codec_limiter(),
            EcRepairConfig::default(),
            PoolTopologyView::build(*owner, &nodes, &labels),
        );
        let writer = Writer {
            cluster: &cluster,
            written: Mutex::new(Vec::new()),
        };
        for chunk in &manifest.chunks {
            for shard in chunk.shards.iter().filter(|shard| shard.node_id == *owner) {
                let key = (chunk.chunk_index, shard.slot_index);
                let task = coordinator
                    .enqueue_repair(EcRepairQueueRequest {
                        object: manifest.object_key(),
                        operation_id: manifest.operation_id,
                        erasure_set_id: manifest.erasure_set_id,
                        chunk_index: chunk.chunk_index,
                        slot_index: shard.slot_index,
                        node_id: *owner,
                        kind: EcRepairKind::MissingShard,
                        reason: "the node came back without its shards".to_string(),
                        failure_domain: None,
                    })
                    .await
                    .unwrap();
                assert_eq!(task.failure_domain.as_deref(), Some("z1"));

                let reads_before = cluster.repair_reads.lock().unwrap().len();
                let report = coordinator
                    .repair_task(task.repair_id, &cluster, &cluster, &writer, &NoFrontier)
                    .await
                    .unwrap_or_else(|error| panic!("chunk {} slot {}: {error}", key.0, key.1));
                assert_eq!(report.task.status, EcRepairStatus::Succeeded);
                outcome.strategies.insert(key, report.strategy);
                let reads = cluster.repair_reads.lock().unwrap().len() - reads_before;
                outcome.sources_read.insert(key, reads);
                outcome.expected_source_bytes += reads as u64 * chunk.encoded_size;
                outcome.cross_zone_bytes += report.transfers.cross_zone();
                outcome.source_bytes += report.transfers.on_leg(RepairLeg::Source);
                assert_eq!(
                    report.transfers.get(RepairLeg::Write, TransferScope::Local),
                    shard.size,
                    "the owner writes its own shard"
                );
            }
        }
        let expected = manifest
            .chunks
            .iter()
            .flat_map(|chunk| {
                chunk
                    .shards
                    .iter()
                    .filter(|shard| shard.node_id == *owner)
                    .map(|shard| (chunk.chunk_index, shard.slot_index))
            })
            .collect::<Vec<_>>();
        assert_eq!(*writer.written.lock().unwrap(), expected);
    }
    outcome
}

/// Whether `slot` is an LRC global parity slot of `lrc_standard`.
fn is_global_parity(slot: u8) -> bool {
    slot >= 8
}

/// One node loses every shard. Each of its data and local parity shards comes
/// back by XOR from the three other members of its group; a global parity
/// shard, which has no group, is decoded from six.
#[tokio::test]
async fn a_node_that_lost_its_shards_gets_them_back_from_their_groups() {
    let set = rule_set();
    for slot in set.slot_assignment.slots() {
        let outcome = repair_everything_on(&[slot.node_id]).await;
        assert!(!outcome.strategies.is_empty());
        for ((chunk, repaired_slot), strategy) in &outcome.strategies {
            let reads = outcome.sources_read[&(*chunk, *repaired_slot)];
            if is_global_parity(*repaired_slot) {
                assert_eq!(*strategy, RepairStrategy::Global, "slot {repaired_slot}");
                assert_eq!(reads, 6, "slot {repaired_slot}");
            } else {
                assert!(
                    matches!(strategy, RepairStrategy::LocalGroup { .. }),
                    "slot {repaired_slot}: {strategy}"
                );
                assert_eq!(reads, 3, "slot {repaired_slot}");
            }
        }
        // One zone: nothing crosses one. Every source read is counted with the
        // bytes it moved.
        assert_eq!(outcome.cross_zone_bytes, 0);
        assert_eq!(outcome.source_bytes, outcome.expected_source_bytes);
    }
}

/// A whole rack loses its shards. The rule gave the rack at most one member of
/// each group, so every data and local parity shard it held is still rebuilt
/// inside its group — the other shard lost with the rack is never one of its
/// sources — and nothing needs more than a decode.
#[tokio::test]
async fn a_rack_that_lost_its_shards_is_rebuilt_group_by_group() {
    let set = rule_set();
    for rack in 0..RACKS {
        let lost = set
            .slot_assignment
            .slots()
            .iter()
            .map(|slot| slot.node_id)
            .filter(|node_id| rack_of(*node_id) == rack)
            .collect::<Vec<_>>();
        assert_eq!(lost.len(), 2, "rack r{rack}");

        let outcome = repair_everything_on(&lost).await;

        for ((_, slot), strategy) in &outcome.strategies {
            if is_global_parity(*slot) {
                assert_eq!(
                    *strategy,
                    RepairStrategy::Global,
                    "rack r{rack} slot {slot}"
                );
            } else {
                assert!(
                    matches!(strategy, RepairStrategy::LocalGroup { .. }),
                    "rack r{rack} slot {slot}: {strategy}"
                );
                assert_eq!(
                    outcome.sources_read[&(0, *slot)],
                    3,
                    "rack r{rack} slot {slot}"
                );
            }
        }
    }
}

/// The metadata quorum and the shards of the cluster as a reader sees them.
struct ReadSide<'a> {
    cluster: &'a Cluster,
}

#[async_trait]
impl EcReadMetadataCoordinator for ReadSide<'_> {
    async fn quorum_read_object_layout(
        &self,
        _object: &EcObjectVersionKey,
    ) -> Result<EcObjectLayoutReadResult, EcReadError> {
        Ok(EcObjectLayoutReadResult::Present(
            ObjectDataLayout::ErasureCoded(
                EcObjectManifestRef::from_manifest(&self.cluster.manifest).unwrap(),
            ),
        ))
    }

    async fn quorum_read_manifest(
        &self,
        _object: &EcObjectVersionKey,
    ) -> Result<EcManifestReadResult, EcReadError> {
        Ok(EcManifestReadResult::Present(self.cluster.manifest.clone()))
    }
}

#[async_trait]
impl EcShardReader for ReadSide<'_> {
    async fn read_shard(
        &self,
        request: EcReadShardRequest,
    ) -> Result<Option<EcReadShardResponse>, EcReadError> {
        match self.cluster.answer((request.chunk_index, request.slot_index), request.node_id) {
            Ok(data) => Ok(data.map(|data| EcReadShardResponse {
                replica_crc32: Some(crc32fast::hash(&data)),
                data,
            })),
            Err(reason) => Err(EcReadError::Unavailable { reason }),
        }
    }
}

#[async_trait]
impl EcReplicatedSmallReader for ReadSide<'_> {
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

/// A rack out of reach: every chunk is two shards short, and the object still
/// reads byte for byte — by decoding when the rack held a data shard, straight
/// from the data shards when it held only parity.
///
/// What the read queues for repair follows from the same split. A rack holding
/// a data shard sends the chunk down the fallback, which fetches the whole
/// chunk and so finds both silent shards; a rack holding only parity is never
/// asked for, so the read reports nothing and the scrubber is the side that
/// finds it (decision EC2-181). Nothing outside the silent rack is queued
/// either way.
#[tokio::test]
async fn an_object_reads_whole_while_a_rack_is_out_of_reach() {
    let set = rule_set();
    let (manifest, shards, data) = write_object(&set);
    let (nodes, labels) = pool();
    for rack in 0..RACKS {
        let mut cluster = Cluster::new(&manifest, shards.clone());
        cluster.unreachable =
            nodes.iter().copied().filter(|node_id| rack_of(*node_id) == rack).collect();
        let reader_node = nodes.iter().copied().find(|node_id| rack_of(*node_id) != rack).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap());
        let reader = EcReadCoordinator::new(store, codec_limiter()).with_topology(Arc::new(
            PoolTopologyView::build(reader_node, &nodes, &labels),
        ));
        let side = ReadSide { cluster: &cluster };
        let repairs = RecordingRepairQueue::default();

        let read = reader
            .read(EcReadRequest::full(object()), &side, &side, &side, &repairs)
            .await
            .unwrap_or_else(|error| panic!("rack r{rack}: {error}"));

        assert_eq!(read.body.as_ref(), data.as_slice(), "rack r{rack}");
        let rack_holds_data = set
            .slot_assignment
            .slots()
            .iter()
            .any(|slot| rack_of(slot.node_id) == rack && slot.slot_index < set.profile.k);
        let expected_degraded = if rack_holds_data {
            manifest.chunks.len()
        } else {
            0
        };
        assert_eq!(read.degraded_chunks, expected_degraded, "rack r{rack}");
        let expected_queued = if rack_holds_data {
            2 * manifest.chunks.len()
        } else {
            0
        };
        let queued = repairs.tasks.lock().unwrap();
        assert_eq!(queued.len(), expected_queued, "rack r{rack}");
        assert!(
            queued.iter().all(|task| cluster.unreachable.contains(&task.node_id)),
            "rack r{rack}"
        );
    }
}
