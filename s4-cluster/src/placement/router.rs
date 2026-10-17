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

//! Full request routing: `bucket/key → pool → [replica nodes]`.
//!
//! The [`PlacementRouter`] combines bucket-to-pool mapping with
//! per-pool placement layouts to produce a [`PlacementDecision`]
//! for any S3 operation: the key's replicas and the quorums of its
//! [`ReplicationClass`]. The decision is the one place those quorums come
//! from; nothing downstream reads them from the configuration.

use std::collections::{HashMap, HashSet};

use crate::config::{ClusterConfig, PoolConfig};
use crate::error::ClusterError;
use crate::identity::{NodeId, PoolId};
use crate::placement::bucket_placement::BucketPlacement;
use std::sync::Arc;

use crate::metadata_placement::{Stage, StageTicket};
use crate::placement::class::{
    BucketMetadataMove, ClassLayout, PoolLayout, ReplicationClass, WriteRule,
};
use crate::placement::generation::GenerationPhase;
use crate::placement::hash_ring::PlacementStrategy;

/// The result of routing a `bucket/key` to its replica set.
#[derive(Debug, Clone)]
pub struct PlacementDecision {
    /// Pool that owns this bucket.
    pub pool_id: PoolId,
    /// Minimum placement generation carried by writes.
    pub placement_generation: u64,
    /// Ordered replica list: `[primary, secondary, …]`. The nodes a write
    /// goes to: for bucket metadata moving between two layouts, those of
    /// both while writes reach both.
    pub replicas: Vec<NodeId>,
    /// The nodes a read asks: `replicas`, except for bucket metadata moving
    /// between two layouts, whose reads take one of them.
    pub read_replicas: Vec<NodeId>,
    /// The current node acting as request coordinator.
    pub coordinator: NodeId,
    /// The class of the key, which chose the replicas and the quorums.
    pub class: ReplicationClass,
    /// Replicas of the key's class: the length of `replicas`.
    pub replication_factor: u8,
    /// Acknowledgements a write of the key needs, among the replicas reads
    /// take: how many of them hold a confirmed write. What a write needs to
    /// succeed is [`Self::write_rule`].
    pub write_quorum: u8,
    /// Answers a read of the key needs from `read_replicas`.
    pub read_quorum: u8,
    /// What a write of the key needs to succeed.
    pub write_rule: WriteRule,
    /// The stage a key of moving bucket metadata was placed by, `None` for
    /// any other key. Sent with its writes, so that a replica can refuse the
    /// writes of a stage too old (decision 6).
    pub bucket_metadata_stage: Option<Stage>,
    /// Holds the stage the key was placed by until the decision is dropped:
    /// a change of stage waits for it (task BMR-03-002).
    pub stage_ticket: Option<Arc<StageTicket>>,
    /// Held while a decision of a growing pool reads its key through the
    /// old owners: the common barrier before the pool settles on its
    /// successor waits for every one of them to be dropped.
    pub old_read_ticket: Option<Arc<super::generation::OldReadTicket>>,
}

/// The owners of one class at one ring position in both generations of a
/// growing pool ([`PlacementRouter::generation_owners`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GenerationOwners {
    /// Owners by the current generation.
    pub old: Vec<NodeId>,
    /// Acknowledgements a write needs from the current generation: as many
    /// of its owners hold a confirmed write, or something newer.
    pub old_write_quorum: u8,
    /// Owners by the successor generation.
    pub new: Vec<NodeId>,
}

/// Full two-level router: bucket → pool → replica nodes.
///
/// Built from [`ClusterConfig`] on startup and rebuilt on topology
/// epoch changes. The router is read-only after construction; bucket
/// assignment mutations go through [`BucketPlacement`] and trigger
/// a rebuild.
#[derive(Debug, Clone)]
pub struct PlacementRouter {
    /// Per-pool placement of every replication class.
    layouts: HashMap<PoolId, PoolLayout>,
    generations: HashMap<PoolId, GenerationPlacement>,
    /// Pool configurations indexed by pool ID.
    pool_configs: HashMap<PoolId, PoolConfig>,
    /// Bucket-to-pool mapping.
    bucket_placement: BucketPlacement,
    /// Local node identity (always the coordinator for received requests).
    local_node_id: NodeId,
}

#[derive(Debug, Clone)]
struct GenerationPlacement {
    record: crate::pool_generation::PoolGenerationRecord,
    next: Option<PoolLayout>,
    ranges: Option<Arc<super::generation::GenerationRanges>>,
}

impl PlacementRouter {
    /// Build a router from cluster configuration.
    ///
    /// For each pool, the appropriate [`PlacementStrategy`] is created
    /// based on pool size vs. replication factor, with the pool's quorums.
    ///
    /// # Arguments
    ///
    /// - `config` — cluster topology.
    /// - `local_node_id` — this node's identity (used as coordinator).
    /// - `bucket_placement` — pre-loaded bucket-to-pool mapping.
    /// - `node_id_resolver` — maps node addresses to `NodeId`s. In a
    ///   running cluster this comes from the gossip member table.
    pub fn new(
        config: &ClusterConfig,
        local_node_id: NodeId,
        bucket_placement: BucketPlacement,
        node_id_resolver: &HashMap<std::net::SocketAddr, NodeId>,
    ) -> Result<Self, ClusterError> {
        let mut pool_node_ids = HashMap::new();
        for pool in &config.pools {
            let node_ids: Vec<NodeId> = pool
                .nodes
                .iter()
                .map(|addr| {
                    node_id_resolver.get(&addr.grpc_addr).copied().ok_or_else(|| {
                        ClusterError::Placement(format!(
                            "no NodeId for address {} in pool {}",
                            addr.grpc_addr, pool.pool_id,
                        ))
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            if pool_node_ids.insert(pool.pool_id, node_ids).is_some() {
                return Err(ClusterError::Placement(format!(
                    "duplicate pool ID {}",
                    pool.pool_id
                )));
            }
        }
        Self::from_pool_node_ids(config, local_node_id, bucket_placement, &pool_node_ids)
    }

    /// Build placement from the stable, already resolved identities of each pool.
    ///
    /// Addresses are transport hints, not membership keys: an absent member's
    /// remembered address may now belong to another member. Keeping the named
    /// roster avoids collapsing those two identities into a single map entry.
    /// Each roster must contain one distinct identity per configured member.
    ///
    /// # Errors
    ///
    /// Rejects missing or extra rosters, duplicate pools or identities, a roster
    /// of the wrong size, a replication factor the roster cannot satisfy, and a
    /// quorum the replication factor cannot satisfy.
    pub fn from_pool_node_ids(
        config: &ClusterConfig,
        local_node_id: NodeId,
        bucket_placement: BucketPlacement,
        pool_node_ids: &HashMap<PoolId, Vec<NodeId>>,
    ) -> Result<Self, ClusterError> {
        let mut layouts = HashMap::new();
        let mut pool_configs = HashMap::new();
        for pool in &config.pools {
            if layouts.contains_key(&pool.pool_id) {
                return Err(ClusterError::Placement(format!(
                    "duplicate pool ID {}",
                    pool.pool_id
                )));
            }
            let node_ids = pool_node_ids.get(&pool.pool_id).ok_or_else(|| {
                ClusterError::Placement(format!("no named roster for pool {}", pool.pool_id))
            })?;
            if node_ids.len() != pool.nodes.len() {
                return Err(ClusterError::Placement(format!(
                    "pool {} names {} identities for {} configured members",
                    pool.pool_id,
                    node_ids.len(),
                    pool.nodes.len()
                )));
            }
            let mut unique = HashSet::with_capacity(node_ids.len());
            for id in node_ids {
                if !unique.insert(*id) {
                    return Err(ClusterError::Placement(format!(
                        "duplicate NodeId {id} in pool {}",
                        pool.pool_id
                    )));
                }
            }
            layouts.insert(
                pool.pool_id,
                PoolLayout::for_pool(config, pool, node_ids.clone())?,
            );
            pool_configs.insert(pool.pool_id, pool.clone());
        }
        if let Some(pool_id) = pool_node_ids.keys().find(|id| !pool_configs.contains_key(id)) {
            return Err(ClusterError::Placement(format!(
                "named roster for unconfigured pool {pool_id}"
            )));
        }
        Ok(Self {
            layouts,
            generations: HashMap::new(),
            pool_configs,
            bucket_placement,
            local_node_id,
        })
    }

    /// Build a single-node router (no cluster, single pool).
    ///
    /// Used when the server runs in standalone mode. All buckets map
    /// to pool 0 with the local node as the sole replica, written and read
    /// through it alone.
    pub fn single_node(local_node_id: NodeId) -> Self {
        let pool_id = PoolId(0);
        let data = ClassLayout::single(local_node_id);

        let mut layouts = HashMap::new();
        layouts.insert(pool_id, PoolLayout::uniform(data));

        Self {
            layouts,
            generations: HashMap::new(),
            pool_configs: HashMap::new(),
            bucket_placement: BucketPlacement::new(),
            local_node_id,
        }
    }

    /// Route an existing object: look up the bucket's pool and resolve
    /// the replica set for `bucket/key`.
    pub fn route(&self, bucket: &str, key: &str) -> Result<PlacementDecision, ClusterError> {
        let pool_id = self.resolve_pool(bucket)?;
        self.route_in_pool(pool_id, bucket, key)
    }

    /// Route within a known pool. Used when the pool is already known
    /// (e.g., from `IndexRecord.pool_id`).
    pub fn route_in_pool(
        &self,
        pool_id: PoolId,
        bucket: &str,
        key: &str,
    ) -> Result<PlacementDecision, ClusterError> {
        self.route_at_position(
            pool_id,
            bucket,
            key,
            s4_core::ring_position_of(&format!("{bucket}/{key}")),
        )
    }

    pub(super) fn route_at_position(
        &self,
        pool_id: PoolId,
        bucket: &str,
        key: &str,
        position: u64,
    ) -> Result<PlacementDecision, ClusterError> {
        let layout = self
            .layouts
            .get(&pool_id)
            .ok_or_else(|| ClusterError::Placement(format!("unknown pool: {pool_id}")))?;

        let class = ReplicationClass::of(bucket, key);
        let generation = self.generations.get(&pool_id);
        let phase = generation.and_then(|g| g.ranges.as_ref().map(|ranges| ranges.phase()));
        let placement_generation = generation.map_or(0, |g| match phase {
            Some(GenerationPhase::Moving | GenerationPhase::Finished) => {
                g.record.proposed.as_ref().map_or(g.record.current.epoch, |next| next.epoch)
            }
            _ => g.record.current.epoch,
        });
        let placement_key = format!("{bucket}/{key}");
        let stage = || {
            (class == ReplicationClass::BucketMetadata)
                .then(|| layout.bucket_metadata_move().map(|moving| moving.stage_cell().stage()))
                .flatten()
        };
        let stage_ticket = || {
            if class == ReplicationClass::BucketMetadata {
                layout
                    .bucket_metadata_move()
                    .map(|moving| Arc::new(moving.stage_cell().enter()))
            } else {
                None
            }
        };
        if let (Some(GenerationPhase::Finished), Some(next)) =
            (phase, generation.and_then(|g| g.next.as_ref()))
        {
            // Settled on the successor: it alone places the key.
            let new = next.class(class);
            let replicas = new.strategy().replicas_at(position);
            return Ok(PlacementDecision {
                pool_id,
                placement_generation,
                read_replicas: replicas.clone(),
                write_rule: WriteRule::quorum(&replicas, new.write_quorum()),
                replication_factor: new.replication_factor(),
                replicas,
                coordinator: self.local_node_id,
                class,
                write_quorum: new.write_quorum(),
                read_quorum: new.read_quorum(),
                bucket_metadata_stage: stage(),
                stage_ticket: stage_ticket(),
                old_read_ticket: None,
            });
        }
        if let Some(generation) = generation.filter(|_| phase == Some(GenerationPhase::Moving)) {
            if let (Some(next), Some(ranges)) = (&generation.next, &generation.ranges) {
                let old = layout.class(class);
                let new = next.class(class);
                let old_nodes = old.strategy().replicas_at(position);
                let new_nodes = new.strategy().replicas_at(position);
                let cut_over =
                    ranges.range_at(position).state == super::generation::RangeState::CutOver;
                let old_read_ticket = (!cut_over).then(|| Arc::new(ranges.old_read_ticket()));
                let read = if cut_over { new } else { old };
                let read_replicas = if cut_over {
                    new_nodes.clone()
                } else {
                    old_nodes.clone()
                };
                let mut replicas = old_nodes.clone();
                for node in &new_nodes {
                    if !replicas.contains(node) {
                        replicas.push(*node);
                    }
                }
                return Ok(PlacementDecision {
                    pool_id,
                    placement_generation,
                    replication_factor: u8::try_from(replicas.len()).unwrap_or(u8::MAX),
                    replicas,
                    read_replicas,
                    coordinator: self.local_node_id,
                    class,
                    write_quorum: read.write_quorum(),
                    read_quorum: read.read_quorum(),
                    write_rule: WriteRule::all(vec![
                        super::WriteRequirement {
                            nodes: old_nodes,
                            count: old.write_quorum(),
                        },
                        super::WriteRequirement {
                            nodes: new_nodes,
                            count: new.write_quorum(),
                        },
                    ]),
                    bucket_metadata_stage: stage(),
                    stage_ticket: stage_ticket(),
                    old_read_ticket,
                });
            }
        }
        if let (ReplicationClass::BucketMetadata, Some(moving)) =
            (class, layout.bucket_metadata_move())
        {
            let ticket = moving.stage_cell().enter();
            let placed = moving.place(&placement_key, &ticket);
            return Ok(PlacementDecision {
                pool_id,
                placement_generation,
                replication_factor: u8::try_from(placed.replicas.len()).unwrap_or(u8::MAX),
                replicas: placed.replicas,
                read_replicas: placed.read_replicas,
                coordinator: self.local_node_id,
                class,
                write_quorum: placed.write_quorum,
                read_quorum: placed.read_quorum,
                write_rule: placed.write_rule,
                bucket_metadata_stage: Some(placed.stage),
                stage_ticket: Some(Arc::new(ticket)),
                old_read_ticket: None,
            });
        }
        let class_layout = layout.class(class);
        let replicas = class_layout.strategy().get_replicas(&placement_key);

        Ok(PlacementDecision {
            pool_id,
            placement_generation,
            read_replicas: replicas.clone(),
            write_rule: WriteRule::quorum(&replicas, class_layout.write_quorum()),
            replicas,
            coordinator: self.local_node_id,
            class,
            replication_factor: class_layout.replication_factor(),
            write_quorum: class_layout.write_quorum(),
            read_quorum: class_layout.read_quorum(),
            bucket_metadata_stage: None,
            stage_ticket: None,
            old_read_ticket: None,
        })
    }

    /// Attaches validated durable generation state without beginning or finishing a move.
    ///
    /// # Errors
    /// Invalid record, unknown pool, or a moving generation while bucket metadata is unsettled.
    pub fn with_pool_generation(
        mut self,
        record: &crate::pool_generation::PoolGenerationRecord,
    ) -> Result<Self, ClusterError> {
        record.validate()?;
        let inherited = self
            .layouts
            .get(&record.pool_id)
            .ok_or_else(|| ClusterError::Placement("unknown generation pool".into()))?;
        if record.moving
            && inherited
                .bucket_metadata_move()
                .is_some_and(|moving| moving.stage_cell().stage() != Stage::Settled)
        {
            return Err(ClusterError::Placement(
                "bucket metadata must settle before pool growth".into(),
            ));
        }
        if let Some(existing) = self.generations.get(&record.pool_id) {
            if existing.record == *record {
                return Ok(self);
            }
            if existing.record.current != record.current
                || (existing.record.proposed.is_some()
                    && existing.record.proposed != record.proposed)
            {
                return Err(ClusterError::Placement(
                    "replacing attached generation identities is forbidden".into(),
                ));
            }
            if existing.record.moving && !record.moving {
                return Err(ClusterError::Placement(
                    "pool generation cannot leave moving locally".into(),
                ));
            }
        }
        let mut old = super::generation::layout_for(&record.current.nodes, inherited)?;
        if let Some(moving) = inherited.bucket_metadata_move() {
            old = old.with_bucket_metadata_move(moving.clone());
            if moving.stage_cell().stage() == Stage::Settled {
                old = old.with_bucket_metadata_layout(moving.new_layout().clone());
            }
        }
        let next = record
            .proposed
            .as_ref()
            .map(|next| super::generation::layout_for(&next.nodes, &old))
            .transpose()?;
        let ranges = record
            .proposed
            .as_ref()
            .map(|next| {
                super::generation::GenerationRanges::new(&record.current.nodes, &next.nodes)
                    .map(Arc::new)
            })
            .transpose()?;
        let ranges = self
            .generations
            .get(&record.pool_id)
            .and_then(|existing| existing.ranges.clone())
            .or(ranges);
        if record.moving {
            if let Some(ranges) = &ranges {
                ranges.begin();
            }
        }
        self.layouts.insert(record.pool_id, old);
        self.generations.insert(
            record.pool_id,
            GenerationPlacement {
                record: record.clone(),
                next,
                ranges,
            },
        );
        Ok(self)
    }

    /// Shared local progress of the pool's union-ring arcs, if growth is proposed.
    pub fn generation_ranges(
        &self,
        pool_id: &PoolId,
    ) -> Option<&Arc<super::generation::GenerationRanges>> {
        self.generations.get(pool_id).and_then(|generation| generation.ranges.as_ref())
    }

    /// Whether the pool moves to its successor generation now: writes reach
    /// both, and read repair fills both.
    pub(crate) fn pool_generation_moving(&self, pool_id: &PoolId) -> bool {
        self.generation_phase(pool_id) == Some(GenerationPhase::Moving)
    }

    /// Where the pool stands in its growth on this node; `None` without a
    /// prepared successor.
    pub fn generation_phase(&self, pool_id: &PoolId) -> Option<GenerationPhase> {
        self.generation_ranges(pool_id).map(|ranges| ranges.phase())
    }

    /// The layout of the pool's successor generation, once one is prepared.
    pub fn successor_layout(&self, pool_id: &PoolId) -> Option<&PoolLayout> {
        self.generations.get(pool_id).and_then(|generation| generation.next.as_ref())
    }

    /// The owners of `class` at ring position `position` by the current and
    /// by the successor generation, with the write quorum of the current
    /// one: what a range's verification compares. `None` without a prepared
    /// successor.
    pub fn generation_owners(
        &self,
        pool_id: &PoolId,
        class: ReplicationClass,
        position: u64,
    ) -> Option<GenerationOwners> {
        let old = self.layouts.get(pool_id)?.class(class);
        let new = self.successor_layout(pool_id)?.class(class);
        Some(GenerationOwners {
            old: old.strategy().replicas_at(position),
            old_write_quorum: old.write_quorum(),
            new: new.strategy().replicas_at(position),
        })
    }

    /// This router with the bucket metadata of `pool_id` moving as `moving`
    /// says.
    ///
    /// # Errors
    ///
    /// An unknown pool, or pool growth already moving.
    pub fn with_bucket_metadata_move(
        mut self,
        pool_id: PoolId,
        moving: BucketMetadataMove,
    ) -> Result<Self, ClusterError> {
        if self
            .generations
            .get(&pool_id)
            .is_some_and(|generation| generation.record.moving)
        {
            return Err(ClusterError::Placement(
                "pool growth must finish before bucket metadata moves".into(),
            ));
        }
        let layout = self
            .layouts
            .remove(&pool_id)
            .ok_or_else(|| ClusterError::Placement(format!("unknown pool: {pool_id}")))?;
        self.layouts.insert(pool_id, layout.with_bucket_metadata_move(moving));
        Ok(self)
    }

    /// Resolve which pool owns a bucket.
    ///
    /// In single-node mode, always returns `PoolId(0)`.
    fn resolve_pool(&self, bucket: &str) -> Result<PoolId, ClusterError> {
        // Single-node: only pool 0 exists.
        if self.layouts.len() == 1 && self.layouts.contains_key(&PoolId(0)) {
            return Ok(PoolId(0));
        }

        self.bucket_placement.get(bucket).ok_or_else(|| {
            ClusterError::Placement(format!("bucket '{}' is not assigned to any pool", bucket,))
        })
    }

    /// Assign a new bucket to the best available pool.
    ///
    /// `pool_capacities` maps `(pool_id, used_bytes, total_bytes)`.
    /// Returns the chosen pool ID.
    pub fn assign_bucket(
        &mut self,
        bucket: &str,
        pool_capacities: &[(PoolId, u64, u64)],
    ) -> Result<PoolId, ClusterError> {
        if let Some(existing) = self.bucket_placement.get(bucket) {
            return Ok(existing);
        }

        let pool_id = BucketPlacement::select_pool(pool_capacities).ok_or_else(|| {
            ClusterError::Placement("no pools available for bucket assignment".into())
        })?;

        if !self.layouts.contains_key(&pool_id) {
            return Err(ClusterError::Placement(format!(
                "selected pool {pool_id} has no placement strategy"
            )));
        }

        self.bucket_placement.assign(bucket, pool_id)?;
        Ok(pool_id)
    }

    /// Remove a bucket assignment.
    pub fn remove_bucket(&mut self, bucket: &str) -> Option<PoolId> {
        self.bucket_placement.remove(bucket)
    }

    /// Access the underlying bucket placement (read-only).
    pub fn bucket_placement(&self) -> &BucketPlacement {
        &self.bucket_placement
    }

    /// Access the placement strategy of a pool's data.
    pub fn strategy(&self, pool_id: &PoolId) -> Option<&PlacementStrategy> {
        self.layouts.get(pool_id).map(|layout| layout.data().strategy())
    }

    /// Access the placement of every replication class of a pool.
    pub fn layout(&self, pool_id: &PoolId) -> Option<&PoolLayout> {
        self.layouts.get(pool_id)
    }

    /// Local node ID (coordinator for received requests).
    pub fn local_node_id(&self) -> NodeId {
        self.local_node_id
    }

    /// All known pool IDs.
    pub fn pool_ids(&self) -> Vec<PoolId> {
        self.layouts.keys().copied().collect()
    }

    /// Access a pool configuration by ID.
    pub fn pool_config(&self, pool_id: &PoolId) -> Option<&PoolConfig> {
        self.pool_configs.get(pool_id)
    }

    /// Return all physical nodes in the pool that owns the given bucket:
    /// once a growth has begun, every node of the grown pool, which holds
    /// the old one.
    pub fn pool_nodes(&self, bucket: &str) -> Result<Vec<NodeId>, ClusterError> {
        let pool_id = self.resolve_pool(bucket)?;
        if matches!(
            self.generation_phase(&pool_id),
            Some(GenerationPhase::Moving | GenerationPhase::Finished)
        ) {
            if let Some(next) = self.successor_layout(&pool_id) {
                return Ok(next.all_nodes().to_vec());
            }
        }
        let layout = self
            .layouts
            .get(&pool_id)
            .ok_or_else(|| ClusterError::Placement(format!("unknown pool: {pool_id}")))?;
        Ok(layout.all_nodes().to_vec())
    }

    /// Check whether the local node is a replica for the given key.
    pub fn is_local_replica(&self, bucket: &str, key: &str) -> Result<bool, ClusterError> {
        let decision = self.route(bucket, key)?;
        Ok(decision.replicas.contains(&self.local_node_id))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ClusterConfig;
    use crate::identity::NodeAddr;
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

    fn addr(port: u16) -> SocketAddr {
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port))
    }

    fn node_addr(port: u16) -> NodeAddr {
        NodeAddr {
            grpc_addr: addr(port),
            http_addr: addr(port + 1000),
            gossip_addr: addr(port + 2000),
        }
    }

    fn setup_three_node_cluster() -> (PlacementRouter, [NodeId; 3]) {
        let nodes = [NodeId(1), NodeId(2), NodeId(3)];
        let ports = [9100, 9101, 9102];

        let mut resolver = HashMap::new();
        for (i, &node) in nodes.iter().enumerate() {
            resolver.insert(addr(ports[i]), node);
        }

        let pool = PoolConfig {
            pool_id: PoolId(1),
            name: "pool-1".into(),
            nodes: ports.iter().map(|&p| node_addr(p)).collect(),
            replication_factor: None,
            write_quorum: None,
            read_quorum: None,
            storage_class: s4_core::types::StorageClass::Standard,
        };

        let config = ClusterConfig {
            pools: vec![pool],
            ..ClusterConfig::default()
        };

        let mut bp = BucketPlacement::new();
        bp.assign("test-bucket", PoolId(1)).unwrap();

        let router = PlacementRouter::new(&config, NodeId(1), bp, &resolver).unwrap();
        (router, nodes)
    }

    // -- Single-node mode -------------------------------------------------

    #[test]
    fn duplicate_node_identities_are_rejected_before_building_the_ring() {
        let (router, _) = setup_three_node_cluster();
        let config = ClusterConfig {
            pools: vec![router.pool_config(&PoolId(1)).unwrap().clone()],
            ..ClusterConfig::default()
        };
        let resolver = HashMap::from([
            (addr(9100), NodeId(1)),
            (addr(9101), NodeId(1)),
            (addr(9102), NodeId(3)),
        ]);

        let result = PlacementRouter::new(&config, NodeId(1), BucketPlacement::new(), &resolver);

        assert!(
            result.is_err(),
            "RF=3 must not be built from two distinct identities"
        );
    }

    #[test]
    fn duplicate_pool_ids_are_not_silently_overwritten() {
        let (router, _) = setup_three_node_cluster();
        let pool = router.pool_config(&PoolId(1)).unwrap().clone();
        let config = ClusterConfig {
            pools: vec![pool.clone(), pool],
            ..ClusterConfig::default()
        };
        let resolver = HashMap::from([
            (addr(9100), NodeId(1)),
            (addr(9101), NodeId(2)),
            (addr(9102), NodeId(3)),
        ]);

        let result = PlacementRouter::new(&config, NodeId(1), BucketPlacement::new(), &resolver);

        assert!(
            result.is_err(),
            "a repeated pool must not replace an earlier strategy"
        );
    }

    #[test]
    fn named_rosters_reject_missing_extra_short_and_duplicate_members() {
        let (router, nodes) = setup_three_node_cluster();
        let config = ClusterConfig {
            pools: vec![router.pool_config(&PoolId(1)).unwrap().clone()],
            ..ClusterConfig::default()
        };
        for roster in [
            HashMap::new(),
            HashMap::from([(PoolId(1), nodes.to_vec()), (PoolId(2), nodes.to_vec())]),
            HashMap::from([(PoolId(1), nodes[..2].to_vec())]),
            HashMap::from([(PoolId(1), vec![nodes[0], nodes[0], nodes[2]])]),
        ] {
            assert!(PlacementRouter::from_pool_node_ids(
                &config,
                nodes[0],
                BucketPlacement::new(),
                &roster
            )
            .is_err());
        }
        let mut impossible = config.clone();
        impossible.pools[0].replication_factor = Some(4);
        assert!(PlacementRouter::from_pool_node_ids(
            &impossible,
            nodes[0],
            BucketPlacement::new(),
            &HashMap::from([(PoolId(1), nodes.to_vec())])
        )
        .is_err());
    }

    #[test]
    fn single_node_routes_to_self() {
        let node = NodeId(42);
        let router = PlacementRouter::single_node(node);
        let decision = router.route("any-bucket", "any-key").unwrap();
        assert_eq!(decision.pool_id, PoolId(0));
        assert_eq!(decision.replicas, vec![node]);
        assert_eq!(decision.coordinator, node);
    }

    // -- Three-node cluster -----------------------------------------------

    #[test]
    fn three_node_full_replication() {
        let (router, nodes) = setup_three_node_cluster();
        let decision = router.route("test-bucket", "hello.txt").unwrap();
        assert_eq!(decision.pool_id, PoolId(1));
        assert_eq!(decision.replicas.len(), 3);
        for node in &nodes {
            assert!(decision.replicas.contains(node));
        }
    }

    #[test]
    fn unassigned_bucket_rejected() {
        let (router, _) = setup_three_node_cluster();
        let err = router.route("unknown-bucket", "key").unwrap_err();
        assert!(err.to_string().contains("not assigned"));
    }

    #[test]
    fn coordinator_is_local_node() {
        let (router, _) = setup_three_node_cluster();
        let decision = router.route("test-bucket", "key").unwrap();
        assert_eq!(decision.coordinator, NodeId(1));
    }

    // -- Bucket assignment ------------------------------------------------

    #[test]
    fn assign_bucket_picks_pool() {
        let (mut router, _) = setup_three_node_cluster();
        let capacities = vec![(PoolId(1), 100, 1000)];
        let pool = router.assign_bucket("new-bucket", &capacities).unwrap();
        assert_eq!(pool, PoolId(1));

        // Now routing works.
        let decision = router.route("new-bucket", "file.txt").unwrap();
        assert_eq!(decision.pool_id, PoolId(1));
    }

    #[test]
    fn assign_bucket_idempotent() {
        let (mut router, _) = setup_three_node_cluster();
        let caps = vec![(PoolId(1), 0, 1000)];
        let p1 = router.assign_bucket("b", &caps).unwrap();
        let p2 = router.assign_bucket("b", &caps).unwrap();
        assert_eq!(p1, p2);
    }

    #[test]
    fn assign_bucket_no_pools_fails() {
        let (mut router, _) = setup_three_node_cluster();
        let err = router.assign_bucket("b", &[]).unwrap_err();
        assert!(err.to_string().contains("no pools available"));
    }

    // -- Remove bucket ----------------------------------------------------

    #[test]
    fn remove_bucket() {
        let (mut router, _) = setup_three_node_cluster();
        assert_eq!(router.remove_bucket("test-bucket"), Some(PoolId(1)));
        assert!(router.route("test-bucket", "key").is_err());
    }

    // -- is_local_replica -------------------------------------------------

    #[test]
    fn is_local_replica_in_full_replication() {
        let (router, _) = setup_three_node_cluster();
        assert!(router.is_local_replica("test-bucket", "key").unwrap());
    }

    // -- Replication classes ----------------------------------------------

    /// A pool of six nodes, RF=3, with the quorums given.
    fn six_node_router(write_quorum: u8, read_quorum: u8) -> PlacementRouter {
        let ports: Vec<u16> = (9200..9206).collect();
        let resolver: HashMap<SocketAddr, NodeId> = ports
            .iter()
            .enumerate()
            .map(|(i, &p)| (addr(p), NodeId(i as u128 + 1)))
            .collect();
        let pool = PoolConfig {
            pool_id: PoolId(1),
            name: "pool-1".into(),
            nodes: ports.iter().map(|&p| node_addr(p)).collect(),
            replication_factor: Some(3),
            write_quorum: Some(write_quorum),
            read_quorum: Some(read_quorum),
            storage_class: s4_core::types::StorageClass::Standard,
        };
        let config = ClusterConfig {
            pools: vec![pool],
            ..ClusterConfig::default()
        };
        let mut bp = BucketPlacement::new();
        bp.assign("photos", PoolId(1)).unwrap();
        bp.assign("__system__", PoolId(1)).unwrap();
        PlacementRouter::new(&config, NodeId(1), bp, &resolver).unwrap()
    }

    /// An IAM record of version 2 is routed to every node of a pool wider
    /// than the data's replication factor, written and read through a
    /// majority of the six; its read replicas are the same nodes.
    #[test]
    fn an_iam_record_is_routed_to_every_node_through_a_majority() {
        let router = six_node_router(2, 2);
        let decision = router.route("__system__", "__s4_iam_v2_user_alice").unwrap();
        assert_eq!(decision.class, ReplicationClass::ClusterControl);
        let all: Vec<NodeId> = (1..=6).map(NodeId).collect();
        assert_eq!(decision.replicas, all);
        assert_eq!(decision.read_replicas, all);
        assert_eq!(
            (
                decision.replication_factor,
                decision.write_quorum,
                decision.read_quorum
            ),
            (6, 4, 4)
        );
        assert!(!decision.write_rule.met(&all[..3]));
        assert!(decision.write_rule.met(&all[2..]));
        assert!(decision.bucket_metadata_stage.is_none());
    }

    /// The decision carries the class of the key, its replicas and the
    /// quorums of the class; until the bucket metadata layout is derived,
    /// both classes are placed like the pool's data.
    #[test]
    fn the_decision_carries_the_class_and_its_quorums() {
        let router = six_node_router(2, 2);
        let marker = router.route("__system__", "__s4_bucket_marker_photos").unwrap();
        assert_eq!(marker.class, ReplicationClass::BucketMetadata);
        let cors = router.route("__system__", "__s4_bucket_cors_photos").unwrap();
        assert_eq!(cors.class, ReplicationClass::BucketMetadata);
        let object = router.route("photos", "cat.jpg").unwrap();
        assert_eq!(object.class, ReplicationClass::Data);
        for decision in [&marker, &cors, &object] {
            assert_eq!(decision.replicas.len(), 3);
            assert_eq!(
                (
                    decision.replication_factor,
                    decision.write_quorum,
                    decision.read_quorum
                ),
                (3, 2, 2)
            );
        }
        let ring = router.strategy(&PoolId(1)).unwrap();
        assert_eq!(
            marker.replicas,
            ring.get_replicas("__system__/__s4_bucket_marker_photos")
        );
    }

    /// The quorums are the pool's own, not the cluster's defaults: a pool that
    /// sets them is held to them.
    #[test]
    fn the_quorums_of_a_decision_are_the_pools() {
        let router = six_node_router(3, 1);
        let decision = router.route("photos", "cat.jpg").unwrap();
        assert_eq!((decision.write_quorum, decision.read_quorum), (3, 1));
        assert_ne!(
            (decision.write_quorum, decision.read_quorum),
            (
                ClusterConfig::default().default_write_quorum,
                ClusterConfig::default().default_read_quorum
            )
        );
    }

    /// A quorum larger than the pool's replication factor can never be met,
    /// and the router refuses to place by it.
    #[test]
    fn a_quorum_above_the_replication_factor_is_refused() {
        let (router, _) = setup_three_node_cluster();
        let mut pool = router.pool_config(&PoolId(1)).unwrap().clone();
        pool.write_quorum = Some(4);
        let config = ClusterConfig {
            pools: vec![pool],
            ..ClusterConfig::default()
        };
        let resolver = HashMap::from([
            (addr(9100), NodeId(1)),
            (addr(9101), NodeId(2)),
            (addr(9102), NodeId(3)),
        ]);
        assert!(
            PlacementRouter::new(&config, NodeId(1), BucketPlacement::new(), &resolver).is_err()
        );
    }

    /// In mode single the one node is the only replica and the quorum of one.
    #[test]
    fn a_single_node_decides_with_quorums_of_one() {
        let router = PlacementRouter::single_node(NodeId(7));
        for (bucket, key) in [
            ("photos", "cat.jpg"),
            ("__system__", "__s4_bucket_marker_photos"),
        ] {
            let decision = router.route(bucket, key).unwrap();
            assert_eq!(decision.replicas, vec![NodeId(7)]);
            assert_eq!(
                (
                    decision.replication_factor,
                    decision.write_quorum,
                    decision.read_quorum
                ),
                (1, 1, 1)
            );
        }
    }

    #[test]
    fn moving_generation_keeps_joint_writes_after_local_cutover() {
        let (router, nodes) = setup_three_node_cluster();
        let stable =
            crate::pool_generation::PoolGenerationRecord::initial(PoolId(1), nodes.to_vec())
                .unwrap();
        let prepared = stable.configure((1..=6).map(NodeId).collect()).unwrap();
        let before = router.clone().with_pool_generation(&prepared).unwrap();
        assert_eq!(before.route("test-bucket", "x").unwrap().replicas, nodes);
        let moving = router.with_pool_generation(&prepared.begin().unwrap()).unwrap();
        let decision = moving.route("test-bucket", "x").unwrap();
        assert_eq!(decision.placement_generation, 2);
        assert_eq!(decision.write_rule.requirements().len(), 2);
        assert_eq!(decision.read_replicas, nodes);
        let ranges = moving.generation_ranges(&PoolId(1)).unwrap();
        let position = s4_core::ring_position_of("test-bucket/x");
        let range = ranges.range_at(position);
        use crate::placement::generation::RangeState;
        assert!(ranges.transition(range.upper_inclusive, RangeState::CutOver).is_err());
        for state in [
            RangeState::Streaming,
            RangeState::Verified,
            RangeState::CutOver,
        ] {
            ranges.transition(range.upper_inclusive, state).unwrap();
        }
        let after = moving.route("test-bucket", "x").unwrap();
        assert_eq!(after.write_rule, decision.write_rule);
        assert_eq!(
            after.read_replicas,
            after.write_rule.requirements()[1].nodes
        );
    }

    /// While the pool moves, a decision that reads an arc through the old
    /// owners holds a ticket the barrier waits for; once the pool settled,
    /// the successor alone places every key, writes need its quorum alone,
    /// and the pool's nodes are the grown roster.
    #[test]
    fn a_settled_pool_is_placed_by_its_successor_alone() {
        use crate::placement::generation::RangeState;
        let (router, nodes) = setup_three_node_cluster();
        let moving =
            crate::pool_generation::PoolGenerationRecord::initial(PoolId(1), nodes.to_vec())
                .unwrap()
                .configure((1..=6).map(NodeId).collect())
                .unwrap()
                .begin()
                .unwrap();
        let router = router.with_pool_generation(&moving).unwrap();
        let ranges = router.generation_ranges(&PoolId(1)).unwrap().clone();
        assert_eq!(router.pool_nodes("test-bucket").unwrap().len(), 6);
        let reading_old = router.route("test-bucket", "x").unwrap();
        assert!(reading_old.old_read_ticket.is_some());
        assert_eq!(ranges.old_reads_in_flight(), 1);
        drop(reading_old);
        assert_eq!(ranges.old_reads_in_flight(), 0);
        for range in ranges.snapshot() {
            ranges.advance_to(range.upper_inclusive, RangeState::CutOver).unwrap();
        }
        let cut = router.route("test-bucket", "x").unwrap();
        assert!(cut.old_read_ticket.is_none());
        assert_eq!(cut.write_rule.requirements().len(), 2);
        ranges.finish(crate::clock::hlc::HlcTimestamp::from_record(1, 0, 1)).unwrap();
        let settled = router.route("test-bucket", "x").unwrap();
        assert_eq!(settled.placement_generation, 2);
        assert_eq!(settled.write_rule.requirements().len(), 1);
        assert_eq!(settled.replicas, cut.write_rule.requirements()[1].nodes);
        assert_eq!(settled.read_replicas, settled.replicas);
        assert_eq!((settled.write_quorum, settled.read_quorum), (2, 2));
        assert!(!router.pool_generation_moving(&PoolId(1)));
        assert_eq!(
            router.generation_phase(&PoolId(1)),
            Some(GenerationPhase::Finished)
        );
        let owners = router.generation_owners(&PoolId(1), ReplicationClass::Data, 0).unwrap();
        assert_eq!(
            (owners.old.len(), owners.new.len(), owners.old_write_quorum),
            (3, 3, 2)
        );
    }

    #[test]
    fn generation_attachment_preserves_shared_progress_and_refuses_rollback() {
        let (router, nodes) = setup_three_node_cluster();
        let stable =
            crate::pool_generation::PoolGenerationRecord::initial(PoolId(1), nodes.to_vec())
                .unwrap();
        let prepared = stable.configure((1..=6).map(NodeId).collect()).unwrap();
        let prepared_router = router.with_pool_generation(&prepared).unwrap();
        let ranges = prepared_router.generation_ranges(&PoolId(1)).unwrap().clone();
        let moving = prepared.begin().unwrap();
        let router = prepared_router.with_pool_generation(&moving).unwrap();
        assert!(Arc::ptr_eq(
            &ranges,
            router.generation_ranges(&PoolId(1)).unwrap()
        ));
        let router = router.with_pool_generation(&moving).unwrap();
        assert!(Arc::ptr_eq(
            &ranges,
            router.generation_ranges(&PoolId(1)).unwrap()
        ));
        assert!(router.with_pool_generation(&prepared).is_err());
    }

    #[test]
    fn growth_preserves_settled_bucket_metadata_shape_and_fencing_ticket() {
        let (router, nodes) = setup_three_node_cluster();
        let legacy = ClassLayout::majority_of(nodes.to_vec());
        let settled =
            ClassLayout::new(PlacementStrategy::new(nodes.to_vec(), 2).unwrap(), 2, 1).unwrap();
        let stable =
            crate::pool_generation::PoolGenerationRecord::initial(PoolId(1), nodes.to_vec())
                .unwrap();
        let moving = stable.configure((1..=6).map(NodeId).collect()).unwrap().begin().unwrap();
        let metadata = BucketMetadataMove::new(
            legacy.clone(),
            settled.clone(),
            crate::metadata_placement::StageCell::new(Stage::Settled),
        );
        let router = router
            .with_bucket_metadata_move(PoolId(1), metadata)
            .unwrap()
            .with_pool_generation(&moving)
            .unwrap();
        let decision = router
            .route_in_pool(PoolId(1), "__system__", "__s4_bucket_marker_test-bucket")
            .unwrap();
        assert_eq!(decision.read_replicas.len(), 2);
        assert_eq!(decision.read_quorum, 1);
        assert!(decision
            .write_rule
            .requirements()
            .iter()
            .all(|rule| rule.count == 2 && rule.nodes.len() == 2));
        assert_eq!(decision.bucket_metadata_stage, Some(Stage::Settled));
        assert_eq!(
            decision.stage_ticket.as_ref().unwrap().stage(),
            Stage::Settled
        );
        let (router, _) = setup_three_node_cluster();
        let unsettled = BucketMetadataMove::new(
            legacy,
            settled,
            crate::metadata_placement::StageCell::new(Stage::DualWrite),
        );
        assert!(router
            .with_bucket_metadata_move(PoolId(1), unsettled)
            .unwrap()
            .with_pool_generation(&moving)
            .is_err());
    }

    #[test]
    fn stable_attachment_can_prepare_one_successor() {
        let (router, nodes) = setup_three_node_cluster();
        let stable =
            crate::pool_generation::PoolGenerationRecord::initial(PoolId(1), nodes.to_vec())
                .unwrap();
        let prepared = stable.configure((1..=6).map(NodeId).collect()).unwrap();
        let router = router.with_pool_generation(&stable).unwrap();
        assert!(router.generation_ranges(&PoolId(1)).is_none());
        let router = router.with_pool_generation(&prepared).unwrap();
        assert!(router.generation_ranges(&PoolId(1)).is_some());
        assert_eq!(
            router.route("test-bucket", "x").unwrap().placement_generation,
            1
        );
        assert!(router.clone().with_pool_generation(&stable).is_err());
        let replacement = stable.configure((1..=9).map(NodeId).collect()).unwrap();
        assert!(router.with_pool_generation(&replacement).is_err());
    }

    #[test]
    fn stable_attachment_and_preparation_preserve_unsettled_bucket_metadata_in_either_order() {
        let (router, nodes) = setup_three_node_cluster();
        let stable =
            crate::pool_generation::PoolGenerationRecord::initial(PoolId(1), nodes.to_vec())
                .unwrap();
        let legacy = ClassLayout::majority_of(nodes.to_vec());
        let next =
            ClassLayout::new(PlacementStrategy::new(nodes.to_vec(), 2).unwrap(), 2, 1).unwrap();
        for stage in [Stage::Legacy, Stage::DualWrite, Stage::ReadNew] {
            let moving = BucketMetadataMove::new(
                legacy.clone(),
                next.clone(),
                crate::metadata_placement::StageCell::new(stage),
            );
            let before =
                router.clone().with_bucket_metadata_move(PoolId(1), moving.clone()).unwrap();
            let expected = before
                .route_in_pool(PoolId(1), "__system__", "__s4_bucket_marker_test-bucket")
                .unwrap();
            let prepared = stable.configure((1..=6).map(NodeId).collect()).unwrap();
            for record in [&stable, &prepared] {
                let attached = before.clone().with_pool_generation(record).unwrap();
                let opposite = router
                    .clone()
                    .with_pool_generation(record)
                    .unwrap()
                    .with_bucket_metadata_move(PoolId(1), moving.clone())
                    .unwrap();
                for actual in [attached, opposite] {
                    let actual = actual
                        .route_in_pool(PoolId(1), "__system__", "__s4_bucket_marker_test-bucket")
                        .unwrap();
                    assert_eq!(actual.bucket_metadata_stage, Some(stage));
                    assert_eq!(actual.write_rule, expected.write_rule);
                    assert_eq!(actual.read_replicas, expected.read_replicas);
                    assert_eq!(actual.placement_generation, 1);
                }
            }
        }
    }

    // -- Pool IDs ---------------------------------------------------------

    #[test]
    fn pool_ids_returns_all() {
        let (router, _) = setup_three_node_cluster();
        assert_eq!(router.pool_ids(), vec![PoolId(1)]);
    }
}
