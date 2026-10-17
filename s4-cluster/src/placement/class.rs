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

//! Replication classes: how many replicas a key has and which quorums its
//! reads and writes need (plan BUCKET-METADATA-REPLICATION.md, decision 1).
//!
//! A pool places every key on one ring, but not every key is placed alike. A
//! bucket's marker decides whether the bucket exists and its configurations
//! how it behaves, so they must survive as many node failures as the pool
//! does, while an object's replicas are the pool's replication factor. The
//! IAM records are read on every request and change rarely, so every node of
//! the pool holds them (plan CLUSTER-IAM-REPLICATION.md, decision 1). The
//! class of a key is computed here and nowhere else
//! ([`ReplicationClass::of`]), and the replicas and quorums of the class come
//! from the pool's [`PoolLayout`]: the coordinators, read repair, hinted
//! handoff and tombstone GC all take them from there.

use std::sync::Arc;

use s4_core::types::{
    is_bucket_metadata, is_bucket_metadata_full_key, is_cluster_control,
    is_cluster_control_full_key,
};

use crate::config::{ClusterConfig, PoolConfig};
use crate::error::ClusterError;
use crate::hlc::HlcTimestamp;
use crate::identity::NodeId;
use crate::metadata_placement::{QuorumShape, ReplicaSide, Stage, StageCell, StageTicket};
use crate::placement::hash_ring::PlacementStrategy;

/// The family of keys a key belongs to, for placement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReplicationClass {
    /// Objects and every other key: the pool's replication factor and quorums.
    Data,
    /// A bucket's marker (`__system__/__s4_bucket_marker_*`), while which the
    /// bucket exists, and its six configurations (`__system__/__s4_bucket_
    /// {cors,versioning,lifecycle,policy,object_lock,encryption}_*`).
    BucketMetadata,
    /// The cluster's control state, the IAM records of version 2
    /// (`__system__/__s4_iam_v2_*`): on every node of the pool, written and
    /// read through a majority of them.
    ClusterControl,
}

impl ReplicationClass {
    /// The class of `bucket/key`.
    pub fn of(bucket: &str, key: &str) -> Self {
        if is_bucket_metadata(bucket, key) {
            Self::BucketMetadata
        } else if is_cluster_control(bucket, key) {
            Self::ClusterControl
        } else {
            Self::Data
        }
    }

    /// The class of a key spelt `bucket/key` in one string, as the version
    /// digests of the storage name it.
    pub fn of_full_key(full_key: &str) -> Self {
        if is_bucket_metadata_full_key(full_key) {
            Self::BucketMetadata
        } else if is_cluster_control_full_key(full_key) {
            Self::ClusterControl
        } else {
            Self::Data
        }
    }

    /// The name of the class in logs and metrics.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Data => "data",
            Self::BucketMetadata => "bucket_metadata",
            Self::ClusterControl => "cluster_control",
        }
    }
}

/// How the keys of one class are placed in a pool: the ring that names their
/// replicas, and the quorums their writes and reads need.
#[derive(Debug, Clone)]
pub struct ClassLayout {
    strategy: PlacementStrategy,
    write_quorum: u8,
    read_quorum: u8,
}

impl ClassLayout {
    /// Keys placed by `strategy`, written through `write_quorum` replicas and
    /// read through `read_quorum`.
    ///
    /// # Errors
    ///
    /// A quorum of zero, or one larger than the replication factor of the
    /// strategy: no write or read could ever reach it.
    pub fn new(
        strategy: PlacementStrategy,
        write_quorum: u8,
        read_quorum: u8,
    ) -> Result<Self, ClusterError> {
        let rf = strategy.replication_factor();
        for (name, quorum) in [("write", write_quorum), ("read", read_quorum)] {
            if quorum == 0 || quorum > rf {
                return Err(ClusterError::Placement(format!(
                    "{name} quorum {quorum} is outside 1..={rf}, the replication factor"
                )));
            }
        }
        Ok(Self {
            strategy,
            write_quorum,
            read_quorum,
        })
    }

    /// Every node of `nodes` a replica of every key, written and read through
    /// a majority of them, `⌊N/2⌋ + 1`: the cluster's control state, which
    /// every node reads locally and a majority confirms. Two majorities always
    /// meet, so a read of the majority sees every confirmed write.
    pub fn majority_of(mut nodes: Vec<NodeId>) -> Self {
        nodes.sort_by_key(|node| node.0);
        nodes.dedup();
        let majority = u8::try_from(nodes.len() / 2 + 1).unwrap_or(u8::MAX);
        Self {
            strategy: PlacementStrategy::FullReplication { nodes },
            write_quorum: majority,
            read_quorum: majority,
        }
    }

    /// One node that is the only replica of every key, written and read
    /// through it alone: a server in mode single.
    pub fn single(node: NodeId) -> Self {
        Self {
            strategy: PlacementStrategy::FullReplication { nodes: vec![node] },
            write_quorum: 1,
            read_quorum: 1,
        }
    }

    /// The ring that names the replicas of the class.
    pub fn strategy(&self) -> &PlacementStrategy {
        &self.strategy
    }

    /// Replicas of every key of the class.
    pub fn replication_factor(&self) -> u8 {
        self.strategy.replication_factor()
    }

    /// Acknowledgements a write of the class needs.
    pub fn write_quorum(&self) -> u8 {
        self.write_quorum
    }

    /// Answers a read of the class needs.
    pub fn read_quorum(&self) -> u8 {
        self.read_quorum
    }

    /// The replicas and quorums of the class, without the ring.
    pub fn shape(&self) -> QuorumShape {
        QuorumShape {
            replication_factor: self.replication_factor(),
            write_quorum: self.write_quorum,
            read_quorum: self.read_quorum,
        }
    }
}

/// So many acknowledgements from these nodes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteRequirement {
    /// The nodes whose acknowledgements count.
    pub nodes: Vec<NodeId>,
    /// How many of them a write needs.
    pub count: u8,
}

/// What a write of one key needs to succeed: every requirement met by the
/// replicas that acknowledged it. A key of one layout has one requirement,
/// its write quorum among its replicas; a bucket metadata key moving between
/// two layouts has one per layout while writes reach both.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteRule {
    requirements: Vec<WriteRequirement>,
}

impl WriteRule {
    /// `count` acknowledgements from `nodes`.
    pub fn quorum(nodes: &[NodeId], count: u8) -> Self {
        Self {
            requirements: vec![WriteRequirement {
                nodes: nodes.to_vec(),
                count,
            }],
        }
    }

    /// Every one of `requirements`.
    pub fn all(requirements: Vec<WriteRequirement>) -> Self {
        Self { requirements }
    }

    /// The requirements, in order.
    pub fn requirements(&self) -> &[WriteRequirement] {
        &self.requirements
    }

    /// The first requirement `acked` does not meet: the acknowledgements it
    /// needs, and how many of its nodes acknowledged.
    pub fn unmet(&self, acked: &[NodeId]) -> Option<(u8, u8)> {
        self.requirements.iter().find_map(|requirement| {
            let achieved = requirement.nodes.iter().filter(|node| acked.contains(node)).count();
            let achieved = u8::try_from(achieved).unwrap_or(u8::MAX);
            (achieved < requirement.count).then_some((requirement.count, achieved))
        })
    }

    /// Whether the nodes in `acked` meet every requirement.
    pub fn met(&self, acked: &[NodeId]) -> bool {
        self.unmet(acked).is_none()
    }
}

/// Where a key of a pool's bucket metadata is placed while the pool moves it
/// from an old layout to a new one (plan BUCKET-METADATA-REPLICATION.md,
/// decision 4): its replicas and rules at this node's stage.
#[derive(Debug, Clone)]
pub struct StagedPlacement {
    /// The nodes a write goes to.
    pub replicas: Vec<NodeId>,
    /// The nodes a read asks.
    pub read_replicas: Vec<NodeId>,
    /// What a write needs.
    pub write_rule: WriteRule,
    /// Answers a read needs from `read_replicas`.
    pub read_quorum: u8,
    /// The write quorum of the layout reads take: how many replicas a
    /// confirmed write leaves among them.
    pub write_quorum: u8,
    /// The stage the key was placed by.
    pub stage: Stage,
}

/// The move of a pool's bucket metadata from `old` to `new`, at the stage in
/// `stage`.
#[derive(Debug, Clone)]
pub struct BucketMetadataMove {
    old: ClassLayout,
    new: ClassLayout,
    stage: StageCell,
}

impl BucketMetadataMove {
    /// Bucket metadata moving from `old` to `new`, at the stage `stage` holds.
    pub fn new(old: ClassLayout, new: ClassLayout, stage: StageCell) -> Self {
        Self { old, new, stage }
    }

    /// The layout moved from.
    pub fn old(&self) -> &ClassLayout {
        &self.old
    }

    /// The layout moved to.
    pub fn new_layout(&self) -> &ClassLayout {
        &self.new
    }

    /// This node's stage.
    pub fn stage_cell(&self) -> &StageCell {
        &self.stage
    }

    /// The nodes that hold a key at `stage`, given its replicas in each
    /// layout: the old ones at stage 0, both at stages 1 and 2 — the old ones
    /// first — and the new ones at stage 3.
    fn holders(stage: Stage, old: Vec<NodeId>, new: Vec<NodeId>) -> Vec<NodeId> {
        match stage {
            Stage::Legacy => old,
            Stage::Settled => new,
            Stage::DualWrite | Stage::ReadNew => {
                let mut both = old;
                for node in new {
                    if !both.contains(&node) {
                        both.push(node);
                    }
                }
                both
            }
        }
    }

    /// The placement of the key at `placement_key` at the stage `ticket`
    /// holds, by the rules of [`Stage::rules`].
    pub fn place(&self, placement_key: &str, ticket: &StageTicket) -> StagedPlacement {
        let stage = ticket.stage();
        let old = self.old.strategy.get_replicas(placement_key);
        let new = self.new.strategy.get_replicas(placement_key);
        let rules = stage.rules(self.old.shape(), self.new.shape());
        let side = |side: ReplicaSide| match side {
            ReplicaSide::Old => &old,
            ReplicaSide::New => &new,
        };
        let write_rule = WriteRule::all(
            rules
                .write
                .iter()
                .map(|requirement| WriteRequirement {
                    nodes: side(requirement.side).clone(),
                    count: requirement.count,
                })
                .collect(),
        );
        let read_replicas = side(rules.read.side).clone();
        let write_quorum = match rules.read.side {
            ReplicaSide::Old => self.old.write_quorum,
            ReplicaSide::New => self.new.write_quorum,
        };
        StagedPlacement {
            replicas: Self::holders(stage, old.clone(), new.clone()),
            read_replicas,
            write_rule,
            read_quorum: rules.read.count,
            write_quorum,
            stage,
        }
    }

    /// The nodes that hold the key at ring `position` at this node's stage.
    fn holders_at(&self, position: u64) -> Vec<NodeId> {
        Self::holders(
            self.stage.stage(),
            self.old.strategy.replicas_at(position),
            self.new.strategy.replicas_at(position),
        )
    }

    /// The nodes that hold the key `full_key` at this node's stage.
    fn holders_of(&self, full_key: &str) -> Vec<NodeId> {
        Self::holders(
            self.stage.stage(),
            self.old.strategy.get_replicas(full_key),
            self.new.strategy.get_replicas(full_key),
        )
    }
}

/// How long a tombstone stays beyond what anti-entropy proves of every key
/// ([`PoolLayout::tombstone_keep`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TombstoneKeep {
    /// It goes once the cut of every other replica is past it.
    Proven,
    /// It stays.
    Held,
    /// It goes once the cut of every other replica is past it and past this
    /// moment too.
    ComparedSince(HlcTimestamp),
}

/// The placement of every class of keys in one pool.
///
/// The bucket metadata are placed like the data, unless the pool moves them
/// to the replicas its failures call for
/// ([`PoolLayout::with_bucket_metadata_move`]). The cluster's control state is
/// on every node of the pool ([`ClassLayout::majority_of`]).
#[derive(Debug, Clone)]
pub struct PoolLayout {
    data: ClassLayout,
    bucket_metadata: ClassLayout,
    bucket_metadata_move: Option<Arc<BucketMetadataMove>>,
    cluster_control: ClassLayout,
}

impl PoolLayout {
    /// The layout of `pool`, whose members are `node_ids`: its replication
    /// factor and quorums ([`ClusterConfig::effective_quorum`]) for every
    /// class. The router and every service that places keys by themselves
    /// build it here, so they cannot place a key apart.
    ///
    /// # Errors
    ///
    /// A replication factor the members cannot satisfy, or a quorum the
    /// replication factor cannot.
    pub fn for_pool(
        config: &ClusterConfig,
        pool: &PoolConfig,
        node_ids: Vec<NodeId>,
    ) -> Result<Self, ClusterError> {
        let (rf, write_quorum, read_quorum) = config.effective_quorum(pool);
        let strategy = PlacementStrategy::new(node_ids, rf)?;
        Ok(Self::uniform(ClassLayout::new(
            strategy,
            write_quorum,
            read_quorum,
        )?))
    }

    /// Every class placed by `data`, the cluster's control state on every
    /// node of its ring.
    pub fn uniform(data: ClassLayout) -> Self {
        let cluster_control = ClassLayout::majority_of(data.strategy.all_nodes().to_vec());
        Self {
            bucket_metadata: data.clone(),
            data,
            bucket_metadata_move: None,
            cluster_control,
        }
    }

    /// This layout with the pool's bucket metadata moving as `moving` says:
    /// their keys are placed by its stage from now on.
    pub fn with_bucket_metadata_move(mut self, moving: BucketMetadataMove) -> Self {
        self.bucket_metadata = moving.old.clone();
        self.bucket_metadata_move = Some(Arc::new(moving));
        self
    }

    pub(super) fn with_bucket_metadata_layout(mut self, layout: ClassLayout) -> Self {
        self.bucket_metadata = layout;
        self
    }

    /// The move of the pool's bucket metadata, if they move.
    pub fn bucket_metadata_move(&self) -> Option<&BucketMetadataMove> {
        self.bucket_metadata_move.as_deref()
    }

    /// The layout of `class`.
    pub fn class(&self, class: ReplicationClass) -> &ClassLayout {
        match class {
            ReplicationClass::Data => &self.data,
            ReplicationClass::BucketMetadata => &self.bucket_metadata,
            ReplicationClass::ClusterControl => &self.cluster_control,
        }
    }

    /// The layout of the pool's data.
    pub fn data(&self) -> &ClassLayout {
        &self.data
    }

    /// The replicas of `bucket/key`, spelt in one string, at ring position
    /// `position` ([`s4_core::ring_position_of`] of the key). A key of moving
    /// bucket metadata is held by the replicas of its stage.
    pub fn replicas_at(&self, full_key: &str, position: u64) -> Vec<NodeId> {
        let class = ReplicationClass::of_full_key(full_key);
        if let (ReplicationClass::BucketMetadata, Some(moving)) =
            (class, &self.bucket_metadata_move)
        {
            return moving.holders_at(position);
        }
        self.class(class).strategy.replicas_at(position)
    }

    /// The replicas of `bucket/key`, spelt in one string. A key of moving
    /// bucket metadata is held by the replicas of its stage.
    pub fn replicas_of_full_key(&self, full_key: &str) -> Vec<NodeId> {
        let class = ReplicationClass::of_full_key(full_key);
        if let (ReplicationClass::BucketMetadata, Some(moving)) =
            (class, &self.bucket_metadata_move)
        {
            return moving.holders_of(full_key);
        }
        self.class(class).strategy.get_replicas(full_key)
    }

    /// How long the tombstones of `full_key` stay beyond what anti-entropy
    /// proves of every key (decision 7). Bucket metadata moving at stage 1
    /// or 2 keep them: no round compares the new replicas. Once settled, a
    /// tombstone goes only when the cut of every other replica is past the
    /// moment this node took stage 3, as well as past the tombstone: only
    /// rounds begun after it compared the key on its new replicas.
    pub fn tombstone_keep(&self, full_key: &str) -> TombstoneKeep {
        if ReplicationClass::of_full_key(full_key) != ReplicationClass::BucketMetadata {
            return TombstoneKeep::Proven;
        }
        let Some(moving) = &self.bucket_metadata_move else {
            return TombstoneKeep::Proven;
        };
        match moving.stage.stage() {
            Stage::Legacy => TombstoneKeep::Proven,
            Stage::DualWrite | Stage::ReadNew => TombstoneKeep::Held,
            Stage::Settled => match moving.stage.settled_since() {
                Some(since) => TombstoneKeep::ComparedSince(since),
                None => TombstoneKeep::Held,
            },
        }
    }

    /// Every node of the pool.
    pub fn all_nodes(&self) -> &[NodeId] {
        self.data.strategy.all_nodes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nodes(n: u128) -> Vec<NodeId> {
        (1..=n).map(NodeId).collect()
    }

    /// A bucket's marker and its six configurations are bucket metadata;
    /// objects, a user's key named like them, the IAM records of earlier
    /// releases and erasure-coding metadata are data.
    #[test]
    fn a_bucket_marker_and_its_configurations_are_bucket_metadata() {
        let mut metadata = vec!["__s4_bucket_marker_photos".to_string()];
        metadata.extend(
            s4_core::types::BUCKET_CONFIG_KEY_PREFIXES
                .iter()
                .map(|prefix| format!("{prefix}photos")),
        );
        assert_eq!(metadata.len(), 7);
        for key in &metadata {
            assert_eq!(
                ReplicationClass::of("__system__", key),
                ReplicationClass::BucketMetadata,
                "{key}"
            );
            assert_eq!(
                ReplicationClass::of_full_key(&format!("__system__/{key}")),
                ReplicationClass::BucketMetadata,
                "{key}"
            );
            // A user's key that looks like bucket metadata is still the user's key.
            assert_eq!(
                ReplicationClass::of("photos", key),
                ReplicationClass::Data,
                "{key}"
            );
        }
        for (bucket, key) in [
            ("photos", "cat.jpg"),
            ("__system__", "__s4_iam_user_alice"),
            ("__system__", "__s4_ec_v1/pools/pool-1"),
        ] {
            assert_eq!(
                ReplicationClass::of(bucket, key),
                ReplicationClass::Data,
                "{bucket}/{key}"
            );
            assert_eq!(
                ReplicationClass::of_full_key(&format!("{bucket}/{key}")),
                ReplicationClass::Data,
                "{bucket}/{key}"
            );
        }
    }

    /// The IAM records of version 2 are the cluster's control state: on
    /// every node of the pool, wider than the data's replication factor or
    /// not, written and read through a majority — two majorities always
    /// meet — and their tombstones go once every other node's cut is past
    /// them.
    #[test]
    fn the_control_state_is_on_every_node_through_a_majority() {
        for key in ["__s4_iam_v2_user_alice", "__s4_iam_v2_root_state"] {
            assert_eq!(
                ReplicationClass::of("__system__", key),
                ReplicationClass::ClusterControl
            );
            assert_eq!(
                ReplicationClass::of_full_key(&format!("__system__/{key}")),
                ReplicationClass::ClusterControl
            );
            assert_eq!(ReplicationClass::of("photos", key), ReplicationClass::Data);
        }
        for (pool, majority) in [(1u128, 1u8), (3, 2), (4, 3), (5, 3), (6, 4)] {
            let rf = u8::try_from(pool.min(3)).unwrap();
            let quorum = rf / 2 + 1;
            let layout = PoolLayout::uniform(
                ClassLayout::new(
                    PlacementStrategy::new(nodes(pool), rf).unwrap(),
                    quorum,
                    quorum,
                )
                .unwrap(),
            );
            let control = layout.class(ReplicationClass::ClusterControl);
            assert_eq!(control.replication_factor(), u8::try_from(pool).unwrap());
            assert_eq!(
                (control.write_quorum(), control.read_quorum()),
                (majority, majority),
                "a pool of {pool}"
            );
            assert!(2 * u16::from(majority) > u16::try_from(pool).unwrap());
            let key = "__system__/__s4_iam_v2_user_alice";
            let position = s4_core::ring_position_of(key);
            assert_eq!(layout.replicas_at(key, position), nodes(pool));
            assert_eq!(layout.replicas_of_full_key(key), nodes(pool));
            assert_eq!(layout.tombstone_keep(key), TombstoneKeep::Proven);
        }
    }

    #[test]
    fn a_quorum_the_replicas_cannot_reach_is_refused() {
        let strategy = PlacementStrategy::new(nodes(6), 3).unwrap();
        assert!(ClassLayout::new(strategy.clone(), 2, 2).is_ok());
        assert!(ClassLayout::new(strategy.clone(), 0, 2).is_err());
        assert!(ClassLayout::new(strategy.clone(), 2, 4).is_err());
        assert!(ClassLayout::new(strategy, 4, 2).is_err());
    }

    #[test]
    fn a_uniform_layout_places_both_classes_like_the_data() {
        let layout = PoolLayout::uniform(
            ClassLayout::new(PlacementStrategy::new(nodes(6), 3).unwrap(), 2, 2).unwrap(),
        );
        let marker = "__system__/__s4_bucket_marker_photos";
        let position = s4_core::ring_position_of(marker);
        assert_eq!(
            layout.replicas_at(marker, position),
            layout.data().strategy().get_replicas(marker)
        );
        assert_eq!(
            layout.replicas_of_full_key(marker),
            layout.data().strategy().get_replicas(marker)
        );
        let meta = layout.class(ReplicationClass::BucketMetadata);
        assert_eq!(
            (
                meta.replication_factor(),
                meta.write_quorum(),
                meta.read_quorum()
            ),
            (3, 2, 2)
        );
    }
}
