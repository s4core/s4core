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

//! Durable, append-only pool generations and the gate before a ring move.
//!
//! A proposal does not change ownership: traffic stays on `current` until the
//! transition driver calls `begin` after all its gates open, and the pool
//! settles on the successor only through `finish`, after every range cut over
//! and the common barrier passed.

use std::collections::HashSet;

use s4_core::storage::IndexDb;
use serde::{Deserialize, Serialize};

use crate::protocol::POOL_GENERATION_MIN_PROTOCOL_VERSION;
use crate::{ClusterError, NodeId, PoolId};

/// The topology epoch every replica call of a pool carries. A growth does not
/// change it: the generation is what replicas fence a coordinator by
/// (`placement_generation`), while a node that settled on the successor and
/// one that has not yet must still talk, and erasure sets keep the epoch they
/// were laid out at.
pub const WIRE_TOPOLOGY_EPOCH: u64 = 1;

/// A ring identity: a monotonic epoch and the exact ordered member roster.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PoolGeneration {
    /// Epoch of this ring, beginning at one.
    pub epoch: u64,
    /// Stable member identities in configured order.
    pub nodes: Vec<NodeId>,
}

impl PoolGeneration {
    fn validate(&self) -> Result<(), ClusterError> {
        if self.epoch == 0 || self.nodes.is_empty() {
            return Err(config(
                "a generation needs a nonzero epoch and a nonempty roster",
            ));
        }
        let unique: HashSet<_> = self.nodes.iter().collect();
        if unique.len() != self.nodes.len() {
            return Err(config("duplicate node identities in the roster"));
        }
        Ok(())
    }

    fn stamp(&self, pool: PoolId) -> GenerationStamp {
        let mut hash = blake3::Hasher::new();
        hash.update(&pool.as_u32().to_be_bytes());
        hash.update(&self.epoch.to_be_bytes());
        for node in &self.nodes {
            hash.update(&node.0.to_be_bytes());
        }
        let mut fingerprint = [0; 16];
        fingerprint.copy_from_slice(&hash.finalize().as_bytes()[..16]);
        GenerationStamp {
            epoch: self.epoch,
            node_count: self.nodes.len() as u64,
            fingerprint,
        }
    }
}

/// The node's durable knowledge of its pool. `proposed` is preparation until
/// `moving` is set; reads continue to use `current` until ranges cut over.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PoolGenerationRecord {
    /// Encoding version, independent of the gossip protocol.
    pub format: u32,
    /// Pool this record belongs to.
    pub pool_id: PoolId,
    /// The generation that owns data before the move.
    pub current: PoolGeneration,
    /// The proposed append-only successor, if any.
    pub proposed: Option<PoolGeneration>,
    /// Whether the move has begun and rollback is forbidden.
    pub moving: bool,
}

impl PoolGenerationRecord {
    /// Initial stable pool, at epoch one.
    pub fn initial(pool_id: PoolId, nodes: Vec<NodeId>) -> Result<Self, ClusterError> {
        let record = Self {
            format: 1,
            pool_id,
            current: PoolGeneration { epoch: 1, nodes },
            proposed: None,
            moving: false,
        };
        record.validate()?;
        Ok(record)
    }

    /// Validates decoded state before it can affect placement.
    pub fn validate(&self) -> Result<(), ClusterError> {
        if self.format != 1 {
            return Err(config("unsupported generation record format"));
        }
        self.current.validate()?;
        if let Some(next) = &self.proposed {
            next.validate()?;
            if self.current.epoch.checked_add(1) != Some(next.epoch)
                || next.nodes.len() <= self.current.nodes.len()
                || !next.nodes.starts_with(&self.current.nodes)
            {
                return Err(config(
                    "the successor must append nodes and increment the epoch once",
                ));
            }
        } else if self.moving {
            return Err(config("a moving pool needs its successor"));
        }
        Ok(())
    }

    /// Applies startup configuration without changing the active generation.
    /// Removal, replacement, reordering and a second growth are refused.
    pub fn configure(&self, nodes: Vec<NodeId>) -> Result<Self, ClusterError> {
        self.validate()?;
        PoolGeneration {
            epoch: self.current.epoch,
            nodes: nodes.clone(),
        }
        .validate()?;
        if nodes == self.current.nodes {
            if self.moving {
                return Err(config("cannot roll back a pool whose move already began"));
            }
            let mut stable = self.clone();
            stable.proposed = None;
            return Ok(stable);
        }
        if let Some(next) = &self.proposed {
            return if next.nodes == nodes {
                Ok(self.clone())
            } else {
                Err(config(
                    "a growth is already pending; finish it before another growth",
                ))
            };
        }
        if !nodes.starts_with(&self.current.nodes) {
            return Err(config("S4_POOL_NODES must append to the recorded ordered roster; removal, replacement and reordering are forbidden"));
        }
        let epoch = self
            .current
            .epoch
            .checked_add(1)
            .ok_or_else(|| config("generation epoch overflow"))?;
        let mut result = self.clone();
        result.proposed = Some(PoolGeneration { epoch, nodes });
        result.validate()?;
        Ok(result)
    }

    /// Marks a prepared transition irreversible. The migration driver must
    /// first check `gate`, disk space and readiness of joint placement.
    pub fn begin(&self) -> Result<Self, ClusterError> {
        self.validate()?;
        if self.proposed.is_none() {
            return Err(config("there is no proposed generation to begin"));
        }
        let mut result = self.clone();
        result.moving = true;
        Ok(result)
    }

    /// The pool settled on its successor: `Stable(g + 1)`. Only a moving pool
    /// finishes, after every range cut over and the common barrier passed
    /// (plan ERASURE-CODING-FIXING-POOL-GROWTH-REBALANCE.md, `EPGR-04-2`).
    ///
    /// # Errors
    ///
    /// A pool that is not moving.
    pub fn finish(&self) -> Result<Self, ClusterError> {
        self.validate()?;
        let next = match (&self.proposed, self.moving) {
            (Some(next), true) => next.clone(),
            _ => return Err(config("only a moving pool can finish its growth")),
        };
        let result = Self {
            format: self.format,
            pool_id: self.pool_id,
            current: next,
            proposed: None,
            moving: false,
        };
        result.validate()?;
        Ok(result)
    }

    /// Whether `finished` is this moving record's own finish: the successor
    /// became the current generation.
    pub fn finishes_into(&self, finished: &Self) -> bool {
        self.finish().is_ok_and(|expected| expected == *finished)
    }

    /// Checks the announcement of every new member, including the old ones.
    /// Bucket metadata must have settled before growth can begin.
    pub fn gate(
        &self,
        view: impl Fn(NodeId) -> Option<GenerationPeer>,
    ) -> Result<(), ClusterError> {
        self.validate()?;
        let next = self.proposed.as_ref().ok_or_else(|| config("no growth is proposed"))?;
        let expected = GenerationAnnouncement::of(self, false);
        for node in &next.nodes {
            let peer = view(*node)
                .ok_or_else(|| config(format!("waiting for node {node}: no metadata")))?;
            if !peer.alive || peer.protocol < POOL_GENERATION_MIN_PROTOCOL_VERSION {
                return Err(config(format!("waiting for node {node}: alive at protocol {POOL_GENERATION_MIN_PROTOCOL_VERSION} required")));
            }
            // The moving bit is not a readiness condition: peers may observe
            // the start in different orders, while agreeing on both rings.
            if !peer.announcement.is_some_and(|a| {
                !a.bootstrapping && a.current == expected.current && a.proposed == expected.proposed
            }) {
                return Err(config(format!(
                    "waiting for node {node}: successor generation not confirmed"
                )));
            }
            if !peer.metadata_settled {
                return Err(config(format!(
                    "waiting for node {node}: bucket metadata placement has not settled"
                )));
            }
        }
        Ok(())
    }

    /// Encodes the record carried by `__system__` and `cluster_meta` alike.
    pub fn encode(&self) -> Result<Vec<u8>, ClusterError> {
        self.validate()?;
        serde_json::to_vec(self).map_err(|e| ClusterError::Storage(e.to_string()))
    }

    /// Decodes and validates durable or replicated state.
    pub fn decode(bytes: &[u8]) -> Result<Self, ClusterError> {
        let record: Self = serde_json::from_slice(bytes).map_err(|e| {
            ClusterError::Storage(format!("pool generation could not be decoded: {e}"))
        })?;
        record.validate()?;
        Ok(record)
    }
}

/// Compact identity of an ordered generation; its roster is a prefix of config.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct GenerationStamp {
    /// Epoch of the generation.
    pub epoch: u64,
    /// Length of the prefix in `S4_POOL_NODES`.
    pub node_count: u64,
    /// Fingerprint of pool id, epoch and every identity in order.
    pub fingerprint: [u8; 16],
}

/// Fixed-size gossip tail, independent of pool size and safe under the MTU.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct GenerationAnnouncement {
    /// Current owners.
    pub current: GenerationStamp,
    /// Proposed successor.
    pub proposed: Option<GenerationStamp>,
    /// The move already began.
    pub moving: bool,
    /// A fresh node's initial claim, not yet backed by durable pool knowledge.
    pub bootstrapping: bool,
}

impl GenerationAnnouncement {
    /// Builds a bounded announcement from validated local state.
    pub fn of(record: &PoolGenerationRecord, bootstrapping: bool) -> Self {
        Self {
            current: record.current.stamp(record.pool_id),
            proposed: record.proposed.as_ref().map(|g| g.stamp(record.pool_id)),
            moving: record.moving,
            bootstrapping,
        }
    }

    /// Reconstructs the exact generations from configured prefixes. A different
    /// identity or order cannot masquerade as the same generation.
    pub fn resolve(
        &self,
        pool_id: PoolId,
        configured: &[NodeId],
    ) -> Result<PoolGenerationRecord, ClusterError> {
        let resolve = |stamp: GenerationStamp| {
            let count =
                usize::try_from(stamp.node_count).map_err(|_| config("roster length overflow"))?;
            let nodes = configured
                .get(..count)
                .ok_or_else(|| config("configured roster omits recorded members"))?
                .to_vec();
            let generation = PoolGeneration {
                epoch: stamp.epoch,
                nodes,
            };
            if generation.stamp(pool_id) != stamp {
                return Err(config(
                    "configured ordered roster differs from the announced generation",
                ));
            }
            Ok(generation)
        };
        let record = PoolGenerationRecord {
            format: 1,
            pool_id,
            current: resolve(self.current)?,
            proposed: self.proposed.map(resolve).transpose()?,
            moving: self.moving,
        };
        record.validate()?;
        Ok(record)
    }

    /// The identity the announced current generation holds at `position` of
    /// the configured roster, found among `candidates`: the roster with that
    /// one place taken by the candidate is exactly the announced generation.
    ///
    /// Answers the question a refused start leaves an operator with — which
    /// node does the pool expect at this address — when the identity is still
    /// known somewhere this node can see. `None` says only that none of the
    /// candidates is it, never that the place is free.
    pub fn holder_of(
        &self,
        pool_id: PoolId,
        configured: &[NodeId],
        position: usize,
        candidates: impl IntoIterator<Item = NodeId>,
    ) -> Option<NodeId> {
        let count = usize::try_from(self.current.node_count).ok()?;
        if position >= count {
            return None;
        }
        let mut nodes = configured.get(..count)?.to_vec();
        candidates.into_iter().find(|candidate| {
            nodes[position] = *candidate;
            let generation = PoolGeneration {
                epoch: self.current.epoch,
                nodes: nodes.clone(),
            };
            generation.stamp(pool_id) == self.current
        })
    }
}

/// One member's input to the generation readiness gate.
#[derive(Debug, Clone, Copy)]
pub struct GenerationPeer {
    /// SWIM currently holds this member alive.
    pub alive: bool,
    /// Protocol explicitly announced by the member.
    pub protocol: u32,
    /// Generation explicitly announced by the member.
    pub announcement: Option<GenerationAnnouncement>,
    /// Its bucket metadata move is settled.
    pub metadata_settled: bool,
}

/// The replicated generation record's object key in `__system__`.
pub fn object_key(pool: PoolId) -> String {
    format!("__s4_pool_generation_v1/{}", pool.as_u32())
}

/// The replicated record of the ranges that cut over in the move to
/// `to_epoch`, kept beside the generation record in `__system__`.
pub fn cut_over_key(pool: PoolId, to_epoch: u64) -> String {
    format!(
        "__s4_pool_generation_v1/{}/cut-over/{to_epoch}",
        pool.as_u32()
    )
}

/// The replicated record of an operator's pause of the move to `to_epoch`,
/// kept beside the generation record in `__system__`.
pub fn pause_key(pool: PoolId, to_epoch: u64) -> String {
    format!("__s4_pool_generation_v1/{}/pause/{to_epoch}", pool.as_u32())
}

/// Whether an operator paused the move of a pool's keys, for every member:
/// the admin API writes it through the quorums, and each member's driver
/// reads it and pauses or resumes its own move. The newest write wins.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PauseRecord {
    /// Encoding version.
    pub format: u32,
    /// Pool of the move.
    pub pool_id: PoolId,
    /// Epoch the keys move from.
    pub from_epoch: u64,
    /// Epoch the keys move to.
    pub to_epoch: u64,
    /// Why the move is paused; `None` once it is resumed.
    pub paused: Option<String>,
}

impl PauseRecord {
    /// The record of the move of `record`: paused for `paused`, or resumed.
    ///
    /// # Errors
    ///
    /// A record without a successor.
    pub fn of(record: &PoolGenerationRecord, paused: Option<String>) -> Result<Self, ClusterError> {
        let next = record
            .proposed
            .as_ref()
            .ok_or_else(|| config("a pause record needs a successor generation"))?;
        Ok(Self {
            format: 1,
            pool_id: record.pool_id,
            from_epoch: record.current.epoch,
            to_epoch: next.epoch,
            paused,
        })
    }

    /// Whether this record describes the move of `record`.
    pub fn belongs_to(&self, record: &PoolGenerationRecord) -> bool {
        record.proposed.as_ref().is_some_and(|next| {
            self.pool_id == record.pool_id
                && self.from_epoch == record.current.epoch
                && self.to_epoch == next.epoch
        })
    }

    /// The encoded record.
    pub fn encode(&self) -> Result<Vec<u8>, ClusterError> {
        serde_json::to_vec(self).map_err(|e| ClusterError::Codec(e.to_string()))
    }

    /// Decodes a record.
    pub fn decode(bytes: &[u8]) -> Result<Self, ClusterError> {
        let record: Self = serde_json::from_slice(bytes)
            .map_err(|e| ClusterError::Codec(format!("pause record: {e}")))?;
        if record.format != 1 {
            return Err(ClusterError::Codec(format!(
                "pause record: unsupported format {}",
                record.format
            )));
        }
        Ok(record)
    }
}

/// The ranges of a move that were verified and cut over, by the upper
/// endpoints of their arcs. Published through the joint quorum with an HLC;
/// it only grows, and two publishers merge what they read before writing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CutOverRecord {
    /// Encoding version.
    pub format: u32,
    /// Pool of the move.
    pub pool_id: PoolId,
    /// Epoch the keys move from.
    pub from_epoch: u64,
    /// Epoch the keys move to.
    pub to_epoch: u64,
    /// Upper endpoints of the arcs cut over, ascending and without repeats.
    pub ranges: Vec<u64>,
}

impl CutOverRecord {
    /// An empty record of the move of `record`.
    ///
    /// # Errors
    ///
    /// A record without a successor.
    pub fn of(record: &PoolGenerationRecord) -> Result<Self, ClusterError> {
        let next = record
            .proposed
            .as_ref()
            .ok_or_else(|| config("a cut-over record needs a successor generation"))?;
        Ok(Self {
            format: 1,
            pool_id: record.pool_id,
            from_epoch: record.current.epoch,
            to_epoch: next.epoch,
            ranges: Vec::new(),
        })
    }

    /// Adds `ranges`; returns whether anything was new.
    pub fn merge(&mut self, ranges: impl IntoIterator<Item = u64>) -> bool {
        let before = self.ranges.len();
        self.ranges.extend(ranges);
        self.ranges.sort_unstable();
        self.ranges.dedup();
        self.ranges.len() != before
    }

    /// Whether this record describes the move of `record`.
    pub fn belongs_to(&self, record: &PoolGenerationRecord) -> bool {
        record.proposed.as_ref().is_some_and(|next| {
            self.pool_id == record.pool_id
                && self.from_epoch == record.current.epoch
                && self.to_epoch == next.epoch
        })
    }

    /// The encoded record.
    pub fn encode(&self) -> Result<Vec<u8>, ClusterError> {
        serde_json::to_vec(self).map_err(|e| ClusterError::Codec(e.to_string()))
    }

    /// Decodes a record.
    pub fn decode(bytes: &[u8]) -> Result<Self, ClusterError> {
        let record: Self = serde_json::from_slice(bytes)
            .map_err(|e| ClusterError::Codec(format!("cut-over record: {e}")))?;
        if record.format != 1 {
            return Err(ClusterError::Codec(format!(
                "cut-over record: unsupported format {}",
                record.format
            )));
        }
        Ok(record)
    }
}

/// Reads and validates the local durable generation, if it exists.
pub async fn load(index: &IndexDb) -> Result<Option<PoolGenerationRecord>, ClusterError> {
    index
        .pool_generation()
        .await
        .map_err(|e| ClusterError::Storage(e.to_string()))?
        .map(|b| PoolGenerationRecord::decode(&b))
        .transpose()
}

/// Saves a validated local generation before announcing or using it.
pub async fn store(index: &IndexDb, record: &PoolGenerationRecord) -> Result<(), ClusterError> {
    record.validate()?;
    if let Some(stored) = load(index).await? {
        if stored == *record {
            return Ok(());
        }
        // The one change of the current owners: a move that finished.
        if stored.finishes_into(record) {
            return index
                .store_pool_generation(record.encode()?)
                .await
                .map_err(|e| ClusterError::Storage(e.to_string()));
        }
        if stored.pool_id != record.pool_id
            || stored.current != record.current
            || (stored.moving && (!record.moving || stored.proposed != record.proposed))
            || (stored.proposed.is_some()
                && record.proposed.is_some()
                && stored.proposed != record.proposed)
        {
            return Err(config(
                "refusing to replace durable ownership or an in-progress growth",
            ));
        }
    }
    index
        .store_pool_generation(record.encode()?)
        .await
        .map_err(|e| ClusterError::Storage(e.to_string()))
}

fn config(message: impl std::fmt::Display) -> ClusterError {
    ClusterError::Config(format!("pool generation: {message}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_pause_record_belongs_to_its_move_alone() {
        let prepared = PoolGenerationRecord::initial(PoolId::new(0), (1..=3).map(NodeId).collect())
            .unwrap()
            .configure((1..=6).map(NodeId).collect())
            .unwrap();
        let record = PauseRecord::of(&prepared, Some("night".into())).unwrap();
        let decoded = PauseRecord::decode(&record.encode().unwrap()).unwrap();
        assert_eq!(decoded, record);
        assert!(decoded.belongs_to(&prepared));
        assert_eq!(
            pause_key(PoolId::new(0), 2),
            "__s4_pool_generation_v1/0/pause/2"
        );
        let settled = prepared.begin().unwrap().finish().unwrap();
        assert!(!decoded.belongs_to(&settled));
        assert!(PauseRecord::of(&settled, None).is_err());
        let mut later = decoded.clone();
        later.format = 2;
        assert!(PauseRecord::decode(&later.encode().unwrap()).is_err());
    }

    #[test]
    fn a_fresh_identity_at_an_owner_address_names_the_owner_it_stands_in_for() {
        let pool = PoolId::new(1);
        let owners: Vec<NodeId> = (1..=6).map(NodeId).collect();
        let record = PoolGenerationRecord::initial(pool, owners.clone()).unwrap();
        let announced = GenerationAnnouncement::of(&record, false);

        let mut seen = owners.clone();
        seen[3] = NodeId(40);
        assert!(announced.resolve(pool, &seen).is_err());
        assert_eq!(
            announced.holder_of(pool, &seen, 3, [NodeId(7), NodeId(4), NodeId(8)]),
            Some(NodeId(4))
        );
        // Not at that place, or not among the candidates: no name, no guess.
        assert_eq!(announced.holder_of(pool, &seen, 2, [NodeId(4)]), None);
        assert_eq!(announced.holder_of(pool, &seen, 3, [NodeId(7)]), None);
        assert_eq!(announced.holder_of(pool, &seen, 6, [NodeId(4)]), None);
    }
}
