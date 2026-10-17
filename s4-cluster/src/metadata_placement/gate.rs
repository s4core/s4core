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

//! The gate of the next stage (plan BUCKET-METADATA-REPLICATION.md,
//! decision 4).
//!
//! A node takes stage `s + 1` only once every node of its pool — by the
//! pool's list of nodes, not by the nodes alive — announced the same move at
//! stage `s` or later, speaks the protocol of the move, and, before reads
//! move to the new layout, finished its backfill. So while one node is at
//! `s + 1`, none is below `s`: no more than two neighbouring stages are ever
//! in force at once, and those meet. A node that is down, unknown, of an
//! earlier release or silent holds the pool where it is; the verdict names
//! it and why.

use crate::identity::NodeId;
use crate::metadata_placement::stage::{PlacementRecord, Stage};

/// What this node knows of one node of its pool, for the gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerPlacement {
    /// Gossip has no entry for the node.
    Unknown,
    /// Gossip holds the node, but not alive.
    Down,
    /// The node is alive and has not confirmed the protocol of the move:
    /// `confirmed` is what it announced, `None` before its first metadata.
    BelowProtocol {
        /// The protocol version the node announced.
        confirmed: Option<u32>,
    },
    /// The node speaks the protocol and announced no placement.
    Silent,
    /// The node's latest announcement.
    Announced(PlacementRecord),
}

/// Where the gate learns about the other nodes of the pool.
pub trait PlacementView {
    /// What is known of `node`, checked against `min_protocol`.
    fn peer_placement(&self, node: NodeId, min_protocol: u32) -> PeerPlacement;
}

/// Why one node holds the next stage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HoldoutReason {
    /// This node is not in the list of its pool.
    NotInPool,
    /// Gossip has no entry for the node.
    Unknown,
    /// The node is not alive.
    Down,
    /// The node has not confirmed the protocol of the move.
    BelowProtocol {
        /// The protocol version the node announced, if any.
        confirmed: Option<u32>,
    },
    /// The node announced no placement.
    Silent,
    /// The node announced another move: a different target, or source.
    OtherMove(PlacementRecord),
    /// The node is at an earlier stage.
    Behind(Stage),
    /// The node has not finished its backfill.
    BackfillPending,
}

impl std::fmt::Display for HoldoutReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotInPool => write!(f, "this node is not in the list of its pool"),
            Self::Unknown => write!(f, "unknown to gossip"),
            Self::Down => write!(f, "not alive"),
            Self::BelowProtocol { confirmed: None } => {
                write!(f, "has not announced its protocol yet")
            }
            Self::BelowProtocol {
                confirmed: Some(version),
            } => write!(f, "speaks protocol {version}"),
            Self::Silent => write!(f, "announced no bucket metadata placement"),
            Self::OtherMove(record) => write!(
                f,
                "moves from {} to {}, another move",
                record.from, record.to
            ),
            Self::Behind(stage) => write!(f, "is at stage {stage}"),
            Self::BackfillPending => write!(f, "has not finished its backfill"),
        }
    }
}

/// One node that holds the next stage, and why.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Holdout {
    /// The node.
    pub node: NodeId,
    /// Why.
    pub reason: HoldoutReason,
}

/// What the gate decided.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GateVerdict {
    /// The move is over: there is no next stage.
    Settled,
    /// This node may take `stage`.
    Advance(Stage),
    /// These nodes hold the next stage, in the order of the pool.
    Hold(Vec<Holdout>),
}

/// The gate of the stage after `record`'s, for `local` in `pool`.
///
/// `local_protocol` is the protocol this binary speaks and `min_protocol`
/// the one the move needs: this node confirms it by itself, every other node
/// by what `view` has of it.
pub fn next_stage(
    pool: &[NodeId],
    local: NodeId,
    record: &PlacementRecord,
    local_protocol: u32,
    min_protocol: u32,
    view: &dyn PlacementView,
) -> GateVerdict {
    let Some(next) = record.stage.next() else {
        return GateVerdict::Settled;
    };
    let needs_backfill = next == Stage::ReadNew;
    let mut holdouts = Vec::new();
    if !pool.contains(&local) {
        holdouts.push(Holdout {
            node: local,
            reason: HoldoutReason::NotInPool,
        });
    }
    for &node in pool {
        let reason = if node == local {
            if local_protocol < min_protocol {
                Some(HoldoutReason::BelowProtocol {
                    confirmed: Some(local_protocol),
                })
            } else if needs_backfill && !record.backfill_complete {
                Some(HoldoutReason::BackfillPending)
            } else {
                None
            }
        } else {
            match view.peer_placement(node, min_protocol) {
                PeerPlacement::Unknown => Some(HoldoutReason::Unknown),
                PeerPlacement::Down => Some(HoldoutReason::Down),
                PeerPlacement::BelowProtocol { confirmed } => {
                    Some(HoldoutReason::BelowProtocol { confirmed })
                }
                PeerPlacement::Silent => Some(HoldoutReason::Silent),
                PeerPlacement::Announced(peer) if !peer.same_move(record) => {
                    Some(HoldoutReason::OtherMove(peer))
                }
                PeerPlacement::Announced(peer) if peer.stage < record.stage => {
                    Some(HoldoutReason::Behind(peer.stage))
                }
                PeerPlacement::Announced(peer) if needs_backfill && !peer.backfill_complete => {
                    Some(HoldoutReason::BackfillPending)
                }
                PeerPlacement::Announced(_) => None,
            }
        };
        if let Some(reason) = reason {
            holdouts.push(Holdout { node, reason });
        }
    }
    if holdouts.is_empty() {
        GateVerdict::Advance(next)
    } else {
        GateVerdict::Hold(holdouts)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::metadata_placement::target::QuorumShape;

    const S3: QuorumShape = QuorumShape {
        replication_factor: 3,
        write_quorum: 2,
        read_quorum: 2,
    };
    const S5: QuorumShape = QuorumShape {
        replication_factor: 5,
        write_quorum: 3,
        read_quorum: 3,
    };
    const S6: QuorumShape = QuorumShape {
        replication_factor: 6,
        write_quorum: 4,
        read_quorum: 4,
    };
    const PROTOCOL: u32 = 7;

    /// A membership made up for the test: what is not in it is unknown.
    #[derive(Default)]
    struct FakeView(HashMap<NodeId, PeerPlacement>);

    impl PlacementView for FakeView {
        fn peer_placement(&self, node: NodeId, _min_protocol: u32) -> PeerPlacement {
            self.0.get(&node).copied().unwrap_or(PeerPlacement::Unknown)
        }
    }

    fn pool() -> Vec<NodeId> {
        (1..=6).map(NodeId).collect()
    }

    fn at(stage: Stage, backfill_complete: bool) -> PlacementRecord {
        PlacementRecord {
            stage,
            backfill_complete,
            ..PlacementRecord::starting(S3, S5)
        }
    }

    /// Every peer of the pool announced `record`.
    fn all_at(record: PlacementRecord) -> FakeView {
        FakeView(
            pool()
                .into_iter()
                .skip(1)
                .map(|node| (node, PeerPlacement::Announced(record)))
                .collect(),
        )
    }

    fn verdict(local: &PlacementRecord, view: &FakeView) -> GateVerdict {
        next_stage(&pool(), NodeId(1), local, PROTOCOL, PROTOCOL, view)
    }

    fn held_by(node: u128, reason: HoldoutReason) -> GateVerdict {
        GateVerdict::Hold(vec![Holdout {
            node: NodeId(node),
            reason,
        }])
    }

    #[test]
    fn the_pool_at_one_stage_lets_a_node_take_the_next() {
        let legacy = at(Stage::Legacy, false);
        assert_eq!(
            verdict(&legacy, &all_at(legacy)),
            GateVerdict::Advance(Stage::DualWrite)
        );
        let read_new = at(Stage::ReadNew, true);
        assert_eq!(
            verdict(&read_new, &all_at(read_new)),
            GateVerdict::Advance(Stage::Settled)
        );
        let settled = at(Stage::Settled, true);
        assert_eq!(verdict(&settled, &all_at(settled)), GateVerdict::Settled);
        // A peer already a stage ahead does not hold this node back.
        assert_eq!(
            verdict(&legacy, &all_at(at(Stage::DualWrite, false))),
            GateVerdict::Advance(Stage::DualWrite)
        );
    }

    #[test]
    fn a_dead_node_holds_the_pool() {
        let legacy = at(Stage::Legacy, false);
        let mut view = all_at(legacy);
        view.0.insert(NodeId(4), PeerPlacement::Down);
        assert_eq!(verdict(&legacy, &view), held_by(4, HoldoutReason::Down));
    }

    #[test]
    fn an_unknown_node_holds_the_pool() {
        let legacy = at(Stage::Legacy, false);
        let mut view = all_at(legacy);
        view.0.remove(&NodeId(6));
        assert_eq!(verdict(&legacy, &view), held_by(6, HoldoutReason::Unknown));
        // This node outside its own pool list does not move either.
        let view = all_at(legacy);
        let verdict = next_stage(&pool(), NodeId(9), &legacy, PROTOCOL, PROTOCOL, &view);
        assert!(
            matches!(verdict, GateVerdict::Hold(ref h) if h[0].reason == HoldoutReason::NotInPool)
        );
    }

    #[test]
    fn a_different_target_holds_the_pool() {
        let legacy = at(Stage::Legacy, false);
        let mut view = all_at(legacy);
        let other = PlacementRecord::starting(S3, S6);
        view.0.insert(NodeId(2), PeerPlacement::Announced(other));
        assert_eq!(
            verdict(&legacy, &view),
            held_by(2, HoldoutReason::OtherMove(other))
        );
    }

    /// A node of an earlier release confirms no protocol of the move and
    /// announces no placement; this binary below the protocol holds itself.
    #[test]
    fn an_earlier_release_holds_the_pool() {
        let legacy = at(Stage::Legacy, false);
        let mut view = all_at(legacy);
        view.0.insert(
            NodeId(3),
            PeerPlacement::BelowProtocol { confirmed: Some(6) },
        );
        view.0.insert(NodeId(5), PeerPlacement::Silent);
        assert_eq!(
            verdict(&legacy, &view),
            GateVerdict::Hold(vec![
                Holdout {
                    node: NodeId(3),
                    reason: HoldoutReason::BelowProtocol { confirmed: Some(6) }
                },
                Holdout {
                    node: NodeId(5),
                    reason: HoldoutReason::Silent
                },
            ])
        );
        let all = all_at(legacy);
        assert_eq!(
            next_stage(&pool(), NodeId(1), &legacy, 6, PROTOCOL, &all),
            held_by(1, HoldoutReason::BelowProtocol { confirmed: Some(6) })
        );
    }

    #[test]
    fn a_node_behind_holds_the_pool() {
        let dual = at(Stage::DualWrite, true);
        let mut view = all_at(dual);
        view.0.insert(
            NodeId(2),
            PeerPlacement::Announced(at(Stage::Legacy, false)),
        );
        assert_eq!(
            verdict(&dual, &view),
            held_by(2, HoldoutReason::Behind(Stage::Legacy))
        );
    }

    /// Reads move to the new layout only once every node, this one included,
    /// finished its backfill; the other stages do not wait for it.
    #[test]
    fn reads_move_only_after_every_backfill() {
        let dual = at(Stage::DualWrite, true);
        let mut view = all_at(dual);
        view.0.insert(
            NodeId(5),
            PeerPlacement::Announced(at(Stage::DualWrite, false)),
        );
        assert_eq!(
            verdict(&dual, &view),
            held_by(5, HoldoutReason::BackfillPending)
        );
        let unfinished = at(Stage::DualWrite, false);
        assert_eq!(
            verdict(&unfinished, &all_at(dual)),
            held_by(1, HoldoutReason::BackfillPending)
        );
        assert_eq!(
            verdict(&dual, &all_at(dual)),
            GateVerdict::Advance(Stage::ReadNew)
        );
        let legacy = at(Stage::Legacy, false);
        assert_eq!(
            verdict(&legacy, &all_at(legacy)),
            GateVerdict::Advance(Stage::DualWrite)
        );
    }
}
