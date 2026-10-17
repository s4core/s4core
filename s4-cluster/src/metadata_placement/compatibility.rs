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

//! The exhaustive check that the rules of two neighbouring stages meet (plan
//! BUCKET-METADATA-REPLICATION.md, decision 4, task BMR-02-003).
//!
//! The gate keeps every node of a pool within two neighbouring stages. A
//! write acknowledged under the rules of either finds a reader under the
//! rules of either, if every set of acknowledgements the write rule accepts
//! meets every set of answers the read rule waits for. The check below tries
//! every such pair on a pool of six nodes, for the move of the bucket
//! metadata from three replicas to five on one ring and for the move of a
//! key to other replicas, as on a grown ring. Stages two apart do not meet,
//! and the check finds the write a read misses: that is why the gate exists.

use crate::metadata_placement::stage::{QuorumRequirement, ReplicaSide, Stage};
use crate::metadata_placement::target::QuorumShape;

/// The nodes of the pool the check runs on.
const NODES: u32 = 6;

/// A set of nodes of the pool, one bit each.
type Nodes = u32;

/// The replicas of one key in the old and the new layout, and their shapes.
#[derive(Debug, Clone, Copy)]
struct Move {
    old: Nodes,
    new: Nodes,
    old_shape: QuorumShape,
    new_shape: QuorumShape,
}

impl Move {
    fn side(&self, side: ReplicaSide) -> Nodes {
        match side {
            ReplicaSide::Old => self.old,
            ReplicaSide::New => self.new,
        }
    }

    fn holds(&self, requirement: QuorumRequirement, nodes: Nodes) -> bool {
        (nodes & self.side(requirement.side)).count_ones() >= u32::from(requirement.count)
    }

    /// Every set of acknowledgements a write at `stage` succeeds with.
    fn acknowledged_writes(&self, stage: Stage) -> Vec<Nodes> {
        let rules = stage.rules(self.old_shape, self.new_shape);
        let targets = self.old | self.new;
        (0..1u32 << NODES)
            .filter(|acks| acks & !targets == 0)
            .filter(|acks| rules.write.iter().all(|r| self.holds(*r, *acks)))
            .collect()
    }

    /// Every set of answers a read at `stage` decides on.
    fn decided_reads(&self, stage: Stage) -> Vec<Nodes> {
        let read = stage.rules(self.old_shape, self.new_shape).read;
        let asked = self.side(read.side);
        (0..1u32 << NODES)
            .filter(|answers| answers & !asked == 0)
            .filter(|answers| answers.count_ones() == u32::from(read.count))
            .collect()
    }

    /// A write at one of `stages` that a read at one of them misses: the
    /// stage and acknowledgements of the write, the stage and answers of the
    /// read.
    fn missed_write(&self, stages: &[Stage]) -> Option<(Stage, Nodes, Stage, Nodes)> {
        for &write_stage in stages {
            for acks in self.acknowledged_writes(write_stage) {
                for &read_stage in stages {
                    for answers in self.decided_reads(read_stage) {
                        if acks & answers == 0 {
                            return Some((write_stage, acks, read_stage, answers));
                        }
                    }
                }
            }
        }
        None
    }
}

fn nodes(list: &[u32]) -> Nodes {
    list.iter().fold(0, |set, node| set | 1 << node)
}

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

/// The bucket metadata of an EC 4+2 pool: three replicas on the ring, then
/// the same three and the next two.
fn three_to_five() -> Move {
    Move {
        old: nodes(&[0, 1, 2]),
        new: nodes(&[0, 1, 2, 3, 4]),
        old_shape: S3,
        new_shape: S5,
    }
}

/// A key whose three replicas move on a grown ring: one replica kept, then
/// none.
fn grown_rings() -> [Move; 2] {
    [
        Move {
            old: nodes(&[0, 1, 2]),
            new: nodes(&[2, 3, 4]),
            old_shape: S3,
            new_shape: S3,
        },
        Move {
            old: nodes(&[0, 1, 2]),
            new: nodes(&[3, 4, 5]),
            old_shape: S3,
            new_shape: S3,
        },
    ]
}

fn every_move() -> Vec<Move> {
    let mut moves = vec![three_to_five()];
    moves.extend(grown_rings());
    moves
}

#[test]
fn neighbouring_stages_meet_on_every_move() {
    for transition in every_move() {
        for pair in Stage::ALL.windows(2) {
            assert_eq!(
                transition.missed_write(pair),
                None,
                "stages {pair:?} of {transition:?}"
            );
        }
        // Every stage writes and reads at all: the check is not empty.
        for stage in Stage::ALL {
            assert!(!transition.acknowledged_writes(stage).is_empty(), "{stage}");
            assert!(!transition.decided_reads(stage).is_empty(), "{stage}");
        }
    }
}

/// Switching the replication factor in one step lets two acknowledgements
/// in the three old replicas and three answers of the five new ones miss
/// each other: the counterexample of the plan.
#[test]
fn stage_zero_and_stage_three_miss_a_write() {
    let (write_stage, acks, read_stage, answers) = three_to_five()
        .missed_write(&[Stage::Legacy, Stage::Settled])
        .expect("a write the read misses");
    assert_eq!((write_stage, read_stage), (Stage::Legacy, Stage::Settled));
    assert_eq!(acks.count_ones(), 2);
    assert_eq!(answers.count_ones(), 3);
    assert_eq!(acks & answers, 0);
    for transition in grown_rings() {
        assert!(transition.missed_write(&[Stage::Legacy, Stage::Settled]).is_some());
    }
}

/// Stages two apart miss writes too: a node may not skip a stage, and the
/// writes of stage 0 reach the new replicas by backfill, not by the rules.
#[test]
fn stages_two_apart_miss_a_write() {
    for transition in every_move() {
        assert!(
            transition.missed_write(&[Stage::Legacy, Stage::ReadNew]).is_some(),
            "{transition:?}"
        );
    }
}
