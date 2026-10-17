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

//! The stages of the move between two layouts, the rules each stage reads
//! and writes by, and the record of where a node is (plan
//! BUCKET-METADATA-REPLICATION.md, decision 4).
//!
//! | Stage | A write succeeds with | A read takes |
//! |---|---|---|
//! | 0 `Legacy` | the old write quorum | the old read quorum |
//! | 1 `DualWrite` | the old write quorum **and** the new one | the old read quorum |
//! | 2 `ReadNew` | as in 1 | the new read quorum |
//! | 3 `Settled` | the new write quorum | the new read quorum |
//!
//! The table lives in [`Stage::rules`] and nowhere else: the coordinators
//! apply it and the exhaustive check of `compatibility` proves it.

use serde::{Deserialize, Serialize};

use crate::error::ClusterError;
use crate::metadata_placement::target::QuorumShape;

/// One of the four stages of the move, in the order a pool passes them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Stage {
    /// The old layout alone.
    Legacy,
    /// Writes reach both layouts; reads take the old one.
    DualWrite,
    /// Writes reach both layouts; reads take the new one.
    ReadNew,
    /// The new layout alone.
    Settled,
}

impl Stage {
    /// Every stage, in order.
    pub const ALL: [Self; 4] = [Self::Legacy, Self::DualWrite, Self::ReadNew, Self::Settled];

    /// The number of the stage, 0 to 3, as logs, metrics and the record
    /// name it.
    pub fn number(self) -> u8 {
        match self {
            Self::Legacy => 0,
            Self::DualWrite => 1,
            Self::ReadNew => 2,
            Self::Settled => 3,
        }
    }

    /// The stage numbered `number`.
    pub fn from_number(number: u8) -> Option<Self> {
        Self::ALL.get(usize::from(number)).copied()
    }

    /// The stage after this one, `None` for [`Stage::Settled`].
    pub fn next(self) -> Option<Self> {
        Self::from_number(self.number() + 1)
    }

    /// The name of the stage in logs.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Legacy => "legacy",
            Self::DualWrite => "dual_write",
            Self::ReadNew => "read_new",
            Self::Settled => "settled",
        }
    }

    /// Whether a replica at this stage refuses a write of bucket metadata a
    /// coordinator placed by `request_stage` (decision 6).
    ///
    /// A client's write placed by a stage below this one less one comes from
    /// a node the gate no longer counts on: one of an earlier release, which
    /// sends 0, or one that came back from long ago. Its write would reach
    /// the replicas of a layout reads no longer take, and is refused. A
    /// repair copy carries the HLC and version of what it copies, which last
    /// writer wins orders at any stage, and is taken.
    pub fn fences(self, request_stage: u32, repair_copy: bool) -> bool {
        !repair_copy && request_stage.saturating_add(1) < u32::from(self.number())
    }

    /// What a write needs and what a read takes at this stage, moving from
    /// `old` to `new`.
    pub fn rules(self, old: QuorumShape, new: QuorumShape) -> StageRules {
        let old_write = QuorumRequirement {
            side: ReplicaSide::Old,
            count: old.write_quorum,
        };
        let new_write = QuorumRequirement {
            side: ReplicaSide::New,
            count: new.write_quorum,
        };
        let old_read = QuorumRequirement {
            side: ReplicaSide::Old,
            count: old.read_quorum,
        };
        let new_read = QuorumRequirement {
            side: ReplicaSide::New,
            count: new.read_quorum,
        };
        match self {
            Self::Legacy => StageRules {
                write: vec![old_write],
                read: old_read,
            },
            Self::DualWrite => StageRules {
                write: vec![old_write, new_write],
                read: old_read,
            },
            Self::ReadNew => StageRules {
                write: vec![old_write, new_write],
                read: new_read,
            },
            Self::Settled => StageRules {
                write: vec![new_write],
                read: new_read,
            },
        }
    }
}

impl std::fmt::Display for Stage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({})", self.number(), self.as_str())
    }
}

/// Which layout's replicas a requirement counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReplicaSide {
    /// The replicas of the layout the pool moves from.
    Old,
    /// The replicas of the layout the pool moves to.
    New,
}

/// So many acknowledgements, or answers, from the replicas of one layout.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct QuorumRequirement {
    /// The layout whose replicas count.
    pub side: ReplicaSide,
    /// How many of them.
    pub count: u8,
}

/// The rules of one stage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StageRules {
    /// A write succeeds when every one of these holds.
    pub write: Vec<QuorumRequirement>,
    /// A read waits for this many answers from these replicas.
    pub read: QuorumRequirement,
}

/// Where a node is in the move of its pool's bucket metadata: from which
/// layout to which, at what stage, and whether its backfill of the new
/// replicas finished. Stored in `cluster_meta` and announced in gossip.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlacementRecord {
    /// The shape the pool moves from.
    pub from: QuorumShape,
    /// The shape the pool moves to.
    pub to: QuorumShape,
    /// The stage of this node.
    pub stage: Stage,
    /// Whether this node brought every bucket metadata key it holds to the
    /// new replicas.
    pub backfill_complete: bool,
}

/// Why a node keeps a record that is not the target it derived.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlacementNote {
    /// The move under way finishes before the pool moves on to the new
    /// target.
    TargetDeferred {
        /// The target this start derived.
        target: QuorumShape,
    },
    /// The pool already holds its bucket metadata on more replicas than the
    /// target; moving to fewer is not supported, so it keeps them.
    LoweringRefused {
        /// The target this start derived.
        target: QuorumShape,
    },
}

/// The layout of a record in `cluster_meta`.
const RECORD_FORMAT: u8 = 1;
/// The length of an encoded record.
const RECORD_LEN: usize = 9;

impl PlacementRecord {
    /// A pool placed by `shape`, with nothing to move.
    pub fn settled(shape: QuorumShape) -> Self {
        Self {
            from: shape,
            to: shape,
            stage: Stage::Settled,
            backfill_complete: true,
        }
    }

    /// The move from `from` to `to`, not started yet.
    pub fn starting(from: QuorumShape, to: QuorumShape) -> Self {
        Self {
            from,
            to,
            stage: Stage::Legacy,
            backfill_complete: false,
        }
    }

    /// The record a node starts with, from what it `stored` before, the shape
    /// of its pool's `data` and the `target` it derived now; and why it is
    /// not the target, if it is not.
    ///
    /// A node that stored nothing starts the move from the data's shape, or
    /// is settled at once when the target is that shape. A stored move goes
    /// on to its own end: a node never steps back, and a new target waits
    /// until the pool settled the move under way. A settled pool moves on to
    /// a larger target, and keeps its replicas against a smaller one.
    pub fn resume(
        stored: Option<Self>,
        data: QuorumShape,
        target: QuorumShape,
    ) -> (Self, Option<PlacementNote>) {
        let Some(stored) = stored else {
            if target == data {
                return (Self::settled(data), None);
            }
            return (Self::starting(data, target), None);
        };
        if stored.to == target {
            return (stored, None);
        }
        if stored.stage != Stage::Settled {
            return (stored, Some(PlacementNote::TargetDeferred { target }));
        }
        if target.replication_factor > stored.to.replication_factor {
            return (Self::starting(stored.to, target), None);
        }
        (stored, Some(PlacementNote::LoweringRefused { target }))
    }

    /// The same move at the next stage, `None` once settled.
    pub fn advanced(self) -> Option<Self> {
        Some(Self {
            stage: self.stage.next()?,
            ..self
        })
    }

    /// Whether the two records describe the same move.
    pub fn same_move(&self, other: &Self) -> bool {
        self.from == other.from && self.to == other.to
    }

    /// Whether this record may replace `stored` in `cluster_meta`: the same
    /// move at the same stage or later, without losing a finished backfill,
    /// or the next move after `stored` settled.
    pub fn follows(&self, stored: &Self) -> bool {
        if self.same_move(stored) {
            return self.stage >= stored.stage
                && (self.backfill_complete || !stored.backfill_complete);
        }
        stored.stage == Stage::Settled
            && self.from == stored.to
            && self.to.replication_factor > stored.to.replication_factor
    }

    /// The bytes of the record in `cluster_meta`.
    pub fn encode(&self) -> Vec<u8> {
        vec![
            RECORD_FORMAT,
            self.from.replication_factor,
            self.from.write_quorum,
            self.from.read_quorum,
            self.to.replication_factor,
            self.to.write_quorum,
            self.to.read_quorum,
            self.stage.number(),
            u8::from(self.backfill_complete),
        ]
    }

    /// The record stored as `bytes`.
    ///
    /// # Errors
    ///
    /// Bytes of another format, length or stage: a record this release
    /// cannot read is not guessed at.
    pub fn decode(bytes: &[u8]) -> Result<Self, ClusterError> {
        let invalid = |why: &str| {
            ClusterError::Config(format!(
                "the stored bucket metadata placement cannot be read: {why}"
            ))
        };
        if bytes.len() != RECORD_LEN || bytes[0] != RECORD_FORMAT {
            return Err(invalid(&format!(
                "{} byte(s) of format {:?}, expected {RECORD_LEN} of format {RECORD_FORMAT}",
                bytes.len(),
                bytes.first()
            )));
        }
        let shape = |at: usize| QuorumShape {
            replication_factor: bytes[at],
            write_quorum: bytes[at + 1],
            read_quorum: bytes[at + 2],
        };
        let stage = Stage::from_number(bytes[7])
            .ok_or_else(|| invalid(&format!("stage {} is not one of 0 to 3", bytes[7])))?;
        let backfill_complete = match bytes[8] {
            0 => false,
            1 => true,
            other => return Err(invalid(&format!("backfill flag {other} is not 0 or 1"))),
        };
        Ok(Self {
            from: shape(1),
            to: shape(4),
            stage,
            backfill_complete,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn the_stages_pass_in_order_and_end_settled() {
        let mut record = PlacementRecord::starting(S3, S5);
        let mut seen = vec![record.stage];
        while let Some(next) = record.advanced() {
            assert!(next.follows(&record));
            assert!(!record.follows(&next) || record == next);
            record = next;
            seen.push(record.stage);
        }
        assert_eq!(seen, Stage::ALL);
        for stage in Stage::ALL {
            assert_eq!(Stage::from_number(stage.number()), Some(stage));
        }
        assert_eq!(Stage::from_number(4), None);
    }

    /// A replica refuses a client's write two stages behind it or more —
    /// an earlier release sends 0 — and takes a repair copy at any stage.
    #[test]
    fn a_replica_fences_the_writes_of_a_stage_too_old() {
        for stage in Stage::ALL {
            for request in 0..=3u32 {
                let fenced = request + 1 < u32::from(stage.number());
                assert_eq!(stage.fences(request, false), fenced, "{stage} {request}");
                assert!(!stage.fences(request, true), "{stage} {request}");
            }
        }
        assert!(
            !Stage::DualWrite.fences(0, false),
            "stage 1 takes an earlier release"
        );
        assert!(
            Stage::ReadNew.fences(0, false),
            "stage 2 refuses an earlier release"
        );
        assert!(!Stage::ReadNew.fences(1, false));
        assert!(Stage::Settled.fences(1, false));
    }

    #[test]
    fn the_rules_are_the_table_of_the_plan() {
        let old_write = QuorumRequirement {
            side: ReplicaSide::Old,
            count: 2,
        };
        let new_write = QuorumRequirement {
            side: ReplicaSide::New,
            count: 3,
        };
        let old_read = old_write;
        let new_read = new_write;
        assert_eq!(
            Stage::Legacy.rules(S3, S5),
            StageRules {
                write: vec![old_write],
                read: old_read
            }
        );
        assert_eq!(
            Stage::DualWrite.rules(S3, S5),
            StageRules {
                write: vec![old_write, new_write],
                read: old_read
            }
        );
        assert_eq!(
            Stage::ReadNew.rules(S3, S5),
            StageRules {
                write: vec![old_write, new_write],
                read: new_read
            }
        );
        assert_eq!(
            Stage::Settled.rules(S3, S5),
            StageRules {
                write: vec![new_write],
                read: new_read
            }
        );
    }

    /// A node without a record starts the move, or is settled when there is
    /// nothing to move; a stored record goes on, and never steps back.
    #[test]
    fn a_node_resumes_where_it_stored_it_was() {
        assert_eq!(
            PlacementRecord::resume(None, S3, S3),
            (PlacementRecord::settled(S3), None)
        );
        assert_eq!(
            PlacementRecord::resume(None, S3, S5),
            (PlacementRecord::starting(S3, S5), None)
        );
        let mid = PlacementRecord {
            stage: Stage::ReadNew,
            backfill_complete: true,
            ..PlacementRecord::starting(S3, S5)
        };
        assert_eq!(PlacementRecord::resume(Some(mid), S3, S5), (mid, None));
        // A higher target waits until the move under way settled.
        assert_eq!(
            PlacementRecord::resume(Some(mid), S3, S6),
            (mid, Some(PlacementNote::TargetDeferred { target: S6 }))
        );
        let settled = PlacementRecord {
            stage: Stage::Settled,
            ..mid
        };
        assert_eq!(
            PlacementRecord::resume(Some(settled), S3, S6),
            (PlacementRecord::starting(S5, S6), None)
        );
        // Fewer replicas than the pool holds are refused, the record kept.
        assert_eq!(
            PlacementRecord::resume(Some(settled), S3, S3),
            (settled, Some(PlacementNote::LoweringRefused { target: S3 }))
        );
    }

    #[test]
    fn a_record_only_moves_forward() {
        let legacy = PlacementRecord::starting(S3, S5);
        let dual = legacy.advanced().unwrap();
        let backfilled = PlacementRecord {
            backfill_complete: true,
            ..dual
        };
        assert!(dual.follows(&legacy));
        assert!(backfilled.follows(&dual));
        assert!(
            !dual.follows(&backfilled),
            "a finished backfill is not forgotten"
        );
        assert!(!legacy.follows(&dual), "no stage back");
        let settled = PlacementRecord {
            stage: Stage::Settled,
            ..backfilled
        };
        assert!(PlacementRecord::starting(S5, S6).follows(&settled));
        assert!(
            !PlacementRecord::starting(S5, S6).follows(&backfilled),
            "not before it settled"
        );
        assert!(
            !PlacementRecord::settled(S3).follows(&settled),
            "no move to fewer replicas"
        );
    }

    #[test]
    fn a_record_reads_back_as_written_and_nothing_else_reads() {
        for stage in Stage::ALL {
            for backfill_complete in [false, true] {
                let record = PlacementRecord {
                    stage,
                    backfill_complete,
                    ..PlacementRecord::starting(S3, S5)
                };
                assert_eq!(PlacementRecord::decode(&record.encode()).unwrap(), record);
            }
        }
        let good = PlacementRecord::settled(S3).encode();
        let mut later_format = good.clone();
        later_format[0] = 2;
        let mut bad_stage = good.clone();
        bad_stage[7] = 4;
        let mut bad_flag = good.clone();
        bad_flag[8] = 2;
        for bytes in [
            &good[..8],
            &later_format[..],
            &bad_stage[..],
            &bad_flag[..],
            &[][..],
        ] {
            assert!(PlacementRecord::decode(bytes).is_err(), "{bytes:?}");
        }
    }
}
