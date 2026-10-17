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

//! Joint placement ranges for an append-only pool generation transition.
use super::hash_ring::DEFAULT_VNODES_PER_NODE;
use super::{ClassLayout, ConsistentHashRing, PlacementStrategy, PoolLayout, ReplicationClass};
use crate::clock::hlc::HlcTimestamp;
use crate::{ClusterError, NodeId};
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::RwLock;

/// Monotonic progress of a union-ring arc.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum RangeState {
    /// No copying has started.
    Pending,
    /// Copying is in progress.
    Streaming,
    /// Both generations have been compared.
    Verified,
    /// This node reads the new generation.
    CutOver,
}
/// A clockwise arc `(lower_exclusive, upper_inclusive]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GenerationRange {
    /// Excluded lower endpoint; greater than upper for a wrapping arc.
    pub lower_exclusive: u64,
    /// Included upper endpoint, also the stable range identifier.
    pub upper_inclusive: u64,
    /// Local progress, advanced explicitly by the migration driver.
    pub state: RangeState,
}
impl GenerationRange {
    /// Whether the arc contains this ring position. Equal endpoints cover the ring.
    pub fn contains(&self, position: u64) -> bool {
        if self.lower_exclusive < self.upper_inclusive {
            self.lower_exclusive < position && position <= self.upper_inclusive
        } else {
            position > self.lower_exclusive || position <= self.upper_inclusive
        }
    }
}
/// Where the pool stands in its growth, as this node applies it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum GenerationPhase {
    /// A successor is prepared; the current owners alone place every key.
    Prepared,
    /// The move began: writes reach both generations' quorums, reads follow
    /// each arc's cut-over.
    Moving,
    /// The pool settled on its successor: it alone places every key.
    Finished,
}

impl GenerationPhase {
    fn code(self) -> u8 {
        match self {
            Self::Prepared => 0,
            Self::Moving => 1,
            Self::Finished => 2,
        }
    }

    fn of_code(code: u8) -> Self {
        match code {
            0 => Self::Prepared,
            1 => Self::Moving,
            _ => Self::Finished,
        }
    }
}

/// How many arcs are in each state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RangeCounts {
    /// Not begun.
    pub pending: usize,
    /// Being moved.
    pub streaming: usize,
    /// Compared, not yet read through the successor.
    pub verified: usize,
    /// Read through the successor.
    pub cut_over: usize,
}

impl RangeCounts {
    /// Every arc.
    pub fn total(&self) -> usize {
        self.pending + self.streaming + self.verified + self.cut_over
    }
}

/// Held by a placement decision that reads an arc through the old owners:
/// the common barrier before the pool settles waits until none is left
/// (`EPGR-04-2`).
#[derive(Debug)]
pub struct OldReadTicket {
    count: std::sync::Arc<AtomicU64>,
}

impl Drop for OldReadTicket {
    fn drop(&mut self) {
        self.count.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Shared local range progress. Cloned routers observe the same transitions.
#[derive(Debug)]
pub struct GenerationRanges {
    ranges: RwLock<Vec<GenerationRange>>,
    phase: AtomicU8,
    old_reads: std::sync::Arc<AtomicU64>,
    /// When this node settled on the successor: anti-entropy compared the
    /// successor's replicas only in rounds begun since.
    finished_at: RwLock<Option<HlcTimestamp>>,
}
impl GenerationRanges {
    pub(crate) fn new(old: &[NodeId], new: &[NodeId]) -> Result<Self, ClusterError> {
        let mut positions =
            ConsistentHashRing::new(old.to_vec(), 1, DEFAULT_VNODES_PER_NODE)?.vnode_positions();
        positions.extend(
            ConsistentHashRing::new(new.to_vec(), 1, DEFAULT_VNODES_PER_NODE)?.vnode_positions(),
        );
        positions.sort_unstable();
        positions.dedup();
        let count = positions.len();
        let ranges = positions
            .iter()
            .enumerate()
            .map(|(index, &upper)| GenerationRange {
                lower_exclusive: positions[(index + count - 1) % count],
                upper_inclusive: upper,
                state: RangeState::Pending,
            })
            .collect();
        Ok(Self {
            ranges: RwLock::new(ranges),
            phase: AtomicU8::new(GenerationPhase::Prepared.code()),
            old_reads: std::sync::Arc::new(AtomicU64::new(0)),
            finished_at: RwLock::new(None),
        })
    }

    /// Where the pool stands, as this node applies it.
    pub fn phase(&self) -> GenerationPhase {
        GenerationPhase::of_code(self.phase.load(Ordering::Acquire))
    }

    /// The move began on this node: writes reach both generations from now
    /// on. Idempotent; a finished pool stays finished.
    pub fn begin(&self) {
        let _ = self.phase.compare_exchange(
            GenerationPhase::Prepared.code(),
            GenerationPhase::Moving.code(),
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }

    /// The pool settled on the successor on this node at `at`.
    ///
    /// # Errors
    ///
    /// An arc not yet cut over here, or a move that has not begun.
    pub fn finish(&self, at: HlcTimestamp) -> Result<(), ClusterError> {
        if self.phase() == GenerationPhase::Finished {
            return Ok(());
        }
        if self.phase() != GenerationPhase::Moving {
            return Err(ClusterError::Placement(
                "a pool settles on its successor only after its move began".into(),
            ));
        }
        if !self.all_cut_over() {
            return Err(ClusterError::Placement(
                "every range must cut over before the pool settles on its successor".into(),
            ));
        }
        *self.finished_at.write().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(at);
        self.phase.store(GenerationPhase::Finished.code(), Ordering::Release);
        Ok(())
    }

    /// When this node settled on the successor, once it did.
    pub fn finished_at(&self) -> Option<HlcTimestamp> {
        *self.finished_at.read().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Placement decisions in flight that read through the old owners.
    pub fn old_reads_in_flight(&self) -> u64 {
        self.old_reads.load(Ordering::Acquire)
    }

    pub(crate) fn old_read_ticket(&self) -> OldReadTicket {
        self.old_reads.fetch_add(1, Ordering::AcqRel);
        OldReadTicket {
            count: self.old_reads.clone(),
        }
    }

    /// How many arcs are in each state.
    pub fn counts(&self) -> RangeCounts {
        let ranges = self.ranges.read().unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut counts = RangeCounts::default();
        for range in ranges.iter() {
            match range.state {
                RangeState::Pending => counts.pending += 1,
                RangeState::Streaming => counts.streaming += 1,
                RangeState::Verified => counts.verified += 1,
                RangeState::CutOver => counts.cut_over += 1,
            }
        }
        counts
    }

    /// Whether every arc reads through the successor.
    pub fn all_cut_over(&self) -> bool {
        self.ranges
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .all(|range| range.state == RangeState::CutOver)
    }

    /// The upper endpoints of the arcs cut over here.
    pub fn cut_over_ranges(&self) -> Vec<u64> {
        self.ranges
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .filter(|range| range.state == RangeState::CutOver)
            .map(|range| range.upper_inclusive)
            .collect()
    }

    /// Advances the arc ending at `upper_inclusive` to `state` through every
    /// step between; an arc already there or beyond stays. Returns whether it
    /// moved.
    ///
    /// # Errors
    ///
    /// An unknown endpoint.
    pub fn advance_to(
        &self,
        upper_inclusive: u64,
        state: RangeState,
    ) -> Result<bool, ClusterError> {
        let mut ranges = self.ranges.write().unwrap_or_else(std::sync::PoisonError::into_inner);
        let range = ranges
            .iter_mut()
            .find(|range| range.upper_inclusive == upper_inclusive)
            .ok_or_else(|| ClusterError::Placement("unknown generation range".into()))?;
        if range.state >= state {
            return Ok(false);
        }
        range.state = state;
        Ok(true)
    }
    /// A consistent snapshot of all arcs in upper-endpoint order.
    pub fn snapshot(&self) -> Vec<GenerationRange> {
        self.ranges.read().unwrap_or_else(std::sync::PoisonError::into_inner).clone()
    }
    /// The arc containing `position`.
    pub fn range_at(&self, position: u64) -> GenerationRange {
        let ranges = self.ranges.read().unwrap_or_else(std::sync::PoisonError::into_inner);
        let index = ranges.partition_point(|range| range.upper_inclusive < position);
        ranges[index % ranges.len()].clone()
    }
    /// Advances exactly one step; repeating the current state is idempotent.
    ///
    /// # Errors
    /// Unknown endpoint, skipped step or regression.
    pub fn transition(&self, upper_inclusive: u64, next: RangeState) -> Result<(), ClusterError> {
        let mut ranges = self.ranges.write().unwrap_or_else(std::sync::PoisonError::into_inner);
        let range = ranges
            .iter_mut()
            .find(|range| range.upper_inclusive == upper_inclusive)
            .ok_or_else(|| ClusterError::Placement("unknown generation range".into()))?;
        if next != range.state && next as u8 != range.state as u8 + 1 {
            return Err(ClusterError::Placement(
                "generation range must advance one step".into(),
            ));
        }
        range.state = next;
        Ok(())
    }
}

pub(super) fn layout_for(
    nodes: &[NodeId],
    inherited: &PoolLayout,
) -> Result<PoolLayout, ClusterError> {
    let build = |layout: &ClassLayout| {
        ClassLayout::new(
            PlacementStrategy::new(nodes.to_vec(), layout.replication_factor())?,
            layout.write_quorum(),
            layout.read_quorum(),
        )
    };
    let metadata = inherited.bucket_metadata_move().map_or_else(
        || inherited.class(ReplicationClass::BucketMetadata),
        |moving| moving.new_layout(),
    );
    Ok(PoolLayout::uniform(build(inherited.data())?).with_bucket_metadata_layout(build(metadata)?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn union_arcs_cover_every_endpoint_once_including_wrap() {
        let old = [NodeId(1), NodeId(2), NodeId(3)];
        let new = (1..=6).map(NodeId).collect::<Vec<_>>();
        let ranges = GenerationRanges::new(&old, &new).unwrap();
        let snapshot = ranges.snapshot();
        assert_eq!(snapshot.len(), 6 * 128);
        for range in &snapshot {
            for position in [range.upper_inclusive, range.lower_exclusive, 0, u64::MAX] {
                assert_eq!(
                    snapshot.iter().filter(|arc| arc.contains(position)).count(),
                    1
                );
                assert!(ranges.range_at(position).contains(position));
            }
        }
        let first = &snapshot[0];
        assert!(first.lower_exclusive > first.upper_inclusive);
        assert!(ranges.transition(first.upper_inclusive, RangeState::Verified).is_err());
        ranges.transition(first.upper_inclusive, RangeState::Pending).unwrap();
        ranges.transition(first.upper_inclusive, RangeState::Streaming).unwrap();
        assert!(ranges.transition(first.upper_inclusive, RangeState::Pending).is_err());
        assert!(ranges.transition(0, RangeState::Streaming).is_err());
    }

    /// A move begins once, settles only after every arc cut over, and
    /// counts the decisions in flight that read through the old owners.
    #[test]
    fn the_phase_advances_only_forwards_and_settles_after_every_cut_over() {
        let old = [NodeId(1), NodeId(2), NodeId(3)];
        let new = (1..=6).map(NodeId).collect::<Vec<_>>();
        let ranges = GenerationRanges::new(&old, &new).unwrap();
        let at = HlcTimestamp::from_record(1_000, 0, 1);
        assert_eq!(ranges.phase(), GenerationPhase::Prepared);
        assert!(ranges.finish(at).is_err());
        ranges.begin();
        ranges.begin();
        assert_eq!(ranges.phase(), GenerationPhase::Moving);
        assert!(ranges.finish(at).is_err());
        let snapshot = ranges.snapshot();
        let first = snapshot[0].upper_inclusive;
        assert!(ranges.advance_to(first, RangeState::CutOver).unwrap());
        assert!(!ranges.advance_to(first, RangeState::Streaming).unwrap());
        assert_eq!(ranges.counts().cut_over, 1);
        assert_eq!(ranges.counts().total(), snapshot.len());
        assert_eq!(ranges.cut_over_ranges(), vec![first]);
        assert!(ranges.advance_to(0, RangeState::CutOver).is_err());
        let ticket = ranges.old_read_ticket();
        let second = ranges.old_read_ticket();
        assert_eq!(ranges.old_reads_in_flight(), 2);
        drop(ticket);
        drop(second);
        assert_eq!(ranges.old_reads_in_flight(), 0);
        for range in &snapshot {
            ranges.advance_to(range.upper_inclusive, RangeState::CutOver).unwrap();
        }
        assert!(ranges.all_cut_over());
        assert_eq!(ranges.finished_at(), None);
        ranges.finish(at).unwrap();
        ranges.begin();
        assert_eq!(ranges.phase(), GenerationPhase::Finished);
        assert_eq!(ranges.finished_at(), Some(at));
    }
}
