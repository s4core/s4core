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

//! Where this node stands in moving each part of the pool's keys, kept in
//! `cluster_meta` so that a restart continues from it.
//!
//! The move is split into work items: every arc of the union ring
//! ([`GenerationRange`]), the replicated records of `__system__` that have no
//! version digests, and the open multipart uploads. Each item is walked in
//! passes. A pass reads the item's keys a page at a time and settles each
//! one; the cursor — the last key of the page — is stored with the page's
//! counts, so a restart repeats at most one page. A key that could not be
//! settled is kept to be tried again after the pass; when more of them fail
//! than are kept, the next pass walks the whole item again. An item is done
//! once a pass, and the retries after it, left no key behind.

use std::collections::BTreeMap;
use std::sync::Mutex;

use s4_core::storage::IndexDb;
use serde::{Deserialize, Serialize};

use crate::placement::GenerationRange;
use crate::{ClusterError, PoolId};

/// Keys kept to be tried again after a pass; beyond them the item is walked
/// again from the start.
pub const MAX_RETRY_KEYS: usize = 1024;

/// One part of the move this node walks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum WorkItem {
    /// An arc of the union ring, by its endpoints.
    Range {
        /// Excluded lower endpoint.
        lower_exclusive: u64,
        /// Included upper endpoint, the arc's identifier.
        upper_inclusive: u64,
    },
    /// The replicated records of `__system__` without version digests
    /// ([`super::families::SYSTEM_RECORD_PREFIXES`]).
    SystemRecords,
    /// The open multipart uploads this node holds.
    MultipartUploads,
}

impl WorkItem {
    /// The item of `range`.
    pub fn of_range(range: &GenerationRange) -> Self {
        Self::Range {
            lower_exclusive: range.lower_exclusive,
            upper_inclusive: range.upper_inclusive,
        }
    }

    /// The name its progress is stored under, within the move to `to_epoch`.
    /// Ranges are named by their zero-padded upper endpoint, so names sort as
    /// the ring does.
    pub fn record_name(&self, to_epoch: u64) -> String {
        match self {
            Self::Range {
                upper_inclusive, ..
            } => format!("{to_epoch}/range/{upper_inclusive:020}"),
            Self::SystemRecords => format!("{to_epoch}/system"),
            Self::MultipartUploads => format!("{to_epoch}/multipart"),
        }
    }

    /// The ring positions of a range's keys as the version digests are read:
    /// `[from, to)` intervals, `None` for the end of the ring. A wrapping
    /// arc is two of them, and an arc whose endpoints are equal is the whole
    /// ring. Empty for an item that is not a range.
    pub fn segments(&self) -> Vec<(u64, Option<u64>)> {
        let Self::Range {
            lower_exclusive: lower,
            upper_inclusive: upper,
        } = *self
        else {
            return Vec::new();
        };
        let past = |position: u64| position.checked_add(1);
        if lower < upper {
            return vec![(lower + 1, past(upper))];
        }
        let mut segments = Vec::with_capacity(2);
        if let Some(from) = past(lower) {
            segments.push((from, None));
        }
        segments.push((0, past(upper)));
        segments
    }
}

/// Where a pass over an item stands.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum Phase {
    /// Reading the item's keys: segment `segment` (a prefix, for the
    /// records of `__system__`), after the key `after`.
    Scan {
        /// Index of the segment being read.
        segment: u32,
        /// The last key settled in it, if any.
        after: Option<String>,
    },
    /// Trying again the keys the pass could not settle.
    Retry,
    /// Every key the item held was settled.
    Done,
}

/// This node's progress in one item.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ItemProgress {
    /// Encoding version.
    pub format: u32,
    /// Where the current pass stands.
    pub phase: Phase,
    /// Passes begun over the whole item.
    pub passes: u32,
    /// Keys of the pass to try again.
    pub retry: Vec<String>,
    /// More keys failed than are kept: the item is walked again.
    pub retry_overflow: bool,
    /// Keys read so far.
    pub scanned: u64,
    /// Keys at least one replica received a version of.
    pub moved: u64,
    /// Bytes of copies delivered.
    pub bytes: u64,
    /// Why the last key that failed did.
    pub last_error: Option<String>,
}

impl Default for ItemProgress {
    fn default() -> Self {
        Self {
            format: 1,
            phase: Phase::Scan {
                segment: 0,
                after: None,
            },
            passes: 0,
            retry: Vec::new(),
            retry_overflow: false,
            scanned: 0,
            moved: 0,
            bytes: 0,
            last_error: None,
        }
    }
}

impl ItemProgress {
    /// Whether every key of the item was settled.
    pub fn is_done(&self) -> bool {
        self.phase == Phase::Done
    }

    /// Keeps `key` to be tried again, or marks the item to be walked again
    /// when the keys kept are full.
    pub fn failed(&mut self, key: &str, why: String) {
        if !self.retry.iter().any(|kept| kept == key) {
            if self.retry.len() < MAX_RETRY_KEYS {
                self.retry.push(key.to_string());
            } else {
                self.retry_overflow = true;
            }
        }
        self.last_error = Some(why);
    }

    /// The pass read the last of the item's keys: the keys it could not
    /// settle are tried again, or, when there were more than are kept, the
    /// whole item is walked again.
    pub fn end_scan(&mut self) {
        self.phase = if self.retry_overflow {
            self.retry.clear();
            self.retry_overflow = false;
            self.passes += 1;
            Phase::Scan {
                segment: 0,
                after: None,
            }
        } else if self.retry.is_empty() {
            Phase::Done
        } else {
            Phase::Retry
        };
    }

    /// The keys tried again left `still` behind.
    pub fn end_retry(&mut self, still: Vec<String>) {
        self.retry = still;
        if self.retry.is_empty() {
            self.phase = Phase::Done;
        }
    }

    /// Encodes the record.
    pub fn encode(&self) -> Result<Vec<u8>, ClusterError> {
        serde_json::to_vec(self).map_err(|e| ClusterError::Codec(e.to_string()))
    }

    /// Decodes a stored record.
    pub fn decode(bytes: &[u8]) -> Result<Self, ClusterError> {
        let progress: Self = serde_json::from_slice(bytes)
            .map_err(|e| ClusterError::Codec(format!("rebalance progress: {e}")))?;
        if progress.format != 1 {
            return Err(ClusterError::Codec(format!(
                "rebalance progress: unsupported format {}",
                progress.format
            )));
        }
        Ok(progress)
    }
}

/// The record of the move as a whole on this node: which move it is, and
/// whether an operator paused it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MoveControl {
    /// Encoding version.
    pub format: u32,
    /// The pool moving.
    pub pool_id: PoolId,
    /// Epoch of the generation the keys move from.
    pub from_epoch: u64,
    /// Epoch of the generation the keys move to.
    pub to_epoch: u64,
    /// Why the move is paused, while it is.
    pub paused: Option<String>,
}

impl MoveControl {
    /// The name the record is stored under.
    pub fn record_name(to_epoch: u64) -> String {
        format!("{to_epoch}/control")
    }

    /// Encodes the record.
    pub fn encode(&self) -> Result<Vec<u8>, ClusterError> {
        serde_json::to_vec(self).map_err(|e| ClusterError::Codec(e.to_string()))
    }

    /// Decodes a stored record.
    pub fn decode(bytes: &[u8]) -> Result<Self, ClusterError> {
        serde_json::from_slice(bytes)
            .map_err(|e| ClusterError::Codec(format!("rebalance control: {e}")))
    }
}

/// Where the progress of the move is kept.
#[async_trait::async_trait]
pub trait ProgressStore: Send + Sync {
    /// Every stored record, by name.
    async fn load(&self) -> Result<Vec<(String, Vec<u8>)>, ClusterError>;

    /// Stores one record durably before returning.
    async fn save(&self, name: &str, record: Vec<u8>) -> Result<(), ClusterError>;
}

/// The progress kept in the node's `cluster_meta`.
pub struct IndexProgressStore {
    index: std::sync::Arc<IndexDb>,
}

impl IndexProgressStore {
    /// Keeps the progress in `index`.
    pub fn new(index: std::sync::Arc<IndexDb>) -> Self {
        Self { index }
    }
}

#[async_trait::async_trait]
impl ProgressStore for IndexProgressStore {
    async fn load(&self) -> Result<Vec<(String, Vec<u8>)>, ClusterError> {
        self.index
            .rebalance_entries()
            .await
            .map_err(|e| ClusterError::Storage(e.to_string()))
    }

    async fn save(&self, name: &str, record: Vec<u8>) -> Result<(), ClusterError> {
        self.index
            .store_rebalance_entry(name, record)
            .await
            .map_err(|e| ClusterError::Storage(e.to_string()))
    }
}

/// Progress kept in memory: for tests, and for a node without durable state.
#[derive(Default)]
pub struct MemoryProgressStore {
    records: Mutex<BTreeMap<String, Vec<u8>>>,
}

#[async_trait::async_trait]
impl ProgressStore for MemoryProgressStore {
    async fn load(&self) -> Result<Vec<(String, Vec<u8>)>, ClusterError> {
        let records = self.records.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        Ok(records.iter().map(|(name, record)| (name.clone(), record.clone())).collect())
    }

    async fn save(&self, name: &str, record: Vec<u8>) -> Result<(), ClusterError> {
        self.records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(name.to_string(), record);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn range(lower: u64, upper: u64) -> WorkItem {
        WorkItem::Range {
            lower_exclusive: lower,
            upper_inclusive: upper,
        }
    }

    /// Every position of an arc falls in exactly one of its segments, a
    /// wrapping arc and the arc of the whole ring included.
    #[test]
    fn segments_cover_exactly_the_positions_of_the_arc() {
        let covers = |item: WorkItem, position: u64| {
            item.segments()
                .iter()
                .filter(|(from, to)| position >= *from && to.is_none_or(|to| position < to))
                .count()
        };
        let plain = range(10, 20);
        assert_eq!(plain.segments(), vec![(11, Some(21))]);
        for (position, inside) in [(10, 0), (11, 1), (20, 1), (21, 0)] {
            assert_eq!(covers(plain, position), inside, "{position}");
        }
        let wrapping = range(u64::MAX - 5, 3);
        for (position, inside) in [
            (u64::MAX - 5, 0),
            (u64::MAX - 4, 1),
            (u64::MAX, 1),
            (0, 1),
            (3, 1),
            (4, 0),
        ] {
            assert_eq!(covers(wrapping, position), inside, "{position}");
        }
        let at_the_top = range(u64::MAX, 7);
        assert_eq!(at_the_top.segments(), vec![(0, Some(8))]);
        let to_the_top = range(7, u64::MAX);
        assert_eq!(to_the_top.segments(), vec![(8, None)]);
        let whole = range(42, 42);
        for position in [0, 41, 42, 43, u64::MAX] {
            assert_eq!(covers(whole, position), 1, "{position}");
        }
        assert!(WorkItem::SystemRecords.segments().is_empty());
    }

    #[test]
    fn range_names_sort_as_the_ring_does() {
        let names: Vec<String> = [range(0, 9), range(9, 10), range(10, u64::MAX)]
            .iter()
            .map(|r| r.record_name(2))
            .collect();
        let mut sorted = names.clone();
        sorted.sort();
        assert_eq!(names, sorted);
        assert_eq!(WorkItem::SystemRecords.record_name(2), "2/system");
        assert_eq!(MoveControl::record_name(2), "2/control");
    }

    /// A pass with failures is followed by retries of exactly those keys;
    /// too many failures walk the whole item again; an item is done only
    /// once nothing is left behind.
    #[test]
    fn a_pass_ends_done_retrying_or_walking_again() {
        let mut clean = ItemProgress::default();
        clean.end_scan();
        assert!(clean.is_done());

        let mut some = ItemProgress::default();
        some.failed("a", "down".into());
        some.failed("a", "down again".into());
        some.failed("b", "down".into());
        some.end_scan();
        assert_eq!(some.phase, Phase::Retry);
        assert_eq!(some.retry, ["a", "b"]);
        assert_eq!(some.last_error.as_deref(), Some("down"));
        some.end_retry(vec!["b".into()]);
        assert_eq!(some.phase, Phase::Retry);
        some.end_retry(Vec::new());
        assert!(some.is_done());

        let mut many = ItemProgress::default();
        for n in 0..=MAX_RETRY_KEYS {
            many.failed(&n.to_string(), "down".into());
        }
        assert!(many.retry_overflow);
        many.end_scan();
        assert_eq!(
            many.phase,
            Phase::Scan {
                segment: 0,
                after: None
            }
        );
        assert!(many.retry.is_empty() && !many.retry_overflow);
        assert_eq!(many.passes, 1);
    }

    #[test]
    fn records_round_trip_and_refuse_unknown_formats() {
        let mut progress = ItemProgress {
            phase: Phase::Scan {
                segment: 1,
                after: Some("bucket/key".into()),
            },
            bytes: 7,
            ..ItemProgress::default()
        };
        assert_eq!(
            ItemProgress::decode(&progress.encode().unwrap()).unwrap(),
            progress
        );
        progress.format = 2;
        assert!(ItemProgress::decode(&progress.encode().unwrap()).is_err());
        let control = MoveControl {
            format: 1,
            pool_id: PoolId(0),
            from_epoch: 1,
            to_epoch: 2,
            paused: Some("operator".into()),
        };
        assert_eq!(
            MoveControl::decode(&control.encode().unwrap()).unwrap(),
            control
        );
    }
}
