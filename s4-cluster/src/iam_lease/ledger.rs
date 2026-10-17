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

//! What one node keeps of the lease of IAM changes: the grants it gives and
//! the writes it admits, as a state machine without I/O.
//!
//! Time comes in as milliseconds of the node's own monotonic clock, so the
//! same machine runs on a node ([`super::IamLeaseKeeper`]) and in the model
//! test, which moves time by hand.
//!
//! # The rules
//!
//! - **Grant.** A node grants an epoch above every epoch it has seen to one
//!   holder at a time, for the lease's duration by its own clock. A holder
//!   holds the lease once a majority of the pool granted it the same epoch.
//!   Any two majorities share a node, and that node grants an epoch once, so
//!   no two holders hold one epoch.
//! - **Fencing.** A node refuses a write of an epoch below the highest it has
//!   seen; a write of a higher epoch teaches it that epoch. A node that granted
//!   the epoch refuses its writes once the grant ran out by its own clock, or,
//!   after the holder released it, any write newer than the release.
//! - **Order.** Every write of an epoch carries an HLC inside the epoch's
//!   window: above the floor — the highest HLC any earlier epoch may have
//!   written with, as the granting majority knows it — and below the epoch's
//!   own ceiling. A node refuses a write outside the window. So every write of
//!   a later epoch outranks every write of an earlier one, whichever node
//!   holds it and whatever the clocks did: a holder that woke up after its
//!   lease ran out and left a write on a node no newer holder reached loses to
//!   the newer write when anti-entropy brings the two together. Safety does
//!   not rest on the clocks; only how soon a lease frees up does.

use serde::{Deserialize, Serialize};

use crate::hlc::HlcTimestamp;
use crate::identity::NodeId;

/// The epoch and HLC window a write of a change of IAM is made under.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseFence {
    /// The epoch of the lease.
    pub epoch: u64,
    /// The node holding it.
    pub holder: NodeId,
    /// Every write of the epoch carries an HLC above this one.
    pub floor: HlcTimestamp,
    /// Every write of the epoch carries an HLC below this one.
    pub ceiling: HlcTimestamp,
}

impl LeaseFence {
    /// Whether `hlc` is inside the epoch's window.
    pub fn admits_hlc(&self, hlc: HlcTimestamp) -> bool {
        self.floor < hlc && hlc < self.ceiling
    }
}

/// The epoch a node last granted or learned of, as it keeps it across
/// restarts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct EpochRecord {
    /// The epoch.
    pub epoch: u64,
    /// The node it was granted to.
    pub holder: u128,
    /// The HLC its writes stay below.
    pub ceiling: HlcTimestamp,
    /// Set once the holder released it: the HLC its writes stay at or below.
    pub closing: Option<HlcTimestamp>,
    /// Whether this node granted it, rather than learned of it from a write.
    pub granted: bool,
}

impl EpochRecord {
    /// The highest HLC a write of this epoch may carry, as far as this node
    /// knows.
    fn bound(&self) -> HlcTimestamp {
        self.closing.unwrap_or(self.ceiling)
    }
}

/// What a node keeps of the lease across restarts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct LedgerRecord {
    /// The highest epoch this node granted or learned of.
    pub max_epoch: u64,
    /// The highest HLC a write of an epoch below `current` may carry.
    pub floor: HlcTimestamp,
    /// The epoch of `max_epoch`.
    pub current: Option<EpochRecord>,
}

impl Default for LedgerRecord {
    fn default() -> Self {
        Self {
            max_epoch: 0,
            floor: HlcTimestamp::zero(),
            current: None,
        }
    }
}

impl LedgerRecord {
    /// The highest HLC a write of any epoch this node has seen may carry: the
    /// floor of the next epoch, as far as this node knows.
    pub fn bound(&self) -> HlcTimestamp {
        match &self.current {
            Some(current) => self.floor.max(current.bound()),
            None => self.floor,
        }
    }
}

/// A node's answer to a holder asking for an epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Grant {
    /// Granted: the holder's writes go above `floor`.
    Granted {
        /// The highest HLC an earlier epoch may have written with.
        floor: HlcTimestamp,
    },
    /// Another holder holds this node's grant for `remaining_ms` more.
    Busy {
        /// That holder.
        holder: NodeId,
        /// How much longer, by this node's clock.
        remaining_ms: u64,
    },
    /// The epoch is not above the highest this node has seen.
    Stale,
}

/// A grant a node holds out for a change of IAM under way.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LiveGrant {
    /// The epoch granted.
    pub epoch: u64,
    /// The node it was granted to.
    pub holder: NodeId,
    /// How much longer it runs, by the granting node's clock.
    pub remaining_ms: u64,
}

/// Why a node refused a write of a change of IAM.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fenced {
    /// The write carries no lease at all.
    NoLease,
    /// The write's epoch is below the highest this node has seen.
    EpochBehind {
        /// The write's epoch.
        epoch: u64,
        /// The highest this node has seen.
        max_epoch: u64,
    },
    /// The epoch is this node's current one, granted to another holder.
    OtherHolder,
    /// The write's HLC is outside the window of its epoch.
    OutsideWindow,
    /// This node's grant of the epoch ran out.
    Expired,
    /// The holder released the epoch, and the write is newer than the release.
    Released,
}

impl std::fmt::Display for Fenced {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoLease => write!(f, "a change of IAM without the mutation lease"),
            Self::EpochBehind { epoch, max_epoch } => write!(
                f,
                "IAM lease epoch {epoch} is behind epoch {max_epoch} this node has seen"
            ),
            Self::OtherHolder => write!(f, "IAM lease epoch granted to another node"),
            Self::OutsideWindow => write!(f, "HLC outside the window of its IAM lease epoch"),
            Self::Expired => write!(f, "IAM lease expired on this node"),
            Self::Released => write!(f, "IAM lease released before this write"),
        }
    }
}

/// The lease as one node keeps it.
#[derive(Debug, Clone, Default)]
pub struct LeaseLedger {
    record: LedgerRecord,
    /// When this node's grant of `record.current` runs out, by its own clock;
    /// `None` when it granted nothing since it started, or the grant was
    /// released. A grant from before a restart has run out.
    expires_at_ms: Option<u64>,
}

impl LeaseLedger {
    /// The ledger a node kept before it restarted: whatever it granted then
    /// has run out.
    pub fn from_record(record: LedgerRecord) -> Self {
        Self {
            record,
            expires_at_ms: None,
        }
    }

    /// What survives a restart.
    pub fn record(&self) -> LedgerRecord {
        self.record
    }

    /// The highest epoch this node granted or learned of.
    pub fn max_epoch(&self) -> u64 {
        self.record.max_epoch
    }

    /// The holder this node's grant is live for at `now_ms`, and for how much
    /// longer.
    fn live_grant(&self, now_ms: u64) -> Option<(NodeId, u64)> {
        self.live_at(now_ms).map(|live| (live.holder, live.remaining_ms))
    }

    /// The grant of this node still live at `now_ms`: neither run out nor
    /// released. `None` when it holds none — no change of IAM is under way
    /// that this node knows of.
    pub fn live_at(&self, now_ms: u64) -> Option<LiveGrant> {
        let current = self.record.current?;
        let expires = self.expires_at_ms?;
        (current.granted && current.closing.is_none() && expires > now_ms).then(|| LiveGrant {
            epoch: current.epoch,
            holder: NodeId(current.holder),
            remaining_ms: expires - now_ms,
        })
    }

    /// Asks this node for `epoch` for `holder`, whose writes stay below
    /// `ceiling`, for `duration_ms`. The same holder asking again for the
    /// epoch it holds here renews it: the grant runs for `duration_ms` from
    /// now, below the higher of the two ceilings. Whether the answer changed
    /// what survives a restart is the second value.
    pub fn acquire(
        &mut self,
        holder: NodeId,
        epoch: u64,
        ceiling: HlcTimestamp,
        duration_ms: u64,
        now_ms: u64,
    ) -> (Grant, bool) {
        if let Some(current) = &mut self.record.current {
            let renewal = current.epoch == epoch
                && current.holder == holder.0
                && current.granted
                && current.closing.is_none()
                && self.expires_at_ms.is_some_and(|expires| expires > now_ms);
            if renewal {
                current.ceiling = current.ceiling.max(ceiling);
                self.expires_at_ms = Some(now_ms + duration_ms);
                return (
                    Grant::Granted {
                        floor: self.record.floor,
                    },
                    true,
                );
            }
        }
        if let Some((other, remaining_ms)) = self.live_grant(now_ms) {
            if other != holder {
                return (
                    Grant::Busy {
                        holder: other,
                        remaining_ms,
                    },
                    false,
                );
            }
        }
        if epoch <= self.record.max_epoch {
            return (Grant::Stale, false);
        }
        let floor = self.record.bound();
        self.record = LedgerRecord {
            max_epoch: epoch,
            floor,
            current: Some(EpochRecord {
                epoch,
                holder: holder.0,
                ceiling,
                closing: None,
                granted: true,
            }),
        };
        self.expires_at_ms = Some(now_ms + duration_ms);
        (Grant::Granted { floor }, true)
    }

    /// `holder` releases `epoch`: its writes stay at or below `closing`, and
    /// the next holder may ask at once. Whether it changed anything.
    pub fn release(&mut self, holder: NodeId, epoch: u64, closing: HlcTimestamp) -> bool {
        let Some(current) = &mut self.record.current else {
            return false;
        };
        if current.epoch != epoch || current.holder != holder.0 || current.closing.is_some() {
            return false;
        }
        current.closing = Some(closing.min(current.ceiling));
        self.expires_at_ms = None;
        true
    }

    /// Whether this node answers a read of the cluster's control state made
    /// under `fence`, for a change's preconditions; when it does, whether
    /// that taught it the epoch, which survives a restart.
    ///
    /// A node that answers knows the epoch from then on and refuses every
    /// write of an earlier one: a write the reader did not see cannot be
    /// confirmed after the read, so a change's preconditions, read from a
    /// majority, see every change confirmed before it.
    ///
    /// # Errors
    ///
    /// [`Fenced`] when the reader's epoch is behind this node's, or is this
    /// node's epoch granted to another holder.
    pub fn admit_read(&mut self, fence: Option<&LeaseFence>) -> Result<bool, Fenced> {
        let fence = fence.filter(|fence| fence.epoch > 0).ok_or(Fenced::NoLease)?;
        if fence.epoch < self.record.max_epoch {
            return Err(Fenced::EpochBehind {
                epoch: fence.epoch,
                max_epoch: self.record.max_epoch,
            });
        }
        if fence.epoch == self.record.max_epoch {
            return match &self.record.current {
                Some(current) if current.holder != fence.holder.0 => Err(Fenced::OtherHolder),
                _ => Ok(false),
            };
        }
        self.learn(fence);
        Ok(true)
    }

    /// Takes `fence`'s epoch, above every epoch this node has seen, as the
    /// highest: it granted none of it.
    fn learn(&mut self, fence: &LeaseFence) {
        let floor = self.record.bound();
        self.record = LedgerRecord {
            max_epoch: fence.epoch,
            floor,
            current: Some(EpochRecord {
                epoch: fence.epoch,
                holder: fence.holder.0,
                ceiling: fence.ceiling,
                closing: None,
                granted: false,
            }),
        };
        self.expires_at_ms = None;
    }

    /// Whether this node takes a write at `hlc` made under `fence`; when it
    /// does, whether that taught it an epoch, which survives a restart.
    ///
    /// # Errors
    ///
    /// [`Fenced`] names why the write is refused.
    pub fn admit(
        &mut self,
        fence: Option<&LeaseFence>,
        hlc: HlcTimestamp,
        now_ms: u64,
    ) -> Result<bool, Fenced> {
        let fence = fence.filter(|fence| fence.epoch > 0).ok_or(Fenced::NoLease)?;
        if fence.epoch < self.record.max_epoch {
            return Err(Fenced::EpochBehind {
                epoch: fence.epoch,
                max_epoch: self.record.max_epoch,
            });
        }
        if !fence.admits_hlc(hlc) {
            return Err(Fenced::OutsideWindow);
        }
        if fence.epoch == self.record.max_epoch {
            if let Some(current) = &self.record.current {
                if current.holder != fence.holder.0 {
                    return Err(Fenced::OtherHolder);
                }
                if let Some(closing) = current.closing {
                    if hlc > closing {
                        return Err(Fenced::Released);
                    }
                } else if current.granted
                    && self.expires_at_ms.is_none_or(|expires| expires <= now_ms)
                {
                    return Err(Fenced::Expired);
                }
            }
            return Ok(false);
        }
        // An epoch this node never granted: the holder got a majority without
        // it. It is the highest now.
        self.learn(fence);
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const H1: NodeId = NodeId(1);
    const H2: NodeId = NodeId(2);

    fn hlc(wall: u64) -> HlcTimestamp {
        HlcTimestamp::from_record(wall, 0, 9)
    }

    /// A ceiling as a holder sets it: a wall time, no node.
    fn mark(wall: u64) -> HlcTimestamp {
        HlcTimestamp::from_record(wall, 0, 0)
    }

    fn fence(epoch: u64, holder: NodeId, floor: u64, ceiling: u64) -> LeaseFence {
        LeaseFence {
            epoch,
            holder,
            floor: mark(floor),
            ceiling: mark(ceiling),
        }
    }

    #[test]
    fn one_holder_at_a_time_until_the_grant_runs_out_or_is_released() {
        let mut ledger = LeaseLedger::default();
        assert_eq!(
            ledger.acquire(H1, 1, mark(5_000), 5_000, 0),
            (Grant::Granted { floor: mark(0) }, true)
        );
        assert_eq!(
            ledger.acquire(H2, 2, mark(6_000), 5_000, 1_000).0,
            Grant::Busy {
                holder: H1,
                remaining_ms: 4_000
            }
        );
        // Run out: the next holder gets it, above everything the first could write.
        assert_eq!(
            ledger.acquire(H2, 2, mark(11_000), 5_000, 5_000).0,
            Grant::Granted { floor: mark(5_000) }
        );
        // Released: the next holder gets it at once, above the release.
        assert!(ledger.release(H2, 2, hlc(6_000)));
        assert_eq!(
            ledger.acquire(H1, 3, mark(12_000), 5_000, 5_001).0,
            Grant::Granted { floor: hlc(6_000) }
        );
    }

    #[test]
    fn the_live_grant_is_the_one_neither_run_out_nor_released() {
        let mut ledger = LeaseLedger::default();
        assert_eq!(ledger.live_at(0), None);
        ledger.acquire(H1, 3, mark(5_000), 5_000, 1_000);
        assert_eq!(
            ledger.live_at(2_000),
            Some(LiveGrant {
                epoch: 3,
                holder: H1,
                remaining_ms: 4_000
            })
        );
        assert_eq!(ledger.live_at(6_000), None, "run out");
        ledger.acquire(H2, 4, mark(12_000), 5_000, 7_000);
        ledger.release(H2, 4, hlc(7_500));
        assert_eq!(ledger.live_at(7_600), None, "released");
        // A ledger read back after a restart holds out no grant.
        ledger.acquire(H1, 5, mark(14_000), 5_000, 8_000);
        assert_eq!(
            LeaseLedger::from_record(ledger.record()).live_at(8_001),
            None
        );
    }

    #[test]
    fn an_epoch_is_granted_once() {
        let mut ledger = LeaseLedger::default();
        ledger.acquire(H1, 4, mark(5_000), 5_000, 0);
        ledger.release(H1, 4, hlc(10));
        assert_eq!(ledger.acquire(H2, 4, mark(5_000), 5_000, 1).0, Grant::Stale);
        assert_eq!(ledger.acquire(H2, 3, mark(5_000), 5_000, 1).0, Grant::Stale);
        assert_eq!(ledger.max_epoch(), 4);
    }

    #[test]
    fn the_holder_renews_its_own_live_grant_only() {
        let mut ledger = LeaseLedger::default();
        ledger.acquire(H1, 1, mark(5_000), 5_000, 0);
        assert_eq!(
            ledger.acquire(H1, 1, mark(9_000), 5_000, 4_000),
            (Grant::Granted { floor: mark(0) }, true)
        );
        assert_eq!(ledger.record().current.unwrap().ceiling, mark(9_000));
        assert!(ledger.admit(Some(&fence(1, H1, 0, 9_000)), hlc(8_000), 8_500).is_ok());
        // Run out: no renewal, and the epoch is not granted again.
        assert_eq!(
            ledger.acquire(H1, 1, mark(15_000), 5_000, 9_001).0,
            Grant::Stale
        );
    }

    #[test]
    fn writes_of_an_earlier_epoch_are_fenced() {
        let mut ledger = LeaseLedger::default();
        ledger.acquire(H1, 5, mark(5_000), 5_000, 0);
        ledger.acquire(H2, 6, mark(11_000), 5_000, 5_000);
        assert_eq!(
            ledger.admit(Some(&fence(5, H1, 0, 5_000)), hlc(4_000), 5_001),
            Err(Fenced::EpochBehind {
                epoch: 5,
                max_epoch: 6
            })
        );
        assert_eq!(
            ledger.admit(Some(&fence(6, H2, 5_000, 11_000)), hlc(6_000), 5_001),
            Ok(false)
        );
        assert_eq!(ledger.admit(None, hlc(6_000), 5_001), Err(Fenced::NoLease));
    }

    #[test]
    fn a_holder_that_woke_up_after_its_grant_ran_out_is_fenced_by_the_granting_node() {
        let mut ledger = LeaseLedger::default();
        ledger.acquire(H1, 1, mark(5_000), 5_000, 0);
        assert!(ledger.admit(Some(&fence(1, H1, 0, 5_000)), hlc(100), 100).is_ok());
        assert_eq!(
            ledger.admit(Some(&fence(1, H1, 0, 5_000)), hlc(200), 5_000),
            Err(Fenced::Expired)
        );
        // After a restart the grant has run out too.
        let mut restarted = LeaseLedger::from_record(ledger.record());
        assert_eq!(
            restarted.admit(Some(&fence(1, H1, 0, 5_000)), hlc(200), 0),
            Err(Fenced::Expired)
        );
    }

    #[test]
    fn a_released_epoch_takes_late_writes_up_to_its_release_only() {
        let mut ledger = LeaseLedger::default();
        ledger.acquire(H1, 1, mark(5_000), 5_000, 0);
        ledger.release(H1, 1, hlc(300));
        assert!(ledger.admit(Some(&fence(1, H1, 0, 5_000)), hlc(300), 9_000).is_ok());
        assert_eq!(
            ledger.admit(Some(&fence(1, H1, 0, 5_000)), hlc(301), 100),
            Err(Fenced::Released)
        );
    }

    #[test]
    fn a_write_outside_its_window_is_fenced() {
        let mut ledger = LeaseLedger::default();
        ledger.acquire(H1, 1, mark(5_000), 5_000, 0);
        let fenced = fence(1, H1, 1_000, 5_000);
        assert_eq!(
            ledger.admit(Some(&fenced), hlc(5_000), 1),
            Err(Fenced::OutsideWindow)
        );
        assert_eq!(
            ledger.admit(Some(&fenced), HlcTimestamp::from_record(1_000, 0, 0), 1),
            Err(Fenced::OutsideWindow)
        );
        assert!(ledger.admit(Some(&fenced), hlc(1_000), 1).is_ok());
    }

    #[test]
    fn a_node_learns_an_epoch_it_did_not_grant_from_its_write() {
        let mut ledger = LeaseLedger::default();
        ledger.acquire(H1, 1, mark(5_000), 5_000, 0);
        // H2 got epoch 2 from a majority without this node.
        assert_eq!(
            ledger.admit(Some(&fence(2, H2, 5_000, 10_000)), hlc(6_000), 100),
            Ok(true)
        );
        assert_eq!(ledger.max_epoch(), 2);
        assert_eq!(ledger.record().floor, mark(5_000));
        // H1's grant here is gone with it.
        assert!(matches!(
            ledger.admit(Some(&fence(1, H1, 0, 5_000)), hlc(200), 200),
            Err(Fenced::EpochBehind { .. })
        ));
        // A learned epoch is not this node's to expire.
        assert!(ledger.admit(Some(&fence(2, H2, 5_000, 10_000)), hlc(7_000), 60_000).is_ok());
        // And it does not hold the lease here: the next holder asks at once.
        assert_eq!(
            ledger.acquire(H1, 3, mark(20_000), 5_000, 100).0,
            Grant::Granted {
                floor: mark(10_000)
            }
        );
    }

    #[test]
    fn the_floor_never_goes_down() {
        let mut ledger = LeaseLedger::default();
        ledger.acquire(H1, 1, mark(9_000), 5_000, 0);
        ledger.release(H1, 1, hlc(8_000));
        ledger.acquire(H2, 2, mark(7_000), 5_000, 1);
        ledger.release(H2, 2, hlc(1_000));
        assert_eq!(ledger.record().bound(), hlc(8_000));
        assert_eq!(
            ledger.acquire(H1, 3, mark(20_000), 5_000, 2).0,
            Grant::Granted { floor: hlc(8_000) }
        );
    }

    #[test]
    fn a_read_under_a_later_epoch_fences_the_writes_of_earlier_ones() {
        let mut ledger = LeaseLedger::default();
        ledger.acquire(H1, 1, mark(5_000), 5_000, 0);
        // H2 holds epoch 2 through a majority without this node, and reads here.
        assert_eq!(
            ledger.admit_read(Some(&fence(2, H2, 5_000, 10_000))),
            Ok(true)
        );
        // H1's grant is live, yet its write would land after a read that did not see it.
        assert!(matches!(
            ledger.admit(Some(&fence(1, H1, 0, 5_000)), hlc(100), 100),
            Err(Fenced::EpochBehind { .. })
        ));
        assert_eq!(
            ledger.admit_read(Some(&fence(2, H2, 5_000, 10_000))),
            Ok(false)
        );
        assert!(matches!(
            ledger.admit_read(Some(&fence(1, H1, 0, 5_000))),
            Err(Fenced::EpochBehind { .. })
        ));
        assert_eq!(
            ledger.admit_read(Some(&fence(2, H1, 5_000, 10_000))),
            Err(Fenced::OtherHolder)
        );
        assert_eq!(ledger.admit_read(None), Err(Fenced::NoLease));
    }

    #[test]
    fn the_record_survives_as_json() {
        let mut ledger = LeaseLedger::default();
        ledger.acquire(H1, 7, mark(5_000), 5_000, 0);
        let json = serde_json::to_vec(&ledger.record()).unwrap();
        let back: LedgerRecord = serde_json::from_slice(&json).unwrap();
        assert_eq!(back, ledger.record());
    }
}
