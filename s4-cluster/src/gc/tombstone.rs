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

//! Tombstone lifecycle policy and purge eligibility checks.
//!
//! # Tombstone Lifecycle
//!
//! 1. A delete of a replicated key leaves a tombstone with the delete's HLC
//!    on every replica it reaches.
//! 2. Anti-entropy carries it to the replicas that missed it; each complete
//!    round with a peer moves that peer's cut of the repair frontier.
//! 3. A node removes its tombstone once it is older than `gc_grace` and the
//!    cut of every other replica of its key is past its HLC: every replica has
//!    the delete, or something newer.
//!
//! # Safety Invariant
//!
//! ```text
//! gc_grace > max_rejoin_downtime + turn of rounds over the peers
//!            + replica write timeout + max_clock_skew
//! ```
//!
//! A node returning from downtime within `max_rejoin_downtime` still finds
//! the tombstones of the deletes it missed, and its peers' rounds — each takes
//! every peer once per turn — bring them to it. A write of an older version
//! still on its way to a replica lands within the replica write timeout, while
//! the tombstone it lost to is still there to outrank it. The frontier guards
//! the rest on its own — a replica that is down holds the tombstones of its
//! keys on every other node — and the invariant keeps a tombstone that
//! anti-entropy does not carry to a replica without the key (one older than
//! `gc_grace`) from being one that replica still needs.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::clock::hlc::HlcTimestamp;
use crate::identity::NodeId;
use crate::repair::frontier::RepairFrontier;

/// Default GC grace period: 7 days (matches Cassandra convention).
const DEFAULT_GC_GRACE_SECS: u64 = 7 * 24 * 3600;

/// Default maximum rejoin downtime: 3 days.
const DEFAULT_MAX_REJOIN_DOWNTIME_SECS: u64 = 3 * 24 * 3600;

/// Default interval between tombstone GC passes: 6 hours.
pub const DEFAULT_GC_INTERVAL_SECS: u64 = 6 * 3600;

/// Default turn of anti-entropy rounds over the peers: the default interval
/// of 600 s, two peers of a three-node pool.
const DEFAULT_REPAIR_TURN_SECS: u64 = 2 * 600;

/// Default replica write timeout of the write coordinator.
const DEFAULT_WRITE_SETTLE_SECS: u64 = 60;

/// Maximum tolerated clock skew between nodes.
pub const MAX_CLOCK_SKEW: Duration = Duration::from_millis(500);

/// How long after its HLC a tombstone may still land on a replica through the
/// write of its delete: the coordinator gives up on a replica after
/// `replica_write_timeout`, and the replica's clock may differ from the
/// coordinator's by the tolerated skew. A repair round's cut is set back by
/// this much, so that every tombstone the coordinator's write brought to the
/// node before the cut was in the round.
pub fn arrival_window(replica_write_timeout: Duration) -> Duration {
    replica_write_timeout.saturating_add(MAX_CLOCK_SKEW)
}

/// Tombstone retention and purge policy.
///
/// Controls how long tombstones are kept before they can be purged and
/// the maximum offline duration for incremental repair eligibility.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TombstonePolicy {
    /// Minimum lifetime of a tombstone before purge eligibility.
    ///
    /// Must be greater than `max_rejoin_downtime + repair_turn + write_settle
    /// + clock_skew`. Default: 7 days.
    pub gc_grace: Duration,

    /// Maximum time a node can be offline and still rejoin via incremental
    /// repair. Beyond this, a full bootstrap (snapshot) is required.
    ///
    /// Default: 3 days.
    pub max_rejoin_downtime: Duration,

    /// How long a node's anti-entropy rounds take to visit every peer once:
    /// `S4_ANTI_ENTROPY_INTERVAL_SECS` × the other nodes of the pool.
    /// Default: 20 minutes.
    pub repair_turn: Duration,

    /// How long a write may still be on its way to a replica: the write
    /// coordinator's replica write timeout. Default: 60 s.
    pub write_settle: Duration,
}

impl Default for TombstonePolicy {
    fn default() -> Self {
        Self {
            gc_grace: Duration::from_secs(DEFAULT_GC_GRACE_SECS),
            max_rejoin_downtime: Duration::from_secs(DEFAULT_MAX_REJOIN_DOWNTIME_SECS),
            repair_turn: Duration::from_secs(DEFAULT_REPAIR_TURN_SECS),
            write_settle: Duration::from_secs(DEFAULT_WRITE_SETTLE_SECS),
        }
    }
}

impl TombstonePolicy {
    /// Validate the policy invariant: `gc_grace > max_rejoin_downtime +
    /// repair_turn + write_settle + clock_skew`. A sum past what a `Duration`
    /// holds is an error too, not a panic.
    pub fn validate(&self) -> Result<(), String> {
        let terms = format!(
            "the rejoin downtime ({:?}, S4_MAX_REJOIN_DOWNTIME_DAYS) + a turn of anti-entropy \
             rounds over the peers ({:?}, S4_ANTI_ENTROPY_INTERVAL_SECS × the other nodes of \
             the pool) + the replica write timeout ({:?}) + the clock skew ({:?})",
            self.max_rejoin_downtime, self.repair_turn, self.write_settle, MAX_CLOCK_SKEW,
        );
        let required_minimum = self
            .max_rejoin_downtime
            .checked_add(self.repair_turn)
            .and_then(|sum| sum.checked_add(self.write_settle))
            .and_then(|sum| sum.checked_add(MAX_CLOCK_SKEW))
            .ok_or_else(|| format!("{terms} add up past any grace period"))?;

        if self.gc_grace <= required_minimum {
            return Err(format!(
                "the tombstone grace period ({:?}, S4_GC_GRACE_SECS or S4_GC_GRACE_DAYS) must be \
                 longer than {terms} = {required_minimum:?}",
                self.gc_grace,
            ));
        }

        Ok(())
    }
}

/// Top-level GC configuration for the distributed GC service.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GcConfig {
    /// Whether distributed GC is enabled.
    pub enabled: bool,

    /// Interval between GC purge passes (`S4_TOMBSTONE_GC_INTERVAL_SECS`).
    /// Default: 6 hours.
    pub interval: Duration,

    /// Tombstone retention policy.
    pub tombstone_policy: TombstonePolicy,
}

impl Default for GcConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval: Duration::from_secs(DEFAULT_GC_INTERVAL_SECS),
            tombstone_policy: TombstonePolicy::default(),
        }
    }
}

impl GcConfig {
    /// Validate the GC configuration.
    pub fn validate(&self) -> Result<(), String> {
        if self.interval.is_zero() {
            return Err("S4_TOMBSTONE_GC_INTERVAL_SECS must be > 0".into());
        }
        self.tombstone_policy.validate()
    }
}

/// Whether a node may remove its tombstone issued at `tombstone`.
///
/// Both have to hold:
///
/// 1. **Age**: the tombstone is older than `gc_grace`;
/// 2. **Frontier**: the cut of every replica of its key other than this node,
///    `other_replicas`, is past its HLC — each of them has had everything
///    this node held before its cut, the tombstone among it.
///
/// The node itself is not in the condition: what it holds is what it compares
/// its peers with. A key with no other replica has nothing to wait for.
///
/// ```text
/// purgeable(T) ⟺ age(T) > gc_grace ∧ ∀ replica ≠ self: cut(replica) > hlc(T)
/// ```
pub fn can_purge_tombstone(
    tombstone: HlcTimestamp,
    now_millis: u64,
    gc_grace: Duration,
    repair_frontier: &RepairFrontier,
    other_replicas: &[NodeId],
) -> bool {
    let age_millis = now_millis.saturating_sub(tombstone.wall_time);
    let gc_grace_millis = u64::try_from(gc_grace.as_millis()).unwrap_or(u64::MAX);
    if age_millis <= gc_grace_millis {
        return false;
    }
    other_replicas.iter().all(|replica| {
        repair_frontier.peer_cut(replica).is_some_and(|cut| {
            (cut.wall_time, cut.logical) > (tombstone.wall_time, tombstone.logical)
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(wall: u64) -> HlcTimestamp {
        HlcTimestamp::from_record(wall, 0, 0)
    }

    fn frontier(cuts: &[(u128, u64)]) -> RepairFrontier {
        let mut frontier = RepairFrontier::new();
        for (node, wall) in cuts {
            frontier.advance_peer(NodeId(*node), ts(*wall));
        }
        frontier
    }

    #[test]
    fn default_policy_is_valid() {
        let policy = TombstonePolicy::default();
        assert!(policy.validate().is_ok());
    }

    #[test]
    fn invalid_policy_gc_grace_too_short() {
        let policy = TombstonePolicy {
            gc_grace: Duration::from_secs(100),
            max_rejoin_downtime: Duration::from_secs(3 * 24 * 3600),
            ..TombstonePolicy::default()
        };
        assert!(policy.validate().is_err());
    }

    fn policy(grace: u64, turn: u64, settle: u64) -> TombstonePolicy {
        TombstonePolicy {
            gc_grace: Duration::from_secs(grace),
            max_rejoin_downtime: Duration::ZERO,
            repair_turn: Duration::from_secs(turn),
            write_settle: Duration::from_secs(settle),
        }
    }

    /// The invariant weighs the turn of rounds over every peer the node is
    /// configured for, not one interval and not a constant.
    #[test]
    fn the_invariant_takes_a_whole_turn_of_rounds() {
        assert!(policy(80, 10, 60).validate().is_ok());
        let error = policy(80, 20, 60).validate().unwrap_err();
        assert!(error.contains("S4_ANTI_ENTROPY_INTERVAL_SECS"), "{error}");
        // Ten nodes, an hour between rounds: a turn takes nine hours, and a
        // grace of 4000 s covers less than one of them.
        assert!(policy(4000, 9 * 3600, 60).validate().is_err());
    }

    /// A write still on its way to a replica must find the tombstone it lost
    /// to: a grace shorter than the replica write timeout is refused.
    #[test]
    fn the_invariant_takes_the_replica_write_timeout() {
        let error = policy(40, 10, 60).validate().unwrap_err();
        assert!(error.contains("replica write timeout"), "{error}");
        assert!(policy(75, 10, 60).validate().is_ok());
    }

    /// Terms that add up past what a duration holds are a configuration
    /// error, not a panic.
    #[test]
    fn terms_past_any_duration_are_an_error() {
        let error = policy(u64::MAX, u64::MAX, 60).validate().unwrap_err();
        assert!(error.contains("add up past any grace period"), "{error}");
    }

    #[test]
    fn gc_config_default_is_valid() {
        let config = GcConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn gc_config_zero_interval_is_invalid() {
        let config = GcConfig {
            interval: Duration::ZERO,
            ..Default::default()
        };
        let error = config.validate().unwrap_err();
        assert!(error.contains("S4_TOMBSTONE_GC_INTERVAL_SECS"), "{error}");
    }

    #[test]
    fn a_young_tombstone_stays_whatever_the_cuts() {
        let cuts = frontier(&[(2, 5000), (3, 5000)]);
        let grace = Duration::from_secs(7 * 24 * 3600);
        assert!(!can_purge_tombstone(
            ts(999),
            1000,
            grace,
            &cuts,
            &[NodeId(2), NodeId(3)]
        ));
    }

    #[test]
    fn an_old_tombstone_without_cuts_stays() {
        let grace = Duration::from_secs(1);
        let none = RepairFrontier::new();
        assert!(!can_purge_tombstone(
            ts(1000),
            3000,
            grace,
            &none,
            &[NodeId(2)]
        ));
    }

    #[test]
    fn an_old_tombstone_goes_once_every_other_replica_is_past_it() {
        let grace = Duration::from_secs(1);
        let both = frontier(&[(2, 2000), (3, 2000)]);
        assert!(can_purge_tombstone(
            ts(1000),
            3000,
            grace,
            &both,
            &[NodeId(2), NodeId(3)]
        ));
    }

    /// A replica without a cut — down, or not reached by a complete round —
    /// holds the tombstones of its keys.
    #[test]
    fn a_replica_without_a_cut_holds_the_tombstone() {
        let grace = Duration::from_secs(1);
        let one = frontier(&[(2, 2000)]);
        assert!(!can_purge_tombstone(
            ts(1000),
            3000,
            grace,
            &one,
            &[NodeId(2), NodeId(3)]
        ));
    }

    /// The cut has to be past the tombstone, not at it.
    #[test]
    fn a_cut_at_the_tombstone_is_not_past_it() {
        let grace = Duration::from_secs(1);
        let at = frontier(&[(2, 1000)]);
        assert!(!can_purge_tombstone(
            ts(1000),
            3000,
            grace,
            &at,
            &[NodeId(2)]
        ));
        let tombstone = HlcTimestamp::from_record(1000, 3, 9);
        let same_wall = frontier(&[(2, 1000)]);
        assert!(!can_purge_tombstone(
            tombstone,
            3000,
            grace,
            &same_wall,
            &[NodeId(2)]
        ));
    }

    /// A key no other node replicates has nobody to wait for.
    #[test]
    fn a_key_with_no_other_replica_goes_by_age_alone() {
        let grace = Duration::from_secs(1);
        let none = RepairFrontier::new();
        assert!(can_purge_tombstone(ts(1000), 3000, grace, &none, &[]));
        assert!(!can_purge_tombstone(ts(1000), 1500, grace, &none, &[]));
    }

    #[test]
    fn policy_boundary_gc_grace_equals_minimum_is_invalid() {
        let mut at = policy(0, 600, 60);
        at.max_rejoin_downtime = Duration::from_secs(100);
        at.gc_grace = Duration::from_secs(100 + 600 + 60) + MAX_CLOCK_SKEW;
        // Exactly equal is not valid (must be strictly greater)
        assert!(at.validate().is_err());
        at.gc_grace += Duration::from_millis(1);
        assert!(at.validate().is_ok());
    }

    #[test]
    fn the_arrival_window_is_the_write_timeout_and_the_clock_skew() {
        assert_eq!(
            arrival_window(Duration::from_secs(60)),
            Duration::from_millis(60_500)
        );
    }
}
