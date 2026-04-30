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

//! Tombstone lifecycle policy and purge eligibility checks (Phase 8).
//!
//! # Tombstone Lifecycle
//!
//! 1. `DELETE` → tombstone created (`is_delete_marker = true`)
//! 2. Tombstone replicated to W replicas via quorum write
//! 3. Tombstone participates in anti-entropy (never deleted early)
//! 4. After `gc_grace` period + repair frontier check → safe to purge
//!
//! # Safety Invariant
//!
//! ```text
//! gc_grace > max_rejoin_downtime + max_repair_interval + max_clock_skew
//! ```
//!
//! This ensures a node returning from downtime within `max_rejoin_downtime`
//! will still find all relevant tombstones alive for safe incremental repair.
//! If a node was offline longer, it must perform a full bootstrap instead.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::clock::hlc::HlcTimestamp;
use crate::identity::NodeId;
use crate::repair::frontier::RepairFrontier;
use crate::repair::merkle::KeyRange;

/// Default GC grace period: 7 days (matches Cassandra convention).
const DEFAULT_GC_GRACE_SECS: u64 = 7 * 24 * 3600;

/// Default maximum rejoin downtime: 3 days.
const DEFAULT_MAX_REJOIN_DOWNTIME_SECS: u64 = 3 * 24 * 3600;

/// Default GC cycle interval: 6 hours.
const DEFAULT_GC_INTERVAL_SECS: u64 = 6 * 3600;

/// Maximum repair interval (anti-entropy round time).
const MAX_REPAIR_INTERVAL: Duration = Duration::from_secs(600);

/// Maximum tolerated clock skew between nodes.
const MAX_CLOCK_SKEW: Duration = Duration::from_millis(500);

/// Tombstone retention and purge policy.
///
/// Controls how long tombstones are kept before they can be purged and
/// the maximum offline duration for incremental repair eligibility.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TombstonePolicy {
    /// Minimum lifetime of a tombstone before purge eligibility.
    ///
    /// Must be greater than `max_rejoin_downtime + repair_interval + clock_skew`.
    /// Default: 7 days.
    pub gc_grace: Duration,

    /// Maximum time a node can be offline and still rejoin via incremental
    /// repair. Beyond this, a full bootstrap (snapshot) is required.
    ///
    /// Default: 3 days.
    pub max_rejoin_downtime: Duration,
}

impl Default for TombstonePolicy {
    fn default() -> Self {
        Self {
            gc_grace: Duration::from_secs(DEFAULT_GC_GRACE_SECS),
            max_rejoin_downtime: Duration::from_secs(DEFAULT_MAX_REJOIN_DOWNTIME_SECS),
        }
    }
}

impl TombstonePolicy {
    /// Validate the policy invariant: `gc_grace > max_rejoin_downtime + safety_margin`.
    pub fn validate(&self) -> Result<(), String> {
        let required_minimum = self.max_rejoin_downtime + MAX_REPAIR_INTERVAL + MAX_CLOCK_SKEW;

        if self.gc_grace <= required_minimum {
            return Err(format!(
                "gc_grace ({:?}) must be > max_rejoin_downtime ({:?}) + \
                 repair_interval ({:?}) + clock_skew ({:?}) = {:?}",
                self.gc_grace,
                self.max_rejoin_downtime,
                MAX_REPAIR_INTERVAL,
                MAX_CLOCK_SKEW,
                required_minimum,
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

    /// Interval between GC purge cycles.
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
            return Err("GC interval must be > 0".into());
        }
        self.tombstone_policy.validate()
    }
}

/// Check whether a tombstone is eligible for safe purge.
///
/// A tombstone can be purged **only** when both conditions are met:
///
/// 1. **Age check**: tombstone age > `gc_grace`
/// 2. **Frontier check**: all replicas have confirmed reconciliation past
///    the tombstone's HLC timestamp
///
/// ```text
/// purgeable(T) ⟺ age(T) > gc_grace AND repair_frontier ≥ version(T) FOR ALL replicas
/// ```
pub fn can_purge_tombstone(
    tombstone_hlc_wall: u64,
    tombstone_hlc_logical: u32,
    now_millis: u64,
    gc_grace: Duration,
    repair_frontier: &RepairFrontier,
    key_range: &KeyRange,
    all_replicas: &[NodeId],
) -> bool {
    // Age check: tombstone must be older than gc_grace.
    let age_millis = now_millis.saturating_sub(tombstone_hlc_wall);
    let gc_grace_millis = gc_grace.as_millis() as u64;
    let old_enough = age_millis > gc_grace_millis;

    if !old_enough {
        return false;
    }

    // Frontier check: all replicas must have reconciled past this tombstone.
    let tombstone_hlc = HlcTimestamp {
        wall_time: tombstone_hlc_wall,
        logical: tombstone_hlc_logical,
        node_id: 0, // node_id is irrelevant for comparison
    };

    repair_frontier.is_safe_to_purge(key_range, &tombstone_hlc, all_replicas)
}

#[cfg(test)]
mod tests {
    use super::*;

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
        };
        assert!(policy.validate().is_err());
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
        assert!(config.validate().is_err());
    }

    #[test]
    fn can_purge_young_tombstone_returns_false() {
        let frontier = RepairFrontier::new();
        let now = 1000;
        let tombstone_hlc_wall = 999; // 1ms old
        let gc_grace = Duration::from_secs(7 * 24 * 3600);
        let key_range = KeyRange::new(0, 1000);

        assert!(!can_purge_tombstone(
            tombstone_hlc_wall,
            0,
            now,
            gc_grace,
            &frontier,
            &key_range,
            &[NodeId(1)],
        ));
    }

    #[test]
    fn can_purge_old_tombstone_without_frontier_returns_false() {
        let frontier = RepairFrontier::new();
        let gc_grace = Duration::from_secs(1);
        let tombstone_hlc_wall = 1000;
        let now = tombstone_hlc_wall + 2000; // 2s old, gc_grace=1s
        let key_range = KeyRange::new(0, 1000);

        assert!(!can_purge_tombstone(
            tombstone_hlc_wall,
            0,
            now,
            gc_grace,
            &frontier,
            &key_range,
            &[NodeId(1)],
        ));
    }

    #[test]
    fn can_purge_old_tombstone_with_frontier_returns_true() {
        let mut frontier = RepairFrontier::new();
        let key_range = KeyRange::new(0, 1000);
        let n1 = NodeId(1);
        let n2 = NodeId(2);
        let n3 = NodeId(3);

        let tombstone_hlc_wall = 1000;
        let tombstone_hlc_logical = 0;

        // All replicas confirmed past the tombstone
        let past_tombstone = HlcTimestamp {
            wall_time: 2000,
            logical: 0,
            node_id: 0,
        };
        frontier.advance(&key_range, n1, past_tombstone);
        frontier.advance(&key_range, n2, past_tombstone);
        frontier.advance(&key_range, n3, past_tombstone);

        let gc_grace = Duration::from_secs(1);
        let now = tombstone_hlc_wall + 2000;

        assert!(can_purge_tombstone(
            tombstone_hlc_wall,
            tombstone_hlc_logical,
            now,
            gc_grace,
            &frontier,
            &key_range,
            &[n1, n2, n3],
        ));
    }

    #[test]
    fn can_purge_fails_when_one_replica_not_synced() {
        let mut frontier = RepairFrontier::new();
        let key_range = KeyRange::new(0, 1000);
        let n1 = NodeId(1);
        let n2 = NodeId(2);
        let n3 = NodeId(3);

        let tombstone_hlc_wall = 1000;

        let past_tombstone = HlcTimestamp {
            wall_time: 2000,
            logical: 0,
            node_id: 0,
        };
        frontier.advance(&key_range, n1, past_tombstone);
        frontier.advance(&key_range, n2, past_tombstone);
        // n3 NOT advanced — simulates an offline or lagging replica

        let gc_grace = Duration::from_secs(1);
        let now = tombstone_hlc_wall + 2000;

        assert!(!can_purge_tombstone(
            tombstone_hlc_wall,
            0,
            now,
            gc_grace,
            &frontier,
            &key_range,
            &[n1, n2, n3],
        ));
    }

    #[test]
    fn can_purge_with_empty_replicas_returns_false() {
        let frontier = RepairFrontier::new();
        let key_range = KeyRange::new(0, 1000);
        let gc_grace = Duration::from_secs(1);

        assert!(!can_purge_tombstone(
            1000,
            0,
            3000,
            gc_grace,
            &frontier,
            &key_range,
            &[]
        ));
    }

    #[test]
    fn policy_boundary_gc_grace_equals_minimum_is_invalid() {
        let max_rejoin = Duration::from_secs(100);
        let policy = TombstonePolicy {
            gc_grace: max_rejoin + MAX_REPAIR_INTERVAL + MAX_CLOCK_SKEW,
            max_rejoin_downtime: max_rejoin,
        };
        // Exactly equal is not valid (must be strictly greater)
        assert!(policy.validate().is_err());
    }

    #[test]
    fn policy_boundary_gc_grace_just_above_minimum_is_valid() {
        let max_rejoin = Duration::from_secs(100);
        let policy = TombstonePolicy {
            gc_grace: max_rejoin + MAX_REPAIR_INTERVAL + MAX_CLOCK_SKEW + Duration::from_secs(1),
            max_rejoin_downtime: max_rejoin,
        };
        assert!(policy.validate().is_ok());
    }
}
