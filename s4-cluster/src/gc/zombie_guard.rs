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

//! Zombie resurrection protection (Phase 8).
//!
//! Prevents the scenario where a deleted object "comes back to life" because
//! a node that was offline during the delete returns with stale data after
//! the tombstone has been purged.
//!
//! # Zombie Resurrection Scenario
//!
//! 1. Object X exists on nodes A, B, C
//! 2. `DELETE X` → tombstone created on A, B (quorum met)
//! 3. Compaction on A: tombstone purged (gc_grace expired)
//! 4. Node C was offline, comes back, still has X
//! 5. Anti-entropy: A sees X on C but has no record → zombie resurrection!
//!
//! # Protection Layers
//!
//! 1. **gc_grace = 7 days**: tombstones live long enough for most recoveries
//! 2. **Repair frontier**: tombstones only purged when ALL replicas confirmed
//! 3. **Rejoin guard**: nodes offline > `max_rejoin_downtime` must full-bootstrap

use std::time::Duration;

use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::gc::tombstone::TombstonePolicy;
use crate::identity::NodeId;

/// Decision for a node attempting to rejoin the cluster.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RejoinDecision {
    /// Node was offline for an acceptable duration.
    /// Safe to perform incremental repair via anti-entropy.
    IncrementalRepair,

    /// Node was offline too long. Tombstones may have been purged.
    /// Must perform a full bootstrap (snapshot from peers) to guarantee
    /// no zombie resurrection.
    FullBootstrapRequired {
        /// How long the node was offline.
        offline_duration: Duration,
        /// The maximum allowed for incremental repair.
        max_allowed: Duration,
    },
}

/// Guard that evaluates whether a returning node can safely rejoin via
/// incremental repair or needs a full bootstrap.
#[derive(Debug, Clone)]
pub struct ZombieGuard {
    policy: TombstonePolicy,
}

impl ZombieGuard {
    /// Create a new guard with the given tombstone policy.
    pub fn new(policy: TombstonePolicy) -> Self {
        Self { policy }
    }

    /// Evaluate whether a node can rejoin via incremental repair.
    ///
    /// # Arguments
    ///
    /// * `node_id` — the returning node
    /// * `last_seen_millis` — the last time this node was seen (HLC wall millis)
    /// * `now_millis` — current time (HLC wall millis)
    pub fn evaluate_rejoin(
        &self,
        node_id: NodeId,
        last_seen_millis: u64,
        now_millis: u64,
    ) -> RejoinDecision {
        let offline_millis = now_millis.saturating_sub(last_seen_millis);
        let offline_duration = Duration::from_millis(offline_millis);

        if offline_duration <= self.policy.max_rejoin_downtime {
            info!(
                %node_id,
                offline_secs = offline_duration.as_secs(),
                "node rejoin: incremental repair (within max_rejoin_downtime)"
            );
            RejoinDecision::IncrementalRepair
        } else {
            warn!(
                %node_id,
                offline_secs = offline_duration.as_secs(),
                max_allowed_secs = self.policy.max_rejoin_downtime.as_secs(),
                "node rejoin: full bootstrap required (offline too long)"
            );
            RejoinDecision::FullBootstrapRequired {
                offline_duration,
                max_allowed: self.policy.max_rejoin_downtime,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_guard() -> ZombieGuard {
        ZombieGuard::new(TombstonePolicy::default())
    }

    #[test]
    fn short_offline_allows_incremental_repair() {
        let guard = default_guard();
        let now = 10_000_000u64; // 10,000 seconds in millis
        let last_seen = now - 60_000; // 60 seconds ago

        let decision = guard.evaluate_rejoin(NodeId(1), last_seen, now);
        assert_eq!(decision, RejoinDecision::IncrementalRepair);
    }

    #[test]
    fn exact_max_rejoin_allows_incremental_repair() {
        let guard = default_guard();
        let max_rejoin_millis = TombstonePolicy::default().max_rejoin_downtime.as_millis() as u64;
        let now = max_rejoin_millis + 1000;
        let last_seen = now - max_rejoin_millis;

        let decision = guard.evaluate_rejoin(NodeId(1), last_seen, now);
        assert_eq!(decision, RejoinDecision::IncrementalRepair);
    }

    #[test]
    fn long_offline_requires_bootstrap() {
        let guard = default_guard();
        let max_rejoin_millis = TombstonePolicy::default().max_rejoin_downtime.as_millis() as u64;
        let now = max_rejoin_millis + 2000;
        let last_seen = now - max_rejoin_millis - 1; // 1ms over the limit

        let decision = guard.evaluate_rejoin(NodeId(1), last_seen, now);
        assert!(matches!(
            decision,
            RejoinDecision::FullBootstrapRequired { .. }
        ));
    }

    #[test]
    fn very_long_offline_requires_bootstrap() {
        let guard = default_guard();
        let now = 30 * 24 * 3600 * 1000u64; // 30 days in millis
        let last_seen = 0; // was seen at epoch start

        let decision = guard.evaluate_rejoin(NodeId(1), last_seen, now);
        match decision {
            RejoinDecision::FullBootstrapRequired {
                offline_duration,
                max_allowed,
            } => {
                assert!(offline_duration > max_allowed);
            }
            _ => panic!("expected FullBootstrapRequired"),
        }
    }

    #[test]
    fn custom_policy_shorter_rejoin_window() {
        let policy = TombstonePolicy {
            gc_grace: Duration::from_secs(3600),
            max_rejoin_downtime: Duration::from_secs(300), // 5 minutes
        };
        let guard = ZombieGuard::new(policy);

        let now = 1_000_000u64;
        // 6 minutes offline → exceeds 5-minute window
        let last_seen = now - 360_000;

        let decision = guard.evaluate_rejoin(NodeId(42), last_seen, now);
        assert!(matches!(
            decision,
            RejoinDecision::FullBootstrapRequired { .. }
        ));
    }

    #[test]
    fn zero_offline_allows_incremental_repair() {
        let guard = default_guard();
        let now = 1_000_000u64;
        let decision = guard.evaluate_rejoin(NodeId(1), now, now);
        assert_eq!(decision, RejoinDecision::IncrementalRepair);
    }
}
