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

//! Hybrid Logical Clock (HLC) for causal ordering in distributed mode.
//!
//! The HLC combines a physical wall clock with a logical counter to provide
//! monotonically increasing timestamps even when physical clocks are skewed.
//! Each timestamp also carries the originating node ID as a tiebreaker for
//! Last-Writer-Wins (LWW) conflict resolution.
//!
//! # Algorithm
//!
//! Based on "Logical Physical Clocks" (Kulkarni et al.):
//!
//! ```text
//! HLC.now():
//!   pt = physical_clock()
//!   l.wall = max(l.wall, pt)
//!   if l.wall == pt: l.logical += 1
//!   else:            l.logical = 0
//!   return (l.wall, l.logical, node_id)
//!
//! HLC.observe(remote):
//!   pt = physical_clock()
//!   l.wall = max(l.wall, remote.wall, pt)
//!   if all three equal:          l.logical = max(l.logical, remote.logical) + 1
//!   elif l.wall == remote.wall:  l.logical = max(l.logical, remote.logical) + 1
//!   elif l.wall == pt:           l.logical += 1
//!   else:                        l.logical = 0
//!   return (l.wall, l.logical, node_id)
//! ```
//!
//! # Ordering
//!
//! Timestamps are compared lexicographically: `(wall_time, logical, node_id)`.
//! This guarantees a total order across all nodes without coordination.
//!
//! # Clock Skew Detection
//!
//! When a remote timestamp's wall_time exceeds the local physical clock by
//! more than `max_clock_skew`, a warning is logged. This indicates NTP
//! misconfiguration or network partition recovery.
//!
//! # Thread Safety
//!
//! [`HybridClock`] is `Send + Sync` and uses a `Mutex` internally.

use std::sync::Mutex;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::identity::NodeId;
use crate::rpc::proto;

/// Default maximum tolerable clock skew between nodes (500ms).
const DEFAULT_MAX_CLOCK_SKEW: Duration = Duration::from_millis(500);

/// A single HLC timestamp with total ordering.
///
/// Comparison is lexicographic: `(wall_time, logical, node_id)`.
/// This ensures deterministic conflict resolution across all nodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct HlcTimestamp {
    /// Physical wall clock time in milliseconds since Unix epoch.
    pub wall_time: u64,
    /// Logical counter for events within the same millisecond.
    pub logical: u32,
    /// Originating node ID (LWW tiebreaker).
    pub node_id: u128,
}

impl HlcTimestamp {
    /// Create a zero timestamp (used as initial value).
    pub fn zero() -> Self {
        Self {
            wall_time: 0,
            logical: 0,
            node_id: 0,
        }
    }

    /// Returns true if this is a zero (uninitialized) timestamp.
    pub fn is_zero(&self) -> bool {
        self.wall_time == 0 && self.logical == 0 && self.node_id == 0
    }

    /// Reconstruct from individual fields stored in `IndexRecord`.
    pub fn from_record(wall_time: u64, logical: u32, node_id: u128) -> Self {
        Self {
            wall_time,
            logical,
            node_id,
        }
    }

    /// Convert to protobuf representation.
    pub fn to_proto(self) -> proto::HybridTimestamp {
        proto::HybridTimestamp {
            wall_time: self.wall_time,
            logical: self.logical,
            node_id: self.node_id.to_be_bytes().to_vec(),
        }
    }

    /// Parse from protobuf representation.
    pub fn from_proto(proto: &proto::HybridTimestamp) -> Self {
        let node_id = if proto.node_id.len() == 16 {
            u128::from_be_bytes(proto.node_id[..16].try_into().unwrap_or([0u8; 16]))
        } else {
            0
        };
        Self {
            wall_time: proto.wall_time,
            logical: proto.logical,
            node_id,
        }
    }
}

impl PartialOrd for HlcTimestamp {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HlcTimestamp {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.wall_time
            .cmp(&other.wall_time)
            .then(self.logical.cmp(&other.logical))
            .then(self.node_id.cmp(&other.node_id))
    }
}

impl std::fmt::Display for HlcTimestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "HLC({}.{:04}@{:016x})",
            self.wall_time, self.logical, self.node_id
        )
    }
}

/// Thread-safe Hybrid Logical Clock.
///
/// Generates monotonically increasing timestamps by tracking the maximum
/// of the local wall clock and any received remote timestamp.
pub struct HybridClock {
    node_id: u128,
    max_clock_skew: Duration,
    state: Mutex<ClockState>,
}

#[derive(Debug)]
struct ClockState {
    last_wall: u64,
    last_logical: u32,
}

impl HybridClock {
    /// Create a new HLC for the given node with default max clock skew (500ms).
    pub fn new(node_id: NodeId) -> Self {
        Self::with_max_skew(node_id, DEFAULT_MAX_CLOCK_SKEW)
    }

    /// Create a new HLC with a custom maximum clock skew threshold.
    pub fn with_max_skew(node_id: NodeId, max_clock_skew: Duration) -> Self {
        Self {
            node_id: node_id.0,
            max_clock_skew,
            state: Mutex::new(ClockState {
                last_wall: 0,
                last_logical: 0,
            }),
        }
    }

    /// Returns the node ID this clock belongs to.
    pub fn node_id(&self) -> u128 {
        self.node_id
    }

    /// Returns the configured maximum clock skew threshold.
    pub fn max_clock_skew(&self) -> Duration {
        self.max_clock_skew
    }

    /// Generate a new timestamp guaranteed to be greater than any previously
    /// generated or observed timestamp.
    pub fn now(&self) -> HlcTimestamp {
        let physical = now_millis();
        let mut state = self.state.lock().expect("HLC mutex poisoned");

        if physical > state.last_wall {
            state.last_wall = physical;
            state.last_logical = 0;
        } else {
            state.last_logical += 1;
        }

        HlcTimestamp {
            wall_time: state.last_wall,
            logical: state.last_logical,
            node_id: self.node_id,
        }
    }

    /// Update the clock after receiving a remote timestamp.
    ///
    /// Ensures that the next call to [`now`](Self::now) will return a
    /// timestamp strictly greater than both the local clock and the
    /// received remote timestamp.
    ///
    /// Logs a warning if the remote wall_time exceeds the local physical
    /// clock by more than `max_clock_skew`.
    pub fn observe(&self, remote: &HlcTimestamp) {
        let physical = now_millis();

        // Detect excessive clock skew
        if remote.wall_time > physical {
            let skew_ms = remote.wall_time - physical;
            if skew_ms > self.max_clock_skew.as_millis() as u64 {
                warn!(
                    remote_wall = remote.wall_time,
                    local_physical = physical,
                    skew_ms = skew_ms,
                    max_skew_ms = self.max_clock_skew.as_millis() as u64,
                    remote_node = remote.node_id,
                    "clock skew exceeds threshold — check NTP synchronization"
                );
            }
        }

        let mut state = self.state.lock().expect("HLC mutex poisoned");

        if physical > state.last_wall && physical > remote.wall_time {
            state.last_wall = physical;
            state.last_logical = 0;
        } else if remote.wall_time > state.last_wall {
            state.last_wall = remote.wall_time;
            state.last_logical = remote.logical + 1;
        } else if remote.wall_time == state.last_wall {
            state.last_logical = state.last_logical.max(remote.logical) + 1;
        }
        // else: local is already ahead, just increment logical on next now()
    }
}

/// Returns current wall clock time in milliseconds since Unix epoch.
pub(crate) fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamps_are_monotonically_increasing() {
        let clock = HybridClock::new(NodeId(1));
        let t1 = clock.now();
        let t2 = clock.now();
        let t3 = clock.now();
        assert!(t1 < t2);
        assert!(t2 < t3);
    }

    #[test]
    fn observe_advances_clock_past_remote() {
        let clock = HybridClock::new(NodeId(1));
        let remote = HlcTimestamp {
            wall_time: u64::MAX / 2,
            logical: 100,
            node_id: 2,
        };
        clock.observe(&remote);
        let local = clock.now();
        assert!(local > remote);
    }

    #[test]
    fn different_nodes_produce_total_order() {
        let c1 = HybridClock::new(NodeId(1));
        let c2 = HybridClock::new(NodeId(2));
        let t1 = c1.now();
        let t2 = c2.now();
        assert_ne!(t1, t2);
    }

    #[test]
    fn proto_roundtrip() {
        let ts = HlcTimestamp {
            wall_time: 1700000000000,
            logical: 42,
            node_id: 12345,
        };
        let proto = ts.to_proto();
        let back = HlcTimestamp::from_proto(&proto);
        assert_eq!(ts, back);
    }

    #[test]
    fn zero_timestamp_is_minimum() {
        let clock = HybridClock::new(NodeId(1));
        let ts = clock.now();
        assert!(ts > HlcTimestamp::zero());
        assert!(HlcTimestamp::zero().is_zero());
    }

    #[test]
    fn from_record_roundtrip() {
        let ts = HlcTimestamp::from_record(1000, 5, 42);
        assert_eq!(ts.wall_time, 1000);
        assert_eq!(ts.logical, 5);
        assert_eq!(ts.node_id, 42);
    }

    #[test]
    fn display_format() {
        let ts = HlcTimestamp {
            wall_time: 1000,
            logical: 42,
            node_id: 0xff,
        };
        let s = ts.to_string();
        assert!(s.contains("1000"));
        assert!(s.contains("0042"));
    }

    #[test]
    fn observe_same_wall_time_increments_logical() {
        let clock = HybridClock::new(NodeId(1));
        // Force internal wall to a known value
        let remote = HlcTimestamp {
            wall_time: u64::MAX / 2,
            logical: 10,
            node_id: 2,
        };
        clock.observe(&remote);

        // Observe again with same wall_time but lower logical
        let remote2 = HlcTimestamp {
            wall_time: u64::MAX / 2,
            logical: 5,
            node_id: 3,
        };
        clock.observe(&remote2);

        let ts = clock.now();
        // Should be past both remotes
        assert!(ts > remote);
        assert!(ts > remote2);
    }

    #[test]
    fn custom_max_skew() {
        let clock = HybridClock::with_max_skew(NodeId(1), Duration::from_secs(1));
        assert_eq!(clock.max_clock_skew(), Duration::from_secs(1));
        assert_eq!(clock.node_id(), 1);
    }

    #[test]
    fn ordering_tiebreaker_by_node_id() {
        let a = HlcTimestamp {
            wall_time: 1000,
            logical: 0,
            node_id: 1,
        };
        let b = HlcTimestamp {
            wall_time: 1000,
            logical: 0,
            node_id: 2,
        };
        assert!(a < b, "same wall_time+logical, higher node_id wins");
    }

    #[test]
    fn ordering_tiebreaker_by_logical() {
        let a = HlcTimestamp {
            wall_time: 1000,
            logical: 1,
            node_id: 1,
        };
        let b = HlcTimestamp {
            wall_time: 1000,
            logical: 2,
            node_id: 1,
        };
        assert!(a < b, "same wall_time, higher logical wins");
    }

    #[test]
    fn serialization_roundtrip() {
        let ts = HlcTimestamp {
            wall_time: 1700000000000,
            logical: 42,
            node_id: u128::MAX,
        };
        let json = serde_json::to_string(&ts).unwrap();
        let back: HlcTimestamp = serde_json::from_str(&json).unwrap();
        assert_eq!(ts, back);
    }
}
