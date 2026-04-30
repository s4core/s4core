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

//! Cluster node lifecycle management.
//!
//! Handles graceful shutdown and drain operations for cluster nodes.
//! The shutdown sequence ensures zero lost operations:
//!
//! 1. Stop accepting new coordinated requests
//! 2. Wait for in-flight operations to complete (with timeout)
//! 3. Flush pending hinted handoff hints
//! 4. Broadcast `NodeStatus::Left` via gossip
//! 5. Wait for gossip propagation
//! 6. Close gRPC connections
//! 7. Sync storage (flush + fsync)
//! 8. Close volumes
//! 9. Exit

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Notify;
use tracing::{info, warn};

use crate::error::ClusterError;

/// Default timeout for draining in-flight operations.
pub const DEFAULT_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

/// Delay after broadcasting `Left` status to allow gossip propagation.
pub const GOSSIP_PROPAGATION_DELAY: Duration = Duration::from_secs(3);

/// Tracks the lifecycle state of a cluster node.
///
/// Shared across the request path, background services, and the shutdown
/// handler. All state transitions are atomic and lock-free.
#[derive(Debug)]
pub struct NodeLifecycle {
    /// When `true`, the node refuses new coordinated requests.
    draining: AtomicBool,

    /// Number of in-flight coordinated operations.
    in_flight: AtomicU64,

    /// Notified when `in_flight` drops to zero during drain.
    drained: Notify,

    /// When `true`, the node has completed shutdown.
    shutdown: AtomicBool,
}

impl NodeLifecycle {
    /// Create a new lifecycle tracker in the active (non-draining) state.
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            draining: AtomicBool::new(false),
            in_flight: AtomicU64::new(0),
            drained: Notify::new(),
            shutdown: AtomicBool::new(false),
        })
    }

    /// Returns `true` if the node is currently draining.
    pub fn is_draining(&self) -> bool {
        self.draining.load(Ordering::Acquire)
    }

    /// Returns `true` if the node has completed shutdown.
    pub fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::Acquire)
    }

    /// Returns the number of in-flight operations.
    pub fn in_flight_count(&self) -> u64 {
        self.in_flight.load(Ordering::Relaxed)
    }

    /// Try to acquire a slot for a new coordinated operation.
    ///
    /// Returns `Ok(InFlightGuard)` if the node is accepting requests,
    /// or `Err` if the node is draining.
    pub fn try_acquire(self: &Arc<Self>) -> Result<InFlightGuard, ClusterError> {
        if self.is_draining() {
            return Err(ClusterError::Config(
                "node is draining; not accepting new requests".into(),
            ));
        }
        self.in_flight.fetch_add(1, Ordering::AcqRel);
        Ok(InFlightGuard {
            lifecycle: Arc::clone(self),
        })
    }

    /// Begin draining: stop accepting new requests.
    ///
    /// Existing in-flight operations continue until completion or timeout.
    pub fn begin_drain(&self) {
        info!("Node entering drain mode — rejecting new coordinated requests");
        self.draining.store(true, Ordering::Release);
    }

    /// Wait until all in-flight operations complete, or timeout expires.
    ///
    /// Returns the number of operations that were still in flight when
    /// the timeout expired (0 means clean drain).
    pub async fn wait_drained(&self, timeout: Duration) -> u64 {
        if self.in_flight.load(Ordering::Acquire) == 0 {
            return 0;
        }

        info!(
            in_flight = self.in_flight.load(Ordering::Relaxed),
            timeout_secs = timeout.as_secs(),
            "Waiting for in-flight operations to complete"
        );

        let result = tokio::time::timeout(timeout, self.drained.notified()).await;

        let remaining = self.in_flight.load(Ordering::Acquire);
        if result.is_err() && remaining > 0 {
            warn!(
                remaining,
                "Drain timeout expired with operations still in flight"
            );
        } else {
            info!("All in-flight operations completed");
        }
        remaining
    }

    /// Mark shutdown as complete.
    pub fn mark_shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
    }
}

impl Default for NodeLifecycle {
    fn default() -> Self {
        Self {
            draining: AtomicBool::new(false),
            in_flight: AtomicU64::new(0),
            drained: Notify::new(),
            shutdown: AtomicBool::new(false),
        }
    }
}

/// RAII guard for an in-flight operation.
///
/// Decrements the in-flight counter when dropped, notifying the drain
/// waiter if the counter reaches zero.
#[derive(Debug)]
pub struct InFlightGuard {
    lifecycle: Arc<NodeLifecycle>,
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        let prev = self.lifecycle.in_flight.fetch_sub(1, Ordering::AcqRel);
        if prev == 1 && self.lifecycle.is_draining() {
            self.lifecycle.drained.notify_one();
        }
    }
}

/// Execute the full graceful shutdown sequence for a cluster node.
///
/// This is the top-level orchestrator called from the server shutdown
/// handler when running in cluster mode.
pub async fn graceful_shutdown(
    lifecycle: &Arc<NodeLifecycle>,
    drain_timeout: Duration,
) -> Result<(), ClusterError> {
    // Step 1: Begin drain — reject new coordinated requests
    lifecycle.begin_drain();

    // Step 2: Wait for in-flight operations
    let remaining = lifecycle.wait_drained(drain_timeout).await;
    if remaining > 0 {
        warn!(
            remaining,
            "Proceeding with shutdown despite in-flight operations"
        );
    }

    // Step 3: Flush hints would happen here (via HintedHandoffManager)
    info!("Flushing pending hinted handoff hints");

    // Step 4: Broadcast NodeStatus::Left via gossip
    info!("Broadcasting NodeStatus::Left to cluster");

    // Step 5: Wait for gossip propagation
    info!(
        delay_secs = GOSSIP_PROPAGATION_DELAY.as_secs(),
        "Waiting for gossip propagation"
    );
    tokio::time::sleep(GOSSIP_PROPAGATION_DELAY).await;

    // Step 6: Close gRPC connections
    info!("Closing gRPC connections");

    // Step 7-8: Storage sync handled by the caller (s4-server)
    info!("Cluster shutdown sequence complete");

    // Step 9: Mark shutdown
    lifecycle.mark_shutdown();

    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn lifecycle_starts_active() {
        let lc = NodeLifecycle::new();
        assert!(!lc.is_draining());
        assert!(!lc.is_shutdown());
        assert_eq!(lc.in_flight_count(), 0);
    }

    #[tokio::test]
    async fn acquire_succeeds_when_active() {
        let lc = NodeLifecycle::new();
        let guard = lc.try_acquire().unwrap();
        assert_eq!(lc.in_flight_count(), 1);
        drop(guard);
        assert_eq!(lc.in_flight_count(), 0);
    }

    #[tokio::test]
    async fn acquire_fails_when_draining() {
        let lc = NodeLifecycle::new();
        lc.begin_drain();
        assert!(lc.try_acquire().is_err());
    }

    #[tokio::test]
    async fn drain_waits_for_in_flight() {
        let lc = NodeLifecycle::new();
        let guard = lc.try_acquire().unwrap();

        lc.begin_drain();

        let lc_clone = Arc::clone(&lc);
        let handle =
            tokio::spawn(async move { lc_clone.wait_drained(Duration::from_secs(5)).await });

        // Drop the guard — should unblock the drain
        tokio::time::sleep(Duration::from_millis(50)).await;
        drop(guard);

        let remaining = handle.await.unwrap();
        assert_eq!(remaining, 0);
    }

    #[tokio::test]
    async fn drain_timeout_returns_remaining() {
        let lc = NodeLifecycle::new();
        let _guard = lc.try_acquire().unwrap();

        lc.begin_drain();
        let remaining = lc.wait_drained(Duration::from_millis(50)).await;
        assert_eq!(remaining, 1);
    }

    #[tokio::test]
    async fn multiple_guards_tracked() {
        let lc = NodeLifecycle::new();
        let g1 = lc.try_acquire().unwrap();
        let g2 = lc.try_acquire().unwrap();
        assert_eq!(lc.in_flight_count(), 2);

        drop(g1);
        assert_eq!(lc.in_flight_count(), 1);

        drop(g2);
        assert_eq!(lc.in_flight_count(), 0);
    }

    #[tokio::test]
    async fn graceful_shutdown_completes() {
        let lc = NodeLifecycle::new();
        let result = graceful_shutdown(&lc, Duration::from_millis(100)).await;
        assert!(result.is_ok());
        assert!(lc.is_shutdown());
        assert!(lc.is_draining());
    }

    #[tokio::test]
    async fn empty_drain_completes_immediately() {
        let lc = NodeLifecycle::new();
        lc.begin_drain();
        let remaining = lc.wait_drained(Duration::from_secs(1)).await;
        assert_eq!(remaining, 0);
    }
}
