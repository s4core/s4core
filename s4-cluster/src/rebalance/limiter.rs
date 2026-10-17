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

//! Keeping the move within its rates, and out of the clients' way.

use std::sync::Mutex;
use std::time::Duration;

use tokio::time::Instant;

/// A rate shared by every range moved at once: at most `rate` units per
/// second on average, with up to one second's worth taken at once.
///
/// What was spent is paid for afterwards: [`Self::spend`] takes the units
/// and, when they run past what the rate allowed so far, waits until it
/// catches up. A copy's size is known only once it is read, so the bytes of
/// a copy are spent after it is sent, and the next one waits.
#[derive(Debug)]
pub struct RateLimiter {
    rate: f64,
    state: Mutex<Bucket>,
}

#[derive(Debug)]
struct Bucket {
    /// Units that may be spent now; negative after spending ahead.
    tokens: f64,
    refilled_at: Instant,
}

impl RateLimiter {
    /// A limiter of `rate` units per second.
    pub fn per_second(rate: u64) -> Self {
        let rate = rate.max(1) as f64;
        Self {
            rate,
            state: Mutex::new(Bucket {
                tokens: rate,
                refilled_at: Instant::now(),
            }),
        }
    }

    /// Spends `units`, and waits while the rate is overdrawn.
    pub async fn spend(&self, units: u64) {
        let wait = {
            let mut bucket = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            let now = Instant::now();
            let elapsed = now.duration_since(bucket.refilled_at).as_secs_f64();
            bucket.tokens = (bucket.tokens + elapsed * self.rate).min(self.rate);
            bucket.refilled_at = now;
            bucket.tokens -= units as f64;
            (bucket.tokens < 0.0).then(|| Duration::from_secs_f64(-bucket.tokens / self.rate))
        };
        if let Some(wait) = wait {
            tokio::time::sleep(wait).await;
        }
    }
}

/// How busy this node is with the clients' traffic.
pub trait ForegroundLoad: Send + Sync {
    /// Client operations in flight on this node now.
    fn in_flight(&self) -> u64;
}

/// The quorum operations this node coordinates: every client read, write and
/// delete in cluster mode is one.
#[derive(Debug, Default)]
pub struct QuorumLoad;

impl ForegroundLoad for QuorumLoad {
    fn in_flight(&self) -> u64 {
        crate::cluster_metrics::quorum_operations_in_flight()
    }
}

/// Waits, a little at a time and at most `limit`, while the clients have at
/// least `threshold` operations in flight: the move takes the gaps between
/// them.
pub async fn give_way(load: &dyn ForegroundLoad, threshold: u64, limit: Duration) {
    let step = Duration::from_millis(20);
    let mut waited = Duration::ZERO;
    while load.in_flight() >= threshold && waited < limit {
        tokio::time::sleep(step).await;
        waited += step;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// At ten units a second, the first ten pass at once and forty more take
    /// four seconds; spending ahead makes the next caller wait.
    #[tokio::test(start_paused = true)]
    async fn spending_beyond_the_rate_waits_for_it() {
        let limiter = RateLimiter::per_second(10);
        let started = Instant::now();
        limiter.spend(10).await;
        assert!(started.elapsed() < Duration::from_millis(1));
        for _ in 0..40 {
            limiter.spend(1).await;
        }
        let elapsed = started.elapsed();
        assert!(
            elapsed >= Duration::from_millis(3_900) && elapsed <= Duration::from_millis(4_100),
            "{elapsed:?}"
        );
        limiter.spend(30).await;
        assert!(started.elapsed() >= Duration::from_millis(6_900));
    }

    struct Busy(AtomicU64);

    impl ForegroundLoad for Busy {
        fn in_flight(&self) -> u64 {
            self.0.load(Ordering::Relaxed)
        }
    }

    #[tokio::test(start_paused = true)]
    async fn the_move_gives_way_while_clients_are_busy_but_not_forever() {
        let idle = Busy(AtomicU64::new(3));
        let started = Instant::now();
        give_way(&idle, 4, Duration::from_secs(1)).await;
        assert_eq!(started.elapsed(), Duration::ZERO);
        let busy = Busy(AtomicU64::new(4));
        give_way(&busy, 4, Duration::from_secs(1)).await;
        assert!(started.elapsed() >= Duration::from_secs(1));
        assert!(started.elapsed() < Duration::from_millis(1_100));
    }
}
