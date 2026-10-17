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

//! Whether this node's copy of IAM may be trusted to check credentials.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Why IAM credentials are refused on this node, for now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IamRefusal {
    /// No majority of the pool has agreed with this node's copy since it
    /// started: it may have missed changes while it was down.
    Starting,
    /// No majority of the pool has agreed with the copy for longer than the
    /// staleness bound: the node may be cut off from the changes.
    Stale,
}

impl IamRefusal {
    /// The value of the `reason` label of `s4_iam_auth_refused_total`.
    pub fn label(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Stale => "stale",
        }
    }
}

impl std::fmt::Display for IamRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Starting => write!(
                f,
                "this node has not yet confirmed its copy of IAM with a majority of its pool"
            ),
            Self::Stale => write!(
                f,
                "this node has not confirmed its copy of IAM with a majority of its pool for \
                 longer than S4_IAM_MAX_STALENESS_SECS"
            ),
        }
    }
}

/// When a majority of the pool last agreed with this node's copy of IAM, and
/// the gate that follows from it.
#[derive(Debug)]
pub struct IamFreshness {
    origin: Instant,
    /// Milliseconds from `origin` to the last confirmation, plus one; zero
    /// before the first.
    confirmed: AtomicU64,
    max_staleness: Option<Duration>,
}

impl IamFreshness {
    /// A gate closed until the first confirmation, refusing a copy older than
    /// `max_staleness` after it; zero takes the bound away.
    pub fn new(max_staleness: Duration) -> Self {
        Self {
            origin: Instant::now(),
            confirmed: AtomicU64::new(0),
            max_staleness: (!max_staleness.is_zero()).then_some(max_staleness),
        }
    }

    /// The staleness bound, `None` when there is none.
    pub fn max_staleness(&self) -> Option<Duration> {
        self.max_staleness
    }

    /// A majority of the pool agreed with the copy as it was at `at`.
    pub fn confirm(&self, at: Instant) {
        let since_origin = at.saturating_duration_since(self.origin).as_millis();
        let mark = u64::try_from(since_origin).unwrap_or(u64::MAX - 1) + 1;
        self.confirmed.fetch_max(mark, Ordering::AcqRel);
    }

    /// How long ago a majority last agreed with the copy; `None` before the
    /// first time.
    pub fn since_confirmed(&self) -> Option<Duration> {
        match self.confirmed.load(Ordering::Acquire) {
            0 => None,
            mark => {
                let at = self.origin + Duration::from_millis(mark - 1);
                Some(Instant::now().saturating_duration_since(at))
            }
        }
    }

    /// Whether IAM credentials may be checked against the copy now.
    ///
    /// # Errors
    ///
    /// [`IamRefusal`] while they may not.
    pub fn check(&self) -> Result<(), IamRefusal> {
        let Some(age) = self.since_confirmed() else {
            return Err(IamRefusal::Starting);
        };
        match self.max_staleness {
            Some(bound) if age > bound => Err(IamRefusal::Stale),
            _ => Ok(()),
        }
    }

    /// [`Self::check`] for a request that carries IAM credentials: a refusal
    /// is counted in `s4_iam_auth_refused_total`.
    ///
    /// # Errors
    ///
    /// [`IamRefusal`] while they may not be checked.
    pub fn admit(&self) -> Result<(), IamRefusal> {
        self.check().inspect_err(|refusal| {
            crate::cluster_metrics::record_iam_auth_refused(refusal.label());
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn closed_until_confirmed_then_open_until_the_bound() {
        let freshness = IamFreshness::new(Duration::from_secs(30));
        assert_eq!(freshness.check(), Err(IamRefusal::Starting));
        assert_eq!(freshness.since_confirmed(), None);

        freshness.confirm(Instant::now());
        assert_eq!(freshness.check(), Ok(()));
        assert!(freshness.since_confirmed().unwrap() < Duration::from_secs(1));
    }

    #[test]
    fn a_confirmation_older_than_the_bound_is_stale() {
        let freshness = IamFreshness::new(Duration::from_millis(20));
        freshness.confirm(Instant::now());
        std::thread::sleep(Duration::from_millis(40));
        assert_eq!(freshness.check(), Err(IamRefusal::Stale));
        // A later confirmation opens it again, an earlier one does not.
        let earlier = Instant::now() - Duration::from_millis(30);
        freshness.confirm(Instant::now());
        freshness.confirm(earlier);
        assert_eq!(freshness.check(), Ok(()));
    }

    #[test]
    fn zero_takes_the_bound_away_but_not_the_start() {
        let freshness = IamFreshness::new(Duration::ZERO);
        assert_eq!(freshness.max_staleness(), None);
        assert_eq!(freshness.check(), Err(IamRefusal::Starting));
        freshness.confirm(Instant::now());
        std::thread::sleep(Duration::from_millis(20));
        assert_eq!(freshness.check(), Ok(()));
    }

    #[test]
    fn refusals_name_their_reason() {
        assert_eq!(IamRefusal::Starting.label(), "starting");
        assert_eq!(IamRefusal::Stale.label(), "stale");
    }
}
