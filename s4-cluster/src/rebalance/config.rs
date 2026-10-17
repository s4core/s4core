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

//! How fast, and how far, the move of a grown pool's keys may go.

use std::time::Duration;

use crate::ClusterError;

/// The limits of the move (`S4_REBALANCE_*`).
#[derive(Debug, Clone, PartialEq)]
pub struct RebalanceConfig {
    /// Bytes of copies this node sends per second, at most
    /// (`S4_REBALANCE_MAX_BYTES_PER_SEC`).
    pub max_bytes_per_sec: u64,
    /// Keys this node settles per second, at most
    /// (`S4_REBALANCE_MAX_KEYS_PER_SEC`).
    pub max_keys_per_sec: u32,
    /// Ranges this node moves at the same time (`S4_REBALANCE_CONCURRENT_RANGES`).
    pub concurrent_ranges: usize,
    /// Share of its disk a receiving node keeps free
    /// (`S4_REBALANCE_MIN_FREE_RATIO`): a move does not begin while a new
    /// node lacks room for its share with this much left over, and a range
    /// waits while one of its receivers is below it.
    pub min_free_ratio: f64,
    /// Keys read from the version digests per page.
    pub page_size: usize,
    /// How long a range waits before a key that could not be settled, or a
    /// receiver short of space, is tried again.
    pub retry_after: Duration,
    /// Quorum operations in flight on this node at which the move gives way
    /// to the clients' traffic before its next key.
    pub yield_at_in_flight: u64,
    /// How long after every member announced the move the records of
    /// `__system__` without digests and the open uploads wait before this node
    /// hands them over: the longest a write placed by the earlier generation
    /// may still land. No range is verified for them, so none may land after.
    pub settle: Duration,
}

impl Default for RebalanceConfig {
    fn default() -> Self {
        Self {
            max_bytes_per_sec: 64 * 1024 * 1024,
            max_keys_per_sec: 500,
            concurrent_ranges: 2,
            min_free_ratio: 0.10,
            page_size: 256,
            retry_after: Duration::from_secs(30),
            yield_at_in_flight: 32,
            settle: Duration::from_secs(10),
        }
    }
}

impl RebalanceConfig {
    /// Validates the limits.
    ///
    /// # Errors
    ///
    /// A zero rate, page or concurrency, or a free ratio outside `[0, 1)`.
    pub fn validate(&self) -> Result<(), ClusterError> {
        let invalid = |what: &str| Err(ClusterError::Config(what.to_string()));
        if self.max_bytes_per_sec == 0 {
            return invalid("S4_REBALANCE_MAX_BYTES_PER_SEC must be > 0");
        }
        if self.max_keys_per_sec == 0 {
            return invalid("S4_REBALANCE_MAX_KEYS_PER_SEC must be > 0");
        }
        if self.concurrent_ranges == 0 {
            return invalid("S4_REBALANCE_CONCURRENT_RANGES must be > 0");
        }
        if !(0.0..1.0).contains(&self.min_free_ratio) {
            return invalid("S4_REBALANCE_MIN_FREE_RATIO must be at least 0 and below 1");
        }
        if self.page_size == 0 {
            return invalid("the rebalance page size must be > 0");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_valid_and_each_limit_is_checked() {
        assert!(RebalanceConfig::default().validate().is_ok());
        for broken in [
            RebalanceConfig {
                max_bytes_per_sec: 0,
                ..RebalanceConfig::default()
            },
            RebalanceConfig {
                max_keys_per_sec: 0,
                ..RebalanceConfig::default()
            },
            RebalanceConfig {
                concurrent_ranges: 0,
                ..RebalanceConfig::default()
            },
            RebalanceConfig {
                min_free_ratio: 1.0,
                ..RebalanceConfig::default()
            },
            RebalanceConfig {
                min_free_ratio: -0.1,
                ..RebalanceConfig::default()
            },
            RebalanceConfig {
                page_size: 0,
                ..RebalanceConfig::default()
            },
        ] {
            assert!(broken.validate().is_err(), "{broken:?}");
        }
    }
}
