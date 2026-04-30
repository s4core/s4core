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

//! Two-level idempotency tracker for quorum writes.
//!
//! Every write operation in the cluster carries a unique `operation_id` (UUID v4).
//! When a coordinator retries a write (e.g., after a timeout), the same
//! `operation_id` is sent. Replicas use this tracker to detect and skip
//! duplicate operations.
//!
//! # Two Levels
//!
//! 1. **Hot cache** (LRU in memory): O(1) lookup for recent operations.
//!    Size: 1M entries (~16 MB). Cleared on restart.
//!
//! 2. **Journal keyspace** (persistent): Source of truth. The `operation_id`
//!    is recorded as part of the atomic batch that commits the write.
//!    Survives restarts.
//!
//! The hot cache is a performance optimization. Correctness relies entirely
//! on the journal keyspace — the worst case when the cache misses is an
//! extra journal lookup, never a duplicate write.

use std::num::NonZeroUsize;
use std::sync::Mutex;

use lru::LruCache;

/// Default number of entries in the LRU hot cache.
const DEFAULT_CACHE_CAPACITY: usize = 1_000_000;

/// Prefix for idempotency keys in the journal keyspace.
///
/// Keys are stored as: `[IDEM_PREFIX | operation_id (16 bytes)]`.
/// This separates idempotency records from journal sequence entries.
const IDEM_PREFIX: u8 = 0xFF;

/// Two-level idempotency tracker.
///
/// Thread-safe: the LRU cache is behind a `Mutex`. The journal keyspace
/// is accessed via fjall which handles concurrency internally.
pub struct IdempotencyTracker {
    hot_cache: Mutex<LruCache<u128, ()>>,
}

impl IdempotencyTracker {
    /// Create a new tracker with the default cache capacity (1M entries).
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_CACHE_CAPACITY)
    }

    /// Create a new tracker with a custom cache capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        let cap = NonZeroUsize::new(capacity).unwrap_or(NonZeroUsize::new(1).unwrap());
        Self {
            hot_cache: Mutex::new(LruCache::new(cap)),
        }
    }

    /// Check if an operation has already been applied.
    ///
    /// Checks the LRU cache first (fast path), then falls back to the
    /// journal keyspace (slow path). If found in the journal but not in
    /// the cache, the entry is promoted to the cache.
    ///
    /// The `journal_lookup` closure is called only if the hot cache misses.
    /// It should check the journal keyspace for the idempotency key.
    pub fn is_duplicate<F>(&self, operation_id: u128, journal_lookup: F) -> bool
    where
        F: FnOnce(&[u8]) -> bool,
    {
        // Fast path: LRU cache
        {
            let mut cache = self.hot_cache.lock().expect("idempotency cache poisoned");
            if cache.get(&operation_id).is_some() {
                return true;
            }
        }

        // Slow path: journal keyspace
        let key = Self::make_key(operation_id);
        let found = journal_lookup(&key);
        if found {
            let mut cache = self.hot_cache.lock().expect("idempotency cache poisoned");
            cache.put(operation_id, ());
        }
        found
    }

    /// Record an operation as applied in the hot cache.
    ///
    /// Called after the atomic batch (which includes the journal entry)
    /// has been committed. The journal write is the durable record;
    /// this just warms the cache.
    pub fn record(&self, operation_id: u128) {
        let mut cache = self.hot_cache.lock().expect("idempotency cache poisoned");
        cache.put(operation_id, ());
    }

    /// Build the journal key for a given operation ID.
    ///
    /// Format: `[0xFF | operation_id as 16 big-endian bytes]` (17 bytes total).
    pub fn make_key(operation_id: u128) -> Vec<u8> {
        let mut key = Vec::with_capacity(17);
        key.push(IDEM_PREFIX);
        key.extend_from_slice(&operation_id.to_be_bytes());
        key
    }

    /// Number of entries currently in the hot cache.
    #[cfg(test)]
    pub fn cache_len(&self) -> usize {
        self.hot_cache.lock().expect("poisoned").len()
    }
}

impl Default for IdempotencyTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_operation_is_not_duplicate() {
        let tracker = IdempotencyTracker::new();
        let op_id: u128 = 42;
        assert!(!tracker.is_duplicate(op_id, |_| false));
    }

    #[test]
    fn recorded_operation_is_duplicate_via_cache() {
        let tracker = IdempotencyTracker::new();
        let op_id: u128 = 42;
        tracker.record(op_id);
        // journal_lookup should NOT be called because cache hits
        assert!(tracker.is_duplicate(op_id, |_| panic!("should not reach journal")));
    }

    #[test]
    fn journal_hit_promotes_to_cache() {
        let tracker = IdempotencyTracker::new();
        let op_id: u128 = 42;

        // First check: cache miss, journal hit
        assert!(tracker.is_duplicate(op_id, |_| true));
        assert_eq!(tracker.cache_len(), 1);

        // Second check: cache hit, journal not called
        assert!(tracker.is_duplicate(op_id, |_| panic!("should not reach journal")));
    }

    #[test]
    fn make_key_format() {
        let key = IdempotencyTracker::make_key(1);
        assert_eq!(key.len(), 17);
        assert_eq!(key[0], 0xFF);
        assert_eq!(&key[1..], &1u128.to_be_bytes());
    }

    #[test]
    fn lru_eviction_works() {
        let tracker = IdempotencyTracker::with_capacity(2);
        tracker.record(1);
        tracker.record(2);
        tracker.record(3); // evicts 1

        // 1 was evicted, should fall through to journal
        let mut journal_called = false;
        tracker.is_duplicate(1, |_| {
            journal_called = true;
            false
        });
        assert!(journal_called);
    }
}
