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

//! Bucket-to-pool assignment.
//!
//! Every bucket is pinned to exactly one server pool at creation time.
//! All objects within a bucket reside in that pool. This keeps LIST
//! operations, versioning, and atomic batches scoped to a single pool.
//!
//! # Assignment policy
//!
//! New buckets are assigned to the pool with the most available capacity.
//! Existing buckets are never moved between pools.

use std::collections::HashMap;

use crate::error::ClusterError;
use crate::identity::PoolId;

/// Thread-safe bucket-to-pool mapping.
///
/// Wraps a `HashMap<String, PoolId>` with domain-specific operations.
/// In a running server this is backed by persistent storage (fjall
/// bucket keyspace); this in-memory structure serves as the fast-path
/// lookup and is rebuilt from the keyspace on startup.
#[derive(Debug, Clone)]
pub struct BucketPlacement {
    /// bucket_name → pool_id.
    mapping: HashMap<String, PoolId>,
}

impl BucketPlacement {
    /// Create an empty placement map.
    pub fn new() -> Self {
        Self {
            mapping: HashMap::new(),
        }
    }

    /// Rebuild from a collection of `(bucket_name, pool_id)` pairs.
    ///
    /// Used on startup to hydrate the in-memory map from persistent state.
    /// See also the [`FromIterator`] implementation.
    pub fn from_entries(entries: impl IntoIterator<Item = (String, PoolId)>) -> Self {
        Self {
            mapping: entries.into_iter().collect(),
        }
    }

    /// Look up the pool for an existing bucket.
    pub fn get(&self, bucket: &str) -> Option<PoolId> {
        self.mapping.get(bucket).copied()
    }

    /// Assign a bucket to a pool.
    ///
    /// Returns an error if the bucket is already assigned to a different
    /// pool (buckets never move).
    pub fn assign(&mut self, bucket: &str, pool_id: PoolId) -> Result<(), ClusterError> {
        if let Some(&existing) = self.mapping.get(bucket) {
            if existing != pool_id {
                return Err(ClusterError::Placement(format!(
                    "bucket '{}' is already assigned to {} (requested {})",
                    bucket, existing, pool_id,
                )));
            }
            return Ok(());
        }
        self.mapping.insert(bucket.to_owned(), pool_id);
        Ok(())
    }

    /// Remove a bucket assignment (bucket deletion).
    pub fn remove(&mut self, bucket: &str) -> Option<PoolId> {
        self.mapping.remove(bucket)
    }

    /// Number of tracked buckets.
    pub fn len(&self) -> usize {
        self.mapping.len()
    }

    /// True if no buckets are tracked.
    pub fn is_empty(&self) -> bool {
        self.mapping.is_empty()
    }

    /// Iterate over all assignments.
    pub fn iter(&self) -> impl Iterator<Item = (&str, PoolId)> {
        self.mapping.iter().map(|(k, v)| (k.as_str(), *v))
    }

    /// Select the best pool for a new bucket based on available capacity.
    ///
    /// `pool_capacities` maps each pool ID to `(used_bytes, total_bytes)`.
    /// Returns the pool with the most free space, or `None` if the list
    /// is empty.
    pub fn select_pool(pool_capacities: &[(PoolId, u64, u64)]) -> Option<PoolId> {
        pool_capacities
            .iter()
            .max_by_key(|&&(_, used, total)| total.saturating_sub(used))
            .map(|&(pool_id, _, _)| pool_id)
    }
}

impl Default for BucketPlacement {
    fn default() -> Self {
        Self::new()
    }
}

impl FromIterator<(String, PoolId)> for BucketPlacement {
    fn from_iter<T: IntoIterator<Item = (String, PoolId)>>(iter: T) -> Self {
        Self {
            mapping: iter.into_iter().collect(),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_is_empty() {
        let bp = BucketPlacement::new();
        assert!(bp.is_empty());
        assert_eq!(bp.len(), 0);
    }

    #[test]
    fn assign_and_get() {
        let mut bp = BucketPlacement::new();
        bp.assign("photos", PoolId(1)).unwrap();
        assert_eq!(bp.get("photos"), Some(PoolId(1)));
        assert_eq!(bp.len(), 1);
    }

    #[test]
    fn idempotent_assign() {
        let mut bp = BucketPlacement::new();
        bp.assign("data", PoolId(2)).unwrap();
        bp.assign("data", PoolId(2)).unwrap(); // same pool — OK
        assert_eq!(bp.get("data"), Some(PoolId(2)));
    }

    #[test]
    fn reassign_to_different_pool_rejected() {
        let mut bp = BucketPlacement::new();
        bp.assign("data", PoolId(1)).unwrap();
        let err = bp.assign("data", PoolId(2)).unwrap_err();
        assert!(err.to_string().contains("already assigned"));
    }

    #[test]
    fn remove_returns_old_pool() {
        let mut bp = BucketPlacement::new();
        bp.assign("logs", PoolId(3)).unwrap();
        assert_eq!(bp.remove("logs"), Some(PoolId(3)));
        assert!(bp.get("logs").is_none());
        assert!(bp.is_empty());
    }

    #[test]
    fn remove_nonexistent_returns_none() {
        let mut bp = BucketPlacement::new();
        assert_eq!(bp.remove("ghost"), None);
    }

    #[test]
    fn from_iter_builds_mapping() {
        let bp = BucketPlacement::from_entries(vec![
            ("a".to_owned(), PoolId(1)),
            ("b".to_owned(), PoolId(2)),
        ]);
        assert_eq!(bp.get("a"), Some(PoolId(1)));
        assert_eq!(bp.get("b"), Some(PoolId(2)));
        assert_eq!(bp.len(), 2);
    }

    #[test]
    fn select_pool_picks_most_free_space() {
        let capacities = vec![
            (PoolId(1), 800, 1000), // 200 free
            (PoolId(2), 100, 1000), // 900 free  <-- best
            (PoolId(3), 500, 1000), // 500 free
        ];
        assert_eq!(BucketPlacement::select_pool(&capacities), Some(PoolId(2)));
    }

    #[test]
    fn select_pool_empty_list() {
        assert_eq!(BucketPlacement::select_pool(&[]), None);
    }

    #[test]
    fn select_pool_with_zero_capacity() {
        let capacities = vec![
            (PoolId(1), 1000, 1000), // 0 free
            (PoolId(2), 999, 1000),  // 1 free
        ];
        assert_eq!(BucketPlacement::select_pool(&capacities), Some(PoolId(2)));
    }

    #[test]
    fn iter_yields_all_entries() {
        let bp = BucketPlacement::from_entries(vec![
            ("x".to_owned(), PoolId(10)),
            ("y".to_owned(), PoolId(20)),
        ]);
        let mut entries: Vec<_> = bp.iter().collect();
        entries.sort_by_key(|(k, _)| k.to_string());
        assert_eq!(entries, vec![("x", PoolId(10)), ("y", PoolId(20))]);
    }
}
