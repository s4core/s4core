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

//! Admission of mutations against an immutable pool placement generation.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::{error::ClusterError, rpc::proto};

/// The lowest placement generation a node admits, shared by its replica
/// handlers and its write coordinator. It only rises: once the pool's move
/// began here, a coordinator of the earlier generation is refused
/// ([`check`]).
#[derive(Debug, Clone, Default)]
pub struct PlacementFloor(Arc<AtomicU64>);

impl PlacementFloor {
    /// A floor at `minimum`; zero admits every generation.
    pub fn new(minimum: u64) -> Self {
        Self(Arc::new(AtomicU64::new(minimum)))
    }

    /// The floor now.
    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Acquire)
    }

    /// Raises the floor to `minimum`; a lower value leaves it.
    pub fn raise(&self, minimum: u64) {
        self.0.fetch_max(minimum, Ordering::AcqRel);
    }
}

/// Refuse mutations issued before `minimum`, except HLC-ordered repair copies
/// and records placed directly on an erasure set rather than on the pool ring.
/// The floor only rises ([`PlacementFloor`]): a coordinator that placed a key
/// by the earlier generation is refused from the moment the move began here.
pub fn check(
    minimum: u64,
    generation: u64,
    bucket: &str,
    key: &str,
    repair_copy: bool,
    hlc: Option<&proto::HybridTimestamp>,
) -> Result<(), ClusterError> {
    let direct_ec = bucket == "__system__"
        && [
            "__s4_ec_v1/shards/",
            "__s4_ec_v1/manifests/",
            "__s4_ec_v1/small/",
        ]
        .iter()
        .any(|prefix| key.starts_with(prefix));
    let ordered_repair = repair_copy && hlc.is_some_and(|stamp| stamp.wall_time != 0);
    if minimum == 0 || generation >= minimum || direct_ec || ordered_repair {
        return Ok(());
    }
    crate::cluster_metrics::record_pool_generation_fenced_write();
    Err(ClusterError::PlacementGenerationFenced {
        coordinator: generation,
        minimum,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The floor shared by a node's handlers only rises.
    #[test]
    fn the_shared_floor_only_rises() {
        let floor = PlacementFloor::new(1);
        let shared = floor.clone();
        floor.raise(2);
        assert_eq!(shared.get(), 2);
        shared.raise(1);
        assert_eq!(floor.get(), 2);
        assert!(check(floor.get(), 1, "bucket", "key", false, None).is_err());
        assert!(check(floor.get(), 2, "bucket", "key", false, None).is_ok());
    }

    #[test]
    fn no_floor_and_current_generations_remain_admitted() {
        assert!(check(0, 0, "bucket", "key", false, None).is_ok());
        assert!(check(2, 2, "bucket", "key", false, None).is_ok());
        assert!(check(2, 3, "bucket", "key", false, None).is_ok());
    }

    #[test]
    fn rejects_legacy_mutations_and_unordered_repairs() {
        for repair in [false, true] {
            assert!(check(2, 0, "bucket", "key", repair, None).is_err());
            assert!(check(
                2,
                1,
                "bucket",
                "key",
                repair,
                Some(&proto::HybridTimestamp::default())
            )
            .is_err());
        }
    }

    #[test]
    fn preserves_ring_metadata_fence_and_direct_ec_records() {
        for key in [
            "__s4_ec_v1/tiering/b",
            "__s4_ec_v1/pools/1/erasure-sets",
            "__s4_ec_v1/pools/1/set-layouts/1",
            "__s4_pool_generation_v1/1",
        ] {
            assert!(
                check(2, 0, "__system__", key, false, None).is_err(),
                "{key}"
            );
        }
        for key in [
            "__s4_ec_v1/shards/1",
            "__s4_ec_v1/manifests/1",
            "__s4_ec_v1/small/1",
        ] {
            assert!(check(2, 0, "__system__", key, false, None).is_ok());
            assert!(check(2, 0, "ordinary", key, false, None).is_err());
        }
    }
}
