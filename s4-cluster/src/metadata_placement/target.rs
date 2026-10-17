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

//! How many replicas a pool's bucket metadata needs (plan
//! BUCKET-METADATA-REPLICATION.md, decision 2).
//!
//! `f` is the number of node failures the pool is guaranteed to survive: for
//! a replicated pool, the failures its quorums still reach past,
//! `min(RF − W, RF − R)`; for an erasure-coded pool, the shard losses every
//! erasure set of the pool decodes past, which the Enterprise edition
//! computes from the profile. The bucket metadata needs a majority quorum
//! that survives as many: `2f + 1` replicas, never fewer than the data has
//! and never more than the pool has nodes.

use serde::{Deserialize, Serialize};

use crate::error::ClusterError;

/// The replicas of a class of keys and the quorums their writes and reads
/// need, without the ring that names the replicas.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct QuorumShape {
    /// Replicas of every key.
    pub replication_factor: u8,
    /// Acknowledgements a write needs.
    pub write_quorum: u8,
    /// Answers a read needs.
    pub read_quorum: u8,
}

impl QuorumShape {
    /// `replication_factor` replicas, written and read through a majority.
    pub fn majority(replication_factor: u8) -> Self {
        let quorum = replication_factor / 2 + 1;
        Self {
            replication_factor,
            write_quorum: quorum,
            read_quorum: quorum,
        }
    }

    /// The node failures a key of this shape stays writable and readable
    /// past.
    pub fn tolerated_failures(self) -> u8 {
        let rf = self.replication_factor;
        rf.saturating_sub(self.write_quorum).min(rf.saturating_sub(self.read_quorum))
    }
}

impl std::fmt::Display for QuorumShape {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} replicas, W={} R={}",
            self.replication_factor, self.write_quorum, self.read_quorum
        )
    }
}

/// The shape a pool's bucket metadata is placed by.
///
/// `nodes` is the number of nodes of the pool and `data` the shape of its
/// data. `erasure_tolerance` is the number of node failures every erasure set
/// of the pool decodes past, `None` for a replicated pool. `requested` is the
/// operator's `S4_BUCKET_METADATA_REPLICATION_FACTOR`, which may raise the
/// derived replication factor up to the pool's nodes.
///
/// A target with as many replicas as the data keeps the data's quorums: the
/// pool has nothing to move, and its bucket metadata stays placed exactly as
/// it was. Any other target is read and written through a majority.
///
/// # Errors
///
/// A requested replication factor below the derived one, which would leave
/// the bucket metadata weaker than the pool, or above the pool's nodes.
pub fn derive_target(
    nodes: usize,
    data: QuorumShape,
    erasure_tolerance: Option<u8>,
    requested: Option<u8>,
) -> Result<QuorumShape, ClusterError> {
    let nodes = u8::try_from(nodes).unwrap_or(u8::MAX);
    let tolerated = erasure_tolerance.unwrap_or_else(|| data.tolerated_failures());
    let derived = data
        .replication_factor
        .max(tolerated.saturating_mul(2).saturating_add(1))
        .min(nodes);
    let replication_factor = match requested {
        None => derived,
        Some(requested) if requested < derived => {
            return Err(ClusterError::Config(format!(
                "S4_BUCKET_METADATA_REPLICATION_FACTOR={requested} is below {derived}, the \
                 replicas the bucket metadata needs to survive the {tolerated} node failure(s) \
                 the pool survives; leave it unset or raise it"
            )));
        }
        Some(requested) if requested > nodes => {
            return Err(ClusterError::Config(format!(
                "S4_BUCKET_METADATA_REPLICATION_FACTOR={requested} is above {nodes}, the nodes \
                 of the pool"
            )));
        }
        Some(requested) => requested,
    };
    if replication_factor == data.replication_factor {
        Ok(data)
    } else {
        Ok(QuorumShape::majority(replication_factor))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shape(rf: u8, w: u8, r: u8) -> QuorumShape {
        QuorumShape {
            replication_factor: rf,
            write_quorum: w,
            read_quorum: r,
        }
    }

    /// RF 3 with quorums of 2 survives one failure, as its bucket metadata
    /// already does: nothing changes. RF 5 with quorums of 3 survives two.
    #[test]
    fn a_replicated_pool_keeps_the_shape_of_its_data() {
        assert_eq!(
            derive_target(3, shape(3, 2, 2), None, None).unwrap(),
            shape(3, 2, 2)
        );
        assert_eq!(
            derive_target(6, shape(3, 2, 2), None, None).unwrap(),
            shape(3, 2, 2)
        );
        assert_eq!(
            derive_target(9, shape(5, 3, 3), None, None).unwrap(),
            shape(5, 3, 3)
        );
        // Quorums that leave no failure keep the data's own.
        assert_eq!(
            derive_target(6, shape(3, 3, 1), None, None).unwrap(),
            shape(3, 3, 1)
        );
    }

    /// EC 4+2 survives two failures: five replicas, quorums of three. EC 6+3
    /// on nine nodes survives three: seven replicas, quorums of four. A pool
    /// narrower than the target places the metadata on every node.
    #[test]
    fn an_erasure_coded_pool_derives_from_its_tolerance() {
        assert_eq!(
            derive_target(6, shape(3, 2, 2), Some(2), None).unwrap(),
            shape(5, 3, 3)
        );
        assert_eq!(
            derive_target(12, shape(3, 2, 2), Some(2), None).unwrap(),
            shape(5, 3, 3)
        );
        assert_eq!(
            derive_target(9, shape(3, 2, 2), Some(3), None).unwrap(),
            shape(7, 4, 4)
        );
        assert_eq!(
            derive_target(4, shape(3, 2, 2), Some(2), None).unwrap(),
            shape(4, 3, 3)
        );
    }

    /// The operator may raise the target up to the pool's nodes; below the
    /// derived target or above the nodes, the start is refused with the
    /// reason.
    #[test]
    fn the_requested_replication_factor_only_raises_the_target() {
        assert_eq!(
            derive_target(6, shape(3, 2, 2), Some(2), Some(5)).unwrap(),
            shape(5, 3, 3)
        );
        assert_eq!(
            derive_target(6, shape(3, 2, 2), Some(2), Some(6)).unwrap(),
            shape(6, 4, 4)
        );
        assert_eq!(
            derive_target(5, shape(3, 2, 2), None, Some(5)).unwrap(),
            shape(5, 3, 3)
        );
        let below = derive_target(6, shape(3, 2, 2), Some(2), Some(4)).unwrap_err().to_string();
        assert!(below.contains("below 5"), "{below}");
        let above = derive_target(6, shape(3, 2, 2), Some(2), Some(7)).unwrap_err().to_string();
        assert!(above.contains("above 6"), "{above}");
        let below_data = derive_target(3, shape(3, 2, 2), None, Some(2)).unwrap_err().to_string();
        assert!(below_data.contains("below 3"), "{below_data}");
    }
}
