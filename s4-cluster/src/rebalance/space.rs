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

//! Room on the new nodes: before the move begins, and while it runs.
//!
//! A ring of `n` nodes holds about the same share of the pool's data on
//! each, so after growing from `n_old` to `n_new` nodes every node holds
//! about `n_old / n_new` of what one node of the old ring holds now. A new
//! node is ready when it has room for that share and keeps
//! `S4_REBALANCE_MIN_FREE_RATIO` of its disk free on top of it. While the
//! move runs, a receiver whose free share falls below that ratio is sent
//! nothing: the ranges that would send to it wait, with the reason named.
//!
//! Capacity comes from gossip ([`NodeCapacity`]), measured on the filesystem
//! of each node's data directory. A node that reported none — zeros — is
//! not known to have room, and the move waits for it.

use std::path::PathBuf;

use crate::identity::NodeCapacity;
use crate::NodeId;

/// The free share of a disk, `None` when its size is unknown.
pub fn free_ratio(capacity: &NodeCapacity) -> Option<f64> {
    (capacity.total_bytes > 0)
        .then(|| capacity.available_bytes() as f64 / capacity.total_bytes as f64)
}

/// Whether `node` may receive copies now.
///
/// # Errors
///
/// The reason it may not: no capacity reported, or its free share below
/// `min_free_ratio`.
pub fn check_receiver(
    node: NodeId,
    capacity: Option<&NodeCapacity>,
    min_free_ratio: f64,
) -> Result<(), String> {
    let ratio = capacity
        .and_then(free_ratio)
        .ok_or_else(|| format!("receiver {node} has not reported its disk capacity"))?;
    if ratio < min_free_ratio {
        return Err(format!(
            "receiver {node} has {:.1}% of its disk free, below the {:.1}% the move keeps",
            ratio * 100.0,
            min_free_ratio * 100.0
        ));
    }
    Ok(())
}

/// What one node holds after the ring grows from `old_nodes` to `new_nodes`,
/// estimated from what one node of the old ring holds now.
pub fn share_after_growth(stored_now: u64, old_nodes: usize, new_nodes: usize) -> u64 {
    if new_nodes == 0 {
        return stored_now;
    }
    let share = u128::from(stored_now) * old_nodes as u128 / new_nodes as u128;
    u64::try_from(share).unwrap_or(u64::MAX)
}

/// Whether every new node has room for `share` bytes with `min_free_ratio`
/// of its disk left free.
///
/// # Errors
///
/// The first node that has not, and why.
pub fn check_before_begin(
    new_members: &[(NodeId, Option<NodeCapacity>)],
    share: u64,
    min_free_ratio: f64,
) -> Result<(), String> {
    for (node, capacity) in new_members {
        let capacity = capacity
            .as_ref()
            .filter(|capacity| capacity.total_bytes > 0)
            .ok_or_else(|| format!("waiting for node {node} to report its disk capacity"))?;
        let keep = (capacity.total_bytes as f64 * min_free_ratio).ceil() as u64;
        let needed = share.saturating_add(keep);
        if capacity.available_bytes() < needed {
            return Err(format!(
                "node {node} has {} bytes free; its share of the pool needs {share} and the move \
                 keeps {keep} more free",
                capacity.available_bytes()
            ));
        }
    }
    Ok(())
}

/// What this node stores now.
pub trait StoredBytes: Send + Sync {
    /// Bytes this node's storage takes, `None` when they cannot be measured.
    fn stored_bytes(&self) -> Option<u64>;
}

/// The size of the volume files in a storage directory: every byte the
/// node stores, blobs and metadata blobs alike.
#[derive(Debug, Clone)]
pub struct VolumeFiles {
    directory: PathBuf,
}

impl VolumeFiles {
    /// Measures the volumes in `directory`.
    pub fn new(directory: impl Into<PathBuf>) -> Self {
        Self {
            directory: directory.into(),
        }
    }
}

impl StoredBytes for VolumeFiles {
    fn stored_bytes(&self) -> Option<u64> {
        let entries = std::fs::read_dir(&self.directory).ok()?;
        let mut total = 0u64;
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.starts_with("volume_") && name.ends_with(".dat") {
                total = total.saturating_add(entry.metadata().ok()?.len());
            }
        }
        Some(total)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn disk(total: u64, used: u64) -> NodeCapacity {
        NodeCapacity {
            total_bytes: total,
            used_bytes: used,
            volume_count: 0,
            object_count: 0,
        }
    }

    #[test]
    fn a_receiver_needs_a_reported_disk_with_its_free_share_kept() {
        assert!(check_receiver(NodeId(4), Some(&disk(100, 80)), 0.1).is_ok());
        assert!(check_receiver(NodeId(4), Some(&disk(100, 90)), 0.1).is_ok());
        let full = check_receiver(NodeId(4), Some(&disk(100, 95)), 0.1).unwrap_err();
        assert!(full.contains("below"), "{full}");
        let unknown = check_receiver(NodeId(4), Some(&NodeCapacity::default()), 0.1).unwrap_err();
        assert!(unknown.contains("not reported"), "{unknown}");
        assert!(check_receiver(NodeId(4), None, 0.1).is_err());
    }

    /// Growing 3 → 6 halves what a node holds; a new node needs that share
    /// and the ratio kept free on top of it.
    #[test]
    fn a_move_begins_only_when_every_new_node_has_room_for_its_share() {
        let share = share_after_growth(600, 3, 6);
        assert_eq!(share, 300);
        assert_eq!(share_after_growth(u64::MAX, 6, 3), u64::MAX);
        let roomy = [
            (NodeId(4), Some(disk(1000, 500))),
            (NodeId(5), Some(disk(1000, 600))),
        ];
        assert!(check_before_begin(&roomy, share, 0.1).is_ok());
        let tight = [
            (NodeId(4), Some(disk(1000, 500))),
            (NodeId(5), Some(disk(1000, 601))),
        ];
        let reason = check_before_begin(&tight, share, 0.1).unwrap_err();
        assert!(
            reason.contains("node 5") || reason.contains("5"),
            "{reason}"
        );
        let silent = [(NodeId(4), None)];
        assert!(check_before_begin(&silent, 0, 0.0).unwrap_err().contains("report"));
    }

    #[test]
    fn stored_bytes_count_volume_files_only() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("volume_000000.dat"), vec![0u8; 100]).unwrap();
        std::fs::write(dir.path().join("volume_000001.dat"), vec![0u8; 20]).unwrap();
        std::fs::write(dir.path().join("journal.log"), vec![0u8; 999]).unwrap();
        assert_eq!(VolumeFiles::new(dir.path()).stored_bytes(), Some(120));
        assert_eq!(
            VolumeFiles::new(dir.path().join("missing")).stored_bytes(),
            None
        );
    }
}
