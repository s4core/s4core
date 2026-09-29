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

//! Measuring how much room a node still has.
//!
//! [`NodeCapacity`] has been part of the gossip metadata since the cluster
//! shipped, but nothing ever filled it: every node broadcast zeros. That was
//! harmless while nobody read it, and stopped being harmless once erasure-coded
//! placement started asking which set has room.
//!
//! What is measured is the filesystem holding the data directory, not S4's own
//! usage of it. A pool cannot write into space a log file or another service has
//! taken, so free space as the operating system reports it is the honest answer
//! to "can this node still accept shards".
//!
//! Every failure here is reported as zeros — the value that means "unknown" to
//! every reader. A node that cannot measure its disk must not look full, and it
//! must not stop the cluster from placing data.

use std::path::{Path, PathBuf};

use crate::identity::NodeCapacity;

/// Where the gossip loop reads this node's storage capacity from.
///
/// A port rather than a direct call, so a test can hand the loop fixed numbers
/// and the production node can hand it the filesystem.
pub trait NodeCapacityProvider: Send + Sync + 'static {
    /// Measure the node's storage right now.
    ///
    /// Called on the gossip metadata tick, so it must be cheap and must not
    /// block for long. Returning [`NodeCapacity::default`] is the way to say
    /// "could not measure".
    fn measure(&self) -> NodeCapacity;
}

/// Capacity of the filesystem holding a directory.
#[derive(Debug, Clone)]
pub struct FilesystemCapacity {
    path: PathBuf,
}

impl FilesystemCapacity {
    /// Measure the filesystem that holds `path` — normally the data directory.
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    /// Path whose filesystem is measured.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl NodeCapacityProvider for FilesystemCapacity {
    fn measure(&self) -> NodeCapacity {
        measure_filesystem(&self.path).unwrap_or_default()
    }
}

/// Total and free bytes of the filesystem holding `path`.
///
/// `None` when the platform cannot be asked or the call fails. Callers turn that
/// into zeros, which every reader treats as "unknown".
#[cfg(unix)]
fn measure_filesystem(path: &Path) -> Option<NodeCapacity> {
    let stats = match rustix::fs::statvfs(path) {
        Ok(stats) => stats,
        Err(error) => {
            tracing::debug!(
                path = %path.display(),
                error = %error,
                "could not measure the data directory filesystem; reporting unknown capacity"
            );
            return None;
        }
    };

    // `f_frsize` is the fragment size, which is what the block counts are
    // expressed in. `f_bsize` is the preferred I/O size and is the wrong
    // multiplier, even though the two usually match.
    let block_size = stats.f_frsize;
    let total_bytes = stats.f_blocks.checked_mul(block_size)?;

    // `f_bavail`, not `f_bfree`: the kernel reserves part of the free space for
    // root, and S4 does not run as root. Reporting the reserved blocks as free
    // would promise room that a write would be refused.
    let available_bytes = stats.f_bavail.checked_mul(block_size)?;

    Some(NodeCapacity {
        total_bytes,
        used_bytes: total_bytes.saturating_sub(available_bytes),
        // Left at zero deliberately: these count what S4 stores, which is a
        // question for the storage engine rather than for the filesystem.
        volume_count: 0,
        object_count: 0,
    })
}

#[cfg(not(unix))]
fn measure_filesystem(_path: &Path) -> Option<NodeCapacity> {
    // No portable way to ask, and guessing would be worse than admitting it:
    // capacity-aware placement degrades to an even spread, which is exactly
    // what it does everywhere else that capacity is unknown.
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A real directory has a real filesystem behind it, and the numbers have to
    /// be internally consistent — otherwise placement would weigh nonsense.
    #[test]
    fn a_real_directory_reports_a_measured_filesystem() {
        let dir = tempfile::tempdir().unwrap();
        let capacity = FilesystemCapacity::new(dir.path()).measure();

        assert!(capacity.total_bytes > 0, "a mounted filesystem has a size");
        assert!(capacity.used_bytes <= capacity.total_bytes);
        assert_eq!(
            capacity.available_bytes(),
            capacity.total_bytes - capacity.used_bytes
        );
    }

    /// A path that is not there reads as unknown, not as full.
    ///
    /// This is the load-bearing failure mode: a node whose data directory has
    /// gone missing must not be reported as a node with no room, or placement
    /// would quietly route around a node that has a much bigger problem.
    #[test]
    fn a_missing_path_reports_unknown_rather_than_full() {
        let capacity = FilesystemCapacity::new("/nonexistent/s4/data/dir").measure();

        assert_eq!(capacity, NodeCapacity::default());
        assert_eq!(capacity.total_bytes, 0, "zero total means unknown");
    }
}
