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

//! S4 Volume Compactor — background garbage collection for append-only volumes.
//!
//! This crate provides the compaction logic used by s4-server's background
//! worker. It can also be used as a standalone CLI tool for maintenance.
//!
//! # How It Works
//!
//! 1. **Scan**: Read each volume file sequentially, deserializing blob headers.
//! 2. **Classify**: For each blob, check the dedup keyspace — if no entry
//!    points to (volume_id, offset), the blob is dead.
//! 3. **Plan**: Calculate fragmentation ratio per volume. Volumes above the
//!    threshold are candidates for compaction.
//! 4. **Compact**: Copy live blobs to a new volume via VolumeWriter.
//! 5. **Update**: Atomically update IndexRecords and DedupEntries to new locations.
//! 6. **Delete**: Remove old volume file after all references are updated.

pub mod compactor;
pub mod scrubber;

pub use compactor::{CompactionConfig, CompactionResult, CompactionStats, VolumeCompactor};
pub use scrubber::{ScrubResult, VolumeScrubber};
