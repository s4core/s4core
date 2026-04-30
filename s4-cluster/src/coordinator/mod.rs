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

//! Coordinator logic for quorum operations.
//!
//! The coordinator is the node that received the S3 HTTP request. It fans
//! out operations to all replicas and waits for quorum responses before
//! returning to the client.
//!
//! - **Write** (Phase 4): fans out PUT/DELETE to W replicas
//! - **Read** (Phase 5): digest-based quorum read with read repair
//! - **List** (Phase 5): scatter-gather LIST across all pool nodes

pub mod list;
pub mod read;
pub mod write;

pub use list::{DistributedListCoordinator, DistributedListResult, ListEntry, ListTimeouts};
pub use read::{
    QuorumHeadResult, QuorumReadCoordinator, QuorumReadResult, QuorumReadStreamResult, ReadTimeouts,
};
pub use write::{
    QuorumMultipartCompleteResult, QuorumMultipartCreateResult, QuorumMultipartPartResult,
    QuorumWriteCoordinator, WriteTimeouts,
};
