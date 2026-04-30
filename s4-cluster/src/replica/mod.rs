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

//! Replica-side handlers for data plane operations.
//!
//! When a coordinator fans out operations to replica nodes, each replica
//! processes the request through the appropriate handler:
//!
//! - [`ReplicaWriteHandler`] (Phase 4): write + delete with durable ACK
//! - [`ReplicaReadHandler`] (Phase 5): read, head, and list operations

pub mod multipart;
pub mod read;
pub mod write;

pub use multipart::{MultipartReplicaStorage, ReplicaMultipartHandler};
pub use read::ReplicaReadHandler;
pub use write::ReplicaWriteHandler;
