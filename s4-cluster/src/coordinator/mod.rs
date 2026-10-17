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
//! - **Bucket configuration**: a configuration read with its bucket's marker

pub mod bucket_config;
pub mod list;
pub mod read;
#[cfg(test)]
pub(crate) mod test_pool;
pub mod write;

pub use bucket_config::{
    config_of_generation, within_clock_tolerance, BucketConfigRead, OwnConfig,
    LEGACY_CONFIG_CLOCK_TOLERANCE,
};
pub use list::{DistributedListCoordinator, DistributedListResult, ListEntry, ListTimeouts};
pub use read::{
    QuorumHeadResult, QuorumReadCoordinator, QuorumReadResult, QuorumReadStreamResult,
    ReadTimeouts, WithHolder,
};
pub use write::{
    QuorumMultipartCompleteResult, QuorumMultipartCreateResult, QuorumMultipartListPartsResult,
    QuorumMultipartPartResult, QuorumWriteCoordinator, WriteTimeouts,
};

use crate::identity::NodeId;
use crate::placement::ReplicationClass;
use crate::protocol::IAM_REPLICATION_MIN_PROTOCOL_VERSION;
use crate::rpc::NodeClient;

/// Whether `node`, a replica of a key of `class`, is sent the key's writes,
/// deletes and reads.
///
/// Every replica is, except that the cluster's control state goes only to
/// nodes that hold it as such (`IAM_REPLICATION_MIN_PROTOCOL_VERSION`): a node
/// of an earlier release would keep it without a digest and delete it without
/// a tombstone. Such a node takes no part in the quorum and gets no hint; the
/// quorum is still a majority of the whole pool, so two of them still meet. A
/// node that has announced no protocol yet — one gossip has just seen come
/// back — is sent the key: a node of this release takes it, and one of an
/// earlier release keeps a record nothing reads, which its upgrade gives a
/// digest.
pub(crate) fn takes_class(
    rpc_client: &NodeClient,
    local_node_id: NodeId,
    node: NodeId,
    class: ReplicationClass,
) -> bool {
    class != ReplicationClass::ClusterControl
        || node == local_node_id
        || rpc_client
            .protocol_version(node)
            .is_none_or(|version| version >= IAM_REPLICATION_MIN_PROTOCOL_VERSION)
}
