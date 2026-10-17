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

/// How a replica's answer begins when it refused a copy whose bytes do not
/// match the SHA-256 the copy was sent with (`verify_content`).
pub const CHECKSUM_MISMATCH: &str = "checksum mismatch";

/// Why a copy sent with `verify_content` is refused: its bytes differ from the
/// SHA-256 it was sent with. `None` when it is not to be checked, carries no
/// whole-object hash (all zeros, as a multipart object's), or matches.
pub(crate) fn content_mismatch(request: &crate::rpc::proto::WriteBlobRequest) -> Option<String> {
    use sha2::Digest;
    if !request.verify_content
        || request.content_hash.len() != 32
        || request.content_hash.iter().all(|byte| *byte == 0)
    {
        return None;
    }
    let actual = sha2::Sha256::digest(&request.data);
    (actual.as_slice() != request.content_hash.as_slice()).then(|| {
        format!(
            "{CHECKSUM_MISMATCH}: {}/{} arrived with a SHA-256 other than the one it was sent with",
            request.bucket, request.key
        )
    })
}

/// Whether a delete of `bucket/key` in a replicated pool leaves a tombstone.
///
/// It does for the keys the pool keeps in step by their versions — the same
/// keys the storage keeps version digests of ([`s4_core::types::is_replicated_key`]).
/// The rest of `__system__` is deleted as it always was.
pub fn delete_leaves_tombstone(bucket: &str, key: &str) -> bool {
    s4_core::types::is_replicated_key(bucket, key)
}

/// Leaves the tombstone of a delete issued at `hlc` on `storage`. When the
/// key holds a newer version — an object or a tombstone — none is left, and
/// the HLC of that version is returned: a coordinator raises a client's
/// delete above it.
pub(crate) async fn leave_tombstone(
    storage: &dyn s4_core::StorageEngine,
    bucket: &str,
    key: &str,
    hlc: crate::hlc::HlcTimestamp,
) -> Result<Option<crate::hlc::HlcTimestamp>, s4_core::StorageError> {
    if storage
        .tombstone_object(bucket, key, hlc.wall_time, hlc.logical, hlc.node_id)
        .await?
    {
        return Ok(None);
    }
    Ok(storage
        .key_version(bucket, key)
        .await?
        .record()
        .filter(|record| record.hlc_order().is_some())
        .map(crate::hlc::HlcTimestamp::of_record))
}
