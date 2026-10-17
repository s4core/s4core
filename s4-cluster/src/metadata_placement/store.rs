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

//! The record of a node's place in the move, in `cluster_meta` (plan
//! BUCKET-METADATA-REPLICATION.md, decision 4): it survives restarts and
//! never goes backwards.

use s4_core::storage::IndexDb;

use crate::error::ClusterError;
use crate::metadata_placement::stage::PlacementRecord;

/// The record this node stored, `None` when it never stored one.
///
/// # Errors
///
/// The index cannot be read, or holds a record this release cannot read.
pub async fn load(index: &IndexDb) -> Result<Option<PlacementRecord>, ClusterError> {
    index
        .bucket_metadata_placement()
        .await
        .map_err(|e| ClusterError::Storage(e.to_string()))?
        .map(|bytes| PlacementRecord::decode(&bytes))
        .transpose()
}

/// Stores `record`, durably, unless it would take the stored one back
/// ([`PlacementRecord::follows`]).
///
/// # Errors
///
/// The record does not follow the stored one, or the index cannot be read
/// or written.
pub async fn store(index: &IndexDb, record: &PlacementRecord) -> Result<(), ClusterError> {
    if let Some(stored) = load(index).await? {
        if stored == *record {
            return Ok(());
        }
        if !record.follows(&stored) {
            return Err(ClusterError::Config(format!(
                "refusing to store bucket metadata placement {record:?} over {stored:?}: a \
                 node's stage only moves forward"
            )));
        }
    }
    index
        .store_bucket_metadata_placement(record.encode())
        .await
        .map_err(|e| ClusterError::Storage(e.to_string()))
}

#[cfg(test)]
mod tests {
    use s4_core::storage::IndexOptions;

    use super::*;
    use crate::metadata_placement::target::QuorumShape;

    #[tokio::test]
    async fn a_stored_record_moves_only_forward_and_survives_a_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("index");
        let options = IndexOptions {
            version_digests: true,
        };
        let legacy = PlacementRecord::starting(QuorumShape::majority(3), QuorumShape::majority(5));
        let dual = legacy.advanced().unwrap();
        {
            let index = IndexDb::open(&path, options).unwrap();
            assert_eq!(load(&index).await.unwrap(), None);
            store(&index, &legacy).await.unwrap();
            store(&index, &dual).await.unwrap();
            store(&index, &dual).await.unwrap();
            let back = store(&index, &legacy).await.unwrap_err().to_string();
            assert!(back.contains("only moves forward"), "{back}");
        }
        let index = IndexDb::open(&path, options).unwrap();
        assert_eq!(load(&index).await.unwrap(), Some(dual));
    }
}
