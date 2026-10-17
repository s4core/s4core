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

//! Steps that prepare a stopped node's data directory for an older release.

use anyhow::{Context, Result};
use s4_core::storage::BitcaskStorageEngine;

use crate::config::Config;

/// Removes every tombstone from the data directory `config` names and returns
/// how many there were (`s4-server --purge-tombstones`).
///
/// A release from before tombstones reads one as a delete marker and answers
/// a read of the deleted key with an error instead of "not found", so this
/// runs on every node, each stopped, before such a release opens its data
/// directory. See [`BitcaskStorageEngine::purge_tombstones`].
///
/// The directory is opened without version digests, which drops them and the
/// tombstone horizon: the older release deletes without tombstones and does
/// not keep digests, so a return to this release rebuilds the digests and
/// decides the horizon anew.
pub async fn purge_tombstones(config: &Config) -> Result<u64> {
    let storage = BitcaskStorageEngine::new(
        &config.storage.data_path,
        &config.storage.metadata_path,
        config.tuning.volume_size_mb * 1024 * 1024,
        config.tuning.inline_threshold,
        config.tuning.strict_sync,
    )
    .await
    .with_context(|| {
        format!(
            "open the data directory {} (is the node stopped?)",
            config.storage.metadata_path.display()
        )
    })?;
    storage.purge_tombstones().await.context("remove the tombstones")
}

#[cfg(test)]
mod tests {
    use super::*;
    use s4_core::storage::{IndexOptions, TombstoneHorizon, VersionDigestsOpening};
    use s4_core::{KeyVersion, StorageEngine};

    #[tokio::test]
    async fn purging_a_data_directory_removes_its_tombstones() {
        let temp = tempfile::TempDir::new().unwrap();
        let mut config = Config::default();
        config.storage.data_path = temp.path().join("volumes");
        config.storage.metadata_path = temp.path().join("metadata_db");
        let cluster_node = || {
            BitcaskStorageEngine::open(
                &config.storage.data_path,
                &config.storage.metadata_path,
                1024 * 1024,
                4096,
                false,
                IndexOptions {
                    version_digests: true,
                },
            )
        };
        {
            let storage = cluster_node().await.unwrap();
            assert!(storage.tombstone_object("bucket", "gone", 20, 0, 1).await.unwrap());
            storage
                .index_db()
                .settle_tombstone_horizon(TombstoneHorizon {
                    hlc_timestamp: 30,
                    hlc_logical: 0,
                    origin_node_id: 1,
                })
                .await
                .unwrap();
        }

        assert_eq!(purge_tombstones(&config).await.unwrap(), 1);

        let storage = cluster_node().await.unwrap();
        assert!(matches!(
            storage.key_version("bucket", "gone").await.unwrap(),
            KeyVersion::Absent
        ));
        assert_eq!(
            storage.index_db().tombstone_horizon().await.unwrap(),
            None,
            "the older release deletes without tombstones: the horizon is decided anew"
        );
        assert!(matches!(
            storage.index_db().version_digests_opening(),
            Some(VersionDigestsOpening::Rebuilt { .. })
        ));
    }
}
