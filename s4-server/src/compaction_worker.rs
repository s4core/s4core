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

//! Background volume compaction worker.
//!
//! Runs periodically to scan volumes for dead space and reclaim it
//! by copying live blobs to new volumes and deleting old ones.

use crate::config::CompactionConfig;
use s4_compactor::VolumeCompactor;
use s4_core::storage::BitcaskStorageEngine;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{interval, Duration};
use tracing::{error, info};

/// Background compaction worker.
///
/// Spawns a periodic task that analyzes volume fragmentation and
/// compacts volumes above the configured threshold.
pub struct CompactionWorker {
    storage: Arc<RwLock<BitcaskStorageEngine>>,
    config: CompactionConfig,
}

impl CompactionWorker {
    /// Creates a new compaction worker.
    pub fn new(storage: Arc<RwLock<BitcaskStorageEngine>>, config: CompactionConfig) -> Self {
        Self { storage, config }
    }

    /// Spawns the compaction worker as a background task.
    ///
    /// Returns a join handle that can be used to wait for or abort the worker.
    pub fn spawn(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            self.run_loop().await;
        })
    }

    /// Main worker loop.
    async fn run_loop(&self) {
        let interval_duration = Duration::from_secs(self.config.interval_hours * 3600);
        let mut timer = interval(interval_duration);

        info!(
            "Compaction worker started (interval: {} hours, threshold: {:.0}%, dry_run: {})",
            self.config.interval_hours,
            self.config.fragmentation_threshold * 100.0,
            self.config.dry_run,
        );

        // Skip first tick (fires immediately)
        timer.tick().await;

        loop {
            timer.tick().await;
            info!("Starting volume compaction cycle");

            let start = std::time::Instant::now();

            // Read-lock the engine to access internal fields
            let storage = self.storage.read().await;

            let compaction_config = s4_compactor::CompactionConfig {
                fragmentation_threshold: self.config.fragmentation_threshold,
                dry_run: self.config.dry_run,
                ..Default::default()
            };

            let compactor = VolumeCompactor::new(
                storage.volumes_dir().to_path_buf(),
                storage.index_db().clone(),
                storage.deduplicator().clone(),
                storage.volume_writer().clone(),
                compaction_config,
            );

            match compactor.run().await {
                Ok(stats) => {
                    info!(
                        "Compaction completed in {:?}: scanned={}, compacted={}, \
                         bytes_reclaimed={}, errors={}",
                        start.elapsed(),
                        stats.volumes_scanned,
                        stats.volumes_compacted,
                        stats.total_bytes_reclaimed,
                        stats.errors,
                    );
                }
                Err(e) => {
                    error!("Compaction failed: {:?}", e);
                }
            }
        }
    }
}
