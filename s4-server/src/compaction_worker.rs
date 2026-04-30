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
//!
//! Supports two modes per cycle:
//! - **Regular**: processes up to 10 volumes (lightweight, runs every N hours)
//! - **Full**: processes all volumes (runs once daily at a configured time, default 02:00)

use crate::config::CompactionConfig;
use chrono::{Local, Timelike};
use s4_compactor::VolumeCompactor;
use s4_core::storage::BitcaskStorageEngine;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{interval, Duration};
use tracing::{error, info};

/// Parses an "HH:MM" string into (hour, minute). Returns `None` on invalid input.
fn parse_time(s: &str) -> Option<(u32, u32)> {
    let parts: Vec<&str> = s.trim().split(':').collect();
    if parts.len() != 2 {
        return None;
    }
    let hour: u32 = parts[0].parse().ok()?;
    let minute: u32 = parts[1].parse().ok()?;
    if hour >= 24 || minute >= 60 {
        return None;
    }
    Some((hour, minute))
}

/// Background compaction worker.
///
/// Spawns a periodic task that analyzes volume fragmentation and
/// compacts volumes above the configured threshold.
pub struct CompactionWorker {
    storage: Arc<RwLock<BitcaskStorageEngine>>,
    config: CompactionConfig,
    /// When true, disables journal compaction (managed by the replication layer).
    cluster_mode: bool,
}

impl CompactionWorker {
    /// Creates a new compaction worker.
    pub fn new(storage: Arc<RwLock<BitcaskStorageEngine>>, config: CompactionConfig) -> Self {
        Self {
            storage,
            config,
            cluster_mode: false,
        }
    }

    /// Creates a new compaction worker for cluster mode.
    ///
    /// In cluster mode, journal compaction is disabled because the replication
    /// layer manages journal compaction with replica-aware sequence tracking.
    pub fn new_cluster(
        storage: Arc<RwLock<BitcaskStorageEngine>>,
        config: CompactionConfig,
    ) -> Self {
        Self {
            storage,
            config,
            cluster_mode: true,
        }
    }

    /// Spawns the compaction worker as a background task.
    ///
    /// Returns a join handle that can be used to wait for or abort the worker.
    pub fn spawn(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            self.run_loop().await;
        })
    }

    /// Returns true if it is time for the daily full compaction run.
    ///
    /// A full run is triggered when the current local time is past the
    /// configured `full_compaction_time` and no full run has been done
    /// on the current date yet.
    fn should_run_full(&self, last_full_date: Option<chrono::NaiveDate>) -> bool {
        let full_time = match parse_time(&self.config.full_compaction_time) {
            Some(t) => t,
            None => return false, // disabled or invalid
        };

        let now = Local::now();
        let today = now.date_naive();
        let (hour, minute) = (now.hour(), now.minute());

        // Already ran today?
        if last_full_date == Some(today) {
            return false;
        }

        // Past the configured time?
        (hour, minute) >= full_time
    }

    /// Main worker loop.
    async fn run_loop(&self) {
        let interval_duration = Duration::from_secs(self.config.interval_hours * 3600);
        let mut timer = interval(interval_duration);

        let full_time_display = if parse_time(&self.config.full_compaction_time).is_some() {
            format!(
                "daily full compaction at {}",
                self.config.full_compaction_time
            )
        } else {
            "daily full compaction disabled".to_string()
        };

        info!(
            "Compaction worker started (interval: {} hours, threshold: {:.0}%, dry_run: {}, journal_compaction: {}, {})",
            self.config.interval_hours,
            self.config.fragmentation_threshold * 100.0,
            self.config.dry_run,
            if self.cluster_mode { "disabled (cluster)" } else { "enabled" },
            full_time_display,
        );

        // Skip first tick (fires immediately)
        timer.tick().await;

        let mut last_full_date: Option<chrono::NaiveDate> = None;

        loop {
            timer.tick().await;

            let is_full_run = self.should_run_full(last_full_date);

            if is_full_run {
                info!("Starting FULL volume compaction (daily scheduled)");
            } else {
                info!("Starting volume compaction cycle");
            }

            let start = std::time::Instant::now();

            // Read-lock the engine to access internal fields
            let storage = self.storage.read().await;

            let compaction_config = if is_full_run {
                // Full compaction: unlimited volumes, no minimum dead bytes
                s4_compactor::CompactionConfig {
                    fragmentation_threshold: self.config.fragmentation_threshold,
                    min_dead_bytes: 0,
                    max_volumes_per_run: 0, // unlimited
                    dry_run: self.config.dry_run,
                    multipart_session_ttl_secs:
                        s4_compactor::CompactionConfig::multipart_ttl_from_env(),
                    compact_journal: !self.cluster_mode,
                    ..Default::default()
                }
            } else {
                // Regular compaction: default limits (10 volumes)
                s4_compactor::CompactionConfig {
                    fragmentation_threshold: self.config.fragmentation_threshold,
                    dry_run: self.config.dry_run,
                    multipart_session_ttl_secs:
                        s4_compactor::CompactionConfig::multipart_ttl_from_env(),
                    compact_journal: !self.cluster_mode,
                    ..Default::default()
                }
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
                    let mode = if is_full_run {
                        "Full compaction"
                    } else {
                        "Compaction"
                    };
                    info!(
                        "{} completed in {:?}: scanned={}, compacted={}, \
                         bytes_reclaimed={}, errors={}",
                        mode,
                        start.elapsed(),
                        stats.volumes_scanned,
                        stats.volumes_compacted,
                        stats.total_bytes_reclaimed,
                        stats.errors,
                    );

                    if is_full_run {
                        last_full_date = Some(Local::now().date_naive());
                    }
                }
                Err(e) => {
                    error!("Compaction failed: {:?}", e);
                }
            }
        }
    }
}
