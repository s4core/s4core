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

//! Background worker that cleans up expired multipart upload temp files.
//!
//! Runs periodically to remove temp files from abandoned uploads
//! that exceed the configured TTL.

use crate::config::MultipartCleanupConfig;
use s4_api::multipart_store::DiskPartStore;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::interval;
use tracing::{error, info};

/// Background multipart cleanup worker.
///
/// Periodically scans for expired multipart uploads and removes
/// their temporary files from disk.
pub struct MultipartCleanupWorker {
    part_store: Arc<DiskPartStore>,
    config: MultipartCleanupConfig,
}

impl MultipartCleanupWorker {
    /// Creates a new cleanup worker.
    pub fn new(part_store: Arc<DiskPartStore>, config: MultipartCleanupConfig) -> Self {
        Self { part_store, config }
    }

    /// Spawns the cleanup worker as a background task.
    ///
    /// Returns a join handle that can be used to wait for or abort the worker.
    pub fn spawn(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            self.run_loop().await;
        })
    }

    /// Main worker loop.
    async fn run_loop(&self) {
        let ttl = Duration::from_secs(self.config.upload_ttl_hours * 3600);
        let check_interval = Duration::from_secs(self.config.cleanup_interval_hours * 3600);
        let mut timer = interval(check_interval);

        info!(
            "Multipart cleanup worker started (TTL: {} hours, interval: {} hours)",
            self.config.upload_ttl_hours, self.config.cleanup_interval_hours,
        );

        // Skip first tick (fires immediately)
        timer.tick().await;

        loop {
            timer.tick().await;

            match self.part_store.cleanup_expired(ttl).await {
                Ok(count) if count > 0 => {
                    info!("Cleaned up {} expired multipart uploads", count);
                }
                Err(e) => {
                    error!("Multipart cleanup error: {:?}", e);
                }
                _ => {}
            }
        }
    }
}
