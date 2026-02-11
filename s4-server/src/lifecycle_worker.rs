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

//! Lifecycle policy worker for automatic object expiration.
//!
//! This module implements a background worker that periodically evaluates
//! lifecycle rules and deletes expired objects and versions.

use chrono::Utc;
use s4_core::storage::BitcaskStorageEngine;
use s4_core::{StorageEngine, VersioningStatus};
use s4_features::lifecycle::{
    evaluate_rule, is_expired_delete_marker, parse_lifecycle_xml, should_expire_noncurrent_version,
    EvaluationResult, LifecycleConfiguration, LifecycleRule, RuleStatus,
};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{interval, Duration as TokioDuration};
use tracing::{debug, error, info, warn};

use crate::config::LifecycleConfig;

const BUCKET_MARKER_PREFIX: &str = "__s4_bucket_marker_";
const VERSIONING_CONFIG_PREFIX: &str = "__s4_bucket_versioning_";
const LIFECYCLE_CONFIG_PREFIX: &str = "__s4_bucket_lifecycle_";

/// Lifecycle policy worker.
///
/// Runs as a background task and periodically evaluates lifecycle rules
/// to delete expired objects and versions.
pub struct LifecycleWorker {
    /// Storage engine.
    storage: Arc<RwLock<BitcaskStorageEngine>>,
    /// Worker configuration.
    config: LifecycleConfig,
}

impl LifecycleWorker {
    /// Creates a new lifecycle worker.
    pub fn new(storage: Arc<RwLock<BitcaskStorageEngine>>, config: LifecycleConfig) -> Self {
        Self { storage, config }
    }

    /// Spawns the lifecycle worker as a background task.
    ///
    /// Returns a join handle that can be used to wait for or abort the worker.
    pub fn spawn(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            self.run_loop().await;
        })
    }

    /// Main worker loop.
    async fn run_loop(&self) {
        let interval_duration = TokioDuration::from_secs(self.config.interval_hours * 3600);
        let mut timer = interval(interval_duration);

        info!(
            "Lifecycle worker started (interval: {} hours, dry_run: {})",
            self.config.interval_hours, self.config.dry_run
        );

        // Skip the first tick (fires immediately)
        timer.tick().await;

        loop {
            timer.tick().await;
            info!("Starting lifecycle policy evaluation");

            let start = std::time::Instant::now();
            match self.evaluate_all_buckets().await {
                Ok(stats) => {
                    info!(
                        "Lifecycle evaluation completed in {:?}. \
                         Buckets: {}, Objects deleted: {}, Versions deleted: {}, \
                         Delete markers removed: {}",
                        start.elapsed(),
                        stats.buckets_processed,
                        stats.objects_deleted,
                        stats.versions_deleted,
                        stats.delete_markers_removed
                    );
                }
                Err(e) => {
                    error!("Lifecycle evaluation failed: {:?}", e);
                }
            }
        }
    }

    /// Evaluates lifecycle rules for all buckets.
    async fn evaluate_all_buckets(&self) -> Result<LifecycleStats, anyhow::Error> {
        let mut stats = LifecycleStats::default();

        // List all buckets
        let buckets = self.list_buckets().await?;
        debug!("Found {} buckets to evaluate", buckets.len());

        for bucket in buckets {
            // Get lifecycle config for this bucket
            if let Some(config) = self.get_bucket_lifecycle(&bucket).await? {
                let enabled_rules: Vec<_> =
                    config.rules.iter().filter(|r| r.status == RuleStatus::Enabled).collect();

                if !enabled_rules.is_empty() {
                    debug!(
                        "Evaluating bucket '{}' with {} enabled rules",
                        bucket,
                        enabled_rules.len()
                    );
                    let bucket_stats = self.evaluate_bucket(&bucket, &enabled_rules).await?;
                    stats.merge(bucket_stats);
                } else {
                    debug!("Bucket '{}' has no enabled lifecycle rules", bucket);
                }
            }
            stats.buckets_processed += 1;
        }

        Ok(stats)
    }

    /// Lists all buckets in the system.
    async fn list_buckets(&self) -> Result<Vec<String>, anyhow::Error> {
        let storage = self.storage.read().await;

        let markers = storage.list_objects("__system__", BUCKET_MARKER_PREFIX, 10000).await?;

        let buckets: Vec<String> = markers
            .into_iter()
            .filter_map(|(key, _)| key.strip_prefix(BUCKET_MARKER_PREFIX).map(|s| s.to_string()))
            .collect();

        Ok(buckets)
    }

    /// Gets the lifecycle configuration for a bucket.
    async fn get_bucket_lifecycle(
        &self,
        bucket: &str,
    ) -> Result<Option<LifecycleConfiguration>, anyhow::Error> {
        let storage = self.storage.read().await;
        let lifecycle_key = format!("{}{}", LIFECYCLE_CONFIG_PREFIX, bucket);

        match storage.get_object("__system__", &lifecycle_key).await {
            Ok((data, _)) => {
                let xml = String::from_utf8_lossy(&data);
                match parse_lifecycle_xml(&xml) {
                    Ok(config) => Ok(Some(config)),
                    Err(e) => {
                        warn!(
                            "Failed to parse lifecycle config for bucket '{}': {:?}",
                            bucket, e
                        );
                        Ok(None)
                    }
                }
            }
            Err(_) => Ok(None),
        }
    }

    /// Gets the versioning status for a bucket.
    async fn get_versioning_status(&self, bucket: &str) -> Result<VersioningStatus, anyhow::Error> {
        let storage = self.storage.read().await;
        let versioning_key = format!("{}{}", VERSIONING_CONFIG_PREFIX, bucket);

        match storage.get_object("__system__", &versioning_key).await {
            Ok((data, _)) => {
                let status_str = String::from_utf8_lossy(&data);
                Ok(match status_str.trim() {
                    "Enabled" => VersioningStatus::Enabled,
                    "Suspended" => VersioningStatus::Suspended,
                    _ => VersioningStatus::Unversioned,
                })
            }
            Err(_) => Ok(VersioningStatus::Unversioned),
        }
    }

    /// Evaluates lifecycle rules for a bucket.
    async fn evaluate_bucket(
        &self,
        bucket: &str,
        rules: &[&LifecycleRule],
    ) -> Result<LifecycleStats, anyhow::Error> {
        let mut stats = LifecycleStats::default();
        let storage = self.storage.read().await;

        // Get versioning status
        let versioning_status = self.get_versioning_status(bucket).await?;
        debug!(
            "Bucket '{}' versioning status: {:?}",
            bucket, versioning_status
        );

        // List all objects in bucket (limit to reasonable number)
        let objects = storage.list_objects(bucket, "", 100_000).await?;
        debug!("Found {} objects in bucket '{}'", objects.len(), bucket);

        // Release storage lock before processing
        drop(storage);

        let now = Utc::now();

        for (key, record) in objects {
            // Skip internal keys
            if key.starts_with("__s4_") {
                continue;
            }

            // Evaluate all rules against this object
            for rule in rules {
                let result =
                    evaluate_rule(rule, &key, record.created_at, record.is_delete_marker, now);

                if result.has_action() {
                    let action_stats =
                        self.apply_actions(bucket, &key, &result, rule, versioning_status).await?;
                    stats.merge(action_stats);
                }
            }
        }

        Ok(stats)
    }

    /// Applies lifecycle actions to an object.
    async fn apply_actions(
        &self,
        bucket: &str,
        key: &str,
        result: &EvaluationResult,
        rule: &LifecycleRule,
        versioning_status: VersioningStatus,
    ) -> Result<LifecycleStats, anyhow::Error> {
        let mut stats = LifecycleStats::default();

        // Action 1: Expire current version
        if result.expire_current {
            if self.config.dry_run {
                info!(
                    "[DRY-RUN] Would expire current version: {}/{} (rule: {})",
                    bucket, key, rule.id
                );
            } else {
                debug!(
                    "Expiring current version: {}/{} (rule: {})",
                    bucket, key, rule.id
                );
                let storage = self.storage.write().await;
                storage.delete_object_versioned(bucket, key, None, versioning_status).await?;
                stats.objects_deleted += 1;
            }
        }

        // Action 2: Check and expire noncurrent versions
        if result.check_noncurrent && versioning_status != VersioningStatus::Unversioned {
            if let Some(ref noncurrent_exp) = rule.noncurrent_version_expiration {
                // Collect versions to delete
                let mut versions_to_delete = Vec::new();

                {
                    let storage = self.storage.read().await;
                    // Get version list
                    let version_list_key = format!("__s4_versions_{}/{}", bucket, key);
                    if let Ok((data, _)) = storage.get_object("__system__", &version_list_key).await
                    {
                        if let Ok(version_list_json) = String::from_utf8(data) {
                            if let Ok(version_list) =
                                serde_json::from_str::<s4_core::storage::VersionList>(
                                    &version_list_json,
                                )
                            {
                                // Skip current version (index 0), process noncurrent versions
                                for i in 1..version_list.versions.len() {
                                    let version_id = &version_list.versions[i];
                                    let version_key = format!("{}/{}#{}", bucket, key, version_id);

                                    if let Ok(version_record) =
                                        storage.head_object("__system__", &version_key).await
                                    {
                                        // Skip delete markers
                                        if version_record.is_delete_marker {
                                            continue;
                                        }

                                        // Calculate when this version became noncurrent
                                        // (when the previous version was created)
                                        let became_noncurrent_at = if i > 0 {
                                            let newer_vid = &version_list.versions[i - 1];
                                            let newer_key =
                                                format!("{}/{}#{}", bucket, key, newer_vid);
                                            if let Ok(newer_record) =
                                                storage.head_object("__system__", &newer_key).await
                                            {
                                                newer_record.created_at
                                            } else {
                                                version_record.created_at
                                            }
                                        } else {
                                            version_record.created_at
                                        };

                                        if should_expire_noncurrent_version(
                                            became_noncurrent_at,
                                            noncurrent_exp,
                                            Utc::now(),
                                        ) {
                                            versions_to_delete.push(version_id.clone());
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                // Delete collected versions
                for version_id in versions_to_delete {
                    if self.config.dry_run {
                        info!(
                            "[DRY-RUN] Would expire noncurrent version: {}/{} ({})",
                            bucket, key, version_id
                        );
                    } else {
                        debug!(
                            "Expiring noncurrent version: {}/{} ({})",
                            bucket, key, version_id
                        );
                        let storage = self.storage.write().await;
                        storage
                            .delete_object_versioned(
                                bucket,
                                key,
                                Some(&version_id),
                                versioning_status,
                            )
                            .await?;
                        stats.versions_deleted += 1;
                    }
                }
            }
        }

        // Action 3: Cleanup expired delete markers
        if result.cleanup_delete_markers && versioning_status != VersioningStatus::Unversioned {
            let storage = self.storage.read().await;
            let version_list_key = format!("__s4_versions_{}/{}", bucket, key);
            if let Ok((data, _)) = storage.get_object("__system__", &version_list_key).await {
                if let Ok(version_list_json) = String::from_utf8(data) {
                    if let Ok(version_list) =
                        serde_json::from_str::<s4_core::storage::VersionList>(&version_list_json)
                    {
                        // Check if latest version is a delete marker and it's the only version
                        if !version_list.versions.is_empty() {
                            let latest_vid = &version_list.versions[0];
                            let latest_key = format!("{}/{}#{}", bucket, key, latest_vid);

                            if let Ok(latest_record) =
                                storage.head_object("__system__", &latest_key).await
                            {
                                if is_expired_delete_marker(
                                    latest_record.is_delete_marker,
                                    version_list.versions.len(),
                                ) {
                                    if self.config.dry_run {
                                        info!(
                                            "[DRY-RUN] Would remove expired delete marker: {}/{} ({})",
                                            bucket, key, latest_vid
                                        );
                                    } else {
                                        debug!(
                                            "Removing expired delete marker: {}/{} ({})",
                                            bucket, key, latest_vid
                                        );
                                        // Drop read lock, acquire write lock
                                        drop(storage);
                                        {
                                            let storage = self.storage.write().await;
                                            storage
                                                .delete_object_versioned(
                                                    bucket,
                                                    key,
                                                    Some(latest_vid),
                                                    versioning_status,
                                                )
                                                .await?;
                                            stats.delete_markers_removed += 1;
                                        }
                                        let _storage = self.storage.read().await;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(stats)
    }
}

/// Statistics from lifecycle evaluation.
#[derive(Debug, Default, Clone)]
pub struct LifecycleStats {
    /// Number of buckets processed.
    pub buckets_processed: u64,
    /// Number of current versions deleted.
    pub objects_deleted: u64,
    /// Number of noncurrent versions deleted.
    pub versions_deleted: u64,
    /// Number of expired delete markers removed.
    pub delete_markers_removed: u64,
}

impl LifecycleStats {
    /// Merges another stats object into this one.
    pub fn merge(&mut self, other: LifecycleStats) {
        self.buckets_processed += other.buckets_processed;
        self.objects_deleted += other.objects_deleted;
        self.versions_deleted += other.versions_deleted;
        self.delete_markers_removed += other.delete_markers_removed;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lifecycle_stats_merge() {
        let mut stats1 = LifecycleStats {
            buckets_processed: 1,
            objects_deleted: 10,
            versions_deleted: 5,
            delete_markers_removed: 2,
        };

        let stats2 = LifecycleStats {
            buckets_processed: 2,
            objects_deleted: 15,
            versions_deleted: 8,
            delete_markers_removed: 3,
        };

        stats1.merge(stats2);

        assert_eq!(stats1.buckets_processed, 3);
        assert_eq!(stats1.objects_deleted, 25);
        assert_eq!(stats1.versions_deleted, 13);
        assert_eq!(stats1.delete_markers_removed, 5);
    }
}
