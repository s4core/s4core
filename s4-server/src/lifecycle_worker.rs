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
//!
//! On a cluster node each node expires the objects of its own storage. The
//! expiry is a delete of the pool's key like any other, so it leaves a
//! tombstone with the HLC of this node's clock: a copy another node still
//! holds is older, and comes back neither on a read nor through a repair.
//!
//! Which buckets there are and what their rules say is a fact of the pool,
//! not of the node: a node holds the objects of a bucket whose marker it
//! does not hold, and a rule written through any node. So a cluster node
//! takes the buckets from the distributed listing of their markers, as
//! ListBuckets does, and each bucket's rules through the read quorums, as
//! GetBucketLifecycle does — a rule left from an earlier bucket of the same
//! name does not count. Until the pool adopted the configurations an earlier
//! release stored on single nodes, the rules this node holds itself are
//! weighed too, by the same rule of generations, and a newer value or delete
//! a replica holds wins over them. Cluster mode keeps no versions, so a
//! cluster node evaluates every bucket as unversioned, whatever versioning
//! setting a node stored before cluster mode refused it.

use chrono::Utc;
use s4_cluster::{
    BucketConfigRead, ConfigReadGate, DistributedListCoordinator, HlcTimestamp, HybridClock,
    OwnConfig, QuorumReadCoordinator,
};
use s4_core::storage::BitcaskStorageEngine;
use s4_core::{IndexRecord, KeyVersion, StorageEngine, VersioningStatus};
use s4_features::lifecycle::{
    evaluate_rule, is_expired_delete_marker, parse_lifecycle_xml, should_expire_noncurrent_version,
    EvaluationResult, LifecycleConfiguration, LifecycleRule, RuleStatus,
};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::interval;
use tracing::{debug, error, info, warn};

use crate::config::LifecycleConfig;

use s4_core::types::{
    BUCKET_MARKER_KEY_PREFIX as BUCKET_MARKER_PREFIX,
    LIFECYCLE_CONFIG_KEY_PREFIX as LIFECYCLE_CONFIG_PREFIX, SYSTEM_BUCKET,
    VERSIONING_CONFIG_KEY_PREFIX as VERSIONING_CONFIG_PREFIX,
};
/// How many bucket markers are listed at a time; a few in tests, which
/// follow the pages without writing thousands of markers.
#[cfg(not(test))]
const BUCKET_PAGE: usize = 1000;
#[cfg(test)]
const BUCKET_PAGE: usize = 10;
/// The most days `S4_LIFECYCLE_DAYS_AHEAD` moves the evaluation ahead: a
/// century, far past any rule a test needs and far from the end of dates.
pub const MAX_DAYS_AHEAD: u32 = 36_500;

/// Where a cluster node learns its pool's buckets and their rules.
struct ClusterBuckets {
    list: Arc<DistributedListCoordinator>,
    read: Arc<QuorumReadCoordinator>,
    gate: ConfigReadGate,
}

/// Lifecycle policy worker.
///
/// Runs as a background task and periodically evaluates lifecycle rules
/// to delete expired objects and versions.
pub struct LifecycleWorker {
    /// Storage engine.
    storage: Arc<RwLock<BitcaskStorageEngine>>,
    /// Worker configuration.
    config: LifecycleConfig,
    /// The node's clock on a cluster node, whose expiries leave tombstones.
    cluster_clock: Option<Arc<HybridClock>>,
    /// The pool's buckets and their rules on a cluster node.
    cluster_buckets: Option<ClusterBuckets>,
}

impl LifecycleWorker {
    /// Creates a new lifecycle worker.
    pub fn new(storage: Arc<RwLock<BitcaskStorageEngine>>, config: LifecycleConfig) -> Self {
        Self {
            storage,
            config,
            cluster_clock: None,
            cluster_buckets: None,
        }
    }

    /// The same worker on a cluster node: an expired object leaves a
    /// tombstone stamped by `clock`, the clock of the node's coordinators.
    pub fn with_cluster_clock(mut self, clock: Arc<HybridClock>) -> Self {
        self.cluster_clock = Some(clock);
        self
    }

    /// The same worker on a cluster node: buckets come from the distributed
    /// listing of their markers through `list`, and their rules through
    /// `read` — until `gate` opens, with this node's own rules weighed too.
    /// Every bucket is evaluated as unversioned.
    pub fn with_cluster_buckets(
        mut self,
        list: Arc<DistributedListCoordinator>,
        read: Arc<QuorumReadCoordinator>,
        gate: ConfigReadGate,
    ) -> Self {
        self.cluster_buckets = Some(ClusterBuckets { list, read, gate });
        self
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
        let interval_duration = self.config.interval();
        let mut timer = interval(interval_duration);

        info!(
            "Lifecycle worker started (interval: {} s, dry_run: {})",
            interval_duration.as_secs(),
            self.config.dry_run
        );
        if self.config.days_ahead > 0 {
            warn!(
                days_ahead = self.days_ahead(),
                "Lifecycle rules are evaluated as if more days had passed \
                 (S4_LIFECYCLE_DAYS_AHEAD, for testing only): objects expire early"
            );
        }
        if self.config.days_ahead > MAX_DAYS_AHEAD {
            warn!(
                days_ahead = self.config.days_ahead,
                max = MAX_DAYS_AHEAD,
                "S4_LIFECYCLE_DAYS_AHEAD is past its maximum and taken as the maximum"
            );
        }

        // Skip the first tick (fires immediately)
        timer.tick().await;

        loop {
            timer.tick().await;
            info!("Starting lifecycle policy evaluation");

            let start = std::time::Instant::now();
            match self.evaluate_once().await {
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

    /// The days the evaluation is moved ahead by, at most
    /// [`MAX_DAYS_AHEAD`].
    fn days_ahead(&self) -> u32 {
        self.config.days_ahead.min(MAX_DAYS_AHEAD)
    }

    /// The time rules are evaluated at: now, moved ahead by the days the
    /// configuration says (for testing only).
    fn now(&self) -> chrono::DateTime<Utc> {
        let now = Utc::now();
        now.checked_add_days(chrono::Days::new(u64::from(self.days_ahead())))
            .unwrap_or(now)
    }

    /// Evaluates the lifecycle rules of every bucket once, expiring what they
    /// say has expired.
    ///
    /// # Errors
    ///
    /// When the buckets cannot be listed, or this node's storage fails.
    pub async fn evaluate_once(&self) -> Result<LifecycleStats, anyhow::Error> {
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

    /// Lists all buckets in the system, a page of markers at a time: in a
    /// cluster from the distributed listing of the pool, on a single node
    /// from its storage.
    async fn list_buckets(&self) -> Result<Vec<String>, anyhow::Error> {
        let mut buckets = Vec::new();
        if let Some(cluster) = &self.cluster_buckets {
            let mut token: Option<String> = None;
            loop {
                let page = cluster
                    .list
                    .list(
                        SYSTEM_BUCKET,
                        BUCKET_MARKER_PREFIX,
                        BUCKET_PAGE,
                        token.as_deref(),
                    )
                    .await?;
                buckets.extend(
                    page.entries
                        .iter()
                        .filter_map(|entry| entry.key.strip_prefix(BUCKET_MARKER_PREFIX))
                        .map(str::to_string),
                );
                match page.next_continuation_token {
                    Some(next) if page.is_truncated => token = Some(next),
                    _ => break,
                }
            }
            buckets.sort();
            buckets.dedup();
            return Ok(buckets);
        }

        let storage = self.storage.read().await;
        let mut after = String::new();
        loop {
            let page = storage
                .list_objects_after(SYSTEM_BUCKET, BUCKET_MARKER_PREFIX, &after, BUCKET_PAGE)
                .await?;
            let full = page.len() == BUCKET_PAGE;
            if let Some((last, _)) = page.last() {
                after = last.clone();
            }
            buckets.extend(
                page.into_iter().filter_map(|(key, _)| {
                    key.strip_prefix(BUCKET_MARKER_PREFIX).map(str::to_string)
                }),
            );
            if !full {
                break;
            }
        }
        Ok(buckets)
    }

    /// Gets the lifecycle configuration for a bucket.
    async fn get_bucket_lifecycle(
        &self,
        bucket: &str,
    ) -> Result<Option<LifecycleConfiguration>, anyhow::Error> {
        let lifecycle_key = format!("{}{}", LIFECYCLE_CONFIG_PREFIX, bucket);

        let stored = match &self.cluster_buckets {
            Some(cluster) => {
                // Before the gate this node's own rules are weighed too, as
                // GetBucketLifecycle weighs them.
                let own = if cluster.gate.is_open() {
                    None
                } else {
                    let storage = self.storage.read().await;
                    storage.get_object(SYSTEM_BUCKET, &lifecycle_key).await.ok().map(
                        |(data, record)| OwnConfig {
                            data,
                            hlc: HlcTimestamp::of_record(&record),
                            etag: record.etag,
                        },
                    )
                };
                match cluster.read.read_bucket_config_with_own(bucket, &lifecycle_key, own).await {
                    Ok(BucketConfigRead::Set(data)) => Some(data),
                    Ok(BucketConfigRead::NotSet | BucketConfigRead::NoSuchBucket) => None,
                    Err(e) => {
                        // The next evaluation tries again; expiring by a rule
                        // that may have changed since is worse than waiting.
                        warn!(
                            bucket,
                            error = %e,
                            "lifecycle rules could not be read through the quorums, bucket skipped"
                        );
                        None
                    }
                }
            }
            None => {
                let storage = self.storage.read().await;
                storage
                    .get_object(SYSTEM_BUCKET, &lifecycle_key)
                    .await
                    .ok()
                    .map(|(data, _)| data)
            }
        };

        let Some(data) = stored else {
            return Ok(None);
        };
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

    /// Gets the versioning status for a bucket.
    async fn get_versioning_status(&self, bucket: &str) -> Result<VersioningStatus, anyhow::Error> {
        if self.cluster_buckets.is_some() {
            return Ok(VersioningStatus::Unversioned);
        }
        let storage = self.storage.read().await;
        let versioning_key = format!("{}{}", VERSIONING_CONFIG_PREFIX, bucket);

        match storage.get_object(SYSTEM_BUCKET, &versioning_key).await {
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

        let now = self.now();

        for (key, record) in objects {
            // Skip internal keys
            if key.starts_with("__s4_") {
                continue;
            }

            // Erasure-coded objects are listed here — their namespace entry
            // survives the move to EC — but expiring one means deleting shards
            // across a pool, not a local blob. Doing that from this worker would
            // drop the entry on one node and leave the shards behind, so it is
            // left to the EC runtime's own delete path.
            if record.is_erasure_coded() {
                continue;
            }

            // Evaluate all rules against this object
            for rule in rules {
                let result =
                    evaluate_rule(rule, &key, record.created_at, record.is_delete_marker, now);

                if result.has_action() {
                    let action_stats = self
                        .apply_actions(bucket, &key, &record, &result, rule, versioning_status)
                        .await?;
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
        evaluated: &IndexRecord,
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
                if let Some(clock) = &self.cluster_clock {
                    if expire_with_tombstone(&storage, bucket, key, evaluated, clock.now()).await? {
                        stats.objects_deleted += 1;
                    }
                } else {
                    storage.delete_object_versioned(bucket, key, None, versioning_status).await?;
                    stats.objects_deleted += 1;
                }
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
                    if let Ok((data, _)) =
                        storage.get_object(SYSTEM_BUCKET, &version_list_key).await
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
                                        storage.head_object(SYSTEM_BUCKET, &version_key).await
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
                                                storage.head_object(SYSTEM_BUCKET, &newer_key).await
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
                                            self.now(),
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
            if let Ok((data, _)) = storage.get_object(SYSTEM_BUCKET, &version_list_key).await {
                if let Ok(version_list_json) = String::from_utf8(data) {
                    if let Ok(version_list) =
                        serde_json::from_str::<s4_core::storage::VersionList>(&version_list_json)
                    {
                        // Check if latest version is a delete marker and it's the only version
                        if !version_list.versions.is_empty() {
                            let latest_vid = &version_list.versions[0];
                            let latest_key = format!("{}/{}#{}", bucket, key, latest_vid);

                            if let Ok(latest_record) =
                                storage.head_object(SYSTEM_BUCKET, &latest_key).await
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

/// Expires the object at `bucket/key` on a cluster node by leaving its
/// tombstone at `now`, if the key still holds the generation of `evaluated`.
///
/// Called under the storage write lock, which the node's replica writes take
/// too, so the check and the tombstone see the same record. An object written
/// since the rules were evaluated is newer than the expiry decision and stays:
/// a tombstone at `now` would outrank it and delete it unseen.
async fn expire_with_tombstone(
    storage: &BitcaskStorageEngine,
    bucket: &str,
    key: &str,
    evaluated: &IndexRecord,
    now: HlcTimestamp,
) -> Result<bool, anyhow::Error> {
    let generation = HlcTimestamp::of_record(evaluated);
    match storage.key_version(bucket, key).await? {
        KeyVersion::Live(record) if HlcTimestamp::of_record(&record) == generation => Ok(storage
            .tombstone_object(bucket, key, now.wall_time, now.logical, now.node_id)
            .await?),
        _ => Ok(false),
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

    // ---- Expiry on a cluster node ----

    use s4_cluster::NodeId;
    use s4_features::lifecycle::{Expiration, LifecycleFilter};
    use std::collections::HashMap;

    fn expire_everything() -> LifecycleRule {
        LifecycleRule {
            id: "expire".to_string(),
            status: RuleStatus::Enabled,
            filter: LifecycleFilter::default(),
            expiration: Some(Expiration { days: Some(0) }),
            noncurrent_version_expiration: None,
            expired_object_delete_marker: None,
        }
    }

    async fn cluster_worker() -> (LifecycleWorker, Arc<HybridClock>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let storage = BitcaskStorageEngine::new(
            dir.path().join("volumes"),
            dir.path().join("metadata"),
            10 * 1024 * 1024,
            4096,
            false,
        )
        .await
        .unwrap();
        let clock = Arc::new(HybridClock::new(NodeId(5)));
        let worker = LifecycleWorker::new(
            Arc::new(RwLock::new(storage)),
            LifecycleConfig {
                enabled: true,
                interval_hours: 24,
                dry_run: false,
                interval_secs: None,
                days_ahead: 0,
            },
        )
        .with_cluster_clock(clock.clone());
        (worker, clock, dir)
    }

    async fn put_at(worker: &LifecycleWorker, key: &str, wall: u64) {
        let metadata = HashMap::from([
            ("_s4_hlc_wall".to_string(), wall.to_string()),
            ("_s4_hlc_logical".to_string(), "0".to_string()),
            ("_s4_hlc_node_id".to_string(), "7".to_string()),
        ]);
        worker
            .storage
            .read()
            .await
            .put_object("bucket", key, b"bytes", "text/plain", &metadata)
            .await
            .unwrap();
    }

    /// On a cluster node an expired object leaves a tombstone stamped by the
    /// node's clock, newer than the copy any other node still holds.
    #[tokio::test]
    async fn an_object_expired_on_a_cluster_node_leaves_a_tombstone_of_its_clock() {
        let (worker, clock, _dir) = cluster_worker().await;
        put_at(&worker, "expired", 10).await;
        let before = clock.now();

        let stats = worker.evaluate_bucket("bucket", &[&expire_everything()]).await.unwrap();

        assert_eq!(stats.objects_deleted, 1);
        let storage = worker.storage.read().await;
        match storage.key_version("bucket", "expired").await.unwrap() {
            KeyVersion::Tombstone(record) => {
                let deleted = HlcTimestamp::of_record(&record);
                assert!(deleted > before, "{deleted:?} is not newer than {before:?}");
                assert_eq!(deleted.node_id, NodeId(5).0);
            }
            other => panic!("{other:?}"),
        }
    }

    /// An object written just now is not a day old; with the evaluation moved
    /// a day ahead it is, and a rule of one day expires it.
    #[tokio::test]
    async fn rules_evaluated_days_ahead_expire_a_new_object() {
        let (worker, _clock, _dir) = cluster_worker().await;
        put_at(&worker, "new", 10).await;
        let mut one_day = expire_everything();
        one_day.expiration = Some(Expiration { days: Some(1) });

        let stats = worker.evaluate_bucket("bucket", &[&one_day]).await.unwrap();
        assert_eq!(stats.objects_deleted, 0);

        let worker = LifecycleWorker {
            config: LifecycleConfig {
                days_ahead: 1,
                ..worker.config.clone()
            },
            ..worker
        };
        let stats = worker.evaluate_bucket("bucket", &[&one_day]).await.unwrap();
        assert_eq!(stats.objects_deleted, 1);
    }

    /// The interval in seconds wins over the one in hours, and is never zero.
    #[test]
    fn the_interval_in_seconds_wins_and_is_never_zero() {
        let config = |interval_hours, interval_secs| LifecycleConfig {
            enabled: true,
            interval_hours,
            dry_run: false,
            interval_secs,
            days_ahead: 0,
        };
        assert_eq!(config(24, None).interval().as_secs(), 86_400);
        assert_eq!(config(24, Some(5)).interval().as_secs(), 5);
        assert_eq!(config(0, None).interval().as_secs(), 1);
        assert_eq!(config(24, Some(0)).interval().as_secs(), 1);
    }

    /// A day count past the end of dates neither panics nor moves the
    /// evaluation past [`MAX_DAYS_AHEAD`].
    #[tokio::test]
    async fn days_ahead_are_capped() {
        let (worker, _clock, _dir) = cluster_worker().await;
        let worker = LifecycleWorker {
            config: LifecycleConfig {
                days_ahead: u32::MAX,
                ..worker.config.clone()
            },
            ..worker
        };
        let ahead = worker.now() - Utc::now();
        assert!(
            (i64::from(MAX_DAYS_AHEAD) - 1..=i64::from(MAX_DAYS_AHEAD)).contains(&ahead.num_days()),
            "{ahead:?}"
        );
    }

    /// Every bucket is listed, however many pages of markers it takes.
    #[tokio::test]
    async fn buckets_are_listed_past_one_page() {
        let (worker, _clock, _dir) = cluster_worker().await;
        let count = BUCKET_PAGE * 2 + 1;
        {
            let storage = worker.storage.read().await;
            for n in 0..count {
                storage
                    .put_object(
                        SYSTEM_BUCKET,
                        &format!("{BUCKET_MARKER_PREFIX}bucket-{n:05}"),
                        b"1",
                        "application/octet-stream",
                        &HashMap::new(),
                    )
                    .await
                    .unwrap();
            }
        }
        let buckets = worker.list_buckets().await.unwrap();
        assert_eq!(buckets.len(), count);
        assert_eq!(buckets.last().map(String::as_str), Some("bucket-00020"));
    }

    /// An object written after the rules were evaluated is newer than the
    /// expiry decision, and the expiry leaves it alone.
    #[tokio::test]
    async fn an_object_rewritten_after_the_evaluation_is_not_expired() {
        let (worker, clock, _dir) = cluster_worker().await;
        put_at(&worker, "rewritten", 10).await;
        let evaluated =
            worker.storage.read().await.head_object("bucket", "rewritten").await.unwrap();
        put_at(&worker, "rewritten", 20).await;

        let storage = worker.storage.write().await;
        let expired =
            expire_with_tombstone(&storage, "bucket", "rewritten", &evaluated, clock.now())
                .await
                .unwrap();

        assert!(!expired);
        assert!(matches!(
            storage.key_version("bucket", "rewritten").await.unwrap(),
            KeyVersion::Live(record) if record.hlc_timestamp == 20
        ));
    }
}
