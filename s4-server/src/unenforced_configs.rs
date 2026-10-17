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

//! Bucket configurations a cluster node stores and does not enforce.
//!
//! Cluster mode refuses versioning and Object Lock with `501`, but a bucket
//! that got either before the refusal still holds it on the node that
//! accepted it, and its owner may count on a protection that never held. A
//! cluster node warns about each one, and keeps `s4_bucket_unenforced_configs`
//! and the count the cluster health reports current for as long as it runs:
//! a configuration leaves the count when its bucket is deleted.
//!
//! The count follows the adoption of configurations stored before protocol 6,
//! which moves them from the nodes that are no replicas of their keys and
//! drops those of buckets gone since: the first count waits for the first pass
//! of adoption, and every pass counts again.

use std::sync::Arc;
use std::time::Duration;

use s4_api::handlers::{
    unenforced_bucket_configs, UnenforcedBucketConfig, UnenforcedConfigKind, UnenforcedConfigs,
};
use s4_core::StorageEngine;
use tokio::sync::watch;

/// How often the count is taken again, besides after each pass of adoption.
pub const REFRESH_INTERVAL: Duration = Duration::from_secs(600);
/// How long the first count waits for the first pass of adoption.
pub const FIRST_PASS_WAIT: Duration = Duration::from_secs(60);

/// Warns about every unenforced configuration this node stores once the first
/// pass of adoption ended (`passes` counts them), then keeps the gauge and
/// `snapshot` current until `shutdown` turns true.
pub async fn run(
    storage: Arc<dyn StorageEngine>,
    snapshot: UnenforcedConfigs,
    mut passes: watch::Receiver<u64>,
    mut shutdown: watch::Receiver<bool>,
) {
    tokio::select! {
        _ = tokio::time::timeout(FIRST_PASS_WAIT, passes.wait_for(|count| *count > 0)) => {}
        changed = shutdown.changed() => {
            if changed.is_err() || *shutdown.borrow() {
                return;
            }
        }
    }
    passes.mark_unchanged();
    let found = unenforced_bucket_configs(&*storage).await;
    warn_about(&found);
    publish(&found);
    snapshot.set(found);
    let mut adopting = true;
    loop {
        let (running, passes) = (adopting, &mut passes);
        let next_pass = async move {
            if running {
                passes.changed().await.is_ok()
            } else {
                std::future::pending().await
            }
        };
        tokio::select! {
            _ = tokio::time::sleep(REFRESH_INTERVAL) => {}
            passed = next_pass => {
                // Adoption ended for good once its sender is gone.
                adopting = passed;
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return;
                }
                continue;
            }
        }
        let found = unenforced_bucket_configs(&*storage).await;
        publish(&found);
        snapshot.set(found);
    }
}

/// One warning per configuration, naming the bucket and what it turned on.
fn warn_about(found: &[UnenforcedBucketConfig]) {
    for config in found {
        let what = match config.kind {
            UnenforcedConfigKind::Versioning => "versioning",
            UnenforcedConfigKind::ObjectLock => "Object Lock",
        };
        tracing::warn!(
            bucket = %config.bucket,
            kind = config.kind.as_str(),
            "bucket {} has {what} stored, which cluster mode does not enforce: objects in it are \
             written and deleted without versions or retention checks",
            config.bucket
        );
    }
}

/// Sets `s4_bucket_unenforced_configs` for every kind, zero included.
fn publish(found: &[UnenforcedBucketConfig]) {
    for kind in UnenforcedConfigKind::ALL {
        let count = found.iter().filter(|config| config.kind == kind).count();
        metrics::gauge!("s4_bucket_unenforced_configs", "kind" => kind.as_str()).set(count as f64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The first count waits for the first pass of adoption, and every pass
    /// counts again: a configuration adoption dropped leaves the count the
    /// health reports. The task stops on shutdown, adoption over or not.
    #[tokio::test]
    async fn the_count_follows_the_passes_of_adoption() {
        let dir = tempfile::tempdir().unwrap();
        let storage = s4_core::storage::BitcaskStorageEngine::new(
            dir.path().join("volumes"),
            dir.path().join("metadata_db"),
            1024 * 1024,
            4096,
            false,
        )
        .await
        .unwrap();
        let key = format!("{}moved", s4_core::types::VERSIONING_CONFIG_KEY_PREFIX);
        storage
            .put_object(
                s4_core::types::SYSTEM_BUCKET,
                &key,
                b"Enabled",
                "text/plain",
                &std::collections::HashMap::new(),
            )
            .await
            .unwrap();
        let storage: Arc<dyn StorageEngine> = Arc::new(storage);
        let snapshot = UnenforcedConfigs::default();
        let (passes, passes_rx) = watch::channel(0u64);
        let (shutdown, shutdown_rx) = watch::channel(false);
        let task = tokio::spawn(run(
            storage.clone(),
            snapshot.clone(),
            passes_rx,
            shutdown_rx,
        ));
        let counted = |expected: usize| {
            let snapshot = snapshot.clone();
            async move {
                for _ in 0..200 {
                    if snapshot.get().len() == expected {
                        return true;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                false
            }
        };

        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(snapshot.get().is_empty(), "counted before the first pass");
        passes.send_modify(|count| *count += 1);
        assert!(counted(1).await);

        storage.delete_object(s4_core::types::SYSTEM_BUCKET, &key).await.unwrap();
        passes.send_modify(|count| *count += 1);
        assert!(counted(0).await);

        drop(passes);
        shutdown.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(5), task).await.unwrap().unwrap();
    }

    /// Each kind is counted under its own label, and a kind with nothing
    /// stored is exported as zero rather than left out.
    #[test]
    fn the_gauge_counts_each_kind() {
        let config = |bucket: &str, kind| UnenforcedBucketConfig {
            bucket: bucket.to_string(),
            kind,
        };
        let recorder = s4_api::prometheus::builder().expect("bucket bounds").build_recorder();
        metrics::with_local_recorder(&recorder, || {
            publish(&[
                config("a", UnenforcedConfigKind::Versioning),
                config("b", UnenforcedConfigKind::Versioning),
            ])
        });
        let exposition = recorder.handle().render();
        assert!(
            exposition.contains("s4_bucket_unenforced_configs{kind=\"versioning\"} 2\n"),
            "{exposition}"
        );
        assert!(
            exposition.contains("s4_bucket_unenforced_configs{kind=\"object_lock\"} 0\n"),
            "{exposition}"
        );
    }
}
