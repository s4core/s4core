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

//! The upkeep of IAM a node does by itself (plan CLUSTER-IAM-REPLICATION.md,
//! decisions 7 and 10).
//!
//! Every [`TICK`] the [`IamUpkeepWorker`]:
//!
//! 1. reports in `s4_iam_records_by_key_id` how many users its copy holds
//!    under each encryption key — the current key always, `0` while none;
//! 2. seals again with the current key the secrets kept under another key or
//!    in clear text, once IAM changes are open and — on a cluster node — every
//!    node of the pool is alive and announces the same key, so no node is left
//!    with secrets it cannot open;
//! 3. every [`PURGE_EVERY`], deletes the entries of the audit log older than
//!    `S4_IAM_AUDIT_RETENTION_DAYS`.
//!
//! Each step reads the node's own copy first and asks the pool for the lease
//! only when there is something to do: a pool at rest sends nothing.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use s4_cluster::IamConfigCheck;
use s4_features::iam::{IamError, IamStorage};
use tokio::sync::watch;
use tracing::{debug, info, warn};

/// How often the worker looks: each look reads the node's own copy only,
/// unless there is something to do.
pub const TICK: Duration = Duration::from_secs(5);

/// How often old entries of the audit log are looked for.
pub const PURGE_EVERY: Duration = Duration::from_secs(3600);

/// The upkeep of this node's IAM.
pub struct IamUpkeepWorker {
    iam: IamStorage,
    /// Whether the pool shares this node's encryption key; `None` on a single
    /// node, which shares it with nobody.
    config: Option<Arc<IamConfigCheck>>,
    retention: Option<chrono::Duration>,
    reported_key_ids: BTreeSet<String>,
    last_purge: Option<Instant>,
}

impl IamUpkeepWorker {
    /// The upkeep of `iam`, keeping the audit log `retention_days` (`0`:
    /// for ever), resealing on a cluster node only while `config` says the
    /// pool shares the key.
    pub fn new(iam: IamStorage, config: Option<Arc<IamConfigCheck>>, retention_days: u64) -> Self {
        Self {
            iam,
            config,
            retention: (retention_days > 0)
                .then(|| chrono::Duration::days(i64::try_from(retention_days).unwrap_or(i64::MAX))),
            reported_key_ids: BTreeSet::new(),
            last_purge: None,
        }
    }

    /// Looks every [`TICK`] until `shutdown`.
    pub async fn run(mut self, mut shutdown: watch::Receiver<bool>) {
        let mut tick = tokio::time::interval(TICK);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = tick.tick() => self.once().await,
                _ = shutdown.changed() => return,
            }
        }
    }

    /// One look.
    pub async fn once(&mut self) {
        self.report_keys().await;
        self.reseal().await;
        self.purge().await;
    }

    /// Reports in `s4_iam_records_by_key_id` how many users this node's copy
    /// holds under each key. Called once before the node serves, so the
    /// first scrape has the series of the current key.
    pub async fn report_keys(&mut self) {
        let current = match self.iam.secret_keys().await {
            Ok(keys) => keys.current().id().to_string(),
            Err(e) => {
                debug!(error = %e, "no IAM encryption key to report");
                return;
            }
        };
        match self.iam.directory().secrets_by_key_id().await {
            Ok(mut held) => {
                held.entry(current).or_insert(0);
                let gone: Vec<String> = self
                    .reported_key_ids
                    .iter()
                    .filter(|key_id| !held.contains_key(*key_id))
                    .cloned()
                    .collect();
                s4_cluster::set_iam_records_by_key_id(
                    held.iter().map(|(key_id, users)| (key_id.as_str(), *users)),
                    gone.iter().map(String::as_str),
                );
                self.reported_key_ids = held.into_keys().collect();
            }
            Err(e) => debug!(error = %e, "IAM users could not be counted by encryption key"),
        }
    }

    async fn reseal(&self) {
        if !self.iam.changes_open() {
            return;
        }
        if let Some(config) = &self.config {
            if !config.pool_shares_encryption_key() {
                return;
            }
        }
        match self.iam.reseal_secrets().await {
            Ok(done) if done.resealed > 0 || done.unreadable > 0 => info!(
                resealed = done.resealed,
                unreadable = done.unreadable,
                "secrets of IAM users' S3 credentials encrypted again with the current key"
            ),
            Ok(_) => {}
            Err(e @ (IamError::Busy { .. } | IamError::Unavailable(_))) => {
                debug!(error = %e, "secrets not encrypted again now; retried later")
            }
            Err(e) => warn!(error = %e, "secrets of IAM users could not be encrypted again"),
        }
    }

    async fn purge(&mut self) {
        let Some(retention) = self.retention else {
            return;
        };
        if self.last_purge.is_some_and(|last| last.elapsed() < PURGE_EVERY) {
            return;
        }
        match self.iam.purge_audit(chrono::Utc::now() - retention).await {
            Ok(deleted) => {
                self.last_purge = Some(Instant::now());
                if deleted > 0 {
                    info!(
                        entries = deleted,
                        "old entries of the IAM audit log deleted"
                    );
                }
            }
            Err(e @ (IamError::Busy { .. } | IamError::Unavailable(_))) => {
                debug!(error = %e, "old entries of the IAM audit log not deleted now")
            }
            Err(e) => warn!(error = %e, "old entries of the IAM audit log could not be deleted"),
        }
    }
}
