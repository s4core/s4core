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

//! The copies a node holds of keys the pool's ring does not give it (plan
//! ERASURE-CODING-FIXING-POOL-GROWTH-REBALANCE.md, `EPGR-04-3` and
//! `EPGR-04-4`).
//!
//! A pass walks every key this node holds — its version digests over the
//! whole ring, the records of `__system__` without digests, its open
//! multipart uploads — and takes each one placement no longer gives this
//! node:
//!
//! - **Hand-over** ([`ForeignMode::HandOver`]): the key is settled across its
//!   owners and this node, as anti-entropy settles a key (newer wins, an
//!   older version never overwrites a newer one). This is the pass of the
//!   former owners of a pool that grew before generations existed, on a
//!   release that deleted a copy outright: an owner without the key may have
//!   deleted it or never had it, and nothing tells the two apart. Settling
//!   leaves such a copy unconfirmed; the pass hands this node's copy over
//!   anyway when it is the newest version any replica holds, because the
//!   alternative is to lose, for good, objects the clients were told are
//!   stored. A delete made on that release after the growth may come back
//!   with it. Bucket metadata never uses this override: its own adoption
//!   and backfill require evidence from the owners, and a lone foreign copy
//!   may have missed the bucket's deletion. A made-up foreign bucket marker
//!   is not included in reconciliation at all: a legacy replica created it
//!   for an object it stored, not by a confirmed CreateBucket. It stays here
//!   without overwriting the owners' marker or tombstone. After a verified
//!   growth, clean-up may remove an aged invented marker locally under the
//!   same grace-period rule as other unconfirmed foreign copies.
//! - **Clean-up** ([`ForeignMode::CleanUp`]): the same, and once the owners
//!   hold the version — the key settled — this node's copy is removed, only
//!   if it is still that version. A copy the owners do not confirm stays,
//!   unless the pass removes such copies once they are older than the grace
//!   period ([`ForeignParts::unconfirmed_after`]): after a growth that
//!   settled, the owners were verified to hold every confirmed version at
//!   the cut-over, so a former owner's copy they neither hold nor outrank
//!   was deleted since — its tombstone collected — or was never confirmed.
//!   Kept, it would be listed for ever; a younger one is weighed again on a
//!   later pass.
//!
//! The progress is stored after every page, so a restart continues where
//! it stopped; a pass with keys that could not be settled is followed by
//! another one.

use std::sync::Arc;
use std::time::Duration;

use s4_core::storage::TombstoneRef;
use s4_core::types::{is_bucket_marker, MultipartUploadState, SYSTEM_BUCKET};
use s4_core::{KeyVersion, StorageEngine};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tracing::{debug, info, warn};

use super::families::SYSTEM_RECORD_PREFIXES;
use super::limiter::{give_way, ForegroundLoad, RateLimiter};
use super::multipart::{MultipartTransfer, UploadHandedOver};
use super::progress::{Phase, ProgressStore, MAX_RETRY_KEYS};
use crate::hlc::HlcTimestamp;
use crate::placement::ReplicationClass;
use crate::repair::reconcile::{KeyRef, Settled};
use crate::repair::Reconciler;
use crate::rpc::{proto, NodeClient};
use crate::{ClusterError, NodeId, PlacementRouter};

/// What a pass does with the copies of keys this node does not own.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForeignMode {
    /// Hands them over to their owners.
    HandOver,
    /// Hands them over, then removes them here.
    CleanUp,
}

/// Where a pass stands, stored after every page.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForeignProgress {
    /// Encoding version.
    pub format: u32,
    /// Where the current pass stands; `Done` once a pass left nothing.
    pub phase: Phase,
    /// Passes begun.
    pub passes: u32,
    /// Keys of the pass to try again (`d:` a key, `u:` an upload).
    pub retry: Vec<String>,
    /// More keys failed than are kept: another pass follows.
    pub retry_overflow: bool,
    /// Keys read.
    pub scanned: u64,
    /// Keys this node does not own.
    pub foreign: u64,
    /// Owners that took a version from this pass.
    pub handed: u64,
    /// Copies removed here.
    pub removed: u64,
    /// Copies kept: their owners do not confirm them.
    pub kept: u64,
    /// Why the last key that failed did.
    pub last_error: Option<String>,
    /// When the last pass ended without failures, Unix milliseconds.
    pub done_at: Option<u64>,
}

impl Default for ForeignProgress {
    fn default() -> Self {
        Self {
            format: 1,
            phase: Phase::Scan {
                segment: 0,
                after: None,
            },
            passes: 0,
            retry: Vec::new(),
            retry_overflow: false,
            scanned: 0,
            foreign: 0,
            handed: 0,
            removed: 0,
            kept: 0,
            last_error: None,
            done_at: None,
        }
    }
}

impl ForeignProgress {
    /// Encoded progress.
    pub fn encode(&self) -> Result<Vec<u8>, ClusterError> {
        serde_json::to_vec(self).map_err(|e| ClusterError::Codec(e.to_string()))
    }

    /// Decodes progress.
    pub fn decode(bytes: &[u8]) -> Result<Self, ClusterError> {
        let progress: Self = serde_json::from_slice(bytes)
            .map_err(|e| ClusterError::Codec(format!("foreign copies progress: {e}")))?;
        if progress.format != 1 {
            return Err(ClusterError::Codec(format!(
                "foreign copies progress: unsupported format {}",
                progress.format
            )));
        }
        Ok(progress)
    }

    /// The progress stored under `name`, if any.
    pub async fn load(store: &dyn ProgressStore, name: &str) -> Result<Option<Self>, ClusterError> {
        for (stored, bytes) in store.load().await? {
            if stored == name {
                return Self::decode(&bytes).map(Some);
            }
        }
        Ok(None)
    }

    fn failed(&mut self, key: String, why: String) {
        if !self.retry.contains(&key) {
            if self.retry.len() < MAX_RETRY_KEYS {
                self.retry.push(key);
            } else {
                self.retry_overflow = true;
            }
        }
        self.last_error = Some(why);
    }
}

/// What became of one key.
enum KeyTaken {
    /// This node owns it, or holds nothing of it.
    Own,
    /// Handed over; removed here when `removed`.
    Handed { brought: bool, removed: bool },
    /// Kept: the owners do not confirm this copy.
    Kept,
    /// Not settled now.
    Failed(String),
}

/// Everything a pass needs.
pub struct ForeignParts {
    /// This node.
    pub local: NodeId,
    /// Placement: who owns each key now.
    pub router: Arc<PlacementRouter>,
    /// This node's storage.
    pub storage: Arc<dyn StorageEngine>,
    /// Settles a key across its owners and this node; its source must ask
    /// this node too ([`crate::coordinator::WithHolder`]).
    pub reconciler: Reconciler,
    /// This node's open uploads, when it takes multipart writes.
    pub multipart: Option<MultipartTransfer>,
    /// Calls to the owners, for erasure-coded namespace entries.
    pub rpc_client: Arc<NodeClient>,
    /// The topology epoch of replica calls.
    pub topology_epoch: u64,
    /// Where the progress is kept.
    pub store: Arc<dyn ProgressStore>,
    /// The name it is kept under.
    pub name: String,
    /// Hand over, or hand over and remove.
    pub mode: ForeignMode,
    /// How busy the clients keep this node.
    pub load: Arc<dyn ForegroundLoad>,
    /// Keys settled per second, at most.
    pub max_keys_per_sec: u64,
    /// Keys read per page.
    pub page_size: usize,
    /// How long a pass with failures waits before the next one.
    pub retry_after: Duration,
    /// A tombstone older than this is not carried to an owner without the key.
    pub gc_grace: Duration,
    /// With [`ForeignMode::CleanUp`] after a growth that settled: a copy the
    /// owners do not confirm is removed once it is this much older than now,
    /// and weighed again later while younger. `None` keeps such copies — the
    /// pass of a pool grown before generations, whose former owner may hold
    /// the only copy.
    pub unconfirmed_after: Option<Duration>,
}

/// Passes over this node's foreign copies; see the module documentation.
pub struct ForeignCopies {
    parts: ForeignParts,
    keys: RateLimiter,
}

/// The segments of a pass: the whole ring's digests, each prefix of the
/// records of `__system__` without digests, the uploads.
const SEGMENTS: u32 = 2 + SYSTEM_RECORD_PREFIXES.len() as u32;

impl ForeignCopies {
    fn recheck_kept(&self, progress: &ForeignProgress) -> bool {
        self.parts.mode == ForeignMode::CleanUp
            && self.parts.unconfirmed_after.is_some()
            && progress.kept > 0
    }

    /// The passes `parts` describe.
    pub fn new(parts: ForeignParts) -> Self {
        Self {
            keys: RateLimiter::per_second(parts.max_keys_per_sec.max(1)),
            parts,
        }
    }

    /// Runs passes once `ready` says so, until one ends without failures or
    /// `shutdown`; the progress survives a restart. Returns the progress of
    /// the last pass.
    pub async fn run(
        &self,
        ready: impl Fn() -> Result<(), String> + Send + Sync,
        mut shutdown: watch::Receiver<bool>,
    ) -> Option<ForeignProgress> {
        // A pass done before a restart is done.
        if let Ok(Some(progress)) =
            ForeignProgress::load(&*self.parts.store, &self.parts.name).await
        {
            if progress.phase == Phase::Done && !self.recheck_kept(&progress) {
                return Some(progress);
            }
        }
        let mut last_wait = None;
        loop {
            match ready() {
                Ok(()) => break,
                Err(why) => {
                    if last_wait.as_ref() != Some(&why) {
                        info!(pass = %self.parts.name, %why, "a pass over this node's copies of keys it does not own waits");
                        last_wait = Some(why);
                    }
                }
            }
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(5)) => {}
                changed = shutdown.changed() => if changed.is_err() || *shutdown.borrow() { return None; },
            }
        }
        loop {
            let progress = match self.pass(&shutdown).await {
                Ok(Some(progress)) => progress,
                Ok(None) => return None,
                Err(e) => {
                    warn!(pass = %self.parts.name, error = %e, "a pass over this node's foreign copies could not go on");
                    ForeignProgress::default()
                }
            };
            if progress.phase == Phase::Done {
                info!(
                    pass = %self.parts.name,
                    foreign = progress.foreign,
                    handed = progress.handed,
                    removed = progress.removed,
                    kept = progress.kept,
                    "a pass over this node's copies of keys it does not own ended"
                );
                return Some(progress);
            }
            tokio::select! {
                _ = tokio::time::sleep(self.parts.retry_after) => {}
                changed = shutdown.changed() => if changed.is_err() || *shutdown.borrow() { return None; },
            }
        }
    }

    async fn save(&self, progress: &ForeignProgress) -> Result<(), ClusterError> {
        self.parts.store.save(&self.parts.name, progress.encode()?).await
    }

    /// Continues the stored pass; `None` once `shutdown`.
    pub async fn pass(
        &self,
        shutdown: &watch::Receiver<bool>,
    ) -> Result<Option<ForeignProgress>, ClusterError> {
        let mut progress = ForeignProgress::load(&*self.parts.store, &self.parts.name)
            .await?
            .unwrap_or_default();
        if progress.phase == Phase::Done {
            if !self.recheck_kept(&progress) {
                return Ok(Some(progress));
            }
            // Earlier builds declared completion while keeping invented
            // markers. A settled growth's bounded clean-up policy can now
            // reconsider them; legacy recovery's completed passes stay done.
            progress = ForeignProgress::default();
        }
        if progress.passes == 0 {
            progress.passes = 1;
        }
        loop {
            if *shutdown.borrow() {
                self.save(&progress).await?;
                return Ok(None);
            }
            match progress.phase.clone() {
                Phase::Done => {
                    progress.done_at = Some(crate::clock::hlc::now_millis());
                    self.save(&progress).await?;
                    return Ok(Some(progress));
                }
                Phase::Retry => {
                    let keys = std::mem::take(&mut progress.retry);
                    for key in keys {
                        let taken = match key.split_once(':') {
                            Some(("u", upload)) => self.take_upload(upload).await,
                            Some((_, full_key)) => self.take_key(full_key).await,
                            None => KeyTaken::Own,
                        };
                        self.account(&mut progress, key, taken);
                    }
                    if progress.retry.is_empty() && !progress.retry_overflow {
                        progress.phase = Phase::Done;
                        continue;
                    }
                    // What still fails is taken by the next pass, after a wait.
                    progress.retry.clear();
                    progress.retry_overflow = false;
                    progress.passes += 1;
                    progress.phase = Phase::Scan {
                        segment: 0,
                        after: None,
                    };
                    self.save(&progress).await?;
                    return Ok(Some(progress));
                }
                Phase::Scan { segment, after } => {
                    if segment >= SEGMENTS {
                        progress.phase = if progress.retry.is_empty() && !progress.retry_overflow {
                            Phase::Done
                        } else {
                            Phase::Retry
                        };
                        continue;
                    }
                    let page = self.page(segment, after.as_deref()).await?;
                    let full = page.len() >= self.parts.page_size;
                    for (id, is_upload) in &page {
                        if *shutdown.borrow() {
                            self.save(&progress).await?;
                            return Ok(None);
                        }
                        let taken = if *is_upload {
                            self.take_upload(id).await
                        } else {
                            self.take_key(id).await
                        };
                        let tag = if *is_upload { "u" } else { "d" };
                        self.account(&mut progress, format!("{tag}:{id}"), taken);
                        progress.phase = Phase::Scan {
                            segment,
                            after: Some(id.clone()),
                        };
                    }
                    if !full {
                        progress.phase = Phase::Scan {
                            segment: segment + 1,
                            after: None,
                        };
                    }
                    self.save(&progress).await?;
                }
            }
        }
    }

    fn account(&self, progress: &mut ForeignProgress, key: String, taken: KeyTaken) {
        progress.scanned += 1;
        match taken {
            KeyTaken::Own => {}
            KeyTaken::Handed { brought, removed } => {
                progress.foreign += 1;
                progress.handed += u64::from(brought);
                progress.removed += u64::from(removed);
            }
            KeyTaken::Kept => {
                progress.foreign += 1;
                progress.kept += 1;
            }
            KeyTaken::Failed(why) => {
                progress.foreign += 1;
                debug!(%key, %why, "a foreign copy could not be settled now");
                progress.failed(key, why);
            }
        }
    }

    /// The ids of segment `segment` after `after`: keys, or uploads.
    async fn page(
        &self,
        segment: u32,
        after: Option<&str>,
    ) -> Result<Vec<(String, bool)>, ClusterError> {
        let limit = self.parts.page_size;
        let storage_error = |e: s4_core::StorageError| ClusterError::Storage(e.to_string());
        if segment == 0 {
            let page = self
                .parts
                .storage
                .version_digests_in(0, None, after, limit)
                .await
                .map_err(storage_error)?;
            return Ok(page.into_iter().map(|entry| (entry.key, false)).collect());
        }
        let index = (segment - 1) as usize;
        if let Some(prefix) = SYSTEM_RECORD_PREFIXES.get(index) {
            let after = after
                .and_then(|full| full.strip_prefix(&format!("{SYSTEM_BUCKET}/")))
                .unwrap_or("");
            let page = self
                .parts
                .storage
                .list_objects_after(SYSTEM_BUCKET, prefix, after, limit)
                .await
                .map_err(storage_error)?;
            return Ok(page
                .into_iter()
                .map(|(key, _)| (format!("{SYSTEM_BUCKET}/{key}"), false))
                .collect());
        }
        let Some(multipart) = &self.parts.multipart else {
            return Ok(Vec::new());
        };
        Ok(multipart
            .sessions_after(after, limit)
            .await?
            .into_iter()
            .map(|session| (session.upload_id, true))
            .collect())
    }

    /// The owners of `bucket/key` now, `None` when this node is one.
    fn owners(&self, bucket: &str, key: &str) -> Result<Option<Vec<NodeId>>, String> {
        let decision = self.parts.router.route(bucket, key).map_err(|e| e.to_string())?;
        Ok((!decision.replicas.contains(&self.parts.local)).then_some(decision.replicas))
    }

    async fn take_key(&self, full_key: &str) -> KeyTaken {
        let Some((bucket, key)) = full_key.split_once('/') else {
            return KeyTaken::Own;
        };
        let owners = match self.owners(bucket, key) {
            Ok(Some(owners)) => owners,
            Ok(None) => return KeyTaken::Own,
            Err(why) => return KeyTaken::Failed(why),
        };
        let held = match self.parts.storage.key_version(bucket, key).await {
            Ok(KeyVersion::Absent) => return KeyTaken::Own,
            Ok(held) => held,
            Err(e) => return KeyTaken::Failed(e.to_string()),
        };
        give_way(self.parts.load.as_ref(), 32, Duration::from_secs(1)).await;
        self.keys.spend(1).await;
        if let KeyVersion::Live(record) = &held {
            // Before pool generations a replica could manufacture a marker
            // merely because it stored an object. Its modification time can
            // rank above a real delete; neither the holder nor that timestamp
            // authorises writing it back to the bucket's actual owners.
            if is_bucket_marker(bucket, key) && HlcTimestamp::of_record(record).is_made_up() {
                if let Some(after) =
                    self.parts.unconfirmed_after.filter(|_| self.parts.mode == ForeignMode::CleanUp)
                {
                    // Only the clean-up of a verified, settled growth sets
                    // this policy. Never reconcile a manufactured marker
                    // with the owners: it could outrank their real delete.
                    let after = u64::try_from(after.as_millis()).unwrap_or(u64::MAX);
                    if HlcTimestamp::of_record(record).wall_time.saturating_add(after)
                        > crate::clock::hlc::now_millis()
                    {
                        return KeyTaken::Failed(
                            "an invented foreign bucket marker is younger than the grace period"
                                .into(),
                        );
                    }
                    return match self
                        .parts
                        .storage
                        .delete_object_if_hlc(
                            bucket,
                            key,
                            record.hlc_timestamp,
                            record.hlc_logical,
                            record.origin_node_id,
                        )
                        .await
                    {
                        Ok(true) => KeyTaken::Handed {
                            brought: false,
                            removed: true,
                        },
                        Ok(false) => KeyTaken::Failed(
                            "the invented foreign bucket marker changed before removal".into(),
                        ),
                        Err(error) => KeyTaken::Failed(error.to_string()),
                    };
                }
                debug!(
                    bucket,
                    key, "a made-up foreign bucket marker stays out of reconciliation"
                );
                return KeyTaken::Kept;
            }
            if record.is_erasure_coded() {
                return self.take_namespace_entry(bucket, key, record, &owners).await;
            }
        }
        let mut required = owners.clone();
        required.push(self.parts.local);
        let (settled, brought) = self
            .parts
            .reconciler
            .settle_counting(KeyRef { bucket, key }, Some(&required), self.parts.gc_grace)
            .await;
        match settled {
            Settled::InStep => {
                let removed = self.parts.mode == ForeignMode::CleanUp
                    && match self.remove_local(bucket, key, full_key).await {
                        Ok(removed) => removed,
                        Err(why) => return KeyTaken::Failed(why),
                    };
                KeyTaken::Handed {
                    brought: brought > 0,
                    removed,
                }
            }
            Settled::Ambiguous(why)
                if self.parts.mode == ForeignMode::HandOver
                    && ReplicationClass::of(bucket, key) != ReplicationClass::BucketMetadata =>
            {
                self.spread_own(bucket, key, &owners, brought, &why).await
            }
            Settled::Ambiguous(why) => match self.parts.unconfirmed_after {
                Some(after) if self.parts.mode == ForeignMode::CleanUp => {
                    match self.remove_unconfirmed(bucket, key, full_key, after).await {
                        Ok(true) => KeyTaken::Handed {
                            brought: brought > 0,
                            removed: true,
                        },
                        Ok(false) => KeyTaken::Failed(format!(
                            "a copy its owners do not confirm is younger than the grace period, \
                             weighed again later: {why}"
                        )),
                        Err(e) => KeyTaken::Failed(e),
                    }
                }
                _ => KeyTaken::Kept,
            },
            Settled::NotOurs => KeyTaken::Kept,
            Settled::Failed(why) => KeyTaken::Failed(why),
        }
    }

    /// Hands this node's copy of `bucket/key` to `owners` when it is the
    /// newest version any replica holds and the owners do not confirm it: the
    /// hand-over of a pool grown before generations (module documentation).
    async fn spread_own(
        &self,
        bucket: &str,
        key: &str,
        owners: &[NodeId],
        brought_before: usize,
        why: &str,
    ) -> KeyTaken {
        let versions = match self.parts.reconciler.source().key_versions(bucket, key).await {
            Ok(versions) => versions,
            Err(e) => return KeyTaken::Failed(e.to_string()),
        };
        let newest = versions.answers.iter().filter_map(|(_, holds)| holds.rank()).max();
        let own = versions
            .answers
            .iter()
            .find(|(node, _)| *node == self.parts.local)
            .and_then(|(_, holds)| holds.rank());
        let Some(newest) = newest.filter(|newest| own == Some(*newest)) else {
            debug!(bucket, key, %why, "a copy newer than this node's is unconfirmed; kept");
            return KeyTaken::Kept;
        };
        match self
            .parts
            .reconciler
            .bring_confirmed(KeyRef { bucket, key }, &versions, newest, owners)
            .await
        {
            Ok(brought) => KeyTaken::Handed {
                brought: brought + brought_before > 0,
                removed: false,
            },
            Err(why) => KeyTaken::Failed(why),
        }
    }

    /// Removes this node's copy of `bucket/key`, which its owners do not
    /// confirm, if it is older than `after`; whether it was removed.
    async fn remove_unconfirmed(
        &self,
        bucket: &str,
        key: &str,
        full_key: &str,
        after: Duration,
    ) -> Result<bool, String> {
        let wall = match self.parts.storage.key_version(bucket, key).await {
            Ok(KeyVersion::Live(record) | KeyVersion::Tombstone(record)) => record.hlc_timestamp,
            Ok(KeyVersion::Absent) => return Ok(true),
            Err(e) => return Err(e.to_string()),
        };
        let now = crate::clock::hlc::now_millis();
        let after = u64::try_from(after.as_millis()).unwrap_or(u64::MAX);
        if wall.saturating_add(after) > now {
            return Ok(false);
        }
        self.remove_local(bucket, key, full_key).await
    }

    /// Removes this node's copy of `bucket/key` as it holds it now: the
    /// version the owners were just brought to, or something it took since
    /// and was weighed against them the same way.
    async fn remove_local(&self, bucket: &str, key: &str, full_key: &str) -> Result<bool, String> {
        match self.parts.storage.key_version(bucket, key).await {
            Ok(KeyVersion::Live(record)) => self
                .parts
                .storage
                .delete_object_if_hlc(
                    bucket,
                    key,
                    record.hlc_timestamp,
                    record.hlc_logical,
                    record.origin_node_id,
                )
                .await
                .map_err(|e| e.to_string()),
            Ok(KeyVersion::Tombstone(record)) => self
                .parts
                .storage
                .gc_tombstones(&[TombstoneRef {
                    full_key: full_key.to_string(),
                    hlc: (
                        record.hlc_timestamp,
                        record.hlc_logical,
                        record.origin_node_id,
                    ),
                }])
                .await
                .map(|removed| removed > 0)
                .map_err(|e| e.to_string()),
            Ok(KeyVersion::Absent) => Ok(false),
            Err(e) => Err(e.to_string()),
        }
    }

    /// Hands the namespace entry of an erasure-coded object to its owners;
    /// one that holds an entry keeps it. Its shards stay on their set.
    async fn take_namespace_entry(
        &self,
        bucket: &str,
        key: &str,
        record: &s4_core::types::IndexRecord,
        owners: &[NodeId],
    ) -> KeyTaken {
        let encoded = match bincode::serialize(record) {
            Ok(encoded) => encoded,
            Err(e) => return KeyTaken::Failed(e.to_string()),
        };
        let generation = self
            .parts
            .router
            .route(bucket, key)
            .map(|decision| decision.placement_generation)
            .unwrap_or(0);
        let mut brought = false;
        for node in owners {
            let request = proto::RestoreNamespaceEntryRequest {
                bucket: bucket.to_string(),
                key: key.to_string(),
                record: encoded.clone(),
                coordinator_id: self.parts.local.to_bytes().to_vec(),
                operation_id: uuid::Uuid::new_v4().as_u128().to_be_bytes().to_vec(),
                topology_epoch: self.parts.topology_epoch,
                placement_generation: generation,
            };
            match self.parts.rpc_client.send_restore_namespace_entry(*node, request).await {
                Ok(response) if response.success => brought |= response.restored,
                Ok(response) => {
                    return KeyTaken::Failed(format!("replica {node}: {}", response.error))
                }
                Err(e) => return KeyTaken::Failed(format!("replica {node}: {e}")),
            }
        }
        let removed = self.parts.mode == ForeignMode::CleanUp
            && match self
                .parts
                .storage
                .delete_object_if_hlc(
                    bucket,
                    key,
                    record.hlc_timestamp,
                    record.hlc_logical,
                    record.origin_node_id,
                )
                .await
            {
                Ok(removed) => removed,
                Err(e) => return KeyTaken::Failed(e.to_string()),
            };
        KeyTaken::Handed { brought, removed }
    }

    async fn take_upload(&self, upload_id: &str) -> KeyTaken {
        let Some(multipart) = &self.parts.multipart else {
            return KeyTaken::Own;
        };
        let Some(session) = multipart.local_session(upload_id).await else {
            return KeyTaken::Own;
        };
        if session.state != MultipartUploadState::Open {
            return KeyTaken::Own;
        }
        let owners = match self.owners(&session.bucket, &session.key) {
            Ok(Some(owners)) => owners,
            Ok(None) => return KeyTaken::Own,
            Err(why) => return KeyTaken::Failed(why),
        };
        give_way(self.parts.load.as_ref(), 32, Duration::from_secs(1)).await;
        self.keys.spend(1).await;
        match multipart.hand_over(&session, Some(&owners)).await {
            UploadHandedOver::InStep { parts, .. } => {
                let removed = self.parts.mode == ForeignMode::CleanUp
                    && match multipart.abort_local(upload_id).await {
                        Ok(()) => true,
                        Err(e) => return KeyTaken::Failed(e.to_string()),
                    };
                KeyTaken::Handed {
                    brought: parts > 0,
                    removed,
                }
            }
            UploadHandedOver::Gone => KeyTaken::Own,
            UploadHandedOver::Failed(why) | UploadHandedOver::Damaged(why) => KeyTaken::Failed(why),
        }
    }
}
