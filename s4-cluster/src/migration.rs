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

//! Resumable migration of legacy records to every current placement owner.

use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use futures::{stream, StreamExt};
use s4_core::storage::{BitcaskStorageEngine, IndexDb, VersionRank};
use s4_core::{IndexRecord, ObjectStream, StorageError};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncReadExt, ReadBuf};
use tokio::sync::{mpsc, watch, OwnedRwLockReadGuard, RwLock};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{info, warn};

use crate::clock::hlc::HlcTimestamp;
use crate::placement::{GenerationPhase, PlacementDecision, PlacementRouter};
use crate::protocol::{PROTOCOL_VERSION, S4_VERSION};
use crate::repair::reconcile::{
    newest_of_among, KeyRef, NewestVersion, Reconciler, ReplicaHolds, Settled,
};
use crate::rpc::proto;
use crate::{ClusterError, GossipHandle, NodeClient, NodeId, NodeStatus};

fn storage_error(error: StorageError) -> ClusterError {
    ClusterError::Storage(error.to_string())
}

const PAGE_SIZE: usize = 256;
const CHUNK_SIZE: usize = 256 * 1024;
const MARKER_PREFIX: &str = "__system__/__s4_bucket_marker_";

/// Source operations needed by migration. The shared server adapter holds
/// the same external storage lock as client writes and compaction.
#[async_trait::async_trait]
pub trait MigrationStore: Send + Sync {
    /// Durable queue and paged legacy index.
    fn index(&self) -> Arc<IndexDb>;
    /// A checked snapshot stream; never follows an obsolete blob offset.
    async fn open(
        &self,
        bucket: &str,
        key: &str,
        expected: &IndexRecord,
    ) -> Result<ObjectStream, StorageError>;
    /// Rechecks the complete current snapshot and finishes the obligation
    /// under the external write lock. A changed source is left pending.
    async fn finalize(
        &self,
        full: &str,
        original: &IndexRecord,
        observed: Option<&IndexRecord>,
        keep: bool,
    ) -> Result<bool, StorageError>;
    /// Marks completion only while both the queue and legacy index are empty.
    async fn complete(&self) -> Result<bool, StorageError>;
}

/// Migration access to the server's shared Bitcask engine.
pub struct SharedMigrationStore {
    storage: Arc<RwLock<BitcaskStorageEngine>>,
    index: Arc<IndexDb>,
}
impl SharedMigrationStore {
    /// Uses the existing shared lock, and its index handle.
    pub fn new(storage: Arc<RwLock<BitcaskStorageEngine>>, index: Arc<IndexDb>) -> Self {
        Self { storage, index }
    }
}
struct LockedReader {
    body: Box<dyn AsyncRead + Unpin + Send>,
    _guard: OwnedRwLockReadGuard<BitcaskStorageEngine>,
}
impl AsyncRead for LockedReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.body).poll_read(cx, buf)
    }
}
#[async_trait::async_trait]
impl MigrationStore for SharedMigrationStore {
    fn index(&self) -> Arc<IndexDb> {
        self.index.clone()
    }
    async fn open(
        &self,
        bucket: &str,
        key: &str,
        expected: &IndexRecord,
    ) -> Result<ObjectStream, StorageError> {
        let guard = self.storage.clone().read_owned().await;
        let mut object = guard.open_legacy_migration_stream(bucket, key, expected).await?;
        object.body = Box::new(LockedReader {
            body: object.body,
            _guard: guard,
        });
        Ok(object)
    }
    async fn finalize(
        &self,
        full: &str,
        original: &IndexRecord,
        observed: Option<&IndexRecord>,
        keep: bool,
    ) -> Result<bool, StorageError> {
        let guard = self.storage.write().await;
        finalize_store(&guard, full, original, observed, keep).await
    }
    async fn complete(&self) -> Result<bool, StorageError> {
        let guard = self.storage.write().await;
        complete_store(&guard).await
    }
}
async fn finalize_store(
    storage: &BitcaskStorageEngine,
    full: &str,
    original: &IndexRecord,
    observed: Option<&IndexRecord>,
    keep: bool,
) -> Result<bool, StorageError> {
    if storage.index_db().get(full).await?.as_ref() != observed {
        return Ok(false);
    }
    let (bucket, key) = split_key(full)?;
    if let Some(current) = observed.filter(|current| {
        current.hlc_order().is_none()
            && VersionRank::of_record(current) == VersionRank::of_record(original)
    }) {
        let changed = if keep {
            storage.stamp_legacy_if_unchanged(bucket, key, current).await?
        } else {
            storage.remove_legacy_if_unchanged(bucket, key, current).await?
        };
        if !changed {
            return Ok(false);
        }
    }
    storage.index_db().finish_legacy_migration_key(full).await?;
    Ok(true)
}
async fn complete_store(storage: &BitcaskStorageEngine) -> Result<bool, StorageError> {
    let index = storage.index_db();
    if !index.legacy_migration_queue_page(None, 1).await?.is_empty() {
        return Ok(false);
    }
    for markers in [true, false] {
        if !index.legacy_migration_page(markers, None, 1).await?.is_empty() {
            return Ok(false);
        }
    }
    index.mark_legacy_migration_complete().await?;
    Ok(true)
}
fn split_key(full: &str) -> Result<(&str, &str), StorageError> {
    full.split_once('/')
        .ok_or_else(|| StorageError::InvalidData(format!("invalid migration key {full}")))
}

/// Migration counts read by health and Prometheus on this node.
#[derive(Default)]
pub struct MigrationStatus {
    pending: AtomicU64,
    migrated: AtomicU64,
    superseded: AtomicU64,
}
impl MigrationStatus {
    /// Obligations still needing confirmation from every owner.
    pub fn pending(&self) -> u64 {
        self.pending.load(Ordering::Relaxed)
    }
    /// Original single versions completed during this process lifetime.
    pub fn migrated(&self) -> u64 {
        self.migrated.load(Ordering::Relaxed)
    }
    /// Obligations completed at a newer client version during this lifetime.
    pub fn superseded(&self) -> u64 {
        self.superseded.load(Ordering::Relaxed)
    }
    pub(crate) fn pending_is(&self, count: u64) {
        self.pending.store(count, Ordering::Relaxed);
        metrics::gauge!("s4_cluster_migration_pending_records").set(count as f64);
    }
    fn finished(&self, superseded: bool) {
        let (counter, outcome) = if superseded {
            (&self.superseded, "superseded")
        } else {
            (&self.migrated, "migrated")
        };
        counter.fetch_add(1, Ordering::Relaxed);
        metrics::counter!("s4_cluster_migration_records_total", "outcome" => outcome).increment(1);
    }
}

/// Existing cluster services used by one migration worker.
pub struct MigrationParts {
    /// Shared engine access.
    pub storage: Arc<dyn MigrationStore>,
    /// All replication classes and current placement fences.
    pub router: Arc<PlacementRouter>,
    /// Streaming peer writes.
    pub rpc: Arc<NodeClient>,
    /// Membership and version announcements.
    pub gossip: Arc<GossipHandle>,
    /// Existing history-preserving reconciliation of newer client versions.
    pub reconciler: Reconciler,
    /// Source node.
    pub local: NodeId,
    /// Every configured member, including the source.
    pub members: Vec<NodeId>,
    /// Wire topology epoch.
    pub epoch: u64,
    /// Maximum concurrent records.
    pub concurrency: usize,
}

/// A resumable, bounded scan with durable obligations and no new HLCs.
pub struct MigrationWorker {
    parts: MigrationParts,
    status: Arc<MigrationStatus>,
    call_timeout: Duration,
}
impl MigrationWorker {
    /// Creates a worker. Call `initialize` before publishing HTTP readiness.
    pub fn new(mut parts: MigrationParts) -> Result<Self, ClusterError> {
        if parts.concurrency == 0 {
            return Err(ClusterError::Config(
                "S4_CLUSTER_MIGRATION_CONCURRENCY must be positive".into(),
            ));
        }
        parts.reconciler = parts.reconciler.without_buffered_source(parts.local);
        Ok(Self {
            parts,
            status: Arc::default(),
            call_timeout: Duration::from_secs(60),
        })
    }
    /// The same worker, bounding each complete record operation by `timeout`.
    pub fn with_call_timeout(mut self, timeout: Duration) -> Self {
        self.call_timeout = timeout.min(Duration::from_secs(60));
        self
    }
    /// Shared counters for cluster health.
    pub fn status(&self) -> Arc<MigrationStatus> {
        self.status.clone()
    }

    /// Persists all original snapshots in bounded batches before serving HTTP,
    /// then counts all outstanding obligations, including superseded records.
    pub async fn initialize(&self) -> Result<(), ClusterError> {
        let index = self.parts.storage.index();
        if index.legacy_migration_complete().await.map_err(storage_error)? {
            self.status.pending_is(0);
            return Ok(());
        }
        self.discover().await?;
        self.count().await?;
        Ok(())
    }
    /// Captures original obligations before inbound replicas or background
    /// writers can change them. Startup calls this before creating identity.
    pub async fn prepare(index: &IndexDb) -> Result<(), ClusterError> {
        if index.legacy_migration_complete().await.map_err(storage_error)? {
            return Ok(());
        }
        for markers in [true, false] {
            let mut after = None;
            loop {
                let page = index
                    .legacy_migration_page(markers, after.as_deref(), PAGE_SIZE)
                    .await
                    .map_err(storage_error)?;
                index.track_legacy_migration_page(&page).await.map_err(storage_error)?;
                after = page.last().map(|(key, _)| key.clone());
                if page.len() < PAGE_SIZE {
                    break;
                }
            }
        }
        Ok(())
    }
    async fn discover(&self) -> Result<(), ClusterError> {
        Self::prepare(&self.parts.storage.index()).await
    }
    async fn count(&self) -> Result<u64, ClusterError> {
        let index = self.parts.storage.index();
        let mut after = None;
        let mut count = 0u64;
        loop {
            let page = index
                .legacy_migration_queue_page(after.as_deref(), PAGE_SIZE)
                .await
                .map_err(storage_error)?;
            count = count.saturating_add(page.len() as u64);
            after = page.last().map(|(key, _)| key.clone());
            if page.len() < PAGE_SIZE {
                break;
            }
        }
        self.status.pending_is(count);
        Ok(count)
    }
    /// Initial gate only: once started, unavailable nodes delay only their keys.
    pub fn start_gate(&self) -> Result<(), String> {
        for node in &self.parts.members {
            if *node == self.parts.local {
                continue;
            }
            let state = self
                .parts
                .gossip
                .get_node(node)
                .ok_or_else(|| format!("member {node} has not announced its version"))?;
            if state.status != NodeStatus::Alive {
                return Err(format!("member {node} is {}", state.status.as_str()));
            }
            if state.meta.s4_version != S4_VERSION {
                return Err(format!(
                    "member {node} announces S4 '{}', expected '{S4_VERSION}'",
                    state.meta.s4_version
                ));
            }
            if state.meta.protocol_version != PROTOCOL_VERSION {
                return Err(format!(
                    "member {node} announces protocol {}, expected {PROTOCOL_VERSION}",
                    state.meta.protocol_version
                ));
            }
        }
        Ok(())
    }
    /// Runs until completion or shutdown. The version gate is checked once;
    /// every attempt still requires every current owner to answer.
    pub async fn run(self, mut shutdown: watch::Receiver<bool>) {
        if self.status.pending() == 0 {
            // A trusted completion already proved the index empty of legacy records.
            if matches!(
                self.parts.storage.index().legacy_migration_complete().await,
                Ok(true)
            ) {
                return;
            }
            if let Err(error) = self.parts.storage.complete().await {
                warn!(%error, "Migration completion could not be recorded");
            }
            return;
        }
        loop {
            if *shutdown.borrow() {
                return;
            }
            match self.start_gate() {
                Ok(()) => break,
                Err(reason) => {
                    info!(%reason, pending = self.status.pending(), "Single-to-cluster migration waiting for members")
                }
            }
            tokio::select! {
                _ = shutdown.changed() => return,
                _ = tokio::time::sleep(Duration::from_secs(5)) => {}
            }
        }
        info!(
            count = self.status.pending(),
            concurrency = self.parts.concurrency,
            "Single-to-cluster migration started"
        );
        loop {
            let pass_shutdown = shutdown.clone();
            tokio::select! {
                _ = shutdown.changed() => return,
                result = self.pass(&pass_shutdown) => match result {
                    Ok(true) => {
                        info!(count = self.status.migrated() + self.status.superseded(), migrated = self.status.migrated(), superseded = self.status.superseded(), "Single-to-cluster migration complete");
                        return;
                    }
                    Ok(false) => {}
                    Err(error) => warn!(%error, "Single-to-cluster migration pass failed"),
                }
            }
            tokio::select! {
                _ = shutdown.changed() => return,
                _ = tokio::time::sleep(Duration::from_secs(5)) => {}
            }
        }
    }
    /// One bounded-memory pass. All markers are attempted before objects;
    /// an unresolved marker blocks only the objects of its own bucket.
    pub async fn pass(&self, shutdown: &watch::Receiver<bool>) -> Result<bool, ClusterError> {
        let index = self.parts.storage.index();
        if index.legacy_migration_complete().await.map_err(storage_error)? {
            return Ok(true);
        }
        for markers in [true, false] {
            let mut after = None;
            loop {
                if *shutdown.borrow() {
                    return Ok(false);
                }
                let page = index
                    .legacy_migration_queue_page(after.as_deref(), PAGE_SIZE)
                    .await
                    .map_err(storage_error)?;
                after = page.last().map(|(key, _)| key.clone());
                let last_page = page.len() < PAGE_SIZE;
                stream::iter(
                    page.into_iter().filter(|(key, _)| key.starts_with(MARKER_PREFIX) == markers),
                )
                .map(|(key, snapshot)| async move {
                    if *shutdown.borrow() {
                        return;
                    }
                    let result =
                        tokio::time::timeout(self.call_timeout, self.take(&key, &snapshot)).await;
                    match result {
                        Ok(Ok(())) => {}
                        Ok(Err(error)) => {
                            warn!(%key, %error, "Single-to-cluster record remains pending")
                        }
                        Err(_) => {
                            warn!(%key, "Single-to-cluster record timed out and remains pending")
                        }
                    }
                })
                .buffer_unordered(self.parts.concurrency)
                .collect::<Vec<_>>()
                .await;
                if last_page {
                    break;
                }
            }
        }
        // Discover defensively: completion never relies on the queue alone.
        self.discover().await?;
        if self.count().await? != 0 {
            return Ok(false);
        }
        self.parts.storage.complete().await.map_err(storage_error)
    }
    fn route(&self, bucket: &str, key: &str) -> Result<PlacementDecision, ClusterError> {
        let decision = self.parts.router.route(bucket, key)?;
        if self
            .parts
            .router
            .generation_phase(&decision.pool_id)
            .is_some_and(|phase| phase != GenerationPhase::Finished)
        {
            return Err(ClusterError::Unavailable(
                "waiting for stable pool generation placement".into(),
            ));
        }
        Ok(decision)
    }
    async fn versions(
        &self,
        key: KeyRef<'_>,
        decision: &PlacementDecision,
    ) -> Result<crate::repair::reconcile::KeyVersions, ClusterError> {
        for node in &decision.replicas {
            if *node != self.parts.local
                && !self
                    .parts
                    .gossip
                    .get_node(node)
                    .is_some_and(|state| state.status == NodeStatus::Alive)
            {
                return Err(ClusterError::Unavailable(format!(
                    "owner {node} is not alive"
                )));
            }
        }
        let versions = self.parts.reconciler.source().key_versions(key.bucket, key.key).await?;
        if versions
            .replicas
            .iter()
            .any(|node| !decision.replicas.contains(node) && *node != self.parts.local)
            || decision.replicas.iter().any(|node| !versions.replicas.contains(node))
            || !versions.unanswered.is_empty()
            || decision
                .replicas
                .iter()
                .any(|node| !versions.answers.iter().any(|(id, _)| id == node))
        {
            return Err(ClusterError::Unavailable(
                "not every current placement owner answered".into(),
            ));
        }
        Ok(versions)
    }
    async fn take(&self, full: &str, original: &IndexRecord) -> Result<(), ClusterError> {
        let (bucket, key) = split_key(full).map_err(storage_error)?;
        let key_ref = KeyRef { bucket, key };
        let index = self.parts.storage.index();
        if bucket != "__system__"
            && index
                .legacy_migration_key_pending(&format!("{MARKER_PREFIX}{bucket}"))
                .await
                .map_err(storage_error)?
        {
            return Err(ClusterError::Unavailable(
                "waiting for this bucket's marker migration".into(),
            ));
        }
        let hlc = HlcTimestamp::of_record(original);
        if hlc.is_zero() {
            return Err(ClusterError::Unavailable(
                "legacy modification time yields zero HLC; operator intervention required".into(),
            ));
        }
        let original_rank = VersionRank::of_record(original);
        let decision = self.route(bucket, key)?;
        let current = index.get(full).await.map_err(storage_error)?;
        let before = self.versions(key_ref, &decision).await?;
        let newer = before
            .answers
            .iter()
            .filter_map(|(_, holds)| holds.rank())
            .any(|rank| rank > original_rank);
        let changed = current.as_ref() != Some(original);
        let local_source = current
            .as_ref()
            .filter(|record| !record.is_tombstone() && !record.is_erasure_coded());
        if let Some(snapshot) = local_source {
            let source_rank = VersionRank::of_record(snapshot);
            let source_hlc = HlcTimestamp::of_record(snapshot);
            // The current local snapshot streams even after compaction or a
            // client overwrite. A foreign holder never acquires an owner vote.
            let mut answers = before.answers.clone();
            if !answers.iter().any(|(node, _)| *node == self.parts.local) {
                answers.push((
                    self.parts.local,
                    ReplicaHolds::Version {
                        hlc: source_hlc,
                        tombstone: false,
                        erasure_coded: false,
                        etag: source_rank.etag,
                    },
                ));
            }
            let newest = newest_of_among(&answers, before.confirming, &before.confirming_replicas);
            let stream_local = matches!(
                newest,
                NewestVersion::Copy {
                    confirmed: true,
                    ..
                }
            ) && newest.rank() == Some(source_rank)
                && source_rank >= original_rank;
            if !stream_local && !changed && !newer {
                return Err(ClusterError::Unavailable("owners cannot confirm legacy history; migration will not resurrect a possible delete".into()));
            }
            if stream_local {
                for target in &decision.replicas {
                    if *target == self.parts.local {
                        continue;
                    }
                    if before
                        .answers
                        .iter()
                        .find(|(node, _)| node == target)
                        .and_then(|(_, holds)| holds.rank())
                        .is_some_and(|rank| rank >= source_rank)
                    {
                        continue;
                    }
                    let response = self.send(*target, key_ref, snapshot, &decision).await?;
                    if !response.success
                        || response.epoch_mismatch
                        || response.stage_fenced
                        || response.lease_fenced
                    {
                        return Err(ClusterError::Unavailable(format!(
                            "owner {target} refused migration: {}",
                            response.error
                        )));
                    }
                    // A newer client version is settled below before this source
                    // can be stamped or removed.
                    if response.superseded_by.is_some() {
                        break;
                    }
                }
            }
        }
        let after_copy = self.versions(key_ref, &decision).await?;
        let now = index.get(full).await.map_err(storage_error)?;
        let superseded = changed
            || newer
            || now.as_ref() != Some(original)
            || after_copy
                .answers
                .iter()
                .filter_map(|(_, holds)| holds.rank())
                .any(|rank| rank > original_rank);
        if superseded {
            match self
                .parts
                .reconciler
                .settle_owners_counting(key_ref, &decision.replicas, Duration::MAX)
                .await
                .0
            {
                Settled::InStep => {}
                other => {
                    return Err(ClusterError::Unavailable(format!(
                        "newest client version not settled: {other:?}"
                    )))
                }
            }
        }
        let observed = index.get(full).await.map_err(storage_error)?;
        let final_versions = self.versions(key_ref, &decision).await?;
        if observed.is_none()
            && final_versions
                .answers
                .iter()
                .filter(|(node, _)| decision.replicas.contains(node))
                .all(|(_, held)| matches!(held, ReplicaHolds::Nothing { .. }))
        {
            let current_placement = self.route(bucket, key)?;
            if decision.replicas != current_placement.replicas
                || decision.placement_generation != current_placement.placement_generation
                || decision.bucket_metadata_stage != current_placement.bucket_metadata_stage
            {
                return Err(ClusterError::Unavailable(
                    "placement changed before absent migration CAS".into(),
                ));
            }
            if self
                .parts
                .storage
                .finalize(
                    full,
                    original,
                    None,
                    decision.replicas.contains(&self.parts.local),
                )
                .await
                .map_err(storage_error)?
            {
                self.status.finished(true);
                return Ok(());
            }
            return Err(ClusterError::Unavailable(
                "absent source changed before migration CAS".into(),
            ));
        }
        let newest = final_versions.answers.iter().filter_map(|(_, holds)| holds.rank()).max();
        let floor = observed
            .as_ref()
            .map(VersionRank::of_record)
            .unwrap_or(original_rank)
            .max(original_rank);
        let Some(newest) = newest.filter(|rank| *rank >= floor) else {
            return Err(ClusterError::Unavailable(
                "owners do not hold the source's current version".into(),
            ));
        };
        if final_versions
            .answers
            .iter()
            .filter(|(node, _)| decision.replicas.contains(node))
            .any(|(_, holds)| holds.rank() != Some(newest))
        {
            return Err(ClusterError::Unavailable(
                "owners still hold different versions".into(),
            ));
        }
        let current_placement = self.route(bucket, key)?;
        if decision.replicas != current_placement.replicas
            || decision.placement_generation != current_placement.placement_generation
            || decision.read_replicas != current_placement.read_replicas
            || decision.bucket_metadata_stage != current_placement.bucket_metadata_stage
        {
            return Err(ClusterError::Unavailable(
                "placement changed before migration source CAS".into(),
            ));
        }
        if !self
            .parts
            .storage
            .finalize(
                full,
                original,
                observed.as_ref(),
                decision.replicas.contains(&self.parts.local),
            )
            .await
            .map_err(storage_error)?
        {
            return Err(ClusterError::Unavailable(
                "source changed before migration CAS; retry required".into(),
            ));
        }
        self.status.finished(newest > original_rank);
        Ok(())
    }
    async fn send(
        &self,
        target: NodeId,
        key: KeyRef<'_>,
        snapshot: &IndexRecord,
        decision: &PlacementDecision,
    ) -> Result<proto::WriteBlobResponse, ClusterError> {
        let object = self
            .parts
            .storage
            .open(key.bucket, key.key, snapshot)
            .await
            .map_err(storage_error)?;
        let header = proto::WriteBlobHeader {
            placement_generation: decision.placement_generation,
            bucket: key.bucket.into(),
            key: key.key.into(),
            content_hash: if snapshot.content_hash == [0; 32] {
                Vec::new()
            } else {
                snapshot.content_hash.to_vec()
            },
            content_type: snapshot.content_type.clone(),
            metadata: snapshot
                .metadata
                .iter()
                .filter(|(name, _)| {
                    !matches!(
                        name.as_str(),
                        "_inline_data"
                            | "_s4_etag"
                            | "_s4_hlc_wall"
                            | "_s4_hlc_logical"
                            | "_s4_hlc_node_id"
                            | "_s4_operation_id"
                    )
                })
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect(),
            hlc: Some(HlcTimestamp::of_record(snapshot).to_proto()),
            version_id: None,
            coordinator_id: self.parts.local.to_bytes().to_vec(),
            operation_id: uuid::Uuid::new_v4().as_u128().to_be_bytes().to_vec(),
            topology_epoch: self.parts.epoch,
            etag: snapshot.etag.clone(),
            bucket_metadata_stage: decision
                .bucket_metadata_stage
                .map_or(0, |stage| u32::from(stage.number())),
            repair_copy: true,
            iam_lease: None,
            total_size: snapshot.size,
        };
        let (tx, rx) = mpsc::channel(2);
        let produce = produce_stream(object.body, header, tx);
        let send =
            async { self.parts.rpc.send_write_stream(target, ReceiverStream::new(rx)).await };
        let ((), response) = futures::try_join!(produce, send)?;
        Ok(response)
    }
}

/// Bounded producer: two queued chunks and one read buffer per record. It
/// verifies EOF as well as length, so a final checksum/read error cannot ACK.
async fn produce_stream(
    mut body: Box<dyn AsyncRead + Unpin + Send>,
    header: proto::WriteBlobHeader,
    tx: mpsc::Sender<proto::WriteBlobChunk>,
) -> Result<(), ClusterError> {
    use proto::write_blob_chunk::Payload;
    let total = header.total_size;
    let expected_hash = header.content_hash.clone();
    tx.send(proto::WriteBlobChunk {
        payload: Some(Payload::Header(header)),
    })
    .await
    .map_err(|_| ClusterError::Rpc("migration receiver closed before header".into()))?;
    let mut hasher = Sha256::new();
    let mut size = 0u64;
    loop {
        let mut buffer = vec![0; CHUNK_SIZE];
        let read = body
            .read(&mut buffer)
            .await
            .map_err(|error| ClusterError::Storage(error.to_string()))?;
        if read == 0 {
            break;
        }
        size = size.saturating_add(read as u64);
        if size > total {
            return Err(ClusterError::Storage(
                "migration source exceeds snapshot size".into(),
            ));
        }
        buffer.truncate(read);
        hasher.update(&buffer);
        tx.send(proto::WriteBlobChunk {
            payload: Some(Payload::Data(buffer)),
        })
        .await
        .map_err(|_| ClusterError::Rpc("migration receiver closed before EOF".into()))?;
    }
    if size != total {
        return Err(ClusterError::Storage(format!(
            "migration source ended at {size} bytes, expected {total}"
        )));
    }
    if !expected_hash.is_empty() && hasher.finalize().as_slice() != expected_hash {
        return Err(ClusterError::Storage(
            "migration source SHA-256 mismatch".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
