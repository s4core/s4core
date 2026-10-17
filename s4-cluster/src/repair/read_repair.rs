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

//! Asynchronous read repair service.
//!
//! When a quorum read detects version divergence across replicas, the
//! coordinator submits a [`RepairTask`] to this service. The service
//! processes repairs in the background without blocking the client response.
//!
//! # Design
//!
//! - Bounded channel prevents unbounded memory growth under heavy read load.
//! - Repairs are best-effort: failures are logged and counted but do not
//!   affect read correctness.
//! - Metrics track initiated vs. completed repairs for observability.
//! - A stale copy whose newest version is a tombstone is deleted, never with
//!   a delete that could take a newer write along: see [`Repair::Delete`].

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use futures::StreamExt;
use s4_core::storage::{ReadOptions, StorageEngine};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::sync::mpsc;
use tracing::{debug, error, warn};

use crate::cluster_metrics::record_read_repair;
use crate::hlc::HlcTimestamp;
use crate::identity::NodeId;
use crate::protocol::{CONDITIONAL_DELETE_MIN_PROTOCOL_VERSION, TOMBSTONE_MIN_PROTOCOL_VERSION};
use crate::rpc::proto;
use crate::rpc::NodeClient;

/// Maximum number of pending repair tasks in the channel.
const DEFAULT_REPAIR_QUEUE_SIZE: usize = 4096;

/// A task to bring a stale replica to the newest version of a key.
#[derive(Debug)]
pub struct RepairTask {
    /// Node that has the stale version.
    pub target_node: NodeId,
    /// Bucket name.
    pub bucket: String,
    /// Object key.
    pub key: String,
    /// HLC of the newest version, which the target is brought to.
    pub hlc: HlcTimestamp,
    /// Topology epoch at the time of the read.
    pub topology_epoch: u64,
    /// What the target gets.
    pub repair: Repair,
}

/// What a stale replica gets.
#[derive(Debug)]
pub enum Repair {
    /// Fetch exactly the selected version as a bounded stream in the worker.
    Fetch {
        /// Replica whose digest selected this version.
        source_node: NodeId,
        /// Expected ETag, paired with the task's HLC.
        etag: String,
    },
    /// The newest version is an object: the target takes its copy at the
    /// task's HLC.
    Copy {
        /// The object's bytes.
        data: Vec<u8>,
        /// The ETag of the version, which the target keeps for the copy.
        etag: String,
        /// SHA-256 content hash.
        content_hash: Vec<u8>,
        /// Content type.
        content_type: String,
        /// User metadata.
        metadata: std::collections::HashMap<String, String>,
        /// Version ID (if versioning enabled).
        version_id: Option<String>,
    },
    /// The newest version is a tombstone: the target's copy, whose HLC is
    /// `stale`, goes. A target that keeps tombstones takes the tombstone at
    /// the task's HLC. One that only deletes a generation it is told the HLC
    /// of loses exactly the stale copy, and keeps a write that landed since.
    /// One older than both is not sent anything: it would delete whatever it
    /// holds.
    Delete {
        /// HLC of the target's stale copy.
        stale: HlcTimestamp,
    },
}

/// This node's replica as a repair source and target.
struct LocalTarget {
    node: NodeId,
    /// Read as a source.
    storage: Arc<dyn StorageEngine>,
    /// Takes the repairs whose target is this node.
    write: Arc<crate::replica::ReplicaWriteHandler>,
}

/// Asynchronous read repair service.
///
/// Submit repair tasks via [`submit`](Self::submit). A background worker
/// drains the channel and sends write requests to stale replicas.
pub struct ReadRepairService {
    repair_tx: mpsc::Sender<RepairTask>,
    repairs_initiated: Arc<AtomicU64>,
    repairs_completed: Arc<AtomicU64>,
    repairs_failed: Arc<AtomicU64>,
}

impl ReadRepairService {
    /// Start the read repair service with a background worker.
    pub fn start(rpc_client: Arc<NodeClient>) -> Self {
        Self::start_with_capacity(rpc_client, DEFAULT_REPAIR_QUEUE_SIZE)
    }

    /// Start with a custom queue capacity (useful for testing).
    pub fn start_with_capacity(rpc_client: Arc<NodeClient>, capacity: usize) -> Self {
        Self::start_inner(rpc_client, capacity, None)
    }

    /// Starts repair with access to the local replica as a streaming source and target,
    /// written through a handler of its own without the node's fences.
    pub fn start_with_local(
        rpc_client: Arc<NodeClient>,
        local_node_id: NodeId,
        storage: Arc<dyn StorageEngine>,
        topology_epoch: u64,
    ) -> Self {
        let write = Arc::new(crate::replica::ReplicaWriteHandler::new(
            storage.clone(),
            Arc::new(crate::idempotency::IdempotencyTracker::new()),
            topology_epoch,
        ));
        Self::start_with_local_replica(rpc_client, local_node_id, storage, write)
    }

    /// Starts repair with access to the local replica as a streaming source,
    /// and as a target through `write` — the handler the node's gRPC server
    /// answers peers with, so that a local repair passes the node's fences
    /// as a remote one does.
    pub fn start_with_local_replica(
        rpc_client: Arc<NodeClient>,
        local_node_id: NodeId,
        storage: Arc<dyn StorageEngine>,
        write: Arc<crate::replica::ReplicaWriteHandler>,
    ) -> Self {
        Self::start_inner(
            rpc_client,
            DEFAULT_REPAIR_QUEUE_SIZE,
            Some(LocalTarget {
                node: local_node_id,
                storage,
                write,
            }),
        )
    }

    fn start_inner(
        rpc_client: Arc<NodeClient>,
        capacity: usize,
        local: Option<LocalTarget>,
    ) -> Self {
        let (tx, rx) = mpsc::channel(capacity);
        let completed = Arc::new(AtomicU64::new(0));
        let failed = Arc::new(AtomicU64::new(0));

        let worker_completed = completed.clone();
        let worker_failed = failed.clone();

        tokio::spawn(Self::worker(
            rx,
            rpc_client,
            worker_completed,
            worker_failed,
            local,
        ));

        Self {
            repair_tx: tx,
            repairs_initiated: Arc::new(AtomicU64::new(0)),
            repairs_completed: completed,
            repairs_failed: failed,
        }
    }

    /// Submit a repair task. Returns immediately without blocking.
    ///
    /// If the repair queue is full, the task is dropped and a warning is logged.
    pub fn submit(&self, task: RepairTask) {
        self.repairs_initiated.fetch_add(1, Ordering::Relaxed);

        if let Err(mpsc::error::TrySendError::Full(_)) = self.repair_tx.try_send(task) {
            // The stale replica stays stale: a repair that never ran failed.
            record_read_repair(false);
            warn!("read repair queue full, dropping repair task");
        }
    }

    /// Number of repair tasks submitted.
    pub fn repairs_initiated(&self) -> u64 {
        self.repairs_initiated.load(Ordering::Relaxed)
    }

    /// Number of repair tasks completed successfully.
    pub fn repairs_completed(&self) -> u64 {
        self.repairs_completed.load(Ordering::Relaxed)
    }

    /// Number of repair tasks that failed.
    pub fn repairs_failed(&self) -> u64 {
        self.repairs_failed.load(Ordering::Relaxed)
    }

    async fn worker(
        mut rx: mpsc::Receiver<RepairTask>,
        rpc_client: Arc<NodeClient>,
        completed: Arc<AtomicU64>,
        failed: Arc<AtomicU64>,
        local: Option<LocalTarget>,
    ) {
        while let Some(task) = rx.recv().await {
            let target = task.target_node;
            let bucket = task.bucket.clone();
            let key = task.key.clone();

            match Self::repair(&rpc_client, local.as_ref(), task).await {
                Ok(()) => {
                    completed.fetch_add(1, Ordering::Relaxed);
                    record_read_repair(true);
                    debug!(
                        target_node = %target,
                        bucket = %bucket,
                        key = %key,
                        "read repair completed"
                    );
                }
                Err(reason) => {
                    failed.fetch_add(1, Ordering::Relaxed);
                    record_read_repair(false);
                    error!(
                        target_node = %target,
                        bucket = %bucket,
                        key = %key,
                        error = %reason,
                        "read repair failed"
                    );
                }
            }
        }

        debug!("read repair worker shutting down");
    }

    /// Sends `task` to its target; `Err` says why the target is still stale.
    async fn repair(
        rpc_client: &NodeClient,
        local: Option<&LocalTarget>,
        task: RepairTask,
    ) -> Result<(), String> {
        let target = task.target_node;
        let coordinator_id = Vec::new();
        let operation_id = uuid::Uuid::new_v4().as_u128().to_be_bytes().to_vec();
        if let Repair::Fetch {
            source_node,
            ref etag,
        } = task.repair
        {
            return bounded_fetch(rpc_client.request_timeout(), async {
                let (incoming, body) =
                    open_fetch_source(rpc_client, local, &task, source_node).await?;
                let header = fetch_header(&task, etag, incoming)?;
                if let Some(local) = local.filter(|local| local.node == target) {
                    return apply_local_fetch(&local.write, header, body).await;
                }
                stream_fetch(rpc_client, task.target_node, header, body).await
            })
            .await;
        }
        let local_handler = local.filter(|local| local.node == target).map(|local| &local.write);
        match task.repair {
            Repair::Fetch { .. } => Err("fetch task was not dispatched".into()),
            Repair::Copy {
                data,
                etag,
                content_hash,
                content_type,
                metadata,
                version_id,
            } => {
                let request = proto::WriteBlobRequest {
                    verify_content: false,
                    placement_generation: 0,
                    bucket: task.bucket,
                    key: task.key,
                    data,
                    content_hash,
                    content_type,
                    metadata,
                    etag,
                    hlc: Some(task.hlc.to_proto()),
                    version_id,
                    coordinator_id,
                    operation_id,
                    topology_epoch: task.topology_epoch,
                    bucket_metadata_stage: 0,
                    repair_copy: true,
                    iam_lease: None,
                };
                match if let Some(handler) = &local_handler {
                    handler.handle_write(&request).await
                } else {
                    rpc_client.send_write(target, request).await
                } {
                    Ok(resp) if resp.success => Ok(()),
                    Ok(resp) => Err(format!("write rejected: {}", resp.error)),
                    Err(e) => Err(e.to_string()),
                }
            }
            Repair::Delete { stale } => {
                let expected_source_hlc = tombstone_delivery(
                    if local_handler.is_some() {
                        Some(crate::protocol::PROTOCOL_VERSION)
                    } else {
                        rpc_client.protocol_version(target)
                    },
                    stale,
                )?;
                let request = proto::DeleteBlobRequest {
                    placement_generation: 0,
                    bucket: task.bucket,
                    key: task.key,
                    hlc: Some(task.hlc.to_proto()),
                    coordinator_id,
                    operation_id,
                    topology_epoch: task.topology_epoch,
                    version_id: None,
                    expected_source_hlc: expected_source_hlc.map(HlcTimestamp::to_proto),
                    bucket_metadata_stage: 0,
                    repair_copy: true,
                    iam_lease: None,
                };
                match if let Some(handler) = &local_handler {
                    handler.handle_delete(&request).await
                } else {
                    rpc_client.send_delete(target, request).await
                } {
                    Ok(resp) if resp.success => Ok(()),
                    Ok(resp) => Err(format!("delete rejected: {}", resp.error)),
                    Err(e) => Err(e.to_string()),
                }
            }
        }
    }
}

fn fetch_header(
    task: &RepairTask,
    etag: &str,
    incoming: proto::ReadBlobHeader,
) -> Result<proto::WriteBlobHeader, String> {
    if incoming.hlc.as_ref().map(HlcTimestamp::from_proto) != Some(task.hlc)
        || incoming.etag != etag
    {
        return Err("repair source changed since its digest was selected".into());
    }
    if incoming.range_start.is_some()
        || incoming.range_end.is_some()
        || incoming.content_length != incoming.total_size
    {
        return Err("repair requires a complete object stream".into());
    }
    Ok(proto::WriteBlobHeader {
        bucket: task.bucket.clone(),
        key: task.key.clone(),
        etag: etag.to_string(),
        hlc: Some(task.hlc.to_proto()),
        content_hash: incoming.content_hash,
        content_type: incoming.content_type,
        metadata: incoming.metadata,
        version_id: incoming.version_id,
        operation_id: uuid::Uuid::new_v4().as_u128().to_be_bytes().to_vec(),
        topology_epoch: task.topology_epoch,
        total_size: incoming.total_size,
        repair_copy: true,
        ..Default::default()
    })
}

async fn apply_local_fetch(
    handler: &crate::replica::ReplicaWriteHandler,
    header: proto::WriteBlobHeader,
    body: Box<dyn AsyncRead + Unpin + Send>,
) -> Result<(), String> {
    // Like the existing RPC receiver, a local target assembles the body before
    // ReplicaWriteHandler applies atomic LWW and preserves the repair's ETag.
    // StorageEngine::put_object_streaming does not provide those guarantees;
    // remote forwarding remains bounded, while receivers still materialize —
    // which is why a fetch is scheduled only for objects up to
    // `READ_REPAIR_FETCH_MAX_BYTES`, and refused here above it.
    if header.total_size > crate::coordinator::read::READ_REPAIR_FETCH_MAX_BYTES {
        return Err(format!(
            "a fetch repair of {} bytes is above the {} bytes a repair assembles in memory",
            header.total_size,
            crate::coordinator::read::READ_REPAIR_FETCH_MAX_BYTES
        ));
    }
    let mut data = Vec::new();
    body.take(header.total_size.saturating_add(1))
        .read_to_end(&mut data)
        .await
        .map_err(|error| error.to_string())?;
    if data.len() as u64 != header.total_size {
        return Err("repair source body length differs from its header".into());
    }
    let response = handler
        .handle_write(&proto::WriteBlobRequest {
            verify_content: false,
            placement_generation: header.placement_generation,
            bucket: header.bucket,
            key: header.key,
            data,
            content_hash: header.content_hash,
            content_type: header.content_type,
            metadata: header.metadata,
            etag: header.etag,
            hlc: header.hlc,
            version_id: header.version_id,
            coordinator_id: header.coordinator_id,
            operation_id: header.operation_id,
            topology_epoch: header.topology_epoch,
            bucket_metadata_stage: header.bucket_metadata_stage,
            repair_copy: true,
            iam_lease: None,
        })
        .await
        .map_err(|error| error.to_string())?;
    if response.success {
        Ok(())
    } else {
        Err(format!("local repair rejected: {}", response.error))
    }
}

async fn open_fetch_source(
    rpc_client: &NodeClient,
    local: Option<&LocalTarget>,
    task: &RepairTask,
    source: NodeId,
) -> Result<(proto::ReadBlobHeader, Box<dyn AsyncRead + Unpin + Send>), String> {
    if let Some(local) = local.filter(|local| local.node == source) {
        let stream = local
            .storage
            .open_object_stream(&task.bucket, &task.key, ReadOptions::default())
            .await
            .map_err(|error| error.to_string())?;
        let hlc = HlcTimestamp::of_record(&stream.record);
        let header = proto::ReadBlobHeader {
            content_hash: stream.record.content_hash.to_vec(),
            content_type: stream.record.content_type,
            metadata: stream.record.metadata,
            hlc: Some(hlc.to_proto()),
            version_id: stream.record.version_id,
            etag: stream.record.etag,
            content_length: stream.content_length,
            total_size: stream.total_size,
            range_start: stream.content_range.map(|range| range.0),
            range_end: stream.content_range.map(|range| range.1),
        };
        return Ok((header, stream.body));
    }
    let mut stream = rpc_client
        .send_read_stream(
            source,
            proto::ReadBlobRequest {
                bucket: task.bucket.clone(),
                key: task.key.clone(),
                topology_epoch: task.topology_epoch,
                ..Default::default()
            },
        )
        .await
        .map_err(|error| error.to_string())?;
    let first = stream
        .message()
        .await
        .map_err(|error| error.to_string())?
        .ok_or("repair source sent no header")?;
    let Some(proto::read_blob_chunk::Payload::Header(header)) = first.payload else {
        return Err("repair source sent body before header".into());
    };
    let body = stream.map(|result| match result {
        Ok(proto::ReadBlobChunk {
            payload: Some(proto::read_blob_chunk::Payload::Data(data)),
        }) => Ok(bytes::Bytes::from(data)),
        Ok(_) => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "unexpected repair source payload",
        )),
        Err(error) => Err(std::io::Error::other(error.to_string())),
    });
    Ok((header, Box::new(tokio_util::io::StreamReader::new(body))))
}

async fn produce_fetch(
    tx: mpsc::Sender<proto::WriteBlobChunk>,
    header: proto::WriteBlobHeader,
    mut body: Box<dyn AsyncRead + Unpin + Send>,
) -> Result<(), String> {
    tx.send(proto::WriteBlobChunk {
        payload: Some(proto::write_blob_chunk::Payload::Header(header)),
    })
    .await
    .map_err(|_| "repair target closed stream".to_string())?;
    let mut buffer = vec![0u8; 64 * 1024];
    loop {
        match body.read(&mut buffer).await {
            Ok(0) => return Ok(()),
            Ok(count) => tx
                .send(proto::WriteBlobChunk {
                    payload: Some(proto::write_blob_chunk::Payload::Data(
                        buffer[..count].to_vec(),
                    )),
                })
                .await
                .map_err(|_| "repair target closed stream".to_string())?,
            Err(error) => {
                // Poison the stream before EOF so the receiver cannot commit a partial or invalid read.
                let _ = tx.send(proto::WriteBlobChunk { payload: None }).await;
                return Err(error.to_string());
            }
        }
    }
}

async fn stream_fetch(
    rpc_client: &NodeClient,
    target: NodeId,
    header: proto::WriteBlobHeader,
    body: Box<dyn AsyncRead + Unpin + Send>,
) -> Result<(), String> {
    let (tx, rx) = mpsc::channel(2);
    // Both futures belong to this transfer: rejection or an outer deadline drops
    // the producer and its source immediately, without a detached background task.
    tokio::try_join!(produce_fetch(tx, header, body), async {
        let response = rpc_client
            .send_write_stream(target, tokio_stream::wrappers::ReceiverStream::new(rx))
            .await
            .map_err(|error| error.to_string())?;
        if response.success {
            Ok(())
        } else {
            Err(format!("streamed repair rejected: {}", response.error))
        }
    })?;
    Ok(())
}

async fn bounded_fetch(
    timeout: std::time::Duration,
    fetch: impl std::future::Future<Output = Result<(), String>>,
) -> Result<(), String> {
    tokio::time::timeout(timeout, fetch)
        .await
        .map_err(|_| "repair fetch exceeded its request deadline".to_string())?
}

/// How a tombstone reaches a target at `protocol` that holds a stale copy at
/// `stale`: `Ok(None)` as a tombstone, `Ok(Some(generation))` as the delete of
/// exactly that generation, `Err` not at all (see [`Repair::Delete`]).
fn tombstone_delivery(
    protocol: Option<u32>,
    stale: HlcTimestamp,
) -> Result<Option<HlcTimestamp>, String> {
    match protocol {
        Some(version) if version >= TOMBSTONE_MIN_PROTOCOL_VERSION => Ok(None),
        // A copy with a made-up HLC may store none, and a delete naming the
        // made-up one would not match it.
        Some(version)
            if version >= CONDITIONAL_DELETE_MIN_PROTOCOL_VERSION && !stale.is_made_up() =>
        {
            Ok(Some(stale))
        }
        Some(version) => Err(format!(
            "the target speaks protocol {version}, which cannot delete the stale copy alone"
        )),
        None => Err("the target has not announced its protocol yet".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn handler_of(
        storage: Arc<s4_core::BitcaskStorageEngine>,
    ) -> crate::replica::ReplicaWriteHandler {
        crate::replica::ReplicaWriteHandler::new(
            storage,
            Arc::new(crate::idempotency::IdempotencyTracker::new()),
            1,
        )
    }

    struct PendingBody(
        Arc<std::sync::atomic::AtomicBool>,
        Option<Arc<tokio::sync::Notify>>,
    );

    impl AsyncRead for PendingBody {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
            _: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            if let Some(started) = &self.1 {
                started.notify_one();
            }
            std::task::Poll::Pending
        }
    }

    impl Drop for PendingBody {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn fetch_stalled_source_is_cancelled_when_target_is_unavailable() {
        use crate::gossip::NodeState;
        use crate::identity::{Edition, NodeAddr, NodeCapacity, NodeMeta, NodeStatus, PoolId};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let started = Arc::new(tokio::sync::Notify::new());
        let source_started = started.clone();
        let target = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            source_started.notified().await;
            drop(socket);
        });
        let client = NodeClient::new(
            Arc::new(crate::gossip::GossipHandle::new_for_testing(
                std::collections::HashMap::from([(
                    NodeId(99),
                    NodeState {
                        meta: NodeMeta {
                            node_id: NodeId(99),
                            node_name: "unavailable-target".into(),
                            addr: NodeAddr {
                                grpc_addr: addr,
                                http_addr: addr,
                                gossip_addr: addr,
                            },
                            pool_id: PoolId(1),
                            topology_epoch: 1,
                            protocol_version: crate::PROTOCOL_VERSION,
                            capacity: NodeCapacity::default(),
                            started_at: 0,
                            s4_version: "test".into(),
                            edition: Edition::Community,
                        },
                        status: NodeStatus::Alive,
                        last_seen: std::time::Instant::now(),
                    },
                )]),
            )),
            crate::rpc::NodeClientConfig {
                max_retries: 0,
                ..Default::default()
            },
        );
        let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            stream_fetch(
                &client,
                NodeId(99),
                proto::WriteBlobHeader::default(),
                Box::new(PendingBody(dropped.clone(), Some(started))),
            ),
        )
        .await;
        target.await.unwrap();
        assert!(result.is_ok_and(|result| result.is_err()));
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn fetch_stalled_local_source_has_a_deadline_and_releases_its_body() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = Arc::new(
            s4_core::storage::BitcaskStorageEngine::new(
                tmp.path().join("volumes"),
                tmp.path().join("metadata"),
                10 * 1024 * 1024,
                4096,
                false,
            )
            .await
            .unwrap(),
        );
        let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let result = tokio::time::timeout(
            std::time::Duration::from_millis(200),
            bounded_fetch(
                std::time::Duration::from_millis(20),
                apply_local_fetch(
                    &handler_of(storage.clone()),
                    proto::WriteBlobHeader {
                        bucket: "bucket".into(),
                        key: "stalled".into(),
                        total_size: 1,
                        ..Default::default()
                    },
                    Box::new(PendingBody(dropped.clone(), None)),
                ),
            ),
        )
        .await;
        assert!(result.is_ok_and(|result| result.is_err()));
        assert!(dropped.load(Ordering::SeqCst));
        assert!(storage.get_object("bucket", "stalled").await.is_err());
    }

    fn copy(data: &[u8]) -> Repair {
        Repair::Copy {
            data: data.to_vec(),
            etag: String::new(),
            content_hash: vec![0u8; 32],
            content_type: "text/plain".into(),
            metadata: Default::default(),
            version_id: None,
        }
    }

    #[test]
    fn fetched_header_must_match_expected_version_and_full_object() {
        let task = repair_task(NodeId(2));
        let header = proto::ReadBlobHeader {
            hlc: Some(task.hlc.to_proto()),
            etag: "chosen".into(),
            content_length: 5,
            total_size: 5,
            ..Default::default()
        };
        assert!(fetch_header(&task, "chosen", header.clone()).is_ok());
        let mut changed = header.clone();
        changed.etag = "other".into();
        assert!(fetch_header(&task, "chosen", changed).is_err());
        let mut changed = header.clone();
        changed.hlc = None;
        assert!(fetch_header(&task, "chosen", changed).is_err());
        let mut changed = header.clone();
        changed.hlc = Some(HlcTimestamp::zero().to_proto());
        assert!(fetch_header(&task, "chosen", changed).is_err());
        let mut changed = header.clone();
        changed.range_start = Some(0);
        assert!(fetch_header(&task, "chosen", changed).is_err());
        let mut changed = header;
        changed.content_length = 4;
        assert!(fetch_header(&task, "chosen", changed).is_err());
    }

    #[tokio::test]
    async fn fetch_producer_poison_prevents_commit_after_source_failure() {
        struct Broken(bool);
        impl tokio::io::AsyncRead for Broken {
            fn poll_read(
                mut self: std::pin::Pin<&mut Self>,
                _: &mut std::task::Context<'_>,
                buffer: &mut tokio::io::ReadBuf<'_>,
            ) -> std::task::Poll<std::io::Result<()>> {
                if self.0 {
                    return std::task::Poll::Ready(Err(std::io::Error::other("source corrupted")));
                }
                self.0 = true;
                buffer.put_slice(b"body");
                std::task::Poll::Ready(Ok(()))
            }
        }
        let (tx, mut rx) = mpsc::channel(2);
        let worker = tokio::spawn(produce_fetch(
            tx,
            proto::WriteBlobHeader::default(),
            Box::new(Broken(false)),
        ));
        assert!(matches!(
            rx.recv().await.unwrap().payload,
            Some(proto::write_blob_chunk::Payload::Header(_))
        ));
        assert!(matches!(
            rx.recv().await.unwrap().payload,
            Some(proto::write_blob_chunk::Payload::Data(_))
        ));
        assert!(rx.recv().await.unwrap().payload.is_none());
        assert!(worker.await.unwrap().unwrap_err().contains("source corrupted"));
    }

    #[tokio::test]
    async fn local_fetch_restores_absence_and_preserves_a_newer_local_version() {
        use s4_core::storage::BitcaskStorageEngine;
        use std::collections::HashMap;
        let tmp = tempfile::tempdir().unwrap();
        let storage = Arc::new(
            BitcaskStorageEngine::new(
                tmp.path().join("volumes"),
                tmp.path().join("metadata"),
                10 * 1024 * 1024,
                4096,
                false,
            )
            .await
            .unwrap(),
        );
        let task = repair_task(NodeId(2));
        let incoming = proto::ReadBlobHeader {
            hlc: Some(task.hlc.to_proto()),
            etag: "original-multipart-etag".into(),
            content_length: 6,
            total_size: 6,
            content_type: "text/plain".into(),
            ..Default::default()
        };
        let header = fetch_header(&task, "original-multipart-etag", incoming).unwrap();
        apply_local_fetch(
            &handler_of(storage.clone()),
            header.clone(),
            Box::new(std::io::Cursor::new(b"repair")),
        )
        .await
        .unwrap();
        let (data, record) = storage.get_object(&task.bucket, &task.key).await.unwrap();
        assert_eq!(data, b"repair");
        assert_eq!(HlcTimestamp::of_record(&record), task.hlc);
        assert_eq!(record.etag, "original-multipart-etag");
        let metadata = HashMap::from([
            ("_s4_hlc_wall".into(), "30".into()),
            ("_s4_hlc_logical".into(), "0".into()),
            ("_s4_hlc_node_id".into(), "1".into()),
        ]);
        storage
            .put_object(&task.bucket, &task.key, b"newer", "text/plain", &metadata)
            .await
            .unwrap();
        apply_local_fetch(
            &handler_of(storage.clone()),
            header.clone(),
            Box::new(std::io::Cursor::new(b"repair")),
        )
        .await
        .unwrap();
        let (data, record) = storage.get_object(&task.bucket, &task.key).await.unwrap();
        assert_eq!(data, b"newer");
        assert_eq!(HlcTimestamp::of_record(&record).wall_time, 30);
        assert!(apply_local_fetch(
            &handler_of(storage),
            header,
            Box::new(std::io::Cursor::new(b"short"))
        )
        .await
        .is_err());
    }

    #[test]
    fn repair_task_fields() {
        let task = RepairTask {
            target_node: NodeId(42),
            bucket: "test-bucket".to_string(),
            key: "file.txt".to_string(),
            hlc: HlcTimestamp::from_record(1000, 0, 0),
            topology_epoch: 1,
            repair: copy(b"hello"),
        };

        assert_eq!(task.target_node, NodeId(42));
        assert_eq!(task.bucket, "test-bucket");
        assert_eq!(task.key, "file.txt");
    }

    #[tokio::test]
    async fn metrics_track_submissions() {
        use crate::gossip::GossipHandle;
        use crate::rpc::NodeClientConfig;
        use std::collections::HashMap;

        let gossip = Arc::new(GossipHandle::new_for_testing(HashMap::new()));
        let client = Arc::new(NodeClient::new(gossip, NodeClientConfig::default()));
        let service = ReadRepairService::start_with_capacity(client, 8);

        assert_eq!(service.repairs_initiated(), 0);
        assert_eq!(service.repairs_completed(), 0);
        assert_eq!(service.repairs_failed(), 0);

        service.submit(RepairTask {
            target_node: NodeId(99),
            bucket: "b".into(),
            key: "k".into(),
            hlc: HlcTimestamp::zero(),
            topology_epoch: 1,
            repair: copy(b""),
        });

        assert_eq!(service.repairs_initiated(), 1);
    }

    fn repair_task(target: NodeId) -> RepairTask {
        RepairTask {
            target_node: target,
            bucket: "bucket".into(),
            key: "stale.txt".into(),
            hlc: HlcTimestamp::from_record(20, 0, 1),
            topology_epoch: 1,
            repair: copy(b"latest"),
        }
    }

    /// A tombstone reaches a node that keeps them as one, and a node that
    /// only deletes generations as the delete of the stale copy alone. A node
    /// older than both, or one that has not said what it speaks, gets nothing:
    /// it would delete whatever it holds.
    #[test]
    fn a_tombstone_reaches_each_target_as_much_as_it_can_take() {
        let stale = HlcTimestamp::from_record(10, 0, 7);
        let made_up = HlcTimestamp::from_record(10, 0, 0);
        assert_eq!(tombstone_delivery(Some(5), stale), Ok(None));
        assert_eq!(tombstone_delivery(Some(4), stale), Ok(None));
        assert_eq!(tombstone_delivery(Some(4), made_up), Ok(None));
        assert_eq!(tombstone_delivery(Some(3), stale), Ok(Some(stale)));
        assert_eq!(tombstone_delivery(Some(2), stale), Ok(Some(stale)));
        assert!(tombstone_delivery(Some(3), made_up).is_err());
        assert!(tombstone_delivery(Some(1), stale).is_err());
        assert!(tombstone_delivery(None, stale).is_err());
    }

    #[tokio::test]
    async fn a_repair_that_lands_is_ok_and_one_that_cannot_reach_its_replica_failed() {
        use crate::gossip::{GossipHandle, NodeState};
        use crate::identity::{Edition, NodeAddr, NodeCapacity, NodeMeta, NodeStatus, PoolId};
        use crate::replica::ReplicaWriteHandler;
        use crate::rpc::server::{NodeRpcServer, RpcServerConfig};
        use crate::rpc::NodeClientConfig;
        use std::collections::HashMap;
        use std::time::{Duration, Instant};

        let metrics = crate::test_metrics::TestMetrics::install();
        let state = |node_id: NodeId, port: u16, status: NodeStatus| NodeState {
            meta: NodeMeta {
                node_id,
                node_name: "replica".into(),
                addr: NodeAddr {
                    grpc_addr: format!("127.0.0.1:{port}").parse().unwrap(),
                    http_addr: "127.0.0.1:9000".parse().unwrap(),
                    gossip_addr: "127.0.0.1:9200".parse().unwrap(),
                },
                pool_id: PoolId::new(1),
                topology_epoch: 1,
                protocol_version: 1,
                capacity: NodeCapacity::default(),
                started_at: 0,
                s4_version: "0.1.0".into(),
                edition: Edition::Community,
            },
            status,
            last_seen: Instant::now(),
        };

        // A replica that takes the repair write into real storage.
        let tmp = tempfile::tempdir().unwrap();
        let storage = Arc::new(
            s4_core::storage::BitcaskStorageEngine::new(
                tmp.path().join("volumes"),
                tmp.path().join("metadata"),
                10 * 1024 * 1024,
                4096,
                false,
            )
            .await
            .unwrap(),
        );
        let replica = NodeId::generate();
        let replica_state = state(replica, 19182, NodeStatus::Alive);
        let server = NodeRpcServer::new(
            replica,
            Arc::new(replica_state.meta.clone()),
            Arc::new(GossipHandle::new_for_testing(HashMap::new())),
        )
        .with_replica_handler(Arc::new(ReplicaWriteHandler::new(
            storage,
            Arc::new(crate::idempotency::IdempotencyTracker::new()),
            1,
        )));
        let handle = server
            .start(RpcServerConfig {
                bind_addr: replica_state.meta.addr.grpc_addr,
                ..Default::default()
            })
            .await
            .unwrap();

        let gone = NodeId::generate();
        let gossip = GossipHandle::new_for_testing(HashMap::from([
            (replica, replica_state),
            (gone, state(gone, 19183, NodeStatus::Dead)),
        ]));
        let client = Arc::new(NodeClient::new(
            Arc::new(gossip),
            NodeClientConfig::default(),
        ));
        let service = ReadRepairService::start_with_capacity(client, 8);

        service.submit(repair_task(replica));
        service.submit(repair_task(gone));
        let deadline = Instant::now() + Duration::from_secs(10);
        while service.repairs_completed() + service.repairs_failed() < 2 {
            assert!(Instant::now() < deadline, "repairs did not finish");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let repairs = "s4_read_repairs_total";
        assert_eq!(
            metrics.value(repairs, &[("outcome", "ok")]),
            Some(1.0),
            "{}",
            metrics.render()
        );
        assert_eq!(metrics.value(repairs, &[("outcome", "failed")]), Some(1.0));
        handle.shutdown().await.unwrap();
    }
}
