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

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::sync::mpsc;
use tracing::{debug, error, warn};

use crate::identity::NodeId;
use crate::rpc::proto;
use crate::rpc::NodeClient;

/// Maximum number of pending repair tasks in the channel.
const DEFAULT_REPAIR_QUEUE_SIZE: usize = 4096;

/// A task to repair a stale replica by writing the latest version to it.
#[derive(Debug)]
pub struct RepairTask {
    /// Node that has the stale version.
    pub target_node: NodeId,
    /// Bucket name.
    pub bucket: String,
    /// Object key.
    pub key: String,
    /// Latest data to write.
    pub data: Vec<u8>,
    /// SHA-256 content hash.
    pub content_hash: Vec<u8>,
    /// Content type.
    pub content_type: String,
    /// User metadata.
    pub metadata: std::collections::HashMap<String, String>,
    /// HLC timestamp of the latest version.
    pub hlc: proto::HybridTimestamp,
    /// Version ID (if versioning enabled).
    pub version_id: Option<String>,
    /// Topology epoch at the time of the read.
    pub topology_epoch: u64,
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
    ) {
        while let Some(task) = rx.recv().await {
            let target = task.target_node;
            let bucket = task.bucket.clone();
            let key = task.key.clone();

            let coordinator_id = Vec::new();
            let operation_id = uuid::Uuid::new_v4().as_u128().to_be_bytes().to_vec();

            let request = proto::WriteBlobRequest {
                bucket: task.bucket,
                key: task.key,
                data: task.data,
                content_hash: task.content_hash,
                content_type: task.content_type,
                metadata: task.metadata,
                hlc: Some(task.hlc),
                version_id: task.version_id,
                coordinator_id,
                operation_id,
                topology_epoch: task.topology_epoch,
            };

            match rpc_client.send_write(target, request).await {
                Ok(resp) if resp.success => {
                    completed.fetch_add(1, Ordering::Relaxed);
                    debug!(
                        target_node = %target,
                        bucket = %bucket,
                        key = %key,
                        "read repair completed"
                    );
                }
                Ok(resp) => {
                    failed.fetch_add(1, Ordering::Relaxed);
                    warn!(
                        target_node = %target,
                        bucket = %bucket,
                        key = %key,
                        error = %resp.error,
                        "read repair write rejected"
                    );
                }
                Err(e) => {
                    failed.fetch_add(1, Ordering::Relaxed);
                    error!(
                        target_node = %target,
                        bucket = %bucket,
                        key = %key,
                        error = %e,
                        "read repair failed"
                    );
                }
            }
        }

        debug!("read repair worker shutting down");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repair_task_fields() {
        let task = RepairTask {
            target_node: NodeId(42),
            bucket: "test-bucket".to_string(),
            key: "file.txt".to_string(),
            data: b"hello".to_vec(),
            content_hash: vec![0u8; 32],
            content_type: "text/plain".to_string(),
            metadata: Default::default(),
            hlc: proto::HybridTimestamp {
                wall_time: 1000,
                logical: 0,
                node_id: Vec::new(),
            },
            version_id: None,
            topology_epoch: 1,
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
            data: vec![],
            content_hash: vec![],
            content_type: String::new(),
            metadata: Default::default(),
            hlc: proto::HybridTimestamp {
                wall_time: 0,
                logical: 0,
                node_id: Vec::new(),
            },
            version_id: None,
            topology_epoch: 1,
        });

        assert_eq!(service.repairs_initiated(), 1);
    }
}
