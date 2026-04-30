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

//! Hinted handoff for temporarily offline replicas.
//!
//! When a coordinator cannot reach a replica during a quorum write, it
//! stores a **hint** locally. When the replica comes back (detected via
//! gossip), the coordinator delivers the hint so the replica can catch up
//! without waiting for full anti-entropy.
//!
//! # Design
//!
//! - Hints are persisted to disk (fsync) before the coordinator returns 200.
//! - Each hint has a TTL (default: 3 hours). Expired hints are garbage
//!   collected. Anti-entropy (Phase 7) handles data that outlives the TTL.
//! - Per-target size limit (default: 1 GB) prevents unbounded growth.
//! - Hints are best-effort, NOT a correctness mechanism. Correctness
//!   is guaranteed by anti-entropy.
//!
//! # Storage Format
//!
//! Hints are stored as individual files in `{data_dir}/hints/{target_node_id}/`.
//! Each file is named `{operation_id}.hint` and contains bincode-serialized
//! [`Hint`] data.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tracing::{debug, error, info, warn};

use crate::error::ClusterError;
use crate::gossip::GossipHandle;
use crate::identity::NodeId;
use crate::rpc::proto;
use crate::rpc::NodeClient;

/// Default hint TTL: 3 hours.
const DEFAULT_HINT_TTL: Duration = Duration::from_secs(3 * 3600);

/// Default maximum hints size per target node: 1 GB.
const DEFAULT_MAX_HINTS_SIZE: u64 = 1024 * 1024 * 1024;

/// Delivery check interval: how often to scan for nodes that came back online.
const DELIVERY_CHECK_INTERVAL: Duration = Duration::from_secs(30);

/// A single hinted write or delete operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Hint {
    /// The node that should have received this write.
    pub target_node: u128,
    /// The operation to replay.
    pub operation: HintOperation,
    /// When this hint was created (Unix milliseconds).
    pub created_at: u64,
    /// TTL after which this hint should be discarded.
    pub ttl_ms: u64,
}

/// The payload of a hint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HintOperation {
    /// A write that needs to be replayed.
    Write {
        /// Bucket name.
        bucket: String,
        /// Object key.
        key: String,
        /// Raw object data.
        data: Vec<u8>,
        /// SHA-256 content hash.
        content_hash: Vec<u8>,
        /// MIME type.
        content_type: String,
        /// User metadata.
        metadata: std::collections::HashMap<String, String>,
        /// Operation ID for idempotency.
        operation_id: Vec<u8>,
        /// Optional version ID.
        version_id: Option<String>,
        /// Topology epoch at the time of the write.
        topology_epoch: u64,
    },
    /// A delete that needs to be replayed.
    Delete {
        /// Bucket name.
        bucket: String,
        /// Object key.
        key: String,
        /// Operation ID for idempotency.
        operation_id: Vec<u8>,
        /// Optional version ID.
        version_id: Option<String>,
        /// Topology epoch at the time of the delete.
        topology_epoch: u64,
    },
}

/// Configuration for the hinted handoff subsystem.
#[derive(Debug, Clone)]
pub struct HintedHandoffConfig {
    /// Directory where hints are stored.
    pub hints_dir: PathBuf,
    /// TTL for each hint (default: 3 hours).
    pub hint_ttl: Duration,
    /// Maximum total hints size per target node (default: 1 GB).
    pub max_hints_size: u64,
}

impl HintedHandoffConfig {
    /// Create a config with the given data directory.
    /// Hints will be stored in `{data_dir}/hints/`.
    pub fn new(data_dir: impl AsRef<Path>) -> Self {
        Self {
            hints_dir: data_dir.as_ref().join("hints"),
            hint_ttl: DEFAULT_HINT_TTL,
            max_hints_size: DEFAULT_MAX_HINTS_SIZE,
        }
    }
}

/// Manages hinted handoff storage and delivery.
pub struct HintedHandoffManager {
    config: HintedHandoffConfig,
    gossip: Arc<GossipHandle>,
    rpc_client: Arc<NodeClient>,
}

impl HintedHandoffManager {
    /// Create a new manager.
    pub fn new(
        config: HintedHandoffConfig,
        gossip: Arc<GossipHandle>,
        rpc_client: Arc<NodeClient>,
    ) -> Self {
        Self {
            config,
            gossip,
            rpc_client,
        }
    }

    /// Store a hint for a target node that was unreachable during a write.
    ///
    /// The hint is persisted to disk synchronously (fsync) before returning.
    pub async fn store_hint(&self, hint: Hint) -> Result<(), ClusterError> {
        let target_dir = self.target_dir(NodeId(hint.target_node));
        tokio::fs::create_dir_all(&target_dir).await?;

        // Check size limit
        let current_size = dir_size(&target_dir).await;
        if current_size >= self.config.max_hints_size {
            warn!(
                target_node = %NodeId(hint.target_node),
                current_size,
                limit = self.config.max_hints_size,
                "hint storage full for target, dropping hint"
            );
            return Ok(());
        }

        // Extract operation_id for filename
        let op_id = match &hint.operation {
            HintOperation::Write { operation_id, .. }
            | HintOperation::Delete { operation_id, .. } => hex_encode(operation_id),
        };

        let hint_path = target_dir.join(format!("{op_id}.hint"));
        let data = bincode::serialize(&hint).map_err(|e| ClusterError::Codec(e.to_string()))?;

        // Write and fsync using a single file handle to avoid Windows locking issues
        {
            use tokio::io::AsyncWriteExt;
            let mut file = tokio::fs::File::create(&hint_path).await?;
            file.write_all(&data).await?;
            file.sync_all().await?;
        }

        debug!(
            target_node = %NodeId(hint.target_node),
            path = %hint_path.display(),
            bytes = data.len(),
            "stored hint"
        );

        Ok(())
    }

    /// Start the background hint delivery loop.
    ///
    /// Periodically checks for target nodes that have come back online
    /// and delivers pending hints to them.
    ///
    /// Returns a shutdown sender. Drop or send `true` to stop the loop.
    pub fn start_delivery_loop(self: Arc<Self>) -> watch::Sender<bool> {
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);

        tokio::spawn(async move {
            info!("hinted handoff delivery loop started");

            loop {
                tokio::select! {
                    _ = tokio::time::sleep(DELIVERY_CHECK_INTERVAL) => {
                        if let Err(e) = self.deliver_pending_hints().await {
                            error!(error = %e, "hint delivery error");
                        }
                        if let Err(e) = self.gc_expired_hints().await {
                            error!(error = %e, "hint GC error");
                        }
                    }
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            info!("hinted handoff delivery loop stopping");
                            break;
                        }
                    }
                }
            }
        });

        shutdown_tx
    }

    /// Deliver all pending hints for nodes that are now alive.
    async fn deliver_pending_hints(&self) -> Result<(), ClusterError> {
        if !self.config.hints_dir.exists() {
            return Ok(());
        }

        let mut entries = tokio::fs::read_dir(&self.config.hints_dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            if !entry.file_type().await?.is_dir() {
                continue;
            }

            let dir_name = entry.file_name();
            let target_node = match dir_name.to_str().and_then(|s| s.parse::<u128>().ok()) {
                Some(id) => NodeId(id),
                None => continue,
            };

            // Only deliver to nodes that are alive
            if !self.is_node_alive(target_node) {
                continue;
            }

            self.deliver_hints_for_node(target_node, &entry.path()).await;
        }

        Ok(())
    }

    /// Deliver all hints for a specific node.
    async fn deliver_hints_for_node(&self, target_node: NodeId, dir: &Path) {
        let Ok(mut entries) = tokio::fs::read_dir(dir).await else {
            return;
        };

        let mut delivered = 0u64;
        let mut failed = 0u64;

        while let Ok(Some(entry)) = entries.next_entry().await {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("hint") {
                continue;
            }

            match self.deliver_single_hint(target_node, &path).await {
                Ok(true) => {
                    let _ = tokio::fs::remove_file(&path).await;
                    delivered += 1;
                }
                Ok(false) => {
                    // Hint expired or invalid, remove it
                    let _ = tokio::fs::remove_file(&path).await;
                }
                Err(e) => {
                    debug!(
                        target_node = %target_node,
                        path = %path.display(),
                        error = %e,
                        "failed to deliver hint, will retry"
                    );
                    failed += 1;
                }
            }
        }

        if delivered > 0 || failed > 0 {
            info!(
                target_node = %target_node,
                delivered,
                failed,
                "hint delivery batch complete"
            );
        }
    }

    /// Deliver a single hint file.
    ///
    /// Returns `Ok(true)` if delivered successfully, `Ok(false)` if the
    /// hint is expired/invalid and should be removed, or `Err` on
    /// transient failure (keep for retry).
    async fn deliver_single_hint(
        &self,
        target_node: NodeId,
        path: &Path,
    ) -> Result<bool, ClusterError> {
        let data = tokio::fs::read(path).await?;
        let hint: Hint =
            bincode::deserialize(&data).map_err(|e| ClusterError::Codec(e.to_string()))?;

        // Check TTL
        let now = now_millis();
        if now.saturating_sub(hint.created_at) > hint.ttl_ms {
            debug!(
                target_node = %target_node,
                path = %path.display(),
                "hint expired, removing"
            );
            return Ok(false);
        }

        // Convert to gRPC request and send
        let request = hint_to_request(&hint);
        let response = self.rpc_client.send_hint(target_node, request).await?;

        if response.accepted {
            Ok(true)
        } else {
            warn!(
                target_node = %target_node,
                error = %response.error,
                "hint rejected by target"
            );
            Ok(false)
        }
    }

    /// Remove expired hint files.
    async fn gc_expired_hints(&self) -> Result<(), ClusterError> {
        if !self.config.hints_dir.exists() {
            return Ok(());
        }

        let now = now_millis();
        let mut entries = tokio::fs::read_dir(&self.config.hints_dir).await?;

        while let Some(entry) = entries.next_entry().await? {
            if !entry.file_type().await?.is_dir() {
                continue;
            }

            let mut sub_entries = tokio::fs::read_dir(entry.path()).await?;
            let mut removed = 0u64;

            while let Ok(Some(sub_entry)) = sub_entries.next_entry().await {
                let path = sub_entry.path();
                if path.extension().and_then(|e| e.to_str()) != Some("hint") {
                    continue;
                }

                if let Ok(data) = tokio::fs::read(&path).await {
                    if let Ok(hint) = bincode::deserialize::<Hint>(&data) {
                        if now.saturating_sub(hint.created_at) > hint.ttl_ms {
                            let _ = tokio::fs::remove_file(&path).await;
                            removed += 1;
                        }
                    }
                }
            }

            if removed > 0 {
                debug!(
                    dir = %entry.path().display(),
                    removed,
                    "expired hints removed"
                );
            }

            // Remove empty target directories
            if is_dir_empty(&entry.path()).await {
                let _ = tokio::fs::remove_dir(&entry.path()).await;
            }
        }

        Ok(())
    }

    fn is_node_alive(&self, node_id: NodeId) -> bool {
        self.gossip
            .get_node(&node_id)
            .map(|state| matches!(state.status, crate::identity::NodeStatus::Alive))
            .unwrap_or(false)
    }

    fn target_dir(&self, target_node: NodeId) -> PathBuf {
        self.config.hints_dir.join(target_node.0.to_string())
    }
}

/// Convert a `Hint` to a `HintRequest` for gRPC delivery.
fn hint_to_request(hint: &Hint) -> proto::HintRequest {
    match &hint.operation {
        HintOperation::Write {
            bucket,
            key,
            data,
            content_hash,
            content_type,
            metadata,
            operation_id,
            version_id,
            ..
        } => proto::HintRequest {
            target_node_id: hint.target_node.to_be_bytes().to_vec(),
            bucket: bucket.clone(),
            key: key.clone(),
            data: data.clone(),
            content_hash: content_hash.clone(),
            hlc: None,
            operation_id: operation_id.clone(),
            version_id: version_id.clone(),
            metadata: metadata.clone(),
            content_type: content_type.clone(),
        },
        HintOperation::Delete {
            bucket,
            key,
            operation_id,
            ..
        } => proto::HintRequest {
            target_node_id: hint.target_node.to_be_bytes().to_vec(),
            bucket: bucket.clone(),
            key: key.clone(),
            data: Vec::new(),
            content_hash: Vec::new(),
            hlc: None,
            operation_id: operation_id.clone(),
            version_id: None,
            metadata: Default::default(),
            content_type: String::new(),
        },
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

async fn dir_size(dir: &Path) -> u64 {
    let mut total = 0u64;
    if let Ok(mut entries) = tokio::fs::read_dir(dir).await {
        while let Ok(Some(entry)) = entries.next_entry().await {
            if let Ok(meta) = entry.metadata().await {
                total += meta.len();
            }
        }
    }
    total
}

async fn is_dir_empty(dir: &Path) -> bool {
    match tokio::fs::read_dir(dir).await {
        Ok(mut entries) => entries.next_entry().await.ok().flatten().is_none(),
        Err(_) => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hint_serialization_roundtrip() {
        let hint = Hint {
            target_node: 42,
            operation: HintOperation::Write {
                bucket: "test-bucket".into(),
                key: "test-key".into(),
                data: b"hello world".to_vec(),
                content_hash: vec![0u8; 32],
                content_type: "text/plain".into(),
                metadata: Default::default(),
                operation_id: 123u128.to_be_bytes().to_vec(),
                version_id: None,
                topology_epoch: 1,
            },
            created_at: 1700000000000,
            ttl_ms: DEFAULT_HINT_TTL.as_millis() as u64,
        };

        let bytes = bincode::serialize(&hint).unwrap();
        let deserialized: Hint = bincode::deserialize(&bytes).unwrap();
        assert_eq!(deserialized.target_node, 42);
        assert_eq!(deserialized.created_at, 1700000000000);
    }

    #[test]
    fn hint_to_request_conversion() {
        let hint = Hint {
            target_node: 42,
            operation: HintOperation::Write {
                bucket: "b".into(),
                key: "k".into(),
                data: b"data".to_vec(),
                content_hash: vec![1u8; 32],
                content_type: "application/octet-stream".into(),
                metadata: Default::default(),
                operation_id: 99u128.to_be_bytes().to_vec(),
                version_id: Some("v1".into()),
                topology_epoch: 1,
            },
            created_at: 0,
            ttl_ms: 0,
        };

        let req = hint_to_request(&hint);
        assert_eq!(req.bucket, "b");
        assert_eq!(req.key, "k");
        assert_eq!(req.data, b"data");
        assert_eq!(req.version_id, Some("v1".into()));
    }

    #[tokio::test]
    async fn store_and_read_hint() {
        let tmp = tempfile::tempdir().unwrap();
        let config = HintedHandoffConfig {
            hints_dir: tmp.path().join("hints"),
            hint_ttl: DEFAULT_HINT_TTL,
            max_hints_size: DEFAULT_MAX_HINTS_SIZE,
        };

        let gossip = Arc::new(GossipHandle::new_for_testing(Default::default()));
        let rpc_client = Arc::new(NodeClient::new(
            gossip.clone(),
            crate::rpc::NodeClientConfig::default(),
        ));

        let manager = HintedHandoffManager::new(config, gossip, rpc_client);

        let hint = Hint {
            target_node: 42,
            operation: HintOperation::Write {
                bucket: "b".into(),
                key: "k".into(),
                data: b"test".to_vec(),
                content_hash: vec![0u8; 32],
                content_type: "text/plain".into(),
                metadata: Default::default(),
                operation_id: 1u128.to_be_bytes().to_vec(),
                version_id: None,
                topology_epoch: 1,
            },
            created_at: now_millis(),
            ttl_ms: DEFAULT_HINT_TTL.as_millis() as u64,
        };

        manager.store_hint(hint).await.unwrap();

        // Verify the file was created
        let target_dir = tmp.path().join("hints").join("42");
        assert!(target_dir.exists());
        let mut entries = tokio::fs::read_dir(&target_dir).await.unwrap();
        let entry = entries.next_entry().await.unwrap().unwrap();
        assert!(entry.path().extension().unwrap() == "hint");
    }

    #[tokio::test]
    async fn gc_removes_expired_hints() {
        let tmp = tempfile::tempdir().unwrap();
        let config = HintedHandoffConfig {
            hints_dir: tmp.path().join("hints"),
            hint_ttl: Duration::from_millis(1), // Very short TTL
            max_hints_size: DEFAULT_MAX_HINTS_SIZE,
        };

        let gossip = Arc::new(GossipHandle::new_for_testing(Default::default()));
        let rpc_client = Arc::new(NodeClient::new(
            gossip.clone(),
            crate::rpc::NodeClientConfig::default(),
        ));

        let manager = HintedHandoffManager::new(config, gossip, rpc_client);

        // Store a hint that will expire immediately
        let hint = Hint {
            target_node: 42,
            operation: HintOperation::Write {
                bucket: "b".into(),
                key: "k".into(),
                data: b"test".to_vec(),
                content_hash: vec![0u8; 32],
                content_type: "text/plain".into(),
                metadata: Default::default(),
                operation_id: 1u128.to_be_bytes().to_vec(),
                version_id: None,
                topology_epoch: 1,
            },
            created_at: 0, // Very old
            ttl_ms: 1,     // 1ms TTL
        };

        manager.store_hint(hint).await.unwrap();

        // Wait a bit and GC
        tokio::time::sleep(Duration::from_millis(10)).await;
        manager.gc_expired_hints().await.unwrap();

        // Verify the hint was removed
        let target_dir = tmp.path().join("hints").join("42");
        assert!(!target_dir.exists() || is_dir_empty(&target_dir).await);
    }
}
