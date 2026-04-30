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

//! Distributed LIST (scatter-gather) for cluster mode.
//!
//! Since each bucket is pinned to one pool, LIST sends `ListObjects` to
//! ALL nodes in the pool, merges the results (union + deduplicate by key),
//! resolves conflicts via LWW on HLC, then sorts and paginates.
//!
//! This approach ensures a complete view even when keys are distributed
//! across different replica subsets (hash ring mode with pool_size > RF).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use s4_core::StorageEngine;
use tracing::{debug, error, warn};

use crate::conflict;
use crate::error::ClusterError;
use crate::hlc::HlcTimestamp;
use crate::identity::NodeId;
use crate::placement::PlacementRouter;
use crate::rpc::proto;
use crate::rpc::NodeClient;

/// A merged object entry from distributed LIST.
#[derive(Debug, Clone)]
pub struct ListEntry {
    /// Object key.
    pub key: String,
    /// Object size in bytes.
    pub size: u64,
    /// Content type.
    pub content_type: String,
    /// ETag.
    pub etag: String,
    /// HLC timestamp.
    pub hlc: HlcTimestamp,
    /// Version ID.
    pub version_id: Option<String>,
}

/// Result of a distributed list operation.
#[derive(Debug, Clone)]
pub struct DistributedListResult {
    /// Merged, deduplicated, sorted list entries.
    pub entries: Vec<ListEntry>,
    /// Whether there are more results.
    pub is_truncated: bool,
    /// Token for fetching the next page.
    pub next_continuation_token: Option<String>,
    /// Number of nodes that responded.
    pub nodes_responded: usize,
    /// Total number of nodes queried.
    pub nodes_queried: usize,
}

/// Timeout configuration for list operations.
#[derive(Debug, Clone)]
pub struct ListTimeouts {
    /// Maximum time to wait for all node responses.
    pub scatter_timeout: Duration,
    /// Maximum time for a single node response.
    pub per_node_timeout: Duration,
}

impl Default for ListTimeouts {
    fn default() -> Self {
        Self {
            scatter_timeout: Duration::from_secs(10),
            per_node_timeout: Duration::from_secs(5),
        }
    }
}

/// Coordinator for distributed LIST operations.
pub struct DistributedListCoordinator {
    local_node_id: NodeId,
    storage: Arc<dyn StorageEngine>,
    rpc_client: Arc<NodeClient>,
    router: Arc<PlacementRouter>,
    timeouts: ListTimeouts,
    topology_epoch: u64,
}

impl DistributedListCoordinator {
    /// Create a new distributed list coordinator.
    pub fn new(
        local_node_id: NodeId,
        storage: Arc<dyn StorageEngine>,
        rpc_client: Arc<NodeClient>,
        router: Arc<PlacementRouter>,
        topology_epoch: u64,
        timeouts: ListTimeouts,
    ) -> Self {
        Self {
            local_node_id,
            storage,
            rpc_client,
            router,
            timeouts,
            topology_epoch,
        }
    }

    /// Execute a distributed LIST operation.
    ///
    /// Scatter to ALL nodes in the bucket's pool, gather results,
    /// merge/deduplicate by key with LWW resolution, sort, and paginate.
    pub async fn list(
        &self,
        bucket: &str,
        prefix: &str,
        max_keys: usize,
        continuation_token: Option<&str>,
    ) -> Result<DistributedListResult, ClusterError> {
        let pool_nodes = self.router.pool_nodes(bucket)?;

        // Single-node bypass
        if pool_nodes.len() == 1 && pool_nodes[0] == self.local_node_id {
            return self.list_single_node(bucket, prefix, max_keys, continuation_token).await;
        }

        let nodes_queried = pool_nodes.len();

        // Scatter: send ListObjects to all nodes in the pool
        let mut handles = Vec::with_capacity(pool_nodes.len());

        for node_id in pool_nodes {
            let handle = if node_id == self.local_node_id {
                let storage = self.storage.clone();
                let bucket = bucket.to_string();
                let prefix = prefix.to_string();
                let token = continuation_token.map(|s| s.to_string());
                let timeout_dur = self.timeouts.per_node_timeout;

                // Fetch more than max_keys per node because we need to merge
                let fetch_limit = max_keys.saturating_mul(2).max(1000);

                tokio::spawn(async move {
                    let result = tokio::time::timeout(timeout_dur, async {
                        if let Some(ref tok) = token {
                            storage.list_objects_after(&bucket, &prefix, tok, fetch_limit).await
                        } else {
                            storage.list_objects(&bucket, &prefix, fetch_limit).await
                        }
                    })
                    .await;

                    match result {
                        Ok(Ok(entries)) => NodeListResult::Success {
                            node_id,
                            entries: entries
                                .into_iter()
                                .map(|(key, record)| ListEntry {
                                    key,
                                    size: record.size,
                                    content_type: record.content_type,
                                    etag: record.etag,
                                    hlc: HlcTimestamp {
                                        wall_time: record.modified_at / 1_000_000, // nanos → millis
                                        logical: 0,
                                        node_id: node_id.0,
                                    },
                                    version_id: record.version_id,
                                })
                                .collect(),
                        },
                        Ok(Err(e)) => NodeListResult::Error {
                            node_id,
                            error: e.to_string(),
                        },
                        Err(_) => NodeListResult::Timeout { node_id },
                    }
                })
            } else {
                let client = self.rpc_client.clone();
                let request = proto::ListObjectsRequest {
                    bucket: bucket.to_string(),
                    prefix: prefix.to_string(),
                    delimiter: String::new(),
                    max_keys: max_keys.saturating_mul(2).max(1000) as u32,
                    continuation_token: continuation_token.unwrap_or("").to_string(),
                    topology_epoch: self.topology_epoch,
                };
                let timeout_dur = self.timeouts.per_node_timeout;

                tokio::spawn(async move {
                    let result = tokio::time::timeout(
                        timeout_dur,
                        client.send_list_objects(node_id, request),
                    )
                    .await;

                    match result {
                        Ok(Ok(resp)) if resp.error.is_empty() => NodeListResult::Success {
                            node_id,
                            entries: resp
                                .objects
                                .into_iter()
                                .map(|obj| {
                                    let hlc = obj
                                        .hlc
                                        .map(|h| HlcTimestamp::from_proto(&h))
                                        .unwrap_or_else(HlcTimestamp::zero);

                                    ListEntry {
                                        key: obj.key,
                                        size: obj.size,
                                        content_type: obj.content_type,
                                        etag: obj.etag,
                                        hlc,
                                        version_id: obj.version_id,
                                    }
                                })
                                .collect(),
                        },
                        Ok(Ok(resp)) => NodeListResult::Error {
                            node_id,
                            error: resp.error,
                        },
                        Ok(Err(e)) => NodeListResult::Error {
                            node_id,
                            error: e.to_string(),
                        },
                        Err(_) => NodeListResult::Timeout { node_id },
                    }
                })
            };

            handles.push(handle);
        }

        // Gather: collect all results with a scatter timeout
        let mut all_entries = Vec::new();
        let mut nodes_responded = 0usize;
        let mut set = tokio::task::JoinSet::new();
        for handle in handles {
            set.spawn(handle);
        }

        let deadline = tokio::time::Instant::now() + self.timeouts.scatter_timeout;

        loop {
            tokio::select! {
                maybe_result = set.join_next() => {
                    match maybe_result {
                        Some(Ok(Ok(NodeListResult::Success { node_id, entries }))) => {
                            debug!(
                                %node_id,
                                count = entries.len(),
                                "received list response from node"
                            );
                            nodes_responded += 1;
                            all_entries.extend(entries);
                        }
                        Some(Ok(Ok(NodeListResult::Error { node_id, error }))) => {
                            warn!(%node_id, %error, "node list request failed");
                        }
                        Some(Ok(Ok(NodeListResult::Timeout { node_id }))) => {
                            warn!(%node_id, "node list request timed out");
                        }
                        Some(Ok(Err(e))) => {
                            error!(error = %e, "list task panicked");
                        }
                        Some(Err(e)) => {
                            error!(error = %e, "join error in list collection");
                        }
                        None => break,
                    }
                }
                _ = tokio::time::sleep_until(deadline) => {
                    warn!(
                        responded = nodes_responded,
                        total = nodes_queried,
                        "list scatter timeout"
                    );
                    break;
                }
            }
        }

        // Merge: deduplicate by key, keep latest version (LWW by HLC)
        let merged = Self::merge_entries(all_entries);

        // Sort by key (S3 lexicographic ordering)
        let mut sorted: Vec<ListEntry> = merged.into_values().collect();
        sorted.sort_by(|a, b| a.key.cmp(&b.key));

        // Paginate
        let is_truncated = sorted.len() > max_keys;
        let entries: Vec<ListEntry> = sorted.into_iter().take(max_keys).collect();
        let next_token = if is_truncated {
            entries.last().map(|e| e.key.clone())
        } else {
            None
        };

        Ok(DistributedListResult {
            entries,
            is_truncated,
            next_continuation_token: next_token,
            nodes_responded,
            nodes_queried,
        })
    }

    /// Single-node LIST bypass.
    async fn list_single_node(
        &self,
        bucket: &str,
        prefix: &str,
        max_keys: usize,
        continuation_token: Option<&str>,
    ) -> Result<DistributedListResult, ClusterError> {
        let result = if let Some(token) = continuation_token {
            self.storage.list_objects_after(bucket, prefix, token, max_keys + 1).await
        } else {
            self.storage.list_objects(bucket, prefix, max_keys + 1).await
        };

        let objects = result.map_err(|e| ClusterError::Storage(e.to_string()))?;
        let is_truncated = objects.len() > max_keys;

        let entries: Vec<ListEntry> = objects
            .into_iter()
            .take(max_keys)
            .map(|(key, record)| ListEntry {
                key,
                size: record.size,
                content_type: record.content_type,
                etag: record.etag,
                hlc: HlcTimestamp {
                    wall_time: record.modified_at / 1_000_000, // nanos → millis
                    logical: 0,
                    node_id: self.local_node_id.0,
                },
                version_id: record.version_id,
            })
            .collect();

        let next_token = if is_truncated {
            entries.last().map(|e| e.key.clone())
        } else {
            None
        };

        Ok(DistributedListResult {
            entries,
            is_truncated,
            next_continuation_token: next_token,
            nodes_responded: 1,
            nodes_queried: 1,
        })
    }

    /// Merge entries from multiple nodes: deduplicate by key, keep latest
    /// version using LWW conflict resolution on HLC timestamps.
    fn merge_entries(entries: Vec<ListEntry>) -> HashMap<String, ListEntry> {
        let mut merged: HashMap<String, ListEntry> = HashMap::new();

        for entry in entries {
            merged
                .entry(entry.key.clone())
                .and_modify(|existing| {
                    if conflict::resolve_conflict(&entry.hlc, &existing.hlc).is_a() {
                        *existing = entry.clone();
                    }
                })
                .or_insert(entry);
        }

        merged
    }
}

/// Internal result type for per-node list responses.
#[derive(Debug)]
#[allow(dead_code)]
enum NodeListResult {
    Success {
        node_id: NodeId,
        entries: Vec<ListEntry>,
    },
    Error {
        node_id: NodeId,
        error: String,
    },
    Timeout {
        node_id: NodeId,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gossip::GossipHandle;
    use crate::placement::PlacementRouter;
    use crate::rpc::NodeClientConfig;

    fn setup_single_node() -> DistributedListCoordinator {
        let node_id = NodeId(1);
        let gossip = Arc::new(GossipHandle::new_for_testing(Default::default()));
        let rpc_client = Arc::new(NodeClient::new(gossip, NodeClientConfig::default()));
        let router = Arc::new(PlacementRouter::single_node(node_id));
        let storage = Arc::new(MockListStorage);

        DistributedListCoordinator::new(
            node_id,
            storage,
            rpc_client,
            router,
            1,
            ListTimeouts::default(),
        )
    }

    #[tokio::test]
    async fn single_node_list() {
        let coordinator = setup_single_node();
        let result = coordinator.list("any-bucket", "", 100, None).await.unwrap();
        assert_eq!(result.entries.len(), 2);
        assert_eq!(result.nodes_responded, 1);
        assert!(!result.is_truncated);
    }

    #[tokio::test]
    async fn single_node_list_pagination() {
        let coordinator = setup_single_node();
        let result = coordinator.list("any-bucket", "", 1, None).await.unwrap();
        assert_eq!(result.entries.len(), 1);
        assert!(result.is_truncated);
        assert!(result.next_continuation_token.is_some());
    }

    #[test]
    fn merge_deduplicates_by_key() {
        let e1 = ListEntry {
            key: "a.txt".into(),
            size: 10,
            content_type: "text/plain".into(),
            etag: "\"old\"".into(),
            hlc: HlcTimestamp {
                wall_time: 1000,
                logical: 0,
                node_id: 1,
            },
            version_id: None,
        };
        let e2 = ListEntry {
            key: "a.txt".into(),
            size: 20,
            content_type: "text/plain".into(),
            etag: "\"new\"".into(),
            hlc: HlcTimestamp {
                wall_time: 2000,
                logical: 0,
                node_id: 2,
            },
            version_id: None,
        };
        let e3 = ListEntry {
            key: "b.txt".into(),
            size: 30,
            content_type: "text/plain".into(),
            etag: "\"b\"".into(),
            hlc: HlcTimestamp {
                wall_time: 1500,
                logical: 0,
                node_id: 1,
            },
            version_id: None,
        };

        let merged = DistributedListCoordinator::merge_entries(vec![e1, e2, e3]);
        assert_eq!(merged.len(), 2);
        assert_eq!(merged["a.txt"].etag, "\"new\"");
        assert_eq!(merged["a.txt"].size, 20);
        assert_eq!(merged["b.txt"].etag, "\"b\"");
    }

    #[test]
    fn list_timeouts_default() {
        let t = ListTimeouts::default();
        assert_eq!(t.scatter_timeout, Duration::from_secs(10));
        assert_eq!(t.per_node_timeout, Duration::from_secs(5));
    }

    struct MockListStorage;

    #[async_trait::async_trait]
    impl StorageEngine for MockListStorage {
        async fn put_object(
            &self,
            _b: &str,
            _k: &str,
            _d: &[u8],
            _ct: &str,
            _m: &HashMap<String, String>,
        ) -> Result<String, s4_core::StorageError> {
            Ok("\"mock\"".into())
        }
        async fn get_object(
            &self,
            _b: &str,
            _k: &str,
        ) -> Result<(Vec<u8>, s4_core::IndexRecord), s4_core::StorageError> {
            Err(s4_core::StorageError::ObjectNotFound { key: "m".into() })
        }
        async fn delete_object(&self, _b: &str, _k: &str) -> Result<(), s4_core::StorageError> {
            Ok(())
        }
        async fn head_object(
            &self,
            _b: &str,
            _k: &str,
        ) -> Result<s4_core::IndexRecord, s4_core::StorageError> {
            Err(s4_core::StorageError::ObjectNotFound { key: "m".into() })
        }
        async fn list_objects(
            &self,
            _b: &str,
            _p: &str,
            _m: usize,
        ) -> Result<Vec<(String, s4_core::IndexRecord)>, s4_core::StorageError> {
            let record = s4_core::IndexRecord {
                file_id: 1,
                offset: 0,
                size: 10,
                etag: "\"e\"".to_string(),
                content_type: "text/plain".to_string(),
                metadata: HashMap::new(),
                content_hash: [0u8; 32],
                created_at: 1000,
                modified_at: 2000,
                version_id: None,
                is_delete_marker: false,
                retention_mode: None,
                retain_until_timestamp: None,
                legal_hold: false,
                layout: None,
                pool_id: 0,
                hlc_timestamp: 0,
                hlc_logical: 0,
                origin_node_id: 0,
                operation_id: 0,
            };
            Ok(vec![
                ("a.txt".to_string(), record.clone()),
                ("b.txt".to_string(), record),
            ])
        }
        async fn list_objects_after(
            &self,
            _b: &str,
            _p: &str,
            _s: &str,
            _m: usize,
        ) -> Result<Vec<(String, s4_core::IndexRecord)>, s4_core::StorageError> {
            Ok(vec![])
        }
        async fn put_object_versioned(
            &self,
            _b: &str,
            _k: &str,
            _d: &[u8],
            _ct: &str,
            _m: &HashMap<String, String>,
            _vs: s4_core::VersioningStatus,
        ) -> Result<(String, Option<String>), s4_core::StorageError> {
            Ok(("\"m\"".into(), None))
        }
        async fn get_object_version(
            &self,
            _b: &str,
            _k: &str,
            _v: &str,
        ) -> Result<(Vec<u8>, s4_core::IndexRecord), s4_core::StorageError> {
            Err(s4_core::StorageError::ObjectNotFound { key: "m".into() })
        }
        async fn delete_object_versioned(
            &self,
            _b: &str,
            _k: &str,
            _v: Option<&str>,
            _vs: s4_core::VersioningStatus,
        ) -> Result<s4_core::DeleteResult, s4_core::StorageError> {
            Ok(s4_core::DeleteResult {
                delete_marker: false,
                version_id: None,
            })
        }
        async fn head_object_version(
            &self,
            _b: &str,
            _k: &str,
            _v: &str,
        ) -> Result<s4_core::IndexRecord, s4_core::StorageError> {
            Err(s4_core::StorageError::ObjectNotFound { key: "m".into() })
        }
        async fn list_object_versions(
            &self,
            _b: &str,
            _p: &str,
            _km: Option<&str>,
            _vm: Option<&str>,
            _m: usize,
        ) -> Result<s4_core::ListVersionsResult, s4_core::StorageError> {
            Ok(Default::default())
        }
        #[allow(clippy::too_many_arguments)]
        async fn put_object_with_retention(
            &self,
            _b: &str,
            _k: &str,
            _d: &[u8],
            _ct: &str,
            _m: &HashMap<String, String>,
            _vs: s4_core::VersioningStatus,
            _dr: Option<s4_core::types::DefaultRetention>,
        ) -> Result<(String, Option<String>), s4_core::StorageError> {
            Ok(("\"m\"".into(), None))
        }
        async fn update_object_metadata(
            &self,
            _b: &str,
            _k: &str,
            _v: &str,
            _r: s4_core::IndexRecord,
        ) -> Result<(), s4_core::StorageError> {
            Ok(())
        }
        async fn open_object_stream(
            &self,
            _b: &str,
            _k: &str,
            _opts: s4_core::ReadOptions,
        ) -> Result<s4_core::ObjectStream, s4_core::StorageError> {
            Err(s4_core::StorageError::ObjectNotFound { key: "m".into() })
        }
        async fn open_object_version_stream(
            &self,
            _b: &str,
            _k: &str,
            _v: &str,
            _opts: s4_core::ReadOptions,
        ) -> Result<s4_core::ObjectStream, s4_core::StorageError> {
            Err(s4_core::StorageError::ObjectNotFound { key: "m".into() })
        }
        async fn put_object_streaming(
            &self,
            _b: &str,
            _k: &str,
            _r: Box<dyn tokio::io::AsyncRead + Unpin + Send>,
            _cl: u64,
            _ct: &str,
            _m: &HashMap<String, String>,
            _e: &str,
        ) -> Result<s4_core::StreamingPutResult, s4_core::StorageError> {
            Ok(s4_core::StreamingPutResult {
                content_hash: [0u8; 32],
                crc32: 0,
                bytes_written: 0,
                etag: "\"m\"".into(),
            })
        }
    }
}
