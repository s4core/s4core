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
//!
//! A node's entry for a key counts only when placement gives the node a
//! replica of it: a former owner's copy of a key a growth moved away is not
//! listed, as no read of the key would return it.
//!
//! Tombstones take part in the merge like any version, so a delete outranks
//! the older copy a replica that missed it still lists; a key whose newest
//! version is a tombstone is then left out. A page is filled with objects:
//! when tombstones thin it out, the scatter goes on past them.
//!
//! A listing is a read of every key under the prefix, and answers only what
//! a read quorum vouches for: every acknowledged write and delete of a key
//! is on `W` of its replicas, so with no more than `RF - R` nodes of the pool
//! silent every key still has `R` replicas answering, one of which holds its
//! last acknowledged version (`W + R > RF`). With more nodes silent some key
//! may have only replicas that missed its last write or delete, and the
//! listing would drop an object or show a deleted one; it answers
//! [`ClusterError::QuorumNotMet`] instead — `503`, a retry for the client —
//! as a read of that key would. A DeleteBucket that took such a page for an
//! empty bucket would delete one that is not.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use s4_core::storage::{VersionKind, VersionRank};
use s4_core::StorageEngine;
use tracing::{debug, error, warn};

use crate::cluster_metrics::{observe_quorum, QuorumOperation};
use crate::error::ClusterError;
use crate::hlc::HlcTimestamp;
use crate::identity::NodeId;
use crate::placement::PlacementRouter;
use crate::rpc::proto;
use crate::rpc::NodeClient;

use super::read::{QuorumHeadResult, QuorumReadCoordinator};

/// The replicas of the keys of one listing, `None` for a key placement
/// could not route.
type Owners = HashMap<String, Option<Vec<NodeId>>>;

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

impl ListEntry {
    /// The entry for a record in local storage, with the HLC a replica would
    /// report for it: entries from different nodes are merged by HLC.
    fn of_record(key: String, record: s4_core::IndexRecord) -> Self {
        Self {
            hlc: HlcTimestamp::of_record(&record),
            key,
            size: record.size,
            content_type: record.content_type,
            etag: record.etag,
            version_id: record.version_id,
        }
    }
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

/// One node's entry for a key: an object, or the tombstone of a deleted one.
#[derive(Debug, Clone)]
struct NodeEntry {
    entry: ListEntry,
    tombstone: bool,
}

impl NodeEntry {
    /// The rank of the version: a listing does not tell an erasure-coded
    /// entry from an object, which shares its HLC and ETag anyway.
    fn rank(&self) -> VersionRank {
        let kind = if self.tombstone {
            VersionKind::Tombstone
        } else {
            VersionKind::Live
        };
        VersionRank::new(self.entry.hlc.as_tuple(), kind, &self.entry.etag)
    }
}

/// What one scatter over the pool gathered.
#[derive(Debug, Default)]
struct Gathered {
    entries: Vec<NodeEntry>,
    /// Owners that returned a version of each key, rather than absence.
    holders: HashMap<String, std::collections::HashSet<NodeId>>,
    /// For every node that holds more keys than it returned, the last key it
    /// returned: past it, what that node holds is not known yet.
    truncated_after: Vec<String>,
    nodes_responded: usize,
}

/// How many scatters one page may take when tombstones keep thinning it out.
/// A page still short after that many is returned as it is, truncated, with a
/// token past every key scanned — empty, past a long enough run of
/// tombstones: S3 lets a page hold fewer keys than asked, none included. Only
/// a page that is not truncated means there are no more keys
/// ([`DistributedListCoordinator::list_up_to`] follows the pages).
const MAX_LIST_ROUNDS: usize = 16;

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
    /// The fewest entries one node is asked for in one scatter.
    min_fetch: usize,
    read_coordinator: Option<Arc<QuorumReadCoordinator>>,
    #[cfg(test)]
    after_scatter: std::sync::Mutex<Option<futures::future::BoxFuture<'static, ()>>>,
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
            min_fetch: 1000,
            read_coordinator: None,
            #[cfg(test)]
            after_scatter: std::sync::Mutex::new(None),
        }
    }

    /// Confirms candidates whose replicas' listings straddled a write or a
    /// range cut-over through this pool's quorum read coordinator. Without
    /// it, an unconfirmed candidate retains the conservative quorum error.
    pub fn with_read_coordinator(mut self, read: Arc<QuorumReadCoordinator>) -> Self {
        self.read_coordinator = Some(read);
        self
    }

    /// The same coordinator asking each node for as few as `min_fetch`
    /// entries per scatter, so that a test reaches the paging past a node's
    /// batch with a handful of keys.
    #[cfg(test)]
    fn with_min_fetch(mut self, min_fetch: usize) -> Self {
        self.min_fetch = min_fetch;
        self
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
        observe_quorum(
            QuorumOperation::List,
            self.list_inner(bucket, prefix, max_keys, continuation_token, true),
        )
        .await
    }

    /// The same listing of what the nodes that answer hold, whether or not
    /// every key kept a read quorum of them: `nodes_responded` below
    /// `nodes_queried` says the page may miss keys or show deleted ones. For
    /// a report that says it is incomplete, such as the admin API's usage,
    /// never for an answer to a client.
    pub async fn list_answered(
        &self,
        bucket: &str,
        prefix: &str,
        max_keys: usize,
        continuation_token: Option<&str>,
    ) -> Result<DistributedListResult, ClusterError> {
        observe_quorum(
            QuorumOperation::List,
            self.list_inner(bucket, prefix, max_keys, continuation_token, false),
        )
        .await
    }

    /// Up to `limit` entries of `bucket` under `prefix` from its first key,
    /// following pages as far as it takes: a page past a run of tombstones
    /// may come back empty but truncated, and only the end of the keys says
    /// there are no more.
    pub async fn list_up_to(
        &self,
        bucket: &str,
        prefix: &str,
        limit: usize,
    ) -> Result<Vec<ListEntry>, ClusterError> {
        let mut entries = Vec::new();
        let mut token: Option<String> = None;
        while entries.len() < limit {
            let page = self.list(bucket, prefix, limit - entries.len(), token.as_deref()).await?;
            entries.extend(page.entries);
            match page.next_continuation_token {
                Some(next) if page.is_truncated => token = Some(next),
                _ => break,
            }
        }
        entries.truncate(limit);
        Ok(entries)
    }

    async fn list_inner(
        &self,
        bucket: &str,
        prefix: &str,
        max_keys: usize,
        continuation_token: Option<&str>,
        require_quorum: bool,
    ) -> Result<DistributedListResult, ClusterError> {
        let pool_nodes = self.router.pool_nodes(bucket)?;
        let confirmation_deadline = tokio::time::Instant::now() + self.timeouts.scatter_timeout;

        // Single-node bypass
        if pool_nodes.len() == 1 && pool_nodes[0] == self.local_node_id {
            return self.list_single_node(bucket, prefix, max_keys, continuation_token).await;
        }

        let nodes_queried = pool_nodes.len();
        // The nodes that may stay silent while every key under the prefix
        // keeps a read quorum of answering replicas.
        let decision = self.router.route(bucket, prefix)?;
        let tolerated_silent =
            decision.read_replicas.len().saturating_sub(usize::from(decision.read_quorum));
        // Fetch more than max_keys per node because we need to merge
        let fetch_limit = max_keys.saturating_mul(2).max(self.min_fetch);

        let mut objects: Vec<ListEntry> = Vec::new();
        let mut cursor = continuation_token.map(str::to_string);
        let mut nodes_responded = nodes_queried;
        let mut unscanned = false;
        for round in 1.. {
            let gathered =
                self.scatter(bucket, prefix, cursor.as_deref(), fetch_limit, &pool_nodes).await;
            #[cfg(test)]
            {
                let step = self.after_scatter.lock().unwrap().take();
                if let Some(step) = step {
                    step.await;
                }
            }
            nodes_responded = nodes_responded.min(gathered.nodes_responded);
            if require_quorum && nodes_queried - gathered.nodes_responded > tolerated_silent {
                return Err(ClusterError::QuorumNotMet {
                    needed: u8::try_from(nodes_queried - tolerated_silent).unwrap_or(u8::MAX),
                    achieved: u8::try_from(gathered.nodes_responded).unwrap_or(u8::MAX),
                });
            }

            // Merge: deduplicate by key, keep latest version (LWW by HLC);
            // only up to the first key a node held back is every version of
            // a key known
            let horizon = gathered.truncated_after.into_iter().min();
            let mut merged: Vec<NodeEntry> = Self::merge_entries(gathered.entries)
                .into_values()
                .filter(|node_entry| {
                    horizon.as_ref().is_none_or(|horizon| node_entry.entry.key <= *horizon)
                })
                .collect();

            // Sort by key (S3 lexicographic ordering)
            merged.sort_by(|a, b| a.entry.key.cmp(&b.entry.key));
            if require_quorum {
                let mut confirmed = 0;
                for node_entry in merged.iter_mut().filter(|entry| !entry.tombstone) {
                    if confirmed >= max_keys.saturating_sub(objects.len()) {
                        break;
                    }
                    let decision = self.router.route(bucket, &node_entry.entry.key)?;
                    let holding = gathered.holders.get(&node_entry.entry.key).map_or(0, |nodes| {
                        decision.read_replicas.iter().filter(|node| nodes.contains(node)).count()
                    });
                    if holding < usize::from(decision.read_quorum) {
                        let unconfirmed = ClusterError::QuorumNotMet {
                            needed: decision.read_quorum,
                            achieved: u8::try_from(holding).unwrap_or(u8::MAX),
                        };
                        let Some(read) = &self.read_coordinator else {
                            return Err(unconfirmed);
                        };
                        // LIST responses are separate snapshots. A write may
                        // have reached a quorum after early replicas listed,
                        // or placement may have cut over since their replies.
                        // Reuse HEAD's version/absence rules, including its
                        // refusal of lone legacy copies; this is part of LIST
                        // and does not count an extra client HEAD operation.
                        match self
                            .confirm_candidate(
                                read,
                                bucket,
                                &node_entry.entry.key,
                                confirmation_deadline,
                            )
                            .await
                        {
                            Ok(head) => {
                                node_entry.entry.size = head.size;
                                node_entry.entry.content_type = head.content_type;
                                node_entry.entry.etag = head.etag;
                                node_entry.entry.hlc = head.hlc;
                                node_entry.entry.version_id = head.version_id;
                            }
                            Err(ClusterError::ObjectNotFound { .. }) => {
                                node_entry.tombstone = true;
                                continue;
                            }
                            Err(error) => return Err(error),
                        }
                    }
                    confirmed += 1;
                }
            }
            objects.extend(
                merged
                    .into_iter()
                    .filter(|node_entry| !node_entry.tombstone)
                    .map(|node_entry| node_entry.entry),
            );

            unscanned = horizon.is_some();
            // A page cut short by the round cap may hold no key at all: past
            // a run of tombstones it is empty and truncated, and its token
            // goes on from the last key scanned, as S3 allows.
            let page_done =
                objects.len() > max_keys || horizon.is_none() || round >= MAX_LIST_ROUNDS;
            // The next batch, or the next page, starts past the keys this
            // batch scanned
            cursor = horizon;
            if page_done {
                break;
            }
        }

        // Paginate
        let is_truncated = objects.len() > max_keys || unscanned;
        let next_token = if objects.len() > max_keys {
            match max_keys.checked_sub(1) {
                Some(last) => objects.get(last).map(|entry| entry.key.clone()),
                // A page of no keys moves the listing nowhere
                None => continuation_token.map(str::to_string),
            }
        } else if unscanned {
            cursor
        } else {
            None
        };
        objects.truncate(max_keys);

        Ok(DistributedListResult {
            entries: objects,
            is_truncated,
            next_continuation_token: next_token,
            nodes_responded,
            nodes_queried,
        })
    }

    /// A sparse candidate can be a PUT still reaching its quorum. Retry only
    /// that ambiguity, with fresh placement each time and one deadline shared
    /// by all candidates and scatters; no attempt can reset the LIST budget.
    async fn confirm_candidate(
        &self,
        read: &QuorumReadCoordinator,
        bucket: &str,
        key: &str,
        deadline: tokio::time::Instant,
    ) -> Result<QuorumHeadResult, ClusterError> {
        let mut backoff = Duration::from_millis(10);
        let mut last_quorum_error = None;
        loop {
            if tokio::time::Instant::now() >= deadline {
                return Err(last_quorum_error
                    .unwrap_or(ClusterError::ReadTimeout(self.timeouts.scatter_timeout)));
            }
            match tokio::time::timeout_at(deadline, read.head_inner(bucket, key)).await {
                Ok(Err(error @ ClusterError::QuorumNotMet { .. })) => {
                    last_quorum_error = Some(error);
                    tokio::time::sleep_until((tokio::time::Instant::now() + backoff).min(deadline))
                        .await;
                    backoff = (backoff * 2).min(Duration::from_millis(50));
                }
                Ok(result) => return result,
                Err(_) => return Err(ClusterError::ReadTimeout(self.timeouts.scatter_timeout)),
            }
        }
    }

    /// Asks every node of `pool_nodes` for up to `fetch_limit` entries of
    /// `bucket` under `prefix` after `start_after`, tombstones included.
    async fn scatter(
        &self,
        bucket: &str,
        prefix: &str,
        start_after: Option<&str>,
        fetch_limit: usize,
        pool_nodes: &[NodeId],
    ) -> Gathered {
        let nodes_queried = pool_nodes.len();

        // Scatter: send ListObjects to all nodes in the pool
        let mut handles = Vec::with_capacity(pool_nodes.len());

        for &node_id in pool_nodes {
            let handle = if node_id == self.local_node_id {
                let storage = self.storage.clone();
                let bucket = bucket.to_string();
                let prefix = prefix.to_string();
                let start_after = start_after.map(str::to_string);
                let timeout_dur = self.timeouts.per_node_timeout;

                tokio::spawn(async move {
                    let result = tokio::time::timeout(
                        timeout_dur,
                        storage.list_objects_with_tombstones(
                            &bucket,
                            &prefix,
                            start_after.as_deref(),
                            fetch_limit + 1,
                        ),
                    )
                    .await;

                    match result {
                        Ok(Ok(mut entries)) => {
                            let truncated = entries.len() > fetch_limit;
                            entries.truncate(fetch_limit);
                            NodeListResult::Success {
                                node_id,
                                truncated_after: entries
                                    .last()
                                    .filter(|_| truncated)
                                    .map(|(key, _)| key.clone()),
                                entries: entries
                                    .into_iter()
                                    .map(|(key, record)| NodeEntry {
                                        tombstone: record.is_tombstone(),
                                        entry: ListEntry::of_record(key, record),
                                    })
                                    .collect(),
                            }
                        }
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
                    max_keys: fetch_limit as u32,
                    continuation_token: start_after.unwrap_or("").to_string(),
                    topology_epoch: self.topology_epoch,
                    include_tombstones: true,
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
                            truncated_after: resp
                                .objects
                                .last()
                                .filter(|_| resp.is_truncated)
                                .map(|obj| obj.key.clone()),
                            entries: resp
                                .objects
                                .into_iter()
                                .map(|obj| {
                                    let hlc = obj
                                        .hlc
                                        .map(|h| HlcTimestamp::from_proto(&h))
                                        .unwrap_or_else(HlcTimestamp::zero);

                                    NodeEntry {
                                        tombstone: obj.tombstone,
                                        entry: ListEntry {
                                            key: obj.key,
                                            size: obj.size,
                                            content_type: obj.content_type,
                                            etag: obj.etag,
                                            hlc,
                                            version_id: obj.version_id,
                                        },
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
        let mut gathered = Gathered::default();
        let mut set = tokio::task::JoinSet::new();
        for handle in handles {
            set.spawn(handle);
        }

        let deadline = tokio::time::Instant::now() + self.timeouts.scatter_timeout;
        // The replicas of each key, placed once for every node's answer.
        let mut owners = Owners::new();

        loop {
            tokio::select! {
                maybe_result = set.join_next() => {
                    match maybe_result {
                        Some(Ok(Ok(NodeListResult::Success {
                            node_id,
                            mut entries,
                            truncated_after,
                        }))) => {
                            debug!(
                                %node_id,
                                count = entries.len(),
                                "received list response from node"
                            );
                            gathered.nodes_responded += 1;
                            entries.retain(|node_entry| {
                                self.places(&mut owners, node_id, bucket, &node_entry.entry.key)
                            });
                            for entry in &entries {
                                gathered.holders.entry(entry.entry.key.clone()).or_default().insert(node_id);
                            }
                            gathered.entries.extend(entries);
                            gathered.truncated_after.extend(truncated_after);
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
                        responded = gathered.nodes_responded,
                        total = nodes_queried,
                        "list scatter timeout"
                    );
                    break;
                }
            }
        }

        gathered
    }

    /// Whether placement gives `node` a replica of `bucket/key` now — in a
    /// growing pool, by either generation. Only such a node's entry for the
    /// key is weighed: a former owner keeps its copies of the keys a growth
    /// moved away until its clean-up, and a copy of a key deleted since the
    /// growth settled — whose tombstone the owners may have collected — would
    /// otherwise list a deleted object, which no read of the key returns.
    ///
    /// A key's replicas are placed once per listing and kept in `owners`:
    /// every node answers with much the same keys.
    fn places(&self, owners: &mut Owners, node: NodeId, bucket: &str, key: &str) -> bool {
        let replicas = match owners.get(key) {
            Some(replicas) => replicas,
            None => owners.entry(key.to_string()).or_insert_with(|| {
                self.router.route(bucket, key).ok().map(|decision| decision.replicas)
            }),
        };
        replicas.as_ref().is_none_or(|replicas| replicas.contains(&node))
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
            .map(|(key, record)| ListEntry::of_record(key, record))
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

    /// Merge entries from multiple nodes: deduplicate by key, keep the
    /// version ranked highest ([`VersionRank`]), a tombstone included, as a
    /// quorum read picks it.
    fn merge_entries(entries: Vec<NodeEntry>) -> HashMap<String, NodeEntry> {
        let mut merged: HashMap<String, NodeEntry> = HashMap::new();

        for node_entry in entries {
            merged
                .entry(node_entry.entry.key.clone())
                .and_modify(|existing| {
                    if node_entry.rank() > existing.rank() {
                        *existing = node_entry.clone();
                    }
                })
                .or_insert(node_entry);
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
        entries: Vec<NodeEntry>,
        /// The last key returned, when the node holds more.
        truncated_after: Option<String>,
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

    /// Entries from different nodes are merged by HLC, so a local entry
    /// carries the HLC a replica would report for it: the one it was written
    /// with, not the time this node stored it; for a record written without
    /// one, the made-up HLC with node 0.
    #[tokio::test]
    async fn a_local_entry_carries_the_hlc_its_record_was_written_with() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Arc::new(
            s4_core::storage::BitcaskStorageEngine::new(
                dir.path().join("volumes"),
                dir.path().join("metadata"),
                10 * 1024 * 1024,
                4096,
                false,
            )
            .await
            .unwrap(),
        );
        let issued = HlcTimestamp::from_record(5, 3, 7);
        let metadata = HashMap::from([
            ("_s4_hlc_wall".to_string(), issued.wall_time.to_string()),
            ("_s4_hlc_logical".to_string(), issued.logical.to_string()),
            ("_s4_hlc_node_id".to_string(), issued.node_id.to_string()),
        ]);
        storage
            .put_object("bucket", "issued", b"a", "text/plain", &metadata)
            .await
            .unwrap();
        storage
            .put_object("bucket", "plain", b"b", "text/plain", &HashMap::new())
            .await
            .unwrap();
        let plain = storage.head_object("bucket", "plain").await.unwrap();
        let gossip = Arc::new(GossipHandle::new_for_testing(Default::default()));
        let coordinator = DistributedListCoordinator::new(
            NodeId(1),
            storage,
            Arc::new(NodeClient::new(gossip, NodeClientConfig::default())),
            Arc::new(PlacementRouter::single_node(NodeId(1))),
            1,
            ListTimeouts::default(),
        );

        let result = coordinator.list("bucket", "", 100, None).await.unwrap();
        let hlc_of =
            |key: &str| result.entries.iter().find(|entry| entry.key == key).map(|entry| entry.hlc);
        assert_eq!(hlc_of("issued"), Some(issued));
        assert_eq!(
            hlc_of("plain"),
            Some(HlcTimestamp::from_record(
                plain.modified_at / 1_000_000,
                0,
                0
            ))
        );
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

        let object = |entry: ListEntry| NodeEntry {
            entry,
            tombstone: false,
        };
        let merged =
            DistributedListCoordinator::merge_entries(vec![object(e1), object(e2), object(e3)]);
        assert_eq!(merged.len(), 2);
        assert_eq!(merged["a.txt"].entry.etag, "\"new\"");
        assert_eq!(merged["a.txt"].entry.size, 20);
        assert_eq!(merged["b.txt"].entry.etag, "\"b\"");
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

    #[tokio::test]
    async fn a_listing_is_one_list_operation() {
        let metrics = crate::test_metrics::TestMetrics::install();
        let coordinator = setup_single_node();

        coordinator.list("bucket", "", 10, None).await.expect("listing");

        assert_eq!(
            metrics.value(
                "s4_quorum_operations_total",
                &[
                    ("operation", "list"),
                    ("outcome", "ok"),
                    ("origin", "system")
                ]
            ),
            Some(1.0)
        );
    }

    // ---- A deleted key, as the listing shows it ----

    /// A listing merges the entries of every node of the pool by HLC. A key
    /// whose delete reached two replicas of three, or one, has the delete as
    /// its newest version and is not listed; the keys around it are. Without
    /// a trace of the delete a replica that took it has no entry, and the old
    /// copy of the replica that missed it is listed as if the object were
    /// there.
    #[tokio::test]
    async fn a_key_deleted_on_some_replicas_is_not_listed() {
        use crate::coordinator::test_pool::{at, Replica, TestPool, BUCKET};

        let pool = TestPool::start(Replica::Up, Replica::Up, 21008).await;
        for key in ["a-live", "b-kept-by-one", "c-kept-by-two", "d-live"] {
            for node in 1..=3 {
                pool.put(node, key, b"version 10", at(10)).await;
            }
        }
        // Nodes 1 and 2 took the delete, node 3 kept its copy.
        for node in [1, 2] {
            pool.delete(node, "b-kept-by-one", at(20)).await;
        }
        // Node 3 took the delete, nodes 1 and 2 kept their copies.
        pool.delete(3, "c-kept-by-two", at(20)).await;

        let listed = pool.list_coordinator().list(BUCKET, "", 100, None).await.unwrap();
        let keys: Vec<&str> = listed.entries.iter().map(|entry| entry.key.as_str()).collect();
        assert_eq!(keys, ["a-live", "d-live"]);
        assert!(!listed.is_truncated);
        assert_eq!(listed.nodes_responded, 3);
    }

    /// With one node of three down every key keeps two replicas answering,
    /// a read quorum, and the listing is complete. With two down a key may
    /// have only a replica that missed its last write or delete: the listing
    /// is refused as a read of that key would be, rather than miss an object
    /// or show a deleted one.
    #[tokio::test]
    async fn a_listing_needs_a_read_quorum_of_every_key() {
        use crate::coordinator::test_pool::{at, Replica, TestPool, BUCKET};

        let one_down = TestPool::start(Replica::Up, Replica::Down, 21084).await;
        for node in [1, 2] {
            one_down.put(node, "kept", b"version 10", at(10)).await;
        }
        let listed = one_down.list_coordinator().list(BUCKET, "", 100, None).await.unwrap();
        let keys: Vec<&str> = listed.entries.iter().map(|entry| entry.key.as_str()).collect();
        assert_eq!(keys, ["kept"]);
        assert_eq!(listed.nodes_responded, 2);

        let two_down = TestPool::start(Replica::Down, Replica::Silent, 21080).await;
        two_down.put(1, "written-to-others", b"version 10", at(10)).await;
        match two_down.list_coordinator().list(BUCKET, "", 100, None).await {
            Err(ClusterError::QuorumNotMet {
                needed: 2,
                achieved: 1,
            }) => {}
            other => panic!("a listing with two of three nodes silent: {other:?}"),
        }
    }

    /// The first scatter can straddle a new write: an early replica did not
    /// list it yet, while a later one did. Once the write reaches its quorum,
    /// fresh metadata confirms it without weakening the quorum requirement.
    #[tokio::test]
    async fn a_listing_confirms_a_write_that_straddled_its_replica_snapshots() {
        use crate::coordinator::test_pool::{at, Replica, TestPool, BUCKET};

        let pool = Arc::new(TestPool::start(Replica::Up, Replica::Up, 21120).await);
        for node in 1..=3 {
            pool.put(node, "a-kept", b"confirmed", at(10)).await;
        }
        pool.put(1, "b-arriving", b"new", at(20)).await;
        let coordinator = pool.list_coordinator();
        let arriving = pool.clone();
        *coordinator.after_scatter.lock().unwrap() = Some(Box::pin(async move {
            arriving.put(2, "b-arriving", b"new", at(20)).await;
        }));
        let page = coordinator.list(BUCKET, "", 10, None).await.unwrap();
        let keys: Vec<_> = page.entries.iter().map(|entry| entry.key.as_str()).collect();
        assert_eq!(keys, ["a-kept", "b-arriving"]);
        assert_eq!(page.entries[1].hlc, at(20));
    }

    /// Seeing a new key before its PUT finished must wait for confirmation
    /// within the existing request budget rather than fail the healthy LIST.
    #[tokio::test]
    async fn a_listing_waits_for_an_in_flight_write_to_reach_its_quorum() {
        use crate::coordinator::test_pool::{at, Replica, TestPool, BUCKET};

        let pool = Arc::new(TestPool::start(Replica::Up, Replica::Up, 21136).await);
        pool.put(1, "arriving", b"new", at(20)).await;
        let coordinator = pool.list_coordinator();
        let (started, start_write) = tokio::sync::oneshot::channel();
        let writing = pool.clone();
        let write = tokio::spawn(async move {
            start_write.await.unwrap();
            tokio::time::sleep(Duration::from_millis(200)).await;
            writing.put(2, "arriving", b"new", at(20)).await;
        });
        *coordinator.after_scatter.lock().unwrap() = Some(Box::pin(async move {
            started.send(()).unwrap();
        }));
        let result = coordinator.list(BUCKET, "", 10, None).await;
        write.await.unwrap();
        let page = result.unwrap();
        assert_eq!(page.entries.len(), 1);
        assert_eq!(page.entries[0].key, "arriving");
        assert_eq!(page.entries[0].hlc, at(20));
    }

    /// A delete that finishes between the scatter and confirmation must not
    /// turn its old candidate into an error or leave a hole in the page.
    #[tokio::test]
    async fn a_listing_confirms_a_delete_and_fills_the_page_with_the_next_key() {
        use crate::coordinator::test_pool::{at, Replica, TestPool, BUCKET};

        let pool = Arc::new(TestPool::start(Replica::Up, Replica::Up, 21124).await);
        pool.put(1, "a-deleting", b"old", at(10)).await;
        for node in 1..=3 {
            pool.put(node, "b-kept", b"confirmed", at(20)).await;
        }
        let coordinator = pool.list_coordinator().with_min_fetch(1);
        let deleting = pool.clone();
        *coordinator.after_scatter.lock().unwrap() = Some(Box::pin(async move {
            for node in [2, 3] {
                deleting.delete(node, "a-deleting", at(30)).await;
            }
        }));
        let page = coordinator.list(BUCKET, "", 1, None).await.unwrap();
        assert_eq!(page.entries.len(), 1);
        assert_eq!(page.entries[0].key, "b-kept");
        assert!(!page.is_truncated);
    }

    /// Placement can cut over after LIST took the old replicas' snapshots.
    #[tokio::test]
    async fn a_listing_confirms_metadata_after_placement_cuts_over() {
        use crate::coordinator::test_pool::{at, hlc_metadata, Replica, TestMove, TestPool};
        use crate::metadata_placement::{QuorumShape, Stage};

        let pool = Arc::new(
            TestPool::start_moving(
                Replica::Up,
                Replica::Up,
                21132,
                TestMove {
                    from: QuorumShape {
                        replication_factor: 1,
                        write_quorum: 1,
                        read_quorum: 1,
                    },
                    to: QuorumShape {
                        replication_factor: 3,
                        write_quorum: 2,
                        read_quorum: 2,
                    },
                    stages: [Stage::DualWrite; 3],
                },
            )
            .await,
        );
        let key = "__s4_bucket_cors_cutover";
        pool.storage(1)
            .put_object(
                "__system__",
                key,
                b"old",
                "text/plain",
                &hlc_metadata(at(10)),
            )
            .await
            .unwrap();
        let coordinator = pool.list_coordinator();
        let moving = pool.clone();
        *coordinator.after_scatter.lock().unwrap() = Some(Box::pin(async move {
            for node in [2, 3] {
                moving
                    .storage(node)
                    .put_object(
                        "__system__",
                        key,
                        b"new metadata",
                        "application/json",
                        &hlc_metadata(at(20)),
                    )
                    .await
                    .unwrap();
            }
            for node in 1..=3 {
                moving.stage_cell(node).advance_to(Stage::ReadNew).await;
            }
        }));
        let page = coordinator.list("__system__", "", 10, None).await.unwrap();
        assert_eq!(page.entries.len(), 1);
        let expected = pool.storage(2).head_object("__system__", key).await.unwrap();
        assert_eq!(page.entries[0].key, key);
        assert_eq!(page.entries[0].hlc, at(20));
        assert_eq!(page.entries[0].size, expected.size);
        assert_eq!(page.entries[0].etag, expected.etag);
        assert_eq!(page.entries[0].content_type, expected.content_type);
    }

    /// Sparse confirmation cannot add HEAD's own timeout to the LIST budget.
    #[tokio::test]
    async fn a_listing_confirmation_keeps_the_original_scatter_deadline() {
        use crate::coordinator::test_pool::{at, Replica, TestPool, BUCKET};

        let pool = TestPool::start(Replica::Up, Replica::Silent, 21128).await;
        pool.put(1, "unconfirmed", b"one copy", at(10)).await;
        pool.rpc_client.send_health(crate::NodeId(2)).await.unwrap();
        let mut coordinator = pool.list_coordinator();
        coordinator.timeouts = ListTimeouts {
            scatter_timeout: Duration::from_millis(1800),
            per_node_timeout: Duration::from_millis(1200),
        };
        // The silent LIST consumes 1.2 s before HEAD starts. Replacing the
        // original 1.8 s deadline with a fresh one would take 3 s and must
        // fail the outer limit, with 0.6 s scheduling margin on either side.
        let result = tokio::time::timeout(
            Duration::from_millis(2400),
            coordinator.list(BUCKET, "", 10, None),
        )
        .await
        .unwrap();
        assert!(
            matches!(result, Err(ClusterError::ReadTimeout(timeout)) if timeout == Duration::from_millis(1800)),
            "{result:?}"
        );
    }

    /// Answering with absence cannot confirm an object: a legacy DELETE
    /// left no tombstone. LIST must refuse the same lone copy as GET/HEAD.
    #[tokio::test]
    async fn a_listing_refuses_a_lone_legacy_copy_even_when_all_nodes_answer() {
        use crate::coordinator::test_pool::{at, Replica, TestPool, BUCKET};

        let pool = TestPool::start(Replica::Up, Replica::Up, 21112).await;
        for node in 1..=3 {
            pool.put(node, "a-kept", b"confirmed", at(10)).await;
        }
        pool.put(3, "b-legacy-copy", b"deleted elsewhere", at(10)).await;
        let coordinator = pool.list_coordinator().with_min_fetch(1);
        for prefix in ["", "b-"] {
            assert!(matches!(
                coordinator.list(BUCKET, prefix, 100, None).await,
                Err(ClusterError::QuorumNotMet {
                    needed: 2,
                    achieved: 1
                })
            ));
        }
        // An explicitly incomplete administrative report may still show it.
        let report = coordinator.list_answered(BUCKET, "", 10, None).await.unwrap();
        assert_eq!(report.entries.len(), 2);
        // Two different held versions suffice, exactly as for quorum reads.
        pool.put(2, "b-legacy-copy", b"newer", at(20)).await;
        let page = coordinator.list(BUCKET, "b-", 1, None).await.unwrap();
        assert_eq!(page.entries[0].hlc, at(20));
    }

    #[tokio::test]
    async fn a_lone_copy_beyond_a_page_does_not_block_confirmed_entries() {
        use crate::coordinator::test_pool::{at, Replica, TestPool, BUCKET};

        let pool = TestPool::start(Replica::Up, Replica::Up, 21116).await;
        for node in 1..=3 {
            pool.put(node, "a-kept", b"confirmed", at(10)).await;
        }
        pool.put(3, "b-legacy-copy", b"deleted elsewhere", at(10)).await;
        let coordinator = pool.list_coordinator().with_min_fetch(1);
        let first = coordinator.list(BUCKET, "", 1, None).await.unwrap();
        assert_eq!(first.entries[0].key, "a-kept");
        assert!(first.is_truncated);
        assert_eq!(first.next_continuation_token.as_deref(), Some("a-kept"));
        let next = coordinator.list(BUCKET, "", 1, first.next_continuation_token.as_deref()).await;
        assert!(
            matches!(
                next,
                Err(ClusterError::QuorumNotMet {
                    needed: 2,
                    achieved: 1
                })
            ),
            "{next:?}"
        );
        let empty = coordinator.list(BUCKET, "", 0, None).await.unwrap();
        assert!(empty.entries.is_empty() && empty.is_truncated);
    }

    /// A page cut short by `MAX_LIST_ROUNDS` ends at the last key it scanned:
    /// its token lies past the objects of the last batch, so the next page
    /// does not list them again.
    #[tokio::test]
    async fn a_page_cut_short_by_the_round_cap_lists_no_key_twice() {
        use crate::coordinator::test_pool::{at, Replica, TestPool, BUCKET};

        let pool = TestPool::start(Replica::Up, Replica::Up, 21052).await;
        let keys: Vec<String> = (0..70).map(|i| format!("k{i:02}")).collect();
        for key in &keys {
            for node in 1..=3 {
                pool.put(node, key, b"v", at(10)).await;
            }
        }
        // Batches of four keys: the first holds k00, the next fourteen only
        // tombstones, and the sixteenth, the last one a page may take, only
        // k60 — so the page stops on the cap, not on being full.
        let deleted: Vec<&String> = keys[1..60].iter().chain(&keys[61..64]).collect();
        for key in &deleted {
            pool.delete(2, key, at(20)).await;
        }
        let coordinator = pool.list_coordinator().with_min_fetch(1);

        let first = coordinator.list(BUCKET, "", 2, None).await.unwrap();
        let page: Vec<&str> = first.entries.iter().map(|entry| entry.key.as_str()).collect();
        assert_eq!(page, ["k00", "k60"]);
        assert!(first.is_truncated);

        let mut listed = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let page = coordinator.list(BUCKET, "", 2, token.as_deref()).await.unwrap();
            listed.extend(page.entries.into_iter().map(|entry| entry.key));
            if !page.is_truncated {
                break;
            }
            token = page.next_continuation_token;
        }
        let objects: Vec<&String> = keys.iter().filter(|key| !deleted.contains(key)).collect();
        assert_eq!(listed.iter().collect::<Vec<_>>(), objects);
    }

    /// A page of no keys says whether there are any, and does not fail.
    #[tokio::test]
    async fn a_page_of_no_keys_says_whether_there_are_any() {
        use crate::coordinator::test_pool::{at, Replica, TestPool, BUCKET};

        let pool = TestPool::start(Replica::Up, Replica::Up, 21056).await;
        let coordinator = pool.list_coordinator();
        let empty = coordinator.list(BUCKET, "", 0, None).await.unwrap();
        assert!(empty.entries.is_empty() && !empty.is_truncated, "{empty:?}");

        for node in 1..=3 {
            pool.put(node, "a", b"v", at(10)).await;
        }
        let page = coordinator.list(BUCKET, "", 0, Some("0")).await.unwrap();
        assert!(page.entries.is_empty());
        assert!(page.is_truncated);
        assert_eq!(page.next_continuation_token.as_deref(), Some("0"));
    }

    /// A page is filled with objects however many tombstones lie between
    /// them: the scatter goes on past each node's batch until it has the
    /// page, and a page still short after `MAX_LIST_ROUNDS` batches comes
    /// back truncated, with a token past the keys it scanned — empty, when
    /// the run of tombstones is longer than that. One request never scans
    /// without bound. Following the tokens lists exactly the objects, each
    /// once and in order. Each node is asked for twice the page per batch
    /// here, not a thousand keys.
    #[tokio::test]
    async fn pages_are_filled_with_objects_past_runs_of_tombstones() {
        use crate::coordinator::test_pool::{at, Replica, TestPool, BUCKET};

        let pool = TestPool::start(Replica::Up, Replica::Up, 21028).await;
        let keys: Vec<String> = (0..60).map(|i| format!("k{i:02}")).collect();
        for key in &keys {
            for node in 1..=3 {
                pool.put(node, key, b"v", at(10)).await;
            }
        }
        // A run of forty keys deleted on node 2 alone, longer than the
        // batches of one page reach; two more deleted on one node each.
        let deleted: Vec<String> = (1..=40).map(|i| format!("k{i:02}")).collect();
        for key in &deleted {
            pool.delete(2, key, at(20)).await;
        }
        pool.delete(1, "k45", at(20)).await;
        pool.delete(3, "k50", at(20)).await;
        let objects: Vec<&str> = keys
            .iter()
            .map(String::as_str)
            .filter(|key| !deleted.iter().any(|d| d == key) && *key != "k45" && *key != "k50")
            .collect();
        let coordinator = pool.list_coordinator().with_min_fetch(1);

        // Forty tombstones in a row take twenty batches of two, past the
        // cap: the page stops at the cap empty and truncated, its token past
        // the 32 keys scanned, and the next page reaches the object after
        // them. A prefix with nothing but tombstones is an empty, final page.
        let past_the_run = coordinator.list(BUCKET, "", 1, Some("k00")).await.unwrap();
        assert!(past_the_run.entries.is_empty(), "{past_the_run:?}");
        assert!(past_the_run.is_truncated);
        assert_eq!(past_the_run.next_continuation_token.as_deref(), Some("k32"));
        let next = coordinator.list(BUCKET, "", 1, Some("k32")).await.unwrap();
        let page: Vec<&str> = next.entries.iter().map(|entry| entry.key.as_str()).collect();
        assert_eq!(page, ["k41"]);
        let first_object = coordinator.list_up_to(BUCKET, "k", 1).await.unwrap();
        assert_eq!(first_object.len(), 1);
        let after_k00 = coordinator.list_up_to(BUCKET, "k0", 10).await.unwrap();
        let keys_after: Vec<&str> = after_k00.iter().map(|entry| entry.key.as_str()).collect();
        assert_eq!(keys_after, ["k00"]);
        let only_tombstones = coordinator.list(BUCKET, "k1", 1, None).await.unwrap();
        assert!(
            only_tombstones.entries.is_empty() && !only_tombstones.is_truncated,
            "{only_tombstones:?}"
        );

        let first = coordinator.list(BUCKET, "", 2, None).await.unwrap();
        let page: Vec<&str> = first.entries.iter().map(|entry| entry.key.as_str()).collect();
        assert_eq!(page, ["k00", "k41"]);
        assert!(first.is_truncated);
        assert_eq!(first.next_continuation_token.as_deref(), Some("k41"));

        for max_keys in [1, 2, 7] {
            let mut listed = Vec::new();
            let mut token: Option<String> = None;
            loop {
                let page = coordinator.list(BUCKET, "", max_keys, token.as_deref()).await.unwrap();
                assert!(page.entries.len() <= max_keys);
                listed.extend(page.entries.into_iter().map(|entry| entry.key));
                if !page.is_truncated {
                    break;
                }
                token = page.next_continuation_token;
                assert!(token.is_some(), "a truncated page without a token");
            }
            assert_eq!(listed, objects, "pages of {max_keys}");
        }

        // With the first key deleted too, the first page of one key is empty
        // and truncated; following the pages finds the first object.
        pool.delete(2, "k00", at(20)).await;
        let first = coordinator.list(BUCKET, "", 1, None).await.unwrap();
        assert!(first.entries.is_empty() && first.is_truncated, "{first:?}");
        let found = coordinator.list_up_to(BUCKET, "", 1).await.unwrap();
        let found: Vec<&str> = found.iter().map(|entry| entry.key.as_str()).collect();
        assert_eq!(found, ["k41"]);
    }
}
