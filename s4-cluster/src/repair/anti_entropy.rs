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

//! Background anti-entropy service for maintaining replica consistency.
//!
//! Two-level repair strategy:
//!
//! 1. **Active repair** (read repair, Phase 5) — fixes stale replicas on
//!    every quorum read. Handled by [`super::ReadRepairService`].
//!
//! 2. **Background anti-entropy** (this module, Phase 7) — periodic Merkle
//!    tree exchange with random replica peers. Discovers and repairs
//!    divergences that read repair cannot catch (e.g., keys that are
//!    never read, or replicas that missed writes while offline).
//!
//! # Frequency
//!
//! Every `T = 10 minutes`, each node compares its Merkle tree with one
//! random replica peer per key range. With this interval, 99.9% of
//! divergences are detected within one hour.
//!
//! # Repair Flow
//!
//! ```text
//! for each key_range this node owns:
//!   1. Pick a random replica peer
//!   2. Exchange Merkle root hashes
//!   3. If roots match → range is consistent, skip
//!   4. Walk tree top-down to find divergent leaf ranges
//!   5. Request keys in divergent ranges via RepairSync RPC
//!   6. Apply LWW conflict resolution
//!   7. Write winning version to the lagging replica
//!   8. Advance repair frontier
//! ```

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use s4_core::StorageEngine;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use crate::clock::hlc::HlcTimestamp;
use crate::config::AntiEntropyConfig;
use crate::error::ClusterError;
use crate::gossip::GossipHandle;
use crate::identity::{NodeId, NodeStatus, PoolId};
use crate::placement;
use crate::repair::frontier::RepairFrontier;
use crate::repair::merkle::{KeyRange, MerkleTree};
use crate::rpc::proto;
use crate::rpc::NodeClient;

/// Metrics collected by the anti-entropy service.
#[derive(Debug, Default)]
pub struct AntiEntropyMetrics {
    /// Number of Merkle exchange rounds completed.
    pub rounds_completed: AtomicU64,
    /// Total divergent keys discovered across all rounds.
    pub divergent_keys_found: AtomicU64,
    /// Total keys successfully repaired.
    pub repairs_completed: AtomicU64,
    /// Total repair attempts that failed.
    pub repairs_failed: AtomicU64,
    /// Total duration of all repair rounds in milliseconds.
    pub total_repair_duration_ms: AtomicU64,
}

impl AntiEntropyMetrics {
    /// Create a new metrics instance.
    pub fn new() -> Self {
        Self::default()
    }

    /// Snapshot the current metrics.
    pub fn snapshot(&self) -> AntiEntropySnapshot {
        AntiEntropySnapshot {
            rounds_completed: self.rounds_completed.load(Ordering::Relaxed),
            divergent_keys_found: self.divergent_keys_found.load(Ordering::Relaxed),
            repairs_completed: self.repairs_completed.load(Ordering::Relaxed),
            repairs_failed: self.repairs_failed.load(Ordering::Relaxed),
            total_repair_duration_ms: self.total_repair_duration_ms.load(Ordering::Relaxed),
        }
    }
}

/// Point-in-time snapshot of anti-entropy metrics.
#[derive(Debug, Clone, Default)]
pub struct AntiEntropySnapshot {
    /// Number of Merkle exchange rounds completed.
    pub rounds_completed: u64,
    /// Total divergent keys discovered.
    pub divergent_keys_found: u64,
    /// Total keys successfully repaired.
    pub repairs_completed: u64,
    /// Total repair failures.
    pub repairs_failed: u64,
    /// Total repair duration in milliseconds.
    pub total_repair_duration_ms: u64,
}

/// Maps ring position → list of (bucket, key) pairs at that position.
/// BTreeMap enables efficient range queries for divergent Merkle ranges.
type KeyRegistry = BTreeMap<u64, Vec<(String, String)>>;

/// Background anti-entropy service.
///
/// Manages Merkle trees for each key range this node is responsible for,
/// and periodically exchanges digests with replica peers to discover and
/// repair divergences.
///
/// The service maintains a `KeyRegistry` — a BTreeMap from ring position
/// to `(bucket, key)` pairs ��� that enables efficient range lookups when
/// resolving divergent Merkle tree ranges to actual object keys.
pub struct AntiEntropyService {
    config: AntiEntropyConfig,
    local_node_id: NodeId,
    pool_id: PoolId,
    gossip: Arc<GossipHandle>,
    rpc_client: Arc<NodeClient>,
    storage: Arc<dyn StorageEngine>,
    merkle_trees: Arc<RwLock<HashMap<u64, MerkleTree>>>,
    key_registry: Arc<RwLock<KeyRegistry>>,
    repair_frontier: Arc<RwLock<RepairFrontier>>,
    metrics: Arc<AntiEntropyMetrics>,
    pool_nodes: Vec<NodeId>,
}

impl AntiEntropyService {
    /// Create a new anti-entropy service.
    pub fn new(
        config: AntiEntropyConfig,
        local_node_id: NodeId,
        pool_id: PoolId,
        pool_nodes: Vec<NodeId>,
        gossip: Arc<GossipHandle>,
        rpc_client: Arc<NodeClient>,
        storage: Arc<dyn StorageEngine>,
    ) -> Self {
        Self {
            config,
            local_node_id,
            pool_id,
            gossip,
            rpc_client,
            storage,
            merkle_trees: Arc::new(RwLock::new(HashMap::new())),
            key_registry: Arc::new(RwLock::new(BTreeMap::new())),
            repair_frontier: Arc::new(RwLock::new(RepairFrontier::new())),
            metrics: Arc::new(AntiEntropyMetrics::new()),
            pool_nodes,
        }
    }

    /// Get a reference to the metrics.
    pub fn metrics(&self) -> &Arc<AntiEntropyMetrics> {
        &self.metrics
    }

    /// Get a reference to the repair frontier.
    pub fn repair_frontier(&self) -> &Arc<RwLock<RepairFrontier>> {
        &self.repair_frontier
    }

    /// Get a reference to the Merkle trees.
    pub fn merkle_trees(&self) -> &Arc<RwLock<HashMap<u64, MerkleTree>>> {
        &self.merkle_trees
    }

    /// Scan local storage and build the initial Merkle tree + key registry.
    ///
    /// Must be called once at startup, after the storage engine is ready.
    /// Enumerates all buckets (via `__system__` markers) and all objects
    /// within each bucket to populate the Merkle tree and the ring
    /// position → (bucket, key) registry.
    pub async fn init_from_storage(&self) {
        let mut entries: Vec<(u64, [u8; 32])> = Vec::new();
        let mut registry: KeyRegistry = BTreeMap::new();

        // Discover buckets from __system__ markers.
        let markers = self
            .storage
            .list_objects("__system__", "__s4_bucket_marker_", 10_000)
            .await
            .unwrap_or_default();

        let bucket_names: Vec<String> = markers
            .iter()
            .filter_map(|(key, _)| key.strip_prefix("__s4_bucket_marker_").map(String::from))
            .collect();

        let mut total_keys = 0u64;

        for bucket in &bucket_names {
            let mut start_after = String::new();
            loop {
                let batch = if start_after.is_empty() {
                    self.storage.list_objects(bucket, "", 1000).await
                } else {
                    self.storage.list_objects_after(bucket, "", &start_after, 1000).await
                };

                let objects = match batch {
                    Ok(objs) => objs,
                    Err(e) => {
                        warn!(bucket = %bucket, error = %e, "failed to list objects for merkle init");
                        break;
                    }
                };

                if objects.is_empty() {
                    break;
                }

                for (key, record) in &objects {
                    let ring_pos = placement::ring_position(bucket, key);
                    entries.push((ring_pos, record.content_hash));
                    registry.entry(ring_pos).or_default().push((bucket.clone(), key.clone()));
                    total_keys += 1;
                }

                start_after = objects.last().map(|(k, _)| k.clone()).unwrap_or_default();

                if objects.len() < 1000 {
                    break;
                }
            }
        }

        // Build Merkle tree for the full ring range.
        let range = KeyRange::full();
        let tree = MerkleTree::build(&entries, range, self.config.merkle_depth);
        let mut trees = self.merkle_trees.write().await;
        trees.insert(range.start, tree);
        drop(trees);

        *self.key_registry.write().await = registry;

        info!(
            buckets = bucket_names.len(),
            total_keys, "anti-entropy: merkle tree and key registry initialized from storage"
        );
    }

    /// Register a key in the key registry and update the Merkle tree.
    ///
    /// Called by `ReplicaWriteHandler` after every successful write.
    pub async fn register_key(&self, bucket: &str, key: &str, content_hash: [u8; 32]) {
        let ring_pos = placement::ring_position(bucket, key);
        let range = KeyRange::full();

        // Update key registry.
        {
            let mut registry = self.key_registry.write().await;
            let entries = registry.entry(ring_pos).or_default();
            // Avoid duplicates — replace if exists, otherwise insert.
            if !entries.iter().any(|(b, k)| b == bucket && k == key) {
                entries.push((bucket.to_string(), key.to_string()));
            }
        }

        // Update Merkle tree.
        let mut trees = self.merkle_trees.write().await;
        if let Some(tree) = trees.get_mut(&range.start) {
            tree.insert(ring_pos, content_hash);
        }
    }

    /// Unregister a key from the key registry and update the Merkle tree.
    ///
    /// Called by `ReplicaWriteHandler` after every successful delete.
    pub async fn unregister_key(&self, bucket: &str, key: &str, content_hash: [u8; 32]) {
        let ring_pos = placement::ring_position(bucket, key);
        let range = KeyRange::full();

        // Update key registry.
        {
            let mut registry = self.key_registry.write().await;
            if let Some(entries) = registry.get_mut(&ring_pos) {
                entries.retain(|(b, k)| !(b == bucket && k == key));
                if entries.is_empty() {
                    registry.remove(&ring_pos);
                }
            }
        }

        // Update Merkle tree.
        let mut trees = self.merkle_trees.write().await;
        if let Some(tree) = trees.get_mut(&range.start) {
            tree.remove(ring_pos, content_hash);
        }
    }

    /// Initialize a Merkle tree for a key range from existing entries.
    pub async fn init_tree(&self, key_range: KeyRange, entries: &[(u64, [u8; 32])]) {
        let tree = MerkleTree::build(entries, key_range, self.config.merkle_depth);
        let mut trees = self.merkle_trees.write().await;
        trees.insert(key_range.start, tree);
        debug!(
            range_start = key_range.start,
            range_end = key_range.end,
            entries = entries.len(),
            "merkle tree initialized"
        );
    }

    /// Update the Merkle tree incrementally when a key is written.
    pub async fn on_key_write(
        &self,
        key_range: &KeyRange,
        ring_position: u64,
        old_hash: [u8; 32],
        new_hash: [u8; 32],
    ) {
        let mut trees = self.merkle_trees.write().await;
        if let Some(tree) = trees.get_mut(&key_range.start) {
            tree.update(ring_position, old_hash, new_hash);
        }
    }

    /// Update the Merkle tree when a new key is inserted (no old hash).
    pub async fn on_key_insert(&self, key_range: &KeyRange, ring_position: u64, hash: [u8; 32]) {
        let mut trees = self.merkle_trees.write().await;
        if let Some(tree) = trees.get_mut(&key_range.start) {
            tree.insert(ring_position, hash);
        }
    }

    /// Update the Merkle tree when a key is deleted.
    pub async fn on_key_delete(&self, key_range: &KeyRange, ring_position: u64, hash: [u8; 32]) {
        let mut trees = self.merkle_trees.write().await;
        if let Some(tree) = trees.get_mut(&key_range.start) {
            tree.remove(ring_position, hash);
        }
    }

    /// Look up actual `"bucket/key"` strings for ring positions in the given ranges.
    ///
    /// Uses the BTreeMap key registry for efficient range queries.
    fn keys_in_ranges(registry: &KeyRegistry, ranges: &[KeyRange]) -> Vec<String> {
        let mut keys = Vec::new();
        for range in ranges {
            for (_pos, bucket_keys) in registry.range(range.start..range.end) {
                for (bucket, key) in bucket_keys {
                    keys.push(format!("{bucket}/{key}"));
                }
            }
        }
        keys
    }

    /// Run the anti-entropy background loop.
    ///
    /// This is the main entry point. Spawns as a tokio task and runs
    /// until the cancellation token fires or the service is dropped.
    pub async fn run(&self, mut shutdown: tokio::sync::watch::Receiver<bool>) {
        if !self.config.enabled {
            info!("anti-entropy service disabled by configuration");
            return;
        }

        let interval = Duration::from_secs(self.config.interval_secs);
        info!(
            interval_secs = self.config.interval_secs,
            merkle_depth = self.config.merkle_depth,
            pool_id = self.pool_id.as_u32(),
            "anti-entropy service started"
        );

        loop {
            tokio::select! {
                _ = tokio::time::sleep(interval) => {
                    self.run_round().await;
                }
                _ = shutdown.changed() => {
                    info!("anti-entropy service shutting down");
                    return;
                }
            }
        }
    }

    /// Execute a single anti-entropy round.
    ///
    /// Iterates over all key ranges this node owns, picks a random alive
    /// replica peer, and performs a Merkle exchange + repair.
    async fn run_round(&self) {
        let start = std::time::Instant::now();
        let trees = self.merkle_trees.read().await;
        let ranges: Vec<(u64, KeyRange)> =
            trees.iter().map(|(&range_id, tree)| (range_id, tree.key_range())).collect();
        drop(trees);

        for (range_id, key_range) in ranges {
            let peer = match self.pick_random_alive_peer() {
                Some(p) => p,
                None => {
                    debug!("no alive peers available for anti-entropy");
                    continue;
                }
            };

            if let Err(e) = self.exchange_with_peer(peer, range_id, key_range).await {
                warn!(
                    %peer,
                    range_start = key_range.start,
                    error = %e,
                    "anti-entropy exchange failed"
                );
            }
        }

        let elapsed = start.elapsed();
        self.metrics
            .total_repair_duration_ms
            .fetch_add(elapsed.as_millis() as u64, Ordering::Relaxed);
        self.metrics.rounds_completed.fetch_add(1, Ordering::Relaxed);

        debug!(
            elapsed_ms = elapsed.as_millis(),
            "anti-entropy round completed"
        );
    }

    /// Exchange Merkle digests with a peer and repair divergent keys.
    async fn exchange_with_peer(
        &self,
        peer: NodeId,
        range_id: u64,
        key_range: KeyRange,
    ) -> Result<(), ClusterError> {
        let trees = self.merkle_trees.read().await;
        let local_tree = match trees.get(&range_id) {
            Some(t) => t.clone(),
            None => return Ok(()),
        };
        drop(trees);

        let local_root = local_tree.root_hash();

        // Step 1: Exchange root hash.
        let response = self
            .rpc_client
            .send_merkle_digest(
                peer,
                proto::MerkleDigestRequest {
                    bucket: String::new(), // Range-based, not bucket-scoped
                    range_start: key_range.start.to_be_bytes().to_vec(),
                    range_end: key_range.end.to_be_bytes().to_vec(),
                    level: 0,
                    tree_hash: local_root.to_vec(),
                    topology_epoch: 0,
                },
            )
            .await?;

        if response.r#match {
            debug!(
                %peer,
                range_start = key_range.start,
                "merkle trees match, range is consistent"
            );
            return Ok(());
        }

        // Step 2: Find divergent key ranges from the remote's response.
        let divergent_ranges: Vec<KeyRange> = response
            .divergent_ranges
            .iter()
            .filter_map(|r| {
                let start_bytes: [u8; 8] = r.range_start.as_slice().try_into().ok()?;
                let end_bytes: [u8; 8] = r.range_end.as_slice().try_into().ok()?;
                Some(KeyRange::new(
                    u64::from_be_bytes(start_bytes),
                    u64::from_be_bytes(end_bytes),
                ))
            })
            .collect();

        if divergent_ranges.is_empty() {
            return Ok(());
        }

        let num_divergent = divergent_ranges.len();
        self.metrics
            .divergent_keys_found
            .fetch_add(num_divergent as u64, Ordering::Relaxed);

        info!(
            %peer,
            range_start = key_range.start,
            divergent_ranges = num_divergent,
            "divergence detected, starting repair"
        );

        // Step 3: Resolve divergent ranges to actual object keys.
        //
        // We send LOCAL keys in the divergent ranges to the peer. The peer
        // returns its version of each key. We apply LWW to pull newer data.
        //
        // Keys that exist ONLY on the peer (not in our registry) will be
        // discovered when the PEER runs its own anti-entropy round against
        // us — both sides run this loop, ensuring bidirectional convergence.
        let registry = self.key_registry.read().await;
        let keys = Self::keys_in_ranges(&registry, &divergent_ranges);
        drop(registry);

        if keys.is_empty() {
            debug!(
                %peer,
                "divergent ranges found but no local keys to check — \
                 peer will push missing keys in its own anti-entropy round"
            );
            return Ok(());
        }

        let keys_to_sync = keys
            .into_iter()
            .take(self.config.max_keys_per_batch as usize)
            .collect::<Vec<_>>();

        let mut repair_stream = self
            .rpc_client
            .send_repair_sync(
                peer,
                proto::RepairSyncRequest {
                    bucket: String::new(),
                    keys: keys_to_sync,
                    topology_epoch: 0,
                },
            )
            .await?;

        let mut repaired = 0u64;
        let mut failed = 0u64;

        while let Some(item) = repair_stream
            .message()
            .await
            .map_err(|e| ClusterError::Rpc(format!("repair sync stream error: {e}")))?
        {
            if item.key.is_empty() {
                continue;
            }

            let peer_hlc = item.hlc.as_ref().map(|h| HlcTimestamp {
                wall_time: h.wall_time,
                logical: h.logical,
                node_id: u128::from_be_bytes(h.node_id.as_slice().try_into().unwrap_or([0u8; 16])),
            });

            if let Some(peer_hlc_ts) = peer_hlc {
                // Parse "bucket/key" from the response.
                let (bucket, obj_key) = match item.key.split_once('/') {
                    Some((b, k)) => (b, k),
                    None => {
                        warn!(key = %item.key, "invalid repair key format, expected bucket/key");
                        failed += 1;
                        continue;
                    }
                };

                // LWW: only write if peer's version is newer than local.
                //
                // Note on tie-breaking: IndexRecord doesn't store the HLC
                // logical counter, so when wall times match we treat any
                // peer logical > 0 as "newer". This is a safe bias toward
                // convergence — the worst case is a redundant overwrite
                // with identical data.
                let should_write = match self.storage.head_object(bucket, obj_key).await {
                    Ok(local_record) => {
                        let local_millis = local_record.modified_at / 1_000_000;
                        peer_hlc_ts.wall_time > local_millis
                            || (peer_hlc_ts.wall_time == local_millis && peer_hlc_ts.logical > 0)
                    }
                    Err(_) => true, // Key missing locally — always write.
                };

                if !should_write {
                    continue;
                }

                // Defense-in-depth: ensure bucket marker exists locally.
                // The bucket marker itself is also an object in __system__,
                // so it will be synced by anti-entropy. But if a data object
                // arrives before its bucket marker, auto-create the marker
                // so API-layer bucket checks pass.
                if bucket != "__system__" {
                    let marker_key = format!("__s4_bucket_marker_{bucket}");
                    if self.storage.head_object("__system__", &marker_key).await.is_err() {
                        debug!(bucket = %bucket, "auto-creating bucket marker during anti-entropy repair");
                        let _ = self
                            .storage
                            .put_object(
                                "__system__",
                                &marker_key,
                                b"1",
                                "application/octet-stream",
                                &HashMap::new(),
                            )
                            .await;
                    }
                }

                // Use content_type and metadata from the peer's response
                // (preserves the original values from when the object was written).
                let content_type = if item.content_type.is_empty() {
                    "application/octet-stream"
                } else {
                    &item.content_type
                };

                match self
                    .storage
                    .put_object(bucket, obj_key, &item.data, content_type, &item.metadata)
                    .await
                {
                    Ok(_) => {
                        debug!(
                            bucket = %bucket,
                            key = %obj_key,
                            peer_wall_time = peer_hlc_ts.wall_time,
                            "anti-entropy: repaired key from peer"
                        );
                        repaired += 1;
                    }
                    Err(e) => {
                        warn!(
                            bucket = %bucket,
                            key = %obj_key,
                            error = %e,
                            "anti-entropy: failed to write repair data"
                        );
                        failed += 1;
                    }
                }
            } else {
                failed += 1;
            }
        }

        self.metrics.repairs_completed.fetch_add(repaired, Ordering::Relaxed);
        self.metrics.repairs_failed.fetch_add(failed, Ordering::Relaxed);

        // Step 4: Advance repair frontier.
        if repaired > 0 {
            let now = HlcTimestamp {
                wall_time: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64,
                logical: 0,
                node_id: self.local_node_id.0,
            };
            let mut frontier = self.repair_frontier.write().await;
            frontier.advance(&key_range, peer, now);
        }

        debug!(
            %peer,
            repaired,
            failed,
            "anti-entropy repair round completed"
        );

        Ok(())
    }

    /// Pick a random alive replica peer (not self).
    fn pick_random_alive_peer(&self) -> Option<NodeId> {
        let members = self.gossip.members();
        let alive_peers: Vec<NodeId> = self
            .pool_nodes
            .iter()
            .filter(|&&n| n != self.local_node_id)
            .filter(|n| {
                members.get(n).map(|s| matches!(s.status, NodeStatus::Alive)).unwrap_or(false)
            })
            .copied()
            .collect();

        if alive_peers.is_empty() {
            return None;
        }

        // Simple random selection using wall clock as seed.
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let idx = (now as usize) % alive_peers.len();
        Some(alive_peers[idx])
    }

    /// Handle an incoming Merkle digest exchange request from a peer.
    ///
    /// Compares the peer's tree hash against local state and returns
    /// the list of divergent ranges.
    pub async fn handle_merkle_exchange(
        &self,
        request: &proto::MerkleDigestRequest,
    ) -> proto::MerkleDigestResponse {
        let range_start =
            request.range_start.as_slice().try_into().map(u64::from_be_bytes).unwrap_or(0);

        let trees = self.merkle_trees.read().await;
        let local_tree = match trees.get(&range_start) {
            Some(t) => t,
            None => {
                return proto::MerkleDigestResponse {
                    r#match: false,
                    divergent_ranges: Vec::new(),
                    tree_hash: Vec::new(),
                    epoch_mismatch: false,
                };
            }
        };

        let local_root = local_tree.root_hash();
        let remote_root: [u8; 32] = request.tree_hash.as_slice().try_into().unwrap_or([0u8; 32]);

        if local_root == remote_root {
            return proto::MerkleDigestResponse {
                r#match: true,
                divergent_ranges: Vec::new(),
                tree_hash: local_root.to_vec(),
                epoch_mismatch: false,
            };
        }

        // Build a remote hash map for comparison.
        // For the initial exchange, we only have the root. Report all
        // leaf ranges as potentially divergent. In a production system,
        // this would do a multi-round drill-down.
        let key_range = local_tree.key_range();
        let num_leaves = local_tree.leaf_count();
        let range_size = key_range.end.wrapping_sub(key_range.start);

        let divergent_ranges: Vec<proto::MerkleRange> = (0..num_leaves)
            .map(|i| {
                let leaf_start = key_range.start.wrapping_add(if range_size == 0 {
                    0
                } else {
                    (i as u128 * range_size as u128 / num_leaves as u128) as u64
                });
                let leaf_end = key_range.start.wrapping_add(if range_size == 0 {
                    0
                } else {
                    ((i + 1) as u128 * range_size as u128 / num_leaves as u128) as u64
                });
                let leaf_idx = num_leaves - 1 + i;
                let local_hash = local_tree.hash_at(leaf_idx).unwrap_or([0u8; 32]);

                proto::MerkleRange {
                    range_start: leaf_start.to_be_bytes().to_vec(),
                    range_end: leaf_end.to_be_bytes().to_vec(),
                    local_hash: local_hash.to_vec(),
                }
            })
            .collect();

        proto::MerkleDigestResponse {
            r#match: false,
            divergent_ranges,
            tree_hash: local_root.to_vec(),
            epoch_mismatch: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gossip::{GossipHandle, NodeState};
    use crate::identity::{NodeAddr, NodeCapacity, NodeMeta};
    use crate::repair::merkle::KeyRange;
    use crate::rpc::NodeClientConfig;
    use std::net::SocketAddr;

    fn test_node_state(node_id: NodeId, status: NodeStatus) -> NodeState {
        NodeState {
            meta: NodeMeta {
                node_id,
                node_name: "test".into(),
                addr: NodeAddr {
                    grpc_addr: SocketAddr::from(([127, 0, 0, 1], 9100)),
                    http_addr: SocketAddr::from(([127, 0, 0, 1], 9000)),
                    gossip_addr: SocketAddr::from(([127, 0, 0, 1], 9200)),
                },
                pool_id: PoolId::new(1),
                topology_epoch: 1,
                protocol_version: 1,
                capacity: NodeCapacity::default(),
                started_at: 0,
                s4_version: "0.1.0".into(),
                edition: crate::identity::Edition::Community,
            },
            status,
            last_seen: std::time::Instant::now(),
        }
    }

    /// Minimal mock storage for unit tests.
    struct MockStorage;

    #[async_trait::async_trait]
    impl StorageEngine for MockStorage {
        async fn put_object(
            &self,
            _b: &str,
            _k: &str,
            _d: &[u8],
            _ct: &str,
            _m: &HashMap<String, String>,
        ) -> Result<String, s4_core::StorageError> {
            Ok("\"mock-etag\"".into())
        }
        async fn get_object(
            &self,
            _b: &str,
            _k: &str,
        ) -> Result<(Vec<u8>, s4_core::IndexRecord), s4_core::StorageError> {
            Err(s4_core::StorageError::ObjectNotFound { key: "mock".into() })
        }
        async fn delete_object(&self, _b: &str, _k: &str) -> Result<(), s4_core::StorageError> {
            Ok(())
        }
        async fn head_object(
            &self,
            _b: &str,
            _k: &str,
        ) -> Result<s4_core::IndexRecord, s4_core::StorageError> {
            Err(s4_core::StorageError::ObjectNotFound { key: "mock".into() })
        }
        async fn list_objects(
            &self,
            _b: &str,
            _p: &str,
            _m: usize,
        ) -> Result<Vec<(String, s4_core::IndexRecord)>, s4_core::StorageError> {
            Ok(vec![])
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
            Ok(("\"mock-etag\"".into(), None))
        }
        async fn get_object_version(
            &self,
            _b: &str,
            _k: &str,
            _v: &str,
        ) -> Result<(Vec<u8>, s4_core::IndexRecord), s4_core::StorageError> {
            Err(s4_core::StorageError::ObjectNotFound { key: "mock".into() })
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
            Err(s4_core::StorageError::ObjectNotFound { key: "mock".into() })
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
            Ok(("\"mock-etag\"".into(), None))
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
            Err(s4_core::StorageError::ObjectNotFound { key: "mock".into() })
        }
        async fn open_object_version_stream(
            &self,
            _b: &str,
            _k: &str,
            _v: &str,
            _opts: s4_core::ReadOptions,
        ) -> Result<s4_core::ObjectStream, s4_core::StorageError> {
            Err(s4_core::StorageError::ObjectNotFound { key: "mock".into() })
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
                etag: "\"mock-etag\"".into(),
            })
        }
    }

    fn make_service(local: NodeId, peers: Vec<(NodeId, NodeStatus)>) -> AntiEntropyService {
        let mut members = HashMap::new();
        let mut pool_nodes = vec![local];
        members.insert(local, test_node_state(local, NodeStatus::Alive));
        for (id, status) in &peers {
            members.insert(*id, test_node_state(*id, *status));
            pool_nodes.push(*id);
        }

        let gossip = Arc::new(GossipHandle::new_for_testing(members));
        let client = Arc::new(NodeClient::new(gossip.clone(), NodeClientConfig::default()));
        let storage: Arc<dyn StorageEngine> = Arc::new(MockStorage);

        AntiEntropyService::new(
            AntiEntropyConfig::default(),
            local,
            PoolId::new(1),
            pool_nodes,
            gossip,
            client,
            storage,
        )
    }

    #[test]
    fn metrics_default_zero() {
        let metrics = AntiEntropyMetrics::new();
        let snap = metrics.snapshot();
        assert_eq!(snap.rounds_completed, 0);
        assert_eq!(snap.divergent_keys_found, 0);
        assert_eq!(snap.repairs_completed, 0);
        assert_eq!(snap.repairs_failed, 0);
    }

    #[tokio::test]
    async fn init_tree_stores_merkle_tree() {
        let local = NodeId(1);
        let service = make_service(local, vec![(NodeId(2), NodeStatus::Alive)]);

        let range = KeyRange::full();
        let entries = vec![
            (100u64, *blake3::hash(b"a").as_bytes()),
            (200u64, *blake3::hash(b"b").as_bytes()),
        ];
        service.init_tree(range, &entries).await;

        let trees = service.merkle_trees.read().await;
        assert!(trees.contains_key(&range.start));
        let tree = &trees[&range.start];
        assert_ne!(tree.root_hash(), [0u8; 32]);
    }

    #[tokio::test]
    async fn incremental_update_changes_tree() {
        let local = NodeId(1);
        let service = make_service(local, vec![]);

        let range = KeyRange::full();
        service.init_tree(range, &[]).await;

        let root_before = {
            let trees = service.merkle_trees.read().await;
            trees[&range.start].root_hash()
        };

        let hash = *blake3::hash(b"data").as_bytes();
        service.on_key_insert(&range, 100, hash).await;

        let root_after = {
            let trees = service.merkle_trees.read().await;
            trees[&range.start].root_hash()
        };

        assert_ne!(root_before, root_after);
    }

    #[tokio::test]
    async fn handle_merkle_exchange_matching_trees() {
        let local = NodeId(1);
        let service = make_service(local, vec![]);

        let range = KeyRange::full();
        let entries = vec![(100u64, *blake3::hash(b"x").as_bytes())];
        service.init_tree(range, &entries).await;

        let root = {
            let trees = service.merkle_trees.read().await;
            trees[&range.start].root_hash()
        };

        let request = proto::MerkleDigestRequest {
            bucket: String::new(),
            range_start: range.start.to_be_bytes().to_vec(),
            range_end: range.end.to_be_bytes().to_vec(),
            level: 0,
            tree_hash: root.to_vec(),
            topology_epoch: 0,
        };

        let response = service.handle_merkle_exchange(&request).await;
        assert!(response.r#match);
        assert!(response.divergent_ranges.is_empty());
    }

    #[tokio::test]
    async fn handle_merkle_exchange_divergent_trees() {
        let local = NodeId(1);
        let service = make_service(local, vec![]);

        let range = KeyRange::full();
        let entries = vec![(100u64, *blake3::hash(b"x").as_bytes())];
        service.init_tree(range, &entries).await;

        // Send a different root hash.
        let request = proto::MerkleDigestRequest {
            bucket: String::new(),
            range_start: range.start.to_be_bytes().to_vec(),
            range_end: range.end.to_be_bytes().to_vec(),
            level: 0,
            tree_hash: vec![0u8; 32], // different from actual root
            topology_epoch: 0,
        };

        let response = service.handle_merkle_exchange(&request).await;
        assert!(!response.r#match);
        assert!(!response.divergent_ranges.is_empty());
    }

    #[test]
    fn pick_peer_excludes_self() {
        let local = NodeId(1);
        let service = make_service(
            local,
            vec![
                (NodeId(2), NodeStatus::Alive),
                (NodeId(3), NodeStatus::Alive),
            ],
        );

        // Run multiple times — should never return self.
        for _ in 0..100 {
            if let Some(peer) = service.pick_random_alive_peer() {
                assert_ne!(peer, local);
            }
        }
    }

    #[test]
    fn pick_peer_excludes_dead_nodes() {
        let local = NodeId(1);
        let service = make_service(
            local,
            vec![(NodeId(2), NodeStatus::Dead), (NodeId(3), NodeStatus::Dead)],
        );

        assert!(service.pick_random_alive_peer().is_none());
    }

    #[test]
    fn pick_peer_returns_alive_node() {
        let local = NodeId(1);
        let service = make_service(
            local,
            vec![
                (NodeId(2), NodeStatus::Dead),
                (NodeId(3), NodeStatus::Alive),
            ],
        );

        let peer = service.pick_random_alive_peer();
        assert_eq!(peer, Some(NodeId(3)));
    }

    #[tokio::test]
    async fn repair_frontier_integration() {
        let local = NodeId(1);
        let service = make_service(local, vec![]);

        let range = KeyRange::full();
        let hlc = HlcTimestamp {
            wall_time: 1000,
            logical: 0,
            node_id: 0,
        };

        {
            let mut frontier = service.repair_frontier.write().await;
            frontier.advance(&range, NodeId(2), hlc);
        }

        let frontier = service.repair_frontier.read().await;
        assert_eq!(frontier.get_cut(&range, &NodeId(2)), Some(hlc));
    }

    #[tokio::test]
    async fn disabled_service_returns_immediately() {
        let local = NodeId(1);
        let config = AntiEntropyConfig {
            enabled: false,
            ..Default::default()
        };

        let gossip = Arc::new(GossipHandle::new_for_testing(HashMap::new()));
        let client = Arc::new(NodeClient::new(gossip.clone(), NodeClientConfig::default()));
        let storage: Arc<dyn StorageEngine> = Arc::new(MockStorage);
        let service = AntiEntropyService::new(
            config,
            local,
            PoolId::new(1),
            vec![local],
            gossip,
            client,
            storage,
        );

        let (tx, rx) = tokio::sync::watch::channel(false);
        // Send shutdown immediately.
        let _ = tx.send(true);
        service.run(rx).await;
        // If we get here, the service exited properly.
    }
}
