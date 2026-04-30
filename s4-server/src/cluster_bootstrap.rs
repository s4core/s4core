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

//! Cluster mode bootstrap — wires up gossip, gRPC, coordinators, and background workers.
//!
//! Called from `App::run` when `S4_MODE=cluster`. All cluster services are
//! started here and their handles returned for lifecycle management.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use s4_cluster::rpc::server::RpcServerHandle;
use s4_cluster::{
    AntiEntropyConfig, AntiEntropyService, BucketPlacement, ClusterConfig, ClusterEvent,
    ClusterLimits, DataScrubber, DistributedGcService, DistributedListCoordinator, Edition,
    GcConfig, GossipConfig, GossipIdentity, GossipService, HealingContext, HintedHandoffConfig,
    HintedHandoffManager, HybridClock, IdempotencyTracker, ListTimeouts, MultipartReplicaStorage,
    NodeAddr, NodeCapacity, NodeClient, NodeClientConfig, NodeId, NodeMeta, NodeRpcServer,
    PlacementRouter, PoolConfig, PoolId, QuorumReadCoordinator, QuorumWriteCoordinator,
    ReadRepairService, ReadTimeouts, ReplicaMultipartHandler, ReplicaReadHandler,
    ReplicaWriteHandler, RpcServerConfig, ScrubberConfig, TombstonePolicy, WriteTimeouts,
    PROTOCOL_VERSION, S4_VERSION,
};
use s4_core::storage::engine::StorageEngine;
use s4_core::storage::BitcaskStorageEngine;
use tokio::sync::{mpsc, watch, RwLock};
use tracing::{debug, info};

use crate::config::ClusterModeConfig;

// ---------------------------------------------------------------------------
// StorageEngine adapter
// ---------------------------------------------------------------------------

/// Adapts `Arc<RwLock<BitcaskStorageEngine>>` to `Arc<dyn StorageEngine>`.
///
/// Cluster components (coordinators, replica handlers) require
/// `Arc<dyn StorageEngine>`.  The server holds storage behind an
/// `Arc<RwLock<…>>` for shared access — this adapter bridges the two.
struct SharedStorageAdapter(Arc<RwLock<BitcaskStorageEngine>>);

#[async_trait::async_trait]
impl StorageEngine for SharedStorageAdapter {
    async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        data: &[u8],
        content_type: &str,
        metadata: &std::collections::HashMap<String, String>,
    ) -> Result<String, s4_core::error::StorageError> {
        let storage = self.0.write().await;
        storage.put_object(bucket, key, data, content_type, metadata).await
    }

    async fn get_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<(Vec<u8>, s4_core::types::IndexRecord), s4_core::error::StorageError> {
        let storage = self.0.read().await;
        storage.get_object(bucket, key).await
    }

    async fn delete_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<(), s4_core::error::StorageError> {
        let storage = self.0.write().await;
        storage.delete_object(bucket, key).await
    }

    async fn head_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<s4_core::types::IndexRecord, s4_core::error::StorageError> {
        let storage = self.0.read().await;
        storage.head_object(bucket, key).await
    }

    async fn list_objects(
        &self,
        bucket: &str,
        prefix: &str,
        max_keys: usize,
    ) -> Result<Vec<(String, s4_core::types::IndexRecord)>, s4_core::error::StorageError> {
        let storage = self.0.read().await;
        storage.list_objects(bucket, prefix, max_keys).await
    }

    async fn list_objects_after(
        &self,
        bucket: &str,
        prefix: &str,
        start_after: &str,
        max_keys: usize,
    ) -> Result<Vec<(String, s4_core::types::IndexRecord)>, s4_core::error::StorageError> {
        let storage = self.0.read().await;
        storage.list_objects_after(bucket, prefix, start_after, max_keys).await
    }

    // Versioning — not used by cluster coordinators (separate code path in handlers).
    async fn put_object_versioned(
        &self,
        _bucket: &str,
        _key: &str,
        _data: &[u8],
        _content_type: &str,
        _metadata: &std::collections::HashMap<String, String>,
        _versioning_status: s4_core::types::VersioningStatus,
    ) -> Result<(String, Option<String>), s4_core::error::StorageError> {
        Err(s4_core::error::StorageError::InvalidOperation {
            operation: "versioning".to_string(),
            reason: "Not used in cluster coordinator path".to_string(),
        })
    }

    async fn get_object_version(
        &self,
        _bucket: &str,
        _key: &str,
        _version_id: &str,
    ) -> Result<(Vec<u8>, s4_core::types::IndexRecord), s4_core::error::StorageError> {
        Err(s4_core::error::StorageError::InvalidOperation {
            operation: "versioning".to_string(),
            reason: "Not used in cluster coordinator path".to_string(),
        })
    }

    async fn delete_object_versioned(
        &self,
        _bucket: &str,
        _key: &str,
        _version_id: Option<&str>,
        _versioning_status: s4_core::types::VersioningStatus,
    ) -> Result<s4_core::storage::DeleteResult, s4_core::error::StorageError> {
        Err(s4_core::error::StorageError::InvalidOperation {
            operation: "versioning".to_string(),
            reason: "Not used in cluster coordinator path".to_string(),
        })
    }

    async fn head_object_version(
        &self,
        _bucket: &str,
        _key: &str,
        _version_id: &str,
    ) -> Result<s4_core::types::IndexRecord, s4_core::error::StorageError> {
        Err(s4_core::error::StorageError::InvalidOperation {
            operation: "versioning".to_string(),
            reason: "Not used in cluster coordinator path".to_string(),
        })
    }

    async fn list_object_versions(
        &self,
        _bucket: &str,
        _prefix: &str,
        _key_marker: Option<&str>,
        _version_id_marker: Option<&str>,
        _max_keys: usize,
    ) -> Result<s4_core::storage::ListVersionsResult, s4_core::error::StorageError> {
        Err(s4_core::error::StorageError::InvalidOperation {
            operation: "versioning".to_string(),
            reason: "Not used in cluster coordinator path".to_string(),
        })
    }

    async fn put_object_with_retention(
        &self,
        _bucket: &str,
        _key: &str,
        _data: &[u8],
        _content_type: &str,
        _metadata: &std::collections::HashMap<String, String>,
        _versioning_status: s4_core::types::VersioningStatus,
        _default_retention: Option<s4_core::types::DefaultRetention>,
    ) -> Result<(String, Option<String>), s4_core::error::StorageError> {
        Err(s4_core::error::StorageError::InvalidOperation {
            operation: "object_lock".to_string(),
            reason: "Not used in cluster coordinator path".to_string(),
        })
    }

    async fn update_object_metadata(
        &self,
        _bucket: &str,
        _key: &str,
        _version_id: &str,
        _updated_record: s4_core::types::IndexRecord,
    ) -> Result<(), s4_core::error::StorageError> {
        Err(s4_core::error::StorageError::InvalidOperation {
            operation: "metadata_update".to_string(),
            reason: "Not used in cluster coordinator path".to_string(),
        })
    }

    async fn put_object_streaming(
        &self,
        bucket: &str,
        key: &str,
        reader: Box<dyn tokio::io::AsyncRead + Unpin + Send>,
        content_length: u64,
        content_type: &str,
        metadata: &std::collections::HashMap<String, String>,
        etag: &str,
    ) -> Result<s4_core::storage::StreamingPutResult, s4_core::error::StorageError> {
        let storage = self.0.read().await;
        storage
            .put_object_streaming(
                bucket,
                key,
                reader,
                content_length,
                content_type,
                metadata,
                etag,
            )
            .await
    }

    async fn open_object_stream(
        &self,
        bucket: &str,
        key: &str,
        options: s4_core::storage::ReadOptions,
    ) -> Result<s4_core::storage::ObjectStream, s4_core::error::StorageError> {
        let storage = self.0.read().await;
        storage.open_object_stream(bucket, key, options).await
    }

    async fn open_object_version_stream(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
        options: s4_core::storage::ReadOptions,
    ) -> Result<s4_core::storage::ObjectStream, s4_core::error::StorageError> {
        let storage = self.0.read().await;
        storage.open_object_version_stream(bucket, key, version_id, options).await
    }
}

#[async_trait::async_trait]
impl MultipartReplicaStorage for SharedStorageAdapter {
    async fn create_multipart_session(
        &self,
        upload_id: &str,
        bucket: &str,
        key: &str,
        content_type: &str,
        metadata: &std::collections::HashMap<String, String>,
    ) -> Result<(), s4_core::error::StorageError> {
        let storage = self.0.read().await;
        storage
            .create_multipart_session(upload_id, bucket, key, content_type, metadata)
            .await
    }

    async fn get_multipart_session(
        &self,
        upload_id: &str,
    ) -> Result<s4_core::types::composite::MultipartUploadSession, s4_core::error::StorageError>
    {
        let storage = self.0.read().await;
        storage.get_multipart_session(upload_id).await
    }

    async fn upload_multipart_part_streaming(
        &self,
        upload_id: &str,
        part_number: u32,
        reader: Box<dyn tokio::io::AsyncRead + Unpin + Send>,
        content_length: u64,
    ) -> Result<s4_core::types::composite::MultipartPartRecord, s4_core::error::StorageError> {
        let storage = self.0.read().await;
        storage
            .upload_part_streaming(upload_id, part_number, reader, content_length)
            .await
            .map(|result| result.record)
    }

    async fn list_multipart_parts(
        &self,
        upload_id: &str,
    ) -> Result<Vec<s4_core::types::composite::MultipartPartRecord>, s4_core::error::StorageError>
    {
        let storage = self.0.read().await;
        storage.list_multipart_parts(upload_id)
    }

    async fn mark_session_completing(
        &self,
        upload_id: &str,
    ) -> Result<s4_core::types::composite::MultipartUploadSession, s4_core::error::StorageError>
    {
        let storage = self.0.read().await;
        storage.mark_session_completing(upload_id).await
    }

    async fn revert_session_to_open(
        &self,
        upload_id: &str,
    ) -> Result<(), s4_core::error::StorageError> {
        let storage = self.0.read().await;
        storage.revert_session_to_open(upload_id).await
    }

    async fn complete_multipart_native(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        selected_parts: &[s4_core::types::composite::MultipartPartRecord],
        s3_etag: &str,
        content_type: &str,
        metadata: &std::collections::HashMap<String, String>,
    ) -> Result<s4_core::types::IndexRecord, s4_core::error::StorageError> {
        let storage = self.0.read().await;
        storage
            .complete_multipart_native(
                bucket,
                key,
                upload_id,
                selected_parts,
                s3_etag,
                content_type,
                metadata,
            )
            .await
    }

    async fn abort_multipart_native(
        &self,
        upload_id: &str,
    ) -> Result<(), s4_core::error::StorageError> {
        let storage = self.0.read().await;
        storage.abort_multipart_native(upload_id).await
    }
}

// ---------------------------------------------------------------------------
// Cluster state returned to App
// ---------------------------------------------------------------------------

/// Handles and services created during cluster bootstrap.
///
/// Returned to `App::run` so it can inject coordinators into `AppState`
/// and abort background tasks on shutdown.
pub struct ClusterState {
    /// Write coordinator — injected into `AppState` for distributed PUT/DELETE.
    pub write_coordinator: Arc<QuorumWriteCoordinator>,
    /// Read coordinator — injected into `AppState` for distributed GET/HEAD.
    pub read_coordinator: Arc<QuorumReadCoordinator>,
    /// List coordinator — injected into `AppState` for distributed LIST.
    pub list_coordinator: Arc<DistributedListCoordinator>,
    /// Background task handles to abort on shutdown.
    pub background_handles: Vec<tokio::task::JoinHandle<()>>,
    /// gRPC server handle (for graceful shutdown).
    pub rpc_handle: RpcServerHandle,
}

// ---------------------------------------------------------------------------
// Bootstrap entry point
// ---------------------------------------------------------------------------

/// Start all cluster services and return handles for lifecycle management.
///
/// This function:
/// 1. Creates or loads a persistent node identity
/// 2. Starts the SWIM gossip service for cluster membership
/// 3. Creates the HLC for causal ordering
/// 4. Creates the gRPC client pool and starts the gRPC server
/// 5. Builds the placement router for key → replica mapping
/// 6. Creates quorum coordinators (write, read, list)
/// 7. Spawns background workers (anti-entropy, scrubber, GC, event logger)
pub async fn bootstrap_cluster(
    cluster_config: &ClusterModeConfig,
    storage: Arc<RwLock<BitcaskStorageEngine>>,
    data_dir: &Path,
    cluster_limits: &ClusterLimits,
) -> Result<ClusterState> {
    // -----------------------------------------------------------------------
    // 1. Node identity
    // -----------------------------------------------------------------------
    let node_id = s4_cluster::get_or_create_node_id(data_dir)
        .map_err(|e| anyhow::anyhow!("Failed to get/create node identity: {e}"))?;

    let node_name = cluster_config.node_id.clone().unwrap_or_else(|| node_id.to_string());

    info!(node_id = %node_id, node_name = %node_name, "Cluster node identity");

    // -----------------------------------------------------------------------
    // 2. Parse addresses
    // -----------------------------------------------------------------------
    let grpc_addr: SocketAddr = resolve_addr(
        cluster_config
            .grpc_addr
            .as_ref()
            .context("S4_NODE_GRPC_ADDR is required in cluster mode")?,
    )
    .await
    .context("Invalid S4_NODE_GRPC_ADDR")?;

    let http_addr: SocketAddr = resolve_addr(
        cluster_config
            .http_addr
            .as_ref()
            .context("S4_NODE_HTTP_ADDR is required in cluster mode")?,
    )
    .await
    .context("Invalid S4_NODE_HTTP_ADDR")?;

    // Gossip uses the same port as gRPC (UDP instead of TCP).
    let gossip_addr = grpc_addr;

    let node_addr = NodeAddr {
        grpc_addr,
        http_addr,
        gossip_addr,
    };

    // -----------------------------------------------------------------------
    // 3. Parse pool nodes and build ClusterConfig
    // -----------------------------------------------------------------------
    let pool_name = cluster_config
        .pool_name
        .as_ref()
        .context("S4_POOL_NAME is required in cluster mode")?
        .clone();

    let pool_nodes_str = cluster_config
        .pool_nodes
        .as_ref()
        .context("S4_POOL_NODES is required in cluster mode")?;

    let pool_node_addrs = parse_pool_nodes(pool_nodes_str)
        .await
        .context("Failed to parse S4_POOL_NODES")?;

    let mut seeds: Vec<SocketAddr> = Vec::with_capacity(cluster_config.seeds.len());
    for s in &cluster_config.seeds {
        seeds.push(resolve_addr(s).await.with_context(|| format!("Failed to resolve seed '{s}'"))?);
    }

    let pool_id = PoolId::new(0);

    let pool_config = PoolConfig {
        pool_id,
        name: pool_name,
        nodes: pool_node_addrs
            .iter()
            .map(|(_name, addr)| NodeAddr {
                grpc_addr: *addr,
                http_addr: *addr, // Updated via gossip metadata
                gossip_addr: *addr,
            })
            .collect(),
        replication_factor: Some(cluster_config.replication_factor),
        write_quorum: Some(cluster_config.write_quorum),
        read_quorum: Some(cluster_config.read_quorum),
    };

    let cluster_cfg = ClusterConfig {
        cluster_name: cluster_config.cluster_name.clone(),
        seeds: seeds.clone(),
        pools: vec![pool_config],
        default_replication_factor: cluster_config.replication_factor,
        default_write_quorum: cluster_config.write_quorum,
        default_read_quorum: cluster_config.read_quorum,
        epoch: 1,
    };

    cluster_cfg
        .validate_with_limits(cluster_limits)
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    info!(
        pool_nodes = pool_node_addrs.len(),
        seeds = seeds.len(),
        rf = cluster_config.replication_factor,
        wq = cluster_config.write_quorum,
        rq = cluster_config.read_quorum,
        "Cluster configuration validated"
    );

    // -----------------------------------------------------------------------
    // 4. Build NodeMeta
    // -----------------------------------------------------------------------
    let started_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let local_edition = if cluster_limits.is_community() {
        Edition::Community
    } else {
        Edition::Enterprise
    };

    let node_meta = NodeMeta {
        node_id,
        node_name,
        addr: node_addr,
        pool_id,
        topology_epoch: cluster_cfg.epoch,
        protocol_version: PROTOCOL_VERSION,
        capacity: NodeCapacity::default(),
        started_at,
        s4_version: S4_VERSION.to_string(),
        edition: local_edition,
    };

    // -----------------------------------------------------------------------
    // 5. Start gossip service
    // -----------------------------------------------------------------------
    // Use epoch millis as the initial incarnation so that a restarted node
    // always has a higher incarnation than any previous run.  This ensures
    // foca's `win_addr_conflict` picks the new identity, allowing the node
    // to rejoin the cluster after being declared dead.
    let initial_incarnation = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    let gossip_identity = GossipIdentity {
        node_id,
        gossip_addr,
        incarnation: initial_incarnation,
    };

    let gossip_config = GossipConfig {
        bind_addr: gossip_addr,
        seeds: seeds.clone(),
        meta_broadcast_interval: Duration::from_secs(30),
        max_packet_size: 1400,
    };

    let (gossip_handle, cluster_events) =
        GossipService::start(gossip_config, gossip_identity, node_meta.clone())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to start gossip service: {e}"))?;

    let gossip_handle = Arc::new(gossip_handle);
    info!(%gossip_addr, "Gossip service started");

    // -----------------------------------------------------------------------
    // 6. Create HLC
    // -----------------------------------------------------------------------
    let clock = Arc::new(HybridClock::new(node_id));

    // -----------------------------------------------------------------------
    // 7. Create RPC client pool
    // -----------------------------------------------------------------------
    let rpc_client = Arc::new(NodeClient::new(
        gossip_handle.clone(),
        NodeClientConfig::default(),
    ));

    // -----------------------------------------------------------------------
    // 8. Create StorageEngine adapter for cluster components
    // -----------------------------------------------------------------------
    let shared_storage = Arc::new(SharedStorageAdapter(storage.clone()));
    let storage_engine: Arc<dyn StorageEngine> = shared_storage.clone();
    let multipart_storage: Arc<dyn MultipartReplicaStorage> = shared_storage.clone();

    // -----------------------------------------------------------------------
    // 9. Create read replica handler (write handler created after anti-entropy)
    // -----------------------------------------------------------------------
    let idempotency = Arc::new(IdempotencyTracker::default());

    let replica_read = Arc::new(ReplicaReadHandler::new(
        storage_engine.clone(),
        cluster_cfg.epoch,
    ));

    // -----------------------------------------------------------------------
    // 10. Wait for gossip to discover all pool nodes (real NodeIds)
    // -----------------------------------------------------------------------
    let pool_nodes_ids: Vec<NodeId> = {
        let expected_count = pool_node_addrs.len();
        let timeout = Duration::from_secs(30);
        let poll_interval = Duration::from_millis(500);
        let start = std::time::Instant::now();

        info!(
            expected = expected_count,
            "Waiting for gossip to discover all pool nodes..."
        );

        loop {
            let members = gossip_handle.members();

            // Build addr→NodeId map from gossip members
            let mut addr_to_id: HashMap<SocketAddr, NodeId> = HashMap::new();
            addr_to_id.insert(grpc_addr, node_id); // Local node is always known
            for (mid, state) in &members {
                addr_to_id.insert(state.meta.addr.grpc_addr, *mid);
            }

            // Check if all pool node addresses are resolved
            let resolved: Vec<NodeId> = pool_node_addrs
                .iter()
                .filter_map(|(_name, addr)| addr_to_id.get(addr).copied())
                .collect();

            if resolved.len() == expected_count {
                info!(
                    elapsed_ms = start.elapsed().as_millis() as u64,
                    "All {} pool nodes discovered via gossip", expected_count
                );

                // Return NodeIds in the same order as pool_node_addrs
                break pool_node_addrs
                    .iter()
                    .map(|(_name, addr)| *addr_to_id.get(addr).expect("just checked all resolved"))
                    .collect();
            }

            if start.elapsed() > timeout {
                let missing: Vec<_> = pool_node_addrs
                    .iter()
                    .filter(|(_name, addr)| !addr_to_id.contains_key(addr))
                    .map(|(name, addr)| format!("{}({})", name, addr))
                    .collect();
                anyhow::bail!(
                    "Gossip convergence timeout after {}s: missing pool nodes: {}",
                    timeout.as_secs(),
                    missing.join(", ")
                );
            }

            tokio::time::sleep(poll_interval).await;
        }
    };

    // -----------------------------------------------------------------------
    // 10b. Validate edition compatibility across the pool
    // -----------------------------------------------------------------------
    //
    // After all nodes are discovered, verify that no node in the pool is
    // running a different edition. A CE node cannot join an EE pool and
    // vice-versa — mixed-edition clusters are not supported because they
    // would disagree on topology limits and available features.
    //
    // NOTE: This check runs once at bootstrap. If a replacement node with
    // a different edition joins later (via admin API), the mismatch won't
    // be detected until the next restart. A future enhancement could
    // subscribe to gossip `Joined` events for continuous validation.
    {
        let members = gossip_handle.members();
        for pid in &pool_nodes_ids {
            // Skip self — we already know our own edition.
            if *pid == node_id {
                continue;
            }
            if let Some(state) = members.get(pid) {
                // Only check if the remote node has real metadata (not placeholder).
                // Nodes that just joined gossip but haven't broadcast metadata yet
                // will have empty s4_version — skip them with a warning.
                if state.meta.s4_version.is_empty() {
                    tracing::warn!(
                        node_id = %pid,
                        "Skipping edition check for node with placeholder metadata \
                         (metadata not yet received via gossip broadcast)"
                    );
                    continue;
                }
                if state.meta.edition != local_edition {
                    anyhow::bail!(
                        "Edition mismatch: this node is running {} but node {} ({}) \
                         is running {}. Mixed-edition clusters are not supported. \
                         All nodes in a pool must run the same edition.",
                        local_edition,
                        pid,
                        state.meta.node_name,
                        state.meta.edition,
                    );
                }
            }
        }
        info!(
            "Edition compatibility check passed (all nodes: {})",
            local_edition
        );
    }

    // -----------------------------------------------------------------------
    // 11. Create anti-entropy service
    // -----------------------------------------------------------------------

    let ae_config = AntiEntropyConfig {
        enabled: true,
        interval_secs: cluster_config.anti_entropy_interval_secs,
        ..AntiEntropyConfig::default()
    };

    let anti_entropy = Arc::new(AntiEntropyService::new(
        ae_config,
        node_id,
        pool_id,
        pool_nodes_ids.clone(),
        gossip_handle.clone(),
        rpc_client.clone(),
        storage_engine.clone(),
    ));

    // Scan local storage to build the initial Merkle tree and key registry.
    // This must happen before the gRPC server starts serving anti-entropy
    // exchange requests, otherwise the local Merkle tree would be empty and
    // every exchange would report full divergence.
    anti_entropy.init_from_storage().await;

    // -----------------------------------------------------------------------
    // 11b. Create write replica handler (needs anti-entropy for Merkle updates)
    // -----------------------------------------------------------------------
    let replica_write = Arc::new(
        ReplicaWriteHandler::new(storage_engine.clone(), idempotency, cluster_cfg.epoch)
            .with_anti_entropy(anti_entropy.clone()),
    );

    let replica_multipart = Arc::new(ReplicaMultipartHandler::new(
        multipart_storage.clone(),
        Arc::new(IdempotencyTracker::default()),
        cluster_cfg.epoch,
    ));

    // -----------------------------------------------------------------------
    // 12. Start gRPC server
    // -----------------------------------------------------------------------
    let rpc_server = NodeRpcServer::new(node_id, Arc::new(node_meta), gossip_handle.clone())
        .with_replica_handler(replica_write)
        .with_multipart_handler(replica_multipart)
        .with_read_handler(replica_read)
        .with_anti_entropy(anti_entropy.clone());

    let rpc_server_config = RpcServerConfig {
        bind_addr: grpc_addr,
        ..RpcServerConfig::default()
    };

    let rpc_handle = rpc_server
        .start(rpc_server_config)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to start gRPC server: {e}"))?;

    info!(%grpc_addr, "gRPC server started");

    // -----------------------------------------------------------------------
    // 12. Build placement router
    // -----------------------------------------------------------------------
    // Build node_id_resolver: maps gRPC address → real NodeId (from gossip).
    let mut node_id_resolver: HashMap<SocketAddr, NodeId> = HashMap::new();
    for (i, (_name, addr)) in pool_node_addrs.iter().enumerate() {
        node_id_resolver.insert(*addr, pool_nodes_ids[i]);
    }

    let bucket_placement = BucketPlacement::new();
    let router = Arc::new(
        PlacementRouter::new(&cluster_cfg, node_id, bucket_placement, &node_id_resolver)
            .map_err(|e| anyhow::anyhow!("Failed to build placement router: {e}"))?,
    );

    // -----------------------------------------------------------------------
    // 13. Create hinted handoff manager
    // -----------------------------------------------------------------------
    let hint_config = HintedHandoffConfig::new(data_dir);
    let hints = Arc::new(HintedHandoffManager::new(
        hint_config,
        gossip_handle.clone(),
        rpc_client.clone(),
    ));

    // -----------------------------------------------------------------------
    // 14. Create read repair service
    // -----------------------------------------------------------------------
    let read_repair = Arc::new(ReadRepairService::start(rpc_client.clone()));

    // -----------------------------------------------------------------------
    // 15. Create quorum coordinators
    // -----------------------------------------------------------------------
    let write_coordinator = Arc::new(
        QuorumWriteCoordinator::new(
            node_id,
            storage_engine.clone(),
            rpc_client.clone(),
            router.clone(),
            clock.clone(),
            hints.clone(),
            &cluster_cfg,
            WriteTimeouts::default(),
        )
        .with_multipart_storage(multipart_storage),
    );

    let read_coordinator = Arc::new(QuorumReadCoordinator::new(
        node_id,
        storage_engine.clone(),
        rpc_client.clone(),
        router.clone(),
        clock.clone(),
        read_repair,
        &cluster_cfg,
        ReadTimeouts::default(),
    ));

    let list_coordinator = Arc::new(DistributedListCoordinator::new(
        node_id,
        storage_engine.clone(),
        rpc_client.clone(),
        router,
        cluster_cfg.epoch,
        ListTimeouts::default(),
    ));

    info!(
        "Quorum coordinators created (W={}, R={})",
        cluster_config.write_quorum, cluster_config.read_quorum
    );

    // -----------------------------------------------------------------------
    // 16. Spawn background workers
    // -----------------------------------------------------------------------
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut background_handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();

    // Gossip event logger
    let event_handle = tokio::spawn(cluster_event_logger(cluster_events));
    background_handles.push(event_handle);

    // Distributed GC
    let gc_config = GcConfig {
        enabled: true,
        interval: Duration::from_secs(
            cluster_config.gc_grace_days as u64 * 24 * 3600 / 10, // Check ~10x per grace period
        ),
        tombstone_policy: TombstonePolicy {
            gc_grace: Duration::from_secs(cluster_config.gc_grace_days as u64 * 24 * 3600),
            max_rejoin_downtime: Duration::from_secs(
                cluster_config.max_rejoin_downtime_days as u64 * 24 * 3600,
            ),
        },
    };

    let index_db = {
        let storage_guard = storage.read().await;
        storage_guard.index_db().clone()
    };

    let repair_frontier = anti_entropy.repair_frontier().clone();
    let pool_replicas = Arc::new(RwLock::new(pool_nodes_ids.clone()));

    let gc_service = Arc::new(DistributedGcService::new(
        gc_config,
        index_db.clone(),
        repair_frontier,
        pool_replicas,
    ));

    {
        let gc = gc_service.clone();
        let rx = shutdown_rx.clone();
        let handle = tokio::spawn(async move {
            gc.run(rx).await;
        });
        background_handles.push(handle);
        info!(
            "Distributed GC worker started (grace period: {} days)",
            cluster_config.gc_grace_days
        );
    }

    // Data scrubber
    let scrubber_config = ScrubberConfig {
        enabled: true,
        full_scan_period: Duration::from_secs(
            cluster_config.scrubber_full_scan_days as u64 * 24 * 3600,
        ),
        ..ScrubberConfig::default()
    };

    let volumes_dir = data_dir.join("volumes");
    let healing_context = HealingContext {
        node_id,
        rpc_client: rpc_client.clone(),
        gossip: gossip_handle.clone(),
        placement: Arc::new(
            s4_cluster::PlacementStrategy::new(
                pool_nodes_ids.clone(),
                cluster_config.replication_factor,
            )
            .map_err(|e| anyhow::anyhow!("Failed to create placement strategy: {e}"))?,
        ),
    };

    let scrubber = Arc::new(DataScrubber::new_cluster(
        scrubber_config,
        volumes_dir,
        index_db,
        healing_context,
    ));

    {
        let scrub = scrubber.clone();
        let rx = shutdown_rx.clone();
        let handle = tokio::spawn(async move {
            scrub.run(rx).await;
        });
        background_handles.push(handle);
        info!(
            "Data scrubber started (full scan: {} days)",
            cluster_config.scrubber_full_scan_days
        );
    }

    // Store shutdown sender so we can signal workers on shutdown
    // (we'll drop it in app.rs shutdown sequence)
    let shutdown_handle = tokio::spawn(async move {
        // Keep the shutdown_tx alive until this task is aborted.
        // When aborted, drop signals all receivers.
        let _tx = shutdown_tx;
        std::future::pending::<()>().await;
    });
    background_handles.push(shutdown_handle);

    info!("Cluster bootstrap complete — node is ready to serve distributed requests");

    Ok(ClusterState {
        write_coordinator,
        read_coordinator,
        list_coordinator,
        background_handles,
        rpc_handle,
    })
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Resolve an address string to a `SocketAddr`.
///
/// Accepts both IP:port (`127.0.0.1:9100`) and hostname:port (`s4-node1:9100`).
/// Tries plain `parse()` first for zero-cost IP paths, then falls back to
/// `tokio::net::lookup_host` for DNS resolution (e.g. Docker Compose service names).
async fn resolve_addr(addr: &str) -> Result<SocketAddr> {
    // Fast path: already a valid IP:port
    if let Ok(sa) = addr.parse::<SocketAddr>() {
        return Ok(sa);
    }

    // Slow path: DNS lookup (Docker internal DNS, /etc/hosts, etc.)
    let resolved = tokio::net::lookup_host(addr)
        .await
        .with_context(|| format!("DNS lookup failed for '{addr}'"))?
        .next()
        .with_context(|| format!("DNS lookup returned no results for '{addr}'"))?;

    debug!(hostname = addr, resolved = %resolved, "Resolved hostname to IP");
    Ok(resolved)
}

/// Parse the `S4_POOL_NODES` format: `name:host:port,name:host:port,...`
///
/// Each entry is a pool member defined as `<node_name>:<grpc_host>:<grpc_port>`.
async fn parse_pool_nodes(s: &str) -> Result<Vec<(String, SocketAddr)>> {
    let mut result = Vec::new();

    for entry in s.split(',') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }

        // Format: name:host:port
        let parts: Vec<&str> = entry.splitn(2, ':').collect();
        if parts.len() != 2 {
            anyhow::bail!(
                "Invalid pool node entry '{}': expected 'name:host:port'",
                entry
            );
        }

        let name = parts[0].to_string();
        let addr = resolve_addr(parts[1])
            .await
            .with_context(|| format!("Invalid address in pool node '{}': '{}'", name, parts[1]))?;

        result.push((name, addr));
    }

    if result.is_empty() {
        anyhow::bail!("S4_POOL_NODES must contain at least one entry");
    }

    Ok(result)
}

/// Log cluster events from the gossip service.
async fn cluster_event_logger(mut events: mpsc::Receiver<ClusterEvent>) {
    while let Some(event) = events.recv().await {
        match &event {
            ClusterEvent::Joined(node_id, meta) => {
                info!(
                    node_id = %node_id,
                    node_name = %meta.node_name,
                    grpc = %meta.addr.grpc_addr,
                    pool = %meta.pool_id,
                    "Node joined cluster"
                );
            }
            ClusterEvent::Left(node_id) => {
                info!(node_id = %node_id, "Node left cluster");
            }
            ClusterEvent::Suspected(node_id) => {
                tracing::warn!(node_id = %node_id, "Node suspected (not responding)");
            }
            ClusterEvent::Recovered(node_id) => {
                info!(node_id = %node_id, "Node recovered");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use s4_core::storage::ReadOptions;
    use s4_core::StorageEngine;
    use std::collections::HashMap;
    use tokio::io::AsyncReadExt;

    async fn test_adapter() -> (SharedStorageAdapter, tempfile::TempDir) {
        let temp_dir = tempfile::TempDir::new().expect("create temp dir");
        let data_path = temp_dir.path().join("volumes");
        let metadata_path = temp_dir.path().join("metadata_db");
        std::fs::create_dir_all(&data_path).expect("create data dir");

        let engine =
            BitcaskStorageEngine::new(data_path, metadata_path, 10 * 1024 * 1024, 4096, false)
                .await
                .expect("create storage engine");

        (
            SharedStorageAdapter(Arc::new(RwLock::new(engine))),
            temp_dir,
        )
    }

    #[tokio::test]
    async fn shared_storage_adapter_delegates_streaming_read_and_write() {
        let (adapter, _temp_dir) = test_adapter().await;

        let metadata = HashMap::new();
        let small_data = b"cluster streaming read path";
        adapter
            .put_object(
                "cluster-test",
                "small.txt",
                small_data,
                "text/plain",
                &metadata,
            )
            .await
            .expect("put small object");

        let mut object_stream = adapter
            .open_object_stream("cluster-test", "small.txt", ReadOptions::default())
            .await
            .expect("open object stream");
        let mut read_back = Vec::new();
        object_stream
            .body
            .read_to_end(&mut read_back)
            .await
            .expect("read streamed object");
        assert_eq!(read_back, small_data);

        let streamed_data = b"cluster streaming write path".to_vec();
        let reader: Box<dyn tokio::io::AsyncRead + Unpin + Send> =
            Box::new(std::io::Cursor::new(streamed_data.clone()));
        let result = adapter
            .put_object_streaming(
                "cluster-test",
                "streamed.txt",
                reader,
                streamed_data.len() as u64,
                "text/plain",
                &metadata,
                "streamed-etag",
            )
            .await
            .expect("streaming put through adapter");
        assert_eq!(result.bytes_written, streamed_data.len() as u64);

        let mut object_stream = adapter
            .open_object_stream("cluster-test", "streamed.txt", ReadOptions::default())
            .await
            .expect("open streamed put object");
        let mut read_back = Vec::new();
        object_stream
            .body
            .read_to_end(&mut read_back)
            .await
            .expect("read streamed put object");
        assert_eq!(read_back, streamed_data);
    }
}
