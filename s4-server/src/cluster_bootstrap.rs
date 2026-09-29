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
    FilesystemCapacity, GcConfig, GossipConfig, GossipIdentity, GossipService, HealingContext,
    HintedHandoffConfig, HintedHandoffManager, HybridClock, IdempotencyTracker, ListTimeouts,
    MultipartReplicaStorage, NodeAddr, NodeCapacityProvider, NodeClient, NodeClientConfig, NodeId,
    NodeMeta, NodeRpcServer, PlacementRouter, PoolConfig, PoolId, QuorumReadCoordinator,
    QuorumWriteCoordinator, ReadRepairService, ReadTimeouts, ReplicaMultipartHandler,
    ReplicaReadHandler, ReplicaWriteHandler, RpcServerConfig, ScrubberConfig, TombstonePolicy,
    WriteTimeouts, PROTOCOL_VERSION, S4_VERSION,
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

    // Must be delegated explicitly: the trait's default refuses, because
    // releasing a blob reference cannot be expressed through the other methods.
    // This is the path EC cutover takes on both the coordinator and the replica.
    async fn detach_object_data_if_hlc(
        &self,
        bucket: &str,
        key: &str,
        expected_hlc_timestamp: u64,
        expected_hlc_logical: u32,
        expected_origin_node_id: u128,
        layout: s4_core::types::ObjectLayout,
    ) -> Result<bool, s4_core::error::StorageError> {
        let storage = self.0.write().await;
        storage
            .detach_object_data_if_hlc(
                bucket,
                key,
                expected_hlc_timestamp,
                expected_hlc_logical,
                expected_origin_node_id,
                layout,
            )
            .await
    }

    // The mirror of the detach above, and delegated for the same reason: the
    // trait's default refuses, because writing a blob and rewriting the index
    // entry in one batch cannot be expressed through the other methods. This is
    // the path a reverse migration takes on every replica.
    async fn attach_object_data_if_hlc(
        &self,
        bucket: &str,
        key: &str,
        expected_hlc_timestamp: u64,
        expected_hlc_logical: u32,
        expected_origin_node_id: u128,
        data: &[u8],
    ) -> Result<bool, s4_core::error::StorageError> {
        let storage = self.0.write().await;
        storage
            .attach_object_data_if_hlc(
                bucket,
                key,
                expected_hlc_timestamp,
                expected_hlc_logical,
                expected_origin_node_id,
                data,
            )
            .await
    }

    // Local-only counterpart of the detach above; see the trait for why the
    // default refuses.
    async fn restore_erasure_coded_entry(
        &self,
        bucket: &str,
        key: &str,
        record: s4_core::types::IndexRecord,
    ) -> Result<bool, s4_core::error::StorageError> {
        let storage = self.0.write().await;
        storage.restore_erasure_coded_entry(bucket, key, record).await
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

    async fn heal_object_blob(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        data: &[u8],
    ) -> Result<(), s4_core::error::StorageError> {
        let storage = self.0.write().await;
        storage.heal_object_blob(bucket, key, version_id, data).await
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

type EcAdminServiceHandle = Arc<dyn s4_cluster::EcAdminService>;
type EcObjectRuntimeHandle = Arc<dyn s4_cluster::EcObjectRuntime>;

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
    /// Enterprise Erasure Coding admin service, when this pool is EC.
    pub ec_admin_service: Option<EcAdminServiceHandle>,
    /// Enterprise Erasure Coding S3 object runtime, when this pool is EC.
    pub ec_object_runtime: Option<EcObjectRuntimeHandle>,
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
    // The identity the operator says this data directory belongs to, when they
    // said so. Checked here rather than later because every other step —
    // gossip, the erasure set roster, the repair queue — is addressed by this
    // identity, and a node that adopted the wrong one is wrong in all of them.
    let expected_identity = match cluster_config.expected_node_identity.as_deref() {
        Some(raw) => Some(raw.parse::<NodeId>().map_err(|e| {
            anyhow::anyhow!("S4_EXPECTED_NODE_IDENTITY is not a node identity: {e}")
        })?),
        None => None,
    };
    let identity = s4_cluster::load_node_identity(data_dir, expected_identity)
        .map_err(|e| anyhow::anyhow!("Failed to get/create node identity: {e}"))?;
    let node_id = identity.node_id;

    let node_name = cluster_config.node_id.clone().unwrap_or_else(|| node_id.to_string());

    info!(
        node_id = %node_id,
        node_name = %node_name,
        origin = identity.origin.as_str(),
        "Cluster node identity"
    );

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

    // What this node already knows about its pool, read before anything asks
    // the network. It answers both questions a start has about a member that is
    // down: where it lives and who it is (decision EC2-203).
    let mut peer_record = s4_cluster::PoolPeerRecord::load(data_dir);
    let pool_members = parse_pool_nodes(pool_nodes_str, &peer_record.last_known_addrs())
        .await
        .context("Failed to parse S4_POOL_NODES")?;
    peer_record.retain_configured(
        &pool_members
            .iter()
            .map(|member| member.configured_addr.clone())
            .collect::<Vec<_>>(),
    );
    let pool_node_addrs: Vec<(String, SocketAddr)> =
        pool_members.iter().map(|member| (member.name.clone(), member.addr)).collect();

    // A seed is where gossip starts looking, not a member of the pool, and a
    // seed that is down is the ordinary case this whole mechanism exists for.
    // An orchestrator takes the DNS name of a stopped container away, so
    // failing the start over one unresolvable seed would put the requirement
    // back exactly where decision EC2-203 took it from. The start still needs
    // somewhere to start: it fails only when no seed at all can be resolved.
    let mut seeds: Vec<SocketAddr> = Vec::with_capacity(cluster_config.seeds.len());
    let mut unresolved_seeds: Vec<String> = Vec::new();
    for s in &cluster_config.seeds {
        match resolve_addr(s).await {
            Ok(addr) => seeds.push(addr),
            Err(error) => {
                tracing::warn!(seed = %s, %error, "seed address does not resolve; skipping it");
                unresolved_seeds.push(s.clone());
            }
        }
    }
    if seeds.is_empty() && !cluster_config.seeds.is_empty() {
        anyhow::bail!(
            "no seed of S4_SEEDS resolves: {}",
            unresolved_seeds.join(", ")
        );
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
        storage_class: cluster_config.pool_storage_class().map_err(|e| anyhow::anyhow!("{e}"))?,
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
    // 3b. Refuse the start if a live pool member already answers for us
    // -----------------------------------------------------------------------
    // Two machines under one identity are two histories of the same slot
    // owner: each repairs shards the other does not have, each accepts writes
    // the other never sees, and the pool has no way to tell which of them holds
    // the shard a manifest names. Every outcome of that is worse than not
    // starting (`EC2-NREP-02-001`).
    //
    // Asked here, before gossip: after the announcement this node is itself a
    // claimant, and the question "who holds this identity" no longer has one
    // answer. The peers are asked directly at their configured addresses, which
    // is also why this needs no membership view of its own.
    if cluster_config.identity_conflict_check {
        if let Err(refusal) =
            refuse_start_on_identity_twin(node_id, grpc_addr, &pool_node_addrs).await
        {
            // Undo the one thing this start wrote. An identity adopted into a
            // blank directory was created by this start and belongs to a node
            // that is alive elsewhere; leaving it behind would mean the next
            // start reads a file claiming an identity this machine must not
            // have. A refused start changes nothing (EC2-142).
            if identity.origin.is_adopted() {
                let id_path = s4_cluster::node_identity_path(data_dir);
                if let Err(error) = std::fs::remove_file(&id_path) {
                    tracing::warn!(
                        path = %id_path.display(),
                        %error,
                        "could not remove the identity this refused start had adopted; clear the \
                         data directory before starting this node again"
                    );
                }
            }
            return Err(refusal);
        }
    } else {
        tracing::warn!(
            %node_id,
            "S4_NODE_IDENTITY_CONFLICT_CHECK is off: this node starts even if another live member \
             already answers for its identity"
        );
    }

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

    // Measured once here and re-measured by the gossip loop on every metadata
    // tick. Without the first measurement the node would announce zeros until
    // that tick, and a peer placing data in the meantime would read this node as
    // having no measured room.
    let node_capacity: Arc<dyn NodeCapacityProvider> = Arc::new(FilesystemCapacity::new(data_dir));
    let node_meta = NodeMeta {
        node_id,
        node_name,
        addr: node_addr,
        pool_id,
        topology_epoch: cluster_cfg.epoch,
        protocol_version: PROTOCOL_VERSION,
        capacity: node_capacity.measure(),
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
        ..GossipConfig::default()
    };

    let (gossip_handle, cluster_events) = GossipService::start(
        gossip_config,
        gossip_identity,
        node_meta.clone(),
        Some(Arc::clone(&node_capacity)),
    )
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
    // 10. Name every node of the pool (real NodeIds)
    // -----------------------------------------------------------------------
    //
    // Everything under the configuration — placement, quorum, repair, erasure
    // set slots — is addressed by `NodeId`, and the configuration names
    // addresses. This step turns one into the other, and it has to name every
    // address: a pool this node could name only in part would compute a
    // different ring and a different slot order from the pool it belongs to,
    // and place data where no manifest looks for it.
    //
    // What it must *not* require is that every node be up. A restart is most
    // needed when the cluster is already short a node, and a start that waits
    // for full membership turns one dead node into a pool nobody may restart
    // (decision EC2-203). Identities do not change, so they do not have to be
    // rediscovered: what gossip cannot show is taken from what this node
    // recorded the last time it saw that address answer.
    let configured_addrs: Vec<SocketAddr> =
        pool_node_addrs.iter().map(|(_name, addr)| *addr).collect();
    let recalled_identities = peer_record.recalled_identities();

    let pool_roll = {
        let expected_count = pool_node_addrs.len();
        let quorum = pool_visibility_quorum(expected_count);
        let timeout = Duration::from_secs(30);
        let poll_interval = Duration::from_millis(500);
        let start = std::time::Instant::now();

        info!(
            expected = expected_count,
            quorum,
            recorded = recalled_identities.len(),
            "Naming the pool's nodes from gossip..."
        );

        loop {
            let members = gossip_handle.members();

            // Build addr→NodeId map from gossip members
            let mut seen_alive: HashMap<SocketAddr, NodeId> = HashMap::new();
            seen_alive.insert(grpc_addr, node_id); // Local node is always known
            for (mid, state) in &members {
                seen_alive.insert(state.meta.addr.grpc_addr, *mid);
            }

            let alive_count =
                configured_addrs.iter().filter(|addr| seen_alive.contains_key(addr)).count();
            let roll = name_pool_nodes(&pool_members, &seen_alive, &recalled_identities);
            let out_of_time = start.elapsed() > timeout;

            if let Ok(roll) = &roll {
                // Everyone is up: the ordinary start, and nothing is recalled.
                //
                // Otherwise a quorum being up, with the rest named from the
                // record, means there is nothing further worth waiting for.
                // Below quorum the wait runs its full course anyway, because a
                // live answer beats a remembered one for an address whose
                // machine was replaced while this node was down.
                if roll.recalled.is_empty() || alive_count >= quorum || out_of_time {
                    break (roll.clone(), start.elapsed(), alive_count);
                }
            }

            if out_of_time {
                let missing = match &roll {
                    Ok(_) => Vec::new(),
                    Err(unnamed) => unnamed.clone(),
                };
                anyhow::bail!(
                    "Gossip convergence timeout after {}s: missing pool nodes: {}",
                    timeout.as_secs(),
                    missing.join(", ")
                );
            }

            tokio::time::sleep(poll_interval).await;
        }
    };

    let (pool_roll, naming_elapsed, pool_nodes_seen_alive) = pool_roll;
    let pool_nodes_ids: Vec<NodeId> = pool_roll.node_ids.clone();

    if pool_roll.recalled.is_empty() {
        info!(
            elapsed_ms = naming_elapsed.as_millis() as u64,
            pool_nodes = pool_nodes_ids.len(),
            "All pool nodes discovered via gossip"
        );
    } else {
        tracing::warn!(
            elapsed_ms = naming_elapsed.as_millis() as u64,
            pool_nodes = pool_nodes_ids.len(),
            seen_alive = pool_nodes_seen_alive,
            not_seen_alive = pool_roll.recalled.len(),
            nodes = %pool_roll.recalled_names(),
            "Some pool nodes were not seen alive at start; their identities come from what this \
             node recorded before, and their slots are served degraded until they answer"
        );
    }

    // Keep what this start learned, so the next one can name these addresses
    // even if their machines are down by then. Failing to write it costs the
    // next start its shortcut, not this one its correctness.
    for (member, node_id) in pool_members.iter().zip(pool_nodes_ids.iter()) {
        peer_record.remember(
            &member.name,
            &member.configured_addr,
            member.addr,
            *node_id,
            unix_time_secs(),
        );
    }
    if let Err(error) = peer_record.save(data_dir) {
        tracing::warn!(%error, "could not record what this node learned about its pool");
    }

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
    // 10c. Validate Enterprise erasure-coded pool shape/profile/topology
    // -----------------------------------------------------------------------
    #[cfg(feature = "enterprise")]
    let mut ec_operations_service = None;

    let (ec_admin_service, ec_object_runtime): (
        Option<EcAdminServiceHandle>,
        Option<EcObjectRuntimeHandle>,
    ) = if cluster_cfg.pools[0].storage_class.is_erasure_coded() {
        #[cfg(feature = "enterprise")]
        {
            let named_nodes: Vec<(String, NodeId)> = pool_node_addrs
                .iter()
                .zip(pool_nodes_ids.iter())
                .map(|((name, _addr), node_id)| (name.clone(), *node_id))
                .collect();

            // A pool is a registry of erasure sets. Startup configuration still
            // describes exactly one — the pool's inherited set — so a deployment
            // that changes nothing keeps the EC v1 shape. Building the registry
            // is also where a bad set configuration is refused: duplicate set
            // ids, a set of another pool, or two sets claiming the inherited
            // identity all fail here rather than at the first write.
            let ec_layout = s4_ee::erasure_coding::validate_pool_from_env(
                &cluster_cfg.pools[0],
                &named_nodes,
                cluster_cfg.epoch,
                started_at,
            )
            .map_err(|e| anyhow::anyhow!("{e}"))?;
            let ec_sets = ec_layout.sets;

            for ec_set in ec_sets.sets() {
                info!(
                    pool = %cluster_cfg.pools[0].name,
                    set_id = ec_set.set_id,
                    inherited = ec_set.inherited,
                    profile = %ec_set.profile.name,
                    total_shards = ec_set.profile.total_shards(),
                    manifest_quorum = ec_set.profile.manifest_quorum(),
                    topology_health = ?ec_set.topology_health,
                    "Enterprise erasure-coded pool validated"
                );
                for risk in &ec_set.topology_risks {
                    tracing::warn!(
                        pool = %cluster_cfg.pools[0].name,
                        set_id = ec_set.set_id,
                        risk = %risk,
                        "EC topology risk"
                    );
                }
            }

            let ec_metadata_path = data_dir.parent().unwrap_or(data_dir).join("ec_metadata_db");
            let service = Arc::new(
                s4_ee::ec_ops::EcOperationsService::open(
                    &ec_metadata_path,
                    cluster_cfg.pools[0].name.clone(),
                    ec_sets,
                    ec_layout.builder,
                    node_id,
                )
                .map_err(|e| anyhow::anyhow!("{e}"))?,
            );
            // The sets and layout records this node kept locally, taken before
            // any worker can encode an object. The cluster read path is not
            // configured yet, so nothing is read over the network here; this
            // only stops a restart with changed topology labels from laying
            // new objects out by those labels until the cluster answers. The
            // cluster's copies are taken once the workers are up, below.
            service
                .adopt_erasure_sets_at_startup()
                .await
                .context("node cannot serve a pool whose erasure sets it could not take")?;
            let ec_admin_service = Some(service.clone() as Arc<dyn s4_cluster::EcAdminService>);
            let ec_object_runtime = Some(service.clone() as Arc<dyn s4_cluster::EcObjectRuntime>);
            ec_operations_service = Some(service.clone());
            info!(
                path = %ec_metadata_path.display(),
                "Enterprise erasure-coded admin and S3 object runtime initialized"
            );
            (ec_admin_service, ec_object_runtime)
        }

        #[cfg(not(feature = "enterprise"))]
        {
            anyhow::bail!("S4_POOL_TYPE=ec requires an Enterprise Edition build");
        }
    } else {
        (None, None)
    };

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
    // Only the Enterprise branch below reassigns it, so a CE build sees a
    // binding it never mutates.
    #[cfg_attr(not(feature = "enterprise"), allow(unused_mut))]
    let mut rpc_server = NodeRpcServer::new(node_id, Arc::new(node_meta), gossip_handle.clone())
        .with_replica_handler(replica_write)
        .with_multipart_handler(replica_multipart)
        .with_read_handler(replica_read)
        .with_anti_entropy(anti_entropy.clone());

    // Auto-tiering records an access on whichever node served it, so a node
    // deciding that an object has gone cold has to ask its peers first.
    #[cfg(feature = "enterprise")]
    if let Some(service) = &ec_operations_service {
        rpc_server =
            rpc_server.with_heat_query(service.clone() as Arc<dyn s4_cluster::EcHeatQuery>);
        // A node holds only the manifests it transcoded or read, so the dedup
        // measurement has to ask every node of the pool for its share.
        rpc_server = rpc_server
            .with_content_digest_scan(service.clone() as Arc<dyn s4_cluster::EcContentDigestScan>);
        // The tombstone sweep asks every owner whether it has seen each delete
        // before it releases a shard. Batched, that is one request per owner
        // per cycle instead of one per delete per owner.
        rpc_server = rpc_server
            .with_tombstone_probe(service.clone() as Arc<dyn s4_cluster::EcTombstoneProbeService>);
    }

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
    // Keep identities attached to configured members, not to transport addresses.
    // An absent member's remembered IP may already belong to a live neighbour.
    let named_pools = HashMap::from([(pool_id, pool_nodes_ids.clone())]);
    let router = Arc::new(
        PlacementRouter::from_pool_node_ids(
            &cluster_cfg,
            node_id,
            BucketPlacement::new(),
            &named_pools,
        )
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

    #[cfg(feature = "enterprise")]
    if let Some(service) = ec_operations_service {
        service.configure_cluster_membership(gossip_handle.clone());
        // The tiering surface answers for the whole pool, not for the objects
        // this node happens to hold, so it needs the distributed catalog.
        service.configure_tiering_catalog(list_coordinator.clone());

        match service.start_heat_tracker() {
            Some(handle) => {
                background_handles.push(handle);
                info!("Enterprise EC object activity tracker started");
            }
            None => {
                info!(
                    "Enterprise EC object activity tracker not started (disabled or already running)"
                );
            }
        }

        match service
            .start_transcode_worker(
                read_coordinator.clone(),
                write_coordinator.clone(),
                storage_engine.clone(),
                rpc_client.clone(),
                cluster_cfg.epoch,
            )
            .map_err(|e| anyhow::anyhow!("{e}"))?
        {
            Some(handle) => {
                background_handles.push(handle);
                info!("Enterprise EC transcode worker started");
            }
            None => {
                info!("Enterprise EC transcode worker not started (disabled or already running)");
            }
        }

        match service.start_tiering_worker() {
            Some(handle) => {
                background_handles.push(handle);
                info!("Enterprise EC auto-tiering worker started");
            }
            None => {
                // The worker also declines when cluster membership is not
                // wired, and says so at error level from inside the call: the
                // rolling-upgrade gate would have nothing to check the pool
                // against. Kept out of this line so the reason an operator
                // reads is the real one rather than this list.
                info!(
                    "Enterprise EC auto-tiering worker not started (disabled, already running, \
                     or not wired — see any preceding error)"
                );
            }
        }

        // Started whether or not tiering is on. The same queue carries the
        // operator's rollback command, and a node that stops draining it when
        // the feature is switched off would take that rollback away exactly
        // when it is reached for.
        match service.start_promotion_worker() {
            Some(handle) => {
                background_handles.push(handle);
                info!("Enterprise EC reverse tiering worker started");
            }
            None => {
                info!("Enterprise EC reverse tiering worker not started (already running)");
            }
        }

        match service
            .start_integrity_workers(
                read_coordinator.clone(),
                write_coordinator.clone(),
                storage_engine.clone(),
                rpc_client.clone(),
                cluster_cfg.epoch,
            )
            .map_err(|e| anyhow::anyhow!("{e}"))?
        {
            Some(handle) => {
                background_handles.push(handle);
                info!("Enterprise EC repair/anti-entropy/scrub worker started");
            }
            None => {
                info!(
                    "Enterprise EC repair/anti-entropy/scrub worker not started (disabled or already running)"
                );
            }
        }

        // Started last, because it needs the read runtime the workers above
        // configure. An operator adds an erasure set on one node; this is how
        // every other node learns about it, and until a node has, it cannot
        // serve that set's objects. A node whose own configuration contradicts
        // the pool's roster refuses to start, while defects in shared cluster
        // state or temporary read failures fall back to its configured sets.
        service
            .adopt_erasure_sets_at_startup()
            .await
            .context("node cannot serve a pool whose erasure sets it could not take")?;

        // What this node is allowed to do on its way back in, decided now that
        // the pool's slot layout is known and before the first EC read reaches
        // it. A node that owes shards it has not rebuilt is held out of the EC
        // read path rather than allowed to answer from storage that is empty or
        // a month stale (`EC2-NREP-02-002`).
        service
            .evaluate_node_rejoin(identity.origin)
            .await
            .context("node could not decide what its return to the pool allows")?;

        match service.start_erasure_set_refresher() {
            Some(handle) => {
                background_handles.push(handle);
                info!("Enterprise EC erasure set refresher started");
            }
            None => info!("Enterprise EC erasure set refresher already running"),
        }

        // Seals packed segments that stopped receiving objects. A busy pool
        // seals them on the write that fills them and never needs this; a quiet
        // one would otherwise leave a half-full segment open indefinitely, and
        // its objects would keep costing three copies.
        if let Some(handle) = service.start_packed_segment_maintenance() {
            background_handles.push(handle);
            info!("Enterprise EC packed segment maintenance started");
        }
    }

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
        start_delay: Duration::from_secs(cluster_config.scrubber_start_delay_secs),
        unhealed_retry_period: Duration::from_secs(cluster_config.scrubber_unhealed_retry_secs),
        ..ScrubberConfig::default()
    };

    // Asked of the storage engine rather than built from `data_dir`: what this
    // function is given is already the volumes directory (the storage config's
    // `data_path`), and a second `join("volumes")` sent the scrubber to a path
    // that does not exist — where it dutifully scanned nothing, cycle after
    // cycle, and said so only as `volumes_scanned=0`.
    let volumes_dir = {
        let storage_guard = storage.read().await;
        storage_guard.volumes_dir().to_path_buf()
    };
    let healing_context = HealingContext {
        node_id,
        rpc_client: rpc_client.clone(),
        gossip: gossip_handle.clone(),
        storage: storage_engine.clone(),
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
        ec_admin_service,
        ec_object_runtime,
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

/// One entry of `S4_POOL_NODES`, as configured and as resolved.
#[derive(Debug, Clone, PartialEq, Eq)]
struct PoolMember {
    /// The member's name, as the configuration spells it.
    name: String,
    /// The member's address exactly as configured, before resolution.
    configured_addr: String,
    /// The address it resolves to now, or the one this node last recorded.
    addr: SocketAddr,
    /// Whether `addr` came from the record rather than from resolution.
    ///
    /// A remembered address is a placeholder for a member that is away, not a
    /// statement about who is reachable there now.
    address_is_remembered: bool,
}

/// Parse the `S4_POOL_NODES` format: `name:host:port,name:host:port,...`
///
/// Each entry is a pool member defined as `<node_name>:<grpc_host>:<grpc_port>`.
///
/// `last_known` answers for entries whose address no longer resolves: the
/// address a member lived at does not change while it is away, and an
/// orchestrator that takes away the DNS name of a stopped container must not
/// take away every other node's ability to start.
async fn parse_pool_nodes(
    s: &str,
    last_known: &HashMap<String, SocketAddr>,
) -> Result<Vec<PoolMember>> {
    let mut result = Vec::new();
    let mut unreachable = Vec::new();

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
        let configured_addr = parts[1].to_string();

        // A name that does not resolve is not the same as a member this node
        // does not know. An orchestrator takes the DNS name of a stopped
        // container away, so the ordinary consequence of a neighbour being down
        // would otherwise be that nobody else may start (decision EC2-203).
        let mut remembered = false;
        let addr = match resolve_addr(&configured_addr).await {
            Ok(addr) => addr,
            Err(error) => match last_known.get(&configured_addr) {
                Some(addr) => {
                    tracing::warn!(
                        node = %name,
                        configured_addr = %configured_addr,
                        last_known = %addr,
                        %error,
                        "pool member address does not resolve; standing on the one this node \
                         recorded, and taking its identity from the record rather than from \
                         whoever answers there"
                    );
                    remembered = true;
                    *addr
                }
                None => {
                    unreachable.push(format!("{name}({configured_addr}): {error}"));
                    continue;
                }
            },
        };

        result.push(PoolMember {
            name,
            configured_addr,
            addr,
            address_is_remembered: remembered,
        });
    }

    if !unreachable.is_empty() {
        anyhow::bail!(
            "S4_POOL_NODES names members this node can neither resolve nor recall: {}",
            unreachable.join("; ")
        );
    }

    if result.is_empty() {
        anyhow::bail!("S4_POOL_NODES must contain at least one entry");
    }

    Ok(result)
}

/// How long the identity probe waits for one peer to answer.
///
/// Short on purpose. The probe runs on the startup path of every cluster node
/// and most peers answer in a millisecond or are not listening at all; a peer
/// that needs longer than this is one this node would not be able to work with
/// anyway, and it is asked again on the next start.
const IDENTITY_PROBE_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);

/// How long the identity probe waits for the `Health` answer once connected.
const IDENTITY_PROBE_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

/// Stop the start when another live pool member already answers for `node_id`.
///
/// Every configured pool address except this node's own is asked who it is. A
/// peer that does not answer proves nothing — it may be down, starting, or not
/// deployed yet — so silence is never read as "the identity is free"; only a
/// peer that answers *with this identity* refuses the start.
///
/// All peers are asked at once. Asked one after another, a pool whose peers are
/// unreachable rather than refusing would add the connect timeout to this
/// node's startup once per peer, and a restart of a wide pool would pay that on
/// every node in turn.
async fn refuse_start_on_identity_twin(
    node_id: NodeId,
    own_grpc_addr: SocketAddr,
    pool_node_addrs: &[(String, SocketAddr)],
) -> Result<()> {
    let mut probes = tokio::task::JoinSet::new();
    for (peer_name, peer_addr) in pool_node_addrs {
        if *peer_addr == own_grpc_addr {
            continue;
        }
        let peer_name = peer_name.clone();
        let peer_addr = *peer_addr;
        probes.spawn(async move {
            let answer = s4_cluster::probe_node_identity(
                peer_addr,
                IDENTITY_PROBE_CONNECT_TIMEOUT,
                IDENTITY_PROBE_REQUEST_TIMEOUT,
                None,
            )
            .await;
            (peer_name, peer_addr, answer)
        });
    }

    let probed = probes.len();
    let mut answered = 0usize;
    while let Some(probe) = probes.join_next().await {
        let (peer_name, peer_addr, answer) = probe.context("identity probe task failed")?;
        match answer {
            Ok(peer_identity) => {
                answered += 1;
                if peer_identity == node_id {
                    anyhow::bail!(
                        "node identity {node_id} is already held by a live member of this pool: \
                         {peer_name} at {peer_addr} answers for it. Two nodes under one identity \
                         write two histories of the same erasure-coded slots, and nothing in the \
                         pool can tell them apart afterwards. Stop the node at {peer_addr} before \
                         starting this one, or give this one its own identity and let repair \
                         rebuild its shards"
                    );
                }
            }
            Err(error) => {
                // Not evidence about the identity, so it is recorded and the
                // remaining answers are still collected. A pool where nothing
                // answers leaves the start exactly as permissive as it was
                // before this check existed.
                debug!(
                    peer = %peer_name,
                    addr = %peer_addr,
                    error = %error,
                    "identity probe got no answer from this pool address"
                );
            }
        }
    }

    info!(
        %node_id,
        probed,
        answered,
        "No live pool member answers for this node identity"
    );
    Ok(())
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

/// How many of the pool's nodes have to be visible before a start stops waiting
/// for the rest.
///
/// A simple majority, the same threshold the pool already uses to decide that
/// a metadata write stuck: `total / 2 + 1`. It is not a permission to start —
/// see [`name_pool_nodes`] — it is the point past which waiting for more of the
/// pool to appear stops buying anything.
fn pool_visibility_quorum(pool_size: usize) -> usize {
    pool_size / 2 + 1
}

/// The pool as one start managed to name it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct PoolRoll {
    /// Node identities in configured order, one per configured address.
    node_ids: Vec<NodeId>,
    /// Addresses named from the record because gossip did not show them.
    recalled: Vec<(String, SocketAddr)>,
}

impl PoolRoll {
    /// The recalled nodes as one line for a log.
    fn recalled_names(&self) -> String {
        self.recalled
            .iter()
            .map(|(name, addr)| format!("{name}({addr})"))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

/// Name every configured pool address, preferring the node that answers now
/// over the one this node remembers.
///
/// A live answer wins because the machine behind an address can be replaced
/// while this node is down, and the pool addresses the new identity from then
/// on. The record answers only for addresses nobody showed.
///
/// # Errors
///
/// Returns the configured names of the addresses that neither gossip nor the
/// record can name. A start cannot proceed on those: an unnamed member is a
/// slot owner this node cannot place, not a node that is merely down.
fn name_pool_nodes(
    members: &[PoolMember],
    seen_alive: &HashMap<SocketAddr, NodeId>,
    recalled_identities: &HashMap<String, NodeId>,
) -> Result<PoolRoll, Vec<String>> {
    let mut node_ids = Vec::with_capacity(members.len());
    let mut recalled = Vec::new();
    let mut unnamed = Vec::new();

    for member in members {
        // A member whose configured address no longer resolves carries the
        // address it had last, and that address may since have been handed to
        // somebody else. Whoever answers there answers for themselves, so an
        // entry standing on a remembered address is never named from gossip:
        // its identity comes from the record, under the configuration entry,
        // which is the one handle that does not move.
        let seen_alive_here =
            (!member.address_is_remembered).then(|| seen_alive.get(&member.addr)).flatten();

        if let Some(node_id) = seen_alive_here {
            node_ids.push(*node_id);
        } else if let Some(node_id) = recalled_identities.get(&member.configured_addr) {
            node_ids.push(*node_id);
            recalled.push((member.name.clone(), member.addr));
        } else {
            unnamed.push(format!("{}({})", member.name, member.configured_addr));
        }
    }

    if unnamed.is_empty() {
        Ok(PoolRoll { node_ids, recalled })
    } else {
        Err(unnamed)
    }
}

/// Wall clock in seconds since the Unix epoch, for records an operator reads.
fn unix_time_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|since| since.as_secs())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use s4_core::storage::ReadOptions;
    use s4_core::StorageEngine;
    use std::collections::HashMap;
    use tokio::io::AsyncReadExt;

    // -----------------------------------------------------------------------
    // Naming the pool (decision EC2-203)
    // -----------------------------------------------------------------------

    /// Members whose configured address resolved to itself.
    fn pool(names: &[(&str, u16)]) -> Vec<PoolMember> {
        names
            .iter()
            .map(|(name, port)| PoolMember {
                name: (*name).to_string(),
                configured_addr: at(*port).to_string(),
                addr: at(*port),
                address_is_remembered: false,
            })
            .collect()
    }

    /// One member standing on the address the record remembers for it.
    fn remembered(name: &str, configured: &str, port: u16) -> PoolMember {
        PoolMember {
            name: name.to_string(),
            configured_addr: configured.to_string(),
            addr: at(port),
            address_is_remembered: true,
        }
    }

    fn recalled(pairs: &[(&str, u128)]) -> HashMap<String, NodeId> {
        pairs
            .iter()
            .map(|(configured, id)| ((*configured).to_string(), NodeId(*id)))
            .collect()
    }

    fn at(port: u16) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], port))
    }

    fn ids(pairs: &[(u16, u128)]) -> HashMap<SocketAddr, NodeId> {
        pairs.iter().map(|(port, id)| (at(*port), NodeId(*id))).collect()
    }

    #[tokio::test]
    async fn a_pool_member_whose_name_stops_resolving_is_taken_from_the_record() {
        let last_known: HashMap<String, SocketAddr> =
            [("s4-node6:9100".to_string(), at(9106))].into_iter().collect();

        let members = parse_pool_nodes("node-1:127.0.0.1:9101,node-6:s4-node6:9100", &last_known)
            .await
            .expect("both members are named");

        assert_eq!(members.len(), 2);
        assert_eq!(members[1].name, "node-6");
        assert_eq!(members[1].configured_addr, "s4-node6:9100");
        // A stopped container loses its DNS name; where it lives does not
        // change while it is away.
        assert_eq!(members[1].addr, at(9106));
    }

    #[tokio::test]
    async fn a_member_that_neither_resolves_nor_is_recalled_refuses_the_start() {
        let error = parse_pool_nodes(
            "node-1:127.0.0.1:9101,node-6:s4-node6-never-seen.invalid:9100",
            &HashMap::new(),
        )
        .await
        .expect_err("node-6 cannot be named");

        let message = format!("{error:#}");
        assert!(message.contains("node-6"), "names the member: {message}");
        assert!(
            message.contains("neither resolve nor recall"),
            "says why: {message}"
        );
    }

    #[tokio::test]
    async fn an_address_that_resolves_is_not_taken_from_the_record() {
        // The machine behind an entry can be replaced while this node is down,
        // so what answers now wins over what was recorded.
        let last_known: HashMap<String, SocketAddr> =
            [("127.0.0.1:9101".to_string(), at(1))].into_iter().collect();

        let members = parse_pool_nodes("node-1:127.0.0.1:9101", &last_known).await.expect("named");

        assert_eq!(members[0].addr, at(9101));
    }

    #[test]
    fn the_visibility_quorum_is_a_majority_of_the_pool() {
        assert_eq!(pool_visibility_quorum(1), 1);
        assert_eq!(pool_visibility_quorum(2), 2);
        assert_eq!(pool_visibility_quorum(3), 2);
        assert_eq!(pool_visibility_quorum(6), 4);
        assert_eq!(pool_visibility_quorum(12), 7);
    }

    #[test]
    fn a_pool_whose_nodes_are_all_up_is_named_from_gossip_alone() {
        let addrs = pool(&[("node-1", 1), ("node-2", 2), ("node-3", 3)]);
        let alive = ids(&[(1, 0x11), (2, 0x22), (3, 0x33)]);

        let roll = name_pool_nodes(&addrs, &alive, &HashMap::new()).expect("all named");

        assert_eq!(
            roll.node_ids,
            vec![NodeId(0x11), NodeId(0x22), NodeId(0x33)]
        );
        assert!(roll.recalled.is_empty(), "nothing was recalled");
    }

    #[test]
    fn a_node_that_is_down_is_named_from_the_record_and_flagged() {
        let addrs = pool(&[("node-1", 1), ("node-2", 2), ("node-3", 3)]);
        let alive = ids(&[(1, 0x11), (2, 0x22)]);
        let recorded = recalled(&[("127.0.0.1:3", 0x33)]);

        let roll = name_pool_nodes(&addrs, &alive, &recorded).expect("all named");

        // The identity of a node that is down is the same identity it had when
        // it was up: the ring and the slot order do not move because a machine
        // went away.
        assert_eq!(
            roll.node_ids,
            vec![NodeId(0x11), NodeId(0x22), NodeId(0x33)]
        );
        assert_eq!(roll.recalled, vec![("node-3".to_string(), at(3))]);
        assert_eq!(roll.recalled_names(), "node-3(127.0.0.1:3)");
    }

    #[test]
    fn a_member_standing_on_a_remembered_address_is_not_named_by_whoever_answers_there() {
        // node-6 is down and its hostname is gone, so it stands on the address
        // it had last — which the orchestrator has since handed to node-4.
        let mut members = pool(&[("node-4", 4)]);
        members.push(remembered("node-6", "s4-node6:9100", 4));

        let alive = ids(&[(4, 0x44)]);
        let recorded = recalled(&[("s4-node6:9100", 0x66)]);

        let roll = name_pool_nodes(&members, &alive, &recorded).expect("all named");

        // Whoever answers at an address answers for themselves. Taking node-4's
        // identity for node-6 would give one machine two slots and leave the
        // other's unowned.
        assert_eq!(roll.node_ids, vec![NodeId(0x44), NodeId(0x66)]);
        assert_eq!(roll.recalled, vec![("node-6".to_string(), at(4))]);
    }

    #[test]
    fn remembered_address_reuse_preserves_user_and_system_placement() {
        let original = pool(&[
            ("n1", 1),
            ("n2", 2),
            ("n3", 3),
            ("n4", 4),
            ("n5", 5),
            ("n6", 6),
        ]);
        let mut reused = original[..5].to_vec();
        reused.push(remembered("n6", "n6:9100", 4));
        let alive = ids(&[(1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6)]);
        let recorded = recalled(&[("n6:9100", 6)]);
        let build = |members: &[PoolMember]| {
            let roll = name_pool_nodes(members, &alive, &recorded).unwrap();
            let config = ClusterConfig {
                pools: vec![PoolConfig {
                    pool_id: PoolId(0),
                    name: "pool".into(),
                    nodes: members
                        .iter()
                        .map(|member| NodeAddr {
                            grpc_addr: member.addr,
                            http_addr: member.addr,
                            gossip_addr: member.addr,
                        })
                        .collect(),
                    replication_factor: Some(3),
                    write_quorum: Some(2),
                    read_quorum: Some(2),
                    storage_class: s4_core::types::StorageClass::Standard,
                }],
                ..ClusterConfig::default()
            };
            PlacementRouter::from_pool_node_ids(
                &config,
                NodeId(1),
                BucketPlacement::new(),
                &HashMap::from([(PoolId(0), roll.node_ids)]),
            )
            .unwrap()
        };
        let before = build(&original);
        let after = build(&reused);
        for bucket in ["user-bucket", "__system__"] {
            assert_eq!(after.pool_nodes(bucket).unwrap().len(), 6);
            for index in 0..1_000 {
                let key = format!("key-{index}");
                assert_eq!(
                    before.route(bucket, &key).unwrap().replicas,
                    after.route(bucket, &key).unwrap().replicas
                );
            }
        }
    }

    #[test]
    fn a_live_answer_beats_a_remembered_one() {
        let addrs = pool(&[("node-1", 1), ("node-2", 2)]);
        let alive = ids(&[(1, 0x11), (2, 0xbeef)]);
        // The machine at node-2 was replaced while this node was down.
        let recorded = recalled(&[("127.0.0.1:1", 0x11), ("127.0.0.1:2", 0x22)]);

        let roll = name_pool_nodes(&addrs, &alive, &recorded).expect("all named");

        assert_eq!(roll.node_ids, vec![NodeId(0x11), NodeId(0xbeef)]);
        assert!(
            roll.recalled.is_empty(),
            "an address that answered is not recalled"
        );
    }

    #[test]
    fn an_address_nobody_can_name_refuses_the_start() {
        let addrs = pool(&[("node-1", 1), ("node-2", 2), ("node-3", 3)]);
        let alive = ids(&[(1, 0x11)]);
        let recorded = recalled(&[("127.0.0.1:2", 0x22)]);

        let unnamed = name_pool_nodes(&addrs, &alive, &recorded).expect_err("node-3 is unnamed");

        // Only the address nobody can name is reported: node-2 is down, but
        // being down is not the same as being unknown.
        assert_eq!(unnamed, vec!["node-3(127.0.0.1:3)".to_string()]);
    }

    #[test]
    fn the_pool_is_named_in_configured_order() {
        let addrs = pool(&[("node-3", 3), ("node-1", 1), ("node-2", 2)]);
        let alive = ids(&[(1, 0x11), (2, 0x22), (3, 0x33)]);

        let roll = name_pool_nodes(&addrs, &alive, &HashMap::new()).expect("all named");

        // Configured order is what every manifest written so far names, so the
        // roll follows it and not the order the map happens to iterate in.
        assert_eq!(
            roll.node_ids,
            vec![NodeId(0x33), NodeId(0x11), NodeId(0x22)]
        );
    }

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

    /// The adapter must forward both halves of the EC layout migration.
    ///
    /// The trait defaults refuse rather than do nothing, so a method this
    /// adapter forgets to delegate turns into a replica that answers "this
    /// storage engine does not support erasure-coded layout migration" — and
    /// the coordinator above it into a migration that can never reach quorum.
    /// One of the two was missed exactly this way.
    #[tokio::test]
    async fn the_adapter_forwards_the_ec_layout_migration_both_ways() {
        let (adapter, _temp) = test_adapter().await;
        let mut metadata = HashMap::new();
        metadata.insert("_s4_hlc_wall".to_string(), "10".to_string());
        metadata.insert("_s4_hlc_logical".to_string(), "0".to_string());
        metadata.insert("_s4_hlc_node_id".to_string(), "1".to_string());
        adapter
            .put_object("bucket", "object.bin", b"payload", "text/plain", &metadata)
            .await
            .expect("write the object");

        assert!(adapter
            .detach_object_data_if_hlc(
                "bucket",
                "object.bin",
                10,
                0,
                1,
                s4_core::types::ObjectLayout::ErasureCoded {
                    erasure_set_id: 1,
                    topology_epoch: 1,
                },
            )
            .await
            .expect("the detach must be delegated, not refused by the trait default"));

        assert!(adapter
            .attach_object_data_if_hlc("bucket", "object.bin", 10, 0, 1, b"payload")
            .await
            .expect("the attach must be delegated, not refused by the trait default"));

        let (body, record) = adapter.get_object("bucket", "object.bin").await.expect("read back");
        assert_eq!(body, b"payload");
        assert!(!record.is_erasure_coded());
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

    /// A node whose identity is already answered for by a live pool member has
    /// to stop, and a pool where nobody answers has to start exactly as before.
    #[tokio::test]
    async fn a_live_twin_stops_the_start_and_silence_does_not() {
        let twin_addr: SocketAddr = "127.0.0.1:19171".parse().unwrap();
        let own_addr: SocketAddr = "127.0.0.1:19172".parse().unwrap();
        // An address nothing is listening on, standing for a peer that is down.
        let quiet_addr: SocketAddr = "127.0.0.1:19173".parse().unwrap();
        let node_id = s4_cluster::NodeId::generate();

        // Nothing is up yet: no answer is not an answer, and the start proceeds.
        refuse_start_on_identity_twin(
            node_id,
            own_addr,
            &[
                ("twin".to_string(), twin_addr),
                ("quiet".to_string(), quiet_addr),
            ],
        )
        .await
        .expect("a pool where nobody answers must start as it did before");

        let twin = s4_cluster::NodeRpcServer::new(
            node_id,
            Arc::new(NodeMeta {
                node_id,
                node_name: "twin".into(),
                addr: NodeAddr {
                    grpc_addr: twin_addr,
                    http_addr: "127.0.0.1:19174".parse().unwrap(),
                    gossip_addr: twin_addr,
                },
                pool_id: PoolId::new(0),
                topology_epoch: 1,
                protocol_version: PROTOCOL_VERSION,
                capacity: s4_cluster::NodeCapacity::default(),
                started_at: 1_700_000_000,
                s4_version: S4_VERSION.to_string(),
                edition: Edition::Community,
            }),
            Arc::new(s4_cluster::GossipHandle::new_for_testing(HashMap::new())),
        );
        let twin_handle = twin
            .start(RpcServerConfig {
                bind_addr: twin_addr,
                ..Default::default()
            })
            .await
            .expect("start the twin");

        let refusal = refuse_start_on_identity_twin(
            node_id,
            own_addr,
            &[
                ("twin".to_string(), twin_addr),
                ("quiet".to_string(), quiet_addr),
            ],
        )
        .await
        .expect_err("a live member answering for this identity must stop the start");
        let refusal = refusal.to_string();
        // The operator has to be told where the other claimant is: that is the
        // one machine they have to look at.
        assert!(refusal.contains(&twin_addr.to_string()), "{refusal}");
        assert!(refusal.contains(&node_id.to_string()), "{refusal}");

        // A peer that answers for a different identity is an ordinary peer.
        let other_id = s4_cluster::NodeId::generate();
        refuse_start_on_identity_twin(other_id, own_addr, &[("twin".to_string(), twin_addr)])
            .await
            .expect("a peer holding another identity is not a twin");

        twin_handle.shutdown().await.expect("stop the twin");
    }
}
