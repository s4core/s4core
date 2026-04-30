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

//! gRPC client pool for inter-node communication.
//!
//! [`NodeClient`] maintains persistent gRPC connections to peer nodes,
//! discovers addresses via the gossip layer, and provides retry logic
//! with exponential backoff.
//!
//! # Retry Policy
//!
//! - Max retries: 3
//! - Backoff: 100ms -> 200ms -> 400ms (exponential)
//! - Dead nodes (per gossip) are not retried — fail immediately
//! - Non-retryable errors (InvalidArgument, Unauthenticated) are not retried

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use tokio_stream::Stream;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Response, Status};
use tracing::{debug, trace, warn};

use crate::error::ClusterError;
use crate::gossip::GossipHandle;
use crate::identity::{NodeId, NodeStatus};
use crate::rpc::proto::s4_node_client::S4NodeClient;
use crate::rpc::proto::{self};
use crate::rpc::tls::TlsConfig;
use crate::rpc::DEFAULT_MAX_MESSAGE_SIZE;

/// Configuration for the gRPC client pool.
#[derive(Debug, Clone)]
pub struct NodeClientConfig {
    /// Connection timeout for establishing new gRPC channels.
    pub connect_timeout: Duration,
    /// Per-request timeout.
    pub request_timeout: Duration,
    /// Maximum number of retry attempts for failed requests.
    pub max_retries: u32,
    /// Initial retry backoff duration (doubles each attempt).
    pub initial_backoff: Duration,
    /// Maximum inbound/outbound message size.
    pub max_message_size: usize,
    /// Optional TLS configuration for connecting to peers.
    pub tls: Option<TlsConfig>,
}

impl Default for NodeClientConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(10 * 60),
            max_retries: 3,
            initial_backoff: Duration::from_millis(100),
            max_message_size: DEFAULT_MAX_MESSAGE_SIZE,
            tls: None,
        }
    }
}

/// gRPC client pool for communicating with peer nodes.
///
/// Maintains persistent connections keyed by [`NodeId`]. Connections are
/// lazily created on first use and cached for subsequent requests.
/// Node addresses are resolved via the [`GossipHandle`].
pub struct NodeClient {
    connections: DashMap<NodeId, S4NodeClient<Channel>>,
    gossip: Arc<GossipHandle>,
    config: NodeClientConfig,
}

impl NodeClient {
    /// Create a new client pool.
    pub fn new(gossip: Arc<GossipHandle>, config: NodeClientConfig) -> Self {
        Self {
            connections: DashMap::new(),
            gossip,
            config,
        }
    }

    /// Send a `WriteBlob` request to a specific node with retry.
    pub async fn send_write(
        &self,
        node_id: NodeId,
        request: proto::WriteBlobRequest,
    ) -> Result<proto::WriteBlobResponse, ClusterError> {
        self.with_retry(node_id, |mut client| {
            let req = request.clone();
            async move { client.write_blob(req).await }
        })
        .await
    }

    /// Send a `ReadBlob` request to a specific node with retry.
    pub async fn send_read(
        &self,
        node_id: NodeId,
        request: proto::ReadBlobRequest,
    ) -> Result<proto::ReadBlobResponse, ClusterError> {
        self.with_retry(node_id, |mut client| {
            let req = request.clone();
            async move { client.read_blob(req).await }
        })
        .await
    }

    /// Send a `DeleteBlob` request to a specific node with retry.
    pub async fn send_delete(
        &self,
        node_id: NodeId,
        request: proto::DeleteBlobRequest,
    ) -> Result<proto::DeleteBlobResponse, ClusterError> {
        self.with_retry(node_id, |mut client| {
            let req = request.clone();
            async move { client.delete_blob(req).await }
        })
        .await
    }

    /// Send a multipart create-session request to a specific node with retry.
    pub async fn send_multipart_create(
        &self,
        node_id: NodeId,
        request: proto::CreateMultipartUploadReplicaRequest,
    ) -> Result<proto::CreateMultipartUploadReplicaResponse, ClusterError> {
        self.with_retry(node_id, |mut client| {
            let req = request.clone();
            async move { client.create_multipart_upload_replica(req).await }
        })
        .await
    }

    /// Send a streaming multipart upload-part request to a specific node.
    pub async fn send_multipart_upload_part_stream(
        &self,
        node_id: NodeId,
        stream: impl Stream<Item = proto::UploadMultipartPartReplicaChunk> + Send + 'static,
    ) -> Result<proto::UploadMultipartPartReplicaResponse, ClusterError> {
        self.check_node_alive(node_id)?;
        let mut client = self.get_or_connect(node_id).await?;
        let mut request = Request::new(stream);
        request.set_timeout(self.config.request_timeout);
        let response = client
            .upload_multipart_part_replica(request)
            .await
            .map_err(|e| ClusterError::Rpc(format!("multipart part stream to {node_id}: {e}")))?;
        Ok(response.into_inner())
    }

    /// Inspect multipart session and part metadata on a specific node.
    pub async fn send_multipart_inspect(
        &self,
        node_id: NodeId,
        request: proto::InspectMultipartUploadReplicaRequest,
    ) -> Result<proto::InspectMultipartUploadReplicaResponse, ClusterError> {
        self.with_retry(node_id, |mut client| {
            let req = request.clone();
            async move { client.inspect_multipart_upload_replica(req).await }
        })
        .await
    }

    /// Send a multipart complete request to a specific node with retry.
    pub async fn send_multipart_complete(
        &self,
        node_id: NodeId,
        request: proto::CompleteMultipartUploadReplicaRequest,
    ) -> Result<proto::CompleteMultipartUploadReplicaResponse, ClusterError> {
        self.with_retry(node_id, |mut client| {
            let req = request.clone();
            async move { client.complete_multipart_upload_replica(req).await }
        })
        .await
    }

    /// Send a multipart abort request to a specific node with retry.
    pub async fn send_multipart_abort(
        &self,
        node_id: NodeId,
        request: proto::AbortMultipartUploadReplicaRequest,
    ) -> Result<proto::AbortMultipartUploadReplicaResponse, ClusterError> {
        self.with_retry(node_id, |mut client| {
            let req = request.clone();
            async move { client.abort_multipart_upload_replica(req).await }
        })
        .await
    }

    /// Send a `WriteMetadata` request to a specific node with retry.
    pub async fn send_write_metadata(
        &self,
        node_id: NodeId,
        request: proto::WriteMetadataRequest,
    ) -> Result<proto::WriteMetadataResponse, ClusterError> {
        self.with_retry(node_id, |mut client| {
            let req = request.clone();
            async move { client.write_metadata(req).await }
        })
        .await
    }

    /// Send a `ReadMetadata` request to a specific node with retry.
    pub async fn send_read_metadata(
        &self,
        node_id: NodeId,
        request: proto::ReadMetadataRequest,
    ) -> Result<proto::ReadMetadataResponse, ClusterError> {
        self.with_retry(node_id, |mut client| {
            let req = request.clone();
            async move { client.read_metadata(req).await }
        })
        .await
    }

    /// Send a `HeadObject` request to a specific node with retry.
    pub async fn send_head(
        &self,
        node_id: NodeId,
        request: proto::HeadObjectRequest,
    ) -> Result<proto::HeadObjectResponse, ClusterError> {
        self.with_retry(node_id, |mut client| {
            let req = request.clone();
            async move { client.head_object(req).await }
        })
        .await
    }

    /// Send a `ListObjects` request to a specific node with retry.
    pub async fn send_list_objects(
        &self,
        node_id: NodeId,
        request: proto::ListObjectsRequest,
    ) -> Result<proto::ListObjectsResponse, ClusterError> {
        self.with_retry(node_id, |mut client| {
            let req = request.clone();
            async move { client.list_objects(req).await }
        })
        .await
    }

    /// Send a `Health` request to a specific node with retry.
    pub async fn send_health(
        &self,
        node_id: NodeId,
    ) -> Result<proto::HealthResponse, ClusterError> {
        self.with_retry(node_id, |mut client| async move {
            client.health(proto::HealthRequest {}).await
        })
        .await
    }

    /// Send a `DeliverHint` request to a specific node with retry.
    pub async fn send_hint(
        &self,
        node_id: NodeId,
        request: proto::HintRequest,
    ) -> Result<proto::HintResponse, ClusterError> {
        self.with_retry(node_id, |mut client| {
            let req = request.clone();
            async move { client.deliver_hint(req).await }
        })
        .await
    }

    /// Send an `ExchangeMerkleDigest` request to a specific node with retry.
    pub async fn send_merkle_digest(
        &self,
        node_id: NodeId,
        request: proto::MerkleDigestRequest,
    ) -> Result<proto::MerkleDigestResponse, ClusterError> {
        self.with_retry(node_id, |mut client| {
            let req = request.clone();
            async move { client.exchange_merkle_digest(req).await }
        })
        .await
    }

    /// Send a `RepairSync` request and return the response stream.
    pub async fn send_repair_sync(
        &self,
        node_id: NodeId,
        request: proto::RepairSyncRequest,
    ) -> Result<tonic::Streaming<proto::RepairSyncResponse>, ClusterError> {
        let mut client = self.get_or_connect(node_id).await?;
        let response = client
            .repair_sync(request)
            .await
            .map_err(|e| ClusterError::Rpc(format!("repair sync to {node_id}: {e}")))?;
        Ok(response.into_inner())
    }

    /// Start a streaming write to a node (for objects > 8MB).
    pub async fn send_write_stream(
        &self,
        node_id: NodeId,
        stream: impl Stream<Item = proto::WriteBlobChunk> + Send + 'static,
    ) -> Result<proto::WriteBlobResponse, ClusterError> {
        let mut client = self.get_or_connect(node_id).await?;
        let response = client
            .write_blob_stream(Request::new(stream))
            .await
            .map_err(|e| ClusterError::Rpc(format!("streaming write to {node_id}: {e}")))?;
        Ok(response.into_inner())
    }

    /// Start a streaming read from a node (for objects > 8MB).
    pub async fn send_read_stream(
        &self,
        node_id: NodeId,
        request: proto::ReadBlobRequest,
    ) -> Result<tonic::Streaming<proto::ReadBlobChunk>, ClusterError> {
        let mut client = self.get_or_connect(node_id).await?;
        let response = client
            .read_blob_stream(request)
            .await
            .map_err(|e| ClusterError::Rpc(format!("streaming read from {node_id}: {e}")))?;
        Ok(response.into_inner())
    }

    /// Remove a cached connection (e.g., when a node is declared dead).
    pub fn disconnect(&self, node_id: &NodeId) {
        if self.connections.remove(node_id).is_some() {
            debug!(%node_id, "removed cached gRPC connection");
        }
    }

    /// Return the number of cached connections.
    pub fn connection_count(&self) -> usize {
        self.connections.len()
    }

    // ========================================================================
    // Internal helpers
    // ========================================================================

    /// Execute an RPC with retry and exponential backoff.
    async fn with_retry<F, Fut, R>(&self, node_id: NodeId, make_call: F) -> Result<R, ClusterError>
    where
        F: Fn(S4NodeClient<Channel>) -> Fut,
        Fut: std::future::Future<Output = Result<Response<R>, Status>>,
    {
        // Fast-fail if gossip says the node is dead.
        self.check_node_alive(node_id)?;

        let mut backoff = self.config.initial_backoff;

        for attempt in 0..=self.config.max_retries {
            let client = self.get_or_connect(node_id).await?;

            match make_call(client).await {
                Ok(response) => return Ok(response.into_inner()),
                Err(status) => {
                    if !is_retryable(&status) || attempt == self.config.max_retries {
                        // Evict bad connections on transport errors.
                        if is_connection_error(&status) {
                            self.disconnect(&node_id);
                        }
                        return Err(ClusterError::Rpc(format!(
                            "RPC to {node_id} failed after {} attempts: {}",
                            attempt + 1,
                            status,
                        )));
                    }

                    warn!(
                        %node_id,
                        attempt = attempt + 1,
                        max = self.config.max_retries,
                        status = %status,
                        backoff_ms = backoff.as_millis(),
                        "retrying RPC"
                    );

                    // Evict on connection-level errors before retry.
                    if is_connection_error(&status) {
                        self.disconnect(&node_id);
                    }

                    tokio::time::sleep(backoff).await;
                    backoff *= 2;
                }
            }
        }

        unreachable!("loop always returns")
    }

    /// Get a cached connection or create a new one.
    async fn get_or_connect(&self, node_id: NodeId) -> Result<S4NodeClient<Channel>, ClusterError> {
        // Fast path: return cached connection.
        if let Some(client) = self.connections.get(&node_id) {
            trace!(%node_id, "reusing cached gRPC connection");
            return Ok(client.clone());
        }

        // Resolve address from gossip.
        let addr = self.resolve_grpc_addr(node_id)?;
        let channel = self.connect_to(addr).await?;
        let client = S4NodeClient::new(channel)
            .max_decoding_message_size(self.config.max_message_size)
            .max_encoding_message_size(self.config.max_message_size);

        self.connections.insert(node_id, client.clone());
        debug!(%node_id, %addr, "established gRPC connection");
        Ok(client)
    }

    /// Create a gRPC channel to the given address.
    async fn connect_to(&self, addr: SocketAddr) -> Result<Channel, ClusterError> {
        let scheme = if self.config.tls.is_some() {
            "https"
        } else {
            "http"
        };
        let uri = format!("{scheme}://{addr}");

        let mut endpoint = Endpoint::from_shared(uri)
            .map_err(|e| ClusterError::Rpc(format!("invalid endpoint: {e}")))?
            .connect_timeout(self.config.connect_timeout)
            .timeout(self.config.request_timeout);

        if let Some(tls) = &self.config.tls {
            let tls_cfg = tls.client_config()?;
            endpoint = endpoint
                .tls_config(tls_cfg)
                .map_err(|e| ClusterError::Tls(format!("client TLS config error: {e}")))?;
        }

        endpoint
            .connect()
            .await
            .map_err(|e| ClusterError::Rpc(format!("failed to connect to {addr}: {e}")))
    }

    /// Resolve the gRPC address for a node via gossip.
    fn resolve_grpc_addr(&self, node_id: NodeId) -> Result<SocketAddr, ClusterError> {
        let state = self.gossip.get_node(&node_id).ok_or_else(|| {
            ClusterError::Rpc(format!("node {node_id} not found in gossip membership"))
        })?;
        Ok(state.meta.addr.grpc_addr)
    }

    /// Fast-fail if the node is known to be dead.
    fn check_node_alive(&self, node_id: NodeId) -> Result<(), ClusterError> {
        if let Some(state) = self.gossip.get_node(&node_id) {
            if matches!(state.status, NodeStatus::Dead | NodeStatus::Left) {
                self.disconnect(&node_id);
                return Err(ClusterError::Rpc(format!(
                    "node {node_id} is {:?}, skipping RPC",
                    state.status
                )));
            }
        }
        Ok(())
    }
}

/// Determine if a gRPC status code is retryable.
fn is_retryable(status: &Status) -> bool {
    matches!(
        status.code(),
        tonic::Code::Unavailable
            | tonic::Code::DeadlineExceeded
            | tonic::Code::ResourceExhausted
            | tonic::Code::Aborted
            | tonic::Code::Internal
            | tonic::Code::Unknown
    )
}

/// Determine if a gRPC status indicates a connection-level error.
fn is_connection_error(status: &Status) -> bool {
    matches!(
        status.code(),
        tonic::Code::Unavailable | tonic::Code::Unknown
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gossip::{GossipHandle, NodeState};
    use crate::identity::{NodeAddr, NodeCapacity, NodeMeta, PoolId};
    use std::collections::HashMap;

    fn test_node(node_id: NodeId, grpc_port: u16) -> NodeState {
        NodeState {
            meta: NodeMeta {
                node_id,
                node_name: "test-node".into(),
                addr: NodeAddr {
                    grpc_addr: SocketAddr::from(([127, 0, 0, 1], grpc_port)),
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
            status: NodeStatus::Alive,
            last_seen: std::time::Instant::now(),
        }
    }

    fn dead_node(node_id: NodeId) -> NodeState {
        let mut state = test_node(node_id, 9100);
        state.status = NodeStatus::Dead;
        state
    }

    #[test]
    fn is_retryable_categorization() {
        assert!(is_retryable(&Status::unavailable("down")));
        assert!(is_retryable(&Status::deadline_exceeded("slow")));
        assert!(is_retryable(&Status::resource_exhausted("busy")));
        assert!(is_retryable(&Status::aborted("conflict")));
        assert!(is_retryable(&Status::internal("oops")));

        assert!(!is_retryable(&Status::invalid_argument("bad")));
        assert!(!is_retryable(&Status::not_found("missing")));
        assert!(!is_retryable(&Status::permission_denied("no")));
        assert!(!is_retryable(&Status::unimplemented("nope")));
    }

    #[test]
    fn dead_node_fails_immediately() {
        let node_id = NodeId::generate();
        let mut members = HashMap::new();
        members.insert(node_id, dead_node(node_id));

        let gossip = Arc::new(GossipHandle::new_for_testing(members));
        let client = NodeClient::new(gossip, NodeClientConfig::default());

        let result = client.check_node_alive(node_id);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Dead"));
    }

    #[test]
    fn alive_node_passes_check() {
        let node_id = NodeId::generate();
        let mut members = HashMap::new();
        members.insert(node_id, test_node(node_id, 9100));

        let gossip = Arc::new(GossipHandle::new_for_testing(members));
        let client = NodeClient::new(gossip, NodeClientConfig::default());

        assert!(client.check_node_alive(node_id).is_ok());
    }

    #[test]
    fn unknown_node_passes_alive_check() {
        // If gossip doesn't know the node, we let the connection attempt proceed.
        // This handles bootstrap scenarios where gossip hasn't converged yet.
        let gossip = Arc::new(GossipHandle::new_for_testing(HashMap::new()));
        let client = NodeClient::new(gossip, NodeClientConfig::default());

        let unknown_id = NodeId::generate();
        assert!(client.check_node_alive(unknown_id).is_ok());
    }

    #[test]
    fn resolve_addr_for_known_node() {
        let node_id = NodeId::generate();
        let port = 19150;
        let mut members = HashMap::new();
        members.insert(node_id, test_node(node_id, port));

        let gossip = Arc::new(GossipHandle::new_for_testing(members));
        let client = NodeClient::new(gossip, NodeClientConfig::default());

        let addr = client.resolve_grpc_addr(node_id).unwrap();
        assert_eq!(addr.port(), port);
    }

    #[test]
    fn resolve_addr_for_unknown_node_fails() {
        let gossip = Arc::new(GossipHandle::new_for_testing(HashMap::new()));
        let client = NodeClient::new(gossip, NodeClientConfig::default());

        let result = client.resolve_grpc_addr(NodeId::generate());
        assert!(result.is_err());
    }

    #[test]
    fn disconnect_removes_cached_connection() {
        let gossip = Arc::new(GossipHandle::new_for_testing(HashMap::new()));
        let client = NodeClient::new(gossip, NodeClientConfig::default());

        assert_eq!(client.connection_count(), 0);

        // Disconnect a non-existent connection is a no-op.
        client.disconnect(&NodeId::generate());
        assert_eq!(client.connection_count(), 0);
    }

    #[tokio::test]
    async fn health_check_to_running_server() {
        use crate::rpc::server::{NodeRpcServer, RpcServerConfig};

        let node_id = NodeId::generate();
        let meta = Arc::new(NodeMeta {
            node_id,
            node_name: "health-test-node".into(),
            addr: NodeAddr {
                grpc_addr: "127.0.0.1:19160".parse().unwrap(),
                http_addr: "127.0.0.1:19000".parse().unwrap(),
                gossip_addr: "127.0.0.1:19200".parse().unwrap(),
            },
            pool_id: PoolId::new(1),
            topology_epoch: 1,
            protocol_version: 1,
            capacity: NodeCapacity {
                total_bytes: 500_000_000,
                used_bytes: 50_000_000,
                volume_count: 3,
                object_count: 500,
            },
            started_at: 1700000000,
            s4_version: "0.1.0".into(),
            edition: crate::identity::Edition::Community,
        });

        // Start a server for the node.
        let server_gossip = Arc::new(GossipHandle::new_for_testing(HashMap::new()));
        let server = NodeRpcServer::new(node_id, meta.clone(), server_gossip);
        let server_handle = server
            .start(RpcServerConfig {
                bind_addr: "127.0.0.1:19160".parse().unwrap(),
                ..Default::default()
            })
            .await
            .unwrap();

        // Create a client that knows about this node.
        let mut members = HashMap::new();
        members.insert(
            node_id,
            NodeState {
                meta: (*meta).clone(),
                status: NodeStatus::Alive,
                last_seen: std::time::Instant::now(),
            },
        );
        let client_gossip = Arc::new(GossipHandle::new_for_testing(members));
        let client = NodeClient::new(client_gossip, NodeClientConfig::default());

        // Call health via the client pool.
        let health = client.send_health(node_id).await.unwrap();
        assert_eq!(health.node_name, "health-test-node");
        assert_eq!(health.pool_id, 1);
        assert_eq!(health.status, proto::NodeHealthStatus::Healthy as i32);

        server_handle.shutdown().await.unwrap();
    }
}
