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

//! Inter-node gRPC communication for S4 cluster.
//!
//! This module provides:
//! - **Server**: gRPC service running on each node (default port 9100)
//! - **Client**: Connection pool with retry logic for calling peer nodes
//! - **TLS**: Optional mutual TLS for inter-node traffic
//!
//! # Architecture
//!
//! The coordinator node (whichever node received the S3 HTTP request) fans out
//! to replica nodes via these RPCs. Every node runs both a server (to receive
//! replica requests) and holds a client pool (to send requests to peers).
//!
//! Data plane operations (WriteBlob, ReadBlob, DeleteBlob) will be wired to the
//! storage engine in Phase 4+. In Phase 2, the infrastructure is established
//! with Health endpoint fully operational.

pub mod client;
pub mod server;
pub mod tls;

/// Generated protobuf types and gRPC service definitions.
pub mod proto {
    #![allow(missing_docs, clippy::large_enum_variant)]
    tonic::include_proto!("s4.cluster");
}

pub use client::{NodeClient, NodeClientConfig};
pub use server::{NodeRpcServer, RpcServerConfig};
pub use tls::TlsConfig;

/// Default gRPC port for inter-node communication.
pub const DEFAULT_GRPC_PORT: u16 = 9100;

/// Default maximum message size for non-streaming RPCs (16 MB).
pub const DEFAULT_MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;

/// Threshold above which streaming RPCs should be used instead of unary (8 MB).
pub const STREAMING_THRESHOLD: usize = 8 * 1024 * 1024;

/// Default chunk size for streaming transfers (256 KB).
pub const DEFAULT_CHUNK_SIZE: usize = 256 * 1024;
