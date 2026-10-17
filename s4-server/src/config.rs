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

//! Configuration management for S4 server.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use crate::secrets::{secret_from_env, SecretError};
use s4_features::iam::jwt::generate_secret;

/// Server configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// HTTP server settings (bind address, TLS, etc.)
    pub server: ServerConfig,
    /// Storage engine configuration (paths, volumes)
    pub storage: StorageConfig,
    /// Performance tuning options
    pub tuning: TuningConfig,
    /// Feature flags
    pub features: FeaturesConfig,
    /// Security settings (authentication, audit logging)
    pub security: SecurityConfig,
    /// Metrics and monitoring configuration
    pub metrics: MetricsConfig,
    /// Lifecycle worker configuration
    pub lifecycle: LifecycleConfig,
    /// Volume compaction worker configuration
    pub compaction: CompactionConfig,
    /// Multipart upload cleanup configuration
    pub multipart: MultipartCleanupConfig,
    /// Cluster mode configuration (Phase 10).
    pub cluster: ClusterModeConfig,
}

/// Operating mode of the server.
///
/// Determined by the `S4_MODE` environment variable:
/// - `single` (default): standalone server, no clustering
/// - `cluster`: full cluster member with storage and quorum
/// - `gateway`: stateless router that proxies to cluster nodes
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ServerMode {
    /// Standalone server, no clustering. Existing deployments work unchanged.
    #[default]
    Single,
    /// Cluster member with storage, gossip, gRPC, and quorum operations.
    Cluster,
    /// Stateless gateway that routes requests to cluster nodes (no local storage).
    Gateway,
}

/// Cluster mode configuration, loaded from `S4_*` environment variables.
///
/// Only relevant when `mode == ServerMode::Cluster` or `ServerMode::Gateway`.
/// In `Single` mode, all cluster fields are ignored.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterModeConfig {
    /// Operating mode (`S4_MODE`).
    pub mode: ServerMode,

    /// Cluster name for network isolation (`S4_CLUSTER_NAME`).
    pub cluster_name: String,

    /// Stable node identifier (`S4_NODE_ID`). Auto-generated on first run if not set.
    pub node_id: Option<String>,

    /// Cluster identity this data directory must belong to
    /// (`S4_EXPECTED_NODE_IDENTITY`, a UUID).
    ///
    /// Not the same thing as [`Self::node_id`], which is only a display name.
    /// This is the persistent identity in `<data dir>/node_id` that the pool
    /// assigns erasure-coded slots to. Set during node replacement: an empty
    /// data directory adopts it, and a directory holding a different identity
    /// stops the start.
    pub expected_node_identity: Option<String>,

    /// Whether to refuse the start when a live pool member already answers for
    /// this node's identity (`S4_NODE_IDENTITY_CONFLICT_CHECK`, default `true`).
    ///
    /// Two machines under one identity are two histories of the same slot
    /// owner, and every outcome of that is worse than a refused start. The
    /// switch exists so the check can be taken out of the way during a recovery
    /// where the operator already knows what is running where.
    pub identity_conflict_check: bool,

    /// gRPC address for inter-node communication (`S4_NODE_GRPC_ADDR`).
    pub grpc_addr: Option<String>,

    /// HTTP address advertised to other nodes (`S4_NODE_HTTP_ADDR`).
    pub http_addr: Option<String>,

    /// Seed nodes for gossip discovery (`S4_SEEDS`, comma-separated).
    pub seeds: Vec<String>,

    /// Pool name this node belongs to (`S4_POOL_NAME`).
    pub pool_name: Option<String>,

    /// Pool node definitions (`S4_POOL_NODES`, format: `id:addr,id:addr,...`).
    pub pool_nodes: Option<String>,

    /// Pool storage type (`S4_POOL_TYPE`, default: `standard`; EE EC uses `ec`).
    pub pool_type: String,

    /// Replication factor (`S4_REPLICATION_FACTOR`, default: 3).
    pub replication_factor: u8,

    /// Write quorum size (`S4_WRITE_QUORUM`, default: 2).
    pub write_quorum: u8,

    /// Read quorum size (`S4_READ_QUORUM`, default: 2).
    pub read_quorum: u8,

    /// GC grace period in days (`S4_GC_GRACE_DAYS`, default: 7).
    pub gc_grace_days: u32,

    /// GC grace period in seconds (`S4_GC_GRACE_SECS`). When set, it is the
    /// grace period and `S4_GC_GRACE_DAYS` is not read.
    pub gc_grace_secs: Option<u64>,

    /// Seconds between two tombstone GC passes
    /// (`S4_TOMBSTONE_GC_INTERVAL_SECS`, default: 21600, six hours).
    pub tombstone_gc_interval_secs: u64,

    /// Maximum downtime (days) before a node must full-bootstrap (`S4_MAX_REJOIN_DOWNTIME_DAYS`).
    pub max_rejoin_downtime_days: u32,

    /// Anti-entropy interval in seconds (`S4_ANTI_ENTROPY_INTERVAL_SECS`, default: 600).
    pub anti_entropy_interval_secs: u64,

    /// Whether this node starts anti-entropy rounds (`S4_ANTI_ENTROPY_ENABLED`,
    /// default: true). A node that does not still answers its peers' rounds.
    /// `Err` holds a value that is neither on nor off: [`Self::validate`]
    /// refuses it, so that a kill switch set with a typo is not taken either
    /// way.
    pub anti_entropy_enabled: Result<bool, String>,

    /// Most keys one anti-entropy round settles
    /// (`S4_ANTI_ENTROPY_MAX_KEYS_PER_ROUND`, default: 1000).
    pub anti_entropy_max_keys_per_round: u32,

    /// Scrubber full-scan interval in days (`S4_SCRUBBER_FULL_SCAN_DAYS`, default: 30).
    pub scrubber_full_scan_days: u32,

    /// Delay before the scrubber's first cycle, in seconds
    /// (`S4_SCRUBBER_START_DELAY_SECS`, default: 30).
    ///
    /// Healing a damaged blob means fetching a healthy copy from a peer, and a
    /// node that has just started has not finished hearing about its peers.
    pub scrubber_start_delay_secs: u64,

    /// How soon the scrubber runs again after a cycle that left damage
    /// unhealed, in seconds (`S4_SCRUBBER_UNHEALED_RETRY_SECS`, default: 300).
    pub scrubber_unhealed_retry_secs: u64,

    /// Hinted handoff TTL in hours (`S4_HINT_TTL_HOURS`, default: 3).
    pub hint_ttl_hours: u32,

    /// How long a node grants the lease of IAM changes for, in seconds
    /// (`S4_IAM_LEASE_SECS`, default: 5, at least 1).
    pub iam_lease_secs: u64,

    /// How long a change of IAM waits for a lease another node holds before
    /// it is refused with 503 and `Retry-After`, in seconds
    /// (`S4_IAM_LEASE_WAIT_SECS`, default: 10).
    pub iam_lease_wait_secs: u64,

    /// How long a node checks IAM credentials against its own copy of IAM
    /// after a majority of its pool last agreed with it, in seconds
    /// (`S4_IAM_MAX_STALENESS_SECS`, default: 30); past it, they are refused
    /// with 503 until a majority agrees again. `0` takes the bound away.
    pub iam_max_staleness_secs: u64,

    /// Drain timeout in seconds for graceful shutdown (`S4_DRAIN_TIMEOUT_SECS`, default: 30).
    pub drain_timeout_secs: u64,

    /// Replicas of every bucket's metadata in the pool
    /// (`S4_BUCKET_METADATA_REPLICATION_FACTOR`), above the number derived
    /// from the failures the pool survives; unset, the derived number. `Err`
    /// holds a value that is not a number: [`Self::validate`] refuses it.
    pub bucket_metadata_replication_factor: Result<Option<u8>, String>,

    /// Bytes of copies a node sends per second while a grown pool moves its
    /// keys to their new owners (`S4_REBALANCE_MAX_BYTES_PER_SEC`, default:
    /// 67108864, 64 MiB).
    pub rebalance_max_bytes_per_sec: u64,

    /// Concurrent single-to-cluster records (`S4_CLUSTER_MIGRATION_CONCURRENCY`, default 4).
    pub migration_concurrency: Result<usize, String>,

    /// Keys a node settles per second while a grown pool moves its keys
    /// (`S4_REBALANCE_MAX_KEYS_PER_SEC`, default: 500).
    pub rebalance_max_keys_per_sec: u32,

    /// Ranges of the ring a node moves at the same time
    /// (`S4_REBALANCE_CONCURRENT_RANGES`, default: 2).
    pub rebalance_concurrent_ranges: usize,

    /// Share of its disk a node receiving keys keeps free
    /// (`S4_REBALANCE_MIN_FREE_RATIO`, default: 0.10): the move does not begin
    /// while a new node lacks room for its share with this much left over,
    /// and ranges sending to a node below it wait.
    pub rebalance_min_free_ratio: f64,
}

impl Default for ClusterModeConfig {
    fn default() -> Self {
        let mode = match std::env::var("S4_MODE").as_deref() {
            Ok("cluster") => ServerMode::Cluster,
            Ok("gateway") => ServerMode::Gateway,
            _ => ServerMode::Single,
        };

        let seeds = std::env::var("S4_SEEDS")
            .unwrap_or_default()
            .split(',')
            .map(|s| s.trim().to_owned())
            .filter(|s| !s.is_empty())
            .collect();

        Self {
            mode,
            cluster_name: std::env::var("S4_CLUSTER_NAME")
                .unwrap_or_else(|_| "s4-cluster".to_string()),
            node_id: std::env::var("S4_NODE_ID").ok(),
            expected_node_identity: std::env::var("S4_EXPECTED_NODE_IDENTITY")
                .ok()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty()),
            identity_conflict_check: std::env::var("S4_NODE_IDENTITY_CONFLICT_CHECK")
                .map(|s| s.to_lowercase() == "true" || s == "1")
                .unwrap_or(true),
            grpc_addr: std::env::var("S4_NODE_GRPC_ADDR").ok(),
            http_addr: std::env::var("S4_NODE_HTTP_ADDR").ok(),
            seeds,
            pool_name: std::env::var("S4_POOL_NAME").ok(),
            pool_nodes: std::env::var("S4_POOL_NODES").ok(),
            pool_type: std::env::var("S4_POOL_TYPE").unwrap_or_else(|_| "standard".to_string()),
            replication_factor: std::env::var("S4_REPLICATION_FACTOR")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(3),
            write_quorum: std::env::var("S4_WRITE_QUORUM")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(2),
            read_quorum: std::env::var("S4_READ_QUORUM")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(2),
            gc_grace_days: std::env::var("S4_GC_GRACE_DAYS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(7),
            gc_grace_secs: std::env::var("S4_GC_GRACE_SECS").ok().and_then(|s| s.parse().ok()),
            tombstone_gc_interval_secs: std::env::var("S4_TOMBSTONE_GC_INTERVAL_SECS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(s4_cluster::gc::tombstone::DEFAULT_GC_INTERVAL_SECS),
            max_rejoin_downtime_days: std::env::var("S4_MAX_REJOIN_DOWNTIME_DAYS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(3),
            anti_entropy_interval_secs: std::env::var("S4_ANTI_ENTROPY_INTERVAL_SECS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(600),
            anti_entropy_enabled: parse_switch(
                "S4_ANTI_ENTROPY_ENABLED",
                std::env::var("S4_ANTI_ENTROPY_ENABLED").ok(),
                true,
            ),
            anti_entropy_max_keys_per_round: std::env::var("S4_ANTI_ENTROPY_MAX_KEYS_PER_ROUND")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(1000),
            migration_concurrency: parse_migration_concurrency(
                std::env::var("S4_CLUSTER_MIGRATION_CONCURRENCY").ok(),
            ),
            rebalance_max_bytes_per_sec: std::env::var("S4_REBALANCE_MAX_BYTES_PER_SEC")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(s4_cluster::rebalance::RebalanceConfig::default().max_bytes_per_sec),
            rebalance_max_keys_per_sec: std::env::var("S4_REBALANCE_MAX_KEYS_PER_SEC")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(s4_cluster::rebalance::RebalanceConfig::default().max_keys_per_sec),
            rebalance_concurrent_ranges: std::env::var("S4_REBALANCE_CONCURRENT_RANGES")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(s4_cluster::rebalance::RebalanceConfig::default().concurrent_ranges),
            rebalance_min_free_ratio: std::env::var("S4_REBALANCE_MIN_FREE_RATIO")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(s4_cluster::rebalance::RebalanceConfig::default().min_free_ratio),
            scrubber_full_scan_days: std::env::var("S4_SCRUBBER_FULL_SCAN_DAYS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(30),
            scrubber_start_delay_secs: std::env::var("S4_SCRUBBER_START_DELAY_SECS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(30),
            scrubber_unhealed_retry_secs: std::env::var("S4_SCRUBBER_UNHEALED_RETRY_SECS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(300),
            hint_ttl_hours: std::env::var("S4_HINT_TTL_HOURS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(3),
            iam_lease_secs: std::env::var("S4_IAM_LEASE_SECS")
                .ok()
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(5)
                .max(1),
            iam_lease_wait_secs: std::env::var("S4_IAM_LEASE_WAIT_SECS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(10),
            iam_max_staleness_secs: std::env::var("S4_IAM_MAX_STALENESS_SECS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(30),
            drain_timeout_secs: std::env::var("S4_DRAIN_TIMEOUT_SECS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(30),
            bucket_metadata_replication_factor: parse_count(
                "S4_BUCKET_METADATA_REPLICATION_FACTOR",
                std::env::var("S4_BUCKET_METADATA_REPLICATION_FACTOR").ok(),
            ),
        }
    }
}

/// A count of 1 to 255 named `name`, read from `raw`; `None` when unset or
/// empty. Anything else is an error naming the variable and the value.
fn parse_migration_concurrency(raw: Option<String>) -> Result<usize, String> {
    let Some(raw) = raw else {
        return Ok(4);
    };
    match raw.trim().parse::<usize>() {
        Ok(value) if value > 0 => Ok(value),
        _ => Err(format!(
            "S4_CLUSTER_MIGRATION_CONCURRENCY must be a positive integer; got '{raw}'"
        )),
    }
}

fn parse_count(name: &str, raw: Option<String>) -> Result<Option<u8>, String> {
    let Some(raw) = raw.filter(|raw| !raw.trim().is_empty()) else {
        return Ok(None);
    };
    match raw.trim().parse::<u8>() {
        Ok(count) if count > 0 => Ok(Some(count)),
        _ => Err(format!(
            "{name} must be a number from 1 to 255; got '{raw}'"
        )),
    }
}

/// An on/off setting `name` read from `raw`: `1`, `true`, `yes`, `on` or `0`,
/// `false`, `no`, `off`, in any case; `default` when unset. Anything else is
/// an error naming the variable and the value.
fn parse_switch(name: &str, raw: Option<String>, default: bool) -> Result<bool, String> {
    let Some(raw) = raw else {
        return Ok(default);
    };
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => Err(format!(
            "{name} must be one of true, false, 1, 0, yes, no, on, off; got '{raw}'"
        )),
    }
}

impl ClusterModeConfig {
    /// How this node holds the lease of IAM changes.
    pub fn iam_lease(&self) -> s4_cluster::IamLeaseConfig {
        s4_cluster::IamLeaseConfig {
            duration: std::time::Duration::from_secs(self.iam_lease_secs.max(1)),
            wait: std::time::Duration::from_secs(self.iam_lease_wait_secs),
        }
    }

    /// How long this node checks IAM credentials against its own copy after a
    /// majority of its pool last agreed with it; zero for no bound.
    pub fn iam_max_staleness(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.iam_max_staleness_secs)
    }

    /// How this node runs anti-entropy rounds. A switch that is neither on
    /// nor off, which [`Self::validate`] refuses, counts as off.
    pub fn anti_entropy(&self) -> s4_cluster::AntiEntropyConfig {
        s4_cluster::AntiEntropyConfig {
            enabled: self.anti_entropy_enabled.clone().unwrap_or(false),
            interval_secs: self.anti_entropy_interval_secs,
            max_keys_per_round: self.anti_entropy_max_keys_per_round,
        }
    }

    /// The limits of the move of a grown pool's keys to their new owners.
    pub fn rebalance(&self) -> s4_cluster::rebalance::RebalanceConfig {
        s4_cluster::rebalance::RebalanceConfig {
            max_bytes_per_sec: self.rebalance_max_bytes_per_sec,
            max_keys_per_sec: self.rebalance_max_keys_per_sec,
            concurrent_ranges: self.rebalance_concurrent_ranges,
            min_free_ratio: self.rebalance_min_free_ratio,
            ..s4_cluster::rebalance::RebalanceConfig::default()
        }
    }

    /// How long a tombstone is kept at least (`S4_GC_GRACE_SECS`, or else
    /// `S4_GC_GRACE_DAYS`): the one value tombstone GC and anti-entropy both
    /// go by.
    pub fn gc_grace(&self) -> std::time::Duration {
        std::time::Duration::from_secs(
            self.gc_grace_secs.unwrap_or(u64::from(self.gc_grace_days) * 24 * 3600),
        )
    }

    /// How this node removes tombstones. The safety invariant weighs a turn
    /// of this node's anti-entropy rounds over every other node of the pool
    /// and the write coordinator's replica write timeout.
    pub fn tombstone_gc(&self) -> s4_cluster::GcConfig {
        let peers = self.pool_node_count().saturating_sub(1).max(1);
        s4_cluster::GcConfig {
            enabled: true,
            interval: std::time::Duration::from_secs(self.tombstone_gc_interval_secs),
            tombstone_policy: s4_cluster::TombstonePolicy {
                gc_grace: self.gc_grace(),
                max_rejoin_downtime: std::time::Duration::from_secs(
                    u64::from(self.max_rejoin_downtime_days) * 24 * 3600,
                ),
                repair_turn: std::time::Duration::from_secs(
                    self.anti_entropy_interval_secs.saturating_mul(peers as u64),
                ),
                write_settle: s4_cluster::WriteTimeouts::default().replica_write_timeout,
            },
        }
    }

    /// How many nodes `S4_POOL_NODES` names.
    fn pool_node_count(&self) -> usize {
        self.pool_nodes
            .as_deref()
            .unwrap_or_default()
            .split(',')
            .filter(|node| !node.trim().is_empty())
            .count()
    }

    /// Returns `true` if the server is running in cluster mode.
    pub fn is_cluster(&self) -> bool {
        self.mode == ServerMode::Cluster
    }

    /// Returns `true` if the server is running in gateway mode.
    pub fn is_gateway(&self) -> bool {
        self.mode == ServerMode::Gateway
    }

    /// Returns `true` if the server is running in single-node mode.
    pub fn is_single(&self) -> bool {
        self.mode == ServerMode::Single
    }

    /// Validate cluster-mode configuration.
    ///
    /// Returns an error if required fields are missing for cluster/gateway modes.
    pub fn validate(&self) -> Result<(), String> {
        let pool_storage_class = self.pool_storage_class()?;

        match self.mode {
            ServerMode::Single => {
                if pool_storage_class.is_erasure_coded() {
                    return Err("S4_POOL_TYPE=ec requires S4_MODE=cluster".to_string());
                }
                Ok(())
            }
            ServerMode::Cluster => {
                if self.seeds.is_empty() {
                    return Err("S4_SEEDS is required in cluster mode".to_string());
                }
                if self.pool_name.is_none() {
                    return Err("S4_POOL_NAME is required in cluster mode".to_string());
                }
                if self.pool_nodes.is_none() {
                    return Err("S4_POOL_NODES is required in cluster mode".to_string());
                }
                if self.migration_concurrency.clone()? == 0 {
                    return Err(
                        "S4_CLUSTER_MIGRATION_CONCURRENCY must be a positive integer".into(),
                    );
                }
                self.anti_entropy_enabled.clone()?;
                self.bucket_metadata_replication_factor.clone()?;
                self.anti_entropy().validate().map_err(|e| e.to_string())?;
                self.rebalance().validate().map_err(|e| e.to_string())?;
                // Tombstones of a pool of erasure-coded objects are the
                // Enterprise runtime's, with settings of their own.
                if !pool_storage_class.is_erasure_coded() {
                    self.tombstone_gc().validate()?;
                }
                Ok(())
            }
            ServerMode::Gateway => {
                if pool_storage_class.is_erasure_coded() {
                    return Err("S4_POOL_TYPE=ec requires S4_MODE=cluster".to_string());
                }
                if self.seeds.is_empty() {
                    return Err("S4_SEEDS is required in gateway mode".to_string());
                }
                Ok(())
            }
        }
    }

    /// Parsed S4 physical storage class for this pool.
    pub fn pool_storage_class(&self) -> Result<s4_core::types::StorageClass, String> {
        s4_core::types::StorageClass::from_pool_type(&self.pool_type).ok_or_else(|| {
            format!(
                "S4_POOL_TYPE must be 'standard' or 'ec', got '{}'",
                self.pool_type
            )
        })
    }
}

/// Server configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    /// Bind address (e.g., "0.0.0.0:9000")
    pub bind: String,
    /// Number of worker threads
    pub workers: Option<usize>,
    /// Enable HTTP/3 (QUIC)
    pub http3_enabled: Option<bool>,
    /// Maximum upload size in bytes.
    /// Can be set via S4_MAX_UPLOAD_SIZE environment variable (e.g., "5GB", "100MB", "1024KB").
    pub max_upload_size: usize,
    /// TLS configuration for HTTPS support.
    pub tls: TlsConfig,
}

/// TLS/HTTPS configuration.
///
/// TLS is disabled by default. To enable TLS, set the `S4_TLS_CERT` and `S4_TLS_KEY`
/// environment variables to point to PEM-encoded certificate and private key files.
///
/// Example:
/// ```bash
/// export S4_TLS_CERT=/path/to/cert.pem
/// export S4_TLS_KEY=/path/to/key.pem
/// ./s4-server
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsConfig {
    /// Whether TLS is enabled.
    /// Automatically set to true when both cert_path and key_path are provided.
    pub enabled: bool,
    /// Path to PEM-encoded certificate file.
    /// Can be set via S4_TLS_CERT environment variable.
    pub cert_path: Option<PathBuf>,
    /// Path to PEM-encoded private key file.
    /// Can be set via S4_TLS_KEY environment variable.
    pub key_path: Option<PathBuf>,
}

impl Default for TlsConfig {
    fn default() -> Self {
        let cert_path = std::env::var("S4_TLS_CERT").ok().map(PathBuf::from);
        let key_path = std::env::var("S4_TLS_KEY").ok().map(PathBuf::from);

        // Enable TLS only if both cert and key are provided
        let enabled = cert_path.is_some() && key_path.is_some();

        Self {
            enabled,
            cert_path,
            key_path,
        }
    }
}

impl TlsConfig {
    /// Validates TLS configuration.
    ///
    /// Returns an error if TLS is enabled but certificate or key paths are missing.
    pub fn validate(&self) -> Result<(), String> {
        if self.enabled {
            if self.cert_path.is_none() {
                return Err("TLS enabled but S4_TLS_CERT is not set".to_string());
            }
            if self.key_path.is_none() {
                return Err("TLS enabled but S4_TLS_KEY is not set".to_string());
            }
        }
        Ok(())
    }
}

/// Parses a size string like "10GB", "100MB", "1024KB", "5000" into bytes.
///
/// Supported suffixes (case-insensitive):
/// - GB, G: Gigabytes
/// - MB, M: Megabytes
/// - KB, K: Kilobytes
/// - B or no suffix: Bytes
pub fn parse_size(s: &str) -> Result<usize, String> {
    let s = s.trim().to_uppercase();

    if s.is_empty() {
        return Err("Empty size string".to_string());
    }

    // Find where the numeric part ends
    let num_end = s.chars().position(|c| !c.is_ascii_digit() && c != '.').unwrap_or(s.len());

    let (num_str, suffix) = s.split_at(num_end);
    let suffix = suffix.trim();

    let num: f64 = num_str.parse().map_err(|_| format!("Invalid number: {}", num_str))?;

    let multiplier: usize = match suffix {
        "GB" | "G" => 1024 * 1024 * 1024,
        "MB" | "M" => 1024 * 1024,
        "KB" | "K" => 1024,
        "B" | "" => 1,
        _ => return Err(format!("Unknown size suffix: {}", suffix)),
    };

    Ok((num * multiplier as f64) as usize)
}

/// Storage configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Path to volume files
    pub data_path: PathBuf,
    /// Path to metadata database
    pub metadata_path: PathBuf,
}

/// Tuning configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TuningConfig {
    /// Threshold for inline storage (bytes)
    pub inline_threshold: usize,
    /// Maximum volume size (MB)
    pub volume_size_mb: u64,
    /// Strict sync (fsync after each write)
    pub strict_sync: bool,
}

/// Features configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeaturesConfig {
    /// Enable deduplication
    pub deduplication: bool,
    /// Enable atomic directory operations
    pub atomic_dirs: bool,
    /// Enable extended metadata
    pub extended_metadata: bool,
}

/// Security configuration.
///
/// Every credential here can be set directly (`S4_ACCESS_KEY_ID=...`) or, for
/// Docker and Kubernetes secrets, through a file (`S4_ACCESS_KEY_ID_FILE=/run/secrets/...`);
/// see [`crate::secrets`] for the rules. `Debug` redacts the secret values.
#[derive(Clone, Serialize, Deserialize)]
pub struct SecurityConfig {
    /// Access key ID for S3 authentication (`S4_ACCESS_KEY_ID`).
    pub access_key_id: String,
    /// Secret access key for S3 authentication (`S4_SECRET_ACCESS_KEY`).
    #[serde(skip_serializing, default = "dev_secret_access_key")]
    pub secret_access_key: String,
    /// Root admin user name (`S4_ROOT_USERNAME`, default `root`).
    #[serde(default = "default_root_username")]
    pub root_username: String,
    /// Root admin password (`S4_ROOT_PASSWORD`). The root user is created only
    /// when it is set.
    #[serde(skip_serializing, default)]
    pub root_password: Option<String>,
    /// Secret that signs admin JWTs (`S4_JWT_SECRET`), at least 32 bytes. When
    /// unset, a single node generates a random one at startup; see
    /// [`crate::secrets::resolve_jwt_secret`].
    #[serde(skip_serializing, default)]
    pub jwt_secret: Option<String>,
    /// Key the secrets of IAM users' S3 credentials are encrypted with
    /// (`S4_IAM_ENCRYPTION_KEY`), 32 bytes in hex, the same on every node.
    /// When unset, the key is derived from `S4_JWT_SECRET`.
    #[serde(skip_serializing, default)]
    pub iam_encryption_key: Option<String>,
    /// The key that replaced `iam_encryption_key` is still read with
    /// (`S4_IAM_ENCRYPTION_KEY_PREVIOUS`), until every secret is encrypted
    /// again.
    #[serde(skip_serializing, default)]
    pub iam_encryption_key_previous: Option<String>,
    /// How long the audit log of IAM changes keeps an entry
    /// (`S4_IAM_AUDIT_RETENTION_DAYS`, default 90; `0` keeps every entry).
    #[serde(default = "default_iam_audit_retention_days")]
    pub iam_audit_retention_days: u64,
    /// Audit log path
    pub audit_log_path: Option<PathBuf>,
}

impl SecurityConfig {
    /// Reads the credentials from `S4_*` variables or the files their `S4_*_FILE`
    /// counterparts name.
    ///
    /// Unset S3 keys fall back to random development keys. A key set to an empty
    /// value stays empty, which switches off the login with these keys, as it
    /// always has. An empty user name, root password or JWT secret counts as unset.
    ///
    /// # Errors
    ///
    /// Returns [`SecretError`] when a variable and its `_FILE` form are both set,
    /// or a `_FILE` cannot be read, is too large, is not UTF-8 text or is empty.
    pub fn from_env() -> Result<Self, SecretError> {
        Self::from_secrets(secret_from_env)
    }

    /// [`Self::from_env`] over any source of secrets, so it can be tested
    /// without touching the process environment.
    fn from_secrets(
        secret: impl Fn(&str) -> Result<Option<String>, SecretError>,
    ) -> Result<Self, SecretError> {
        let non_empty = |value: Option<String>| value.filter(|value| !value.is_empty());
        Ok(Self {
            access_key_id: secret("S4_ACCESS_KEY_ID")?.unwrap_or_else(dev_access_key_id),
            secret_access_key: secret("S4_SECRET_ACCESS_KEY")?
                .unwrap_or_else(dev_secret_access_key),
            root_username: non_empty(secret("S4_ROOT_USERNAME")?)
                .unwrap_or_else(default_root_username),
            root_password: non_empty(secret("S4_ROOT_PASSWORD")?),
            jwt_secret: non_empty(secret("S4_JWT_SECRET")?),
            iam_encryption_key: non_empty(secret("S4_IAM_ENCRYPTION_KEY")?),
            iam_encryption_key_previous: non_empty(secret("S4_IAM_ENCRYPTION_KEY_PREVIOUS")?),
            iam_audit_retention_days: secret("S4_IAM_AUDIT_RETENTION_DAYS")?
                .and_then(|days| days.trim().parse().ok())
                .unwrap_or_else(default_iam_audit_retention_days),
            audit_log_path: default_audit_log_path(),
        })
    }
}

impl Default for SecurityConfig {
    /// Development defaults: random S3 keys, no root user, no JWT secret.
    fn default() -> Self {
        Self {
            access_key_id: dev_access_key_id(),
            secret_access_key: dev_secret_access_key(),
            root_username: default_root_username(),
            root_password: None,
            jwt_secret: None,
            iam_encryption_key: None,
            iam_encryption_key_previous: None,
            iam_audit_retention_days: default_iam_audit_retention_days(),
            audit_log_path: default_audit_log_path(),
        }
    }
}

impl std::fmt::Debug for SecurityConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        const REDACTED: &str = "<redacted>";
        f.debug_struct("SecurityConfig")
            .field("access_key_id", &self.access_key_id)
            .field("secret_access_key", &REDACTED)
            .field("root_username", &self.root_username)
            .field(
                "root_password",
                &self.root_password.as_ref().map(|_| REDACTED),
            )
            .field("jwt_secret", &self.jwt_secret.as_ref().map(|_| REDACTED))
            .field(
                "iam_encryption_key",
                &self.iam_encryption_key.as_ref().map(|_| REDACTED),
            )
            .field(
                "iam_encryption_key_previous",
                &self.iam_encryption_key_previous.as_ref().map(|_| REDACTED),
            )
            .field("iam_audit_retention_days", &self.iam_audit_retention_days)
            .field("audit_log_path", &self.audit_log_path)
            .finish()
    }
}

fn default_iam_audit_retention_days() -> u64 {
    90
}

fn default_root_username() -> String {
    "root".to_string()
}

fn default_audit_log_path() -> Option<PathBuf> {
    Some(PathBuf::from("/var/log/s4/audit.log"))
}

/// Development access key used when `S4_ACCESS_KEY_ID` is not set.
///
/// Both development keys come from the OS random generator: the legacy login
/// with these keys acts as a SuperUser, so they must not be guessable from the
/// start time or the process id.
fn dev_access_key_id() -> String {
    let random = generate_secret();
    format!("dev-{}", &random[..16])
}

/// Development secret key used when `S4_SECRET_ACCESS_KEY` is not set.
fn dev_secret_access_key() -> String {
    format!("dev-secret-{}", generate_secret())
}

/// Metrics configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsConfig {
    /// Enable Prometheus metrics
    pub prometheus_enabled: bool,
    /// Prometheus metrics port
    pub prometheus_port: u16,
}

/// Lifecycle worker configuration.
///
/// The lifecycle worker runs as a background task and periodically
/// evaluates lifecycle rules to delete expired objects.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LifecycleConfig {
    /// Enable lifecycle worker.
    /// Can be set via S4_LIFECYCLE_ENABLED environment variable.
    pub enabled: bool,
    /// Evaluation interval in hours (default: 24).
    /// Can be set via S4_LIFECYCLE_INTERVAL_HOURS environment variable.
    pub interval_hours: u64,
    /// Dry-run mode - log actions without executing.
    /// Can be set via S4_LIFECYCLE_DRY_RUN environment variable.
    pub dry_run: bool,
    /// Evaluation interval in seconds; when set, `interval_hours` is not read.
    /// Can be set via S4_LIFECYCLE_INTERVAL_SECS environment variable.
    pub interval_secs: Option<u64>,
    /// Dev and testing only: rules are evaluated as if this many more days
    /// had passed, so that a test sees an object expire without waiting a
    /// day. Can be set via S4_LIFECYCLE_DAYS_AHEAD environment variable.
    pub days_ahead: u32,
}

impl LifecycleConfig {
    /// Time between two evaluations, never zero.
    pub fn interval(&self) -> std::time::Duration {
        let secs = self.interval_secs.unwrap_or(self.interval_hours.saturating_mul(3600));
        std::time::Duration::from_secs(secs.max(1))
    }
}

/// Volume compaction worker configuration.
///
/// The compactor runs as a background task and periodically reclaims
/// dead space from append-only volume files.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactionConfig {
    /// Enable compaction worker.
    /// Can be set via S4_COMPACTION_ENABLED environment variable.
    pub enabled: bool,
    /// Compaction check interval in hours (default: 6).
    /// Can be set via S4_COMPACTION_INTERVAL_HOURS environment variable.
    pub interval_hours: u64,
    /// Minimum fragmentation ratio to trigger compaction (0.0-1.0, default: 0.3).
    /// Can be set via S4_COMPACTION_THRESHOLD environment variable.
    pub fragmentation_threshold: f64,
    /// Dry-run mode — analyze without compacting.
    /// Can be set via S4_COMPACTION_DRY_RUN environment variable.
    pub dry_run: bool,
    /// TTL (hours) for multipart sessions the compactor considers expired.
    /// Defaults to the same value as `S4_MULTIPART_UPLOAD_TTL_HOURS` (24).
    pub multipart_session_ttl_hours: u64,
    /// Optional override in seconds — takes priority over `multipart_session_ttl_hours`.
    /// Intended for testing only (`S4_COMPACTION_MULTIPART_TTL_SECS`).
    pub multipart_session_ttl_secs_override: Option<u64>,
    /// Daily full compaction time in "HH:MM" format (default: "02:00").
    /// At this time the compactor runs with unlimited volumes (processes all).
    /// Set to empty string to disable daily full compaction.
    /// Can be set via `S4_COMPACTION_FULL_TIME` environment variable.
    pub full_compaction_time: String,
}

/// Multipart upload cleanup worker configuration.
///
/// The cleanup worker runs as a background task and periodically removes
/// temp files from abandoned multipart uploads.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultipartCleanupConfig {
    /// TTL for incomplete multipart uploads in hours (default: 24).
    /// Can be set via S4_MULTIPART_UPLOAD_TTL_HOURS environment variable.
    pub upload_ttl_hours: u64,
    /// Cleanup check interval in hours (default: 1).
    /// Can be set via S4_MULTIPART_CLEANUP_INTERVAL_HOURS environment variable.
    pub cleanup_interval_hours: u64,
}

impl Default for MultipartCleanupConfig {
    fn default() -> Self {
        Self {
            upload_ttl_hours: std::env::var("S4_MULTIPART_UPLOAD_TTL_HOURS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(24),
            cleanup_interval_hours: std::env::var("S4_MULTIPART_CLEANUP_INTERVAL_HOURS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(1),
        }
    }
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            enabled: std::env::var("S4_COMPACTION_ENABLED")
                .map(|s| s.to_lowercase() == "true" || s == "1")
                .unwrap_or(true),
            interval_hours: std::env::var("S4_COMPACTION_INTERVAL_HOURS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(1),
            fragmentation_threshold: std::env::var("S4_COMPACTION_THRESHOLD")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.3),
            dry_run: std::env::var("S4_COMPACTION_DRY_RUN")
                .map(|s| s.to_lowercase() == "true" || s == "1")
                .unwrap_or(false),
            multipart_session_ttl_hours: std::env::var("S4_MULTIPART_UPLOAD_TTL_HOURS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(24),
            multipart_session_ttl_secs_override: std::env::var("S4_COMPACTION_MULTIPART_TTL_SECS")
                .ok()
                .and_then(|s| s.parse().ok()),
            full_compaction_time: std::env::var("S4_COMPACTION_FULL_TIME")
                .unwrap_or_else(|_| "02:00".to_string()),
        }
    }
}

impl Default for LifecycleConfig {
    fn default() -> Self {
        Self {
            enabled: std::env::var("S4_LIFECYCLE_ENABLED")
                .map(|s| s.to_lowercase() == "true" || s == "1")
                .unwrap_or(true),
            interval_hours: std::env::var("S4_LIFECYCLE_INTERVAL_HOURS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(24),
            dry_run: std::env::var("S4_LIFECYCLE_DRY_RUN")
                .map(|s| s.to_lowercase() == "true" || s == "1")
                .unwrap_or(false),
            interval_secs: std::env::var("S4_LIFECYCLE_INTERVAL_SECS")
                .ok()
                .and_then(|s| s.parse().ok()),
            days_ahead: std::env::var("S4_LIFECYCLE_DAYS_AHEAD")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0),
        }
    }
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            prometheus_enabled: std::env::var("S4_METRICS_ENABLED")
                .map(|s| s.to_lowercase() == "true" || s == "1")
                .unwrap_or(true),
            prometheus_port: 9090,
        }
    }
}

impl Config {
    /// Loads configuration from the environment on top of the defaults.
    ///
    /// # Errors
    ///
    /// Fails when a credential is misconfigured (see [`SecurityConfig::from_env`]),
    /// so a broken secret stops the start instead of being replaced by a default.
    pub fn load() -> anyhow::Result<Self> {
        Ok(Self {
            security: SecurityConfig::from_env()?,
            ..Self::default()
        })
    }
}

impl Default for Config {
    fn default() -> Self {
        // Use temp directory for development, can be overridden by config file
        let data_dir = std::env::var("S4_DATA_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| std::env::temp_dir().join("s4-data"));

        Self {
            server: ServerConfig {
                bind: std::env::var("S4_BIND").unwrap_or_else(|_| "127.0.0.1:9000".to_string()),
                workers: Some(8),
                http3_enabled: Some(false),
                max_upload_size: std::env::var("S4_MAX_UPLOAD_SIZE")
                    .ok()
                    .and_then(|s| parse_size(&s).ok())
                    .unwrap_or(5 * 1024 * 1024 * 1024), // Default: 5GB
                tls: TlsConfig::default(),
            },
            storage: StorageConfig {
                data_path: data_dir.join("volumes"),
                metadata_path: data_dir.join("metadata_db"),
            },
            tuning: TuningConfig {
                inline_threshold: 4096,
                volume_size_mb: 1024,
                strict_sync: true,
            },
            features: FeaturesConfig {
                deduplication: true,
                atomic_dirs: true,
                extended_metadata: true,
            },
            // Credentials are read, with their `_FILE` forms, by `Config::load`.
            security: SecurityConfig::default(),
            metrics: MetricsConfig::default(),
            lifecycle: LifecycleConfig::default(),
            compaction: CompactionConfig::default(),
            multipart: MultipartCleanupConfig::default(),
            cluster: ClusterModeConfig::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Security settings read from a fake environment with the given values.
    fn security_from(env: &[(&str, &str)]) -> SecurityConfig {
        SecurityConfig::from_secrets(|name| {
            Ok(env.iter().find(|(key, _)| *key == name).map(|(_, value)| value.to_string()))
        })
        .unwrap()
    }

    #[test]
    fn unset_s3_keys_fall_back_to_random_development_keys() {
        let first = security_from(&[]);
        let second = security_from(&[]);

        assert!(
            first.access_key_id.starts_with("dev-"),
            "{}",
            first.access_key_id
        );
        assert_eq!(first.access_key_id.len(), "dev-".len() + 16);
        assert!(first.secret_access_key.starts_with("dev-secret-"));
        assert_eq!(first.secret_access_key.len(), "dev-secret-".len() + 64);
        assert_ne!(first.access_key_id, second.access_key_id);
        assert_ne!(first.secret_access_key, second.secret_access_key);
    }

    #[test]
    fn empty_s3_keys_stay_empty_and_switch_the_key_login_off() {
        let security = security_from(&[("S4_ACCESS_KEY_ID", ""), ("S4_SECRET_ACCESS_KEY", "")]);

        assert_eq!(security.access_key_id, "");
        assert_eq!(security.secret_access_key, "");
    }

    #[test]
    fn empty_root_credentials_and_jwt_secret_count_as_unset() {
        let security = security_from(&[
            ("S4_ROOT_USERNAME", ""),
            ("S4_ROOT_PASSWORD", ""),
            ("S4_JWT_SECRET", ""),
        ]);

        assert_eq!(security.root_username, "root");
        assert!(security.root_password.is_none());
        assert!(security.jwt_secret.is_none());
    }

    #[test]
    fn configured_credentials_are_used() {
        let security = security_from(&[
            ("S4_ACCESS_KEY_ID", "ak"),
            ("S4_SECRET_ACCESS_KEY", "sk"),
            ("S4_ROOT_USERNAME", "admin"),
            ("S4_ROOT_PASSWORD", "pw"),
            ("S4_JWT_SECRET", "jwt"),
        ]);

        assert_eq!(security.access_key_id, "ak");
        assert_eq!(security.secret_access_key, "sk");
        assert_eq!(security.root_username, "admin");
        assert_eq!(security.root_password.as_deref(), Some("pw"));
        assert_eq!(security.jwt_secret.as_deref(), Some("jwt"));
    }

    #[test]
    fn iam_encryption_keys_and_audit_retention_are_read() {
        let unset = security_from(&[("S4_IAM_ENCRYPTION_KEY", "")]);
        assert!(unset.iam_encryption_key.is_none());
        assert!(unset.iam_encryption_key_previous.is_none());
        assert_eq!(unset.iam_audit_retention_days, 90);

        let set = security_from(&[
            ("S4_IAM_ENCRYPTION_KEY", "aa"),
            ("S4_IAM_ENCRYPTION_KEY_PREVIOUS", "bb"),
            ("S4_IAM_AUDIT_RETENTION_DAYS", "7"),
        ]);
        assert_eq!(set.iam_encryption_key.as_deref(), Some("aa"));
        assert_eq!(set.iam_encryption_key_previous.as_deref(), Some("bb"));
        assert_eq!(set.iam_audit_retention_days, 7);
    }

    #[test]
    fn security_debug_output_hides_the_secrets() {
        let security = security_from(&[
            ("S4_SECRET_ACCESS_KEY", "secret-key-value"),
            ("S4_ROOT_PASSWORD", "root-password-value"),
            ("S4_JWT_SECRET", "jwt-secret-value"),
            ("S4_IAM_ENCRYPTION_KEY", "iam-key-value"),
        ]);
        let shown = format!("{security:?}");

        for secret in [
            "secret-key-value",
            "root-password-value",
            "jwt-secret-value",
            "iam-key-value",
        ] {
            assert!(!shown.contains(secret), "{shown}");
        }
    }

    #[test]
    fn test_parse_size_bytes() {
        assert_eq!(parse_size("1024").unwrap(), 1024);
        assert_eq!(parse_size("0").unwrap(), 0);
    }

    #[test]
    fn test_parse_size_kb() {
        assert_eq!(parse_size("1KB").unwrap(), 1024);
        assert_eq!(parse_size("1K").unwrap(), 1024);
        assert_eq!(parse_size("10kb").unwrap(), 10 * 1024);
    }

    #[test]
    fn test_parse_size_mb() {
        assert_eq!(parse_size("1MB").unwrap(), 1024 * 1024);
        assert_eq!(parse_size("1M").unwrap(), 1024 * 1024);
        assert_eq!(parse_size("100mb").unwrap(), 100 * 1024 * 1024);
    }

    #[test]
    fn test_parse_size_gb() {
        assert_eq!(parse_size("1GB").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_size("1G").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_size("5gb").unwrap(), 5 * 1024 * 1024 * 1024);
    }

    #[test]
    fn test_parse_size_invalid() {
        assert!(parse_size("").is_err());
        assert!(parse_size("abc").is_err());
        assert!(parse_size("1TB").is_err()); // TB not supported
    }

    #[test]
    fn test_tls_config_disabled_by_default() {
        // Clear environment variables for this test
        std::env::remove_var("S4_TLS_CERT");
        std::env::remove_var("S4_TLS_KEY");

        let tls = TlsConfig::default();
        assert!(!tls.enabled);
        assert!(tls.cert_path.is_none());
        assert!(tls.key_path.is_none());
    }

    #[test]
    fn test_tls_config_enabled_when_both_paths_set() {
        // This test modifies environment variables, so it should be isolated
        // In a real test suite, use a test harness that isolates env vars
        let tls = TlsConfig {
            enabled: true,
            cert_path: Some(PathBuf::from("/path/to/cert.pem")),
            key_path: Some(PathBuf::from("/path/to/key.pem")),
        };

        assert!(tls.enabled);
        assert_eq!(
            tls.cert_path.as_ref().unwrap().to_str().unwrap(),
            "/path/to/cert.pem"
        );
        assert_eq!(
            tls.key_path.as_ref().unwrap().to_str().unwrap(),
            "/path/to/key.pem"
        );
    }

    #[test]
    fn test_tls_config_validation_success() {
        let tls = TlsConfig {
            enabled: true,
            cert_path: Some(PathBuf::from("/path/to/cert.pem")),
            key_path: Some(PathBuf::from("/path/to/key.pem")),
        };

        assert!(tls.validate().is_ok());
    }

    #[test]
    fn test_tls_config_validation_disabled() {
        let tls = TlsConfig {
            enabled: false,
            cert_path: None,
            key_path: None,
        };

        assert!(tls.validate().is_ok());
    }

    #[test]
    fn test_tls_config_validation_missing_cert() {
        let tls = TlsConfig {
            enabled: true,
            cert_path: None,
            key_path: Some(PathBuf::from("/path/to/key.pem")),
        };

        let result = tls.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("S4_TLS_CERT"));
    }

    #[test]
    fn test_tls_config_validation_missing_key() {
        let tls = TlsConfig {
            enabled: true,
            cert_path: Some(PathBuf::from("/path/to/cert.pem")),
            key_path: None,
        };

        let result = tls.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("S4_TLS_KEY"));
    }

    // ----- Cluster mode config tests -----

    #[test]
    fn server_mode_default_is_single() {
        std::env::remove_var("S4_MODE");
        let config = ClusterModeConfig::default();
        assert_eq!(config.mode, ServerMode::Single);
        assert!(config.is_single());
        assert!(!config.is_cluster());
        assert!(!config.is_gateway());
    }

    #[test]
    fn cluster_mode_validate_single_always_ok() {
        let config = ClusterModeConfig {
            mode: ServerMode::Single,
            ..ClusterModeConfig::default()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn cluster_mode_validate_rejects_ec_pool_type_in_single_mode() {
        let config = ClusterModeConfig {
            mode: ServerMode::Single,
            pool_type: "ec".into(),
            ..ClusterModeConfig::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.contains("S4_POOL_TYPE=ec requires S4_MODE=cluster"));
    }

    /// The anti-entropy kill switch takes on and off in the usual spellings,
    /// and refuses anything else instead of reading it either way.
    #[test]
    fn the_anti_entropy_switch_is_on_off_or_an_error() {
        let name = "S4_ANTI_ENTROPY_ENABLED";
        assert_eq!(parse_switch(name, None, true), Ok(true));
        for on in ["true", "1", "YES", " on "] {
            assert_eq!(parse_switch(name, Some(on.into()), false), Ok(true), "{on}");
        }
        for off in ["false", "0", "No", "OFF"] {
            assert_eq!(
                parse_switch(name, Some(off.into()), true),
                Ok(false),
                "{off}"
            );
        }
        for typo in ["disabled", "n", "flase", ""] {
            let err = parse_switch(name, Some(typo.into()), true).unwrap_err();
            assert!(err.contains(name) && err.contains(typo), "{err}");
        }
    }

    /// Cluster mode refuses a switch that is neither on nor off, and checks
    /// the interval and the key limit only on a node that starts rounds.
    #[test]
    fn cluster_mode_checks_rounds_only_where_they_run() {
        let cluster = |enabled, interval| ClusterModeConfig {
            mode: ServerMode::Cluster,
            seeds: vec!["10.0.1.1:9100".into()],
            pool_name: Some("pool-1".into()),
            pool_nodes: Some("node-1:10.0.1.1:9100".into()),
            anti_entropy_enabled: enabled,
            anti_entropy_interval_secs: interval,
            ..ClusterModeConfig::default()
        };
        assert!(cluster(Ok(false), 0).validate().is_ok());
        assert!(cluster(Ok(true), 600).validate().is_ok());
        let err = cluster(Ok(true), 0).validate().unwrap_err();
        assert!(err.contains("S4_ANTI_ENTROPY_INTERVAL_SECS"), "{err}");
        let err = cluster(Err("S4_ANTI_ENTROPY_ENABLED: 'disabled'".into()), 600)
            .validate()
            .unwrap_err();
        assert!(err.contains("S4_ANTI_ENTROPY_ENABLED"), "{err}");
        assert!(!cluster(Err(String::new()), 600).anti_entropy().enabled);
    }

    #[test]
    fn cluster_mode_parses_ec_pool_type() {
        let config = ClusterModeConfig {
            mode: ServerMode::Cluster,
            seeds: vec!["10.0.1.1:9100".into()],
            pool_name: Some("pool-1".into()),
            pool_nodes: Some("node-1:10.0.1.1:9100".into()),
            pool_type: "ec".into(),
            ..ClusterModeConfig::default()
        };
        assert_eq!(
            config.pool_storage_class().unwrap(),
            s4_core::types::StorageClass::ErasureCoded
        );
        assert!(config.validate().is_ok());
    }

    /// The replicas of the bucket metadata: unset or empty is the derived
    /// number, a number from 1 to 255 is taken, anything else refused at
    /// validation with the variable named.
    #[test]
    fn the_bucket_metadata_replication_factor_is_a_count() {
        let name = "S4_BUCKET_METADATA_REPLICATION_FACTOR";
        assert_eq!(parse_count(name, None), Ok(None));
        assert_eq!(parse_count(name, Some(" ".into())), Ok(None));
        assert_eq!(parse_count(name, Some("5".into())), Ok(Some(5)));
        for bad in ["0", "five", "-1", "256"] {
            let err = parse_count(name, Some(bad.into())).unwrap_err();
            assert!(err.contains(name) && err.contains(bad), "{err}");
        }
        let config = ClusterModeConfig {
            mode: ServerMode::Cluster,
            seeds: vec!["10.0.1.1:9100".into()],
            pool_name: Some("pool-1".into()),
            pool_nodes: Some("node-1:10.0.1.1:9100".into()),
            bucket_metadata_replication_factor: parse_count(name, Some("five".into())),
            ..ClusterModeConfig::default()
        };
        assert!(config.validate().unwrap_err().contains(name));
    }

    #[test]
    fn migration_concurrency_defaults_and_rejects_malformed_or_zero() {
        assert_eq!(parse_migration_concurrency(None), Ok(4));
        assert_eq!(parse_migration_concurrency(Some("8".into())), Ok(8));
        for bad in ["0", "", "no", "-1", "184467440737095516160"] {
            let error = parse_migration_concurrency(Some(bad.into())).unwrap_err();
            assert!(
                error.contains("S4_CLUSTER_MIGRATION_CONCURRENCY"),
                "{error}"
            );
            let config = cluster(|config| config.migration_concurrency = Err(error));
            assert!(config.validate().unwrap_err().contains("S4_CLUSTER_MIGRATION_CONCURRENCY"));
        }
        assert!(cluster(|config| config.migration_concurrency = Ok(0)).validate().is_err());
    }

    #[test]
    fn cluster_mode_validate_requires_seeds() {
        let config = ClusterModeConfig {
            mode: ServerMode::Cluster,
            seeds: vec![],
            pool_name: Some("pool-1".into()),
            pool_nodes: Some("node-1:10.0.1.1:9100".into()),
            ..ClusterModeConfig::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.contains("S4_SEEDS"));
    }

    #[test]
    fn cluster_mode_validate_requires_pool_name() {
        let config = ClusterModeConfig {
            mode: ServerMode::Cluster,
            seeds: vec!["10.0.1.1:9100".into()],
            pool_name: None,
            pool_nodes: Some("node-1:10.0.1.1:9100".into()),
            ..ClusterModeConfig::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.contains("S4_POOL_NAME"));
    }

    #[test]
    fn cluster_mode_validate_requires_pool_nodes() {
        let config = ClusterModeConfig {
            mode: ServerMode::Cluster,
            seeds: vec!["10.0.1.1:9100".into()],
            pool_name: Some("pool-1".into()),
            pool_nodes: None,
            ..ClusterModeConfig::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.contains("S4_POOL_NODES"));
    }

    #[test]
    fn cluster_mode_valid_config() {
        let config = ClusterModeConfig {
            mode: ServerMode::Cluster,
            cluster_name: "production".into(),
            seeds: vec!["10.0.1.1:9100".into(), "10.0.1.2:9100".into()],
            pool_name: Some("pool-1".into()),
            pool_nodes: Some(
                "node-1:10.0.1.1:9100,node-2:10.0.1.2:9100,node-3:10.0.1.3:9100".into(),
            ),
            ..ClusterModeConfig::default()
        };
        assert!(config.validate().is_ok());
        assert!(config.is_cluster());
    }

    #[test]
    fn gateway_mode_validate_requires_seeds() {
        let config = ClusterModeConfig {
            mode: ServerMode::Gateway,
            seeds: vec![],
            ..ClusterModeConfig::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.contains("S4_SEEDS"));
    }

    #[test]
    fn gateway_mode_valid_config() {
        let config = ClusterModeConfig {
            mode: ServerMode::Gateway,
            seeds: vec!["10.0.1.1:9100".into()],
            ..ClusterModeConfig::default()
        };
        assert!(config.validate().is_ok());
        assert!(config.is_gateway());
    }

    #[test]
    fn server_mode_serialization() {
        assert_eq!(
            serde_json::to_string(&ServerMode::Single).unwrap(),
            "\"single\""
        );
        assert_eq!(
            serde_json::to_string(&ServerMode::Cluster).unwrap(),
            "\"cluster\""
        );
        assert_eq!(
            serde_json::to_string(&ServerMode::Gateway).unwrap(),
            "\"gateway\""
        );
    }

    /// A limit of the move of a grown pool that cannot be kept is refused at
    /// startup rather than taken.
    #[test]
    fn cluster_mode_refuses_rebalance_limits_that_cannot_be_kept() {
        let cluster = |ratio: f64, ranges: usize| ClusterModeConfig {
            mode: ServerMode::Cluster,
            seeds: vec!["10.0.1.1:9100".into()],
            pool_name: Some("pool-1".into()),
            pool_nodes: Some("node-1:10.0.1.1:9100".into()),
            rebalance_min_free_ratio: ratio,
            rebalance_concurrent_ranges: ranges,
            ..ClusterModeConfig::default()
        };
        assert!(cluster(0.1, 2).validate().is_ok());
        assert!(cluster(1.0, 2).validate().unwrap_err().contains("S4_REBALANCE_MIN_FREE_RATIO"));
        assert!(cluster(0.1, 0)
            .validate()
            .unwrap_err()
            .contains("S4_REBALANCE_CONCURRENT_RANGES"));
    }

    #[test]
    fn cluster_defaults_match_plan() {
        let config = ClusterModeConfig::default();
        assert_eq!(config.replication_factor, 3);
        assert_eq!(config.write_quorum, 2);
        assert_eq!(config.read_quorum, 2);
        assert_eq!(config.gc_grace_days, 7);
        assert_eq!(config.max_rejoin_downtime_days, 3);
        assert_eq!(config.anti_entropy_interval_secs, 600);
        assert_eq!(config.anti_entropy_enabled, Ok(true));
        assert_eq!(config.anti_entropy_max_keys_per_round, 1000);
        assert_eq!(
            config.rebalance(),
            s4_cluster::rebalance::RebalanceConfig::default()
        );
        assert_eq!(config.scrubber_full_scan_days, 30);
        assert_eq!(config.hint_ttl_hours, 3);
        assert_eq!(config.iam_lease_secs, 5);
        assert_eq!(config.iam_lease_wait_secs, 10);
        assert_eq!(config.iam_max_staleness_secs, 30);
        assert_eq!(config.drain_timeout_secs, 30);
        assert_eq!(config.gc_grace_secs, None);
        assert_eq!(config.tombstone_gc_interval_secs, 6 * 3600);
        assert_eq!(
            config.gc_grace(),
            std::time::Duration::from_secs(7 * 24 * 3600)
        );
    }

    fn cluster(adjust: impl FnOnce(&mut ClusterModeConfig)) -> ClusterModeConfig {
        let mut config = ClusterModeConfig {
            mode: ServerMode::Cluster,
            seeds: vec!["10.0.1.1:9100".into()],
            pool_name: Some("pool-1".into()),
            pool_nodes: Some(
                "node-1:10.0.1.1:9100,node-2:10.0.1.2:9100,node-3:10.0.1.3:9100".into(),
            ),
            ..ClusterModeConfig::default()
        };
        adjust(&mut config);
        config
    }

    /// `S4_GC_GRACE_SECS`, when set, is the grace period, whatever
    /// `S4_GC_GRACE_DAYS` says.
    #[test]
    fn the_grace_period_in_seconds_wins_over_days() {
        let config = cluster(|config| {
            config.gc_grace_days = 30;
            config.gc_grace_secs = Some(75);
            config.max_rejoin_downtime_days = 0;
            config.anti_entropy_interval_secs = 5;
            config.tombstone_gc_interval_secs = 5;
        });
        assert_eq!(config.gc_grace(), std::time::Duration::from_secs(75));
        let gc = config.tombstone_gc();
        assert_eq!(gc.interval, std::time::Duration::from_secs(5));
        // Three nodes: a turn of rounds takes the interval twice.
        assert_eq!(
            gc.tombstone_policy.repair_turn,
            std::time::Duration::from_secs(10)
        );
        assert_eq!(
            gc.tombstone_policy.write_settle,
            std::time::Duration::from_secs(60)
        );
        assert_eq!(config.validate(), Ok(()));
    }

    /// A grace period no longer than the rejoin downtime, a turn of rounds
    /// over the peers, the replica write timeout and the clock skew stops a
    /// node of a replicated pool from starting, and so does a GC interval of
    /// zero — with an error, not a panic, however large the terms.
    #[test]
    fn a_grace_period_that_breaks_the_invariant_is_refused() {
        let invariant = |grace: u64, interval: u64, nodes: usize| {
            cluster(|config| {
                config.gc_grace_secs = Some(grace);
                config.max_rejoin_downtime_days = 0;
                config.anti_entropy_interval_secs = interval;
                config.pool_nodes = Some(
                    (1..=nodes)
                        .map(|n| format!("node-{n}:10.0.1.{n}:9100"))
                        .collect::<Vec<_>>()
                        .join(","),
                );
            })
            .validate()
        };
        assert_eq!(invariant(75, 5, 3), Ok(()));
        // Shorter than the replica write timeout.
        let error = invariant(40, 5, 3).unwrap_err();
        assert!(error.contains("replica write timeout"), "{error}");
        // Ten nodes, an hour between rounds: a turn takes nine hours.
        let error = invariant(4000, 3600, 10).unwrap_err();
        assert!(error.contains("S4_ANTI_ENTROPY_INTERVAL_SECS"), "{error}");
        assert!(invariant(9 * 3600 + 61, 3600, 10).is_ok());
        let error = invariant(u64::MAX, u64::MAX, 3).unwrap_err();
        assert!(error.contains("add up past"), "{error}");

        let never = cluster(|config| config.tombstone_gc_interval_secs = 0);
        let error = never.validate().unwrap_err();
        assert!(error.contains("S4_TOMBSTONE_GC_INTERVAL_SECS"), "{error}");

        // A pool of erasure-coded objects has no replicated tombstones.
        let erasure_coded = cluster(|config| {
            config.gc_grace_secs = Some(40);
            config.anti_entropy_interval_secs = 60;
            config.pool_type = "ec".into();
        });
        assert_eq!(erasure_coded.validate(), Ok(()));
    }
}
