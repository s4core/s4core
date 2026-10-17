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

//! Application initialization and runtime.
//!
//! This module handles:
//! - Storage engine initialization
//! - HTTP server setup and routing
//! - TLS/HTTPS configuration
//! - Graceful shutdown

use crate::cluster_bootstrap;
use crate::compaction_worker::CompactionWorker;
use crate::config::{Config, ServerMode};
use crate::edition;
use crate::lifecycle_worker::LifecycleWorker;
use crate::multipart_cleanup_worker::MultipartCleanupWorker;
use crate::secrets::{check_iam_users, resolve_jwt_secret, JwtSecret};
use anyhow::{Context, Result};
use axum::http::Uri;
use axum::ServiceExt;
use s4_api::{create_router, AppState};
use s4_cluster::{ClusterLimits, ClusterServices, NodeLifecycle, PROTOCOL_VERSION, S4_VERSION};
use s4_core::storage::{BitcaskStorageEngine, IndexOptions, VersionDigestsOpening};
use s4_features::iam::{
    IamStorage, KeyOrigin, LegacyConversion, SecretKey, SecretKeySettings, SecretKeyring,
};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tower::Service;
use tracing::{info, warn};

/// S3-aware path normalizer that only strips trailing slashes on bucket-level paths.
///
/// Unlike `NormalizePath::trim_trailing_slash`, this preserves trailing slashes on
/// object key paths (2+ path segments) because the trailing slash is part of the S3
/// object key. For example, `PUT /bucket/dir/` creates an object with key `dir/`.
///
/// Only single-segment paths (bucket operations like `/bucket/`) have their trailing
/// slash removed, since those need to route to `/:bucket` handlers.
#[derive(Clone)]
struct S3NormalizePath<S> {
    inner: S,
}

impl<S, B> Service<axum::http::Request<B>> for S3NormalizePath<S>
where
    S: Service<axum::http::Request<B>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::result::Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: axum::http::Request<B>) -> Self::Future {
        let path = req.uri().path();

        // Only normalize trailing slash for single-segment paths (bucket-level routes).
        // Multi-segment paths like /bucket/key/ keep their trailing slash because
        // the slash is part of the S3 object key.
        if path.ends_with('/') && path.len() > 1 {
            let segments = path.split('/').filter(|s| !s.is_empty()).count();
            if segments <= 1 {
                if let Some(new_uri) = strip_trailing_slash(req.uri()) {
                    *req.uri_mut() = new_uri;
                }
            }
        }

        self.inner.call(req)
    }
}

/// Rebuild a URI with the trailing slash removed from the path.
fn strip_trailing_slash(uri: &Uri) -> Option<Uri> {
    let path = uri.path().trim_end_matches('/');
    let path_and_query = if let Some(query) = uri.query() {
        format!("{path}?{query}")
    } else {
        path.to_string()
    };
    Uri::builder().path_and_query(path_and_query).build().ok()
}

/// Main application.
pub struct App {
    config: Config,
    /// Storage engine.
    storage: BitcaskStorageEngine,
    /// Edition-derived cluster limits (CE or EE).
    #[allow(dead_code)]
    cluster_limits: ClusterLimits,
    /// The edition as resolved at start, reported in the metrics.
    edition: edition::ResolvedEdition,
    /// Trait-object DI container for edition-dependent services.
    #[allow(dead_code)]
    cluster_services: ClusterServices,
    /// Cluster node lifecycle tracker (drain/shutdown coordination).
    node_lifecycle: Arc<NodeLifecycle>,
    /// Secret that signs and verifies admin JWTs.
    jwt_secret: JwtSecret,
    /// What the configuration says about the keys the secrets of IAM users'
    /// S3 credentials are encrypted with.
    iam_keys: SecretKeySettings,
}

impl App {
    /// Creates a new application instance.
    ///
    /// Initializes the storage engine with configuration settings.
    pub async fn new(config: Config) -> Result<Self> {
        info!("S4 {} starting", edition::edition_label());

        // Validate cluster configuration early
        config.cluster.validate().map_err(|e| anyhow::anyhow!(e))?;

        // Decide the admin JWT secret before touching storage: a secret that is
        // too short, or a cluster node without the shared one, stops here.
        let jwt_secret = resolve_jwt_secret(
            config.security.jwt_secret.as_deref(),
            config.cluster.mode,
            config.security.root_password.is_some(),
        )?;
        log_jwt_secret(&jwt_secret, config.cluster.mode);
        let iam_keys = iam_key_settings(&config, &jwt_secret)?;

        // Log operating mode
        match config.cluster.mode {
            ServerMode::Single => info!("Operating mode: standalone (single-node)"),
            ServerMode::Cluster => {
                info!(
                    cluster_name = %config.cluster.cluster_name,
                    protocol_version = PROTOCOL_VERSION,
                    s4_version = S4_VERSION,
                    rf = config.cluster.replication_factor,
                    wq = config.cluster.write_quorum,
                    rq = config.cluster.read_quorum,
                    "Operating mode: cluster"
                );
            }
            ServerMode::Gateway => {
                info!(
                    cluster_name = %config.cluster.cluster_name,
                    "Operating mode: gateway (stateless router)"
                );
            }
        }

        // Resolve CE/EE edition limits and services
        let resolved_edition = edition::resolve_edition();
        let cluster_limits = resolved_edition.limits.clone();
        let cluster_services = edition::build_cluster_services(&cluster_limits);

        // Create lifecycle tracker
        let node_lifecycle = NodeLifecycle::new();

        info!("Initializing S4 application...");

        // Refuse unsupported single-node transitions before opening the engine:
        // that open may recover data and rebuild digests. Existing cluster
        // configurations from older releases remain eligible for adoption.
        let has_cluster_identity =
            s4_cluster::node_identity_path(&config.storage.data_path).exists();
        if config.cluster.is_cluster() {
            BitcaskStorageEngine::preflight_cluster_transition(
                &config.storage.data_path,
                &config.storage.metadata_path,
                config.cluster.pool_name.as_deref().unwrap_or("unnamed"),
                config
                    .cluster
                    .pool_storage_class()
                    .map_err(anyhow::Error::msg)?
                    .is_erasure_coded(),
                has_cluster_identity,
            )
            .await?;
        }

        // Ensure directories exist
        if let Some(parent) = config.storage.data_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        if let Some(parent) = config.storage.metadata_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::create_dir_all(&config.storage.data_path).await?;

        // Initialize storage engine. A cluster node keeps the version digest
        // of every replicated key, which anti-entropy compares replicas by.
        let storage = BitcaskStorageEngine::open(
            &config.storage.data_path,
            &config.storage.metadata_path,
            config.tuning.volume_size_mb * 1024 * 1024, // Convert MB to bytes
            config.tuning.inline_threshold,
            config.tuning.strict_sync,
            IndexOptions {
                version_digests: config.cluster.is_cluster(),
            },
        )
        .await?;
        // Opening without digests is also used by rollback maintenance. Only
        // a real single-server start records the intent to serve single writes.
        if config.cluster.is_single() || (config.cluster.is_cluster() && !has_cluster_identity) {
            storage.index_db().mark_single_mode_open().await?;
        }
        if config.cluster.is_cluster() {
            if !config
                .cluster
                .pool_storage_class()
                .map_err(anyhow::Error::msg)?
                .is_erasure_coded()
            {
                s4_cluster::migration::MigrationWorker::prepare(storage.index_db()).await?;
            } else if !has_cluster_identity {
                // Preflight proved an empty new EC source. Close the origin
                // mark only after checking that no legacy or queued debt exists.
                let index = storage.index_db();
                if index.legacy_migration_page(true, None, 1).await?.is_empty()
                    && index.legacy_migration_page(false, None, 1).await?.is_empty()
                    && index.legacy_migration_queue_page(None, 1).await?.is_empty()
                {
                    index.mark_legacy_migration_complete().await?;
                } else {
                    anyhow::bail!("single-node migration obligations require a replicated pool");
                }
            }
        }
        log_version_digests(storage.index_db().version_digests_opening());

        info!("Storage engine initialized successfully");

        Ok(Self {
            config,
            storage,
            cluster_limits,
            edition: resolved_edition,
            cluster_services,
            node_lifecycle,
            jwt_secret,
            iam_keys,
        })
    }

    /// Runs the application (HTTP/HTTPS server).
    ///
    /// If TLS is configured via `S4_TLS_CERT` and `S4_TLS_KEY` environment variables,
    /// the server will use HTTPS. Otherwise, it runs as HTTP.
    pub async fn run(self) -> Result<()> {
        // Validate TLS configuration early
        self.config
            .server
            .tls
            .validate()
            .map_err(|e| anyhow::anyhow!("TLS configuration error: {}", e))?;

        info!("S4 Server starting...");
        info!("Storage path: {:?}", self.config.storage.data_path);
        info!("Metadata path: {:?}", self.config.storage.metadata_path);

        // Credentials come from `Config::load`: S4_ACCESS_KEY_ID and S4_SECRET_ACCESS_KEY
        // or their `_FILE` forms. Use chars() to safely handle multi-byte UTF-8 characters
        let key_preview: String = self.config.security.access_key_id.chars().take(8).collect();
        info!("Using access key: {}...", key_preview);
        info!(
            "Max upload size: {} bytes ({:.2} GB)",
            self.config.server.max_upload_size,
            self.config.server.max_upload_size as f64 / (1024.0 * 1024.0 * 1024.0)
        );

        // Parse bind address
        let addr: SocketAddr = self.config.server.bind.parse()?;

        // Check if TLS is enabled and load configuration
        let tls_config = if self.config.server.tls.enabled {
            Some(self.load_tls_config().await?)
        } else {
            None
        };

        // Initialize Prometheus metrics recorder if enabled
        let prometheus_handle = if self.config.metrics.prometheus_enabled {
            match s4_api::prometheus::builder().and_then(|builder| builder.install_recorder()) {
                Ok(handle) => {
                    info!("Prometheus metrics enabled (available at /metrics)");
                    s4_cluster::describe_metrics();
                    edition::record_edition_metrics(&self.edition);
                    Some(handle)
                }
                Err(e) => {
                    tracing::warn!(
                        "Failed to install Prometheus recorder: {}. Metrics disabled.",
                        e
                    );
                    None
                }
            }
        } else {
            info!("Prometheus metrics disabled");
            None
        };

        // Create application state (consumes self.storage)
        let mut state = AppState::with_max_upload_size(
            self.storage,
            self.config.security.access_key_id.clone(),
            self.config.security.secret_access_key.clone(),
            self.config.server.max_upload_size,
            &self.config.storage.data_path,
        )
        .await
        .with_jwt_secret(self.jwt_secret.value())
        .with_default_bucket_storage_class(
            self.config.cluster.pool_storage_class().map_err(|e| anyhow::anyhow!(e))?,
        );
        if let Some(handle) = prometheus_handle {
            state = state.with_prometheus_handle(handle);
        }
        if let Some(password) = &self.config.security.root_password {
            let root = s4_features::iam::RootAccount::new(
                &self.config.security.root_username,
                password,
                self.jwt_secret.value(),
            )
            .map_err(|e| anyhow::anyhow!("Failed to set up the root account: {e}"))?;
            state = state.with_root_account(root);
        }

        // The keys the secrets of S3 credentials are sealed with, before any
        // record is read or written (plan CLUSTER-IAM-REPLICATION.md, decision 7).
        let iam_keyring = state
            .iam_storage
            .use_secret_keys(self.iam_keys.clone())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to set up the IAM encryption keys: {e}"))?;
        log_iam_keys(&iam_keyring);

        // Which IAM records this node serves, decided before anything reads them
        // and before the node joins a cluster.
        prepare_iam(&state, &self.config).await?;

        // IAM users left from an earlier start need the shared secret as much as
        // S4_ROOT_PASSWORD does. Checked before the node joins the cluster, and
        // only where the answer can stop the start.
        if matches!(self.jwt_secret, JwtSecret::Generated(_)) && !self.config.cluster.is_single() {
            let iam_users = state
                .iam_storage
                .list_users()
                .await
                .map_err(|e| anyhow::anyhow!("Failed to read IAM users: {e}"))?
                .len();
            check_iam_users(&self.jwt_secret, self.config.cluster.mode, iam_users)?;
        }

        // Bootstrap cluster services if in cluster mode
        let mut cluster_state = if self.config.cluster.is_cluster() {
            let cs = cluster_bootstrap::bootstrap_cluster(
                &self.config.cluster,
                state.storage.clone(),
                Some(state.iam_storage.directory()),
                &self.config.storage.data_path,
                &self.cluster_limits,
            )
            .await
            .context("Failed to bootstrap cluster services")?;

            // Inject quorum coordinators into AppState so handlers route through them
            state = state.with_cluster_coordinators(
                cs.write_coordinator.clone(),
                cs.read_coordinator.clone(),
                cs.list_coordinator.clone(),
                cs.iam_lease.clone(),
            );
            state = state
                .with_iam_freshness(cs.iam_freshness.clone())
                .with_iam_status(cs.iam_status.clone());
            if let Some(ec_admin_service) = cs.ec_admin_service.clone() {
                state = state.with_ec_admin_service(ec_admin_service);
            }
            if let Some(ec_object_runtime) = cs.ec_object_runtime.clone() {
                state = state.with_ec_object_runtime(ec_object_runtime);
            }
            state = state.with_cluster_admin_service(cs.cluster_admin_service.clone());
            state = state.with_config_read_gate(cs.config_read_gate.clone());
            state = state.with_unenforced_configs(cs.unenforced_configs.clone());

            // IAM changes wait until no node of the pool keeps IAM of its own
            // the pool has not taken over (plan CLUSTER-IAM-REPLICATION.md,
            // decision 8).
            let parts = &cs.iam_adoption;
            let adoption = s4_api::cluster_iam::ClusterIamAdoption::new(
                parts.collector.clone(),
                parts.gate.clone(),
            );
            let node_name = adoption.local_name();
            state = state.with_iam_adoption(adoption).with_iam_node_name(&node_name);
            // Root, the JWT secret and the encryption key, as the pool compares
            // its nodes by them (decision 6).
            parts.config.announce(s4_cluster::IamConfigPrint {
                root: state.auth_service.root().map(|root| root.fingerprint_bytes()),
                jwt_secret: match &self.jwt_secret {
                    JwtSecret::Configured(secret) => {
                        Some(s4_features::iam::jwt_secret_fingerprint(secret))
                    }
                    JwtSecret::Generated(_) => None,
                },
                encryption_key: iam_keyring.shared_key_fingerprint(),
            });
            let worker = crate::iam_adoption::IamAdoptionWorker::new(
                state.iam_storage.clone(),
                root_name(&self.config).map(str::to_string),
                parts.node_id,
                parts.pool.clone(),
                parts.gossip.clone(),
                parts.gate.clone(),
                Some(parts.index_db.clone()),
            );
            let shutdown = parts.shutdown.clone();
            let mut upkeep = crate::iam_upkeep::IamUpkeepWorker::new(
                (*state.iam_storage).clone(),
                Some(parts.config.clone()),
                self.config.security.iam_audit_retention_days,
            );
            upkeep.report_keys().await;
            let mut cs = cs;
            cs.background_handles.push(tokio::spawn(worker.run(shutdown.clone())));
            cs.background_handles.push(tokio::spawn(upkeep.run(shutdown)));

            Some(cs)
        } else {
            None
        };

        // A single node keeps its IAM by itself: its secrets sealed with the
        // current key, its audit log within the retention.
        let (iam_upkeep_stop, iam_upkeep_handle) = if self.config.cluster.is_single() {
            let (stop, stopped) = tokio::sync::watch::channel(false);
            let mut upkeep = crate::iam_upkeep::IamUpkeepWorker::new(
                (*state.iam_storage).clone(),
                None,
                self.config.security.iam_audit_retention_days,
            );
            upkeep.report_keys().await;
            (Some(stop), Some(tokio::spawn(upkeep.run(stopped))))
        } else {
            (None, None)
        };

        // Root from the configured credentials, if any
        if let Some(handle) = start_root_account(&state).await? {
            if let Some(cs) = cluster_state.as_mut() {
                cs.background_handles.push(handle);
            }
        }

        // Cleanup orphaned multipart temp files from previous server instance
        match state.part_store.cleanup_orphaned().await {
            Ok(count) if count > 0 => {
                info!("Cleaned up {} orphaned multipart temp files", count);
            }
            Err(e) => {
                tracing::warn!("Failed to cleanup orphaned multipart temp files: {:?}", e);
            }
            _ => {}
        }

        // Spawn multipart cleanup worker
        let multipart_cleanup_handle = {
            info!(
                "Starting multipart cleanup worker (TTL: {} hours, interval: {} hours)",
                self.config.multipart.upload_ttl_hours,
                self.config.multipart.cleanup_interval_hours,
            );
            let worker = MultipartCleanupWorker::new(
                state.part_store.clone(),
                self.config.multipart.clone(),
            );
            worker.spawn()
        };

        // Spawn lifecycle worker if enabled
        let lifecycle_handle = if self.config.lifecycle.enabled {
            info!(
                "Starting lifecycle worker (interval: {} s, dry_run: {})",
                self.config.lifecycle.interval().as_secs(),
                self.config.lifecycle.dry_run
            );
            let mut worker =
                LifecycleWorker::new(state.storage.clone(), self.config.lifecycle.clone());
            if let Some(cs) = &cluster_state {
                worker = worker.with_cluster_clock(cs.clock.clone()).with_cluster_buckets(
                    cs.list_coordinator.clone(),
                    cs.read_coordinator.clone(),
                    cs.config_read_gate.clone(),
                );
            }
            Some(worker.spawn())
        } else {
            info!("Lifecycle worker disabled");
            None
        };

        // Spawn compaction worker if enabled
        let compaction_handle = if self.config.compaction.enabled {
            info!(
                "Starting compaction worker (interval: {} hours, threshold: {:.0}%, dry_run: {})",
                self.config.compaction.interval_hours,
                self.config.compaction.fragmentation_threshold * 100.0,
                self.config.compaction.dry_run,
            );
            let worker = if self.config.cluster.is_cluster() {
                CompactionWorker::new_cluster(state.storage.clone(), self.config.compaction.clone())
            } else {
                CompactionWorker::new(state.storage.clone(), self.config.compaction.clone())
            };
            Some(worker.spawn())
        } else {
            info!("Compaction worker disabled");
            None
        };

        // Kept to seal the version digests once everything that writes stopped.
        let sealed_storage = state.storage.clone();

        // Create router
        let router = create_router(state);

        // Run server with or without TLS
        let result = if let Some(rustls_config) = tls_config {
            info!("Listening on https://{}", addr);
            run_https_server(addr, router, rustls_config).await
        } else {
            info!("Listening on http://{}", addr);
            run_http_server(addr, router).await
        };

        // Run cluster graceful shutdown if in cluster mode
        if self.config.cluster.is_cluster() {
            let drain_timeout =
                std::time::Duration::from_secs(self.config.cluster.drain_timeout_secs);
            if let Err(e) = s4_cluster::graceful_shutdown(&self.node_lifecycle, drain_timeout).await
            {
                tracing::warn!("Cluster graceful shutdown error: {}", e);
            }
        }

        // Abort background workers on shutdown
        multipart_cleanup_handle.abort();
        info!("Multipart cleanup worker stopped");
        if let Some(handle) = lifecycle_handle {
            handle.abort();
            info!("Lifecycle worker stopped");
        }
        if let Some(handle) = compaction_handle {
            handle.abort();
            info!("Compaction worker stopped");
        }
        if let Some(handle) = iam_upkeep_handle {
            drop(iam_upkeep_stop);
            handle.abort();
        }

        // Shutdown cluster background workers and gRPC server
        if let Some(cs) = cluster_state {
            for handle in cs.background_handles {
                handle.abort();
            }
            if let Err(e) = cs.rpc_handle.shutdown().await {
                tracing::warn!("gRPC server shutdown error: {}", e);
            }
            info!("Cluster services stopped");
        }

        // Nothing writes to the index any more: its version digests match its
        // records, and the next start keeps them unless something wrote since.
        if let Err(e) = sealed_storage.read().await.index_db().seal_version_digests() {
            tracing::warn!(error = %e, "the version digests could not be sealed; the next start rebuilds them unless an earlier seal still matches");
        }

        result
    }

    /// Loads TLS configuration from certificate and key files.
    async fn load_tls_config(&self) -> Result<axum_server::tls_rustls::RustlsConfig> {
        use axum_server::tls_rustls::RustlsConfig;

        let tls_config = &self.config.server.tls;

        let cert_path =
            tls_config.cert_path.as_ref().context("TLS certificate path not configured")?;
        let key_path =
            tls_config.key_path.as_ref().context("TLS private key path not configured")?;

        info!("Loading TLS certificate from {:?}", cert_path);
        info!("Loading TLS private key from {:?}", key_path);

        // Load TLS configuration
        let rustls_config = RustlsConfig::from_pem_file(cert_path, key_path)
            .await
            .context("Failed to load TLS certificate and key")?;

        info!("TLS configured successfully");
        Ok(rustls_config)
    }
}

/// Runs the HTTP server (without TLS).
async fn run_http_server(addr: SocketAddr, router: axum::Router) -> Result<()> {
    // Create TCP listener
    let listener = TcpListener::bind(addr).await?;

    // Normalize paths: strip trailing slashes only for bucket-level routes,
    // preserving them for object keys (e.g., `dir/` is a valid S3 key).
    let app = S3NormalizePath { inner: router };

    // Run server with graceful shutdown
    axum::serve(
        listener,
        ServiceExt::<axum::http::Request<axum::body::Body>>::into_make_service(app),
    )
    .with_graceful_shutdown(shutdown_signal())
    .await?;

    info!("Server shutdown complete");
    Ok(())
}

/// Runs the HTTPS server (with TLS).
async fn run_https_server(
    addr: SocketAddr,
    router: axum::Router,
    rustls_config: axum_server::tls_rustls::RustlsConfig,
) -> Result<()> {
    // Create handle for graceful shutdown
    let handle = axum_server::Handle::new();
    let shutdown_handle = handle.clone();

    // Spawn shutdown signal handler
    tokio::spawn(async move {
        shutdown_signal().await;
        shutdown_handle.graceful_shutdown(Some(std::time::Duration::from_secs(30)));
    });

    // Normalize paths: strip trailing slashes only for bucket-level routes,
    // preserving them for object keys (e.g., `dir/` is a valid S3 key).
    let app = S3NormalizePath { inner: router };

    // Run HTTPS server
    axum_server::bind_rustls(addr, rustls_config)
        .handle(handle)
        .serve(ServiceExt::<axum::http::Request<axum::body::Body>>::into_make_service(app))
        .await?;

    info!("Server shutdown complete");
    Ok(())
}

/// Handles graceful shutdown signals.
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c().await.expect("Failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("Failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {
            info!("Received Ctrl+C, starting graceful shutdown...");
        }
        _ = terminate => {
            info!("Received terminate signal, starting graceful shutdown...");
        }
    }
}

/// The keys the configuration names for the secrets of IAM users' S3
/// credentials: `S4_IAM_ENCRYPTION_KEY`, `S4_IAM_ENCRYPTION_KEY_PREVIOUS`, and
/// the JWT secret the key is derived from without them — a configured one
/// only: a generated secret is gone at the next start, and so would be every
/// secret sealed with a key derived from it.
///
/// # Errors
///
/// A key that is not 32 bytes in hex stops the start.
fn iam_key_settings(config: &Config, jwt_secret: &JwtSecret) -> Result<SecretKeySettings> {
    let key = |value: &Option<String>, name: &str, origin: KeyOrigin| {
        value
            .as_deref()
            .map(|hex| SecretKey::from_hex(hex, origin))
            .transpose()
            .map_err(|e| anyhow::anyhow!("{name}: {e}"))
    };
    Ok(SecretKeySettings {
        key: key(
            &config.security.iam_encryption_key,
            "S4_IAM_ENCRYPTION_KEY",
            KeyOrigin::Configured,
        )?,
        previous: key(
            &config.security.iam_encryption_key_previous,
            "S4_IAM_ENCRYPTION_KEY_PREVIOUS",
            KeyOrigin::Previous,
        )?,
        jwt_secret: match jwt_secret {
            JwtSecret::Configured(secret) => Some(secret.clone()),
            JwtSecret::Generated(_) => None,
        },
    })
}

/// Logs which key seals the secrets of S3 credentials, by its id.
fn log_iam_keys(keys: &SecretKeyring) {
    let current = keys.current();
    let readable: Vec<&str> =
        keys.keys().map(SecretKey::id).filter(|id| *id != current.id()).collect();
    info!(
        key_id = %current.id(),
        origin = ?current.origin(),
        also_readable = ?readable,
        "Secrets of IAM users' S3 credentials are encrypted with AES-256-GCM"
    );
}

/// Logs where the admin JWT secret came from, and what that means for sessions.
fn log_jwt_secret(secret: &JwtSecret, mode: ServerMode) {
    match secret {
        JwtSecret::Generated(_) if mode == ServerMode::Single => tracing::warn!(
            "S4_JWT_SECRET is not set: admin tokens are signed with a random secret \
             generated at startup, so admin sessions end when the server restarts. \
             Set S4_JWT_SECRET or S4_JWT_SECRET_FILE to keep them"
        ),
        JwtSecret::Generated(_) => tracing::warn!(
            "S4_JWT_SECRET is not set: this node signs admin tokens with its own random \
             secret, so a token works only on the node that issued it. Set the same \
             S4_JWT_SECRET or S4_JWT_SECRET_FILE on every node"
        ),
        JwtSecret::Configured(_) => info!("Admin JWT secret: configured"),
    }
}

/// Reports what opening the storage found about the version digests: whether
/// they were kept since the last start or rebuilt from the records.
fn log_version_digests(opening: Option<&VersionDigestsOpening>) {
    match opening {
        Some(VersionDigestsOpening::Current) => {
            info!("Version digests are current, no rebuild needed");
        }
        Some(VersionDigestsOpening::Rebuilt {
            records,
            elapsed,
            previous_format,
        }) => {
            let reason = match previous_format {
                Some(format) => format!("stored in layout {format}"),
                None => "not built or not kept by every write".to_string(),
            };
            info!(
                records,
                elapsed_ms = elapsed.as_millis() as u64,
                "Version digests rebuilt ({reason})"
            );
        }
        None => {}
    }
}

/// Decides which IAM records this node serves (plan CLUSTER-IAM-REPLICATION.md,
/// decisions 2 and 8).
///
/// A single node's IAM is all there is: the records an earlier release stored
/// become records of version 2 at once. A cluster node cannot do that on its
/// own — its IAM of an earlier release may hold users deleted through another
/// node, and spreading it would bring them back on every node — so while it
/// holds such users it serves them as before and refuses changes until the
/// cluster takes them over. A stored root without S3 credentials is no such
/// user: root is configured now.
async fn prepare_iam(state: &AppState, config: &Config) -> Result<()> {
    let root_name = root_name(config);
    match config.cluster.mode {
        ServerMode::Single => {
            let conversion = state.iam_storage.convert_legacy(root_name).await.map_err(|e| {
                anyhow::anyhow!("Failed to convert the IAM of an earlier release: {e}")
            })?;
            if conversion != LegacyConversion::default() {
                info!(
                    converted = conversion.converted,
                    already_current = conversion.already_current,
                    dropped_root = conversion.dropped_root,
                    "IAM users stored by an earlier release now have one record each"
                );
            }
            // Should this node ever start as a cluster node, its users are its
            // own IAM, which the pool takes over only as the operator decides.
            state.iam_storage.mark_single_node_users().await.map_err(|e| {
                anyhow::anyhow!("Failed to record that this single node holds IAM users: {e}")
            })?;
        }
        ServerMode::Cluster => {
            let taken_back =
                state.iam_storage.take_back_single_node_users().await.map_err(|e| {
                    anyhow::anyhow!("Failed to set aside the IAM users of a single node: {e}")
                })?;
            if taken_back > 0 {
                info!(
                    users = taken_back,
                    "This node held IAM users as a single node: they are its own IAM until the \
                     cluster takes them over (GET /api/admin/iam/adoption)"
                );
            }
            let legacy: Vec<String> = state
                .iam_storage
                .legacy_users()
                .await
                .map_err(|e| anyhow::anyhow!("Failed to read the IAM of an earlier release: {e}"))?
                .into_iter()
                .filter(|user| IamStorage::legacy_user_matters(user, root_name))
                .map(|user| user.username)
                .collect();
            if !legacy.is_empty() {
                state.iam_storage.serve_legacy().await.map_err(|e| {
                    anyhow::anyhow!("Failed to load the IAM of an earlier release: {e}")
                })?;
                warn!(
                    users = ?legacy,
                    "This node holds IAM users of its own, stored on it alone by an earlier \
                     release or as a single node. It serves them as before, only through this \
                     node, and the cluster refuses IAM changes (409 IamAdoptionPending) until \
                     they are taken over: see GET and POST /api/admin/iam/adoption"
                );
            }
        }
        ServerMode::Gateway => {}
    }
    Ok(())
}

/// Root's name when the environment configures root: a stored user of that
/// name without S3 credentials means nothing any more.
fn root_name(config: &Config) -> Option<&str> {
    config
        .security
        .root_password
        .as_ref()
        .map(|_| config.security.root_username.as_str())
}

/// Starts root from the configured credentials.
///
/// Root is not stored: logins with `S4_ROOT_USERNAME` (default `root`) are
/// checked against `S4_ROOT_PASSWORD` (or their `_FILE` forms), so a new
/// password takes effect at this start, and the sessions root had under the old
/// one end. Nothing happens when no password is configured.
///
/// Root's state is the pool's on a cluster node: a majority of the pool has to
/// answer, which it may not yet while the pool starts. The node then starts
/// anyway and records root's start in the background, retrying until the pool
/// answers; until then root's logins and tokens are checked against the state
/// this node holds. The returned task is that retry.
async fn start_root_account(state: &AppState) -> Result<Option<tokio::task::JoinHandle<()>>> {
    let Some(root) = state.auth_service.root().cloned() else {
        info!("Root account disabled (S4_ROOT_PASSWORD not set)");
        return Ok(None);
    };
    if let Ok(Some(stored)) = state.iam_storage.get_user_by_username(root.username()).await {
        warn!(
            "A stored user '{}' (id {}) left by an earlier release is ignored: root is \
             configured by S4_ROOT_USERNAME and S4_ROOT_PASSWORD now. S3 credentials \
             issued to it keep working until the user is deleted through the Admin API",
            stored.username, stored.id
        );
    }
    if !state.is_cluster_mode() {
        let started = state
            .auth_service
            .start_root()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to read the root account state: {e}"))?;
        log_root_start(root.username(), started.is_some_and(|(_, ended)| ended));
        return Ok(None);
    }
    let auth_service = state.auth_service.clone();
    Ok(Some(tokio::spawn(async move {
        let mut attempt: u32 = 0;
        loop {
            match auth_service.start_root().await {
                Ok(started) => {
                    log_root_start(root.username(), started.is_some_and(|(_, ended)| ended));
                    return;
                }
                Err(e) => {
                    if attempt.is_multiple_of(ROOT_START_WARN_EVERY) {
                        warn!(
                            error = %e,
                            "Root's state cannot be recorded in the pool yet; root logs in \
                             against this node's copy and the start is retried"
                        );
                    }
                    attempt = attempt.wrapping_add(1);
                    tokio::time::sleep(ROOT_START_RETRY).await;
                }
            }
        }
    })))
}

/// How often a cluster node retries recording root's start in the pool.
const ROOT_START_RETRY: std::time::Duration = std::time::Duration::from_secs(2);

/// Every how many failed attempts to record root's start a warning is logged.
const ROOT_START_WARN_EVERY: u32 = 30;

fn log_root_start(username: &str, sessions_ended: bool) {
    if sessions_ended {
        info!(
            "Root '{username}' starts with another password or JWT secret than before: \
             root sessions issued before this start have ended"
        );
    } else {
        info!("Root '{username}' configured from the environment");
    }
}

#[cfg(test)]
mod cluster_transition_tests {
    use super::*;
    use s4_core::{StorageEngine, VersioningStatus};
    use std::collections::HashMap;

    fn config(dir: &tempfile::TempDir) -> Config {
        let mut config = Config::default();
        config.storage.data_path = dir.path().join("volumes");
        config.storage.metadata_path = dir.path().join("metadata");
        config.tuning.volume_size_mb = 1;
        config.security.jwt_secret = Some("shared-test-secret-at-least-32-bytes".into());
        config.cluster.mode = ServerMode::Cluster;
        config.cluster.seeds = vec!["127.0.0.1:19100".into()];
        config.cluster.pool_name = Some("migration-pool".into());
        config.cluster.pool_nodes =
            Some("node-1=127.0.0.1:19100,node-2=127.0.0.1:19101,node-3=127.0.0.1:19102".into());
        config.cluster.grpc_addr = Some("127.0.0.1:19100".into());
        config.cluster.http_addr = Some("127.0.0.1:19000".into());
        config
    }

    async fn single(config: &Config) -> BitcaskStorageEngine {
        BitcaskStorageEngine::new(
            &config.storage.data_path,
            &config.storage.metadata_path,
            1024 * 1024,
            4096,
            true,
        )
        .await
        .unwrap()
    }

    async fn refusal(config: Config) -> String {
        match App::new(config).await {
            Err(error) => error.to_string(),
            Ok(_) => panic!("unsupported transition was allowed"),
        }
    }

    #[tokio::test]
    async fn app_refuses_enabled_and_suspended_before_digest_rebuild_or_identity() {
        for status in [VersioningStatus::Enabled, VersioningStatus::Suspended] {
            let dir = tempfile::tempdir().unwrap();
            let config = config(&dir);
            let storage = single(&config).await;
            storage
                .put_object_versioned(
                    "history",
                    "key",
                    b"old",
                    "text/plain",
                    &HashMap::new(),
                    VersioningStatus::Enabled,
                )
                .await
                .unwrap();
            storage
                .put_object_versioned(
                    "history",
                    "key",
                    b"new",
                    "text/plain",
                    &HashMap::new(),
                    VersioningStatus::Enabled,
                )
                .await
                .unwrap();
            storage
                .put_object(
                    "__system__",
                    "__s4_bucket_versioning_history",
                    status.as_xml_status().unwrap().as_bytes(),
                    "text/plain",
                    &HashMap::new(),
                )
                .await
                .unwrap();
            drop(storage);
            let original =
                std::fs::read(config.storage.data_path.join("volume_000000.dat")).unwrap();
            let error = refusal(config.clone()).await;
            assert!(error.contains("history"), "{error}");
            assert!(error.contains(status.as_xml_status().unwrap()), "{error}");
            assert!(!s4_cluster::node_identity_path(&config.storage.data_path).exists());
            assert!(!dir.path().join("metadata.digests-sealed").exists());
            assert_eq!(
                original,
                std::fs::read(config.storage.data_path.join("volume_000000.dat")).unwrap()
            );
            let storage = single(&config).await;
            assert_eq!(
                storage
                    .list_object_versions("history", "key", None, None, 10)
                    .await
                    .unwrap()
                    .versions
                    .len(),
                2
            );
            assert_eq!(
                storage.get_object("history", "key").await.unwrap().0,
                b"new"
            );
        }
    }

    #[tokio::test]
    async fn app_allows_empty_and_compatible_single_and_prior_cluster_versioning() {
        let dir = tempfile::tempdir().unwrap();
        let config = config(&dir);
        let app = App::new(config.clone()).await.unwrap();
        assert!(app.storage.index_db().keeps_version_digests());
        drop(app);
        let storage = single(&config).await;
        storage
            .put_object("bucket", "key", b"original", "text/plain", &HashMap::new())
            .await
            .unwrap();
        drop(storage);
        let app = App::new(config.clone()).await.unwrap();
        assert_eq!(
            app.storage.get_object("bucket", "key").await.unwrap().0,
            b"original"
        );
        drop(app);
        // A prior cluster is a separate completed history, not a failed
        // bootstrap of the single source above.
        let prior_dir = tempfile::tempdir().unwrap();
        let config = self::config(&prior_dir);
        let prior = BitcaskStorageEngine::open(
            &config.storage.data_path,
            &config.storage.metadata_path,
            1024 * 1024,
            4096,
            true,
            IndexOptions {
                version_digests: true,
            },
        )
        .await
        .unwrap();
        // A cluster of an earlier release may have retained versioning
        // configurations that it never enforced. It must still start.
        prior
            .put_object(
                "__system__",
                "__s4_bucket_versioning_legacy",
                b"Enabled",
                "text/plain",
                &HashMap::new(),
            )
            .await
            .unwrap();
        prior.index_db().seal_version_digests().unwrap();
        drop(prior);
        std::fs::write(
            s4_cluster::node_identity_path(&config.storage.data_path),
            7u128.to_be_bytes(),
        )
        .unwrap();
        drop(App::new(config.clone()).await.unwrap());
        let mut single_config = config.clone();
        single_config.cluster.mode = ServerMode::Single;
        let single_app = App::new(single_config).await.unwrap();
        for body in [b"v1", b"v2"] {
            single_app
                .storage
                .put_object_versioned(
                    "legacy",
                    "history",
                    body,
                    "text/plain",
                    &HashMap::new(),
                    VersioningStatus::Enabled,
                )
                .await
                .unwrap();
        }
        single_app
            .storage
            .put_object(
                "__system__",
                "__s4_bucket_versioning_legacy",
                b"Suspended",
                "text/plain",
                &HashMap::new(),
            )
            .await
            .unwrap();
        drop(single_app);
        let error = refusal(config.clone()).await;
        assert!(
            error.contains("legacy") && error.contains("Suspended"),
            "{error}"
        );
        let storage = single(&config).await;
        assert_eq!(
            storage
                .list_object_versions("legacy", "history", None, None, 10)
                .await
                .unwrap()
                .versions
                .len(),
            2
        );
        assert_eq!(
            storage.get_object("legacy", "history").await.unwrap().0,
            b"v2"
        );
    }

    #[tokio::test]
    async fn app_refuses_existing_single_data_in_ec_but_allows_empty_ec() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = config(&dir);
        config.cluster.pool_type = "ec".into();
        drop(App::new(config.clone()).await.unwrap());
        let storage = single(&config).await;
        storage
            .put_object("bucket", "key", b"single", "text/plain", &HashMap::new())
            .await
            .unwrap();
        drop(storage);
        let error = refusal(config.clone()).await;
        assert!(
            error.contains("migration-pool") && error.contains("erasure"),
            "{error}"
        );
        assert_eq!(
            single(&config).await.get_object("bucket", "key").await.unwrap().0,
            b"single"
        );
    }
    #[tokio::test]
    async fn app_still_allows_prior_cluster_versioning_after_actual_tombstone_maintenance() {
        let dir = tempfile::tempdir().unwrap();
        let config = config(&dir);
        let app = App::new(config.clone()).await.unwrap();
        app.storage
            .put_object(
                "__system__",
                "__s4_bucket_versioning_legacy",
                b"Enabled",
                "text/plain",
                &HashMap::new(),
            )
            .await
            .unwrap();
        app.storage.tombstone_object("bucket", "gone", 20, 0, 7).await.unwrap();
        app.storage.index_db().mark_legacy_migration_complete().await.unwrap();
        app.storage.index_db().seal_version_digests().unwrap();
        drop(app);
        std::fs::write(
            s4_cluster::node_identity_path(&config.storage.data_path),
            7u128.to_be_bytes(),
        )
        .unwrap();
        assert_eq!(crate::rollback::purge_tombstones(&config).await.unwrap(), 1);
        let app = App::new(config).await.unwrap();
        assert!(
            !app.storage.index_db().legacy_migration_complete().await.unwrap(),
            "maintenance must still invalidate completion/history"
        );
        assert!(matches!(
            app.storage.index_db().version_digests_opening(),
            Some(VersionDigestsOpening::Rebuilt { .. })
        ));
        assert_eq!(
            app.storage
                .get_object("__system__", "__s4_bucket_versioning_legacy")
                .await
                .unwrap()
                .0,
            b"Enabled"
        );
    }
    #[tokio::test]
    async fn legacy_originals_are_captured_before_bootstrap_can_expose_replica_writes() {
        let dir = tempfile::tempdir().unwrap();
        let config = config(&dir);
        let storage = single(&config).await;
        storage
            .put_object("bucket", "legacy", b"source", "text/plain", &HashMap::new())
            .await
            .unwrap();
        drop(storage);
        let app = App::new(config).await.unwrap();
        assert!(app
            .storage
            .index_db()
            .legacy_migration_key_pending("bucket/legacy")
            .await
            .unwrap());
        assert!(!s4_cluster::node_identity_path(&app.config.storage.data_path).exists());
    }

    #[tokio::test]
    async fn a_failed_first_cluster_start_cannot_reclassify_legacy_single_data_as_ec() {
        let dir = tempfile::tempdir().unwrap();
        let config = config(&dir);
        let storage = single(&config).await; // Also models a binary before explicit single-mode marks.
        storage
            .put_object("bucket", "legacy", b"source", "text/plain", &HashMap::new())
            .await
            .unwrap();
        drop(storage);
        let app = App::new(config.clone()).await.unwrap();
        s4_cluster::load_node_identity(&config.storage.data_path, None).unwrap(); // Bootstrap fails immediately afterwards.
        drop(app);
        let mut ec = config.clone();
        ec.cluster.pool_type = "ec".into();
        let error = refusal(ec).await;
        assert!(error.contains("erasure"), "{error}");
        assert_eq!(
            single(&config).await.get_object("bucket", "legacy").await.unwrap().0,
            b"source"
        );
    }
}
