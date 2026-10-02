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
use crate::config::{Config, SecurityConfig, ServerMode};
use crate::edition;
use crate::lifecycle_worker::LifecycleWorker;
use crate::multipart_cleanup_worker::MultipartCleanupWorker;
use crate::secrets::{check_iam_users, resolve_jwt_secret, JwtSecret};
use anyhow::{Context, Result};
use axum::http::Uri;
use axum::ServiceExt;
use s4_api::{create_router, AppState};
use s4_cluster::{ClusterLimits, ClusterServices, NodeLifecycle, PROTOCOL_VERSION, S4_VERSION};
use s4_core::storage::BitcaskStorageEngine;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tower::Service;
use tracing::info;

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

        // Ensure directories exist
        if let Some(parent) = config.storage.data_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        if let Some(parent) = config.storage.metadata_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::create_dir_all(&config.storage.data_path).await?;

        // Initialize storage engine
        let storage = BitcaskStorageEngine::new(
            &config.storage.data_path,
            &config.storage.metadata_path,
            config.tuning.volume_size_mb * 1024 * 1024, // Convert MB to bytes
            config.tuning.inline_threshold,
            config.tuning.strict_sync,
        )
        .await?;

        info!("Storage engine initialized successfully");

        Ok(Self {
            config,
            storage,
            cluster_limits,
            edition: resolved_edition,
            cluster_services,
            node_lifecycle,
            jwt_secret,
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
        let cluster_state = if self.config.cluster.is_cluster() {
            let cs = cluster_bootstrap::bootstrap_cluster(
                &self.config.cluster,
                state.storage.clone(),
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
            );
            if let Some(ec_admin_service) = cs.ec_admin_service.clone() {
                state = state.with_ec_admin_service(ec_admin_service);
            }
            if let Some(ec_object_runtime) = cs.ec_object_runtime.clone() {
                state = state.with_ec_object_runtime(ec_object_runtime);
            }
            state = state.with_cluster_admin_service(cs.cluster_admin_service.clone());

            Some(cs)
        } else {
            None
        };

        // Initialize root user from the configured credentials, if any
        initialize_root_user(&state, &self.config.security).await?;

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
                "Starting lifecycle worker (interval: {} hours, dry_run: {})",
                self.config.lifecycle.interval_hours, self.config.lifecycle.dry_run
            );
            let worker = LifecycleWorker::new(state.storage.clone(), self.config.lifecycle.clone());
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

/// Creates the root user from the configured credentials.
///
/// Credentials come from `S4_ROOT_USERNAME` (default `root`) and
/// `S4_ROOT_PASSWORD`, or their `_FILE` forms. Nothing happens when no password
/// is configured or the root user already exists.
async fn initialize_root_user(state: &AppState, security: &SecurityConfig) -> Result<()> {
    use s4_features::iam::Role;

    let root_username = security.root_username.clone();
    let root_password = match &security.root_password {
        Some(password) => password.clone(),
        None => {
            info!("Root user initialization skipped (S4_ROOT_PASSWORD not set)");
            return Ok(());
        }
    };

    // Check if root user already exists
    match state.iam_storage.user_exists(&root_username).await {
        Ok(true) => {
            info!("Root user '{}' already exists", root_username);
            return Ok(());
        }
        Ok(false) => {
            // Continue with initialization
        }
        Err(e) => {
            tracing::warn!("Failed to check if root user exists: {:?}", e);
            // Continue with initialization attempt
        }
    }

    // Create root user
    info!("Creating root user '{}'...", root_username);
    match state
        .iam_storage
        .create_user(root_username.clone(), root_password, Role::SuperUser)
        .await
    {
        Ok(user) => {
            info!(
                "Root user '{}' created successfully (ID: {})",
                user.username, user.id
            );
            info!("Use /api/admin/login to authenticate and create S3 credentials");
            Ok(())
        }
        Err(e) => {
            tracing::error!("Failed to create root user: {:?}", e);
            Err(anyhow::anyhow!("Failed to create root user: {}", e))
        }
    }
}
