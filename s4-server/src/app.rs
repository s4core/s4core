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

use crate::config::Config;
use crate::lifecycle_worker::LifecycleWorker;
use anyhow::{Context, Result};
use axum::ServiceExt;
use s4_api::{create_router, AppState};
use s4_core::storage::BitcaskStorageEngine;
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tower_http::normalize_path::NormalizePath;
use tracing::info;

/// Main application.
pub struct App {
    config: Config,
    /// Storage engine.
    storage: BitcaskStorageEngine,
}

impl App {
    /// Creates a new application instance.
    ///
    /// Initializes the storage engine with configuration settings.
    pub async fn new(config: Config) -> Result<Self> {
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

        Ok(Self { config, storage })
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

        // Create application state with credentials from config
        // Credentials are loaded from S4_ACCESS_KEY_ID and S4_SECRET_ACCESS_KEY env vars
        // Use chars() to safely handle multi-byte UTF-8 characters
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
            use metrics_exporter_prometheus::PrometheusBuilder;
            match PrometheusBuilder::new().install_recorder() {
                Ok(handle) => {
                    info!("Prometheus metrics enabled (available at /metrics)");
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
        );
        if let Some(handle) = prometheus_handle {
            state = state.with_prometheus_handle(handle);
        }

        // Initialize root user from environment variables if configured
        initialize_root_user(&state).await?;

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

        // Abort lifecycle worker on shutdown
        if let Some(handle) = lifecycle_handle {
            handle.abort();
            info!("Lifecycle worker stopped");
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

    // Wrap with NormalizePath to trim trailing slashes (required for S3 client compatibility)
    let app = NormalizePath::trim_trailing_slash(router);

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

    // Wrap with NormalizePath to trim trailing slashes (required for S3 client compatibility)
    let app = NormalizePath::trim_trailing_slash(router);

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

/// Initializes root user from environment variables if configured.
///
/// Environment variables:
/// - `S4_ROOT_USERNAME` - Username for root user (default: "root")
/// - `S4_ROOT_PASSWORD` - Password for root user (required for initialization)
///
/// If root user already exists, this function does nothing.
async fn initialize_root_user(state: &AppState) -> Result<()> {
    use s4_features::iam::Role;

    // Get root credentials from environment
    let root_username = std::env::var("S4_ROOT_USERNAME").unwrap_or_else(|_| "root".to_string());
    let root_password = match std::env::var("S4_ROOT_PASSWORD") {
        Ok(password) => password,
        Err(_) => {
            // No root password configured - skip initialization
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
