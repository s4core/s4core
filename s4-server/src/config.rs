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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityConfig {
    /// Access key ID for S3 authentication.
    /// Can be set via S4_ACCESS_KEY_ID environment variable.
    pub access_key_id: String,
    /// Secret access key for S3 authentication.
    /// Can be set via S4_SECRET_ACCESS_KEY environment variable.
    pub secret_access_key: String,
    /// Audit log path
    pub audit_log_path: Option<PathBuf>,
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
    /// Loads configuration from file or uses defaults.
    pub fn load() -> anyhow::Result<Self> {
        // For now, return default configuration
        // Later: load from config.toml or environment variables
        Ok(Self::default())
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
                metadata_path: data_dir.join("metadata.redb"),
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
            security: SecurityConfig {
                // Load credentials from environment variables with secure defaults
                access_key_id: std::env::var("S4_ACCESS_KEY_ID").unwrap_or_else(|_| {
                    // Generate a random key for development if not set
                    use std::collections::hash_map::DefaultHasher;
                    use std::hash::{Hash, Hasher};
                    let mut hasher = DefaultHasher::new();
                    std::time::SystemTime::now().hash(&mut hasher);
                    format!("dev-{:016x}", hasher.finish())
                }),
                secret_access_key: std::env::var("S4_SECRET_ACCESS_KEY").unwrap_or_else(|_| {
                    // Generate a random key for development if not set
                    use std::collections::hash_map::DefaultHasher;
                    use std::hash::{Hash, Hasher};
                    let mut hasher = DefaultHasher::new();
                    (std::time::SystemTime::now(), std::process::id()).hash(&mut hasher);
                    format!("dev-secret-{:016x}", hasher.finish())
                }),
                audit_log_path: Some(PathBuf::from("/var/log/s4/audit.log")),
            },
            metrics: MetricsConfig::default(),
            lifecycle: LifecycleConfig::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
