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

//! TLS configuration for inter-node gRPC communication.

use std::path::PathBuf;

use tonic::transport::{Certificate, ClientTlsConfig, Identity, ServerTlsConfig};

use crate::error::ClusterError;

/// TLS configuration for inter-node gRPC traffic.
///
/// Supports server-side TLS and optional mutual TLS (mTLS) where both
/// client and server authenticate via certificates.
#[derive(Debug, Clone)]
pub struct TlsConfig {
    /// Path to the PEM-encoded server/client certificate.
    pub cert_path: PathBuf,
    /// Path to the PEM-encoded private key.
    pub key_path: PathBuf,
    /// Path to the PEM-encoded CA certificate for verifying peers.
    /// When set, enables mutual TLS (mTLS).
    pub ca_cert_path: Option<PathBuf>,
    /// TLS domain name override for certificate validation.
    /// Useful when nodes use IP addresses instead of hostnames.
    pub domain_override: Option<String>,
}

impl TlsConfig {
    /// Build a tonic [`ServerTlsConfig`] from this configuration.
    pub fn server_config(&self) -> Result<ServerTlsConfig, ClusterError> {
        let cert = std::fs::read(&self.cert_path).map_err(|e| {
            ClusterError::Tls(format!("failed to read cert {:?}: {}", self.cert_path, e))
        })?;
        let key = std::fs::read(&self.key_path).map_err(|e| {
            ClusterError::Tls(format!("failed to read key {:?}: {}", self.key_path, e))
        })?;

        let identity = Identity::from_pem(cert, key);
        let mut config = ServerTlsConfig::new().identity(identity);

        if let Some(ca_path) = &self.ca_cert_path {
            let ca_cert = std::fs::read(ca_path).map_err(|e| {
                ClusterError::Tls(format!("failed to read CA cert {:?}: {}", ca_path, e))
            })?;
            config = config.client_ca_root(Certificate::from_pem(ca_cert));
        }

        Ok(config)
    }

    /// Build a tonic [`ClientTlsConfig`] from this configuration.
    pub fn client_config(&self) -> Result<ClientTlsConfig, ClusterError> {
        let cert = std::fs::read(&self.cert_path).map_err(|e| {
            ClusterError::Tls(format!("failed to read cert {:?}: {}", self.cert_path, e))
        })?;
        let key = std::fs::read(&self.key_path).map_err(|e| {
            ClusterError::Tls(format!("failed to read key {:?}: {}", self.key_path, e))
        })?;

        let identity = Identity::from_pem(cert, key);
        let mut config = ClientTlsConfig::new().identity(identity);

        if let Some(ca_path) = &self.ca_cert_path {
            let ca_cert = std::fs::read(ca_path).map_err(|e| {
                ClusterError::Tls(format!("failed to read CA cert {:?}: {}", ca_path, e))
            })?;
            config = config.ca_certificate(Certificate::from_pem(ca_cert));
        }

        if let Some(domain) = &self.domain_override {
            config = config.domain_name(domain.clone());
        }

        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tls_config_missing_cert_returns_error() {
        let config = TlsConfig {
            cert_path: PathBuf::from("/nonexistent/cert.pem"),
            key_path: PathBuf::from("/nonexistent/key.pem"),
            ca_cert_path: None,
            domain_override: None,
        };
        assert!(config.server_config().is_err());
        assert!(config.client_config().is_err());
    }
}
