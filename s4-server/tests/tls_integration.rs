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

//! TLS integration tests for S4 server.
//!
//! These tests verify TLS configuration and certificate loading functionality.

use std::io::Write;
use std::path::PathBuf;
use tempfile::TempDir;

/// Test helper to generate a self-signed certificate and key.
fn generate_test_certs(dir: &std::path::Path) -> (PathBuf, PathBuf) {
    use std::process::Command;

    let cert_path = dir.join("cert.pem");
    let key_path = dir.join("key.pem");

    // Generate self-signed certificate using openssl
    let status = Command::new("openssl")
        .args([
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-keyout",
            key_path.to_str().unwrap(),
            "-out",
            cert_path.to_str().unwrap(),
            "-days",
            "1",
            "-nodes",
            "-subj",
            "/CN=localhost",
        ])
        .output();

    match status {
        Ok(output) if output.status.success() => (cert_path, key_path),
        Ok(output) => {
            eprintln!(
                "OpenSSL stderr: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            panic!("OpenSSL command failed");
        }
        Err(e) => {
            panic!(
                "Failed to run openssl: {}. Make sure openssl is installed.",
                e
            );
        }
    }
}

/// Test that TLS configuration is correctly loaded from PEM files.
#[tokio::test]
async fn test_tls_config_loads_valid_certs() {
    use axum_server::tls_rustls::RustlsConfig;

    // Create temporary directory for certificates
    let temp_dir = TempDir::new().expect("Failed to create temp directory");

    // Generate test certificates
    let (cert_path, key_path) = generate_test_certs(temp_dir.path());

    // Verify files exist
    assert!(cert_path.exists(), "Certificate file should exist");
    assert!(key_path.exists(), "Key file should exist");

    // Try to load TLS configuration
    let result = RustlsConfig::from_pem_file(&cert_path, &key_path).await;

    assert!(
        result.is_ok(),
        "Should successfully load TLS config: {:?}",
        result.err()
    );
}

/// Test that TLS configuration fails with invalid certificate.
#[tokio::test]
async fn test_tls_config_fails_with_invalid_cert() {
    use axum_server::tls_rustls::RustlsConfig;

    // Create temporary directory
    let temp_dir = TempDir::new().expect("Failed to create temp directory");

    let cert_path = temp_dir.path().join("invalid_cert.pem");
    let key_path = temp_dir.path().join("invalid_key.pem");

    // Write invalid certificate content
    let mut cert_file = std::fs::File::create(&cert_path).unwrap();
    cert_file.write_all(b"not a valid certificate").unwrap();

    let mut key_file = std::fs::File::create(&key_path).unwrap();
    key_file.write_all(b"not a valid key").unwrap();

    // Try to load TLS configuration - should fail
    let result = RustlsConfig::from_pem_file(&cert_path, &key_path).await;

    assert!(result.is_err(), "Should fail to load invalid TLS config");
}

/// Test that TLS configuration fails when certificate file doesn't exist.
#[tokio::test]
async fn test_tls_config_fails_with_missing_cert() {
    use axum_server::tls_rustls::RustlsConfig;

    let cert_path = PathBuf::from("/nonexistent/cert.pem");
    let key_path = PathBuf::from("/nonexistent/key.pem");

    let result = RustlsConfig::from_pem_file(&cert_path, &key_path).await;

    assert!(
        result.is_err(),
        "Should fail when certificate file doesn't exist"
    );
}

/// Test TlsConfig validation logic.
#[test]
fn test_tls_config_validation() {
    use s4_server::config::TlsConfig;

    // Test disabled TLS - should always validate
    let disabled = TlsConfig {
        enabled: false,
        cert_path: None,
        key_path: None,
    };
    assert!(disabled.validate().is_ok());

    // Test enabled TLS with both paths - should validate
    let valid = TlsConfig {
        enabled: true,
        cert_path: Some(PathBuf::from("/path/to/cert.pem")),
        key_path: Some(PathBuf::from("/path/to/key.pem")),
    };
    assert!(valid.validate().is_ok());

    // Test enabled TLS without cert - should fail
    let no_cert = TlsConfig {
        enabled: true,
        cert_path: None,
        key_path: Some(PathBuf::from("/path/to/key.pem")),
    };
    assert!(no_cert.validate().is_err());

    // Test enabled TLS without key - should fail
    let no_key = TlsConfig {
        enabled: true,
        cert_path: Some(PathBuf::from("/path/to/cert.pem")),
        key_path: None,
    };
    assert!(no_key.validate().is_err());
}
