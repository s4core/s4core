// Copyright 2026 S4Core Team
//
// Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

//! License loading, parsing, and Ed25519 signature verification.

use chrono::{DateTime, Utc};
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use serde::{Deserialize, Serialize};
use std::path::Path;

use crate::capabilities::Capabilities;
use crate::error::LicenseError;

/// Ed25519 public key for **production** license signature verification.
///
/// The corresponding private key is stored offline and used exclusively
/// by the `license-gen` tool to sign license payloads. Generated 2026-04-09.
///
/// **IMPORTANT**: The test keypair is separate and defined in `#[cfg(test)]`
/// below. The production private key is NEVER present in source code.
const PUBLIC_KEY_BYTES: [u8; 32] = [
    0xc6, 0xf6, 0x76, 0x1e, 0xb1, 0xf9, 0xbb, 0x5e, 0x88, 0xdf, 0x74, 0x01, 0x1f, 0x66, 0xd8, 0xc6,
    0x04, 0x0a, 0xfd, 0x59, 0x99, 0xcc, 0x61, 0xfe, 0x20, 0x92, 0x98, 0xb0, 0x08, 0x8d, 0x04, 0x98,
];

/// A validated enterprise license.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct License {
    /// Customer display name.
    pub customer: String,
    /// Unique customer identifier.
    pub customer_id: String,
    /// When this license was issued.
    pub issued_at: DateTime<Utc>,
    /// When this license expires.
    pub expires_at: DateTime<Utc>,
    /// Capabilities granted by this license.
    pub capabilities: Capabilities,
}

impl License {
    /// Returns `true` if the license has not yet expired.
    pub fn is_valid(&self) -> bool {
        Utc::now() < self.expires_at
    }

    /// Returns `true` if the license has expired.
    pub fn is_expired(&self) -> bool {
        !self.is_valid()
    }
}

/// Signed license payload: base64(json) + "." + base64(signature).
#[derive(Debug)]
struct SignedPayload {
    json_bytes: Vec<u8>,
    signature: Signature,
}

impl SignedPayload {
    fn parse(encoded: &str) -> Result<Self, LicenseError> {
        let encoded = encoded.trim();
        let dot_pos = encoded.rfind('.').ok_or_else(|| {
            LicenseError::InvalidEncoding("missing signature separator '.'".into())
        })?;

        let payload_b64 = &encoded[..dot_pos];
        let sig_b64 = &encoded[dot_pos + 1..];

        use base64::Engine;
        let engine = base64::engine::general_purpose::STANDARD;

        let json_bytes = engine
            .decode(payload_b64)
            .map_err(|e| LicenseError::InvalidEncoding(format!("payload: {e}")))?;

        let sig_bytes = engine
            .decode(sig_b64)
            .map_err(|e| LicenseError::InvalidEncoding(format!("signature: {e}")))?;

        let signature =
            Signature::from_slice(&sig_bytes).map_err(|_| LicenseError::InvalidSignature)?;

        Ok(Self {
            json_bytes,
            signature,
        })
    }
}

/// Verify the Ed25519 signature and parse the license payload.
pub fn validate_license(encoded: &str) -> Result<License, LicenseError> {
    validate_license_with_key(encoded, &PUBLIC_KEY_BYTES)
}

/// Verify a license against a specific Ed25519 public key.
///
/// Production code should use [`validate_license`] which uses the
/// embedded production key. This function is exposed for testing
/// with a separate test keypair.
fn validate_license_with_key(
    encoded: &str,
    public_key: &[u8; 32],
) -> Result<License, LicenseError> {
    let payload = SignedPayload::parse(encoded)?;

    let verifying_key =
        VerifyingKey::from_bytes(public_key).map_err(|_| LicenseError::InvalidSignature)?;

    verifying_key
        .verify(&payload.json_bytes, &payload.signature)
        .map_err(|_| LicenseError::InvalidSignature)?;

    let license: License = serde_json::from_slice(&payload.json_bytes)?;

    if license.is_expired() {
        return Err(LicenseError::Expired(license.expires_at.to_rfc3339()));
    }

    Ok(license)
}

/// Load a license from environment variables.
///
/// Checks in order:
/// 1. `S4_LICENSE_DEV=1` — returns development license (debug builds only)
/// 2. `S4_LICENSE_KEY` — base64-encoded signed license
/// 3. `S4_LICENSE_FILE` — path to license file
pub fn load_license() -> Result<License, LicenseError> {
    // Dev/CI shortcut (debug builds only)
    #[cfg(any(test, debug_assertions))]
    {
        if std::env::var("S4_LICENSE_DEV").unwrap_or_default() == "1" {
            tracing::warn!("Using development license — not for production use");
            return Ok(License {
                customer: "Development".into(),
                customer_id: "dev-000".into(),
                issued_at: Utc::now(),
                expires_at: Utc::now() + chrono::Duration::days(365),
                capabilities: Capabilities::development(),
            });
        }
    }

    // Try environment variable
    if let Ok(key) = std::env::var("S4_LICENSE_KEY") {
        return validate_license(&key);
    }

    // Try file path
    if let Ok(path) = std::env::var("S4_LICENSE_FILE") {
        let content = std::fs::read_to_string(Path::new(&path))?;
        return validate_license(&content);
    }

    Err(LicenseError::NotFound)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// Mutex to serialize tests that manipulate environment variables.
    /// `set_var`/`remove_var` affect the whole process, so parallel tests race.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    // -----------------------------------------------------------------------
    // Test-only keypair — NOT the production key.
    //
    // The production private key is NEVER in source code. These test keys
    // are a separate keypair used exclusively for unit tests.
    // -----------------------------------------------------------------------
    const TEST_PUBLIC_KEY: [u8; 32] = [
        0x93, 0x73, 0xdc, 0xa9, 0xd6, 0xaa, 0x35, 0x97, 0x2e, 0x8b, 0x0f, 0x60, 0x4e, 0x9d, 0x44,
        0xe8, 0x13, 0x7d, 0xca, 0x45, 0xad, 0x93, 0xf1, 0xd7, 0x8e, 0x9e, 0x7e, 0x1a, 0xc8, 0x91,
        0xd6, 0x3b,
    ];
    const TEST_PRIVATE_SEED: &str = "EmsxsGpkANqm7m8Mhvh1sAIB3Cd+O7ABKZIgO8WRL+Y=";

    /// Helper: create an Ed25519 signing key from the test seed.
    fn test_signing_key() -> ed25519_dalek::SigningKey {
        use base64::Engine;
        let engine = base64::engine::general_purpose::STANDARD;
        let seed_bytes = engine.decode(TEST_PRIVATE_SEED).unwrap();
        let mut seed = [0u8; 32];
        seed.copy_from_slice(&seed_bytes);
        ed25519_dalek::SigningKey::from_bytes(&seed)
    }

    /// Helper: sign a JSON payload and return a license key string.
    fn sign_payload(signing_key: &ed25519_dalek::SigningKey, json_bytes: &[u8]) -> String {
        use base64::Engine;
        use ed25519_dalek::Signer;
        let engine = base64::engine::general_purpose::STANDARD;
        let signature = signing_key.sign(json_bytes);
        let payload_b64 = engine.encode(json_bytes);
        let sig_b64 = engine.encode(signature.to_bytes());
        format!("{payload_b64}.{sig_b64}")
    }

    #[test]
    fn test_keypair_is_consistent() {
        let sk = test_signing_key();
        assert_eq!(sk.verifying_key().to_bytes(), TEST_PUBLIC_KEY);
    }

    #[test]
    fn test_key_differs_from_production_key() {
        // Ensure we are NOT accidentally using the production key for tests.
        assert_ne!(TEST_PUBLIC_KEY, PUBLIC_KEY_BYTES);
    }

    #[test]
    fn load_dev_license() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("S4_LICENSE_DEV", "1");
        let license = load_license().expect("dev license should load");
        assert_eq!(license.customer, "Development");
        assert!(license.is_valid());
        assert_eq!(license.capabilities.max_pools, u32::MAX);
        std::env::remove_var("S4_LICENSE_DEV");
    }

    #[test]
    fn no_license_returns_not_found() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::remove_var("S4_LICENSE_DEV");
        std::env::remove_var("S4_LICENSE_KEY");
        std::env::remove_var("S4_LICENSE_FILE");
        let result = load_license();
        assert!(matches!(result, Err(LicenseError::NotFound)));
    }

    #[test]
    fn invalid_encoding_rejected() {
        let result = validate_license("not-a-valid-license");
        assert!(matches!(result, Err(LicenseError::InvalidEncoding(_))));
    }

    #[test]
    fn round_trip_sign_and_validate() {
        let signing_key = test_signing_key();

        let payload = serde_json::json!({
            "customer": "Test Corp",
            "customer_id": "test-001",
            "issued_at": "2026-04-01T00:00:00Z",
            "expires_at": "2027-04-01T00:00:00Z",
            "capabilities": {
                "max_pools": 10,
                "max_nodes_per_pool": 20,
                "deep_scrubber": true,
                "rolling_upgrades": true,
                "ldap": true,
                "saml": false,
                "webdav": true,
                "audit_log": true
            }
        });

        let json_bytes = serde_json::to_vec(&payload).unwrap();
        let license_key = sign_payload(&signing_key, &json_bytes);

        // Validate against the TEST key (not production)
        let license = validate_license_with_key(&license_key, &TEST_PUBLIC_KEY)
            .expect("license should validate with test key");
        assert_eq!(license.customer, "Test Corp");
        assert_eq!(license.customer_id, "test-001");
        assert_eq!(license.capabilities.max_pools, 10);
        assert_eq!(license.capabilities.max_nodes_per_pool, 20);
        assert!(license.capabilities.deep_scrubber);
        assert!(!license.capabilities.saml);
        assert!(license.is_valid());
    }

    #[test]
    fn test_signed_license_rejected_by_production_key() {
        // A license signed with the test key must NOT pass production validation.
        let signing_key = test_signing_key();

        let payload = serde_json::json!({
            "customer": "Test Corp",
            "customer_id": "test-001",
            "issued_at": "2026-04-01T00:00:00Z",
            "expires_at": "2027-04-01T00:00:00Z",
            "capabilities": {
                "max_pools": 10,
                "max_nodes_per_pool": 20,
                "deep_scrubber": true,
                "rolling_upgrades": true,
                "ldap": true,
                "saml": false,
                "webdav": true,
                "audit_log": true
            }
        });

        let json_bytes = serde_json::to_vec(&payload).unwrap();
        let license_key = sign_payload(&signing_key, &json_bytes);

        // validate_license uses PUBLIC_KEY_BYTES (production) — must reject test-signed key
        let result = validate_license(&license_key);
        assert!(
            matches!(result, Err(LicenseError::InvalidSignature)),
            "test-signed license must not pass production validation"
        );
    }

    #[test]
    fn tampered_payload_rejected() {
        let signing_key = test_signing_key();

        let payload = serde_json::json!({
            "customer": "Legit Corp",
            "customer_id": "legit-001",
            "issued_at": "2026-04-01T00:00:00Z",
            "expires_at": "2027-04-01T00:00:00Z",
            "capabilities": {
                "max_pools": 1,
                "max_nodes_per_pool": 3,
                "deep_scrubber": false,
                "rolling_upgrades": false,
                "ldap": false,
                "saml": false,
                "webdav": false,
                "audit_log": false
            }
        });

        let json_bytes = serde_json::to_vec(&payload).unwrap();
        let license_key = sign_payload(&signing_key, &json_bytes);

        // Tamper: re-encode with different payload but same signature
        use base64::Engine;
        let engine = base64::engine::general_purpose::STANDARD;
        let sig_b64 = license_key.split('.').next_back().unwrap();

        let tampered = serde_json::json!({
            "customer": "Legit Corp",
            "customer_id": "legit-001",
            "issued_at": "2026-04-01T00:00:00Z",
            "expires_at": "2027-04-01T00:00:00Z",
            "capabilities": {
                "max_pools": 999999,
                "max_nodes_per_pool": 999999,
                "deep_scrubber": true,
                "rolling_upgrades": true,
                "ldap": true,
                "saml": true,
                "webdav": true,
                "audit_log": true
            }
        });
        let tampered_bytes = serde_json::to_vec(&tampered).unwrap();
        let tampered_b64 = engine.encode(&tampered_bytes);
        let tampered_key = format!("{tampered_b64}.{sig_b64}");

        let result = validate_license_with_key(&tampered_key, &TEST_PUBLIC_KEY);
        assert!(matches!(result, Err(LicenseError::InvalidSignature)));
    }

    #[test]
    fn expired_license_rejected() {
        let signing_key = test_signing_key();

        let payload = serde_json::json!({
            "customer": "Expired Corp",
            "customer_id": "exp-001",
            "issued_at": "2024-01-01T00:00:00Z",
            "expires_at": "2025-01-01T00:00:00Z",
            "capabilities": {
                "max_pools": 10,
                "max_nodes_per_pool": 10,
                "deep_scrubber": true,
                "rolling_upgrades": true,
                "ldap": true,
                "saml": true,
                "webdav": true,
                "audit_log": true
            }
        });

        let json_bytes = serde_json::to_vec(&payload).unwrap();
        let license_key = sign_payload(&signing_key, &json_bytes);

        let result = validate_license_with_key(&license_key, &TEST_PUBLIC_KEY);
        assert!(
            matches!(result, Err(LicenseError::Expired(_))),
            "expired license must be rejected"
        );
    }
}
