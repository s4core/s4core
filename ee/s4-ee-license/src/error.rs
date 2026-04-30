// Copyright 2026 S4Core Team
//
// Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

//! License validation errors.

/// Errors that can occur during license validation.
#[derive(Debug, thiserror::Error)]
pub enum LicenseError {
    /// No license key found in environment or file.
    #[error("no license key found: set S4_LICENSE_KEY or S4_LICENSE_FILE")]
    NotFound,

    /// License payload could not be decoded from base64.
    #[error("invalid license encoding: {0}")]
    InvalidEncoding(String),

    /// License JSON payload could not be parsed.
    #[error("invalid license format: {0}")]
    InvalidFormat(#[from] serde_json::Error),

    /// Ed25519 signature verification failed.
    #[error("invalid license signature")]
    InvalidSignature,

    /// License has expired.
    #[error("license expired at {0}")]
    Expired(String),

    /// License file could not be read.
    #[error("failed to read license file: {0}")]
    IoError(#[from] std::io::Error),
}
