// Copyright 2026 S4Core Team
//
// Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

//! # S4 Enterprise License Validation
//!
//! Validates Ed25519-signed license keys for S4Core Enterprise Edition.
//! Provides capability extraction and expiry checking.
//!
//! ## License Loading
//!
//! Licenses can be loaded via:
//! - `S4_LICENSE_KEY` environment variable (base64-encoded signed payload)
//! - `S4_LICENSE_FILE` environment variable (path to license file)
//! - `S4_LICENSE_DEV=1` for development/CI (debug builds only)

mod capabilities;
mod error;
mod license;

pub use capabilities::Capabilities;
pub use error::LicenseError;
pub use license::{load_license, validate_license, License};
