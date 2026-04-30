// Copyright 2026 S4Core Team
//
// Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

//! # S4 Enterprise Edition
//!
//! Enterprise feature implementations for S4Core.
//! This crate provides production implementations of the trait boundaries
//! defined in `s4-cluster`, replacing the CE noop defaults.
//!
//! ## Features
//!
//! - **Deep Scrubber**: Background SHA-256 verification with replica healing
//! - **Automated Node Replacement**: Single-step drain + replace + heal
//! - **Rolling Upgrades**: Zero-downtime cluster upgrades
//! - **LDAP/SAML/OIDC**: Enterprise authentication integrations
//! - **WebDAV**: WebDAV protocol support
//! - **Audit Log**: Operation audit trail
//!
//! ## Usage
//!
//! This crate is only included when building with `--features enterprise`.
//! At startup, `s4-server` checks for a valid license key and injects
//! EE implementations via trait objects. Without a valid license, the
//! server falls back to CE defaults automatically.

pub use s4_ee_license::{self, load_license, Capabilities, License, LicenseError};

// Enterprise feature modules will be added as they are implemented.
// Each module provides a concrete implementation of a trait defined in s4-cluster.
//
// Example (Phase 9):
//   pub mod deep_scrubber;     // impl DeepScrubber for EnterpriseDeepScrubber
//
// Example (Phase 10):
//   pub mod node_replacer;     // impl NodeReplacer for AutomatedNodeReplacer
//   pub mod rolling_upgrade;   // impl RollingUpgrader for EnterpriseRollingUpgrader
