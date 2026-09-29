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

// The EC pool-health snapshot is one `serde_json::json!` object with a field
// per runtime setting, and the macro expands one recursion level per field.
// Raised rather than split, because the snapshot is a contract an acceptance
// test reads and is easier to check against the configuration when it is
// written in one place.
#![recursion_limit = "256"]

pub use s4_ee_license::{self, load_license, Capabilities, License, LicenseError};

/// Runtime-independent erasure coding codec engine.
pub use s4_codec as codec;

/// Runtime controls for Enterprise codec jobs.
pub mod codec_runtime;

/// Enterprise erasure coding anti-entropy and scrubbing.
pub mod ec_anti_entropy;

/// Enterprise erasure coding durable metadata.
pub mod ec_metadata;

/// Enterprise erasure coding duplicate-content measurement.
pub mod ec_dedup;

/// Enterprise erasure coding metrics.
pub mod ec_metrics;

/// Enterprise erasure coding codec engine selection.
pub mod ec_codec;

/// Enterprise erasure coding operations and admin surfaces.
pub mod ec_ops;

/// Enterprise erasure coding deterministic test hooks.
pub mod ec_test_hooks;

/// Enterprise erasure coding live transcode runtime.
pub mod ec_runtime;

/// Enterprise erasure coding tombstone, orphan, and distributed GC.
pub mod ec_gc;

/// Enterprise erasure coding read path.
pub mod ec_read;

/// What the pool still owes a slot owner whose shards are being rebuilt.
pub mod ec_node_rebuild;

/// What a returning node may do before its shards are rebuilt.
pub mod ec_rejoin;

/// Enterprise erasure coding repair path.
pub mod ec_repair;

/// Enterprise erasure coding repair planning: which shards a repair reads.
pub mod ec_repair_plan;

// Documented by the module itself, in `ec_packed_segments.rs`, for the same
// reason as `ec_tiering` below: a doc comment here would be joined with the
// module's own, and its links would then resolve in this scope, where
// `PackedSegmentJournal` does not exist.
pub mod ec_packed_segments;

/// Enterprise erasure coding placement of new objects across erasure sets.
pub mod ec_placement;

/// Enterprise erasure coding slot placement of LRC local groups across
/// failure domains.
pub mod ec_lrc_placement;

/// Enterprise erasure coding staged transcode path.
pub mod ec_transcode;

// Documented by the module itself, in `ec_tiering/mod.rs`. A doc comment here
// as well would be concatenated with those, and rustdoc resolves the intra-doc
// links of the combined text in this scope rather than the module's — which is
// why `[`heat`]` and its neighbours would go unresolved.
pub mod ec_tiering;

/// Enterprise erasure coding foundation.
pub mod erasure_coding;

// Enterprise feature modules will be added as they are implemented.
// Each module provides a concrete implementation of a trait defined in s4-cluster.
//
// Example (Phase 9):
//   pub mod deep_scrubber;     // impl DeepScrubber for EnterpriseDeepScrubber
//
// Example (Phase 10):
//   pub mod node_replacer;     // impl NodeReplacer for AutomatedNodeReplacer
//   pub mod rolling_upgrade;   // impl RollingUpgrader for EnterpriseRollingUpgrader
