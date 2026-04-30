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

//! Protocol versioning for rolling upgrades.
//!
//! Every gRPC message carries a `protocol_version` field. During a rolling
//! upgrade, nodes running different binary versions coexist in the cluster.
//! This module defines the compatibility rules:
//!
//! - **Minor bump** (1 → 2): backward compatible. New fields are optional;
//!   older nodes ignore unknown fields.
//! - **Major bump** (requires code change to `MIN_COMPATIBLE_VERSION`):
//!   requires a full cluster upgrade (stop writes → upgrade all → restart).
//!   Should happen extremely rarely (once per year at most).

use crate::error::ClusterError;

/// Current protocol version spoken by this binary.
///
/// Bumped when new RPC fields/methods are added.
pub const PROTOCOL_VERSION: u32 = 1;

/// Minimum protocol version this binary can interoperate with.
///
/// Set equal to `PROTOCOL_VERSION` for a breaking change; otherwise
/// keep it at 1 to maintain backward compatibility with older nodes.
pub const MIN_COMPATIBLE_VERSION: u32 = 1;

/// S4 server version string embedded in gossip metadata.
pub const S4_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Check whether a remote node's protocol version is compatible with this node.
///
/// Returns `Ok(())` if compatible, or a descriptive error otherwise.
pub fn check_compatibility(remote_version: u32) -> Result<(), ClusterError> {
    if remote_version < MIN_COMPATIBLE_VERSION {
        return Err(ClusterError::Config(format!(
            "remote protocol version {remote_version} is below minimum compatible \
             version {MIN_COMPATIBLE_VERSION}; the remote node must be upgraded"
        )));
    }
    if remote_version > PROTOCOL_VERSION {
        return Err(ClusterError::Config(format!(
            "remote protocol version {remote_version} is newer than local \
             version {PROTOCOL_VERSION}; this node must be upgraded"
        )));
    }
    Ok(())
}

/// Returns `true` if the remote version is within the supported range.
pub fn is_compatible(remote_version: u32) -> bool {
    remote_version >= MIN_COMPATIBLE_VERSION && remote_version <= PROTOCOL_VERSION
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn current_version_is_compatible() {
        assert!(is_compatible(PROTOCOL_VERSION));
        assert!(check_compatibility(PROTOCOL_VERSION).is_ok());
    }

    #[test]
    fn min_compatible_version_is_accepted() {
        assert!(is_compatible(MIN_COMPATIBLE_VERSION));
        assert!(check_compatibility(MIN_COMPATIBLE_VERSION).is_ok());
    }

    #[test]
    fn version_below_minimum_rejected() {
        if MIN_COMPATIBLE_VERSION > 0 {
            assert!(!is_compatible(MIN_COMPATIBLE_VERSION - 1));
            let err = check_compatibility(MIN_COMPATIBLE_VERSION - 1).unwrap_err();
            assert!(err.to_string().contains("below minimum"));
        }
    }

    #[test]
    fn version_above_current_rejected() {
        assert!(!is_compatible(PROTOCOL_VERSION + 1));
        let err = check_compatibility(PROTOCOL_VERSION + 1).unwrap_err();
        assert!(err.to_string().contains("newer than local"));
    }

    #[test]
    fn s4_version_is_not_empty() {
        assert!(!S4_VERSION.is_empty());
    }

    #[test]
    fn version_constants_are_consistent() {
        // Compile-time assertion: MIN_COMPATIBLE_VERSION must not exceed PROTOCOL_VERSION
        const { assert!(MIN_COMPATIBLE_VERSION <= PROTOCOL_VERSION) };
    }
}
