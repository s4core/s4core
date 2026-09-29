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
/// Bumped when new RPC fields/methods are added, and also when the binary gains
/// a cluster-wide capability that peers must be able to detect before they may
/// use it. Version 2 marks EC v2: a node advertising it can follow an
/// auto-tiering migration from RF=3 into erasure coding. Version 3 marks
/// erasure-set layout records: a node advertising it takes a set's slot order
/// from the set's record instead of recomputing it from topology labels.
///
/// A bump must stay backward compatible — see [`MIN_COMPATIBLE_VERSION`]. It is
/// not a licence to change the wire format; it is a claim that a node can do
/// everything every lower version could, plus something new.
pub const PROTOCOL_VERSION: u32 = 3;

/// Minimum protocol version this binary can interoperate with.
///
/// Set equal to `PROTOCOL_VERSION` for a breaking change; otherwise
/// keep it at 1 to maintain backward compatibility with older nodes.
pub const MIN_COMPATIBLE_VERSION: u32 = 1;

/// Lowest protocol version whose node can take part in EC v2 auto-tiering.
///
/// Read by the rolling-upgrade gate in `gossip.rs`: a pool migrates objects
/// into erasure coding only once every member advertises at least this much.
/// Named rather than written as a literal `2` so that the next protocol bump
/// does not silently change which nodes count as EC v2 capable.
pub const EC_V2_MIN_PROTOCOL_VERSION: u32 = 2;

/// Lowest protocol version whose node reads erasure-set layout records.
///
/// A node below it rebuilds every set from topology labels. For a set laid out
/// by a rule other than EC v1 that gives a different slot order, and the node
/// would refuse that set's manifests. So creating such a set, or applying such
/// a rule to an existing one, waits until every node of the pool advertises at
/// least this version (decision EC2-084, point 4).
pub const EC_SET_LAYOUT_MIN_PROTOCOL_VERSION: u32 = 3;

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
    (MIN_COMPATIBLE_VERSION..=PROTOCOL_VERSION).contains(&remote_version)
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

    /// A protocol bump must not lock out the nodes it is rolling past.
    ///
    /// The EC v2 capability is advertised by raising `PROTOCOL_VERSION`, which
    /// only works if a cluster mid-upgrade still interoperates: the whole point
    /// of the tiering gate is that old and new nodes coexist while the operator
    /// works through them one at a time. Raising `MIN_COMPATIBLE_VERSION` along
    /// with `PROTOCOL_VERSION` would make the first upgraded node refuse the
    /// five that have not been touched yet.
    #[test]
    fn every_shipped_version_stays_interoperable() {
        for version in MIN_COMPATIBLE_VERSION..=PROTOCOL_VERSION {
            assert!(
                is_compatible(version),
                "version {version} must stay supported"
            );
            assert!(
                check_compatibility(version).is_ok(),
                "version {version} must stay supported"
            );
        }
        const {
            assert!(
                MIN_COMPATIBLE_VERSION == 1,
                "dropping v1 support is a breaking change"
            )
        };
    }

    /// The capability marker must name a version this binary actually speaks.
    ///
    /// Compile-time, because every term is a constant: a build where EC v2 is
    /// implemented but never advertised should not get as far as running tests.
    #[test]
    fn ec_v2_capability_is_within_the_supported_range() {
        const {
            assert!(
                EC_V2_MIN_PROTOCOL_VERSION >= MIN_COMPATIBLE_VERSION,
                "the EC v2 floor is below the versions this binary still talks to"
            )
        };
        const {
            assert!(
                EC_V2_MIN_PROTOCOL_VERSION <= PROTOCOL_VERSION,
                "this binary implements EC v2 but does not advertise it"
            )
        };
    }

    /// Same guarantee for erasure-set layout records, plus the ordering the
    /// capabilities were introduced in: a node that reads layout records is an
    /// EC v2 node, never the other way round.
    #[test]
    fn set_layout_capability_is_within_the_supported_range() {
        const {
            assert!(
                EC_SET_LAYOUT_MIN_PROTOCOL_VERSION <= PROTOCOL_VERSION,
                "this binary reads layout records but does not advertise it"
            )
        };
        const {
            assert!(
                EC_SET_LAYOUT_MIN_PROTOCOL_VERSION > EC_V2_MIN_PROTOCOL_VERSION,
                "a plain EC v2 node does not read layout records, so it must not qualify"
            )
        };
    }
}
