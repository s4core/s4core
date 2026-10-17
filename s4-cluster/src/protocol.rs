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
/// Version 4 marks tombstones: a node advertising it keeps a deleted key as a
/// tombstone with the delete's HLC and reports it on reads and listings.
/// Version 5 marks anti-entropy rounds: a node advertising it answers
/// `AntiEntropyLeaves` and `AntiEntropyKeys`. Version 6 marks replicated
/// bucket configurations: a node advertising it keeps their digests and
/// tombstones. Version 7 marks the stages of the move of bucket metadata: a
/// node advertising it places them by its stage and fences the writes of a
/// stage too old. Version 8 marks the cluster's control state: a node
/// advertising it holds the IAM records of version 2 on every node of the
/// pool, with their digests and tombstones. Version 9 announces durable pool
/// generations, separating the configured roster from the ring in use.
///
/// A bump must stay backward compatible — see [`MIN_COMPATIBLE_VERSION`]. It is
/// not a licence to change the wire format; it is a claim that a node can do
/// everything every lower version could, plus something new.
pub const PROTOCOL_VERSION: u32 = 9;

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

/// Lowest protocol version whose node deletes one exact generation of an
/// object when a delete carries `expected_source_hlc`.
///
/// The field arrived with the erasure-coding work while the protocol was still
/// at 1, and a node that predates it reads such a delete as an unconditional
/// one. Every release that advertises 2 or more has it (v1.1.0 onwards; v1.0.0
/// advertises 1 and has not), so hint delivery sends a conditional delete only
/// to a node at this version or above.
pub const CONDITIONAL_DELETE_MIN_PROTOCOL_VERSION: u32 = 2;

/// Lowest protocol version whose node keeps tombstones.
///
/// Such a node takes a `DeleteBlob` with an HLC and no `expected_source_hlc`
/// as a tombstone that a write older than the delete cannot replace. A node
/// below it removes the record whatever it holds, a write newer than the
/// delete included. So a delete repaired onto a replica after the fact — by
/// read repair or hint delivery — is sent only to a node at this version or
/// above; a client's delete reaches every replica as it always did.
pub const TOMBSTONE_MIN_PROTOCOL_VERSION: u32 = 4;

/// Lowest protocol version whose node answers anti-entropy rounds.
///
/// A round asks its peer for the leaves and keys of their shared tree. A node
/// with tombstones but without rounds (protocol 4, a build of the branch
/// between the two) would answer `Unimplemented`, so a node starts rounds only
/// once every node of its pool announces at least this version; until then a
/// round is skipped and counted as such (plan ANTI-ENTROPY.md, decision 10).
pub const ANTI_ENTROPY_MIN_PROTOCOL_VERSION: u32 = 5;

/// Lowest protocol version whose node keeps the bucket configurations as
/// replicated keys: with a version digest, and a tombstone for a delete.
///
/// Two nodes on either side of it give a different set of keys a digest, so
/// the leaves of their shared tree differ wherever a configuration lies, and
/// a round between them would settle keys one of them does not replicate.
/// Rounds start only once every node of the pool announces at least this
/// version, and a node answers rounds only from a node whose digests are of
/// its own format (plan BUCKET-CONFIG-REPLICATION.md, decision 7).
pub const BUCKET_CONFIG_MIN_PROTOCOL_VERSION: u32 = 6;

/// Lowest protocol version whose node places bucket metadata by the stages
/// of their move to the pool's replication factor: it writes and reads them
/// by the rules of its stage, and its replicas refuse the writes of a stage
/// too old (plan BUCKET-METADATA-REPLICATION.md, decisions 4 and 6).
///
/// The gate of the first stage asks every node of the pool for it: a node of
/// an earlier release writes bucket metadata to the old layout alone, and
/// its writes carry no stage a replica could fence them by.
pub const BUCKET_METADATA_MIN_PROTOCOL_VERSION: u32 = 7;

/// Lowest protocol version whose node holds the cluster's control state: the
/// IAM records of version 2 (`__system__/__s4_iam_v2_*`) on every node of the
/// pool, written and read through a majority, with a version digest and a
/// tombstone for a delete (plan CLUSTER-IAM-REPLICATION.md, decisions 1, 2
/// and 9).
///
/// A node below it keeps such a key as an unreplicated record of `__system__`:
/// no digest, and a delete that leaves nothing behind. So the coordinators
/// send the writes, deletes and reads of the class only to nodes that
/// announced at least this version, and anti-entropy rounds wait until every
/// live node of the pool does: below it a node gives another set of keys a
/// digest.
pub const IAM_REPLICATION_MIN_PROTOCOL_VERSION: u32 = 8;

/// Lowest protocol version that announces and preserves pool generations.
/// A growth gate requires this capability from every member of the new roster.
pub const POOL_GENERATION_MIN_PROTOCOL_VERSION: u32 = 9;

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

    /// The tombstone floor names a version this binary speaks, and one above
    /// every version released before tombstones existed.
    #[test]
    fn tombstone_capability_is_within_the_supported_range() {
        const {
            assert!(
                TOMBSTONE_MIN_PROTOCOL_VERSION <= PROTOCOL_VERSION,
                "this binary keeps tombstones but does not advertise it"
            )
        };
        const {
            assert!(
                TOMBSTONE_MIN_PROTOCOL_VERSION > EC_SET_LAYOUT_MIN_PROTOCOL_VERSION,
                "protocol 3 was released without tombstones"
            )
        };
    }

    /// The anti-entropy floor names a version this binary speaks, and one
    /// above tombstones: a node with tombstones alone answers no round.
    #[test]
    fn anti_entropy_capability_is_within_the_supported_range() {
        const {
            assert!(
                ANTI_ENTROPY_MIN_PROTOCOL_VERSION <= PROTOCOL_VERSION,
                "this binary answers anti-entropy rounds but does not advertise it"
            )
        };
        const {
            assert!(
                ANTI_ENTROPY_MIN_PROTOCOL_VERSION > TOMBSTONE_MIN_PROTOCOL_VERSION,
                "protocol 4 keeps tombstones but answers no round"
            )
        };
    }

    /// The bucket configuration floor names a version this binary speaks, and
    /// one above anti-entropy rounds: protocol 5 gave configurations no digest.
    #[test]
    fn bucket_config_capability_is_within_the_supported_range() {
        const {
            assert!(
                BUCKET_CONFIG_MIN_PROTOCOL_VERSION <= PROTOCOL_VERSION,
                "this binary replicates bucket configurations but does not advertise it"
            )
        };
        const {
            assert!(
                BUCKET_CONFIG_MIN_PROTOCOL_VERSION > ANTI_ENTROPY_MIN_PROTOCOL_VERSION,
                "protocol 5 runs rounds without the digests of bucket configurations"
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
