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

//! Node identity types and persistence.
//!
//! Every node in an S4 cluster has a unique [`NodeId`] (UUID v4) that is
//! generated once on first startup and persisted to disk. The identity
//! never changes across restarts, ensuring the node retains ownership of
//! its data and position in the cluster.

use std::fmt;
use std::net::SocketAddr;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::error::ClusterError;

// ---------------------------------------------------------------------------
// NodeId
// ---------------------------------------------------------------------------

/// Unique identifier for a node in the cluster.
///
/// Internally a UUID v4 stored as `u128`.  Serialized as 16 big-endian
/// bytes on disk and over the wire (protobuf `bytes`).
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeId(pub u128);

impl NodeId {
    /// Size of the on-disk representation in bytes.
    const BYTES_LEN: usize = 16;

    /// Generate a new random node ID.
    pub fn generate() -> Self {
        Self(uuid::Uuid::new_v4().as_u128())
    }

    /// Encode as big-endian bytes.
    pub fn to_bytes(self) -> [u8; Self::BYTES_LEN] {
        self.0.to_be_bytes()
    }

    /// Decode from big-endian bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ClusterError> {
        let arr: [u8; Self::BYTES_LEN] = bytes.try_into().map_err(|_| {
            ClusterError::Identity(format!(
                "invalid node ID length: expected {} bytes, got {}",
                Self::BYTES_LEN,
                bytes.len()
            ))
        })?;
        Ok(Self(u128::from_be_bytes(arr)))
    }
}

impl std::str::FromStr for NodeId {
    type Err = ClusterError;

    /// Parse the display form, which is the form every admin surface uses.
    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        let trimmed = raw.trim();
        let uuid = uuid::Uuid::parse_str(trimmed).map_err(|error| {
            ClusterError::Identity(format!("node id '{trimmed}' is not a UUID: {error}"))
        })?;
        Ok(Self(uuid.as_u128()))
    }
}

impl fmt::Debug for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NodeId({})", uuid::Uuid::from_u128(self.0))
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", uuid::Uuid::from_u128(self.0))
    }
}

// ---------------------------------------------------------------------------
// PoolId
// ---------------------------------------------------------------------------

/// Identifier for a server pool.
///
/// Pools are immutable groupings of nodes.  A pool's composition never
/// changes after creation; horizontal scaling is achieved by adding new
/// pools.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PoolId(pub u32);

impl PoolId {
    /// Create a new pool identifier.
    pub fn new(id: u32) -> Self {
        Self(id)
    }

    /// Return the inner `u32` value.
    pub fn as_u32(self) -> u32 {
        self.0
    }
}

impl fmt::Display for PoolId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "pool-{}", self.0)
    }
}

// ---------------------------------------------------------------------------
// NodeAddr
// ---------------------------------------------------------------------------

/// Network addresses for inter-node communication.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeAddr {
    /// gRPC endpoint for inter-node RPCs (Phase 2+).
    pub grpc_addr: SocketAddr,
    /// HTTP endpoint for the S3 API.
    pub http_addr: SocketAddr,
    /// UDP endpoint for gossip protocol.
    pub gossip_addr: SocketAddr,
}

// ---------------------------------------------------------------------------
// NodeCapacity
// ---------------------------------------------------------------------------

/// Storage capacity information for a node, propagated via gossip.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeCapacity {
    /// Total disk capacity in bytes.
    pub total_bytes: u64,
    /// Currently used bytes.
    pub used_bytes: u64,
    /// Number of active volume files.
    pub volume_count: u32,
    /// Total number of stored objects.
    pub object_count: u64,
}

impl NodeCapacity {
    /// Available capacity in bytes.
    pub fn available_bytes(&self) -> u64 {
        self.total_bytes.saturating_sub(self.used_bytes)
    }
}

// ---------------------------------------------------------------------------
// NodeMeta
// ---------------------------------------------------------------------------

/// Edition label broadcast by each node for CE/EE compatibility checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Edition {
    /// Community Edition (Apache 2.0) — 1 pool, max 3 nodes.
    Community,
    /// Enterprise Edition (ELv2) — limits from license key.
    Enterprise,
}

impl std::fmt::Display for Edition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Edition::Community => write!(f, "Community Edition"),
            Edition::Enterprise => write!(f, "Enterprise Edition"),
        }
    }
}

/// Metadata about a node, disseminated through the gossip layer.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeMeta {
    /// Unique node identifier.
    pub node_id: NodeId,
    /// Human-readable name (set via `S4_NODE_NAME` env var).
    pub node_name: String,
    /// Network addresses.
    pub addr: NodeAddr,
    /// Pool this node belongs to.
    pub pool_id: PoolId,
    /// Current topology epoch observed by this node.
    pub topology_epoch: u64,
    /// Protocol version for rolling-upgrade compatibility.
    pub protocol_version: u32,
    /// Storage capacity snapshot.
    pub capacity: NodeCapacity,
    /// Unix timestamp (seconds) of node startup.
    pub started_at: u64,
    /// Semantic version string (e.g. `"0.1.0"`).
    pub s4_version: String,
    /// CE or EE — used to detect edition mismatches in a pool.
    ///
    /// Defaults to `Community` for backward compatibility with older nodes
    /// that don't broadcast this field.
    #[serde(default = "default_edition")]
    pub edition: Edition,
}

fn default_edition() -> Edition {
    Edition::Community
}

// ---------------------------------------------------------------------------
// NodeStatus
// ---------------------------------------------------------------------------

/// Liveness status of a cluster node as perceived locally.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeStatus {
    /// Node is reachable and healthy.
    Alive,
    /// Node has not responded to a probe; awaiting indirect confirmation.
    Suspect,
    /// Node has been declared unreachable after indirect probes failed.
    Dead,
    /// Node performed a graceful shutdown.
    Left,
}

impl NodeStatus {
    /// Every status, in the order the metrics and the admin API list them.
    pub const ALL: [NodeStatus; 4] = [
        NodeStatus::Alive,
        NodeStatus::Suspect,
        NodeStatus::Dead,
        NodeStatus::Left,
    ];

    /// The name of the status in the metrics and in the admin API.
    pub const fn as_str(self) -> &'static str {
        match self {
            NodeStatus::Alive => "alive",
            NodeStatus::Suspect => "suspect",
            NodeStatus::Dead => "dead",
            NodeStatus::Left => "left",
        }
    }
}

// ---------------------------------------------------------------------------
// GossipIdentity — foca::Identity implementation
// ---------------------------------------------------------------------------

/// Lightweight identity exchanged in every SWIM protocol message.
///
/// Kept small intentionally: richer metadata ([`NodeMeta`]) is
/// propagated via the gossip broadcast channel.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GossipIdentity {
    /// Stable node identifier (UUID v4).
    pub node_id: NodeId,
    /// UDP address used for gossip traffic.
    pub gossip_addr: SocketAddr,
    /// Monotonically increasing incarnation number.
    /// Bumped when the node re-joins after being declared down.
    pub incarnation: u64,
}

impl foca::Identity for GossipIdentity {
    type Addr = SocketAddr;

    /// Create a renewed identity with an incremented incarnation,
    /// allowing the node to automatically rejoin the cluster after
    /// being declared down.
    fn renew(&self) -> Option<Self> {
        Some(Self {
            node_id: self.node_id,
            gossip_addr: self.gossip_addr,
            incarnation: self.incarnation + 1,
        })
    }

    /// Return the gossip UDP address used for routing.
    fn addr(&self) -> SocketAddr {
        self.gossip_addr
    }

    /// Resolve address conflicts: the identity with the higher
    /// incarnation number wins.
    fn win_addr_conflict(&self, adversary: &Self) -> bool {
        self.incarnation > adversary.incarnation
    }
}

// ---------------------------------------------------------------------------
// Node ID persistence
// ---------------------------------------------------------------------------

const NODE_ID_FILENAME: &str = "node_id";

/// Where the identity this start runs under came from.
///
/// The three cases are not cosmetic: they are the only local evidence a node
/// has about whether the pool has met it before. An identity read from disk
/// belongs to a node that has run here; a generated one cannot be an identity
/// any pool remembers, because it was invented a moment ago; an adopted one is
/// the operator saying this machine takes over a node that is gone
/// (`EC2-NREP-02-002`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeIdentityOrigin {
    /// Read from the identity file this data directory already had.
    Existing,
    /// Freshly generated, because the data directory held no identity.
    Generated,
    /// Taken from `S4_EXPECTED_NODE_IDENTITY` into a blank data directory.
    Adopted,
}

impl NodeIdentityOrigin {
    /// Whether this start took over an identity whose storage it does not have.
    pub fn is_adopted(self) -> bool {
        matches!(self, Self::Adopted)
    }

    /// A short label for logs and admin responses.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Existing => "existing",
            Self::Generated => "generated",
            Self::Adopted => "adopted",
        }
    }
}

/// The identity this node starts under, and where it came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeIdentity {
    /// The persistent identity the pool addresses this node by.
    pub node_id: NodeId,
    /// How this start arrived at it.
    pub origin: NodeIdentityOrigin,
}

/// Where a data directory keeps the identity of the node that owns it.
pub fn node_identity_path(data_dir: &Path) -> std::path::PathBuf {
    data_dir.join(NODE_ID_FILENAME)
}

/// Load or generate the persistent node ID.
///
/// On first startup a new UUID v4 is generated and written to
/// `<data_dir>/node_id`.  Subsequent starts read the existing file,
/// ensuring the node retains its identity across restarts.
pub fn get_or_create_node_id(data_dir: &Path) -> Result<NodeId, ClusterError> {
    load_or_adopt_node_id(data_dir, None)
}

/// Load the persistent node ID, optionally checking it against the identity the
/// operator says this data directory belongs to.
///
/// Without an expectation this behaves exactly like
/// [`get_or_create_node_id`]: an existing file is honoured, a blank directory
/// gets a fresh UUID.
///
/// With an expectation it becomes the node replacement tool:
///
/// * a blank data directory **adopts** the expected identity instead of
///   generating one, which is how a replacement machine takes over the slots of
///   the node it replaces;
/// * a data directory that already holds a *different* identity is refused, and
///   the node does not start.
///
/// The refusal is the point. Two identities are involved in every replacement —
/// the one the operator means and the one on disk — and a silent disagreement
/// means the operator brought the replacement up for the wrong node. Nothing
/// downstream notices: the pool keeps assigning the dead node's slots to an
/// identity nobody holds, the objects go on reading through the surviving
/// shards, and the only symptom is a rebuild that never finishes.
pub fn load_or_adopt_node_id(
    data_dir: &Path,
    expected: Option<NodeId>,
) -> Result<NodeId, ClusterError> {
    load_node_identity(data_dir, expected).map(|identity| identity.node_id)
}

/// [`load_or_adopt_node_id`], keeping where the identity came from.
///
/// The identity itself is all most callers need. Node replacement needs one
/// thing more: whether this start read an identity that was already here,
/// invented one, or took over one it was handed. A blank data directory looks
/// the same to every other check, and only this distinction separates a brand
/// new node from a machine standing in for a dead one.
pub fn load_node_identity(
    data_dir: &Path,
    expected: Option<NodeId>,
) -> Result<NodeIdentity, ClusterError> {
    let id_path = node_identity_path(data_dir);

    if id_path.exists() {
        let bytes = std::fs::read(&id_path)?;
        let found = NodeId::from_bytes(&bytes)?;
        if let Some(expected) = expected {
            if expected != found {
                return Err(ClusterError::Identity(format!(
                    "this node is configured to be {expected}, but its data directory belongs to \
                     {found} ({}). A node's identity is the pool's only name for the shards it \
                     holds, so serving another node's data under this one's name is refused. \
                     Either point this node at the data directory of {expected}, or clear \
                     {} to bring it up as a replacement that rebuilds from its peers",
                    id_path.display(),
                    data_dir.display()
                )));
            }
        }
        return Ok(NodeIdentity {
            node_id: found,
            origin: NodeIdentityOrigin::Existing,
        });
    }

    let origin = if expected.is_some() {
        NodeIdentityOrigin::Adopted
    } else {
        NodeIdentityOrigin::Generated
    };
    let id = expected.unwrap_or_else(NodeId::generate);
    // Ensure the parent directory exists.
    if let Some(parent) = id_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&id_path, id.to_bytes())?;
    if origin.is_adopted() {
        tracing::warn!(
            %id,
            path = %id_path.display(),
            "empty data directory adopted the configured node identity; this node now answers for \
             that identity's shards and must rebuild them from its peers"
        );
    } else {
        tracing::info!(%id, "generated new node identity");
    }
    Ok(NodeIdentity {
        node_id: id,
        origin,
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, SocketAddrV4};

    #[test]
    fn node_id_generate_is_unique() {
        let a = NodeId::generate();
        let b = NodeId::generate();
        assert_ne!(a, b);
    }

    #[test]
    fn node_id_bytes_round_trip() {
        let id = NodeId::generate();
        let bytes = id.to_bytes();
        let restored = NodeId::from_bytes(&bytes).unwrap();
        assert_eq!(id, restored);
    }

    #[test]
    fn node_id_from_bytes_rejects_wrong_length() {
        assert!(NodeId::from_bytes(&[0u8; 8]).is_err());
        assert!(NodeId::from_bytes(&[]).is_err());
    }

    #[test]
    fn node_id_display_is_uuid_format() {
        let id = NodeId::generate();
        let display = format!("{id}");
        // UUID v4 format: 8-4-4-4-12 hex chars
        assert_eq!(display.len(), 36);
        assert_eq!(display.chars().filter(|c| *c == '-').count(), 4);
    }

    #[test]
    fn node_id_persistence() {
        let dir = tempfile::tempdir().unwrap();

        let id1 = get_or_create_node_id(dir.path()).unwrap();
        let id2 = get_or_create_node_id(dir.path()).unwrap();

        assert_eq!(id1, id2, "must return same ID across calls");
    }

    #[test]
    fn node_id_parses_its_own_display_form() {
        let id = NodeId::generate();
        assert_eq!(id.to_string().parse::<NodeId>().unwrap(), id);
        // Surrounding whitespace comes from shells and config files, not from
        // the operator meaning a different node.
        assert_eq!(format!("  {id}\n").parse::<NodeId>().unwrap(), id);
        assert!("not-a-uuid".parse::<NodeId>().is_err());
    }

    #[test]
    fn expected_identity_matching_the_directory_is_accepted() {
        let dir = tempfile::tempdir().unwrap();
        let id = get_or_create_node_id(dir.path()).unwrap();

        assert_eq!(load_or_adopt_node_id(dir.path(), Some(id)).unwrap(), id);
    }

    #[test]
    fn expected_identity_differing_from_the_directory_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let on_disk = get_or_create_node_id(dir.path()).unwrap();
        let expected = NodeId::generate();

        let error = load_or_adopt_node_id(dir.path(), Some(expected)).unwrap_err().to_string();

        // Both identities have to be in the message: the operator is holding
        // two of them and needs to know which one this directory is.
        assert!(error.contains(&expected.to_string()), "{error}");
        assert!(error.contains(&on_disk.to_string()), "{error}");
        // The directory is left exactly as it was; a refused start writes nothing.
        assert_eq!(get_or_create_node_id(dir.path()).unwrap(), on_disk);
    }

    #[test]
    fn empty_directory_adopts_the_expected_identity() {
        let dir = tempfile::tempdir().unwrap();
        let expected = NodeId::generate();

        assert_eq!(
            load_or_adopt_node_id(dir.path(), Some(expected)).unwrap(),
            expected
        );
        // Written down, so the next start needs no environment variable.
        assert_eq!(get_or_create_node_id(dir.path()).unwrap(), expected);
    }

    #[test]
    fn no_expectation_leaves_the_generated_identity_alone() {
        let dir = tempfile::tempdir().unwrap();
        let generated = get_or_create_node_id(dir.path()).unwrap();

        assert_eq!(load_or_adopt_node_id(dir.path(), None).unwrap(), generated);
    }

    #[test]
    fn identity_origin_separates_the_three_ways_a_node_gets_its_name() {
        // A blank directory and no expectation: an identity no pool can know.
        let fresh = tempfile::tempdir().unwrap();
        let generated = load_node_identity(fresh.path(), None).unwrap();
        assert_eq!(generated.origin, NodeIdentityOrigin::Generated);

        // The same directory a second time: the identity was already here.
        let reread = load_node_identity(fresh.path(), None).unwrap();
        assert_eq!(reread.origin, NodeIdentityOrigin::Existing);
        assert_eq!(reread.node_id, generated.node_id);

        // A blank directory taking over the identity of a node that is gone.
        let replacement = tempfile::tempdir().unwrap();
        let adopted = load_node_identity(replacement.path(), Some(generated.node_id)).unwrap();
        assert_eq!(adopted.origin, NodeIdentityOrigin::Adopted);
        assert_eq!(adopted.node_id, generated.node_id);

        // Adoption happens once: the next start reads what it wrote.
        let after_adoption =
            load_node_identity(replacement.path(), Some(generated.node_id)).unwrap();
        assert_eq!(after_adoption.origin, NodeIdentityOrigin::Existing);
    }

    #[test]
    fn gossip_identity_renew_increments_incarnation() {
        let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 9200));
        let identity = GossipIdentity {
            node_id: NodeId::generate(),
            gossip_addr: addr,
            incarnation: 0,
        };

        let renewed = foca::Identity::renew(&identity).unwrap();
        assert_eq!(renewed.incarnation, 1);
        assert_eq!(renewed.node_id, identity.node_id);
        assert_eq!(renewed.gossip_addr, identity.gossip_addr);
    }

    #[test]
    fn gossip_identity_addr_returns_gossip_addr() {
        let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 9200));
        let identity = GossipIdentity {
            node_id: NodeId::generate(),
            gossip_addr: addr,
            incarnation: 0,
        };
        assert_eq!(foca::Identity::addr(&identity), addr);
    }

    #[test]
    fn gossip_identity_higher_incarnation_wins_conflict() {
        let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 9200));
        let node_id = NodeId::generate();

        let old = GossipIdentity {
            node_id,
            gossip_addr: addr,
            incarnation: 1,
        };
        let new = GossipIdentity {
            node_id,
            gossip_addr: addr,
            incarnation: 2,
        };

        assert!(foca::Identity::win_addr_conflict(&new, &old));
        assert!(!foca::Identity::win_addr_conflict(&old, &new));
    }

    #[test]
    fn node_capacity_available_bytes() {
        let cap = NodeCapacity {
            total_bytes: 1000,
            used_bytes: 300,
            volume_count: 1,
            object_count: 10,
        };
        assert_eq!(cap.available_bytes(), 700);
    }

    #[test]
    fn node_capacity_saturating_subtraction() {
        let cap = NodeCapacity {
            total_bytes: 100,
            used_bytes: 200,
            volume_count: 0,
            object_count: 0,
        };
        assert_eq!(cap.available_bytes(), 0);
    }

    #[test]
    fn pool_id_display() {
        assert_eq!(format!("{}", PoolId(1)), "pool-1");
    }

    #[test]
    fn edition_display() {
        assert_eq!(format!("{}", Edition::Community), "Community Edition");
        assert_eq!(format!("{}", Edition::Enterprise), "Enterprise Edition");
    }

    #[test]
    fn edition_equality() {
        assert_eq!(Edition::Community, Edition::Community);
        assert_eq!(Edition::Enterprise, Edition::Enterprise);
        assert_ne!(Edition::Community, Edition::Enterprise);
    }

    #[test]
    fn edition_serde_round_trip() {
        let ce = Edition::Community;
        let json = serde_json::to_string(&ce).unwrap();
        let restored: Edition = serde_json::from_str(&json).unwrap();
        assert_eq!(ce, restored);

        let ee = Edition::Enterprise;
        let json = serde_json::to_string(&ee).unwrap();
        let restored: Edition = serde_json::from_str(&json).unwrap();
        assert_eq!(ee, restored);
    }

    #[test]
    fn node_meta_default_edition_is_community() {
        // Simulate deserializing a NodeMeta from an older node that doesn't
        // send the edition field — it should default to Community.
        let json = r#"{
            "node_id": 1,
            "node_name": "old-node",
            "addr": {"grpc_addr": "127.0.0.1:9100", "http_addr": "127.0.0.1:9000", "gossip_addr": "127.0.0.1:9200"},
            "pool_id": 1,
            "topology_epoch": 1,
            "protocol_version": 1,
            "capacity": {"total_bytes": 0, "used_bytes": 0, "volume_count": 0, "object_count": 0},
            "started_at": 0,
            "s4_version": "0.0.1"
        }"#;
        let meta: NodeMeta = serde_json::from_str(json).unwrap();
        assert_eq!(meta.edition, Edition::Community);
    }
}
