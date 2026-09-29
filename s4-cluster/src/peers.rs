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

//! What a node remembers about the members of its pool.
//!
//! A pool is configured as a list of names and addresses, but everything
//! underneath the configuration — placement, quorum, repair, erasure-set slots
//! — is addressed by [`NodeId`]. Getting from one to the other takes two steps
//! that both need the peer to be reachable: resolving its address, and learning
//! the identity behind it. A node that is down does neither. Its hostname can
//! stop resolving (a stopped container leaves the DNS of its orchestrator), and
//! a node down long enough is dropped from gossip entirely — so a restart can
//! find itself unable to name a member it has known since the pool was built.
//!
//! Neither fact changes while the peer is away, so neither has to be learned
//! twice. This record is what this node has already learned, kept beside its
//! own identity — the same thing Cassandra keeps in `system.peers` and Ceph in
//! its monmap: a cluster's roster is durable state, not something to re-derive
//! from the network on every start.
//!
//! The record is a cache of facts, never a source of truth about liveness. It
//! says where a member lives and who it is, not whether it is up.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::error::ClusterError;
use crate::identity::NodeId;

/// File the record is kept in, beside the node's own identity.
const POOL_PEERS_FILENAME: &str = "pool_peers.json";

/// Record format this build writes and reads.
pub const POOL_PEERS_FORMAT_VERSION: u32 = 1;

/// Where a data directory keeps what this node learned about its pool.
pub fn pool_peers_path(data_dir: &Path) -> PathBuf {
    data_dir.join(POOL_PEERS_FILENAME)
}

/// One pool member, as configured and as last seen.
///
/// Every field is a string so that an operator reading the file sees the same
/// spelling they configured and the same UUID the logs print.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PoolPeer {
    /// The member's name, as `S4_POOL_NODES` spells it.
    pub name: String,
    /// The member's address exactly as configured, before any resolution.
    ///
    /// This is the key of the record, because it is the one thing that does not
    /// change while the peer is away: its hostname may stop resolving and its
    /// address may be reassigned, but what the configuration says stays put.
    pub configured_addr: String,
    /// The address that spelling last resolved to.
    pub grpc_addr: String,
    /// The identity that answered at that address, as a UUID.
    pub node_id: String,
    /// When this node last saw that answer, in seconds since the Unix epoch.
    pub last_seen_unix_secs: u64,
}

/// What this node has learned about the members of its pool.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PoolPeerRecord {
    /// Record format; always [`POOL_PEERS_FORMAT_VERSION`] today.
    pub format_version: u32,
    /// One entry per configured address this node has ever resolved.
    pub peers: Vec<PoolPeer>,
}

// A narrow filesystem boundary lets the crash-safety ordering and every I/O
// failure be checked without relying on a particular disk's cache behaviour.
trait PeerRecordIo {
    fn write(&mut self, path: &Path, bytes: &[u8]) -> std::io::Result<()>;
    fn sync_file(&mut self, path: &Path) -> std::io::Result<()>;
    fn rename(&mut self, from: &Path, to: &Path) -> std::io::Result<()>;
    fn sync_directory(&mut self, path: &Path) -> std::io::Result<()>;
}

struct FilePeerRecordIo;

impl PeerRecordIo for FilePeerRecordIo {
    fn write(&mut self, path: &Path, bytes: &[u8]) -> std::io::Result<()> {
        std::fs::write(path, bytes)
    }

    fn sync_file(&mut self, path: &Path) -> std::io::Result<()> {
        std::fs::OpenOptions::new().write(true).open(path)?.sync_all()
    }

    fn rename(&mut self, from: &Path, to: &Path) -> std::io::Result<()> {
        std::fs::rename(from, to)
    }

    fn sync_directory(&mut self, path: &Path) -> std::io::Result<()> {
        std::fs::File::open(path)?.sync_all()
    }
}

impl Default for PoolPeerRecord {
    fn default() -> Self {
        Self {
            format_version: POOL_PEERS_FORMAT_VERSION,
            peers: Vec::new(),
        }
    }
}

impl PoolPeerRecord {
    /// Read the record a previous start left behind.
    ///
    /// Never fails a start. A missing, unreadable or unparsable record means
    /// this node has learned nothing yet, which is exactly the state of a node
    /// that has never run — and the caller already handles that state. Losing
    /// the cache costs discovery, not correctness, so refusing to start over it
    /// would trade a cheap problem for an expensive one.
    pub fn load(data_dir: &Path) -> Self {
        let path = pool_peers_path(data_dir);
        let raw = match std::fs::read(&path) {
            Ok(raw) => raw,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Self::default();
            }
            Err(error) => {
                tracing::warn!(
                    path = %path.display(),
                    %error,
                    "could not read what this node knew about its pool; starting without it"
                );
                return Self::default();
            }
        };

        match serde_json::from_slice::<Self>(&raw) {
            Ok(record) if record.format_version == POOL_PEERS_FORMAT_VERSION => record,
            Ok(record) => {
                tracing::warn!(
                    path = %path.display(),
                    found = record.format_version,
                    expected = POOL_PEERS_FORMAT_VERSION,
                    "pool peer record is in a format this build does not read; starting without it"
                );
                Self::default()
            }
            Err(error) => {
                tracing::warn!(
                    path = %path.display(),
                    %error,
                    "pool peer record could not be parsed; starting without it"
                );
                Self::default()
            }
        }
    }

    /// The address each configured spelling last resolved to.
    ///
    /// Answers the question a start asks before anything else: "the DNS name in
    /// my configuration does not resolve — where did this member live?"
    pub fn last_known_addrs(&self) -> HashMap<String, SocketAddr> {
        let mut addrs = HashMap::with_capacity(self.peers.len());
        for peer in &self.peers {
            let Ok(addr) = SocketAddr::from_str(&peer.grpc_addr) else {
                tracing::warn!(
                    configured_addr = %peer.configured_addr,
                    grpc_addr = %peer.grpc_addr,
                    "skipping a pool peer address this build cannot read"
                );
                continue;
            };
            addrs.insert(peer.configured_addr.clone(), addr);
        }
        addrs
    }

    /// The identity recorded for each configured spelling.
    ///
    /// Keyed by the configured address and never by the resolved one, because
    /// an orchestrator reassigns addresses: the address a member had last week
    /// can belong to a different member today, and naming a node by a stale
    /// address would give one machine another's identity. The configuration
    /// entry is the only stable handle on a member that is away.
    ///
    /// An entry whose identity does not parse is skipped rather than failing
    /// the lot: one bad line must not cost the other five.
    pub fn recalled_identities(&self) -> HashMap<String, NodeId> {
        let mut identities = HashMap::with_capacity(self.peers.len());
        for peer in &self.peers {
            let Ok(node_id) = NodeId::from_str(&peer.node_id) else {
                tracing::warn!(
                    configured_addr = %peer.configured_addr,
                    node_id = %peer.node_id,
                    "skipping a pool peer identity this build cannot read"
                );
                continue;
            };
            identities.insert(peer.configured_addr.clone(), node_id);
        }
        identities
    }

    /// Record what this start learned about one member, replacing what was
    /// there for the same configured address.
    ///
    /// The newest answer wins: an address whose machine was replaced carries a
    /// new identity, and the pool addresses the new one from then on.
    pub fn remember(
        &mut self,
        name: &str,
        configured_addr: &str,
        grpc_addr: SocketAddr,
        node_id: NodeId,
        now_unix_secs: u64,
    ) {
        let grpc_addr = grpc_addr.to_string();
        let node_id = node_id.to_string();
        if let Some(existing) =
            self.peers.iter_mut().find(|peer| peer.configured_addr == configured_addr)
        {
            existing.name = name.to_string();
            existing.grpc_addr = grpc_addr;
            existing.node_id = node_id;
            existing.last_seen_unix_secs = now_unix_secs;
            return;
        }
        self.peers.push(PoolPeer {
            name: name.to_string(),
            configured_addr: configured_addr.to_string(),
            grpc_addr,
            node_id,
            last_seen_unix_secs: now_unix_secs,
        });
    }

    /// Forget every member whose configured address is not in `keep`.
    ///
    /// Called with the pool as it is configured now, so that a node removed
    /// from `S4_POOL_NODES` stops being named by a node that once knew it.
    pub fn retain_configured(&mut self, keep: &[String]) {
        self.peers.retain(|peer| keep.contains(&peer.configured_addr));
    }

    /// Write the record, replacing the previous one atomically.
    ///
    /// Written through a temporary file and a rename so that a crash during the
    /// write leaves the previous record rather than half of this one. The file
    /// is synced before rename and its directory afterwards; success means the
    /// replacement has crossed both persistence barriers.
    ///
    /// # Errors
    ///
    /// Returns [`ClusterError::Identity`] when the record cannot be encoded or
    /// written. Callers treat that as a warning: a node that could not save
    /// what it learned still knows it for this run.
    pub fn save(&self, data_dir: &Path) -> Result<(), ClusterError> {
        self.save_with(data_dir, &mut FilePeerRecordIo)
    }

    fn save_with(&self, data_dir: &Path, io: &mut impl PeerRecordIo) -> Result<(), ClusterError> {
        let path = pool_peers_path(data_dir);
        let encoded = serde_json::to_vec_pretty(self).map_err(|error| {
            ClusterError::Identity(format!("could not encode the pool peer record: {error}"))
        })?;

        let tmp_path = path.with_extension("json.tmp");
        io.write(&tmp_path, &encoded).map_err(|error| {
            ClusterError::Identity(format!("could not write {}: {error}", tmp_path.display()))
        })?;
        io.sync_file(&tmp_path).map_err(|error| {
            ClusterError::Identity(format!("could not sync {}: {error}", tmp_path.display()))
        })?;
        io.rename(&tmp_path, &path).map_err(|error| {
            ClusterError::Identity(format!("could not replace {}: {error}", path.display()))
        })?;
        io.sync_directory(data_dir).map_err(|error| {
            ClusterError::Identity(format!("could not sync {}: {error}", data_dir.display()))
        })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct RecordingIo {
        steps: Vec<&'static str>,
        fail_at: Option<&'static str>,
    }

    impl RecordingIo {
        fn step(&mut self, name: &'static str) -> std::io::Result<()> {
            self.steps.push(name);
            if self.fail_at == Some(name) {
                Err(std::io::Error::other(name))
            } else {
                Ok(())
            }
        }
    }

    impl PeerRecordIo for RecordingIo {
        fn write(&mut self, _: &Path, bytes: &[u8]) -> std::io::Result<()> {
            let record: PoolPeerRecord = serde_json::from_slice(bytes).unwrap();
            assert_eq!(record.format_version, POOL_PEERS_FORMAT_VERSION);
            self.step("write")
        }
        fn sync_file(&mut self, _: &Path) -> std::io::Result<()> {
            self.step("sync_file")
        }
        fn rename(&mut self, _: &Path, _: &Path) -> std::io::Result<()> {
            self.step("rename")
        }
        fn sync_directory(&mut self, _: &Path) -> std::io::Result<()> {
            self.step("sync_directory")
        }
    }

    #[test]
    fn saving_syncs_contents_before_rename_and_directory_afterwards() {
        let mut io = RecordingIo::default();
        PoolPeerRecord::default().save_with(Path::new("pool"), &mut io).unwrap();
        assert_eq!(io.steps, ["write", "sync_file", "rename", "sync_directory"]);
    }

    #[test]
    fn saving_propagates_each_io_failure_and_stops_at_that_step() {
        let steps = ["write", "sync_file", "rename", "sync_directory"];
        for (index, step) in steps.iter().enumerate() {
            let mut io = RecordingIo {
                fail_at: Some(step),
                ..Default::default()
            };
            assert!(
                PoolPeerRecord::default().save_with(Path::new("pool"), &mut io).is_err(),
                "failure in {step} must reach the caller"
            );
            assert_eq!(io.steps, steps[..=index]);
        }
    }

    fn addr(port: u16) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], port))
    }

    fn node(seed: u128) -> NodeId {
        NodeId(seed)
    }

    #[test]
    fn a_node_that_has_learned_nothing_reads_an_empty_record() {
        let dir = tempfile::tempdir().unwrap();
        let record = PoolPeerRecord::load(dir.path());
        assert_eq!(record.format_version, POOL_PEERS_FORMAT_VERSION);
        assert!(record.recalled_identities().is_empty());
        assert!(record.last_known_addrs().is_empty());
    }

    #[test]
    fn what_a_node_learned_survives_the_start_that_learned_it() {
        let dir = tempfile::tempdir().unwrap();
        let mut record = PoolPeerRecord::default();
        record.remember("node-2", "s4-node2:9100", addr(20702), node(2), 1_000);
        record.remember("node-3", "s4-node3:9100", addr(20703), node(3), 1_000);
        record.save(dir.path()).unwrap();

        let reloaded = PoolPeerRecord::load(dir.path());

        // Both halves of the answer survive: where the member lives, and who it
        // is. A restart needs the first before it can use the second.
        let addrs = reloaded.last_known_addrs();
        assert_eq!(addrs.get("s4-node2:9100"), Some(&addr(20702)));
        assert_eq!(addrs.get("s4-node3:9100"), Some(&addr(20703)));

        let ids = reloaded.recalled_identities();
        assert_eq!(ids.get("s4-node2:9100"), Some(&node(2)));
        assert_eq!(ids.get("s4-node3:9100"), Some(&node(3)));
    }

    #[test]
    fn the_newest_answer_for_a_configured_address_replaces_the_old_one() {
        let mut record = PoolPeerRecord::default();
        record.remember("node-3", "s4-node3:9100", addr(20703), node(3), 1_000);
        // Same entry, new container: another IP and another identity.
        record.remember("node-3", "s4-node3:9100", addr(30703), node(30), 2_000);

        assert_eq!(record.peers.len(), 1, "an entry is named once, not twice");
        assert_eq!(
            record.last_known_addrs().get("s4-node3:9100"),
            Some(&addr(30703))
        );
        assert_eq!(
            record.recalled_identities().get("s4-node3:9100"),
            Some(&node(30))
        );
        assert_eq!(record.peers[0].last_seen_unix_secs, 2_000);
    }

    #[test]
    fn a_node_taken_out_of_the_pool_stops_being_named() {
        let mut record = PoolPeerRecord::default();
        record.remember("node-2", "s4-node2:9100", addr(20702), node(2), 1_000);
        record.remember("node-3", "s4-node3:9100", addr(20703), node(3), 1_000);

        record.retain_configured(&["s4-node2:9100".to_string()]);

        assert_eq!(record.peers.len(), 1);
        assert!(record.last_known_addrs().contains_key("s4-node2:9100"));
        assert!(!record.last_known_addrs().contains_key("s4-node3:9100"));
    }

    #[test]
    fn a_record_this_build_cannot_read_costs_discovery_and_not_the_start() {
        let dir = tempfile::tempdir().unwrap();

        std::fs::write(pool_peers_path(dir.path()), b"{ this is not json").unwrap();
        assert!(PoolPeerRecord::load(dir.path()).recalled_identities().is_empty());

        std::fs::write(
            pool_peers_path(dir.path()),
            br#"{"format_version":99,"peers":[]}"#,
        )
        .unwrap();
        assert!(PoolPeerRecord::load(dir.path()).recalled_identities().is_empty());
    }

    #[test]
    fn one_unreadable_entry_does_not_cost_the_others() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            pool_peers_path(dir.path()),
            format!(
                r#"{{"format_version":{POOL_PEERS_FORMAT_VERSION},"peers":[
                    {{"name":"node-1","configured_addr":"a:1","grpc_addr":"not-an-address","node_id":"{}","last_seen_unix_secs":1}},
                    {{"name":"node-2","configured_addr":"b:2","grpc_addr":"127.0.0.1:20702","node_id":"not-a-uuid","last_seen_unix_secs":1}},
                    {{"name":"node-3","configured_addr":"c:3","grpc_addr":"127.0.0.1:20703","node_id":"{}","last_seen_unix_secs":1}}
                ]}}"#,
                node(2),
                node(3)
            ),
        )
        .unwrap();

        let record = PoolPeerRecord::load(dir.path());

        // The entry with a broken identity still names an address: the two
        // questions are answered separately and fail separately.
        let addrs = record.last_known_addrs();
        assert_eq!(addrs.len(), 2);
        assert_eq!(addrs.get("b:2"), Some(&addr(20702)));
        assert_eq!(addrs.get("c:3"), Some(&addr(20703)));

        let ids = record.recalled_identities();
        assert_eq!(
            ids.len(),
            2,
            "a broken address does not hide a good identity"
        );
        assert_eq!(ids.get("c:3"), Some(&node(3)));
        assert!(
            !ids.contains_key("b:2"),
            "an unreadable identity is skipped"
        );
    }
}
