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

//! Whether the nodes of a pool are configured alike for IAM (plan
//! CLUSTER-IAM-REPLICATION.md, decision 6).
//!
//! Root's password, the JWT secret and the IAM encryption key are set on
//! every node by its environment, and nothing but the operator keeps them
//! equal. When they differ, a token issued by one node is refused by
//! another, root logs in with one password here and another there, and a
//! secret sealed by one node does not open on another: answers that look
//! like wrong credentials. So every node announces in gossip a fingerprint
//! of each ([`IamConfigPrint`]) — a keyed hash that says nothing of the
//! secret — and [`IamConfigCheck`] compares them across the live nodes of
//! the pool. A node that differs from the majority is named in the log, in
//! `s4_iam_config_mismatch{kind}` and in the cluster health, which is then
//! `critical`. The comparison reads gossip only: it sends nothing.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::cluster_metrics::PoolRoster;
use crate::gossip::GossipHandle;
use crate::identity::{NodeId, NodeStatus};

/// How often the check compares the pool's announcements.
const TICK: Duration = Duration::from_secs(1);

/// What a node announces of its IAM configuration: a 16-byte fingerprint of
/// each setting, `None` where the node has none to share.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct IamConfigPrint {
    /// `HMAC-SHA256(S4_JWT_SECRET, "root:" + name + ":" + password)`; `None`
    /// without `S4_ROOT_PASSWORD`.
    pub root: Option<[u8; 16]>,
    /// `HMAC-SHA256(S4_JWT_SECRET, "jwt-secret-check")`; `None` while the
    /// node signs with a secret it generated, which no other node has.
    pub jwt_secret: Option<[u8; 16]>,
    /// The id of the key secrets are sealed with; `None` for a key the node
    /// generated for itself.
    pub encryption_key: Option<[u8; 16]>,
}

/// A setting the nodes of a pool must share.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IamConfigKind {
    /// `S4_ROOT_USERNAME` and `S4_ROOT_PASSWORD`.
    Root,
    /// `S4_JWT_SECRET`.
    JwtSecret,
    /// `S4_IAM_ENCRYPTION_KEY`, or the key derived from the JWT secret.
    EncryptionKey,
}

impl IamConfigKind {
    /// Every kind.
    pub const ALL: [Self; 3] = [Self::Root, Self::JwtSecret, Self::EncryptionKey];

    /// Its label in `s4_iam_config_mismatch{kind}`.
    pub fn label(self) -> &'static str {
        match self {
            Self::Root => "root",
            Self::JwtSecret => "jwt_secret",
            Self::EncryptionKey => "encryption_key",
        }
    }

    /// The variables it is set by.
    fn variables(self) -> &'static str {
        match self {
            Self::Root => "S4_ROOT_USERNAME and S4_ROOT_PASSWORD",
            Self::JwtSecret => "S4_JWT_SECRET",
            Self::EncryptionKey => "S4_IAM_ENCRYPTION_KEY (or S4_JWT_SECRET it is derived from)",
        }
    }

    fn of(self, print: &IamConfigPrint) -> Option<[u8; 16]> {
        match self {
            Self::Root => print.root,
            Self::JwtSecret => print.jwt_secret,
            Self::EncryptionKey => print.encryption_key,
        }
    }
}

/// Nodes configured differently from the rest of the pool.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IamConfigMismatch {
    /// The setting.
    pub kind: IamConfigKind,
    /// The nodes that differ from the majority, by name; every node that
    /// announced when no majority shares one value.
    pub nodes: Vec<String>,
    /// What to do about it.
    pub reason: String,
}

/// The settings on which `prints` — the announcement of every live node of
/// a pool, by name — disagree.
///
/// A node is named when its value is not the one most of them share; when
/// none is shared by most, every node is. A node without a value of a
/// setting differs from one with it: root that logs in through one node but
/// not another is as much a difference as two passwords.
pub fn mismatches(prints: &[(String, IamConfigPrint)]) -> Vec<IamConfigMismatch> {
    let mut found = Vec::new();
    for kind in IamConfigKind::ALL {
        let mut groups: BTreeMap<Option<[u8; 16]>, Vec<&str>> = BTreeMap::new();
        for (name, print) in prints {
            groups.entry(kind.of(print)).or_default().push(name);
        }
        if groups.len() < 2 {
            continue;
        }
        let majority = groups.iter().find(|(_, nodes)| nodes.len() * 2 > prints.len());
        let mut nodes: Vec<String> = groups
            .iter()
            .filter(|(value, _)| majority.is_none_or(|(shared, _)| *shared != **value))
            .flat_map(|(_, nodes)| nodes.iter().map(|name| name.to_string()))
            .collect();
        nodes.sort();
        let reason = match majority {
            Some(_) => format!(
                "{} on {} differs from the rest of the pool: set the same value on every node",
                kind.variables(),
                nodes.join(", ")
            ),
            None => format!(
                "{} differs between the nodes and no value is shared by most of them: set the \
                 same value on every node",
                kind.variables()
            ),
        };
        found.push(IamConfigMismatch {
            kind,
            nodes,
            reason,
        });
    }
    found
}

/// Compares the IAM configuration of the live nodes of a pool, as gossip
/// carries it.
pub struct IamConfigCheck {
    local: NodeId,
    pool: Vec<NodeId>,
    roster: PoolRoster,
    gossip: Arc<GossipHandle>,
    reported: Mutex<Vec<IamConfigMismatch>>,
}

impl IamConfigCheck {
    /// The check of node `local` of `pool`, named by `roster`.
    pub fn new(
        local: NodeId,
        pool: Vec<NodeId>,
        roster: PoolRoster,
        gossip: Arc<GossipHandle>,
    ) -> Self {
        Self {
            local,
            pool,
            roster,
            gossip,
            reported: Mutex::new(Vec::new()),
        }
    }

    /// Announces this node's configuration to the pool.
    pub fn announce(&self, print: IamConfigPrint) {
        self.gossip.announce_iam_config(print);
    }

    /// The announcement of every live node of the pool that made one, by
    /// name; this node counts whenever it announced.
    fn prints(&self) -> Vec<(String, IamConfigPrint)> {
        self.pool
            .iter()
            .filter(|node| {
                **node == self.local || self.gossip.peer_status(node) == Some(NodeStatus::Alive)
            })
            .filter_map(|node| {
                let print = self.gossip.iam_config(node)?;
                let name =
                    self.roster.name_of(*node).map_or_else(|| node.to_string(), str::to_string);
                Some((name, print))
            })
            .collect()
    }

    /// The settings the live nodes of the pool disagree on now.
    pub fn mismatches(&self) -> Vec<IamConfigMismatch> {
        mismatches(&self.prints())
    }

    /// Whether every node of the pool is alive and announces the key this
    /// node seals secrets with: only then may records be sealed again with
    /// it, or a node would be left with secrets it cannot open.
    pub fn pool_shares_encryption_key(&self) -> bool {
        let Some(ours) = self.gossip.iam_config(&self.local) else {
            return false;
        };
        self.pool.iter().all(|node| {
            (*node == self.local || self.gossip.peer_status(node) == Some(NodeStatus::Alive))
                && self
                    .gossip
                    .iam_config(node)
                    .is_some_and(|print| print.encryption_key == ours.encryption_key)
        })
    }

    /// Compares once: sets `s4_iam_config_mismatch` and logs what changed
    /// since the last comparison.
    pub fn compare(&self) {
        let found = self.mismatches();
        for kind in IamConfigKind::ALL {
            crate::cluster_metrics::set_iam_config_mismatch(
                kind.label(),
                found.iter().any(|mismatch| mismatch.kind == kind),
            );
        }
        let mut reported = self.reported.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if *reported == found {
            return;
        }
        for mismatch in &found {
            if !reported.contains(mismatch) {
                tracing::error!(
                    kind = mismatch.kind.label(),
                    nodes = %mismatch.nodes.join(", "),
                    "IAM configuration differs between the nodes of the pool: {}",
                    mismatch.reason
                );
            }
        }
        if found.is_empty() {
            tracing::info!("IAM configuration is the same on every live node of the pool");
        }
        *reported = found;
    }

    /// Compares every second until `shutdown`.
    pub async fn run(self: Arc<Self>, mut shutdown: tokio::sync::watch::Receiver<bool>) {
        let mut tick = tokio::time::interval(TICK);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = tick.tick() => self.compare(),
                _ = shutdown.changed() => return,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn print(root: u8, jwt: u8, key: u8) -> IamConfigPrint {
        IamConfigPrint {
            root: Some([root; 16]),
            jwt_secret: Some([jwt; 16]),
            encryption_key: Some([key; 16]),
        }
    }

    fn named(prints: &[IamConfigPrint]) -> Vec<(String, IamConfigPrint)> {
        prints
            .iter()
            .enumerate()
            .map(|(n, print)| (format!("node-{}", n + 1), *print))
            .collect()
    }

    #[test]
    fn a_pool_configured_alike_has_no_mismatch() {
        assert!(mismatches(&named(&[print(1, 2, 3); 3])).is_empty());
        assert!(mismatches(&[]).is_empty());
        let unset = IamConfigPrint {
            root: None,
            jwt_secret: None,
            encryption_key: None,
        };
        assert!(mismatches(&named(&[unset; 3])).is_empty());
    }

    #[test]
    fn the_node_that_differs_from_the_majority_is_named() {
        let found = mismatches(&named(&[print(1, 2, 3), print(9, 2, 3), print(1, 2, 3)]));
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].kind, IamConfigKind::Root);
        assert_eq!(found[0].nodes, vec!["node-2".to_string()]);
        assert!(found[0].reason.contains("S4_ROOT_PASSWORD"));
        assert!(found[0].reason.contains("node-2"));
    }

    #[test]
    fn a_node_without_a_setting_differs_from_those_with_it() {
        let mut without_root = print(1, 2, 3);
        without_root.root = None;
        let found = mismatches(&named(&[print(1, 2, 3), print(1, 2, 3), without_root]));
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].nodes, vec!["node-3".to_string()]);
    }

    #[test]
    fn without_a_majority_every_node_is_named() {
        let found = mismatches(&named(&[print(1, 1, 3), print(1, 2, 3)]));
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].kind, IamConfigKind::JwtSecret);
        assert_eq!(
            found[0].nodes,
            vec!["node-1".to_string(), "node-2".to_string()]
        );
        assert!(found[0].reason.contains("no value is shared"));
    }

    #[test]
    fn every_setting_is_compared_on_its_own() {
        let found = mismatches(&named(&[print(1, 2, 3), print(1, 2, 3), print(4, 5, 6)]));
        let kinds: Vec<IamConfigKind> = found.iter().map(|mismatch| mismatch.kind).collect();
        assert_eq!(kinds, IamConfigKind::ALL.to_vec());
        assert!(found.iter().all(|mismatch| mismatch.nodes == vec!["node-3".to_string()]));
    }
}
