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

//! The pool taking over the IAM its nodes kept of their own (plan
//! CLUSTER-IAM-REPLICATION.md, decision 8).
//!
//! A node of an earlier release kept the users created through it on it
//! alone, and so does a single node that becomes a cluster node. Nothing in
//! that data tells "created only on node-2" from "deleted through node-1 and
//! left on node-2", so merging it by itself would bring deleted users and
//! their keys back on every node. The operator decides instead:
//!
//! - every node answers what it holds of its own ([`OwnUsers`], with a
//!   fingerprint of exactly those records), and the answers make one
//!   [`AdoptionReport`]: every user name with the nodes that hold it, its
//!   records, their roles, flags and keys, and the conflicts between them;
//! - the operator answers with an [`AdoptionRequest`] — every user, those on
//!   a majority of the pool, or an explicit list — naming the report it saw;
//! - [`plan`] turns the decision into records of version 2: one name, one
//!   user, the earliest created keeping it and the others renamed and
//!   disabled; and the pool keeps the decision as an [`AdoptionRecord`],
//!   which tells each node, by the fingerprint of what it answered, that it
//!   may drop the records it held of its own.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::error::IamError;
use super::models::{Role, User};

/// The key of the pool's latest decision on the IAM its nodes kept of their
/// own, in `__system__`: a record of the cluster's control state, held by
/// every node of the pool.
pub const ADOPTION_KEY: &str = "__s4_iam_v2_adoption";

/// How many characters of a user's id a renamed user's name carries.
const RENAME_ID_CHARS: usize = 8;

/// The copies of the records of one user name, by id, each with the position
/// of the node holding it.
type CopiesById<'a> = BTreeMap<&'a str, Vec<(usize, &'a User)>>;

/// The first 16 bytes of `digest`, in lowercase hex.
fn hex16(digest: &[u8]) -> String {
    digest.iter().take(16).map(|byte| format!("{byte:02x}")).collect()
}

/// Whether `user`, kept by a node of its own, still means something: every
/// user does but a stored root without S3 credentials, which can neither log
/// in (root is configured now) nor sign a request.
pub fn own_user_matters(user: &User, root_name: Option<&str>) -> bool {
    root_name != Some(user.username.as_str()) || user.access_key.is_some()
}

/// The IAM one node keeps of its own: the users an earlier release, or the
/// node as a single node, stored on it, which its pool has not taken over.
#[derive(Debug, Clone)]
pub struct OwnUsers {
    users: Vec<User>,
    digest: String,
}

impl OwnUsers {
    /// The users of `records` — keys and bodies as the node stores them — but
    /// a stored root without S3 credentials (see [`own_user_matters`]), with
    /// the fingerprint of the records kept. Two answers of a node have the
    /// same fingerprint exactly when they hold the same records, whether the
    /// secrets in them are in clear text or sealed.
    ///
    /// # Errors
    ///
    /// `Json` when a record is no user.
    pub fn from_records(
        records: Vec<(String, Vec<u8>)>,
        root_name: Option<&str>,
    ) -> Result<Self, IamError> {
        let mut kept = Vec::new();
        for (key, data) in records {
            let user: User = serde_json::from_slice(&data)?;
            if own_user_matters(&user, root_name) {
                kept.push((key, data, user));
            }
        }
        kept.sort_by(|a, b| a.0.cmp(&b.0));
        let mut hasher = Sha256::new();
        for (key, _, user) in &kept {
            // The secret is left out: a node sends it sealed, with a nonce of
            // its own every time, and the hash of the secret names it anyway.
            let mut fingerprinted = user.clone();
            fingerprinted.secret_key = None;
            fingerprinted.secret_key_sealed = None;
            let data = serde_json::to_vec(&fingerprinted)?;
            hasher.update((key.len() as u64).to_be_bytes());
            hasher.update(key.as_bytes());
            hasher.update((data.len() as u64).to_be_bytes());
            hasher.update(&data);
        }
        Ok(Self {
            digest: hex16(&hasher.finalize()),
            users: kept.into_iter().map(|(_, _, user)| user).collect(),
        })
    }

    /// The users, in the order of their keys.
    pub fn users(&self) -> &[User] {
        &self.users
    }

    /// The fingerprint of the records the users were read from.
    pub fn digest(&self) -> &str {
        &self.digest
    }

    /// Whether the node keeps no user of its own.
    pub fn is_empty(&self) -> bool {
        self.users.is_empty()
    }
}

/// What one node of the pool answered when asked for the IAM it keeps of its
/// own.
#[derive(Debug, Clone)]
pub struct NodeHoldings {
    /// The node's identity.
    pub node_id: String,
    /// Its name in `S4_POOL_NODES`, or its identity when it has none.
    pub node_name: String,
    /// Its users, or why they could not be read.
    pub own: Result<OwnUsers, String>,
}

/// One node in the report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReportedNode {
    /// The node's identity.
    pub node_id: String,
    /// Its name in `S4_POOL_NODES`.
    pub node_name: String,
    /// Whether it answered.
    pub answered: bool,
    /// Why it did not.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// The users it keeps of its own.
    pub own_users: usize,
    /// The fingerprint of its answer.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub digest: Option<String>,
}

/// One copy of a user's record, as one node holds it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReportedCopy {
    /// The node holding it.
    pub node: String,
    /// The role it gives the user.
    pub role: Role,
    /// Whether the user may log in and sign requests.
    pub is_active: bool,
    /// When the user was created.
    pub created_at: DateTime<Utc>,
    /// When the record last changed.
    pub updated_at: DateTime<Utc>,
    /// The S3 access key it holds; the secret is never reported.
    pub access_key: Option<String>,
}

/// One user, by id, with every copy of its record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReportedRecord {
    /// The user's id.
    pub id: String,
    /// Its copies, in the order of the nodes.
    pub copies: Vec<ReportedCopy>,
}

/// How the records of one user name disagree.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NameConflict {
    /// The name belongs to users of different ids: each node created its
    /// own.
    Ids,
    /// The records give the name different roles.
    Roles,
    /// Some records are active and some are not.
    Active,
    /// Copies of one user's record hold different passwords. Records of
    /// different ids are not compared: each node hashed its password with a
    /// salt of its own, so equal passwords look different anyway.
    Passwords,
}

/// One user name in the report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReportedName {
    /// The name.
    pub username: String,
    /// The nodes holding a record of it, in the order of the nodes.
    pub nodes: Vec<String>,
    /// Whether a majority of the pool holds a record of it.
    pub on_majority: bool,
    /// How its records disagree; empty when they do not.
    pub conflicts: Vec<NameConflict>,
    /// Its records, by id.
    pub records: Vec<ReportedRecord>,
}

/// What the nodes of a pool keep of their own, for the operator to decide on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdoptionReport {
    /// Names exactly these answers: a decision on another report is
    /// refused.
    pub report_id: String,
    /// Whether every node of the pool answered.
    pub complete: bool,
    /// Nodes of the pool.
    pub pool_size: usize,
    /// How many of them are a majority.
    pub majority: usize,
    /// Every node of the pool and its answer.
    pub nodes: Vec<ReportedNode>,
    /// Every user name some node keeps of its own, in name order.
    pub users: Vec<ReportedName>,
    /// Names whose records disagree.
    pub conflicts: usize,
    /// Names no majority of the pool holds.
    pub not_on_majority: usize,
}

impl AdoptionReport {
    /// The report of the answers `holdings`, one per node of the pool.
    pub fn build(holdings: &[NodeHoldings]) -> Self {
        let pool_size = holdings.len();
        let majority = pool_size / 2 + 1;
        let nodes: Vec<ReportedNode> = holdings
            .iter()
            .map(|node| match &node.own {
                Ok(own) => ReportedNode {
                    node_id: node.node_id.clone(),
                    node_name: node.node_name.clone(),
                    answered: true,
                    error: None,
                    own_users: own.users().len(),
                    digest: Some(own.digest().to_string()),
                },
                Err(error) => ReportedNode {
                    node_id: node.node_id.clone(),
                    node_name: node.node_name.clone(),
                    answered: false,
                    error: Some(error.clone()),
                    own_users: 0,
                    digest: None,
                },
            })
            .collect();
        let report_id = report_id(&nodes);

        // name → id → copies, each copy with the node's position, so that
        // copies and nodes keep the pool's order.
        let mut names: BTreeMap<&str, CopiesById<'_>> = BTreeMap::new();
        for (position, node) in holdings.iter().enumerate() {
            let Ok(own) = &node.own else { continue };
            for user in own.users() {
                names
                    .entry(user.username.as_str())
                    .or_default()
                    .entry(user.id.as_str())
                    .or_default()
                    .push((position, user));
            }
        }
        let users: Vec<ReportedName> = names
            .into_iter()
            .map(|(username, ids)| {
                let holders: BTreeSet<usize> = ids
                    .values()
                    .flat_map(|copies| copies.iter().map(|(position, _)| *position))
                    .collect();
                let mut conflicts = BTreeSet::new();
                if ids.len() > 1 {
                    conflicts.insert(NameConflict::Ids);
                }
                let every = || ids.values().flatten().map(|(_, user)| *user);
                if every().map(|user| user.role as u8).collect::<BTreeSet<_>>().len() > 1 {
                    conflicts.insert(NameConflict::Roles);
                }
                if every().map(|user| user.is_active).collect::<BTreeSet<_>>().len() > 1 {
                    conflicts.insert(NameConflict::Active);
                }
                if ids.values().any(|copies| {
                    copies
                        .iter()
                        .map(|(_, user)| user.password_hash.as_str())
                        .collect::<BTreeSet<_>>()
                        .len()
                        > 1
                }) {
                    conflicts.insert(NameConflict::Passwords);
                }
                ReportedName {
                    username: username.to_string(),
                    nodes: holders
                        .iter()
                        .map(|position| nodes[*position].node_name.clone())
                        .collect(),
                    on_majority: holders.len() >= majority,
                    conflicts: conflicts.into_iter().collect(),
                    records: ids
                        .into_iter()
                        .map(|(id, copies)| ReportedRecord {
                            id: id.to_string(),
                            copies: copies
                                .into_iter()
                                .map(|(position, user)| ReportedCopy {
                                    node: nodes[position].node_name.clone(),
                                    role: user.role,
                                    is_active: user.is_active,
                                    created_at: user.created_at,
                                    updated_at: user.updated_at,
                                    access_key: user.access_key.clone(),
                                })
                                .collect(),
                        })
                        .collect(),
                }
            })
            .collect();
        Self {
            report_id,
            complete: nodes.iter().all(|node| node.answered),
            pool_size,
            majority,
            conflicts: users.iter().filter(|name| !name.conflicts.is_empty()).count(),
            not_on_majority: users.iter().filter(|name| !name.on_majority).count(),
            nodes,
            users,
        }
    }

    /// Whether some node keeps users of its own: there is something to
    /// decide on.
    pub fn has_own_users(&self) -> bool {
        self.nodes.iter().any(|node| node.own_users > 0)
    }

    /// The nodes that did not answer, by name.
    pub fn unanswered(&self) -> Vec<String> {
        self.nodes
            .iter()
            .filter(|node| !node.answered)
            .map(|node| node.node_name.clone())
            .collect()
    }
}

/// The id of a report of `nodes`: the fingerprint of every node's answer, in
/// the order of the node identities.
fn report_id(nodes: &[ReportedNode]) -> String {
    let mut ordered: Vec<&ReportedNode> = nodes.iter().collect();
    ordered.sort_by(|a, b| a.node_id.cmp(&b.node_id));
    let mut hasher = Sha256::new();
    for node in ordered {
        hasher.update((node.node_id.len() as u64).to_be_bytes());
        hasher.update(node.node_id.as_bytes());
        match &node.digest {
            Some(digest) => {
                hasher.update([1]);
                hasher.update(digest.as_bytes());
            }
            None => hasher.update([0]),
        }
    }
    hex16(&hasher.finalize())
}

/// Which users the pool takes over.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdoptionMode {
    /// Every user some node keeps.
    Union,
    /// The users whose name a majority of the pool holds.
    Majority,
    /// The users the request lists by id.
    Users,
}

/// The operator's decision on a report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdoptionRequest {
    /// The report the decision was taken on.
    pub report_id: String,
    /// Which users are taken over.
    pub mode: AdoptionMode,
    /// With `mode = users`, the ids of the users taken over.
    #[serde(default)]
    pub users: Vec<String>,
}

/// One user the decision took over.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdoptedUser {
    /// Its id.
    pub id: String,
    /// The name it has now.
    pub username: String,
    /// The name it had, when another user kept that one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub renamed_from: Option<String>,
    /// Whether it is active: a renamed user is not until the operator says
    /// so.
    pub is_active: bool,
    /// The nodes that held it.
    pub nodes: Vec<String>,
}

/// One user the decision left out: its records are dropped with the rest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeftOutUser {
    /// Its id.
    pub id: String,
    /// Its name.
    pub username: String,
    /// The nodes that held it.
    pub nodes: Vec<String>,
}

/// The records a decision writes and what it did with every user.
#[derive(Debug, Clone)]
pub struct AdoptionPlan {
    /// The records of version 2 to write.
    pub records: Vec<User>,
    /// The users taken over.
    pub adopted: Vec<AdoptedUser>,
    /// Users chosen that the pool already holds a record of version 2 of: a
    /// decision taken again after one stopped half way. Their records stay.
    pub already_adopted: Vec<String>,
    /// The users left out.
    pub left_out: Vec<LeftOutUser>,
}

/// The pool's decision on the IAM its nodes kept of their own, kept as
/// [`ADOPTION_KEY`]. It is written after the users it takes over, so a node
/// that finds it may drop what it kept: the users the decision chose are in
/// the pool already.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdoptionRecord {
    /// The report the decision was taken on.
    pub report_id: String,
    /// Which users were taken over.
    pub mode: AdoptionMode,
    /// When.
    pub decided_at: DateTime<Utc>,
    /// The account that decided.
    pub decided_by: String,
    /// The node the decision came through.
    pub through: String,
    /// Every node the report covered, by identity, with the fingerprint of
    /// what it kept of its own: a node drops its records only while they are
    /// still the ones the operator saw.
    pub nodes: BTreeMap<String, String>,
    /// The users taken over.
    pub adopted: Vec<AdoptedUser>,
    /// Users chosen that the pool held already.
    pub already_adopted: Vec<String>,
    /// The users left out.
    pub left_out: Vec<LeftOutUser>,
    /// The report the decision was taken on, kept as a record of what the
    /// nodes held before they dropped it.
    pub report: AdoptionReport,
}

impl AdoptionRecord {
    /// Whether the decision covers node `node_id` keeping the records of
    /// fingerprint `digest`: the node may drop them.
    pub fn covers(&self, node_id: &str, digest: &str) -> bool {
        self.nodes.get(node_id).is_some_and(|covered| covered == digest)
    }
}

/// The name a user gets when another user keeps its name.
pub fn renamed(username: &str, id: &str) -> String {
    let short: String = id.chars().take(RENAME_ID_CHARS).collect();
    format!("{username}~{short}")
}

/// The records version 2 a decision `request` on the answers `holdings`
/// writes, next to the users the pool holds already, `existing`, under the
/// lease of epoch `epoch` at `now`.
///
/// Of the copies of one user on several nodes, the one changed last is
/// taken. Of the users of one name, the pool's own keeps it, then the one
/// created first (then the lower id); every other is renamed
/// `<name>~<first 8 characters of its id>` and disabled, its S3 credentials
/// with it — the operator enables it.
///
/// # Errors
///
/// `AdoptionInvalid` for an id the report does not name, or ids with
/// another mode than `users`.
pub fn plan(
    holdings: &[NodeHoldings],
    request: &AdoptionRequest,
    existing: &[User],
    epoch: u64,
    now: DateTime<Utc>,
) -> Result<AdoptionPlan, IamError> {
    let majority = holdings.len() / 2 + 1;
    // id → its copies with the nodes holding them; name → nodes holding it.
    let mut copies: BTreeMap<&str, Vec<(&str, &User)>> = BTreeMap::new();
    let mut name_holders: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
    for node in holdings {
        let Ok(own) = &node.own else { continue };
        for user in own.users() {
            copies
                .entry(user.id.as_str())
                .or_default()
                .push((node.node_name.as_str(), user));
            name_holders
                .entry(user.username.as_str())
                .or_default()
                .insert(node.node_id.as_str());
        }
    }

    let chosen: BTreeSet<&str> = match request.mode {
        AdoptionMode::Union | AdoptionMode::Majority if !request.users.is_empty() => {
            return Err(IamError::AdoptionInvalid(
                "users are listed only with mode=users".to_string(),
            ));
        }
        AdoptionMode::Union => copies.keys().copied().collect(),
        AdoptionMode::Majority => copies
            .iter()
            .filter(|(_, held)| {
                held.first().is_some_and(|(_, user)| {
                    name_holders.get(user.username.as_str()).map_or(0, BTreeSet::len) >= majority
                })
            })
            .map(|(id, _)| *id)
            .collect(),
        AdoptionMode::Users => {
            let mut listed = BTreeSet::new();
            for id in &request.users {
                let Some((id, _)) = copies.get_key_value(id.as_str()) else {
                    return Err(IamError::AdoptionInvalid(format!(
                        "no node keeps a user of id '{id}'"
                    )));
                };
                listed.insert(*id);
            }
            listed
        }
    };

    let existing_ids: BTreeSet<&str> = existing.iter().map(|user| user.id.as_str()).collect();
    let existing_names: BTreeSet<&str> =
        existing.iter().map(|user| user.username.as_str()).collect();
    let nodes_of = |held: &[(&str, &User)]| -> Vec<String> {
        held.iter().map(|(node, _)| node.to_string()).collect()
    };

    let mut already_adopted = Vec::new();
    let mut left_out = Vec::new();
    let mut by_name: BTreeMap<String, Vec<(User, Vec<String>)>> = BTreeMap::new();
    for (id, held) in &copies {
        let Some((_, newest)) = held.iter().max_by(|(node_a, a), (node_b, b)| {
            (a.updated_at, a.credential_version)
                .cmp(&(b.updated_at, b.credential_version))
                .then_with(|| node_b.cmp(node_a))
        }) else {
            continue;
        };
        if !chosen.contains(id) {
            left_out.push(LeftOutUser {
                id: id.to_string(),
                username: newest.username.clone(),
                nodes: nodes_of(held),
            });
        } else if existing_ids.contains(id) {
            already_adopted.push(id.to_string());
        } else {
            by_name
                .entry(newest.username.clone())
                .or_default()
                .push(((*newest).clone(), nodes_of(held)));
        }
    }

    let mut records = Vec::new();
    let mut adopted = Vec::new();
    for (username, mut group) in by_name {
        group.sort_by(|(a, _), (b, _)| (a.created_at, &a.id).cmp(&(b.created_at, &b.id)));
        let name_taken = existing_names.contains(username.as_str());
        for (position, (mut user, nodes)) in group.into_iter().enumerate() {
            let renamed_from = if position == 0 && !name_taken {
                None
            } else {
                user.username = renamed(&username, &user.id);
                user.is_active = false;
                user.credential_version += 1;
                user.updated_at = now;
                Some(username.clone())
            };
            user.created_epoch = epoch;
            adopted.push(AdoptedUser {
                id: user.id.clone(),
                username: user.username.clone(),
                renamed_from,
                is_active: user.is_active,
                nodes,
            });
            records.push(user);
        }
    }
    Ok(AdoptionPlan {
        records,
        adopted,
        already_adopted,
        left_out,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    fn user(id: &str, username: &str, role: Role, minutes_ago: i64) -> User {
        let at = Utc::now() - Duration::minutes(minutes_ago);
        User {
            id: id.to_string(),
            username: username.to_string(),
            password_hash: format!("hash-of-{id}"),
            role,
            access_key: Some(format!("S4AK{id}")),
            secret_key: Some(format!("secret-{id}")),
            secret_key_sealed: None,
            secret_key_hash: Some(format!("secret-hash-{id}")),
            created_at: at,
            updated_at: at,
            is_active: true,
            credential_version: 0,
            created_epoch: 0,
        }
    }

    fn records(users: &[&User]) -> Vec<(String, Vec<u8>)> {
        users
            .iter()
            .map(|user| {
                (
                    format!("__s4_iam_user_{}", user.id),
                    serde_json::to_vec(user).unwrap(),
                )
            })
            .collect()
    }

    fn node(n: usize, users: &[&User]) -> NodeHoldings {
        NodeHoldings {
            node_id: format!("id-{n}"),
            node_name: format!("node-{n}"),
            own: Ok(OwnUsers::from_records(records(users), Some("root")).unwrap()),
        }
    }

    fn request(mode: AdoptionMode, users: &[&str]) -> AdoptionRequest {
        AdoptionRequest {
            report_id: String::new(),
            mode,
            users: users.iter().map(|id| id.to_string()).collect(),
        }
    }

    /// Three nodes of an earlier release: alice on node-1 only; bob
    /// created on every node, a Writer on two and a Reader on the third;
    /// carol created on node-1 and node-2 (on the third she was deleted).
    fn pool() -> (Vec<NodeHoldings>, [User; 6]) {
        let alice = user("a1", "alice", Role::Writer, 50);
        let bob1 = user("b1", "bob", Role::Writer, 40);
        let bob2 = user("b2", "bob", Role::Writer, 30);
        let bob3 = user("b3", "bob", Role::Reader, 20);
        let carol1 = user("c1", "carol", Role::Reader, 10);
        let carol2 = user("c2", "carol", Role::Reader, 5);
        let holdings = vec![
            node(1, &[&alice, &bob1, &carol1]),
            node(2, &[&bob2, &carol2]),
            node(3, &[&bob3]),
        ];
        (holdings, [alice, bob1, bob2, bob3, carol1, carol2])
    }

    #[test]
    fn the_fingerprint_names_exactly_the_records() {
        let alice = user("a1", "alice", Role::Writer, 5);
        let mut changed = alice.clone();
        changed.role = Role::Reader;
        let one = OwnUsers::from_records(records(&[&alice]), None).unwrap();
        let again = OwnUsers::from_records(records(&[&alice]), None).unwrap();
        let other = OwnUsers::from_records(records(&[&changed]), None).unwrap();
        let none = OwnUsers::from_records(Vec::new(), None).unwrap();
        assert_eq!(one.digest(), again.digest());
        assert_ne!(one.digest(), other.digest());
        assert_ne!(one.digest(), none.digest());
        assert_eq!(one.digest().len(), 32);
        assert!(none.is_empty());
    }

    #[test]
    fn a_stored_root_without_keys_is_no_user_of_its_own() {
        let mut root = user("r1", "root", Role::SuperUser, 5);
        root.access_key = None;
        let keyed_root = user("r2", "root", Role::SuperUser, 5);
        let own = OwnUsers::from_records(records(&[&root]), Some("root")).unwrap();
        assert!(own.is_empty());
        assert_eq!(
            own.digest(),
            OwnUsers::from_records(Vec::new(), None).unwrap().digest()
        );
        assert_eq!(
            OwnUsers::from_records(records(&[&keyed_root]), Some("root"))
                .unwrap()
                .users()
                .len(),
            1
        );
        assert_eq!(
            OwnUsers::from_records(records(&[&root]), None).unwrap().users().len(),
            1
        );
    }

    #[test]
    fn the_report_names_every_user_with_its_nodes_and_conflicts() {
        let (holdings, _) = pool();
        let report = AdoptionReport::build(&holdings);
        assert!(report.complete);
        assert!(report.has_own_users());
        assert_eq!((report.pool_size, report.majority), (3, 2));
        let names: Vec<&str> = report.users.iter().map(|name| name.username.as_str()).collect();
        assert_eq!(names, ["alice", "bob", "carol"]);

        let alice = &report.users[0];
        assert_eq!(alice.nodes, ["node-1"]);
        assert!(!alice.on_majority);
        assert!(alice.conflicts.is_empty());

        let bob = &report.users[1];
        assert_eq!(bob.nodes, ["node-1", "node-2", "node-3"]);
        assert!(bob.on_majority);
        assert_eq!(bob.conflicts, [NameConflict::Ids, NameConflict::Roles]);
        assert_eq!(bob.records.len(), 3);
        assert_eq!(bob.records[2].copies[0].node, "node-3");
        assert_eq!(bob.records[2].copies[0].role, Role::Reader);
        assert_eq!(
            bob.records[0].copies[0].access_key.as_deref(),
            Some("S4AKb1")
        );

        let carol = &report.users[2];
        assert!(carol.on_majority);
        assert_eq!(carol.conflicts, [NameConflict::Ids]);
        assert_eq!((report.conflicts, report.not_on_majority), (2, 1));

        let json = serde_json::to_string(&report).unwrap();
        assert!(!json.contains("secret"), "no secret is reported: {json}");
        assert!(!json.contains("hash-of"), "no password hash is reported");
    }

    #[test]
    fn the_report_id_follows_every_answer() {
        let (holdings, users) = pool();
        let id = AdoptionReport::build(&holdings).report_id;
        assert_eq!(AdoptionReport::build(&holdings).report_id, id);

        let mut changed = holdings.clone();
        let mut bob3 = users[3].clone();
        bob3.is_active = false;
        changed[2] = node(3, &[&bob3]);
        assert_ne!(AdoptionReport::build(&changed).report_id, id);

        let mut silent = holdings.clone();
        silent[2].own = Err("unreachable".to_string());
        let report = AdoptionReport::build(&silent);
        assert_ne!(report.report_id, id);
        assert!(!report.complete);
        assert_eq!(report.unanswered(), ["node-3"]);
    }

    #[test]
    fn copies_of_one_user_with_different_passwords_conflict() {
        let dana = user("d1", "dana", Role::Writer, 5);
        let mut dana_elsewhere = dana.clone();
        dana_elsewhere.password_hash = "another-hash".to_string();
        let report = AdoptionReport::build(&[node(1, &[&dana]), node(2, &[&dana_elsewhere])]);
        assert_eq!(report.users[0].conflicts, [NameConflict::Passwords]);
        assert_eq!(report.users[0].records.len(), 1);
    }

    #[test]
    fn majority_takes_the_names_most_nodes_hold_and_renames_the_later_ones() {
        let (holdings, _) = pool();
        let now = Utc::now();
        let plan = plan(
            &holdings,
            &request(AdoptionMode::Majority, &[]),
            &[],
            7,
            now,
        )
        .unwrap();
        let left: Vec<&str> = plan.left_out.iter().map(|user| user.id.as_str()).collect();
        assert_eq!(left, ["a1"], "alice is on one node of three");

        let by_id: BTreeMap<&str, &User> =
            plan.records.iter().map(|user| (user.id.as_str(), user)).collect();
        assert_eq!(by_id.len(), 5);
        assert_eq!(
            by_id["b1"].username, "bob",
            "the first created keeps the name"
        );
        assert!(by_id["b1"].is_active);
        assert_eq!(by_id["b1"].credential_version, 0);
        for id in ["b2", "b3"] {
            assert_eq!(by_id[id].username, format!("bob~{id}"));
            assert!(!by_id[id].is_active, "a renamed user is disabled");
            assert_eq!(by_id[id].credential_version, 1);
            assert_eq!(by_id[id].updated_at, now);
            assert!(by_id[id].access_key.is_some(), "keys go with their user");
        }
        assert_eq!(by_id["c1"].username, "carol");
        assert_eq!(by_id["c2"].username, "carol~c2");
        assert!(plan.records.iter().all(|user| user.created_epoch == 7));

        let renamed: Vec<_> = plan
            .adopted
            .iter()
            .filter(|user| user.renamed_from.is_some())
            .map(|user| user.id.as_str())
            .collect();
        assert_eq!(renamed, ["b2", "b3", "c2"]);
        assert_eq!(plan.adopted[0].nodes, ["node-1"]);
    }

    #[test]
    fn union_takes_everyone_and_users_exactly_the_list() {
        let (holdings, _) = pool();
        let union = plan(
            &holdings,
            &request(AdoptionMode::Union, &[]),
            &[],
            1,
            Utc::now(),
        )
        .unwrap();
        assert_eq!(union.records.len(), 6);
        assert!(union.left_out.is_empty());

        let listed = plan(
            &holdings,
            &request(AdoptionMode::Users, &["b3", "a1"]),
            &[],
            1,
            Utc::now(),
        )
        .unwrap();
        let ids: BTreeSet<&str> = listed.records.iter().map(|user| user.id.as_str()).collect();
        assert_eq!(ids, BTreeSet::from(["a1", "b3"]));
        let b3 = listed.records.iter().find(|user| user.id == "b3").unwrap();
        assert_eq!(b3.username, "bob", "the only bob taken over keeps the name");
        assert_eq!(listed.left_out.len(), 4);

        let nobody = plan(
            &holdings,
            &request(AdoptionMode::Users, &[]),
            &[],
            1,
            Utc::now(),
        )
        .unwrap();
        assert!(nobody.records.is_empty());
        assert_eq!(nobody.left_out.len(), 6);
    }

    #[test]
    fn a_decision_that_names_what_the_report_does_not_is_refused() {
        let (holdings, _) = pool();
        assert!(matches!(
            plan(
                &holdings,
                &request(AdoptionMode::Users, &["zz"]),
                &[],
                1,
                Utc::now()
            ),
            Err(IamError::AdoptionInvalid(_))
        ));
        assert!(matches!(
            plan(
                &holdings,
                &request(AdoptionMode::Union, &["a1"]),
                &[],
                1,
                Utc::now()
            ),
            Err(IamError::AdoptionInvalid(_))
        ));
    }

    /// Taken again after a decision stopped half way, the users the pool
    /// holds already stay as they are, and a name the pool's own user holds
    /// stays with it.
    #[test]
    fn the_pool_keeps_what_it_holds_already() {
        let (holdings, users) = pool();
        let mut held_alice = users[0].clone();
        held_alice.role = Role::Reader;
        let pools_bob = user("p1", "bob", Role::SuperUser, 100);
        let plan = plan(
            &holdings,
            &request(AdoptionMode::Union, &[]),
            &[held_alice, pools_bob],
            2,
            Utc::now(),
        )
        .unwrap();
        assert_eq!(plan.already_adopted, ["a1"]);
        assert!(plan.records.iter().all(|user| user.id != "a1"));
        let bobs: Vec<&str> = plan
            .records
            .iter()
            .filter(|user| user.username.starts_with("bob"))
            .map(|user| user.username.as_str())
            .collect();
        assert_eq!(bobs, ["bob~b1", "bob~b2", "bob~b3"]);
    }

    #[test]
    fn the_newest_copy_of_a_user_is_taken() {
        let eve = user("e1", "eve", Role::Writer, 30);
        let mut eve_changed = eve.clone();
        eve_changed.role = Role::Reader;
        eve_changed.updated_at = eve.updated_at + Duration::minutes(1);
        let holdings = [node(1, &[&eve]), node(2, &[&eve_changed])];
        let plan = plan(
            &holdings,
            &request(AdoptionMode::Union, &[]),
            &[],
            1,
            Utc::now(),
        )
        .unwrap();
        assert_eq!(plan.records.len(), 1);
        assert_eq!(plan.records[0].role, Role::Reader);
        assert_eq!(plan.adopted[0].nodes, ["node-1", "node-2"]);
    }

    #[test]
    fn a_decision_covers_a_node_while_it_keeps_what_the_operator_saw() {
        let (holdings, _) = pool();
        let report = AdoptionReport::build(&holdings);
        let record = AdoptionRecord {
            report_id: report.report_id.clone(),
            mode: AdoptionMode::Majority,
            decided_at: Utc::now(),
            decided_by: "root".to_string(),
            through: "node-1".to_string(),
            nodes: report
                .nodes
                .iter()
                .map(|node| (node.node_id.clone(), node.digest.clone().unwrap()))
                .collect(),
            adopted: Vec::new(),
            already_adopted: Vec::new(),
            left_out: Vec::new(),
            report: report.clone(),
        };
        let digest = report.nodes[1].digest.clone().unwrap();
        assert!(record.covers("id-2", &digest));
        assert!(!record.covers("id-2", "another"));
        assert!(!record.covers("id-9", &digest));
        let json = serde_json::to_vec(&record).unwrap();
        assert_eq!(
            serde_json::from_slice::<AdoptionRecord>(&json).unwrap(),
            record
        );
    }
}
