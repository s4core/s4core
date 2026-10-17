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

//! The audit log of IAM changes.
//!
//! Every change of IAM writes, under the same lease and through the same
//! quorums, one entry `__system__/__s4_iam_v2_audit_<epoch>_<time>_<n>`: who
//! made it, what it did and to whom, through which node and under which
//! epoch of the lease (plan CLUSTER-IAM-REPLICATION.md, decision 10). The
//! entries are records of the cluster's control state, on every node of a
//! pool, so the log is read from the node's own copy. The key orders them as
//! the changes were made: the epoch of the lease grows with every change of
//! the pool; on a single node it is `0` and the time orders them. Entries
//! older than the retention are deleted under the lease.
//!
//! An entry never holds a password, a secret or a hash of one.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use rand::Rng;
use serde::{Deserialize, Serialize};

use super::models::{AdminPrincipal, Role, User};

/// Key prefix of an audit entry in `__system__`.
pub const AUDIT_KEY_PREFIX: &str = "__s4_iam_v2_audit_";

/// Who made a change of IAM.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IamActor {
    /// The account's id; `None` for the server itself.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// The account's name, or `system`.
    pub name: String,
}

impl IamActor {
    /// The server itself: a start, a background pass.
    pub fn system() -> Self {
        Self {
            id: None,
            name: "system".into(),
        }
    }
}

impl From<&AdminPrincipal> for IamActor {
    fn from(principal: &AdminPrincipal) -> Self {
        Self {
            id: Some(principal.user_id.clone()),
            name: principal.username.clone(),
        }
    }
}

/// What a change of IAM did.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditAction {
    /// A user was created.
    UserCreated,
    /// A user's password, role or active flag changed.
    UserUpdated,
    /// A user was deleted.
    UserDeleted,
    /// S3 credentials were issued to a user, replacing any it had.
    CredentialsIssued,
    /// A user's S3 credentials were deleted.
    CredentialsDeleted,
    /// A user's admin sessions were ended.
    SessionsRevoked,
    /// Root's admin sessions were ended: revoked, or a new configuration of
    /// root at a start.
    RootSessionsRevoked,
    /// The pool took over the IAM its nodes kept of their own.
    IamAdopted,
    /// Secrets of S3 credentials were encrypted again with the current key.
    SecretsResealed,
    /// Entries older than the retention were deleted.
    AuditPurged,
}

/// One entry of the audit log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditEntry {
    /// The entry's id: its key without the prefix.
    pub id: String,
    /// When the change was made, by the clock of the node that made it.
    pub at: DateTime<Utc>,
    /// Who made it.
    pub actor: IamActor,
    /// What it did.
    pub action: AuditAction,
    /// The id of the user it was made to.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_id: Option<String>,
    /// That user's name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
    /// The node the change was made through.
    pub through: String,
    /// The epoch of the lease it was made under; `0` on a single node.
    pub epoch: u64,
    /// What changed, as `field → value` or `field → before -> after`; never a
    /// password or a secret.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub details: BTreeMap<String, String>,
}

impl AuditEntry {
    /// An entry of `action` by `actor` through `through` under `epoch`, at
    /// `at`, with an id that orders it after every entry of an earlier epoch
    /// and, in one epoch, by time.
    pub fn new(
        action: AuditAction,
        actor: IamActor,
        through: &str,
        epoch: u64,
        at: DateTime<Utc>,
    ) -> Self {
        let micros = at.timestamp_micros().max(0);
        let nonce: u32 = rand::thread_rng().gen();
        Self {
            id: format!("{epoch:020}_{micros:020}_{nonce:08x}"),
            at,
            actor,
            action,
            target_id: None,
            target: None,
            through: through.to_string(),
            epoch,
            details: BTreeMap::new(),
        }
    }

    /// The same entry, about `user`.
    pub fn about(mut self, user: &User) -> Self {
        self.target_id = Some(user.id.clone());
        self.target = Some(user.username.clone());
        self
    }

    /// The same entry, about the account `id` named `name`.
    pub fn about_account(mut self, id: &str, name: &str) -> Self {
        self.target_id = Some(id.to_string());
        self.target = Some(name.to_string());
        self
    }

    /// The same entry, saying `field` is `value`.
    pub fn detail(mut self, field: &str, value: impl Into<String>) -> Self {
        self.details.insert(field.to_string(), value.into());
        self
    }

    /// The entry's key in `__system__`.
    pub fn key(&self) -> String {
        audit_key(&self.id)
    }
}

/// The key of the audit entry `id`.
pub fn audit_key(id: &str) -> String {
    format!("{AUDIT_KEY_PREFIX}{id}")
}

/// How a user changed, for the audit log: what an update changed, by name.
pub fn changes_of(before: &User, after: &User) -> BTreeMap<String, String> {
    let mut changes = BTreeMap::new();
    if before.password_hash != after.password_hash {
        changes.insert("password".into(), "changed".into());
    }
    if before.role != after.role {
        changes.insert(
            "role".into(),
            format!("{} -> {}", role_name(before.role), role_name(after.role)),
        );
    }
    if before.is_active != after.is_active {
        changes.insert(
            "is_active".into(),
            format!("{} -> {}", before.is_active, after.is_active),
        );
    }
    changes
}

/// The name of `role`, as the API spells it.
pub fn role_name(role: Role) -> &'static str {
    match role {
        Role::Reader => "Reader",
        Role::Writer => "Writer",
        Role::SuperUser => "SuperUser",
    }
}

/// A page of the audit log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AuditPage {
    /// The entries, in the order the changes were made.
    pub entries: Vec<AuditEntry>,
    /// The id of the last entry, to continue after, when more follow.
    pub next_after: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn user(name: &str, role: Role) -> User {
        User {
            id: format!("id-{name}"),
            username: name.into(),
            password_hash: "hash".into(),
            role,
            access_key: None,
            secret_key: None,
            secret_key_sealed: None,
            secret_key_hash: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            is_active: true,
            credential_version: 0,
            created_epoch: 0,
        }
    }

    #[test]
    fn ids_order_entries_by_epoch_then_time() {
        let at = Utc::now();
        let early = AuditEntry::new(AuditAction::UserCreated, IamActor::system(), "n", 7, at);
        let later_time = AuditEntry::new(
            AuditAction::UserCreated,
            IamActor::system(),
            "n",
            7,
            at + chrono::Duration::milliseconds(1),
        );
        // A later epoch is a later change, whatever the clock of its node says.
        let later_epoch = AuditEntry::new(
            AuditAction::UserCreated,
            IamActor::system(),
            "n",
            8,
            at - chrono::Duration::seconds(5),
        );
        assert!(early.key() < later_time.key());
        assert!(later_time.key() < later_epoch.key());
        assert!(early.key().starts_with(AUDIT_KEY_PREFIX));
    }

    #[test]
    fn an_update_is_described_without_its_secrets() {
        let before = user("ann", Role::Writer);
        let mut after = before.clone();
        after.password_hash = "another-hash".into();
        after.role = Role::Reader;
        after.is_active = false;

        let changes = changes_of(&before, &after);
        assert_eq!(changes["password"], "changed");
        assert_eq!(changes["role"], "Writer -> Reader");
        assert_eq!(changes["is_active"], "true -> false");
        let entry = AuditEntry::new(
            AuditAction::UserUpdated,
            IamActor::system(),
            "node-1",
            3,
            Utc::now(),
        )
        .about(&after);
        let json = serde_json::to_string(&entry).unwrap();
        assert!(!json.contains("hash"), "{json}");
        assert!(changes_of(&before, &before).is_empty());
    }
}
