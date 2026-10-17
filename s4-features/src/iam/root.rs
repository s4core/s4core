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

//! The root account: configured by the environment, not stored.
//!
//! Root is a principal like the SuperUser of `S4_ACCESS_KEY_ID`: its name and
//! password come from `S4_ROOT_USERNAME` and `S4_ROOT_PASSWORD`, a login is
//! checked against the password the node started with, and its id is derived
//! from its name, so every node and every restart sees the same root. The only
//! stored pieces are [`RootState`] — the credential version that ends root's
//! sessions, and the fingerprint of the configuration it was last raised for,
//! the pool's on a cluster node — and each node's own record of the
//! configuration it last started with, which tells it when root changed.

use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::error::IamError;
use super::models::{Role, User};
use super::password::PasswordHasher;

/// Storage key of [`RootState`] in the `__system__` bucket: a record of the
/// cluster's control state, on every node of a pool.
pub const ROOT_STATE_KEY: &str = "__s4_iam_v2_root_state";

/// Storage key, in the `__system__` bucket, of the fingerprint of the root
/// configuration this node last started with. The node's own, never
/// replicated: it is outside `__s4_iam_v2_`.
pub const OWN_ROOT_FINGERPRINT_KEY: &str = "__s4_iam_node_root_fingerprint";

/// What a node's start does about root, by the configuration it last started
/// with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RootStart {
    /// The node started with this configuration before: nothing to do.
    Unchanged,
    /// The node never started with root: it records the configuration as its
    /// own and raises nothing — there were no root tokens of this node.
    First,
    /// The configuration changed since the node last started: the state is
    /// read, and raised unless it already records this configuration.
    Changed,
}

impl RootStart {
    /// The start with the configuration of fingerprint `current`, after the
    /// node last started with `own`.
    pub fn of(own: Option<&str>, current: &str) -> Self {
        match own {
            Some(own) if own == current => Self::Unchanged,
            Some(_) => Self::Changed,
            None => Self::First,
        }
    }
}

/// The root account of this node, as its configuration defines it.
#[derive(Clone)]
pub struct RootAccount {
    id: String,
    username: String,
    password_hash: String,
    fingerprint: String,
}

impl std::fmt::Debug for RootAccount {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RootAccount")
            .field("id", &self.id)
            .field("username", &self.username)
            .finish_non_exhaustive()
    }
}

impl RootAccount {
    /// Root named `username` with `password`.
    ///
    /// `jwt_secret` keys the configuration fingerprint, so the fingerprint, which
    /// is stored, says nothing about the password to whoever reads the data
    /// directory without the secret.
    ///
    /// # Errors
    ///
    /// `HashingFailed` if the password cannot be hashed or fingerprinted.
    pub fn new(username: &str, password: &str, jwt_secret: &str) -> Result<Self, IamError> {
        Ok(Self {
            id: Self::id_for(username),
            username: username.to_string(),
            password_hash: PasswordHasher::new().hash_password(password)?,
            fingerprint: fingerprint(jwt_secret, username, password)?,
        })
    }

    /// The id of the root named `username`: a UUIDv8 made of the first bytes of
    /// SHA-256 over the name, the same on every node and after every restart.
    pub fn id_for(username: &str) -> String {
        let digest = Sha256::digest(format!("s4-root:{username}").as_bytes());
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(&digest[..16]);
        uuid::Builder::from_custom_bytes(bytes).into_uuid().to_string()
    }

    /// Root's id.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Root's user name.
    pub fn username(&self) -> &str {
        &self.username
    }

    /// Fingerprint of the configured name, password and JWT secret.
    pub fn fingerprint(&self) -> &str {
        &self.fingerprint
    }

    /// The first 16 bytes of [`Self::fingerprint`]: what the node announces
    /// to its pool, which compares its nodes' root by it (decision 6).
    pub fn fingerprint_bytes(&self) -> [u8; 16] {
        let mut print = [0u8; 16];
        if let Some(bytes) = super::secrets::decode_hex(&self.fingerprint) {
            print.copy_from_slice(&bytes[..16]);
        }
        print
    }

    /// Whether `password` is the configured one.
    ///
    /// # Errors
    ///
    /// `InvalidHash` if the hash made at start cannot be read back.
    pub fn verify_password(&self, password: &str) -> Result<bool, IamError> {
        PasswordHasher::new().verify_password(password, &self.password_hash)
    }

    /// Root as a user record, for admin responses. It carries no password and
    /// no S3 credentials: neither is stored for root.
    pub fn as_user(&self, credential_version: u64) -> User {
        let epoch = DateTime::<Utc>::UNIX_EPOCH;
        User {
            id: self.id.clone(),
            username: self.username.clone(),
            password_hash: String::new(),
            role: Role::SuperUser,
            access_key: None,
            secret_key: None,
            secret_key_sealed: None,
            secret_key_hash: None,
            created_at: epoch,
            updated_at: epoch,
            is_active: true,
            credential_version,
            created_epoch: 0,
        }
    }
}

/// What a node keeps about root.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RootState {
    /// Root's credential version: root tokens carry it, and raising it ends them.
    pub credential_version: u64,
    /// Fingerprint of the configuration the version was last raised for.
    pub fingerprint: Option<String>,
}

impl RootState {
    /// The state after a node started with the changed configuration of
    /// fingerprint `current` ([`RootStart::Changed`]), and whether that ends
    /// root's sessions.
    ///
    /// A changed password (or name, or JWT secret) raises the version: tokens
    /// issued under the old password must not outlive it. A state that
    /// already records `current` was raised for it by another node of the
    /// pool, restarted with the same change first, and is kept: a change
    /// rolled through the nodes ends root's sessions once.
    pub fn after_change(&self, current: &str) -> (RootState, bool) {
        if self.fingerprint.as_deref() == Some(current) {
            return (self.clone(), false);
        }
        (
            RootState {
                credential_version: self.credential_version + 1,
                fingerprint: Some(current.to_string()),
            },
            true,
        )
    }
}

/// `HMAC-SHA256(jwt_secret, "root:" + username + ":" + password)`, hex.
fn fingerprint(jwt_secret: &str, username: &str, password: &str) -> Result<String, IamError> {
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(jwt_secret.as_bytes())
        .map_err(|_| IamError::HashingFailed)?;
    mac.update(format!("root:{username}:{password}").as_bytes());
    Ok(mac.finalize().into_bytes().iter().map(|b| format!("{b:02x}")).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECRET: &str = "test_secret_key_at_least_32_bytes";

    #[test]
    fn root_id_depends_on_the_name_only() {
        assert_eq!(RootAccount::id_for("root"), RootAccount::id_for("root"));
        assert_ne!(RootAccount::id_for("root"), RootAccount::id_for("admin"));

        let first = RootAccount::new("root", "first-password", SECRET).unwrap();
        let second = RootAccount::new(
            "root",
            "second-password",
            "another-secret-of-32-bytes-or-more",
        )
        .unwrap();
        assert_eq!(first.id(), second.id());

        let id = uuid::Uuid::parse_str(first.id()).unwrap();
        assert_eq!(id.get_version_num(), 8);
    }

    #[test]
    fn root_checks_the_configured_password() {
        let root = RootAccount::new("root", "the-password", SECRET).unwrap();

        assert!(root.verify_password("the-password").unwrap());
        assert!(!root.verify_password("another").unwrap());
    }

    #[test]
    fn fingerprint_changes_with_password_name_and_secret_and_hides_the_password() {
        let base = RootAccount::new("root", "pw-one", SECRET).unwrap();

        assert_eq!(
            base.fingerprint(),
            RootAccount::new("root", "pw-one", SECRET).unwrap().fingerprint()
        );
        assert_ne!(
            base.fingerprint(),
            RootAccount::new("root", "pw-two", SECRET).unwrap().fingerprint()
        );
        assert_ne!(
            base.fingerprint(),
            RootAccount::new("admin", "pw-one", SECRET).unwrap().fingerprint()
        );
        assert_ne!(
            base.fingerprint(),
            RootAccount::new("root", "pw-one", "another-secret-of-32-bytes-or-more")
                .unwrap()
                .fingerprint()
        );
        assert!(!base.fingerprint().contains("pw-one"));
        assert_eq!(base.fingerprint().len(), 64);
    }

    #[test]
    fn a_start_is_judged_by_the_configuration_the_node_last_started_with() {
        assert_eq!(RootStart::of(None, "fp-1"), RootStart::First);
        assert_eq!(RootStart::of(Some("fp-1"), "fp-1"), RootStart::Unchanged);
        assert_eq!(RootStart::of(Some("fp-1"), "fp-2"), RootStart::Changed);
    }

    #[test]
    fn a_new_password_raises_the_credential_version_once() {
        let (changed, ended) = RootState::default().after_change("fp-2");
        assert_eq!(changed.credential_version, 1);
        assert_eq!(changed.fingerprint.as_deref(), Some("fp-2"));
        assert!(ended);

        // The next node restarted with the same change finds it raised.
        let (again, ended) = changed.after_change("fp-2");
        assert_eq!(again, changed);
        assert!(!ended);

        let (next, ended) = again.after_change("fp-3");
        assert_eq!(next.credential_version, 2);
        assert!(ended);
    }

    #[test]
    fn root_record_has_no_secrets() {
        let user = RootAccount::new("root", "pw", SECRET).unwrap().as_user(3);

        assert_eq!(user.role, Role::SuperUser);
        assert!(user.password_hash.is_empty());
        assert!(user.secret_key.is_none());
        assert_eq!(user.credential_version, 3);
    }
}
