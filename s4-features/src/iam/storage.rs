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

//! IAM data storage in the `__system__` bucket.
//!
//! A user is one record, `__s4_iam_v2_user_<id>`: name, password hash, role,
//! active flag, S3 credentials and credential version together (plan
//! CLUSTER-IAM-REPLICATION.md, decision 2). Creating a user, issuing its keys
//! and deleting it each write or delete that one record, so no change is left
//! half done. Lookups by id, name and access key read the node's own copy
//! through [`IamDirectory`]; a change reads the record from, and writes it to,
//! the [`IamRecordStore`]: the node's storage on a single node, a majority of
//! the pool on a cluster node.

use std::sync::Arc;

use uuid::Uuid;

use s4_core::storage::StorageEngine;
use s4_core::types::SYSTEM_BUCKET;

use super::adoption::{
    self, own_user_matters, AdoptionRecord, AdoptionReport, AdoptionRequest, NodeHoldings,
    OwnUsers, ADOPTION_KEY,
};
use super::audit::{
    audit_key, changes_of, role_name, AuditAction, AuditEntry, AuditPage, IamActor,
    AUDIT_KEY_PREFIX,
};
use super::error::IamError;
use super::gate::{AdoptionGate, CredentialGate, NotServing};
use super::models::{Role, S3Credentials, User};
use super::password::PasswordHasher;
use super::root::{RootState, OWN_ROOT_FINGERPRINT_KEY, ROOT_STATE_KEY};
use super::secrets::{SecretKeySettings, SecretKeyring};
use super::store::{IamChange, IamRecordStore, LocalRecordStore};
use super::user_directory::{
    list_keys, user_key, IamDirectory, UserRecords, LEGACY_USER_KEY_PREFIX, USER_KEY_PREFIX,
};

/// Key prefix of a name index of an earlier release: `__s4_iam_username_<name>`.
const LEGACY_USERNAME_KEY_PREFIX: &str = "__s4_iam_username_";

/// Key prefix of an access key index of an earlier release:
/// `__s4_iam_access_key_<key>`.
const LEGACY_ACCESS_KEY_KEY_PREFIX: &str = "__s4_iam_access_key_";

/// Present in a node's own storage while the users of version 2 it holds were
/// written by it as a single node: they are its own IAM, which a pool it
/// joins has to take over (plan CLUSTER-IAM-REPLICATION.md, decision 8.6). Not
/// replicated.
const SINGLE_NODE_USERS_KEY: &str = "__s4_iam_node_single_users";

/// What [`IamStorage::convert_legacy`] did with the records of an earlier
/// release.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LegacyConversion {
    /// Users written as records of version 2.
    pub converted: usize,
    /// Users that already had a record of version 2, which was kept.
    pub already_current: usize,
    /// Stored root users without S3 credentials, dropped: root is configured,
    /// and such a record can neither log in nor sign a request.
    pub dropped_root: usize,
}

/// What [`IamStorage::reseal_secrets`] did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Resealing {
    /// Users whose secret was sealed again with the current key.
    pub resealed: usize,
    /// Users whose secret is sealed with a key this node does not have: left
    /// as they are, for a node that has it.
    pub unreadable: usize,
}

/// The name a node gives itself in the audit log when nothing else names it.
const DEFAULT_NODE_NAME: &str = "standalone";

/// IAM data storage in the `__system__` bucket.
#[derive(Clone)]
pub struct IamStorage {
    /// This node's own storage: the records of an earlier release and root's
    /// state are read here.
    local: Arc<dyn StorageEngine>,
    /// Where a change is read and written.
    store: Arc<dyn IamRecordStore>,
    directory: Arc<IamDirectory>,
    hasher: PasswordHasher,
    /// When IAM credentials are checked against `directory`; always, without
    /// one.
    gate: Option<Arc<dyn CredentialGate>>,
    /// When IAM may be changed: once no node of the pool keeps IAM of its own
    /// the pool has not taken over; always, without one.
    adoption_gate: Option<Arc<dyn AdoptionGate>>,
    /// Who the changes made through this value are made by, for the audit log.
    actor: IamActor,
    /// The node they are made through, for the audit log.
    node: Arc<str>,
}

impl IamStorage {
    /// IAM in `storage`, this node's own, changed in it too: a single node.
    ///
    /// # Arguments
    ///
    /// * `storage` - Storage engine to use for persisting IAM data
    pub fn new(storage: Arc<dyn StorageEngine>) -> Self {
        Self {
            store: Arc::new(LocalRecordStore::new(storage.clone())),
            directory: Arc::new(IamDirectory::new(storage.clone())),
            local: storage,
            hasher: PasswordHasher::new(),
            gate: None,
            adoption_gate: None,
            actor: IamActor::system(),
            node: Arc::from(DEFAULT_NODE_NAME),
        }
    }

    /// The same IAM, its changes made by `actor` in the audit log.
    pub fn acting_as(&self, actor: IamActor) -> Self {
        Self {
            actor,
            ..self.clone()
        }
    }

    /// The same IAM, its changes made through the node `name` in the audit
    /// log.
    pub fn with_node_name(&self, name: &str) -> Self {
        Self {
            node: Arc::from(name),
            ..self.clone()
        }
    }

    /// Seals and opens the secrets of S3 credentials with the keys
    /// `settings` name from now on, through every value sharing this
    /// directory (plan CLUSTER-IAM-REPLICATION.md, decision 7). The keys they
    /// make; without configured ones, the key this node generated for
    /// itself, generated now if it has none.
    ///
    /// # Errors
    ///
    /// `Storage`, `Json` or `InvalidSecretKey` when the records or the node's
    /// own key cannot be read or kept.
    pub async fn use_secret_keys(
        &self,
        settings: SecretKeySettings,
    ) -> Result<Arc<SecretKeyring>, IamError> {
        self.directory.use_key_settings(settings).await?;
        self.secret_keys().await
    }

    /// The keys secrets are sealed and opened with.
    ///
    /// # Errors
    ///
    /// `Storage` or `InvalidSecretKey` when the node's own key cannot be read
    /// or kept.
    pub async fn secret_keys(&self) -> Result<Arc<SecretKeyring>, IamError> {
        self.directory
            .keyring(true)
            .await?
            .ok_or_else(|| IamError::InvalidSecretKey("no key to seal secrets with".into()))
    }

    /// `user` as its record is written: the secret of its S3 credentials
    /// sealed with the current key, none in clear text.
    async fn sealed(&self, user: &User) -> Result<User, IamError> {
        self.directory.sealed(user).await
    }

    /// Writes `entry` to the audit log under `change`.
    async fn audit_under(
        &self,
        change: &dyn IamChange,
        entry: &AuditEntry,
    ) -> Result<(), IamError> {
        change.write(&entry.key(), &serde_json::to_vec(entry)?).await
    }

    /// A new entry of the audit log for a change by this value's actor
    /// through this node under `change`.
    fn entry(&self, action: AuditAction, change: &dyn IamChange) -> AuditEntry {
        AuditEntry::new(
            action,
            self.actor.clone(),
            &self.node,
            change.epoch(),
            chrono::Utc::now(),
        )
    }

    /// The same IAM, changed through `store` from now on: a cluster node reads
    /// a record it changes from a majority of its pool and writes it to one.
    /// Lookups still read this node's own copy, through the same directory.
    pub fn with_record_store(&self, store: Arc<dyn IamRecordStore>) -> Self {
        Self {
            store,
            ..self.clone()
        }
    }

    /// The same IAM, its credentials checked only while `gate` admits them: a
    /// cluster node, whose own copy has to be confirmed current with its
    /// pool first (plan CLUSTER-IAM-REPLICATION.md, decision 4).
    pub fn with_credential_gate(&self, gate: Arc<dyn CredentialGate>) -> Self {
        Self {
            gate: Some(gate),
            ..self.clone()
        }
    }

    /// The same IAM, changed only while `gate` is open: a cluster node, whose
    /// pool may hold IAM nodes kept of their own, which the operator has to
    /// decide on first (plan CLUSTER-IAM-REPLICATION.md, decision 8).
    pub fn with_adoption_gate(&self, gate: Arc<dyn AdoptionGate>) -> Self {
        Self {
            adoption_gate: Some(gate),
            ..self.clone()
        }
    }

    /// Whether IAM may be changed through this node now: it serves the
    /// records of version 2 and no node of its pool keeps IAM of its own.
    pub fn changes_open(&self) -> bool {
        self.directory.records() == UserRecords::Current
            && self.adoption_gate.as_ref().is_none_or(|gate| gate.is_open())
    }

    /// Whether IAM credentials — a password, a token or an S3 key of a stored
    /// user — may be checked against this node's copy now. The IAM an
    /// earlier release left on this node is its own and never changes: it is
    /// served as before.
    ///
    /// # Errors
    ///
    /// `NotServing` while the gate is closed.
    pub fn admit_credentials(&self) -> Result<(), IamError> {
        if self.directory.records() == UserRecords::Legacy {
            return Ok(());
        }
        match &self.gate {
            Some(gate) => gate.admit().map_err(IamError::NotServing),
            None => Ok(()),
        }
    }

    /// The users of this node's own copy. A cluster node tells it of every
    /// change of its storage ([`IamDirectory::observe`]).
    pub fn directory(&self) -> Arc<IamDirectory> {
        self.directory.clone()
    }

    /// Changes are refused while the node serves the IAM it keeps of its
    /// own, and while some other node of its pool does (decision 8).
    fn ensure_changeable(&self) -> Result<(), IamError> {
        if self.changes_open() {
            Ok(())
        } else {
            Err(IamError::AdoptionPending)
        }
    }

    /// Takes the lease of IAM changes for one change of a user.
    async fn begin(&self) -> Result<Box<dyn IamChange>, IamError> {
        self.ensure_changeable()?;
        self.store.begin().await
    }

    /// The record of user `id` as the store has it, read under `change`.
    async fn user_under(change: &dyn IamChange, id: &str) -> Result<User, IamError> {
        let data = change.read(&user_key(id)).await?.ok_or(IamError::UserNotFound)?;
        let user: User = serde_json::from_slice(&data)?;
        if user.id != id {
            return Err(IamError::UserNotFound);
        }
        Ok(user)
    }

    /// Writes `user` as its record under `change`, and brings this node's
    /// directory to it.
    async fn save_under(&self, change: &dyn IamChange, user: &User) -> Result<(), IamError> {
        let data = serde_json::to_vec(&self.sealed(user).await?)?;
        change.write(&user_key(&user.id), &data).await?;
        self.directory.refresh(&user.id).await
    }

    /// Changes user `id` by `edit` under the lease, and writes it, with an
    /// entry of `action` in the audit log, if `edit` says it changed
    /// something; the user as it is afterwards.
    async fn change_user<F>(&self, id: &str, action: AuditAction, edit: F) -> Result<User, IamError>
    where
        F: FnOnce(&mut User) -> bool + Send,
    {
        let change = self.begin().await?;
        let result = async {
            let mut user = Self::user_under(&*change, id).await?;
            let before = user.clone();
            if edit(&mut user) {
                user.updated_at = chrono::Utc::now();
                self.save_under(&*change, &user).await?;
                let mut entry = self.entry(action, &*change).about(&user);
                match action {
                    AuditAction::UserUpdated => entry.details = changes_of(&before, &user),
                    AuditAction::CredentialsIssued => {
                        entry =
                            entry.detail("access_key", user.access_key.clone().unwrap_or_default())
                    }
                    AuditAction::CredentialsDeleted => {
                        entry = entry
                            .detail("access_key", before.access_key.clone().unwrap_or_default())
                    }
                    _ => {}
                }
                self.audit_under(&*change, &entry).await?;
            }
            Ok(user)
        }
        .await;
        change.finish().await;
        result
    }

    /// Whether a user named `username` exists, as the store has the users,
    /// read under `change`: on a cluster node, from a majority of the pool,
    /// so a name taken through any node is seen.
    async fn name_taken(change: &dyn IamChange, username: &str) -> Result<bool, IamError> {
        for (_, data) in change.records(USER_KEY_PREFIX).await? {
            let user: User = serde_json::from_slice(&data)?;
            if user.username == username {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Create new user.
    ///
    /// # Arguments
    ///
    /// * `username` - Username (3-32 alphanumeric characters or underscore)
    /// * `password` - Plain text password
    /// * `role` - User role
    ///
    /// # Returns
    ///
    /// Returns created user with generated ID.
    ///
    /// # Errors
    ///
    /// - `InvalidUsername` if username format is invalid
    /// - `UserAlreadyExists` if username is already taken, through any node
    /// - `AdoptionPending` while the node serves the IAM of an earlier release
    /// - `Busy` if another change of IAM held the lease for the whole wait
    /// - `Unavailable` if too few nodes of the pool answered
    /// - `Storage` errors if database operations fail
    pub async fn create_user(
        &self,
        username: String,
        password: String,
        role: Role,
    ) -> Result<User, IamError> {
        self.ensure_changeable()?;
        // Validate username (alphanumeric + underscore, 3-32 chars)
        if !username.chars().all(|c| c.is_alphanumeric() || c == '_') {
            return Err(IamError::InvalidUsername);
        }
        if username.len() < 3 || username.len() > 32 {
            return Err(IamError::InvalidUsername);
        }
        // Hashed before the lease is taken: the lease is held for the change
        // alone.
        let password_hash = self.hasher.hash_password(&password)?;

        let change = self.begin().await?;
        let result = async {
            if Self::name_taken(&*change, &username).await? {
                return Err(IamError::UserAlreadyExists);
            }
            let now = chrono::Utc::now();
            let user = User {
                id: Uuid::new_v4().to_string(),
                username,
                password_hash,
                role,
                access_key: None,
                secret_key: None,
                secret_key_sealed: None,
                secret_key_hash: None,
                created_at: now,
                updated_at: now,
                is_active: true,
                credential_version: 0,
                created_epoch: change.epoch(),
            };
            self.save_under(&*change, &user).await?;
            let entry = self
                .entry(AuditAction::UserCreated, &*change)
                .about(&user)
                .detail("role", role_name(user.role));
            self.audit_under(&*change, &entry).await?;
            Ok(user)
        }
        .await;
        change.finish().await;
        result
    }

    /// Get user by username.
    pub async fn get_user_by_username(&self, username: &str) -> Result<Option<User>, IamError> {
        self.directory.user_by_name(username).await
    }

    /// Get user by ID.
    pub async fn get_user(&self, user_id: &str) -> Result<Option<User>, IamError> {
        self.directory.user(user_id).await
    }

    /// Get user by access key.
    pub async fn get_user_by_access_key(&self, access_key: &str) -> Result<Option<User>, IamError> {
        self.directory.user_by_access_key(access_key).await
    }

    /// Generate S3 credentials for user, replacing any it had.
    ///
    /// # Arguments
    ///
    /// * `user_id` - User ID to generate credentials for
    ///
    /// # Returns
    ///
    /// Returns S3 credentials with access key and secret key.
    /// **IMPORTANT**: Secret key is only returned once and should be saved by client.
    ///
    /// # Errors
    ///
    /// - `UserNotFound` if user doesn't exist
    /// - `Busy` if another change of IAM held the lease for the whole wait
    /// - `Unavailable` if too few nodes of the pool answered
    /// - `Storage` errors if database operations fail
    pub async fn generate_s3_credentials(&self, user_id: &str) -> Result<S3Credentials, IamError> {
        // Generate random access key and secret key
        let access_key = format!("S4AK{}", nanoid::nanoid!(20));
        let secret_key = nanoid::nanoid!(40);
        let secret_key_hash = self.hasher.hash_password(&secret_key)?;

        // The keys are part of the user's record: the previous access key goes
        // with the same write.
        self.change_user(user_id, AuditAction::CredentialsIssued, |user| {
            user.access_key = Some(access_key.clone());
            user.secret_key = Some(secret_key.clone());
            user.secret_key_sealed = None;
            user.secret_key_hash = Some(secret_key_hash);
            true
        })
        .await?;

        Ok(S3Credentials {
            access_key,
            secret_key,
        })
    }

    /// List all users, in the order of their ids.
    pub async fn list_users(&self) -> Result<Vec<User>, IamError> {
        self.directory.users().await
    }

    /// Up to `max` users after the id `after`, in the order of their ids, and
    /// whether more follow.
    ///
    /// # Errors
    ///
    /// `Storage` or `Json` when the records cannot be loaded.
    pub async fn list_users_after(
        &self,
        after: Option<&str>,
        max: usize,
    ) -> Result<(Vec<User>, bool), IamError> {
        self.directory.users_after(after, max).await
    }

    /// Check if user exists.
    pub async fn user_exists(&self, username: &str) -> Result<bool, IamError> {
        Ok(self.get_user_by_username(username).await?.is_some())
    }

    /// Verify password for user.
    pub async fn verify_password(&self, username: &str, password: &str) -> Result<bool, IamError> {
        let user = match self.get_user_by_username(username).await? {
            Some(u) => u,
            None => return Ok(false),
        };

        if !user.is_active {
            return Ok(false);
        }

        self.hasher.verify_password(password, &user.password_hash)
    }

    /// Get user by access key with secret key (for AWS Signature V4).
    ///
    /// This method returns the user with their secret_key field populated,
    /// which is needed for AWS Signature V4 HMAC computation.
    ///
    /// # Security
    ///
    /// The secret_key is stored in the database but never returned in API responses
    /// (they go through `UserResponse`). This method should only be used
    /// internally for signature verification.
    pub async fn get_user_by_access_key_with_secret(
        &self,
        access_key: &str,
    ) -> Result<Option<User>, IamError> {
        self.admit_credentials()?;
        let user = match self.get_user_by_access_key(access_key).await? {
            Some(u) => u,
            None => return Ok(None),
        };

        if !user.is_active {
            return Ok(None);
        }

        if user.secret_key.is_none() {
            // Sealed with a key this node does not have, or not opening:
            // another node may check it.
            if user.secret_key_sealed.is_some() {
                return Err(IamError::NotServing(NotServing::SecretKeyUnknown));
            }
            return Err(IamError::NoS3Credentials);
        }

        Ok(Some(user))
    }

    /// Verify S3 secret key for access key.
    pub async fn verify_s3_credentials(
        &self,
        access_key: &str,
        secret_key: &str,
    ) -> Result<Option<User>, IamError> {
        self.admit_credentials()?;
        let user = match self.get_user_by_access_key(access_key).await? {
            Some(u) => u,
            None => return Ok(None),
        };

        if !user.is_active {
            return Ok(None);
        }

        let secret_hash = user.secret_key_hash.as_ref().ok_or(IamError::NoS3Credentials)?;

        if self.hasher.verify_password(secret_key, secret_hash)? {
            Ok(Some(user))
        } else {
            Ok(None)
        }
    }

    /// Update user.
    ///
    /// # Arguments
    ///
    /// * `user_id` - User ID to update
    /// * `password` - New password (if provided)
    /// * `role` - New role (if provided)
    /// * `is_active` - New active status (if provided)
    ///
    /// # Errors
    ///
    /// - `UserNotFound` if user doesn't exist
    /// - `Busy` if another change of IAM held the lease for the whole wait
    /// - `Unavailable` if too few nodes of the pool answered
    pub async fn update_user(
        &self,
        user_id: &str,
        password: Option<String>,
        role: Option<Role>,
        is_active: Option<bool>,
    ) -> Result<User, IamError> {
        let password_hash = match password {
            Some(new_password) => Some(self.hasher.hash_password(&new_password)?),
            None => None,
        };
        // A new password, a new role and a deactivation each end the sessions
        // issued before them.
        self.change_user(user_id, AuditAction::UserUpdated, |user| {
            let mut ends_sessions = false;
            if let Some(hash) = password_hash {
                user.password_hash = hash;
                ends_sessions = true;
            }
            if let Some(new_role) = role {
                ends_sessions |= user.role != new_role;
                user.role = new_role;
            }
            if let Some(active) = is_active {
                ends_sessions |= user.is_active != active;
                user.is_active = active;
            }
            if ends_sessions {
                user.credential_version += 1;
            }
            true
        })
        .await
    }

    /// Ends every session of the user: tokens issued before the call stop
    /// validating. The password, role and S3 credentials stay as they are.
    ///
    /// # Errors
    ///
    /// - `UserNotFound` if user doesn't exist
    /// - `Busy` if another change of IAM held the lease for the whole wait
    /// - `Unavailable` if too few nodes of the pool answered
    /// - `Storage` errors if database operations fail
    pub async fn revoke_sessions(&self, user_id: &str) -> Result<User, IamError> {
        self.change_user(user_id, AuditAction::SessionsRevoked, |user| {
            user.credential_version += 1;
            true
        })
        .await
    }

    /// Root's state as this node holds it; the default when nothing is kept
    /// yet. What a login and a token of root are checked against.
    pub async fn root_state(&self) -> Result<RootState, IamError> {
        match self.local.get_object(SYSTEM_BUCKET, ROOT_STATE_KEY).await {
            Ok((bytes, _)) => Ok(serde_json::from_slice(&bytes)?),
            Err(s4_core::error::StorageError::ObjectNotFound { .. }) => Ok(RootState::default()),
            Err(e) => Err(e.into()),
        }
    }

    /// Changes root's state by `edit` under the lease of IAM changes: `edit`
    /// gets the state as the store has it — on a cluster node, the newest a
    /// majority of the pool holds — and the result is written when it
    /// differs. The state before and after.
    ///
    /// Root's state is not a user's and changes also while the node serves
    /// the IAM of an earlier release. A change that ends root's sessions is
    /// written to the audit log about `root` — its id and name — for
    /// `reason`.
    ///
    /// # Errors
    ///
    /// `Busy` if another change held the lease for the whole wait;
    /// `Unavailable` if too few nodes of the pool answered.
    pub async fn change_root_state<F>(
        &self,
        root: (&str, &str),
        reason: &str,
        edit: F,
    ) -> Result<(RootState, RootState), IamError>
    where
        F: FnOnce(&RootState) -> RootState + Send,
    {
        let change = self.store.begin().await?;
        let result = async {
            let known = match change.read(ROOT_STATE_KEY).await? {
                Some(bytes) => serde_json::from_slice(&bytes)?,
                None => RootState::default(),
            };
            let state = edit(&known);
            if state != known {
                change.write(ROOT_STATE_KEY, &serde_json::to_vec(&state)?).await?;
                if state.credential_version != known.credential_version {
                    let entry = self
                        .entry(AuditAction::RootSessionsRevoked, &*change)
                        .about_account(root.0, root.1)
                        .detail("reason", reason);
                    self.audit_under(&*change, &entry).await?;
                }
            }
            Ok((known, state))
        }
        .await;
        change.finish().await;
        result
    }

    /// The fingerprint of the root configuration this node last started
    /// with, kept in its own storage only.
    ///
    /// # Errors
    ///
    /// `Storage` when it cannot be read.
    pub async fn own_root_fingerprint(&self) -> Result<Option<String>, IamError> {
        match LocalRecordStore::new(self.local.clone()).read(OWN_ROOT_FINGERPRINT_KEY).await? {
            Some(bytes) => Ok(Some(String::from_utf8(bytes)?)),
            None => Ok(None),
        }
    }

    /// Records `fingerprint` as the root configuration this node started
    /// with, in its own storage only.
    ///
    /// # Errors
    ///
    /// `Storage` when it cannot be written.
    pub async fn save_own_root_fingerprint(&self, fingerprint: &str) -> Result<(), IamError> {
        LocalRecordStore::new(self.local.clone())
            .write(OWN_ROOT_FINGERPRINT_KEY, fingerprint.as_bytes())
            .await
    }

    /// Delete user: its record, and with it its name and S3 credentials.
    ///
    /// # Arguments
    ///
    /// * `user_id` - User ID to delete
    ///
    /// # Errors
    ///
    /// - `UserNotFound` if user doesn't exist
    /// - `Busy` if another change of IAM held the lease for the whole wait
    /// - `Unavailable` if too few nodes of the pool confirmed the delete
    /// - `Storage` errors if database operations fail
    pub async fn delete_user(&self, user_id: &str) -> Result<(), IamError> {
        let change = self.begin().await?;
        let result = async {
            let user = Self::user_under(&*change, user_id).await?;
            change.delete(&user_key(user_id)).await?;
            self.directory.refresh(user_id).await?;
            let entry = self.entry(AuditAction::UserDeleted, &*change).about(&user);
            self.audit_under(&*change, &entry).await
        }
        .await;
        change.finish().await;
        result
    }

    /// Delete S3 credentials for user.
    ///
    /// # Arguments
    ///
    /// * `user_id` - User ID to delete credentials for
    ///
    /// # Errors
    ///
    /// - `UserNotFound` if user doesn't exist
    /// - `Busy` if another change of IAM held the lease for the whole wait
    /// - `Unavailable` if too few nodes of the pool answered
    pub async fn delete_s3_credentials(&self, user_id: &str) -> Result<(), IamError> {
        self.change_user(user_id, AuditAction::CredentialsDeleted, |user| {
            user.access_key = None;
            user.secret_key = None;
            user.secret_key_sealed = None;
            user.secret_key_hash = None;
            true
        })
        .await
        .map(|_| ())
    }

    /// The users an earlier release stored on this node, one record each
    /// under `__s4_iam_user_<id>`.
    ///
    /// # Errors
    ///
    /// `Storage` or `Json` when a record cannot be read.
    pub async fn legacy_users(&self) -> Result<Vec<User>, IamError> {
        let mut users = Vec::new();
        for key in list_keys(&*self.local, LEGACY_USER_KEY_PREFIX).await? {
            match self.local.get_object(SYSTEM_BUCKET, &key).await {
                Ok((data, _)) => users.push(serde_json::from_slice(&data)?),
                Err(s4_core::error::StorageError::ObjectNotFound { .. }) => {}
                Err(e) => return Err(e.into()),
            }
        }
        Ok(users)
    }

    /// Whether `user`, stored by an earlier release, still means something:
    /// every user does but a stored root without S3 credentials, which can
    /// neither log in (root is configured now) nor sign a request.
    pub fn legacy_user_matters(user: &User, root_name: Option<&str>) -> bool {
        own_user_matters(user, root_name)
    }

    /// Serves the users an earlier release stored on this node, as it did,
    /// and refuses every change of them (`AdoptionPending`): a cluster node
    /// whose IAM of an earlier release has not been taken over by the pool
    /// (decision 8). Root's state is not among them and still changes.
    ///
    /// # Errors
    ///
    /// `Storage` or `Json` when the records cannot be read.
    pub async fn serve_legacy(&self) -> Result<(), IamError> {
        self.directory.serve_from(UserRecords::Legacy).await
    }

    /// Turns the IAM an earlier release stored on this node into records of
    /// version 2, and removes the records it was stored as: a single node,
    /// whose own IAM is all there is.
    ///
    /// A user that already has a record of version 2 keeps it: a conversion
    /// stopped half way left it, and it may have changed since. A stored root
    /// without S3 credentials is dropped (see [`Self::legacy_user_matters`]);
    /// one with them is converted, its keys keep working, and it still cannot
    /// log in. Every step can run again, so a conversion stopped by a crash
    /// finishes at the next start.
    ///
    /// # Errors
    ///
    /// `Storage` or `Json` when a record cannot be read or written; the
    /// records not yet converted stay as they were.
    pub async fn convert_legacy(
        &self,
        root_name: Option<&str>,
    ) -> Result<LegacyConversion, IamError> {
        let mut conversion = LegacyConversion::default();
        let local = LocalRecordStore::new(self.local.clone());
        for user in self.legacy_users().await? {
            if !Self::legacy_user_matters(&user, root_name) {
                conversion.dropped_root += 1;
            } else if local.read(&user_key(&user.id)).await?.is_some() {
                conversion.already_current += 1;
            } else {
                let sealed = self.sealed(&user).await?;
                local.write(&user_key(&user.id), &serde_json::to_vec(&sealed)?).await?;
                conversion.converted += 1;
            }
            Self::remove_legacy(&local, &user).await?;
            self.directory.refresh(&user.id).await?;
        }
        Ok(conversion)
    }

    /// Removes the record of an earlier release of `user` from this node's
    /// storage, its indexes first.
    async fn remove_legacy(local: &LocalRecordStore, user: &User) -> Result<(), IamError> {
        local.delete(&format!("{LEGACY_USERNAME_KEY_PREFIX}{}", user.username)).await?;
        if let Some(access_key) = &user.access_key {
            local.delete(&format!("{LEGACY_ACCESS_KEY_KEY_PREFIX}{access_key}")).await?;
        }
        local.delete(&format!("{LEGACY_USER_KEY_PREFIX}{}", user.id)).await
    }

    /// The user records an earlier release stored on this node, as they are
    /// stored: key and body.
    ///
    /// # Errors
    ///
    /// `Storage` when they cannot be read.
    pub async fn legacy_records(&self) -> Result<Vec<(String, Vec<u8>)>, IamError> {
        let local = LocalRecordStore::new(self.local.clone());
        let mut records = Vec::new();
        for key in list_keys(&*self.local, LEGACY_USER_KEY_PREFIX).await? {
            if let Some(data) = local.read(&key).await? {
                records.push((key, data));
            }
        }
        Ok(records)
    }

    /// The IAM this node keeps of its own, which its pool has not taken over:
    /// the users of [`Self::legacy_records`] but a stored root without S3
    /// credentials, with their fingerprint.
    ///
    /// # Errors
    ///
    /// `Storage` or `Json` when the records cannot be read.
    pub async fn own_users(&self, root_name: Option<&str>) -> Result<OwnUsers, IamError> {
        OwnUsers::from_records(self.legacy_records().await?, root_name)
    }

    /// The pool's latest decision on the IAM its nodes kept of their own, as
    /// this node holds it.
    ///
    /// # Errors
    ///
    /// `Storage` or `Json` when it cannot be read.
    pub async fn adoption_record(&self) -> Result<Option<AdoptionRecord>, IamError> {
        match LocalRecordStore::new(self.local.clone()).read(ADOPTION_KEY).await? {
            Some(data) => Ok(Some(serde_json::from_slice(&data)?)),
            None => Ok(None),
        }
    }

    /// Takes over the IAM the nodes of the pool kept of their own, as
    /// `request` decides on the report of `holdings`, the answer of every
    /// node of the pool (plan CLUSTER-IAM-REPLICATION.md, decision 8).
    ///
    /// Under the lease of IAM changes: reads the users the pool holds, writes
    /// a record of version 2 for every user taken over (see
    /// [`adoption::plan`]), and then the decision itself, with the report and
    /// the fingerprint of every node's answer. Every node that finds the
    /// decision covering what it keeps drops it; a decision stopped half way
    /// is taken again on the same report, and the users written already stay.
    /// Not refused while the gate of changes is closed: it is what opens it.
    ///
    /// # Errors
    ///
    /// - `AdoptionIncomplete` when a node of the pool did not answer
    /// - `AdoptionNotPending` when no node keeps users of its own
    /// - `AdoptionReportChanged` when `request` names another report
    /// - `AdoptionInvalid` when it names users the report does not
    /// - `Busy` or `Unavailable` as every change of IAM
    pub async fn adopt(
        &self,
        holdings: &[NodeHoldings],
        request: &AdoptionRequest,
        decided_by: &str,
        through: &str,
    ) -> Result<AdoptionRecord, IamError> {
        let report = AdoptionReport::build(holdings);
        if !report.complete {
            return Err(IamError::AdoptionIncomplete(format!(
                "no answer from {}",
                report.unanswered().join(", ")
            )));
        }
        if !report.has_own_users() {
            return Err(IamError::AdoptionNotPending);
        }
        if request.report_id != report.report_id {
            return Err(IamError::AdoptionReportChanged);
        }
        let change = self.store.begin().await?;
        let result = async {
            let mut existing = Vec::new();
            for (_, data) in change.records(USER_KEY_PREFIX).await? {
                existing.push(serde_json::from_slice::<User>(&data)?);
            }
            let plan = adoption::plan(
                holdings,
                request,
                &existing,
                change.epoch(),
                chrono::Utc::now(),
            )?;
            for user in &plan.records {
                self.save_under(&*change, user).await?;
            }
            let record = AdoptionRecord {
                report_id: report.report_id.clone(),
                mode: request.mode,
                decided_at: chrono::Utc::now(),
                decided_by: decided_by.to_string(),
                through: through.to_string(),
                nodes: report
                    .nodes
                    .iter()
                    .filter_map(|node| {
                        node.digest.clone().map(|digest| (node.node_id.clone(), digest))
                    })
                    .collect(),
                adopted: plan.adopted,
                already_adopted: plan.already_adopted,
                left_out: plan.left_out,
                report,
            };
            change.write(ADOPTION_KEY, &serde_json::to_vec(&record)?).await?;
            let entry = AuditEntry::new(
                AuditAction::IamAdopted,
                IamActor {
                    id: None,
                    name: decided_by.to_string(),
                },
                through,
                change.epoch(),
                record.decided_at,
            )
            .detail("report_id", record.report_id.clone())
            .detail("mode", format!("{:?}", record.mode).to_lowercase())
            .detail("adopted", record.adopted.len().to_string())
            .detail("left_out", record.left_out.len().to_string());
            self.audit_under(&*change, &entry).await?;
            Ok(record)
        }
        .await;
        change.finish().await;
        result
    }

    /// Seals again, with the current key, the secret of every user whose
    /// record keeps it under another key or in clear text, as an earlier
    /// release did: the pass that lets an old key be removed (decision 7).
    /// Asks no other node unless this node's own copy holds such a record;
    /// then rewrites them under the lease, with one entry in the audit log.
    ///
    /// # Errors
    ///
    /// `AdoptionPending` while IAM changes are refused; `Busy` or
    /// `Unavailable` as every change of IAM.
    pub async fn reseal_secrets(&self) -> Result<Resealing, IamError> {
        let keys = self.secret_keys().await?;
        let current = keys.current().id().to_string();
        let held = self.directory.secrets_by_key_id().await?;
        if held.keys().all(|key_id| *key_id == current) {
            return Ok(Resealing::default());
        }
        let change = self.begin().await?;
        let result = async {
            let mut resealing = Resealing::default();
            for (_, data) in change.records(USER_KEY_PREFIX).await? {
                let mut user: User = serde_json::from_slice(&data)?;
                let Some(access_key) = user.access_key.clone() else {
                    continue;
                };
                match &user.secret_key_sealed {
                    Some(sealed) if sealed.key_id == current => continue,
                    Some(sealed) if !keys.opens(&sealed.key_id) => {
                        resealing.unreadable += 1;
                        continue;
                    }
                    Some(sealed) => {
                        user.secret_key = Some(keys.open(&user.id, &access_key, sealed)?);
                    }
                    None if user.secret_key.is_none() => continue,
                    None => {}
                }
                self.save_under(&*change, &user).await?;
                resealing.resealed += 1;
            }
            if resealing.resealed > 0 {
                let entry = self
                    .entry(AuditAction::SecretsResealed, &*change)
                    .detail("users", resealing.resealed.to_string())
                    .detail("key_id", current.clone());
                self.audit_under(&*change, &entry).await?;
            }
            Ok(resealing)
        }
        .await;
        change.finish().await;
        result
    }

    /// Up to `max` entries of the audit log after the entry `after`, as this
    /// node's own copy holds them, in the order the changes were made.
    ///
    /// # Errors
    ///
    /// `Storage` or `Json` when the entries cannot be read.
    pub async fn audit_page(&self, after: Option<&str>, max: usize) -> Result<AuditPage, IamError> {
        let listed = match after {
            None => self.local.list_objects(SYSTEM_BUCKET, AUDIT_KEY_PREFIX, max + 1).await?,
            Some(id) => {
                self.local
                    .list_objects_after(SYSTEM_BUCKET, AUDIT_KEY_PREFIX, &audit_key(id), max + 1)
                    .await?
            }
        };
        let keys: Vec<String> = listed
            .into_iter()
            .map(|(key, _)| key)
            .filter(|key| key.starts_with(AUDIT_KEY_PREFIX))
            .collect();
        let more = keys.len() > max;
        let local = LocalRecordStore::new(self.local.clone());
        let mut entries = Vec::new();
        for key in keys.iter().take(max) {
            if let Some(data) = local.read(key).await? {
                entries.push(serde_json::from_slice::<AuditEntry>(&data)?);
            }
        }
        let next_after = if more {
            keys.get(max - 1)
                .and_then(|key| key.strip_prefix(AUDIT_KEY_PREFIX))
                .map(str::to_string)
        } else {
            None
        };
        Ok(AuditPage {
            entries,
            next_after,
        })
    }

    /// Deletes, under the lease, the entries of the audit log made before
    /// `before`, as this node's own copy holds them, and records that with
    /// one entry. Asks no other node when there are none. The number
    /// deleted.
    ///
    /// # Errors
    ///
    /// `Storage` when the log cannot be listed; `Busy` or `Unavailable` as
    /// every change of IAM.
    pub async fn purge_audit(
        &self,
        before: chrono::DateTime<chrono::Utc>,
    ) -> Result<usize, IamError> {
        let horizon = before.timestamp_micros();
        let old: Vec<String> = list_keys(&*self.local, AUDIT_KEY_PREFIX)
            .await?
            .into_iter()
            .filter(|key| entry_micros(key).is_some_and(|micros| micros < horizon))
            .collect();
        if old.is_empty() {
            return Ok(0);
        }
        let change = self.store.begin().await?;
        let result = async {
            for key in &old {
                change.delete(key).await?;
            }
            let entry = self
                .entry(AuditAction::AuditPurged, &*change)
                .detail("entries", old.len().to_string())
                .detail("before", before.to_rfc3339());
            self.audit_under(&*change, &entry).await?;
            Ok(old.len())
        }
        .await;
        change.finish().await;
        result
    }

    /// Drops the IAM this node kept of its own — every record of an earlier
    /// release, a stored root among them — and serves the records of
    /// version 2 from now on: the pool took it over, or there was nothing to
    /// take over. The number of user records dropped. Every step can run
    /// again.
    ///
    /// # Errors
    ///
    /// `Storage` or `Json` when a record cannot be read or removed.
    pub async fn drop_legacy(&self) -> Result<usize, IamError> {
        let local = LocalRecordStore::new(self.local.clone());
        let users = self.legacy_users().await?;
        for user in &users {
            Self::remove_legacy(&local, user).await?;
        }
        if self.directory.records() == UserRecords::Legacy {
            self.directory.serve_from(UserRecords::Current).await?;
        }
        Ok(users.len())
    }

    /// Records whether a single node holds users of version 2 — its own IAM,
    /// should it ever start as a cluster node. Called at every start of a
    /// single node.
    ///
    /// # Errors
    ///
    /// `Storage` when the users cannot be listed or the mark written.
    pub async fn mark_single_node_users(&self) -> Result<bool, IamError> {
        let local = LocalRecordStore::new(self.local.clone());
        let holds = !list_keys(&*self.local, USER_KEY_PREFIX).await?.is_empty();
        if holds {
            local.write(SINGLE_NODE_USERS_KEY, b"1").await?;
        } else {
            local.delete(SINGLE_NODE_USERS_KEY).await?;
        }
        Ok(holds)
    }

    /// Turns the users a single node wrote as records of version 2 back into
    /// records of an earlier release, the IAM a node keeps of its own: this
    /// node starts as a cluster node, and its pool takes them over only as
    /// the operator decides, as any node's own (plan CLUSTER-IAM-REPLICATION.md,
    /// decision 8.6). Nothing happens unless the node was a single node with
    /// users since it last started in cluster mode. Called before the node
    /// joins its pool, so the records are no peer's yet; they are removed
    /// from this node's storage alone and leave no tombstone. The number of
    /// users turned back. Every step can run again.
    ///
    /// # Errors
    ///
    /// `Storage` or `Json` when a record cannot be read, written or removed.
    pub async fn take_back_single_node_users(&self) -> Result<usize, IamError> {
        let local = LocalRecordStore::new(self.local.clone());
        if local.read(SINGLE_NODE_USERS_KEY).await?.is_none() {
            return Ok(0);
        }
        let mut moved = 0;
        for key in list_keys(&*self.local, USER_KEY_PREFIX).await? {
            let Some(data) = local.read(&key).await? else {
                continue;
            };
            let user: User = serde_json::from_slice(&data)?;
            local.write(&format!("{LEGACY_USER_KEY_PREFIX}{}", user.id), &data).await?;
            local
                .write(
                    &format!("{LEGACY_USERNAME_KEY_PREFIX}{}", user.username),
                    user.id.as_bytes(),
                )
                .await?;
            if let Some(access_key) = &user.access_key {
                local
                    .write(
                        &format!("{LEGACY_ACCESS_KEY_KEY_PREFIX}{access_key}"),
                        user.id.as_bytes(),
                    )
                    .await?;
            }
            local.delete(&key).await?;
            moved += 1;
        }
        local.delete(SINGLE_NODE_USERS_KEY).await?;
        Ok(moved)
    }
}

/// The time, in microseconds, in the id of the audit entry at `key`.
fn entry_micros(key: &str) -> Option<i64> {
    key.strip_prefix(AUDIT_KEY_PREFIX)?.split('_').nth(1)?.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    use s4_core::storage::BitcaskStorageEngine;

    async fn storage() -> (Arc<dyn StorageEngine>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let engine = BitcaskStorageEngine::new(
            dir.path().join("volumes"),
            dir.path().join("metadata"),
            10 * 1024 * 1024,
            4096,
            false,
        )
        .await
        .unwrap();
        (Arc::new(engine), dir)
    }

    fn legacy_user(id: &str, username: &str, access_key: Option<&str>) -> User {
        User {
            id: id.to_string(),
            username: username.to_string(),
            password_hash: PasswordHasher::new().hash_password("Old-Pass-123").unwrap(),
            role: Role::Writer,
            access_key: access_key.map(str::to_string),
            secret_key: access_key.map(|_| "legacy-secret".to_string()),
            secret_key_sealed: None,
            secret_key_hash: access_key
                .map(|_| PasswordHasher::new().hash_password("legacy-secret").unwrap()),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            is_active: true,
            credential_version: 0,
            created_epoch: 0,
        }
    }

    /// Writes `user` as an earlier release did: its record and both indexes.
    async fn store_legacy(storage: &Arc<dyn StorageEngine>, user: &User) {
        let put = |key: String, data: Vec<u8>| {
            let storage = storage.clone();
            async move {
                storage
                    .put_object(
                        SYSTEM_BUCKET,
                        &key,
                        &data,
                        "application/json",
                        &HashMap::new(),
                    )
                    .await
                    .unwrap();
            }
        };
        put(
            format!("{LEGACY_USER_KEY_PREFIX}{}", user.id),
            serde_json::to_vec(user).unwrap(),
        )
        .await;
        put(
            format!("{LEGACY_USERNAME_KEY_PREFIX}{}", user.username),
            user.id.clone().into_bytes(),
        )
        .await;
        if let Some(access_key) = &user.access_key {
            put(
                format!("{LEGACY_ACCESS_KEY_KEY_PREFIX}{access_key}"),
                user.id.clone().into_bytes(),
            )
            .await;
        }
    }

    /// The IAM keys of `storage` that hold users: the audit log and the
    /// node's own key left out.
    async fn system_keys(storage: &Arc<dyn StorageEngine>) -> Vec<String> {
        storage
            .list_objects(SYSTEM_BUCKET, "__s4_iam_", 1000)
            .await
            .unwrap()
            .into_iter()
            .map(|(key, _)| key)
            .filter(|key| {
                !key.starts_with(AUDIT_KEY_PREFIX)
                    && key != crate::iam::secrets::NODE_SECRET_KEY_KEY
            })
            .collect()
    }

    /// The bytes of the record at `key`.
    async fn raw(storage: &Arc<dyn StorageEngine>, key: &str) -> Vec<u8> {
        storage.get_object(SYSTEM_BUCKET, key).await.unwrap().0
    }

    const KEY_A: &str = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
    const KEY_B: &str = "1f1e1d1c1b1a191817161514131211100f0e0d0c0b0a09080706050403020100";

    fn configured(current: &str, previous: Option<&str>) -> SecretKeySettings {
        use crate::iam::secrets::{KeyOrigin, SecretKey};
        SecretKeySettings {
            key: Some(SecretKey::from_hex(current, KeyOrigin::Configured).unwrap()),
            previous: previous.map(|hex| SecretKey::from_hex(hex, KeyOrigin::Previous).unwrap()),
            jwt_secret: None,
        }
    }

    /// The secret of S3 credentials is written sealed, never in clear text,
    /// and the next start opens it with the same key: the node's own key
    /// without a configured one, kept in its storage.
    #[tokio::test]
    async fn a_secret_is_stored_sealed_and_opens_at_the_next_start() {
        let (storage, _dir) = storage().await;
        let iam = IamStorage::new(storage.clone());
        let user = iam
            .create_user("sam".into(), "Sam-Pass-123".into(), Role::Writer)
            .await
            .unwrap();
        let keys = iam.generate_s3_credentials(&user.id).await.unwrap();

        let record = raw(&storage, &user_key(&user.id)).await;
        let text = String::from_utf8_lossy(&record);
        assert!(!text.contains(&keys.secret_key), "{text}");
        let stored: User = serde_json::from_slice(&record).unwrap();
        assert!(stored.secret_key.is_none());
        let sealed = stored.secret_key_sealed.unwrap();
        assert_eq!(
            sealed.key_id,
            iam.secret_keys().await.unwrap().current().id()
        );
        assert_eq!(iam.secret_keys().await.unwrap().shared_key_id(), None);

        let restarted = IamStorage::new(storage.clone());
        let found = restarted
            .get_user_by_access_key_with_secret(&keys.access_key)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(found.secret_key.as_deref(), Some(keys.secret_key.as_str()));
        assert!(!format!("{found:?}").contains(&keys.secret_key));
    }

    /// A record sealed with a key this node does not have is not checked
    /// here — `503`, for another node to check — rather than refused as a
    /// wrong secret; a key that comes later opens it.
    #[tokio::test]
    async fn a_secret_sealed_with_an_unknown_key_is_not_served() {
        let (storage, _dir) = storage().await;
        let iam = IamStorage::new(storage.clone());
        iam.use_secret_keys(configured(KEY_A, None)).await.unwrap();
        let user = iam
            .create_user("tia".into(), "Tia-Pass-1234".into(), Role::Writer)
            .await
            .unwrap();
        let keys = iam.generate_s3_credentials(&user.id).await.unwrap();

        let other = IamStorage::new(storage.clone());
        other.use_secret_keys(configured(KEY_B, None)).await.unwrap();
        assert!(matches!(
            other.get_user_by_access_key_with_secret(&keys.access_key).await,
            Err(IamError::NotServing(NotServing::SecretKeyUnknown))
        ));
        other.use_secret_keys(configured(KEY_B, Some(KEY_A))).await.unwrap();
        assert!(other
            .get_user_by_access_key_with_secret(&keys.access_key)
            .await
            .unwrap()
            .is_some());
    }

    /// A record leaves the node with its secret sealed with the current key:
    /// one in clear text is sealed, one under the node's own key sealed again,
    /// one under the current key and anything else sent as it is.
    #[tokio::test]
    async fn a_record_leaves_the_node_sealed_with_the_current_key() {
        let (storage, _dir) = storage().await;
        // A single node without configured keys seals with its own.
        let single = IamStorage::new(storage.clone());
        let user = single
            .create_user("yan".into(), "Yan-Pass-12345".into(), Role::Writer)
            .await
            .unwrap();
        let keys = single.generate_s3_credentials(&user.id).await.unwrap();
        let own_key = single.secret_keys().await.unwrap().current().id().to_string();
        let record = raw(&storage, &user_key(&user.id)).await;

        let pooled = IamStorage::new(storage.clone());
        let ring = pooled.use_secret_keys(configured(KEY_A, None)).await.unwrap();
        assert!(ring.opens(&own_key));
        let sent: User =
            serde_json::from_slice(&pooled.directory().sealed_record(&record).await.unwrap())
                .unwrap();
        let sealed = sent.secret_key_sealed.clone().unwrap();
        assert_eq!(sealed.key_id, ring.current().id());
        assert!(sent.secret_key.is_none());
        // The pool, which has only the configured key, opens it.
        let only_a = configured(KEY_A, None).keyring(None).unwrap();
        assert_eq!(
            only_a.open(&user.id, &keys.access_key, &sealed).unwrap(),
            keys.secret_key
        );

        let current = serde_json::to_vec(&sent).unwrap();
        assert_eq!(
            pooled.directory().sealed_record(&current).await.unwrap(),
            current
        );
        let clear = serde_json::to_vec(&legacy_user("z-1", "zed", Some("S4AKZED"))).unwrap();
        let sent: User =
            serde_json::from_slice(&pooled.directory().sealed_record(&clear).await.unwrap())
                .unwrap();
        assert!(sent.secret_key.is_none());
        assert_eq!(sent.secret_key_sealed.unwrap().key_id, ring.current().id());
        assert_eq!(
            pooled.directory().sealed_record(b"not a user").await.unwrap(),
            b"not a user".to_vec()
        );
    }

    /// The pass seals again with the current key every secret kept under the
    /// previous key or in clear text, writes one entry to the audit log,
    /// leaves what it cannot open, and does nothing the next time.
    #[tokio::test]
    async fn the_pass_seals_every_secret_with_the_current_key() {
        let (storage, _dir) = storage().await;
        let old = IamStorage::new(storage.clone());
        old.use_secret_keys(configured(KEY_B, None)).await.unwrap();
        let uma = old
            .create_user("uma".into(), "Uma-Pass-1234".into(), Role::Writer)
            .await
            .unwrap();
        let uma_keys = old.generate_s3_credentials(&uma.id).await.unwrap();
        // A user an earlier release wrote, its secret in clear text.
        let clear = legacy_user("v-1", "vic", Some("S4AKVIC"));
        storage
            .put_object(
                SYSTEM_BUCKET,
                &user_key(&clear.id),
                &serde_json::to_vec(&clear).unwrap(),
                "application/json",
                &HashMap::new(),
            )
            .await
            .unwrap();

        let iam = IamStorage::new(storage.clone());
        let keys = iam.use_secret_keys(configured(KEY_A, Some(KEY_B))).await.unwrap();
        let held = iam.directory().secrets_by_key_id().await.unwrap();
        assert_eq!(held.get("clear"), Some(&1));
        assert_eq!(held.len(), 2);

        let done = iam.reseal_secrets().await.unwrap();
        assert_eq!(
            done,
            Resealing {
                resealed: 2,
                unreadable: 0
            }
        );
        let held = iam.directory().secrets_by_key_id().await.unwrap();
        assert_eq!(
            held.into_iter().collect::<Vec<_>>(),
            vec![(keys.current().id().to_string(), 2)]
        );
        for (id, secret) in [
            (&uma.id, uma_keys.secret_key.as_str()),
            (&clear.id, "legacy-secret"),
        ] {
            let text = String::from_utf8(raw(&storage, &user_key(id)).await).unwrap();
            assert!(!text.contains(secret), "{text}");
        }
        assert!(iam.verify_s3_credentials("S4AKVIC", "legacy-secret").await.unwrap().is_some());
        let found = iam
            .get_user_by_access_key_with_secret(&uma_keys.access_key)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            found.secret_key.as_deref(),
            Some(uma_keys.secret_key.as_str())
        );
        assert_eq!(iam.reseal_secrets().await.unwrap(), Resealing::default());
        let log = iam.audit_page(None, 100).await.unwrap();
        let resealed: Vec<_> = log
            .entries
            .iter()
            .filter(|entry| entry.action == AuditAction::SecretsResealed)
            .collect();
        assert_eq!(resealed.len(), 1);
        assert_eq!(resealed[0].details["users"], "2");

        // Without the previous key, a secret under it stays as it is.
        let lost = IamStorage::new(storage.clone());
        lost.use_secret_keys(configured(KEY_B, None)).await.unwrap();
        assert_eq!(
            lost.reseal_secrets().await.unwrap(),
            Resealing {
                resealed: 0,
                unreadable: 2
            }
        );
    }

    /// Every change writes one entry of the audit log, by the actor through
    /// the node, in the order made, without a password or a secret; the log
    /// reads a page at a time.
    #[tokio::test]
    async fn every_change_is_in_the_audit_log() {
        let (storage, _dir) = storage().await;
        let iam = IamStorage::new(storage.clone()).with_node_name("node-7").acting_as(IamActor {
            id: Some("root-id".into()),
            name: "root".into(),
        });
        let user = iam
            .create_user("wes".into(), "Wes-Pass-1234".into(), Role::Writer)
            .await
            .unwrap();
        let keys = iam.generate_s3_credentials(&user.id).await.unwrap();
        iam.update_user(
            &user.id,
            Some("New-Pass-1234".into()),
            Some(Role::Reader),
            None,
        )
        .await
        .unwrap();
        iam.revoke_sessions(&user.id).await.unwrap();
        iam.delete_s3_credentials(&user.id).await.unwrap();
        iam.delete_user(&user.id).await.unwrap();

        let log = iam.audit_page(None, 100).await.unwrap();
        let actions: Vec<AuditAction> = log.entries.iter().map(|entry| entry.action).collect();
        assert_eq!(
            actions,
            vec![
                AuditAction::UserCreated,
                AuditAction::CredentialsIssued,
                AuditAction::UserUpdated,
                AuditAction::SessionsRevoked,
                AuditAction::CredentialsDeleted,
                AuditAction::UserDeleted,
            ]
        );
        assert!(log.entries.iter().all(|entry| entry.through == "node-7"
            && entry.actor.name == "root"
            && entry.target.as_deref() == Some("wes")));
        assert_eq!(log.entries[1].details["access_key"], keys.access_key);
        assert_eq!(log.entries[2].details["role"], "Writer -> Reader");
        assert_eq!(log.entries[2].details["password"], "changed");
        assert_eq!(log.next_after, None);
        let json = serde_json::to_string(&log).unwrap();
        assert!(!json.contains(&keys.secret_key));
        assert!(!json.contains("New-Pass-1234"));

        let first = iam.audit_page(None, 4).await.unwrap();
        assert_eq!(first.entries.len(), 4);
        let rest = iam.audit_page(first.next_after.as_deref(), 4).await.unwrap();
        assert_eq!(rest.entries.len(), 2);
        assert_eq!(rest.next_after, None);
        assert_eq!(rest.entries[0], log.entries[4]);
    }

    /// Entries older than the retention go, the purge itself is one entry,
    /// and nothing happens when there is nothing to delete.
    #[tokio::test]
    async fn old_entries_of_the_audit_log_are_purged() {
        let (storage, _dir) = storage().await;
        let iam = IamStorage::new(storage.clone());
        let old = AuditEntry::new(
            AuditAction::UserCreated,
            IamActor::system(),
            "n",
            0,
            chrono::Utc::now() - chrono::Duration::days(100),
        );
        LocalRecordStore::new(storage.clone())
            .write(&old.key(), &serde_json::to_vec(&old).unwrap())
            .await
            .unwrap();
        iam.create_user("xan".into(), "Xan-Pass-1234".into(), Role::Reader)
            .await
            .unwrap();

        let horizon = chrono::Utc::now() - chrono::Duration::days(90);
        assert_eq!(iam.purge_audit(horizon).await.unwrap(), 1);
        let log = iam.audit_page(None, 100).await.unwrap();
        let actions: Vec<AuditAction> = log.entries.iter().map(|entry| entry.action).collect();
        assert_eq!(
            actions,
            vec![AuditAction::UserCreated, AuditAction::AuditPurged]
        );
        assert_eq!(iam.purge_audit(horizon).await.unwrap(), 0);
    }

    /// Users read a page at a time, in the order of their ids.
    #[tokio::test]
    async fn users_are_listed_a_page_at_a_time() {
        let (storage, _dir) = storage().await;
        let iam = IamStorage::new(storage);
        for n in 0..5 {
            iam.create_user(format!("page_{n}"), "Page-Pass-123".into(), Role::Reader)
                .await
                .unwrap();
        }
        let all = iam.list_users().await.unwrap();
        let (first, more) = iam.list_users_after(None, 2).await.unwrap();
        assert!(more);
        let (second, more) = iam.list_users_after(Some(&first[1].id), 2).await.unwrap();
        assert!(more);
        let (third, more) = iam.list_users_after(Some(&second[1].id), 2).await.unwrap();
        assert!(!more);
        let paged: Vec<String> =
            first.iter().chain(&second).chain(&third).map(|user| user.id.clone()).collect();
        let ids: Vec<String> = all.iter().map(|user| user.id.clone()).collect();
        assert_eq!(paged, ids);
    }

    #[test]
    fn test_username_validation() {
        // Valid usernames
        assert!(is_valid_username("user123"));
        assert!(is_valid_username("test_user"));
        assert!(is_valid_username("abc"));

        // Invalid usernames
        assert!(!is_valid_username("ab")); // Too short
        assert!(!is_valid_username("a".repeat(33).as_str())); // Too long
        assert!(!is_valid_username("user-name")); // Dash not allowed
        assert!(!is_valid_username("user@example")); // @ not allowed
    }

    fn is_valid_username(username: &str) -> bool {
        if username.len() < 3 || username.len() > 32 {
            return false;
        }
        username.chars().all(|c| c.is_alphanumeric() || c == '_')
    }

    /// A user, its keys and its deletion are each one record of version 2:
    /// nothing else is written, and the keys of an earlier release never are.
    #[tokio::test]
    async fn a_user_is_one_record_through_its_whole_life() {
        let (storage, _dir) = storage().await;
        let iam = IamStorage::new(storage.clone());

        let user = iam
            .create_user("alice".into(), "Alice-Pass-1".into(), Role::Writer)
            .await
            .unwrap();
        assert_eq!(system_keys(&storage).await, vec![user_key(&user.id)]);

        let first = iam.generate_s3_credentials(&user.id).await.unwrap();
        let second = iam.generate_s3_credentials(&user.id).await.unwrap();
        assert_eq!(system_keys(&storage).await, vec![user_key(&user.id)]);
        assert!(iam.get_user_by_access_key(&first.access_key).await.unwrap().is_none());
        let by_key = iam.get_user_by_access_key(&second.access_key).await.unwrap().unwrap();
        assert_eq!(by_key.id, user.id);
        assert!(iam
            .verify_s3_credentials(&second.access_key, &second.secret_key)
            .await
            .unwrap()
            .is_some());

        iam.delete_user(&user.id).await.unwrap();
        assert!(system_keys(&storage).await.is_empty());
        assert!(iam.get_user(&user.id).await.unwrap().is_none());
        assert!(iam.get_user_by_username("alice").await.unwrap().is_none());
        assert!(iam.get_user_by_access_key(&second.access_key).await.unwrap().is_none());
        assert!(matches!(
            iam.delete_user(&user.id).await,
            Err(IamError::UserNotFound)
        ));
    }

    /// A second user of one name is refused, and a changed name index
    /// follows the record.
    #[tokio::test]
    async fn a_name_is_taken_once() {
        let (storage, _dir) = storage().await;
        let iam = IamStorage::new(storage);
        iam.create_user("bob".into(), "Bob-Pass-123".into(), Role::Reader)
            .await
            .unwrap();

        assert!(matches!(
            iam.create_user("bob".into(), "Other-Pass-1".into(), Role::SuperUser).await,
            Err(IamError::UserAlreadyExists)
        ));
        assert!(iam.verify_password("bob", "Bob-Pass-123").await.unwrap());
        assert!(!iam.verify_password("bob", "Other-Pass-1").await.unwrap());
    }

    /// Another `IamStorage` over the same storage — the next start — finds
    /// every user, by id, name and access key.
    #[tokio::test]
    async fn the_directory_is_built_from_the_records_at_the_next_start() {
        let (storage, _dir) = storage().await;
        let iam = IamStorage::new(storage.clone());
        let carol = iam
            .create_user("carol".into(), "Carol-Pass-1".into(), Role::Writer)
            .await
            .unwrap();
        let keys = iam.generate_s3_credentials(&carol.id).await.unwrap();
        iam.update_user(&carol.id, None, Some(Role::Reader), None).await.unwrap();

        let restarted = IamStorage::new(storage);
        let by_name = restarted.get_user_by_username("carol").await.unwrap().unwrap();
        assert_eq!(by_name.id, carol.id);
        assert_eq!(by_name.role, Role::Reader);
        assert_eq!(by_name.credential_version, 1);
        let by_key = restarted.get_user_by_access_key(&keys.access_key).await.unwrap().unwrap();
        assert_eq!(by_key.id, carol.id);
        assert_eq!(restarted.list_users().await.unwrap().len(), 1);
    }

    /// A record written to the node's storage by someone else — a peer, a
    /// hint, anti-entropy — is in the directory once it is observed, and so is
    /// its deletion.
    #[tokio::test]
    async fn a_record_written_by_another_node_is_seen_once_observed() {
        let (storage, _dir) = storage().await;
        let iam = IamStorage::new(storage.clone());
        assert!(iam.list_users().await.unwrap().is_empty());

        let dave = legacy_user("d-1", "dave", Some("S4AKDAVE"));
        let key = user_key(&dave.id);
        storage
            .put_object(
                SYSTEM_BUCKET,
                &key,
                &serde_json::to_vec(&dave).unwrap(),
                "application/json",
                &HashMap::new(),
            )
            .await
            .unwrap();
        assert!(iam.get_user_by_username("dave").await.unwrap().is_none());

        iam.directory().observe(SYSTEM_BUCKET, &key).await.unwrap();
        assert_eq!(
            iam.get_user_by_username("dave").await.unwrap().unwrap().id,
            "d-1"
        );
        assert_eq!(
            iam.get_user_by_access_key("S4AKDAVE").await.unwrap().unwrap().id,
            "d-1"
        );

        storage.delete_object(SYSTEM_BUCKET, &key).await.unwrap();
        iam.directory().observe(SYSTEM_BUCKET, &key).await.unwrap();
        assert!(iam.get_user("d-1").await.unwrap().is_none());
        assert!(iam.get_user_by_access_key("S4AKDAVE").await.unwrap().is_none());

        // Other keys are not the directory's.
        iam.directory().observe("photos", &key).await.unwrap();
        iam.directory().observe(SYSTEM_BUCKET, ROOT_STATE_KEY).await.unwrap();
    }

    /// Two records of one name: the one created first answers for it, on
    /// every node alike, and the other takes over once it is gone.
    #[tokio::test]
    async fn of_two_records_of_one_name_the_first_created_answers() {
        let (storage, _dir) = storage().await;
        let iam = IamStorage::new(storage.clone());
        let mut early = legacy_user("z-early", "erin", None);
        early.created_at = chrono::Utc::now() - chrono::Duration::seconds(60);
        let late = legacy_user("a-late", "erin", None);
        for user in [&late, &early] {
            storage
                .put_object(
                    SYSTEM_BUCKET,
                    &user_key(&user.id),
                    &serde_json::to_vec(user).unwrap(),
                    "application/json",
                    &HashMap::new(),
                )
                .await
                .unwrap();
        }

        assert_eq!(
            iam.get_user_by_username("erin").await.unwrap().unwrap().id,
            "z-early"
        );
        storage.delete_object(SYSTEM_BUCKET, &user_key("z-early")).await.unwrap();
        iam.directory().observe(SYSTEM_BUCKET, &user_key("z-early")).await.unwrap();
        assert_eq!(
            iam.get_user_by_username("erin").await.unwrap().unwrap().id,
            "a-late"
        );
    }

    /// Creations of one name at once on a single node take turns: one
    /// succeeds, the others find the name taken.
    #[tokio::test]
    async fn one_name_created_at_once_on_a_single_node_is_one_user() {
        let (storage, _dir) = storage().await;
        let iam = IamStorage::new(storage);
        let attempt = |n: usize| {
            let iam = iam.clone();
            async move { iam.create_user("zoe".into(), format!("Zoe-Pass-{n:04}"), Role::Writer).await }
        };
        let (a, b, c) = tokio::join!(attempt(1), attempt(2), attempt(3));
        let results = [a, b, c];
        assert_eq!(
            results.iter().filter(|r| r.is_ok()).count(),
            1,
            "{results:?}"
        );
        assert!(results
            .iter()
            .filter(|r| r.is_err())
            .all(|r| matches!(r, Err(IamError::UserAlreadyExists))));
        assert_eq!(iam.list_users().await.unwrap().len(), 1);
    }

    /// Of two records of one name, the one created under the later epoch of
    /// the lease answers for it, though created later: the other could only
    /// come from a holder that wrote after its lease passed on.
    #[tokio::test]
    async fn of_two_records_of_one_name_the_later_epoch_answers() {
        let (storage, _dir) = storage().await;
        let iam = IamStorage::new(storage.clone());
        let mut stale = legacy_user("s-1", "uma", None);
        stale.created_epoch = 4;
        stale.created_at = chrono::Utc::now() - chrono::Duration::seconds(60);
        let mut confirmed = legacy_user("c-1", "uma", None);
        confirmed.created_epoch = 5;
        for user in [&stale, &confirmed] {
            storage
                .put_object(
                    SYSTEM_BUCKET,
                    &user_key(&user.id),
                    &serde_json::to_vec(user).unwrap(),
                    "application/json",
                    &HashMap::new(),
                )
                .await
                .unwrap();
        }
        assert_eq!(
            iam.get_user_by_username("uma").await.unwrap().unwrap().id,
            "c-1"
        );
    }

    /// A single node turns the IAM of an earlier release into records of
    /// version 2: every user keeps its id, password and keys, the old records
    /// and indexes go, a stored root without keys is dropped, and a second run
    /// finds nothing left to do.
    #[tokio::test]
    async fn a_single_node_converts_the_iam_of_an_earlier_release() {
        let (storage, _dir) = storage().await;
        let frank = legacy_user("f-1", "frank", Some("S4AKFRANK"));
        let grace = legacy_user("g-1", "grace", None);
        let stored_root = legacy_user("r-1", "root", None);
        let keyed_root = legacy_user("r-2", "admin", Some("S4AKADMIN"));
        for user in [&frank, &grace, &stored_root, &keyed_root] {
            store_legacy(&storage, user).await;
        }
        let iam = IamStorage::new(storage.clone());

        let conversion = iam.convert_legacy(Some("root")).await.unwrap();
        assert_eq!(
            conversion,
            LegacyConversion {
                converted: 3,
                already_current: 0,
                dropped_root: 1,
            }
        );
        let mut keys = system_keys(&storage).await;
        keys.sort();
        let mut expected: Vec<String> =
            ["f-1", "g-1", "r-2"].iter().map(|id| user_key(id)).collect();
        expected.sort();
        assert_eq!(keys, expected);
        assert!(iam.verify_password("frank", "Old-Pass-123").await.unwrap());
        assert_eq!(
            iam.get_user_by_access_key("S4AKFRANK").await.unwrap().unwrap().id,
            "f-1"
        );
        assert!(iam.verify_s3_credentials("S4AKFRANK", "legacy-secret").await.unwrap().is_some());
        assert!(iam.get_user("r-1").await.unwrap().is_none());

        assert_eq!(
            iam.convert_legacy(Some("root")).await.unwrap(),
            LegacyConversion::default()
        );
    }

    /// A conversion stopped after writing a user's record of version 2 keeps
    /// that record, which may have changed since, and still removes the old
    /// one.
    #[tokio::test]
    async fn a_conversion_stopped_half_way_keeps_the_record_it_wrote() {
        let (storage, _dir) = storage().await;
        let henry = legacy_user("h-1", "henry", None);
        store_legacy(&storage, &henry).await;
        let iam = IamStorage::new(storage.clone());
        let mut changed = henry.clone();
        changed.role = Role::Reader;
        storage
            .put_object(
                SYSTEM_BUCKET,
                &user_key("h-1"),
                &serde_json::to_vec(&changed).unwrap(),
                "application/json",
                &HashMap::new(),
            )
            .await
            .unwrap();

        let conversion = iam.convert_legacy(None).await.unwrap();
        assert_eq!(conversion.already_current, 1);
        assert_eq!(system_keys(&storage).await, vec![user_key("h-1")]);
        assert_eq!(
            iam.get_user("h-1").await.unwrap().unwrap().role,
            Role::Reader
        );
    }

    /// A cluster node that serves the IAM of an earlier release finds its
    /// users as before and refuses every change of them, while root's state
    /// still changes.
    #[tokio::test]
    async fn a_node_serving_the_iam_of_an_earlier_release_refuses_changes() {
        let (storage, _dir) = storage().await;
        let ivy = legacy_user("i-1", "ivy", Some("S4AKIVY"));
        store_legacy(&storage, &ivy).await;
        let iam = IamStorage::new(storage);

        iam.serve_legacy().await.unwrap();
        assert!(iam.verify_password("ivy", "Old-Pass-123").await.unwrap());
        assert_eq!(
            iam.get_user_by_access_key("S4AKIVY").await.unwrap().unwrap().id,
            "i-1"
        );
        assert!(matches!(
            iam.create_user("jack".into(), "Jack-Pass-12".into(), Role::Reader).await,
            Err(IamError::AdoptionPending)
        ));
        assert!(matches!(
            iam.update_user("i-1", None, Some(Role::Reader), None).await,
            Err(IamError::AdoptionPending)
        ));
        assert!(matches!(
            iam.delete_user("i-1").await,
            Err(IamError::AdoptionPending)
        ));
        let state = RootState {
            credential_version: 4,
            fingerprint: None,
        };
        let wanted = state.clone();
        iam.change_root_state(("root-id", "root"), "test", move |_| wanted)
            .await
            .unwrap();
        assert_eq!(iam.root_state().await.unwrap(), state);
    }

    /// A gate that never opens: a cluster node whose copy of IAM no
    /// majority has agreed with.
    struct Closed;

    impl CredentialGate for Closed {
        fn admit(&self) -> Result<(), crate::iam::NotServing> {
            Err(crate::iam::NotServing::Stale)
        }
    }

    /// Behind a closed gate a user's S3 credentials are not checked at all —
    /// neither found nor refused — while a lookup for the Admin API still
    /// reads the copy; the IAM an earlier release left on the node is its own
    /// and is served as before.
    #[tokio::test]
    async fn a_closed_gate_holds_the_credentials_of_the_users_of_version_2() {
        let (current, _dir) = storage().await;
        let iam = IamStorage::new(current);
        let user = iam
            .create_user("gail".into(), "Gail-Pass-123".into(), Role::Writer)
            .await
            .unwrap();
        let keys = iam.generate_s3_credentials(&user.id).await.unwrap();
        let gated = iam.with_credential_gate(Arc::new(Closed));
        assert!(matches!(
            gated.get_user_by_access_key_with_secret(&keys.access_key).await,
            Err(IamError::NotServing(crate::iam::NotServing::Stale))
        ));
        assert!(matches!(
            gated.verify_s3_credentials(&keys.access_key, &keys.secret_key).await,
            Err(IamError::NotServing(_))
        ));
        assert!(matches!(
            gated.get_user_by_access_key_with_secret("nobody").await,
            Err(IamError::NotServing(_))
        ));
        assert_eq!(gated.get_user(&user.id).await.unwrap().unwrap().id, user.id);
        assert!(iam
            .get_user_by_access_key_with_secret(&keys.access_key)
            .await
            .unwrap()
            .is_some());

        let (legacy_storage, _legacy_dir) = storage().await;
        store_legacy(&legacy_storage, &legacy_user("i-1", "ivy", Some("S4AKIVY"))).await;
        let legacy = IamStorage::new(legacy_storage).with_credential_gate(Arc::new(Closed));
        legacy.serve_legacy().await.unwrap();
        assert!(legacy.admit_credentials().is_ok());
        assert!(legacy.get_user_by_access_key_with_secret("S4AKIVY").await.unwrap().is_some());
    }

    /// A store of changes that must not be reached: the only way IAM reaches
    /// other nodes.
    struct Unreachable;

    #[async_trait::async_trait]
    impl IamRecordStore for Unreachable {
        async fn begin(&self) -> Result<Box<dyn IamChange>, IamError> {
            panic!("a check of credentials reached the store of changes");
        }
    }

    /// Checking a password, a token's account and an S3 key reads this
    /// node's own copy and nothing else: a cluster node's store of changes,
    /// which asks the pool, is never reached (plan CLUSTER-IAM-REPLICATION.md,
    /// decision 4).
    #[tokio::test]
    async fn checking_credentials_never_reaches_the_store_of_changes() {
        let (current, _dir) = storage().await;
        let iam = IamStorage::new(current);
        let user = iam
            .create_user("hana".into(), "Hana-Pass-123".into(), Role::Writer)
            .await
            .unwrap();
        let keys = iam.generate_s3_credentials(&user.id).await.unwrap();

        let checking = iam.with_record_store(Arc::new(Unreachable));
        assert!(checking.verify_password("hana", "Hana-Pass-123").await.unwrap());
        assert!(!checking.verify_password("hana", "wrong").await.unwrap());
        assert_eq!(
            checking.get_user(&user.id).await.unwrap().unwrap().id,
            user.id
        );
        assert!(checking
            .get_user_by_access_key_with_secret(&keys.access_key)
            .await
            .unwrap()
            .is_some());
        assert!(checking
            .verify_s3_credentials(&keys.access_key, &keys.secret_key)
            .await
            .unwrap()
            .is_some());
        assert!(checking.root_state().await.is_ok());
    }

    /// A gate of changes that stays as it is set.
    struct Switch(std::sync::atomic::AtomicBool);

    impl AdoptionGate for Switch {
        fn is_open(&self) -> bool {
            self.0.load(std::sync::atomic::Ordering::Acquire)
        }
    }

    /// While the pool's gate of changes is closed, a node that keeps no IAM
    /// of its own refuses changes too, and checks credentials as before;
    /// once it opens, changes go through.
    #[tokio::test]
    async fn a_closed_gate_of_changes_refuses_every_change() {
        let (storage, _dir) = storage().await;
        let iam = IamStorage::new(storage);
        let user = iam
            .create_user("lena".into(), "Lena-Pass-123".into(), Role::Writer)
            .await
            .unwrap();
        let gate = Arc::new(Switch(std::sync::atomic::AtomicBool::new(false)));
        let gated = iam.with_adoption_gate(gate.clone());
        assert!(!gated.changes_open());
        assert!(matches!(
            gated.create_user("mona".into(), "Mona-Pass-123".into(), Role::Reader).await,
            Err(IamError::AdoptionPending)
        ));
        assert!(matches!(
            gated.update_user(&user.id, None, Some(Role::Reader), None).await,
            Err(IamError::AdoptionPending)
        ));
        assert!(matches!(
            gated.generate_s3_credentials(&user.id).await,
            Err(IamError::AdoptionPending)
        ));
        assert!(gated.verify_password("lena", "Lena-Pass-123").await.unwrap());

        gate.0.store(true, std::sync::atomic::Ordering::Release);
        assert!(gated.changes_open());
        gated.update_user(&user.id, None, Some(Role::Reader), None).await.unwrap();
    }

    /// What each of two nodes answers when asked for the IAM it keeps of its
    /// own.
    fn holdings(nodes: &[(&str, &[&User])]) -> Vec<NodeHoldings> {
        nodes
            .iter()
            .map(|(name, users)| {
                let records = users
                    .iter()
                    .map(|user| {
                        (
                            format!("{LEGACY_USER_KEY_PREFIX}{}", user.id),
                            serde_json::to_vec(user).unwrap(),
                        )
                    })
                    .collect();
                NodeHoldings {
                    node_id: format!("id-{name}"),
                    node_name: name.to_string(),
                    own: Ok(OwnUsers::from_records(records, Some("root")).unwrap()),
                }
            })
            .collect()
    }

    /// A decision on the report writes one record of version 2 per user
    /// taken over — the one name conflict renamed and disabled — and then the
    /// decision; the node that kept them drops its records once the decision
    /// covers them and finds the users through its directory.
    #[tokio::test]
    async fn a_decision_takes_the_users_over_and_the_node_drops_its_own() {
        let (storage, _dir) = storage().await;
        let mut nina = legacy_user("n-1", "nina", Some("S4AKNINA"));
        nina.created_at = chrono::Utc::now() - chrono::Duration::minutes(5);
        let nina_elsewhere = legacy_user("n-2", "nina", None);
        let omar = legacy_user("o-1", "omar", None);
        for user in [&nina, &omar] {
            store_legacy(&storage, user).await;
        }
        let iam = IamStorage::new(storage.clone());
        iam.serve_legacy().await.unwrap();
        let own = iam.own_users(Some("root")).await.unwrap();
        assert_eq!(own.users().len(), 2);
        let answers = holdings(&[("node-1", &[&nina, &omar]), ("node-2", &[&nina_elsewhere])]);
        assert_eq!(answers[0].own.as_ref().unwrap().digest(), own.digest());
        let report = AdoptionReport::build(&answers);

        let stale = AdoptionRequest {
            report_id: "another".into(),
            mode: crate::iam::AdoptionMode::Majority,
            users: Vec::new(),
        };
        assert!(matches!(
            iam.adopt(&answers, &stale, "root", "node-1").await,
            Err(IamError::AdoptionReportChanged)
        ));
        let request = AdoptionRequest {
            report_id: report.report_id.clone(),
            ..stale
        };
        let record = iam.adopt(&answers, &request, "root", "node-1").await.unwrap();
        assert_eq!(
            record.adopted.len(),
            2,
            "nina twice; omar is on one node of two"
        );
        assert_eq!(record.left_out.len(), 1);
        assert!(record.covers("id-node-1", own.digest()));
        assert_eq!(iam.adoption_record().await.unwrap(), Some(record));

        // Still serving its own until it drops them.
        assert!(iam.get_user_by_username("omar").await.unwrap().is_some());
        assert_eq!(iam.drop_legacy().await.unwrap(), 2);
        assert_eq!(iam.directory().records(), UserRecords::Current);
        assert!(iam.legacy_records().await.unwrap().is_empty());
        assert!(iam.get_user_by_username("omar").await.unwrap().is_none());
        let kept = iam.get_user_by_username("nina").await.unwrap().unwrap();
        assert_eq!(kept.id, "n-1");
        assert!(iam.verify_password("nina", "Old-Pass-123").await.unwrap());
        assert!(iam.verify_s3_credentials("S4AKNINA", "legacy-secret").await.unwrap().is_some());
        let renamed = iam.get_user_by_username("nina~n-2").await.unwrap().unwrap();
        assert!(!renamed.is_active);
        let mut keys = system_keys(&storage).await;
        keys.retain(|key| key.starts_with(USER_KEY_PREFIX));
        assert_eq!(keys.len(), 2);
        assert!(iam.changes_open());
    }

    /// A decision needs every node's answer, something to decide on and a
    /// valid choice.
    #[tokio::test]
    async fn a_decision_is_refused_without_every_answer_or_anything_to_decide() {
        let (storage, _dir) = storage().await;
        let iam = IamStorage::new(storage);
        let pia = legacy_user("p-1", "pia", None);
        let mut answers = holdings(&[("node-1", &[&pia]), ("node-2", &[])]);
        answers[1].own = Err("connection refused".into());
        let request = |report_id: String| AdoptionRequest {
            report_id,
            mode: crate::iam::AdoptionMode::Union,
            users: Vec::new(),
        };
        let report = AdoptionReport::build(&answers);
        assert!(matches!(
            iam.adopt(&answers, &request(report.report_id), "root", "node-1").await,
            Err(IamError::AdoptionIncomplete(message)) if message.contains("node-2")
        ));

        let empty = holdings(&[("node-1", &[]), ("node-2", &[])]);
        let report = AdoptionReport::build(&empty);
        assert!(matches!(
            iam.adopt(&empty, &request(report.report_id), "root", "node-1").await,
            Err(IamError::AdoptionNotPending)
        ));

        let answers = holdings(&[("node-1", &[&pia])]);
        let report = AdoptionReport::build(&answers);
        let listed = AdoptionRequest {
            users: vec!["nobody".into()],
            mode: crate::iam::AdoptionMode::Users,
            ..request(report.report_id)
        };
        assert!(matches!(
            iam.adopt(&answers, &listed, "root", "node-1").await,
            Err(IamError::AdoptionInvalid(_))
        ));
        assert!(iam.adoption_record().await.unwrap().is_none());
        assert!(iam.list_users().await.unwrap().is_empty());
    }

    /// A single node marks that it holds users; started as a cluster node, it
    /// turns them back into its own IAM, keeping ids, passwords and keys, and
    /// a second start finds nothing more to turn.
    #[tokio::test]
    async fn a_single_node_s_users_become_its_own_iam_in_a_cluster() {
        let (storage, _dir) = storage().await;
        let single = IamStorage::new(storage.clone());
        assert!(!single.mark_single_node_users().await.unwrap());
        let quinn = single
            .create_user("quinn".into(), "Quinn-Pass-12".into(), Role::Writer)
            .await
            .unwrap();
        let keys = single.generate_s3_credentials(&quinn.id).await.unwrap();
        assert!(single.mark_single_node_users().await.unwrap());

        let clustered = IamStorage::new(storage.clone());
        assert_eq!(clustered.take_back_single_node_users().await.unwrap(), 1);
        let mut left = system_keys(&storage).await;
        left.sort();
        let mut expected = vec![
            format!("{LEGACY_ACCESS_KEY_KEY_PREFIX}{}", keys.access_key),
            format!("{LEGACY_USER_KEY_PREFIX}{}", quinn.id),
            format!("{LEGACY_USERNAME_KEY_PREFIX}quinn"),
        ];
        expected.sort();
        assert_eq!(left, expected, "no record of version 2 and no mark is left");
        let own = clustered.own_users(None).await.unwrap();
        assert_eq!(own.users()[0].id, quinn.id);

        clustered.serve_legacy().await.unwrap();
        assert!(clustered.verify_password("quinn", "Quinn-Pass-12").await.unwrap());
        assert!(clustered
            .verify_s3_credentials(&keys.access_key, &keys.secret_key)
            .await
            .unwrap()
            .is_some());
        assert_eq!(
            IamStorage::new(storage).take_back_single_node_users().await.unwrap(),
            0
        );
    }

    /// Which records of an earlier release still mean something.
    #[test]
    fn only_a_stored_root_without_keys_is_meaningless() {
        let plain = legacy_user("k-1", "kate", None);
        let root = legacy_user("r-1", "root", None);
        let keyed_root = legacy_user("r-2", "root", Some("S4AKROOT"));

        assert!(IamStorage::legacy_user_matters(&plain, Some("root")));
        assert!(IamStorage::legacy_user_matters(&plain, None));
        assert!(!IamStorage::legacy_user_matters(&root, Some("root")));
        assert!(IamStorage::legacy_user_matters(&root, None));
        assert!(IamStorage::legacy_user_matters(&keyed_root, Some("root")));
    }
}
