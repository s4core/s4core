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

//! The users this node knows, and how to find one by name or access key.
//!
//! Every user is one record, `__system__/__s4_iam_v2_user_<id>` (plan
//! CLUSTER-IAM-REPLICATION.md, decision 2), and every node of a pool holds all
//! of them. The indexes "name → user" and "access key → user" are not stored:
//! the node builds them in memory from its own copy of the records when it
//! first needs them and changes them whenever a record of its storage changes
//! — a change made through this node, or one a peer, a hint or anti-entropy
//! brought ([`IamDirectory::observe`]). Checking a password, a signature or a
//! token reads here and asks no other node.
//!
//! Two records may name one user name or one access key: creations through
//! different nodes before changes were serialised by the lease of decision 3,
//! or a creation by a node that wrote after its lease had passed on, which
//! was never confirmed but may still reach a node no later holder did. The
//! record created under the later epoch of the lease answers for the name —
//! the stale creation is under the earlier one — then the one created first,
//! then the one of the lower id, so every node that holds both picks the
//! same.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use chrono::{DateTime, Utc};
use s4_core::error::StorageError;
use s4_core::storage::StorageEngine;
use s4_core::types::SYSTEM_BUCKET;
use tokio::sync::Mutex;

use super::error::IamError;
use super::models::User;
use super::secrets::{SecretKey, SecretKeySettings, SecretKeyring, NODE_SECRET_KEY_KEY};

/// Key prefix of a user record: `__s4_iam_v2_user_<id>` in `__system__`.
pub const USER_KEY_PREFIX: &str = "__s4_iam_v2_user_";

/// Key prefix of a user record of an earlier release: `__s4_iam_user_<id>`,
/// next to the indexes `__s4_iam_username_<name>` and
/// `__s4_iam_access_key_<key>` it was written with.
pub const LEGACY_USER_KEY_PREFIX: &str = s4_core::types::IAM_LEGACY_USER_KEY_PREFIX;

/// The key of the record of user `id`.
pub fn user_key(id: &str) -> String {
    format!("{USER_KEY_PREFIX}{id}")
}

/// Records read from storage at a time while the directory loads.
const LOAD_PAGE: usize = 1000;

/// Which records a node serves its users from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UserRecords {
    /// The records of version 2, one per user, which the pool keeps in step.
    Current,
    /// The records an earlier release left on this node, read once and never
    /// changed: a cluster node whose IAM of an earlier release has not been
    /// taken over yet (decision 8). Changes are refused until it is.
    Legacy,
}

/// Which of the records of one name or access key answers for it: the first
/// in this order.
type Rank = (Reverse<u64>, DateTime<Utc>, String);

/// The rank of `user` among the records of its name and access key.
fn rank(user: &User) -> Rank {
    (
        Reverse(user.created_epoch),
        user.created_at,
        user.id.clone(),
    )
}

/// Users by id, with the indexes built from them.
#[derive(Debug, Default)]
struct Indexes {
    users: BTreeMap<String, User>,
    by_name: HashMap<String, BTreeSet<Rank>>,
    by_access_key: HashMap<String, BTreeSet<Rank>>,
}

impl Indexes {
    fn insert(&mut self, user: User) {
        self.remove(&user.id);
        let entry = rank(&user);
        self.by_name.entry(user.username.clone()).or_default().insert(entry.clone());
        if let Some(access_key) = &user.access_key {
            self.by_access_key.entry(access_key.clone()).or_default().insert(entry);
        }
        self.users.insert(user.id.clone(), user);
    }

    fn remove(&mut self, id: &str) {
        let Some(user) = self.users.remove(id) else {
            return;
        };
        let entry = rank(&user);
        unindex(&mut self.by_name, &user.username, &entry);
        if let Some(access_key) = &user.access_key {
            unindex(&mut self.by_access_key, access_key, &entry);
        }
    }

    fn first_of(&self, index: &HashMap<String, BTreeSet<Rank>>, name: &str) -> Option<User> {
        let (_, _, id) = index.get(name)?.first()?;
        self.users.get(id).cloned()
    }
}

fn unindex(index: &mut HashMap<String, BTreeSet<Rank>>, name: &str, entry: &Rank) {
    if let Some(ids) = index.get_mut(name) {
        ids.remove(entry);
        if ids.is_empty() {
            index.remove(name);
        }
    }
}

/// The users of this node's own copy of IAM, indexed by id, name and access
/// key.
pub struct IamDirectory {
    storage: Arc<dyn StorageEngine>,
    records: RwLock<UserRecords>,
    loaded: AtomicBool,
    indexes: RwLock<Indexes>,
    /// Held while the directory reads storage and applies what it read, so
    /// that a later read is never applied before an earlier one.
    applying: Mutex<()>,
    /// What the configuration says about the keys secrets are sealed with.
    key_settings: RwLock<SecretKeySettings>,
    /// The keys these settings make, once resolved.
    keys: RwLock<Option<Arc<SecretKeyring>>>,
}

impl std::fmt::Debug for IamDirectory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IamDirectory")
            .field("records", &self.records())
            .field("loaded", &self.loaded.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}

impl IamDirectory {
    /// The users of `storage`, this node's own, from the records of version 2.
    /// Nothing is read until the first lookup.
    pub fn new(storage: Arc<dyn StorageEngine>) -> Self {
        Self {
            storage,
            records: RwLock::new(UserRecords::Current),
            loaded: AtomicBool::new(false),
            indexes: RwLock::new(Indexes::default()),
            applying: Mutex::new(()),
            key_settings: RwLock::new(SecretKeySettings::default()),
            keys: RwLock::new(None),
        }
    }

    /// Seals and opens secrets with the keys `settings` name from now on,
    /// and reads the users again with them.
    ///
    /// # Errors
    ///
    /// `Storage`, `Json` or `InvalidSecretKey` when the records or the node's
    /// own key cannot be read.
    pub async fn use_key_settings(&self, settings: SecretKeySettings) -> Result<(), IamError> {
        let _applying = self.applying.lock().await;
        *self.key_settings.write().unwrap_or_else(|poisoned| poisoned.into_inner()) = settings;
        *self.keys.write().unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
        if self.loaded.load(Ordering::Acquire) {
            self.load_locked().await?;
        }
        Ok(())
    }

    /// The keys secrets are sealed and opened with: the configured ones, with
    /// the key this node generated for itself readable too when it has one.
    /// Without configured keys, the node's own key; when it has none, one is
    /// generated and kept if `create`, and `None` is answered otherwise —
    /// nothing can be sealed with a key that does not exist.
    ///
    /// # Errors
    ///
    /// `Storage` or `InvalidSecretKey` when the node's own key cannot be read
    /// or kept.
    pub async fn keyring(&self, create: bool) -> Result<Option<Arc<SecretKeyring>>, IamError> {
        if let Some(keys) =
            self.keys.read().unwrap_or_else(|poisoned| poisoned.into_inner()).clone()
        {
            return Ok(Some(keys));
        }
        let settings = self
            .key_settings
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        let node_key = self.node_key().await?;
        let keys = match settings.keyring(node_key) {
            Some(keys) => keys,
            None if create => {
                let (bytes, key) = SecretKey::generate();
                self.storage
                    .put_object(
                        SYSTEM_BUCKET,
                        NODE_SECRET_KEY_KEY,
                        super::secrets::encode_hex(&bytes).as_bytes(),
                        "text/plain",
                        &std::collections::HashMap::new(),
                    )
                    .await?;
                tracing::warn!(
                    key_id = %key.id(),
                    "neither S4_IAM_ENCRYPTION_KEY nor S4_JWT_SECRET is set: the secrets of S3 \
                     credentials are encrypted with a key this node generated and keeps in its \
                     data directory"
                );
                match settings.keyring(Some(key)) {
                    Some(keys) => keys,
                    None => return Ok(None),
                }
            }
            None => return Ok(None),
        };
        *self.keys.write().unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(keys.clone());
        Ok(Some(keys))
    }

    /// `user` as its record is written: the secret of its S3 credentials
    /// sealed with the current key, none in clear text. A secret already
    /// sealed and not opened stays as it is.
    ///
    /// # Errors
    ///
    /// `Storage` or `InvalidSecretKey` when there is no key to seal with;
    /// `SecretSealing` if sealing fails.
    pub async fn sealed(&self, user: &User) -> Result<User, IamError> {
        let mut stored = user.clone();
        if let (Some(secret), Some(access_key)) = (&user.secret_key, &user.access_key) {
            let keys = self
                .keyring(true)
                .await?
                .ok_or_else(|| IamError::InvalidSecretKey("no key to seal secrets with".into()))?;
            stored.secret_key_sealed = Some(keys.seal(&user.id, access_key, secret)?);
        }
        stored.secret_key = None;
        Ok(stored)
    }

    /// A user record of this node, as it is sent to another node: its secret
    /// sealed with the current key, never in clear text (plan
    /// CLUSTER-IAM-REPLICATION.md, decision 7). A secret sealed with another
    /// key this node opens — its own, generated while it was a single node —
    /// is sealed again, so that the pool opens it. What is not a user record
    /// with a secret, or a secret this node does not open, is sent as it is.
    ///
    /// # Errors
    ///
    /// As [`Self::sealed`].
    pub async fn sealed_record(&self, data: &[u8]) -> Result<Vec<u8>, IamError> {
        let Ok(mut user) = serde_json::from_slice::<User>(data) else {
            return Ok(data.to_vec());
        };
        if let (Some(sealed), Some(access_key)) = (&user.secret_key_sealed, &user.access_key) {
            let Some(keys) = self.keyring(false).await? else {
                return Ok(data.to_vec());
            };
            if sealed.key_id == keys.current().id() || !keys.opens(&sealed.key_id) {
                return Ok(data.to_vec());
            }
            user.secret_key = Some(keys.open(&user.id, access_key, sealed)?);
        } else if !user.holds_secret_in_clear() {
            return Ok(data.to_vec());
        }
        Ok(serde_json::to_vec(&self.sealed(&user).await?)?)
    }

    /// The key this node generated for itself, if it has one.
    async fn node_key(&self) -> Result<Option<SecretKey>, IamError> {
        match self.storage.get_object(SYSTEM_BUCKET, NODE_SECRET_KEY_KEY).await {
            Ok((bytes, _)) => Ok(Some(SecretKey::from_hex(
                &String::from_utf8(bytes)?,
                super::secrets::KeyOrigin::Node,
            )?)),
            Err(StorageError::ObjectNotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// How many users hold the secret of their S3 credentials under each key
    /// id; `clear` for secrets in clear text, as an earlier release kept them.
    ///
    /// # Errors
    ///
    /// `Storage` or `Json` when the records cannot be loaded.
    pub async fn secrets_by_key_id(&self) -> Result<BTreeMap<String, usize>, IamError> {
        self.ensure_loaded().await?;
        let mut counts = BTreeMap::new();
        for user in self.read_indexes().users.values() {
            let key_id = match (&user.secret_key_sealed, user.holds_secret_in_clear()) {
                (Some(sealed), _) => sealed.key_id.clone(),
                (None, true) => "clear".to_string(),
                (None, false) => continue,
            };
            *counts.entry(key_id).or_insert(0) += 1;
        }
        Ok(counts)
    }

    /// Which records the users come from.
    pub fn records(&self) -> UserRecords {
        *self.records.read().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Serves the users from `records` from now on, read afresh.
    ///
    /// # Errors
    ///
    /// `Storage` or `Json` when the records cannot be read.
    pub async fn serve_from(&self, records: UserRecords) -> Result<(), IamError> {
        let _applying = self.applying.lock().await;
        *self.records.write().unwrap_or_else(|poisoned| poisoned.into_inner()) = records;
        self.load_locked().await?;
        self.loaded.store(true, Ordering::Release);
        Ok(())
    }

    /// Brings the directory up to the record at `bucket/key` of this node's
    /// storage when it is a user record of version 2; any other key is not
    /// the directory's. Called after every change of the storage a cluster
    /// node makes — through this node, from a peer, a hint or anti-entropy.
    ///
    /// # Errors
    ///
    /// `Storage` or `Json` when the record cannot be read.
    pub async fn observe(&self, bucket: &str, key: &str) -> Result<(), IamError> {
        match key.strip_prefix(USER_KEY_PREFIX) {
            Some(id) if bucket == SYSTEM_BUCKET => self.refresh(id).await,
            _ => Ok(()),
        }
    }

    /// Brings the directory up to the record of user `id` in storage: the
    /// record as it is now, or none.
    ///
    /// Reads and applies under one lock, so of two refreshes the one that read
    /// later is applied later: once every change of storage is followed by a
    /// refresh, the directory ends where storage is. The records of an earlier
    /// release never change, and a directory serving them is not refreshed.
    ///
    /// # Errors
    ///
    /// `Storage` or `Json` when the record cannot be read.
    pub async fn refresh(&self, id: &str) -> Result<(), IamError> {
        let _applying = self.applying.lock().await;
        if !self.loaded.load(Ordering::Acquire) {
            // The first load reads every record, this one included.
            self.load_locked().await?;
            self.loaded.store(true, Ordering::Release);
            return Ok(());
        }
        if self.records() == UserRecords::Legacy {
            return Ok(());
        }
        match self.read_user(&user_key(id)).await? {
            Some(mut user) if user.id == id => {
                if user.secret_key_sealed.is_some() {
                    if let Some(keys) = self.keyring(false).await? {
                        open_secret(&mut user, &keys);
                    }
                }
                self.write_indexes().insert(user)
            }
            Some(user) => {
                tracing::warn!(
                    key = %user_key(id),
                    id = %user.id,
                    "an IAM record holds a user of another id; ignored"
                );
                self.write_indexes().remove(id);
            }
            None => self.write_indexes().remove(id),
        }
        Ok(())
    }

    /// The user `id`.
    ///
    /// # Errors
    ///
    /// `Storage` or `Json` when the records cannot be loaded.
    pub async fn user(&self, id: &str) -> Result<Option<User>, IamError> {
        self.ensure_loaded().await?;
        Ok(self.read_indexes().users.get(id).cloned())
    }

    /// The user named `username`; of two records of one name, the one created
    /// first.
    ///
    /// # Errors
    ///
    /// `Storage` or `Json` when the records cannot be loaded.
    pub async fn user_by_name(&self, username: &str) -> Result<Option<User>, IamError> {
        self.ensure_loaded().await?;
        let indexes = self.read_indexes();
        Ok(indexes.first_of(&indexes.by_name, username))
    }

    /// The user holding `access_key`; of two records holding it, the one
    /// created first.
    ///
    /// # Errors
    ///
    /// `Storage` or `Json` when the records cannot be loaded.
    pub async fn user_by_access_key(&self, access_key: &str) -> Result<Option<User>, IamError> {
        self.ensure_loaded().await?;
        let indexes = self.read_indexes();
        Ok(indexes.first_of(&indexes.by_access_key, access_key))
    }

    /// Every user, in the order of their ids.
    ///
    /// # Errors
    ///
    /// `Storage` or `Json` when the records cannot be loaded.
    pub async fn users(&self) -> Result<Vec<User>, IamError> {
        self.ensure_loaded().await?;
        Ok(self.read_indexes().users.values().cloned().collect())
    }

    /// Up to `max` users after the id `after` — from the first without it —
    /// in the order of their ids, and whether more follow.
    ///
    /// # Errors
    ///
    /// `Storage` or `Json` when the records cannot be loaded.
    pub async fn users_after(
        &self,
        after: Option<&str>,
        max: usize,
    ) -> Result<(Vec<User>, bool), IamError> {
        use std::ops::Bound;
        self.ensure_loaded().await?;
        let indexes = self.read_indexes();
        let start = match after {
            Some(id) => Bound::Excluded(id.to_string()),
            None => Bound::Unbounded,
        };
        let mut range = indexes.users.range((start, Bound::Unbounded));
        let page: Vec<User> = range.by_ref().take(max).map(|(_, user)| user.clone()).collect();
        Ok((page, range.next().is_some()))
    }

    async fn ensure_loaded(&self) -> Result<(), IamError> {
        if self.loaded.load(Ordering::Acquire) {
            return Ok(());
        }
        let _applying = self.applying.lock().await;
        if !self.loaded.load(Ordering::Acquire) {
            self.load_locked().await?;
            self.loaded.store(true, Ordering::Release);
        }
        Ok(())
    }

    /// Reads every user record of the served kind into fresh indexes. The
    /// caller holds `applying`.
    async fn load_locked(&self) -> Result<(), IamError> {
        let prefix = match self.records() {
            UserRecords::Current => USER_KEY_PREFIX,
            UserRecords::Legacy => LEGACY_USER_KEY_PREFIX,
        };
        let mut indexes = Indexes::default();
        for key in list_keys(&*self.storage, prefix).await? {
            if let Some(user) = self.read_user(&key).await? {
                indexes.insert(user);
            }
        }
        if indexes.users.values().any(|user| user.secret_key_sealed.is_some()) {
            if let Some(keys) = self.keyring(false).await? {
                for user in indexes.users.values_mut() {
                    open_secret(user, &keys);
                }
            }
        }
        *self.write_indexes() = indexes;
        Ok(())
    }

    async fn read_user(&self, key: &str) -> Result<Option<User>, IamError> {
        match self.storage.get_object(SYSTEM_BUCKET, key).await {
            Ok((data, _)) => Ok(Some(serde_json::from_slice(&data)?)),
            Err(StorageError::ObjectNotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    fn read_indexes(&self) -> RwLockReadGuard<'_, Indexes> {
        self.indexes.read().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn write_indexes(&self) -> RwLockWriteGuard<'_, Indexes> {
        self.indexes.write().unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// Opens the sealed secret of `user` into `secret_key` with `keys`; a secret
/// that does not open stays sealed, and the user's S3 credentials are not
/// checked on this node.
fn open_secret(user: &mut User, keys: &SecretKeyring) {
    let (Some(sealed), Some(access_key)) = (&user.secret_key_sealed, &user.access_key) else {
        return;
    };
    match keys.open(&user.id, access_key, sealed) {
        Ok(secret) => user.secret_key = Some(secret),
        Err(error) => {
            user.secret_key = None;
            tracing::error!(
                user_id = %user.id,
                key_id = %sealed.key_id,
                %error,
                "the secret of a user's S3 credentials does not open on this node"
            );
        }
    }
}

/// Every key of `__system__` under `prefix` in `storage`, a page at a time.
pub(crate) async fn list_keys(
    storage: &dyn StorageEngine,
    prefix: &str,
) -> Result<Vec<String>, IamError> {
    let mut keys = Vec::new();
    let mut after: Option<String> = None;
    loop {
        let page = match &after {
            None => storage.list_objects(SYSTEM_BUCKET, prefix, LOAD_PAGE).await?,
            Some(last) => {
                storage.list_objects_after(SYSTEM_BUCKET, prefix, last, LOAD_PAGE).await?
            }
        };
        let full = page.len() == LOAD_PAGE;
        after = page.last().map(|(key, _)| key.clone());
        keys.extend(page.into_iter().map(|(key, _)| key).filter(|key| key.starts_with(prefix)));
        if !full {
            return Ok(keys);
        }
    }
}
