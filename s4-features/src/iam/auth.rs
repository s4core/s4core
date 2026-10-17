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

//! Authentication service.
//!
//! Admin tokens refer to an account and are checked against it at every
//! request: [`AuthService::authenticate`] accepts a token only while its account
//! exists, is active and still has the credential version the token was issued
//! under, and returns the role the account has now.

use super::error::IamError;
use super::jwt::JwtManager;
use super::models::{AdminPrincipal, LoginRequest, LoginResponse, Role, User, UserResponse};
use super::root::{RootAccount, RootStart, RootState};
use super::storage::IamStorage;

/// Authentication service that combines IAM storage and JWT management.
#[derive(Clone)]
pub struct AuthService {
    storage: IamStorage,
    jwt_manager: JwtManager,
    root: Option<RootAccount>,
}

impl AuthService {
    /// Create new authentication service.
    ///
    /// # Arguments
    ///
    /// * `storage` - IAM storage for user data
    /// * `jwt_manager` - JWT manager for token generation/validation
    pub fn new(storage: IamStorage, jwt_manager: JwtManager) -> Self {
        Self {
            storage,
            jwt_manager,
            root: None,
        }
    }

    /// The same service over `storage`: the same IAM changed another way.
    pub fn with_storage(&self, storage: IamStorage) -> Self {
        Self {
            storage,
            ..self.clone()
        }
    }

    /// The same service, its changes made by `actor` in the audit log.
    pub fn acting_as(&self, actor: super::audit::IamActor) -> Self {
        Self {
            storage: self.storage.acting_as(actor),
            ..self.clone()
        }
    }

    /// The same service with `root` as the root account.
    pub fn with_root(mut self, root: RootAccount) -> Self {
        self.root = Some(root);
        self
    }

    /// The configured root account, if any.
    pub fn root(&self) -> Option<&RootAccount> {
        self.root.as_ref()
    }

    /// Whether `user_id` is the configured root.
    pub fn is_root_id(&self, user_id: &str) -> bool {
        self.root.as_ref().is_some_and(|root| root.id() == user_id)
    }

    /// Whether `username` is the configured root's name.
    pub fn is_root_name(&self, username: &str) -> bool {
        self.root.as_ref().is_some_and(|root| root.username() == username)
    }

    /// Records the configuration root started with and returns root's state.
    ///
    /// A password, name or JWT secret other than the one this node last
    /// started with raises root's credential version, so root tokens issued
    /// before the change stop validating; the second value says whether that
    /// happened. On a cluster node the state is the pool's: it is read from
    /// and written to a majority of the pool, only when the configuration
    /// changed, and a change already raised by another node is not raised
    /// again. A start with the configuration of the previous one, or the
    /// node's first start with root, asks no other node.
    ///
    /// # Errors
    ///
    /// `Storage` errors if the state cannot be read or written; `Unavailable`
    /// if too few nodes of the pool answered.
    pub async fn start_root(&self) -> Result<Option<(RootState, bool)>, IamError> {
        let Some(root) = &self.root else {
            return Ok(None);
        };
        let current = root.fingerprint();
        let recorded = self.storage.own_root_fingerprint().await?;
        // A node of the previous release recorded its configuration in root's
        // state, which was its own then.
        let own = match recorded.clone() {
            Some(own) => Some(own),
            None => self.storage.root_state().await?.fingerprint,
        };
        let (state, ended) = match RootStart::of(own.as_deref(), current) {
            RootStart::Unchanged | RootStart::First => (self.storage.root_state().await?, false),
            RootStart::Changed => {
                let (known, state) = self
                    .storage
                    .change_root_state(
                        (root.id(), root.username()),
                        "a new root configuration",
                        |known| known.after_change(current).0,
                    )
                    .await?;
                let ended = state != known;
                (state, ended)
            }
        };
        if recorded.as_deref() != Some(current) {
            self.storage.save_own_root_fingerprint(current).await?;
        }
        Ok(Some((state, ended)))
    }

    /// Authenticate user with username and password.
    ///
    /// The configured root's name is checked against the configured password;
    /// a stored account of that name is never used to log in.
    ///
    /// # Errors
    ///
    /// - `InvalidCredentials` if username or password is incorrect
    /// - `NotServing` if a user other than root logs in while this node does
    ///   not check IAM credentials against its copy of IAM
    /// - `TokenGenerationFailed` if JWT generation fails
    pub async fn login(&self, request: LoginRequest) -> Result<LoginResponse, IamError> {
        if let Some(root) = self.root.as_ref().filter(|root| root.username() == request.username) {
            if !root.verify_password(&request.password)? {
                return Err(IamError::InvalidCredentials);
            }
            let cv = self.storage.root_state().await?.credential_version;
            let issued = self.jwt_manager.issue(root.id(), root.username(), Role::SuperUser, cv)?;
            return Ok(LoginResponse {
                token: issued.token,
                expires_at: issued.expires_at,
            });
        }

        // Checked against this node's copy of IAM, once it may be trusted.
        self.storage.admit_credentials()?;
        let is_valid = self.storage.verify_password(&request.username, &request.password).await?;
        if !is_valid {
            return Err(IamError::InvalidCredentials);
        }

        // Get user
        let user = self
            .storage
            .get_user_by_username(&request.username)
            .await?
            .ok_or(IamError::InvalidCredentials)?;

        let issued =
            self.jwt_manager
                .issue(&user.id, &user.username, user.role, user.credential_version)?;

        Ok(LoginResponse {
            token: issued.token,
            expires_at: issued.expires_at,
        })
    }

    /// Checks an admin token against its account and returns who the request
    /// acts as, with the role the account has now.
    ///
    /// # Errors
    ///
    /// - `InvalidToken` if the token is malformed, badly signed or expired, its
    ///   account does not exist or is inactive, or the account's credentials
    ///   changed since the token was issued
    /// - `NotServing` if the token is a user's other than root while this node
    ///   does not check IAM credentials against its copy of IAM: the token may
    ///   be fine, another node checks it
    /// - `Storage` errors if the account cannot be read: the caller answers that
    ///   as a fault, not as a rejected token
    pub async fn authenticate(&self, token: &str) -> Result<AdminPrincipal, IamError> {
        let claims = self.jwt_manager.validate_token(token)?;

        if let Some(root) = self.root.as_ref().filter(|root| root.id() == claims.sub) {
            let cv = self.storage.root_state().await?.credential_version;
            if claims.cv != cv {
                return Err(IamError::InvalidToken);
            }
            return Ok(AdminPrincipal {
                user_id: root.id().to_string(),
                username: root.username().to_string(),
                role: Role::SuperUser,
            });
        }

        self.storage.admit_credentials()?;
        let user = self.storage.get_user(&claims.sub).await?.ok_or(IamError::InvalidToken)?;
        // An account stored under root's name is not root, and cannot act as it.
        if !user.is_active
            || user.credential_version != claims.cv
            || self.is_root_name(&user.username)
        {
            return Err(IamError::InvalidToken);
        }

        Ok(AdminPrincipal {
            user_id: user.id,
            username: user.username,
            role: user.role,
        })
    }

    /// Ends every session of the account `user_id`, root included.
    ///
    /// # Errors
    ///
    /// - `UserNotFound` if there is no such account
    /// - `Storage` errors if database operations fail
    pub async fn revoke_sessions(&self, user_id: &str) -> Result<(), IamError> {
        if let Some(root) = self.root.as_ref().filter(|root| root.id() == user_id) {
            return self
                .storage
                .change_root_state((root.id(), root.username()), "revoked", |known| RootState {
                    credential_version: known.credential_version + 1,
                    ..known.clone()
                })
                .await
                .map(|_| ());
        }
        self.storage.revoke_sessions(user_id).await.map(|_| ())
    }

    /// Every account: the configured root first, then the stored users.
    ///
    /// A stored account under root's name, left by an earlier release that
    /// stored root, is not listed: it can no longer log in, and listing it would
    /// show two accounts of one name.
    ///
    /// # Errors
    ///
    /// `Storage` errors if database operations fail.
    pub async fn list_users(&self) -> Result<Vec<UserResponse>, IamError> {
        let mut users = Vec::new();
        if let Some(root) = &self.root {
            let cv = self.storage.root_state().await?.credential_version;
            users.push(UserResponse::from(root.as_user(cv)));
        }
        users.extend(
            self.storage
                .list_users()
                .await?
                .into_iter()
                .filter(|user| !self.is_root_name(&user.username))
                .map(UserResponse::from),
        );
        Ok(users)
    }

    /// Up to `max` accounts after the account `after`: the configured root
    /// first — it comes before every stored user, after none — then the
    /// stored users in the order of their ids. The id to continue after, when
    /// more follow; `""` after root alone.
    ///
    /// # Errors
    ///
    /// `Storage` errors if database operations fail.
    pub async fn list_users_page(
        &self,
        after: Option<&str>,
        max: usize,
    ) -> Result<(Vec<UserResponse>, Option<String>), IamError> {
        let mut users = Vec::new();
        if after.is_none() {
            if let Some(root) = &self.root {
                let cv = self.storage.root_state().await?.credential_version;
                users.push(UserResponse::from(root.as_user(cv)));
            }
        }
        let room = max.saturating_sub(users.len());
        let after_users = match after {
            Some("") | None => None,
            Some(id) => Some(id),
        };
        let (stored, more) = if room == 0 {
            let (first, _) = self.storage.list_users_after(after_users, 1).await?;
            (Vec::new(), !first.is_empty())
        } else {
            self.storage.list_users_after(after_users, room).await?
        };
        let next = if more {
            Some(stored.last().map(|user| user.id.clone()).unwrap_or_default())
        } else {
            None
        };
        users.extend(
            stored
                .into_iter()
                .filter(|user| !self.is_root_name(&user.username))
                .map(UserResponse::from),
        );
        Ok((users, next))
    }

    /// The account `user_id`, root included.
    ///
    /// # Errors
    ///
    /// `Storage` errors if database operations fail.
    pub async fn get_user(&self, user_id: &str) -> Result<Option<User>, IamError> {
        if let Some(root) = self.root.as_ref().filter(|root| root.id() == user_id) {
            let cv = self.storage.root_state().await?.credential_version;
            return Ok(Some(root.as_user(cv)));
        }
        self.storage.get_user(user_id).await
    }

    /// Get user by username.
    pub async fn get_user_by_username(&self, username: &str) -> Result<Option<User>, IamError> {
        self.storage.get_user_by_username(username).await
    }

    /// Get user by access key.
    pub async fn get_user_by_access_key(&self, access_key: &str) -> Result<Option<User>, IamError> {
        self.storage.get_user_by_access_key(access_key).await
    }

    /// Verify S3 credentials.
    ///
    /// # Arguments
    ///
    /// * `access_key` - S3 access key
    /// * `secret_key` - S3 secret key
    ///
    /// # Returns
    ///
    /// Returns user if credentials are valid, None if invalid.
    pub async fn verify_s3_credentials(
        &self,
        access_key: &str,
        secret_key: &str,
    ) -> Result<Option<User>, IamError> {
        self.storage.verify_s3_credentials(access_key, secret_key).await
    }
}
