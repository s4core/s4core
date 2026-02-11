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

//! IAM data storage using __system__ bucket.

use std::collections::HashMap;
use std::sync::Arc;

use uuid::Uuid;

use s4_core::storage::StorageEngine;

use super::error::IamError;
use super::models::{Role, S3Credentials, User};
use super::password::PasswordHasher;

/// IAM data storage using __system__ bucket.
///
/// Stores user data in the special `__system__` bucket with the following keys:
/// - `__s4_iam_user_{user_id}` - User records (JSON)
/// - `__s4_iam_username_{username}` - Username → User ID index
/// - `__s4_iam_access_key_{access_key}` - Access Key → User ID index
#[derive(Clone)]
pub struct IamStorage {
    storage: Arc<dyn StorageEngine>,
    hasher: PasswordHasher,
}

impl IamStorage {
    /// Create new IAM storage.
    ///
    /// # Arguments
    ///
    /// * `storage` - Storage engine to use for persisting IAM data
    pub fn new(storage: Arc<dyn StorageEngine>) -> Self {
        Self {
            storage,
            hasher: PasswordHasher::new(),
        }
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
    /// - `UserAlreadyExists` if username is already taken
    /// - `Storage` errors if database operations fail
    pub async fn create_user(
        &self,
        username: String,
        password: String,
        role: Role,
    ) -> Result<User, IamError> {
        // Validate username (alphanumeric + underscore, 3-32 chars)
        if !username.chars().all(|c| c.is_alphanumeric() || c == '_') {
            return Err(IamError::InvalidUsername);
        }
        if username.len() < 3 || username.len() > 32 {
            return Err(IamError::InvalidUsername);
        }

        // Check if username exists
        if self.user_exists(&username).await? {
            return Err(IamError::UserAlreadyExists);
        }

        // Hash password
        let password_hash = self.hasher.hash_password(&password)?;

        // Create user
        let user = User {
            id: Uuid::new_v4().to_string(),
            username: username.clone(),
            password_hash,
            role,
            access_key: None,
            secret_key: None,
            secret_key_hash: None,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            is_active: true,
        };

        // Serialize user
        let user_json = serde_json::to_vec(&user)?;

        // Store user record
        let user_key = format!("__s4_iam_user_{}", user.id);
        self.storage
            .put_object(
                "__system__",
                &user_key,
                &user_json,
                "application/json",
                &HashMap::new(),
            )
            .await?;

        // Store username index
        let username_key = format!("__s4_iam_username_{}", username);
        self.storage
            .put_object(
                "__system__",
                &username_key,
                user.id.as_bytes(),
                "text/plain",
                &HashMap::new(),
            )
            .await?;

        Ok(user)
    }

    /// Get user by username.
    pub async fn get_user_by_username(&self, username: &str) -> Result<Option<User>, IamError> {
        // Get user ID from username index
        let username_key = format!("__s4_iam_username_{}", username);
        let (user_id_bytes, _) = match self.storage.get_object("__system__", &username_key).await {
            Ok(data) => data,
            Err(_) => return Ok(None),
        };

        let user_id = String::from_utf8(user_id_bytes)?;

        // Get user record
        let user_key = format!("__s4_iam_user_{}", user_id);
        let (user_bytes, _) = match self.storage.get_object("__system__", &user_key).await {
            Ok(data) => data,
            Err(_) => return Ok(None),
        };

        let user: User = serde_json::from_slice(&user_bytes)?;
        Ok(Some(user))
    }

    /// Get user by access key.
    pub async fn get_user_by_access_key(&self, access_key: &str) -> Result<Option<User>, IamError> {
        // Get user ID from access key index
        let access_key_key = format!("__s4_iam_access_key_{}", access_key);
        let (user_id_bytes, _) = match self.storage.get_object("__system__", &access_key_key).await
        {
            Ok(data) => data,
            Err(_) => return Ok(None),
        };

        let user_id = String::from_utf8(user_id_bytes)?;

        // Get user record
        let user_key = format!("__s4_iam_user_{}", user_id);
        let (user_bytes, _) = match self.storage.get_object("__system__", &user_key).await {
            Ok(data) => data,
            Err(_) => return Ok(None),
        };

        let user: User = serde_json::from_slice(&user_bytes)?;
        Ok(Some(user))
    }

    /// Generate S3 credentials for user.
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
    /// - `Storage` errors if database operations fail
    pub async fn generate_s3_credentials(&self, user_id: &str) -> Result<S3Credentials, IamError> {
        // Get user
        let user_key = format!("__s4_iam_user_{}", user_id);
        let (user_bytes, _) = self.storage.get_object("__system__", &user_key).await?;
        let mut user: User = serde_json::from_slice(&user_bytes)?;

        // Generate random access key and secret key
        let access_key = format!("S4AK{}", nanoid::nanoid!(20));
        let secret_key = nanoid::nanoid!(40);
        let secret_key_hash = self.hasher.hash_password(&secret_key)?;

        // Update user (store both secret_key and its hash)
        user.access_key = Some(access_key.clone());
        user.secret_key = Some(secret_key.clone());
        user.secret_key_hash = Some(secret_key_hash);
        user.updated_at = chrono::Utc::now();

        // Save updated user
        let user_json = serde_json::to_vec(&user)?;
        self.storage
            .put_object(
                "__system__",
                &user_key,
                &user_json,
                "application/json",
                &HashMap::new(),
            )
            .await?;

        // Store access key index
        let access_key_key = format!("__s4_iam_access_key_{}", access_key);
        self.storage
            .put_object(
                "__system__",
                &access_key_key,
                user_id.as_bytes(),
                "text/plain",
                &HashMap::new(),
            )
            .await?;

        Ok(S3Credentials {
            access_key,
            secret_key,
        })
    }

    /// List all users.
    pub async fn list_users(&self) -> Result<Vec<User>, IamError> {
        // List all objects with prefix __s4_iam_user_
        let objects = self.storage.list_objects("__system__", "__s4_iam_user_", 1000).await?;

        let mut users = Vec::new();
        for (key, _) in objects {
            if !key.starts_with("__s4_iam_user_") {
                continue;
            }

            let (user_bytes, _) = self.storage.get_object("__system__", &key).await?;
            let user: User = serde_json::from_slice(&user_bytes)?;
            users.push(user);
        }

        Ok(users)
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
    /// (it has `#[serde(skip_serializing)]`). This method should only be used
    /// internally for signature verification.
    pub async fn get_user_by_access_key_with_secret(
        &self,
        access_key: &str,
    ) -> Result<Option<User>, IamError> {
        let user = match self.get_user_by_access_key(access_key).await? {
            Some(u) => u,
            None => return Ok(None),
        };

        if !user.is_active {
            return Ok(None);
        }

        if user.secret_key.is_none() {
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
    pub async fn update_user(
        &self,
        user_id: &str,
        password: Option<String>,
        role: Option<Role>,
        is_active: Option<bool>,
    ) -> Result<User, IamError> {
        // Get user
        let user_key = format!("__s4_iam_user_{}", user_id);
        let (user_bytes, _) = self.storage.get_object("__system__", &user_key).await?;
        let mut user: User = serde_json::from_slice(&user_bytes)?;

        // Update fields
        if let Some(new_password) = password {
            user.password_hash = self.hasher.hash_password(&new_password)?;
        }
        if let Some(new_role) = role {
            user.role = new_role;
        }
        if let Some(active) = is_active {
            user.is_active = active;
        }
        user.updated_at = chrono::Utc::now();

        // Save updated user
        let user_json = serde_json::to_vec(&user)?;
        self.storage
            .put_object(
                "__system__",
                &user_key,
                &user_json,
                "application/json",
                &HashMap::new(),
            )
            .await?;

        Ok(user)
    }

    /// Delete user.
    ///
    /// # Arguments
    ///
    /// * `user_id` - User ID to delete
    ///
    /// # Errors
    ///
    /// - `UserNotFound` if user doesn't exist
    /// - `Storage` errors if database operations fail
    pub async fn delete_user(&self, user_id: &str) -> Result<(), IamError> {
        // Get user
        let user_key = format!("__s4_iam_user_{}", user_id);
        let (user_bytes, _) = self.storage.get_object("__system__", &user_key).await?;
        let user: User = serde_json::from_slice(&user_bytes)?;

        // Delete username index
        let username_key = format!("__s4_iam_username_{}", user.username);
        self.storage.delete_object("__system__", &username_key).await.ok();

        // Delete access key index if exists
        if let Some(access_key) = &user.access_key {
            let access_key_key = format!("__s4_iam_access_key_{}", access_key);
            self.storage.delete_object("__system__", &access_key_key).await.ok();
        }

        // Delete user record
        self.storage.delete_object("__system__", &user_key).await?;

        Ok(())
    }

    /// Delete S3 credentials for user.
    ///
    /// # Arguments
    ///
    /// * `user_id` - User ID to delete credentials for
    pub async fn delete_s3_credentials(&self, user_id: &str) -> Result<(), IamError> {
        // Get user
        let user_key = format!("__s4_iam_user_{}", user_id);
        let (user_bytes, _) = self.storage.get_object("__system__", &user_key).await?;
        let mut user: User = serde_json::from_slice(&user_bytes)?;

        // Delete access key index if exists
        if let Some(access_key) = &user.access_key {
            let access_key_key = format!("__s4_iam_access_key_{}", access_key);
            self.storage.delete_object("__system__", &access_key_key).await.ok();
        }

        // Remove credentials from user
        user.access_key = None;
        user.secret_key = None;
        user.secret_key_hash = None;
        user.updated_at = chrono::Utc::now();

        // Save updated user
        let user_json = serde_json::to_vec(&user)?;
        self.storage
            .put_object(
                "__system__",
                &user_key,
                &user_json,
                "application/json",
                &HashMap::new(),
            )
            .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    // Note: Integration tests with actual storage will be in s4-api/tests

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
}
