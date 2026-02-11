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

use super::error::IamError;
use super::jwt::JwtManager;
use super::models::{JwtClaims, LoginRequest, LoginResponse, User};
use super::storage::IamStorage;

/// Authentication service that combines IAM storage and JWT management.
#[derive(Clone)]
pub struct AuthService {
    storage: IamStorage,
    jwt_manager: JwtManager,
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
        }
    }

    /// Authenticate user with username and password.
    ///
    /// # Arguments
    ///
    /// * `request` - Login request with username and password
    ///
    /// # Returns
    ///
    /// Returns login response with JWT token and expiration time.
    ///
    /// # Errors
    ///
    /// - `InvalidCredentials` if username or password is incorrect
    /// - `UserNotFound` if user doesn't exist
    /// - `TokenGenerationFailed` if JWT generation fails
    pub async fn login(&self, request: LoginRequest) -> Result<LoginResponse, IamError> {
        // Verify credentials
        let is_valid = self.storage.verify_password(&request.username, &request.password).await?;
        if !is_valid {
            return Err(IamError::InvalidCredentials);
        }

        // Get user
        let user = self
            .storage
            .get_user_by_username(&request.username)
            .await?
            .ok_or(IamError::UserNotFound)?;

        // Generate JWT
        let token = self.jwt_manager.generate_token(&user)?;

        let expires_at = chrono::Utc::now() + chrono::Duration::hours(24);

        Ok(LoginResponse { token, expires_at })
    }

    /// Validate JWT token and return claims.
    ///
    /// # Arguments
    ///
    /// * `token` - JWT token string to validate
    ///
    /// # Returns
    ///
    /// Returns decoded JWT claims if token is valid.
    ///
    /// # Errors
    ///
    /// - `InvalidToken` if token is invalid or expired
    pub fn validate_token(&self, token: &str) -> Result<JwtClaims, IamError> {
        self.jwt_manager.validate_token(token)
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

#[cfg(test)]
mod tests {
    // Integration tests with actual storage will be in s4-api/tests
}
