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

//! JWT token generation and validation.

use chrono::{Duration, Utc};
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};

use super::error::IamError;
use super::models::{JwtClaims, User};

/// JWT token manager.
#[derive(Clone)]
pub struct JwtManager {
    encoding_key: EncodingKey,
    decoding_key: DecodingKey,
    token_lifetime: Duration,
}

impl JwtManager {
    /// Create new JWT manager with secret key.
    ///
    /// # Arguments
    ///
    /// * `secret` - Secret key for signing tokens (should be at least 32 bytes)
    /// * `token_lifetime_hours` - Token lifetime in hours
    ///
    /// # Example
    ///
    /// ```no_run
    /// use s4_features::iam::jwt::JwtManager;
    ///
    /// let manager = JwtManager::new("my-secret-key-at-least-32-bytes-long", 24);
    /// ```
    pub fn new(secret: &str, token_lifetime_hours: i64) -> Self {
        Self {
            encoding_key: EncodingKey::from_secret(secret.as_bytes()),
            decoding_key: DecodingKey::from_secret(secret.as_bytes()),
            token_lifetime: Duration::hours(token_lifetime_hours),
        }
    }

    /// Generate JWT token for user.
    ///
    /// # Arguments
    ///
    /// * `user` - User to generate token for
    ///
    /// # Returns
    ///
    /// Returns JWT token string.
    ///
    /// # Errors
    ///
    /// Returns `IamError::TokenGenerationFailed` if token generation fails.
    pub fn generate_token(&self, user: &User) -> Result<String, IamError> {
        let expiration = Utc::now() + self.token_lifetime;

        let claims = JwtClaims {
            sub: user.id.clone(),
            username: user.username.clone(),
            role: user.role,
            exp: expiration.timestamp(),
        };

        let token = encode(&Header::default(), &claims, &self.encoding_key)
            .map_err(|_| IamError::TokenGenerationFailed)?;

        Ok(token)
    }

    /// Validate and decode JWT token.
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
    /// Returns `IamError::InvalidToken` if token is invalid or expired.
    pub fn validate_token(&self, token: &str) -> Result<JwtClaims, IamError> {
        let validation = Validation::default();

        let token_data = decode::<JwtClaims>(token, &self.decoding_key, &validation)
            .map_err(|_| IamError::InvalidToken)?;

        Ok(token_data.claims)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iam::models::Role;

    fn create_test_user() -> User {
        User {
            id: "user123".to_string(),
            username: "testuser".to_string(),
            password_hash: String::new(),
            role: Role::Writer,
            access_key: None,
            secret_key: None,
            secret_key_hash: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            is_active: true,
        }
    }

    #[test]
    fn test_jwt_generation_and_validation() {
        let manager = JwtManager::new("test_secret_key_at_least_32_bytes", 24);
        let user = create_test_user();

        let token = manager.generate_token(&user).unwrap();
        let claims = manager.validate_token(&token).unwrap();

        assert_eq!(claims.sub, "user123");
        assert_eq!(claims.username, "testuser");
        assert_eq!(claims.role, Role::Writer);
    }

    #[test]
    fn test_invalid_token() {
        let manager = JwtManager::new("test_secret_key_at_least_32_bytes", 24);

        let result = manager.validate_token("invalid_token");
        assert!(result.is_err());
    }

    #[test]
    fn test_token_with_wrong_secret() {
        let manager1 = JwtManager::new("secret_key_1_at_least_32_bytes_long", 24);
        let manager2 = JwtManager::new("secret_key_2_at_least_32_bytes_long", 24);

        let user = create_test_user();
        let token = manager1.generate_token(&user).unwrap();

        // Token signed with secret1 should not validate with secret2
        let result = manager2.validate_token(&token);
        assert!(result.is_err());
    }

    #[test]
    fn test_token_different_roles() {
        let manager = JwtManager::new("test_secret_key_at_least_32_bytes", 24);

        let roles = vec![Role::Reader, Role::Writer, Role::SuperUser];

        for role in roles {
            let mut user = create_test_user();
            user.role = role;

            let token = manager.generate_token(&user).unwrap();
            let claims = manager.validate_token(&token).unwrap();

            assert_eq!(claims.role, role);
        }
    }

    #[test]
    fn test_token_expiration_time() {
        let manager = JwtManager::new("test_secret_key_at_least_32_bytes", 24);
        let user = create_test_user();

        let token = manager.generate_token(&user).unwrap();
        let claims = manager.validate_token(&token).unwrap();

        let now = Utc::now().timestamp();
        let expected_exp = now + (24 * 3600); // 24 hours in seconds

        // Allow 5 second tolerance for test execution time
        assert!((claims.exp - expected_exp).abs() < 5);
    }
}
