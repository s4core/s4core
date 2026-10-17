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

use chrono::{DateTime, Duration, Utc};
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use rand::{rngs::OsRng, RngCore};

use super::error::IamError;
use super::models::{JwtClaims, Role, User};

/// Shortest signing secret considered strong enough for HS256, in bytes.
pub const MIN_SECRET_LEN: usize = 32;

/// Generates a random signing secret: 32 bytes from the OS CSPRNG, hex-encoded.
///
/// Used whenever no secret is configured, so tokens are never signed with a
/// value anyone could know in advance.
pub fn generate_secret() -> String {
    let mut bytes = [0u8; 32];
    OsRng.fill_bytes(&mut bytes);
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// A signed token and the moment it expires.
#[derive(Debug, Clone)]
pub struct IssuedToken {
    /// The signed JWT
    pub token: String,
    /// Expiration time, the token's `exp`
    pub expires_at: DateTime<Utc>,
}

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
    /// The token carries the user's current `credential_version`, so it stops
    /// validating against the account once the version moves on.
    ///
    /// # Errors
    ///
    /// Returns `IamError::TokenGenerationFailed` if token generation fails.
    pub fn generate_token(&self, user: &User) -> Result<String, IamError> {
        Ok(self.issue(&user.id, &user.username, user.role, user.credential_version)?.token)
    }

    /// Issues a token for the account `sub` at credential version `cv`.
    ///
    /// `username` and `role` go into the token for clients to show; the server
    /// reads both from the account instead.
    ///
    /// # Errors
    ///
    /// Returns `IamError::TokenGenerationFailed` if token generation fails.
    pub fn issue(
        &self,
        sub: &str,
        username: &str,
        role: Role,
        cv: u64,
    ) -> Result<IssuedToken, IamError> {
        let now = Utc::now();
        let expires_at = now + self.token_lifetime;

        let claims = JwtClaims {
            sub: sub.to_string(),
            username: username.to_string(),
            role,
            cv,
            iat: now.timestamp(),
            exp: expires_at.timestamp(),
            jti: uuid::Uuid::new_v4().to_string(),
        };

        let token = encode(&Header::default(), &claims, &self.encoding_key)
            .map_err(|_| IamError::TokenGenerationFailed)?;

        Ok(IssuedToken { token, expires_at })
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
            secret_key_sealed: None,
            secret_key_hash: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            is_active: true,
            credential_version: 7,
            created_epoch: 0,
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
        assert_eq!(claims.cv, 7);
    }

    #[test]
    fn every_token_has_its_own_id_and_issue_time() {
        let manager = JwtManager::new("test_secret_key_at_least_32_bytes", 24);
        let user = create_test_user();

        let first = manager.validate_token(&manager.generate_token(&user).unwrap()).unwrap();
        let second = manager.validate_token(&manager.generate_token(&user).unwrap()).unwrap();

        assert_ne!(first.jti, second.jti);
        assert!((first.iat - Utc::now().timestamp()).abs() < 5);
    }

    #[test]
    fn token_without_credential_version_is_rejected() {
        // The claims tokens of earlier releases carried: no `cv`, `iat`, `jti`.
        #[derive(serde::Serialize)]
        struct OldClaims {
            sub: String,
            username: String,
            role: Role,
            exp: i64,
        }
        let secret = "test_secret_key_at_least_32_bytes";
        let old = encode(
            &Header::default(),
            &OldClaims {
                sub: "user123".to_string(),
                username: "testuser".to_string(),
                role: Role::SuperUser,
                exp: Utc::now().timestamp() + 3600,
            },
            &EncodingKey::from_secret(secret.as_bytes()),
        )
        .unwrap();

        assert!(JwtManager::new(secret, 24).validate_token(&old).is_err());
    }

    #[test]
    fn issued_expiry_is_the_token_exp() {
        let manager = JwtManager::new("test_secret_key_at_least_32_bytes", 2);

        let issued = manager.issue("u", "name", Role::Reader, 0).unwrap();
        let claims = manager.validate_token(&issued.token).unwrap();

        assert_eq!(claims.exp, issued.expires_at.timestamp());
        assert!((claims.exp - Utc::now().timestamp() - 2 * 3600).abs() < 5);
    }

    #[test]
    fn generated_secrets_are_long_random_and_hex() {
        let first = generate_secret();
        let second = generate_secret();

        assert_eq!(first.len(), 64);
        assert!(first.len() >= MIN_SECRET_LEN);
        assert!(first.chars().all(|c| c.is_ascii_hexdigit()));
        assert_ne!(first, second);
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
