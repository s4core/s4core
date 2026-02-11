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

//! Password hashing and verification using Argon2.

use argon2::{
    password_hash::{PasswordHash, PasswordHasher as _, PasswordVerifier, SaltString},
    Argon2,
};
use rand::rngs::OsRng;

use super::error::IamError;

/// Password hasher using Argon2.
#[derive(Clone)]
pub struct PasswordHasher {
    argon2: Argon2<'static>,
}

impl PasswordHasher {
    /// Create new password hasher with default parameters.
    pub fn new() -> Self {
        Self {
            argon2: Argon2::default(),
        }
    }

    /// Hash password using Argon2.
    ///
    /// # Arguments
    ///
    /// * `password` - Plain text password to hash
    ///
    /// # Returns
    ///
    /// Returns PHC string format hash that can be stored in database.
    ///
    /// # Errors
    ///
    /// Returns `IamError::HashingFailed` if hashing fails.
    pub fn hash_password(&self, password: &str) -> Result<String, IamError> {
        let salt = SaltString::generate(&mut OsRng);
        let hash = self
            .argon2
            .hash_password(password.as_bytes(), &salt)
            .map_err(|_| IamError::HashingFailed)?;
        Ok(hash.to_string())
    }

    /// Verify password against hash.
    ///
    /// # Arguments
    ///
    /// * `password` - Plain text password to verify
    /// * `hash` - PHC string format hash from database
    ///
    /// # Returns
    ///
    /// Returns `Ok(true)` if password matches, `Ok(false)` if it doesn't.
    ///
    /// # Errors
    ///
    /// Returns `IamError::InvalidHash` if hash is malformed.
    pub fn verify_password(&self, password: &str, hash: &str) -> Result<bool, IamError> {
        let parsed_hash = PasswordHash::new(hash).map_err(|_| IamError::InvalidHash)?;

        Ok(self.argon2.verify_password(password.as_bytes(), &parsed_hash).is_ok())
    }
}

impl Default for PasswordHasher {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_password_hash_and_verify() {
        let hasher = PasswordHasher::new();
        let password = "test_password_123";

        let hash = hasher.hash_password(password).unwrap();
        assert!(hasher.verify_password(password, &hash).unwrap());
        assert!(!hasher.verify_password("wrong_password", &hash).unwrap());
    }

    #[test]
    fn test_different_passwords_different_hashes() {
        let hasher = PasswordHasher::new();

        let hash1 = hasher.hash_password("password").unwrap();
        let hash2 = hasher.hash_password("password").unwrap();

        // Different salts = different hashes
        assert_ne!(hash1, hash2);

        // But both should verify correctly
        assert!(hasher.verify_password("password", &hash1).unwrap());
        assert!(hasher.verify_password("password", &hash2).unwrap());
    }

    #[test]
    fn test_invalid_hash() {
        let hasher = PasswordHasher::new();

        let result = hasher.verify_password("password", "invalid_hash");
        assert!(result.is_err());
    }

    #[test]
    fn test_empty_password() {
        let hasher = PasswordHasher::new();

        let hash = hasher.hash_password("").unwrap();
        assert!(hasher.verify_password("", &hash).unwrap());
        assert!(!hasher.verify_password("nonempty", &hash).unwrap());
    }

    #[test]
    fn test_long_password() {
        let hasher = PasswordHasher::new();
        let long_password = "a".repeat(1000);

        let hash = hasher.hash_password(&long_password).unwrap();
        assert!(hasher.verify_password(&long_password, &hash).unwrap());
    }
}
