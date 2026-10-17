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

//! IAM data models.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// User account in the IAM system.
///
/// Note: Sensitive fields (password_hash, secret_key_sealed, secret_key_hash) are
/// stored in the database but filtered out in API responses via UserResponse DTO,
/// and `Debug` does not show them.
#[derive(Clone, Serialize, Deserialize)]
pub struct User {
    /// Unique user identifier (UUID v4)
    pub id: String,

    /// Username (unique, alphanumeric + underscore)
    pub username: String,

    /// Argon2 password hash (stored in DB, filtered in API responses)
    pub password_hash: String,

    /// User role (Reader, Writer, SuperUser)
    pub role: Role,

    /// S3 Access Key (optional, for S3 API access)
    pub access_key: Option<String>,

    /// S3 Secret Key in clear text, as AWS Signature V4 needs it.
    ///
    /// A record is written with the secret sealed in `secret_key_sealed` and
    /// none here; a node opens it into this field when it reads the record
    /// into its directory. Records of earlier releases hold the secret here,
    /// in clear text, until they are sealed (plan CLUSTER-IAM-REPLICATION.md,
    /// decision 7).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret_key: Option<String>,

    /// S3 Secret Key encrypted with the IAM encryption key, bound to the
    /// user's id and access key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret_key_sealed: Option<super::secrets::SealedSecret>,

    /// S3 Secret Key hash (for additional verification, filtered in API responses)
    pub secret_key_hash: Option<String>,

    /// Account creation timestamp
    pub created_at: DateTime<Utc>,

    /// Last update timestamp
    pub updated_at: DateTime<Utc>,

    /// Active status (disabled users cannot login)
    pub is_active: bool,

    /// Version of the account's credentials: every admin token carries the value
    /// it was issued under and is accepted only while the record still has it.
    ///
    /// It grows on every change that must end the user's sessions: a new
    /// password, a new role, deactivation, and an explicit revocation or logout.
    /// Records of earlier releases have no such field and read as `0`.
    #[serde(default)]
    pub credential_version: u64,

    /// The epoch of the lease of IAM changes the user was created under; `0`
    /// for a user created on a single node or by an earlier release.
    ///
    /// Of two records of one name, the one created under the later epoch
    /// answers for it: the earlier one could only come from a node that
    /// wrote after its lease had passed on, a creation that was never
    /// confirmed (plan CLUSTER-IAM-REPLICATION.md, decision 3).
    #[serde(default)]
    pub created_epoch: u64,
}

impl std::fmt::Debug for User {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        const REDACTED: &str = "<redacted>";
        f.debug_struct("User")
            .field("id", &self.id)
            .field("username", &self.username)
            .field("password_hash", &REDACTED)
            .field("role", &self.role)
            .field("access_key", &self.access_key)
            .field("secret_key", &self.secret_key.as_ref().map(|_| REDACTED))
            .field(
                "secret_key_sealed",
                &self.secret_key_sealed.as_ref().map(|sealed| &sealed.key_id),
            )
            .field(
                "secret_key_hash",
                &self.secret_key_hash.as_ref().map(|_| REDACTED),
            )
            .field("created_at", &self.created_at)
            .field("updated_at", &self.updated_at)
            .field("is_active", &self.is_active)
            .field("credential_version", &self.credential_version)
            .field("created_epoch", &self.created_epoch)
            .finish()
    }
}

impl User {
    /// Whether the record keeps the secret of its S3 credentials in clear
    /// text, as an earlier release wrote it.
    pub fn holds_secret_in_clear(&self) -> bool {
        self.secret_key_sealed.is_none() && self.secret_key.is_some()
    }
}

/// User response for API (sensitive fields filtered out).
#[derive(Debug, Clone, Serialize)]
pub struct UserResponse {
    /// Unique user identifier (UUID v4)
    pub id: String,

    /// Username (unique, alphanumeric + underscore)
    pub username: String,

    /// User role (Reader, Writer, SuperUser)
    pub role: Role,

    /// S3 Access Key (optional, for S3 API access)
    pub access_key: Option<String>,

    /// Account creation timestamp
    pub created_at: DateTime<Utc>,

    /// Last update timestamp
    pub updated_at: DateTime<Utc>,

    /// Active status (disabled users cannot login)
    pub is_active: bool,
}

impl From<User> for UserResponse {
    fn from(user: User) -> Self {
        Self {
            id: user.id,
            username: user.username,
            role: user.role,
            access_key: user.access_key,
            created_at: user.created_at,
            updated_at: user.updated_at,
            is_active: user.is_active,
        }
    }
}

impl From<&User> for UserResponse {
    fn from(user: &User) -> Self {
        Self {
            id: user.id.clone(),
            username: user.username.clone(),
            role: user.role,
            access_key: user.access_key.clone(),
            created_at: user.created_at,
            updated_at: user.updated_at,
            is_active: user.is_active,
        }
    }
}

/// User role with permission levels.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum Role {
    /// Read-only access to S3 operations
    Reader,

    /// Read/Write access to S3 operations
    Writer,

    /// Full admin access (S3 + Admin API)
    SuperUser,
}

impl Role {
    /// Check if role can perform S3 read operations.
    pub fn can_read(&self) -> bool {
        matches!(self, Role::Reader | Role::Writer | Role::SuperUser)
    }

    /// Check if role can perform S3 write operations.
    pub fn can_write(&self) -> bool {
        matches!(self, Role::Writer | Role::SuperUser)
    }

    /// Check if role can access Admin API.
    pub fn can_admin(&self) -> bool {
        matches!(self, Role::SuperUser)
    }
}

/// Login request for Admin API.
#[derive(Debug, Deserialize)]
pub struct LoginRequest {
    /// Username to authenticate
    pub username: String,
    /// Plain text password
    pub password: String,
}

/// Login response with JWT token.
#[derive(Debug, Serialize)]
pub struct LoginResponse {
    /// JWT access token
    pub token: String,
    /// Token expiration timestamp
    pub expires_at: DateTime<Utc>,
}

/// Claims of an admin token.
///
/// A token refers to an account; it is not a copy of it. The server takes the
/// role and the active status from the account at every request and accepts the
/// token only while `cv` equals the account's `credential_version`, so a change
/// of password or role, a deactivation, a deletion or a revocation ends the
/// token at once. Tokens of earlier releases carry no `cv` and are rejected.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JwtClaims {
    /// Subject (user ID)
    pub sub: String,

    /// Username at the time the token was issued.
    ///
    /// For clients to show; the server never reads it.
    pub username: String,

    /// Role at the time the token was issued.
    ///
    /// For clients to show; the server never decides anything by it.
    pub role: Role,

    /// The account's `credential_version` when the token was issued.
    pub cv: u64,

    /// Issue time (Unix timestamp)
    pub iat: i64,

    /// Expiration time (Unix timestamp)
    pub exp: i64,

    /// Unique token id
    pub jti: String,
}

/// The account an admin request acts as, read from the account itself after
/// its token was checked against it.
///
/// Admin handlers decide by this, never by the token's claims.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdminPrincipal {
    /// User ID
    pub user_id: String,
    /// Username, as the account has it now
    pub username: String,
    /// Role, as the account has it now
    pub role: Role,
}

/// Request to create new user.
#[derive(Debug, Deserialize)]
pub struct CreateUserRequest {
    /// Username for the new user
    pub username: String,
    /// Plain text password
    pub password: String,
    /// User role
    pub role: Role,
}

/// Request to update user.
#[derive(Debug, Deserialize)]
pub struct UpdateUserRequest {
    /// New password (if changing)
    pub password: Option<String>,
    /// New role (if changing)
    pub role: Option<Role>,
    /// New active status (if changing)
    pub is_active: Option<bool>,
}

/// S3 credentials (Access Key + Secret Key).
#[derive(Debug, Serialize)]
pub struct S3Credentials {
    /// S3 access key (public identifier)
    pub access_key: String,
    /// S3 secret key - shown only once on creation
    pub secret_key: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_role_permissions() {
        assert!(Role::Reader.can_read());
        assert!(!Role::Reader.can_write());
        assert!(!Role::Reader.can_admin());

        assert!(Role::Writer.can_read());
        assert!(Role::Writer.can_write());
        assert!(!Role::Writer.can_admin());

        assert!(Role::SuperUser.can_read());
        assert!(Role::SuperUser.can_write());
        assert!(Role::SuperUser.can_admin());
    }

    #[test]
    fn test_user_serialization() {
        let user = User {
            id: "test-id".to_string(),
            username: "testuser".to_string(),
            password_hash: "secret_hash".to_string(),
            role: Role::Writer,
            access_key: Some("access123".to_string()),
            secret_key: Some("secret_key_value".to_string()),
            secret_key_sealed: None,
            secret_key_hash: Some("secret_hash".to_string()),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            is_active: true,
            credential_version: 0,
            created_epoch: 0,
        };

        // User serialization includes all fields (for storage)
        let json = serde_json::to_string(&user).unwrap();
        assert!(json.contains("testuser"));
        assert!(json.contains("Writer"));
        assert!(json.contains("password_hash")); // Now serialized for storage

        // UserResponse filters sensitive fields (for API responses)
        let response = UserResponse::from(&user);
        let response_json = serde_json::to_string(&response).unwrap();
        assert!(!response_json.contains("password_hash"));
        assert!(!response_json.contains("secret_key"));
        assert!(!response_json.contains("secret_hash"));
        assert!(response_json.contains("testuser"));
        assert!(response_json.contains("Writer"));
    }

    #[test]
    fn records_of_earlier_releases_read_with_credential_version_zero() {
        let json = r#"{"id":"u1","username":"old_user","password_hash":"h","role":"Writer",
            "access_key":null,"secret_key":null,"secret_key_hash":null,
            "created_at":"2026-01-01T00:00:00Z","updated_at":"2026-01-01T00:00:00Z",
            "is_active":true}"#;

        let user: User = serde_json::from_str(json).unwrap();

        assert_eq!(user.credential_version, 0);
    }
}
