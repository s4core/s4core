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
/// Note: Sensitive fields (password_hash, secret_key, secret_key_hash) are stored
/// in the database but filtered out in API responses via UserResponse DTO.
#[derive(Debug, Clone, Serialize, Deserialize)]
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

    /// S3 Secret Key (stored for AWS Signature V4, filtered in API responses)
    pub secret_key: Option<String>,

    /// S3 Secret Key hash (for additional verification, filtered in API responses)
    pub secret_key_hash: Option<String>,

    /// Account creation timestamp
    pub created_at: DateTime<Utc>,

    /// Last update timestamp
    pub updated_at: DateTime<Utc>,

    /// Active status (disabled users cannot login)
    pub is_active: bool,
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

/// JWT claims structure.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JwtClaims {
    /// Subject (user ID)
    pub sub: String,

    /// Username
    pub username: String,

    /// User role
    pub role: Role,

    /// Expiration time (Unix timestamp)
    pub exp: i64,
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
            secret_key_hash: Some("secret_hash".to_string()),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            is_active: true,
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
}
