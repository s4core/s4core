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

//! IAM (Identity and Access Management) module.
//!
//! Provides user authentication, authorization, and permission management.

pub mod adoption;
pub mod audit;
pub mod auth;
pub mod error;
pub mod gate;
pub mod jwt;
pub mod models;
pub mod password;
pub mod permissions;
pub mod policy;
pub mod root;
pub mod secrets;
pub mod storage;
pub mod store;
pub mod user_directory;

pub use adoption::{
    AdoptedUser, AdoptionMode, AdoptionPlan, AdoptionRecord, AdoptionReport, AdoptionRequest,
    LeftOutUser, NameConflict, NodeHoldings, OwnUsers, ReportedCopy, ReportedName, ReportedNode,
    ReportedRecord, ADOPTION_KEY,
};
pub use audit::{AuditAction, AuditEntry, AuditPage, IamActor, AUDIT_KEY_PREFIX};
pub use auth::AuthService;
pub use error::IamError;
pub use gate::{AdoptionGate, CredentialGate, NotServing};
pub use jwt::{IssuedToken, JwtManager};
pub use models::{
    AdminPrincipal, CreateUserRequest, JwtClaims, LoginRequest, LoginResponse, Role, S3Credentials,
    UpdateUserRequest, User, UserResponse,
};
pub use password::PasswordHasher;
pub use permissions::check_permission;
pub use policy::Policy;
pub use root::{RootAccount, RootStart, RootState};
pub use secrets::{
    jwt_secret_fingerprint, KeyOrigin, SealedSecret, SecretKey, SecretKeySettings, SecretKeyring,
};
pub use storage::{IamStorage, LegacyConversion, Resealing};
pub use store::{IamChange, IamRecordStore, LocalRecordStore};
pub use user_directory::{IamDirectory, UserRecords, USER_KEY_PREFIX};
