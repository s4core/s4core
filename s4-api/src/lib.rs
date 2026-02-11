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

//! S4 API Layer - HTTP API and S3 Compatibility
//!
//! This crate provides the HTTP API layer for S4, including:
//! - AWS S3 API compatibility
//! - AWS Signature V4 authentication
//! - HTTP handlers for buckets and objects
//! - Middleware for auth, metrics, and logging

pub mod auth;
pub mod handlers;
pub mod middleware;
pub mod s3;
pub mod server;

pub use auth::credentials::Credentials;
pub use auth::signature_v4::verify_signature_v4;
pub use handlers::bucket;
pub use handlers::object;
pub use s3::errors::S3Error;
pub use server::{create_router, AppState, ServerConfig, DEFAULT_MAX_UPLOAD_SIZE};
