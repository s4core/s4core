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

//! S4 Advanced Features
//!
//! This crate provides advanced features for S4:
//! - Atomic directory operations
//! - Extended metadata
//! - IAM and permissions
//! - CORS (Cross-Origin Resource Sharing)
//! - Object versioning
//! - Lifecycle policies
//! - Object Lock (WORM storage with retention periods)

pub mod atomic;
pub mod cors;
pub mod iam;
pub mod lifecycle;
pub mod metadata;
pub mod object_lock;

pub use atomic::*;
pub use cors::*;
pub use iam::*;
pub use lifecycle::*;
pub use metadata::*;
pub use object_lock::*;
