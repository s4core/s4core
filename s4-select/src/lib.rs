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

//! S4 Select — SQL query engine for S4 object storage.
//!
//! Provides two query modes:
//! - **S3 Select compatible**: Single-object SQL queries via `POST /{bucket}/{key}?select`
//! - **S4 Extended SQL**: Multi-object queries via `POST /{bucket}?sql`
//!
//! Built on a custom lightweight SQL engine using
//! [`sqlparser`](https://docs.rs/sqlparser) for parsing and Arrow compute
//! kernels for vectorized evaluation.
//!
//! # Supported formats
//! - CSV (with configurable headers, delimiters, quoting)
//! - JSON (Lines and Document modes)

pub mod config;
pub mod engine;
pub mod error;
pub mod eval;
pub mod formats;
pub mod functions;
pub mod multi_object;
pub mod planner;
pub mod request;
pub mod response;
pub mod sql_validator;

pub use config::QueryConfig;
pub use engine::SelectEngine;
pub use error::SelectError;
pub use formats::{InputFormat, OutputFormat};
pub use request::SelectRequest;
