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

//! HTTP middleware.

pub mod admin_auth;
pub mod auth;
pub mod aws_chunked;
pub mod iam_auth;
pub mod logging;
pub mod metrics;

pub use admin_auth::admin_auth_middleware;
pub use auth::auth_middleware;
pub use aws_chunked::{decode_aws_chunked, is_aws_chunked, validate_decoded_content_length};
pub use iam_auth::iam_auth_middleware;
pub use logging::logging_middleware;
pub use metrics::metrics_middleware;
