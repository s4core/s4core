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

//! Authentication middleware.

use axum::extract::Request;
use axum::extract::State;
use axum::middleware::Next;
use axum::response::Response;
use tracing::warn;

use crate::auth::signature_v4::verify_signature_v4;
use crate::auth::Credentials;
use crate::server::AppState;

/// Authentication middleware that verifies AWS Signature V4.
///
/// For development/testing, you can bypass auth by setting
/// `S4_DISABLE_AUTH=1` environment variable.
///
/// This middleware supports two modes:
/// 1. **IAM mode** (default): Uses IAM database for authentication and authorization
/// 2. **Legacy mode**: Uses ENV-based credentials (S4_ACCESS_KEY_ID + S4_SECRET_ACCESS_KEY)
///
/// The authenticated user is stored in request extensions for permission checks in handlers.
pub async fn auth_middleware(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Response {
    // Check if auth is disabled (for development/testing)
    let auth_disabled =
        std::env::var("S4_DISABLE_AUTH").unwrap_or_default().parse::<u8>().unwrap_or(0) == 1;

    if auth_disabled {
        return next.run(request).await;
    }

    // Extract credentials from AppState
    let credentials =
        Credentials::new(state.access_key_id.clone(), state.secret_access_key.clone());

    // Verify signature
    match verify_signature_v4(&request, &credentials) {
        Ok(()) => next.run(request).await,
        Err(e) => {
            warn!("Authentication failed: {:?}", e);
            let status = e.status_code();
            let xml_body = crate::s3::xml::error_response(
                e.code(),
                &e.to_string(),
                "",
                &uuid::Uuid::new_v4().to_string(),
            );

            Response::builder()
                .status(status)
                .header("content-type", "application/xml")
                .body(axum::body::Body::from(xml_body))
                .unwrap()
        }
    }
}
