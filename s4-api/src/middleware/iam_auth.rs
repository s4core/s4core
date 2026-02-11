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

//! IAM-based authentication middleware for S3 API.

use axum::extract::Request;
use axum::extract::State;
use axum::middleware::Next;
use axum::response::Response;
use s4_features::iam::{Role, User};
use tracing::{debug, warn};

use crate::auth::signature_v4::{
    verify_signature_v4, verify_signature_v4_with_iam_data, SignatureRequestData,
};
use crate::auth::Credentials;
use crate::server::AppState;

/// IAM authentication middleware that verifies AWS Signature V4.
///
/// This middleware supports two authentication modes:
///
/// 1. **IAM mode** (primary): Looks up users in IAM database by access key
/// 2. **Legacy mode** (fallback): Uses ENV-based credentials (S4_ACCESS_KEY_ID + S4_SECRET_ACCESS_KEY)
///
/// The middleware tries IAM first, and if the access key is not found in IAM,
/// it falls back to legacy ENV credentials for backwards compatibility.
///
/// For development/testing, you can bypass auth by setting
/// `S4_DISABLE_AUTH=1` environment variable.
pub async fn iam_auth_middleware(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Response {
    // Shadow request as mutable for extensions
    let mut request = request;

    // Check if auth is disabled (for development/testing)
    let auth_disabled =
        std::env::var("S4_DISABLE_AUTH").unwrap_or_default().parse::<u8>().unwrap_or(0) == 1;

    if auth_disabled {
        return next.run(request).await;
    }

    // Extract all request data SYNCHRONOUSLY before any async operations.
    // This avoids holding borrows across await points, which would make
    // the future !Send and break axum middleware compatibility.
    let req_data = match SignatureRequestData::from_request(&request) {
        Ok(data) => data,
        Err(e) => {
            warn!("Failed to extract request data: {:?}", e);
            return error_response(&e);
        }
    };

    // Get IAM storage (this is Arc, so cheap to clone)
    let iam_storage = state.iam_storage.clone();

    // Try IAM authentication first
    match verify_signature_v4_with_iam_data(req_data, &iam_storage).await {
        Ok(user) => {
            // Store authenticated user in request extensions for permission checks
            request.extensions_mut().insert::<User>(user);
            next.run(request).await
        }
        Err(e) => {
            // If access key not found in IAM, try legacy ENV-based authentication
            if matches!(e, crate::s3::errors::S3Error::AccessDenied) {
                debug!("IAM lookup failed, trying legacy ENV credentials");

                // Check if legacy credentials are configured
                if !state.access_key_id.is_empty() && !state.secret_access_key.is_empty() {
                    let credentials = Credentials::new(
                        state.access_key_id.clone(),
                        state.secret_access_key.clone(),
                    );

                    // Verify with legacy credentials
                    match verify_signature_v4(&request, &credentials) {
                        Ok(()) => {
                            debug!("Legacy ENV authentication successful");
                            // Create a synthetic SuperUser for legacy auth
                            // This maintains backwards compatibility
                            let legacy_user = User {
                                id: "legacy-env-user".to_string(),
                                username: "env-admin".to_string(),
                                password_hash: String::new(),
                                role: Role::SuperUser,
                                access_key: Some(state.access_key_id.clone()),
                                secret_key: None,
                                secret_key_hash: None,
                                created_at: chrono::Utc::now(),
                                updated_at: chrono::Utc::now(),
                                is_active: true,
                            };
                            request.extensions_mut().insert::<User>(legacy_user);
                            return next.run(request).await;
                        }
                        Err(legacy_err) => {
                            warn!("Legacy ENV authentication also failed: {:?}", legacy_err);
                            return error_response(&legacy_err);
                        }
                    }
                }
            }

            warn!("IAM authentication failed: {:?}", e);
            error_response(&e)
        }
    }
}

/// Helper to create XML error response.
fn error_response(e: &crate::s3::errors::S3Error) -> Response {
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
