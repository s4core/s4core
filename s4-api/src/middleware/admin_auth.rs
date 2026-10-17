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

//! JWT authentication middleware for Admin API.

use axum::{
    extract::{Request, State},
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use s4_features::iam::IamError;
use serde_json::json;
use tracing::error;

use crate::AppState;

/// JWT authentication middleware for Admin API endpoints.
///
/// Extracts the JWT from the `Authorization: Bearer` header, checks it against
/// its account (signature, expiry, the account exists, is active and still has
/// the token's credential version) and inserts the [`AdminPrincipal`] — the
/// account's current name and role — into the request extensions.
///
/// # Usage
///
/// ```ignore
/// async fn handler(Extension(principal): Extension<AdminPrincipal>) -> impl IntoResponse {
///     // principal.role is the role the account has now, not the token's
/// }
/// ```
///
/// [`AdminPrincipal`]: s4_features::iam::AdminPrincipal
pub async fn admin_auth_middleware(
    State(state): State<AppState>,
    mut request: Request,
    next: Next,
) -> Response {
    // Every failure below is one and the same thing — the request carries no
    // credentials this API accepts — so all of them answer 401. A malformed
    // header is not a malformed request: 400 would tell the caller to fix its
    // body when what it has to do is log in again.

    // Extract Authorization header
    let auth_header = match request.headers().get("authorization") {
        Some(header) => match header.to_str() {
            Ok(s) => s,
            Err(_) => {
                return unauthorized("Invalid Authorization header");
            }
        },
        None => {
            return unauthorized("Missing Authorization header");
        }
    };

    // Extract Bearer token
    let token = match auth_header.strip_prefix("Bearer ") {
        Some(t) => t,
        None => {
            return unauthorized("Authorization header must be in format: Bearer <token>");
        }
    };

    // Check the token against its account
    let principal = match state.auth_service.authenticate(token).await {
        Ok(principal) => principal,
        Err(IamError::InvalidToken) => {
            return unauthorized("Invalid or expired token");
        }
        Err(error @ IamError::NotServing(_)) => {
            // This node does not check the tokens of IAM users now: another
            // node, or this one a moment later, does. Root's tokens work here.
            let body = Json(json!({
                "error": error.to_string(),
                "code": "ServiceUnavailable",
            }));
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                [(axum::http::header::RETRY_AFTER, "1")],
                body,
            )
                .into_response();
        }
        Err(e) => {
            // The account could not be read: the token may be fine, so this is
            // a fault of the server, not a reason to send the caller to log in.
            error!("Admin token check failed: {e}");
            let body = Json(json!({
                "error": "The account of this token could not be read",
                "code": "InternalError",
            }));
            return (StatusCode::INTERNAL_SERVER_ERROR, body).into_response();
        }
    };

    request.extensions_mut().insert(principal);

    next.run(request).await
}

/// Builds a 401 in the same shape the admin handlers use, so a client can read
/// `code` here exactly as it does on every other admin response.
fn unauthorized(message: &str) -> Response {
    let body = Json(json!({
        "error": message,
        "code": "Unauthorized",
    }));

    (StatusCode::UNAUTHORIZED, body).into_response()
}
