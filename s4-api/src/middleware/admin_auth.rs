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
use serde_json::json;
use tracing::warn;

use crate::AppState;

/// JWT authentication middleware for Admin API endpoints.
///
/// Extracts JWT token from Authorization: Bearer header,
/// validates it, and inserts claims into request extensions.
///
/// # Usage
///
/// Protected routes will receive JwtClaims as an extractor:
///
/// ```ignore
/// async fn handler(claims: JwtClaims) -> impl IntoResponse {
///     // claims.role, claims.username, etc. are available
/// }
/// ```
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

    // Validate token
    let claims = match state.auth_service.validate_token(token) {
        Ok(claims) => claims,
        Err(e) => {
            warn!("JWT validation failed: {:?}", e);
            return unauthorized("Invalid or expired token");
        }
    };

    // Insert claims into request extensions for handlers to use
    request.extensions_mut().insert(claims);

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
