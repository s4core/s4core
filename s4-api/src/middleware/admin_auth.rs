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
    // Extract Authorization header
    let auth_header = match request.headers().get("authorization") {
        Some(header) => match header.to_str() {
            Ok(s) => s,
            Err(_) => {
                return error_response(StatusCode::BAD_REQUEST, "Invalid Authorization header");
            }
        },
        None => {
            return error_response(StatusCode::UNAUTHORIZED, "Missing Authorization header");
        }
    };

    // Extract Bearer token
    let token = match auth_header.strip_prefix("Bearer ") {
        Some(t) => t,
        None => {
            return error_response(
                StatusCode::BAD_REQUEST,
                "Authorization header must be in format: Bearer <token>",
            );
        }
    };

    // Validate token
    let claims = match state.auth_service.validate_token(token) {
        Ok(claims) => claims,
        Err(e) => {
            warn!("JWT validation failed: {:?}", e);
            return error_response(StatusCode::UNAUTHORIZED, "Invalid or expired token");
        }
    };

    // Insert claims into request extensions for handlers to use
    request.extensions_mut().insert(claims);

    next.run(request).await
}

/// Helper to create error response.
fn error_response(status: StatusCode, message: &str) -> Response {
    let body = Json(json!({
        "error": message,
    }));

    (status, body).into_response()
}
