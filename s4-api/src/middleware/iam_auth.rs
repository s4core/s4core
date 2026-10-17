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

use crate::auth::signature_v2::{
    detect_v2_auth, verify_signature_v2, verify_signature_v2_with_iam_data, SignatureV2RequestData,
};
use crate::auth::signature_v4::{
    verify_signature_v4, verify_signature_v4_with_iam_data, SignatureRequestData,
};
use crate::auth::Credentials;
use crate::s3::errors::S3Error;
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

    // POST object uploads use multipart/form-data with policy-based auth
    // inside the form body. They don't have Authorization header or presigned
    // query params, so we must let them through here — auth is done in the handler.
    let is_post_object_upload = request.method() == axum::http::Method::POST
        && request
            .headers()
            .get(axum::http::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.starts_with("multipart/form-data"))
            .unwrap_or(false);

    if is_post_object_upload {
        return next.run(request).await;
    }

    // Get IAM storage (this is Arc, so cheap to clone)
    let iam_storage = state.iam_storage.clone();

    // Detect signature version: V2 first, then V4 (default).
    if detect_v2_auth(&request) {
        // ---- AWS Signature V2 path ----
        let v2_data = match SignatureV2RequestData::from_request(&request) {
            Ok(data) => data,
            Err(e) => {
                warn!("Failed to extract V2 request data: {:?}", e);
                return error_response(&e);
            }
        };

        // Try IAM first, then fallback to ENV credentials
        match verify_signature_v2_with_iam_data(v2_data, &iam_storage).await {
            Ok(user) => {
                request.extensions_mut().insert::<User>(user);
                return next.run(request).await;
            }
            Err(e) => {
                if falls_back_to_environment(&e) {
                    debug!("V2 IAM lookup failed, trying legacy ENV credentials");

                    if !state.access_key_id.is_empty() && !state.secret_access_key.is_empty() {
                        let credentials = Credentials::new(
                            state.access_key_id.clone(),
                            state.secret_access_key.clone(),
                        );

                        match verify_signature_v2(&request, &credentials) {
                            Ok(()) => {
                                debug!("V2 legacy ENV authentication successful");
                                let legacy_user = create_legacy_user(&state);
                                request.extensions_mut().insert::<User>(legacy_user);
                                return next.run(request).await;
                            }
                            Err(legacy_err) => {
                                warn!("V2 legacy ENV authentication also failed: {:?}", legacy_err);
                                return error_response(&answer_of(e, legacy_err));
                            }
                        }
                    }
                }

                warn!("V2 IAM authentication failed: {:?}", e);
                return error_response(&e);
            }
        }
    }

    // ---- AWS Signature V4 path (unchanged) ----

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

    // Try IAM authentication first
    match verify_signature_v4_with_iam_data(req_data, &iam_storage).await {
        Ok(user) => {
            // Store authenticated user in request extensions for permission checks
            request.extensions_mut().insert::<User>(user);
            next.run(request).await
        }
        Err(e) => {
            // If access key not found in IAM, or IAM credentials are not checked
            // on this node now, try legacy ENV-based authentication
            if falls_back_to_environment(&e) {
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
                            let legacy_user = create_legacy_user(&state);
                            request.extensions_mut().insert::<User>(legacy_user);
                            return next.run(request).await;
                        }
                        Err(legacy_err) => {
                            warn!("Legacy ENV authentication also failed: {:?}", legacy_err);
                            return error_response(&answer_of(e, legacy_err));
                        }
                    }
                }
            }

            warn!("IAM authentication failed: {:?}", e);
            error_response(&e)
        }
    }
}

/// Whether a request IAM did not authenticate is tried against the S3
/// credentials of the environment: its key is not an IAM user's, or IAM
/// credentials are not checked on this node now (plan
/// CLUSTER-IAM-REPLICATION.md, decision 4) — the credentials of the
/// environment are not IAM's, and work while the node confirms its copy of
/// IAM.
fn falls_back_to_environment(iam_error: &S3Error) -> bool {
    matches!(
        iam_error,
        S3Error::AccessDenied | S3Error::ServiceUnavailable(_)
    )
}

/// What a request that neither IAM nor the environment authenticated is
/// answered: unavailable when IAM could not check it now and its key is not
/// the environment's — it may be an IAM key, which another node, or this one
/// a moment later, checks — and what the environment's check found
/// otherwise: a wrong signature with the environment's key is a wrong
/// signature, whatever IAM's state.
fn answer_of(iam_error: S3Error, environment_error: S3Error) -> S3Error {
    match (&iam_error, &environment_error) {
        (S3Error::ServiceUnavailable(_), S3Error::AccessDenied) => iam_error,
        _ => environment_error,
    }
}

/// Creates a synthetic SuperUser for legacy ENV-based authentication.
fn create_legacy_user(state: &AppState) -> User {
    User {
        id: "legacy-env-user".to_string(),
        username: "env-admin".to_string(),
        password_hash: String::new(),
        role: Role::SuperUser,
        access_key: Some(state.access_key_id.clone()),
        secret_key: None,
        secret_key_sealed: None,
        secret_key_hash: None,
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
        is_active: true,
        credential_version: 0,
        created_epoch: 0,
    }
}

/// Helper to create XML error response.
fn error_response(e: &S3Error) -> Response {
    let status = e.status_code();
    let xml_body = crate::s3::xml::error_response(
        e.code(),
        &e.to_string(),
        "",
        &uuid::Uuid::new_v4().to_string(),
    );

    let mut builder = Response::builder().status(status).header("content-type", "application/xml");
    if matches!(e, S3Error::ServiceUnavailable(_)) {
        builder = builder.header(axum::http::header::RETRY_AFTER, "1");
    }
    builder.body(axum::body::Body::from(xml_body)).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_key_iam_does_not_check_now_is_tried_against_the_environment() {
        assert!(falls_back_to_environment(&S3Error::AccessDenied));
        assert!(falls_back_to_environment(&S3Error::ServiceUnavailable(
            "starting".into()
        )));
        assert!(!falls_back_to_environment(&S3Error::SignatureDoesNotMatch));
    }

    /// A key the environment does not hold is answered 503 while IAM could
    /// not check it; anything else as the environment's check found.
    #[test]
    fn a_key_neither_checks_is_unavailable_only_when_iam_could_not() {
        assert!(matches!(
            answer_of(
                S3Error::ServiceUnavailable("starting".into()),
                S3Error::AccessDenied
            ),
            S3Error::ServiceUnavailable(_)
        ));
        assert!(matches!(
            answer_of(S3Error::AccessDenied, S3Error::SignatureDoesNotMatch),
            S3Error::SignatureDoesNotMatch
        ));
        // The environment's key with a wrong signature: not IAM's to judge.
        assert!(matches!(
            answer_of(
                S3Error::ServiceUnavailable("starting".into()),
                S3Error::SignatureDoesNotMatch
            ),
            S3Error::SignatureDoesNotMatch
        ));
        let response = error_response(&S3Error::ServiceUnavailable("starting".into()));
        assert_eq!(
            response.status(),
            axum::http::StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(response.headers()[axum::http::header::RETRY_AFTER], "1");
    }
}
