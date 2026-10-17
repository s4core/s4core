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

//! Public readiness of this node, from already-published in-memory state.

use axum::{
    extract::State,
    http::{
        header::{CACHE_CONTROL, RETRY_AFTER},
        HeaderValue, StatusCode,
    },
    response::{IntoResponse, Response},
};
use s4_cluster::{ConfigAdoptionPhase, IamRefusal};

use crate::server::AppState;

/// Answers GET and HEAD `/minio/health/ready` without storage or peer calls.
///
/// Recovery gates cover local bucket configurations, migration and IAM. The
/// pool's overall health and background metadata movement do not determine
/// whether this recovered node may serve requests. Public reasons are fixed;
/// detailed adoption diagnostics remain in the protected cluster health API.
pub async fn readiness(State(state): State<AppState>) -> Response {
    let mut response = match pending_reason(&state) {
        Some(reason) => (
            StatusCode::SERVICE_UNAVAILABLE,
            [(RETRY_AFTER.as_str(), "1"), ("x-s4-readiness", reason)],
        )
            .into_response(),
        None => StatusCode::OK.into_response(),
    };
    response
        .headers_mut()
        .insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

fn pending_reason(state: &AppState) -> Option<&'static str> {
    // Single and gateway modes publish no cluster health and retain their
    // readiness without waiting for cluster recovery gates.
    let health = state.cluster_admin_service.cluster_health()?;
    match health.bucket_config_adoption.as_ref().map(|snapshot| snapshot.phase) {
        None | Some(ConfigAdoptionPhase::Starting) => return Some("bucket_config_starting"),
        Some(ConfigAdoptionPhase::Pending) => return Some("bucket_config_pending"),
        Some(ConfigAdoptionPhase::Ready) => {}
    }
    if health.migration_pending_records != 0 {
        return Some("migration_pending");
    }
    if !state.iam_storage.changes_open()
        || health.iam_adoption.as_ref().is_some_and(|adoption| adoption.pending)
    {
        return Some("iam_adoption_pending");
    }
    // The same Arc drives credential admission, but checking readiness must
    // never increment the credential-refusal counter.
    match state.iam_freshness.as_ref().map(|freshness| freshness.check()) {
        Some(Err(IamRefusal::Starting)) => Some("iam_starting"),
        Some(Err(IamRefusal::Stale)) => Some("iam_stale"),
        _ => None,
    }
}
