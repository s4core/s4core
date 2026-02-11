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

//! Metrics middleware for recording HTTP request metrics.

use axum::extract::Request;
use axum::middleware::Next;
use axum::response::Response;
use std::time::Instant;

/// Middleware that records HTTP request count and latency metrics.
///
/// Records two metrics via the `metrics` crate:
/// - `http_requests_total` (counter): incremented per request, labeled by method and status.
/// - `http_request_duration_seconds` (histogram): request latency, labeled by method.
///
/// Path labels are normalized to avoid high cardinality (bucket names and object keys
/// are replaced with placeholders).
pub async fn metrics_middleware(request: Request, next: Next) -> Response {
    let method = request.method().to_string();
    let raw_path = request.uri().path().to_string();
    let start = Instant::now();

    let response = next.run(request).await;

    let duration = start.elapsed().as_secs_f64();
    let status = response.status().as_u16().to_string();
    let path = normalize_path(&raw_path);

    metrics::counter!("http_requests_total", "method" => method.clone(), "status" => status, "path" => path.clone());
    metrics::histogram!("http_request_duration_seconds", "method" => method, "path" => path)
        .record(duration);

    response
}

/// Normalizes request paths to avoid label cardinality explosion.
///
/// - `/` → `/`
/// - `/metrics` → `/metrics`
/// - `/api/admin/...` → kept as-is
/// - `/api/stats` → `/api/stats`
/// - `/{bucket}` → `/{bucket}`
/// - `/{bucket}/{key...}` → `/{bucket}/{key}`
fn normalize_path(path: &str) -> String {
    if path == "/" || path == "/metrics" || path == "/api/stats" {
        return path.to_string();
    }
    if path.starts_with("/api/admin") {
        return "/api/admin".to_string();
    }

    // S3 paths: /{bucket} or /{bucket}/{key...}
    let trimmed = path.trim_start_matches('/');
    match trimmed.find('/') {
        None => "/{bucket}".to_string(),
        Some(_) => "/{bucket}/{key}".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_path_root() {
        assert_eq!(normalize_path("/"), "/");
    }

    #[test]
    fn test_normalize_path_metrics() {
        assert_eq!(normalize_path("/metrics"), "/metrics");
    }

    #[test]
    fn test_normalize_path_stats() {
        assert_eq!(normalize_path("/api/stats"), "/api/stats");
    }

    #[test]
    fn test_normalize_path_admin() {
        assert_eq!(normalize_path("/api/admin/users"), "/api/admin");
        assert_eq!(normalize_path("/api/admin/login"), "/api/admin");
    }

    #[test]
    fn test_normalize_path_bucket() {
        assert_eq!(normalize_path("/my-bucket"), "/{bucket}");
    }

    #[test]
    fn test_normalize_path_object() {
        assert_eq!(normalize_path("/my-bucket/some/key.txt"), "/{bucket}/{key}");
    }
}
