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

//! Metrics and stats handlers for observability endpoints.
//!
//! Provides two endpoints:
//! - `GET /metrics` — Prometheus text format for external monitoring systems.
//! - `GET /api/stats` — JSON format for Web UI widgets.

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use serde::Serialize;

use crate::server::AppState;

/// JSON stats response for Web UI.
#[derive(Debug, Serialize)]
pub struct StatsResponse {
    /// Server uptime in seconds.
    pub uptime_seconds: u64,
    /// Number of buckets.
    pub buckets_count: u64,
    /// Number of objects (excluding delete markers and internal keys).
    pub objects_count: u64,
    /// Total size of stored objects in bytes.
    pub storage_used_bytes: u64,
    /// Number of unique content blobs in the deduplicator.
    pub dedup_unique_blobs: u64,
    /// Total number of content references (including duplicates).
    pub dedup_total_references: u64,
    /// Deduplication ratio (0.0 = no dedup, 1.0 = all content is deduplicated).
    /// Computed as `1.0 - (unique_blobs / total_references)`.
    pub dedup_ratio: f64,
}

/// Handler for `GET /metrics` — Prometheus text format.
///
/// Returns all metrics collected by the `metrics` crate in Prometheus exposition format.
/// Returns 503 if the Prometheus recorder is not initialized.
pub async fn prometheus_metrics(State(state): State<AppState>) -> impl IntoResponse {
    match &state.prometheus_handle {
        Some(handle) => {
            let body = handle.render();
            (
                StatusCode::OK,
                [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
                body,
            )
                .into_response()
        }
        None => (StatusCode::SERVICE_UNAVAILABLE, "Metrics disabled").into_response(),
    }
}

/// Handler for `GET /api/stats` — JSON stats for Web UI.
///
/// Queries the storage engine for current bucket/object counts, storage usage,
/// and deduplication statistics. Also reports server uptime.
pub async fn api_stats(State(state): State<AppState>) -> impl IntoResponse {
    let storage = state.storage.read().await;
    let stats = match storage.get_stats().await {
        Ok(s) => s,
        Err(e) => {
            tracing::error!("Failed to get storage stats: {}", e);
            return (StatusCode::INTERNAL_SERVER_ERROR, "Failed to get stats").into_response();
        }
    };

    let uptime_seconds = state.start_time.elapsed().as_secs();

    let dedup_ratio = if stats.dedup_total_references > 0 {
        1.0 - (stats.dedup_unique_blobs as f64 / stats.dedup_total_references as f64)
    } else {
        0.0
    };

    let response = StatsResponse {
        uptime_seconds,
        buckets_count: stats.buckets_count,
        objects_count: stats.objects_count,
        storage_used_bytes: stats.storage_used_bytes,
        dedup_unique_blobs: stats.dedup_unique_blobs,
        dedup_total_references: stats.dedup_total_references,
        dedup_ratio,
    };

    (StatusCode::OK, axum::Json(response)).into_response()
}
