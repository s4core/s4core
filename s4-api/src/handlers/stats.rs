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
/// What `GET /api/stats` answers. On a cluster node the bucket and object
/// numbers are the pool's, counted at most a few seconds ago, while the
/// deduplication numbers are the node's own: each node deduplicates what it
/// stores.
#[derive(Debug, Serialize)]
pub struct StatsResponse {
    /// Server uptime in seconds.
    pub uptime_seconds: u64,
    /// Number of buckets.
    pub buckets_count: u64,
    /// Number of objects (excluding delete markers and internal keys).
    pub objects_count: u64,
    /// Total size of stored objects in bytes, before deduplication and
    /// replication.
    pub storage_used_bytes: u64,
    /// Number of unique content blobs in this node's deduplicator.
    pub dedup_unique_blobs: u64,
    /// Total number of content references on this node (including duplicates).
    pub dedup_total_references: u64,
    /// Deduplication ratio (0.0 = no dedup, 1.0 = all content is deduplicated).
    /// Computed as `1.0 - (unique_blobs / total_references)`.
    pub dedup_ratio: f64,
    /// False when a node of the pool did not answer a listing, so that the
    /// bucket and object numbers miss what only it holds. Always true on a
    /// single node.
    pub complete: bool,
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
    let uptime_seconds = state.start_time.elapsed().as_secs();

    // Cluster mode: the pool's buckets and objects, this node's deduplication.
    let (buckets_count, objects_count, storage_used_bytes, unique, references, complete) = if state
        .list_coordinator
        .is_some()
    {
        let usage = match super::usage::usage(&state).await {
            Ok(usage) => usage,
            Err(super::usage::UsageError::Cluster(e)) => {
                tracing::error!("Failed to count the pool's objects: {}", e);
                return super::cluster_error_to_response(&e);
            }
            Err(super::usage::UsageError::Storage(e)) => {
                tracing::error!("Failed to count the pool's objects: {}", e);
                return (StatusCode::INTERNAL_SERVER_ERROR, "Failed to get stats").into_response();
            }
        };
        let dedup = state.storage.read().await.dedup_stats().await;
        let (unique, references) = match dedup {
            Ok(dedup) => dedup,
            Err(e) => {
                tracing::error!("Failed to get deduplication stats: {}", e);
                return (StatusCode::INTERNAL_SERVER_ERROR, "Failed to get stats").into_response();
            }
        };
        (
            usage.buckets.len() as u64,
            usage.objects_count(),
            usage.storage_used_bytes(),
            unique,
            references,
            usage.complete,
        )
    } else {
        let storage = state.storage.read().await;
        let stats = match storage.get_stats().await {
            Ok(s) => s,
            Err(e) => {
                tracing::error!("Failed to get storage stats: {}", e);
                return (StatusCode::INTERNAL_SERVER_ERROR, "Failed to get stats").into_response();
            }
        };
        (
            stats.buckets_count,
            stats.objects_count,
            stats.storage_used_bytes,
            stats.dedup_unique_blobs,
            stats.dedup_total_references,
            true,
        )
    };

    let dedup_ratio = if references > 0 {
        1.0 - (unique as f64 / references as f64)
    } else {
        0.0
    };

    let response = StatsResponse {
        uptime_seconds,
        buckets_count,
        objects_count,
        storage_used_bytes,
        dedup_unique_blobs: unique,
        dedup_total_references: references,
        dedup_ratio,
        complete,
    };

    (StatusCode::OK, axum::Json(response)).into_response()
}
