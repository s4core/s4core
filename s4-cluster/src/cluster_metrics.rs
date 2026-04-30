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

//! Prometheus metrics for the cluster subsystem.
//!
//! All metrics use the `s4_` prefix and follow Prometheus naming conventions.
//! Metrics are recorded via the [`metrics`] crate facade and rendered by
//! the `metrics-exporter-prometheus` recorder installed at startup.

use std::time::Duration;

/// Record a successful quorum write.
pub fn record_quorum_write_ok(latency: Duration) {
    metrics::counter!("s4_quorum_writes_total").increment(1);
    metrics::histogram!("s4_write_latency_seconds").record(latency.as_secs_f64());
}

/// Record a failed quorum write.
pub fn record_quorum_write_failed() {
    metrics::counter!("s4_quorum_writes_total").increment(1);
    metrics::counter!("s4_quorum_writes_failed_total").increment(1);
}

/// Record a successful quorum read.
pub fn record_quorum_read_ok(latency: Duration) {
    metrics::counter!("s4_quorum_reads_total").increment(1);
    metrics::histogram!("s4_read_latency_seconds").record(latency.as_secs_f64());
}

/// Record a failed quorum read.
pub fn record_quorum_read_failed() {
    metrics::counter!("s4_quorum_reads_total").increment(1);
    metrics::counter!("s4_quorum_reads_failed_total").increment(1);
}

/// Record a read repair triggered during a quorum read.
pub fn record_read_repair() {
    metrics::counter!("s4_read_repairs_total").increment(1);
}

/// Record an anti-entropy repair.
pub fn record_anti_entropy_repair() {
    metrics::counter!("s4_anti_entropy_repairs_total").increment(1);
}

/// Update the number of divergent keys found during anti-entropy.
pub fn set_anti_entropy_divergent_keys(count: u64) {
    metrics::gauge!("s4_anti_entropy_divergent_keys").set(count as f64);
}

/// Update repair frontier lag in seconds.
pub fn set_repair_frontier_lag(lag_secs: f64) {
    metrics::gauge!("s4_repair_frontier_lag_seconds").set(lag_secs);
}

/// Update active tombstone count.
pub fn set_tombstones_active(count: u64) {
    metrics::gauge!("s4_tombstones_active").set(count as f64);
}

/// Record tombstones purged.
pub fn record_tombstones_purged(count: u64) {
    metrics::counter!("s4_tombstones_purged_total").increment(count);
}

/// Update GC grace remaining minimum in seconds.
pub fn set_gc_grace_remaining_min(secs: f64) {
    metrics::gauge!("s4_gc_grace_remaining_min_seconds").set(secs);
}

/// Update cluster node counts by status.
pub fn set_cluster_nodes(pool: &str, total: u64, alive: u64, suspect: u64) {
    metrics::gauge!("s4_cluster_nodes_total", "pool" => pool.to_owned()).set(total as f64);
    metrics::gauge!("s4_cluster_nodes_alive", "pool" => pool.to_owned()).set(alive as f64);
    metrics::gauge!("s4_cluster_nodes_suspect", "pool" => pool.to_owned()).set(suspect as f64);
}

/// Update the current topology epoch.
pub fn set_cluster_epoch(epoch: u64) {
    metrics::gauge!("s4_cluster_epoch").set(epoch as f64);
}

/// Update pending hinted handoff count.
pub fn set_hints_pending(count: u64) {
    metrics::gauge!("s4_hints_pending").set(count as f64);
}

/// Record hints delivered.
pub fn record_hints_delivered(count: u64) {
    metrics::counter!("s4_hints_delivered_total").increment(count);
}

/// Record a LWW conflict resolution.
pub fn record_lww_resolution() {
    metrics::counter!("s4_lww_resolutions_total").increment(1);
}

/// Update scrubber scan progress (0.0 .. 1.0).
pub fn set_scrubber_scan_progress(progress: f64) {
    metrics::gauge!("s4_scrubber_scan_progress").set(progress);
}

/// Record scrubber corruption found.
pub fn record_scrubber_corruption_found() {
    metrics::counter!("s4_scrubber_corruptions_found_total").increment(1);
}

/// Record scrubber corruption healed.
pub fn record_scrubber_corruption_healed() {
    metrics::counter!("s4_scrubber_corruptions_healed_total").increment(1);
}

/// Record scrubber blobs scanned.
pub fn record_scrubber_blobs_scanned(count: u64) {
    metrics::counter!("s4_scrubber_blobs_scanned_total").increment(count);
}

// ---------------------------------------------------------------------------
// Cluster health snapshot (for Admin API)
// ---------------------------------------------------------------------------

use serde::{Deserialize, Serialize};

/// Aggregated cluster health status for the Admin API.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterHealthStatus {
    /// Overall cluster health.
    pub status: HealthLevel,
    /// Total number of nodes across all pools.
    pub nodes_total: u64,
    /// Number of alive nodes.
    pub nodes_alive: u64,
    /// Number of suspect nodes.
    pub nodes_suspect: u64,
    /// Number of dead nodes.
    pub nodes_dead: u64,
    /// Current topology epoch.
    pub epoch: u64,
    /// Protocol version of this node.
    pub protocol_version: u32,
    /// S4 server version of this node.
    pub s4_version: String,
}

/// High-level health classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HealthLevel {
    /// All nodes alive, quorum easily met.
    Healthy,
    /// Some nodes suspect or dead, but quorum is still met.
    Degraded,
    /// Quorum cannot be met — writes/reads may fail.
    Critical,
}

/// Per-node health information for the Admin API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeHealthInfo {
    /// Node identifier.
    pub node_id: String,
    /// Node human-readable name.
    pub node_name: String,
    /// Current status.
    pub status: String,
    /// Pool this node belongs to.
    pub pool_id: u32,
    /// Protocol version.
    pub protocol_version: u32,
    /// S4 server version.
    pub s4_version: String,
    /// gRPC address.
    pub grpc_addr: String,
    /// HTTP address.
    pub http_addr: String,
}

/// Repair status for the Admin API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepairStatus {
    /// Number of divergent keys found in the last anti-entropy round.
    pub divergent_keys: u64,
    /// Repair frontier lag in seconds.
    pub repair_frontier_lag_secs: f64,
    /// Total repairs performed.
    pub total_repairs: u64,
    /// Last repair timestamp (ISO 8601).
    pub last_repair: Option<String>,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn health_level_serialization() {
        assert_eq!(
            serde_json::to_string(&HealthLevel::Healthy).unwrap(),
            "\"healthy\""
        );
        assert_eq!(
            serde_json::to_string(&HealthLevel::Degraded).unwrap(),
            "\"degraded\""
        );
        assert_eq!(
            serde_json::to_string(&HealthLevel::Critical).unwrap(),
            "\"critical\""
        );
    }

    #[test]
    fn cluster_health_status_round_trip() {
        let status = ClusterHealthStatus {
            status: HealthLevel::Healthy,
            nodes_total: 3,
            nodes_alive: 3,
            nodes_suspect: 0,
            nodes_dead: 0,
            epoch: 5,
            protocol_version: 1,
            s4_version: "0.2.0".to_string(),
        };
        let json = serde_json::to_string(&status).unwrap();
        let restored: ClusterHealthStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(status, restored);
    }

    #[test]
    fn repair_status_serialization() {
        let status = RepairStatus {
            divergent_keys: 0,
            repair_frontier_lag_secs: 120.0,
            total_repairs: 500,
            last_repair: Some("2026-03-22T10:00:00Z".to_string()),
        };
        let json = serde_json::to_string(&status).unwrap();
        assert!(json.contains("divergent_keys"));
        assert!(json.contains("500"));
    }

    #[test]
    fn node_health_info_serialization() {
        let info = NodeHealthInfo {
            node_id: "abc-123".to_string(),
            node_name: "node-1".to_string(),
            status: "alive".to_string(),
            pool_id: 1,
            protocol_version: 1,
            s4_version: "0.2.0".to_string(),
            grpc_addr: "10.0.1.1:9100".to_string(),
            http_addr: "10.0.1.1:9000".to_string(),
        };
        let json = serde_json::to_string(&info).unwrap();
        assert!(json.contains("node-1"));
    }

    #[test]
    fn metrics_functions_do_not_panic() {
        // These just call the metrics facade which is a no-op without a recorder.
        record_quorum_write_ok(Duration::from_millis(5));
        record_quorum_write_failed();
        record_quorum_read_ok(Duration::from_millis(2));
        record_quorum_read_failed();
        record_read_repair();
        record_anti_entropy_repair();
        set_anti_entropy_divergent_keys(0);
        set_repair_frontier_lag(120.0);
        set_tombstones_active(5000);
        record_tombstones_purged(100);
        set_gc_grace_remaining_min(600000.0);
        set_cluster_nodes("pool-1", 3, 3, 0);
        set_cluster_epoch(5);
        set_hints_pending(0);
        record_hints_delivered(10);
        record_lww_resolution();
        set_scrubber_scan_progress(0.45);
        record_scrubber_corruption_found();
        record_scrubber_corruption_healed();
        record_scrubber_blobs_scanned(1000);
    }
}
