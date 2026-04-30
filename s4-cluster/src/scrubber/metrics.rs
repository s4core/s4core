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

//! Scrubber metrics for monitoring bit rot detection and healing.

use std::sync::atomic::{AtomicU64, Ordering};

/// Metrics emitted by the background data scrubber.
///
/// All counters are monotonically increasing across scrub cycles
/// (except `scan_progress_pct` and `last_full_scan_timestamp`).
#[derive(Debug, Default)]
pub struct ScrubberMetrics {
    /// Total blobs scanned across all cycles.
    pub blobs_scanned_total: AtomicU64,
    /// CRC32 corruptions detected.
    pub corruptions_found_total: AtomicU64,
    /// Corruptions successfully healed from replicas.
    pub corruptions_healed_total: AtomicU64,
    /// Corruptions that could not be healed (no healthy replica).
    pub corruptions_unrecoverable_total: AtomicU64,
    /// Current scan cycle progress (0..100).
    pub scan_progress_pct: AtomicU64,
    /// Unix timestamp (seconds) of the last completed full scan.
    pub last_full_scan_timestamp: AtomicU64,
    /// Total number of completed full scan cycles.
    pub cycles_completed: AtomicU64,
    /// Total volumes scanned across all cycles.
    pub volumes_scanned_total: AtomicU64,
    /// Total bytes scanned across all cycles.
    pub bytes_scanned_total: AtomicU64,
}

/// Point-in-time snapshot of scrubber metrics.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ScrubberMetricsSnapshot {
    /// Total blobs scanned across all cycles.
    pub blobs_scanned_total: u64,
    /// CRC32 corruptions detected.
    pub corruptions_found_total: u64,
    /// Corruptions successfully healed from replicas.
    pub corruptions_healed_total: u64,
    /// Corruptions that could not be healed.
    pub corruptions_unrecoverable_total: u64,
    /// Current scan cycle progress (0..100).
    pub scan_progress_pct: u64,
    /// Unix timestamp (seconds) of last completed full scan.
    pub last_full_scan_timestamp: u64,
    /// Total completed full scan cycles.
    pub cycles_completed: u64,
    /// Total volumes scanned.
    pub volumes_scanned_total: u64,
    /// Total bytes scanned.
    pub bytes_scanned_total: u64,
}

impl ScrubberMetrics {
    /// Takes a consistent snapshot of all metrics.
    pub fn snapshot(&self) -> ScrubberMetricsSnapshot {
        ScrubberMetricsSnapshot {
            blobs_scanned_total: self.blobs_scanned_total.load(Ordering::Relaxed),
            corruptions_found_total: self.corruptions_found_total.load(Ordering::Relaxed),
            corruptions_healed_total: self.corruptions_healed_total.load(Ordering::Relaxed),
            corruptions_unrecoverable_total: self
                .corruptions_unrecoverable_total
                .load(Ordering::Relaxed),
            scan_progress_pct: self.scan_progress_pct.load(Ordering::Relaxed),
            last_full_scan_timestamp: self.last_full_scan_timestamp.load(Ordering::Relaxed),
            cycles_completed: self.cycles_completed.load(Ordering::Relaxed),
            volumes_scanned_total: self.volumes_scanned_total.load(Ordering::Relaxed),
            bytes_scanned_total: self.bytes_scanned_total.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_metrics_are_zero() {
        let m = ScrubberMetrics::default();
        let snap = m.snapshot();
        assert_eq!(snap.blobs_scanned_total, 0);
        assert_eq!(snap.corruptions_found_total, 0);
        assert_eq!(snap.corruptions_healed_total, 0);
        assert_eq!(snap.corruptions_unrecoverable_total, 0);
        assert_eq!(snap.scan_progress_pct, 0);
        assert_eq!(snap.last_full_scan_timestamp, 0);
        assert_eq!(snap.cycles_completed, 0);
    }

    #[test]
    fn snapshot_reflects_updates() {
        let m = ScrubberMetrics::default();
        m.blobs_scanned_total.store(42, Ordering::Relaxed);
        m.corruptions_found_total.store(2, Ordering::Relaxed);
        m.corruptions_healed_total.store(1, Ordering::Relaxed);

        let snap = m.snapshot();
        assert_eq!(snap.blobs_scanned_total, 42);
        assert_eq!(snap.corruptions_found_total, 2);
        assert_eq!(snap.corruptions_healed_total, 1);
    }
}
