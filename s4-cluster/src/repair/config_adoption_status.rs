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

//! Local readiness while legacy bucket configurations are being adopted.

use serde::{Deserialize, Serialize};
use std::sync::{Arc, RwLock};

/// The local node's progress adopting its own bucket configurations.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConfigAdoptionPhase {
    /// The first adoption pass has not completed.
    #[default]
    Starting,
    /// Some configurations still need adoption.
    Pending,
    /// Every local configuration has been adopted.
    Ready,
}

/// A consistent view of the local adoption progress.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigAdoptionSnapshot {
    /// Current adoption phase.
    pub phase: ConfigAdoptionPhase,
    /// Number still pending, when a pass could determine it.
    pub pending_records: Option<u64>,
    /// A bounded sample of reasons published by the adoption worker.
    pub reasons: Vec<String>,
}

/// Shared local adoption progress for readiness and administration.
#[derive(Debug, Clone, Default)]
pub struct ConfigAdoptionStatus(Arc<RwLock<ConfigAdoptionSnapshot>>);

impl ConfigAdoptionStatus {
    /// Returns a consistent snapshot.
    pub fn snapshot(&self) -> ConfigAdoptionSnapshot {
        self.0.read().unwrap_or_else(|p| p.into_inner()).clone()
    }

    /// Publishes a pending pass; the publisher bounds the reason sample.
    pub fn waiting(&self, pending_records: Option<u64>, reasons: Vec<String>) {
        *self.0.write().unwrap_or_else(|p| p.into_inner()) = ConfigAdoptionSnapshot {
            phase: ConfigAdoptionPhase::Pending,
            pending_records,
            reasons,
        };
    }

    /// Publishes that every local configuration has been adopted.
    pub fn mark_ready(&self) {
        *self.0.write().unwrap_or_else(|p| p.into_inner()) = ConfigAdoptionSnapshot {
            phase: ConfigAdoptionPhase::Ready,
            pending_records: Some(0),
            reasons: Vec::new(),
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_adoption_status_starts_without_a_completed_pass() {
        assert_eq!(
            ConfigAdoptionStatus::default().snapshot(),
            ConfigAdoptionSnapshot {
                phase: ConfigAdoptionPhase::Starting,
                pending_records: None,
                reasons: vec![],
            }
        );
    }

    #[test]
    fn config_adoption_status_clones_share_pending_and_ready_snapshots() {
        let status = ConfigAdoptionStatus::default();
        let reader = status.clone();
        status.waiting(Some(3), vec!["owner unavailable".into()]);
        assert_eq!(
            reader.snapshot(),
            ConfigAdoptionSnapshot {
                phase: ConfigAdoptionPhase::Pending,
                pending_records: Some(3),
                reasons: vec!["owner unavailable".into()],
            }
        );
        status.waiting(None, vec!["scan failed".into()]);
        assert_eq!(reader.snapshot().pending_records, None);
        status.mark_ready();
        assert_eq!(
            reader.snapshot(),
            ConfigAdoptionSnapshot {
                phase: ConfigAdoptionPhase::Ready,
                pending_records: Some(0),
                reasons: vec![],
            }
        );
    }

    #[test]
    fn config_adoption_status_snapshot_round_trips() {
        let status = ConfigAdoptionStatus::default();
        status.waiting(Some(1), vec!["peer below protocol".into()]);
        let snapshot = status.snapshot();
        let encoded = serde_json::to_vec(&snapshot).unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&encoded).unwrap()["phase"],
            "pending"
        );
        assert_eq!(
            serde_json::from_slice::<ConfigAdoptionSnapshot>(&encoded).unwrap(),
            snapshot
        );
    }
}
