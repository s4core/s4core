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

//! S4 Server library.
//!
//! This module exports the public API for the S4 server, including
//! configuration types and the main application.

pub mod bucket_metadata_placement;
pub mod cluster_bootstrap;
pub mod compaction_worker;
pub mod config;
pub mod config_adoption;
pub mod edition;
pub mod iam_adoption;
pub mod iam_upkeep;
pub mod lifecycle_worker;
pub mod multipart_cleanup_worker;
pub mod pool_generation;
pub mod rollback;
pub mod secrets;
pub mod unenforced_configs;

mod app;

pub use app::App;
pub use compaction_worker::CompactionWorker;
pub use config::{ClusterModeConfig, Config, ServerMode};
pub use edition::{
    build_cluster_services, edition_label, record_edition_metrics, resolve_cluster_limits,
    resolve_edition, ResolvedEdition,
};
pub use lifecycle_worker::LifecycleWorker;
pub use multipart_cleanup_worker::MultipartCleanupWorker;
