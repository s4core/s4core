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

//! How `/metrics` renders what the `metrics` facade records.
//!
//! The server and the tests build their recorder here, so a test reads the
//! exposition the server would serve and not one configured on its own.

use metrics_exporter_prometheus::{BuildError, PrometheusBuilder};

/// Bucket bounds, in seconds, of every histogram S4 exports. Defined next to
/// the cluster metrics, whose tests build their recorder with the same bounds.
pub use s4_cluster::DURATION_BUCKETS_SECONDS;

/// Builder for the Prometheus recorder behind `/metrics`.
///
/// Without buckets the exporter renders every histogram as a summary: the
/// quantiles one node computed over its own requests. Those cannot be added up
/// across the nodes of a cluster, so a p99 for the cluster could not be had at
/// all. With buckets each node exports counts per bound, which do add up.
pub fn builder() -> Result<PrometheusBuilder, BuildError> {
    PrometheusBuilder::new().set_buckets(DURATION_BUCKETS_SECONDS)
}
